//#define RPERF

using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Diagnostics;
using Microsoft.WindowsAzure.Storage.ChainTableInterface;
using Microsoft.Azure.Toolkit.Replication;

namespace Microsoft.WindowsAzure.Storage.RTable
{
    class RTableAdapter : IChainTable
    {
        public RTableAdapter(ReplicatedTable rtable)
        {
            this.rtable = rtable;
        }

        public TableResult Execute(TableOperation operation,
            TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            var oldOp = operation;

            // Encapsulate
            operation = EncapsulateOp(oldOp, operationContext);

            // Execute

            var res = rtable.Execute(operation, requestOptions, operationContext);


            // Currently RTable do not throw exception on error, so we throw it...
            var opType = GetOpType(oldOp);

            // the published rtable returns 204 for insert
            if (opType == TableOperationType.Insert && res.HttpStatusCode == (int)HttpStatusCode.NoContent)
                res.HttpStatusCode = (int)HttpStatusCode.Created;
            // the published rtable returns 409 for precondition failure on merge / replace / delete
            if ((opType == TableOperationType.Merge || opType == TableOperationType.Replace || opType == TableOperationType.Delete)
                && res.HttpStatusCode == (int)HttpStatusCode.Conflict)
                res.HttpStatusCode = (int)HttpStatusCode.PreconditionFailed;

            if (
                (opType == TableOperationType.Insert && res.HttpStatusCode != (int)HttpStatusCode.Created) ||
                (opType == TableOperationType.Replace && res.HttpStatusCode != (int)HttpStatusCode.NoContent) ||
                (opType == TableOperationType.Merge && res.HttpStatusCode != (int)HttpStatusCode.NoContent) ||
                (opType == TableOperationType.InsertOrReplace && res.HttpStatusCode != (int)HttpStatusCode.NoContent) ||
                (opType == TableOperationType.InsertOrMerge && res.HttpStatusCode != (int)HttpStatusCode.NoContent) ||
                (opType == TableOperationType.Delete && res.HttpStatusCode != (int)HttpStatusCode.NoContent) ||
                (opType == TableOperationType.Retrieve && res.HttpStatusCode != (int)HttpStatusCode.OK && res.HttpStatusCode != (int)HttpStatusCode.NotFound)
                )
            {
                // throw an exception...
                var reqres = new RequestResult();
                reqres.HttpStatusCode = res.HttpStatusCode;
                var ex = new StorageException(reqres, null, null);

                throw ex;
            }

            // Decapsulate
            Decapsulate(res, oldOp, operationContext);

            return res;
        }

              
        public IList<TableResult> ExecuteBatch(TableBatchOperation batch,
            TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            TableBatchOperation newBatch = new TableBatchOperation();
            List<TableOperation> opList = new List<TableOperation>();
            foreach (var op in batch)
            {
                opList.Add(op);
                newBatch.Add(EncapsulateOp(op, operationContext));
            }

            try
            {
                var res = rtable.ExecuteBatch(newBatch, requestOptions, operationContext);

                for (int i = 0; i < res.Count; ++i)
                {
                    Decapsulate(res[i], opList[i], operationContext);
                }

                return res;
            }
            catch (StorageException ex)
            {
                int errorIndex = 0;

                // Currently RTable does not return failed index, always set to 0

                /*
                if (batch.Count > 1)
                {
                    try
                    {
                        throw new ChainTableBatchException(0, ex);
                        // find the error index
                        // WARNING: this is not very reliable
                        // c.f. http://stackoverflow.com/questions/14282385/azure-cloudtable-executebatchtablebatchoperation-throws-a-storageexception-ho/14290910#14290910
                        var msg = ex.RequestInformation.ExtendedErrorInformation.ErrorMessage;
                        var parts = msg.Split(':');
                        errorIndex = int.Parse(parts[0]);
                    }
                    catch (Exception)
                    {
                        // ignore, just say errorIndex = 0
                    }
                }
                */

                // change RTableConflictException to a standard StorageException with httpcode = conflict
                if (ex is ReplicatedTableConflictException)
                {
                    RequestResult res = new RequestResult();
                    res.HttpStatusCode = (int)HttpStatusCode.Conflict;
                    ex = new StorageException(res, "RTable conflict exception", null);
                }

                throw new ChainTableInterface.ChainTableBatchException(errorIndex, ex);
            }           
        }
        

        public Task<TableResult> ExecuteAsync(TableOperation operation,
            TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            // WARNING:
            // KNOWN ISSUE: if the user changes the entity in operation immediately after executeAsync, there is a race
            // that the operation may be done with the new entity.
            // Consider clone the operation before returning from ExecuteAsync
            // One caveat: the returned TableResult.Result should point to the same object as the one in request...

            // KNOWN ISSUE: if the user changes the entity in operation later than Encapsulate during Execute, but before
            // Decapsulate, the operation will be done with the old entity, and Decapsulate will overwrite the user-changed
            // entity back to the old version

            var handle = Task.Factory.StartNew<TableResult>(() => Execute(operation, requestOptions, operationContext));
            return handle;
        }

        private TableOperation EncapsulateOp(TableOperation oldOp, OperationContext operationContext)
        {
            TableOperation result = null;
            var opType = GetOpType(oldOp);
            // Encapsulate
            if (opType == TableOperationType.Insert
                || opType == TableOperationType.Replace
                || opType == TableOperationType.Merge
                || opType == TableOperationType.InsertOrReplace
                || opType == TableOperationType.InsertOrMerge
                || opType == TableOperationType.Delete)
            {
                // encapsulate user's argument
                var row = GetEntityFromOperation(oldOp);
                DynamicReplicatedTableEntity encapsulatedRow =
                    new DynamicReplicatedTableEntity(row.PartitionKey, row.RowKey, row.ETag, row.WriteEntity(operationContext));

                if (opType == TableOperationType.Insert)
                    result = TableOperation.Insert(encapsulatedRow, true);
                else if (opType == TableOperationType.Replace)
                    result = TableOperation.Replace(encapsulatedRow);
                else if (opType == TableOperationType.Merge)
                    result = TableOperation.Merge(encapsulatedRow);
                else if (opType == TableOperationType.InsertOrReplace)
                    result = TableOperation.InsertOrReplace(encapsulatedRow);
                else if (opType == TableOperationType.InsertOrMerge)
                    result = TableOperation.InsertOrMerge(encapsulatedRow);
                else if (opType == TableOperationType.Delete)
                    result = TableOperation.Delete(encapsulatedRow);
            }
            else if (opType == TableOperationType.Retrieve)
            {
                var pKey = GetPartitionKeyFromOperation(oldOp);
                var rKey = GetRowKeyFromOperation(oldOp);
                result = TableOperation.Retrieve<DynamicReplicatedTableEntity>(pKey, rKey);
            }

            return result;
        }

        private void Decapsulate(TableResult res, TableOperation oldOp, OperationContext operationContext)
        {
            if (res.Result != null)
            {
                if (!(res.Result is DynamicReplicatedTableEntity))
                    throw new Exception("SHOULD NOT HAPPEN");

                var opType = GetOpType(oldOp);
                var result = (DynamicReplicatedTableEntity)res.Result;
                if (opType == TableOperationType.Retrieve)
                {
                    // For retrieve, we need to construct a new user-defined type using reflection
                    var resolverField = oldOp.GetType().GetField("retrieveResolver", BindingFlags.Instance | BindingFlags.NonPublic);
                    var resolver = (Func<string, string, DateTimeOffset, IDictionary<string, EntityProperty>, System.String, System.Object>)
                        resolverField.GetValue(oldOp);

                    var newRes = (ITableEntity)resolver.Invoke(result.PartitionKey, result.RowKey, result.Timestamp, result.Properties, result.ETag);
                    res.Result = newRes;
                }
                else
                {
                    var row = GetEntityFromOperation(oldOp);

                    row.PartitionKey = result.PartitionKey;
                    row.RowKey = result.RowKey;
                    row.ETag = result.ETag;
                    row.Timestamp = result.Timestamp;
                    row.ReadEntity(result.Properties, operationContext);

                    res.Result = row;
                }
            }
        }

        private TableOperationType GetOpType(TableOperation operation)
        {

            // WARNING: We use reflection to read an internal field in OperationType.
            //          We have a dependency on TableOperation fields in WindowsAzureStorage dll
            PropertyInfo opType = operation.GetType().GetProperty("OperationType", System.Reflection.BindingFlags.GetProperty |
                                                               System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            TableOperationType opTypeValue = (TableOperationType)(opType.GetValue(operation, null));

            return opTypeValue;
        }

        private ITableEntity GetEntityFromOperation(TableOperation operation)
        {

            // WARNING: We use reflection to read an internal field in OperationType.
            //          We have a dependency on TableOperation fields WindowsAzureStorage dll
            PropertyInfo entity = operation.GetType().GetProperty("Entity", System.Reflection.BindingFlags.GetProperty |
                                                                   System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            return (ITableEntity)(entity.GetValue(operation, null));
        }

        private string GetPartitionKeyFromOperation(TableOperation operation)
        {

            // WARNING: We use reflection to read an internal field in OperationType.
            //          We have a dependency on TableOperation fields WindowsAzureStorage dll
            PropertyInfo entity = operation.GetType().GetProperty("RetrievePartitionKey", System.Reflection.BindingFlags.GetProperty |
                                                                   System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            return (string)(entity.GetValue(operation, null));
        }

        private string GetRowKeyFromOperation(TableOperation operation)
        {

            // WARNING: We use reflection to read an internal field in OperationType.
            //          We have a dependency on TableOperation fields WindowsAzureStorage dll
            PropertyInfo entity = operation.GetType().GetProperty("RetrieveRowKey", System.Reflection.BindingFlags.GetProperty |
                                                                   System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            return (string)(entity.GetValue(operation, null));
        }

        private void SetEntityInOperation(TableOperation operation, DynamicReplicatedTableEntity encapsulatedRow)
        {

            // WARNING: We use reflection to read an internal field in OperationType.
            //          We have a dependency on TableOperation fields WindowsAzureStorage dll
            PropertyInfo entity = operation.GetType().GetProperty("Entity", System.Reflection.BindingFlags.GetProperty |
                                                                   System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            entity.SetValue(operation, encapsulatedRow);
        }


        private ReplicatedTable rtable;


        public string GetTableID()
        {
            return "__RTable_" + rtable.TableName;
        }


        public bool CreateIfNotExists()
        {
            return rtable.CreateIfNotExists();
        }

        public bool DeleteIfExists()
        {
            return rtable.DeleteIfExists();
        }


        public IEnumerable<TElement> ExecuteQuery<TElement>(TableQuery<TElement> q) where TElement : ITableEntity, new()
        {
            TableQuery<DynamicReplicatedTableEntity> encapedQuery = new TableQuery<DynamicReplicatedTableEntity>();
            encapedQuery.FilterString = q.FilterString;
            encapedQuery.SelectColumns = q.SelectColumns;

            if (encapedQuery.SelectColumns != null)
            {
                // should at least contain rtable's metadata
                encapedQuery.SelectColumns.Add("_rtable_RowLock");
                encapedQuery.SelectColumns.Add("_rtable_Version");
                encapedQuery.SelectColumns.Add("_rtable_Tombstone");
                encapedQuery.SelectColumns.Add("_rtable_ViewId");
                encapedQuery.SelectColumns.Add("_rtable_Operation");
                encapedQuery.SelectColumns.Add("_rtable_BatchId");
                encapedQuery.SelectColumns.Add("_rtable_LockAcquisition");
            }
            encapedQuery.TakeCount = q.TakeCount;

            var rawres = rtable.ExecuteQuery(encapedQuery);

            var res = from rawEnt in rawres
                      select DecapsulateEnt<TElement>(rawEnt);

            return res;
        }

        private TElement DecapsulateEnt<TElement>(DynamicReplicatedTableEntity rawEnt) where TElement : ITableEntity, new()
        {
            // For retrieve, we need to construct a new user-defined type using reflection
            var retOp = TableOperation.Retrieve<TElement>("1", "1");
            var resolverField = retOp.GetType().GetField("retrieveResolver", BindingFlags.Instance | BindingFlags.NonPublic);
            var resolver = (Func<string, string, DateTimeOffset, IDictionary<string, EntityProperty>, System.String, System.Object>)
                resolverField.GetValue(retOp);

            // RTable's query interface forgot to virtualize etag, so we virtualize it here
            // i.e., use Version.toString() rather than rawEnt.ETag, c.f., Decapsulate()
            var newEnt = (TElement)resolver.Invoke(rawEnt.PartitionKey, rawEnt.RowKey, rawEnt.Timestamp, rawEnt.Properties, rawEnt._rtable_Version.ToString());
            return newEnt;
        }
    }
}
