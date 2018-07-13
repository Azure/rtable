using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage.ChainTableInterface;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.STable
{
    public class STable : IChainTable
    {
        public string TableName { get; private set; }

        public STable(string tableName, ISTableConfig config, IChainTableService cts)
        {
            TableName = tableName;
            this.ts = new TableService(config, cts);
        }


        // Table management

        public bool CreateIfNotExists()
        {
            IChainTable headTable = ts.GetHead();
            if (!headTable.CreateIfNotExists())
                return false;

            var meta = new STableMetadataEntity();

            var res = StorageLayer.ModifyRow(headTable, TableOperationType.Insert, meta, null);
            if (res == null || res.HttpStatusCode != (int)HttpStatusCode.Created)
                return false;

            return true;

        }

        public bool DeleteIfExists()
        {
            IChainTable headTable = ts.GetHead();

            var metaRes = StorageLayer.RetrieveRow(headTable, STableMetadataEntity.RetrieveMetaOp());

            if (metaRes != null && metaRes.HttpStatusCode == (int)HttpStatusCode.OK && metaRes.Result != null)
            {
                var metaBlock = (STableMetadataEntity)metaRes.Result;
                int ssid = metaBlock.CurrentSSID;
                for (int i = 0; i < metaBlock.CurrentSSID; ++i)
                {
                    var snapshot = ts.GetSnapshot(i);
                    snapshot.DeleteIfExists();
                }

                headTable.DeleteIfExists();
                return true;
            }
            else
                return false;
        }


        // Operation API

        // standard operation
        public TableResult Execute(TableOperation operation, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            if (operation == null)
                throw new ArgumentNullException();

            TableOperationType opType = GetOpType(operation);
            if (opType == TableOperationType.Retrieve)
                return HandleRetrieve(operation, requestOptions, operationContext);
            else if (opType == TableOperationType.Insert)
                return HandleInsert(operation, requestOptions, operationContext);
            else if (opType == TableOperationType.Replace || opType == TableOperationType.Merge || opType == TableOperationType.Delete)
                return HandleModify(operation, requestOptions, operationContext);
            else if (opType == TableOperationType.InsertOrMerge || opType == TableOperationType.InsertOrReplace)
                return HandleIOX(operation, requestOptions, operationContext);
            else
                throw new Exception("Unknown operation type: " + opType);
        }

        // added operation: read history
        public TableResult RetrieveFromSnapshot(TableOperation operation, int snapshotId, TableRequestOptions options = null, OperationContext context = null)
        {
            return HandleRetrieveFromSnapshot(operation, snapshotId, options, context);
        }


        // Batch API

        public IList<TableResult> ExecuteBatch(TableBatchOperation batch, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            return HandleExecuteBatch(batch, requestOptions, operationContext);
        }


        // Async API, naive impl.

        public Task<TableResult> ExecuteAsync(TableOperation operation, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            var handle = Task.Factory.StartNew<TableResult>(() => Execute(operation, requestOptions, operationContext));
            return handle;
        }


        // Query API
        public IEnumerable<TElement> ExecuteQuery<TElement>(TableQuery<TElement> q) where TElement : ITableEntity, new()
        {
            return HandleExecuteQuery<TElement>(q);
        }


        // Snapshot management

        // return all the history information of this STable
        public STableHistoryInfo GetSTableHistory()
        {
            return HandleGetSTableHistory();
        }

        // Return: snapshot Id
        public int CreateSnapshot()
        {
            return HandleCreateSnapshot(-1).CurrentSSID - 1;
        }

        public void DeleteSnapshot(int snapshotId)
        {
            HandleDeleteSnapshot(snapshotId);
        }
        public IList<int> ListValidSnapshots()
        {
            return HandleListValidSnapshots();
        }

        // Return: the created snapshot containing the system state before rollback
        public int Rollback(int snapshotId)
        {
            return HandleRollback(snapshotId);
        }


        // Garbage Collection

        public void GC()
        {
            HandleGC();
        }


        // For IChainTable interface

        // TableID is used by IChainTableFactory, which is transparent to user
        // User only care about TableName
        public string GetTableID()
        {
            return "__STable_" + TableName;
        }



        // private members
        private TableService ts = null;
        private Random random = new Random();



        // Single-row operations, including RetrieveFromSnapshot

        // Retrieve the latest version of a row
        private TableResult HandleRetrieve(TableOperation operation, TableRequestOptions options, OperationContext context)
        {
            // get parameters
            var partitionKey = GetPartitionKeyFromOperation(operation);
            var rowKey = GetRowKeyFromOperation(operation);

            // retrieve can be executed directly, throw any exception
            var headTable = ts.GetHead();
            var response = StorageLayer.RetrieveRow(headTable, TableOperation.Retrieve<STableEntity>(partitionKey, rowKey), options, context);

            // handle result
            if (response == null)
                throw GenerateException(HttpStatusCode.ServiceUnavailable);
            else if (response.HttpStatusCode == (int)HttpStatusCode.NotFound)
                return response;
            else if (response.HttpStatusCode == (int)HttpStatusCode.OK && response.Result != null)
            {
                var rawData = (STableEntity)response.Result;

                // check delete bit, construct response
                var ret = new TableResult();
                if (rawData.Deleted)
                {
                    ret.HttpStatusCode = (int)HttpStatusCode.NotFound;
                    ret.Result = null;
                    ret.Etag = null;
                }
                else
                {
                    ret.Etag = rawData.VETag;
                    ret.HttpStatusCode = response.HttpStatusCode;
                    ret.Result = Decapsulate(rawData, operation);
                }
                return ret;
            }
            else
                throw GenerateException((HttpStatusCode)response.HttpStatusCode);
        }

        private TableResult HandleInsert(TableOperation operation, TableRequestOptions options, OperationContext context)
        {
            // get parameters
            var userArg = GetEntityFromOperation(operation);
            if (userArg == null)
                throw GenerateException(HttpStatusCode.BadRequest);

            // parse user arg, extract key
            var partitionKey = userArg.PartitionKey;
            var rowKey = userArg.RowKey;

            // First, retrieve the metadata
            var headTable = ts.GetHead();
            var metaResult = StorageLayer.RetrieveRow(headTable, STableMetadataEntity.RetrieveMetaOp());
            if (metaResult == null || metaResult.HttpStatusCode != (int)HttpStatusCode.OK || metaResult.Result == null)
                throw GenerateException(HttpStatusCode.ServiceUnavailable);
            var meta = (STableMetadataEntity)metaResult.Result;


            // encapsulate to STable entity
            var arg = Encapsulate(userArg);

            // two cases to check: either the row does not exist, or the row exists with a deleted bit
            // always try to insert first
            bool succeed = false;
            try
            {
                InsertRowCOW(arg, meta, options, context);
                succeed = true;
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.Conflict)
                    // give up
                    throw e;
            }

            if (!succeed)
            {
                // row exists, retrieve
                var retrieveRes = StorageLayer.RetrieveRow(headTable, TableOperation.Retrieve<STableEntity>(partitionKey, rowKey));
                if (retrieveRes == null || retrieveRes.HttpStatusCode != (int)HttpStatusCode.OK || retrieveRes.Result == null)
                    // something weird happened
                    throw GenerateException(HttpStatusCode.ServiceUnavailable);

                // row physically exist, check delete bit
                var originalRow = (STableEntity)retrieveRes.Result;
                if (!originalRow.Deleted)
                    throw GenerateException(HttpStatusCode.Conflict);

                // now we can simply replace the current row w/ COW feature, throw any exception
                // ignore returned petag
                
                ModifyRowCOW(originalRow, TableOperationType.Replace, arg, meta, options, context);
            }

            // succeed
            var ret = new TableResult();
            ret.Result = userArg;
            userArg.ETag = arg.VETag;    // also set new vetag to userArg
            userArg.Timestamp = arg.Timestamp;  // insert sets timestamp to userArg

            ret.HttpStatusCode = (int)HttpStatusCode.Created;
            ret.Etag = userArg.ETag;

            return ret;
        }

        private TableResult HandleModify(TableOperation operation, TableRequestOptions options, OperationContext context)
        {
            // get parameters
            var opType = GetOpType(operation);
            var opBackup = opType;
            Assert.IsTrue(opType == TableOperationType.Replace || opType == TableOperationType.Merge || opType == TableOperationType.Delete);
            var userArg = GetEntityFromOperation(operation);
            if (userArg == null)
                throw GenerateException(HttpStatusCode.BadRequest);

            // parse user arg, extract key & precondition
            var partitionKey = userArg.PartitionKey;
            var rowKey = userArg.RowKey;
            var precond = userArg.ETag;

            // First, retrieve the row and the metadata
            var headTable = ts.GetHead();
            var retrieveOps = new List<InternalOperation>();
            retrieveOps.Add(new InternalOperation(headTable, STableMetadataEntity.RetrieveMetaOp()));
            retrieveOps.Add(new InternalOperation(headTable, TableOperation.Retrieve<STableEntity>(partitionKey, rowKey)));
            var retrieveRes = StorageLayer.ConcurrentRetrieve(retrieveOps);

            // check retrieved results
            if (retrieveRes == null || retrieveRes[0] == null || retrieveRes[1] == null 
                || retrieveRes[0].HttpStatusCode != (int)HttpStatusCode.OK || retrieveRes[0].Result == null)
                // throw ServiceUnavaialbe if we fail to access metadata
                throw GenerateException(HttpStatusCode.ServiceUnavailable);

            if (retrieveRes[1].HttpStatusCode != (int)HttpStatusCode.OK || retrieveRes[1].Result == null)
                // if we get notfound, throw notfound. this is the correct behavior.
                // otherwise, throw whatever we get.
                throw GenerateException((HttpStatusCode)retrieveRes[1].HttpStatusCode);


            // row physically exist
            var meta = (STableMetadataEntity)retrieveRes[0].Result;
            var originalRow = (STableEntity)retrieveRes[1].Result;

            // check delete bit and vetag
            if (originalRow.Deleted)
                throw GenerateException(HttpStatusCode.NotFound);

            if (!VETagMatch(precond, originalRow.VETag))
                throw GenerateException(HttpStatusCode.PreconditionFailed);

            // encapsulate user's arg to STable's arg
            var arg = Encapsulate(userArg);

            // change delete to a special replace
            if (opType == TableOperationType.Delete)
            {
                opType = TableOperationType.Replace;
                arg.Deleted = true;
            }

            // modify the row, throw any exception, ignore returned physical etag
            ModifyRowCOW(originalRow, opType, arg, meta, options, context);

            var ret = new TableResult();
            ret.Result = userArg;
            if (opBackup != TableOperationType.Delete)
            {
                // replace & merge set new vetag to userArg, delete does not set it.
                userArg.ETag = arg.VETag;
            }
            // replace, merge and delete do not change timestamp of the returned entity

            ret.HttpStatusCode = (int)HttpStatusCode.NoContent;
            ret.Etag = userArg.ETag;

            return ret;       
        }

        private TableResult HandleIOX(TableOperation operation, TableRequestOptions options, OperationContext context)
        {
            // get parameters
            var opType = GetOpType(operation);
            var userArg = GetEntityFromOperation(operation);
            if (userArg == null)
                throw GenerateException(HttpStatusCode.BadRequest);

            // parse user arg, extract key & precondition
            var partitionKey = userArg.PartitionKey;
            var rowKey = userArg.RowKey;

            // First, retrieve the row and the metadata
            var headTable = ts.GetHead();
            var retrieveOps = new List<InternalOperation>();
            retrieveOps.Add(new InternalOperation(headTable, STableMetadataEntity.RetrieveMetaOp()));
            retrieveOps.Add(new InternalOperation(headTable, TableOperation.Retrieve<STableEntity>(partitionKey, rowKey)));
            var retrieveRes = StorageLayer.ConcurrentRetrieve(retrieveOps);

            // check retrieved results
            if (retrieveRes == null || retrieveRes[0] == null || retrieveRes[1] == null 
                || retrieveRes[0].HttpStatusCode != (int)HttpStatusCode.OK || retrieveRes[0].Result == null)
                throw GenerateException(HttpStatusCode.ServiceUnavailable);

            var meta = (STableMetadataEntity)retrieveRes[0].Result;
            
            // encapsulate user's arg to STable's arg
            var arg = Encapsulate(userArg);

            if (retrieveRes[1].HttpStatusCode == (int)HttpStatusCode.NotFound)
            {
                // row does not exist, just insert a row, throw any exception
                InsertRowCOW(arg, meta, options, context);
            }
            else if (retrieveRes[1].HttpStatusCode == (int)HttpStatusCode.OK && retrieveRes[1].Result != null)
            {
                // row exists
                var originalRow = (STableEntity)retrieveRes[1].Result;

                if (!originalRow.Deleted)
                {
                    // if the row is logically there, issue replace / merge
                    if (opType == TableOperationType.InsertOrReplace)
                        opType = TableOperationType.Replace;
                    else
                        opType = TableOperationType.Merge;
                }
                else
                {
                    // if the row is logically deleted, always issue replace
                    opType = TableOperationType.Replace;
                }

                ModifyRowCOW(originalRow, opType, arg, meta, options, context);
            }
            else
                // unknown cases
                throw GenerateException(HttpStatusCode.ServiceUnavailable);

            // succeed
            var ret = new TableResult();
            ret.Result = userArg;
            userArg.ETag = arg.VETag;    // IOX sets new vetag to userArg
            // iox does not change timestamp of userArg
            
            ret.HttpStatusCode = (int)HttpStatusCode.NoContent;
            ret.Etag = arg.VETag;

            return ret;      
        }

        private TableResult HandleRetrieveFromSnapshot(TableOperation operation, int ssid, TableRequestOptions options, OperationContext context)
        {
            // get parameters
            var partitionKey = GetPartitionKeyFromOperation(operation);
            var rowKey = GetRowKeyFromOperation(operation);

            // First, retrieve the row and the metadata
            var headTable = ts.GetHead();
            var retrieveOps = new List<InternalOperation>();
            retrieveOps.Add(new InternalOperation(headTable, STableMetadataEntity.RetrieveMetaOp()));
            retrieveOps.Add(new InternalOperation(headTable, TableOperation.Retrieve<STableEntity>(partitionKey, rowKey)));
            var retrieveRes = StorageLayer.ConcurrentRetrieve(retrieveOps);

            // check retrieved results
            if (retrieveRes == null || retrieveRes[0] == null || retrieveRes[1] == null
                || retrieveRes[0].HttpStatusCode != (int)HttpStatusCode.OK || retrieveRes[0].Result == null)
                throw GenerateException(HttpStatusCode.ServiceUnavailable);

            var meta = (STableMetadataEntity)retrieveRes[0].Result;
            
            if (!meta.IsSnapshotValid(ssid))
                // not alowed, we can only read into history
                throw GenerateException(HttpStatusCode.BadRequest);

            // res should return the TableResult of accessing the snapshot (i.e., not decapsulated, contain the STableEntity)
            // needOpt should be set to true if the snapshot is not retrieved from ST_{ssid}.
            TableResult res = null;
            bool needOpt = false;

            // handle result
            if (retrieveRes[1].HttpStatusCode == (int)HttpStatusCode.NotFound)
            {
                // row has never been inserted throughout the history
                // in this case, we need to insert a dummy row to solidify this snapshot
                // see comment below

                // no vetag & payload
                var dummyInsertArg = new STableEntity(partitionKey, rowKey);
                dummyInsertArg.SSID = meta.CurrentSSID;
                dummyInsertArg.Deleted = true;

                // throw any exception
                InsertRowCOW(dummyInsertArg, meta, null, null);

                // succeed, return the "deleted" dummy row for optimiation, same as returning a "NotFound"
                res = new TableResult();
                res.HttpStatusCode = (int)HttpStatusCode.OK;
                res.Result = dummyInsertArg;
                res.Etag = null;
                needOpt = true;
            }
            else if (retrieveRes[1].HttpStatusCode == (int)HttpStatusCode.OK && retrieveRes[1].Result != null)
            {
                var headData = (STableEntity)retrieveRes[1].Result;

                if (headData.SSID > ssid)
                {
                    // in this case, row's snapshot 0 - snapshot headData.SSID - 1 (incl. ssid) have been solidified
                    // directly go to the corresponding table, throw any exception
                    int steps = 0;
                    res = ReadSnapshotChain(partitionKey, rowKey, ssid, meta, out steps, options, context);
                    needOpt = (steps > 1);
                }
                else
                {
                    // headData.SSID <= ssid < CurrentSSID
                    // In this case, we should return the row in place.
                    // However, the snapshot ssid is not yet valid, and we are vulnerable to concurrent modification (esp. slow update)
                    // We want to solidify the row to CurrentSSID - 1 (will make snapshot ssid valid, and equal to current row) before returning.
                    // See invariant 4 in ModifyRowCOW & comment on SSID in STableEntity.

                    // We issue a "dummy" update on this row in SSID CurrentSSID.
                    // This will COW the current row to snapshot table headData.SSID, and increament this row's current SSID to CurrentSSID.
                    // Once this "dummy" update succeeds, no one can change this row from SSID headData.SSID to CurrentSSID - 1, including ssid.

                    // This technique prevents concurrent updates to change the row's content on snapshot ssid, and causes non-repeatable read.
                    // E.g., Assume initially the row's head SSID is ssid, and an update reads CurrentSSID = ssid, then sleeps a long time
                    // before updating the row. Someone takes snapshot ssid, and we come to read this row in snapshot ssid before the update awakes.
                    // If we do not issue the dummy update, later the update can wake up and successfully update the row in ssid.
                    // In this case, if we read this row in history ssid again, we will get a different result.

                    var dummyMergeArg = new STableEntity(partitionKey, rowKey);
                    dummyMergeArg.Deleted = headData.Deleted;
                    dummyMergeArg.VETag = headData.VETag;
                    dummyMergeArg.Payload.Clear();

                    // throw any exception, ignore returned physical ETag
                    ModifyRowCOW(headData, TableOperationType.Merge, dummyMergeArg, meta, null, null);

                    // now, the snapshots have been solidified
                    res = retrieveRes[1];
                    needOpt = (headData.SSID < ssid);
                }
            }
            else
                throw GenerateException(HttpStatusCode.ServiceUnavailable);

            Assert.IsTrue(res != null && (res.HttpStatusCode == (int)HttpStatusCode.NotFound
                || (res.HttpStatusCode == (int)HttpStatusCode.OK && res.Result != null && res.Result is STableEntity)));

            if (needOpt)
                // optimization, copy snapshot in place to accelerate future requests
                CopySnapshot(partitionKey, rowKey, res, ssid);

            // decapsulate
            if (res.HttpStatusCode == (int)HttpStatusCode.OK)
            {
                var rawData = (STableEntity)res.Result;
                if (rawData.Deleted)
                {
                    res.HttpStatusCode = (int)HttpStatusCode.NotFound;
                    res.Result = null;
                    res.Etag = null;
                }
                else
                {
                    res.HttpStatusCode = (int)HttpStatusCode.OK;
                    res.Result = Decapsulate(rawData, operation);
                    res.Etag = rawData.VETag;
                }
            }

            return res;   
        }


        // Methods to modify a row in head table while corrspondingly updating the chained
        // snapshot tables.
        // All operations to the head table should go through these methods.

        // Insert a row into the head table as the initial version. No COW is needed.
        // The row shold have correct delete bit, vetag and payload, The ssid and the version
        // are managed by this method.
        // SSID will be set to meta.CurrentSSID.
        // Version will be set to 1.
        // 
        // Requirement:
        //    1. The row must not exist.
        // Return:
        //    1. On success, return the physical eTag of the inserted row in head table.
        //       The ssid and the version are assigned to the row. Timestamp is updated.
        //    2. Otherwise, throw a storage exception with http error code.
        private string InsertRowCOW(STableEntity row, STableMetadataEntity meta, 
            TableRequestOptions options, OperationContext context)
        {
            row.SSID = meta.CurrentSSID;
            row.Version = STableEntity.InitialVersion;

            var headTable = ts.GetHead();
            var flushRes = StorageLayer.ModifyRow(headTable, TableOperationType.Insert, row, null, options, context);
            if (flushRes == null)
                throw GenerateException(HttpStatusCode.ServiceUnavailable);
            else if (flushRes.HttpStatusCode != (int)HttpStatusCode.Created)
                // failed
                // if the row exist, we get conflict, and also throw conflict
                throw GenerateException((HttpStatusCode)flushRes.HttpStatusCode);

            return flushRes.Etag;
        }

        // Input an existing row in head table. Execute opType (can only be Replace /
        // Merge) with arg on it. COW if necessary.
        // The argument should have correct delete bit, vetag and payload. The SSID and
        // the version are managed by this method.
        // SSID will be updated to max(meta.CurrentSSID, originalRow.SSID).
        // Version will be originalRow.Version + 1.
        //
        // Requirement:
        //    1. the row exists, and is latest (we will condition on its current petag).
        //    2. opType is replace or merge.
        //    3. the operation is on the original row, i.e., arg.key == originalRow.key.
        // Return:
        //    1. On success, return the physical eTag of the modified row in head table.
        //       The ssid and the version number are also assigned to the arg. Timestamp
        //       is updated.
        //    2. On fail, throw a http error code.
        // Invariants:
        //    1. Update to a row in head table via this API is linearlizable.
        //    2. The ssid of a row in head table never decrease, i.e., we only modify the
        //       row to the same or later ssid (= meta.CurrentSSID).
        //    3. The version of a row in head table always increase for every successful
        //       call. Every <s, v> maps to a unique entity.
        //    4. Once a ModifyRowCOW takes a row in head table from snapshot s to s',
        //       snapshots of the row in s, s+1, ..., s'-1 are solidified forever in the
        //       version that this operation sees (i.e., parameter originalRow).
        private string ModifyRowCOW(STableEntity originalRow, TableOperationType opType, STableEntity arg,
            STableMetadataEntity meta, TableRequestOptions options, OperationContext context)
        {
            Assert.IsTrue(originalRow != null);
            Assert.IsTrue(opType == TableOperationType.Replace || opType == TableOperationType.Merge);
            Assert.IsTrue(arg != null);
            Assert.IsTrue(arg.PartitionKey == originalRow.PartitionKey && arg.RowKey == originalRow.RowKey);

            // extract parameters
            var originalPETag = originalRow.ETag;
            var partitionKey = originalRow.PartitionKey;
            var rowKey = originalRow.RowKey;

            // Assign new ssid of the row after this modification.
            // In most cases, new ssid = meta.CurrentSSID.
            // But if currentSSID <= originalRow.SSID, we are too slow, others has made huge progress.
            // We are concurrent with the snapshot op & the op that updates the row after snapshot.
            // In this case, we simply set new SSID = originalRow.SSID.
            // This ensures a row's ssid never decrease.
            arg.SSID = Math.Max(meta.CurrentSSID, originalRow.SSID);

            // Assign new version of the row
            arg.Version = originalRow.Version + 1;

            var headTable = ts.GetHead();
            if (arg.SSID == originalRow.SSID)
            {
                // no need to COW, use a simple atomic write
                var flushRes = StorageLayer.ModifyRow(headTable, opType, arg, originalPETag, options, context);
                if (flushRes == null || flushRes.HttpStatusCode != (int)HttpStatusCode.NoContent)
                    // failed
                    throw GenerateException(HttpStatusCode.Conflict);

                return flushRes.Etag;
            }
            else
            {
                // arg.SSID > originalRow.SSID, need COW
                Assert.IsTrue(arg.SSID > originalRow.SSID);
                Assert.IsTrue(arg.SSID == meta.CurrentSSID);

                // but what if snapshot originalRow.SSID has been deleted?
                // we should COW to the next valid snapshot...
                // note that meta.CurrentSSID == new SSID > originalRow.SSID
                // so meta has information for snapshot ids 0 .. new SSID - 1, incl. originalRow.SSID.
                if (!meta.IsSnapshotValid(originalRow.SSID))
                {
                    int? nextSnapshot = meta.NextSnapshot(originalRow.SSID);
                    if (nextSnapshot.HasValue)
                    {
                        Assert.IsTrue(arg.SSID > nextSnapshot.Value);
                        originalRow.SSID = nextSnapshot.Value;
                    }
                    else
                    {
                        // we are sure there is no valid snapshot from originalRow.SSID to arg.SSID - 1
                        // as we are just writing arg.SSID, we do not need to COW
                        originalRow.SSID = arg.SSID;
                    }
                }

                if (arg.SSID > originalRow.SSID)
                {
                    // really need COW
                    var cowResponse = CopyOnWrite(originalRow);
                    if (cowResponse != HttpStatusCode.Created)
                    {
                        // failed to cow, abort
                        throw GenerateException(cowResponse);
                    }
                }

                // flush data in place
                var flushRes = StorageLayer.ModifyRow(headTable, opType, arg, originalPETag, options, context);
                if (flushRes == null || flushRes.HttpStatusCode != (int)HttpStatusCode.NoContent)
                {
                    // failed, abort
                    // In such cases, we need not to worry about the COW data we just written to snapshot table.
                    //   - If the head is still in originalRow.SSID, the data we COWed is not yet valid, and will
                    //     be replaced during future modification of the row.
                    //   - If the head has progressed to higher SSID, another operation o' must have done a successful
                    //     ModifyRowCOW, taking the row from SSID to a higher one. So o' must have written the correct
                    //     data to snapshot table.
                    //     But can we violate invariant 4 by having overwritten the COWed data from o'? Note that
                    //     the data COWed by o' must be at least in version <s, v> (otherwise o' will fail). Since
                    //     COW will not decreament the version of this row on ST_s, so either o' overwrites our
                    //     COWed data, or we are both writing <s, v>.
                    throw GenerateException(HttpStatusCode.Conflict);
                }

                return flushRes.Etag;
            }
        }

        // Copy on write the current row to its snapshot table.
        // We only copy a row that once physically exists in head table (i.e., currentRow is read from head).
        // The COW will only succeed if ST_{row.s}[row].ver <= currentRow.ver.
        // If ST_{row.s}[row].ver == currentRow.ver, we will succeed but will not copy again.
        // On succeed, return Created, otherwise return error code.
        private HttpStatusCode CopyOnWrite(STableEntity currentRow)
        {
            var ssTable = ts.GetSnapshot(currentRow.SSID);
                    
            var res = StorageLayer.ModifyRow(ssTable, TableOperationType.Insert, currentRow, null);
            if (res == null)
                return HttpStatusCode.ServiceUnavailable;
            else if (res.HttpStatusCode == (int)HttpStatusCode.NotFound)
                // table not found, meta block is out of date, abort with conflict
                // TODO: shall we return conflict for all cases?
                return HttpStatusCode.Conflict;
            else if (res.HttpStatusCode == (int)HttpStatusCode.Conflict)
            {
                // need to check version
                var partitionKey = currentRow.PartitionKey;
                var rowKey = currentRow.RowKey;
                var retrieveRes = StorageLayer.RetrieveRow(ssTable, TableOperation.Retrieve<STableEntity>(partitionKey, rowKey));
                if (retrieveRes == null || retrieveRes.HttpStatusCode != (int)HttpStatusCode.OK || retrieveRes.Result == null)
                    return HttpStatusCode.ServiceUnavailable;

                var oldContent = (STableEntity)retrieveRes.Result;
                if (oldContent.Version > currentRow.Version)
                    return HttpStatusCode.Conflict;

                if (oldContent.Version == currentRow.Version)
                    return HttpStatusCode.Created;

                // oldContent.Version < currentRow.Version, need to copy again
                var replaceRes = StorageLayer.ModifyRow(ssTable, TableOperationType.Replace, currentRow, oldContent.ETag);
                if (replaceRes == null)
                    return HttpStatusCode.ServiceUnavailable;
                else if (replaceRes.HttpStatusCode == (int)HttpStatusCode.NoContent)
                    return HttpStatusCode.Created;
                else
                    return (HttpStatusCode)replaceRes.HttpStatusCode;
            }
            else
                // on succeed, return created, otherwise return error code
                return (HttpStatusCode)res.HttpStatusCode;
        }


        // Methods to read snapshot chain & optimizations

        private TableResult ReadSnapshotChain(string pKey, string rKey, int ssid, STableMetadataEntity meta,
            out int steps, TableRequestOptions options, OperationContext context)
        {
            steps = 0;
            var ssids = meta.GetSSIDEnumerator(ssid);

            while (ssids.hasMoreSSID())
            {
                ++steps;
                int v = ssids.getSSIDAndMovePrev();
                var retrieveRes = StorageLayer.RetrieveRow(ts.GetSnapshot(v), 
                    TableOperation.Retrieve<STableEntity>(pKey, rKey), options, context);

                if (retrieveRes == null)
                    throw GenerateException(HttpStatusCode.ServiceUnavailable);
                if (retrieveRes.HttpStatusCode == (int)HttpStatusCode.OK && retrieveRes.Result != null)
                {
                    // got it
                    return retrieveRes;
                }
                else if (retrieveRes.HttpStatusCode == (int)HttpStatusCode.NotFound)
                    // check next table
                    continue;
                else
                    throw GenerateException(HttpStatusCode.ServiceUnavailable);
            }

            // not found
            var ret = new TableResult();
            ret.HttpStatusCode = (int)HttpStatusCode.NotFound;
            ret.Result = null;
            ret.Etag = null;
            return ret;
        }

        // optimization, copy a retrieved snapshot (from chain) into the direct snapshot table
        private void CopySnapshot(string pKey, string rKey, TableResult res, int ssid)
        {
            Assert.IsTrue(res != null && (res.HttpStatusCode == (int)HttpStatusCode.NotFound
                || (res.HttpStatusCode == (int)HttpStatusCode.OK && res.Result != null && res.Result is STableEntity)));

            var ssTab = ts.GetSnapshot(ssid);

            STableEntity arg = null;
            if (res.HttpStatusCode == (int)HttpStatusCode.NotFound)
            {
                // in such case, insert a deleted dummy entity
                arg = new STableEntity(pKey, rKey);
                arg.SSID = ssid;
                // version of a dummy entity in an already-solidified snapshot is not important
                arg.Version = STableEntity.InitialVersion;
                arg.Deleted = true;
            }
            else
            {
                // simply copy res.Result to table ssid
                arg = (STableEntity)res.Result;
                // don't care if it succeeds or not
            }

            // don't care if it succeeds or not
            StorageLayer.ModifyRow(ssTab, TableOperationType.Insert, arg, null);
        }



        // batch API

        private IList<TableResult> HandleExecuteBatch(TableBatchOperation batch, TableRequestOptions options, OperationContext context)
        {
            if (batch == null)
                throw new ArgumentException();

            // extract all operation types and arguments
            var ops = new List<Tuple<TableOperationType, ITableEntity>>();
            foreach (TableOperation op in batch)
            {
                var opType = GetOpType(op);
                if (opType == TableOperationType.Retrieve)
                {
                    // in such case, only a single retrieve can be executed
                    if (batch.Count != 1)
                        throw GenerateException(HttpStatusCode.BadRequest);

                    var res = new List<TableResult>();
                    res.Add(HandleRetrieve(op, options, context));
                    return res;
                }

                var arg = GetEntityFromOperation(op);
                ops.Add(new Tuple<TableOperationType, ITableEntity>(opType, arg));
            }

            // Retrieve all the rows and the metadata;
            var headTable = ts.GetHead();
            var retrieveOps = new List<InternalOperation>();
            retrieveOps.Add(new InternalOperation(headTable, STableMetadataEntity.RetrieveMetaOp()));
            foreach (var op in ops)
            {
                retrieveOps.Add(new InternalOperation(headTable, TableOperation.Retrieve<STableEntity>(op.Item2.PartitionKey, op.Item2.RowKey)));
            }

            var retrieveRes = StorageLayer.ConcurrentRetrieve(retrieveOps);
            if (retrieveRes == null || retrieveRes.Count != (ops.Count + 1))
                throw GenerateException(HttpStatusCode.ServiceUnavailable);

            if (retrieveRes[0] == null || retrieveRes[0].HttpStatusCode != (int)HttpStatusCode.OK || retrieveRes[0].Result == null)
                throw GenerateException(HttpStatusCode.ServiceUnavailable);

            // Decide op ssid, here we must have meta.CurrentSSID >= originalRow.ssid
            var meta = (STableMetadataEntity)retrieveRes[0].Result;
            if (!CheckBatchMetaUpToDate(meta, retrieveRes))
            {
                // Try a second time
                var retryMetaRes = StorageLayer.RetrieveRow(headTable, STableMetadataEntity.RetrieveMetaOp());
                if (retryMetaRes == null || retryMetaRes.HttpStatusCode != (int)HttpStatusCode.OK || retryMetaRes.Result == null)
                    throw GenerateException(HttpStatusCode.ServiceUnavailable);

                meta = (STableMetadataEntity)retryMetaRes.Result;

                if (!CheckBatchMetaUpToDate(meta, retrieveRes))
                    // give up
                    throw GenerateException(HttpStatusCode.Conflict);
            }
            
            // Check user-level precondition & generate COW operations & build replies, throw any exception
            List<Tuple<STableEntity, int>> cowRows = null;
            List<Tuple<TableOperationType, ITableEntity, string>> writeOps = null;
            List<TableResult> replies = null;
            CheckBatchAndGeneratePlan(ops, retrieveRes, meta, out cowRows, out writeOps, out replies);

            // Execute COW, throw any exception
            ExecuteBatchCOW(cowRows);

            // Execute write operation, throw any exception
            Assert.IsTrue(writeOps.Count == batch.Count);
            ExecuteBatchWrite(writeOps, options, context);

            // succeed, update userarg's etag & timestamp
            for (int i = 0; i < ops.Count; ++i)
            {
                Assert.IsTrue(((STableEntity)writeOps[i].Item2).VETag.Equals(replies[i].Etag));

                // Delete should not update etag, although we generated a (useless) vetag for the deleted entity.
                if (ops[i].Item1 != TableOperationType.Delete)
                    ops[i].Item2.ETag = replies[i].Etag;

                // only insert update userarg's timestamp
                // TODO: we may fail to get the correct timestamp when insert is changed to a replace
                // see comment on HandleInsert()
                if (ops[i].Item1 == TableOperationType.Insert)
                    ops[i].Item2.Timestamp = writeOps[i].Item2.Timestamp;
            }

            // return replies
            return replies;
        }

        private bool CheckBatchMetaUpToDate(STableMetadataEntity meta, List<TableResult> retrieveRes)
        {
            for (int i = 1; i < retrieveRes.Count; ++i)
            {
                if (retrieveRes[i] != null && retrieveRes[i].HttpStatusCode == (int)HttpStatusCode.OK && retrieveRes[i].Result != null)
                {
                    if (meta.CurrentSSID < ((STableEntity)retrieveRes[i].Result).SSID)
                        return false;
                }
            }

            return true;
        }

        // Check if all the ops in a batch matches their precondition, generate a plan for execute the batch, including:
        //   (1) a list of rows that needs to be copy-on-write (including the opindex of each row for error report)
        //   (2) a list of flushing operations (size == size of batch)
        //   (3) a list of expected replies to the user (size == size of batch)
        private void CheckBatchAndGeneratePlan(List<Tuple<TableOperationType, ITableEntity>> ops, List<TableResult> currentRows, STableMetadataEntity meta,
            out List<Tuple<STableEntity, int>> cowRows, out List<Tuple<TableOperationType, ITableEntity, string>> writeOps, out List<TableResult> replies)
        {
            cowRows = new List<Tuple<STableEntity, int>>();     // Item2 is used to store index, for error report
            writeOps = new List<Tuple<TableOperationType, ITableEntity, string>>();
            replies = new List<TableResult>();

            for (int i = 0; i < ops.Count; ++i)
            {
                var op = ops[i];
                var rowRes = currentRows[i + 1];

                if (rowRes == null)
                    throw GenerateBatchException(HttpStatusCode.ServiceUnavailable, i);

                string newVETag = null;

                if (op.Item1 == TableOperationType.Insert)
                {
                    if (rowRes.HttpStatusCode == (int)HttpStatusCode.NotFound)
                    {
                        STableEntity arg = Encapsulate(op.Item2);
                        arg.SSID = meta.CurrentSSID;
                        arg.Version = STableEntity.InitialVersion;
                        // we cannot immediately overwrite op.Item2, since the batch may fail, in which case we should not change userArg
                        newVETag = arg.VETag;
                        writeOps.Add(new Tuple<TableOperationType, ITableEntity, string>(TableOperationType.Insert, arg, null));
                    }
                    else if (rowRes.HttpStatusCode == (int)HttpStatusCode.OK && rowRes.Result != null)
                    {
                        var row = (STableEntity)rowRes.Result;

                        if (!row.Deleted)
                            throw GenerateBatchException(HttpStatusCode.Conflict, i);

                        int? cowTarget = FindBatchCOWTargetSSID(row, meta);
                        if (cowTarget.HasValue)
                        {
                            // set row.ssid to indicate where to cow
                            row.SSID = cowTarget.Value;
                            cowRows.Add(new Tuple<STableEntity, int>(row, i));
                        }

                        STableEntity arg = Encapsulate(op.Item2);
                        arg.SSID = meta.CurrentSSID;
                        arg.Version = row.Version + 1;
                        newVETag = arg.VETag;
                        writeOps.Add(new Tuple<TableOperationType, ITableEntity, string>(TableOperationType.Replace, arg, row.ETag));
                    }
                    else
                        throw GenerateBatchException((HttpStatusCode)rowRes.HttpStatusCode, i);

                    replies.Add(new TableResult() { Result = op.Item2, HttpStatusCode = (int)HttpStatusCode.Created, Etag = newVETag });
                }
                else if (op.Item1 == TableOperationType.Replace || op.Item1 == TableOperationType.Merge || op.Item1 == TableOperationType.Delete)
                {
                    if (rowRes.HttpStatusCode == (int)HttpStatusCode.NotFound)
                        throw GenerateBatchException(HttpStatusCode.NotFound, i);
                    else if (rowRes.HttpStatusCode == (int)HttpStatusCode.OK && rowRes.Result != null)
                    {
                        var row = (STableEntity)rowRes.Result;

                        if (row.Deleted)
                            throw GenerateBatchException(HttpStatusCode.NotFound, i);

                        if (!VETagMatch(op.Item2.ETag, row.VETag))
                            throw GenerateBatchException(HttpStatusCode.PreconditionFailed, i);

                        int? cowTarget = FindBatchCOWTargetSSID(row, meta);
                        if (cowTarget.HasValue)
                        {
                            // set row.ssid to indicate where to cow
                            row.SSID = cowTarget.Value;
                            cowRows.Add(new Tuple<STableEntity, int>(row, i));
                        }

                        STableEntity arg = Encapsulate(op.Item2);
                        arg.SSID = meta.CurrentSSID;
                        arg.Version = row.Version + 1;
                        newVETag = arg.VETag;

                        // Change delete to replace
                        TableOperationType targetOpType = op.Item1;
                        if (op.Item1 == TableOperationType.Delete)
                        {
                            targetOpType = TableOperationType.Replace;
                            arg.Deleted = true;
                        }

                        writeOps.Add(new Tuple<TableOperationType, ITableEntity, string>(targetOpType, arg, row.ETag));
                    }
                    else
                        throw GenerateBatchException((HttpStatusCode)rowRes.HttpStatusCode, i);

                    replies.Add(new TableResult() { Result = op.Item2, HttpStatusCode = (int)HttpStatusCode.NoContent, Etag = newVETag });
                }
                else if (op.Item1 == TableOperationType.InsertOrReplace || op.Item1 == TableOperationType.InsertOrMerge)
                {
                    if (rowRes.HttpStatusCode == (int)HttpStatusCode.NotFound)
                    {
                        STableEntity arg = Encapsulate(op.Item2);
                        arg.SSID = meta.CurrentSSID;
                        arg.Version = STableEntity.InitialVersion;
                        newVETag = arg.VETag;
                        writeOps.Add(new Tuple<TableOperationType, ITableEntity, string>(TableOperationType.Insert, arg, null));
                    }
                    else if (rowRes.HttpStatusCode == (int)HttpStatusCode.OK && rowRes.Result != null)
                    {
                        var row = (STableEntity)rowRes.Result;

                        int? cowTarget = FindBatchCOWTargetSSID(row, meta);
                        if (cowTarget.HasValue)
                        {
                            // set row.ssid to indicate where to cow
                            row.SSID = cowTarget.Value;
                            cowRows.Add(new Tuple<STableEntity, int>(row, i));
                        }

                        STableEntity arg = Encapsulate(op.Item2);
                        arg.SSID = meta.CurrentSSID;
                        arg.Version = row.Version + 1;
                        newVETag = arg.VETag;

                        if (row.Deleted)
                        {
                            // always replace in such case
                            writeOps.Add(new Tuple<TableOperationType, ITableEntity, string>(TableOperationType.Replace, arg, row.ETag));
                        }
                        else
                        {
                            // issue replace / merge
                            var targetOpType = (op.Item1 == TableOperationType.InsertOrReplace) ? TableOperationType.Replace : TableOperationType.Merge;
                            writeOps.Add(new Tuple<TableOperationType, ITableEntity, string>(targetOpType, arg, row.ETag));
                        }
                    }
                    else
                        throw GenerateBatchException((HttpStatusCode)rowRes.HttpStatusCode, i);

                    replies.Add(new TableResult() { Result = op.Item2, HttpStatusCode = (int)HttpStatusCode.NoContent, Etag = newVETag });
                }
                else
                    // impossible
                    Assert.IsTrue(false);
            }
        }

        // Return if we need to COW a row in batch operation.
        private int? FindBatchCOWTargetSSID(STableEntity row, STableMetadataEntity meta)
        {
            Assert.IsTrue(row.SSID <= meta.CurrentSSID);

            int cowTarget = row.SSID;
            if (cowTarget == meta.CurrentSSID)
                // no need for COW
                return null;

            // logically need to COW to snapshot row.SSID, check if it still exists...
            if (meta.IsSnapshotValid(cowTarget))
                return cowTarget;
            else
            {
                int? nextSnapshot = meta.NextSnapshot(cowTarget);
                if (nextSnapshot.HasValue)
                {
                    Assert.IsTrue(nextSnapshot.Value < meta.CurrentSSID);
                    return nextSnapshot.Value;
                }
                else
                {
                    // we are sure there is no valid snapshot from row.SSID to currentSSID - 1
                    // as we are just writing currentSSID, we do not need to COW
                    return null;
                }
            }
        }

        private void ExecuteBatchCOW(List<Tuple<STableEntity, int>> cowRows)
        {
            var cowBySSIDs =
                from row in cowRows
                group row by row.Item1.SSID into g
                select new Tuple<int, List<Tuple<STableEntity, int>>>(g.Key,
                    g.Aggregate(new List<Tuple<STableEntity, int>>(), (b, row) => {b.Add(row); return b;}));

            foreach (var eachSSID in cowBySSIDs)
            {
                var tab = eachSSID.Item1;
                var rows = eachSSID.Item2;
                if (rows.Count == 1)
                {
                    // batch with single op, issue directly
                    var response = CopyOnWrite(rows[0].Item1);
                    if (response != HttpStatusCode.Created)
                        // failed
                        throw GenerateBatchException(response, rows[0].Item2);
                }
                else
                {
                    // first try to issue a batch
                    var batch = new List<Tuple<TableOperationType, ITableEntity, string>>();
                    foreach (var row in rows)
                        batch.Add(new Tuple<TableOperationType, ITableEntity, string>(TableOperationType.Insert, row.Item1, null));


                    IList<TableResult> batchRes = null;
                    var errorIndex = StorageLayer.BatchWrite(ts.GetSnapshot(tab), batch, out batchRes);
                    
                    if (errorIndex < 0)
                    {
                        // check each result
                        Assert.IsTrue(batchRes != null && batchRes.Count == rows.Count);
                        foreach (var res in batchRes)
                        {
                            Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.Created);
                        }
                    }
                    else
                    {
                        // if we encounter any exception, do the cow one by one
                        foreach (var row in rows)
                        {
                            var response = CopyOnWrite(row.Item1);

                            if (response != HttpStatusCode.Created)
                                // failed
                                throw GenerateBatchException(response, row.Item2);
                        }
                    }
                }
            }
        }

        private void ExecuteBatchWrite(List<Tuple<TableOperationType, ITableEntity, string>> ops, TableRequestOptions options, OperationContext context)
        {
            IList<TableResult> results = null;

            var errorIndex = StorageLayer.BatchWrite(ts.GetHead(), ops, out results, options, context);
            if (errorIndex < 0)
            {
                Assert.IsTrue(results != null && results.Count == ops.Count);
                for (int i = 0; i < ops.Count; ++i)
                {
                    if (ops[i].Item1 == TableOperationType.Insert)
                        Assert.IsTrue(results[i].HttpStatusCode == (int)HttpStatusCode.Created);
                    else if (ops[i].Item1 == TableOperationType.Replace || ops[i].Item1 == TableOperationType.Merge)
                        Assert.IsTrue(results[i].HttpStatusCode == (int)HttpStatusCode.NoContent);
                    else
                        // impossible
                        Assert.IsTrue(false);
                }
            }
            else
            {
                Assert.IsTrue(results != null && results.Count == 1);
                throw GenerateBatchException((HttpStatusCode)results[0].HttpStatusCode, errorIndex);
            }
        }



        // Query API

        private IEnumerable<TElement> HandleExecuteQuery<TElement>(TableQuery<TElement> q) where TElement : ITableEntity, new()
        {
            TableQuery<STableEntity> encapedQuery = new TableQuery<STableEntity>();
            encapedQuery.SelectColumns = q.SelectColumns;
            if (encapedQuery.SelectColumns != null)
            {
                // should at least contain STable's metadata
                encapedQuery.SelectColumns.Add(STableEntity.KeySSID);
                encapedQuery.SelectColumns.Add(STableEntity.KeyVersion);
                encapedQuery.SelectColumns.Add(STableEntity.KeyDeleted);
                encapedQuery.SelectColumns.Add(STableEntity.KeyVETag);
            }

            encapedQuery.TakeCount = q.TakeCount;

            // set filter, need to combine a filter on delete bit
            var deleteFilter = TableQuery.GenerateFilterConditionForBool(STableEntity.KeyDeleted, QueryComparisons.Equal, false);
            if (q.FilterString != null && !q.FilterString.Equals(""))
            {
                var combinedFilter = TableQuery.CombineFilters(deleteFilter, TableOperators.And, q.FilterString);
                encapedQuery.Where(combinedFilter);
            }
            else
                encapedQuery.Where(deleteFilter);

            var headTable = ts.GetHead();
            var rawres = headTable.ExecuteQuery(encapedQuery);

            // use a generated retrieve operation for decapsulation
            // keys are useless, Decapsulate() only uses its default resolver
            var retrieveOp = TableOperation.Retrieve<TElement>("1", "1");
            var res = from rawEnt in rawres
                      select (TElement)Decapsulate(rawEnt, retrieveOp);

            return res;
        }



        // Snapshot management

        private STableHistoryInfo HandleGetSTableHistory()
        {
            // First, retrieve the meta data
            var headTable = ts.GetHead();
            var metaResult = StorageLayer.RetrieveRow(headTable, STableMetadataEntity.RetrieveMetaOp());
            if (metaResult == null || metaResult.HttpStatusCode != (int)HttpStatusCode.OK || metaResult.Result == null)
                throw GenerateException(HttpStatusCode.ServiceUnavailable);
            var meta = (STableMetadataEntity)metaResult.Result;

            var res = new STableHistoryInfo();
            res.HeadSSID = meta.CurrentSSID;
            for (int i = 0; i <= res.HeadSSID; ++i)
            {
                var state = SnapshotState.Deleted;
                if (i == res.HeadSSID)
                    state = SnapshotState.Head;
                else if (meta.IsSnapshotValid(i))
                    state = SnapshotState.Valid;

                res.Snapshots.Add(new SnapshotInfo(i, state, meta.GetParent(i)));
            }

            return res;
        }

        // Create a new snapshot, return the new metadata block after creating the snapshot
        // argument: parentOfNewHead, the parent ssid of the new (unsealed) head version
        // -1: the parent of the new head is the current version (i.e., parentOfNewHead == CurrentSSID)
        // CurrentSSID > parentOfNewHead >= 0: the ssid of the parent of the new head version
        private STableMetadataEntity HandleCreateSnapshot(int parentOfNewHead)
        {
            // First, retrieve the meta data
            var headTable = ts.GetHead();
            var metaResult = StorageLayer.RetrieveRow(headTable, STableMetadataEntity.RetrieveMetaOp());
            if (metaResult == null || metaResult.HttpStatusCode != (int)HttpStatusCode.OK || metaResult.Result == null)
                throw GenerateException(HttpStatusCode.ServiceUnavailable);
            var meta = (STableMetadataEntity)metaResult.Result;

            // Create snapshot table for CurrentSSID
            ts.GetSnapshot(meta.CurrentSSID).CreateIfNotExists();

            // translate parent of new head
            Assert.IsTrue(parentOfNewHead == -1 || parentOfNewHead >= 0);
            Assert.IsTrue(parentOfNewHead < meta.CurrentSSID);
            if (parentOfNewHead == -1)
                parentOfNewHead = meta.CurrentSSID;

            // Create snapshot in meta block
            meta.CreateSnapshot(parentOfNewHead);

            var updateRes = StorageLayer.ModifyRow(headTable, TableOperationType.Replace, meta, meta.ETag);
            if (updateRes == null)
                throw GenerateException(HttpStatusCode.ServiceUnavailable);
            else if (updateRes.HttpStatusCode == (int)HttpStatusCode.Conflict || updateRes.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
                throw GenerateException(HttpStatusCode.Conflict);
            else if (updateRes.HttpStatusCode != (int)HttpStatusCode.NoContent)
                throw GenerateException((HttpStatusCode)updateRes.HttpStatusCode);

            return meta;
        }

        private IList<int> HandleListValidSnapshots()
        {
            // First, retrieve the meta data
            var headTable = ts.GetHead();
            var metaResult = StorageLayer.RetrieveRow(headTable, STableMetadataEntity.RetrieveMetaOp());
            if (metaResult == null || metaResult.HttpStatusCode != (int)HttpStatusCode.OK || metaResult.Result == null)
                throw GenerateException(HttpStatusCode.ServiceUnavailable);
            var meta = (STableMetadataEntity)metaResult.Result;

            var e = meta.GetSSIDEnumerator();
            List<int> res = new List<int>();

            while (e.hasMoreSSID())
                res.Add(e.getSSIDAndMovePrev());

            return res;
        }

        // Assume no concurrent operations
        private int HandleRollback(int ssid)
        {
            // get metadata
            var headTable = ts.GetHead();
            var metaResult = StorageLayer.RetrieveRow(headTable, STableMetadataEntity.RetrieveMetaOp());
            if (metaResult == null || metaResult.HttpStatusCode != (int)HttpStatusCode.OK || metaResult.Result == null)
                throw GenerateException(HttpStatusCode.ServiceUnavailable);
            var meta = (STableMetadataEntity)metaResult.Result;

            if (!meta.IsSnapshotValid(ssid))
                // cannnot rollback to a non-existing snapshot
                throw GenerateException(HttpStatusCode.BadRequest);

            // Create a new snapshot
            meta = HandleCreateSnapshot(ssid);
            var saveSSID = meta.CurrentSSID - 1;

            var query = new TableQuery<STableEntity>().Where(TableQuery.GenerateFilterConditionForInt(STableEntity.KeySSID, QueryComparisons.GreaterThan, ssid));
            var res = headTable.ExecuteQuery<STableEntity>(query);
            foreach (var row in res)
            {
                int steps = 0;
                var oldVal = ReadSnapshotChain(row.PartitionKey, row.RowKey, ssid, meta, out steps, null, null);
                Assert.IsTrue(oldVal != null && (oldVal.HttpStatusCode == (int)HttpStatusCode.NotFound || oldVal.HttpStatusCode == (int)HttpStatusCode.OK));
                STableEntity oldData = null;
                if (oldVal.HttpStatusCode == (int)HttpStatusCode.NotFound)
                {
                    oldData = new STableEntity(row.PartitionKey, row.RowKey);
                    oldData.Deleted = true;
                }
                else
                {
                    Assert.IsTrue(oldVal.Result != null);
                    oldData = (STableEntity)oldVal.Result;
                    // restore old delete bit, vetag, and payload
                }

                // copy old data to head table, but with new ssid and version number, throw any exception
                ModifyRowCOW(row, TableOperationType.Replace, oldData, meta, null, null);
            }

            return saveSSID;
        }

        // Assume no concurrent create snapshot / rollback / delete snapshot / readFromSnapshot
        private void HandleDeleteSnapshot(int ssid)
        {
            // get metadata
            var headTable = ts.GetHead();
            var metaResult = StorageLayer.RetrieveRow(headTable, STableMetadataEntity.RetrieveMetaOp());
            if (metaResult == null || metaResult.HttpStatusCode != (int)HttpStatusCode.OK || metaResult.Result == null)
                throw GenerateException(HttpStatusCode.ServiceUnavailable);
            var meta = (STableMetadataEntity)metaResult.Result;

            if (!meta.IsSnapshotValid(ssid))
                throw GenerateException(HttpStatusCode.BadRequest);
                
            IChainTable targetSS = ts.GetSnapshot(ssid);
            var adoptor = meta.NextSnapshot(ssid);
            IChainTable adoptorSS = null;
            if (adoptor.HasValue)
                adoptorSS = ts.GetSnapshot(adoptor.Value);

            if (adoptorSS != null)
            {
                var query = new TableQuery<STableEntity>();
                var res = targetSS.ExecuteQuery<STableEntity>(query);

                foreach (var row in res)
                {
                    row.SSID = adoptor.Value;

                    // simply do an insert, we accept Created / Conflict
                    // (NOTICE: IN SUCH CASE, WE REQUIRE CONFLICT TO ONLY MEAN "THE ROW EXISTS", WHICH MAY NOT BE THE CASE FOR RTABLE / STABLE)
                    var adoptRes = StorageLayer.ModifyRow(adoptorSS, TableOperationType.Insert, row, null);
                    if (adoptRes == null || (adoptRes.HttpStatusCode != (int)HttpStatusCode.Created && adoptRes.HttpStatusCode != (int)HttpStatusCode.Conflict))
                        throw GenerateException(HttpStatusCode.ServiceUnavailable);
                }
            }


            // delete ssid in meta block
            meta.DeleteSnapshot(ssid);
            var updateMetaRes = StorageLayer.ModifyRow(headTable, TableOperationType.Replace, meta, meta.ETag);
            if (updateMetaRes == null)
                throw GenerateException(HttpStatusCode.ServiceUnavailable);
            else if (updateMetaRes.HttpStatusCode != (int)HttpStatusCode.NoContent)
                throw GenerateException((HttpStatusCode)updateMetaRes.HttpStatusCode);

            // finally, delete the target table, do not care succeess or not...
            targetSS.DeleteIfExists();
        }



        // Garbage collection of deleted rows
        private static readonly int GCBatchSize = 20;

        private void HandleGC()
        {
            // get metadata
            var headTable = ts.GetHead();
            var metaResult = StorageLayer.RetrieveRow(headTable, STableMetadataEntity.RetrieveMetaOp());
            if (metaResult == null || metaResult.HttpStatusCode != (int)HttpStatusCode.OK || metaResult.Result == null)
                throw GenerateException(HttpStatusCode.ServiceUnavailable);
            var meta = (STableMetadataEntity)metaResult.Result;

            var targetCols = new List<string>();

            var query = new TableQuery<DynamicTableEntity>().Where(TableQuery.GenerateFilterConditionForBool(STableEntity.KeyDeleted, QueryComparisons.Equal, true))
                .Select(targetCols);
            var res = headTable.ExecuteQuery<DynamicTableEntity>(query);
            
            var deletedRows = new List<Tuple<string, string>>();
            foreach (var row in res)
                deletedRows.Add(new Tuple<string, string>(row.PartitionKey, row.RowKey));
            
            try
            {
                GCDeletedRows(deletedRows, meta);
            }
            catch (StorageException)
            {
                // ignore any exception
            }
        }

        private void GCDeletedRows(List<Tuple<string, string>> deletedRows, STableMetadataEntity meta)
        {
            List<bool> canDelete = new List<bool>();
            for (int i = 0; i < deletedRows.Count; ++i)
                canDelete.Add(true);

            var rowIdAndPETagToDeleteInEachTable = new List<List<Tuple<int, string>>>();

            var table = ts.GetHead();
            rowIdAndPETagToDeleteInEachTable.Add(new List<Tuple<int, string>>());
            GCCheckTable(table, deletedRows, canDelete, rowIdAndPETagToDeleteInEachTable[0]);

            int tableId = 0;
            var ssids = meta.GetSSIDEnumerator();
            while (ssids.hasMoreSSID())
            {
                var ssid = ssids.getSSIDAndMovePrev();
                table = ts.GetSnapshot(ssid);

                ++tableId;
                rowIdAndPETagToDeleteInEachTable.Add(new List<Tuple<int, string>>());

                GCCheckTable(table, deletedRows, canDelete, rowIdAndPETagToDeleteInEachTable[tableId]);
            }
            ExecuteGC(deletedRows, canDelete, rowIdAndPETagToDeleteInEachTable, meta);
        }

        private void GCCheckTable(IChainTable table, List<Tuple<string, string>> rows, List<bool> canDelete,
            List<Tuple<int, string>> rowIdAndPETagToDeleteInThisTable)
        {
            List<int> index = new List<int>();
            for (int i = 0; i < rows.Count; ++i)
            {
                if (canDelete[i])
                    index.Add(i);

                if (index.Count == GCBatchSize)
                {
                    GCCheckRows(table, rows, index, canDelete, rowIdAndPETagToDeleteInThisTable);
                    index.Clear();
                }
            }

            if (index.Count > 0)
                GCCheckRows(table, rows, index, canDelete, rowIdAndPETagToDeleteInThisTable);
        }

        private void GCCheckRows(IChainTable table, List<Tuple<string, string>> rows, List<int> index, List<bool> canDelete,
            List<Tuple<int, string>> rowIdAndPETagToDeleteInThisTable)
        {
            if (index.Count == 0)
                return;

            List<InternalOperation> readOps = new List<InternalOperation>();
            for (int i = 0; i < index.Count; ++i)
                readOps.Add(new InternalOperation(table, TableOperation.Retrieve<STableEntity>(rows[index[i]].Item1, rows[index[i]].Item2)));

            var res = StorageLayer.ConcurrentRetrieve(readOps);
            if (res == null)
            {
                // read fail, mark these rows as "cannot delete"
                for (int i = 0; i < index.Count; ++i)
                    canDelete[index[i]] = false;
                return;
            }

            Assert.IsTrue(res.Count == index.Count);

            for (int i = 0; i < index.Count; ++i)
            {
                if (res[i] == null)
                    canDelete[index[i]] = false;
                else if (res[i].HttpStatusCode == (int)HttpStatusCode.NotFound)
                {
                    // keep mark unchanged
                }
                else if (res[i].HttpStatusCode == (int)HttpStatusCode.OK && res[i].Result != null)
                {
                    var entity = (STableEntity)res[i].Result;
                    if (!entity.Deleted)
                        canDelete[index[i]] = false;
                    else
                    {
                        // keep mark unchanged, need to delete this row in this table if this row eventually needs to be GCed
                        rowIdAndPETagToDeleteInThisTable.Add(new Tuple<int, string>(index[i], res[i].Etag));
                    }
                }
                else
                {
                    // for all unknown cases, mark the row as "cannot delete"
                    canDelete[index[i]] = false;
                }
            }
        }

        private void ExecuteGC(List<Tuple<string, string>> rows, List<bool> canDelete, List<List<Tuple<int, string>>> rowIdAndPETagToDelete,
            STableMetadataEntity meta)
        {
            var table = ts.GetHead();
            ExecuteGCTable(table, rows, canDelete, rowIdAndPETagToDelete[0]);

            int tableId = 0;
            var ssids = meta.GetSSIDEnumerator();
            while (ssids.hasMoreSSID())
            {
                var ssid = ssids.getSSIDAndMovePrev();
                table = ts.GetSnapshot(ssid);
                ++tableId;
                ExecuteGCTable(table, rows, canDelete, rowIdAndPETagToDelete[tableId]);
            }
        }

        private void ExecuteGCTable(IChainTable table, List<Tuple<string, string>> rows, List<bool> canDelete,
            List<Tuple<int, string>> rowIdAndPETagToDelete)  // Item1 == row id, Item2 == petag
        {
            var partitionedRows = from rowIdAndPETag in rowIdAndPETagToDelete
                                  where canDelete[rowIdAndPETag.Item1]
                                  group rowIdAndPETag by rows[rowIdAndPETag.Item1].Item1 into g
                                  select g.Aggregate(new List<Tuple<string, string, string>>(), (res, idAndPETag) => 
                                  { res.Add(new Tuple<string, string, string>   // item1 = pkey, item2 = rkey, item3 = petag
                                      (rows[idAndPETag.Item1].Item1, rows[idAndPETag.Item1].Item2, idAndPETag.Item2)); return res; });

            foreach (var p in partitionedRows)
            {
                var ops = new List<Tuple<TableOperationType,ITableEntity,string>>();
                foreach (var r in p)
                    ops.Add(new Tuple<TableOperationType,ITableEntity,string>(TableOperationType.Delete, new STableEntity(r.Item1, r.Item2), r.Item3));

                IList<TableResult> res = null;
                var errorIndex = StorageLayer.BatchWrite(table, ops, out res);

                if (errorIndex >= 0)
                {
                    // delete one by one, ignore any future error
                    foreach (var r in p)
                    {
                        StorageLayer.ModifyRow(table, TableOperationType.Delete, new STableEntity(r.Item1, r.Item2), r.Item3);
                    }
                }
            }
        }



        // helpers
        private TableOperationType GetOpType(TableOperation operation)
        {

            // WARNING: We use reflection to read an internal field in OperationType.
            //          We have a dependency on TableOperation fields in WindowsAzureStorage dll
            PropertyInfo opType = operation.GetType().GetProperty("OperationType", System.Reflection.BindingFlags.GetProperty |
                                                               System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public);
            TableOperationType opTypeValue = (TableOperationType)(opType.GetValue(operation, null));

            return opTypeValue;
        }

        private ITableEntity GetEntityFromOperation(TableOperation operation)
        {

            // WARNING: We use reflection to read an internal field in OperationType.
            //          We have a dependency on TableOperation fields WindowsAzureStorage dll
            PropertyInfo entity = operation.GetType().GetProperty("Entity", System.Reflection.BindingFlags.GetProperty |
                                                                   System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public);
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


        // Generate a operation Id
        private string GenerateVETag()
        {
            var low = random.Next();
            var high = random.Next();
            return low.ToString() + "," + high.ToString();
        }

        // Check if a vetag (can't be *) matches the requested pattern (can be *)
        private bool VETagMatch(string pattern, string vetag)
        {
            return (pattern.Equals("*") || pattern.Equals(vetag));
        }


        // Encapsulate an ITableEntity to STable's entity with given payload, generate a random vetag
        // Protocol fields (i.e., ssid & version) are left unset by this method
        private STableEntity Encapsulate(ITableEntity userEntity)
        {
            var res = new STableEntity(userEntity.PartitionKey, userEntity.RowKey, userEntity.Timestamp, userEntity.ETag);
            res.Deleted = false;
            res.VETag = GenerateVETag();
            res.Payload = userEntity.WriteEntity(null);

            return res;
        }

        // Decapsulate a STable entity back to a given ITableEntity
        // This will also virtaulize the etag
        private ITableEntity Decapsulate(STableEntity rawData, TableOperation retrieveOp)
        {
            var resolverField = retrieveOp.GetType().GetField("retrieveResolver", BindingFlags.Instance | BindingFlags.NonPublic);
            var resolver = (Func<string, string, DateTimeOffset, IDictionary<string, EntityProperty>, System.String, System.Object>)
                resolverField.GetValue(retrieveOp);

            var res = (ITableEntity)resolver.Invoke(rawData.PartitionKey, rawData.RowKey, rawData.Timestamp, rawData.Payload, rawData.VETag);

            return res;
        }

        private StorageException GenerateException(HttpStatusCode cause)
        {
            RequestResult res = new RequestResult();
            res.HttpStatusCode = (int)cause;
            return new StorageException(res, "STable exception", null);
        }

        private ChainTableBatchException GenerateBatchException(HttpStatusCode cause, int opId)
        {
            var underlyingEx = GenerateException(cause);
            throw new ChainTableBatchException(opId, underlyingEx);
        }
    }
}
