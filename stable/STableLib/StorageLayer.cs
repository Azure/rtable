using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Core.Util;
using System.Net;
using System.Diagnostics;
using Microsoft.WindowsAzure.Storage.ChainTableInterface;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.WindowsAzure.Storage.STable
{

    class StorageLayer
    {
        // Single-op API: execute a single operation on an underlying table
        /*
         * RetrieveRow
         *   Read a row.
         * 
         * param:
         *   A retrieve operation
         *   (Currently do not support partitionID + rowID, since Operation has another important information, i.e., deserializer)
         * 
         * return:
         *   query result.
         */

        internal static TableResult RetrieveRow(IChainTable table, TableOperation operation, TableRequestOptions requestOptions = null,
            OperationContext operationContext = null)
        {
            TableResult retrievedResult = null;
            try
            {
                retrievedResult = table.Execute(operation, requestOptions, operationContext);
            }
            catch (StorageException e)
            {
                return new TableResult() { Result = null, Etag = e.RequestInformation.Etag, HttpStatusCode = e.RequestInformation.HttpStatusCode };
            }
            catch (Exception e)
            {
                Console.WriteLine("Error in Retrieve: caught exception {0}", e);
                return null;
            }

            return retrievedResult;
        }

        internal static TableResult RetrieveRowThrow(IChainTable table, TableOperation operation, TableRequestOptions requestOptions = null,
            OperationContext operationContext = null)
        {
            return table.Execute(operation, requestOptions, operationContext); 
        }

        /*
         * ReadModifyWriteRow
         *   Execute a conditional write operation on a row.
         *   
         *   Insert         - Success if currently no row exists on the key.
         *   Update & Merge - success only if current row exists and etag matches.
         *   Delete         - Success only if current row exist, and etag matches.
         * 
         * param:
         *   table  - target table.
         *   opType - operation to execute, Insert / Replace / Merge / Delete.
         *   row    - target row to insert / delete, also serves as the parameter used for insert / replace / merge.
         *   etag   - for replace / merge / delete, provide the etag of target row.
         *   
         * return:
         *   The result of query. Return null if the Azure request throws an unknown exception.
         */
        internal static TableResult ModifyRow(IChainTable table, TableOperationType opType, ITableEntity row, string eTag,
            TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            Assert.IsTrue(opType == TableOperationType.Insert || eTag != null);
            
            if (eTag != null)
                row.ETag = eTag;

            return WriteConditionalRow(table, opType, row, requestOptions, operationContext);
        }

        // guarantee to return a list of same size
        internal static List<TableResult> ConcurrentRetrieve(List<InternalOperation> ops)
        {
            var tasks = new List<Task<TableResult>>();
            foreach (var op in ops)
            {
                tasks.Add(op.target.ExecuteAsync(op.op, op.options, op.context));
            }

            var res = new List<TableResult>();
            try
            {
                foreach (var task in tasks)
                {
                    task.Wait();
                    res.Add(task.Result);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
                return null;
            }

            return res;
        }

        // execute batch write on a same partition of a same underlying table
        // on succeed, res is filled with exact number of table results, and return -1
        // otherwise, res only contain a single table result of error, and return the error index
        internal static int BatchWrite(IChainTable table, List<Tuple<TableOperationType, ITableEntity, string>> ops, out IList<TableResult> res,
            TableRequestOptions options = null, OperationContext context = null)
        {
            TableBatchOperation batch = new TableBatchOperation();
            foreach (var op in ops)
            {
                Assert.IsTrue(op.Item1 == TableOperationType.Insert || op.Item3 != null);
                if (op.Item3 != null)
                    op.Item2.ETag = op.Item3;
                batch.Add(TranslateWriteOp(op.Item1, op.Item2));
            }

            try
            {
                res = table.ExecuteBatch(batch);
                return -1;
            }
            catch (ChainTableBatchException e)
            {
                res = new List<TableResult>();
                res.Add(new TableResult() { Result = null, Etag = e.RequestInformation.Etag, HttpStatusCode = e.RequestInformation.HttpStatusCode });
                return e.FailedOpIndex;
            }
        }




        // Helper functions for single-op API
        private static TableOperation TranslateWriteOp(TableOperationType opType, ITableEntity row)
        {
            TableOperation top = null;
            if (opType == TableOperationType.Delete)
                top = TableOperation.Delete(row);
            else if (opType == TableOperationType.Merge)
                top = TableOperation.Merge(row);
            else if (opType == TableOperationType.Replace)
                top = TableOperation.Replace(row);
            else if (opType == TableOperationType.Insert)
                top = TableOperation.Insert(row, true);
            else
                throw new Exception("Unknown write operation.");

            return top;
        }

        private static TableResult WriteConditionalRow(IChainTable table, TableOperationType opType, ITableEntity row,
            TableRequestOptions options = null, OperationContext oc = null)
        {
            try
            {
                TableOperation top = TranslateWriteOp(opType, row);
                return table.Execute(top, options, oc);
            }
            catch (StorageException e)
            {
                return new TableResult() { Result = null, Etag = e.RequestInformation.Etag, HttpStatusCode = e.RequestInformation.HttpStatusCode };
            }
            catch (Exception e)
            {
                Console.WriteLine("TryWriteConditionalRow:Error: exception {0}", e);
                return null;
            }
        }
    }

    class InternalOperation
    {
        internal InternalOperation(IChainTable target, TableOperation op, TableRequestOptions options = null, OperationContext context = null)
        {
            this.target = target;
            this.op = op;
            this.options = options;
            this.context = context;
        }

        internal IChainTable target;
        internal TableOperation op;
        internal TableRequestOptions options;
        internal OperationContext context;
    }
 
}
