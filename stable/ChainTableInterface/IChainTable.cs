using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.ChainTableInterface
{
    // Any storage implementation that implements IChainTable can be used in TxTable
    public interface IChainTable
    {
        /*
         * Execute API:
         *   (1) Retrieve
         *          On success, return TableResult, code = 200 (OK);
         *          If the row does not exist, return TableResult, code = 404 (NotFound);
         *          Otherwise, throw storage exception with corresponding http error code.
         *   (2) Insert
         *          On success, return TableResult, code = 201 (Created);
         *          If the row already exists, throw storage exception, code = 409 (Conflict);
         *          Otherwise, throw storage exception with corresponding http error code.
         *   (3) Replace / Merge / Delete
         *          On success, return TableResult, code = 204 (NoContent);
         *          If the row does not exist, throw storage exception, code = 404 (NotFound);
         *          If ETag mismatches, throw storage exception, code = 412 (Precondition);
         *          Otherwise, throw storage exception with corresponding http error code.
         *   (4) InsertOrReplace / InsertOrMerge
         *          On success, return TableResult, code = 204 (NoContent);
         *          Otherwise, throw storage exception with corresponding http error code.
         */         
        TableResult Execute(TableOperation operation,
            TableRequestOptions requestOptions = null, OperationContext operationContext = null);

        /*
         * Batch Execute API:
         *   Caller should not put retrieve operations inside a batch, so any error will lead to
         *   an Exception.
         *   
         *   On success, return a list of TableResult. Each TableResult should follow the API
         *   defined in Execute operation.
         *   
         *   Otherwise, throw a ChainTableBatchException, with failedOpIndex marks to the first
         *   operation that causes the failure, and storageEx being the corresponding exception
         *   defined in the Execute API.
         */
        IList<TableResult> ExecuteBatch(TableBatchOperation batch,
            TableRequestOptions requestOptions = null, OperationContext operationContext = null);

        Task<TableResult> ExecuteAsync(TableOperation operation,
            TableRequestOptions requestOptions = null, OperationContext operationContext = null);


        /*
         * Query interface:
         */
        IEnumerable<TElement> ExecuteQuery<TElement>(TableQuery<TElement> q) where TElement : ITableEntity, new();

        string GetTableID();

        bool CreateIfNotExists();

        bool DeleteIfExists();

        // Should add ExecuteQuery Interface?
    }
}
