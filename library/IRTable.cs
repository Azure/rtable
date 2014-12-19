using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.RTable.ConfigurationAgentLibrary;

namespace Microsoft.WindowsAzure.Storage.RTable
{
    public interface IRTable
    {
        string tableName { get; }
        bool CreateIfNotExists(TableRequestOptions requestOptions = null, OperationContext operationContext = null);
        bool Exists();
        bool DeleteIfExists();
        TableResult Execute(TableOperation operation, TableRequestOptions requestOptions = null, OperationContext operationContext = null);
        IList<TableResult> CheckRetrieveInBatch(TableBatchOperation batch, TableRequestOptions requestOptions = null, OperationContext operationContext = null);
        IList<TableResult> ExecuteBatch(TableBatchOperation batch, TableRequestOptions requestOptions = null, OperationContext operationContext = null);
        TableResult Retrieve(TableOperation operation, TableRequestOptions requestOptions = null, OperationContext operationContext = null);
        TableResult Delete(TableOperation operation, TableRequestOptions requestOptions = null, OperationContext operationContext = null);
        TableResult Merge(TableOperation operation, TableResult retrievedResult, TableRequestOptions requestOptions = null, OperationContext operationContext = null);
        TableResult InsertOrMerge(TableOperation operation, TableRequestOptions requestOptions = null, OperationContext operationContext = null);
        TableResult Replace(TableOperation operation, TableResult retrievedResult, TableRequestOptions requestOptions = null, OperationContext operationContext = null);
        TableResult Insert(TableOperation operation, TableResult retrievedResult, TableRequestOptions requestOptions = null, OperationContext operationContext = null);
        TableResult InsertOrReplace(TableOperation operation, TableRequestOptions requestOptions = null, OperationContext operationContext = null);
        TableResult FlushAndRetrieve(IRTableEntity row, TableRequestOptions requestOptions = null, OperationContext operationContext = null, bool virtualizeEtag = true);
        IEnumerable<TElement> ExecuteQuery<TElement>(TableQuery<TElement> query, TableRequestOptions requestOptions = null, OperationContext operationContext = null) where TElement : ITableEntity, new();
        TableResult RepairRow(string partitionKey, string rowKey, IRTableEntity existingRow);
    }
}
