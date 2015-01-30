//-----------------------------------------------------------------------
// <copyright file="CommandLineArgument.cs" company="Microsoft">
//    Copyright 2013 Microsoft Corporation
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//      http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// </copyright>
//-

namespace Microsoft.WindowsAzure.Storage.RTable
{
    using System.Collections.Generic;
    using Microsoft.WindowsAzure.Storage.Table;

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
