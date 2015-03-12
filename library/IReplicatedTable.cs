// azure-rtable ver. 0.9
//
// Copyright (c) Microsoft Corporation
//
// All rights reserved. 
//
// MIT License
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files 
// (the ""Software""), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, 
// merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished 
// to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF 
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE 
// FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION 
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

namespace Microsoft.Azure.Toolkit.Replication
{
    using System.Collections.Generic;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;

    public interface IReplicatedTable
    {
        string TableName { get; }
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
        TableResult FlushAndRetrieve(IReplicatedTableEntity row, TableRequestOptions requestOptions = null, OperationContext operationContext = null, bool virtualizeEtag = true);
        IEnumerable<TElement> ExecuteQuery<TElement>(TableQuery<TElement> query, TableRequestOptions requestOptions = null, OperationContext operationContext = null) where TElement : ITableEntity, new();
        TableQuery<TElement> CreateQuery<TElement>()
            where TElement : ITableEntity, new();
        TableResult RepairRow(string partitionKey, string rowKey, IReplicatedTableEntity existingRow);
    }
}
