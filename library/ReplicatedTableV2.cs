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
    using System;
    using System.Collections.Generic;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;

    public class ReplicatedTableV2 : IReplicatedTable
    {
        private readonly ReplicatedTableConfigurationV2Wrapper _configurationWrapper;
        private ReplicatedTable _replicatedTableInstance;

        public string TableName
        {
            get; private set;
        }

        public ReplicatedTableV2(string name, ReplicatedTableConfigurationServiceV2 replicatedTableConfigurationAgent)
        {
            this._configurationWrapper = new ReplicatedTableConfigurationV2Wrapper(name, replicatedTableConfigurationAgent);
            TableName = name;

            _replicatedTableInstance = new ReplicatedTable(name, replicatedTableConfigurationAgent);
        }

        public bool CreateIfNotExists(TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            throw new NotImplementedException();
        }

        public bool Exists()
        {
            throw new NotImplementedException();
        }

        public bool DeleteIfExists()
        {
            throw new NotImplementedException();
        }

        public TableResult Execute(TableOperation operation, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            throw new NotImplementedException();
        }

        public IList<TableResult> CheckRetrieveInBatch(TableBatchOperation batch, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            throw new NotImplementedException();
        }

        public IList<TableResult> ExecuteBatch(TableBatchOperation batch, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            throw new NotImplementedException();
        }

        public TableResult Retrieve(TableOperation operation, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            throw new NotImplementedException();
        }

        public TableResult Delete(TableOperation operation, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            throw new NotImplementedException();
        }

        public TableResult Merge(TableOperation operation, TableResult retrievedResult, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            throw new NotImplementedException();
        }

        public TableResult InsertOrMerge(TableOperation operation, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            throw new NotImplementedException();
        }

        public TableResult Replace(TableOperation operation, TableResult retrievedResult, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            throw new NotImplementedException();
        }

        public TableResult Insert(TableOperation operation, TableResult retrievedResult, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            throw new NotImplementedException();
        }

        public TableResult InsertOrReplace(TableOperation operation, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            throw new NotImplementedException();
        }

        public TableResult FlushAndRetrieve(IReplicatedTableEntity row, TableRequestOptions requestOptions = null, OperationContext operationContext = null, bool virtualizeEtag = true)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<TElement> ExecuteQuery<TElement>(TableQuery<TElement> query, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
            where TElement : ITableEntity, new()
        {
            throw new NotImplementedException();
        }

        public TableQuery<TElement> CreateQuery<TElement>()
                where TElement : ITableEntity, new()
        {
            throw new NotImplementedException();
        }

        public TableResult RepairRow(string partitionKey, string rowKey, IReplicatedTableEntity existingRow)
        {
            throw new NotImplementedException();
        }

        public ReconfigurationStatus RepairTable(int viewIdToRecoverFrom, TableBatchOperation unfinishedOps, long maxBatchSize = 100L)
        {
            throw new NotImplementedException();
        }

        public void ConvertXStoreTable(out long successCount, out long skippedCount, out long failedCount, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            successCount = 0;
            skippedCount = 0;
            failedCount = 0;

            throw new NotImplementedException();
        }
    }
}
