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
    using global::Azure;
    using global::Azure.Data.Tables;
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;

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

        public bool CreateIfNotExists()
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

        //public TableResult Execute(TableOperation operation)
        //{
        //    throw new NotImplementedException();
        //}

        //public IList<TableResult> CheckRetrieveInBatch(IEnumerable<TableTransactionAction> batch)
        //{
        //    throw new NotImplementedException();
        //}

        public IList<TableResult> ExecuteBatch(IEnumerable<TableTransactionAction> batch)
        {
            throw new NotImplementedException();
        }

        public TableResult Retrieve(string partitionKey, string rowKey)
        {
            throw new NotImplementedException();
        }

        public TableResult Delete(ITableEntity entity)
        {
            throw new NotImplementedException();
        }

        public TableResult Merge(ITableEntity entity, TableResult retrievedResult = null)
        {
            throw new NotImplementedException();
        }

        public TableResult InsertOrMerge(ITableEntity entity)
        {
            throw new NotImplementedException();
        }

        public TableResult Replace(ITableEntity entity, TableResult retrievedResult = null)
        {
            throw new NotImplementedException();
        }

        public TableResult Insert(ITableEntity entity, TableResult retrievedResult = null)
        {
            throw new NotImplementedException();
        }

        public TableResult InsertOrReplace(ITableEntity entity)
        {
            throw new NotImplementedException();
        }

        public TableResult FlushAndRetrieve(IReplicatedTableEntity row, bool virtualizeEtag = true)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<TElement> ExecuteQuery<TElement>(Expression<Func<TElement, bool>> filter)
            where TElement : ReplicatedTableEntity, new()
        {
            throw new NotImplementedException();
        }

        public IEnumerable<TElement> ExecuteQuery<TElement>(string filter, IEnumerable<string> select = null)
            where TElement : ReplicatedTableEntity, new()
        {
            throw new NotImplementedException();
        }

        public Pageable<TElement> CreateQuery<TElement>(Expression<Func<TElement, bool>> filter, int? maxPerPage = default, IEnumerable<string> select = null)
                where TElement : ReplicatedTableEntity, new()
        {
            throw new NotImplementedException();
        }

        public ReplicatedTableQuery<TElement> CreateReplicatedQuery<TElement>(Expression<Func<TElement, bool>> filter, int? maxPerPage = default, IEnumerable<string> select = null)
            where TElement : ReplicatedTableEntity, new()
        {
            throw new NotImplementedException();
        }

        public TableResult RepairRow(string partitionKey, string rowKey, IReplicatedTableEntity existingRow)
        {
            throw new NotImplementedException();
        }

        public ReconfigurationStatus RepairTable(int viewIdToRecoverFrom, IEnumerable<TableTransactionAction> unfinishedOps, long maxBatchSize = 100L)
        {
            throw new NotImplementedException();
        }

        public void ConvertXStoreTable(out long successCount, out long skippedCount, out long failedCount)
        {
            successCount = 0;
            skippedCount = 0;
            failedCount = 0;

            throw new NotImplementedException();
        }
    }
}
