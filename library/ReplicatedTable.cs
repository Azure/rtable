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
    using System.Data.Services.Client;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Net;
    using System.Threading.Tasks;
    using System.Threading;
    using System.Runtime.Remoting.Messaging;
    using global::Azure;
    using global::Azure.Data.Tables;

    public class ReplicatedTable : IReplicatedTable
    {
        private const string ParentThreadCallContextKey = "ParentThreadId";

        /// <summary>
        /// Configuration wrapper (layer) on top of RTable ConfigurationService instance to present a simplified interface to ReplicatedTable class.
        /// ReplicatedTable needs access to a subset of the configuration service that deals with the "TableName".
        /// </summary>
        private readonly IReplicatedTableConfigurationWrapper _configurationWrapper;
        private string myName;

        public bool ThrowOnStaleViewInLinqQueryFlag = false;

        public string TableName
        {
            get
            {
                return myName;
            }
            private set
            {
                myName = value;
            }
        }

        private View CurrentView
        {
            get { return this._configurationWrapper.GetWriteView(); }
        }

        // Used for read optimization to read from random replica
        private static Random random = new Random();

        // 2PC protocol constants
        private const int PREPARE_PHASE = 1;
        private const int COMMIT_PHASE = 2;

        private const int RETRY_LIMIT = 10;

        // Following fields are used by the caller to find the 
        // number of replicas created or deleted when 
        // CreateIfNotExists and DeleteIfExists are called.
        public short replicasCreated { get; private set; }
        public short replicasDeleted { get; private set; }

        public ReplicatedTable(string name, ReplicatedTableConfigurationService replicatedTableConfigurationAgent)
        {
            this._configurationWrapper = new ReplicatedTableConfigurationWrapper(replicatedTableConfigurationAgent);
            TableName = name;
        }

        public ReplicatedTable(string name, ReplicatedTableConfigurationServiceV2 replicatedTableConfigurationAgent)
        {
            this._configurationWrapper = new ReplicatedTableConfigurationV2Wrapper(name, replicatedTableConfigurationAgent);
            TableName = name;
        }

        private void ValidateTxnView(View txnView, bool viewMustBeWritable = true)
        {
            if (txnView.IsEmpty)
            {
                throw new ReplicatedTableStaleViewException(ReplicatedTableViewErrorCodes.EmptyView, "Empty view.");
            }

            if (txnView.IsExpired() && CurrentView.ViewId != txnView.ViewId)
            {
                throw new ReplicatedTableStaleViewException(ReplicatedTableViewErrorCodes.ViewIdChanged,
                                                            string.Format("View id changed from {0} to {1}", txnView.ViewId, CurrentView.ViewId ));
            }

            if (viewMustBeWritable && !txnView.IsWritable())
            {
                throw new ReplicatedTableStaleViewException(ReplicatedTableViewErrorCodes.ViewIsNotWritable, "View is not Writable");
            }
        }

        // Create ReplicatedTable replicas if they do not exist.
        // Returns: true if it creates all replicas as defined in the configuration service
        //        : false if it cannot create all replicas or if the config file has zero replicas
        // It sets the number of replicas created for the caller to check.
        public bool CreateIfNotExists()
        {
            ReplicatedTableLogger.LogVerbose("CreateIfNotExists");

            using (new StopWatchInternal(this.TableName, "CreateIfNotExists", this._configurationWrapper))
            {
                View txnView = CurrentView;
                ValidateTxnView(txnView, false);

                // Create individual table replicas if they are not created already 
                int tablesCreated = 0;
                foreach (var entry in txnView.Chain)
                {
                    ReplicatedTableLogger.LogVerbose("Creating table {0} on replica: {1}", this.TableName,
                        entry.Item2.Uri.ToString());
                    TableClient ctable = entry.Item2.GetTableClient(this.TableName);
                    if (!ctable.Exists(entry.Item2))
                    {
                        if (ctable.CreateIfNotExists() != null)
                        {
                            tablesCreated++;
                        }
                    }
                }

                return (tablesCreated > 0);
            }
        }

        // Check if a table (and its replicas) exist.
        // Returns: true if all replicas exist
        //        : false if any replica doesnt exist        
        public bool Exists()
        {
            using (new StopWatchInternal(this.TableName, "Exists", this._configurationWrapper))
            {
                View txnView = CurrentView;
                ValidateTxnView(txnView, false);

                // Return false if individual tables do not exist 
                foreach (var entry in txnView.Chain)
                {
                    TableClient ctable = entry.Item2.GetTableClient(this.TableName);
                    if (ctable.Exists(entry.Item2) == false)
                    {
                        return false;
                    }
                }

                return true;
            }
        }


        // Delete ReplicatedTable replicas if they exist.
        // Returns: true if it deletes all replicas as defined in the configuration service
        //        : false if it cannot delete all replicas or if the config file has zero replicas
        // It sets the number of replicas deleted for the caller to check.
        public bool DeleteIfExists()
        {
            using (new StopWatchInternal(this.TableName, "DeleteIfExists", _configurationWrapper))
            {
                View txnView = CurrentView;
                ValidateTxnView(txnView, false);

                replicasDeleted = 0;

                // Create individual table replicas if they are not created already 
                foreach (var entry in txnView.Chain)
                {
                    TableClient ctable = entry.Item2.GetTableClient(this.TableName);
                    var resp = ctable.Delete();
                    if (resp.Status == (int)HttpStatusCode.NoContent)
                    {
                        replicasDeleted++;
                    }
                }

                return (replicasDeleted > 0);
            }
        }

        /// <summary>
        /// Transform an operation in a batch before executing it on ReplicatedTable.
        /// If row._rtable_RowLock == true (i.e. Prepare phase) or tailIndex, and if it is not an Insert operation,
        /// this function will retrieve the row from the specified replica and increment row._rtable_Version.
        /// Finally, (does not matter whether it is in Prepare or Commit phase), create and return an appropriate TableOperation.
        /// The caller of this function will then create a IEnumerable<TableTransactionAction> based on the return value of this function.
        /// If needInsertTombstone == true, it will generate the tombstone insert operation and put it to insertTombstones if we need so
        /// </summary>
        /// <param name="row"></param>
        /// <param name="phase"></param>
        /// <param name="index"></param>
        /// <param name="needInsertTombstone"></param>
        /// <param name="insertTombstones"></param>
        /// <returns></returns>
        private TableTransactionAction TransformOp(View txnView, IReplicatedTableEntity row, int phase, int index, 
            bool needInsertTombstone, ref IList<TableTransactionAction> insertTombstones)
        {
            int tailIndex = txnView.TailIndex;

            if ((row._rtable_RowLock == true) || (index == tailIndex))
            {
                // Here is the transformation we do on the operation for the prepare phase of non-tail replicas
                //  or the commit phase for the tail replica.

                if (row._rtable_Operation != GetTableOperation(TableOperationType.Insert))
                {

                    // If it is InsertOrReplace or InsertOrMerge, then we do not have to check the etag.
                    bool checkETag = ((row._rtable_Operation == GetTableOperation(TableOperationType.InsertOrReplace)) ||
                                      (row._rtable_Operation == GetTableOperation(TableOperationType.InsertOrMerge)))
                        ? false
                        : true;

                    // If Etag is not supplied, retrieve the row first before writing
                    TableResult retrievedResult = RetrieveFromReplica(txnView, index, row.PartitionKey, row.RowKey);
                    if (retrievedResult == null ||
                       (retrievedResult.Result == null && retrievedResult.HttpStatusCode == (int)HttpStatusCode.ServiceUnavailable))
                    {
                        return null;
                    }

                    IReplicatedTableEntity currentRow = ConvertToIReplicatedTableEntity(retrievedResult);

                    if (checkETag == true)
                    {
                        if (retrievedResult.HttpStatusCode != (int)HttpStatusCode.OK)
                        {
                            // Row is not present, return appropriate error code as Merge, Delete and Replace
                            // requires row to be present.
                            ReplicatedTableLogger.LogInformational("Row is not present.");
                            return null;
                        }

                        // Check if vertual Etag matches at the head replica. We don't need to match it every replica.
                        if (IsEtagMismatchInternal(row, currentRow) && (index == 0))
                        {
                            // Return the error code that Etag does not match with the input ETag
                            ReplicatedTableLogger.LogInformational("TransformOp(): Etag does not match. row.ETag ({0}) != currentRow._rtable_Version ({1})",
                                                    row.ETag, currentRow._rtable_Version);
                            return null;
                        }

                        // do not need tombstone
                        needInsertTombstone = false;
                    }
                    else
                    {
                        // IOR or IOM, if exists, then not insert tombstone
                        needInsertTombstone &= (retrievedResult.HttpStatusCode != (int)HttpStatusCode.OK);
                    }

                    if (currentRow != null)
                    {
                        // Set appropriate values in ETag and _rtable_Version for merge, delete, replace,
                        // insertormerge, insertorreplace
                        row.ETag = new ETag(retrievedResult.Etag);
                        row._rtable_Version = currentRow._rtable_Version + 1;
                    }
                    else
                    {
                        // Initialize Etag if the row is not present
                        row.ETag = default;
                    }
                }
            }


            if ((row._rtable_Tombstone == true) && (phase == COMMIT_PHASE))
            {
                // In the commit phase, we delete the rows if tombstones are set in the prepare phase
                return new TableTransactionAction(TableTransactionActionType.Delete, row, row.ETag);
            }
            else if ((row._rtable_Operation == GetTableOperation(TableOperationType.Delete)) && (phase == PREPARE_PHASE))
            {
                // In the prepare phase, we replace the rows with tombstones for delete operations
                row._rtable_Tombstone = true;
                return new TableTransactionAction(TableTransactionActionType.UpdateReplace, row, row.ETag);
            }
            else if (
                (row._rtable_Operation == GetTableOperation(TableOperationType.Replace))
                ||
                (((row._rtable_Operation == GetTableOperation(TableOperationType.Insert)) ||
                (row._rtable_Operation == GetTableOperation(TableOperationType.InsertOrMerge)) ||
                (row._rtable_Operation == GetTableOperation(TableOperationType.InsertOrReplace)))
                && (phase == COMMIT_PHASE) && (index != tailIndex))
                )
            {
                // We use replace for the replace operation in both phases and for insert family operations in the commit phase 
                // for non-tail replicas.
                return new TableTransactionAction(TableTransactionActionType.UpsertReplace, row, row.ETag);
            }
            else if (row._rtable_Operation == GetTableOperation(TableOperationType.Merge))
            {
                // We use merge in both phases
                return new TableTransactionAction(TableTransactionActionType.UpsertMerge, row, row.ETag);
            }

            if (needInsertTombstone)
            {
                DynamicReplicatedTableEntity tsRow = new DynamicReplicatedTableEntity(row.PartitionKey, row.RowKey);
                tsRow._rtable_RowLock = true;
                tsRow._rtable_LockAcquisition = DateTime.UtcNow;
                tsRow._rtable_ViewId = txnView.ViewId;
                tsRow._rtable_Version = 0;
                tsRow._rtable_Tombstone = true;
                tsRow._rtable_Operation = GetTableOperation(TableOperationType.Insert);

                insertTombstones.Add(new TableTransactionAction(TableTransactionActionType.Add, tsRow, tsRow.ETag));
            }

            // we have already inserted tombstone, so replace/merge now
            row.ETag = ETag.All;
            if (row._rtable_Operation == GetTableOperation(TableOperationType.InsertOrMerge))
            {
                return new TableTransactionAction(TableTransactionActionType.UpsertMerge, row, row.ETag);
            }
            else
            {
                // insert / IOR
                return new TableTransactionAction(TableTransactionActionType.UpsertReplace, row, row.ETag);
            }
        }

        private IEnumerable<TableTransactionAction> TransformUpdateBatchOp(View txnView, IEnumerable<TableTransactionAction> batch, int phase, int index,
            out RequestFailedException rfe, IList<TableResult> results = null)
        {
            rfe = null;
            IEnumerator<TableTransactionAction> enumerator = batch.GetEnumerator();
            IList<TableTransactionAction> batchOp = new List<TableTransactionAction>();
            IEnumerator<TableResult> iter = (results != null) ? results.GetEnumerator() : null;
            int tailIndex = txnView.TailIndex;
            bool tombstone = (phase == PREPARE_PHASE && index == txnView.WriteHeadIndex);
            IList<TableTransactionAction> insertTombstone = new List<TableTransactionAction>();

            while (enumerator.MoveNext())
            {
                var operation = enumerator.Current;
                var opType = operation.ActionType;
                TableTransactionAction prepOp = null;
                IReplicatedTableEntity row = (IReplicatedTableEntity)operation.Entity;

                if (phase == PREPARE_PHASE)
                {
                    // Initialize the operation in the prepare/lock phase
                    row._rtable_Operation = GetTableOperation(opType);
                    row._rtable_RowLock = true;
                    row._rtable_LockAcquisition = DateTime.UtcNow;
                    row._rtable_Tombstone = false;
                    row._rtable_ViewId = txnView.ViewId;
                    row._rtable_Version = 0;
                    // Warning: We do not do a sanity check to check for Guid collisions, which should be very unlikely.
                    //          It may be better to check for safety but involves a round trip to the server.
                    row._rtable_BatchId = Guid.NewGuid();

                    if ((prepOp = TransformOp(txnView, row, phase, index, tombstone, ref insertTombstone)) == null)
                    {
                        return null;
                    }
                }
                else
                {
                    // commit-unlock phase, use the etags from the prepare phase.

                    // Commit phase should include the results from the prepare phase
                    // for all replicas except the tail, which has no prepare phase
                    if ((iter == null) && (index != tailIndex))
                    {
                        return null;
                    }
                    row._rtable_RowLock = false;

                    if ((prepOp = TransformOp(txnView, row, phase, index, tombstone, ref insertTombstone)) == null)
                    {
                        throw new ArgumentException();
                    }

                    if (index != tailIndex)
                    {
                        iter.MoveNext();
                        // Update ETag in batch operation entity
                        prepOp.Entity.ETag = new ETag(iter.Current.Etag);
                        prepOp = new TableTransactionAction(prepOp.ActionType, prepOp.Entity, prepOp.Entity.ETag);
                    }
                }

                // Add transformed operation to the batch
                batchOp.Add(prepOp);
            }

            if (tombstone)
            {
                // index == write head
                // lock head
                var tableClient = txnView[txnView.WriteHeadIndex];
                var table = tableClient.GetTableClient(this.TableName);
                if (insertTombstone.Count > 0)
                {
                    try
                    {
                        var resp = table.SubmitTransaction(TransformTableTransactionEntityToTableEntity(insertTombstone));
                        results = TransactionResponseToTableResultList(resp, insertTombstone);
                    } 
                    catch (RequestFailedException e)
                    {
                        rfe = e;
                        ReplicatedTableLogger.LogError("Batch: Failed to insert tombstones. Status: {0}. ErrorCode: {1}", e.Status, e.ErrorCode);
                        return null;
                    }

                    ValidateTxnView(txnView);

                    // lock rest of the replica
                    for (int i = txnView.WriteHeadIndex+1; i <= txnView.TailIndex; ++i)
                    {
                        tableClient = txnView[i];
                        table = tableClient.GetTableClient(this.TableName);
                        try
                        {
                            var resp = table.SubmitTransaction(TransformTableTransactionEntityToTableEntity(insertTombstone));
                            results = TransactionResponseToTableResultList(resp, insertTombstone);
                        }
                        catch (RequestFailedException e)
                        {
                            rfe = e;
                            ReplicatedTableLogger.LogError("Batch: Failed to insert tombstones. Status: {0}. ErrorCode: {1}", e.Status, e.ErrorCode);
                            return null;
                        }
                    }
                }
            }

            return batchOp;
        }

        // 
        // Validate the batch results
        //
        private bool PostProcessBatchExec(IEnumerable<TableTransactionAction> requests, IList<TableResult> results, int phase)
        {
            IEnumerator<TableTransactionAction> enumerator = requests.GetEnumerator();
            IEnumerator<TableResult> iter = (results != null) ? results.GetEnumerator() : null;

            // It's a failure if the batch size of request and results don't match
            if (requests.Count() != results.Count) return false;

            // 1. Check if all the results are fine
            while (enumerator.MoveNext() && iter.MoveNext())
            {
                TableResult result = iter.Current;
                TableTransactionAction operation = enumerator.Current;
                IReplicatedTableEntity row = (IReplicatedTableEntity)operation.Entity;

                TableOperationType opType;
                TableOperationType.TryParse(row._rtable_Operation, out opType);

                switch (opType)
                {
                    case TableOperationType.Insert:
                    case TableOperationType.Merge:
                    case TableOperationType.Replace:
                    case TableOperationType.Delete:
                    case TableOperationType.InsertOrMerge:
                    case TableOperationType.InsertOrReplace:
                        if ((result == null) ||
                            ((result.HttpStatusCode != (int)HttpStatusCode.Created) &&
                             (result.HttpStatusCode != (int)HttpStatusCode.NoContent)))
                        {
                            return false;
                        }
                        break;

                    default:
                        // Should not reach here
                        return false;
                }


                // 2. Virtualize etags in the results as well as request (etags include physical etags in the batch after their execution).
                if (phase == PREPARE_PHASE)
                {
                    long version = row._rtable_Version - 1;
                    row.ETag = new ETag(version.ToString()); // set it back to the prev version
                }
                else
                {
                    row.ETag = new ETag(row._rtable_Version.ToString());
                    result.Etag = row.ETag.ToString();
                }
            }

            return true;
        }

        /// <summary>
        /// Executes a batch operation on a table as an atomic operation, using the specified <see cref="TableRequestOptions"/> and <see cref="OperationContext"/>.
        /// </summary>
        public IList<TableResult> ExecuteBatch(
            IEnumerable<TableTransactionAction> batch)
        {
            // Invalid argument
            if (batch == null || batch.Count() == 0)
            {
                throw new InvalidOperationException("Cannot execute an empty batch operation");
            }

            using (new StopWatchInternal(this.TableName, "ExecuteBatch (size=" + batch.Count() + ")", this._configurationWrapper))
            {
                //   Otherwise, transform operations and run prepare phase
                View txnView = CurrentView;
                ValidateTxnView(txnView);

                // First, make sure all the rows in the batch operation are not locked. If they are locked, flush them.
                var oldResults = this.FlushAndRetrieveBatch(txnView, batch);

                if (oldResults == null)
                {
                    throw new Exception("something wrong in flush and retrieve");
                }

                // Perform the Prepare phase for the headIndex.
                IList<TableResult> headResults = this.RunPreparePhaseAgainstHeadReplica(txnView, batch);

                // Perform the Prepare phase for the other replicas and the Commit phase for all replica.
                IList<TableResult>[] results = this.Flush2PCBatch(txnView, batch, headResults);

                // Return the results returned by the tail replica, where all original operations are run.
                return results[txnView.TailIndex];
            }
        }

        /// <summary>
        /// Retrieve all the rows in the batchOperation from the headIndex.
        /// Check to see whether any row is locked.
        /// If a row is locked, call Flush2PC() to flush it through the chain.
        /// </summary>
        /// <param name="batch"></param>
        private IList<TableResult> FlushAndRetrieveBatch(
            View txnView,
            IEnumerable<TableTransactionAction> batch)
        {
            IEnumerable<TableTransactionAction> flushBatch = new List<TableTransactionAction>();
            IList<TableResult> results = new List<TableResult>();

            // TODO: repair them first
            bool failed = false;
            Parallel.ForEach(batch, op => {
                var row = op.Entity;
                var repairResult = this.RepairRow(row.PartitionKey, row.RowKey, null);
                if (repairResult == null)
                {
                    // service unavailable
                    failed = true;
                }
            });
            if (failed)
            {
                return null;
            }

            IEnumerator<TableTransactionAction> enumerator = batch.GetEnumerator();
            while (enumerator.MoveNext())
            {
                TableTransactionAction operation = enumerator.Current;
                IReplicatedTableEntity row = (IReplicatedTableEntity)operation.Entity;
                TableResult retrievedResult = RetrieveFromReplica(txnView, txnView.ReadHeadIndex, row.PartitionKey, row.RowKey);
                if (retrievedResult == null)
                {
                    return null;
                }
                else if (retrievedResult.HttpStatusCode == (int)HttpStatusCode.OK && retrievedResult.Result != null)
                {
                    IReplicatedTableEntity currentRow = ConvertToIReplicatedTableEntity(retrievedResult);

                    if (currentRow._rtable_RowLock == true)
                    {
                        if (DateTime.UtcNow >= currentRow._rtable_LockAcquisition + this._configurationWrapper.GetLockTimeout())
                        {
                            try
                            {
                                ReplicatedTableLogger.LogInformational("FlushAndRetrieveBatch(): Row is locked and has expired. PartitionKey={0} RowKey={1}",
                                                        row.PartitionKey, row.RowKey);
                                var result = this.Flush2PC(txnView, currentRow);
                                if (result == null ||
                                    (result.HttpStatusCode != (int)HttpStatusCode.OK && result.HttpStatusCode != (int)HttpStatusCode.NoContent))
                                {
                                    return null;
                                }
                                    
                                results.Add(result);
                            }
                            catch (Exception ex)
                            {
                                ReplicatedTableLogger.LogError("FlushAndRetrieveBatch(): Flush2PC() exception {0}", ex.ToString());
                                return null;
                            }
                        }
                        else
                        {
                            ReplicatedTableLogger.LogInformational("FlushAndRetrieveBatch(): Row is locked but NOT expired. PartitionKey={0} RowKey={1}",
                                                    row.PartitionKey, row.RowKey);
                            return null;
                        }
                    }
                    else
                    {
                        results.Add(retrievedResult);
                    }
                }
                else
                {
                    // row may not be present
                    results.Add(new TableResult() { HttpStatusCode = (int) HttpStatusCode.NotFound, Etag = null, Result = null });
                }
            }

            return results;
        }

        /// <summary>
        /// Run Prepare phase of the batch operatin against the head replica.
        /// </summary>
        /// <param name="batch"></param>
        /// <returns></returns>
        private IList<TableResult> RunPreparePhaseAgainstHeadReplica(
            View txnView,
            IEnumerable<TableTransactionAction> batch)
        {
            IList<TableResult> results;

            int phase = PREPARE_PHASE;

            TableServiceClient tableClient = txnView[txnView.WriteHeadIndex];
            TableClient table = tableClient.GetTableClient(this.TableName);

            // insert tombstone when transform
            IEnumerable<TableTransactionAction> batchOp = TransformUpdateBatchOp(txnView, batch, phase, txnView.WriteHeadIndex, out var rfe, null);
            if (batchOp == null)
            {
                throw new ReplicatedTableConflictException("Please retry again after random timeout", rfe);
            }
            var resp = table.SubmitTransaction(TransformTableTransactionEntityToTableEntity(batchOp));
            results = TransactionResponseToTableResultList(resp, batchOp);
            if (PostProcessBatchExec(batch, results, phase) == false)
            {
                throw new DataServiceRequestException();
            }

            return results;
        }

        /// <summary>
        /// Run Prepare phase for the non-head replicas and the Commit Phase for all replicas.
        /// The assumption is that Prepare phase for the same batch operation has been executed against the Head replica already.
        /// </summary>
        /// <param name="batch"></param>
        /// <param name="headResults"></param>
        /// <returns></returns>
        private IList<TableResult>[] Flush2PCBatch(
            View txnView,
            IEnumerable<TableTransactionAction> batch,
            IList<TableResult> headResults)
        {
            IList<TableResult>[] results = new IList<TableResult>[txnView.Chain.Count];

            results[0] = headResults;

            // Run the prepare and lock phase on the non-head replica using the transformed batch operation.
            int phase = PREPARE_PHASE;
            int tailIndex = txnView.TailIndex;
            for (int index = 1; index < tailIndex; index++)
            {
                TableServiceClient tableClient = txnView[index];
                TableClient table = tableClient.GetTableClient(this.TableName);
                IEnumerable<TableTransactionAction> batchOp = TransformUpdateBatchOp(txnView, batch, phase, index, out var rfe, null);
                if (batchOp == null)
                {
                    throw new ReplicatedTableConflictException("Please retry again after a random delay", rfe);
                }
                var resp = table.SubmitTransaction(TransformTableTransactionEntityToTableEntity(batchOp));
                results[index] = TransactionResponseToTableResultList(resp, batchOp);
                if (PostProcessBatchExec(batch, results[index], phase) == false)
                {
                    throw new DataServiceRequestException();
                }
            }

            // Run the commit phase to unlock and commit the batch 
            for (int index = tailIndex; index >= 0; index--)
            {
                TableServiceClient tableClient = txnView[index];
                TableClient table = tableClient.GetTableClient(this.TableName);
                IEnumerable<TableTransactionAction> batchOp;

                // If there is only one replica then we have to transform the operation 
                // here as it is not transformed in the prepare phase.
                if (tailIndex == 0)
                {
                    phase = PREPARE_PHASE;
                    batchOp = TransformUpdateBatchOp(txnView, batch, phase, index, out _, null);
                }
                phase = COMMIT_PHASE;
                batchOp = TransformUpdateBatchOp(txnView, batch, phase, index, out _, results[index]);
                var resp = table.SubmitTransaction(TransformTableTransactionEntityToTableEntity(batchOp));
                results[index] = TransactionResponseToTableResultList(resp, batchOp);
                if (PostProcessBatchExec(batch, results[index], phase) == false)
                {
                    throw new DataServiceRequestException();
                }
            }


            return results;
        }


        //
        // Retrieve: Read the last committed update (update that cannot be lost despite replica failures). 
        //           This may not include an outstanding update that is yet to reach the tail replica. 
        //
        public TableResult Retrieve(string partitionKey, string rowKey)
        {
            // The tail replica is guaranteed to have the latest committed version. 
            // The tail replica is usually configured to be the closest one, so always go there. This policy optimizes
            // for latency. If read load balancing across replicas is desired, then choose a random replica and read if lock bit is 0.
            View txnView = CurrentView;
            ValidateTxnView(txnView, false);

            int tailIndex = txnView.ReadTailIndex; // [Head] -> ... -> [ReadTailIndex] -> ... -> [Tail]
            int index = tailIndex;                 //                            ^
                                                   // start readings from here:  |

            TableResult retrievedResult = null;


            //The current read algorithm is as follows:
            //  1. Try read from tail, since tail always has committed data
            //  2. If above succeeds, return the result
            //  3. If read at tail fails, then traverse the chain in reverse from tail to readHeadIndex. The first replica that
            //     can be reached and whose rowLock = false has the committed data. If no such replica exists we fail the read
            while (true)
            {
                retrievedResult = RetrieveFromReplica(txnView, index, partitionKey, rowKey);
                if (retrievedResult == null)
                {
                    //If we attempted at the read head and still failed, we just fail the request
                    if (index == txnView.ReadHeadIndex)
                    {
                        throw new Exception("Read failed at all replicas in the read view");
                    }

                    // If it failed trying to read from a replica other than the read head, 
                    // try the replica previous to index
                    index--;
                    continue;
                }

                if (retrievedResult.Result == null)
                {
                    if (retrievedResult.HttpStatusCode == (int)HttpStatusCode.ServiceUnavailable &&
                        index != txnView.ReadHeadIndex)
                    {
                        // If failed to read, from other than the Head, try from a previous replica
                        index--;
                        continue;
                    }


                    // entity does not exist, so return the error code returned by any replica  
                    return retrievedResult;
                }

                if (retrievedResult.HttpStatusCode != (int)HttpStatusCode.OK)
                {
                    return retrievedResult;
                }

                IReplicatedTableEntity currentRow = retrievedResult.Result as IReplicatedTableEntity;

                // if the row is not committed, throw an exception
                if (index != tailIndex && currentRow._rtable_RowLock)
                {
                    throw new Exception("Uncommitted value");
                }

                // if the entry has a tombstone set, don't return it.
                if (currentRow._rtable_Tombstone)
                {
                    return new TableResult() { Result = null, Etag = null, HttpStatusCode = (int)HttpStatusCode.NotFound };
                }

                // We read a committed value. return it after virtualizing the ETag
                retrievedResult.Etag = currentRow._rtable_Version.ToString();
                currentRow.ETag = new ETag(retrievedResult.Etag);

                return retrievedResult;
            }
        }

        //
        // Delete: Delete a row
        //
        public TableResult Delete(ITableEntity entity)
        {
            var row = (IReplicatedTableEntity)entity;
            row._rtable_Operation = GetTableOperation(TableOperationType.Delete);

            // Delete() = Replace() with "_rtable_Tombstone = true", rows are deleted in the commit phase 
            // after they are replaced with tombstones in the prepare phase.
            row._rtable_Tombstone = true;
            return Replace(row);
        }

        //
        // Merge: Merge a row if ETag matches
        // 
        //
        public TableResult Merge(ITableEntity entity, TableResult retrievedResult = null)
        {
            View txnView = CurrentView;
            ValidateTxnView(txnView);
            return MergeInternal(txnView, entity, retrievedResult);
        }

        private TableResult MergeInternal(View txnView, ITableEntity entity, TableResult retrievedResult)
        {
            IReplicatedTableEntity row = (IReplicatedTableEntity)entity;
            TableResult result;
            bool checkETag = (row._rtable_Operation == GetTableOperation(TableOperationType.InsertOrMerge))
                ? false
                : true;
            row._rtable_Operation = GetTableOperation(TableOperationType.Merge);

            if (retrievedResult == null)
            {
                retrievedResult = FlushAndRetrieveInternal(txnView, row, false);
            }

            if (retrievedResult.HttpStatusCode != (int)HttpStatusCode.OK)
            {
                // Row is not present, return appropriate error code
                ReplicatedTableLogger.LogInformational("MergeInternal: Row is already present ");
                return new TableResult() { Result = null, Etag = null, HttpStatusCode = (int)HttpStatusCode.NotFound };
            }
            else
            {
                // Row is present at the replica
                // Merge the row
                ReplicatedTableEntity currentRow = (ReplicatedTableEntity)(retrievedResult.Result);
                if (checkETag && IsEtagMismatch(row, currentRow))
                {
                    // Return the error code that Etag does not match with the input ETag
                    ReplicatedTableLogger.LogInformational(
                        "MergeInternal: ETag mismatch. row.ETag ({0}) != currentRow._rtable_Version ({1})",
                        row.ETag, currentRow._rtable_Version);
                    return new TableResult()
                    {
                        Result = null,
                        Etag = null,
                        HttpStatusCode = (int)HttpStatusCode.PreconditionFailed
                    };
                }

                TableServiceClient tableClient = txnView[0];
                row.ETag = new ETag(retrievedResult.Etag);
                row._rtable_RowLock = true;
                row._rtable_LockAcquisition = DateTime.UtcNow;
                row._rtable_Tombstone = false;
                row._rtable_Version = currentRow._rtable_Version + 1;
                row._rtable_ViewId = txnView.ViewId;

                // Lock the head first by inserting the row
                result = UpdateOrDeleteRow(tableClient, row);
                ValidateTxnView(txnView);
                if (result == null)
                {
                    ReplicatedTableLogger.LogError("MergeInternal: Failed to lock the head. ");
                    return new TableResult()
                    {
                        Result = null,
                        Etag = null,
                        HttpStatusCode = (int)HttpStatusCode.ServiceUnavailable
                    };
                }

                if (result.HttpStatusCode != (int)HttpStatusCode.NoContent)
                {
                    ReplicatedTableLogger.LogError("MergeInternal: Failed to take lock on head: {0}", result.HttpStatusCode);
                    return result;
                }

                // Call Flush2PC to run 2PC on backup (non-head) replicas
                // If successful, it returns HttpStatusCode 204 (no content returned, when replaced in the second phase) 
                if (((result = Flush2PC(txnView, row, result.Etag)) == null) ||
                    (result.HttpStatusCode != (int)HttpStatusCode.NoContent))
                {
                    // Failed, abort with error and let the application take care of it by reissuing it 
                    // TO DO: Alternately, we could wait and retry after sometime using requestOptions.
                    ReplicatedTableLogger.LogError("MergeInternal: Failed during prepare phase in 2PC for row key: {0}", row.RowKey);
                    return new TableResult()
                    {
                        Result = null,
                        Etag = null,
                        HttpStatusCode = (int)HttpStatusCode.ServiceUnavailable
                    };
                }
                else
                {
                    // Success. Virtualize Etag before returning the result
                    result.Etag = row._rtable_Version.ToString();
                    row.ETag = new ETag(result.Etag);
                }

                return result;
            }
        }

        //
        // InsertOrMerge: Insert a row or update the row if it already exists
        //
        public TableResult InsertOrMerge(ITableEntity entity)
        {
            IReplicatedTableEntity row = (IReplicatedTableEntity)entity;
            row._rtable_Operation = GetTableOperation(TableOperationType.InsertOrMerge);
            View txnView = CurrentView;
            ValidateTxnView(txnView);

            TableResult retrievedResult = FlushAndRetrieve(row, false);

            if (retrievedResult.HttpStatusCode == (int)HttpStatusCode.OK)
            {
                // Row is present at the replica, merge the row
                return MergeInternal(txnView, row, retrievedResult);
            }
            else
            {
                // Row is not present at the head, insert the row
                return Insert(row, retrievedResult);
            }
        }

        //
        // Replace: Replace a row if ETag matches
        //
        public TableResult Replace(ITableEntity entity, TableResult retrievedResult = null)
        {
            View txnView = CurrentView;
            ValidateTxnView(txnView);
            return ReplaceInternal(txnView, entity, retrievedResult);
        }

        private TableResult ReplaceInternal(View txnView, ITableEntity entity, TableResult retrievedResult)
        {
            IReplicatedTableEntity row = (IReplicatedTableEntity)entity;
            TableResult result;
            ReplicatedTableEntity currentRow = null;
            using (new StopWatchInternal(this.TableName, "ReplaceInternal (internal)", this._configurationWrapper))
            {
                bool checkETag = false;
                if (row._rtable_Operation != GetTableOperation(TableOperationType.InsertOrReplace) &&
                    row.ETag != ETag.All)
                {
                    checkETag = true;
                }

                // If it's called by the delete operation do not set the tombstone
                if (row._rtable_Operation != GetTableOperation(TableOperationType.Delete))
                {
                    row._rtable_Tombstone = false;
                }

                row._rtable_Operation = GetTableOperation(TableOperationType.Replace);

                if (retrievedResult == null)
                {
                    retrievedResult = FlushAndRetrieve(row, false);
                }

                if (retrievedResult.HttpStatusCode == (int) HttpStatusCode.Conflict)
                {
                    return new TableResult()
                    {
                        Result = null,
                        Etag = null,
                        HttpStatusCode = (int)(checkETag ? HttpStatusCode.PreconditionFailed : HttpStatusCode.Conflict)
                    };
                }

                if (retrievedResult.HttpStatusCode != (int) HttpStatusCode.OK)
                {
                    // Row is not present, return appropriate error code
                    ReplicatedTableLogger.LogInformational(
                        "ReplaceInternal: Row is not present. ParitionKey={0} RowKey={1}", row.PartitionKey, row.RowKey);
                    return new TableResult()
                    {
                        Result = null,
                        Etag = null,
                        HttpStatusCode = (int) HttpStatusCode.NotFound
                    };
                }

                // Row is present at the replica
                // Replace the row 
                currentRow = (ReplicatedTableEntity) (retrievedResult.Result);
                if (checkETag && IsEtagMismatch(row, currentRow))
                {
                    // Return the error code that Etag does not match with the input ETag
                    ReplicatedTableLogger.LogInformational(
                        "ReplaceInternal: ETag mismatch. row.ETag ({0}) != currentRow._rtable_Version ({1})",
                        row.ETag, currentRow._rtable_Version);
                    return new TableResult()
                    {
                        Result = null,
                        Etag = null,
                        HttpStatusCode = (int) HttpStatusCode.PreconditionFailed
                    };
                }
                TableServiceClient tableClient = txnView[txnView.WriteHeadIndex];
                row.ETag = new ETag(retrievedResult.Etag);
                row._rtable_RowLock = true;
                row._rtable_LockAcquisition = DateTime.UtcNow;
                row._rtable_Version = currentRow._rtable_Version + 1;
                row._rtable_ViewId = txnView.ViewId;

                // Lock the head first by inserting the row
                result = UpdateOrDeleteRow(tableClient, row);
                ValidateTxnView(txnView);
                if (result == null)
                {
                    ReplicatedTableLogger.LogError("ReplaceInternal: Failed to lock the head. ");
                    return new TableResult()
                    {
                        Result = null,
                        Etag = null,
                        HttpStatusCode = (int) HttpStatusCode.ServiceUnavailable
                    };
                }

                if (result.HttpStatusCode != (int) HttpStatusCode.NoContent)
                {
                    ReplicatedTableLogger.LogError("ReplaceInternal: Failed to take lock on head: {0}",
                        result.HttpStatusCode);
                    return result;
                }

                // Call Flush2PC to run 2PC on the chain
                // If successful, it returns HttpStatusCode 204 (no content returned, when replaced in the second phase) 
                if (((result = Flush2PC(txnView, row, result.Etag)) == null) ||
                    (result.HttpStatusCode != (int) HttpStatusCode.NoContent))
                {
                    // Failed, abort with error and let the application take care of it by reissuing it 
                    // TO DO: Alternately, we could wait and retry after sometime using requestOptions.
                    ReplicatedTableLogger.LogError(
                        "ReplaceInternal: Failed during prepare phase in 2PC for row PK:{0} RK:{1}", row.PartitionKey, row.RowKey);
                    return new TableResult()
                    {
                        Result = null,
                        Etag = null,
                        HttpStatusCode = (int) HttpStatusCode.ServiceUnavailable
                    };
                }

                // Success. Virtualize Etag before returning the result
                result.Etag = row._rtable_Version.ToString();
                row.ETag = new ETag(result.Etag);

                return result;
            }
        }


        //
        // Insert: Insert a row
        //
        public TableResult Insert(ITableEntity entity, TableResult retrievedResult = null)
        {

            View txnView = CurrentView;
            ValidateTxnView(txnView);
            return InsertInternal(txnView, entity, retrievedResult);

        }

        private TableResult InsertInternal(View txnView, ITableEntity entity, TableResult retrievedResult)
        {
            IReplicatedTableEntity row = (IReplicatedTableEntity)entity;
            row._rtable_Operation = GetTableOperation(TableOperationType.Insert);
            TableResult result;
            string[] eTagStrings = new string[txnView.Chain.Count];

            if (retrievedResult == null)
            {
                // In case the entry in Head account has _rtable_RowLock=true
                retrievedResult = FlushAndRetrieve(row, false);
                if (retrievedResult == null)
                {
                    ReplicatedTableLogger.LogError("Insert: failure in flush.");
                    return null;
                }

                if (retrievedResult.HttpStatusCode == (int) HttpStatusCode.Conflict)
                {
                    ReplicatedTableLogger.LogError("Insert: row is locked in read head.");
                    return retrievedResult;
                }
            }

            TableServiceClient headTableClient = txnView[txnView.WriteHeadIndex];

            // insert a tombstone first. we insert it without a lock since insert will detect conflict anyway.
            DynamicReplicatedTableEntity tsRow = new DynamicReplicatedTableEntity(row.PartitionKey, row.RowKey);
            tsRow._rtable_RowLock = true;
            tsRow._rtable_LockAcquisition = DateTime.UtcNow;
            tsRow._rtable_ViewId = txnView.ViewId;
            tsRow._rtable_Version = 0;
            tsRow._rtable_Tombstone = true;
            tsRow._rtable_Operation = GetTableOperation(TableOperationType.Insert);

            // Lock the head first by inserting the row
            result = InsertRow(headTableClient, tsRow);
            ValidateTxnView(txnView);
            if (result == null)
            {
                ReplicatedTableLogger.LogError("Insert: Failed to insert at the head.");
                return null;
            }

            // insert must return the contents of the row
            if (result.HttpStatusCode != (int)HttpStatusCode.Created &&
                result.HttpStatusCode != (int)HttpStatusCode.NoContent)
            {
                ReplicatedTableLogger.LogError("Insert: Failed to insert at the head with HttpStatusCode = {0}", result.HttpStatusCode);
                return new TableResult()
                {
                    Result = null,
                    Etag = null,
                    HttpStatusCode = result.HttpStatusCode
                };
            }

            // copy the resulting row from the head into the head result to pass to the Replace operation later on.
            retrievedResult = result;
            retrievedResult.HttpStatusCode = (int)HttpStatusCode.OK;
            retrievedResult.Result = tsRow;

            // we have taken a lock on the head.
            // now flush this row to the remaining replicas.
            result = FlushPreparePhase(txnView, tsRow, eTagStrings);
            if (result == null)
            {
                return null;
            }

            // now replace the row with version 0 in ReplicatedTable and return the result
            row.ETag = new ETag(tsRow._rtable_Version.ToString());
            return ReplaceInternal(txnView, row, retrievedResult);
        }

        //
        // InsertOrReplace: Insert a row or update the row if it already exists
        //
        public TableResult InsertOrReplace(ITableEntity entity)
        {
            IReplicatedTableEntity row = (IReplicatedTableEntity)entity;
            row._rtable_Operation = GetTableOperation(TableOperationType.InsertOrReplace);
            TableResult result;

            //
            // InsertOrReplace would normally not fail due to a conflict.
            // However, RTable can return a conflict because it needs to enforce strict
            // ordering of write operations.
            // if the operation replace returns a conflict, we will retry the operation up to
            // five times.
            //

            var rnd = new Random((int)DateTime.UtcNow.Ticks);
            int retryLimit = RETRY_LIMIT;

            Func<bool> RetryIf = RetryPolicy.RetryWithDelayIf(() => rnd.Next(100, 300), () => --retryLimit > 0);

            do
            {
                View txnView = CurrentView;
                ValidateTxnView(txnView);
                result = FlushAndRetrieveInternal(txnView, row, false);

                if (result.HttpStatusCode == (int) HttpStatusCode.NotFound)
                {
                    // Row is not present at the head, insert the row
                    result = InsertInternal(txnView, row, result);
                }
                else if (result.HttpStatusCode == (int) HttpStatusCode.OK)
                {
                    // Row is present at the replica, replace the row
                    row.ETag = ETag.All;
                    result = ReplaceInternal(txnView, row, result);
                }

            } while (result != null &&
                     (result.HttpStatusCode == (int) HttpStatusCode.Conflict || result.HttpStatusCode == (int) HttpStatusCode.PreconditionFailed) &&
                     RetryIf());

            return result;
        }


        //
        // FlushAndRetrieve: Flush (if it is not committed) and retrieve a row.  
        //
        public TableResult FlushAndRetrieve(IReplicatedTableEntity row, bool virtualizeEtag = true)
        {
            View txnView = CurrentView;
            ValidateTxnView(txnView);
            return FlushAndRetrieveInternal(txnView, row, virtualizeEtag);
        }

        private TableResult FlushAndRetrieveInternal(View txnView, IReplicatedTableEntity row, bool virtualizeEtag = true)
        {
            //
            // If this row needs repair due to an unstable view, do it now
            //
            TableResult repairRowTableResult = this.RepairRow(row.PartitionKey, row.RowKey, null);

            if (repairRowTableResult == null ||
                (repairRowTableResult.HttpStatusCode != (int)HttpStatusCode.OK && repairRowTableResult.HttpStatusCode != (int) HttpStatusCode.NoContent))
            {
                ReplicatedTableLogger.LogError(
                    "FlushAndRetrieve(): RepairRow() returned Unexpected StatusCode {0}. ParitionKey={1} RowKey={2}",
                    (repairRowTableResult != null) ? repairRowTableResult.HttpStatusCode.ToString() : "null",
                    row.PartitionKey,
                    row.RowKey);

                // Row still locked
                if (repairRowTableResult != null && repairRowTableResult.HttpStatusCode == (int)HttpStatusCode.Conflict)
                {
                    return repairRowTableResult;
                }

                // else
                return new TableResult()
                {
                    Result = null,
                    Etag = null,
                    HttpStatusCode = (int) HttpStatusCode.NotFound
                };
            }


            TableResult retrievedResult = null;
            if (this._configurationWrapper.IsConvertToRTableMode())
            {
                // When we are in ConvertToRTable, the existing entities were created as XStore entities.
                // Hence, need to use InitDynamicReplicatedTableEntity which catches KeyNotFoundException
                retrievedResult = RetrieveFromReplica(txnView, txnView.WriteHeadIndex, row.PartitionKey, row.RowKey);
            }
            else
            {
                retrievedResult = RetrieveFromReplica(txnView, txnView.WriteHeadIndex, row.PartitionKey, row.RowKey);
            }

            if (retrievedResult == null)
            {
                // If retrieve fails for some reason then return "service unavailable - 503 " error code
                return new TableResult()
                {
                    Result = null,
                    Etag = null,
                    HttpStatusCode = (int) HttpStatusCode.ServiceUnavailable
                };
            }

            if (retrievedResult.HttpStatusCode != (int) HttpStatusCode.OK)
            {
                // return the error code!
                return retrievedResult;
            }

            IReplicatedTableEntity readRow = ConvertToIReplicatedTableEntity(retrievedResult);
            // Retrieve from the head
            TableResult result = null;


            if (readRow._rtable_RowLock == false)
            {
                // The row is already committed, return the retrieved result from the head
                result = retrievedResult;
                if (virtualizeEtag)
                {
                    result.Etag = readRow._rtable_Version.ToString();
                }
                return result;
            }

            // If the row is not committed, either:
            // (1) (Lock expired) flush the row to other replicas, commit it, and return the result.
            // Or (2) (Lock not expired) return a Conflict so that the caller can try again later,
            if (DateTime.UtcNow >= readRow._rtable_LockAcquisition + this._configurationWrapper.GetLockTimeout())
            {
                ReplicatedTableLogger.LogInformational(
                    "FlushAndRetrieve(): _rtable_RowLock has expired. So, calling Flush2PC(). _rtable_LockAcquisition={0} CurrentTime={1}",
                    readRow._rtable_LockAcquisition, DateTime.UtcNow);

                // The entity was locked by a different client a long time ago, so flush it.

                result = Flush2PC(txnView, readRow);
                
                if ((result.HttpStatusCode == (int) HttpStatusCode.OK) ||
                    (result.HttpStatusCode == (int) HttpStatusCode.NoContent))
                {
                    // If flush is successful, return the result from the head.
                    result = retrievedResult;
                    if (virtualizeEtag)
                    {
                        result.Etag = readRow._rtable_Version.ToString();
                    }
                }
            }
            else
            {
                // The entity was locked by a different client recently. Return conflict so that the caller can retry.
                ReplicatedTableLogger.LogInformational(
                    "FlushAndRetrieve(): Row is locked. _rtable_LockAcquisition={0} CurrentTime={1} timeout={2}",
                    readRow._rtable_LockAcquisition, DateTime.UtcNow, this._configurationWrapper.GetLockTimeout());
                result = new TableResult()
                {
                    Result = null,
                    Etag = null,
                    HttpStatusCode = (int) HttpStatusCode.Conflict
                };
            }

            return result;
        }

        public IEnumerable<TElement> ExecuteQuery<TElement>(string filter, IEnumerable<string> select = null)
            where TElement : class, ITableEntity
        {
            using (new StopWatchInternal(this.TableName, "ExecuteQuery", this._configurationWrapper))
            {
                IEnumerable<TElement> rows = Enumerable.Empty<TElement>();
                View txnView = CurrentView;

                try
                {
                    if (select != null)
                    {
                        select = select.AddRTableProperties()
                            .AddITableEntityProperties();
                    }
                    int tailIndex = txnView.ReadTailIndex; // [Head] -> ... -> [ReadTailIndex] -> ... -> [Tail]
                                                           //                       ^
                                                           // query this replica :  |
                    TableClient tail = txnView[tailIndex].GetTableClient(TableName);
                    rows = tail.Query<TElement>(filter, select: select);
                }
                catch (Exception e)
                {
                    ReplicatedTableLogger.LogError("Error in ExecuteQuery: caught exception {0}", e);
                }

                return new ReplicatedTableEnumerable<TElement>(
                                        rows,
                                        this._configurationWrapper.IsConvertToRTableMode(),
                                        txnView.ViewId,
                                        GetBehaviorOnStaleView());
            }
        }

        public Pageable<TElement> CreateQuery<TElement>(Expression<Func<TElement, bool>> filter, int? maxPerPage = default, IEnumerable<string> select = null)
                where TElement : class, ITableEntity
        {
            using (new StopWatchInternal(this.TableName, "CreateQuery", this._configurationWrapper))
            {
                Pageable<TElement> query = null;
                View txnView = CurrentView;

                try
                {
                    int tailIndex = txnView.ReadTailIndex; // [Head] -> ... -> [ReadTailIndex] -> ... -> [Tail]
                                                           //                       ^
                                                           // query this replica :  |

                    TableClient tail = txnView[tailIndex].GetTableClient(TableName);
                    query = tail.Query(filter, maxPerPage, select);
                }
                catch (Exception e)
                {
                    ReplicatedTableLogger.LogError("Error in CreateQuery: caught exception {0}", e);
                }

                return query;
            }
        }

        public ReplicatedTableQuery<TElement> CreateReplicatedQuery<TElement>(string filter)
                where TElement : class, ITableEntity
        {
            using (new StopWatchInternal(this.TableName, "CreateReplicatedQuery", this._configurationWrapper))
            {
                Pageable<TElement> query = null;
                View txnView = CurrentView;

                try
                {
                    int tailIndex = txnView.ReadTailIndex; // [Head] -> ... -> [ReadTailIndex] -> ... -> [Tail]
                                                           //                       ^
                                                           // query this replica :  |

                    TableClient tail = txnView[tailIndex].GetTableClient(TableName);
                    query = tail.Query<TElement>(filter);
                }
                catch (Exception e)
                {
                    ReplicatedTableLogger.LogError("Error in CreateReplicatedQuery: caught exception {0}", e);
                }

                return new ReplicatedTableQuery<TElement>(
                                        query.AsQueryable(),
                                        this._configurationWrapper.IsConvertToRTableMode(),
                                        txnView.ViewId,
                                        GetBehaviorOnStaleView());
            }
        }

        /// <summary>
        /// Defines LINQ behavior on stale viewId
        /// </summary>
        /// <returns></returns>
        private StaleViewHandling GetBehaviorOnStaleView()
        {
            // ignore rows with higher viewId
            if (this._configurationWrapper.IsIgnoreHigherViewIdRows())
            {
                return StaleViewHandling.TreatAsNotFound;
            }

            // Else, throw if we detect a row with a higher viewId
            if (ThrowOnStaleViewInLinqQueryFlag)
            {
                return StaleViewHandling.ThrowOnStaleView;
            }

            // Else, return rows with higher viewIds as is.
            return StaleViewHandling.NoThrowOnStaleView;
        }

        #region VirtualizeEtag helpers

        internal static void VirtualizeEtagForInitDynamicReplicatedTableEntity(ITableEntity curr)
        {
            var row = curr as InitDynamicReplicatedTableEntity;
            row.ETag = new ETag(row._rtable_Version.ToString());
        }

        internal static void VirtualizeEtagForTableEntityInConvertMode(ITableEntity curr)
        {
            var row = curr as TableEntity;
            row.ETag = row.ContainsKey("_rtable_Version")
                            ? new ETag(row["_rtable_Version"].ToString())
                            : new ETag("0");
        }

        internal static void VirtualizeEtagForReplicatedTableEntity(ITableEntity curr)
        {
            var row = curr as ReplicatedTableEntity;
            row.ETag = new ETag(row._rtable_Version.ToString());
        }

        internal static void VirtualizeEtagForTableEntity(ITableEntity curr)
        {
            var row = curr as TableEntity;
            row.ETag = new ETag(row["_rtable_Version"].ToString());
        }

        #endregion


        #region Tombstone check helpers

        internal static bool HasTombstoneForInitDynamicReplicatedTableEntity(ITableEntity curr)
        {
            var row = curr as InitDynamicReplicatedTableEntity;
            return row._rtable_Tombstone;
        }

        internal static bool HasTombstoneForTableEntityInConvertMode(ITableEntity curr)
        {
            var row = curr as TableEntity;
            return row.ContainsKey("_rtable_Tombstone") && bool.TryParse(row["_rtable_Tombstone"].ToString(), out var tombstone) && tombstone;
        }

        internal static bool HasTombstoneForReplicatedTableEntity(ITableEntity curr)
        {
            var row = curr as ReplicatedTableEntity;
            return row._rtable_Tombstone;
        }

        internal static bool HasTombstoneForTableEntity(ITableEntity curr)
        {
            var row = curr as TableEntity;
            return bool.TryParse(row["_rtable_Tombstone"].ToString(), out var tombstone) && tombstone;
        }

        #endregion


        #region Row viewId helpers

        internal static long RowViewIdForInitDynamicReplicatedTableEntity(ITableEntity curr)
        {
            var row = curr as InitDynamicReplicatedTableEntity;
            return row._rtable_ViewId;
        }

        internal static long RowViewIdForTableEntityInConvertMode(ITableEntity curr)
        {
            var row = curr as TableEntity;
            if (row.ContainsKey("_rtable_ViewId") && long.TryParse(row["_rtable_ViewId"].ToString(), out var viewId))
            {
                return viewId;
            }

            return 0;
        }

        internal static long RowViewIdForReplicatedTableEntity(ITableEntity curr)
        {
            var row = curr as ReplicatedTableEntity;
            return row._rtable_ViewId;
        }

        internal static long RowViewIdForTableEntity(ITableEntity curr)
        {
            var row = curr as TableEntity;
            long.TryParse(row["_rtable_ViewId"].ToString(), out var viewId);
            return viewId;
        }

        #endregion


        /// <summary>
        /// Retrieve a row from a specific replica i.e. @index.
        /// No sanity check (avoiding reflection calls) so make sure argument operation == TableOperationType.Retrieve
        ///
        /// Return null when an exception, or xstore retunred null !
        /// Return (TableResult.Result = null) when the type of entity is unknown.
        /// Throws if the entity.ViewId > View.viewId.
        /// Return TableResult.Result = entity converted to RTable format.
        /// </summary>
        private TableResult RetrieveFromReplica(View txnView, int index, string partitionKey, string rowKey)
        {
            // Assert (GetOpType(operation) == TableOperationType.Retrieve)

            TableServiceClient tableClient = txnView[index];
            TableClient table = tableClient.GetTableClient(this.TableName);
            Response<TableEntity> resp;

            try
            {
                resp = table.GetEntity<TableEntity>(partitionKey, rowKey);
                ValidateTxnView(txnView, false);

                if (resp == null || !resp.HasValue)
                {
                    return null;
                }
            }
            catch (RequestFailedException re)
            {
                ReplicatedTableLogger.LogError("Storage exception from replicaIndex:{0},\nPK:{1},\nRK:{2},\n{3}", index, partitionKey, rowKey, re);

                var innerException = re.InnerException as WebException;
                var statusCode = (HttpStatusCode)re.Status;
                if (innerException != null)
                {
                    HttpWebResponse httpWebResponse = (HttpWebResponse)innerException.Response;
                    if(httpWebResponse == null)
                    {
                        ReplicatedTableLogger.LogError("Unable to get HTTPWebResponse from Storage exception returning ServiceUnavailable");
                        return new TableResult() { Result = null, Etag = null, HttpStatusCode = (int)HttpStatusCode.ServiceUnavailable };
                    }

                    statusCode = httpWebResponse.StatusCode;
                }

                switch (statusCode)
                {
                    case HttpStatusCode.BadRequest:
                        {
                            throw;
                        }
                    case HttpStatusCode.ServiceUnavailable:
                        {
                            return new TableResult() { Result = null, Etag = null, HttpStatusCode = (int)HttpStatusCode.ServiceUnavailable };
                        }

                    case HttpStatusCode.NotFound:
                        {
                            return new TableResult() { Result = null, Etag = null, HttpStatusCode = (int)HttpStatusCode.NotFound };
                        }

                    default:
                        return null;
                }
            }
            catch (Exception e)
            {
                ReplicatedTableLogger.LogError("Exception from replicaIndex:{0},\nPK:{1},\nRK:{2},\n{3}", index, partitionKey, rowKey, e);

                return null;
            }

            // Convert to RTable
            var retrievedResult = new TableResult
            {
                Result = resp.Value,
                Etag = resp.HasValue ? resp.Value.ETag.ToString() : null,
                HttpStatusCode = (int)resp?.GetRawResponse().Status
            };
            IReplicatedTableEntity readRow = ConvertToIReplicatedTableEntity(retrievedResult);
            if (readRow == null)
            {
                // retrievedResult.Result = null (set by ConvertToIReplicatedTableEntity call)
                return retrievedResult;
            }

            try
            {
                ThrowIfViewIdNotConsistent(txnView.ViewId, readRow._rtable_ViewId);
            }
            catch (ReplicatedTableStaleViewException)
            {
                // Assert(ex.ErrorCode == ReplicatedTableViewErrorCodes.ViewIdSmallerThanEntryViewId);
                if (this._configurationWrapper.IsIgnoreHigherViewIdRows())
                {
                    // Treat as NotFound!
                    return new TableResult() { Result = null, Etag = null, HttpStatusCode = (int)HttpStatusCode.NotFound };
                }

                throw;
            }

            return retrievedResult;
        }

        /// <summary>
        /// Check consistency of viewId between the currentView and existing entity.
        /// </summary>
        /// <param name="txnViewId"></param>
        /// <param name="rowViewId"></param>
        internal static void ThrowIfViewIdNotConsistent(long txnViewId, long rowViewId)
        {
            if (txnViewId < rowViewId)
            {
                var msg = string.Format("current _rtable_ViewId {0} is smaller than _rtable_ViewId of existing row {1}", txnViewId, rowViewId);
                throw new ReplicatedTableStaleViewException(ReplicatedTableViewErrorCodes.ViewIdSmallerThanEntryViewId, msg);
            }
        }

        /// <summary>
        /// Convert a retrieved TableEntity to DynamicReplicatedTableEntity.
        /// Otherwise, set retrievedResult.Result = null and return null.
        /// </summary>
        private IReplicatedTableEntity ConvertToIReplicatedTableEntity(TableResult retrievedResult)
        {
            // If the generic TableOperation.Retrive<T>() is used, the returned result is of IReplicatedTableEntity type
            if (retrievedResult.Result is IReplicatedTableEntity)
            {
                return (IReplicatedTableEntity)retrievedResult.Result;
            }

            // If the non-generic TableOperation.Retrive() is used, the returned result is of TableEntity type
            if (retrievedResult.Result is TableEntity)
            {
                // Convert to an equivalent DynamicReplicatedTableEntity
                TableEntity tableEntity = (TableEntity)retrievedResult.Result;

                IReplicatedTableEntity readRow = this._configurationWrapper.IsConvertToRTableMode()
                                                    ? new InitDynamicReplicatedTableEntity(tableEntity)
                                                    : new DynamicReplicatedTableEntity(tableEntity);
                
                IDictionary<string, object> props = new Dictionary<string, object>();
                foreach (var key in tableEntity.Keys)
                {
                    props[key] = tableEntity[key];
                }
                readRow.ReadEntity(props);
                retrievedResult.Result = readRow;
                return readRow;
            }

            // Unknown type !
            retrievedResult.Result = null;
            return null;
        }

        //
        // Flush2PC protocol: Executes chain 2PC protocol after a row is updated and locked at the head.
        //
        private TableResult Flush2PC(View txnView, IReplicatedTableEntity row, string etagOnHead = null)
        {
            TableResult result = null;
            string[] eTagsStrings = new string[txnView.Chain.Count];

            if (etagOnHead != null)
            {
                eTagsStrings[0] = etagOnHead;
            }

            result = FlushPreparePhase(txnView, row, eTagsStrings);
            if (result == null)
            {
                return null;
            }

            return FlushCommitPhase(txnView, row, eTagsStrings);
        }

        private TableResult FlushPreparePhase(View txnView, IReplicatedTableEntity row, string[] eTagsStrings)
        {
            TableResult result = new TableResult() { HttpStatusCode = (int)HttpStatusCode.OK };

            // PREPARE PHASE: Uses chain replication to prepare replicas starting from "head+1" to 
            // "tail" sequentially
            for (int index = 1; index <= txnView.TailIndex; index++)
            {
                result = InsertUpdateOrDeleteRow(txnView, index, row, eTagsStrings[index]);

                if (result == null ||
                    (result.HttpStatusCode != (int)HttpStatusCode.OK && result.HttpStatusCode != (int)HttpStatusCode.NoContent))
                {
                    // Failed in the middle, abort with error
                    ReplicatedTableLogger.LogError(
                        "F2PC: Failed during prepare phase in 2PC at replica with index: {0} for row with row key: {1} with result: {2}",
                        index,
                        row.RowKey,
                        (result != null) ? result.HttpStatusCode.ToString() : "null");

                    return null;
                }

                // Cache the Etag for the commit phase
                eTagsStrings[index] = result.Etag;
            }

            return result;
        }

        private TableResult FlushCommitPhase(View txnView, IReplicatedTableEntity row, string[] eTagStrings)
        {
            TableResult result = null;

            // COMMIT PHASE: Commits the replicas in the reverse order starting from the tail replica
            for (int index = txnView.TailIndex; index >= 0; index--)
            {
                row._rtable_RowLock = false;
                result = InsertUpdateOrDeleteRow(txnView, index, row, eTagStrings[index]);

                // It is possible that UpdateInsertOrDeleteRow() returns result.Result = null
                // It happens when the Head entry is _rtable_Tombstone and the Tail entry is gone already.
                // So, just check for "result == null" and return error
                if (result == null ||
                    (result.HttpStatusCode != (int)HttpStatusCode.OK && result.HttpStatusCode != (int)HttpStatusCode.NoContent))
                {
                    // Failed in the middle, abort with error
                    ReplicatedTableLogger.LogError(
                        "F2PC: Failed during commit phase in 2PC at replica with index: {0} for row with PK:{1} RK:{2}",
                        index,
                        row.PartitionKey,
                        row.RowKey);
                    break;
                }
            }

            return result;
        }

        //
        // ReadModifyWriteRow : Reads an existing row (or creates a new if it does not exist) and 
        //                      updates it with the new data using existing row's etag as the pre-condintion
        //                      to prevent race conditions from multiple writers.
        //  Returns the new table result if it succeeds. Otherwise, it returns null
        //
        private TableResult InsertUpdateOrDeleteRow(View txnView, int index, IReplicatedTableEntity row, string Etag)
        {
            // Read the row before updating it
            TableServiceClient tableClient = txnView[index];
            TableResult result; 

            // If the Etag is supplied, this is an update or delete based on existing eTag
            // no need to retrieve
            if (!string.IsNullOrEmpty(Etag))
            {
                row.ETag = new ETag(Etag);
                result = UpdateOrDeleteRow(tableClient, row);
                ValidateTxnView(txnView);
                return result;
            }

            if (row._rtable_RowLock == true && row._rtable_Operation == GetTableOperation(TableOperationType.Insert))
            {
                // if the operation is insert and it is in the prepare phase, just insert the data
                result = InsertRow(tableClient, row);
                ValidateTxnView(txnView);
                return result;
            }

            // TODO: this read seems redundant. remove it.
            // If Etag is not supplied, retrieve the row first before writing
            TableResult retrievedResult = RetrieveFromReplica(txnView, index, row.PartitionKey, row.RowKey);
            if (retrievedResult == null || retrievedResult.HttpStatusCode == (int)HttpStatusCode.ServiceUnavailable)
            {
                // If retrieve fails for some reason then return "service unavailable - 503 " error code
                ReplicatedTableLogger.LogError(
                    "IUD: Failed to access replica with index: {0} when reading a row with row key: {1}",
                    index,
                    row.RowKey);
                return null;
            }

            if (retrievedResult.HttpStatusCode == (int)HttpStatusCode.OK)
            {
                // Row is present, overwrite the row
                row.ETag = new ETag(retrievedResult.Etag);
                result = UpdateOrDeleteRow(tableClient, row);
                ValidateTxnView(txnView);
                return result;
            }

            if (retrievedResult.HttpStatusCode == (int)HttpStatusCode.NotFound || retrievedResult.Result == null)
            {
                // Row is not present, insert the row
                // **Except** when the row is being deleted
                // during recovery by another client, the operation type might not be delete, 
                // check for the presence of tombstone during commit phase
                if (row._rtable_Operation == GetTableOperation(TableOperationType.Delete) || (row._rtable_Tombstone && row._rtable_RowLock == false))
                {
                    return retrievedResult;
                }

                // non-delete operation: Row is not present, create the row and return the result
                result = InsertRow(tableClient, row);
                ValidateTxnView(txnView);
                if ((result == null) ||
                    (result.HttpStatusCode != (int)HttpStatusCode.NoContent) || (result.Result == null))
                {
                    ReplicatedTableLogger.LogError(
                        "IUD: Failed at replica with index: {0} when inserting a new row with row key: {1}", index,
                        row.RowKey);
                    return null;
                }
                return result;
            }

            ReplicatedTableLogger.LogError("IUD: Failed to access replica with index: {0}, error code = {1}", index,
                retrievedResult.HttpStatusCode);
            return null;
        }

        private TableResult InsertRow(TableServiceClient tableClient, IReplicatedTableEntity row)
        {
            TableResult result;

            try
            {
                TableClient table = tableClient.GetTableClient(TableName);
                var entity = ToTableEntity(row);
                var resp = table.AddEntity(entity);
                result = TableResult.ConvertResponseToTableResult(resp, entity);
            }
            catch (RequestFailedException ex)
            {
                ReplicatedTableLogger.LogError("TryInsertRow:Error: RequestFailedException {0}", ex);
                result = new TableResult() { Result = null, Etag = null, HttpStatusCode = (int)ex.Status };
                return result;
            }
            catch (Exception e)
            {
                ReplicatedTableLogger.LogError("TryInsertRow:Error: exception {0}", e);
                return null;
            }

            return result;
        }

        private TableResult UpdateOrDeleteRow(TableServiceClient tableClient, IReplicatedTableEntity row)
        {
            Response resp;
            var entity = ToTableEntity(row);

            try
            {
                TableClient table = tableClient.GetTableClient(TableName);
                if ((row._rtable_Tombstone == true) && (row._rtable_RowLock == false))
                {
                    // For delete operations, call the delete operation if it is being committed
                    resp = table.DeleteEntity(row.PartitionKey, row.RowKey, row.ETag);
                }
                else if (row._rtable_Operation == GetTableOperation(TableOperationType.Merge))
                {
                    // For Merge operations, call merge row for both phases
                    resp = table.UpdateEntity(entity, entity.ETag, TableUpdateMode.Merge);
                }
                else
                {
                    resp = table.UpdateEntity(entity, entity.ETag, TableUpdateMode.Replace);
                }
            }
            catch (RequestFailedException e)
            {
                ReplicatedTableLogger.LogError("UpdateOrDeleteRow(): {0}", e);

                return new TableResult()
                {
                    Result = null,
                    Etag = null,
                    HttpStatusCode = e.Status
                };
            }
            catch (Exception e)
            {
                ReplicatedTableLogger.LogError("UpdateOrDeleteRow(): Error: exception {0}", e);
                return null;
            }

            return TableResult.ConvertResponseToTableResult(resp, entity);
        }

        public TableServiceClient GetReplicaTableClient(int index)
        {
            return CurrentView[index];
        }

        public TableServiceClient GetTailTableClient()
        {
            return CurrentView[CurrentView.TailIndex];
        }

        protected void Cleanup(String partitionkey, String rowkey)
        {
            throw new NotImplementedException();
        }

        private bool ValidateAndUnlock(IEnumerable<TableTransactionAction> inBatch, IList<TableResult> results, bool unlock)
        {
            IEnumerator<TableResult> iter = (results != null) ? results.GetEnumerator() : null;
            bool result2;
            if (iter == null || inBatch.Count() != results.Count)
            {
                result2 = false;
            }
            else
            {
                IEnumerator<TableTransactionAction> enumerator = inBatch.GetEnumerator();
                while (enumerator.MoveNext() && iter.MoveNext())
                {
                    TableResult result = iter.Current;
                    if (result == null || (result.HttpStatusCode != 201 && result.HttpStatusCode != 204))
                    {
                        result2 = false;
                        return result2;
                    }
                    if (unlock)
                    {
                        TableTransactionAction operation = enumerator.Current;
                        IReplicatedTableEntity row = (IReplicatedTableEntity)operation.Entity;
                        row._rtable_RowLock = false;
                        row.ETag = new ETag(result.Etag);
                    }
                }
                result2 = true;
            }
            return result2;
        }

        private IList<TableResult> RunBatch(TableServiceClient tableClient, IEnumerable<TableTransactionAction> batch, bool unlock)
        {
            IList<TableResult> result;
            try
            {
                TableClient table = tableClient.GetTableClient(this.TableName);
                var resp = table.SubmitTransaction(TransformTableTransactionEntityToTableEntity(batch));
                var results = TransactionResponseToTableResultList(resp, batch);
                if (results == null ||
                    !this.ValidateAndUnlock(batch, results, unlock))
                {
                    result = null;
                }
                else
                {
                    result = results;
                }
            }
            catch (Exception e)
            {
                ReplicatedTableLogger.LogError("RunBatch: exception {0}", e);
                result = null;
            }
            return result;
        }

        private void MergeResults(IList<TableResult> results1, IList<TableResult> results2)
        {
            IEnumerator<TableResult> enumerator = results1.GetEnumerator();
            while (enumerator.MoveNext())
            {
                results2.Add(enumerator.Current);
            }
        }

        private void MergeOperations(IList<TableTransactionAction> batch1, IList<TableTransactionAction> batch2)
        {
            IEnumerator<TableTransactionAction> enumerator = batch1.GetEnumerator();
            while (enumerator.MoveNext())
            {
                if (!batch2.Contains(enumerator.Current))
                {
                    batch2.Add(enumerator.Current);
                }
            }
        }

        private IList<TableResult> RunBatchSplit(TableServiceClient tableClient, IList<TableTransactionAction> batch, bool unlock,
            ref IList<TableTransactionAction> outBatch)
        {
            int batchSize = batch.Count;
            var leftBatch = new List<TableTransactionAction>();
            var rightBatch = new List<TableTransactionAction>();
            IEnumerator<TableTransactionAction> enumerator = batch.GetEnumerator();
            IList<TableResult> result;
            if (batchSize == 0)
            {
                result = null;
            }
            else
            {
                IList<TableResult> leftResults;
                if ((leftResults = this.RunBatch(tableClient, batch, unlock)) != null)
                {
                    this.MergeOperations(batch, outBatch);
                    result = leftResults;
                }
                else
                {
                    int i = 0;
                    while (i < batchSize && enumerator.MoveNext())
                    {
                        if (i < batchSize / 2)
                        {
                            leftBatch.Add(enumerator.Current);
                        }
                        else
                        {
                            rightBatch.Add(enumerator.Current);
                        }
                        i++;
                    }
                    if ((leftResults = this.RunBatch(tableClient, leftBatch, unlock)) !=
                        null)
                    {
                        this.MergeOperations(leftBatch, outBatch);
                        IList<TableResult> rightResults;
                        if (
                            (rightResults =
                                this.RunBatch(tableClient, rightBatch, unlock)) !=
                            null)
                        {
                            this.MergeOperations(rightBatch, outBatch);
                            this.MergeResults(rightResults, leftResults);
                        }
                        else
                        {
                            if (
                                (rightResults =
                                    this.RunBatchSplit(tableClient, rightBatch, unlock, ref outBatch)) != null)
                            {
                                this.MergeOperations(rightBatch, outBatch);
                                this.MergeResults(rightResults, leftResults);
                            }
                            else
                            {
                                if (rightBatch.Count > 0)
                                {
                                    result = leftResults;
                                    return result;
                                }
                            }
                        }
                    }
                    else
                    {
                        if (
                            (leftResults =
                                this.RunBatchSplit(tableClient, leftBatch, unlock, ref outBatch)) != null)
                        {
                            this.MergeOperations(leftBatch, outBatch);
                            IList<TableResult> rightResults;
                            if (
                                (rightResults =
                                    this.RunBatch(tableClient, rightBatch, unlock)) !=
                                null)
                            {
                                this.MergeOperations(rightBatch, outBatch);
                                this.MergeResults(rightResults, leftResults);
                            }
                            else
                            {
                                if (
                                    (rightResults =
                                        this.RunBatchSplit(tableClient, rightBatch, unlock, ref outBatch)) != null)
                                {
                                    this.MergeOperations(rightBatch, outBatch);
                                    this.MergeResults(rightResults, leftResults);
                                }
                                else
                                {
                                    if (rightBatch.Count > 0)
                                    {
                                        result = leftResults;
                                        return result;
                                    }
                                }
                            }
                        }
                        else
                        {
                            if (leftBatch.Count > 0)
                            {
                                result = null;
                                return result;
                            }
                        }
                    }
                    result = leftResults;
                }
            }
            return result;
        }

        /// <summary>
        /// Reapir a newly added replica.
        /// </summary>
        /// <param name="viewIdToRecoverFrom"></param>
        /// <param name="unfinishedOps"></param>
        /// <param name="maxBatchSize"></param>
        /// <param name="parallelizationDegree"></param>
        /// <returns></returns>
        public ReconfigurationStatus RepairTable(int viewIdToRecoverFrom, IEnumerable<TableTransactionAction> unfinishedOps,
            long maxBatchSize = 100L, int parallelizationDegree = 1, RepairRowDelegate filter = null)
        {
            using (new StopWatchInternal(this.TableName, "RepairTable", this._configurationWrapper))
            {
                ReconfigurationStatus status = ReconfigurationStatus.SUCCESS;
                if (this._configurationWrapper.IsViewStable())
                {
                    ReplicatedTableLogger.LogWarning("Returning Success for Table : {0} as the view is stable", this.TableName);
                    return ReconfigurationStatus.SUCCESS;
                }

                View txnView = CurrentView;
                ValidateTxnView(txnView);

                TableServiceClient writeHeadClient = txnView[txnView.WriteHeadIndex];
                TableClient writeHeadTable = writeHeadClient.GetTableClient(this.TableName);

                // TO DO: Optimization
                // Now we are relying on caller to pass the viewIdToRecoverFrom. Instead, we can find the viewIdToRecoverFrom of the old replica 
                //(when it was part of the stable view) and only get the entries that are greater than the viewIdToRecoverFrom.
                // However, we will miss the entries that are deleted since the old left the view.
                // In order to delete those entries, we need to keep the tombstones around when a replica
                // leaves the system. We have two options to store the tombstones.
                //  1. Store the tombstone entries in a seperate table/blob, whenever an entry is deleted.
                //  2. Store the tombstone entries in other rows of the existing table in a separate column.

                writeHeadTable.CreateIfNotExists();

                TableServiceClient readHeadTableClient = txnView[txnView.ReadHeadIndex];
                TableClient readHeadTable = readHeadTableClient.GetTableClient(this.TableName);

                DateTime startTime = DateTime.UtcNow;
                var query = readHeadTable.Query<DynamicReplicatedTableEntity>(ent =>
                    ent._rtable_ViewId >= viewIdToRecoverFrom);

                ReplicatedTableLogger.LogInformational("RepairReplica: Parallelization Degree : {0}",
                        parallelizationDegree);

                if (parallelizationDegree <= 1)
                {
                    ReplicatedTableLogger.LogInformational("RepairReplica: Non Parallel Path");
                    foreach (var entry in query)
                    {
                        RepairRowWithFilter(filter, entry, ref status);
                    }
                }
                else
                {
                    ReplicatedTableLogger.LogInformational("RepairReplica: Parallelization Path");

                    // We set parent thread onto call context to be used by every child.
                    var parentThreadId = Thread.CurrentThread.ManagedThreadId;
                    CallContext.LogicalSetData(ParentThreadCallContextKey, parentThreadId);

                    Parallel.ForEach(query, new ParallelOptions { MaxDegreeOfParallelism = parallelizationDegree }, 
                        entry => RepairRowWithFilter(filter, entry, ref status));

                    //Clearing the call context being pessimistic
                    CallContext.FreeNamedDataSlot(ParentThreadCallContextKey);

                }

                // now find any entries that are in the write view but not in the read view
                query = writeHeadTable.Query<DynamicReplicatedTableEntity>(ent =>
                    ent._rtable_ViewId < txnView.ViewId);

                foreach (DynamicReplicatedTableEntity extraEntity in query)
                {
                    ReplicatedTableLogger.LogWarning("RepairReplica: deleting entity pk: {0}, rk: {1}",
                        extraEntity.PartitionKey, extraEntity.RowKey);

                    TableResult result = RepairRow(extraEntity.PartitionKey, extraEntity.RowKey, null);

                    if (result == null ||
                        (result.HttpStatusCode != (int) HttpStatusCode.OK &&
                         result.HttpStatusCode != (int) HttpStatusCode.NoContent))
                    {
                        ReplicatedTableLogger.LogError("RepairReplica: RepairRow Failed: Pk: {0}, Rk: {1}", 
                            extraEntity.PartitionKey, extraEntity.RowKey);
                        status = ReconfigurationStatus.PARTIAL_FAILURE;
                    }
                }

                ReplicatedTableLogger.LogInformational("RepairTable: took {0} for table : {1}", DateTime.UtcNow - startTime, this.TableName);

                return status;
            }
        }

        private void RepairRowWithFilter(RepairRowDelegate filter, DynamicReplicatedTableEntity entry, ref ReconfigurationStatus status)
        {
            // By default all rows are taken
            if (filter == null)
            {
                filter = (row => RepairRowActionType.RepairRow);
            }

            // What to do with this row ?
            RepairRowActionType action = filter(entry);
            
            switch (action)
            {
                case RepairRowActionType.RepairRow:
                    ReplicatedTableLogger.LogWarning("RepairReplica: RepairRow: Pk: {0}, Rk: {1}",
                        entry.PartitionKey, entry.RowKey);
                    break;

                case RepairRowActionType.SkipRow:
                    ReplicatedTableLogger.LogWarning("RepairReplica: SkipRow: Pk: {0}, Rk: {1}",
                        entry.PartitionKey, entry.RowKey);
                    return;

                case RepairRowActionType.InvalidRow:
                default:
                    status = ReconfigurationStatus.PARTIAL_FAILURE;
                    ReplicatedTableLogger.LogWarning("RepairReplica: InvalidRow: Pk: {0}, Rk: {1}",
                        entry.PartitionKey, entry.RowKey);
                    return;
            }

            TableResult result = RepairRow(entry.PartitionKey, entry.RowKey, null);

            if (result == null ||
                (result.HttpStatusCode != (int)HttpStatusCode.OK &&
                 result.HttpStatusCode != (int)HttpStatusCode.NoContent))
            {
                ReplicatedTableLogger.LogError("RepairReplica: RepairRow Failed: Pk: {0}, Rk: {1}",
                        entry.PartitionKey, entry.RowKey);
                status = ReconfigurationStatus.PARTIAL_FAILURE;
            }
        }

        private void FlushRecoveryBatch(IList<TableTransactionAction> unfinishedOps, long maxBatchSize, ref ReconfigurationStatus status,
            TableServiceClient newTableClient, TableServiceClient headTableClient, IList<TableTransactionAction> batchHead,
            IList<TableTransactionAction> batchNewReplica, ref long batchCount, ref Guid batchId)
        {
            IList<TableTransactionAction> outBatch = new List<TableTransactionAction>();

            // Lock the head the replica
            if (this.RunBatchSplit(headTableClient, batchHead, true, ref outBatch) != null &&
                outBatch.Count == batchHead.Count)
            {
                outBatch.Clear();
                // Copy rows to the new replica
                if (this.RunBatchSplit(newTableClient, batchNewReplica, false, ref outBatch) != null &&
                    outBatch.Count == batchNewReplica.Count)
                {
                    outBatch.Clear();
                    // Unlock the head replica
                    if (this.RunBatchSplit(headTableClient, batchHead, false, ref outBatch) == null ||
                        outBatch.Count != batchHead.Count)
                    {
                        status = ReconfigurationStatus.UNLOCK_FAILURE;
                    }
                }
                else
                {
                    // Copy failed at the new replica, unlock the head first 
                    IList<TableTransactionAction> newOutBatch = new List<TableTransactionAction>();
                    if (this.RunBatchSplit(headTableClient, batchHead, false, ref newOutBatch) == null)
                    {
                        // Oops, unlock failed
                        status |= ReconfigurationStatus.UNLOCK_FAILURE;
                    }

                    // Copy unfinished operations in the head to the unfinished batch
                    IEnumerator<TableTransactionAction> enumerator = batchNewReplica.GetEnumerator();
                    while (enumerator.MoveNext())
                    {
                        if (!outBatch.Contains(enumerator.Current))
                        {
                            unfinishedOps.Add(enumerator.Current);
                            status |= ReconfigurationStatus.PARTIAL_FAILURE;
                        }
                    }
                }
            }
            else
            {
                // Failed when locking the head, unlock the head.
                IList<TableTransactionAction> newOutBatch = new List<TableTransactionAction>();
                if (outBatch.Count > 0 &&
                    this.RunBatchSplit(headTableClient, outBatch, false, ref newOutBatch) == null)
                {
                    status |= ReconfigurationStatus.UNLOCK_FAILURE;
                }
                status |= ReconfigurationStatus.PARTIAL_FAILURE;
                unfinishedOps.Concat(batchNewReplica);
            }
            batchCount = 0L;
            batchId = Guid.NewGuid();
            batchHead.Clear();
            batchNewReplica.Clear();
        }

        internal static string GetTableOperation(TableOperationType tableOperationType)
        {
            switch (tableOperationType)
            {
                case TableOperationType.Insert:
                    return "Insert";
                case TableOperationType.Delete:
                    return "Delete";
                case TableOperationType.Replace:
                    return "Replace";
                case TableOperationType.Merge:
                    return "Merge";
                case TableOperationType.InsertOrReplace:
                    return "InsertOrReplace";
                case TableOperationType.InsertOrMerge:
                    return "InsertOrMerge";
                case TableOperationType.Retrieve:
                    return "Retrieve";
            }

            return "null";
        }

        private static string GetTableOperation(TableTransactionActionType tableTransactionActionType)
        {
            switch (tableTransactionActionType)
            {
                case TableTransactionActionType.Add:
                    return "Insert";
                case TableTransactionActionType.Delete:
                    return "Delete";
                case TableTransactionActionType.UpdateReplace:
                    return "Replace";
                case TableTransactionActionType.UpdateMerge:
                    return "Merge";
                case TableTransactionActionType.UpsertReplace:
                    return "InsertOrReplace";
                case TableTransactionActionType.UpsertMerge:
                    return "InsertOrMerge";
            }

            return "null";
        }

        /// <summary>
        /// Repairs a row coditionally based on filter
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="rowKey"></param>
        /// <param name="filter">Delegate used to check if the row should be repaired or not</param>
        /// <returns></returns>
        public ReconfigurationStatus RepairRowWithFilter(string partitionKey, string rowKey, RepairRowDelegate filter = null)
        {
            using (new StopWatchInternal(this.TableName, "RepairRowWithFilter", _configurationWrapper))
            {
                View txnView = CurrentView;
                ValidateTxnView(txnView);

                TableServiceClient readHeadTableClient = txnView[txnView.ReadHeadIndex];
                TableClient readHeadTable = readHeadTableClient.GetTableClient(this.TableName);
                DynamicReplicatedTableEntity entity = readHeadTable.Query<DynamicReplicatedTableEntity>(ent =>
                    ent.PartitionKey == partitionKey && ent.RowKey == rowKey).FirstOrDefault();
                if (entity == null)
                {
                    ReplicatedTableLogger.LogError("RepairRowWithFilter: Entity with PartitionKey {0} and RowKey {1} does not exist", partitionKey, rowKey);
                    return ReconfigurationStatus.FAILURE;
                }

                ReconfigurationStatus status = ReconfigurationStatus.SUCCESS;
                RepairRowWithFilter(filter, entity, ref status);
                return status;
            }
        }

        /// <summary>
        /// Repair a single row from the current read view to the write view.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="rowKey"></param>
        /// <param name="existingRow">if existing row is specified, then the row is already locked in read view</param>
        /// <returns></returns>
        public TableResult RepairRow(string partitionKey, string rowKey, IReplicatedTableEntity existingRow)
        {
            using (new StopWatchInternal(this.TableName, "RepairRow", _configurationWrapper))
            {
                var rnd = new Random((int)DateTime.UtcNow.Ticks);
                int retryLimit = RETRY_LIMIT;
                Func<bool> RetryIf = RetryPolicy.RetryWithDelayIf(() => rnd.Next(100, 300), () => --retryLimit > 0);
                TableResult result;
                do
                {
                    View txnView = CurrentView;
                    ValidateTxnView(txnView);
                    result = RepairRowInternal(txnView, partitionKey, rowKey, existingRow);
                }
                while (result != null && result.HttpStatusCode != (int)HttpStatusCode.OK && result.HttpStatusCode != (int)HttpStatusCode.NoContent && RetryIf());
                return result;
            }
        }

        private TableResult RepairRowInternal(View txnView, string partitionKey, string rowKey, IReplicatedTableEntity existingRow)
        {
            TableResult result = new TableResult() { HttpStatusCode = (int)HttpStatusCode.OK };

            if (txnView.IsStable)
            {
                return result;
            }

            // read from the head of the read view
            TableResult readHeadResult = RetrieveFromReplica(txnView, txnView.ReadHeadIndex, partitionKey, rowKey);
            if (readHeadResult == null || readHeadResult.HttpStatusCode == (int)HttpStatusCode.ServiceUnavailable)
            {
                // If retrieve fails for some reason then return "service unavailable - 503 " error code
                return new TableResult()
                {
                    Result = null,
                    Etag = null,
                    HttpStatusCode = (int)HttpStatusCode.ServiceUnavailable
                };
            }

            // read from the head of the write view
            TableResult writeHeadResult = RetrieveFromReplica(txnView, txnView.WriteHeadIndex, partitionKey, rowKey);
            if (writeHeadResult == null || writeHeadResult.HttpStatusCode == (int)HttpStatusCode.ServiceUnavailable)
            {
                // If retrieve fails for some reason then return "service unavailable - 503 " error code
                return new TableResult()
                {
                    Result = null,
                    Etag = null,
                    HttpStatusCode = (int)HttpStatusCode.ServiceUnavailable
                };
            }

            IReplicatedTableEntity writeViewEntity = null;
            if (writeHeadResult.HttpStatusCode == (int)HttpStatusCode.OK && writeHeadResult.Result != null)
            {
                writeViewEntity = (IReplicatedTableEntity)writeHeadResult.Result;
                if (writeViewEntity._rtable_ViewId >= txnView.GetReplicaInfo(txnView.WriteHeadIndex).ViewInWhichAddedToChain)
                {
                    // nothing to repair in this case.
                    return result;
                }
            }

            // if row does not exist in the read view, delete it from the write view
            if (readHeadResult.Result == null || readHeadResult.HttpStatusCode == (int)HttpStatusCode.NoContent)
            {
                if (writeViewEntity != null)
                {
                    writeViewEntity._rtable_Tombstone = true;
                    writeViewEntity._rtable_RowLock = false;

                    // delete row from the write view
                    result = UpdateOrDeleteRow(txnView[txnView.WriteHeadIndex], writeViewEntity);
                    ValidateTxnView(txnView);
                    ReplicatedTableLogger.LogWarning("RepairRow: attempt to delete from write head returned: {0}", result.HttpStatusCode);
                    return result;
                }
            }

            if (readHeadResult.HttpStatusCode != (int)HttpStatusCode.OK || readHeadResult.Result == null)
            {
                ReplicatedTableLogger.LogError("RepairRow: unexpected result on the read view head: {0}", readHeadResult.HttpStatusCode);
                return readHeadResult;
            }

            IReplicatedTableEntity readHeadEntity = ConvertToIReplicatedTableEntity(readHeadResult);
            bool readHeadLocked = readHeadEntity._rtable_RowLock;
            bool readHeadLockExpired = (readHeadEntity._rtable_LockAcquisition + this._configurationWrapper.GetLockTimeout() < DateTime.UtcNow);

            if (readHeadLocked && !readHeadLockExpired)
            {
                ReplicatedTableLogger.LogError("RepairRow: skip as row is locked in read head.");

                return new TableResult()
                {
                    Result = null,
                    Etag = null,
                    HttpStatusCode = (int)HttpStatusCode.Conflict
                };
            }

            // take a lock on the read view entity unless the entity is already locked
            readHeadEntity._rtable_RowLock = true;
            readHeadEntity._rtable_ViewId = txnView.ViewId;
            readHeadEntity._rtable_LockAcquisition = DateTime.UtcNow;
            readHeadEntity._rtable_Operation = GetTableOperation(TableOperationType.Replace);

            result = UpdateOrDeleteRow(txnView[txnView.ReadHeadIndex], readHeadEntity);
            ValidateTxnView(txnView);
            if (result == null)
            {
                ReplicatedTableLogger.LogError("RepairRow: failed to take lock on read head.");
                return null;
            }

            if (result.HttpStatusCode != (int)HttpStatusCode.OK && result.HttpStatusCode != (int)HttpStatusCode.NoContent)
            {
                ReplicatedTableLogger.LogError("RepairRow: failed to take lock on read head: {0}", result.HttpStatusCode);
                return null;
            }

            string readHeadEtag = result.Etag;

            // now copy the row to the write head
            if (writeViewEntity != null)
            {
                readHeadEntity.ETag = writeViewEntity.ETag;
            }
            else
            {
                readHeadEntity.ETag = new ETag();
            }

            result = InsertUpdateOrDeleteRow(txnView, txnView.WriteHeadIndex, readHeadEntity, readHeadEntity.ETag.ToString());
            if (result == null)
            {
                ReplicatedTableLogger.LogError("RepairRow: failed to write entity on the write view.");
                return null;
            }

            if (result.HttpStatusCode != (int)HttpStatusCode.OK && result.HttpStatusCode != (int)HttpStatusCode.NoContent)
            {
                ReplicatedTableLogger.LogError("RepairRow: failed to write to write head: {0}", result.HttpStatusCode);
                return result;
            }

            string writeHeadEtagForCommit = result.Etag;

            if (!readHeadLocked)
            {
                readHeadEntity._rtable_RowLock = false;
                readHeadEntity.ETag = new ETag(readHeadEtag);
                result = UpdateOrDeleteRow(txnView[txnView.ReadHeadIndex], readHeadEntity);
                ValidateTxnView(txnView);
                if (result == null)
                {
                    ReplicatedTableLogger.LogError("RepairRow: failed to unlock read view.");
                    return null;
                }


                if (result.HttpStatusCode != (int)HttpStatusCode.OK &&
                    result.HttpStatusCode != (int)HttpStatusCode.NoContent)
                {
                    ReplicatedTableLogger.LogError("RepairRow: failed to unlock read head: {0}", result.HttpStatusCode);
                    return result;
                }

                ReplicatedTableLogger.LogInformational("RepairRow: read head unlock result: {0}", result.HttpStatusCode);

                readHeadEntity._rtable_RowLock = false;
                readHeadEntity.ETag = new ETag(writeHeadEtagForCommit);
                result = UpdateOrDeleteRow(txnView[txnView.WriteHeadIndex], readHeadEntity);
                ValidateTxnView(txnView);

                if (result == null)
                {
                    ReplicatedTableLogger.LogError("RepairRow: failed to unlock write view.");
                    return null;
                }

                if (result.HttpStatusCode != (int)HttpStatusCode.OK &&
                    result.HttpStatusCode != (int)HttpStatusCode.NoContent)
                {
                    ReplicatedTableLogger.LogError("RepairRow: failed to unlock write head: {0}", result.HttpStatusCode);
                    return result;
                }

                ReplicatedTableLogger.LogInformational("RepairRow: write head unlock result: {0}", result.HttpStatusCode);
            }

            // if the lock on the read head had expired, flush this row
            else if (readHeadLockExpired)
            {
                readHeadEntity._rtable_RowLock = true;
                result = Flush2PC(txnView, readHeadEntity, writeHeadEtagForCommit);
                if (result == null)
                {
                    ReplicatedTableLogger.LogError("RepairRow: failed flush2pc on expired lock.");
                }
            }

            return result;
        }

        /// <summary>
        /// Call this function to convert all the remaining XStore Table entities to ReplicatedTable entities.
        /// "remaining" XStore entities are those with _rtable_ViewId == 0.
        /// </summary>
        /// <param name="successCount"></param>
        /// <param name="skippedCount"></param>
        /// <param name="failedCount"></param>
        public void ConvertXStoreTable(
            out long successCount,
            out long skippedCount,
            out long failedCount)
        {
            successCount = 0;
            skippedCount = 0;
            failedCount = 0;

            View txnView = CurrentView;
            ValidateTxnView(txnView);

            if (this._configurationWrapper.IsConvertToRTableMode() == false)
            {
                throw new InvalidOperationException("ConvertXStoreTable() API is NOT supported when ReplicatedTable is NOT in ConvertToRTable.");
            }

            int tailIndex = txnView.TailIndex;

            DateTime startTime = DateTime.UtcNow;
            ReplicatedTableLogger.LogInformational("ConvertXStoreTable() started {0}", startTime);

            TableServiceClient tailTableClient = txnView[tailIndex];
            TableClient tailTable = tailTableClient.GetTableClient(this.TableName);

            var query = tailTable.Query<InitDynamicReplicatedTableEntity>();

            using (IEnumerator<InitDynamicReplicatedTableEntity> enumerator = query.GetEnumerator())
            {
                while (enumerator.MoveNext())
                {
                    InitDynamicReplicatedTableEntity entity = enumerator.Current;
                    if (entity._rtable_ViewId != 0)
                    {
                        ReplicatedTableLogger.LogInformational("Skipped XStore entity with Partition={0} Row={1}", entity.PartitionKey, entity.RowKey);
                        skippedCount++;
                        continue;
                    }

                    // _rtable_ViewId = 0 means that the entity has not been operated on since the XStore Table was converted to ReplicatedTable.
                    // So, convert it manually now.
                    entity._rtable_ViewId = txnView.ViewId;
                    entity._rtable_Version = 1;
                    try
                    {
                        tailTable.UpdateEntity(ToTableEntity(entity), entity.ETag, TableUpdateMode.Replace);
                        successCount++;
                        ReplicatedTableLogger.LogInformational("Converted XStore entity with Partition={0} Row={1}", entity.PartitionKey, entity.RowKey);
                    }
                    catch (Exception ex)
                    {
                        failedCount++;
                        ReplicatedTableLogger.LogError("Exception when converting XStore entity with Partition={0} Row={1}. Ex = {2}",
                            entity.PartitionKey, entity.RowKey, ex.ToString());
                    }
                }
            }
            DateTime endTime = DateTime.UtcNow;
            ReplicatedTableLogger.LogInformational("ConvertXStoreTable() finished {0}. Time took to convert = {1}", endTime, endTime - startTime);
            ReplicatedTableLogger.LogInformational("successCount={0} skippedCount={1} failedCount={2}", successCount, skippedCount, failedCount);
        }

        private bool IsEtagMismatch(IReplicatedTableEntity row, IReplicatedTableEntity currentRow)
        {
            bool mismatch = IsEtagMismatchInternal(row, currentRow);

            if (this._configurationWrapper.IsConvertToRTableMode() == false)
            {
                return mismatch;
            }

            // Reading a row via XStore lib. then Saving it via RTable lib. results in ETag mismatch i.e. PreconditionFailed
            //
            // Such case can happen even when ConvertMode == False.
            // This work around is for ConvertMode == True.
            // Therefore, always on-board a table with ConvertMode == True, first.
            if (mismatch)
            {
                // This is to support "Live" on-boarding of tables to RTable
                return row.ETag != currentRow.ETag;
            }

            return false;
        }

        private bool IsEtagMismatchInternal(IReplicatedTableEntity row, IReplicatedTableEntity currentRow)
        {
            var validETag = long.TryParse(row.ETag.ToString().Trim('\"'), out long eTag);
            return !(validETag && eTag == currentRow._rtable_Version);
        }

        private IList<TableResult> TransactionResponseToTableResultList(Response<IReadOnlyList<Response>> resp, IEnumerable<TableTransactionAction> transactionActions)
        {
            int index = 0;
            IList<TableResult> results = new List<TableResult>();
            if (resp.HasValue)
            {
                foreach (var transactionAction in transactionActions)
                {
                    var transactionResp = resp.Value[index];
                    results.Add(TableResult.ConvertResponseToTableResult(transactionResp, transactionAction.Entity));
                    index++;
                }
            }

            return results;
        }

        private static TableEntity ToTableEntity(IReplicatedTableEntity replicatedTableEntity)
        {
            if (replicatedTableEntity == null)
            {
                throw new ArgumentNullException("replicatedTableEntity");
            }

            var properties = replicatedTableEntity.WriteEntity();

            return new TableEntity(properties)
            {
                PartitionKey = replicatedTableEntity.PartitionKey,
                RowKey = replicatedTableEntity.RowKey,
                ETag = string.IsNullOrEmpty(replicatedTableEntity.ETag.ToString()) ? ETag.All : replicatedTableEntity.ETag
            };
        }

        public static IEnumerable<TableTransactionAction> TransformTableTransactionEntityToTableEntity(IEnumerable<TableTransactionAction> actions)
        {
            var batch = new List<TableTransactionAction>();
            foreach (var action in actions)
            {
                batch.Add(new TableTransactionAction(action.ActionType, ToTableEntity((IReplicatedTableEntity)action.Entity), action.ETag));
            }

            return batch;
        }

    }

    public static class IEnumerableExtensions
    {
        /// <summary>
        /// Add RTable properties to select list.
        /// </summary>
        /// <param name="source">List of columsn to append to.</param>
        /// <returns></returns>
        public static IEnumerable<string> AddRTableProperties(this IEnumerable<string> source)
        {
            ReplicatedTableEntity entity;
            return source.Concat(new List<string>
            {
                nameof(entity._rtable_RowLock),
                nameof(entity._rtable_Version),
                nameof(entity._rtable_Tombstone),
                nameof(entity._rtable_ViewId),
                nameof(entity._rtable_Operation),
                nameof(entity._rtable_BatchId),
                nameof(entity._rtable_LockAcquisition)
            }).ToHashSet();
        }

        /// <summary>
        /// Add ITableEntity properties to list. No need to add ETag as property will always be returned in latest SDK.
        /// </summary>
        /// <param name="source">List of columsn to append to.</param>
        /// <returns></returns>
        public static IEnumerable<string> AddITableEntityProperties(this IEnumerable<string> source)
        {
            return source.Concat(new HashSet<string>
            {
                nameof(ITableEntity.PartitionKey),
                nameof(ITableEntity.RowKey),
                nameof(ITableEntity.Timestamp)
            }).ToHashSet();
        }
    }
}