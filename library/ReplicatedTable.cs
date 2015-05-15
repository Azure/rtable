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
    using System.Net;
    using System.Reflection;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;

    public class ReplicatedTable : IReplicatedTable
    {
        private ReplicatedTableConfigurationService _replicatedTableConfigurationService;
        private string myName;

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

        // Current view
        private View CurrentView
        {
            get { return _replicatedTableConfigurationService.GetWriteView(); }
        }

        // Used for read optimization to read from random replica
        private static Random random = new Random();

        // 2PC protocol constants
        private const int PREPARE_PHASE = 1;
        private const int COMMIT_PHASE = 2;

        // Following fields are used by the caller to find the 
        // number of replicas created or deleted when 
        // CreateIfNotExists and DeleteIfExists are called.
        public short replicasCreated { get; private set; }
        public short replicasDeleted { get; private set; }

        public ReplicatedTable(string name, ReplicatedTableConfigurationService replicatedTableConfigurationAgent)
        {
            this._replicatedTableConfigurationService = replicatedTableConfigurationAgent;
            TableName = name;
        }

        // Create ReplicatedTable replicas if they do not exist.
        // Returns: true if it creates all replicas as defined in the configuration service
        //        : false if it cannot create all replicas or if the config file has zero replicas
        // It sets the number of replicas created for the caller to check.
        public bool CreateIfNotExists(TableRequestOptions requestOptions = null,
            OperationContext operationContext = null)
        {
            ReplicatedTableLogger.LogVerbose("CreateIfNotExists");

            if (CurrentView.Chain.Count == 0)
            {
                return false;
            }

            // Create individual table replicas if they are not created already 
            replicasCreated = 0;
            foreach (var entry in this.CurrentView.Chain)
            {
                ReplicatedTableLogger.LogVerbose("Replica: {0}", entry.Item2.BaseUri.ToString());
                CloudTable ctable = entry.Item2.GetTableReference(this.TableName);
                if (!ctable.Exists())
                {
                    if (ctable.CreateIfNotExists() == false)
                    {
                        return false;
                    }

                    replicasCreated++;
                }
            }

            return (replicasCreated > 0);
        }

        // Check if a table (and its replicas) exist.
        // Returns: true if all replicas exist
        //        : false if any replica doesnt exist        
        public bool Exists()
        {
            if (CurrentView.Chain.Count == 0)
            {
                return false;
            }

            // Return false if individual tables do not exist 
            foreach (var entry in this.CurrentView.Chain)
            {
                CloudTable ctable = entry.Item2.GetTableReference(this.TableName);
                if (ctable.Exists() == false)
                {
                    return false;
                }
            }

            return true;
        }


        // Delete ReplicatedTable replicas if they exist.
        // Returns: true if it deletes all replicas as defined in the configuration service
        //        : false if it cannot delete all replicas or if the config file has zero replicas
        // It sets the number of replicas deleted for the caller to check.
        public bool DeleteIfExists()
        {
            replicasDeleted = 0;

            if (CurrentView.Chain.Count == 0)
            {
                return false;
            }

            // Create individual table replicas if they are not created already 
            foreach (var entry in this.CurrentView.Chain)
            {
                CloudTable ctable = entry.Item2.GetTableReference(this.TableName);
                if (ctable.DeleteIfExists() == false)
                {
                    return false;
                }
                replicasDeleted++;
            }

            return true;
        }


        //
        // Validate the views
        // 
        private bool ValidateViews()
        {
            // Always replicas should be added (1) before the tail replica and (2) after the head replica if there are more than 2 replicas.
            View readView = _replicatedTableConfigurationService.GetReadView();
            View writeView = _replicatedTableConfigurationService.GetWriteView();

            // 0. If there is no write view or read view, return false
            if (writeView == null || readView == null)
            {
                return false;
            }

            // 1. If either view has no elements in it, then return false.
            if (writeView.Chain.Count == 0 ||
                readView.Chain.Count == 0)
            {
                return false;
            }

            // 2. Write view set size should be greater than or equal to the read view set size and the tail replicas should match
            int writeTailIndex = writeView.Chain.Count - 1;
            int readTailIndex = readView.Chain.Count - 1;
            if ((writeTailIndex <= readTailIndex) ||
                (writeView[writeTailIndex].BaseUri.AbsoluteUri.Equals(
                    readView[readTailIndex].BaseUri.AbsoluteUri) == false))
            {
                return false;
            }

            return true;
        }


        //
        // Validate and return appropriate view for given an operation. 
        // 
        private View ValidateAndFetchView(TableOperationType opTypeValue)
        {

            if ((opTypeValue == TableOperationType.Retrieve) || (_replicatedTableConfigurationService.IsViewStable() == true))
            {
                // Return read view if the operation is retrieve or if the view is stable
                return _replicatedTableConfigurationService.GetReadView();
            }
            else if (ValidateViews() == true)
            {
                // Validate the write view and return it
                return _replicatedTableConfigurationService.GetWriteView();
            }
            else
                return null;
        }

        private TableOperationType GetOpType(TableOperation operation)
        {

            // WARNING: We use reflection to read an internal field in OperationType.
            //          We have a dependency on TableOperation fields in WindowsAzureStorage dll
            PropertyInfo opType = operation.GetType()
                .GetProperty("OperationType", System.Reflection.BindingFlags.GetProperty |
                                              System.Reflection.BindingFlags.Instance |
                                              System.Reflection.BindingFlags.NonPublic);
            TableOperationType opTypeValue = (TableOperationType)(opType.GetValue(operation, null));

            return opTypeValue;
        }

        //
        // Execute a table operation
        //
        public TableResult Execute(TableOperation operation, TableRequestOptions requestOptions = null,
            OperationContext operationContext = null)
        {
            TableResult result = null;

            // Invalid argument
            if (operation == null)
                throw new ArgumentNullException();

            TableOperationType opTypeValue = GetOpType(operation);

            switch (opTypeValue)
            {
                /// <summary>
                /// Represents a retrieve operation.
                /// </summary>
                case TableOperationType.Retrieve:
                    result = Retrieve(operation, requestOptions, operationContext);
                    break;

                /// <summary>
                /// Represents an insert or replace operation.
                /// </summary>
                case TableOperationType.InsertOrReplace:
                    result = InsertOrReplace(operation, requestOptions, operationContext);
                    break;

                /// <summary>
                /// Represents an insert operation.
                /// </summary>
                case TableOperationType.Insert:
                    result = Insert(operation, null, requestOptions, operationContext);
                    break;

                /// <summary>
                /// Represents a delete operation.
                /// </summary>
                case TableOperationType.Delete:
                    // If there is a write view, then overwrite the currentView with the write view
                    result = Delete(operation, requestOptions, operationContext);
                    break;

                /// <summary>
                /// Represents a replace operation.
                /// </summary>
                case TableOperationType.Replace:
                    // If there is a write view, then overwrite the currentView with the write view
                    result = Replace(operation, null, requestOptions, operationContext);
                    break;

                /// <summary>
                /// Represents a merge operation.
                /// </summary>
                case TableOperationType.Merge:
                    // If there is a write view, then overwrite the currentView with the write view
                    result = Merge(operation, null, requestOptions, operationContext);
                    break;

                /// <summary>
                /// Represents an insert or merge operation.
                /// </summary>
                case TableOperationType.InsertOrMerge:
                    // If there is a write view, then overwrite the currentView with the write view
                    result = InsertOrMerge(operation, requestOptions, operationContext);
                    break;

            }
            return result;
        }

        /// <summary>
        /// Transform an operation in a batch before executing it on ReplicatedTable.
        /// If row._rtable_RowLock == true (i.e. Prepare phase) or tailIndex, and if it is not an Insert operation,
        /// this function will retrieve the row from the specified replica and increment row._rtable_Version.
        /// Finally, (does not matter whether it is in Prepare or Commit phase), create and return an appropriate TableOperation.
        /// The caller of this function will then create a TableBatchOperation based on the return value of this function.
        /// </summary>
        /// <param name="row"></param>
        /// <param name="phase"></param>
        /// <param name="index"></param>
        /// <param name="requestOptions"></param>
        /// <param name="operationContext"></param>
        /// <returns></returns>
        private TableOperation TransformOp(IReplicatedTableEntity row, int phase, int index,
            TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            int tailIndex = CurrentView.Chain.Count - 1;

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
                    TableOperation operation = TableOperation.Retrieve<DynamicReplicatedTableEntity>(row.PartitionKey, row.RowKey);
                    TableResult retrievedResult = RetrieveFromReplica(operation, index, requestOptions, operationContext);
                    if (retrievedResult == null)
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
                        if ((row.ETag != currentRow._rtable_Version.ToString()) && (index == 0))
                        {
                            // Return the error code that Etag does not match with the input ETag
                            ReplicatedTableLogger.LogInformational("TransformOp(): Etag does not match. row.ETag ({0}) != currentRow._rtable_Version ({1})",
                                                    row.ETag, currentRow._rtable_Version);
                            return null;
                        }
                    }

                    if (currentRow != null)
                    {
                        // Set appropriate values in ETag and _rtable_Version for merge, delete, replace,
                        // insertormerge, insertorreplace
                        row.ETag = retrievedResult.Etag;
                        row._rtable_Version = currentRow._rtable_Version + 1;
                    }
                    else
                    {
                        // Initialize Etag if the row is not present
                        row.ETag = null;
                    }
                }
            }


            if ((row._rtable_Tombstone == true) && (phase == COMMIT_PHASE))
            {
                // In the commit phase, we delete the rows if tombstones are set in the prepare phase
                return TableOperation.Delete(row);
            }
            else if ((row._rtable_Operation == GetTableOperation(TableOperationType.Delete)) && (phase == PREPARE_PHASE))
            {
                // In the prepare phase, we replace the rows with tombstones for delete operations
                row._rtable_Tombstone = true;
                return TableOperation.Replace(row);
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
                return TableOperation.Replace(row);
            }
            else if (row._rtable_Operation == GetTableOperation(TableOperationType.Merge))
            {
                // We use merge in both phases
                return TableOperation.Merge(row);
            }
            else if (row._rtable_Operation == GetTableOperation(TableOperationType.Insert))
            {
                // We insert in the prepare phase for non-tail replicas and during the commit phase at the tail replica
                return TableOperation.Insert(row);
            }
            else if (row._rtable_Operation == GetTableOperation(TableOperationType.InsertOrReplace))
            {
                return TableOperation.InsertOrReplace(row);
            }
            else if (row._rtable_Operation == GetTableOperation(TableOperationType.InsertOrMerge))
            {
                return TableOperation.InsertOrMerge(row);
            }
            else
            {
                // we shouldn't reach here
                return null;
            }
        }

        public IList<TableResult> CheckRetrieveInBatch(TableBatchOperation batch,
            TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            IList<TableResult> results = new List<TableResult>();
            IEnumerator<TableOperation> enumerator = batch.GetEnumerator();

            while (enumerator.MoveNext())
            {
                TableOperation operation = enumerator.Current;
                TableOperationType opType = GetOpType(operation);
                IReplicatedTableEntity row = (IReplicatedTableEntity)GetEntityFromOperation(operation);

                if (opType == TableOperationType.Retrieve)
                {
                    // throw exception if the batch has more than one operation along with a retrieve operation
                    if (batch.Count > 1)
                    {
                        return null;
                    }
                    // Run the retrieve operation and return the result 
                    results.Add(Retrieve(operation, requestOptions, operationContext));
                    return results;
                }
            }
            return null;
        }

        private ITableEntity GetEntityFromOperation(TableOperation operation)
        {

            // WARNING: We use reflection to read an internal field in OperationType.
            //          We have a dependency on TableOperation fields WindowsAzureStorage dll
            PropertyInfo entity = operation.GetType().GetProperty("Entity", System.Reflection.BindingFlags.GetProperty |
                                                                            System.Reflection.BindingFlags.Instance |
                                                                            System.Reflection.BindingFlags.NonPublic);
            return (ITableEntity)(entity.GetValue(operation, null));
        }

        private TableBatchOperation TransformUpdateBatchOp(TableBatchOperation batch, int phase, int index,
            IList<TableResult> results = null, TableRequestOptions requestOptions = null,
            OperationContext operationContext = null)
        {

            IEnumerator<TableOperation> enumerator = batch.GetEnumerator();
            TableBatchOperation batchOp = new TableBatchOperation();
            IEnumerator<TableResult> iter = (results != null) ? results.GetEnumerator() : null;
            int tailIndex = CurrentView.Chain.Count - 1;

            while (enumerator.MoveNext())
            {
                TableOperation operation = enumerator.Current;
                TableOperationType opType = GetOpType(operation);
                TableOperation prepOp = null;
                IReplicatedTableEntity row = (IReplicatedTableEntity)GetEntityFromOperation(operation);

                if (phase == PREPARE_PHASE)
                {
                    // Initialize the operation in the prepare/lock phase
                    row._rtable_Operation = GetTableOperation(opType);
                    row._rtable_RowLock = true;
                    row._rtable_LockAcquisition = DateTime.UtcNow;
                    row._rtable_Tombstone = false;
                    row._rtable_ViewId = CurrentView.ViewId;
                    row._rtable_Version = 0;
                    // Warning: We do not do a sanity check to check for Guid collisions, which should be very unlikely.
                    //          It may be better to check for safety but involves a round trip to the server.
                    row._rtable_BatchId = Guid.NewGuid();

                    if ((prepOp = TransformOp(row, phase, index, requestOptions, operationContext)) == null)
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

                    if ((prepOp = TransformOp(row, phase, index, requestOptions, operationContext)) == null)
                    {
                        throw new ArgumentException();
                    }

                    if (index != tailIndex)
                    {
                        iter.MoveNext();
                        row.ETag = iter.Current.Etag;
                    }
                }

                // Add transformed operation to the batch
                batchOp.Add(prepOp);
            }

            return batchOp;
        }

        // 
        // Validate the batch results
        //
        private bool PostProcessBatchExec(TableBatchOperation requests, IList<TableResult> results, int phase)
        {
            IEnumerator<TableOperation> enumerator = requests.GetEnumerator();
            IEnumerator<TableResult> iter = (results != null) ? results.GetEnumerator() : null;

            // It's a failure if the batch size of request and results don't match
            if (requests.Count != results.Count) return false;

            // 1. Check if all the results are fine
            while (enumerator.MoveNext() && iter.MoveNext())
            {
                TableResult result = iter.Current;
                TableOperation operation = enumerator.Current;
                IReplicatedTableEntity row = (IReplicatedTableEntity)GetEntityFromOperation(operation);

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
                    row.ETag = version.ToString(); // set it back to the prev version
                }
                else
                {
                    row.ETag = row._rtable_Version.ToString();
                    result.Etag = row.ETag;
                }
            }

            return true;
        }

        /// <summary>
        /// Executes a batch operation on a table as an atomic operation, using the specified <see cref="TableRequestOptions"/> and <see cref="OperationContext"/>.
        /// </summary>
        public IList<TableResult> ExecuteBatch(
            TableBatchOperation batch,
            TableRequestOptions requestOptions = null,
            OperationContext operationContext = null)
        {
            // Invalid argument
            if (batch == null || batch.Count == 0)
            {
                throw new InvalidOperationException("Cannot execute an empty batch operation");
            }

            //   Extract operations from the batch and transform them to ReplicatedTable operations. 
            //   If it is a retrieve operation, just call the retrieve function
            //   Otherwise, transform operations and run prepare phase
            IList<TableResult>[] results = new IList<TableResult>[CurrentView.Chain.Count];
            if ((results[0] = CheckRetrieveInBatch(batch, requestOptions, operationContext)) != null)
            {
                return results[0];
            }

            // First, make sure all the rows in the batch operation are not locked. If they are locked, flush them.
            this.FlushAndRetrieveBatch(batch, requestOptions, null);

            // Perform the Prepare phase for the headIndex.
            IList<TableResult> headResults = this.RunPreparePhaseAgainstHeadReplica(batch, requestOptions, operationContext);

            // Perform the Prepare phase for the other replicas and the Commit phase for all replica.
            results = this.Flush2PCBatch(batch, headResults, requestOptions, operationContext);

            // Return the results returned by the tail replica, where all original operations are run.
            return results[CurrentView.Chain.Count - 1];
        }


        /// <summary>
        /// Retrieve all the rows in the batchOperation from the headIndex.
        /// Check to see whether any row is locked.
        /// If a row is locked, call Flush2PC() to flush it through the chain.
        /// </summary>
        /// <param name="batch"></param>
        /// <param name="requestOptions"></param>
        /// <param name="operationContext"></param>
        private void FlushAndRetrieveBatch(
            TableBatchOperation batch,
            TableRequestOptions requestOptions = null,
            OperationContext operationContext = null)
        {
            TableBatchOperation flushBatch = new TableBatchOperation();

            int headIndex = 0;
            IEnumerator<TableOperation> enumerator = batch.GetEnumerator();
            while (enumerator.MoveNext())
            {
                TableOperation operation = enumerator.Current;
                TableOperationType opType = GetOpType(operation); // This is the caller's intended operation on the row
                IReplicatedTableEntity row = (IReplicatedTableEntity)GetEntityFromOperation(operation);

                TableOperation retrieveOperation = TableOperation.Retrieve<DynamicReplicatedTableEntity>(row.PartitionKey, row.RowKey);
                TableResult retrievedResult = RetrieveFromReplica(retrieveOperation, headIndex, requestOptions, operationContext);
                if (retrievedResult == null)
                {
                    // service unavailable
                }
                else if (retrievedResult.Result == null)
                {
                    // row may not be present
                }
                else if (retrievedResult.HttpStatusCode == (int)HttpStatusCode.OK)
                {
                    IReplicatedTableEntity currentRow = ConvertToIReplicatedTableEntity(retrievedResult);

                    if (currentRow._rtable_RowLock == true)
                    {
                        if (DateTime.UtcNow >= currentRow._rtable_LockAcquisition + _replicatedTableConfigurationService.LockTimeout)
                        {
                            try
                            {
                                ReplicatedTableLogger.LogInformational("FlushAndRetrieveBatch(): Row is locked and has expired. PartitionKey={0} RowKey={1}",
                                                        row.PartitionKey, row.RowKey);
                                this.Flush2PC(currentRow, requestOptions, operationContext);
                            }
                            catch (Exception ex)
                            {
                                ReplicatedTableLogger.LogError("FlushAndRetrieveBatch(): Flush2PC() exception {0}", ex.ToString());
                            }
                        }
                        else
                        {
                            ReplicatedTableLogger.LogInformational("FlushAndRetrieveBatch(): Row is locked but NOT expired. PartitionKey={1} RowKey={1}",
                                                    row.PartitionKey, row.RowKey);
                        }
                    }
                }
                else
                {
                    // row may not be present
                }
            }
        }

        /// <summary>
        /// Run Prepare phase of the batch operatin against the head replica.
        /// </summary>
        /// <param name="batch"></param>
        /// <param name="requestOptions"></param>
        /// <param name="operationContext"></param>
        /// <returns></returns>
        private IList<TableResult> RunPreparePhaseAgainstHeadReplica(
            TableBatchOperation batch,
            TableRequestOptions requestOptions = null,
            OperationContext operationContext = null)
        {
            IList<TableResult> results;

            int headIndex = 0;
            int phase = PREPARE_PHASE;

            CloudTableClient tableClient = CurrentView[headIndex];
            CloudTable table = tableClient.GetTableReference(this.TableName);
            TableBatchOperation batchOp = TransformUpdateBatchOp(batch, phase, headIndex, null, requestOptions,
                operationContext);
            if (batchOp == null)
            {
                throw new ReplicatedTableConflictException("Please retry again after random timeout");
            }
            results = table.ExecuteBatch(batchOp, requestOptions, operationContext);
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
        /// <param name="requestOptions"></param>
        /// <param name="operationContext"></param>
        /// <returns></returns>
        private IList<TableResult>[] Flush2PCBatch(
            TableBatchOperation batch,
            IList<TableResult> headResults,
            TableRequestOptions requestOptions = null,
            OperationContext operationContext = null)
        {
            IList<TableResult>[] results = new IList<TableResult>[CurrentView.Chain.Count];

            results[0] = headResults;

            // Run the prepare and lock phase on the non-head replica using the transformed batch operation.
            int phase = PREPARE_PHASE;
            int tailIndex = CurrentView.Chain.Count - 1;
            for (int index = 1; index < tailIndex; index++)
            {
                CloudTableClient tableClient = CurrentView[index];
                CloudTable table = tableClient.GetTableReference(this.TableName);
                TableBatchOperation batchOp = TransformUpdateBatchOp(batch, phase, index, null, requestOptions,
                    operationContext);
                if (batchOp == null)
                {
                    throw new ReplicatedTableConflictException("Please retry again after a random delay");
                }
                results[index] = table.ExecuteBatch(batchOp, requestOptions, operationContext);
                if (PostProcessBatchExec(batch, results[index], phase) == false)
                {
                    throw new DataServiceRequestException();
                }
            }

            // Run the commit phase to unlock and commit the batch 
            for (int index = tailIndex; index >= 0; index--)
            {
                CloudTableClient tableClient = CurrentView[index];
                CloudTable table = tableClient.GetTableReference(this.TableName);
                TableBatchOperation batchOp;

                // If there is only one replica then we have to transform the operation 
                // here as it is not transformed in the prepare phase.
                if (tailIndex == 0)
                {
                    phase = PREPARE_PHASE;
                    batchOp = TransformUpdateBatchOp(batch, phase, index, null, requestOptions, operationContext);
                }
                phase = COMMIT_PHASE;
                batchOp = TransformUpdateBatchOp(batch, phase, index, results[index], requestOptions, operationContext);
                results[index] = table.ExecuteBatch(batchOp, requestOptions, operationContext);
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
        public TableResult Retrieve(TableOperation operation, TableRequestOptions requestOptions = null,
            OperationContext operationContext = null)
        {
            // The tail replica is guaranteed to have the latest committed version. 
            // The tail replica is usually configured to be the closest one, so always go there. This policy optimizes
            // for latency. If read load balancing across replicas is desired, then choose a random replica and read if lock bit is 0.
            int tailIndex = CurrentView.Chain.Count - 1;
            int index = tailIndex;

            TableResult retrievedResult = null;


            //The current read algorithm is as follows:
            //  1. Try read from tail, since tail always has committed data
            //  2. If above succeeds, return the result
            //  2. If read at tail fails, then traverse the chain in reverse from tail to readHeadIndex. The first replica that 
            //     can be reached and whose rowLock = false has the committed data. If no such replica exists we fail the read
            while (true)
            {
                retrievedResult = RetrieveFromReplica(operation, index, requestOptions, operationContext);

                if (retrievedResult == null)
                {
                    //If we attempted at the read head and still failed, we just fail the request
                    if (index == CurrentView.ReadHeadIndex)
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
                    // entity does not exist, so return the error code returned by any replica  
                    return retrievedResult;
                }

                if (retrievedResult.HttpStatusCode != (int)HttpStatusCode.OK)
                {
                    return retrievedResult;
                }

                IReplicatedTableEntity currentRow = null;
                if (retrievedResult.Result is DynamicReplicatedTableEntity)
                {
                    currentRow = retrievedResult.Result as DynamicReplicatedTableEntity;
                }
                else if (retrievedResult.Result is ReplicatedTableEntity)
                {
                    currentRow = retrievedResult.Result as ReplicatedTableEntity;
                }
                else
                {
                    throw new Exception(
                        "Illegal entity type used in ReplicatedTable.");
                }

                if (index != tailIndex && currentRow._rtable_RowLock)
                {
                    //Since we always try the tail first, if we are here, it means all replicas from index to readHeadIndex
                    //will also have the rowLock as true and hence we can just fail the read at this point
                    throw new Exception("Read failed at all replicas");
                }

                // if the entry has a tombstone set, don't return it.
                if (currentRow._rtable_Tombstone)
                {
                    return null;
                }

                // We read a committed value. return it after virtualizing the ETag
                retrievedResult.Etag = currentRow._rtable_Version.ToString();
                IReplicatedTableEntity row = ConvertToIReplicatedTableEntity(retrievedResult);
                row.ETag = retrievedResult.Etag;

                return retrievedResult;
            }
        }

        //
        // Delete: Delete a row
        //
        public TableResult Delete(TableOperation operation, TableRequestOptions requestOptions = null,
            OperationContext operationContext = null)
        {
            IReplicatedTableEntity row = (IReplicatedTableEntity)GetEntityFromOperation(operation);
            row._rtable_Operation = GetTableOperation(TableOperationType.Delete);
            TableOperation top;

            // Delete() = Replace() with "_rtable_Tombstone = true", rows are deleted in the commit phase 
            // after they are replaced with tombstones in the prepare phase.
            row._rtable_Tombstone = true;
            top = TableOperation.Replace(row);
            return Replace(top, null, requestOptions, operationContext);
        }

        //
        // Merge: Merge a row if ETag matches
        // 
        //
        public TableResult Merge(TableOperation operation, TableResult retrievedResult,
            TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            IReplicatedTableEntity row = (IReplicatedTableEntity)GetEntityFromOperation(operation);
            TableResult result;
            bool checkETag = (row._rtable_Operation == GetTableOperation(TableOperationType.InsertOrMerge)) ? false : true;
            row._rtable_Operation = GetTableOperation(TableOperationType.Merge);

            if (retrievedResult == null)
            {
                retrievedResult = FlushAndRetrieve(row, requestOptions, operationContext, false);
            }

            if (retrievedResult.HttpStatusCode != (int)HttpStatusCode.OK)
            {
                // Row is not present, return appropriate error code
                ReplicatedTableLogger.LogInformational("Insert: Row is already present ");
                return new TableResult() { Result = null, Etag = null, HttpStatusCode = (int)HttpStatusCode.NotFound };
            }
            else
            {
                // Row is present at the replica
                // Merge the row
                ReplicatedTableEntity currentRow = (ReplicatedTableEntity)(retrievedResult.Result);
                if (checkETag && (row.ETag != (currentRow._rtable_Version.ToString())))
                {
                    // Return the error code that Etag does not match with the input ETag
                    ReplicatedTableLogger.LogInformational("Merge: ETag mismatch. row.ETag ({0}) != currentRow._rtable_Version ({1})",
                                            row.ETag, currentRow._rtable_Version);
                    return new TableResult()
                    {
                        Result = null,
                        Etag = null,
                        HttpStatusCode = (int)HttpStatusCode.Conflict
                    };
                }

                CloudTableClient tableClient = CurrentView[0];
                row.ETag = retrievedResult.Etag;
                row._rtable_RowLock = true;
                row._rtable_LockAcquisition = DateTime.UtcNow;
                row._rtable_Tombstone = false;
                row._rtable_Version = currentRow._rtable_Version + 1;
                row._rtable_ViewId = CurrentView.ViewId;

                // Lock the head first by inserting the row
                if (((result = UpdateOrDeleteRow(tableClient, row)) == null) ||
                    (result.HttpStatusCode != (int)HttpStatusCode.NoContent))
                {
                    ReplicatedTableLogger.LogError("Merge: Failed to lock the head. ");
                    return new TableResult()
                    {
                        Result = null,
                        Etag = null,
                        HttpStatusCode = (int)HttpStatusCode.ServiceUnavailable
                    };
                }

                // Call Flush2PC to run 2PC on backup (non-head) replicas
                // If successful, it returns HttpStatusCode 204 (no content returned, when replaced in the second phase) 
                if (((result = Flush2PC(row, requestOptions, operationContext, result.Etag)) == null) ||
                    (result.HttpStatusCode != (int)HttpStatusCode.NoContent))
                {
                    // Failed, abort with error and let the application take care of it by reissuing it 
                    // TO DO: Alternately, we could wait and retry after sometime using requestOptions. 
                    ReplicatedTableLogger.LogError("Failed during prepare phase in 2PC for row key: {0}", row.RowKey);
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
                    row.ETag = result.Etag;
                }

                return result;
            }
        }

        //
        // InsertOrMerge: Insert a row or update the row if it already exists
        //
        public TableResult InsertOrMerge(TableOperation operation, TableRequestOptions requestOptions = null,
            OperationContext operationContext = null)
        {
            IReplicatedTableEntity row = (IReplicatedTableEntity)GetEntityFromOperation(operation);
            row._rtable_Operation = GetTableOperation(TableOperationType.InsertOrMerge);
            TableOperation top = TableOperation.Retrieve<DynamicReplicatedTableEntity>(row.PartitionKey, row.RowKey);
            TableResult retrievedResult = FlushAndRetrieve(row, requestOptions, operationContext, false);

            if (retrievedResult.HttpStatusCode == (int)HttpStatusCode.OK)
            {
                // Row is present at the replica, merge the row
                top = TableOperation.Merge(row);
                return Merge(top, retrievedResult, requestOptions, operationContext);
            }
            else
            {
                // Row is not present at the head, insert the row
                top = TableOperation.Insert(row);
                return Insert(top, retrievedResult, requestOptions, operationContext);
            }
        }



        //
        // Replace: Replace a row if ETag matches
        //
        public TableResult Replace(TableOperation operation, TableResult retrievedResult,
            TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            IReplicatedTableEntity row = (IReplicatedTableEntity)GetEntityFromOperation(operation);
            TableResult result;

            bool checkETag = false;
            if (row._rtable_Operation != GetTableOperation(TableOperationType.InsertOrReplace) && 
                row.ETag != "*")
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
                retrievedResult = FlushAndRetrieve(row, requestOptions, operationContext, false);
            }

            if (retrievedResult.HttpStatusCode == (int)HttpStatusCode.Conflict)
            {
                return new TableResult() { Result = null, Etag = null, HttpStatusCode = (int)HttpStatusCode.Conflict };
            }

            if (retrievedResult.HttpStatusCode != (int)HttpStatusCode.OK)
            {
                // Row is not present, return appropriate error code
                ReplicatedTableLogger.LogInformational("Replace: Row is not present. ParitionKey={0} RowKey={1}", row.PartitionKey, row.RowKey);
                return new TableResult() { Result = null, Etag = null, HttpStatusCode = (int)HttpStatusCode.NotFound };
            }

            // Row is present at the replica
            // Replace the row 
            ReplicatedTableEntity currentRow = (ReplicatedTableEntity)(retrievedResult.Result);
            if (checkETag && (row.ETag != (currentRow._rtable_Version.ToString())))
            {
                // Return the error code that Etag does not match with the input ETag
                ReplicatedTableLogger.LogInformational("Replace: Row is not present at the head. ETag mismatch. row.ETag ({0}) != currentRow._rtable_Version ({1})",
                                        row.ETag, currentRow._rtable_Version);
                return new TableResult()
                {
                    Result = null,
                    Etag = null,
                    HttpStatusCode = (int)HttpStatusCode.Conflict
                };
            }

            CloudTableClient tableClient = CurrentView[0];
            row.ETag = (row.ETag != "*") ? retrievedResult.Etag : row.ETag;
            row._rtable_RowLock = true;
            row._rtable_LockAcquisition = DateTime.UtcNow;
            row._rtable_Version = currentRow._rtable_Version + 1;
            row._rtable_ViewId = CurrentView.ViewId;

            // Lock the head first by inserting the row
            if (((result = UpdateOrDeleteRow(tableClient, row)) == null) ||
                (result.HttpStatusCode != (int)HttpStatusCode.NoContent))
            {
                ReplicatedTableLogger.LogError("Insert: Failed to lock the head. ");
                return new TableResult()
                {
                    Result = null,
                    Etag = null,
                    HttpStatusCode = (int)HttpStatusCode.ServiceUnavailable
                };
            }

            // Call Flush2PC to run 2PC on the chain
            // If successful, it returns HttpStatusCode 204 (no content returned, when replaced in the second phase) 
            if (((result = Flush2PC(row, requestOptions, operationContext, result.Etag)) == null) ||
                (result.HttpStatusCode != (int)HttpStatusCode.NoContent))
            {
                // Failed, abort with error and let the application take care of it by reissuing it 
                // TO DO: Alternately, we could wait and retry after sometime using requestOptions. 
                ReplicatedTableLogger.LogError("IOR: Failed during prepare phase in 2PC for row key: {0}", row.RowKey);
                return new TableResult()
                {
                    Result = null,
                    Etag = null,
                    HttpStatusCode = (int)HttpStatusCode.ServiceUnavailable
                };
            }

            // Success. Virtualize Etag before returning the result
            result.Etag = row._rtable_Version.ToString();
            row.ETag = result.Etag;

            return result;
        }


        //
        // Insert: Insert a row
        //
        public TableResult Insert(TableOperation operation, TableResult retrievedResult,
            TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            IReplicatedTableEntity row = (IReplicatedTableEntity)GetEntityFromOperation(operation);
            row._rtable_Operation = GetTableOperation(TableOperationType.Insert);
            TableResult result;
            string[] eTagStrings = new string[CurrentView.Chain.Count];

            if (retrievedResult == null)
            {
                // In case the entry in Head account has _rtable_RowLock=true
                retrievedResult = FlushAndRetrieve(row, requestOptions, operationContext, false);
                if (retrievedResult == null)
                {
                    ReplicatedTableLogger.LogError("Insert: failure in flush.");
                    return null;
                }
            }

            CloudTableClient headTableClient = CurrentView[0];

            // insert a tombstone first. we insert it without a lock since insert will detect conflict anyway.
            DynamicReplicatedTableEntity tsRow = new DynamicReplicatedTableEntity(row.PartitionKey, row.RowKey);
            tsRow._rtable_RowLock = true;
            tsRow._rtable_LockAcquisition = DateTime.UtcNow;
            tsRow._rtable_ViewId = CurrentView.ViewId;
            tsRow._rtable_Version = 0;
            tsRow._rtable_Tombstone = true;
            tsRow._rtable_Operation = GetTableOperation(TableOperationType.Insert);

            // Lock the head first by inserting the row
            if ((result = InsertRow(headTableClient, tsRow)) == null)
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
            result = FlushPreparePhase(tsRow, requestOptions, operationContext, eTagStrings);
            if (result == null)
            {
                return null;
            }

            // now replace the row with version 0 in ReplicatedTable and return the result
            row.ETag = tsRow._rtable_Version.ToString();
            return Replace(TableOperation.Replace(row), retrievedResult, requestOptions, operationContext);
        }

        //
        // InsertOrReplace: Insert a row or update the row if it already exists
        //
        public TableResult InsertOrReplace(TableOperation operation, TableRequestOptions requestOptions = null,
            OperationContext operationContext = null)
        {
            IReplicatedTableEntity row = (IReplicatedTableEntity)GetEntityFromOperation(operation);
            row._rtable_Operation = GetTableOperation(TableOperationType.InsertOrReplace);
            TableOperation top = TableOperation.Retrieve<DynamicReplicatedTableEntity>(row.PartitionKey, row.RowKey);
            TableResult retrievedResult = FlushAndRetrieve(row, requestOptions, operationContext, false);

            if (retrievedResult.HttpStatusCode != (int)HttpStatusCode.OK)
            {
                // Row is not present at the head, insert the row
                return Insert(operation, retrievedResult, requestOptions, operationContext);
            }
            else
            {
                // Row is present at the replica, replace the row
                return Replace(operation, retrievedResult, requestOptions, operationContext);
            }
        }


        //
        // FlushAndRetrieve: Flush (if it is not committed) and retrieve a row.  
        //
        public TableResult FlushAndRetrieve(IReplicatedTableEntity row, TableRequestOptions requestOptions = null,
            OperationContext operationContext = null, bool virtualizeEtag = true)
        {
            //
            // If this row needs repair due to an unstable view, do it now
            //
            TableResult repairRowTableResult = this.RepairRow(row.PartitionKey, row.RowKey, null);
            if (repairRowTableResult.HttpStatusCode != (int)HttpStatusCode.OK
                && repairRowTableResult.HttpStatusCode != (int)HttpStatusCode.NoContent)
            {
                ReplicatedTableLogger.LogError(
                    "FlushAndRetrieve(): RepairRow() returned Unexpected StatusCode {0}. ParitionKey={1} RowKey={2}",
                    repairRowTableResult.HttpStatusCode, row.PartitionKey, row.RowKey);
                return new TableResult()
                {
                    Result = null,
                    Etag = null,
                    HttpStatusCode = (int)HttpStatusCode.NotFound
                };
            }

            TableResult retrievedResult = null;
            if (this._replicatedTableConfigurationService.ConvertXStoreTableMode)
            {
                // When we are in ConvertXStoreTableMode, the existing entities were created as XStore entities.
                // Hence, need to use InitDynamicReplicatedTableEntity which catches KeyNotFoundException
                TableOperation operation = TableOperation.Retrieve<InitDynamicReplicatedTableEntity>(row.PartitionKey, row.RowKey);
                retrievedResult = RetrieveFromReplica(operation, CurrentView.WriteHeadIndex, requestOptions, operationContext);
            }
            else
            {
                TableOperation operation = TableOperation.Retrieve<DynamicReplicatedTableEntity>(row.PartitionKey, row.RowKey);
                retrievedResult = RetrieveFromReplica(operation, CurrentView.WriteHeadIndex, requestOptions, operationContext);
            }

            if (retrievedResult == null)
            {
                // If retrieve fails for some reason then return "service unavailable - 503 " error code
                return new TableResult()
                {
                    Result = null,
                    Etag = null,
                    HttpStatusCode = (int)HttpStatusCode.ServiceUnavailable
                };
            }

            if (retrievedResult.HttpStatusCode != (int) HttpStatusCode.OK)
            {
                // Row may not have been present, return the error code 
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
            if (DateTime.UtcNow >= readRow._rtable_LockAcquisition + _replicatedTableConfigurationService.LockTimeout)
            {
                ReplicatedTableLogger.LogInformational(
                    "FlushAndRetrieve(): _rtable_RowLock has expired. So, calling Flush2PC(). _rtable_LockAcquisition={0} CurrentTime={1}",
                    readRow._rtable_LockAcquisition, DateTime.UtcNow);

                // The entity was locked by a different client a long time ago, so flush it.

                result = Flush2PC(readRow, requestOptions, operationContext);
                if ((result.HttpStatusCode == (int) HttpStatusCode.OK) ||
                    (result.HttpStatusCode == (int) HttpStatusCode.NoContent))
                {
                    // If flush is successful, return the result from the head.
                    result = retrievedResult;
                    if (virtualizeEtag) result.Etag = readRow._rtable_Version.ToString();
                }
            }
            else
            {
                // The entity was locked by a different client recently. Return conflict so that the caller can retry.
                ReplicatedTableLogger.LogInformational(
                    "FlushAndRetrieve(): Row is locked. _rtable_LockAcquisition={0} CurrentTime={1} timeout={2}",
                    readRow._rtable_LockAcquisition, DateTime.UtcNow, _replicatedTableConfigurationService.LockTimeout);
                result = new TableResult()
                {
                    Result = null,
                    Etag = null,
                    HttpStatusCode = (int) HttpStatusCode.Conflict
                };
            }

            return result;
        }

        public IEnumerable<TElement> ExecuteQuery<TElement>(TableQuery<TElement> query,
            TableRequestOptions requestOptions = null, OperationContext operationContext = null)
            where TElement : ITableEntity, new()
        {
            IEnumerable<TElement> rows = Enumerable.Empty<TElement>();

            try
            {
                CloudTable tail = GetTailTableClient().GetTableReference(TableName);
                rows = tail.ExecuteQuery(query);
            }
            catch (Exception e)
            {
                ReplicatedTableLogger.LogError("Error in ExecuteQuery: caught exception {0}", e);
            }

            return rows;
        }

        public TableQuery<TElement> CreateQuery<TElement>()
                where TElement : ITableEntity, new()
        {
            TableQuery<TElement> query = new TableQuery<TElement>();

            try
            {
                CloudTable tail = GetTailTableClient().GetTableReference(TableName);
                query = tail.CreateQuery<TElement>();
            }
            catch (Exception e)
            {
                ReplicatedTableLogger.LogError("Error in CreateQuery: caught exception {0}", e);
            }

            return query;
        }
        
        //
        // RetrieveFromReplica: Retrieve row from a specific replica
        // The caller has to make sure that the argument "operation" is of type TableOperationType.Retrieve. 
        // We do not do the sanity check of the operation type to avoid reflection calls.
        // 
        private TableResult RetrieveFromReplica(TableOperation operation, int index,
            TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            CloudTableClient tableClient = CurrentView[index];
            CloudTable table = tableClient.GetTableReference(this.TableName);
            TableResult retrievedResult = null;

            try
            {
                retrievedResult = table.Execute(operation, requestOptions, operationContext);
            }
            catch (Exception e)
            {
                ReplicatedTableLogger.LogError("Error in RetrieveFromReplica(): caught exception {0}", e);
                return null;
            }

            // If we are able to retrieve an existing entity, 
            // then check consistency of viewId between the currentView and existing entity.
            if (retrievedResult != null)
            {
                IReplicatedTableEntity readRow = ConvertToIReplicatedTableEntity(retrievedResult);
                if (readRow != null && this.CurrentView != null && this.CurrentView.ViewId < readRow._rtable_ViewId)
                {
                    throw new ReplicatedTableStaleViewException(
                        string.Format("current _rtable_ViewId {0} is smaller than _rtable_ViewId of existing row {1}",
                        this.CurrentView.ViewId.ToString(),
                        readRow._rtable_ViewId));
                }
            }

            return retrievedResult;
        }

        private IReplicatedTableEntity ConvertToIReplicatedTableEntity(TableResult retrievedResult)
        {
            IReplicatedTableEntity readRow = null;

            //If the non-generic TableOperation.Retrive() is used, the returned result is of DynamicTableEntity type
            if (retrievedResult.Result is DynamicTableEntity)
            {
                //Convert to an equivalent DynamicReplicatedTableEntity
                DynamicTableEntity tableEntity = (DynamicTableEntity) retrievedResult.Result;
                readRow = new DynamicReplicatedTableEntity(tableEntity.PartitionKey, tableEntity.RowKey, tableEntity.ETag, tableEntity.Properties);
                readRow.ReadEntity(tableEntity.Properties, null);
            }
            //If the generic TableOperation.Retrive<T>() is used, the returned result is of IReplicatedTableEntity type
            else if (retrievedResult.Result is IReplicatedTableEntity)
            {
                readRow = (IReplicatedTableEntity) retrievedResult.Result;
            }

            retrievedResult.Result = readRow;

            return readRow;
        }

        //
        // Flush2PC protocol: Executes chain 2PC protocol after a row is updated and locked at the head.
        //
        private TableResult Flush2PC(IReplicatedTableEntity row, TableRequestOptions requestOptions = null,
            OperationContext operationContext = null, string etagOnHead = null)
        {
            TableResult result = null;
            string[] eTagsStrings = new string[CurrentView.Chain.Count];

            if (etagOnHead != null)
            {
                eTagsStrings[0] = etagOnHead;
            }

            result = FlushPreparePhase(row, requestOptions, operationContext, eTagsStrings);
            if (result == null)
            {
                return null;
            }

            return FlushCommitPhase(row, requestOptions, operationContext, eTagsStrings);
        }

        private TableResult FlushPreparePhase(IReplicatedTableEntity row, TableRequestOptions requestOptions,
            OperationContext operationContext, string[] eTagsStrings)
        {
            TableResult result = new TableResult() { HttpStatusCode = (int)HttpStatusCode.OK };

            // PREPARE PHASE: Uses chain replication to prepare replicas starting from "head+1" to 
            // "tail" sequentially
            for (int index = 1; index <= CurrentView.TailIndex; index++)
            {
                if ((result = InsertUpdateOrDeleteRow(row, index, eTagsStrings[index], requestOptions, operationContext)) ==
                    null)
                {
                    // Failed in the middle, abort with error
                    ReplicatedTableLogger.LogError(
                        "F2PC: Failed during prepare phase in 2PC at replica with index: {0} for row with row key: {1}",
                        index, row.RowKey);
                    return null;
                }

                // Cache the Etag for the commit phase
                eTagsStrings[index] = result.Etag;
            }

            return result;
        }

        private TableResult FlushCommitPhase(IReplicatedTableEntity row, TableRequestOptions requestOptions, OperationContext operationContext,
            string[] eTagStrings)
        {
            TableResult result = null;

            // COMMIT PHASE: Commits the replicas in the reverse order starting from the tail replica
            for (int index = CurrentView.TailIndex; index >= 0; index--)
            {
                row._rtable_RowLock = false;
                result = InsertUpdateOrDeleteRow(row, index, eTagStrings[index], requestOptions, operationContext);

                // It is possible that UpdateInsertOrDeleteRow() returns result.Result = null
                // It happens when the Head entry is _rtable_Tombstone and the Tail entry is gone already.
                // So, just check for "result == null" and return error
                if (result == null)
                {
                    // Failed in the middle, abort with error
                    ReplicatedTableLogger.LogError(
                        "F2PC: Failed during commit phase in 2PC at replica with index: {0} when reading a row with row key: {1}",
                        index,
                        row.RowKey);
                    break;
                }

                //
                // TODO: commit on the head can fail due to Etag mismatch because repair might have updated the Etag. refresh view??
                // check that the row version is still what is expected and then re-issue the commit
                //
            }

            return result;
        }

        //
        // ReadModifyWriteRow : Reads an existing row (or creates a new if it does not exist) and 
        //                      updates it with the new data using existing row's etag as the pre-condintion
        //                      to prevent race conditions from multiple writers.
        //  Returns the new table result if it succeeds. Otherwise, it returns null
        //
        private TableResult InsertUpdateOrDeleteRow(IReplicatedTableEntity row, int index, string Etag,
            TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            // Read the row before updating it
            CloudTableClient tableClient = CurrentView[index];
            TableOperation operation = TableOperation.Retrieve<DynamicReplicatedTableEntity>(row.PartitionKey, row.RowKey);

            // If the Etag is supplied, this is an update or delete based on existing eTag
            // no need to retrieve
            if (Etag != null)
            {
                row.ETag = Etag;
                return UpdateOrDeleteRow(tableClient, row);
            }

            if (row._rtable_RowLock == true && row._rtable_Operation == GetTableOperation(TableOperationType.Insert))
            {
                // if the operation is insert and it is in the prepare phase, just insert the data
                return InsertRow(tableClient, row);
            }

            // TODO: this read seems redundant. remove it.
            // If Etag is not supplied, retrieve the row first before writing
            TableResult retrievedResult = RetrieveFromReplica(operation, index, requestOptions, operationContext);

            if (retrievedResult == null)
            {
                // If retrieve fails for some reason then return "service unavailable - 503 " error code
                ReplicatedTableLogger.LogError(
                    "IUD: Failed to access replica with index: {0} when reading a row with row key: {1}", index,
                    row.RowKey);
                return null;
            }

            if (retrievedResult.HttpStatusCode == (int)HttpStatusCode.OK)
            {
                // Row is present, overwrite the row
                row.ETag = retrievedResult.Etag;
                return UpdateOrDeleteRow(tableClient, row);
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
                TableResult result;
                if (((result = InsertRow(tableClient, row)) == null) ||
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

        private TableResult InsertRow(CloudTableClient tableClient, IReplicatedTableEntity row)
        {
            TableResult result;

            try
            {
                CloudTable table = tableClient.GetTableReference(TableName);
                TableOperation top = TableOperation.Insert(row);
                result = table.Execute(top);
            }
            catch (StorageException ex)
            {
                ReplicatedTableLogger.LogError(ex.ToString());
                result = new TableResult() { Result = null, Etag = null, HttpStatusCode = (int)ex.RequestInformation.HttpStatusCode };
                return result;
            }
            catch (Exception e)
            {
                ReplicatedTableLogger.LogError("TryInsertRow:Error: exception {0}", e);
                return null;
            }

            return result;
        }

        private TableResult UpdateOrDeleteRow(CloudTableClient tableClient, IReplicatedTableEntity row)
        {
            try
            {
                CloudTable table = tableClient.GetTableReference(TableName);
                if ((row._rtable_Tombstone == true) && (row._rtable_RowLock == false))
                {
                    // For delete operations, call the delete operation if it is being committed
                    TableOperation top = TableOperation.Delete(row);
                    return table.Execute(top);
                }
                else if (row._rtable_Operation == GetTableOperation(TableOperationType.Merge))
                {
                    // For Merge operations, call merge row for both phases
                    TableOperation top = TableOperation.Merge(row);
                    return table.Execute(top);
                }
                else
                {
                    TableOperation top = TableOperation.Replace(row);
                    return table.Execute(top);
                }
            }
            catch (Exception e)
            {
                ReplicatedTableLogger.LogError("UpdateOrDeleteRow(): Error: exception {0}", e);
                return null;
            }
        }

        public CloudTableClient GetReplicaTableClient(int index)
        {
            return CurrentView[index];
        }

        public CloudTableClient GetTailTableClient()
        {
            return CurrentView[CurrentView.TailIndex];
        }

        protected void Cleanup(String partitionkey, String rowkey)
        {
            throw new NotImplementedException();
        }

        private bool ValidateAndUnlock(TableBatchOperation inBatch, IList<TableResult> results, bool unlock)
        {
            IEnumerator<TableResult> iter = (results != null) ? results.GetEnumerator() : null;
            bool result2;
            if (iter == null || inBatch.Count != results.Count)
            {
                result2 = false;
            }
            else
            {
                IEnumerator<TableOperation> enumerator = inBatch.GetEnumerator();
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
                        TableOperation operation = enumerator.Current;
                        IReplicatedTableEntity row = (IReplicatedTableEntity)this.GetEntityFromOperation(operation);
                        row._rtable_RowLock = false;
                        row.ETag = result.Etag;
                    }
                }
                result2 = true;
            }
            return result2;
        }

        private IList<TableResult> RunBatch(CloudTableClient tableClient, TableBatchOperation batch, bool unlock,
            TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            IList<TableResult> result;
            try
            {
                CloudTable table = tableClient.GetTableReference(this.TableName);
                IList<TableResult> results;
                if ((results = table.ExecuteBatch(batch, requestOptions, operationContext)) == null ||
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

        private void MergeOperations(TableBatchOperation batch1, TableBatchOperation batch2)
        {
            IEnumerator<TableOperation> enumerator = batch1.GetEnumerator();
            while (enumerator.MoveNext())
            {
                if (!batch2.Contains(enumerator.Current))
                {
                    batch2.Add(enumerator.Current);
                }
            }
        }

        private IList<TableResult> RunBatchSplit(CloudTableClient tableClient, TableBatchOperation batch, bool unlock,
            ref TableBatchOperation outBatch, TableRequestOptions requestOptions = null,
            OperationContext operationContext = null)
        {
            int batchSize = batch.Count;
            TableBatchOperation leftBatch = new TableBatchOperation();
            TableBatchOperation rightBatch = new TableBatchOperation();
            IEnumerator<TableOperation> enumerator = batch.GetEnumerator();
            IList<TableResult> result;
            if (batchSize == 0)
            {
                result = null;
            }
            else
            {
                IList<TableResult> leftResults;
                if ((leftResults = this.RunBatch(tableClient, batch, unlock, requestOptions, operationContext)) != null)
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
                    if ((leftResults = this.RunBatch(tableClient, leftBatch, unlock, requestOptions, operationContext)) !=
                        null)
                    {
                        this.MergeOperations(leftBatch, outBatch);
                        IList<TableResult> rightResults;
                        if (
                            (rightResults =
                                this.RunBatch(tableClient, rightBatch, unlock, requestOptions, operationContext)) !=
                            null)
                        {
                            this.MergeOperations(rightBatch, outBatch);
                            this.MergeResults(rightResults, leftResults);
                        }
                        else
                        {
                            if (
                                (rightResults =
                                    this.RunBatchSplit(tableClient, rightBatch, unlock, ref outBatch, requestOptions,
                                        operationContext)) != null)
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
                                this.RunBatchSplit(tableClient, leftBatch, unlock, ref outBatch, requestOptions,
                                    operationContext)) != null)
                        {
                            this.MergeOperations(leftBatch, outBatch);
                            IList<TableResult> rightResults;
                            if (
                                (rightResults =
                                    this.RunBatch(tableClient, rightBatch, unlock, requestOptions, operationContext)) !=
                                null)
                            {
                                this.MergeOperations(rightBatch, outBatch);
                                this.MergeResults(rightResults, leftResults);
                            }
                            else
                            {
                                if (
                                    (rightResults =
                                        this.RunBatchSplit(tableClient, rightBatch, unlock, ref outBatch, requestOptions,
                                            operationContext)) != null)
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
        /// <returns></returns>
        public ReconfigurationStatus RepairTable(int viewIdToRecoverFrom, TableBatchOperation unfinishedOps,
            long maxBatchSize = 100L)
        {
            ReconfigurationStatus status = ReconfigurationStatus.SUCCESS;
            if (this._replicatedTableConfigurationService.IsViewStable())
            {
                return ReconfigurationStatus.SUCCESS;
            }
            if (!this.ValidateViews())
            {
                return ReconfigurationStatus.FAULTY_WRITE_VIEW;
            }

            CloudTableClient writeHeadClient = CurrentView[CurrentView.WriteHeadIndex];
            CloudTable writeHeadTable = writeHeadClient.GetTableReference(this.TableName);

            // TO DO: Optimization
            // Now we are relying on caller to pass the viewIdToRecoverFrom. Instead, we can find the viewIdToRecoverFrom of the old replica 
            //(when it was part of the stable view) and only get the entries that are greater than the viewIdToRecoverFrom.
            // However, we will miss the entries that are deleted since the old left the view.
            // In order to delete those entries, we need to keep the tombstones around when a replica
            // leaves the system. We have two options to store the tombstones.
            //  1. Store the tombstone entries in a seperate table/blob, whenever an entry is deleted.
            //  2. Store the tombstone entries in other rows of the existing table in a separate column.

            writeHeadTable.CreateIfNotExists(null, null);

            CloudTableClient readHeadTableClient = this.CurrentView[CurrentView.ReadHeadIndex];
            CloudTable readHeadTable = readHeadTableClient.GetTableReference(this.TableName);

            DateTime startTime = DateTime.UtcNow;
            IQueryable<DynamicReplicatedTableEntity> query =
                from ent in readHeadTable.CreateQuery<DynamicReplicatedTableEntity>()
                where ent._rtable_ViewId >= viewIdToRecoverFrom
                select ent;

            foreach (DynamicReplicatedTableEntity entry in query)
            {
                ReplicatedTableLogger.LogWarning("RepairReplica: repairing entity: Pk: {0}, Rk: {1}", entry.PartitionKey, entry.RowKey);

                TableResult result = RepairRow(entry.PartitionKey, entry.RowKey, null);

                if (result.HttpStatusCode != (int)HttpStatusCode.OK &&
                    result.HttpStatusCode != (int)HttpStatusCode.NoContent)
                {
                    status = ReconfigurationStatus.PARTIAL_FAILURE;
                }
            }

            // now find any entries that are in the write view but not in the read view
            query = from ent in writeHeadTable.CreateQuery<DynamicReplicatedTableEntity>()
                    where ent._rtable_ViewId < CurrentView.ViewId
                    select ent;

            foreach (DynamicReplicatedTableEntity extraEntity in query)
            {
                ReplicatedTableLogger.LogWarning("RepairReplica: deleting entity pk: {0}, rk: {1}", extraEntity.PartitionKey, extraEntity.RowKey);

                TableResult result = RepairRow(extraEntity.PartitionKey, extraEntity.RowKey, null);

                if (result.HttpStatusCode != (int)HttpStatusCode.OK &&
                    result.HttpStatusCode != (int)HttpStatusCode.NoContent)
                {
                    status = ReconfigurationStatus.PARTIAL_FAILURE;
                }
            }

            ReplicatedTableLogger.LogInformational("RepairTable: took {0}", DateTime.UtcNow - startTime);

            return status;
        }

        private void FlushRecoveryBatch(TableBatchOperation unfinishedOps, long maxBatchSize, ref ReconfigurationStatus status,
            CloudTableClient newTableClient, CloudTableClient headTableClient, TableBatchOperation batchHead,
            TableBatchOperation batchNewReplica, ref long batchCount, ref Guid batchId)
        {
            TableBatchOperation outBatch = new TableBatchOperation();

            // Lock the head the replica
            if (this.RunBatchSplit(headTableClient, batchHead, true, ref outBatch, null, null) != null &&
                outBatch.Count == batchHead.Count)
            {
                outBatch.Clear();
                // Copy rows to the new replica
                if (this.RunBatchSplit(newTableClient, batchNewReplica, false, ref outBatch, null, null) != null &&
                    outBatch.Count == batchNewReplica.Count)
                {
                    outBatch.Clear();
                    // Unlock the head replica
                    if (this.RunBatchSplit(headTableClient, batchHead, false, ref outBatch, null, null) == null ||
                        outBatch.Count != batchHead.Count)
                    {
                        status = ReconfigurationStatus.UNLOCK_FAILURE;
                    }
                }
                else
                {
                    // Copy failed at the new replica, unlock the head first 
                    TableBatchOperation newOutBatch = new TableBatchOperation();
                    if (this.RunBatchSplit(headTableClient, batchHead, false, ref newOutBatch, null, null) == null)
                    {
                        // Oops, unlock failed
                        status |= ReconfigurationStatus.UNLOCK_FAILURE;
                    }

                    // Copy unfinished operations in the head to the unfinished batch
                    IEnumerator<TableOperation> enumerator = batchNewReplica.GetEnumerator();
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
                TableBatchOperation newOutBatch = new TableBatchOperation();
                if (outBatch.Count > 0 &&
                    this.RunBatchSplit(headTableClient, outBatch, false, ref newOutBatch, null, null) == null)
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

        public static string GetTableOperation(TableOperationType tableOperationType)
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

        /// <summary>
        /// Repair a single row from the current read view to the write view.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="rowKey"></param>
        /// <param name="existingRow">if existing row is specified, then the row is already locked in read view</param>
        /// <returns></returns>
        public TableResult RepairRow(string partitionKey, string rowKey, IReplicatedTableEntity existingRow)
        {
            TableResult result = new TableResult() { HttpStatusCode = (int)HttpStatusCode.OK };

            if (CurrentView.IsStable)
            {
                return result;
            }

            // read from the head of the read view
            TableOperation operation = TableOperation.Retrieve<DynamicReplicatedTableEntity>(partitionKey, rowKey);
            TableResult readHeadResult = RetrieveFromReplica(operation, CurrentView.ReadHeadIndex);
            if (readHeadResult == null)
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
            TableResult writeHeadResult = RetrieveFromReplica(operation, CurrentView.WriteHeadIndex);
            if (writeHeadResult == null)
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
                if (writeViewEntity._rtable_ViewId >= CurrentView.GetReplicaInfo(CurrentView.WriteHeadIndex).ViewInWhichAddedToChain)
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
                    result = UpdateOrDeleteRow(CurrentView[CurrentView.WriteHeadIndex], writeViewEntity);
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
            bool readHeadLockExpired = (readHeadEntity._rtable_LockAcquisition + _replicatedTableConfigurationService.LockTimeout >
                                        DateTime.UtcNow);

            // take a lock on the read view entity unless the entity is already locked
            readHeadEntity._rtable_RowLock = true;
            readHeadEntity._rtable_ViewId = CurrentView.ViewId;
            readHeadEntity._rtable_LockAcquisition = DateTime.UtcNow;
            readHeadEntity._rtable_Operation = GetTableOperation(TableOperationType.Replace);

            result = UpdateOrDeleteRow(CurrentView[CurrentView.ReadHeadIndex], readHeadEntity);
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

            string readHeadEtag = readHeadEntity.ETag;

            // now copy the row to the write head
            if (writeViewEntity != null)
            {
                readHeadEntity.ETag = writeViewEntity.ETag;
            }
            else
            {
                readHeadEntity.ETag = null;
            }

            result = InsertUpdateOrDeleteRow(readHeadEntity, CurrentView.WriteHeadIndex, readHeadEntity.ETag);
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
                readHeadEntity.ETag = readHeadEtag;
                result = UpdateOrDeleteRow(CurrentView[CurrentView.ReadHeadIndex], readHeadEntity);
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
                readHeadEntity.ETag = writeHeadEtagForCommit;
                result = UpdateOrDeleteRow(CurrentView[CurrentView.WriteHeadIndex], readHeadEntity);

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
                result = Flush2PC(readHeadEntity, null, null, writeHeadEtagForCommit);
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
        /// <param name="requestOptions"></param>
        /// <param name="operationContext"></param>
        public void ConvertXStoreTable(
            out long successCount,
            out long skippedCount,
            out long failedCount,
            TableRequestOptions requestOptions = null,
            OperationContext operationContext = null)
        {
            successCount = 0;
            skippedCount = 0;
            failedCount = 0;

            View currentReadView = this.ValidateAndFetchView(TableOperationType.Retrieve);
            if (currentReadView == null)
            {
                throw new ApplicationException("Unable to load the current read view");
            }

            if (this._replicatedTableConfigurationService.ConvertXStoreTableMode == false)
            {
                throw new InvalidOperationException("ConvertXStoreTable() API is NOT supported when ReplicatedTable is NOT in ConvertXStoreTableMode.");
            }

            int tailIndex = currentReadView.TailIndex;

            DateTime startTime = DateTime.UtcNow;
            ReplicatedTableLogger.LogInformational("ConvertXStoreTable() started {0}", startTime);

            CloudTableClient tailTableClient = currentReadView[tailIndex];
            CloudTable tailTable = tailTableClient.GetTableReference(this.TableName);

            IQueryable<InitDynamicReplicatedTableEntity> query =
                                    from ent in tailTable.CreateQuery<InitDynamicReplicatedTableEntity>().AsQueryable<InitDynamicReplicatedTableEntity>()
                                    select ent;

            using (IEnumerator<InitDynamicReplicatedTableEntity> enumerator = query.GetEnumerator())
            {
                while (enumerator.MoveNext())
                {
                    InitDynamicReplicatedTableEntity entity = enumerator.Current;
                    if (entity._rtable_ViewId != 0)
                    {
                        // _rtable_ViewId = 0 means that the entity has not been operated on since ther XStore Table was converted to ReplicatedTable.
                        // So, convert it manually now.
                        ReplicatedTableLogger.LogInformational("Skipped XStore entity with Partition={0} Row={1}", entity.PartitionKey, entity.RowKey);
                        skippedCount++;
                        continue;
                    }
                    entity._rtable_ViewId = currentReadView.ViewId;
                    entity._rtable_Version = 1;
                    TableOperation top = TableOperation.Replace(entity);
                    try
                    {
                        TableResult result = tailTable.Execute(top, requestOptions, operationContext);
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

    }


}