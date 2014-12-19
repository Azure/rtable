using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Core.Util;
using Microsoft.WindowsAzure.Storage.RTable.ConfigurationAgentLibrary;
using System.Configuration;
using System.Data.Services.Client;
using System.Reflection;

namespace Microsoft.WindowsAzure.Storage.RTable
{

    public class RTable : IRTable
    {
        private RTableConfigurationService rTableConfigurationService;
        private string myName;

        public string tableName
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

        // Current read view 
        private View CurrentView
        {
            get { return rTableConfigurationService.GetWriteView(); }
        }

        // Used for read optimization to read from random replica
        private static Random random = new Random();

        // 2PC protocol constants
        private const int PREPARE_PHASE = 1;
        private const int COMMIT_PHASE = 2;

        // Reconfig status
        public enum ReconfigStatus
        {
            SUCCESS = 0,
            PARTIAL_FAILURE = 1,
            LOCK_FAILURE = 2,
            UNLOCK_FAILURE = 4,
            FAULTY_WRITE_VIEW = 8,
            TABLE_NOT_FOUND = 16,
            FAILURE = 32
        }

        // Following fields are used by the caller to find the 
        // number of replicas created or deleted when 
        // CreateIfNotExists and DeleteIfExists are called.
        public short replicasCreated { get; private set; }
        public short replicasDeleted { get; private set; }

        public RTable(string name, RTableConfigurationService rTableConfigurationAgent)
        {
            this.rTableConfigurationService = rTableConfigurationAgent;
            tableName = name;
        }

        // Create RTable replicas if they do not exist.
        // Returns: true if it creates all replicas as defined in the configuration service
        //        : false if it cannot create all replicas or if the config file has zero replicas
        // It sets the number of replicas created for the caller to check.
        public bool CreateIfNotExists(TableRequestOptions requestOptions = null,
            OperationContext operationContext = null)
        {

            if (CurrentView.Chain.Count == 0)
            {
                return false;
            }

            // Create individual table replicas if they are not created already 
            foreach (var entry in this.CurrentView.Chain)
            {
                Console.WriteLine(entry.Item2.BaseUri);
                CloudTable ctable = entry.Item2.GetTableReference(this.tableName);
                if (ctable.CreateIfNotExists() == false)
                {
                    return false;
                }
                replicasCreated++;
            }

            return true;
        }

        // Chek if a table (and its replicas) exist.
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
                Console.WriteLine(entry.Item2.BaseUri);
                CloudTable ctable = entry.Item2.GetTableReference(this.tableName);
                if (ctable.Exists() == false)
                {
                    return false;
                }
            }

            return true;
        }


        // Delete RTable replicas if they exist.
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
                Console.WriteLine(entry.Item2.BaseUri);
                CloudTable ctable = entry.Item2.GetTableReference(this.tableName);
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
            View readView = rTableConfigurationService.GetReadView();
            View writeView = rTableConfigurationService.GetWriteView();

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

            if ((opTypeValue == TableOperationType.Retrieve) || (rTableConfigurationService.IsViewStable() == true))
            {
                // Return read view if the operation is retrieve or if the view is stable
                return rTableConfigurationService.GetReadView();
            }
            else if (ValidateViews() == true)
            {
                // Validate the write view and return it
                return rTableConfigurationService.GetWriteView();
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
            TableOperationType opTypeValue = (TableOperationType) (opType.GetValue(operation, null));

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
        /// Transform an operation in a batch before executing it on RTable.
        /// If row.RowLock == true (i.e. Prepare phase) or tailIndex, and if it is not an Insert operation,
        /// this function will retrieve the row from the specified replica and increment row.Version.
        /// Finally, (does not matter whether it is in Prepare or Commit phase), create and return an appropriate TableOperation.
        /// The caller of this function will then create a TableBatchOperation based on the return value of this function.
        /// </summary>
        /// <param name="row"></param>
        /// <param name="phase"></param>
        /// <param name="index"></param>
        /// <param name="requestOptions"></param>
        /// <param name="operationContext"></param>
        /// <returns></returns>
        private TableOperation TransformOp(IRTableEntity row, int phase, int index,
            TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            int tailIndex = CurrentView.Chain.Count - 1;

            if ((row.RowLock == true) || (index == tailIndex))
            {
                // Here is the transformation we do on the operation for the prepare phase of non-tail replicas
                //  or the commit phase for the tail replica.

                if (row.Operation != GetTableOperation(TableOperationType.Insert))
                {

                    // If it is InsertOrReplace or InsertOrMerge, then we do not have to check the etag.
                    bool checkETag = ((row.Operation == GetTableOperation(TableOperationType.InsertOrReplace)) ||
                                      (row.Operation == GetTableOperation(TableOperationType.InsertOrMerge)))
                        ? false
                        : true;

                    // If Etag is not supplied, retrieve the row first before writing
                    TableOperation operation = TableOperation.Retrieve<DynamicRTableEntity>(row.PartitionKey, row.RowKey);
                    TableResult retrievedResult = RetrieveFromReplica(operation, index, requestOptions, operationContext);
                    if (retrievedResult == null)
                    {
                        return null;
                    }

                    IRTableEntity currentRow = (IRTableEntity) (retrievedResult.Result);

                    if (checkETag == true)
                    {
                        if (retrievedResult.HttpStatusCode != (int) HttpStatusCode.OK)
                        {
                            // Row is not present, return appropriate error code as Merge, Delete and Replace
                            // requires row to be present.
                            Console.WriteLine("Row is not present.");
                            return null;
                        }

                        // Check if vertual Etag matches at the head replica. We don't need to match it every replica.
                        if ((row.ETag != currentRow.Version.ToString()) && (index == 0))
                        {
                            // Return the error code that Etag does not match with the input ETag
                            Console.WriteLine("TransformOp(): Etag does not match. row.ETag ({0}) != currentRow.Version ({1})\n",
                                row.ETag, currentRow.Version);
                            return null;
                        }
                    }

                    if (currentRow != null)
                    {
                        // Set appropriate values in ETag and Version for merge, delete, replace,
                        // insertormerge, insertorreplace
                        row.ETag = retrievedResult.Etag;
                        row.Version = currentRow.Version + 1;
                    }
                    else
                    {
                        // Initialize Etag if the row is not present
                        row.ETag = null;
                    }
                }
            }


            if ((row.Tombstone == true) && (phase == COMMIT_PHASE))
            {
                // In the commit phase, we delete the rows if tombstones are set in the prepare phase
                return TableOperation.Delete(row);
            }
            else if ((row.Operation == GetTableOperation(TableOperationType.Delete)) && (phase == PREPARE_PHASE))
            {
                // In the prepare phase, we replace the rows with tombstones for delete operations
                row.Tombstone = true;
                return TableOperation.Replace(row);
            }
            else if (
                (row.Operation == GetTableOperation(TableOperationType.Replace))
                ||
                (((row.Operation == GetTableOperation(TableOperationType.Insert)) ||
                (row.Operation == GetTableOperation(TableOperationType.InsertOrMerge)) ||
                (row.Operation == GetTableOperation(TableOperationType.InsertOrReplace)))
                && (phase == COMMIT_PHASE) && (index != tailIndex))
                )
            {
                // We use replace for the replace operation in both phases and for insert family operations in the commit phase 
                // for non-tail replicas.
                return TableOperation.Replace(row);
            }
            else if (row.Operation == GetTableOperation(TableOperationType.Merge))
            {
                // We use merge in both phases
                return TableOperation.Merge(row);
            }
            else if (row.Operation == GetTableOperation(TableOperationType.Insert))
            {
                // We insert in the prepare phase for non-tail replicas and during the commit phase at the tail replica
                return TableOperation.Insert(row);
            }
            else if (row.Operation == GetTableOperation(TableOperationType.InsertOrReplace))
            {
                return TableOperation.InsertOrReplace(row);
            }
            else if (row.Operation == GetTableOperation(TableOperationType.InsertOrMerge))
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
                IRTableEntity row = (IRTableEntity) GetEntityFromOperation(operation);

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
            return (ITableEntity) (entity.GetValue(operation, null));
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
                IRTableEntity row = (IRTableEntity) GetEntityFromOperation(operation);

                if (phase == PREPARE_PHASE)
                {
                    // Initialize the operation in the prepare/lock phase
                    row.Operation = GetTableOperation(opType);
                    row.RowLock = true;
                    row.LockAcquisition = DateTime.UtcNow;
                    row.Tombstone = false;
                    row.ViewId = CurrentView.ViewId;
                    row.Version = 0;
                    // Warning: We do not do a sanity check to check for Guid collisions, which should be very unlikely.
                    //          It may be better to check for safety but involves a round trip to the server.
                    row.BatchId = Guid.NewGuid();

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
                    row.RowLock = false;

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
                IRTableEntity row = (IRTableEntity) GetEntityFromOperation(operation);

                TableOperationType opType;
                TableOperationType.TryParse(row.Operation, out opType);

                switch (opType)
                {
                    case TableOperationType.Insert:
                    case TableOperationType.Merge:
                    case TableOperationType.Replace:
                    case TableOperationType.Delete:
                    case TableOperationType.InsertOrMerge:
                    case TableOperationType.InsertOrReplace:
                        if ((result == null) ||
                            ((result.HttpStatusCode != (int) HttpStatusCode.Created) &&
                             (result.HttpStatusCode != (int) HttpStatusCode.NoContent)))
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
                    long version = row.Version - 1;
                    row.ETag = version.ToString(); // set it back to the prev version
                }
                else
                {
                    row.ETag = row.Version.ToString();
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

            //   Extract operations from the batch and transform them to RTable operations. 
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
                IRTableEntity row = (IRTableEntity)GetEntityFromOperation(operation);

                TableOperation retrieveOperation = TableOperation.Retrieve<DynamicRTableEntity>(row.PartitionKey, row.RowKey);
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
                    IRTableEntity currentRow = (IRTableEntity)(retrievedResult.Result);                  

                    if (currentRow.RowLock == true)
                    {
                        if (DateTime.UtcNow >= currentRow.LockAcquisition + rTableConfigurationService.LockTimeout)
                        {
                            try
                            {
                                Console.WriteLine("FlushAndRetrieveBatch(): Row is locked and has expired. PartitionKey={0} RowKey={1}",
                                    row.PartitionKey, row.RowKey);
                                this.Flush2PC(currentRow, requestOptions, operationContext);
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("FlushAndRetrieveBatch(): Flush2PC() exception {0}", ex.ToString());
                            }
                        }
                        else
                        {
                            Console.WriteLine("FlushAndRetrieveBatch(): Row is locked but NOT expired. PartitionKey={1} RowKey={1}",
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
            CloudTable table = tableClient.GetTableReference(this.tableName);
            TableBatchOperation batchOp = TransformUpdateBatchOp(batch, phase, headIndex, null, requestOptions,
                operationContext);
            if (batchOp == null)
            {
                throw new RTableConflictException("Please retry again after random timeout");
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
                CloudTable table = tableClient.GetTableReference(this.tableName);
                TableBatchOperation batchOp = TransformUpdateBatchOp(batch, phase, index, null, requestOptions,
                    operationContext);
                if (batchOp == null)
                {
                    throw new RTableConflictException("Please retry again after a random delay");
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
                CloudTable table = tableClient.GetTableReference(this.tableName);
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
            // Read from any (random) replica in the current view 
            // If the lock bit is set after reading, retrieve from the tail replica.
            // The tail replica is guaranteed to have the latest committed version.
            int index = random.Next(0, CurrentView.Chain.Count);
            int tailIndex = CurrentView.Chain.Count - 1;
            TableResult retrievedResult = null;

            while (true)
            {
                retrievedResult = RetrieveFromReplica(operation, index, requestOptions, operationContext);

                if (retrievedResult == null)
                {
                    if (index == tailIndex)
                    {
                        // Throw an exception if we cannot reach the tail replica
                        throw new Exception("Cannot reach the tail replica.");
                    }

                    // If it failed trying to read from a non-tail replica, then try the tail replica
                    index = tailIndex;
                    continue;
                }

                if (retrievedResult.Result == null)
                {
                    // entity does not exist, so return the error code returned by any replica  
                    return retrievedResult;
                }

                if (retrievedResult.HttpStatusCode != (int) HttpStatusCode.OK)
                {
                    return retrievedResult;
                }

                IRTableEntity currentRow = null;
                if (retrievedResult.Result is DynamicRTableEntity)
                {
                    currentRow = retrievedResult.Result as DynamicRTableEntity;
                }
                else if (retrievedResult.Result is RTableEntity)
                {
                    currentRow = retrievedResult.Result as RTableEntity;
                }
                else
                {
                    // Throw an exception if we cannot reach the tail replica
                    throw new Exception(
                        "Illegal entity type used in RTable. Use TableOperation.Retrieve<T> with proper entity type T. ");
                }

                if (index != tailIndex && currentRow.RowLock)
                {
                    index = tailIndex;
                    continue;
                }

                // if the entry has a tombstone set, don't return it.
                if (currentRow.Tombstone)
                {
                    return null;
                }

                // We read a committed value. return it after virtualizing the ETag
                retrievedResult.Etag = currentRow.Version.ToString();
                IRTableEntity row = (IRTableEntity) retrievedResult.Result;
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
            IRTableEntity row = (IRTableEntity) GetEntityFromOperation(operation);
            row.Operation = GetTableOperation(TableOperationType.Delete);
            TableOperation top;

            // Delete() = Replace() with "Tombstone = true", rows are deleted in the commit phase 
            // after they are replaced with tombstones in the prepare phase.
            row.Tombstone = true;
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
            IRTableEntity row = (IRTableEntity) GetEntityFromOperation(operation);
            TableResult result;
            bool checkETag = (row.Operation == GetTableOperation(TableOperationType.InsertOrMerge)) ? false : true;
            row.Operation = GetTableOperation(TableOperationType.Merge);

            if (retrievedResult == null)
            {
                retrievedResult = FlushAndRetrieve(row, requestOptions, operationContext, false);
            }

            if (retrievedResult.HttpStatusCode != (int) HttpStatusCode.OK)
            {
                // Row is not present, return appropriate error code
                Console.WriteLine("Insert: Row is already present ");
                return new TableResult() {Result = null, Etag = null, HttpStatusCode = (int) HttpStatusCode.NotFound};
            }
            else
            {
                // Row is present at the replica
                // Merge the row
                RTableEntity currentRow = (RTableEntity) (retrievedResult.Result);
                if (checkETag && (row.ETag != (currentRow.Version.ToString())))
                {
                    // Return the error code that Etag does not match with the input ETag
                    Console.WriteLine("Merge: ETag mismatch. row.ETag ({0}) != currentRow.Version ({1})",
                        row.ETag, currentRow.Version);
                    return new TableResult()
                    {
                        Result = null,
                        Etag = null,
                        HttpStatusCode = (int)HttpStatusCode.Conflict
                    };                    
                }
                
                CloudTableClient tableClient = CurrentView[0];
                row.ETag = retrievedResult.Etag;
                row.RowLock = true;
                row.LockAcquisition = DateTime.UtcNow;
                row.Tombstone = false;
                row.Version = currentRow.Version + 1;
                row.ViewId = CurrentView.ViewId;

                // Lock the head first by inserting the row
                if (((result = UpdateOrDeleteRow(tableClient, row)) == null) ||
                    (result.HttpStatusCode != (int) HttpStatusCode.NoContent))
                {
                    Console.WriteLine("Merge: Failed to lock the head. ");
                    return new TableResult()
                    {
                        Result = null,
                        Etag = null,
                        HttpStatusCode = (int) HttpStatusCode.ServiceUnavailable
                    };
                }

                // Call Flush2PC to run 2PC on backup (non-head) replicas
                // If successful, it returns HttpStatusCode 204 (no content returned, when replaced in the second phase) 
                if (((result = Flush2PC(row, requestOptions, operationContext, result.Etag)) == null) ||
                    (result.HttpStatusCode != (int) HttpStatusCode.NoContent))
                {
                    // Failed, abort with error and let the application take care of it by reissuing it 
                    // TO DO: Alternately, we could wait and retry after sometime using requestOptions. 
                    Console.WriteLine("IOR: Failed during prepare phase in 2PC for row key: {0} \n", row.RowKey);
                    return new TableResult()
                    {
                        Result = null,
                        Etag = null,
                        HttpStatusCode = (int) HttpStatusCode.ServiceUnavailable
                    };
                }
                else
                {
                    // Success. Virtualize Etag before returning the result
                    result.Etag = row.Version.ToString();
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
            IRTableEntity row = (IRTableEntity) GetEntityFromOperation(operation);
            row.Operation = GetTableOperation(TableOperationType.InsertOrMerge);
            TableOperation top = TableOperation.Retrieve<DynamicRTableEntity>(row.PartitionKey, row.RowKey);
            TableResult retrievedResult = FlushAndRetrieve(row, requestOptions, operationContext, false);

            if (retrievedResult.HttpStatusCode == (int) HttpStatusCode.OK)
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
            IRTableEntity row = (IRTableEntity) GetEntityFromOperation(operation);
            TableResult result;

            bool checkETag = (row.Operation != GetTableOperation(TableOperationType.InsertOrReplace));

            // If it's called by the delete operation do not set the tombstone
            if (row.Operation != GetTableOperation(TableOperationType.Delete))
            {
                row.Tombstone = false;
            }

            row.Operation = GetTableOperation(TableOperationType.Replace);

            if (retrievedResult == null)
            {
                retrievedResult = FlushAndRetrieve(row, requestOptions, operationContext, false);
            }

            if (retrievedResult.HttpStatusCode == (int) HttpStatusCode.Conflict)
            {
                return new TableResult() {Result = null, Etag = null, HttpStatusCode = (int) HttpStatusCode.Conflict};
            }

            if (retrievedResult.HttpStatusCode != (int) HttpStatusCode.OK)
            {
                // Row is not present, return appropriate error code
                Console.WriteLine("Insert: Row is not present ");
                return new TableResult() {Result = null, Etag = null, HttpStatusCode = (int) HttpStatusCode.NotFound};
            }

            // Row is present at the replica
            // Replace the row 
            RTableEntity currentRow = (RTableEntity) (retrievedResult.Result);
            if (checkETag && (row.ETag != (currentRow.Version.ToString())))
            {
                // Return the error code that Etag does not match with the input ETag
                Console.WriteLine("Replace: Row is not present at the head. ETag mismatch. row.ETag ({0}) != currentRow.Version ({1})",
                    row.ETag, currentRow.Version);
                return new TableResult()
                {
                    Result = null,
                    Etag = null,
                    HttpStatusCode = (int)HttpStatusCode.Conflict
                };
            }

            CloudTableClient tableClient = CurrentView[0];
            row.ETag = retrievedResult.Etag;
            row.RowLock = true;
            row.LockAcquisition = DateTime.UtcNow;
            row.Version = currentRow.Version + 1;
            row.ViewId = CurrentView.ViewId;

            // Lock the head first by inserting the row
            if (((result = UpdateOrDeleteRow(tableClient, row)) == null) ||
                (result.HttpStatusCode != (int) HttpStatusCode.NoContent))
            {
                Console.WriteLine("Insert: Failed to lock the head. ");
                return new TableResult()
                {
                    Result = null,
                    Etag = null,
                    HttpStatusCode = (int) HttpStatusCode.ServiceUnavailable
                };
            }

            // Call Flush2PC to run 2PC on the chain
            // If successful, it returns HttpStatusCode 204 (no content returned, when replaced in the second phase) 
            if (((result = Flush2PC(row, requestOptions, operationContext, result.Etag)) == null) ||
                (result.HttpStatusCode != (int) HttpStatusCode.NoContent))
            {
                // Failed, abort with error and let the application take care of it by reissuing it 
                // TO DO: Alternately, we could wait and retry after sometime using requestOptions. 
                Console.WriteLine("IOR: Failed during prepare phase in 2PC for row key: {0} \n", row.RowKey);
                return new TableResult()
                {
                    Result = null,
                    Etag = null,
                    HttpStatusCode = (int) HttpStatusCode.ServiceUnavailable
                };
            }

            // Success. Virtualize Etag before returning the result
            result.Etag = row.Version.ToString();
            row.ETag = result.Etag;

            return result;
        }


        //
        // Insert: Insert a row
        //
        public TableResult Insert(TableOperation operation, TableResult retrievedResult,
            TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            IRTableEntity row = (IRTableEntity) GetEntityFromOperation(operation);
            row.Operation = GetTableOperation(TableOperationType.Insert);
            TableResult result;
            string[] eTagStrings = new string[CurrentView.Chain.Count];
            
            if (retrievedResult == null)
            {
                // In case the entry in Head account has RowLock=true
                retrievedResult = FlushAndRetrieve(row, requestOptions, operationContext, false);
                if (retrievedResult == null)
                {
                    Console.WriteLine("Insert: failure in flush.");
                    return null;
                }
            }

            CloudTableClient headTableClient = CurrentView[0];

            // insert a tombstone first. we insert it without a lock since insert will detect conflict anyway.
            DynamicRTableEntity tsRow = new DynamicRTableEntity(row.PartitionKey, row.RowKey);
            tsRow.RowLock = true;
            tsRow.LockAcquisition = DateTime.UtcNow;
            tsRow.ViewId = CurrentView.ViewId;
            tsRow.Version = 0;
            tsRow.Tombstone = true;
            tsRow.Operation = GetTableOperation(TableOperationType.Insert);

            // Lock the head first by inserting the row
            if ((result = InsertRow(headTableClient, tsRow)) == null)
            {
                Console.WriteLine("Insert: Failed to insert at the head.");
                return null;
            }

            // insert must return the contents of the row
            if (result.HttpStatusCode != (int) HttpStatusCode.Created &&
                result.HttpStatusCode != (int) HttpStatusCode.NoContent)
            {
                Console.WriteLine("Insert: Failed to insert at the head with HttpStatusCode = {0}", result.HttpStatusCode);
                return new TableResult()
                {
                    Result = null,
                    Etag = null,
                    HttpStatusCode = result.HttpStatusCode
                };
            }

            // copy the resulting row from the head into the head result to pass to the Replace operation later on.
            retrievedResult = result;
            retrievedResult.HttpStatusCode = (int) HttpStatusCode.OK;
            retrievedResult.Result = tsRow;

            // we have taken a lock on the head.
            // now flush this row to the remaining replicas.
            result = FlushPreparePhase(tsRow, requestOptions, operationContext, eTagStrings);
            if (result == null)
            {
                return null;
            }

            // now replace the row with version 0 in RTable and return the result
            row.ETag = tsRow.Version.ToString();
            return Replace(TableOperation.Replace(row), retrievedResult, requestOptions, operationContext);
        }

        //
        // InsertOrReplace: Insert a row or update the row if it already exists
        //
        public TableResult InsertOrReplace(TableOperation operation, TableRequestOptions requestOptions = null,
            OperationContext operationContext = null)
        {
            IRTableEntity row = (IRTableEntity) GetEntityFromOperation(operation);
            row.Operation = GetTableOperation(TableOperationType.InsertOrReplace);
            TableOperation top = TableOperation.Retrieve<DynamicRTableEntity>(row.PartitionKey, row.RowKey);
            TableResult retrievedResult = FlushAndRetrieve(row, requestOptions, operationContext, false);

            if (retrievedResult.HttpStatusCode != (int) HttpStatusCode.OK)
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
        public TableResult FlushAndRetrieve(IRTableEntity row, TableRequestOptions requestOptions = null,
            OperationContext operationContext = null, bool virtualizeEtag = true)
        {
            // Retrieve from the head
            TableResult result = null;
            int headIndex = 0;

            TableResult retrievedResult = null;
            if (this.rTableConfigurationService.ConvertXStoreTableMode)
            {
                // When we are in ConvertXStoreTableMode, the existing entities were created as XStore entities.
                // Hence, need to use DynamicRTableEntity2 which catches KeyNotFoundException
                TableOperation operation = TableOperation.Retrieve<DynamicRTableEntity2>(row.PartitionKey, row.RowKey);
                retrievedResult = RetrieveFromReplica(operation, headIndex, requestOptions, operationContext);
            }
            else
            {
                TableOperation operation = TableOperation.Retrieve<DynamicRTableEntity>(row.PartitionKey, row.RowKey);
                retrievedResult = RetrieveFromReplica(operation, headIndex, requestOptions, operationContext);
            }

            if (retrievedResult == null)
            {
                // If retrieve fails for some reason then return "service unavailable - 503 " error code
                result = new TableResult()
                {
                    Result = null,
                    Etag = null,
                    HttpStatusCode = (int) HttpStatusCode.ServiceUnavailable
                };
            }
            else if (retrievedResult.Result == null)
            {
                // Row may not have been present, return the error code 
                result = retrievedResult;
            }
            else if (retrievedResult.HttpStatusCode == (int) HttpStatusCode.OK)
            {
                IRTableEntity readRow = (IRTableEntity) retrievedResult.Result;                

                // If it is not committed, either:
                // (1) (Lock expired) flush the row to other replicas, commit it, and return the result.
                // Or (2) (Lock not expired) return a Conflict so that the caller can try again later,
                if (readRow.RowLock == true)
                {
                    if (DateTime.UtcNow >= readRow.LockAcquisition + rTableConfigurationService.LockTimeout)
                    {
                        Console.WriteLine("FlushAndRetrieve(): RowLock has expired. So, calling Flush2PC(). LockAcquisition={0} CurrentTime={1}",
                            readRow.LockAcquisition, DateTime.UtcNow);

                        // The entity was locked by a different client a long time ago, so flush it.
                        
                        result = Flush2PC(readRow, requestOptions, operationContext);
                        if ((result.HttpStatusCode == (int)HttpStatusCode.OK) ||
                            (result.HttpStatusCode == (int)HttpStatusCode.NoContent))
                        {
                            // If flush is successful, return the result from the head.
                            result = retrievedResult;
                            if (virtualizeEtag) result.Etag = readRow.Version.ToString();
                        }
                    }
                    else
                    {
                        // The entity was locked by a different client recently. Return conflict so that the caller can retry.
                        Console.WriteLine("FlushAndRetrieve(): Row is locked. LockAcquisition={0} CurrentTime={1} timeout={2}", 
                            readRow.LockAcquisition, DateTime.UtcNow, rTableConfigurationService.LockTimeout);
                        result = new TableResult()
                        {
                            Result = null,
                            Etag = null,
                            HttpStatusCode = (int)HttpStatusCode.Conflict
                        };
                    }
                }
                else
                {
                    // It is already committed, return the retrieved result from the head
                    result = retrievedResult;
                    if (virtualizeEtag) result.Etag = readRow.Version.ToString();
                }
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
                CloudTable tail = GetTailTableClient().GetTableReference(tableName);
                rows = tail.ExecuteQuery(query);
            }
            catch (Exception e)
            {
                Console.WriteLine("Error in ExecuteQuery: caught exception {0}", e);
            }

            return rows;
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
            CloudTable table = tableClient.GetTableReference(this.tableName);
            TableResult retrievedResult = null;

            try
            {
                retrievedResult = table.Execute(operation, requestOptions, operationContext);
            }
            catch (Exception e)
            {
                Console.WriteLine("Error in RetrieveFromReplica(): caught exception {0}", e);                
                return null;
            }

            // If we are able to retrieve an existing entity, 
            // then check consistency of viewId between the currentView and existing entity.
            if (retrievedResult != null)
            {
                IRTableEntity readRow = (IRTableEntity)(retrievedResult.Result);
                if (readRow != null && this.CurrentView != null && this.CurrentView.ViewId < readRow.ViewId)
                {
                    throw new RTableStaleViewException(
                        string.Format("current ViewId {0} is smaller than ViewId of existing row {1}",
                        this.CurrentView.ViewId.ToString(),
                        readRow.ViewId));
                }
            }
            
            return retrievedResult;
        }

        //
        // Flush2PC protocol: Executes chain 2PC protocol after a row is updated and locked at the head.
        //
        private TableResult Flush2PC(IRTableEntity row, TableRequestOptions requestOptions = null,
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

        private TableResult FlushPreparePhase(IRTableEntity row, TableRequestOptions requestOptions,
            OperationContext operationContext, string[] eTagsStrings)
        {
            TableResult result = new TableResult() {HttpStatusCode = (int) HttpStatusCode.OK};

            // PREPARE PHASE: Uses chain replication to prepare replicas starting from "head+1" to 
            // "tail" sequentially
            for (int index = 1; index <= CurrentView.TailIndex; index++)
            {
                if ((result = InsertUpdateOrDeleteRow(row, index, eTagsStrings[index], requestOptions, operationContext)) ==
                    null)
                {
                    // Failed in the middle, abort with error
                    Console.WriteLine(
                        "F2PC: Failed during prepare phase in 2PC at replica with index: {0} for row with row key: {1} \n",
                        index, row.RowKey);
                    return null;
                }

                // Cache the Etag for the commit phase
                eTagsStrings[index] = result.Etag;
            }

            return result;
        }

        private TableResult FlushCommitPhase(IRTableEntity row, TableRequestOptions requestOptions, OperationContext operationContext,
            string[] eTagStrings)
        {
            TableResult result = null;

            // COMMIT PHASE: Commits the replicas in the reverse order starting from the tail replica
            for (int index = CurrentView.TailIndex; index >= 0; index--)
            {
                row.RowLock = false;
                result = InsertUpdateOrDeleteRow(row, index, eTagStrings[index], requestOptions, operationContext);

                // It is possible that UpdateInsertOrDeleteRow() returns result.Result = null
                // It happens when the Head entry is Tombstone and the Tail entry is gone already.
                // So, just check for "result == null" and return error
                if (result == null)
                {
                    // Failed in the middle, abort with error
                    Console.WriteLine(
                        "F2PC: Failed during commit phase in 2PC at replica with index: {0} when reading a row with row key: {1}. \n",
                        index, row.RowKey);
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
        private TableResult InsertUpdateOrDeleteRow(IRTableEntity row, int index, string Etag,
            TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            // Read the row before updating it
            CloudTableClient tableClient = CurrentView[index];
            TableOperation operation = TableOperation.Retrieve<DynamicRTableEntity>(row.PartitionKey, row.RowKey);

            // If the Etag is supplied, this is an update or delete based on existing eTag
            // no need to retrieve
            if (Etag != null)
            {
                row.ETag = Etag;
                return UpdateOrDeleteRow(tableClient, row);
            }

            if (row.Operation == GetTableOperation(TableOperationType.Insert))
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
                Console.WriteLine(
                    "RMWR: Failed to access replica with index: {0} when reading a row with row key: {1}. \n", index,
                    row.RowKey);
                return null;
            }

            if (retrievedResult.HttpStatusCode == (int) HttpStatusCode.OK)
            {
                // Row is present, overwrite the row
                row.ETag = retrievedResult.Etag;
                return UpdateOrDeleteRow(tableClient, row);
            }

            if (retrievedResult.HttpStatusCode == (int) HttpStatusCode.NotFound || retrievedResult.Result == null)
            {
                // Row is not present, insert the row
                // **Except** when the row is being deleted
                // during recovery by another client, the operation type might not be delete, 
                // check for the presence of tombstone during commit phase
                if (row.Operation == GetTableOperation(TableOperationType.Delete) || (row.Tombstone && row.RowLock == false))
                {
                    return retrievedResult;
                }

                // non-delete operation: Row is not present, create the row and return the result
                TableResult result;
                if (((result = InsertRow(tableClient, row)) == null) ||
                    (result.HttpStatusCode != (int) HttpStatusCode.NoContent) || (result.Result == null))
                {
                    Console.WriteLine(
                        "RMWR: Failed at replica with index: {0} when inserting a new row with row key: {1}. \n", index,
                        row.RowKey);
                    return null;
                }
                return result;
            }

            Console.WriteLine("RMWR: Failed to access replica with index: {0}, error code = {1}. \n", index,
                retrievedResult.HttpStatusCode);
            return null;
        }

        private TableResult InsertRow(CloudTableClient tableClient, IRTableEntity row)
        {
            TableResult result;

            try
            {
                CloudTable table = tableClient.GetTableReference(tableName);
                TableOperation top = TableOperation.Insert(row);
                result = table.Execute(top);
            }
            catch (StorageException ex)
            {
                Console.WriteLine(ex.ToString());
                result = new TableResult() {Result = null, Etag = null, HttpStatusCode = (int)ex.RequestInformation.HttpStatusCode};
                return result;
            }
            catch (Exception e)
            {
                Console.WriteLine("TryInsertRow:Error: exception {0}", e);
                return null;
            }

            return result;
        }

        private TableResult UpdateOrDeleteRow(CloudTableClient tableClient, IRTableEntity row)
        {
            try
            {
                CloudTable table = tableClient.GetTableReference(tableName);
                if ((row.Tombstone == true) && (row.RowLock == false))
                {                    
                    // For delete operations, call the delete operation if it is being committed
                    TableOperation top = TableOperation.Delete(row);
                    return table.Execute(top);
                }
                else if (row.Operation == GetTableOperation(TableOperationType.Merge))
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
                Console.WriteLine("TryWriteConditionalRow:Error: exception {0}", e);
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
                        IRTableEntity row = (IRTableEntity) this.GetEntityFromOperation(operation);
                        row.RowLock = false;
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
                CloudTable table = tableClient.GetTableReference(this.tableName);
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
                Console.WriteLine("RunBatch: exception {0}", e);
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
                        if (i < batchSize/2)
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
        public ReconfigStatus RepairTable(int viewIdToRecoverFrom, TableBatchOperation unfinishedOps,
            long maxBatchSize = 100L)
        {
            ReconfigStatus status = (int) ReconfigStatus.SUCCESS;
            if (this.rTableConfigurationService.IsViewStable())
            {
                return ReconfigStatus.SUCCESS;
            }
            if (!this.ValidateViews())
            {
                return ReconfigStatus.FAULTY_WRITE_VIEW;
            }

            CloudTableClient writeHeadClient = CurrentView[CurrentView.WriteHeadIndex];
            CloudTable writeHeadTable = writeHeadClient.GetTableReference(this.tableName);

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
            CloudTable readHeadTable = readHeadTableClient.GetTableReference(this.tableName);

            DateTime startTime = DateTime.UtcNow;
            IQueryable<DynamicRTableEntity> query =
                from ent in readHeadTable.CreateQuery<DynamicRTableEntity>()
                where ent.ViewId >= viewIdToRecoverFrom
                select ent;

            foreach (DynamicRTableEntity entry in query)
            {
                Console.WriteLine("RepairReplica: repairing entity: Pk: {0}, Rk: {1}", entry.PartitionKey, entry.RowKey);

                TableResult result = RepairRow(entry.PartitionKey, entry.RowKey, null);

                if (result.HttpStatusCode != (int) HttpStatusCode.OK &&
                    result.HttpStatusCode != (int) HttpStatusCode.NoContent)
                {
                    status = ReconfigStatus.PARTIAL_FAILURE;
                }
            }

            // now find any entries that are in the write view but not in the read view
            query = from ent in writeHeadTable.CreateQuery<DynamicRTableEntity>()
                where ent.ViewId < CurrentView.ViewId
                select ent;

            foreach (DynamicRTableEntity extraEntity in query)
            {
                Console.WriteLine("RepairReplica: deleting entity pk: {0}, rk: {1}", extraEntity.PartitionKey, extraEntity.RowKey);

                TableResult result = RepairRow(extraEntity.PartitionKey, extraEntity.RowKey, null);

                if (result.HttpStatusCode != (int)HttpStatusCode.OK &&
                    result.HttpStatusCode != (int)HttpStatusCode.NoContent)
                {
                    status = ReconfigStatus.PARTIAL_FAILURE;
                }
            }

            Console.WriteLine("RepairTable: took {0}", DateTime.UtcNow - startTime);

            return status;
        }

        private void FlushRecoveryBatch(TableBatchOperation unfinishedOps, long maxBatchSize, ref ReconfigStatus status,
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
                        status = ReconfigStatus.UNLOCK_FAILURE;
                    }
                }
                else
                {
                    // Copy failed at the new replica, unlock the head first 
                    TableBatchOperation newOutBatch = new TableBatchOperation();
                    if (this.RunBatchSplit(headTableClient, batchHead, false, ref newOutBatch, null, null) == null)
                    {
                        // Oops, unlock failed
                        status |= ReconfigStatus.UNLOCK_FAILURE;
                    }

                    // Copy unfinished operations in the head to the unfinished batch
                    IEnumerator<TableOperation> enumerator = batchNewReplica.GetEnumerator();
                    while (enumerator.MoveNext())
                    {
                        if (!outBatch.Contains(enumerator.Current))
                        {
                            unfinishedOps.Add(enumerator.Current);
                            status |= ReconfigStatus.PARTIAL_FAILURE;
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
                    status |= ReconfigStatus.UNLOCK_FAILURE;
                }
                status |= ReconfigStatus.PARTIAL_FAILURE;
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
        public TableResult RepairRow(string partitionKey, string rowKey, IRTableEntity existingRow)
        {
            TableResult result = new TableResult() {HttpStatusCode = (int) HttpStatusCode.OK};

            // read from the head of the read view
            TableOperation operation = TableOperation.Retrieve<DynamicRTableEntity>(partitionKey, rowKey);
            TableResult readHeadResult = RetrieveFromReplica(operation, CurrentView.ReadHeadIndex);
            if (readHeadResult == null)
            {
                // If retrieve fails for some reason then return "service unavailable - 503 " error code
                return new TableResult()
                {
                    Result = null,
                    Etag = null,
                    HttpStatusCode = (int) HttpStatusCode.ServiceUnavailable
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

            DynamicRTableEntity writeViewEntity = null;
            if (writeHeadResult.HttpStatusCode == (int) HttpStatusCode.OK && writeHeadResult.Result != null)
            {
                writeViewEntity = (DynamicRTableEntity) writeHeadResult.Result;
                if (writeViewEntity.ViewId >= CurrentView.GetReplicaInfo(CurrentView.WriteHeadIndex).ViewInWhichAddedToChain)
                {
                    // nothing to repair in this case.
                    return result;
                }
            }

            // if row does not exist in the read view, delete it from the write view
            if (readHeadResult.Result == null || readHeadResult.HttpStatusCode == (int) HttpStatusCode.NoContent)
            {
                if (writeViewEntity != null)
                {
                    writeViewEntity.Tombstone = true;
                    writeViewEntity.RowLock = false;

                    // delete row from the write view
                    result = UpdateOrDeleteRow(CurrentView[CurrentView.WriteHeadIndex], writeViewEntity);
                    Console.WriteLine("RepairRow: attempt to delete from write head returned: {0}", result.HttpStatusCode);
                    return result;
                }
            }

            if (readHeadResult.HttpStatusCode != (int)HttpStatusCode.OK || readHeadResult.Result == null)
            {
                Console.WriteLine("RepairRow: unexpected result on the read view head: {0}", readHeadResult.HttpStatusCode);
                return readHeadResult;
            }

            IRTableEntity readHeadEntity = (IRTableEntity) readHeadResult.Result;
            bool readHeadLocked = readHeadEntity.RowLock;
            bool readHeadLockExpired = (readHeadEntity.LockAcquisition + rTableConfigurationService.LockTimeout >
                                        DateTime.UtcNow);

            // take a lock on the read view entity unless the entity is already locked
            readHeadEntity.RowLock = true;
            readHeadEntity.ViewId = CurrentView.ViewId;
            readHeadEntity.LockAcquisition = DateTime.UtcNow;
            readHeadEntity.Operation = GetTableOperation(TableOperationType.Replace);

            result = UpdateOrDeleteRow(CurrentView[CurrentView.ReadHeadIndex], readHeadEntity);
            if (result == null)
            {
                Console.WriteLine("RepairRow: failed to take lock on read head.");
                return null;
            }

            if (result.HttpStatusCode != (int) HttpStatusCode.OK && result.HttpStatusCode != (int) HttpStatusCode.NoContent)
            {
                Console.WriteLine("RepairRow: failed to take lock on read head: {0}", result.HttpStatusCode);
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
                Console.WriteLine("RepairRow: failed to write entity on the write view.");
                return null;
            }

            if (result.HttpStatusCode != (int) HttpStatusCode.OK && result.HttpStatusCode != (int) HttpStatusCode.NoContent)
            {
                Console.WriteLine("RepairRow: failed to write to write head: {0}", result.HttpStatusCode);
                return result;
            }

            string writeHeadEtagForCommit = result.Etag;

            if (!readHeadLocked)
            {
                readHeadEntity.RowLock = false;
                readHeadEntity.ETag = readHeadEtag;
                result = UpdateOrDeleteRow(CurrentView[CurrentView.ReadHeadIndex], readHeadEntity);
                if (result == null)
                {
                    Console.WriteLine("RepairRow: failed to unlock read view.");
                    return null;
                }

                if (result.HttpStatusCode != (int) HttpStatusCode.OK &&
                    result.HttpStatusCode != (int) HttpStatusCode.NoContent)
                {
                    Console.WriteLine("RepairRow: failed to unlock read head: {0}", result.HttpStatusCode);
                    return result;
                }

                Console.WriteLine("RepairRow: read head unlock result: {0}", result.HttpStatusCode);

                readHeadEntity.RowLock = false;
                readHeadEntity.ETag = writeHeadEtagForCommit;
                result = UpdateOrDeleteRow(CurrentView[CurrentView.WriteHeadIndex], readHeadEntity);

                if (result == null)
                {
                    Console.WriteLine("RepairRow: failed to unlock write view.");
                    return null;
                }

                if (result.HttpStatusCode != (int)HttpStatusCode.OK &&
                    result.HttpStatusCode != (int)HttpStatusCode.NoContent)
                {
                    Console.WriteLine("RepairRow: failed to unlock write head: {0}", result.HttpStatusCode);
                    return result;
                }

                Console.WriteLine("RepairRow: write head unlock result: {0}", result.HttpStatusCode);
            }

            // if the lock on the read head had expired, flush this row
            else if (readHeadLockExpired)
            {
                readHeadEntity.RowLock = true;
                result = Flush2PC(readHeadEntity, null, null, writeHeadEtagForCommit);
                if (result == null)
                {
                    Console.WriteLine("RepairRow: failed flush2pc on expired lock.");
                }
            }

            return result;
        }

        /// <summary>
        /// Call this function to convert all the remaining XStore Table entities to RTable entities.
        /// "remaining" XStore entities are those with ViewId == 0.
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
            
            if (this.rTableConfigurationService.ConvertXStoreTableMode == false)
            {
                throw new InvalidOperationException("ConvertXStoreTable() API is NOT supported when RTable is NOT in ConvertXStoreTableMode.");
            }

            int tailIndex = currentReadView.TailIndex;
            if (tailIndex != 0)
            {
                throw new ApplicationException("ConvertXStoreTable() API currently only supports one Replica.");
            }

            DateTime startTime = DateTime.UtcNow;
            Console.WriteLine("ConvertXStoreTable() started {0}", startTime);

            CloudTableClient tailTableClient = currentReadView[tailIndex];
            CloudTable tailTable = tailTableClient.GetTableReference(this.tableName);
            
            IQueryable<DynamicRTableEntity2> query =
                                    from ent in tailTable.CreateQuery<DynamicRTableEntity2>().AsQueryable<DynamicRTableEntity2>()                                    
                                    select ent;
            
            using (IEnumerator<DynamicRTableEntity2> enumerator = query.GetEnumerator())
            {
                while (enumerator.MoveNext())
                {
                    DynamicRTableEntity2 entity = enumerator.Current;
                    if (entity.ViewId != 0)
                    {
                        // ViewId = 0 means that the entity has not been operated on since ther XStore Table was converted to RTable.
                        // So, convert it manually now.
                        Console.WriteLine("Skipped XStore entity with Partition={0} Row={1}", entity.PartitionKey, entity.RowKey);
                        skippedCount++;
                        continue;
                    }
                    entity.ViewId = currentReadView.ViewId;
                    entity.Version = 1;
                    TableOperation top = TableOperation.Replace(entity);
                    try
                    {
                        TableResult result = tailTable.Execute(top, requestOptions, operationContext); // TODO context
                        successCount++;
                        Console.WriteLine("Converted XStore entity with Partition={0} Row={1}", entity.PartitionKey, entity.RowKey);
                    }
                    catch (Exception ex)
                    {
                        failedCount++;
                        Console.WriteLine("Exception when converting XStore entity with Partition={0} Row={1}. Ex = {2}", 
                            entity.PartitionKey, entity.RowKey, ex.ToString());
                    }
                }
            }
            DateTime endTime = DateTime.UtcNow;
            Console.WriteLine("ConvertXStoreTable() finished {0}. Time took to convert = {1}", endTime, endTime - startTime);
            Console.WriteLine("successCount={0} skippedCount={1} failedCount={2}", successCount, skippedCount, failedCount);
        }

    }


}