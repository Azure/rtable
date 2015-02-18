namespace Microsoft.Azure.Toolkit.Replication.Test
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Threading;
    using Microsoft.WindowsAzure.Storage.Table;
    using Microsoft.Azure.Toolkit.Replication;

    public abstract class RTableWrapperBase<T> where T : ReplicatedTableEntity, new()
    {
        // Max number of retries in case of failure.
        public const int MaxRetries = 3;
        public const int MaxConflicts = 3;
        public const int WaitTimeForConflictRetryInSeconds = 5;

        private int lockExpirationTimeout = 0;

        public RTableWrapperBase(IReplicatedTable table, int lockExpirationTimeout = 0)
        {
            this.rTable = table;
            this.lockExpirationTimeout = lockExpirationTimeout;
        }

        public RTableWrapperBase(IReplicatedTable table)
        {
            this.rTable = table;
        }


        /// <summary>
        /// Inserts a row to a table if it doesnt already exist
        /// </summary>
        /// <param name="rEntity"></param>
        public void InsertRow(T rEntity)
        {
            TableOperation operation = TableOperation.Insert(rEntity);
            TableResult result = rTable.Execute(operation);
            // TODO What exception to throw if the item could not be inserted? (Maybe because it already exists... Found? Conflict?)
            HandleResult(result);
        }

        /// <summary>
        /// Inserts or replaces existing row with rEntity.
        /// Will throw exception if row exists but etags dont match
        /// </summary>
        /// <param name="rEntity">The row to be replaced</param>
        public void InsertOrReplaceRow(T rEntity)
        {
            TableOperation operation = TableOperation.InsertOrReplace(rEntity);
            TableResult result = rTable.Execute(operation);
            HandleResult(result);
        }

        /// <summary>
        /// replaces existing row with rEntity.
        /// Will throw exception if row doesnt exist of if etags dont match
        /// </summary>
        /// <param name="rEntity">The row to be replaced</param>
        public void ReplaceRow(T rEntity)
        {
            TableOperation operation = TableOperation.Replace(rEntity);
            TableResult result = rTable.Execute(operation);
            HandleResult(result);
        }

        /// <summary>
        /// Merges properties of rEntity with existing row.
        /// Will throw exception if row doesnt exist of if etags dont match
        /// </summary>
        /// <param name="rEntity">The row to be merged</param>
        public void MergeRow(T rEntity)
        {
            TableOperation operation = TableOperation.Merge(rEntity);
            TableResult result = rTable.Execute(operation);
            HandleResult(result);
        }

        /// <summary>
        /// Merges properties of rEntity with existing row. Inserts the row if it does not exist.
        /// Will throw exception if row doesnt exist of if etags dont match
        /// </summary>
        /// <param name="rEntity">The row to be merged</param>
        public void InsertOrMergeRow(T rEntity)
        {
            TableOperation operation = TableOperation.InsertOrMerge(rEntity);
            TableResult result = rTable.Execute(operation);
            HandleResult(result);
        }

        /// <summary>
        /// Returns a row with the given partition key and row key
        /// Will throw exception if row doesnt exist
        /// </summary>
        /// <param name="partitionKey">The partition key for the row</param>
        /// <param name="rowKey">The row key for the row</param>
        /// <returns></returns>
        public T FindRow(string partitionKey, string rowKey)
        {
            TableOperation operation = TableOperation.Retrieve<T>(partitionKey, rowKey);
            TableResult result = rTable.Execute(operation);
            HandleResult(result);
            return (T)result.Result;
        }

        /// <summary>
        /// Deletes the given row.
        /// Will throw exception if row is not found.
        /// </summary>
        /// <param name="rEntity"></param>
        public void DeleteRow(T rEntity)
        {
            TableOperation operation = TableOperation.Delete(rEntity);
            TableResult result = rTable.Execute(operation);
            HandleResult(result);
        }

        /// <summary>
        /// Finds a row with given partition key and row key. Will create the row if it is not found.
        /// Handles conflicts of the kind where someone else creates the row in between.
        /// This function will retry the operation (can set the  retry count) in case of retriable errors. 
        /// </summary>
        /// <param name="partitionKey"></param>      
        /// <param name="rowKey"></param>      
        /// <param name="retryCount">Number of times to retry before giving up</param>
        /// <returns>The row, if it exists</returns>
        public T FindRowWithRetry(string partitionKey, string rowKey, int retryCount = MaxRetries)
        {
            int myRetryCount = 0;
            while (true)
            {
                try
                {
                    T tableRow = FindRow(partitionKey, rowKey);
                    return tableRow;
                }
                catch (RTableRetriableException)
                {
                    if (myRetryCount++ == retryCount)
                    {
                        Console.WriteLine("Error finding row with partition key:{0}, row key:{1} to table {2} after {3} retires",
                            partitionKey, rowKey, rTable.TableName, myRetryCount);
                        throw new RTableException(String.Format("Unable to find row in table {0} after {1} retries ", rTable.TableName, retryCount));
                    }
                }
                // Anything else will get thrown
            }
        }


        /// <summary>
        /// Finds a row with given partition key and row key. Will create the row if it is not found.
        /// Handles conflicts of the kind where someone else creates the row in between.
        /// This function will retry the operation (can set the  retry count) in case of retriable errors. 
        /// </summary>
        /// <param name="newRow">The row to be created if not found</param>      
        /// <param name="retryCount">Number of times to retry before giving up</param>
        /// <returns></returns>
        public T FindOrInsertRowWithRetry(T newRow, int retryCount = MaxRetries)
        {
            int myRetryCount = 0;
            int myConflictCount = 0;
            while (true)
            {
                try
                {
                    T tableRow = FindRow(newRow.PartitionKey, newRow.RowKey);
                    return tableRow;
                }
                catch (RTableRetriableException)
                {
                    if (myRetryCount++ == retryCount)
                    {
                        Console.WriteLine("Error adding row with partition key:{0}, row key:{1} to table {2} after {3} retires",
                            newRow.PartitionKey, newRow.RowKey, rTable.TableName, myRetryCount);
                        throw new RTableException(String.Format("Unable to add row to table {0} after {1} retries ", rTable.TableName, retryCount));
                    }
                }
                catch (RTableResourceNotFoundException)
                {
                    // Row not foud, try and add it
                    try
                    {
                        InsertRow(newRow);
                        return newRow;
                    }
                    catch (RTableConflictException)
                    {
                        Console.WriteLine("Conflict while adding row with partition key {0} " +
                                                           "row key {1} to table:{2}", newRow.PartitionKey, newRow.RowKey,
                                                           rTable.TableName);
                        // This means someone added the row after we did a find.
                        // We go back and read that 
                        if (myConflictCount++ == MaxConflicts)
                        {
                            Console.WriteLine("Error adding row with partition key:{0}, row key:{1} to table {2} after {3} conflicts",
                                newRow.PartitionKey, newRow.RowKey, rTable.TableName, myConflictCount);
                            throw new RTableConflictException(String.Format("Unable to insert row with partition key:{0}, " +
                                                  "row key {1} due to continuous conflicts", newRow.PartitionKey, newRow.RowKey));
                        }
                    }
                    catch (RTableRetriableException)
                    {
                        if (myRetryCount++ == retryCount)
                        {
                            Console.WriteLine("Error adding row with partition key:{0}, row key:{1} to table {2} after {3} retires",
                                newRow.PartitionKey, newRow.RowKey, rTable.TableName, myRetryCount);
                            throw new RTableException(String.Format("Unable to insert row with partition key:{0}, " +
                                                  "row key {1} after {2} retries ", newRow.PartitionKey, newRow.RowKey, retryCount));
                        }
                    }
                    // Anything else will get thrown
                }
            }
        }

        /// <summary>
        /// Read-modify-write the entry
        /// Will throw exception if the entry is not found
        /// </summary>
        /// <param name="newRow">The row that replaces the existing row</param>
        public void ReplaceRowWithRetry(T newRow)
        {
            ReplaceRowWithRetryInternal(newRow, false, MaxRetries, MaxConflicts);
        }

        /// <summary>
        /// Read-modify-write the entry
        /// Will insert the row if it is not found
        /// </summary>
        /// <param name="newRow">The row that replaces the existing row</param>
        public void InsertOrReplaceRowWithRetry(T newRow)
        {
            ReplaceRowWithRetryInternal(newRow, true, MaxRetries, MaxConflicts);
        }

        /// <summary>
        /// Replace the existing row, but only if therse is no conflict
        /// </summary>
        /// <param name="newRow">The row that replaces the existing row</param>
        public void ReplaceRowWithoutConflict(T newRow)
        {
            ReplaceRowWithRetryInternal(newRow, false, MaxRetries, 0);
        }

        /// <summary>
        /// Replace the existing row, but only if therse is no conflict
        /// </summary>
        /// <param name="newRow">The row that replaces the existing row</param>
        public bool TryReplaceRowWithoutConflict(T newRow)
        {
            try
            {
                ReplaceRowWithoutConflict(newRow);
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error replacing row in Rtable: {0}", ex);
            }
            return false;
        }

        public IEnumerable<T> GetAllRows(string partitionKey)
        {
            TableQuery<T> query = new TableQuery<T>()
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));

            return rTable.ExecuteQuery<T>(query);
        }

        /// <summary>
        /// Read-modify-write with retry.
        /// This function merges rEntity with an existing row.
        /// If there is a conflict because someone modified the row in between.
        /// the function retries the merge. The function will also retry on retriable errors.
        /// Row not being found will cause an exception.
        /// </summary>
        /// <param name="newRow">The row that will replace the existing row</param>
        /// <param name="insertIfNotFound">Number of retires before giving up</param>
        /// <param name="retryCount">Number of retires before giving up</param>
        /// <param name="retryOnConflictCount">Number of retires on conflicts before giving up</param>
        private void ReplaceRowWithRetryInternal(T newRow, bool insertIfNotFound, int retryCount, int retryOnConflictCount)
        {
            DateTime startTime = DateTime.UtcNow;

            int myRetryCount = 0;
            int myConflictCount = 0;
            T tableRow = newRow;

            while (true)
            {
                try
                {
                    if (insertIfNotFound)
                    {
                        InsertOrReplaceRow(tableRow);
                    }
                    else
                    {
                        ReplaceRow(tableRow);
                    }
                    return;
                }
                catch (RTableResourceNotFoundException)
                {
                    Console.WriteLine("Error replacing row with partition key:{0}, row key:{1} in table {2}, row not found",
                        newRow.PartitionKey, newRow.RowKey, rTable.TableName);
                    throw new RTableResourceNotFoundException(String.Format("Unable to find row with partition key:{0}, " +
                        "row key {1} ", newRow.PartitionKey, newRow.RowKey));
                }
                catch (RTableRetriableException)
                {
                    if (myRetryCount++ == retryCount)
                    {
                        Console.WriteLine("Error replacing row with partition key:{0}, row key:{1} to table {2} after {3} retries",
                            newRow.PartitionKey, newRow.RowKey, rTable.TableName, myRetryCount);
                        throw new RTableException(String.Format("Unable to update row with partition key:{0}, " +
                                              "row key {1} after {2} retries ", newRow.PartitionKey, newRow.RowKey, retryCount));
                    }
                }
                catch (RTableConflictException)
                {
                    Console.WriteLine("Conflict while adding row with partition key {0} " +
                                                       "row key {1} to table:{2}", newRow.PartitionKey, newRow.RowKey,
                                                       rTable.TableName);

                    //There are two cases under which conflct can happen in rtable
                    //  1. Multiple writes race with each other. This is same as conflict in Azure Tables
                    //  2. The lock bit on the head is true and DateTime.UtcNow - entity.LockAcquisitionTime < LockTimeoutInSeconds
                    //As such, only when we have waited for a duration >= LockTimeoutInSeconds and retry counts have exceeded the threshold, 
                    //we can declare the write operation as truly failed. Until then, we have to retry, with a short wait time of WaitTimeForConflictRetryInSeconds
                    //between each retry attempt.
                    TimeSpan totalRetryInterval = DateTime.UtcNow - startTime;
                    if (++myConflictCount > retryOnConflictCount &&
                        (totalRetryInterval > TimeSpan.FromSeconds(Constants.LockTimeoutInSeconds)))
                    {
                        Console.WriteLine("Error replacing row with partition key:{0}, row key:{1} to table {2} after {3} conflicts",
                            newRow.PartitionKey, newRow.RowKey, rTable.TableName, myConflictCount);

                        if (totalRetryInterval > TimeSpan.FromSeconds(Constants.LockTimeoutInSeconds))
                        {
                            Console.WriteLine("Error due to conflict even after lock timeout");
                        }

                        throw new RTableConflictException(String.Format("Unable to update row with partition key:{0}, " +
                                              "row key {1} due to continuous conflicts", newRow.PartitionKey, newRow.RowKey));
                    }

                    //Wait for a short duration till the next attempt
                    Thread.Sleep(TimeSpan.FromSeconds(WaitTimeForConflictRetryInSeconds));

                    // Someone updated the row before us, read the latest row
                    try
                    {
                        tableRow = FindRow(newRow.PartitionKey, newRow.RowKey);
                        ModifyRowData(tableRow, newRow);
                    }
                    catch (RTableResourceNotFoundException)
                    {
                        // This means someone deleted the row, we go back and insert the row
                        insertIfNotFound = true;
                    }
                    catch (RTableRetriableException)
                    {
                        if (myRetryCount++ == retryCount)
                        {
                            Console.WriteLine("Error replacing row with partition key:{0}, row key:{1} to table {2} after {3} retries",
                                newRow.PartitionKey, newRow.RowKey, rTable.TableName, myRetryCount);
                            throw new RTableException(String.Format("Unable to update row with partition key:{0}, " +
                                                  "row key {1} after {2} retries ", newRow.PartitionKey, newRow.RowKey, retryCount));
                        }
                    }
                    // Anything else will get thrown
                }
            }
        }


        private void HandleResult(TableResult result)
        {
            if (result == null || result.Equals(null))
            {
                throw new RTableResourceNotFoundException("The resource could not be found or time out.");
            }
            if (result.HttpStatusCode == (int)HttpStatusCode.NoContent)
            {
                return;
            }
            else if (result.HttpStatusCode == (int)HttpStatusCode.ServiceUnavailable)
            {
                throw new RTableRetriableException("The operation should be retried.");
            }
            else if (result.HttpStatusCode == (int)HttpStatusCode.Conflict ||
                     result.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
            {
                throw new RTableConflictException("A conflict occurred.");
            }
            else if (result.Result == null || result.HttpStatusCode == (int)HttpStatusCode.NotFound)
            {
                throw new RTableResourceNotFoundException(
                    string.Format("http not found error, http status code: {0}, result {1}",
                        result.HttpStatusCode, result.Result));
            }
        }

        /// <summary>
        /// Called from UpdateRowWithRetry, this function should return the
        /// row to be merged.
        /// </summary>
        /// <param name="newRow">The row contains data passed by the caller</param>
        /// <param name="tableRow">The row contains data currently in the table</param>
        protected abstract void ModifyRowData(T tableRow, T newRow);

        protected IReplicatedTable rTable { get; set; }

    }
}
