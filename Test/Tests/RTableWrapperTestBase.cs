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


using NUnit.Framework.Constraints;

namespace Microsoft.Azure.Toolkit.Replication.Test
{
    using Microsoft.WindowsAzure.Storage.Table;
    using NUnit.Framework;
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// Helper class to perform RTable operations using rtableWrapper.
    /// </summary>
    public class RTableWrapperTestBase : RTableLibraryTestBase
    {
        protected string message = "message";
        protected string updatedMessage = "updated-message";
        protected string updatedAgainMessage = "updated-again-message";

        protected int numberOfPartitions = 1;
        protected int numberOfRowsPerPartition = 1;


        #region Helper functions
        /// <summary>
        /// Helper function to retrieve the set of entities for the specified set of jobType and jobId.
        /// Validate that entity.Message matches "entityMessage")
        /// </summary>
        /// <param name="jobType"></param>
        /// <param name="jobId"></param>
        /// <param name="entityMessage"></param>
        /// <param name="checkViewId">True means validate _rtable_ViewId of the entity</param>
        protected void PerformRetrieveOperationAndValidate(string jobType, string jobId, string entityMessage, bool checkViewId = true)
        {
            Console.WriteLine("\nValidating Retrieve operation...");
            for (int i = 0; i < this.numberOfPartitions; i++)
            {
                List<string> rowKeys = new List<string>(); // list of rowKey for the given paritionKey
                string partitionKey;
                string rowKey;
                SampleRTableEntity.GenerateKeys(this.GenerateJobType(jobType, i), "don't care", out partitionKey, out rowKey);
                //
                // GetAllRows()
                //
                Console.WriteLine("Calling GetAllRows() for partition {0}", i);
                IEnumerable<SampleRTableEntity> allRows = this.rtableWrapper.GetAllRows(partitionKey);
                int j = 0; // counting the number of rows per partition
                foreach (var retrievedEntity in allRows)
                {
                    this.ValidateRetrievedRTableEntity(retrievedEntity, jobType, jobId, entityMessage, i, j, checkViewId);
                    rowKeys.Add(retrievedEntity.RowKey);
                    j++;
                }
                Assert.AreEqual(this.numberOfRowsPerPartition, j, "Partition {0} only has {1} rows. Expected {2} rows", i, j, this.numberOfRowsPerPartition);

                //
                // FindRow()
                //
                Console.WriteLine("Calling FindRow() for partitionKey={0}", partitionKey);
                for (j = 0; j < rowKeys.Count; j++)
                {
                    SampleRTableEntity retrievedEntity = this.rtableWrapper.FindRow(partitionKey, rowKeys[j]);
                    this.ValidateRetrievedRTableEntity(retrievedEntity, jobType, jobId, entityMessage, i, j, checkViewId);
                }
            }
            Console.WriteLine("Passed Retrieve validation.\n");
        }

        /// <summary>
        /// Helper function to validate the specified entries do not exist.
        /// </summary>
        /// <param name="jobType"></param>
        /// <param name="jobId"></param>
        protected void ValidateRetrieveNotFound(string jobType, string jobId)
        {
            Console.WriteLine("\nValidating Retrieve NotFound operation jobType={0} jobId={1}...", jobType, jobId);
            for (int i = 0; i < this.numberOfPartitions; i++)
            {
                List<string> rowKeys = new List<string>(); // list of rowKey for the given paritionKey
                string partitionKey;
                string rowKey;
                SampleRTableEntity.GenerateKeys(this.GenerateJobType(jobType, i), "don't care", out partitionKey, out rowKey);
                //
                // GetAllRows()
                //
                Console.WriteLine("Calling GetAllRows() for partition {0}", i);
                IEnumerable<SampleRTableEntity> allRows = this.rtableWrapper.GetAllRows(partitionKey);
                int count = 0; // counting the number of rows per partition
                List<SampleRTableEntity> allRetrievedEntities = new List<SampleRTableEntity>();
                foreach (var retrievedEntity in allRows)
                {
                    allRetrievedEntities.Add(retrievedEntity);
                    count++;
                }
                Assert.AreEqual(0, count, "GetAllRows() should return 0 entries.");
            }
            Console.WriteLine("Passed Retrieve validation.\n");
        }

        /// <summary>
        /// Helper function to perform Delete, InsertOrReplace, Merge, or Replace operation for the specified set of jobType and jobId.
        /// And validate the results.
        /// </summary>
        /// <param name="opType"></param>
        /// <param name="jobType"></param>
        /// <param name="jobId"></param>
        /// <param name="updatedEntityMessage"></param>
        protected void PerformOperationAndValidate(
            TableOperationType opType,
            string jobType,
            string jobId,
            string updatedEntityMessage = "")
        {
            Console.WriteLine("\nValidating {0} operation: updatedEntityMessage={1}...", opType, updatedEntityMessage);
            for (int i = 0; i < this.numberOfPartitions; i++)
            {
                List<string> rowKeys = new List<string>(); // list of rowKey for the given paritionKey
                string partitionKey;
                string rowKey;
                SampleRTableEntity.GenerateKeys(this.GenerateJobType(jobType, i), "don't care", out partitionKey, out rowKey);
                //
                // GetAllRows()
                //
                Console.WriteLine("Calling GetAllRows() and then ReplaceRow() for partition {0}", i);
                IEnumerable<SampleRTableEntity> allRows = this.rtableWrapper.GetAllRows(partitionKey);
                int j = 0; // counting the number of rows per partition
                List<SampleRTableEntity> allRetrievedEntities = new List<SampleRTableEntity>();
                foreach (var retrievedEntity in allRows)
                {
                    allRetrievedEntities.Add(retrievedEntity);
                    j++;
                }
                Assert.AreEqual(this.numberOfRowsPerPartition, j, "Partition {0} only has {1} rows. Expected {2} rows", i, j, this.numberOfRowsPerPartition);

                j = 0;
                foreach (var oneEntry in allRetrievedEntities)
                {
                    int attempts = 1;
                    bool passed = true;
                    while (attempts < 3)
                    {
                        try
                        {
                            SampleRTableEntity retrievedEntity = this.rtableWrapper.FindRow(partitionKey, oneEntry.RowKey);
                            Console.WriteLine("attempts={0}. partitionKey={1} rowKey={2}. Calling {3} API...",
                                attempts, partitionKey, oneEntry.RowKey, opType);
                            switch (opType)
                            {
                                case TableOperationType.Delete:
                                    {
                                        retrievedEntity.ETag = retrievedEntity._rtable_Version.ToString(); // set ETag
                                        this.rtableWrapper.DeleteRow(retrievedEntity);
                                    }
                                    break;
                                case TableOperationType.InsertOrReplace:
                                    {
                                        retrievedEntity.Message = this.GenerateMessage(updatedEntityMessage, i, j);
                                        this.rtableWrapper.InsertOrReplaceRow(retrievedEntity);
                                        rowKeys.Add(retrievedEntity.RowKey);
                                    }
                                    break;
                                case TableOperationType.Merge:
                                    {
                                        retrievedEntity.Message = this.GenerateMessage(updatedEntityMessage, i, j);
                                        retrievedEntity.ETag = retrievedEntity._rtable_Version.ToString(); // set ETag
                                        this.rtableWrapper.MergeRow(retrievedEntity);
                                        rowKeys.Add(retrievedEntity.RowKey);
                                    }
                                    break;
                                case TableOperationType.Replace:
                                    {
                                        retrievedEntity.Message = this.GenerateMessage(updatedEntityMessage, i, j);
                                        retrievedEntity.ETag = retrievedEntity._rtable_Version.ToString(); // set ETag
                                        this.rtableWrapper.ReplaceRow(retrievedEntity);
                                        rowKeys.Add(retrievedEntity.RowKey);
                                    }
                                    break;
                                default:
                                    {
                                        throw new InvalidOperationException(string.Format("opType={0} is NOT supported", opType));
                                    }
                            }
                            passed = true;
                            break; // get out of while(attempts) if no RTableConflictException
                        }
                        catch (RTableConflictException)
                        {
                            passed = false;
                            Console.WriteLine("Got RTableConflictException. attempts={0}", attempts);
                            attempts++;
                            System.Threading.Thread.Sleep(1000);
                        }
                    }
                    Assert.IsTrue(passed, "Keep getting RTableConflictException when calling {0} API", opType);
                    j++;
                }
                Assert.AreEqual(this.numberOfRowsPerPartition, j, "Partition {0} only has {1} rows. Expected {2} rows", i, j, this.numberOfRowsPerPartition);

                //
                // FindRow()
                //
                Console.WriteLine("Calling FindRow() for partitionKey={0}", partitionKey);
                for (j = 0; j < rowKeys.Count; j++)
                {
                    if (opType == TableOperationType.Delete)
                    {
                        try
                        {
                            SampleRTableEntity retrievedEntity = this.rtableWrapper.FindRow(partitionKey, rowKeys[j]);
                            Assert.Fail("After DeleteRow() was called, FindRow() did not throw RTableResourceNotFoundException");
                        }
                        catch (RTableResourceNotFoundException)
                        {
                        }
                    }
                    else
                    {
                        SampleRTableEntity retrievedEntity = this.rtableWrapper.FindRow(partitionKey, rowKeys[j]);
                        this.ValidateRetrievedRTableEntity(retrievedEntity, jobType, jobId, updatedEntityMessage, i, j);
                    }
                }
            }
            Console.WriteLine("Passed {0} validation.\n", opType);
        }


        /// <summary>
        /// Helper function to insert the specified set of jobType and jobId and validate the results.
        /// Make sure there are no existing entries for the jobType and jobId before calling this function.
        /// </summary>
        /// <param name="jobType"></param>
        /// <param name="jobId"></param>
        /// <param name="entityMessage"></param>
        protected void PerformInsertOperationAndValidate(string jobType, string jobId, string entityMessage)
        {
            Console.WriteLine("\nValidating Insert operation...");
            for (int i = 0; i < this.numberOfPartitions; i++)
            {
                List<string> rowKeys = new List<string>(); // list of rowKey for the given paritionKey
                string partitionKey;
                string rowKey;
                SampleRTableEntity.GenerateKeys(this.GenerateJobType(jobType, i), "don't care", out partitionKey, out rowKey);
                //
                // GetAllRows() to confirm nothing exists and then call InsertRow()
                //
                Console.WriteLine("Calling GetAllRows() for partition {0}, expecting 0 rows...", i);
                IEnumerable<SampleRTableEntity> allRows = this.rtableWrapper.GetAllRows(partitionKey);
                Assert.AreEqual(0, allRows.Count(), "Partition {0} should have 0 rows in order for InsertRow() to work.", i);

                for (int j = 0; j < this.numberOfRowsPerPartition; j++)
                {
                    SampleRTableEntity sampleRtableEntity = new SampleRTableEntity(
                        this.GenerateJobType(jobType, i),
                        this.GenerateJobId(jobId, i, j),
                        this.GenerateMessage(entityMessage, i, j));

                    int attempts = 1;
                    bool passed = true;
                    while (attempts < 3)
                    {
                        try
                        {
                            Console.WriteLine("attempts={0}. partitionKey={1} rowKey={2}. Calling InsertRow API...",
                                    attempts, partitionKey, sampleRtableEntity.RowKey);
                            this.rtableWrapper.InsertRow(sampleRtableEntity);
                            passed = true;
                            break; // get out of while(attempts) if no RTableConflictException
                        }                        
                        catch (RTableConflictException)
                        {
                            passed = false;
                            Console.WriteLine("Got RTableConflictException. attempts={0}", attempts);
                            attempts++;
                            System.Threading.Thread.Sleep(1000);
                        }
                    }
                    Assert.IsTrue(passed, "Keep getting RTableConflictException when calling InsertRow API");
                }

                //
                // FindRow()
                //
                Console.WriteLine("Calling FindRow() for partitionKey={0}", partitionKey);
                for (int j = 0; j < rowKeys.Count; j++)
                {
                    SampleRTableEntity retrievedEntity = this.rtableWrapper.FindRow(partitionKey, rowKeys[j]);
                    this.ValidateRetrievedRTableEntity(retrievedEntity, jobType, jobId, entityMessage, i, j);
                }
            }
            Console.WriteLine("Passed Insert validation.\n");
        }

        /// <summary>
        /// Helper function to insert the specified set of jobType and jobId. It is expected that InsertRow() will fail.
        /// </summary>
        /// <param name="jobType"></param>
        /// <param name="jobId"></param>
        /// <param name="entityMessage"></param>
        protected void PerformInsertOperationAndExpectToFail(string jobType, string jobId, string entityMessage)
        {
            Console.WriteLine("\nValidating Insert operation will fail. jobType={0} jobId={1}...", jobType, jobId);
            for (int i = 0; i < this.numberOfPartitions; i++)
            {
                List<string> rowKeys = new List<string>(); // list of rowKey for the given paritionKey
                string partitionKey;
                string rowKey;
                SampleRTableEntity.GenerateKeys(this.GenerateJobType(jobType, i), "don't care", out partitionKey, out rowKey);
                
                for (int j = 0; j < this.numberOfRowsPerPartition; j++)
                {
                    SampleRTableEntity sampleRtableEntity = new SampleRTableEntity(
                        this.GenerateJobType(jobType, i),
                        this.GenerateJobId(jobId, i, j),
                        this.GenerateMessage(entityMessage, i, j));

                    int attempts = 1;
                    bool failed = false;
                    while (attempts < 3)
                    {
                        try
                        {
                            Console.WriteLine("attempts={0}. partitionKey={1} rowKey={2}. Calling InsertRow API...",
                                    attempts, partitionKey, sampleRtableEntity.RowKey);
                            this.rtableWrapper.InsertRow(sampleRtableEntity);
                            failed = false;
                            break; // get out of while(attempts) if no RTableConflictException
                        }
                        catch (RTableConflictException)
                        {
                            failed = true;
                            Console.WriteLine("Got RTableConflictException. attempts={0}", attempts);
                            attempts++;
                            System.Threading.Thread.Sleep(1000);
                        }
                    }
                    Assert.IsTrue(failed, "InsertRow() actually passed when we expect it to fail!");
                }                
            }
            Console.WriteLine("Passed 'Insert will fail' validation.\n");
        }

        /// <summary>
        /// Helper function to perform Delete, InsertOrReplace, Merge, or Replace operation for the specified set of jobType and jobId.
        /// And validate the results.
        /// </summary>
        /// <param name="opType"></param>
        /// <param name="jobType"></param>
        /// <param name="jobId"></param>
        /// <param name="partition"></param>
        /// <param name="row"></param>
        /// <param name="originalMessage"></param>
        /// <param name="updatedMessage"></param>
        protected void PerformIndividualOperationAndValidate(
            TableOperationType opType,
            string jobType,
            string jobId,
            int partition,
            int row,
            string originalMessage,
            string updatedMessage = "")
        {
            Console.WriteLine("\nValidating {0} operation: updatedEntityMessage={1}...", opType, message);

            string partitionKey;
            string rowKey;
            SampleRTableEntity.GenerateKeys(this.GenerateJobType(jobType, partition), this.GenerateJobId(jobId, partition, row), out partitionKey, out rowKey);

            Console.WriteLine("PartitionKey = {0}, RowKey = {1}", partitionKey, rowKey);

            int attempts = 1;
            bool passed = true;
            bool resetRow = false;

            SampleRTableEntity retrievedEntity = null;
            while (attempts < 3)
            {
                try
                {
                    if (opType != TableOperationType.Insert)
                    {
                        retrievedEntity = this.rtableWrapper.FindRow(partitionKey, rowKey);    
                    }
                    
                    Console.WriteLine("attempts={0}. partitionKey={1} rowKey={2}. Calling {3} API...", attempts, partitionKey, rowKey, opType);
                    switch (opType)
                    {
                        case TableOperationType.Delete:
                            {
                                this.rtableWrapper.DeleteRow(retrievedEntity);

                                //Validate
                                try
                                {
                                    retrievedEntity = this.rtableWrapper.FindRow(partitionKey, rowKey);
                                    Assert.Fail("After DeleteRow() was called, FindRow() did not throw RTableResourceNotFoundException");
                                }
                                catch (RTableResourceNotFoundException)
                                {
                                }
                            }
                            break;
                        case TableOperationType.Insert:
                            {
                                SampleRTableEntity sampleRtableEntity = new SampleRTableEntity(
                                    this.GenerateJobType(jobType, partition),
                                    this.GenerateJobId(jobId, partition, row),
                                    this.GenerateMessage(originalMessage, partition, row));
                                this.rtableWrapper.InsertRow(sampleRtableEntity);

                                //Validate
                                try
                                {
                                    retrievedEntity = this.rtableWrapper.FindRow(partitionKey, rowKey);
                                }
                                catch (RTableResourceNotFoundException)
                                {
                                    Assert.Fail("After InsertRow() was called, FindRow() threw RTableResourceNotFoundException");
                                }
                            }
                            break;
                        case TableOperationType.InsertOrMerge:
                            {
                                retrievedEntity.Message = this.GenerateMessage(updatedMessage, partition, row);
                                this.rtableWrapper.InsertOrMergeRow(retrievedEntity);

                                retrievedEntity = this.rtableWrapper.FindRow(partitionKey, rowKey);
                                this.ValidateRetrievedRTableEntity(retrievedEntity, jobType, jobId, updatedMessage, partition, row);
                            }
                            break;
                        case TableOperationType.InsertOrReplace:
                            {
                                retrievedEntity.Message = this.GenerateMessage(updatedMessage, partition, row);
                                this.rtableWrapper.InsertOrReplaceRow(retrievedEntity);

                                retrievedEntity = this.rtableWrapper.FindRow(partitionKey, rowKey);
                                this.ValidateRetrievedRTableEntity(retrievedEntity, jobType, jobId, updatedMessage, partition, row);
                                resetRow = true;
                            }
                            break;
                        case TableOperationType.Merge:
                            {
                                retrievedEntity.Message = this.GenerateMessage(updatedMessage, partition, row);
                                this.rtableWrapper.MergeRow(retrievedEntity);

                                retrievedEntity = this.rtableWrapper.FindRow(partitionKey, rowKey);
                                this.ValidateRetrievedRTableEntity(retrievedEntity, jobType, jobId, updatedMessage, partition, row);
                            }
                            break;
                        case TableOperationType.Replace:
                            {
                                retrievedEntity.Message = this.GenerateMessage(updatedMessage, partition, row);
                                this.rtableWrapper.ReplaceRow(retrievedEntity);

                                retrievedEntity = this.rtableWrapper.FindRow(partitionKey, rowKey);
                                this.ValidateRetrievedRTableEntity(retrievedEntity, jobType, jobId, updatedMessage, partition, row);
                                resetRow = true;
                            }
                            break;
                        case TableOperationType.Retrieve:
                            {
                                this.rtableWrapper.FindRow(partitionKey, rowKey);
                            }
                            break;
                        default:
                            {
                                throw new InvalidOperationException(string.Format("opType={0} is NOT supported", opType));
                            }
                    }
                    passed = true;
                    break; // get out of while(attempts) if no RTableConflictException
                }
                catch (RTableConflictException)
                {
                    passed = false;
                    Console.WriteLine("Got RTableConflictException. attempts={0}", attempts);
                    attempts++;
                    System.Threading.Thread.Sleep(1000);
                }
            }
            Assert.IsTrue(passed, "Keep getting RTableConflictException when calling {0} API", opType);
            
            Console.WriteLine("Passed {0} validation.\n", opType);

            //Reset row to original values so subsequent updates on the same row get validated correctly
            if (resetRow == true)
            {
                Console.WriteLine("Resetting row to original values");
                retrievedEntity.Message = this.GenerateMessage(originalMessage, partition, row);
                this.rtableWrapper.ReplaceRow(retrievedEntity);
            }
        }

        /// <summary>
        /// Generate the actual jobType for the specified partition
        /// </summary>
        /// <param name="jobType"></param>
        /// <param name="partition"></param>
        /// <returns></returns>
        protected string GenerateJobType(string jobType, int partition)
        {
            return string.Format("{0}-{1}", jobType, partition);
        }

        /// <summary>
        /// Generate the actual jobId for the specified partition and row
        /// </summary>
        /// <param name="jobId"></param>
        /// <param name="partition"></param>
        /// <param name="row"></param>
        /// <returns></returns>
        protected string GenerateJobId(string jobId, int partition, int row)
        {
            return string.Format("{0}-{1}-{2}", jobId, partition, row);
        }

        /// <summary>
        /// Generate the actual Message for the specified partition and row
        /// </summary>
        /// <param name="message"></param>
        /// <param name="partition"></param>
        /// <param name="row"></param>
        /// <returns></returns>
        protected string GenerateMessage(string message, int partition, int row)
        {
            return string.Format("{0}-{1}-{2}", message, partition, row);
        }

        /// <summary>
        /// Helper function to validate whether "retrievedEntity" is correct or not.
        /// </summary>
        /// <param name="retrievedEntity"></param>
        /// <param name="jobType"></param>
        /// <param name="jobId"></param>
        /// <param name="message"></param>
        /// <param name="partition"></param>
        /// <param name="row"></param>
        /// <param name="checkViewId"></param>
        protected void ValidateRetrievedRTableEntity(
            SampleRTableEntity retrievedEntity, 
            string jobType, 
            string jobId, 
            string message, 
            int partition, 
            int row, 
            bool checkViewId = true)
        {
            Assert.AreEqual(
                this.GenerateJobType(jobType, partition),
                retrievedEntity.JobType,
                "JobType of retrievedEntity mismatch");
            Assert.AreEqual(
                this.GenerateJobId(jobId, partition, row),
                retrievedEntity.JobId,
                "JobId of retrievedEntity mismatch");
            Assert.AreEqual(
                this.GenerateMessage(message, partition, row),
                retrievedEntity.Message,
                "Message of retrievedEntity mismatch");
            if (checkViewId)
            {
                Assert.AreEqual(
                    this.configurationService.GetReadView().ViewId,
                    retrievedEntity._rtable_ViewId,
                    "_rtable_ViewId of retrievedEntity mismatch");
            }
        }

        #endregion Helper functions
    }
}
