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

namespace Microsoft.Azure.Toolkit.Replication.Test
{
    using Microsoft.Azure.Toolkit.Replication;
    using Microsoft.WindowsAzure.Storage.RTableTest;
    using Microsoft.WindowsAzure.Storage.Table;
    using Microsoft.WindowsAzure.Test.Network;
    using Microsoft.WindowsAzure.Test.Network.Behaviors;
    using NUnit.Framework;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Net;
    using System.Text;
    using System.Threading;

    /// <summary>
    /// The scenario is this: 
    /// Either the Head or Tail storage account suffers a short outage. 
    /// During the short outage, we make some Table API calls which are expected to fail.
    /// After the storage account has recovered from that short outage, we want to confirm that,
    /// Table API calls (to the same row/entity) will work as expected. 
    /// In other words, the system can resume normal operation after a short outage.
    /// </summary>
    [TestFixture]
    [Parallelizable(ParallelScope.None)]
    public class ShortOutageTests : HttpManglerTestBase
    {
        [OneTimeSetUp]
        public void TestFixtureSetup()
        {
            this.OneTimeSetUpInternal();
        }

        [OneTimeTearDown]
        public void TestFixtureTearDown()
        {
            base.DeleteAllRtableResources();
        }
        
        //
        // binaries.amd64fre\Gateway\UnitTests>
        // runuts /run=Microsoft.WindowsAzure.Storage.RTableHttpManglerTest.ShortOutageTests /verbose
        //

        //
        // NOTE:
        // Run the tests that deal with the first storage account first, then the second storage account, then the third storage account.
        // It fixes the HttpMangler issues for now. Still need to figure out the root cause.
        //

        /// <summary>
        /// Call ReplaceRow() during Head storage account outage. Expected operation to fail.
        /// After recovery, expect ReplaceRow() API to work for the same row.
        /// </summary>
        [Test(Description = "Call ReplaceRow() API during short outage at the Head storage account.")]
        public void A00TamperReplaceRowHeadTest()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);
            
            string entityPartitionKey = "jobType-TamperReplaceRowHeadTest";
            string entityRowKey = "jobId-TamperReplaceRowHeadTest";
            
            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            int targetStorageAccount = 0;
            TargetRTableWrapperApi<SampleRTableEntity> targetApi = this.rtableWrapper.ReplaceRow;
            bool targetApiExpectedToFail = true;
            bool checkOriginalEntityUnchanged = true;
            bool checkStorageAccountsConsistent = true;

            DateTime httpManglerStartTime;
            this.SetupAndRunTamperTableBehavior(
                entityPartitionKey, 
                entityRowKey,
                targetStorageAccount, 
                targetApi, 
                targetApiExpectedToFail,
                checkOriginalEntityUnchanged,
                checkStorageAccountsConsistent,
                out httpManglerStartTime);
           
            // After recovery from short outage, confirm that we can update the row.
            this.ExecuteReplaceRowAndValidate(entityPartitionKey, entityRowKey);
        }

        /// <summary>
        /// Call ReplaceRow() during Head storage account outage. Expected operation to fail.
        /// After recovery, expect ReplaceRow() API to work for the same row.
        /// Use HandleFiddlerEvent_DEBUG() in HttpMangler.cs for debugging.
        /// </summary>
        [Test(Description = "Call ReplaceRow() API during short outage at the Head storage account. Skipping initial sessions to Head storage account")]
        public void A00TamperSkipReplaceRowHeadTest()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);

            string entityPartitionKey = "jobType-TamperSkipReplaceRowHeadTest";
            string entityRowKey = "jobId-TamperSkipReplaceRowHeadTest";

            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            int targetStorageAccount = 0;
            TargetRTableWrapperApi<SampleRTableEntity> targetApi = this.rtableWrapper.ReplaceRow;
            bool targetApiExpectedToFail = true;
            bool checkOriginalEntityUnchanged = false;
            bool checkStorageAccountsConsistent = false;
            int skipInitialSessions = 1;

            this.SetupAndRunTamperTableBehavior(
                entityPartitionKey,
                entityRowKey,
                targetStorageAccount,
                targetApi,
                targetApiExpectedToFail,                
                checkOriginalEntityUnchanged,
                checkStorageAccountsConsistent,
                skipInitialSessions);

            // After recovery from short outage, confirm that we can update the row.
            this.ExecuteReplaceRowAndValidate(entityPartitionKey, entityRowKey);
        }

        /// <summary>
        /// Call ReplaceRow() during Inner storage account outage. Expected operation to fail.
        /// After recovery, expect ReplaceRow() API to work for the same row.
        /// </summary>
        [Test(Description = "Call ReplaceRow() API during short outage at the Inner storage account.")]
        public void B00TamperReplaceRowInnerTest()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);
            
            string entityPartitionKey = "jobType-TamperReplaceRowInnerTest";
            string entityRowKey = "jobId-TamperReplaceRowInnerTest";

            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            int targetStorageAccount = 1;
            TargetRTableWrapperApi<SampleRTableEntity> targetApi = this.rtableWrapper.ReplaceRow;
            bool targetApiExpectedToFail = true;
            bool checkOriginalEntityUnchanged = true;
            bool checkStorageAccountsConsistent = false;

            DateTime httpManglerStartTime;
            this.SetupAndRunTamperTableBehavior(
                entityPartitionKey,
                entityRowKey,
                targetStorageAccount,
                targetApi,
                targetApiExpectedToFail,
                checkOriginalEntityUnchanged,
                checkStorageAccountsConsistent,
                out httpManglerStartTime);

            // After recovery from short outage, sleep some time to wait for entity to be unlocked, confirm that we can update the row.
            this.SleepUntilRowLockHasExpired(httpManglerStartTime);

            this.ExecuteReplaceRowAndValidate(entityPartitionKey, entityRowKey);
        }

        /// <summary>
        /// Call ReplaceRow() during Inner storage account outage. Expected operation to fail.
        /// After recovery, expect ReplaceRow() API to work for the same row.
        /// </summary>
        [Test(Description = "Call ReplaceRow() API during short outage at the Inner storage account. Skipping initial sessions to Inner storage account")]
        public void B00TamperSkipReplaceRowInnerTest()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);

            string entityPartitionKey = "jobType-TamperSkipReplaceRowInnerTest";
            string entityRowKey = "jobId-TamperSkipReplaceRowInnerTest";

            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            int targetStorageAccount = 1;
            TargetRTableWrapperApi<SampleRTableEntity> targetApi = this.rtableWrapper.ReplaceRow;
            bool targetApiExpectedToFail = true;
            int skipInitialSessions = 2;
            bool checkOriginalEntityUnchanged = false;
            bool checkStorageAccountsConsistent = false;

            DateTime httpManglerStartTime;
            this.SetupAndRunTamperTableBehavior(
                entityPartitionKey,
                entityRowKey,
                targetStorageAccount,
                targetApi,
                targetApiExpectedToFail,
                checkOriginalEntityUnchanged,
                checkStorageAccountsConsistent,
                out httpManglerStartTime,
                skipInitialSessions);

            // After recovery from short outage, sleep some time to wait for entity to be unlocked, then confirm that we can update the row.
            this.SleepUntilRowLockHasExpired(httpManglerStartTime);

            this.ExecuteReplaceRowAndValidate(entityPartitionKey, entityRowKey);
        }

        /// <summary>
        /// Call ReplaceRow() during Tail storage account outage. Expected operation to fail.
        /// After recovery, expect ReplaceRow() API to work for the same row.
        /// </summary>
        [Test(Description = "Call ReplaceRow() API during short outage at the Tail storage account.")]
        public void C00TamperReplaceRowTailTest()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);
            
            string entityPartitionKey = "jobType-TamperReplaceRowTailTest";
            string entityRowKey = "jobId-TamperReplaceRowTailTest";

            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            int targetStorageAccount = this.actualStorageAccountsUsed.Count - 1;
            TargetRTableWrapperApi<SampleRTableEntity> targetApi = this.rtableWrapper.ReplaceRow;
            bool targetApiExpectedToFail = true;
            bool checkOriginalEntityUnchanged = true;
            bool checkStorageAccountsConsistent = false;

            DateTime httpManglerStartTime;
            this.SetupAndRunTamperTableBehavior(
                entityPartitionKey,
                entityRowKey,
                targetStorageAccount,
                targetApi,
                targetApiExpectedToFail,
                checkOriginalEntityUnchanged,
                checkStorageAccountsConsistent,
                out httpManglerStartTime);

            // After recovery from short outage, sleep some time to wait for entity to be unlocked, then confirm that we can update the row.
            this.SleepUntilRowLockHasExpired(httpManglerStartTime);

            this.ExecuteReplaceRowAndValidate(entityPartitionKey, entityRowKey);
        }        

        /// <summary>
        /// Call DeleteRow() during Head storage account outage. Expected operation to fail.
        /// After recovery, expect DeleteRow() API to work for the same row.
        /// </summary>
        [Test(Description = "Call DeleteRow() API during short outage at the Head storage account.")]
        public void A00TamperDeleteRowHeadTest()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);
            
            string entityPartitionKey = "jobType-TamperDeleteRowHeadTest";
            string entityRowKey = "jobId-TamperDeleteRowHeadTest";

            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            int targetStorageAccount = 0;
            TargetRTableWrapperApi<SampleRTableEntity> targetApi = this.rtableWrapper.DeleteRow;
            bool targetApiExpectedToFail = true;
            bool checkOriginalEntityUnchanged = true;
            bool checkStorageAccountsConsistent = true;

            this.SetupAndRunTamperTableBehavior(
                entityPartitionKey,
                entityRowKey,
                targetStorageAccount,
                targetApi,
                targetApiExpectedToFail,
                checkOriginalEntityUnchanged,
                checkStorageAccountsConsistent);

            if (targetApiExpectedToFail)
            {
                // After recovery from outage, confirm that we can delete the row.
                this.ExecuteDeleteRowAndValidate(entityPartitionKey, entityRowKey);
            }
            else
            {
                // After recovery from outage, confirm the entity is gone.
                this.ExecuteReadRowAndValidateNotExist(entityPartitionKey, entityRowKey);
            }
            // Confirm we can create entity using the same partition and row keys again.
            this.ExecuteCreateRowAndValidate(entityPartitionKey, entityRowKey);
        }

        /// <summary>
        /// Call DeleteRow() during Head storage account outage. Skip the first sessions. Expected operation to fail.
        /// After recovery, expect DeleteRow() API to work for the same row.
        /// </summary>
        [Test(Description = "Call DeleteRow() API during short outage at the Head storage account. Skipping initial sessions to Head storage account.")]
        public void A00TamperSkipDeleteRowHeadTest()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);

            string entityPartitionKey = "jobType-TamperSkipDeleteRowHeadTest";
            string entityRowKey = "jobId-TamperSkipDeleteRowHeadTest";

            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            int targetStorageAccount = 0;
            TargetRTableWrapperApi<SampleRTableEntity> targetApi = this.rtableWrapper.DeleteRow;
            bool targetApiExpectedToFail = true;
            bool checkOriginalEntityUnchanged = false;
            bool checkStorageAccountsConsistent = false;
            int skipInitialiSessions = 2;

            this.SetupAndRunTamperTableBehavior(
                entityPartitionKey,
                entityRowKey,
                targetStorageAccount,
                targetApi,
                targetApiExpectedToFail,
                checkOriginalEntityUnchanged,
                checkStorageAccountsConsistent,
                skipInitialiSessions);

            if (targetApiExpectedToFail)
            {
                // After recovery from outage, confirm that we can delete the row.
                this.ExecuteDeleteRowAndValidate(entityPartitionKey, entityRowKey);
            }
            else
            {
                // After recovery from outage, confirm the entity is gone.
                this.ExecuteReadRowAndValidateNotExist(entityPartitionKey, entityRowKey);
            }
            // Confirm we can create entity using the same partition and row keys again.
            this.ExecuteCreateRowAndValidate(entityPartitionKey, entityRowKey);
        }

        /// <summary>
        /// Call DeleteRow() during Inner storage account outage. Expected operation to fail.
        /// After recovery, expect DeleteRow() API to work for the same row.
        /// </summary>
        [Test(Description = "Call DeleteRow() API during short outage at the Inner storage account.")]
        public void B00TamperDeleteRowInnerTest()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);
            
            string entityPartitionKey = "jobType-TamperDeleteRowInnerTest";
            string entityRowKey = "jobId-TamperDeleteRowInnerTest";

            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            int targetStorageAccount = 1;
            TargetRTableWrapperApi<SampleRTableEntity> targetApi = this.rtableWrapper.DeleteRow;
            bool targetApiExpectedToFail = true;
            bool checkOriginalEntityUnchanged = true;
            bool checkStorageAccountsConsistent = false;

            DateTime httpManglerStartTime;
            this.SetupAndRunTamperTableBehavior(
                entityPartitionKey,
                entityRowKey,
                targetStorageAccount,
                targetApi,
                targetApiExpectedToFail,
                checkOriginalEntityUnchanged,
                checkStorageAccountsConsistent,
                out httpManglerStartTime);

            if (targetApiExpectedToFail)
            {
                // After recovery from outage, sleep some time to wait for entity to be unlocked, then confirm that we can delete the row.
                this.SleepUntilRowLockHasExpired(httpManglerStartTime);

                this.ExecuteDeleteRowAndValidate(entityPartitionKey, entityRowKey);
            }
            else
            {
                // After recovery from outage, confirm the entity is gone.
                this.ExecuteReadRowAndValidateNotExist(entityPartitionKey, entityRowKey);
            }
            // Confirm we can create entity using the same partition and row keys again.
            this.ExecuteCreateRowAndValidate(entityPartitionKey, entityRowKey);
        }

        /// <summary>
        /// Call DeleteRow() during Inner storage account outage. Expected operation to fail.
        /// After recovery, expect DeleteRow() API to work for the same row.
        /// </summary>
        [Test(Description = "Call DeleteRow() API during short outage at the Inner storage account. Skipping initial sessions to Inner storage account.")]
        public void B00TamperSkipDeleteRowInnerTest()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);

            string entityPartitionKey = "jobType-TamperSkipDeleteRowInnerTest";
            string entityRowKey = "jobId-TamperSkipDeleteRowInnerTest";

            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            int targetStorageAccount = 1;
            TargetRTableWrapperApi<SampleRTableEntity> targetApi = this.rtableWrapper.DeleteRow;
            bool targetApiExpectedToFail = true;
            int skipInitialiSessions = 2;
            bool checkOriginalEntityUnchanged = false;
            bool checkStorageAccountsConsistent = false;

            DateTime httpManglerStartTime;
            this.SetupAndRunTamperTableBehavior(
                entityPartitionKey,
                entityRowKey,
                targetStorageAccount,
                targetApi,
                targetApiExpectedToFail,
                checkOriginalEntityUnchanged,
                checkStorageAccountsConsistent,
                out httpManglerStartTime,
                skipInitialiSessions);

            if (targetApiExpectedToFail)
            {
                // After recovery from outage, sleep some time to wait for entity to be unlocked, then confirm that we can delete the row.
                this.SleepUntilRowLockHasExpired(httpManglerStartTime);

                this.ExecuteDeleteRowAndValidate(entityPartitionKey, entityRowKey);
            }
            else
            {
                // After recovery from outage, confirm the entity is gone.
                this.ExecuteReadRowAndValidateNotExist(entityPartitionKey, entityRowKey);
            }
            // Confirm we can create entity using the same partition and row keys again.
            this.ExecuteCreateRowAndValidate(entityPartitionKey, entityRowKey);
        }     

        /// <summary>
        /// Call DeleteRow() during Head storage account outage. Expected operation to fail.
        /// After recovery, expect DeleteRow() API to work for the same row.
        /// </summary>
        [Test(Description = "Call DeleteRow() API during short outage at the Tail storage account.")]
        public void C00TamperDeleteRowTailTest()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);
            
            string entityPartitionKey = "jobType-TamperDeleteRowTailTest";
            string entityRowKey = "jobId-TamperDeleteRowTailTest";

            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            int targetStorageAccount = this.actualStorageAccountsUsed.Count - 1;
            TargetRTableWrapperApi<SampleRTableEntity> targetApi = this.rtableWrapper.DeleteRow;
            bool targetApiExpectedToFail = true;
            bool checkOriginalEntityUnchanged = true;
            bool checkStorageAccountsConsistent = false;

            DateTime httpManglerStartTime;
            this.SetupAndRunTamperTableBehavior(
                entityPartitionKey, 
                entityRowKey,
                targetStorageAccount, 
                targetApi, 
                targetApiExpectedToFail,
                checkOriginalEntityUnchanged,
                checkStorageAccountsConsistent,
                out httpManglerStartTime);

            if (targetApiExpectedToFail)
            {
                // After recovery from outage, sleep some time to wait for entity to be unlocked, then confirm that we can delete the row.
                this.SleepUntilRowLockHasExpired(httpManglerStartTime);

                this.ExecuteDeleteRowAndValidate(entityPartitionKey, entityRowKey);
            }
            else
            {
                // After recovery from outage, confirm the entity is gone.
                this.ExecuteReadRowAndValidateNotExist(entityPartitionKey, entityRowKey);
            }
            this.ExecuteCreateRowAndValidate(entityPartitionKey, entityRowKey);
        }       

        // [Test(Description = "Tamper Batch operation at the Inner replica")]
        public void B00TamperBatchReplaceInnerTest()
        {
            int targetStorageAccount = 1;
            int skipInitialSessions = 0;
            bool targetApiExpectedToFail = true;
            bool checkOriginalEntityUnchanged = true;
            bool checkStorageAccountsConsistent = false;

            string jobType = "jobType-TamperBatchReplaceInnerTest";
            string jobId = "jobId-TamperBatchReplaceInnerTest-{0}";
            int count = 2; // number of operations in the batch _rtable_Operation
            List<TableOperationType> opTypes = new List<TableOperationType>()
            {
                TableOperationType.Replace,
                TableOperationType.Replace,
            };

            DateTime httpManglerStartTime;

            string partitionKey = this.SetupAndRunTemperBatchOperation(
                 count,
                 jobType,
                 jobId,
                 targetStorageAccount,
                 opTypes,
                 targetApiExpectedToFail,
                 checkOriginalEntityUnchanged,
                 checkStorageAccountsConsistent,
                 out httpManglerStartTime,
                 skipInitialSessions);

            // After recovery from short outage, sleep some time to wait for entity to be unlocked, confirm that we can update the row.
            this.SleepUntilRowLockHasExpired(httpManglerStartTime);

            Console.WriteLine("After httpMangler is turned off, calling ExecuteBatchOperationAndValidate()...");
            this.ExecuteBatchOperationAndValidate(
                count,
                partitionKey,
                jobType,
                jobId,
                opTypes);
        }

        // [Test(Description = "Tamper Batch _rtable_Operation Replace at the Tail replica")]
        public void C00TamperBatchReplaceTailTest()
        {
            int targetStorageAccount = this.actualStorageAccountsUsed.Count - 1;
            int skipInitialSessions = 0;
            bool targetApiExpectedToFail = true;            
            bool checkOriginalEntityUnchanged = true;            
            bool checkStorageAccountsConsistent = false;

            string jobType = "jobType-TamperBatchReplaceTailTest";
            string jobId = "jobId-TamperBatchReplaceTailTest";
            int count = 2; // number of operations in the batch _rtable_Operation
            List<TableOperationType> opTypes = new List<TableOperationType>()
            {
                TableOperationType.Replace,
                TableOperationType.Replace,
            };

            DateTime httpManglerStartTime;

            string partitionKey = this.SetupAndRunTemperBatchOperation(
                 count,
                 jobType,
                 jobId,
                 targetStorageAccount,
                 opTypes,
                 targetApiExpectedToFail,
                 checkOriginalEntityUnchanged,
                 checkStorageAccountsConsistent,
                 out httpManglerStartTime,
                 skipInitialSessions);

            // After recovery from short outage, sleep some time to wait for entity to be unlocked, confirm that we can update the row.
            this.SleepUntilRowLockHasExpired(httpManglerStartTime);

            Console.WriteLine("After httpMangler is turned off, calling ExecuteBatchOperationAndValidate()...");
            this.ExecuteBatchOperationAndValidate(
                count,
                partitionKey,
                jobType,
                jobId,
                opTypes);
        }


        // [Test(Description = "Tamper Batch operation at the Inner replica")]
        public void B00TamperBatchDeleteInnerTest()
        {
            int targetStorageAccount = 1;
            int skipInitialSessions = 0;
            bool targetApiExpectedToFail = true;
            bool checkOriginalEntityUnchanged = true;
            bool checkStorageAccountsConsistent = false;

            string jobType = "jobType-TamperBatchDeleteInnerTest";
            string jobId = "jobId-TamperBatchDeleteInnerTest-{0}";
            int count = 2; // number of operations in the batch _rtable_Operation
            List<TableOperationType> opTypes = new List<TableOperationType>()
            {
                TableOperationType.Delete,
                TableOperationType.Delete,
            };

            DateTime httpManglerStartTime;

            string partitionKey = this.SetupAndRunTemperBatchOperation(
                 count,
                 jobType,
                 jobId,
                 targetStorageAccount,
                 opTypes,
                 targetApiExpectedToFail,
                 checkOriginalEntityUnchanged,
                 checkStorageAccountsConsistent,
                 out httpManglerStartTime,
                 skipInitialSessions);

            // After recovery from short outage, sleep some time to wait for entity to be unlocked, confirm that we can update the row.
            this.SleepUntilRowLockHasExpired(httpManglerStartTime);

            Console.WriteLine("After httpMangler is turned off, calling ExecuteBatchOperationAndValidate()...");
            this.ExecuteBatchOperationAndValidate(
                count,
                partitionKey,
                jobType,
                jobId,
                opTypes);
        }


        // [Test(Description = "Tamper Batch operation at the Tail replica")]
        public void C00TamperBatchDeleteTailTest()
        {
            int targetStorageAccount = this.actualStorageAccountsUsed.Count - 1;
            int skipInitialSessions = 0;
            bool targetApiExpectedToFail = true;
            bool checkOriginalEntityUnchanged = true;
            bool checkStorageAccountsConsistent = false;

            string jobType = "jobType-TamperBatchDeleteTailTest";
            string jobId = "jobId-TamperBatchDeleteTailTest-{0}";
            int count = 2; // number of operations in the batch _rtable_Operation
            List<TableOperationType> opTypes = new List<TableOperationType>()
            {
                TableOperationType.Delete,
                TableOperationType.Delete,
            };

            DateTime httpManglerStartTime;

            string partitionKey = this.SetupAndRunTemperBatchOperation(
                 count,
                 jobType,
                 jobId,
                 targetStorageAccount,
                 opTypes,
                 targetApiExpectedToFail,
                 checkOriginalEntityUnchanged,
                 checkStorageAccountsConsistent,
                 out httpManglerStartTime,
                 skipInitialSessions);

            // After recovery from short outage, sleep some time to wait for entity to be unlocked, confirm that we can update the row.
            this.SleepUntilRowLockHasExpired(httpManglerStartTime);

            Console.WriteLine("After httpMangler is turned off, calling ExecuteBatchOperationAndValidate()...");
            this.ExecuteBatchOperationAndValidate(
                count,
                partitionKey,
                jobType,
                jobId,
                opTypes);
        }


        /// <summary>
        /// Tampering Head account should not affect ReadRow() because the RTable library will eventually go to the Tail to read.
        /// So, run SetupAndRunTamperTableBehavior() many times and confirm that it does not fail.
        /// </summary>
        [Test(Description = "Call ReadRow() API during short outage at the Head storage account.")]
        public void A00TamperReadRowHeadTest()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);

            string entityPartitionKey = "jobType-TamperReadRowHeadTest";
            string entityRowKey = "jobId-TamperReadRowHeadTest";

            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            int targetStorageAccount = 0;
            bool targetApiExpectedToFail = false;
            bool checkOriginalEntityUnchanged = true;
            bool checkStorageAccountsConsistent = true;

            // Try this many times. It should not fail.
            for (int i = 0; i < 3 * this.actualStorageAccountsUsed.Count; i++)
            {
                this.SetupAndRunTamperTableBehavior(
                    entityPartitionKey,
                    entityRowKey,
                    targetStorageAccount,
                    this.rtableWrapper.ReadEntity,
                    targetApiExpectedToFail,
                    checkOriginalEntityUnchanged,
                    checkStorageAccountsConsistent);
                Console.WriteLine("\n ----------------------------------- Finished iteration {0}", i);
            }
        }

        /// <summary>
        /// Tampering Inner account should not affect ReadRow() because the RTable library will eventually go to the Tail to read.
        /// So, run SetupAndRunTamperTableBehavior() many times and confirm that it does not fail.
        /// </summary>
        [Test(Description = "Call ReadRow() API during short outage at the Inner storage account.")]
        public void B00TamperReadRowInnerTest()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);

            string entityPartitionKey = "jobType-TamperReadRowInnerTest";
            string entityRowKey = "jobId-TamperReadRowInnerTest";

            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            int targetStorageAccount = 1;
            bool targetApiExpectedToFail = false;
            bool checkOriginalEntityUnchanged = true;
            bool checkStorageAccountsConsistent = true;

            // Try this many times. It should not fail.
            for (int i = 0; i < 3 * this.actualStorageAccountsUsed.Count; i++)
            {
                this.SetupAndRunTamperTableBehavior(
                    entityPartitionKey,
                    entityRowKey,
                    targetStorageAccount,
                    this.rtableWrapper.ReadEntity,
                    targetApiExpectedToFail,
                    checkOriginalEntityUnchanged,
                    checkStorageAccountsConsistent);
                Console.WriteLine("\n ----------------------------------- Finished iteration {0}", i);
            }
        }


        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        /// <summary>
        /// This test method shows the reference behavior of the ***Storage DLL*** when tampering is encountered.
        /// Edit HttpMangler.cs to use HandleFiddlerEvent_DEBUG().
        /// </summary>
        //[Test(Description = "XStore Tampering test.")]
        //public void A00XStoreTamperTest()
        //{
        //    string entityPartitionKey = "jobType-XStoreTamperTest";
        //    string entityRowKey = "jobId-XStoreTamperTest";

        //    this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

        //    int targetStorageAccount = 0;
        //    bool targetApiExpectedToFail = true;

        //    Assert.IsTrue(0 <= targetStorageAccount && targetStorageAccount < this.actualStorageAccountsUsed.Count,
        //            "targetStorageAccount={0} is out-of-range", targetStorageAccount);
        //    int index = this.actualStorageAccountsUsed[targetStorageAccount];
        //    string accountNameToTamper = this.rtableTestConfiguration.StorageInformation.AccountNames[index];
        //    Console.WriteLine("accountNameToTamper={0}", accountNameToTamper);

        //    // Tamper behavior
        //    ProxyBehavior[] behaviors = new[]
        //        {
        //            TamperBehaviors.TamperAllRequestsIf(
        //                session => ThreadPool.QueueUserWorkItem(state =>
        //                {
        //                    Thread.Sleep(10);
        //                    try
        //                    {
        //                        session.Abort();
        //                    }
        //                    catch (Exception ex)
        //                    {
        //                        Console.WriteLine("HttpMangler exception: {0}", ex.ToString());                              
        //                    }
        //                }),
        //                AzureStorageSelectors.TableTraffic().IfHostNameContains(accountNameToTamper))
        //        };

        //    this.SetupAndRunXStoreHttpManglerTest(
        //        entityPartitionKey,
        //        entityRowKey,
        //        targetStorageAccount,
        //        targetApiExpectedToFail,
        //        behaviors);
        //}

    }
}
