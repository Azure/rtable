//////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) Microsoft Corporation. All rights reserved.
//
//////////////////////////////////////////////////////////////////////////////

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
    /// </summary>
    [TestFixture]
    public class ThrottlingTests : HttpManglerTestBase
    {
        [TestFixtureSetUp]
        public void TestFixtureSetup()
        {                    
            this.TestFixtureSetupInternal();
        }

        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            base.DeleteAllRtableResources();
        }

        //
        // binaries.amd64fre\Gateway\UnitTests>
        // runuts /run=Microsoft.WindowsAzure.Storage.RTableHttpManglerTest.ThrottlingTests /verbose
        //

        //
        // NOTE:
        // Run the tests that deal with the first storage account first, then the second storage account, then the third storage account.
        // It fixes the HttpMangler issues for now. Still need to figure out the root cause.
        //
        
        /// <summary>
        /// Edit HttpMangler.cs to use HandleFiddlerEvent_DEBUG().
        /// => Timestamp of the transmissions were:
        /// 11:03:45.701
        /// 11:03:46.184
        /// 11:03:49.304
        /// 11:03:56.892
        /// 11:04:12.985
        /// => Delays were: 0.4, 3.2, 7.6, 16.1 seconds (i.e., exponential backoff)
        /// </summary>
        [Test(Description = "Call ReplaceRow() API during throttling at the Head storage account.")]
        public void A00ThrottleReplaceRowHeadTest()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);
            
            string entityPartitionKey = "jobType-ThrottleReplaceRowHeadTest";
            string entityRowKey = "jobId-ThrottleReplaceRowHeadTest";

            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            int targetStorageAccount = 0;
            TargetRTableWrapperApi<SampleRTableEntity> targetApi = this.rtableWrapper.ReplaceRow;
            bool targetApiExpectedToFail = true;
            bool checkOriginalEntityUnchanged = true;
            bool checkStorageAccountsConsistent = true;

            this.SetupAndRunThrottleTableBehavior(
                entityPartitionKey,
                entityRowKey,
                targetStorageAccount,
                targetApi,
                targetApiExpectedToFail,
                checkOriginalEntityUnchanged,
                checkStorageAccountsConsistent);

            // After recovery from throttling, confirm that we can update the row.
            this.ExecuteReplaceRowAndValidate(entityPartitionKey, entityRowKey);
        }

        [Test(Description = "Call ReplaceRow() API during throttling at the Head storage account. Skip initial sessions to Head storage account")]
        public void A00ThrottleSkipReplaceRowHeadTest()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);

            string entityPartitionKey = "jobType-ThrottleSkipReplaceRowHeadTest";
            string entityRowKey = "jobId-ThrottleSkipReplaceRowHeadTest";

            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            int targetStorageAccount = 0;
            TargetRTableWrapperApi<SampleRTableEntity> targetApi = this.rtableWrapper.ReplaceRow;
            bool targetApiExpectedToFail = true;
            bool checkOriginalEntityUnchanged = false;
            bool checkStorageAccountsConsistent = false;
            int skipInitialSessions = 2;

            DateTime httpManglerStartTime;
            this.SetupAndRunThrottleTableBehavior(
                entityPartitionKey,
                entityRowKey,
                targetStorageAccount,
                targetApi,
                targetApiExpectedToFail,
                checkOriginalEntityUnchanged,
                checkStorageAccountsConsistent,
                out httpManglerStartTime,
                skipInitialSessions);

            // After recovery from throttling, sleep some time to wait for entity to be unlocked, then confirm that we can update the row.
            this.SleepUntilRowLockHasExpired(httpManglerStartTime);

            this.ExecuteReplaceRowAndValidate(entityPartitionKey, entityRowKey);
        }


        [Test(Description = "Call ReplaceRow() API during throttling at the Inner storage account.")]
        public void B00ThrottleReplaceRowInnerTest()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);
            
            string entityPartitionKey = "jobType-ThrottleReplaceRowInnerTest";
            string entityRowKey = "jobId-ThrottleReplaceRowInnerTest";

            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            int targetStorageAccount = 1;
            TargetRTableWrapperApi<SampleRTableEntity> targetApi = this.rtableWrapper.ReplaceRow;
            bool targetApiExpectedToFail = true;
            bool checkOriginalEntityUnchanged = true;
            bool checkStorageAccountsConsistent = false;

            DateTime httpManglerStartTime;
            this.SetupAndRunThrottleTableBehavior(
                entityPartitionKey,
                entityRowKey,
                targetStorageAccount,
                targetApi,
                targetApiExpectedToFail,
                checkOriginalEntityUnchanged,
                checkStorageAccountsConsistent,
                out httpManglerStartTime);

            // After recovery from throttling, sleep some time to wait for entity to be unlocked, thenconfirm that we can update the row.
            this.SleepUntilRowLockHasExpired(httpManglerStartTime);

            this.ExecuteReplaceRowAndValidate(entityPartitionKey, entityRowKey);
        }


        [Test(Description = "Call ReplaceRow() API during throttling at the Inner storage account. Skip initial sessions")]
        public void B00ThrottleSkipReplaceRowInnerTest()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);

            string entityPartitionKey = "jobType-ThrottleSkipReplaceRowInnerTest";
            string entityRowKey = "jobId-ThrottleSkipReplaceRowInnerTest";

            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            int targetStorageAccount = 1;
            TargetRTableWrapperApi<SampleRTableEntity> targetApi = this.rtableWrapper.ReplaceRow;
            bool targetApiExpectedToFail = true;
            bool checkOriginalEntityUnchanged = false;
            bool checkStorageAccountsConsistent = false;
            int skipInitialSessions = 2;

            DateTime httpManglerStartTime;
            this.SetupAndRunThrottleTableBehavior(
                entityPartitionKey,
                entityRowKey,
                targetStorageAccount,
                targetApi,
                targetApiExpectedToFail,
                checkOriginalEntityUnchanged,
                checkStorageAccountsConsistent,
                out httpManglerStartTime,
                skipInitialSessions);

            // After recovery from throttling, sleep some time to wait for entity to be unlocked, then confirm that we can update the row.
            this.SleepUntilRowLockHasExpired(httpManglerStartTime);

            this.ExecuteReplaceRowAndValidate(entityPartitionKey, entityRowKey);
        }


        [Test(Description = "Call ReplaceRow() API during throttling at the Tail storage account.")]
        public void C00ThrottleReplaceRowTailTest()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);
            
            string entityPartitionKey = "jobType-ThrottleReplaceRowTailTest";
            string entityRowKey = "jobId-ThrottleReplaceRowTailTest";

            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            int targetStorageAccount = this.actualStorageAccountsUsed.Count - 1;
            TargetRTableWrapperApi<SampleRTableEntity> targetApi = this.rtableWrapper.ReplaceRow;
            bool targetApiExpectedToFail = true;
            bool checkOriginalEntityUnchanged = true;
            bool checkStorageAccountsConsistent = false;

            DateTime httpManglerStartTime;
            this.SetupAndRunThrottleTableBehavior(
                entityPartitionKey,
                entityRowKey,
                targetStorageAccount,
                targetApi,
                targetApiExpectedToFail,
                checkOriginalEntityUnchanged,
                checkStorageAccountsConsistent,
                out httpManglerStartTime);

            // After recovery from throttling, sleep some time to wait for entity to be unlocked, then confirm that we can update the row.
            this.SleepUntilRowLockHasExpired(httpManglerStartTime);

            this.ExecuteReplaceRowAndValidate(entityPartitionKey, entityRowKey);
        }


        [Test(Description = "Call DeleteRow() API during throttling at the Head storage account.")]
        public void A00ThrottleDeleteRowHeadTest()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);
            
            string entityPartitionKey = "jobType-ThrottleDeleteRowHeadTest";
            string entityRowKey = "jobId-ThrottleDeleteRowHeadTest";

            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            int targetStorageAccount = 0;
            TargetRTableWrapperApi<SampleRTableEntity> targetApi = this.rtableWrapper.DeleteRow;
            bool targetApiExpectedToFail = true;
            bool checkOriginalEntityUnchanged = true;
            bool checkStorageAccountsConsistent = true;

            this.SetupAndRunThrottleTableBehavior(
                entityPartitionKey,
                entityRowKey,
                targetStorageAccount,
                targetApi,
                targetApiExpectedToFail,
                checkOriginalEntityUnchanged,
                checkStorageAccountsConsistent);

            if (targetApiExpectedToFail)
            {
                // After recovery from throttling, confirm that we can delete the row.
                this.ExecuteDeleteRowAndValidate(entityPartitionKey, entityRowKey);
            }
            else
            {
                // After recovery from throttling, confirm the entity is gone
                this.ExecuteReadRowAndValidateNotExist(entityPartitionKey, entityRowKey);
            }
            // Confirm we can create entity using the same partition and row keys again.
            this.ExecuteCreateRowAndValidate(entityPartitionKey, entityRowKey);
        }

        [Test(Description = "Call DeleteRow() API during throttling at the Head storage account. Skip initial sessions.")]
        public void A00ThrottleSkipDeleteRowHeadTest()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);

            string entityPartitionKey = "jobType-ThrottleSkipDeleteRowHeadTest";
            string entityRowKey = "jobId-ThrottleSkipDeleteRowHeadTest";

            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            int targetStorageAccount = 0;
            TargetRTableWrapperApi<SampleRTableEntity> targetApi = this.rtableWrapper.DeleteRow;
            bool targetApiExpectedToFail = true;
            bool checkOriginalEntityUnchanged = false;
            bool checkStorageAccountsConsistent = false;
            int skipInitialiSessions = 2;

            DateTime httpManglerStartTime;
            this.SetupAndRunThrottleTableBehavior(
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
                // After recovery from throttling, sleep some time to wait for entity to be unlocked, then confirm that we can delete the row.
                this.SleepUntilRowLockHasExpired(httpManglerStartTime);

                this.ExecuteDeleteRowAndValidate(entityPartitionKey, entityRowKey);
            }
            else
            {
                // After recovery from throttling, confirm the entity is gone
                this.ExecuteReadRowAndValidateNotExist(entityPartitionKey, entityRowKey);
            }
            // Confirm we can create entity using the same partition and row keys again.
            this.ExecuteCreateRowAndValidate(entityPartitionKey, entityRowKey);
        }


        [Test(Description = "Call DeleteRow() API during throttling at the Inner storage account.")]
        public void B00ThrottleDeleteRowInnerTest()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);
            
            string entityPartitionKey = "jobType-ThrottleDeleteRowInnerTest";
            string entityRowKey = "jobId-ThrottleDeleteRowInnerTest";

            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            int targetStorageAccount = 1;
            TargetRTableWrapperApi<SampleRTableEntity> targetApi = this.rtableWrapper.DeleteRow;
            bool targetApiExpectedToFail = true;
            bool checkOriginalEntityUnchanged = true;
            bool checkStorageAccountsConsistent = false;

            DateTime httpManglerStartTime;
            this.SetupAndRunThrottleTableBehavior(
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
                // After recovery from throttling, sleep some time to wait for entity to be unlocked, confirm that we can delete the row.
                this.SleepUntilRowLockHasExpired(httpManglerStartTime);

                this.ExecuteDeleteRowAndValidate(entityPartitionKey, entityRowKey);
            }
            else
            {
                // After recovery from throttling, confirm the entity is gone
                this.ExecuteReadRowAndValidateNotExist(entityPartitionKey, entityRowKey);
            }
            // Confirm we can create entity using the same partition and row keys again.
            this.ExecuteCreateRowAndValidate(entityPartitionKey, entityRowKey);
        }

        [Test(Description = "Call DeleteRow() API during throttling at the Inner storage account. Skip initial sessions")]
        public void B00ThrottleSkipDeleteRowInnerTest()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);

            string entityPartitionKey = "jobType-ThrottleSkipDeleteRowInnerTest";
            string entityRowKey = "jobId-ThrottleSkipDeleteRowInnerTest";

            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            int targetStorageAccount = 1;
            TargetRTableWrapperApi<SampleRTableEntity> targetApi = this.rtableWrapper.DeleteRow;
            bool targetApiExpectedToFail = true;
            bool checkOriginalEntityUnchanged = false;
            bool checkStorageAccountsConsistent = false;
            int skipInitialiSessions = 2;

            DateTime httpManglerStartTime;
            this.SetupAndRunThrottleTableBehavior(
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
                // After recovery from throttling, sleep some time to wait for entity to be unlocked, then confirm that we can delete the row.
                this.SleepUntilRowLockHasExpired(httpManglerStartTime);

                this.ExecuteDeleteRowAndValidate(entityPartitionKey, entityRowKey);
            }
            else
            {
                // After recovery from throttling, confirm the entity is gone
                this.ExecuteReadRowAndValidateNotExist(entityPartitionKey, entityRowKey);
            }
            // Confirm we can create entity using the same partition and row keys again.
            this.ExecuteCreateRowAndValidate(entityPartitionKey, entityRowKey);
        }

        [Test(Description = "Call DeleteRow() API during throttling at the Tail storage account.")]
        public void C00ThrottleDeleteRowTailTest()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);
            
            string entityPartitionKey = "jobType-ThrottleDeleteRowTailTest";
            string entityRowKey = "jobId-ThrottleDeleteRowTailTest";

            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            int targetStorageAccount = this.actualStorageAccountsUsed.Count - 1;
            TargetRTableWrapperApi<SampleRTableEntity> targetApi = this.rtableWrapper.DeleteRow;
            bool targetApiExpectedToFail = true;
            bool checkOriginalEntityUnchanged = true;
            bool checkStorageAccountsConsistent = false;

            DateTime httpManglerStartTime;
            this.SetupAndRunThrottleTableBehavior(
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
                // After recovery from throttling, sleep some time to wait for entity to be unlocked, then confirm that we can delete the row.
                this.SleepUntilRowLockHasExpired(httpManglerStartTime);

                this.ExecuteDeleteRowAndValidate(entityPartitionKey, entityRowKey);
            }
            else
            {
                // After recovery from throttling, confirm the entity is gone
                this.ExecuteReadRowAndValidateNotExist(entityPartitionKey, entityRowKey);                
            }
            // Confirm we can create entity using the same partition and row keys again.
            this.ExecuteCreateRowAndValidate(entityPartitionKey, entityRowKey);
        }




        /// <summary>
        /// This test method shows the reference behavior of the ***Storage DLL*** when throttling is encountered.
        /// Edit HttpMangler.cs to use HandleFiddlerEvent_DEBUG().
        /// There were four transmissions whose timestamps were:
        /// 10:43:19.623    (first attempt)
        /// 10:43:22.746    (first retry after throttled)
        /// 10:43:29.455    (second retry)
        /// 10:43:46.349    (final retry)
        /// So, the delays were:  3.1, 6.7, 16.9 seconds
        /// </summary>
        [Test(Description = "XStore Throttling test.")]
        public void A00XStoreThrottleTest()
        {
            string entityPartitionKey = "jobType-XStoreThrottleTest";
            string entityRowKey = "jobId-XStoreThrottleTest";

            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            int targetStorageAccount = 0;
            bool targetApiExpectedToFail = true;

            Assert.IsTrue(0 <= targetStorageAccount && targetStorageAccount < this.actualStorageAccountsUsed.Count,
                    "targetStorageAccount={0} is out-of-range", targetStorageAccount);
            int index = this.actualStorageAccountsUsed[targetStorageAccount];
            string accountNameToTamper = this.rtableTestConfiguration.StorageInformation.AccountNames[index];
            Console.WriteLine("accountNameToTamper={0}", accountNameToTamper);

            // Throttle behavior
            ProxyBehavior[] behaviors = new[]
                {
                    TamperBehaviors.TamperAllRequestsIf(
                            Actions.ThrottleTableRequest,
                            AzureStorageSelectors.TableTraffic().IfHostNameContains(accountNameToTamper))         
                };

            this.SetupAndRunXStoreHttpManglerTest(
                entityPartitionKey,
                entityRowKey,
                targetStorageAccount,
                targetApiExpectedToFail,
                behaviors);
        }

    }
}
