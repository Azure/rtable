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
    using System.Threading.Tasks;

    /// <summary>
    /// HttpMablger's Delay behavior is used in this set of tests. 
    /// </summary>
    [TestFixture]
    [Parallelizable(ParallelScope.None)]
    public class DelayTests : HttpManglerTestBase
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
        // runuts /run=Microsoft.WindowsAzure.Storage.RTableHttpManglerTest.DelayTests /verbose
        //

        //
        // NOTE:
        // Run the tests that deal with the first storage account first, then the second storage account, then the third storage account.
        // It fixes the HttpMangler issues for now. Still need to figure out the root cause.
        //

        /// <summary>
        /// Call ReplaceRow() when transmission to Head storage account suffers a short delay. Expected operation to pass.
        /// </summary>
        [Test(Description = "Call ReplaceRow() API during short delay at the Head storage account.")]
        public void A00DelayReplaceRowHeadTest()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);
            
            string entityPartitionKey = "jobType-DelayReplaceRowHeadTest";
            string entityRowKey = "jobId-DelayReplaceRowHeadTest";

            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            int targetStorageAccount = 0;
            TargetRTableWrapperApi<SampleRTableEntity> targetApi = this.rtableWrapper.ReplaceRow;
            bool targetApiExpectedToFail = false;
            int delayInMs = 3000;

            this.SetupAndRunDelayTableBehavior(
                entityPartitionKey, 
                entityRowKey,
                targetStorageAccount,
                delayInMs,
                targetApi, 
                targetApiExpectedToFail);

            // After recovery from delay, confirm that we can update the row.
            this.ExecuteReplaceRowAndValidate(entityPartitionKey, entityRowKey);
        }

        /// <summary>
        /// Call ReplaceRow() when transmission to Inner storage account suffers a short delay. Expected operation to pass.
        /// </summary>
        [Test(Description = "Call ReplaceRow() API during short delay at the Inner storage account.")]
        public void B00DelayReplaceRowInnerTest()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);
            
            string entityPartitionKey = "jobType-DelayReplaceRowInnerTest";
            string entityRowKey = "jobId-DelayReplaceRowInnerTest";

            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            int targetStorageAccount = 1;
            TargetRTableWrapperApi<SampleRTableEntity> targetApi = this.rtableWrapper.ReplaceRow;
            bool targetApiExpectedToFail = false;
            int delayInMs = 3000;

            this.SetupAndRunDelayTableBehavior(
                entityPartitionKey,
                entityRowKey,
                targetStorageAccount,
                delayInMs,
                targetApi,
                targetApiExpectedToFail);

            // After recovery from delay, confirm that we can update the row.
            this.ExecuteReplaceRowAndValidate(entityPartitionKey, entityRowKey);
        }

        /// <summary>
        /// Call ReplaceRow() when transmission to Tail storage account suffers a short delay. Expected operation to pass.
        /// </summary>
        [Test(Description = "Call ReplaceRow() API during short delay at the Tail storage account.")]
        public void C00DelayReplaceRowTailTest()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);
            
            string entityPartitionKey = "jobType-DelayReplaceRowTailTest";
            string entityRowKey = "jobId-DelayReplaceRowTailTest";

            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            int targetStorageAccount = this.actualStorageAccountsUsed.Count - 1;
            TargetRTableWrapperApi<SampleRTableEntity> targetApi = this.rtableWrapper.ReplaceRow;
            bool targetApiExpectedToFail = false;
            int delayInMs = 3000;

            this.SetupAndRunDelayTableBehavior(
                entityPartitionKey,
                entityRowKey,
                targetStorageAccount,
                delayInMs,
                targetApi,
                targetApiExpectedToFail);

            // After recovery from delay, confirm that we can update the row.
            this.ExecuteReplaceRowAndValidate(entityPartitionKey, entityRowKey);
        }

        /// <summary>
        /// Call DelayRow() when transmission to Tail storage account suffers a short delay. Expected operation to pass.
        /// </summary>
        [Test(Description = "Call DeleteRow() API during short delay at the Head storage account.")]
        public void A00DelayDeleteRowHeadTest()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);
            
            string entityPartitionKey = "jobType-DelayDeleteRowHeadTest";
            string entityRowKey = "jobId-DelayDeleteRowHeadTest";

            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            int targetStorageAccount = 0;
            TargetRTableWrapperApi<SampleRTableEntity> targetApi = this.rtableWrapper.DeleteRow;
            bool targetApiExpectedToFail = false;
            int delayInMs = 3000;

            this.SetupAndRunDelayTableBehavior(
                entityPartitionKey,
                entityRowKey,
                targetStorageAccount,
                delayInMs,
                targetApi,
                targetApiExpectedToFail);

            if (targetApiExpectedToFail)
            {
                // After recovery from delay, confirm that we can delete the row.
                this.ExecuteDeleteRowAndValidate(entityPartitionKey, entityRowKey);
            }
            else
            {
                // After recovery from delay, confirm the entity is gone.
                this.ExecuteReadRowAndValidateNotExist(entityPartitionKey, entityRowKey);
            }
            // Confirm we can create entity using the same partition and row keys again.
            this.ExecuteCreateRowAndValidate(entityPartitionKey, entityRowKey);
        }

        /// <summary>
        /// Call DelayRow() when transmission to Tail storage account suffers a short delay. Expected operation to pass.
        /// </summary>
        [Test(Description = "Call DeleteRow() API during short delay at the Inner storage account.")]
        public void B00DelayDeleteRowInnerTest()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);
            
            string entityPartitionKey = "jobType-DelayDeleteRowInnerTest";
            string entityRowKey = "jobId-DelayDeleteRowInnerTest";

            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            int targetStorageAccount = 1;
            TargetRTableWrapperApi<SampleRTableEntity> targetApi = this.rtableWrapper.DeleteRow;
            bool targetApiExpectedToFail = false;
            int delayInMs = 3000;

            this.SetupAndRunDelayTableBehavior(
                entityPartitionKey,
                entityRowKey,
                targetStorageAccount,
                delayInMs,
                targetApi,
                targetApiExpectedToFail);

            if (targetApiExpectedToFail)
            {
                // After recovery from delay, confirm that we can delete the row.
                this.ExecuteDeleteRowAndValidate(entityPartitionKey, entityRowKey);
            }
            else
            {
                // After recovery from delay, confirm the entity is gone.
                this.ExecuteReadRowAndValidateNotExist(entityPartitionKey, entityRowKey);
            }
            // Confirm we can create entity using the same partition and row keys again.
            this.ExecuteCreateRowAndValidate(entityPartitionKey, entityRowKey);
        }

        /// <summary>
        /// Call DelayRow() when transmission to Tail storage account suffers a short delay. Expected operation to pass.
        /// </summary>
        [Test(Description = "Call DeleteRow() API during short delay at the Tail storage account.")]
        public void C00DelayDeleteRowTailTest()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);
            
            string entityPartitionKey = "jobType-DelayDeleteRowTailTest";
            string entityRowKey = "jobId-DelayDeleteRowTailTest";


            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            int targetStorageAccount = this.actualStorageAccountsUsed.Count - 1;
            TargetRTableWrapperApi<SampleRTableEntity> targetApi = this.rtableWrapper.DeleteRow;
            bool targetApiExpectedToFail = false;
            int delayInMs = 3000;

            this.SetupAndRunDelayTableBehavior(
                entityPartitionKey,
                entityRowKey,
                targetStorageAccount,
                delayInMs,
                targetApi,
                targetApiExpectedToFail);

            if (targetApiExpectedToFail)
            {
                // After recovery from delay, confirm that we can delete the row.
                this.ExecuteDeleteRowAndValidate(entityPartitionKey, entityRowKey);
            }
            else
            {
                // After recovery from delay, confirm the entity is gone.
                this.ExecuteReadRowAndValidateNotExist(entityPartitionKey, entityRowKey);
            }
            // Confirm we can create entity using the same partition and row keys again.
            this.ExecuteCreateRowAndValidate(entityPartitionKey, entityRowKey);
        }

        /// <summary>
        /// Call InsertOrReplace() from 2 threads concurrently, at least one should succeed.
        /// </summary>
        [Test(Description = "Call InsertOrReplace() API from 2 threads concurrently, at least one should succeed.")]
        public void A00DelayTwoConflictingInsertOrReplaceCalls()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);

            string entityPartitionKey = "jobType-DelayInsertOrReplaceRowHeadTest";
            string entityRowKey = "jobId-DelayInsertOrReplaceRowHeadTest";

            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            string accountNameToTamper = this.rtableTestConfiguration.StorageInformation.AccountNames[0];
            Console.WriteLine("RunHttpManglerBehaviorHelper(): accountNameToTamper={0}", accountNameToTamper);

            int delayInMs = 3000;
            int insertRequestCount = 0;
            int conflictResponseCount = 0;
            bool secondUpsertConflicted = false;
            int failedCallIndex = -1;

            // Delay behavior
            ProxyBehavior[] behaviors = new[]
            {
                // Delay Insert calls so they end up conflicting
                TamperBehaviors.TamperAllRequestsIf(
                    (session =>
                    {
                        Interlocked.Increment(ref insertRequestCount);

                        while (insertRequestCount != 2)
                        {
                            Console.WriteLine("insertRequestCount={0}. Waiting on count to reach 2 ...", insertRequestCount);
                            Thread.Sleep(delayInMs);
                        }
                    }),
                    (session =>
                    {
                        if (session.hostname.Contains(accountNameToTamper + ".") &&
                            session.HTTPMethodIs("POST") &&
                            session.GetRequestBodyAsString().Contains("\"_rtable_Operation\":\"Insert\""))
                        {
                            return true;
                        }

                        return false;
                    })),

                // Delay conflict response
                DelayBehaviors.DelayAllResponsesIf(
                    delayInMs,
                    (session =>
                    {
                        if (session.hostname.Contains(accountNameToTamper + ".") &&
                            session.GetRequestBodyAsString().Contains("\"_rtable_Operation\":\"Insert\""))
                        {
                            if (session.responseCode == (int) HttpStatusCode.Conflict)
                            {
                                Interlocked.Increment(ref conflictResponseCount);

                                return true;
                            }
                        }

                        return false;
                    })),
            };

            using (new HttpMangler(false, behaviors))
            {
                Parallel.For(0, 2, (index) =>
                {
                    var entry = new SampleRTableEntity(entityPartitionKey, entityRowKey, string.Format("upsert message {0}", index));
                    try
                    {
                        this.rtableWrapper.InsertOrReplaceRow(entry);
                    }
                    catch (RTableConflictException)
                    {
                        if (secondUpsertConflicted)
                        {
                            // should never reach here
                            throw;
                        }

                        // That's possible, but that's the Replace step of upsert which conflicted with ongoing write
                        // can't do anything, client should retry on conflict
                        secondUpsertConflicted = true;
                    }
                });
            }

            // got 2 inserts?
            Assert.AreEqual(2, insertRequestCount, "Two insert calls expected!");

            // got one conflict?
            Assert.AreEqual(1, conflictResponseCount, "One conflict response expected!");

            // at least one upsert would have succeeded
            SampleRTableEntity upsertedEntity = this.rtableWrapper.ReadEntity(entityPartitionKey, entityRowKey);
            Assert.NotNull(upsertedEntity, "at least one upsert should have succeeded");

            // second upsert failed?
            if (secondUpsertConflicted)
            {
                Assert.AreEqual(1, upsertedEntity._rtable_Version, "one upsert succeeded so version should be = 1");
                Assert.AreEqual(upsertedEntity.Message, string.Format("upsert message {0}", (1 - failedCallIndex)));
            }
            else
            {
                Assert.AreEqual(2, upsertedEntity._rtable_Version, "both upserts succeeded so version should be = 2");
            }


            // After recovery from delay, confirm that we can update the row.
            //this.ExecuteReplaceRowAndValidate(entityPartitionKey, entityRowKey);
        }

        /// <summary>
        /// Execute an Update entity in a Replicated mode, which the chain is not stable.
        /// Before commit to the head is executed, a RepairRow happens.
        /// The RepairRow is a NOP since the entry has the right ViewId.
        /// After that, the commit to the head is executed.
        /// </summary>
        [Test(Description = "Issue a RepairRow in middle of an Update i.e. before commit to the head is done")]
        public void RepairMidAnUpdate()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);

            // Not-Stable chain
            // Reconfigure RTable so Head is WriteOnly.
            View view = this.configurationService.GetTableView(this.repTable.TableName);

            ReplicatedTableConfiguration config;
            ReplicatedTableQuorumReadResult readStatus = this.configurationService.RetrieveConfiguration(out config);
            Assert.IsTrue(readStatus.Code == ReplicatedTableQuorumReadCode.Success);

            // Set Head as WriteOnly mode
            ReplicatedTableConfigurationStore viewConfg = config.GetView(view.Name);
            viewConfg.ReplicaChain[0].Status = ReplicaStatus.WriteOnly;
            config.SetView(view.Name, viewConfg);

            // Upload RTable config back
            this.configurationService.UpdateConfiguration(config);

            // Sanity: Replicated mode and chain Not-Stable
            view = this.configurationService.GetTableView(this.repTable.TableName);
            Assert.IsTrue(view != null && view.Chain.Count > 1, "Two replicas should be used.");
            Assert.IsFalse(view.IsStable);


            // Insert one entry
            Console.WriteLine("Inserting entry ...");
            string entityPartitionKey = "jobType-RepairMidAnUpdate-Replace";
            string entityRowKey = "jobId-RepairMidAnUpdate-Replace";
            var entry = new SampleRTableEntity(entityPartitionKey, entityRowKey, "message");
            this.rtableWrapper.InsertRow(entry);


            // 1 - Launch a RepairRow task in wait mode ...
            bool triggerRepair = false;
            bool repaireDone = false;
            bool headAfterRepairWasLocked = false;

            TableResult repairResult = null;
            Task.Run(() =>
            {
                ReplicatedTable repairTable = new ReplicatedTable(this.repTable.TableName, this.configurationService);

                while (!triggerRepair)
                {
                    Thread.Sleep(5);
                }

                Console.WriteLine("RepairRow started ...");
                repairResult = repairTable.RepairRow(entry.PartitionKey, entry.RowKey, null);
                Console.WriteLine("RepairRow completed with HttpStatus={0}", repairResult == null ? "NULL" : repairResult.HttpStatusCode.ToString());

                // Check the entry at the Head is still locked i.e. RepairRow was NOP
                headAfterRepairWasLocked = HeadIsLocked(entry);

                Console.WriteLine("Signal the commit to Head job");
                repaireDone = true;
            });


            // 2 - Configure Mangler ...
            string accountNameToTamper = this.rtableTestConfiguration.StorageInformation.AccountNames[0];
            Console.WriteLine("RunHttpManglerBehaviorHelper(): accountNameToTamper={0}", accountNameToTamper);

            ProxyBehavior[] behaviors = new[]
            {
                TamperBehaviors.TamperAllRequestsIf(
                    (session =>
                    {
                        Console.WriteLine("Delaying commit to the Head ... => signal RepairRow job");

                        // Let RepairRow task go through
                        triggerRepair = true;

                        int iter = 0;
                        while (!repaireDone)
                        {
                            // TODO: break the loop after couple of iteration ...

                            Thread.Sleep(100);
                            Console.WriteLine("Waiting on RepairRow to finish ({0}) ...", ++iter);
                        }

                        Console.WriteLine("Request a commit to the head");
                    }),
                    (session =>
                    {
                        // Commit on head i.e. a PUT with RowLock == false
                        if (session.hostname.Contains(accountNameToTamper + ".") &&
                            session.HTTPMethodIs("PUT") &&
                            session.GetRequestBodyAsString().Contains("\"_rtable_RowLock\":false"))
                        {
                            return true;
                        }

                        return false;
                    }))
            };

            using (new HttpMangler(false, behaviors))
            {
                Console.WriteLine("Updating entry ...");
                entry = this.rtableWrapper.FindRow(entry.PartitionKey, entry.RowKey);
                entry.Message = "updated message";

                this.rtableWrapper.ReplaceRow(entry);
            }

            Assert.IsTrue(triggerRepair);
            Assert.IsTrue(repairResult != null && repairResult.HttpStatusCode == (int)HttpStatusCode.OK, "Repair failed.");
            Assert.IsTrue(repaireDone);
            Assert.IsTrue(headAfterRepairWasLocked);

            Console.WriteLine("DONE. Test passed.");
        }

        private bool HeadIsLocked(SampleRTableEntity entry)
        {
            CloudTable table = this.cloudTableClients[0].GetTableReference(this.repTable.TableName);

            TableOperation retrieveOperation = TableOperation.Retrieve<SampleRTableEntity>(entry.PartitionKey, entry.RowKey);
            TableResult retrieveResult = table.Execute(retrieveOperation);

            if (retrieveResult == null ||
                retrieveResult.HttpStatusCode != (int)HttpStatusCode.OK ||
                retrieveResult.Result == null)
            {
                return false;
            }

            SampleRTableEntity head = (SampleRTableEntity)retrieveResult.Result;
            return head._rtable_RowLock == true;
        }
    }
}
