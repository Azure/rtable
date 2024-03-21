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
    using NUnit.Framework;
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Threading;
    using Microsoft.WindowsAzure.Test.Network;
    using Microsoft.WindowsAzure.Test.Network.Behaviors;
    using System.Threading.Tasks;

    [TestFixture]
    [Parallelizable(ParallelScope.None)]
    public class RTableInsertHeadTests : HttpManglerTestBase
    {
        [SetUp]
        public void TestFixtureSetup()
        {
            // Use 2 Replicas for simplicity
            this.OneTimeSetUpInternal(new List<int> { 0, 1 });
        }

        [TearDown]
        public void TestFixtureTearDown()
        {
            // force reload the new view so both replicas Head/Tail get cleaned
            this.configurationService.ConfigurationChangeNotification();

            base.DeleteAllRtableResources();
        }

        /// <summary>
        /// Setup:
        /// 1 - Upload config w/o Head => [None]->[RW]->...->[RW]
        /// 2 - Initialize Thread_1 with previous config which never expires (6000 sec.)
        /// 3 - Upload config with Head => [WO]->[RW]->...->[RW]
        /// 4 - Initialize Thread_2 with new config which never expires (6000 sec.)
        /// </summary>
        void SetupStaleViewAndNewView(out ReplicatedTableConfigurationServiceV2 configServiceOne, out ReplicatedTableConfigurationServiceV2 configServiceTwo)
        {
            // Verify current View has 2 replicas ?
            View view = this.configurationWrapper.GetWriteView();
            Assert.IsTrue(view.Chain.Count == 2, "expects 2 replicas only!");

            int leaseDurationInSec = 6000;

            /*
             * 1 - Remove the Head, set a high LeaseDuration.
             *     The new config will be [None]->[RW]->...->[RW]
             */
            configServiceOne = new ReplicatedTableConfigurationServiceV2(this.configurationInfos, this.connectionStringMap, useHttps);
            RemoveHeadFromView(view.Name, leaseDurationInSec, configServiceOne);

            /*
             * 2 - Insert the Head, set a high LeaseDuration.
             *     The new config will be [WO]->[RW]->...->[RW]
             */
            configServiceTwo = new ReplicatedTableConfigurationServiceV2(this.configurationInfos, this.connectionStringMap, useHttps);
            InsertHeadInView(view.Name, leaseDurationInSec, configServiceTwo);

            /*
             * At this stage:
             *  - configServiceOne sees a stale View (LeaseDuration = 6000sec.)
             *  - configServiceTwo sees the latest View (LeaseDuration = 6000sec.)
             */
            long staleViewId = configServiceOne.GetTableView(this.repTable.TableName).ViewId;
            long latestViewId = configServiceTwo.GetTableView(this.repTable.TableName).ViewId;
            Assert.AreNotEqual(staleViewId, latestViewId, "View should have changed !!!");
        }

        /// <summary>
        /// Test:
        /// 1 - Thread_1: inserts a new entry (using stale viewId)
        /// 2 - Thread_2: updates the previous entry (using new viewId)
        /// 3 - Thread_1: fails to retrieve the entry (using stale viewId)
        /// 4 - Notify of config change => Thread_1 sees latest View
        /// 5 - Thread_1: retrieves the entry (using latest viewId)
        /// </summary>
        [Test(Description = "Trigger configuration change notification after detecting a stale view")]
        public void ConfigurationChangeNotificationAfterDetectingStaleView()
        {
            ReplicatedTableConfigurationServiceV2 configServiceOne, configServiceTwo;

            // setup:
            //  stale view = [None] ->  [RW]
            //  new view   = [WO]   ->  [RW]
            SetupStaleViewAndNewView(out configServiceOne, out configServiceTwo);

            long staleViewId = configServiceOne.GetTableView(this.repTable.TableName).ViewId;
            long latestViewId = configServiceTwo.GetTableView(this.repTable.TableName).ViewId;

            string firstName = "FirstName01";
            string lastName = "LastName01";

            /*
             * 1 - WorkerOne => inserts an entry using the stale View
             */
            var workerOne = new ReplicatedTable(this.repTable.TableName, configServiceOne);

            var customer = new CustomerEntity(firstName, lastName);
            customer.Email = "workerOne@dns.com";
            InsertCustormer(customer, workerOne);

            customer = RetrieveCustomer(firstName, lastName, workerOne);

            Assert.AreEqual(staleViewId, customer._rtable_ViewId, "customer._rtable_ViewId mismatch");
            Assert.AreEqual("workerOne@dns.com", customer.Email, "customer.Email mismatch");
            Console.WriteLine("workerOne inserted a customer in ViewId={0}", staleViewId);


            /*
             * 2 - WorkerTwo => updates the entry using latest View
             */
            var workerTwo = new ReplicatedTable(this.repTable.TableName, configServiceTwo);

            customer = RetrieveCustomer(firstName, lastName, workerTwo);
            customer.Email = "workerTwo@dns.com";
            UpdateCustomer(customer, workerTwo);

            customer = RetrieveCustomer(firstName, lastName, workerTwo);

            Assert.AreEqual(latestViewId, customer._rtable_ViewId, "customer._rtable_ViewId mismatch");
            Assert.AreEqual("workerTwo@dns.com", customer.Email, "customer.Email mismatch");
            Console.WriteLine("workerTwo updated the customer in ViewId={0}", latestViewId);


            /*
             * 3 - WorkerOne => access existing entry using "the stale View"
             */
            try
            {
                customer = RetrieveCustomer(firstName, lastName, workerOne);
                Assert.Fail("Retrieve() is expected to get an RTableStaleViewException but did not get it.");
            }
            catch (ReplicatedTableStaleViewException ex)
            {
                Console.WriteLine("workerOne got exception: {0}", ex.Message);
                Assert.IsTrue(ex.ErrorCode == ReplicatedTableViewErrorCodes.ViewIdSmallerThanEntryViewId);
                Assert.IsTrue(ex.Message.Contains(string.Format("current _rtable_ViewId {0} is smaller than _rtable_ViewId of existing row {1}", staleViewId, latestViewId)), "Got unexpected exception message");
            }

            // Note:
            //  No need to test all table APIs since already covered by UT:
            //  => "Microsoft.Azure.Toolkit.Replication.Test.RTableViewIdTests.ExceptionWhenUsingSmallerViewId()"


            /*
             * 4 - Notify "configServiceOne" instance of config change
             */
            Assert.AreEqual(configServiceOne.GetTableView(this.repTable.TableName).ViewId, staleViewId, "we should see old View!!!");
            {
                configServiceOne.ConfigurationChangeNotification();
            }
            Assert.AreEqual(configServiceOne.GetTableView(this.repTable.TableName).ViewId, latestViewId, "we should see latest View!!!");


            /*
             * 5 - WorkerOne => updates the entry using latest View
             */
            customer = RetrieveCustomer(firstName, lastName, workerTwo);
            customer.Email = "happy.workerOne@dns.com";
            UpdateCustomer(customer, workerTwo);

            customer = RetrieveCustomer(firstName, lastName, workerTwo);

            Assert.AreEqual(latestViewId, customer._rtable_ViewId, "customer._rtable_ViewId mismatch");
            Assert.AreEqual("happy.workerOne@dns.com", customer.Email, "customer.Email mismatch");
            Console.WriteLine("workerOne updated the customer in ViewId={0}", latestViewId);
        }

        /// <summary>
        /// 1 - Thread_1: locks the row in Tail (using stale ViewId)
        /// 2 - Thread_2: starts an update (using new ViewId)
        ///               - row is locked so repair row will fail with a conflict
        ///                      => fails with Precondition as the row is still "Locked"
        /// 3 - Thread_1: commits to Tail (using stale viewId)
        ///                      => Succeed
        /// </summary>
        [Test(Description = "Concurrent updates: first uses a stale View and the second uses a newView. " +
                            "Second update interleaves Lock/Commit phases of first update")]
        public void UpdateInStaleViewConflictingWithUpdateInNewView_T01()
        {
            ReplicatedTableConfigurationServiceV2 configServiceOne, configServiceTwo;

            // setup:        Acc0       Acc1
            //  stale view = [None] ->  [RW]
            //  new view   = [WO]   ->  [RW]
            SetupStaleViewAndNewView(out configServiceOne, out configServiceTwo);

            long staleViewId = configServiceOne.GetTableView(this.repTable.TableName).ViewId;
            long latestViewId = configServiceTwo.GetTableView(this.repTable.TableName).ViewId;

            string firstName = "FirstName01";
            string lastName = "LastName01";

            /*
             * 1 - WorkerOne => inserts an entry in stale View
             */
            var workerOne = new ReplicatedTable(this.repTable.TableName, configServiceOne);
            var workerTwo = new ReplicatedTable(this.repTable.TableName, configServiceTwo);

            var customer = new CustomerEntity(firstName, lastName);
            customer.Email = "***";
            InsertCustormer(customer, workerOne);

            string accountNameToTamper = this.rtableTestConfiguration.StorageInformation.AccountNames[1];
            Console.WriteLine("RunHttpManglerBehaviorHelper(): accountNameToTamper={0}", accountNameToTamper);

            TableResult oldUpdateResult = null;
            TableResult newUpdateResult = null;
            bool triggerUpdateWithNewView = false;
            bool oldUpdateResume = false;

            // Start new Update in wait
            var newUpdateTask = Task.Run(() =>
            {
                while (!triggerUpdateWithNewView)
                {
                    Thread.Sleep(100);
                }

                /*
                 * 3 - Executes after step 2 below:
                 *     WorkerTwo => updates the entry using new View
                 */
                customer = RetrieveCustomer(firstName, lastName, workerTwo);
                customer.Email = "workerTwo";

                newUpdateResult = workerTwo.Replace(customer);

                oldUpdateResume = true;
            });


            // Delay behavior
            ProxyBehavior[] behaviors =
            {
                TamperBehaviors.TamperAllRequestsIf(
                    (session =>
                    {
                        // => trigger new update to start
                        triggerUpdateWithNewView = true;

                        // Delaying commit to the Tail by stale view Update
                        while (!oldUpdateResume)
                        {
                            Thread.Sleep(100);
                        }
                    }),
                    (session =>
                    {
                        var body = session.GetRequestBodyAsString();

                        // Commit to Tail by stale view
                        if (session.hostname.Contains(accountNameToTamper + ".") &&
                            session.HTTPMethodIs("PUT") &&
                            body.Contains("\"Email\":\"workerOne\"") &&
                            body.Contains(string.Format("\"_rtable_ViewId\":\"{0}\"", staleViewId)) &&
                            body.Contains("\"_rtable_RowLock\":false"))
                        {
                            return true;
                        }

                        return false;
                    })),
            };

            /*
             * 2 - WorkerOne => update an entry using the stale View
             */
            using (new HttpMangler(false, behaviors))
            {
                customer = RetrieveCustomer(firstName, lastName, workerOne);
                customer.Email = "workerOne";

                oldUpdateResult = workerOne.Replace(customer);
            }

            // Wait on new Update to finish
            newUpdateTask.Wait();


            // Expected behavior:

            // Thread_1 (stale ViewId)
            Assert.IsNotNull(oldUpdateResult, "oldUpdateResult = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, oldUpdateResult.HttpStatusCode, "oldUpdateResult.HttpStatusCode mismatch");
            Console.WriteLine("Update in stale View Succeeded with HttpStatus={0}", oldUpdateResult.HttpStatusCode);

            // Thread_2 (new ViewId)
            Assert.IsNotNull(newUpdateResult, "newUpdateResult = null");
            Console.WriteLine("Update in new View failed with HttpStatus={0}", newUpdateResult.HttpStatusCode);

            // Thread_2 got "PreconditionFailed"
            Assert.AreEqual((int)HttpStatusCode.PreconditionFailed, newUpdateResult.HttpStatusCode, "newUpdateResult.HttpStatusCode mismatch");

            customer = RetrieveCustomer(firstName, lastName, workerTwo);

            Assert.AreEqual(staleViewId, customer._rtable_ViewId, "customer._rtable_ViewId mismatch");
            Assert.AreEqual("workerOne", customer.Email, "customer.Email mismatch");

            Console.WriteLine("workerOne write Succeeded");
            Console.WriteLine("workerTwo got PreconditionFailed because repair failed since row was locked.");
        }

        /// <summary>
        /// 1 - Thread_1: locks the row in Tail (using stale ViewId)
        /// 2 - Thread_2: reads the row (using new ViewId)
        /// 3 - Thread_1: succeeds to commit the write to Tail (using stale Viewid)
        /// 4 - Thread_2: performs the update
        ///               - row is repaired
        ///               - succeeds to commit the write (using new Viewid)
        /// </summary>
        [Test(Description = "Concurrent updates: first uses a stale View and the second uses a newView. " +
                            "Second update reads uncommitted row (by first), but repairs it once it is committed (by first)")]
        public void UpdateInStaleViewConflictingWithUpdateInNewView_T02()
        {
            ReplicatedTableConfigurationServiceV2 configServiceOne, configServiceTwo;

            // setup:        Acc0       Acc1
            //  stale view = [None] ->  [RW]
            //  new view   = [WO]   ->  [RW]
            SetupStaleViewAndNewView(out configServiceOne, out configServiceTwo);

            long staleViewId = configServiceOne.GetTableView(this.repTable.TableName).ViewId;
            long latestViewId = configServiceTwo.GetTableView(this.repTable.TableName).ViewId;

            string firstName = "FirstName01";
            string lastName = "LastName01";

            /*
             * 1 - WorkerOne => inserts an entry in stale View
             */
            var workerOne = new ReplicatedTable(this.repTable.TableName, configServiceOne);
            var workerTwo = new ReplicatedTable(this.repTable.TableName, configServiceTwo);

            var customer = new CustomerEntity(firstName, lastName);
            customer.Email = "***";
            InsertCustormer(customer, workerOne);

            string accountNameToTamper = this.rtableTestConfiguration.StorageInformation.AccountNames[1];
            Console.WriteLine("RunHttpManglerBehaviorHelper(): accountNameToTamper={0}", accountNameToTamper);

            TableResult oldUpdateResult = null;
            TableResult newUpdateResult = null;
            bool triggerUpdateWithNewView = false;
            bool oldUpdateResume = false;
            bool updateWithOldViewFinished = false;

            // Start new Update in wait
            var newUpdateTask = Task.Run(() =>
            {
                while (!triggerUpdateWithNewView)
                {
                    Thread.Sleep(100);
                }

                /*
                 * 3 - Executes after step 2 below:
                 *     WorkerTwo => reads the entry using new View while it is locked by WorkeOne (not yet commited)
                 */
                customer = RetrieveCustomer(firstName, lastName, workerTwo);

                // Signal old Update to resume ... and wait for it to commit the write
                oldUpdateResume = true;
                while (!updateWithOldViewFinished)
                {
                    Thread.Sleep(100);
                }

                /*
                 * 4 - WorkerTwo => updates the entry using new View ...
                 */
                customer.Email = "workerTwo";
                newUpdateResult = workerTwo.Replace(customer);
            });


            // Delay behavior
            ProxyBehavior[] behaviors =
            {
                TamperBehaviors.TamperAllRequestsIf(
                    (session =>
                    {
                        // => trigger new update to start
                        triggerUpdateWithNewView = true;

                        // Delaying commit to the Tail by stale view Update
                        while (!oldUpdateResume)
                        {
                            Thread.Sleep(100);
                        }
                    }),
                    (session =>
                    {
                        var body = session.GetRequestBodyAsString();

                        // Commit to Tail by stale view
                        if (session.hostname.Contains(accountNameToTamper + ".") &&
                            session.HTTPMethodIs("PUT") &&
                            body.Contains("\"Email\":\"workerOne\"") &&
                            body.Contains(string.Format("\"_rtable_ViewId\":\"{0}\"", staleViewId)) &&
                            body.Contains("\"_rtable_RowLock\":false"))
                        {
                            return true;
                        }

                        return false;
                    })),
            };

            /*
             * 2 - WorkerOne => update an entry using the stale View
             */
            using (new HttpMangler(false, behaviors))
            {
                customer = RetrieveCustomer(firstName, lastName, workerOne);
                customer.Email = "workerOne";

                oldUpdateResult = workerOne.Replace(customer);

                updateWithOldViewFinished = true;
            }

            // Wait on new Update to finish
            newUpdateTask.Wait();


            // Expected behavior:

            // Thread_1 (stale ViewId)
            Assert.IsNotNull(oldUpdateResult, "oldUpdateResult = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, oldUpdateResult.HttpStatusCode, "oldUpdateResult.HttpStatusCode mismatch");
            Console.WriteLine("Update in stale View Succeeded with HttpStatus={0}", oldUpdateResult.HttpStatusCode);

            // Thread_2 (new ViewId)
            Assert.IsNotNull(newUpdateResult, "newUpdateResult = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, newUpdateResult.HttpStatusCode, "newUpdateResult.HttpStatusCode mismatch");
            Console.WriteLine("Update in new View Succeeded with HttpStatus={0}", newUpdateResult.HttpStatusCode);

            customer = RetrieveCustomer(firstName, lastName, workerTwo);

            Assert.AreEqual(latestViewId, customer._rtable_ViewId, "customer._rtable_ViewId mismatch");
            Assert.AreEqual("workerTwo", customer.Email, "customer.Email mismatch");

            Console.WriteLine("workerOne write Succeeded, was repaired in new view by workerTwo.");
            Console.WriteLine("workerTwo Succeeded to overwrite afterward");
        }

        /// <summary>
        /// 1 - Thread_1: inserts a new entry with Lock in Tail (using stale ViewId)
        /// 2 - Thread_2: starts an insert (using new ViewId)
        ///               - row is locked so repair row will fail with a conflict
        ///                      => insert fails with Conflict
        /// 3 - Thread_1: commits to Tail (using stale viewId)
        ///                      => Succeed
        /// </summary>
        [Test(Description = "Concurrent inserts: first uses a stale View and the second uses a newView. " +
                            "Second insert happens after the first insert locks the row, but before it commits it.")]
        public void InsertInStaleViewConflictingWithInsertInNewView()
        {
            ReplicatedTableConfigurationServiceV2 configServiceOne, configServiceTwo;

            // setup:        Acc0       Acc1
            //  stale view = [None] ->  [RW]
            //  new view   = [WO]   ->  [RW]
            SetupStaleViewAndNewView(out configServiceOne, out configServiceTwo);

            long staleViewId = configServiceOne.GetTableView(this.repTable.TableName).ViewId;
            long latestViewId = configServiceTwo.GetTableView(this.repTable.TableName).ViewId;

            string firstName = "FirstName01";
            string lastName = "LastName01";

            var workerOne = new ReplicatedTable(this.repTable.TableName, configServiceOne);
            var workerTwo = new ReplicatedTable(this.repTable.TableName, configServiceTwo);

            string accountNameToTamper = this.rtableTestConfiguration.StorageInformation.AccountNames[1];
            Console.WriteLine("RunHttpManglerBehaviorHelper(): accountNameToTamper={0}", accountNameToTamper);

            TableResult oldInsertResult = null;
            TableResult newInsertResult = null;
            bool triggerInsertWithNewView = false;
            bool oldInsertResume = false;

            // Start new newInsertTask in wait
            var newInsertTask = Task.Run(() =>
            {
                while (!triggerInsertWithNewView)
                {
                    Thread.Sleep(100);
                }

                /*
                 * 2 - Executes after step 1 below:
                 *     WorkerTwo => Insert a new row using new view
                 */
                var customer = new CustomerEntity(firstName, lastName);
                customer.Email = "workerTwo";

                newInsertResult = workerTwo.Insert(customer);

                // Signal old Insert to resume
                oldInsertResume = true;
            });


            // Delay behavior
            ProxyBehavior[] behaviors =
            {
                TamperBehaviors.TamperAllRequestsIf(
                    (session =>
                    {
                        // => trigger new insert to start
                        triggerInsertWithNewView = true;

                        // Delaying commit to the Tail by stale view Update
                        while (!oldInsertResume)
                        {
                            Thread.Sleep(100);
                        }
                    }),
                    (session =>
                    {
                        var body = session.GetRequestBodyAsString();

                        // Commit to Tail by stale view
                        if (session.hostname.Contains(accountNameToTamper + ".") &&
                            session.HTTPMethodIs("PUT") &&
                            body.Contains("\"Email\":\"workerOne\"") &&
                            body.Contains(string.Format("\"_rtable_ViewId\":\"{0}\"", staleViewId)) &&
                            body.Contains("\"_rtable_RowLock\":false"))
                        {
                            return true;
                        }

                        return false;
                    })),
            };

            /*
             * 1 - WorkerOne => update an entry using the stale View
             */
            using (new HttpMangler(false, behaviors))
            {
                var customer = new CustomerEntity(firstName, lastName);
                customer.Email = "workerOne";

                oldInsertResult = workerOne.Insert(customer);
            }

            // Wait on new Insert to finish
            newInsertTask.Wait();


            // Expected behavior:

            // Thread_1 (stale ViewId)
            Assert.IsNotNull(oldInsertResult, "oldInsertResult = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, oldInsertResult.HttpStatusCode, "oldInsertResult.HttpStatusCode mismatch");
            Console.WriteLine("Insert in stale View Succeeded with HttpStatus={0}", oldInsertResult.HttpStatusCode);

            // Thread_2 (new ViewId)
            Assert.IsNotNull(newInsertResult, "newInsertResult = null");
            Assert.AreEqual((int)HttpStatusCode.Conflict, newInsertResult.HttpStatusCode, "newUpdateResult.HttpStatusCode mismatch");
            Console.WriteLine("Insert in new View failed with HttpStatus={0}", newInsertResult.HttpStatusCode);

            var currCustomer = RetrieveCustomer(firstName, lastName, workerTwo);

            Assert.AreEqual(staleViewId, currCustomer._rtable_ViewId, "customer._rtable_ViewId mismatch");
            Assert.AreEqual("workerOne", currCustomer.Email, "customer.Email mismatch");

            Console.WriteLine("workerOne write Succeeded");
            Console.WriteLine("workerTwo got Conflict because row was lock.");
        }

        [Test(Description = "RepairTable when we have some rows with higher wiewId and IgnoreHigherViewIdRows flag is set")]
        public void RepairTableWontRepairRowsWithHigherViewIdWhenIgnoreHigherViewIdRowsFlagIsSet()
        {
            TableResult result;
            CustomerEntity customer;


            // - View has 2 replicas ?
            View view = this.configurationWrapper.GetWriteView();
            Assert.IsTrue(view.Chain.Count == 2, "expects 2 replicas only!");


            // - Remove the Head, [None]->[RW]
            RemoveHeadFromView(view.Name, 600, this.configurationService);


            // 1 - Insert entries in old viewId
            var rtable = new ReplicatedTable(this.repTable.TableName, this.configurationService);

            string firstName = "FirstName";
            string lastName = "LastName";

            for (int i = 0; i < 10; i++)
            {
                customer = new CustomerEntity(firstName + i, lastName + i);

                rtable.Insert(customer);
            }


            // 2 - Increase viewId => so we can create rows with higher viewId
            //   - Update entry #5 and #8 in new viewId
            ReplicatedTableConfiguration config;
            ReplicatedTableQuorumReadResult readStatus = this.configurationService.RetrieveConfiguration(out config);
            Assert.IsTrue(readStatus.Code == ReplicatedTableQuorumReadCode.Success);
            ReplicatedTableConfigurationStore viewConfg = config.GetView(view.Name);
            viewConfg.ViewId += 100;
            this.configurationService.UpdateConfiguration(config);

            foreach (int entryId in new int[] { 5, 8 })
            {
                result = rtable.Retrieve(firstName + entryId, lastName + entryId);

                Assert.IsTrue(result != null && result.HttpStatusCode == (int)HttpStatusCode.OK && new CustomerEntity((ReplicatedTableEntity)result.Result) != null, "Retrieve customer failed");

                customer = new CustomerEntity((ReplicatedTableEntity)result.Result);
                customer.Email = "new_view@email.com";

                result = rtable.Replace(customer);

                Assert.IsTrue(result != null && result.HttpStatusCode == (int)HttpStatusCode.NoContent, "Update customer failed");
            }


            // 3 - Restore previous viewId, and,
            //   - Set 'IgnoreHigherViewIdRows' flag so we ignore rows with higher viewIds
            readStatus = this.configurationService.RetrieveConfiguration(out config);
            Assert.IsTrue(readStatus.Code == ReplicatedTableQuorumReadCode.Success);
            viewConfg = config.GetView(view.Name);
            viewConfg.ViewId -= 100;
            config.SetIgnoreHigherViewIdRowsFlag(true);
            this.configurationService.UpdateConfiguration(config);

            try
            {
                // Check Retrieve of row #5 and #8 returns NotFound
                foreach (int entryId in new int[] { 5, 8 })
                {
                    var retrievedResult = rtable.Retrieve(firstName + entryId, lastName + entryId);

                    Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");
                    Assert.AreEqual((int)HttpStatusCode.NotFound, retrievedResult.HttpStatusCode, "retrievedResult.HttpStatusCode mismatch");
                }
            }
            catch (ReplicatedTableStaleViewException)
            {
                Assert.Fail("Retrieve() is expected to NotFound the row, but got RTableStaleViewException !");
            }


            // 4 - Now insert a Head [WO]->[RW]
            //   - Then, call RepairTable ...
            InsertHeadInView(view.Name, 600, this.configurationService);
            ReconfigurationStatus status = rtable.RepairTable(0, null);
            Assert.AreEqual(status, ReconfigurationStatus.PARTIAL_FAILURE, "rows with higher viewId should not be repaired");


            // 5 - Check rows with higher viewId still NotFound, even after repair ...
            try
            {
                // Check Retrieve of row #5 and #8 returns NotFound
                foreach (int entryId in new int[] { 5, 8 })
                {
                    var retrievedResult = rtable.Retrieve(firstName + entryId, lastName + entryId);

                    Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");
                    Assert.AreEqual((int)HttpStatusCode.NotFound, retrievedResult.HttpStatusCode, "retrievedResult.HttpStatusCode mismatch");
                }
            }
            catch (ReplicatedTableStaleViewException)
            {
                Assert.Fail("Retrieve() is expected to NotFound the row, but got RTableStaleViewException !");
            }
        }

        #region // moving from [WO]->[RW] to [RW]->[none] without prior repair"

        [Test(Description = "Read a row in old view, cut-off Tail w/o prior repair to Head, then update the row")]
        public void CaseNoRepair_ReadRowFromOldViewCutOffTailUpdateRow()
        {
            string firstName = "FirstName";
            string lastName = "LastName";

            TableResult result;
            CustomerEntity customer;

            // - View has 2 replicas ?
            View view = this.configurationWrapper.GetWriteView();
            Assert.IsTrue(view.Chain.Count == 2, "expects 2 replicas only!");


            // 1 - [None]->[RW]
            ReplicatedTableConfiguration config;
            this.configurationService.RetrieveConfiguration(out config);
            ReplicatedTableConfigurationStore viewConfg = config.GetView(view.Name);
            viewConfg.ReplicaChain[0].Status = ReplicaStatus.None;
            viewConfg.ViewId++;
            this.configurationService.UpdateConfiguration(config);

            // *** - Insert entries in old viewId
            var rtable = new ReplicatedTable(this.repTable.TableName, this.configurationService);
            for (int i = 0; i < 3; i++)
            {
                customer = new CustomerEntity(firstName + i, lastName + i);

                rtable.Insert(customer);
            }


            // 2 - Insert Head => [WO]->[RW]
            this.configurationService.RetrieveConfiguration(out config);
            viewConfg = config.GetView(view.Name);
            viewConfg.ReplicaChain[0].Status = ReplicaStatus.WriteOnly;
            viewConfg.ViewId++;
            this.configurationService.UpdateConfiguration(config);

            // *** - Insert entries in new viewId
            rtable = new ReplicatedTable(this.repTable.TableName, this.configurationService);
            for (int i = 10; i < 13; i++)
            {
                customer = new CustomerEntity(firstName + i, lastName + i);

                rtable.Insert(customer);
            }


            // => Read old entry - from Tail -
            int entryId = 1;
            result = rtable.Retrieve(firstName + entryId, lastName + entryId);


            // 3 - Cut-off Tail without repairing replicas
            configurationService.RetrieveConfiguration(out config);
            viewConfg = config.GetView(view.Name);
            viewConfg.ReplicaChain[0].Status = ReplicaStatus.ReadWrite;
            viewConfg.ReplicaChain[1].Status = ReplicaStatus.None;
            viewConfg.ViewId++;
            this.configurationService.UpdateConfiguration(config);

            // => Update the row
            customer = new CustomerEntity((ReplicatedTableEntity)result.Result);
            customer.Email = "new_view@email.com";
            result = rtable.Replace(customer);
            Assert.IsTrue(result != null && result.HttpStatusCode == (int)HttpStatusCode.NotFound, "Update customer should failed");
        }

        [Test(Description = "Read a row in new view, cut-off Tail w/o prior repair to Head, then update the row")]
        public void CaseNoRepair_ReadRowFromNewViewCutOffTailUpdateRow()
        {
            string firstName = "FirstName";
            string lastName = "LastName";

            TableResult result;
            CustomerEntity customer;

            // - View has 2 replicas ?
            View view = this.configurationWrapper.GetWriteView();
            Assert.IsTrue(view.Chain.Count == 2, "expects 2 replicas only!");


            // 1 - [None]->[RW]
            ReplicatedTableConfiguration config;
            this.configurationService.RetrieveConfiguration(out config);
            ReplicatedTableConfigurationStore viewConfg = config.GetView(view.Name);
            viewConfg.ReplicaChain[0].Status = ReplicaStatus.None;
            viewConfg.ViewId++;
            this.configurationService.UpdateConfiguration(config);

            // *** - Insert entries in old viewId
            var rtable = new ReplicatedTable(this.repTable.TableName, this.configurationService);
            for (int i = 0; i < 3; i++)
            {
                customer = new CustomerEntity(firstName + i, lastName + i);

                rtable.Insert(customer);
            }


            // 2 - Insert Head => [WO]->[RW]
            this.configurationService.RetrieveConfiguration(out config);
            viewConfg = config.GetView(view.Name);
            viewConfg.ReplicaChain[0].Status = ReplicaStatus.WriteOnly;
            viewConfg.ViewId++;
            this.configurationService.UpdateConfiguration(config);

            // *** - Insert entries in new viewId
            rtable = new ReplicatedTable(this.repTable.TableName, this.configurationService);
            for (int i = 10; i < 13; i++)
            {
                customer = new CustomerEntity(firstName + i, lastName + i);

                rtable.Insert(customer);
            }


            // => Read old entry - from Tail -
            int entryId = 10;
            result = rtable.Retrieve(firstName + entryId, lastName + entryId);


            // 3 - Cut-off Tail without repairing replicas
            configurationService.RetrieveConfiguration(out config);
            viewConfg = config.GetView(view.Name);
            viewConfg.ReplicaChain[0].Status = ReplicaStatus.ReadWrite;
            viewConfg.ReplicaChain[1].Status = ReplicaStatus.None;
            viewConfg.ViewId++;
            this.configurationService.UpdateConfiguration(config);

            // => Update the row
            customer = new CustomerEntity((ReplicatedTableEntity)result.Result);
            customer.Email = "new_view@email.com";
            result = rtable.Replace(customer);
            Assert.IsTrue(result != null && result.HttpStatusCode == (int)HttpStatusCode.NoContent, "Update customer failed");
        }

        #endregion

        #region Config helpers

        private void RemoveHeadFromView(string viewName, int leaseDuration, ReplicatedTableConfigurationServiceV2 configService)
        {
            // Download config using provided instance
            ReplicatedTableConfiguration config;
            ReplicatedTableQuorumReadResult readStatus = configService.RetrieveConfiguration(out config);

            Assert.IsTrue(readStatus.Code == ReplicatedTableQuorumReadCode.Success);

            // Remove the Head
            ReplicatedTableConfigurationStore viewConfg = config.GetView(viewName);
            viewConfg.ReplicaChain[0].Status = ReplicaStatus.None;
            viewConfg.ViewId++;

            config.LeaseDuration = leaseDuration;

            // Upload config using provided instance
            configService.UpdateConfiguration(config);
        }

        private void InsertHeadInView(string viewName, int leaseDuration, ReplicatedTableConfigurationServiceV2 configService)
        {
            // Download config using provided instance
            ReplicatedTableConfiguration config;
            ReplicatedTableQuorumReadResult readStatus = configurationService.RetrieveConfiguration(out config);

            Assert.IsTrue(readStatus.Code == ReplicatedTableQuorumReadCode.Success);

            // Add the Head
            ReplicatedTableConfigurationStore viewConfg = config.GetView(viewName);
            viewConfg.ReplicaChain[0].Status = ReplicaStatus.WriteOnly;
            viewConfg.ViewId++;

            config.LeaseDuration = leaseDuration;

            // Upload config using provided instance
            configService.UpdateConfiguration(config);
        }

        #endregion

        #region Customer APIs

        private void InsertCustormer(CustomerEntity customer, ReplicatedTable repTable)
        {
            TableResult result = repTable.Insert(customer);

            Assert.AreNotEqual(null, result, "result = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, result.Result, "result.Result = null");
        }

        private CustomerEntity RetrieveCustomer(string firstName, string lastName, ReplicatedTable repTable)
        {
            TableResult retrievedResult = repTable.Retrieve(firstName, lastName);

            Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");
            Assert.AreEqual((int)HttpStatusCode.OK, retrievedResult.HttpStatusCode, "retrievedResult.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, retrievedResult.Result, "retrievedResult.Result = null");

            return new CustomerEntity((ReplicatedTableEntity)retrievedResult.Result);
        }

        private void UpdateCustomer(CustomerEntity customer, ReplicatedTable repTable)
        {
            TableResult updateResult = repTable.Replace(customer);

            Assert.IsNotNull(updateResult, "updateResult = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, updateResult.HttpStatusCode, "updateResult.HttpStatusCode mismatch");
        }

        #endregion
    }
}
