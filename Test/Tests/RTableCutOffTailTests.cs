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
    using Microsoft.WindowsAzure.Test.Network;
    using Microsoft.WindowsAzure.Test.Network.Behaviors;

    [TestFixture]
    [Parallelizable(ParallelScope.None)]
    public class RTableCutOffTailTests : HttpManglerTestBase
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
            // reload the view with both replicas Head/Tail so they get cleaned
            EnableTailInView(this.configurationWrapper.GetWriteView().Name, 60, this.configurationService);
            this.configurationService.ConfigurationChangeNotification();

            base.DeleteAllRtableResources();
        }

        /// <summary>
        /// Setup:
        /// 1 - Upload config => [Head]->[Tail]
        /// 2 - Initialize Thread_1 with previous config which never expires (6000 sec.)
        /// 3 - Cut-off Tail and upload config => [Head]
        /// 4 - Initialize Thread_2 with new config which never expires (6000 sec.)
        /// </summary>
        void SetupStaleViewAndNewView(out ReplicatedTableConfigurationServiceV2 configServiceOne, out ReplicatedTableConfigurationServiceV2 configServiceTwo)
        {
            // Verify current View has 2 replicas ?
            View view = this.configurationWrapper.GetWriteView();
            Assert.IsTrue(view.Chain.Count == 2, "expects 2 replicas only!");

            int leaseDurationInSec = 6000;

            /*
             * 1 - Upload config [Head]->[Tail]
             */
            configServiceOne = new ReplicatedTableConfigurationServiceV2(this.configurationInfos, this.connectionStringMap, useHttps);
            UploadHeadTailView(view.Name, leaseDurationInSec, configServiceOne);

            /*
             * 2 - Cut-off the Tail, set a high LeaseDuration.
             *     The new config will be [Head]
             */
            configServiceTwo = new ReplicatedTableConfigurationServiceV2(this.configurationInfos, this.connectionStringMap, useHttps);
            CutOffTailInView(view.Name, leaseDurationInSec, configServiceTwo);

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
            //  stale view = [H] ->  [T]
            //  new view   = [H]
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
             *                  row is updated in [Head] replica
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
             *                  Read is served from [Head] i.e. we are not reading stale data from [Tail]
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
        /// 1 - Thread_1: locks the row in [Head] but fails to lock the [Tail] (using stale ViewId)
        /// 2 - Thread_2: reads from [Head] (using new ViewId) => Succeeds
        /// 3 - Thread_1: reads from [Head] (using stale ViewId) => Succeeds eventhough the row is "uncommitted".
        ///     This is because "ReadTailIndex = [Head]" which means the [Head] is treated as a [Tail] for Reads.
        ///     => data is assumed "committed"
        /// </summary>
        [Test(Description = "Reading uncommitted data from a replica identified by ReadTailIndex won't throw \"uncommitted\" exception")]
        public void ReadingUncommittedDataFromReplicaIdentifiedByReadTailIndexWontThrow()
        {
            ReplicatedTableConfigurationServiceV2 configServiceOne, configServiceTwo;

            // setup:        Acc0    Acc1
            //  stale view = [H] ->  [T]
            //  new view   = [H]
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
            CustomerEntity customerSeenByNewView = null;

            // Delay behavior
            ProxyBehavior[] behaviors =
            {
                TamperBehaviors.TamperAllRequestsIf(
                    (session =>
                    {
                        session.oRequest.FailSession((int)HttpStatusCode.ServiceUnavailable, "ServerBusy", "");
                    }),
                    (session =>
                    {
                        var body = session.GetRequestBodyAsString();

                        // Fail Lock to Tail by stale view
                        if (session.hostname.Contains(accountNameToTamper + ".") &&
                            session.HTTPMethodIs("PUT") &&
                            body.Contains("\"Email\":\"workerOne\"") &&
                            body.Contains(string.Format("\"_rtable_ViewId\":\"{0}\"", staleViewId)) &&
                            body.Contains("\"_rtable_RowLock\":true"))
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

                oldUpdateResult = workerOne.Replace(customer, oldUpdateResult);
            }


            // Expected behavior:

            // Thread_1 (stale ViewId) fails to commit to Tail
            Assert.IsNotNull(oldUpdateResult, "oldUpdateResult = null");
            Assert.AreEqual((int)HttpStatusCode.ServiceUnavailable, oldUpdateResult.HttpStatusCode, "oldUpdateResult.HttpStatusCode mismatch");
            Console.WriteLine("Update in stale View Succeeded with HttpStatus={0}", oldUpdateResult.HttpStatusCode);

            // Thread_1 (stale ViewId): Reads the entry => Succeeds eventhough the row is "uncommitted".
            //                          "ReadTailIndex = [Head]" means [Head] is treated as [Tail] for Reads.
            //                           => row is assumed "committed"
            customer = RetrieveCustomer(firstName, lastName, workerOne);
            Assert.IsNotNull(customer, "customer = null");
            Assert.AreEqual("workerOne", customer.Email, "customer.Email mismatch");

            // Thread_2 (new ViewId): Reads the entry => Succeed since reading from Head
            customerSeenByNewView = RetrieveCustomer(firstName, lastName, workerTwo);
            Assert.IsNotNull(customerSeenByNewView, "customerSeenByNewView = null");
            Assert.AreEqual("workerOne", customer.Email, "customer.Email mismatch");
        }


        #region Config helpers

        private void UploadHeadTailView(string viewName, int leaseDuration, ReplicatedTableConfigurationServiceV2 configService)
        {
            // Download config using provided instance
            ReplicatedTableConfiguration config;
            ReplicatedTableQuorumReadResult readStatus = configService.RetrieveConfiguration(out config);

            Assert.IsTrue(readStatus.Code == ReplicatedTableQuorumReadCode.Success);

            // [Head] -> [Tail]
            ReplicatedTableConfigurationStore viewConfg = config.GetView(viewName);
            Assert.IsTrue(viewConfg.GetCurrentReplicaChain().Count == 2);

            // Force Reads on Head replica
            viewConfg.ReadViewTailIndex = 0;
            viewConfg.ViewId++;

            config.LeaseDuration = leaseDuration;

            // Upload config using provided instance
            configService.UpdateConfiguration(config);
        }

        private void CutOffTailInView(string viewName, int leaseDuration, ReplicatedTableConfigurationServiceV2 configService)
        {
            // Download config using provided instance
            ReplicatedTableConfiguration config;
            ReplicatedTableQuorumReadResult readStatus = configurationService.RetrieveConfiguration(out config);

            Assert.IsTrue(readStatus.Code == ReplicatedTableQuorumReadCode.Success);

            // Cut-off Tail => [Head]
            ReplicatedTableConfigurationStore viewConfg = config.GetView(viewName);
            Assert.IsTrue(viewConfg.ReplicaChain.Count == 2);
            Assert.IsTrue(viewConfg.ReadViewTailIndex == 0);

            viewConfg.ReplicaChain[1].Status = ReplicaStatus.None;
            viewConfg.ViewId++;

            config.LeaseDuration = leaseDuration;

            // Upload config using provided instance
            configService.UpdateConfiguration(config);
        }

        private void EnableTailInView(string viewName, int leaseDuration, ReplicatedTableConfigurationServiceV2 configService)
        {
            // Download config using provided instance
            ReplicatedTableConfiguration config;
            ReplicatedTableQuorumReadResult readStatus = configurationService.RetrieveConfiguration(out config);

            Assert.IsTrue(readStatus.Code == ReplicatedTableQuorumReadCode.Success);

            // Enable the Tail
            ReplicatedTableConfigurationStore viewConfg = config.GetView(viewName);
            Assert.IsTrue(viewConfg.ReplicaChain.Count == 2);
            Assert.IsTrue(viewConfg.ReadViewTailIndex == 0);

            viewConfg.ReplicaChain[1].Status = ReplicaStatus.ReadWrite;
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

            return (CustomerEntity)retrievedResult.Result;
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

