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
    using System.Linq;
    using System.Net;
    using Microsoft.WindowsAzure.Storage.Table;

    [TestFixture]
    [Parallelizable(ParallelScope.None)]
    public class RTableInsertHeadTests : HttpManglerTestBase
    {
        private string tableName;
        private bool useHttps = true;

        [OneTimeSetUp]
        public void TestFixtureSetup()
        {
            this.LoadTestConfiguration();
            this.tableName = this.GenerateRandomTableName();
            Console.WriteLine("tableName = {0}", this.tableName);

            this.SetupRTableEnv(this.tableName, useHttps);
        }

        [OneTimeTearDown]
        public void TestFixtureTearDown()
        {
            base.DeleteAllRtableResources();
        }

        /// <summary>
        /// Setup:
        /// 1 - Upload config w/o Head => [None]->[RW]->...->[RW]
        /// 2 - Initialize Thread_1 with previous config which never expires (6000 sec.)
        /// 3 - Upload config with Head => [WO]->[RW]->...->[RW]
        /// 4 - Initialize Thread_2 with new config which never expires (6000 sec.)
        ///
        /// Test:
        /// 5 - Thread_1: inserts a new entry (using stale viewId)
        /// 6 - Thread_2: updates the previous entry (using new viewId)
        /// 7 - Thread_1: fails to retrieve the entry (using stale viewId)
        /// 8 - Notify of config change => Thread_1 sees latest View
        /// 9 - Thread_1: retrieves the entry (using latest viewId)
        /// </summary>
        [Test(Description = "Trigger configuration change notification after detecting a stale view")]
        public void ConfigurationChangeNotificationAfterDetectingStaleView()
        {
            // Verify current View has at least 2 replicas ?
            View view = this.configurationWrapper.GetWriteView();
            Assert.IsTrue(view.Chain.Count >= 2);

            int leaseDurationInSec = 6000;

            /*
             * 1 - Remove the Head, set a high LeaseDuration.
             *     The new config will be [None]->[RW]->...->[RW]
             */
            var configServiceOne = new ReplicatedTableConfigurationServiceV2(this.configurationInfos, this.connectionStringMap, useHttps);
            RemoveHeadFromView(view.Name, leaseDurationInSec, configServiceOne);

            /*
             * 2 - Insert the Head, set a high LeaseDuration.
             *     The new config will be [WO]->[RW]->...->[RW]
             */
            var configServiceTwo = new ReplicatedTableConfigurationServiceV2(this.configurationInfos, this.connectionStringMap, useHttps);
            InsertHeadInView(view.Name, leaseDurationInSec, configServiceTwo);

            /*
             * At this stage:
             *  - configServiceOne sees a stale View (LeaseDuration = 6000sec.)
             *  - configServiceTwo sees the latest View (LeaseDuration = 6000sec.)
             */
            long staleViewId = configServiceOne.GetTableView(this.tableName).ViewId;
            long latestViewId = configServiceTwo.GetTableView(this.tableName).ViewId;
            Assert.AreNotEqual(staleViewId, latestViewId, "View should have changed !!!");


            string firstName = "FirstName01";
            string lastName = "LastName01";

            /*
             * 3 - WorkerOne => inserts an entry using the stale View
             */
            var workerOne = new ReplicatedTable(this.tableName, configServiceOne);

            var customer = new CustomerEntity(firstName, lastName);
            customer.Email = "workerOne@dns.com";
            InsertCustormer(customer, workerOne);

            customer = RetrieveCustomer(firstName, lastName, workerOne);

            Assert.AreEqual(staleViewId, customer._rtable_ViewId, "customer._rtable_ViewId mismatch");
            Assert.AreEqual("workerOne@dns.com", customer.Email, "customer.Email mismatch");
            Console.WriteLine("workerOne inserted a customer in ViewId={0}", staleViewId);


            /*
             * 4 - WorkerTwo => updates the entry using latest View
             */
            var workerTwo = new ReplicatedTable(this.tableName, configServiceTwo);

            customer = RetrieveCustomer(firstName, lastName, workerTwo);
            customer.Email = "workerTwo@dns.com";
            UpdateCustomer(customer, workerTwo);

            customer = RetrieveCustomer(firstName, lastName, workerTwo);

            Assert.AreEqual(latestViewId, customer._rtable_ViewId, "customer._rtable_ViewId mismatch");
            Assert.AreEqual("workerTwo@dns.com", customer.Email, "customer.Email mismatch");
            Console.WriteLine("workerTwo updated the customer in ViewId={0}", latestViewId);


            /*
             * 5 - WorkerOne => access existing entry using "the stale View"
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
             * 6 - Notify "configServiceOne" instance of config change
             */
            Assert.AreEqual(configServiceOne.GetTableView(this.tableName).ViewId, staleViewId, "we should see old View!!!");
            {
                configServiceOne.ConfigurationChangeNotification();
            }
            Assert.AreEqual(configServiceOne.GetTableView(this.tableName).ViewId, latestViewId, "we should see latest View!!!");


            /*
             * 7 - WorkerOne => updates the entry using latest View
             */
            customer = RetrieveCustomer(firstName, lastName, workerTwo);
            customer.Email = "happy.workerOne@dns.com";
            UpdateCustomer(customer, workerTwo);

            customer = RetrieveCustomer(firstName, lastName, workerTwo);

            Assert.AreEqual(latestViewId, customer._rtable_ViewId, "customer._rtable_ViewId mismatch");
            Assert.AreEqual("happy.workerOne@dns.com", customer.Email, "customer.Email mismatch");
            Console.WriteLine("workerOne updated the customer in ViewId={0}", latestViewId);
        }

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
            TableOperation operation = TableOperation.Insert(customer);
            TableResult result = repTable.Execute(operation);

            Assert.AreNotEqual(null, result, "result = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, result.Result, "result.Result = null");
        }

        private CustomerEntity RetrieveCustomer(string firstName, string lastName, ReplicatedTable repTable)
        {
            TableOperation operation = TableOperation.Retrieve<CustomerEntity>(firstName, lastName);
            TableResult retrievedResult = repTable.Execute(operation);

            Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");
            Assert.AreEqual((int)HttpStatusCode.OK, retrievedResult.HttpStatusCode, "retrievedResult.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, retrievedResult.Result, "retrievedResult.Result = null");

            return (CustomerEntity)retrievedResult.Result;
        }

        private void UpdateCustomer(CustomerEntity customer, ReplicatedTable repTable)
        {
            TableOperation operation = TableOperation.Replace(customer);
            TableResult updateResult = repTable.Execute(operation);

            Assert.IsNotNull(updateResult, "updateResult = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, updateResult.HttpStatusCode, "updateResult.HttpStatusCode mismatch");
        }

        #endregion

    }
}
