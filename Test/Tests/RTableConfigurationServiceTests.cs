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
    using Microsoft.Azure.Toolkit.Replication;
    using Microsoft.WindowsAzure.Storage.Table;
    
    [TestFixture]
    public class RTableConfigurationServiceTests : RTableLibraryTestBase
    {
        //For each test method, we create new tables
        [SetUp]
        public void TestFixtureSetup()
        {
            this.LoadTestConfiguration();

            string tableName = this.GenerateRandomTableName();
            this.SetupRTableEnv(true, tableName);
        }

        [TearDown]
        public void TestFixtureTearDown()
        {
            this.DeleteAllRtableResources();
        }

        //
        // binaries.amd64fre\Gateway\UnitTests>
        // runuts /fixture=Microsoft.WindowsAzure.Storage.RTableTest.RTableConfigurationServiceTests
        //
        #region RTable Configuration service test methods

        [Test(Description = "Validate the read and write view when view is stable")]
        public void ValidateRtableReadWriteViews()
        {
            Assert.IsTrue(this.repTable.Exists(), "RTable does not exist");

            View readView = this.configurationService.GetReadView();
            View writeView = this.configurationService.GetWriteView();
                
            // validate the views
            Assert.IsTrue(this.configurationService.IsViewStable());
            Assert.IsTrue(readView == writeView);
        }

        [Test(Description = "Validate the read and write view when a new replica is added")]
        public void AddReplicaTest()
        {
            Assert.IsTrue(this.repTable.Exists(), "RTable does not exist");

            View v = this.configurationService.GetWriteView();
            int index = v.Chain.Count - 1;
            string accountName = v.GetReplicaInfo(index).StorageAccountName;
            string accountKey = v.GetReplicaInfo(index).StorageAccountKey;

            List<ReplicaInfo> replicas = new List<ReplicaInfo>();
            for (int i = 0; i <= index; i++)
            {
                replicas.Add(v.Chain[i].Item1);
            }

            //Just add the last replica again at the head to simulate a new replica addition
            ReplicaInfo newReplica = new ReplicaInfo()
            {
                StorageAccountName = accountName,
                StorageAccountKey = accountKey
            };

            //Add the new replica at the head
            replicas.Insert(0, newReplica);

            this.configurationService.UpdateConfiguration(replicas, 1);

            // validate all state
            Assert.IsFalse(this.configurationService.IsViewStable(), "View = {0}", this.configurationService.GetWriteView().IsStable);
            View readView = this.configurationService.GetReadView();
            View writeView = this.configurationService.GetWriteView();
            long viewIdAfterFirstUpdate = writeView.ViewId;
            Assert.IsTrue(readView != writeView);

            int headIndex = 0;
            long readViewHeadViewId = readView.GetReplicaInfo(headIndex).ViewInWhichAddedToChain;
            Assert.IsTrue(writeView.GetReplicaInfo(headIndex).ViewInWhichAddedToChain == readViewHeadViewId + 1);

            //Now, make the read and write views the same
            this.configurationService.UpdateConfiguration(replicas, 0);
            // validate all state
            Assert.IsTrue(this.configurationService.IsViewStable());
            readView = this.configurationService.GetReadView();
            writeView = this.configurationService.GetWriteView();
            Assert.IsTrue(readView == writeView);
            Assert.IsTrue(readView.ViewId == viewIdAfterFirstUpdate + 1);
        }

        [Test(Description = "Validate that quorum view wins")]
        public void QuorumViewTest()
        {
            Assert.IsTrue(this.repTable.Exists(), "RTable does not exist");
            View v = this.configurationService.GetWriteView();
            int index = v.Chain.Count - 1;
            string accountName = v.GetReplicaInfo(index).StorageAccountName;
            string accountKey = v.GetReplicaInfo(index).StorageAccountKey;

            Assert.IsTrue(v.Chain.Count >= 3, "Replica chain only has {0} accounts", v.Chain.Count);

            List<ReplicaInfo> replicas = new List<ReplicaInfo>();
            for (int i = 0; i <= index; i++)
            {
                replicas.Add(v.Chain[i].Item1);
            }

            //Just add the last replica again at the head to simulate a new replica addition
            ReplicaInfo newReplica = new ReplicaInfo()
            {
                StorageAccountName = accountName,
                StorageAccountKey = accountKey
            };

            //Add the new replica at the head
            replicas.Insert(0, newReplica);

            //
            // create a new config service with only one replica
            // add the new view only to that one
            // check that the majority view has not changed
            // add the same replica to all the blobs
            // now check that the majority view has changed
            //

            this.configurationService.UpdateConfiguration(replicas, 1);

            // validate all state
            Assert.IsFalse(this.configurationService.IsViewStable(), "View = {0}", this.configurationService.GetWriteView().IsStable);
            View readView = this.configurationService.GetReadView();
            View writeView = this.configurationService.GetWriteView();
            long viewIdAfterFirstUpdate = writeView.ViewId;
            Assert.IsTrue(readView != writeView);

            int headIndex = 0;
            long readViewHeadViewId = readView.GetReplicaInfo(headIndex).ViewInWhichAddedToChain;
            Assert.IsTrue(writeView.GetReplicaInfo(headIndex).ViewInWhichAddedToChain == readViewHeadViewId + 1);

            //Now, make the read and write views the same
            this.configurationService.UpdateConfiguration(replicas, 0);
            // validate all state
            Assert.IsTrue(this.configurationService.IsViewStable());
            readView = this.configurationService.GetReadView();
            writeView = this.configurationService.GetWriteView();
            Assert.IsTrue(readView == writeView);
            Assert.IsTrue(readView.ViewId == viewIdAfterFirstUpdate + 1);
        }

        [Test(Description = "TableOperation Repair Row API")]
        public void RTableRepairRow()
        {
            // Insert entity
            Assert.IsTrue(this.repTable.Exists(), "RTable does not exist");

            View fullView = configurationService.GetWriteView();
            List<ReplicaInfo> fullViewReplicas = new List<ReplicaInfo>();
            for (int i = 0; i <= fullView.TailIndex; i++)
            {
                fullViewReplicas.Add(fullView.GetReplicaInfo(i));
            }

            List<ReplicaInfo> newReplicas = new List<ReplicaInfo>();
            for (int i = 1; i <= fullView.TailIndex; i++)
            {
                newReplicas.Add(fullView.GetReplicaInfo(i));
            }
            configurationService.UpdateConfiguration(newReplicas, 0);
            Assert.IsTrue(configurationService.IsViewStable());

            SampleRTableEntity newCustomer = new SampleRTableEntity("firstname1", "lastname1", "email1@company.com");

            TableOperation operation = TableOperation.Insert(newCustomer);
            TableResult result = repTable.Execute(operation);
            Assert.AreNotEqual(null, result, "result = null");
            ReplicatedTableEntity row = (ReplicatedTableEntity)result.Result;

            Assert.AreEqual((int) HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, result.Result, "result.Result = null");
            Assert.AreEqual("1", result.Etag, "result.Etag mismatch");
            Assert.AreEqual(false, row._rtable_RowLock, "row._rtable_RowLock mismatch");
            Assert.AreEqual(1, row._rtable_Version, "row._rtable_Version mismatch");
            Assert.AreEqual(false, row._rtable_Tombstone, "row._rtable_Tombstone mismatch");
            Assert.AreEqual(configurationService.GetWriteView().ViewId, row._rtable_ViewId, "row._rtable_ViewId mismatch");

            ReadFromIndividualAccountsDirectly(newCustomer.PartitionKey, newCustomer.RowKey, true);

            //Add replica at head
            configurationService.UpdateConfiguration(fullViewReplicas, 1);

            // repair row on the new head
            Console.WriteLine("Calling TableOperation.Replace(newCustomer)...");
            result = repTable.RepairRow(row.PartitionKey, row.RowKey, null);
            Assert.AreNotEqual(null, result, "result = null");

            // Retrieve Entity
            Console.WriteLine("Calling TableOperation.Retrieve<SampleRtableEntity>(firstName, lastName)...");
            operation = TableOperation.Retrieve<SampleRTableEntity>("firstname1", "lastname1");
            TableResult retrievedResult = repTable.Execute(operation);
            Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");

            Assert.AreEqual((int) HttpStatusCode.OK, retrievedResult.HttpStatusCode,
                "retrievedResult.HttpStatusCode mismatch");

            ReadFromIndividualAccountsDirectly(newCustomer.PartitionKey, newCustomer.RowKey, true);
        }

        [Test(Description = "TableOperation Repair Row API")]
        public void RTableRepairRowDelete()
        {
            // Insert entity
            Assert.IsTrue(this.repTable.Exists(), "RTable does not exist");

            View fullView = configurationService.GetWriteView();
            List<ReplicaInfo> fullViewReplicas = new List<ReplicaInfo>();
            for (int i = 0; i <= fullView.TailIndex; i++)
            {
                fullViewReplicas.Add(fullView.GetReplicaInfo(i));
            }

            List<ReplicaInfo> newReplicas = new List<ReplicaInfo>();
            for (int i = 1; i <= fullView.TailIndex; i++)
            {
                newReplicas.Add(fullView.GetReplicaInfo(i));
            }

            SampleRTableEntity newCustomer = new SampleRTableEntity("firstName1", "lastName1", "email1@company.com");

            TableOperation operation = TableOperation.Insert(newCustomer);
            TableResult result = repTable.Execute(operation);
            Assert.AreNotEqual(null, result, "result = null");
            ReplicatedTableEntity row = (ReplicatedTableEntity)result.Result;

            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, result.Result, "result.Result = null");
            Assert.AreEqual("1", result.Etag, "result.Etag mismatch");
            Assert.AreEqual(false, row._rtable_RowLock, "row._rtable_RowLock mismatch");
            Assert.AreEqual(1, row._rtable_Version, "row._rtable_Version mismatch");
            Assert.AreEqual(false, row._rtable_Tombstone, "row._rtable_Tombstone mismatch");
            Assert.AreEqual(configurationService.GetWriteView().ViewId, row._rtable_ViewId, "row._rtable_ViewId mismatch");

            ReadFromIndividualAccountsDirectly(newCustomer.PartitionKey, newCustomer.RowKey, true);

            // remove replica from the head
            configurationService.UpdateConfiguration(newReplicas, 0);
            Assert.IsTrue(configurationService.IsViewStable());

            // delete row
            TableOperation deleteOperation = TableOperation.Delete(newCustomer);
            result = repTable.Execute(deleteOperation);
            Assert.AreNotEqual(null, result, "result = null");
            Assert.AreEqual((int) HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");

            //Add replica at head
            configurationService.UpdateConfiguration(fullViewReplicas, 1);

            // repair row on the new head
            Console.WriteLine("Calling repair row");
            result = repTable.RepairRow(row.PartitionKey, row.RowKey, null);
            Assert.AreNotEqual(null, result, "result = null");

            // Retrieve Entity
            Console.WriteLine("Calling TableOperation.Retrieve<SampleRtableEntity>(firstName, lastName)...");
            operation = TableOperation.Retrieve<SampleRTableEntity>("firstname1", "lastName1");
            TableResult retrievedResult = repTable.Execute(operation);
            Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");

            Assert.AreEqual((int)HttpStatusCode.NotFound, retrievedResult.HttpStatusCode,
                "retrievedResult.HttpStatusCode mismatch");

            ReadFromIndividualAccountsDirectly(newCustomer.PartitionKey, newCustomer.RowKey, true);
        }

        [Test(Description = "TableOperation Repair Row API")]
        public void RTableRepairTable()
        {
            // Insert entity
            Assert.IsTrue(this.repTable.Exists(), "RTable does not exist");

            View fullView = configurationService.GetWriteView();
            List<ReplicaInfo> fullViewReplicas = new List<ReplicaInfo>();
            for (int i = 0; i <= fullView.TailIndex; i++)
            {
                fullViewReplicas.Add(fullView.GetReplicaInfo(i));
            }

            List<ReplicaInfo> newReplicas = new List<ReplicaInfo>();
            for (int i = 1; i <= fullView.TailIndex; i++)
            {
                newReplicas.Add(fullView.GetReplicaInfo(i));
            }

            SampleRTableEntity customer1 = new SampleRTableEntity("firstName1", "lastName1", "email1@company.com");

            TableOperation operation = TableOperation.Insert(customer1);
            TableResult result = repTable.Execute(operation);
            Assert.AreNotEqual(null, result, "result = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");

            SampleRTableEntity customer2 = new SampleRTableEntity("firstName2", "lastName2", "email2@company.com");

            operation = TableOperation.Insert(customer2);
            result = repTable.Execute(operation);
            Assert.AreNotEqual(null, result, "result = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            customer2 = (SampleRTableEntity) result.Result;

            SampleRTableEntity customer3 = new SampleRTableEntity("firstName3", "lastName3", "email3@company.com");

            operation = TableOperation.Insert(customer3);
            result = repTable.Execute(operation);
            Assert.AreNotEqual(null, result, "result = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");

            ReadFromIndividualAccountsDirectly(customer1.PartitionKey, customer1.RowKey, true);
            ReadFromIndividualAccountsDirectly(customer2.PartitionKey, customer2.RowKey, true);
            ReadFromIndividualAccountsDirectly(customer3.PartitionKey, customer3.RowKey, true);

            // remove replica from the head
            configurationService.UpdateConfiguration(newReplicas, 0);
            Assert.IsTrue(configurationService.IsViewStable());

            // delete a row
            TableOperation deleteOperation = TableOperation.Delete(customer1);
            result = repTable.Execute(deleteOperation);
            Assert.AreNotEqual(null, result, "result = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");

            // add a row
            SampleRTableEntity customer4 = new SampleRTableEntity("firstName4", "lastName4", "email4@company.com");

            operation = TableOperation.Insert(customer4);
            result = repTable.Execute(operation);
            Assert.AreNotEqual(null, result, "result = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");

            // replace a row
            customer2.Message = "updated after view update";
            operation = TableOperation.Replace(customer2);
            result = repTable.Execute(operation);
            Assert.AreNotEqual(null, result, "result = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");

            //Add replica at head
            configurationService.UpdateConfiguration(fullViewReplicas, 1);

            // repair table on the new head
            Console.WriteLine("Calling repair table");
            ReconfigurationStatus status = repTable.RepairTable(0, null);
            Assert.AreEqual(ReconfigurationStatus.SUCCESS, status, "RepairTable status is not success: {0}", status);

            ReadFromIndividualAccountsDirectly(customer1.PartitionKey, customer1.RowKey, true);
            ReadFromIndividualAccountsDirectly(customer2.PartitionKey, customer2.RowKey, true);
            ReadFromIndividualAccountsDirectly(customer3.PartitionKey, customer3.RowKey, true);
            ReadFromIndividualAccountsDirectly(customer4.PartitionKey, customer4.RowKey, true);
        }

        #endregion RTable Configuration service test methods
    }
}
