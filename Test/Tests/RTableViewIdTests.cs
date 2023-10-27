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
    using global::Azure;
    using global::Azure.Data.Tables;
    using Microsoft.Azure.Toolkit.Replication;
    using NUnit.Framework;
    using System;
    using System.Collections.Generic;
    using System.Net;

    /// <summary>
    /// Testing the viewId field of the config blob. Create some entries using a viewId. 
    /// Then, try to call some API with a new viewId, which may be smaller or larger than the original viewId.
    /// </summary>
    [TestFixture]
    [Parallelizable(ParallelScope.None)]
    public class RTableViewIdTests : HttpManglerTestBase
    {
        private string tableName;
        List<ReplicaInfo> replicas = new List<ReplicaInfo>();

        [OneTimeSetUp]
        public void TestFixtureSetup()
        {            
            this.LoadTestConfiguration();
            this.tableName = this.GenerateRandomTableName();
            Console.WriteLine("tableName = {0}", this.tableName); 
            this.SetupRTableEnv(this.tableName);

            for (int i = 0; i < configurationWrapper.GetWriteView().Chain.Count; i++)
            {
                replicas.Add(configurationWrapper.GetWriteView().Chain[i].Item1);
            }
        }

        [OneTimeTearDown]
        public void TestFixtureTearDown()
        {
            base.DeleteAllRtableResources();
        }

        [Test(Description="Got exception when library has a smaller viewId")]
        public void ExceptionWhenUsingSmallerViewId()
        {
            long currentViewId = 100;
            long badViewId = currentViewId - 1;

            this.UpdateConfiguration(replicas, 0, false, currentViewId);

            string firstName = "FirstName01";
            string lastName = "LastName01";
            string email = "email01@company.com";
            string phone = "1-800-123-0001";

            // Insert entity
            CustomerEntity newCustomer = new CustomerEntity(firstName, lastName);
            newCustomer.Email = email;
            newCustomer.PhoneNumber = phone;

            TableResult result = this.repTable.Insert(newCustomer);
            Assert.AreNotEqual(null, result, "result = null");
            ReplicatedTableEntity row = (ReplicatedTableEntity)result.Result;
            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, result.Result, "result.Result = null");
            Assert.AreEqual("1", result.Etag, "result.Etag mismatch");
            Assert.AreEqual(false, row._rtable_RowLock, "row._rtable_RowLock mismatch");
            Assert.AreEqual(1, row._rtable_Version, "row._rtable_Version mismatch");
            Assert.AreEqual(false, row._rtable_Tombstone, "row._rtable_Tombstone mismatch");
            Assert.AreEqual(currentViewId, row._rtable_ViewId, "row._rtable_ViewId mismatch");
            Console.WriteLine("Successfully created an entity");

            // Retrieve entity
            TableResult retrievedResult = this.repTable.Retrieve(firstName, lastName);
            Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");
            CustomerEntity customer = (CustomerEntity)retrievedResult.Result;

            Assert.AreEqual((int)HttpStatusCode.OK, retrievedResult.HttpStatusCode, "retrievedResult.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, retrievedResult.Result, "retrievedResult.Result = null");
            Assert.AreEqual("1", retrievedResult.Etag, "retrievedResult.Etag mismatch");
            Assert.AreEqual(false, customer._rtable_RowLock, "customer._rtable_RowLock mismatch");
            Assert.AreEqual(1, customer._rtable_Version, "customer._rtable_Version mismatch");
            Assert.AreEqual(false, customer._rtable_Tombstone, "customer._rtable_Tombstone mismatch");
            Assert.AreEqual(currentViewId, customer._rtable_ViewId, "customer._rtable_ViewId mismatch");
            Assert.AreEqual(newCustomer.PhoneNumber, customer.PhoneNumber, "customer.PhoneNumber mismatch");
            Assert.AreEqual(newCustomer.Email, customer.Email, "customer.Email mismatch");
            Console.WriteLine("Successfully retrieved the entity");

            //
            // Call RefreshRTableEnvJsonConfigBlob to change the viewId of the wrapper to an older value
            //
            Console.WriteLine("Changing the viewId to badViewId {0}", badViewId);
            this.UpdateConfiguration(replicas, 0, false, badViewId);

            //
            // Retrieve with bad viewId
            //
            Console.WriteLine("\nCalling Retrieve with badViewId...");
            try
            {
                retrievedResult = this.repTable.Retrieve(firstName, lastName);                   
                Assert.Fail("Retrieve() is expected to get an RTableStaleViewException but did not get it.");
            }
            catch (ReplicatedTableStaleViewException ex)
            {
                Console.WriteLine("Get this RTableStaleViewException: {0}", ex.Message);
                Assert.IsTrue(ex.ErrorCode == ReplicatedTableViewErrorCodes.ViewIdSmallerThanEntryViewId);
                Assert.IsTrue(ex.Message.Contains(string.Format("current _rtable_ViewId {0} is smaller than", badViewId)), "Got unexpected exception message");
            }

            //
            // Replace with bad viewId
            //
            Console.WriteLine("\nCalling Replace with badViewId...");
            try
            {
                retrievedResult = this.repTable.Replace(customer);
                Assert.Fail("Replace() is expected to get an RTableStaleViewException but did not get it.");
            }
            catch (ReplicatedTableStaleViewException ex)
            {
                Console.WriteLine("Get this RTableStaleViewException: {0}", ex.Message);
                Assert.IsTrue(ex.ErrorCode == ReplicatedTableViewErrorCodes.ViewIdSmallerThanEntryViewId);
                Assert.IsTrue(ex.Message.Contains(string.Format("current _rtable_ViewId {0} is smaller than", badViewId)), "Got unexpected exception message");
            }

            //
            // InsertOrMerge with bad viewId
            //
            Console.WriteLine("\nCalling InsertOrMerge with badViewId...");
            try
            {
                retrievedResult = this.repTable.InsertOrMerge(customer);
                Assert.Fail("InsertOrMerge() is expected to get an RTableStaleViewException but did not get it.");
            }
            catch (ReplicatedTableStaleViewException ex)
            {
                Console.WriteLine("Get this RTableStaleViewException: {0}", ex.Message);
                Assert.IsTrue(ex.ErrorCode == ReplicatedTableViewErrorCodes.ViewIdSmallerThanEntryViewId);
                Assert.IsTrue(ex.Message.Contains(string.Format("current _rtable_ViewId {0} is smaller than", badViewId)), "Got unexpected exception message");
            }

            //
            // InsertOrReplace with bad viewId
            //
            Console.WriteLine("\nCalling InsertOrReplace with badViewId...");
            try
            {
                retrievedResult = this.repTable.InsertOrReplace(customer);
                Assert.Fail("InsertOrReplace() is expected to get an RTableStaleViewException but did not get it.");
            }
            catch (ReplicatedTableStaleViewException ex)
            {
                Console.WriteLine("Get this RTableStaleViewException: {0}", ex.Message);
                Assert.IsTrue(ex.ErrorCode == ReplicatedTableViewErrorCodes.ViewIdSmallerThanEntryViewId);
                Assert.IsTrue(ex.Message.Contains(string.Format("current _rtable_ViewId {0} is smaller than", badViewId)), "Got unexpected exception message");
            }

            //
            // Merge with bad viewId
            //
            Console.WriteLine("\nCalling Merge with badViewId...");
            try
            {
                retrievedResult = this.repTable.Merge(customer);
                Assert.Fail("Merge() is expected to get an RTableStaleViewException but did not get it.");
            }
            catch (ReplicatedTableStaleViewException ex)
            {
                Console.WriteLine("Get this RTableStaleViewException: {0}", ex.Message);
                Assert.IsTrue(ex.ErrorCode == ReplicatedTableViewErrorCodes.ViewIdSmallerThanEntryViewId);
                Assert.IsTrue(ex.Message.Contains(string.Format("current _rtable_ViewId {0} is smaller than", badViewId)), "Got unexpected exception message");
            }

            //
            // Delete with bad viewId
            //
            Console.WriteLine("\nCalling Delete with badViewId...");
            try
            {
                retrievedResult = this.repTable.Delete(customer);
                Assert.Fail("Delete() is expected to get an RTableStaleViewException but did not get it.");
            }
            catch (ReplicatedTableStaleViewException ex)
            {
                Console.WriteLine("Get this RTableStaleViewException: {0}", ex.Message);
                Assert.IsTrue(ex.ErrorCode == ReplicatedTableViewErrorCodes.ViewIdSmallerThanEntryViewId);
                Assert.IsTrue(ex.Message.Contains(string.Format("current _rtable_ViewId {0} is smaller than", badViewId)), "Got unexpected exception message");
            }
        }

        [Test(Description = "Rows with higher wiewId NotFound when IgnoreHigherViewIdRows flag is set")]
        public void RowsWithHigherViewIdNotFoundWhenIgnoreHigherViewIdRowsFlagIsSet()
        {
            long higherViewId = 200;

            // Configure RTable with a higher ViewId
            this.UpdateConfiguration(replicas, 0, false, higherViewId);

            string firstName = "FirstName02";
            string lastName = "LastName02";
            string email = "email01@company.com";
            string phone = "1-800-123-0001";

            // Insert entity
            CustomerEntity newCustomer = new CustomerEntity(firstName, lastName);
            newCustomer.Email = email;
            newCustomer.PhoneNumber = phone;

            TableResult result = this.repTable.Insert(newCustomer);
            Assert.AreNotEqual(null, result, "result = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");

            // Retrieve entity
            TableResult retrievedResult = this.repTable.Retrieve(firstName, lastName);
            Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");
            Assert.AreEqual((int)HttpStatusCode.OK, retrievedResult.HttpStatusCode, "retrievedResult.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, retrievedResult.Result, "retrievedResult.Result = null");
            CustomerEntity customer = (CustomerEntity)retrievedResult.Result;
            Assert.AreEqual(higherViewId, customer._rtable_ViewId, "customer._rtable_ViewId mismatch");
            Assert.AreEqual(newCustomer.PhoneNumber, customer.PhoneNumber, "customer.PhoneNumber mismatch");
            Assert.AreEqual(newCustomer.Email, customer.Email, "customer.Email mismatch");
            Console.WriteLine("Successfully retrieved the entity");

            //
            // Change RTable config to a lower viewId, and force RTable to ignore rows with higher viewId
            //
            long currentViewId = 10;
            Assert.IsTrue(currentViewId < higherViewId, "expected currentViewId < higherViewId !");

            Console.WriteLine("Changing the viewId to currentViewId {0}", currentViewId);
            ReplicatedTableConfiguration config;
            ReplicatedTableQuorumReadResult readStatus = this.configurationService.RetrieveConfiguration(out config);
            Assert.IsTrue(readStatus.Code == ReplicatedTableQuorumReadCode.Success);
            ReplicatedTableConfigurationStore viewConfg = config.GetView(this.configurationWrapper.GetWriteView().Name);
            viewConfg.ViewId = currentViewId;
            config.SetIgnoreHigherViewIdRowsFlag(true);
            this.configurationService.UpdateConfiguration(config);


            //
            // Retrieve with lower viewId
            //
            Console.WriteLine("\nCalling Retrieve with lower ViewId...");
            try
            {
                retrievedResult = this.repTable.Retrieve(firstName, lastName);

                Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");
                Assert.AreEqual((int)HttpStatusCode.NotFound, retrievedResult.HttpStatusCode, "retrievedResult.HttpStatusCode mismatch");
            }
            catch (ReplicatedTableStaleViewException)
            {
                Assert.Fail("Retrieve() is expected to NotFound the row, but got RTableStaleViewException !");
            }

            //
            // Replace with lower viewId
            //
            Console.WriteLine("\nCalling Replace with lower ViewId...");
            try
            {
                retrievedResult = this.repTable.Replace(customer);

                Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");
                Assert.AreEqual((int)HttpStatusCode.NotFound, retrievedResult.HttpStatusCode, "retrievedResult.HttpStatusCode mismatch");
            }
            catch (ReplicatedTableStaleViewException)
            {
                Assert.Fail("Replace() is expected to NotFound the row, but got RTableStaleViewException !");
            }

            //
            // InsertOrMerge with lower viewId
            //
            Console.WriteLine("\nCalling InsertOrMerge with lower ViewId...");
            try
            {
                retrievedResult = this.repTable.InsertOrMerge(customer);

                Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");
                Assert.AreEqual((int)HttpStatusCode.Conflict, retrievedResult.HttpStatusCode, "retrievedResult.HttpStatusCode mismatch");
            }
            catch (ReplicatedTableStaleViewException)
            {
                Assert.Fail("InsertOrMerge() is expected to get Conflict, but got RTableStaleViewException !");
            }

            //
            // InsertOrReplace with lower viewId
            //
            Console.WriteLine("\nCalling InsertOrReplace with lower ViewId...");
            try
            {
                retrievedResult = this.repTable.InsertOrReplace(customer);

                Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");
                Assert.AreEqual((int)HttpStatusCode.Conflict, retrievedResult.HttpStatusCode, "retrievedResult.HttpStatusCode mismatch");
            }
            catch (ReplicatedTableStaleViewException)
            {
                Assert.Fail("InsertOrReplace() is expected to get Conflict, but got RTableStaleViewException !");
            }

            //
            // Merge with lower viewId
            //
            Console.WriteLine("\nCalling Merge with lower ViewId...");
            try
            {
                retrievedResult = this.repTable.Merge(customer);

                Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");
                Assert.AreEqual((int)HttpStatusCode.NotFound, retrievedResult.HttpStatusCode, "retrievedResult.HttpStatusCode mismatch");
            }
            catch (ReplicatedTableStaleViewException)
            {
                Assert.Fail("Merge() is expected to NotFound the row, but got RTableStaleViewException !");
            }

            //
            // Delete with lower viewId
            //
            Console.WriteLine("\nCalling Delete with lower ViewId...");
            try
            {
                retrievedResult = this.repTable.Delete(customer);

                Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");
                Assert.AreEqual((int)HttpStatusCode.NotFound, retrievedResult.HttpStatusCode, "retrievedResult.HttpStatusCode mismatch");
            }
            catch (ReplicatedTableStaleViewException)
            {
                Assert.Fail("Delete() is expected to NotFound the row, but got RTableStaleViewException !");
            }
        }

        [Test(Description="Replace using larger viewId is ok")]
        public void ReplaceUsingLargerViewId()
        {
            long currentViewId = 100;
            long futureViewId = currentViewId + 1;

            this.UpdateConfiguration(replicas, 0, false, currentViewId);

            string firstName = "FirstName02";
            string lastName = "LastName02";
            string email = "email01@company.com";
            string phone = "1-800-123-0001";

            // Insert entity
            CustomerEntity newCustomer = new CustomerEntity(firstName, lastName);
            newCustomer.Email = email;
            newCustomer.PhoneNumber = phone;

            TableResult result = this.repTable.Insert(newCustomer);
            Assert.AreNotEqual(null, result, "result = null");
            ReplicatedTableEntity row = (ReplicatedTableEntity)result.Result;
            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, result.Result, "result.Result = null");
            Assert.AreEqual("1", result.Etag, "result.Etag mismatch");
            Assert.AreEqual(false, row._rtable_RowLock, "row._rtable_RowLock mismatch");
            Assert.AreEqual(1, row._rtable_Version, "row._rtable_Version mismatch");
            Assert.AreEqual(false, row._rtable_Tombstone, "row._rtable_Tombstone mismatch");
            Assert.AreEqual(currentViewId, row._rtable_ViewId, "row._rtable_ViewId mismatch");
            Console.WriteLine("Successfully created an entity");

            // Retrieve entity
            TableResult retrievedResult = this.repTable.Retrieve(firstName, lastName);
            Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");
            CustomerEntity customer = (CustomerEntity)retrievedResult.Result;

            Assert.AreEqual((int)HttpStatusCode.OK, retrievedResult.HttpStatusCode, "retrievedResult.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, retrievedResult.Result, "retrievedResult.Result = null");
            Assert.AreEqual("1", retrievedResult.Etag, "retrievedResult.Etag mismatch");
            Assert.AreEqual(false, customer._rtable_RowLock, "customer._rtable_RowLock mismatch");
            Assert.AreEqual(1, customer._rtable_Version, "customer._rtable_Version mismatch");
            Assert.AreEqual(false, customer._rtable_Tombstone, "customer._rtable_Tombstone mismatch");
            Assert.AreEqual(currentViewId, customer._rtable_ViewId, "customer._rtable_ViewId mismatch");
            Assert.AreEqual(newCustomer.PhoneNumber, customer.PhoneNumber, "customer.PhoneNumber mismatch");
            Assert.AreEqual(newCustomer.Email, customer.Email, "customer.Email mismatch");
            Console.WriteLine("Successfully retrieved the entity");

            //
            // Call ModifyConfigurationBlob to change the viewId of the wrapper to a larger value
            //
            Console.WriteLine("Changing the viewId to futureViewId {0}", futureViewId);
            this.UpdateConfiguration(replicas, 0, false, futureViewId);

            //
            // Replace entity
            //
            Console.WriteLine("\nCalling Replace with larger viewId...");
            email = "email01b@company.com";
            phone = "1-800-123-0002";            
            customer.Email = email;
            customer.PhoneNumber = phone;
            result = repTable.Replace(customer);
            Assert.AreNotEqual(null, result, "result = null");

            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, result.Result, "result.Result = null");

            // Retrieve Entity
            Console.WriteLine("\nCalling Retrieve() with larger viewId...");
            retrievedResult = repTable.Retrieve(firstName, lastName);
            Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");
            CustomerEntity customer2 = (CustomerEntity)retrievedResult.Result;

            Assert.AreEqual((int)HttpStatusCode.OK, retrievedResult.HttpStatusCode, "retrievedResult.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, retrievedResult.Result, "retrievedResult.Result = null");
            Assert.AreEqual("2", retrievedResult.Etag, "retrievedResult.Etag mismatch");
            Assert.AreEqual(false, customer2._rtable_RowLock, "customer2._rtable_RowLock mismatch");
            Assert.AreEqual(2, customer2._rtable_Version, "customer2._rtable_Version mismatch");
            Assert.AreEqual(false, customer2._rtable_Tombstone, "customer2._rtable_Tombstone mismatch");
            Assert.AreEqual(futureViewId, customer2._rtable_ViewId, "customer2._rtable_ViewId mismatch");
            Assert.AreEqual(phone, customer2.PhoneNumber, "customer2.PhoneNumber mismatch");
            Assert.AreEqual(email, customer2.Email, "customer2.Email mismatch");
            Console.WriteLine("Successfully retrieved the entity");

            //
            // Delete entity
            //
            Console.WriteLine("\nCalling Delete with larger viewId...");
            TableResult deleteResult = repTable.Delete(customer);
            Assert.AreNotEqual(null, deleteResult, "deleteResult = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, deleteResult.HttpStatusCode, "deleteResult.HttpStatusCode mismatch");
            Assert.IsNotNull(deleteResult.Result, "deleteResult.Result = null");

            // Retrieve
            Console.WriteLine("Calling TableOperation.Retrieve() after Delete() was called...");
            TableResult retrievedResult2 = repTable.Retrieve(firstName, lastName);
            Assert.AreEqual((int)HttpStatusCode.NotFound, retrievedResult2.HttpStatusCode, "retrievedResult2.HttpStatusCode mismatch");
            Assert.IsNull(retrievedResult2.Result, "retrievedResult2.Result != null");

        }

        [Test(Description="Merge using larger viewId is ok")]
        public void MergeUsingLargerViewId()
        {
            long currentViewId = 100;
            long futureViewId = currentViewId + 1;
            int expectedVersion = 1;

            this.UpdateConfiguration(replicas, 0, false, currentViewId);

            // Insert Entity            
            DynamicReplicatedTableEntity baseEntity = new DynamicReplicatedTableEntity("merge test02", "foo02");
            baseEntity.Properties.Add("prop1", "value1");
            Console.WriteLine("Calling TableOperation.Insert()...");
            TableResult result = this.repTable.Insert(baseEntity);
            Assert.AreNotEqual(null, result, "Insert(): result = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "Insert(): result.HttpStatusCode mismatch");

            //
            // Call ModifyConfigurationBlob to change the viewId of the wrapper to a larger value
            //
            Console.WriteLine("Changing the viewId to futureViewId {0}", futureViewId);
            this.UpdateConfiguration(replicas, 0, false, futureViewId);

            //
            // Merge
            //
            DynamicReplicatedTableEntity mergeEntity = new DynamicReplicatedTableEntity(baseEntity.PartitionKey, baseEntity.RowKey) { ETag = new ETag(result.Etag) };
            mergeEntity.Properties.Add("prop2", "value2");
            Console.WriteLine("\nCalling TableOperation.Merge() with a larger viewId...");
            result = this.repTable.Merge(mergeEntity);
            expectedVersion++;
            Assert.AreNotEqual(null, result, "Merge(): result = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "Merge(): result.HttpStatusCode mismatch");

            //
            // InsertOrMerge
            //
            DynamicReplicatedTableEntity mergeEntity2 = new DynamicReplicatedTableEntity(baseEntity.PartitionKey, baseEntity.RowKey) { ETag = new ETag(result.Etag) };
            mergeEntity2.Properties.Add("prop3", "value3");
            Console.WriteLine("\nCalling TableOperation.InsertOrMerge() with a larger viewId...");
            result = this.repTable.InsertOrMerge(mergeEntity2);
            expectedVersion++;
            Assert.AreNotEqual(null, result, "InsertOrMerge(): result = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "InsertOrMerge(): result.HttpStatusCode mismatch");

            // Retrieve Entity & Verify Contents
            Console.WriteLine("\nCalling TableOperation.Retrieve() with a larger viewId...");
            result = this.repTable.Retrieve(baseEntity.PartitionKey, baseEntity.RowKey);
            Assert.AreNotEqual(null, result, "Retrieve(): result = null");
            DynamicReplicatedTableEntity retrievedEntity = result.Result as DynamicReplicatedTableEntity;

            Assert.IsNotNull(retrievedEntity, "retrievedEntity = null");
            Assert.AreEqual(3, retrievedEntity.Properties.Count, "Properties.Count mismatch");
            Assert.AreEqual(baseEntity.Properties["prop1"], retrievedEntity.Properties["prop1"], "Properties[prop1] mismatch");
            Assert.AreEqual(mergeEntity.Properties["prop2"], retrievedEntity.Properties["prop2"], "Properties[prop2] mismatch");
            Assert.AreEqual(mergeEntity2.Properties["prop3"], retrievedEntity.Properties["prop3"], "Properties[prop3] mismatch");
            Assert.AreEqual(futureViewId, retrievedEntity._rtable_ViewId, "retrievedEntity._rtable_ViewId mismatch");
            Assert.AreEqual(expectedVersion, retrievedEntity._rtable_Version, "retrievedEntity._rtable_Version mismatch");
        }

        [Test(Description = "Batch _rtable_Operation: Got exception when library has a smaller viewId")]
        public void BatchOperationExceptionWhenUsingSmallerViewId()
        {
            long currentViewId = 100;
            long badViewId = currentViewId - 1;

            this.UpdateConfiguration(replicas, 0, false, currentViewId);

            string jobType = "jobType-BatchOperationExceptionWhenUsingSmallerViewId";
            string jobId = "jobId-BatchOperationExceptionWhenUsingSmallerViewId";
            int count = 3; // number of operations in the batch _rtable_Operation

            List<TableOperationType> opTypes = new List<TableOperationType>()
            {
                TableOperationType.Replace,
                TableOperationType.InsertOrReplace,
                TableOperationType.Delete,
            };

            //
            // Insert
            //
            string jobIdTemplate = jobId + "-{0}";
            string messageTemplate = "message-{0}";
            string updatedMessageTemplate = "updated-" + messageTemplate;

            string partitionKey = string.Empty;
            //
            // Insert entities
            //
            for (int i = 0; i < count; i++)
            {
                SampleRTableEntity originalEntity = new SampleRTableEntity(
                    jobType,
                    string.Format(jobIdTemplate, i),
                    string.Format(messageTemplate, i));

                this.repTable.Insert(originalEntity);
                partitionKey = originalEntity.PartitionKey;
            }

            //
            // Retrieve entities and use them to create batchOperation to Replace or Delete
            //
            IEnumerable<SampleRTableEntity> allEntities = this.rtableWrapper.GetAllRows(partitionKey);
            IList<TableTransactionAction> batchOperation = new List<TableTransactionAction>();
            int m = 0;
            foreach (SampleRTableEntity entity in allEntities)
            {
                Console.WriteLine("{0}", entity.ToString());
                Console.WriteLine("---------------------------------------");
                if (opTypes[m] == TableOperationType.Replace)
                {
                    SampleRTableEntity replaceEntity = new SampleRTableEntity(
                        entity.JobType,
                        entity.JobId,
                        string.Format(updatedMessageTemplate, m))
                    {
                        ETag = entity.ETag
                    };
                    batchOperation.Add(new TableTransactionAction(TableTransactionActionType.UpdateReplace, replaceEntity, replaceEntity.ETag));

                }
                else if (opTypes[m] == TableOperationType.InsertOrReplace)
                {
                    SampleRTableEntity replaceEntity = new SampleRTableEntity(
                        entity.JobType,
                        entity.JobId,
                        string.Format(updatedMessageTemplate, m))
                    {
                        ETag = entity.ETag
                    };
                    batchOperation.Add(new TableTransactionAction(TableTransactionActionType.UpsertReplace, replaceEntity, replaceEntity.ETag));
                }
                else if (opTypes[m] == TableOperationType.Delete)
                {
                    batchOperation.Add(new TableTransactionAction(TableTransactionActionType.Delete, entity, entity.ETag));
                }
                else
                {
                    throw new ArgumentException(
                        string.Format("opType={0} is NOT supported", opTypes[m]),
                        "opType");
                }
                m++;
            }

            //
            // Call ModifyConfigurationBlob to change the viewId of the wrapper to an older value
            //
            Console.WriteLine("Changing the viewId to badViewId {0}", badViewId);
            this.UpdateConfiguration(replicas, 0, false, badViewId);

            //
            // Execute Batch _rtable_Operation with bad viewId
            //
            Console.WriteLine("\nCalling BatchOperation with badViewId...");
            try
            {
                this.repTable.ExecuteBatch(batchOperation);
            }
            catch (ReplicatedTableStaleViewException ex)
            {
                Console.WriteLine("Get this RTableStaleViewException: {0}", ex.Message);
                Assert.IsTrue(ex.ErrorCode == ReplicatedTableViewErrorCodes.ViewIdSmallerThanEntryViewId);
                Assert.IsTrue(ex.Message.Contains(string.Format("current _rtable_ViewId {0} is smaller than", badViewId)), "Got unexpected exception message");
            }
        }

        [Test(Description = "Batch _rtable_Operation: using larger viewId is ok")]
        public void BatchOperationUsingLargerViewId()
        {
            long currentViewId = 100;
            long futureViewId = currentViewId + 1;

            this.UpdateConfiguration(replicas, 0, false, currentViewId);

            string jobType = "jobType-BatchOperationUsingLargerViewId";
            string jobId = "jobId-BatchOperationUsingLargerViewId";
            int count = 2; // number of operations in the batch _rtable_Operation

            List<TableOperationType> opTypes = new List<TableOperationType>()
            {
                TableOperationType.Replace,
                TableOperationType.Delete,
            };

            //
            // Insert
            //
            string jobIdTemplate = jobId + "-{0}";
            string messageTemplate = "message-{0}";
            string updatedMessageTemplate = "updated-" + messageTemplate;

            string partitionKey = string.Empty;
            //
            // Insert entities
            //
            for (int i = 0; i < count; i++)
            {
                SampleRTableEntity originalEntity = new SampleRTableEntity(
                    jobType,
                    string.Format(jobIdTemplate, i),
                    string.Format(messageTemplate, i));

                this.repTable.Insert(originalEntity);
                partitionKey = originalEntity.PartitionKey;
            }

            //
            // Retrieve entities and use them to create batchOperation to Replace or Delete
            //
            IEnumerable<SampleRTableEntity> allEntities = this.rtableWrapper.GetAllRows(partitionKey);
            IList<TableTransactionAction> batchOperation = new List<TableTransactionAction>();
            int m = 0;
            foreach (SampleRTableEntity entity in allEntities)
            {
                Console.WriteLine("{0}", entity.ToString());
                Console.WriteLine("---------------------------------------");
                if (opTypes[m] == TableOperationType.Replace)
                {
                    SampleRTableEntity replaceEntity = new SampleRTableEntity(
                        entity.JobType,
                        entity.JobId,
                        string.Format(updatedMessageTemplate, m))
                    {
                        ETag = entity.ETag
                    };
                    batchOperation.Add(new TableTransactionAction(TableTransactionActionType.UpdateReplace, replaceEntity, replaceEntity.ETag));
                }
                else if (opTypes[m] == TableOperationType.InsertOrReplace)
                {
                    SampleRTableEntity replaceEntity = new SampleRTableEntity(
                        entity.JobType,
                        entity.JobId,
                        string.Format(updatedMessageTemplate, m))
                    {
                        ETag = entity.ETag
                    };
                    batchOperation.Add(new TableTransactionAction(TableTransactionActionType.UpsertReplace, replaceEntity, replaceEntity.ETag));
                }
                else if (opTypes[m] == TableOperationType.Delete)
                {
                    batchOperation.Add(new TableTransactionAction(TableTransactionActionType.Delete, entity, entity.ETag));
                }
                else
                {
                    throw new ArgumentException(
                        string.Format("opType={0} is NOT supported", opTypes[m]),
                        "opType");
                }
                m++;
            }

            //
            // Call ModifyConfigurationBlob to change the viewId of the wrapper to an older value
            //
            Console.WriteLine("Changing the viewId to larger viewId {0}", futureViewId);
            this.UpdateConfiguration(replicas, 0, false, futureViewId);

            Console.WriteLine("\nCalling BatchOperation with a larger viewId...");
            this.repTable.ExecuteBatch(batchOperation);

            this.ExecuteBatchOperationAndValidate(
                count,
                partitionKey,
                jobType,
                jobId,
                opTypes);
        }
    }
}
