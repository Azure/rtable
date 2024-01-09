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
    using System;
    using System.Collections.Generic;
    using Microsoft.Azure.Toolkit.Replication;
    using NUnit.Framework;
    using System.Net;
    using global::Azure;
    using global::Azure.Data.Tables;

    [TestFixture]
    public class RTableCRUDUnitTests : RTableWrapperTestBase
    {
        [OneTimeSetUp]
        public void TestFixtureSetup()
        {
            this.LoadTestConfiguration();
            string tableName = this.GenerateRandomTableName();
            Console.WriteLine("tableName = {0}", tableName);
            this.SetupRTableEnv(tableName, true, "", new List<int> { 0 });
        }

        [OneTimeTearDown]
        public void TestFixtureTearDown()
        {
            base.DeleteAllRtableResources();
        }

        //
        // binaries.amd64fre\Gateway\UnitTests>
        // runuts /run=Microsoft.WindowsAzure.Storage.RTableTest.RTableCRUDUnitTests.RTableCreateTableSync
        // runuts /fixture=Microsoft.WindowsAzure.Storage.RTableTest.RTableCRUDUnitTests
        //       

        #region XStore Test
        /// <summary>
        /// This test does not use RTable. It only uses XStore APIs. 
        /// The purpose is to get a reference of all the HttpStatus codes, etc.
        /// </summary>
        [Test(Description = "XStore tests to execute Insert, Replace, Update, Delete operations")]
        public void XStoreCRUDTest()
        {
            TableServiceClient tableClient = this.cloudTableClients[0];
            TableClient table = tableClient.GetTableClient(this.repTable.TableName);

            string firstName = "FirstNameXStore";
            string lastName = "LastNameXStore";
            string email = "xstore1@company.com";
            string phone = "1-800-123-0001";
            CustomerEntity newCustomer = new CustomerEntity(firstName, lastName);
            newCustomer.Email = email;
            newCustomer.PhoneNumber = phone;

            // XStore Insert
            Console.WriteLine("Calling XStore Insert...");
            var insertResult = table.AddEntity(newCustomer);

            Assert.IsNotNull(insertResult, "insertResult = null");
            Console.WriteLine("insertResult.HttpStatusCode = {0}", insertResult.Status);
            Console.WriteLine("insertResult.ETag = {0}", insertResult.Headers.ETag);
            Assert.AreEqual((int)HttpStatusCode.NoContent, insertResult.Status, "insertResult.HttpStatusCode mismatch");
            Assert.IsFalse(string.IsNullOrEmpty(insertResult.Headers.ETag.ToString()), "insertResult.ETag = null or empty");
            
            // Retrieve
            Console.WriteLine("Calling XStore Retrieve...");
            var retrieveResult = table.GetEntity<CustomerEntity>(newCustomer.PartitionKey, newCustomer.RowKey);
            Assert.IsNotNull(retrieveResult, "retrieveResult = null");
            Console.WriteLine("retrieveResult.HttpStatusCode = {0}", retrieveResult?.GetRawResponse().Status);
            Assert.AreEqual((int)HttpStatusCode.OK, retrieveResult?.GetRawResponse().Status, "retrieveResult.HttpStatusCode mismatch");
            var customer = retrieveResult.Value;
            Assert.IsNotNull(customer, "Retrieve: customer = null");
            Console.WriteLine("Retrieve: customer.Email = {0}", customer.Email);
            Console.WriteLine("Retrieve: customer.PhoneNumber = {0}", customer.PhoneNumber);
            Assert.AreEqual(newCustomer.Email, customer.Email, "Retrieve: customer.Email mismatch");
            Assert.AreEqual(newCustomer.PhoneNumber, customer.PhoneNumber, "Retrieve: customer.PhoneNumber mismatch");

            // Replace entity
            Console.WriteLine("Calling XStore Replace...");
            string email2 = "xstore2@company.com";
            string phone2 = "1-800-123-0002";
            customer.PhoneNumber = phone2;
            customer.Email = email2;
            var updateResult = table.UpdateEntity(customer, customer.ETag);
            Assert.IsNotNull(updateResult, "updateResult = null");
            Console.WriteLine("updateResult.HttpStatusCode = {0}", updateResult.Status);
            Assert.AreEqual((int)HttpStatusCode.NoContent, updateResult.Status, "updateResult.HttpStatusCode mismatch");

            // Retrieve after Replace
            Console.WriteLine("Calling XStore Retrieve after Replace was called...");
            retrieveResult = table.GetEntity<CustomerEntity>(customer.PartitionKey, customer.RowKey);
            Assert.IsNotNull(retrieveResult, "RetrieveAfterReplace: retrieveResult = null");
            Console.WriteLine("RetrieveAfterReplace: retrieveResult.HttpStatusCode = {0}", retrieveResult?.GetRawResponse().Status);
            Assert.AreEqual((int)HttpStatusCode.OK, retrieveResult?.GetRawResponse().Status, "RetrieveAfterReplace: retrieveResult.HttpStatusCode mismatch");
            CustomerEntity customer2 = retrieveResult.Value;
            Assert.IsNotNull(customer2, "RetrieveAfterReplace: customer2 = null");
            Console.WriteLine("RetrieveAfterReplace: customer2.Email = {0}", customer2.Email);
            Console.WriteLine("RetrieveAfterReplace: customer2.PhoneNumber = {0}", customer2.PhoneNumber);
            Assert.AreEqual(email2, customer2.Email, "RetrieveAfterReplace: customer2.Email mismatch");
            Assert.AreEqual(phone2, customer2.PhoneNumber, "RetrieveAfterReplace: customer2.PhoneNumber mismatch");
            
            // Delete
            Console.WriteLine("Calling XStore Delete...");
            var deleteResult = table.DeleteEntity(customer2.PartitionKey, customer2.RowKey, customer2.ETag);
            Assert.IsNotNull(deleteResult, "deleteResult = null");
            Console.WriteLine("deleteResult.HttpStatusCode = {0}", deleteResult.Status);
            Assert.AreEqual((int)HttpStatusCode.NoContent, deleteResult.Status, "deleteResult.HttpStatusCode mismatch");

            // Retrieve after Delete
            Console.WriteLine("Calling XStore Retrieve after Delete was called...");
            try
            {
                retrieveResult = table.GetEntity<CustomerEntity>(customer.PartitionKey, customer.RowKey);
                Assert.Fail("RetrieveAfterDelete should throw RFE after delete.");
            }
            catch (RequestFailedException rfe)
            {
                Console.WriteLine("RetrieveAfterDelete: retrieveResult.HttpStatusCode = {0}", rfe.Status);
                Assert.AreEqual((int)HttpStatusCode.NotFound, rfe.Status, "RetrieveAfterDelete: retrieveResult.HttpStatusCode mismatch");
            }
            catch (Exception)
            {
                Assert.Fail("RetrieveAfterDelete should throw RFE after delete.");
            }

            // Replace after Delete
            Console.WriteLine("Calling XStore Replace after Delete...");
            try
            {
                updateResult = table.UpdateEntity(customer2, customer2.ETag, TableUpdateMode.Replace);
                Assert.Fail("ReplaceAfterDelete should throw.");
            }
            catch (RequestFailedException rfe)
            {
                Console.WriteLine("RetrieveAfterDelete: retrieveResult.HttpStatusCode = {0}", rfe.Status);
                Assert.AreEqual((int)HttpStatusCode.NotFound, rfe.Status, "RetrieveAfterDelete: retrieveResult.HttpStatusCode mismatch");
            }
            catch (Exception)
            {
                Assert.Fail("ReplaceAfterDelete should throw RFE after delete.");
            }
        }
        #endregion

        #region Table Operations Test Methods
        /// <summary>
        /// This test uses rtableWrapper to perform CRUD operation.
        /// The same set of operations are used in ConvertXStoreTableToRTableTests.cs
        /// </summary>
        [Test(Description = "CRUD test using rtableWrapper")]
        public void RTableWrapperCRUDTest()
        {
            string jobType = "jobType-RTableWrapperCRUDTest";
            string jobId = "jobId-RTableWrapperCRUDTest";

            this.PerformInsertOperationAndValidate(jobType, jobId, this.message);
            this.PerformRetrieveOperationAndValidate(jobType, jobId, this.message);
            this.PerformOperationAndValidate(TableOperationType.Replace, jobType, jobId, this.updatedMessage);
            this.PerformOperationAndValidate(TableOperationType.Merge, jobType, jobId, this.updatedAgainMessage);
            this.PerformOperationAndValidate(TableOperationType.InsertOrReplace, jobType, jobId, this.updatedMessage);
            this.PerformOperationAndValidate(TableOperationType.Delete, jobType, jobId);
            this.PerformInsertOperationAndValidate(jobType, jobId, this.message);
        }


        [Test(Description = "TableOperation InsertOrReplace API")]
        public void RTableInsertOrReplaceSync()
        {
            string firstName = "FirstName01";
            string lastName = "LastName01";
            string email = "email01@company.com";
            string phone = "1-800-123-0001";

            string dynamicFirstName = "DynFirstName01";
            string dynamicLastName = "DynLastName01";
            string dynamicEmail = "dynemail01@company.com";
            string dynamicPhone = "1-800-123-0002";


            // Insert entity
            CustomerEntity newCustomer = new CustomerEntity(firstName, lastName);
            newCustomer.Email = email;
            newCustomer.PhoneNumber = phone;

            TableResult result = repTable.Insert(newCustomer);
            Assert.AreNotEqual(null, result, "result = null");
            ReplicatedTableEntity row = new ReplicatedTableEntity((TableEntity)result.Result);
            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, result.Result, "result.Result = null");
            Assert.AreEqual("1", result.Etag, "result.Etag mismatch");
            Assert.AreEqual(false, row._rtable_RowLock, "row._rtable_RowLock mismatch");
            Assert.AreEqual(1, row._rtable_Version, "row._rtable_Version mismatch");
            Assert.AreEqual(false, row._rtable_Tombstone, "row._rtable_Tombstone mismatch");
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, row._rtable_ViewId, "row._rtable_ViewId mismatch");

            //Non-generic retrieve 
            TableResult retrievedResult = this.repTable.Retrieve(firstName, lastName);
            Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");
            Assert.IsTrue(retrievedResult.Result is IReplicatedTableEntity, "Expected entity to be of type IReplicatedTableEntity");
            DynamicReplicatedTableEntity readRow = retrievedResult.Result as DynamicReplicatedTableEntity;
            Assert.IsTrue(readRow.ETag == new ETag("1"), "Returned etag is not zero");
            Assert.IsTrue(readRow.Properties.ContainsKey("Email"), "DynamicRTableEntity returned didnt contain Email");
            Assert.IsTrue(readRow.Properties.ContainsKey("PhoneNumber"), "DynamicRTableEntity returned didnt contain PhoneNumber");
            Assert.IsTrue(readRow.Properties.Count == 6, "DynamicRTableEntity returned contained diff number of properties");
                       
            ////Dynamic insert entity
            DynamicReplicatedTableEntity newDynamicCustomer = new DynamicReplicatedTableEntity(dynamicFirstName, dynamicLastName);
            newDynamicCustomer.Properties["Email"] = dynamicEmail;
            newDynamicCustomer.Properties["PhoneNumber"] = dynamicPhone;

            result = repTable.Insert(newDynamicCustomer, null);
            Assert.AreNotEqual(null, result, "result = null");
            row = new DynamicReplicatedTableEntity((TableEntity)result.Result);
            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, result.Result, "result.Result = null");
            Assert.AreEqual("1", result.Etag, "result.Etag mismatch");
            Assert.AreEqual(false, row._rtable_RowLock, "row._rtable_RowLock mismatch");
            Assert.AreEqual(1, row._rtable_Version, "row._rtable_Version mismatch");
            Assert.AreEqual(false, row._rtable_Tombstone, "row._rtable_Tombstone mismatch");
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, row._rtable_ViewId, "row._rtable_ViewId mismatch");

            //Non-generic retrieve for dynamic entity
            retrievedResult = this.repTable.Retrieve(dynamicFirstName, dynamicLastName);
            Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");
            Assert.IsTrue(retrievedResult.Result is IReplicatedTableEntity, "Expected entity to be of type IReplicatedTableEntity");
            readRow = retrievedResult.Result as DynamicReplicatedTableEntity;
            Assert.IsTrue(readRow.ETag == new ETag("1"), "Returned etag is not zero");
            Assert.IsTrue(readRow.Properties.ContainsKey("Email"), "DynamicRTableEntity returned didnt contain Email");
            Assert.IsTrue(readRow.Properties.ContainsKey("PhoneNumber"), "DynamicRTableEntity returned didnt contain PhoneNumber");
            Assert.IsTrue(readRow.Properties.Count == 6, "DynamicRTableEntity returned contained diff number of properties");
          
            // Retrieve entity
            retrievedResult = this.repTable.Retrieve(firstName, lastName);
            Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null"); 
            CustomerEntity customer = new CustomerEntity((ReplicatedTableEntity)retrievedResult.Result);

            Assert.AreEqual((int)HttpStatusCode.OK, retrievedResult.HttpStatusCode, "retrievedResult.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, retrievedResult.Result, "retrievedResult.Result = null");
            Assert.AreEqual("1", retrievedResult.Etag, "retrievedResult.Etag mismatch");
            Assert.AreEqual(false, customer._rtable_RowLock, "customer._rtable_RowLock mismatch");
            Assert.AreEqual(1, customer._rtable_Version, "customer._rtable_Version mismatch");
            Assert.AreEqual(false, customer._rtable_Tombstone, "customer._rtable_Tombstone mismatch");
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, customer._rtable_ViewId, "customer._rtable_ViewId mismatch");
            Assert.AreEqual(newCustomer.PhoneNumber, customer.PhoneNumber, "customer.PhoneNumber mismatch");
            Assert.AreEqual(newCustomer.Email, customer.Email, "customer.Email mismatch");            

            // Update the entity
            email = "email01b@company.com";
            phone = "1-800-456-0001";
            newCustomer = new CustomerEntity(firstName, lastName);
            newCustomer.Email = email;
            newCustomer.PhoneNumber = phone;

            // InsertOrReplace
            Console.WriteLine("Calling TableOperation.InsertOrReplace(newCustomer)...");
            result = this.repTable.InsertOrReplace(newCustomer);
            Assert.AreNotEqual(result, null);
            row = new ReplicatedTableEntity((TableEntity)result.Result);

            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, result.Result, "result.Result = null");
            Assert.AreEqual("2", result.Etag, "result.Etag mismatch");
            Assert.AreEqual(false, row._rtable_RowLock, "row._rtable_RowLock mismatch");
            Assert.AreEqual(2, row._rtable_Version, "row._rtable_Version mismatch");
            Assert.AreEqual(false, row._rtable_Tombstone, "row._rtable_Tombstone mismatch");
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, row._rtable_ViewId, "row._rtable_ViewId mismatch");

            // Retrieve entity
            Console.WriteLine("Calling Retrieve...");
            retrievedResult = this.repTable.Retrieve(firstName, lastName);
            Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");

            customer = new CustomerEntity((ReplicatedTableEntity)retrievedResult.Result);
            Assert.AreEqual((int)HttpStatusCode.OK, retrievedResult.HttpStatusCode, "retrievedResult.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, retrievedResult.Result, "retrievedResult.Result = null");
            Assert.AreEqual("2", retrievedResult.Etag, "retrievedResult.Etag mismatch");
            Assert.AreEqual(false, customer._rtable_RowLock, "customer._rtable_RowLock mismatch");
            Assert.AreEqual(2, customer._rtable_Version, "customer._rtable_Version mismatch");
            Assert.AreEqual(false, customer._rtable_Tombstone, "customer._rtable_Tombstone mismatch");
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, customer._rtable_ViewId, "customer._rtable_ViewId mismatch");
            Assert.AreEqual(newCustomer.PhoneNumber, customer.PhoneNumber, "customer.PhoneNumber mismatch");
            Assert.AreEqual(newCustomer.Email, customer.Email, "customer.Email mismatch");            
        }

        [Test(Description = "TableOperation Upsert (InsertOrReplace) API")]
        public void RTableUpsert()
        {
            string firstName = "FirstName01_upsert";
            string lastName = "LastName01_upsert";
            string email = "email01@company.com";
            string phone = "1-800-123-0001";

            // Insert entity
            var customer = new CustomerEntity(firstName, lastName)
            {
                Email = email,
                PhoneNumber = phone,
            };

            // Upsert when entry doesn't exist => Insert
            Console.WriteLine("Performing an Upsert when entry doesn't exist i.e. insert (customer)");
            TableResult result = this.repTable.InsertOrReplace(customer);
            Assert.AreNotEqual(result, null);
            ReplicatedTableEntity row = new ReplicatedTableEntity((TableEntity)result.Result);

            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, result.Result, "result.Result = null");
            Assert.AreEqual("1", result.Etag, "result.Etag mismatch");
            Assert.AreEqual(false, row._rtable_RowLock, "row._rtable_RowLock mismatch");
            Assert.AreEqual(1, row._rtable_Version, "row._rtable_Version mismatch");
            Assert.AreEqual(false, row._rtable_Tombstone, "row._rtable_Tombstone mismatch");
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, row._rtable_ViewId, "row._rtable_ViewId mismatch");


            // Upsert on existing entry => Replace
            Console.WriteLine("Performing an Upsert on existing entry i.e. replace (customer)");
            customer.Email = "email02@company.com";
            customer.PhoneNumber = "1-800-123-0002";
            result = this.repTable.InsertOrReplace(customer);
            Assert.AreNotEqual(result, null);
            row = new ReplicatedTableEntity((TableEntity)result.Result);

            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, result.Result, "result.Result = null");
            Assert.AreEqual("2", result.Etag, "result.Etag mismatch");
            Assert.AreEqual(false, row._rtable_RowLock, "row._rtable_RowLock mismatch");
            Assert.AreEqual(2, row._rtable_Version, "row._rtable_Version mismatch");
            Assert.AreEqual(false, row._rtable_Tombstone, "row._rtable_Tombstone mismatch");
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, row._rtable_ViewId, "row._rtable_ViewId mismatch");
        }

        [Test(Description = "TableOperation Insert API")]
        public void RTableInsertRetrieveSync()
        {
            string firstName = "FirstName02";
            string lastName = "LastName02";
            string email = "email02@company.com";
            string phone = "1-800-123-0002";

            // Insert entity
            CustomerEntity newCustomer = new CustomerEntity(firstName, lastName);
            newCustomer.Email = email;
            newCustomer.PhoneNumber = phone;
            TableResult result = repTable.Insert(newCustomer);
            Assert.AreNotEqual(null, result, "result = null");
            ReplicatedTableEntity row = new ReplicatedTableEntity((TableEntity)result.Result);
            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, result.Result, "result.Result = null");
            Assert.AreEqual("1", result.Etag, "result.Etag mismatch");
            Assert.AreEqual(false, row._rtable_RowLock, "row._rtable_RowLock mismatch");
            Assert.AreEqual(1, row._rtable_Version, "row._rtable_Version mismatch");
            Assert.AreEqual(false, row._rtable_Tombstone, "row._rtable_Tombstone mismatch");
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, row._rtable_ViewId, "row._rtable_ViewId mismatch");

            // Retrieve entity
            TableResult retrievedResult = repTable.Retrieve(firstName, lastName);
            Assert.AreNotEqual(retrievedResult, null, "retrievedResult = null");
            CustomerEntity customer = new CustomerEntity((ReplicatedTableEntity)retrievedResult.Result);
            Assert.AreEqual((int)HttpStatusCode.OK, retrievedResult.HttpStatusCode, "retrievedResult.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, retrievedResult.Result, "retrievedResult.Result = null");
            Assert.AreEqual("1", retrievedResult.Etag, "retrievedResult.Etag mismatch");
            Assert.AreEqual(false, customer._rtable_RowLock, "customer._rtable_RowLock mismatch");
            Assert.AreEqual(1, customer._rtable_Version, "customer._rtable_Version mismatch");
            Assert.AreEqual(customer._rtable_Version.ToString(), customer.ETag.ToString(), "customer.Etag mismatch");
            Assert.AreEqual(false, customer._rtable_Tombstone, "customer._rtable_Tombstone mismatch");
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, customer._rtable_ViewId, "customer._rtable_ViewId mismatch");
            Assert.AreEqual(newCustomer.PhoneNumber, customer.PhoneNumber, "customer.PhoneNumber mismatch");
            Assert.AreEqual(newCustomer.Email, customer.Email, "customer.Email mismatch");

            Console.WriteLine("Duplicate insert...");
            // try insert again and it should fail
            result = repTable.Insert(customer);
            Assert.AreNotEqual(result, null, "result null for dup insert");
            Assert.AreEqual(result.HttpStatusCode, (int) HttpStatusCode.Conflict, "Duplicate insert should result in a conflict error. Error = {0}", result.HttpStatusCode);

            Console.WriteLine("Insert Dynamic entity...");
            // Insert Dynamic entity
            // Add insert
            string pk = Guid.NewGuid().ToString();
            DynamicReplicatedTableEntity sendEnt = GenerateRandomEnitity(pk);

            // generate a set of properties for all supported Types
            sendEnt.Properties = new ComplexIEntity().WriteEntity();
            result = repTable.Insert(sendEnt);
            Assert.AreNotEqual(result, null);
            DynamicReplicatedTableEntity currentRow = new DynamicReplicatedTableEntity((TableEntity)result.Result);
            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, currentRow, "currentRow = null");
            Assert.AreEqual("1", result.Etag, "result.Result mismatch");
            Assert.AreEqual(false, currentRow._rtable_RowLock, "currentRow._rtable_RowLock mismatch");
            Assert.AreEqual(1, currentRow._rtable_Version, "currentRow._rtable_Version mismatch");
            Assert.AreEqual(false, currentRow._rtable_Tombstone, "currentRow._rtable_Tombstone mismatch");
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, currentRow._rtable_ViewId, "currentRow._rtable_ViewId mismatch");

            // Retrieve Dynamic entity
            retrievedResult = repTable.Retrieve(sendEnt.PartitionKey, sendEnt.RowKey);
            Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");
            DynamicReplicatedTableEntity retrievedEntity = retrievedResult.Result as DynamicReplicatedTableEntity;
            Assert.AreNotEqual(null, retrievedEntity, "retrievedEntity = null");

            // Validate entity
            Assert.AreEqual(sendEnt["String"], retrievedEntity["String"], "[String] mismatch");
            Assert.AreEqual(sendEnt["Int64"], retrievedEntity["Int64"], "[Int64] mismatch");
            Assert.AreEqual(sendEnt["Int64N"], retrievedEntity["Int64N"], "[Int64N] mismatch");
            Assert.AreEqual(sendEnt["LongPrimitive"], retrievedEntity["LongPrimitive"], "[LongPrimitive] mismatch");
            Assert.AreEqual(sendEnt["LongPrimitiveN"], retrievedEntity["LongPrimitiveN"], "[LongPrimitiveN] mismatch");
            Assert.AreEqual(sendEnt["Int32"], retrievedEntity["Int32"], "[Int32] mismatch");
            Assert.AreEqual(sendEnt["Int32N"], retrievedEntity["Int32N"], "[Int32N] mismatch");
            Assert.AreEqual(sendEnt["IntegerPrimitive"], retrievedEntity["IntegerPrimitive"], "[IntegerPrimitive] mismatch");
            Assert.AreEqual(sendEnt["IntegerPrimitiveN"], retrievedEntity["IntegerPrimitiveN"], "[IntegerPrimitiveN] mismatch");
            Assert.AreEqual(sendEnt["Guid"], retrievedEntity["Guid"], "[Guid] mismatch");
            Assert.AreEqual(sendEnt["GuidN"], retrievedEntity["GuidN"], "[GuidN] mismatch");
            Assert.AreEqual(sendEnt["Double"], retrievedEntity["Double"], "[Double] mismatch");
            Assert.AreEqual(sendEnt["DoubleN"], retrievedEntity["DoubleN"], "[DoubleN] mismatch");
            Assert.AreEqual(sendEnt["DoublePrimitive"], retrievedEntity["DoublePrimitive"], "[DoublePrimitive] mismatch");
            Assert.AreEqual(sendEnt["DoublePrimitiveN"], retrievedEntity["DoublePrimitiveN"], "[DoublePrimitiveN] mismatch");
            Assert.AreEqual(sendEnt["BinaryPrimitive"], retrievedEntity["BinaryPrimitive"], "[BinaryPrimitive] mismatch");
            Assert.AreEqual(sendEnt["Binary"], retrievedEntity["Binary"], "[Binary] mismatch");
            Assert.AreEqual(sendEnt["BoolPrimitive"], retrievedEntity["BoolPrimitive"], "[BoolPrimitive] mismatch");
            Assert.AreEqual(sendEnt["BoolPrimitiveN"], retrievedEntity["BoolPrimitiveN"], "[BoolPrimitiveN] mismatch");
            Assert.AreEqual(sendEnt["Bool"], retrievedEntity["Bool"], "[Bool] mismatch");
            Assert.AreEqual(sendEnt["BoolN"], retrievedEntity["BoolN"], "[BoolN] mismatch");
            Assert.AreEqual(sendEnt["DateTimeOffsetN"], retrievedEntity["DateTimeOffsetN"], "[DateTimeOffsetN] mismatch");
            Assert.AreEqual(sendEnt["DateTimeOffset"], retrievedEntity["DateTimeOffset"], "[DateTimeOffset] mismatch");
            Assert.AreEqual(sendEnt["DateTime"], ((DateTimeOffset)retrievedEntity["DateTime"]).UtcDateTime, "[DateTime] mismatch");
            Assert.AreEqual(sendEnt["DateTimeN"], ((DateTimeOffset)retrievedEntity["DateTimeN"]).UtcDateTime, "[DateTimeN] mismatch");
        }

        [Test(Description = "TableOperation Replace(Etag=* API")]
        public void RTableReplaceWithStarEtagSync()
        {
            // Insert entity
            string firstName = "FirstName03_star";
            string lastName = "LastName03_star";
            string email = "email03@company.com";
            string phone = "1-800-123-0003";

            CustomerEntity newCustomer = new CustomerEntity(firstName, lastName);
            newCustomer.Email = email;
            newCustomer.PhoneNumber = phone;

            TableResult result = repTable.Insert(newCustomer);
            Assert.AreNotEqual(null, result, "result = null");
            ReplicatedTableEntity row = new ReplicatedTableEntity((TableEntity)result.Result);

            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, result.Result, "result.Result = null");
            Assert.AreEqual("1", result.Etag, "result.Etag mismatch");
            Assert.AreEqual(false, row._rtable_RowLock, "row._rtable_RowLock mismatch");
            Assert.AreEqual(1, row._rtable_Version, "row._rtable_Version mismatch");
            Assert.AreEqual(false, row._rtable_Tombstone, "row._rtable_Tombstone mismatch");
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, row._rtable_ViewId, "row._rtable_ViewId mismatch");

            // Replace entity
            Console.WriteLine("Calling TableOperation.Replace(newCustomer)...");
            email = "email03b@company.com";
            phone = "1-800-456-0003";
            newCustomer = new CustomerEntity(firstName, lastName);
            newCustomer.Email = email;
            newCustomer.PhoneNumber = phone;
            newCustomer.ETag = ETag.All;
            result = repTable.Replace(newCustomer);
            Assert.AreNotEqual(null, result, "result = null");

            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, result.Result, "result.Result = null");

            // Retrieve Entity
            Console.WriteLine("Calling TableOperation.Retrieve<CustomerEntity>(firstName, lastName)...");
            TableResult retrievedResult = repTable.Retrieve(firstName, lastName);
            Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");

            CustomerEntity customer = new CustomerEntity((ReplicatedTableEntity)retrievedResult.Result);

            Assert.AreEqual((int)HttpStatusCode.OK, retrievedResult.HttpStatusCode, "retrievedResult.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, retrievedResult.Result, "retrievedResult.Result = null");
            Assert.AreEqual("2", retrievedResult.Etag, "retrievedResult.Etag mismatch");
            Assert.AreEqual(false, customer._rtable_RowLock, "customer._rtable_RowLock mismatch");
            Assert.AreEqual(2, customer._rtable_Version, "customer._rtable_Version mismatch");
            Assert.AreEqual(false, customer._rtable_Tombstone, "customer._rtable_Tombstone mismatch");
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, customer._rtable_ViewId, "customer._rtable_ViewId mismatch");
            Assert.AreEqual(newCustomer.PhoneNumber, customer.PhoneNumber, "customer.PhoneNumber mismatch");
            Assert.AreEqual(newCustomer.Email, customer.Email, "customer.Email mismatch");

            customer.Email = "retrieveandreplace@company.com";
            // Replace again
            Console.WriteLine("Calling TableOperation.Replace after Retrieve...");
            result = repTable.Replace(customer);
            Assert.AreNotEqual(null, result, "result = null");
            Assert.IsTrue(result.HttpStatusCode == (int)HttpStatusCode.NoContent || result.HttpStatusCode == (int)HttpStatusCode.OK, 
                "result.HttpStatusCode mismatch");

            //Non-gen Retrieve and Replace with star           
            Console.WriteLine("Calling TableOperation.Retrieve(firstName, lastName)...");
            retrievedResult = repTable.Retrieve(firstName, lastName);
            Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");
            DynamicReplicatedTableEntity dynCustomer = (DynamicReplicatedTableEntity)retrievedResult.Result;
            Assert.AreEqual((int)HttpStatusCode.OK, retrievedResult.HttpStatusCode, "retrievedResult.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, retrievedResult.Result, "retrievedResult.Result = null");
            Assert.AreEqual("3", retrievedResult.Etag, "retrievedResult.Etag mismatch");
            Assert.AreEqual(false, dynCustomer._rtable_RowLock, "dynCustomer._rtable_RowLock mismatch");
            Assert.AreEqual(3, dynCustomer._rtable_Version, "dynCustomer._rtable_Version mismatch");
            Assert.AreEqual(false, dynCustomer._rtable_Tombstone, "dynCustomer._rtable_Tombstone mismatch");
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, dynCustomer._rtable_ViewId, "customer._rtable_ViewId mismatch");
            Assert.AreEqual(customer.PhoneNumber, dynCustomer.Properties["PhoneNumber"], "dynCustomer.PhoneNumber mismatch");
            Assert.AreEqual(customer.Email, dynCustomer.Properties["Email"], "dynCustomer.Email mismatch");

            dynCustomer.Properties["Email"] = "nongenretrieveandreplace@company.com";
            dynCustomer.ETag = ETag.All;

            // Replace again
            Console.WriteLine("Calling TableOperation.Replace after Retrieve...");

            result = repTable.Replace(dynCustomer);
            Assert.AreNotEqual(null, result, "result = null");
            Console.WriteLine("Foo - returned status: {0}", result.HttpStatusCode);
            Assert.IsTrue(result.HttpStatusCode == (int)HttpStatusCode.NoContent || result.HttpStatusCode == (int)HttpStatusCode.OK,
                "result.HttpStatusCode mismatch");            

        }

        [Test(Description = "TableOperation Replace API")]
        public void RTableReplaceSync()
        {
            // Insert entity
            string firstName = "FirstName03";
            string lastName = "LastName03";
            string email = "email03@company.com";
            string phone = "1-800-123-0003";

            CustomerEntity newCustomer = new CustomerEntity(firstName, lastName);
            newCustomer.Email = email;
            newCustomer.PhoneNumber = phone;

            TableResult result = repTable.Insert(newCustomer);
            Assert.AreNotEqual(null, result, "result = null");
            ReplicatedTableEntity row = new ReplicatedTableEntity((TableEntity)result.Result);

            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, result.Result, "result.Result = null");
            Assert.AreEqual("1", result.Etag, "result.Etag mismatch");
            Assert.AreEqual(false, row._rtable_RowLock, "row._rtable_RowLock mismatch");
            Assert.AreEqual(1, row._rtable_Version, "row._rtable_Version mismatch");
            Assert.AreEqual(false, row._rtable_Tombstone, "row._rtable_Tombstone mismatch");
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, row._rtable_ViewId, "row._rtable_ViewId mismatch");

            // Replace entity
            Console.WriteLine("Calling TableOperation.Replace(newCustomer)...");
            email = "email03b@company.com";
            phone = "1-800-456-0003";
            newCustomer = new CustomerEntity(firstName, lastName);
            newCustomer.Email = email;
            newCustomer.PhoneNumber = phone;
            newCustomer.ETag = new ETag(result.Etag);
            result = repTable.Replace(newCustomer);
            Assert.AreNotEqual(null, result, "result = null");

            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, result.Result, "result.Result = null");

            // Retrieve Entity
            Console.WriteLine("Calling TableOperation.Retrieve<CustomerEntity>(firstName, lastName)...");
            TableResult retrievedResult = repTable.Retrieve(firstName, lastName);
            Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");

            CustomerEntity customer = new CustomerEntity((ReplicatedTableEntity)retrievedResult.Result);

            Assert.AreEqual((int)HttpStatusCode.OK, retrievedResult.HttpStatusCode, "retrievedResult.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, retrievedResult.Result, "retrievedResult.Result = null");
            Assert.AreEqual("2", retrievedResult.Etag, "retrievedResult.Etag mismatch");
            Assert.AreEqual(false, customer._rtable_RowLock, "customer._rtable_RowLock mismatch");
            Assert.AreEqual(2, customer._rtable_Version, "customer._rtable_Version mismatch");
            Assert.AreEqual(false, customer._rtable_Tombstone, "customer._rtable_Tombstone mismatch");
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, customer._rtable_ViewId, "customer._rtable_ViewId mismatch");
            Assert.AreEqual(newCustomer.PhoneNumber, customer.PhoneNumber, "customer.PhoneNumber mismatch");
            Assert.AreEqual(newCustomer.Email, customer.Email, "customer.Email mismatch");

            //Non-gen Retrieve and Replace with etag check
            Console.WriteLine("Calling TableOperation.Retrieve(firstName, lastName)...");
            retrievedResult = repTable.Retrieve(firstName, lastName);
            Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");
            DynamicReplicatedTableEntity dynCustomer = (DynamicReplicatedTableEntity)retrievedResult.Result;
            Assert.AreEqual((int)HttpStatusCode.OK, retrievedResult.HttpStatusCode, "retrievedResult.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, retrievedResult.Result, "retrievedResult.Result = null");
            Assert.AreEqual("2", retrievedResult.Etag, "retrievedResult.Etag mismatch");
            Assert.AreEqual(false, dynCustomer._rtable_RowLock, "dynCustomer._rtable_RowLock mismatch");
            Assert.AreEqual(2, dynCustomer._rtable_Version, "dynCustomer._rtable_Version mismatch");
            Assert.AreEqual(false, dynCustomer._rtable_Tombstone, "dynCustomer._rtable_Tombstone mismatch");
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, dynCustomer._rtable_ViewId, "customer._rtable_ViewId mismatch");
            Assert.AreEqual(customer.PhoneNumber, dynCustomer.Properties["PhoneNumber"], "dynCustomer.PhoneNumber mismatch");
            Assert.AreEqual(customer.Email, dynCustomer.Properties["Email"], "dynCustomer.Email mismatch");

            dynCustomer.Properties["Email"] = "nongenretrieveandreplace@company.com";

            // Replace again
            Console.WriteLine("Calling TableOperation.Replace after Retrieve...");

            result = repTable.Replace(dynCustomer);
            Assert.AreNotEqual(null, result, "result = null");
            Console.WriteLine("Foo - returned status: {0}", result.HttpStatusCode);
            Assert.IsTrue(result.HttpStatusCode == (int)HttpStatusCode.NoContent || result.HttpStatusCode == (int)HttpStatusCode.OK,
                "result.HttpStatusCode mismatch");

            //Non-gen Retrieve
            Console.WriteLine("Calling TableOperation.Retrieve(firstName, lastName)...");
            retrievedResult = repTable.Retrieve(firstName, lastName);
            Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");
            dynCustomer = (DynamicReplicatedTableEntity)retrievedResult.Result;
            Assert.AreEqual((int)HttpStatusCode.OK, retrievedResult.HttpStatusCode, "retrievedResult.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, retrievedResult.Result, "retrievedResult.Result = null");
            Assert.AreEqual("3", retrievedResult.Etag, "retrievedResult.Etag mismatch");
            Assert.AreEqual(false, dynCustomer._rtable_RowLock, "dynCustomer._rtable_RowLock mismatch");
            Assert.AreEqual(3, dynCustomer._rtable_Version, "dynCustomer._rtable_Version mismatch");
            Assert.AreEqual(false, dynCustomer._rtable_Tombstone, "dynCustomer._rtable_Tombstone mismatch");
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, dynCustomer._rtable_ViewId, "customer._rtable_ViewId mismatch");
            Assert.AreEqual(customer.PhoneNumber, dynCustomer.Properties["PhoneNumber"], "dynCustomer.PhoneNumber mismatch");
            Assert.AreEqual("nongenretrieveandreplace@company.com", dynCustomer.Properties["Email"], "dynCustomer.Email mismatch");
        }

        [Test(Description = "TableOperation Delete")]
        public void RTableDeleteSync()
        {
            string firstName = "FirstName04";
            string lastName = "LastName04";
            string email = "email04@company.com";
            string phone = "1-800-123-0004";

            // Insert entity
            CustomerEntity newCustomer = new CustomerEntity(firstName, lastName);
            newCustomer.Email = email;
            newCustomer.PhoneNumber = phone;

            TableResult result = repTable.InsertOrReplace(newCustomer);
            Assert.AreNotEqual(null, result, "result = null");

            ReplicatedTableEntity row = new ReplicatedTableEntity((TableEntity)result.Result);

            Assert.AreNotEqual(result, null);
            Assert.AreNotEqual(result.HttpStatusCode, 503);
            Assert.AreNotEqual(result.Result, null);
            Assert.AreEqual(result.Etag, "1");
            Assert.AreEqual(row._rtable_RowLock, false);
            Assert.AreEqual(row._rtable_Version, 1);
            Assert.AreEqual(row._rtable_Tombstone, false);
            Assert.AreEqual(row._rtable_ViewId, this.rtableTestConfiguration.RTableInformation.ViewId);

            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, result.Result, "result.Result = null");
            Assert.AreEqual("1", result.Etag, "result.Etag mismatch");
            Assert.AreEqual(false, row._rtable_RowLock, "row._rtable_RowLock mismatch");
            Assert.AreEqual(1, row._rtable_Version, "row._rtable_Version mismatch");
            Assert.AreEqual(false, row._rtable_Tombstone, "row._rtable_Tombstone mismatch");
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, row._rtable_ViewId, "row._rtable_ViewId mismatch");

            // Delete operation
            Console.WriteLine("Calling TableOperation.Delete(newCustomer)...");
            newCustomer = new CustomerEntity(firstName, lastName);
            newCustomer.ETag = new ETag(result.Etag);
            TableResult deleteResult = repTable.Delete(newCustomer);

            Assert.AreNotEqual(null, deleteResult, "deleteResult = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, deleteResult.HttpStatusCode, "deleteResult.HttpStatusCode mismatch");
            Assert.IsNotNull(deleteResult.Result, "deleteResult.Result = null");

            // Retrieve
            Console.WriteLine("Calling TableOperation.Retrieve() after Delete() was called...");
            TableResult retrievedResult2 = repTable.Retrieve(firstName, lastName);
            Assert.AreEqual((int)HttpStatusCode.NotFound, retrievedResult2.HttpStatusCode, "retrievedResult2.HttpStatusCode mismatch");
            Assert.IsNull(retrievedResult2.Result, "retrievedResult2.Result != null");
        }

        [Test(Description = "TableOperation Delete(Etag=*)")]
        public void RTableDeleteWithStarEtagSync()
        {
            string firstName = "FirstName04";
            string lastName = "LastName04";
            string email = "email04@company.com";
            string phone = "1-800-123-0004";

            // Insert entity
            CustomerEntity newCustomer = new CustomerEntity(firstName, lastName);
            newCustomer.Email = email;
            newCustomer.PhoneNumber = phone;

            TableResult result = repTable.InsertOrReplace(newCustomer);
            Assert.AreNotEqual(null, result, "result = null");

            ReplicatedTableEntity row = new ReplicatedTableEntity((TableEntity)result.Result);

            Assert.AreNotEqual(result, null);
            Assert.AreNotEqual(result.HttpStatusCode, 503);
            Assert.AreNotEqual(result.Result, null);
            Assert.AreEqual(result.Etag, "1");
            Assert.AreEqual(row._rtable_RowLock, false);
            Assert.AreEqual(row._rtable_Version, 1);
            Assert.AreEqual(row._rtable_Tombstone, false);
            Assert.AreEqual(row._rtable_ViewId, this.rtableTestConfiguration.RTableInformation.ViewId);

            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, result.Result, "result.Result = null");
            Assert.AreEqual("1", result.Etag, "result.Etag mismatch");
            Assert.AreEqual(false, row._rtable_RowLock, "row._rtable_RowLock mismatch");
            Assert.AreEqual(1, row._rtable_Version, "row._rtable_Version mismatch");
            Assert.AreEqual(false, row._rtable_Tombstone, "row._rtable_Tombstone mismatch");
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, row._rtable_ViewId, "row._rtable_ViewId mismatch");

            // Delete operation
            Console.WriteLine("Calling TableOperation.Delete(newCustomer)...");
            newCustomer = new CustomerEntity(firstName, lastName);
            newCustomer.ETag = ETag.All;
            TableResult deleteResult = repTable.Delete(newCustomer);

            Assert.AreNotEqual(null, deleteResult, "deleteResult = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, deleteResult.HttpStatusCode, "deleteResult.HttpStatusCode mismatch");
            Assert.IsNotNull(deleteResult.Result, "deleteResult.Result = null");

            // Retrieve
            Console.WriteLine("Calling TableOperation.Retrieve() after Delete() was called...");
            TableResult retrievedResult2 = repTable.Retrieve(firstName, lastName);
            Assert.AreEqual((int)HttpStatusCode.NotFound, retrievedResult2.HttpStatusCode, "retrievedResult2.HttpStatusCode mismatch");
            Assert.IsNull(retrievedResult2.Result, "retrievedResult2.Result != null");
        }

        [Test(Description = "TableOperation Merge Sync")]
        public void RTableMergeSync()
        {
            // Insert Entity
            Console.WriteLine("Calling TableOperation.Insert()...");
            ReplicatedTable currentTable = this.repTable;
            DynamicReplicatedTableEntity baseEntity = new DynamicReplicatedTableEntity("merge test01", "foo01");
            baseEntity.Properties.Add("prop1", "value1");
            TableResult result = currentTable.Insert(baseEntity);
            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, result, "result = null");
            Assert.AreEqual("1", result.Etag, "result.Etag mismatch");

            // Retrieve Entity & Verify Contents
            Console.WriteLine("Calling TableOperation.Retrieve()...");
            result = currentTable.Retrieve(baseEntity.PartitionKey, baseEntity.RowKey);
            Assert.AreNotEqual(null, result, "result = null");
            DynamicReplicatedTableEntity retrievedEntity = result.Result as DynamicReplicatedTableEntity;
            Assert.IsNotNull(retrievedEntity, "retrievedEntity = null");

            DynamicReplicatedTableEntity row = retrievedEntity;
            Assert.AreEqual(false, row._rtable_RowLock, "row._rtable_RowLock mismatch");
            Assert.AreEqual(1, row._rtable_Version, "row._rtable_Version mismatch");
            Assert.AreEqual(false, row._rtable_Tombstone, "row._rtable_Tombstone mismatch");
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, row._rtable_ViewId, "row._rtable_ViewId mismatch");

            // Merge 
            Console.WriteLine("Calling TableOperation.Merge()...");
            DynamicReplicatedTableEntity mergeEntity = new DynamicReplicatedTableEntity(baseEntity.PartitionKey, baseEntity.RowKey) { ETag = new ETag(result.Etag) };
            mergeEntity.Properties.Add("prop2", "value2");
            result = currentTable.Merge(mergeEntity);

            Assert.AreNotEqual(null, result, "result = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, result.Result, "result.Result = null");
            Assert.AreEqual("2", result.Etag, "result.Etag mismatch");

            row = new DynamicReplicatedTableEntity((TableEntity)result.Result);
            Assert.AreEqual(false, row._rtable_RowLock, "row._rtable_RowLock mismatch");
            Assert.AreEqual(2, row._rtable_Version, "row._rtable_Version mismatch");
            Assert.AreEqual(false, row._rtable_Tombstone, "row._rtable_Tombstone mismatch");
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, row._rtable_ViewId, "row._rtable_ViewId mismatch");

            // Retrieve Entity & Verify Contents
            Console.WriteLine("Calling TableOperation.Retrieve() after Merge() was called...");
            result = currentTable.Retrieve(baseEntity.PartitionKey, baseEntity.RowKey);
            Assert.AreNotEqual(null, result, "result = null");
            Assert.AreEqual((int)HttpStatusCode.OK, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, result.Result, "result.Result = null");

            retrievedEntity = result.Result as DynamicReplicatedTableEntity;
            Assert.IsNotNull(retrievedEntity, "retrievedEntity = null");

            row = retrievedEntity;            
            Assert.AreEqual(false, row._rtable_RowLock, "row._rtable_RowLock mismatch");
            Assert.AreEqual(2, row._rtable_Version, "row._rtable_Version mismatch");
            Assert.AreEqual(false, row._rtable_Tombstone, "row._rtable_Tombstone mismatch");
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, row._rtable_ViewId, "row._rtable_ViewId mismatch");
            Assert.AreEqual(6, retrievedEntity.Properties.Count, "retrievedEntity.Properties.Count mismatch");
            Assert.AreEqual(baseEntity.Properties["prop1"], retrievedEntity.Properties["prop1"], "Properties[prop1] mismatch");
            Assert.AreEqual(mergeEntity.Properties["prop2"], retrievedEntity.Properties["prop2"], "Properties[prop2] mismatch");

        }

        [Test(Description = "TableOperation InsetOrMerge")]
        public void RTableInsertOrMergeSync()
        {
            // Insert Entity
            ReplicatedTable currentTable = this.repTable;
            DynamicReplicatedTableEntity baseEntity = new DynamicReplicatedTableEntity("merge test02", "foo02");
            baseEntity.Properties.Add("prop1", "value1");
            Console.WriteLine("Calling TableOperation.Insert()...");
            TableResult result = currentTable.Insert(baseEntity);
            Assert.AreNotEqual(null, result, "Insert(): result = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "Insert(): result.HttpStatusCode mismatch");

            DynamicReplicatedTableEntity mergeEntity = new DynamicReplicatedTableEntity(baseEntity.PartitionKey, baseEntity.RowKey) { ETag = new ETag(result.Etag) };
            mergeEntity.Properties.Add("prop2", "value2");
            Console.WriteLine("Calling TableOperation.Merge()...");
            result = currentTable.Merge(mergeEntity);
            Assert.AreNotEqual(null, result, "Merge(): result = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "Merge(): result.HttpStatusCode mismatch");

            DynamicReplicatedTableEntity mergeEntity2 = new DynamicReplicatedTableEntity(baseEntity.PartitionKey, baseEntity.RowKey) { ETag = new ETag(result.Etag) };
            mergeEntity2.Properties.Add("prop3", "value3");
            Console.WriteLine("Calling TableOperation.InsertOrMerge()...");
            result = currentTable.InsertOrMerge(mergeEntity2);
            Assert.AreNotEqual(null, result, "InsertOrMerge(): result = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "InsertOrMerge(): result.HttpStatusCode mismatch");

            // Retrieve Entity & Verify Contents
            Console.WriteLine("Calling TableOperation.Retrieve()...");
            result = currentTable.Retrieve(baseEntity.PartitionKey, baseEntity.RowKey);
            Assert.AreNotEqual(null, result, "Retrieve(): result = null");
            DynamicReplicatedTableEntity retrievedEntity = result.Result as DynamicReplicatedTableEntity;

            Assert.IsNotNull(retrievedEntity, "retrievedEntity = null");
            Assert.AreEqual(7, retrievedEntity.Properties.Count, "Properties.Count mismatch");
            Assert.AreEqual(baseEntity.Properties["prop1"], retrievedEntity.Properties["prop1"], "Properties[prop1] mismatch");
            Assert.AreEqual(mergeEntity.Properties["prop2"], retrievedEntity.Properties["prop2"], "Properties[prop2] mismatch");
            Assert.AreEqual(mergeEntity2.Properties["prop3"], retrievedEntity.Properties["prop3"], "Properties[prop3] mismatch");


            // InitDynamicReplicatedTableEntity Insert
            currentTable = this.repTable;
            IDictionary<string, object> properties = new Dictionary<string, object>();
            properties.Add("Name", "Cisco-ASR-1006");
            properties.Add("Description","Cisco ASR 1006 Router");
            properties.Add("MaxSubInterfaces", 1000);
            properties.Add("MaxTunnelInterfaces", 4000);
            properties.Add("MaxVrfs", 4000);
            properties.Add("MaxBfdSessions", 4000);

            InitDynamicReplicatedTableEntity initEntity = new InitDynamicReplicatedTableEntity("0", "devicetype__Cisco-ASR-1006", null, properties);
            result = currentTable.Insert(initEntity);
            Assert.IsTrue(result.HttpStatusCode == (int)HttpStatusCode.NoContent);

        }

        #endregion Table Operations Test Methods

        #region Helpers
        private static DynamicReplicatedTableEntity GenerateRandomEnitity(string pk)
        {
            DynamicReplicatedTableEntity ent = new DynamicReplicatedTableEntity();
            ent.Properties.Add("foo", "bar");

            ent.PartitionKey = pk;
            ent.RowKey = Guid.NewGuid().ToString();
            return ent;
        }

        #endregion Helpers
    }
}
