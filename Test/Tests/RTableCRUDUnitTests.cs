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
    using Microsoft.WindowsAzure.Storage.Table;
    using NUnit.Framework;
    using System.Net;

    [TestFixture]
    public class RTableCRUDUnitTests : RTableWrapperTestBase
    {
        [TestFixtureSetUp]
        public void TestFixtureSetup()
        {
            this.LoadTestConfiguration();
            string tableName = this.GenerateRandomTableName();
            Console.WriteLine("tableName = {0}", tableName);
            this.SetupRTableEnv(true, tableName, true, "", new List<int> { 0 });
        }

        [TestFixtureTearDown]
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
            CloudTableClient tableClient = this.cloudTableClients[0];
            CloudTable table = tableClient.GetTableReference(this.repTable.TableName);

            string firstName = "FirstNameXStore";
            string lastName = "LastNameXStore";
            string email = "xstore1@company.com";
            string phone = "1-800-123-0001";
            CustomerEntity newCustomer = new CustomerEntity(firstName, lastName);
            newCustomer.Email = email;
            newCustomer.PhoneNumber = phone;

            // XStore Insert
            Console.WriteLine("Calling XStore Insert...");
            TableOperation insertOperation = TableOperation.Insert(newCustomer);
            TableResult insertResult = table.Execute(insertOperation);

            Assert.IsNotNull(insertResult, "insertResult = null");
            Console.WriteLine("insertResult.HttpStatusCode = {0}", insertResult.HttpStatusCode);
            Console.WriteLine("insertResult.ETag = {0}", insertResult.Etag);
            Assert.AreEqual((int)HttpStatusCode.NoContent, insertResult.HttpStatusCode, "insertResult.HttpStatusCode mismatch");
            Assert.IsFalse(string.IsNullOrEmpty(insertResult.Etag), "insertResult.ETag = null or empty");

            ITableEntity row = (ITableEntity)insertResult.Result;
            Assert.IsNotNull(row, "insertResult.Result = null");
            Console.WriteLine("row.PartitionKey = {0}", row.PartitionKey);
            Console.WriteLine("row.RowKey = {0}", row.RowKey);

            CustomerEntity customer = (CustomerEntity)insertResult.Result;
            Assert.IsNotNull(customer, "Insert: customer = null");
            Console.WriteLine("Insert: customer.Email = {0}", customer.Email);
            Console.WriteLine("Insert: customer.PhoneNumber = {0}", customer.PhoneNumber);
            Assert.AreEqual(newCustomer.Email, customer.Email, "Insert: customer.Email mismatch");
            Assert.AreEqual(newCustomer.PhoneNumber, customer.PhoneNumber, "Insert: customer.PhoneNumber mismatch");
            
            // Retrieve
            Console.WriteLine("Calling XStore Retrieve...");
            TableOperation retrieveOperation = TableOperation.Retrieve<CustomerEntity>(row.PartitionKey, row.RowKey);
            TableResult retrieveResult = table.Execute(retrieveOperation);
            Assert.IsNotNull(retrieveResult, "retrieveResult = null");
            Console.WriteLine("retrieveResult.HttpStatusCode = {0}", retrieveResult.HttpStatusCode);
            Assert.AreEqual((int)HttpStatusCode.OK, retrieveResult.HttpStatusCode, "retrieveResult.HttpStatusCode mismatch");
            customer = (CustomerEntity)retrieveResult.Result;
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
            TableOperation updateOperation = TableOperation.Replace(customer);
            TableResult updateResult = table.Execute(updateOperation);
            Assert.IsNotNull(updateResult, "updateResult = null");
            Console.WriteLine("updateResult.HttpStatusCode = {0}", updateResult.HttpStatusCode);
            Assert.AreEqual((int)HttpStatusCode.NoContent, updateResult.HttpStatusCode, "updateResult.HttpStatusCode mismatch");

            // Retrieve after Replace
            Console.WriteLine("Calling XStore Retrieve after Replace was called...");
            retrieveOperation = TableOperation.Retrieve<CustomerEntity>(row.PartitionKey, row.RowKey);
            retrieveResult = table.Execute(retrieveOperation);
            Assert.IsNotNull(retrieveResult, "RetrieveAfterReplace: retrieveResult = null");
            Console.WriteLine("RetrieveAfterReplace: retrieveResult.HttpStatusCode = {0}", retrieveResult.HttpStatusCode);
            Assert.AreEqual((int)HttpStatusCode.OK, retrieveResult.HttpStatusCode, "RetrieveAfterReplace: retrieveResult.HttpStatusCode mismatch");
            CustomerEntity customer2 = (CustomerEntity)retrieveResult.Result;
            Assert.IsNotNull(customer2, "RetrieveAfterReplace: customer2 = null");
            Console.WriteLine("RetrieveAfterReplace: customer2.Email = {0}", customer2.Email);
            Console.WriteLine("RetrieveAfterReplace: customer2.PhoneNumber = {0}", customer2.PhoneNumber);
            Assert.AreEqual(email2, customer2.Email, "RetrieveAfterReplace: customer2.Email mismatch");
            Assert.AreEqual(phone2, customer2.PhoneNumber, "RetrieveAfterReplace: customer2.PhoneNumber mismatch");
            
            // Delete
            Console.WriteLine("Calling XStore Delete...");
            TableOperation deleteOperation = TableOperation.Delete(customer2);
            TableResult deleteResult = table.Execute(deleteOperation);
            Assert.IsNotNull(deleteResult, "deleteResult = null");
            Console.WriteLine("deleteResult.HttpStatusCode = {0}", deleteResult.HttpStatusCode);
            Assert.AreEqual((int)HttpStatusCode.NoContent, deleteResult.HttpStatusCode, "deleteResult.HttpStatusCode mismatch");

            // Retrieve after Delete
            Console.WriteLine("Calling XStore Retrieve after Delete was called...");
            retrieveOperation = TableOperation.Retrieve<CustomerEntity>(row.PartitionKey, row.RowKey);
            retrieveResult = table.Execute(retrieveOperation);
            Assert.IsNotNull(retrieveResult, "RetrieveAfterDelete: retrieveResult = null");
            Console.WriteLine("RetrieveAfterDelete: retrieveResult.HttpStatusCode = {0}", retrieveResult.HttpStatusCode);
            Assert.AreEqual((int)HttpStatusCode.NotFound, retrieveResult.HttpStatusCode, "RetrieveAfterDelete: retrieveResult.HttpStatusCode mismatch");
            CustomerEntity customer3 = (CustomerEntity)retrieveResult.Result;
            Assert.IsNull(customer3, "RetrieveAfterDelete: customer3 != null");

            // Replace after Delete
            Console.WriteLine("Calling XStore Replace after Delete...");
            updateOperation = TableOperation.Replace(customer2);
            try
            {
                updateResult = table.Execute(updateOperation);
                Assert.Fail("RetrieveAfterDelete: table.Execute() should throw.");
            }
            catch (Exception ex)
            {                
                WebException webEx = ex.InnerException as WebException;
                if (webEx != null)
                {
                    Console.WriteLine("webEx.Message = {0}", webEx.Message);
                    HttpWebResponse webResponse = webEx.Response as HttpWebResponse;
                    if (webResponse != null)
                    {
                        Console.WriteLine("webResponse.StatusCode = {0}", webResponse.StatusCode);
                        Assert.AreEqual(HttpStatusCode.NotFound.ToString(), webResponse.StatusCode.ToString(), "RetrieveAfterDelete: webResponse.StatusCode mismatch");
                    }
                    Assert.NotNull(webResponse, "RetrieveAfterDelete: WebResponse is null");
                }
                Assert.NotNull(webEx, "RetrieveAfterDelete: StorageException was not a WebException");
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
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, row._rtable_ViewId, "row._rtable_ViewId mismatch");

            //Non-generic retrieve 
            operation = TableOperation.Retrieve(firstName, lastName);
            TableResult retrievedResult = this.repTable.Execute(operation);
            Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");
            Assert.IsTrue(retrievedResult.Result is IReplicatedTableEntity, "Expected entity to be of type IReplicatedTableEntity");
            DynamicReplicatedTableEntity readRow = retrievedResult.Result as DynamicReplicatedTableEntity;
            Assert.IsTrue(readRow.ETag == "1", "Returned etag is not zero");
            Assert.IsTrue(readRow.Properties.ContainsKey("Email"), "DynamicRTableEntity returned didnt contain Email");
            Assert.IsTrue(readRow.Properties.ContainsKey("PhoneNumber"), "DynamicRTableEntity returned didnt contain PhoneNumber");
            Assert.IsTrue(readRow.Properties.Count == 2, "DynamicRTableEntity returned contained diff number of properties");
                       
            ////Dynamic insert entity
            DynamicReplicatedTableEntity newDynamicCustomer = new DynamicReplicatedTableEntity(dynamicFirstName, dynamicLastName);
            newDynamicCustomer.Properties["Email"] = EntityProperty.CreateEntityPropertyFromObject(dynamicEmail);
            newDynamicCustomer.Properties["PhoneNumber"] = EntityProperty.CreateEntityPropertyFromObject(dynamicPhone);

            operation = TableOperation.Insert(newDynamicCustomer);
            result = repTable.Execute(operation);
            Assert.AreNotEqual(null, result, "result = null");
            row = (DynamicReplicatedTableEntity)result.Result;
            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, result.Result, "result.Result = null");
            Assert.AreEqual("1", result.Etag, "result.Etag mismatch");
            Assert.AreEqual(false, row._rtable_RowLock, "row._rtable_RowLock mismatch");
            Assert.AreEqual(1, row._rtable_Version, "row._rtable_Version mismatch");
            Assert.AreEqual(false, row._rtable_Tombstone, "row._rtable_Tombstone mismatch");
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, row._rtable_ViewId, "row._rtable_ViewId mismatch");

            //Non-generic retrieve for dynamic entity
            operation = TableOperation.Retrieve(dynamicFirstName, dynamicLastName);
            retrievedResult = this.repTable.Execute(operation);
            Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");
            Assert.IsTrue(retrievedResult.Result is IReplicatedTableEntity, "Expected entity to be of type IReplicatedTableEntity");
            readRow = retrievedResult.Result as DynamicReplicatedTableEntity;
            Assert.IsTrue(readRow.ETag == "1", "Returned etag is not zero");
            Assert.IsTrue(readRow.Properties.ContainsKey("Email"), "DynamicRTableEntity returned didnt contain Email");
            Assert.IsTrue(readRow.Properties.ContainsKey("PhoneNumber"), "DynamicRTableEntity returned didnt contain PhoneNumber");
            Assert.IsTrue(readRow.Properties.Count == 2, "DynamicRTableEntity returned contained diff number of properties");
          
            // Retrieve entity
            operation = TableOperation.Retrieve<CustomerEntity>(firstName, lastName);
            retrievedResult = this.repTable.Execute(operation);
            Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null"); 
            CustomerEntity customer = (CustomerEntity)retrievedResult.Result;

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
            operation = TableOperation.InsertOrReplace(newCustomer);
            result = this.repTable.Execute(operation);
            Assert.AreNotEqual(result, null);
            row = (ReplicatedTableEntity)result.Result;

            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, result.Result, "result.Result = null");
            Assert.AreEqual("2", result.Etag, "result.Etag mismatch");
            Assert.AreEqual(false, row._rtable_RowLock, "row._rtable_RowLock mismatch");
            Assert.AreEqual(2, row._rtable_Version, "row._rtable_Version mismatch");
            Assert.AreEqual(false, row._rtable_Tombstone, "row._rtable_Tombstone mismatch");
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, row._rtable_ViewId, "row._rtable_ViewId mismatch");

            // Retrieve entity
            Console.WriteLine("Calling Retrieve...");
            operation = TableOperation.Retrieve<CustomerEntity>(firstName, lastName);
            retrievedResult = this.repTable.Execute(operation);
            Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");

            customer = (CustomerEntity)retrievedResult.Result;
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
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, row._rtable_ViewId, "row._rtable_ViewId mismatch");

            // Retrieve entity
            operation = TableOperation.Retrieve<CustomerEntity>(firstName, lastName);
            TableResult retrievedResult = repTable.Execute(operation);
            Assert.AreNotEqual(retrievedResult, null, "retrievedResult = null");
            CustomerEntity customer = (CustomerEntity)retrievedResult.Result;
            Assert.AreEqual((int)HttpStatusCode.OK, retrievedResult.HttpStatusCode, "retrievedResult.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, retrievedResult.Result, "retrievedResult.Result = null");
            Assert.AreEqual("1", retrievedResult.Etag, "retrievedResult.Etag mismatch");
            Assert.AreEqual(false, customer._rtable_RowLock, "customer._rtable_RowLock mismatch");
            Assert.AreEqual(1, customer._rtable_Version, "customer._rtable_Version mismatch");
            Assert.AreEqual(customer._rtable_Version.ToString(), customer.ETag, "customer.Etag mismatch");
            Assert.AreEqual(false, customer._rtable_Tombstone, "customer._rtable_Tombstone mismatch");
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, customer._rtable_ViewId, "customer._rtable_ViewId mismatch");
            Assert.AreEqual(newCustomer.PhoneNumber, customer.PhoneNumber, "customer.PhoneNumber mismatch");
            Assert.AreEqual(newCustomer.Email, customer.Email, "customer.Email mismatch");

            Console.WriteLine("Duplicate insert...");
            // try insert again and it should fail
            result = repTable.Execute(TableOperation.Insert(customer));
            Assert.AreNotEqual(result, null, "result null for dup insert");
            Assert.AreEqual(result.HttpStatusCode, (int) HttpStatusCode.Conflict, "Duplicate insert should result in a conflict error. Error = {0}", result.HttpStatusCode);

            Console.WriteLine("Insert Dynamic entity...");
            // Insert Dynamic entity
            // Add insert
            string pk = Guid.NewGuid().ToString();
            DynamicReplicatedTableEntity sendEnt = GenerateRandomEnitity(pk);

            // generate a set of properties for all supported Types
            sendEnt.Properties = new ComplexIEntity().WriteEntity(null);
            result = repTable.Execute(TableOperation.Insert(sendEnt));
            Assert.AreNotEqual(result, null);
            DynamicReplicatedTableEntity currentRow = (DynamicReplicatedTableEntity)result.Result;            
            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, currentRow, "currentRow = null");
            Assert.AreEqual("1", result.Etag, "result.Result mismatch");
            Assert.AreEqual(false, currentRow._rtable_RowLock, "currentRow._rtable_RowLock mismatch");
            Assert.AreEqual(1, currentRow._rtable_Version, "currentRow._rtable_Version mismatch");
            Assert.AreEqual(false, currentRow._rtable_Tombstone, "currentRow._rtable_Tombstone mismatch");
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, currentRow._rtable_ViewId, "currentRow._rtable_ViewId mismatch");

            // Retrieve Dynamic entity
            operation = TableOperation.Retrieve<DynamicReplicatedTableEntity>(sendEnt.PartitionKey, sendEnt.RowKey);
            retrievedResult = repTable.Execute(operation);
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
            Assert.AreEqual(sendEnt["DateTime"], retrievedEntity["DateTime"], "[DateTime] mismatch");
            Assert.AreEqual(sendEnt["DateTimeN"], retrievedEntity["DateTimeN"], "[DateTimeN] mismatch");
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
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, row._rtable_ViewId, "row._rtable_ViewId mismatch");

            // Replace entity
            Console.WriteLine("Calling TableOperation.Replace(newCustomer)...");
            email = "email03b@company.com";
            phone = "1-800-456-0003";
            newCustomer = new CustomerEntity(firstName, lastName);
            newCustomer.Email = email;
            newCustomer.PhoneNumber = phone;
            newCustomer.ETag = "*";
            operation = TableOperation.Replace(newCustomer);
            result = repTable.Execute(operation);
            Assert.AreNotEqual(null, result, "result = null");

            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, result.Result, "result.Result = null");

            // Retrieve Entity
            Console.WriteLine("Calling TableOperation.Retrieve<CustomerEntity>(firstName, lastName)...");
            operation = TableOperation.Retrieve<CustomerEntity>(firstName, lastName);
            TableResult retrievedResult = repTable.Execute(operation);
            Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");

            CustomerEntity customer = (CustomerEntity)retrievedResult.Result;

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
            operation = TableOperation.Replace(customer);
            result = repTable.Execute(operation);
            Assert.AreNotEqual(null, result, "result = null");
            Assert.IsTrue(result.HttpStatusCode == (int)HttpStatusCode.NoContent || result.HttpStatusCode == (int)HttpStatusCode.OK, 
                "result.HttpStatusCode mismatch");

            //Non-gen Retrieve and Replace with star           
            Console.WriteLine("Calling TableOperation.Retrieve(firstName, lastName)...");
            operation = TableOperation.Retrieve(firstName, lastName);
            retrievedResult = repTable.Execute(operation);
            Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");
            DynamicReplicatedTableEntity dynCustomer = (DynamicReplicatedTableEntity)retrievedResult.Result;
            Assert.AreEqual((int)HttpStatusCode.OK, retrievedResult.HttpStatusCode, "retrievedResult.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, retrievedResult.Result, "retrievedResult.Result = null");
            Assert.AreEqual("3", retrievedResult.Etag, "retrievedResult.Etag mismatch");
            Assert.AreEqual(false, dynCustomer._rtable_RowLock, "dynCustomer._rtable_RowLock mismatch");
            Assert.AreEqual(3, dynCustomer._rtable_Version, "dynCustomer._rtable_Version mismatch");
            Assert.AreEqual(false, dynCustomer._rtable_Tombstone, "dynCustomer._rtable_Tombstone mismatch");
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, dynCustomer._rtable_ViewId, "customer._rtable_ViewId mismatch");
            Assert.AreEqual(customer.PhoneNumber, dynCustomer.Properties["PhoneNumber"].StringValue, "dynCustomer.PhoneNumber mismatch");
            Assert.AreEqual(customer.Email, dynCustomer.Properties["Email"].StringValue, "dynCustomer.Email mismatch");

            dynCustomer.Properties["Email"] = EntityProperty.CreateEntityPropertyFromObject("nongenretrieveandreplace@company.com");
            dynCustomer.ETag = "*";

            // Replace again
            Console.WriteLine("Calling TableOperation.Replace after Retrieve...");

            operation = TableOperation.Replace(dynCustomer);
            result = repTable.Execute(operation);
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
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, row._rtable_ViewId, "row._rtable_ViewId mismatch");

            // Replace entity
            Console.WriteLine("Calling TableOperation.Replace(newCustomer)...");
            email = "email03b@company.com";
            phone = "1-800-456-0003";
            newCustomer = new CustomerEntity(firstName, lastName);
            newCustomer.Email = email;
            newCustomer.PhoneNumber = phone;
            newCustomer.ETag = result.Etag;
            operation = TableOperation.Replace(newCustomer);
            result = repTable.Execute(operation);
            Assert.AreNotEqual(null, result, "result = null");

            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, result.Result, "result.Result = null");

            // Retrieve Entity
            Console.WriteLine("Calling TableOperation.Retrieve<CustomerEntity>(firstName, lastName)...");
            operation = TableOperation.Retrieve<CustomerEntity>(firstName, lastName);
            TableResult retrievedResult = repTable.Execute(operation);
            Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");

            CustomerEntity customer = (CustomerEntity)retrievedResult.Result;

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
            operation = TableOperation.Retrieve(firstName, lastName);
            retrievedResult = repTable.Execute(operation);
            Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");
            DynamicReplicatedTableEntity dynCustomer = (DynamicReplicatedTableEntity)retrievedResult.Result;
            Assert.AreEqual((int)HttpStatusCode.OK, retrievedResult.HttpStatusCode, "retrievedResult.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, retrievedResult.Result, "retrievedResult.Result = null");
            Assert.AreEqual("2", retrievedResult.Etag, "retrievedResult.Etag mismatch");
            Assert.AreEqual(false, dynCustomer._rtable_RowLock, "dynCustomer._rtable_RowLock mismatch");
            Assert.AreEqual(2, dynCustomer._rtable_Version, "dynCustomer._rtable_Version mismatch");
            Assert.AreEqual(false, dynCustomer._rtable_Tombstone, "dynCustomer._rtable_Tombstone mismatch");
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, dynCustomer._rtable_ViewId, "customer._rtable_ViewId mismatch");
            Assert.AreEqual(customer.PhoneNumber, dynCustomer.Properties["PhoneNumber"].StringValue, "dynCustomer.PhoneNumber mismatch");
            Assert.AreEqual(customer.Email, dynCustomer.Properties["Email"].StringValue, "dynCustomer.Email mismatch");

            dynCustomer.Properties["Email"] = EntityProperty.CreateEntityPropertyFromObject("nongenretrieveandreplace@company.com");

            // Replace again
            Console.WriteLine("Calling TableOperation.Replace after Retrieve...");

            operation = TableOperation.Replace(dynCustomer);
            result = repTable.Execute(operation);
            Assert.AreNotEqual(null, result, "result = null");
            Console.WriteLine("Foo - returned status: {0}", result.HttpStatusCode);
            Assert.IsTrue(result.HttpStatusCode == (int)HttpStatusCode.NoContent || result.HttpStatusCode == (int)HttpStatusCode.OK,
                "result.HttpStatusCode mismatch");

            //Non-gen Retrieve
            Console.WriteLine("Calling TableOperation.Retrieve(firstName, lastName)...");
            operation = TableOperation.Retrieve(firstName, lastName);
            retrievedResult = repTable.Execute(operation);
            Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");
            dynCustomer = (DynamicReplicatedTableEntity)retrievedResult.Result;
            Assert.AreEqual((int)HttpStatusCode.OK, retrievedResult.HttpStatusCode, "retrievedResult.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, retrievedResult.Result, "retrievedResult.Result = null");
            Assert.AreEqual("3", retrievedResult.Etag, "retrievedResult.Etag mismatch");
            Assert.AreEqual(false, dynCustomer._rtable_RowLock, "dynCustomer._rtable_RowLock mismatch");
            Assert.AreEqual(3, dynCustomer._rtable_Version, "dynCustomer._rtable_Version mismatch");
            Assert.AreEqual(false, dynCustomer._rtable_Tombstone, "dynCustomer._rtable_Tombstone mismatch");
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, dynCustomer._rtable_ViewId, "customer._rtable_ViewId mismatch");
            Assert.AreEqual(customer.PhoneNumber, dynCustomer.Properties["PhoneNumber"].StringValue, "dynCustomer.PhoneNumber mismatch");
            Assert.AreEqual("nongenretrieveandreplace@company.com", dynCustomer.Properties["Email"].StringValue, "dynCustomer.Email mismatch");
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

            TableOperation operation = TableOperation.InsertOrReplace(newCustomer);
            TableResult result = repTable.Execute(operation);
            Assert.AreNotEqual(null, result, "result = null");

            ReplicatedTableEntity row = (ReplicatedTableEntity)result.Result;

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
            newCustomer.ETag = result.Etag;
            operation = TableOperation.Delete(newCustomer);
            TableResult deleteResult = repTable.Execute(operation);

            Assert.AreNotEqual(null, deleteResult, "deleteResult = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, deleteResult.HttpStatusCode, "deleteResult.HttpStatusCode mismatch");
            Assert.IsNotNull(deleteResult.Result, "deleteResult.Result = null");

            // Retrieve
            Console.WriteLine("Calling TableOperation.Retrieve() after Delete() was called...");
            operation = TableOperation.Retrieve<CustomerEntity>(firstName, lastName);
            TableResult retrievedResult2 = repTable.Execute(operation);
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

            TableOperation operation = TableOperation.InsertOrReplace(newCustomer);
            TableResult result = repTable.Execute(operation);
            Assert.AreNotEqual(null, result, "result = null");

            ReplicatedTableEntity row = (ReplicatedTableEntity)result.Result;

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
            newCustomer.ETag = "*";
            operation = TableOperation.Delete(newCustomer);
            TableResult deleteResult = repTable.Execute(operation);

            Assert.AreNotEqual(null, deleteResult, "deleteResult = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, deleteResult.HttpStatusCode, "deleteResult.HttpStatusCode mismatch");
            Assert.IsNotNull(deleteResult.Result, "deleteResult.Result = null");

            // Retrieve
            Console.WriteLine("Calling TableOperation.Retrieve() after Delete() was called...");
            operation = TableOperation.Retrieve<CustomerEntity>(firstName, lastName);
            TableResult retrievedResult2 = repTable.Execute(operation);
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
            baseEntity.Properties.Add("prop1", new EntityProperty("value1"));
            TableResult result = currentTable.Execute(TableOperation.Insert(baseEntity));
            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, result, "result = null");
            Assert.AreEqual("1", result.Etag, "result.Etag mismatch");

            // Retrieve Entity & Verify Contents
            Console.WriteLine("Calling TableOperation.Retrieve()...");
            result = currentTable.Execute(TableOperation.Retrieve<DynamicReplicatedTableEntity>(baseEntity.PartitionKey, baseEntity.RowKey));
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
            DynamicReplicatedTableEntity mergeEntity = new DynamicReplicatedTableEntity(baseEntity.PartitionKey, baseEntity.RowKey) { ETag = result.Etag };
            mergeEntity.Properties.Add("prop2", new EntityProperty("value2"));
            result = currentTable.Execute(TableOperation.Merge(mergeEntity));

            Assert.AreNotEqual(null, result, "result = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "result.HttpStatusCode mismatch");
            Assert.AreNotEqual(null, result.Result, "result.Result = null");
            Assert.AreEqual("2", result.Etag, "result.Etag mismatch");

            row = (DynamicReplicatedTableEntity)result.Result;
            Assert.AreEqual(false, row._rtable_RowLock, "row._rtable_RowLock mismatch");
            Assert.AreEqual(2, row._rtable_Version, "row._rtable_Version mismatch");
            Assert.AreEqual(false, row._rtable_Tombstone, "row._rtable_Tombstone mismatch");
            Assert.AreEqual(this.rtableTestConfiguration.RTableInformation.ViewId, row._rtable_ViewId, "row._rtable_ViewId mismatch");

            // Retrieve Entity & Verify Contents
            Console.WriteLine("Calling TableOperation.Retrieve() after Merge() was called...");
            result = currentTable.Execute(TableOperation.Retrieve<DynamicReplicatedTableEntity>(baseEntity.PartitionKey, baseEntity.RowKey));
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
            Assert.AreEqual(2, retrievedEntity.Properties.Count, "retrievedEntity.Properties.Count mismatch");
            Assert.AreEqual(baseEntity.Properties["prop1"], retrievedEntity.Properties["prop1"], "Properties[prop1] mismatch");
            Assert.AreEqual(mergeEntity.Properties["prop2"], retrievedEntity.Properties["prop2"], "Properties[prop2] mismatch");

        }

        [Test(Description = "TableOperation InsetOrMerge")]
        public void RTableInsertOrMergeSync()
        {
            // Insert Entity
            ReplicatedTable currentTable = this.repTable;
            DynamicReplicatedTableEntity baseEntity = new DynamicReplicatedTableEntity("merge test02", "foo02");
            baseEntity.Properties.Add("prop1", new EntityProperty("value1"));
            Console.WriteLine("Calling TableOperation.Insert()...");
            TableResult result = currentTable.Execute(TableOperation.Insert(baseEntity));
            Assert.AreNotEqual(null, result, "Insert(): result = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "Insert(): result.HttpStatusCode mismatch");

            DynamicReplicatedTableEntity mergeEntity = new DynamicReplicatedTableEntity(baseEntity.PartitionKey, baseEntity.RowKey) { ETag = result.Etag };
            mergeEntity.Properties.Add("prop2", new EntityProperty("value2"));
            Console.WriteLine("Calling TableOperation.Merge()...");
            result = currentTable.Execute(TableOperation.Merge(mergeEntity));
            Assert.AreNotEqual(null, result, "Merge(): result = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "Merge(): result.HttpStatusCode mismatch");

            DynamicReplicatedTableEntity mergeEntity2 = new DynamicReplicatedTableEntity(baseEntity.PartitionKey, baseEntity.RowKey) { ETag = result.Etag };
            mergeEntity2.Properties.Add("prop3", new EntityProperty("value3"));
            Console.WriteLine("Calling TableOperation.InsertOrMerge()...");
            result = currentTable.Execute(TableOperation.InsertOrMerge(mergeEntity2));
            Assert.AreNotEqual(null, result, "InsertOrMerge(): result = null");
            Assert.AreEqual((int)HttpStatusCode.NoContent, result.HttpStatusCode, "InsertOrMerge(): result.HttpStatusCode mismatch");

            // Retrieve Entity & Verify Contents
            Console.WriteLine("Calling TableOperation.Retrieve()...");
            result = currentTable.Execute(TableOperation.Retrieve<DynamicReplicatedTableEntity>(baseEntity.PartitionKey, baseEntity.RowKey));
            Assert.AreNotEqual(null, result, "Retrieve(): result = null");
            DynamicReplicatedTableEntity retrievedEntity = result.Result as DynamicReplicatedTableEntity;

            Assert.IsNotNull(retrievedEntity, "retrievedEntity = null");
            Assert.AreEqual(3, retrievedEntity.Properties.Count, "Properties.Count mismatch");
            Assert.AreEqual(baseEntity.Properties["prop1"], retrievedEntity.Properties["prop1"], "Properties[prop1] mismatch");
            Assert.AreEqual(mergeEntity.Properties["prop2"], retrievedEntity.Properties["prop2"], "Properties[prop2] mismatch");
            Assert.AreEqual(mergeEntity2.Properties["prop3"], retrievedEntity.Properties["prop3"], "Properties[prop3] mismatch");
        }

        #endregion Table Operations Test Methods

        #region Helpers
        private static DynamicReplicatedTableEntity GenerateRandomEnitity(string pk)
        {
            DynamicReplicatedTableEntity ent = new DynamicReplicatedTableEntity();
            ent.Properties.Add("foo", new EntityProperty("bar"));

            ent.PartitionKey = pk;
            ent.RowKey = Guid.NewGuid().ToString();
            return ent;
        }

        #endregion Helpers
    }
}
