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
    using Microsoft.WindowsAzure.Storage.Table;
    using Microsoft.WindowsAzure.Storage;
    using NUnit.Framework;
    using System;
    using System.Linq;
    using System.Net;
    using System.Collections.Generic;

    [TestFixture]
    public class RTableBatchOperationTests : RTableLibraryTestBase
    {

        [TestFixtureSetUp]
        public void TestFixtureSetup()
        {
            this.LoadTestConfiguration();
            string tableName = this.GenerateRandomTableName();
            this.SetupRTableEnv(tableName);
        }

        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            base.DeleteAllRtableResources();
        }

        //
        // binaries.amd64fre\Gateway\UnitTests>        
        // runuts /fixture=Microsoft.WindowsAzure.Storage.RTableTest.RTableBatchOperationTests /verbose
        //
        #region Insert Test Methods
        [Test(Description="A test to check the DynamicReplicatedTableEntity constructor")]
        public void TableDynamicReplicatedTableEntityConstructor()
        {
            string pk = Guid.NewGuid().ToString();
            string rk = Guid.NewGuid().ToString();
            Dictionary<string, EntityProperty> properties = new Dictionary<string, EntityProperty>();
            properties.Add("foo", new EntityProperty("bar"));
            properties.Add("foo1", new EntityProperty("bar1"));

            DynamicReplicatedTableEntity ent = new DynamicReplicatedTableEntity(pk, rk, "*", properties);
            this.repTable.Execute(TableOperation.Insert(ent));
        }

        [Test(Description="A test to check the DynamicReplicatedTableEntity setter")]
        public void TableDynamicReplicatedTableEntitySetter()
        {
            string pk = Guid.NewGuid().ToString();
            string rk = Guid.NewGuid().ToString();
            Dictionary<string, EntityProperty> properties = new Dictionary<string, EntityProperty>();
            properties.Add("foo", new EntityProperty("bar"));
            properties.Add("foo1", new EntityProperty("bar1"));

            DynamicReplicatedTableEntity ent = new DynamicReplicatedTableEntity();
            ent.PartitionKey = pk;
            ent.RowKey = rk;
            ent.Properties = properties;
            ent.ETag = "*";
            ent.Timestamp = DateTimeOffset.MinValue;
            this.repTable.Execute(TableOperation.Insert(ent));
        }

        [Test(Description="A test to check EntityProperty")]
        public void TableEntityPropertyGenerator()
        {
            string pk = Guid.NewGuid().ToString();
            string rk = Guid.NewGuid().ToString();
            Dictionary<string, EntityProperty> properties = new Dictionary<string, EntityProperty>();
            EntityProperty boolEntity = EntityProperty.GeneratePropertyForBool(true);
            properties.Add("boolEntity", boolEntity);
            EntityProperty timeEntity = EntityProperty.GeneratePropertyForDateTimeOffset(DateTimeOffset.UtcNow);
            properties.Add("timeEntity", timeEntity);
            EntityProperty doubleEntity = EntityProperty.GeneratePropertyForDouble(0.1);
            properties.Add("doubleEntity", doubleEntity);
            EntityProperty guidEntity = EntityProperty.GeneratePropertyForGuid(Guid.NewGuid());
            properties.Add("guidEntity", guidEntity);
            EntityProperty intEntity = EntityProperty.GeneratePropertyForInt(1);
            properties.Add("intEntity", intEntity);
            EntityProperty longEntity = EntityProperty.GeneratePropertyForLong(1);
            properties.Add("longEntity", longEntity);
            EntityProperty stringEntity = EntityProperty.GeneratePropertyForString("string");
            properties.Add("stringEntity", stringEntity);

            DynamicReplicatedTableEntity ent = new DynamicReplicatedTableEntity(pk, rk, "*", properties);
            this.repTable.Execute(TableOperation.Insert(ent));
        }

        [Test(Description="A test to check EntityProperty setter")]
        public void TableEntityPropertySetter()
        {
            string pk = Guid.NewGuid().ToString();
            string rk = Guid.NewGuid().ToString();
            Dictionary<string, EntityProperty> properties = new Dictionary<string, EntityProperty>();
            EntityProperty boolEntity = EntityProperty.GeneratePropertyForBool(null);
            boolEntity.BooleanValue = true;
            properties.Add("boolEntity", boolEntity);

            EntityProperty timeEntity = EntityProperty.GeneratePropertyForDateTimeOffset(null);
            timeEntity.DateTimeOffsetValue = DateTimeOffset.UtcNow;
            properties.Add("timeEntity", timeEntity);

            EntityProperty doubleEntity = EntityProperty.GeneratePropertyForDouble(null);
            doubleEntity.DoubleValue = 0.1;
            properties.Add("doubleEntity", doubleEntity);

            EntityProperty guidEntity = EntityProperty.GeneratePropertyForGuid(null);
            guidEntity.GuidValue = Guid.NewGuid();
            properties.Add("guidEntity", guidEntity);

            EntityProperty intEntity = EntityProperty.GeneratePropertyForInt(null);
            intEntity.Int32Value = 1;
            properties.Add("intEntity", intEntity);

            EntityProperty longEntity = EntityProperty.GeneratePropertyForLong(null);
            longEntity.Int64Value = 1;
            properties.Add("longEntity", longEntity);

            EntityProperty stringEntity = EntityProperty.GeneratePropertyForString(null);
            stringEntity.StringValue = "string";
            properties.Add("stringEntity", stringEntity);

            DynamicReplicatedTableEntity ent = new DynamicReplicatedTableEntity(pk, rk, "*", properties);
            this.repTable.Execute(TableOperation.Insert(ent));
        }
       
        [Test(Description="A test to check batch insert functionality")]
        public void TableBatchInsertSync()
        {
            TableBatchOperation batch = new TableBatchOperation();
            string pk = Guid.NewGuid().ToString();

            for (int m = 0; m < 3; m++)
            {
                AddInsertToBatch(pk, batch);
            }

            // Add insert
            DynamicReplicatedTableEntity ent = GenerateRandomEnitity(pk);

            this.repTable.Execute(TableOperation.Insert(ent));

            // Add delete
            batch.Delete(ent);

            IList<TableResult> results = this.repTable.ExecuteBatch(batch);

            Assert.AreEqual(results.Count, 4);

            IEnumerator<TableResult> enumerator = results.GetEnumerator();
            enumerator.MoveNext();
            Assert.AreEqual((int)HttpStatusCode.NoContent, enumerator.Current.HttpStatusCode);
            enumerator.MoveNext();
            Assert.AreEqual((int)HttpStatusCode.NoContent, enumerator.Current.HttpStatusCode);
            enumerator.MoveNext();
            Assert.AreEqual((int)HttpStatusCode.NoContent, enumerator.Current.HttpStatusCode);
            enumerator.MoveNext();
            // delete
            Assert.AreEqual((int)HttpStatusCode.NoContent, enumerator.Current.HttpStatusCode);
        }

        [Test(Description="A test to check batch insert functionality when entity already exists")]
        public void TableBatchInsertFailSync()
        {
            IReplicatedTableEntity ent = GenerateRandomEnitity("foo");

            // add entity
            this.repTable.Execute(TableOperation.Insert(ent));

            TableBatchOperation batch = new TableBatchOperation();
            batch.Insert(ent);

            OperationContext opContext = new OperationContext();
            try
            {
                this.repTable.ExecuteBatch(batch, null, opContext);
                Assert.Fail("We should be reach here. Calling Insert() again should throw an exception.");
            }
            catch (StorageException)
            {
                TestHelper.ValidateResponse(opContext, 1, (int)HttpStatusCode.Conflict, new string[] { "EntityAlreadyExists" }, "The specified entity already exists");
            }
        }
        #endregion Insert Test Methods


        #region Insert or Merge
        // This test failed
        //retrievedEntity.Properties.Count=1. (Expecting 2)
        // Expected: 2
        // But was:  1
        [Test(Description="TableBatch Insert Or Merge")]
        public void TableBatchInsertOrMergeSync()
        {
            // Insert Or Merge with no pre-existing entity
            DynamicReplicatedTableEntity insertOrMergeEntity = new DynamicReplicatedTableEntity("batchInsertOrMerge entity", "foo");
            insertOrMergeEntity.Properties.Add("prop1", new EntityProperty("value1"));

            TableBatchOperation batch = new TableBatchOperation();
            batch.InsertOrMerge(insertOrMergeEntity);
            IList<TableResult> batchResultList = this.repTable.ExecuteBatch(batch);            

            // Retrieve Entity & Verify Contents
            TableResult result = this.repTable.Execute(TableOperation.Retrieve<DynamicReplicatedTableEntity>(insertOrMergeEntity.PartitionKey, insertOrMergeEntity.RowKey));
            DynamicReplicatedTableEntity retrievedEntity = result.Result as DynamicReplicatedTableEntity;
            Assert.IsNotNull(retrievedEntity);
            Assert.AreEqual(insertOrMergeEntity.Properties.Count, retrievedEntity.Properties.Count);

            DynamicReplicatedTableEntity mergeEntity = new DynamicReplicatedTableEntity(insertOrMergeEntity.PartitionKey, 
                                                                      insertOrMergeEntity.RowKey, 
                                                                      batchResultList[0].Etag,
                                                                      retrievedEntity.Properties);
            mergeEntity.Properties.Add("prop2", new EntityProperty("value2"));

            TableBatchOperation batch2 = new TableBatchOperation();
            batch2.InsertOrMerge(mergeEntity);
            this.repTable.ExecuteBatch(batch2);

            // Retrieve Entity & Verify Contents
            result = this.repTable.Execute(TableOperation.Retrieve<DynamicReplicatedTableEntity>(insertOrMergeEntity.PartitionKey, insertOrMergeEntity.RowKey));
            retrievedEntity = result.Result as DynamicReplicatedTableEntity;
            Assert.IsNotNull(retrievedEntity);
            Assert.AreEqual(2, retrievedEntity.Properties.Count, "retrievedEntity.Properties.Count={0}. (Expecting 2)", retrievedEntity.Properties.Count);

            Assert.IsNotNull(retrievedEntity);
            Assert.AreEqual(insertOrMergeEntity.Properties["prop1"], retrievedEntity.Properties["prop1"]);
            Assert.AreEqual(mergeEntity.Properties["prop2"], retrievedEntity.Properties["prop2"]);
        }
        #endregion Insert or Merge


        #region Delete        
        [Test(Description = "A test to check batch delete functionality. Retrieve existing entities using TableQuery")]
        public void TableBatchDeleteSyncUsingExecuteQuery()
        {
            // Insert Entity
            Console.WriteLine("Calling Insert()...");
            DynamicReplicatedTableEntity baseEntity = new DynamicReplicatedTableEntity("replace test", "foo");
            baseEntity.Properties.Add("prop1", new EntityProperty("value1"));
            this.repTable.Execute(TableOperation.Insert(baseEntity));


            TableBatchOperation batch = new TableBatchOperation();

            // Retrieve existing entities using TableQuery
            TableQuery<DynamicReplicatedTableEntity> query = new TableQuery<DynamicReplicatedTableEntity>().Where(
                TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, baseEntity.PartitionKey));
         
            IEnumerable<DynamicReplicatedTableEntity> allEntities = this.repTable.ExecuteQuery<DynamicReplicatedTableEntity>(query);
            foreach (DynamicReplicatedTableEntity entity in allEntities)
            {
                // Add delete
                batch.Delete(entity);
            }

            Console.WriteLine("Calling ExecuteBatch() to delete...");
            IList<TableResult> results = this.repTable.ExecuteBatch(batch);
            Assert.AreEqual(results.Count, 1);
            Assert.AreEqual(results.First().HttpStatusCode, (int)HttpStatusCode.NoContent);
        }

        [Test(Description = "A test to check batch delete functionality. Retrieve existing entities using Retrieve.")]
        public void TableBatchDeleteSyncUsingRetrieve()
        {
            // Insert Entity
            Console.WriteLine("Calling Insert()...");
            DynamicReplicatedTableEntity baseEntity = new DynamicReplicatedTableEntity("replace test", "foo");
            baseEntity.Properties.Add("prop1", new EntityProperty("value1"));
            this.repTable.Execute(TableOperation.Insert(baseEntity));

            // Retrieve existing entities using Retrieve
            Console.WriteLine("Calling Retrieve()...");
            TableResult result = this.repTable.Execute(TableOperation.Retrieve<DynamicReplicatedTableEntity>(baseEntity.PartitionKey, baseEntity.RowKey));
            DynamicReplicatedTableEntity retrievedEntity = result.Result as DynamicReplicatedTableEntity;

            Assert.IsNotNull(retrievedEntity);
            Assert.AreEqual(baseEntity.Properties.Count, retrievedEntity.Properties.Count);
            Assert.AreEqual(baseEntity.Properties["prop1"], retrievedEntity.Properties["prop1"]);

            TableBatchOperation batch = new TableBatchOperation();

            // Add delete
            batch.Delete(retrievedEntity);

            Console.WriteLine("Calling ExecuteBatch() to delete...");
            IList<TableResult> results = this.repTable.ExecuteBatch(batch);
            Assert.AreEqual(results.Count, 1);
            Assert.AreEqual(results.First().HttpStatusCode, (int)HttpStatusCode.NoContent);
        }

        [Test(Description="A test to check batch delete functionality")]
        public void TableBatchDeleteSync()
        {
            string pk = Guid.NewGuid().ToString();

            // Add insert
            DynamicReplicatedTableEntity ent = GenerateRandomEnitity(pk);
            this.repTable.Execute(TableOperation.Insert(ent));

            TableBatchOperation batch = new TableBatchOperation();

            // Add delete
            batch.Delete(ent);

            // success
            IList<TableResult> results = this.repTable.ExecuteBatch(batch);
            Assert.AreEqual(results.Count, 1);
            Assert.AreEqual(results.First().HttpStatusCode, (int)HttpStatusCode.NoContent);

            // fail - not found
            OperationContext opContext = new OperationContext();
            try
            {
                this.repTable.ExecuteBatch(batch, null, opContext);
                Assert.Fail();
            }
            catch (StorageException)
            {
                TestHelper.ValidateResponse(opContext, 1, (int)HttpStatusCode.NotFound, new string[] { "ResourceNotFound" }, "The specified resource does not exist.");
            }
        }
        #endregion Delete


        #region Merge
        [Test(Description="TableBatch Merge Sync")]
        public void TableBatchMergeSync()
        {
            // Insert Entity
            DynamicReplicatedTableEntity baseEntity = new DynamicReplicatedTableEntity("merge test", "foo");
            baseEntity.Properties.Add("prop1", new EntityProperty("value1"));
            this.repTable.Execute(TableOperation.Insert(baseEntity));

            DynamicReplicatedTableEntity mergeEntity = new DynamicReplicatedTableEntity(baseEntity.PartitionKey, baseEntity.RowKey) { ETag = baseEntity.ETag };
            mergeEntity.Properties.Add("prop2", new EntityProperty("value2"));

            TableBatchOperation batch = new TableBatchOperation();
            batch.Merge(mergeEntity);

            this.repTable.ExecuteBatch(batch);

            // Retrieve Entity & Verify Contents
            TableResult result = this.repTable.Execute(TableOperation.Retrieve<DynamicReplicatedTableEntity>(baseEntity.PartitionKey, baseEntity.RowKey));

            DynamicReplicatedTableEntity retrievedEntity = result.Result as DynamicReplicatedTableEntity;

            Assert.IsNotNull(retrievedEntity);
            Assert.AreEqual(2, retrievedEntity.Properties.Count);
            Assert.AreEqual(baseEntity.Properties["prop1"], retrievedEntity.Properties["prop1"]);
            Assert.AreEqual(mergeEntity.Properties["prop2"], retrievedEntity.Properties["prop2"]);
        }
        #endregion Merge


        #region Replace            
        [Test(Description = "TableBatch ReplaceSync")]
        public void TableBatchReplaceSync()
        {
            // Insert Entity
            Console.WriteLine("Calling Insert()...");
            DynamicReplicatedTableEntity baseEntity = new DynamicReplicatedTableEntity("replace test", "foo");
            baseEntity.Properties.Add("prop1", new EntityProperty("value1"));
            this.repTable.Execute(TableOperation.Insert(baseEntity));

            // ReplaceEntity
            DynamicReplicatedTableEntity replaceEntity = new DynamicReplicatedTableEntity(baseEntity.PartitionKey, baseEntity.RowKey) 
            { 
                ETag = "1"
            };
            replaceEntity.Properties.Add("prop2", new EntityProperty("value2"));

            Console.WriteLine("Calling Replace()...");
            TableBatchOperation batch = new TableBatchOperation();
            batch.Replace(replaceEntity);
            this.repTable.ExecuteBatch(batch);

            // Retrieve Entity & Verify Contents
            Console.WriteLine("Calling Retrieve()...");
            TableResult result = this.repTable.Execute(TableOperation.Retrieve<DynamicReplicatedTableEntity>(baseEntity.PartitionKey, baseEntity.RowKey));
            DynamicReplicatedTableEntity retrievedEntity = result.Result as DynamicReplicatedTableEntity;

            Assert.IsNotNull(retrievedEntity);
            Assert.AreEqual(replaceEntity.Properties.Count, retrievedEntity.Properties.Count);
            Assert.AreEqual(replaceEntity.Properties["prop2"], retrievedEntity.Properties["prop2"]);

            //
            // Replace() again
            //
            Console.WriteLine("Calling Replace() again, setting Etag to {0}", retrievedEntity._rtable_Version);
            replaceEntity = new DynamicReplicatedTableEntity(baseEntity.PartitionKey, baseEntity.RowKey) 
            {                 
                ETag = retrievedEntity._rtable_Version.ToString()
            };
            replaceEntity.Properties.Add("prop3", new EntityProperty("value3"));
            batch = new TableBatchOperation();
            batch.Replace(replaceEntity);
            this.repTable.ExecuteBatch(batch);

            Console.WriteLine("Calling Retrieve()...");
            result = this.repTable.Execute(TableOperation.Retrieve<DynamicReplicatedTableEntity>(baseEntity.PartitionKey, baseEntity.RowKey));
            retrievedEntity = result.Result as DynamicReplicatedTableEntity;

            Assert.IsNotNull(retrievedEntity);

            Console.WriteLine("{0}", retrievedEntity.ToString());
            Assert.AreEqual(replaceEntity.Properties.Count, retrievedEntity.Properties.Count);
            Assert.AreEqual(replaceEntity.Properties["prop3"], retrievedEntity.Properties["prop3"]);
        }
        #endregion Replace

        #region Batch With All Supported Operations        
        [Test(Description="A test to check batch with all supported operations")]
        public void TableBatchAllSupportedOperationsSync()
        {
            TableBatchOperation batch = new TableBatchOperation();
            string pk = Guid.NewGuid().ToString();

            // insert
            batch.Insert(GenerateRandomEnitity(pk));

            // delete
            {
                DynamicReplicatedTableEntity entity = GenerateRandomEnitity(pk);
                this.repTable.Execute(TableOperation.Insert(entity));
                batch.Delete(entity);
            }

            // replace
            {
                DynamicReplicatedTableEntity entity = GenerateRandomEnitity(pk);
                this.repTable.Execute(TableOperation.Insert(entity));
                batch.Replace(entity);
            }

            // insert or replace
            {
                DynamicReplicatedTableEntity entity = GenerateRandomEnitity(pk);
                this.repTable.Execute(TableOperation.Insert(entity));
                batch.InsertOrReplace(entity);
            }

            // merge
            {
                DynamicReplicatedTableEntity entity = GenerateRandomEnitity(pk);
                this.repTable.Execute(TableOperation.Insert(entity));
                batch.Merge(entity);
            }

            // insert or merge
            {
                DynamicReplicatedTableEntity entity = GenerateRandomEnitity(pk);
                this.repTable.Execute(TableOperation.Insert(entity));
                batch.InsertOrMerge(entity);
            }

            IList<TableResult> results = this.repTable.ExecuteBatch(batch);

            Assert.AreEqual(results.Count, 6);
            IEnumerator<TableResult> enumerator = results.GetEnumerator();
            for (int i = 0; i < results.Count; i++)
            {
                enumerator.MoveNext();
                Assert.AreEqual((int)HttpStatusCode.NoContent, enumerator.Current.HttpStatusCode, "HttpStatusCode mismatch i={0}", i);
            }
        }        
        #endregion Batch With All Supported Operations


        #region Retrieve
        [Test(Description="A test to check batch retrieve functionality")]
        public void TableBatchRetrieveSync()
        {
            string pk = Guid.NewGuid().ToString();

            // Add insert
            DynamicReplicatedTableEntity sendEnt = GenerateRandomEnitity(pk);

            // generate a set of properties for all supported Types
            sendEnt.Properties = new ComplexIEntity().WriteEntity(null);

            TableBatchOperation batch = new TableBatchOperation();
            batch.Retrieve<DynamicReplicatedTableEntity>(sendEnt.PartitionKey, sendEnt.RowKey);

            // not found
            IList<TableResult> results = this.repTable.ExecuteBatch(batch);
            Assert.AreEqual(results.Count, 1);
            Assert.AreEqual(results.First().HttpStatusCode, (int)HttpStatusCode.NotFound);
            Assert.IsNull(results.First().Result);
            Assert.IsNull(results.First().Etag);

            // insert entity
            this.repTable.Execute(TableOperation.Insert(sendEnt));

            // Success
            results = this.repTable.ExecuteBatch(batch);
            Assert.AreEqual(results.Count, 1);
            Assert.AreEqual(results.First().HttpStatusCode, (int)HttpStatusCode.OK);
            DynamicReplicatedTableEntity retrievedEntity = results.First().Result as DynamicReplicatedTableEntity;

            // Validate entity
            Assert.AreEqual(sendEnt["String"], retrievedEntity["String"]);
            Assert.AreEqual(sendEnt["Int64"], retrievedEntity["Int64"]);
            Assert.AreEqual(sendEnt["Int64N"], retrievedEntity["Int64N"]);
            Assert.AreEqual(sendEnt["LongPrimitive"], retrievedEntity["LongPrimitive"]);
            Assert.AreEqual(sendEnt["LongPrimitiveN"], retrievedEntity["LongPrimitiveN"]);
            Assert.AreEqual(sendEnt["Int32"], retrievedEntity["Int32"]);
            Assert.AreEqual(sendEnt["Int32N"], retrievedEntity["Int32N"]);
            Assert.AreEqual(sendEnt["IntegerPrimitive"], retrievedEntity["IntegerPrimitive"]);
            Assert.AreEqual(sendEnt["IntegerPrimitiveN"], retrievedEntity["IntegerPrimitiveN"]);
            Assert.AreEqual(sendEnt["Guid"], retrievedEntity["Guid"]);
            Assert.AreEqual(sendEnt["GuidN"], retrievedEntity["GuidN"]);
            Assert.AreEqual(sendEnt["Double"], retrievedEntity["Double"]);
            Assert.AreEqual(sendEnt["DoubleN"], retrievedEntity["DoubleN"]);
            Assert.AreEqual(sendEnt["DoublePrimitive"], retrievedEntity["DoublePrimitive"]);
            Assert.AreEqual(sendEnt["DoublePrimitiveN"], retrievedEntity["DoublePrimitiveN"]);
            Assert.AreEqual(sendEnt["BinaryPrimitive"], retrievedEntity["BinaryPrimitive"]);
            Assert.AreEqual(sendEnt["Binary"], retrievedEntity["Binary"]);
            Assert.AreEqual(sendEnt["BoolPrimitive"], retrievedEntity["BoolPrimitive"]);
            Assert.AreEqual(sendEnt["BoolPrimitiveN"], retrievedEntity["BoolPrimitiveN"]);
            Assert.AreEqual(sendEnt["Bool"], retrievedEntity["Bool"]);
            Assert.AreEqual(sendEnt["BoolN"], retrievedEntity["BoolN"]);
            Assert.AreEqual(sendEnt["DateTimeOffsetN"], retrievedEntity["DateTimeOffsetN"]);
            Assert.AreEqual(sendEnt["DateTimeOffset"], retrievedEntity["DateTimeOffset"]);
            Assert.AreEqual(sendEnt["DateTime"], retrievedEntity["DateTime"]);
            Assert.AreEqual(sendEnt["DateTimeN"], retrievedEntity["DateTimeN"]);
        }
        #endregion Retrieve


        #region Empty Keys Tests
        [Test(Description = "TableBatchOperations with Empty keys")]
        public void TableBatchOperationsWithEmptyKeys()
        {
            // Insert Entity
            DynamicReplicatedTableEntity ent = new DynamicReplicatedTableEntity() { PartitionKey = "", RowKey = "" };
            ent.Properties.Add("foo2", new EntityProperty("bar2"));
            ent.Properties.Add("foo", new EntityProperty("bar"));
            TableBatchOperation batch = new TableBatchOperation();
            batch.Insert(ent);
            this.repTable.ExecuteBatch(batch);

            // Retrieve Entity
            TableBatchOperation retrieveBatch = new TableBatchOperation();
            retrieveBatch.Retrieve<DynamicReplicatedTableEntity>(ent.PartitionKey, ent.RowKey);
            TableResult result = this.repTable.ExecuteBatch(retrieveBatch).First();

            DynamicReplicatedTableEntity retrievedEntity = result.Result as DynamicReplicatedTableEntity;
            Assert.IsNotNull(retrievedEntity);
            Assert.AreEqual(ent.PartitionKey, retrievedEntity.PartitionKey);
            Assert.AreEqual(ent.RowKey, retrievedEntity.RowKey);
            Assert.AreEqual(ent.Properties.Count, retrievedEntity.Properties.Count);
            Assert.AreEqual(ent.Properties["foo"].StringValue, retrievedEntity.Properties["foo"].StringValue);
            Assert.AreEqual(ent.Properties["foo"], retrievedEntity.Properties["foo"]);
            Assert.AreEqual(ent.Properties["foo2"].StringValue, retrievedEntity.Properties["foo2"].StringValue);
            Assert.AreEqual(ent.Properties["foo2"], retrievedEntity.Properties["foo2"]);

            // InsertOrMerge
            DynamicReplicatedTableEntity insertOrMergeEntity = new DynamicReplicatedTableEntity(ent.PartitionKey, ent.RowKey);
            insertOrMergeEntity.Properties.Add("foo3", new EntityProperty("value"));
            batch = new TableBatchOperation();
            batch.InsertOrMerge(insertOrMergeEntity);
            this.repTable.ExecuteBatch(batch);

            result = this.repTable.ExecuteBatch(retrieveBatch).First();
            retrievedEntity = result.Result as DynamicReplicatedTableEntity;
            Assert.IsNotNull(retrievedEntity);
            Assert.AreEqual(insertOrMergeEntity.Properties["foo3"], retrievedEntity.Properties["foo3"]);

            // InsertOrReplace
            DynamicReplicatedTableEntity insertOrReplaceEntity = new DynamicReplicatedTableEntity(ent.PartitionKey, ent.RowKey);
            insertOrReplaceEntity.Properties.Add("prop2", new EntityProperty("otherValue"));
            batch = new TableBatchOperation();
            batch.InsertOrReplace(insertOrReplaceEntity);
            this.repTable.ExecuteBatch(batch);

            result = this.repTable.ExecuteBatch(retrieveBatch).First();
            retrievedEntity = result.Result as DynamicReplicatedTableEntity;
            Assert.IsNotNull(retrievedEntity);
            Assert.AreEqual(1, retrievedEntity.Properties.Count);
            Assert.AreEqual(insertOrReplaceEntity.Properties["prop2"], retrievedEntity.Properties["prop2"]);

            // Merge
            DynamicReplicatedTableEntity mergeEntity = new DynamicReplicatedTableEntity(retrievedEntity.PartitionKey, retrievedEntity.RowKey) { ETag = retrievedEntity.ETag };
            mergeEntity.Properties.Add("mergeProp", new EntityProperty("merged"));
            batch = new TableBatchOperation();
            batch.Merge(mergeEntity);
            this.repTable.ExecuteBatch(batch);

            // Retrieve Entity & Verify Contents
            result = this.repTable.ExecuteBatch(retrieveBatch).First();
            retrievedEntity = result.Result as DynamicReplicatedTableEntity;

            Assert.IsNotNull(retrievedEntity);
            Assert.AreEqual(mergeEntity.Properties["mergeProp"], retrievedEntity.Properties["mergeProp"]);

            // Replace
            DynamicReplicatedTableEntity replaceEntity = new DynamicReplicatedTableEntity(ent.PartitionKey, ent.RowKey) { ETag = retrievedEntity.ETag };
            replaceEntity.Properties.Add("replaceProp", new EntityProperty("replace"));
            batch = new TableBatchOperation();
            batch.Replace(replaceEntity);
            this.repTable.ExecuteBatch(batch);

            // Retrieve Entity & Verify Contents
            result = this.repTable.ExecuteBatch(retrieveBatch).First();
            retrievedEntity = result.Result as DynamicReplicatedTableEntity;
            Assert.IsNotNull(retrievedEntity);
            Assert.AreEqual(replaceEntity.Properties.Count, retrievedEntity.Properties.Count);
            Assert.AreEqual(replaceEntity.Properties["replaceProp"], retrievedEntity.Properties["replaceProp"]);

            // Delete Entity
            batch = new TableBatchOperation();
            batch.Delete(retrievedEntity);
            this.repTable.ExecuteBatch(batch);

            // Retrieve Entity
            result = this.repTable.ExecuteBatch(retrieveBatch).First();
            Assert.IsNull(result.Result);
        }
        #endregion Empty Keys Tests


        #region Bulk Insert
        [Test(Description="A test to peform batch insert and delete with batch size of 1")]       
        public void TableBatchInsert1()
        {
            InsertAndDeleteBatchWithNEntities(1);
        }

        [Test(Description="A test to peform batch insert and delete with batch size of 10")]
        public void TableBatchInsert10()
        {
            InsertAndDeleteBatchWithNEntities(10);
        }

        [Test(Description="A test to peform batch insert and delete with batch size of 99")]
        public void TableBatchInsert99()
        {
            InsertAndDeleteBatchWithNEntities(99);
        }

        [Test(Description="A test to peform batch insert and delete with batch size of 100")]
        public void TableBatchInsert100()
        {
            InsertAndDeleteBatchWithNEntities(100);
        }
        #endregion Bulk Insert


        #region Bulk Upsert

        [Test(Description="A test to peform batch InsertOrMerge with batch size of 1")]
        public void TableBatchInsertOrMerge1()
        {
            InsertOrMergeBatchWithNEntities(1);
        }

        [Test(Description = "A test to peform batch InsertOrMerge with batch size of 10")]
        public void TableBatchInsertOrMerge10()
        {
            InsertOrMergeBatchWithNEntities(10);
        }

        [Test(Description = "A test to peform batch InsertOrMerge with batch size of 99")]
        public void TableBatchInsertOrMerge99()
        {
            InsertOrMergeBatchWithNEntities(99);
        }

        [Test(Description = "A test to peform batch InsertOrMerge with batch size of 100")]
        public void TableBatchInsertOrMerge100()
        {
            InsertOrMergeBatchWithNEntities(100);
        }

        #endregion Bulk Upsert


        #region Boundary Conditions

        [Test(Description = "Ensure that adding null to the batch will throw")]
        public void TableBatchAddNullShouldThrow()
        {
            TableBatchOperation batch = new TableBatchOperation();
            try
            {
                batch.Add(null);
                Assert.Fail();
            }
            catch (ArgumentNullException)
            {
                // no op
            }
            catch (Exception)
            {
                Assert.Fail();
            }

            try
            {
                batch.Insert(0, null);
                Assert.Fail();
            }
            catch (ArgumentNullException)
            {
                // no op
            }
            catch (Exception)
            {
                Assert.Fail();
            }
        }

        [Test(Description = "Ensure that adding multiple queries to the batch will throw")]
        public void TableBatchAddMultiQueryShouldThrow()
        {
            TableBatchOperation batch = new TableBatchOperation();
            batch.Retrieve("foo", "bar");
            try
            {
                batch.Retrieve("foo", "bar2");
                Assert.Fail();
            }
            catch (ArgumentException)
            {
                // no op
            }
            catch (Exception)
            {
                Assert.Fail();
            }
        }

        [Test(Description = "Ensure that a batch that contains multiple operations on the same entity fails")]
        public void TableBatchWithMultipleOperationsOnSameEntityShouldFail()
        {
            TableBatchOperation batch = new TableBatchOperation();
            string pk = Guid.NewGuid().ToString();

            // Add entity 0
            ITableEntity first = GenerateRandomEnitity(pk);
            batch.Insert(first);

            // Add entities 1 - 98
            for (int m = 1; m < 99; m++)
            {
                batch.Insert(GenerateRandomEnitity(pk));
            }

            // Insert Duplicate of entity 0
            batch.Insert(first);

            OperationContext opContext = new OperationContext();
            try
            {
                this.repTable.ExecuteBatch(batch, null, opContext);
                Assert.Fail();
            }
            catch (StorageException)
            {
                TestHelper.ValidateResponse(opContext, 1, (int)HttpStatusCode.BadRequest, new string[] { "InvalidInput" }, new string[] { "99:One of the request inputs is not valid." }, false);
            }
        }

        [Test(Description = "Ensure that a batch with over 100 entities will throw")]
        public void TableBatchOver100EntitiesShouldThrow()
        {
            TableBatchOperation batch = new TableBatchOperation();
            string pk = Guid.NewGuid().ToString();
            for (int m = 0; m < 101; m++)
            {
                batch.Insert(GenerateRandomEnitity(pk));
            }

            OperationContext opContext = new OperationContext();
            try
            {
                this.repTable.ExecuteBatch(batch, null, opContext);
                Assert.Fail();
            }
            catch (StorageException)
            {
                TestHelper.ValidateResponse(opContext, 1, (int)HttpStatusCode.BadRequest, new string[] { "InvalidInput" }, "One of the request inputs is not valid.");
            }
        }

        [Test(Description = "Ensure that a batch with entity over 1 MB will throw")]
        public void TableBatchEntityOver1MBShouldThrow()
        {
            TableBatchOperation batch = new TableBatchOperation();
            string pk = Guid.NewGuid().ToString();

            DynamicReplicatedTableEntity ent = GenerateRandomEnitity(pk);
            ent.Properties.Add("binary", EntityProperty.GeneratePropertyForByteArray(new byte[1024 * 1024]));
            batch.Insert(ent);

            OperationContext opContext = new OperationContext();
            try
            {
                this.repTable.ExecuteBatch(batch, null, opContext);
                Assert.Fail();
            }
            catch (StorageException)
            {
                TestHelper.ValidateResponse(opContext, 2, (int)HttpStatusCode.BadRequest, new string[] { "EntityTooLarge" }, "The entity is larger than the maximum allowed size (1MB).");
            }
        }
        
        [Test(Description = "Ensure that a batch over 4 MB will throw")]
        public void TableBatchOver4MBShouldThrow()
        {
            TableBatchOperation batch = new TableBatchOperation();
            string pk = Guid.NewGuid().ToString();

            for (int m = 0; m < 65; m++)
            {
                DynamicReplicatedTableEntity ent = GenerateRandomEnitity(pk);

                // Maximum Entity size is 64KB
                ent.Properties.Add("binary", EntityProperty.GeneratePropertyForByteArray(new byte[64 * 1024]));
                batch.Insert(ent);
            }

            OperationContext opContext = new OperationContext();
            try
            {
                this.repTable.ExecuteBatch(batch, null, opContext);
                Assert.Fail();
            }
            catch (StorageException)
            {
                //
                // ###XXX: NEW SOURCE CODES: The commented out codes were the original codes
                //
                //TestHelper.ValidateResponse(opContext, 1, (int)HttpStatusCode.BadRequest, new string[] { "ContentLengthExceeded" }, "The content length for the requested operation has exceeded the limit (4MB).");
                TestHelper.ValidateResponse(
                    opContext, 
                    2, 
                    (int)HttpStatusCode.RequestEntityTooLarge, 
                    new string[] { "RequestBodyTooLarge" }, 
                    "The request body is too large and exceeds the maximum permissible limit"
                    );
            }
        }

        [Test(Description = "Ensure that a query and one more operation will throw")]
        public void TableBatchAddQueryAndOneMoreOperationShouldThrow()
        {
            TableBatchOperation batch = new TableBatchOperation();
            TableOperation operation = TableOperation.Retrieve<DynamicReplicatedTableEntity>("foo", "bar");

            try
            {
                batch.Add(operation);
                Assert.IsTrue(batch.Contains(operation));
                batch.Add(TableOperation.Insert(GenerateRandomEnitity("foo")));
                Assert.Fail();
            }
            catch (ArgumentException)
            {
                // no op
            }
            catch (Exception)
            {
                Assert.Fail();
            }

            batch.Clear();
            Assert.IsFalse(batch.Contains(operation));

            try
            {
                batch.Add(TableOperation.Insert(GenerateRandomEnitity("foo")));
                batch.Add(TableOperation.Retrieve<DynamicReplicatedTableEntity>("foo", "bar"));
                Assert.Fail();
            }
            catch (ArgumentException)
            {
                // no op
            }
            catch (Exception)
            {
                Assert.Fail();
            }

            batch.Clear();

            try
            {
                batch.Add(TableOperation.Retrieve<DynamicReplicatedTableEntity>("foo", "bar"));
                batch.Insert(0, TableOperation.Insert(GenerateRandomEnitity("foo")));

                Assert.Fail();
            }
            catch (ArgumentException)
            {
                // no op
            }
            catch (Exception)
            {
                Assert.Fail();
            }

            try
            {
                batch.Insert(0, TableOperation.Insert(GenerateRandomEnitity("foo")));
                batch.Insert(0, TableOperation.Retrieve<DynamicReplicatedTableEntity>("foo", "bar"));

                Assert.Fail();
            }
            catch (ArgumentException)
            {
                // no op
            }
            catch (Exception)
            {
                Assert.Fail();
            }

        }

        [Test(Description = "Ensure that empty batch will throw")]
        public void TableBatchEmptyBatchShouldThrow()
        {
            TableBatchOperation batch = new TableBatchOperation();
            TestHelper.ExpectedException<InvalidOperationException>(
                () => this.repTable.ExecuteBatch(batch),
                "Empty batch operation should fail");
        }

        [Test(Description = "Ensure that a given batch only allows entities with the same partitionkey")]
        public void TableBatchLockToPartitionKey()
        {
            TableBatchOperation batch = new TableBatchOperation();
            batch.Add(TableOperation.Insert(GenerateRandomEnitity("foo")));

            try
            {
                batch.Add(TableOperation.Insert(GenerateRandomEnitity("foo2")));
                Assert.Fail();
            }
            catch (ArgumentException)
            {
                // no op
            }
            catch (Exception)
            {
                Assert.Fail();
            }

            // should reset pk lock
            batch.RemoveAt(0);
            batch.Add(TableOperation.Insert(GenerateRandomEnitity("foo2")));

            try
            {
                batch.Add(TableOperation.Insert(GenerateRandomEnitity("foo2")));
            }
            catch (ArgumentException)
            {
                Assert.Fail();
            }
            catch (Exception)
            {
                Assert.Fail();
            }
        }

        [Test(Description="Ensure that a batch with an entity property over 255 chars will throw")]
        public void TableBatchWithPropertyOver255CharsShouldThrow()
        {
            TableBatchOperation batch = new TableBatchOperation();
            string pk = Guid.NewGuid().ToString();

            string propName = new string('a', 256);  // propName has 256 chars

            DynamicReplicatedTableEntity ent = new DynamicReplicatedTableEntity("foo", "bar");
            ent.Properties.Add(propName, new EntityProperty("propbar"));
            batch.Insert(ent);

            OperationContext opContext = new OperationContext();
            try
            {
                this.repTable.ExecuteBatch(batch, null, opContext);
                Assert.Fail();
            }
            catch (StorageException)
            {
                TestHelper.ValidateResponse(opContext, 2, (int)HttpStatusCode.BadRequest, new string[] { "PropertyNameTooLong" }, "The property name exceeds the maximum allowed length (255).");
            }
        }
        #endregion Boundary Conditions



        #region Helpers
        private static void AddInsertToBatch(string pk, TableBatchOperation batch)
        {
            batch.Insert(GenerateRandomEnitity(pk));
        }

        private static DynamicReplicatedTableEntity GenerateRandomEnitity(string pk)
        {
            DynamicReplicatedTableEntity ent = new DynamicReplicatedTableEntity();
            ent.Properties.Add("foo", new EntityProperty("bar"));

            ent.PartitionKey = pk;
            ent.RowKey = Guid.NewGuid().ToString();
            return ent;
        }


        private void InsertAndDeleteBatchWithNEntities(int n)
        {
            TableBatchOperation batch = new TableBatchOperation();
            string pk = Guid.NewGuid().ToString();
            for (int m = 0; m < n; m++)
            {
                batch.Insert(GenerateRandomEnitity(pk));
            }

            IList<TableResult> results = this.repTable.ExecuteBatch(batch);

            TableBatchOperation delBatch = new TableBatchOperation();

            foreach (TableResult res in results)
            {
                delBatch.Delete((ITableEntity)res.Result);
                Assert.AreEqual((int)HttpStatusCode.NoContent, res.HttpStatusCode, "Mismatch in results");
            }

            IList<TableResult> delResults = this.repTable.ExecuteBatch(delBatch);
            foreach (TableResult res in delResults)
            {
                Assert.AreEqual((int)HttpStatusCode.NoContent, res.HttpStatusCode, "Mismatch in delResults");
            }
        }


        private void InsertOrMergeBatchWithNEntities(int n)
        {
            string pk = Guid.NewGuid().ToString();

            TableBatchOperation insertBatch = new TableBatchOperation();
            TableBatchOperation mergeBatch = new TableBatchOperation();
            TableBatchOperation delBatch = new TableBatchOperation();

            for (int m = 0; m < n; m++)
            {
                insertBatch.InsertOrMerge(GenerateRandomEnitity(pk));
            }

            IList<TableResult> results = this.repTable.ExecuteBatch(insertBatch);
            foreach (TableResult res in results)
            {
                Assert.AreEqual(res.HttpStatusCode, (int)HttpStatusCode.NoContent);

                // update entity and add to merge batch
                DynamicReplicatedTableEntity ent = res.Result as DynamicReplicatedTableEntity;
                ent.Properties.Add("foo2", new EntityProperty("bar2"));
                mergeBatch.InsertOrMerge(ent);

            }

            // execute insertOrMerge batch, this time entities exist
            IList<TableResult> mergeResults = this.repTable.ExecuteBatch(mergeBatch);

            foreach (TableResult res in mergeResults)
            {
                Assert.AreEqual(res.HttpStatusCode, (int)HttpStatusCode.NoContent);

                // Add to delete batch
                delBatch.Delete((ITableEntity)res.Result);
            }

            IList<TableResult> delResults = this.repTable.ExecuteBatch(delBatch);
            foreach (TableResult res in delResults)
            {
                Assert.AreEqual(res.HttpStatusCode, (int)HttpStatusCode.NoContent);
            }
        }

        private void POCOInsertAndDeleteBatchWithNEntities(int n)
        {
            TableBatchOperation batch = new TableBatchOperation();
            string pk = Guid.NewGuid().ToString();
            for (int m = 0; m < n; m++)
            {
                batch.Insert(new BaseEntity(pk, Guid.NewGuid().ToString()));
            }

            IList<TableResult> results = this.repTable.ExecuteBatch(batch);

            TableBatchOperation delBatch = new TableBatchOperation();

            foreach (TableResult res in results)
            {
                delBatch.Delete((ITableEntity)res.Result);
                Assert.AreEqual(res.HttpStatusCode, (int)HttpStatusCode.Created);
            }

            IList<TableResult> delResults = this.repTable.ExecuteBatch(delBatch);
            foreach (TableResult res in delResults)
            {
                Assert.AreEqual(res.HttpStatusCode, (int)HttpStatusCode.NoContent);
            }
        }
        #endregion
    }
}
