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
    using NUnit.Framework;
    using System;
    using System.Linq;
    using System.Net;
    using System.Collections.Generic;
    using global::Azure;
    using global::Azure.Data.Tables;

    [TestFixture]
    public class RTableBatchOperationTests : RTableLibraryTestBase
    {

        [OneTimeSetUp]
        public void TestFixtureSetup()
        {
            this.LoadTestConfiguration();
            string tableName = this.GenerateRandomTableName();
            this.SetupRTableEnv(tableName);
        }

        [OneTimeTearDown]
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
            Dictionary<string, object> properties = new Dictionary<string, object>();
            properties.Add("foo", "bar");
            properties.Add("foo1", "bar1");

            DynamicReplicatedTableEntity ent = new DynamicReplicatedTableEntity(pk, rk, "*", properties);
            this.repTable.Insert(ent);
        }

        [Test(Description="A test to check the DynamicReplicatedTableEntity setter")]
        public void TableDynamicReplicatedTableEntitySetter()
        {
            string pk = Guid.NewGuid().ToString();
            string rk = Guid.NewGuid().ToString();
            Dictionary<string, object> properties = new Dictionary<string, object>();
            properties.Add("foo", "bar");
            properties.Add("foo1", "bar1");

            DynamicReplicatedTableEntity ent = new DynamicReplicatedTableEntity();
            ent.PartitionKey = pk;
            ent.RowKey = rk;
            ent.Properties = properties;
            ent.ETag = ETag.All;
            ent.Timestamp = DateTimeOffset.MinValue;
            this.repTable.Insert(ent);
        }

        [Test(Description="A test to check EntityProperty")]
        public void TableEntityPropertyGenerator()
        {
            string pk = Guid.NewGuid().ToString();
            string rk = Guid.NewGuid().ToString();
            Dictionary<string, object> properties = new Dictionary<string, object>();
            properties.Add("boolEntity", true);
            properties.Add("timeEntity", DateTimeOffset.UtcNow);
            properties.Add("doubleEntity", 0.1);
            properties.Add("guidEntity", Guid.NewGuid());
            properties.Add("intEntity", 1);
            properties.Add("longEntity", (long)1);
            properties.Add("stringEntity", "string");

            DynamicReplicatedTableEntity ent = new DynamicReplicatedTableEntity(pk, rk, "*", properties);
            this.repTable.Insert(ent);
        }
       
        [Test(Description="A test to check batch insert functionality")]
        public void TableBatchInsertSync()
        {
            IList<TableTransactionAction> batch = new List<TableTransactionAction>();
            string pk = Guid.NewGuid().ToString();

            for (int m = 0; m < 3; m++)
            {
                AddInsertToBatch(pk, batch);
            }

            // Add insert
            DynamicReplicatedTableEntity ent = GenerateRandomEnitity(pk);

            this.repTable.Insert(ent);

            // Add delete
            batch.Add(new TableTransactionAction(TableTransactionActionType.Delete, ent, ent.ETag));

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
            var result = this.repTable.Insert(ent);

            IList<TableTransactionAction> batch = new List<TableTransactionAction>
            {
                new TableTransactionAction(TableTransactionActionType.Add, ent, new ETag(result.Etag))
            };

            try
            {
                this.repTable.ExecuteBatch(batch);
                Assert.Fail("We should be reach here. Calling Insert() again should throw an exception.");
            }
            catch (RequestFailedException e)
            {
                TestHelper.ValidateResponse(e, 1, (int)HttpStatusCode.Conflict, new string[] { "EntityAlreadyExists" }, "The specified entity already exists");
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
            insertOrMergeEntity.Properties.Add("prop1", "value1");

            IList<TableTransactionAction> batch = new List<TableTransactionAction>
            {
                new TableTransactionAction(TableTransactionActionType.UpsertMerge, insertOrMergeEntity, insertOrMergeEntity.ETag)
            };
            IList<TableResult> batchResultList = this.repTable.ExecuteBatch(batch);            

            // Retrieve Entity & Verify Contents
            TableResult result = this.repTable.Retrieve(insertOrMergeEntity.PartitionKey, insertOrMergeEntity.RowKey);
            DynamicReplicatedTableEntity retrievedEntity = result.Result as DynamicReplicatedTableEntity;
            Assert.IsNotNull(retrievedEntity);
            Assert.AreEqual(insertOrMergeEntity.Properties.Count, retrievedEntity.Properties.Count);

            DynamicReplicatedTableEntity mergeEntity = new DynamicReplicatedTableEntity(insertOrMergeEntity.PartitionKey, 
                                                                      insertOrMergeEntity.RowKey, 
                                                                      batchResultList[0].Etag,
                                                                      retrievedEntity.Properties);
            mergeEntity.Properties.Add("prop2", "value2");

            IList<TableTransactionAction> batch2 = new List<TableTransactionAction>
            {
                new TableTransactionAction(TableTransactionActionType.UpsertMerge, mergeEntity, mergeEntity.ETag)
            };
            this.repTable.ExecuteBatch(batch2);

            // Retrieve Entity & Verify Contents
            result = this.repTable.Retrieve(insertOrMergeEntity.PartitionKey, insertOrMergeEntity.RowKey);
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
            baseEntity.Properties.Add("prop1", "value1");
            this.repTable.Insert(baseEntity);
            IList<TableTransactionAction> batch = new List<TableTransactionAction>();

            // Retrieve existing entities using TableQuery
            IEnumerable<DynamicReplicatedTableEntity> allEntities = this.repTable.ExecuteQuery<DynamicReplicatedTableEntity>(e =>
                e.PartitionKey == baseEntity.PartitionKey);
            foreach (DynamicReplicatedTableEntity entity in allEntities)
            {
                // Add delete
                batch.Add(new TableTransactionAction(TableTransactionActionType.Delete, entity, entity.ETag));
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
            baseEntity.Properties.Add("prop1", "value1");
            this.repTable.Insert(baseEntity);

            // Retrieve existing entities using Retrieve
            Console.WriteLine("Calling Retrieve()...");
            TableResult result = this.repTable.Retrieve(baseEntity.PartitionKey, baseEntity.RowKey);
            DynamicReplicatedTableEntity retrievedEntity = result.Result as DynamicReplicatedTableEntity;

            Assert.IsNotNull(retrievedEntity);
            Assert.AreEqual(baseEntity.Properties.Count, retrievedEntity.Properties.Count);
            Assert.AreEqual(baseEntity.Properties["prop1"], retrievedEntity.Properties["prop1"]);

            IList<TableTransactionAction> batch = new List<TableTransactionAction>();

            // Add delete
            batch.Add(new TableTransactionAction(TableTransactionActionType.Delete, retrievedEntity, retrievedEntity.ETag));


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
            this.repTable.Insert(ent);

            IList<TableTransactionAction> batch = new List<TableTransactionAction>
            {
                // Add delete
                new TableTransactionAction(TableTransactionActionType.Delete, ent, ent.ETag)
            };

            // success
            IList<TableResult> results = this.repTable.ExecuteBatch(batch);
            Assert.AreEqual(results.Count, 1);
            Assert.AreEqual(results.First().HttpStatusCode, (int)HttpStatusCode.NoContent);

            // fail - not found
            try
            {
                this.repTable.ExecuteBatch(batch);
                Assert.Fail();
            }
            catch (RequestFailedException e)
            {
                // ExecuteBatch throw ReplicatedTableConflictException since TransformUpdateBatchOp returns null.
                Assert.IsNull(e.InnerException);
                Assert.AreEqual("Please retry again after random timeout", e.Message);
            }
        }
        #endregion Delete


        #region Merge
        [Test(Description="TableBatch Merge Sync")]
        public void TableBatchMergeSync()
        {
            // Insert Entity
            DynamicReplicatedTableEntity baseEntity = new DynamicReplicatedTableEntity("merge test", "foo");
            baseEntity.Properties.Add("prop1", "value1");
            this.repTable.Insert(baseEntity);

            DynamicReplicatedTableEntity mergeEntity = new DynamicReplicatedTableEntity(baseEntity.PartitionKey, baseEntity.RowKey) { ETag = baseEntity.ETag };
            mergeEntity.Properties.Add("prop2", "value2");

            IList<TableTransactionAction> batch = new List<TableTransactionAction>
            {
                new TableTransactionAction(TableTransactionActionType.UpdateMerge, mergeEntity, mergeEntity.ETag)
            };

            this.repTable.ExecuteBatch(batch);

            // Retrieve Entity & Verify Contents
            TableResult result = this.repTable.Retrieve(baseEntity.PartitionKey, baseEntity.RowKey);

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
            baseEntity.Properties.Add("prop1", "value1");
            this.repTable.Insert(baseEntity);

            // ReplaceEntity
            DynamicReplicatedTableEntity replaceEntity = new DynamicReplicatedTableEntity(baseEntity.PartitionKey, baseEntity.RowKey) 
            { 
                ETag = new ETag("1")
            };
            replaceEntity.Properties.Add("prop2", "value2");

            Console.WriteLine("Calling Replace()...");
            IList<TableTransactionAction> batch = new List<TableTransactionAction>
            {
                new TableTransactionAction(TableTransactionActionType.UpdateReplace, replaceEntity, replaceEntity.ETag)
            };
            this.repTable.ExecuteBatch(batch);

            // Retrieve Entity & Verify Contents
            Console.WriteLine("Calling Retrieve()...");
            TableResult result = this.repTable.Retrieve(baseEntity.PartitionKey, baseEntity.RowKey);
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
                ETag = new ETag(retrievedEntity._rtable_Version.ToString())
            };
            replaceEntity.Properties.Add("prop3", "value3");
            batch = new List<TableTransactionAction>
            {
                new TableTransactionAction(TableTransactionActionType.UpdateReplace, replaceEntity, replaceEntity.ETag)
            };
            this.repTable.ExecuteBatch(batch);

            Console.WriteLine("Calling Retrieve()...");
            result = this.repTable.Retrieve(baseEntity.PartitionKey, baseEntity.RowKey);
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
            string pk = Guid.NewGuid().ToString();
            var ent = GenerateRandomEnitity(pk);

            // insert
            IList<TableTransactionAction> batch = new List<TableTransactionAction>
            {
                new TableTransactionAction(TableTransactionActionType.Add, ent, ent.ETag)
            };

            // delete
            {
                DynamicReplicatedTableEntity entity = GenerateRandomEnitity(pk);
                this.repTable.Insert(entity);
                batch.Add(new TableTransactionAction(TableTransactionActionType.Delete, entity, entity.ETag));
            }

            // replace
            {
                DynamicReplicatedTableEntity entity = GenerateRandomEnitity(pk);
                this.repTable.Insert(entity);
                batch.Add(new TableTransactionAction(TableTransactionActionType.UpdateReplace, entity, entity.ETag));
            }

            // insert or replace
            {
                DynamicReplicatedTableEntity entity = GenerateRandomEnitity(pk);
                this.repTable.Insert(entity);
                batch.Add(new TableTransactionAction(TableTransactionActionType.UpsertReplace, entity, entity.ETag));
            }

            // merge
            {
                DynamicReplicatedTableEntity entity = GenerateRandomEnitity(pk);
                this.repTable.Insert(entity);
                batch.Add(new TableTransactionAction(TableTransactionActionType.UpdateMerge, entity, entity.ETag));
            }

            // insert or merge
            {
                DynamicReplicatedTableEntity entity = GenerateRandomEnitity(pk);
                this.repTable.Insert(entity);
                batch.Add(new TableTransactionAction(TableTransactionActionType.UpsertMerge, entity, entity.ETag));
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

        #region Empty Keys Tests
        [Test(Description = "TableBatchOperations with Empty keys")]
        public void TableBatchOperationsWithEmptyKeys()
        {
            // Insert Entity
            DynamicReplicatedTableEntity ent = new DynamicReplicatedTableEntity() { PartitionKey = "", RowKey = "" };
            ent.Properties.Add("foo2", "bar2");
            ent.Properties.Add("foo", "bar");
            IList<TableTransactionAction> batch = new List<TableTransactionAction>
            {
                new TableTransactionAction(TableTransactionActionType.Add, ent, ent.ETag)
            };
            this.repTable.ExecuteBatch(batch);

            // Retrieve Entity
            TableResult result = this.repTable.Retrieve(ent.PartitionKey, ent.RowKey);

            DynamicReplicatedTableEntity retrievedEntity = result.Result as DynamicReplicatedTableEntity;
            Assert.IsNotNull(retrievedEntity);
            Assert.AreEqual(ent.PartitionKey, retrievedEntity.PartitionKey);
            Assert.AreEqual(ent.RowKey, retrievedEntity.RowKey);
            Assert.AreEqual(ent.Properties.Count, retrievedEntity.Properties.Count);
            Assert.AreEqual(ent.Properties["foo"], retrievedEntity.Properties["foo"]);
            Assert.AreEqual(ent.Properties["foo"], retrievedEntity.Properties["foo"]);
            Assert.AreEqual(ent.Properties["foo2"], retrievedEntity.Properties["foo2"]);
            Assert.AreEqual(ent.Properties["foo2"], retrievedEntity.Properties["foo2"]);

            // InsertOrMerge
            DynamicReplicatedTableEntity insertOrMergeEntity = new DynamicReplicatedTableEntity(ent.PartitionKey, ent.RowKey);
            insertOrMergeEntity.Properties.Add("foo3", "value");
            batch = new List<TableTransactionAction>
            {
                new TableTransactionAction(TableTransactionActionType.UpsertMerge, insertOrMergeEntity, insertOrMergeEntity.ETag)
            };
            this.repTable.ExecuteBatch(batch);

            result = this.repTable.Retrieve(ent.PartitionKey, ent.RowKey);
            retrievedEntity = result.Result as DynamicReplicatedTableEntity;
            Assert.IsNotNull(retrievedEntity);
            Assert.AreEqual(insertOrMergeEntity.Properties["foo3"], retrievedEntity.Properties["foo3"]);

            // InsertOrReplace
            DynamicReplicatedTableEntity insertOrReplaceEntity = new DynamicReplicatedTableEntity(ent.PartitionKey, ent.RowKey);
            insertOrReplaceEntity.Properties.Add("prop2", "otherValue");
            batch = new List<TableTransactionAction>
            {
                new TableTransactionAction(TableTransactionActionType.UpsertReplace, insertOrReplaceEntity, insertOrReplaceEntity.ETag)
            };
            this.repTable.ExecuteBatch(batch);

            result = this.repTable.Retrieve(ent.PartitionKey, ent.RowKey);
            retrievedEntity = result.Result as DynamicReplicatedTableEntity;
            Assert.IsNotNull(retrievedEntity);
            Assert.AreEqual(1, retrievedEntity.Properties.Count);
            Assert.AreEqual(insertOrReplaceEntity.Properties["prop2"], retrievedEntity.Properties["prop2"]);

            // Merge
            DynamicReplicatedTableEntity mergeEntity = new DynamicReplicatedTableEntity(retrievedEntity.PartitionKey, retrievedEntity.RowKey) { ETag = retrievedEntity.ETag };
            mergeEntity.Properties.Add("mergeProp", "merged");
            batch = new List<TableTransactionAction>
            {
                new TableTransactionAction(TableTransactionActionType.UpdateMerge, mergeEntity, mergeEntity.ETag)
            };
            this.repTable.ExecuteBatch(batch);

            // Retrieve Entity & Verify Contents
            result = this.repTable.Retrieve(ent.PartitionKey, ent.RowKey);
            retrievedEntity = result.Result as DynamicReplicatedTableEntity;

            Assert.IsNotNull(retrievedEntity);
            Assert.AreEqual(mergeEntity.Properties["mergeProp"], retrievedEntity.Properties["mergeProp"]);

            // Replace
            DynamicReplicatedTableEntity replaceEntity = new DynamicReplicatedTableEntity(ent.PartitionKey, ent.RowKey) { ETag = retrievedEntity.ETag };
            replaceEntity.Properties.Add("replaceProp", "replace");
            batch = new List<TableTransactionAction>
            {
                new TableTransactionAction(TableTransactionActionType.UpdateReplace, replaceEntity, replaceEntity.ETag)
            };
            this.repTable.ExecuteBatch(batch);

            // Retrieve Entity & Verify Contents
            result = this.repTable.Retrieve(ent.PartitionKey, ent.RowKey);
            retrievedEntity = result.Result as DynamicReplicatedTableEntity;
            Assert.IsNotNull(retrievedEntity);
            Assert.AreEqual(replaceEntity.Properties.Count, retrievedEntity.Properties.Count);
            Assert.AreEqual(replaceEntity.Properties["replaceProp"], retrievedEntity.Properties["replaceProp"]);

            // Delete Entity
            batch = new List<TableTransactionAction>
            {
                new TableTransactionAction(TableTransactionActionType.Delete, retrievedEntity, retrievedEntity.ETag)
            };
            this.repTable.ExecuteBatch(batch);

            // Retrieve Entity
            result = this.repTable.Retrieve(ent.PartitionKey, ent.RowKey);
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

        //[Test(Description = "Ensure that adding null to the batch will throw")]
        //public void TableBatchAddNullShouldThrow()
        //{
        //    try
        //    {
        //        this.repTable.ExecuteBatch(null);
        //        Assert.Fail();
        //    }
        //    catch (ArgumentNullException)
        //    {
        //        // no op
        //    }
        //    catch (Exception)
        //    {
        //        Assert.Fail();
        //    }
        //}

        [Test(Description = "Ensure that adding empty list to the batch will throw")]
        public void TableBatchAddEmptyListShouldThrow()
        {
            try
            {
                this.repTable.ExecuteBatch(new List<TableTransactionAction>());
                Assert.Fail();
            }
            catch (InvalidOperationException)
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
            IList<TableTransactionAction> batch = new List<TableTransactionAction>();
            string pk = Guid.NewGuid().ToString();

            // Add entity 0
            var first = GenerateRandomEnitity(pk);
            batch.Add(new TableTransactionAction(TableTransactionActionType.Add, first));

            // Add entities 1 - 98
            for (int m = 1; m < 99; m++)
            {
                var entity = GenerateRandomEnitity(pk);
                batch.Add(new TableTransactionAction(TableTransactionActionType.Add, entity));
            }

            // Insert Duplicate of entity 0
            batch.Add(new TableTransactionAction(TableTransactionActionType.Add, first));

            try
            {
                this.repTable.ExecuteBatch(batch);
                Assert.Fail();
            }
            catch (RequestFailedException e)
            {
                TestHelper.ValidateResponse(
                    e,
                    1,
                    (int)HttpStatusCode.BadRequest,
                    new string[] { "InvalidDuplicateRow" },
                    new string[] { "99:The batch request contains multiple changes with same row key. An entity can appear only once in a batch request." },
                    false);
            }
        }

        [Test(Description = "Ensure that a batch with over 100 entities will throw")]
        public void TableBatchOver100EntitiesShouldThrow()
        {
            IList<TableTransactionAction> batch = new List<TableTransactionAction>();
            string pk = Guid.NewGuid().ToString();
            for (int m = 0; m < 101; m++)
            {
                var ent = GenerateRandomEnitity(pk);
                batch.Add(new TableTransactionAction(TableTransactionActionType.Add, ent));
            }

            try
            {
                this.repTable.ExecuteBatch(batch);
                Assert.Fail();
            }
            catch (RequestFailedException e)
            {
                TestHelper.ValidateResponse(
                    e,
                    1,
                    (int)HttpStatusCode.BadRequest,
                    new string[] { "InvalidInput" },
                    "The batch request operation exceeds the maximum 100 changes per change set.");
            }
        }

        [Test(Description = "Ensure that a batch with entity over 1 MB will throw")]
        public void TableBatchEntityOver1MBShouldThrow()
        {
            IList<TableTransactionAction> batch = new List<TableTransactionAction>();
            string pk = Guid.NewGuid().ToString();

            DynamicReplicatedTableEntity ent = GenerateRandomEnitity(pk);
            ent.Properties.Add("binary", new byte[1024 * 1024]);
            batch.Add(new TableTransactionAction(TableTransactionActionType.Add, ent, ent.ETag));

            try
            {
                this.repTable.ExecuteBatch(batch);
                Assert.Fail();
            }
            catch (RequestFailedException e)
            {
                TestHelper.ValidateResponse(e, 2, (int)HttpStatusCode.BadRequest, new string[] { "EntityTooLarge" }, "The entity is larger than the maximum allowed size (1MB).");
            }
        }
        
        [Test(Description = "Ensure that a batch over 4 MB will throw")]
        public void TableBatchOver4MBShouldThrow()
        {
            IList<TableTransactionAction> batch = new List<TableTransactionAction>();
            string pk = Guid.NewGuid().ToString();

            for (int m = 0; m < 65; m++)
            {
                DynamicReplicatedTableEntity ent = GenerateRandomEnitity(pk);

                // Maximum Entity size is 64KB
                ent.Properties.Add("binary",new byte[64 * 1024]);
                batch.Add(new TableTransactionAction(TableTransactionActionType.Add, ent, ent.ETag));
            }

            try
            {
                this.repTable.ExecuteBatch(batch);
                Assert.Fail();
            }
            catch (RequestFailedException e)
            {
                //
                // ###XXX: NEW SOURCE CODES: The commented out codes were the original codes
                //
                //TestHelper.ValidateResponse(opContext, 1, (int)HttpStatusCode.BadRequest, new string[] { "ContentLengthExceeded" }, "The content length for the requested operation has exceeded the limit (4MB).");
                TestHelper.ValidateResponse(
                    e, 
                    2, 
                    (int)HttpStatusCode.RequestEntityTooLarge, 
                    new string[] { "RequestBodyTooLarge" }, 
                    "The request body is too large and exceeds the maximum permissible limit"
                    );
            }
        }

        [Test(Description = "Ensure that empty batch will throw")]
        public void TableBatchEmptyBatchShouldThrow()
        {
            IList<TableTransactionAction> batch = new List<TableTransactionAction>();
            TestHelper.ExpectedException<InvalidOperationException>(
                () => this.repTable.ExecuteBatch(batch),
                "Empty batch operation should fail");
        }

        [Test(Description = "Ensure that a given batch only allows entities with the same partitionkey")]
        public void TableBatchLockToPartitionKey()
        {
            IList<TableTransactionAction> batch = new List<TableTransactionAction>();
            var entity = GenerateRandomEnitity("foo");
            batch.Add(new TableTransactionAction(TableTransactionActionType.Add, entity, entity.ETag));

            try
            {
                entity = GenerateRandomEnitity("foo2");
                batch.Add(new TableTransactionAction(TableTransactionActionType.Add, entity, entity.ETag));
                var resp = this.repTable.ExecuteBatch(batch);
                Assert.Fail();
            }
            catch (RequestFailedException e)
            {
                TestHelper.ValidateResponse(
                    e,
                    1,
                    (int)HttpStatusCode.BadRequest,
                    new string[] { "CommandsInBatchActOnDifferentPartitions" },
                    "All commands in a batch must operate on same entity group.");
                // no op
            }
            catch (Exception)
            {
                Assert.Fail();
            }

            // should reset pk lock
            batch.RemoveAt(0);
            batch.Add(new TableTransactionAction(TableTransactionActionType.Add, entity, entity.ETag));

            try
            {
                batch.Add(new TableTransactionAction(TableTransactionActionType.Add, entity, entity.ETag));
            }
            catch (Exception)
            {
                Assert.Fail();
            }
        }

        [Test(Description="Ensure that a batch with an entity property over 255 chars will throw")]
        public void TableBatchWithPropertyOver255CharsShouldThrow()
        {
            IList<TableTransactionAction> batch = new List<TableTransactionAction>();
            string pk = Guid.NewGuid().ToString();

            string propName = new string('a', 256);  // propName has 256 chars

            DynamicReplicatedTableEntity ent = new DynamicReplicatedTableEntity("foo", "bar");
            ent.Properties.Add(propName, "propbar");
            batch.Add(new TableTransactionAction(TableTransactionActionType.Add, ent, ent.ETag));

            try
            {
                this.repTable.ExecuteBatch(batch);
                Assert.Fail();
            }
            catch (RequestFailedException e)
            {
                TestHelper.ValidateResponse(e, 2, (int)HttpStatusCode.BadRequest, new string[] { "PropertyNameTooLong" }, "The property name exceeds the maximum allowed length (255).");
            }
        }
        #endregion Boundary Conditions



        #region Helpers
        private static void AddInsertToBatch(string pk, IList<TableTransactionAction> batch)
        {
            var entity = GenerateRandomEnitity(pk);
            batch.Add(new TableTransactionAction(TableTransactionActionType.Add, entity, entity.ETag));
        }

        private static DynamicReplicatedTableEntity GenerateRandomEnitity(string pk)
        {
            DynamicReplicatedTableEntity ent = new DynamicReplicatedTableEntity();
            ent.Properties.Add("foo", "bar");

            ent.PartitionKey = pk;
            ent.RowKey = Guid.NewGuid().ToString();
            return ent;
        }


        private void InsertAndDeleteBatchWithNEntities(int n)
        {
            IList<TableTransactionAction> batch = new List<TableTransactionAction>();
            string pk = Guid.NewGuid().ToString();
            for (int m = 0; m < n; m++)
            {
                var entity = GenerateRandomEnitity(pk);
                batch.Add(new TableTransactionAction(TableTransactionActionType.Add, entity, entity.ETag));
            }

            IList<TableResult> results = this.repTable.ExecuteBatch(batch);

            IList<TableTransactionAction> delBatch = new List<TableTransactionAction>();

            foreach (TableResult res in results)
            {
                var entity = (ITableEntity)res.Result;
                delBatch.Add(new TableTransactionAction(TableTransactionActionType.Delete, entity, entity.ETag));
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

            IList<TableTransactionAction> insertBatch = new List<TableTransactionAction>();
            IList<TableTransactionAction> mergeBatch = new List<TableTransactionAction>();
            IList<TableTransactionAction> delBatch = new List<TableTransactionAction>();

            for (int m = 0; m < n; m++)
            {
                var entity = GenerateRandomEnitity(pk);
                insertBatch.Add(new TableTransactionAction(TableTransactionActionType.UpsertMerge, entity, entity.ETag));
            }

            IList<TableResult> results = this.repTable.ExecuteBatch(insertBatch);
            foreach (TableResult res in results)
            {
                Assert.AreEqual(res.HttpStatusCode, (int)HttpStatusCode.NoContent);

                // update entity and add to merge batch
                DynamicReplicatedTableEntity ent = res.Result as DynamicReplicatedTableEntity;
                ent.Properties.Add("foo2", "bar2");
                mergeBatch.Add(new TableTransactionAction(TableTransactionActionType.UpsertMerge, ent, ent.ETag));

            }

            // execute insertOrMerge batch, this time entities exist
            IList<TableResult> mergeResults = this.repTable.ExecuteBatch(mergeBatch);

            foreach (TableResult res in mergeResults)
            {
                Assert.AreEqual(res.HttpStatusCode, (int)HttpStatusCode.NoContent);

                // Add to delete batch
                var entity = (ITableEntity)res.Result;
                delBatch.Add(new TableTransactionAction(TableTransactionActionType.Delete, entity, entity.ETag));
            }

            IList<TableResult> delResults = this.repTable.ExecuteBatch(delBatch);
            foreach (TableResult res in delResults)
            {
                Assert.AreEqual(res.HttpStatusCode, (int)HttpStatusCode.NoContent);
            }
        }

        private void POCOInsertAndDeleteBatchWithNEntities(int n)
        {
            IList<TableTransactionAction> batch = new List<TableTransactionAction>();
            string pk = Guid.NewGuid().ToString();
            for (int m = 0; m < n; m++)
            {
                var entity = new BaseEntity(pk, Guid.NewGuid().ToString());
                batch.Add(new TableTransactionAction(TableTransactionActionType.Add, entity, entity.ETag));
            }

            IList<TableResult> results = this.repTable.ExecuteBatch(batch);

            IList<TableTransactionAction> delBatch = new List<TableTransactionAction>();

            foreach (TableResult res in results)
            {
                var entity = (ITableEntity)res.Result;
                batch.Add(new TableTransactionAction(TableTransactionActionType.Delete, entity, entity.ETag));
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
