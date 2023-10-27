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
    using System.Threading;
    using System.Collections.Generic;
    using System.Net;
    using global::Azure;
    using global::Azure.Data.Tables;
    using Newtonsoft.Json.Linq;

    [TestFixture]
    public class RTableQueryGenericTests : RTableLibraryTestBase
    {
        private const int NumberOfBatches = 15;
        private const int BatchSize = 100;

        private TableClient currentTable = null;
        
        [OneTimeSetUp]
        public void TestFixtureSetup()
        {
            this.LoadTestConfiguration();
            string tableName = this.GenerateRandomTableName();
            this.SetupRTableEnv(tableName);

            // Initialize the table to be queried to the tail replica         
            TableServiceClient tableClient = this.repTable.GetTailTableClient();
            this.currentTable = tableClient.GetTableClient(repTable.TableName);

            try
            {
                for (int i = 0; i < NumberOfBatches; i++)
                {
                    IList<TableTransactionAction> batch = new List<TableTransactionAction>();

                    for (int j = 0; j < BatchSize; j++)
                    {
                        BaseEntity ent = GenerateRandomEntity("tables_batch_" + i.ToString());
                        ent.RowKey = string.Format("{0:0000}", j);
                        batch.Add(new TableTransactionAction(TableTransactionActionType.Add, ent, ent.ETag));
                    }

                    repTable.ExecuteBatch(batch);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("TestFixtureSetup() exception {0}", ex.ToString());
                throw;
            }
        }
        
        [OneTimeTearDown]
        public void TestFixtureTearDown()
        {
            base.DeleteAllRtableResources();
        }

        //
        // binaries.amd64fre\Gateway\UnitTests>
        // runuts /fixture=Microsoft.WindowsAzure.Storage.RTableTest.RTableQueryGenericTests
        //
        #region Query Test Methods
        [Test(Description="A test to validate basic table query")]
        public void TableGenericQueryBasicSync()
        {
            var query = currentTable.Query<BaseEntity>(e => e.PartitionKey == "tables_batch_1");

            foreach (BaseEntity ent in query)
            {
                Assert.AreEqual(ent.PartitionKey, "tables_batch_1");
                ent.Validate();
            }
        }
        
        [Test(Description="Generic query with Continuation sync")]
        public void TableGenericQueryWithContinuationSync()
        {
            var query = currentTable.Query<BaseEntity>();
            int count = 0;
            var seg = query.AsPages().FirstOrDefault();
            foreach (BaseEntity ent in seg.Values)
            {
                Assert.IsTrue(ent.PartitionKey.StartsWith("tables_batch"));
                ent.Validate();
                count++;
            }

            Assert.IsTrue(NumberOfBatches * BatchSize > count);

            // Second segment
            Assert.IsNotNull(seg.ContinuationToken);
            seg = query.AsPages(seg.ContinuationToken).FirstOrDefault();
            foreach (BaseEntity ent in seg.Values)
            {
                Assert.IsTrue(ent.PartitionKey.StartsWith("tables_batch"));
                ent.Validate();
                count++;
            }
            Assert.AreEqual(NumberOfBatches * BatchSize, count);
        }

        [Test(Description = "A test to validate basic table filtering")]
        public void TableGenericQueryWithFilter()
        {
            int ninety = 90;
            string ninetyString = string.Format("{0:0000}", ninety);

            int count = 0;

            foreach (BaseEntity ent in currentTable.Query<BaseEntity>(string.Format("(PartitionKey eq '{0}') and (RowKey ge '{1}')", "tables_batch_1", ninetyString)))
            {
                Assert.AreEqual(ent.PartitionKey, "tables_batch_1");
                Assert.AreEqual(ent.RowKey, string.Format("{0:0000}", count + ninety));
                ent.Validate();
                count++;
            }

            Assert.AreEqual(count, BatchSize - ninety);
        }

        [Test(Description="query enumerate twice")]
        public void TableGenericQueryEnumerateTwice()
        {
            IEnumerable<BaseEntity> enumerable = currentTable.Query<BaseEntity>();

            List<BaseEntity> firstIteration = new List<BaseEntity>();
            List<BaseEntity> secondIteration = new List<BaseEntity>();

            foreach (BaseEntity ent in enumerable)
            {
                Assert.IsTrue(ent.PartitionKey.StartsWith("tables_batch"));
                ent.Validate();
                firstIteration.Add(ent);
            }

            foreach (BaseEntity ent in enumerable)
            {
                Assert.IsTrue(ent.PartitionKey.StartsWith("tables_batch"));
                ent.Validate();
                secondIteration.Add(ent);
            }

            Assert.AreEqual(firstIteration.Count, secondIteration.Count);

            for (int m = 0; m < firstIteration.Count; m++)
            {
                Assert.AreEqual(firstIteration[m].PartitionKey, secondIteration[m].PartitionKey);
                Assert.AreEqual(firstIteration[m].RowKey, secondIteration[m].RowKey);
                Assert.AreEqual(firstIteration[m].Timestamp, secondIteration[m].Timestamp);
                Assert.AreEqual(firstIteration[m].ETag, secondIteration[m].ETag);
                firstIteration[m].Validate();
            }
        }
        
        [Test(Description="Basic projection test")]
        public void TableGenericQueryProjection()
        {
            foreach (BaseEntity ent in currentTable.Query<BaseEntity>(select: new List<string>() { "A", "C" }))
            {
                Assert.IsNotNull(ent.PartitionKey);
                Assert.IsNotNull(ent.RowKey);
                Assert.IsNotNull(ent.Timestamp);
                Assert.AreEqual("a", ent.A);
                Assert.IsNull(ent.B);
                Assert.AreEqual("c", ent.C);
                Assert.IsNull(ent.D);                
            }
        }

        [Test(Description = "Basic with resolver")]
        public void TableGenericWithResolver()
        {
            foreach (string ent in currentTable.Query<TableEntity>(select: new List<string>() { "A", "C" }).Select(e => (string)e["A"] + (string)e["C"]))
            {
                Assert.AreEqual("ac", ent);
            }

            foreach (BaseEntity ent in currentTable.Query<TableEntity>().Select(e => 
                new BaseEntity() { PartitionKey = e.PartitionKey, RowKey = e.RowKey, Timestamp = e.Timestamp, A = (string)e["A"], C = (string)e["C"], ETag = e.ETag }))
            {
                Assert.IsNotNull(ent.PartitionKey);
                Assert.IsNotNull(ent.RowKey);
                Assert.IsNotNull(ent.Timestamp);
                Assert.IsNotNull(ent.ETag);

                Assert.AreEqual("a", ent.A);
                Assert.IsNull(ent.B);
                Assert.AreEqual("c", ent.C);
                Assert.IsNull(ent.D);
            }
        }

        [Test(Description = "TableQuerySegmented resolver test")]
        public void TableQuerySegmentedResolver()
        {
            string token = null;
            List<string> list = new List<string>();
            do
            {
                var query = currentTable.Query<TableEntity>(select: new List<string>() { "A", "C" });
                var page = query.AsPages(token).FirstOrDefault();
                var segment = page.Values.Select(e => (string)e["A"] + (string)e["C"]);
                list.AddRange(segment);
                token = page.ContinuationToken;
            } while (token != null);

            foreach (string ent in list)
            {
                Assert.AreEqual("ac", ent);
            }

            List<BaseEntity> list1 = new List<BaseEntity>();
            token = null;
            do
            {
                var query = currentTable.Query<TableEntity>();
                var page = query.AsPages(token).FirstOrDefault();
                var segment = page.Values.Select(e =>
                    new BaseEntity() { PartitionKey = e.PartitionKey, RowKey = e.RowKey, Timestamp = e.Timestamp, A = (string)e["A"], C = (string)e["C"], ETag = e.ETag });
                list1.AddRange(segment);
                token = page.ContinuationToken;
            } while (token != null);

            foreach (BaseEntity ent in list1)
            {
                Assert.IsNotNull(ent.PartitionKey);
                Assert.IsNotNull(ent.RowKey);
                Assert.IsNotNull(ent.Timestamp);
                Assert.IsNotNull(ent.ETag);

                Assert.AreEqual("a", ent.A);
                Assert.IsNull(ent.B);
                Assert.AreEqual("c", ent.C);
                Assert.IsNull(ent.D);
            }
        }

        [Test(Description = "TableQuerySegmented resolver test")]
        public void TableQuerySegmentedResolverWithDynamic()
        {
            string token = null;
            List<BaseEntity> list1 = new List<BaseEntity>();

            do
            {
                var query = currentTable.Query<BaseEntity>();
                var page = query.AsPages(token).FirstOrDefault();
                var segment = page.Values.Select(e =>
                    new BaseEntity() { PartitionKey = e.PartitionKey, RowKey = e.RowKey, Timestamp = e.Timestamp, A = e.A, C = e.C, ETag = e.ETag });
                list1.AddRange(segment);
                token = page.ContinuationToken;
            } while (token != null);


            foreach (BaseEntity ent in list1)
            {
                Assert.IsNotNull(ent.PartitionKey);
                Assert.IsNotNull(ent.RowKey);
                Assert.IsNotNull(ent.Timestamp);
                Assert.IsNotNull(ent.ETag);

                Assert.AreEqual("a", ent.A);
                Assert.IsNull(ent.B);
                Assert.AreEqual("c", ent.C);
                Assert.IsNull(ent.D);
            }
        }

        [Test(Description = "Query with internal type (InternalEntity)")]
        public void TableGenericQueryWithInternalType()
        {
            var query = currentTable.Query<InternalEntity>(e => e.PartitionKey == "tables_batch_1");

            foreach (InternalEntity ent in query)
            {
                Assert.AreEqual(ent.PartitionKey, "tables_batch_1");
                ent.Validate();
            }
        }
        
        [Test(Description="A test to ensure that a generic query must have a type with a default constructor ")]
        public void TableGenericQueryOnTypeWithNoCtor()
        {
            TestHelper.ExpectedException<NotSupportedException>(() => currentTable.Query<NoCtorEntity>(), "Query should not be able to be instantiated with a generic type that has no default constructor");
        }

        [Test(Description = "IQueryable - A test to validate basic CreateQuery")]
        public void TableQueryableCreateQueryGeneric()
        {
            string tableName = this.GenerateRandomTableName();
            ReplicatedTable localRTable = new ReplicatedTable(tableName, this.configurationService);
            localRTable.CreateIfNotExists();
            RTableWrapperForSampleRTableEntity localRTableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(localRTable);

            TableServiceClient tableClient = localRTable.GetTailTableClient();
            TableClient table = tableClient.GetTableClient(localRTable.TableName);

            string pk = "0";
            try
            {
                try
                {
                    IList<TableTransactionAction> batch = new List<TableTransactionAction>();

                    for (int j = 0; j < 10; j++)
                    {
                        BaseEntity ent = GenerateRandomEntity(pk);
                        ent.RowKey = string.Format("{0:0000}", j);
                        batch.Add(new TableTransactionAction(TableTransactionActionType.Add, ent, ent.ETag));
                    }

                    localRTable.ExecuteBatch(batch);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Exception during test case init {0}", ex.ToString());
                    throw;
                }
                
                var tableQuery = table.Query<BaseEntity>(x => x.PartitionKey == pk);
                var rtableQuery = localRTable.CreateQuery<BaseEntity>(x => x.PartitionKey == pk);

                var list = tableQuery.AsEnumerable();

                int tableCount = 0;
                int rtableCount = 0;
                foreach (BaseEntity ent in list)
                {
                    tableCount++;
                }
                foreach (BaseEntity ent in rtableQuery.ToList())
                {
                    rtableCount++;

                    Assert.IsTrue(ent.ETag != new ETag(ent._rtable_Version.ToString()), "ETag is not virtualized when using CreateQuery()");
                }

                Assert.IsTrue(tableCount == rtableCount, "Query counts are different");
                Assert.IsTrue(tableCount == 10, "Query counts are different");

                // But, with "CreateReplicatedQuery" ETag is virtualized
                IQueryable<BaseEntity> virtualizedRtableQuery = localRTable.CreateReplicatedQuery<BaseEntity>(x => x.PartitionKey == pk);

                foreach (BaseEntity ent in virtualizedRtableQuery.ToList())
                {
                    Assert.IsTrue(ent._rtable_Version == 0);
                    Assert.IsTrue(ent.ETag == new ETag(ent._rtable_Version.ToString()), "ETag is virtualized when using CreateReplicatedQuery()");

                    ent.A += "`";

                    // Update should go fine since ETag is virtualized
                    TableResult result = localRTable.Replace(ent);
                    Assert.IsTrue(result != null && result.HttpStatusCode == (int)HttpStatusCode.NoContent);
                }

                virtualizedRtableQuery = localRTable.CreateReplicatedQuery<BaseEntity>(x => x.PartitionKey == pk);

                foreach (BaseEntity ent in virtualizedRtableQuery.ToList())
                {
                    Assert.IsTrue(ent._rtable_Version == 1);
                    Assert.IsTrue(ent.ETag == new ETag(ent._rtable_Version.ToString()), "ETag is virtualized when using CreateReplicatedQuery()");
                }
            }
            catch(Exception e)
            {
                Console.WriteLine("Error during query processing: {0}", e.ToString());
            }
            finally
            {
                localRTable.DeleteIfExists();
            }
        }


        [Test(Description = "Test to validate query without partition key")]
        public void TableQueryableCreateQueryNoPartitionKey()
        {
            Thread.Sleep(10000);

            string tableName = this.GenerateRandomTableName();
            ReplicatedTable localRTable = new ReplicatedTable(tableName, this.configurationService);
            localRTable.CreateIfNotExists();
            RTableWrapperForSampleRTableEntity localRTableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(localRTable);

            TableServiceClient tableClient = localRTable.GetTailTableClient();
            TableClient table = tableClient.GetTableClient(localRTable.TableName);

            string pk = "0";
            try
            {
                try
                {
                    IList<TableTransactionAction> batch = new List<TableTransactionAction>();

                    for (int j = 0; j < 10; j++)
                    {
                        BaseEntity ent = GenerateRandomEntity(pk);
                        ent.RowKey = string.Format("{0:0000}", j);
                        batch.Add(new TableTransactionAction(TableTransactionActionType.Add, ent, ent.ETag));
                    }

                    localRTable.ExecuteBatch(batch);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Exception during test case init {0}", ex.ToString());
                    throw;
                }

                try
                {
                    pk = "1";
                    IList<TableTransactionAction> batch = new List<TableTransactionAction>();

                    for (int j = 0; j < 10; j++)
                    {
                        BaseEntity ent = GenerateRandomEntity(pk);
                        ent.RowKey = string.Format("{0:0000}", j);
                        batch.Add(new TableTransactionAction(TableTransactionActionType.Add, ent, ent.ETag));
                    }

                    localRTable.ExecuteBatch(batch);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Exception during test case init {0}", ex.ToString());
                    throw;
                }

                var tableQuery = table.Query<BaseEntity>();
                var rtableQuery = localRTable.CreateQuery<BaseEntity>(e => true);

                var list = tableQuery.AsEnumerable();

                int tableCount = 0;
                int rtableCount = 0;
                foreach (BaseEntity ent in list)
                {
                    tableCount++;
                }
                foreach (BaseEntity ent in rtableQuery.ToList())
                {
                    rtableCount++;

                    Assert.IsTrue(ent.ETag != new ETag(ent._rtable_Version.ToString()), "ETag is not virtualized when using CreateQuery()");
                }

                Assert.IsTrue(tableCount == rtableCount, "Query counts are different");
                Assert.IsTrue(tableCount == 20, "Query counts are different");

                // But, with "CreateReplicatedQuery" ETag is virtualized
                IQueryable<BaseEntity> virtualizedRtableQuery = localRTable.CreateReplicatedQuery<BaseEntity>(e => true);

                foreach (BaseEntity ent in virtualizedRtableQuery.ToList())
                {
                    Assert.IsTrue(ent._rtable_Version == 0);
                    Assert.IsTrue(ent.ETag == new ETag(ent._rtable_Version.ToString()), "ETag is virtualized when using CreateReplicatedQuery()");

                    ent.A += "`";

                    // Update should go fine since ETag is virtualized
                    TableResult result = localRTable.Replace(ent);
                    Assert.IsTrue(result != null && result.HttpStatusCode == (int)HttpStatusCode.NoContent);
                }

                virtualizedRtableQuery = localRTable.CreateReplicatedQuery<BaseEntity>(e => true);

                foreach (BaseEntity ent in virtualizedRtableQuery.ToList())
                {
                    Assert.IsTrue(ent._rtable_Version == 1);
                    Assert.IsTrue(ent.ETag == new ETag(ent._rtable_Version.ToString()), "ETag is virtualized when using CreateReplicatedQuery()");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error during query processing: {0}", e.ToString());
            }
            finally
            {
                localRTable.DeleteIfExists();
            }
        }
      

        #endregion Query Test Methods
        

        #region Helpers

        private static BaseEntity GenerateRandomEntity(string pk)
        {
            BaseEntity ent = new BaseEntity();
            ent.Populate();
            ent.PartitionKey = pk;
            ent.RowKey = Guid.NewGuid().ToString();
            return ent;
        }
        #endregion Helpers
    }
}
