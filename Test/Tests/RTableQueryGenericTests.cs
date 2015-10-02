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
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;
    using NUnit.Framework;
    using System;
    using System.Linq;
    using System.Threading;
    using System.Collections.Generic;

    [TestFixture]
    public class RTableQueryGenericTests : RTableLibraryTestBase
    {
        private const int NumberOfBatches = 15;
        private const int BatchSize = 100;

        private CloudTable currentTable = null;
        
        [TestFixtureSetUp]
        public void TestFixtureSetup()
        {
            this.LoadTestConfiguration();
            string tableName = this.GenerateRandomTableName();
            this.SetupRTableEnv(tableName);

            // Initialize the table to be queried to the tail replica         
            CloudTableClient tableClient = this.repTable.GetTailTableClient();
            this.currentTable = tableClient.GetTableReference(repTable.TableName);

            try
            {
                for (int i = 0; i < NumberOfBatches; i++)
                {
                    TableBatchOperation batch = new TableBatchOperation();

                    for (int j = 0; j < BatchSize; j++)
                    {
                        BaseEntity ent = GenerateRandomEntity("tables_batch_" + i.ToString());
                        ent.RowKey = string.Format("{0:0000}", j);
                        batch.Insert(ent);
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
        
        [TestFixtureTearDown]
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
            TableQuery<BaseEntity> query = new TableQuery<BaseEntity>().Where(
                TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "tables_batch_1"));

            TableQuerySegment<BaseEntity> seg = currentTable.ExecuteQuerySegmented(query, null);

            foreach (BaseEntity ent in seg)
            {
                Assert.AreEqual(ent.PartitionKey, "tables_batch_1");
                ent.Validate();
            }
        }
        
        [Test(Description="Generic query with Continuation sync")]
        public void TableGenericQueryWithContinuationSync()
        {
            TableQuery<BaseEntity> query = new TableQuery<BaseEntity>();
            OperationContext opContext = new OperationContext();
            TableQuerySegment<BaseEntity> seg = currentTable.ExecuteQuerySegmented(query, null, null, opContext);
            int count = 0;
            foreach (BaseEntity ent in seg)
            {
                Assert.IsTrue(ent.PartitionKey.StartsWith("tables_batch"));
                ent.Validate();
                count++;
            }
            Assert.IsTrue(NumberOfBatches * BatchSize > count);

            // Second segment
            Assert.IsNotNull(seg.ContinuationToken);
            seg = currentTable.ExecuteQuerySegmented(query, seg.ContinuationToken, null, opContext);
            foreach (BaseEntity ent in seg)
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

            TableQuery<BaseEntity> query = new TableQuery<BaseEntity>().Where(
                string.Format("(PartitionKey eq '{0}') and (RowKey ge '{1}')", "tables_batch_1", ninetyString));

            OperationContext opContext = new OperationContext();
            int count = 0;

            foreach (BaseEntity ent in currentTable.ExecuteQuery(query))
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
            TableQuery<BaseEntity> query = new TableQuery<BaseEntity>();

            OperationContext opContext = new OperationContext();
            IEnumerable<BaseEntity> enumerable = currentTable.ExecuteQuery(query);

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
            TableQuery<BaseEntity> query = new TableQuery<BaseEntity>().Select(new List<string>() { "A", "C" });

            foreach (BaseEntity ent in currentTable.ExecuteQuery(query))
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

        [Test(Description="Basic with resolver")]        
        public void TableGenericWithResolver()
        {
            TableQuery<TableEntity> query = new TableQuery<TableEntity>().Select(new List<string>() { "A", "C" });
            query.TakeCount = 1000;

            foreach (string ent in currentTable.ExecuteQuery(query, (pk, rk, ts, prop, etag) => prop["A"].StringValue + prop["C"].StringValue))
            {
                Assert.AreEqual("ac", ent);
            }

            foreach (BaseEntity ent in currentTable.ExecuteQuery(query,
                (pk, rk, ts, prop, etag) => new BaseEntity() { PartitionKey = pk, RowKey = rk, Timestamp = ts, A = prop["A"].StringValue, C = prop["C"].StringValue, ETag = etag }))
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

            Assert.AreEqual(1000, query.TakeCount);
        }

        [Test(Description="query resolver with dynamic")]
        public void TableQueryResolverWithDynamic()
        {
            TableQuery query = new TableQuery().Select(new List<string>() { "A", "C" });
            foreach (string ent in currentTable.ExecuteQuery(query, (pk, rk, ts, prop, etag) => prop["A"].StringValue + prop["C"].StringValue))
            {
                Assert.AreEqual("ac", ent);
            }
            foreach (BaseEntity ent in currentTable.ExecuteQuery(query,
                            (pk, rk, ts, prop, etag) => new BaseEntity() { PartitionKey = pk, RowKey = rk, Timestamp = ts, A = prop["A"].StringValue, C = prop["C"].StringValue, ETag = etag }))
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

        [Test(Description="TableQuerySegmented resolver test")]
        public void TableQuerySegmentedResolver()
        {
            TableQuery<TableEntity> query = new TableQuery<TableEntity>().Select(new List<string>() { "A", "C" });
            TableContinuationToken token = null;
            List<string> list = new List<string>();
            do
            {
                TableQuerySegment<string> segment = currentTable.ExecuteQuerySegmented<TableEntity, string>(query, (pk, rk, ts, prop, etag) => prop["A"].StringValue + prop["C"].StringValue, token);
                list.AddRange(segment.Results);
                token = segment.ContinuationToken;
            } while (token != null);

            foreach (string ent in list)
            {
                Assert.AreEqual("ac", ent);
            }

            List<BaseEntity> list1 = new List<BaseEntity>();
            do
            {
                TableQuerySegment<BaseEntity> segment = currentTable.ExecuteQuerySegmented<TableEntity, BaseEntity>(query, (pk, rk, ts, prop, etag) => new BaseEntity() { PartitionKey = pk, RowKey = rk, Timestamp = ts, A = prop["A"].StringValue, C = prop["C"].StringValue, ETag = etag }, token);
                list1.AddRange(segment.Results);
                token = segment.ContinuationToken;
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

        [Test(Description="TableQuerySegmented resolver test")]
        public void TableQuerySegmentedResolverWithDynamic()
        {
            TableQuery query = new TableQuery().Select(new List<string>() { "A", "C" });
            TableContinuationToken token = null;
            List<string> list = new List<string>();
            do
            {
                TableQuerySegment<string> segment = currentTable.ExecuteQuerySegmented<string>(query, (pk, rk, ts, prop, etag) => prop["A"].StringValue + prop["C"].StringValue, token);
                list.AddRange(segment.Results);
                token = segment.ContinuationToken;
            } while (token != null);

            foreach (string ent in list)
            {
                Assert.AreEqual("ac", ent);
            }

            List<BaseEntity> list1 = new List<BaseEntity>();
            do
            {
                TableQuerySegment<BaseEntity> segment = currentTable.ExecuteQuerySegmented<BaseEntity>(query, (pk, rk, ts, prop, etag) => new BaseEntity() { PartitionKey = pk, RowKey = rk, Timestamp = ts, A = prop["A"].StringValue, C = prop["C"].StringValue, ETag = etag }, token);
                list1.AddRange(segment.Results);
                token = segment.ContinuationToken;
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
        
        [Test(Description="Basic with resolver")]
        public void TableGenericWithResolverAPM()
        {
            TableQuery<TableEntity> query = new TableQuery<TableEntity>().Select(new List<string>() { "A", "C" });

            using (AutoResetEvent waitHandle = new AutoResetEvent(false))
            {
                TableContinuationToken token = null;
                List<string> list = new List<string>();
                do
                {
                    IAsyncResult result = currentTable.BeginExecuteQuerySegmented(query, (pk, rk, ts, prop, etag) => prop["A"].StringValue + prop["C"].StringValue, token, ar => waitHandle.Set(), null);
                    waitHandle.WaitOne();
                    TableQuerySegment<string> segment = currentTable.EndExecuteQuerySegmented<TableEntity, string>(result);
                    list.AddRange(segment.Results);
                    token = segment.ContinuationToken;
                } while (token != null);

                foreach (string ent in list)
                {
                    Assert.AreEqual("ac", ent);
                }

                List<BaseEntity> list1 = new List<BaseEntity>();
                do
                {
                    IAsyncResult result = currentTable.BeginExecuteQuerySegmented(query, (pk, rk, ts, prop, etag) => new BaseEntity()
                    {
                        PartitionKey = pk,
                        RowKey = rk,
                        Timestamp = ts,
                        A = prop["A"].StringValue,
                        C = prop["C"].StringValue,
                        ETag = etag
                    }, token, ar => waitHandle.Set(), null);
                    waitHandle.WaitOne();
                    TableQuerySegment<BaseEntity> segment = currentTable.EndExecuteQuerySegmented<TableEntity, BaseEntity>(result);
                    list1.AddRange(segment.Results);
                    token = segment.ContinuationToken;
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
        }

        [Test(Description="Basic resolver test")]
        public void TableQueryResolverWithDynamicAPM()
        {
            TableQuery query = new TableQuery().Select(new List<string>() { "A", "C" });
            using (AutoResetEvent waitHandle = new AutoResetEvent(false))
            {
                TableContinuationToken token = null;
                List<string> list = new List<string>();
                do
                {
                    IAsyncResult result = currentTable.BeginExecuteQuerySegmented(query, (pk, rk, ts, prop, etag) => prop["A"].StringValue + prop["C"].StringValue, token, ar => waitHandle.Set(), null);
                    waitHandle.WaitOne();
                    TableQuerySegment<string> segment = currentTable.EndExecuteQuerySegmented<string>(result);
                    list.AddRange(segment.Results);
                    token = segment.ContinuationToken;
                } while (token != null);

                foreach (string ent in list)
                {
                    Assert.AreEqual(ent, "ac");
                }

                List<BaseEntity> list1 = new List<BaseEntity>();
                do
                {
                    IAsyncResult result = currentTable.BeginExecuteQuerySegmented(query, (pk, rk, ts, prop, etag) => new BaseEntity() { PartitionKey = pk, RowKey = rk, Timestamp = ts, A = prop["A"].StringValue, C = prop["C"].StringValue, ETag = etag }, token, ar => waitHandle.Set(), null);
                    waitHandle.WaitOne();
                    TableQuerySegment<BaseEntity> segment = currentTable.EndExecuteQuerySegmented<BaseEntity>(result);
                    list1.AddRange(segment.Results);
                    token = segment.ContinuationToken;
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
        }      
       
        [Test(Description="A test to validate basic take Count with and without continuations")]
        public void TableQueryGenericWithTakeCount()
        {
            // No continuation
            TableQuery<BaseEntity> query = new TableQuery<BaseEntity>().Take(100);

            OperationContext opContext = new OperationContext();
            IEnumerable<BaseEntity> enumerable = currentTable.ExecuteQuery(query, null, opContext);

            Assert.AreEqual(query.TakeCount, enumerable.Count());
            TestHelper.AssertNAttempts(opContext, 1);

            // With continuations
            query.TakeCount = 1200;
            opContext = new OperationContext();
            enumerable = currentTable.ExecuteQuery(query, null, opContext);

            Assert.AreEqual(query.TakeCount, enumerable.Count());
            TestHelper.AssertNAttempts(opContext, 2);
        }

        [Test(Description="A test to validate basic take Count with a resolver, with and without continuations")]
        public void TableQueryGenericWithTakeCountAndResolver()
        {
            // No continuation
            TableQuery<BaseEntity> query = new TableQuery<BaseEntity>().Take(100);

            OperationContext opContext = new OperationContext();
            IEnumerable<string> enumerable = currentTable.ExecuteQuery(query, (pk, rk, ts, prop, etag) => pk + rk, null, opContext);

            Assert.AreEqual(query.TakeCount, enumerable.Count());
            TestHelper.AssertNAttempts(opContext, 1);

            // With continuations
            query.TakeCount = 1200;
            opContext = new OperationContext();
            enumerable = currentTable.ExecuteQuery(query, (pk, rk, ts, prop, etag) => pk + rk, null, opContext);

            Assert.AreEqual(query.TakeCount, enumerable.Count());
            TestHelper.AssertNAttempts(opContext, 2);
        }

        [Test(Description = "Query with internal type (InternalEntity)")]
        public void TableGenericQueryWithInternalType()
        {
            TableQuery<InternalEntity> query = new TableQuery<InternalEntity>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "tables_batch_1"));

            TableQuerySegment<InternalEntity> seg = currentTable.ExecuteQuerySegmented(query, null);

            foreach (InternalEntity ent in seg)
            {
                Assert.AreEqual(ent.PartitionKey, "tables_batch_1");
                ent.Validate();
            }
        }
        
        [Test(Description="A test to ensure that a generic query must have a type with a default constructor ")]
        public void TableGenericQueryOnTypeWithNoCtor()
        {
            TestHelper.ExpectedException<NotSupportedException>(() => new TableQuery<NoCtorEntity>(), "TableQuery should not be able to be instantiated with a generic type that has no default constructor");
        }

        [Test(Description = "IQueryable - A test to validate basic CreateQuery")]
        public void TableQueryableCreateQueryGeneric()
        {
            string tableName = this.GenerateRandomTableName();
            ReplicatedTable localRTable = new ReplicatedTable(tableName, this.configurationService);
            localRTable.CreateIfNotExists();
            RTableWrapperForSampleRTableEntity localRTableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(localRTable);

            CloudTableClient tableClient = localRTable.GetTailTableClient();
            CloudTable table = tableClient.GetTableReference(localRTable.TableName);

            string pk = "0";
            try
            {
                try
                {
                    TableBatchOperation batch = new TableBatchOperation();

                    for (int j = 0; j < 10; j++)
                    {
                        BaseEntity ent = GenerateRandomEntity(pk);
                        ent.RowKey = string.Format("{0:0000}", j);
                        batch.Insert(ent);
                    }

                    localRTable.ExecuteBatch(batch);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Exception during test case init {0}", ex.ToString());
                    throw;
                }
                
                IQueryable<BaseEntity> tableQuery = table.CreateQuery<BaseEntity>().Where(x => x.PartitionKey == pk);
                IQueryable<BaseEntity> rtableQuery = localRTable.CreateQuery<BaseEntity>().Where(x => x.PartitionKey == pk);

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
                }

                Assert.IsTrue(tableCount == rtableCount, "Query counts are different");
                Assert.IsTrue(tableCount == 10, "Query counts are different");
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

            CloudTableClient tableClient = localRTable.GetTailTableClient();
            CloudTable table = tableClient.GetTableReference(localRTable.TableName);

            string pk = "0";
            try
            {
                try
                {
                    TableBatchOperation batch = new TableBatchOperation();

                    for (int j = 0; j < 10; j++)
                    {
                        BaseEntity ent = GenerateRandomEntity(pk);
                        ent.RowKey = string.Format("{0:0000}", j);
                        batch.Insert(ent);
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
                    TableBatchOperation batch = new TableBatchOperation();

                    for (int j = 0; j < 10; j++)
                    {
                        BaseEntity ent = GenerateRandomEntity(pk);
                        ent.RowKey = string.Format("{0:0000}", j);
                        batch.Insert(ent);
                    }

                    localRTable.ExecuteBatch(batch);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Exception during test case init {0}", ex.ToString());
                    throw;
                }

                IQueryable<BaseEntity> tableQuery = table.CreateQuery<BaseEntity>();
                IQueryable<BaseEntity> rtableQuery = localRTable.CreateQuery<BaseEntity>();

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
                }

                Assert.IsTrue(tableCount == rtableCount, "Query counts are different");
                Assert.IsTrue(tableCount == 20, "Query counts are different");
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

        private static void ExecuteQueryAndAssertResults(CloudTable table, string filter, int expectedResults)
        {
            Assert.AreEqual(expectedResults, table.ExecuteQuery(new TableQuery<ComplexEntity>().Where(filter)).Count());
        }

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
