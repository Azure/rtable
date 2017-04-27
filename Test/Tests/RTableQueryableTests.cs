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
    using Microsoft.WindowsAzure.Storage.Table.Queryable;
    using NUnit.Framework;
    using System;
    using System.Linq;
    using System.Reflection;
    using System.Threading;
    using System.Collections.Generic;
    
    [TestFixture]
    public class RTableQueryableTests : RTableLibraryTestBase
    {
        private const int NumberOfBatches = 15;
        private const int BatchSize = 100;

        private CloudTable currentTable = null;
        private CloudTable complexEntityTable = null;
        private ComplexEntity middleRef = null;

        private ReplicatedTable repComplexTable = null;

        [OneTimeSetUp]
        public void TestFixtureSetup()
        {
            this.LoadTestConfiguration();
            string tableName = this.GenerateRandomTableName();
            this.SetupRTableEnv(tableName);

            // Initialize the table to be queried to the tail replica
            CloudTableClient tableClient = this.repTable.GetTailTableClient();
            this.currentTable = tableClient.GetTableReference(this.repTable.TableName);

            // Bulk Query Entities
            for (int i = 0; i < NumberOfBatches; i++)
            {
                TableBatchOperation batch = new TableBatchOperation();

                for (int j = 0; j < BatchSize; j++)
                {
                    DynamicReplicatedTableEntity ent = GenerateRandomEnitity("tables_batch_" + i.ToString());
                    ent.RowKey = string.Format("{0:0000}", j);

                    batch.Insert(ent);
                }

                this.currentTable.ExecuteBatch(batch);
            }

            // Create another ReplicatedTable.
            tableName = GenerateRandomTableName();
            this.repComplexTable = new ReplicatedTable(tableName, this.configurationService);
            this.repComplexTable.CreateIfNotExists();

            tableClient = repComplexTable.GetTailTableClient();
            this.complexEntityTable = tableClient.GetTableReference(repComplexTable.TableName);

            // Setup
            TableBatchOperation complexBatch = new TableBatchOperation();
            string pk = Guid.NewGuid().ToString();
            for (int m = 0; m < 100; m++)
            {
                ComplexEntity complexEntity = new ComplexEntity(pk, string.Format("{0:0000}", m));
                complexEntity.String = string.Format("{0:0000}", m);
                complexEntity.Binary = new byte[] { 0x01, 0x02, (byte)m };
                complexEntity.BinaryPrimitive = new byte[] { 0x01, 0x02, (byte)m };
                complexEntity.Bool = m % 2 == 0 ? true : false;
                complexEntity.BoolPrimitive = m % 2 == 0 ? true : false;
                complexEntity.Double = m + ((double)m / 100);
                complexEntity.DoublePrimitive = m + ((double)m / 100);
                complexEntity.Int32 = m;
                complexEntity.Int32N = m;
                complexEntity.IntegerPrimitive = m;
                complexEntity.IntegerPrimitiveN = m;
                complexEntity.Int64 = (long)int.MaxValue + m;
                complexEntity.LongPrimitive = (long)int.MaxValue + m;
                complexEntity.LongPrimitiveN = (long)int.MaxValue + m;
                complexEntity.Guid = Guid.NewGuid();

                complexBatch.Insert(complexEntity);

                if (m == 50)
                {
                    middleRef = complexEntity;
                }

                // Add delay to make times unique
                Thread.Sleep(100);
            }
            this.repComplexTable.ExecuteBatch(complexBatch);            
        }

        [OneTimeTearDown]
        public void TestFixtureTearDown()
        {
            base.DeleteAllRtableResources();
            this.repComplexTable.DeleteIfExists();
        }

        //
        // binaries.amd64fre\Gateway\UnitTests>        
        // runuts /fixture=Microsoft.WindowsAzure.Storage.RTableTest.RTableQueryableTests /verbose
        //
        #region Test Methods

        #region Query Segmented
        #region Sync        
        [Test(Description="IQueryable - A test to validate basic table query")]    
        public void TableQueryableExecuteQueryGeneric()
        {
            string tableName = this.GenerateRandomTableName();
            ReplicatedTable localRTable = new ReplicatedTable(tableName, this.configurationService);
            localRTable.CreateIfNotExists();
            RTableWrapperForSampleRTableEntity localRTableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(localRTable);

            CloudTableClient tableClient = localRTable.GetTailTableClient();
            CloudTable table = tableClient.GetTableReference(localRTable.TableName);

            try
            {
                BaseEntity entity = new BaseEntity("mypk", "myrk");
                TableOperation operation = TableOperation.Insert(entity);
                table.Execute(operation);

                IQueryable<BaseEntity> query = table.CreateQuery<BaseEntity>().Where(x => x.PartitionKey == "mypk");
                int itemCount = 0;
                foreach (BaseEntity ent in query.ToList())
                {
                    Assert.AreEqual("mypk", ent.PartitionKey);
                    itemCount++;
                }
                Assert.IsTrue(itemCount > 0, "itemCount = 0");

                IEnumerable<BaseEntity> entities1 = GetEntities<BaseEntity>(table, "mypk");
                itemCount = 0;
                foreach (BaseEntity ent in entities1)
                {
                    Assert.AreEqual("mypk", ent.PartitionKey);
                    itemCount++;
                }
                Assert.IsTrue(itemCount > 0, "itemCount = 0");
            }
            finally
            {
                localRTable.DeleteIfExists();
            }

        }
       
        [Test(Description="IQueryable - A test to validate basic table query")]
        public void TableQueryableExecuteQuerySync()
        {
            TableQuery<DynamicReplicatedTableEntity> query = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                     where ent.PartitionKey == "tables_batch_1"
                                                     select ent).AsTableQuery<DynamicReplicatedTableEntity>();

            IEnumerable<DynamicReplicatedTableEntity> seg = query.Execute();
            int itemCount = 0;
            foreach (DynamicReplicatedTableEntity ent in seg)
            {
                Assert.AreEqual(ent.PartitionKey, "tables_batch_1");
                Assert.AreEqual(ent.Properties.Count, 4);
                itemCount++;
            }
            Assert.IsTrue(itemCount > 0, "itemCount = 0");

            // Try running the query on the Table object.
            List<DynamicReplicatedTableEntity> segList = currentTable.ExecuteQuery(query).ToList();
            itemCount = 0;
            foreach (DynamicReplicatedTableEntity ent in segList)
            {
                Assert.AreEqual(ent.PartitionKey, "tables_batch_1");
                Assert.AreEqual(ent.Properties.Count, 4);
                itemCount++;
            }
            Assert.IsTrue(itemCount > 0, "itemCount = 0");
        }

        [Test(Description="IQueryable - A test to validate basic table query")]
        public void TableQueryableExecuteQueryWithResolverSync()
        {
            TableQuery<string> query = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                        where ent.PartitionKey == "tables_batch_1"
                                        select ent).Resolve((pk, rk, ts, props, etag) => props["a"].StringValue).AsTableQuery();

            IEnumerable<string> seg = query.Execute();

            int count = 0;
            foreach (string ent in seg)
            {
                Assert.AreEqual("a", ent);
                count++;
            }

            Assert.AreEqual(100, count);

            TableQuery<DynamicReplicatedTableEntity> query2 = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                      where ent.PartitionKey == "tables_batch_1"
                                                      select ent).AsTableQuery();

            // Try running the query on the Table object.
            List<string> segList = currentTable.ExecuteQuery(query2, (pk, rk, ts, props, etag) => props["a"].StringValue).ToList();

            count = 0;
            foreach (string ent in segList)
            {
                Assert.AreEqual("a", ent);
                count++;
            }

            Assert.AreEqual(100, count);
        }

        [Test(Description="IQueryable - A test to validate basic table query")]
        public void TableQueryableBasicSync()
        {
            TableQuery<DynamicReplicatedTableEntity> query = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                     where ent.PartitionKey == "tables_batch_1"
                                                     select ent).AsTableQuery();

            TableQuerySegment<DynamicReplicatedTableEntity> seg = query.ExecuteSegmented(null);

            int itemCount = 0;
            foreach (DynamicReplicatedTableEntity ent in seg)
            {
                Assert.AreEqual(ent.PartitionKey, "tables_batch_1");
                Assert.AreEqual(ent.Properties.Count, 4);
                itemCount++;
            }
            Assert.IsTrue(itemCount > 0, "itemCount == 0");

            // Try running the query on the Table object.
            List<DynamicReplicatedTableEntity> segList = currentTable.ExecuteQuerySegmented(query, null).ToList();
            itemCount = 0;
            foreach (DynamicReplicatedTableEntity ent in segList)
            {
                Assert.AreEqual(ent.PartitionKey, "tables_batch_1");
                Assert.AreEqual(ent.Properties.Count, 4);
                itemCount++;
            }
            Assert.IsTrue(itemCount > 0, "itemCount = 0");
        }

        [Test(Description="IQueryable - A test to validate basic table query")]
        public void TableQueryableBasicWithResolverSync()
        {
            TableQuery<string> query = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                        where ent.PartitionKey == "tables_batch_1"
                                        select ent).Resolve((pk, rk, ts, props, etag) => props["a"].StringValue).AsTableQuery();

            TableQuerySegment<string> seg = query.ExecuteSegmented(null);

            int count = 0;
            foreach (string ent in seg)
            {
                Assert.AreEqual("a", ent);
                count++;
            }

            Assert.AreEqual(100, count);

            TableQuery<DynamicReplicatedTableEntity> query2 = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                      where ent.PartitionKey == "tables_batch_1"
                                                      select ent).AsTableQuery();

            // Try running the query on the Table object.
            List<string> segList = currentTable.ExecuteQuerySegmented(query2, (pk, rk, ts, props, etag) => props["a"].StringValue, null).ToList();

            count = 0;
            foreach (string ent in segList)
            {
                Assert.AreEqual("a", ent);
                count++;
            }

            Assert.AreEqual(100, count);
        }

        [Test(Description="IQueryable - A test to validate basic binary comparison operators")]
        public void TableQueryableBinaryOperatorsSync()
        {
            // == 
            TableQuery<DynamicReplicatedTableEntity> query = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                     where ent.PartitionKey == "tables_batch_1"
                                                     select ent).AsTableQuery();

            IEnumerable<DynamicReplicatedTableEntity> seg = query.Execute();
            int count = 0;
            foreach (DynamicReplicatedTableEntity ent in seg)
            {
                Assert.AreEqual("tables_batch_1", ent.PartitionKey, "ent.PartitionKey={0} != tables_batch_1", ent.PartitionKey);
                Assert.AreEqual(4, ent.Properties.Count, "ent.Properties.Count={0} != 4", ent.Properties.Count);
                count++;
            }
            Assert.AreEqual(100, count, "count != 100");

            // !=
            TableQuery<DynamicReplicatedTableEntity> query2 = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                      where ent.PartitionKey == "tables_batch_1" &&
                                                      ent.RowKey != "0050"
                                                      select ent).AsTableQuery();
            IEnumerable<DynamicReplicatedTableEntity> seg2 = query2.Execute();
            count = 0;
            foreach (DynamicReplicatedTableEntity ent in seg2)
            {
                Assert.AreEqual("tables_batch_1", ent.PartitionKey, "ent.PartitionKey={0} != tables_batch_1", ent.PartitionKey);
                Assert.AreEqual(4, ent.Properties.Count, "ent.Properties.Count={0} != 4", ent.Properties.Count);
                Assert.AreNotEqual("0050", ent.RowKey, "ent.RowKey={0} != 0050", ent.RowKey);
                count++;
            }
            Assert.AreEqual(99, count, "count != 99");

            // <
            TableQuery<DynamicReplicatedTableEntity> query3 = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                      where ent.PartitionKey == "tables_batch_1" &&
                                                      ent.RowKey.CompareTo("0050") < 0
                                                      select ent).AsTableQuery();
            IEnumerable<DynamicReplicatedTableEntity> seg3 = query3.Execute();
            count = 0;
            foreach (DynamicReplicatedTableEntity ent in seg3)
            {
                Assert.AreEqual("tables_batch_1", ent.PartitionKey);
                Assert.AreEqual(4, ent.Properties.Count);
                Assert.AreNotEqual("0050", ent.RowKey);
                count++;
            }
            Assert.AreEqual(50, count);

            // >
            TableQuery<DynamicReplicatedTableEntity> query4 = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                      where ent.PartitionKey == "tables_batch_1" &&
                                                      ent.RowKey.CompareTo("0050") > 0
                                                      select ent).AsTableQuery();
            IEnumerable<DynamicReplicatedTableEntity> seg4 = query4.Execute();
            count = 0;
            foreach (DynamicReplicatedTableEntity ent in seg4)
            {
                Assert.AreEqual("tables_batch_1", ent.PartitionKey);
                Assert.AreEqual(4, ent.Properties.Count);
                Assert.AreNotEqual("0050", ent.RowKey);
                count++;
            }
            Assert.AreEqual(49, count);

            // >=
            TableQuery<DynamicReplicatedTableEntity> query5 = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                      where ent.PartitionKey == "tables_batch_1" &&
                                                      ent.RowKey.CompareTo("0050") >= 0
                                                      select ent).AsTableQuery();
            IEnumerable<DynamicReplicatedTableEntity> seg5 = query5.Execute();
            bool flag = false;
            count = 0;
            foreach (DynamicReplicatedTableEntity ent in seg5)
            {
                Assert.AreEqual("tables_batch_1", ent.PartitionKey);
                Assert.AreEqual(4, ent.Properties.Count);
                count++;
                if (ent.RowKey == "0050")
                {
                    flag = true;
                }
            }
            Assert.AreEqual(50, count);
            Assert.IsTrue(flag);

            // <=
            TableQuery<DynamicReplicatedTableEntity> query6 = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                      where ent.PartitionKey == "tables_batch_1" &&
                                                      ent.RowKey.CompareTo("0050") <= 0
                                                      select ent).AsTableQuery();
            IEnumerable<DynamicReplicatedTableEntity> seg6 = query6.Execute();
            count = 0;
            flag = false;
            foreach (DynamicReplicatedTableEntity ent in seg6)
            {
                Assert.AreEqual("tables_batch_1", ent.PartitionKey);
                Assert.AreEqual(4, ent.Properties.Count);
                count++;
                if (ent.RowKey == "0050")
                {
                    flag = true;
                }
            }
            Assert.AreEqual(51, count);
            Assert.IsTrue(flag);

            // +
            TableQuery<ComplexEntity> query7 = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                                                where (ent.Int32) < 1 + 50
                                                select ent).AsTableQuery();
            IEnumerable<ComplexEntity> seg7 = query7.Execute();
            Assert.AreEqual(51, seg7.Count());

            // -
            TableQuery<ComplexEntity> query8 = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                                                where (ent.Int32) < 100 - 50
                                                select ent).AsTableQuery();
            IEnumerable<ComplexEntity> seg8 = query8.Execute();
            Assert.AreEqual(50, seg8.Count());

            // *
            TableQuery<ComplexEntity> query9 = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                                                where (ent.Int32) < 2 * 25
                                                select ent).AsTableQuery();
            IEnumerable<ComplexEntity> seg9 = query9.Execute();
            Assert.AreEqual(50, seg9.Count());

            // /
            TableQuery<ComplexEntity> query10 = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                                                 where (ent.Int32) < 100 / 2
                                                 select ent).AsTableQuery();
            IEnumerable<ComplexEntity> seg10 = query10.Execute();
            Assert.AreEqual(50, seg10.Count());

            // %
            TableQuery<ComplexEntity> query11 = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                                                 where (ent.Int32) < 100 % 12
                                                 select ent).AsTableQuery();
            IEnumerable<ComplexEntity> seg11 = query11.Execute();
            Assert.AreEqual(4, seg11.Count());

            // left shift
            TableQuery<ComplexEntity> query12 = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                                                 where (ent.Int32) < (1 << 2)
                                                 select ent).AsTableQuery();
            IEnumerable<ComplexEntity> seg12 = query12.Execute();
            Assert.AreEqual(4, seg12.Count());

            // right shift
            TableQuery<ComplexEntity> query13 = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                                                 where (ent.Int32) < (8 >> 1)
                                                 select ent).AsTableQuery();
            IEnumerable<ComplexEntity> seg13 = query13.Execute();
            Assert.AreEqual(4, seg13.Count());

            // &
            TableQuery<ComplexEntity> query14 = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                                                 where (ent.Int32) > (2 & 4)
                                                 select ent).AsTableQuery();
            IEnumerable<ComplexEntity> seg14 = query14.Execute();
            Assert.AreEqual(99, seg14.Count());

            // |
            TableQuery<ComplexEntity> query15 = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                                                 where (ent.Int32) > (2 | 4)
                                                 select ent).AsTableQuery();
            IEnumerable<ComplexEntity> seg15 = query15.Execute();
            Assert.AreEqual(93, seg15.Count());
        }

        [Test(Description="IQueryable - A test to validate filter combinations")]
        public void TableQueryableCombineFilters()
        {
            OperationContext opContext = new OperationContext();

            TableQuery<DynamicReplicatedTableEntity> res = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                   where (ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") >= 0) ||
                                                   (ent.PartitionKey == "tables_batch_2" && ent.RowKey.CompareTo("0050") >= 0)
                                                   select ent).WithContext(opContext);

            int count = 0;
            foreach (DynamicReplicatedTableEntity ent in res)
            {
                Assert.AreEqual(ent.Properties["test"].StringValue, "test");
                Assert.IsTrue(ent.RowKey.CompareTo("0050") >= 0);
                count++;
            }

            Assert.AreEqual(100, count);

            TableQuery<DynamicReplicatedTableEntity> res2 = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                    where ((ent.RowKey.CompareTo("0050") == 0 || ent.RowKey.CompareTo("0051") == 0) && ent.PartitionKey == "tables_batch_2")
                                                    select ent).WithContext(opContext);
            count = 0;
            foreach (DynamicReplicatedTableEntity ent in res2)
            {
                count++;
                Assert.AreEqual("tables_batch_2", ent.PartitionKey);
            }

            Assert.AreEqual(2, count);
        }

        [Test(Description="IQueryable - A test to validate First and FirstOrDefault")]
        public void TableQueryableFirst()
        {
            DynamicReplicatedTableEntity res = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                       where (ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") < 0)
                                       select ent).First();
            Assert.AreEqual("tables_batch_1", res.PartitionKey);
            Assert.AreEqual("0000", res.RowKey);

            DynamicReplicatedTableEntity res2 = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                        where (ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") < 0)
                                        select ent).FirstOrDefault();
            Assert.AreEqual("tables_batch_1", res2.PartitionKey);
            Assert.AreEqual("0000", res2.RowKey);

            TestHelper.ExpectedException<TargetInvocationException>(() => (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                                           where (ent.PartitionKey == "tables_batch_16" && ent.RowKey.CompareTo("0050") < 0)
                                                                           select ent).First(), "Invoking First on a query that does not give any results should fail");

            DynamicReplicatedTableEntity res3 = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                        where (ent.PartitionKey == "tables_batch_16" && ent.RowKey.CompareTo("0050") < 0)
                                        select ent).FirstOrDefault();
            Assert.IsNull(res3);
        }

        [Test(Description="IQueryable - A test to validate Single and SingleOrDefault")]
        public void TableQueryableSingle()
        {
            DynamicReplicatedTableEntity res = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                       where (ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") == 0)
                                       select ent).Single();
            Assert.AreEqual("tables_batch_1", res.PartitionKey);
            Assert.AreEqual("0050", res.RowKey);

            DynamicReplicatedTableEntity res2 = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                        where (ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") == 0)
                                        select ent).SingleOrDefault();
            Assert.AreEqual("tables_batch_1", res2.PartitionKey);
            Assert.AreEqual("0050", res2.RowKey);

            TestHelper.ExpectedException<TargetInvocationException>(() => (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                                           where (ent.PartitionKey == "tables_batch_16" && ent.RowKey.CompareTo("0050") == 0)
                                                                           select ent).Single(), "Invoking Single on a query that does not return anything should fail");

            TestHelper.ExpectedException<TargetInvocationException>(() => (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                                           where (ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") > 0)
                                                                           select ent).Single(), "Invoking Single on a query that returns more than one result should fail");

            DynamicReplicatedTableEntity res3 = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                        where (ent.PartitionKey == "tables_batch_16" && ent.RowKey.CompareTo("0050") == 0)
                                        select ent).SingleOrDefault();
            Assert.IsNull(res3);
        }

        [Test(Description="IQueryable - A test to validate Take")]
        public void TableQueryableTake()
        {
            IQueryable<DynamicReplicatedTableEntity> query = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                     where (ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") == 0)
                                                     select ent).Take(1);
            Assert.AreEqual(1, query.ToList().Count);

            IQueryable<DynamicReplicatedTableEntity> query2 = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                      where (ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") > 0)
                                                      select ent).Take(5);
            Assert.AreEqual(5, query2.ToList().Count);

            IQueryable<DynamicReplicatedTableEntity> query3 = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                      where (ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") > 0)
                                                      select ent).Take(51);
            // If we do a Take with more than result number of entities, we just get the max. No error is thrown.
            Assert.AreEqual(49, query3.ToList().Count);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Anvil.RdUsage!Perf", "27142")]    
        [Test(Description="IQueryable - A test to validate multiple Take options")]
        public void TableQueryableMultipleTake()
        {
            IQueryable<DynamicReplicatedTableEntity> query = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                     where (ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") > 0)
                                                     select ent).Take(5).Take(1);
            Assert.AreEqual(1, query.ToList().Count);

            IQueryable<DynamicReplicatedTableEntity> query2 = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                      where (ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") > 0)
                                                      select ent).Take(1).Take(5);
            // Should still give just 1.
            Assert.AreEqual(1, query2.ToList().Count);

            TestHelper.ExpectedException<ArgumentException>(() => (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                                   where (ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") == 0)
                                                                   select ent).Take(1).Take(-1).ToList(), "Negative Take count should fail");
        }

        [Test(Description="IQueryable - A test to validate Cast")]
        public void TableQueryableCast()
        {
            IQueryable<POCOEntity> query = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                            where (ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") > 0)
                                            select ent).Cast<POCOEntity>();
            int count = 0;
            foreach (POCOEntity ent in query)
            {
                Assert.AreEqual("a", ent.a);
                Assert.AreEqual("b", ent.b);
                Assert.AreEqual("c", ent.c);
                Assert.AreEqual("test", ent.test);
                count++;
            }
            Assert.AreEqual(49, count);

            IQueryable<ProjectedPOCO> query2 = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                where (ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") > 0)
                                                select ent).Cast<ProjectedPOCO>();
            int count2 = 0;
            foreach (ProjectedPOCO ent in query2)
            {
                Assert.AreEqual("a", ent.a);
                Assert.AreEqual("b", ent.b);
                Assert.AreEqual("c", ent.c);
                Assert.IsNull(ent.d);
                Assert.AreEqual("test", ent.test);
                count2++;
            }
            Assert.AreEqual(49, count2);
        }

        [Test(Description="IQueryable - A test to validate multiple Cast options")]
        public void TableQueryableMultipleCast()
        {
            IQueryable<ProjectedPOCO> query = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                               where (ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") > 0)
                                               select ent).Cast<POCOEntity>().Cast<ProjectedPOCO>();
            int count = 0;
            foreach (ProjectedPOCO ent in query)
            {
                Assert.AreEqual("a", ent.a);
                Assert.AreEqual("b", ent.b);
                Assert.AreEqual("c", ent.c);
                Assert.IsNull(ent.d);
                Assert.AreEqual("test", ent.test);
                count++;
            }
            Assert.AreEqual(49, count);

            IQueryable<POCOEntity> query2 = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                             where (ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") > 0)
                                             select ent).Cast<ProjectedPOCO>().Cast<POCOEntity>();
            int count2 = 0;
            foreach (POCOEntity ent in query2)
            {
                Assert.AreEqual("a", ent.a);
                Assert.AreEqual("b", ent.b);
                Assert.AreEqual("c", ent.c);
                Assert.AreEqual("test", ent.test);
                count2++;
            }
            Assert.AreEqual(49, count2);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Anvil.RdUsage!Perf", "27143")]       
        [Test(Description="IQueryable - A test to validate ToArray")]
        public void TableQueryableToArray()
        {
            DynamicReplicatedTableEntity[] query = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                           where (ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") == 0)
                                           select ent).Take(1).ToArray();
            Assert.AreEqual(1, query.Length);

            DynamicReplicatedTableEntity[] query2 = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                            where (ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") > 0)
                                            select ent).Take(5).ToArray();
            Assert.AreEqual(5, query2.Length);

            DynamicReplicatedTableEntity[] query3 = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                            where (ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") > 0)
                                            select ent).Take(51).ToArray();
            // If we do a Take with more than result number of entities, we just get the max. No error is thrown.
            Assert.AreEqual(49, query3.Length);
        }

        [Test(Description="IQueryable - A test to use a filter and a predicate")]
        public void TableQueryableFilterPredicate()
        {
            // Filter before key predicate.
            IQueryable<DynamicReplicatedTableEntity> query = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                     where (ent.RowKey != "0050" && ent.PartitionKey == "tables_batch_1")
                                                     select ent);
            int count = 0;
            foreach (DynamicReplicatedTableEntity ent in query)
            {
                Assert.AreEqual("tables_batch_1", ent.PartitionKey);
                Assert.AreNotEqual("0050", ent.RowKey);
                count++;
            }
            Assert.AreEqual(99, count);

            // Key predicate before filter.
            IQueryable<DynamicReplicatedTableEntity> query2 = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                      where (ent.PartitionKey == "tables_batch_1" && ent.RowKey != "0050")
                                                      select ent);
            int count2 = 0;
            foreach (DynamicReplicatedTableEntity ent in query2)
            {
                Assert.AreEqual("tables_batch_1", ent.PartitionKey);
                Assert.AreNotEqual("0050", ent.RowKey);
                count2++;
            }
            Assert.AreEqual(99, count2);
        }

        [Test(Description="IQueryable - A test to use a complex expression filter")]
        public void TableQueryableComplexFilter()
        {
            IQueryable<ComplexEntity> query = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                                               where ((ent.RowKey == "0050" && ent.Int32 == 50) || ((ent.Int32 == 30) && (ent.String == "wrong string" || ent.Bool == true)) || (ent.LongPrimitiveN == (long)int.MaxValue + 50))
                                               select ent);
            int count = 0;
            foreach (ComplexEntity ent in query)
            {
                count++;
                Assert.IsTrue(ent.Int32 == 50 || ent.Int32 == 30);
            }

            Assert.AreEqual(2, count);
        }

        [Test(Description="IQueryable - A test to use a complex expression filter consisting of multiple nested paranthesis")]
        public void TableQueryableNestedParanthesis()
        {
            IQueryable<ComplexEntity> query = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                                               where ((ent.RowKey == "0050" && ent.Int32 == 50) ||
                                               ((ent.Int32 == 30) && (ent.String == "wrong string" || ent.Bool == true) && !(ent.IntegerPrimitive == 31 && ent.LongPrimitive == (long)int.MaxValue + 31)) ||
                                               (ent.LongPrimitiveN == (long)int.MaxValue + 50))
                                               select ent);
            int count = 0;
            foreach (ComplexEntity ent in query)
            {
                count++;
                Assert.IsTrue(ent.Int32 == 50 || ent.Int32 == 30);
            }

            Assert.AreEqual(2, count);
        }

        [Test(Description="IQueryable - A test to validate Unary operators")]
        public void TableQueryableUnary()
        {
            OperationContext opContext = new OperationContext();

            // Not.
            TableQuery<DynamicReplicatedTableEntity> res = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                   where (ent.PartitionKey == "tables_batch_1" &&
                                                   !(ent.RowKey.CompareTo("0050") == 0))
                                                   select ent).WithContext(opContext);
            IEnumerable<DynamicReplicatedTableEntity> queryResult = res.Execute();
            Assert.AreEqual(99, queryResult.Count());
            foreach (DynamicReplicatedTableEntity ent in queryResult)
            {
                Assert.AreNotEqual("0050", ent.RowKey);
            }

            // Unary +.
            TableQuery<ComplexEntity> query = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                                               where +(ent.Int32) < +50
                                               select ent).AsTableQuery();
            IEnumerable<ComplexEntity> seg = query.Execute();
            Assert.AreEqual(50, seg.Count());

            // Unary -.
            TableQuery<ComplexEntity> query2 = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                                                where (ent.Int32) > -1
                                                select ent).AsTableQuery();
            IEnumerable<ComplexEntity> seg2 = query2.Execute();
            Assert.AreEqual(100, seg2.Count());
        }
        
        [Test(Description="IQueryable - A test to validate basic table continuation")]
        public void TableQueryableWithContinuationSync()
        {
            TableQuery<DynamicReplicatedTableEntity> query = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                     select ent).AsTableQuery();

            OperationContext opContext = new OperationContext();
            TableQuerySegment<DynamicReplicatedTableEntity> seg = query.ExecuteSegmented(null, null, opContext);

            int count = 0;
            foreach (DynamicReplicatedTableEntity ent in seg)
            {
                Assert.IsTrue(ent.PartitionKey.StartsWith("tables_batch"));
                Assert.AreEqual(ent.Properties.Count, 4);
                count++;
            }

            // Second segment
            Assert.IsNotNull(seg.ContinuationToken);
            seg = query.ExecuteSegmented(seg.ContinuationToken, null, opContext);
            foreach (DynamicReplicatedTableEntity ent in seg)
            {
                Assert.IsTrue(ent.PartitionKey.StartsWith("tables_batch"));
                Assert.AreEqual(ent.Properties.Count, 4);
                count++;
            }

            Assert.AreEqual(1500, count);
            TestHelper.AssertNAttempts(opContext, 2);
        }

        #endregion Sync

        #region APM
        [Test(Description="IQueryable - A test to validate basic table query APM")]
        public void TableGenericQueryableBasicAPM()
        {
            TableQuery<DynamicReplicatedTableEntity> query = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                     where ent.PartitionKey == "tables_batch_1"
                                                     select ent).AsTableQuery();

            TableQuerySegment<DynamicReplicatedTableEntity> seg = null;
            using (ManualResetEvent evt = new ManualResetEvent(false))
            {
                IAsyncResult asyncRes = null;
                query.BeginExecuteSegmented(null, (res) =>
                {
                    asyncRes = res;
                    evt.Set();
                }, null);
                evt.WaitOne();

                seg = query.EndExecuteSegmented(asyncRes);
            }

            int itemCount = 0;
            foreach (DynamicReplicatedTableEntity ent in seg)
            {
                Assert.AreEqual(ent.PartitionKey, "tables_batch_1");
                Assert.AreEqual(ent.Properties.Count, 4);
                itemCount++;
            }
            Assert.IsTrue(itemCount > 0, "itemCount = 0");

            List<DynamicReplicatedTableEntity> segList = null;
            using (ManualResetEvent evt = new ManualResetEvent(false))
            {
                IAsyncResult asyncRes = null;
                currentTable.BeginExecuteQuerySegmented(query, null, (res) =>
                {
                    asyncRes = res;
                    evt.Set();
                }, null);
                evt.WaitOne();

                segList = currentTable.EndExecuteQuerySegmented<DynamicReplicatedTableEntity>(asyncRes).ToList();
            }

            itemCount = 0;
            foreach (DynamicReplicatedTableEntity ent in segList)
            {
                Assert.AreEqual(ent.PartitionKey, "tables_batch_1");
                Assert.AreEqual(ent.Properties.Count, 4);
                itemCount++;
            }
            Assert.IsTrue(itemCount > 0, "itemCount = 0");
        }

        [Test(Description="IQueryable - A test to validate basic table query APM")]
        public void TableGenericQueryableBasicWithResolverAPM()
        {
            TableQuery<string> query = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                        where ent.PartitionKey == "tables_batch_1"
                                        select ent).Resolve((pk, rk, ts, props, etag) => props["a"].StringValue).AsTableQuery();

            TableQuerySegment<string> seg = null;
            using (ManualResetEvent evt = new ManualResetEvent(false))
            {
                IAsyncResult asyncRes = null;
                query.BeginExecuteSegmented(null, (res) =>
                {
                    asyncRes = res;
                    evt.Set();
                }, null);
                evt.WaitOne();

                seg = query.EndExecuteSegmented(asyncRes);
            }

            int count = 0;
            foreach (string ent in seg)
            {
                Assert.AreEqual("a", ent);
                count++;
            }

            Assert.AreEqual(100, count);

            TableQuery<DynamicReplicatedTableEntity> query2 = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                      where ent.PartitionKey == "tables_batch_1"
                                                      select ent).AsTableQuery();
            List<string> segList = null;
            using (ManualResetEvent evt = new ManualResetEvent(false))
            {
                IAsyncResult asyncRes = null;
                currentTable.BeginExecuteQuerySegmented(query2, (pk, rk, ts, props, etag) => props["a"].StringValue, null, (res) =>
                {
                    asyncRes = res;
                    evt.Set();
                }, null);
                evt.WaitOne();

                segList = currentTable.EndExecuteQuerySegmented<string>(asyncRes).ToList();
            }

            count = 0;
            foreach (string ent in segList)
            {
                Assert.AreEqual("a", ent);
                count++;
            }

            Assert.AreEqual(100, count);
        }

        [Test(Description="IQueryable - A test to validate basic table continuation APM")]
        public void TableGenericQueryableWithContinuationAPM()
        {
            TableQuery<DynamicReplicatedTableEntity> query = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                     select ent).AsTableQuery();

            OperationContext opContext = new OperationContext();
            TableQuerySegment<DynamicReplicatedTableEntity> seg = null;
            using (ManualResetEvent evt = new ManualResetEvent(false))
            {
                IAsyncResult asyncRes = null;
                query.BeginExecuteSegmented(null, null, opContext, (res) =>
                {
                    asyncRes = res;
                    evt.Set();
                }, null);
                evt.WaitOne();

                seg = query.EndExecuteSegmented(asyncRes);
            }

            int count = 0;
            foreach (DynamicReplicatedTableEntity ent in seg)
            {
                Assert.IsTrue(ent.PartitionKey.StartsWith("tables_batch"));
                Assert.AreEqual(ent.Properties.Count, 4);
                count++;
            }

            // Second segment
            Assert.IsNotNull(seg.ContinuationToken);
            using (ManualResetEvent evt = new ManualResetEvent(false))
            {
                IAsyncResult asyncRes = null;
                query.BeginExecuteSegmented(seg.ContinuationToken, null, opContext, (res) =>
                {
                    asyncRes = res;
                    evt.Set();
                }, null);
                evt.WaitOne();

                seg = query.EndExecuteSegmented(asyncRes);
            }

            foreach (DynamicReplicatedTableEntity ent in seg)
            {
                Assert.IsTrue(ent.PartitionKey.StartsWith("tables_batch"));
                Assert.AreEqual(ent.Properties.Count, 4);
                count++;
            }

            Assert.AreEqual(1500, count);
            TestHelper.AssertNAttempts(opContext, 2);
        }
        #endregion APM

        #region Task
        [Test(Description="IQueryable - A test to validate basic table query")]
        public void TableQueryableBasicTask()
        {
            TableQuery<DynamicReplicatedTableEntity> query = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                     where ent.PartitionKey == "tables_batch_1"
                                                     select ent).AsTableQuery();

            TableQuerySegment<DynamicReplicatedTableEntity> seg = query.ExecuteSegmentedAsync(null).Result;

            foreach (DynamicReplicatedTableEntity ent in seg)
            {
                Assert.AreEqual(ent.PartitionKey, "tables_batch_1");
                Assert.AreEqual(ent.Properties.Count, 4);
            }

            // Try running the query on the Table object.
            List<DynamicReplicatedTableEntity> segList = currentTable.ExecuteQuerySegmentedAsync(query, null).Result.ToList();

            foreach (DynamicReplicatedTableEntity ent in segList)
            {
                Assert.AreEqual(ent.PartitionKey, "tables_batch_1");
                Assert.AreEqual(ent.Properties.Count, 4);
            }
        }

        [Test(Description="IQueryable - A test to validate basic table query")]
        public void TableQueryableBasicWithResolverTask()
        {
            TableQuery<string> query = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                        where ent.PartitionKey == "tables_batch_1"
                                        select ent).Resolve((pk, rk, ts, props, etag) => props["a"].StringValue).AsTableQuery();

            TableQuerySegment<string> seg = query.ExecuteSegmentedAsync(null).Result;

            int count = 0;
            foreach (string ent in seg)
            {
                Assert.AreEqual("a", ent);
                count++;
            }

            Assert.AreEqual(100, count);

            TableQuery<DynamicReplicatedTableEntity> query2 = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                      where ent.PartitionKey == "tables_batch_1"
                                                      select ent).AsTableQuery();

            // Try running the query on the Table object.
            List<string> segList = currentTable.ExecuteQuerySegmentedAsync(query2, (pk, rk, ts, props, etag) => props["a"].StringValue, null).Result.ToList();

            count = 0;
            foreach (string ent in segList)
            {
                Assert.AreEqual("a", ent);
                count++;
            }

            Assert.AreEqual(100, count);
        }
        
        [Test(Description="IQueryable - A test to validate basic table continuation")]
        public void TableQueryableWithContinuationTask()
        {
            TableQuery<DynamicReplicatedTableEntity> query = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                     select ent).AsTableQuery();

            OperationContext opContext = new OperationContext();
            TableQuerySegment<DynamicReplicatedTableEntity> seg = query.ExecuteSegmentedAsync(null, null, opContext).Result;

            int count = 0;
            foreach (DynamicReplicatedTableEntity ent in seg)
            {
                Assert.IsTrue(ent.PartitionKey.StartsWith("tables_batch"));
                Assert.AreEqual(ent.Properties.Count, 4);
                count++;
            }

            // Second segment
            Assert.IsNotNull(seg.ContinuationToken);
            seg = query.ExecuteSegmentedAsync(seg.ContinuationToken, null, opContext).Result;

            foreach (DynamicReplicatedTableEntity ent in seg)
            {
                Assert.IsTrue(ent.PartitionKey.StartsWith("tables_batch"));
                Assert.AreEqual(ent.Properties.Count, 4);
                count++;
            }

            Assert.AreEqual(1500, count);
            TestHelper.AssertNAttempts(opContext, 2);
        }

        #endregion Task
        #endregion Query Segmented

        #region Other Tests
        [Test(Description="IQueryable DynamicTableEntityQuery")]
        public void TableQueryableDynamicTableEntityQuery()
        {
            OperationContext opContext = new OperationContext();

            Func<string, string> identityFunc = (s) => s;

            TableQuery<DynamicTableEntity> res = (from ent in complexEntityTable.CreateQuery<DynamicTableEntity>()
                                                  where ent.PartitionKey == middleRef.PartitionKey &&
                                                  ent.Properties[identityFunc("DateTimeOffset")].DateTimeOffsetValue >= middleRef.DateTimeOffset
                                                  select ent).WithContext(opContext);

            List<DynamicTableEntity> entities = res.ToList();

            Assert.AreEqual(entities.Count, 50);
        }

        [Test(Description="IQueryable Where")]
        public void TableQueryableWhere()
        {
            OperationContext opContext = new OperationContext();

            TableQuery<DynamicReplicatedTableEntity> res = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                   where ent.PartitionKey == "tables_batch_1" &&
                                                   ent.RowKey.CompareTo("0050") >= 0
                                                   select ent).WithContext(opContext);

            int count = 0;
            foreach (DynamicReplicatedTableEntity ent in res)
            {
                Assert.AreEqual(ent.Properties["test"].StringValue, "test");

                Assert.AreEqual(ent.PartitionKey, "tables_batch_1");
                Assert.AreEqual(ent.RowKey, string.Format("{0:0000}", count + 50));
                count++;
            }

            Assert.AreEqual(count, 50);
        }

        [Test(Description="IQueryable - A test to validate a query with multiple where clauses")]
        public void TableQueryableMultipleWhereSync()
        {
            OperationContext opContext = new OperationContext();
            
            TableQuery<DynamicReplicatedTableEntity> res = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                   where ent.PartitionKey == "tables_batch_1"
                                                   where ent.RowKey.CompareTo("0050") >= 0
                                                   select ent).WithContext(opContext);

            int count = 0;
            foreach (DynamicReplicatedTableEntity ent in res)
            {
                Assert.AreEqual(ent.Properties["test"].StringValue, "test");
                Assert.AreEqual(ent.PartitionKey, "tables_batch_1");
                Assert.AreEqual(ent.RowKey, string.Format("{0:0000}", count + 50));
                count++;
            }

            Assert.AreEqual(count, 50);
        }

        [Test(Description="TableQueryable - A test to validate basic table continuation & query is able to correctly execute multiple times")]
        public void TableQueryableEnumerateTwice()
        {
            OperationContext opContext = new OperationContext();
            TableQuery<DynamicReplicatedTableEntity> res = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                   select ent).WithContext(opContext);

            List<DynamicReplicatedTableEntity> firstIteration = new List<DynamicReplicatedTableEntity>();
            List<DynamicReplicatedTableEntity> secondIteration = new List<DynamicReplicatedTableEntity>();

            foreach (DynamicReplicatedTableEntity ent in res)
            {
                Assert.IsTrue(ent.PartitionKey.StartsWith("tables_batch"));
                Assert.AreEqual(ent.Properties.Count, 4);
                firstIteration.Add(ent);
            }

            foreach (DynamicReplicatedTableEntity ent in res)
            {
                Assert.IsTrue(ent.PartitionKey.StartsWith("tables_batch"));
                Assert.AreEqual(ent.Properties.Count, 4);
                secondIteration.Add(ent);
            }

            Assert.AreEqual(firstIteration.Count, secondIteration.Count);

            for (int m = 0; m < firstIteration.Count; m++)
            {
                Assert.AreEqual(firstIteration[m].PartitionKey, secondIteration[m].PartitionKey);
                Assert.AreEqual(firstIteration[m].RowKey, secondIteration[m].RowKey);
                Assert.AreEqual(firstIteration[m].Properties.Count, secondIteration[m].Properties.Count);
                Assert.AreEqual(firstIteration[m].Timestamp, secondIteration[m].Timestamp);
                Assert.AreEqual(firstIteration[m].ETag, secondIteration[m].ETag);
            }
        }
       
        [Test(Description="IQueryable Basic projection test")]
        public void TableQueryableProjection()
        {
            OperationContext opContext = new OperationContext();
            var baseQuery = currentTable.CreateQuery<POCOEntity>().WithContext(opContext);

            var pocoRes = (from ent in baseQuery
                           select new ProjectedPOCO()
                           {
                               PartitionKey = ent.PartitionKey,
                               RowKey = ent.RowKey,
                               Timestamp = ent.Timestamp,
                               a = ent.a,
                               c = ent.c
                           });
            int count = 0;

            foreach (ProjectedPOCO ent in pocoRes)
            {
                Assert.IsNotNull(ent.PartitionKey);
                Assert.IsNotNull(ent.RowKey);
                Assert.IsNotNull(ent.Timestamp);

                Assert.AreEqual("a", ent.a);
                Assert.IsNull(ent.b);
                Assert.AreEqual("c", ent.c);
                Assert.IsNull(ent.d);
                count++;
            }

            // Project a single property via Select
            var stringRes = (from ent in baseQuery
                             select ent.b).ToList();

            Assert.AreEqual(stringRes.Count, count);

            // Project a single property and modify it via Select
            TestHelper.ExpectedException<TargetInvocationException>(() => (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                                                                           where ent.RowKey == "0050"
                                                                           select (ent.Int32 + 1)).Single(), "Specifying any operation after last navigation other than take should fail");

            TestHelper.ExpectedException<TargetInvocationException>(() => (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                                                                           where ent.RowKey.CompareTo("0050") > 0
                                                                           select ent.Int32).Skip(5).Single(), "Specifying any operation after last navigation other than take should fail");
            
            // TableQuery.Project no resolver
            IQueryable<POCOEntity> projectionResult = (from ent in baseQuery
                                                       select TableQuery.Project(ent, "a", "b"));
            count = 0;
            foreach (POCOEntity ent in projectionResult)
            {
                Assert.IsNotNull(ent.PartitionKey);
                Assert.IsNotNull(ent.RowKey);
                Assert.IsNotNull(ent.Timestamp);

                Assert.AreEqual("a", ent.a);
                Assert.AreEqual("b", ent.b);
                Assert.IsNull(ent.c);
                Assert.IsNull(ent.test);
                count++;
            }

            Assert.AreEqual(stringRes.Count, count);

            // TableQuery.Project with resolver
            IQueryable<string> resolverRes = (from ent in baseQuery
                                              select TableQuery.Project(ent, "a", "b")).Resolve((pk, rk, ts, props, etag) => props["a"].StringValue);
            count = 0;
            foreach (string s in resolverRes)
            {
                Assert.AreEqual("a", s);
                count++;
            }
            Assert.AreEqual(stringRes.Count, count);

            // Project multiple properties via Select
            IEnumerable<Tuple<string, string>> result = (from ent in baseQuery
                                                         select new Tuple<string, string>
                                                             (
                                                                 ent.a,
                                                                 ent.b
                                                             )).ToList();

            count = 0;
            foreach (Tuple<string, string> entTuple in (IEnumerable<Tuple<string, string>>)result)
            {
                Assert.AreEqual("a", entTuple.Item1);
                Assert.AreEqual("b", entTuple.Item2);
                count++;
            }

            Assert.AreEqual(1500, count);

            // Project with query options
            TableQuery<ProjectedPOCO> queryOptionsResult = (from ent in baseQuery
                                                            where (ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") > 0)
                                                            select new ProjectedPOCO()
                                                            {
                                                                PartitionKey = ent.PartitionKey,
                                                                RowKey = ent.RowKey,
                                                                Timestamp = ent.Timestamp,
                                                                a = ent.a,
                                                                c = ent.c
                                                            }).AsTableQuery<ProjectedPOCO>();

            count = 0;
            foreach (ProjectedPOCO ent in queryOptionsResult)
            {
                Assert.IsNotNull(ent.PartitionKey);
                Assert.IsNotNull(ent.RowKey);
                Assert.IsNotNull(ent.Timestamp);

                Assert.AreEqual("a", ent.a);
                Assert.IsNull(ent.b);
                Assert.AreEqual("c", ent.c);
                Assert.IsNull(ent.d);
                count++;
            }
            Assert.AreEqual(49, count);

            // Project to an anonymous type
            var anonymousRes = (from ent in baseQuery
                                select new
                                {
                                    a = ent.a,
                                    b = ent.b
                                });
            Assert.AreEqual(1500, anonymousRes.ToList().Count);

            // Project with resolver
            IQueryable<string> resolverResult = (from ent in baseQuery
                                                 select new ProjectedPOCO()
                                                 {
                                                     a = ent.a,
                                                     c = ent.c
                                                 }).AsTableQuery<ProjectedPOCO>().Resolve((pk, rk, ts, props, etag) => props["a"].StringValue);
            count = 0;
            foreach (string s in resolverResult)
            {
                Assert.AreEqual("a", s);
                count++;
            }

            Assert.AreEqual(stringRes.Count, count);

            // Single with entity types
            ProjectedPOCO singleRes = (from ent in baseQuery
                                       where ent.PartitionKey == "tables_batch_1" && ent.RowKey == "0050"
                                       select new ProjectedPOCO()
                                       {
                                           PartitionKey = ent.PartitionKey,
                                           RowKey = ent.RowKey,
                                           Timestamp = ent.Timestamp,
                                           a = ent.a,
                                           c = ent.c
                                       }).Single();

            Assert.AreEqual("a", singleRes.a);
            Assert.IsNull(singleRes.b);
            Assert.AreEqual("c", singleRes.c);
            Assert.IsNull(singleRes.d);
            Assert.AreEqual("0050", singleRes.RowKey);

            // SingleOrDefault
            ProjectedPOCO singleOrDefaultRes = (from ent in baseQuery
                                                where ent.PartitionKey == "tables_batch_1" && ent.RowKey == "0050"
                                                select new ProjectedPOCO()
                                                {
                                                    PartitionKey = ent.PartitionKey,
                                                    RowKey = ent.RowKey,
                                                    Timestamp = ent.Timestamp,
                                                    a = ent.a,
                                                    c = ent.c
                                                }).SingleOrDefault();

            Assert.AreEqual("a", singleOrDefaultRes.a);
            Assert.IsNull(singleOrDefaultRes.b);
            Assert.AreEqual("c", singleOrDefaultRes.c);
            Assert.IsNull(singleOrDefaultRes.d);
            Assert.AreEqual("0050", singleOrDefaultRes.RowKey);

            // First with entity types
            ProjectedPOCO firstRes = (from ent in baseQuery
                                      where ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") > 0
                                      select new ProjectedPOCO()
                                      {
                                          PartitionKey = ent.PartitionKey,
                                          RowKey = ent.RowKey,
                                          Timestamp = ent.Timestamp,
                                          a = ent.a,
                                          c = ent.c
                                      }).First();

            Assert.AreEqual("a", firstRes.a);
            Assert.IsNull(firstRes.b);
            Assert.AreEqual("c", firstRes.c);
            Assert.IsNull(firstRes.d);
            Assert.AreEqual("0051", firstRes.RowKey);

            // FirstOrDefault with entity types
            ProjectedPOCO firstOrDefaultRes = (from ent in baseQuery
                                               where ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") > 0
                                               select new ProjectedPOCO()
                                               {
                                                   PartitionKey = ent.PartitionKey,
                                                   RowKey = ent.RowKey,
                                                   Timestamp = ent.Timestamp,
                                                   a = ent.a,
                                                   c = ent.c
                                               }).FirstOrDefault();

            Assert.AreEqual("a", firstOrDefaultRes.a);
            Assert.IsNull(firstOrDefaultRes.b);
            Assert.AreEqual("c", firstOrDefaultRes.c);
            Assert.IsNull(firstOrDefaultRes.d);
            Assert.AreEqual("0051", firstOrDefaultRes.RowKey);
        }

        [Test(Description="IQueryable - validate all supported query types")]
        public void TableQueryableOnSupportedTypes()
        {
            // 1. Filter on String
            var stringQuery = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                               where ent.String.CompareTo("0050") >= 0
                               select ent);

            Assert.AreEqual(50, stringQuery.AsTableQuery().Execute().Count());

            // 2. Filter on Guid
            var guidQuery = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                             where ent.Guid == middleRef.Guid
                             select ent);

            Assert.AreEqual(1, guidQuery.AsTableQuery().Execute().Count());

            // 3. Filter on Long
            var longQuery = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                             where ent.Int64 >= middleRef.Int64
                             select ent);

            Assert.AreEqual(50, longQuery.AsTableQuery().Execute().Count());

            var longPrimitiveQuery = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                                      where ent.LongPrimitive >= middleRef.LongPrimitive
                                      select ent);

            Assert.AreEqual(50, longPrimitiveQuery.AsTableQuery().Execute().Count());

            var longNullableQuery = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                                     where ent.LongPrimitiveN >= middleRef.LongPrimitiveN
                                     select ent);

            Assert.AreEqual(50, longNullableQuery.AsTableQuery().Execute().Count());

            // 4. Filter on Double
            var doubleQuery = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                               where ent.Double >= middleRef.Double
                               select ent);

            Assert.AreEqual(50, doubleQuery.AsTableQuery().Execute().Count());

            var doubleNullableQuery = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                                       where ent.DoublePrimitive >= middleRef.DoublePrimitive
                                       select ent);

            Assert.AreEqual(50, doubleNullableQuery.AsTableQuery().Execute().Count());

            // 5. Filter on Integer
            var int32Query = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                              where ent.Int32 >= middleRef.Int32
                              select ent);

            Assert.AreEqual(50, int32Query.AsTableQuery().Execute().Count());

            var int32NullableQuery = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                                      where ent.Int32N >= middleRef.Int32N
                                      select ent);

            Assert.AreEqual(50, int32NullableQuery.AsTableQuery().Execute().Count());

            // 6. Filter on Date
            var dtoQuery = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                            where ent.DateTimeOffset >= middleRef.DateTimeOffset
                            select ent);

            Assert.AreEqual(50, dtoQuery.AsTableQuery().Execute().Count());

            // 7. Filter on Boolean
            var boolQuery = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                             where ent.Bool == middleRef.Bool
                             select ent);

            Assert.AreEqual(50, boolQuery.AsTableQuery().Execute().Count());

            var boolPrimitiveQuery = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                                      where ent.BoolPrimitive == middleRef.BoolPrimitive
                                      select ent);

            Assert.AreEqual(50, boolPrimitiveQuery.AsTableQuery().Execute().Count());

            // 8. Filter on Binary 
            var binaryQuery = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                               where ent.Binary == middleRef.Binary
                               select ent);

            Assert.AreEqual(1, binaryQuery.AsTableQuery().Execute().Count());

            var binaryPrimitiveQuery = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                                        where ent.BinaryPrimitive == middleRef.BinaryPrimitive
                                        select ent);

            Assert.AreEqual(1, binaryPrimitiveQuery.AsTableQuery().Execute().Count());

            // 10. Complex Filter on Binary GTE

            var complexFilter = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                                 where ent.PartitionKey == middleRef.PartitionKey &&
                                 ent.String.CompareTo("0050") >= 0 &&
                                 ent.Int64 >= middleRef.Int64 &&
                                 ent.LongPrimitive >= middleRef.LongPrimitive &&
                                 ent.LongPrimitiveN >= middleRef.LongPrimitiveN &&
                                 ent.Int32 >= middleRef.Int32 &&
                                 ent.Int32N >= middleRef.Int32N &&
                                 ent.DateTimeOffset >= middleRef.DateTimeOffset
                                 select ent);

            Assert.AreEqual(50, complexFilter.AsTableQuery().Execute().Count());
        }

        // CHECK: In the following function, we cannot use DynamicReplicatedTableEntity instead of
        // DynamicTableEntity as this Query type expects the enities to be of type 
        // DynamicTableEntity (hard coded in line 197 of ExpressionWriter.cs)
        [Test(Description="IQueryable - validate all supported query types")]
        public void TableQueryableOnSupportedTypesViaDynamicTableEntity()
        {
            // 1. Filter on String
            var stringQuery = (from ent in complexEntityTable.CreateQuery<DynamicTableEntity>()
                               where ent.Properties["String"].StringValue.CompareTo("0050") >= 0
                               select ent);

            Assert.AreEqual(50, stringQuery.AsTableQuery().Execute().Count());

            // 2. Filter on Guid
            var guidQuery = (from ent in complexEntityTable.CreateQuery<DynamicTableEntity>()
                             where ent.Properties["Guid"].GuidValue == middleRef.Guid
                             select ent);

            Assert.AreEqual(1, guidQuery.AsTableQuery().Execute().Count());

            // 3. Filter on Long
            var longQuery = (from ent in complexEntityTable.CreateQuery<DynamicTableEntity>()
                             where ent.Properties["Int64"].Int64Value >= middleRef.Int64
                             select ent);

            Assert.AreEqual(50, longQuery.AsTableQuery().Execute().Count());

            var longPrimitiveQuery = (from ent in complexEntityTable.CreateQuery<DynamicTableEntity>()
                                      where ent.Properties["LongPrimitive"].Int64Value >= middleRef.LongPrimitive
                                      select ent);

            Assert.AreEqual(50, longPrimitiveQuery.AsTableQuery().Execute().Count());

            var longNullableQuery = (from ent in complexEntityTable.CreateQuery<DynamicTableEntity>()
                                     where ent.Properties["LongPrimitiveN"].Int64Value >= middleRef.LongPrimitiveN
                                     select ent);

            Assert.AreEqual(50, longNullableQuery.AsTableQuery().Execute().Count());

            // 4. Filter on Double
            var doubleQuery = (from ent in complexEntityTable.CreateQuery<DynamicTableEntity>()
                               where ent.Properties["Double"].DoubleValue >= middleRef.Double
                               select ent);

            Assert.AreEqual(50, doubleQuery.AsTableQuery().Execute().Count());

            var doubleNullableQuery = (from ent in complexEntityTable.CreateQuery<DynamicTableEntity>()
                                       where ent.Properties["DoublePrimitive"].DoubleValue >= middleRef.DoublePrimitive
                                       select ent);

            Assert.AreEqual(50, doubleNullableQuery.AsTableQuery().Execute().Count());

            // 5. Filter on Integer
            var int32Query = (from ent in complexEntityTable.CreateQuery<DynamicTableEntity>()
                              where ent.Properties["Int32"].Int32Value >= middleRef.Int32
                              select ent);

            Assert.AreEqual(50, int32Query.AsTableQuery().Execute().Count());

            var int32NullableQuery = (from ent in complexEntityTable.CreateQuery<DynamicTableEntity>()
                                      where ent.Properties["Int32N"].Int32Value >= middleRef.Int32N
                                      select ent);

            Assert.AreEqual(50, int32NullableQuery.AsTableQuery().Execute().Count());

            // 6. Filter on Date
            var dtoQuery = (from ent in complexEntityTable.CreateQuery<DynamicTableEntity>()
                            where ent.Properties["DateTimeOffset"].DateTimeOffsetValue >= middleRef.DateTimeOffset
                            select ent);

            Assert.AreEqual(50, dtoQuery.AsTableQuery().Execute().Count());

            // 7. Filter on Boolean
            var boolQuery = (from ent in complexEntityTable.CreateQuery<DynamicTableEntity>()
                             where ent.Properties["Bool"].BooleanValue == middleRef.Bool
                             select ent);

            Assert.AreEqual(50, boolQuery.AsTableQuery().Execute().Count());

            var boolPrimitiveQuery = (from ent in complexEntityTable.CreateQuery<DynamicTableEntity>()
                                      where ent.Properties["BoolPrimitive"].BooleanValue == middleRef.BoolPrimitive
                                      select ent);

            Assert.AreEqual(50, boolPrimitiveQuery.AsTableQuery().Execute().Count());

            // 8. Filter on Binary 
            var binaryQuery = (from ent in complexEntityTable.CreateQuery<DynamicTableEntity>()
                               where ent.Properties["Binary"].BinaryValue == middleRef.Binary
                               select ent);

            Assert.AreEqual(1, binaryQuery.AsTableQuery().Execute().Count());

            var binaryPrimitiveQuery = (from ent in complexEntityTable.CreateQuery<DynamicTableEntity>()
                                        where ent.Properties["BinaryPrimitive"].BinaryValue == middleRef.BinaryPrimitive
                                        select ent);

            Assert.AreEqual(1, binaryPrimitiveQuery.AsTableQuery().Execute().Count());

            // 10. Complex Filter on Binary GTE
            var complexFilter = (from ent in complexEntityTable.CreateQuery<DynamicTableEntity>()
                                 where ent.PartitionKey == middleRef.PartitionKey &&
                                 ent.Properties["String"].StringValue.CompareTo("0050") >= 0 &&
                                 ent.Properties["Int64"].Int64Value >= middleRef.Int64 &&
                                 ent.Properties["LongPrimitive"].Int64Value >= middleRef.LongPrimitive &&
                                 ent.Properties["LongPrimitiveN"].Int64Value >= middleRef.LongPrimitiveN &&
                                 ent.Properties["Int32"].Int32Value >= middleRef.Int32 &&
                                 ent.Properties["Int32N"].Int32Value >= middleRef.Int32N &&
                                 ent.Properties["DateTimeOffset"].DateTimeOffsetValue >= middleRef.DateTimeOffset
                                 select ent);

            Assert.AreEqual(50, complexFilter.AsTableQuery().Execute().Count());
        }
        #endregion Other Tests

        #region Negative Tests
        [Test(Description="IQueryable - A test with invalid take count")]
        public void TableQueryableWithInvalidTakeCount()
        {
            try
            {
                var stringQuery = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                                   where ent.String.CompareTo("0050") > 0
                                   select ent).Take(0).ToList();

                Assert.Fail();
            }
            catch (ArgumentException ex)
            {
                Assert.AreEqual("Take count must be positive and greater than 0.", ex.Message);
            }
            catch (Exception)
            {
                Assert.Fail();
            }

            try
            {
                var stringQuery = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                                   where ent.String.CompareTo("0050") > 0
                                   select ent).Take(-1).ToList();
                Assert.Fail();
            }
            catch (ArgumentException ex)
            {
                Assert.AreEqual("Take count must be positive and greater than 0.", ex.Message);
            }
            catch (Exception)
            {
                Assert.Fail();
            }
        }

        [Test(Description="IQueryable - A test with invalid query")]
        public void TableQueryableWithInvalidQuery()
        {
            IQueryable<DynamicReplicatedTableEntity> query = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                     where (ent.PartitionKey == "tables_batch_1" && ent.PartitionKey == "tables_batch_2")
                                                     select ent);
            int count = 0;
            foreach (DynamicReplicatedTableEntity ent in query)
            {
                count++;
            }
            Assert.AreEqual(0, count);
        }

        [Test(Description="IQueryable - multiple from")]
        public void TableQueryableMultipleFrom()
        {
            TableQuery<DynamicReplicatedTableEntity> query = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                     from ent2 in complexEntityTable.CreateQuery<ComplexEntity>()
                                                     where ent.RowKey == ent2.RowKey
                                                     select ent).AsTableQuery();

            TestHelper.ExpectedException<NotSupportedException>(() => query.ExecuteSegmented(null), "Multiple from option not allowed in a query");
        }

        [Test(Description="IQueryable - validate Reverse")]
        public void TableQueryableReverse()
        {
            IQueryable<DynamicReplicatedTableEntity> query = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                     where (ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") > 0)
                                                     select ent).Reverse();
            TestHelper.ExpectedException<TargetInvocationException>(() => query.Count(), "Reverse option is not supported");
        }

        [Test(Description="IQueryable - validate GroupBy")]
        public void TableQueryableGroupBy()
        {
            var query = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                         group ent.Int32 by ent.Int32 % 5 into mod5Group
                         select new
                         {
                             numbers = mod5Group
                         });

            TestHelper.ExpectedException<TargetInvocationException>(() => query.Count(), "group by option is not supported");
        }

        [Test(Description="IQueryable - validate Distinct")]
        public void TableQueryableDistinct()
        {
            IQueryable<string> query = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                        select ent.RowKey).Distinct();

            TestHelper.ExpectedException<TargetInvocationException>(() => query.Count(), "Distinct option is not supported");
        }

        [Test(Description="IQueryable - validate Set and miscellaneous operators")]
        public void TableQueryableSetOperators()
        {
            IQueryable<string> query = (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                        where ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") >= 0
                                        select ent.RowKey);

            IQueryable<string> query2 = (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                                         where ent.RowKey.CompareTo("0050") <= 0
                                         select ent.RowKey);
            // Set operators
            TestHelper.ExpectedException<TargetInvocationException>(() => query.Union(query2).Count(), "Union is not supported");
            TestHelper.ExpectedException<TargetInvocationException>(() => query.Intersect(query2).Count(), "Intersect is not supported");
            TestHelper.ExpectedException<TargetInvocationException>(() => query.Except(query2).Count(), "Except is not supported");

            // Miscellaneous operators
            TestHelper.ExpectedException<TargetInvocationException>(() => query.Concat(query2).Count(), "Concat is not supported");
            TestHelper.ExpectedException<TargetInvocationException>(() => query.SequenceEqual(query2), "SequenceEqual is not supported");
        }

        [Test(Description="IQueryable - validate ElementAt")]
        public void TableQueryableElementAt()
        {
            TestHelper.ExpectedException<TargetInvocationException>(() => (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                                           where (ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") < 0)
                                                                           select ent).ElementAt(0), "ElementAt is not supported");

            TestHelper.ExpectedException<TargetInvocationException>(() => (from ent in currentTable.CreateQuery<DynamicReplicatedTableEntity>()
                                                                           where (ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") < 0)
                                                                           select ent).ElementAtOrDefault(0), "ElementAtOrDefault is not supported");
        }

        [Test(Description="IQueryable - validate various aggregation operators")]
        public void TableQueryableAggregation()
        {
            TestHelper.ExpectedException<TargetInvocationException>(() => (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                                                                           where ent.RowKey.CompareTo("0050") > 0
                                                                           select ent.Int32).Sum(), "Sum is not supported");

            TestHelper.ExpectedException<TargetInvocationException>(() => (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                                                                           where ent.RowKey.CompareTo("0050") > 0
                                                                           select ent.Int32).Min(), "Min is not supported");

            TestHelper.ExpectedException<TargetInvocationException>(() => (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                                                                           where ent.RowKey.CompareTo("0050") > 0
                                                                           select ent.Int32).Max(), "Max is not supported");

            TestHelper.ExpectedException<TargetInvocationException>(() => (from ent in complexEntityTable.CreateQuery<ComplexEntity>()
                                                                           where ent.RowKey.CompareTo("0050") > 0
                                                                           select ent.Int32).Average(), "Average is not supported");
        }
        #endregion Negative Tests

        #endregion Test Methods


        #region Helper
        private IEnumerable<T> GetEntities<T>(CloudTable table, string id) where T : ITableEntity, new()
        {
            IQueryable<T> query = table.CreateQuery<T>()
               .Where(x => x.PartitionKey == "mypk");
            return query.ToList();
        }

        private static DynamicReplicatedTableEntity GenerateRandomEnitity(string pk)
        {
            DynamicReplicatedTableEntity ent = new DynamicReplicatedTableEntity();
            ent.Properties.Add("test", new EntityProperty("test"));
            ent.Properties.Add("a", new EntityProperty("a"));
            ent.Properties.Add("b", new EntityProperty("b"));
            ent.Properties.Add("c", new EntityProperty("c"));

            ent.PartitionKey = pk;
            ent.RowKey = Guid.NewGuid().ToString();
            return ent;
        }
        #endregion Helper

    }
}
