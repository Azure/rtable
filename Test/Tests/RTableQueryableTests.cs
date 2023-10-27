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
    using System.Reflection;
    using System.Threading;
    using System.Collections.Generic;
    using global::Azure.Data.Tables;

    [TestFixture]
    public class RTableQueryableTests : RTableLibraryTestBase
    {
        private const int NumberOfBatches = 15;
        private const int BatchSize = 100;

        private TableClient currentTable = null;
        private TableClient complexEntityTable = null;
        private ComplexEntity middleRef = null;

        private ReplicatedTable repComplexTable = null;

        [OneTimeSetUp]
        public void TestFixtureSetup()
        {
            this.LoadTestConfiguration();
            string tableName = this.GenerateRandomTableName();
            this.SetupRTableEnv(tableName);

            // Initialize the table to be queried to the tail replica
            TableServiceClient tableClient = this.repTable.GetTailTableClient();
            this.currentTable = tableClient.GetTableClient(this.repTable.TableName);

            // Bulk Query Entities
            for (int i = 0; i < NumberOfBatches; i++)
            {
                IList<TableTransactionAction> batch = new List<TableTransactionAction>();

                for (int j = 0; j < BatchSize; j++)
                {
                    DynamicReplicatedTableEntity ent = GenerateRandomEnitity("tables_batch_" + i.ToString());
                    ent.RowKey = string.Format("{0:0000}", j);

                    batch.Add(new TableTransactionAction(TableTransactionActionType.Add, ent, ent.ETag));

                }

                this.currentTable.SubmitTransaction(batch);
            }

            // Create another ReplicatedTable.
            tableName = GenerateRandomTableName();
            this.repComplexTable = new ReplicatedTable(tableName, this.configurationService);
            this.repComplexTable.CreateIfNotExists();

            tableClient = repComplexTable.GetTailTableClient();
            this.complexEntityTable = tableClient.GetTableClient(repComplexTable.TableName);

            // Setup
            IList<TableTransactionAction> complexBatch = new List<TableTransactionAction>();
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

                complexBatch.Add(new TableTransactionAction(TableTransactionActionType.Add, complexEntity, complexEntity.ETag));

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

            TableServiceClient tableClient = localRTable.GetTailTableClient();
            TableClient table = tableClient.GetTableClient(localRTable.TableName);

            try
            {
                BaseEntity entity = new BaseEntity("mypk", "myrk");
                table.AddEntity(entity);

                var query = table.Query<BaseEntity>(x => x.PartitionKey == "mypk");
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
            // Try running the query on the Table object.
            List<DynamicReplicatedTableEntity> segList = currentTable.Query<DynamicReplicatedTableEntity>(e => e.PartitionKey == "tables_batch_1").ToList();
            var itemCount = 0;
            foreach (DynamicReplicatedTableEntity ent in segList)
            {
                Assert.AreEqual(ent.PartitionKey, "tables_batch_1");
                Assert.AreEqual(ent.Properties.Count, 4);
                itemCount++;
            }
            Assert.IsTrue(itemCount > 0, "itemCount = 0");
        }

        [Test(Description = "IQueryable - A test to validate basic table query")]
        public void TableQueryableExecuteQueryWithResolverSync()
        {
            var seg = currentTable.Query<DynamicReplicatedTableEntity>(ent => ent.PartitionKey == "tables_batch_1").Select(e => (string)e["A"]);
            int count = 0;
            foreach (string ent in seg)
            {
                Assert.AreEqual("a", ent);
                count++;
            }

            Assert.AreEqual(100, count);
            // Try running the query on the Table object.
            List<string> segList = currentTable.Query<DynamicReplicatedTableEntity>(ent => ent.PartitionKey == "tables_batch_1").Select(e => (string)e["A"]).ToList();
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
            var query = currentTable.Query<DynamicReplicatedTableEntity>(ent => ent.PartitionKey == "tables_batch_1");
            var seg = query.AsPages().FirstOrDefault();
            int itemCount = 0;
            foreach (DynamicReplicatedTableEntity ent in seg.Values)
            {
                Assert.AreEqual(ent.PartitionKey, "tables_batch_1");
                Assert.AreEqual(ent.Properties.Count, 4);
                itemCount++;
            }
            Assert.IsTrue(itemCount > 0, "itemCount == 0");

            // Try running the query on the Table object.
            List<DynamicReplicatedTableEntity> segList = currentTable.Query<DynamicReplicatedTableEntity>(ent => ent.PartitionKey == "tables_batch_1").ToList();
            itemCount = 0;
            foreach (DynamicReplicatedTableEntity ent in segList)
            {
                Assert.AreEqual(ent.PartitionKey, "tables_batch_1");
                Assert.AreEqual(ent.Properties.Count, 4);
                itemCount++;
            }
            Assert.IsTrue(itemCount > 0, "itemCount = 0");
        }

        [Test(Description = "IQueryable - A test to validate basic table query")]
        public void TableQueryableBasicWithResolverSync()
        {
            var seg = currentTable.Query<DynamicReplicatedTableEntity>(ent => ent.PartitionKey == "tables_batch_1").Select(e => (string)e["A"]);
            int count = 0;
            foreach (string ent in seg)
            {
                Assert.AreEqual("a", ent);
                count++;
            }

            Assert.AreEqual(100, count);

            // Try running the query on the Table object.
            List<string> segList = currentTable.Query<DynamicReplicatedTableEntity>(ent => ent.PartitionKey == "tables_batch_1").Select(e => (string)e["A"]).ToList();
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
            var seg = currentTable.Query<DynamicReplicatedTableEntity>(ent => ent.PartitionKey == "tables_batch_1");
            int count = 0;
            foreach (DynamicReplicatedTableEntity ent in seg)
            {
                Assert.AreEqual("tables_batch_1", ent.PartitionKey, "ent.PartitionKey={0} != tables_batch_1", ent.PartitionKey);
                Assert.AreEqual(4, ent.Properties.Count, "ent.Properties.Count={0} != 4", ent.Properties.Count);
                count++;
            }
            Assert.AreEqual(100, count, "count != 100");

            // !=
            var seg2 = currentTable.Query<DynamicReplicatedTableEntity>(ent => ent.PartitionKey == "tables_batch_1" && ent.RowKey != "0050");
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
            var seg3 = currentTable.Query<DynamicReplicatedTableEntity>(ent => ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") < 0);
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
            var seg4 = currentTable.Query<DynamicReplicatedTableEntity>(ent => ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") > 0);
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
            var seg5 = currentTable.Query<DynamicReplicatedTableEntity>(ent => ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") >= 0);
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
            var seg6 = currentTable.Query<DynamicReplicatedTableEntity>(ent => ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") <= 0);
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
            var seg7 = currentTable.Query<ComplexEntity>(ent => ent.Int32 < 1 + 50);
            Assert.AreEqual(51, seg7.Count());

            // -
            var seg8 = currentTable.Query<ComplexEntity>(ent => ent.Int32 < 100 - 50);
            Assert.AreEqual(50, seg8.Count());

            // *
            var seg9 = currentTable.Query<ComplexEntity>(ent => ent.Int32 < 2 * 25);
            Assert.AreEqual(50, seg9.Count());

            // /
            var seg10 = currentTable.Query<ComplexEntity>(ent => ent.Int32 < 100 / 2);
            Assert.AreEqual(50, seg10.Count());

            // %
            var seg11 = currentTable.Query<ComplexEntity>(ent => ent.Int32 < 100 % 12);
            Assert.AreEqual(4, seg11.Count());

            // left shift
            var seg12 = currentTable.Query<ComplexEntity>(ent => ent.Int32 < (1 << 2));
            Assert.AreEqual(4, seg12.Count());

            // right shift
            var seg13 = currentTable.Query<ComplexEntity>(ent => ent.Int32 < (8 >> 1));
            Assert.AreEqual(4, seg13.Count());

            // &
            var seg14 = currentTable.Query<ComplexEntity>(ent => ent.Int32 < (2 & 4));
            Assert.AreEqual(99, seg14.Count());

            // |
            var seg15 = currentTable.Query<ComplexEntity>(ent => ent.Int32 < (2 | 4));
            Assert.AreEqual(93, seg15.Count());
        }

        [Test(Description="IQueryable - A test to validate filter combinations")]
        public void TableQueryableCombineFilters()
        {
            var res = currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                (ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") >= 0) ||
                (ent.PartitionKey == "tables_batch_2" && ent.RowKey.CompareTo("0050") >= 0));
            int count = 0;
            foreach (DynamicReplicatedTableEntity ent in res)
            {
                Assert.AreEqual((string)ent.Properties["test"], "test");
                Assert.IsTrue(ent.RowKey.CompareTo("0050") >= 0);
                count++;
            }

            Assert.AreEqual(100, count);

            var res2 = currentTable.Query<DynamicReplicatedTableEntity>(ent => 
                (ent.RowKey.CompareTo("0050") == 0 || ent.RowKey.CompareTo("0051") == 0) && 
                ent.PartitionKey == "tables_batch_2");
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
            DynamicReplicatedTableEntity res = currentTable.Query<DynamicReplicatedTableEntity>(ent => 
                ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") < 0)
                .First();
            Assert.AreEqual("tables_batch_1", res.PartitionKey);
            Assert.AreEqual("0000", res.RowKey);

            DynamicReplicatedTableEntity res2 = currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") < 0)
                .FirstOrDefault();
            Assert.AreEqual("tables_batch_1", res2.PartitionKey);
            Assert.AreEqual("0000", res2.RowKey);

            TestHelper.ExpectedException<TargetInvocationException>(() => currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                ent.PartitionKey == "tables_batch_16" && ent.RowKey.CompareTo("0050") < 0).First(), "Invoking First on a query that does not give any results should fail");

            DynamicReplicatedTableEntity res3 = currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                ent.PartitionKey == "tables_batch_16" && ent.RowKey.CompareTo("0050") < 0)
                .FirstOrDefault();
            Assert.IsNull(res3);
        }

        [Test(Description="IQueryable - A test to validate Single and SingleOrDefault")]
        public void TableQueryableSingle()
        {
            DynamicReplicatedTableEntity res = currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") == 0)
                .Single();
            Assert.AreEqual("tables_batch_1", res.PartitionKey);
            Assert.AreEqual("0050", res.RowKey);

            DynamicReplicatedTableEntity res2 = currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") == 0)
                .SingleOrDefault();
            Assert.AreEqual("tables_batch_1", res2.PartitionKey);
            Assert.AreEqual("0050", res2.RowKey);

            TestHelper.ExpectedException<TargetInvocationException>(() => currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                ent.PartitionKey == "tables_batch_16" && ent.RowKey.CompareTo("0050") == 0).Single(), "Invoking Single on a query that does not return anything should fail");
            TestHelper.ExpectedException<TargetInvocationException>(() => currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") > 0).Single(), "Invoking Single on a query that returns more than one result should fail");

            DynamicReplicatedTableEntity res3 = currentTable.Query<DynamicReplicatedTableEntity>(ent =>
               ent.PartitionKey == "tables_batch_16" && ent.RowKey.CompareTo("0050") == 0)
                .SingleOrDefault();
            Assert.IsNull(res3);
        }

        [Test(Description="IQueryable - A test to validate Take")]
        public void TableQueryableTake()
        {
            var query = currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") == 0)
                .Take(1);
            Assert.AreEqual(1, query.ToList().Count);

            var query2 = currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") > 0)
                .Take(5);
            Assert.AreEqual(5, query2.ToList().Count);

            var query3 = currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") > 0)
                .Take(51);
            // If we do a Take with more than result number of entities, we just get the max. No error is thrown.
            Assert.AreEqual(49, query3.ToList().Count);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Anvil.RdUsage!Perf", "27142")]    
        [Test(Description="IQueryable - A test to validate multiple Take options")]
        public void TableQueryableMultipleTake()
        {
            var query = currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") > 0)
                .Take(5).Take(1);
            Assert.AreEqual(1, query.ToList().Count);

            var query2 = currentTable.Query<DynamicReplicatedTableEntity>(ent =>
               ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") > 0)
               .Take(1).Take(5);
            // Should still give just 1.
            Assert.AreEqual(1, query2.ToList().Count);

            TestHelper.ExpectedException<ArgumentException>(() => currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") == 0).Take(1).Take(-1).ToList(), "Negative Take count should fail");
        }

        [Test(Description="IQueryable - A test to validate Cast")]
        public void TableQueryableCast()
        {
            var query = currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") > 0)
                .Cast<POCOEntity>();
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

            var query2 = currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") > 0)
                .Cast<ProjectedPOCO>();
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
            var query = currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") > 0)
                .Cast<POCOEntity>().Cast<ProjectedPOCO>();
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

            var query2 = currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") > 0)
                .Cast<ProjectedPOCO>().Cast<POCOEntity>();
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
            var query = currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") == 0)
                .Take(1).ToArray();
            Assert.AreEqual(1, query.Length);

            var query2 = currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") > 0)
                .Take(5).ToArray();
            Assert.AreEqual(5, query2.Length);

            var query3 = currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") > 0)
                .Take(51).ToArray();
            // If we do a Take with more than result number of entities, we just get the max. No error is thrown.
            Assert.AreEqual(49, query3.Length);
        }

        [Test(Description="IQueryable - A test to use a filter and a predicate")]
        public void TableQueryableFilterPredicate()
        {
            // Filter before key predicate.
            var query = currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                ent.RowKey != "0050" && ent.PartitionKey == "tables_batch_1");
            int count = 0;
            foreach (DynamicReplicatedTableEntity ent in query)
            {
                Assert.AreEqual("tables_batch_1", ent.PartitionKey);
                Assert.AreNotEqual("0050", ent.RowKey);
                count++;
            }
            Assert.AreEqual(99, count);

            // Key predicate before filter.
            var query2 = currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                ent.PartitionKey == "tables_batch_1" && ent.RowKey != "0050");
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
            var query = currentTable.Query<ComplexEntity>(ent =>
                (ent.RowKey == "0050" && ent.Int32 == 50) || ((ent.Int32 == 30) && (ent.String == "wrong string" || ent.Bool == true)));
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
            var query = currentTable.Query<ComplexEntity>(ent =>
                (ent.RowKey == "0050" && ent.Int32 == 50) ||
                ((ent.Int32 == 30) && (ent.String == "wrong string" || ent.Bool == true) && !(ent.IntegerPrimitive == 31 && ent.LongPrimitive == (long)int.MaxValue + 31)) ||
                (ent.LongPrimitiveN == (long)int.MaxValue + 50));
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
            // Not.
            var queryResult = currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                ent.PartitionKey == "tables_batch_1" &&
                !(ent.RowKey.CompareTo("0050") == 0));
            Assert.AreEqual(99, queryResult.Count());
            foreach (DynamicReplicatedTableEntity ent in queryResult)
            {
                Assert.AreNotEqual("0050", ent.RowKey);
            }

            // Unary +.
            var seg = currentTable.Query<ComplexEntity>(ent =>
                +ent.Int32 < +50);
            Assert.AreEqual(50, seg.Count());

            // Unary -.
            var seg2 = currentTable.Query<ComplexEntity>(ent =>
                ent.Int32 > -1);
            Assert.AreEqual(100, seg2.Count());
        }
        
        [Test(Description="IQueryable - A test to validate basic table continuation")]
        public void TableQueryableWithContinuationSync()
        {
            var query = currentTable.Query<DynamicReplicatedTableEntity>(e => true);
            var seg = query.AsPages().FirstOrDefault();
            int count = 0;
            foreach (DynamicReplicatedTableEntity ent in seg.Values)
            {
                Assert.IsTrue(ent.PartitionKey.StartsWith("tables_batch"));
                Assert.AreEqual(ent.Properties.Count, 4);
                count++;
            }

            // Second segment
            Assert.IsNotNull(seg.ContinuationToken);
            seg = query.AsPages(seg.ContinuationToken).FirstOrDefault();
            foreach (DynamicReplicatedTableEntity ent in seg.Values)
            {
                Assert.IsTrue(ent.PartitionKey.StartsWith("tables_batch"));
                Assert.AreEqual(ent.Properties.Count, 4);
                count++;
            }

            Assert.AreEqual(1500, count);
        }

        #endregion Sync

        #region Task
        [Test(Description="IQueryable - A test to validate basic table query")]
        public void TableQueryableBasicTask()
        {
            var query = currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                ent.PartitionKey == "tables_batch_1");
            var seg = query.AsPages().FirstOrDefault();

            foreach (DynamicReplicatedTableEntity ent in seg.Values)
            {
                Assert.AreEqual(ent.PartitionKey, "tables_batch_1");
                Assert.AreEqual(ent.Properties.Count, 4);
            }

            // Try running the query on the Table object.
            List<DynamicReplicatedTableEntity> segList = query.AsPages(seg.ContinuationToken).FirstOrDefault().Values.ToList();

            foreach (DynamicReplicatedTableEntity ent in segList)
            {
                Assert.AreEqual(ent.PartitionKey, "tables_batch_1");
                Assert.AreEqual(ent.Properties.Count, 4);
            }
        }

        [Test(Description = "IQueryable - A test to validate basic table query")]
        public void TableQueryableBasicWithResolverTask()
        {
            var seg = currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                ent.PartitionKey == "tables_batch_1")
                .Select(e => (string)e["A"]);

            int count = 0;
            foreach (string ent in seg)
            {
                Assert.AreEqual("a", ent);
                count++;
            }

            Assert.AreEqual(100, count);
            
            // Try running the query on the Table object.
            var segList = currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                ent.PartitionKey == "tables_batch_1")
                .Select(e => (string)e["A"])
                .ToList();

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
            var query = currentTable.Query<DynamicReplicatedTableEntity>(e => true);
            var seg = query.AsPages().FirstOrDefault();

            int count = 0;
            foreach (DynamicReplicatedTableEntity ent in seg.Values)
            {
                Assert.IsTrue(ent.PartitionKey.StartsWith("tables_batch"));
                Assert.AreEqual(ent.Properties.Count, 4);
                count++;
            }

            // Second segment
            Assert.IsNotNull(seg.ContinuationToken);
            seg = query.AsPages(seg.ContinuationToken).FirstOrDefault();

            foreach (DynamicReplicatedTableEntity ent in seg.Values)
            {
                Assert.IsTrue(ent.PartitionKey.StartsWith("tables_batch"));
                Assert.AreEqual(ent.Properties.Count, 4);
                count++;
            }

            Assert.AreEqual(1500, count);
        }

        #endregion Task
        #endregion Query Segmented

        #region Other Tests
        [Test(Description="IQueryable DynamicTableEntityQuery")]
        public void TableQueryableDynamicTableEntityQuery()
        {
            Func<string, string> identityFunc = (s) => s;

            var res = complexEntityTable.Query<TableEntity>(ent =>
                ent.PartitionKey == middleRef.PartitionKey &&
                ent.GetDateTimeOffset("DateTimeOffset") >= middleRef.DateTimeOffset);

            List<TableEntity> entities = res.ToList();

            Assert.AreEqual(entities.Count, 50);
        }

        [Test(Description="IQueryable Where")]
        public void TableQueryableWhere()
        {
            var res = currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                ent.PartitionKey == "tables_batch_1" &&
                ent.RowKey.CompareTo("0050") >= 0);

            int count = 0;
            foreach (DynamicReplicatedTableEntity ent in res)
            {
                Assert.AreEqual((string)ent.Properties["test"], "test");

                Assert.AreEqual(ent.PartitionKey, "tables_batch_1");
                Assert.AreEqual(ent.RowKey, string.Format("{0:0000}", count + 50));
                count++;
            }

            Assert.AreEqual(count, 50);
        }

        [Test(Description="TableQueryable - A test to validate basic table continuation & query is able to correctly execute multiple times")]
        public void TableQueryableEnumerateTwice()
        {
            var res = currentTable.Query<DynamicReplicatedTableEntity>(e => true);

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
            var baseQuery = currentTable.Query<POCOEntity>(e => true);
            var pocoRes = baseQuery.Select(ent => new ProjectedPOCO()
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
            TestHelper.ExpectedException<TargetInvocationException>(() => complexEntityTable.Query<ComplexEntity>(ent =>
                ent.RowKey == "0050").Select(ent => ent.Int32 + 1).Single(), "Specifying any operation after last navigation other than take should fail");

            TestHelper.ExpectedException<TargetInvocationException>(() => complexEntityTable.Query<ComplexEntity>(ent =>
                ent.RowKey.CompareTo("0050") > 0).Select(ent => ent.Int32).Skip(5).Single(), "Specifying any operation after last navigation other than take should fail");

            // TableQuery.Project no resolver
            var projectionResult = currentTable.Query<POCOEntity>(e => true, select: new List<string>() { "a", "b" });
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
            var resolverRes = currentTable.Query<POCOEntity>(e => true, select: new List<string>() { "a", "b" })
                .Select(e => e.a);
            count = 0;
            foreach (string s in resolverRes)
            {
                Assert.AreEqual("a", s);
                count++;
            }
            Assert.AreEqual(stringRes.Count, count);

            // Project multiple properties via Select
            var result = currentTable.Query<POCOEntity>(e => true, select: new List<string>() { "a", "b" })
                .Select(ent => new Tuple<string, string>
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
            var queryOptionsResult = currentTable.Query<POCOEntity>(ent =>
                ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") > 0, select: new List<string>() { "a", "b" })
                .Select(ent => new ProjectedPOCO()
                {
                    PartitionKey = ent.PartitionKey,
                    RowKey = ent.RowKey,
                    Timestamp = ent.Timestamp,
                    a = ent.a,
                    c = ent.c
                });

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
            var resolverResult = currentTable.Query<POCOEntity>(e => true)
                .Select(ent => new ProjectedPOCO()
                {
                    a = ent.a,
                    c = ent.c
                }).Select(ent => ent.a);
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
            var stringQuery = complexEntityTable.Query<ComplexEntity>(ent => 
                ent.String.CompareTo("0050") >= 0);

            Assert.AreEqual(50, stringQuery.Count());

            // 2. Filter on Guid
            var guidQuery = complexEntityTable.Query<ComplexEntity>(ent =>
                ent.Guid == middleRef.Guid);

            Assert.AreEqual(1, guidQuery.Count());

            // 3. Filter on Long
            var longQuery = complexEntityTable.Query<ComplexEntity>(ent =>
                ent.Int64 >= middleRef.Int64);

            Assert.AreEqual(50, longQuery.Count());

            var longPrimitiveQuery = complexEntityTable.Query<ComplexEntity>(ent =>
                ent.LongPrimitive >= middleRef.LongPrimitive);

            Assert.AreEqual(50, longPrimitiveQuery.Count());

            var longNullableQuery = complexEntityTable.Query<ComplexEntity>(ent =>
                ent.LongPrimitiveN >= middleRef.LongPrimitiveN);

            Assert.AreEqual(50, longNullableQuery.Count());

            // 4. Filter on Double
            var doubleQuery = complexEntityTable.Query<ComplexEntity>(ent =>
                ent.Double >= middleRef.Double);

            Assert.AreEqual(50, doubleQuery.Count());

            var doubleNullableQuery = complexEntityTable.Query<ComplexEntity>(ent =>
                ent.DoublePrimitive >= middleRef.DoublePrimitive);

            Assert.AreEqual(50, doubleNullableQuery.Count());

            // 5. Filter on Integer
            var int32Query = complexEntityTable.Query<ComplexEntity>(ent =>
                ent.Int32 >= middleRef.Int32);

            Assert.AreEqual(50, int32Query.Count());

            var int32NullableQuery = complexEntityTable.Query<ComplexEntity>(ent =>
                ent.Int32N >= middleRef.Int32N);

            Assert.AreEqual(50, int32NullableQuery.Count());

            // 6. Filter on Date
            var dtoQuery = complexEntityTable.Query<ComplexEntity>(ent =>
                ent.DateTimeOffset >= middleRef.DateTimeOffset);

            Assert.AreEqual(50, dtoQuery.Count());

            // 7. Filter on Boolean
            var boolQuery = complexEntityTable.Query<ComplexEntity>(ent =>
                ent.Bool == middleRef.Bool);

            Assert.AreEqual(50, boolQuery.Count());

            var boolPrimitiveQuery = complexEntityTable.Query<ComplexEntity>(ent =>
                ent.BoolPrimitive == middleRef.BoolPrimitive);

            Assert.AreEqual(50, boolPrimitiveQuery.Count());

            // 8. Filter on Binary 
            var binaryQuery = complexEntityTable.Query<ComplexEntity>(ent =>
                ent.Binary == middleRef.Binary);

            Assert.AreEqual(1, binaryQuery.Count());

            var binaryPrimitiveQuery = complexEntityTable.Query<ComplexEntity>(ent =>
                ent.BinaryPrimitive == middleRef.BinaryPrimitive);

            Assert.AreEqual(1, binaryPrimitiveQuery.Count());

            // 10. Complex Filter on Binary GTE
            var complexFilter = complexEntityTable.Query<ComplexEntity>(ent =>
                ent.PartitionKey == middleRef.PartitionKey &&
                ent.String.CompareTo("0050") >= 0 &&
                ent.Int64 >= middleRef.Int64 &&
                ent.LongPrimitive >= middleRef.LongPrimitive &&
                ent.LongPrimitiveN >= middleRef.LongPrimitiveN &&
                ent.Int32 >= middleRef.Int32 &&
                ent.Int32N >= middleRef.Int32N &&
                ent.DateTimeOffset >= middleRef.DateTimeOffset);

            Assert.AreEqual(50, complexFilter.Count());
        }

        // CHECK: In the following function, we cannot use DynamicReplicatedTableEntity instead of
        // DynamicTableEntity as this Query type expects the enities to be of type 
        // DynamicTableEntity (hard coded in line 197 of ExpressionWriter.cs)
        [Test(Description="IQueryable - validate all supported query types")]
        public void TableQueryableOnSupportedTypesViaDynamicTableEntity()
        {
            // 1. Filter on String
            var stringQuery = complexEntityTable.Query<TableEntity>(ent =>
                ent.GetString("String").CompareTo("0050") >= 0);

            Assert.AreEqual(50, stringQuery.Count());

            // 2. Filter on Guid
            var guidQuery = complexEntityTable.Query<TableEntity>(ent =>
                ent.GetGuid("Guid") == middleRef.Guid);

            Assert.AreEqual(1, guidQuery.Count());

            // 3. Filter on Long
            var longQuery = complexEntityTable.Query<TableEntity>(ent =>
                ent.GetInt64("Int64") >= middleRef.Int64);

            Assert.AreEqual(50, longQuery.Count());

            var longPrimitiveQuery = complexEntityTable.Query<TableEntity>(ent =>
                ent.GetInt64("LongPrimitive") >= middleRef.LongPrimitive);

            Assert.AreEqual(50, longPrimitiveQuery.Count());

            var longNullableQuery = complexEntityTable.Query<TableEntity>(ent =>
                ent.GetInt64("LongPrimitiveN") >= middleRef.LongPrimitiveN);

            Assert.AreEqual(50, longNullableQuery.Count());

            // 4. Filter on Double
            var doubleQuery = complexEntityTable.Query<TableEntity>(ent =>
                ent.GetDouble("Double") >= middleRef.Double);

            Assert.AreEqual(50, doubleQuery.Count());

            var doubleNullableQuery = complexEntityTable.Query<TableEntity>(ent =>
                ent.GetDouble("DoublePrimitive") >= middleRef.DoublePrimitive);

            Assert.AreEqual(50, doubleNullableQuery.Count());

            // 5. Filter on Integer
            var int32Query = complexEntityTable.Query<TableEntity>(ent =>
                ent.GetInt32("Int32") >= middleRef.Int32);

            Assert.AreEqual(50, int32Query.Count());

            var int32NullableQuery = complexEntityTable.Query<TableEntity>(ent =>
                ent.GetInt32("Int32N") >= middleRef.Int32N);

            Assert.AreEqual(50, int32NullableQuery.Count());

            // 6. Filter on Date
            var dtoQuery = complexEntityTable.Query<TableEntity>(ent =>
                ent.GetDateTimeOffset("DateTimeOffset") >= middleRef.DateTimeOffset);

            Assert.AreEqual(50, dtoQuery.Count());

            // 7. Filter on Boolean
            var boolQuery = complexEntityTable.Query<TableEntity>(ent =>
                ent.GetBoolean("Bool") == middleRef.Bool);

            Assert.AreEqual(50, boolQuery.Count());

            var boolPrimitiveQuery = complexEntityTable.Query<TableEntity>(ent =>
                ent.GetBoolean("BoolPrimitive") == middleRef.BoolPrimitive);

            Assert.AreEqual(50, boolPrimitiveQuery.Count());

            // 8. Filter on Binary 
            var binaryQuery = complexEntityTable.Query<TableEntity>(ent =>
                ent.GetBinary("Binary") == middleRef.Binary);

            Assert.AreEqual(1, binaryQuery.Count());

            var binaryPrimitiveQuery = complexEntityTable.Query<TableEntity>(ent =>
                ent.GetBinary("BinaryPrimitive") == middleRef.BinaryPrimitive);

            Assert.AreEqual(1, binaryPrimitiveQuery.Count());

            // 10. Complex Filter on Binary GTE
            var complexFilter = complexEntityTable.Query<TableEntity>(ent =>
                ent.PartitionKey == middleRef.PartitionKey &&
                ent.GetString("String").CompareTo("0050") >= 0 &&
                ent.GetInt64("Int64") >= middleRef.Int64 &&
                ent.GetInt64("LongPrimitive") >= middleRef.LongPrimitive &&
                ent.GetInt64("LongPrimitiveN") >= middleRef.LongPrimitiveN &&
                ent.GetInt32("Int32") >= middleRef.Int32 &&
                ent.GetInt32("Int32N") >= middleRef.Int32N &&
                ent.GetDateTimeOffset("DateTimeOffset") >= middleRef.DateTimeOffset);

            Assert.AreEqual(50, complexFilter.Count());
        }
        #endregion Other Tests

        #region Negative Tests
        [Test(Description="IQueryable - A test with invalid take count")]
        public void TableQueryableWithInvalidTakeCount()
        {
            try
            {
                var stringQuery = complexEntityTable.Query<ComplexEntity>(ent =>
                    ent.String.CompareTo("0050") > 0)
                    .Take(0).ToList();

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
                var stringQuery = complexEntityTable.Query<ComplexEntity>(ent =>
                    ent.String.CompareTo("0050") > 0)
                    .Take(-1).ToList();
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
            var query = currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                ent.PartitionKey == "tables_batch_1" && ent.PartitionKey == "tables_batch_2");
            int count = 0;
            foreach (DynamicReplicatedTableEntity ent in query)
            {
                count++;
            }
            Assert.AreEqual(0, count);
        }

        [Test(Description="IQueryable - validate Reverse")]
        public void TableQueryableReverse()
        {
            var query = currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") > 0)
                .Reverse();
            TestHelper.ExpectedException<TargetInvocationException>(() => query.Count(), "Reverse option is not supported");
        }

        [Test(Description="IQueryable - validate Distinct")]
        public void TableQueryableDistinct()
        {
            var query = currentTable.Query<DynamicReplicatedTableEntity>(e => true)
                .Distinct();

            TestHelper.ExpectedException<TargetInvocationException>(() => query.Count(), "Distinct option is not supported");
        }

        [Test(Description="IQueryable - validate Set and miscellaneous operators")]
        public void TableQueryableSetOperators()
        {
            var query = currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") >= 0)
                .Select(e => e.RowKey);

            var query2 = currentTable.Query<DynamicReplicatedTableEntity>(ent =>
                ent.RowKey.CompareTo("0050") <= 0)
                .Select(e => e.RowKey);
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
            TestHelper.ExpectedException<TargetInvocationException>(() => currentTable.Query<DynamicReplicatedTableEntity>(ent =>
               ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") < 0).ElementAt(0), "ElementAt is not supported");

            TestHelper.ExpectedException<TargetInvocationException>(() => currentTable.Query<DynamicReplicatedTableEntity>(ent =>
               ent.PartitionKey == "tables_batch_1" && ent.RowKey.CompareTo("0050") < 0).ElementAtOrDefault(0), "ElementAtOrDefault is not supported");
        }

        [Test(Description="IQueryable - validate various aggregation operators")]
        public void TableQueryableAggregation()
        {
            TestHelper.ExpectedException<TargetInvocationException>(() => currentTable.Query<ComplexEntity>(ent =>
                ent.RowKey.CompareTo("0050") > 0).Select(ent => ent.Int32).Sum(), "Sum is not supported");

            TestHelper.ExpectedException<TargetInvocationException>(() => currentTable.Query<ComplexEntity>(ent =>
                ent.RowKey.CompareTo("0050") > 0).Select(ent => ent.Int32).Min(), "Min is not supported");

            TestHelper.ExpectedException<TargetInvocationException>(() => currentTable.Query<ComplexEntity>(ent =>
                ent.RowKey.CompareTo("0050") > 0).Select(ent => ent.Int32).Max(), "Max is not supported");

            TestHelper.ExpectedException<TargetInvocationException>(() => currentTable.Query<ComplexEntity>(ent =>
                ent.RowKey.CompareTo("0050") > 0).Select(ent => ent.Int32).Average(), "Average is not supported");
        }
        #endregion Negative Tests

        #endregion Test Methods


        #region Helper
        private IEnumerable<T> GetEntities<T>(TableClient table, string id) where T : BaseEntity, new()
        {
            return table.Query<T>(x => x.PartitionKey == "mypk").ToList();
        }

        private static DynamicReplicatedTableEntity GenerateRandomEnitity(string pk)
        {
            DynamicReplicatedTableEntity ent = new DynamicReplicatedTableEntity();
            ent.Properties.Add("test", "test");
            ent.Properties.Add("a", "a");
            ent.Properties.Add("b", "b");
            ent.Properties.Add("c", "c");

            ent.PartitionKey = pk;
            ent.RowKey = Guid.NewGuid().ToString();
            return ent;
        }
        #endregion Helper

    }
}
