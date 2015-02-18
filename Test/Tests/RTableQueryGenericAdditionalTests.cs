//////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) Microsoft Corporation. All rights reserved.
//
//////////////////////////////////////////////////////////////////////////////

namespace Microsoft.Azure.Toolkit.Replication.Test
{
    using Microsoft.Azure.Toolkit.Replication;
    using Microsoft.WindowsAzure.Storage.Table;
    using NUnit.Framework;
    using System;   
    using System.Linq;
    using System.Threading;
    using System.Collections.Generic;

    [TestFixture]
    public class RTableQueryGenericAdditionalTests : RTableLibraryTestBase
    {
        private const int Midpoint = 50;
        private const int BatchSize = 100;

        private CloudTable currentTable = null;
        private ComplexEntity middleRef = null;

        [TestFixtureSetUp]
        public void TestFixtureSetup()
        {
            this.LoadTestConfiguration();
            string tableName = this.GenerateRandomTableName();
            this.SetupRTableEnv(true, tableName);

            // Initialize the table to be queried to the tail replica         
            CloudTableClient tableClient = this.repTable.GetTailTableClient();
            this.currentTable = tableClient.GetTableReference(repTable.TableName);

            // Setup
            TableBatchOperation batch = new TableBatchOperation();
            string pk = Guid.NewGuid().ToString();
            for (int m = 0; m < BatchSize; m++)
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
                complexEntity.IntegerPrimitive = m;
                complexEntity.Int64 = (long)int.MaxValue + m;
                complexEntity.LongPrimitive = (long)int.MaxValue + m;
                complexEntity.Guid = Guid.NewGuid();

                batch.Insert(complexEntity);

                if (m == Midpoint)
                {
                    this.middleRef = complexEntity;
                }

                // Add delay to make times unique
                Thread.Sleep(100);
            }

            this.currentTable.ExecuteBatch(batch);
        }

        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            base.DeleteAllRtableResources();
        }

        //
        // binaries.amd64fre\Gateway\UnitTests>
        // runuts /fixture=Microsoft.WindowsAzure.Storage.RTableTest.RTableQueryGenericAdditionalTests
        //
        #region Query Test Methods       
        [Test(Description = "A test validate all supported query types")]        
        public void TableGenericQueryOnSupportedTypes()
        {
            // 1. Filter on String
            ExecuteQueryAndAssertResults(this.currentTable,
                    TableQuery.GenerateFilterCondition("String", QueryComparisons.GreaterThanOrEqual, "0050"), Midpoint);

            // 2. Filter on Guid
            ExecuteQueryAndAssertResults(this.currentTable,
                    TableQuery.GenerateFilterConditionForGuid("Guid", QueryComparisons.Equal, this.middleRef.Guid), 1);

            // 3. Filter on Long
            ExecuteQueryAndAssertResults(this.currentTable,
                    TableQuery.GenerateFilterConditionForLong("Int64", QueryComparisons.GreaterThanOrEqual,
                            this.middleRef.LongPrimitive), Midpoint);

            ExecuteQueryAndAssertResults(this.currentTable, TableQuery.GenerateFilterConditionForLong("LongPrimitive",
                    QueryComparisons.GreaterThanOrEqual, this.middleRef.LongPrimitive), Midpoint);

            // 4. Filter on Double
            ExecuteQueryAndAssertResults(this.currentTable,
                    TableQuery.GenerateFilterConditionForDouble("Double", QueryComparisons.GreaterThanOrEqual,
                            this.middleRef.Double), Midpoint);

            ExecuteQueryAndAssertResults(this.currentTable, TableQuery.GenerateFilterConditionForDouble("DoublePrimitive",
                    QueryComparisons.GreaterThanOrEqual, this.middleRef.DoublePrimitive), Midpoint);

            // 5. Filter on Integer
            ExecuteQueryAndAssertResults(this.currentTable,
                    TableQuery.GenerateFilterConditionForInt("Int32", QueryComparisons.GreaterThanOrEqual,
                            this.middleRef.Int32), Midpoint);

            ExecuteQueryAndAssertResults(this.currentTable, TableQuery.GenerateFilterConditionForInt("IntegerPrimitive",
                    QueryComparisons.GreaterThanOrEqual, middleRef.IntegerPrimitive), Midpoint);

            // 6. Filter on Date
            ExecuteQueryAndAssertResults(this.currentTable,
                    TableQuery.GenerateFilterConditionForDate("DateTimeOffset", QueryComparisons.GreaterThanOrEqual,
                            this.middleRef.DateTimeOffset), Midpoint);

            // 7. Filter on Boolean
            ExecuteQueryAndAssertResults(this.currentTable,
                    TableQuery.GenerateFilterConditionForBool("Bool", QueryComparisons.Equal, this.middleRef.Bool), Midpoint);

            ExecuteQueryAndAssertResults(this.currentTable,
                    TableQuery.GenerateFilterConditionForBool("BoolPrimitive", QueryComparisons.Equal, this.middleRef.BoolPrimitive),
                    Midpoint);

            // 8. Filter on Binary 
            ExecuteQueryAndAssertResults(this.currentTable,
                    TableQuery.GenerateFilterConditionForBinary("Binary", QueryComparisons.Equal, this.middleRef.Binary), 1);

            ExecuteQueryAndAssertResults(this.currentTable,
                    TableQuery.GenerateFilterConditionForBinary("BinaryPrimitive", QueryComparisons.Equal,
                            middleRef.BinaryPrimitive), 1);

            // 9. Filter on Binary GTE
            ExecuteQueryAndAssertResults(this.currentTable,
                    TableQuery.GenerateFilterConditionForBinary("Binary", QueryComparisons.GreaterThanOrEqual,
                            this.middleRef.Binary), Midpoint);

            ExecuteQueryAndAssertResults(this.currentTable, TableQuery.GenerateFilterConditionForBinary("BinaryPrimitive",
                    QueryComparisons.GreaterThanOrEqual, this.middleRef.BinaryPrimitive), Midpoint);

            // 10. Complex Filter on Binary GTE
            ExecuteQueryAndAssertResults(this.currentTable, TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal,
                            this.middleRef.PartitionKey),
                    TableOperators.And,
                    TableQuery.GenerateFilterConditionForBinary("Binary", QueryComparisons.GreaterThanOrEqual,
                            this.middleRef.Binary)), Midpoint);

            ExecuteQueryAndAssertResults(this.currentTable, TableQuery.GenerateFilterConditionForBinary("BinaryPrimitive",
                    QueryComparisons.GreaterThanOrEqual, this.middleRef.BinaryPrimitive), Midpoint);
        }

        //
        // TODO: What is the difference of this test method??
        //
        [Test(Description = "A test validate all supported query types")]
        public void TableGenericQueryWithSpecificOnSupportedTypes()
        {   
            // 1. Filter on String
            ExecuteQueryAndAssertResults(this.currentTable,
                    TableQuery.GenerateFilterCondition("String", QueryComparisons.GreaterThanOrEqual, "0050"), 50);

            // 2. Filter on Guid
            ExecuteQueryAndAssertResults(this.currentTable,
                    TableQuery.GenerateFilterConditionForGuid("Guid", QueryComparisons.Equal, middleRef.Guid), 1);

            // 3. Filter on Long
            ExecuteQueryAndAssertResults(this.currentTable,
                    TableQuery.GenerateFilterConditionForLong("Int64", QueryComparisons.GreaterThanOrEqual,
                            middleRef.LongPrimitive), 50);

            ExecuteQueryAndAssertResults(this.currentTable, TableQuery.GenerateFilterConditionForLong("LongPrimitive",
                    QueryComparisons.GreaterThanOrEqual, middleRef.LongPrimitive), 50);

            // 4. Filter on Double
            ExecuteQueryAndAssertResults(this.currentTable,
                    TableQuery.GenerateFilterConditionForDouble("Double", QueryComparisons.GreaterThanOrEqual,
                            middleRef.Double), 50);

            ExecuteQueryAndAssertResults(this.currentTable, TableQuery.GenerateFilterConditionForDouble("DoublePrimitive",
                    QueryComparisons.GreaterThanOrEqual, middleRef.DoublePrimitive), 50);

            // 5. Filter on Integer
            ExecuteQueryAndAssertResults(this.currentTable,
                    TableQuery.GenerateFilterConditionForInt("Int32", QueryComparisons.GreaterThanOrEqual,
                            middleRef.Int32), 50);

            ExecuteQueryAndAssertResults(this.currentTable, TableQuery.GenerateFilterConditionForInt("IntegerPrimitive",
                    QueryComparisons.GreaterThanOrEqual, middleRef.IntegerPrimitive), 50);

            // 6. Filter on Date
            ExecuteQueryAndAssertResults(this.currentTable,
                    TableQuery.GenerateFilterConditionForDate("DateTimeOffset", QueryComparisons.GreaterThanOrEqual,
                            middleRef.DateTimeOffset), 50);

            // 7. Filter on Boolean
            ExecuteQueryAndAssertResults(this.currentTable,
                    TableQuery.GenerateFilterConditionForBool("Bool", QueryComparisons.Equal, middleRef.Bool), 50);

            ExecuteQueryAndAssertResults(this.currentTable,
                    TableQuery.GenerateFilterConditionForBool("BoolPrimitive", QueryComparisons.Equal, middleRef.BoolPrimitive),
                    50);

            // 8. Filter on Binary 
            ExecuteQueryAndAssertResults(this.currentTable,
                    TableQuery.GenerateFilterConditionForBinary("Binary", QueryComparisons.Equal, middleRef.Binary), 1);

            ExecuteQueryAndAssertResults(this.currentTable,
                    TableQuery.GenerateFilterConditionForBinary("BinaryPrimitive", QueryComparisons.Equal,
                            middleRef.BinaryPrimitive), 1);

            // 9. Filter on Binary GTE
            ExecuteQueryAndAssertResults(this.currentTable,
                    TableQuery.GenerateFilterConditionForBinary("Binary", QueryComparisons.GreaterThanOrEqual,
                            middleRef.Binary), 50);

            ExecuteQueryAndAssertResults(this.currentTable, TableQuery.GenerateFilterConditionForBinary("BinaryPrimitive",
                    QueryComparisons.GreaterThanOrEqual, middleRef.BinaryPrimitive), 50);

            // 10. Complex Filter on Binary GTE
            ExecuteQueryAndAssertResults(this.currentTable, TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal,
                            middleRef.PartitionKey),
                    TableOperators.And,
                    TableQuery.GenerateFilterConditionForBinary("Binary", QueryComparisons.GreaterThanOrEqual,
                            middleRef.Binary)), 50);

            ExecuteQueryAndAssertResults(this.currentTable, TableQuery.GenerateFilterConditionForBinary("BinaryPrimitive",
                    QueryComparisons.GreaterThanOrEqual, middleRef.BinaryPrimitive), 50);
        }

        #endregion Query Test Methods



        #region Helpers

        private static void ExecuteQueryAndAssertResults(CloudTable table, string filter, int expectedResults)
        {
            Assert.AreEqual(expectedResults, table.ExecuteQuery(new TableQuery<ComplexEntity>().Where(filter)).Count());
        }

        //private static BaseEntity GenerateRandomEntity(string pk)
        //{
        //    BaseEntity ent = new BaseEntity();
        //    ent.Populate();
        //    ent.PartitionKey = pk;
        //    ent.RowKey = Guid.NewGuid().ToString();
        //    return ent;
        //}
        #endregion Helpers
    }
}
