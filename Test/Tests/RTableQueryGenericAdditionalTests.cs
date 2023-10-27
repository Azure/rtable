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
    using NUnit.Framework;
    using System;   
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading;
    using global::Azure.Data.Tables;

    [TestFixture]
    public class RTableQueryGenericAdditionalTests : RTableLibraryTestBase
    {
        private const int Midpoint = 50;
        private const int BatchSize = 100;

        private TableClient currentTable = null;
        private ComplexEntity middleRef = null;

        [OneTimeSetUp]
        public void TestFixtureSetup()
        {
            this.LoadTestConfiguration();
            string tableName = this.GenerateRandomTableName();
            this.SetupRTableEnv(tableName);

            // Initialize the table to be queried to the tail replica         
            TableServiceClient tableClient = this.repTable.GetTailTableClient();
            this.currentTable = tableClient.GetTableClient(repTable.TableName);

            // Setup
            IList<TableTransactionAction> batch = new List<TableTransactionAction>();
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

                batch.Add(new TableTransactionAction(TableTransactionActionType.Add, complexEntity, complexEntity.ETag));

                if (m == Midpoint)
                {
                    this.middleRef = complexEntity;
                }

                // Add delay to make times unique
                Thread.Sleep(100);
            }

            this.currentTable.SubmitTransaction(batch);
        }

        [OneTimeTearDown]
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
            ExecuteQueryAndAssertResults(this.currentTable, e => string.Compare(e.String, "0050") >= 0, Midpoint);

            // 2. Filter on Guid
            ExecuteQueryAndAssertResults(this.currentTable, e => e.Guid == this.middleRef.Guid, 1);

            // 3. Filter on Long
            ExecuteQueryAndAssertResults(this.currentTable, e => e.Int64 >= this.middleRef.LongPrimitive, Midpoint);

            ExecuteQueryAndAssertResults(this.currentTable, e => e.LongPrimitive >= this.middleRef.LongPrimitive, Midpoint);

            // 4. Filter on Double
            ExecuteQueryAndAssertResults(this.currentTable, e => e.Double >= this.middleRef.Double, Midpoint);
            
            ExecuteQueryAndAssertResults(this.currentTable, e => e.DoublePrimitive >= this.middleRef.DoublePrimitive, Midpoint);

            // 5. Filter on Integer
            ExecuteQueryAndAssertResults(this.currentTable, e => e.Int32 >= this.middleRef.Int32, Midpoint);
            
            ExecuteQueryAndAssertResults(this.currentTable, e => e.IntegerPrimitive >= this.middleRef.IntegerPrimitive, Midpoint);

            // 6. Filter on Date
            ExecuteQueryAndAssertResults(this.currentTable, e => e.DateTimeOffset >= this.middleRef.DateTimeOffset, Midpoint);

            // 7. Filter on Boolean
            ExecuteQueryAndAssertResults(this.currentTable, e => e.Bool == this.middleRef.Bool, Midpoint);
            
            ExecuteQueryAndAssertResults(this.currentTable, e => e.BoolPrimitive == this.middleRef.BoolPrimitive, Midpoint);

            // 8. Filter on Binary 
            ExecuteQueryAndAssertResults(this.currentTable, e => e.Binary == this.middleRef.Binary, 1);

            ExecuteQueryAndAssertResults(this.currentTable, e => e.BinaryPrimitive == this.middleRef.BinaryPrimitive, 1);

            // 9. Filter on Binary GTE
            // https://stackoverflow.com/questions/42502643/how-to-compare-two-byte-arrays-with-greater-than-or-less-than-operator-value-in
            ExecuteQueryAndAssertResults(this.currentTable, e => ((IStructuralComparable)e.Binary).CompareTo(this.middleRef.Binary, Comparer<byte>.Default) >= 0, Midpoint);
            
            ExecuteQueryAndAssertResults(this.currentTable, e => ((IStructuralComparable)e.BinaryPrimitive).CompareTo(this.middleRef.BinaryPrimitive, Comparer<byte>.Default) >= 0, Midpoint);

            // 10. Complex Filter on Binary GTE
            ExecuteQueryAndAssertResults(this.currentTable,
                e => e.PartitionKey == this.middleRef.PartitionKey &&((IStructuralComparable)e.Binary).CompareTo(this.middleRef.Binary, Comparer<byte>.Default) >= 0,
                Midpoint);
        }

        //
        // TODO: What is the difference of this test method??
        //
        [Test(Description = "A test validate all supported query types")]
        public void TableGenericQueryWithSpecificOnSupportedTypes()
        {   
            // 1. Filter on String
            ExecuteQueryAndAssertResults(this.currentTable, e => string.Compare(e.String, "0050") >= 0, 50);

            // 2. Filter on Guid
            ExecuteQueryAndAssertResults(this.currentTable, e => e.Guid == middleRef.Guid, 1);

            // 3. Filter on Long
            ExecuteQueryAndAssertResults(this.currentTable, e => e.Int64 >= middleRef.LongPrimitive, 50);

            ExecuteQueryAndAssertResults(this.currentTable, e => e.LongPrimitive >= middleRef.LongPrimitive, 50);

            // 4. Filter on Double
            ExecuteQueryAndAssertResults(this.currentTable, e => e.Double >= middleRef.Double, 50);

            ExecuteQueryAndAssertResults(this.currentTable, e => e.DoublePrimitive >= middleRef.DoublePrimitive, 50);

            // 5. Filter on Integer
            ExecuteQueryAndAssertResults(this.currentTable, e => e.Int32 >= middleRef.Int32, 50);

            ExecuteQueryAndAssertResults(this.currentTable, e => e.IntegerPrimitive >= middleRef.IntegerPrimitive, 50);

            // 6. Filter on Date
            ExecuteQueryAndAssertResults(this.currentTable, e => e.DateTimeOffset >= middleRef.DateTimeOffset, 50);

            // 7. Filter on Boolean
            ExecuteQueryAndAssertResults(this.currentTable, e => e.Bool == middleRef.Bool, 50);

            ExecuteQueryAndAssertResults(this.currentTable, e => e.BoolPrimitive == middleRef.BoolPrimitive, 50);

            // 8. Filter on Binary 
            ExecuteQueryAndAssertResults(this.currentTable, e => e.Binary == middleRef.Binary, 1);

            ExecuteQueryAndAssertResults(this.currentTable, e => e.BinaryPrimitive == middleRef.BinaryPrimitive, 1);

            // 9. Filter on Binary GTE
            ExecuteQueryAndAssertResults(this.currentTable, e => ((IStructuralComparable)e.Binary).CompareTo(middleRef.Binary, Comparer<byte>.Default) >= 0, 50);

            ExecuteQueryAndAssertResults(this.currentTable, e => ((IStructuralComparable)e.BinaryPrimitive).CompareTo(middleRef.BinaryPrimitive, Comparer<byte>.Default) >= 0, 50);

            // 10. Complex Filter on Binary GTE
            ExecuteQueryAndAssertResults(this.currentTable,
                e => e.PartitionKey == middleRef.PartitionKey && ((IStructuralComparable)e.Binary).CompareTo(middleRef.Binary, Comparer<byte>.Default) >= 0,
                50);
        }

        #endregion Query Test Methods



        #region Helpers

        //private void ExecuteQueryAndAssertResults(TableClient table, string filter, int expectedResults)
        private void ExecuteQueryAndAssertResults(TableClient table, Expression<Func<ComplexEntity, bool>> filter, int expectedResults)
        {
            var resultSet = this.repTable.ExecuteQuery(filter);
            foreach (var row in resultSet)
            {
                Assert.AreEqual(row.ETag, row._rtable_Version.ToString());
            }

            Assert.AreEqual(expectedResults, table.Query(filter).Count());
            Assert.AreEqual(expectedResults, resultSet.Count());
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
