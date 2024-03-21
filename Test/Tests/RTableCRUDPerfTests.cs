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
    using global::Azure;
    using global::Azure.Data.Tables;
    using NUnit.Framework;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Text;

    /// <summary>
    /// Keeps track of data points, and returns min/max/avg
    /// </summary>
    internal class DataSampling
    {
        private readonly string sampleName;
        private readonly List<long> samples = new List<long>();
        private long sum = 0;

        public DataSampling(string sampleName)
        {
            if (string.IsNullOrEmpty(sampleName))
            {
                throw new ArgumentException("sampleName is null or empty!");
            }

            this.sampleName = sampleName;

            Min = Int64.MaxValue;
            Max = -1;
            sum = 0;
        }

        public long Min { get; private set; }
        public long Max { get; private set; }

        public long Avg
        {
            get
            {
                if (samples.Count == 0)
                {
                    throw new ArgumentException("no data sampling!");
                }

                return sum / samples.Count;
            }
        }

        public new string ToString()
        {
            return samples.Count == 0
                        ? "there are no samples!!!!"
                        : string.Format("{0}:\tmin={1}, max={2}, avg={3}", sampleName, Min, Max, Avg);
        }

        /// <summary>
        /// Add a sample to the data
        /// </summary>
        /// <param name="curr"></param>
        public void AddPoint(long curr)
        {
            Min = Math.Min(curr, Min);
            Max = Math.Max(curr, Max);
            sum += curr;

            samples.Add(curr);
        }

        /// <summary>
        /// Dump samples to a file
        /// </summary>
        /// <param name="path"></param>
        public void DumpToFile(string path)
        {
            throw new NotImplementedException();
        }
    }


    [TestFixture]
    public class RTableCRUDPerfTests : RTableWrapperTestBase
    {
        // Sampling size = NumberOfPartitions * NumberOfRowsPerPartition
        private const int NumberOfPartitions = 5;
        private const int NumberOfRowsPerPartition = 2;

        private List<CustomerEntity> entries = null;

        [OneTimeSetUp]
        public void TestFixtureSetup()
        {
            this.LoadTestConfiguration();

            // Generate entries
            entries = GenerateEntries();
            if (entries == null || !entries.Any())
            {
                throw new ArgumentException("list is empty!");
            }
        }

        [OneTimeTearDown]
        public void TestFixtureTearDown()
        {
            // nothing for now
        }


        #region XStore Test

        /// <summary>
        /// Test XStore API to get perf. reference numbers
        /// </summary>
        [Test(Description = "XStore CRUD perf. numbers")]
        public void XStoreCRUDTest()
        {
            try
            {
                string tableName = this.GenerateRandomTableName("XStorePerf");
                this.SetupRTableEnv(tableName, true, "", new List<int> { 0 });
                Console.WriteLine("tableName = {0}", tableName);

                TableServiceClient tableClient = this.cloudTableClients[0];
                TableClient table = tableClient.GetTableClient(tableName);

                Console.WriteLine("[C]reate stats ...");
                entries = ShuffleList(entries);
                DataSampling createStats = CreateEntriesStats(table, entries);

                Console.WriteLine("[R]etrieve stats ...");
                entries = ShuffleList(entries);
                DataSampling retrieveStats = RetrieveEntriesPerf(table, entries);

                Console.WriteLine("[U]pdate stats ...");
                entries = ShuffleList(entries);
                DataSampling updateStats = UpdateEntriesPerf(table, entries);

                Console.WriteLine("[D]elete stats ...");
                entries = ShuffleList(entries);
                DataSampling deleteStats = DeleteEntriesPerf(table, entries);


                // TODO: Add perf. tests for LINQ query ...


                IEnumerable<CustomerEntity> customers = GetCustomerEntities(table);
                Assert.AreEqual(customers.Count(), 0);

                var report = new StringBuilder();
                report.AppendLine("XStore CRUD perf (ms)");
                report.AppendFormat("{0}\n", createStats.ToString());
                report.AppendFormat("{0}\n", retrieveStats.ToString());
                report.AppendFormat("{0}\n", updateStats.ToString());
                report.AppendFormat("{0}\n", deleteStats.ToString());
                report.AppendLine("End report.");

                Console.WriteLine(report);
            }
            catch (Exception ex)
            {
                Console.WriteLine("{0}", ex);
                Assert.Fail();
            }
            finally
            {
                base.DeleteAllRtableResources();
            }
        }

        private DataSampling CreateEntriesStats(TableClient table, List<CustomerEntity> entries)
        {
            var stats = new DataSampling("[C]reate");

            foreach (var entry in entries)
            {
                TableResult insertResult = null;
                var watch = System.Diagnostics.Stopwatch.StartNew();
                {
                    var resp = table.AddEntity(entry);
                    insertResult = TableResult.ConvertResponseToTableResult(resp, entry);
                }
                watch.Stop();
                stats.AddPoint(watch.ElapsedMilliseconds);

                Assert.IsNotNull(insertResult, "insertResult = null");
                Assert.AreEqual((int)HttpStatusCode.NoContent, insertResult.HttpStatusCode, "insertResult.HttpStatusCode mismatch");
            }

            return stats;
        }

        private DataSampling RetrieveEntriesPerf(TableClient table, List<CustomerEntity> entries)
        {
            var stats = new DataSampling("[R]etrieve");

            foreach (var entry in entries)
            {
                TableResult retrieveResult = null;
                var watch = System.Diagnostics.Stopwatch.StartNew();
                {
                    var resp = table.GetEntity<CustomerEntity>(entry.PartitionKey, entry.RowKey);
                    retrieveResult = new TableResult
                    {
                        Result = resp.Value,
                        Etag = resp.HasValue ? resp.Value.ETag.ToString() : null,
                        HttpStatusCode = (int)resp?.GetRawResponse().Status
                    };
                }
                watch.Stop();
                stats.AddPoint(watch.ElapsedMilliseconds);

                Assert.IsNotNull(retrieveResult, "retrieveResult = null");
                Assert.AreEqual((int)HttpStatusCode.OK, retrieveResult.HttpStatusCode, "retrieveResult.HttpStatusCode mismatch");

                var customer = (CustomerEntity)retrieveResult.Result;
                Assert.IsNotNull(customer, "Retrieve: customer = null");
            }

            return stats;
        }

        private DataSampling UpdateEntriesPerf(TableClient table, List<CustomerEntity> entries)
        {
            var stats = new DataSampling("[U]pdate");

            foreach (var entry in entries)
            {
                var retrieveResult = table.GetEntity<CustomerEntity>(entry.PartitionKey, entry.RowKey);

                Assert.IsNotNull(retrieveResult, "retrieveResult = null");
                Assert.AreEqual((int)HttpStatusCode.OK, retrieveResult?.GetRawResponse().Status, "retrieveResult.HttpStatusCode mismatch");
                Assert.IsNotNull(retrieveResult.Value, "Retrieve: customer = null");

                // Update entity
                var customer = retrieveResult.Value;
                customer.Email = string.Format("{0}.{1}@email.com", entry.PartitionKey, entry.RowKey);

                Response updateResult;
                var watch = System.Diagnostics.Stopwatch.StartNew();
                {
                    updateResult = table.UpdateEntity(customer, customer.ETag, TableUpdateMode.Replace);
                }
                watch.Stop();
                stats.AddPoint(watch.ElapsedMilliseconds);

                Assert.IsNotNull(updateResult, "updateResult = null");
                Assert.AreEqual((int)HttpStatusCode.NoContent, updateResult.Status, "updateResult.HttpStatusCode mismatch");
            }

            return stats;
        }

        private DataSampling DeleteEntriesPerf(TableClient table, List<CustomerEntity> entries)
        {
            var stats = new DataSampling("[D]elete");

            foreach (var entry in entries)
            {
                var retrieveResult = table.GetEntity<CustomerEntity>(entry.PartitionKey, entry.RowKey);

                Assert.IsNotNull(retrieveResult, "retrieveResult = null");
                Assert.AreEqual((int)HttpStatusCode.OK, retrieveResult?.GetRawResponse().Status, "retrieveResult.HttpStatusCode mismatch");
                Assert.IsNotNull(retrieveResult.Value, "Retrieve: customer = null");

                // Delete entity
                var customer = retrieveResult.Value;

                Response deleteResult;
                var watch = System.Diagnostics.Stopwatch.StartNew();
                {
                    deleteResult = table.DeleteEntity(customer.PartitionKey, customer.RowKey, customer.ETag);
                }
                watch.Stop();
                stats.AddPoint(watch.ElapsedMilliseconds);

                Assert.IsNotNull(deleteResult, "deleteResult = null");
                Assert.AreEqual((int)HttpStatusCode.NoContent, deleteResult.Status, "deleteResult.HttpStatusCode mismatch");
            }

            return stats;
        }

        private IEnumerable<CustomerEntity> GetCustomerEntities(TableClient table)
        {
            return table.Query<CustomerEntity>();
        }

        #endregion


        #region RTable "One-Replica" Test

        /// <summary>
        /// Test RTable API when "One-Replica" to get perf. numbers
        /// </summary>
        [Test(Description = "RTable One-Replica CRUD perf. numbers")]
        public void RTableOneReplicaCRUDTest()
        {
            try
            {
                string tableName = this.GenerateRandomTableName("RTable1Perf");
                this.SetupRTableEnv(tableName, true, "", new List<int> { 0 });
                Console.WriteLine("tableName = {0}", tableName);

                // one replica ?
                View view = this.configurationService.GetTableView(tableName);
                Assert.IsTrue(view != null && view.Chain != null && view.Chain.Count == 1);
                // convert mode is Off ?
                ReplicatedTableConfiguredTable tableConfig;
                Assert.IsTrue(this.configurationService.IsConfiguredTable(tableName, out tableConfig));
                Assert.IsFalse(tableConfig.ConvertToRTable);

                ReplicatedTable rtable = this.repTable;

                Console.WriteLine("[C]reate stats ...");
                entries = ShuffleList(entries);
                DataSampling createStats = CreateEntriesStats(rtable, entries);

                Console.WriteLine("[R]etrieve stats ...");
                entries = ShuffleList(entries);
                DataSampling retrieveStats = RetrieveEntriesPerf(rtable, entries);

                Console.WriteLine("[U]pdate stats ...");
                entries = ShuffleList(entries);
                DataSampling updateStats = UpdateEntriesPerf(rtable, entries);

                Console.WriteLine("[D]elete stats ...");
                entries = ShuffleList(entries);
                DataSampling deleteStats = DeleteEntriesPerf(rtable, entries);


                //// TODO: Add perf. tests for LINQ query ...


                IEnumerable<CustomerEntity> customers = GetCustomerEntities(rtable);
                Assert.AreEqual(customers.Count(), 0);

                var report = new StringBuilder();
                report.AppendLine("RTable One-Replica CRUD perf (ms)");
                report.AppendFormat("{0}\n", createStats.ToString());
                report.AppendFormat("{0}\n", retrieveStats.ToString());
                report.AppendFormat("{0}\n", updateStats.ToString());
                report.AppendFormat("{0}\n", deleteStats.ToString());
                report.AppendLine("End report.");

                Console.WriteLine(report);
            }
            catch (Exception ex)
            {
                Console.WriteLine("{0}", ex);
                Assert.Fail();
            }
            finally
            {
                base.DeleteAllRtableResources();
            }
        }

        private DataSampling CreateEntriesStats(ReplicatedTable rtable, List<CustomerEntity> entries)
        {
            var stats = new DataSampling("[C]reate");

            foreach (var entry in entries)
            {
                TableResult insertResult = null;
                var watch = System.Diagnostics.Stopwatch.StartNew();
                {
                    insertResult = rtable.Insert(entry);
                }
                watch.Stop();
                stats.AddPoint(watch.ElapsedMilliseconds);

                Assert.IsNotNull(insertResult, "insertResult = null");
                Assert.AreEqual((int)HttpStatusCode.NoContent, insertResult.HttpStatusCode, "insertResult.HttpStatusCode mismatch");
            }

            return stats;
        }

        private DataSampling RetrieveEntriesPerf(ReplicatedTable rtable, List<CustomerEntity> entries)
        {
            var stats = new DataSampling("[R]etrieve");

            foreach (var entry in entries)
            {
                TableResult retrieveResult = null;
                var watch = System.Diagnostics.Stopwatch.StartNew();
                {
                    retrieveResult = rtable.Retrieve(entry.PartitionKey, entry.RowKey);
                }
                watch.Stop();
                stats.AddPoint(watch.ElapsedMilliseconds);

                Assert.IsNotNull(retrieveResult, "retrieveResult = null");
                Assert.AreEqual((int)HttpStatusCode.OK, retrieveResult.HttpStatusCode, "retrieveResult.HttpStatusCode mismatch");

                var customer = new CustomerEntity((ReplicatedTableEntity)retrieveResult.Result);
                Assert.IsNotNull(customer, "Retrieve: customer = null");

                Assert.IsTrue(customer._rtable_Version == 1, "new entry should have version 1");
            }

            return stats;
        }

        private DataSampling UpdateEntriesPerf(ReplicatedTable rtable, List<CustomerEntity> entries)
        {
            var stats = new DataSampling("[U]pdate");

            foreach (var entry in entries)
            {
                TableResult retrieveResult = rtable.Retrieve(entry.PartitionKey, entry.RowKey);

                Assert.IsNotNull(retrieveResult, "retrieveResult = null");
                Assert.AreEqual((int)HttpStatusCode.OK, retrieveResult.HttpStatusCode, "retrieveResult.HttpStatusCode mismatch");
                Assert.IsNotNull(new CustomerEntity((ReplicatedTableEntity)retrieveResult.Result), "Retrieve: customer = null");

                // Update entity
                var customer = new CustomerEntity((ReplicatedTableEntity)retrieveResult.Result);
                customer.Email = string.Format("{0}.{1}@email.com", entry.PartitionKey, entry.RowKey);

                TableResult updateResult;
                var watch = System.Diagnostics.Stopwatch.StartNew();
                {
                    updateResult = rtable.Replace(customer);
                }
                watch.Stop();
                stats.AddPoint(watch.ElapsedMilliseconds);

                Assert.IsNotNull(updateResult, "updateResult = null");
                Assert.AreEqual((int)HttpStatusCode.NoContent, updateResult.HttpStatusCode, "updateResult.HttpStatusCode mismatch");
            }

            return stats;
        }

        private DataSampling DeleteEntriesPerf(ReplicatedTable rtable, List<CustomerEntity> entries)
        {
            var stats = new DataSampling("[D]elete");

            foreach (var entry in entries)
            {
                TableResult retrieveResult = rtable.Retrieve(entry.PartitionKey, entry.RowKey);

                Assert.IsNotNull(retrieveResult, "retrieveResult = null");
                Assert.AreEqual((int)HttpStatusCode.OK, retrieveResult.HttpStatusCode, "retrieveResult.HttpStatusCode mismatch");
                Assert.IsNotNull(new CustomerEntity((ReplicatedTableEntity)retrieveResult.Result), "Retrieve: customer = null");

                // Delete entity
                var customer = new CustomerEntity((ReplicatedTableEntity)retrieveResult.Result);
                Assert.IsTrue(customer._rtable_Version == 2, "entry was updated once, version should be 2");

                TableResult deleteResult;
                var watch = System.Diagnostics.Stopwatch.StartNew();
                {
                    deleteResult = rtable.Delete(customer);
                }
                watch.Stop();
                stats.AddPoint(watch.ElapsedMilliseconds);

                Assert.IsNotNull(deleteResult, "deleteResult = null");
                Assert.AreEqual((int)HttpStatusCode.NoContent, deleteResult.HttpStatusCode, "deleteResult.HttpStatusCode mismatch");
            }

            return stats;
        }

        private IEnumerable<CustomerEntity> GetCustomerEntities(ReplicatedTable rtable)
        {
            ReplicatedTableQuery<CustomerEntity> query = rtable.CreateReplicatedQuery<CustomerEntity>(null);
            return query.AsEnumerable();
        }

        #endregion


        #region RTable "Stable Two-Replicas" Test

        /// <summary>
        /// Test RTable API when "Stable Two-Replicas" to get perf. numbers
        /// </summary>
        [Test(Description = "RTable Stable Two-Replicas CRUD perf. numbers")]
        public void RTableStableTwoReplicaCRUDTest()
        {
            try
            {
                string tableName = this.GenerateRandomTableName("RTable2Perf");
                this.SetupRTableEnv(tableName, true, "", new List<int> { 0, 1 });
                Console.WriteLine("tableName = {0}", tableName);

                // two-replica and stable?
                View view = this.configurationService.GetTableView(tableName);
                Assert.IsTrue(view != null && view.Chain != null && view.Chain.Count == 2 && view.IsStable);
                // convert mode is Off ? yes, because we can't have more than 1 replica while in convert mode!

                ReplicatedTable rtable = this.repTable;

                Console.WriteLine("[C]reate stats ...");
                entries = ShuffleList(entries);
                DataSampling createStats = CreateEntriesStats(rtable, entries);

                Console.WriteLine("[R]etrieve stats ...");
                entries = ShuffleList(entries);
                DataSampling retrieveStats = RetrieveEntriesPerf(rtable, entries);

                Console.WriteLine("[U]pdate stats ...");
                entries = ShuffleList(entries);
                DataSampling updateStats = UpdateEntriesPerf(rtable, entries);

                Console.WriteLine("[D]elete stats ...");
                entries = ShuffleList(entries);
                DataSampling deleteStats = DeleteEntriesPerf(rtable, entries);


                //// TODO: Add perf. tests for LINQ query ...


                IEnumerable<CustomerEntity> customers = GetCustomerEntities(rtable);
                Assert.AreEqual(customers.Count(), 0);

                var report = new StringBuilder();
                report.AppendLine("RTable Stable Two-Replicas CRUD perf (ms)");
                report.AppendFormat("{0}\n", createStats.ToString());
                report.AppendFormat("{0}\n", retrieveStats.ToString());
                report.AppendFormat("{0}\n", updateStats.ToString());
                report.AppendFormat("{0}\n", deleteStats.ToString());
                report.AppendLine("End report.");

                Console.WriteLine(report);
            }
            catch (Exception ex)
            {
                Console.WriteLine("{0}", ex);
                Assert.Fail();
            }
            finally
            {
                base.DeleteAllRtableResources();
            }
        }

        #endregion



        #region Helper Methods

        private static List<CustomerEntity> GenerateEntries()
        {
            var entries = new List<CustomerEntity>();

            for (int partitionId = 0; partitionId < NumberOfPartitions; partitionId++)
            {
                for (int rowId = 0; rowId < NumberOfRowsPerPartition; rowId++)
                {
                    string partitionKey = partitionId.ToString();
                    string rowKey = rowId.ToString();

                    entries.Add(new CustomerEntity(partitionKey, rowKey));
                }
            }

            return entries;
        }

        private static List<CustomerEntity> ShuffleList(List<CustomerEntity> list)
        {
            if (list == null || !list.Any())
            {
                return list;
            }

            var rnd = new Random();
            var size = list.Count;
            return list.OrderBy(e => rnd.Next(size)).ToList();
        }

        #endregion
    }
}
