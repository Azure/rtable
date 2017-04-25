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
    using Microsoft.WindowsAzure.Storage.Table;
    using NUnit.Framework;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;

    /// <summary>
    /// This cs file only tests the ConvertXStoreTable() API.
    /// For other XStore conversion tests, see ConvertXStoreTableToRTableTests.cs
    /// 
    /// Created some XStore Table entities.
    /// Then, create RTable using that XStore Table.
    /// Run rtableWrapper with convertXStoreTableMode = true.
    /// Confirm we can do operations on existing XStore Table entries.
    /// Then, call RTable's ConvertXStoreTable() API to convert all the remaining entities.
    /// </summary>
    [TestFixture]
    public class ConvertXStoreTableAPITests : ConvertXStoreTableTestBase
    {
        private List<string> entityNames = null;

        [TestFixtureSetUp]
        public void TestFixtureSetup()
        {
            //
            // Create an XStore Table
            //
            this.SetupXStoreTableEnv();

            //
            // Insert some XStore entities into XStore Table.
            // Each test case will have its own set of entities.
            //
            this.entityNames = new List<string>()
                {
                    "ReplaceXStoreEntity",
                    "ConvertXStoreTable-A",                    
                    "ConvertXStoreTable-B",
                };
            foreach (string entityName in this.entityNames)
            {
                this.InsertXStoreEntities("jobType-" + entityName, "jobId-" + entityName, this.message);
            }

            //
            // Set up RTable and its wrapper that uses only one storage account.
            //
            this.SetupRTableEnv(this.xstoreTableName, true, "", this.actualStorageAccountsUsed, true);

            Assert.True(this.configurationWrapper.IsConvertToRTableMode(), "Convert flag should be True");
        }

        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            base.DeleteAllRtableResources();
        }

        [Test(Description = "Call RTable's ConvertXStoreTable() API to convert existing XStore entity to RTable entity")]
        public void ConvertXStoreTable()
        {
            // BEFORE:
            // LINQ query in convert mode, using "CreateReplicatedQuery" => ETag is virtualized
            IQueryable<InitDynamicReplicatedTableEntity> virtualizedRtableQuery = this.repTable.CreateReplicatedQuery<InitDynamicReplicatedTableEntity>();
            foreach (var ent in virtualizedRtableQuery)
            {
                Assert.IsTrue(ent.ETag == "0", "ETag is virtualized when using CreateReplicatedQuery()");
            }


            string jobType = "jobType-ReplaceXStoreEntity";
            string jobId = "jobId-ReplaceXStoreEntity";

            // First, make some API calls to operate on one entity.
            this.PerformOperationAndValidate(TableOperationType.Replace, jobType, jobId, this.updatedMessage);
            this.PerformOperationAndValidate(TableOperationType.Replace, jobType, jobId, this.updatedAgainMessage);

            // Next, call ConvertXStoreTable API to convert all the remaining entities.
            long successCount = 0;
            long skippedCount = 0;
            long failedCount = 0;
            this.repTable.ConvertXStoreTable(out successCount, out skippedCount, out failedCount);
            Console.WriteLine("successCount={0} skippedCount={1} failedCount={2}", successCount, skippedCount, failedCount);

            long expectedSccess = this.numberOfPartitions * this.numberOfRowsPerPartition;

            // (expectedSccess * 2) because of "ConvertXStoreTable-A" and "ConvertXStoreTable-B" in TestFixtureSetup()
            Assert.AreEqual(expectedSccess * 2, successCount, "Number of successfully converted entries does NOT match");

            Assert.AreEqual(expectedSccess, skippedCount, "Number of skipped entries does NOT match");
            Assert.AreEqual(0, failedCount, "Number of failed converted entries does NOT match");

            // Retrieve all entries and confirm that _rtable_ViewId are ok.

            // AFTER:
            // LINQ query in convert mode, using "CreateReplicatedQuery" => ETag is virtualized
            virtualizedRtableQuery = this.repTable.CreateReplicatedQuery<InitDynamicReplicatedTableEntity>();
            foreach (var ent in virtualizedRtableQuery.ToList())
            {
                Assert.IsTrue(ent.ETag == ent._rtable_Version.ToString(), "ETag is virtualized when using CreateReplicatedQuery()");
            }
        }
    }
}
