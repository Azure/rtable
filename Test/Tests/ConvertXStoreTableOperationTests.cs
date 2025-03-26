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
    using NUnit.Framework;
    using System;
    using System.Collections.Generic;
    using System.Net;

    /// <summary>
    /// Created some XStore Table entities.
    /// Then, create RTable using that XStore Table.
    /// Run rtableWrapper with convertXStoreTableMode = true.
    /// Confirm we can do operations on existing XStore Table entries,
    /// and confirm that write operations will convert the entities to RTable automatically.
    /// 
    /// Add one test case to do these extra steps:
    /// Run rtableWrapper with convertXStoreTableMode = false (which is the default behavior).
    /// Confirm we can do operations on those entries.
    /// </summary>
    [TestFixture]
    public class ConvertXStoreTableOperationTests : ConvertXStoreTableTestBase
    {    
        private List<string> entityNames = null;

        [OneTimeSetUp]
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
                    "DeleteXStoreEntity",
                    "InsertOrReplaceXStoreEntity",
                    "MergeXStoreEntity",
                    "ReplaceXStoreEntity",
                    "RetrieveXStoreEntity",
                    "TurnOffConvertXStoreTableMode-A",                    
                    "TurnOffConvertXStoreTableMode-B",
                };
            foreach (string entityName in this.entityNames)
            {
                try
                {
                    this.InsertXStoreEntities("jobType-" + entityName, "jobId-" + entityName, this.message);
                }
                catch
                {
                }
            }

            //
            // Set up RTable and its wrapper that uses only one storage account.
            //
            this.SetupRTableEnv(this.xstoreTableName, true, "", this.actualStorageAccountsUsed, true);

            Assert.True(this.configurationWrapper.IsConvertToRTableMode(), "Convert flag should be True");
        }

        [OneTimeTearDown]
        public void TestFixtureTearDown()
        {
            base.DeleteAllRtableResources();
        }

        [Test(Description = "Verify that DeleteRow operation works on an existing XStore entity")]
        public void DeleteXStoreEntity()
        {
            string jobType = "jobType-DeleteXStoreEntity";
            string jobId = "jobId-DeleteXStoreEntity";

            this.PerformOperationAndValidate(TableOperationType.Delete, jobType, jobId);
            this.PerformInsertOperationAndValidate(jobType, jobId, this.message);
            this.PerformRetrieveOperationAndValidate(jobType, jobId, this.message);
        }

        [Test(Description = "Verify that InsertOrReplace operation works on an existing XStore entity")]
        public void InsertOrReplaceXStoreEntity()
        {
            string jobType = "jobType-InsertOrReplaceXStoreEntity";
            string jobId = "jobId-InsertOrReplaceXStoreEntity";

            this.PerformOperationAndValidate(TableOperationType.InsertOrReplace, jobType, jobId, this.updatedMessage);
            this.PerformOperationAndValidate(TableOperationType.InsertOrReplace, jobType, jobId, this.updatedAgainMessage);
            this.PerformOperationAndValidate(TableOperationType.Delete, jobType, jobId);
            this.PerformInsertOperationAndValidate(jobType, jobId, this.message);
            this.PerformRetrieveOperationAndValidate(jobType, jobId, this.message);
        }

        [Test(Description = "Verify that Merge operation works on an existing XStore entity")]
        public void MergeXStoreEntity()
        {
            string jobType = "jobType-MergeXStoreEntity";
            string jobId = "jobId-MergeXStoreEntity";

            this.PerformOperationAndValidate(TableOperationType.Merge, jobType, jobId, this.updatedMessage);
            this.PerformOperationAndValidate(TableOperationType.Merge, jobType, jobId, this.updatedAgainMessage);
            this.PerformOperationAndValidate(TableOperationType.Delete, jobType, jobId);
            this.PerformInsertOperationAndValidate(jobType, jobId, this.message);
            this.PerformRetrieveOperationAndValidate(jobType, jobId, this.message);
        }

        [Test(Description = "Verify that Replace operation works on an existing XStore entity")]
        public void ReplaceXStoreEntity()
        {
            string jobType = "jobType-ReplaceXStoreEntity";
            string jobId = "jobId-ReplaceXStoreEntity";

            this.PerformOperationAndValidate(TableOperationType.Replace, jobType, jobId, this.updatedMessage);
            this.PerformOperationAndValidate(TableOperationType.Replace, jobType, jobId, this.updatedAgainMessage);
            this.PerformOperationAndValidate(TableOperationType.Delete, jobType, jobId);
            this.PerformInsertOperationAndValidate(jobType, jobId, this.message);
            this.PerformRetrieveOperationAndValidate(jobType, jobId, this.message);
        }

        [Test(Description = "Verify that Replace operation works on an existing XStore entity")]
        public void RetrieveXStoreEntity()
        {
            string jobType = "jobType-RetrieveXStoreEntity";
            string jobId = "jobId-RetrieveXStoreEntity";

            this.PerformRetrieveOperationAndValidate(jobType, jobId, this.message, false); // do not check _rtable_ViewId
            this.PerformOperationAndValidate(TableOperationType.Replace, jobType, jobId, this.updatedMessage);
            this.PerformOperationAndValidate(TableOperationType.InsertOrReplace, jobType, jobId, this.updatedAgainMessage);
            this.PerformOperationAndValidate(TableOperationType.Delete, jobType, jobId);
            this.PerformInsertOperationAndValidate(jobType, jobId, this.message);
            this.PerformRetrieveOperationAndValidate(jobType, jobId, this.message, true); // check _rtable_ViewId
        }

        [Test(Description = "Verify Retrieve to SampleXStoreEntity which doesn't inherit ReplicatedTableEntity")]
        public void RetrieveAsSampleXStoreEntity()
        {
            string jobType = "jobType-RetrieveXStoreEntity";
            string jobId = "jobId-RetrieveXStoreEntity";

            //
            // Modify the Json Config Blob to indicate NOT running convertXSToreTableMode anymore
            //
            Console.WriteLine("\n Setting convertXSToreTableMode to false (default state)...");
            this.RefreshRTableEnvJsonConfigBlob(false);

            Assert.False(this.configurationWrapper.IsConvertToRTableMode(), "Convert flag should be False");

            // 1 - Should throw KeyNotFoundException becasue the row is missing RTable meta.
            KeyNotFoundException exception = TestHelper.ExpectedException<KeyNotFoundException>(
                                                    () => this.RetrieveAsSampleXStoreEntity(jobType, jobId, this.message),
                                                    "Expected KeyNotFoundException since the row doesn't have RTable meta and convert mode = False!");

            Assert.IsTrue(exception.GetType() == typeof(KeyNotFoundException));
            Assert.IsTrue(exception.Message == "The given key was not present in the dictionary.");

            //
            // Change it back to convertXSToreTableMode = true so rows without RTable meta are retrieved.
            //
            this.RefreshRTableEnvJsonConfigBlob(true);

            Assert.True(this.configurationWrapper.IsConvertToRTableMode(), "Convert flag should be True");


            // 2 - Should not throw in convert mode = True eventhough RTable meta are missing
            this.RetrieveAsSampleXStoreEntity(jobType, jobId, this.message);

            this.PerformOperationAndValidate(TableOperationType.Replace, jobType, jobId, this.updatedMessage);

            // 3 - The row has RTable meta now, and we can still retrieve it as SampleXStoreEntity
            this.RetrieveAsSampleXStoreEntity(jobType, jobId, this.updatedMessage);

            this.PerformOperationAndValidate(TableOperationType.Delete, jobType, jobId);
            this.PerformInsertOperationAndValidate(jobType, jobId, this.message);
            this.PerformRetrieveOperationAndValidate(jobType, jobId, this.message, true); // check _rtable_ViewId
        }

        [Test(Description = "Verify Replace doesn't throw ETag mismatch when turning RTable ON Live")]
        public void RetrieveViaXStoreThenEnableRTableThenReplaceViaRTable()
        {
            // This issue happens when we transition such:
            // 1. we Retrieve using an XStore lib.
            // 2. switched to RTable lib.
            // 3. then Replace the entry using RTable lib.
            string jobType = "jobType-RetrieveXStoreEntity";
            string jobId = "jobId-RetrieveXStoreEntity";

            //
            // Retrieve using XStore library
            //
            string partitionKey;
            string rowKey;
            SampleXStoreEntity.GenerateKeys(
                this.GenerateJobType(jobType, 0),
                this.GenerateJobId(jobId, 0, 0),
                out partitionKey,
                out rowKey);
            var getResp = this.xstoreCloudTable.GetEntity<SampleXStoreEntity>(partitionKey, rowKey);
            var retrieveResult = new TableResult
            {
                Result = getResp.Value,
                Etag = getResp.HasValue ? getResp.Value.ETag.ToString() : null,
                HttpStatusCode = (int)getResp?.GetRawResponse().Status
            };

            Assert.IsNotNull(retrieveResult, "retrieveResult = null");
            SampleXStoreEntity retrievedEntity = (SampleXStoreEntity)retrieveResult.Result;

            Assert.AreEqual(
                this.GenerateMessage(this.message, 0, 0),
                retrievedEntity.Message,
                "Retrieve(): Message mismatch");


            // update the entry
            retrievedEntity.Message = this.GenerateMessage(this.updatedMessage, 0, 0);

            //
            // we switch to RTable and convertXSToreTableMode = true
            //
            Assert.True(this.configurationWrapper.IsConvertToRTableMode(), "Convert flag should be True");

            //
            // Replace using RTable library
            //
            InitDynamicReplicatedTableEntity replaceTableEntity = SampleXStoreEntity.ToInitDynamicReplicatedTableEntity(retrievedEntity);

            TableResult replaceResult = this.repTable.Replace(replaceTableEntity);

            Assert.IsNotNull(replaceResult, "replaceResult = null");
            this.PerformRetrieveOperationAndValidate(jobType, jobId, this.updatedMessage, true); // check _rtable_ViewId

            this.PerformOperationAndValidate(TableOperationType.Delete, jobType, jobId);
            this.PerformInsertOperationAndValidate(jobType, jobId, this.message);
            this.PerformRetrieveOperationAndValidate(jobType, jobId, this.message, true); // check _rtable_ViewId
        }

        [Test(Description = "Verify Merge doesn't throw ETag mismatch when turning RTable ON Live")]
        public void RetrieveViaXStoreThenEnableRTableThenMergeViaRTable()
        {
            // This issue happens when we transition such:
            // 1. we Retrieve using an XStore lib.
            // 2. switched to RTable lib.
            // 3. then Merge the entry using RTable lib.
            string jobType = "jobType-RetrieveXStoreEntity";
            string jobId = "jobId-RetrieveXStoreEntity";

            //
            // Retrieve using XStore library
            //
            string partitionKey;
            string rowKey;
            SampleXStoreEntity.GenerateKeys(
                this.GenerateJobType(jobType, 0),
                this.GenerateJobId(jobId, 0, 0),
                out partitionKey,
                out rowKey);
            var getResp = this.xstoreCloudTable.GetEntity<SampleXStoreEntity>(partitionKey, rowKey);
            var retrieveResult = new TableResult
            {
                Result = getResp.Value,
                Etag = getResp.HasValue ? getResp.Value.ETag.ToString() : null,
                HttpStatusCode = (int)getResp?.GetRawResponse().Status
            };

            Assert.IsNotNull(retrieveResult, "retrieveResult = null");
            SampleXStoreEntity retrievedEntity = (SampleXStoreEntity)retrieveResult.Result;

            Assert.AreEqual(
                this.GenerateMessage(this.message, 0, 0),
                retrievedEntity.Message,
                "Retrieve(): Message mismatch");


            // update the entry
            retrievedEntity.Message = this.GenerateMessage(this.updatedMessage, 0, 0);

            //
            // we switch to RTable and convertXSToreTableMode = true
            //
            Assert.True(this.configurationWrapper.IsConvertToRTableMode(), "Convert flag should be True");

            //
            // Merge using RTable library
            //
            InitDynamicReplicatedTableEntity mergeTableEntity = SampleXStoreEntity.ToInitDynamicReplicatedTableEntity(retrievedEntity);

            TableResult mergeResult = this.repTable.Merge(mergeTableEntity);

            Assert.IsNotNull(mergeResult, "mergeResult = null");
            this.PerformRetrieveOperationAndValidate(jobType, jobId, this.updatedMessage, true); // check _rtable_ViewId

            this.PerformOperationAndValidate(TableOperationType.Delete, jobType, jobId);
            this.PerformInsertOperationAndValidate(jobType, jobId, this.message);
            this.PerformRetrieveOperationAndValidate(jobType, jobId, this.message, true); // check _rtable_ViewId
        }

        [Test(Description = "Verify Delete doesn't throw ETag mismatch when turning RTable ON Live")]
        public void RetrieveViaXStoreThenEnableRTableThenDeleteViaRTable()
        {
            // This issue happens when we transition such:
            // 1. we Retrieve using an XStore lib.
            // 2. switched to RTable lib.
            // 3. then Delete the entry using RTable lib.
            string jobType = "jobType-RetrieveXStoreEntity";
            string jobId = "jobId-RetrieveXStoreEntity";

            //
            // Retrieve using XStore library
            //
            string partitionKey;
            string rowKey;
            SampleXStoreEntity.GenerateKeys(
                this.GenerateJobType(jobType, 0),
                this.GenerateJobId(jobId, 0, 0),
                out partitionKey,
                out rowKey);
            var getResp = this.xstoreCloudTable.GetEntity<SampleXStoreEntity>(partitionKey, rowKey);
            var retrieveResult = new TableResult
            {
                Result = getResp.Value,
                Etag = getResp.HasValue ? getResp.Value.ETag.ToString() : null,
                HttpStatusCode = (int)getResp?.GetRawResponse().Status
            };

            Assert.IsNotNull(retrieveResult, "retrieveResult = null");
            SampleXStoreEntity retrievedEntity = (SampleXStoreEntity)retrieveResult.Result;

            Assert.AreEqual(
                this.GenerateMessage(this.message, 0, 0),
                retrievedEntity.Message,
                "Retrieve(): Message mismatch");


            //
            // we switch to RTable and convertXSToreTableMode = true
            //
            Assert.True(this.configurationWrapper.IsConvertToRTableMode(), "Convert flag should be True");

            //
            // Delete using RTable library
            //
            InitDynamicReplicatedTableEntity deleteTableEntity = SampleXStoreEntity.ToInitDynamicReplicatedTableEntity(retrievedEntity);

            var deleteResp = this.xstoreCloudTable.DeleteEntity(deleteTableEntity.PartitionKey, deleteTableEntity.RowKey, deleteTableEntity.ETag);
            var deleteResult = new TableResult
            {
                Result = deleteTableEntity,
                Etag = deleteResp.Headers.ETag.ToString(),
                HttpStatusCode = deleteResp.Status
            };
            Assert.IsNotNull(deleteResult, "deleteResult = null");

            try
            {
                getResp = this.xstoreCloudTable.GetEntity<SampleXStoreEntity>(partitionKey, rowKey);
                Assert.Fail("GetEntity should throw when entity does not exist.");
            }
            catch (RequestFailedException ex)
            {
                Assert.AreEqual(ex.Status, (int)HttpStatusCode.NotFound);
            }
            catch (Exception)
            {
                Assert.Fail("GetEntity should throw RequestFailedException when entity does not exist.");
            }

            this.PerformInsertOperationAndValidate(jobType, jobId, this.message);
            this.PerformRetrieveOperationAndValidate(jobType, jobId, this.message, true); // check _rtable_ViewId
        }

        /// <summary>
        /// Initially, we have some XStore Table entities.
        /// Turn on ConvertXStoreTableMode.
        /// Confirm that we can operate on them while ConvertXStoreTableMode = true.
        /// Turn off ConvertXStoreTableMode (default mode).
        /// Confirm that we can continue to operate on them.
        /// </summary>
        [Test(Description = "Verify that we can operate on entities after we have turned off ConvertXStoreTableMode to false (default)")]
        public void TurnOffConvertXStoreTableMode()
        {
            // jobTypeA will go through Replace, Replace, Delete, Insert
            string jobTypeA = "jobType-TurnOffConvertXStoreTableMode-A";
            string jobIdA = "jobId-TurnOffConvertXStoreTableMode-A";

            // jobTypeB will go through Replace (and NOT Delete)
            string jobTypeB = "jobType-TurnOffConvertXStoreTableMode-B";
            string jobIdB = "jobId-TurnOffConvertXStoreTableMode-B";

            this.PerformOperationAndValidate(TableOperationType.Replace, jobTypeA, jobIdA, this.updatedMessage);
            this.PerformOperationAndValidate(TableOperationType.Replace, jobTypeA, jobIdA, this.updatedAgainMessage);
            this.PerformOperationAndValidate(TableOperationType.Delete, jobTypeA, jobIdA);
            this.PerformInsertOperationAndValidate(jobTypeA, jobIdA, this.message);

            this.PerformOperationAndValidate(TableOperationType.Replace, jobTypeB, jobIdB, this.updatedMessage);

            //
            // Modify the Json Config Blob to indicate NOT running convertXSToreTableMode anymore
            //
            Console.WriteLine("\n Setting convertXSToreTableMode to false (default state)...");
            this.RefreshRTableEnvJsonConfigBlob(false);

            Assert.False(this.configurationWrapper.IsConvertToRTableMode(), "Convert flag should be False");

            this.PerformOperationAndValidate(TableOperationType.Replace, jobTypeA, jobIdA, this.updatedMessage);
            this.PerformOperationAndValidate(TableOperationType.InsertOrReplace, jobTypeA, jobIdA, this.updatedAgainMessage);

            this.PerformOperationAndValidate(TableOperationType.InsertOrReplace, jobTypeB, jobIdB, this.updatedAgainMessage);
            this.PerformOperationAndValidate(TableOperationType.Delete, jobTypeB, jobIdB);
            this.PerformInsertOperationAndValidate(jobTypeB, jobIdB, this.message);

            //
            // Change it back to convertXSToreTableMode = true so that other test cases in this suite would work.
            //
            this.RefreshRTableEnvJsonConfigBlob(true);

            Assert.True(this.configurationWrapper.IsConvertToRTableMode(), "Convert flag should be True");

            // Simulate a rollback:
            // Perform xstore operations to see if it works. 
            Console.WriteLine("Performing xstore operations to simulate rollback...");
            this.ReplaceXStoreEntities(jobTypeA, jobIdA, this.updatedAgainMessage);
            this.ReplaceXStoreEntities(jobTypeB, jobIdB, this.updatedAgainMessage);
        }
    }
}
