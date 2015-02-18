//////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) Microsoft Corporation. All rights reserved.
//
//////////////////////////////////////////////////////////////////////////////

namespace Microsoft.Azure.Toolkit.Replication.Test
{
    using Microsoft.WindowsAzure.Storage.Table;
    using NUnit.Framework;
    using System;
    using System.Collections.Generic;

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
            this.SetupRTableEnv(true, this.xstoreTableName, true, "", this.actualStorageAccountsUsed, true);
        }

        [TestFixtureTearDown]
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

            this.PerformOperationAndValidate(TableOperationType.Replace, jobTypeA, jobIdA, this.updatedMessage);
            this.PerformOperationAndValidate(TableOperationType.InsertOrReplace, jobTypeA, jobIdA, this.updatedAgainMessage);

            this.PerformOperationAndValidate(TableOperationType.InsertOrReplace, jobTypeB, jobIdB, this.updatedAgainMessage);
            this.PerformOperationAndValidate(TableOperationType.Delete, jobTypeB, jobIdB);
            this.PerformInsertOperationAndValidate(jobTypeB, jobIdB, this.message);

            //
            // Change it back to convertXSToreTableMode = true so that other test cases in this suite would work.
            //
            this.RefreshRTableEnvJsonConfigBlob(true);

            // Simulate a rollback:
            // Perform xstore operations to see if it works. 
            Console.WriteLine("Performing xstore operations to simulate rollback...");
            this.ReplaceXStoreEntities(jobTypeA, jobIdA, this.updatedAgainMessage);
            this.ReplaceXStoreEntities(jobTypeB, jobIdB, this.updatedAgainMessage);
        }
    }
}
