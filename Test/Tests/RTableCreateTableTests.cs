//
// Copyright (c) Microsoft Corporation. All rights reserved.
//
//////////////////////////////////////////////////////////////////////////////

namespace Microsoft.Azure.Toolkit.Replication.Test
{
    using Microsoft.Azure.Toolkit.Replication;
    using NUnit.Framework;

    public class RTableCreateTableTests : RTableLibraryTestBase
    {
        [TestFixtureSetUp]
        public void TestFixtureSetup()
        {
            this.LoadTestConfiguration();
        }


        //
        // binaries.amd64fre\Gateway\UnitTests>
        // runuts /fixture=Microsoft.WindowsAzure.Storage.RTableTest.RTableCreateTableTests
        //
        #region Table Create Test Methods
        [Test(Description = "Test table create when table does not exist - Sync")]
        public void RTableCreateTableSync()
        {
            try
            {
                string tableName = this.GenerateRandomTableName();
                this.SetupRTableEnv(true, tableName);
                Assert.IsTrue(this.repTable.Exists(), "RTable does not exist");
            }
            finally
            {
                base.DeleteAllRtableResources();
            }
        }

        [Test(Description = "Test table create when table already exists - Sync")]
        public void RTableCreateTableAlreadyExistsSync()
        {
            try
            {
                string tableName = this.GenerateRandomTableName();
                this.SetupRTableEnv(true, tableName);
                Assert.IsTrue(this.repTable.Exists(), "RTable does not exist");

                // Try to create the same RTable again.
                ReplicatedTable curTable = new ReplicatedTable(this.repTable.TableName, this.configurationService);
                Assert.IsFalse(curTable.CreateIfNotExists(), "Calling CreateIfNotExists() again returned true. Should be false.");
                int replicasCreated = curTable.replicasCreated;
                Assert.AreEqual(replicasCreated, 0, "Calling CreateIfNotExists() again returned replicasCreated={0} > 0. Should be 0.", replicasCreated);
            }
            finally
            {
                base.DeleteAllRtableResources();
            }
        }
        #endregion Table Create Test Methods
    }
}
