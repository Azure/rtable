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
