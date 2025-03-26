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
    using System.Collections.Generic;

    [TestFixture]
    public class RTableStressTests : RTableWrapperTestBase
    {
        //Run this test against two replicas
        private const int HeadReplicaAccountIndex = 0;
        private const int TailReplicaAccountIndex = 1;

        //Number of entities in the table
        private const int NumberofEntities = 10;

        //Number of operations run
        private const int NumberOfOperations = 10;

        private const string JobType = "jobType-RandomTableOperationTest";
        private const string JobId = "jobId-RandomTableOperationTest";
        private const string OriginalMessage = "message";

        [OneTimeSetUp]
        public void TestFixtureSetup()
        {
            this.LoadTestConfiguration();

            string tableName = this.GenerateRandomTableName();
            bool useHttps = true;
            long viewId = 1;
            string viewIdString = viewId.ToString();
            var actualStorageAccountsToBeUsed = new List<int>() { HeadReplicaAccountIndex, TailReplicaAccountIndex };
            bool convertXStoreTableMode = false;

            Console.WriteLine("Setting up RTable that has a Head Replica and a Tail Replica...");
            this.SetupRTableEnv(
                tableName,
                useHttps,
                viewIdString,
                actualStorageAccountsToBeUsed,
                convertXStoreTableMode);
            Assert.AreEqual(2, this.actualStorageAccountsUsed.Count, "Two storage accounts should be used at this point.");

            //Fill up the table with the specified number of entities
            Console.WriteLine("Inserting entities to the RTable...");
            this.numberOfPartitions = 1;
            this.numberOfRowsPerPartition = NumberofEntities;
            this.PerformInsertOperationAndValidate(JobType, JobId, OriginalMessage);
        }

        [OneTimeTearDown]
        public void TestFixtureTearDown()
        {
            base.DeleteAllRtableResources();
        }

        [Test(Description = "Randomly performs operations on multiple replicas and validates the tables.")]
        public void RandomTableOperationTest()
        {   
            const int partitionIndex = 0;
            // Insert some entries. 

            //Key = RowNumber, Value = RowKey
            var currentEntities = new Dictionary<int, String>();
            for (int i = 0; i < this.numberOfRowsPerPartition; i++)
            {
                currentEntities.Add(i, GenerateJobId(JobId, partitionIndex, i));
            }

            //Key = RowNumber, Value = RowKey
            var deletedEntities = new Dictionary<int, String>();

            var random = new Random();
            for (int i = 0; i < NumberOfOperations; i++)
            {
                int operation = random.Next(0, Enum.GetNames(typeof(TableOperationType)).Count());
                int randomRow = random.Next(0, NumberofEntities);

                if ((TableOperationType) operation == TableOperationType.Insert)
                {
                    if (deletedEntities.Count == 0)
                    {
                        //Ignore insertion if there is nothing to be inserted
                        continue;
                    }
                    randomRow = deletedEntities.First().Key;
                    deletedEntities.Remove(randomRow);
                }

                if (deletedEntities.ContainsKey(randomRow))
                {
                    continue;
                }

                Console.WriteLine("Operation# {0}, {1} on row {2}", i, (TableOperationType)operation, randomRow);
                PerformIndividualOperationAndValidate((TableOperationType)operation, JobType, JobId, partitionIndex, randomRow, OriginalMessage);

                if ((TableOperationType)operation == TableOperationType.Delete)
                {
                    deletedEntities.Add(randomRow, GenerateJobId(JobId, partitionIndex, randomRow));
                }
            }

            string rowKey;
            string partitionKey;
            SampleRTableEntity.GenerateKeys(this.GenerateJobType(JobType, partitionIndex), "don't care", out partitionKey, out rowKey);

            //Validations
            Console.WriteLine("Performing replica validations");
            PerformInvariantChecks(partitionKey, HeadReplicaAccountIndex, TailReplicaAccountIndex);
            Console.WriteLine("DONE. Test passed.");
        }
    }
}
