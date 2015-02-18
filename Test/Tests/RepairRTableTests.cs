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
    using Microsoft.WindowsAzure.Storage.Table;
    using NUnit.Framework;
    using System;
    using System.Collections.Generic;

    [TestFixture]
    public class RepairRTableTests : RTableWrapperTestBase
    {             
        [Test(Description = "Add new Head Replica to an existing Tail Replica, and call RepairTable()")]
        public void AddNewHeadReplicaTest()
        {
            try
            {
                int retrieveAttemtps = 2;

                this.LoadTestConfiguration();

                bool reUploadRTableConfigBlob = true;
                string tableName = this.GenerateRandomTableName();
                bool useHttps = true;
                long viewId = 1;
                string viewIdString = viewId.ToString();
                int headReplicaAccountIndex = 0;
                int tailReplicaAccountIndex = 1;
                List<int> actualStorageAccountsToBeUsed = new List<int>() { tailReplicaAccountIndex };
                bool convertXStoreTableMode = false;

                Console.WriteLine("Setting up RTable that only has a Tail Replica...");
                this.SetupRTableEnv(
                    reUploadRTableConfigBlob,
                    tableName,
                    useHttps,
                    viewIdString,
                    actualStorageAccountsToBeUsed,
                    convertXStoreTableMode);
                Assert.AreEqual(1, this.actualStorageAccountsUsed.Count, "Only one stoarge account should be used at this point.");

                Console.WriteLine("Inserting entities to the Tail Replica...");

                this.numberOfPartitions = 1;
                this.numberOfRowsPerPartition = 1;

                string jobTypeReplace = "jobType-AddNewHeadReplicaTest-Replace"; // will call Replace in Phase One and Two
                string jobIdReplace = "jobId-AddNewHeadReplicaTest-Replace"; 
                string jobTypeDelete = "jobType-AddNewHeadReplicaTest-Delete"; // will call Delete in Phase One, then try to Insert again in Phase Two
                string jobIdDelete = "jobId-AddNewHeadReplicaTest-Delete";
                string jobTypeStatic = "jobType-AddNewHeadReplicaTest-Static"; // will NOT call Replace in Phase One. Let RepairTable() fix this row. Call Replace in Phase Two.
                string jobIdStatic = "jobId-AddNewHeadReplicaTest-Static";

                string originalMessage = "message";
                string updatedMessage = "updatedMessage";
                string updatedMessage2 = "updatedMessageTwo";
                string staticMessage = "staticMessage";

                // Insert some entries. 
                this.PerformInsertOperationAndValidate(jobTypeReplace,
                                                    jobIdReplace,
                                                    originalMessage);   
                this.PerformInsertOperationAndValidate(jobTypeDelete,
                                                    jobIdDelete,
                                                    originalMessage);             
                this.PerformInsertOperationAndValidate(jobTypeStatic,
                                                       jobIdStatic,
                                                       staticMessage);

                Console.WriteLine("\n\nAdding new a new Head Replica to the chain. Will sleep some time...");
                int readViewHeadIndex = 1;
                this.actualStorageAccountsUsed = new List<int> { headReplicaAccountIndex, tailReplicaAccountIndex };
                viewId++;
                this.RefreshRTableEnvJsonConfigBlob(viewId, convertXStoreTableMode, readViewHeadIndex);
                this.repTable.CreateIfNotExists(); // since we are adding a new Replica, need to create RTable (if needed).

                //
                // Phase One:
                // Using Head Replica and Tail Replica. readViewHeadIndex = 1
                //
                Console.WriteLine("\n\nPhase One: New Head Replica has been added. readViewHeadIndex = 1. Verifying Replace(), Delete() and Insert() API...");
                for (int i = 0; i < retrieveAttemtps; i++)
                {
                    this.PerformRetrieveOperationAndValidate(jobTypeReplace, jobIdReplace, originalMessage, false);
                }
                for (int i = 0; i < retrieveAttemtps; i++)
                {
                    this.PerformRetrieveOperationAndValidate(jobTypeDelete, jobIdDelete, originalMessage, false);
                }
                // Replace() API
                this.PerformOperationAndValidate(TableOperationType.Replace, jobTypeReplace, jobIdReplace, updatedMessage);
                // Delete() API:
                this.PerformOperationAndValidate(TableOperationType.Delete, jobTypeDelete, jobIdDelete);

                string jobTypeInsert = "jobType-AddNewHeadReplicaTest-Insert"; // Will call Insert in Phase One, and Replace in Phase Two
                string jobIdInsert = "jobId-AddNewHeadReplicaTest-Insert";
                // Insert() API:
                this.PerformInsertOperationAndValidate(jobTypeInsert,
                                                    jobIdInsert,
                                                    originalMessage);   
                for (int i = 0; i < retrieveAttemtps; i++)
                {
                    this.PerformRetrieveOperationAndValidate(jobTypeInsert, jobIdInsert, originalMessage, false);
                }

                Console.WriteLine("\n\nRepairing RTable...");
                ReconfigurationStatus repairStatus = this.repTable.RepairTable(1, null);
                Assert.AreEqual(ReconfigurationStatus.SUCCESS, repairStatus, "Repair failed.");

                Console.WriteLine("\n\nDone repairing. Changing RTable config to use ReadViewHeadIndex=0, will sleep for some time...");
                readViewHeadIndex = 0;
                viewId++;
                this.RefreshRTableEnvJsonConfigBlob(viewId, convertXStoreTableMode, readViewHeadIndex);

                //
                // Phase Two:
                // Using Head Replica and Tail Replica. readViewHeadIndex = 0
                //
                Console.WriteLine("\n\nPhase Two: RepairTable() completed successfully. readViewHeadIndex = 0. Verifying Replace(), Delete() and Insert() API...");
                Console.WriteLine("Checking entries after Table is repaired...");
                for (int i = 0; i < retrieveAttemtps; i++)
                {
                    this.PerformRetrieveOperationAndValidate(jobTypeReplace, jobIdReplace, updatedMessage, false);
                }
                // Replace() API:
                this.PerformOperationAndValidate(TableOperationType.Replace, jobTypeReplace, jobIdReplace, updatedMessage2);

                for (int i = 0; i < retrieveAttemtps; i++)
                {
                    this.PerformRetrieveOperationAndValidate(jobTypeStatic, jobIdStatic, staticMessage, false);
                }
                // Replace() API:
                this.PerformOperationAndValidate(TableOperationType.Replace, jobTypeStatic, jobIdStatic, updatedMessage);

                for (int i = 0; i < retrieveAttemtps; i++)
                {
                    this.PerformRetrieveOperationAndValidate(jobTypeInsert, jobIdInsert, originalMessage, false);
                }
                // Replace() API:
                this.PerformOperationAndValidate(TableOperationType.Replace, jobTypeInsert, jobIdInsert, updatedMessage);

                // Verify NotFound
                Console.WriteLine("Verifying jobIdDelete does not exist after RepairTable...");
                for (int i = 0; i < retrieveAttemtps; i++)
                {
                    this.ValidateRetrieveNotFound(jobTypeDelete, jobIdDelete);
                }
                // Insert() API:
                Console.WriteLine("Should be able to Insert() jobIdDelete again after RepairTable...");
                this.PerformInsertOperationAndValidate(jobTypeDelete,
                                                jobIdDelete,
                                                updatedMessage);

                Console.WriteLine("DONE. Test passed.");

            }
            finally
            {
                base.DeleteAllRtableResources();
            }
        }

        [Test(Description = "Remove an existing Head Replica, call some APIs, then add Head Replica (which will have some stale entries) back.")]
        public void RemoveAndReAddHeadReplicaTest()
        {
            try
            {
                int retrieveAttemtps = 2;

                this.LoadTestConfiguration();

                bool reUploadRTableConfigBlob = true;
                string tableName = this.GenerateRandomTableName();
                bool useHttps = true;
                long viewId = 1;
                string viewIdString = viewId.ToString();
                int headReplicaAccountIndex = 0;
                int tailReplicaAccountIndex = 1;
                List<int> actualStorageAccountsToBeUsed = new List<int>() { headReplicaAccountIndex, tailReplicaAccountIndex };
                bool convertXStoreTableMode = false;

                Console.WriteLine("Setting up RTable that has a Head Replica and a Tail Replica...");
                this.SetupRTableEnv(
                    reUploadRTableConfigBlob,
                    tableName,
                    useHttps,
                    viewIdString,
                    actualStorageAccountsToBeUsed,
                    convertXStoreTableMode);
                Assert.AreEqual(2, this.actualStorageAccountsUsed.Count, "Two stoarge accounts should be used at this point.");

                Console.WriteLine("Inserting entities to the RTable...");

                this.numberOfPartitions = 1;
                this.numberOfRowsPerPartition = 1;

                string jobTypeInsert = "jobType-RemoveAndReAddHeadReplicaTest-Insert"; // will call Insert() in Phase One, Two (expect to fail), Three (expect to fail)
                string jobIdInsert = "jobId-RemoveAndReAddHeadReplicaTest-Insert";

                string jobTypeStale = "jobType-RemoveAndReAddHeadReplicaTest-Stale"; // will call Replace() in Phase One, Two, Three
                string jobIdStale = "jobId-RemoveAndReAddHeadReplicaTest-Stale";
                string jobTypeStatic = "jobType-RemoveAndReAddHeadReplicaTest-Static"; // will NOT call Replace() in Phase One. Let RepairTable() fix this row.
                string jobIdStatic = "jobId-RemoveAndReAddHeadReplicaTest-Static";
                string jobTypeDelete = "jobType-RemoveAndReAddHeadReplicaTest-Delete"; // will call Delete() in Phase One, Insert() in Phase Two.
                string jobIdDelete = "jobId-RemoveAndReAddHeadReplicaTest-Delete";
                string jobTypeReplaceDelete = "jobType-RemoveAndReAddHeadReplicaTest-ReplaceDelete"; // will call Replace() in Phase One, Delete() in Phase Two, Insert() in Phase Three.
                string jobIdReplaceDelete = "jobId-RemoveAndReAddHeadReplicaTest-ReplaceDelete";

                string originalMessage = "message";
                string updatedMessage = "updatedMessage";
                string updatedMessage2 = "updatedMessageTwo";
                string staticMessage = "staticMessage";

                // Insert some entries. 
                this.PerformInsertOperationAndValidate(jobTypeInsert,
                                                        jobIdInsert,
                                                        originalMessage);    
                this.PerformInsertOperationAndValidate(jobTypeStale,
                                                    jobIdStale,
                                                    originalMessage);                
                this.PerformInsertOperationAndValidate(jobTypeStatic,
                                                       jobIdStatic,
                                                       staticMessage);
                this.PerformInsertOperationAndValidate(jobTypeDelete,
                                                       jobIdDelete,
                                                       originalMessage);
                this.PerformInsertOperationAndValidate(jobTypeReplaceDelete,
                                                       jobIdReplaceDelete,
                                                       originalMessage);

                Console.WriteLine("\n\nRemoving Head Replica from the chain. Will sleep some time...");
                int readViewHeadIndex = 0;
                this.actualStorageAccountsUsed = new List<int> { tailReplicaAccountIndex };
                viewId++;
                this.RefreshRTableEnvJsonConfigBlob(viewId, convertXStoreTableMode, readViewHeadIndex);

                //
                // Phase One:
                // Using Tail Replica only. readViewHeadIndex = 0.
                //
                Console.WriteLine("\n\nPhase One: Head Replica has been removed. Using Tail Replica only. Verifying Replace() and Delete() API...");
                for (int i = 0; i < retrieveAttemtps; i++)
                {
                    this.PerformRetrieveOperationAndValidate(jobTypeStale, jobIdStale, originalMessage, false);
                }
                for (int i = 0; i < retrieveAttemtps; i++)
                {
                    this.PerformRetrieveOperationAndValidate(jobTypeStatic, jobIdStatic, staticMessage, false);
                }
                for (int i = 0; i < retrieveAttemtps; i++)
                {
                    this.PerformRetrieveOperationAndValidate(jobTypeDelete, jobIdDelete, originalMessage, false);
                }
                for (int i = 0; i < retrieveAttemtps; i++)
                {
                    this.PerformRetrieveOperationAndValidate(jobTypeReplaceDelete, jobIdReplaceDelete, originalMessage, false);
                }
                // Replace() API
                this.PerformOperationAndValidate(TableOperationType.Replace, jobTypeStale, jobIdStale, updatedMessage);
                this.PerformOperationAndValidate(TableOperationType.Replace, jobTypeReplaceDelete, jobIdReplaceDelete, updatedMessage);
                // Delete() API
                this.PerformOperationAndValidate(TableOperationType.Delete, jobTypeDelete, jobIdDelete);

                Console.WriteLine("\n\nAdding the Head Replica back to the chain. readViewHeadIndex = 1. Will sleep some time...");
                readViewHeadIndex = 1;
                this.actualStorageAccountsUsed = new List<int> { headReplicaAccountIndex, tailReplicaAccountIndex };
                viewId++;
                this.RefreshRTableEnvJsonConfigBlob(viewId, convertXStoreTableMode, readViewHeadIndex);

                //
                // Phase Two:
                // Using Head Replica and Tail Replica. readViewHeadIndex = 1.
                // Just added back the Head Replica, which contains stale entries.
                //
                Console.WriteLine("\n\nPhase Two: Head Replica has been added back. readViewHeadIndex = 1. Verifying Replace(), Delete() and Insert() API...");
                
                // Insert() existing row will fail
                Console.WriteLine("Verifying that Insert() an existing row will fail...");
                this.PerformInsertOperationAndExpectToFail(jobTypeInsert, jobIdInsert, updatedMessage);

                for (int i = 0; i < retrieveAttemtps; i++)
                {
                    this.PerformRetrieveOperationAndValidate(jobTypeStale, jobIdStale, updatedMessage, false);
                }
                for (int i = 0; i < retrieveAttemtps; i++)
                {
                    this.PerformRetrieveOperationAndValidate(jobTypeStatic, jobIdStatic, staticMessage, false);
                }
                for (int i = 0; i < retrieveAttemtps; i++)
                {
                    this.PerformRetrieveOperationAndValidate(jobTypeReplaceDelete, jobIdReplaceDelete, updatedMessage, false);
                }                

                // Replace():
                this.PerformOperationAndValidate(TableOperationType.Replace, jobTypeStale, jobIdStale, updatedMessage2);

                // Verify NotFound:
                Console.WriteLine("Verifying jobIdDelete does not exist in Phase Two (Head and Tail Replica; readViewHeadIndex = 1)...");
                for (int i = 0; i < retrieveAttemtps; i++)
                {
                    this.ValidateRetrieveNotFound(jobTypeDelete, jobIdDelete);
                }
                // Insert()
                Console.WriteLine("Should be able to Insert() jobIdDelete in Phase Two (Head and Tail Replica; readViewHeadIndex = 1)...");
                this.PerformInsertOperationAndValidate(jobTypeDelete,
                                                   jobIdDelete,
                                                   updatedMessage);

                // Delete():
                Console.WriteLine("Verifying Delete() jobTypeReplaceDelete in Phase Two (Head and Tail Replica; readViewHeadIndex = 1)...");
                this.PerformOperationAndValidate(TableOperationType.Delete, jobTypeReplaceDelete, jobIdReplaceDelete);
                

                Console.WriteLine("\n\nRepairing RTable...");
                ReconfigurationStatus repairStatus = this.repTable.RepairTable(1, null);
                Assert.AreEqual(ReconfigurationStatus.SUCCESS, repairStatus, "Repair failed.");

                Console.WriteLine("\n\nDone repairing. Changing RTable config to use ReadViewHeadIndex=0, will sleep for some time...");
                readViewHeadIndex = 0;
                viewId++;
                this.RefreshRTableEnvJsonConfigBlob(viewId, convertXStoreTableMode, readViewHeadIndex);

                //
                // Phase Three:
                // Using Head Replica and Tail Replica. readViewHeadIndex = 0
                //
                Console.WriteLine("\n\nPhase Three: RepairTable() completed successfully. readViewHeadIndex = 0. Verifying Replace(), Delete() and Insert() API...");
                
                // Insert() existing row will fail
                Console.WriteLine("Verifying that Insert() an existing row will fail...");
                this.PerformInsertOperationAndExpectToFail(jobTypeInsert, jobIdInsert, updatedMessage);
                
                for (int i = 0; i < retrieveAttemtps; i++)
                {
                    this.PerformRetrieveOperationAndValidate(jobTypeStale, jobIdStale, updatedMessage2, false);
                }
                for (int i = 0; i < retrieveAttemtps; i++)
                {
                    this.PerformRetrieveOperationAndValidate(jobTypeStatic, jobIdStatic, staticMessage, false);
                }
                for (int i = 0; i < retrieveAttemtps; i++)
                {
                    this.PerformRetrieveOperationAndValidate(jobTypeDelete, jobIdDelete, updatedMessage, false);
                }

                // Verify NotFound:
                Console.WriteLine("Verifying jobTypeReplaceDelete does not exist in Phase Three (Head and Tail Replica; readViewHeadIndex = 0)...");
                for (int i = 0; i < retrieveAttemtps; i++)
                {
                    this.ValidateRetrieveNotFound(jobTypeReplaceDelete, jobIdReplaceDelete);
                }
                // Insert()
                Console.WriteLine("Should be able to Insert() jobTypeReplaceDelete in Phase Three (Head and Tail Replica; readViewHeadIndex = 0)...");
                this.PerformInsertOperationAndValidate(jobTypeReplaceDelete,
                                                   jobIdReplaceDelete,
                                                   updatedMessage2);

                Console.WriteLine("Checking Replace() API...");
                this.PerformOperationAndValidate(TableOperationType.Replace, jobTypeStale, jobIdStale, updatedMessage);
                this.PerformOperationAndValidate(TableOperationType.Replace, jobTypeStatic, jobIdStatic, updatedMessage);
                this.PerformOperationAndValidate(TableOperationType.Replace, jobTypeDelete, jobIdDelete, updatedMessage2);

                Console.WriteLine("DONE. Test passed.");
            }
            finally
            {
                base.DeleteAllRtableResources();
            }
        }

    }

}


