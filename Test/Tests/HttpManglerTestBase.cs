//////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) Microsoft Corporation. All rights reserved.
//
//////////////////////////////////////////////////////////////////////////////


namespace Microsoft.Azure.Toolkit.Replication.Test
{
    using Microsoft.WindowsAzure.Storage.Table;
    using Microsoft.WindowsAzure.Storage.RTableTest;
    using Microsoft.WindowsAzure.Test.Network;
    using Microsoft.WindowsAzure.Test.Network.Behaviors;
    using NUnit.Framework;
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Threading;

    /// <summary>
    /// Base class for RTable tests that use HttpMangler.
    /// </summary>
    public class HttpManglerTestBase : RTableLibraryTestBase
    {
        /// <summary>
        /// When using HttpMangler, try not to use Https otherwise active sessions from previous test run may be re-used by the next test.
        /// That will cause HttpMangler to miss the session and test failure.
        /// </summary>
        protected bool useHttps = false;

        /// <summary>
        /// This is the RTable API under test when HttpMangler is enabled.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="rEntity"></param>
        public delegate void TargetRTableWrapperApi<T>(T rEntity);
        
        /// <summary>
        /// Due to HttpMangler, RTable library may return a ConflictException. 
        /// When that happens, will retry after sleeping this much time.
        /// </summary>
        protected const int ConflictExceptionSleepTimeInMsec = 5000;

        /// <summary>
        /// Due to HttpMangler, RTable library may return a ConflictException. 
        /// When that happens, will retry this many times max.
        /// </summary>
        private const int MaxRetries = 4;

        #region Protected Helper functions
        /// <summary>
        /// Call this helper function to set up RTable before running any tests.
        /// </summary>
        protected void TestFixtureSetupInternal()
        {
            this.LoadTestConfiguration();
            string tableName = this.GenerateRandomTableName();
            Console.WriteLine("tableName = {0}", tableName);
            this.SetupRTableEnv(true, tableName, this.useHttps);

            for (int i = 0; i < this.cloudTables.Count; i++)
            {
                Assert.IsTrue(this.cloudTables[i].Exists(), "RTable does not exist in storage account #{0}", i);
            }        
        }

        /// <summary>
        /// This help function is for getting the reference behavior of the Storage DLL when HttpMangler is enabled.
        /// Edit HttpMangler.cs to use HandleFiddlerEvent_DEBUG().
        /// </summary>
        /// <param name="entityPartitionKey"></param>
        /// <param name="entityRowKey"></param>
        /// <param name="targetStorageAccount"></param>
        /// <param name="targetApiExpectedToFail"></param>
        /// <param name="behaviors"></param>
        protected void SetupAndRunXStoreHttpManglerTest(
            string entityPartitionKey,
            string entityRowKey,
            int targetStorageAccount,
            bool targetApiExpectedToFail,
            ProxyBehavior[] behaviors)
        {
            Assert.IsTrue(0 <= targetStorageAccount && targetStorageAccount < this.actualStorageAccountsUsed.Count,
                    "targetStorageAccount={0} is out-of-range", targetStorageAccount);
            int index = this.actualStorageAccountsUsed[targetStorageAccount];
            string accountNameToTamper = this.rtableTestConfiguration.StorageInformation.AccountNames[index];
            Console.WriteLine("accountNameToTamper={0}", accountNameToTamper);

            CloudTableClient tableClient = this.cloudTableClients[targetStorageAccount];
            CloudTable table = tableClient.GetTableReference(this.repTable.TableName);

            //
            // Insert
            //            
            string jobType = entityPartitionKey;
            string jobId = entityRowKey;
            string referenceMessage = SampleRTableEntity.GenerateRandomMessage();
            SampleRTableEntity originalEntity = new SampleRTableEntity(jobType, jobId, referenceMessage);

            Console.WriteLine("\nCalling XStore Insert...");
            TableOperation insertOperation = TableOperation.Insert(originalEntity);
            TableResult insertResult = table.Execute(insertOperation);

            Assert.IsNotNull(insertResult, "insertResult = null");
            Console.WriteLine("insertResult.HttpStatusCode = {0}", insertResult.HttpStatusCode);
            Console.WriteLine("insertResult.ETag = {0}", insertResult.Etag);
            Assert.AreEqual((int)HttpStatusCode.NoContent, insertResult.HttpStatusCode, "insertResult.HttpStatusCode mismatch");
            Assert.IsFalse(string.IsNullOrEmpty(insertResult.Etag), "insertResult.ETag = null or empty");

            ITableEntity row = (ITableEntity)insertResult.Result;

            //
            // Retrieve
            //
            Console.WriteLine("Calling XStore Retrieve...");
            TableOperation retrieveOperation = TableOperation.Retrieve<SampleRTableEntity>(row.PartitionKey, row.RowKey);
            TableResult retrieveResult = table.Execute(retrieveOperation);

            Assert.IsNotNull(retrieveResult, "retrieveResult = null");
            Console.WriteLine("retrieveResult.HttpStatusCode = {0}", retrieveResult.HttpStatusCode);
            Assert.AreEqual((int)HttpStatusCode.OK, retrieveResult.HttpStatusCode, "retrieveResult.HttpStatusCode mismatch");
            SampleRTableEntity retrievedEntity = (SampleRTableEntity)retrieveResult.Result;

            Console.WriteLine("retrieveEntity:\n{0}", retrievedEntity);
            Assert.IsTrue(originalEntity.Equals(retrievedEntity), "originalEntity != retrievedEntity");

            //
            // Replace with HttpMangler enabled
            //
            Console.WriteLine("Calling XStore TableOperation.Replace with HttpMangler enabled...");
            referenceMessage = SampleRTableEntity.GenerateRandomMessage();
            retrievedEntity.Message = referenceMessage;

            TableOperation updateOperation = TableOperation.Replace(retrievedEntity);
            bool abortTest = false;
            try
            {
                using (HttpMangler proxy = new HttpMangler(false, behaviors))
                {
                    Console.WriteLine("Calling table.Execute(updateOperation)");
                    TableResult updateResult = table.Execute(updateOperation);
                    if (targetApiExpectedToFail)
                    {
                        // if targetApi is expected to fail, and we are here, that means something is wrong.
                        abortTest = true;
                        throw new Exception("SetupAndRunXStoreHttpManglerTest(): Should not reach here. HttpMangler allowed an targetApi() to go through UNEXPECTEDLY.");
                    }
                }
            }
            catch (Exception ex)
            {
                if (abortTest)
                {
                    throw;
                }
                else
                {
                    Console.WriteLine("\nException is Expected. targetApi(entity1) threw an exception: {0}\n", ex.ToString());
                }
            }

            //
            // Retrieve again
            //
            Console.WriteLine("After HttpMangler is disabled, calling XStore Retrieve again...");
            retrieveOperation = TableOperation.Retrieve<SampleRTableEntity>(row.PartitionKey, row.RowKey);
            retrieveResult = table.Execute(retrieveOperation);

            Assert.IsNotNull(retrieveResult, "retrieveResult = null");
            Console.WriteLine("retrieveResult.HttpStatusCode = {0}", retrieveResult.HttpStatusCode);
            Assert.AreEqual((int)HttpStatusCode.OK, retrieveResult.HttpStatusCode, "retrieveResult.HttpStatusCode mismatch");
            SampleRTableEntity retrievedEntity2 = (SampleRTableEntity)retrieveResult.Result;

            Console.WriteLine("retrieveEntity2:\n{0}", retrievedEntity2);
            Assert.IsTrue(originalEntity.Equals(retrievedEntity2), "originalEntity != retrievedEntity2");
        }


        protected void SetupAndRunTamperTableBehavior(
            string entityPartitionKey,
            string entityRowKey,
            int targetStorageAccount,
            TargetRTableWrapperApi<SampleRTableEntity> targetApi,
            bool targetApiExpectedToFail,
            bool checkOriginalEntityUnchanged,
            bool checkStorageAccountsConsistent,
            int skipInitialSessions = 0)
        {
            DateTime httpManglerStartTime;
            this.SetupAndRunTamperTableBehavior(
                entityPartitionKey,
                entityRowKey,
                targetStorageAccount,
                targetApi,
                targetApiExpectedToFail,
                checkOriginalEntityUnchanged,
                checkStorageAccountsConsistent,
                out httpManglerStartTime,
                skipInitialSessions);
        }


        /// <summary>
        /// Helper function to set up an original entity and then run the **Tamper** Behavior against the specified storage account.
        /// </summary>
        /// <param name="entityPartitionKey"></param>
        /// <param name="entityRowKey"></param>
        /// <param name="targetStorageAccount">Specify which storage account to tamper with. 0 = Head; Count-1 = Tail</param>
        /// <param name="targetApi">Will call this RTable API while HttpMangler is enabled</param>
        /// <param name="targetApiExpectedToFail">True means the targetAPi is expected to fail when HttpMangler is enabled</param>
        /// <param name="checkOriginalEntityUnchanged">True means verify that the original entity (via GetRow()) is unchanged after this function.</param>
        /// <param name="checkStorageAccountsConsistent">True means verify that the individual storage accounts are consistent.</param>
        /// <param name="skipInitialSessions">Skip this many initial sessions. (Let them go through without being tampered)</param>
        protected void SetupAndRunTamperTableBehavior(
            string entityPartitionKey,
            string entityRowKey,
            int targetStorageAccount,
            TargetRTableWrapperApi<SampleRTableEntity> targetApi,
            bool targetApiExpectedToFail,
            bool checkOriginalEntityUnchanged,
            bool checkStorageAccountsConsistent,
            out DateTime httpManglerStartTime,
            int skipInitialSessions = 0)
        {
            Assert.IsTrue(0 <= targetStorageAccount && targetStorageAccount < this.actualStorageAccountsUsed.Count,
                    "SetupAndRunTamperTableBehavior() is called with out-of-range targetStorageAccount={0}", targetStorageAccount);
            int index = this.actualStorageAccountsUsed[targetStorageAccount];
            string accountNameToTamper = this.rtableTestConfiguration.StorageInformation.AccountNames[index];
            Console.WriteLine("SetupAndRunTamperTableBehavior(): accountNameToTamper={0} skipInitialSessions={1}", 
                accountNameToTamper, skipInitialSessions);

            // Tamper behavior
            ProxyBehavior[] behaviors = new[]
                {
                    TamperBehaviors.TamperAllRequestsIf(
                        (session => { session.Abort(); }),
                        skipInitialSessions,
                        AzureStorageSelectors.TableTraffic().IfHostNameContains(accountNameToTamper))
                };

            //
            // Arrange and Act:
            //
            SampleRTableEntity originalEntity = this.SetupAndRunHttpManglerBehaviorHelper(
                                                        entityPartitionKey,
                                                        entityRowKey,
                                                        behaviors,
                                                        targetApi,
                                                        targetApiExpectedToFail,
                                                        out httpManglerStartTime);

            //
            // Assert: 
            //      
            if (checkOriginalEntityUnchanged)
            {
                // Validate originalEntity remain unchanged.
                this.ExecuteReadRowAndValidate(entityPartitionKey, entityRowKey, originalEntity);
            }
            
            // For debug purposes: read from the Head and Tail accounts, and check for consistency
            this.ReadFromIndividualAccountsDirectly(entityPartitionKey, entityRowKey, checkStorageAccountsConsistent);            
        }



        /// <summary>
        /// Helper function to create some initial entities and then call the specified batchOperation with HttpMangler enabled.
        /// </summary>
        /// <param name="count"></param>
        /// <param name="jobType"></param>
        /// <param name="jobId"></param>
        /// <param name="targetStorageAccount"></param>
        /// <param name="opTypes"></param>
        /// <param name="targetApiExpectedToFail"></param>
        /// <param name="checkOriginalEntityUnchanged"></param>
        /// <param name="checkStorageAccountsConsistent"></param>
        /// <param name="httpManglerStartTime"></param>
        /// <param name="skipInitialSessions"></param>
        /// <returns>ParitionKey of the initial entities created</returns>
        protected string SetupAndRunTemperBatchOperation(
            int count,
            string jobType,
            string jobId,
            int targetStorageAccount,
            List<TableOperationType> opTypes,
            bool targetApiExpectedToFail,
            bool checkOriginalEntityUnchanged,
            bool checkStorageAccountsConsistent,
            out DateTime httpManglerStartTime,
            int skipInitialSessions = 0)
        {

            Assert.IsTrue(0 <= targetStorageAccount && targetStorageAccount < this.actualStorageAccountsUsed.Count,
                   "SetupAndRunTemperBatchOperation() is called with out-of-range targetStorageAccount={0}", targetStorageAccount);
            int index = this.actualStorageAccountsUsed[targetStorageAccount];
            string accountNameToTamper = this.rtableTestConfiguration.StorageInformation.AccountNames[index];
            Console.WriteLine("SetupAndRunTemperBatchOperation(): accountNameToTamper={0} skipInitialSessions={1}",
                accountNameToTamper, skipInitialSessions);

            Assert.AreEqual(count, opTypes.Count, "count and opTypes.Count should be the same");

            //
            // Tamper behavior
            //
            ProxyBehavior[] behaviors = new[]
                {
                    TamperBehaviors.TamperAllRequestsIf(
                        (session => { session.Abort();}),
                        skipInitialSessions,
                        AzureStorageSelectors.TableTraffic().IfHostNameContains(accountNameToTamper))
                };

            string jobIdTemplate = jobId + "-{0}";
            string messageTemplate = "message-{0}";
            string updatedMessageTemplate = "updated-" + messageTemplate;

            string partitionKey = string.Empty;
            //
            // Insert entities
            //
            for (int i = 0; i < count; i++)
            {
                SampleRTableEntity originalEntity = new SampleRTableEntity(
                    jobType,
                    string.Format(jobIdTemplate, i),
                    string.Format(messageTemplate, i));

                this.repTable.Execute(TableOperation.Insert(originalEntity));
                partitionKey = originalEntity.PartitionKey;
            }

            //
            // Retrieve entities and use them to create batchOperation to Replace or Delete
            //
            IEnumerable<SampleRTableEntity> allEntities = this.rtableWrapper.GetAllRows(partitionKey);
            TableBatchOperation batchOperation = new TableBatchOperation();
            int m = 0;
            foreach (SampleRTableEntity entity in allEntities)
            {
                Console.WriteLine("{0}", entity.ToString());
                Console.WriteLine("---------------------------------------");
                if (opTypes[m] == TableOperationType.Replace)
                {
                    SampleRTableEntity replaceEntity = new SampleRTableEntity(
                        entity.JobType,
                        entity.JobId,
                        string.Format(updatedMessageTemplate, m))
                    {
                        ETag = entity._rtable_Version.ToString()
                    };
                    batchOperation.Replace(replaceEntity);
                }
                else if (opTypes[m] == TableOperationType.Delete)
                {
                    entity.ETag = entity._rtable_Version.ToString();
                    batchOperation.Delete(entity);
                }
                else
                {
                    throw new ArgumentException(
                        string.Format("opType={0} is NOT supported", opTypes[m]),
                        "opType");
                }
                m++;
            }

            //
            // Enable HttpMangler
            // Call this.repTable.ExecuteBatch(batchOperation)
            //
            this.RunHttpManglerBehaviorHelper(
                batchOperation,
                behaviors,
                targetApiExpectedToFail,
                out httpManglerStartTime);

            if (checkOriginalEntityUnchanged)
            {
                Console.WriteLine("Validate originalEntity remain unchanged.");
                allEntities = this.rtableWrapper.GetAllRows(partitionKey);
                batchOperation = new TableBatchOperation();
                m = 0;
                foreach (SampleRTableEntity entity in allEntities)
                {
                    Console.WriteLine("{0}", entity.ToString());
                    Console.WriteLine("---------------------------------------");
                    Assert.AreEqual(string.Format(jobType, m), entity.JobType, "JobType does not match");
                    Assert.AreEqual(string.Format(jobIdTemplate, m), entity.JobId, "JobId does not match");
                    Assert.AreEqual(string.Format(messageTemplate, m), entity.Message, "Message does not match");
                    m++;
                }
                Console.WriteLine("Passed validation");
            }

            //
            // After httpMangler is turned off, read from individual accounts...
            //            
            Console.WriteLine("\nAfter httpMangler is turned off, read from individual accounts...");
            for (int i = 0; i < count; i++)
            {
                this.ReadFromIndividualAccountsDirectly(
                    jobType,
                    string.Format(jobIdTemplate, i),
                    checkStorageAccountsConsistent);
            }

            return partitionKey;
        }


        /// <summary>
        /// Helper function to set up an original entity and then run the **Tamper** Behavior against the specified storage account.
        /// This function takes a targetApi of this form: Func<string, string, SampleRTableEntity> targetApi
        /// </summary>
        /// <param name="entityPartitionKey"></param>
        /// <param name="entityRowKey"></param>
        /// <param name="targetStorageAccount"></param>
        /// <param name="targetApi"></param>
        /// <param name="targetApiExpectedToFail"></param>
        protected void SetupAndRunTamperTableBehavior(
            string entityPartitionKey,
            string entityRowKey,
            int targetStorageAccount,
            Func<string, string, SampleRTableEntity> targetApi,
            bool targetApiExpectedToFail,
            bool checkOriginalEntityUnchanged,
            bool checkStorageAccountsConsistent)
        {
            Assert.IsTrue(0 <= targetStorageAccount && targetStorageAccount < this.actualStorageAccountsUsed.Count,
                    "SetupAndRunTamperTableBehavior() is called with out-of-range targetStorageAccount={0}", targetStorageAccount);
            int index = this.actualStorageAccountsUsed[targetStorageAccount];
            string accountNameToTamper = this.rtableTestConfiguration.StorageInformation.AccountNames[index];
            Console.WriteLine("SetupAndRunTamperTableBehavior(): accountNameToTamper={0}", accountNameToTamper);

            // Tamper behavior
            ProxyBehavior[] behaviors = new[]
                {
                    TamperBehaviors.TamperAllRequestsIf(
                        Actions.ThrottleTableRequest,
                        AzureStorageSelectors.TableTraffic().IfHostNameContains(accountNameToTamper))
                };

            //
            // Arrange and Act:
            //
            SampleRTableEntity originalEntity = this.SetupAndRunHttpManglerBehaviorHelper(
                                                        entityPartitionKey,
                                                        entityRowKey,
                                                        behaviors,
                                                        targetApi,
                                                        targetApiExpectedToFail);

            //
            // Assert: 
            //      
            if (checkOriginalEntityUnchanged)
            {
                // Validate originalEntity remain unchanged.
                this.ExecuteReadRowAndValidate(entityPartitionKey, entityRowKey, originalEntity);
            }

            // For debug purposes: read from the Head and Tail accounts, and check for consistency
            this.ReadFromIndividualAccountsDirectly(entityPartitionKey, entityRowKey, checkStorageAccountsConsistent);    
        }

        /// <summary>
        /// Helper function to set up an original entity and then run the **Delay** Behavior against the specified storage account.
        /// </summary>
        /// <param name="entityPartitionKey"></param>
        /// <param name="entityRowKey"></param>
        /// <param name="targetStorageAccount">Specify which storage account to tamper with. 0 = Head; Count-1 = Tail</param>
        /// <param name="delayInMs">This much delay will be introduced to transmissions to the specified storage account.</param>
        /// <param name="targetApi">Will call this RTable API while HttpMangler is enabled</param>
        /// <param name="targetApiExpectedToFail">True means the targetAPi is expected to fail when HttpMangler is enabled</param>
        protected void SetupAndRunDelayTableBehavior(
            string entityPartitionKey, 
            string entityRowKey, 
            int targetStorageAccount, 
            int delayInMs,
            TargetRTableWrapperApi<SampleRTableEntity> targetApi,
            bool targetApiExpectedToFail)
        {
            Assert.IsTrue(0 <= targetStorageAccount && targetStorageAccount < this.actualStorageAccountsUsed.Count,
                    "SetupAndRunDelayTableBehavior() is called with out-of-range targetStorageAccount={0}", targetStorageAccount);
            int index = this.actualStorageAccountsUsed[targetStorageAccount];
            string accountNameToTamper = this.rtableTestConfiguration.StorageInformation.AccountNames[index];
            Console.WriteLine("SetupAndRunDelayTableBehavior(): accountNameToTamper={0}", accountNameToTamper);

            // Delay bahavior
            ProxyBehavior[] behaviors = new[]
                {
                    DelayBehaviors.DelayAllRequestsIf(
                            delayInMs,
                            AzureStorageSelectors.TableTraffic().IfHostNameContains(accountNameToTamper))
                };

            //
            // Arrange and Act:
            //            
            DateTime httpManglerStartTime;
            SampleRTableEntity originalEntity = this.SetupAndRunHttpManglerBehaviorHelper(
                                                        entityPartitionKey,
                                                        entityRowKey,
                                                        behaviors,
                                                        targetApi,
                                                        targetApiExpectedToFail,
                                                        out httpManglerStartTime);

            //
            // Assert: 
            //      
            if (targetApiExpectedToFail)
            {
                // targetApi is expected to fail, so originalEntity should be untouched.
                this.ExecuteReadRowAndValidate(entityPartitionKey, entityRowKey, originalEntity);
            }
            else
            {
                // For debug purposes: read from the Head and Tail accounts, and check for consistency
                this.ReadFromIndividualAccountsDirectly(entityPartitionKey, entityRowKey, true);
            }            
        }

        protected void SetupAndRunThrottleTableBehavior(
            string entityPartitionKey,
            string entityRowKey,
            int targetStorageAccount,
            TargetRTableWrapperApi<SampleRTableEntity> targetApi,
            bool targetApiExpectedToFail,
            bool checkOriginalEntityUnchanged,
            bool checkStorageAccountsConsistent,
            int skipInitialSessions = 0)
        {
            DateTime httpManglerStartTime;
            this.SetupAndRunThrottleTableBehavior(
                entityPartitionKey,
                entityRowKey,
                targetStorageAccount,
                targetApi,
                targetApiExpectedToFail,
                checkOriginalEntityUnchanged,
                checkStorageAccountsConsistent,
                out httpManglerStartTime,
                skipInitialSessions);
        }

        /// <summary>
        /// Helper function to set up an original entity and then run the **Throttle** Behavior against the specified storage account.
        /// </summary>
        /// <param name="entityPartitionKey"></param>
        /// <param name="entityRowKey"></param>
        /// <param name="targetStorageAccount">Specify which storage account to tamper with. 0 = Head; Count-1 = Tail</param>
        /// <param name="targetApi">Will call this RTable API while HttpMangler is enabled</param>
        /// <param name="targetApiExpectedToFail">True means the targetAPi is expected to fail when HttpMangler is enabled</param>
        /// <param name="checkOriginalEntityUnchanged">True means verify that the original entity (via GetRow()) is unchanged after this function.</param>
        /// <param name="checkStorageAccountsConsistent">True means verify that the individual storage accounts are consistent.</param>
        /// <param name="skipInitialSessions">Skip this many initial sessions. (Let them go through without being tampered)</param>
        protected void SetupAndRunThrottleTableBehavior(
            string entityPartitionKey,
            string entityRowKey,
            int targetStorageAccount,
            TargetRTableWrapperApi<SampleRTableEntity> targetApi,
            bool targetApiExpectedToFail,
            bool checkOriginalEntityUnchanged,
            bool checkStorageAccountsConsistent,
            out DateTime httpManglerStartTime,
            int skipInitialSessions = 0)
        {
            Assert.IsTrue(0 <= targetStorageAccount && targetStorageAccount < this.actualStorageAccountsUsed.Count,
                    "SetupAndRunThrottleTableBehavior() is called with out-of-range targetStorageAccount={0}", targetStorageAccount);
            int index = this.actualStorageAccountsUsed[targetStorageAccount];
            string accountNameToTamper = this.rtableTestConfiguration.StorageInformation.AccountNames[index];
            Console.WriteLine("SetupAndRunThrottleTableBehavior(): accountNameToTamper={0} skipInitialSessions={1}",
                accountNameToTamper, skipInitialSessions);

            // Throttle behavior
            ProxyBehavior[] behaviors = new[]
                {
                    TamperBehaviors.TamperAllRequestsIf(
                            Actions.ThrottleTableRequest,
                            skipInitialSessions,
                            AzureStorageSelectors.TableTraffic().IfHostNameContains(accountNameToTamper))         
                };

            //
            // Arrange and Act:
            //
            SampleRTableEntity originalEntity = this.SetupAndRunHttpManglerBehaviorHelper(
                                                        entityPartitionKey,
                                                        entityRowKey,
                                                        behaviors,
                                                        targetApi,
                                                        targetApiExpectedToFail,
                                                        out httpManglerStartTime);

            //
            // Assert: 
            //      
            if (checkOriginalEntityUnchanged)
            {
                // Validate originalEntity remain unchanged.
                this.ExecuteReadRowAndValidate(entityPartitionKey, entityRowKey, originalEntity);
            }

            // For debug purposes: read from the Head and Tail accounts, and check for consistency
            this.ReadFromIndividualAccountsDirectly(entityPartitionKey, entityRowKey, checkStorageAccountsConsistent);            
        }


        /// <summary>
        /// Helper function to delete the entity with the specified "partitionKey" and "rowKey" from the 
        /// storage tables directly. Call this before running a test if you want to start with a clean Table.
        /// </summary>
        /// <param name="entityPartitionKey"></param>
        /// <param name="entityRowKey"></param>
        protected void ForceDeleteEntryFromStorageTablesDirectly(string entityPartitionKey, string entityRowKey)
        {
            string partitionKey;
            string rowKey;

            SampleRTableEntity.GenerateKeys(entityPartitionKey, entityRowKey, out partitionKey, out rowKey);

            TableOperation retrieveOperation = TableOperation.Retrieve(partitionKey, rowKey);
            for (int i = 0; i < this.cloudTables.Count; i++)
            {
                TableResult result = this.cloudTables[i].Execute(retrieveOperation);
                if (result != null && result.HttpStatusCode == (int)HttpStatusCode.OK)
                {
                    TableOperation deleteOperation = TableOperation.Delete((ITableEntity)result.Result);
                    this.cloudTables[i].Execute(deleteOperation);
                }
            }
        }

        /// <summary>
        /// Helper function to make ReadEntity() API call and validate the specified entity does not exist.
        /// </summary>
        /// <param name="entityPartitionKey"></param>
        /// <param name="entityRowKey"></param>
        protected void ExecuteReadRowAndValidateNotExist(string entityPartitionKey, string entityRowKey)
        {
            Console.WriteLine("\nExecuteReadRowAndValidateNotExist(): Read entity back and validate...");
            string jobType = entityPartitionKey;
            string jobId = entityRowKey;
            SampleRTableEntity retrievedEntity = this.rtableWrapper.ReadEntity(jobType, jobId);
            if (retrievedEntity != null)
            {
                Console.WriteLine("ERROR: retrievedEntity != null");
                Console.WriteLine("retrievedEntity = \n{0}", retrievedEntity.ToString());
            }
            Assert.IsNull(retrievedEntity, "retrievedEntity != null");
        }

        /// <summary>
        /// Helper function to make ReadEntity() API call and validate the retrieve contents match the specified originalEntity.
        /// Call this helper after you have finished tampered some operation on the same entity.
        /// </summary>
        /// <param name="entityPartitionKey"></param>
        /// <param name="entityRowKey"></param>
        /// <param name="originalEntity">Set it to null to by-pass validation against originalEntity.</param>
        /// <param name="checkIndividualAccounts">True means will check the entity in each storage account for consistency</param>
        protected void ExecuteReadRowAndValidate(
            string entityPartitionKey, 
            string entityRowKey, 
            SampleRTableEntity originalEntity, 
            bool checkIndividualAccounts = false)
        {
            Console.WriteLine("\nExecuteReadRowAndValidate(): Read entity back and validate...");
            string jobType = entityPartitionKey;
            string jobId = entityRowKey;
            SampleRTableEntity retrievedEntity = this.rtableWrapper.ReadEntity(jobType, jobId);
            if (retrievedEntity == null)
            {
                Console.WriteLine("ERROR: retrievedEntity = null");
            }
            else
            {
                Console.WriteLine("retrievedEntity = \n{0}", retrievedEntity.ToString());
            }
            if (originalEntity != null)
            {
                Console.WriteLine("originalEntity = \n{0}", originalEntity.ToString());
            }

            // For debug purposes: read from the Head and Tail accounts:
            this.ReadFromIndividualAccountsDirectly(jobType, jobId, checkIndividualAccounts);

            Assert.IsNotNull(retrievedEntity, "retrievedEntity is null UNEXPECTEDLY");
            if (originalEntity != null)
            {
                Assert.IsTrue(retrievedEntity.Equals(originalEntity), "retrievedEntity NOT equal to originalEntity");
                Console.WriteLine("Passed validation");
            }
        }

        /// <summary>
        /// Helper function to create a new entity and validate the creation was successful.
        /// </summary>
        /// <param name="entityPartitionKey"></param>
        /// <param name="entityRowKey"></param>
        protected void ExecuteCreateRowAndValidate(
            string entityPartitionKey, 
            string entityRowKey) 
        {
            Console.WriteLine("\nExecuteCreateRowAndValidate(): Create entity and validate...");
            string jobType = entityPartitionKey;
            string jobId = entityRowKey;
            string referenceMessage = SampleRTableEntity.GenerateRandomMessage();

            SampleRTableEntity createEntity = new SampleRTableEntity(jobType, jobId, referenceMessage);
            bool gotExceptionInLastAttempt = true;
            int retries = 0;
            while (retries < MaxRetries)
            {
                try
                {
                    Console.WriteLine("\nretries={0}. Calling InsertRow(). referenceMessage={1}", retries, referenceMessage);
                    this.rtableWrapper.InsertRow(createEntity);
                    gotExceptionInLastAttempt = false;
                    break;
                }
                catch (RTableConflictException ex)
                {
                    Console.WriteLine("retries={0}. InsertRow() got an RTableConflictException: {1}",
                        retries, ex.ToString()); 
                    retries++;
                    gotExceptionInLastAttempt = true;
                    Thread.Sleep(this.configurationService.LockTimeout);

                    // For debug purposes: read from the Head and Tail accounts:
                    this.ReadFromIndividualAccountsDirectly(entityPartitionKey, entityRowKey);                    
                }
                catch (RTableRetriableException ex)
                {
                    Console.WriteLine("retries={0}. InsertRow() got an RTableRetriableException: {1}",
                        retries, ex.ToString());
                    retries++;
                    gotExceptionInLastAttempt = true;
                    Thread.Sleep(ConflictExceptionSleepTimeInMsec);

                    // For debug purposes: read from the Head and Tail accounts:
                    this.ReadFromIndividualAccountsDirectly(entityPartitionKey, entityRowKey);
                }
            }
            Console.WriteLine("gotExceptionInLastAttempt = {0}", gotExceptionInLastAttempt);

            Console.WriteLine("\nRead entity back and validate...");
            SampleRTableEntity retrievedEntity = this.rtableWrapper.ReadEntity(jobType, jobId);

            Assert.IsNotNull(retrievedEntity, "retrievedEntity is null UNEXPECTEDLY.");
            Console.WriteLine("retrievedEntity:\n{0}", retrievedEntity.ToString());
            Assert.IsTrue(retrievedEntity.JobType == jobType, "retrievedEntity.JobType is incorrect.");
            Assert.IsTrue(retrievedEntity.JobId == jobId, "retrievedEntity.JobId is incorrect.");
            Assert.IsTrue(retrievedEntity.Message == referenceMessage, "retrievedEntity.Message is incorrect.");
            Console.WriteLine("Passed validation");
        }

        /// <summary>
        /// Helper function to make ReplaceRow() API call and validate correctness. It assumes the specified entity already exists.
        /// Call this helper after you have finished tampered some operation on the same entity.
        /// </summary>
        /// <param name="entityPartitionKey"></param>
        /// <param name="entityRowKey"></param>
        protected void ExecuteReplaceRowAndValidate(string entityPartitionKey, string entityRowKey)
        {
            string jobType = entityPartitionKey;
            string jobId = entityRowKey;
            Console.WriteLine("\nExecuteReplaceRowAndValidate(): Trying to update the entity...");

            SampleRTableEntity originalEntity = null;
            string referenceMessage = SampleRTableEntity.GenerateRandomMessage();

            bool gotExceptionInLastAttempt = true;
            int retries = 0;
            while (retries < MaxRetries)
            {
                try
                {
                    Console.WriteLine("\nretries={0}. Will modify Message and calling ReplaceRow(originalEntity). New referenceMessage={1}",
                        retries, referenceMessage);

                    originalEntity = this.rtableWrapper.ReadEntity(jobType, jobId);
                    Assert.IsNotNull(originalEntity, "originalEntity is null. Make sure entity exists before calling ExecuteReplaceRowAndValidate()");
                    Console.WriteLine("originalEntity:\n{0}", originalEntity.ToString());
                    originalEntity.Message = referenceMessage;

                    this.rtableWrapper.ReplaceRow(originalEntity);
                    Console.WriteLine("retries={0}. Done ReplaceRow(originalEntity)", retries);
                    gotExceptionInLastAttempt = false;
                    break;
                }
                catch (RTableConflictException ex)
                {
                    Console.WriteLine("retries={0}. ReadEntity() or ReplaceRow() got an RTableConflictException: {1}",
                        retries, ex.ToString());
                    retries++;
                    gotExceptionInLastAttempt = true;
                    Thread.Sleep(ConflictExceptionSleepTimeInMsec);

                    // For debug purposes: read from the Head and Tail accounts:
                    this.ReadFromIndividualAccountsDirectly(jobType, jobId);
                }
                catch (RTableRetriableException ex)
                {
                    Console.WriteLine("retries={0}. ReadEntity() or ReplaceRow() got an RTableRetriableException: {1}",
                        retries, ex.ToString());
                    retries++;
                    gotExceptionInLastAttempt = true;
                    Thread.Sleep(ConflictExceptionSleepTimeInMsec);

                    // For debug purposes: read from the Head and Tail accounts:
                    this.ReadFromIndividualAccountsDirectly(jobType, jobId);
                }
            }
            Console.WriteLine("gotExceptionInLastAttempt={0} retries={1} MaxRetries={2}", gotExceptionInLastAttempt, retries, MaxRetries);

            Console.WriteLine("\nRead entity back and validate...");
            SampleRTableEntity updatedEntity = this.rtableWrapper.ReadEntity(jobType, jobId);
            if (updatedEntity == null)
            {
                Console.WriteLine("ERROR: updatedEntity = null");
            }
            else
            {
                Console.WriteLine("updatedEntity = \n{0}", updatedEntity.ToString());
            }
            Console.WriteLine("referenceMessage = {0}", referenceMessage);

            // For debug purposes: read from the Head and Tail accounts and validate consistency of differnt accounts
            this.ReadFromIndividualAccountsDirectly(jobType, jobId, true);

            Assert.IsFalse(gotExceptionInLastAttempt, "The last API call should not throw an exception.");
            Assert.IsNotNull(updatedEntity, "updatedEntity is null UNEXPECTEDLY");
            Assert.AreEqual(
                referenceMessage,
                updatedEntity.Message,
                "updatedEntity.Message is NOT equal to originalEntity.Message (referenceMessage).");

            Assert.IsTrue(
                updatedEntity.Equals(originalEntity), "updatedEntity NOT equal to originalEntity");

            Assert.AreEqual(
                jobType,
                updatedEntity.JobType,
                "updatedEntity.JobType is NOT equal to jobType.");

            Assert.AreEqual(
                jobId,
                updatedEntity.JobId,
                "updatedEntity.JobId is NOT equal to jobId.");

            Console.WriteLine("Passed validation");
        }

        /// <summary>
        /// Helper function to make DeleteRow() API call and validate the entity is gone. 
        /// This function assumes the entity already exists.
        /// Call this helper after you have finished tampered some operation on the same entity.
        /// </summary>
        /// <param name="entityPartitionKey"></param>
        /// <param name="entityRowKey"></param>
        protected void ExecuteDeleteRowAndValidate(string entityPartitionKey, string entityRowKey)
        {
            Console.WriteLine("\nExecuteDeleteRowAndValidate(): Delete entity and validate...");
            string jobType = entityPartitionKey;
            string jobId = entityRowKey;

            bool gotExceptionInLastAttempt = true;
            int retries = 0;
            while (retries < MaxRetries)
            {
                try
                {
                    Console.WriteLine("\nretries={0}. Calling ReadEntity()...", retries);
                    SampleRTableEntity originalEntity = this.rtableWrapper.ReadEntity(jobType, jobId);
                    if (originalEntity != null)
                    {
                        Console.WriteLine("retries={0}. ReadEntity() returned non-null, so calling DeleteRow().", retries);
                        this.rtableWrapper.DeleteRow(originalEntity);
                        gotExceptionInLastAttempt = false;
                        break;
                    }
                    else
                    {
                        Console.WriteLine("retries={0}. ReadEntity() returned null, so no need to call DeleteRow().", retries);
                        gotExceptionInLastAttempt = false;
                        break;
                    }
                }
                catch (RTableConflictException ex)
                {
                    Console.WriteLine("retries={0}. ReadEntity() or DeleteRow() got an RTableConflictException: {1}",
                        retries, ex.ToString());
                    retries++;
                    gotExceptionInLastAttempt = true;
                    Thread.Sleep(ConflictExceptionSleepTimeInMsec);

                    // For debug purposes: read from the Head and Tail accounts:
                    this.ReadFromIndividualAccountsDirectly(jobType, jobId);
                }
                catch (RTableRetriableException ex)
                {
                    Console.WriteLine("retries={0}. ReadEntity() or DeleteRow() got an RTableRetriableException: {1}",
                        retries, ex.ToString());
                    retries++;
                    gotExceptionInLastAttempt = true;
                    Thread.Sleep(ConflictExceptionSleepTimeInMsec);

                    // For debug purposes: read from the Head and Tail accounts:
                    this.ReadFromIndividualAccountsDirectly(jobType, jobId);
                }
            }
            Console.WriteLine("gotExceptionInLastAttempt={0} retries={1} MaxRetries={2}", gotExceptionInLastAttempt, retries, MaxRetries);

            // Read it back to confirm it is null
            Console.WriteLine("\nCalling ReadEntity() and expecting null...");
            SampleRTableEntity entityAfterDeleteRow = this.rtableWrapper.ReadEntity(jobType, jobId);
            if (entityAfterDeleteRow == null)
            {
                Console.WriteLine("Pass: entityAfterDeleteRow = null");
            }
            else
            {
                Console.WriteLine("ERROR: entityAfterDeleteRow != null");
                Console.WriteLine("entityAfterDeleteRow = {0}", entityAfterDeleteRow.ToString());
            }

            Assert.IsFalse(gotExceptionInLastAttempt, "The last API call should not throw an exception.");
            Assert.IsNull(entityAfterDeleteRow, "entityAfterDeleteRow is NOT null UNEXPECTEDLY.");
            Console.WriteLine("Passed validation");
        }


        /// <summary>
        /// Helper function to execute the specified BatchOperation after HttpMangler is turned off and validate correctness.
        /// </summary>
        /// <param name="count">Number of operations in the batch</param>
        /// <param name="partitionKey">partitionKey to operate on</param>
        /// <param name="jobType">partitionKey is generated from jobType</param>
        /// <param name="jobId">RowKey is generated from jobId. JobIdTemplate = jobId-{0}</param>
        /// <param name="opTypes">Specifies the batch operation to be performed</param>
        protected void ExecuteBatchOperationAndValidate(
            int count,
            string partitionKey,
            string jobType,
            string jobId,
            List<TableOperationType> opTypes)
        {
            Console.WriteLine("\nExecuteBatchOperationAndValidate(): Trying to batch update {0} entities...", count);

            Assert.IsNotNull(opTypes, "opTypes = null");
            Assert.AreEqual(count, opTypes.Count, "count and opTypes.Count should be the same");

            string jobIdTemplate = jobId + "-{0}";
            string replaceMessageTemplate = "updated-after-httpMangler-{0}";

            IEnumerable<SampleRTableEntity> allEntities = null;
            TableBatchOperation batchOperation = null;
            int m = 0;
            bool gotExceptionInLastAttempt = true;
            int retries = 0;
            while (retries < MaxRetries)
            {
                try
                {
                    //
                    // GetAllRows()
                    //
                    allEntities = this.rtableWrapper.GetAllRows(partitionKey);

                    //
                    // Create a batchOperation to perform the specified opTypes
                    //
                    batchOperation = new TableBatchOperation();
                    m = 0;
                    foreach (SampleRTableEntity entity in allEntities)
                    {
                        if (opTypes[m] == TableOperationType.Replace)
                        {
                            // set up the new entity to be used in the batch operation
                            SampleRTableEntity replaceEntity = new SampleRTableEntity(
                                entity.JobType,
                                entity.JobId,
                                string.Format(replaceMessageTemplate, m))
                            {
                                ETag = entity._rtable_Version.ToString()
                            };
                            // add the operation to the batch operation

                            batchOperation.Replace(replaceEntity);
                        }
                        else if (opTypes[m] == TableOperationType.Delete)
                        {
                            batchOperation.Delete(entity);
                        }
                        else
                        {
                            throw new ArgumentException(
                                            string.Format("opType={0} is NOT supported", opTypes[m]),
                                            "opType");
                        }
                        m++;
                    }

                    //
                    // Call this.repTable.ExecuteBatch(batchOperation);
                    //
                    if (batchOperation.Count == 0)
                    {
                        Console.WriteLine("retries={0}. Done. batchOperation.Count == 0", retries);
                    }
                    else
                    {
                        this.repTable.ExecuteBatch(batchOperation);
                        Console.WriteLine("retries={0}. Done ExecuteBatch()", retries);
                    }
                    gotExceptionInLastAttempt = false;
                    break;
                }
                catch (RTableConflictException ex)
                {
                    Console.WriteLine("retries={0}. ExecuteBatch() got an RTableConflictException: {1}",
                        retries, ex.ToString());
                    retries++;
                    gotExceptionInLastAttempt = true;
                    Thread.Sleep(ConflictExceptionSleepTimeInMsec);
                }
            }

            Console.WriteLine("gotExceptionInLastAttempt={0} retries={1} MaxRetries={2}",
                gotExceptionInLastAttempt, retries, MaxRetries);

            Assert.IsFalse(gotExceptionInLastAttempt, "The last API call should not throw an exception.");

            //
            // Final validation
            //
            Console.WriteLine("Final validation...");
            allEntities = this.rtableWrapper.GetAllRows(partitionKey);            
            m = 0;
            int opTypesCounter = 0;
            foreach (SampleRTableEntity entity in allEntities)
            {
                Console.WriteLine("{0}", entity.ToString());
                Console.WriteLine("---------------------------------------");

                // If the operation is Delete, then skip it. No need to validate.
                while (opTypesCounter < count && opTypes[m] == TableOperationType.Delete)
                {
                    m++;
                }
                Assert.IsTrue(m < count, "m={0} count={1}:  m shoud be < count, but it is not.", m, count);

                if (opTypes[m] == TableOperationType.Replace)
                {
                    Assert.AreEqual(string.Format(jobType, m), entity.JobType, "JobType does not match");
                    Assert.AreEqual(string.Format(jobIdTemplate, m), entity.JobId, "JobId does not match");
                    Assert.AreEqual(string.Format(replaceMessageTemplate, m), entity.Message, "Message does not match");
                    m++;
                }
                else 
                {
                    throw new ArgumentException(
                                            string.Format("opType={0} is NOT supported", opTypes[opTypesCounter]),
                                            "opType");
                }                
            }

            for (int i = 0; i < count; i++)
            {
                this.ReadFromIndividualAccountsDirectly(
                    jobType,
                    string.Format(jobIdTemplate, i),
                    true);
            }

            Console.WriteLine("Passed final validation.");
        }


        /// <summary>
        /// Helper function to sleep some time until the _rtable_RowLock has expired.
        /// </summary>
        /// <param name="rowLockStartTime">Approximate time the row was locked</param>
        protected void SleepUntilRowLockHasExpired(DateTime rowLockStartTime)
        {
            // After recovery from short outage, sleep some time to wait for entity to be unlocked, then confirm that we can update the row.
            TimeSpan elapsedTimeSinceRowLocked = DateTime.UtcNow - rowLockStartTime;
            int sleepTimeInMs = 0;
            if (this.configurationService.LockTimeout > elapsedTimeSinceRowLocked)
            {
                sleepTimeInMs = (int)(this.configurationService.LockTimeout - elapsedTimeSinceRowLocked).TotalMilliseconds;
            }
            sleepTimeInMs -= ConflictExceptionSleepTimeInMsec; // reduce the sleep time a little bit so that we will hit "_rtable_RowLock not expired" in RTable library.
            if (sleepTimeInMs < 0)
            {
                sleepTimeInMs = 0;
            }
            Console.WriteLine("Sleeping for {0} msec to wait for entity to be unlocked", sleepTimeInMs);
            Thread.Sleep(sleepTimeInMs);
        }
        #endregion Protected Helper functions


        #region Private Helper functions
        /// <summary>
        /// Helper function to set up an original entity and then run the specified HttpMangler Behavior. 
        /// It will return the original entity created.
        /// Step (1): Make sure "entity1" with the specified partitionKey and rowKey exists. If not create it. Read "entity1".
        /// Step (2): Read "entity1" from individual storage tables directly for debugging.
        /// Step (3): Turn on HttpMangler and the specified behavior and call the specified RTable API to operate on "entity1". 
        /// </summary>
        /// <param name="entityPartitionKey"></param>
        /// <param name="entityRowKey"></param>
        /// <param name="behaviors">HttpMangler behaviors</param>
        /// <param name="targetApi">Will call this RTable API while HttpMangler is enabled</param>
        /// <param name="targetApiExpectedToFail">True means the targetAPi is expected to fail when HttpMangler is enabled</param>
        /// <param name="httpManglerStartTime">Output: time at which HttpMangler was enabled.</param>
        /// <returns>Original entity created</returns>        
        private SampleRTableEntity SetupAndRunHttpManglerBehaviorHelper(
            string entityPartitionKey,
            string entityRowKey,
            ProxyBehavior[] behaviors,
            TargetRTableWrapperApi<SampleRTableEntity> targetApi,
            bool targetApiExpectedToFail,
            out DateTime httpManglerStartTime)
        {
            SampleRTableEntity originalEntity = new SampleRTableEntity();

            // "entity1":
            string jobType = entityPartitionKey;
            string jobId = entityRowKey;
            string referenceMessage = SampleRTableEntity.GenerateRandomMessage();

            //
            // Arrange
            //            
            // Make sure an entity exists with the specified partitionKey and rowKey    
            Console.WriteLine("\nMaking sure entity1 exists...");
            SampleRTableEntity entity1 = this.rtableWrapper.ReadEntity(jobType, jobId);
            if (entity1 == null)
            {
                SampleRTableEntity createEntity1 = new SampleRTableEntity(jobType, jobId, referenceMessage);
                this.rtableWrapper.InsertRow(createEntity1);
                entity1 = this.rtableWrapper.ReadEntity(jobType, jobId);
            }
            else
            {
                // entity1 already exists. Save the value of its Message for later use
                referenceMessage = entity1.Message;
            }

            Assert.IsNotNull(entity1, "entity1 is null UNEXPECTEDLY.");
            Console.WriteLine("entity1:\n{0}", entity1.ToString());
            Assert.IsTrue(entity1.JobType == jobType, "entity.JobType is incorrect.");
            Assert.IsTrue(entity1.JobId == jobId, "entity.JobId is incorrect.");

            originalEntity = entity1.Clone();

            // Read from Storage Accounts directly for debugging. Check for consistency of different accounts.
            this.ReadFromIndividualAccountsDirectly(jobType, jobId, true);

            this.RunHttpManglerBehaviorHelper(
                entity1,
                behaviors,
                targetApi,
                targetApiExpectedToFail,
                out httpManglerStartTime);

            return originalEntity;
        }


        /// <summary>
        /// Helper function to set up an original entity and then run the specified HttpMangler Behavior. 
        /// It will return the original entity created.
        /// Step (1): Make sure "entity1" with the specified partitionKey and rowKey exists. If not create it. Read "entity1".
        /// Step (2): Read "entity1" from individual storage tables directly for debugging.
        /// Step (3): Turn on HttpMangler and the specified behavior and call the specified RTable API to operate on "entity1". 
        /// This function accepts a targetApi with this form: Func<string, string, SampleRTableEntity> targetApi
        /// </summary>
        /// <param name="entityPartitionKey"></param>
        /// <param name="entityRowKey"></param>
        /// <param name="behaviors"></param>
        /// <param name="targetApi"></param>
        /// <param name="targetApiExpectedToFail"></param>
        /// <returns></returns>
        private SampleRTableEntity SetupAndRunHttpManglerBehaviorHelper(
            string entityPartitionKey,
            string entityRowKey,
            ProxyBehavior[] behaviors,
            Func<string, string, SampleRTableEntity> targetApi,
            bool targetApiExpectedToFail)
        {
            DateTime httpManglerStartTime;
            SampleRTableEntity originalEntity = new SampleRTableEntity();

            // "entity1":
            string jobType = entityPartitionKey;
            string jobId = entityRowKey;
            string referenceMessage = SampleRTableEntity.GenerateRandomMessage();

            //
            // Arrange
            //            
            // Make sure an entity exists with the specified partitionKey and rowKey    
            Console.WriteLine("\nMaking sure entity1 exists...");
            SampleRTableEntity entity1 = this.rtableWrapper.ReadEntity(jobType, jobId);
            if (entity1 == null)
            {
                SampleRTableEntity createEntity1 = new SampleRTableEntity(jobType, jobId, referenceMessage);
                this.rtableWrapper.InsertRow(createEntity1);
                entity1 = this.rtableWrapper.ReadEntity(jobType, jobId);
            }
            else
            {
                // entity1 already exists. Save the value of its Message for later use
                referenceMessage = entity1.Message;
            }

            Assert.IsNotNull(entity1, "entity1 is null UNEXPECTEDLY.");
            Console.WriteLine("entity1:\n{0}", entity1.ToString());
            Assert.IsTrue(entity1.JobType == jobType, "entity.JobType is incorrect.");
            Assert.IsTrue(entity1.JobId == jobId, "entity.JobId is incorrect.");

            originalEntity = entity1.Clone();

            // Read from Storage Accounts directly for debugging. Check for consistency of different accounts.
            this.ReadFromIndividualAccountsDirectly(jobType, jobId, true);

            this.RunHttpManglerBehaviorHelper(
                entity1,
                behaviors,
                targetApi,
                targetApiExpectedToFail,
                out httpManglerStartTime);

            return originalEntity;
        }

        /// <summary>
        /// Run the specified HttpMangler proxy behaviors for the specified Api.
        /// </summary>
        /// <param name="originalEntity"></param>
        /// <param name="behaviors"></param>
        /// <param name="targetApi"></param>
        /// <param name="targetApiExpectedToFail"></param>
        /// <param name="httpManglerStartTime"></param>
        protected void RunHttpManglerBehaviorHelper(
            SampleRTableEntity originalEntity,
            ProxyBehavior[] behaviors,
            TargetRTableWrapperApi<SampleRTableEntity> targetApi,
            bool targetApiExpectedToFail,
            out DateTime httpManglerStartTime)
        {          
            //
            // Act
            //
            // Call targetApi(entity1) and tamper the request
            httpManglerStartTime = DateTime.UtcNow;

            Console.WriteLine("\nRunHttpManglerBehaviorHelper(): Call targetApi(entity1) with HttpMangler enabled...");
            originalEntity.Message = SampleRTableEntity.GenerateRandomMessage();
            bool abortTest = false;
            try
            {
                using (HttpMangler proxy = new HttpMangler(false, behaviors))
                {
                    Console.WriteLine("Calling targetApi(entity1)");
                    targetApi(originalEntity);
                    if (targetApiExpectedToFail)
                    {
                        // if targetApi is expected to fail, and we are here, that means something is wrong.
                        abortTest = true;
                        throw new Exception("RunHttpManglerBehaviorHelper(): Should not reach here. HttpMangler allowed an targetApi() to go through UNEXPECTEDLY.");
                    }
                }
            }
            catch (Exception ex)
            {
                if (targetApiExpectedToFail == false)
                {
                    // if targetApi is NOT expected to fail, and we are here, that means something is wrong.
                    throw new Exception(
                        string.Format("RunHttpManglerBehaviorHelper(): targetApi() is NOT unexpected to throw an exception. Got this exception: {0}",
                        ex.ToString()));
                }

                if (abortTest)
                {
                    throw;
                }
                else
                {
                    Console.WriteLine("\nException is Expected. targetApi(entity1) threw an exception: {0}\n", ex.ToString());
                }
            }
        }


        /// <summary>
        /// Run the specified HttpMangler proxy behaviors for the specified Api, which takes this form:
        /// Func<string, string, SampleRTableEntity> targetApi
        /// </summary>
        /// <param name="originalEntity"></param>
        /// <param name="behaviors"></param>
        /// <param name="targetApi"></param>
        /// <param name="targetApiExpectedToFail"></param>
        /// <param name="httpManglerStartTime"></param>
        protected void RunHttpManglerBehaviorHelper(
            SampleRTableEntity originalEntity,
            ProxyBehavior[] behaviors,
            Func<string, string, SampleRTableEntity> targetApi,
            bool targetApiExpectedToFail,
            out DateTime httpManglerStartTime)
        {
            //
            // Act
            //
            // Call targetApi(entity1) and tamper the request
            httpManglerStartTime = DateTime.UtcNow;

            Console.WriteLine("\nRunHttpManglerBehaviorHelper(): Call targetApi(entity1) with HttpMangler enabled...");
            originalEntity.Message = SampleRTableEntity.GenerateRandomMessage();
            bool abortTest = false;
            try
            {
                using (HttpMangler proxy = new HttpMangler(false, behaviors))
                {
                    Console.WriteLine("Calling targetApi(entity1)");
                    targetApi(originalEntity.PartitionKey, originalEntity.RowKey);
                    if (targetApiExpectedToFail)
                    {
                        // if targetApi is expected to fail, and we are here, that means something is wrong.
                        abortTest = true;
                        throw new Exception("RunHttpManglerBehaviorHelper(): Should not reach here. HttpMangler allowed an targetApi() to go through UNEXPECTEDLY.");
                    }
                }
            }
            catch (Exception ex)
            {
                if (targetApiExpectedToFail == false)
                {
                    // if targetApi is NOT expected to fail, and we are here, that means something is wrong.
                    throw new Exception(
                        string.Format("RunHttpManglerBehaviorHelper(): targetApi() is NOT unexpected to throw an exception. Got this exception: {0}",
                        ex.ToString()));
                }

                if (abortTest)
                {
                    throw;
                }
                else
                {
                    Console.WriteLine("\nException is Expected. targetApi(entity1) threw an exception: {0}\n", ex.ToString());
                }
            }
        }


        /// <summary>
        /// Run the specified HttpMangler proxy behaviors for the specified batchOperation.
        /// </summary>
        /// <param name="batchOperation"></param>
        /// <param name="behaviors"></param>
        /// <param name="targetApiExpectedToFail"></param>
        /// <param name="httpManglerStartTime"></param>
        protected void RunHttpManglerBehaviorHelper(
            TableBatchOperation batchOperation,
            ProxyBehavior[] behaviors,
            bool targetApiExpectedToFail,
            out DateTime httpManglerStartTime)
        {
            //
            // Act
            //
            // Call targetApi(entity1) and tamper the request
            httpManglerStartTime = DateTime.UtcNow;

            Console.WriteLine("\nRunHttpManglerBehaviorHelper(): Call this.repTable.ExecuteBatch(batchOperation) with HttpMangler enabled...");            
            bool abortTest = false;
            try
            {
                using (HttpMangler proxy = new HttpMangler(false, behaviors))
                {
                    Console.WriteLine("Calling targetApi(entity1)");
                    this.repTable.ExecuteBatch(batchOperation);
                    if (targetApiExpectedToFail)
                    {
                        // if targetApi is expected to fail, and we are here, that means something is wrong.
                        abortTest = true;
                        throw new Exception("RunHttpManglerBehaviorHelper(): Should not reach here. HttpMangler allowed this.repTable.ExecuteBatch(batchOperation) to go through UNEXPECTEDLY.");
                    }
                }
            }
            catch (Exception ex)
            {
                if (targetApiExpectedToFail == false)
                {
                    // if targetApi is NOT expected to fail, and we are here, that means something is wrong.
                    throw new Exception(
                        string.Format("RunHttpManglerBehaviorHelper(): this.repTable.ExecuteBatch(batchOperation) is NOT unexpected to throw an exception. Got this exception: {0}",
                        ex.ToString()));
                }

                if (abortTest)
                {
                    throw;
                }
                else
                {
                    Console.WriteLine("\nException is Expected. this.repTable.ExecuteBatch(batchOperation) threw an exception: {0}\n", ex.ToString());
                }
            }

        }

        #endregion Private Helper functions    







        /////// <summary>
        /////// Run the specified HttpMangler proxy behaviors for the specified Api, which takes this form:
        /////// Func<string, string, SampleRTableEntity> targetApi
        /////// </summary>
        /////// <param name="originalEntity"></param>
        /////// <param name="behaviors"></param>
        /////// <param name="targetApi"></param>
        /////// <param name="targetApiExpectedToFail"></param>
        /////// <param name="httpManglerStartTime"></param>
        ////protected void RunHttpManglerBehaviorHelper(
        ////    SampleRTableEntity originalEntity,
        ////    ProxyBehavior[] behaviors,
        ////    Func<string, string, SampleRTableEntity> targetApi,
        ////    bool targetApiExpectedToFail,
        ////    out DateTime httpManglerStartTime)
        ////{
        ////    //
        ////    // Act
        ////    //
        ////    // Call targetApi(entity1) and tamper the request
        ////    httpManglerStartTime = DateTime.UtcNow;

        ////    Console.WriteLine("\nRunHttpManglerBehaviorHelper(): Call targetApi(entity1) with HttpMangler enabled...");
        ////    originalEntity.Message = SampleRTableEntity.GenerateRandomMessage();
        ////    bool abortTest = false;
        ////    try
        ////    {
        ////        using (HttpMangler proxy = new HttpMangler(false, behaviors))
        ////        {
        ////            Console.WriteLine("Calling targetApi(entity1)");
        ////            targetApi(originalEntity.PartitionKey, originalEntity.RowKey);
        ////            if (targetApiExpectedToFail)
        ////            {
        ////                // if targetApi is expected to fail, and we are here, that means something is wrong.
        ////                abortTest = true;
        ////                throw new Exception("RunHttpManglerBehaviorHelper(): Should not reach here. HttpMangler allowed an targetApi() to go through UNEXPECTEDLY.");
        ////            }
        ////        }
        ////    }
        ////    catch (Exception ex)
        ////    {
        ////        if (targetApiExpectedToFail == false)
        ////        {
        ////            // if targetApi is NOT expected to fail, and we are here, that means something is wrong.
        ////            throw new Exception(
        ////                string.Format("RunHttpManglerBehaviorHelper(): targetApi() is NOT unexpected to throw an exception. Got this exception: {0}",
        ////                ex.ToString()));
        ////        }

        ////        if (abortTest)
        ////        {
        ////            throw;
        ////        }
        ////        else
        ////        {
        ////            Console.WriteLine("\nException is Expected. targetApi(entity1) threw an exception: {0}\n", ex.ToString());
        ////        }
        ////    }
        ////}







    }
}
