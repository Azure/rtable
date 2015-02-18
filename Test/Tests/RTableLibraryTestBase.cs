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
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.Azure.Toolkit.Replication;
    using Microsoft.WindowsAzure.Storage.Table;
    using Microsoft.WindowsAzure.Storage.RTableTest;
    using System;
    using NUnit.Framework;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Xml.Serialization;
    
    public class RTableLibraryTestBase
    {
        /// <summary>
        /// Test configuration (e.g., storage account names / keys used in the unit tests) specified in an xml.
        /// </summary>
        protected RTableTestConfiguration rtableTestConfiguration;

        /// <summary>
        ///  The RTable under test.
        /// </summary>
        protected ReplicatedTable repTable;

        /// <summary>
        /// ConfigurationInfo used to construct the RTable.
        /// </summary>
        protected List<ConfigurationStoreLocationInfo> configurationInfos = new List<ConfigurationStoreLocationInfo>();

        /// <summary>
        /// ConfigurationService associated with the RTable.
        /// </summary>
        protected ReplicatedTableConfigurationService configurationService;

        /// <summary>
        /// Ues this to insert, modify, etc. entities in the RTable.
        /// </summary>
        protected RTableWrapperForSampleRTableEntity rtableWrapper;

        /// <summary>
        /// Cloud Table Client of the individual storage account used to construct the RTable. Used for debugging and validation only.
        /// </summary>
        protected List<CloudTableClient> cloudTableClients;

        /// <summary>
        /// Define how to construct the Rtable using the storage account names/keys specified in the xml file. 
        /// E.g., {0,1} means to use storage account #0 and #1 in the xml file to construct the RTable.
        /// </summary>
        protected List<int> actualStorageAccountsUsed;

        /// <summary>
        /// This is the index of the storage account where the RTable config is stored.
        /// </summary>
        protected int rtableConfigStorageAccountIndex = 0;

        /// <summary>
        /// Cloud Table of the individual storage account used to construct the RTable. Used for debugging and validation only.
        /// </summary>
        protected List<CloudTable> cloudTables;

        /// <summary>
        /// Lock timeout for tests
        /// </summary>
        private const int TestLockTimeoutInSeconds = 5;

        /// <summary>
        /// Read the test configuration from "RTableTestConfiguration.xml"
        /// </summary>
        protected void LoadTestConfiguration()
        {
            this.LoadTestConfiguration("RTableTestConfiguration.xml");
        }

        /// <summary>
        /// Read the test configuration from the specifieid filename
        /// </summary>
        /// <param name="filename"></param>
        protected void LoadTestConfiguration(string filename)
        {
            if (!File.Exists(filename))
            {
                throw new Exception(string.Format("LoadTestConfiguration(): file not found. {0}", filename));
            }
            else
            {
                Console.WriteLine("Loading RTableTestConfiguration from {0}", filename);
            }
            StreamReader reader = new StreamReader(filename);
            XmlSerializer xSerializer = new XmlSerializer(typeof(RTableTestConfiguration));
            this.rtableTestConfiguration = (RTableTestConfiguration)xSerializer.Deserialize(reader);

            if (this.rtableTestConfiguration.StorageInformation.AccountNames.Count() < 2)
            {
                throw new Exception(string.Format("Must specify at least two AccountNames in RTableTestConfiguration: {0}", filename));
            }
            if (this.rtableTestConfiguration.StorageInformation.AccountKeys.Count() < 2)
            {
                throw new Exception(string.Format("Must specify at least two AccountKeys in RTableTestConfiguration: {0}", filename));
            }
            if (this.rtableTestConfiguration.StorageInformation.AccountKeys.Count() != this.rtableTestConfiguration.StorageInformation.AccountNames.Count())
            {
                throw new Exception(string.Format("Must specify the same number of AccountNames and AccountKeys in RTableTestConfiguration: {0}", filename));
            }
        }

        /// <summary>
        /// Setup the RTable Env in order to run unit tests.
        /// </summary>
        /// <param name="reUploadRTableConfigBlob">set to true if want to re-upload a config blob to the container</param>        
        /// <param name="tableName">Name of the RTable. If string.Empty or null, then use this.rtableTestConfiguration.RTableInformation.RTableName</param>
        /// <param name="useHttps">When using HttpMangler, set this to false</param>
        /// <param name="viewIdString">_rtable_ViewId used in the RTable config blob. Set to string.Empty or null to use the _rtable_ViewId value in the xml config</param>
        /// <param name="actualStorageAccountsToBeUsed">Specify how/which Storage accounts in the config.xml will be used to construct the RTable. Set to null to use all accounts in xml. E.g., {0,1}</param>
        protected void SetupRTableEnv(
            bool reUploadRTableConfigBlob = true,
            string tableName = "",
            bool useHttps = true,
            string viewIdString = "",
            List<int> actualStorageAccountsToBeUsed = null,
            bool convertXStoreTableMode = false)
        {            
            int numberOfStorageAccounts = this.rtableTestConfiguration.StorageInformation.AccountNames.Count();
           
            // If user does not specify which storage accounts to construct the RTable, then use all the storage accounts in the xml.
            this.actualStorageAccountsUsed = actualStorageAccountsToBeUsed;
            if (this.actualStorageAccountsUsed == null || this.actualStorageAccountsUsed.Count == 0)
            {
                this.actualStorageAccountsUsed = new List<int>();
                for (int i = 0; i < numberOfStorageAccounts; i++)
                {
                    this.actualStorageAccountsUsed.Add(i);
                }
            }

            // Upload RTable configuration to blob
            int viewId = this.rtableTestConfiguration.RTableInformation.ViewId;
            if (!string.IsNullOrEmpty(viewIdString))
            {
                viewId = int.Parse(viewIdString);
            }

            this.configurationInfos = this.UploadRTableConfigToBlob(reUploadRTableConfigBlob, viewId, convertXStoreTableMode);
            this.configurationService =
                new ReplicatedTableConfigurationService(this.configurationInfos, useHttps);

            this.configurationService.LockTimeout = TimeSpan.FromSeconds(TestLockTimeoutInSeconds);

            if (string.IsNullOrEmpty(tableName))
            {
                tableName = this.rtableTestConfiguration.RTableInformation.RTableName;
            }
            this.repTable = new ReplicatedTable(tableName, this.configurationService);
            this.repTable.CreateIfNotExists();            
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);  

            this.cloudTableClients = new List<CloudTableClient>();
            this.cloudTables = new List<CloudTable>();
            for (int i = 0; i < this.actualStorageAccountsUsed.Count; i++)
            {
                CloudTableClient cloudBloblClient = repTable.GetReplicaTableClient(i);
                this.cloudTableClients.Add(cloudBloblClient);
                this.cloudTables.Add(cloudBloblClient.GetTableReference(repTable.TableName));
            }
        }

        /// <summary>
        /// Generate a random table name by appending a timestamp and Guid to the RTableName specified in the xml config.
        /// </summary>
        /// <returns></returns>
        protected string GenerateRandomTableName(string prefix = "")
        {
            string randomTableName = DateTime.UtcNow.ToString("yyyyMMddHHmmss") + Guid.NewGuid().ToString("N");
            if (string.IsNullOrEmpty(prefix))
            {
                randomTableName = this.rtableTestConfiguration.RTableInformation.RTableName + randomTableName;
            }
            else
            {
                randomTableName = prefix + randomTableName;
            }            
            if (randomTableName.Length > 64)
            {
                return randomTableName.Substring(0, 64);
            }
            else
            {
                return randomTableName;
            }
        }

        protected void DeleteAllRtableResources()
        {
            this.DeleteConfigurationBlob();
            this.configurationService.Dispose();
            this.repTable.DeleteIfExists();            
        }

        /// <summary>
        /// Call this function at the end of the tests to delete the RTable configuration blob
        /// </summary>
        protected void DeleteConfigurationBlob()
        {
            try
            {
                string blobName = this.configurationInfos.FirstOrDefault().BlobPath;
                int index = blobName.IndexOf("/");
                blobName = blobName.Substring(index + 1); // +1 to acording for "/"
                if (blobName.Equals(this.rtableTestConfiguration.RTableInformation.BlobName, StringComparison.InvariantCultureIgnoreCase))
                {
                    Console.WriteLine("Not deleting RTable configuration blob because its name is specified in xml");
                    return;
                }

                Console.WriteLine("Deleting configuration blob at {0}", blobName);
                index = this.rtableConfigStorageAccountIndex;
                string connectionString;
                CloudBlobClient cloudBloblClient = this.GenerateCloudBlobClient(
                                       this.rtableTestConfiguration.StorageInformation.AccountNames[index],
                                       this.rtableTestConfiguration.StorageInformation.AccountKeys[index],
                                       this.rtableTestConfiguration.StorageInformation.DomainName,
                                       out connectionString);

                CloudBlobContainer container = cloudBloblClient.GetContainerReference(this.rtableTestConfiguration.RTableInformation.ContainerName);
                CloudBlockBlob blockBlob = container.GetBlockBlobReference(blobName);
                blockBlob.Delete();
            }
            catch (Exception ex)
            {
                Console.WriteLine("DeleteConfigurationBlob() encountered exception {0}", ex.ToString());
            }
        }

        /// <summary>
        /// Call this function to modify the contents of the RTable configuration blob to use the specified viewId
        /// and convertXStoreTableMode and update the appropriate wrapper.
        /// </summary>
        /// <param name="updatedViewId"></param>
        protected void RefreshRTableEnvJsonConfigBlob(long updatedViewId, bool convertXStoreTableMode = false, int readViewHeadIndex = 0)
        {
            Console.WriteLine("Calling RefreshRTableEnvJsonConfigBlob() with updatedViewId={0} convertXStoreTableMode={1} readViewHeadIndex={2}",
                updatedViewId, convertXStoreTableMode, readViewHeadIndex);
            this.ModifyConfigurationBlob(updatedViewId, convertXStoreTableMode, readViewHeadIndex);
            this.repTable = new ReplicatedTable(this.repTable.TableName, this.configurationService);
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);

            Console.WriteLine("Sleeping {0} seconds for new viewId to take effect...", Constants.LeaseDurationInSec);
            Thread.Sleep(Constants.LeaseDurationInSec * 1000);
        }

        /// <summary>
        /// Call this function to modify the contents of the RTable configuration blob to use the specified convertXStoreTableMode
        /// and update the appropriate wrapper.
        /// </summary>
        /// <param name="convertXStoreTableMode"></param>
        protected void RefreshRTableEnvJsonConfigBlob(bool convertXStoreTableMode)
        {
            long viewId = this.configurationService.GetReadView().ViewId;
            this.RefreshRTableEnvJsonConfigBlob(viewId, convertXStoreTableMode);            
        }

        /// <summary>
        /// Modify the contents of the RTable configuration blob to use the updated viewId
        /// </summary>
        /// <param name="updatedViewId"></param>
        /// <param name="convertXStoreTableMode"></param>
        /// <param name="readViewHeadIndex"></param>
        private void ModifyConfigurationBlob(long updatedViewId, bool convertXStoreTableMode, int readViewHeadIndex)
        {
            try
            {
                string blobName = this.configurationInfos.FirstOrDefault().BlobPath;
                int index = blobName.IndexOf("/");
                blobName = blobName.Substring(index + 1); // +1 to acording for "/"

                Console.WriteLine("Configuration blobname = {0}", blobName);
                index = this.rtableConfigStorageAccountIndex;
                string connectionString;
                CloudBlobClient cloudBloblClient = this.GenerateCloudBlobClient(
                                       this.rtableTestConfiguration.StorageInformation.AccountNames[index],
                                       this.rtableTestConfiguration.StorageInformation.AccountKeys[index],
                                       this.rtableTestConfiguration.StorageInformation.DomainName,
                                       out connectionString);

                CloudBlobContainer container = cloudBloblClient.GetContainerReference(this.rtableTestConfiguration.RTableInformation.ContainerName);
                CloudBlockBlob blockBlob = container.GetBlockBlobReference(blobName);
                string currentConfigText = blockBlob.DownloadText();
                Console.WriteLine("currentConfigText = {0}", currentConfigText);

                string updatedConfigText = this.GetRTableJsonConfiguration(updatedViewId, convertXStoreTableMode, readViewHeadIndex);
                Console.WriteLine("updatedConfigText = {0}", updatedConfigText);
                blockBlob.UploadText(updatedConfigText);
            }
            catch (Exception ex)
            {
                Console.WriteLine("ModifyConfigurationBlob() encountered exception {0}", ex.ToString());
                throw;
            }
        }

        /// <summary>
        /// Create the contents of the RTable configuration, and upload it to the appropriate container and blob.
        /// Return ConfigurationStoreLocationInfo.
        /// </summary>
        /// <param name="reUploadRTableConfigBlob"></param>
        /// <param name="viewId"></param>
        private List<ConfigurationStoreLocationInfo> UploadRTableConfigToBlob(
            bool reUploadRTableConfigBlob, 
            int viewId, 
            bool convertXStoreTableMode = false)
        {
            string connectionString;

            // Currently, RTable config is only uploaded to ONE storage account's blob only.
            // The first storage account is given by this.actualStorageAccountsUsed[0].
            // Create a CloudBlobClient and upload the config there.
            int index = this.rtableConfigStorageAccountIndex;
            CloudBlobClient cloudBloblClient = this.GenerateCloudBlobClient(
                                   this.rtableTestConfiguration.StorageInformation.AccountNames[index],
                                   this.rtableTestConfiguration.StorageInformation.AccountKeys[index],
                                   this.rtableTestConfiguration.StorageInformation.DomainName,
                                   out connectionString);

            // e.g., Convert "config.txt" into "config-2014-09-01-23-55-59-123.txt"
            string blobName = this.rtableTestConfiguration.RTableInformation.BlobName; 
            index = blobName.IndexOf(".");
            string dateString = DateTime.UtcNow.ToString("yyyy-MM-dd-HH-mm-ss-fff");
            if (index >= 0)
            {
                blobName = blobName.Substring(0, index) + "-" + dateString + blobName.Substring(index);
            }
            else
            {
                blobName = blobName + "-" + dateString;
            }

            CloudBlobContainer container = cloudBloblClient.GetContainerReference(this.rtableTestConfiguration.RTableInformation.ContainerName);            
            bool justCreatedContainer = container.CreateIfNotExists();
            if (justCreatedContainer || reUploadRTableConfigBlob)
            {
                // If it is a new container, then upload the RTable config
                // Or, if the user specifies re-upload, then upload the RTable config
                CloudBlockBlob blockBlob = container.GetBlockBlobReference(blobName);
                Console.WriteLine("Uploading RTable config because we just created a new container in Head storage account or user specifies re-upload");
                string jsonConfigText = this.GetRTableJsonConfiguration(viewId, convertXStoreTableMode);
                blockBlob.UploadText(jsonConfigText);
            }

            string configFilePath = string.Format(@"{0}/{1}",
                this.rtableTestConfiguration.RTableInformation.ContainerName,
                blobName);

            Console.WriteLine("RTable configuration blob location is: {0}", configFilePath);

            ConfigurationStoreLocationInfo configurationStoreLocationInfo = new ConfigurationStoreLocationInfo(connectionString);
            configurationStoreLocationInfo.BlobPath = configFilePath;
            return new List<ConfigurationStoreLocationInfo>(){configurationStoreLocationInfo};
        }

        /// <summary>
        /// Return the connection string given the specified accountName, accountKey and domainName
        /// </summary>
        /// <param name="accountName"></param>
        /// <param name="accountKey"></param>
        /// <param name="domainName"></param>
        /// <returns></returns>
        protected string GetConnectionString(string accountName, string accountKey, string domainName)
        {
            return string.Format(@"DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1};BlobEndpoint=https://{0}.blob.{2};QueueEndpoint=https://{0}.queue.{2};TableEndpoint=https://{0}.table.{2}",
                accountName,
                accountKey,
                domainName);
        }

        /// <summary>
        /// Create a CloudBlobClient for the specified accountName, accountKey, and domainName.
        /// </summary>
        /// <param name="accountName"></param>
        /// <param name="accountKey"></param>
        /// <param name="domainName"></param>
        /// <param name="connectionString">(Output)</param>
        /// <returns></returns>
        private CloudBlobClient GenerateCloudBlobClient(
            string accountName,
            string accountKey,
            string domainName,
            out string connectionString)
        {
            connectionString = GetConnectionString(accountName, accountKey, domainName);
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            CloudBlobClient client = storageAccount.CreateCloudBlobClient();
            return client;
        }
        
        /// <summary>
        /// Helper function to return the RTableConfig as a string. 
        /// Note that even if 3 (say) accounts are specified in the xml, 
        /// the test codes can specify that only accounts #0 and #2 are used to construct the RTable.
        /// </summary>
        /// <param name="viewId"></param>
        /// <param name="convertXStoreTableMode"></param>
        /// <param name="readViewHeadIndex"></param>
        /// <returns></returns>
        private string GetRTableJsonConfiguration(long viewId, bool convertXStoreTableMode, int readViewHeadIndex = 0)
        {
            if (viewId <= 0)
            {
                throw new Exception(string.Format("GetRTableConfigText() was called with invalid viewId {0}", viewId));
            }

            ReplicatedTableConfigurationStore configuration = new ReplicatedTableConfigurationStore();

            configuration.ViewId = viewId;
            configuration.ReadViewHeadIndex = readViewHeadIndex;
            configuration.ConvertXStoreTableMode = convertXStoreTableMode;

            int numberOfStorageAccounts = this.rtableTestConfiguration.StorageInformation.AccountNames.Count();
            for (int i = 0; i < this.actualStorageAccountsUsed.Count; i++)
            {
                int index = this.actualStorageAccountsUsed[i];
                if (index < 0 || index > numberOfStorageAccounts)
                {
                    throw new Exception(string.Format("this.actualStorageAccountsUsed[{0}] = {1} is out of range.", i, index));
                }

                ReplicaInfo replica = new ReplicaInfo();
                replica.StorageAccountName = this.rtableTestConfiguration.StorageInformation.AccountNames[index];
                replica.StorageAccountKey = this.rtableTestConfiguration.StorageInformation.AccountKeys[index];

                if (readViewHeadIndex != 0)
                {
                    //If the read view head index is not 0, this means we are introducing 1 or more replicas at the head. For 
                    //each such replica, update the view id in which it was added to the write view of the chain
                    if (i < readViewHeadIndex)
                    {
                        replica.ViewInWhichAddedToChain = viewId;
                    }
                }

                configuration.ReplicaChain.Add(replica);
            }

            return JsonStore<ReplicatedTableConfigurationStore>.Serialize(configuration);
        }

        /// <summary>
        /// Read and print the entity of the specified jobType and jobId from individual storage accounts for debugging.
        /// </summary>
        /// <param name="jobType"></param>
        /// <param name="jobId"></param>
        /// <param name="checkIndividualAccounts">True means will check the entity in each storage account for consistency</param>
        protected void ReadFromIndividualAccountsDirectly(string partitionKey, string rowKey, bool checkIndividualAccounts = false)
        {
            Console.WriteLine("\nBEGIN ReadFromIndividualAccountsDirectly()...");

            partitionKey = partitionKey.ToLower().Replace(" ", "");
            rowKey = rowKey.ToLower().Replace(" ", "");

            string filter1 = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey);
            string filter2 = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey);

            TableQuery<SampleRTableEntity> query = new TableQuery<SampleRTableEntity>().
                Where(TableQuery.CombineFilters(filter1, TableOperators.And, filter2));

            SampleRTableEntity[] entities = new SampleRTableEntity[configurationService.GetWriteView().Chain.Count];
            for (int i = 0; i < configurationService.GetWriteView().Chain.Count; i++)
            {
                Console.WriteLine("Executing query for CloudTable #{0}", i);
                foreach (var item in ((CloudTableClient) configurationService.GetWriteView()[i]).GetTableReference(repTable.TableName).ExecuteQuery(query))
                {
                    Console.WriteLine("{0}", item.ToString());
                    entities[i] = item;
                }
            }

            if (checkIndividualAccounts)
            {
                Console.WriteLine("Checking for consistency...");
                for (int i = 1; i < configurationService.GetWriteView().Chain.Count; i++)
                {
                    Assert.IsTrue((entities[0] == null && entities[i] == null) || (entities[0]  != null && entities[0].Equals(entities[i])),
                        "Entities in storage accounts: #0 and #{0} do NOT match", i);
                }
                Console.WriteLine("Entities in different accounts are consistent.");
            }

            Console.WriteLine("END ReadFromIndividualAccountsDirectly()\n");
        }

        /// <summary>
        /// Performs checks against the Rtable invariants
        /// </summary>
        /// <param name="partitionKey">jobType</param>
        /// <param name="headReplicaAccountIndex">headReplicaAccountIndex</param>
        /// <param name="tailReplicaAccountIndex">tailReplicaAccountIndex</param>
        protected void PerformInvariantChecks(string partitionKey, int headReplicaAccountIndex, int tailReplicaAccountIndex)
        {
            Console.WriteLine("\nBEGIN PerformInvariantChecks()...");

            partitionKey = partitionKey.ToLower().Replace(" ", "");
            
            TableQuery<SampleRTableEntity> query = new TableQuery<SampleRTableEntity>().
                                                   Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));

            //Key = RowKey, Value = Entity
            var headEntities = configurationService.GetWriteView()[headReplicaAccountIndex].GetTableReference(repTable.TableName).ExecuteQuery(query).ToDictionary(e => e.RowKey, e => e);

            //Key = RowKey, Value = Entity
            var tailEntities = configurationService.GetWriteView()[tailReplicaAccountIndex].GetTableReference(repTable.TableName).ExecuteQuery(query).ToDictionary(e => e.RowKey, e => e);

            Assert.IsTrue(headEntities.Count >= tailEntities.Count);

            foreach (var headEntity in headEntities)
            {
                //Check if the same row exists in the tail replica
                //If not, the entity must be locked
                if (!tailEntities.ContainsKey(headEntity.Key))
                {
                    Assert.IsTrue(headEntity.Value._rtable_RowLock);
                }
                else
                {
                    //If an entity exists on both head and tail, their content should be the same
                    Assert.AreEqual(headEntity.Value, tailEntities[headEntity.Key]);
                }

                //For each entity on head, version on head >= version on tail
                Assert.IsTrue(headEntity.Value._rtable_Version >= tailEntities[headEntity.Key]._rtable_Version);
            }

            //Every entity on tail must be present on the head
            foreach (var tailEntity in tailEntities)
            {
                Assert.IsTrue(headEntities.ContainsKey(tailEntity.Key));
                Assert.AreEqual(tailEntity.Value, headEntities[tailEntity.Key]);
            }

            Console.WriteLine("END PerformInvariantChecks()\n");
        }
    }
}
