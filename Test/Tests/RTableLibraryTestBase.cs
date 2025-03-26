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


#if true
    #define USE_CONFIGSERVICE_V2
#else
    #define USE_CONFIGSERVICE_V1
#endif

namespace Microsoft.Azure.Toolkit.Replication.Test
{
    using Microsoft.Azure.Toolkit.Replication;
    using Microsoft.WindowsAzure.Storage.RTableTest;
    using System;
    using NUnit.Framework;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Xml.Serialization;
    using System.Security;
    using global::Azure.Data.Tables;
    using global::Azure.Identity;
    using global::Azure.Storage.Blobs;
    using global::Azure.Core;

    public class RTableLibraryTestBase
    {
        /// <summary>
        /// Configuration wrapper (layer) which interacts with the RTable ConfigurationService instance.
        /// </summary>
        protected IReplicatedTableConfigurationWrapper configurationWrapper;

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
        /// Ues this to insert, modify, etc. entities in the RTable.
        /// </summary>
        protected RTableWrapperForSampleRTableEntity rtableWrapper;

        /// <summary>
        /// Cloud Table Client of the individual storage account used to construct the RTable. Used for debugging and validation only.
        /// </summary>
        protected List<TableServiceClient> cloudTableClients;

        /// <summary>
        /// Define how to construct the Rtable using the storage account names/keys specified in the xml file. 
        /// E.g., {0,1} means to use storage account #0 and #1 in the xml file to construct the RTable.
        /// </summary>
        protected List<int> actualStorageAccountsUsed;

        /// <summary>
        /// Cloud Table of the individual storage account used to construct the RTable. Used for debugging and validation only.
        /// </summary>
        protected List<TableClient> cloudTables;

        /// <summary>
        /// Lock timeout for tests
        /// </summary>
        private const int TestLockTimeoutInSeconds = 5;

        /// <summary>
        /// Mapping from account name -> connection string
        /// </summary>
        protected Dictionary<string, SecureString> connectionStringMap;

        private int leaseDuration = 0;
        private int clockFactor = 0;
        private int minimumLeaseRenewalInterval = 0;
        private int lockTimeoutInSeconds = 0;

        /// <summary>
        /// Used for token-based authentication testing to storage.
        /// Cache the token credential from interactive login to avoid repeated prompts.
        /// </summary>
        private static TokenCredential s_interactiveLoginTokenCredential = null;
        private static object _loginLock = new object();
        private TokenCredential InteractiveLoginTokenCredential
        {
            get
            {
                if (s_interactiveLoginTokenCredential == null)
                {
                    lock (_loginLock)
                    {
                        if (s_interactiveLoginTokenCredential == null)
                        {
                            Guid tenantId = this.rtableTestConfiguration.RunnerInformation.InteractiveLoginTenantId;
                            if (tenantId == Guid.Empty)
                            {
                                s_interactiveLoginTokenCredential = new InteractiveBrowserCredential();
                            }
                            else
                            {
                                s_interactiveLoginTokenCredential = new InteractiveBrowserCredential(
                                    new InteractiveBrowserCredentialOptions()
                                    {
                                        TenantId = tenantId.ToString()
                                    });
                            }
                        }
                    }
                }

                return s_interactiveLoginTokenCredential;
            }
        }

        protected RTableLibraryTestBase()
        {
            // Enable/Use default Logger ...
            ReplicatedTableEtwLogger.EnableEvents();
            ReplicatedTableLogger.Subscribe(new ReplicatedTableEtwLogger());
        }

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
        /// <param name="tableName">Name of the RTable. If string.Empty or null, then use this.rtableTestConfiguration.RTableInformation.RTableName</param>
        /// <param name="useHttps">When using HttpMangler, set this to false</param>
        /// <param name="viewIdString">_rtable_ViewId used in the RTable config blob. Set to string.Empty or null to use the _rtable_ViewId value in the xml config</param>
        /// <param name="actualStorageAccountsToBeUsed">Specify how/which Storage accounts in the config.xml will be used to construct the RTable. Set to null to use all accounts in xml. E.g., {0,1}</param>
        protected void SetupRTableEnv(
            string tableName = "",
            bool useHttps = true,
            string viewIdString = "",
            List<int> actualStorageAccountsToBeUsed = null,
            bool convertXStoreTableMode = false,
            int numberOfBlobs = 0)
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

            this.UpdateConfigurationConstants();

            // Upload RTable configuration to blob
            int viewId = this.rtableTestConfiguration.RTableInformation.ViewId;
            if (!string.IsNullOrEmpty(viewIdString))
            {
                viewId = int.Parse(viewIdString);
            }

            numberOfBlobs = numberOfBlobs == 0
                ? this.rtableTestConfiguration.StorageInformation.AccountKeys.Count()
                : numberOfBlobs;

            if (string.IsNullOrEmpty(tableName))
            {
                tableName = this.rtableTestConfiguration.RTableInformation.RTableName;
            }

            this.InitConnectionStringMap(useHttps);

            InitialSetup(tableName, useHttps, viewId, convertXStoreTableMode, numberOfBlobs);

            this.repTable = new ReplicatedTable(tableName, this.configurationService);
            this.repTable.CreateIfNotExists();
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);

            this.cloudTableClients = new List<TableServiceClient>();
            this.cloudTables = new List<TableClient>();
            for (int i = 0; i < this.actualStorageAccountsUsed.Count; i++)
            {
                TableServiceClient cloudBloblClient = repTable.GetReplicaTableClient(i);
                this.cloudTableClients.Add(cloudBloblClient);
                this.cloudTables.Add(cloudBloblClient.GetTableClient(repTable.TableName));
            }
        }

        private void UpdateConfigurationConstants()
        {
            this.leaseDuration = Constants.LeaseDurationInSec;
            this.clockFactor = Constants.ClockFactorInSec;
            this.minimumLeaseRenewalInterval = Constants.MinimumLeaseRenewalInterval;
            this.lockTimeoutInSeconds = Constants.LockTimeoutInSeconds;

            Constants.LeaseDurationInSec = 5;
            Constants.ClockFactorInSec = 1;
            Constants.MinimumLeaseRenewalInterval = 1;
            Constants.LockTimeoutInSeconds = 5;
        }

        private void RestoreConfigurationConstants()
        {
            Constants.LeaseDurationInSec = this.leaseDuration;
            Constants.ClockFactorInSec = this.clockFactor;
            Constants.MinimumLeaseRenewalInterval = this.minimumLeaseRenewalInterval;
            Constants.LockTimeoutInSeconds = this.lockTimeoutInSeconds;
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
            this.repTable.DeleteIfExists();
            this.DeleteConfigurationBlob();
            this.configurationService.Dispose();
            this.RestoreConfigurationConstants();

            // Disable/Dispose default Logger
            ReplicatedTableEtwLogger.DisableEvents();
            ReplicatedTableEtwLogger.DisposeLogger();
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
                for (index = 0;
                    index < this.rtableTestConfiguration.StorageInformation.AccountKeys.Count();
                    index++)
                {
                    string connectionString;
                    BlobServiceClient cloudBloblClient = this.GenerateBlobServiceClient(
                        this.rtableTestConfiguration.StorageInformation.AccountNames[index],
                        this.rtableTestConfiguration.StorageInformation.AccountKeys[index],
                        this.rtableTestConfiguration.StorageInformation.DomainName,
                        out connectionString);

                    BlobContainerClient container =
                        cloudBloblClient.GetBlobContainerClient(
                            this.rtableTestConfiguration.RTableInformation.ContainerName);
                    BlobClient blockBlob = container.GetBlobClient(blobName);
                    blockBlob.Delete();
                }
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
        protected void RefreshRTableEnvJsonConfigBlob(long updatedViewId, bool convertXStoreTableMode = false,
            int readViewHeadIndex = 0, List<int> blobIndexes = null, bool waitForConfig = true)
        {
            if (blobIndexes == null)
            {
                blobIndexes = new List<int> { 0 };
            }

            Console.WriteLine(
                "Calling RefreshRTableEnvJsonConfigBlob() with updatedViewId={0} convertXStoreTableMode={1} readViewHeadIndex={2}",
                updatedViewId, convertXStoreTableMode, readViewHeadIndex);
            this.ModifyConfigurationBlob(updatedViewId, convertXStoreTableMode, readViewHeadIndex, blobIndexes);
            this.repTable = new ReplicatedTable(this.repTable.TableName, this.configurationService);
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);

            if (!waitForConfig)
            {
                return;
            }

            while (this.configurationWrapper.GetWriteView().ViewId != updatedViewId)
            {
                Console.WriteLine("Sleeping {0} seconds for new viewId to take effect...",
                    Constants.LeaseDurationInSec/2);
                Thread.Sleep((Constants.LeaseDurationInSec/2)*1000);
            }
        }

        /// <summary>
        /// Call this function to modify the contents of the RTable configuration blob to use the specified convertXStoreTableMode
        /// and update the appropriate wrapper.
        /// </summary>
        /// <param name="convertXStoreTableMode"></param>
        protected void RefreshRTableEnvJsonConfigBlob(bool convertXStoreTableMode)
        {
            long viewId = this.configurationWrapper.GetReadView().ViewId;
            this.RefreshRTableEnvJsonConfigBlob(++viewId, convertXStoreTableMode, 0, new List<int> { 0, 1, 2});
        }

        /// <summary>
        /// Return RTableConfiguration LocationInfo.
        /// </summary>
        private List<ConfigurationStoreLocationInfo> GetRTableConfigurationLocationInfo(int numberOfBlobs)
        {
            // e.g., Convert "config.txt" into "config-2014-09-01-23-55-59-123.txt"
            string blobName = this.rtableTestConfiguration.RTableInformation.BlobName;
            string dateString = DateTime.UtcNow.ToString("yyyy-MM-dd-HH-mm-ss-fff");

            int index = blobName.IndexOf(".");
            if (index >= 0)
            {
                blobName = blobName.Substring(0, index) + "-" + dateString + blobName.Substring(index);
            }
            else
            {
                blobName = blobName + "-" + dateString;
            }

            string configFilePath = string.Format(@"{0}/{1}", this.rtableTestConfiguration.RTableInformation.ContainerName, blobName);

            Console.WriteLine("RTable configuration blob location is: {0}", configFilePath);

            var locationInfos = new List<ConfigurationStoreLocationInfo>();
            for (int blobIndex = 0; blobIndex < numberOfBlobs; blobIndex++)
            {
                string connectionString = this.GetConnectionString(
                                                    this.rtableTestConfiguration.StorageInformation.AccountNames[blobIndex],
                                                    this.rtableTestConfiguration.StorageInformation.AccountKeys[blobIndex],
                                                    this.rtableTestConfiguration.StorageInformation.DomainName
                                                    );

                ConfigurationStoreLocationInfo configurationStoreLocationInfo = new ConfigurationStoreLocationInfo(connectionString)
                {
                    BlobPath = configFilePath,
                };

                locationInfos.Add(configurationStoreLocationInfo);
            }

            return locationInfos;
        }

        private void InitConnectionStringMap(bool useHttps)
        {
            this.connectionStringMap = new Dictionary<string, SecureString>();

            for (int i = 0; i < this.rtableTestConfiguration.StorageInformation.AccountNames.Count(); i++)
            {
                string connectionString = string.Format(Constants.LongConnectioStringTemplate,
                                                        useHttps ? "https" : "http",
                                                        this.rtableTestConfiguration.StorageInformation.AccountNames[i],
                                                        this.rtableTestConfiguration.StorageInformation.AccountKeys[i],
                                                        this.rtableTestConfiguration.StorageInformation.DomainName);

                this.connectionStringMap.Add(
                                            this.rtableTestConfiguration.StorageInformation.AccountNames[i],
                                            SecureStringHelper.ToSecureString(connectionString));
            }
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
        /// Create a BlobServiceClient for the specified accountName, accountKey, and domainName.
        /// </summary>
        /// <param name="accountName"></param>
        /// <param name="accountKey"></param>
        /// <param name="domainName"></param>
        /// <param name="connectionString">(Output)</param>
        /// <returns></returns>
        protected BlobServiceClient GenerateBlobServiceClient(
            string accountName,
            string accountKey,
            string domainName,
            out string connectionString)
        {
            connectionString = GetConnectionString(accountName, accountKey, domainName);
            BlobServiceClient client = new BlobServiceClient(connectionString); ;
            return client;
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

            SampleRTableEntity[] entities = new SampleRTableEntity[configurationWrapper.GetWriteView().Chain.Count];
            for (int i = 0; i < configurationWrapper.GetWriteView().Chain.Count; i++)
            {
                Console.WriteLine("Executing query for CloudTable #{0}", i);
                foreach (var item in configurationWrapper.GetWriteView()[i].GetTableClient(repTable.TableName).Query<SampleRTableEntity>(e =>
                    e.PartitionKey == partitionKey &&
                    e.RowKey == rowKey))
                {
                    Console.WriteLine("{0}", item.ToString());
                    entities[i] = item;
                }
            }

            if (checkIndividualAccounts)
            {
                Console.WriteLine("Checking for consistency...");
                for (int i = 1; i < configurationWrapper.GetWriteView().Chain.Count; i++)
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
            
            //Key = RowKey, Value = Entity
            var headEntities = configurationWrapper.GetWriteView()[headReplicaAccountIndex].GetTableClient(repTable.TableName).Query<SampleRTableEntity>(e => e.PartitionKey == partitionKey).ToDictionary(e => e.RowKey, e => e);

            //Key = RowKey, Value = Entity
            var tailEntities = configurationWrapper.GetWriteView()[tailReplicaAccountIndex].GetTableClient(repTable.TableName).Query<SampleRTableEntity>(e => e.PartitionKey == partitionKey).ToDictionary(e => e.RowKey, e => e);

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


        #region ReplicatedTableConfigurationService class related helpers/wrappers

#if USE_CONFIGSERVICE_V1

        /// <summary>
        /// ConfigurationService associated with the RTable.
        /// </summary>
        protected ReplicatedTableConfigurationService configurationService;

        private void InitialSetup(string tableName, bool useHttps, int viewId, bool convertXStoreTableMode, int numberOfBlobs)
        {
            this.configurationInfos = this.GetRTableConfigurationLocationInfo(numberOfBlobs);

            this.UploadRTableConfigToBlob(viewId, convertXStoreTableMode, numberOfBlobs);

            this.configurationService = new ReplicatedTableConfigurationService(this.configurationInfos, this.connectionStringMap, useHttps);

            this.configurationWrapper = new ReplicatedTableConfigurationWrapper(this.configurationService);
            this.configurationWrapper.SetLockTimeout(TimeSpan.FromSeconds(TestLockTimeoutInSeconds));
        }

        /// <summary>
        /// Helper function to return the RTableConfig
        /// Note that even if 3 (say) accounts are specified in the xml,
        /// the test codes can specify that only accounts #0 and #2 are used to construct the RTable.
        /// </summary>
        /// <param name="viewId"></param>
        /// <param name="convertXStoreTableMode"></param>
        /// <param name="readViewHeadIndex"></param>
        /// <returns></returns>
        private ReplicatedTableConfigurationStore GetRTableConfiguration(long viewId, bool convertXStoreTableMode, int readViewHeadIndex = 0)
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

            return configuration;
        }

        /// <summary>
        /// Create the contents of the RTable configuration, and upload it to the appropriate container and blob.
        /// </summary>
        private void UploadRTableConfigToBlob(int viewId, bool convertXStoreTableMode, int numberOfBlobs)
        {
            string blobName = this.configurationInfos.FirstOrDefault().BlobPath;
            int index = blobName.IndexOf("/");
            blobName = blobName.Substring(index + 1); // +1 to acording for "/"

            Console.WriteLine("Uploading RTable config ...");

            for (int blobIndex = 0; blobIndex < numberOfBlobs; blobIndex++)
            {
                string connectionString;
                CloudBlobClient cloudBloblClient = this.GenerateCloudBlobClient(
                    this.rtableTestConfiguration.StorageInformation.AccountNames[blobIndex],
                    this.rtableTestConfiguration.StorageInformation.AccountKeys[blobIndex],
                    this.rtableTestConfiguration.StorageInformation.DomainName,
                    out connectionString);

                CloudBlobContainer container = cloudBloblClient.GetContainerReference(this.rtableTestConfiguration.RTableInformation.ContainerName);
                container.CreateIfNotExists();

                CloudBlockBlob blockBlob = container.GetBlockBlobReference(blobName);

                ReplicatedTableConfigurationStore configuration = this.GetRTableConfiguration(viewId, convertXStoreTableMode);

                string jsonConfigText = JsonStore<ReplicatedTableConfigurationStore>.Serialize(configuration);
                blockBlob.UploadText(jsonConfigText);
            }
        }

        /// <summary>
        /// Modify the contents of the RTable configuration blob to use the updated viewId
        /// </summary>
        /// <param name="updatedViewId"></param>
        /// <param name="convertXStoreTableMode"></param>
        /// <param name="readViewHeadIndex"></param>
        private void ModifyConfigurationBlob(long updatedViewId, bool convertXStoreTableMode, int readViewHeadIndex, List<int> blobIndexes)
        {
            try
            {
                foreach (var blobIndex in blobIndexes)
                {
                    Console.WriteLine("ModifyConfigurationBlob#{0} ...", blobIndex);

                    string blobName = this.configurationInfos[blobIndex].BlobPath;
                    int index = blobName.IndexOf("/");
                    blobName = blobName.Substring(index + 1); // +1 to acording for "/"

                    Console.WriteLine("Configuration blobname = {0}", blobName);

                    string connectionString;
                    CloudBlobClient cloudBloblClient = this.GenerateCloudBlobClient(
                        this.rtableTestConfiguration.StorageInformation.AccountNames[blobIndex],
                        this.rtableTestConfiguration.StorageInformation.AccountKeys[blobIndex],
                        this.rtableTestConfiguration.StorageInformation.DomainName,
                        out connectionString);

                    CloudBlobContainer container =
                        cloudBloblClient.GetContainerReference(
                            this.rtableTestConfiguration.RTableInformation.ContainerName);
                    CloudBlockBlob blockBlob = container.GetBlockBlobReference(blobName);

                    string currentConfigText = blockBlob.DownloadText();
                    Console.WriteLine("currentConfigText = {0}", currentConfigText);

                    ReplicatedTableConfigurationStore configuration = this.GetRTableConfiguration(updatedViewId,
                        convertXStoreTableMode, readViewHeadIndex);

                    string updatedConfigText = JsonStore<ReplicatedTableConfigurationStore>.Serialize(configuration);
                    Console.WriteLine("updatedConfigText = {0}", updatedConfigText);

                    blockBlob.UploadText(updatedConfigText);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ModifyConfigurationBlob() encountered exception {0}", ex.ToString());
                throw;
            }
        }

        protected void UpdateConfiguration(List<ReplicaInfo> replicaChain, int readViewHeadIndex, bool convertXStoreTableMode = false, long viewId = 0)
        {
            this.configurationService.UpdateConfiguration(replicaChain, readViewHeadIndex, convertXStoreTableMode, viewId);
        }

#endif

        #endregion


        #region ReplicatedTableConfigurationServiceV2 class related helpers/wrappers

#if USE_CONFIGSERVICE_V2

        /// <summary>
        /// ConfigurationService associated with the RTable.
        /// </summary>
        protected ReplicatedTableConfigurationServiceV2 configurationService;

        private readonly string DefaultViewName = "DefaultView";
        private readonly string DefaultTableConfigName = "DefaultTableConfig";

        private void InitialSetup(string tableName, bool useHttps, int viewId, bool convertXStoreTableMode, int numberOfBlobs)
        {
            this.configurationInfos = this.GetRTableConfigurationLocationInfo(numberOfBlobs);

            Guid tenantId = this.rtableTestConfiguration.RunnerInformation.InteractiveLoginTenantId;
            if (useHttps && tenantId != Guid.Empty)
            {
                // TokenCredentials can only be used with https. Requirement from Azure Storage.
                var replicaStorageTokenMap = new Dictionary<string, TokenCredential>();
                this.connectionStringMap.Keys.ToList().ForEach(accountName =>
                {
                    replicaStorageTokenMap[accountName] = this.InteractiveLoginTokenCredential;
                });
                this.configurationService = new ReplicatedTableConfigurationServiceV2(this.configurationInfos,
                    this.InteractiveLoginTokenCredential,
                    replicaStorageTokenMap,
                    useHttps,
                    "core.windows.net");
            }
            else
            {
                this.configurationService = new ReplicatedTableConfigurationServiceV2(this.configurationInfos, this.connectionStringMap, useHttps);
            }

            this.UploadRTableConfigToBlob(viewId, convertXStoreTableMode, numberOfBlobs);

            this.configurationWrapper = new ReplicatedTableConfigurationV2Wrapper(tableName, this.configurationService);
            this.configurationWrapper.SetLockTimeout(TimeSpan.FromSeconds(TestLockTimeoutInSeconds));
        }

        /// <summary>
        /// Helper function to return the RTableConfig
        /// Note that even if 3 (say) accounts are specified in the xml,
        /// the test codes can specify that only accounts #0 and #2 are used to construct the RTable.
        /// </summary>
        /// <param name="viewId"></param>
        /// <param name="convertXStoreTableMode"></param>
        /// <param name="readViewHeadIndex"></param>
        /// <returns></returns>
        private ReplicatedTableConfiguration GetRTableConfiguration(long viewId, bool convertXStoreTableMode, int readViewHeadIndex = 0)
        {
            if (viewId <= 0)
            {
                throw new Exception(string.Format("GetRTableConfigText() was called with invalid viewId {0}", viewId));
            }

            // 1 - Create a default view
            var viewConfig = new ReplicatedTableConfigurationStore
            {
                ViewId = viewId,
                ReadViewHeadIndex = readViewHeadIndex,
            };

            int numberOfStorageAccounts = this.rtableTestConfiguration.StorageInformation.AccountNames.Count();
            for (int i = 0; i < this.actualStorageAccountsUsed.Count; i++)
            {
                int index = this.actualStorageAccountsUsed[i];
                if (index < 0 || index > numberOfStorageAccounts)
                {
                    throw new Exception(string.Format("this.actualStorageAccountsUsed[{0}] = {1} is out of range.", i, index));
                }

                ReplicaInfo replica = new ReplicaInfo
                {
                    StorageAccountName = this.rtableTestConfiguration.StorageInformation.AccountNames[index],
                    Status = ReplicaStatus.ReadWrite,
                };

                if (readViewHeadIndex != 0 && i < readViewHeadIndex)
                {
                    replica.Status = ReplicaStatus.WriteOnly;
                }

                viewConfig.ReplicaChain.Add(replica);
            }

            // 2 - Create a default table config. that references the "DefaultView"
            var tableConfig = new ReplicatedTableConfiguredTable
            {
                TableName = DefaultTableConfigName,
                ViewName = DefaultViewName,
                ConvertToRTable = convertXStoreTableMode,
                UseAsDefault = true,
            };

            // 3 - Create the final RTable configuration
            ReplicatedTableConfiguration configuration = new ReplicatedTableConfiguration();
            configuration.SetView(DefaultViewName, viewConfig);
            configuration.SetTable(tableConfig);

            return configuration;
        }

        /// <summary>
        /// Create the contents of the RTable configuration, and upload it to the appropriate container and blob.
        /// </summary>
        private void UploadRTableConfigToBlob(int viewId, bool convertXStoreTableMode, int numberOfBlobs)
        {
            // upload the RTable config
            Console.WriteLine("Uploading RTable config ...");

            for (int blobIndex = 0; blobIndex < numberOfBlobs; blobIndex++)
            {
                string connectionString;
                BlobServiceClient cloudBloblClient = this.GenerateBlobServiceClient(
                                       this.rtableTestConfiguration.StorageInformation.AccountNames[blobIndex],
                                       this.rtableTestConfiguration.StorageInformation.AccountKeys[blobIndex],
                                       this.rtableTestConfiguration.StorageInformation.DomainName,
                                       out connectionString);

                BlobContainerClient container = cloudBloblClient.GetBlobContainerClient(this.rtableTestConfiguration.RTableInformation.ContainerName);
                container.CreateIfNotExists();
            }

            ReplicatedTableConfiguration rtableConfig = this.GetRTableConfiguration(viewId, convertXStoreTableMode);
            this.configurationService.UpdateConfiguration(rtableConfig, false);
        }

        /// <summary>
        /// Modify the contents of the RTable configuration blob to use the updated viewId
        /// </summary>
        /// <param name="updatedViewId"></param>
        /// <param name="convertXStoreTableMode"></param>
        /// <param name="readViewHeadIndex"></param>
        private void ModifyConfigurationBlob(long updatedViewId, bool convertXStoreTableMode, int readViewHeadIndex, List<int> blobIndexes)
        {
            try
            {
                List<ReplicatedTableConfiguration> configurations;
                List<ReplicatedTableReadBlobResult> result = this.configurationService.RetrieveConfiguration(out configurations);

                foreach (var blobIndex in blobIndexes)
                {
                    if (blobIndex >= result.Count)
                    {
                        var msg = string.Format("BlobIndex={0} >= ResultCount={1}", blobIndex, result.Count);
                        throw new Exception(msg);
                    }

                    if (result[blobIndex].Code != ReadBlobCode.Success)
                    {
                        throw new Exception(result.ToString());
                    }

                    string currentConfigText = configurations[blobIndex].ToJson();
                    Console.WriteLine("Blob#{0}:\nCurrentConfigText = {1}", blobIndex, currentConfigText);
                }

                ReplicatedTableConfiguration newConfig = this.GetRTableConfiguration(updatedViewId, convertXStoreTableMode, readViewHeadIndex);
                string updatedConfigText = newConfig.ToJson();

                Console.WriteLine("\nModifying blobs:\nUpdatedConfigText = {0}", updatedConfigText);

                ReplicatedTableQuorumWriteResult writeResult = this.configurationService.UploadConfigurationToBlobs(blobIndexes, newConfig);
                if (writeResult.Code != ReplicatedTableQuorumWriteCode.Success)
                {
                    throw new Exception(writeResult.ToString());
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ModifyConfigurationBlob() encountered exception {0}", ex.ToString());
                throw;
            }
        }

        protected void UpdateConfiguration(List<ReplicaInfo> replicaChain, int readViewHeadIndex, bool convertXStoreTableMode = false, long viewId = 0)
        {
            for (int i = 0; i < replicaChain.Count; i++)
            {
                replicaChain[i].Status = ReplicaStatus.ReadWrite;

                if (readViewHeadIndex != 0 && i < readViewHeadIndex)
                {
                    replicaChain[i].Status = ReplicaStatus.WriteOnly;
                }
            }

            // - view config
            var viewConfig = new ReplicatedTableConfigurationStore
            {
                ViewId = viewId,
                ReadViewHeadIndex = readViewHeadIndex,
                ReplicaChain = replicaChain,
            };

            // - table config
            var tableConfig = new ReplicatedTableConfiguredTable
            {
                TableName = DefaultTableConfigName,
                ViewName = DefaultViewName,
                ConvertToRTable = convertXStoreTableMode,
                UseAsDefault = true,
            };

            // - Update RTable configuration
            ReplicatedTableConfiguration configuration = new ReplicatedTableConfiguration();
            configuration.SetView(DefaultViewName, viewConfig);
            configuration.SetTable(tableConfig);

            this.configurationService.UpdateConfiguration(configuration, false);
        }

#endif

        #endregion

    }
}
