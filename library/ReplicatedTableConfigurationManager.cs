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


namespace Microsoft.Azure.Toolkit.Replication
{
    using global::Azure.Core;
    using global::Azure.Data.Tables;
    using global::Azure.Storage.Blobs;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Security;

    class ReplicatedTableConfigurationManager
    {
        private PeriodicTimer viewRefreshTimer;
        private List<ConfigurationStoreLocationInfo> blobLocations;
        private TokenCredential blobLocationsStorageToken;
        private bool useHttps;
        private string storageEndpointDomain;
        private Dictionary<string, BlobClient> blobs = new Dictionary<string, BlobClient>();
        private readonly IReplicatedTableConfigurationParser blobParser;
        private Dictionary<string, View> viewMap = new Dictionary<string, View>();
        private Dictionary<string, ReplicatedTableConfiguredTable> tableMap = new Dictionary<string, ReplicatedTableConfiguredTable>();
        private ReplicatedTableConfiguredTable defaultConfiguredRule = null;
        private Guid currentRunningConfigId = Guid.Empty;
        private volatile bool instrumentation = false;
        private volatile bool ignoreHigherViewIdRows = false;

        private readonly object connectionStringLock = new object();
        private Dictionary<string, SecureString> connectionStringMap = null;
        private Dictionary<string, TokenCredential> replicaStorageTokenMap = null;
        private Func<ReplicaInfo, TableServiceClient> ReplicaTableClientCreationStrategy;

        /// <summary>
        /// Connection string provided by user
        /// </summary>
        /// <param name="replica"></param>
        private TableServiceClient NewTableClientFromConnectionStringStrategy(ReplicaInfo replica)
        {
            if (this.connectionStringMap.ContainsKey(replica.StorageAccountName))
            {
                replica.ConnectionString = this.connectionStringMap[replica.StorageAccountName];
            }
            else
            {
                replica.ConnectionString = null;
            }

            return GetConnectionStringTableClientForReplica(replica);
        }

        /// <summary>
        /// Connection string inferred from blob content
        /// </summary>
        /// <param name="replica"></param>
        private TableServiceClient OldTableClientFromConnectionStringStrategy(ReplicaInfo replica)
        {
            string connectionString = String.Format(Constants.ShortConnectioStringTemplate, useHttps ? "https" : "http", replica.StorageAccountName, replica.StorageAccountKey);
            replica.ConnectionString = SecureStringHelper.ToSecureString(connectionString);

            return GetConnectionStringTableClientForReplica(replica);
        }

        /// <summary>
        /// Managed Identity provided by user.
        /// </summary>
        /// <param name="replica"></param>
        /// <returns></returns>
        private TableServiceClient TableClientFromManagedIdentityStrategy(ReplicaInfo replica)
        {
            TokenCredential storageToken = null;
            if (this.replicaStorageTokenMap.ContainsKey(replica.StorageAccountName))
            {
                storageToken = this.replicaStorageTokenMap[replica.StorageAccountName];
            }

            string tableStorageEndpoint = String.Format(Constants.TableStorageEndpointTemplate,
                ((this.useHttps == true) ? "https" : "http"),
                replica.StorageAccountName,
                this.storageEndpointDomain);

            return GetTokenTableClientForReplica(replica, tableStorageEndpoint, storageToken);
        }

        /// <summary>
        /// Initializes the ReplicatedTableConfigurationManager for connection string auth to storage.
        /// </summary>
        /// <param name="blobLocations"></param>
        /// <param name="connectionStringMap"></param>
        /// <param name="useHttps"></param>
        /// <param name="lockTimeoutInSeconds"></param>
        /// <param name="blobParser"></param>
        internal protected ReplicatedTableConfigurationManager(
                                    List<ConfigurationStoreLocationInfo> blobLocations,
                                    Dictionary<string, SecureString> connectionStringMap,
                                    bool useHttps,
                                    int lockTimeoutInSeconds,
                                    IReplicatedTableConfigurationParser blobParser)
        {
            // Setup configuration blob and replica connection variables.
            this.blobLocations = blobLocations;
            this.ConnectionStrings = connectionStringMap;
            this.useHttps = useHttps;

            this.LockTimeout = TimeSpan.FromSeconds(lockTimeoutInSeconds == 0 ? Constants.LockTimeoutInSeconds : lockTimeoutInSeconds);
            this.LeaseDuration = TimeSpan.FromSeconds(Constants.LeaseRenewalIntervalInSec);

            this.InitializeConfigurationStoreWithConnectionStrings();

            this.blobParser = blobParser;

            /* IMPORTANT:
             * It is wrong to get a call-back before Constructor finishes !
             * For that, Timer has to be DISABLED initially.
             */
            viewRefreshTimer = new PeriodicTimer(RefreshReadAndWriteViewsFromBlobs, null, TimeSpan.FromMilliseconds(-1), TimeSpan.FromSeconds(TimerIntervalInSeconds));
        }

        /// <summary>
        /// Initializes the ReplicatedTableConfigurationManager for managed identity auth to storage.
        /// </summary>
        /// <param name="blobLocations"></param>
        /// <param name="blobLocationsStorageToken"></param>
        /// <param name="replicaStorageTokenMap"></param>
        /// <param name="useHttps"></param>
        /// <param name="lockTimeoutInSeconds"></param>
        /// <param name="blobParser"></param>
        internal protected ReplicatedTableConfigurationManager(
                                    List<ConfigurationStoreLocationInfo> blobLocations,
                                    TokenCredential blobLocationsStorageToken,
                                    Dictionary<string, TokenCredential> replicaStorageTokenMap,
                                    bool useHttps,
                                    string storageEndpointDomain,
                                    int lockTimeoutInSeconds,
                                    IReplicatedTableConfigurationParser blobParser)
        {
            // Setup configuration blob and replica connection variables.
            this.blobLocations = blobLocations;
            this.blobLocationsStorageToken = blobLocationsStorageToken;
            this.ReplicaStorageTokenMap = replicaStorageTokenMap;
            this.useHttps = useHttps;
            this.storageEndpointDomain = storageEndpointDomain;

            this.LockTimeout = TimeSpan.FromSeconds(lockTimeoutInSeconds == 0 ? Constants.LockTimeoutInSeconds : lockTimeoutInSeconds);
            this.LeaseDuration = TimeSpan.FromSeconds(Constants.LeaseRenewalIntervalInSec);

            this.InitializeConfigurationStoreWithTokenCredentials();

            this.blobParser = blobParser;

            /* IMPORTANT:
             * It is wrong to get a call-back before Constructor finishes !
             * For that, Timer has to be DISABLED initially.
             */
            viewRefreshTimer = new PeriodicTimer(RefreshReadAndWriteViewsFromBlobs, null, TimeSpan.FromMilliseconds(-1), TimeSpan.FromSeconds(TimerIntervalInSeconds));
        }

        private void InitializeConfigurationStoreWithConnectionStrings()
        {
            if ((this.blobLocations.Count % 2) == 0)
            {
                throw new ArgumentException("Number of blob locations must be odd");
            }

            foreach (var blobLocation in blobLocations)
            {
                string accountConnectionString = String.Format(Constants.ShortConnectioStringTemplate,
                                    ((this.useHttps == true) ? "https" : "http"),
                                    blobLocation.StorageAccountName,
                                    blobLocation.StorageAccountKey);

                try
                {
                    BlobClient blob = CloudBlobHelpers.GetBlockBlob(accountConnectionString, blobLocation.BlobPath);

                    this.blobs.Add(blobLocation.StorageAccountName + ';' + blobLocation.BlobPath, blob);
                }
                catch (Exception e)
                {
                    ReplicatedTableLogger.LogError("Failed to init blob Acc={0}, Blob={1}. Exception: {2}",
                        blobLocation.StorageAccountName,
                        blobLocation.BlobPath,
                        e.Message);
                }
            }

            int quorumSize = (this.blobLocations.Count / 2) + 1;

            if (this.blobs.Count < quorumSize)
            {
                throw new Exception(string.Format("Retrieved blobs count ({0}) is less than quorum !", this.blobs.Count));
            }
        }

        private void InitializeConfigurationStoreWithTokenCredentials()
        {
            if ((this.blobLocations.Count % 2) == 0)
            {
                throw new ArgumentException("Number of blob locations must be odd");
            }

            foreach (ConfigurationStoreLocationInfo blobLocation in blobLocations)
            {
                TokenCredential blobStorageToken = this.blobLocationsStorageToken;
                string blobStorageEndpoint = String.Format(Constants.BlobStorageEndpointTemplate,
                    ((this.useHttps == true) ? "https" : "http"),
                    blobLocation.StorageAccountName,
                    this.storageEndpointDomain);

                try
                {
                    BlobClient blob = CloudBlobHelpers.GetBlockBlob(
                        blobStorageEndpoint,
                        blobStorageToken,
                        blobLocation.BlobPath);

                    this.blobs.Add(blobLocation.StorageAccountName + ';' + blobLocation.BlobPath, blob);
                }
                catch (Exception e)
                {
                    ReplicatedTableLogger.LogError("Failed to init blob Acc={0}, Blob={1}. Exception: {2}",
                        blobLocation.StorageAccountName,
                        blobLocation.BlobPath,
                        e.Message);
                }
            }

            int quorumSize = (this.blobLocations.Count / 2) + 1;

            if (this.blobs.Count < quorumSize)
            {
                throw new Exception(string.Format("Retrieved blobs count ({0}) is less than quorum !", this.blobs.Count));
            }
        }

        private TimeSpan LeaseDuration
        {
            get;
            set;
        }

        private int TimerIntervalInSeconds
        {
            get
            {
                return Math.Max(((int)LeaseDuration.TotalSeconds / 3 - Constants.MinimumLeaseRenewalInterval), Constants.MinimumLeaseRenewalInterval);
            }
        }

        private void UpdateTimer()
        {
            int timerIntervalInSeconds = TimerIntervalInSeconds;

            lock (this)
            {
                if ((int) viewRefreshTimer.Period.TotalSeconds == timerIntervalInSeconds)
                {
                    return;
                }

                viewRefreshTimer.Stop();
                viewRefreshTimer = new PeriodicTimer(RefreshReadAndWriteViewsFromBlobs, TimeSpan.FromSeconds(timerIntervalInSeconds));
            }
        }

        private void RefreshReadAndWriteViewsFromBlobs(object arg)
        {
            List<ReplicatedTableConfiguredTable> tableConfigList;
            int leaseDuration;
            Guid configId;
            List<View> views;
            bool instrumentationFlag;
            bool ignoreHigherViewIdRows;

            // Lock because both ReplicaTableClientCreationStrategy and connectionStringMap can be updated OOB!
            lock (connectionStringLock)
            {
                DateTime startTime = DateTime.UtcNow;
                views = this.blobParser.ParseBlob(
                                            this.blobs.Values.ToList(),
                                            this.ReplicaTableClientCreationStrategy,
                                            out tableConfigList,
                                            out leaseDuration,
                                            out configId,
                                            out instrumentationFlag,
                                            out ignoreHigherViewIdRows);

                ReplicatedTableLogger.LogInformational("ParseBlob took {0}", DateTime.UtcNow - startTime);

                if (views == null)
                {
                    return;
                }
            }

            lock (this)
            {
                // - Update lease duration
                LeaseDuration = TimeSpan.FromSeconds(leaseDuration);

                // - Update list of views
                this.viewMap.Clear();

                foreach (var view in views)
                {
                    if (view == null || string.IsNullOrEmpty(view.Name))
                    {
                        continue;
                    }

                    // Set view LeaseDuration to the config LeaseDuration
                    view.LeaseDuration = LeaseDuration;
                    this.viewMap.Add(view.Name, view);
                }

                // - Update list of configured tables
                this.tableMap.Clear();
                defaultConfiguredRule = null;

                if (tableConfigList != null)
                {
                    foreach (var tableConfig in tableConfigList)
                    {
                        if (tableConfig == null || string.IsNullOrEmpty(tableConfig.TableName))
                        {
                            continue;
                        }

                        this.tableMap.Add(tableConfig.TableName, tableConfig);

                        if (tableConfig.UseAsDefault)
                        {
                            defaultConfiguredRule = tableConfig;
                        }
                    }
                }

                // - Update current config Id
                currentRunningConfigId = configId;

                // update instrumentation flag
                this.instrumentation = instrumentationFlag;

                // update ignoreHigherViewIdRows flag
                this.ignoreHigherViewIdRows = ignoreHigherViewIdRows;

                UpdateTimer();
            }
        }


        internal protected void StartMonitor()
        {
            /* IMPORTANT:
             * start periodic timer now
             */
            this.viewRefreshTimer.Start();

            RefreshReadAndWriteViewsFromBlobs(null);
        }

        internal protected void StopMonitor()
        {
            this.viewRefreshTimer.Stop();
        }

        internal protected TimeSpan LockTimeout
        {
            get;
            set;
        }

        internal protected List<BlobClient> GetBlobs()
        {
            return this.blobs.Values.ToList();
        }

        internal protected void Invalidate()
        {
            RefreshReadAndWriteViewsFromBlobs(null);
        }

        internal protected Guid GetCurrentRunningConfigId()
        {
            return currentRunningConfigId;
        }

        /// <summary>
        /// Update connection strings before uploading new RTable config to blob.
        /// Make sure new connection strings are an uper-set of previous one - MBB -
        /// </summary>
        internal protected Dictionary<string, SecureString> ConnectionStrings
        {
            private get { return null; }

            set
            {
                lock (connectionStringLock)
                {
                    connectionStringMap = value;

                    if (this.connectionStringMap != null)
                    {
                        ReplicaTableClientCreationStrategy = NewTableClientFromConnectionStringStrategy;
                    }
                    else
                    {
                        ReplicaTableClientCreationStrategy = OldTableClientFromConnectionStringStrategy;
                    }
                }
            }
        }

        /// <summary>
        /// Managed identity token strategy as an alternative to ConnectionStrings.
        /// </summary>
        internal protected Dictionary<string, TokenCredential> ReplicaStorageTokenMap
        {
            private get { return null; }

            set
            {
                lock (connectionStringLock)
                {
                    replicaStorageTokenMap = value;
                    ReplicaTableClientCreationStrategy = TableClientFromManagedIdentityStrategy;
                }
            }
        }

        /*
         * Views functions:
         */
        internal protected View GetView(string viewName)
        {
            lock (this)
            {
                if (string.IsNullOrEmpty(viewName) || !this.viewMap.ContainsKey(viewName))
                {
                    return new View(viewName);
                }

                View view = this.viewMap[viewName];
                return view.IsExpired() ? new View(viewName) : view;
            }
        }

        internal protected bool IsViewExpired(string viewName)
        {
            var view = GetView(viewName);
            return view.IsExpired();
        }

        internal protected bool IsViewStable(string viewName)
        {
            View view = GetView(viewName);
            return !view.IsEmpty && view.IsStable;
        }

        internal protected bool IsIntrumenationEnabled()
        {
            // Not adding lock here as it is not critical to add lock here
            return instrumentation;
        }

        internal protected bool IsIgnoreHigherViewIdRows()
        {
            // Not critical to lock here
            return ignoreHigherViewIdRows;
        }

        /*
         * Configured tables functions:
         */
        internal protected ReplicatedTableConfiguredTable FindConfiguredTable(string tableName)
        {
            lock (this)
            {
                if (string.IsNullOrEmpty(tableName))
                {
                    return null;
                }

                if (this.tableMap.ContainsKey(tableName))
                {
                    return this.tableMap[tableName];
                }

                return defaultConfiguredRule;                
            }
        }

        /*
         * Class functions:
         */
        static internal protected TableServiceClient GetConnectionStringTableClientForReplica(ReplicaInfo replica)
        {
            TableServiceClient tableClient = null;
            if (!CloudBlobHelpers.TryCreateCloudTableClient(replica.ConnectionString, out tableClient))
            {
                ReplicatedTableLogger.LogError("No table client (connection string) created for replica info: {0}", replica);
            }

            return tableClient;
        }

        static internal protected TableServiceClient GetTokenTableClientForReplica(ReplicaInfo replica, string tableStorageEndpoint, TokenCredential tableToken)
        {
            TableServiceClient tableClient = null;
            if (!CloudBlobHelpers.TryCreateCloudTableClient(tableStorageEndpoint, tableToken, out tableClient))
            {
                ReplicatedTableLogger.LogError("No table client (token) created for replica info: {0}", replica);
            }

            return tableClient;
        }
    }
}
