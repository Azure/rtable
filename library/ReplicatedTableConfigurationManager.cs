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
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.Table;

    class ReplicatedTableConfigurationManager
    {
        private PeriodicTimer viewRefreshTimer;
        private List<ConfigurationStoreLocationInfo> blobLocations;
        private bool useHttps;
        private Dictionary<string, CloudBlockBlob> blobs = new Dictionary<string, CloudBlockBlob>();
        private readonly IReplicatedTableConfigurationParser blobParser;
        private Dictionary<string, RTableChainViews> chainsMap = new Dictionary<string, RTableChainViews>();
        private Dictionary<string, RTableConfiguredTable> tableMap = new Dictionary<string, RTableConfiguredTable>();

        internal protected ReplicatedTableConfigurationManager(List<ConfigurationStoreLocationInfo> blobLocations, bool useHttps, int lockTimeoutInSeconds, IReplicatedTableConfigurationParser blobParser)
        {
            this.blobLocations = blobLocations;
            this.useHttps = useHttps;
            this.LockTimeout = TimeSpan.FromSeconds(lockTimeoutInSeconds == 0 ? Constants.LockTimeoutInSeconds : lockTimeoutInSeconds);
            this.LeaseDuration = TimeSpan.FromSeconds(Constants.LeaseRenewalIntervalInSec);

            this.Initialize();

            this.blobParser = blobParser;
        }

        private void Initialize()
        {
            if ((this.blobLocations.Count % 2) == 0)
            {
                throw new ArgumentException("Number of blob locations must be odd");
            }

            foreach (var blobLocation in blobLocations)
            {
                string accountConnectionString = String.Format(Constants.CloudStorageAccountTemplate,
                                    ((this.useHttps == true) ? "https" : "http"),
                                    blobLocation.StorageAccountName,
                                    blobLocation.StorageAccountKey);

                CloudBlockBlob blob = CloudBlobHelpers.GetBlockBlob(accountConnectionString, blobLocation.BlobPath);
                this.blobs.Add(blobLocation.StorageAccountName + ';' + blobLocation.BlobPath, blob);
            }
        }

        private TimeSpan LeaseDuration
        {
            get;
            set;
        }

        private void UpdateTimer()
        {
            int timerIntervalInSeconds = Math.Max(((int)LeaseDuration.TotalSeconds / 2 - Constants.MinimumLeaseRenewalInterval), Constants.MinimumLeaseRenewalInterval);

            if (viewRefreshTimer != null)
            {
                if ((int)viewRefreshTimer.Period.TotalSeconds == timerIntervalInSeconds)
                {
                    return;
                }

                viewRefreshTimer.Stop();
            }

            viewRefreshTimer = new PeriodicTimer(RefreshReadAndWriteViewsFromBlobs, TimeSpan.FromSeconds(timerIntervalInSeconds));
        }

        private void RefreshReadAndWriteViewsFromBlobs(object arg)
        {
            List<RTableConfiguredTable> tableConfigList;
            int leaseDuration;

            List<RTableChainViews> chains = this.blobParser.ParseBlob(this.blobs.Values.ToList(), this.useHttps, out tableConfigList, out leaseDuration);
            if (chains == null)
            {
                return;
            }

            lock (this)
            {
                // - Update list of chains
                this.chainsMap.Clear();

                foreach (var chain in chains)
                {
                    if (chain == null || string.IsNullOrEmpty(chain.Name))
                    {
                        continue;
                    }

                    this.chainsMap.Add(chain.Name, chain);
                }

                // - Update list of configured tables
                this.tableMap.Clear();

                if (tableConfigList != null)
                {
                    foreach (var tableConfig in tableConfigList)
                    {
                        if (tableConfig == null || string.IsNullOrEmpty(tableConfig.TableName))
                        {
                            continue;
                        }

                        this.tableMap.Add(tableConfig.TableName, tableConfig);
                    }
                }

                // - Update lease duration
                LeaseDuration = TimeSpan.FromSeconds(leaseDuration);

                UpdateTimer();
            }
        }


        internal protected void StartMonitor()
        {
            UpdateTimer();
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

        internal protected List<CloudBlockBlob> GetBlobs()
        {
            return this.blobs.Values.ToList();
        }

        internal protected void Invalidate()
        {
            RefreshReadAndWriteViewsFromBlobs(null);
        }


        /*
         * Chains functions:
         */
        private RTableChainViews FindChain(string chainName)
        {
            lock (this)
            {
                if (string.IsNullOrEmpty(chainName) || !this.chainsMap.ContainsKey(chainName))
                {
                    return null;
                }

                return this.chainsMap[chainName];
            }
        }

        internal protected View GetReadView(string chainName)
        {
            var chain = FindChain(chainName);
            if (chain == null)
            {
                return new View();
            }

            return chain.IsExpired(LeaseDuration) ? new View() : chain.ReadView;
        }

        internal protected View GetWriteView(string chainName)
        {
            var chain = FindChain(chainName);
            if (chain == null)
            {
                return new View();
            }

            return chain.IsExpired(LeaseDuration) ? new View() : chain.WriteView;
        }

        internal protected bool IsChainExpired(string chainName)
        {
            var chain = FindChain(chainName);
            if (chain == null)
            {
                return true;
            }

            return chain.IsExpired(LeaseDuration);
        }

        internal protected bool IsChainStable(string chainName)
        {
            View view = GetWriteView(chainName);

            if (view.IsEmpty)
            {
                return false;
            }

            return view.IsStable;
        }


        /*
         * Configured tables functions:
         */
        internal protected RTableConfiguredTable FindConfiguredTable(string tableName)
        {
            lock (this)
            {
                if (string.IsNullOrEmpty(tableName) || !this.tableMap.ContainsKey(tableName))
                {
                    return null;
                }

                return this.tableMap[tableName];
            }
        }


        /*
         * Class functions:
         */
        static internal protected CloudTableClient GetTableClientForReplica(ReplicaInfo replica, bool useHttps)
        {
            string connectionString = String.Format(Constants.CloudStorageAccountTemplate,
                        useHttps ? "https" : "http",
                        replica.StorageAccountName,
                        replica.StorageAccountKey);

            CloudTableClient tableClient = null;
            if (!CloudBlobHelpers.TryCreateCloudTableClient(connectionString, out tableClient))
            {
                ReplicatedTableLogger.LogError("No table client created for replica info: {0}", replica);
            }

            return tableClient;
        }

        static internal protected void WriteConfigToBlob(CloudBlockBlob blob, string content)
        {
            try
            {
                //Step 1: Delete the current configuration
                blob.UploadText(Constants.ConfigurationStoreUpdatingText);

                //Step 2: Wait for L + CF to make sure no pending transaction working on old views
                Thread.Sleep(TimeSpan.FromSeconds(Constants.LeaseDurationInSec + Constants.ClockFactorInSec));

                //Step 3: Update new config
                blob.UploadText(content);
            }
            catch (StorageException e)
            {
                ReplicatedTableLogger.LogError("Updating the blob: {0} failed. Exception: {1}", blob, e.Message);
            }
        }
    }
}
