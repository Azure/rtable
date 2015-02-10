// The MIT License (MIT)
//
// Copyright (c) 2015 Microsoft Corporation
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN


namespace Microsoft.Azure.Toolkit.Replication
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.Table;

    public class ReplicatedTableConfigurationService : IDisposable
    {
        private List<ConfigurationStoreLocationInfo> blobLocations;
        private Dictionary<string, CloudBlockBlob> blobs = new Dictionary<string, CloudBlockBlob>();

        private bool useHttps;

        private int quorumSize = 0;
        private string cloudStorageAccountTemplate = "DefaultEndpointsProtocol={0};AccountName={1};AccountKey={2}";

        private DateTime lastViewRefreshTime;
        private View lastRenewedReadView;
        private View lastRenewedWriteView;

        private PeriodicTimer viewRefreshTimer;
        private bool disposed = false;


        public ReplicatedTableConfigurationService(List<ConfigurationStoreLocationInfo> blobLocations, 
            bool useHttps, 
            int lockTimeoutInSeconds = 0)
        {
            this.blobLocations = blobLocations;
            this.useHttps = useHttps;
            this.lastViewRefreshTime = DateTime.MinValue;
            this.lastRenewedReadView = new View();
            this.lastRenewedWriteView = new View();
            this.LockTimeout = TimeSpan.FromSeconds(lockTimeoutInSeconds == 0 ? Constants.LockTimeoutInSeconds : lockTimeoutInSeconds);
            this.Initialize();

            this.viewRefreshTimer = new PeriodicTimer(RefreshReadAndWriteViewsFromBlobs, 
                                                      TimeSpan.FromSeconds(Constants.LeaseRenewalIntervalInSec));
        }

        ~ReplicatedTableConfigurationService()
        {
            this.Dispose(false);
        }

        private void Initialize()
        {
            if ((this.blobLocations.Count % 2) == 0)
            {
                throw new ArgumentException("Number of blob locations must be odd");
            }

            foreach (var blobLocation in blobLocations)
            {
                string accountConnectionString =
                    String.Format(cloudStorageAccountTemplate, ((this.useHttps == true) ? "https" : "http"), 
                                    blobLocation.StorageAccountName, 
                                    blobLocation.StorageAccountKey);

                CloudBlockBlob blob = CloudBlobHelpers.GetBlockBlob(accountConnectionString, blobLocation.BlobPath);
                this.blobs.Add(blobLocation.StorageAccountName + ';' + blobLocation.BlobPath, blob);
            }

            this.quorumSize = (this.blobs.Count / 2) + 1;
        }

        public TimeSpan LockTimeout
        {
            get; set;
        }

        public bool ConvertXStoreTableMode
        {
            get; set;
        }

        public void UpdateConfiguration(List<ReplicaInfo> replicaChain, int readViewHeadIndex, bool convertXStoreTableMode = false)
        {
            Parallel.ForEach(this.blobs, blob =>
            {
                ReplicatedTableConfigurationStore configurationStore = null;
                long newViewId = 0;
                if (!CloudBlobHelpers.TryReadBlob<ReplicatedTableConfigurationStore>(blob.Value, out configurationStore))
                {
                    //This is the first time we are uploading the config
                    configurationStore = new ReplicatedTableConfigurationStore();
                }

                newViewId = configurationStore.ViewId + 1;

                configurationStore.LeaseDuration = Constants.LeaseDurationInSec;
                configurationStore.Timestamp = DateTime.UtcNow;
                configurationStore.ReplicaChain = replicaChain;
                configurationStore.ReadViewHeadIndex = readViewHeadIndex;
                configurationStore.ConvertXStoreTableMode = convertXStoreTableMode;

                configurationStore.ViewId = newViewId;

                //If the read view head index is not 0, this means we are introducing 1 or more replicas at the head. For 
                //each such replica, update the view id in which it was added to the write view of the chain
                if (readViewHeadIndex != 0)
                {
                    for (int i = 0; i < readViewHeadIndex; i++)
                    {
                        replicaChain[i].ViewInWhichAddedToChain = newViewId;
                    }
                }

                try
                {
                    //Step 1: Delete the current configuration
                    blob.Value.UploadText(Constants.ConfigurationStoreUpdatingText);

                    //Step 2: Wait for L + CF to make sure no pending transaction working on old views
                    Thread.Sleep(TimeSpan.FromSeconds(Constants.LeaseDurationInSec +
                                                        Constants.ClockFactorInSec));

                    //Step 3: Update new config
                    blob.Value.UploadText(JsonStore<ReplicatedTableConfigurationStore>.Serialize(configurationStore));

                }
                catch (StorageException e)
                {
                    ReplicatedTableLogger.LogError("Updating the blob: {0} failed. Exception: {1}", blob.Value, e.Message);
                }
            });

            //Invalidate the lastViewRefreshTime so that updated views get returned
            this.lastViewRefreshTime = DateTime.MinValue;
        }

        public View GetReadView()
        {
            lock (this)
            {
                //Even though we have periodic renewal, we still need to check if we need a renewal at this point. This 
                //is to prevent us from returning a stale view if for some reason the periodic renewal was delayed (we are not a real time
                //OS! ) 
                if (this.DoesViewNeedRefresh())
                {
                    RefreshReadAndWriteViewsFromBlobs(null);
                }

                return this.lastRenewedReadView;
            }
        }

        public View GetWriteView()
        {
            lock (this)
            {
                //Even though we have periodic renewal, we still need to check if we need a renewal at this point. This 
                //is to prevent us from returning a stale view if for some reason the periodic renewal was delayed (we are not a real time
                //OS! ) 
                if (this.DoesViewNeedRefresh())
                {
                    RefreshReadAndWriteViewsFromBlobs(null);
                }

                return this.lastRenewedWriteView;
            }
        }

        private void RefreshReadAndWriteViewsFromBlobs(object arg)
        {
            lock (this)
            {
                this.lastRenewedReadView = new View();
                this.lastRenewedWriteView = new View();

                Dictionary<long, List<CloudBlockBlob>> viewResult = new Dictionary<long, List<CloudBlockBlob>>();

                foreach (var blob in this.blobs)
                {
                    ReplicatedTableConfigurationStore configurationStore;
                    if (!CloudBlobHelpers.TryReadBlob<ReplicatedTableConfigurationStore>(blob.Value, out configurationStore))
                    {
                        continue;
                    }

                    if (configurationStore.ViewId <= 0)
                    {
                        ReplicatedTableLogger.LogInformational("ViewId={0} is invalid. Must be >= 1. Skipping this blob {1}.",
                            configurationStore.ViewId, 
                            blob.Value.Uri);
                        continue;
                    }

                    List<CloudBlockBlob> viewBlobs;
                    if (!viewResult.TryGetValue(configurationStore.ViewId, out viewBlobs))
                    {
                        viewBlobs = new List<CloudBlockBlob>();
                        viewResult.Add(configurationStore.ViewId, viewBlobs);
                    }

                    viewBlobs.Add(blob.Value);

                    if (viewBlobs.Count >= this.quorumSize)
                    {
                        this.lastRenewedReadView.ViewId = this.lastRenewedWriteView.ViewId = configurationStore.ViewId;
                        for (int i = 0; i < configurationStore.ReplicaChain.Count; i++)
                        {
                            ReplicaInfo replica = configurationStore.ReplicaChain[i];
                            CloudTableClient tableClient = GetTableClientForReplica(replica);
                            if (replica != null && tableClient != null)
                            {
                                //Update the write view always
                                this.lastRenewedWriteView.Chain.Add(new Tuple<ReplicaInfo, CloudTableClient>(replica, tableClient));

                                //Update the read view only for replicas part of the view
                                if (i >= configurationStore.ReadViewHeadIndex)
                                {
                                    this.lastRenewedReadView.Chain.Add(new Tuple<ReplicaInfo, CloudTableClient>(replica, tableClient));
                                }

                                //Note the time when the view was updated
                                this.lastViewRefreshTime = DateTime.UtcNow;

                                this.ConvertXStoreTableMode = configurationStore.ConvertXStoreTableMode;
                            }
                        }

                        this.lastRenewedWriteView.ReadHeadIndex = configurationStore.ReadViewHeadIndex;

                        break;
                    }
                }
            }
        }

        /// <summary>
        /// True, if the read and write views are the same. False, otherwise.
        /// </summary>
        public bool IsViewStable()
        {
            int viewStableCount = 0;
            foreach (var blob in this.blobs)
            {
                ReplicatedTableConfigurationStore configurationStore;
                if (!CloudBlobHelpers.TryReadBlob<ReplicatedTableConfigurationStore>(blob.Value, out configurationStore))
                {
                    continue;
                }

                if (configurationStore.ReadViewHeadIndex == 0)
                    viewStableCount++;
            }

            return viewStableCount >= this.quorumSize;
        }

        private CloudTableClient GetTableClientForReplica(ReplicaInfo replica)
        {
            string connectionString = String.Format(cloudStorageAccountTemplate,
                        this.useHttps ? "https" : "http",
                        replica.StorageAccountName,
                        replica.StorageAccountKey);

            CloudTableClient tableClient = null;
            if (!CloudBlobHelpers.TryCreateCloudTableClient(connectionString, out tableClient))
            {
                ReplicatedTableLogger.LogError("No table client created for replica info: {0}", replica);
            }

            return tableClient;
        }


        private bool DoesViewNeedRefresh()
        {
            lock (this)
            {
                if ((DateTime.UtcNow - this.lastViewRefreshTime) >
                    TimeSpan.FromSeconds(Constants.LeaseRenewalIntervalInSec))
                {
                    ReplicatedTableLogger.LogInformational("Need to renew lease on the view/refresh the view");
                    return true;
                }

                return false;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (this.disposed)
                return;

            if (disposing)
            {
                this.viewRefreshTimer.Stop();
            }

            this.disposed = true;
        }
    }
}
