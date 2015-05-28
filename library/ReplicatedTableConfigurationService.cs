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


using System.Linq;

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
        private TimeSpan leaseDuration;
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
            LeaseDuration = TimeSpan.FromSeconds(Constants.LeaseRenewalIntervalInSec);
            this.Initialize();
        }

        private TimeSpan LeaseDuration
        {
            get { return leaseDuration; }
            set
            {
                leaseDuration = value;
                UpdateTimer(Math.Max(((int) leaseDuration.TotalSeconds/2 - Constants.MinimumLeaseRenewalInterval),
                    Constants.MinimumLeaseRenewalInterval));
            }
        }

        private void UpdateTimer(int timerIntervalInSeconds)
        {
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
            RefreshReadAndWriteViewsFromBlobs(null);
        }

        public TimeSpan LockTimeout
        {
            get; set;
        }

        public bool ConvertXStoreTableMode
        {
            get; set;
        }

        public void UpdateConfiguration(List<ReplicaInfo> replicaChain, int readViewHeadIndex, bool convertXStoreTableMode = false, long viewId = 0)
        {
            View currentView = GetWriteView();

            if (viewId == 0)
            {
                if (!currentView.IsEmpty)
                {
                    viewId = currentView.ViewId + 1;
                }
                else
                {
                    viewId = 1;
                }
            }

            Parallel.ForEach(this.blobs, blob =>
            {
                ReplicatedTableConfigurationStore configurationStore = null;
                string eTag;
                if (!CloudBlobHelpers.TryReadBlob<ReplicatedTableConfigurationStore>(blob.Value, out configurationStore, out eTag))
                {
                    //This is the first time we are uploading the config
                    configurationStore = new ReplicatedTableConfigurationStore();
                }

                configurationStore.LeaseDuration = Constants.LeaseDurationInSec;
                configurationStore.Timestamp = DateTime.UtcNow;
                configurationStore.ReplicaChain = replicaChain;
                configurationStore.ReadViewHeadIndex = readViewHeadIndex;
                configurationStore.ConvertXStoreTableMode = convertXStoreTableMode;

                configurationStore.ViewId = viewId;

                //If the read view head index is not 0, this means we are introducing 1 or more replicas at the head. For 
                //each such replica, update the view id in which it was added to the write view of the chain
                if (readViewHeadIndex != 0)
                {
                    for (int i = 0; i < readViewHeadIndex; i++)
                    {
                        replicaChain[i].ViewInWhichAddedToChain = viewId;
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
            RefreshReadAndWriteViewsFromBlobs(null);
        }

        public View GetReadView()
        {
            lock (this)
            {
                if (HasViewExpired)
                {
                    return new View();
                }

                return this.lastRenewedReadView;
            }
        }

        public View GetWriteView()
        {
            lock (this)
            {
                if (HasViewExpired)
                {
                    return new View();
                }

                return this.lastRenewedWriteView;
            }
        }

        private void RefreshReadAndWriteViewsFromBlobs(object arg)
        {
            View newReadView = new View();
            View newWriteView = new View();

            DateTime refreshStartTime = DateTime.UtcNow;
            bool convertXStoreTableMode = false;
            Dictionary<long, List<CloudBlockBlob>> viewResult = new Dictionary<long, List<CloudBlockBlob>>();

            ReplicatedTableConfigurationStore configurationStore;
            List<string> eTags;
            if (!CloudBlobHelpers.TryReadBlobQuorum(this.blobs.Values.ToList(), out configurationStore, out eTags))
            {
                Console.WriteLine("Unable to refresh view");
                ReplicatedTableLogger.LogError("Unable to refresh view.");
                return;
            }


            if (configurationStore.ViewId <= 0)
            {
                ReplicatedTableLogger.LogError("ViewId={0} is invalid. Must be >= 1.", configurationStore.ViewId);
                return;
            }

            newReadView.ViewId = newWriteView.ViewId = configurationStore.ViewId;
            for (int i = 0; i < configurationStore.ReplicaChain.Count; i++)
            {
                ReplicaInfo replica = configurationStore.ReplicaChain[i];
                CloudTableClient tableClient = GetTableClientForReplica(replica);
                if (replica != null && tableClient != null)
                {
                    //Update the write view always
                    newWriteView.Chain.Add(new Tuple<ReplicaInfo, CloudTableClient>(replica,
                        tableClient));

                    //Update the read view only for replicas part of the view
                    if (i >= configurationStore.ReadViewHeadIndex)
                    {
                        newReadView.Chain.Add(new Tuple<ReplicaInfo, CloudTableClient>(replica,
                            tableClient));
                    }

                    convertXStoreTableMode = configurationStore.ConvertXStoreTableMode;
                }
            }

            newWriteView.ReadHeadIndex = configurationStore.ReadViewHeadIndex;

            lock (this)
            {
                if (!newReadView.IsEmpty && !newWriteView.IsEmpty)
                {
                    this.lastViewRefreshTime = refreshStartTime;
                    this.ConvertXStoreTableMode = convertXStoreTableMode;

                    this.lastRenewedReadView = newReadView;
                    this.lastRenewedWriteView = newWriteView;

                    LeaseDuration = TimeSpan.FromSeconds(configurationStore.LeaseDuration);
                }
            }
        }

        /// <summary>
        /// True, if the read and write views are the same. False, otherwise.
        /// </summary>
        public bool IsViewStable()
        {
            View view = this.GetWriteView();

            if (view.IsEmpty)
            {
                return false;
            }

            return view.IsStable;
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

        public bool HasViewExpired
        {
            get
            {
                if (DateTime.UtcNow - lastViewRefreshTime > LeaseDuration)
                {
                    return true;
                }

                return false;
            }
        }
    }
}
