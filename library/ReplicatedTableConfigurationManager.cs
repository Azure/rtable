﻿// azure-rtable ver. 0.9
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
        private Dictionary<string, View> viewMap = new Dictionary<string, View>();
        private Dictionary<string, ReplicatedTableConfiguredTable> tableMap = new Dictionary<string, ReplicatedTableConfiguredTable>();
        private ReplicatedTableConfiguredTable defaultConfiguredRule = null;
        private Guid currentRunningConfigId = Guid.Empty;

        internal protected ReplicatedTableConfigurationManager(List<ConfigurationStoreLocationInfo> blobLocations, bool useHttps, int lockTimeoutInSeconds, IReplicatedTableConfigurationParser blobParser)
        {
            this.blobLocations = blobLocations;
            this.useHttps = useHttps;
            this.LockTimeout = TimeSpan.FromSeconds(lockTimeoutInSeconds == 0 ? Constants.LockTimeoutInSeconds : lockTimeoutInSeconds);
            this.LeaseDuration = TimeSpan.FromSeconds(Constants.LeaseRenewalIntervalInSec);

            this.Initialize();

            this.blobParser = blobParser;

            /* IMPORTANT:
             * It is wrong to get a call-back before Constructor finishes !
             * For that, Timer has to be DISABLED initially.
             */
            viewRefreshTimer = new PeriodicTimer(RefreshReadAndWriteViewsFromBlobs, null, TimeSpan.FromMilliseconds(-1), TimeSpan.FromSeconds(TimerIntervalInSeconds));
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

        private int TimerIntervalInSeconds
        {
            get
            {
                return Math.Max(((int)LeaseDuration.TotalSeconds / 2 - Constants.MinimumLeaseRenewalInterval), Constants.MinimumLeaseRenewalInterval);
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

            List<View> views = this.blobParser.ParseBlob(this.blobs.Values.ToList(), this.useHttps, out tableConfigList, out leaseDuration, out configId);
            if (views == null)
            {
                return;
            }

            lock (this)
            {
                // - Update list of views
                this.viewMap.Clear();

                foreach (var view in views)
                {
                    if (view == null || string.IsNullOrEmpty(view.Name))
                    {
                        continue;
                    }

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

                // - Update lease duration
                LeaseDuration = TimeSpan.FromSeconds(leaseDuration);

                // - Update current config Id
                currentRunningConfigId = configId;

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

        internal protected List<CloudBlockBlob> GetBlobs()
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
                return view.IsExpired(LeaseDuration) ? new View(viewName) : view;
            }
        }

        internal protected bool IsViewExpired(string viewName)
        {
            var view = GetView(viewName);
            return view.IsExpired(LeaseDuration);
        }

        internal protected bool IsViewStable(string viewName)
        {
            View view = GetView(viewName);
            return !view.IsEmpty && view.IsStable;
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
    }
}