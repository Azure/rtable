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
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.Table;

    public class ReplicatedTableConfigurationService : IDisposable
    {
        private bool disposed = false;
        private readonly ReplicatedTableConfigurationManager configManager;

        public ReplicatedTableConfigurationService(List<ConfigurationStoreLocationInfo> blobLocations, bool useHttps, int lockTimeoutInSeconds = 0)
        {
            this.configManager = new ReplicatedTableConfigurationManager(blobLocations, useHttps, lockTimeoutInSeconds, new ReplicatedTableConfigurationStoreParser());
            this.configManager.StartMonitor();
        }

        ~ReplicatedTableConfigurationService()
        {
            this.Dispose(false);
        }

        public TimeSpan LockTimeout
        {
            get { return this.configManager.LockTimeout; }

            set { this.configManager.LockTimeout = value; }
        }

        public View GetReadView()
        {
            return this.configManager.GetReadView(ReplicatedTableConfigurationStoreParser.DefaultChainName);
        }

        public View GetWriteView()
        {
            return this.configManager.GetWriteView(ReplicatedTableConfigurationStoreParser.DefaultChainName);
        }

        public bool HasViewExpired
        {
            get
            {
                return this.configManager.IsChainExpired(ReplicatedTableConfigurationStoreParser.DefaultChainName);
            }
        }

        public bool IsViewStable()
        {
            return this.configManager.IsChainStable(ReplicatedTableConfigurationStoreParser.DefaultChainName);
        }

        public bool ConvertXStoreTableMode
        {
            get
            {
                RTableConfiguredTable config = this.configManager.FindConfiguredTable(ReplicatedTableConfigurationStoreParser.AllTables);
                return config != null && config.ConvertXStoreTableMode;
            }
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

            ReplicatedTableConfigurationStore newConfig = new ReplicatedTableConfigurationStore
            {
                LeaseDuration = Constants.LeaseDurationInSec,
                Timestamp = DateTime.UtcNow,
                ReplicaChain = replicaChain,
                ReadViewHeadIndex = readViewHeadIndex,
                ConvertXStoreTableMode = convertXStoreTableMode,
                ViewId = viewId
            };

            //If the read view head index is not 0, this means we are introducing 1 or more replicas at the head. For
            //each such replica, update the view id in which it was added to the write view of the chain
            if (readViewHeadIndex != 0)
            {
                for (int i = 0; i < readViewHeadIndex; i++)
                {
                    replicaChain[i].ViewInWhichAddedToChain = viewId;
                }
            }

            Parallel.ForEach(this.configManager.GetBlobs(), blob =>
            {
                ReplicatedTableConfigurationStore configurationStore = null;
                string eTag;

                /*
                 * TODO: (per Parveen Patel <Parveen.Patel@microsoft.com>)
                 * The below code is incomplete because we are supposed to use eTag to make the changes if the old blob exists.
                 * This is to support multiple clients updating the config, not a high priority scenario but something we should look at.
                 */
                if (!CloudBlobHelpers.TryReadBlob<ReplicatedTableConfigurationStore>(blob, out configurationStore, out eTag))
                {
                    //This is the first time we are uploading the config
                    configurationStore = new ReplicatedTableConfigurationStore();
                }

                configurationStore = newConfig;

                ReplicatedTableConfigurationManager.WriteConfigToBlob(blob, configurationStore.ToJson());
            });

            this.configManager.Invalidate();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (this.disposed)
            {
                return;
            }

            if (disposing)
            {
                this.configManager.StopMonitor();
            }

            this.disposed = true;
        }
    }
}
