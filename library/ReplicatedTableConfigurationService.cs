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
    using System;
    using System.Collections.Generic;
    using System.Security;
    using System.Threading.Tasks;

    public class ReplicatedTableConfigurationService : IDisposable
    {
        private bool disposed = false;
        private readonly ReplicatedTableConfigurationManager configManager;

        /// <summary>
        /// ** Depricated **
        /// </summary>
        /// <param name="blobLocations"></param>
        /// <param name="useHttps"></param>
        /// <param name="lockTimeoutInSeconds"></param>
        public ReplicatedTableConfigurationService(List<ConfigurationStoreLocationInfo> blobLocations, bool useHttps, int lockTimeoutInSeconds = 0)
            : this(blobLocations, null, useHttps, lockTimeoutInSeconds)
        {
            /* Warning :
            * Don't add any initialization here.
            * Timer thread already started before we make it to here.
            * If needed, consider refactoring this class initialization.
            *
            * However, this is being Depricated => so don't add anything here.
            */
        }

        /// <summary>
        /// User provides all connection strings.
        /// If null is passed in, then connection strings are infered from the blob itself - backward compatibility -
        /// </summary>
        /// <param name="blobLocations"></param>
        /// <param name="connectionStringMap"></param>
        /// <param name="useHttps"></param>
        /// <param name="lockTimeoutInSeconds"></param>
        public ReplicatedTableConfigurationService(List<ConfigurationStoreLocationInfo> blobLocations, Dictionary<string, SecureString> connectionStringMap, bool useHttps, int lockTimeoutInSeconds = 0)
        {
            this.configManager = new ReplicatedTableConfigurationManager(blobLocations, connectionStringMap, useHttps, lockTimeoutInSeconds, new ReplicatedTableConfigurationStoreParser());
            this.configManager.StartMonitor();
        }

        /// <summary>
        /// User provides all token credentials.
        /// </summary>
        /// <param name="blobLocations"></param>
        /// <param name="connectionStringMap"></param>
        /// <param name="useHttps"></param>
        /// <param name="lockTimeoutInSeconds"></param>
        public ReplicatedTableConfigurationService(List<ConfigurationStoreLocationInfo> blobLocations, TokenCredential blobLocationsStorageToken, Dictionary<string, TokenCredential> replicaStorageTokenMap, bool useHttps, string storageEndpointDomain, int lockTimeoutInSeconds = 0)
        {
            this.configManager = new ReplicatedTableConfigurationManager(blobLocations, blobLocationsStorageToken, replicaStorageTokenMap, useHttps, storageEndpointDomain, lockTimeoutInSeconds, new ReplicatedTableConfigurationStoreParser());
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
            return this.configManager.GetView(ReplicatedTableConfigurationStoreParser.DefaultViewName);
        }

        public View GetWriteView()
        {
            return this.configManager.GetView(ReplicatedTableConfigurationStoreParser.DefaultViewName);
        }

        public bool HasViewExpired
        {
            get
            {
                return this.configManager.IsViewExpired(ReplicatedTableConfigurationStoreParser.DefaultViewName);
            }
        }

        public bool IsViewStable()
        {
            return this.configManager.IsViewStable(ReplicatedTableConfigurationStoreParser.DefaultViewName);
        }

        public bool ConvertXStoreTableMode
        {
            get
            {
                ReplicatedTableConfiguredTable config = this.configManager.FindConfiguredTable(ReplicatedTableConfigurationStoreParser.AllTables);
                return config != null && config.ConvertToRTable;
            }
        }

        public bool IsIntrumentationEnabled()
        {
            return this.configManager.IsIntrumenationEnabled();
        }

        public bool IsIgnoreHigherViewIdRows()
        {
            return this.configManager.IsIgnoreHigherViewIdRows();
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
                //ReadViewTailIndex >>> not supported in V1
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
                ReplicatedTableReadBlobResult result = CloudBlobHelpers.TryReadBlob(
                                                                blob,
                                                                out configurationStore,
                                                                out eTag,
                                                                JsonStore<ReplicatedTableConfigurationStore>.Deserialize);
                if (result.Code != ReadBlobCode.Success)
                {
                    //This is the first time we are uploading the config
                    configurationStore = new ReplicatedTableConfigurationStore();
                }

                configurationStore = newConfig;

                CloudBlobHelpers.TryWriteBlob(blob, configurationStore.ToJson());
            });

            this.configManager.Invalidate();
        }

        public void ConfigurationChangeNotification()
        {
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
