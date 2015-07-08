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
    using System.Threading;
    using System.Linq;

    public class ReplicatedTableConfigurationServiceV2 : IDisposable
    {
        private bool disposed = false;
        private readonly ReplicatedTableConfigurationManager configManager;

        public ReplicatedTableConfigurationServiceV2(List<ConfigurationStoreLocationInfo> blobLocations, bool useHttps, int lockTimeoutInSeconds = 0)
        {
            this.configManager = new ReplicatedTableConfigurationManager(blobLocations, useHttps, lockTimeoutInSeconds, new ReplicatedTableConfigurationParser());
            this.configManager.StartMonitor();
        }

        ~ReplicatedTableConfigurationServiceV2()
        {
            this.Dispose(false);
        }

        /*
         * Configuration management APIs
         */
        public ReplicatedTableQuorumReadResult RetrieveConfiguration(out ReplicatedTableConfiguration configuration)
        {
            List<string> eTags;

            ReplicatedTableQuorumReadResult
            result = CloudBlobHelpers.TryReadBlobQuorum(
                                                this.configManager.GetBlobs(),
                                                out configuration,
                                                out eTags,
                                                ReplicatedTableConfiguration.FromJson);

            if (result.Code != ReplicatedTableQuorumReadCode.Success)
            {
                ReplicatedTableLogger.LogError("Failed to read configuration, \n{0}", result.ToString());
            }

            return result;
        }

        public List<ReplicatedTableReadBlobResult> RetrieveConfiguration(out List<ReplicatedTableConfiguration> configuration)
        {
            List<string> eTagsArray;

            return CloudBlobHelpers.TryReadAllBlobs(
                                                this.configManager.GetBlobs(),
                                                out configuration,
                                                out eTagsArray,
                                                ReplicatedTableConfiguration.FromJson);
        }

        public ReplicatedTableQuorumWriteResult UpdateConfiguration(ReplicatedTableConfiguration configuration, bool useConditionalUpdate = true)
        {
            if (configuration == null)
            {
                throw new ArgumentNullException("configuration");
            }


            // TODO: LockBlobs(***)


            return UpdateConfigurationInternal(configuration, useConditionalUpdate);
        }

        private ReplicatedTableQuorumWriteResult UpdateConfigurationInternal(ReplicatedTableConfiguration configuration, bool useConditionalUpdate)
        {
            // - Sanitize configuration ...
            foreach (var view in configuration.viewMap)
            {
                var viewName = view.Key;
                var viewConf = view.Value;

                long viewId = viewConf.ViewId;
                View currentView = GetView(viewName);

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

                viewConf.Timestamp = DateTime.UtcNow;
                viewConf.ViewId = viewId;


                // If the read view head index is not 0, this means we are introducing 1 or more replicas at the head.
                // For each such replica, update the view id in which it was added to the write view of the chain
                foreach (var replica in viewConf.GetCurrentReplicaChain())
                {
                    if (replica.IsWriteOnly())
                    {
                        replica.ViewInWhichAddedToChain = viewId;
                        continue;
                    }

                    // stop at the first Readable replica
                    break;
                }
            }

            // - Upload configuration ...
            Func<ReplicatedTableConfiguration, ReplicatedTableConfiguration, bool> comparer = (a, b) => a.Id == b.Id;
            if (!useConditionalUpdate)
            {
                comparer = (a, b) => true;
            }

            ReplicatedTableQuorumWriteResult
            result = CloudBlobHelpers.TryWriteBlobQuorum(
                                            this.configManager.GetBlobs(),
                                            configuration,
                                            ReplicatedTableConfiguration.FromJson,
                                            comparer,
                                            ReplicatedTableConfiguration.GenerateNewConfigId);

            if (result.Code == ReplicatedTableQuorumWriteCode.Success)
            {
                this.configManager.Invalidate();
            }
            else
            {
                ReplicatedTableLogger.LogError("Failed to update configuration, \n{0}", result.ToString());
            }

            return result;
        }


        /*
         * View/Table APIs
         */
        public TimeSpan LockTimeout
        {
            get { return this.configManager.LockTimeout; }

            set { this.configManager.LockTimeout = value; }
        }

        public View GetView(string viewName)
        {
            return this.configManager.GetView(viewName);
        }

        public bool IsViewStable(string viewName)
        {
            return this.configManager.IsViewStable(viewName);
        }

        public bool IsConfiguredTable(string tableName, out ReplicatedTableConfiguredTable configuredTable)
        {
            configuredTable = null;

            ReplicatedTableConfiguredTable config = this.configManager.FindConfiguredTable(tableName);

            // Neither explicit config, nor default config
            if (config == null)
            {
                return false;
            }

            // Placeholder config i.e. a config with No View
            if (string.IsNullOrEmpty(config.ViewName))
            {
                return false;
            }

            configuredTable = config;
            return true;
        }

        public View GetTableView(string tableName)
        {
            ReplicatedTableConfiguredTable config;
            if (IsConfiguredTable(tableName, out config))
            {
                return GetView(config.ViewName);
            }

            var msg = string.Format("Table={0}: is not configured!", tableName);
            ReplicatedTableLogger.LogError(msg);
            throw new Exception(msg);
        }

        public bool IsTableViewStable(string tableName)
        {
            ReplicatedTableConfiguredTable config;
            if (IsConfiguredTable(tableName, out config))
            {
                return IsViewStable(config.ViewName);
            }

            var msg = string.Format("Table={0}: is not configured!", tableName);
            ReplicatedTableLogger.LogError(msg);
            throw new Exception(msg);
        }

        public bool ConvertToRTable(string tableName)
        {
            ReplicatedTableConfiguredTable config;
            if (IsConfiguredTable(tableName, out config))
            {
                return config.ConvertToRTable;
            }

            var msg = string.Format("Table={0}: is not configured", tableName);
            ReplicatedTableLogger.LogError(msg);
            throw new Exception(msg);
        }


        /*
         * Replica management APIs
         */
        public void TurnReplicaOn(string storageAccountName, List<string> tablesToRepair)
        {
            if (string.IsNullOrEmpty(storageAccountName))
            {
                throw new ArgumentNullException("storageAccountName");
            }

            if (tablesToRepair == null)
            {
                throw new ArgumentNullException("tablesToRepair");
            }


            // TODO: LockBlobs(***)


            ReplicatedTableConfiguration configuration = null;

            // - Retrieve configuration ...
            ReplicatedTableQuorumReadResult readResult = RetrieveConfiguration(out configuration);
            if (readResult.Code != ReplicatedTableQuorumReadCode.Success)
            {
                var msg = string.Format("TurnReplicaOn={0}: failed to read configuration, \n{1}", storageAccountName, readResult.ToString());

                ReplicatedTableLogger.LogError(msg);
                throw new Exception(msg);
            }


            /* - Phase 1:
             *      Move the *replica* to the front and set it to None.
             *      Make the view ReadOnly.
             **/
            #region Phase 1

            foreach (var entry in configuration.viewMap)
            {
                var viewName = entry.Key;
                var viewConf = entry.Value;

                MoveReplicaToFrontAndSetViewToReadOnly(viewName, viewConf, storageAccountName);
            }

            // - Write back configuration ...
            ReplicatedTableQuorumWriteResult writeResult = UpdateConfigurationInternal(configuration, true);
            if (writeResult.Code != ReplicatedTableQuorumWriteCode.Success)
            {
                var msg = string.Format("TurnReplicaOn={0}: failed to update -Phase 1- configuration, \n{1}", storageAccountName, writeResult.ToString());

                ReplicatedTableLogger.LogError(msg);
                throw new Exception(msg);
            }

            #endregion
            /**
             * Chain is such: [None] -> [RO] -> ... -> [RO]
             *            or: [None] -> [None] -> ... -> [None]
             **/


            // - Wait for L + CF to make sure no pending transaction working on old views
            Thread.Sleep(TimeSpan.FromSeconds(Constants.LeaseDurationInSec + Constants.ClockFactorInSec));


            /* - Phase 2:
             *      Set the *replica* (head) to WriteOnly and other active replicas to ReadWrite.
             *   Or, in case of one replic chain
             *      Set only the *replica* (head) to ReadWrite.
             **/
            #region Phase 2

            foreach (var entry in configuration.viewMap)
            {
                var viewName = entry.Key;
                var viewConf = entry.Value;

                EnableWriteOnReplicas(viewName, viewConf, storageAccountName);
            }

            // - Write back configuration ...
            //   Note: configuration *Id* has changed since previous updated So disable conditional update
            writeResult = UpdateConfigurationInternal(configuration, false);
            if (writeResult.Code != ReplicatedTableQuorumWriteCode.Success)
            {
                var msg = string.Format("TurnReplicaOn={0}: failed to update -Phase 2- configuration, \n{1}", storageAccountName, writeResult.ToString());

                ReplicatedTableLogger.LogError(msg);
                throw new Exception(msg);
            }

            // - Confirm the new config is the current loaded into the RTable config manager
            if (writeResult.Message != this.configManager.GetCurrentRunningConfigId().ToString())
            {
                var msg = string.Format("TurnReplicaOn={0}: new configId({1}) != current loaded configurationId({2}) ?",
                                        storageAccountName,
                                        writeResult.Message,
                                        this.configManager.GetCurrentRunningConfigId());

                ReplicatedTableLogger.LogError(msg);
                throw new Exception(msg);
            }

            #endregion
            /**
             * Chain is such: [W] -> [RW] -> ... -> [RW]
             *            or: [RW] -> [None] -> ... -> [None]
             **/


            /* - Phase 3:
             *      Repair all tables
             **/
            #region Phase 3

            foreach (var tableName in tablesToRepair)
            {
                RepairTable(tableName, storageAccountName);
            }

            #endregion


            // TODO:
            // Set R2 to RW
            // UpdateConfg

            //-------------------------------------------------
            // t3:      = 0             R2(RW)   --> R1(RW)

        }

        public void TurnReplicaOff(string storageAccountName)
        {
            if (string.IsNullOrEmpty(storageAccountName))
            {
                throw new ArgumentNullException("storageAccountName");
            }


            // TODO: LockBlobs(***)


            ReplicatedTableConfiguration configuration = null;

            // - Retrieve configuration ...
            ReplicatedTableQuorumReadResult readResult = RetrieveConfiguration(out configuration);
            if (readResult.Code != ReplicatedTableQuorumReadCode.Success)
            {
                var msg = string.Format("TurnReplicaOff={0}: failed to read configuration, \n{1}", storageAccountName, readResult.ToString());

                ReplicatedTableLogger.LogError(msg);
                throw new Exception(msg);
            }

            // - Parse/Update all views ...
            foreach (var viewConf in configuration.viewMap.Values)
            {
                var foundReplicas = viewConf.GetCurrentReplicaChain()
                                            .FindAll(r => r.StorageAccountName == storageAccountName);

                foreach (var replica in foundReplicas)
                {
                    replica.Status = ReplicaStatus.None;
                }
            }

            // - Write back configuration ...
            ReplicatedTableQuorumWriteResult writeResult = UpdateConfigurationInternal(configuration, true);
            if (writeResult.Code != ReplicatedTableQuorumWriteCode.Success)
            {
                var msg = string.Format("TurnReplicaOff={0}: failed to update configuration, \n{1}", storageAccountName, writeResult.ToString());

                ReplicatedTableLogger.LogError(msg);
                throw new Exception(msg);
            }
        }

        private static void MoveReplicaToFrontAndSetViewToReadOnly(string viewName, ReplicatedTableConfigurationStore conf, string storageAccountName)
        {
            List<ReplicaInfo> list = conf.ReplicaChain;

            int matchIndex = list.FindIndex(r => r.StorageAccountName == storageAccountName);
            if (matchIndex == -1)
            {
                return;
            }

            // - Ensure its status is *None*
            ReplicaInfo candidateReplica = list[matchIndex];
            candidateReplica.Status = ReplicaStatus.None;

            // - Move it to the front of the chain
            list.RemoveAt(matchIndex);
            list.Insert(0, candidateReplica);

            // Set all active replicas to *ReadOnly*
            foreach (ReplicaInfo replica in conf.GetCurrentReplicaChain())
            {
                if (replica.Status == ReplicaStatus.WriteOnly)
                {
                    var msg = string.Format("View:\'{0}\' : can't set a WriteOnly replica to ReadOnly !!!", viewName);

                    ReplicatedTableLogger.LogError(msg);
                    throw new Exception(msg);
                }

                replica.Status = ReplicaStatus.ReadOnly;
            }
        }

        private static void EnableWriteOnReplicas(string viewName, ReplicatedTableConfigurationStore conf, string storageAccountName)
        {
            List<ReplicaInfo> list = conf.ReplicaChain;

            if (!list.Any() ||
                list[0].StorageAccountName != storageAccountName)
            {
                return;
            }

            // First, enable Write on all replicas
            foreach (ReplicaInfo replica in conf.GetCurrentReplicaChain())
            {
                replica.Status = ReplicaStatus.ReadWrite;
            }

            // Then, set the head to WriteOnly
            list[0].Status = ReplicaStatus.WriteOnly;

            // one replica chain ? Force to ReadWrite
            if (conf.GetCurrentReplicaChain().Count == 1)
            {
                list[0].Status = ReplicaStatus.ReadWrite;
            }
        }

        private void RepairTable(string tableName, string storageAccountName)
        {
            ReplicatedTableConfiguredTable tableConfig;
            if (!IsConfiguredTable(tableName, out tableConfig))
            {
                return;
            }

            var viewConf = GetView(tableConfig.ViewName);
            if (viewConf.IsEmpty)
            {
                return;
            }

            ReplicaInfo head = viewConf.GetReplicaInfo(0);
            if (head.StorageAccountName != storageAccountName)
            {
                return;
            }

            if (head.Status != ReplicaStatus.WriteOnly)
            {
                return;
            }

            // TODO: table.Repair(viewIdToRecoverFrom);
        }

        // ...

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
