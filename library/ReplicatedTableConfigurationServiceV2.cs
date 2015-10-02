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
    using System.Security;
    using Microsoft.WindowsAzure.Storage.Blob;

    public class ReplicatedTableConfigurationServiceV2 : IDisposable
    {
        private bool disposed = false;
        private readonly ReplicatedTableConfigurationManager configManager;

        public ReplicatedTableConfigurationServiceV2(List<ConfigurationStoreLocationInfo> blobLocations, Dictionary<string, SecureString> connectionStringMap, bool useHttps, int lockTimeoutInSeconds = 0)
        {
            this.configManager = new ReplicatedTableConfigurationManager(blobLocations, connectionStringMap, useHttps, lockTimeoutInSeconds, new ReplicatedTableConfigurationParser());
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

            return UpdateConfigurationInternal(configuration, useConditionalUpdate);
        }

        public ReplicatedTableQuorumWriteResult UploadConfigurationToBlobs(List<int> blobIndexes, ReplicatedTableConfiguration configuration)
        {
            if (blobIndexes == null || !blobIndexes.Any())
            {
                throw new ArgumentNullException("blobIndexes");
            }

            if (configuration == null)
            {
                throw new ArgumentNullException("configuration");
            }

            List<CloudBlockBlob> blobs = new List<CloudBlockBlob>();
            foreach (var blobIndex in blobIndexes)
            {
                if (blobIndex < this.configManager.GetBlobs().Count)
                {
                    blobs.Add(this.configManager.GetBlobs()[blobIndex]);
                    continue;
                }

                var msg = string.Format("blobIndex={0} >= BlobCount={1}", blobIndex, this.configManager.GetBlobs().Count);

                ReplicatedTableLogger.LogError(msg);
                throw new Exception(msg);
            }

            SanitizeConfiguration(configuration);

            // - Upload to blobs ...
            ReplicatedTableQuorumWriteResult result = CloudBlobHelpers.TryUploadBlobs(blobs, configuration);

            this.configManager.Invalidate();

            if (result.Code != ReplicatedTableQuorumWriteCode.Success)
            {
                ReplicatedTableLogger.LogError("Failed to upload configuration to blobs, \n{0}", result.ToString());
            }

            return result;
        }

        private ReplicatedTableQuorumWriteResult UpdateConfigurationInternal(ReplicatedTableConfiguration configuration, bool useConditionalUpdate)
        {
            SanitizeConfiguration(configuration);

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

        private void SanitizeConfiguration(ReplicatedTableConfiguration configuration)
        {
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

                foreach (var replica in viewConf.GetCurrentReplicaChain())
                {
                    // We are introducing 1 or more replicas at the head.
                    // For each such replica, update the view id in which it was added to the write view of the chain
                    if (replica.IsWriteOnly())
                    {
                        replica.ViewInWhichAddedToChain = viewId;
                        continue;
                    }

                    // stop at the first Readable replica
                    break;
                }
            }
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
        public void TurnReplicaOn(string storageAccountName, List<string> tablesToRepair, out List<ReplicatedTableRepairResult> failures)
        {
            if (string.IsNullOrEmpty(storageAccountName))
            {
                throw new ArgumentNullException("storageAccountName");
            }

            if (tablesToRepair == null)
            {
                throw new ArgumentNullException("tablesToRepair");
            }

            ReplicatedTableConfiguration configuration = null;
            failures = new List<ReplicatedTableRepairResult>();

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

            // - Write back configuration, refresh its Id with the new one, and then validate it is loaded now
            SaveConfigAndRefreshItsIdAndValidateIsLoaded(configuration, "Phase 1");

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

            // - Write back configuration, refresh its Id with the new one, and then validate it is loaded now
            SaveConfigAndRefreshItsIdAndValidateIsLoaded(configuration, "Phase 2");

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
                ReplicatedTableRepairResult result = RepairTable(tableName, storageAccountName, configuration);
                if (result.Code != ReplicatedTableRepairCode.Error)
                {
                    ReplicatedTableLogger.LogInformational(result.ToString());
                    continue;
                }

                ReplicatedTableLogger.LogError(result.ToString());

                failures.Add(result);
            }

            #endregion


            /* - Phase 4:
             *      Set the *replica* (head) to ReadWrite.
             **/
            #region Phase 4

            foreach (var entry in configuration.viewMap)
            {
                var viewName = entry.Key;
                var viewConf = entry.Value;

                if (failures.Any(r => r.ViewName == viewName))
                {
                    // skip
                    continue;
                }

                EnableReadWriteOnReplicas(viewName, viewConf, storageAccountName);
            }

            // - Write back configuration, refresh its Id with the new one, and then validate it is loaded now
            SaveConfigAndRefreshItsIdAndValidateIsLoaded(configuration, "Phase 4");

            #endregion
            /**
             * Chain is such: [RW] -> [RW] -> ... -> [RW] if all configured table repaired
             *            or:  [W] -> [RW] -> ... -> [RW] if at least one configured table failed repair !
             **/
        }

        public void TurnReplicaOff(string storageAccountName)
        {
            if (string.IsNullOrEmpty(storageAccountName))
            {
                throw new ArgumentNullException("storageAccountName");
            }

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

                if (!foundReplicas.Any())
                {
                    continue;
                }

                foreach (var replica in foundReplicas)
                {
                    replica.Status = ReplicaStatus.None;
                    replica.ViewWhenTurnedOff = viewConf.ViewId;
                }

                // Update view id
                viewConf.ViewId++;
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

            // Update view id
            conf.ViewId++;
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

            // Update view id
            conf.ViewId++;
        }

        private ReplicatedTableRepairResult RepairTable(string tableName, string storageAccountName, ReplicatedTableConfiguration configuration)
        {
            string viewName = "";

            try
            {
                ReplicatedTableConfiguredTable tableConfig;
                if (!configuration.IsConfiguredTable(tableName, out tableConfig))
                {
                    return new ReplicatedTableRepairResult(ReplicatedTableRepairCode.NotConfiguredTable, tableName);
                }

                viewName = tableConfig.ViewName;

                List<ReplicaInfo> list = configuration.GetView(viewName).ReplicaChain;
                if (!list.Any() ||
                    list[0].StorageAccountName != storageAccountName)
                {
                    return new ReplicatedTableRepairResult(ReplicatedTableRepairCode.NotImpactedTable, tableName, viewName, storageAccountName);
                }

                ReplicaInfo head = list[0];
                if (head.Status != ReplicaStatus.WriteOnly)
                {
                    return new ReplicatedTableRepairResult(ReplicatedTableRepairCode.StableTable, tableName, viewName, storageAccountName);
                }

                int viewIdToRecoverFrom = (int)head.ViewWhenTurnedOff;

                ReplicatedTableLogger.LogInformational("RepairTable={0}, View={1}, StorageAccountName={2}, from viewId={3} ...",
                                                        tableName,
                                                        viewName,
                                                        storageAccountName,
                                                        viewIdToRecoverFrom);
                // Repairing ...
                ReconfigurationStatus status = new ReplicatedTable(tableName, this).RepairTable(viewIdToRecoverFrom, null);

                ReplicatedTableLogger.LogInformational("RepairTable={0}, View={1}, StorageAccountName={2}, from viewId={3} => Status={4}",
                                                        tableName,
                                                        viewName,
                                                        storageAccountName,
                                                        viewIdToRecoverFrom,
                                                        status);

                if (status == ReconfigurationStatus.SUCCESS)
                {
                    return new ReplicatedTableRepairResult(ReplicatedTableRepairCode.Success, tableName, viewName, storageAccountName);
                }

                // Failure!
                return new ReplicatedTableRepairResult(ReplicatedTableRepairCode.Error, tableName, viewName, storageAccountName)
                {
                    Status = status,
                };
            }
            catch (Exception ex)
            {
                return new ReplicatedTableRepairResult(ReplicatedTableRepairCode.Error, tableName, viewName, storageAccountName)
                {
                    Status = ReconfigurationStatus.FAILURE,
                    Message = ex.ToString(),
                };
            }
        }

        private static void EnableReadWriteOnReplicas(string viewName, ReplicatedTableConfigurationStore conf, string storageAccountName)
        {
            List<ReplicaInfo> list = conf.ReplicaChain;

            if (!list.Any() ||
                list[0].StorageAccountName != storageAccountName ||
                list[0].Status != ReplicaStatus.WriteOnly)
            {
                return;
            }

            list[0].Status = ReplicaStatus.ReadWrite;

            // Update view id
            conf.ViewId++;
        }

        private void SaveConfigAndRefreshItsIdAndValidateIsLoaded(ReplicatedTableConfiguration configuration, string iteration)
        {
            string msg = "";

            ReplicatedTableQuorumWriteResult writeResult = UpdateConfigurationInternal(configuration, true);
            if (writeResult.Code != ReplicatedTableQuorumWriteCode.Success)
            {
                msg = string.Format("{0} : Failed to update configuration, \n{1}", iteration, writeResult.ToString());

                ReplicatedTableLogger.LogError(msg);
                throw new Exception(msg);
            }

            // Update config with new Id
            configuration.Id = new Guid(writeResult.Message);

            // - Confirm the new config is the current loaded into the RTable config manager
            if (configuration.Id == this.configManager.GetCurrentRunningConfigId())
            {
                return;
            }

            msg = string.Format("{0} : ConfigId({1}) != currently running configurationId({2})",
                                iteration,
                                configuration.Id,
                                this.configManager.GetCurrentRunningConfigId());

            ReplicatedTableLogger.LogError(msg);
            throw new Exception(msg);
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
