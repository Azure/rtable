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
    using global::Azure.Storage.Blobs;
    using global::Azure.Core;

    public class ReplicatedTableConfigurationServiceV2 : IDisposable
    {
        private bool disposed = false;
        private readonly ReplicatedTableConfigurationManager configManager;

        public ReplicatedTableConfigurationServiceV2(
            List<ConfigurationStoreLocationInfo> blobLocations,
            Dictionary<string, SecureString> connectionStringMap,
            bool useHttps,
            int lockTimeoutInSeconds = 0)
        {
            this.configManager = new ReplicatedTableConfigurationManager(blobLocations, connectionStringMap, useHttps, lockTimeoutInSeconds, new ReplicatedTableConfigurationParser());
            this.configManager.StartMonitor();
        }

        public ReplicatedTableConfigurationServiceV2(
            List<ConfigurationStoreLocationInfo> blobLocations,
            TokenCredential blobLocationsStorageToken,
            Dictionary<string, TokenCredential> replicaStorageTokenMap,
            bool useHttps,
            string storageEndpointDomain,
            int lockTimeoutInSeconds = 0)
        {
            this.configManager = new ReplicatedTableConfigurationManager(blobLocations, blobLocationsStorageToken, replicaStorageTokenMap, useHttps, storageEndpointDomain, lockTimeoutInSeconds, new ReplicatedTableConfigurationParser());
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

            List<BlobClient> blobs = new List<BlobClient>();
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

                View currentView = GetView(viewName);
                viewConf.SanitizeWithCurrentView(currentView);
            }
        }

        public void ConfigurationChangeNotification()
        {
            this.configManager.Invalidate();
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

            if (config.IsAnyViewNullOrEmpty())
            {
                return false;
            }

            configuredTable = config;
            return true;
        }

        public View GetTableView(string tableName, string viewToUse = null)
        {
            ReplicatedTableConfiguredTable config;
            if (IsConfiguredTable(tableName, out config))
            {
                // Use table default view
                if (string.IsNullOrEmpty(viewToUse))
                {
                    viewToUse = config.ViewName;
                }

                if (config.IsViewReferenced(viewToUse))
                {
                    return GetView(viewToUse);
                }
            }

            var msg = string.Format("Table={0}: is not configured or ViewToUse={1} is not referenced!", tableName, viewToUse);
            ReplicatedTableLogger.LogError(msg);
            throw new Exception(msg);
        }

        public bool IsTableViewStable(string tableName, string viewToUse = null)
        {
            ReplicatedTableConfiguredTable config;
            if (IsConfiguredTable(tableName, out config))
            {
                // Use table default view
                if (string.IsNullOrEmpty(viewToUse))
                {
                    viewToUse = config.ViewName;
                }

                if (config.IsViewReferenced(viewToUse))
                {
                    return IsViewStable(viewToUse);
                }
            }

            var msg = string.Format("Table={0}: is not configured or ViewToUse={1} is not referenced!", tableName, viewToUse);
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

            var msg = string.Format("Table={0}: is not configured!", tableName);
            ReplicatedTableLogger.LogError(msg);
            throw new Exception(msg);
        }

        public bool IsIntrumentationEnabled()
        {
            return this.configManager.IsIntrumenationEnabled();
        }

        public bool IsIgnoreHigherViewIdRows()
        {
            return this.configManager.IsIgnoreHigherViewIdRows();
        }

        /*
         * Replica management APIs
         */
        /// <summary>
        /// Update connection strings before uploading new RTable config to blob.
        /// Make sure new connection strings are an uper-set of previous one - MBB -
        /// </summary>
        public void UpdateConnectionStrings(Dictionary<string, SecureString> connectionStringMap)
        {
            this.configManager.ConnectionStrings = connectionStringMap;
        }

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

            configuration.MoveReplicaToHeadAndSetViewToReadOnly(storageAccountName);

            // - Write back configuration, refresh its Id with the new one,
            //   but don't validate it is loaded bcz if all views of the config are empty, the config won't be refreshed by RefreshReadAndWriteViewsFromBlobs() thread!
            SaveConfigAndRefreshItsIdAndValidateIsLoaded(configuration, "Phase 1", false);

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

            configuration.EnableWriteOnReplicas(storageAccountName);

            // - Write back configuration, refresh its Id with the new one,
            //   and then validate it is loaded now (it has to be working since next Phase is "Repair")
            SaveConfigAndRefreshItsIdAndValidateIsLoaded(configuration, "Phase 2");

            #endregion
            /**
             * Chain is such: [W] -> [RW] -> ... -> [RW]
             *            or: [RW] -> [None] -> ... -> [None]
             **/


            // To be safe:
            // - Wait for L + CF to make sure no pending transaction working on old views
            Thread.Sleep(TimeSpan.FromSeconds(Constants.LeaseDurationInSec + Constants.ClockFactorInSec));


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

                // List of tables (and corresponding views) failed to repair!
                failures.Add(result);
            }

            #endregion


            /* - Phase 4:
             *      Set the *replica* (head) to ReadWrite.
             **/
            #region Phase 4

            // TODO: re-evaluate if we will support this API for partitioned tables ?
            configuration.EnableReadWriteOnReplicas(storageAccountName, failures.Select(r => r.ViewName).ToList());

            // - Write back configuration, refresh its Id with the new one,
            //   and then validate it is loaded now (i.e. it is a working config)
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

            // - Update all views ...
            configuration.TurnReplicaOff(storageAccountName);

            // - Write back configuration ...
            ReplicatedTableQuorumWriteResult writeResult = UpdateConfigurationInternal(configuration, true);
            if (writeResult.Code != ReplicatedTableQuorumWriteCode.Success)
            {
                var msg = string.Format("TurnReplicaOff={0}: failed to update configuration, \n{1}", storageAccountName, writeResult.ToString());

                ReplicatedTableLogger.LogError(msg);
                throw new Exception(msg);
            }
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

                // TODO: re-evaluate how we would Repair a partitioned table ?
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

        private void SaveConfigAndRefreshItsIdAndValidateIsLoaded(ReplicatedTableConfiguration configuration, string iteration, bool validateConfigIsLoaded = true)
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

            // do we need to validate if the config is loaded by the config manager ?
            if (!validateConfigIsLoaded)
            {
                return;
            }

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
