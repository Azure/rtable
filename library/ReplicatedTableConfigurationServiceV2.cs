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
    using System.Threading.Tasks;
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
        public QuorumReadResult RetrieveConfiguration(out ReplicatedTableConfiguration configuration)
        {
            List<string> eTags;

            QuorumReadResult
            result = CloudBlobHelpers.TryReadBlobQuorum(
                                                this.configManager.GetBlobs(),
                                                out configuration,
                                                out eTags,
                                                ReplicatedTableConfiguration.FromJson);

            if (result != QuorumReadResult.Success)
            {
                ReplicatedTableLogger.LogError("Failed to read configuration, result={0}", result);
            }

            return result;
        }

        public List<ReadBlobResult> RetrieveConfiguration(out List<ReplicatedTableConfiguration> configuration)
        {
            List<string> eTagsArray;

            return CloudBlobHelpers.TryReadAllBlobs(
                                                this.configManager.GetBlobs(),
                                                out configuration,
                                                out eTagsArray,
                                                ReplicatedTableConfiguration.FromJson);
        }

        public QuorumWriteResult UpdateConfiguration(ReplicatedTableConfiguration configuration, bool useConditionalUpdate = true)
        {
            if (configuration == null)
            {
                throw new ArgumentNullException("configuration");
            }


            // TODO: LockBlobs(***)


            return UpdateConfigurationInternal(configuration, useConditionalUpdate);
        }

        private QuorumWriteResult UpdateConfigurationInternal(ReplicatedTableConfiguration configuration, bool useConditionalUpdate)
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

            QuorumWriteResult
            result = CloudBlobHelpers.TryWriteBlobQuorum(
                                            this.configManager.GetBlobs(),
                                            configuration,
                                            ReplicatedTableConfiguration.FromJson,
                                            comparer,
                                            ReplicatedTableConfiguration.GenerateNewConfigId);

            if (result == QuorumWriteResult.Success)
            {
                this.configManager.Invalidate();
            }
            else
            {
                ReplicatedTableLogger.LogError("Failed to update configuration, result={0}", result);
            }

            return result;
        }


        /*
         * View/Table APIs
         */
        public View GetView(string viewName)
        {
            return this.configManager.GetView(viewName);
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


        /*
         * Replica management APIs
         */
        public void TurnReplicaOn(string storageAccountName)
        {
            if (string.IsNullOrEmpty(storageAccountName))
            {
                throw new ArgumentNullException("storageAccountName");
            }


            // TODO: LockBlobs(***)


            ReplicatedTableConfiguration configuration = null;

            // - Retrieve configuration ...
            QuorumReadResult readResult = RetrieveConfiguration(out configuration);
            if (readResult != QuorumReadResult.Success)
            {
                ReplicatedTableLogger.LogError("TurnReplicaOn={0}: failed to read configuration, result={1}", storageAccountName, readResult);

                var msg = string.Format("TurnReplicaOn={0}: failed to read configuration, result={1}", storageAccountName, readResult);
                throw new Exception(msg);
            }


            /* - Phase 1:
             *      Move the *replica* to the front and set it to None.
             *      Make the view ReadOnly.
             **/
            foreach (var entry in configuration.viewMap)
            {
                var viewName = entry.Key;
                var viewConf = entry.Value;

                MoveReplicaToFrontAndSetViewToReadOnly(viewName, viewConf, storageAccountName);
            }

            // - Write back configuration ...
            QuorumWriteResult writeResult = UpdateConfigurationInternal(configuration, true);
            if (writeResult != QuorumWriteResult.Success)
            {
                ReplicatedTableLogger.LogError("TurnReplicaOn={0}: failed to update -Phase 1- configuration, result={1}", storageAccountName, writeResult);

                var msg = string.Format("TurnReplicaOn={0}: failed to update -Phase 1- configuration, result={1}", storageAccountName, writeResult);
                throw new Exception(msg);
            }

            /**
             * Chain is such: [None] -> [RO] -> ... -> [RO]
             **/


            // - Wait for L + CF to make sure no pending transaction working on old views
            Thread.Sleep(TimeSpan.FromSeconds(Constants.LeaseDurationInSec + Constants.ClockFactorInSec));


            /* - Phase 2:
             *      Set the *replica* (head) to WriteOnly.
             *      Set other active replicas to ReadWrite.
             **/
            foreach (var entry in configuration.viewMap)
            {
                var viewName = entry.Key;
                var viewConf = entry.Value;

                SetReplicaToWriteOnly(viewName, viewConf, storageAccountName);
            }

            // - Write back configuration ...
            //   Note: configuration *Id* has changed since previous updated So disable conditional update
            writeResult = UpdateConfigurationInternal(configuration, false);
            if (writeResult != QuorumWriteResult.Success)
            {
                ReplicatedTableLogger.LogError("TurnReplicaOn={0}: failed to update -Phase 2- configuration, result={1}", storageAccountName, writeResult);

                var msg = string.Format("TurnReplicaOn={0}: failed to update -Phase 2- configuration, result={1}", storageAccountName, writeResult);
                throw new Exception(msg);
            }

            /**
             * Chain is such: [W] -> [RW] -> ... -> [RW]
             **/


            /* - Phase 3:
             *      Call Repair API
             **/
            // TODO: ...
            // TODO: if chains is such :   [W] -> [None] -> ... -> [None]   will Repair work ???

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
            QuorumReadResult readResult = RetrieveConfiguration(out configuration);
            if (readResult != QuorumReadResult.Success)
            {
                ReplicatedTableLogger.LogError("TurnReplicaOff={0}: failed to read configuration, result={1}", storageAccountName, readResult);

                var msg = string.Format("TurnReplicaOff={0}: failed to read configuration, result={1}", storageAccountName, readResult);
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
            QuorumWriteResult writeResult = UpdateConfigurationInternal(configuration, true);
            if (writeResult != QuorumWriteResult.Success)
            {
                ReplicatedTableLogger.LogError("TurnReplicaOff={0}: failed to update configuration, result={1}", storageAccountName, writeResult);

                var msg = string.Format("TurnReplicaOff={0}: failed to update configuration, result={1}", storageAccountName, writeResult);
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
                    ReplicatedTableLogger.LogError("View:\'{0}\' : can't set a WriteOnly replica to ReadOnly !!!", viewName);

                    var msg = string.Format("View:\'{0}\' : can't set a WriteOnly replica to ReadOnly !!!", viewName);
                    throw new Exception(msg);
                }

                replica.Status = ReplicaStatus.ReadOnly;
            }
        }

        private static void SetReplicaToWriteOnly(string viewName, ReplicatedTableConfigurationStore conf, string storageAccountName)
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
