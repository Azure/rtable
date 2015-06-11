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
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.Table;

    internal class ReplicatedTableConfigurationStoreParser : IReplicatedTableConfigurationParser
    {
        public const string DefaultChainName = "DefaultChainName";
        public const string AllTables = "AllTables";

        /// <summary>
        /// Parses the RTable configuration blobs.
        /// Returns the list of chains, the list of configured tables and the lease duration.
        /// If null is returned, then the value of tableConfigList/leaseDuration are not relevant.
        /// </summary>
        /// <param name="blobs"></param>
        /// <param name="useHttps"></param>
        /// <param name="tableConfigList"></param>
        /// <param name="leaseDuration"></param>
        /// <returns></returns>
        public List<RTableChainViews> ParseBlob(
                                                List<CloudBlockBlob> blobs,
                                                bool useHttps,
                                                out List<RTableConfiguredTable> tableConfigList,
                                                out int leaseDuration
                                                )
        {
            tableConfigList = null;
            leaseDuration = 0;

            ReplicatedTableConfigurationStore configurationStore;
            List<string> eTags;

            if (!CloudBlobHelpers.TryReadBlobQuorum(blobs, out configurationStore, out eTags))
            {
                ReplicatedTableLogger.LogError("Unable to refresh view.");
                return null;
            }

            if (configurationStore.ViewId <= 0)
            {
                ReplicatedTableLogger.LogError("ViewId={0} is invalid. Must be >= 1.", configurationStore.ViewId);
                return null;
            }

            var chain = new RTableChainViews(DefaultChainName)
            {
                RefreshTime = DateTime.UtcNow
            };

            for (int i = 0; i < configurationStore.ReplicaChain.Count; i++)
            {
                ReplicaInfo replica = configurationStore.ReplicaChain[i];
                CloudTableClient tableClient = ReplicatedTableConfigurationManager.GetTableClientForReplica(replica,
                    useHttps);

                if (replica != null && tableClient != null)
                {
                    //Update the write view always
                    chain.WriteView.Chain.Add(new Tuple<ReplicaInfo, CloudTableClient>(replica, tableClient));

                    //Update the read view only for replicas part of the view
                    if (i >= configurationStore.ReadViewHeadIndex)
                    {
                        chain.ReadView.Chain.Add(new Tuple<ReplicaInfo, CloudTableClient>(replica, tableClient));
                    }
                }
            }

            if (chain.ReadView.IsEmpty || chain.WriteView.IsEmpty)
            {
                return null;
            }

            // - chain:
            chain.ReadView.ViewId = chain.WriteView.ViewId = configurationStore.ViewId;
            chain.WriteView.ReadHeadIndex = configurationStore.ReadViewHeadIndex;

            // - configured table:
            tableConfigList = new List<RTableConfiguredTable>
            {
                new RTableConfiguredTable
                {
                    TableName = AllTables,
                    ChainName = DefaultChainName,
                    ConvertXStoreTableMode = configurationStore.ConvertXStoreTableMode,
                }
            };

            // - lease duration
            leaseDuration = configurationStore.LeaseDuration;

            return new List<RTableChainViews> {chain};
        }
    }
}