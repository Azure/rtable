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
    using global::Azure.Data.Tables;
    using global::Azure.Storage.Blobs;

    internal class ReplicatedTableConfigurationStoreParser : IReplicatedTableConfigurationParser
    {
        public const string DefaultViewName = "DefaultViewName";
        public const string AllTables = "AllTables";

        public List<View> ParseBlob(
                                List<BlobClient> blobs,
                                Func<ReplicaInfo, TableServiceClient> tableClientCreationStrategy,
                                out List<ReplicatedTableConfiguredTable> tableConfigList,
                                out int leaseDuration,
                                out Guid configId,
                                out bool instrumentation,
                                out bool ignoreHigherViewIdRows)
        {
            tableConfigList = null;
            leaseDuration = 0;
            configId = Guid.Empty;
            instrumentation = false;
            ignoreHigherViewIdRows = false;

            ReplicatedTableConfigurationStore configurationStore;
            List<string> eTags;

            ReplicatedTableQuorumReadResult result = CloudBlobHelpers.TryReadBlobQuorum(
                                                                    blobs,
                                                                    out configurationStore,
                                                                    out eTags,
                                                                    JsonStore<ReplicatedTableConfigurationStore>.Deserialize);
            if (result.Code != ReplicatedTableQuorumReadCode.Success)
            {
                ReplicatedTableLogger.LogError("Unable to refresh view, \n{0}", result.ToString());
                return null;
            }


            /**
             * View:
             */
            var view = View.InitFromConfigVer1(DefaultViewName, configurationStore, tableClientCreationStrategy);

            if (view.ViewId <= 0)
            {
                ReplicatedTableLogger.LogError("ViewId={0} is invalid. Must be >= 1.", view.ViewId);
                return null;
            }

            if (view.IsEmpty)
            {
                ReplicatedTableLogger.LogError("ViewName={0} is empty, skipping ...", view.Name);
                return null;
            }


            /**
             * Tables:
             */
            tableConfigList = new List<ReplicatedTableConfiguredTable>
            {
                new ReplicatedTableConfiguredTable
                {
                    TableName = AllTables,
                    ViewName = DefaultViewName,
                    ConvertToRTable = configurationStore.ConvertXStoreTableMode,
                }
            };


            // - lease duration
            leaseDuration = configurationStore.LeaseDuration;

            // - Instrumentation
            instrumentation = configurationStore.Instrumentation;

            // - IgnoreHigherViewIdRows >>> Not supported in RTable V1
            ignoreHigherViewIdRows = false;

            return new List<View> { view };
        }
    }
}