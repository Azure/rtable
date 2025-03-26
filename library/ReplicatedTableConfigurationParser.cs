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
    using global::Azure.Data.Tables;
    using global::Azure.Storage.Blobs;

    internal class ReplicatedTableConfigurationParser : IReplicatedTableConfigurationParser
    {
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

            ReplicatedTableConfiguration configuration;
            List<string> eTags;

            ReplicatedTableQuorumReadResult result = CloudBlobHelpers.TryReadBlobQuorumFast(
                                                                    blobs,
                                                                    out configuration,
                                                                    out eTags,
                                                                    ReplicatedTableConfiguration.FromJson);
            if (result.Code != ReplicatedTableQuorumReadCode.Success)
            {
                ReplicatedTableLogger.LogError("Unable to refresh views, \n{0}", result.ToString());
                return null;
            }


            /**
             * Views:
             */
            var viewList = new List<View>();

            foreach (var entry in configuration.viewMap)
            {
                ReplicatedTableConfigurationStore configurationStore = entry.Value;

                var view = View.InitFromConfigVer2(entry.Key, configurationStore, tableClientCreationStrategy);

                if (view.ViewId <= 0)
                {
                    ReplicatedTableLogger.LogError("ViewId={0} of  ViewName={1} is invalid. Must be >= 1.", view.ViewId, view.Name);
                    continue;
                }

                if (view.IsEmpty)
                {
                    ReplicatedTableLogger.LogError("ViewName={0} is empty, skipping ...", view.Name);
                    continue;
                }

                // - ERROR!
                if (view.ReadHeadIndex > view.TailIndex)
                {
                    ReplicatedTableLogger.LogError("ReadHeadIndex={0} of  ViewName={1} is out of range. Must be <= {2}", view.ReadHeadIndex, view.Name, view.TailIndex);
                    continue;
                }

                viewList.Add(view);
            }

            if (!viewList.Any())
            {
                ReplicatedTableLogger.LogError("Config has no active Views !");
                return null;
            }


            /**
             * Tables:
             */
            tableConfigList = configuration.tableList.ToList();


            // - lease duration
            leaseDuration = configuration.LeaseDuration;

            // - ConfigId
            configId = configuration.GetConfigId();

            // - Instrumentation
            instrumentation = configuration.GetInstrumentationFlag();

            // - IgnoreHigherViewIdRows
            ignoreHigherViewIdRows = configuration.IsIgnoreHigherViewIdRows();

            return viewList;
        }
    }
}