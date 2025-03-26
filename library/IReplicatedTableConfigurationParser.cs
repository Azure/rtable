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

    internal interface IReplicatedTableConfigurationParser
    {
        /// <summary>
        /// Parses the RTable configuration blobs.
        /// Returns the list of views, the list of configured tables and the lease duration.
        /// If null is returned, then the value of tableConfigList/leaseDuration are not relevant.
        /// </summary>
        /// <param name="blobs"></param>
        /// <param name="TableClientCreationStrategy"></param>
        /// <param name="tableConfigList"></param>
        /// <param name="leaseDuration"></param>
        /// <param name="configId"></param>
        /// <param name="instrumentation"></param>
        /// <param name="ignoreHigherViewIdRows"></param>
        /// <returns></returns>
        List<View> ParseBlob(
                        List<BlobClient> blobs,
                        Func<ReplicaInfo, TableServiceClient> TableClientCreationStrategy,
                        out List<ReplicatedTableConfiguredTable> tableConfigList,
                        out int leaseDuration,
                        out Guid configId,
                        out bool instrumentation,
                        out bool ignoreHigherViewIdRows);
    }
}