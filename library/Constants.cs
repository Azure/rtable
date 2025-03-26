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
    public class Constants
    {
        /// <summary>
        /// The duration for which the lease on the current configuration is valid. 
        /// </summary>
        public static int LeaseDurationInSec = 60;

        /// <summary>
        /// The clock factor takes into account that two machine's clock might count up or down at different speeds. This is also
        /// known as time dilation. This is an upper bound on time delay introduced due to the clock factor
        /// </summary>
        public static int ClockFactorInSec = 5;

        public static int MinimumLeaseRenewalInterval = 5;

        /// <summary>
        /// When LeaseRenewalIntervalInSec has expired since we last renewed the lease, a new renewal is needed
        /// </summary>
        public static int LeaseRenewalIntervalInSec = LeaseDurationInSec / 2;

        public static string ConfigurationStoreUpdatingText =
            "The configuration store is in the process of being updated...";

        public static string RTableConfigurationBlobLocationContainerName = "rtableconfiguration";

        public const string LogSourceName = "Microsoft.Azure.Toolkit.Replication.ReplicatedTable";

        /// <summary>
        /// When an entity is locked by a client and that client did not unlock the entity, other clients are free to unlock it afer this much time has elasped.
        /// </summary>
        public static int LockTimeoutInSeconds = 60;

        /// <summary>
        /// Time after which we check if we have a Quorum or no ?
        /// </summary>
        public static int QuorumPollingInMilliSeconds = 50;

        public static string ShortConnectioStringTemplate =
            @"DefaultEndpointsProtocol={0};AccountName={1};AccountKey={2};";

        public static string LongConnectioStringTemplate =
            @"DefaultEndpointsProtocol={0};AccountName={1};AccountKey={2};BlobEndpoint={0}://{1}.blob.{3};QueueEndpoint={0}://{1}.queue.{3};TableEndpoint={0}://{1}.table.{3}";

        public static string BlobStorageEndpointTemplate = @"{0}://{1}.blob.{2}";
        public static string TableStorageEndpointTemplate = @"{0}://{1}.table.{2}";
    }
}
