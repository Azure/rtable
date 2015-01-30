//-----------------------------------------------------------------------
// <copyright file="ConfigurationConstants.cs" company="Microsoft">
//    Copyright 2013 Microsoft Corporation
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//      http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// </copyright>
//-----------

namespace Microsoft.WindowsAzure.Storage.RTable.ConfigurationAgentLibrary
{
    public class ConfigurationConstants
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

        public static int LeaseRenewalExpirationWatermark = 5;

        /// <summary>
        /// When LeaseRenewalIntervalInSec has expired since we last renewed the lease, a new renewal is needed
        /// </summary>
        public static int LeaseRenewalIntervalInSec = LeaseDurationInSec - LeaseRenewalExpirationWatermark;

        public static string ConfigurationStoreUpdatingText =
            "The configuration store is in the process of being updated...";

        public static string RTableConfigurationBlobLocationContainerName = "rtableconfiguration";
    }
}
