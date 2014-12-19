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
