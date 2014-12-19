namespace Microsoft.WindowsAzure.Storage.RTable.ConfigurationAgentLibrary
{
    using System;
    using System.Runtime.Serialization;

    [DataContract(Namespace = "http://schemas.microsoft.com/windowsazure")]
    public class ConfigurationStore
    {
        public ConfigurationStore()
        {
            this.LeaseDuration = ConfigurationConstants.LeaseDurationInSec;
            this.Timestamp = DateTime.UtcNow;
            this.ViewId = 1; // minimum ViewId is 1.
        }

        [DataMember(IsRequired = true)]
        public long ViewId { get; set; }

        [DataMember(IsRequired = true)]
        public int LeaseDuration { get; set; }

        [DataMember(IsRequired = true)]
        public DateTime Timestamp { get; set; }
    }
}
