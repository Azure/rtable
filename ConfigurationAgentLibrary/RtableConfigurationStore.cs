namespace Microsoft.WindowsAzure.Storage.RTable.ConfigurationAgentLibrary
{
    using System.Collections.Generic;
    using System.Runtime.Serialization;

    [DataContract(Namespace = "http://schemas.microsoft.com/windowsazure")]
    public class RTableConfigurationStore : ConfigurationStore
    {
        public RTableConfigurationStore()
            : base()
        {
            this.ReplicaChain = new List<ReplicaInfo>();
        }

        [DataMember(IsRequired = true)]
        public List<ReplicaInfo> ReplicaChain { get; set; }

        [DataMember(IsRequired = true)]
        public int ReadViewHeadIndex { get; set; }

        [DataMember(IsRequired = false)]
        public bool ConvertXStoreTableMode { get; set; }

    }
}
