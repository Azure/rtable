namespace Microsoft.WindowsAzure.Storage.RTable.ConfigurationAgentLibrary
{
    using System.Runtime.Serialization;
    
    [DataContract(Namespace = "http://schemas.microsoft.com/windowsazure")]
    public class ReplicaInfo
    {
        [DataMember(IsRequired = true)]
        public string StorageAccountName { get; set; }

        //Eventually, need a way to NOT pass this in here
        [DataMember(IsRequired = true)]
        public string StorageAccountKey { get; set; }

        [DataMember(IsRequired = true)]
        public long ViewInWhichAddedToChain { get; set; }

        public ReplicaInfo()
        {
            this.ViewInWhichAddedToChain = 1;
        }

        public override string ToString()
        {
            return string.Format("Account Name: {0}, AccountKey: {1}, ViewInWhichAddedToChain: {2}", 
                this.StorageAccountName, 
                "XXXXXX", 
                this.ViewInWhichAddedToChain);
        }
    }
}
