using System.Data.Common;

namespace Microsoft.WindowsAzure.Storage.RTable.ConfigurationAgentLibrary
{
    using System;
    using System.Runtime.Serialization;

    [DataContract(Namespace = "http://schemas.microsoft.com/windowsazure")]
    public class ConfigurationStoreLocationInfo
    {
        [DataMember(IsRequired = true)]
        public string StorageAccountName { get; set; }

        //Eventually, need a way to NOT pass this in here
        [DataMember(IsRequired = true)]
        public string StorageAccountKey { get; set; }

        [DataMember(IsRequired = true)]
        public string BlobPath { get; set; }

        public ConfigurationStoreLocationInfo()
        {
            this.StorageAccountName = string.Empty;
            this.StorageAccountKey = string.Empty;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="storageAccountConnectionString"></param>
        /// <exception cref="IndexOutOfRangeException"></exception>
        public ConfigurationStoreLocationInfo(string storageAccountConnectionString)
        {
            string[] accountNameSeparator1 = new string[] { "AccountName=" };
            string[] accountNameSeparator2 = new string[] { ";AccountKey" };
            string[] accountKeySeparator = new string[] { "AccountKey=" };

            string accountNameSuffix = storageAccountConnectionString.Split(accountNameSeparator1, StringSplitOptions.None)[1];
            this.StorageAccountName = accountNameSuffix.Split(accountNameSeparator2, StringSplitOptions.None)[0];
            this.StorageAccountKey = storageAccountConnectionString.Split(accountKeySeparator, StringSplitOptions.None)[1];
        }
    }
}
