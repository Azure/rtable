//-----------------------------------------------------------------------
// <copyright file="ConfigurationStoreLocationInfo.cs" company="Microsoft">
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
