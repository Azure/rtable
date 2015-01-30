//-----------------------------------------------------------------------
// <copyright file="CommandLineArgument.cs" company="Microsoft">
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
