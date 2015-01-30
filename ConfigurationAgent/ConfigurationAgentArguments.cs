//-----------------------------------------------------------------------
// <copyright file="ConfigurationAgentArguments.cs" company="Microsoft">
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

namespace Microsoft.WindowsAzure.Storage.RTable.ConfigurationAgent
{
    class ConfigurationAgentArguments
    {
        public ConfigurationAgentArguments()
        {
            this.configStoreAccountName = new[] {"a", "b", "c"};
            this.configStoreAccountKey = new[] { "a", "b", "c" };
            this.replicaChainAccountName = new[] { "a", "b", "c" };
            this.replicaChainAccountKey = new[] { "a", "b", "c" };
            this.configLocation = string.Empty;
            this.readViewHeadIndex = 0;
            this.convertXStoreTableMode = false;
        }

        [Argument(ArgumentType.MultipleUnique, ShortName = "csa", HelpText = "Storage account name where the configuration store is located")]
        public string[] configStoreAccountName;

        [Argument(ArgumentType.MultipleUnique, ShortName = "csk", HelpText = "Storage account key where the configuration store is located")]
        public string[] configStoreAccountKey;

        [Argument(ArgumentType.MultipleUnique, ShortName = "rca", HelpText = "Storage account name of a replica in the chain")]
        public string[] replicaChainAccountName;

        [Argument(ArgumentType.MultipleUnique, ShortName = "rck", HelpText = "Storage account key of a replica in the chain")]
        public string[] replicaChainAccountKey;

        [Argument(ArgumentType.Required, ShortName = "rhi", HelpText = "The index of the head in the read view")]
        public int readViewHeadIndex;

        [Argument(ArgumentType.Required, ShortName = "convert", HelpText = "Convert XStore Table into RTable")]
        public bool convertXStoreTableMode;

        [Argument(ArgumentType.AtMostOnce, ShortName = "cl", HelpText = "Storage account key of a replica in the chain")]
        public string configLocation;
    }
}
