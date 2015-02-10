// Copyright (c) Microsoft Corporation
// All rights reserved.
//
// The MIT License (MIT)
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN



namespace Microsoft.Azure.Toolkit.Replication.ConfigurationAgent
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
