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
    using System;
    using System.Collections.Generic;
    using Microsoft.Azure.Toolkit.Replication;
    
    class Program
    {
        public static void Main(string[] args)
        {
            ConfigurationAgentArguments parsedArguments = new ConfigurationAgentArguments();

            List<ConfigurationStoreLocationInfo> configLocationInfo = new List<ConfigurationStoreLocationInfo>();
            List<ReplicaInfo> replicaChain = new List<ReplicaInfo>();
            if (Parser.ParseArgumentsWithUsage(args, parsedArguments))
            {
                Console.WriteLine("The storage accounts for configuration store are:");
                
                for(int i = 0; i < parsedArguments.configStoreAccountName.Length; i++)
                {
                    Console.WriteLine("Account Name: {0}, Account Key: {1}", parsedArguments.configStoreAccountName[i], parsedArguments.configStoreAccountKey[i]);
                    configLocationInfo.Add(new ConfigurationStoreLocationInfo()
                    {
                        StorageAccountName = parsedArguments.configStoreAccountName[i],
                        StorageAccountKey = parsedArguments.configStoreAccountKey[i],
                        BlobPath = Constants.RTableConfigurationBlobLocationContainerName + "/" + parsedArguments.configLocation
                    });
                }

                Console.WriteLine("The Replica Chain is:");
                for (int i = 0; i < parsedArguments.replicaChainAccountName.Length; i++)
                {
                    if (i != parsedArguments.replicaChainAccountName.Length - 1)
                    {
                        Console.Write("{0} -> ", parsedArguments.replicaChainAccountName[i]);
                    }
                    else
                    {
                        Console.WriteLine("{0}", parsedArguments.replicaChainAccountName[i]);
                    }

                    replicaChain.Add(new ReplicaInfo()
                    {
                        StorageAccountName = parsedArguments.replicaChainAccountName[i],
                        StorageAccountKey = parsedArguments.replicaChainAccountKey[i]
                    });
                }

                Console.WriteLine("The head index in read view is : {0}", parsedArguments.readViewHeadIndex);

                if (parsedArguments.readViewHeadIndex == 0)
                {
                    Console.WriteLine("The read and write views are identical.");                    
                }
                else
                {
                    Console.WriteLine("The read and write views are different.");                    
                }
            }

            Console.WriteLine("Updating the configuration store...");

            ReplicatedTableConfigurationService agent = new ReplicatedTableConfigurationService(
                configLocationInfo, 
                 false);

            agent.UpdateConfiguration(replicaChain, parsedArguments.readViewHeadIndex, parsedArguments.convertXStoreTableMode);
            
            Console.WriteLine("Done updating the configuration store");

            Console.WriteLine("New Read View is:");
            View readView = agent.GetReadView();
            for(int i = 0; i < readView.Chain.Count; i++)
            {
                Console.WriteLine("{0} -> ", readView.GetReplicaInfo(i));
            }

            Console.WriteLine("New Write View is:");
            View writeView = agent.GetWriteView();
            for (int i = 0; i < writeView.Chain.Count; i++)
            {
                Console.Write("{0} -> ", writeView.GetReplicaInfo(i));
            }
        }
    }
}
