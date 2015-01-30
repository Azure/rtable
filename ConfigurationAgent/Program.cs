//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Microsoft">
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
    using System;
    using System.Collections.Generic;
    using Microsoft.WindowsAzure.Storage.RTable.ConfigurationAgentLibrary;
    
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
                        BlobPath = ConfigurationConstants.RTableConfigurationBlobLocationContainerName + "/" + parsedArguments.configLocation
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

            RTableConfigurationService agent = new RTableConfigurationService(
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
