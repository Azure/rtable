// azure-rtable ver. 0.9
//
// Copyright (c) Microsoft Corporation
//
// All rights reserved.
//
// MIT License
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files
// (the ""Software""), to deal in the Software without restriction, including without limitation the rights to use, copy, modify,
// merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished
// to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
// FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


namespace Microsoft.Azure.Toolkit.Replication
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;

    [DataContract(Namespace = "http://schemas.microsoft.com/windowsazure")]
    public class ReplicatedTableChainAndTableConfigurationStore
    {
        [DataMember(IsRequired = true, Order = 0)]
        private Dictionary<string, ReplicatedTableConfigurationStore> chainMap = new Dictionary<string, ReplicatedTableConfigurationStore>();

        [DataMember(IsRequired = true, Order = 1)]
        private List<RTableConfiguredTable> tableList = new List<RTableConfiguredTable>();


        /*
         * Chain APIs:
         */
        public void SetChain(string chainName, ReplicatedTableConfigurationStore config)
        {
            if (string.IsNullOrEmpty(chainName))
            {
                throw new ArgumentNullException("chainName");
            }

            if (config == null)
            {
                throw new ArgumentNullException("config");
            }

            chainMap.Remove(chainName);
            chainMap.Add(chainName, config);
        }

        public ReplicatedTableConfigurationStore GetChain(string chainName)
        {
            if (string.IsNullOrEmpty(chainName))
            {
                return null;
            }

            return !chainMap.ContainsKey(chainName) ? null : chainMap[chainName];
        }

        public void RemoveChain(string chainName)
        {
            if (GetChain(chainName) == null)
            {
                return;
            }

            RTableConfiguredTable table = tableList.Find(e => chainName.Equals(e.ChainName, StringComparison.OrdinalIgnoreCase));
            if (table != null)
            {
                var msg = string.Format("Chain:\'{0}\' is referenced by table:\'{1}\'! First, delete the table then the chain.",
                                        chainName,
                                        table.TableName);
                throw new Exception(msg);
            }

            chainMap.Remove(chainName);
        }

        /*
         * Configured tables APIs:
         */
        public void SetTable(RTableConfiguredTable config)
        {
            if (config == null)
            {
                throw new ArgumentNullException("config");
            }

            var tableName = config.TableName;
            if (string.IsNullOrEmpty(tableName))
            {
                throw new ArgumentNullException("TableName");
            }

            // If pointing a chain, then the chain must exist ?
            ThrowIfChainIsMissing(config);

            tableList.RemoveAll(e => tableName.Equals(e.TableName, StringComparison.OrdinalIgnoreCase));
            tableList.Add(config);
        }

        public RTableConfiguredTable GetTable(string tableName)
        {
            if (string.IsNullOrEmpty(tableName))
            {
                return null;
            }

            return tableList.Find(e => tableName.Equals(e.TableName, StringComparison.OrdinalIgnoreCase));
        }

        public void RemoveTable(string tableName)
        {
            if (string.IsNullOrEmpty(tableName))
            {
                return;
            }

            tableList.RemoveAll(e => tableName.Equals(e.TableName, StringComparison.OrdinalIgnoreCase));
        }

        private void ThrowIfChainIsMissing(RTableConfiguredTable config)
        {
            if (string.IsNullOrEmpty(config.ChainName))
            {
                return;
            }

            if (GetChain(config.ChainName) != null)
            {
                return;
            }

            var msg = string.Format("Table:\'{0}\' refers a missing chain:\'{1}\'! First, create the chain and then configure the table.",
                                    config.TableName,
                                    config.ChainName);
            throw new Exception(msg);
        }

        /*
         * Helpers ...
         */
        internal protected void ValidateAndFixConfig()
        {
            /*
             * 1 - Chains validation
             */
            // - Enforce chainMap not null
            if (chainMap == null)
            {
                chainMap = new Dictionary<string, ReplicatedTableConfigurationStore>();
            }
            else
            {
                //- Enforce chainName not empty
                chainMap.Remove("");

                // - Enforce config not null
                foreach (var key in chainMap.Keys.ToList().Where(key => chainMap[key] == null))
                {
                    chainMap.Remove(key);
                }
            }


            /*
             * 2 - Tables config validation
             */

            // - Enforce tableList not null
            if (tableList == null)
            {
                tableList = new List<RTableConfiguredTable>();
            }
            else
            {
                //- Enforce table config not null
                tableList.RemoveAll(cfg => cfg == null);

                //- Enforce tableName not null per configured table
                tableList.RemoveAll(cfg => string.IsNullOrEmpty(cfg.TableName));

                // - Enforce no duplicate table config
                var duplicates = tableList.GroupBy(cfg => cfg.TableName).Where(group => group.Count() > 1).ToList();
                if (duplicates.Any())
                {
                    var msg = string.Format("Table:\'{0}\' is configured more than once! Only one config per table.", duplicates.First().Key);
                    throw new Exception(msg);
                }

                // - Enforce tables refering existing chains
                tableList.TrueForAll(cfg =>
                {
                    ThrowIfChainIsMissing(cfg);
                    return true;
                });
            }
        }

        public string ToJson()
        {
            return JsonStore<ReplicatedTableChainAndTableConfigurationStore>.Serialize(this);
        }

        public static ReplicatedTableChainAndTableConfigurationStore FromJson(string json)
        {
            if (string.IsNullOrEmpty(json))
            {
                return new ReplicatedTableChainAndTableConfigurationStore();
            }

            var config = JsonStore<ReplicatedTableChainAndTableConfigurationStore>.Deserialize(json);
            config.ValidateAndFixConfig();

            return config;
        }
    }
}