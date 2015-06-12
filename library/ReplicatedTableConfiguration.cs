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
    public class ReplicatedTableConfiguration
    {
        [DataMember(IsRequired = true, Order = 0)]
        private Dictionary<string, ReplicatedTableConfigurationStore> viewMap = new Dictionary<string, ReplicatedTableConfigurationStore>();

        [DataMember(IsRequired = true, Order = 1)]
        private List<RTableConfiguredTable> tableList = new List<RTableConfiguredTable>();


        /*
         * View APIs:
         */
        public void SetView(string viewName, ReplicatedTableConfigurationStore config)
        {
            if (string.IsNullOrEmpty(viewName))
            {
                throw new ArgumentNullException("viewName");
            }

            if (config == null)
            {
                throw new ArgumentNullException("config");
            }

            viewMap.Remove(viewName);
            viewMap.Add(viewName, config);
        }

        public ReplicatedTableConfigurationStore GetView(string viewName)
        {
            if (string.IsNullOrEmpty(viewName))
            {
                return null;
            }

            return !viewMap.ContainsKey(viewName) ? null : viewMap[viewName];
        }

        public void RemoveView(string viewName)
        {
            if (GetView(viewName) == null)
            {
                return;
            }

            RTableConfiguredTable table = tableList.Find(e => viewName.Equals(e.ViewName, StringComparison.OrdinalIgnoreCase));
            if (table != null)
            {
                var msg = string.Format("View:\'{0}\' is referenced by table:\'{1}\'! First, delete the table then the view.",
                                        viewName,
                                        table.TableName);
                throw new Exception(msg);
            }

            viewMap.Remove(viewName);
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

            // If pointing a view, then the view must exist ?
            ThrowIfViewIsMissing(config);

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

        private void ThrowIfViewIsMissing(RTableConfiguredTable config)
        {
            if (string.IsNullOrEmpty(config.ViewName))
            {
                return;
            }

            if (GetView(config.ViewName) != null)
            {
                return;
            }

            var msg = string.Format("Table:\'{0}\' refers a missing view:\'{1}\'! First, create the view and then configure the table.",
                                    config.TableName,
                                    config.ViewName);
            throw new Exception(msg);
        }

        /*
         * Helpers ...
         */
        internal protected void ValidateAndFixConfig()
        {
            /*
             * 1 - Views validation
             */
            // - Enforce viewMap not null
            if (viewMap == null)
            {
                viewMap = new Dictionary<string, ReplicatedTableConfigurationStore>();
            }
            else
            {
                //- Enforce viewName not empty
                viewMap.Remove("");

                // - Enforce config not null
                foreach (var key in viewMap.Keys.ToList().Where(key => viewMap[key] == null))
                {
                    viewMap.Remove(key);
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

                // - Enforce tables refering existing views
                tableList.TrueForAll(cfg =>
                {
                    ThrowIfViewIsMissing(cfg);
                    return true;
                });
            }
        }

        public string ToJson()
        {
            return JsonStore<ReplicatedTableConfiguration>.Serialize(this);
        }

        public static ReplicatedTableConfiguration FromJson(string json)
        {
            if (string.IsNullOrEmpty(json))
            {
                return new ReplicatedTableConfiguration();
            }

            var config = JsonStore<ReplicatedTableConfiguration>.Deserialize(json);
            config.ValidateAndFixConfig();

            return config;
        }
    }
}