﻿// azure-rtable ver. 0.9
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
    using Microsoft.WindowsAzure.Storage.Table;

    public class View
    {
        public View(string name)
        {
            this.Name = name;
            this.RefreshTime = DateTime.MinValue;
            this.Chain = new List<Tuple<ReplicaInfo, CloudTableClient>>();
        }

        public static View InitFromConfigVer1(string name, ReplicatedTableConfigurationStore configurationStore, bool useHttps)
        {
            View view = new View(name);

            if (configurationStore != null)
            {
                view.ViewId = configurationStore.ViewId;
                view.ReadHeadIndex = configurationStore.ReadViewHeadIndex;

                foreach (ReplicaInfo replica in configurationStore.ReplicaChain)
                {
                    CloudTableClient tableClient = ReplicatedTableConfigurationManager.GetTableClientForReplica(replica, useHttps);

                    if (replica != null && tableClient != null)
                    {
                        view.Chain.Add(new Tuple<ReplicaInfo, CloudTableClient>(replica, tableClient));
                    }
                }

                if (!view.IsEmpty)
                {
                    ReplicaInfo head = view.GetReplicaInfo(0);
                    head.Status = ReplicaStatus.WriteOnly;

                    if (view.IsStable)
                    {
                        head.Status = ReplicaStatus.ReadWrite;
                    }
                }
            }

            return view;
        }

        public static View InitFromConfigVer2(string name, ReplicatedTableConfigurationStore configurationStore, bool useHttps)
        {
            View view = new View(name);

            if (configurationStore != null)
            {
                view.ViewId = configurationStore.ViewId;

                foreach (ReplicaInfo replica in configurationStore.GetCurrentReplicaChain())
                {
                    CloudTableClient tableClient = ReplicatedTableConfigurationManager.GetTableClientForReplica(replica, useHttps);
                    if (tableClient != null)
                    {
                        view.Chain.Add(new Tuple<ReplicaInfo, CloudTableClient>(replica, tableClient));
                    }
                }

                // Infered: first readable replica
                view.ReadHeadIndex = view.Chain.FindIndex(tuple => tuple.Item1.IsReadable());
            }

            return view;
        }

        public string Name { get; private set; }

        public DateTime RefreshTime { get; set; }

        public long ViewId { get; set; }

        public List<Tuple<ReplicaInfo, CloudTableClient>> Chain;

        public CloudTableClient this[int index]
        {
            get
            {
                return this.Chain[index].Item2;
            }
        }

        public ReplicaInfo GetReplicaInfo(int index)
        {
            return this.Chain[index].Item1;
        }

        public int TailIndex
        {
            get { return this.Chain.Count - 1; }
        }

        public int ReadHeadIndex
        {
            get; set;
        }

        public int WriteHeadIndex
        {
            get { return 0; }
        }

        public bool IsStable
        {
            get { return ReadHeadIndex == 0; }
        }

        public bool IsEmpty
        {
            get { return Chain.Count == 0; }
        }

        public bool IsReadOnly()
        {
            if (IsEmpty)
            {
                return false;
            }

            return this.Chain[0].Item1.IsReadOnly();
        }

        public bool IsWritable()
        {
            if (IsEmpty)
            {
                return false;
            }

            return this.Chain[0].Item1.IsWritable();
        }

        public bool IsExpired(TimeSpan leaseDuration)
        {
            if (DateTime.UtcNow - RefreshTime > leaseDuration)
            {
                return true;
            }

            return false;
        }

        public static bool operator ==(View view1, View view2)
        {
            if ( object.ReferenceEquals(view1, null) ||
                 object.ReferenceEquals(view2, null))
            {
                return false;
            }

            if ( ! string.Equals(view1.Name, view2.Name, StringComparison.OrdinalIgnoreCase) )
            {
                return false;
            }

            if (view1.Chain.Count != view2.Chain.Count)
            {
                return false;
            }

            if (view1.ViewId != view2.ViewId)
            {
                return false;
            }

            if (view1.ReadHeadIndex != view2.ReadHeadIndex)
            {
                return false;
            }

            for (int i = 0; i < view1.Chain.Count; i++)
            {
                if ((view1.GetReplicaInfo(i).StorageAccountName != view2.GetReplicaInfo(i).StorageAccountName) ||
                    (view1.GetReplicaInfo(i).StorageAccountKey != view2.GetReplicaInfo(i).StorageAccountKey) ||
                    (view1.GetReplicaInfo(i).ViewInWhichAddedToChain != view2.GetReplicaInfo(i).ViewInWhichAddedToChain))
                {
                    return false;
                }
            }

            return true;
        }

        public static bool operator !=(View view1, View view2)
        {
            return !(view1 == view2);
        }

        public override bool Equals(object obj)
        {
            try
            {
                return (bool) (this == (View) obj);
            }
            catch (Exception)
            {
                return false;
            }
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }
}
