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
    using global::Azure.Data.Tables;
    using System;
    using System.Collections.Generic;

    public class View
    {
        public View(string name)
        {
            this.Name = name;
            this.RefreshTime = DateTime.MinValue;
            this.LeaseDuration = TimeSpan.FromSeconds(0);
            this.Chain = new List<Tuple<ReplicaInfo, TableServiceClient>>();
            this.ReadTailIndex = TailIndex;
        }

        public static View InitFromConfigVer1(string name, ReplicatedTableConfigurationStore configurationStore, Func<ReplicaInfo, TableServiceClient> GetReplicaTableClient)
        {
            View view = new View(name);

            if (configurationStore != null)
            {
                view.ViewId = configurationStore.ViewId;
                view.ReadHeadIndex = configurationStore.ReadViewHeadIndex;
                view.RefreshTime = DateTime.UtcNow;

                foreach (ReplicaInfo replica in configurationStore.ReplicaChain)
                {
                    if (replica == null)
                    {
                        continue;
                    }

                    TableServiceClient tableClient = GetReplicaTableClient(replica);
                    if (tableClient == null)
                    {
                        // All replicas MUST exist or View is not relevant
                        view.Chain.Clear();

                        ReplicatedTableLogger.LogError("ViewName={0} could not load replica ({1})", name, replica);
                        break;
                    }

                    view.Chain.Add(new Tuple<ReplicaInfo, TableServiceClient>(replica, tableClient));
                }

                // If not configured use Tail. Chain must be defined at this point, so don't move this code up!
                view.ReadTailIndex = configurationStore.ReadViewTailIndex;
                if (view.ReadTailIndex < 0)
                {
                    view.ReadTailIndex = view.TailIndex;
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

        public static View InitFromConfigVer2(string name, ReplicatedTableConfigurationStore configurationStore, Func<ReplicaInfo, TableServiceClient> GetReplicaTableClient)
        {
            View view = new View(name);

            if (configurationStore != null)
            {
                view.ViewId = configurationStore.ViewId;
                view.RefreshTime = DateTime.UtcNow;

                foreach (ReplicaInfo replica in configurationStore.GetCurrentReplicaChain())
                {
                    TableServiceClient tableClient = GetReplicaTableClient(replica);
                    if (tableClient == null)
                    {
                        // All replicas MUST exist or View is not relevant
                        view.Chain.Clear();

                        ReplicatedTableLogger.LogError("ViewName={0} could not load replica ({1})", name, replica);
                        break;
                    }

                    view.Chain.Add(new Tuple<ReplicaInfo, TableServiceClient>(replica, tableClient));
                }

                // Infered: first readable replica
                view.ReadHeadIndex = view.Chain.FindIndex(tuple => tuple.Item1.IsReadable());

                // If not configured use Tail. Chain must be defined at this point, so don't move this code up!
                view.ReadTailIndex = configurationStore.ReadViewTailIndex;
                if (view.ReadTailIndex < 0)
                {
                    view.ReadTailIndex = view.TailIndex;
                }
            }

            return view;
        }

        public string Name { get; private set; }

        public DateTime RefreshTime { get; set; }

        public TimeSpan LeaseDuration { get; set; }

        public long ViewId { get; set; }

        public List<Tuple<ReplicaInfo, TableServiceClient>> Chain;

        public TableServiceClient this[int index]
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

        public int ReadTailIndex
        {
            get;
            private set;
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

        public bool IsExpired()
        {
            if (DateTime.UtcNow - RefreshTime > LeaseDuration)
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
                if ( ! view1.GetReplicaInfo(i).Equals(view2.GetReplicaInfo(i)) )
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
