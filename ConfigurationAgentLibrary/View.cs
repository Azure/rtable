//-----------------------------------------------------------------------
// <copyright file="View.cs" company="Microsoft">
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
    using System.Collections.Generic;
    using Microsoft.WindowsAzure.Storage.Table;

    public class View
    {
        public View()
        {
            this.Chain = new List<Tuple<ReplicaInfo, CloudTableClient>>();
        }

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

        public static bool operator ==(View view1, View view2)
        {
            if ( object.ReferenceEquals(view1, null) ||
                 object.ReferenceEquals(view2, null))
            {
                return false;
            }
            
            if (view1.Chain.Count != view2.Chain.Count)
                return false;

            if (view1.ViewId != view2.ViewId)
                return false;

            if (view1.ReadHeadIndex != view2.ReadHeadIndex)
                return false;

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
