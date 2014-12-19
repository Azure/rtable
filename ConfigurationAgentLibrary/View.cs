using Microsoft.WindowsAzure.Storage.Blob.Protocol;
using Microsoft.WindowsAzure.Storage.Table;

namespace Microsoft.WindowsAzure.Storage.RTable.ConfigurationAgentLibrary
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

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
