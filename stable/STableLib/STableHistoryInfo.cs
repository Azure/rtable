using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.STable
{
    public class STableHistoryInfo
    {
        public int HeadSSID { get; set; }

        public List<SnapshotInfo> Snapshots { get; set; }

        public SnapshotInfo GetSnapshot(int ssid)
        {
            if (ssid < 0 || ssid > HeadSSID)
                throw new ArgumentOutOfRangeException();

            return Snapshots[ssid];
        }

        public STableHistoryInfo()
        {
            Snapshots = new List<SnapshotInfo>();
        }
    }

    public enum SnapshotState
    {
        Head,       // this is the head version, not a valid snapshot
        Valid,      // this is a valid and existing snapshot
        Deleted,    // this snapshot has been deleted
    }

    public class SnapshotInfo
    {
        public static readonly int NoParent = -1;

        public int SSID { get; private set; }
        public SnapshotState State { get; private set; }
        public int Parent { get; private set; }

        public SnapshotInfo(int ssid, SnapshotState state, int parent)
        {
            this.SSID = ssid;
            this.State = state;
            this.Parent = parent;
        }
    }
}
