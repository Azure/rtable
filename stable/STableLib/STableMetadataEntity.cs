using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.STable
{
    class STableMetadataEntity : ITableEntity
    {
        // Generate a retrieve op to get meta entity
        internal static TableOperation RetrieveMetaOp()
        {
            return TableOperation.Retrieve<STableMetadataEntity>(MetaPKey, MetaRKey);
        }



        public string PartitionKey { get; set; }

        public string RowKey { get; set; }

        public string ETag { get; set; }

        public DateTimeOffset Timestamp { get; set; }



        // Metadata block

        // Valid snapshots: 0 .. CurrentSSID - 1
        // Current (unsealed) snapshot: CurrentSSID
        internal int CurrentSSID { get; private set; }

        // valid ssid list does not contain the current unsealed snapshot (CurrentSSID)
        private List<int> ValidSSID { get; set; }

        // SSIDHistory contains the parent information of the current unsealed snapshot (CurrentSSID)
        // It also contains the parent information for all the deleted snapshots
        // i.e., it contains information for SSID 0 .. CurrentSSID
        // SSIDHistory.Count == CurrentSSID + 1;
        private List<int> SSIDHistory { get; set; }


        public STableMetadataEntity()
        {
            PartitionKey = MetaPKey;
            RowKey = MetaRKey;
            ETag = null;
            Timestamp = DateTimeOffset.MinValue;

            CurrentSSID = 0;
            ValidSSID = new List<int>();
            SSIDHistory = new List<int>();
            SSIDHistory.Add(SnapshotInfo.NoParent);    // ssid0 is the root, no parent
        }
        
        public STableMetadataEntity(string partitionKey, string rowKey)
            : this(partitionKey, rowKey, DateTimeOffset.MinValue, null)
        {
        }

        public STableMetadataEntity(string partitionKey, string rowKey, DateTimeOffset timestamp, string etag)
            : this()
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
            this.Timestamp = timestamp;
            this.ETag = etag;
        }

        public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            CurrentSSID = (int)properties[KeyCurrentSSID].Int32Value;

            ValidSSID.Clear();
            string vsData = properties[KeyValidSSID].StringValue;
            Assert.IsTrue(vsData != null);
            if (!vsData.Equals(""))
            {
                string[] vs = vsData.Split(',');
                for (int i = 0; i < vs.Length; ++i)
                    ValidSSID.Add(int.Parse(vs[i]));
            }

            SSIDHistory.Clear();
            string shData = properties[KeySSIDHist].StringValue;
            Assert.IsTrue(shData != null && !shData.Equals(""));
            string[] sh = shData.Split(',');
            for (int i = 0; i < sh.Length; ++i)
                SSIDHistory.Add(int.Parse(sh[i]));

            Assert.IsTrue(SSIDHistory.Count == CurrentSSID + 1);
        }


        public IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var prop = new Dictionary<string, EntityProperty>();
            prop.Add(KeyCurrentSSID, new EntityProperty((int?)CurrentSSID));
            
            StringBuilder sb = new StringBuilder();
            if (ValidSSID.Count > 0)
            {
                sb.Append(ValidSSID[0]);
                for (int i = 1; i < ValidSSID.Count; ++i)
                {
                    sb.Append(",");
                    sb.Append(ValidSSID[i]);
                }
            }
            prop.Add(KeyValidSSID, new EntityProperty(sb.ToString()));

            sb = new StringBuilder();
            Assert.IsTrue(SSIDHistory.Count > 0 && SSIDHistory.Count == CurrentSSID + 1);
            sb.Append(SSIDHistory[0]);
            for (int i = 1; i < SSIDHistory.Count; ++i)
            {
                sb.Append(",");
                sb.Append(SSIDHistory[i]);
            }
            prop.Add(KeySSIDHist, new EntityProperty(sb.ToString()));
            
            return prop;
        }



        internal bool IsSnapshotValid(int ssid)
        {
            return (ValidSSID.BinarySearch(ssid) >= 0);
        }

        internal void CreateSnapshot(int parentOfNewHead)
        {
            Assert.IsTrue(SSIDHistory.Count == CurrentSSID + 1);
            Assert.IsTrue(parentOfNewHead >= 0 && parentOfNewHead <= CurrentSSID);

            ValidSSID.Add(CurrentSSID);
            CurrentSSID++;

            SSIDHistory.Add(parentOfNewHead);
        }

        internal int GetParent(int ssid)
        {
            Assert.IsTrue(ssid >= 0 && ssid <= CurrentSSID);
            return SSIDHistory[ssid];
        }

        internal void DeleteSnapshot(int ssid)
        {
            int index = ValidSSID.BinarySearch(ssid);
            Assert.IsTrue(index >= 0 && index < ValidSSID.Count);
            ValidSSID.RemoveAt(index);
        }

        internal int? PrevSnapshot(int ssid)
        {
            int index = ValidSSID.BinarySearch(ssid);
            if (index < 0)
                index = (~index) - 1;
            else
                index -= 1;

            if (index < 0)
                return null;
            else
                return ValidSSID[index];
        }

        internal int? NextSnapshot(int ssid)
        {
            int index = ValidSSID.BinarySearch(ssid);
            if (index < 0)
                index = ~index;
            else
                index += 1;

            if (index >= ValidSSID.Count)
                return null;
            else
                return ValidSSID[index];
        }

        // Get a valid snapshot id enumerator, the first call to prevSSID() returns ssid
        internal SSIDEnumerator GetSSIDEnumerator(int ssid)
        {
            int index = ValidSSID.BinarySearch(ssid);
            if (index < 0)
                return null;

            return new SSIDEnumerator(ValidSSID, index);
        }

        // get a enumerator for all alid ssids
        internal SSIDEnumerator GetSSIDEnumerator()
        {
            return new SSIDEnumerator(ValidSSID, ValidSSID.Count - 1);
        }



        internal class SSIDEnumerator
        {
            private List<int> ssids;
            private int nowat;

            internal SSIDEnumerator(List<int> ssids, int initPos)
            {
                this.ssids = ssids;
                this.nowat = initPos;
            }

            internal bool hasMoreSSID()
            {
                return (nowat >= 0);
            }

            internal int getSSIDAndMovePrev()
            {
                Assert.IsTrue(nowat >= 0);
                int ans = ssids[nowat];
                nowat--;

                return ans;
            }
        }



        // address of this metadata entity
        private static readonly string MetaPKey = "__STable_Meta";
        private static readonly string MetaRKey = "__STable_Meta";

        private static readonly string KeyCurrentSSID = "__CurrentSSID";
        private static readonly string KeyValidSSID = "__ValidSSID";
        private static readonly string KeySSIDHist = "__SSIDHistory";
    }


}
