using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.STable
{
    internal sealed class STableEntity : ITableEntity
    {

        public string PartitionKey { get; set; }

        public string RowKey { get; set; }

        public string ETag { get; set; }

        public DateTimeOffset Timestamp { get; set; }



        // Metadata for snapshot table

        // When head row's ssid = x, all the snapshots from 0 to x - 1 has been solidated,
        // and can be retrieved by go through the snapshot chain.
        internal int SSID { get; set; }

        // The version of the row, this will be incremented upon any modification to the
        // row in head table (incl. user-level & internal), i.e., ModifyRowCOW.
        internal long Version { get; set; }

        internal bool Deleted { get; set; }

        // Virtual ETag exposed to the user, only valid if !Deleted.
        internal string VETag { get; set; }

        // User's payload, only valid if !Deleted.
        internal IDictionary<string, EntityProperty> Payload { get; set; }



        public STableEntity()
        {
            PartitionKey = null;
            RowKey = null;
            ETag = null;
            Timestamp = DateTimeOffset.MinValue;

            SSID = 0;
            Version = 0;
            Deleted = false;
            VETag = "";
            Payload = new Dictionary<string, EntityProperty>();
        }
        
        public STableEntity(string partitionKey, string rowKey)
            : this(partitionKey, rowKey, DateTimeOffset.MinValue, null)
        {
        }

        public STableEntity(string partitionKey, string rowKey, DateTimeOffset timestamp, string etag)
            : this()
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
            this.Timestamp = timestamp;
            this.ETag = etag;
        }

        public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            Dictionary<string, EntityProperty> prop = new Dictionary<string, EntityProperty>(properties);

            SSID = (int)prop[KeySSID].Int32Value;
            prop.Remove(KeySSID);

            Version = (long)prop[KeyVersion].Int64Value;
            prop.Remove(KeyVersion);

            Deleted = (bool)prop[KeyDeleted].BooleanValue;
            prop.Remove(KeyDeleted);

            VETag = prop[KeyVETag].StringValue;
            prop.Remove(KeyVETag);

            Payload = prop;
        }


        public IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var prop = new Dictionary<string, EntityProperty>(Payload);
            prop.Add(KeySSID, new EntityProperty(SSID));
            prop.Add(KeyVersion, new EntityProperty(Version));
            prop.Add(KeyDeleted, new EntityProperty(Deleted));
            prop.Add(KeyVETag, new EntityProperty(VETag));

            return prop;
        }

        internal static readonly int InitialVersion = 1;
        internal static readonly string KeySSID = "__SSID";
        internal static readonly string KeyVersion = "__Version";
        internal static readonly string KeyDeleted = "__Deleted";
        internal static readonly string KeyVETag = "__VETag";
    }
}
