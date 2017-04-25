using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace STableUnitTest
{
    class TestEntity : ITableEntity
    {

        public string PartitionKey { get; set; }

        public string RowKey { get; set; }

        public string ETag { get; set; }

        public DateTimeOffset Timestamp { get; set; }

        public TestEntity()
        {
            PartitionKey = "";
            RowKey = "";
            ETag = "";
            d = ""; // no null value
            Timestamp = DateTimeOffset.MinValue;
        }

        public TestEntity(string pKey, string rKey) : this()
        {
            PartitionKey = pKey;
            RowKey = rKey;
        }

        public TestEntity(string pKey, string rKey, string eTag, DateTime time) : this()
        {
            PartitionKey = pKey;
            RowKey = rKey;
            ETag = eTag;
            Timestamp = time;
        }

        // payload

        public int a { get; set; }
        public int b { get; set; }
        public int c { get; set; }
        public string d { get; set; }
        public bool e { get; set; }
        public double f { get; set; }

        public void ReadEntity(IDictionary<string, EntityProperty> properties, Microsoft.WindowsAzure.Storage.OperationContext operationContext)
        {
            a = properties["a"].Int32Value.Value;
            b = properties["b"].Int32Value.Value;
            c = properties["c"].Int32Value.Value;
            d = properties["d"].StringValue;
            e = properties["e"].BooleanValue.Value;
            f = properties["f"].DoubleValue.Value;
        }



        public IDictionary<string, EntityProperty> WriteEntity(Microsoft.WindowsAzure.Storage.OperationContext operationContext)
        {
            var res = new Dictionary<string, EntityProperty>();
            res.Add("a", new EntityProperty(a));
            res.Add("b", new EntityProperty(b));
            res.Add("c", new EntityProperty(c));
            res.Add("d", new EntityProperty(d));
            res.Add("e", new EntityProperty(e));
            res.Add("f", new EntityProperty(f));

            return res;
        }
    }
}
