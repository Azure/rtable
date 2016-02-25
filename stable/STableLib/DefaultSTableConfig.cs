using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.STable
{
    public class DefaultSTableConfig : ISTableConfig
    {
        public DefaultSTableConfig(string sTableName, bool useRTable)
        {
            this.namebase = sTableName;
            this.useRTable = useRTable;
        }

        public string GetHeadTableId()
        {
            var prefix = useRTable ? "__RTable_" : "";
            return prefix + namebase;
        }

        public string GetSnapshotTableId(int ssid)
        {
            var prefix = useRTable ? "__RTable_" : "";
            return prefix + namebase + "SSTab" + ssid;
        }

        private string namebase;
        private bool useRTable;
    }
}
