using Microsoft.WindowsAzure.Storage.ChainTableInterface;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.STable
{
    // A wrapper for IChainTableService, providing buffer of known tables
    class TableService
    {
        internal TableService(ISTableConfig config, IChainTableService cs)
        {
            this.config = config;
            this.cs = cs;
        }

        internal IChainTable GetHead()
        {
            return GetTable(config.GetHeadTableId());
        }

        internal IChainTable GetSnapshot(int ssid)
        {
            return GetTable(config.GetSnapshotTableId(ssid));
        }



        private ISTableConfig config;
        private IChainTableService cs;
        // buffer for involved IChainTables, we will ask CS if we cannot find a IChainTable here
        private Dictionary<string, IChainTable> knownTables = new Dictionary<string, IChainTable>();



        private IChainTable GetTable(string tableId)
        {
            if (knownTables.ContainsKey(tableId))
                return knownTables[tableId];
            else
            {
                var table = cs.GetChainTable(tableId);
                knownTables[tableId] = table;
                return table;
            }
        }
    }
}
