using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.ChainTableInterface
{
    public interface IChainTableService
    {
        // Translate a Kiwi table identity to a Kiwi table
        IChainTable GetChainTable(string tableId);

        // serialize the parameter for this table servicce
        string Serialize();
    }
}
