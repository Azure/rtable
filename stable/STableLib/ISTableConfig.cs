using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.STable
{
    public interface ISTableConfig
    {
        string GetHeadTableId();

        string GetSnapshotTableId(int ssid);
    }
}
