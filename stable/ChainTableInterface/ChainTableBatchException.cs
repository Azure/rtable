using Microsoft.WindowsAzure.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.ChainTableInterface
{
    public class ChainTableBatchException : StorageException
    {
        public int FailedOpIndex { get; private set; }

        public ChainTableBatchException(int failedOpIndex, StorageException storageEx)
            : base(storageEx.RequestInformation, storageEx.Message, storageEx.InnerException)
        {
            this.FailedOpIndex = failedOpIndex;
        }
    }
}
