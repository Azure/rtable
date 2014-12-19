using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Core.Util;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using System.Configuration;

namespace Microsoft.WindowsAzure.Storage.RTable
{

    //this is the class that users extend to store their own data in a row
    public interface IRTableEntity : ITableEntity
    {
        // lock bit: used to detect that replication is in progress
        bool RowLock { get; set; }

        // version: used as (virtual) etag for replicated table with different (physical) etags  
        long Version { get; set; }

        // tomb stone: to support deletes
        bool Tombstone { get; set; }

        // view number: to support changes in replica configurations
        long ViewId { get; set; }

        // Operation type: last operation type that modified this data
        string Operation { get; set; }

        // Batch id: to support batch operations
        Guid BatchId { get; set; }

        /// <summary>
        /// (If RowLock=true) LockAcquisition is the timestamp in which the row was locked by the client. 
        /// New clients can use their own timeout (LockTimeout) value to determine whether to initiate replication or not.
        /// </summary>
        DateTimeOffset LockAcquisition { get; set; }
    }
}

