//-----------------------------------------------------------------------
// <copyright file="IRTableEntity.cs" company="Microsoft">
//    Copyright 2013 Microsoft Corporation
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//      http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// </copyright>
//-

namespace Microsoft.WindowsAzure.Storage.RTable
{
    using System;
    using Microsoft.WindowsAzure.Storage.Table;

    //this is the class that users extend to store their own data in a row
    public interface IRTableEntity : ITableEntity
    {
        // lock bit: used to detect that replication is in progress
        bool _rtable_RowLock { get; set; }

        // version: used as (virtual) etag for replicated table with different (physical) etags  
        long _rtable_Version { get; set; }

        // tomb stone: to support deletes
        bool _rtable_Tombstone { get; set; }

        // view number: to support changes in replica configurations
        long _rtable_ViewId { get; set; }

        // _rtable_Operation type: last operation type that modified this data
        string _rtable_Operation { get; set; }

        // Batch id: to support batch operations
        Guid _rtable_BatchId { get; set; }

        /// <summary>
        /// (If _rtable_RowLock=true) _rtable_LockAcquisition is the timestamp in which the row was locked by the client. 
        /// New clients can use their own timeout (LockTimeout) value to determine whether to initiate replication or not.
        /// </summary>
        DateTimeOffset _rtable_LockAcquisition { get; set; }
    }
}

