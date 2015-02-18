// azure-rtable ver. 0.9
//
// Copyright (c) Microsoft Corporation
//
// All rights reserved. 
//
// MIT License
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files 
// (the ""Software""), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, 
// merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished 
// to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF 
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE 
// FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION 
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


namespace Microsoft.Azure.Toolkit.Replication
{
    using System;
    using Microsoft.WindowsAzure.Storage.Table;

    //this is the class that users extend to store their own data in a row
    public interface IReplicatedTableEntity : ITableEntity
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

