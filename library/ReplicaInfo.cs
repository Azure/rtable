﻿// azure-rtable ver. 0.9
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
    using System.Runtime.Serialization;

    public enum ReplicaStatus
    {
        None,
        ReadOnly,
        WriteOnly,
        ReadWrite,
    }

    [DataContract(Namespace = "http://schemas.microsoft.com/windowsazure")]
    public class ReplicaInfo
    {
        [DataMember(IsRequired = true)]
        public string StorageAccountName { get; set; }

        //Eventually, need a way to NOT pass this in here
        [DataMember(IsRequired = true)]
        public string StorageAccountKey { get; set; }

        [DataMember(IsRequired = true)]
        public long ViewInWhichAddedToChain { get; set; }

        // Required = false for backward compatibility
        [DataMember(IsRequired = false)]
        public ReplicaStatus Status { get; set; }

        // Required = false for backward compatibility
        [DataMember(IsRequired = false)]
        public long ViewWhenTurnedOff { get; set; }

        public ReplicaInfo()
        {
            this.ViewInWhichAddedToChain = 1;
            this.Status = ReplicaStatus.ReadWrite;
            this.ViewWhenTurnedOff = 0;
        }

        public bool IsReadOnly()
        {
            return (this.Status == ReplicaStatus.ReadOnly);
        }

        public bool IsReadable()
        {
            return (IsReadOnly() || this.Status == ReplicaStatus.ReadWrite);
        }

        public bool IsWriteOnly()
        {
            return (this.Status == ReplicaStatus.WriteOnly);
        }

        public bool IsWritable()
        {
            return (IsWriteOnly() || this.Status == ReplicaStatus.ReadWrite);
        }

        public override string ToString()
        {
            return string.Format("Account Name: {0}, AccountKey: {1}, ViewInWhichAddedToChain: {2}, Status: {3}",
                this.StorageAccountName, 
                "***********", 
                this.ViewInWhichAddedToChain,
                this.Status);
        }
    }
}
