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
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;

    [DataContract(Namespace = "http://schemas.microsoft.com/windowsazure")]
    public class ReplicatedTableConfigurationStore
    {
        public ReplicatedTableConfigurationStore()
        {
            this.LeaseDuration = Constants.LeaseDurationInSec;
            this.Timestamp = DateTime.UtcNow;
            this.ViewId = 1; // minimum ViewId is 1.
            this.ReplicaChain = new List<ReplicaInfo>();
        }

        [DataMember(IsRequired = true)]
        public List<ReplicaInfo> ReplicaChain { get; set; }

        [DataMember(IsRequired = true)]
        public int ReadViewHeadIndex { get; set; }

        [DataMember(IsRequired = false)]
        public bool ConvertXStoreTableMode { get; set; }

        [DataMember(IsRequired = true)]
        public long ViewId { get; set; }

        [DataMember(IsRequired = true)]
        public int LeaseDuration { get; set; }

        [DataMember(IsRequired = true)]
        public DateTime Timestamp { get; set; }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            ReplicatedTableConfigurationStore other = obj as ReplicatedTableConfigurationStore;
            if (other == null)
            {
                return false;
            }

            if (this.ViewId == other.ViewId)
            {
                return true;
            }

            return false;
        }

        public string ToJson()
        {
            return JsonStore<ReplicatedTableConfigurationStore>.Serialize(this);
        }

        public List<ReplicaInfo> GetCurrentReplicaChain()
        {
            return ReplicaChain.Where(r => r.Status != ReplicaStatus.None).ToList();
        }
    }
}
