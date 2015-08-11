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
    using System.Security;
    using System.Runtime.Serialization;

    [DataContract(Namespace = "http://schemas.microsoft.com/windowsazure")]
    public class ReplicaInfo
    {
        [DataMember(IsRequired = true)]
        public string StorageAccountName { get; set; }

        ///
        /// Depricated: kept for backward compatibility.
        /// Pass the Connection strings directly to RTable service.
        ///
        [DataMember(IsRequired = false)]
        public string StorageAccountKey { get; set; }

        [DataMember(IsRequired = true)]
        public long ViewInWhichAddedToChain { get; set; }

        public ReplicaInfo()
        {
            this.ViewInWhichAddedToChain = 1;
        }

        public override string ToString()
        {
            return string.Format("Account Name: {0}, AccountKey: {1}, ViewInWhichAddedToChain: {2}", 
                this.StorageAccountName, 
                "***********", 
                this.ViewInWhichAddedToChain);
        }

        internal SecureString ConnectionString
        {
            get;
            set;
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            ReplicaInfo other = obj as ReplicaInfo;
            if (other == null)
            {
                return false;
            }

            if (StorageAccountName != other.StorageAccountName)
            {
                return false;
            }

            if (ViewInWhichAddedToChain != other.ViewInWhichAddedToChain)
            {
                return false;
            }

            return true;
         }
    }
}
