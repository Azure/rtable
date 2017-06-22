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
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Text.RegularExpressions;

    [DataContract(Namespace = "http://schemas.microsoft.com/windowsazure")]
    public class ReplicatedTableConfigurationStore
    {
        public ReplicatedTableConfigurationStore()
        {
            this.ReplicaChain = new List<ReplicaInfo>();
            this.ReadViewTailIndex = -1;
            this.ViewId = 1; // minimum ViewId is 1.
            this.LeaseDuration = Constants.LeaseDurationInSec;
            this.Timestamp = DateTime.UtcNow;
            this.Instrumentation = false;
        }

        [OnDeserializing]
        private void OnDeserializing(StreamingContext context)
        {
            // Default value when Json file doesnt define it
            this.ReadViewTailIndex = -1;
        }

        [DataMember(IsRequired = true)]
        public List<ReplicaInfo> ReplicaChain { get; set; }

        [DataMember(IsRequired = true)]
        public int ReadViewHeadIndex { get; set; }

        [DataMember(IsRequired = false)]
        public int ReadViewTailIndex { get; set; }

        [DataMember(IsRequired = false)]
        public bool ConvertXStoreTableMode { get; set; }

        [DataMember(IsRequired = true)]
        public long ViewId { get; set; }

        [DataMember(IsRequired = true)]
        public int LeaseDuration { get; set; }

        [DataMember(IsRequired = true)]
        public DateTime Timestamp { get; set; }

        [DataMember(IsRequired = false)]
        public bool Instrumentation { get; set; }

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

        /// <summary>
        /// List of active Replicas i.e. RO / WO / RW
        /// </summary>
        /// <returns></returns>
        public List<ReplicaInfo> GetCurrentReplicaChain()
        {
            return ReplicaChain.Where(r => r.Status != ReplicaStatus.None).ToList();
        }


        /*
         * Helpers ...
         */
        internal protected void SanitizeWithCurrentView(View currentView)
        {
            if (ViewId == 0)
            {
                // Assert(currentView != null)

                if (!currentView.IsEmpty)
                {
                    ViewId = currentView.ViewId + 1;
                }
                else
                {
                    ViewId = 1;
                }
            }

            Timestamp = DateTime.UtcNow;

            foreach (var replica in GetCurrentReplicaChain())
            {
                // We are introducing 1 or more replicas at the head.
                // For each such replica, update the view id in which it was added to the write view of the chain
                if (replica.IsWriteOnly())
                {
                    replica.ViewInWhichAddedToChain = ViewId;
                    continue;
                }

                // stop at the first Readable replica
                break;
            }
        }

        internal protected void MoveReplicaToHeadAndSetViewToReadOnly(string viewName, string storageAccountName)
        {
            // Assert (storageAccountName != null)

            int matchIndex = ReplicaChain.FindIndex(r => r.StorageAccountName == storageAccountName);
            if (matchIndex == -1)
            {
                return;
            }

            // - Ensure its status is *None*
            ReplicaInfo candidateReplica = ReplicaChain[matchIndex];

            var oldStatus = candidateReplica.Status;
            candidateReplica.Status = ReplicaStatus.None;

            // First check it will be possible to modify the sequence
            foreach (ReplicaInfo replica in GetCurrentReplicaChain())
            {
                // Change is not possible
                if (replica.Status == ReplicaStatus.WriteOnly)
                {
                    // Restore previous status
                    candidateReplica.Status = oldStatus;

                    var msg = string.Format("View:\'{0}\' : can't set a WriteOnly replica to ReadOnly !!!", viewName);

                    ReplicatedTableLogger.LogError(msg);
                    throw new Exception(msg);
                }
            }

            // Do the change ...

            // - Move it to the front of the chain
            ReplicaChain.RemoveAt(matchIndex);
            ReplicaChain.Insert(0, candidateReplica);

            // Set all active replicas to *ReadOnly*
            foreach (ReplicaInfo replica in GetCurrentReplicaChain())
            {
                replica.Status = ReplicaStatus.ReadOnly;
            }

            // Update view id
            ViewId++;

            // Reset 'ReadViewTailIndex' => user has to set it again
            ResetReadViewTailIndex();
        }

        internal protected void EnableWriteOnReplicas(string viewName, string headStorageAccountName)
        {
            // Assert (headStorageAccountName != null)

            if (!ReplicaChain.Any() ||
                ReplicaChain[0].StorageAccountName != headStorageAccountName)
            {
                return;
            }

            // First, enable Write on all replicas
            foreach (ReplicaInfo replica in GetCurrentReplicaChain())
            {
                replica.Status = ReplicaStatus.ReadWrite;
            }

            // Then, set the head to WriteOnly
            ReplicaChain[0].Status = ReplicaStatus.WriteOnly;

            // one replica chain ? Force to ReadWrite
            if (GetCurrentReplicaChain().Count == 1)
            {
                ReplicaChain[0].Status = ReplicaStatus.ReadWrite;
            }

            // Update view id
            ViewId++;

            // Reset 'ReadViewTailIndex' => user has to set it again
            ResetReadViewTailIndex();
        }

        internal protected void EnableReadWriteOnReplica(string viewName, string headStorageAccountName)
        {
            if (!ReplicaChain.Any() ||
                ReplicaChain[0].StorageAccountName != headStorageAccountName ||
                ReplicaChain[0].Status != ReplicaStatus.WriteOnly)
            {
                return;
            }

            ReplicaChain[0].Status = ReplicaStatus.ReadWrite;

            // Update view id
            ViewId++;
        }

        internal protected void TurnReplicaOff(string viewName, string storageAccountName)
        {
            // Assert (storageAccountName != null)

            var foundReplicas = GetCurrentReplicaChain().FindAll(r => r.StorageAccountName == storageAccountName);

            if (!foundReplicas.Any())
            {
                return;
            }

            foreach (var replica in foundReplicas)
            {
                replica.Status = ReplicaStatus.None;
                replica.ViewWhenTurnedOff = ViewId;
            }

            // Update view id
            ViewId++;

            // Reset 'ReadViewTailIndex' => user has to set it again
            ResetReadViewTailIndex();
        }

        internal protected void ThrowIfChainIsNotValid(string viewName)
        {
            List<ReplicaInfo> chainList = GetCurrentReplicaChain();
            if (chainList.Any())
            {
                /* RULE 1:
                 * =======
                 * Read replicas rule:
                 *  - [R] replicas are contiguous from Tail backwards
                 *  - [R] replica count >= 1
                 */
                string readPattern = "^W*R+$";

                /* RULE 2:
                 * =======
                 * Write replicas rule:
                 *  - [W] replicas are contiguous from Head onwards
                 *  - [W] replica count = 0 or = ChainLength
                 */
                string writePattern = "^((R+)|(W+))$";

                // Get replica sequences
                string readSeq = "";
                string writeSeq = "";

                foreach (var replica in chainList)
                {
                    // Read sequence:
                    if (replica.IsReadable())
                    {
                        readSeq += "R";
                    }
                    else
                    {
                        readSeq += "W";
                    }

                    // Write sequence:
                    if (replica.IsWritable())
                    {
                        writeSeq += "W";
                    }
                    else
                    {
                        writeSeq += "R";
                    }
                }

                // Verify RULE 1:
                if (!Regex.IsMatch(readSeq, readPattern))
                {
                    var msg = string.Format("View:\'{0}\' has invalid Read chain:\'{1}\' !!!", viewName, readSeq);
                    throw new Exception(msg);
                }

                // Verify RULE 2:
                if (!Regex.IsMatch(writeSeq, writePattern))
                {
                    var msg = string.Format("View:\'{0}\' has invalid Write chain:\'{1}\' !!!", viewName, writeSeq);
                    throw new Exception(msg);
                }
            }
        }

        internal protected void ThrowIfReadViewTailIndexIsNotValid(string viewName)
        {
            if (ReadViewTailIndex < GetCurrentReplicaChain().Count)
            {
                return;
            }

            var msg = string.Format("View:\'{0}\' has ReadViewTailIndex:\'{1}\' greater then replica size:\'{2}\' !!!",
                                    viewName,
                                    ReadViewTailIndex,
                                    GetCurrentReplicaChain().Count);

            throw new Exception(msg);
        }

        internal protected void ResetReadViewTailIndex()
        {
            ReadViewTailIndex = -1;
        }
    }
}
