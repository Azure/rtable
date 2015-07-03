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

    public enum ReplicatedTableQuorumWriteCode
    {
        ConflictDueToUpdateInProgress,
        ReadExceptions,
        ReadNoQuorum,
        ConflictDueToBlobChange,
        BlobsNotInSyncOrTransitioning,
        Success,
        QuorumWriteFailure,
    }

    public class ReplicatedTableQuorumWriteResult
    {
        public ReplicatedTableQuorumWriteResult(ReplicatedTableQuorumWriteCode code, string msg)
            : this(code, msg, null)
        {
        }

        public ReplicatedTableQuorumWriteResult(ReplicatedTableQuorumWriteCode code, List<ReplicatedTableWriteBlobResult> results)
            : this(code, "", results)
        {
        }

        public ReplicatedTableQuorumWriteResult(ReplicatedTableQuorumWriteCode code, string msg, List<ReplicatedTableWriteBlobResult> results)
        {
            Code = code;
            Message = msg;
            Results = results;
        }

        public ReplicatedTableQuorumWriteCode Code { get; private set; }

        public string Message { get; private set; }

        public List<ReplicatedTableWriteBlobResult> Results { get; private set; }

        public override string ToString()
        {
            string msg = string.Format("QuorumWriteResult Code: {0}, Message: {1}", Code, Message);

            if (Results == null)
            {
                return msg;
            }

            int index = 0;
            msg += Results.Aggregate("\n",
                                     (current, result) =>
                                     {
                                         if (!string.IsNullOrEmpty(current))
                                         {
                                             current += "\n";
                                         }
                                         return current + string.Format("Blob #{0} -> {1}", index++, result.ToString());
                                     });
            return msg;
        }
    }
}
