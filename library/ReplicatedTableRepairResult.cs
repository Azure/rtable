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
    public enum ReplicatedTableRepairCode
    {
        NotConfiguredTable,
        TableViewEmpty,
        NotSpecifiedReplica,
        RepairNotNeeded,
        Success,
        Error,
    }

    public class ReplicatedTableRepairResult
    {
        public ReplicatedTableRepairResult(ReplicatedTableRepairCode code, string tableName = "", string viewName = "", string storageAccountName = "")
        {
            Code = code;
            TableName = tableName;
            ViewName = viewName;
            StorageAccountName = storageAccountName;
        }

        public ReplicatedTableRepairCode Code { get; set; }

        public string TableName { get; private set; }

        public string ViewName { get; private set; }

        public string StorageAccountName { get; private set; }

        public ReconfigurationStatus Status { get; set; }

        public string Message { get; set; }

        public override string ToString()
        {
            return string.Format("ReplicatedTableRepairResult Code: {0} \n" +
                                 " TableName: {1} \n" +
                                 " ViewName: {2} \n" +
                                 " StorageAccountName: {3} \n" +
                                 " ReconfigurationStatus: {4} \n" +
                                 " Message:{5}",
                                 Code,
                                 TableName,
                                 ViewName,
                                 StorageAccountName,
                                 Status,
                                 Message);
        }
    }
}
