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

    public class ReplicatedTableConfigurationV2Wrapper : IReplicatedTableConfigurationWrapper
    {
        private readonly string _tableName;
        private readonly ReplicatedTableConfigurationServiceV2 _service;

        public ReplicatedTableConfigurationV2Wrapper(string tableName, ReplicatedTableConfigurationServiceV2 service)
        {
            this._tableName = tableName;
            this._service = service;

            // i.e. use the table default view
            ViewToUse = null;
        }

        /// <summary>
        /// Caller sets the view to use explicitly ...
        /// Needed for partitioned table where we want to run RTable APIs on a selected replica-chain.
        /// </summary>
        internal string ViewToUse { get; set; }

        public TimeSpan GetLockTimeout()
        {
            return this._service.LockTimeout;
        }

        public void SetLockTimeout(TimeSpan value)
        {
            this._service.LockTimeout = value;
        }

        public View GetReadView()
        {
            return this._service.GetTableView(this._tableName, ViewToUse);
        }

        public View GetWriteView()
        {
            return this._service.GetTableView(this._tableName, ViewToUse);
        }

        public bool IsViewStable()
        {
            return this._service.IsTableViewStable(this._tableName, ViewToUse);
        }

        public bool IsConvertToRTableMode()
        {
            return this._service.ConvertToRTable(this._tableName);
        }

        public bool IsIntrumentationEnabled()
        {
            return this._service.IsIntrumentationEnabled();
        }
    }
}