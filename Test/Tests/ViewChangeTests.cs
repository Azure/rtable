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

namespace Microsoft.Azure.Toolkit.Replication.Test
{
    using NUnit.Framework;
    using System;
    using System.Threading.Tasks;

    /// <summary>
    /// The scenario is this: 
    /// Either the Head or Tail storage account suffers a short outage. 
    /// During the short outage, we make some Table API calls which are expected to fail.
    /// After the storage account has recovered from that short outage, we want to confirm that,
    /// Table API calls (to the same row/entity) will work as expected. 
    /// In other words, the system can resume normal operation after a short outage.
    /// </summary>
    [TestFixture]
    [Parallelizable(ParallelScope.None)]
    public class ViewChangeTests : HttpManglerTestBase
    {
        [OneTimeSetUp]
        public void TestFixtureSetup()
        {
            this.OneTimeSetUpInternal();
        }

        [OneTimeTearDown]
        public void TestFixtureTearDown()
        {
            base.DeleteAllRtableResources();
        }

        /// <summary>
        /// Call ReplaceRow() during Head storage account outage. Expected operation to fail.
        /// After recovery, expect ReplaceRow() API to work for the same row.
        /// </summary>
        [Test(Description = "Call ReplaceRow() API during short outage at the Head storage account.")]
        public void ReplaceRowTest()
        {
            this.rtableWrapper = RTableWrapperForSampleRTableEntity.GetRTableWrapper(this.repTable);

            string entityPartitionKey = "jobType-TamperReplaceRowHeadTest";
            string entityRowKey = "jobId-TamperReplaceRowHeadTest";

            this.ForceDeleteEntryFromStorageTablesDirectly(entityPartitionKey, entityRowKey);

            int targetStorageAccount = 1;
            TargetRTableWrapperApi<SampleRTableEntity> targetApi = this.rtableWrapper.ReplaceRow;
            bool targetApiExpectedToFail = false;
            bool checkOriginalEntityUnchanged = false;
            bool checkStorageAccountsConsistent = false;

            DateTime httpManglerStartTime;
            Task task = Task.Run(() =>
                this.SetupAndRunDelayTableBehavior(
                    entityPartitionKey,
                    entityRowKey,
                    targetStorageAccount,
                    targetApi,
                    targetApiExpectedToFail,
                    checkOriginalEntityUnchanged,
                    checkStorageAccountsConsistent,
                    out httpManglerStartTime));

            task.Wait();

        }
    }
}
