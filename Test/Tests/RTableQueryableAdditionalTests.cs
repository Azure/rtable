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
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using Microsoft.WindowsAzure.Storage.Table;
    using System.Threading;
    using Microsoft.WindowsAzure.Test.Network;
    using Microsoft.WindowsAzure.Test.Network.Behaviors;
    using System.Threading.Tasks;

    [TestFixture]
    [Parallelizable(ParallelScope.None)]
    public class RTableQueryableAdditionalTests : HttpManglerTestBase
    {
        [SetUp]
        public void TestFixtureSetup()
        {
            this.OneTimeSetUpInternal();
        }

        [TearDown]
        public void TestFixtureTearDown()
        {
            base.DeleteAllRtableResources();
        }

        [Test(Description = "LINQ queries don't return physically deleted entries")]
        public void LinqQueriesDontReturnPhysicallyDeletedEntries()
        {
            var rtable = new ReplicatedTable(this.repTable.TableName, this.configurationService);

            string firstName = "FirstName";
            string lastName = "LastName";

            /*
             * 1 - insert entries
             */
            for (int i = 0; i < 10; i++)
            {
                var customer = new CustomerEntity(firstName + i, lastName + i);

                TableOperation operation = TableOperation.Insert(customer);
                rtable.Execute(operation);
            }

            /*
             * 2 - delete entries #2 and #4
             */
            foreach (var i in new int[] {2,4})
            {
                TableOperation operation = TableOperation.Retrieve<CustomerEntity>(firstName + i, lastName + i);
                TableResult retrievedResult = rtable.Execute(operation);

                var customer = (CustomerEntity)retrievedResult.Result;

                TableOperation deleteOperation = TableOperation.Delete(customer);
                TableResult deleteResult = rtable.Execute(deleteOperation);

                Assert.IsNotNull(deleteResult, "deleteResult = null");
                Assert.AreEqual((int)HttpStatusCode.NoContent, deleteResult.HttpStatusCode, "deleteResult.HttpStatusCode mismatch");
            }

            /*
             * 3 - CreateQuery doesn't return entries #2 and #4
             */
            foreach (var customer in rtable.CreateReplicatedQuery<CustomerEntity>().AsEnumerable())
            {
                int id = int.Parse(customer.PartitionKey.Replace(firstName, ""));

                Assert.AreNotEqual(id, 2, "entry #2 should have been deleted");
                Assert.AreNotEqual(id, 4, "entry #4 should have been deleted");
            }

            /*
             * 4 - ExecuteQuery doesn't return entries #2 and #4
             */
            foreach (var customer in rtable.ExecuteQuery<CustomerEntity>(new TableQuery<CustomerEntity>()))
            {
                int id = int.Parse(customer.PartitionKey.Replace(firstName, ""));

                Assert.AreNotEqual(id, 2, "entry #2 should have been deleted");
                Assert.AreNotEqual(id, 4, "entry #4 should have been deleted");
            }
        }

        [Test(Description = "LINQ queries don't return entries with tombstone")]
        public void LinqQueriesDontReturnEntriesWithTombstone()
        {
            var rtable = new ReplicatedTable(this.repTable.TableName, this.configurationService);

            string firstName = "FirstName";
            string lastName = "LastName";

            /*
             * 1 - insert entries
             */
            for (int i = 0; i < 10; i++)
            {
                var customer = new CustomerEntity(firstName + i, lastName + i);

                TableOperation operation = TableOperation.Insert(customer);
                rtable.Execute(operation);
            }


            // Identify the Tail account
            View view = this.configurationWrapper.GetWriteView();
            Assert.IsTrue(view.Chain.Count > 1, "expects at least one replica!");

            string accountNameToTamper = view.Chain.Last().Item1.StorageAccountName;
            Console.WriteLine("RunHttpManglerBehaviorHelper(): accountNameToTamper={0}", accountNameToTamper);

            // We will fail requests to delete a row in Tail
            ProxyBehavior[] behaviors =
            {
                TamperBehaviors.TamperAllRequestsIf(
                    (session =>
                    {
                        session.oRequest.FailSession((int)HttpStatusCode.ServiceUnavailable, "ServerBusy", "");
                    }),
                    (session =>
                    {
                        // Commit to Tail i.e. physically delete the row
                        if (session.hostname.Contains(accountNameToTamper + ".") &&
                            session.HTTPMethodIs("DELETE"))
                        {
                            return true;
                        }

                        return false;
                    })),
            };

            using (new HttpMangler(false, behaviors))
            {
                /*
                * 2 - delete entries #2 and #4
                */
                foreach (var i in new int[] { 2, 4 })
                {
                    TableOperation operation = TableOperation.Retrieve<CustomerEntity>(firstName + i, lastName + i);
                    TableResult retrievedResult = rtable.Execute(operation);

                    var customer = (CustomerEntity)retrievedResult.Result;

                    TableOperation deleteOperation = TableOperation.Delete(customer);
                    TableResult deleteResult = rtable.Execute(deleteOperation);

                    Assert.IsNotNull(deleteResult, "deleteResult = null");
                    Assert.AreEqual((int)HttpStatusCode.ServiceUnavailable, deleteResult.HttpStatusCode, "deleteResult.HttpStatusCode mismatch");
                }
            }


            // Verify, Retrieve doesn't return entries #2 and #4
            int deleteId = 2;
            var result = rtable.Execute(TableOperation.Retrieve<CustomerEntity>(firstName + deleteId, lastName + deleteId));
            Assert.IsNotNull(result, "result = null");
            Assert.AreEqual((int)HttpStatusCode.NotFound, result.HttpStatusCode, "result.HttpStatusCode mismatch");

            deleteId = 4;
            result = rtable.Execute(TableOperation.Retrieve<CustomerEntity>(firstName + deleteId, lastName + deleteId));
            Assert.IsNotNull(result, "result = null");
            Assert.AreEqual((int)HttpStatusCode.NotFound, result.HttpStatusCode, "result.HttpStatusCode mismatch");


            /*
             * 3 - CreateQuery doesn't return entries #2 and #4
             */
            foreach (var customer in rtable.CreateReplicatedQuery<CustomerEntity>().AsEnumerable())
            {
                int id = int.Parse(customer.PartitionKey.Replace(firstName, ""));
                Assert.AreNotEqual(id, 2, "entry #2 should have been deleted");
                Assert.AreNotEqual(id, 4, "entry #4 should have been deleted");
            }


            /*
             * 4 - ExecuteQuery doesn't return entries #2 and #4
             */
            foreach (var customer in rtable.ExecuteQuery<CustomerEntity>(new TableQuery<CustomerEntity>()))
            {
                int id = int.Parse(customer.PartitionKey.Replace(firstName, ""));
                Assert.AreNotEqual(id, 2, "entry #2 should have been deleted");
                Assert.AreNotEqual(id, 4, "entry #4 should have been deleted");
            }
        }

        [Test(Description = "LINQ queries don't throw on stale view by default")]
        public void LinqQueriesDontThrowOnStaleViewByDefault()
        {
            TableOperation operation;
            TableResult result;
            CustomerEntity customer;


            /*
             * Set config viewId to 5
             */
            long staleViewId = 5;
            SetConfigViewIdAndIgnoreHigherViewIdRowsFlag(staleViewId, false);
            Assert.AreEqual(staleViewId, this.configurationService.GetTableView(this.repTable.TableName).ViewId, "View should be 5!!!");


            // Insert entries in stale viewId 5
            var rtable = new ReplicatedTable(this.repTable.TableName, this.configurationService);

            string firstName = "FirstName";
            string lastName = "LastName";

            for (int i = 0; i < 10; i++)
            {
                customer = new CustomerEntity(firstName + i, lastName + i);

                operation = TableOperation.Insert(customer);
                rtable.Execute(operation);
            }


            /*
             * Set config new viewId to 6
             */
            long newViewId = 6;
            SetConfigViewIdAndIgnoreHigherViewIdRowsFlag(newViewId, false);
            Assert.AreEqual(newViewId, this.configurationService.GetTableView(this.repTable.TableName).ViewId, "View should be 6!!!");


            // Update entry #5 in new viewId 6
            int entryId = 5;

            operation = TableOperation.Retrieve<CustomerEntity>(firstName + entryId, lastName + entryId);
            result = rtable.Execute(operation);

            Assert.IsTrue(result != null && result.HttpStatusCode == (int)HttpStatusCode.OK && (CustomerEntity)result.Result != null, "Retrieve customer failed");

            customer = (CustomerEntity)result.Result;
            customer.Email = "new_view@email.com";

            operation = TableOperation.Replace(customer);
            result = rtable.Execute(operation);

            Assert.IsTrue(result != null && result.HttpStatusCode == (int) HttpStatusCode.NoContent, "Update customer failed");


            /*
             * Simulate a stale client => Set config viewId back to 5
             */
            SetConfigViewIdAndIgnoreHigherViewIdRowsFlag(staleViewId, false);
            Assert.AreEqual(staleViewId, this.configurationService.GetTableView(this.repTable.TableName).ViewId, "View should be 5!!!");

            try
            {
                // Check Retrieve of row #5 throws stale view as expected
                operation = TableOperation.Retrieve<CustomerEntity>(firstName + entryId, lastName + entryId);
                rtable.Execute(operation);

                Assert.Fail("Retrieve() is expected to get an RTableStaleViewException but did not get it.");
            }
            catch (ReplicatedTableStaleViewException ex)
            {
                Assert.IsTrue(ex.ErrorCode == ReplicatedTableViewErrorCodes.ViewIdSmallerThanEntryViewId);
                Assert.IsTrue(ex.Message.Contains(string.Format("current _rtable_ViewId {0} is smaller than _rtable_ViewId of existing row {1}", staleViewId, newViewId)), "Got unexpected exception message");
            }


            // By default stale view detection is turned Off
            Assert.IsTrue(rtable.ThrowOnStaleViewInLinqQueryFlag == false);

            /*
             * stale client using LINQ: CreateReplicatedQuery
             */
            foreach (var entry in rtable.CreateReplicatedQuery<CustomerEntity>().AsEnumerable())
            {
                int id = int.Parse(entry.PartitionKey.Replace(firstName, ""));
                if (id == entryId)
                {
                    Assert.AreEqual(entry._rtable_ViewId, newViewId, "CreateReplicatedQuery: entry viewId should be '6'");
                }
                else
                {
                    Assert.AreEqual(entry._rtable_ViewId, staleViewId, "CreateReplicatedQuery: entry viewId should be '5'");
                }
            }

            /*
             * stale client using LINQ: ExecuteQuery
             */
            foreach (var entry in rtable.ExecuteQuery<CustomerEntity>(new TableQuery<CustomerEntity>()))
            {
                int id = int.Parse(entry.PartitionKey.Replace(firstName, ""));
                if (id == entryId)
                {
                    Assert.AreEqual(entry._rtable_ViewId, newViewId, "ExecuteQuery: entry viewId should be '6'");
                }
                else
                {
                    Assert.AreEqual(entry._rtable_ViewId, staleViewId, "ExecuteQuery: entry viewId should be '5'");
                }
            }
        }

        [Test(Description = "LINQ queries throw after detecting a stale view when ThrowOnStaleViewInLinqQueryFlag is set")]
        public void LinqQueriesThrowAfterDetectingStaleViewWhenThrowOnStaleViewInLinqQueryFlagIsSet()
        {
            TableOperation operation;
            TableResult result;
            CustomerEntity customer;


            /*
             * Set config viewId to 5
             */
            long staleViewId = 5;
            SetConfigViewIdAndIgnoreHigherViewIdRowsFlag(staleViewId, false);
            Assert.AreEqual(staleViewId, this.configurationService.GetTableView(this.repTable.TableName).ViewId, "View should be 5!!!");


            // Insert entries in stale viewId 5
            var rtable = new ReplicatedTable(this.repTable.TableName, this.configurationService);

            string firstName = "FirstName";
            string lastName = "LastName";

            for (int i = 0; i < 10; i++)
            {
                customer = new CustomerEntity(firstName + i, lastName + i);

                operation = TableOperation.Insert(customer);
                rtable.Execute(operation);
            }


            /*
             * Set config new viewId to 6
             */
            long newViewId = 6;
            SetConfigViewIdAndIgnoreHigherViewIdRowsFlag(newViewId, false);
            Assert.AreEqual(newViewId, this.configurationService.GetTableView(this.repTable.TableName).ViewId, "View should be 6!!!");


            // Update entry #5 in new viewId 6
            int entryId = 5;

            operation = TableOperation.Retrieve<CustomerEntity>(firstName + entryId, lastName + entryId);
            result = rtable.Execute(operation);

            Assert.IsTrue(result != null && result.HttpStatusCode == (int)HttpStatusCode.OK && (CustomerEntity)result.Result != null, "Retrieve customer failed");

            customer = (CustomerEntity)result.Result;
            customer.Email = "new_view@email.com";

            operation = TableOperation.Replace(customer);
            result = rtable.Execute(operation);

            Assert.IsTrue(result != null && result.HttpStatusCode == (int)HttpStatusCode.NoContent, "Update customer failed");


            /*
             * Simulate a stale client => Set config viewId back to 5
             */
            SetConfigViewIdAndIgnoreHigherViewIdRowsFlag(staleViewId, false);
            Assert.AreEqual(staleViewId, this.configurationService.GetTableView(this.repTable.TableName).ViewId, "View should be 5!!!");

            try
            {
                // Check Retrieve of row #5 throws stale view as expected
                operation = TableOperation.Retrieve<CustomerEntity>(firstName + entryId, lastName + entryId);
                rtable.Execute(operation);

                Assert.Fail("Retrieve() is expected to get an RTableStaleViewException but did not get it.");
            }
            catch (ReplicatedTableStaleViewException ex)
            {
                Assert.IsTrue(ex.ErrorCode == ReplicatedTableViewErrorCodes.ViewIdSmallerThanEntryViewId);
                Assert.IsTrue(ex.Message.Contains(string.Format("current _rtable_ViewId {0} is smaller than _rtable_ViewId of existing row {1}", staleViewId, newViewId)), "Got unexpected exception message");
            }


            // Enable throwing on stale view detection
            rtable.ThrowOnStaleViewInLinqQueryFlag = true;

            /*
             * stale client using LINQ: CreateReplicatedQuery
             */
            try
            {
                foreach (var entry in rtable.CreateReplicatedQuery<CustomerEntity>().AsEnumerable())
                {
                    int id = int.Parse(entry.PartitionKey.Replace(firstName, ""));
                    Assert.IsTrue(id != entryId, "we should throw on entry #5");

                    Assert.AreEqual(entry._rtable_ViewId, staleViewId, "CreateReplicatedQuery: entry viewId should be '5'");
                }
            }
            catch (ReplicatedTableStaleViewException ex)
            {
                Assert.IsTrue(ex.ErrorCode == ReplicatedTableViewErrorCodes.ViewIdSmallerThanEntryViewId);
                Assert.IsTrue(ex.Message.Contains(string.Format("current _rtable_ViewId {0} is smaller than _rtable_ViewId of existing row {1}", staleViewId, newViewId)), "Got unexpected exception message");
            }

            /*
             * stale client using LINQ: ExecuteQuery
             */
            try
            {
                foreach (var entry in rtable.ExecuteQuery<CustomerEntity>(new TableQuery<CustomerEntity>()))
                {
                    int id = int.Parse(entry.PartitionKey.Replace(firstName, ""));
                    Assert.IsTrue(id != entryId, "we should throw on entry #5");

                    Assert.AreEqual(entry._rtable_ViewId, staleViewId, "ExecuteQuery: entry viewId should be '5'");
                }
            }
            catch (ReplicatedTableStaleViewException ex)
            {
                Assert.IsTrue(ex.ErrorCode == ReplicatedTableViewErrorCodes.ViewIdSmallerThanEntryViewId);
                Assert.IsTrue(ex.Message.Contains(string.Format("current _rtable_ViewId {0} is smaller than _rtable_ViewId of existing row {1}", staleViewId, newViewId)), "Got unexpected exception message");
            }
        }

        [Test(Description = "LINQ queries don't return rows with higher viewIds when flag IgnoreHigherViewIdRows is set")]
        public void LinqQueriesDontReturnRowsWithHighViewIdWhenFlagIgnoreHigherViewIdRowsIsSet()
        {
            TableOperation operation;
            TableResult result;
            CustomerEntity customer;


            /*
             * Set config viewId to 5
             */
            long staleViewId = 5;
            SetConfigViewIdAndIgnoreHigherViewIdRowsFlag(staleViewId, false);
            Assert.AreEqual(staleViewId, this.configurationService.GetTableView(this.repTable.TableName).ViewId, "View should be 5!!!");


            // Insert entries in stale viewId 5
            var rtable = new ReplicatedTable(this.repTable.TableName, this.configurationService);

            string firstName = "FirstName";
            string lastName = "LastName";

            for (int i = 0; i < 10; i++)
            {
                customer = new CustomerEntity(firstName + i, lastName + i);

                operation = TableOperation.Insert(customer);
                rtable.Execute(operation);
            }


            /*
             * Set config new viewId to 6
             */
            long newViewId = 6;
            SetConfigViewIdAndIgnoreHigherViewIdRowsFlag(newViewId, false);
            Assert.AreEqual(newViewId, this.configurationService.GetTableView(this.repTable.TableName).ViewId, "View should be 6!!!");


            // Update entry #5 and #8 in new viewId 6
            foreach (int entryId in new int[]{5, 8})
            {
                operation = TableOperation.Retrieve<CustomerEntity>(firstName + entryId, lastName + entryId);
                result = rtable.Execute(operation);

                Assert.IsTrue(result != null && result.HttpStatusCode == (int)HttpStatusCode.OK && (CustomerEntity)result.Result != null, "Retrieve customer failed");

                customer = (CustomerEntity)result.Result;
                customer.Email = "new_view@email.com";

                operation = TableOperation.Replace(customer);
                result = rtable.Execute(operation);

                Assert.IsTrue(result != null && result.HttpStatusCode == (int)HttpStatusCode.NoContent, "Update customer failed");
            }


            /*
             * Simulate a stale client => Set config viewId back to 5
             *                            and Set 'IgnoreHigherViewIdRows' flag so we ignore rows with higher viewIds
             */
            SetConfigViewIdAndIgnoreHigherViewIdRowsFlag(staleViewId, true);
            Assert.AreEqual(staleViewId, this.configurationService.GetTableView(this.repTable.TableName).ViewId, "View should be 5!!!");

            try
            {
                // Check Retrieve of row #5 and #8 returns NotFound
                foreach (int entryId in new int[] {5, 8})
                {
                    operation = TableOperation.Retrieve<CustomerEntity>(firstName + entryId, lastName + entryId);
                    var retrievedResult = rtable.Execute(operation);

                    Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");
                    Assert.AreEqual((int)HttpStatusCode.NotFound, retrievedResult.HttpStatusCode, "retrievedResult.HttpStatusCode mismatch");
                }
            }
            catch (ReplicatedTableStaleViewException)
            {
                Assert.Fail("Retrieve() is expected to NotFound the row, but got RTableStaleViewException !");
            }

            /*
             * stale client using LINQ: CreateReplicatedQuery
             */
            foreach (var entry in rtable.CreateReplicatedQuery<CustomerEntity>().AsEnumerable())
            {
                int id = int.Parse(entry.PartitionKey.Replace(firstName, ""));

                Assert.AreNotEqual(id, 5, "row #5 should not be returned");
                Assert.AreNotEqual(id, 8, "row #8 should not be returned");

                Assert.AreEqual(entry._rtable_ViewId, staleViewId, "CreateReplicatedQuery: entry viewId should be '5'");
            }

            /*
             * stale client using LINQ: ExecuteQuery
             */
            foreach (var entry in rtable.ExecuteQuery<CustomerEntity>(new TableQuery<CustomerEntity>()))
            {
                int id = int.Parse(entry.PartitionKey.Replace(firstName, ""));

                Assert.AreNotEqual(id, 5, "row #5 should not be returned");
                Assert.AreNotEqual(id, 8, "row #8 should not be returned");

                Assert.AreEqual(entry._rtable_ViewId, staleViewId, "CreateReplicatedQuery: entry viewId should be '5'");
            }
        }

        [Test(Description = "LINQ queries are served from Tail when 'ReadViewTailIndex' is not set")]
        public void LinqQueriesAreServedFromTailWhenReadViewTailIndexIsNotSet()
        {
            var rtable = new ReplicatedTable(this.repTable.TableName, this.configurationService);

            string firstName = "FirstName";
            string lastName = "LastName";

            int dataSize = 5;

            /*
             * 1 - insert entries
             */
            for (int i = 0; i < dataSize; i++)
            {
                var customer = new CustomerEntity(firstName + i, lastName + i);
                customer.Email = "***";

                TableOperation operation = TableOperation.Insert(customer);
                rtable.Execute(operation);
            }


            // Identify the Tail account
            View view = this.configurationWrapper.GetWriteView();
            Assert.IsTrue(view.Chain.Count > 1, "expects at least one replica!");

            string accountNameToTamper = view.Chain.Last().Item1.StorageAccountName;
            Console.WriteLine("RunHttpManglerBehaviorHelper(): accountNameToTamper={0}", accountNameToTamper);

            // We will fail requests to update a row in Tail
            ProxyBehavior[] behaviors =
            {
                TamperBehaviors.TamperAllRequestsIf(
                    (session =>
                    {
                        session.oRequest.FailSession((int)HttpStatusCode.ServiceUnavailable, "ServerBusy", "");
                    }),
                    (session =>
                    {
                        var body = session.GetRequestBodyAsString();

                        // Fail Lock to Tail by stale view
                        if (session.hostname.Contains(accountNameToTamper + ".") &&
                            session.HTTPMethodIs("PUT") &&
                            body.Contains("\"_rtable_RowLock\":true"))
                        {
                            return true;
                        }

                        return false;
                    })),
            };

            using (new HttpMangler(false, behaviors))
            {
                /*
                * 2 - update entries
                */
                for (int i = 0; i < dataSize; i++)
                {
                    TableOperation operation = TableOperation.Retrieve<CustomerEntity>(firstName + i, lastName + i);
                    TableResult retrievedResult = rtable.Execute(operation);

                    var customer = (CustomerEntity)retrievedResult.Result;
                    customer.Email = "updated";

                    TableOperation replaceOperation = TableOperation.Replace(customer);
                    TableResult replaceResult = rtable.Execute(replaceOperation);

                    Assert.IsNotNull(replaceResult, "replaceResult = null");
                    Assert.AreEqual((int)HttpStatusCode.ServiceUnavailable, replaceResult.HttpStatusCode, "replaceResult.HttpStatusCode mismatch");
                }
            }


            /*
             * 3 - CreateQuery is served from [Tail], data should be old
             */
            foreach (var customer in rtable.CreateReplicatedQuery<CustomerEntity>().AsEnumerable())
            {
                Assert.AreEqual(customer.Email, "***", "expected old data");
            }


            /*
             * 4 - ExecuteQuery is served from [Tail], data should be old
             */
            foreach (var customer in rtable.ExecuteQuery<CustomerEntity>(new TableQuery<CustomerEntity>()))
            {
                Assert.AreEqual(customer.Email, "***", "expected old data");
            }
        }

        [Test(Description = "LINQ queries are served from Head when 'ReadViewTailIndex' is set")]
        public void LinqQueriesAreServedFromHeadWhenReadViewTailIndexIsSet()
        {
            var rtable = new ReplicatedTable(this.repTable.TableName, this.configurationService);

            string firstName = "FirstName";
            string lastName = "LastName";

            int dataSize = 5;

            /*
             * 1 - insert entries
             */
            for (int i = 0; i < dataSize; i++)
            {
                var customer = new CustomerEntity(firstName + i, lastName + i);
                customer.Email = "***";

                TableOperation operation = TableOperation.Insert(customer);
                rtable.Execute(operation);
            }


            // Identify the Tail account
            View view = this.configurationWrapper.GetWriteView();
            Assert.IsTrue(view.Chain.Count > 1, "expects at least one replica!");

            string accountNameToTamper = view.Chain.Last().Item1.StorageAccountName;
            Console.WriteLine("RunHttpManglerBehaviorHelper(): accountNameToTamper={0}", accountNameToTamper);

            // We will fail requests to update a row in Tail
            ProxyBehavior[] behaviors =
            {
                TamperBehaviors.TamperAllRequestsIf(
                    (session =>
                    {
                        session.oRequest.FailSession((int)HttpStatusCode.ServiceUnavailable, "ServerBusy", "");
                    }),
                    (session =>
                    {
                        var body = session.GetRequestBodyAsString();

                        // Fail Lock to Tail by stale view
                        if (session.hostname.Contains(accountNameToTamper + ".") &&
                            session.HTTPMethodIs("PUT") &&
                            body.Contains("\"_rtable_RowLock\":true"))
                        {
                            return true;
                        }

                        return false;
                    })),
            };

            using (new HttpMangler(false, behaviors))
            {
                /*
                * 2 - update entries
                */
                for (int i = 0; i < dataSize; i++)
                {
                    TableOperation operation = TableOperation.Retrieve<CustomerEntity>(firstName + i, lastName + i);
                    TableResult retrievedResult = rtable.Execute(operation);

                    var customer = (CustomerEntity)retrievedResult.Result;
                    customer.Email = "updated";

                    TableOperation replaceOperation = TableOperation.Replace(customer);
                    TableResult replaceResult = rtable.Execute(replaceOperation);

                    Assert.IsNotNull(replaceResult, "replaceResult = null");
                    Assert.AreEqual((int)HttpStatusCode.ServiceUnavailable, replaceResult.HttpStatusCode, "replaceResult.HttpStatusCode mismatch");
                }
            }


            SetReadViewTailIndex(0);
            Assert.AreEqual(0, this.configurationService.GetTableView(this.repTable.TableName).ReadTailIndex, "ReadTailIndex should be 0!!!");


            /*
             * 3 - CreateQuery is served from [Head], data should be new
             */
            foreach (var customer in rtable.CreateReplicatedQuery<CustomerEntity>().AsEnumerable())
            {
                Assert.AreEqual(customer.Email, "updated", "expected new data");
            }


            /*
             * 4 - ExecuteQuery is served from [Head], data should be new
             */
            foreach (var customer in rtable.ExecuteQuery<CustomerEntity>(new TableQuery<CustomerEntity>()))
            {
                Assert.AreEqual(customer.Email, "updated", "expected new data");
            }
        }


        #region Config helpers

        private void SetConfigViewIdAndIgnoreHigherViewIdRowsFlag(long viewId, bool ignoreHigherViewIdRowsFlag)
        {
            View view = this.configurationWrapper.GetWriteView();

            ReplicatedTableConfiguration config;
            ReplicatedTableQuorumReadResult readStatus = this.configurationService.RetrieveConfiguration(out config);

            Assert.IsTrue(readStatus.Code == ReplicatedTableQuorumReadCode.Success);

            ReplicatedTableConfigurationStore viewConfg = config.GetView(view.Name);
            viewConfg.ViewId = viewId;

            config.SetIgnoreHigherViewIdRowsFlag(ignoreHigherViewIdRowsFlag);

            this.configurationService.UpdateConfiguration(config);
        }

        private void SetReadViewTailIndex(int index)
        {
            View view = this.configurationWrapper.GetWriteView();

            ReplicatedTableConfiguration config;
            ReplicatedTableQuorumReadResult readStatus = this.configurationService.RetrieveConfiguration(out config);

            Assert.IsTrue(readStatus.Code == ReplicatedTableQuorumReadCode.Success);

            ReplicatedTableConfigurationStore viewConfg = config.GetView(view.Name);
            viewConfg.ReadViewTailIndex = index;

            this.configurationService.UpdateConfiguration(config);
        }

        #endregion
    }
}
