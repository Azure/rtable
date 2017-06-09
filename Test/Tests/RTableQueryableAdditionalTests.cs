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

                Console.WriteLine(customer.PartitionKey + "-" + customer.RowKey);
                Assert.AreNotEqual(id, 2, "entry #2 should have been deleted");
                Assert.AreNotEqual(id, 4, "entry #4 should have been deleted");
            }
        }
    }
}
