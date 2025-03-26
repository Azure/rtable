

namespace Microsoft.Azure.Toolkit.Replication.Test
{
    using System.Net;
    using Microsoft.WindowsAzure.Test.Network;
    using Microsoft.WindowsAzure.Test.Network.Behaviors;
    using NUnit.Framework;
    using System;
    using global::Azure;
    using global::Azure.Data.Tables;

    [TestFixture]
    [Parallelizable(ParallelScope.None)]
    public class RTableRetrieveTests : HttpManglerTestBase
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

        [Test]
        public void RetrieveThrowsBadRequest()
        {
            string jobType = "jobType//RTableWrapperCRUDTest";
            string jobId = "jobId//RTableWrapperCRUDTest";
            int getCallCounts = 0;

            var manglingBehaviors = new[]
            {
                TamperBehaviors.TamperAllRequestsIf(
                    (session) =>
                    {
                        getCallCounts++;
                    }, 
                    (session) =>
                    {
                        if(session.HTTPMethodIs("GET"))
                        {
                            return true;
                        }
                        return false;
                    })
            };

            using (new HttpMangler(false, manglingBehaviors))
            {

                Assert.Throws<RequestFailedException>(() =>
                {
                    try
                    {
                        this.rtableWrapper.ReadEntity(jobType, jobId);
                    }
                    catch (RequestFailedException rfe)
                    {
                        Assert.AreEqual((int)HttpStatusCode.BadRequest, rfe.Status);
                        throw;
                    }
                });
            }

            Assert.AreEqual(1, getCallCounts);
        }

        [Test(Description = "When read from Tail returns 'ServiceUnavailable', we should read from Head and succeed")]
        public void WhenReadFromTailFailsWithServiceUnavailableWeReadFromHeadAndSucceed()
        {
            View view = this.configurationWrapper.GetWriteView();
            Assert.IsTrue(view.Chain.Count > 1, "expects at least 2 replicas!");

            // Insert one entry
            var rtable = new ReplicatedTable(this.repTable.TableName, this.configurationService);

            string firstName = "FirstName01";
            string lastName = "LastName01";

            var customer = new CustomerEntity(firstName, lastName);
            rtable.Insert(customer);


            // Using xstore modify the row in each replica individually ... so we know, later, which replica RTable will retrieve from
            for (int replicaIndex = 0; replicaIndex < this.cloudTableClients.Count; replicaIndex++)
            {
                TableServiceClient tableClient = this.cloudTableClients[replicaIndex];
                TableClient table = tableClient.GetTableClient(this.repTable.TableName);

                var retrieveResult = table.GetEntity<CustomerEntity>(firstName, lastName);

                customer = retrieveResult.Value;
                customer.Email = replicaIndex.ToString(); // Head = 0
                // ...  = 1
                // Tail = 2

                var updateResult = table.UpdateEntity(customer, customer.ETag);

                Assert.IsNotNull(updateResult, "updateResult = null");
                Console.WriteLine("updateResult.HttpStatusCode = {0}", updateResult.Status);
                Assert.AreEqual((int)HttpStatusCode.NoContent, updateResult.Status, "updateResult.HttpStatusCode mismatch");
            }

            string accountNameToNotTamper = this.rtableTestConfiguration.StorageInformation.AccountNames[0];
            Console.WriteLine("RunHttpManglerBehaviorHelper(): accountNameToNotTamper={0}", accountNameToNotTamper);

            // Delay behavior
            ProxyBehavior[] behaviors =
            {
                TamperBehaviors.TamperAllRequestsIf(
                    (session =>
                    {
                        session.oRequest.FailSession((int) HttpStatusCode.ServiceUnavailable, "ServerBusy", "");
                    }),
                    (session =>
                    {
                        if (session.HTTPMethodIs("Get"))
                        {
                            // Fail Get from all replicas, except Head
                            if(!session.hostname.Contains(accountNameToNotTamper + "."))
                            {
                                return true;
                            }
                        }

                        return false;
                    })),
            };

            using (new HttpMangler(false, behaviors))
            {
                TableResult retrievedResult = repTable.Retrieve(firstName, lastName);

                Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");
                Assert.AreEqual((int)HttpStatusCode.OK, retrievedResult.HttpStatusCode, "retrievedResult.HttpStatusCode mismatch");
                Assert.AreNotEqual(null, retrievedResult.Result, "retrievedResult.Result = null");

                customer = new CustomerEntity((ReplicatedTableEntity)retrievedResult.Result);
                Assert.AreEqual(customer.Email, (0).ToString(), "we should have read the row from Head");
            }
        }

        [Test(Description = "When read from all replicas returns 'ServiceUnavailable', we should return 'ServiceUnavailable'")]
        public void WhenReadFromAllReplicasFailsWithServiceUnavailableWeShouldReturnServiceUnavailable()
        {
            View view = this.configurationWrapper.GetWriteView();
            Assert.IsTrue(view.Chain.Count > 1, "expects at least 2 replicas!");

            // Insert one entry
            var rtable = new ReplicatedTable(this.repTable.TableName, this.configurationService);

            string firstName = "FirstName01";
            string lastName = "LastName01";

            var customer = new CustomerEntity(firstName, lastName);
            rtable.Insert(customer);


            // Using xstore modify the row in each replica individually ... so we know, later, which replica RTable will retrieve from
            for (int replicaIndex = 0; replicaIndex < this.cloudTableClients.Count; replicaIndex++)
            {
                TableServiceClient tableClient = this.cloudTableClients[replicaIndex];
                TableClient table = tableClient.GetTableClient(this.repTable.TableName);

                var retrieveResult = table.GetEntity<CustomerEntity>(firstName, lastName);

                customer = retrieveResult.Value;
                customer.Email = replicaIndex.ToString(); // Head = 0
                // ...  = 1
                // Tail = 2

                var updateResult = table.UpdateEntity(customer, customer.ETag);

                Assert.IsNotNull(updateResult, "updateResult = null");
                Console.WriteLine("updateResult.HttpStatusCode = {0}", updateResult.Status);
                Assert.AreEqual((int)HttpStatusCode.NoContent, updateResult.Status, "updateResult.HttpStatusCode mismatch");
            }

            // string accountNameToNotTamper = this.rtableTestConfiguration.StorageInformation.AccountNames[0];
            // Console.WriteLine("RunHttpManglerBehaviorHelper(): accountNameToNotTamper={0}", accountNameToNotTamper);

            // Delay behavior
            ProxyBehavior[] behaviors =
            {
                TamperBehaviors.TamperAllRequestsIf(
                    (session =>
                    {
                        session.oRequest.FailSession((int) HttpStatusCode.ServiceUnavailable, "ServerBusy", "");
                    }),
                    (session =>
                    {
                        if (session.HTTPMethodIs("Get"))
                        {
                            // Fail Get from all replicas, including Head
                            //if(!session.hostname.Contains(accountNameToNotTamper + "."))
                            {
                                return true;
                            }
                        }

                        return false;
                    })),
            };

            using (new HttpMangler(false, behaviors))
            {
                TableResult retrievedResult = repTable.Retrieve(firstName, lastName);

                Assert.AreNotEqual(null, retrievedResult, "retrievedResult = null");
                Assert.AreEqual((int)HttpStatusCode.ServiceUnavailable, retrievedResult.HttpStatusCode, "retrievedResult.HttpStatusCode mismatch");
                Assert.AreEqual(null, retrievedResult.Result, "retrievedResult.Result = null");
            }
        }

    }
}
