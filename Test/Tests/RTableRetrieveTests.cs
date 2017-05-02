
namespace Microsoft.Azure.Toolkit.Replication.Test
{
    using System.Net;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Test.Network;
    using Microsoft.WindowsAzure.Test.Network.Behaviors;
    using NUnit.Framework;

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

                Assert.Throws<StorageException>(() =>
                {
                    try
                    {
                        this.rtableWrapper.ReadEntity(jobType, jobId);
                    }
                    catch (StorageException se)
                    {
                        Assert.IsNotNull(se.InnerException);
                        var webException = se.InnerException as WebException;
                        Assert.IsNotNull(webException);
                        var response = (HttpWebResponse)webException.Response;
                        Assert.IsNotNull(response);
                        Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
                        throw;
                    }
                });
            }

            Assert.AreEqual(1, getCallCounts);
        }
    }
}
