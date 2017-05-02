namespace Microsoft.Azure.Toolkit.Replication.Test
{
    using Microsoft.Azure.Toolkit.Replication;
    using NUnit.Framework;

    [TestFixture]
    public class ReplicaInfoTest
    {
        [OneTimeSetUp]
        public void TestFixtureSetup()
        {
        }

        [OneTimeTearDown]
        public void TestFixtureTearDown()
        {
        }

        [Test(Description = "Test ReplicaStatus()")]
        public void TestReplicaStatus()
        {
            var replica = new ReplicaInfo();
            Assert.IsTrue(replica.Status != ReplicaStatus.None);

            // - None
            replica.Status = ReplicaStatus.None;
            Assert.IsTrue(replica.IsReadOnly() == false);
            Assert.IsTrue(replica.IsReadable() == false);
            Assert.IsTrue(replica.IsWriteOnly() == false);
            Assert.IsTrue(replica.IsWritable() == false);

            // - RO
            replica.Status = ReplicaStatus.ReadOnly;
            Assert.IsTrue(replica.IsReadOnly() == true);
            Assert.IsTrue(replica.IsReadable() == true);
            Assert.IsTrue(replica.IsWriteOnly() == false);
            Assert.IsTrue(replica.IsWritable() == false);

            // WO
            replica.Status = ReplicaStatus.WriteOnly;
            Assert.IsTrue(replica.IsReadOnly() == false);
            Assert.IsTrue(replica.IsReadable() == false);
            Assert.IsTrue(replica.IsWriteOnly() == true);
            Assert.IsTrue(replica.IsWritable() == true);

            // RW
            replica.Status = ReplicaStatus.ReadWrite;
            Assert.IsTrue(replica.IsReadOnly() == false);
            Assert.IsTrue(replica.IsReadable() == true);
            Assert.IsTrue(replica.IsWriteOnly() == false);
            Assert.IsTrue(replica.IsWritable() == true);
        }

        [Test(Description = "Test ToString()")]
        public void TestToString()
        {
            var replica = new ReplicaInfo();

            string acc = "some_name";
            string key = "some_key";
            int whenAddedToChain = 8;

            replica.StorageAccountName = acc;
            replica.StorageAccountKey = key;
            replica.ViewInWhichAddedToChain = whenAddedToChain;
            replica.Status = ReplicaStatus.ReadWrite;

            Assert.IsTrue(replica.ToString() == string.Format("Account Name: {0}, AccountKey: {1}, ViewInWhichAddedToChain: {2}, Status: {3}", acc, "***********", whenAddedToChain, ReplicaStatus.ReadWrite));
        }

        [Test(Description = "Test ReplicaEquality()")]
        public void TestReplicaEquality()
        {
            var r1 = new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                ViewInWhichAddedToChain = 8,
            };

            // - vs. NUll
            Assert.IsFalse(r1.Equals(null));

            // - vs. different AccountName
            var r2 = new ReplicaInfo
            {
                StorageAccountName = "Acc2"
            };
            Assert.IsFalse(r1.Equals(r2));

            // - vs. same AccountName but different ViewInWhichAddedToChain
            var r3 = new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                ViewInWhichAddedToChain = 6,
            };
            Assert.IsFalse(r1.Equals(r3));

            // - vs. same AccountName and same ViewInWhichAddedToChain
            var r4 = new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                ViewInWhichAddedToChain = 8,
            };
            Assert.IsTrue(r1.Equals(r4));
        }
    }
}
