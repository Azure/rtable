
using System.Collections.Generic;

namespace Microsoft.Azure.Toolkit.Replication.Test
{
    using NUnit.Framework;

    // Needed to access protected members ...
    class ReplicatedTableConfiguredTableAccessor : ReplicatedTableConfiguredTable
    {
        public new bool IsViewReferenced(string viewName)
        {
            return base.IsViewReferenced(viewName);
        }

        public new bool IsAnyViewNullOrEmpty()
        {
            return base.IsAnyViewNullOrEmpty();
        }

        public new string GetViewForPartition(string partition = null)
        {
            return base.GetViewForPartition(partition);
        }

        public new bool IsTablePartitioned()
        {
            return base.IsTablePartitioned();
        }
    }

    [TestFixture]
    public class ReplicatedTableConfiguredTableTest
    {
        [OneTimeSetUp]
        public void TestFixtureSetup()
        {
        }

        [OneTimeTearDown]
        public void TestFixtureTearDown()
        {
        }

        [Test(Description = "Test IsViewReferenced()")]
        public void TestIsViewReferenced()
        {
            // ViewName is null
            var conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = null,
            };
            Assert.IsFalse(conf.IsViewReferenced(null));
            Assert.IsFalse(conf.IsViewReferenced("SomeView"));

            // ViewName is null
            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "",
            };
            Assert.IsFalse(conf.IsViewReferenced(null));
            Assert.IsFalse(conf.IsViewReferenced("SomeView"));

            // ViewName has a value
            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "view1",
            };
            Assert.IsFalse(conf.IsViewReferenced(null));
            Assert.IsFalse(conf.IsViewReferenced("SomeView"));
            Assert.True(conf.IsViewReferenced("view1"));
            Assert.True(conf.IsViewReferenced("VIEW1"));

            // - Disable Partitioning
            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "DefaultView",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "view0" },
                    { "1", "view1" },
                    { "2", "view2" },
                }
            };

            Assert.False(conf.IsViewReferenced(null));
            Assert.False(conf.IsViewReferenced("SomeView"));

            // - Default view of the table
            Assert.True(conf.IsViewReferenced("DefaultView"));
            Assert.True(conf.IsViewReferenced("DEFAULTVIEW"));

            // - The view is not associated with a partition value => treat as not referenced view
            Assert.False(conf.IsViewReferenced("view0"));
            Assert.False(conf.IsViewReferenced("VIEW0"));

            // - these views are not referenced since partitioning is disabled
            Assert.False(conf.IsViewReferenced("view1"));
            Assert.False(conf.IsViewReferenced("VIEW1"));

            Assert.False(conf.IsViewReferenced("view2"));
            Assert.False(conf.IsViewReferenced("VIEW2"));


            // - Enable Partitioning
            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "DefaultView",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "view0" },
                    { "1", "view1" },
                    { "2", "view2" },
                }
            };

            Assert.False(conf.IsViewReferenced(null));
            Assert.False(conf.IsViewReferenced("SomeView"));

            // - Default view of the table
            Assert.True(conf.IsViewReferenced("DefaultView"));
            Assert.True(conf.IsViewReferenced("DEFAULTVIEW"));

            // - The view is not associated with a partition value => treat as not referenced view
            Assert.False(conf.IsViewReferenced("view0"));
            Assert.False(conf.IsViewReferenced("VIEW0"));

            // - Other referenced views
            Assert.True(conf.IsViewReferenced("view1"));
            Assert.True(conf.IsViewReferenced("VIEW1"));

            Assert.True(conf.IsViewReferenced("view2"));
            Assert.True(conf.IsViewReferenced("VIEW2"));
        }

        [Test(Description = "Test IsAnyViewNullOrEmpty()")]
        public void TestIsAnyViewNullOrEmpty()
        {
            // ViewName is null
            var conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = null,
            };
            Assert.IsTrue(conf.IsAnyViewNullOrEmpty());

            // ViewName is null
            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "",
            };
            Assert.IsTrue(conf.IsAnyViewNullOrEmpty());

            // ViewName has a value
            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "view1",
            };
            Assert.IsFalse(conf.IsAnyViewNullOrEmpty());

            // Partition views ?
            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "view1",
                PartitionsToViewMap = null,
            };
            Assert.IsFalse(conf.IsAnyViewNullOrEmpty());

            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "view1",
                PartitionsToViewMap = new Dictionary<string, string>(),
            };
            Assert.IsFalse(conf.IsAnyViewNullOrEmpty());

            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "view",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "" },
                }
            };
            Assert.IsFalse(conf.IsAnyViewNullOrEmpty());

            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "view",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "view" },
                }
            };
            Assert.IsFalse(conf.IsAnyViewNullOrEmpty());

            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "view",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "view" },
                    { "1", "view1" },
                    { "2", "view2" },
                    { "3", "view3" },
                }
            };
            Assert.IsFalse(conf.IsAnyViewNullOrEmpty());

            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "view",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "view" },
                    { "1", "view1" },
                    { "2", "" },
                    { "3", "view3" },
                }
            };
            Assert.IsTrue(conf.IsAnyViewNullOrEmpty());
        }

        [Test(Description = "Test GetViewForPartition()")]
        public void TestGetViewForPartition()
        {
            // - no partitions defined
            var conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "DefaultView",
            };
            Assert.IsTrue(conf.GetViewForPartition(null) == conf.ViewName);
            Assert.IsTrue(conf.GetViewForPartition("") == conf.ViewName);
            Assert.IsTrue(conf.GetViewForPartition("X") == conf.ViewName);
            Assert.IsTrue(conf.GetViewForPartition("x") == conf.ViewName);

            // - partitions defined but partition is disabled
            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "DefaultView",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "view0" },
                    { "X", "view1" },
                    { "Y", "view2" },
                    { "Z", "" },
                }
            };
            Assert.IsTrue(conf.GetViewForPartition(null) == conf.ViewName);
            Assert.IsTrue(conf.GetViewForPartition("") == conf.ViewName);
            Assert.IsTrue(conf.GetViewForPartition("X") == conf.ViewName);
            Assert.IsTrue(conf.GetViewForPartition("x") == conf.ViewName);
            Assert.IsTrue(conf.GetViewForPartition("Z") == conf.ViewName);
            Assert.IsTrue(conf.GetViewForPartition("z") == conf.ViewName);

            // - partition enabled
            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "DefaultView",
                PartitionOnProperty = "SomeProperty",
                PartitionsToViewMap = null,
            };
            Assert.IsTrue(conf.GetViewForPartition(null) == conf.ViewName);
            Assert.IsTrue(conf.GetViewForPartition("") == conf.ViewName);
            Assert.IsTrue(conf.GetViewForPartition("X") == conf.ViewName);
            Assert.IsTrue(conf.GetViewForPartition("x") == conf.ViewName);

            // - variant ...
            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "DefaultView",
                PartitionOnProperty = "SomeProperty",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "view0" },
                    { "X", "view1" },
                    { "Y", "view2" },
                    { "Z", "" },
                }
            };
            Assert.IsTrue(conf.GetViewForPartition(null) == conf.ViewName);
            Assert.IsTrue(conf.GetViewForPartition("") == conf.ViewName);
            Assert.IsTrue(conf.GetViewForPartition("A") == conf.ViewName);
            Assert.IsTrue(conf.GetViewForPartition("a") == conf.ViewName);
            Assert.IsTrue(conf.GetViewForPartition("X") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("x") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("Y") == "view2");
            Assert.IsTrue(conf.GetViewForPartition("y") == "view2");
            Assert.IsTrue(conf.GetViewForPartition("Z") == conf.ViewName);
            Assert.IsTrue(conf.GetViewForPartition("z") == conf.ViewName);

            // - variant ...
            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "DefaultView",
                PartitionOnProperty = "SomeProperty",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "view0" },
                    { "X", "" },
                    { "Y", "" },
                    { "Z", "" },
                }
            };
            Assert.IsFalse(conf.IsTablePartitioned());
            Assert.IsTrue(conf.GetViewForPartition(null) == conf.ViewName);
            Assert.IsTrue(conf.GetViewForPartition("") == conf.ViewName);
            Assert.IsTrue(conf.GetViewForPartition("A") == conf.ViewName);
            Assert.IsTrue(conf.GetViewForPartition("a") == conf.ViewName);
            Assert.IsTrue(conf.GetViewForPartition("X") == conf.ViewName);
            Assert.IsTrue(conf.GetViewForPartition("x") == conf.ViewName);
            Assert.IsTrue(conf.GetViewForPartition("Y") == conf.ViewName);
            Assert.IsTrue(conf.GetViewForPartition("y") == conf.ViewName);
            Assert.IsTrue(conf.GetViewForPartition("Z") == conf.ViewName);
            Assert.IsTrue(conf.GetViewForPartition("z") == conf.ViewName);
        }

        [Test(Description = "Test IsTablePartitioned()")]
        public void TestIsTablePartitioned()
        {
            // Partitioning not enabled
            var conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "view1",
            };
            Assert.IsFalse(conf.IsTablePartitioned());
            Assert.IsTrue(conf.GetViewForPartition(null) == "view1");
            Assert.IsTrue(conf.GetViewForPartition("") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("1") == "view1");

            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "view1",
                PartitionsToViewMap = null,
            };
            Assert.IsFalse(conf.IsTablePartitioned());
            Assert.IsTrue(conf.GetViewForPartition(null) == "view1");
            Assert.IsTrue(conf.GetViewForPartition("") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("1") == "view1");

            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "view1",
                PartitionsToViewMap = new Dictionary<string, string>(),
            };
            Assert.IsFalse(conf.IsTablePartitioned());
            Assert.IsTrue(conf.GetViewForPartition(null) == "view1");
            Assert.IsTrue(conf.GetViewForPartition("") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("1") == "view1");

            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "view1",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "" },
                }
            };
            Assert.IsFalse(conf.IsTablePartitioned());
            Assert.IsTrue(conf.GetViewForPartition(null) == "view1");
            Assert.IsTrue(conf.GetViewForPartition("") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("1") == "view1");

            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "view1",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "viewIgnored" },
                }
            };
            Assert.IsFalse(conf.IsTablePartitioned());
            Assert.IsTrue(conf.GetViewForPartition(null) == "view1");
            Assert.IsTrue(conf.GetViewForPartition("") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("1") == "view1");

            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "view1",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "viewIgnored" },
                    { "1", "view1" },
                    { "2", "view2" },
                    { "3", "view3" },
                }
            };
            Assert.IsFalse(conf.IsTablePartitioned());
            Assert.IsTrue(conf.GetViewForPartition(null) == "view1");
            Assert.IsTrue(conf.GetViewForPartition("") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("1") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("2") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("3") == "view1");

            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "view1",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "viewIgnored" },
                    { "1", "view1" },
                    { "2", "" },
                    { "3", "" },
                }
            };
            Assert.IsFalse(conf.IsTablePartitioned());
            Assert.IsTrue(conf.GetViewForPartition(null) == "view1");
            Assert.IsTrue(conf.GetViewForPartition("") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("1") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("2") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("3") == "view1");



            // Partitioning enabled
            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "view1",
                PartitionOnProperty = "X",
            };
            Assert.IsFalse(conf.IsTablePartitioned());
            Assert.IsTrue(conf.GetViewForPartition(null) == "view1");
            Assert.IsTrue(conf.GetViewForPartition("") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("1") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("2") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("3") == "view1");

            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "view1",
                PartitionOnProperty = "X",
                PartitionsToViewMap = null,
            };
            Assert.IsFalse(conf.IsTablePartitioned());
            Assert.IsTrue(conf.GetViewForPartition(null) == "view1");
            Assert.IsTrue(conf.GetViewForPartition("") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("1") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("2") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("3") == "view1");

            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "view1",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>(),
            };
            Assert.IsFalse(conf.IsTablePartitioned());
            Assert.IsTrue(conf.GetViewForPartition(null) == "view1");
            Assert.IsTrue(conf.GetViewForPartition("") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("1") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("2") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("3") == "view1");

            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "view1",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "" },
                }
            };
            Assert.IsFalse(conf.IsTablePartitioned());
            Assert.IsTrue(conf.GetViewForPartition(null) == "view1");
            Assert.IsTrue(conf.GetViewForPartition("") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("1") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("2") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("3") == "view1");

            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "view1",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "viewIgnored" },
                }
            };
            Assert.IsFalse(conf.IsTablePartitioned());
            Assert.IsTrue(conf.GetViewForPartition(null) == "view1");
            Assert.IsTrue(conf.GetViewForPartition("") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("1") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("2") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("3") == "view1");

            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "view1",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "viewIgnored" },
                    { "1", "view1" },
                    { "2", "view2" },
                    { "3", "view3" },
                }
            };
            Assert.IsTrue(conf.IsTablePartitioned());
            Assert.IsTrue(conf.GetViewForPartition(null) == "view1");
            Assert.IsTrue(conf.GetViewForPartition("") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("1") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("2") == "view2");
            Assert.IsTrue(conf.GetViewForPartition("3") == "view3");

            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "view1",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "viewIgnored" },
                    { "1", "view1" },
                    { "2", "" },
                    { "3", "" },
                }
            };
            Assert.IsTrue(conf.IsTablePartitioned());
            Assert.IsTrue(conf.GetViewForPartition(null) == "view1");
            Assert.IsTrue(conf.GetViewForPartition("") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("1") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("2") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("3") == "view1");

            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "view1",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "viewIgnored" },
                    { "1", "" },
                    { "2", "" },
                    { "3", "" },
                }
            };
            Assert.IsFalse(conf.IsTablePartitioned());
            Assert.IsTrue(conf.GetViewForPartition(null) == "view1");
            Assert.IsTrue(conf.GetViewForPartition("") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("1") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("2") == "view1");
            Assert.IsTrue(conf.GetViewForPartition("3") == "view1");
        }
    }
}
