
using System.Collections.Generic;

namespace Microsoft.Azure.Toolkit.Replication.Test
{
    using NUnit.Framework;

    // Needed to access protected members ...
    class ReplicatedTableConfiguredTableAccessor : ReplicatedTableConfiguredTable
    {
        public bool ReferencingView(string viewName)
        {
            return base.IsViewReferenced(viewName);
        }

        public bool IsAnyViewNullOrEmpty()
        {
            return base.IsAnyViewNullOrEmpty();
        }
    }

    [TestFixture]
    public class ReplicatedTableConfiguredTableTest
    {
        [TestFixtureSetUp]
        public void TestFixtureSetup()
        {
        }

        [TestFixtureTearDown]
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
            Assert.IsFalse(conf.ReferencingView(null));
            Assert.IsFalse(conf.ReferencingView("SomeView"));

            // ViewName is null
            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "",
            };
            Assert.IsFalse(conf.ReferencingView(null));
            Assert.IsFalse(conf.ReferencingView("SomeView"));

            // ViewName has a value
            conf = new ReplicatedTableConfiguredTableAccessor
            {
                ViewName = "view1",
            };
            Assert.IsFalse(conf.ReferencingView(null));
            Assert.IsFalse(conf.ReferencingView("SomeView"));
            Assert.True(conf.ReferencingView("view1"));
            Assert.True(conf.ReferencingView("VIEW1"));

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

            Assert.False(conf.ReferencingView(null));
            Assert.False(conf.ReferencingView("SomeView"));

            // - Default view of the table
            Assert.True(conf.ReferencingView("DefaultView"));
            Assert.True(conf.ReferencingView("DEFAULTVIEW"));

            // - The view is not associated with a partition value => treat as not referenced view
            Assert.False(conf.ReferencingView("view0"));
            Assert.False(conf.ReferencingView("VIEW0"));

            // - these views are not referenced since partitioning is disabled
            Assert.False(conf.ReferencingView("view1"));
            Assert.False(conf.ReferencingView("VIEW1"));

            Assert.False(conf.ReferencingView("view2"));
            Assert.False(conf.ReferencingView("VIEW2"));


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

            Assert.False(conf.ReferencingView(null));
            Assert.False(conf.ReferencingView("SomeView"));

            // - Default view of the table
            Assert.True(conf.ReferencingView("DefaultView"));
            Assert.True(conf.ReferencingView("DEFAULTVIEW"));

            // - The view is not associated with a partition value => treat as not referenced view
            Assert.False(conf.ReferencingView("view0"));
            Assert.False(conf.ReferencingView("VIEW0"));

            // - Other referenced views
            Assert.True(conf.ReferencingView("view1"));
            Assert.True(conf.ReferencingView("VIEW1"));

            Assert.True(conf.ReferencingView("view2"));
            Assert.True(conf.ReferencingView("VIEW2"));
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
    }
}
