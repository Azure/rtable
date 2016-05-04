
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

            // Build the Partition map
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

            // - Other referenced views
            Assert.True(conf.ReferencingView("view1"));
            Assert.True(conf.ReferencingView("VIEW1"));

            Assert.True(conf.ReferencingView("view2"));
            Assert.True(conf.ReferencingView("VIEW2"));
        }
    }
}
