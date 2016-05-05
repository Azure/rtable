namespace Microsoft.Azure.Toolkit.Replication.Test
{
    using Microsoft.Azure.Toolkit.Replication;
    using NUnit.Framework;
    using System.Threading;
    using System;
    using System.IO;
    using System.Collections.Generic;
    using System.Linq;

    // Needed to access protected members ...
    class ReplicatedTableConfigurationAccessor : ReplicatedTableConfiguration
    {
        public void MoveReplicaToHeadAndSetViewToReadOnly(string storageAccountName)
        {
            base.MoveReplicaToHeadAndSetViewToReadOnly(storageAccountName);
        }

        public void EnableWriteOnReplicas(string storageAccountName)
        {
            base.EnableWriteOnReplicas(storageAccountName);
        }

        public void EnableReadWriteOnReplicas(string storageAccountName, List<string> viewsToSkip)
        {
            base.EnableReadWriteOnReplicas(storageAccountName, viewsToSkip);
        }
    }

    [TestFixture]
    public class ReplicatedTableConfigurationTest : RTableLibraryTestBase
    {
        [TestFixtureSetUp]
        public void TestFixtureSetup()
        {
            LoadTestConfiguration();
        }

        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
        }

        #region // *** View UTs *** //

        [Test(Description = "Test SetView()")]
        public void TestSetView()
        {
            ArgumentNullException nullException;
            Exception exception;

            var conf = new ReplicatedTableConfiguration
            {
                LeaseDuration = 5,
            };

            // - Validate viewName param:
            nullException = TestHelper.ExpectedException<ArgumentNullException>(() => conf.SetView(null, null), "SetView should fail if view name is null");
            Assert.IsTrue(nullException.ParamName == "viewName");

            nullException = TestHelper.ExpectedException<ArgumentNullException>(() => conf.SetView("", null), "SetView should fail if view name is empty");
            Assert.IsTrue(nullException.ParamName == "viewName");

            // - Validate config param:
            nullException = TestHelper.ExpectedException<ArgumentNullException>(() => conf.SetView("view1", null), "SetView should fail if config is null");
            Assert.IsTrue(nullException.ParamName == "config");

            var viewConf = new ReplicatedTableConfigurationStore
            {
                ReplicaChain = null
            };
            exception = TestHelper.ExpectedException<Exception>(() => conf.SetView("view1", viewConf), "SetView should fail if replica chain is null");
            Assert.IsTrue(exception.Message == string.Format("View:\'{0}\' has a null replica(s) !!!", "view1"));

            viewConf.ReplicaChain = new List<ReplicaInfo>
            {
                new ReplicaInfo(),
                null,
                new ReplicaInfo(),
            };
            exception = TestHelper.ExpectedException<Exception>(() => conf.SetView("view1", viewConf), "SetView should fail if replica chain any null element");
            Assert.IsTrue(exception.Message == string.Format("View:\'{0}\' has a null replica(s) !!!", "view1"));


            // - We would have here some UTs to validate replica-chains sequences.
            //   That is redundant since class ReplicatedTableConfigurationStore already has UTs which cover all cases.


            // - Add views
            var replicaInfo = new ReplicaInfo
            {
                StorageAccountKey = this.rtableTestConfiguration.StorageInformation.AccountKeys[0],
                StorageAccountName = this.rtableTestConfiguration.StorageInformation.AccountNames[0],
            };

            viewConf = new ReplicatedTableConfigurationStore();
            viewConf.ReplicaChain.Add(replicaInfo);

            conf.SetView("view1", viewConf);
            conf.SetView("view2", viewConf);

            // - Fetching ...
            Assert.IsTrue(conf.GetView("view1").Equals(viewConf));
            Assert.IsTrue(conf.GetView("view2").Equals(viewConf));

            // - Replacing ...
            var viewConf2 = new ReplicatedTableConfigurationStore();
            viewConf2.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountKey = this.rtableTestConfiguration.StorageInformation.AccountKeys[0],
                StorageAccountName = this.rtableTestConfiguration.StorageInformation.AccountNames[0],
            });
            viewConf2.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountKey = this.rtableTestConfiguration.StorageInformation.AccountKeys[1],
                StorageAccountName = this.rtableTestConfiguration.StorageInformation.AccountNames[1],
            });

            conf.SetView("view1", viewConf2);
            conf.SetView("view2", viewConf);

            // - Fetching ...
            Assert.IsTrue(conf.GetView("view1").Equals(viewConf2));
            Assert.IsTrue(conf.GetView("view2").Equals(viewConf));

            // *** CONSTRAINT: Tables with ConvertToRTable = true can only refer to a single-replica view ***
            // - Add a table with ConvertToRTable = false, and refering a single-replica view1
            var table1 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "view1",
                ConvertToRTable = false,
            };
            conf.SetTable(table1);
            // - Now, make view1 a 2-replicas view. Since table1 has ConvertToRTable = false => this should PASS
            conf.SetView("view1", viewConf2);

            // - Make view1 a single-replica again, and add a table with ConvertToRTable = true, and refering to a single-replica view1
            conf.SetView("view1", viewConf);
            var table2 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl2",
                ViewName = "view1",
                ConvertToRTable = true,
            };
            conf.SetTable(table2);
            // - Now, make view1 a 2-replicas view. Since table2 has ConvertToRTable = true => this should FAIL
            exception = TestHelper.ExpectedException<Exception>(() => conf.SetView("view1", viewConf2), "SetView should not break existing table constraints");
            Assert.IsTrue(exception.Message == string.Format("Table:\'{0}\' should not have a view:\'{1}\' with more than 1 replica since it is in Conversion mode!", table2.TableName, "view1"));
        }

        [Test(Description = "Test SetView() with Partitioning feature")]
        public void TestSetViewWithPartitioningFeature()
        {
            Exception exception;

            var conf = new ReplicatedTableConfiguration();
            conf.SetView("DefaultView", new ReplicatedTableConfigurationStore());

            // - zero-replica view
            var zeroReplicaView = new ReplicatedTableConfigurationStore();

            // - Single-replica view
            var oneReplicaView = new ReplicatedTableConfigurationStore();
            oneReplicaView.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
            });

            // - 2-replicas view
            var twoReplicasView = new ReplicatedTableConfigurationStore();
            twoReplicasView.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
            });
            twoReplicasView.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
            });


            // *** Cases where Table is not in ConvertMode ***

            // - configure a zero-replica view
            conf.SetView("view1", zeroReplicaView);

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "DefaultView",
                PartitionsToViewMap = null,
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                TableName = "tabl2",
                ViewName = "DefaultView",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>(),
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                TableName = "tabl3",
                ViewName = "DefaultView",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "" },
                }
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                TableName = "tabl4",
                ViewName = "DefaultView",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "view1" }, // this view is ignored since key is empty
                }
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                TableName = "tabl5",
                ViewName = "DefaultView",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "ignoredView" },
                    { "1", "view1" },
                }
            });


            // - update view1 to one-replica view
            conf.SetView("view1", oneReplicaView);

            // - update view1 to two-replica view
            conf.SetView("view1", twoReplicasView);



            // *** Cases where Table is in ConvertMode ***

            // - configure a zero-replica view
            conf.SetView("view1", zeroReplicaView);

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,
                TableName = "tabl1",
                ViewName = "DefaultView",
                PartitionsToViewMap = null,
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,
                TableName = "tabl2",
                ViewName = "DefaultView",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>(),
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,
                TableName = "tabl3",
                ViewName = "DefaultView",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "" },
                }
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,
                TableName = "tabl4",
                ViewName = "DefaultView",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "view1" }, // this view is ignored since key is empty
                }
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,
                TableName = "tabl5",
                ViewName = "DefaultView",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "ignoredView" },
                    { "1", "view1" },
                }
            });

            // view2 is not refered by any table
            conf.SetView("view2", zeroReplicaView);
            conf.SetView("view2", oneReplicaView);
            conf.SetView("view2", twoReplicasView);

            // - update view1 to one-replica view
            conf.SetView("view1", oneReplicaView);

            // - update view1 to two-replica view => FAIL bcz "table4" is refering "view1"
            exception = TestHelper.ExpectedException<Exception>(() => conf.SetView("view1", twoReplicasView), "SetView should fail if the view is a partition view of a table in conversion mode");
            Assert.IsTrue(exception.Message == string.Format("Table:\'{0}\' should not have a view:\'{1}\' with more than 1 replica since it is in Conversion mode!", "tabl5", "view1"));
        }

        [Test(Description = "Test GetView()")]
        public void TestGetView()
        {
            var conf = new ReplicatedTableConfiguration
            {
                LeaseDuration = 5,
            };

            var replicaInfo = new ReplicaInfo
            {
                StorageAccountKey = this.rtableTestConfiguration.StorageInformation.AccountKeys[0],
                StorageAccountName = this.rtableTestConfiguration.StorageInformation.AccountNames[0],
            };

            var viewConf = new ReplicatedTableConfigurationStore();
            viewConf.ReplicaChain.Add(replicaInfo);

            conf.SetView("view1", viewConf);
            conf.SetView("view2", viewConf);

            // - Fetching ...
            Assert.IsTrue(conf.GetView(null) == null);
            Assert.IsTrue(conf.GetView("") == null);
            Assert.IsTrue(conf.GetView("NotExistingView") == null);
            Assert.IsTrue(conf.GetView("view1").Equals(viewConf));
            Assert.IsTrue(conf.GetView("view2").Equals(viewConf));
        }

        [Test(Description = "Test RemoveView()")]
        public void TestRemoveView()
        {
            Exception exception;

            var conf = new ReplicatedTableConfiguration
            {
                LeaseDuration = 5,
            };

            // - Add views
            var replicaInfo = new ReplicaInfo
            {
                StorageAccountKey = this.rtableTestConfiguration.StorageInformation.AccountKeys[0],
                StorageAccountName = this.rtableTestConfiguration.StorageInformation.AccountNames[0],
            };

            var viewConf = new ReplicatedTableConfigurationStore();
            viewConf.ReplicaChain.Add(replicaInfo);

            conf.SetView("view1", viewConf);
            conf.SetView("view2", viewConf);

            // - Checking ...
            Assert.IsTrue(conf.GetView("view1").Equals(viewConf));
            Assert.IsTrue(conf.GetView("view2").Equals(viewConf));

            // - Removing ...
            conf.RemoveView(null);
            conf.RemoveView("");
            conf.RemoveView("NotExisting");
            Assert.IsTrue(conf.GetView("view1").Equals(viewConf));
            Assert.IsTrue(conf.GetView("view2").Equals(viewConf));

            // - Removing ...
            conf.RemoveView("view1");
            Assert.IsTrue(conf.GetView("view1") == null);
            Assert.IsTrue(conf.GetView("view2").Equals(viewConf));

            // - Removing ...
            conf.RemoveView("view2");
            Assert.IsTrue(conf.GetView("view1") == null);
            Assert.IsTrue(conf.GetView("view2") == null);

            // Add back views
            conf.SetView("view1", viewConf);
            conf.SetView("view2", viewConf);

            // Configure tables:
            // this doesn't refer any view
            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                TableName = "tbl1"
            });
            // this table refers to view2
            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                TableName = "tbl2",
                ViewName = "view2",
            });

            // - Removing ...
            conf.RemoveView("view1");
            Assert.IsTrue(conf.GetView("view1") == null);
            Assert.IsTrue(conf.GetView("view2").Equals(viewConf));

            // - Removing a view that is referenced by a table
            exception = TestHelper.ExpectedException<Exception>(() => conf.RemoveView("view2"), "RemoveSetView should fail if view is referenced");
            Assert.IsTrue(exception.Message == string.Format("View:\'{0}\' is referenced by table:\'{1}\'! First, delete the table then the view.", "view2", "tbl2"));

            // - Deleting the table first ...
            conf.RemoveTable("tbl2");
            Assert.IsTrue(conf.GetTable("tbl2") == null);

            // - now delete the view
            conf.RemoveView("view2");
        }

        [Test(Description = "Test RemoveView() with Partitioning feature")]
        public void TestRemoveViewWithPartitioningFeature()
        {
            Exception exception;

            var conf = new ReplicatedTableConfiguration();
            conf.SetView("DefaultView", new ReplicatedTableConfigurationStore());

            // - configure views
            conf.SetView("view1", new ReplicatedTableConfigurationStore());
            conf.SetView("view2", new ReplicatedTableConfigurationStore());

            // - configure tables
            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "DefaultView",
                PartitionsToViewMap = null,
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                TableName = "tabl2",
                ViewName = "DefaultView",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>(),
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                TableName = "tabl3",
                ViewName = "DefaultView",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "" },
                }
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                TableName = "tabl4",
                ViewName = "DefaultView",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "view1" }, // this view is ignored since key is empty
                }
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                TableName = "tabl5",
                ViewName = "DefaultView",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "ignoredView" },
                    { "1", "view1" },
                }
            });


            // - Removing ... "view2" is not a partition view of any table
            conf.RemoveView("view2");
            Assert.IsTrue(conf.GetView("view2") == null);

            // - Removing ... "view1" is a partition view of "tabl5"
            exception = TestHelper.ExpectedException<Exception>(() => conf.RemoveView("view1"), "RemoveView should fail if the view is a partition view of a table");
            Assert.IsTrue(exception.Message == string.Format("View:\'{0}\' is referenced by table:\'{1}\'! First, delete the table then the view.", "view1", "tabl5"));
            Assert.IsFalse(conf.GetView("view1") == null);


            // - Deleting the table first ...
            conf.RemoveTable("tabl5");
            Assert.IsTrue(conf.GetTable("tabl5") == null);

            // - now delete the view
            conf.RemoveView("view1");
        }

        #endregion


        #region // *** Table UTs *** //

        [Test(Description = "Test SetTable()")]
        public void TestSetTable()
        {
            ArgumentNullException nullException;
            Exception exception;

            var conf = new ReplicatedTableConfiguration
            {
                LeaseDuration = 5,
            };

            // - Validate config param:
            nullException = TestHelper.ExpectedException<ArgumentNullException>(() => conf.SetTable(null), "SetTable should fail if config is null");
            Assert.IsTrue(nullException.ParamName == "config");

            // - Validate TableName param:
            var tableConf = new ReplicatedTableConfiguredTable
            {
                TableName = null
            };
            nullException = TestHelper.ExpectedException<ArgumentNullException>(() => conf.SetTable(tableConf), "SetTable should fail if TableName is null");
            Assert.IsTrue(nullException.ParamName == "TableName");

            tableConf = new ReplicatedTableConfiguredTable
            {
                TableName = ""
            };
            nullException = TestHelper.ExpectedException<ArgumentNullException>(() => conf.SetTable(tableConf), "SetTable should fail if TableName is empty");
            Assert.IsTrue(nullException.ParamName == "TableName");

            // - It is ok to add a table that doesn't refer any view
            var table1 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = null,
            };
            conf.SetTable(table1);

            var table2 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl2",
                ViewName = "",
            };
            conf.SetTable(table2);

            // - If the table refers a view, the view has to exist
            var table3 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl3",
                ViewName = "view1",
            };
            exception = TestHelper.ExpectedException<Exception>(() => conf.SetTable(table3), "SetTable should fail if table refers a missing view");
            Assert.IsTrue(exception.Message == string.Format("Table:\'{0}\' refers a missing view:\'{1}\'! First, create the view and then configure the table.",table3.TableName, table3.ViewName));

            // - Add a view and configure the table
            conf.SetView("view1", new ReplicatedTableConfigurationStore());
            conf.SetTable(table3);

            // - Add a view with 2 replicas chain
            var viewConf = new ReplicatedTableConfigurationStore();
            viewConf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountKey = this.rtableTestConfiguration.StorageInformation.AccountKeys[0],
                StorageAccountName = this.rtableTestConfiguration.StorageInformation.AccountNames[0],
            });
            viewConf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountKey = this.rtableTestConfiguration.StorageInformation.AccountKeys[1],
                StorageAccountName = this.rtableTestConfiguration.StorageInformation.AccountNames[1],
            });

            conf.SetView("view2", viewConf);

            // - Table not in convert mode, so it can refer to a view with many replicas
            var table4 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl4",
                ViewName = "view2",
                ConvertToRTable = false,
            };
            conf.SetTable(table4);

            // Table is in convert mode, but refering no view
            var table5 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl5",
                ViewName = "",
                ConvertToRTable = true,
            };
            conf.SetTable(table5);

            // Table is in convert mode, can't refer to a view with many replicas
            var table6 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl6",
                ViewName = "view2",
                ConvertToRTable = true,
            };
            exception = TestHelper.ExpectedException<Exception>(() => conf.SetTable(table6), "SetTable should fail if table in convert mode refers a view with many replica");
            Assert.IsTrue(exception.Message == string.Format("Table:\'{0}\' refers a view:\'{1}\' with more than 1 replica while in Conversion mode!", table6.TableName, table6.ViewName));

            // Table is in convert mode, but refering a view with less than 2 replicas
            var table7 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl7",
                ViewName = "view1",
                ConvertToRTable = true,
            };
            conf.SetTable(table7);

            // Add a default table config
            var defaultTableConfig1 = new ReplicatedTableConfiguredTable
            {
                TableName = "*",
                ViewName = "view1",
                UseAsDefault = true,
            };
            conf.SetTable(defaultTableConfig1);
            Assert.IsTrue(conf.GetDefaultConfiguredTable().Equals(defaultTableConfig1));
            Assert.IsTrue(defaultTableConfig1.UseAsDefault == true);

            // Add a new default table config, it will override previous one since we only allow one default rule
            var defaultTableConfig2 = new ReplicatedTableConfiguredTable
            {
                TableName = "*",
                ViewName = "view2",
                UseAsDefault = true,
            };
            conf.SetTable(defaultTableConfig2);
            Assert.IsTrue(conf.GetDefaultConfiguredTable().Equals(defaultTableConfig2));
            Assert.IsTrue(defaultTableConfig1.UseAsDefault == false);
            Assert.IsTrue(defaultTableConfig2.UseAsDefault == true);
        }

        [Test(Description = "Test SetTable() with Partitioning feature")]
        public void TestSetTableWithPartitioningFeature()
        {
            Exception exception;

            var conf = new ReplicatedTableConfiguration();

            // *** Cases where Table is not in ConvertMode ***

            // - no default view, partition views is null
            var partitionedTable1 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",

                ViewName = "",
                PartitionsToViewMap = null,
            };
            conf.SetTable(partitionedTable1);

            // - no default view, partition views exist but partitioning is disable
            partitionedTable1 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "",

                PartitionOnProperty = null,
                PartitionsToViewMap = new Dictionary<string, string>(),
            };
            exception = TestHelper.ExpectedException<Exception>(() => conf.SetTable(partitionedTable1), "SetTable should fail if table partition map butpartitioning is disabled");
            Assert.IsTrue(exception.Message == string.Format("Table:\'{0}\' can't have a partition view while partitioning is disabled!", partitionedTable1.TableName));

            partitionedTable1 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "",

                PartitionOnProperty = "",
                PartitionsToViewMap = new Dictionary<string, string>(),
            };
            exception = TestHelper.ExpectedException<Exception>(() => conf.SetTable(partitionedTable1), "SetTable should fail if table partition map butpartitioning is disabled");
            Assert.IsTrue(exception.Message == string.Format("Table:\'{0}\' can't have a partition view while partitioning is disabled!", partitionedTable1.TableName));

            // - no default view, partition views is empty
            partitionedTable1 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "",

                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>(),
            };
            conf.SetTable(partitionedTable1);

            // - no default view, but only empty partition i.e. (partition value is not defined i.e. no partition)
            partitionedTable1 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "",

                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "" },
                }
            };
            conf.SetTable(partitionedTable1);

            // - no default view, but only empty partition i.e. (partition value is not defined i.e. no partition)
            partitionedTable1 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "",

                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "view0" },
                }
            };
            conf.SetTable(partitionedTable1);

            // - no default view, but at least one partition value (key) has an empty view
            partitionedTable1 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "",

                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "view0" },
                    { "1", "" },
                }
            };
            exception = TestHelper.ExpectedException<Exception>(() => conf.SetTable(partitionedTable1), "SetTable should fail if table has no default view and at least one partition value has a view");
            Assert.IsTrue(exception.Message == string.Format("Table:\'{0}\' can't have a partition view but no default view.", partitionedTable1.TableName));

            // - no default view, but at least one partition value (key) has a view
            partitionedTable1 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "",

                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "" },
                    { "1", "view1" },
                }
            };
            exception = TestHelper.ExpectedException<Exception>(() => conf.SetTable(partitionedTable1), "SetTable should fail if table has no default view and at least one partition value has a view");
            Assert.IsTrue(exception.Message == string.Format("Table:\'{0}\' can't have a partition view but no default view.", partitionedTable1.TableName));


            // - we have a default view, ...
            conf.SetView("DefaultView", new ReplicatedTableConfigurationStore());

            // - no partition views
            partitionedTable1 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "DefaultView",

                PartitionsToViewMap = null,
            };
            conf.SetTable(partitionedTable1);

            // - partition views but partitioning is disable
            partitionedTable1 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "DefaultView",

                PartitionOnProperty = null,
                PartitionsToViewMap = new Dictionary<string, string>(),
            };
            exception = TestHelper.ExpectedException<Exception>(() => conf.SetTable(partitionedTable1), "SetTable should fail if table partition map butpartitioning is disabled");
            Assert.IsTrue(exception.Message == string.Format("Table:\'{0}\' can't have a partition view while partitioning is disabled!", partitionedTable1.TableName));

            partitionedTable1 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "DefaultView",

                PartitionOnProperty = "",
                PartitionsToViewMap = new Dictionary<string, string>(),
            };
            exception = TestHelper.ExpectedException<Exception>(() => conf.SetTable(partitionedTable1), "SetTable should fail if table partition map butpartitioning is disabled");
            Assert.IsTrue(exception.Message == string.Format("Table:\'{0}\' can't have a partition view while partitioning is disabled!", partitionedTable1.TableName));

            // - no partition views
            partitionedTable1 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "DefaultView",

                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>(),
            };
            conf.SetTable(partitionedTable1);

            // - we should not allow a partition with no view
            partitionedTable1 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "DefaultView",

                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "view0" }, // considered as not configured view
                    { "1", "" },
                }
            };
            exception = TestHelper.ExpectedException<Exception>(() => conf.SetTable(partitionedTable1), "SetTable should fail if table has a configured partition view which is empty");
            Assert.IsTrue(exception.Message == string.Format("Table:\'{0}\' refers a missing partition view:\'{1}\'! First, create the view and then configure the table.", partitionedTable1.TableName, ""));

            // - view has to exist
            partitionedTable1 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "DefaultView",

                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "view0" }, // considered as not configured view
                    { "1", "view1" },
                }
            };
            exception = TestHelper.ExpectedException<Exception>(() => conf.SetTable(partitionedTable1), "SetTable should fail if table has a configured partition view which doesn't exist");
            Assert.IsTrue(exception.Message == string.Format("Table:\'{0}\' refers a missing partition view:\'{1}\'! First, create the view and then configure the table.", partitionedTable1.TableName, "view1"));


            // - configure view1 ...
            conf.SetView("view1", new ReplicatedTableConfigurationStore());

            // this should be Ok
            partitionedTable1 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "DefaultView",

                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "view0" }, // considered as not configured view
                    { "1", "view1" },
                }
            };
            conf.SetTable(partitionedTable1);

            // view2 is missing ?
            partitionedTable1 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "DefaultView",

                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "view0" },
                    { "1", "view1" },
                    { "2", "view2" },
                }
            };
            exception = TestHelper.ExpectedException<Exception>(() => conf.SetTable(partitionedTable1), "SetTable should fail if table has a configured partition view which doesn't exist");
            Assert.IsTrue(exception.Message == string.Format("Table:\'{0}\' refers a missing partition view:\'{1}\'! First, create the view and then configure the table.", partitionedTable1.TableName, "view2"));



            // *** Cases where Table is in ConvertMode ***

            // - configure view1 with 0 replica
            conf.SetView("view1", new ReplicatedTableConfigurationStore());

            // no partition views
            partitionedTable1 = new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "tabl1",
                ViewName = "DefaultView",

                PartitionsToViewMap = null,
            };
            conf.SetTable(partitionedTable1);

            // - partition views but partitioning is disable
            partitionedTable1 = new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "tabl1",
                ViewName = "DefaultView",

                PartitionOnProperty = null,
                PartitionsToViewMap = new Dictionary<string, string>(),
            };
            exception = TestHelper.ExpectedException<Exception>(() => conf.SetTable(partitionedTable1), "SetTable should fail if table partition map butpartitioning is disabled");
            Assert.IsTrue(exception.Message == string.Format("Table:\'{0}\' can't have a partition view while partitioning is disabled!", partitionedTable1.TableName));

            partitionedTable1 = new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "tabl1",
                ViewName = "DefaultView",

                PartitionOnProperty = "",
                PartitionsToViewMap = new Dictionary<string, string>(),
            };
            exception = TestHelper.ExpectedException<Exception>(() => conf.SetTable(partitionedTable1), "SetTable should fail if table partition map butpartitioning is disabled");
            Assert.IsTrue(exception.Message == string.Format("Table:\'{0}\' can't have a partition view while partitioning is disabled!", partitionedTable1.TableName));

            // no partition views
            partitionedTable1 = new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "tabl1",
                ViewName = "DefaultView",

                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>(),
            };
            conf.SetTable(partitionedTable1);

            // empty partition view will be ignored
            partitionedTable1 = new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "tabl1",
                ViewName = "DefaultView",

                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "" },
                }
            };
            conf.SetTable(partitionedTable1);

            // empty key (partition value) we'll ignore the corresponding partition view
            partitionedTable1 = new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "tabl1",
                ViewName = "DefaultView",

                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    {"", "ignoredView"},
                }
            };
            conf.SetTable(partitionedTable1);

            // this should be Ok
            partitionedTable1 = new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "tabl1",
                ViewName = "DefaultView",

                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "1", "view1" },
                }
            };
            conf.SetTable(partitionedTable1);

            // - configure view1 with 1 replica
            var oneReplicaView = new ReplicatedTableConfigurationStore();
            oneReplicaView.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
            });
            conf.SetView("view1", oneReplicaView);

            // this should be Ok
            partitionedTable1 = new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "tabl1",
                ViewName = "DefaultView",

                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "1", "view1" },
                    { "2", "view1" },
                }
            };
            conf.SetTable(partitionedTable1);

            // - configure view2 with 2 replicas
            var twoReplicaView = new ReplicatedTableConfigurationStore();
            twoReplicaView.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
            });
            twoReplicaView.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
            });
            conf.SetView("view2", twoReplicaView);

            // this should be OK
            partitionedTable1 = new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "tabl1",
                ViewName = "DefaultView",

                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "1", "view1" },
                    { "2", "view2" },
                }
            };
            exception = TestHelper.ExpectedException<Exception>(() => conf.SetTable(partitionedTable1), "SetTable should fail if table in conversion mode has a configured partition view with >=2 replicas");
            Assert.IsTrue(exception.Message == string.Format("Table:\'{0}\' refers a partition view:\'{1}\' with more than 1 replica while in Conversion mode!", partitionedTable1.TableName, "view2"));


            // Add a default table config
            var defaultTableConfig1 = new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "*",
                ViewName = "DefaultView",

                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "1", "view1" },
                },

                UseAsDefault = true,
            };
            conf.SetTable(defaultTableConfig1);
            Assert.IsTrue(conf.GetDefaultConfiguredTable().Equals(defaultTableConfig1));
            Assert.IsTrue(defaultTableConfig1.UseAsDefault == true);

            // Add a new default table config, it will override previous one since we only allow one default rule
            var defaultTableConfig2 = new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "*",
                ViewName = "DefaultView",

                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "1", "view1" },
                    { "2", "view1" },
                },

                UseAsDefault = true,
            };
            conf.SetTable(defaultTableConfig2);
            Assert.IsTrue(conf.GetDefaultConfiguredTable().Equals(defaultTableConfig2));
            Assert.IsTrue(defaultTableConfig1.UseAsDefault == false);
            Assert.IsTrue(defaultTableConfig2.UseAsDefault == true);
        }

        [Test(Description = "Test GetTable()")]
        public void TestGetTable()
        {
            var conf = new ReplicatedTableConfiguration
            {
                LeaseDuration = 5,
            };

            Assert.IsTrue(conf.GetTable(null) == null);
            Assert.IsTrue(conf.GetTable("") == null);
            Assert.IsTrue(conf.GetTable("NotExist") == null);

            // - Add a view
            conf.SetView("view1", new ReplicatedTableConfigurationStore());

            // - Add tables
            var table1 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = null,
            };
            conf.SetTable(table1);
            Assert.IsTrue(conf.GetTable(table1.TableName).Equals(table1));

            var table2 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl2",
                ViewName = "view1",
            };
            conf.SetTable(table2);
            Assert.IsTrue(conf.GetTable(table2.TableName).Equals(table2));
        }

        [Test(Description = "Test GetDefaultConfiguredTable()")]
        public void TestGetDefaultConfiguredTable()
        {
            var conf = new ReplicatedTableConfiguration
            {
                LeaseDuration = 5,
            };

            // - Add a view
            conf.SetView("view1", new ReplicatedTableConfigurationStore());

            // Add a default table config
            var defaultTableConfig1 = new ReplicatedTableConfiguredTable
            {
                TableName = "*",
                ViewName = null,
                UseAsDefault = true,
            };
            conf.SetTable(defaultTableConfig1);
            Assert.IsTrue(conf.GetDefaultConfiguredTable().Equals(defaultTableConfig1));
            Assert.IsTrue(defaultTableConfig1.UseAsDefault == true);

            // Add a new default table config,
            // this will override previous one since we only allow one default rule
            var defaultTableConfig2 = new ReplicatedTableConfiguredTable
            {
                TableName = "*",
                ViewName = "view1",
                UseAsDefault = true,
            };
            conf.SetTable(defaultTableConfig2);
            Assert.IsTrue(conf.GetDefaultConfiguredTable().Equals(defaultTableConfig2));
            Assert.IsTrue(defaultTableConfig1.UseAsDefault == false);
            Assert.IsTrue(defaultTableConfig2.UseAsDefault == true);
        }

        [Test(Description = "Test RemoveTable()")]
        public void TestRemoveTable()
        {
            var conf = new ReplicatedTableConfiguration
            {
                LeaseDuration = 5,
            };

            conf.RemoveTable(null);
            conf.RemoveTable("");

            // - Add view
            conf.SetView("view1", new ReplicatedTableConfigurationStore());

            // - Add tables
            var table1 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = null,
            };
            conf.SetTable(table1);
            Assert.IsTrue(conf.GetTable(table1.TableName).Equals(table1));
            conf.RemoveTable(table1.TableName);
            Assert.IsTrue(conf.GetTable(table1.TableName) == null);

            var table2 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl2",
                ViewName = "view1",
            };
            conf.SetTable(table2);
            Assert.IsTrue(conf.GetTable(table2.TableName).Equals(table2));
            conf.RemoveTable(table2.TableName);
            Assert.IsTrue(conf.GetTable(table2.TableName) == null);
        }

        [Test(Description = "Test IsConfiguredTable()")]
        public void TestIsConfiguredTable()
        {
            var conf = new ReplicatedTableConfiguration
            {
                LeaseDuration = 5,
            };

            conf.RemoveTable(null);
            conf.RemoveTable("");

            // - Add view
            conf.SetView("view1", new ReplicatedTableConfigurationStore());

            // - Add tables
            var table1 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = null,
            };
            conf.SetTable(table1);

            ReplicatedTableConfiguredTable configuredTable;

            Assert.IsTrue(conf.IsConfiguredTable(null, out configuredTable) == false);
            Assert.IsTrue(conf.IsConfiguredTable("", out configuredTable) == false);
            Assert.IsTrue(conf.IsConfiguredTable("MissingTableName", out configuredTable) == false);

            // - becasuse the view is empty, the table is not configured
            Assert.IsTrue(conf.IsConfiguredTable(table1.TableName, out configuredTable) == false);

            // - Change the table view, and re-program the config
            table1 = new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "view1",
            };
            conf.SetTable(table1);

            Assert.IsTrue(conf.IsConfiguredTable(table1.TableName, out configuredTable) == true);
            Assert.IsTrue(configuredTable.Equals(table1));

            // - Add default table rule:
            var defaultTableConfig = new ReplicatedTableConfiguredTable
            {
                TableName = "*",
                ViewName = null,
                UseAsDefault = true,
            };
            conf.SetTable(defaultTableConfig);

            Assert.IsTrue(conf.IsConfiguredTable(null, out configuredTable) == false);
            Assert.IsTrue(conf.IsConfiguredTable("", out configuredTable) == false);
            Assert.IsTrue(conf.IsConfiguredTable("MissingTableName", out configuredTable) == false);
            Assert.IsTrue(conf.IsConfiguredTable(table1.TableName, out configuredTable) == true);
            Assert.IsTrue(configuredTable.Equals(table1));

            // - Re-program the default Table config with a view
            defaultTableConfig = new ReplicatedTableConfiguredTable
            {
                TableName = "*",
                ViewName = "view1",
                UseAsDefault = true,
            };
            conf.SetTable(defaultTableConfig);

            Assert.IsTrue(conf.IsConfiguredTable(null, out configuredTable) == false);
            Assert.IsTrue(configuredTable == null);
            Assert.IsTrue(conf.IsConfiguredTable("", out configuredTable) == false);
            Assert.IsTrue(configuredTable == null);

            Assert.IsTrue(conf.IsConfiguredTable("MissingTableName", out configuredTable) == true);
            Assert.IsTrue(configuredTable.Equals(defaultTableConfig));

            Assert.IsTrue(conf.IsConfiguredTable(table1.TableName, out configuredTable) == true);
            Assert.IsTrue(configuredTable.Equals(table1));
        }

        [Test(Description = "Test IsConfiguredTable() with Partitioning feature")]
        public void TestIsConfiguredTableWithPartitioningFeature()
        {
            ReplicatedTableConfiguredTable configuredTable;

            var conf = new ReplicatedTableConfiguration();
            conf.SetView("DefaultView", new ReplicatedTableConfigurationStore());
            conf.SetView("view1", new ReplicatedTableConfigurationStore());



            // *** cases where default view is not configured ***

            // - configure different tables
            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "",
                PartitionsToViewMap = null,
            });
            Assert.IsFalse(conf.IsConfiguredTable("tabl1", out configuredTable));
            Assert.IsNull(configuredTable);

            // -
            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>(),
            });
            Assert.IsFalse(conf.IsConfiguredTable("tabl1", out configuredTable));
            Assert.IsNull(configuredTable);

            // -
            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "" },
                }
            });
            Assert.IsFalse(conf.IsConfiguredTable("tabl1", out configuredTable));
            Assert.IsNull(configuredTable);

            // -
            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "view1" }, // this view is ignored since key is empty
                }
            });
            Assert.IsFalse(conf.IsConfiguredTable("tabl1", out configuredTable));
            Assert.IsNull(configuredTable);

            // -
            TestHelper.ExpectedException<Exception>(() => conf.SetTable(new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "1", "" },
                }
            }), "can't configure partition view when missing default view");

            // -
            TestHelper.ExpectedException<Exception>(() => conf.SetTable(new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "1", "view1" },
                }
            }), "can't configure partition view when missing default view");



            // *** cases where default view is configured ***

            // - configure different tables
            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "DefaultView",
                PartitionsToViewMap = null,
            });
            Assert.IsTrue(conf.IsConfiguredTable("tabl1", out configuredTable));
            Assert.IsNotNull(configuredTable);

            // -
            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "DefaultView",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>(),
            });
            Assert.IsTrue(conf.IsConfiguredTable("tabl1", out configuredTable));
            Assert.IsNotNull(configuredTable);

            // -
            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "DefaultView",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "" },
                }
            });
            Assert.IsTrue(conf.IsConfiguredTable("tabl1", out configuredTable));
            Assert.IsNotNull(configuredTable);

            // -
            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "DefaultView",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "view1" }, // this view is ignored since key is empty
                }
            });
            Assert.IsTrue(conf.IsConfiguredTable("tabl1", out configuredTable));
            Assert.IsNotNull(configuredTable);

            // -
            TestHelper.ExpectedException<Exception>(() => conf.SetTable(new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "DefaultView",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "1", "" },
                }
            }), "missing partition view");

            // -
            TestHelper.ExpectedException<Exception>(() => conf.SetTable(new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "DefaultView",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "1", "MissingView" },
                }
            }), "partition view has to exist");

            // -
            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                TableName = "tabl1",
                ViewName = "DefaultView",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "ignored" },
                    { "2", "view1" },
                }
            });
            Assert.IsTrue(conf.IsConfiguredTable("tabl1", out configuredTable));
            Assert.IsNotNull(configuredTable);



            // *** Add default table config ***
            Assert.IsFalse(conf.IsConfiguredTable("tabl2", out configuredTable));
            Assert.IsNull(configuredTable);


            // - configure different tables
            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                UseAsDefault = true,
                TableName = "*",
                ViewName = "DefaultView",
                PartitionsToViewMap = null,
            });
            Assert.IsTrue(conf.IsConfiguredTable("tabl2", out configuredTable));
            Assert.IsNotNull(configuredTable);

            // -
            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                UseAsDefault = true,
                TableName = "*",
                ViewName = "DefaultView",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>(),
            });
            Assert.IsTrue(conf.IsConfiguredTable("tabl2", out configuredTable));
            Assert.IsNotNull(configuredTable);

            // -
            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                UseAsDefault = true,
                TableName = "*",
                ViewName = "DefaultView",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "" },
                }
            });
            Assert.IsTrue(conf.IsConfiguredTable("tabl2", out configuredTable));
            Assert.IsNotNull(configuredTable);

            // -
            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                UseAsDefault = true,
                TableName = "*",
                ViewName = "DefaultView",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "view1" }, // this view is ignored since key is empty
                }
            });
            Assert.IsTrue(conf.IsConfiguredTable("tabl2", out configuredTable));
            Assert.IsNotNull(configuredTable);

            // -
            TestHelper.ExpectedException<Exception>(() => conf.SetTable(new ReplicatedTableConfiguredTable
            {
                UseAsDefault = true,
                TableName = "*",
                ViewName = "DefaultView",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "1", "" },
                }
            }), "missing partition view");

            // -
            TestHelper.ExpectedException<Exception>(() => conf.SetTable(new ReplicatedTableConfiguredTable
            {
                UseAsDefault = true,
                TableName = "*",
                ViewName = "DefaultView",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "1", "MissingView" },
                }
            }), "partition view has to exist");

            // -
            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                UseAsDefault = true,
                TableName = "*",
                ViewName = "DefaultView",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "", "ignored" },
                    { "2", "view1" },
                }
            });
            Assert.IsTrue(conf.IsConfiguredTable("tabl2", out configuredTable));
            Assert.IsNotNull(configuredTable);
        }

        #endregion


        #region // *** Config. manipulation UTs *** //

        [Test(Description = "Test equality between ReplicatedTableConfiguration objects")]
        public void TestReplicatedTableConfigurationEquality()
        {
            var conf1 = new ReplicatedTableConfiguration
            {
                LeaseDuration = 5,
            };

            var conf2 = new ReplicatedTableConfiguration
            {
                LeaseDuration = 5,
            };

            Assert.False(conf1.Equals(null));
            Assert.False(conf1.Equals(conf2));
            Assert.False(conf1.GetConfigId() == conf2.GetConfigId());
        }

        [Test(Description = "Test MakeCopy()")]
        public void TestMakeCopy()
        {
            ReplicatedTableConfiguration copy = ReplicatedTableConfiguration.MakeCopy(null);
            Assert.True(copy == null);

            var conf = new ReplicatedTableConfiguration
            {
                LeaseDuration = 5,
            };

            copy = ReplicatedTableConfiguration.MakeCopy(conf);
            Assert.True(conf.Equals(copy));
            Assert.True(conf.GetConfigId() == copy.GetConfigId());
            // but they are different objects
            Assert.False(object.ReferenceEquals(conf, copy));
        }

        [Test(Description = "Test GenerateNewConfigId()")]
        public void TestGenerateNewConfigId()
        {
            ArgumentNullException nullException;

            // - Validate config param:
            nullException = TestHelper.ExpectedException<ArgumentNullException>(() => ReplicatedTableConfiguration.GenerateNewConfigId(null), "Config can't be null");
            Assert.IsTrue(nullException.ParamName == "config");

            var conf = new ReplicatedTableConfiguration
            {
                LeaseDuration = 5,
            };

            // - Configure some views
            var replicaInfo = new ReplicaInfo
            {
                StorageAccountKey = this.rtableTestConfiguration.StorageInformation.AccountKeys[0],
                StorageAccountName = this.rtableTestConfiguration.StorageInformation.AccountNames[0],
            };

            var viewConf = new ReplicatedTableConfigurationStore();
            viewConf.ReplicaChain.Add(replicaInfo);

            conf.SetView("view1", viewConf);
            conf.SetView("view2", viewConf);

            // Configure some tables
            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                TableName = "tbl1"
            });
            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                TableName = "tbl2",
                ViewName = "view2",
            });
            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                TableName = "*",
                ViewName = "view1",
                UseAsDefault = true,
            });


            ReplicatedTableConfiguration copy = ReplicatedTableConfiguration.GenerateNewConfigId(conf);
            Assert.True(copy != null);
            Assert.True(!conf.Equals(copy));
            Assert.True(conf.GetConfigId() != copy.GetConfigId());
            Assert.False(object.ReferenceEquals(conf, copy));

            // But they have same content i.e. if we replace configId in 'copy' with configId from 'conf' we get same strings ?
            Assert.True(copy.ToJson().Replace(copy.GetConfigId().ToString(), conf.GetConfigId().ToString()) == conf.ToJson());
        }

        [Test(Description = "Test FromJson()")]
        public void TestFromJson()
        {
            Exception exception;
            ReplicatedTableConfiguration conf;

            conf = ReplicatedTableConfiguration.FromJson(null);
            Assert.True(conf.GetViewSize() == 0 && conf.GetTableSize() == 0);

            conf = ReplicatedTableConfiguration.FromJson("");
            Assert.True(conf.GetViewSize() == 0 && conf.GetTableSize() == 0);


            string jsonConf = null;

            // - Empty view map and empty table list
            jsonConf = @"{'viewMap': null, 'tableList': null, 'LeaseDuration': 10, 'Id': '450a44a6-26fd-4ca2-89b1-32bf99af9248'}";
            conf = ReplicatedTableConfiguration.FromJson(jsonConf);
            Assert.True(conf.GetViewSize() == 0 && conf.GetTableSize() == 0);
            Assert.True(conf.LeaseDuration == 10);
            Assert.True(conf.GetConfigId().ToString() == "450a44a6-26fd-4ca2-89b1-32bf99af9248");

            jsonConf = @"{'viewMap': '', 'tableList': '', 'LeaseDuration': 10, 'Id': '450a44a6-26fd-4ca2-89b1-32bf99af9248'}";
            conf = ReplicatedTableConfiguration.FromJson(jsonConf);
            Assert.True(conf.GetViewSize() == 0 && conf.GetTableSize() == 0);
            Assert.True(conf.LeaseDuration == 10);
            Assert.True(conf.GetConfigId().ToString() == "450a44a6-26fd-4ca2-89b1-32bf99af9248");

            string configPath = null;


            #region // *** View related UTs *** //

            //- Enforce viewName not empty
            configPath = Directory.GetCurrentDirectory() + "\\" + @"..\Tests\ConfigFiles\SomeViewNamesAreEmpty.txt";
            try
            {
                using (StreamReader sr = new StreamReader(configPath))
                {
                    conf = ReplicatedTableConfiguration.FromJson(sr.ReadToEnd());
                    Assert.True(conf.GetViewSize() == 2 && conf.GetTableSize() == 0);
                    Assert.IsTrue(conf.GetView("view1") != null);
                    Assert.IsTrue(conf.GetView("view2") != null);
                }
            }
            catch (Exception ex)
            {
                Assert.Fail("#View1 - Received exception {0} while parsing {1}", ex.Message, configPath);
            }

            // - Enforce view config not null
            configPath = Directory.GetCurrentDirectory() + "\\" + @"..\Tests\ConfigFiles\SomeViewConfigIsEmpty.txt";
            try
            {
                using (StreamReader sr = new StreamReader(configPath))
                {
                    conf = ReplicatedTableConfiguration.FromJson(sr.ReadToEnd());
                    Assert.True(conf.GetViewSize() == 2 && conf.GetTableSize() == 0);
                    Assert.IsTrue(conf.GetView("view1") != null);
                    Assert.IsTrue(conf.GetView("view2") != null);
                    Assert.IsTrue(conf.GetView("ViewHasNoConfig") == null);
                }
            }
            catch (Exception ex)
            {
                Assert.Fail("#View2 - Received exception {0} while parsing {1}", ex.Message, configPath);
            }

            // - Enforce replicas are not null, and well sequenced
            // *** well-sequencing of replicas is covered in TestSetView() UT as this is common part. Here we won't test on that ***
            configPath = Directory.GetCurrentDirectory() + "\\" + @"..\Tests\ConfigFiles\ViewHasEmptyReplicaChain.txt";
            try
            {
                using (StreamReader sr = new StreamReader(configPath))
                {
                    exception = TestHelper.ExpectedException<Exception>(() => ReplicatedTableConfiguration.FromJson(sr.ReadToEnd()), "Parsing config. fails if replica chain is null");
                    Assert.IsTrue(exception.Message == string.Format("View:\'{0}\' has a null replica(s) !!!", "ViewHasEmptyReplicaChain"));
                }
            }
            catch (Exception ex)
            {
                Assert.Fail("#View3 - Received exception {0} while parsing {1}", ex.Message, configPath);
            }

            // -variant ...
            configPath = Directory.GetCurrentDirectory() + "\\" + @"..\Tests\ConfigFiles\ViewHasSomeNullReplica.txt";
            try
            {
                using (StreamReader sr = new StreamReader(configPath))
                {
                    exception = TestHelper.ExpectedException<Exception>(() => ReplicatedTableConfiguration.FromJson(sr.ReadToEnd()), "Parsing config. fails if replica chain is null");
                    Assert.IsTrue(exception.Message == string.Format("View:\'{0}\' has a null replica(s) !!!", "ViewHasEmptyReplicaChain"));
                }
            }
            catch (Exception ex)
            {
                Assert.Fail("#View4 - Received exception {0} while parsing {1}", ex.Message, configPath);
            }

            #endregion


            #region // *** Table related UTs *** //

            //- Enforce tableList not null
            configPath = Directory.GetCurrentDirectory() + "\\" + @"..\Tests\ConfigFiles\TableConfigNull.txt";
            try
            {
                using (StreamReader sr = new StreamReader(configPath))
                {
                    conf = ReplicatedTableConfiguration.FromJson(sr.ReadToEnd());
                    Assert.True(conf.GetViewSize() == 1 && conf.GetTableSize() == 0);
                }
            }
            catch (Exception ex)
            {
                Assert.Fail("#Table1 - Received exception {0} while parsing {1}", ex.Message, configPath);
            }

            //- Enforce table config not null
            configPath = Directory.GetCurrentDirectory() + "\\" + @"..\Tests\ConfigFiles\SomeTableConfigNull.txt";
            try
            {
                using (StreamReader sr = new StreamReader(configPath))
                {
                    conf = ReplicatedTableConfiguration.FromJson(sr.ReadToEnd());
                    Assert.True(conf.GetViewSize() == 1 && conf.GetTableSize() == 2);
                    Assert.True(conf.GetTable("tbl1") != null);
                    Assert.True(conf.GetTable("tbl2") != null);
                }
            }
            catch (Exception ex)
            {
                Assert.Fail("#Table2 - Received exception {0} while parsing {1}", ex.Message, configPath);
            }

            //- Enforce tableName not null per configured table
            configPath = Directory.GetCurrentDirectory() + "\\" + @"..\Tests\ConfigFiles\SomeTableConfigWithEmptyName.txt";
            try
            {
                using (StreamReader sr = new StreamReader(configPath))
                {
                    conf = ReplicatedTableConfiguration.FromJson(sr.ReadToEnd());
                    Assert.True(conf.GetViewSize() == 1 && conf.GetTableSize() == 2);
                    Assert.True(conf.GetTable("tbl1") != null);
                    Assert.True(conf.GetTable("tbl2") != null);
                }
            }
            catch (Exception ex)
            {
                Assert.Fail("#Table3 - Received exception {0} while parsing {1}", ex.Message, configPath);
            }

            // - Enforce no duplicate table config
            configPath = Directory.GetCurrentDirectory() + "\\" + @"..\Tests\ConfigFiles\DuplicateTableConfig.txt";
            try
            {
                using (StreamReader sr = new StreamReader(configPath))
                {
                    exception = TestHelper.ExpectedException<Exception>(() => ReplicatedTableConfiguration.FromJson(sr.ReadToEnd()), "Config can't have duplicate TableConfig");
                    Assert.IsTrue(exception.Message == string.Format("Table:\'{0}\' is configured more than once! Only one config per table.", "tbl2"));
                }
            }
            catch (Exception ex)
            {
                Assert.Fail("#Table4 - Received exception {0} while parsing {1}", ex.Message, configPath);
            }

            // Enforce that:
            // 1 - each table, if it refers a view, then the view has to exist
            configPath = Directory.GetCurrentDirectory() + "\\" + @"..\Tests\ConfigFiles\TableReferMissingView.txt";
            try
            {
                using (StreamReader sr = new StreamReader(configPath))
                {
                    exception = TestHelper.ExpectedException<Exception>(() => ReplicatedTableConfiguration.FromJson(sr.ReadToEnd()), "Table can't refer a missing view");
                    Assert.IsTrue(exception.Message == string.Format("Table:\'{0}\' refers a missing view:\'{1}\'! First, create the view and then configure the table.", "tbl3", "MissingViewName"));
                }
            }
            catch (Exception ex)
            {
                Assert.Fail("#Table5 - Received exception {0} while parsing {1}", ex.Message, configPath);
            }

            // 2 - and, each table in ConvertToRTable mode has no more than one replica
            configPath = Directory.GetCurrentDirectory() + "\\" + @"..\Tests\ConfigFiles\TableInConvertModeRefersNoneSingleReplica.txt";
            try
            {
                using (StreamReader sr = new StreamReader(configPath))
                {
                    exception = TestHelper.ExpectedException<Exception>(() => ReplicatedTableConfiguration.FromJson(sr.ReadToEnd()), "Table in convert mode can refer only a single-replica view");
                    Assert.IsTrue(exception.Message == string.Format("Table:\'{0}\' refers a view:\'{1}\' with more than 1 replica while in Conversion mode!", "tbl4", "view2"));
                }
            }
            catch (Exception ex)
            {
                Assert.Fail("#Table6 - Received exception {0} while parsing {1}", ex.Message, configPath);
            }

            // - Enforce no more than 1 default configured table (rule)
            configPath = Directory.GetCurrentDirectory() + "\\" + @"..\Tests\ConfigFiles\MultipleDefaultTableConfig.txt";
            try
            {
                using (StreamReader sr = new StreamReader(configPath))
                {
                    exception = TestHelper.ExpectedException<Exception>(() => ReplicatedTableConfiguration.FromJson(sr.ReadToEnd()), "We can't have more than one default table config");
                    Assert.IsTrue(exception.Message == "Can't have more than 1 configured table as a default!");
                }
            }
            catch (Exception ex)
            {
                Assert.Fail("#Table7 - Received exception {0} while parsing {1}", ex.Message, configPath);
            }

            #endregion
        }

        [Test(Description = "Test FromJson() with Partitioning feature")]
        public void TestFromJsonWithPartitioningFeature()
        {
            Exception exception;
            ReplicatedTableConfiguration conf;
            string configPath = null;
            ReplicatedTableConfiguredTable configuredTable;

            #region // *** Table related UTs *** //

            // Enforce that:
            // 1 - each table, if it refers a partition view, then the view has to exist
            configPath = Directory.GetCurrentDirectory() + "\\" + @"..\Tests\ConfigFiles\PartitionedTableNoDefaultView.txt";
            try
            {
                using (StreamReader sr = new StreamReader(configPath))
                {
                    exception = TestHelper.ExpectedException<Exception>(() => ReplicatedTableConfiguration.FromJson(sr.ReadToEnd()), "Table can't have a partition view and no default view");
                    Assert.IsTrue(exception.Message == string.Format("Table:\'{0}\' can't have a partition view but no default view.", "tbl5"));
                }
            }
            catch (Exception ex)
            {
                Assert.Fail("#Table1 - Received exception {0} while parsing {1}", ex.Message, configPath);
            }

            // - variant ...
            configPath = Directory.GetCurrentDirectory() + "\\" + @"..\Tests\ConfigFiles\PartitionedTableWithDefaultView.txt";
            try
            {
                using (StreamReader sr = new StreamReader(configPath))
                {
                    exception = TestHelper.ExpectedException<Exception>(() => ReplicatedTableConfiguration.FromJson(sr.ReadToEnd()), "Table can't have a missing partition view");
                    Assert.IsTrue(exception.Message == string.Format("Table:\'{0}\' refers a missing partition view:\'{1}\'! First, create the view and then configure the table.", "tbl5", "MissingView"));
                }
            }
            catch (Exception ex)
            {
                Assert.Fail("#Table2 - Received exception {0} while parsing {1}", ex.Message, configPath);
            }

            // - variant ...
            configPath = Directory.GetCurrentDirectory() + "\\" + @"..\Tests\ConfigFiles\PartitionedTableConfig.txt";
            try
            {
                using (StreamReader sr = new StreamReader(configPath))
                {
                    conf = ReplicatedTableConfiguration.FromJson(sr.ReadToEnd());
                    Assert.IsTrue(conf.GetViewSize() == 2);
                    Assert.IsTrue(conf.GetTableSize() == 5);

                    Assert.IsTrue(conf.IsConfiguredTable("tbl2", out configuredTable));
                    Assert.IsNull(configuredTable.PartitionsToViewMap);

                    Assert.IsTrue(conf.IsConfiguredTable("tbl3", out configuredTable));
                    Assert.IsTrue(configuredTable.PartitionsToViewMap != null && configuredTable.PartitionsToViewMap.Count == 0);

                    Assert.IsTrue(conf.IsConfiguredTable("tbl4", out configuredTable));
                    Assert.IsTrue(configuredTable.PartitionsToViewMap != null && configuredTable.PartitionsToViewMap.Count == 1);

                    Assert.IsTrue(conf.IsConfiguredTable("tbl5", out configuredTable));
                    Assert.IsTrue(configuredTable.PartitionsToViewMap != null && configuredTable.PartitionsToViewMap.Count == 2);
                }
            }
            catch (Exception ex)
            {
                Assert.Fail("#Table3 - Received exception {0} while parsing {1}", ex.Message, configPath);
            }


            // 2 - and, each table in ConvertToRTable mode has no more than one replica
            configPath = Directory.GetCurrentDirectory() + "\\" + @"..\Tests\ConfigFiles\PartitionedTableInConvertModeRefersNoneSingleReplica.txt";
            try
            {
                using (StreamReader sr = new StreamReader(configPath))
                {
                    exception = TestHelper.ExpectedException<Exception>(() => ReplicatedTableConfiguration.FromJson(sr.ReadToEnd()), "Table in convert mode can refer only single-replica partition view");
                    Assert.IsTrue(exception.Message == string.Format("Table:\'{0}\' refers a partition view:\'{1}\' with more than 1 replica while in Conversion mode!", "tbl5", "view2"));
                }
            }
            catch (Exception ex)
            {
                Assert.Fail("#Table4 - Received exception {0} while parsing {1}", ex.Message, configPath);
            }

            // - variant ...
            configPath = Directory.GetCurrentDirectory() + "\\" + @"..\Tests\ConfigFiles\PartitionedTableInConvertMode.txt";
            try
            {
                using (StreamReader sr = new StreamReader(configPath))
                {
                    conf = ReplicatedTableConfiguration.FromJson(sr.ReadToEnd());

                    Assert.IsTrue(conf.IsConfiguredTable("tbl2", out configuredTable));
                    Assert.IsNull(configuredTable.PartitionsToViewMap);
                    Assert.IsTrue(configuredTable.ConvertToRTable);

                    Assert.IsTrue(conf.IsConfiguredTable("tbl3", out configuredTable));
                    Assert.IsTrue(configuredTable.PartitionsToViewMap != null && configuredTable.PartitionsToViewMap.Count == 0);
                    Assert.IsTrue(configuredTable.ConvertToRTable);

                    Assert.IsTrue(conf.IsConfiguredTable("tbl4", out configuredTable));
                    Assert.IsTrue(configuredTable.PartitionsToViewMap != null && configuredTable.PartitionsToViewMap.Count == 1);
                    Assert.IsTrue(configuredTable.ConvertToRTable);

                    Assert.IsTrue(conf.IsConfiguredTable("tbl5", out configuredTable));
                    Assert.IsTrue(configuredTable.PartitionsToViewMap != null && configuredTable.PartitionsToViewMap.Count == 3);
                    Assert.IsTrue(configuredTable.ConvertToRTable);

                    Assert.IsTrue(conf.IsConfiguredTable("tbl6", out configuredTable));
                    Assert.IsTrue(configuredTable.PartitionsToViewMap != null && configuredTable.PartitionsToViewMap.Count == 3);
                    Assert.IsFalse(configuredTable.ConvertToRTable);
                }
            }
            catch (Exception ex)
            {
                Assert.Fail("#Table5 - Received exception {0} while parsing {1}", ex.Message, configPath);
            }

            // 3 - We can't have a partition view map when partitioning is disabled.
            configPath = Directory.GetCurrentDirectory() + "\\" + @"..\Tests\ConfigFiles\PartitionedTableWherePartitioningIsDisable.txt";
            try
            {
                using (StreamReader sr = new StreamReader(configPath))
                {
                    exception = TestHelper.ExpectedException<Exception>(() => ReplicatedTableConfiguration.FromJson(sr.ReadToEnd()), "We can't have a partition view map when partitioning is disabled.");
                    Assert.IsTrue(exception.Message == string.Format("Table:\'{0}\' can't have a partition view while partitioning is disabled!", "tbl3"));
                }
            }
            catch (Exception ex)
            {
                Assert.Fail("#Table6 - Received exception {0} while parsing {1}", ex.Message, configPath);
            }

            #endregion
        }

        #endregion


        #region // *** Chain manipulation UTs *** //

        [Test(Description = "Test MoveReplicaToHeadAndSetViewToReadOnly()")]
        public void TestMoveReplicaToHeadAndSetViewToReadOnly()
        {
            // Basic feature is already UTed by implementing class, we UT here to validate we iterate on all configured views ...

            Exception exception;

            var conf = new ReplicatedTableConfigurationAccessor();

            //   we assume the replica chain abides by RTable requirements:
            //   RULE 1 : [R] -> [R] ... [R] -> [R] (at least one Readable and no Writtable replicas)
            //   RULE 2 : [W] -> [W] ... [RW] -> [RW] (at least one Readable and all Writtable replica)

            // - view0
            var view0 = new ReplicatedTableConfigurationStore();
            conf.SetView("view0", view0);

            // - view1
            var view1 = new ReplicatedTableConfigurationStore();
            view1.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc#",
                Status = ReplicaStatus.None,
            });
            view1.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.ReadWrite,
            });
            conf.SetView("view1", view1);

            // - view2
            var view2 = new ReplicatedTableConfigurationStore();
            view2.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.ReadWrite,
            });
            view2.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.None,
            });
            view2.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc3",
                Status = ReplicaStatus.ReadWrite,
            });
            conf.SetView("view2", view2);

            // - view3
            var view3 = new ReplicatedTableConfigurationStore();
            view3.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.ReadOnly,
            });
            view3.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.ReadOnly,
            });
            conf.SetView("view3", view3);

            // - view4
            var view4 = new ReplicatedTableConfigurationStore();
            view4.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc#",
                Status = ReplicaStatus.None,
            });
            view4.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.ReadWrite,
            });
            conf.SetView("view4", view4);

            // - view5
            var view5 = new ReplicatedTableConfigurationStore();
            view5.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc#",
                Status = ReplicaStatus.None,
            });
            view5.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.None,
            });
            conf.SetView("view5", view5);


            // make the updates ...
            conf.MoveReplicaToHeadAndSetViewToReadOnly("Acc1");

            // Check all views new status
            Assert.IsFalse(conf.GetView("view0").ReplicaChain.Any());

            Assert.IsTrue(conf.GetView("view1").ReplicaChain[0].Status == ReplicaStatus.None);
            Assert.IsTrue(conf.GetView("view1").ReplicaChain[1].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view2").ReplicaChain[0].Status == ReplicaStatus.None);
            Assert.IsTrue(conf.GetView("view2").ReplicaChain[1].Status == ReplicaStatus.ReadOnly);

            Assert.IsTrue(conf.GetView("view3").ReplicaChain[0].Status == ReplicaStatus.None);
            Assert.IsTrue(conf.GetView("view3").ReplicaChain[1].Status == ReplicaStatus.ReadOnly);

            Assert.IsTrue(conf.GetView("view4").ReplicaChain[0].StorageAccountName == "Acc1");
            Assert.IsTrue(conf.GetView("view4").ReplicaChain[0].Status == ReplicaStatus.None);

            Assert.IsTrue(conf.GetView("view5").ReplicaChain[0].StorageAccountName == "Acc1");
            Assert.IsTrue(conf.GetView("view5").ReplicaChain[0].Status == ReplicaStatus.None);



            // Add more views

            // - view6
            var view6 = new ReplicatedTableConfigurationStore();
            view6.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.WriteOnly,
            });
            view6.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.None,
            });
            view6.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc3",
                Status = ReplicaStatus.ReadWrite,
            });
            conf.SetView("view6", view6);


            // make the updates ... we are idempotent
            // but view6 should fail ...
            exception = TestHelper.ExpectedException<Exception>(() => conf.MoveReplicaToHeadAndSetViewToReadOnly("Acc1"), "can't turn WriteOnly replica to ReadOnly!");
            Assert.IsTrue(exception.Message == string.Format("View:\'{0}\' : can't set a WriteOnly replica to ReadOnly !!!", "view6"));


            // Check all views new status
            Assert.IsFalse(conf.GetView("view0").ReplicaChain.Any());

            Assert.IsTrue(conf.GetView("view1").ReplicaChain[0].Status == ReplicaStatus.None);
            Assert.IsTrue(conf.GetView("view1").ReplicaChain[1].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view2").ReplicaChain[0].Status == ReplicaStatus.None);
            Assert.IsTrue(conf.GetView("view2").ReplicaChain[1].Status == ReplicaStatus.ReadOnly);

            Assert.IsTrue(conf.GetView("view3").ReplicaChain[0].Status == ReplicaStatus.None);
            Assert.IsTrue(conf.GetView("view3").ReplicaChain[1].Status == ReplicaStatus.ReadOnly);

            Assert.IsTrue(conf.GetView("view4").ReplicaChain[0].StorageAccountName == "Acc1");
            Assert.IsTrue(conf.GetView("view4").ReplicaChain[0].Status == ReplicaStatus.None);

            Assert.IsTrue(conf.GetView("view5").ReplicaChain[0].StorageAccountName == "Acc1");
            Assert.IsTrue(conf.GetView("view5").ReplicaChain[0].Status == ReplicaStatus.None);

            Assert.IsTrue(conf.GetView("view6").ReplicaChain[0].StorageAccountName == "Acc2");
            Assert.IsTrue(conf.GetView("view6").ReplicaChain[0].Status == ReplicaStatus.WriteOnly);
            Assert.IsTrue(conf.GetView("view6").ReplicaChain[1].StorageAccountName == "Acc1");
            Assert.IsTrue(conf.GetView("view6").ReplicaChain[1].Status == ReplicaStatus.None);
            Assert.IsTrue(conf.GetView("view6").ReplicaChain[2].StorageAccountName == "Acc3");
            Assert.IsTrue(conf.GetView("view6").ReplicaChain[2].Status == ReplicaStatus.ReadWrite);
        }


        #region EnableWriteOnReplicas UTs

        [Test(Description = "Test EnableWriteOnReplicas() for Rule1 and no tables configured")]
        public void TestEnableWriteOnReplicas_Rule1_NoTables()
        {
            // Basic feature is already UTed by implementing class, we UT here to validate we iterate on all configured views ...

            var conf = new ReplicatedTableConfigurationAccessor();

            //   we assume the replica chain abides by RTable requirements:
            //   RULE 1 : [R] -> [R] ... [R] -> [R] (at least one Readable and no Writtable replicas)
            //   RULE 2 : [W] -> [W] ... [RW] -> [RW] (at least one Readable and all Writtable replica)

            // *** CASES for RULE 1 : [R] -> [R] ... [R] -> [R] ***

            // - view0
            var view0 = new ReplicatedTableConfigurationStore();
            conf.SetView("view0", view0);

            // - view1
            var view1 = new ReplicatedTableConfigurationStore();
            view1.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc#",
                Status = ReplicaStatus.None,
            });
            view1.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.ReadOnly,
            });
            conf.SetView("view1", view1);

            // - view2
            var view2 = new ReplicatedTableConfigurationStore();
            view2.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.None,
            });
            view2.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.ReadOnly,
            });
            conf.SetView("view2", view2);

            // - view3
            var view3 = new ReplicatedTableConfigurationStore();
            view3.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.ReadOnly,
            });
            view3.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.ReadOnly,
            });
            conf.SetView("view3", view3);

            // - view4 (Acc1 is not the head of replica so it should not have any impact on that view)
            var view4 = new ReplicatedTableConfigurationStore();
            view4.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc#",
                Status = ReplicaStatus.ReadOnly,
            });
            view4.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.ReadOnly,
            });
            conf.SetView("view4", view4);

            // - view5
            var view5 = new ReplicatedTableConfigurationStore();
            view5.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.ReadOnly,
            });
            conf.SetView("view5", view5);

            // - view6
            var view6 = new ReplicatedTableConfigurationStore();
            view6.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.None,
            });
            conf.SetView("view6", view6);


            // make the updates ...
            conf.EnableWriteOnReplicas("Acc1");


            // Check all views new status
            Assert.IsFalse(conf.GetView("view0").ReplicaChain.Any());

            Assert.IsTrue(conf.GetView("view1").ReplicaChain[0].Status == ReplicaStatus.None);
            Assert.IsTrue(conf.GetView("view1").ReplicaChain[1].Status == ReplicaStatus.ReadOnly);

            Assert.IsTrue(conf.GetView("view2").ReplicaChain[0].Status == ReplicaStatus.WriteOnly);
            Assert.IsTrue(conf.GetView("view2").ReplicaChain[1].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view3").ReplicaChain[0].Status == ReplicaStatus.WriteOnly);
            Assert.IsTrue(conf.GetView("view3").ReplicaChain[1].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view4").ReplicaChain[0].Status == ReplicaStatus.ReadOnly);
            Assert.IsTrue(conf.GetView("view4").ReplicaChain[1].Status == ReplicaStatus.ReadOnly);

            Assert.IsTrue(conf.GetView("view5").ReplicaChain[0].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view6").ReplicaChain[0].Status == ReplicaStatus.ReadWrite);
        }

        [Test(Description = "Test EnableWriteOnReplicas() for Rule1 and tables configured in none conversion mode")]
        public void TestEnableWriteOnReplicas_Rule1_WithTablesNotInConversionMode()
        {
            // Basic feature is already UTed by implementing class, we UT here to validate we iterate on all configured views ...

            var conf = new ReplicatedTableConfigurationAccessor();

            //   we assume the replica chain abides by RTable requirements:
            //   RULE 1 : [R] -> [R] ... [R] -> [R] (at least one Readable and no Writtable replicas)
            //   RULE 2 : [W] -> [W] ... [RW] -> [RW] (at least one Readable and all Writtable replica)

            // *** CASES for RULE 1 : [R] -> [R] ... [R] -> [R] ***

            // - view0
            var view0 = new ReplicatedTableConfigurationStore();
            conf.SetView("view0", view0);

            // - view1
            var view1 = new ReplicatedTableConfigurationStore();
            view1.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc#",
                Status = ReplicaStatus.None,
            });
            view1.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.ReadOnly,
            });
            conf.SetView("view1", view1);

            // - view2
            var view2 = new ReplicatedTableConfigurationStore();
            view2.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.None,
            });
            view2.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.ReadOnly,
            });
            conf.SetView("view2", view2);

            // - view3
            var view3 = new ReplicatedTableConfigurationStore();
            view3.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.ReadOnly,
            });
            view3.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.ReadOnly,
            });
            conf.SetView("view3", view3);

            // - view4 (Acc1 is not the head of replica so it should not have any impact on that view)
            var view4 = new ReplicatedTableConfigurationStore();
            view4.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc#",
                Status = ReplicaStatus.ReadOnly,
            });
            view4.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.ReadOnly,
            });
            conf.SetView("view4", view4);

            // - view5
            var view5 = new ReplicatedTableConfigurationStore();
            view5.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.ReadOnly,
            });
            conf.SetView("view5", view5);

            // - view6
            var view6 = new ReplicatedTableConfigurationStore();
            view6.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.None,
            });
            conf.SetView("view6", view6);


            // - configure none partitioned tables
            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = false,

                TableName = "tabl0",
                ViewName = "view0",
                PartitionsToViewMap = null,
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = false,

                TableName = "tabl1",
                ViewName = "view1",
                PartitionsToViewMap = null,
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = false,

                TableName = "tabl2",
                ViewName = "view2",
                PartitionsToViewMap = null,
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = false,

                TableName = "tabl3",
                ViewName = "view3",
                PartitionsToViewMap = null,
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = false,

                TableName = "tabl4",
                ViewName = "view4",
                PartitionsToViewMap = null,
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = false,

                TableName = "tabl5",
                ViewName = "view5",
                PartitionsToViewMap = null,
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = false,

                TableName = "tabl6",
                ViewName = "view6",
                PartitionsToViewMap = null,
            });



            // - configure partitioned tables
            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = false,

                TableName = "partitioned_tabl0",
                ViewName = "view0",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "X", "view0" },
                }
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = false,

                TableName = "partitioned_tabl1",
                ViewName = "view0",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "X", "view1" },
                }
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = false,

                TableName = "partitioned_tabl2",
                ViewName = "view0",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "X", "view2" },
                }
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = false,

                TableName = "partitioned_tabl3",
                ViewName = "view0",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "X", "view3" },
                }
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = false,

                TableName = "partitioned_tabl4",
                ViewName = "view0",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "X", "view4" },
                }
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = false,

                TableName = "partitioned_tabl5",
                ViewName = "view0",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "X", "view5" },
                }
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = false,

                TableName = "partitioned_tabl6",
                ViewName = "view0",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "X", "view6" },
                }
            });


            // make the updates ...
            conf.EnableWriteOnReplicas("Acc1");


            // Check all views new status
            Assert.IsFalse(conf.GetView("view0").ReplicaChain.Any());

            Assert.IsTrue(conf.GetView("view1").ReplicaChain[0].Status == ReplicaStatus.None);
            Assert.IsTrue(conf.GetView("view1").ReplicaChain[1].Status == ReplicaStatus.ReadOnly);

            Assert.IsTrue(conf.GetView("view2").ReplicaChain[0].Status == ReplicaStatus.WriteOnly);
            Assert.IsTrue(conf.GetView("view2").ReplicaChain[1].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view3").ReplicaChain[0].Status == ReplicaStatus.WriteOnly);
            Assert.IsTrue(conf.GetView("view3").ReplicaChain[1].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view4").ReplicaChain[0].Status == ReplicaStatus.ReadOnly);
            Assert.IsTrue(conf.GetView("view4").ReplicaChain[1].Status == ReplicaStatus.ReadOnly);

            Assert.IsTrue(conf.GetView("view5").ReplicaChain[0].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view6").ReplicaChain[0].Status == ReplicaStatus.ReadWrite);
        }

        [Test(Description = "Test EnableWriteOnReplicas() for Rule1 and tables configured in conversion mode")]
        public void TestEnableWriteOnReplicas_Rule1_WithTablesInConversionMode()
        {
            // Basic feature is already UTed by implementing class, we UT here to validate we iterate on all configured views ...

            Exception exception;

            var conf = new ReplicatedTableConfigurationAccessor();

            //   we assume the replica chain abides by RTable requirements:
            //   RULE 1 : [R] -> [R] ... [R] -> [R] (at least one Readable and no Writtable replicas)
            //   RULE 2 : [W] -> [W] ... [RW] -> [RW] (at least one Readable and all Writtable replica)

            // *** CASES for RULE 1 : [R] -> [R] ... [R] -> [R] ***

            // - view0
            var view0 = new ReplicatedTableConfigurationStore();
            conf.SetView("view0", view0);

            // - view1
            var view1 = new ReplicatedTableConfigurationStore();
            view1.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc#",
                Status = ReplicaStatus.None,
            });
            view1.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.ReadOnly,
            });
            conf.SetView("view1", view1);

            // - view2
            var view2 = new ReplicatedTableConfigurationStore();
            view2.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.None,
            });
            view2.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.ReadOnly,
            });
            conf.SetView("view2", view2);

            // - view3
            var view3 = new ReplicatedTableConfigurationStore();
            view3.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.ReadOnly,
            });
            view3.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.ReadOnly,
            });
            conf.SetView("view3", view3);

            // - view4 (Acc1 is not the head of replica so it should not have any impact on that view)
            var view4 = new ReplicatedTableConfigurationStore();
            view4.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc#",
                Status = ReplicaStatus.ReadOnly,
            });
            view4.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.ReadOnly,
            });
            conf.SetView("view4", view4);

            // - view5
            var view5 = new ReplicatedTableConfigurationStore();
            view5.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.ReadOnly,
            });
            conf.SetView("view5", view5);

            // - view6
            var view6 = new ReplicatedTableConfigurationStore();
            view6.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.None,
            });
            conf.SetView("view6", view6);


            // - configure none partitioned tables
            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "tabl0",
                ViewName = "view0",
                PartitionsToViewMap = null,
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "tabl1",
                ViewName = "view1",
                PartitionsToViewMap = null,
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "tabl2",
                ViewName = "view2",
                PartitionsToViewMap = null,
            });

            // we can't have a table in conversion mode refering to "view3" which has multiple replicas

            // we can't have a table in conversion mode refering to "view4" which has multiple replicas

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "tabl5",
                ViewName = "view5",
                PartitionsToViewMap = null,
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "tabl6",
                ViewName = "view6",
                PartitionsToViewMap = null,
            });



            // make the updates ...
            exception = TestHelper.ExpectedException<Exception>(() => conf.EnableWriteOnReplicas("Acc1"), "can't change the view in a view table constraint is broken");
            Assert.IsTrue(exception.Message == string.Format("Table:\'{0}\' should not have a view:\'{1}\' with more than 1 replica since it is in Conversion mode!", "tabl2", "view2"));

            conf.RemoveTable("tabl2");

            conf.EnableWriteOnReplicas("Acc1");

            // Check all views new status
            Assert.IsFalse(conf.GetView("view0").ReplicaChain.Any());

            Assert.IsTrue(conf.GetView("view1").ReplicaChain[0].Status == ReplicaStatus.None);
            Assert.IsTrue(conf.GetView("view1").ReplicaChain[1].Status == ReplicaStatus.ReadOnly);

            Assert.IsTrue(conf.GetView("view2").ReplicaChain[0].Status == ReplicaStatus.WriteOnly);
            Assert.IsTrue(conf.GetView("view2").ReplicaChain[1].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view3").ReplicaChain[0].Status == ReplicaStatus.WriteOnly);
            Assert.IsTrue(conf.GetView("view3").ReplicaChain[1].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view4").ReplicaChain[0].Status == ReplicaStatus.ReadOnly);
            Assert.IsTrue(conf.GetView("view4").ReplicaChain[1].Status == ReplicaStatus.ReadOnly);

            Assert.IsTrue(conf.GetView("view5").ReplicaChain[0].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view6").ReplicaChain[0].Status == ReplicaStatus.ReadWrite);
        }

        [Test(Description = "Test EnableWriteOnReplicas() for Rule1 and partitioned tables configured in conversion mode")]
        public void TestEnableWriteOnReplicas_Rule1_WithPartitionedTablesInConversionMode()
        {
            // Basic feature is already UTed by implementing class, we UT here to validate we iterate on all configured views ...

            Exception exception;

            var conf = new ReplicatedTableConfigurationAccessor();

            //   we assume the replica chain abides by RTable requirements:
            //   RULE 1 : [R] -> [R] ... [R] -> [R] (at least one Readable and no Writtable replicas)
            //   RULE 2 : [W] -> [W] ... [RW] -> [RW] (at least one Readable and all Writtable replica)

            // *** CASES for RULE 1 : [R] -> [R] ... [R] -> [R] ***

            // - view0
            var view0 = new ReplicatedTableConfigurationStore();
            conf.SetView("view0", view0);

            // - view1
            var view1 = new ReplicatedTableConfigurationStore();
            view1.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc#",
                Status = ReplicaStatus.None,
            });
            view1.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.ReadOnly,
            });
            conf.SetView("view1", view1);

            // - view2
            var view2 = new ReplicatedTableConfigurationStore();
            view2.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.None,
            });
            view2.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.ReadOnly,
            });
            conf.SetView("view2", view2);

            // - view3
            var view3 = new ReplicatedTableConfigurationStore();
            view3.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.ReadOnly,
            });
            view3.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.ReadOnly,
            });
            conf.SetView("view3", view3);

            // - view4 (Acc1 is not the head of replica so it should not have any impact on that view)
            var view4 = new ReplicatedTableConfigurationStore();
            view4.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc#",
                Status = ReplicaStatus.ReadOnly,
            });
            view4.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.ReadOnly,
            });
            conf.SetView("view4", view4);

            // - view5
            var view5 = new ReplicatedTableConfigurationStore();
            view5.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.ReadOnly,
            });
            conf.SetView("view5", view5);

            // - view6
            var view6 = new ReplicatedTableConfigurationStore();
            view6.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.None,
            });
            conf.SetView("view6", view6);


            // - configure partitioned tables
            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "partitioned_tabl0",
                ViewName = "view0",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "X", "view0" },
                }
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "partitioned_tabl1",
                ViewName = "view0",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "X", "view1" },
                }
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "partitioned_tabl2",
                ViewName = "view0",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "X", "view2" },
                }
            });

            // we can't have a table in conversion mode refering to "view3" which has multiple replicas

            // we can't have a table in conversion mode refering to "view4" which has multiple replicas

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "partitioned_tabl5",
                ViewName = "view0",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "X", "view5" },
                }
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "partitioned_tabl6",
                ViewName = "view0",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "X", "view6" },
                }
            });



            // make the updates ...
            exception = TestHelper.ExpectedException<Exception>(() => conf.EnableWriteOnReplicas("Acc1"), "can't change the view in a view table constraint is broken");
            Assert.IsTrue(exception.Message == string.Format("Table:\'{0}\' should not have a view:\'{1}\' with more than 1 replica since it is in Conversion mode!", "partitioned_tabl2", "view2"));

            conf.RemoveTable("partitioned_tabl2");

            conf.EnableWriteOnReplicas("Acc1");

            // Check all views new status
            Assert.IsFalse(conf.GetView("view0").ReplicaChain.Any());

            Assert.IsTrue(conf.GetView("view1").ReplicaChain[0].Status == ReplicaStatus.None);
            Assert.IsTrue(conf.GetView("view1").ReplicaChain[1].Status == ReplicaStatus.ReadOnly);

            Assert.IsTrue(conf.GetView("view2").ReplicaChain[0].Status == ReplicaStatus.WriteOnly);
            Assert.IsTrue(conf.GetView("view2").ReplicaChain[1].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view3").ReplicaChain[0].Status == ReplicaStatus.WriteOnly);
            Assert.IsTrue(conf.GetView("view3").ReplicaChain[1].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view4").ReplicaChain[0].Status == ReplicaStatus.ReadOnly);
            Assert.IsTrue(conf.GetView("view4").ReplicaChain[1].Status == ReplicaStatus.ReadOnly);

            Assert.IsTrue(conf.GetView("view5").ReplicaChain[0].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view6").ReplicaChain[0].Status == ReplicaStatus.ReadWrite);
        }

        [Test(Description = "Test EnableWriteOnReplicas() for Rule2 and no tables configured")]
        public void TestEnableWriteOnReplicas_Rule2_NoTables()
        {
            // Basic feature is already UTed by implementing class, we UT here to validate we iterate on all configured views ...

            var conf = new ReplicatedTableConfigurationAccessor();

            //   we assume the replica chain abides by RTable requirements:
            //   RULE 1 : [R] -> [R] ... [R] -> [R] (at least one Readable and no Writtable replicas)
            //   RULE 2 : [W] -> [W] ... [RW] -> [RW] (at least one Readable and all Writtable replica)

            // *** CASES for RULE 2 : [W] -> [W] ... [RW] -> [RW] ***

            // - view0
            var view0 = new ReplicatedTableConfigurationStore();
            conf.SetView("view0", view0);

            // - view1
            var view1 = new ReplicatedTableConfigurationStore();
            view1.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc#",
                Status = ReplicaStatus.None,
            });
            view1.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.ReadWrite,
            });
            conf.SetView("view1", view1);

            // - view2
            var view2 = new ReplicatedTableConfigurationStore();
            view2.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.None,
            });
            view2.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.ReadWrite,
            });
            conf.SetView("view2", view2);

            // - view3
            var view3 = new ReplicatedTableConfigurationStore();
            view3.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.WriteOnly,
            });
            view3.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.ReadWrite,
            });
            conf.SetView("view3", view3);

            // - view4 (Acc1 is not the head of replica so it should not have any impact on that view)
            var view4 = new ReplicatedTableConfigurationStore();
            view4.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc#",
                Status = ReplicaStatus.WriteOnly,
            });
            view4.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.ReadWrite,
            });
            conf.SetView("view4", view4);

            // - view5
            var view5 = new ReplicatedTableConfigurationStore();
            view5.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.ReadWrite,
            });
            conf.SetView("view5", view5);

            // - view6
            var view6 = new ReplicatedTableConfigurationStore();
            view6.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.None,
            });
            conf.SetView("view6", view6);


            // make the updates ...
            conf.EnableWriteOnReplicas("Acc1");


            // Check all views new status
            Assert.IsFalse(conf.GetView("view0").ReplicaChain.Any());

            Assert.IsTrue(conf.GetView("view1").ReplicaChain[0].Status == ReplicaStatus.None);
            Assert.IsTrue(conf.GetView("view1").ReplicaChain[1].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view2").ReplicaChain[0].Status == ReplicaStatus.WriteOnly);
            Assert.IsTrue(conf.GetView("view2").ReplicaChain[1].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view3").ReplicaChain[0].Status == ReplicaStatus.WriteOnly);
            Assert.IsTrue(conf.GetView("view3").ReplicaChain[1].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view4").ReplicaChain[0].Status == ReplicaStatus.WriteOnly);
            Assert.IsTrue(conf.GetView("view4").ReplicaChain[1].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view5").ReplicaChain[0].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view6").ReplicaChain[0].Status == ReplicaStatus.ReadWrite);
        }

        [Test(Description = "Test EnableWriteOnReplicas() for Rule2 and tables configured in none conversion mode")]
        public void TestEnableWriteOnReplicas_Rule2_WithTablesNotInConversionMode()
        {
            // Basic feature is already UTed by implementing class, we UT here to validate we iterate on all configured views ...

            var conf = new ReplicatedTableConfigurationAccessor();

            //   we assume the replica chain abides by RTable requirements:
            //   RULE 1 : [R] -> [R] ... [R] -> [R] (at least one Readable and no Writtable replicas)
            //   RULE 2 : [W] -> [W] ... [RW] -> [RW] (at least one Readable and all Writtable replica)

            // *** CASES for RULE 2 : [W] -> [W] ... [RW] -> [RW] ***

            // - view0
            var view0 = new ReplicatedTableConfigurationStore();
            conf.SetView("view0", view0);

            // - view1
            var view1 = new ReplicatedTableConfigurationStore();
            view1.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc#",
                Status = ReplicaStatus.None,
            });
            view1.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.ReadWrite,
            });
            conf.SetView("view1", view1);

            // - view2
            var view2 = new ReplicatedTableConfigurationStore();
            view2.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.None,
            });
            view2.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.ReadWrite,
            });
            conf.SetView("view2", view2);

            // - view3
            var view3 = new ReplicatedTableConfigurationStore();
            view3.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.WriteOnly,
            });
            view3.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.ReadWrite,
            });
            conf.SetView("view3", view3);

            // - view4 (Acc1 is not the head of replica so it should not have any impact on that view)
            var view4 = new ReplicatedTableConfigurationStore();
            view4.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc#",
                Status = ReplicaStatus.WriteOnly,
            });
            view4.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.ReadWrite,
            });
            conf.SetView("view4", view4);

            // - view5
            var view5 = new ReplicatedTableConfigurationStore();
            view5.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.ReadWrite,
            });
            conf.SetView("view5", view5);

            // - view6
            var view6 = new ReplicatedTableConfigurationStore();
            view6.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.None,
            });
            conf.SetView("view6", view6);


            // - configure none partitioned tables
            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = false,

                TableName = "tabl0",
                ViewName = "view0",
                PartitionsToViewMap = null,
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = false,

                TableName = "tabl1",
                ViewName = "view1",
                PartitionsToViewMap = null,
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = false,

                TableName = "tabl2",
                ViewName = "view2",
                PartitionsToViewMap = null,
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = false,

                TableName = "tabl3",
                ViewName = "view3",
                PartitionsToViewMap = null,
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = false,

                TableName = "tabl4",
                ViewName = "view4",
                PartitionsToViewMap = null,
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = false,

                TableName = "tabl5",
                ViewName = "view5",
                PartitionsToViewMap = null,
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = false,

                TableName = "tabl6",
                ViewName = "view6",
                PartitionsToViewMap = null,
            });



            // - configure partitioned tables
            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = false,

                TableName = "partitioned_tabl0",
                ViewName = "view0",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "X", "view0" },
                }
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = false,

                TableName = "partitioned_tabl1",
                ViewName = "view0",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "X", "view1" },
                }
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = false,

                TableName = "partitioned_tabl2",
                ViewName = "view0",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "X", "view2" },
                }
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = false,

                TableName = "partitioned_tabl3",
                ViewName = "view0",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "X", "view3" },
                }
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = false,

                TableName = "partitioned_tabl4",
                ViewName = "view0",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "X", "view4" },
                }
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = false,

                TableName = "partitioned_tabl5",
                ViewName = "view0",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "X", "view5" },
                }
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = false,

                TableName = "partitioned_tabl6",
                ViewName = "view0",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "X", "view6" },
                }
            });


            // make the updates ...
            conf.EnableWriteOnReplicas("Acc1");


            // Check all views new status
            Assert.IsFalse(conf.GetView("view0").ReplicaChain.Any());

            Assert.IsTrue(conf.GetView("view1").ReplicaChain[0].Status == ReplicaStatus.None);
            Assert.IsTrue(conf.GetView("view1").ReplicaChain[1].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view2").ReplicaChain[0].Status == ReplicaStatus.WriteOnly);
            Assert.IsTrue(conf.GetView("view2").ReplicaChain[1].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view3").ReplicaChain[0].Status == ReplicaStatus.WriteOnly);
            Assert.IsTrue(conf.GetView("view3").ReplicaChain[1].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view4").ReplicaChain[0].Status == ReplicaStatus.WriteOnly);
            Assert.IsTrue(conf.GetView("view4").ReplicaChain[1].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view5").ReplicaChain[0].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view6").ReplicaChain[0].Status == ReplicaStatus.ReadWrite);
        }

        [Test(Description = "Test EnableWriteOnReplicas() for Rule2 and tables configured in conversion mode")]
        public void TestEnableWriteOnReplicas_Rule2_WithTablesInConversionMode()
        {
            // Basic feature is already UTed by implementing class, we UT here to validate we iterate on all configured views ...

            Exception exception;

            var conf = new ReplicatedTableConfigurationAccessor();

            //   we assume the replica chain abides by RTable requirements:
            //   RULE 1 : [R] -> [R] ... [R] -> [R] (at least one Readable and no Writtable replicas)
            //   RULE 2 : [W] -> [W] ... [RW] -> [RW] (at least one Readable and all Writtable replica)

            // *** CASES for RULE 2 : [W] -> [W] ... [RW] -> [RW] ***

            // - view0
            var view0 = new ReplicatedTableConfigurationStore();
            conf.SetView("view0", view0);

            // - view1
            var view1 = new ReplicatedTableConfigurationStore();
            view1.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc#",
                Status = ReplicaStatus.None,
            });
            view1.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.ReadWrite,
            });
            conf.SetView("view1", view1);

            // - view2
            var view2 = new ReplicatedTableConfigurationStore();
            view2.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.None,
            });
            view2.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.ReadWrite,
            });
            conf.SetView("view2", view2);

            // - view3
            var view3 = new ReplicatedTableConfigurationStore();
            view3.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.WriteOnly,
            });
            view3.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.ReadWrite,
            });
            conf.SetView("view3", view3);

            // - view4 (Acc1 is not the head of replica so it should not have any impact on that view)
            var view4 = new ReplicatedTableConfigurationStore();
            view4.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc#",
                Status = ReplicaStatus.WriteOnly,
            });
            view4.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.ReadWrite,
            });
            conf.SetView("view4", view4);

            // - view5
            var view5 = new ReplicatedTableConfigurationStore();
            view5.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.ReadWrite,
            });
            conf.SetView("view5", view5);

            // - view6
            var view6 = new ReplicatedTableConfigurationStore();
            view6.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.None,
            });
            conf.SetView("view6", view6);



            // - configure none partitioned tables
            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "tabl0",
                ViewName = "view0",
                PartitionsToViewMap = null,
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "tabl1",
                ViewName = "view1",
                PartitionsToViewMap = null,
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "tabl2",
                ViewName = "view2",
                PartitionsToViewMap = null,
            });

            // we can't have a table in conversion mode refering to "view3" which has multiple replicas

            // we can't have a table in conversion mode refering to "view4" which has multiple replicas

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "tabl5",
                ViewName = "view5",
                PartitionsToViewMap = null,
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "tabl6",
                ViewName = "view6",
                PartitionsToViewMap = null,
            });


            // make the updates ...
            exception = TestHelper.ExpectedException<Exception>(() => conf.EnableWriteOnReplicas("Acc1"), "can't change the view in a view table constraint is broken");
            Assert.IsTrue(exception.Message == string.Format("Table:\'{0}\' should not have a view:\'{1}\' with more than 1 replica since it is in Conversion mode!", "tabl2", "view2"));

            conf.RemoveTable("tabl2");

            conf.EnableWriteOnReplicas("Acc1");

            // Check all views new status
            Assert.IsFalse(conf.GetView("view0").ReplicaChain.Any());

            Assert.IsTrue(conf.GetView("view1").ReplicaChain[0].Status == ReplicaStatus.None);
            Assert.IsTrue(conf.GetView("view1").ReplicaChain[1].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view2").ReplicaChain[0].Status == ReplicaStatus.WriteOnly);
            Assert.IsTrue(conf.GetView("view2").ReplicaChain[1].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view3").ReplicaChain[0].Status == ReplicaStatus.WriteOnly);
            Assert.IsTrue(conf.GetView("view3").ReplicaChain[1].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view4").ReplicaChain[0].Status == ReplicaStatus.WriteOnly);
            Assert.IsTrue(conf.GetView("view4").ReplicaChain[1].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view5").ReplicaChain[0].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view6").ReplicaChain[0].Status == ReplicaStatus.ReadWrite);
        }

        [Test(Description = "Test EnableWriteOnReplicas() for Rule2 and partitioned tables configured in conversion mode")]
        public void TestEnableWriteOnReplicas_Rule2_WithPartitionedTablesInConversionMode()
        {
            // Basic feature is already UTed by implementing class, we UT here to validate we iterate on all configured views ...

            Exception exception;

            var conf = new ReplicatedTableConfigurationAccessor();

            //   we assume the replica chain abides by RTable requirements:
            //   RULE 1 : [R] -> [R] ... [R] -> [R] (at least one Readable and no Writtable replicas)
            //   RULE 2 : [W] -> [W] ... [RW] -> [RW] (at least one Readable and all Writtable replica)

            // *** CASES for RULE 2 : [W] -> [W] ... [RW] -> [RW] ***

            // - view0
            var view0 = new ReplicatedTableConfigurationStore();
            conf.SetView("view0", view0);

            // - view1
            var view1 = new ReplicatedTableConfigurationStore();
            view1.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc#",
                Status = ReplicaStatus.None,
            });
            view1.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.ReadWrite,
            });
            conf.SetView("view1", view1);

            // - view2
            var view2 = new ReplicatedTableConfigurationStore();
            view2.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.None,
            });
            view2.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.ReadWrite,
            });
            conf.SetView("view2", view2);

            // - view3
            var view3 = new ReplicatedTableConfigurationStore();
            view3.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.WriteOnly,
            });
            view3.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.ReadWrite,
            });
            conf.SetView("view3", view3);

            // - view4 (Acc1 is not the head of replica so it should not have any impact on that view)
            var view4 = new ReplicatedTableConfigurationStore();
            view4.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc#",
                Status = ReplicaStatus.WriteOnly,
            });
            view4.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.ReadWrite,
            });
            conf.SetView("view4", view4);

            // - view5
            var view5 = new ReplicatedTableConfigurationStore();
            view5.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.ReadWrite,
            });
            conf.SetView("view5", view5);

            // - view6
            var view6 = new ReplicatedTableConfigurationStore();
            view6.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.None,
            });
            conf.SetView("view6", view6);



            // - configure partitioned tables
            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "partitioned_tabl0",
                ViewName = "view0",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "X", "view0" },
                }
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "partitioned_tabl1",
                ViewName = "view0",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "X", "view1" },
                }
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "partitioned_tabl2",
                ViewName = "view0",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "X", "view2" },
                }
            });

            // we can't have a table in conversion mode refering to "view3" which has multiple replicas

            // we can't have a table in conversion mode refering to "view4" which has multiple replicas

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "partitioned_tabl5",
                ViewName = "view0",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "X", "view5" },
                }
            });

            conf.SetTable(new ReplicatedTableConfiguredTable
            {
                ConvertToRTable = true,

                TableName = "partitioned_tabl6",
                ViewName = "view0",
                PartitionOnProperty = "X",
                PartitionsToViewMap = new Dictionary<string, string>
                {
                    { "X", "view6" },
                }
            });


            // make the updates ...
            exception = TestHelper.ExpectedException<Exception>(() => conf.EnableWriteOnReplicas("Acc1"), "can't change the view in a view table constraint is broken");
            Assert.IsTrue(exception.Message == string.Format("Table:\'{0}\' should not have a view:\'{1}\' with more than 1 replica since it is in Conversion mode!", "partitioned_tabl2", "view2"));

            conf.RemoveTable("partitioned_tabl2");

            conf.EnableWriteOnReplicas("Acc1");

            // Check all views new status
            Assert.IsFalse(conf.GetView("view0").ReplicaChain.Any());

            Assert.IsTrue(conf.GetView("view1").ReplicaChain[0].Status == ReplicaStatus.None);
            Assert.IsTrue(conf.GetView("view1").ReplicaChain[1].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view2").ReplicaChain[0].Status == ReplicaStatus.WriteOnly);
            Assert.IsTrue(conf.GetView("view2").ReplicaChain[1].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view3").ReplicaChain[0].Status == ReplicaStatus.WriteOnly);
            Assert.IsTrue(conf.GetView("view3").ReplicaChain[1].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view4").ReplicaChain[0].Status == ReplicaStatus.WriteOnly);
            Assert.IsTrue(conf.GetView("view4").ReplicaChain[1].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view5").ReplicaChain[0].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view6").ReplicaChain[0].Status == ReplicaStatus.ReadWrite);
        }

        #endregion


        [Test(Description = "Test EnableReadWriteOnReplicas()")]
        public void TestEnableReadWriteOnReplicas()
        {
            // basic feature is already UTed by implementing class, we need UT here to validate we iterate on all configured views ...

            var conf = new ReplicatedTableConfigurationAccessor();

            //   we assume the replica chain abides by RTable requirements:
            //   RULE 1 : [R] -> [R] ... [R] -> [R] (at least one Readable and no Writtable replicas)
            //   RULE 2 : [W] -> [W] ... [RW] -> [RW] (at least one Readable and all Writtable replica)

            // - view0
            var view0 = new ReplicatedTableConfigurationStore();
            conf.SetView("view0", view0);

            // - view1
            var view1 = new ReplicatedTableConfigurationStore();
            view1.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.WriteOnly,
            });
            view1.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.ReadWrite,
            });
            conf.SetView("view1", view1);

            // - view2
            var view2 = new ReplicatedTableConfigurationStore();
            view2.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.None,
            });
            view2.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.ReadWrite,
            });
            conf.SetView("view2", view2);

            // - view3
            var view3 = new ReplicatedTableConfigurationStore();
            view3.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.WriteOnly,
            });
            view3.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.ReadWrite,
            });
            conf.SetView("view3", view3);

            // - view4
            var view4 = new ReplicatedTableConfigurationStore();
            view4.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.ReadWrite,
            });
            view4.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.None,
            });
            conf.SetView("view4", view4);


            // make the updates ...
            var skipList = new List<string> { "view3" };
            conf.EnableReadWriteOnReplicas("Acc1", skipList);


            // Check all views new status
            Assert.IsFalse(conf.GetView("view0").ReplicaChain.Any());

            Assert.IsTrue(conf.GetView("view1").ReplicaChain[0].Status == ReplicaStatus.WriteOnly);
            Assert.IsTrue(conf.GetView("view1").ReplicaChain[1].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view2").ReplicaChain[0].Status == ReplicaStatus.None);
            Assert.IsTrue(conf.GetView("view2").ReplicaChain[1].Status == ReplicaStatus.ReadWrite);

            // was skipped ... nothing happened!
            Assert.IsTrue(conf.GetView("view3").ReplicaChain[0].StorageAccountName == "Acc1");
            Assert.IsTrue(conf.GetView("view3").ReplicaChain[0].Status == ReplicaStatus.WriteOnly);

            Assert.IsTrue(conf.GetView("view4").ReplicaChain[0].Status == ReplicaStatus.ReadWrite);


            // make the updates ...
            conf.EnableReadWriteOnReplicas("Acc1", new List<string> ());


            // Check all views new status
            Assert.IsFalse(conf.GetView("view0").ReplicaChain.Any());

            Assert.IsTrue(conf.GetView("view1").ReplicaChain[0].Status == ReplicaStatus.WriteOnly);
            Assert.IsTrue(conf.GetView("view1").ReplicaChain[1].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view2").ReplicaChain[0].Status == ReplicaStatus.None);
            Assert.IsTrue(conf.GetView("view2").ReplicaChain[1].Status == ReplicaStatus.ReadWrite);

            // was skipped ... nothing happened!
            Assert.IsTrue(conf.GetView("view3").ReplicaChain[0].StorageAccountName == "Acc1");
            Assert.IsTrue(conf.GetView("view3").ReplicaChain[0].Status == ReplicaStatus.ReadWrite);

            Assert.IsTrue(conf.GetView("view4").ReplicaChain[0].Status == ReplicaStatus.ReadWrite);


            // - view5
            var view5 = new ReplicatedTableConfigurationStore();
            view5.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc1",
                Status = ReplicaStatus.WriteOnly,
            });
            view5.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc2",
                Status = ReplicaStatus.WriteOnly,
            });
            view5.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "Acc3",
                Status = ReplicaStatus.ReadWrite,
            });
            conf.SetView("view5", view5);

            // Before : view5 = [W] -> [W] -> [RW]  => this is a valid sequence.
            conf.EnableReadWriteOnReplicas("Acc1", new List<string>());
            // After : view5 = [RW] -> [W] -> [RW] => which is invalid sequence !!!

            /*
                This API doesn't do post-check on the validity of the resulting replica-chain. The caller is responsible of ensuring that.
                In real life, we (TurnReplicaOn()) call this API on such sequence [W] -> [RW] -> [RW] so we end up in consistent state.
             */

            // check that such call would fails ...
            Exception
            exception = TestHelper.ExpectedException<Exception>(() => conf.SetView("viewInvalid", view5), "Replicas chain is invalid");
            Assert.IsTrue(exception.Message == string.Format("View:\'{0}\' has invalid Read chain:\'{1}\' !!!", "viewInvalid", "RWR"));
        }

        #endregion

    }
}
