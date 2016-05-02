namespace Microsoft.Azure.Toolkit.Replication.Test
{
    using Microsoft.Azure.Toolkit.Replication;
    using NUnit.Framework;
    using System.Threading;
    using System;
    using System.IO;
    using System.Collections.Generic;

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
            Assert.IsTrue(exception.Message == string.Format("Table:\'{0}\' should not have a view:\'{1}\' with more than 1 replica since it is in Convertion mode!", table2.TableName, "view1"));
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
            Assert.IsTrue(exception.Message == string.Format("Table:\'{0}\' refers a view:\'{1}\' with more than 1 replica while in Convertion mode!", table6.TableName, table6.ViewName));

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
                    exception = TestHelper.ExpectedException<Exception>(() => ReplicatedTableConfiguration.FromJson(sr.ReadToEnd()), "SetView should fail if replica chain is null");
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
                    exception = TestHelper.ExpectedException<Exception>(() => ReplicatedTableConfiguration.FromJson(sr.ReadToEnd()), "SetView should fail if replica chain is null");
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
                    Assert.IsTrue(exception.Message == string.Format("Table:\'{0}\' refers a view:\'{1}\' with more than 1 replica while in Convertion mode!", "tbl4", "view2"));
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

        #endregion


        #region // *** Chain manipulation UTs *** //

        [Test(Description = "Test MoveReplicaToHeadAndSetViewToReadOnly()")]
        public void TestMoveReplicaToHeadAndSetViewToReadOnly()
        {
            // TODO: basic feature is already UTed by implementing class, we need UT here to validate we iterate on all configured views ...
        }

        [Test(Description = "Test EnableWriteOnReplicas()")]
        public void TestEnableWriteOnReplicas()
        {
            // TODO: basic feature is already UTed by implementing class, we need UT here to validate we iterate on all configured views ...
        }


        [Test(Description = "Test EnableReadWriteOnReplicas()")]
        public void TestEnableReadWriteOnReplicas()
        {
            // TODO: basic feature is already UTed by implementing class, we need UT here to validate we iterate on all configured views ...
        }

        #endregion

    }
}
