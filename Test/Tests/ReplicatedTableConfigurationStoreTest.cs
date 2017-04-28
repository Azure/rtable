namespace Microsoft.Azure.Toolkit.Replication.Test
{
    using Microsoft.Azure.Toolkit.Replication;
    using NUnit.Framework;
    using System;
    using System.Linq;
    using System.IO;
    using System.Threading;
    using Microsoft.WindowsAzure.Storage.Table;

    // Needed to access protected members ...
    class ReplicatedTableConfigurationStoreAccessor : ReplicatedTableConfigurationStore
    {
        public new void SanitizeWithCurrentView(View currentView)
        {
            base.SanitizeWithCurrentView(currentView);
        }

        public new void MoveReplicaToHeadAndSetViewToReadOnly(string viewName, string storageAccountName)
        {
            base.MoveReplicaToHeadAndSetViewToReadOnly(viewName, storageAccountName);
        }

        public new void EnableWriteOnReplicas(string viewName, string headStorageAccountName)
        {
            base.EnableWriteOnReplicas(viewName, headStorageAccountName);
        }

        public new void EnableReadWriteOnReplica(string viewName, string headStorageAccountName)
        {
            base.EnableReadWriteOnReplica(viewName, headStorageAccountName);
        }

        public new void ThrowIfChainIsNotValid(string viewName)
        {
            base.ThrowIfChainIsNotValid(viewName);
        }
    }

    [TestFixture]
    public class ReplicatedTableConfigurationStoreTest
    {
        [TestFixtureSetUp]
        public void TestFixtureSetup()
        {
        }

        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
        }

        [Test(Description = "Test ReplicatedTableConfigurationStoreEquality()")]
        public void TestReplicatedTableConfigurationStoreEquality()
        {
            var conf1 = new ReplicatedTableConfigurationStore
            {
                ViewId = 5,
            };

            Assert.IsFalse(conf1.Equals(null));

            // - vs. different viewId
            var conf2 = new ReplicatedTableConfigurationStore
            {
                ViewId = 6,
            };
            Assert.IsFalse(conf1.Equals(conf2));

            // vs. same viewId
            var conf3 = new ReplicatedTableConfigurationStore
            {
                ViewId = 5,
            };
            Assert.IsTrue(conf1.Equals(conf3));
        }

        [Test(Description = "Test GetCurrentReplicaChain()")]
        public void TestGetCurrentReplicaChain()
        {
            var conf = new ReplicatedTableConfigurationStore();
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.None,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.ReadOnly,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.None,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.ReadWrite,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.WriteOnly,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.None,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.ReadWrite,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.None,
            });

            // only Active replicas are returned i.e. RO, WO and RW
            var chain = conf.GetCurrentReplicaChain();
            Assert.IsFalse(chain.Any(r => r.Status==ReplicaStatus.None));
        }

        [Test(Description = "Test SanitizeWithCurrentView()")]
        public void TestSanitizeWithCurrentView()
        {
            var conf = new ReplicatedTableConfigurationStoreAccessor
            {
                ViewId = 0,
            };

            var view = new View("view empty")
            {
                ViewId = 9,
            };

            // - conf view is 0, and we pass a view which is empty
            // => conf view is set to 1
            DateTime old = conf.Timestamp;
            Thread.Sleep(100);

            Assert.IsTrue(conf.ViewId == 0);
            Assert.IsTrue(view.IsEmpty);
            conf.SanitizeWithCurrentView(view);
            Assert.IsTrue(conf.ViewId == 1 && conf.Timestamp > old);

            // - conf view != 0, and we pass a view which is empty
            // => conf view keeps its value
            conf.ViewId = 3;
            old = conf.Timestamp;
            Thread.Sleep(100);

            Assert.IsTrue(conf.ViewId != 0);
            Assert.IsTrue(view.IsEmpty);
            conf.SanitizeWithCurrentView(view);// the head of the chain, but not WriteOnly mode => nothing to do [R1 -> R2*]
            Assert.IsTrue(conf.ViewId == 3 && conf.Timestamp > old);

            // - conf view is 0, and we pass a view not empty
            // => conf view is set to the view.viewId + 1
            conf.ViewId = 0;
            view.Chain.Add(new Tuple<ReplicaInfo, CloudTableClient>(new ReplicaInfo(), null));

            old = conf.Timestamp;
            Thread.Sleep(100);

            Assert.IsTrue(conf.ViewId == 0);
            Assert.IsFalse(view.IsEmpty);
            conf.SanitizeWithCurrentView(view);
            Assert.IsTrue(conf.ViewId == view.ViewId + 1 && conf.Timestamp > old);

            // - conf view != 0, and we pass a view not empty
            // => conf view keeps its value
            conf.ViewId = 6;
            view.Chain.Add(new Tuple<ReplicaInfo, CloudTableClient>(new ReplicaInfo(), null));

            old = conf.Timestamp;
            Thread.Sleep(100);

            Assert.IsTrue(conf.ViewId != 0);
            Assert.IsFalse(view.IsEmpty);
            conf.SanitizeWithCurrentView(view);
            Assert.IsTrue(conf.ViewId == 6 && conf.Timestamp > old);


            // - Now add replicas to the config
            //   we assume the replica chain abides by RTable requirements:
            //   RULE 1 : [R] -> [R] ... [R] -> [R] (at least one Readable and no Writtable replicas)
            //   RULE 2 : [W] -> [W] ... [RW] -> [RW] (at least one Readable and all Writtable replica)

            // RULE 1:
            conf.ReplicaChain.Clear();
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.None,
                ViewInWhichAddedToChain = 3,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.ReadOnly,
                ViewInWhichAddedToChain = 3,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.None,
                ViewInWhichAddedToChain = 3,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.ReadOnly,
                ViewInWhichAddedToChain = 3,
            });

            // - only WriteOnly replica should be updated
            conf.ViewId = 6;
            conf.SanitizeWithCurrentView(view);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.ViewInWhichAddedToChain == 3) == 4);


            // RULE 2:
            conf.ReplicaChain.Clear();
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.None,
                ViewInWhichAddedToChain = 3,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.WriteOnly,
                ViewInWhichAddedToChain = 3,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.None,
                ViewInWhichAddedToChain = 3,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.ReadWrite,
                ViewInWhichAddedToChain = 3,
            });

            // - only WriteOnly replica should be updated
            conf.ViewId = 6;
            conf.SanitizeWithCurrentView(view);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.WriteOnly && r.ViewInWhichAddedToChain == 6) == 1);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.ViewInWhichAddedToChain == 3) == 3);
        }

        [Test(Description = "Test MoveReplicaToHeadAndSetViewToReadOnly()")]
        public void TestMoveReplicaToHeadAndSetViewToReadOnly()
        {
            var conf = new ReplicatedTableConfigurationStoreAccessor();

            //   we assume the replica chain abides by RTable requirements:
            //   RULE 1 : [R] -> [R] ... [R] -> [R] (at least one Readable and no Writtable replicas)
            //   RULE 2 : [W] -> [W] ... [RW] -> [RW] (at least one Readable and all Writtable replica)

            // RULE 1:
            conf.ReplicaChain.Clear();
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R1",
                Status = ReplicaStatus.None,
                ViewInWhichAddedToChain = 3,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R2",
                Status = ReplicaStatus.ReadOnly,
                ViewInWhichAddedToChain = 3,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R3",
                Status = ReplicaStatus.None,
                ViewInWhichAddedToChain = 3,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R4",
                Status = ReplicaStatus.ReadOnly,
                ViewInWhichAddedToChain = 3,
            });

            // - Replica not found => nothing to do [R1 -> R2* -> R3 -> R4*]
            conf.ViewId = 6;
            conf.MoveReplicaToHeadAndSetViewToReadOnly("view1", "replicaX");
            Assert.IsTrue(conf.ViewId == 6);
            Assert.IsTrue(conf.ReplicaChain[0].StorageAccountName == "R1");
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.None) == 2);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.ReadOnly) == 2);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.IsWritable()) == 0);

            // - Replica found and was None => move replica to head [R3 -> R1 -> R2* -> R4*]
            conf.ViewId = 6;
            conf.MoveReplicaToHeadAndSetViewToReadOnly("view1", "R3");
            Assert.IsTrue(conf.ViewId == 7);
            Assert.IsTrue(conf.ReplicaChain[0].StorageAccountName == "R3");
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.None) == 2);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.ReadOnly) == 2);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.IsWritable()) == 0);

            // - Replica found but was ReadOnly => make replica None and move it to head [R4 -> R3 -> R1 -> R2*]
            conf.ViewId = 9;
            conf.MoveReplicaToHeadAndSetViewToReadOnly("view1", "R4");
            Assert.IsTrue(conf.ViewId == 10);
            Assert.IsTrue(conf.ReplicaChain[0].StorageAccountName == "R4");
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.None) == 3);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.ReadOnly) == 1);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.IsWritable()) == 0);


            // RULE 2:
            conf.ReplicaChain.Clear();
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R1",
                Status = ReplicaStatus.None,
                ViewInWhichAddedToChain = 3,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R2",
                Status = ReplicaStatus.WriteOnly,
                ViewInWhichAddedToChain = 3,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R3",
                Status = ReplicaStatus.None,
                ViewInWhichAddedToChain = 3,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R4",
                Status = ReplicaStatus.ReadWrite,
                ViewInWhichAddedToChain = 3,
            });

            // - Replica not found => nothing to do [R1 -> R2* -> R3 -> R4*]
            conf.ViewId = 5;
            conf.MoveReplicaToHeadAndSetViewToReadOnly("view1", "replicaX");
            Assert.IsTrue(conf.ViewId == 5);
            Assert.IsTrue(conf.ReplicaChain[0].StorageAccountName == "R1");
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.None) == 2);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.WriteOnly) == 1);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.ReadWrite) == 1);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.IsWritable()) == 2);

            // - Replica found and was None
            //   But this call fails becasuse we can't change replica R2 from WriteOnly to ReadOnly !
            //   preserve the chain as is [R1 -> R2* -> R3 -> R4*]
            Exception exception;

            conf.ViewId = 5;
            exception = TestHelper.ExpectedException<Exception>(() => conf.MoveReplicaToHeadAndSetViewToReadOnly("view1", "R3"), "we can't change replica from WriteOnly to ReadOnly");
            Assert.IsTrue(exception.Message == string.Format("View:\'{0}\' : can't set a WriteOnly replica to ReadOnly !!!", "view1"));

            Assert.IsTrue(conf.ViewId == 5);
            Assert.IsTrue(conf.ReplicaChain[0].StorageAccountName == "R1");
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.None) == 2);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.WriteOnly) == 1);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.ReadWrite) == 1);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.IsWritable()) == 2);

            // - Replica found and was RW
            //   But this call fails becasuse we can't change replica R2 from WriteOnly to ReadOnly !
            //   preserve the chain as is [R1 -> R2* -> R3 -> R4*]
            conf.ViewId = 5;
            exception = TestHelper.ExpectedException<Exception>(() => conf.MoveReplicaToHeadAndSetViewToReadOnly("view1", "R4"), "we can't change replica from WriteOnly to ReadOnly");
            Assert.IsTrue(exception.Message == string.Format("View:\'{0}\' : can't set a WriteOnly replica to ReadOnly !!!", "view1"));

            Assert.IsTrue(conf.ViewId == 5);
            Assert.IsTrue(conf.ReplicaChain[0].StorageAccountName == "R1");
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.None) == 2);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.WriteOnly) == 1);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.ReadWrite) == 1);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.IsWritable()) == 2);

            // - Replica found and was WO => preserve the chain as is [R2 -> R1 -> R3 -> R4*]
            conf.ViewId = 5;
            conf.MoveReplicaToHeadAndSetViewToReadOnly("view1", "R2");

            Assert.IsTrue(conf.ViewId == 6);
            Assert.IsTrue(conf.ReplicaChain[0].StorageAccountName == "R2");
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.None) == 3);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.WriteOnly) == 0);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.ReadWrite) == 0);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.ReadOnly) == 1);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.IsWritable()) == 0);

            // - Replica found and was WriteOnly => move the replica to the head [R2 -> R3 -> R1 -> R4*]
            conf.ViewId = 9;
            conf.MoveReplicaToHeadAndSetViewToReadOnly("view1", "R2");
            Assert.IsTrue(conf.ViewId == 10);
            Assert.IsTrue(conf.ReplicaChain[0].StorageAccountName == "R2");
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.None) == 3);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.WriteOnly) == 0);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.ReadOnly) == 1);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.ReadWrite) == 0);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.IsWritable()) == 0);



            // RULE 2: variant ...
            conf.ReplicaChain.Clear();
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R1",
                Status = ReplicaStatus.None,
                ViewInWhichAddedToChain = 3,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R2",
                Status = ReplicaStatus.ReadWrite,
                ViewInWhichAddedToChain = 3,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R3",
                Status = ReplicaStatus.None,
                ViewInWhichAddedToChain = 3,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R4",
                Status = ReplicaStatus.ReadWrite,
                ViewInWhichAddedToChain = 3,
            });

            // - Replica found and was None => move the replica to the head [R3 -> R1 -> R2* -> R4*]
            conf.ViewId = 5;
            conf.MoveReplicaToHeadAndSetViewToReadOnly("view1", "R3");

            Assert.IsTrue(conf.ViewId == 6);
            Assert.IsTrue(conf.ReplicaChain[0].StorageAccountName == "R3");
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.None) == 2);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.ReadWrite) == 0);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.IsReadable()) == 2);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.IsWritable()) == 0);


            // RULE 2: variant ...
            conf.ReplicaChain.Clear();
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R1",
                Status = ReplicaStatus.None,
                ViewInWhichAddedToChain = 3,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R2",
                Status = ReplicaStatus.ReadWrite,
                ViewInWhichAddedToChain = 3,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R3",
                Status = ReplicaStatus.None,
                ViewInWhichAddedToChain = 3,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R4",
                Status = ReplicaStatus.ReadWrite,
                ViewInWhichAddedToChain = 3,
            });

            // - Replica found and was RW => move the replica to the head [R2 -> R1 -> R3 -> R4*]
            conf.ViewId = 5;
            conf.MoveReplicaToHeadAndSetViewToReadOnly("view1", "R2");

            Assert.IsTrue(conf.ViewId == 6);
            Assert.IsTrue(conf.ReplicaChain[0].StorageAccountName == "R2");
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.None) == 3);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.ReadWrite) == 0);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.IsReadable()) == 1);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.IsWritable()) == 0);
        }

        [Test(Description = "Test EnableWriteOnReplicas()")]
        public void TestEnableWriteOnReplicas()
        {
            var conf = new ReplicatedTableConfigurationStoreAccessor();

            //   we assume the replica chain abides by RTable requirements:
            //   RULE 1 : [R] -> [R] ... [R] -> [R] (at least one Readable and no Writtable replicas)
            //   RULE 2 : [W] -> [W] ... [RW] -> [RW] (at least one Readable and all Writtable replica)


            // - Empty replica , nothing to do ...
            conf.ViewId = 6;
            conf.EnableWriteOnReplicas("view1", "replicaX");
            Assert.IsTrue(conf.ViewId == 6);
            Assert.IsFalse(conf.ReplicaChain.Any());

            // RULE 1:
            conf.ReplicaChain.Clear();
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R1",
                Status = ReplicaStatus.None,
                ViewInWhichAddedToChain = 3,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R2",
                Status = ReplicaStatus.ReadOnly,
                ViewInWhichAddedToChain = 3,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R3",
                Status = ReplicaStatus.None,
                ViewInWhichAddedToChain = 3,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R4",
                Status = ReplicaStatus.ReadOnly,
                ViewInWhichAddedToChain = 3,
            });

            // - Replica not found => nothing to do [R1 -> R2* -> R3 -> R4*]
            conf.ViewId = 6;
            conf.EnableWriteOnReplicas("view1", "replicaX");
            Assert.IsTrue(conf.ViewId == 6);
            Assert.IsTrue(conf.ReplicaChain[0].StorageAccountName == "R1");
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.None) == 2);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.ReadOnly) == 2);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.IsWritable()) == 0);

            // - Replica found but not at the head => nothing to do [R1 -> R2* -> R3 -> R4*]
            conf.ViewId = 6;
            conf.EnableWriteOnReplicas("view1", "R2");
            Assert.IsTrue(conf.ViewId == 6);
            Assert.IsTrue(conf.ReplicaChain[0].StorageAccountName == "R1");
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.None) == 2);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.ReadOnly) == 2);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.IsWritable()) == 0);

            conf.ViewId = 6;
            conf.EnableWriteOnReplicas("view1", "R3");
            Assert.IsTrue(conf.ViewId == 6);
            Assert.IsTrue(conf.ReplicaChain[0].StorageAccountName == "R1");
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.None) == 2);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.ReadOnly) == 2);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.IsWritable()) == 0);

            // - Replica found at the head => enable writting on all replicas [R1* -> R2* -> R3 -> R4*]
            conf.ViewId = 6;
            conf.EnableWriteOnReplicas("view1", "R1");
            Assert.IsTrue(conf.ViewId == 7);
            Assert.IsTrue(conf.ReplicaChain[0].StorageAccountName == "R1");
            Assert.IsTrue(conf.ReplicaChain[0].Status == ReplicaStatus.WriteOnly);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.WriteOnly) == 1);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.None) == 1);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.ReadOnly) == 0);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.ReadWrite) == 2);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.IsWritable()) == 3);


            // RULE 2:
            // for example [R1(none) -> R2(wo) -> R3(none) -> R4(rw)] which is a valid chain
            // in reality, we would not end-up calling this function with such chain, ... that is enforced by the business logic (i.e. TurnOn replica).
            // this function doesn't control how or in which order it is being called.
            // so here we are just checking the expected behavior.
            // at the end, we should endup with a valid chain as well.
            conf.ReplicaChain.Clear();
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R1",
                Status = ReplicaStatus.None,
                ViewInWhichAddedToChain = 3,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R2",
                Status = ReplicaStatus.WriteOnly,
                ViewInWhichAddedToChain = 3,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R3",
                Status = ReplicaStatus.None,
                ViewInWhichAddedToChain = 3,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R4",
                Status = ReplicaStatus.ReadWrite,
                ViewInWhichAddedToChain = 3,
            });

            // - Replica found at the head => enable writting on all replicas [R1(w) -> R2(rw) -> R3(none) -> R4(rw)]
            conf.ViewId = 6;
            conf.EnableWriteOnReplicas("view1", "R1");
            Assert.IsTrue(conf.ViewId == 7);
            Assert.IsTrue(conf.ReplicaChain[0].StorageAccountName == "R1");
            Assert.IsTrue(conf.ReplicaChain[0].Status == ReplicaStatus.WriteOnly);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.WriteOnly) == 1);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.None) == 1);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.ReadOnly) == 0);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.ReadWrite) == 2);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.IsWritable()) == 3);


            //- Single-replica scenario [R1(none) -> R2(none)]
            conf.ReplicaChain.Clear();
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R1",
                Status = ReplicaStatus.None,
                ViewInWhichAddedToChain = 3,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R2",
                Status = ReplicaStatus.None,
                ViewInWhichAddedToChain = 3,
            });

            // - Replica found at the head => enable RW on the replica [R1(rw) -> R2(none)]
            conf.ViewId = 6;
            conf.EnableWriteOnReplicas("view1", "R1");
            Assert.IsTrue(conf.ViewId == 7);
            Assert.IsTrue(conf.ReplicaChain[0].StorageAccountName == "R1");
            Assert.IsTrue(conf.ReplicaChain[0].Status == ReplicaStatus.ReadWrite);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.WriteOnly) == 0);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.None) == 1);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.ReadOnly) == 0);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.Status == ReplicaStatus.ReadWrite) == 1);
            Assert.IsTrue(conf.ReplicaChain.Count(r => r.IsWritable()) == 1);
        }

        [Test(Description = "Test EnableReadWriteOnReplica()")]
        public void TestEnableReadWriteOnReplica()
        {
            var conf = new ReplicatedTableConfigurationStoreAccessor();

            //   we assume the replica chain abides by RTable requirements:
            //   RULE 1 : [R] -> [R] ... [R] -> [R] (at least one Readable and no Writtable replicas)
            //   RULE 2 : [W] -> [W] ... [RW] -> [RW] (at least one Readable and all Writtable replica)

            // - Empty replica , nothing to do ...
            conf.ViewId = 6;
            conf.EnableReadWriteOnReplica("view1", "replicaX");
            Assert.IsTrue(conf.ViewId == 6);
            Assert.IsFalse(conf.ReplicaChain.Any());

            // Not the head of the chain: => nothing to do [R1 -> R2*]
            conf.ReplicaChain.Clear();
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R1",
                Status = ReplicaStatus.None,
                ViewInWhichAddedToChain = 3,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R2",
                Status = ReplicaStatus.ReadOnly,
                ViewInWhichAddedToChain = 3,
            });

            conf.ViewId = 6;
            conf.EnableReadWriteOnReplica("view1", "R2");
            Assert.IsTrue(conf.ViewId == 6);
            Assert.IsTrue(conf.ReplicaChain[0].StorageAccountName == "R1");
            Assert.IsTrue(conf.ReplicaChain[0].Status == ReplicaStatus.None);

            // the head of the chain, but not WriteOnly mode => nothing to do [R1 -> R2*]
            conf.ReplicaChain.Clear();
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R1",
                Status = ReplicaStatus.None,
                ViewInWhichAddedToChain = 3,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R2",
                Status = ReplicaStatus.ReadOnly,
                ViewInWhichAddedToChain = 3,
            });

            conf.ViewId = 6;
            conf.EnableReadWriteOnReplica("view1", "R1");
            Assert.IsTrue(conf.ViewId == 6);
            Assert.IsTrue(conf.ReplicaChain[0].StorageAccountName == "R1");
            Assert.IsTrue(conf.ReplicaChain[0].Status == ReplicaStatus.None);

            // variant: the head of the chain, but not WriteOnly mode => nothing to do [R1* -> R2*]
            conf.ReplicaChain.Clear();
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R1",
                Status = ReplicaStatus.ReadOnly,
                ViewInWhichAddedToChain = 3,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R2",
                Status = ReplicaStatus.ReadOnly,
                ViewInWhichAddedToChain = 3,
            });

            conf.ViewId = 6;
            conf.EnableReadWriteOnReplica("view1", "R1");
            Assert.IsTrue(conf.ViewId == 6);
            Assert.IsTrue(conf.ReplicaChain[0].StorageAccountName == "R1");
            Assert.IsTrue(conf.ReplicaChain[0].Status == ReplicaStatus.ReadOnly);

            // variant: the head of the chain, and WriteOnly mode => make RW [R1* -> R2*]
            conf.ReplicaChain.Clear();
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R1",
                Status = ReplicaStatus.WriteOnly,
                ViewInWhichAddedToChain = 3,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                StorageAccountName = "R2",
                Status = ReplicaStatus.ReadWrite,
                ViewInWhichAddedToChain = 3,
            });

            conf.ViewId = 6;
            conf.EnableReadWriteOnReplica("view1", "R1");
            Assert.IsTrue(conf.ViewId == 7);
            Assert.IsTrue(conf.ReplicaChain[0].StorageAccountName == "R1");
            Assert.IsTrue(conf.ReplicaChain[0].Status == ReplicaStatus.ReadWrite);
        }

        [Test(Description = "Test ThrowIfChainIsNotValid()")]
        public void TestThrowIfChainIsNotValid()
        {
            Exception exception;

            var conf = new ReplicatedTableConfigurationStoreAccessor();

            /* RULE 1:
             * =======
             * Read replicas rule:
             *  - [R] replicas are contiguous from Tail backwards
             *  - [R] replica count >= 1
             */

            /* RULE 2:
             * =======
             * Write replicas rule:
             *  - [W] replicas are contiguous from Head onwards
             *  - [W] replica count = 0 or = ChainLength
             */


            #region *** RULE 1 : [R] -> [R] ... [R] -> [R] (at least one Readable and no Writtable replicas) ***

            // Empty chain is ok!
            conf.ReplicaChain.Clear();
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.None,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.None,
            });
            conf.ThrowIfChainIsNotValid("view1");

            // - Chain not empty and not ([R] replica count >= 1)
            conf.ReplicaChain.Clear();
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.None,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.WriteOnly,
            });

            exception = TestHelper.ExpectedException<Exception>(() => conf.ThrowIfChainIsNotValid("view1"), "we need to have at least one Readable replica");
            Assert.IsTrue(exception.Message == string.Format("View:\'{0}\' has invalid Read chain:\'{1}\' !!!", "view1", "W"));

            // One ReadOnly replicas
            conf.ReplicaChain.Clear();
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.None,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.ReadOnly,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.None,
            });
            conf.ThrowIfChainIsNotValid("view1");

            // Only ReadOnly replicas
            conf.ReplicaChain.Clear();
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.None,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.ReadOnly,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.None,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.ReadOnly,
            });
            conf.ThrowIfChainIsNotValid("view1");

            // [R] replicas not contiguous from Tail backward
            conf.ReplicaChain.Clear();
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.None,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.ReadOnly,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.WriteOnly,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.ReadOnly,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.ReadOnly,
            });

            exception = TestHelper.ExpectedException<Exception>(() => conf.ThrowIfChainIsNotValid("view1"), "Replicas must be not Writable or All are Wraitable");
            Assert.IsTrue(exception.Message == string.Format("View:\'{0}\' has invalid Read chain:\'{1}\' !!!", "view1", "RWRR"));

            // [R] replicas not contiguous from Tail backward
            conf.ReplicaChain.Clear();
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.None,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.ReadOnly,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.WriteOnly,
            });

            exception = TestHelper.ExpectedException<Exception>(() => conf.ThrowIfChainIsNotValid("view1"), "Replicas must be not Writable or All are Wraitable");
            Assert.IsTrue(exception.Message == string.Format("View:\'{0}\' has invalid Read chain:\'{1}\' !!!", "view1", "RW"));

            #endregion



            #region *** RULE 2 : [W] -> [W] ... [RW] -> [RW] (at least one Readable and all Writtable replica) ***

            // 1 Writable replica, but the chain length is 4 and all Readable
            conf.ReplicaChain.Clear();
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.None,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.ReadOnly,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.ReadWrite,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.ReadOnly,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.ReadOnly,
            });

            exception = TestHelper.ExpectedException<Exception>(() => conf.ThrowIfChainIsNotValid("view1"), "Replicas must be not Writable or All are Wraitable");
            Assert.IsTrue(exception.Message == string.Format("View:\'{0}\' has invalid Write chain:\'{1}\' !!!", "view1", "RWRR"));

            // All replica are ReadWritable
            conf.ReplicaChain.Clear();
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.None,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.ReadWrite,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.ReadWrite,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.ReadWrite,
            });
            conf.ReplicaChain.Add(new ReplicaInfo
            {
                Status = ReplicaStatus.ReadWrite,
            });

            conf.ThrowIfChainIsNotValid("view1");

            #endregion

        }

        [Test(Description = "Test DeserializeJsonConfig()")]
        public void DeserializeJsonConfig()
        {
            ReplicatedTableConfigurationStore conf;
            string jsonConf = null;

            // - Empty chain
            jsonConf = @"{'ConvertXStoreTableMode': true, 'LeaseDuration': 60, 'ReadViewHeadIndex': 1, 'ReplicaChain': '', 'Timestamp': '/Date(1460152261966)/', 'ViewId': 58}";
            conf = JsonStore<ReplicatedTableConfigurationStore>.Deserialize(jsonConf);
            Assert.True(conf.ViewId == 58);
            Assert.IsTrue(conf.ReplicaChain != null);
            Assert.IsTrue(conf.ReplicaChain.Count == 0);

            jsonConf = @"{'ConvertXStoreTableMode': true, 'LeaseDuration': 60, 'ReadViewHeadIndex': 1, 'ReplicaChain': [], 'Timestamp': '/Date(1460152261966)/', 'ViewId': 58}";
            conf = JsonStore<ReplicatedTableConfigurationStore>.Deserialize(jsonConf);
            Assert.True(conf.ViewId == 58);
            Assert.IsTrue(conf.ReplicaChain != null);
            Assert.IsTrue(conf.ReplicaChain.Count == 0);


            string configPath = null;

            //- Read legacy V1 config
            configPath = Directory.GetCurrentDirectory() + "\\" + @"..\Tests\ConfigFiles\V1ConfigLegacy.txt";
            try
            {
                using (StreamReader sr = new StreamReader(configPath))
                {
                    conf = JsonStore<ReplicatedTableConfigurationStore>.Deserialize(sr.ReadToEnd());

                    Assert.True(conf.ViewId == 58);
                    Assert.IsTrue(conf.ReplicaChain != null);
                    Assert.IsTrue(conf.ReplicaChain.Count == 2);
                }
            }
            catch (Exception ex)
            {
                Assert.Fail("#Test1 - Received exception {0} while parsing {1}", ex.Message, configPath);
            }

            //- Read V1 config with extra attribute (add as part of V2 enhancements)
            configPath = Directory.GetCurrentDirectory() + "\\" + @"..\Tests\ConfigFiles\V1ConfigWithV2Attributes.txt";
            try
            {
                using (StreamReader sr = new StreamReader(configPath))
                {
                    conf = JsonStore<ReplicatedTableConfigurationStore>.Deserialize(sr.ReadToEnd());

                    Assert.True(conf.ViewId == 58);
                    Assert.IsTrue(conf.ReplicaChain != null);
                    Assert.IsTrue(conf.ReplicaChain.Count == 2);
                }
            }
            catch (Exception ex)
            {
                Assert.Fail("#Test2 - Received exception {0} while parsing {1}", ex.Message, configPath);
            }

            //- Read V1 config with extra attribute (add as part of V2 enhancements)
            configPath = Directory.GetCurrentDirectory() + "\\" + @"..\Tests\ConfigFiles\V1ConfigWithV2AttributesWithoutAccKey.txt";
            try
            {
                using (StreamReader sr = new StreamReader(configPath))
                {
                    conf = JsonStore<ReplicatedTableConfigurationStore>.Deserialize(sr.ReadToEnd());

                    Assert.True(conf.ViewId == 58);
                    Assert.IsTrue(conf.ReplicaChain != null);
                    Assert.IsTrue(conf.ReplicaChain.Count == 2);
                }
            }
            catch (Exception ex)
            {
                Assert.Fail("#Test2 - Received exception {0} while parsing {1}", ex.Message, configPath);
            }
        }

        [Test(Description = "Testing Instrumentation Flag sent in the config")]
        public void TestInstrumentationFlagV1()
        {
            var testConfigWithNoInstrumentation =
                @"{'ConvertXStoreTableMode': true, 'LeaseDuration': 60, 'ReadViewHeadIndex': 1, 'ReplicaChain': '', 'Timestamp': '/Date(1460152261966)/', 'ViewId': 58}";
            var conf = JsonStore<ReplicatedTableConfigurationStore>.Deserialize(testConfigWithNoInstrumentation); 
            Assert.IsNotNull(conf);
            Assert.AreEqual(false, conf.Instrumentation);


            var testConfigWithInstrumentation =
               @"{'ConvertXStoreTableMode': true, 'LeaseDuration': 60, 'ReadViewHeadIndex': 1, 'ReplicaChain': '', 'Timestamp': '/Date(1460152261966)/', 'ViewId': 58, 'Instrumentation': false}";
            conf = JsonStore<ReplicatedTableConfigurationStore>.Deserialize(testConfigWithInstrumentation);
            Assert.IsNotNull(conf);
            Assert.AreEqual(false, conf.Instrumentation);

            var testConfigWithInstrumentationAsTrue =
               @"{'ConvertXStoreTableMode': true, 'LeaseDuration': 60, 'ReadViewHeadIndex': 1, 'ReplicaChain': '', 'Timestamp': '/Date(1460152261966)/', 'ViewId': 58, 'Instrumentation': true}";
            conf = JsonStore<ReplicatedTableConfigurationStore>.Deserialize(testConfigWithInstrumentationAsTrue);
            Assert.IsNotNull(conf);
            Assert.AreEqual(true, conf.Instrumentation);
        }
    }
}