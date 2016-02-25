using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.ChainTableInterface;
using Microsoft.WindowsAzure.Storage.STable;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace STableUnitTest
{
    class User
    {
        public User(int id, STable s, int nRow, TextWriter logger, int seed)
        {
            this.id = id;
            this.s = s;
            this.nRow = nRow;
            this.logger = logger;

            r = new Random(seed);
        }

        public void Start()
        {
            running = true;
            opId = 0;
            t = new Thread(new ThreadStart(Run));
            t.Start();
        }

        public void Stop()
        {
            logger.WriteLine("Stopping user " + id + ".");
            running = false;
        }

        public void Join()
        {
            t.Join();
        }



        private int id;
        private STable s;
        private int nRow;

        private Thread t;
        private bool running;
        private int opId;

        private Random r;
        private TextWriter logger;



        private void Run()
        {
            logger.WriteLine("User " + id + " started.");

            try
            {
                while (running)
                {
                    ++opId;

                    int rnd = r.Next(100);
                    if (rnd < 5)
                    {
                        try
                        {
                            var res = DoCreateSnapshot();
                            logger.WriteLine("Created snapshot " + res);
                        }
                        catch (StorageException e)
                        {
                            logger.WriteLine("Warning: Create snapshot returns " + e.RequestInformation.HttpStatusCode);
                        }
                    }
                    else if (rnd < 20)
                    {
                        int key = r.Next(nRow);
                        string pKey = GetPKey(key);
                        string rKey = key.ToString();

                        var ssids = DoListSnapshots();

                        if (ssids.Count > 0)
                        {
                            var ssid = ssids[r.Next(ssids.Count)];

                            try
                            {
                                var res = DoRetrieveSnapshot(ssid, pKey, rKey);

                                Assert.IsTrue(res != null);
                                if (res.HttpStatusCode == (int)HttpStatusCode.OK)
                                {
                                    Assert.IsTrue(res.Result != null && res.Result is TestEntity);
                                    var ent = (TestEntity)res.Result;

                                    PrintEntity(ssid, rKey, ent);
                                }
                                else if (res.HttpStatusCode == (int)HttpStatusCode.NotFound)
                                    PrintEntity(ssid, rKey, null);
                                else
                                    Assert.IsTrue(false);
                            }
                            catch (StorageException e)
                            {
                                logger.WriteLine("Warning: retrieve history returns " + e.RequestInformation.HttpStatusCode);
                            }
                        }
                    }
                    else if (rnd < 80)
                    {
                        int key = r.Next(nRow);
                        string pKey = GetPKey(key);
                        string rKey = key.ToString();

                        var currState = DoRetrieve(pKey, rKey);
                        Assert.IsTrue(currState != null);
                        if (currState.HttpStatusCode == (int)HttpStatusCode.NotFound)
                        {
                            TestEntity newEnt = new TestEntity(pKey, rKey);
                            newEnt.a = id;
                            newEnt.b = opId;

                            int op = r.Next(3);

                            if (op == 0)
                                ExecInsert(newEnt);
                            else if (op == 1)
                                ExecIOR(newEnt);
                            else
                                ExecIOM(newEnt);
                        }
                        else if (currState.HttpStatusCode == (int)HttpStatusCode.OK)
                        {
                            Assert.IsTrue(currState.Result != null && currState.Result is TestEntity);
                            var ent = (TestEntity)currState.Result;
                            int op = r.Next(5);

                            if (op == 0)
                                ExecDelete(ent);
                            else
                            {
                                ent.a = id;
                                ent.b = opId;

                                if (op == 1)
                                    ExecIOR(ent);
                                else if (op == 2)
                                    ExecIOM(ent);
                                else if (op == 3)
                                    ExecReplace(ent);
                                else
                                    ExecMerge(ent);
                            }
                        }
                        else
                            logger.WriteLine("Warning: Retrieve returns " + currState.HttpStatusCode);
                    }
                    else
                    {
                        int batchSize = 3;
                        int targetPKey = r.Next(10);
                        List<int> keys = GenerateRows(targetPKey, batchSize);
                        var batch = new TableBatchOperation();

                        for (int i = 0; i < batchSize; ++i)
                        {
                            string pKey = GetPKey(keys[i]);
                            string rKey = keys[i].ToString();

                            var currState = DoRetrieve(pKey, rKey);
                            Assert.IsTrue(currState != null);
                            if (currState.HttpStatusCode == (int)HttpStatusCode.NotFound)
                            {
                                TestEntity newEnt = new TestEntity(pKey, rKey);
                                newEnt.a = id;
                                newEnt.b = opId;
                                batch.Add(TableOperation.Insert(newEnt));
                            }
                            else if (currState.HttpStatusCode == (int)HttpStatusCode.OK)
                            {
                                Assert.IsTrue(currState.Result != null && currState.Result is TestEntity);
                                var ent = (TestEntity)currState.Result;
                                int op = r.Next(5);

                                if (op == 0)
                                    batch.Add(TableOperation.Delete(ent));
                                else
                                {
                                    ent.a = id;
                                    ent.b = opId;

                                    if (op == 1)
                                        batch.Add(TableOperation.InsertOrReplace(ent));
                                    else if (op == 2)
                                        batch.Add(TableOperation.InsertOrMerge(ent));
                                    else if (op == 3)
                                        batch.Add(TableOperation.Replace(ent));
                                    else
                                        batch.Add(TableOperation.Merge(ent));
                                }
                            }
                            else
                                logger.WriteLine("Warning: Retrieve returns " + currState.HttpStatusCode);
                        }

                        ExecBatch(batch);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
                Assert.IsTrue(false);
            }

            logger.WriteLine("User " + id + " stopped.");
        }

        private List<int> GenerateRows(int targetPKey, int n)
        {
            Assert.IsTrue(n <= nRow / 20);

            var res = new List<int>();
            for (int i = 0; i < n; ++i)
            {
                int t = r.Next(nRow) / 10 * 10 + targetPKey;
                while (res.Contains(t))
                    t = r.Next(nRow) / 10 * 10 + targetPKey;

                res.Add(t);
            }

            return res;
        }

        private void PrintEntity(int ssid, string rkey, TestEntity ent)
        {
            StringBuilder sb = new StringBuilder();
            if (ssid != null)
                sb.Append("ssid = " + ssid);
            else
                sb.Append("ssid = h");

            sb.Append(", row = " + rkey);

            if (ent == null)
                sb.Append(", content = NotFound");
            else
                sb.Append(", content = {a = " + ent.a + ", b = " + ent.b + "}");

            logger.WriteLine(sb.ToString());
        }


        private string GetPKey(int rKey)
        {
            return (rKey % 10).ToString();
        }

        private TableResult DoRetrieve(string pKey, string rKey)
        {
            return s.Execute(TableOperation.Retrieve<TestEntity>(pKey, rKey));
        }

        private TableResult DoRetrieveSnapshot(int ssid, string pKey, string rKey)
        {
            return s.RetrieveFromSnapshot(TableOperation.Retrieve<TestEntity>(pKey, rKey), ssid);
        }

        private void ExecInsert(TestEntity entity)
        {
            try
            {
                var res = s.Execute(TableOperation.Insert(entity));
                Assert.IsTrue(res != null);
                Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);
            }
            catch (StorageException e)
            {
                logger.WriteLine("Warning: Insert returns " + e.RequestInformation.HttpStatusCode);
            }
        }

        private void ExecReplace(TestEntity entity)
        {
            try
            {
                var res = s.Execute(TableOperation.Replace(entity));
                Assert.IsTrue(res != null);
                Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.NoContent);
            }
            catch (StorageException e)
            {
                logger.WriteLine("Warning: Replace returns " + e.RequestInformation.HttpStatusCode);
            }
        }

        private void ExecMerge(ITableEntity entity)
        {
            try
            {
                var res = s.Execute(TableOperation.Merge(entity));
                Assert.IsTrue(res != null);
                Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.NoContent);
            }
            catch (StorageException e)
            {
                logger.WriteLine("Warning: Merge returns " + e.RequestInformation.HttpStatusCode);
            }
        }

        private void ExecDelete(TestEntity entity)
        {
            try
            {
                var res = s.Execute(TableOperation.Delete(entity));
                Assert.IsTrue(res != null);
                Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.NoContent);
            }
            catch (StorageException e)
            {
                logger.WriteLine("Warning: Delete returns " + e.RequestInformation.HttpStatusCode);
            }
        }

        private void ExecIOR(TestEntity entity)
        {
            try
            {
                var res = s.Execute(TableOperation.InsertOrReplace(entity));
                Assert.IsTrue(res != null);
                Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.NoContent);
            }
            catch (StorageException e)
            {
                logger.WriteLine("Warning: IOR returns " + e.RequestInformation.HttpStatusCode);
            }
        }

        private void ExecIOM(ITableEntity entity)
        {
            try
            {
                var res = s.Execute(TableOperation.InsertOrMerge(entity));
                Assert.IsTrue(res != null);
                Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.NoContent);
            }
            catch (StorageException e)
            {
                logger.WriteLine("Warning: IOM returns " + e.RequestInformation.HttpStatusCode);
            }
        }

        private void ExecBatch(TableBatchOperation batch)
        {
            try
            {
                var res = s.ExecuteBatch(batch);

                Assert.IsTrue(res != null && res.Count == batch.Count);
                for (int i = 0; i < res.Count; ++i)
                {
                    Assert.IsTrue(res[i] != null && (res[i].HttpStatusCode == (int)HttpStatusCode.Created ||
                        res[i].HttpStatusCode == (int)HttpStatusCode.NoContent));
                }
            }
            catch (StorageException e)
            {
                Assert.IsTrue(e is ChainTableBatchException);
                logger.WriteLine("Warning: batch returns " + e.RequestInformation.HttpStatusCode);
            }
        }

        private IList<int> DoListSnapshots()
        {
            return s.ListValidSnapshots();
        }

        private int DoCreateSnapshot()
        {
            return s.CreateSnapshot();
        }

        private void DoDeleteSnapshot(int ssid)
        {
            s.DeleteSnapshot(ssid);
        }

        private int DoRollback(int ssid)
        {
            return s.Rollback(ssid);
        }
    }
}
