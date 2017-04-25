using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage.STable;
using Microsoft.WindowsAzure.Storage.ChainTableFactory;
using Microsoft.WindowsAzure.Storage.ChainTableInterface;
using Microsoft.WindowsAzure.Storage.Table;
using System.Net;
using Microsoft.WindowsAzure.Storage;
using System.Collections.Generic;
using System.Threading;
using System.IO;
using System.Linq;

namespace STableUnitTest
{
    [TestClass]
    public class UnitTest1
    {
        private static readonly string configFile = "D:/config.txt";

        [TestInitialize]
        public void InitSTableTest()
        {
            name = "STable" + random.Next().ToString();
            config = new DefaultSTableConfig(name, false);
            ts = new DefaultChainTableService(configFile);
            s = new STable(name, config, ts);
        }

        [TestCleanup]
        public void CleanupSTableTest()
        {
            if (s != null)
                s.DeleteIfExists();
        }

        [TestMethod]
        public void CreateTabTest()
        {
            Assert.IsTrue(DoCreateTable());
        }

        [TestMethod]
        public void CreateTabExistTest()
        {
            Assert.IsTrue(DoCreateTable());
            Assert.IsTrue(!DoCreateTable());
            Assert.IsTrue(!DoCreateTable());
        }

        [TestMethod]
        public void DeleteTabTest()
        {
            Assert.IsTrue(DoCreateTable());
            Assert.IsTrue(DoDeleteTable());
        }

        [TestMethod]
        public void DeleteTabNonExistTest()
        {
            Assert.IsTrue(!DoDeleteTable());
            Assert.IsTrue(!DoDeleteTable());
        }

        [TestMethod]
        public void InsertTest()
        {
            Assert.IsTrue(DoCreateTable());
            TestEntity entity = new TestEntity("1", "1");

            entity.a = 1;
            entity.b = 2;
            entity.c = 3;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);
            Assert.IsTrue(res.Result == entity);
            Assert.IsTrue(res.Etag.Equals(entity.ETag));
            Assert.IsTrue(entity.ETag.Contains(","));
            
        }

        [TestMethod]
        public void InsertExistTest()
        {
            Assert.IsTrue(DoCreateTable());
            TestEntity entity = new TestEntity("1", "1");

            entity.a = 1;
            entity.b = 2;
            entity.c = 3;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);

            try
            {
                res = DoInsert(entity);
                Assert.IsTrue(false);
            }
            catch (StorageException e)
            {
                Assert.IsTrue(e.RequestInformation.HttpStatusCode == (int)HttpStatusCode.Conflict);
            }
        }

        [TestMethod]
        public void RetrieveTest()
        {
            Assert.IsTrue(DoCreateTable());
            TestEntity entity = new TestEntity("1", "1");

            entity.a = 1;
            entity.b = 2;
            entity.c = 3;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);

            var retrieved = DoRetrieve("1", "1");

            Assert.IsTrue(retrieved != null && retrieved.HttpStatusCode == (int)HttpStatusCode.OK && retrieved.Result != null);
            Assert.IsTrue(retrieved.Result is TestEntity);
            var ret = (TestEntity)retrieved.Result;
            Assert.IsTrue(ret.ETag.Equals(retrieved.Etag));

            Assert.IsTrue(ret != entity);
            Assert.IsTrue(ret.PartitionKey.Equals(entity.PartitionKey));
            Assert.IsTrue(ret.RowKey.Equals(entity.RowKey));
            Assert.IsTrue(ret.ETag.Equals(entity.ETag));
            //Assert.IsTrue(ret.Timestamp.Equals(entity.Timestamp));
            Assert.IsTrue(ret.a == entity.a && ret.a == 1);
            Assert.IsTrue(ret.b == entity.b && ret.b == 2);
            Assert.IsTrue(ret.c == entity.c && ret.c == 3);
            Assert.IsTrue(ret.d.Equals(entity.d) && ret.d.Equals("hello world!"));
            Assert.IsTrue(ret.e == entity.e && ret.e == true);
            Assert.IsTrue(ret.f == entity.f && ret.f == 123.45);
        }

        [TestMethod]
        public void RetrieveNotExistTest()
        {
            Assert.IsTrue(DoCreateTable());

            var retrieved = DoRetrieve("1", "1");
            Assert.IsTrue(retrieved != null && retrieved.HttpStatusCode == (int)HttpStatusCode.NotFound && retrieved.Result == null);
        }

        [TestMethod]
        public void RetrieveDeletedTest()
        {
            Assert.IsTrue(DoCreateTable());
            TestEntity entity = new TestEntity("1", "1");

            entity.a = 1;
            entity.b = 2;
            entity.c = 3;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);

            res = DoDelete(entity);
            Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.NoContent);

            var retrieved = DoRetrieve("1", "1");
            Assert.IsTrue(retrieved != null && retrieved.HttpStatusCode == (int)HttpStatusCode.NotFound && retrieved.Result == null);
        }

        [TestMethod]
        public void RetrieveInsertBackTest()
        {
            Assert.IsTrue(DoCreateTable());
            TestEntity entity = new TestEntity("1", "1");

            entity.a = 1;
            entity.b = 2;
            entity.c = 3;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);

            res = DoDelete(entity);
            Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.NoContent);

            entity.a = 555;
            entity.d = "come back alive";
            res = DoInsert(entity);
            Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.Created);

            var retrieved = DoRetrieve("1", "1");

            Assert.IsTrue(retrieved != null && retrieved.HttpStatusCode == (int)HttpStatusCode.OK && retrieved.Result != null);
            Assert.IsTrue(retrieved.Result is TestEntity);
            var ret = (TestEntity)retrieved.Result;
            Assert.IsTrue(ret.ETag.Equals(retrieved.Etag));

            Assert.IsTrue(ret != entity);
            Assert.IsTrue(ret.PartitionKey.Equals(entity.PartitionKey));
            Assert.IsTrue(ret.RowKey.Equals(entity.RowKey));
            Assert.IsTrue(ret.ETag.Equals(entity.ETag));
            Assert.IsTrue(ret.a == entity.a && ret.a == 555);
            Assert.IsTrue(ret.b == entity.b && ret.b == 2);
            Assert.IsTrue(ret.c == entity.c && ret.c == 3);
            Assert.IsTrue(ret.d.Equals(entity.d) && ret.d.Equals("come back alive"));
            Assert.IsTrue(ret.e == entity.e && ret.e == true);
            Assert.IsTrue(ret.f == entity.f && ret.f == 123.45);
        }


        [TestMethod]
        public void ReplaceTest()
        {
            Assert.IsTrue(DoCreateTable());
            TestEntity entity = new TestEntity("1", "1");

            entity.a = 3;
            entity.b = 2;
            entity.c = 1;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);

            TestEntity entity2 = new TestEntity("1", "1");

            entity2.a = 1;
            entity2.b = 2;
            entity2.c = 3;
            entity2.d = "!dlrow olleh";
            entity2.e = false;
            entity2.f = 54.321;

            entity2.ETag = entity.ETag;
            entity2.Timestamp = entity.Timestamp;

            var replaced = DoReplace(entity2);

            Assert.IsTrue(replaced != null && replaced.HttpStatusCode == (int)HttpStatusCode.NoContent && replaced.Result != null);
            Assert.IsTrue(replaced.Result == entity2);
            Assert.IsTrue(replaced.Etag.Equals(entity2.ETag));
            Assert.IsTrue(!replaced.Etag.Equals(entity.ETag));
            Assert.IsTrue(replaced.Etag.Contains(","));
            //Assert.IsTrue(entity2.Timestamp.Equals(entity.Timestamp));  // weird, replace does not change timestamp


            var retrieved = DoRetrieve("1", "1");

            Assert.IsTrue(retrieved != null && retrieved.HttpStatusCode == (int)HttpStatusCode.OK && retrieved.Result != null);
            Assert.IsTrue(retrieved.Result is TestEntity);
            var ret = (TestEntity)retrieved.Result;
            Assert.IsTrue(ret.ETag.Equals(retrieved.Etag));

            Assert.IsTrue(ret != entity && ret != entity2);
            Assert.IsTrue(ret.PartitionKey.Equals(entity2.PartitionKey));
            Assert.IsTrue(ret.RowKey.Equals(entity2.RowKey));
            Assert.IsTrue(ret.ETag.Equals(entity2.ETag));
            Assert.IsTrue(ret.Timestamp >= entity2.Timestamp);
            Assert.IsTrue(ret.a == entity2.a && ret.a == 1);
            Assert.IsTrue(ret.b == entity2.b && ret.b == 2);
            Assert.IsTrue(ret.c == entity2.c && ret.c == 3);
            Assert.IsTrue(ret.d.Equals(entity2.d) && ret.d.Equals("!dlrow olleh"));
            Assert.IsTrue(ret.e == entity2.e && ret.e == false);
            Assert.IsTrue(ret.f == entity2.f && ret.f == 54.321);
        }

        [TestMethod]
        public void ReplaceStarTest()
        {
            Assert.IsTrue(DoCreateTable());
            TestEntity entity = new TestEntity("1", "1");

            entity.a = 3;
            entity.b = 2;
            entity.c = 1;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);

            TestEntity entity2 = new TestEntity("1", "1");

            entity2.a = 1;
            entity2.b = 2;
            entity2.c = 3;
            entity2.d = "!dlrow olleh";
            entity2.e = false;
            entity2.f = 54.321;

            entity2.ETag = "*";
            entity2.Timestamp = entity.Timestamp;

            var replaced = DoReplace(entity2);

            Assert.IsTrue(replaced != null && replaced.HttpStatusCode == (int)HttpStatusCode.NoContent && replaced.Result != null);
            Assert.IsTrue(replaced.Result == entity2);
            Assert.IsTrue(replaced.Etag.Equals(entity2.ETag));
            Assert.IsTrue(!replaced.Etag.Equals(entity.ETag));
            Assert.IsTrue(replaced.Etag.Contains(","));
            //Assert.IsTrue(entity2.Timestamp.Equals(entity.Timestamp));  // weird, replace does not change timestamp


            var retrieved = DoRetrieve("1", "1");

            Assert.IsTrue(retrieved != null && retrieved.HttpStatusCode == (int)HttpStatusCode.OK && retrieved.Result != null);
            Assert.IsTrue(retrieved.Result is TestEntity);
            var ret = (TestEntity)retrieved.Result;
            Assert.IsTrue(ret.ETag.Equals(retrieved.Etag));

            Assert.IsTrue(ret != entity && ret != entity2);
            Assert.IsTrue(ret.PartitionKey.Equals(entity2.PartitionKey));
            Assert.IsTrue(ret.RowKey.Equals(entity2.RowKey));
            Assert.IsTrue(ret.ETag.Equals(entity2.ETag));
            Assert.IsTrue(ret.Timestamp >= entity2.Timestamp);
            Assert.IsTrue(ret.a == entity2.a);
            Assert.IsTrue(ret.b == entity2.b);
            Assert.IsTrue(ret.c == entity2.c);
            Assert.IsTrue(ret.d.Equals(entity2.d));
            Assert.IsTrue(ret.e == entity2.e);
            Assert.IsTrue(ret.f == entity2.f);
        }


        [TestMethod]
        public void ReplaceBadETagTest()
        {
            Assert.IsTrue(DoCreateTable());
            TestEntity entity = new TestEntity("1", "1");

            entity.a = 3;
            entity.b = 2;
            entity.c = 1;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);

            TestEntity entity2 = new TestEntity("1", "1");

            entity2.a = 1;
            entity2.b = 2;
            entity2.c = 3;
            entity2.d = "!dlrow olleh";
            entity2.e = false;
            entity2.f = 54.321;

            entity2.ETag = "0";
            entity2.Timestamp = entity.Timestamp;

            try
            {
                var replaced = DoReplace(entity2);
                Assert.IsTrue(false);
            }
            catch (StorageException e)
            {
                Assert.IsTrue(e != null && e.RequestInformation.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed);
            }
        }

        [TestMethod]
        public void ReplaceNullETagTest()
        {
            Assert.IsTrue(DoCreateTable());
            TestEntity entity = new TestEntity("1", "1");

            entity.a = 3;
            entity.b = 2;
            entity.c = 1;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);

            TestEntity entity2 = new TestEntity("1", "1");

            entity2.a = 1;
            entity2.b = 2;
            entity2.c = 3;
            entity2.d = "!dlrow olleh";
            entity2.e = false;
            entity2.f = 54.321;

            entity2.ETag = null;
            entity2.Timestamp = entity.Timestamp;

            try
            {
                var replaced = DoReplace(entity2);
                Assert.IsTrue(false);
            }
            catch (ArgumentException e)
            {
                Assert.IsTrue(e != null);
            }
        }

        [TestMethod]
        public void ReplaceNotExistTest()
        {
            Assert.IsTrue(DoCreateTable());
            TestEntity entity = new TestEntity("1", "1");

            entity.a = 3;
            entity.b = 2;
            entity.c = 1;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);

            TestEntity entity2 = new TestEntity("2", "1");

            entity2.a = 1;
            entity2.b = 2;
            entity2.c = 3;
            entity2.d = "!dlrow olleh";
            entity2.e = false;
            entity2.f = 54.321;

            entity2.ETag = entity.ETag;
            entity2.Timestamp = entity.Timestamp;

            try
            {
                var replaced = DoReplace(entity2);
                Assert.IsTrue(false);
            }
            catch (StorageException e)
            {
                Assert.IsTrue(e != null && e.RequestInformation.HttpStatusCode == (int)HttpStatusCode.NotFound);
            }
        }

        [TestMethod]
        public void MergeTest()
        {
            Assert.IsTrue(DoCreateTable());
            TestEntity entity = new TestEntity("1", "1");

            entity.a = 3;
            entity.b = 2;
            entity.c = 1;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);

            var entity2 = new DynamicTableEntity("1", "1");

            entity2.Properties["a"] = new EntityProperty(1);
            entity2.Properties["d"] = new EntityProperty("wow!");

            entity2.ETag = entity.ETag;
            entity2.Timestamp = DateTimeOffset.MinValue;

            var replaced = DoMerge(entity2);

            Assert.IsTrue(replaced != null && replaced.HttpStatusCode == (int)HttpStatusCode.NoContent && replaced.Result != null);
            Assert.IsTrue(replaced.Result == entity2);
            Assert.IsTrue(replaced.Etag.Equals(entity2.ETag));
            Assert.IsTrue(!replaced.Etag.Equals(entity.ETag));
            Assert.IsTrue(replaced.Etag.Contains(","));
            //Assert.IsTrue(entity2.Timestamp.Equals(DateTimeOffset.MinValue));  // merge does not change timestamp


            var retrieved = DoRetrieve("1", "1");

            Assert.IsTrue(retrieved != null && retrieved.HttpStatusCode == (int)HttpStatusCode.OK && retrieved.Result != null);
            Assert.IsTrue(retrieved.Result is TestEntity);
            var ret = (TestEntity)retrieved.Result;
            Assert.IsTrue(ret.ETag.Equals(retrieved.Etag));

            Assert.IsTrue(ret != entity && ((ITableEntity)ret != (ITableEntity)entity2));
            Assert.IsTrue(ret.PartitionKey.Equals(entity.PartitionKey));
            Assert.IsTrue(ret.RowKey.Equals(entity.RowKey));
            Assert.IsTrue(ret.ETag.Equals(entity2.ETag));
            Assert.IsTrue(ret.Timestamp >= entity2.Timestamp && ret.Timestamp >= entity.Timestamp);
            Assert.IsTrue(ret.a == 1);
            Assert.IsTrue(ret.b == entity.b);
            Assert.IsTrue(ret.c == entity.c);
            Assert.IsTrue(ret.d.Equals("wow!"));
            Assert.IsTrue(ret.e == entity.e);
            Assert.IsTrue(ret.f == entity.f);
        }

        [TestMethod]
        public void MergeStarTest()
        {
            Assert.IsTrue(DoCreateTable());
            TestEntity entity = new TestEntity("1", "1");

            entity.a = 3;
            entity.b = 2;
            entity.c = 1;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);

            var entity2 = new DynamicTableEntity("1", "1");

            entity2.Properties["a"] = new EntityProperty(1);
            entity2.Properties["d"] = new EntityProperty("wow!");

            entity2.ETag = "*";

            var replaced = DoMerge(entity2);

            Assert.IsTrue(replaced != null && replaced.HttpStatusCode == (int)HttpStatusCode.NoContent && replaced.Result != null);
            Assert.IsTrue(replaced.Result == entity2);
            Assert.IsTrue(replaced.Etag.Equals(entity2.ETag));
            Assert.IsTrue(!replaced.Etag.Equals(entity.ETag));
            Assert.IsTrue(replaced.Etag.Contains(","));


            var retrieved = DoRetrieve("1", "1");

            Assert.IsTrue(retrieved != null && retrieved.HttpStatusCode == (int)HttpStatusCode.OK && retrieved.Result != null);
            Assert.IsTrue(retrieved.Result is TestEntity);
            var ret = (TestEntity)retrieved.Result;
            Assert.IsTrue(ret.ETag.Equals(retrieved.Etag));

            Assert.IsTrue(ret != entity && ((ITableEntity)ret != (ITableEntity)entity2));
            Assert.IsTrue(ret.PartitionKey.Equals(entity.PartitionKey));
            Assert.IsTrue(ret.RowKey.Equals(entity.RowKey));
            Assert.IsTrue(ret.ETag.Equals(entity2.ETag));
            Assert.IsTrue(ret.Timestamp >= entity2.Timestamp && ret.Timestamp >= entity.Timestamp);
            Assert.IsTrue(ret.a == 1);
            Assert.IsTrue(ret.b == entity.b);
            Assert.IsTrue(ret.c == entity.c);
            Assert.IsTrue(ret.d.Equals("wow!"));
            Assert.IsTrue(ret.e == entity.e);
            Assert.IsTrue(ret.f == entity.f);
        }


        [TestMethod]
        public void MergeBadETagTest()
        {
            Assert.IsTrue(DoCreateTable());
            TestEntity entity = new TestEntity("1", "1");

            entity.a = 3;
            entity.b = 2;
            entity.c = 1;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);

            var entity2 = new DynamicTableEntity("1", "1");

            entity2.Properties["a"] = new EntityProperty(1);
            entity2.Properties["d"] = new EntityProperty("wow!");

            entity2.ETag = "haha";

            try
            {
                var replaced = DoMerge(entity2);
                Assert.IsTrue(false);
            }
            catch (StorageException e)
            {
                Assert.IsTrue(e != null && e.RequestInformation.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed);
            }
        }

        [TestMethod]
        public void MergeNotFoundTest()
        {
            Assert.IsTrue(DoCreateTable());
            TestEntity entity = new TestEntity("1", "1");

            entity.a = 3;
            entity.b = 2;
            entity.c = 1;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);

            var entity2 = new DynamicTableEntity("1", "a");

            entity2.Properties["a"] = new EntityProperty(1);
            entity2.Properties["d"] = new EntityProperty("wow!");

            entity2.ETag = "haha";

            try
            {
                var replaced = DoMerge(entity2);
                Assert.IsTrue(false);
            }
            catch (StorageException e)
            {
                Assert.IsTrue(e != null && e.RequestInformation.HttpStatusCode == (int)HttpStatusCode.NotFound);
            }
        }

        [TestMethod]
        public void DeleteTest()
        {
            Assert.IsTrue(DoCreateTable());
            TestEntity entity = new TestEntity("1", "1");

            entity.a = 3;
            entity.b = 2;
            entity.c = 1;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);

            var entity2 = new TestEntity("1", "1");
            entity2.ETag = entity.ETag;
            entity2.Timestamp = DateTimeOffset.MinValue;
            entity2.a = 333;
            entity2.d = "WTF";

            var replaced = DoDelete(entity2);

            Assert.IsTrue(replaced != null && replaced.HttpStatusCode == (int)HttpStatusCode.NoContent && replaced.Result != null);
            Assert.IsTrue(replaced.Result == entity2);
            Assert.IsTrue(replaced.Etag.Equals(entity2.ETag));
            Assert.IsTrue(replaced.Etag.Equals(entity.ETag));
            Assert.IsTrue(entity2.a == 333);
            Assert.IsTrue(entity2.d.Equals("WTF"));
            Assert.IsTrue(entity2.Timestamp == DateTimeOffset.MinValue);


            var retrieved = DoRetrieve("1", "1");

            Assert.IsTrue(retrieved != null && retrieved.HttpStatusCode == (int)HttpStatusCode.NotFound && retrieved.Result == null);
        }

        [TestMethod]
        public void DeleteStarTest()
        {
            Assert.IsTrue(DoCreateTable());
            TestEntity entity = new TestEntity("1", "1");

            entity.a = 3;
            entity.b = 2;
            entity.c = 1;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);

            var entity2 = new TestEntity("1", "1");
            entity2.ETag = "*";
            entity2.Timestamp = DateTimeOffset.MinValue;
            entity2.a = 333;
            entity2.d = "WTF";

            var replaced = DoDelete(entity2);

            Assert.IsTrue(replaced != null && replaced.HttpStatusCode == (int)HttpStatusCode.NoContent && replaced.Result != null);
            Assert.IsTrue(replaced.Result == entity2);
            Assert.IsTrue(replaced.Etag.Equals(entity2.ETag));
            Assert.IsTrue(replaced.Etag.Equals("*"));
            Assert.IsTrue(entity2.a == 333);
            Assert.IsTrue(entity2.d.Equals("WTF"));
            Assert.IsTrue(entity2.Timestamp == DateTimeOffset.MinValue);


            var retrieved = DoRetrieve("1", "1");

            Assert.IsTrue(retrieved != null && retrieved.HttpStatusCode == (int)HttpStatusCode.NotFound && retrieved.Result == null);
        }


        [TestMethod]
        public void DeleteBadETagTest()
        {
            Assert.IsTrue(DoCreateTable());
            TestEntity entity = new TestEntity("1", "1");

            entity.a = 3;
            entity.b = 2;
            entity.c = 1;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);

            var entity2 = new TestEntity("1", "1");
            entity2.ETag = "*badetag*";
            entity2.Timestamp = DateTimeOffset.MinValue;
            entity2.a = 333;
            entity2.d = "WTF";

            try
            {
                var replaced = DoDelete(entity2);
                Assert.IsTrue(false);
            }
            catch (StorageException e)
            {
                Assert.IsTrue(e != null && e.RequestInformation.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed);
            }
        }

        [TestMethod]
        public void DeleteNotFoundTest()
        {
            Assert.IsTrue(DoCreateTable());
            TestEntity entity = new TestEntity("1", "1");

            entity.a = 3;
            entity.b = 2;
            entity.c = 1;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);

            var entity2 = new TestEntity("", "2");
            entity2.ETag = "*";
            entity2.Timestamp = DateTimeOffset.MinValue;
            entity2.a = 333;
            entity2.d = "WTF";

            try
            {
                var replaced = DoDelete(entity2);
                Assert.IsTrue(false);
            }
            catch (StorageException e)
            {
                Assert.IsTrue(e != null && e.RequestInformation.HttpStatusCode == (int)HttpStatusCode.NotFound);
            }
        }

        [TestMethod]
        public void DeleteInsertTest()
        {
            Assert.IsTrue(DoCreateTable());
            TestEntity entity = new TestEntity("1", "1");

            entity.a = 3;
            entity.b = 2;
            entity.c = 1;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);

            var entity2 = new TestEntity("1", "1");
            entity2.ETag = "*";
            entity2.Timestamp = DateTimeOffset.MinValue;
            entity2.a = 333;
            entity2.d = "WTF";

            var replaced = DoDelete(entity2);

            Assert.IsTrue(replaced != null && replaced.HttpStatusCode == (int)HttpStatusCode.NoContent && replaced.Result != null);
            Assert.IsTrue(replaced.Result == entity2);
            Assert.IsTrue(replaced.Etag.Equals(entity2.ETag));
            Assert.IsTrue(replaced.Etag.Equals("*"));
            Assert.IsTrue(entity2.a == 333);
            Assert.IsTrue(entity2.d.Equals("WTF"));
            Assert.IsTrue(entity2.Timestamp == DateTimeOffset.MinValue);


            var retrieved = DoRetrieve("1", "1");

            Assert.IsTrue(retrieved != null && retrieved.HttpStatusCode == (int)HttpStatusCode.NotFound && retrieved.Result == null);

            TestEntity newEntity = new TestEntity("1", "1");
            newEntity.a = 2;
            newEntity.b = 3;
            newEntity.c = 4;
            newEntity.d = "reborn";
            newEntity.e = false;
            newEntity.f = 33.33;

            var insertAgain = DoInsert(newEntity);
            Assert.IsTrue(insertAgain != null && insertAgain.HttpStatusCode == (int)HttpStatusCode.Created);
            Assert.IsTrue(!insertAgain.Etag.Equals(entity.ETag) && !insertAgain.Etag.Equals(entity2.ETag));

            var retrieveAgain = DoRetrieve("1", "1");
            Assert.IsTrue(retrieveAgain != null && retrieveAgain.HttpStatusCode == (int)HttpStatusCode.OK && retrieveAgain.Result != null);
            Assert.IsTrue(retrieveAgain.Result is TestEntity);
            var entity3 = (TestEntity)retrieveAgain.Result;
            Assert.IsTrue(entity3 != newEntity);
            Assert.IsTrue(entity3.a == newEntity.a);
            Assert.IsTrue(entity3.b == newEntity.b);
            Assert.IsTrue(entity3.c == newEntity.c);
            Assert.IsTrue(entity3.d.Equals(newEntity.d));
            Assert.IsTrue(entity3.e == newEntity.e);
            Assert.IsTrue(entity3.f == newEntity.f);
            // TODO: for delete -> insert, we changed it to replace, which will not return a valid timestamp...
            //Assert.IsTrue(entity3.Timestamp == newEntity.Timestamp);
            Assert.IsTrue(entity3.ETag.Equals(newEntity.ETag));
        }

        [TestMethod]
        public void IORInsertTest()
        {
            Assert.IsTrue(DoCreateTable());
            TestEntity entity = new TestEntity("1", "1");

            entity.a = 1;
            entity.b = 2;
            entity.c = 3;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            entity.Timestamp = DateTimeOffset.MinValue;

            var res = DoIOR(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.NoContent);
            Assert.IsTrue(res.Result == entity);
            Assert.IsTrue(res.Etag.Equals(entity.ETag));
            Assert.IsTrue(entity.ETag.Contains(","));
            //Assert.IsTrue(entity.Timestamp.Equals(DateTimeOffset.MinValue));

            var retrieved = DoRetrieve("1", "1");

            Assert.IsTrue(retrieved != null && retrieved.HttpStatusCode == (int)HttpStatusCode.OK && retrieved.Result != null);
            Assert.IsTrue(retrieved.Result is TestEntity);
            var ret = (TestEntity)retrieved.Result;
            Assert.IsTrue(ret.ETag.Equals(retrieved.Etag));

            Assert.IsTrue(ret != entity);
            Assert.IsTrue(ret.PartitionKey.Equals(entity.PartitionKey));
            Assert.IsTrue(ret.RowKey.Equals(entity.RowKey));
            Assert.IsTrue(ret.ETag.Equals(entity.ETag));
            Assert.IsTrue(ret.a == entity.a && ret.a == 1);
            Assert.IsTrue(ret.b == entity.b && ret.b == 2);
            Assert.IsTrue(ret.c == entity.c && ret.c == 3);
            Assert.IsTrue(ret.d.Equals(entity.d) && ret.d.Equals("hello world!"));
            Assert.IsTrue(ret.e == entity.e && ret.e == true);
            Assert.IsTrue(ret.f == entity.f && ret.f == 123.45);
        }

        [TestMethod]
        public void IOMInsertTest()
        {
            Assert.IsTrue(DoCreateTable());
            TestEntity entity = new TestEntity("1", "1");

            entity.a = 1;
            entity.b = 2;
            entity.c = 3;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            entity.Timestamp = DateTimeOffset.MinValue;

            var res = DoIOM(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.NoContent);
            Assert.IsTrue(res.Result == entity);
            Assert.IsTrue(res.Etag.Equals(entity.ETag));
            Assert.IsTrue(entity.ETag.Contains(","));
            //Assert.IsTrue(entity.Timestamp.Equals(DateTimeOffset.MinValue));

            var retrieved = DoRetrieve("1", "1");

            Assert.IsTrue(retrieved != null && retrieved.HttpStatusCode == (int)HttpStatusCode.OK && retrieved.Result != null);
            Assert.IsTrue(retrieved.Result is TestEntity);
            var ret = (TestEntity)retrieved.Result;
            Assert.IsTrue(ret.ETag.Equals(retrieved.Etag));

            Assert.IsTrue(ret != entity);
            Assert.IsTrue(ret.PartitionKey.Equals(entity.PartitionKey));
            Assert.IsTrue(ret.RowKey.Equals(entity.RowKey));
            Assert.IsTrue(ret.ETag.Equals(entity.ETag));
            Assert.IsTrue(ret.a == entity.a && ret.a == 1);
            Assert.IsTrue(ret.b == entity.b && ret.b == 2);
            Assert.IsTrue(ret.c == entity.c && ret.c == 3);
            Assert.IsTrue(ret.d.Equals(entity.d) && ret.d.Equals("hello world!"));
            Assert.IsTrue(ret.e == entity.e && ret.e == true);
            Assert.IsTrue(ret.f == entity.f && ret.f == 123.45);
        }

        [TestMethod]
        public void IORReplaceTest()
        {
            Assert.IsTrue(DoCreateTable());
            TestEntity entity = new TestEntity("1", "1");

            entity.a = 1;
            entity.b = 2;
            entity.c = 3;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);

            TestEntity entity2 = new TestEntity("1", "1");
            entity2.a = 33;
            entity2.b = 22;
            entity2.c = 11;
            entity2.d = "iorReplaced";
            entity2.e = true;
            entity2.f = 4444;

            var replaced = DoIOR(entity2);
            Assert.IsTrue(replaced != null && replaced.HttpStatusCode == (int)HttpStatusCode.NoContent);

            var retrieved = DoRetrieve("1", "1");
            Assert.IsTrue(retrieved != null && retrieved.HttpStatusCode == (int)HttpStatusCode.OK && retrieved.Result != null);
            Assert.IsTrue(retrieved.Result is TestEntity);
            var ret = (TestEntity)retrieved.Result;
            Assert.IsTrue(ret.ETag.Equals(retrieved.Etag));

            Assert.IsTrue(ret != entity && ret != entity2);
            Assert.IsTrue(ret.PartitionKey.Equals(entity2.PartitionKey));
            Assert.IsTrue(ret.RowKey.Equals(entity2.RowKey));
            Assert.IsTrue(ret.ETag.Equals(entity2.ETag));
            Assert.IsTrue(ret.a == entity2.a && ret.a == 33);
            Assert.IsTrue(ret.b == entity2.b && ret.b == 22);
            Assert.IsTrue(ret.c == entity2.c && ret.c == 11);
            Assert.IsTrue(ret.d.Equals(entity2.d) && ret.d.Equals("iorReplaced"));
            Assert.IsTrue(ret.e == entity2.e && ret.e == true);
            Assert.IsTrue(ret.f == entity2.f && ret.f == 4444);
        }

        [TestMethod]
        public void IOMMergeTest()
        {
            Assert.IsTrue(DoCreateTable());
            TestEntity entity = new TestEntity("1", "1");

            entity.a = 1;
            entity.b = 2;
            entity.c = 3;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);

            var entity2 = new DynamicTableEntity("1", "1");
            entity2.Properties["a"] = new EntityProperty(33);
            entity2.Properties["d"] = new EntityProperty("iomMerged");

            var replaced = DoIOM(entity2);
            Assert.IsTrue(replaced != null && replaced.HttpStatusCode == (int)HttpStatusCode.NoContent);

            var retrieved = DoRetrieve("1", "1");
            Assert.IsTrue(retrieved != null && retrieved.HttpStatusCode == (int)HttpStatusCode.OK && retrieved.Result != null);
            Assert.IsTrue(retrieved.Result is TestEntity);
            var ret = (TestEntity)retrieved.Result;
            Assert.IsTrue(ret.ETag.Equals(retrieved.Etag));

            Assert.IsTrue(ret != entity && ((ITableEntity)ret != (ITableEntity)entity2));
            Assert.IsTrue(ret.PartitionKey.Equals(entity2.PartitionKey));
            Assert.IsTrue(ret.RowKey.Equals(entity2.RowKey));
            Assert.IsTrue(ret.ETag.Equals(entity2.ETag));
            Assert.IsTrue(ret.a == 33);
            Assert.IsTrue(ret.b == 2);
            Assert.IsTrue(ret.c ==3);
            Assert.IsTrue(ret.d.Equals("iomMerged"));
            Assert.IsTrue(ret.e == true);
            Assert.IsTrue(ret.f == 123.45);
        }

        [TestMethod]
        public void IORInsertDeletedTest()
        {
            Assert.IsTrue(DoCreateTable());
            TestEntity entity = new TestEntity("1", "1");

            entity.a = 1;
            entity.b = 2;
            entity.c = 3;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);

            var deleted = DoDelete(entity);
            Assert.IsTrue(deleted != null && deleted.HttpStatusCode == (int)HttpStatusCode.NoContent);

            TestEntity entity2 = new TestEntity("1", "1");
            entity2.a = 33;
            entity2.b = 22;
            entity2.c = 11;
            entity2.d = "iorInsertDeleted";
            entity2.e = true;
            entity2.f = 4444;

            var replaced = DoIOR(entity2);
            Assert.IsTrue(replaced != null && replaced.HttpStatusCode == (int)HttpStatusCode.NoContent);

            var retrieved = DoRetrieve("1", "1");
            Assert.IsTrue(retrieved != null && retrieved.HttpStatusCode == (int)HttpStatusCode.OK && retrieved.Result != null);
            Assert.IsTrue(retrieved.Result is TestEntity);
            var ret = (TestEntity)retrieved.Result;
            Assert.IsTrue(ret.ETag.Equals(retrieved.Etag));

            Assert.IsTrue(ret != entity && ret != entity2);
            Assert.IsTrue(ret.PartitionKey.Equals(entity2.PartitionKey));
            Assert.IsTrue(ret.RowKey.Equals(entity2.RowKey));
            Assert.IsTrue(ret.ETag.Equals(entity2.ETag));
            Assert.IsTrue(ret.a == entity2.a && ret.a == 33);
            Assert.IsTrue(ret.b == entity2.b && ret.b == 22);
            Assert.IsTrue(ret.c == entity2.c && ret.c == 11);
            Assert.IsTrue(ret.d.Equals(entity2.d) && ret.d.Equals("iorInsertDeleted"));
            Assert.IsTrue(ret.e == entity2.e && ret.e == true);
            Assert.IsTrue(ret.f == entity2.f && ret.f == 4444);
        }

        [TestMethod]
        public void IOMInsertDeletedTest()
        {
            Assert.IsTrue(DoCreateTable());
            TestEntity entity = new TestEntity("1", "1");

            entity.a = 1;
            entity.b = 2;
            entity.c = 3;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);

            var deleted = DoDelete(entity);
            Assert.IsTrue(deleted != null && deleted.HttpStatusCode == (int)HttpStatusCode.NoContent);

            TestEntity entity2 = new TestEntity("1", "1");
            entity2.a = 33;
            entity2.b = 22;
            entity2.c = 11;
            entity2.d = "iomInsertDeleted";
            entity2.e = true;
            entity2.f = 4444;

            var replaced = DoIOM(entity2);
            Assert.IsTrue(replaced != null && replaced.HttpStatusCode == (int)HttpStatusCode.NoContent);

            var retrieved = DoRetrieve("1", "1");
            Assert.IsTrue(retrieved != null && retrieved.HttpStatusCode == (int)HttpStatusCode.OK && retrieved.Result != null);
            Assert.IsTrue(retrieved.Result is TestEntity);
            var ret = (TestEntity)retrieved.Result;
            Assert.IsTrue(ret.ETag.Equals(retrieved.Etag));

            Assert.IsTrue(ret != entity && ret != entity2);
            Assert.IsTrue(ret.PartitionKey.Equals(entity2.PartitionKey));
            Assert.IsTrue(ret.RowKey.Equals(entity2.RowKey));
            Assert.IsTrue(ret.ETag.Equals(entity2.ETag));
            Assert.IsTrue(ret.a == entity2.a && ret.a == 33);
            Assert.IsTrue(ret.b == entity2.b && ret.b == 22);
            Assert.IsTrue(ret.c == entity2.c && ret.c == 11);
            Assert.IsTrue(ret.d.Equals(entity2.d) && ret.d.Equals("iomInsertDeleted"));
            Assert.IsTrue(ret.e == entity2.e && ret.e == true);
            Assert.IsTrue(ret.f == entity2.f && ret.f == 4444);
        }

        [TestMethod]
        public void CreateSnapshotTest()
        {
            Assert.IsTrue(DoCreateTable());
            TestEntity entity = new TestEntity("1", "1");

            entity.a = 1;
            entity.b = 2;
            entity.c = 3;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);
            Assert.IsTrue(res.Result == entity);
            Assert.IsTrue(res.Etag.Equals(entity.ETag));
            Assert.IsTrue(entity.ETag.Contains(","));

            var ssid = DoCreateSnapshot();
            Assert.IsTrue(ssid == 0);
        }

        [TestMethod]
        public void ReadSnapshotTest()
        {
            Assert.IsTrue(DoCreateTable());
            TestEntity entity = new TestEntity("1", "1");

            entity.a = 1;
            entity.b = 2;
            entity.c = 3;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);
            Assert.IsTrue(res.Result == entity);
            Assert.IsTrue(res.Etag.Equals(entity.ETag));
            Assert.IsTrue(entity.ETag.Contains(","));

            var ssid = DoCreateSnapshot();
            Assert.IsTrue(ssid == 0);


            var retrieved1 = DoRetrieveSnapshot(ssid, "1", "1");

            Assert.IsTrue(retrieved1 != null && retrieved1.HttpStatusCode == (int)HttpStatusCode.OK && retrieved1.Result != null);
            Assert.IsTrue(retrieved1.Result is TestEntity);
            var ret1 = (TestEntity)retrieved1.Result;
            Assert.IsTrue(ret1.ETag.Equals(retrieved1.Etag));

            Assert.IsTrue(ret1 != entity);
            Assert.IsTrue(ret1.PartitionKey.Equals(entity.PartitionKey));
            Assert.IsTrue(ret1.RowKey.Equals(entity.RowKey));
            Assert.IsTrue(ret1.ETag.Equals(entity.ETag));
            // TODO: COW to snapshot table kills the original timestamp, is this ok?
            //Assert.IsTrue(ret1.Timestamp.Equals(entity.Timestamp));
            Assert.IsTrue(ret1.a == entity.a && ret1.a == 1);
            Assert.IsTrue(ret1.b == entity.b && ret1.b == 2);
            Assert.IsTrue(ret1.c == entity.c && ret1.c == 3);
            Assert.IsTrue(ret1.d.Equals(entity.d) && ret1.d.Equals("hello world!"));
            Assert.IsTrue(ret1.e == entity.e && ret1.e == true);
            Assert.IsTrue(ret1.f == entity.f && ret1.f == 123.45);
        }

        [TestMethod]
        public void ReadInvalidSnapshotTest()
        {
            Assert.IsTrue(DoCreateTable());
            TestEntity entity = new TestEntity("1", "1");

            entity.a = 1;
            entity.b = 2;
            entity.c = 3;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);
            Assert.IsTrue(res.Result == entity);
            Assert.IsTrue(res.Etag.Equals(entity.ETag));
            Assert.IsTrue(entity.ETag.Contains(","));

            var ssid0 = DoCreateSnapshot();
            Assert.IsTrue(ssid0 == 0);

            var ssid1 = DoCreateSnapshot();
            Assert.IsTrue(ssid1 == 1);


            try
            {
                var retrieved1 = DoRetrieveSnapshot(2, "1", "1");
                Assert.IsTrue(false);
            }
            catch (StorageException e)
            {
                Assert.IsTrue(e.RequestInformation.HttpStatusCode == (int)HttpStatusCode.BadRequest);
            }

            try
            {
                var retrieved1 = DoRetrieveSnapshot(3, "1", "1");
                Assert.IsTrue(false);
            }
            catch (StorageException e)
            {
                Assert.IsTrue(e.RequestInformation.HttpStatusCode == (int)HttpStatusCode.BadRequest);
            }
        }

        [TestMethod]
        public void ReadSnapshotNotExistRowTest()
        {
            Assert.IsTrue(DoCreateTable());
            TestEntity entity = new TestEntity("1", "1");

            entity.a = 1;
            entity.b = 2;
            entity.c = 3;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);
            Assert.IsTrue(res.Result == entity);
            Assert.IsTrue(res.Etag.Equals(entity.ETag));
            Assert.IsTrue(entity.ETag.Contains(","));

            var ssid = DoCreateSnapshot();
            Assert.IsTrue(ssid == 0);


            var retrieved1 = DoRetrieveSnapshot(ssid, "1", "2");
            Assert.IsTrue(retrieved1 != null && retrieved1.HttpStatusCode == (int)HttpStatusCode.NotFound && retrieved1.Result == null);


            TestEntity entity2 = new TestEntity("1", "2");

            entity2.a = 3;
            entity2.b = 3;
            entity2.c = 3;
            entity2.d = "hello world!";
            entity2.e = true;
            entity2.f = 323.45;

            var res2 = DoInsert(entity2);
            Assert.IsTrue(res2 != null);
            Assert.IsTrue(res2.HttpStatusCode == (int)HttpStatusCode.Created);

            var retrieved2 = DoRetrieveSnapshot(ssid, "1", "2");
            Assert.IsTrue(retrieved2 != null && retrieved2.HttpStatusCode == (int)HttpStatusCode.NotFound && retrieved2.Result == null);
        }

        [TestMethod]
        public void ReadSnapshotHistoryNotExistTest()
        {
            Assert.IsTrue(DoCreateTable());

            var ssid = DoCreateSnapshot();
            Assert.IsTrue(ssid == 0);

            TestEntity entity = new TestEntity("1", "1");

            entity.a = 1;
            entity.b = 2;
            entity.c = 3;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);
            Assert.IsTrue(res.Result == entity);
            Assert.IsTrue(res.Etag.Equals(entity.ETag));
            Assert.IsTrue(entity.ETag.Contains(","));


            var retrieved1 = DoRetrieveSnapshot(ssid, "1", "2");
            Assert.IsTrue(retrieved1 != null && retrieved1.HttpStatusCode == (int)HttpStatusCode.NotFound && retrieved1.Result == null);


            TestEntity entity2 = new TestEntity("1", "1");

            entity2.a = 3;
            entity2.b = 3;
            entity2.c = 3;
            entity2.d = "hello world!";
            entity2.e = true;
            entity2.f = 323.45;
            entity2.ETag = entity.ETag;

            var res2 = DoReplace(entity2);
            Assert.IsTrue(res2 != null);
            Assert.IsTrue(res2.HttpStatusCode == (int)HttpStatusCode.NoContent);

            var retrieved2 = DoRetrieveSnapshot(ssid, "1", "1");
            Assert.IsTrue(retrieved2 != null && retrieved2.HttpStatusCode == (int)HttpStatusCode.NotFound && retrieved2.Result == null);

            ssid = DoCreateSnapshot();

            var retrieved3 = DoRetrieveSnapshot(ssid, "1", "1");
            Assert.IsTrue(retrieved3 != null && retrieved3.HttpStatusCode == (int)HttpStatusCode.OK && retrieved3.Result != null);
            var ret = (TestEntity)retrieved3.Result;
            Assert.IsTrue(ret.ETag.Equals(entity2.ETag));
            Assert.IsTrue(ret.a == 3);
            Assert.IsTrue(ret.b == 3);
            Assert.IsTrue(ret.c == 3);
            Assert.IsTrue(ret.d.Equals("hello world!"));
            Assert.IsTrue(ret.e == true);
            Assert.IsTrue(ret.f == 323.45);
        }

        [TestMethod]
        public void ReadSnapshotUpdatedRowTest()
        {
            Assert.IsTrue(DoCreateTable());
            TestEntity entity = new TestEntity("1", "1");

            entity.a = 1;
            entity.b = 2;
            entity.c = 3;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);
            Assert.IsTrue(res.Result == entity);
            Assert.IsTrue(res.Etag.Equals(entity.ETag));
            Assert.IsTrue(entity.ETag.Contains(","));

            var ssid = DoCreateSnapshot();
            Assert.IsTrue(ssid == 0);

            TestEntity entity2 = new TestEntity("1", "1");

            entity2.a = 5;
            entity2.b = 5;
            entity2.c = 5;
            entity2.d = "!dlrow olleh";
            entity2.e = false;
            entity2.f = 54.321;

            entity2.ETag = entity.ETag;

            var replaced = DoReplace(entity2);
            Assert.IsTrue(replaced != null && replaced.HttpStatusCode == (int)HttpStatusCode.NoContent && replaced.Result != null);

            var retrieved1 = DoRetrieveSnapshot(ssid, "1", "1");

            Assert.IsTrue(retrieved1 != null && retrieved1.HttpStatusCode == (int)HttpStatusCode.OK && retrieved1.Result != null);
            Assert.IsTrue(retrieved1.Result is TestEntity);
            var ret1 = (TestEntity)retrieved1.Result;
            Assert.IsTrue(ret1.ETag.Equals(retrieved1.Etag));

            Assert.IsTrue(ret1 != entity && ret1 != entity2);
            Assert.IsTrue(ret1.PartitionKey.Equals(entity.PartitionKey));
            Assert.IsTrue(ret1.RowKey.Equals(entity.RowKey));
            Assert.IsTrue(ret1.ETag.Equals(entity.ETag));
            // TODO: COW to snapshot table kills the original timestamp, is this ok?
            //Assert.IsTrue(ret1.Timestamp.Equals(entity.Timestamp));
            Assert.IsTrue(ret1.a == entity.a && ret1.a == 1);
            Assert.IsTrue(ret1.b == entity.b && ret1.b == 2);
            Assert.IsTrue(ret1.c == entity.c && ret1.c == 3);
            Assert.IsTrue(ret1.d.Equals(entity.d) && ret1.d.Equals("hello world!"));
            Assert.IsTrue(ret1.e == entity.e && ret1.e == true);
            Assert.IsTrue(ret1.f == entity.f && ret1.f == 123.45);


            var retrieved2 = DoRetrieve("1", "1");
            Assert.IsTrue(retrieved2 != null && retrieved2.HttpStatusCode == (int)HttpStatusCode.OK && retrieved2.Result != null);
            Assert.IsTrue(retrieved2.Result is TestEntity);
            var ret2 = (TestEntity)retrieved2.Result;
            Assert.IsTrue(ret2.ETag.Equals(retrieved2.Etag));

            Assert.IsTrue(ret2 != entity && ret2 != entity2);
            Assert.IsTrue(ret2.PartitionKey.Equals(entity2.PartitionKey));
            Assert.IsTrue(ret2.RowKey.Equals(entity2.RowKey));
            Assert.IsTrue(ret2.ETag.Equals(entity2.ETag));
            Assert.IsTrue(ret2.a == entity2.a && ret2.a == 5);
            Assert.IsTrue(ret2.b == entity2.b && ret2.b == 5);
            Assert.IsTrue(ret2.c == entity2.c && ret2.c == 5);
            Assert.IsTrue(ret2.d.Equals(entity2.d) && ret2.d.Equals("!dlrow olleh"));
            Assert.IsTrue(ret2.e == entity2.e && ret2.e == false);
            Assert.IsTrue(ret2.f == entity2.f && ret2.f == 54.321);
        }

        [TestMethod]
        public void ReadSnapshotJumpTest()
        {
            Assert.IsTrue(DoCreateTable());

            TestEntity entity = new TestEntity("1", "1");

            entity.a = 0;
            entity.b = 2;
            entity.c = 3;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);
            Assert.IsTrue(res.Result == entity);
            Assert.IsTrue(res.Etag.Equals(entity.ETag));
            Assert.IsTrue(entity.ETag.Contains(","));

            var ssid0 = DoCreateSnapshot();
            Assert.IsTrue(ssid0 == 0);

            var ssid1 = DoCreateSnapshot();
            Assert.IsTrue(ssid1 == 1);

            TestEntity entity2 = new TestEntity("1", "1");

            entity2.a = 2;
            entity2.b = 3;
            entity2.c = 3;
            entity2.d = "hello world!";
            entity2.e = true;
            entity2.f = 323.45;
            entity2.ETag = entity.ETag;

            var res2 = DoReplace(entity2);
            Assert.IsTrue(res2 != null);
            Assert.IsTrue(res2.HttpStatusCode == (int)HttpStatusCode.NoContent);

            var ssid2 = DoCreateSnapshot();
            Assert.IsTrue(ssid2 == 2);

            var ssid3 = DoCreateSnapshot();
            Assert.IsTrue(ssid3 == 3);

            var ssid4 = DoCreateSnapshot();
            Assert.IsTrue(ssid4 == 4);

            var retrieved1 = DoRetrieveSnapshot(ssid1, "1", "1");
            Assert.IsTrue(retrieved1 != null && retrieved1.HttpStatusCode == (int)HttpStatusCode.OK && retrieved1.Result != null);
            var ret1 = (TestEntity)retrieved1.Result;
            Assert.IsTrue(ret1.ETag == entity.ETag && ret1.a == 0); // ignore other check

            TestEntity entity3 = new TestEntity("1", "1");

            entity3.a = 5;
            entity3.b = 3;
            entity3.c = 3;
            entity3.d = "hello world!";
            entity3.e = true;
            entity3.f = 323.45;
            entity3.ETag = entity2.ETag;

            var res3 = DoReplace(entity3);
            Assert.IsTrue(res3 != null);
            Assert.IsTrue(res3.HttpStatusCode == (int)HttpStatusCode.NoContent);

            var retrieved2 = DoRetrieveSnapshot(ssid4, "1", "1");
            Assert.IsTrue(retrieved2 != null && retrieved2.HttpStatusCode == (int)HttpStatusCode.OK && retrieved2.Result != null);
            var ret2 = (TestEntity)retrieved2.Result;
            Assert.IsTrue(ret2.ETag == entity2.ETag && ret2.a == 2); // ignore other check

            var retrieved3 = DoRetrieveSnapshot(ssid4, "1", "1");
            Assert.IsTrue(retrieved3 != null && retrieved3.HttpStatusCode == (int)HttpStatusCode.OK && retrieved3.Result != null);
            var ret3 = (TestEntity)retrieved3.Result;
            Assert.IsTrue(ret3.ETag == entity2.ETag && ret3.a == 2); // ignore other check
        }


        [TestMethod]
        public void ReadSnapshotDeleteInsertTest()
        {
            Assert.IsTrue(DoCreateTable());

            var ssid0 = DoCreateSnapshot();
            Assert.IsTrue(ssid0 == 0);

            var ssid1 = DoCreateSnapshot();
            Assert.IsTrue(ssid1 == 1);

            TestEntity entity = new TestEntity("1", "1");

            entity.a = 2;
            entity.b = 2;
            entity.c = 3;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);
            Assert.IsTrue(res.Result == entity);
            Assert.IsTrue(res.Etag.Equals(entity.ETag));
            Assert.IsTrue(entity.ETag.Contains(","));

            var ssid2 = DoCreateSnapshot();
            Assert.IsTrue(ssid2 == 2);

            var ssid3 = DoCreateSnapshot();
            Assert.IsTrue(ssid3 == 3);

            TestEntity dEntity = new TestEntity("1", "1");
            dEntity.ETag = entity.ETag;
            var res2 = DoDelete(dEntity);
            Assert.IsTrue(res2 != null && res2.HttpStatusCode == (int)HttpStatusCode.NoContent);

            var ssid4 = DoCreateSnapshot();
            Assert.IsTrue(ssid4 == 4);

            var ssid5 = DoCreateSnapshot();
            Assert.IsTrue(ssid5 == 5);

            var entity2 = new TestEntity("1", "1");
            entity2.a = 6;
            entity2.b = 3;
            entity2.c = 3;
            entity2.d = "hello world!";
            entity2.e = true;
            entity2.f = 323.45;

            var res3 = DoIOR(entity2);
            Assert.IsTrue(res3 != null);
            Assert.IsTrue(res3.HttpStatusCode == (int)HttpStatusCode.NoContent);

            var ssid6 = DoCreateSnapshot();
            Assert.IsTrue(ssid6 == 6);

            var ssid7 = DoCreateSnapshot();
            Assert.IsTrue(ssid7 == 7);


            var entity3 = new TestEntity("1", "1");
            entity3.a = 8;
            entity3.b = 3;
            entity3.c = 3;
            entity3.d = "hello world!";
            entity3.e = true;
            entity3.f = 323.45;
            entity3.ETag = entity2.ETag;

            var res4 = DoReplace(entity3);
            Assert.IsTrue(res4 != null);
            Assert.IsTrue(res4.HttpStatusCode == (int)HttpStatusCode.NoContent);


            var retrieved1 = DoRetrieveSnapshot(ssid4, "1", "1");
            Assert.IsTrue(retrieved1 != null && retrieved1.HttpStatusCode == (int)HttpStatusCode.NotFound);

            var retrieved2 = DoRetrieveSnapshot(ssid5, "1", "1");
            Assert.IsTrue(retrieved2 != null && retrieved2.HttpStatusCode == (int)HttpStatusCode.NotFound);

            var retrieved3 = DoRetrieveSnapshot(ssid6, "1", "1");
            Assert.IsTrue(retrieved3 != null && retrieved3.HttpStatusCode == (int)HttpStatusCode.OK && retrieved3.Result != null);
            var ret3 = (TestEntity)retrieved3.Result;
            Assert.IsTrue(ret3.ETag == entity2.ETag && ret3.a == 6); // ignore other check

            var retrieved4 = DoRetrieveSnapshot(ssid7, "1", "1");
            Assert.IsTrue(retrieved4 != null && retrieved4.HttpStatusCode == (int)HttpStatusCode.OK && retrieved4.Result != null);
            var ret4 = (TestEntity)retrieved4.Result;
            Assert.IsTrue(ret4.ETag == entity2.ETag && ret4.a == 6); // ignore other check

            var retrieved5 = DoRetrieveSnapshot(ssid3, "1", "1");
            Assert.IsTrue(retrieved5 != null && retrieved5.HttpStatusCode == (int)HttpStatusCode.OK && retrieved5.Result != null);
            var ret5 = (TestEntity)retrieved5.Result;
            Assert.IsTrue(ret5.ETag == entity.ETag && ret5.a == 2); // ignore other check

            var retrieved6 = DoRetrieveSnapshot(ssid1, "1", "1");
            Assert.IsTrue(retrieved6 != null && retrieved6.HttpStatusCode == (int)HttpStatusCode.NotFound);
        }

        [TestMethod]
        public void DeleteSnapshotTest()
        {
            Assert.IsTrue(DoCreateTable());

            TestEntity entity = new TestEntity("1", "1");

            entity.a = 0;
            entity.b = 2;
            entity.c = 3;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);
            Assert.IsTrue(res.Result == entity);
            Assert.IsTrue(res.Etag.Equals(entity.ETag));
            Assert.IsTrue(entity.ETag.Contains(","));


            var ssid0 = DoCreateSnapshot();
            Assert.IsTrue(ssid0 == 0);

            var ssid1 = DoCreateSnapshot();
            Assert.IsTrue(ssid1 == 1);


            var entity2 = new TestEntity("1", "1");
            entity2.a = 2;
            entity2.b = 3;
            entity2.c = 3;
            entity2.d = "hello world!";
            entity2.e = true;
            entity2.f = 323.45;

            var res3 = DoIOR(entity2);
            Assert.IsTrue(res3 != null);
            Assert.IsTrue(res3.HttpStatusCode == (int)HttpStatusCode.NoContent);

            DoDeleteSnapshot(ssid0);

            var retrieved1 = DoRetrieveSnapshot(ssid1, "1", "1");
            Assert.IsTrue(retrieved1 != null && retrieved1.HttpStatusCode == (int)HttpStatusCode.OK && retrieved1.Result != null);
            var ret1 = (TestEntity)retrieved1.Result;
            Assert.IsTrue(ret1.ETag == entity.ETag && ret1.a == 0); // ignore other check

            var ssid2 = DoCreateSnapshot();

            var retrieved2 = DoRetrieveSnapshot(ssid2, "1", "1");
            Assert.IsTrue(retrieved2 != null && retrieved2.HttpStatusCode == (int)HttpStatusCode.OK && retrieved2.Result != null);
            var ret2 = (TestEntity)retrieved2.Result;
            Assert.IsTrue(ret2.ETag == entity2.ETag && ret2.a == 2); // ignore other check
        }

        [TestMethod]
        public void DeleteSnapshotStopPushTest()
        {
            Assert.IsTrue(DoCreateTable());

            TestEntity entity = new TestEntity("1", "1");

            entity.a = 0;
            entity.b = 2;
            entity.c = 3;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);
            Assert.IsTrue(res.Result == entity);
            Assert.IsTrue(res.Etag.Equals(entity.ETag));
            Assert.IsTrue(entity.ETag.Contains(","));


            var ssid0 = DoCreateSnapshot();
            Assert.IsTrue(ssid0 == 0);

            var entity2 = new TestEntity("1", "1");
            entity2.a = 1;
            entity2.b = 3;
            entity2.c = 3;
            entity2.d = "hello world!";
            entity2.e = true;
            entity2.f = 323.45;

            var res3 = DoIOR(entity2);
            Assert.IsTrue(res3 != null);
            Assert.IsTrue(res3.HttpStatusCode == (int)HttpStatusCode.NoContent);

            var ssid1 = DoCreateSnapshot();
            Assert.IsTrue(ssid1 == 1);

            DoDeleteSnapshot(ssid0);

            var retrieved1 = DoRetrieveSnapshot(ssid1, "1", "1");
            Assert.IsTrue(retrieved1 != null && retrieved1.HttpStatusCode == (int)HttpStatusCode.OK && retrieved1.Result != null);
            var ret1 = (TestEntity)retrieved1.Result;
            Assert.IsTrue(ret1.ETag == entity2.ETag && ret1.a == 1); // ignore other check
        }

        [TestMethod]
        public void DeleteSnapshotPushAcrossHoleTest()
        {
            Assert.IsTrue(DoCreateTable());

            TestEntity entity = new TestEntity("1", "1");

            entity.a = 0;
            entity.b = 2;
            entity.c = 3;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);
            Assert.IsTrue(res.Result == entity);
            Assert.IsTrue(res.Etag.Equals(entity.ETag));
            Assert.IsTrue(entity.ETag.Contains(","));


            var ssid0 = DoCreateSnapshot();
            Assert.IsTrue(ssid0 == 0);

            var ssid1 = DoCreateSnapshot();
            Assert.IsTrue(ssid1 == 1);

            var ssid2 = DoCreateSnapshot();
            Assert.IsTrue(ssid2 == 2);

            var entity2 = new TestEntity("1", "1");
            entity2.a = 3;
            entity2.b = 3;
            entity2.c = 3;
            entity2.d = "hello world!";
            entity2.e = true;
            entity2.f = 323.45;

            var res3 = DoIOR(entity2);
            Assert.IsTrue(res3 != null);
            Assert.IsTrue(res3.HttpStatusCode == (int)HttpStatusCode.NoContent);

            DoDeleteSnapshot(ssid1);
            DoDeleteSnapshot(ssid0);    // need to push across ssid1 to ssid2

            var retrieved1 = DoRetrieveSnapshot(ssid2, "1", "1");
            Assert.IsTrue(retrieved1 != null && retrieved1.HttpStatusCode == (int)HttpStatusCode.OK && retrieved1.Result != null);
            var ret1 = (TestEntity)retrieved1.Result;
            Assert.IsTrue(ret1.ETag == entity.ETag && ret1.a == 0); // ignore other check
        }

        [TestMethod]
        public void COWPushAcrossDeletedSnapshotTest()
        {
            Assert.IsTrue(DoCreateTable());

            TestEntity entity = new TestEntity("1", "1");

            entity.a = 0;
            entity.b = 2;
            entity.c = 3;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);
            Assert.IsTrue(res.Result == entity);
            Assert.IsTrue(res.Etag.Equals(entity.ETag));
            Assert.IsTrue(entity.ETag.Contains(","));

            var ssid0 = DoCreateSnapshot();
            Assert.IsTrue(ssid0 == 0);

            var ssid1 = DoCreateSnapshot();
            Assert.IsTrue(ssid1 == 1);

            var ssid2 = DoCreateSnapshot();
            Assert.IsTrue(ssid2 == 2);

            DoDeleteSnapshot(ssid1);
            DoDeleteSnapshot(ssid0);


            var entity2 = new TestEntity("1", "1");
            entity2.a = 3;
            entity2.b = 3;
            entity2.c = 3;
            entity2.d = "hello world!";
            entity2.e = true;
            entity2.f = 323.45;

            var res3 = DoIOR(entity2);      // need to push old content to snapshot 2 (0 & 1 deleted)
            Assert.IsTrue(res3 != null);
            Assert.IsTrue(res3.HttpStatusCode == (int)HttpStatusCode.NoContent);



            var retrieved1 = DoRetrieveSnapshot(ssid2, "1", "1");
            Assert.IsTrue(retrieved1 != null && retrieved1.HttpStatusCode == (int)HttpStatusCode.OK && retrieved1.Result != null);
            var ret1 = (TestEntity)retrieved1.Result;
            Assert.IsTrue(ret1.ETag == entity.ETag && ret1.a == 0); // ignore other check
        }

        [TestMethod]
        public void COWCancelledDueToDeletedSnapshotTest()
        {
            Assert.IsTrue(DoCreateTable());

            TestEntity entity = new TestEntity("1", "1");

            entity.a = 0;
            entity.b = 2;
            entity.c = 3;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);
            Assert.IsTrue(res.Result == entity);
            Assert.IsTrue(res.Etag.Equals(entity.ETag));
            Assert.IsTrue(entity.ETag.Contains(","));

            var ssid0 = DoCreateSnapshot();
            Assert.IsTrue(ssid0 == 0);

            var ssid1 = DoCreateSnapshot();
            Assert.IsTrue(ssid1 == 1);

            DoDeleteSnapshot(ssid1);
            DoDeleteSnapshot(ssid0);


            var entity2 = new TestEntity("1", "1");
            entity2.a = 2;
            entity2.b = 3;
            entity2.c = 3;
            entity2.d = "hello world!";
            entity2.e = true;
            entity2.f = 323.45;

            var res3 = DoIOR(entity2);      // plan to cow old data to 0, but cancelled since there are no valid snapshot from 0 to Curr -1 
            Assert.IsTrue(res3 != null);
            Assert.IsTrue(res3.HttpStatusCode == (int)HttpStatusCode.NoContent);


            var ssid2 = DoCreateSnapshot();
            Assert.IsTrue(ssid2 == 2);

            var retrieved1 = DoRetrieveSnapshot(ssid2, "1", "1");
            Assert.IsTrue(retrieved1 != null && retrieved1.HttpStatusCode == (int)HttpStatusCode.OK && retrieved1.Result != null);
            var ret1 = (TestEntity)retrieved1.Result;
            Assert.IsTrue(ret1.ETag == entity2.ETag && ret1.a == 2); // ignore other check
        }

        [TestMethod]
        public void ReadDeleteSnapshotTest()
        {
            Assert.IsTrue(DoCreateTable());

            TestEntity entity = new TestEntity("1", "1");

            entity.a = 0;
            entity.b = 2;
            entity.c = 3;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);
            Assert.IsTrue(res.Result == entity);
            Assert.IsTrue(res.Etag.Equals(entity.ETag));
            Assert.IsTrue(entity.ETag.Contains(","));


            var ssid0 = DoCreateSnapshot();
            Assert.IsTrue(ssid0 == 0);

            var ssid1 = DoCreateSnapshot();
            Assert.IsTrue(ssid1 == 1);

            DoDeleteSnapshot(ssid0);

            try
            {
                var retrieved1 = DoRetrieveSnapshot(ssid0, "1", "1");
                Assert.IsTrue(false);
            }
            catch (StorageException e)
            {
                Assert.IsTrue(e.RequestInformation.HttpStatusCode == (int)HttpStatusCode.BadRequest);
            }

            var retrieved2 = DoRetrieveSnapshot(ssid1, "1", "1");
            Assert.IsTrue(retrieved2 != null && retrieved2.HttpStatusCode == (int)HttpStatusCode.OK);
            var ret2 = (TestEntity)retrieved2.Result;
            Assert.IsTrue(ret2.ETag.Equals(entity.ETag) && ret2.a == 0);
        }

        [TestMethod]
        public void ReadSnapshotJumpWithDeleteSnapshotTest()
        {
            Assert.IsTrue(DoCreateTable());

            var ssid0 = DoCreateSnapshot();
            Assert.IsTrue(ssid0 == 0);

            var ssid1 = DoCreateSnapshot();
            Assert.IsTrue(ssid1 == 1);

            DoDeleteSnapshot(ssid0);

            TestEntity entity = new TestEntity("1", "1");

            entity.a = 2;
            entity.b = 2;
            entity.c = 3;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);
            Assert.IsTrue(res.Result == entity);
            Assert.IsTrue(res.Etag.Equals(entity.ETag));
            Assert.IsTrue(entity.ETag.Contains(","));

            var ssid2 = DoCreateSnapshot();
            Assert.IsTrue(ssid2 == 2);

            var ssid3 = DoCreateSnapshot();
            Assert.IsTrue(ssid3 == 3);

            DoDeleteSnapshot(ssid3);

            TestEntity dEntity = new TestEntity("1", "1");
            dEntity.ETag = entity.ETag;
            var res2 = DoDelete(dEntity);
            Assert.IsTrue(res2 != null && res2.HttpStatusCode == (int)HttpStatusCode.NoContent);

            var ssid4 = DoCreateSnapshot();
            Assert.IsTrue(ssid4 == 4);

            var ssid5 = DoCreateSnapshot();
            Assert.IsTrue(ssid5 == 5);

            DoDeleteSnapshot(ssid4);

            var entity2 = new TestEntity("1", "1");
            entity2.a = 6;
            entity2.b = 3;
            entity2.c = 3;
            entity2.d = "hello world!";
            entity2.e = true;
            entity2.f = 323.45;

            var res3 = DoIOR(entity2);
            Assert.IsTrue(res3 != null);
            Assert.IsTrue(res3.HttpStatusCode == (int)HttpStatusCode.NoContent);

            var ssid6 = DoCreateSnapshot();
            Assert.IsTrue(ssid6 == 6);

            var ssid7 = DoCreateSnapshot();
            Assert.IsTrue(ssid7 == 7);
            
            var ssid8 = DoCreateSnapshot();
            Assert.IsTrue(ssid8 == 8);

            DoDeleteSnapshot(ssid7);

            var entity3 = new TestEntity("1", "1");
            entity3.a = 9;
            entity3.b = 3;
            entity3.c = 3;
            entity3.d = "hello world!";
            entity3.e = true;
            entity3.f = 323.45;
            entity3.ETag = entity2.ETag;

            var res4 = DoReplace(entity3);
            Assert.IsTrue(res4 != null);
            Assert.IsTrue(res4.HttpStatusCode == (int)HttpStatusCode.NoContent);


            var retrieved1 = DoRetrieveSnapshot(ssid5, "1", "1");
            Assert.IsTrue(retrieved1 != null && retrieved1.HttpStatusCode == (int)HttpStatusCode.NotFound);

            var retrieved2 = DoRetrieveSnapshot(ssid2, "1", "1");
            Assert.IsTrue(retrieved2 != null && retrieved2.HttpStatusCode == (int)HttpStatusCode.OK && retrieved2.Result != null);
            var ret2 = (TestEntity)retrieved2.Result;
            Assert.IsTrue(ret2.ETag == entity.ETag && ret2.a == 2); // ignore other check

            var retrieved3 = DoRetrieveSnapshot(ssid8, "1", "1");
            Assert.IsTrue(retrieved3 != null && retrieved3.HttpStatusCode == (int)HttpStatusCode.OK && retrieved3.Result != null);
            var ret3 = (TestEntity)retrieved3.Result;
            Assert.IsTrue(ret3.ETag == entity2.ETag && ret3.a == 6); // ignore other check

            var retrieved4 = DoRetrieveSnapshot(ssid6, "1", "1");
            Assert.IsTrue(retrieved4 != null && retrieved4.HttpStatusCode == (int)HttpStatusCode.OK && retrieved4.Result != null);
            var ret4 = (TestEntity)retrieved4.Result;
            Assert.IsTrue(ret4.ETag == entity2.ETag && ret4.a == 6); // ignore other check

            var retrieved5 = DoRetrieveSnapshot(ssid1, "1", "1");
            Assert.IsTrue(retrieved5 != null && retrieved5.HttpStatusCode == (int)HttpStatusCode.NotFound);
        }

        [TestMethod]
        public void DeleteSnapshotGCTest()
        {
            Assert.IsTrue(DoCreateTable());

            var ssid0 = DoCreateSnapshot();
            Assert.IsTrue(ssid0 == 0);

            var ssid1 = DoCreateSnapshot();
            Assert.IsTrue(ssid1 == 1);

            TestEntity entity = new TestEntity("1", "1");

            entity.a = 2;
            entity.b = 2;
            entity.c = 3;
            entity.d = "hello world!";
            entity.e = true;
            entity.f = 123.45;

            var res = DoInsert(entity);
            Assert.IsTrue(res != null);
            Assert.IsTrue(res.HttpStatusCode == (int)HttpStatusCode.Created);
            Assert.IsTrue(res.Result == entity);
            Assert.IsTrue(res.Etag.Equals(entity.ETag));
            Assert.IsTrue(entity.ETag.Contains(","));

            var ssid2 = DoCreateSnapshot();
            Assert.IsTrue(ssid2 == 2);

            var ssid3 = DoCreateSnapshot();
            Assert.IsTrue(ssid3 == 3);

            TestEntity dEntity = new TestEntity("1", "1");
            dEntity.ETag = entity.ETag;
            var res2 = DoDelete(dEntity);
            Assert.IsTrue(res2 != null && res2.HttpStatusCode == (int)HttpStatusCode.NoContent);
            
            var ssid4 = DoCreateSnapshot();
            Assert.IsTrue(ssid4 == 4);

            var ssid5 = DoCreateSnapshot();
            Assert.IsTrue(ssid5 == 5);

            var retrieved1 = DoRetrieveSnapshot(ssid5, "1", "1");   // will push the deleted row to table ss_5
            Assert.IsTrue(retrieved1 != null && retrieved1.HttpStatusCode == (int)HttpStatusCode.NotFound);

            DoDeleteSnapshot(ssid4);

            DoGC(); // should not delete the row

            var retrieved2 = DoRetrieveSnapshot(ssid2, "1", "1");
            Assert.IsTrue(retrieved2 != null && retrieved2.HttpStatusCode == (int)HttpStatusCode.OK && retrieved2.Result != null);
            var ret2 = (TestEntity)retrieved2.Result;
            Assert.IsTrue(ret2.ETag == entity.ETag && ret2.a == 2); // ignore other check

            var retrieved3 = DoRetrieveSnapshot(ssid1, "1", "1");    // this will create a deleted dummy row in ss_1
            Assert.IsTrue(retrieved3 != null && retrieved3.HttpStatusCode == (int)HttpStatusCode.NotFound);

            DoDeleteSnapshot(ssid2);

            DoGC(); // should not delete the row

            var retrieved4 = DoRetrieveSnapshot(ssid3, "1", "1");
            Assert.IsTrue(retrieved4 != null && retrieved4.HttpStatusCode == (int)HttpStatusCode.OK && retrieved4.Result != null);
            var ret4 = (TestEntity)retrieved4.Result;
            Assert.IsTrue(ret4.ETag == entity.ETag && ret4.a == 2); // ignore other check

            DoDeleteSnapshot(ssid3);    // now, the row is marked as "deleted" in all tables

            var headTab = ts.GetChainTable(config.GetHeadTableId());
            var ss1 = ts.GetChainTable(config.GetSnapshotTableId(ssid1));

            var acc = headTab.Execute(TableOperation.Retrieve<DynamicTableEntity>("1", "1"));
            Assert.IsTrue(acc != null && acc.HttpStatusCode == (int)HttpStatusCode.OK);

            acc = ss1.Execute(TableOperation.Retrieve<DynamicTableEntity>("1", "1"));
            Assert.IsTrue(acc != null && acc.HttpStatusCode == (int)HttpStatusCode.OK);

            DoGC(); // should delete the row

            acc = headTab.Execute(TableOperation.Retrieve<DynamicTableEntity>("1", "1"));
            Assert.IsTrue(acc != null && acc.HttpStatusCode == (int)HttpStatusCode.NotFound);

            acc = ss1.Execute(TableOperation.Retrieve<DynamicTableEntity>("1", "1"));
            Assert.IsTrue(acc != null && acc.HttpStatusCode == (int)HttpStatusCode.NotFound);
        }

        [TestMethod]
        public void BatchSimpleOpTest()
        {
            Assert.IsTrue(DoCreateTable());

            // test insert
            TestEntity[] entities = new TestEntity[10];
            for (int i = 0; i < 10; ++i)
            {
                entities[i] = new TestEntity("1", i.ToString());
                entities[i].a = i;
                entities[i].d = "hello" + i.ToString();
                entities[i].ETag = "useless";
                entities[i].Timestamp = DateTimeOffset.MinValue;
            }

            var batch = new TableBatchOperation();
            for (int i = 0; i < 10; ++i)
            {
                batch.Add(TableOperation.Insert(entities[i]));
            }

            var resList1 = DoExecuteBatch(batch);

            Assert.IsTrue(resList1 != null && resList1.Count == 10);
            for (int i = 0; i < 10; ++i)
            {
                Assert.IsTrue(resList1[i] != null && resList1[i].HttpStatusCode == (int)HttpStatusCode.Created);
                Assert.IsTrue(resList1[i].Etag == entities[i].ETag);
                Assert.IsTrue(entities[i].ETag.Contains(","));
                //Assert.IsTrue(entities[i].Timestamp != DateTimeOffset.MinValue);
            }

            for (int i = 0; i < 10; ++i)
            {
                var ret = DoRetrieve("1", i.ToString());
                Assert.IsTrue(ret != null && ret.HttpStatusCode == (int)HttpStatusCode.OK && ret.Result != null);
                var ent = (TestEntity)ret.Result;
                Assert.IsTrue(entities[i].ETag.Equals(ent.ETag));
                //Assert.IsTrue(entities[i].Timestamp.Equals(ent.Timestamp));
            }

            // test replace / merge / ior / iom
            batch = new TableBatchOperation();
            for (int i = 0; i < 10; ++i)
            {
                entities[i].a = i * 2;
                entities[i].b = i;

                if (i % 4 == 0)
                    batch.Add(TableOperation.Replace(entities[i]));
                else if (i % 4 == 1)
                    batch.Add(TableOperation.Merge(entities[i]));
                else if (i % 4 == 2)
                    batch.Add(TableOperation.InsertOrReplace(entities[i]));
                else if (i % 4 == 3)
                    batch.Add(TableOperation.InsertOrMerge(entities[i]));
            }

            var resList2 = DoExecuteBatch(batch);
            Assert.IsTrue(resList2 != null && resList2.Count == 10);
            for (int i = 0; i < 10; ++i)
            {
                Assert.IsTrue(resList2[i] != null && resList2[i].HttpStatusCode == (int)HttpStatusCode.NoContent);
                Assert.IsTrue(resList2[i].Etag == entities[i].ETag);
                Assert.IsTrue(entities[i].ETag.Contains(","));
            }

            for (int i = 0; i < 10; ++i)
            {
                var ret = DoRetrieve("1", i.ToString());
                Assert.IsTrue(ret != null && ret.HttpStatusCode == (int)HttpStatusCode.OK && ret.Result != null);
                var ent = (TestEntity)ret.Result;
                Assert.IsTrue(ent.a == i * 2);
                Assert.IsTrue(ent.ETag.Equals(entities[i].ETag));
            }

            // test delete
            batch = new TableBatchOperation();
            for (int i = 0; i < 10; ++i)
                batch.Add(TableOperation.Delete(entities[i]));

            var resList3 = DoExecuteBatch(batch);
            Assert.IsTrue(resList3 != null && resList3.Count == 10);
            for (int i = 0; i < 10; ++i)
                Assert.IsTrue(resList3[i] != null && resList3[i].HttpStatusCode == (int)HttpStatusCode.NoContent);

            for (int i = 0; i < 10; ++i)
            {
                var ret = DoRetrieve("1", i.ToString());
                Assert.IsTrue(ret != null && ret.HttpStatusCode == (int)HttpStatusCode.NotFound);
            }

            // test insert back
            batch = new TableBatchOperation();
            for (int i = 0; i < 10; ++i)
            {
                entities[i].a = i * 3;
                entities[i].d = "wow!" + i.ToString();
                entities[i].ETag = "";

                if (i % 3 == 0)
                    batch.Add(TableOperation.Insert(entities[i]));
                else if (i % 3 == 1)
                    batch.Add(TableOperation.InsertOrReplace(entities[i]));
                else if (i % 3 == 2)
                    batch.Add(TableOperation.InsertOrMerge(entities[i]));
            }

            var resList4 = DoExecuteBatch(batch);
            Assert.IsTrue(resList4 != null && resList4.Count == 10);
            for (int i = 0; i < 10; ++i)
            {
                if (i % 3 == 0)
                    Assert.IsTrue(resList4[i] != null && resList4[i].HttpStatusCode == (int)HttpStatusCode.Created);
                else
                    Assert.IsTrue(resList4[i] != null && resList4[i].HttpStatusCode == (int)HttpStatusCode.NoContent);
                Assert.IsTrue(entities[i].ETag.Contains(","));
            }

            for (int i = 0; i < 10; ++i)
            {
                var ret = DoRetrieve("1", i.ToString());
                Assert.IsTrue(ret != null && ret.HttpStatusCode == (int)HttpStatusCode.OK && ret.Result != null);
                var ent = (TestEntity)ret.Result;
                Assert.IsTrue(ent.a == i * 3);
                Assert.IsTrue(ent.ETag.Equals(entities[i].ETag));
            }

            // test retrieve
            batch = new TableBatchOperation();
            batch.Add(TableOperation.Retrieve<TestEntity>("1", "1"));
            var resList5 = DoExecuteBatch(batch);
            Assert.IsTrue(resList5 != null && resList5.Count == 1);
            Assert.IsTrue(resList5[0] != null && resList5[0].HttpStatusCode == (int)HttpStatusCode.OK && resList5[0].Result != null);
            Assert.IsTrue(resList5[0].Etag.Equals(entities[1].ETag));
        }

        [TestMethod]
        public void BatchErrorReturnTest()
        {
            Assert.IsTrue(DoCreateTable());

            // insert
            TestEntity[] entities = new TestEntity[10];
            for (int i = 0; i < 10; ++i)
            {
                entities[i] = new TestEntity("1", i.ToString());
                entities[i].a = i;
                entities[i].d = "hello" + i.ToString();
            }

            var batch = new TableBatchOperation();
            for (int i = 0; i < 5; ++i)
            {
                batch.Add(TableOperation.Insert(entities[i]));
            }

            var resList = DoExecuteBatch(batch);

            Assert.IsTrue(resList != null && resList.Count == 5);
            for (int i = 0; i < 5; ++i)
            {
                Assert.IsTrue(resList[i] != null && resList[i].HttpStatusCode == (int)HttpStatusCode.Created);
                Assert.IsTrue(resList[i].Etag == entities[i].ETag);
                Assert.IsTrue(entities[i].ETag.Contains(","));
            }


            // Case 1: insert over existing entities
            batch = new TableBatchOperation();
            for (int i = 5; i < 10; ++i)
                batch.Add(TableOperation.Insert(entities[i]));
            
            entities[0].a = 555;
            batch.Add(TableOperation.Insert(entities[0]));

            try
            {
                resList = DoExecuteBatch(batch);
                Assert.IsTrue(false);
            }
            catch (StorageException e)
            {
                Assert.IsTrue(e is ChainTableBatchException);
                var ee = (ChainTableBatchException)e;
                Assert.IsTrue(ee.FailedOpIndex == 5 && ee.RequestInformation.HttpStatusCode == (int)HttpStatusCode.Conflict);
            }

            entities[0].a = 0;

            // should rollback any change
            for (int i = 0; i < 10; ++i)
            {
                var res = DoRetrieve("1", i.ToString());
                if (i < 5)
                {
                    Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.OK && res.Result != null);
                    var raw = (TestEntity)res.Result;
                    Assert.IsTrue(raw.a == i);
                }
                else
                    Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.NotFound);
            }


            // Case 2: replace / merge / delete with wrong etag
            for (int i = 0; i < 5; ++i)
                entities[i].a = 2 * i;

            for (int t = 1; t < 4; ++t)
            {
                // t = 1, test merge; t = 2, test delete; t = 3, test replace
                var etagBackup = entities[t].ETag;
                entities[t].ETag = "wrong etag";

                batch = new TableBatchOperation();
                for (int i = 0; i < 5; ++i)
                {
                    if (i % 3 == 0)
                        batch.Add(TableOperation.Replace(entities[i]));
                    else if (i % 3 == 1)
                        batch.Add(TableOperation.Merge(entities[i]));
                    else
                        batch.Add(TableOperation.Delete(entities[i]));
                }

                try
                {
                    resList = DoExecuteBatch(batch);
                    Assert.IsTrue(false);
                }
                catch (StorageException e)
                {
                    Assert.IsTrue(e is ChainTableBatchException);
                    var ee = (ChainTableBatchException)e;
                    Assert.IsTrue(ee.FailedOpIndex == t && ee.RequestInformation.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed);
                }

                // should rollback any change
                for (int i = 0; i < 5; ++i)
                {
                    var res = DoRetrieve("1", i.ToString());
                    Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.OK && res.Result != null);
                    var raw = (TestEntity)res.Result;
                    Assert.IsTrue(raw.a == i);
                }

                entities[t].ETag = etagBackup;
            }


            // Case 3: replace / merge / delete with nonexisting row
            for (int i = 0; i < 10; ++i)
                entities[i].a = 3 * i;

            for (int i = 5; i < 10; ++i)
                entities[i].ETag = "useless etag";

            for (int t = 0; t < 3; ++t)
            {
                // t = 0, test replace; t = 1, test merge; t = 2, test delete;

                batch = new TableBatchOperation();
                for (int i = 0; i < 5; ++i)
                {
                    if (i % 3 == 0)
                        batch.Add(TableOperation.Replace(entities[i]));
                    else if (i % 3 == 1)
                        batch.Add(TableOperation.Merge(entities[i]));
                    else
                        batch.Add(TableOperation.Delete(entities[i]));
                }

                if (t == 0)
                    batch.Add(TableOperation.Replace(entities[6]));
                else if (t == 1)
                    batch.Add(TableOperation.Merge(entities[7]));
                else
                    batch.Add(TableOperation.Delete(entities[8]));

                try
                {
                    resList = DoExecuteBatch(batch);
                    Assert.IsTrue(false);
                }
                catch (StorageException e)
                {
                    Assert.IsTrue(e is ChainTableBatchException);
                    var ee = (ChainTableBatchException)e;
                    Assert.IsTrue(ee.FailedOpIndex == 5 && ee.RequestInformation.HttpStatusCode == (int)HttpStatusCode.NotFound);
                }

                // should rollback any change
                for (int i = 0; i < 10; ++i)
                {
                    var res = DoRetrieve("1", i.ToString());
                    if (i < 5)
                    {
                        Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.OK && res.Result != null);
                        var raw = (TestEntity)res.Result;
                        Assert.IsTrue(raw.a == i);
                    }
                    else
                        Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.NotFound);
                }
            }


            // Case 4: replace / merge / delete with deleted row
            for (int i = 0; i < 10; ++i)
                entities[i].a = 4 * i;

            for (int i = 5; i < 10; ++i)
            {
                var res = DoInsert(entities[i]);
                Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.Created);

                res = DoDelete(entities[i]);
                Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.NoContent);
            }

            for (int t = 0; t < 3; ++t)
            {
                // t = 0, test replace; t = 1, test merge; t = 2, test delete;

                batch = new TableBatchOperation();
                for (int i = 0; i < 5; ++i)
                {
                    if (i % 3 == 0)
                        batch.Add(TableOperation.Replace(entities[i]));
                    else if (i % 3 == 1)
                        batch.Add(TableOperation.Merge(entities[i]));
                    else
                        batch.Add(TableOperation.Delete(entities[i]));
                }

                if (t == 0)
                    batch.Add(TableOperation.Replace(entities[6]));
                else if (t == 1)
                    batch.Add(TableOperation.Merge(entities[7]));
                else
                    batch.Add(TableOperation.Delete(entities[8]));

                try
                {
                    resList = DoExecuteBatch(batch);
                    Assert.IsTrue(false);
                }
                catch (StorageException e)
                {
                    Assert.IsTrue(e is ChainTableBatchException);
                    var ee = (ChainTableBatchException)e;
                    Assert.IsTrue(ee.FailedOpIndex == 5 && ee.RequestInformation.HttpStatusCode == (int)HttpStatusCode.NotFound);
                }

                // should rollback any change
                for (int i = 0; i < 10; ++i)
                {
                    var res = DoRetrieve("1", i.ToString());
                    if (i < 5)
                    {
                        Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.OK && res.Result != null);
                        var raw = (TestEntity)res.Result;
                        Assert.IsTrue(raw.a == i);
                    }
                    else
                        Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.NotFound);
                }
            }

            // Case 5: retrieve in non-trivial batch, this is prohibitted by batch.Add()
        }

        [TestMethod]
        public void BatchCOWTest()
        {
            Assert.IsTrue(DoCreateTable());

            // insert
            TestEntity[] entities = new TestEntity[10];
            for (int i = 0; i < 10; ++i)
            {
                entities[i] = new TestEntity("1", i.ToString());
                entities[i].a = i;
                entities[i].d = "hello" + i.ToString();
            }

            var batch = new TableBatchOperation();
            for (int i = 0; i < 10; ++i)
            {
                batch.Add(TableOperation.Insert(entities[i]));
            }

            var resList1 = DoExecuteBatch(batch);

            Assert.IsTrue(resList1 != null && resList1.Count == 10);
            for (int i = 0; i < 10; ++i)
            {
                Assert.IsTrue(resList1[i] != null && resList1[i].HttpStatusCode == (int)HttpStatusCode.Created);
                Assert.IsTrue(resList1[i].Etag == entities[i].ETag);
                Assert.IsTrue(entities[i].ETag.Contains(","));
            }

            var ssid0 = DoCreateSnapshot();
            
            // test replace / merge / ior / iom
            batch = new TableBatchOperation();
            for (int i = 0; i < 5; ++i)
            {
                entities[i].a = i * 2;
                entities[i].b = i;

                if (i % 4 == 0)
                    batch.Add(TableOperation.Replace(entities[i]));
                else if (i % 4 == 1)
                    batch.Add(TableOperation.Merge(entities[i]));
                else if (i % 4 == 2)
                    batch.Add(TableOperation.InsertOrReplace(entities[i]));
                else if (i % 4 == 3)
                    batch.Add(TableOperation.InsertOrMerge(entities[i]));
            }

            var resList2 = DoExecuteBatch(batch);
            Assert.IsTrue(resList2 != null && resList2.Count == 5);
            for (int i = 0; i < 5; ++i)
            {
                Assert.IsTrue(resList2[i] != null && resList2[i].HttpStatusCode == (int)HttpStatusCode.NoContent);
                Assert.IsTrue(resList2[i].Etag == entities[i].ETag);
                Assert.IsTrue(entities[i].ETag.Contains(","));
            }

            var ssid1 = DoCreateSnapshot();

            // test delete
            batch = new TableBatchOperation();
            for (int i = 5; i < 10; ++i)
                batch.Add(TableOperation.Delete(entities[i]));

            var resList3 = DoExecuteBatch(batch);
            Assert.IsTrue(resList3 != null && resList3.Count == 5);
            for (int i = 0; i < 5; ++i)
                Assert.IsTrue(resList3[i] != null && resList3[i].HttpStatusCode == (int)HttpStatusCode.NoContent);

            for (int i = 5; i < 10; ++i)
            {
                var ret = DoRetrieve("1", i.ToString());
                Assert.IsTrue(ret != null && ret.HttpStatusCode == (int)HttpStatusCode.NotFound);
            }

            var ssid2 = DoCreateSnapshot();
            
            // test insert back
            batch = new TableBatchOperation();
            for (int i = 5; i < 10; ++i)
            {
                entities[i].a = i * 3;
                entities[i].d = "wow!" + i.ToString();
                entities[i].ETag = "";

                if (i % 3 == 0)
                    batch.Add(TableOperation.Insert(entities[i]));
                else if (i % 3 == 1)
                    batch.Add(TableOperation.InsertOrReplace(entities[i]));
                else if (i % 3 == 2)
                    batch.Add(TableOperation.InsertOrMerge(entities[i]));
            }

            var resList4 = DoExecuteBatch(batch);
            Assert.IsTrue(resList4 != null && resList4.Count == 5);
            for (int i = 0; i < 5; ++i)
            {
                if ((5 + i) % 3 == 0)
                    Assert.IsTrue(resList4[i] != null && resList4[i].HttpStatusCode == (int)HttpStatusCode.Created);
                else
                    Assert.IsTrue(resList4[i] != null && resList4[i].HttpStatusCode == (int)HttpStatusCode.NoContent);
                Assert.IsTrue(entities[i].ETag.Contains(","));
            }

            var ssid3 = DoCreateSnapshot();

            // check
            for (int i = 0; i < 10; ++i)
            {
                var res = DoRetrieveSnapshot(ssid0, "1", i.ToString());
                Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.OK && res.Result != null);
                var raw = (TestEntity)res.Result;
                Assert.IsTrue(raw.a == i);
            }

            for (int i = 0; i < 10; ++i)
            {
                var res = DoRetrieveSnapshot(ssid1, "1", i.ToString());
                Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.OK && res.Result != null);
                var raw = (TestEntity)res.Result;
                if (i < 5)
                    Assert.IsTrue(raw.a == 2 * i);
                else
                    Assert.IsTrue(raw.a == i);
            }

            for (int i = 0; i < 10; ++i)
            {
                var res = DoRetrieveSnapshot(ssid2, "1", i.ToString());
                if (i < 5)
                {
                    Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.OK && res.Result != null);
                    var raw = (TestEntity)res.Result;
                    Assert.IsTrue(raw.a == 2 * i);
                }
                else
                    Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.NotFound);
            }

            for (int i = 0; i < 10; ++i)
            {
                var res = DoRetrieveSnapshot(ssid3, "1", i.ToString());
                Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.OK && res.Result != null);
                var raw = (TestEntity)res.Result;
                if (i < 5)
                {
                    Assert.IsTrue(raw.a == 2 * i);
                }
                else
                    Assert.IsTrue(raw.a == 3 * i);
            }
        }

        [TestMethod]
        public void BatchSingleOpTest()
        {
            Assert.IsTrue(DoCreateTable());

            // test retrieve
            var batch = new TableBatchOperation();
            batch.Add(TableOperation.Retrieve<TestEntity>("1", "1"));
            var retList = DoExecuteBatch(batch);
            Assert.IsTrue(retList != null && retList.Count == 1);
            Assert.IsTrue(retList[0] != null && retList[0].HttpStatusCode == (int)HttpStatusCode.NotFound);

            // insert
            var entity = new TestEntity("1", "1");
            entity.a = 1;
            entity.d = "Hello 1";

            batch = new TableBatchOperation();
            batch.Add(TableOperation.Insert(entity));

            var resList1 = DoExecuteBatch(batch);

            Assert.IsTrue(resList1 != null && resList1.Count == 1);
            Assert.IsTrue(resList1[0] != null && resList1[0].HttpStatusCode == (int)HttpStatusCode.Created);
            Assert.IsTrue(resList1[0].Etag == entity.ETag);
            Assert.IsTrue(entity.ETag.Contains(","));

            var ssid0 = DoCreateSnapshot();

            // test replace
            batch = new TableBatchOperation();
            entity.a = 2;
            batch.Add(TableOperation.Replace(entity));

            var resList2 = DoExecuteBatch(batch);
            Assert.IsTrue(resList2 != null && resList2.Count == 1);
            Assert.IsTrue(resList2[0] != null && resList2[0].HttpStatusCode == (int)HttpStatusCode.NoContent);

            // test retrieve
            batch = new TableBatchOperation();
            batch.Add(TableOperation.Retrieve<TestEntity>(entity.PartitionKey, entity.RowKey));
            retList = DoExecuteBatch(batch);
            Assert.IsTrue(retList != null && retList.Count == 1);
            Assert.IsTrue(retList[0] != null && retList[0].HttpStatusCode == (int)HttpStatusCode.OK && retList[0].Result != null);
            var tup = (TestEntity)retList[0].Result;
            Assert.IsTrue(tup.ETag == entity.ETag);
            Assert.IsTrue(tup.a == 2);

            var ssid1 = DoCreateSnapshot();

            // test delete
            batch = new TableBatchOperation();
            batch.Add(TableOperation.Delete(entity));

            var resList3 = DoExecuteBatch(batch);
            Assert.IsTrue(resList3 != null && resList3.Count == 1);
            Assert.IsTrue(resList3[0] != null && resList3[0].HttpStatusCode == (int)HttpStatusCode.NoContent);

            // test retrieve
            batch = new TableBatchOperation();
            batch.Add(TableOperation.Retrieve<TestEntity>(entity.PartitionKey, entity.RowKey));
            retList = DoExecuteBatch(batch);
            Assert.IsTrue(retList != null && retList.Count == 1);
            Assert.IsTrue(retList[0] != null && retList[0].HttpStatusCode == (int)HttpStatusCode.NotFound);


            var ssid2 = DoCreateSnapshot();

            // test insert back
            batch = new TableBatchOperation();
            entity.a = 3;
            batch.Add(TableOperation.Insert(entity));

            var resList4 = DoExecuteBatch(batch);
            Assert.IsTrue(resList4 != null && resList4.Count == 1);
            Assert.IsTrue(resList4[0] != null && resList4[0].HttpStatusCode == (int)HttpStatusCode.Created);

            var ssid3 = DoCreateSnapshot();

            // check
            var res = DoRetrieveSnapshot(ssid0, "1", "1");
            Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.OK && res.Result != null);
            var raw = (TestEntity)res.Result;
            Assert.IsTrue(raw.a == 1);

            res = DoRetrieveSnapshot(ssid1, "1", "1");
            Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.OK && res.Result != null);
            raw = (TestEntity)res.Result;
            Assert.IsTrue(raw.a == 2);

            res = DoRetrieveSnapshot(ssid2, "1", "1");
            Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.NotFound);

            res = DoRetrieveSnapshot(ssid3, "1", "1");
            Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.OK && res.Result != null);
            raw = (TestEntity)res.Result;
            Assert.IsTrue(raw.a == 3);
        }

        [TestMethod]
        public void BatchCOWOverDeletedSnapshotsTest()
        {
            Assert.IsTrue(DoCreateTable());

            // insert
            TestEntity[] entities = new TestEntity[10];
            for (int i = 0; i < 10; ++i)
            {
                entities[i] = new TestEntity("1", i.ToString());
                entities[i].a = i;
                entities[i].d = "hello" + i.ToString();
            }

            var batch = new TableBatchOperation();
            for (int i = 0; i < 10; ++i)
            {
                if (i % 3 == 0)
                    batch.Add(TableOperation.Insert(entities[i]));
                else if (i % 3 == 1)
                    batch.Add(TableOperation.InsertOrReplace(entities[i]));
                else
                    batch.Add(TableOperation.InsertOrMerge(entities[i]));
            }

            var resList1 = DoExecuteBatch(batch);

            Assert.IsTrue(resList1 != null && resList1.Count == 10);
            for (int i = 0; i < 10; ++i)
            {
                if (i % 3 == 0)
                    Assert.IsTrue(resList1[i] != null && resList1[i].HttpStatusCode == (int)HttpStatusCode.Created);
                else
                    Assert.IsTrue(resList1[i] != null && resList1[i].HttpStatusCode == (int)HttpStatusCode.NoContent);

                Assert.IsTrue(resList1[i].Etag == entities[i].ETag);
                Assert.IsTrue(entities[i].ETag.Contains(","));
            }

            var ssid0 = DoCreateSnapshot();
            var ssid1 = DoCreateSnapshot();

            DoDeleteSnapshot(ssid0);

            // test replace / merge / ior / iom, cow to ssid1
            batch = new TableBatchOperation();
            for (int i = 0; i < 5; ++i)
            {
                entities[i].a = i * 2;
                entities[i].b = i;

                if (i % 4 == 0)
                    batch.Add(TableOperation.Replace(entities[i]));
                else if (i % 4 == 1)
                    batch.Add(TableOperation.Merge(entities[i]));
                else if (i % 4 == 2)
                    batch.Add(TableOperation.InsertOrReplace(entities[i]));
                else if (i % 4 == 3)
                    batch.Add(TableOperation.InsertOrMerge(entities[i]));
            }

            var resList2 = DoExecuteBatch(batch);
            Assert.IsTrue(resList2 != null && resList2.Count == 5);
            for (int i = 0; i < 5; ++i)
            {
                Assert.IsTrue(resList2[i] != null && resList2[i].HttpStatusCode == (int)HttpStatusCode.NoContent);
                Assert.IsTrue(resList2[i].Etag == entities[i].ETag);
                Assert.IsTrue(entities[i].ETag.Contains(","));
            }

            var ssid2 = DoCreateSnapshot();

            // test delete, 0-4 cow to ssid2, 5-9 skip ssid0 and cow to ssid1
            batch = new TableBatchOperation();
            for (int i = 0; i < 10; ++i)
                batch.Add(TableOperation.Delete(entities[i]));

            var resList3 = DoExecuteBatch(batch);
            Assert.IsTrue(resList3 != null && resList3.Count == 10);
            for (int i = 0; i < 10; ++i)
                Assert.IsTrue(resList3[i] != null && resList3[i].HttpStatusCode == (int)HttpStatusCode.NoContent);

            for (int i = 0; i < 10; ++i)
            {
                var ret = DoRetrieve("1", i.ToString());
                Assert.IsTrue(ret != null && ret.HttpStatusCode == (int)HttpStatusCode.NotFound);
            }

            var ssid3 = DoCreateSnapshot();

            // test insert back, cow to ssid3
            batch = new TableBatchOperation();
            for (int i = 0; i < 10; ++i)
            {
                entities[i].a = i * 3;
                entities[i].d = "wow!" + i.ToString();
                entities[i].ETag = "";

                if (i % 3 == 0)
                    batch.Add(TableOperation.Insert(entities[i]));
                else if (i % 3 == 1)
                    batch.Add(TableOperation.InsertOrReplace(entities[i]));
                else if (i % 3 == 2)
                    batch.Add(TableOperation.InsertOrMerge(entities[i]));
            }

            var resList4 = DoExecuteBatch(batch);
            Assert.IsTrue(resList4 != null && resList4.Count == 10);
            for (int i = 0; i < 10; ++i)
            {
                if ((i) % 3 == 0)
                    Assert.IsTrue(resList4[i] != null && resList4[i].HttpStatusCode == (int)HttpStatusCode.Created);
                else
                    Assert.IsTrue(resList4[i] != null && resList4[i].HttpStatusCode == (int)HttpStatusCode.NoContent);
                Assert.IsTrue(entities[i].ETag.Contains(","));
            }

            var ssid4 = DoCreateSnapshot();

            // replace / merge / ior / iom, cow to ssid4
            batch = new TableBatchOperation();
            for (int i = 0; i < 5; ++i)
            {
                entities[i].a = i * 4;
                entities[i].b = i;

                if (i % 4 == 0)
                    batch.Add(TableOperation.Replace(entities[i]));
                else if (i % 4 == 1)
                    batch.Add(TableOperation.Merge(entities[i]));
                else if (i % 4 == 2)
                    batch.Add(TableOperation.InsertOrReplace(entities[i]));
                else if (i % 4 == 3)
                    batch.Add(TableOperation.InsertOrMerge(entities[i]));
            }

            var resList5 = DoExecuteBatch(batch);
            Assert.IsTrue(resList5 != null && resList5.Count == 5);
            for (int i = 0; i < 5; ++i)
            {
                Assert.IsTrue(resList5[i] != null && resList5[i].HttpStatusCode == (int)HttpStatusCode.NoContent);
                Assert.IsTrue(resList5[i].Etag == entities[i].ETag);
                Assert.IsTrue(entities[i].ETag.Contains(","));
            }

            var ssid5 = DoCreateSnapshot();
            DoDeleteSnapshot(ssid5);

            // replace / merge / ior / iom, 0-4 do not cow since there is no valid ss at 5 (head = 6), 5-9 cow to ssid4
            batch = new TableBatchOperation();
            for (int i = 0; i < 10; ++i)
            {
                entities[i].a = i * 5;
                entities[i].b = i;

                if (i % 4 == 0)
                    batch.Add(TableOperation.Replace(entities[i]));
                else if (i % 4 == 1)
                    batch.Add(TableOperation.Merge(entities[i]));
                else if (i % 4 == 2)
                    batch.Add(TableOperation.InsertOrReplace(entities[i]));
                else if (i % 4 == 3)
                    batch.Add(TableOperation.InsertOrMerge(entities[i]));
            }

            var resList6 = DoExecuteBatch(batch);
            Assert.IsTrue(resList6 != null && resList6.Count == 10);
            for (int i = 0; i < 10; ++i)
            {
                Assert.IsTrue(resList6[i] != null && resList6[i].HttpStatusCode == (int)HttpStatusCode.NoContent);
                Assert.IsTrue(resList6[i].Etag == entities[i].ETag);
                Assert.IsTrue(entities[i].ETag.Contains(","));
            }


            // snapshot 0: not exist
            // snapshot 1: initial state, 10 row with a = i
            // snapshot 2: 1st state, 0-4 with a = 2*i, 5-9 missing (fall to ss1, a = i)
            // snapshot 3: all deleted
            // snapshot 4: all inserted with a = 3*i
            // snapshot 5: not exist
            // head: all with a = 5*i

            var historyInfo = DoGetHistory();
            Assert.IsTrue(historyInfo != null);
            Assert.IsTrue(historyInfo.HeadSSID == 6);
            Assert.IsTrue(historyInfo.Snapshots != null);
            Assert.IsTrue(historyInfo.Snapshots.Count == 7);
            for (int i = 0; i <= 6; ++i)
            {
                Assert.IsTrue(historyInfo.GetSnapshot(i) == historyInfo.Snapshots[i]);
                Assert.IsTrue(historyInfo.Snapshots[i].SSID == i);
            }

            Assert.IsTrue(historyInfo.Snapshots[0].State == SnapshotState.Deleted);
            Assert.IsTrue(historyInfo.Snapshots[0].Parent == SnapshotInfo.NoParent);
            Assert.IsTrue(historyInfo.Snapshots[1].State == SnapshotState.Valid);
            Assert.IsTrue(historyInfo.Snapshots[1].Parent == 0);
            Assert.IsTrue(historyInfo.Snapshots[2].State == SnapshotState.Valid);
            Assert.IsTrue(historyInfo.Snapshots[2].Parent == 1);
            Assert.IsTrue(historyInfo.Snapshots[3].State == SnapshotState.Valid);
            Assert.IsTrue(historyInfo.Snapshots[3].Parent == 2);
            Assert.IsTrue(historyInfo.Snapshots[4].State == SnapshotState.Valid);
            Assert.IsTrue(historyInfo.Snapshots[4].Parent == 3);
            Assert.IsTrue(historyInfo.Snapshots[5].State == SnapshotState.Deleted);
            Assert.IsTrue(historyInfo.Snapshots[5].Parent == 4);
            Assert.IsTrue(historyInfo.Snapshots[6].State == SnapshotState.Head);
            Assert.IsTrue(historyInfo.Snapshots[6].Parent == 5);


            for (int i = 0; i < 10; ++i)
            {
                var res = DoRetrieveSnapshot(ssid1, "1", i.ToString());
                Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.OK && res.Result != null);
                var raw = (TestEntity)res.Result;
                Assert.IsTrue(raw.a == i);
            }

            for (int i = 0; i < 10; ++i)
            {
                var res = DoRetrieveSnapshot(ssid2, "1", i.ToString());
                Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.OK && res.Result != null);
                var raw = (TestEntity)res.Result;
                if (i < 5)
                    Assert.IsTrue(raw.a == 2 * i);
                else
                    Assert.IsTrue(raw.a == i);
            }

            for (int i = 0; i < 10; ++i)
            {
                var res = DoRetrieveSnapshot(ssid3, "1", i.ToString());
                Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.NotFound);
            }

            for (int i = 0; i < 10; ++i)
            {
                var res = DoRetrieveSnapshot(ssid4, "1", i.ToString());
                Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.OK && res.Result != null);
                var raw = (TestEntity)res.Result;
                Assert.IsTrue(raw.a == 3 * i);
            }

            for (int i = 0; i < 10; ++i)
            {
                var res = DoRetrieve("1", i.ToString());
                Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.OK && res.Result != null);
                var raw = (TestEntity)res.Result;
                Assert.IsTrue(raw.a == 5 * i);
            }
        }

        [TestMethod]
        public void SingleOpGeneralTest()
        {
            // same as BatchCOWOverDeletedSnapshot, use single op API

            Assert.IsTrue(DoCreateTable());

            // insert
            TestEntity[] entities = new TestEntity[10];
            for (int i = 0; i < 10; ++i)
            {
                entities[i] = new TestEntity("1", i.ToString());
                entities[i].a = i;
                entities[i].d = "hello" + i.ToString();
            }

            var resList1 = new List<TableResult>();
            for (int i = 0; i < 10; ++i)
            {
                if (i % 3 == 0)
                    resList1.Add(DoInsert(entities[i]));
                else if (i % 3 == 1)
                    resList1.Add(DoIOR(entities[i]));
                else
                    resList1.Add(DoIOM(entities[i]));
            }

            Assert.IsTrue(resList1 != null && resList1.Count == 10);
            for (int i = 0; i < 10; ++i)
            {
                if (i % 3 == 0)
                    Assert.IsTrue(resList1[i] != null && resList1[i].HttpStatusCode == (int)HttpStatusCode.Created);
                else
                    Assert.IsTrue(resList1[i] != null && resList1[i].HttpStatusCode == (int)HttpStatusCode.NoContent);

                Assert.IsTrue(resList1[i].Etag == entities[i].ETag);
                Assert.IsTrue(entities[i].ETag.Contains(","));
            }

            var ssid0 = DoCreateSnapshot();
            var ssid1 = DoCreateSnapshot();

            DoDeleteSnapshot(ssid0);

            // test replace / merge / ior / iom, cow to ssid1
            var resList2 = new List<TableResult>();
            for (int i = 0; i < 5; ++i)
            {
                entities[i].a = i * 2;
                entities[i].b = i;

                if (i % 4 == 0)
                    resList2.Add(DoReplace(entities[i]));
                else if (i % 4 == 1)
                    resList2.Add(DoMerge(entities[i]));
                else if (i % 4 == 2)
                    resList2.Add(DoIOR(entities[i]));
                else if (i % 4 == 3)
                    resList2.Add(DoIOM(entities[i]));
            }

            Assert.IsTrue(resList2 != null && resList2.Count == 5);
            for (int i = 0; i < 5; ++i)
            {
                Assert.IsTrue(resList2[i] != null && resList2[i].HttpStatusCode == (int)HttpStatusCode.NoContent);
                Assert.IsTrue(resList2[i].Etag == entities[i].ETag);
                Assert.IsTrue(entities[i].ETag.Contains(","));
            }

            var ssid2 = DoCreateSnapshot();

            // test delete, 0-4 cow to ssid2, 5-9 skip ssid0 and cow to ssid1
            var resList3 = new List<TableResult>();
            for (int i = 0; i < 10; ++i)
                resList3.Add(DoDelete(entities[i]));

            Assert.IsTrue(resList3 != null && resList3.Count == 10);
            for (int i = 0; i < 10; ++i)
                Assert.IsTrue(resList3[i] != null && resList3[i].HttpStatusCode == (int)HttpStatusCode.NoContent);

            for (int i = 0; i < 10; ++i)
            {
                var ret = DoRetrieve("1", i.ToString());
                Assert.IsTrue(ret != null && ret.HttpStatusCode == (int)HttpStatusCode.NotFound);
            }

            var ssid3 = DoCreateSnapshot();

            // test insert back, cow to ssid3
            var resList4 = new List<TableResult>();
            for (int i = 0; i < 10; ++i)
            {
                entities[i].a = i * 3;
                entities[i].d = "wow!" + i.ToString();
                entities[i].ETag = "";

                if (i % 3 == 0)
                    resList4.Add(DoInsert(entities[i]));
                else if (i % 3 == 1)
                    resList4.Add(DoIOR(entities[i]));
                else if (i % 3 == 2)
                    resList4.Add(DoIOM(entities[i]));
            }

            Assert.IsTrue(resList4 != null && resList4.Count == 10);
            for (int i = 0; i < 10; ++i)
            {
                if ((i) % 3 == 0)
                    Assert.IsTrue(resList4[i] != null && resList4[i].HttpStatusCode == (int)HttpStatusCode.Created);
                else
                    Assert.IsTrue(resList4[i] != null && resList4[i].HttpStatusCode == (int)HttpStatusCode.NoContent);
                Assert.IsTrue(entities[i].ETag.Contains(","));
            }

            var ssid4 = DoCreateSnapshot();

            // replace / merge / ior / iom, cow to ssid4
            var resList5 = new List<TableResult>();
            for (int i = 0; i < 5; ++i)
            {
                entities[i].a = i * 4;
                entities[i].b = i;

                if (i % 4 == 0)
                    resList5.Add(DoReplace(entities[i]));
                else if (i % 4 == 1)
                    resList5.Add(DoMerge(entities[i]));
                else if (i % 4 == 2)
                    resList5.Add(DoIOR(entities[i]));
                else if (i % 4 == 3)
                    resList5.Add(DoIOM(entities[i]));
            }

            Assert.IsTrue(resList5 != null && resList5.Count == 5);
            for (int i = 0; i < 5; ++i)
            {
                Assert.IsTrue(resList5[i] != null && resList5[i].HttpStatusCode == (int)HttpStatusCode.NoContent);
                Assert.IsTrue(resList5[i].Etag == entities[i].ETag);
                Assert.IsTrue(entities[i].ETag.Contains(","));
            }

            var ssid5 = DoCreateSnapshot();
            DoDeleteSnapshot(ssid5);

            // replace / merge / ior / iom, 0-4 do not cow since there is no valid ss at 5 (head = 6), 5-9 cow to ssid4
            var resList6 = new List<TableResult>();
            for (int i = 0; i < 10; ++i)
            {
                entities[i].a = i * 5;
                entities[i].b = i;

                if (i % 4 == 0)
                    resList6.Add(DoReplace(entities[i]));
                else if (i % 4 == 1)
                    resList6.Add(DoMerge(entities[i]));
                else if (i % 4 == 2)
                    resList6.Add(DoIOR(entities[i]));
                else if (i % 4 == 3)
                    resList6.Add(DoIOM(entities[i]));
            }

            Assert.IsTrue(resList6 != null && resList6.Count == 10);
            for (int i = 0; i < 10; ++i)
            {
                Assert.IsTrue(resList6[i] != null && resList6[i].HttpStatusCode == (int)HttpStatusCode.NoContent);
                Assert.IsTrue(resList6[i].Etag == entities[i].ETag);
                Assert.IsTrue(entities[i].ETag.Contains(","));
            }


            // snapshot 0: not exist
            // snapshot 1: initial state, 10 row with a = i
            // snapshot 2: 1st state, 0-4 with a = 2*i, 5-9 missing (fall to ss1, a = i)
            // snapshot 3: all deleted
            // snapshot 4: all inserted with a = 3*i
            // snapshot 5: not exist
            // head: all with a = 5*i

            var historyInfo = DoGetHistory();
            Assert.IsTrue(historyInfo != null);
            Assert.IsTrue(historyInfo.HeadSSID == 6);
            Assert.IsTrue(historyInfo.Snapshots != null);
            Assert.IsTrue(historyInfo.Snapshots.Count == 7);
            for (int i = 0; i <= 6; ++i)
            {
                Assert.IsTrue(historyInfo.GetSnapshot(i) == historyInfo.Snapshots[i]);
                Assert.IsTrue(historyInfo.Snapshots[i].SSID == i);
            }

            Assert.IsTrue(historyInfo.Snapshots[0].State == SnapshotState.Deleted);
            Assert.IsTrue(historyInfo.Snapshots[0].Parent == SnapshotInfo.NoParent);
            Assert.IsTrue(historyInfo.Snapshots[1].State == SnapshotState.Valid);
            Assert.IsTrue(historyInfo.Snapshots[1].Parent == 0);
            Assert.IsTrue(historyInfo.Snapshots[2].State == SnapshotState.Valid);
            Assert.IsTrue(historyInfo.Snapshots[2].Parent == 1);
            Assert.IsTrue(historyInfo.Snapshots[3].State == SnapshotState.Valid);
            Assert.IsTrue(historyInfo.Snapshots[3].Parent == 2);
            Assert.IsTrue(historyInfo.Snapshots[4].State == SnapshotState.Valid);
            Assert.IsTrue(historyInfo.Snapshots[4].Parent == 3);
            Assert.IsTrue(historyInfo.Snapshots[5].State == SnapshotState.Deleted);
            Assert.IsTrue(historyInfo.Snapshots[5].Parent == 4);
            Assert.IsTrue(historyInfo.Snapshots[6].State == SnapshotState.Head);
            Assert.IsTrue(historyInfo.Snapshots[6].Parent == 5);


            for (int i = 0; i < 10; ++i)
            {
                var res = DoRetrieveSnapshot(ssid1, "1", i.ToString());
                Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.OK && res.Result != null);
                var raw = (TestEntity)res.Result;
                Assert.IsTrue(raw.a == i);
            }

            for (int i = 0; i < 10; ++i)
            {
                var res = DoRetrieveSnapshot(ssid2, "1", i.ToString());
                Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.OK && res.Result != null);
                var raw = (TestEntity)res.Result;
                if (i < 5)
                    Assert.IsTrue(raw.a == 2 * i);
                else
                    Assert.IsTrue(raw.a == i);
            }

            for (int i = 0; i < 10; ++i)
            {
                var res = DoRetrieveSnapshot(ssid3, "1", i.ToString());
                Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.NotFound);
            }

            for (int i = 0; i < 10; ++i)
            {
                var res = DoRetrieveSnapshot(ssid4, "1", i.ToString());
                Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.OK && res.Result != null);
                var raw = (TestEntity)res.Result;
                Assert.IsTrue(raw.a == 3 * i);
            }

            for (int i = 0; i < 10; ++i)
            {
                var res = DoRetrieve("1", i.ToString());
                Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.OK && res.Result != null);
                var raw = (TestEntity)res.Result;
                Assert.IsTrue(raw.a == 5 * i);
            }
        }

        [TestMethod]
        public void RollbackGeneralTest()
        {
            // same as SingleOpGeneralTest, do not delete snapshots, add rollback

            Assert.IsTrue(DoCreateTable());

            // insert
            TestEntity[] entities = new TestEntity[10];
            for (int i = 0; i < 10; ++i)
            {
                entities[i] = new TestEntity("1", i.ToString());
                entities[i].a = i;
                entities[i].d = "hello" + i.ToString();
            }

            var resList1 = new List<TableResult>();
            for (int i = 0; i < 10; ++i)
            {
                if (i % 3 == 0)
                    resList1.Add(DoInsert(entities[i]));
                else if (i % 3 == 1)
                    resList1.Add(DoIOR(entities[i]));
                else
                    resList1.Add(DoIOM(entities[i]));
            }

            Assert.IsTrue(resList1 != null && resList1.Count == 10);
            for (int i = 0; i < 10; ++i)
            {
                if (i % 3 == 0)
                    Assert.IsTrue(resList1[i] != null && resList1[i].HttpStatusCode == (int)HttpStatusCode.Created);
                else
                    Assert.IsTrue(resList1[i] != null && resList1[i].HttpStatusCode == (int)HttpStatusCode.NoContent);

                Assert.IsTrue(resList1[i].Etag == entities[i].ETag);
                Assert.IsTrue(entities[i].ETag.Contains(","));
            }

            string[] etagBackup0 = new string[10];
            for (int i = 0; i < 10; ++i)
                etagBackup0[i] = entities[i].ETag;

            var ssid0 = DoCreateSnapshot();

            // test replace / merge / ior / iom, cow to ssid0
            var resList2 = new List<TableResult>();
            for (int i = 0; i < 5; ++i)
            {
                entities[i].a = i * 2;
                entities[i].b = i;

                if (i % 4 == 0)
                    resList2.Add(DoReplace(entities[i]));
                else if (i % 4 == 1)
                    resList2.Add(DoMerge(entities[i]));
                else if (i % 4 == 2)
                    resList2.Add(DoIOR(entities[i]));
                else if (i % 4 == 3)
                    resList2.Add(DoIOM(entities[i]));
            }

            Assert.IsTrue(resList2 != null && resList2.Count == 5);
            for (int i = 0; i < 5; ++i)
            {
                Assert.IsTrue(resList2[i] != null && resList2[i].HttpStatusCode == (int)HttpStatusCode.NoContent);
                Assert.IsTrue(resList2[i].Etag == entities[i].ETag);
                Assert.IsTrue(entities[i].ETag.Contains(","));
            }

            string[] etagBackup1 = new string[10];
            for (int i = 0; i < 10; ++i)
                etagBackup1[i] = entities[i].ETag;

            // rollback to ssid0
            var ssid1 = DoRollback(ssid0);

            for (int i = 0; i < 10; ++i)
            {
                var res = DoRetrieve("1", i.ToString());
                Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.OK && res.Result != null);
                var raw = (TestEntity)res.Result;
                Assert.IsTrue(raw.a == i && raw.ETag == etagBackup0[i]);
                
                entities[i].ETag = raw.ETag;
            }

            // test delete, cow to ssid1
            var resList3 = new List<TableResult>();
            for (int i = 0; i < 10; ++i)
                resList3.Add(DoDelete(entities[i]));

            Assert.IsTrue(resList3 != null && resList3.Count == 10);
            for (int i = 0; i < 10; ++i)
                Assert.IsTrue(resList3[i] != null && resList3[i].HttpStatusCode == (int)HttpStatusCode.NoContent);

            for (int i = 0; i < 10; ++i)
            {
                var ret = DoRetrieve("1", i.ToString());
                Assert.IsTrue(ret != null && ret.HttpStatusCode == (int)HttpStatusCode.NotFound);
            }

            // rollback the rollback, to ssid1

            var ssid2 = DoRollback(ssid1);

            for (int i = 0; i < 10; ++i)
            {
                var res = DoRetrieve("1", i.ToString());
                Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.OK && res.Result != null);
                var raw = (TestEntity)res.Result;
                Assert.IsTrue(raw.ETag == etagBackup1[i]);

                if (i < 5)
                    Assert.IsTrue(raw.a == 2 * i);
                else
                    Assert.IsTrue(raw.a == i);

                entities[i].ETag = raw.ETag;
            }

            // insert a row
            var newRow = new TestEntity("1", "10");
            newRow.a = 999;
            var newRowIns = DoInsert(newRow);
            Assert.IsTrue(newRowIns != null && newRowIns.HttpStatusCode == (int)HttpStatusCode.Created);
            var newRowETagBackup = newRow.ETag;

            var ssid3 = DoCreateSnapshot();

            // modify rows
            var resList4 = new List<TableResult>();
            for (int i = 5; i < 10; ++i)
            {
                entities[i].a = i * 4;
                entities[i].b = i;

                if (i % 4 == 0)
                    resList4.Add(DoReplace(entities[i]));
                else if (i % 4 == 1)
                    resList4.Add(DoMerge(entities[i]));
                else if (i % 4 == 2)
                    resList4.Add(DoIOR(entities[i]));
                else if (i % 4 == 3)
                    resList4.Add(DoIOM(entities[i]));
            }

            Assert.IsTrue(resList4 != null && resList4.Count == 5);
            for (int i = 0; i < 5; ++i)
            {
                Assert.IsTrue(resList4[i] != null && resList4[i].HttpStatusCode == (int)HttpStatusCode.NoContent);
                Assert.IsTrue(resList4[i].Etag == entities[5 + i].ETag);
                Assert.IsTrue(resList4[i].Etag.Contains(","));
            }

            string[] etagBackup4 = new string[10];
            for (int i = 0; i < 10; ++i)
                etagBackup4[i] = entities[i].ETag;

            // rollback to ssid0 again, test deleting a row in rollback
            var ssid4 = DoRollback(ssid0);

            for (int i = 0; i < 10; ++i)
            {
                var res = DoRetrieve("1", i.ToString());
                Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.OK && res.Result != null);
                var raw = (TestEntity)res.Result;
                Assert.IsTrue(raw.a == i && raw.ETag == etagBackup0[i]);

                entities[i].ETag = raw.ETag;
            }
            var newRowRet = DoRetrieve("1", "10");
            Assert.IsTrue(newRowRet != null && newRowRet.HttpStatusCode == (int)HttpStatusCode.NotFound);

            // rollback the rollback again, back to ssid4, test creating row in rollback
            var ssid5 = DoRollback(ssid4);

            for (int i = 0; i < 10; ++i)
            {
                var res = DoRetrieve("1", i.ToString());
                Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.OK && res.Result != null);
                var raw = (TestEntity)res.Result;

                Assert.IsTrue(raw.ETag == etagBackup4[i]);

                if (i < 5)
                    Assert.IsTrue(raw.a == i * 2);
                else
                    Assert.IsTrue(raw.a == i * 4);

                entities[i].ETag = raw.ETag;
            }

            newRowRet = DoRetrieve("1", "10");
            Assert.IsTrue(newRowRet != null && newRowRet.HttpStatusCode == (int)HttpStatusCode.OK && newRowRet.Result != null);
            var newRowData = (TestEntity)newRowRet.Result;
            Assert.IsTrue(newRowData.ETag == newRowETagBackup && newRowData.a == 999);


            // snapshot 0: initial state, 10 row with a = i
            // snapshot 1: second state, 0-4 with a = 2*i, 5-9 with a = i
            // snapshot 2: rollback to 0, then delete all rows
            // snapshot 3: rollback to snapshot 1, 0-4 with a = 2*i, 5-9 with a = i, insert row 10 with a = 999
            // snapshot 4: further update, 0-4 with a = 2*i, 5-9 with a = 4*i, 10 with a = 999
            // snapshot 5: rollback to ss0, 10 row with a = i
            // head (6): rollback to ss4

            var historyInfo = DoGetHistory();
            Assert.IsTrue(historyInfo != null);
            Assert.IsTrue(historyInfo.HeadSSID == 6);
            Assert.IsTrue(historyInfo.Snapshots != null);
            Assert.IsTrue(historyInfo.Snapshots.Count == 7);
            for (int i = 0; i <= 6; ++i)
            {
                Assert.IsTrue(historyInfo.GetSnapshot(i) == historyInfo.Snapshots[i]);
                Assert.IsTrue(historyInfo.Snapshots[i].SSID == i);
            }

            Assert.IsTrue(historyInfo.Snapshots[0].State == SnapshotState.Valid);
            Assert.IsTrue(historyInfo.Snapshots[0].Parent == SnapshotInfo.NoParent);
            Assert.IsTrue(historyInfo.Snapshots[1].State == SnapshotState.Valid);
            Assert.IsTrue(historyInfo.Snapshots[1].Parent == 0);
            Assert.IsTrue(historyInfo.Snapshots[2].State == SnapshotState.Valid);
            Assert.IsTrue(historyInfo.Snapshots[2].Parent == 0);
            Assert.IsTrue(historyInfo.Snapshots[3].State == SnapshotState.Valid);
            Assert.IsTrue(historyInfo.Snapshots[3].Parent == 1);
            Assert.IsTrue(historyInfo.Snapshots[4].State == SnapshotState.Valid);
            Assert.IsTrue(historyInfo.Snapshots[4].Parent == 3);
            Assert.IsTrue(historyInfo.Snapshots[5].State == SnapshotState.Valid);
            Assert.IsTrue(historyInfo.Snapshots[5].Parent == 0);
            Assert.IsTrue(historyInfo.Snapshots[6].State == SnapshotState.Head);
            Assert.IsTrue(historyInfo.Snapshots[6].Parent == 4);

            for (int i = 0; i < 10; ++i)
            {
                var res = DoRetrieveSnapshot(ssid0, "1", i.ToString());
                Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.OK && res.Result != null);
                var raw = (TestEntity)res.Result;
                Assert.IsTrue(raw.a == i);
                Assert.IsTrue(raw.ETag == etagBackup0[i]);
            }

            for (int i = 0; i < 10; ++i)
            {
                var res = DoRetrieveSnapshot(ssid1, "1", i.ToString());
                Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.OK && res.Result != null);
                var raw = (TestEntity)res.Result;
                if (i < 5)
                    Assert.IsTrue(raw.a == 2 * i);
                else
                    Assert.IsTrue(raw.a == i);
                Assert.IsTrue(raw.ETag == etagBackup1[i]);
            }

            for (int i = 0; i < 10; ++i)
            {
                var res = DoRetrieveSnapshot(ssid2, "1", i.ToString());
                Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.NotFound);
            }

            for (int i = 0; i < 11; ++i)
            {
                var res = DoRetrieveSnapshot(ssid3, "1", i.ToString());
                Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.OK && res.Result != null);
                var raw = (TestEntity)res.Result;
                if (i == 10)
                {
                    Assert.IsTrue(raw.a == 999 && raw.ETag == newRowETagBackup);
                }
                else
                {
                    if (i < 5)
                        Assert.IsTrue(raw.a == 2 * i);
                    else
                        Assert.IsTrue(raw.a == i);
                    Assert.IsTrue(raw.ETag == etagBackup1[i]);
                }
            }

            for (int i = 0; i < 11; ++i)
            {
                var res = DoRetrieveSnapshot(ssid4, "1", i.ToString());
                Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.OK && res.Result != null);
                var raw = (TestEntity)res.Result;
                if (i == 10)
                {
                    Assert.IsTrue(raw.a == 999 && raw.ETag == newRowETagBackup);
                }
                else
                {
                    if (i < 5)
                        Assert.IsTrue(raw.a == 2 * i);
                    else
                        Assert.IsTrue(raw.a == 4 * i);
                    Assert.IsTrue(raw.ETag == etagBackup4[i]);
                }
            }

            for (int i = 0; i < 11; ++i)
            {
                var res = DoRetrieveSnapshot(ssid5, "1", i.ToString());
                if (i == 10)
                {
                    Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.NotFound);
                }
                else
                {
                    Assert.IsTrue(res != null && res.HttpStatusCode == (int)HttpStatusCode.OK && res.Result != null);
                    var raw = (TestEntity)res.Result;
                    Assert.IsTrue(raw.a == i && raw.ETag == etagBackup0[i]);
                }
            }
        }

        [TestMethod]
        public void ConcurrentAccess()
        {
            int nUser = 20;
            int nRow = 100;
            int runTime = 90;

            Assert.IsTrue(DoCreateTable());

            Random r = new Random();
            StreamWriter logger = new StreamWriter("d:/kiwi/debug.out", true);
            logger.AutoFlush = true;

            User[] users = new User[nUser];
            for (int i = 0; i < nUser; ++i)
                users[i] = new User(i, new STable(name, config, ts), nRow, logger, r.Next());

            for (int i = 0; i < nUser; ++i)
                users[i].Start();

            Thread.Sleep(runTime * 1000);

            for (int i = 0; i < nUser; ++i)
                users[i].Stop();

            for (int i = 0; i < nUser; ++i)
                users[i].Join();

            logger.Flush();
            logger.Close();
                
        }

        [TestMethod]
        public void QueryAPIGeneralTest()
        {
            Assert.IsTrue(DoCreateTable());

            // insert
            TestEntity[] entities = new TestEntity[10];
            for (int i = 0; i < 10; ++i)
            {
                entities[i] = new TestEntity("1", i.ToString());
                entities[i].a = i;
                entities[i].d = "hello" + i.ToString();
            }

            var resList1 = new List<TableResult>();
            for (int i = 0; i < 10; ++i)
            {
                if (i % 3 == 0)
                    resList1.Add(DoInsert(entities[i]));
                else if (i % 3 == 1)
                    resList1.Add(DoIOR(entities[i]));
                else
                    resList1.Add(DoIOM(entities[i]));
            }

            Assert.IsTrue(resList1 != null && resList1.Count == 10);
            for (int i = 0; i < 10; ++i)
            {
                if (i % 3 == 0)
                    Assert.IsTrue(resList1[i] != null && resList1[i].HttpStatusCode == (int)HttpStatusCode.Created);
                else
                    Assert.IsTrue(resList1[i] != null && resList1[i].HttpStatusCode == (int)HttpStatusCode.NoContent);

                Assert.IsTrue(resList1[i].Etag == entities[i].ETag);
                Assert.IsTrue(entities[i].ETag.Contains(","));
            }

            // Query all rows
            var query = new TableQuery<TestEntity>();
            var queryRes = DoQuery(query).OrderBy((x) => int.Parse(x.RowKey));
            int qcnt = 0;
            foreach (var qres in queryRes)
            {
                Assert.IsTrue(qcnt < 10);
                Assert.IsTrue(qres.RowKey.Equals(entities[qcnt].RowKey));
                Assert.IsTrue(qres.PartitionKey.Equals(entities[qcnt].PartitionKey));
                Assert.IsTrue(qres.ETag.Equals(entities[qcnt].ETag));
                Assert.IsTrue(qres.a == qcnt);
                Assert.IsTrue(qres.d.Equals(entities[qcnt].d));
                ++qcnt;
            }
            Assert.IsTrue(qcnt == 10);

            // Query with filter
            query = new TableQuery<TestEntity>().Where(TableQuery.GenerateFilterConditionForInt("a", QueryComparisons.LessThan, 5));
            queryRes = DoQuery(query).OrderBy((x) => int.Parse(x.RowKey));
            qcnt = 0;
            foreach (var qres in queryRes)
            {
                Assert.IsTrue(qcnt < 5);
                Assert.IsTrue(qres.RowKey.Equals(entities[qcnt].RowKey));
                Assert.IsTrue(qres.PartitionKey.Equals(entities[qcnt].PartitionKey));
                Assert.IsTrue(qres.ETag.Equals(entities[qcnt].ETag));
                Assert.IsTrue(qres.a == qcnt);
                Assert.IsTrue(qres.d.Equals(entities[qcnt].d));
                ++qcnt;
            }
            Assert.IsTrue(qcnt == 5);

            // Query with take
            query = new TableQuery<TestEntity>().Where(TableQuery.GenerateFilterConditionForInt("a", QueryComparisons.LessThan, 5)).Take(3);
            queryRes = DoQuery(query).OrderBy((x) => int.Parse(x.RowKey));
            qcnt = 0;
            int prevId = -1;
            foreach (var qres in queryRes)
            {
                Assert.IsTrue(qcnt < 3);
                Assert.IsTrue(int.Parse(qres.RowKey) > prevId);
                prevId = int.Parse(qres.RowKey);

                Assert.IsTrue(qres.RowKey.Equals(entities[prevId].RowKey));
                Assert.IsTrue(qres.PartitionKey.Equals(entities[prevId].PartitionKey));
                Assert.IsTrue(qres.ETag.Equals(entities[prevId].ETag));
                Assert.IsTrue(qres.a == prevId);
                Assert.IsTrue(qres.d.Equals(entities[prevId].d));
                ++qcnt;
            }
            Assert.IsTrue(qcnt == 3);

            // Do not test select(), since TestEntity requires all cols to exist

            var ssid0 = DoCreateSnapshot();
            var ssid1 = DoCreateSnapshot();

            DoDeleteSnapshot(ssid0);

            // test delete, 0-4 cow to ssid1
            var resList3 = new List<TableResult>();
            for (int i = 0; i < 5; ++i)
                resList3.Add(DoDelete(entities[i]));

            Assert.IsTrue(resList3 != null && resList3.Count == 5);
            for (int i = 0; i < 5; ++i)
                Assert.IsTrue(resList3[i] != null && resList3[i].HttpStatusCode == (int)HttpStatusCode.NoContent);

            // Query all rows, check behavior with deleted rows
            query = new TableQuery<TestEntity>();
            queryRes = DoQuery(query).OrderBy((x) => int.Parse(x.RowKey));
            qcnt = 0;
            foreach (var qres in queryRes)
            {
                Assert.IsTrue(qcnt < 5);
                Assert.IsTrue(qres.RowKey.Equals(entities[qcnt + 5].RowKey));
                Assert.IsTrue(qres.PartitionKey.Equals(entities[qcnt + 5].PartitionKey));
                Assert.IsTrue(qres.ETag.Equals(entities[qcnt + 5].ETag));
                Assert.IsTrue(qres.a == qcnt + 5);
                Assert.IsTrue(qres.d.Equals(entities[qcnt + 5].d));
                ++qcnt;
            }
            Assert.IsTrue(qcnt == 5);
        }




        private string name;
        private Random random = new Random();
        private ISTableConfig config;
        private IChainTableService ts;
        private STable s;


        private bool DoCreateTable()
        {
            return s.CreateIfNotExists();
        }

        private bool DoDeleteTable()
        {
            return s.DeleteIfExists();
        }

        private TableResult DoInsert(TestEntity entity)
        {
            return s.Execute(TableOperation.Insert(entity));
        }

        private TableResult DoRetrieve(string pKey, string rKey)
        {
            return s.Execute(TableOperation.Retrieve<TestEntity>(pKey, rKey));
        }

        private TableResult DoReplace(TestEntity entity)
        {
            return s.Execute(TableOperation.Replace(entity));
        }

        private TableResult DoMerge(ITableEntity entity)
        {
            return s.Execute(TableOperation.Merge(entity));
        }

        private TableResult DoDelete(TestEntity entity)
        {
            return s.Execute(TableOperation.Delete(entity));
        }

        private TableResult DoIOR(TestEntity entity)
        {
            return s.Execute(TableOperation.InsertOrReplace(entity));
        }

        private TableResult DoIOM(ITableEntity entity)
        {
            return s.Execute(TableOperation.InsertOrMerge(entity));
        }

        private IList<TableResult> DoExecuteBatch(TableBatchOperation batch)
        {
            return s.ExecuteBatch(batch);
        }

        private TableResult DoRetrieveSnapshot(int ssid, string pKey, string rKey)
        {
            return s.RetrieveFromSnapshot(TableOperation.Retrieve<TestEntity>(pKey, rKey), ssid);
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

        private void DoGC()
        {
            s.GC();
        }

        private IEnumerable<TestEntity> DoQuery(TableQuery<TestEntity> q)
        {
            return s.ExecuteQuery(q);
        }

        private STableHistoryInfo DoGetHistory()
        {
            return s.GetSTableHistory();
        }
    }
}
