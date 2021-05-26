using Microsoft.EntityFrameworkCore;
using NUnit.Framework;
using Phy.EfBulk.TestDb;
using Phy.EfBulk.TestDb.Entities;
using System.Collections.Generic;
using System.Linq;

namespace Phy.EfBulk.MySqlTest
{
    public class BulkTests
    {
        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public void BulkTest()
        {
            using (var db = Get())
            {
                var categories = Enumerable.Range(0, 1000).Select(x => new Category
                {
                    Id = x,
                    Name = $"Name{x}"
                });
                var listCount = categories.Count();
                var insertedCount = db.BulkInsert(categories);
                Assert.AreEqual(listCount, insertedCount);


                var updateQuery = db.Categories.Where(x => x.Id >= 500);
                var updateQueryCount = updateQuery.Count();
                var updatedCount = db.BulkUpdate(updateQuery, x => new Category
                {
                    Name = x.Id.ToString() + "-Update"
                });
                Assert.AreEqual(updateQueryCount, updatedCount);


                var deleteQuery = db.Categories.Where(x => x.Id < 500);
                var deleteQueryCount = deleteQuery.Count();
                var deletedCount = db.BulkDelete(deleteQuery);
                Assert.AreEqual(deleteQueryCount, deletedCount);

                var updateSuccessed = db.Categories.Where(x => x.Id >= 500).All(x => x.Name.EndsWith("Update"));
                Assert.AreEqual(updateSuccessed, true);

                var remainedCount = db.Categories.Count();
                var clearCount = db.BulkDelete(db.Categories);
                Assert.AreEqual(remainedCount, clearCount);

                Assert.IsTrue(db.Categories.Count() == 0);
            }
        }

        public MyDbContext Get()
        {
            var contextOptions = new DbContextOptionsBuilder<MyDbContext>()
            .UseMySql("Server=192.168.1.254;Port=3306;Database=bulktestedb;Uid=cvuser;Pwd=123456;Pooling=true;Max Pool Size=99;Min Pool Size=10;SslMode=Preferred;CharSet=utf8mb4;Allow User Variables=True;", new MySqlServerVersion("8.0.24"))
            .Options;
            var db = new MyDbContext(contextOptions);
            db.Database.EnsureDeleted();
            //db.Database.EnsureCreated();
            return db;
        }
    }
}