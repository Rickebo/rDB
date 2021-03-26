using NUnit.Framework;

using rDB.Tests.Data;

using SqlKata.Execution;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace rDB.Tests
{
    public class SqliteDatabaseTests
    {
        private const string DatabaseDirectory = "test-db";
        private static volatile int CurrentDatabase = 0;

        [SetUp]
        public void Setup() => Clear();

        [TearDown]
        public void TearDown() => Clear();

        private static void Clear()
        {
            var dir = new DirectoryInfo(DatabaseDirectory);

            if (!Directory.Exists(dir.FullName))
            {
                dir.Create();
                return;
            }

            foreach (var file in dir.GetFiles("*.db3"))
                file.Delete();
        }

        private static string GetDatabaseName()
        {
            var number = Interlocked.Increment(ref CurrentDatabase);
            return Path.Combine(DatabaseDirectory, $"db-{number}.db3");
        }

        private static async Task<SqliteDatabase> CreateDatabase()
        {
            var name = GetDatabaseName();

            return await SqliteDatabase
                .Builder(name)
                .WithTable(
                    new TestReferencedTable(),
                    new TestDoubleReferencedTable(),
                    new TestTable())
                .Build();
        }
            

        [Test]
        public async Task TestSqlBuilder()
        {
            try
            {
                var db = await CreateDatabase();
                
                Assert.Zero(db.OpenConnections);
            } catch
            {
                Assert.Fail("An exception occurred.");
            }
        }

        [Test]
        public async Task TestInsertion()
        {
            const int id = 111;

            try
            {
                var db = await CreateDatabase();

                await using var table = await db.Table<TestReferencedTable>();
                var result = await table.Insert(new TestReferencedTable()
                {
                    Id = id
                });

                Assert.Greater(result, 0);
                Assert.Zero(db.OpenConnections);
            } catch
            {
                Assert.Fail();
            }
        }

        [Test]
        public async Task TestInsertionSelection()
        {
            const int id = 111;

            try
            {
                var db = await CreateDatabase();

                await using var table = await db.Table<TestReferencedTable>();
                var result = await table.Insert(new TestReferencedTable()
                {
                    Id = id
                });

                Assert.Greater(result, 0);

                var select = await table.SelectFirst(q => q.Where(nameof(TestReferencedTable.Id), id));

                Assert.AreEqual(select.Id, id);
                Assert.Zero(db.OpenConnections);
            } catch
            {
                Assert.Fail();
            }
        }

        
        [Test]
        public async Task TestInsertionSelection2()
        {
            const int id = 111;

            try
            {
                var db = await CreateDatabase();

                await using (var table = await db.Table<TestReferencedTable>())
                {
                    var result = await table.Insert(new TestReferencedTable()
                    {
                        Id = id
                    });

                    Assert.Greater(result, 0);
                }

                var selection = await db.Select<TestReferencedTable, int>(query => query
                    .FirstAsync<int>());

                Assert.AreEqual(id, selection);
                Assert.Zero(db.OpenConnections);
            } catch
            {
                Assert.Fail("An exception occurred.");
            }
        }
    }
}
