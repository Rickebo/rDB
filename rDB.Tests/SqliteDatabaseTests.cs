using NUnit.Framework;

using rDB.Tests.Data;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace rDB.Tests
{
    public class SqliteDatabaseTests
    {
        private const string DatabaseFile = "test.db3";

        [SetUp]
        public void Setup()
        {
            File.Delete(DatabaseFile);
        }

        [Test]
        public async Task TestSqlBuilder()
        {
            const int id = 111;

            var db = await SqliteDatabase
                .Builder(DatabaseFile)
                .WithTable(
                    new SqliteTestReferencedTable(),
                    new SqliteTestTable())
                .Build();

            await using var table = await db.Table<SqliteTestReferencedTable>();
            var result = await table.Insert(new SqliteTestReferencedTable()
            {
                Id = id
            });

            Assert.Greater(result, 0);

            var select = await table.SelectFirst(q => q.Where(nameof(SqliteTestReferencedTable.Id), id));

            Assert.AreEqual(select.Id, id);
        }
    }
}
