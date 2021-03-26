using Microsoft.Data.Sqlite;

using rDB.Builder;

using SqlKata.Compilers;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace rDB.Tests
{
    public class SqliteDatabase : Database<SqliteConnection>
    {
        private readonly string _path;

        protected SqliteDatabase(string path) : base(new SqliteCompiler())
        {
            _path = path;
        }

        public static DatabaseBuilder<SqliteDatabase, SqliteConnection> Builder(string path) => 
            new DatabaseBuilder<SqliteDatabase, SqliteConnection>(new SqliteDatabase(path));

        protected override async Task<SqliteConnection> GetConnection()
        {
            var connection = new SqliteConnection($"Data Source={_path}");
            await connection.OpenAsync();

            return connection;
        }
    }
}
