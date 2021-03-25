using SqlKata;
using SqlKata.Compilers;
using SqlKata.Execution;

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace rDB
{
    public class TableConnectionContext<T, T1> : ConnectionContext<T1>, IDisposable, IAsyncDisposable
        where T1 : DbConnection 
        where T : DatabaseEntry
    {
        public Type TableType { get; }
        public string TableName { get; }
        public ColumnSet Columns { get; }

        public TableConnectionContext(Type tableType, string tableName, ColumnSet tableColumns, 
            T1 connection, Compiler compiler) : base(connection, compiler)
        {
            TableType = tableType;
            TableName = tableName;
            Columns = tableColumns;
        }

        public async Task<int> Insert(T entry, Predicate<DatabaseColumnContext> columnSelector = null)
        {
            var command = Connection.CreateCommand();
            var columns = columnSelector != null
                ? Columns.Where(col => columnSelector(col))
                : Columns;

            var columnNames = string.Join(",", columns.Select(col => col.Name));
            var columnParameterNames = string.Join(",", columns.Select(col => "@" + col.Name));

            command.CommandText = $"INSERT INTO {TableName} ({columnNames}) VALUES ({columnParameterNames})";

            entry.Save(columns, (name, value) =>
            {
                var parameter = command.CreateParameter();

                parameter.ParameterName = name;
                parameter.Value = value;

                command.Parameters.Add(parameter);
            });

            return await command.ExecuteNonQueryAsync()
                .ConfigureAwait(false);
        }

        public Query Query() => 
            Factory.Query(TableName);

        public async Task<T2> Query<T2>(Func<Query, Task<T2>> selector)
        {
            var query = Factory.Query(TableName);
            return await selector(query);
        }

        public async Task<T> SelectFirst(Func<Query, Query> processor) =>
            await processor(Query()).FirstAsync<T>()
                .ConfigureAwait(false);

        public async Task<T> SelectFirstOrDefault(Func<Query, Query> processor) =>
            await processor(Query()).FirstOrDefaultAsync<T>()
                .ConfigureAwait(false);

        public async Task<IEnumerable<T>> Select(Func<Query, Query> processor) =>
            await processor(Query()).GetAsync<T>()
                .ConfigureAwait(false);
    }
}
