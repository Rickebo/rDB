using SqlKata;
using SqlKata.Compilers;
using SqlKata.Execution;

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using ColumnSet = System.Collections.Immutable.ImmutableHashSet<rDB.DatabaseColumnContext>;
using ColumnMap = System.Collections.Immutable.ImmutableDictionary<System.Type, System.Collections.Immutable.ImmutableHashSet<rDB.DatabaseColumnContext>>;
using TypeMap = System.Collections.Immutable.ImmutableDictionary<System.Type, string>;

namespace rDB
{
    public struct TableConnectionContext<TTable, TConnection> : IDisposable, IAsyncDisposable
        where TConnection : DbConnection 
        where TTable : DatabaseEntry
    {
        private readonly ConnectionContext<TConnection> ConnectionContext { get; }
        public readonly string TableName { get; }
        public readonly ColumnSet Columns { get; }

        public TableConnectionContext(string tableName, ColumnSet tableColumns, 
            TConnection connection, Compiler compiler)
        {
            ConnectionContext = new ConnectionContext<TConnection>(connection, compiler);
            TableName = tableName;
            Columns = tableColumns;
        }

        public async Task<int> Insert(TTable entry, Predicate<DatabaseColumnContext> columnSelector = null)
        {
            var command = ConnectionContext.Connection.CreateCommand();
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
            ConnectionContext.Factory.Query(TableName);

        public async Task<T2> Query<T2>(Func<Query, Task<T2>> selector)
        {
            var query = ConnectionContext.Factory.Query(TableName);
            return await selector(query);
        }

        public async Task<TTable> SelectFirst(Func<Query, Query> processor) =>
            await processor(Query()).FirstAsync<TTable>()
                .ConfigureAwait(false);

        public async Task<TTable> SelectFirstOrDefault(Func<Query, Query> processor) =>
            await processor(Query()).FirstOrDefaultAsync<TTable>()
                .ConfigureAwait(false);

        public async Task<IEnumerable<TTable>> Select(Func<Query, Query> processor) =>
            await processor(Query()).GetAsync<TTable>()
                .ConfigureAwait(false);

        public void Dispose() => ConnectionContext.Dispose();
        public async ValueTask DisposeAsync() => await ConnectionContext.DisposeAsync();
    }
}
