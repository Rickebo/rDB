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
using System.Collections.Immutable;

namespace rDB
{
    public class TableConnectionContext<TTable, TConnection> : BaseConnectionContext<TConnection>, IDisposable, IAsyncDisposable
        where TConnection : DbConnection
        where TTable : DatabaseEntry
    {
        private BaseConnectionContext<TConnection> ConnectionContext { get; }
        public string TableName { get; }
        public IImmutableSet<DatabaseColumnContext> Columns { get; }

        public override SchemaContext Schema => ConnectionContext.Schema;

        public override TConnection Connection => ConnectionContext.Connection;

        public override QueryFactory Factory => ConnectionContext.Factory;

        public TableConnectionContext(BaseConnectionContext<TConnection> connectionContext)
        {
            ConnectionContext = connectionContext;

            TableName = connectionContext.Schema.TableName<TTable>();
            Columns = connectionContext.Schema.ColumnMap[typeof(TTable)];
        }

        public Query Query() => Query<TTable>(); 

        public async Task SelectOrInsert(Func<Query, Query> reader, Func<TTable> itemCreator) =>
            await SelectOrInsert(reader, itemCreator)
                .ConfigureAwait(false);

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

        #region Select
        public async Task<TTable> SelectFirst(Func<Query, Query> processor) =>
            await SelectFirst<TTable, TTable>(processor)
                .ConfigureAwait(false);

        public async Task<TTable> SelectFirstOrDefault(Func<Query, Query> processor) =>
            await SelectFirstOrDefault<TTable, TTable>(processor)
                .ConfigureAwait(false);

        public async Task<IEnumerable<TTable>> Select(Func<Query, Query> processor) =>
            await Select<TTable, TTable>(processor)
                .ConfigureAwait(false);

        #endregion

        public void Dispose() => ConnectionContext.Dispose();
        public async ValueTask DisposeAsync() => await ConnectionContext.DisposeAsync();
    }
}
