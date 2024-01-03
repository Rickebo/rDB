using SqlKata;
using SqlKata.Compilers;
using SqlKata.Execution;

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using ColumnSet =
    System.Collections.Immutable.ImmutableHashSet<rDB.DatabaseColumnContext>;
using ColumnMap =
    System.Collections.Immutable.ImmutableDictionary<System.Type, System.
        Collections.Immutable.ImmutableHashSet<rDB.DatabaseColumnContext>>;
using TypeMap =
    System.Collections.Immutable.ImmutableDictionary<System.Type, string>;

using System.Collections.Immutable;
using System.Data;

namespace rDB
{
    public class TableConnectionContext<TTable, TConnection>
        : BaseConnectionContext<TConnection>, IDisposable, IAsyncDisposable
        where TConnection : DbConnection
        where TTable : DatabaseEntry
    {
        private BaseConnectionContext<TConnection> ConnectionContext { get; }
        public string TableName { get; }
        public IImmutableSet<DatabaseColumnContext> Columns { get; }

        public override SchemaContext Schema => ConnectionContext.Schema;

        public override TConnection Connection => ConnectionContext.Connection;

        public override QueryFactory Factory => ConnectionContext.Factory;

        public TableConnectionContext(
            BaseConnectionContext<TConnection> connectionContext
        )
        {
            ConnectionContext = connectionContext;

            TableName = connectionContext.Schema.TableName<TTable>();
            Columns = connectionContext.Schema.ColumnMap[typeof(TTable)];
        }

        public Query Query() => Query<TTable>();

        public async Task<TTable> SelectOrInsert(
            Func<Query, Query> reader,
            Func<TTable> itemCreator
        ) =>
            await SelectOrInsert<TTable>(reader, itemCreator)
                .ConfigureAwait(false);

        public async Task<int> Insert(
            TTable entry,
            Predicate<DatabaseColumnContext> columnSelector = null,
            bool returnInsertedId = false,
            IDbTransaction transaction = null
        )
        {
            await using var command = Connection.CreateCommand();
            var allColumns = 
                Schema.ColumnMap[typeof(TTable)]?
                .Where(col => col.Column.IsInserted);

            var columns = columnSelector != null
                ? allColumns.Where(col => columnSelector(col))
                : allColumns;

            var columnNames = string.Join(",", columns.Select(col => col.Name));
            var columnValues = new Dictionary<string, object>();

            entry.Save(
                columns,
                (name, value) =>
                    columnValues.TryAdd(name, value)
            );

            return returnInsertedId
                ? await Query<TTable>()
                    .InsertGetIdAsync<int>(
                        columnValues,
                        transaction: transaction
                    )
                    .ConfigureAwait(false)
                : await Query<TTable>()
                    .InsertAsync(columnValues, transaction: transaction)
                    .ConfigureAwait(false);
        }

        public async Task<int> Insert(
            IEnumerable<TTable> entries,
            IDbTransaction transaction = null
        ) =>
            await Query().InsertAsync(entries, transaction: transaction);

        public async Task<int> UpdateWhere(
            Func<Query, Query> queryProcessor,
            TTable entry
        ) =>
            await queryProcessor(Query())
                .UpdateAsync(entry)
                .ConfigureAwait(false);

#region Select

        public async Task<TTable> SelectFirst(Func<Query, Query> processor) =>
            await SelectFirst<TTable, TTable>(processor)
                .ConfigureAwait(false);

        public async Task<TTable> SelectFirstOrDefault(
            Func<Query, Query> processor
        ) =>
            await SelectFirstOrDefault<TTable, TTable>(processor)
                .ConfigureAwait(false);

        public async Task<IEnumerable<TTable>> Select(
            Func<Query, Query> processor
        ) =>
            await Select<TTable, TTable>(processor)
                .ConfigureAwait(false);

#endregion

        public async Task<int> Delete(Func<Query, Query> processor) =>
            await processor(Query())
                .DeleteAsync()
                .ConfigureAwait(false);

        public void Dispose() => ConnectionContext.Dispose();

        public async ValueTask DisposeAsync() =>
            await ConnectionContext.DisposeAsync();
    }
}