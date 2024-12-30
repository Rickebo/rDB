using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using SqlKata;
using SqlKata.Execution;
using ColumnSet =
    System.Collections.Immutable.ImmutableHashSet<rDB.DatabaseColumnContext>;
using ColumnMap =
    System.Collections.Immutable.ImmutableDictionary<System.Type, System.Collections.
        Immutable.ImmutableHashSet<rDB.DatabaseColumnContext>>;
using TypeMap = System.Collections.Immutable.ImmutableDictionary<System.Type, string>;

namespace rDB
{
    public class TableConnectionContext<TTable, TConnection>
        : BaseConnectionContext<TConnection>
        where TConnection : DbConnection
        where TTable : DatabaseEntry
    {
        protected bool DisposeConnection { get; }

        public TableConnectionContext(
            BaseConnectionContext<TConnection> connectionContext,
            bool disposeConnection = false
        )
        {
            ConnectionContext = connectionContext;
            DisposeConnection = disposeConnection;

            TableName = connectionContext.Schema.TableName<TTable>();
            Columns = connectionContext.Schema.ColumnMap[typeof(TTable)];
        }

        private BaseConnectionContext<TConnection> ConnectionContext { get; }
        public string TableName { get; }
        public IImmutableSet<DatabaseColumnContext> Columns { get; }

        public override SchemaContext Schema => ConnectionContext.Schema;

        public override TConnection Connection => ConnectionContext.Connection;

        public override QueryFactory Factory => ConnectionContext.Factory;

        public Query Query()
        {
            return Query<TTable>();
        }

        public async Task<TTable> SelectOrInsert(
            Func<Query, Query> reader,
            Func<TTable> itemCreator
        )
        {
            return await SelectOrInsert<TTable>(reader, itemCreator)
                .ConfigureAwait(false);
        }

        public async Task<int> Insert(
            TTable entry,
            Predicate<DatabaseColumnContext> columnSelector = null,
            bool returnInsertedId = false,
            IDbTransaction transaction = null
        )
        {
            await using var command = Connection.CreateCommand();
            var allColumns =
                Schema.ColumnMap[typeof(TTable)]
                    ?
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
                        transaction
                    )
                    .ConfigureAwait(false)
                : await Query<TTable>()
                    .InsertAsync(columnValues, transaction)
                    .ConfigureAwait(false);
        }

        public async Task<int> Insert(
            IEnumerable<TTable> entries,
            IDbTransaction transaction = null
        )
        {
            return await Query().InsertAsync(entries, transaction);
        }

        public async Task<int> UpdateWhere(
            Func<Query, Query> queryProcessor,
            TTable entry,
<<<<<<< HEAD
=======
            IDbTransaction transaction = null,
>>>>>>> 3f1c5cb (Added transaction & cancellation to UpdateWhere)
            CancellationToken cancellationToken = default
        )
        {
            return await queryProcessor(Query())
<<<<<<< HEAD
                .UpdateAsync(entry, cancellationToken: cancellationToken)
=======
                .UpdateAsync(
                    entry,
                    transaction: transaction,
                    cancellationToken: cancellationToken
                )
>>>>>>> 3f1c5cb (Added transaction & cancellation to UpdateWhere)
                .ConfigureAwait(false);
        }

        public virtual async Task<int> Delete(
            Func<Query, Query> processor,
            IDbTransaction transaction = null,
            CancellationToken cancellationToken = default
        )
        {
            return await processor(Query())
                .DeleteAsync(
                    transaction,
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false);
        }

        #region Select

        public async Task<TTable> SelectFirst(Func<Query, Query> processor)
        {
            return await SelectFirst<TTable, TTable>(processor)
                .ConfigureAwait(false);
        }

        public async Task<TTable> SelectFirstOrDefault(Func<Query, Query> processor)
        {
            return await SelectFirstOrDefault<TTable, TTable>(processor)
                .ConfigureAwait(false);
        }

        public async Task<IEnumerable<TTable>> Select(Func<Query, Query> processor)
        {
            return await Select<TTable, TTable>(processor)
                .ConfigureAwait(false);
        }

        #endregion

        public override ValueTask DisposeAsync()
        {
            return new ValueTask();
        }

        public override void Dispose()
        {
        }
    }
}