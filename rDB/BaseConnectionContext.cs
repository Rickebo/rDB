using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using SqlKata;
using SqlKata.Compilers;
using SqlKata.Execution;

namespace rDB
{
    public abstract class BaseConnectionContext<TConnection>
        : IDisposable, IAsyncDisposable
        where TConnection : DbConnection
    {
        public abstract SchemaContext Schema { get; }
        public abstract TConnection Connection { get; }
        public abstract QueryFactory Factory { get; }

        public virtual TableConnectionContext<TTable, TConnection>
            Table<TTable>()
            where TTable : DatabaseEntry
        {
            return Table<TableConnectionContext<TTable, TConnection>, TTable>(
                context =>
                    new TableConnectionContext<TTable, TConnection>(context)
            );
        }

        protected virtual TContext Table<TContext, TTable>(
            Func<BaseConnectionContext<TConnection>, TContext> constructor
        )
            where TContext : TableConnectionContext<TTable, TConnection>
            where TTable : DatabaseEntry
        {
            return constructor(this);
        }

        public virtual async Task<TTable> SelectOrInsert<TTable>(
            Func<Query, Query> reader,
            Func<TTable> itemCreator,
            IDbTransaction transaction = null,
            CancellationToken cancellationToken = default
        )
            where TTable : DatabaseEntry
        {
            var result = await reader(Query<TTable>())
                .FirstOrDefaultAsync<TTable>(
                    transaction,
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false);

            if (result != null)
                return result;

            await Insert(itemCreator(), transaction: transaction)
                .ConfigureAwait(false);

            return await reader(Query<TTable>())
                .FirstOrDefaultAsync<TTable>(
                    transaction,
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false);
        }

        public virtual async Task<TSelect> SelectOrInsert<TTable, TSelect>(
            Func<Query, Query> reader,
            Func<TTable> itemCreator,
            IDbTransaction transaction = null,
            CancellationToken cancellationToken = default
        )
            where TTable : DatabaseEntry
        {
            var result = await reader(Query<TTable>())
                .FirstOrDefaultAsync<TSelect>(
                    transaction,
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false);

            if (result != null)
                return result;

            await Insert(
                    itemCreator(),
                    transaction: transaction,
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false);

            return await reader(Query<TTable>())
                .FirstOrDefaultAsync<TSelect>(
                    transaction,
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false);
        }

        public string TableName<TTable>() where TTable : DatabaseEntry
        {
            return Schema.TableName<TTable>();
        }

        public async Task<int> Insert<TTable>(
            TTable entry,
            Predicate<DatabaseColumnContext> columnSelector = null,
            bool returnInsertedId = false,
            IDbTransaction transaction = null,
            bool ignoreConflict = false,
            CancellationToken cancellationToken = default
        )
            where TTable : DatabaseEntry
        {
            return await Insert(
                    entry,
                    columnSelector,
                    returnInsertedId,
                    transaction,
                    ignoreConflict: ignoreConflict,
                    idConverter: Convert.ToInt32,
                    nonIdConverter: i => i,
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false);
        }

        public async Task<long> InsertLong<TTable>(
            TTable entry,
            Predicate<DatabaseColumnContext> columnSelector = null,
            bool returnInsertedId = false,
            IDbTransaction transaction = null,
            bool ignoreConflict = false,
            CancellationToken cancellationToken = default
        )
            where TTable : DatabaseEntry
        {
            return await Insert(
                    entry,
                    columnSelector,
                    returnInsertedId,
                    transaction,
                    ignoreConflict: ignoreConflict,
                    idConverter: Convert.ToInt64,
                    nonIdConverter: i => i,
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false);
        }

        public virtual async Task<TId> Insert<TTable, TId>(
            TTable entry,
            Predicate<DatabaseColumnContext> columnSelector = null,
            bool returnInsertedId = false,
            IDbTransaction transaction = null,
            Func<object, TId> idConverter = null,
            Func<int, TId> nonIdConverter = null,
            bool ignoreConflict = false,
            CancellationToken cancellationToken = default
        )
            where TTable : DatabaseEntry
        {
            var allColumns = Schema.ColumnMap[typeof(TTable)]
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

            var query = Query<TTable>()
                .AsInsert(columnValues);

            var compiler = Factory.Compiler;
            var compiled = compiler.Compile(query);
            var commandText = compiled.Sql;

            if (ignoreConflict)
                switch (compiler)
                {
                    case MySqlCompiler _:
                        const string insert = "INSERT";
                        if (commandText.StartsWith(insert))
                            commandText = commandText.Insert(
                                insert.Length,
                                " IGNORE"
                            );

                        break;

                    case PostgresCompiler _:
                        commandText += " ON CONFLICT DO NOTHING";

                        break;

                    default:
                        throw new InvalidOperationException(
                            "Cannot ignore conflicts for the current DBMS."
                        );
                }

            var commandDefinition = new CommandDefinition(
                commandText,
                compiled.NamedBindings,
                transaction,
                Factory.QueryTimeout,
                cancellationToken: cancellationToken
            );

            if ((!returnInsertedId || idConverter != null) &&
                (returnInsertedId || nonIdConverter != null))
                return returnInsertedId
                    ? idConverter(
                        await Connection.ExecuteScalarAsync(commandDefinition)
                            .ConfigureAwait(false)
                    )
                    : nonIdConverter(
                        await Connection.ExecuteAsync(commandDefinition)
                            .ConfigureAwait(false)
                    );

            await Connection.ExecuteAsync(commandDefinition)
                .ConfigureAwait(false);
            return default;
        }

        public async Task<bool> Exists<TTable>(
            Func<Query, Query> queryProcessor,
            IDbTransaction transaction = null,
            CancellationToken cancellationToken = default
        )
            where TTable : DatabaseEntry
        {
            return await queryProcessor(Query<TTable>().SelectRaw("1").Limit(1))
                       .CountAsync<int>(
                           transaction: transaction,
                           cancellationToken: cancellationToken
                       )
                       .ConfigureAwait(false) >
                   0;
        }

        public Query Query<TTable>() where TTable : DatabaseEntry
        {
            return Factory.Query(Schema.TableName<TTable>());
        }

        public async Task<T> Query<T, TTable>(Func<Query, Task<T>> selector)
            where TTable : DatabaseEntry
        {
            var query = Factory.Query(Schema.TableName<TTable>());
            return await selector(query)
                .ConfigureAwait(false);
        }

        public async Task<T> Query<T, TTable>(
            Func<Query, IDbTransaction, CancellationToken, Task<T>> selector,
            IDbTransaction transaction = null,
            CancellationToken cancellationToken = default
        )
            where TTable : DatabaseEntry
        {
            var query = Factory.Query(Schema.TableName<TTable>());
            return await selector(query, transaction, cancellationToken)
                .ConfigureAwait(false);
        }

        public string Column<TTable>(
            string columnName,
            bool cite = false,
            bool citeLeft = false,
            bool citeRight = false
        ) where TTable : DatabaseEntry
        {
            return Cite(TableName<TTable>(), cite || citeLeft) +
                   "." +
                   Cite(columnName, cite || citeRight);
        }

        private static string Cite(string text, bool cite)
        {
            return cite
                ? "\"" + text + "\""
                : text;
        }

        #region Select generic

        public virtual async Task<TTable> SelectFirst<TTable>(
            Func<Query, Query> processor,
            IDbTransaction transaction = null,
            CancellationToken cancellationToken = default
        )
            where TTable : DatabaseEntry
        {
            return await SelectFirst<TTable, TTable>(
                    processor,
                    transaction,
                    cancellationToken
                )
                .ConfigureAwait(false);
        }

        public virtual async Task<T> SelectFirst<T, TTable>(
            Func<Query, Query> processor,
            IDbTransaction transaction = null,
            CancellationToken cancellationToken = default
        )
            where TTable : DatabaseEntry
        {
            return await processor(Query<TTable>())
                .FirstAsync<T>(
                    transaction,
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false);
        }

        public virtual async Task<TTable> SelectFirstOrDefault<TTable>(
            Func<Query, Query> processor,
            IDbTransaction transaction = null,
            CancellationToken cancellationToken = default
        )
            where TTable : DatabaseEntry
        {
            return await SelectFirstOrDefault<TTable, TTable>(
                    processor,
                    transaction
                )
                .ConfigureAwait(false);
        }

        public virtual async Task<T> SelectFirstOrDefault<T, TTable>(
            Func<Query, Query> processor,
            IDbTransaction transaction = null,
            CancellationToken cancellationToken = default
        )
            where TTable : DatabaseEntry
        {
            return await processor(Query<TTable>())
                .FirstOrDefaultAsync<T>(
                    transaction,
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false);
        }


        public virtual async Task<IEnumerable<TTable>> Select<TTable>(
            Func<Query, Query> processor,
            IDbTransaction transaction = null,
            CancellationToken cancellationToken = default
        )
            where TTable : DatabaseEntry
        {
            return await Select<TTable, TTable>(
                    processor,
                    transaction,
                    cancellationToken
                )
                .ConfigureAwait(false);
        }

        public virtual async Task<IEnumerable<T>> Select<T, TTable>(
            Func<Query, Query> processor,
            IDbTransaction transaction = null,
            CancellationToken cancellationToken = default
        )
            where TTable : DatabaseEntry
        {
            return await processor(Query<TTable>())
                .GetAsync<T>(
                    transaction,
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false);
        }

        public virtual async IAsyncEnumerable<TTable> Paginate<TTable>(
            Func<Query, Query> processor,
            int page = 0,
            int pageSize = 32,
            IDbTransaction transaction = null,
            [EnumeratorCancellation]
            CancellationToken cancellationToken = default
        ) where TTable : DatabaseEntry
        {
            await foreach (var item in Paginate<TTable, TTable>(
                               processor,
                               page,
                               pageSize,
                               transaction,
                               cancellationToken
                           ))
                yield return item;
        }

        public virtual async IAsyncEnumerable<TTable> Paginate<TTable>(
            Query query,
            int page = 0,
            int pageSize = 32,
            IDbTransaction transaction = null,
            [EnumeratorCancellation]
            CancellationToken cancellationToken = default
        ) where TTable : DatabaseEntry
        {
            await foreach (var item in Paginate<TTable, TTable>(
                               query,
                               page,
                               pageSize,
                               transaction,
                               cancellationToken
                           ))
                yield return item;
        }

        public virtual async IAsyncEnumerable<T> Paginate<T, TTable>(
            Func<Query, Query> processor,
            int page = 0,
            int pageSize = 32,
            IDbTransaction transaction = null,
            [EnumeratorCancellation]
            CancellationToken cancellationToken = default
        ) where TTable : DatabaseEntry
        {
            var pagination = await processor(Query<TTable>())
                .PaginateAsync<T>(
                    page,
                    pageSize,
                    transaction,
                    cancellationToken: cancellationToken
                );

            foreach (var paginationPage in pagination.Each)
            foreach (var item in paginationPage.List)
                yield return item;
        }

        public virtual async IAsyncEnumerable<T> Paginate<T, TTable>(
            Query query,
            int page = 0,
            int pageSize = 32,
            IDbTransaction transaction = null,
            [EnumeratorCancellation]
            CancellationToken cancellationToken = default
        ) where TTable : DatabaseEntry
        {
            var pagination = await query
                .PaginateAsync<T>(
                    page,
                    pageSize,
                    transaction,
                    cancellationToken: cancellationToken
                );

            foreach (var paginationPage in pagination.Each)
            foreach (var item in paginationPage.List)
                yield return item;
        }

        #endregion

        #region Select reader

        public virtual async Task<TTable> SelectFirstReader<TTable>(
            Func<Query, Query> processor,
            IDbTransaction transaction = null,
            CancellationToken cancellationToken = default
        )
            where TTable : DatabaseEntry
        {
            return await SelectFirst<TTable, TTable>(
                    processor,
                    transaction,
                    cancellationToken
                )
                .ConfigureAwait(false);
        }

        public virtual async Task<T> SelectFirstReader<T, TTable>(
            Func<Query, Query> processor,
            IDbTransaction transaction = null,
            CancellationToken cancellationToken = default
        )
            where TTable : DatabaseEntry
        {
            return await processor(Query<TTable>())
                .FirstAsync<T>(
                    transaction,
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false);
        }

        public virtual async Task<TTable> SelectFirstOrDefaultReader<TTable>(
            Func<Query, Query> processor,
            IDbTransaction transaction = null,
            CancellationToken cancellationToken = default
        )
            where TTable : DatabaseEntry
        {
            return await SelectFirstOrDefault<TTable, TTable>(
                    processor,
                    transaction,
                    cancellationToken
                )
                .ConfigureAwait(false);
        }

        //public virtual async Task<DbDataReader> SelectFirstOrDefaultReader<TTable>(Func<Query, Query> processor)
        //    where TTable : DatabaseEntry =>
        //    await processor(Query<TTable>()).Reader<T>()
        //        .ConfigureAwait(false);


        public virtual async Task<IEnumerable<TTable>> SelectReader<TTable>(
            Func<Query, Query> processor,
            IDbTransaction transaction = null,
            CancellationToken cancellationToken = default
        )
            where TTable : DatabaseEntry
        {
            return await SelectReader<TTable, TTable>(
                    processor,
                    transaction,
                    cancellationToken
                )
                .ConfigureAwait(false);
        }

        public virtual async Task<IEnumerable<T>> SelectReader<T, TTable>(
            Func<Query, Query> processor,
            IDbTransaction transaction = null,
            CancellationToken cancellationToken = default
        )
            where TTable : DatabaseEntry
        {
            return await processor(Query<TTable>())
                .GetAsync<T>(
                    transaction,
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false);
        }

        #endregion

        #region Disposable

        public virtual void Dispose()
        {
            Factory?.Dispose();
            Connection.Dispose();
        }

        public virtual async ValueTask DisposeAsync()
        {
            Factory?.Dispose();
            await Connection.DisposeAsync();
        }

        #endregion
    }
}