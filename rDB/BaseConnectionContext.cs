﻿using Serilog;
using Serilog.Core;
using SqlKata;
using SqlKata.Execution;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using SqlKata.Compilers;

namespace rDB
{
    public abstract class BaseConnectionContext<TConnection> : IDisposable, IAsyncDisposable
        where TConnection : DbConnection
    {
        public abstract SchemaContext Schema { get; }
        public abstract TConnection Connection { get; }
        public abstract QueryFactory Factory { get; }

        public virtual TableConnectionContext<TTable, TConnection> Table<TTable>()
            where TTable : DatabaseEntry =>
            Table<TableConnectionContext<TTable, TConnection>, TTable>(
                context => new TableConnectionContext<TTable, TConnection>(context));

        protected virtual TContext Table<TContext, TTable>(
            Func<BaseConnectionContext<TConnection>, TContext> constructor)
            where TContext : TableConnectionContext<TTable, TConnection>
            where TTable : DatabaseEntry =>
            constructor(this);

        public virtual async Task<TTable> SelectOrInsert<TTable>(Func<Query, Query> reader, Func<TTable> itemCreator)
            where TTable : DatabaseEntry
        {
            var result = await reader(Query<TTable>())
                .FirstOrDefaultAsync<TTable>()
                .ConfigureAwait(false);

            if (result != null)
                return result;

            await Insert(itemCreator())
                .ConfigureAwait(false);

            return await reader(Query<TTable>())
                .FirstOrDefaultAsync<TTable>()
                .ConfigureAwait(false);
        }

        public virtual async Task<TSelect> SelectOrInsert<TTable, TSelect>(Func<Query, Query> reader,
            Func<TTable> itemCreator)
            where TTable : DatabaseEntry
        {
            var result = await reader(Query<TTable>())
                .FirstOrDefaultAsync<TSelect>()
                .ConfigureAwait(false);

            if (result != null)
                return result;

            await Insert(itemCreator())
                .ConfigureAwait(false);

            return await reader(Query<TTable>())
                .FirstOrDefaultAsync<TSelect>()
                .ConfigureAwait(false);
        }

        public string TableName<TTable>() where TTable : DatabaseEntry =>
            Schema.TableName<TTable>();

        public async Task<int> Insert<TTable>(TTable entry,
            Predicate<DatabaseColumnContext> columnSelector = null,
            bool returnInsertedId = false,
            IDbTransaction transaction = null,
            bool ignoreConflict = false)
            where TTable : DatabaseEntry
            => await Insert<TTable, int>(
                    entry: entry, 
                    columnSelector: columnSelector, 
                    returnInsertedId: returnInsertedId, 
                    transaction: transaction,
                    ignoreConflict: ignoreConflict,
                    idConverter: Convert.ToInt32,
                    nonIdConverter: i => i)
                .ConfigureAwait(false);

        public async Task<long> InsertLong<TTable>(TTable entry,
            Predicate<DatabaseColumnContext> columnSelector = null,
            bool returnInsertedId = false,
            IDbTransaction transaction = null,
            bool ignoreConflict = false)
            where TTable : DatabaseEntry
            => await Insert<TTable, long>(
                    entry: entry, 
                    columnSelector: columnSelector, 
                    returnInsertedId: returnInsertedId, 
                    transaction: transaction, 
                    ignoreConflict: ignoreConflict,
                    idConverter: Convert.ToInt64,
                    nonIdConverter: i => i)
                .ConfigureAwait(false);

        public virtual async Task<TId> Insert<TTable, TId>(TTable entry,
            Predicate<DatabaseColumnContext> columnSelector = null,
            bool returnInsertedId = false,
            IDbTransaction transaction = null,
            Func<object, TId> idConverter = null,
            Func<int, TId> nonIdConverter = null,
            bool ignoreConflict = false)
            where TTable : DatabaseEntry
        {
            var allColumns = Schema.ColumnMap[typeof(TTable)]?
                .Where(col => col.Column.IsInserted);

            var columns = columnSelector != null
                ? allColumns.Where(col => columnSelector(col))
                : allColumns;

            var columnNames = string.Join(",", columns.Select(col => col.Name));
            var columnValues = new Dictionary<string, object>();

            entry.Save(columns, (name, value) =>
                columnValues.TryAdd(name, value));

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
                            commandText = commandText.Insert(insert.Length, " IGNORE");
                        
                        break;
                    
                    case PostgresCompiler _:
                        commandText += " ON CONFLICT DO NOTHING";
                        
                        break;
                    
                    default:
                        throw new InvalidOperationException(
                            "Cannot ignore conflicts for the current DBMS.");
                }

            var commandDefinition = new CommandDefinition(
                commandText: commandText,
                parameters: compiled.NamedBindings,
                transaction: transaction,
                commandTimeout: Factory.QueryTimeout
            );

            if ((!returnInsertedId || idConverter != null) && (returnInsertedId || nonIdConverter != null))
                return returnInsertedId
                    ? idConverter(await Connection.ExecuteScalarAsync(commandDefinition).ConfigureAwait(false))
                    : nonIdConverter(await Connection.ExecuteAsync(commandDefinition).ConfigureAwait(false));
            
            await Connection.ExecuteAsync(commandDefinition).ConfigureAwait(false);
            return default;
        }

        public async Task<bool> Exists<TTable>(Func<Query, Query> queryProcessor)
            where TTable : DatabaseEntry =>
            await queryProcessor(Query<TTable>().SelectRaw("1").Limit(1)).CountAsync<int>().ConfigureAwait(false) > 0;

        public Query Query<TTable>() where TTable : DatabaseEntry =>
            Factory.Query(Schema.TableName<TTable>());

        public async Task<T> Query<T, TTable>(Func<Query, Task<T>> selector) where TTable : DatabaseEntry
        {
            var query = Factory.Query(Schema.TableName<TTable>());
            return await selector(query)
                .ConfigureAwait(false);
        }

        public string Column<TTable>(string columnName, bool cite = false, bool citeLeft = false,
            bool citeRight = false) where TTable : DatabaseEntry =>
            Cite(TableName<TTable>(), cite || citeLeft) + "." + Cite(columnName, cite || citeRight);

        private static string Cite(string text, bool cite) => cite
            ? "\"" + text + "\""
            : text;

        #region Select generic

        public virtual async Task<TTable> SelectFirst<TTable>(Func<Query, Query> processor)
            where TTable : DatabaseEntry =>
            await SelectFirst<TTable, TTable>(processor).ConfigureAwait(false);

        public virtual async Task<T> SelectFirst<T, TTable>(Func<Query, Query> processor)
            where TTable : DatabaseEntry =>
            await processor(Query<TTable>()).FirstAsync<T>()
                .ConfigureAwait(false);

        public virtual async Task<TTable> SelectFirstOrDefault<TTable>(Func<Query, Query> processor)
            where TTable : DatabaseEntry =>
            await SelectFirstOrDefault<TTable, TTable>(processor).ConfigureAwait(false);

        public virtual async Task<T> SelectFirstOrDefault<T, TTable>(Func<Query, Query> processor)
            where TTable : DatabaseEntry =>
            await processor(Query<TTable>()).FirstOrDefaultAsync<T>()
                .ConfigureAwait(false);


        public virtual async Task<IEnumerable<TTable>> Select<TTable>(Func<Query, Query> processor)
            where TTable : DatabaseEntry =>
            await Select<TTable, TTable>(processor).ConfigureAwait(false);

        public virtual async Task<IEnumerable<T>> Select<T, TTable>(Func<Query, Query> processor)
            where TTable : DatabaseEntry =>
            await processor(Query<TTable>()).GetAsync<T>()
                .ConfigureAwait(false);

        #endregion

        #region Select reader

        public virtual async Task<TTable> SelectFirstReader<TTable>(Func<Query, Query> processor)
            where TTable : DatabaseEntry =>
            await SelectFirst<TTable, TTable>(processor).ConfigureAwait(false);

        public virtual async Task<T> SelectFirstReader<T, TTable>(Func<Query, Query> processor)
            where TTable : DatabaseEntry =>
            await processor(Query<TTable>()).FirstAsync<T>()
                .ConfigureAwait(false);

        public virtual async Task<TTable> SelectFirstOrDefaultReader<TTable>(Func<Query, Query> processor)
            where TTable : DatabaseEntry =>
            await SelectFirstOrDefault<TTable, TTable>(processor).ConfigureAwait(false);

        //public virtual async Task<DbDataReader> SelectFirstOrDefaultReader<TTable>(Func<Query, Query> processor)
        //    where TTable : DatabaseEntry =>
        //    await processor(Query<TTable>()).Reader<T>()
        //        .ConfigureAwait(false);


        public virtual async Task<IEnumerable<TTable>> SelectReader<TTable>(Func<Query, Query> processor)
            where TTable : DatabaseEntry =>
            await SelectReader<TTable, TTable>(processor).ConfigureAwait(false);

        public virtual async Task<IEnumerable<T>> SelectReader<T, TTable>(Func<Query, Query> processor)
            where TTable : DatabaseEntry =>
            await processor(Query<TTable>()).GetAsync<T>()
                .ConfigureAwait(false);

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