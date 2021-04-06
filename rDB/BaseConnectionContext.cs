using SqlKata;
using SqlKata.Execution;

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

        protected virtual TContext Table<TContext, TTable>(Func<BaseConnectionContext<TConnection>, TContext> constructor)
           where TContext : TableConnectionContext<TTable, TConnection>
           where TTable : DatabaseEntry =>
            constructor(this);

        public async Task<TTable> SelectOrInsert<TTable>(Func<Query, Query> reader, Func<TTable> itemCreator)
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

        public string TableName<TTable>() where TTable : DatabaseEntry =>
            Schema.TableName<TTable>();

        public async Task<int> Insert<TTable>(TTable entry, Predicate<DatabaseColumnContext> columnSelector = null)
            where TTable : DatabaseEntry
        {
            await using var command = Connection.CreateCommand();
            var allColumns = Schema.ColumnMap[typeof(TTable)];
            var columns = columnSelector != null
                ? allColumns.Where(col => columnSelector(col))
                : allColumns;

            var columnNames = string.Join(",", columns.Select(col => col.Name));
            var columnParameterNames = string.Join(",", columns.Select(col => "@" + col.Name));

            command.CommandText = $"INSERT INTO {Schema.TableName<TTable>()} ({columnNames}) VALUES ({columnParameterNames})";

            entry.Save(columns, (name, value) =>
            {
                var parameter = command.CreateParameter();

                parameter.ParameterName = name;
                parameter.Value = value ?? DBNull.Value;

                command.Parameters.Add(parameter);
            });

            return await command.ExecuteNonQueryAsync()
                .ConfigureAwait(false);
        }

        public Query Query<TTable>() where TTable : DatabaseEntry =>
            Factory.Query(Schema.TableName<TTable>());

        public async Task<T> Query<T, TTable>(Func<Query, Task<T>> selector) where TTable : DatabaseEntry
        {
            var query = Factory.Query(Schema.TableName<TTable>());
            return await selector(query)
                .ConfigureAwait(false);
        }

        #region Select generic
        public async Task<TTable> SelectFirst<TTable>(Func<Query, Query> processor)
            where TTable : DatabaseEntry =>
                await SelectFirst<TTable, TTable>(processor).ConfigureAwait(false);

        public async Task<T> SelectFirst<T, TTable>(Func<Query, Query> processor)
            where TTable : DatabaseEntry =>
            await processor(Query<TTable>()).FirstAsync<T>()
                .ConfigureAwait(false);

        public async Task<TTable> SelectFirstOrDefault<TTable>(Func<Query, Query> processor)
            where TTable : DatabaseEntry =>
                await SelectFirstOrDefault<TTable, TTable>(processor).ConfigureAwait(false);

        public async Task<T> SelectFirstOrDefault<T, TTable>(Func<Query, Query> processor)
            where TTable : DatabaseEntry =>
            await processor(Query<TTable>()).FirstOrDefaultAsync<T>()
                .ConfigureAwait(false);


        public async Task<IEnumerable<TTable>> Select<TTable>(Func<Query, Query> processor)
            where TTable : DatabaseEntry =>
                await Select<TTable, TTable>(processor).ConfigureAwait(false);

        public async Task<IEnumerable<T>> Select<T, TTable>(Func<Query, Query> processor)
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
