using SqlKata.Compilers;

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data.Common;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using SqlKata;
using System.Collections.Concurrent;

using ColumnSet = System.Collections.Immutable.IImmutableSet<rDB.DatabaseColumnContext>;
using ColumnMap = System.Collections.Immutable.IImmutableDictionary<System.Type, System.Collections.Immutable.IImmutableSet<rDB.DatabaseColumnContext>>;
using TypeMap = System.Collections.Immutable.IImmutableDictionary<System.Type, string>;

namespace rDB
{
    public abstract class Database<TConnection> 
        where TConnection : DbConnection
    {
        private AtomicBoolean _isConfigured = new AtomicBoolean(false);

        private volatile int _openConnections = 0;
        public int OpenConnections => _openConnections;

        private ConcurrentDictionary<TConnection, object> _connections { get; } = new ConcurrentDictionary<TConnection, object>();
        public ICollection<TConnection> Connections => _connections.Keys;

        protected SchemaContext Schema { get; private set; }

        protected Compiler SqlCompiler { get; private set; }

        protected Database(Compiler compiler)
        {
            SqlCompiler = compiler;
        }

        public void Configure(TypeMap typeMap, ColumnMap tableColumnMap)
        {
            if (!_isConfigured.Set(true))
                throw new InvalidOperationException("Cannot configure already configured database.");

            Schema = new SchemaContext(tableColumnMap, typeMap);
        }

        protected abstract Task<TConnection> GetConnection();

        public virtual async Task<ConnectionContext<TConnection>> GetConnectionContext() =>
            await GetConnectionContext(() => CreateConnectionContext())
            .ConfigureAwait(false);

        public virtual async Task<TContext> GetConnectionContext<TContext>(Func<Task<TContext>> contextConstructor)
            where TContext : ConnectionContext<TConnection>
        {
            var context = await contextConstructor()
                .ConfigureAwait(false);
            
            context.OnDispose += (s, a) =>
            {
                _connections.TryRemove(a.Context.Connection, out _);
                Interlocked.Decrement(ref _openConnections);
            };

            _connections.TryAdd(context.Connection, null);
            Interlocked.Increment(ref _openConnections);

            return context;
        }

        public string TableName<TTable>() where TTable : DatabaseEntry =>
            Schema.TableName<TTable>();

        private async Task<ConnectionContext<TConnection>> CreateConnectionContext() =>
            new ConnectionContext<TConnection>(await GetConnection().ConfigureAwait(false), SqlCompiler, Schema);

        protected virtual async Task<TContext> Table<TContext, TTable>(Func<ConnectionContext<TConnection>, TContext> constructor)
           where TContext : TableConnectionContext<TTable, TConnection>
           where TTable : DatabaseEntry =>
            constructor(await GetConnectionContext().ConfigureAwait(false));


        public virtual async Task<TableConnectionContext<TTable, TConnection>> Table<TTable>()
            where TTable : DatabaseEntry =>
            await Table<TableConnectionContext<TTable, TConnection>, TTable>(context =>
                new TableConnectionContext<TTable, TConnection>(context))
                .ConfigureAwait(false);


        public async Task<TResult> Select<TTable, TResult>(Func<Query, Task<TResult>> selector) 
            where TTable : DatabaseEntry
        {
            await using var context = await Table<TTable>();
            var query = context.Query<TTable>();

            return await selector(query);
        }

        public async Task<bool> DropTable<T>() where T : DatabaseEntry =>
            await DropTable(typeof(T));
        
        public async Task<bool> DropTable(Type type) =>
            await Execute("DROP TABLE IF EXISTS @1", new Parameter("1", type))
                .ConfigureAwait(false) > 0;

        public async Task<int> DropTables()
        {
            var sum = 0;
            foreach (var type in Schema.ColumnMap.Keys)
                sum += await DropTable(type).ConfigureAwait(false) ? 1 : 0;

            return sum;
        }

        public async Task<int> Execute(string sql, params Parameter[] parameters)
        {
            await using var connection = await GetConnection()
                .ConfigureAwait(false);
            
            await using var command = connection.CreateCommand();

            command.CommandText = sql;

            foreach (var parameter in parameters)
            {
                var sqlParameter = command.CreateParameter();

                sqlParameter.ParameterName = parameter.Name;
                sqlParameter.Value = parameter is Type type
                    ? Schema.TypeMap[type]
                    : parameter.Value;

                command.Parameters.Add(sqlParameter);
            }

            return await command.ExecuteNonQueryAsync()
                .ConfigureAwait(false);
        }

        public async Task<bool> CreateTable<T>() where T : DatabaseEntry, new() =>
            await CreateTable(new T())
                .ConfigureAwait(false);

        public async Task<bool> CreateTable(DatabaseEntry instance) 
        {
            var sql = TableSqlBuilder
                .Create(
                    Schema.TypeMap, 
                    Schema.ColumnMap.TryGetValue(instance.GetType(), out var colSet) 
                        ? colSet
                        : null, 
                    instance)
                .Build();

            return await Execute(sql).ConfigureAwait(false) > 0;
        }

        public async Task<int> CreateTables(params DatabaseEntry[] entries) => 
            await CreateTables((IEnumerable<DatabaseEntry>) entries)
                .ConfigureAwait(false);

        public async Task<int> CreateTables(IEnumerable<DatabaseEntry> entries)
        {
            var state = 0;
            foreach (var entry in entries)
            {
                if (await CreateTable(entry).ConfigureAwait(false))
                    state++;
            }
            
            return state;
        }

        public class Parameter
        {
            public string Name { get; }
            public object Value { get; }

            public Parameter(string name, object value)
            {
                Name = name;
                Value = value;
            }
        }

        private class AtomicBoolean
        {
            private volatile bool _state;

            public AtomicBoolean(bool state)
            {
                _state = state;
            }

            /// <summary>
            /// Sets the state to the specified value, and returns true if it was updated.
            /// </summary>
            /// <returns>Whether or not the state was updated</returns>
            public bool Set(bool newState)
            {
                lock (this)
                {
                    var isUpdate = _state != newState;
                    _state = newState;

                    return isUpdate;
                }
            }

            public bool Get()
            {
                lock(this)
                    return _state;
            }
        }
    }
}
