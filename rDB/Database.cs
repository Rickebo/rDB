using SqlKata.Compilers;

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data.Common;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ColumnSet = System.Collections.Immutable.ImmutableHashSet<rDB.DatabaseColumnContext>;
using ColumnMap = System.Collections.Immutable.ImmutableDictionary<System.Type, System.Collections.Immutable.ImmutableHashSet<rDB.DatabaseColumnContext>>;
using TypeMap = System.Collections.Immutable.ImmutableDictionary<System.Type, string>;
using SqlKata;
using System.Collections.Concurrent;

namespace rDB
{
    public abstract class Database<TConnection> where TConnection : DbConnection
    {
        private AtomicBoolean _isConfigured = new AtomicBoolean(false);

        private volatile int _openConnections = 0;
        public int OpenConnections => _openConnections;

        private ConcurrentDictionary<TConnection, object> _connections { get; } = new ConcurrentDictionary<TConnection, object>();
        public ICollection<TConnection> Connections => _connections.Keys;


        private TypeMap _typeMap;
        private ColumnMap ColumnMap { get; set; }

        private Compiler _compiler;

        protected Database(Compiler compiler)
        {
            _compiler = compiler;
        }

        protected Database()
        {

        }

        public void Configure(TypeMap typeMap, ColumnMap tableColumnMap)
        {
            if (!_isConfigured.Set(true))
                throw new InvalidOperationException("Cannot configure already configured database.");

            _typeMap = typeMap;
            ColumnMap = tableColumnMap;
        }

        protected abstract Task<TConnection> GetConnection();

        public async Task<ConnectionContext<TConnection>> GetConnectionContext()
        {
            var context = new ConnectionContext<TConnection>(
                await GetConnection().ConfigureAwait(false), 
                _compiler,
                (connection, isAsync) => {
                    _connections.TryRemove(connection, out _);
                    Interlocked.Decrement(ref _openConnections);
                });

            _connections.TryAdd(context.Connection, null);
            Interlocked.Increment(ref _openConnections);

            return context;
        }
        
        public async Task<TableConnectionContext<T, TConnection>> Table<T>() where T : DatabaseEntry
        {
            if (!ColumnMap.TryGetValue(typeof(T), out var columns))
                throw new InvalidOperationException("Cannot access table which is not a part of this database.");

            var connectionContext = await GetConnectionContext();
            var name = _typeMap[typeof(T)];

            return new TableConnectionContext<T, TConnection>(
                name,
                columns,
                connectionContext
            );
        }

        public async Task<TResult> Select<TTable, TResult>(Func<Query, Task<TResult>> selector) 
            where TTable : DatabaseEntry
        {
            await using var context = await Table<TTable>();
            var query = context.Query();

            return await selector(query);
        }

        public async Task<bool> DropTable<T>() where T : DatabaseEntry =>
            await Execute("DROP TABLE IF EXISTS @1", new Parameter("1", typeof(T)))
                .ConfigureAwait(false) > 0;

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
                    ? _typeMap[type]
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
                    _typeMap, 
                    ColumnMap.TryGetValue(instance.GetType(), out var colSet) 
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
