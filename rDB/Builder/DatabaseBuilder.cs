using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using ColumnSet = System.Collections.Immutable.IImmutableSet<rDB.DatabaseColumnContext>;
using ColumnMap = System.Collections.Immutable.ImmutableDictionary<System.Type, System.Collections.Immutable.ImmutableHashSet<rDB.DatabaseColumnContext>>;
using TypeMap = System.Collections.Immutable.ImmutableDictionary<System.Type, string>;
using System.Collections.Immutable;

namespace rDB.Builder
{
    public class DatabaseBuilder<TDatabase, TConnection> 
        where TDatabase : Database<TConnection> 
        where TConnection : DbConnection
    {
        private TDatabase _database;
        private bool _createTables = true;
        private TypeMap _typeMap;
        private Dictionary<Type, DatabaseEntry> _tableMap = new Dictionary<Type, DatabaseEntry>();
        private readonly List<KeyValuePair<Type, ColumnSet>> _tables = new List<KeyValuePair<Type, ColumnSet>>();

        public DatabaseBuilder(TDatabase database)
        {
            _database = database;
        }

        public DatabaseBuilder()
        { 
           
        }

        public DatabaseBuilder<TDatabase, TConnection> WithDatabase(TDatabase database)
        {
            _database = database;
            return this;
        }

        public DatabaseBuilder<TDatabase, TConnection> CreateTables(bool state)
        {
            _createTables = state;
            return this;
        }

        public DatabaseBuilder<TDatabase, TConnection> WithTypeMap(TypeMap map)
        {
            _typeMap = map;
            return this;
        }

        public DatabaseBuilder<TDatabase, TConnection> WithTable(params DatabaseEntry[] tables)
        {
            WithTable((IEnumerable<DatabaseEntry>) tables);
            return this;
        }

        public DatabaseBuilder<TDatabase, TConnection> WithTable(IEnumerable<DatabaseEntry> tables)
        {
            foreach (var table in tables)
            {
                var type = table.GetType();
                if (_tableMap.ContainsKey(type))
                    throw new InvalidOperationException("The specified table has already been added");

                _tableMap.Add(type, table);
                _tables.Add(new KeyValuePair<Type, IImmutableSet<DatabaseColumnContext>>(type, (IImmutableSet<DatabaseColumnContext>) DatabaseEntry.GetColumns(type)));
            }

            return this;
        }

        private IEnumerable<DatabaseEntry> GetTablesInCreationOrder()
        {
            var graph = new DependencyGraph<Type>();
            foreach (var entry in _tableMap)
            {
                var dependencies = entry.Value.GetForeignKeys()
                    .Select(entry => entry.Key.Table);

                graph.Add(entry.Key, dependencies);
            }

            foreach (var entry in graph.Solve())
                yield return _tableMap[entry];
        }

        public async Task<TDatabase> Build()
        {
            _typeMap ??= DatabaseEntry.BuildTypeMap();

            _database.Configure(_typeMap, ImmutableDictionary.CreateRange(_tables));

            if (_createTables)
            {
                var tables = GetTablesInCreationOrder().ToArray();
                await _database.CreateTables(tables);
            }

            return _database;
        }
    }
}
