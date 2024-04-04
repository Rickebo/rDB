using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using ColumnSet =
    System.Collections.Immutable.IImmutableSet<rDB.DatabaseColumnContext>;
using ColumnMap =
    System.Collections.Immutable.ImmutableDictionary<System.Type, System.
        Collections.Immutable.ImmutableHashSet<rDB.DatabaseColumnContext>>;
using TypeMap =
    System.Collections.Immutable.ImmutableDictionary<System.Type, string>;

namespace rDB.Builder
{
    public class DatabaseBuilder<TDatabase, TConnection>
        where TDatabase : Database<TConnection>
        where TConnection : DbConnection
    {
        protected readonly List<KeyValuePair<Type, ColumnSet>> _tables =
            new List<KeyValuePair<Type, ColumnSet>>();

        protected bool CreateTables = true;
        protected TDatabase Database;
        protected bool DropTables;
        protected bool DropTablesCascade;

        protected Dictionary<Type, DatabaseEntry> TableMap =
            new Dictionary<Type, DatabaseEntry>();

        protected TypeMap TypeMap;

        public DatabaseBuilder(TDatabase database)
        {
            Database = database;
        }

        public DatabaseBuilder()
        {
        }

        public DatabaseBuilder<TDatabase, TConnection> WithDropTables(
            bool cascade = false
        )
        {
            DropTables = true;
            DropTablesCascade = true;

            return this;
        }

        public DatabaseBuilder<TDatabase, TConnection> WithDatabase(
            TDatabase database
        )
        {
            Database = database;
            return this;
        }

        public DatabaseBuilder<TDatabase, TConnection> WithCreateTables(
            bool state
        )
        {
            CreateTables = state;
            return this;
        }

        public DatabaseBuilder<TDatabase, TConnection> WithTypeMap(TypeMap map)
        {
            TypeMap = map;
            return this;
        }

        public DatabaseBuilder<TDatabase, TConnection> WithTable(
            params DatabaseEntry[] tables
        )
        {
            WithTable((IEnumerable<DatabaseEntry>)tables);
            return this;
        }

        public DatabaseBuilder<TDatabase, TConnection> WithTable(
            IEnumerable<DatabaseEntry> tables
        )
        {
            foreach (var table in tables)
            {
                var type = table.GetType();
                if (TableMap.ContainsKey(type))
                    throw new InvalidOperationException(
                        "The specified table has already been added");

                TableMap.Add(type, table);
                _tables.Add(
                    new KeyValuePair<Type,
                        ColumnSet>(type,
                        DatabaseEntry
                            .GetColumns(type)));
            }

            return this;
        }

        private IEnumerable<DatabaseEntry> GetTablesInCreationOrder()
        {
            var graph = new DependencyGraph<Type>();
            foreach (var entry in TableMap)
            {
                var dependencies = entry.Value.GetForeignKeys()
                    .Select(entry => entry.Key.Table);

                graph.Add(entry.Key, dependencies);
            }

            foreach (var entry in graph.Solve())
                yield return TableMap[entry];
        }

        public virtual async Task CreateAllTables()
        {
            var tables = GetTablesInCreationOrder().ToArray();
            await Database.CreateTables(tables);
        }

        public virtual async Task<TDatabase> Build()
        {
            TypeMap ??= DatabaseEntry.BuildTypeMap();

            Database.Configure(TypeMap,
                ImmutableDictionary.CreateRange(_tables));

            if (DropTables)
            {
                var tables = GetTablesInCreationOrder()
                    .Reverse()
                    .ToArray();

                foreach (var table in tables)
                    await Database.DropTable(
                        table.GetType(),
                        DropTablesCascade ? " CASCADE" : ""
                    );
            }

            if (CreateTables)
                await CreateAllTables();

            return Database;
        }
    }
}