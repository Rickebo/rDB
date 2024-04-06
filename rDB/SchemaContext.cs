using System;
using System.Collections.Immutable;
using ColumnSet =
    System.Collections.Immutable.ImmutableHashSet<rDB.DatabaseColumnContext>;
using ColumnMap =
    System.Collections.Immutable.ImmutableDictionary<System.Type, System.Collections.
        Immutable.ImmutableHashSet<rDB.DatabaseColumnContext>>;
using TypeMap = System.Collections.Immutable.ImmutableDictionary<System.Type, string>;

namespace rDB
{
    public class SchemaContext
    {
        public SchemaContext(
            IImmutableDictionary<Type, IImmutableSet<DatabaseColumnContext>>
                columnMap,
            IImmutableDictionary<Type, string> typeMap
        )
        {
            ColumnMap = columnMap;
            TypeMap = typeMap;
        }

        public IImmutableDictionary<Type, IImmutableSet<DatabaseColumnContext>>
            ColumnMap { get; }

        public IImmutableDictionary<Type, string> TypeMap { get; }

        public string TableName<TTable>() where TTable : DatabaseEntry
        {
            return TableName(typeof(TTable));
        }

        public string TableName(Type type)
        {
            return TypeMap[type];
        }

        public IImmutableSet<DatabaseColumnContext> Columns<TTable>()
            where TTable : DatabaseEntry
        {
            return ColumnMap[typeof(TTable)];
        }
    }
}