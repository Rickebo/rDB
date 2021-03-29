using System;
using System.Collections.Generic;
using System.Text;

using ColumnSet = System.Collections.Immutable.ImmutableHashSet<rDB.DatabaseColumnContext>;
using ColumnMap = System.Collections.Immutable.ImmutableDictionary<System.Type, System.Collections.Immutable.ImmutableHashSet<rDB.DatabaseColumnContext>>;
using TypeMap = System.Collections.Immutable.ImmutableDictionary<System.Type, string>;
using System.Collections.Immutable;

namespace rDB
{
    public class SchemaContext
    {
        public IImmutableDictionary<Type, IImmutableSet<DatabaseColumnContext>> ColumnMap { get; }
        public IImmutableDictionary<Type, string> TypeMap { get; }

        public SchemaContext(
            IImmutableDictionary<Type, IImmutableSet<DatabaseColumnContext>> columnMap,
            IImmutableDictionary<Type, string> typeMap)
        {
            ColumnMap = columnMap;
            TypeMap = typeMap;
        }

        public string TableName<TTable>() where TTable : DatabaseEntry =>
            TypeMap[typeof(TTable)];

        public IImmutableSet<DatabaseColumnContext> Columns<TTable>()
            where TTable : DatabaseEntry =>
            ColumnMap[typeof(TTable)];
    }
}
