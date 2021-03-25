﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using rDB.Attributes;
using System.Runtime.CompilerServices;

namespace rDB
{
    public abstract class DatabaseEntry
    {
        private static Dictionary<ColumnKey, WeakReference<PropertyInfo>> _propertyCache = new Dictionary<ColumnKey, WeakReference<PropertyInfo>>();

        internal static TypeMap BuildTypeMap()
        {
            var executingAssembly = Assembly.GetExecutingAssembly();
            var executingAssemblyName = executingAssembly.GetName();
            var referencingAssemblies = AppDomain.CurrentDomain.GetAssemblies()
                .Where(assembly => assembly != null && assembly
                    .GetReferencedAssemblies()
                    .Any(referencedAssembly => AssemblyName.ReferenceMatchesDefinition(referencedAssembly, executingAssemblyName)));

            var allTypes = referencingAssemblies.SelectMany(assembly => assembly.GetTypes()
                    .Where(type => 
                        typeof(DatabaseEntry).IsAssignableFrom(type) &&
                        !type.IsAbstract &&
                        type.IsClass
                        ));

            var result = new TypeMap();
            foreach (var type in allTypes)
            {
                var attribute = ReflectionExtensions.GetAttribute<DatabaseTableAttribute>(type);
                result.Add(type, attribute?.Name ?? type.Name);
            }

            return result;
        }

        protected internal static ColumnSet GetColumns<T>() where T : DatabaseEntry => 
            GetColumns(typeof(T));

        protected internal static ColumnSet GetColumns(Type type) 
        {
            var cols = ReflectionExtensions.GetAttributes<DatabaseColumnAttribute>(type);

            var columns = new ColumnSet(cols
                .Select(col => new DatabaseColumnContext(col.Key.Name, col.Value)));

            return columns;
        }

        public virtual void FillColumns(ColumnSet columns)
        {
            var type = this.GetType();
            var properties = type.GetProperties();

            foreach (var property in properties)
            {
                var attribute = property.GetCustomAttribute<DatabaseColumnAttribute>();

                if (attribute == null)
                    continue;

                columns.Add(new DatabaseColumnContext(property.Name, attribute));
            }
        }

        public virtual Dictionary<ForeignKeyAttribute, ISet<string>> GetForeignKeys()
        {
            var attributes = ReflectionExtensions.GetAttributes<ForeignKeyAttribute>(GetType());
            var dict = new Dictionary<ForeignKeyAttribute, ISet<string>>();

            foreach (var entry in attributes)
            {
                var foreignKey = entry.Value;
                var property = entry.Key;

                if (dict.TryGetValue(foreignKey, out var columnSet))
                    columnSet.Add(property.Name);
                else
                    dict.Add(foreignKey, new HashSet<string> { property.Name });
            }

            return dict;
        }

        public virtual void Save(IEnumerable<DatabaseColumnContext> columns, Action<string, object> saver) =>
            Save(columns.Select(col => col.Name), saver);

        public virtual void Save(IEnumerable<string> columns, Action<string, object> saver)
        {
            foreach (var column in columns)
                saver(column, Get(column));
        }

        public virtual object Get(string column)
        {
            var type = this.GetType();
            var key = new ColumnKey(type, column);

            var property = _propertyCache.TryGetValue(key, out var reference) && reference.TryGetTarget(out var referenceTarget)
                ? referenceTarget
                : null;

            var isCached = property != null;
            property ??= type.GetProperty(column);

            if (property == null)
                throw new InvalidOperationException("Cannot get non existing column.");

            if (!isCached)
                _propertyCache.Add(key, new WeakReference<PropertyInfo>(property));

            return property.GetValue(this);
        }

        private readonly struct ColumnKey
        {
            public readonly string ColumnName { get; }
            public readonly Type Table { get; }

            public ColumnKey(Type type, string col)
            {
                ColumnName = col;
                Table = type;
            }

            public override bool Equals(object obj) => 
                obj is ColumnKey key &&
                    ColumnName.Equals(key.ColumnName) &&
                    Table.Equals(key.Table);

            public override int GetHashCode() =>
                HashCode.Combine(Table.GetHashCode(), ColumnName.GetHashCode());
        }
    }
}
