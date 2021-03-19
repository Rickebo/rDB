using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Reflection;
using rDB.Attributes;

namespace rDB
{
    public abstract class DatabaseEntry
    {
        public virtual ISet<DatabaseColumnContext> Columns { get; }

        internal static Dictionary<Type, string> BuildTypeMap()
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

            return allTypes.ToDictionary(
                type => type, 
                type => ReflectionExtensions.GetAttribute<DatabaseTableAttribute>(type)?.Name ?? type.Name);
        }

        protected static ISet<DatabaseColumnContext> GetColumns<T>() where T : DatabaseEntry
        {
            var cols = ReflectionExtensions.GetAttributes<DatabaseColumnAttribute, T>();

            var columns = cols
                .Select(col => new DatabaseColumnContext(col.Key.Name, col.Value))
                .ToHashSet();

            return columns;
        }

        public virtual void FillColumns(ISet<DatabaseColumnContext> columns)
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

        public virtual IEnumerable<string> GetColumns(Predicate<DatabaseColumnContext> predicate)
        {
            return Columns
                    .Where(col => predicate(col))
                    .Select(col => col.Name);
        }

        public virtual void Save(Func<DatabaseColumnContext, bool> predicate, Action<string, object> saver) =>
                Save(Columns.Where(predicate)
                        .Select(column => column.Name), saver);

        public virtual void Save(IEnumerable<string> columns, Action<string, object> saver)
        {
            foreach (var column in columns)
                saver(column, Get(column));
        }

        public abstract object Get(string column);
    }
}
