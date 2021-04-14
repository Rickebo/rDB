using rDB.Attributes;

using System;
using System.Collections.Generic;
using System.Text;

using ColumnSet = System.Collections.Immutable.IImmutableSet<rDB.DatabaseColumnContext>;
using ColumnMap = System.Collections.Immutable.IImmutableDictionary<System.Type, System.Collections.Immutable.IImmutableSet<rDB.DatabaseColumnContext>>;
using TypeMap = System.Collections.Immutable.IImmutableDictionary<System.Type, string>;
using System.Linq;

namespace rDB
{
    public class TableSqlBuilder
    {
        private bool _ifNotExists = true;
        private string _name = null;
        private TypeMap _typeMap = null;
        private Dictionary<ForeignKeyAttribute, ISet<string>> _foreignKeys = null;
        private IEnumerable<string> _options = null;
        private readonly DatabaseEntry _instance;
        private ColumnSet _columnSet;
        private bool _quoteColumnName;

        private static string NewLine => Environment.NewLine;

        public TableSqlBuilder(TypeMap typeMap, ColumnSet columns, DatabaseEntry instance, bool quoteColumnNames = true)
        {
            var type = instance.GetType();

            _instance = instance;
            _name = ReflectionExtensions.GetAttribute<DatabaseTableAttribute>(type)?.Name ?? type.Name;
            _typeMap = typeMap ?? DatabaseEntry.BuildTypeMap();
            _columnSet = columns ?? DatabaseEntry.GetColumns(type);
            _quoteColumnName = quoteColumnNames;
        }

        public static TableSqlBuilder Create<T>(TypeMap typeMap, ColumnSet columns, bool quoteColumnNames = true) where T : DatabaseEntry, new() =>
            new TableSqlBuilder(typeMap, columns, new T(), quoteColumnNames: quoteColumnNames);

        public static TableSqlBuilder Create(TypeMap typeMap, ColumnSet columns, DatabaseEntry instance, bool quoteColumnNames = true) =>
            new TableSqlBuilder(typeMap, columns, instance, quoteColumnNames: quoteColumnNames);

        public TableSqlBuilder WithIfNotExists(bool value)
        {
            _ifNotExists = value;
            return this;
        }

        //public TableSqlBuilder<T> WithName(string name)
        //{
        //    _name = name;
        //    return this;
        //}

        public TableSqlBuilder WithTypeMap(TypeMap typeMap)
        {
            _typeMap = typeMap;
            return this;
        }

        public TableSqlBuilder WithForeignKeys()
        {
            _foreignKeys = _instance.GetForeignKeys();
            return this;
        }

        public TableSqlBuilder WithForeignKeys(Dictionary<ForeignKeyAttribute, ISet<string>> foreignKeys)
        {
            _foreignKeys = foreignKeys;
            return this;
        }

        public TableSqlBuilder WithColumnSet(ColumnSet set)
        {
            _columnSet = set;
            return this;
        }

        private void BuildForeignKeyOptions(Action<string> callback)
        {
            if (_foreignKeys == null || _foreignKeys.Count < 1)
                return;

            foreach (var foreignKeyEntry in _foreignKeys)
            {
                if (_typeMap == null)
                    throw new InvalidOperationException(
                        "Cannot build foreign keys without a type map specified. Use WithTypeMap(...) to specify one.");

                var foreignKey = foreignKeyEntry.Key;
                var columns = foreignKeyEntry.Value;

                if (!_typeMap.TryGetValue(foreignKey.Table, out var referencedTableName))
                    throw new InvalidOperationException(
                        $"Cannot resolve foreign key referencing type {foreignKey.Table} as it is not defined in the specified type map.");

                callback(foreignKey.GenerateSql(referencedTableName, columns));
            }
        }

        private void BuildPrimaryKey(Action<string> callback)
        {
            var primaryKeys = _columnSet
                .Where(column => column.Column.IsPrimaryKey)
                .Select(column => column.Name)
                .Distinct()
                .ToArray();

            if (primaryKeys == null || primaryKeys.Length < 1)
                return;

            callback($"PRIMARY KEY ({string.Join(", ", primaryKeys)})");
        }

        private void BuildOptions(Action<string> callback)
        {
            if (_options == null)
                return;

            foreach (var option in _options)
                callback(option);
        }

        private void BuildConditions(StringBuilder builder)
        {
            var anyAppended = false;
            var separator = "," + NewLine;
            void appendItem(string sql)
            {
                builder
                    .Append(sql)
                    .Append(separator);

                anyAppended = true;
            }

            BuildOptions(appendItem);
            BuildForeignKeyOptions(appendItem);

            if (anyAppended)
                builder.Length = builder.Length - separator.Length;
        }

        private void BuildColumns(StringBuilder builder)
        {
            var columns = _columnSet;
            var separator = "," + Environment.NewLine;
            var prefix = "    ";
            var anyAppended = false;

            void appendItem(string sql)
            {
                builder
                    .Append(prefix)
                    .Append(sql)
                    .Append(separator);

                anyAppended = true;
            }

            foreach (var column in columns)
                appendItem(column.GenerateSql(quoteColumnName: _quoteColumnName));

            if (anyAppended)
                builder.Length = builder.Length - separator.Length;

            BuildPrimaryKey(sql => builder
                .Append(separator)
                .Append(sql));

            builder.Append(Environment.NewLine + ")" + Environment.NewLine);
        }

        public string Build()
        {
            if (_name == null)
                throw new InvalidOperationException(
                    "Cannot build table SQL without a table name specified.");

            if (_foreignKeys != null && _foreignKeys.Count > 0 && _typeMap == null)
                throw new InvalidOperationException(
                    "Cannot build foreign keys without a type map specified. Use WithTypeMap(...) to specify one.");

            var builder = new StringBuilder();

            var header = _ifNotExists
                ? "CREATE TABLE IF NOT EXISTS "
                : "CREATE TABLE ";

            builder
                .Append(header)
                .Append(_name)
                .Append("( ")
                .Append(NewLine);

            BuildColumns(builder);
            BuildConditions(builder);

            return builder.ToString();
        }
    }
}
