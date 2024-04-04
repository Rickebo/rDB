using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using rDB.Attributes;
using ColumnSet =
    System.Collections.Immutable.IImmutableSet<rDB.DatabaseColumnContext>;
using ColumnMap =
    System.Collections.Immutable.IImmutableDictionary<System.Type, System.
        Collections.Immutable.IImmutableSet<rDB.DatabaseColumnContext>>;
using TypeMap =
    System.Collections.Immutable.IImmutableDictionary<System.Type, string>;

namespace rDB
{
    public class TableSqlBuilder
    {
        private readonly bool _createIndicesSeparately;
        private readonly DatabaseEntry _instance;
        private readonly string _name;
        private readonly IEnumerable<string> _options = null;
        private readonly bool _quoteColumnName;
        private ColumnSet _columnSet;
        private Dictionary<ForeignKeyAttribute, ISet<string>> _foreignKeys;
        private bool _ifNotExists = true;
        private Dictionary<IndexAttribute, ISet<string>> _indices;
        private TypeMap _typeMap;

        public TableSqlBuilder(
            TypeMap typeMap,
            ColumnSet columns,
            DatabaseEntry instance,
            bool quoteColumnNames = true,
            bool createIndicesSeparately = true
        )
        {
            var type = instance.GetType();

            _instance = instance;
            _name = type.GetAttribute<DatabaseTableAttribute>()?.Name ??
                    type.Name;
            _typeMap = typeMap ?? DatabaseEntry.BuildTypeMap();
            _columnSet = columns ?? DatabaseEntry.GetColumns(type);
            _quoteColumnName = quoteColumnNames;
            _createIndicesSeparately = createIndicesSeparately;
        }

        private static string NewLine => Environment.NewLine;

        public static TableSqlBuilder Create<T>(
            TypeMap typeMap,
            ColumnSet columns,
            bool quoteColumnNames = true,
            bool quoteTableNames = true
        )
            where T : DatabaseEntry, new()
        {
            return new TableSqlBuilder(typeMap, columns, new T(),
                quoteColumnNames);
        }

        public static TableSqlBuilder Create(
            TypeMap typeMap,
            ColumnSet columns,
            DatabaseEntry instance,
            bool quoteColumnNames = true,
            bool quoteTableNames = true
        )
        {
            return new TableSqlBuilder(typeMap, columns, instance,
                quoteColumnNames);
        }

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

        public TableSqlBuilder WithIndices()
        {
            _indices = _instance.GetIndices();
            return this;
        }

        public TableSqlBuilder WithForeignKeys(
            Dictionary<ForeignKeyAttribute, ISet<string>> foreignKeys
        )
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

                if (!_typeMap.TryGetValue(foreignKey.Table,
                        out var referencedTableName))
                    throw new InvalidOperationException(
                        $"Cannot resolve foreign key referencing type {foreignKey.Table} as it is not defined in the specified type map.");

                callback(foreignKey.GenerateSql(referencedTableName, columns,
                    _quoteColumnName));
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

            var keys = _quoteColumnName
                ? primaryKeys.Select(key => "\"" + key + "\"")
                : primaryKeys;

            callback($"PRIMARY KEY ({string.Join(", ", keys)})");
        }

        private void BuildIndices(Action<string> callback, string table)
        {
            if (_indices == null || _indices.Count < 1)
                return;

            foreach (var index in _indices)
            {
                if (_typeMap == null)
                    throw new InvalidOperationException(
                        "Cannot build foreign keys without a type map specified. Use WithTypeMap(...) to specify one.");

                var indexAttribute = index.Key;
                var columns = index.Value;

                callback(indexAttribute.GenerateSql(columns,
                    _quoteColumnName, table));
            }
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
            {
                if (!column.Column.IsCreated)
                    continue;

                appendItem(column.GenerateSql(_quoteColumnName));
            }

            if (anyAppended)
                builder.Length = builder.Length - separator.Length;

            BuildPrimaryKey(sql => builder
                .Append(separator)
                .Append(prefix)
                .Append(sql));

            if (!_createIndicesSeparately)
                BuildIndices(sql => builder
                        .Append(separator)
                        .Append(prefix)
                        .Append(sql),
                    null);

            BuildForeignKeyOptions(sql => builder
                .Append(separator)
                .Append(prefix)
                .Append(sql));

            builder.Append(Environment.NewLine + ")" + Environment.NewLine);
        }

        public string Build()
        {
            if (_name == null)
                throw new InvalidOperationException(
                    "Cannot build table SQL without a table name specified.");

            if (_foreignKeys != null && _foreignKeys.Count > 0 &&
                _typeMap == null)
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

            if (_createIndicesSeparately)
            {
                builder.Append(';');

                BuildIndices(sql => builder
                        .Append(Environment.NewLine)
                        .Append(sql)
                        .Append(';'),
                    _name);
            }


            return builder.ToString();
        }
    }
}