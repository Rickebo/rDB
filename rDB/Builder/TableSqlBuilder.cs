using rDB.Attributes;

using System;
using System.Collections.Generic;
using System.Text;

namespace rDB
{
    public class TableSqlBuilder<T> where T : DatabaseEntry, new()
    {
        private bool _ifNotExists = true;
        private string _name = null;
        private Dictionary<Type, string> _typeMap = null;
        private Dictionary<ForeignKeyAttribute, ISet<string>> _foreignKeys = null;
        private IEnumerable<string> _options = null;
        private readonly T _instance;

        private static string NewLine => Environment.NewLine;

        public TableSqlBuilder(Dictionary<Type, string> typeMap = null)
        {
            _instance = new T();
            _name = ReflectionExtensions.GetAttribute<DatabaseTableAttribute>(typeof(T))?.Name ?? typeof(T).Name;
            _typeMap = typeMap ?? DatabaseEntry.BuildTypeMap();
        }

        public TableSqlBuilder<T> WithIfNotExists(bool value)
        {
            _ifNotExists = value;
            return this;
        }

        //public TableSqlBuilder<T> WithName(string name)
        //{
        //    _name = name;
        //    return this;
        //}

        public TableSqlBuilder<T> WithTypeMap(Dictionary<Type, string> dictionary)
        {
            _typeMap = dictionary;
            return this;
        }

        public TableSqlBuilder<T> WithForeignKeys()
        {
            _foreignKeys = _instance.GetForeignKeys();
            return this;
        }

        public TableSqlBuilder<T> WithForeignKeys(Dictionary<ForeignKeyAttribute, ISet<string>> foreignKeys)
        {
            _foreignKeys = foreignKeys;
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
            var instance = new T();
            var columns = instance.Columns;
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
                appendItem(column.GenerateSql());

            if (anyAppended)
                builder.Length = builder.Length - separator.Length;

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
