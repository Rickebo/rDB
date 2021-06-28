using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace rDB.Attributes
{
    [AttributeUsage(AttributeTargets.Property)]
    public class IndexAttribute : Attribute
    {
        public string Name { get; set; } = null;
        public string Type { get; set; } = null;
        public bool Unique { get; set; } = true;

        public string GenerateSql(IEnumerable<string> columns, bool quoteColumnNames, string table = null) =>
            $"{GetPrefix(table != null)}{GetUniqueSql()}INDEX {GetIfNotExistsText(table != null)}{Name} {GetTableName(table)}{GetTypeSql()}({string.Join(", ", (quoteColumnNames ? columns.Select(col => "\"" + col + "\"") : columns))})";

        private string GetUniqueSql() => Unique
            ? "UNIQUE "
            : "";

        private string GetTypeSql() => Type != null
            ? $"USING {Type} "
            : "";

        private string GetTableName(string tableName) => tableName != null
            ? $"ON {tableName} "
            : "";

        private string GetPrefix(bool createPrefix) => createPrefix
            ? "CREATE "
            : "";

        private string GetIfNotExistsText(bool shouldAppear) => shouldAppear
            ? "IF NOT EXISTS "
            : "";
    }
}
