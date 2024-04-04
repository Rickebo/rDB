using System;
using System.Collections.Generic;
using System.Linq;

namespace rDB.Attributes
{
    [AttributeUsage(AttributeTargets.Property)]
    public class IndexAttribute : Attribute
    {
        public string Name { get; set; } = null;
        public string Type { get; set; } = null;
        public bool Unique { get; set; } = true;

        public string GenerateSql(
            IEnumerable<string> columns,
            bool quoteColumnNames,
            string table = null
        )
        {
            return
                $"{GetPrefix(table != null)}{GetUniqueSql()}INDEX {GetIfNotExistsText(table != null)}{Name} {GetTableName(table)}{GetTypeSql()}({string.Join(", ", quoteColumnNames ? columns.Select(col => "\"" + col + "\"") : columns)})";
        }

        private string GetUniqueSql()
        {
            return Unique
                ? "UNIQUE "
                : "";
        }

        private string GetTypeSql()
        {
            return Type != null
                ? $"USING {Type} "
                : "";
        }

        private string GetTableName(string tableName)
        {
            return tableName != null
                ? $"ON {tableName} "
                : "";
        }

        private string GetPrefix(bool createPrefix)
        {
            return createPrefix
                ? "CREATE "
                : "";
        }

        private string GetIfNotExistsText(bool shouldAppear)
        {
            return shouldAppear
                ? "IF NOT EXISTS "
                : "";
        }
    }
}