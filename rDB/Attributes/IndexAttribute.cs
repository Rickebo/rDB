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

        public string GenerateSql(IEnumerable<string> columns, bool quoteColumnNames) =>
            $"{GetUniqueSql()}INDEX {Name} {GetTypeSql()}({string.Join(", ", (quoteColumnNames ? columns.Select(col => "\"" + col + "\"") : columns))})";

        private string GetUniqueSql() => Unique
            ? "UNIQUE "
            : "";

        private string GetTypeSql() => Type != null
            ? $"USING {Type} "
            : "";
    }
}
