using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;

namespace rDB.Attributes
{
    [AttributeUsage(AttributeTargets.Property, Inherited = true, AllowMultiple = false)]
    public class ForeignKeyAttribute : Attribute
    {
        public Type Table { get; }
        public string[] Columns { get; }

        public string IndexName { get; set; }

        public ReferenceOption OnDelete { get; set; } = ReferenceOption.Restrict;
        public ReferenceOption OnUpdate { get; set; } = ReferenceOption.Restrict;

        public ForeignKeyAttribute([NotNull] Type table, [NotNull] params string[] columns)
        {
            if (columns == null || columns.Length < 1)
                throw new ArgumentException("Cannot create foreign key referencing invalid or no columns.", nameof(columns));

            Table = table;
            Columns = columns;
        }

        public string GenerateSql([NotNull] string table, [NotNull] IEnumerable<string> columns, 
            bool quoteColumns = true)
        {
            var count = columns?.Count();
            if (count < 1)
                throw new ArgumentException("Cannot create foreign key on zero columns", nameof(columns));

            if (count != Columns.Length)
                throw new ArgumentException("Cannot create foreign key when column counts dont match.", nameof(columns));

            return $"FOREIGN KEY {(IndexName != null ? IndexName + " " : "")}({string.Join(", ", columns.Select(col => QuoteIf(col, quoteColumns)))}) " +
                $"REFERENCES {table}({string.Join(", ", Columns.Select(col => QuoteIf(col, quoteColumns)))}) " +
                $"ON DELETE {GetReferenceOptionName(OnDelete)} " +
                $"ON UPDATE {GetReferenceOptionName(OnUpdate)}";
        }

        private static string QuoteIf(string text, bool doQuote) => doQuote
            ? "\"" + text + "\""
            : text;

        private string GetReferenceOptionName(ReferenceOption option) => option switch
        {
            ReferenceOption.Restrict => "RESTRICT",
            ReferenceOption.Cascade => "CASCADE",
            ReferenceOption.SetNull => "SET NULL",
            ReferenceOption.NoAction => "NO ACTION",
            ReferenceOption.SetDefault => "SET DEFAULT",
            _ => throw new InvalidOperationException("The specified reference option is not supported.")
        };

        public enum ReferenceOption
        {
            Restrict,
            Cascade,
            SetNull,
            NoAction,
            SetDefault
        }

        public override bool Equals(object obj) => obj is ForeignKeyAttribute other &&
            Table == other.Table && Columns.SequenceEqual(other.Columns) && IndexName == other.IndexName &&
            OnDelete == other.OnDelete && OnUpdate == other.OnUpdate;

        public override int GetHashCode() 
        {
            var hashCode = new HashCode();

            var toHash = new object[]
            {
                typeof(ForeignKeyAttribute),
                Table,
                IndexName,
                OnDelete,
                OnUpdate
            }.Concat(Columns);

            foreach (var entry in toHash)
                hashCode.Add(entry);

            return hashCode.ToHashCode();
        }

        public static bool operator ==(ForeignKeyAttribute a, ForeignKeyAttribute b) => 
            a is null
                ? b is null
                : a.Equals(b);

        public static bool operator !=(ForeignKeyAttribute a, ForeignKeyAttribute b) =>
            !(a == b);
    }
}
