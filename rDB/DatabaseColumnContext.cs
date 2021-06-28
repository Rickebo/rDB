using rDB.Attributes;

using System;
using System.Collections.Generic;
using System.Text;

namespace rDB
{
    public class DatabaseColumnContext
    {
        public string Name { get; }
        public DatabaseColumnAttribute Column { get; }

        public DatabaseColumnContext(string name, DatabaseColumnAttribute column)
        {
            Name = name;
            Column = column;
        }

        public string GenerateSql(bool quoteColumnName = true)
        {
            var sql = quoteColumnName 
                ? $"\"{Name}\" {Column.Type}"
                : $"{Name} {Column.Type}";

            if (Column.NotNull)
                sql += " NOT NULL";

            if (Column.AutoIncrement)
                sql += " AUTO_INCREMENT";

            if (Column.Default != null)
                sql += $" DEFAULT {Column.Default}";

            return sql;
        }
    }
}
