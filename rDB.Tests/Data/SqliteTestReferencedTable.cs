using rDB.Attributes;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace rDB.Tests.Data
{
    [DatabaseTable("referenced_table")]
    public class SqliteTestReferencedTable : DatabaseEntry
    {
        [DatabaseColumn("INT", AutoIncrement = true, IsPrimaryKey = true)]
        public int Id { get; set; }

        public override object Get(string column) =>
            column switch
            {
                nameof(Id) => Id,
                _ => throw new NotImplementedException()
            };
    }
}
