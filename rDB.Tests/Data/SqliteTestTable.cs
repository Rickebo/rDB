using rDB.Attributes;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace rDB.Tests.Data
{
    [DatabaseTable("test_table")]
    public class SqliteTestTable : DatabaseEntry
    {
        [DatabaseColumn("INT", AutoIncrement = true, IsPrimaryKey = true)]
        public int TestId { get; set; }

        [DatabaseColumn("INT")]
        [ForeignKey(typeof(SqliteTestReferencedTable), nameof(SqliteTestReferencedTable.Id))]
        public int ReferencedId { get; set; }

        public override object Get(string column) =>
            column switch
            {
                nameof(TestId) => TestId,
                nameof(ReferencedId) => ReferencedId,
                _ => throw new NotImplementedException()
            };
    }
}
