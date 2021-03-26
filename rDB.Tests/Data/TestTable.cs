using rDB.Attributes;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace rDB.Tests.Data
{
    [DatabaseTable("test_table")]
    public class TestTable : DatabaseEntry
    {
        [DatabaseColumn("INT", AutoIncrement = true, IsPrimaryKey = true)]
        public int TestId { get; set; }

        [DatabaseColumn("INT")]
        [ForeignKey(typeof(TestReferencedTable), nameof(TestReferencedTable.Id))]
        public int ReferencedId { get; set; }
    }
}
