using rDB.Attributes;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace rDB.Tests.Data
{
    [DatabaseTable("referenced_table")]
    public class TestReferencedTable : DatabaseEntry
    {
        [DatabaseColumn("INT", AutoIncrement = true, IsPrimaryKey = true)]
        public int Id { get; set; }

        [DatabaseColumn("INT")]
        [ForeignKey(typeof(TestDoubleReferencedTable), nameof(TestDoubleReferencedTable.DoubleReferencedId))]
        public int DoubleReferencedId { get; set; }
    }
}
