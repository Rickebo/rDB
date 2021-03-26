using rDB.Attributes;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace rDB.Tests.Data
{
    [DatabaseTable("double_referenced_table")]
    public class TestDoubleReferencedTable : DatabaseEntry
    {
        [DatabaseColumn("INT", AutoIncrement = true, IsPrimaryKey = true)]
        public int DoubleReferencedId { get; set; }
    }
}
