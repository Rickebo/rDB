using System;
using System.Collections.Generic;
using System.Text;

namespace rDB
{
    public class ColumnSet : HashSet<DatabaseColumnContext>
    {
        public ColumnSet(IEnumerable<DatabaseColumnContext> columns) : base(columns)
        {

        }

        public ColumnSet()
        {

        }
    }
}
