using System;
using System.Collections.Generic;
using System.Text;

namespace rDB
{
    public class ColumnMap : Dictionary<Type, ColumnSet>
    {
        public ColumnSet Get<T>() where T : DatabaseEntry =>
            Get(typeof(T));

        public ColumnSet Get(Type type) => 
            TryGetValue(type, out var set) ? set : null;
    }
}
