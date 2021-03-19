using System;
using System.Collections.Generic;
using System.Text;

namespace rDB.Attributes
{
    [AttributeUsage(AttributeTargets.Class)]
    public class DatabaseTableAttribute : Attribute
    {
        public string Name { get; }

        public DatabaseTableAttribute(string name)
        {
            Name = name;
        }
    }
}
