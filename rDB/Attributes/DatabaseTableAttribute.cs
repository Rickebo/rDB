using System;

namespace rDB.Attributes
{
    [AttributeUsage(AttributeTargets.Class)]
    public class DatabaseTableAttribute : Attribute
    {
        public DatabaseTableAttribute(string name)
        {
            Name = name;
        }

        public string Name { get; }
    }
}