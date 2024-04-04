using System;

namespace rDB.Attributes
{
    [AttributeUsage(AttributeTargets.Property)]
    public class DatabaseColumnAttribute : Attribute
    {
        public DatabaseColumnAttribute(string type)
        {
            Type = type;
        }

        public string Type { get; }

        public bool IsInserted { get; set; } = true;
        public bool IsCreated { get; set; } = true;
        public bool IsPrimaryKey { get; set; } = false;
        public bool AutoIncrement { get; set; } = false;

        public string Default { get; set; } = null;

        public bool NotNull { get; set; } = false;
    }
}