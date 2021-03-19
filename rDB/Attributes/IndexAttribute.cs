using System;
using System.Collections.Generic;
using System.Text;

namespace rDB.Attributes
{
    [AttributeUsage(AttributeTargets.Property)]
    public class IndexAttribute : Attribute
    {
        public string Name { get; set; } = null;
        public bool Unique { get; set; } = true;
    }
}
