using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace rDB
{
    internal static class ReflectionExtensions
    {
        internal static T GetAttribute<T>(this MemberInfo info) where T : Attribute => 
            info.GetCustomAttribute<T>();

        internal static Dictionary<PropertyInfo, T> GetAttributes<T, T2>() where T : Attribute =>
            typeof(T2).GetAttributes<T>();

        internal static Dictionary<PropertyInfo, T> GetAttributes<T>(this Type type) where T : Attribute
        {
            var properties = type.GetProperties();
            var attributes = new Dictionary<PropertyInfo, T>();

            foreach (var property in properties)
            {
                var attribute = property.GetAttribute<T>();

                if (attribute == null)
                    continue;

                attributes.Add(property, attribute);
            }

            return attributes;
        }
    }
}
