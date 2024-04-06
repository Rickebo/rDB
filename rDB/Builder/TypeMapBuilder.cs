using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using rDB.Attributes;

namespace rDB.Builder
{
    public class TypeMapBuilder
    {
        private readonly ImmutableDictionary<Type, string>.Builder _builder =
            ImmutableDictionary.CreateBuilder<Type, string>();

        public TypeMapBuilder()
        {
        }

        public TypeMapBuilder(IEnumerable<KeyValuePair<Type, string>> entries)
        {
            WithRange(entries);
        }

        public TypeMapBuilder WithRange(IEnumerable<KeyValuePair<Type, string>> entries)
        {
            foreach (var entry in entries)
                With(entry.Key, entry.Value);

            return this;
        }

        public TypeMapBuilder With<T>(string name) where T : DatabaseEntry
        {
            return With(typeof(T), name);
        }

        public TypeMapBuilder With(Type type, string name)
        {
            if (!typeof(DatabaseEntry).IsAssignableFrom(type))
                throw new ArgumentException(
                    "The specified type does not inherit from DatabaseEntry.");

            _builder.Add(type, name);
            return this;
        }

        public TypeMapBuilder With<T>() where T : DatabaseEntry
        {
            return With(typeof(T));
        }

        public TypeMapBuilder With(Type type)
        {
            var name = type.GetAttribute<DatabaseTableAttribute>()?.Name ??
                       type.Name;
            return With(type, name);
        }

        public ImmutableDictionary<Type, string> Build()
        {
            return _builder.ToImmutable();
        }
    }
}