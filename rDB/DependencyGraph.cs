using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace rDB
{
    public class DependencyGraph<T>
    {
        private Dictionary<T, HashSet<T>> _tree = new Dictionary<T, HashSet<T>>();

        public DependencyGraph()
        {

        }

        private DependencyGraph(Dictionary<T, HashSet<T>> dict)
        {
            foreach (var entry in dict)
                _tree.Add(entry.Key, new HashSet<T>(entry.Value));
        }

        public void Add(T entry, IEnumerable<T> dependencies) => 
            _tree.Add(entry, dependencies.ToHashSet());

        /// <summary>
        /// Iterates over the entries in the dependency graph. The iteration is done in order,
        /// so that entries that have no dependencies are returned first
        /// </summary>
        /// <returns></returns>
        public IEnumerable<T> Solve() => 
            new DependencyGraph<T>(_tree).SolveDestructive();

        /// <summary>
        /// Iterates over the entries in the dependency graph. The iteration is done in order,
        /// so that entries that have no dependencies are returned first
        /// 
        /// The graph will no longer be empty after this method has finished executing.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<T> SolveDestructive()
        {
            try
            {
                while (Find(out var value))
                {
                    Remove(value);
                    yield return value;
                }
            }
            finally
            {
                _tree?.Clear();
            }
        }

        private bool Find(out T value)
        {
            if (_tree.Count == 0)
            {
                value = default;
                return false;
            }

            foreach (var typeEntry in _tree)
            {
                if (typeEntry.Value.Count > 0)
                    continue;

                value = typeEntry.Key;
                return true;
            }

            throw new Exception("Could not find solution to dependencies. Verify that there are no circular dependencies and that " +
                "there are no dependencies that are not included.");
        }

        private void Remove(T entry)
        {
            _tree.Remove(entry);

            foreach (var set in _tree.Values)
                set.Remove(entry);
        }
    }
}
