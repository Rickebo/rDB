using NUnit.Framework;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace rDB.Tests
{
    public class DependencyGraphTests
    {
        [Test]
        public void TestZero()
        {
            var graph = new DependencyGraph<Type>();
            var result = graph.Solve();

            Assert.IsEmpty(result);
        }

        [Test]
        public void TestOne()
        {
            var graph = new DependencyGraph<Type>();
            graph.Add(typeof(DependencyGraphTests), Array.Empty<Type>());

            var result = graph.Solve();

            Assert.AreEqual(result.Count(), 1);
        }

        [Test]
        public void TestTwo()
        {
            var graph = new DependencyGraph<int>();
            graph.Add(1, new[] { 2 });
            graph.Add(2, Array.Empty<int>());

            var result = graph.Solve();

            CollectionAssert.AreEqual(new[] { 2, 1 }, result);
        }

        [Test]
        public void TestThreeLinear()
        {
            var graph = new DependencyGraph<int>();
            graph.Add(1, new[] { 2 });
            graph.Add(2, new[] { 3 });
            graph.Add(3, Array.Empty<int>());

            var result = graph.Solve();

            CollectionAssert.AreEqual(new[] { 3, 2, 1 }, result);
        }

        [Test]
        public void TestThreeNonLinear()
        {
            var graph = new DependencyGraph<int>();
            graph.Add(1, new[] { 2, 3 });
            graph.Add(2, new[] { 3 });
            graph.Add(3, Array.Empty<int>());

            var result = graph.Solve();

            CollectionAssert.AreEqual(new[] { 3, 2, 1 }, result);
        }

        [Test]
        public void TestTwoCircular()
        {
            var graph = new DependencyGraph<int>();
            graph.Add(1, new[] { 2 });
            graph.Add(2, new[] { 1 });

            try
            {
                var result = graph.Solve().ToArray();
                Assert.Fail();
            } catch 
            {
                
            }
        }

        [Test]
        public void TestThreeCircular()
        {
            var graph = new DependencyGraph<int>();
            graph.Add(1, new[] { 2 });
            graph.Add(2, new[] { 3 });
            graph.Add(3, new[] { 1 });

            try
            {
                var result = graph.Solve().ToArray();
                Assert.Fail();
            } catch 
            {
                
            }
        }
    }
}
