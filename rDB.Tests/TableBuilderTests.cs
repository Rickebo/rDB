using System;
using System.Collections.Generic;
using System.Collections.Immutable;

using NUnit.Framework;

using rDB.Attributes;
using rDB.Builder;

namespace rDB.Tests
{
    public class TableBuilderTests
    {
        [SetUp]
        public void Setup()
        {
            
        }

        [Test]
        public void TestSqlBuilder()
        {
            const string tableName = "testTable";

            var typeMap = new TypeMapBuilder()
                .With<TestDatabaseType>(nameof(TestDatabaseType))
                .With<TestTargetDatabaseType>(nameof(TestTargetDatabaseType))
                .Build();

            var builder = new TableSqlBuilder(typeMap, null, new TestDatabaseType())
                .WithForeignKeys();

            var sql = builder.Build();
            
            Assert.True(sql.Contains("CREATE TABLE", StringComparison.OrdinalIgnoreCase));
            Assert.True(sql.Contains(tableName));
        }

        [DatabaseTable("testTable")]
        public class TestDatabaseType : DatabaseEntry
        {
            [DatabaseColumn("INT")]
            public int Id { get; set; }

            [DatabaseColumn("INT")]
            [ForeignKey(typeof(TestTargetDatabaseType), nameof(TestTargetDatabaseType.TargetId))]
            public int ReferencingId { get; set; }

            public override object Get(string column) => column switch
            {
                nameof(Id) => Id,
                _ => throw new NotImplementedException()
            };
        }

        [DatabaseTable("targett")]
        public class TestTargetDatabaseType : DatabaseEntry
        {
            [DatabaseColumn("INT")]
            public int TargetId { get; set; }

            public override object Get(string column) => column switch
            {
                nameof(TargetId) => TargetId,
                _ => throw new NotImplementedException()
            };
        }
    }
}