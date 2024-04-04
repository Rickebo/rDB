using System;
using System.Collections.Generic;
using SqlKata;

namespace rDB.Builder
{
    public class QueryProcessorBuilder
    {
        private readonly List<Func<Query, Query>> _processors =
            new List<Func<Query, Query>>();

        internal QueryProcessorBuilder()
        {
        }

        public QueryProcessorBuilder Where(QueryProcessor processor)
        {
            return Where(processor.Function);
        }

        public QueryProcessorBuilder Where(Func<Query, Query> processor)
        {
            _processors.Add(processor);
            return this;
        }

        public QueryProcessor Build()
        {
            return new QueryProcessor(_processors.ToArray());
        }
    }
}