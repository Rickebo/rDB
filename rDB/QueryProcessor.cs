using System;
using System.Collections.Generic;
using rDB.Builder;
using SqlKata;

namespace rDB
{
    public struct QueryProcessor
    {
        public IEnumerable<Func<Query, Query>> Processors { get; }

        public QueryProcessor(params Func<Query, Query>[] processors)
        {
            Processors = processors;
        }

        public static QueryProcessorBuilder Builder()
        {
            return new QueryProcessorBuilder();
        }

        public Query Process(Query query)
        {
            foreach (var processor in Processors)
                query = processor(query);

            return query;
        }

        public Func<Query, Query> Function
        {
            get
            {
                var processor = this;
                return query => processor.Process(query);
            }
        }

        public static QueryProcessor operator +(
            QueryProcessor a,
            QueryProcessor b
        )
        {
            return new QueryProcessor(a, b);
        }

        public static implicit operator Func<Query, Query>(
            QueryProcessor processor
        )
        {
            return query => processor.Process(query);
        }

        public static implicit operator QueryProcessor(
            Func<Query, Query> processor
        )
        {
            return new QueryProcessor(processor);
        }
    }
}