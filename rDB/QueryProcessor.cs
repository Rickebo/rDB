using SqlKata;

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq.Expressions;
using System.Text;

namespace rDB
{
    public struct QueryProcessor
    {
        public IEnumerable<Func<Query, Query>> Processors { get; private set; }

        public QueryProcessor(params Func<Query, Query>[] processors)
        {
            Processors = processors;
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

        public static implicit operator Func<Query, Query>(QueryProcessor processor) => 
            query => processor.Process(query);

        public static implicit operator QueryProcessor(Func<Query, Query> processor) => 
            new QueryProcessor(processor);
    }
}
