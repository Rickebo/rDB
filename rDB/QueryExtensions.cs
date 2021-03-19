using SqlKata;
using SqlKata.Execution;

using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading.Tasks;

namespace rDB
{
    public static class QueryExtensions
    {
        public static async Task<int> InsertEntryAsync<T>(this Query query, T data, IDbTransaction? transaction = null,
                int? timeout = null) where T : DatabaseEntry
            => await InsertEntryAsync(query, data, ctx => ctx.Column.IsInserted, transaction, timeout);

        public static async Task<int> InsertEntryAsync<T>(this Query query, T data, Predicate<DatabaseColumnContext> selector, 
                IDbTransaction? transaction = null, int? timeout = null) where T : DatabaseEntry
        {
            var dict = new Dictionary<string, object>();

            data.Save(ctx => selector(ctx), (column, value) => dict.Add(column, value));

            return await query.InsertAsync(dict, transaction, timeout);
        }
    }
}
