using SqlKata;
using SqlKata.Compilers;
using SqlKata.Execution;

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

using ColumnSet = System.Collections.Immutable.IImmutableSet<rDB.DatabaseColumnContext>;
using ColumnMap = System.Collections.Immutable.ImmutableDictionary<System.Type, System.Collections.Immutable.ImmutableHashSet<rDB.DatabaseColumnContext>>;

namespace rDB
{
    public class ConnectionContext<TConnection> : BaseConnectionContext<TConnection>, IDisposable, IAsyncDisposable 
        where TConnection : DbConnection
    {
        public override SchemaContext Schema { get; }
        public override TConnection Connection { get; }
        public override QueryFactory Factory { get; }

        public EventHandler<ConnectionContextDisposeEventArgs> OnDispose { get; set; }

        public ConnectionContext(TConnection connection, Compiler compiler, SchemaContext schema)
        {
            Schema = schema;
            Connection = connection;
            Factory = compiler != null 
                ? new QueryFactory(connection, compiler)
                : null;
        }

        protected virtual TContext Table<TContext, TTable>(Func<string, ColumnSet, ConnectionContext<TConnection>, TContext> constructor)
            where TContext : TableConnectionContext<TTable, TConnection>
            where TTable : DatabaseEntry
        {
            if (!Schema.ColumnMap.TryGetValue(typeof(TTable), out var columns))
                throw new InvalidOperationException("Cannot access table which is not a part of this database.");

            var name = Schema.TypeMap[typeof(TTable)];

            return constructor(name, columns, this);
        }

        public override void Dispose()
        {
            base.Dispose();
            
            OnDispose?.Invoke(this, new ConnectionContextDisposeEventArgs(this, false));
        }

        public override async ValueTask DisposeAsync()
        {
            await base.DisposeAsync();

            OnDispose?.Invoke(this, new ConnectionContextDisposeEventArgs(this, true));
        }

        public Query Query(string table) => Factory.Query(table);

        public readonly struct ConnectionContextDisposeEventArgs
        {
            public readonly ConnectionContext<TConnection> Context { get; }
            public readonly bool IsAsynchronous { get; }

            public ConnectionContextDisposeEventArgs(ConnectionContext<TConnection> context, bool isAsync) 
            {
                Context = context;
                IsAsynchronous = isAsync;
            }
        }
    }
}
