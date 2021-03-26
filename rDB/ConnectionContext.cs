using SqlKata.Compilers;
using SqlKata.Execution;

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace rDB
{
    public class ConnectionContext<TConnection> : IDisposable, IAsyncDisposable 
        where TConnection : DbConnection
    {
        public TConnection Connection { get; }
        public QueryFactory Factory { get; }

        public EventHandler<ConnectionContextDisposeEventArgs> OnDispose { get; set; }

        public ConnectionContext(TConnection connection, Compiler compiler)
        {
            Connection = connection;
            Factory = compiler != null 
                ? new QueryFactory(connection, compiler)
                : null;
        }

        public void Dispose()
        {
            Factory?.Dispose();
            Connection.Dispose();
            
            OnDispose?.Invoke(this, new ConnectionContextDisposeEventArgs(this, false));
        }

        public async ValueTask DisposeAsync()
        {
            Factory?.Dispose();
            await Connection.DisposeAsync();

            OnDispose?.Invoke(this, new ConnectionContextDisposeEventArgs(this, true));
        }

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
