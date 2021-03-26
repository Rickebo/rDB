using SqlKata.Compilers;
using SqlKata.Execution;

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace rDB
{
    public readonly struct ConnectionContext<T> : IDisposable, IAsyncDisposable where T : DbConnection
    {
        public readonly T Connection { get; }
        public readonly QueryFactory Factory { get; }

        private readonly Action<T, bool> _disposeCallback;

        public ConnectionContext(T connection, Compiler compiler, Action<T, bool> disposeCallback)
        {
            Connection = connection;
            Factory = compiler != null 
                ? new QueryFactory(connection, compiler)
                : null;

            _disposeCallback = disposeCallback;
        }

        public void Dispose()
        {
            Factory?.Dispose();
            Connection.Dispose();

            _disposeCallback?.Invoke(Connection, false);
        }

        public async ValueTask DisposeAsync()
        {
            Factory?.Dispose();
            await Connection.DisposeAsync();
            
            _disposeCallback?.Invoke(Connection, true);
        }
    }
}
