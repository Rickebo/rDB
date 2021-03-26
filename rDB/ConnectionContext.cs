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

        public ConnectionContext(T connection, Compiler compiler)
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
        }

        public async ValueTask DisposeAsync()
        {
            Factory?.Dispose();
            await Connection.DisposeAsync();
        }
    }
}
