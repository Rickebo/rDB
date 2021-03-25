using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace rDB
{
    public abstract class Database<T1> where T1 : DbConnection
    {
        protected Database()
        {

        }

        protected abstract T1 GetConnection();

        public bool DropTable<T>() where T : DatabaseEntry
        {

        }
    }
}
