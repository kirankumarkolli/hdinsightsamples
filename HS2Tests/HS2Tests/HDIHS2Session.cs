using Microsoft.HDInsight.Hive.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HS2Tests
{
    public class HDIHS2Session : IDisposable
    {
        protected HiveConnection Connection { get; set; }
        protected long OpenTicks { get; set; }

        public HDIHS2Session(string connectionString)
        {
            var st = Environment.TickCount;
            this.Connection = new HiveConnection(connectionString);
            this.Connection.Open();
            var et = Environment.TickCount;
            this.OpenTicks = et - st;
        }

        public void ExecuteQuery(string query)
        {
            using (var cmd = this.Connection.CreateCommand())
            {
                cmd.CommandText = query;
                cmd.CommandType = System.Data.CommandType.Text;
                var reader = cmd.ExecuteReader();

                while (reader.HasRows && reader.Read())
                {
                    object[] values = new object[reader.FieldCount];
                    while (reader.Read())
                    {
                        reader.GetValues(values);
                        if(Constants.DumpExecutionResults)
                        {
                            Console.WriteLine(string.Join(",", values));
                        }
                    }
                }
            }
        }


        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    this.Connection.Dispose();
                    this.Connection = null;
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~HDIHS2Session() {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
        #endregion
    }
}
