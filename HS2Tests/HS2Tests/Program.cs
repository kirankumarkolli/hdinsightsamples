using Microsoft.HDInsight.Hive.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HS2Tests
{
    public class Program
    {
        private const string connectionString = ;

        static void Main(string[] args)
        {
            // NumOfConnections();
            Program.SimpleQuery().Wait();
        }

        static void NumOfConnections() 
        {
            var connectionList = new List<HiveConnection>();

            int i = 0;
            bool cont = true;

            // for (int i=0; i< 500; i++)
            while(cont)
            {
                var st = Environment.TickCount;
                var connection = new HiveConnection(connectionString);
                connection.Open();
                connectionList.Add(connection);
                var et = Environment.TickCount;

                Console.WriteLine("Open {0} : {1}", i, et - st);
                i++;
            }

            int j = 0;
            foreach(var e in connectionList) {
                var st = Environment.TickCount;
                e.Dispose();
                var et = Environment.TickCount;
                Console.WriteLine("Dispose {0} : {1}", j, et - st);

                j++;
            }
        }

        public static async Task SimpleQuery()
        {
            using (var connection = new HiveConnection(connectionString))
            {
                connection.Open();
                using (var cmd = connection.CreateCommand()) {
                    cmd.CommandText = "select * from hivesampletable where market=\"en-US\" limit 5000";
                    // cmd.CommandText = "show tables";
                    cmd.CommandType = System.Data.CommandType.Text;
                    var reader = cmd.ExecuteReader();

                    while (reader.HasRows && reader.Read())
                    {
                        object[] values = new object[reader.FieldCount];
                        while (reader.Read())
                        {
                            reader.GetValues(values);
                            Console.WriteLine(string.Join(",", values));
                        }
                    }
                }
            }
        }
    }

    public class HDIHS2Session : IDisposable
    {
        protected HiveConnection Connection { get; set; }

        public HDIHS2Session(string connectionString)
        {
            this.Connection = new HiveConnection(connectionString);
            this.Connection.Open();
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
