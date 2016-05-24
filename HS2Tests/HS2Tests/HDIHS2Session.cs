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
        protected string Name { get; set; }
        protected HDIHS2SessionManager SessionManager { get; set; }
        protected bool DumpExecutionResults { get; set; }

        public HDIHS2Session(string name, HDIHS2SessionManager manager)
        {
            this.Name = name;
            this.SessionManager = manager;

            this.EnsureInit();
        }

        protected void EnsureInit()
        {
            if (this.Connection == null)
            {
                var activity = new HS2ActivityRecord()
                {
                    StartTime = DateTimeOffset.UtcNow,
                    Name = HS2ActivityName.OpenSession,
                    SessionName = this.Name,
                    Status = HS2ActivityState.NOINIT,
                };

                try
                {
                    var connection = new HiveConnection(this.SessionManager.ConnectionString);
                    connection.Open();

                    this.Connection = connection;

                    activity.Status = HS2ActivityState.SUCCESS;
                    activity.EndTime = DateTimeOffset.UtcNow;
                    this.SessionManager.NotifyActivity(activity);
                }
                catch
                {
                    activity.Status = HS2ActivityState.FAIL;
                    activity.EndTime = DateTimeOffset.UtcNow;
                    this.SessionManager.NotifyActivity(activity);

                    throw;
                }

            }
        }

        public void ExecuteQuery(string query, string correlationInfo)
        {
            EnsureInit();

            var activity = new HS2ActivityRecord()
            {
                StartTime = DateTimeOffset.UtcNow,
                Name = HS2ActivityName.ExecuteQueryOnly,
                SessionName = this.Name,
                Status = HS2ActivityState.NOINIT,
            };

            try
            {
                using (var cmd = this.Connection.CreateCommand())
                {
                    cmd.CommandText = query;
                    cmd.CommandType = System.Data.CommandType.Text;
                    var reader = cmd.ExecuteReader();

                    activity.EndTime = DateTimeOffset.UtcNow;
                    activity.Status = HS2ActivityState.SUCCESS;
                    this.SessionManager.NotifyActivity(activity);

                    activity = new HS2ActivityRecord()
                    {
                        StartTime = DateTimeOffset.UtcNow,
                        Name = HS2ActivityName.FetchResults,
                        SessionName = this.Name,
                        Status = HS2ActivityState.NOINIT,
                    };

                    int count = 0;
                    while (reader.HasRows && reader.Read())
                    {
                        object[] values = new object[reader.FieldCount];
                        while (reader.Read())
                        {
                            reader.GetValues(values);
                            count++;

                            if (this.DumpExecutionResults)
                            {
                                Console.WriteLine(string.Join(",", values));
                            }
                        }
                    }

                    activity.EndTime = DateTimeOffset.UtcNow;
                    activity.Status = HS2ActivityState.SUCCESS;
                    activity.Details = count.ToString();
                    this.SessionManager.NotifyActivity(activity);
                }
            }
            catch
            {
                activity.EndTime = DateTimeOffset.UtcNow;
                activity.Status = HS2ActivityState.FAIL;
                this.SessionManager.NotifyActivity(activity);

                this.Connection.Dispose();
                this.Connection = null;

                throw;
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
