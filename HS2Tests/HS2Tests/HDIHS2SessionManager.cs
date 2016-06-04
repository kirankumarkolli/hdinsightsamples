using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace HS2Tests
{
    public class HDIHS2SessionManager : IDisposable
    {
        protected string ConnectionString { get; set; }
        protected int NumOfSessions { get; set; }

        protected List<HDIHS2Session> Sessions { get; set; }
        protected ConcurrentQueue<string> Slots { get; set; }

        public HDIHS2SessionManager(string connectionString, int sessionCount)
        {
            this.ConnectionString = connectionString;
            this.NumOfSessions = sessionCount;
            this.Sessions = new List<HDIHS2Session>();

            this.Init();
        }

        /// <summary>
        /// Executed same query on N sessions for given duration
        /// </summary>
        public void RunQuery(string query, int parallism, TimeSpan duration)
        {
            this.Slots = new ConcurrentQueue<string>();
            for (int i=0; i< parallism; i++)
            {
                this.Slots.Enqueue("S" + i);
            }

            // Synchorous execution till duration 
            var tokenSource = new CancellationTokenSource();
            var executors = new List<Task>();
            foreach (var e in this.Sessions)
            {
                executors.Add(ExecuteSession(e, query, tokenSource.Token));
            }

            Task.Delay(duration).Wait();
            tokenSource.Cancel();

            Task.WaitAll(executors.ToArray());

            // TODO: Collect the executor (TBD) results
        }

        protected void Init()
        {
            // Serial initialization 
            for (int i = 0; i < NumOfSessions; i++)
            {
                this.Sessions.Add(new HDIHS2Session(this.ConnectionString, "C" + i.ToString()));
            }
        }

        protected async Task ExecuteSession(HDIHS2Session session, string query, CancellationToken token)
        {
            // Wait 1M before startign execution 
            await Task.Delay(TimeSpan.FromMinutes(1));

            while(! token.IsCancellationRequested)
            {
                string slotName = string.Empty;
                if (this.Slots.TryDequeue(out slotName))
                {
                    try
                    {
                        session.ExecuteQuery(query, slotName);
                    }
                    catch (Exception ex)
                    {
                        Trace.TraceError(ex.ToString());
                    }
                    finally
                    {
                        this.Slots.Enqueue(slotName);
                    }

                }
                await Task.Delay(TimeSpan.FromSeconds(5));
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
                    foreach (var e in this.Sessions)
                    {
                        e.Dispose();
                    }

                    this.Sessions = new List<HDIHS2Session>();
                    // TODO: dispose managed state (managed objects).
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~HDIHS2SessionManager() {
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
