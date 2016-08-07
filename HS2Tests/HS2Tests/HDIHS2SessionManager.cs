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
        internal string ConnectionString { get; set; }
        protected int NumOfSessions { get; set; }

        protected List<HDIHS2Session> Sessions { get; set; }
        protected ConcurrentQueue<Tuple<string, string>> Slots { get; set; }
        protected ConcurrentBag<HS2ActivityRecord> Activities { get; set; }

        public HDIHS2SessionManager(string connectionString, int sessionCount)
        {
            this.ConnectionString = connectionString;
            this.NumOfSessions = sessionCount;
            this.Sessions = new List<HDIHS2Session>();
            this.Activities = new ConcurrentBag<HS2ActivityRecord>();

            this.Init();
        }

        /// <summary>
        /// Executed same query on N sessions for given duration
        /// </summary>
        public void RunQuery(int parallism, Func<string, Tuple<string, string>> slotQuery, int numIter)
        {
            // Clear activity cache 
            this.Activities = new ConcurrentBag<HS2ActivityRecord>();

            this.Slots = new ConcurrentQueue<Tuple<string, string>>();
            List<Task> initQueries = new List<Task>();
            for (int i=0; i< parallism; i++)
            {
                var slotName = "S" + i;
                var queries = slotQuery(slotName);

                // Execute the init queries and then place the 
                // TODO: Fix the init query set-up issues 
                // initQueries.Add(ExecQuery(this.Sessions.ElementAt(i), slotName, queries.Item1));

                var slotObject = new Tuple<string, string>(slotName, queries.Item2);
                this.Slots.Enqueue(slotObject);
            }

            Task.WaitAll(initQueries.ToArray());

            // Synchorous execution till duration 
            var tokenSource = new CancellationTokenSource();
            var executors = new List<Task>();
            foreach (var e in this.Sessions)
            {
                executors.Add(ExecuteSession(e, numIter));
            }

            Task.WaitAll(executors.ToArray());

            // Save and clearr all activities into 
        }

        internal async Task ExecQuery(HDIHS2Session session, string slotName, string query)
        {
            await Task.Yield();
            session.ExecuteQuery(query, slotName, publishActivities: false);
        }

        internal void NotifyActivity(HS2ActivityRecord activity)
        {
            this.Activities.Add(activity);
        }

        protected void Init()
        {
            // Serial initialization 
            for (int i = 0; i < NumOfSessions; i++)
            {
                this.Sessions.Add(new HDIHS2Session("C" + i.ToString(), this));
            }
        }

        protected async Task ExecuteSession(HDIHS2Session session, int numIter)
        {
            // Wait 1M before startign execution 
            await Task.Delay(TimeSpan.FromMinutes(1));

            for (int i=0; i< numIter; i++)
            {
                Tuple<string, string> slotDetails = new Tuple<string, string>(string.Empty, string.Empty);
                if (this.Slots.TryDequeue(out slotDetails))
                {
                    try
                    {
                        Trace.TraceInformation("Slot {0} started new execution", slotDetails.Item1);
                        session.ExecuteQuery(slotDetails.Item2, slotDetails.Item1, publishActivities: true);
                    }
                    catch (Exception ex)
                    {
                        Trace.TraceError("Slot {0} execution failed with", ex.ToString());
                    }
                    finally
                    {
                        this.Slots.Enqueue(slotDetails);
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
