using System;
using System.Linq;
using System.Collections;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading.Tasks;
using System.Data.Odbc;
using System.Data.Common;
using System.Diagnostics;
using System.Collections.Generic;

namespace HiveOdbcUt
{
    [TestClass]
    public class OdbcUnitTests
    {
        /// <summary>
        /// Connection string are hardcoded for visibility, you shold move them to config for real usage
        /// </summary>
        private static string HiveDsnConnectionString = "DSN=Sample Microsoft Hive DSN";
        private static string ConnectionStringDelimiter = ";";
        private static string OdbcSettingsPrefix = "SSP_";

        [TestMethod]
        public void ListTables()
        {
            var cmd = "show tables";
            ExecHqlCmd("", cmd, null, false).Wait();
        }

        [TestMethod]
        public void SimpleGroupByQuery()
        {
            var cmd = "INSERT OVERWRITE TABLE S0 PARTITION (country) SELECT clientid,querytime,market, deviceplatform,devicemake, devicemodel, state, querydwelltime,sessionid,sessionpagevieworder, country FROM hivesampletable;";
            var settings = new string[] { "hive.execution.engine=tez" };
            ExecHqlCmd("", cmd, null, false).Wait();
        }

        [TestMethod]
        public void SingleSelectQuery()
        {
            var cmd = "SELECT market, COUNT(*) FROM hivesampletable GROUP BY market;";
            var settings = new string[] { "hive.execution.engine=tez" };
            ExecHqlCmd("", cmd, null, false).Wait();
        }

        [TestMethod]
        public void ParallelQueryExecution()
        {
            var parallism = 100;
            var tasks = new List<Task>();
            var command = "SELECT market, COUNT(*) FROM hivesampletable GROUP BY market";

            for (int i = 0; i < parallism; i++)
            {
                var j = i;
                var t = ExecHqlCmd(j.ToString(), command);
                /// Hive ODBC driver serializes the execution, its not a true async implementation
                /// You can get parallism by explicty starting queries in parallel as below command 
                //var t = Task.Factory.StartNew(() => ExecHqlCmd(j.ToString(), command));
                tasks.Add(t);
            }

            Task.WaitAll(tasks.ToArray());
        }

        static async Task ExecHqlCmd(string traceId, string hqlCmd, string[] settings = null, bool skipResults = true)
        {
            Trace.TraceInformation(traceId + " : strated");

            var connectionString = OdbcUnitTests.GetConnectionString(OdbcUnitTests.HiveDsnConnectionString, settings);
            Trace.TraceInformation("{0} Using connection string => {1} ", traceId, connectionString);

            try
            {
                using (OdbcConnection conn = new OdbcConnection(connectionString))
                {
                    var startTime = DateTimeOffset.UtcNow;

                    await conn.OpenAsync();
                    using (OdbcCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = hqlCmd;

                        using (DbDataReader dr = await cmd.ExecuteReaderAsync())
                        {
                            var completionTime = DateTimeOffset.UtcNow;
                            var execTime = completionTime.Subtract(startTime);
                            Trace.TraceInformation(traceId + " : execution time (sec) -> " + execTime.TotalSeconds);

                            if (!skipResults)
                            {
                                while (dr.Read())
                                {
                                    var columns = new List<string>();
                                    for (int i = 0; i < dr.VisibleFieldCount; i++)
                                    {
                                        columns.Add(dr.GetValue(i).ToString());
                                    }
                                    Trace.TraceInformation(traceId + " : " + string.Join(", ", columns));
                                }
                            }
                        }
                    }
                }

                Trace.TraceInformation(traceId + " : completed");
            }
            catch (Exception)
            {
                Console.WriteLine("Iteration failed");
            }
        }

        private static string GetConnectionString(string hiveDsnConnectionString, string[] settings)
        {
            return settings == null ? hiveDsnConnectionString
                            : string.Join(OdbcUnitTests.ConnectionStringDelimiter, OdbcUnitTests.GetPrefixedSettings(settings), hiveDsnConnectionString);

        }

        private static string[] GetPrefixedSettings(string[] settings)
        {
            return settings.Select(e => OdbcUnitTests.OdbcSettingsPrefix + e).ToArray();
        }
    }
}
