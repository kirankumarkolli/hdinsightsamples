using Microsoft.HDInsight.Hive.Data;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HS2Tests
{
    public class Program
    {
        private const int CoolOffTimeInMin = 5; // Ideally clean-up function will be great
        private const string connectionString = "Data Source=https://kkhs2tst2.azurehdinsight.net/hive2; User ID=admin; Password=KKProd~123";

        static void Main(string[] args)
        {
            var numCores = 4; 
            var numIterSamples = new int[] { 50, 100, 200, 300};
            var userSamples = UsersSamples(numCores);

            // NumOfIter as outer loop gets the results fast
            // More iterations is a long-haul 
            foreach (var iter in numIterSamples)
            {
                foreach (var users in userSamples)
                {
                    NoDagQueryWorkload(users, iter);
                    DagQueryWorkload(users, iter);
                    Task.Delay(TimeSpan.FromMinutes(Program.CoolOffTimeInMin)).Wait();
                }
            }

            Console.WriteLine("Enter to close");
            Console.ReadLine();
        }

        /// <summary>
        /// Serially raise the load until twice the #cores, then raise multiple 
        /// </summary>
        static List<int> UsersSamples(int numCores)
        {
            // RASHIMG: this will return the concurrency samples. For demo if you want only one valude them return list with that value. 
            var result = new List<int>();
            for (int i = 1; i<= 2 * numCores; i++)
            {
                result.Add(i);
            }

            for (int i = 3; i< 10; i++)
            {
                result.Add( i * numCores);
            }

            return result;
        }

        static void Workload(string name, int numUsers, int numIter, Func<string, Tuple<string, string>> slotQueryFunc)
        {
            try
            {
                Trace.TraceInformation(string.Empty);
                Trace.TraceInformation(string.Empty);
                Trace.TraceInformation(string.Empty);
                Trace.TraceInformation("Starting workload:{0} NumUsers:{1}, NumIter:{2}", name, numUsers, numIter);
                var manager = new HDIHS2SessionManager(connectionString, numUsers);

                manager.RunQuery(
                    name,
                    numUsers,
                    numIter,
                    slotQueryFunc);
            }
            catch (Exception e)
            {
                Trace.TraceError("Workload:{0} Iteration:{1}X{2} failed with {3}", name, numUsers, numIter, e.ToString());
            }
        }

        static void NoDagQueryWorkload(int numUsers, int numIter)
        {
            var noDagQuery = "select * from hivesampletable LIMIT 5000";
            Func<string, Tuple<string, string>> slotQueryFunc = (name) => new Tuple<string, string>(string.Empty, noDagQuery);
            Program.Workload("NODAG", numUsers, numIter, slotQueryFunc);
        }

        static void DagQueryWorkload(int numUsers, int numIter)
        {
            var noDagQuery = "select market, count(*) from hivesampletable group by market";
            Func<string, Tuple<string, string>> slotQueryFunc = (name) => new Tuple<string, string>(string.Empty, noDagQuery);
            Program.Workload("DAG", numUsers, numIter, slotQueryFunc);
        }
    }
}
