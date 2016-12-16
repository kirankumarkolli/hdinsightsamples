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
        private const string connectionString = "";

        static void Main(string[] args)
        {
            var numCores = 4; 
            var numIterSamples = new int[] { 20, 50, 100, 200, 300};
            var userSamples = UsersSamples(numCores);

            // NumOfIter as outer loop gets the results fast
            // More iterations is a long-haul 
            foreach (var iter in numIterSamples)
            {
                foreach (var users in userSamples)
                {
                    Sampler(users, iter);
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
            var result = new List<int>();
            for (int i = 1; i<= 2 * numCores; i++)
            {
                result.Add(i);
            }

            for (int i = 3; i< 20; i++)
            {
                result.Add( i * numCores);
            }

            return result;
        }

        static void Sampler(int numUsers, int numIter)
        {
            try
            {
                Trace.TraceInformation(string.Empty);
                Trace.TraceInformation(string.Empty);
                Trace.TraceInformation(string.Empty);
                Trace.TraceInformation("NumUsers: {0}, NumIter: {1}M", numUsers, numIter);
                var manager = new HDIHS2SessionManager(connectionString, numUsers);

                var query = "select * from <table> LIMIT 5000";
                var initQuery = "DROP TABLE <table>;" +
                                "CREATE TABLE <table> (clientid string,querytime string,market string,deviceplatform string,devicemake string,devicemodel string, state string,querydwelltime double,sessionid bigint,sessionpagevieworder bigint)PARTITIONED BY (country string);" +
                                "INSERT OVERWRITE TABLE <table> PARTITION (country) SELECT clientid,querytime,market, deviceplatform,devicemake, devicemodel, state, querydwelltime,sessionid,sessionpagevieworder, country FROM hivesampletable;";

                var targetTableName = "hivesampletable";
                manager.RunQuery(
                    numUsers,
                    numIter,
                    (name) => new Tuple<string, string>(
                            initQuery.Replace("<table>", targetTableName),
                            query.Replace("<table>", targetTableName)));
            }
            catch(Exception e)
            {
                Trace.TraceError("Iteration{0}X{1} failed with {2}", numUsers, numIter, e.ToString());
            }
        }
    }
}
