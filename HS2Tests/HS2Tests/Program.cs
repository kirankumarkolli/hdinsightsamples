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
        private const string connectionString = "";

        static void Main(string[] args)
        {
            var manager = new HDIHS2SessionManager(connectionString, 200);

            var query = "select * from hivesampletable LIMIT 5000";
            manager.RunQuery(query, 10, TimeSpan.FromMinutes(60));
        }
    }
}
