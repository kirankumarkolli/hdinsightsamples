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
            var manager = new HDIHS2SessionManager(connectionString, 200);

            var query = "select * from hivesampletable LIMIT 5000";
            manager.RunQuery(query, 10, TimeSpan.FromMinutes(60));
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
}
