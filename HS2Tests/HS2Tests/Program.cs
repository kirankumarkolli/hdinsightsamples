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
        private const string connectionString = "Data Source=https://kkhs2tst.azurehdinsight.net/hive2; User ID=admin; Password=KKProd~123";

        static void Main(string[] args)
        {
            var manager = new HDIHS2SessionManager(connectionString, 30);

            var query = "select * from <table> LIMIT 5000";
            var initQuery = "DROP TABLE <table>;" + 
                            "CREATE TABLE <table> (clientid string,querytime string,market string,deviceplatform string,devicemake string,devicemodel string, state string,querydwelltime double,sessionid bigint,sessionpagevieworder bigint)PARTITIONED BY (country string);" + 
                            "INSERT OVERWRITE TABLE <table> PARTITION (country) SELECT clientid,querytime,market, deviceplatform,devicemake, devicemodel, state, querydwelltime,sessionid,sessionpagevieworder, country FROM hivesampletable;";

            manager.RunQuery(
                1, 
                (name) => new Tuple<string, string>(
                        initQuery.Replace("<table>", name), 
                        query.Replace("<table>", name)),
                TimeSpan.FromMinutes(5));
        }
    }
}
