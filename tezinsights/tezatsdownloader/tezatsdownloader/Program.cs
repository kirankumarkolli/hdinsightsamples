using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace tezatsdownloader {
    class Program {
        static void Main(string[] args) {
            ServicePointManager.UseNagleAlgorithm = false;
            ServicePointManager.MaxServicePoints = 48;

            var clusterDns = "https://kkhs2tst.azurehdinsight.net/";
            var httpUserName = "admin";
            var httpPassword = ;

            // var basePath = GetClusterBasePath(clusterDns, httpUserName, httpPassword);
            // var qids = GetQueryIds(clusterDns, httpUserName, httpPassword);

            var opRootDir = @"D:\code\hdisamples\tezinsights";
            var queryId = "hive_20160602021252_cdf6a0a4-d0d0-4e0c-bedb-69505fd0e2a0";
            DumpQueryDetails(queryId, opRootDir, clusterDns, httpUserName, httpPassword);
        }

        static void DumpQueryDetails(string qid, string directory, string dnsName, string userName, string passwd) {
            var rootDir = Path.Combine(directory, qid);
            if (! Directory.Exists(rootDir)) {
                Directory.CreateDirectory(rootDir);
            }

            var downloadMap = new Dictionary<string, string> {
                { "query.json", "ws/v1/timeline/HIVE_QUERY_ID/{HIVE_QUERY_ID}" },
                { "dag.json", "ws/v1/timeline/TEZ_DAG_ID?primaryFilter=callerId:{HIVE_QUERY_ID}" },
                { "vertices.json", "ws/v1/timeline/TEZ_VERTEX_ID?primaryFilter=TEZ_DAG_ID:{TEZ_DAG_ID}" },
                { "tasks.json", "ws/v1/timeline/TEZ_TASK_ID?primaryFilter=TEZ_DAG_ID:{TEZ_DAG_ID}" },
                { "tasks_attempts.json", "ws/v1/timeline/TEZ_TASK_ATTEMPT_ID?primaryFilter=TEZ_DAG_ID:{TEZ_DAG_ID}" },
                };

            var settings = new Dictionary<string, string> {
                { "HIVE_QUERY_ID", qid }
            };

            foreach (var e in downloadMap) {
                var fileName = e.Key;
                var urlSuffix = e.Value;

                foreach(var se in settings) {
                    urlSuffix = urlSuffix.Replace("{" + se.Key + "}", se.Value);
                }

                // Downloading from urlSuffix
                var jPayload = GetAllEntities(dnsName + urlSuffix, userName, passwd);
                var firstElement = jPayload.SelectToken("");
                if (jPayload.Children().Count() == 1) {
                    // List root element
                    var rootName = jPayload.Children().First().Path;
                    firstElement = jPayload[rootName][0];
                }

                var etype = firstElement["entitytype"];
                var eValue = firstElement["entity"];

                settings[etype.Value<string>()] = eValue.Value<string>();
                var varName = "var " + Path.GetFileNameWithoutExtension(fileName) + "Json=";
                File.WriteAllText(Path.Combine(rootDir, fileName), varName + jPayload.ToString());
            }
        }

        static JObject GetAllEntities(string url, string userName, string password) {
            var jPayload = CallGet(url, userName, password);
            if (jPayload.Children().Count() == 1) {
                // List root element
                var rootName = jPayload.Children().First().Path;
                var resultSet = new JArray();
                var lastEntityValue = string.Empty;
                while (true) {
                    foreach(var e in jPayload[rootName].Children()) {
                        bool newEntriesAdded = false;
                        if (! e["entity"].Value<string>().Equals(lastEntityValue)) {
                            resultSet.Add(e);
                            newEntriesAdded = true;
                        }

                        // http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/TimelineServer.html
                        // If not it will get into infinite loop
                        if (!newEntriesAdded) {
                            goto Done;
                        }
                    }

                    var lastToken = jPayload[rootName].Last;
                    lastEntityValue = lastToken["entity"].Value<string>();

                    var iterurl = url + "&fromId=" + lastEntityValue;
                    jPayload = CallGet(iterurl, userName, password);
                }

                // Labels and goto can be avoided but skpt of ease of reading 
                Done:
                    var r = new JObject();
                    r.Add(rootName, resultSet);
                    return r;
            } else {
                return jPayload;
            }
        }

        static string[] GetQueryIds(string dnsName, string userName, string passwd) {
            var atsPath = "ws/v1/timeline/HIVE_QUERY_ID";
            var repPayloadAsJson = CallGet(dnsName + atsPath, userName, passwd);

            return repPayloadAsJson["entities"]
                            .Select(e => new { St = (long)e.SelectToken("starttime"), Id = (string)e.SelectToken("entity") })
                            .OrderByDescending(e => e.St)
                            .Select(e => e.Id)
                            .ToArray();
        }

        static string GetClusterBasePath(string dnsName, string userName, string passwd) {
            var clusterBasePath = "api/v1/clusters";
            var repPayloadAsJson = CallGet(dnsName + clusterBasePath, userName, passwd);

            var clusterName = repPayloadAsJson.SelectToken("items[0].Clusters.cluster_name");
            return dnsName + clusterBasePath + "/" + clusterName;
        }

        static JObject CallGet(string url, string userName, string password) {
            using (var handler = new HttpClientHandler()) {
                handler.CookieContainer = new CookieContainer();

                using (var httpClient = new HttpClient(handler)) {
                    httpClient.Timeout = TimeSpan.FromMinutes(5);
                    httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(
                                    "Basic",
                                    Convert.ToBase64String(ASCIIEncoding.ASCII.GetBytes(string.Format("{0}:{1}", userName, password))));

                    var resp = httpClient.SendAsync(new HttpRequestMessage(HttpMethod.Get, url)).Result;

                    var repPayload = resp.Content.ReadAsStringAsync().Result;
                    return JObject.Parse(repPayload);
                }
            }
        }
    }
}
