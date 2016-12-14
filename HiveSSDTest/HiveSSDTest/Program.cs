using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;

/*
 * 
        double usableMemoryInGb = Math.Min(memoryInGb * 0.9, memoryInGb - 1.5);

        public void FillLlapContainerSizes(double usableMemoryInGb, int cores, int workerNodeCount, Dictionary<string, string> returnConfigs)
        {
            var minContainerSizeInMb = GetMinContainerSizeInMb(usableMemoryInGb, cores, useMsCoresLogic: false);
            var containerSizeInMb = Math.Min(minContainerSizeInMb, 1024); // Cap container size at 1GB
            var numOfContainers = (int)Math.Floor(GbInMb * usableMemoryInGb / containerSizeInMb); // Don't constrain on #cores

            // Set container and #contaienrs in return config 
            returnConfigs[ComputeClusterConfigSettingsKeys.ConfigContainerSizeInMb] = containerSizeInMb.ToString();
            returnConfigs[ComputeClusterConfigSettingsKeys.ConfigContainersCount] = numOfContainers.ToString();

            // Use only 80% for LLAP. 
            // 80% is adjusted at containers and cores. In-case of fractions floor is used. 
            // In-case of fractions, the resulting LLAP configuration will be less than 80% of original value
            const int LLAPQueueCapacity = 80;
            numOfContainers = (numOfContainers * LLAPQueueCapacity) / 100; 
            cores = (cores * LLAPQueueCapacity) / 100;

            // Leave 40% for LLAP cache => 60%
            var tezContainerSize = (int)Math.Min(minContainerSizeInMb, 4096); // Cap container size at 4GB 
            var numOfAmSlotsPerNode = workerNodeCount > 1 ? 1 : 2;
            var llapDaemonExecutors = cores - numOfAmSlotsPerNode;
            var llapDaemonSize = containerSizeInMb * (numOfContainers - numOfAmSlotsPerNode);

            var llapTotalContianerMemory = llapDaemonExecutors * tezContainerSize;
            var llapHeapSize = (int)Math.Max(llapTotalContianerMemory * 0.8, llapTotalContianerMemory - 1024);

            var llapCacheSize = llapDaemonSize - tezContainerSize * llapDaemonExecutors;
            var llapConcurrentQueries = Math.Min(workerNodeCount - 1, 32);
            if (workerNodeCount <= 1)
            {
                llapConcurrentQueries = 1;
            }

            returnConfigs[ComputeClusterConfigSettingsKeys.HadoopConfigHiveTezContainerSize] = tezContainerSize.ToString();
            returnConfigs[ComputeClusterConfigSettingsKeys.LlapDaemonHeapSize] = llapHeapSize.ToString();
            returnConfigs[ComputeClusterConfigSettingsKeys.LlapDaemonContainerMb] = llapDaemonSize.ToString();
            returnConfigs[ComputeClusterConfigSettingsKeys.LlapDaemonNumOfExecutors] = llapDaemonExecutors.ToString();
            returnConfigs[ComputeClusterConfigSettingsKeys.LlapDaemonIoThreadPoolSize] = llapDaemonExecutors.ToString();
            returnConfigs[ComputeClusterConfigSettingsKeys.LlapIoMemorySize] = llapCacheSize.ToString();
            returnConfigs[ComputeClusterConfigSettingsKeys.LlapConcurrentQueries] = llapConcurrentQueries.ToString();
            returnConfigs[ComputeClusterConfigSettingsKeys.LlapNumOfDaemons] = workerNodeCount.ToString();
            returnConfigs[ComputeClusterConfigSettingsKeys.LlapQueueCapacity] = LLAPQueueCapacity.ToString();
            returnConfigs[ComputeClusterConfigSettingsKeys.SliderAmContainerSize] = containerSizeInMb.ToString();
        } 

        private int GetMinContainerSizeInMb(double memoryInGb, int cores, bool useMsCoresLogic = true)
        {
            // We used to use a formula from HWX to calculate minimum container size.
            // The calculation doesn't make sense on large VM node. E.g. customers 
            // are complaining why there can be at most 2 containers of 9GB on a D12 node.

            // The new algorithm is to set the maximum number of containers to 2*cores,
            // with minimum container size of 768 (MB)
            var coresToUse = useMsCoresLogic ? cores * 2 : cores;
            double containerSizeInMb = GbInMb * memoryInGb / coresToUse;

            // Clip to known good sizes (1Gb+overhead, 4Gb+overhead, 8Gb+overhead)
            // The minimum container size should always be 768MB, just like on A3 node. 
            if (containerSizeInMb < 1024)
                return 768;
            if (containerSizeInMb < 1536)
                return 1024;
            if (containerSizeInMb < 3072)
                return 1536; 
            if (containerSizeInMb < 4608)
                return 3072;
            if (containerSizeInMb < 9216)
                return 4608;

            return 9216;
        }

        private int GetXmxValueFromContainerMemory(int memoryInMb)
        {
            // Given the container memmory size, return the Java heap size
            // We need to reserve some memory for the Java code
            if (memoryInMb == 768)
                return 512;
            else if (memoryInMb == 1024)
                return 768;
            else if (memoryInMb == 1536)
                return 1024;
            else if (memoryInMb == 2048)
                return 1536;
            else if (memoryInMb == 3072)
                return 2560;
            else if (memoryInMb == 4608)
                return 4096;
            else if (memoryInMb == 9216)
                return 8192;

            // We should never hit this.
            return 512;
        }

 * 
 * 
 * 
 * 
 *             // Hive interactive env
        var HiveEnvKeys = new string[] { 
                ComputeClusterConfigSettingsKeys.LlapNumOfDaemons,
                ComputeClusterConfigSettingsKeys.LlapDaemonHeapSize,
                ComputeClusterConfigSettingsKeys.LlapQueueCapacity,
                ComputeClusterConfigSettingsKeys.SliderAmContainerSize,
            };
        AddTypeConfigurationValues(HadoopConfigurationType.HiveInteractiveEnv, HiveEnvKeys, calculatedValues);

        // Hive interactive site
        var HiveInteractiveKeys = new string[] { 
                ComputeClusterConfigSettingsKeys.LlapDaemonContainerMb,
                ComputeClusterConfigSettingsKeys.LlapDaemonNumOfExecutors,
                ComputeClusterConfigSettingsKeys.LlapDaemonIoThreadPoolSize,
                ComputeClusterConfigSettingsKeys.LlapIoMemorySize,
                ComputeClusterConfigSettingsKeys.LlapConcurrentQueries,
            };
        AddTypeConfigurationValues(HadoopConfigurationType.HiveInteractiveSite, HiveInteractiveKeys, calculatedValues);

 * 
 */
namespace HiveSSDTest {
    class ComputeClusterConfigSettingsKeys {
        public const string LlapNumOfDaemons = "num_llap_nodes";
        public const string LlapDaemonHeapSize = "llap_heap_size";
        public const string LlapDaemonContainerMb = "hive.llap.daemon.yarn.container.mb";
        public const string LlapDaemonNumOfExecutors = "hive.llap.daemon.num.executors";
        public const string LlapDaemonIoThreadPoolSize = "hive.llap.io.threadpool.size";
        public const string LlapIoMemorySize = "hive.llap.io.memory.size";
        public const string LlapConcurrentQueries = "hive.server2.tez.sessions.per.default.queue";
        public const string LlapQueueCapacity = "llap_queue_capacity";
        public const string SliderAmContainerSize = "slider_am_container_mb";
        public const string LlapEnableMmap = "hive.llap.io.allocator.mmap";
        public const string LlapEnableMmapPath = "hive.llap.io.allocator.mmap.path";

        public const string YarnVCores = "yarn.nodemanager.resource.cpu-vcores";
        public const string YarnMemory = "yarn.nodemanager.resource.memory-mb";
        public const string TezContainerSize = "hive.tez.container.size";
    }


    class Program {
        #region overwrites
        public static readonly Dictionary<string, string> CoreSiteOverwrites = new Dictionary<string, string>() {
            { "fs.azure.account.keyprovider.kkhdistore.blob.core.windows.net", "org.apache.hadoop.fs.azure.SimpleKeyProvider"},
            { "fs.azure.account.key.kkhdistore.blob.core.windows.net", "" },
        };

        public static readonly Dictionary<string, string> YarnOverwrites = new Dictionary<string, string>() {
            { "yarn.nodemanager.resource.percentage-physical-cpu-limit", "100"},
            { ComputeClusterConfigSettingsKeys.YarnVCores, "16" },   // Update vcores as needed
            { "yarn.nodemanager.pmem-check-enabled", "false" },
            { "yarn.nodemanager.vmem-check-enabled", "false" },
            { "yarn.scheduler.maximum-allocation-vcores", "16" }, // update as needed
            { "yarn.nodemanager.container-monitor.procfs-tree.smaps-based-rss.enabled", "true" },
            { ComputeClusterConfigSettingsKeys.YarnMemory, "102400" },
        };

        // https://kkllap1203.azurehdinsight.net/api/v1/clusters/kkllap1203/configurations?type=hive-interactive-env&tag=TOPOLOGY_RESOLVED
        public static readonly Dictionary<string, string> HSIEnvOverwrites = new Dictionary<string, string>() {
                { ComputeClusterConfigSettingsKeys.LlapNumOfDaemons, "10"},
                { ComputeClusterConfigSettingsKeys.LlapDaemonHeapSize, "44032"},
                { "llap_headroom_space", "6144"},
                { ComputeClusterConfigSettingsKeys.LlapQueueCapacity, "100"},
                { ComputeClusterConfigSettingsKeys.SliderAmContainerSize, "1024"},
                { "enable_hive_interactive", "true" },
        };

        // https://kkllap1203.azurehdinsight.net/api/v1/clusters/kkllap1203/configurations?type=hive-interactive-site&tag=TOPOLOGY_RESOLVED
        public static readonly Dictionary<string, string> HSISiteOverwrites = new Dictionary<string, string>() {
                { ComputeClusterConfigSettingsKeys.LlapDaemonNumOfExecutors, "16" },    // yarn.nodemanager.resource.cpu-vcores
                { ComputeClusterConfigSettingsKeys.LlapDaemonIoThreadPoolSize, "16" },
                { ComputeClusterConfigSettingsKeys.LlapDaemonContainerMb, "102400" },    // daemon size 100GB -> 80% -> ~80GB
                { ComputeClusterConfigSettingsKeys.LlapIoMemorySize, "36864" },         // Cache size
                { ComputeClusterConfigSettingsKeys.LlapConcurrentQueries, "1" },
                { ComputeClusterConfigSettingsKeys.LlapEnableMmap, "false" },
                { "hive.llap.io.memory.mode", "cache" },
        };

        // https://kkllap1203.azurehdinsight.net/api/v1/clusters/kkllap1203/configurations?type=hive-interactive-site&tag=TOPOLOGY_RESOLVED
        public static readonly Dictionary<string, string> HiveSiteOverwrites = new Dictionary<string, string>() {
                { ComputeClusterConfigSettingsKeys.TezContainerSize, "4096" },
        };

        public static readonly Dictionary<string, string> TezInteractiveSiteOverwrites = new Dictionary<string, string>() {
                { "tez.am.resource.memory.mb", "1024" },
        };

        #endregion

        static void Main(string[] args) {
            ServicePointManager.ServerCertificateValidationCallback += (object sender, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors) => { return true; };

            // Don't place / at the end of URL
            var baseuri = "";
            var password = "";

            var userName = "admin";

            var nodes = 10;
            var cores = 16;
            var containerSize = 3072;
            var totalMemory = 100 * 1024; // 100GB
            var ssdsize = 50 * 1024; // 300GB
            var llapContainerMemory = totalMemory - containerSize;
            var cache = llapContainerMemory - cores * containerSize;

            var kkhdistorePassword = "";
            Program.CoreSiteOverwrites["fs.azure.account.key.kkhdistore.blob.core.windows.net"] = kkhdistorePassword;

            Program.YarnOverwrites[ComputeClusterConfigSettingsKeys.YarnVCores] = cores.ToString(); 
            Program.YarnOverwrites["yarn.scheduler.maximum-allocation-vcores"] = cores.ToString(); 
            Program.YarnOverwrites[ComputeClusterConfigSettingsKeys.YarnMemory] = totalMemory.ToString(); 

            Program.HSIEnvOverwrites[ComputeClusterConfigSettingsKeys.LlapNumOfDaemons] = nodes.ToString();
            Program.HSIEnvOverwrites[ComputeClusterConfigSettingsKeys.LlapDaemonHeapSize] = ((int)((llapContainerMemory - cache) * 0.8)).ToString(); // Leave 2GB per node for AM etc...

            Program.HSISiteOverwrites[ComputeClusterConfigSettingsKeys.LlapDaemonNumOfExecutors] = cores.ToString();
            Program.HSISiteOverwrites[ComputeClusterConfigSettingsKeys.LlapDaemonIoThreadPoolSize] = cores.ToString();
            Program.HSISiteOverwrites[ComputeClusterConfigSettingsKeys.LlapDaemonContainerMb] = llapContainerMemory.ToString();
            Program.HSISiteOverwrites[ComputeClusterConfigSettingsKeys.LlapIoMemorySize] = cache.ToString();          // Cache size

            ////Program.HSISiteOverwrites[ComputeClusterConfigSettingsKeys.LlapIoMemorySize] = (cache + ssdsize).ToString();          // Cache size
            ////Program.HSISiteOverwrites[ComputeClusterConfigSettingsKeys.LlapEnableMmap] = "true";                  // Enable SSD cache
            ////Program.HSISiteOverwrites[ComputeClusterConfigSettingsKeys.LlapEnableMmapPath] = "/tmp/ssd";          // SSD cache path

            Program.HiveSiteOverwrites[ComputeClusterConfigSettingsKeys.TezContainerSize] = (containerSize).ToString();
            Program.TezInteractiveSiteOverwrites[ComputeClusterConfigSettingsKeys.TezContainerSize] = (containerSize).ToString();

            var ambariclient = new SimpleRestClient(baseuri, userName, password);
            //ambariclient.ApplyOverwrites("core-site", "scale-down", Program.CoreSiteOverwrites);
            //ambariclient.ApplyOverwrites("yarn-site", "scale-down", Program.YarnOverwrites);
            ambariclient.ApplyOverwrites("hive-site", "scale-down", Program.HiveSiteOverwrites);
            ambariclient.ApplyOverwrites("hive-interactive-env", "kirankk-reset", Program.HSIEnvOverwrites);
            ambariclient.ApplyOverwrites("hive-interactive-site", "scale-down", Program.HSISiteOverwrites);
            ambariclient.ApplyOverwrites("tez-interactive-site", "scale-down", Program.TezInteractiveSiteOverwrites);

            // ambariclient.RestartServices(new string[] { "HDFS" });
            // ambariclient.RestartServices(new string[] { "HIVE", "YARN", "HDFS" });
            ambariclient.RestartServices(new string[] { "HIVE" });
        }
    }

    /// <summary>
    /// Simple HTTP REST client that encapsulates some common utilities
    /// and retries internally.
    /// </summary>
    public class SimpleRestClient {
        private Uri _baseUri;
        private string _username;
        private string _password;
        private string _requestedBy;
        private string _contentType;

        private void JoinOverwriteDict(Dictionary<string, string> left, Dictionary<string, string> right) {
            if (right != null) {
                foreach (var e in right) {
                    if (left.Keys.Contains(e.Key)) {
                        left[e.Key] = e.Value;
                    } else {
                        left.Add(e.Key, e.Value);
                    }
                }
            }
        }

        public void ApplyOverwrites(string cfgName, string prefix, Dictionary<string, string> overwrites) {
            UpdateConfiguration(cfgName, prefix, (e) => {
                JoinOverwriteDict(e, overwrites);
            });
        }

        public void UpdateConfiguration(string cfgName, string prefix, Action<Dictionary<string, string>> updater) {
            var clusterBaseUri = GetClusterApiBasePath();
            var latestCfgsQueryString = "fields=Clusters/desired_configs";

            // Get version information 
            var result = this.SubmitHttpGetWebRequest(clusterBaseUri, latestCfgsQueryString);
            var jsonPayload = JObject.Parse(result);
            var yarnSiteToken = jsonPayload.SelectToken(string.Join(".", "$.Clusters.desired_configs", cfgName));
            var currentYarnSiteTag = yarnSiteToken.SelectToken("$.tag").Value<string>();

            var cfgPath = clusterBaseUri + "/configurations";
            var cfgQueryString = "type=" + cfgName + "&tag=" + currentYarnSiteTag;
            var yarnCfgPayload = JObject.Parse(this.SubmitHttpGetWebRequest(cfgPath, cfgQueryString));
            var properties = yarnCfgPayload.SelectToken("$.items[0].properties").ToString();
            var propCollection = JsonConvert.DeserializeObject<Dictionary<string, string>>(properties);

            updater(propCollection);


            var tag = prefix + "-" + DateTimeOffset.UtcNow.ToString("yyyy-MM-dd-HH-mm-ss");
            var payload = "[ { \"Clusters\": { \"desired_config\":[ { \"type\":\"" + cfgName + "\",\"tag\":\"" + tag + "\", \"properties\": " + JsonConvert.SerializeObject(propCollection) + " }]}}]";

            var returnPayload = this.SubmitHttpPutWebRequestInternal(clusterBaseUri, null, payload);
        }

        public void AddressStaleConfig() {
            var clusterBaseUri = GetClusterApiBasePath() + "/host_components";
            var queryString = "HostRoles/stale_configs=true&fields=HostRoles/service_name,HostRoles/state,HostRoles/host_name,HostRoles/stale_configs,&minimal_response=true";
            var staleCfgResult = this.SubmitHttpGetWebRequest(clusterBaseUri, queryString);

            var comp = from e in JObject.Parse(staleCfgResult)["items"].Children()["HostRoles"]
                       select new { key = e["service_name"].Value<string>() + "/" + e["component_name"].Value<string>(), host = e["hosts"].Value<string>() };

            var grouped = comp
                .GroupBy(e => e.key, e => e.host)
                .Select(e => new {
                    service_name = e.Key.Split(new char[] { '/' })[0],
                    component_name = e.Key.Split(new char[] { '/' })[1],
                    hosts = string.Join(",", e.ToArray())
                });

            var p = JsonConvert.SerializeObject(grouped);
        }

        ////public void RestartAllServices(string serviceName) {
        ////    var payload = "{\"RequestInfo\":{\"query\":\"host_components/HostRoles/service_name.in(" + serviceName + ")&Hosts/maintenance_state:OFF\"}}";
        ////    // POST https://kkllap1203.azurehdinsight.net/api/v1/clusters/kkllap1203/hosts?fields=host_components/HostRoles/component_name&minimal_response=true

        ////    var baseUri = this.GetClusterApiBasePath() + "/hosts" ;
        ////    var response = this.SubmitHttpPostWebRequestInternal(baseUri, "fields=host_components/HostRoles/component_name&minimal_response=true", payload);
        ////}

        public void RestartServices(string[] servicenames) {
            // https://cwiki.apache.org/confluence/display/AMBARI/Modify+configurations
            // curl --user admin:admin -i -X PUT -d '{"RequestInfo": {"context": "Stop HDFS"}, "ServiceInfo": {"state": "INSTALLED"}}' http://AMBARI_SERVER_HOST:8080/api/v1/clusters/CLUSTER_NAME/services/HDFS
            // curl--user admin:admin - i - X PUT - d '{"RequestInfo": {"context": "Start HDFS"}, "ServiceInfo": {"state": "STARTED"}}' http://AMBARI_SERVER_HOST:8080/api/v1/clusters/CLUSTER_NAME/services/HDFS

            foreach (var servicename in servicenames) {
                string payload = "{\"RequestInfo\": {\"context\": \"Stop " + servicename + "\"}, \"ServiceInfo\": {\"state\": \"INSTALLED\"}}";
                var ep = GetClusterApiBasePath() + "/services/" + servicename;
                this.SubmitHttpPutWebRequestInternal(ep, null, payload);
            }

            Task.Delay(TimeSpan.FromMinutes(5)).Wait();

            // Start oderder should be reverse
            foreach (var servicename in servicenames.Reverse()) {
                string payload = "{\"RequestInfo\": {\"context\": \"Start " + servicename + "\"}, \"ServiceInfo\": {\"state\": \"STARTED\"}}";
                var ep = GetClusterApiBasePath() + "/services/" + servicename;
                this.SubmitHttpPutWebRequestInternal(ep, null, payload);
            }
        }

        public string GetClusterApiBasePath() {
            var clustersPath = "/api/v1/clusters/";
            var clustersPayload = this.SubmitHttpGetWebRequest(clustersPath);
            var clusterName = JObject.Parse(clustersPayload).SelectToken("$.items[0].Clusters.cluster_name").Value<string>();
            return clustersPath + clusterName;
        }

        public SimpleRestClient(string clusterUri, string username, string password) :
            this(new Uri(clusterUri), username, password, "ambari", null) {
        }

        private SimpleRestClient(Uri baseUri, string username, string password, string requestedBy, string contentType) {
            _username = username;
            _password = password;
            _baseUri = baseUri;
            _requestedBy = requestedBy;
            _contentType = contentType;
        }

        public string SubmitHttpPostWebRequest(string path, string query, byte[] data) {
            // return RetryUtil.ExecuteHttpWithRetry(SubmitHttpPostWebRequestInternal, path, query, data);
            throw new NotImplementedException();
        }

        /// <summary>
        /// Same as SubmitHttpPostWebRequest, but no retries internally
        /// </summary>
        public string SubmitHttpPostWebRequestWithoutRetries(string path, string query, byte[] data) {
            return SubmitHttpPostWebRequestInternal(path, query, data);
        }

        /// <summary>
        /// Submits an HTTP GET request on the given path with the given query.
        /// </summary>
        /// <param name="path"></param>
        /// <param name="query"></param>
        /// <returns>result if server returned any, otherwise empty string.</returns>
        public string SubmitHttpGetWebRequest(string path, string query = null) {
            return SubmitHttpGetWebRequestInternal(path, query);
        }

        /// <summary>
        /// Same as SubmitHttpGetWebRequest but with no retries internally.
        /// </summary>
        public string SubmitHttpGetWebRequestWithoutRetries(string path, string query = null) {
            return SubmitHttpGetWebRequestInternal(path, query);
        }

        private string SubmitHttpUpdateWebRequestInternal(string path, string query, string method, byte[] data) {
            HttpWebRequest request = CreateHttpWebRequest(path, query);
            request.Method = method;

            if (data != null) {
                request.ContentLength = data.Length;

                // write the request stream
                Stream newStream = request.GetRequestStream();
                newStream.Write(data, 0, data.Length);
                newStream.Close();
            }

            using (WebResponse response = (HttpWebResponse)request.GetResponse()) {
                string responseJson = null;
                using (StreamReader sr = new StreamReader(response.GetResponseStream())) {
                    responseJson = sr.ReadToEnd();
                }

                return responseJson;
            }
        }

        private string SubmitHttpDeleteWebRequestInternal(string path, string query, byte[] data) {
            return SubmitHttpUpdateWebRequestInternal(path, query, "DELETE", data);
        }

        private string SubmitHttpPostWebRequestInternal(string path, string query, string data) {
            var bytesData = Encoding.ASCII.GetBytes(data);
            return this.SubmitHttpPostWebRequestInternal(path, query, bytesData);
        }

        private string SubmitHttpPostWebRequestInternal(string path, string query, byte[] data) {
            return SubmitHttpUpdateWebRequestInternal(path, query, "POST", data);
        }

        private string SubmitHttpPutWebRequestInternal(string path, string query, string data) {
            var bytesData = Encoding.ASCII.GetBytes(data);
            return this.SubmitHttpPutWebRequestInternal(path, query, bytesData);
        }

        private string SubmitHttpPutWebRequestInternal(string path, string query, byte[] data) {
            return SubmitHttpUpdateWebRequestInternal(path, query, "PUT", data);
        }

        private string SubmitHttpGetWebRequestInternal(string path, string query = null) {
            HttpWebRequest request = CreateHttpWebRequest(path, query);
            using (WebResponse response = (HttpWebResponse)request.GetResponse()) {
                string responseJson = null;
                using (StreamReader sr = new StreamReader(response.GetResponseStream())) {
                    responseJson = sr.ReadToEnd();
                }

                return responseJson;
            }
        }

        protected virtual HttpWebRequest CreateHttpWebRequest(string path, string query = null) {
            // combine the base Uri with the given path
            UriBuilder builder = new UriBuilder(_baseUri);
            builder.Path += path;
            if (query != null) {
                builder.Query = query;
            }

            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(builder.Uri);

            // TODO: Support unicode later
            // The encoding here should be "iso-8859-1" based on a specification. However, some SDKs uses UTF-encoding 
            // Using iso-8859-1 here would improve CRUD reliabilty, however, users will fail afterward in accessing ambari.
            // Now, being conservative, we keep using ascii here and reject other character sets in AmbariCnofiguration_1_7.cs
            string credentials = Convert.ToBase64String(Encoding.ASCII.GetBytes(_username + ":" + _password));
            request.Headers.Add("Authorization", "Basic " + credentials);

            if (_requestedBy != null) {
                request.Headers.Add("X-Requested-By", _requestedBy);
            }

            if (!String.IsNullOrWhiteSpace(_contentType)) {
                request.ContentType = _contentType;
            }

            return request;
        }
    }
}
