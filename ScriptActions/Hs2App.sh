#! /bin/bash
AMBARICONFIGS_SH=/var/lib/ambari-server/resources/scripts/configs.sh
ACTIVE_AMBARI_HOST=headnodehost
AMBARI_PORT=8080
LOG_FILE_NAME=`basename "$0"`
LOG_FILE=/tmp/$LOG_FILE_NAME.log

# Exit codes
#    1 -> Ran as non ROOT
#    2 -> AddComponent to Host through Ambari failed
#    3 -> Ambari async reqeust processing failed/timed-out after 5M
#    4 -> Ambari async request submission failed
#    5 -> Ambari clusters enumeration failed
#    6 -> HIVE_SERVER component is missing even after install
#    7 -> HIVE_SERVER STARTED validation check failed
#
# Troubleshooting procedure:
#   Run withe bash -x {script name}


# Get Ambari user name and password
AMBARI_USERID=$(echo -e "import hdinsight_common.Constants as Constants\nprint Constants.AMBARI_WATCHDOG_USERNAME" | python)
AMBARI_PASSWD=$(echo -e "import hdinsight_common.ClusterManifestParser as ClusterManifestParser\nimport hdinsight_common.Constants as Constants\nimport base64\nbase64pwd = ClusterManifestParser.parse_local_manifest().ambari_users.usersmap[Constants.AMBARI_WATCHDOG_USERNAME].password\nprint base64.b64decode(base64pwd)" | python 2>/dev/nulll)
CURL_AMBARI_COMMAND="curl -u $AMBARI_USERID:$AMBARI_PASSWD -H X-Requested-By:ambari --write-out %{http_code} --silent --output $LOG_FILE "

# $1 -> SERVICE_NAME
# $2 -> COMPONENT_NAME
# $3 -> TARGET_HOST_NAME
# $4 -> Cluster name
installHostComponent() {
    response=$($CURL_AMBARI_COMMAND -X GET http://$ACTIVE_AMBARI_HOST:$AMBARI_PORT/api/v1/clusters/$4/hosts/$3/host_components/$2)
    echo "Lookup ($3, $2) = $response"

    if [ $response -eq 404 -o $response -eq 403 ]; then
        response=$($CURL_AMBARI_COMMAND -X POST -d "{\"RequestInfo\":{\"context\":\"Install $2\"},\"Body\":{\"host_components\":[{\"HostRoles\":{\"component_name\":\"$2\"}}]}}" http://$ACTIVE_AMBARI_HOST:$AMBARI_PORT/api/v1/clusters/$4/hosts?Hosts/host_name=$3)
        echo "Add ($3, $2) = $response"
        if [ $response -ne 201 -a $response -ne 409 ]
        then
            // Put enough infomration onto console for troublehsooting 
            echo "Add ($3, $2) failed with $response" >&2
            echo $LOG_FILE >&2
            exit 2
        fi
    fi

    response=$($CURL_AMBARI_COMMAND -X PUT -d "{\"RequestInfo\":{\"context\":\"Install $2\",\"operation_level\":{\"level\":\"HOST_COMPONENT\",\"cluster_name\":\"$4\",\"host_name\":\"$3\",\"service_name\":\"$SERVICE_NAME\"}},\"Body\":{\"HostRoles\":{\"state\":\"INSTALLED\"}}}" http://$ACTIVE_AMBARI_HOST:$AMBARI_PORT/api/v1/clusters/$4/hosts/$3/host_components/$2?HostRoles/state=INIT)
    echo "Install ($3, $2) = $response"
    waitForRequestCompletion $response
}

# $1 -> Ambari REST call return code
waitForRequestCompletion() {
    if [ $1 -eq 202 ]
    then
        reqUri=$(cat $LOG_FILE | grep href | cut -d '"' -f 4)

        # Loop for completion, with timeout of ~300 seconds (5M)
        var=0
        while [ $var -lt 30 ]
        do
            response=$($CURL_AMBARI_COMMAND -X GET $reqUri)
            status=$(cat $LOG_FILE | grep request_status | cut -d '"' -f 4)
                echo "Status check for $reqUri resulted in $status"
            if [[ "$status" == "COMPLETED" ]]
            then
                break;
            fi
                
            sleep 10s
            ((var++))
        done
        
        if [[ "$status" != "COMPLETED" ]]
        then
            echo "Ambari request $reqUri failed OR timed-out after 5M" >&2
            exit 3
        fi
    else 
        if [ $1 -ne 200 ]
        then
            echo "Ambari request failed with errorcode $1" >&2
            exit 4
        fi
    fi
}

# $1 -> TARGET_HOST_NAME
# $2 -> Cluster name
getHiveServerStatus() {
    response=$($CURL_AMBARI_COMMAND -X GET http://$ACTIVE_AMBARI_HOST:$AMBARI_PORT/api/v1/clusters/$2/hosts/$1/host_components/HIVE_SERVER)
    if [ $response -eq 200 ]
    then
        currentState=`cat $LOG_FILE | grep \"state\" | cut -d '"' -f 4`
        echo $currentState;
    else
        echo "Unexpected: HIVE_SERVER component NOT FOUND on host $1" >&2
        exit 6
    fi
}

# $1 -> TARGET_HOST_NAME
# $2 -> Cluster name
startHiveServer() {
    currentState=$(getHiveServerStatus ${1} ${2})
    if [[ "$currentState" -ne "STARTED" ]]
    then
        echo "Starting HIVE_SERVER"
        response=$($CURL_AMBARI_COMMAND -X PUT -d "{\"RequestInfo\":{\"context\":\"Start HiveServer2\",\"operation_level\":{\"level\":\"HOST_COMPONENT\",\"cluster_name\":\"$2\",\"host_name\":\"$1\",\"service_name\":\"HIVE\"}},\"Body\":{\"HostRoles\":{\"state\":\"STARTED\"}}}" http://$ACTIVE_AMBARI_HOST:$AMBARI_PORT/api/v1/clusters/$2/hosts/$1/host_components/HIVE_SERVER?HostRoles/state=$currentState)
        echo "StartHiveServer ($1, $2) = $response"
        waitForRequestCompletion $response
    fi
    
    currentState=$(getHiveServerStatus ${1} ${2})
    if [[ "$currentState" -ne "STARTED" ]]
    then
        echo "HIVE_SERVER status is not STARTED" >&2
        exit 7
    fi
}

setCurrentClusterName() {
    echo "Getting cluster name from Ambari"
    respose=$($CURL_AMBARI_COMMAND -X GET http://$ACTIVE_AMBARI_HOST:$AMBARI_PORT/api/v1/clusters)
    if [ $respose -ne 200 ]
    then
       echo "Ambari clusters enumeration failed" >&2
       exit 5;
    fi

    CLUSTERNAME=`cat $LOG_FILE | grep cluster_name | cut -d '"' -f 4`
    if [[ -z "$CLUSTERNAME" ]]; then
       echo "Cluster name population from Ambari payload failed" >&2
       exit 5
    fi
}

if [ "$(id -u)" != "0" ]; then
    echo "The script has to be run as root." >&2
    exit 1
fi

setCurrentClusterName
FQ_HOST_NAME=`hostname -f`

echo "*** HOST         : $FQ_HOST_NAME"
echo "*** CLUSTERNAME  : $CLUSTERNAME"
echo "*** Ambari user  : $AMBARI_USERID"

installHostComponent 'YARN' 'YARN_CLIENT' $FQ_HOST_NAME $CLUSTERNAME
installHostComponent 'MAPREDUCE2' 'MAPREDUCE2_CLIENT' $FQ_HOST_NAME $CLUSTERNAME
installHostComponent 'HIVE' 'HIVE_SERVER' $FQ_HOST_NAME $CLUSTERNAME
startHiveServer $FQ_HOST_NAME $CLUSTERNAME
