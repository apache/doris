#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

##############################################################
# This script is used to load clickbench data into Doris
##############################################################

set -eo pipefail

ROOT=$(dirname "$0")
ROOT=$(
    cd "$ROOT"
    pwd
)

CURDIR=${ROOT}
source "${CURDIR}/../conf/cluster.conf"

CLICKBENCH_DATA_DIR="${DATA_SAVE_DIR}"

if [ "_${CLICKBENCH_DATA_DIR}" == "_clickbench-data" ];then
    CLICKBENCH_DATA_DIR="$CURDIR/../bin/${DATA_SAVE_DIR}"
fi
echo "CLICKBENCH_DATA_DIR: ${CLICKBENCH_DATA_DIR}"

usage() {
    echo "
This script is used to load ClickBench data, 
will use mysql client to connect Doris server which is specified in conf/doris-cluster.conf file.
Usage: $0
  Optional options:
     -c             parallelism to load data of hits, default is 5.

  Eg.
    $0              load data using default value.
    $0 -c 10        load hits table data using parallelism 10.
  "
    exit 1
}

OPTS=$(getopt \
    -n $0 \
    -o '' \
    -o 'hc:' \
    -- "$@")
eval set -- "$OPTS"

PARALLEL=5
HELP=0

if [[ $# == 0 ]]; then
    usage
fi

while true; do
    case "$1" in
    -h)
        HELP=1
        shift
        ;;
    -c)
        PARALLEL=$2
        shift 2
        ;;
    --)
        shift
        break
        ;;
    *)
        echo "Internal error"
        exit 1
        ;;
    esac
done

if [[ "${HELP}" -eq 1 ]]; then
    usage
fi

echo "Parallelism: ${PARALLEL}"

check_prerequest() {
    local CMD=$1
    local NAME=$2
    if ! $CMD; then
        echo "$NAME is missing. This script depends on cURL to load data to Doris."
        exit 1
    fi
}

check_prerequest "mysql --version" "mysql"
check_prerequest "curl --version" "curl"
check_prerequest "wget --version" "wget"

source $CURDIR/../conf/cluster.conf

echo "SUITE: $SUITE"
echo "FE_HOST: $FE_HOST"
echo "FE_QUERY_PORT: $FE_QUERY_PORT"
echo "USER: $USER"
echo "PASSWORD: $PASSWORD"

touch $CURDIR/../clickbench_load_result.csv
truncate -s0 $CURDIR/../clickbench_load_result.csv

clt=""
exec_clt=""
if [ -z "${PASSWORD}" ];then
    clt="mysql -h${FE_HOST} -u${USER} -P${FE_QUERY_PORT} -D${DB} "
    exec_clt="mysql -vvv -h$FE_HOST -u$USER -P$FE_QUERY_PORT -D$DB "
else
    clt="mysql -h${FE_HOST} -u${USER} -p${PASSWORD} -P${FE_QUERY_PORT} -D${DB} "
    exec_clt="mysql -vvv -h$FE_HOST -u$USER -p${PASSWORD} -P$FE_QUERY_PORT -D$DB "
fi


function check_doris_conf() {
    cv=$($clt -e 'admin show frontend config' | grep 'stream_load_default_timeout_second' | awk '{print $2}')
    if (($cv < 3600)); then
        echo "advise: revise your Doris FE's conf to set 'stream_load_default_timeout_second=3600' or above"
    fi

    cv=$(curl "${BE_HOST}:${BE_WEBSERVER_PORT}/varz" 2>/dev/null | grep 'streaming_load_max_mb' | awk -F'=' '{print $2}')
    if (($cv < 16000)); then
        echo -e "advise: revise your Doris BE's conf to set 'streaming_load_max_mb=16000' or above and 'flush_thread_num_per_store=5' to speed up load."
    fi
}

$clt -e "truncate table hits"

echo "start..."
start=$(date +%s.%N)
echo "Loading data for table: hits, with ${PARALLEL} parallel"
function load() {
    echo "$@"
    curl --location-trusted \
        -u "$USER":"$PASSWORD" \
        -H "columns:WatchID,JavaEnable,Title,GoodEvent,EventTime,EventDate,CounterID,ClientIP,RegionID,UserID,CounterClass,OS,UserAgent,URL,Referer,IsRefresh,RefererCategoryID,RefererRegionID,URLCategoryID,URLRegionID,ResolutionWidth,ResolutionHeight,ResolutionDepth,FlashMajor,FlashMinor,FlashMinor2,NetMajor,NetMinor,UserAgentMajor,UserAgentMinor,CookieEnable,JavascriptEnable,IsMobile,MobilePhone,MobilePhoneModel,Params,IPNetworkID,TraficSourceID,SearchEngineID,SearchPhrase,AdvEngineID,IsArtifical,WindowClientWidth,WindowClientHeight,ClientTimeZone,ClientEventTime,SilverlightVersion1,SilverlightVersion2,SilverlightVersion3,SilverlightVersion4,PageCharset,CodeVersion,IsLink,IsDownload,IsNotBounce,FUniqID,OriginalURL,HID,IsOldCounter,IsEvent,IsParameter,DontCountHits,WithHash,HitColor,LocalEventTime,Age,Sex,Income,Interests,Robotness,RemoteIP,WindowName,OpenerName,HistoryLength,BrowserLanguage,BrowserCountry,SocialNetwork,SocialAction,HTTPError,SendTiming,DNSTiming,ConnectTiming,ResponseStartTiming,ResponseEndTiming,FetchTiming,SocialSourceNetworkID,SocialSourcePage,ParamPrice,ParamOrderID,ParamCurrency,ParamCurrencyID,OpenstatServiceName,OpenstatCampaignID,OpenstatAdID,OpenstatSourceID,UTMSource,UTMMedium,UTMCampaign,UTMContent,UTMTerm,FromTag,HasGCLID,RefererHash,URLHash,CLID" \
        -T "$@" http://"$FE_HOST":"$FE_HTTP_PORT"/api/"$DB"/hits/_stream_load
}

# set parallelism
[[ -e /tmp/fd1 ]] || mkfifo /tmp/fd1
exec 3<>/tmp/fd1
rm -rf /tmp/fd1

for ((i = 1; i <= PARALLEL; i++)); do
    echo >&3
done

date
for file in "${CLICKBENCH_DATA_DIR}"/hits_split*; do
    read -r -u3
    {
        load "${file}"
        echo >&3
    } &
done
# wait for child thread finished
wait
# 删除文件标识符
exec 3>&-

end=$(date +%s.%N)
echo "load cost time: $((end-start)) seconds"

cost_time=$(echo "scale=4;$end-$start" | bc)

echo -n "hits_stream_load:" | tee -a $CURDIR/../clickbench_load_result.csv
echo -n "$cost_time" | tee -a $CURDIR/../clickbench_load_result.csv
echo "" | tee -a $CURDIR/../clickbench_load_result.csv