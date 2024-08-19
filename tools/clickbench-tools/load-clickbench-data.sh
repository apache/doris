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
DATA_DIR=$CURDIR/

usage() {
    echo "
This script is used to load ClickBench data, 
will use mysql client to connect Doris server which is specified in conf/doris-cluster.conf file.
Usage: $0 <options>
  Optional options:
    -x              use transaction id. multi times of loading with the same id won't load duplicate data.

  Eg.
    $0              load data using default value.
    $0 -x blabla    use transaction id \"blabla\".
  "
    exit 1
}

OPTS=$(getopt \
    -n $0 \
    -o '' \
    -o 'hx:' \
    -- "$@")
eval set -- "$OPTS"

HELP=0
TXN_ID=""

while true; do
    case "$1" in
    -h)
        HELP=1
        shift
        ;;
    --)
        shift
        break
        ;;
    -x)
        TXN_ID=$2
        shift 2
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

source $CURDIR/conf/doris-cluster.conf

wget_pids=()

echo "FE_HOST: $FE_HOST"
echo "FE_HTTP_PORT: $FE_HTTP_PORT"
echo "USER: $USER"
echo "PASSWORD: $PASSWORD"
echo "DB: $DB"

function check_doris_conf() {
    cv=$(mysql -h$FE_HOST -P$FE_QUERY_PORT -u$USER -e 'admin show frontend config' | grep 'stream_load_default_timeout_second' | awk '{print $2}')
    if (($cv < 3600)); then
        echo "advise: revise your Doris FE's conf to set 'stream_load_default_timeout_second=3600' or above"
    fi

    cv=$(curl "${BE_HOST}:${BE_WEBSERVER_PORT}/varz" 2>/dev/null | grep 'streaming_load_max_mb' | awk -F'=' '{print $2}')
    if (($cv < 16000)); then
        echo -e "advise: revise your Doris BE's conf to set 'streaming_load_max_mb=16000' or above and 'flush_thread_num_per_store=5' to speed up load."
    fi
}

function load() {
    echo "(1/2) prepare clickbench data file"
    need_download=false
    cd $DATA_DIR
    for i in $(seq 0 9); do
        if [ ! -f "$DATA_DIR/hits_split${i}" ]; then
            echo "will download hits_split${i} to $DATA_DIR"
            wget --continue "https://doris-test-data.oss-cn-hongkong.aliyuncs.com/ClickBench/hits_split${i}" &
            PID=$!
            wget_pids[${#wget_pids[@]}]=$PID
        fi
    done

    echo "wait for download task done..."
    wait
    cd -

    echo "(2/2) load clickbench data file $DATA_DIR/hits_split[0-9] into Doris"
    for i in $(seq 0 9); do
        echo -e "
        start loading hits_split${i}"
        if [[ -z ${TXN_ID} ]]; then
            curl --location-trusted \
                -u $USER:$PASSWORD \
                -T "$DATA_DIR/hits_split${i}" \
                -H "columns:WatchID,JavaEnable,Title,GoodEvent,EventTime,EventDate,CounterID,ClientIP,RegionID,UserID,CounterClass,OS,UserAgent,URL,Referer,IsRefresh,RefererCategoryID,RefererRegionID,URLCategoryID,URLRegionID,ResolutionWidth,ResolutionHeight,ResolutionDepth,FlashMajor,FlashMinor,FlashMinor2,NetMajor,NetMinor,UserAgentMajor,UserAgentMinor,CookieEnable,JavascriptEnable,IsMobile,MobilePhone,MobilePhoneModel,Params,IPNetworkID,TraficSourceID,SearchEngineID,SearchPhrase,AdvEngineID,IsArtifical,WindowClientWidth,WindowClientHeight,ClientTimeZone,ClientEventTime,SilverlightVersion1,SilverlightVersion2,SilverlightVersion3,SilverlightVersion4,PageCharset,CodeVersion,IsLink,IsDownload,IsNotBounce,FUniqID,OriginalURL,HID,IsOldCounter,IsEvent,IsParameter,DontCountHits,WithHash,HitColor,LocalEventTime,Age,Sex,Income,Interests,Robotness,RemoteIP,WindowName,OpenerName,HistoryLength,BrowserLanguage,BrowserCountry,SocialNetwork,SocialAction,HTTPError,SendTiming,DNSTiming,ConnectTiming,ResponseStartTiming,ResponseEndTiming,FetchTiming,SocialSourceNetworkID,SocialSourcePage,ParamPrice,ParamOrderID,ParamCurrency,ParamCurrencyID,OpenstatServiceName,OpenstatCampaignID,OpenstatAdID,OpenstatSourceID,UTMSource,UTMMedium,UTMCampaign,UTMContent,UTMTerm,FromTag,HasGCLID,RefererHash,URLHash,CLID" \
                http://$FE_HOST:$FE_HTTP_PORT/api/$DB/hits/_stream_load
        else
            curl --location-trusted \
                -u $USER:$PASSWORD \
                -T "$DATA_DIR/hits_split${i}" \
                -H "label:${TXN_ID}_${i}" \
                -H "columns:WatchID,JavaEnable,Title,GoodEvent,EventTime,EventDate,CounterID,ClientIP,RegionID,UserID,CounterClass,OS,UserAgent,URL,Referer,IsRefresh,RefererCategoryID,RefererRegionID,URLCategoryID,URLRegionID,ResolutionWidth,ResolutionHeight,ResolutionDepth,FlashMajor,FlashMinor,FlashMinor2,NetMajor,NetMinor,UserAgentMajor,UserAgentMinor,CookieEnable,JavascriptEnable,IsMobile,MobilePhone,MobilePhoneModel,Params,IPNetworkID,TraficSourceID,SearchEngineID,SearchPhrase,AdvEngineID,IsArtifical,WindowClientWidth,WindowClientHeight,ClientTimeZone,ClientEventTime,SilverlightVersion1,SilverlightVersion2,SilverlightVersion3,SilverlightVersion4,PageCharset,CodeVersion,IsLink,IsDownload,IsNotBounce,FUniqID,OriginalURL,HID,IsOldCounter,IsEvent,IsParameter,DontCountHits,WithHash,HitColor,LocalEventTime,Age,Sex,Income,Interests,Robotness,RemoteIP,WindowName,OpenerName,HistoryLength,BrowserLanguage,BrowserCountry,SocialNetwork,SocialAction,HTTPError,SendTiming,DNSTiming,ConnectTiming,ResponseStartTiming,ResponseEndTiming,FetchTiming,SocialSourceNetworkID,SocialSourcePage,ParamPrice,ParamOrderID,ParamCurrency,ParamCurrencyID,OpenstatServiceName,OpenstatCampaignID,OpenstatAdID,OpenstatSourceID,UTMSource,UTMMedium,UTMCampaign,UTMContent,UTMTerm,FromTag,HasGCLID,RefererHash,URLHash,CLID" \
                http://$FE_HOST:$FE_HTTP_PORT/api/$DB/hits/_stream_load
        fi
    done
}

function signal_handler() {

    for PID in ${wget_pids[@]}; do
        kill -9 $PID
    done
}

trap signal_handler 2 3 6 15

echo "start..."
start=$(date +%s)
check_doris_conf
load
end=$(date +%s)
echo "load cost time: $((end - start)) seconds"

run_sql() {
  echo $@
  mysql -h$FE_HOST -u$USER -P$FE_QUERY_PORT -D$DB -e "$@"
}

echo '============================================'
echo "analyzing table hits"
run_sql "analyze table hits with sync;"
echo "analyzing table hits finished!"
