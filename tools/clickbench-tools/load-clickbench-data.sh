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
# DATA_DIR=/mnt/disk1/stephen/data/clickbench

usage() {
    echo "
This script is used to load ClickBench data, 
will use mysql client to connect Doris server which is specified in conf/doris-cluster.conf file.
Usage: $0 
  "
    exit 1
}

OPTS=$(getopt \
    -n $0 \
    -o '' \
    -o 'h' \
    -- "$@")
eval set -- "$OPTS"

HELP=0
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
    *)
        echo "Internal error"
        exit 1
        ;;
    esac
done

if [[ ${HELP} -eq 1 ]]; then
    usage
    exit
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

echo "FE_HOST: $FE_HOST"
echo "FE_HTTP_PORT: $FE_HTTP_PORT"
echo "USER: $USER"
echo "PASSWORD: $PASSWORD"
echo "DB: $DB"

function check_doirs_conf() {
    echo "check if Doris FE's and BE's conf"
    exit_flag=false

    cv=$(mysql -h$FE_HOST -P$FE_QUERY_PORT -u$USER -e 'admin show frontend config' | grep 'stream_load_default_timeout_second' | awk '{print $2}')
    if (($cv < 3600)); then
        echo "please revise your Doris FE's conf to set 'stream_load_default_timeout_second=3600' or above"
        exit_flag=true
    fi

    cv=$(curl "${BE_HOST}:${BE_WEBSERVER_PORT}/varz" 2>/dev/null | grep 'streaming_load_max_mb' | awk -F'=' '{print $2}')
    if (($cv < 16000)); then
        echo -e "please revise your Doris BE's conf to set 'streaming_load_max_mb=16000' or above \noptional: 'flush_thread_num_per_store=5' to speed up load."
        exit_flag=true
    fi

    if ${exit_flag}; then exit 1; fi
}

function load() {
    echo "(1/2) prepare clickbench data file"
    if [ ! -f $DATA_DIR/hits.tsv.gz ]; then
        echo "can not find data file in $DATA_DIR, will download it"
        if [ ! -d $DATA_DIR ]; then mkdir -p $DATA_DIR; fi
        cd $DATA_DIR
        wget --continue 'https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz'
        # wget --continue 'https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/ClickBench/hits.tsv.gz'
        cd -
    fi

    echo "(2/2) load clickbench data file $DATA_DIR/hits.tsv.gz into Doris"
    curl --location-trusted \
        -u $USER:$PASSWORD \
        -T "$DATA_DIR/hits.tsv.gz" \
        -H "label:hits" \
        -H "compress_type:GZ" \
        -H "column_separator:\t" \
        -H "columns:WatchID,JavaEnable,Title,GoodEvent,EventTime,EventDate,CounterID,ClientIP,RegionID,UserID,CounterClass,OS,UserAgent,URL,Referer,IsRefresh,RefererCategoryID,RefererRegionID,URLCategoryID,URLRegionID,ResolutionWidth,ResolutionHeight,ResolutionDepth,FlashMajor,FlashMinor,FlashMinor2,NetMajor,NetMinor,UserAgentMajor,UserAgentMinor,CookieEnable,JavascriptEnable,IsMobile,MobilePhone,MobilePhoneModel,Params,IPNetworkID,TraficSourceID,SearchEngineID,SearchPhrase,AdvEngineID,IsArtifical,WindowClientWidth,WindowClientHeight,ClientTimeZone,ClientEventTime,SilverlightVersion1,SilverlightVersion2,SilverlightVersion3,SilverlightVersion4,PageCharset,CodeVersion,IsLink,IsDownload,IsNotBounce,FUniqID,OriginalURL,HID,IsOldCounter,IsEvent,IsParameter,DontCountHits,WithHash,HitColor,LocalEventTime,Age,Sex,Income,Interests,Robotness,RemoteIP,WindowName,OpenerName,HistoryLength,BrowserLanguage,BrowserCountry,SocialNetwork,SocialAction,HTTPError,SendTiming,DNSTiming,ConnectTiming,ResponseStartTiming,ResponseEndTiming,FetchTiming,SocialSourceNetworkID,SocialSourcePage,ParamPrice,ParamOrderID,ParamCurrency,ParamCurrencyID,OpenstatServiceName,OpenstatCampaignID,OpenstatAdID,OpenstatSourceID,UTMSource,UTMMedium,UTMCampaign,UTMContent,UTMTerm,FromTag,HasGCLID,RefererHash,URLHash,CLID" \
        http://$FE_HOST:$FE_HTTP_PORT/api/$DB/hits/_stream_load

}

echo "start..."
start=$(date +%s)
check_doirs_conf
load
end=$(date +%s)
echo "load cost time: $((end - start)) seconds"
