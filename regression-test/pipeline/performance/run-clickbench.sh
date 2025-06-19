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

# Build Step: Command Line
: <<EOF
#!/bin/bash

if [[ -f "${teamcity_build_checkoutDir:-}"/regression-test/pipeline/performance/run-clickbench.sh ]]; then
    cd "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/
    bash -x run-clickbench.sh
else
    echo "Build Step file missing: regression-test/pipeline/performance/run-clickbench.sh" && exit 1
fi
EOF

#####################################################################################
## run-clickbench.sh content ##

# shellcheck source=/dev/null
# check_clickbench_table_rows, stop_doris, set_session_variable, check_clickbench_result
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/doris-utils.sh
# shellcheck source=/dev/null
# create_an_issue_comment
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/github-utils.sh
# shellcheck source=/dev/null
# upload_doris_log_to_oss
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/oss-utils.sh

if ${DEBUG:-false}; then
    pr_num_from_trigger="28431"
    commit_id_from_trigger="5f5c4c80564c76ff4267fc4ce6a5408498ed1ab5"
    target_branch="master"
fi
echo "#### Check env"
if [[ -z "${teamcity_build_checkoutDir}" ]]; then echo "ERROR: env teamcity_build_checkoutDir not set" && exit 1; fi
if [[ -z "${pr_num_from_trigger}" ]]; then echo "ERROR: env pr_num_from_trigger not set" && exit 1; fi
if [[ -z "${commit_id_from_trigger}" ]]; then echo "ERROR: env commit_id_from_trigger not set" && exit 1; fi
if [[ -z "${target_branch}" ]]; then echo "ERROR: env target_branch not set" && exit 1; fi

# shellcheck source=/dev/null
source "$(bash "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/get-or-set-tmp-env.sh 'get')"
if ${skip_pipeline:=false}; then echo "INFO: skip build pipline" && exit 0; else echo "INFO: no skip"; fi

echo "#### Run clickbench test on Doris ####"
DORIS_HOME="${teamcity_build_checkoutDir}/output"
export DORIS_HOME
exit_flag=0

(
    set -e
    shopt -s inherit_errexit

    host="127.0.0.1"
    query_port=$(get_doris_conf_value "${DORIS_HOME}"/fe/conf/fe.conf query_port)
    backup_session_variables_file="${teamcity_build_checkoutDir}/regression-test/pipeline/performance/clickbench/conf/backup_session_variables.sql"
    opt_session_variables_file="${teamcity_build_checkoutDir}/regression-test/pipeline/performance/clickbench/conf/opt_session_variables.sql"

    echo "#### 1. backup session variables to file ${backup_session_variables_file}"
    if ! restart_doris; then echo "ERROR: Restart doris failed" && exit 1; fi
    backup_session_variables() {
        _IFS="${IFS}"
        IFS=$'\n'
        while read -r line; do
            if [[ -z "${line}" || "${line}" == "#"* ]]; then continue; fi
            k="${line/set global /}"
            k="${k%=*}"
            v=$(mysql -h"${host}" -P"${query_port}" -uroot -e"show variables like '${k}'\G" | grep " Value: ")
            v="${v/*Value: /}"
            echo "set global ${k}=${v};" >>"${backup_session_variables_file}"
        done <"${opt_session_variables_file}"
        IFS="${_IFS}"
    }
    backup_session_variables

    echo "#### 2. optimize doris config"
    cp -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/clickbench/conf/fe_custom.conf "${DORIS_HOME}"/fe/conf/
    cp -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/clickbench/conf/be_custom.conf "${DORIS_HOME}"/be/conf/
    target_branch="$(echo "${target_branch}" | sed 's| ||g;s|\.||g;s|-||g')" # remove space、dot、hyphen from branch name
    sed -i "s|^meta_dir=/data/doris-meta-\${branch_name}|meta_dir=/data/doris-meta-${target_branch}|g" "${DORIS_HOME}"/fe/conf/fe_custom.conf
    sed -i "s|^storage_root_path=/data/doris-storage-\${branch_name}|storage_root_path=/data/doris-storage-${target_branch}|g" "${DORIS_HOME}"/be/conf/be_custom.conf
    if ! restart_doris; then echo "ERROR: Restart doris failed" && exit 1; fi

    echo "#### 3. optimize session variables"
    cat "${opt_session_variables_file}"
    mysql -h"${host}" -P"${query_port}" -uroot -e"source ${opt_session_variables_file};"

    echo "#### 4. check data rows"
    data_home="/data/clickbench" # no / at the end
    db_name="clickbench"
    if ! check_clickbench_table_rows "${db_name}"; then
        echo "INFO: need to load clickbench data"
        if ${force_load_data:-false}; then echo "INFO: force_load_data is true"; else echo "ERROR: force_load_data is false" && exit 1; fi
        # prepare data
        mkdir -p "${data_home}"

        # create table and load data
        mysql -h"${host}" -P"${query_port}" -uroot -e "DROP DATABASE IF EXISTS ${db_name}"
        mysql -h"${host}" -P"${query_port}" -uroot -e "CREATE DATABASE IF NOT EXISTS ${db_name}" && sleep 10
        mysql -h"${host}" -P"${query_port}" -uroot "${db_name}" -e"
            CREATE TABLE IF NOT EXISTS  hits (
                CounterID INT NOT NULL, 
                EventDate DateV2 NOT NULL, 
                UserID BIGINT NOT NULL, 
                EventTime DateTimeV2 NOT NULL, 
                WatchID BIGINT NOT NULL, 
                JavaEnable SMALLINT NOT NULL,
                Title STRING NOT NULL,
                GoodEvent SMALLINT NOT NULL,
                ClientIP INT NOT NULL,
                RegionID INT NOT NULL,
                CounterClass SMALLINT NOT NULL,
                OS SMALLINT NOT NULL,
                UserAgent SMALLINT NOT NULL,
                URL STRING NOT NULL,
                Referer STRING NOT NULL,
                IsRefresh SMALLINT NOT NULL,
                RefererCategoryID SMALLINT NOT NULL,
                RefererRegionID INT NOT NULL,
                URLCategoryID SMALLINT NOT NULL,
                URLRegionID INT NOT NULL,
                ResolutionWidth SMALLINT NOT NULL,
                ResolutionHeight SMALLINT NOT NULL,
                ResolutionDepth SMALLINT NOT NULL,
                FlashMajor SMALLINT NOT NULL,
                FlashMinor SMALLINT NOT NULL,
                FlashMinor2 STRING NOT NULL,
                NetMajor SMALLINT NOT NULL,
                NetMinor SMALLINT NOT NULL,
                UserAgentMajor SMALLINT NOT NULL,
                UserAgentMinor VARCHAR(255) NOT NULL,
                CookieEnable SMALLINT NOT NULL,
                JavascriptEnable SMALLINT NOT NULL,
                IsMobile SMALLINT NOT NULL,
                MobilePhone SMALLINT NOT NULL,
                MobilePhoneModel STRING NOT NULL,
                Params STRING NOT NULL,
                IPNetworkID INT NOT NULL,
                TraficSourceID SMALLINT NOT NULL,
                SearchEngineID SMALLINT NOT NULL,
                SearchPhrase STRING NOT NULL,
                AdvEngineID SMALLINT NOT NULL,
                IsArtifical SMALLINT NOT NULL,
                WindowClientWidth SMALLINT NOT NULL,
                WindowClientHeight SMALLINT NOT NULL,
                ClientTimeZone SMALLINT NOT NULL,
                ClientEventTime DateTimeV2 NOT NULL,
                SilverlightVersion1 SMALLINT NOT NULL,
                SilverlightVersion2 SMALLINT NOT NULL,
                SilverlightVersion3 INT NOT NULL,
                SilverlightVersion4 SMALLINT NOT NULL,
                PageCharset STRING NOT NULL,
                CodeVersion INT NOT NULL,
                IsLink SMALLINT NOT NULL,
                IsDownload SMALLINT NOT NULL,
                IsNotBounce SMALLINT NOT NULL,
                FUniqID BIGINT NOT NULL,
                OriginalURL STRING NOT NULL,
                HID INT NOT NULL,
                IsOldCounter SMALLINT NOT NULL,
                IsEvent SMALLINT NOT NULL,
                IsParameter SMALLINT NOT NULL,
                DontCountHits SMALLINT NOT NULL,
                WithHash SMALLINT NOT NULL,
                HitColor CHAR NOT NULL,
                LocalEventTime DateTimeV2 NOT NULL,
                Age SMALLINT NOT NULL,
                Sex SMALLINT NOT NULL,
                Income SMALLINT NOT NULL,
                Interests SMALLINT NOT NULL,
                Robotness SMALLINT NOT NULL,
                RemoteIP INT NOT NULL,
                WindowName INT NOT NULL,
                OpenerName INT NOT NULL,
                HistoryLength SMALLINT NOT NULL,
                BrowserLanguage STRING NOT NULL,
                BrowserCountry STRING NOT NULL,
                SocialNetwork STRING NOT NULL,
                SocialAction STRING NOT NULL,
                HTTPError SMALLINT NOT NULL,
                SendTiming INT NOT NULL,
                DNSTiming INT NOT NULL,
                ConnectTiming INT NOT NULL,
                ResponseStartTiming INT NOT NULL,
                ResponseEndTiming INT NOT NULL,
                FetchTiming INT NOT NULL,
                SocialSourceNetworkID SMALLINT NOT NULL,
                SocialSourcePage STRING NOT NULL,
                ParamPrice BIGINT NOT NULL,
                ParamOrderID STRING NOT NULL,
                ParamCurrency STRING NOT NULL,
                ParamCurrencyID SMALLINT NOT NULL,
                OpenstatServiceName STRING NOT NULL,
                OpenstatCampaignID STRING NOT NULL,
                OpenstatAdID STRING NOT NULL,
                OpenstatSourceID STRING NOT NULL,
                UTMSource STRING NOT NULL,
                UTMMedium STRING NOT NULL,
                UTMCampaign STRING NOT NULL,
                UTMContent STRING NOT NULL,
                UTMTerm STRING NOT NULL,
                FromTag STRING NOT NULL,
                HasGCLID SMALLINT NOT NULL,
                RefererHash BIGINT NOT NULL,
                URLHash BIGINT NOT NULL,
                CLID INT NOT NULL
            )  
            DUPLICATE KEY (CounterID, EventDate, UserID, EventTime, WatchID) 
            DISTRIBUTED BY HASH(UserID) BUCKETS 16
            PROPERTIES ( \"replication_num\"=\"1\");
        "
        echo "####load data"
        if [[ ! -f "${data_home}"/hits.tsv ]] || [[ $(wc -c "${data_home}"/hits.tsv | awk '{print $1}') != '74807831229' ]]; then
            cd "${data_home}"
            wget --continue 'https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz'
            gzip -d hits.tsv.gz
            if ${DEBUG:-false}; then head -n 10000 hits.tsv >hits.tsv.10000; fi
            cd -
        fi
        data_file_name="${data_home}/hits.tsv"
        if ${DEBUG:-false}; then data_file_name="${data_home}/hits.tsv.10000"; fi
        echo "start loading ..."
        START=$(date +%s)
        curl --location-trusted \
            -u root: \
            -T "${data_file_name}" \
            -H "label:hits_${START}" \
            -H "columns: WatchID,JavaEnable,Title,GoodEvent,EventTime,EventDate,CounterID,ClientIP,RegionID,UserID,CounterClass,OS,UserAgent,URL,Referer,IsRefresh,RefererCategoryID,RefererRegionID,URLCategoryID,URLRegionID,ResolutionWidth,ResolutionHeight,ResolutionDepth,FlashMajor,FlashMinor,FlashMinor2,NetMajor,NetMinor,UserAgentMajor,UserAgentMinor,CookieEnable,JavascriptEnable,IsMobile,MobilePhone,MobilePhoneModel,Params,IPNetworkID,TraficSourceID,SearchEngineID,SearchPhrase,AdvEngineID,IsArtifical,WindowClientWidth,WindowClientHeight,ClientTimeZone,ClientEventTime,SilverlightVersion1,SilverlightVersion2,SilverlightVersion3,SilverlightVersion4,PageCharset,CodeVersion,IsLink,IsDownload,IsNotBounce,FUniqID,OriginalURL,HID,IsOldCounter,IsEvent,IsParameter,DontCountHits,WithHash,HitColor,LocalEventTime,Age,Sex,Income,Interests,Robotness,RemoteIP,WindowName,OpenerName,HistoryLength,BrowserLanguage,BrowserCountry,SocialNetwork,SocialAction,HTTPError,SendTiming,DNSTiming,ConnectTiming,ResponseStartTiming,ResponseEndTiming,FetchTiming,SocialSourceNetworkID,SocialSourcePage,ParamPrice,ParamOrderID,ParamCurrency,ParamCurrencyID,OpenstatServiceName,OpenstatCampaignID,OpenstatAdID,OpenstatSourceID,UTMSource,UTMMedium,UTMCampaign,UTMContent,UTMTerm,FromTag,HasGCLID,RefererHash,URLHash,CLID" \
            "http://localhost:8030/api/${db_name}/hits/_stream_load"
        END=$(date +%s)
        LOADTIME=$(echo "${END} - ${START}" | bc)
        echo "INFO: ClickBench Load data costs ${LOADTIME} seconds"
        echo "${LOADTIME}" >clickbench_loadtime

        if ! check_clickbench_table_rows "${db_name}"; then
            exit 1
        fi
        data_reload="true"
    fi

    echo "#### 5. run clickbench query"
    sed -i '/^run_sql \"analyze table hits with sync;\"/d' "${teamcity_build_checkoutDir}"/tools/clickbench-tools/run-clickbench-queries.sh
    bash "${teamcity_build_checkoutDir}"/tools/clickbench-tools/run-clickbench-queries.sh
    cold_run_time_threshold=${cold_run_time_threshold_master:-120} # 单位 秒
    hot_run_time_threshold=${hot_run_time_threshold_master:-34}    # 单位 秒
    if [[ "${target_branch}" == "branch-2.0" ]]; then
        cold_run_time_threshold=${cold_run_time_threshold_branch20:-110} # 单位 秒
        hot_run_time_threshold=${hot_run_time_threshold_branch20:-34}    # 单位 秒
    fi
    echo "INFO: cold_run_time_threshold is ${cold_run_time_threshold}, hot_run_time_threshold is ${hot_run_time_threshold}"
    # result.csv 来自 run-clickbench-queries.sh 的产出
    if ! check_clickbench_performance_result result.csv; then exit 1; fi
    if ! (cd clickbench && bash check-query-result.sh && cd -); then exit 1; fi
    cold_run_sum=$(awk -F ',' '{sum+=$2} END {print sum}' result.csv)
    best_hot_run_sum=$(awk -F ',' '{if($3<$4){sum+=$3}else{sum+=$4}} END {print sum}' result.csv)
    comment_body_summary="Total hot run time: ${best_hot_run_sum} s"
    comment_body_detail="ClickBench test result on commit ${commit_id_from_trigger:-}, data reload: ${data_reload:-"false"}

$(sed 's|,|\t|g' result.csv)
Total cold run time: ${cold_run_sum} s
Total hot run time: ${best_hot_run_sum} s"

    echo "#### 6. comment result on clickbench"
    comment_body_detail=$(echo "${comment_body_detail}" | sed -e ':a;N;$!ba;s/\t/\\t/g;s/\n/\\n/g') # 将所有的 Tab字符替换为\t 换行符替换为\n
    create_an_issue_comment_clickbench "${pr_num_from_trigger:-}" "${comment_body_summary}" "${comment_body_detail}"
    rm -f result.csv
    echo -e "INFO: Restore session variables \n$(cat "${backup_session_variables_file}")"
    mysql -h"${host}" -P"${query_port}" -uroot -e "source ${backup_session_variables_file};"
    rm -f "${backup_session_variables_file}"
)
exit_flag="$?"

echo "#### 7. check if need backup doris logs"
if [[ ${exit_flag} != "0" ]]; then
    stop_doris
    print_doris_fe_log
    print_doris_be_log
    if file_name=$(archive_doris_logs "${pr_num_from_trigger}_${commit_id_from_trigger}_$(date +%Y%m%d%H%M%S)_doris_logs.tar.gz"); then
        upload_doris_log_to_oss "${file_name}"
    fi
fi

exit "${exit_flag}"
