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

if [[ -f "${teamcity_build_checkoutDir:-}"/regression-test/pipeline/performance/run-tpch.sh ]]; then
    cd "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/
    bash -x run-tpch.sh
else
    echo "Build Step file missing: regression-test/pipeline/performance/run-tpch.sh" && exit 1
fi
EOF

#####################################################################################
## run-tpch.sh content ##

# shellcheck source=/dev/null
# check_tpch_table_rows, restart_doris, set_session_variable, check_tpch_result
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
    SF="1"
fi
echo "#### Check env"
if [[ -z "${teamcity_build_checkoutDir}" ]]; then echo "ERROR: env teamcity_build_checkoutDir not set" && exit 1; fi
if [[ -z "${pr_num_from_trigger}" ]]; then echo "ERROR: env pr_num_from_trigger not set" && exit 1; fi
if [[ -z "${commit_id_from_trigger}" ]]; then echo "ERROR: env commit_id_from_trigger not set" && exit 1; fi
if [[ -z "${target_branch}" ]]; then echo "ERROR: env target_branch not set" && exit 1; fi

# shellcheck source=/dev/null
source "$(bash "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/get-or-set-tmp-env.sh 'get')"
if ${skip_pipeline:=false}; then echo "INFO: skip build pipline" && exit 0; else echo "INFO: no skip"; fi

echo "#### Run tpch test on Doris ####"
DORIS_HOME="${teamcity_build_checkoutDir}/output"
export DORIS_HOME
exit_flag=0

(
    set -e
    shopt -s inherit_errexit

    echo "#### 1. Restart doris"
    if ! restart_doris; then echo "ERROR: Restart doris failed" && exit 1; fi

    echo "#### 2. check if need to load data"
    SF=${SF:-"100"}                                                                   # SCALE FACTOR
    TPCH_DATA_DIR="/data/tpch/sf_${SF}"                                               # no / at the end
    TPCH_DATA_DIR_LINK="${teamcity_build_checkoutDir}"/tools/tpch-tools/bin/tpch-data # no / at the end
    db_name="tpch_sf${SF}"
    sed -i "s|^export DB=.*$|export DB='${db_name}'|g" \
        "${teamcity_build_checkoutDir}"/tools/tpch-tools/conf/doris-cluster.conf
    if ! check_tpch_table_rows "${db_name}" "${SF}"; then
        echo "INFO: need to load tpch-sf${SF} data"
        if ${force_load_data:-false}; then echo "INFO: force_load_data is true"; else echo "ERROR: force_load_data is false" && exit 1; fi
        # prepare data
        mkdir -p "${TPCH_DATA_DIR}"
        (
            cd "${TPCH_DATA_DIR}" || exit 1
            declare -A table_file_count
            table_file_count=(['region']=1 ['nation']=1 ['supplier']=1 ['customer']=1 ['part']=1 ['partsupp']=10 ['orders']=10 ['lineitem']=10)
            for table_name in ${!table_file_count[*]}; do
                if [[ ${table_file_count[${table_name}]} -eq 1 ]]; then
                    url="https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/tpch/sf${SF}/${table_name}.tbl"
                    if ! wget --continue -t3 -q "${url}"; then echo "ERROR: wget --continue ${url}" && exit 1; fi
                elif [[ ${table_file_count[${table_name}]} -eq 10 ]]; then
                    (
                        for i in {1..10}; do
                            url="https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/tpch/sf${SF}/${table_name}.tbl.${i}"
                            if ! wget --continue -t3 -q "${url}"; then echo "ERROR: wget --continue ${url}" && exit 1; fi
                        done
                    ) &
                    wait
                fi
            done
        )
        # create table and load data
        bash "${teamcity_build_checkoutDir}"/tools/tpch-tools/bin/create-tpch-tables.sh -s "${SF}"
        rm -rf "${TPCH_DATA_DIR_LINK}"
        ln -s "${TPCH_DATA_DIR}" "${TPCH_DATA_DIR_LINK}"
        bash "${teamcity_build_checkoutDir}"/tools/tpch-tools/bin/load-tpch-data.sh -c 2
        if ! check_tpch_table_rows "${db_name}" "${SF}"; then
            exit 1
        fi
        echo "INFO: sleep 10min to wait compaction done"
        if ${DEBUG:-false}; then sleep 10s; else sleep 10m; fi
        data_reload="true"
    fi

    echo "#### 3. run tpch-sf${SF} query"
    set_session_variable runtime_filter_mode global
    bash "${teamcity_build_checkoutDir}"/tools/tpch-tools/bin/run-tpch-queries.sh -s "${SF}" | tee "${teamcity_build_checkoutDir}"/run-tpch-queries.log
    echo
    cold_run_time_threshold=${cold_run_time_threshold_master:-120000} # ms
    hot_run_time_threshold=${hot_run_time_threshold_master:-42000}    # ms
    if [[ "${target_branch}" == "branch-2.0" ]]; then
        cold_run_time_threshold=${cold_run_time_threshold_branch20:-130000} # ms
        hot_run_time_threshold=${hot_run_time_threshold_branch20:-55000}    # ms
    fi
    echo "INFO: cold_run_time_threshold is ${cold_run_time_threshold}, hot_run_time_threshold is ${hot_run_time_threshold}"
    if ! check_tpch_result "${teamcity_build_checkoutDir}"/run-tpch-queries.log; then
        print_running_pipeline_tasks
        exit 1
    fi
    line_end=$(sed -n '/^Total hot run time/=' "${teamcity_build_checkoutDir}"/run-tpch-queries.log)
    line_begin=$((line_end - 23))
    comment_body_summary="$(sed -n "${line_end}p" "${teamcity_build_checkoutDir}"/run-tpch-queries.log)"
    comment_body_detail="Tpch sf${SF} test result on commit ${commit_id_from_trigger:-}, data reload: ${data_reload:-"false"}

------ Round 1 ----------------------------------
$(sed -n "${line_begin},${line_end}p" "${teamcity_build_checkoutDir}"/run-tpch-queries.log)"

    echo "#### 4. run tpch-sf${SF} query with runtime_filter_mode=off"
    set_session_variable runtime_filter_mode off
    bash "${teamcity_build_checkoutDir}"/tools/tpch-tools/bin/run-tpch-queries.sh | tee "${teamcity_build_checkoutDir}"/run-tpch-queries.log
    if ! grep '^Total hot run time' "${teamcity_build_checkoutDir}"/run-tpch-queries.log >/dev/null; then exit 1; fi
    line_end=$(sed -n '/^Total hot run time/=' "${teamcity_build_checkoutDir}"/run-tpch-queries.log)
    line_begin=$((line_end - 23))
    comment_body_detail="${comment_body_detail}

----- Round 2, with runtime_filter_mode=off -----
$(sed -n "${line_begin},${line_end}p" "${teamcity_build_checkoutDir}"/run-tpch-queries.log)"

    echo "#### 5. comment result on tpch"
    comment_body_summary="$(echo "${comment_body_summary}" | sed -e ':a;N;$!ba;s/\t/\\t/g;s/\n/\\n/g')" # 将所有的 Tab字符替换为\t 换行符替换为\n
    comment_body_detail="$(echo "${comment_body_detail}" | sed -e ':a;N;$!ba;s/\t/\\t/g;s/\n/\\n/g')"   # 将所有的 Tab字符替换为\t 换行符替换为\n
    create_an_issue_comment_tpch "${pr_num_from_trigger:-}" "${comment_body_summary}" "${comment_body_detail}"
    rm -f result.csv
)
exit_flag="$?"

echo "#### 5. check if need backup doris logs"
if [[ ${exit_flag} != "0" ]]; then
    stop_doris
    print_doris_fe_log
    print_doris_be_log
    if file_name=$(archive_doris_logs "${pr_num_from_trigger}_${commit_id_from_trigger}_$(date +%Y%m%d%H%M%S)_doris_logs.tar.gz"); then
        upload_doris_log_to_oss "${file_name}"
    fi
fi

exit "${exit_flag}"
