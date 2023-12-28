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
export DEBUG=true

if [[ -f "${teamcity_build_checkoutDir:-}"/regression-test/pipeline/performance/run-tpcds.sh ]]; then
    cd "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/
    bash -x run-tpcds.sh
else
    echo "Build Step file missing: regression-test/pipeline/performance/run-tpcds.sh" && exit 1
fi
EOF

#####################################################################################
## run-tpcds.sh content ##

# shellcheck source=/dev/null
# check_tpcds_table_rows, restart_doris, set_session_variable, check_tpcds_result
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/doris-utils.sh
# shellcheck source=/dev/null
# create_an_issue_comment
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/github-utils.sh
# shellcheck source=/dev/null
# upload_doris_log_to_oss
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/oss-utils.sh

if ${DEBUG:-false}; then
    pull_request_num="28431"
    commit_id="5f5c4c80564c76ff4267fc4ce6a5408498ed1ab5"
    SF="1"
fi
echo "#### Check env"
if [[ -z "${teamcity_build_checkoutDir}" ||
    -z "${pull_request_num}" ||
    -z "${commit_id}" ]]; then
    echo "ERROR: env teamcity_build_checkoutDir or pull_request_num or commit_id not set"
    exit 1
fi

# shellcheck source=/dev/null
source "$(bash "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/get-or-set-tmp-env.sh 'get')"
if ${skip_pipeline:=false}; then echo "INFO: skip build pipline" && exit 0; else echo "INFO: no skip"; fi

echo "#### Run tpcds test on Doris ####"
DORIS_HOME="${teamcity_build_checkoutDir}/output"
export DORIS_HOME
cold_run_time_threshold=${cold_run_time_threshold:-600000} # ms
hot_run_time_threshold=${hot_run_time_threshold:-240000}   # ms
exit_flag=0

(
    set -e
    shopt -s inherit_errexit

    echo "#### 1. Restart doris"
    if ! restart_doris; then echo "ERROR: Restart doris failed" && exit 1; fi

    echo "#### 2. check if need to load data"
    SF=${SF:-"100"}                                                                      # SCALE FACTOR
    TPCDS_DATA_DIR="/data/tpcds/sf_${SF}"                                                # no / at the end
    TPCDS_DATA_DIR_LINK="${teamcity_build_checkoutDir}"/tools/tpcds-tools/bin/tpcds-data # no / at the end
    db_name="tpcds_sf${SF}"
    sed -i "s|^export DB=.*$|export DB='${db_name}'|g" \
        "${teamcity_build_checkoutDir}"/tools/tpcds-tools/conf/doris-cluster.conf
    if ! check_tpcds_table_rows "${db_name}" "${SF}"; then
        echo "INFO: need to load tpcds-sf${SF} data"
        if ${force_load_data:-false}; then echo "INFO: force_load_data is true"; else echo "ERROR: force_load_data is false" && exit 1; fi
        # prepare data
        mkdir -p "${TPCDS_DATA_DIR}"
        (
            cd "${TPCDS_DATA_DIR}" || exit 1
            declare -A table_file_count
            if [[ ${SF} == "1" ]]; then
                table_file_count=(['income_band']=1 ['ship_mode']=1 ['warehouse']=1 ['reason']=1 ['web_site']=1 ['call_center']=1 ['store']=1 ['promotion']=1 ['household_demographics']=1 ['web_page']=1 ['catalog_page']=1 ['time_dim']=1 ['date_dim']=1 ['item']=1 ['customer_demographics']=10 ['customer_address']=1 ['customer']=1 ['web_returns']=1 ['catalog_returns']=1 ['store_returns']=1 ['inventory']=10 ['web_sales']=1 ['catalog_sales']=1 ['store_sales']=1)
            elif [[ ${SF} == "100" ]]; then
                table_file_count=(['income_band']=1 ['ship_mode']=1 ['warehouse']=1 ['reason']=1 ['web_site']=1 ['call_center']=1 ['store']=1 ['promotion']=1 ['household_demographics']=1 ['web_page']=1 ['catalog_page']=1 ['time_dim']=1 ['date_dim']=1 ['item']=1 ['customer_demographics']=10 ['customer_address']=10 ['customer']=10 ['web_returns']=10 ['catalog_returns']=10 ['store_returns']=10 ['inventory']=10 ['web_sales']=10 ['catalog_sales']=10 ['store_sales']=10)
            fi
            for table_name in ${!table_file_count[*]}; do
                if [[ ${table_file_count[${table_name}]} -eq 1 ]]; then
                    url="https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/tpcds/sf${SF}/${table_name}_1_10.dat.gz"
                    if [[ -f ${table_name}_1_10.dat ]]; then continue; fi
                    if ! wget --continue -t3 -q "${url}"; then echo "ERROR: wget --continue ${url}" && exit 1; fi
                    if ! gzip -d "${table_name}_1_10.dat.gz"; then echo "ERROR: gzip -d ${table_name}_1_10.dat.gz" && exit 1; fi
                elif [[ ${table_file_count[${table_name}]} -eq 10 ]]; then
                    (
                        for i in {1..10}; do
                            url="https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/tpcds/sf${SF}/${table_name}_${i}_10.dat.gz"
                            if [[ -f ${table_name}_${i}_10.dat ]]; then continue; fi
                            if ! wget --continue -t3 -q "${url}"; then echo "ERROR: wget --continue ${url}" && exit 1; fi
                            if ! gzip -d "${table_name}_${i}_10.dat.gz"; then echo "ERROR: gzip -d ${table_name}_${i}_10.dat.gz" && exit 1; fi
                        done
                    ) &
                    wait
                fi
            done
        )
        # create table and load data
        bash "${teamcity_build_checkoutDir}"/tools/tpcds-tools/bin/create-tpcds-tables.sh -s "${SF}"
        rm -rf "${TPCDS_DATA_DIR_LINK}"
        ln -s "${TPCDS_DATA_DIR}" "${TPCDS_DATA_DIR_LINK}"
        bash "${teamcity_build_checkoutDir}"/tools/tpcds-tools/bin/load-tpcds-data.sh -c 10
        if ! check_tpcds_table_rows "${db_name}" "${SF}"; then
            exit 1
        fi
        echo "INFO: sleep 10min to wait compaction done"
        if ${DEBUG:-false}; then sleep 10s; else sleep 10m; fi
        data_reload="true"
    fi

    echo "#### 3. run tpcds-sf${SF} query"
    set_session_variable runtime_filter_mode global
    bash "${teamcity_build_checkoutDir}"/tools/tpcds-tools/bin/run-tpcds-queries.sh -s "${SF}" | tee "${teamcity_build_checkoutDir}"/run-tpcds-queries.log
    if ! check_tpcds_result "${teamcity_build_checkoutDir}"/run-tpcds-queries.log; then exit 1; fi
    line_end=$(sed -n '/^Total hot run time/=' "${teamcity_build_checkoutDir}"/run-tpcds-queries.log)
    line_begin=$((line_end - 100))
    comment_body="TPC-DS sf${SF} test result on commit ${commit_id:-}, data reload: ${data_reload:-"false"}

run tpcds-sf${SF} query with default conf and session variables
$(sed -n "${line_begin},${line_end}p" "${teamcity_build_checkoutDir}"/run-tpcds-queries.log)"

    echo "#### 4. comment result on tpcds"
    comment_body=$(echo "${comment_body}" | sed -e ':a;N;$!ba;s/\t/\\t/g;s/\n/\\n/g') # 将所有的 Tab字符替换为\t 换行符替换为\n
    create_an_issue_comment_tpcds "${pull_request_num:-}" "${comment_body}"
    rm -f result.csv
)
exit_flag="$?"

echo "#### 5. check if need backup doris logs"
if [[ ${exit_flag} != "0" ]]; then
    stop_doris
    print_doris_fe_log
    print_doris_be_log
    if file_name=$(archive_doris_logs "${pull_request_num}_${commit_id}_doris_logs.tar.gz"); then
        upload_doris_log_to_oss "${file_name}"
    fi
fi

exit "${exit_flag}"
