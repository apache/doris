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
export teamcity_build_checkoutDir=${teamcity_build_checkoutDir:-'/home/work/unlimit_teamcity/TeamCity/Agents/20231214145742agent_172.16.0.165_1/work/ad600b267ee7ed84'}
if [[ -f "${teamcity_build_checkoutDir:-}"/regression-test/pipeline/performance/run-clickbench.sh ]]; then
    cd "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/
    bash -x run-clickbench.sh
else
    echo "Build Step file missing: regression-test/pipeline/performance/run-clickbench.sh" && exit 1
fi
EOF

## run.sh content ##

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
    teamcity_build_checkoutDir='/home/work/unlimit_teamcity/TeamCity/Agents/20231214145742agent_172.16.0.165_1/work/ad600b267ee7ed84'
    pull_request_num="28421"
    commit_id="5f5c4c80564c76ff4267fc4ce6a5408498ed1ab5"
fi
echo "#### Check env"
if [[ -z "${teamcity_build_checkoutDir}" ||
    -z "${pull_request_num}" ||
    -z "${commit_id}" ]]; then
    echo "ERROR: env teamcity_build_checkoutDir or pull_request_num or commit_id not set"
    exit 1
fi

echo "#### Run clickbench test on Doris ####"
DORIS_HOME="${teamcity_build_checkoutDir}/output"
export DORIS_HOME
cold_run_time_threshold=${cold_run_time_threshold:-666}
hot_run_time_threshold=${hot_run_time_threshold:-555}
exit_flag=0

(
    set -e
    shopt -s inherit_errexit

    echo "#### 1. Restart doris"
    if ! restart_doris; then echo "ERROR: Restart doris failed" && exit 1; fi

    echo "#### 2. check if need to load data"
    CLICKBENCH_DATA_DIR="/data/clickbench"                                               # no / at the end
    CLICKBENCH_DATA_DIR_LINK="${teamcity_build_checkoutDir}"/tools/clickbench-tools/data # no / at the end
    db_name="clickbench"
    if ! check_clickbench_table_rows "${db_name}"; then
        echo "INFO: need to load clickbench data"
        # prepare data
        mkdir -p "${CLICKBENCH_DATA_DIR}"

        # create table and load data
        bash "${teamcity_build_checkoutDir}"/tools/clickbench-tools/create-clickbench-table.sh
        rm -rf "${CLICKBENCH_DATA_DIR_LINK}"
        ln -s "${CLICKBENCH_DATA_DIR}" "${CLICKBENCH_DATA_DIR_LINK}"
        cd "${CLICKBENCH_DATA_DIR_LINK}"
        bash "${teamcity_build_checkoutDir}"/tools/clickbench-tools/load-clickbench-data.sh
        cd -
        if ! check_clickbench_table_rows "${db_name}"; then
            exit 1
        fi
        echo "INFO: sleep 10min to wait compaction done"
        if ${DEBUG}; then sleep 10s; else sleep 10m; fi
        data_reload="true"
    fi

    echo "#### 3. run clickbench query"
    set_session_variable runtime_filter_mode global
    bash "${teamcity_build_checkoutDir}"/tools/clickbench-tools/run-clickbench-queries.sh
    # result.csv 来自 run-clickbench-queries.sh 的产出
    if ! check_clickbench_performance_result result.csv; then exit 1; fi
    if ! check_clickbench_query_result; then exit 1; fi
    cold_run_sum=$(awk -F ',' '{sum+=$2} END {print sum}' result.csv)
    best_hot_run_sum=$(awk -F ',' '{if($3<$4){sum+=$3}else{sum+=$4}} END {print sum}' result.csv)
    comment_body="ClickBench test result on commit ${commit_id:-}, data reload: ${data_reload:-"false"}

run clickbench query with default conf and session variables
$(cat result.csv)
Total cold run time: ${cold_run_sum} s
Total hot run time: ${best_hot_run_sum} s"

    echo "#### 4. comment result on clickbench"
    comment_body=$(echo "${comment_body}" | sed -e ':a;N;$!ba;s/\t/\\t/g;s/\n/\\n/g') # 将所有的 Tab字符替换为\t 换行符替换为\n
    create_an_issue_comment_clickbench "${pull_request_num:-}" "${comment_body}"
)
exit_flag="$?"

echo "#### 5. check if need backup doris logs"
if [[ ${exit_flag} != "0" ]]; then
    print_doris_fe_log
    print_doris_be_log
    if file_name=$(archive_doris_logs "${pull_request_num}_${commit_id}_doris_logs.tar.gz"); then
        upload_doris_log_to_oss "${file_name}"
    fi
fi

exit "${exit_flag}"
