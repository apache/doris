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

teamcity_build_checkoutDir="%teamcity.build.checkoutDir%"
if [[ -f "${teamcity_build_checkoutDir:-}"/regression-test/pipeline/tpch/tpch-sf100/run.sh ]]; then
    cd "${teamcity_build_checkoutDir}"/regression-test/pipeline/tpch/tpch-sf100/
    bash -x run.sh
else
    echo "Build Step file missing: regression-test/pipeline/tpch/tpch-sf100/run.sh" && exit 1
fi
EOF

## run.sh content ##

# check_tpch_table_rows, stop_doris, set_session_variable
source ../../common/doris-utils.sh
# create_an_issue_comment
source ../../common/github-utils.sh
# upload_doris_log_to_oss
source ../../common/oss-utils.sh

echo "#### Check env"
if [[ -z "${teamcity_build_checkoutDir}" ||
    -z "${pull_request_id}" ||
    -z "${commit_id}" ]]; then
    echo "ERROR: env teamcity_build_checkoutDir or pull_request_id or commit_id not set"
    exit 1
fi

echo "#### Run tpch-sf100 test on Doris ####"
DORIS_HOME="${teamcity_build_checkoutDir}/output"
exit_flag=0

check_tpch_result() {
    log_file="$1"
    if ! grep '^Total cold run time' "${log_file}" || ! grep '^Total hot run time' "${log_file}"; then
        echo "ERROR: can not find 'Total hot run time' in '${log_file}'"
        return 1
    else
        cold_run_time=$(grep '^Total cold run time' "${log_file}" | awk '{print $5}')
        hot_run_time=$(grep '^Total hot run time' "${log_file}" | awk '{print $5}')
    fi
    # 单位是毫秒
    cold_run_time_threshold=${cold_run_time_threshold:-50000}
    hot_run_time_threshold=${hot_run_time_threshold:-42000}
    if [[ ${cold_run_time} -gt 50000 || ${hot_run_time} -gt 42000 ]]; then
        echo "ERROR:
    cold_run_time ${cold_run_time} is great than the threshold ${cold_run_time_threshold},
    or, hot_run_time ${hot_run_time} is great than the threshold ${hot_run_time_threshold}"
        return 1
    else
        echo "INFO:
    cold_run_time ${cold_run_time} is less than the threshold ${cold_run_time_threshold},
    or, hot_run_time ${hot_run_time} is less than the threshold ${hot_run_time_threshold}"
    fi
}

(
    set -e
    shopt -s inherit_errexit

    echo "#### 1. check if need to load data"
    SF="100" # SCALE FACTOR
    if ${DEBUG:-false}; then SF="1"; fi
    db_name="tpch_sf${SF}"
    sed -i "s|^export DB=.*$|export DB='${db_name}'|g" \
        "${teamcity_build_checkoutDir}"/tools/tpch-tools/conf/doris-cluster.conf
    if ! check_tpch_table_rows "${db_name}" "${SF}"; then
        echo "ERROR: check_tpch_table_rows failed." && exit 1
    fi

    echo "#### 2. run tpch-sf${SF} query"
    set_session_variable runtime_filter_mode global
    sed -i "s|^SCALE_FACTOR=[0-9]\+$|SCALE_FACTOR=${SF}|g" "${teamcity_build_checkoutDir}"/tools/tpch-tools/bin/run-tpch-queries.sh
    bash "${teamcity_build_checkoutDir}"/tools/tpch-tools/bin/run-tpch-queries.sh | tee "${teamcity_build_checkoutDir}"/run-tpch-queries.log
    if ! check_tpch_result "${teamcity_build_checkoutDir}"/run-tpch-queries.log; then exit 1; fi
    line_end=$(sed -n '/^Total hot run time/=' "${teamcity_build_checkoutDir}"/run-tpch-queries.log)
    line_begin=$((line_end - 23))
    comment_body="Tpch sf${SF} test result on commit ${commit_id:-}, data reload: ${data_reload:-"false"}

run tpch-sf${SF} query with default conf and session variables
$(sed -n "${line_begin},${line_end}p" "${teamcity_build_checkoutDir}"/run-tpch-queries.log)"

    echo "#### 3. run tpch-sf${SF} query with runtime_filter_mode=off"
    set_session_variable runtime_filter_mode off
    bash "${teamcity_build_checkoutDir}"/tools/tpch-tools/bin/run-tpch-queries.sh | tee "${teamcity_build_checkoutDir}"/run-tpch-queries.log
    if ! grep '^Total hot run time' "${teamcity_build_checkoutDir}"/run-tpch-queries.log >/dev/null; then exit 1; fi
    line_end=$(sed -n '/^Total hot run time/=' "${teamcity_build_checkoutDir}"/run-tpch-queries.log)
    line_begin=$((line_end - 23))
    comment_body="${comment_body}

run tpch-sf${SF} query with default conf and set session variable runtime_filter_mode=off
$(sed -n "${line_begin},${line_end}p" "${teamcity_build_checkoutDir}"/run-tpch-queries.log)"

    echo "#### 4. comment result on tpch"
    comment_body=$(echo "${comment_body}" | sed -e ':a;N;$!ba;s/\t/\\t/g;s/\n/\\n/g') # 将所有的 Tab字符替换为\t 换行符替换为\n
    create_an_issue_comment_tpch "${pull_request_id:-}" "${comment_body}"

    stop_doris
)
exit_flag="$?"

echo "#### 5. check if need backup doris logs"
if [[ ${exit_flag} != "0" ]]; then
    print_doris_fe_log
    print_doris_be_log
    if file_name=$(archive_doris_logs "${pull_request_id}_${commit_id}_doris_logs.tar.gz"); then
        upload_doris_log_to_oss "${file_name}"
    fi
fi

exit "${exit_flag}"
