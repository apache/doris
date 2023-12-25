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

if [[ -f "${teamcity_build_checkoutDir:-}"/regression-test/pipeline/performance/deploy.sh ]]; then
    cd "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance
    bash -x deploy.sh
else
    echo "Build Step file missing: regression-test/pipeline/performance/deploy.sh" && exit 1
fi
EOF

## deploy.sh content ##

# shellcheck source=/dev/null
source "$(bash "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/get-or-set-tmp-env.sh 'get')"
if ${skip_pipeline:=false}; then echo "INFO: skip build pipline" && exit 0; else echo "INFO: no skip"; fi

# shellcheck source=/dev/null
# upload_doris_log_to_oss
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/oss-utils.sh
# shellcheck source=/dev/null
# start_doris_fe, get_doris_conf_value, start_doris_be, stop_doris,
# print_doris_fe_log, print_doris_be_log, archive_doris_logs
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/doris-utils.sh

if ${DEBUG:-false}; then
    pull_request_num="28431"
    commit_id="b052225cd0a180b4576319b5bd6331218dd0d3fe"
fi
echo "#### Check env"
if [[ -z "${teamcity_build_checkoutDir}" ||
    -z "${pull_request_num}" ||
    -z "${commit_id}" ]]; then
    echo "ERROR: env teamcity_build_checkoutDir or pull_request_num or commit_id not set"
    exit 1
fi

echo "#### Deploy Doris ####"
DORIS_HOME="${teamcity_build_checkoutDir}/output"
export DORIS_HOME
exit_flag=0
need_backup_doris_logs=false

echo "#### 1. try to kill old doris process"
stop_doris

echo "#### 2. copy conf from regression-test/pipeline/performance/conf/"
rm -f "${DORIS_HOME}"/fe/conf/fe_custom.conf "${DORIS_HOME}"/be/conf/be_custom.conf
if [[ -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/conf/fe_custom.conf &&
    -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/conf/be_custom.conf ]]; then
    cp -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/conf/fe_custom.conf "${DORIS_HOME}"/fe/conf/
    cp -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/conf/be_custom.conf "${DORIS_HOME}"/be/conf/
else
    echo "ERROR: doris conf file missing in ${teamcity_build_checkoutDir}/regression-test/pipeline/performance/conf/"
    exit 1
fi

echo "#### 3. start Doris"
meta_dir=$(get_doris_conf_value "${DORIS_HOME}"/fe/conf/fe_custom.conf meta_dir)
storage_root_path=$(get_doris_conf_value "${DORIS_HOME}"/be/conf/be_custom.conf storage_root_path)
mkdir -p "${meta_dir}"
mkdir -p "${storage_root_path}"
if ! start_doris_fe; then
    echo "ERROR: Start doris fe failed."
    print_doris_fe_log
    need_backup_doris_logs=true
    exit_flag=1
fi
if ! start_doris_be; then
    echo "ERROR: Start doris be failed."
    print_doris_be_log
    need_backup_doris_logs=true
    exit_flag=1
fi
if ! add_doris_be_to_fe; then
    need_backup_doris_logs=true
    exit_flag=1
fi
# wait 10s for doris totally started, otherwize may encounter the error below,
# ERROR 1105 (HY000) at line 102: errCode = 2, detailMessage = Failed to find enough backend, please check the replication num,replication tag and storage medium.
sleep 10s

echo "#### 4. set session variables"
echo "TODO"

echo "#### 5. check if need backup doris logs"
if ${need_backup_doris_logs}; then
    print_doris_fe_log
    print_doris_be_log
    if file_name=$(archive_doris_logs "${pull_request_num}_${commit_id}_doris_logs.tar.gz"); then
        upload_doris_log_to_oss "${file_name}"
    fi
fi

exit "${exit_flag}"
