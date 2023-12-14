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
export OSS_accessKeyID='LTAI5tMJ8betwXWK7Cwo8tJ3'
export OSS_accessKeySecret='8yAa3kG9Wbpi7uu6uZo2UjLBmGoFFs'
export teamcity_build_checkoutDir=${teamcity_build_checkoutDir:-'/home/work/unlimit_teamcity/TeamCity/Agents/20231214145742agent_172.16.0.165_1/work/ad600b267ee7ed84'}
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
    echo "WARNING: Start doris fe failed at first time"
    print_doris_fe_log
    echo "WARNING: delete meta_dir and storage_root_path, then retry start doris fe"
    rm -rf "${meta_dir:?}/"*
    rm -rf "${storage_root_path:?}/"*
    if ! start_doris_fe; then
        need_backup_doris_logs=true
        exit_flag=1
    fi
fi
if ! start_doris_be; then
    echo "WARNING: Start doris be failed at first time"
    print_doris_be_log
    echo "WARNING: delete storage_root_path, then retry start doris be"
    rm -rf "${storage_root_path:?}/"*
    if ! start_doris_be; then
        need_backup_doris_logs=true
        exit_flag=1
    fi
fi
if ! add_doris_be_to_fe; then
    need_backup_doris_logs=true
    exit_flag=1
else
    # wait 10s for doris totally started, otherwize may encounter the error below,
    # ERROR 1105 (HY000) at line 102: errCode = 2, detailMessage = Failed to find enough backend, please check the replication num,replication tag and storage medium.
    sleep 10s
fi

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
