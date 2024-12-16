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

if [[ -f "${teamcity_build_checkoutDir:-}"/regression-test/pipeline/performance/deploy.sh ]]; then
    cd "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance
    bash -x deploy.sh
else
    echo "Build Step file missing: regression-test/pipeline/performance/deploy.sh" && exit 1
fi
EOF

#####################################################################################
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
    pr_num_from_trigger="28431"
    commit_id_from_trigger="b052225cd0a180b4576319b5bd6331218dd0d3fe"
    target_branch="master"
fi
echo "#### Check env"
if [[ -z "${teamcity_build_checkoutDir}" ]]; then echo "ERROR: env teamcity_build_checkoutDir not set" && exit 1; fi
if [[ -z "${pr_num_from_trigger}" ]]; then echo "ERROR: env pr_num_from_trigger not set" && exit 1; fi
if [[ -z "${commit_id_from_trigger}" ]]; then echo "ERROR: env commit_id_from_trigger not set" && exit 1; fi
if [[ -z "${target_branch}" ]]; then echo "ERROR: env target_branch not set" && exit 1; fi

echo "#### Deploy Doris ####"
DORIS_HOME="${teamcity_build_checkoutDir}/output"
export DORIS_HOME
exit_flag=0

(
    echo "#### 1. try to kill old doris process"
    stop_doris

    set -e
    echo "#### 2. copy conf from regression-test/pipeline/performance/conf/"
    cp -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/conf/fe_custom.conf "${DORIS_HOME}"/fe/conf/
    cp -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/conf/be_custom.conf "${DORIS_HOME}"/be/conf/
    target_branch="$(echo "${target_branch}" | sed 's| ||g;s|\.||g;s|-||g')" # remove space、dot、hyphen from branch name
    sed -i "s|^meta_dir=/data/doris-meta-\${branch_name}|meta_dir=/data/doris-meta-${target_branch}${meta_changed_suffix:-}|g" "${DORIS_HOME}"/fe/conf/fe_custom.conf
    sed -i "s|^storage_root_path=/data/doris-storage-\${branch_name}|storage_root_path=/data/doris-storage-${target_branch}${meta_changed_suffix:-}|g" "${DORIS_HOME}"/be/conf/be_custom.conf

    echo "#### 3. start Doris"
    meta_dir=$(get_doris_conf_value "${DORIS_HOME}"/fe/conf/fe_custom.conf meta_dir)
    storage_root_path=$(get_doris_conf_value "${DORIS_HOME}"/be/conf/be_custom.conf storage_root_path)
    mkdir -p "${meta_dir}"
    mkdir -p "${storage_root_path}"
    if ! start_doris_fe; then echo "ERROR: Start doris fe failed." && exit 1; fi
    if ! start_doris_be; then echo "ERROR: Start doris be failed." && exit 1; fi
    if ! add_doris_be_to_fe; then echo "ERROR: Add doris be failed." && exit 1; fi

    echo "#### 4. reset session variables"
    if ! reset_doris_session_variables; then exit 1; fi
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
