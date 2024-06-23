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

set -x
pwd
rm -rf ../.old/*
set +x

export teamcity_build_checkoutDir="%teamcity.build.checkoutDir%"
export commit_id_from_checkout="%build.vcs.number%"
export target_branch='%teamcity.pullRequest.target.branch%'
export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64/"
if [[ -f "${teamcity_build_checkoutDir:-}"/regression-test/pipeline/performance/prepare.sh ]]; then
    cd "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/
    bash prepare.sh
else
    echo "Build Step file missing: regression-test/pipeline/performance/prepare.sh" && exit 1
fi
EOF

#####################################################################################
## run.sh content ##

if ${DEBUG:-false}; then
    pr_num_from_trigger="28431"
    commit_id_from_trigger="5f5c4c80564c76ff4267fc4ce6a5408498ed1ab5"
    commit_id_from_checkout="5f5c4c80564c76ff4267fc4ce6a5408498ed1ab5" # teamcity checkout commit id
    target_branch="master"
fi
echo "#### Check env"
if [[ -z "${teamcity_build_checkoutDir}" ]]; then echo "ERROR: env teamcity_build_checkoutDir not set" && exit 1; fi
if [[ -z "${pr_num_from_trigger}" ]]; then echo "ERROR: env pr_num_from_trigger not set" && exit 1; fi
if [[ -z "${commit_id_from_trigger}" ]]; then echo "ERROR: env commit_id_from_trigger not set" && exit 1; fi
if [[ -z "${commit_id_from_checkout}" ]]; then echo "ERROR: env commit_id_from_checkout not set" && exit 1; fi
if [[ -z "${target_branch}" ]]; then echo "ERROR: env target_branch not set" && exit 1; fi

echo "#### 1. check if need run"
if [[ "${commit_id_from_trigger}" != "${commit_id_from_checkout}" ]]; then
    echo -e "从触发流水线 -> 流水线开始跑，这个时间段中如果有新commit，
这时候流水线 checkout 出来的 commit 就不是触发时的传过来的 commit 了，
这种情况不需要跑，预期 pr owner 会重新触发。"
    echo -e "ERROR: PR(${pr_num_from_trigger}),
    the commit_id_from_checkout
    ${commit_id_from_checkout}
    not equail to the commit_id_from_trigger
    ${commit_id_from_trigger}
    commit_id_from_trigger is outdate"
    exit 1
fi

# shellcheck source=/dev/null
source "$(bash "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/get-or-set-tmp-env.sh 'get')"
# shellcheck source=/dev/null
# install_java, clear_coredump
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/doris-utils.sh

if ${skip_pipeline:=false}; then echo "INFO: skip build pipline" && exit 0; else echo "INFO: no skip"; fi
if [[ "${target_branch}" == "master" ]]; then
    echo "INFO: PR target branch ${target_branch}"
    install_java
    JAVA_HOME="${JAVA_HOME:-$(find /usr/lib/jvm -maxdepth 1 -type d -name 'java-17-*' | sed -n '1p')}"
    bash "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/get-or-set-tmp-env.sh 'set' "export JAVA_HOME=\"${JAVA_HOME}\""
elif [[ "${target_branch}" == "branch-2.0" ]]; then
    echo "INFO: PR target branch ${target_branch}"
else
    echo "WARNING: PR target branch ${target_branch} is NOT in (master, branch-2.0), skip pipeline."
    bash "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/get-or-set-tmp-env.sh 'set' "export skip_pipeline=true"
    exit 0
fi
# shellcheck source=/dev/null
# _get_pr_changed_files file_changed_performance
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/github-utils.sh
if _get_pr_changed_files "${pr_num_from_trigger}"; then
    if ! file_changed_performance; then
        bash "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/get-or-set-tmp-env.sh 'set' "export skip_pipeline=true"
        exit 0
    fi
    if file_changed_meta; then
        # if PR changed the doris meta file, the next PR deployment on the same mechine which built this PR will fail.
        # make a copy of the meta file for the meta changed PR.
        target_branch="$(echo "${target_branch}" | sed 's| ||g;s|\.||g;s|-||g')" # remove space、dot、hyphen from branch name
        meta_changed_suffix="_2"
        rsync -a --delete "/data/doris-meta-${target_branch}/" "/data/doris-meta-${target_branch}${meta_changed_suffix}"
        rsync -a --delete "/data/doris-storage-${target_branch}/" "/data/doris-storage-${target_branch}${meta_changed_suffix}"
        bash "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/get-or-set-tmp-env.sh 'set' "export meta_changed_suffix=${meta_changed_suffix}"
    fi
fi

echo "#### 2. check if tpch depending files exist"
set -x
if ! [[ -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/oss-utils.sh &&
    -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/doris-utils.sh &&
    -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/github-utils.sh &&
    -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/get-or-set-tmp-env.sh &&
    -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/conf/be_custom.conf &&
    -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/conf/custom_env.sh &&
    -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/conf/fe_custom.conf &&
    -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/clickbench/conf/be_custom.conf &&
    -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/clickbench/conf/fe_custom.conf &&
    -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/clickbench/conf/opt_session_variables.sql &&
    -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/clickbench/check-query-result.sh &&
    -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/clickbench/queries-sort.sql &&
    -d "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/clickbench/query-result-target/ &&
    -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/prepare.sh &&
    -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/compile.sh &&
    -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/deploy.sh &&
    -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/run-tpch.sh &&
    -f "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/run-tpcds.sh &&
    -f "${teamcity_build_checkoutDir}"/tools/tpch-tools/bin/run-tpch-queries.sh &&
    -f "${teamcity_build_checkoutDir}"/tools/tpcds-tools/bin/run-tpcds-queries.sh ]]; then
    echo "ERROR: depending files missing" && exit 1
fi

echo "#### 3. try to kill old doris process"
# shellcheck source=/dev/null
# stop_doris
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/doris-utils.sh
if stop_doris; then echo; fi
clear_coredump
