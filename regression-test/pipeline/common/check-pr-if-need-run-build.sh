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

# shellcheck source=/dev/null
# source ~/.bashrc
# set -ex

usage() {
    echo -e "Usage:
    bash $0 <PULL_NUMBER> <OPTIONS>
    note: https://github.com/apache/doris/pull/13259, PULL_NUMBER is 13259
    OPTIONS should be one of [be-ut|fe-ut|ckb|regression-p0|regression-p1|arm-regression-p0]
    " && return 1
}

_get_pr_changed_files() {
    usage_str="Usage:
    _get_pr_changed_files <PULL_NUMBER> [OPTIONS]
    note: https://github.com/apache/doris/pull/13259, PULL_NUMBER is 13259
    OPTIONS can be one of [all|added|modified|removed], default is all
    "
    if [[ -z "$1" ]]; then echo -e "${usage_str}" && return 1; fi
    if ! curl --version >/dev/null; then echo 'error: curl required...' && return 1; fi
    if ! command -v jq >/dev/null; then sudo yum install jq -y || sudo apt install -y jq; fi

    PULL_NUMBER="$1"
    which_file="$2"
    pr_url="https://github.com/${OWNER:=apache}/${REPO:=doris}/pull/${PULL_NUMBER}"
    try_times=10
    # The number of results per page (max 100), Default 30.
    per_page=100
    file_name='pr_change_files'
    while [[ ${try_times} -gt 0 ]]; do
        if curl \
            -H "Accept: application/vnd.github+json" \
            https://api.github.com/repos/"${OWNER}"/"${REPO}"/pulls/"${PULL_NUMBER}"/files?per_page="${per_page}" \
            2>/dev/null >"${file_name}"; then
            break
        else
            try_times=$((try_times - 1))
        fi
    done
    if [[ ${try_times} = 0 ]]; then echo -e "\033[31m List pull request(${pr_url}) files FAIL... \033[0m" && return 255; fi

    all_files=$(jq -r '.[] | .filename' "${file_name}")
    added_files=$(jq -r '.[] | select(.status == "added") | .filename' "${file_name}")
    modified_files=$(jq -r '.[] | select(.status == "modified") | .filename' "${file_name}")
    removed_files=$(jq -r '.[] | select(.status == "removed") | .filename' "${file_name}")
    rm "${file_name}"
    if [[ -z "${all_files}" ]]; then echo -e "\033[31m List pull request(${pr_url}) files FAIL... \033[0m" && return 255; fi

    echo -e "
https://github.com/apache/doris/pull/${PULL_NUMBER}/files all change files:
---------------------------------------------------------------"
    if [[ "${which_file:-all}" == "all" ]]; then
        echo -e "${all_files}\n" && export all_files
    elif [[ "${which_file}" == "added" ]]; then
        echo -e "${added_files}\n" && export added_files
    elif [[ "${which_file}" == "modified" ]]; then
        echo -e "${modified_files}\n" && export modified_files
    elif [[ "${which_file}" == "removed" ]]; then
        echo -e "${removed_files}\n" && export removed_files
    else
        return 1
    fi
}

_only_modified_regression_conf() {
    if [[ -n ${added_files} || -n ${removed_files} ]]; then echo "Not only modified regression conf, find added/removed files" && return 1; fi
    for f in ${modified_files}; do
        if [[ "${f}" == "regression-test/pipeline/p0/conf/regression-conf.groovy" ]] ||
            [[ "${f}" == "regression-test/pipeline/p1/conf/regression-conf.groovy" ]]; then
            continue
        else
            echo "Not only modified regression conf" && return 1
        fi
    done
    echo "only modified regression conf" && return 0
}

need_run_fe_ut() {
    if ! _get_pr_changed_files "$1"; then echo "get pr changed files failed, return need" && return 0; fi
    if _only_modified_regression_conf; then echo "return no need" && return 1; fi
    for af in ${all_files}; do
        if [[ "${af}" == 'fe'* ]] ||
            [[ "${af}" == 'fe_plugins'* ]] ||
            [[ "${af}" == 'bin/start_fe.sh' ]] ||
            [[ "${af}" == 'docs/zh-CN/docs/sql-manual/'* ]] ||
            [[ "${af}" == 'docs/en/docs/sql-manual/'* ]] ||
            [[ "${af}" == 'bin/stop_fe.sh' ]] ||
            [[ "${af}" == 'run-fe-ut.sh' ]]; then echo "fe-ut related file changed, return need" && return 0; fi
    done
    echo "return no need" && return 1
}

need_run_be_ut() {
    if ! _get_pr_changed_files "$1"; then echo "get pr changed files failed, return need" && return 0; fi
    if _only_modified_regression_conf; then echo "return no need" && return 1; fi
    for af in ${all_files}; do
        if [[ "${af}" == 'be'* ]] ||
            [[ "${af}" == 'contrib'* ]] ||
            [[ "${af}" == 'thirdparty'* ]] ||
            [[ "${af}" == 'bin/start_be.sh' ]] ||
            [[ "${af}" == 'bin/stop_be.sh' ]] ||
            [[ "${af}" == 'run-be-ut.sh' ]]; then
            echo "be-ut related file changed, return need" && return 0
        fi
    done
    echo "return no need" && return 1
}

need_run_regression_p0() {
    if ! _get_pr_changed_files "$1"; then echo "get pr changed files failed, return need" && return 0; fi
    if _only_modified_regression_conf; then echo "return no need" && return 1; fi
    for af in ${all_files}; do
        if [[ "${af}" == 'be'* ]] ||
            [[ "${af}" == 'bin'* ]] ||
            [[ "${af}" == 'conf'* ]] ||
            [[ "${af}" == 'contrib'* ]] ||
            [[ "${af}" == 'fe'* ]] ||
            [[ "${af}" == 'fe_plugins'* ]] ||
            [[ "${af}" == 'gensrc'* ]] ||
            [[ "${af}" == 'regression-test'* ]] ||
            [[ "${af}" == 'thirdparty'* ]] ||
            [[ "${af}" == 'docker'* ]] ||
            [[ "${af}" == 'ui'* ]] ||
            [[ "${af}" == 'webroot'* ]] ||
            [[ "${af}" == 'build.sh' ]] ||
            [[ "${af}" == 'env.sh' ]] ||
            [[ "${af}" == 'run-regression-test.sh' ]]; then
            echo "regression related file changed, return need" && return 0
        fi
    done
    echo "return no need" && return 1
}

need_run_regression_p1() {
    need_run_regression_p0 "$1"
}

need_run_arm_regression_p0() {
    if [[ $(($1 % 2)) -eq 0 ]]; then echo "the pull request id is even, return no need" && return 1; fi
    need_run_regression_p0 "$1"
}

need_run_ckb() {
    if ! _get_pr_changed_files "$1"; then echo "get pr changed files failed, return need" && return 0; fi
    if _only_modified_regression_conf; then echo "return no need" && return 1; fi
    for af in ${all_files}; do
        if [[ "${af}" == 'be'* ]] ||
            [[ "${af}" == 'bin'* ]] ||
            [[ "${af}" == 'conf'* ]] ||
            [[ "${af}" == 'fe'* ]] ||
            [[ "${af}" == 'gensrc'* ]] ||
            [[ "${af}" == 'thirdparty'* ]] ||
            [[ "${af}" == 'build.sh' ]] ||
            [[ "${af}" == 'env.sh' ]]; then
            echo "clickbench performance related file changed, return need" && return 0
        fi
    done
    echo "return no need" && return 1
}

if [[ -z "$1" ]]; then
    usage
elif [[ "$2" == "be-ut" ]]; then
    need_run_be_ut "$1"
elif [[ "$2" == "fe-ut" ]]; then
    need_run_fe_ut "$1"
elif [[ "$2" == "ckb" ]]; then
    need_run_ckb "$1"
elif [[ "$2" == "regression-p0" ]]; then
    need_run_regression_p0 "$1"
elif [[ "$2" == "regression-p1" ]]; then
    need_run_regression_p1 "$1"
elif [[ "$2" == "arm-regression-p0" ]]; then
    need_run_arm_regression_p0 "$1"
else
    usage
fi
