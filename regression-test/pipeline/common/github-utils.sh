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

function create_an_issue_comment() {
    local ISSUE_NUMBER="$1"
    local COMMENT_BODY="$2"
    if [[ -z "${COMMENT_BODY}" ]]; then return 1; fi
    if [[ -z "${GITHUB_TOKEN}" ]]; then return 1; fi

    local OWNER='apache'
    local REPO='doris'
    COMMENT_BODY=$(echo "${COMMENT_BODY}" | sed -e ':a;N;$!ba;s/\t/\\t/g;s/\n/\\n/g') # 将所有的 Tab字符替换为\t 换行符替换为\n
    if ret=$(curl -s \
        -X POST \
        -H "Accept: application/vnd.github+json" \
        -H "Authorization: Bearer ${GITHUB_TOKEN:-}" \
        -H "X-GitHub-Api-Version: 2022-11-28" \
        https://api.github.com/repos/"${OWNER}"/"${REPO}"/issues/"${ISSUE_NUMBER}"/comments \
        -d "{\"body\": \"${COMMENT_BODY}\"}"); then
        if echo "${ret}" | grep "Problems parsing JSON"; then
            is_succ=false
        else
            is_succ=true
        fi
    else
        is_succ=false
    fi

    if ${is_succ}; then
        echo -e "\033[32m Create issue(${ISSUE_NUMBER}) comment SUCCESS... \033[0m" && return 0
    else
        echo -e "\033[31m Create issue(${ISSUE_NUMBER}) comment FAIL... \033[0m" && return 1
    fi
}

function create_an_issue_comment_tpch() {
    local ISSUE_NUMBER="$1"
    local COMMENT_BODY="$2"
    local machine='aliyun_ecs.c7a.8xlarge_32C64G'
    COMMENT_BODY="
TPC-H test result on machine: '${machine}', run with scripts in https://github.com/apache/doris/tree/master/tools/tpch-tools
\`\`\`
${COMMENT_BODY}
\`\`\`
"
    create_an_issue_comment "${ISSUE_NUMBER}" "${COMMENT_BODY}"
}

function create_an_issue_comment_tpcds() {
    local ISSUE_NUMBER="$1"
    local COMMENT_BODY="$2"
    local machine='aliyun_ecs.c7a.8xlarge_32C64G'
    COMMENT_BODY="
TPC-DS test result on machine: '${machine}', run with scripts in https://github.com/apache/doris/tree/master/tools/tpcds-tools
\`\`\`
${COMMENT_BODY}
\`\`\`
"
    create_an_issue_comment "${ISSUE_NUMBER}" "${COMMENT_BODY}"
}

function create_an_issue_comment_clickbench() {
    local ISSUE_NUMBER="$1"
    local COMMENT_BODY="$2"
    local machine='aliyun_ecs.c7a.8xlarge_32C64G'
    COMMENT_BODY="
ClickBench test result on machine: '${machine}', run with scripts in https://github.com/apache/doris/tree/master/tools/clickbench-tools
\`\`\`
${COMMENT_BODY}
\`\`\`
"
    create_an_issue_comment "${ISSUE_NUMBER}" "${COMMENT_BODY}"
}

_get_pr_changed_files_count() {
    PULL_NUMBER="${PULL_NUMBER:-$1}"
    if [[ -z "${PULL_NUMBER}" ]]; then
        echo "Usage: _get_pr_changed_files_count PULL_NUMBER"
        return 1
    fi

    OWNER="${OWNER:=apache}"
    REPO="${REPO:=doris}"
    try_times=10
    while [[ ${try_times} -gt 0 ]]; do
        set -x
        if ret=$(
            curl -s -H "Accept: application/vnd.github+json" \
                https://api.github.com/repos/"${OWNER}"/"${REPO}"/pulls/"${PULL_NUMBER}" | jq -e '.changed_files'
        ); then
            set +x
            echo "${ret}" && return
        fi
        sleep 1s
        try_times=$((try_times - 1))
    done
    set +x
    if [[ ${try_times} -eq 0 ]]; then echo "Failed to get pr(${PULL_NUMBER}) changed file count" && return 1; fi
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
    # The number of results per page (max 100), Default 30.
    per_page=100
    file_name='pr_changed_files'
    rm -f "${file_name}"
    page=1
    if ! changed_files_count="$(_get_pr_changed_files_count "${PULL_NUMBER}")"; then return 1; fi
    while [[ ${changed_files_count} -gt 0 ]]; do
        try_times=10
        while [[ ${try_times} -gt 0 ]]; do
            set -x
            if curl -s \
                -H "Accept: application/vnd.github+json" \
                https://api.github.com/repos/"${OWNER}"/"${REPO}"/pulls/"${PULL_NUMBER}"/files?page="${page}"\&per_page="${per_page}" \
                >>"${file_name}"; then
                set +x
                break
            else
                set +x
                try_times=$((try_times - 1))
            fi
        done
        page=$((page + 1))
        changed_files_count=$((changed_files_count - per_page))
    done
    if [[ ${try_times} -eq 0 ]]; then echo -e "\033[31m List pull request(${pr_url}) files FAIL... \033[0m" && return 1; fi

    all_files=$(jq -r '.[] | .filename' "${file_name}")
    added_files=$(jq -r '.[] | select(.status == "added") | .filename' "${file_name}")
    modified_files=$(jq -r '.[] | select(.status == "modified") | .filename' "${file_name}")
    removed_files=$(jq -r '.[] | select(.status == "removed") | .filename' "${file_name}")
    rm "${file_name}"
    if [[ -z "${all_files}" ]]; then echo -e "\033[31m List pull request(${pr_url}) files FAIL... \033[0m" && return 1; fi
    echo "${all_files}" >all_files
    echo "${added_files}" >added_files
    echo "${modified_files}" >modified_files
    echo "${removed_files}" >removed_files

    echo -e "
https://github.com/apache/doris/pull/${PULL_NUMBER}/files all change files:
---------------------------------------------------------------"
    if [[ "${which_file:-all}" == "all" ]]; then
        echo -e "${all_files}\n"
    elif [[ "${which_file}" == "added" ]]; then
        echo -e "${added_files}\n"
    elif [[ "${which_file}" == "modified" ]]; then
        echo -e "${modified_files}\n"
    elif [[ "${which_file}" == "removed" ]]; then
        echo -e "${removed_files}\n"
    else
        return 1
    fi
}

_only_modified_regression_conf() {
    local added_files
    local removed_files
    local modified_files
    added_files=$(cat added_files)
    removed_files=$(cat removed_files)
    modified_files=$(cat modified_files)
    if [[ -n ${added_files} || -n ${removed_files} ]]; then
        # echo "Not only modified regression conf, find added/removed files"
        return 1
    fi
    if [[ -z ${modified_files} ]]; then
        # echo "modified_files is empty, return false"
        return 1
    fi
    for f in ${modified_files}; do
        if [[ "${f}" == "regression-test/pipeline/p0/conf/regression-conf.groovy" ]] ||
            [[ "${f}" == "regression-test/pipeline/p1/conf/regression-conf.groovy" ]]; then
            continue
        else
            # echo "Not only modified regression conf"
            return 1
        fi
    done
    # echo "only modified regression conf"
    return 0
}

file_changed_fe_ut() {
    local all_files
    all_files=$(cat all_files)
    if _only_modified_regression_conf; then echo "return no need" && return 1; fi
    if [[ -z ${all_files} ]]; then echo "return need" && return 0; fi
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

file_changed_be_ut() {
    local all_files
    all_files=$(cat all_files)
    if _only_modified_regression_conf; then echo "return no need" && return 1; fi
    if [[ -z ${all_files} ]]; then echo "return need" && return 0; fi
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

file_changed_regression_p0() {
    local all_files
    all_files=$(cat all_files)
    if _only_modified_regression_conf; then echo "return no need" && return 1; fi
    if [[ -z ${all_files} ]]; then echo "return need" && return 0; fi
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

file_changed_regression_p1() {
    file_changed_regression_p0
}

file_changed_ckb() {
    local all_files
    all_files=$(cat all_files)
    if _only_modified_regression_conf; then echo "return no need" && return 1; fi
    if [[ -z ${all_files} ]]; then echo "return need" && return 0; fi
    for af in ${all_files}; do
        if [[ "${af}" == 'be'* ]] ||
            [[ "${af}" == 'bin'* ]] ||
            [[ "${af}" == 'conf'* ]] ||
            [[ "${af}" == 'fe'* ]] ||
            [[ "${af}" == 'gensrc'* ]] ||
            [[ "${af}" == 'thirdparty'* ]] ||
            [[ "${af}" == 'build.sh' ]] ||
            [[ "${af}" == 'env.sh' ]] ||
            [[ "${af}" == 'regression-test/pipeline/common/github-utils.sh' ]] ||
            [[ "${af}" == 'regression-test/pipeline/common/doris-utils.sh' ]] ||
            [[ "${af}" == 'regression-test/pipeline/common/oss-utils.sh' ]] ||
            [[ "${af}" == 'tools/tpch-tools/bin/run-tpch-queries.sh' ]] ||
            [[ "${af}" == 'regression-test/pipeline/tpch/tpch-sf100/'* ]]; then
            echo "clickbench performance related file changed, return need" && return 0
        fi
    done
    echo "return no need" && return 1
}

file_changed_performance() {
    local all_files
    all_files=$(cat all_files)
    if _only_modified_regression_conf; then echo "return no need" && return 1; fi
    if [[ -z ${all_files} ]]; then echo "return need" && return 0; fi
    for af in ${all_files}; do
        if [[ "${af}" == 'be'* ]] ||
            [[ "${af}" == 'bin'* ]] ||
            [[ "${af}" == 'conf'* ]] ||
            [[ "${af}" == 'fe'* ]] ||
            [[ "${af}" == 'gensrc'* ]] ||
            [[ "${af}" == 'thirdparty'* ]] ||
            [[ "${af}" == 'build.sh' ]] ||
            [[ "${af}" == 'env.sh' ]] ||
            [[ "${af}" == 'regression-test/pipeline/common/github-utils.sh' ]] ||
            [[ "${af}" == 'regression-test/pipeline/common/doris-utils.sh' ]] ||
            [[ "${af}" == 'regression-test/pipeline/common/oss-utils.sh' ]] ||
            [[ "${af}" == 'regression-test/pipeline/performance/'* ]] ||
            [[ "${af}" == 'tools/tpch-tools/bin/run-tpch-queries.sh' ]] ||
            [[ "${af}" == 'tools/tpcds-tools/bin/run-tpcds-queries.sh' ]] ||
            [[ "${af}" == 'regression-test/pipeline/tpch/tpch-sf100/'* ]]; then
            echo "performance related file changed, return need" && return 0
        fi
    done
    echo "return no need" && return 1
}
