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

#!/bin/bash

# github中评论的要触发的流水线名字
# 到
# teamcity流水线实际的名称
# 的映射
# 新加流水线需要修改这里
declare -A comment_to_pipeline
comment_to_pipeline=(
    ['feut']='Doris_Doris_FeUt'
    ['beut']='Doris_DorisBeUt_BeUt'
    ['compile']='Doris_DorisCompile_Compile'
    ['p0']='Doris_DorisRegression_P0Regression'
    ['p1']='Doris_DorisRegression_P1Regression'
    ['external']='Doris_External_Regression'
    ['clickbench']='Doris_Performance_Clickbench_ClickbenchNew'
    ['pipelinex_p0']='Doris_DorisRegression_P0RegressionPipelineX'
    ['arm']='Doris_ArmPipeline_P0Regression'
    ['tpch']='Tpch_TpchSf100'
    ['performance']='Doris_PerformanceNew_Performance'
)

# github中评论的要触发的流水线名字
# 到
# teamcity流水线返回结果给github的名称
# 的映射
# 新加流水线需要修改这里
declare -A conment_to_context
conment_to_context=(
    ['compile']='COMPILE (DORIS_COMPILE)'
    ['feut']='FE UT (Doris FE UT)'
    ['beut']='BE UT (Doris BE UT)'
    ['p0']='P0 Regression (Doris Regression)'
    ['p1']='P1 Regression (Doris Regression)'
    ['external']='External Regression (Doris External Regression)'
    ['pipelinex_p0']='P0 Regression PipelineX (Doris Regression)'
    ['clickbench']='clickbench-new (clickbench)'
    ['arm']='P0 Regression (ARM pipeline)'
    ['tpch']='tpch-sf100 (tpch)'
    ['performance']='performance (Performance New)'
)

get_commit_id_of_build() {
    # 获取某个build的commit id
    if [[ -z "$1" ]]; then return 1; fi
    local build_id="$1"
    local commit_id
    local ret
    set -x
    if ret=$(
        curl -s -X GET \
            -u OneMoreChance:OneMoreChance \
            -H "Content-Type:text/plain" \
            -H "Accept: application/json" \
            "http://43.132.222.7:8111/app/rest/builds/${build_id}"
    ); then
        set +x
        commit_id=$(echo "${ret}" | jq -r '.revisions.revision[0].version')
        echo "${commit_id}"
    else
        set +x
        return 1
    fi
}

get_running_build_of_pr() {
    # "获取pr在某条流水线上正在跑的build"
    local PULL_REQUEST_NUM="${PULL_REQUEST_NUM:-$1}"
    local COMMENT_TRIGGER_TYPE="${COMMENT_TRIGGER_TYPE:-$2}"
    if [[ -z "${PULL_REQUEST_NUM}" || -z "${COMMENT_TRIGGER_TYPE}" ]]; then
        echo "Usage: get_queue_build_of_pr PULL_REQUEST_NUM COMMENT_TRIGGER_TYPE" && return 1
    fi

    local PIPELINE="${comment_to_pipeline[${COMMENT_TRIGGER_TYPE}]}"
    local running_builds_list
    local ret
    set -x
    if ret=$(
        curl -s -X GET \
            -u OneMoreChance:OneMoreChance \
            -H "Content-Type:text/plain" \
            -H "Accept: application/json" \
            "http://43.132.222.7:8111/app/rest/builds?locator=buildType:${PIPELINE},branch:pull/${PULL_REQUEST_NUM},running:true"
    ); then
        set +x
        running_builds_list=$(echo "${ret}" | jq -r '.build[].id')
        echo "${running_builds_list}"
    else
        set +x
        return 1
    fi
}
# get_running_build_of_pr "$1" "$2"

get_queue_build_of_pr() {
    # "获取pr在某条流水线上排队的build"
    local PULL_REQUEST_NUM="${PULL_REQUEST_NUM:-$1}"
    local COMMENT_TRIGGER_TYPE="${COMMENT_TRIGGER_TYPE:-$2}"
    if [[ -z "${PULL_REQUEST_NUM}" || -z "${COMMENT_TRIGGER_TYPE}" ]]; then
        echo "Usage: get_queue_build_of_pr PULL_REQUEST_NUM COMMENT_TRIGGER_TYPE" && return 1
    fi

    local PIPELINE="${comment_to_pipeline[${COMMENT_TRIGGER_TYPE}]}"
    local queue_builds_list
    local ret
    set -x
    if ret=$(
        curl -s -X GET \
            -u OneMoreChance:OneMoreChance \
            -H "Content-Type:text/plain" \
            -H "Accept: application/json" \
            "http://43.132.222.7:8111/app/rest/buildQueue?locator=buildType:${PIPELINE}"
    ); then
        set +x
        queue_builds_list=$(echo "${ret}" | jq ".build[] | select(.branchName == \"pull/${PULL_REQUEST_NUM}\") | .id")
        echo "${queue_builds_list}"
    else
        set +x
        echo "WARNING: failed to get queue build for PR ${PULL_REQUEST_NUM} of pipeline ${PIPELINE}" && return 1
    fi
}
# get_queue_build_of_pr "$1" "$2"

cancel_running_build() {
    local PULL_REQUEST_NUM="${PULL_REQUEST_NUM:-$1}"
    local COMMENT_TRIGGER_TYPE="${COMMENT_TRIGGER_TYPE:-$2}"
    if [[ -z "${PULL_REQUEST_NUM}" || -z "${COMMENT_TRIGGER_TYPE}" ]]; then
        echo "Usage: get_queue_build_of_pr PULL_REQUEST_NUM COMMENT_TRIGGER_TYPE" && return 1
    fi

    local PIPELINE="${comment_to_pipeline[${COMMENT_TRIGGER_TYPE}]}"
    local build_ids
    if ! build_ids=$(get_running_build_of_pr "${PULL_REQUEST_NUM}" "${COMMENT_TRIGGER_TYPE}"); then return 1; fi
    for id in ${build_ids}; do
        set -x
        if curl -s -X POST \
            -u OneMoreChance:OneMoreChance \
            -H "Content-Type:application/json" \
            -H "Accept: application/json" \
            "http://43.132.222.7:8111/app/rest/builds/id:${id}" \
            -d '{ "comment": "Canceling this running build before triggering a new one", "readdIntoQueue": false }'; then
            set +x
            echo -e "\nINFO: canceled running build(id ${id}) for PR ${PULL_REQUEST_NUM} of pipeline ${PIPELINE}"
        else
            set +x
            echo "WARNING: failed to cancel running build(id ${id}) for PR ${PULL_REQUEST_NUM} of pipeline ${PIPELINE}"
        fi
    done
}
# cancel_running_build "$1" "$2"

cancel_queue_build() {
    local PULL_REQUEST_NUM="${PULL_REQUEST_NUM:-$1}"
    local COMMENT_TRIGGER_TYPE="${COMMENT_TRIGGER_TYPE:-$2}"
    if [[ -z "${PULL_REQUEST_NUM}" || -z "${COMMENT_TRIGGER_TYPE}" ]]; then
        echo "Usage: get_queue_build_of_pr PULL_REQUEST_NUM COMMENT_TRIGGER_TYPE" && return 1
    fi

    local PIPELINE="${comment_to_pipeline[${COMMENT_TRIGGER_TYPE}]}"
    local build_ids
    if ! build_ids=$(get_queue_build_of_pr "${PULL_REQUEST_NUM}" "${COMMENT_TRIGGER_TYPE}"); then return 1; fi
    for id in ${build_ids}; do
        set -x
        if curl -s -X POST \
            -u OneMoreChance:OneMoreChance \
            -H "Content-Type:application/json" \
            -H "Accept: application/json" \
            "http://43.132.222.7:8111/app/rest/buildQueue/id:${id}" \
            -d '{ "comment": "Canceling this queued build before triggering a new one", "readdIntoQueue": false }'; then
            set +x
            echo -e "\nINFO: canceled queue build(id ${id}) for PR ${PULL_REQUEST_NUM} of pipeline ${PIPELINE}"
        else
            set +x
            echo "WARNING: failed to cancel queue build(id ${id}) for PR ${PULL_REQUEST_NUM} of pipeline ${PIPELINE}"
        fi
    done
}
# cancel_queue_build "$1" "$2"

skip_build() {
    # 对于不需要跑teamcity pipeline的PR，直接调用github的接口返回成功
    if [[ -z "${GITHUB_TOKEN}" ]]; then
        echo "ERROR: env GITHUB_TOKEN not set"
        return 1
    fi
    if [[ -z "$2" ]]; then
        echo "Usage: skip_teamcity_pipeline COMMIT_ID_FROM_TRIGGER COMMENT_TRIGGER_TYPE"
        return 1
    fi
    local COMMIT_ID_FROM_TRIGGER="$1"
    local COMMENT_TRIGGER_TYPE="$2"

    local state="${TC_BUILD_STATE:-success}" # 可选值 success failure pending
    local payload="{\"state\":\"${state}\",\"target_url\":\"\",\"description\":\"Skip teamCity build\",\"context\":\"${conment_to_context[${COMMENT_TRIGGER_TYPE}]}\"}"
    set -x
    if curl -L \
        -X POST \
        -H "Accept: application/vnd.github+json" \
        -H "Authorization: Bearer ${GITHUB_TOKEN:-}" \
        -H "X-GitHub-Api-Version: 2022-11-28" \
        "https://api.github.com/repos/apache/doris/statuses/${COMMIT_ID_FROM_TRIGGER:-}" \
        -d "${payload}"; then
        set +x
        echo "INFO: Skipped ${COMMIT_ID_FROM_TRIGGER} ${COMMENT_TRIGGER_TYPE}"
    else
        set +x
        return 1
    fi
}
# skip_build "$1" "$2"

trigger_build() {
    # 新触发一个build
    local PULL_REQUEST_NUM="${PULL_REQUEST_NUM:-$1}"
    local COMMIT_ID_FROM_TRIGGER="${COMMIT_ID_FROM_TRIGGER:-$2}"
    local COMMENT_TRIGGER_TYPE="${COMMENT_TRIGGER_TYPE:-$3}"
    local COMMENT_REPEAT_TIMES="${COMMENT_REPEAT_TIMES:-$4}"
    if [[ -z "${PULL_REQUEST_NUM}" || -z "${COMMIT_ID_FROM_TRIGGER}" || -z "${COMMENT_TRIGGER_TYPE}" ]]; then
        echo "Usage: add_build PULL_REQUEST_NUM COMMIT_ID_FROM_TRIGGER COMMENT_TRIGGER_TYPE [COMMENT_REPEAT_TIMES]"
        return 1
    fi
    local PIPELINE="${comment_to_pipeline[${COMMENT_TRIGGER_TYPE}]}"
    set -x
    if curl -s -X POST \
        -u OneMoreChance:OneMoreChance \
        -H "Content-Type:text/plain" \
        -H "Accept: application/json" \
        "http://43.132.222.7:8111/httpAuth/action.html?add2Queue=${PIPELINE}&branchName=pull/${PULL_REQUEST_NUM}&name=env.pr_num_from_trigger&value=${PULL_REQUEST_NUM:-}&name=env.commit_id_from_trigger&value=${COMMIT_ID_FROM_TRIGGER:-}&name=env.repeat_times_from_trigger&value=${COMMENT_REPEAT_TIMES:-1}"; then
        set +x
        echo "INFO: Add new build to PIPELINE ${PIPELINE} of PR ${PULL_REQUEST_NUM} with COMMENT_REPEAT_TIMES ${COMMENT_REPEAT_TIMES:-1}"
    else
        set +x
        return 1
    fi
}
# trigger_build "$1" "$2" "$3" "$4"

trigger_or_skip_build() {
    # 根据相关文件是否修改，来触发or跳过跑流水线
    local FILE_CHANGED="$1" # 默认为"true"
    local PULL_REQUEST_NUM="${PULL_REQUEST_NUM:-$2}"
    local COMMIT_ID_FROM_TRIGGER="${COMMIT_ID_FROM_TRIGGER:-$3}"
    local COMMENT_TRIGGER_TYPE="${COMMENT_TRIGGER_TYPE:-$4}"
    local COMMENT_REPEAT_TIMES="${COMMENT_REPEAT_TIMES:-$5}"
    if [[ -z "${PULL_REQUEST_NUM}" ||
        -z "${COMMIT_ID_FROM_TRIGGER}" ||
        -z "${COMMENT_TRIGGER_TYPE}" ]]; then
        echo "Usage: add_build FILE_CHANGED PULL_REQUEST_NUM COMMIT_ID_FROM_TRIGGER COMMENT_TRIGGER_TYPE [COMMENT_REPEAT_TIMES]"
        return 1
    fi

    if [[ "${FILE_CHANGED:-"true"}" == "true" ]]; then
        cancel_running_build "${PULL_REQUEST_NUM}" "${COMMENT_TRIGGER_TYPE}"
        cancel_queue_build "${PULL_REQUEST_NUM}" "${COMMENT_TRIGGER_TYPE}"
        trigger_build "${PULL_REQUEST_NUM}" "${COMMIT_ID_FROM_TRIGGER}" "${COMMENT_TRIGGER_TYPE}" "${COMMENT_REPEAT_TIMES}"
    else
        skip_build "${COMMIT_ID_FROM_TRIGGER}" "${COMMENT_TRIGGER_TYPE}"
        if [[ ${COMMENT_TRIGGER_TYPE} == "compile" ]]; then
            # skip compile 的时候，也把 p0 p1 external pipelinex_p0 都 skip 了
            skip_build "${COMMIT_ID_FROM_TRIGGER}" "p0"
            skip_build "${COMMIT_ID_FROM_TRIGGER}" "p1"
            skip_build "${COMMIT_ID_FROM_TRIGGER}" "external"
            skip_build "${COMMIT_ID_FROM_TRIGGER}" "pipelinex_p0"
        fi
    fi
}
# trigger_or_skip_build "$1" "$2" "$3" "$4" "$5"
