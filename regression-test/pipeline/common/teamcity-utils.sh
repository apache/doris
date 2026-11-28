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

# Comment keywords match the suffix of the published TeamCity/<keyword> check.
# External and Performance are optional and therefore absent from required checks.
declare -A conment_to_context
for lane in compile feut beut cloudut p0 cloud_p0 vault_p0 external nonconcurrent performance; do
    conment_to_context[$lane]="TeamCity/$lane"
done
unset lane

teamcity_current_pr() {
    set +x
    curl --fail --silent --show-error --max-time 45 \
        -H "Authorization: Bearer ${GITHUB_TOKEN:?}" \
        "https://api.github.com/repos/apache/doris/pulls/${PULL_REQUEST_NUM}" |
        jq -e --arg head "$COMMIT_ID_FROM_TRIGGER" --arg branch "$PULL_REQUEST_TARGET_BRANCH" \
            '.state=="open" and .head.sha==$head and .base.ref==$branch' >/dev/null
}

# Run only in the OIDC-enabled job after GitHub records the lane approvals.
teamcity_fc_submit() (
    set -euo pipefail
    set +x
    local lane=$1 pr=$2 head=$3 branch=$4 repeat=$5 token body receipt
    local endpoint=${TEAMCITY_FC_TRIGGER_URL:?Set the repository variable TEAMCITY_FC_TRIGGER_URL}
    local expected_endpoint='https://doris-c-trigger-mtjzjrppvu.cn-hongkong.fcapp.run/trigger'
    if [ "$endpoint" != "$expected_endpoint" ]; then
        echo "TEAMCITY_FC_TRIGGER_URL must be $expected_endpoint" >&2
        return 1
    fi
    token=$(printf 'Authorization: Bearer %s\n' "${ACTIONS_ID_TOKEN_REQUEST_TOKEN:?}" |
        curl -q --proto '=https' --fail --silent --show-error --max-time 20 --header @- \
            --get --data-urlencode 'audience=urn:apache:doris:ci:teamcity-trigger' "${ACTIONS_ID_TOKEN_REQUEST_URL:?}" |
        jq -er '.value | select(type == "string" and length > 0)') || return 1
    echo "::add-mask::$token"
    body=$(jq -cn --arg lane "$lane" --argjson pr "$pr" --arg head "$head" --arg base "$branch" --argjson repeat "$repeat" \
        '{pipeline:$lane, pr:$pr, head_sha:$head, base_branch:$base, repeat:$repeat}') || return 1
    local response status
    response=$(printf 'Authorization: Bearer %s\n' "$token" |
        curl -q --proto '=https' --silent --show-error --max-time 300 --header @- \
            -H 'Content-Type: application/json' --write-out '\n%{http_code}' --data-binary "$body" "$endpoint") || return 1
    status=${response##*$'\n'}
    receipt=${response%$'\n'*}
    if [[ $status != 200 ]]; then
        jq -c '{error, request_key, completed}' <<< "$receipt" >&2
        return 1
    fi
    jq -e --argjson repeat "$repeat" '.build_ids | type == "array" and length == $repeat and
        all(.[]; type == "number" and . > 0 and floor == .)' <<< "$receipt" >/dev/null || return 1
    printf '%s queued build IDs: %s\n' "$lane" "$(jq -c '.build_ids' <<< "$receipt")"
)

skip_build() {
    set +x
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

    if [[ -z ${conment_to_context[$COMMENT_TRIGGER_TYPE]:-} ]]; then
        echo "ERROR: Unknown TeamCity check ${COMMENT_TRIGGER_TYPE}" >&2
        return 1
    fi
    local state="${TC_BUILD_STATE:-success}" # 可选值 success failure pending
    local payload="{\"state\":\"${state}\",\"target_url\":\"\",\"description\":\"Skip teamCity build\",\"context\":\"${conment_to_context[${COMMENT_TRIGGER_TYPE}]}\"}"
    set +x
    if curl --fail --silent --show-error --max-time 45 \
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

# A skipped Compile also resolves the checks that its fanout would publish.
teamcity_skip_unaffected() {
    teamcity_current_pr || { echo 'PR revision or target changed; no skip status written' >&2; return 1; }
    local lane lanes="$COMMENT_TRIGGER_TYPE"
    if [[ $COMMENT_TRIGGER_TYPE == compile ]]; then
        lanes='compile p0 cloud_p0 vault_p0 external nonconcurrent'
    fi
    for lane in $lanes; do
        skip_build "$COMMIT_ID_FROM_TRIGGER" "$lane" || return 1
    done
}

trigger_or_skip_build() {
    set +x
    # Preserve the existing per-pipeline file filter.
    local FILE_CHANGED="${1:-true}"
    local PULL_REQUEST_NUM="${PULL_REQUEST_NUM:-$2}"
    local PULL_REQUEST_TARGET_BRANCH="${PULL_REQUEST_TARGET_BRANCH:-$3}"
    local COMMIT_ID_FROM_TRIGGER="${COMMIT_ID_FROM_TRIGGER:-$4}"
    local COMMENT_TRIGGER_TYPE="${COMMENT_TRIGGER_TYPE:-$5}"
    if [[ -z "${PULL_REQUEST_NUM}" || -z "${PULL_REQUEST_TARGET_BRANCH}" ||
        -z "${COMMIT_ID_FROM_TRIGGER}" ||
        -z "${COMMENT_TRIGGER_TYPE}" ]]; then
        echo "Usage: trigger_or_skip_build FILE_CHANGED PR TARGET_BRANCH COMMIT LANE"
        return 1
    fi

    [[ -n ${conment_to_context[$COMMENT_TRIGGER_TYPE]:-} ]] || {
        echo "No pipeline for $COMMENT_TRIGGER_TYPE on $PULL_REQUEST_TARGET_BRANCH" >&2; return 1;
    }
    case "$FILE_CHANGED" in
        true)
            printf '%s\n' "$COMMENT_TRIGGER_TYPE" >> "${TEAMCITY_FC_PLAN:?Missing FC lane plan}"
            ;;
        false) teamcity_skip_unaffected ;;
        *) echo "Invalid FILE_CHANGED: $FILE_CHANGED" >&2; return 1 ;;
    esac
}
# trigger_or_skip_build "$1" "$2" "$3" "$4" "$5"

reporting_build_problem() {
    set +e
    desc="$1"
    if [[ -z "${desc}" ]]; then return 1; fi
    # https://www.jetbrains.com/help/teamcity/service-messages.html#Reporting+Build+Problems
    echo "##teamcity[buildProblem description='${desc}']"
}

reporting_messages_error() {
    set +e
    msg="$1"
    if [[ -z "${msg}" ]]; then return 1; fi
    # https://www.jetbrains.com/help/teamcity/service-messages.html#Reporting+Messages+to+Build+Log
    echo "##teamcity[message text='Error Message:' errorDetails='${msg}' status='ERROR']"
}
