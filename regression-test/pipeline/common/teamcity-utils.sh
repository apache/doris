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
declare -A targetBranch_to_pipelines=(
    [master]='compile feut beut cloudut p0 cloud_p0 vault_p0 external nonconcurrent performance'
    [branch-4.2]='compile feut beut cloudut p0 cloud_p0 vault_p0 external nonconcurrent'
    [branch-4.1]='compile feut beut cloudut p0 cloud_p0 vault_p0 external nonconcurrent'
)
declare -A comment_to_pipeline=(
    [compile]=Doris_Compile [feut]=Doris_FEUT [beut]=Doris_BEUT
    [cloudut]=Doris_CloudUT [p0]=Doris_P0 [cloud_p0]=Doris_CloudP0
    [vault_p0]=Doris_VaultP0 [external]=Doris_External
    [nonconcurrent]=Doris_NonConcurrent [performance]=Doris_Performance
)
declare -A conment_to_context
for lane in "${!comment_to_pipeline[@]}"; do conment_to_context[$lane]="TeamCity/$lane"; done
unset lane
TEAMCITY_URL=${TEAMCITY_CURRENT_URL:-}

# Never trace authenticated requests or follow redirects carrying credentials.
teamcity_request() {
    set +x
    [[ ${TEAMCITY_URL:-} =~ ^https://[A-Za-z0-9.-]+(:[0-9]+)?$ && -n ${TEAMCITY_CURRENT_TOKEN:-} ]] || {
        echo 'TeamCity HTTPS URL/token is not configured' >&2; return 1;
    }
    curl --header "Authorization: Bearer ${TEAMCITY_CURRENT_TOKEN}" "$@"
}

teamcity_rest() {
    local path=$1; shift
    teamcity_request --globoff --fail --silent --show-error --max-time 45 --max-filesize 8388608 \
        -H 'Accept: application/json' -H 'Content-Type: application/json' \
        "${TEAMCITY_URL}/app/rest/${path}" "$@"
}

teamcity_current_pr() {
    set +x
    curl --fail --silent --show-error --max-time 45 \
        -H "Authorization: Bearer ${GITHUB_TOKEN:?}" \
        "https://api.github.com/repos/apache/doris/pulls/${PULL_REQUEST_NUM}" |
        jq -e --arg head "$COMMIT_ID_FROM_TRIGGER" --arg branch "$PULL_REQUEST_TARGET_BRANCH" \
            '.state=="open" and .draft==false and .head.sha==$head and .base.ref==$branch' >/dev/null
}

teamcity_current_root() {
    teamcity_rest "buildTypes/id:$1/vcs-root-entries" |
        jq -e '."vcs-root-entry" | length==1 and .[0]."vcs-root".id=="DorisMigrationValidation_ApacheDoris"' >/dev/null || {
            echo "$1 is not configured for general PR builds; no build submitted" >&2; return 1;
        }
}

teamcity_current_enqueue() {
    local type=$1 params=$2 count=$3 ordinal id instance payload actual
    teamcity_current_root "$type" || return 1
    instance=$(teamcity_rest "vcs-root-instances?locator=buildType:(id:${type})&fields=vcs-root-instance(id,vcs-root-id)" |
        jq -er '[."vcs-root-instance"[] | select(."vcs-root-id"=="DorisMigrationValidation_ApacheDoris")] | select(length==1) | .[0].id') || return 1
    for ((ordinal=1; ordinal<=count; ordinal++)); do
        teamcity_current_pr || { echo 'PR revision or target changed; no new build submitted' >&2; return 1; }
        payload=$(jq -cn --arg type "$type" --arg pr "$PULL_REQUEST_NUM" --arg head "$COMMIT_ID_FROM_TRIGGER" --arg instance "$instance" --argjson params "$params" '
            {buildType:{id:$type},branchName:("pull/"+$pr),revisions:{failOnMissingRevisions:true,revision:[{version:$head,vcsBranchName:("refs/pull/"+$pr+"/head"),"vcs-root-instance":{id:$instance}}]},
             properties:{property:($params|to_entries|map({name:.key,value:.value}))},
             triggeringOptions:{queueAtTop:false,rebuildAllDependencies:false}}') || return 1
        # Queue replacement and duplicate handling belong to TeamCity.
        id=$(teamcity_rest buildQueue -X POST --data-binary "$payload" | jq -er .id) || return 1
        actual=$(teamcity_rest "builds/id:${id}?fields=${TC_FIELDS}") || return 1
        jq -e --arg head "$COMMIT_ID_FROM_TRIGGER" --argjson params "$params" '
            ((.properties.property // []) | map({key:.name,value:.value}) | from_entries) as $p |
            [.revisions.revision[].version]==[$head] and all($params|to_entries[]; $p[.key]==.value)' <<< "$actual" >/dev/null || return 1
        echo "INFO: ${type} build ${id}: ${TEAMCITY_URL}/buildConfiguration/${type}/${id}" >&2
        printf '%s\n' "$id"
    done
}

# Artifact dependencies resolve before any runner executes. Bind an existing
# successful Compile here; a direct regression request must never create one.
teamcity_regression_parameters() (
    set -euo pipefail
    local tmp path page candidate id next
    tmp=$(mktemp -d) || exit 1
    trap 'rm -rf "$tmp"' EXIT
    local fields='id,number,buildTypeId,state,status,branchName,canceledInfo,revisions(revision(version))'
    path="builds?locator=buildType:(id:Doris_Compile),branch:(name:pull/${PULL_REQUEST_NUM}),state:finished,status:SUCCESS,defaultFilter:false,count:100&fields=nextHref,build(${fields})"
    while :; do
        page=$(teamcity_rest "$path") || exit 1
        candidate=$(jq -c --arg head "$COMMIT_ID_FROM_TRIGGER" '
            [.build[]? | select([.revisions.revision[].version]==[$head])][0] // empty' <<< "$page") || exit 1
        if [[ -n $candidate ]]; then break; fi
        next=$(jq -r '.nextHref // empty' <<< "$page") || exit 1
        if [[ -z $next ]]; then
            echo "No successful Compile for PR ${PULL_REQUEST_NUM} at ${COMMIT_ID_FROM_TRIGGER}; run compile first" >&2
            exit 1
        fi
        # Only accept TeamCity pagination on this resource, never an external URL.
        [[ $next == /app/rest/builds\?* && ${next#/app/rest/} != "$path" ]] || exit 1
        path=${next#/app/rest/}
    done
    id=$(jq -er '.id | tostring | select(test("^[1-9][0-9]*$"))' <<< "$candidate") || exit 1
    # Refresh terminal state instead of trusting the list response.
    teamcity_rest "builds/id:${id}?fields=${fields}" > "$tmp/build.json" || exit 1
    teamcity_rest "builds/id:${id}/artifacts/content/regression/manifest.json" > "$tmp/manifest.json" || exit 1
    teamcity_rest "builds/id:${id}/artifacts/children/regression?fields=file(name,size)" > "$tmp/artifacts.json" || exit 1
    python3 "$(dirname "${BASH_SOURCE[0]}")/teamcity_artifacts.py" \
        "$tmp" "$PULL_REQUEST_NUM" "$COMMIT_ID_FROM_TRIGGER" "$PULL_REQUEST_TARGET_BRANCH" || exit 1
    echo "INFO: Using Compile ${id} for PR ${PULL_REQUEST_NUM} at ${COMMIT_ID_FROM_TRIGGER}" >&2
)

teamcity_current_trigger() (
    set -euo pipefail
    set +x
    local lane=$1 count=${2:-1} params
    [[ $PULL_REQUEST_NUM =~ ^[1-9][0-9]*$ && $COMMIT_ID_FROM_TRIGGER =~ ^[0-9a-f]{40}$ && $count =~ ^[1-9][0-9]*$ && $count -le 20 ]]
    [[ " ${targetBranch_to_pipelines[$PULL_REQUEST_TARGET_BRANCH]:-} " == *" $lane "* ]] || {
        echo "No pipeline for $lane on $PULL_REQUEST_TARGET_BRANCH" >&2; exit 1;
    }
    # Compile owns buildall fanout; this lookup is only for explicit regressions.
    local TC_FIELDS='id,revisions(revision(version)),properties(property(name,value))'
    params=$(jq -cn --arg pr "$PULL_REQUEST_NUM" --arg branch "$PULL_REQUEST_TARGET_BRANCH" '{"env.PR_NUMBER":$pr,"env.CI_BRANCH":$branch}')
    case "$lane" in
        p0|cloud_p0|vault_p0|external|nonconcurrent)
            teamcity_current_pr || exit 1
            if ! params=$(teamcity_regression_parameters); then
                echo "Cannot bind Compile artifacts for ${lane}; no build or Compile submitted" >&2
                teamcity_current_pr || exit 1
                TC_BUILD_STATE=failure TC_BUILD_DESCRIPTION='Compile artifacts unavailable or invalid; run compile first' \
                    skip_build "$COMMIT_ID_FROM_TRIGGER" "$lane" || exit 1
                exit 1
            fi
            ;;
    esac
    teamcity_current_enqueue "${comment_to_pipeline[$lane]}" "$params" "$count"
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
    local payload
    payload=$(jq -cn --arg state "$state" --arg context "${conment_to_context[$COMMENT_TRIGGER_TYPE]}" \
        --arg description "${TC_BUILD_DESCRIPTION:-Skip teamCity build}" \
        '{state:$state,target_url:"",description:$description,context:$context}') || return 1
    set +x
    if curl --fail --silent --show-error --max-time 45 \
        -X POST \
        -H "Accept: application/vnd.github+json" \
        -H "Authorization: Bearer ${GITHUB_TOKEN:-}" \
        -H "X-GitHub-Api-Version: 2022-11-28" \
        "https://api.github.com/repos/apache/doris/statuses/${COMMIT_ID_FROM_TRIGGER:-}" \
        -d "${payload}"; then
        set +x
        echo "INFO: Reported ${state} for ${COMMIT_ID_FROM_TRIGGER} ${COMMENT_TRIGGER_TYPE}"
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
    local PULL_REQUEST_TARGET_BRANCH="${PULL_REQUEST_TARGET_BRANCH:-${TARGET_BRANCH:-}}"
    if [[ -z "${PULL_REQUEST_NUM}" || -z "${COMMIT_ID_FROM_TRIGGER}" || -z "${COMMENT_TRIGGER_TYPE}" ]]; then
        echo "Usage: add_build PULL_REQUEST_NUM COMMIT_ID_FROM_TRIGGER COMMENT_TRIGGER_TYPE [COMMENT_REPEAT_TIMES]"
        return 1
    fi
    teamcity_current_trigger "$COMMENT_TRIGGER_TYPE" "${COMMENT_REPEAT_TIMES:-1}"
}
# trigger_build "$1" "$2" "$3" "$4"

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
    # Preserve file filters for independent jobs and Compile-owned fanout.
    local FILE_CHANGED="${1:-true}"
    local PULL_REQUEST_NUM="${PULL_REQUEST_NUM:-$2}"
    local PULL_REQUEST_TARGET_BRANCH="${PULL_REQUEST_TARGET_BRANCH:-$3}"
    local COMMIT_ID_FROM_TRIGGER="${COMMIT_ID_FROM_TRIGGER:-$4}"
    local COMMENT_TRIGGER_TYPE="${COMMENT_TRIGGER_TYPE:-$5}"
    local COMMENT_REPEAT_TIMES="${COMMENT_REPEAT_TIMES:-$6}"
    if [[ -z "${PULL_REQUEST_NUM}" ||
        -z "${COMMIT_ID_FROM_TRIGGER}" ||
        -z "${COMMENT_TRIGGER_TYPE}" ]]; then
        echo "Usage: add_build FILE_CHANGED PULL_REQUEST_NUM COMMIT_ID_FROM_TRIGGER COMMENT_TRIGGER_TYPE [COMMENT_REPEAT_TIMES]"
        return 1
    fi

    [[ " ${targetBranch_to_pipelines[$PULL_REQUEST_TARGET_BRANCH]:-} " == *" $COMMENT_TRIGGER_TYPE "* ]] || {
        echo "No pipeline for $COMMENT_TRIGGER_TYPE on $PULL_REQUEST_TARGET_BRANCH" >&2; return 1;
    }
    # An explicit regression request must check/run its package, even when its
    # file filter would otherwise publish a skip. Buildall never calls these lanes.
    case "$COMMENT_TRIGGER_TYPE" in
        p0|cloud_p0|vault_p0|external|nonconcurrent) FILE_CHANGED=true ;;
    esac
    case "$FILE_CHANGED" in
        true) trigger_build "$PULL_REQUEST_NUM" "$COMMIT_ID_FROM_TRIGGER" "$COMMENT_TRIGGER_TYPE" "${COMMENT_REPEAT_TIMES:-1}" ;;
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
