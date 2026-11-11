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

# The same pinned manifest is used by automatic Compile fanout and comment reruns.
teamcity_artifact_parameters() {
    python3 - "$1" "$2" "$3" "$PULL_REQUEST_NUM" "$COMMIT_ID_FROM_TRIGGER" "$PULL_REQUEST_TARGET_BRANCH" <<'PY'
import datetime, hashlib, json, re, sys
mode, build_path, manifest_path, pr, head, branch = sys.argv[1:]
build = json.load(open(build_path)); raw = open(manifest_path, 'rb').read(); m = json.loads(raw)
s = m['source']; p = m['producer']; bid = str(build['id']); digest = hashlib.sha256(raw).hexdigest()
assert build['state'] == 'finished' and build['status'] == 'SUCCESS' and not build.get('canceledInfo')
assert build['branchName'] == 'pull/' + pr and [r['version'] for r in build['revisions']['revision']] == [head]
assert s['repository'] == 'apache/doris' and str(s['pr_number']) == pr and s['pr_head_sha'] == head and s['target_branch'] == branch
assert all(re.fullmatch('[0-9a-f]{40}', s[k]) for k in ['pr_head_sha', 'target_head_sha', 'checkout_sha'])
assert str(p['compiled_in_build']) == str(p['packaged_in_build']) == bid
assert all(re.fullmatch('[0-9a-f]{64}', v['sha256']) and 0 < v['bytes'] < 30*1024**3 for v in m['packages'].values())
binary = m['packages']['binary.tar.gz']; values = {'PR_NUMBER': pr, 'CI_BRANCH': branch}
if mode == 'performance':
    assert build['buildTypeId'] == p['build_type'] == 'Doris_Performance_Compile'
    assert m['trust_class'] == 'pr-merge-performance-release' and m['flavor']['status'] == 'PASS'
    values.update(PERF_INPUT_MODE='native-teamcity-artifacts', PERF_COMPILED_BUILD_ID=bid,
                  PERF_PRODUCER_BUILD_ID=bid, PERF_PACKAGE_TERMINAL_SUCCESS='true', PERF_MANIFEST_SHA256=digest,
                  PERF_ORIGINAL_BINARY_SHA256=binary['sha256'], PERF_BASE_SHA=s['target_head_sha'], PERF_MERGE_SHA=s['checkout_sha'])
else:
    assert build['buildTypeId'] == 'Doris_Compile' and m['trust_class'] == 'pr-merge'
    if datetime.datetime.fromisoformat(m['expires_at'].replace('Z', '+00:00')) <= datetime.datetime.now(datetime.timezone.utc):
        sys.exit(3)
    assert s['modified'] is False and m['frameworkSource']['modified'] is False and not m.get('patches') and not m.get('frameworkPatched')
    assert set(m['packages']) == {'binary.tar.gz', 'cases.tar.gz'} and m['tools']
    assert re.fullmatch('[0-9a-f]{64}', m['unchangedTestTree']['treeSha256'])
    values.update(COMPILED_PR_HEAD=head, CI_COMPILE_BUILD_ID=bid, CI_BINARY_SHA256=binary['sha256'],
                  CI_PREPARED_MANIFEST_SHA256=digest, CI_PREPARED_BUILD_NUMBER=build['number'],
                  EXPECTED_PRODUCER_IMAGE=p['image'], TEST_TREE_PROOF_SHA256=m['unchangedTestTree']['treeSha256'], CI_FANOUT_KEY='')
    aliases = [(bid, 'COMPILE_BUILD_ID EXPECTED_COMPILE_BUILD_ID PACKAGE_BUILD_ID PREPARED_BUILD_ID EXPECTED_PREPARED_BUILD_ID'),
               (digest, 'COMPILE_MANIFEST_SHA256 EXPECTED_COMPILE_MANIFEST_SHA256 EXPECTED_ORIGINAL_MANIFEST_SHA256 ORIGINAL_MANIFEST_SHA256 PACKAGE_MANIFEST_SHA256 PREPARED_MANIFEST_SHA256 EXPECTED_MANIFEST_SHA256'),
               (binary['sha256'], 'COMPILE_BINARY_SHA256 EXPECTED_BINARY_SHA256 EXPECTED_COMPILE_BINARY_SHA256 ORIGINAL_BINARY_SHA256'),
               (str(binary['bytes']), 'COMPILE_BINARY_BYTES EXPECTED_COMPILE_BINARY_BYTES'),
               (head, 'EXPECTED_PR_HEAD PR_HEAD_SHA'), (s['target_head_sha'], 'EXPECTED_COMPILE_BASE EXPECTED_PR_BASE_SHA EXPECTED_BASE_SHA PR_BASE_SHA'),
               (s['checkout_sha'], 'SOURCE_SHA EXPECTED_SOURCE_SHA PR_MERGE_SHA'), (branch, 'TARGET_BRANCH PR_TARGET_BRANCH'),
               (build['number'], 'PACKAGE_BUILD_NUMBER'), ('finished', 'COMPILE_VERIFIED_STATE PREPARED_VERIFIED_STATE'),
               ('SUCCESS', 'COMPILE_VERIFIED_STATUS PREPARED_VERIFIED_STATUS')]
    for value, names in aliases:
        values.update({name: value for name in names.split()})
print(json.dumps({'env.' + k: str(v) for k, v in values.items()}))
PY
}

teamcity_current_producer() {
    local mode=$1 type=Doris_Compile artifact=regression/manifest.json build rc
    if [[ $mode == performance ]]; then type=Doris_Performance_Compile; artifact=performance-release-packages/manifest.json; fi
    teamcity_rest "builds?locator=buildType:(id:${type}),branch:(name:pull/${PULL_REQUEST_NUM}),state:finished,status:SUCCESS,defaultFilter:false,count:100&fields=build(${TC_FIELDS})" > "$TC_TMP/producers.json" || return 1
    while IFS= read -r build; do
        printf '%s\n' "$build" > "$TC_TMP/producer.json"
        teamcity_rest "builds/id:$(jq -r .id <<< "$build")/artifacts/content/$artifact" > "$TC_TMP/manifest.json" || return 1
        rc=0
        teamcity_artifact_parameters "$mode" "$TC_TMP/producer.json" "$TC_TMP/manifest.json" > "$TC_TMP/parameters.json" || rc=$?
        [[ $rc == 3 ]] && continue
        [[ $rc == 0 ]] || return 1
        return 0
    done < <(jq -c --arg head "$COMMIT_ID_FROM_TRIGGER" '(.build // []) | sort_by(.id) | reverse[] | select([.revisions.revision[].version]==[$head] and .canceledInfo==null)' "$TC_TMP/producers.json")
    return 3
}

teamcity_current_enqueue() {
    local type=$1 params=$2 count=$3 dependencies=${4:-null} ordinal id instance payload actual
    teamcity_current_root "$type" || return 1
    instance=$(teamcity_rest "vcs-root-instances?locator=buildType:(id:${type})&fields=vcs-root-instance(id,vcs-root-id)" |
        jq -er '[."vcs-root-instance"[] | select(."vcs-root-id"=="DorisMigrationValidation_ApacheDoris")] | select(length==1) | .[0].id') || return 1
    for ((ordinal=1; ordinal<=count; ordinal++)); do
        teamcity_current_pr || { echo 'PR revision or target changed; no new build submitted' >&2; return 1; }
        payload=$(jq -cn --arg type "$type" --arg pr "$PULL_REQUEST_NUM" --arg head "$COMMIT_ID_FROM_TRIGGER" --arg instance "$instance" --argjson params "$params" --argjson deps "$dependencies" '
            {buildType:{id:$type},branchName:("pull/"+$pr),revisions:{failOnMissingRevisions:true,revision:[{version:$head,vcsBranchName:("refs/pull/"+$pr+"/head"),"vcs-root-instance":{id:$instance}}]},
             properties:{property:($params|to_entries|map({name:.key,value:.value}))},
             triggeringOptions:{queueAtTop:false,rebuildAllDependencies:false}} +
            (if $deps==null then {} else {"custom-artifact-dependencies":{"artifact-dependency":$deps}} end)') || return 1
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

teamcity_current_trigger() (
    set -euo pipefail
    set +x
    umask 077
    local lane=$1 count=${2:-1} params type rc dependencies=null regression_lane
    [[ $PULL_REQUEST_NUM =~ ^[1-9][0-9]*$ && $COMMIT_ID_FROM_TRIGGER =~ ^[0-9a-f]{40}$ && $count =~ ^[1-9][0-9]*$ && $count -le 20 ]]
    [[ " ${targetBranch_to_pipelines[$PULL_REQUEST_TARGET_BRANCH]:-} " == *" $lane "* ]] || {
        echo "No current pipeline for $lane on $PULL_REQUEST_TARGET_BRANCH" >&2; exit 1;
    }
    TC_TMP=$(mktemp -d); trap 'rm -rf -- "$TC_TMP"' EXIT
    TC_FIELDS='id,number,buildTypeId,branchName,state,status,canceledInfo,revisions(revision(version)),properties(property(name,value))'
    params=$(jq -cn --arg pr "$PULL_REQUEST_NUM" --arg branch "$PULL_REQUEST_TARGET_BRANCH" '{"env.PR_NUMBER":$pr,"env.CI_BRANCH":$branch}')
    type=${comment_to_pipeline[$lane]}
    case $lane in
        p0|cloud_p0|vault_p0|external|nonconcurrent)
            # The Compile server retains its internal camel-case fanout parameter.
            regression_lane=$lane
            [[ $lane != nonconcurrent ]] || regression_lane=nonConcurrent
            rc=0; teamcity_current_producer regression || rc=$?
            if [[ $rc == 3 ]]; then
                echo "No current verified artifact; Compile will trigger $lane" >&2
                params=$(jq -c --arg lane "$regression_lane" '.+{regression_targets:$lane,is_debug:"false"}' <<< "$params")
                type=Doris_Compile
            else
                [[ $rc == 0 ]]
                params=$(cat "$TC_TMP/parameters.json")
                dependencies=$(teamcity_rest "buildTypes/id:${type}/artifact-dependencies" |
                    jq -ce --arg number "$(jq -r .number "$TC_TMP/producer.json")" '
                    ."artifact-dependency" | select(length==1 and .[0]."source-buildType".id=="Doris_Compile") |
                    map({type:"artifact_dependency","source-buildType":{id:"Doris_Compile"},properties:{property:
                      ((.properties.property|map(select(.name!="revisionName" and .name!="revisionValue")))+
                       [{name:"revisionName",value:"buildNumber"},{name:"revisionValue",value:$number}])}})')
            fi;;
        compile) params=$(jq -c '.+{regression_targets:"p0,cloud_p0,vault_p0,external,nonConcurrent",is_debug:"false"}' <<< "$params");;
        performance) teamcity_current_performance; exit $?;;
    esac
    teamcity_current_enqueue "$type" "$params" "$count" "$dependencies"
)

teamcity_current_performance() {
    local producer rc=0 deadline state baseline file dependencies
    teamcity_current_root Doris_Performance_Compile && teamcity_current_root Doris_Performance || return 1
    teamcity_current_producer performance || rc=$?
    if [[ $rc == 3 ]]; then
        comment_to_pipeline[performance_compile]=Doris_Performance_Compile
        producer=$(teamcity_current_enqueue Doris_Performance_Compile "$params" 1) || return 1
        deadline=$((SECONDS+10800))
        while :; do
            state=$(teamcity_rest "builds/id:${producer}?fields=state,status") || return 1
            [[ $(jq -r .state <<< "$state") == finished ]] && break
            [[ $SECONDS -lt $deadline ]] && teamcity_current_pr || return 1
            echo "Waiting for Release Compile $producer; running build is preserved" >&2
            sleep 20
        done
        [[ $(jq -r .status <<< "$state") == SUCCESS ]] || return 1
        teamcity_current_producer performance || return 1
    else
        [[ $rc == 0 ]] || return 1
    fi
    producer=$(jq -r .id "$TC_TMP/producer.json")
    teamcity_rest "builds/id:${producer}/artifacts/content/performance-release-evidence/performance-flavor-proof.json" > "$TC_TMP/flavor.json" || return 1
    teamcity_rest "builds/id:${producer}/artifacts/content/performance-cleanup/cleanup-receipt.json" > "$TC_TMP/release-cleanup.json" || return 1
    baseline=$(teamcity_rest "builds?locator=buildType:(id:Doris_Performance_BaselineCompatibility),state:finished,status:SUCCESS,defaultFilter:false,count:1&fields=build(id,properties(property(name,value)))") || return 1
    jq -e '.build[0]' <<< "$baseline" > "$TC_TMP/baseline.json" || return 1
    baseline=$(jq -r .id "$TC_TMP/baseline.json")
    for file in baseline-before.json baseline-after.json actual-table-rows.json cleanup.json; do
        teamcity_rest "builds/id:${baseline}/artifacts/content/performance-baseline-evidence/${file}" > "$TC_TMP/$file" || return 1
    done
    params=$(python3 - "$TC_TMP" <<'PY'
import hashlib, json, re, sys
from pathlib import Path
root = Path(sys.argv[1]); read = lambda name: json.loads((root / name).read_text())
m = read('manifest.json'); proof = (root / 'flavor.json').read_bytes(); cleanup = read('release-cleanup.json')
assert hashlib.sha256(proof).hexdigest() == m['proof']['sha256'] and json.loads(proof) == m['flavor']
assert cleanup['complete'] and cleanup['cleanup_exit'] == 0
p = {x['name']: x.get('value', '') for x in read('baseline.json')['properties']['property']}
digest, uuid = p['env.PERF_SEED_MANIFEST_SHA256'], p['env.PERF_DATA_UUID']
assert re.fullmatch('[0-9a-f]{64}', digest) and re.fullmatch('[0-9a-f-]{36}', uuid)
before, after = read('baseline-before.json'), read('baseline-after.json')
assert all(x['manifest_sha256'] == digest and x['baseline_immutable_to_agent'] for x in [before, after])
assert before['files_verified'] == after['files_verified'] > 0
rows, cleanup = read('actual-table-rows.json'), read('cleanup.json')
assert rows['state'] == 'ALL_THREE_DATASETS_ACTUAL_ROWS_MATCH' and len(rows['tables']) == 33
assert all(x['actual'] == x['expected'] for x in rows['tables'].values()) and cleanup['complete'] and cleanup['cleanup_exit'] == 0
params = read('parameters.json')
params.update({'env.PERF_SEED_ROOT': '/data/performance-baseline/master',
               'env.PERF_SEED_MANIFEST_SHA256': digest, 'env.PERF_DATA_UUID': uuid})
print(json.dumps(params))
PY
    ) || return 1
    dependencies=$(jq -cn --arg number "$(jq -r .number "$TC_TMP/producer.json")" '[{type:"artifact_dependency","source-buildType":{id:"Doris_Performance_Compile"},properties:{property:[
        {name:"revisionName",value:"buildNumber"},{name:"revisionValue",value:$number},{name:"cleanDestinationDirectory",value:"true"},
        {name:"pathRules",value:"performance-release-packages/manifest.json => performance-input-%teamcity.build.id%\nperformance-release-packages/binary.tar.gz => performance-input-%teamcity.build.id%"}]}}]') || return 1
    teamcity_current_enqueue Doris_Performance "$params" "$count" "$dependencies"
}

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
    # Preserve the existing per-pipeline file filter.
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
