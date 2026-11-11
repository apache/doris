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

# PR的目标分支
# 到
# 可以跑的流水线
# 的映射
# 控制哪些分支可以跑哪些流水线
declare -A targetBranch_to_pipelines
targetBranch_to_pipelines=(
    ['master']='feut beut cloudut compile p0 p1 external performance cloud_p0 cloud_p1 vault_p0 nonConcurrent check_coverage check_coverage_fe'
    ['branch-4.2']='feut beut cloudut compile p0 p1 external cloud_p0 cloud_p1 vault_p0 nonConcurrent check_coverage check_coverage_fe'
    ['branch-4.1']='feut beut cloudut compile p0 p1 external cloud_p0 cloud_p1 vault_p0 nonConcurrent check_coverage check_coverage_fe'
    ['branch-4.0']='feut beut cloudut compile p0 p1 external cloud_p0 cloud_p1 vault_p0 nonConcurrent check_coverage check_coverage_fe'
    ['branch-3.1']='feut beut cloudut compile p0 p1 external cloud_p0 cloud_p1 vault_p0 nonConcurrent check_coverage check_coverage_fe'
    ['branch-3.0']='feut beut cloudut compile p0 p1 external cloud_p0 cloud_p1 vault_p0 nonConcurrent check_coverage'
    ['branch-2.1']='feut beut compile p0 p1 external'
    ['branch-2.0']='feut beut compile p0 p1 external'
)

# github中评论的要触发的流水线名字
# 到
# teamcity流水线实际的名称
# 的映射
# 新加流水线需要修改这里
declare -A comment_to_pipeline
comment_to_pipeline=(
    ['feut']='Doris_Doris_FeUt'
    ['beut']='Doris_DorisBeUt_BeUt'
    ['cloudut']='Doris_DorisCloudUt_CloudUt'
    ['compile']='Doris_DorisCompile_Compile'
    ['p0']='Doris_DorisRegression_P0Regression'
    ['p1']='Doris_DorisRegression_P1Regression'
    ['external']='Doris_External_Regression'
    ['performance']='Doris_DorisPerformance_Performance'
    ['cloud_p0']='Doris_DorisRegression_CloudP0'
    ['cloud_p1']='Doris_DorisCloudRegression_CloudP1'
    ['vault_p0']='Doris_DorisCloudRegression_VaultP0'
    ['nonConcurrent']='Doris_DorisRegression_NonConcurrentRegression'
    ['check_coverage']='Doris_Coverage_Merge_P0_UT'
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
    ['cloudut']='Cloud UT (Doris Cloud UT)'
    ['p0']='P0 Regression (Doris Regression)'
    ['p1']='P1 Regression (Doris Regression)'
    ['external']='External Regression (Doris External Regression)'
    ['performance']='performance (Doris Performance)'
    ['cloud_p0']='cloud_p0 (Doris Cloud Regression)'
    ['cloud_p1']='cloud_p1 (Doris Cloud Regression)'
    ['vault_p0']='vault_p0 (Doris Cloud Regression)'
    ['nonConcurrent']='NonConcurrent Regression (nonConcurrent)'
    ['check_coverage']='check_coverage (Coverage)'
)

# Retain the existing command API; select the server and names by target branch.
teamcity_configure() {
    TC_CURRENT=false
    TEAMCITY_URL=http://43.132.222.7:8111
    case "${1:-}" in master|branch-4.2|branch-4.1) TC_CURRENT=true;; *) return 0;; esac
    TEAMCITY_URL=${TEAMCITY_CURRENT_URL:-}
    comment_to_pipeline=([compile]=Doris_Compile [feut]=Doris_FEUT [beut]=Doris_BEUT
        [cloudut]=Doris_CloudUT [p0]=Doris_P0 [cloud_p0]=Doris_CloudP0
        [vault_p0]=Doris_VaultP0 [external]=Doris_External
        [nonConcurrent]=Doris_NonConcurrent [performance]=Doris_Performance)
    conment_to_context=()
    local lane
    for lane in "${!comment_to_pipeline[@]}"; do conment_to_context[$lane]="TeamCity/${lane,,}"; done
    targetBranch_to_pipelines[master]='compile feut beut cloudut p0 cloud_p0 vault_p0 external nonConcurrent performance'
    targetBranch_to_pipelines[branch-4.2]='compile feut beut cloudut p0 cloud_p0 vault_p0 external nonConcurrent'
    targetBranch_to_pipelines[branch-4.1]=${targetBranch_to_pipelines[branch-4.2]}
}

# Never trace authenticated requests or follow redirects carrying credentials.
teamcity_request() {
    set +x
    if [[ ${TC_CURRENT:-false} == true ]]; then
        [[ ${TEAMCITY_URL:-} =~ ^https://[A-Za-z0-9.-]+(:[0-9]+)?$ && -n ${TEAMCITY_CURRENT_TOKEN:-} ]] || {
            echo 'Current TeamCity HTTPS URL/token is not configured' >&2; return 1;
        }
        curl --header "Authorization: Bearer ${TEAMCITY_CURRENT_TOKEN}" "$@"
    else
        [[ -n ${TEAMCITY_LEGACY_USER:-} && -n ${TEAMCITY_LEGACY_PASSWORD:-} ]] || {
            echo 'Legacy TeamCity credentials are not configured' >&2; return 1;
        }
        curl --user "${TEAMCITY_LEGACY_USER}:${TEAMCITY_LEGACY_PASSWORD}" "$@"
    fi
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
    local type=$1 params=$2 count=$3 dependencies=${4:-null} key_root key ordinal id row props active instance payload actual
    teamcity_current_root "$type" || return 1
    key_root=$(jq -c '[.comment.id,.comment.updated_at,.comment.body]' "${GITHUB_EVENT_PATH:?}" | sha256sum | cut -d' ' -f1)
    key_root="comment:${key_root}:${COMMIT_ID_FROM_TRIGGER}:${type}"
    teamcity_rest "builds?locator=buildType:(id:${type}),branch:(name:pull/${PULL_REQUEST_NUM}),defaultFilter:false,count:100&fields=build(${TC_FIELDS})" > "$TC_TMP/history.json" || return 1
    teamcity_rest "buildQueue?locator=buildType:(id:${type})&fields=build(${TC_FIELDS})" > "$TC_TMP/queued.json" || return 1
    jq -s '{build:([.[].build[]?]|unique_by(.id))}' "$TC_TMP/history.json" "$TC_TMP/queued.json" > "$TC_TMP/combined.json" || return 1
    mv "$TC_TMP/combined.json" "$TC_TMP/history.json"
    # Reuse the existing revision-aware queue/running lookup, without canceling builds.
    local COMMENT_TRIGGER_TYPE=${TC_REQUEST_LANE} duplicate_status=0
    active=$(get_active_builds_of_revision "$PULL_REQUEST_NUM" "$COMMENT_TRIGGER_TYPE" "$COMMIT_ID_FROM_TRIGGER") || duplicate_status=$?
    [[ $duplicate_status == 0 || $duplicate_status == 1 ]] || return 1
    instance=$(teamcity_rest "vcs-root-instances?locator=buildType:(id:${type})&fields=vcs-root-instance(id,vcs-root-id)" |
        jq -er '[."vcs-root-instance"[] | select(."vcs-root-id"=="DorisMigrationValidation_ApacheDoris")] | select(length==1) | .[0].id') || return 1
    for ((ordinal=1; ordinal<=count; ordinal++)); do
        key="${key_root}:${ordinal}"
        id=$(jq -r --arg key "$key" '[.build[]? | select(any(.properties.property[]?; .name=="env.CI_COMMENT_REQUEST_KEY" and .value==$key))] | if length>1 then error("Duplicate request keys") else .[0].id // empty end' "$TC_TMP/history.json") || return 1
        while [[ -z $id && -n $active ]]; do
            row=${active%%$'\n'*}; [[ $active == *$'\n'* ]] && active=${active#*$'\n'} || active=''
            [[ " ${TC_USED_IDS:-} " == *" $row "* ]] && continue
            actual=$(teamcity_rest "builds/id:${row}?fields=${TC_FIELDS}") || return 1
            # Producer pins must match; Compile must cover all requested regression lanes.
            if jq -e --argjson expected "$params" --arg root "$key_root" '
                ((.properties.property // []) | map({key:.name,value:.value}) | from_entries) as $p |
                .status!="FAILURE" and .canceledInfo==null and
                (($p["env.CI_COMMENT_REQUEST_KEY"] // "" | startswith($root)) | not) and
                all($expected | to_entries[]; .key as $k | .value as $v |
                    if $k=="regression_targets" then (($v|split(","))-($p[$k] // "p0,cloud_p0,vault_p0,external,nonConcurrent"|split(","))|length)==0
                    elif $k=="env.CI_FANOUT_KEY" or $k=="env.PR_NUMBER" or $k=="env.CI_BRANCH" then true else $p[$k]==$v end)' <<< "$actual" >/dev/null; then id=$row; fi
        done
        if [[ -z $id ]]; then
            teamcity_current_pr || { echo 'PR revision or target changed; no new build submitted' >&2; return 1; }
            payload=$(jq -cn --arg type "$type" --arg pr "$PULL_REQUEST_NUM" --arg head "$COMMIT_ID_FROM_TRIGGER" --arg instance "$instance" --arg key "$key" --argjson params "$params" --argjson deps "$dependencies" '
                {buildType:{id:$type},branchName:("pull/"+$pr),revisions:{failOnMissingRevisions:true,revision:[{version:$head,vcsBranchName:("refs/pull/"+$pr+"/head"),"vcs-root-instance":{id:$instance}}]},
                 properties:{property:(($params+{"env.CI_COMMENT_REQUEST_KEY":$key})|to_entries|map({name:.key,value:.value}))},
                 triggeringOptions:{queueAtTop:false,rebuildAllDependencies:false}} +
                (if $deps==null then {} else {"custom-artifact-dependencies":{"artifact-dependency":$deps}} end)') || return 1
            # No automatic POST retry: a workflow retry reconciles the persisted request key.
            id=$(teamcity_rest buildQueue -X POST --data-binary "$payload" | jq -er .id) || return 1
            actual=$(teamcity_rest "builds/id:${id}?fields=${TC_FIELDS}") || return 1
            jq -e --arg head "$COMMIT_ID_FROM_TRIGGER" --arg key "$key" --argjson params "$params" '
                ((.properties.property // []) | map({key:.name,value:.value}) | from_entries) as $p |
                [.revisions.revision[].version]==[$head] and $p["env.CI_COMMENT_REQUEST_KEY"]==$key and all($params|to_entries[]; $p[.key]==.value)' <<< "$actual" >/dev/null || return 1
        fi
        TC_USED_IDS="${TC_USED_IDS:-} $id"
        echo "INFO: ${type} build ${id}: ${TEAMCITY_URL}/buildConfiguration/${type}/${id}" >&2
        printf '%s\n' "$id"
    done
}

teamcity_current_trigger() (
    set -euo pipefail
    set +x
    umask 077
    local lane=$1 count=${2:-1} params type rc dependencies=null
    [[ $PULL_REQUEST_NUM =~ ^[1-9][0-9]*$ && $COMMIT_ID_FROM_TRIGGER =~ ^[0-9a-f]{40}$ && $count =~ ^[1-9][0-9]*$ && $count -le 20 ]]
    [[ " ${targetBranch_to_pipelines[$PULL_REQUEST_TARGET_BRANCH]} " == *" $lane "* ]] || {
        echo "No current pipeline for $lane on $PULL_REQUEST_TARGET_BRANCH" >&2; exit 1;
    }
    TC_TMP=$(mktemp -d); trap 'rm -rf -- "$TC_TMP"' EXIT
    TC_FIELDS='id,number,buildTypeId,branchName,state,status,canceledInfo,revisions(revision(version)),properties(property(name,value))'
    TC_REQUEST_LANE=$lane
    params=$(jq -cn --arg pr "$PULL_REQUEST_NUM" --arg branch "$PULL_REQUEST_TARGET_BRANCH" '{"env.PR_NUMBER":$pr,"env.CI_BRANCH":$branch}')
    type=${comment_to_pipeline[$lane]}
    case $lane in
        p0|cloud_p0|vault_p0|external|nonConcurrent)
            rc=0; teamcity_current_producer regression || rc=$?
            if [[ $rc == 3 ]]; then
                echo "No current verified artifact; Compile will trigger $lane" >&2
                params=$(jq -c --arg lane "$lane" '.+{regression_targets:$lane,is_debug:"false"}' <<< "$params")
                type=Doris_Compile; TC_REQUEST_LANE=compile
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
        TC_REQUEST_LANE=performance_compile
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
    TC_REQUEST_LANE=performance
    teamcity_current_enqueue Doris_Performance "$params" "$count" "$dependencies"
}

get_commit_id_of_build() {
    # 获取某个build的commit id
    if [[ -z "$1" ]]; then return 1; fi
    local build_id="$1"
    local commit_id
    local ret
    set +x
    if ret=$(
        teamcity_request -sS --fail -X GET \
            -H "Content-Type:text/plain" \
            -H "Accept: application/json" \
            "${TEAMCITY_URL}/app/rest/builds/id:${build_id}"
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
    set +x
    if ret=$(
        teamcity_request -sS --fail -X GET \
            -H "Content-Type:text/plain" \
            -H "Accept: application/json" \
            "${TEAMCITY_URL}/app/rest/builds?locator=buildType:${PIPELINE},branch:pull/${PULL_REQUEST_NUM},running:true"
    ); then
        set +x
        running_builds_list=$(echo "${ret}" | jq -r '.build[]?.id')
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
    set +x
    if ret=$(
        teamcity_request -sS --fail -X GET \
            -H "Content-Type:text/plain" \
            -H "Accept: application/json" \
            "${TEAMCITY_URL}/app/rest/buildQueue?locator=buildType:${PIPELINE}"
    ); then
        set +x
        queue_builds_list=$(echo "${ret}" | jq ".build[]? | select(.branchName == \"pull/${PULL_REQUEST_NUM}\") | .id")
        echo "${queue_builds_list}"
    else
        set +x
        echo "WARNING: failed to get queue build for PR ${PULL_REQUEST_NUM} of pipeline ${PIPELINE}" && return 1
    fi
}
# get_queue_build_of_pr "$1" "$2"

get_active_builds_of_revision() {
    # Return active build IDs for the same PR, pipeline, and revision.
    # Return 0 when a duplicate exists, 1 when none exists, and 2 when lookup fails.
    local PULL_REQUEST_NUM="${PULL_REQUEST_NUM:-$1}"
    local COMMENT_TRIGGER_TYPE="${COMMENT_TRIGGER_TYPE:-$2}"
    local COMMIT_ID_FROM_TRIGGER="${COMMIT_ID_FROM_TRIGGER:-$3}"
    if [[ -z "${PULL_REQUEST_NUM}" ||
        -z "${COMMENT_TRIGGER_TYPE}" ||
        -z "${COMMIT_ID_FROM_TRIGGER}" ]]; then
        echo "Usage: get_active_builds_of_revision PULL_REQUEST_NUM COMMENT_TRIGGER_TYPE COMMIT_ID_FROM_TRIGGER" >&2
        return 2
    fi

    local queue_build_ids
    local running_build_ids
    if ! queue_build_ids=$(get_queue_build_of_pr "${PULL_REQUEST_NUM}" "${COMMENT_TRIGGER_TYPE}"); then
        echo "WARNING: failed to get queued builds for duplicate check" >&2
        return 2
    fi
    if ! running_build_ids=$(get_running_build_of_pr "${PULL_REQUEST_NUM}" "${COMMENT_TRIGGER_TYPE}"); then
        echo "WARNING: failed to get running builds for duplicate check" >&2
        return 2
    fi

    local build_id
    local build_revision
    local duplicate_build_ids=()
    for build_id in ${queue_build_ids} ${running_build_ids}; do
        if ! build_revision=$(get_commit_id_of_build "${build_id}"); then
            echo "WARNING: failed to get revision of active build ${build_id}" >&2
            return 2
        fi
        if [[ "${build_revision}" == "${COMMIT_ID_FROM_TRIGGER}" ]]; then
            duplicate_build_ids+=("${build_id}")
        fi
    done

    if [[ ${#duplicate_build_ids[@]} -gt 0 ]]; then
        printf '%s\n' "${duplicate_build_ids[@]}"
        return 0
    fi
    return 1
}
# get_active_builds_of_revision "$1" "$2" "$3"

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
        set +x
        if teamcity_request -sS --fail --output /dev/null -X POST \
            -H "Content-Type:application/json" \
            -H "Accept: application/json" \
            "${TEAMCITY_URL}/app/rest/builds/id:${id}" \
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
        set +x
        if teamcity_request -sS --fail --output /dev/null -X POST \
            -H "Content-Type:application/json" \
            -H "Accept: application/json" \
            "${TEAMCITY_URL}/app/rest/buildQueue/id:${id}" \
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

    if [[ ${TC_CURRENT:-false} == true && -z ${conment_to_context[$COMMENT_TRIGGER_TYPE]:-} ]]; then
        echo "INFO: No separate current check for ${COMMENT_TRIGGER_TYPE}"
        return 0
    fi
    local state="${TC_BUILD_STATE:-success}" # 可选值 success failure pending
    local payload="{\"state\":\"${state}\",\"target_url\":\"\",\"description\":\"Skip teamCity build\",\"context\":\"${conment_to_context[${COMMENT_TRIGGER_TYPE}]}\"}"
    set +x
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
    local PULL_REQUEST_TARGET_BRANCH="${PULL_REQUEST_TARGET_BRANCH:-${TARGET_BRANCH:-}}"
    if [[ -z "${PULL_REQUEST_NUM}" || -z "${COMMIT_ID_FROM_TRIGGER}" || -z "${COMMENT_TRIGGER_TYPE}" ]]; then
        echo "Usage: add_build PULL_REQUEST_NUM COMMIT_ID_FROM_TRIGGER COMMENT_TRIGGER_TYPE [COMMENT_REPEAT_TIMES]"
        return 1
    fi
    if [[ ${TC_CURRENT:-false} == true ]]; then
        teamcity_current_trigger "$COMMENT_TRIGGER_TYPE" "${COMMENT_REPEAT_TIMES:-1}"
        return $?
    fi
    local PIPELINE="${comment_to_pipeline[${COMMENT_TRIGGER_TYPE}]}"
    set +x
    if teamcity_request -sS --fail --output /dev/null -X POST \
        -H "Content-Type:text/plain" \
        -H "Accept: application/json" \
        "${TEAMCITY_URL}/httpAuth/action.html?add2Queue=${PIPELINE}&branchName=pull/${PULL_REQUEST_NUM}&name=env.pr_num_from_trigger&value=${PULL_REQUEST_NUM:-}&name=env.commit_id_from_trigger&value=${COMMIT_ID_FROM_TRIGGER:-}&name=env.repeat_times_from_trigger&value=${COMMENT_REPEAT_TIMES:-1}"; then
        set +x
        echo "INFO: Add new build to PIPELINE ${PIPELINE} of PR ${PULL_REQUEST_NUM} with COMMENT_REPEAT_TIMES ${COMMENT_REPEAT_TIMES:-1}"
    else
        set +x
        return 1
    fi
}
# trigger_build "$1" "$2" "$3" "$4"

trigger_or_skip_build() {
    set +x
    # 根据相关文件是否修改，来触发or跳过跑流水线
    local FILE_CHANGED="$1" # 默认为"true"
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

    teamcity_configure "$PULL_REQUEST_TARGET_BRANCH"
    if [[ $TC_CURRENT == true ]]; then
        # An explicit current comment always runs; preserve all active builds.
        trigger_build "$PULL_REQUEST_NUM" "$COMMIT_ID_FROM_TRIGGER" "$COMMENT_TRIGGER_TYPE" "${COMMENT_REPEAT_TIMES:-1}"
        return $?
    fi

    # 有些分支不需要某些跑流水线，在targetBranch_to_pipelines中的分支，需要判断
    if [[ "${!targetBranch_to_pipelines[*]}" =~ ${PULL_REQUEST_TARGET_BRANCH} ]]; then
        if [[ "${targetBranch_to_pipelines[${PULL_REQUEST_TARGET_BRANCH}]}" =~ ${COMMENT_TRIGGER_TYPE} ]]; then
            echo "INFO: pr ${PULL_REQUEST_NUM} to branch ${PULL_REQUEST_TARGET_BRANCH} need to run ${COMMENT_TRIGGER_TYPE}"
        else
            echo "INFO: pr ${PULL_REQUEST_NUM} to branch ${PULL_REQUEST_TARGET_BRANCH} not need to run ${COMMENT_TRIGGER_TYPE}"
            skip_build "${COMMIT_ID_FROM_TRIGGER}" "${COMMENT_TRIGGER_TYPE}"
            return 0
        fi
    fi

    if [[ "${FILE_CHANGED:-"true"}" == "true" ]]; then
        local duplicate_build_ids
        local duplicate_lookup_status=0
        duplicate_build_ids=$(
            get_active_builds_of_revision \
                "${PULL_REQUEST_NUM}" \
                "${COMMENT_TRIGGER_TYPE}" \
                "${COMMIT_ID_FROM_TRIGGER}"
        ) || duplicate_lookup_status=$?
        if [[ ${duplicate_lookup_status} -eq 0 ]]; then
            echo "INFO: active build(s) ${duplicate_build_ids//$'\n'/,} already exist for PR ${PULL_REQUEST_NUM}, pipeline ${COMMENT_TRIGGER_TYPE}, revision ${COMMIT_ID_FROM_TRIGGER}; skip duplicate trigger"
            return 0
        elif [[ ${duplicate_lookup_status} -ne 1 ]]; then
            echo "WARNING: duplicate lookup failed for PR ${PULL_REQUEST_NUM}, pipeline ${COMMENT_TRIGGER_TYPE}; continue with the existing trigger flow"
        fi

        cancel_running_build "${PULL_REQUEST_NUM}" "${COMMENT_TRIGGER_TYPE}"
        cancel_queue_build "${PULL_REQUEST_NUM}" "${COMMENT_TRIGGER_TYPE}"
        trigger_build "${PULL_REQUEST_NUM}" "${COMMIT_ID_FROM_TRIGGER}" "${COMMENT_TRIGGER_TYPE}" "${COMMENT_REPEAT_TIMES}"
    else
        cancel_running_build "${PULL_REQUEST_NUM}" "${COMMENT_TRIGGER_TYPE}"
        cancel_queue_build "${PULL_REQUEST_NUM}" "${COMMENT_TRIGGER_TYPE}"
        skip_build "${COMMIT_ID_FROM_TRIGGER}" "${COMMENT_TRIGGER_TYPE}"
        if [[ ${COMMENT_TRIGGER_TYPE} == "compile" ]]; then
            # skip compile 的时候，也把 p0 p1 external cloud_p0 cloud_p1 vault_p0 nonConcurrent check_coverage 都 skip 了
            skip_build "${COMMIT_ID_FROM_TRIGGER}" "p0"
            skip_build "${COMMIT_ID_FROM_TRIGGER}" "p1"
            skip_build "${COMMIT_ID_FROM_TRIGGER}" "external"
            skip_build "${COMMIT_ID_FROM_TRIGGER}" "cloud_p0"
            skip_build "${COMMIT_ID_FROM_TRIGGER}" "cloud_p1"
            skip_build "${COMMIT_ID_FROM_TRIGGER}" "vault_p0"
            skip_build "${COMMIT_ID_FROM_TRIGGER}" "nonConcurrent"
            skip_build "${COMMIT_ID_FROM_TRIGGER}" "check_coverage"
        elif [[ ${COMMENT_TRIGGER_TYPE} == "beut" ]]; then
            # skip beut 的时候，也把 check_coverage skip 了
            skip_build "${COMMIT_ID_FROM_TRIGGER}" "check_coverage"
        fi
    fi
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

teamcity_configure "${TARGET_BRANCH:-${PULL_REQUEST_TARGET_BRANCH:-}}"
