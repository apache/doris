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

##############################################################
# Build profiling helper for build.sh
# Usage:
#   build_profile.sh collect <state_file> <build_args>
#   build_profile.sh record  <state_file> <exit_code>
#
# Controlled by DORIS_BUILD_PROFILE=1 in custom_env.sh.
# All errors are non-fatal — profiling failures never affect
# the build itself.
##############################################################

set +e

DORIS_HOME="${DORIS_HOME:-$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)}"
LOG_FILE="${DORIS_HOME}/.build_profile.jsonl"

# Auto-detect base branch: find the closest remote main branch to HEAD
detect_base_branch() {
    local min_count=999999 best="unknown"
    while read -r ref; do
        local count
        count=$(git rev-list --count HEAD ^"${ref}" 2>/dev/null) || continue
        if [[ "${count}" -lt "${min_count}" ]]; then
            min_count="${count}"
            best="${ref#origin/}"
        fi
    done < <(git branch -r | grep -oE 'origin/(master|branch-[0-9.]+|branch-selectdb-doris-[0-9.]+)$')
    echo "${best}"
}

# Collect modified files: git diff + untracked, filtered by mtime > last_build_time
collect_files() {
    local last_time="$1"
    while IFS= read -r f; do
        [[ -z "$f" || ! -f "$f" ]] && continue
        local mtime
        if [[ "$(uname -s)" == "Darwin" ]]; then
            mtime=$(stat -f %m "$f")
        else
            mtime=$(stat -c %Y "$f")
        fi
        [[ "$mtime" -gt "$last_time" ]] && echo "$f"
    done < <(git diff --name-only 2>/dev/null; git ls-files --others --exclude-standard 2>/dev/null)
}

# Read last_build_time from log (0 for first build)
get_last_build_time() {
    local last_time=0
    if [[ -f "${LOG_FILE}" ]]; then
        last_time=$(tail -1 "${LOG_FILE}" | python3 -c \
            "import sys,json; print(json.load(sys.stdin).get('start_time',0))" 2>/dev/null || echo 0)
    fi
    echo "${last_time}"
}

cmd_collect() {
    local state_file="$1"
    local build_args="$2"

    if ! command -v python3 &>/dev/null; then
        echo "WARNING: python3 not found, build profiling disabled"
        return 1
    fi

    local start_time
    start_time=$(date +%s)
    local user
    user=$(whoami)
    local build_dir
    build_dir=$(pwd)
    local commit
    commit=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
    local base_branch
    base_branch=$(detect_base_branch)
    local last_time
    last_time=$(get_last_build_time)
    local files
    files=$(collect_files "${last_time}")

    # Write state to temp file
    cat > "${state_file}" <<EOF
_BP_START=${start_time}
_BP_USER=${user}
_BP_DIR=${build_dir}
_BP_COMMIT=${commit}
_BP_BASE_BRANCH=${base_branch}
_BP_ARGS=${build_args}
EOF
    # Write files as separate lines after a marker
    echo "===FILES==="  >> "${state_file}"
    echo "${files}"     >> "${state_file}"
}

cmd_record() {
    local state_file="$1"
    local exit_code="$2"

    if [[ ! -f "${state_file}" ]]; then
        echo "WARNING: build profile state file not found, skipping"
        return 1
    fi

    # Read state (only lines matching _BP_* pattern)
    local _BP_START _BP_USER _BP_DIR _BP_COMMIT _BP_BASE_BRANCH _BP_ARGS
    while IFS='=' read -r key value; do
        [[ "$key" == "===FILES===" ]] && break
        [[ "$key" == _BP_* ]] || continue
        eval "${key}='${value}'"
    done < "${state_file}"

    # Read files (everything after ===FILES=== marker)
    local files
    files=$(sed -n '/^===FILES===$/,$ p' "${state_file}" | tail -n +2)

    local end_time
    end_time=$(date +%s)
    local load_avg
    load_avg=$(uptime | grep -oE 'load average[s]?: .*' | sed 's/load average[s]\{0,1\}: //')

    # Write record via python3 (env vars + stdin for safety)
    echo "${files}" | \
    _BP_USER="${_BP_USER}" \
    _BP_DIR="${_BP_DIR}" \
    _BP_BASE_BRANCH="${_BP_BASE_BRANCH}" \
    _BP_COMMIT="${_BP_COMMIT}" \
    _BP_ARGS="${_BP_ARGS}" \
    _BP_START="${_BP_START}" \
    _BP_EXIT_CODE="${exit_code}" \
    _BP_END_TIME="${end_time}" \
    _BP_LOAD_AVG="${load_avg}" \
    python3 -c "
import json, os, sys

files = [line.strip() for line in sys.stdin if line.strip()]
start = int(os.environ['_BP_START'])
end = int(os.environ['_BP_END_TIME'])
record = {
    'user': os.environ['_BP_USER'],
    'build_dir': os.environ['_BP_DIR'],
    'base_branch': os.environ['_BP_BASE_BRANCH'],
    'commit': os.environ['_BP_COMMIT'],
    'args': os.environ.get('_BP_ARGS', ''),
    'files': files,
    'start_time': start,
    'end_time': end,
    'duration_sec': end - start,
    'exit_code': int(os.environ['_BP_EXIT_CODE']),
    'load_avg': os.environ['_BP_LOAD_AVG'],
}
print(json.dumps(record))
" >> "${LOG_FILE}"

    # Clean up state file
    rm -f "${state_file}"
}

case "$1" in
    collect)
        cmd_collect "$2" "$3"
        ;;
    record)
        cmd_record "$2" "$3"
        ;;
    *)
        echo "Usage: $0 {collect|record} <state_file> <args|exit_code>"
        exit 1
        ;;
esac
