#!/usr/bin/env bash
#
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
#
# Run Claude Code in fresh one-task sessions until HANDOFF.md reports no work.
#
# Usage:
#   tools/loop-engineer.sh /path/to/HANDOFF.md
#   tools/loop-engineer.sh ./HANDOFF.md --workdir /path/to/repo --max-rounds 10
#
# Each round starts a new Claude Code print-mode session. The only cross-round
# state is the workspace itself, HANDOFF.md, and this script's log directory.

set -euo pipefail
shopt -s expand_aliases

alias claude='https_proxy=http://127.0.0.1:7890 http_proxy=http://127.0.0.1:7890 all_proxy=socks5://127.0.0.1:7890 claude --dangerously-skip-permissions --effort max'

SCRIPT_NAME="$(basename "$0")"
HANDOFF_PATH=""
WORKDIR="$(pwd)"
STATE_DIR=""
MAX_ROUNDS=0
TOKEN_RETRY_SECONDS="${LOOP_ENGINEER_TOKEN_RETRY_SECONDS:-600}"
CLAUDE_EXTRA_ARGS=()
LOCK_DIR=""

usage() {
    cat <<EOF
Usage:
  ${SCRIPT_NAME} HANDOFF.md [options]

Options:
  --workdir DIR              Workspace where Claude Code should run. Defaults to current directory.
  --state-dir DIR            Directory for logs and state. Defaults to WORKDIR/.loop-engineer.
  --max-rounds N             Stop after N successful task rounds. 0 means unlimited.
  --token-retry-seconds N    Sleep interval after a token/rate limit. Defaults to ${TOKEN_RETRY_SECONDS}.
  --claude-arg ARG           Extra argument passed to claude. May be repeated.
  -h, --help                 Show this help.

Environment:
  LOOP_ENGINEER_TOKEN_RETRY_SECONDS   Default retry interval for token/rate limits.

Claude alias used by this script:
  alias claude='https_proxy=http://127.0.0.1:7890 http_proxy=http://127.0.0.1:7890 all_proxy=socks5://127.0.0.1:7890 claude --dangerously-skip-permissions --effort max'

Claude must finish each successful round with exactly one status line:
  LOOP_ENGINEER_STATUS: CONTINUE
  LOOP_ENGINEER_STATUS: DONE
  LOOP_ENGINEER_STATUS: NEEDS_USER
EOF
}

log() {
    local level="$1"
    local message="$2"
    printf '[%s] %s %s\n' "${level}" "$(date '+%Y-%m-%d %H:%M:%S')" "${message}"
}

die() {
    log "ERROR" "$1" >&2
    exit "${2:-1}"
}

parse_args() {
    while [[ "$#" -gt 0 ]]; do
        case "$1" in
        -h | --help)
            usage
            exit 0
            ;;
        --workdir)
            [[ "$#" -ge 2 ]] || die "--workdir requires a value"
            WORKDIR="$2"
            shift 2
            ;;
        --state-dir)
            [[ "$#" -ge 2 ]] || die "--state-dir requires a value"
            STATE_DIR="$2"
            shift 2
            ;;
        --max-rounds)
            [[ "$#" -ge 2 ]] || die "--max-rounds requires a value"
            MAX_ROUNDS="$2"
            shift 2
            ;;
        --token-retry-seconds)
            [[ "$#" -ge 2 ]] || die "--token-retry-seconds requires a value"
            TOKEN_RETRY_SECONDS="$2"
            shift 2
            ;;
        --claude-arg)
            [[ "$#" -ge 2 ]] || die "--claude-arg requires a value"
            CLAUDE_EXTRA_ARGS+=("$2")
            shift 2
            ;;
        --)
            shift
            break
            ;;
        -*)
            die "unknown option: $1"
            ;;
        *)
            if [[ -n "${HANDOFF_PATH}" ]]; then
                die "only one HANDOFF.md path may be specified"
            fi
            HANDOFF_PATH="$1"
            shift
            ;;
        esac
    done

    [[ "$#" -eq 0 ]] || die "unexpected trailing arguments: $*"
    [[ -n "${HANDOFF_PATH}" ]] || die "HANDOFF.md path is required"
    [[ "${MAX_ROUNDS}" =~ ^[0-9]+$ ]] || die "--max-rounds must be a non-negative integer"
    [[ "${TOKEN_RETRY_SECONDS}" =~ ^[0-9]+$ ]] || die "--token-retry-seconds must be a non-negative integer"
}

abspath_from() {
    local base_dir="$1"
    local path="$2"
    if [[ "${path}" = /* ]]; then
        printf '%s\n' "${path}"
    else
        printf '%s/%s\n' "${base_dir}" "${path}"
    fi
}

write_prompt() {
    local prompt_file="$1"
    cat >"${prompt_file}" <<EOF
You are running inside an automated loop-engineer wrapper.

Fresh-session contract:
- Treat this as a brand-new Claude Code session. Do not rely on previous chat context.
- Use the workspace files and the HANDOFF document as the only durable context.
- Before making changes, inspect the current repository state as needed. If a prior loop was interrupted, reconcile partial work with the HANDOFF document.

Task contract:
- Read this HANDOFF document first: ${HANDOFF_PATH}
- Follow the HANDOFF document's instructions to identify the next unfinished task.
- Work on exactly one next unfinished task in this session.
- When that task is complete, update the HANDOFF document exactly as it instructs.
- If the HANDOFF document says every task is complete, do not make task changes.
- If a decision needs the human user's judgment and the HANDOFF document does not already answer it, stop immediately and ask for that judgment.

Final response contract:
- End your final response with exactly one of these lines:
  LOOP_ENGINEER_STATUS: CONTINUE
  LOOP_ENGINEER_STATUS: DONE
  LOOP_ENGINEER_STATUS: NEEDS_USER
- Use CONTINUE only after completing one task and updating the HANDOFF document.
- Use DONE only when the HANDOFF document has no remaining unfinished tasks.
- Use NEEDS_USER when human judgment is required before progress can continue.
EOF
}

detect_token_limit() {
    local log_file="$1"
    grep -Eiq \
        'usage limit|rate limit|token limit|quota|too many requests|429|limit[[:space:]]+(reached|exceeded)|exceeded[[:space:]].*limit|try again later|reset[[:space:]]+(at|in)' \
        "${log_file}"
}

extract_status() {
    local log_file="$1"
    sed -n 's/.*LOOP_ENGINEER_STATUS:[[:space:]]*\(CONTINUE\|DONE\|NEEDS_USER\).*/\1/p' "${log_file}" | tail -1
}

acquire_lock() {
    local lock_dir="$1"
    if ! mkdir "${lock_dir}" 2>/dev/null; then
        die "another loop-engineer process appears to be running: ${lock_dir}"
    fi
    LOCK_DIR="${lock_dir}"
    trap 'rm -rf "${LOCK_DIR}"' EXIT
}

run_round() {
    local round="$1"
    local prompt_file="$2"
    local log_file="$3"
    local status_code=0

    write_prompt "${prompt_file}"
    log "INFO" "round ${round}: starting fresh Claude Code session"

    set +e
    (
        cd "${WORKDIR}"
        claude -p --no-session-persistence --output-format text "${CLAUDE_EXTRA_ARGS[@]}" "$(cat "${prompt_file}")"
    ) >"${log_file}" 2>&1
    status_code="$?"
    set -e

    printf '%s\n' "${status_code}" >"${STATE_DIR}/last-exit-code"
    return "${status_code}"
}

main() {
    parse_args "$@"

    [[ -d "${WORKDIR}" ]] || die "workdir does not exist: ${WORKDIR}"
    WORKDIR="$(cd "${WORKDIR}" && pwd)"
    HANDOFF_PATH="$(abspath_from "${WORKDIR}" "${HANDOFF_PATH}")"
    STATE_DIR="${STATE_DIR:-${WORKDIR}/.loop-engineer}"
    STATE_DIR="$(abspath_from "${WORKDIR}" "${STATE_DIR}")"

    [[ -f "${HANDOFF_PATH}" ]] || die "HANDOFF.md does not exist: ${HANDOFF_PATH}"

    mkdir -p "${STATE_DIR}/logs"
    acquire_lock "${STATE_DIR}/lock"

    log "INFO" "workdir: ${WORKDIR}"
    log "INFO" "handoff: ${HANDOFF_PATH}"
    log "INFO" "state: ${STATE_DIR}"

    local completed_rounds=0
    local round=1
    if [[ -f "${STATE_DIR}/round" ]]; then
        round="$(<"${STATE_DIR}/round")"
        [[ "${round}" =~ ^[0-9]+$ ]] || round=1
    fi

    while true; do
        if [[ "${MAX_ROUNDS}" -gt 0 && "${completed_rounds}" -ge "${MAX_ROUNDS}" ]]; then
            log "INFO" "stopped after --max-rounds=${MAX_ROUNDS}"
            exit 0
        fi

        printf '%s\n' "${round}" >"${STATE_DIR}/round"

        local prompt_file="${STATE_DIR}/prompt-round-${round}.txt"
        local log_file="${STATE_DIR}/logs/round-${round}.log"
        local status_code=0
        if run_round "${round}" "${prompt_file}" "${log_file}"; then
            status_code=0
        else
            status_code="$?"
            if detect_token_limit "${log_file}"; then
                log "WARN" "round ${round}: token/rate limit detected; retrying after ${TOKEN_RETRY_SECONDS}s"
                log "WARN" "round ${round}: log: ${log_file}"
                sleep "${TOKEN_RETRY_SECONDS}"
                continue
            fi
            die "round ${round}: claude exited with ${status_code}; inspect ${log_file}" "${status_code}"
        fi

        local loop_status
        loop_status="$(extract_status "${log_file}")"
        printf '%s\n' "${loop_status}" >"${STATE_DIR}/last-status"

        case "${loop_status}" in
        CONTINUE)
            log "INFO" "round ${round}: task complete; continuing"
            completed_rounds="$((completed_rounds + 1))"
            round="$((round + 1))"
            ;;
        DONE)
            log "INFO" "round ${round}: all tasks complete"
            exit 0
            ;;
        NEEDS_USER)
            log "WARN" "round ${round}: human judgment required; inspect ${log_file}"
            exit 3
            ;;
        *)
            die "round ${round}: missing LOOP_ENGINEER_STATUS sentinel; inspect ${log_file}" 4
            ;;
        esac
    done
}

main "$@"
