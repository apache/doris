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

stage_timer__now() {
    date +%s
}

stage_timer__format_seconds() {
    local total_seconds="${1:-0}"
    local hours=$((total_seconds / 3600))
    local minutes=$(((total_seconds % 3600) / 60))
    local seconds=$((total_seconds % 60))
    printf '%02d:%02d:%02d' "${hours}" "${minutes}" "${seconds}"
}

stage_timer__extract_exit_trap() {
    local trap_desc
    trap_desc="$(trap -p EXIT)"
    if [[ "${trap_desc}" =~ ^trap\ --\ \'(.*)\'\ EXIT$ ]]; then
        printf '%s' "${BASH_REMATCH[1]}"
    fi
}

stage_timer__record_current_stage() {
    local end_at="$1"
    if [[ -z "${STAGE_TIMER_CURRENT_STAGE:-}" ]]; then
        return 0
    fi
    STAGE_TIMER_STAGE_NAMES+=("${STAGE_TIMER_CURRENT_STAGE}")
    STAGE_TIMER_STAGE_SECONDS+=("$((end_at - STAGE_TIMER_CURRENT_STAGE_STARTED_AT))")
    STAGE_TIMER_CURRENT_STAGE=''
    STAGE_TIMER_CURRENT_STAGE_STARTED_AT=''
}

stage_timer__print_summary() {
    local exit_code="$1"
    local finished_at="$2"
    local total_seconds="$((finished_at - STAGE_TIMER_STARTED_AT))"
    local index

    echo "========== ${STAGE_TIMER_PIPELINE_NAME} 阶段耗时汇总 =========="
    for index in "${!STAGE_TIMER_STAGE_NAMES[@]}"; do
        printf '[stage-timer] %s: %s (%ss)\n' \
            "${STAGE_TIMER_STAGE_NAMES[$index]}" \
            "$(stage_timer__format_seconds "${STAGE_TIMER_STAGE_SECONDS[$index]}")" \
            "${STAGE_TIMER_STAGE_SECONDS[$index]}"
    done
    printf '[stage-timer] 总耗时: %s (%ss)\n' \
        "$(stage_timer__format_seconds "${total_seconds}")" \
        "${total_seconds}"
    printf '[stage-timer] 退出码: %s\n' "${exit_code}"
    echo "=================================================="
}

stage_timer_finish() {
    local exit_code="${1:-$?}"
    local finished_at
    if [[ "${STAGE_TIMER_INITIALIZED:-false}" != "true" ]]; then
        return 0
    fi
    if [[ "${STAGE_TIMER_SUMMARY_PRINTED:-false}" == "true" ]]; then
        return 0
    fi
    finished_at="$(stage_timer__now)"
    stage_timer__record_current_stage "${finished_at}"
    stage_timer__print_summary "${exit_code}" "${finished_at}"
    STAGE_TIMER_SUMMARY_PRINTED=true
}

stage_timer__handle_exit() {
    local exit_code="${1:-0}"
    stage_timer_finish "${exit_code}"
    if [[ -n "${STAGE_TIMER_PREVIOUS_EXIT_TRAP:-}" ]] &&
        [[ "${STAGE_TIMER_PREVIOUS_EXIT_TRAP}" != *"stage_timer__handle_exit"* ]]; then
        eval "${STAGE_TIMER_PREVIOUS_EXIT_TRAP}"
    fi
}

stage_timer_init() {
    local pipeline_name="${1:-}"
    if [[ -z "${pipeline_name}" ]]; then
        echo "ERROR: stage_timer_init need pipeline name"
        return 1
    fi

    STAGE_TIMER_PIPELINE_NAME="${pipeline_name}"
    STAGE_TIMER_STARTED_AT="$(stage_timer__now)"
    STAGE_TIMER_CURRENT_STAGE=''
    STAGE_TIMER_CURRENT_STAGE_STARTED_AT=''
    STAGE_TIMER_SUMMARY_PRINTED=false
    STAGE_TIMER_INITIALIZED=true
    STAGE_TIMER_STAGE_NAMES=()
    STAGE_TIMER_STAGE_SECONDS=()
    STAGE_TIMER_PREVIOUS_EXIT_TRAP="$(stage_timer__extract_exit_trap)"
    trap 'stage_timer__handle_exit "$?"' EXIT
}

stage_timer_enter() {
    local stage_name="${1:-}"
    local now
    if [[ "${STAGE_TIMER_INITIALIZED:-false}" != "true" ]]; then
        echo "ERROR: stage_timer_enter called before stage_timer_init"
        return 1
    fi
    if [[ -z "${stage_name}" ]]; then
        echo "ERROR: stage_timer_enter need stage name"
        return 1
    fi

    now="$(stage_timer__now)"
    stage_timer__record_current_stage "${now}"
    STAGE_TIMER_CURRENT_STAGE="${stage_name}"
    STAGE_TIMER_CURRENT_STAGE_STARTED_AT="${now}"
}
