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

if [[ -z "${teamcity_build_checkoutDir:-}" ]]; then
    echo "ERROR: env teamcity_build_checkoutDir not set"
    return 1 2>/dev/null || exit 1
fi

# shellcheck source=/dev/null
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/stage-timer.sh

external_regression_stage_timer__extract_debug_trap() {
    local trap_desc
    trap_desc="$(trap -p DEBUG)"
    if [[ "${trap_desc}" =~ ^trap\ --\ \'(.*)\'\ DEBUG$ ]]; then
        printf '%s' "${BASH_REMATCH[1]}"
    fi
}

external_regression_stage_timer__enter_if_needed() {
    local stage_name="${1:-}"
    if [[ -z "${stage_name}" ]]; then
        return 1
    fi
    if [[ "${STAGE_TIMER_CURRENT_STAGE:-}" == "${stage_name}" ]]; then
        return 0
    fi
    stage_timer_enter "${stage_name}"
}

external_regression_stage_timer__handle_command() {
    local current_command="${1:-}"
    if [[ -z "${current_command}" ]]; then
        return 0
    fi

    case "${current_command}" in
        *"bash deploy_cluster.sh "*)
            external_regression_stage_timer__enter_if_needed "启动 Doris"
            ;;
        *"START EXTERNAL DOCKER"* | *"run-thirdparties-docker.sh "*)
            if [[ "${current_command}" != *"--stop"* ]]; then
                external_regression_stage_timer__enter_if_needed "启动依赖"
            fi
            ;;
        *"./run-regression-test.sh --teamcity --clean --run"*)
            external_regression_stage_timer__enter_if_needed "执行 Case"
            ;;
        *"COLLECT DOCKER LOGS"* | *"collect_docker_logs "*)
            external_regression_stage_timer__enter_if_needed "收尾归档"
            ;;
    esac
}

external_regression_stage_timer__run_previous_debug_trap() {
    if [[ -n "${EXTERNAL_REGRESSION_STAGE_TIMER_PREVIOUS_DEBUG_TRAP:-}" ]] &&
        [[ "${EXTERNAL_REGRESSION_STAGE_TIMER_PREVIOUS_DEBUG_TRAP}" != *"external_regression_stage_timer__debug_hook"* ]]; then
        eval "${EXTERNAL_REGRESSION_STAGE_TIMER_PREVIOUS_DEBUG_TRAP}"
    fi
}

external_regression_stage_timer__debug_hook() {
    local current_command="${1:-$BASH_COMMAND}"
    if [[ "${EXTERNAL_REGRESSION_STAGE_TIMER_IN_DEBUG_HOOK:-false}" == "true" ]]; then
        return 0
    fi

    EXTERNAL_REGRESSION_STAGE_TIMER_IN_DEBUG_HOOK=true
    external_regression_stage_timer__handle_command "${current_command}"
    external_regression_stage_timer__run_previous_debug_trap
    EXTERNAL_REGRESSION_STAGE_TIMER_IN_DEBUG_HOOK=false
}

external_regression_stage_timer_init() {
    stage_timer_init "external regression"
    stage_timer_enter "前置准备"
}

external_regression_stage_timer_enter_start_doris() {
    stage_timer_enter "启动 Doris"
}

external_regression_stage_timer_enter_start_dependencies() {
    stage_timer_enter "启动依赖"
}

external_regression_stage_timer_enter_run_cases() {
    stage_timer_enter "执行 Case"
}

external_regression_stage_timer_enter_cleanup() {
    stage_timer_enter "收尾归档"
}

external_regression_stage_timer_enable_auto_hooks() {
    if [[ "${EXTERNAL_REGRESSION_STAGE_TIMER_AUTO_HOOK_ENABLED:-false}" == "true" ]]; then
        return 0
    fi

    external_regression_stage_timer_init
    EXTERNAL_REGRESSION_STAGE_TIMER_PREVIOUS_DEBUG_TRAP="$(external_regression_stage_timer__extract_debug_trap)"
    EXTERNAL_REGRESSION_STAGE_TIMER_AUTO_HOOK_ENABLED=true
    trap 'external_regression_stage_timer__debug_hook "$BASH_COMMAND"' DEBUG
}
