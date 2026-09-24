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

# Lists running jobs without confusing "no jobs" with a failed Flink command.
# The caller supplies the shared-deadline helpers from run-init-sql.sh.
list_running_job_ids() {
    local command_timeout output status
    command_timeout="$(bounded_command_timeout "${INIT_DEADLINE_EPOCH}")" || return 1
    if output="$(timeout "${command_timeout}" "${FLINK_BIN}" list -r 2>&1)"; then
        printf '%s\n' "${output}" | grep -oE '[[:xdigit:]]{32}' || true
        return 0
    else
        status=$?
        echo "ERROR: failed to list running Flink jobs (status ${status})" >&2
        printf '%s\n' "${output}" >&2
        return "${status}"
    fi
}

# Cancels every running job, then proves the running set is empty before the
# caller writes rows that must remain in the Fluss log. A successful cancel RPC
# is only an acknowledgement; polling is what establishes the frozen boundary.
cancel_all_jobs() {
    local ids id command_timeout output status running deadline
    deadline="$(stage_deadline "${TIERING_CANCEL_WAIT_SECONDS}")"
    ids="$(list_running_job_ids)" || return 1
    for id in ${ids}; do
        echo "Cancelling flink job ${id}"
        command_timeout="$(bounded_command_timeout "${INIT_DEADLINE_EPOCH}")" || return 1
        if output="$(timeout "${command_timeout}" "${FLINK_BIN}" cancel "${id}" 2>&1)"; then
            :
        else
            status=$?
            echo "ERROR: failed to cancel Flink job ${id} (status ${status})" >&2
            printf '%s\n' "${output}" >&2
            return "${status}"
        fi
    done

    while :; do
        running="$(list_running_job_ids)" || return 1
        if [[ -z "${running}" ]]; then
            echo "No running Flink jobs remain"
            return 0
        fi
        if ! remaining_until "${deadline}" >/dev/null; then
            echo "ERROR: Flink jobs did not reach a terminal state before the cancellation deadline:${running//$'\n'/ }" >&2
            return 1
        fi
        sleep_before 2 "${deadline}" || return 1
    done
}
