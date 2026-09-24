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

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$(mktemp -d)"
trap 'rm -rf "${TEST_DIR}"' EXIT

cat >"${TEST_DIR}/timeout" <<'FAKE_TIMEOUT'
#!/usr/bin/env bash
shift
exec "$@"
FAKE_TIMEOUT

cat >"${TEST_DIR}/flink" <<'FAKE_FLINK'
#!/usr/bin/env bash
job_id=0123456789abcdef0123456789abcdef
case "$1" in
    list)
        if [[ "${FAKE_FLINK_MODE}" == "list-failure" ]]; then
            echo "injected list failure" >&2
            exit 41
        fi
        if [[ "${FAKE_FLINK_MODE}" != "success" || ! -f "${FAKE_FLINK_STATE}/cancelled" ]]; then
            echo "${job_id} : tiering (RUNNING)"
        fi
        ;;
    cancel)
        if [[ "${FAKE_FLINK_MODE}" == "cancel-failure" ]]; then
            echo "injected cancel failure" >&2
            exit 42
        fi
        touch "${FAKE_FLINK_STATE}/cancelled"
        ;;
    *)
        exit 43
        ;;
esac
FAKE_FLINK

chmod +x "${TEST_DIR}/timeout" "${TEST_DIR}/flink"
PATH="${TEST_DIR}:${PATH}"
export PATH
FLINK_BIN="${TEST_DIR}/flink"
FAKE_FLINK_STATE="${TEST_DIR}/state"
export FAKE_FLINK_STATE
mkdir -p "${FAKE_FLINK_STATE}"

INIT_DEADLINE_EPOCH=$(($(date +%s) + 60))
TIERING_CANCEL_WAIT_SECONDS=1

bounded_command_timeout() {
    printf '5\n'
}

stage_deadline() {
    printf '%s\n' "$(($(date +%s) + $1))"
}

remaining_until() {
    (($(date +%s) < $1))
}

sleep_before() {
    sleep 0.05
}

# shellcheck source=fluss-job-control.sh
source "${SCRIPT_DIR}/fluss-job-control.sh"

run_fixture_gate() {
    local marker="$1"
    cancel_all_jobs || return 1
    touch "${marker}"
}

assert_gate_fails() {
    local mode="$1"
    local marker="${TEST_DIR}/SUCCESS-${mode}"
    rm -f "${FAKE_FLINK_STATE}/cancelled" "${marker}"
    export FAKE_FLINK_MODE="${mode}"
    if run_fixture_gate "${marker}" >"${TEST_DIR}/${mode}.log" 2>&1; then
        echo "expected fixture gate to fail in ${mode} mode" >&2
        exit 1
    fi
    if [[ -e "${marker}" ]]; then
        echo "fixture published SUCCESS after ${mode}" >&2
        exit 1
    fi
}

assert_gate_fails list-failure
assert_gate_fails cancel-failure
assert_gate_fails never-terminal

success_marker="${TEST_DIR}/SUCCESS-success"
rm -f "${FAKE_FLINK_STATE}/cancelled" "${success_marker}"
export FAKE_FLINK_MODE=success
run_fixture_gate "${success_marker}" >"${TEST_DIR}/success.log" 2>&1
test -f "${success_marker}"

echo "fluss job-control tests passed"
