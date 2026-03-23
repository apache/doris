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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${ROOT}/.." && pwd)"
BENCH_DIR="${BENCH_DIR:-${REPO_ROOT}/output/hive-fullflow-bench}"
mkdir -p "${BENCH_DIR}"

run_case() {
    local label="$1"
    shift
    local log_file="${BENCH_DIR}/${label}.log"
    local start_ts end_ts elapsed

    echo "INFO: running ${label}"
    start_ts="$(date +%s)"
    if "$@" >"${log_file}" 2>&1; then
        end_ts="$(date +%s)"
        elapsed="$((end_ts - start_ts))"
        echo "${label},success,${elapsed},${log_file}"
        return 0
    fi

    end_ts="$(date +%s)"
    elapsed="$((end_ts - start_ts))"
    echo "${label},failure,${elapsed},${log_file}"
    return 1
}

main() {
    local overall_rc=0
    {
        echo "label,status,seconds,log_file"
        run_case \
            "current_hive3_fullflow" \
            env \
                POC_WORKDIR="${REPO_ROOT}/output/hive3-current-poc" \
                POC_TMPDIR="${REPO_ROOT}/output/hive3-current-poc/tmp" \
                HEALTH_TIMEOUT_SECONDS="${HEALTH_TIMEOUT_SECONDS:-180}" \
                bash "${ROOT}/docker-compose/hive/run-hive3-current-poc.sh" || overall_rc=1
        run_case \
            "tez_hive3_fullflow" \
            env \
                POC_WORKDIR="${REPO_ROOT}/output/hive3-tez-poc" \
                POC_TMPDIR="${REPO_ROOT}/output/hive3-tez-poc/tmp" \
                SERVICE_TIMEOUT_SECONDS="${SERVICE_TIMEOUT_SECONDS:-180}" \
                HEALTH_TIMEOUT_SECONDS="${HEALTH_TIMEOUT_SECONDS:-240}" \
                bash "${ROOT}/docker-compose/kerberos/run-hive3-tez-poc.sh" || overall_rc=1
    } | tee "${BENCH_DIR}/summary.csv"

    exit "${overall_rc}"
}

main "$@"
