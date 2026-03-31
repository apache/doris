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

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_PATH="${ROOT}/hive-metastore.sh"

extract_function() {
    local function_name="$1"
    awk -v function_name="${function_name}" '
        $0 ~ "^" function_name "\\(\\) \\{" {capture=1}
        capture {print}
        capture && /^}$/ {exit}
    ' "${SCRIPT_PATH}"
}

main() {
    local wait_for_hive_tez_runtime
    wait_for_hive_tez_runtime="$(extract_function wait_for_hive_tez_runtime)"
    if [[ -z "${wait_for_hive_tez_runtime}" ]]; then
        echo "ERROR: failed to extract wait_for_hive_tez_runtime from ${SCRIPT_PATH}" >&2
        exit 1
    fi

    local temp_root
    temp_root="$(mktemp -d)"
    trap 'rm -rf "'"${temp_root}"'"' EXIT

    local warmup_sql_path="${temp_root}/hive-tez-warmup.hql"
    local warmup_log="${temp_root}/hive-tez-warmup.log"
    local invocation_log="${temp_root}/hive-invocation.log"
    local test_script="${temp_root}/test.sh"

    cat >"${test_script}" <<EOF
#!/usr/bin/env bash
set -euo pipefail

${wait_for_hive_tez_runtime}

sleep() { return 0; }
tail() { return 0; }
yarn() { return 0; }
hive() {
    printf '%s\n' "\$*" >"${invocation_log}"
}

export ENABLE_HIVE3_TEZ_RUNTIME=true
export HIVE_TEZ_WARMUP_SQL_PATH="${warmup_sql_path}"
export HIVE_TEZ_WARMUP_LOG_PATH="${warmup_log}"

wait_for_hive_tez_runtime
EOF
    chmod +x "${test_script}"

    bash "${test_script}"

    grep -Fx -- "-f ${warmup_sql_path}" "${invocation_log}"
    grep -Fx -- 'CREATE DATABASE IF NOT EXISTS doris_tez_warmup;' "${warmup_sql_path}"
    grep -Fx -- 'INSERT INTO TABLE doris_tez_warmup.tez_runtime_probe VALUES (1);' "${warmup_sql_path}"
    grep -Fx -- 'SELECT COUNT(*) FROM doris_tez_warmup.tez_runtime_probe;' "${warmup_sql_path}"
}

main "$@"
