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

extract_tez_startup_block() {
    awk '
        /^if \[\[ "\$\{ENABLE_HIVE3_TEZ_RUNTIME:-false\}" == "true" \]\]; then$/ {capture=1}
        capture {print}
        capture && /^fi$/ {exit}
    ' "${SCRIPT_PATH}"
}

main() {
    local tez_block
    tez_block="$(extract_tez_startup_block)"
    if [[ -z "${tez_block}" ]]; then
        echo "ERROR: failed to extract Hive3 Tez startup block from ${SCRIPT_PATH}" >&2
        exit 1
    fi

    local test_script
    test_script="$(mktemp)"
    local test_script_path="${test_script}"
    trap 'rm -f "'"${test_script_path}"'"' EXIT

    cat >"${test_script}" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

wait_calls=0
rm_pid_value=""
nm_pid_value=""

wait_for_yarn_services() {
    wait_calls=$((wait_calls + 1))
    rm_pid_value="$1"
    nm_pid_value="$2"
}

mkdir() { return 0; }
cp() { return 0; }
hdfs() { return 0; }
nohup() { "$@"; }
yarn() { sleep 0.1; }

export ENABLE_HIVE3_TEZ_RUNTIME=true
export YARN_RM_PORT=8032
export YARN_NM_WEBAPP_PORT=8042
EOF

    printf '\n%s\n' "${tez_block}" >>"${test_script}"

    cat >>"${test_script}" <<'EOF'
[[ "${wait_calls}" -eq 1 ]]
[[ -n "${rm_pid_value}" ]]
[[ -n "${nm_pid_value}" ]]
EOF

    bash "${test_script}"
}

main "$@"
