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
HIVE_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
THIRDPARTY_ROOT="$(cd "${HIVE_ROOT}/../.." && pwd)"
RUN_THIRDPARTIES_PATH="${THIRDPARTY_ROOT}/run-thirdparties-docker.sh"
ENV_PATH="${HIVE_ROOT}/hadoop-hive-3x.env.tpl"

extract_function() {
    local function_name="$1"
    awk -v function_name="${function_name}" '
        $0 ~ "^" function_name "\\(\\) \\{" {capture=1}
        capture {print}
        capture && /^}$/ {exit}
    ' "${RUN_THIRDPARTIES_PATH}"
}

main() {
    local port_is_available pick_free_port assign_hive3_yarn_ports
    port_is_available="$(extract_function port_is_available)"
    pick_free_port="$(extract_function pick_free_port)"
    assign_hive3_yarn_ports="$(extract_function assign_hive3_yarn_ports)"

    if [[ -z "${port_is_available}" || -z "${pick_free_port}" || -z "${assign_hive3_yarn_ports}" ]]; then
        echo "ERROR: failed to extract Hive3 port helpers from ${RUN_THIRDPARTIES_PATH}" >&2
        exit 1
    fi

    local temp_root
    temp_root="$(mktemp -d)"
    trap 'rm -rf "'"${temp_root}"'"' EXIT

    local test_script="${temp_root}/test.sh"
    cat >"${test_script}" <<EOF
#!/usr/bin/env bash
set -euo pipefail

${port_is_available}

${pick_free_port}

${assign_hive3_yarn_ports}

ss() { return 0; }

export YARN_RM_SCHEDULER_PORT=20001
export YARN_RM_TRACKER_PORT=20002
export YARN_RM_PORT=20003
export YARN_RM_ADMIN_PORT=20004
export YARN_RM_WEBAPP_PORT=20005
export YARN_NM_LOCAL_PORT=20006
export YARN_NM_WEBAPP_PORT=20007
unset MAPREDUCE_SHUFFLE_PORT

assign_hive3_yarn_ports

[[ -n "\${MAPREDUCE_SHUFFLE_PORT:-}" ]]
[[ "\${MAPREDUCE_SHUFFLE_PORT}" =~ ^[0-9]+\$ ]]
[[ "\${MAPREDUCE_SHUFFLE_PORT}" != "\${YARN_RM_SCHEDULER_PORT}" ]]
[[ "\${MAPREDUCE_SHUFFLE_PORT}" != "\${YARN_RM_TRACKER_PORT}" ]]
[[ "\${MAPREDUCE_SHUFFLE_PORT}" != "\${YARN_RM_PORT}" ]]
[[ "\${MAPREDUCE_SHUFFLE_PORT}" != "\${YARN_RM_ADMIN_PORT}" ]]
[[ "\${MAPREDUCE_SHUFFLE_PORT}" != "\${YARN_RM_WEBAPP_PORT}" ]]
[[ "\${MAPREDUCE_SHUFFLE_PORT}" != "\${YARN_NM_LOCAL_PORT}" ]]
[[ "\${MAPREDUCE_SHUFFLE_PORT}" != "\${YARN_NM_WEBAPP_PORT}" ]]
EOF
    chmod +x "${test_script}"

    bash "${test_script}"
    grep -Fx 'MAPRED_CONF_mapreduce_shuffle_port=${MAPREDUCE_SHUFFLE_PORT}' "${ENV_PATH}"
}

main "$@"
