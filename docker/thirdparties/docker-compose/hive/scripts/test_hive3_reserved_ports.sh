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
    local initialize_hive3_settings append_hive3_yarn_ports_to_reserved_ports prepare_hive3_reserved_ports
    port_is_available="$(extract_function port_is_available)"
    pick_free_port="$(extract_function pick_free_port)"
    assign_hive3_yarn_ports="$(extract_function assign_hive3_yarn_ports)"
    initialize_hive3_settings="$(extract_function initialize_hive3_settings)"
    append_hive3_yarn_ports_to_reserved_ports="$(extract_function append_hive3_yarn_ports_to_reserved_ports)"
    prepare_hive3_reserved_ports="$(extract_function prepare_hive3_reserved_ports)"

    if [[ -z "${port_is_available}" || -z "${pick_free_port}" || -z "${assign_hive3_yarn_ports}" || -z "${initialize_hive3_settings}" || -z "${append_hive3_yarn_ports_to_reserved_ports}" || -z "${prepare_hive3_reserved_ports}" ]]; then
        echo "ERROR: failed to extract Hive3 reserved-port helpers from ${RUN_THIRDPARTIES_PATH}" >&2
        exit 1
    fi

    local temp_root
    temp_root="$(mktemp -d)"
    trap 'rm -rf "'"${temp_root}"'"' EXIT

    local test_script="${temp_root}/test.sh"
    cat >"${test_script}" <<EOF
#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "${THIRDPARTY_ROOT}" && pwd)"
CONTAINER_UID=test--
RESERVED_PORTS="65535,50070,50075"
HIVE3_SETTINGS_INITIALIZED=0

${port_is_available}

${pick_free_port}

${assign_hive3_yarn_ports}

${initialize_hive3_settings}

${append_hive3_yarn_ports_to_reserved_ports}

${prepare_hive3_reserved_ports}

ss() { return 0; }

prepare_hive3_reserved_ports

for port_var in \
    YARN_RM_SCHEDULER_PORT \
    YARN_RM_TRACKER_PORT \
    YARN_RM_PORT \
    YARN_RM_ADMIN_PORT \
    YARN_RM_WEBAPP_PORT \
    YARN_NM_LOCAL_PORT \
    YARN_NM_WEBAPP_PORT \
    MAPREDUCE_SHUFFLE_PORT; do
    current_port="\${!port_var:-}"
    [[ -n "\${current_port}" ]]
    [[ ",\${RESERVED_PORTS}," == *",\${current_port},"* ]]
done
EOF
    chmod +x "${test_script}"

    bash "${test_script}"
}

main "$@"
