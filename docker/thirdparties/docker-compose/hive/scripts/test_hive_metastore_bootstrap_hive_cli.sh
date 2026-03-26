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
    local configure_hive_bootstrap_cli
    configure_hive_bootstrap_cli="$(extract_function configure_hive_bootstrap_cli)"
    if [[ -z "${configure_hive_bootstrap_cli}" ]]; then
        echo "ERROR: failed to extract configure_hive_bootstrap_cli from ${SCRIPT_PATH}" >&2
        exit 1
    fi

    local temp_root
    temp_root="$(mktemp -d)"
    trap 'rm -rf "'"${temp_root}"'"' EXIT

    local fake_hive_bin_dir="${temp_root}/opt/hive/bin"
    local fake_hive_bin="${fake_hive_bin_dir}/hive"
    local wrapper_dir="${temp_root}/bootstrap-bin"
    local invocation_log="${temp_root}/hive-invocation.log"

    mkdir -p "${fake_hive_bin_dir}"
    cat >"${fake_hive_bin}" <<EOF
#!/usr/bin/env bash
printf '%s\n' "\$*" >"${invocation_log}"
EOF
    chmod +x "${fake_hive_bin}"

    local test_script="${temp_root}/test.sh"
    cat >"${test_script}" <<EOF
#!/usr/bin/env bash
set -euo pipefail

${configure_hive_bootstrap_cli}

export ENABLE_HIVE3_TEZ_RUNTIME=true
export HIVE_CLI_BIN="${fake_hive_bin}"
export HIVE_BOOTSTRAP_BIN_DIR="${wrapper_dir}"
export PATH="/usr/bin:/bin"

configure_hive_bootstrap_cli
hive -f /tmp/bootstrap.hql
EOF
    chmod +x "${test_script}"

    bash "${test_script}"

    [[ -x "${wrapper_dir}/hive" ]]
    [[ "$(cat "${invocation_log}")" == "--hiveconf hive.execution.engine=mr -f /tmp/bootstrap.hql" ]]
}

main "$@"
