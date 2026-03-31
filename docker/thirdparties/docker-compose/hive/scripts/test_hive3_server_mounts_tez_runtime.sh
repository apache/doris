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

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
ENV_PATH="${ROOT}/hadoop-hive-3x.env.tpl"
YAML_PATH="${ROOT}/hive-3x.yaml.tpl"

extract_hive_server_block() {
    awk '
        /^  hive-server:$/ {capture=1}
        capture && /^  hive-metastore:$/ {exit}
        capture {print}
    ' "${YAML_PATH}"
}

main() {
    if ! grep -q '^HIVE_AUX_JARS_PATH=/mnt/scripts/tez-runtime$' "${ENV_PATH}"; then
        exit 0
    fi

    if ! grep -q '^TEZ_CONF_DIR=/mnt/scripts/tez-conf$' "${ENV_PATH}"; then
        exit 0
    fi

    local hive_server_block
    hive_server_block="$(extract_hive_server_block)"
    if [[ -z "${hive_server_block}" ]]; then
        echo "ERROR: failed to extract hive-server block from ${YAML_PATH}" >&2
        exit 1
    fi

    grep -q '^    volumes:$' <<<"${hive_server_block}"
    grep -q '^      - \./scripts:/mnt/scripts$' <<<"${hive_server_block}"
}

main "$@"

