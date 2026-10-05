#!/bin/bash
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

HIVE_SCRIPTS_ROOT="/mnt/scripts"
HIVE_STATE_DIR="${HIVE_STATE_DIR:-/mnt/state}"

# Route all `hive -f` / `hive -e` invocations (including those inside
# scripts/data/**/run.sh) through a beeline-backed shim that talks to the
# already-running HiveServer2. This removes the ~3-5s Hive CLI JVM cold-start
# cost per call, which is the dominant cost when loading many small DDL files.
if [[ ":${PATH}:" != *":${HIVE_SCRIPTS_ROOT}/bin:"* ]]; then
    export PATH="${HIVE_SCRIPTS_ROOT}/bin:${PATH}"
fi

ensure_hive_state_layout() {
    mkdir -p "${HIVE_STATE_DIR}/modules"
}

prepare_hive_aux_lib() {
    local aux_lib="${HIVE_SCRIPTS_ROOT}/auxlib"
    local file=""

    for file in "${aux_lib}"/*.tar.gz; do
        [[ -e "${file}" ]] || continue
        tar -xzvf "${file}" -C "${aux_lib}"
    done

    cp -r "${aux_lib}"/* /opt/hive/lib/

    shopt -s nullglob
    local juicefs_jars=("${aux_lib}"/juicefs-hadoop-*.jar)
    if (( ${#juicefs_jars[@]} > 0 )); then
        local target=""
        for target in /opt/hadoop-3.2.1/share/hadoop/common/lib /opt/hadoop/share/hadoop/common/lib; do
            if [[ -d "${target}" ]]; then
                cp -f "${juicefs_jars[@]}" "${target}"/
            fi
        done
    fi
    shopt -u nullglob
}

start_hive_metastore_service() {
    nohup /opt/hive/bin/hive --service metastore >/tmp/hive-metastore.log 2>&1 &
}

wait_hive_metastore_ready() {
    while ! bash -c "exec 3<>/dev/tcp/127.0.0.1/${HMS_PORT:-9083}" 2>/dev/null; do
        sleep 5s
    done
}

run_hive_hql() {
    local hql_path="$1"
    local description="$2"
    local start_time
    local end_time
    local execution_time

    start_time=$(date +%s)
    hive -f "${hql_path}"
    end_time=$(date +%s)
    execution_time=$((end_time - start_time))
    echo "${description} executed in ${execution_time} seconds"
}
