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
REPO_ROOT="$(cd "${ROOT}/../../../.." && pwd)"
WORKDIR="${POC_WORKDIR:-${REPO_ROOT}/output/hive3-current-poc}"
TMPDIR="${POC_TMPDIR:-${WORKDIR}/tmp}"
POC_CONTAINER_UID="${POC_CONTAINER_UID:-hive3curpoc}"
POC_KEEP_UP="${POC_KEEP_UP:-0}"
HS_PORT="${POC_HS_PORT:-23000}"
HMS_PORT="${POC_HMS_PORT:-29383}"
PG_PORT="${POC_PG_PORT:-25732}"
FS_PORT="${POC_FS_PORT:-28320}"
YARN_RM_SCHEDULER_PORT="${POC_YARN_RM_SCHEDULER_PORT:-23030}"
YARN_RM_TRACKER_PORT="${POC_YARN_RM_TRACKER_PORT:-23031}"
YARN_RM_PORT="${POC_YARN_RM_PORT:-23032}"
YARN_RM_ADMIN_PORT="${POC_YARN_RM_ADMIN_PORT:-23033}"
YARN_RM_WEBAPP_PORT="${POC_YARN_RM_WEBAPP_PORT:-23088}"
YARN_NM_LOCAL_PORT="${POC_YARN_NM_LOCAL_PORT:-23400}"
YARN_NM_WEBAPP_PORT="${POC_YARN_NM_WEBAPP_PORT:-23442}"
LOAD_PARALLEL="${POC_LOAD_PARALLEL:-1}"
HEALTH_TIMEOUT_SECONDS="${HEALTH_TIMEOUT_SECONDS:-180}"
IP_HOST="${POC_IP_HOST:-127.0.0.1}"
TEZ_SOURCE_IMAGE="${POC_TEZ_SOURCE_IMAGE:-doristhirdpartydocker/trinodb:hdp3.1-hive-kerberized_96}"

COMPOSE_DIR="${WORKDIR}/hive"
COMPOSE_FILE="${COMPOSE_DIR}/hive-3x.yaml"
ENV_FILE="${COMPOSE_DIR}/hadoop-hive-3x.env"
SERVER_CONTAINER="${POC_CONTAINER_UID}hive3-server"

port_is_available() {
    local port="$1"
    local reserved_ports="${2:-}"

    if [[ " ${reserved_ports} " == *" ${port} "* ]]; then
        return 1
    fi

    if ss -H -tan | awk '{print $4}' | grep -Eq "[:.]${port}$"; then
        return 1
    fi

    return 0
}

pick_free_port() {
    local reserved_ports="${1:-}"
    local port

    while true; do
        port="$(( (RANDOM % 12000) + 20000 ))"
        if port_is_available "${port}" "${reserved_ports}"; then
            printf '%s\n' "${port}"
            return 0
        fi
    done
}

assign_free_poc_ports() {
    local reserved_ports=""
    local port_var current_port

    for port_var in \
        FS_PORT \
        HMS_PORT \
        HS_PORT \
        PG_PORT \
        YARN_RM_SCHEDULER_PORT \
        YARN_RM_TRACKER_PORT \
        YARN_RM_PORT \
        YARN_RM_ADMIN_PORT \
        YARN_RM_WEBAPP_PORT \
        YARN_NM_LOCAL_PORT \
        YARN_NM_WEBAPP_PORT; do
        current_port="${!port_var}"
        if port_is_available "${current_port}" "${reserved_ports}"; then
            reserved_ports="${reserved_ports} ${current_port}"
            continue
        fi

        current_port="$(pick_free_port "${reserved_ports}")"
        export "${port_var}=${current_port}"
        reserved_ports="${reserved_ports} ${current_port}"
    done
}

cleanup() {
    if [[ "${POC_KEEP_UP}" == "1" ]]; then
        echo "INFO: keep current Hive POC environment at ${WORKDIR}"
        return
    fi

    if [[ -f "${COMPOSE_FILE}" ]]; then
        docker exec "${POC_CONTAINER_UID}hive3-metastore" bash -lc '
            rm -rf /mnt/scripts/nm-local-dir/* /mnt/scripts/nm-log-dir/* /mnt/scripts/hive-local-scratch/* || true
        ' >/dev/null 2>&1 || true
        docker compose -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" down >/dev/null 2>&1 || true
    fi
    rm -rf "${WORKDIR}"
}

find_beeline() {
    docker exec "${SERVER_CONTAINER}" bash -lc '
        if command -v beeline >/dev/null 2>&1; then
            command -v beeline
        elif [[ -x /opt/hive/bin/beeline ]]; then
            echo /opt/hive/bin/beeline
        else
            exit 1
        fi
    '
}

probe_beeline() {
    local beeline_bin="$1"
    docker exec "${SERVER_CONTAINER}" bash -lc "
        set -euo pipefail
        ${beeline_bin} -u 'jdbc:hive2://127.0.0.1:${HS_PORT}/default' -n root --showHeader=false --outputformat=tsv2 -e 'show databases;'
    "
}

probe_execution_engine() {
    local beeline_bin="$1"
    docker exec "${SERVER_CONTAINER}" bash -lc "
        set -euo pipefail
        ${beeline_bin} -u 'jdbc:hive2://127.0.0.1:${HS_PORT}/default' -n root --showHeader=false --outputformat=tsv2 -e 'set hive.execution.engine;'
    "
}

assert_execution_engine_is_tez() {
    local beeline_bin output
    beeline_bin="$(find_beeline)"
    output="$(probe_execution_engine "${beeline_bin}")"
    printf '%s\n' "${output}"
    printf '%s\n' "${output}" | grep -q 'hive.execution.engine=tez'
}

wait_until_ready() {
    local beeline_bin waited
    beeline_bin="$(find_beeline)"
    waited=0
    while (( waited < HEALTH_TIMEOUT_SECONDS )); do
        if probe_beeline "${beeline_bin}" >/dev/null 2>&1; then
            echo "INFO: current HiveServer2 is ready on ${HS_PORT}"
            return 0
        fi
        if (( waited % 30 == 0 )); then
            echo "INFO: waiting for current HiveServer2 to become ready"
        fi
        sleep 5
        waited=$((waited + 5))
    done

    echo "ERROR: timed out waiting for current HiveServer2"
    docker ps -a --filter "name=${SERVER_CONTAINER}" --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}' || true
    docker logs --tail 200 "${SERVER_CONTAINER}" || true
    return 1
}

render_temp_compose() {
    rm -rf "${WORKDIR}"
    mkdir -p "${WORKDIR}"
    mkdir -p "${TMPDIR}"
    export TMPDIR
    cp -r "${ROOT}" "${WORKDIR}/"

    if ! docker image inspect "${TEZ_SOURCE_IMAGE}" >/dev/null 2>&1; then
        docker pull "${TEZ_SOURCE_IMAGE}" >/dev/null
    fi
    local tez_container_id
    tez_container_id="$(docker create "${TEZ_SOURCE_IMAGE}")"
    mkdir -p "${COMPOSE_DIR}/scripts/tez-runtime" "${COMPOSE_DIR}/scripts/tez-conf"
    mkdir -p "${COMPOSE_DIR}/scripts/nm-local-dir" "${COMPOSE_DIR}/scripts/nm-log-dir"
    mkdir -p "${COMPOSE_DIR}/scripts/hive-local-scratch"
    docker cp "${tez_container_id}:/usr/hdp/3.1.0.0-78/tez/." "${COMPOSE_DIR}/scripts/tez-runtime/"
    docker cp "${tez_container_id}:/etc/tez/conf/tez-site.xml" "${COMPOSE_DIR}/scripts/tez-conf/tez-site.xml"
    docker rm -f "${tez_container_id}" >/dev/null

    export CONTAINER_UID="${POC_CONTAINER_UID}"
    export NEED_LOAD_DATA=0
    export LOAD_PARALLEL
    export IP_HOST
    export FS_PORT HMS_PORT HS_PORT PG_PORT
    export YARN_RM_SCHEDULER_PORT YARN_RM_TRACKER_PORT YARN_RM_PORT
    export YARN_RM_ADMIN_PORT YARN_RM_WEBAPP_PORT YARN_NM_LOCAL_PORT YARN_NM_WEBAPP_PORT

    pushd "${COMPOSE_DIR}" >/dev/null
    envsubst < hadoop-hive.env.tpl > hadoop-hive-3x.env
    envsubst < hadoop-hive-3x.env.tpl >> hadoop-hive-3x.env
    envsubst < hive-3x.yaml.tpl > hive-3x.yaml
    popd >/dev/null
}

run_poc_queries() {
    local beeline_bin
    beeline_bin="$(find_beeline)"
    docker exec "${SERVER_CONTAINER}" bash -lc "
        set -euo pipefail
        ${beeline_bin} -u 'jdbc:hive2://127.0.0.1:${HS_PORT}/default' -n root --showHeader=false --outputformat=tsv2 -e \"
            drop table if exists hive3_cur_poc_orc;
            create table hive3_cur_poc_orc(id int) stored as orc;
            insert into hive3_cur_poc_orc values (1),(2),(3);
            select count(*) from hive3_cur_poc_orc;
        \"
    "
}

main() {
    trap cleanup EXIT

    echo "INFO: rendering temporary current Hive3 POC under ${WORKDIR}"
    assign_free_poc_ports
    render_temp_compose

    echo "INFO: starting current Hive3 compose"
    docker compose -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" down >/dev/null 2>&1 || true
    docker compose -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" up -d --wait

    wait_until_ready
    assert_execution_engine_is_tez
    run_poc_queries

    echo "INFO: current Hive3 POC finished successfully"
}

main "$@"
