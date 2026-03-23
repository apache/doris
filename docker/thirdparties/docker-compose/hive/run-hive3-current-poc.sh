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
HS_PORT="${POC_HS_PORT:-43000}"
HMS_PORT="${POC_HMS_PORT:-49383}"
PG_PORT="${POC_PG_PORT:-45732}"
FS_PORT="${POC_FS_PORT:-48320}"
LOAD_PARALLEL="${POC_LOAD_PARALLEL:-1}"
HEALTH_TIMEOUT_SECONDS="${HEALTH_TIMEOUT_SECONDS:-180}"
IP_HOST="${POC_IP_HOST:-127.0.0.1}"

COMPOSE_DIR="${WORKDIR}/hive"
COMPOSE_FILE="${COMPOSE_DIR}/hive-3x.yaml"
ENV_FILE="${COMPOSE_DIR}/hadoop-hive-3x.env"
SERVER_CONTAINER="${POC_CONTAINER_UID}hive3-server"

cleanup() {
    if [[ "${POC_KEEP_UP}" == "1" ]]; then
        echo "INFO: keep current Hive POC environment at ${WORKDIR}"
        return
    fi

    if [[ -f "${COMPOSE_FILE}" ]]; then
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

    export CONTAINER_UID="${POC_CONTAINER_UID}"
    export NEED_LOAD_DATA=0
    export LOAD_PARALLEL
    export IP_HOST
    export FS_PORT HMS_PORT HS_PORT PG_PORT

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
    render_temp_compose

    echo "INFO: starting current Hive3 compose"
    docker compose -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" down >/dev/null 2>&1 || true
    docker compose -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" up -d --wait

    wait_until_ready
    run_poc_queries

    echo "INFO: current Hive3 POC finished successfully"
}

main "$@"
