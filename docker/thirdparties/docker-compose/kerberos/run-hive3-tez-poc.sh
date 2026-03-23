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
REPO_ROOT="$(cd "${ROOT}/../../../.." && pwd)"
WORKDIR="${POC_WORKDIR:-/tmp/hive3-tez-poc}"
TMPDIR="${POC_TMPDIR:-${WORKDIR}/tmp}"
POC_CONTAINER_UID="${POC_CONTAINER_UID:-tezpoc}"
POC_KEEP_UP="${POC_KEEP_UP:-0}"
SERVICE_TIMEOUT_SECONDS="${SERVICE_TIMEOUT_SECONDS:-300}"
HEALTH_TIMEOUT_SECONDS="${HEALTH_TIMEOUT_SECONDS:-180}"
POC_USE_HOST_NETWORK="${POC_USE_HOST_NETWORK:-0}"
CONTAINER_NAME="doris-${POC_CONTAINER_UID}-kerberos1"
PRINCIPAL="hive/hadoop-master@LABS.TERADATA.COM"
COMPOSE_FILE="${WORKDIR}/docker-compose/kerberos/kerberos.yaml"
ENTRYPOINT_FILE="${WORKDIR}/docker-compose/kerberos/entrypoint-hive-master.sh"
WORKING_JDBC_URL=""

cleanup() {
    if [[ "${POC_KEEP_UP}" == "1" ]]; then
        echo "INFO: keep temporary environment running at ${WORKDIR}"
        return
    fi

    if [[ -f "${COMPOSE_FILE}" ]]; then
        docker compose -f "${COMPOSE_FILE}" down >/dev/null 2>&1 || true
    fi
    rm -rf "${WORKDIR}"
}

print_failure_context() {
    echo "INFO: dumping Tez POC diagnostics"
    docker ps -a --filter "name=${CONTAINER_NAME}" --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}' || true
    docker logs --tail 200 "${CONTAINER_NAME}" || true
    docker exec "${CONTAINER_NAME}" bash -lc 'supervisorctl status' || true
    docker exec "${CONTAINER_NAME}" bash -lc "ss -ltnp | egrep ':10000|:13000|:15000' || true" || true
}

render_temp_compose() {
    rm -rf "${WORKDIR}"
    mkdir -p "${WORKDIR}/docker-compose"
    mkdir -p "${TMPDIR}"
    export TMPDIR
    cp -r "${REPO_ROOT}/docker/thirdparties/docker-compose/common" "${WORKDIR}/docker-compose/"
    cp -r "${REPO_ROOT}/docker/thirdparties/docker-compose/kerberos" "${WORKDIR}/docker-compose/"

    # The kerberized image downloads extra jars from the regression bucket at startup.
    source "${REPO_ROOT}/docker/thirdparties/custom_settings.env"
    sed -i "s/s3Endpoint/${s3Endpoint}/g" "${ENTRYPOINT_FILE}"
    sed -i "s/s3BucketName/${s3BucketName}/g" "${ENTRYPOINT_FILE}"

    export CONTAINER_UID="${POC_CONTAINER_UID}"
    for i in 1 2; do
        # shellcheck disable=SC1090
        source "${WORKDIR}/docker-compose/kerberos/kerberos${i}_settings.env"
        if [[ "${POC_USE_HOST_NETWORK}" == "1" ]]; then
            IP_HOST="$(hostname -I | awk '{print $1}')"
        else
            # The POC runs all Hadoop, Hive, and YARN services inside one container.
            # Point metastore URIs at loopback so the rendered config stays self-contained.
            IP_HOST="127.0.0.1"
        fi
        export IP_HOST
        envsubst <"${WORKDIR}/docker-compose/kerberos/hadoop-hive.env.tpl" >"${WORKDIR}/docker-compose/kerberos/hadoop-hive-${i}.env"
        envsubst <"${WORKDIR}/docker-compose/kerberos/conf/my.cnf.tpl" >"${WORKDIR}/docker-compose/kerberos/conf/kerberos${i}/my.cnf"
        envsubst <"${WORKDIR}/docker-compose/kerberos/conf/kerberos${i}/kdc.conf.tpl" >"${WORKDIR}/docker-compose/kerberos/conf/kerberos${i}/kdc.conf"
        envsubst <"${WORKDIR}/docker-compose/kerberos/conf/kerberos${i}/krb5.conf.tpl" >"${WORKDIR}/docker-compose/kerberos/conf/kerberos${i}/krb5.conf"
    done
    envsubst <"${WORKDIR}/docker-compose/kerberos/kerberos.yaml.tpl" >"${COMPOSE_FILE}"

    # The Tez POC runs a single container and only validates in-container services.
    # Avoid host-network port collisions with unrelated local services by default.
    if [[ "${POC_USE_HOST_NETWORK}" != "1" ]]; then
        sed -i '/network_mode: "host"/d' "${COMPOSE_FILE}"
    fi
}

wait_until_services_running() {
    local waited=0
    local expected_services=(
        "hdfs-datanode"
        "hdfs-namenode"
        "hive-metastore"
        "hive-server2"
        "kadmind"
        "krb5kdc"
        "mysql-metastore"
        "yarn-nodemanager"
        "yarn-resourcemanager"
    )

    while (( waited < SERVICE_TIMEOUT_SECONDS )); do
        local container_state
        container_state="$(docker inspect --format '{{.State.Status}}' "${CONTAINER_NAME}" 2>/dev/null || true)"
        if [[ "${container_state}" == "exited" ]]; then
            echo "ERROR: ${CONTAINER_NAME} exited before all services became ready"
            print_failure_context
            return 1
        fi

        local supervisor_output
        supervisor_output="$(docker exec "${CONTAINER_NAME}" bash -lc 'supervisorctl status' 2>/dev/null || true)"
        if [[ -n "${supervisor_output}" ]]; then
            local all_running=1
            local service
            for service in "${expected_services[@]}"; do
                if ! printf '%s\n' "${supervisor_output}" | grep -q "^${service}[[:space:]].*RUNNING"; then
                    all_running=0
                    break
                fi
            done
            if (( all_running == 1 )); then
                echo "INFO: all core Hadoop, YARN, and Hive services are RUNNING"
                return 0
            fi
        fi

        if (( waited % 30 == 0 )); then
            echo "INFO: waiting for core services in ${CONTAINER_NAME} to become RUNNING"
        fi
        sleep 5
        waited=$((waited + 5))
    done

    echo "ERROR: timed out waiting for core services in ${CONTAINER_NAME}"
    print_failure_context
    return 1
}

wait_until_container_healthy() {
    local waited=0
    while (( waited < HEALTH_TIMEOUT_SECONDS )); do
        local status
        status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${CONTAINER_NAME}" 2>/dev/null || true)"
        if [[ "${status}" == "healthy" ]]; then
            echo "INFO: ${CONTAINER_NAME} healthcheck is healthy"
            return 0
        fi
        if [[ "${status}" == "exited" ]]; then
            echo "ERROR: ${CONTAINER_NAME} exited before healthcheck became healthy"
            print_failure_context
            return 1
        fi
        if (( waited % 30 == 0 )); then
            echo "INFO: waiting for ${CONTAINER_NAME} healthcheck to become healthy"
        fi
        sleep 5
        waited=$((waited + 5))
    done

    echo "ERROR: timed out waiting for ${CONTAINER_NAME} healthcheck"
    print_failure_context
    return 1
}

leave_hdfs_safe_mode() {
    echo "INFO: leaving HDFS safe mode if needed"
    docker exec "${CONTAINER_NAME}" bash -lc "
        set -euo pipefail
        kinit -kt /etc/hadoop/conf/hdfs.keytab hdfs/hadoop-master@LABS.TERADATA.COM
        hdfs dfsadmin -safemode leave >/dev/null 2>&1 || true
        hdfs dfsadmin -safemode get
    "
}

probe_jdbc_url() {
    local jdbc_url="$1"
    docker exec "${CONTAINER_NAME}" bash -lc "
        set -euo pipefail
        kinit -kt /etc/hive/conf/hive.keytab ${PRINCIPAL}
        beeline -u '${jdbc_url}' --showHeader=false --outputformat=tsv2 -e 'set hive.execution.engine;'
    "
}

find_working_jdbc_url() {
    local candidates=(
        "jdbc:hive2://localhost:15000/default;principal=${PRINCIPAL}"
        "jdbc:hive2://localhost:13000/default;principal=${PRINCIPAL}"
        "jdbc:hive2://localhost:13000/default;transportMode=http;httpPath=cliservice;principal=${PRINCIPAL}"
    )

    local jdbc_url
    for jdbc_url in "${candidates[@]}"; do
        echo "INFO: probing HiveServer2 with ${jdbc_url}"
        if probe_jdbc_url "${jdbc_url}"; then
            WORKING_JDBC_URL="${jdbc_url}"
            echo "INFO: found working HiveServer2 JDBC URL"
            return 0
        fi
    done

    echo "ERROR: failed to connect to HiveServer2 using known JDBC URL variants"
    print_failure_context
    return 1
}

run_poc_queries() {
    echo "INFO: supervisor status"
    docker exec "${CONTAINER_NAME}" bash -lc 'supervisorctl status'

    echo "INFO: running minimal Tez DML validation"
    docker exec "${CONTAINER_NAME}" bash -lc "
        set -euo pipefail
        kinit -kt /etc/hive/conf/hive.keytab ${PRINCIPAL}
        beeline -u '${WORKING_JDBC_URL}' --showHeader=false --outputformat=tsv2 -e \"
            drop table if exists tez_poc_orc;
            create table tez_poc_orc(id int) stored as orc;
            insert into tez_poc_orc values (1),(2),(3);
            select count(*) from tez_poc_orc;
        \"
    "
}

main() {
    trap cleanup EXIT

    echo "INFO: rendering temporary hive3 Tez POC under ${WORKDIR}"
    render_temp_compose

    echo "INFO: starting temporary kerberized hive service"
    docker compose -f "${COMPOSE_FILE}" down >/dev/null 2>&1 || true
    docker compose -f "${COMPOSE_FILE}" up -d hive-krb1

    wait_until_services_running
    wait_until_container_healthy
    leave_hdfs_safe_mode
    find_working_jdbc_url
    run_poc_queries

    echo "INFO: hive3 Tez POC finished successfully"
}

main "$@"
