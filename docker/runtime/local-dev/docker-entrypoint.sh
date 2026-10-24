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

set -eo pipefail

# Prefer eth0/docker IP over loopback so FE/BE agree on published addresses (see log: HostInfo host=192.168.x.x).
pick_primary_ipv4() {
    local _tok
    for _tok in $(hostname -I 2>/dev/null); do
        [[ "${_tok}" == "127.0.0.1" ]] && continue
        [[ -n "${_tok}" ]] && echo "${_tok}" && return
    done
    echo "127.0.0.1"
}

_DEFAULT_CONTAINER_IP="$(pick_primary_ipv4)"

readonly DORIS_HOME="/opt/apache-doris"
readonly FE_HOME="${DORIS_HOME}/fe"
readonly BE_HOME="${DORIS_HOME}/be"
readonly MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
readonly MYSQL_PORT="${MYSQL_PORT:-9030}"
readonly FE_HTTP_PORT="${FE_HTTP_PORT:-8030}"
readonly BE_HEARTBEAT_PORT="${BE_HEARTBEAT_PORT:-9050}"
readonly BE_IP="${BE_IP:-${_DEFAULT_CONTAINER_IP}}"
readonly MAX_WAIT_SECONDS="${MAX_WAIT_SECONDS:-300}"

log() {
    echo "$(date -Iseconds) [local-dev] $*"
}

_cleaned=0
cleanup() {
    [[ "${_cleaned}" -eq 1 ]] && return
    _cleaned=1
    log "Shutting down (stop BE then FE)..."
    "${BE_HOME}/bin/stop_be.sh" 2>/dev/null || true
    sleep 2
    "${FE_HOME}/bin/stop_fe.sh" 2>/dev/null || true
}

on_signal() {
    cleanup
    exit 143
}
trap on_signal SIGTERM
trap 'cleanup; exit 130' SIGINT
trap cleanup EXIT

export SKIP_CHECK_ULIMIT="${SKIP_CHECK_ULIMIT:-true}"

mysql_root() {
    mysql -uroot -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" --protocol=tcp --connect-timeout=5 "$@"
}

first_fe_bootstrap() {
    [[ ! -d "${FE_HOME}/doris-meta/image" ]]
}

priority_networks_value() {
    if [[ -n "${PRIORITY_NETWORKS:-}" ]]; then
        echo "${PRIORITY_NETWORKS}"
    elif [[ "${BE_IP}" == "127.0.0.1" ]]; then
        echo "127.0.0.1/24"
    else
        echo "${BE_IP%.*}.0/24"
    fi
}

append_priority_networks() {
    local f="$1"
    local pn
    pn="$(priority_networks_value)"
    if [[ -f "$f" ]] && ! grep -qE '^[[:space:]]*priority_networks[[:space:]]*=' "$f"; then
        log "Appending priority_networks = ${pn} to $(basename "$(dirname "$f")")/conf/$(basename "$f")"
        echo "priority_networks = ${pn}" >>"$f"
    fi
}

fe_http_up() {
    curl -sf --max-time 2 "http://127.0.0.1:${FE_HTTP_PORT}/" &>/dev/null
}

wait_for_fe() {
    local i
    for ((i = 0; i < MAX_WAIT_SECONDS; i++)); do
        # SELECT 1 can go through Nereids and require a BE (Doris 4.x+), while no BE is running yet — deadlock.
        if mysql_root -e "SHOW FRONTENDS" &>/dev/null; then
            log "FE query port is ready (SHOW FRONTENDS)."
            return 0
        fi
        if ((i % 20 == 0)); then
            if fe_http_up; then
                log "Waiting for FE MySQL port ${MYSQL_PORT} (HTTP ${FE_HTTP_PORT} is up)... (${i}/${MAX_WAIT_SECONDS}s)"
            else
                log "Waiting for FE on ${MYSQL_HOST}:${MYSQL_PORT}... (${i}/${MAX_WAIT_SECONDS}s)"
            fi
        fi
        sleep 1
    done
    log "ERROR: FE did not become ready in time."
    return 1
}

backend_registered() {
    mysql_root -N -e "SHOW BACKENDS" 2>/dev/null | grep -w "${BE_IP}" | grep -w "${BE_HEARTBEAT_PORT}" &>/dev/null
}

register_backend() {
    if backend_registered; then
        log "BE ${BE_IP}:${BE_HEARTBEAT_PORT} already registered."
        return 0
    fi
    local i
    for ((i = 0; i < MAX_WAIT_SECONDS; i++)); do
        if mysql_root -e "ALTER SYSTEM ADD BACKEND '${BE_IP}:${BE_HEARTBEAT_PORT}'" &>/dev/null; then
            log "Registered BE ${BE_IP}:${BE_HEARTBEAT_PORT}."
            return 0
        fi
        if ((i % 20 == 0)); then
            log "Retrying ADD BACKEND... (${i}/${MAX_WAIT_SECONDS}s)"
        fi
        sleep 1
        backend_registered && return 0
    done
    log "ERROR: Failed to register BE."
    return 1
}

bootstrap_users() {
    log "Ensuring admin user exists (empty password) and ADMIN_PRIV; root left as cluster default."
    set +e
    mysql_root --comments <<'EOSQL'
CREATE USER IF NOT EXISTS 'admin'@'%' IDENTIFIED BY '';
GRANT ADMIN_PRIV ON *.*.* TO 'admin'@'%';
EOSQL
    local st=$?
    set -e
    if [[ $st -ne 0 ]]; then
        log "WARN: admin bootstrap SQL returned $st (empty password may be rejected on this version)."
    fi
}

if first_fe_bootstrap; then
    append_priority_networks "${BE_HOME}/conf/be.conf"
    append_priority_networks "${FE_HOME}/conf/fe.conf"
fi

log "Container primary IPv4: ${_DEFAULT_CONTAINER_IP}; BE_IP=${BE_IP} (ALTER SYSTEM ADD BACKEND); MySQL host=${MYSQL_HOST}"

export JAVA_TOOL_OPTIONS='-XX:-UseContainerSupport'

log "Starting FE..."
"${FE_HOME}/bin/start_fe.sh" --console &

wait_for_fe || exit 1
register_backend || exit 1
bootstrap_users

log "Starting BE (console, SKIP_CHECK_ULIMIT=${SKIP_CHECK_ULIMIT})..."
"${BE_HOME}/bin/start_be.sh" --console &
be_pid=$!
wait "${be_pid}" || true
