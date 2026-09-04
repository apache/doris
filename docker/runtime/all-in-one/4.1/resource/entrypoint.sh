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
#
# Brings up one FE and one BE inside a single container and keeps them there.
#
# Fail-fast by design: if either process exits, so does the container, with a
# non-zero status. A test fixture that quietly restarts a dead FE turns a
# two-second failure into a job timeout.

set -Eeuo pipefail

# Job control, so each child lands in its own process group. This is what makes
# shutdown work: start_fe.sh / start_be.sh run the real java / doris_be process
# in the foreground and do not forward signals, and neither writes a usable pid
# file under --console, so stop_fe.sh / stop_be.sh cannot help us. Signalling
# the whole group reaches the actual process.
set -m

DORIS_HOME="${DORIS_HOME:-/opt/apache-doris}"
FE_HOME="${DORIS_HOME}/fe"
BE_HOME="${DORIS_HOME}/be"
READY_FLAG="${DORIS_HOME}/.ready"

HOST=127.0.0.1
FE_HTTP_PORT="${FE_HTTP_PORT:-8030}"
FE_QUERY_PORT="${FE_QUERY_PORT:-9030}"
BE_HEARTBEAT_PORT="${BE_HEARTBEAT_PORT:-9050}"
START_TIMEOUT="${START_TIMEOUT:-300}"
STOP_TIMEOUT="${STOP_TIMEOUT:-30}"

FE_PID=
BE_PID=

log()  { printf '%s [%-5s] [entrypoint] %s\n' "$(date -Iseconds)" "$1" "${*:2}"; }
info() { log INFO "$@"; }
warn() { log WARN "$@" >&2; }
die()  { log ERROR "$@" >&2; exit 1; }

sql() {
    mysql -uroot -h"${HOST}" -P"${FE_QUERY_PORT}" -N --batch --connect-timeout=2 -e "$1" 2>/dev/null
}

fe_health() {
    # Public endpoint (HealthAction): 503 until FE is ready, otherwise a body
    # carrying online_backend_num. curl -f turns the 503 into a non-zero exit.
    curl -fsS --max-time 4 "http://${HOST}:${FE_HTTP_PORT}/api/health" 2>/dev/null
}

# ---------------------------------------------------------------- config ----
# The image already carries the integration-test defaults; this is the runtime
# escape hatch for downstream projects that need one knob changed.
apply_env_overrides() {
    if [[ -n "${FE_CONFIG_EXTRA:-}" ]]; then
        info "appending FE_CONFIG_EXTRA to fe.conf"
        printf '\n# --- FE_CONFIG_EXTRA ---\n%s\n' "${FE_CONFIG_EXTRA}" >>"${FE_HOME}/conf/fe.conf"
    fi
    if [[ -n "${BE_CONFIG_EXTRA:-}" ]]; then
        info "appending BE_CONFIG_EXTRA to be.conf"
        printf '\n# --- BE_CONFIG_EXTRA ---\n%s\n' "${BE_CONFIG_EXTRA}" >>"${BE_HOME}/conf/be.conf"
    fi
}

# ------------------------------------------------------------------- FE -----
start_fe() {
    info "starting FE"
    "${FE_HOME}/bin/start_fe.sh" --console &
    FE_PID=$!
}

wait_fe_ready() {
    local deadline=$((SECONDS + START_TIMEOUT))
    while ((SECONDS < deadline)); do
        kill -0 "${FE_PID}" 2>/dev/null \
            || die "FE exited during startup, see ${FE_HOME}/log/fe.log"
        # Two gates: the HTTP endpoint reports FE readiness, and a metadata
        # query proves the MySQL port is actually serving. It has to be a
        # metadata query -- `select 1` goes through Nereids, which picks a
        # backend as its scan node and fails with "No backend available" until
        # one is registered. The BE is not started yet at this point, so using
        # it here would deadlock the two waits against each other.
        if fe_health >/dev/null && sql 'show frontends' | grep -q "${HOST}"; then
            info "FE is ready after ${SECONDS}s"
            return 0
        fi
        sleep 1
    done
    die "FE did not become ready within ${START_TIMEOUT}s, see ${FE_HOME}/log/fe.log"
}

# ------------------------------------------------------------------- BE -----
start_be() {
    info "starting BE"
    "${BE_HOME}/bin/start_be.sh" --console &
    BE_PID=$!
}

register_be() {
    # Idempotent: a container restarted on a mounted doris-meta already has the
    # backend in its metadata.
    if sql 'show backends' | grep -qE "[[:space:]]${HOST}[[:space:]]+${BE_HEARTBEAT_PORT}[[:space:]]"; then
        info "backend ${HOST}:${BE_HEARTBEAT_PORT} already registered"
    else
        info "registering backend ${HOST}:${BE_HEARTBEAT_PORT}"
        sql "alter system add backend '${HOST}:${BE_HEARTBEAT_PORT}'" \
            || die "ALTER SYSTEM ADD BACKEND failed"
    fi
}

wait_be_alive() {
    local deadline=$((SECONDS + START_TIMEOUT))
    while ((SECONDS < deadline)); do
        kill -0 "${BE_PID}" 2>/dev/null \
            || die "BE exited during startup, see ${BE_HOME}/log/be.INFO and ${BE_HOME}/log/be.out"
        # FE reports how many backends it considers alive, so one request
        # answers both "is the BE up" and "did FE notice".
        if fe_health | grep -qE '"online_backend_num"[[:space:]]*:[[:space:]]*[1-9]'; then
            info "backend is alive after ${SECONDS}s"
            return 0
        fi
        sleep 1
    done
    die "backend did not come alive within ${START_TIMEOUT}s"
}

# -------------------------------------------------------------- shutdown ----
stop_one() {
    local name=$1 pid=$2
    [[ -n "${pid}" ]] || return 0
    kill -0 "${pid}" 2>/dev/null || return 0
    info "stopping ${name}"
    # Negative pid signals the whole process group, which is where the real
    # java / doris_be process lives.
    kill -TERM -"${pid}" 2>/dev/null || kill -TERM "${pid}" 2>/dev/null || true
    local deadline=$((SECONDS + STOP_TIMEOUT))
    while ((SECONDS < deadline)); do
        kill -0 "${pid}" 2>/dev/null || { info "${name} stopped"; return 0; }
        sleep 1
    done
    warn "${name} did not stop within ${STOP_TIMEOUT}s, killing"
    kill -KILL -"${pid}" 2>/dev/null || kill -KILL "${pid}" 2>/dev/null || true
}

shutdown() {
    trap - SIGTERM SIGINT
    rm -f "${READY_FLAG}"
    # BE first, so it stops reporting to an FE that is about to go away.
    stop_one BE "${BE_PID}"
    stop_one FE "${FE_PID}"
    exit 0
}

# ------------------------------------------------------------------ main ----
main() {
    trap shutdown SIGTERM SIGINT
    rm -f "${READY_FLAG}"

    apply_env_overrides
    start_fe
    wait_fe_ready
    start_be
    register_be
    wait_be_alive

    touch "${READY_FLAG}"
    info "cluster is ready -- mysql -uroot -h127.0.0.1 -P${FE_QUERY_PORT}"

    # Park here until something dies. `wait -n` also returns when a trap fires,
    # so the explicit re-check below distinguishes the two cases.
    local rc=0
    wait -n "${FE_PID}" "${BE_PID}" || rc=$?
    rm -f "${READY_FLAG}"

    if ! kill -0 "${FE_PID}" 2>/dev/null; then
        die "FE exited (rc=${rc}), see ${FE_HOME}/log/fe.log"
    fi
    die "BE exited (rc=${rc}), see ${BE_HOME}/log/be.INFO and ${BE_HOME}/log/be.out"
}

main "$@"
