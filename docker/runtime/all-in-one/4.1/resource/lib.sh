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
# Shared helpers for entrypoint.sh and health_check.sh. Sourced, not run.

DORIS_HOME="${DORIS_HOME:-/opt/apache-doris}"
FE_HOME="${DORIS_HOME}/fe"
BE_HOME="${DORIS_HOME}/be"
MS_HOME="${DORIS_HOME}/ms"
READY_FLAG="${DORIS_HOME}/.ready"

# What this container is. See the header of entrypoint.sh for the list.
DORIS_ROLE="${DORIS_ROLE:-all}"
# local = storage and compute on the BEs; cloud = BEs are compute nodes over a
# meta-service and an object store.
DEPLOY_MODE="${DEPLOY_MODE:-local}"

# Ports are the Doris defaults. They only need changing when several nodes
# share one network namespace, which none of the shipped topologies do.
FE_HTTP_PORT="${FE_HTTP_PORT:-8030}"
FE_QUERY_PORT="${FE_QUERY_PORT:-9030}"
FE_EDIT_LOG_PORT="${FE_EDIT_LOG_PORT:-9010}"
BE_HTTP_PORT="${BE_HTTP_PORT:-8040}"
BE_HEARTBEAT_PORT="${BE_HEARTBEAT_PORT:-9050}"
MS_PORT="${MS_PORT:-5000}"

# The master FE other nodes register with. Empty on an FE means "I am the
# first FE and bootstrap the cluster". A hostname resolved by docker DNS works
# as well as an IP.
FE_MASTER="${FE_MASTER:-}"

START_TIMEOUT="${START_TIMEOUT:-300}"
STOP_TIMEOUT="${STOP_TIMEOUT:-30}"

log()  { printf '%s [%-5s] [%s] %s\n' "$(date -Iseconds)" "$1" "${DORIS_ROLE}" "${*:2}"; }
info() { log INFO "$@"; }
warn() { log WARN "$@" >&2; }
die()  { log ERROR "$@" >&2; exit 1; }

# The address this node is known by. Single-container mode binds everything
# to loopback; every other role uses the container's first IP, which the
# compose files pin so that a restarted container keeps its identity.
my_ip() {
    if [[ "${DORIS_ROLE}" == all ]]; then
        echo 127.0.0.1
    else
        hostname -i | awk '{print $1}'
    fi
}

# sql <host> <statement>: root, no password, batch output, no header.
sql() {
    mysql -uroot -h"$1" -P"${FE_QUERY_PORT}" -N --batch --connect-timeout=2 -e "$2" 2>/dev/null
}

# The node probes go through SHOW FRONTENDS / SHOW BACKENDS, which FE answers
# on its own. The frontends() / backends() table functions look handier but
# are queries, and a query needs a live BE to scan -- which is the very thing
# being waited for. Columns are picked by header name, not position.

# node_alive <fe host> <frontends|backends> <node ip> <port>: prints the
# node's Alive column (true/false), nothing when the node is not listed.
node_alive() {
    local fe=$1 what=$2 ip=$3 port=$4 portcol
    case "${what}" in
        frontends) portcol=EditLogPort ;;
        backends)  portcol=HeartbeatPort ;;
    esac
    mysql -uroot -h"${fe}" -P"${FE_QUERY_PORT}" --batch --connect-timeout=2 -e "show ${what}" 2>/dev/null \
        | awk -F'\t' -v ip="${ip}" -v port="${port}" -v pc="${portcol}" '
            NR == 1 { for (i = 1; i <= NF; i++) col[$i] = i; next }
            $col["Host"] == ip && $col[pc] == port { print $col["Alive"]; exit }'
}

# alive_count <fe host> <frontends|backends>: how many nodes report Alive.
alive_count() {
    mysql -uroot -h"$1" -P"${FE_QUERY_PORT}" --batch --connect-timeout=2 -e "show $2" 2>/dev/null \
        | awk -F'\t' 'NR == 1 { for (i = 1; i <= NF; i++) col[$i] = i; next }
                      $col["Alive"] == "true" { n++ } END { print n + 0 }'
}

# Public FE endpoint (HealthAction): 503 until FE is ready, otherwise a body
# carrying online_backend_num. curl -f turns the 503 into a non-zero exit.
fe_health() {
    curl -fsS --max-time 4 "http://$1:${FE_HTTP_PORT}/api/health" 2>/dev/null
}

# wait_until <seconds> <description> <command...>: poll the command once a
# second until it succeeds; die when the deadline passes.
wait_until() {
    local timeout=$1 what=$2; shift 2
    local deadline=$((SECONDS + timeout))
    while ((SECONDS < deadline)); do
        if "$@"; then return 0; fi
        sleep 1
    done
    die "timed out after ${timeout}s waiting for ${what}"
}

# render_conf <conf file> <block>: rewrite the file as the shipped copy plus
# the block, so that a restarted container (same layer, entrypoint run again)
# does not keep appending. The shipped copy is kept next to it on first run.
# Doris takes the last assignment of a key, which is what lets the block win
# over the upstream defaults above it.
render_conf() {
    local conf=$1 block=$2
    [[ -f "${conf}.orig" ]] || cp "${conf}" "${conf}.orig"
    { cat "${conf}.orig"; printf '\n# --- generated by %s at container start ---\n%s\n' "$(basename "$0")" "${block}"; } >"${conf}"
}

# set_heap <conf file> <size>: the JVM heap lives inside a long JAVA_OPTS line
# that also carries every --add-opens FE needs on JDK 17. Rewriting only the
# -Xmx/-Xms tokens keeps the rest of that line as shipped.
set_heap() {
    sed -i -E "s/-Xmx[0-9]+[kKmMgG]/-Xmx$2/g; s/-Xms[0-9]+[kKmMgG]/-Xms$2/g" "$1"
}

# Kill a background job by pid, gracefully first. Negative pid signals the
# whole process group, which is where the real java / doris_be process lives.
stop_one() {
    local name=$1 pid=$2
    [[ -n "${pid}" ]] || return 0
    kill -0 "${pid}" 2>/dev/null || return 0
    info "stopping ${name}"
    kill -TERM -"${pid}" 2>/dev/null || kill -TERM "${pid}" 2>/dev/null || true
    local deadline=$((SECONDS + STOP_TIMEOUT))
    while ((SECONDS < deadline)); do
        kill -0 "${pid}" 2>/dev/null || { info "${name} stopped"; return 0; }
        sleep 1
    done
    warn "${name} did not stop within ${STOP_TIMEOUT}s, killing"
    kill -KILL -"${pid}" 2>/dev/null || kill -KILL "${pid}" 2>/dev/null || true
}
