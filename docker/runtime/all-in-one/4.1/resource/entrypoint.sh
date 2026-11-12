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
# Entrypoint for every role of the all-in-one image. DORIS_ROLE selects what
# the container runs; each role is plain Doris started with --console, so
# `docker logs` carries the process output and the container exits non-zero
# when the process does. `docker stop` shuts down gracefully and exits 0.
#
#   all         one FE and one BE on loopback -- the single-container image (default)
#   fe          an FE: bootstraps the cluster when FE_MASTER is unset, joins it otherwise
#   be          a BE that registers with FE_MASTER
#   ms          the cloud meta-service  } DEPLOY_MODE=cloud only; FDB_CLUSTER
#   recycler    the cloud recycler      } names the FoundationDB to use
#   cloud-init  one-shot: create the cloud instance on the object store, then exit 0
#   client      waits until EXPECT_FE / EXPECT_BE nodes are alive, then idles
#
# Every role reads FE_CONFIG_EXTRA / BE_CONFIG_EXTRA / MS_CONFIG_EXTRA (appended
# to the respective conf), FE_HEAP / BE_HEAP (JVM -Xmx, e.g. 1024m) and
# START_TIMEOUT. The compose files under ../compose show the rest in use.

set -Eeuo pipefail
set -m

CI_HOME="${CI_HOME:-/opt/doris-ci}"
# shellcheck source=lib.sh
source "${CI_HOME}/lib.sh"

MY_IP="$(my_ip)"
PRIORITY_NETWORKS="${PRIORITY_NETWORKS:-${MY_IP}/32}"
MASTER_IP=
FE_PID=
BE_PID=
MS_PID=

require_env() {
    local v
    for v in "$@"; do
        [[ -n "${!v:-}" ]] || die "${v} must be set for DORIS_ROLE=${DORIS_ROLE} DEPLOY_MODE=${DEPLOY_MODE}"
    done
}

# ------------------------------------------------------------ conf blocks ---
# What each role appends to the shipped conf. Topology keys live here rather
# than in the image so that one image serves every topology.

fe_conf_block() {
    printf 'priority_networks = %s\nenable_fqdn_mode = false\n' "${PRIORITY_NETWORKS}"
    if [[ "${DORIS_ROLE}" == all ]]; then
        # One BE means one replica: saves every downstream CREATE TABLE from
        # spelling out replication_num=1 (Config.java calls this a
        # test-environment knob, which is what this image is), and there is
        # nothing to balance across a single backend.
        printf 'force_olap_table_replication_num = 1\ndisable_balance = true\n'
    fi
    if [[ "${DEPLOY_MODE}" == cloud ]]; then
        # SQL-managed cloud node: FE derives its cloud_unique_id from
        # cluster_id, which therefore has to be the numeric instance id that
        # cloud-init created on the meta-service.
        printf 'deploy_mode = cloud\ncluster_id = %s\nmeta_service_endpoint = %s\n' \
            "${INSTANCE_ID}" "${MS_ENDPOINT}"
    fi
    [[ -z "${FE_CONFIG_EXTRA:-}" ]] || printf '# --- FE_CONFIG_EXTRA ---\n%s\n' "${FE_CONFIG_EXTRA}"
}

be_conf_block() {
    printf 'priority_networks = %s\n' "${PRIORITY_NETWORKS}"
    if [[ "${DEPLOY_MODE}" == cloud ]]; then
        printf 'deploy_mode = cloud\nmeta_service_endpoint = %s\nenable_file_cache = true\n' "${MS_ENDPOINT}"
        printf 'file_cache_path = [{"path":"%s/storage/file_cache","total_size":%s,"query_limit":%s}]\n' \
            "${BE_HOME}" "${BE_FILE_CACHE_BYTES:-2147483648}" "${BE_FILE_CACHE_QUERY_LIMIT_BYTES:-1073741824}"
        printf 'tmp_file_dirs = [{"path":"%s/storage/tmp","max_cache_bytes":104857600,"max_upload_bytes":104857600}]\n' \
            "${BE_HOME}"
    fi
    [[ -z "${BE_CONFIG_EXTRA:-}" ]] || printf '# --- BE_CONFIG_EXTRA ---\n%s\n' "${BE_CONFIG_EXTRA}"
}

ms_conf_block() {
    printf 'fdb_cluster = %s\nbrpc_listen_port = %s\n' "${FDB_CLUSTER}" "${MS_PORT}"
    [[ -z "${MS_CONFIG_EXTRA:-}" ]] || printf '# --- MS_CONFIG_EXTRA ---\n%s\n' "${MS_CONFIG_EXTRA}"
}

# ---------------------------------------------------------------- probes ---

fe_up() { fe_health "$1" >/dev/null; }

# Pick the FE to talk to: the first entry of FE_MASTER (comma-separated
# hosts) that resolves and answers /api/health. Any live FE will do --
# ALTER SYSTEM is forwarded to whichever FE is master -- so listing every FE
# keeps a BE restart from waiting on the one FE that happens to be down.
# Doris identifies nodes by IP here (enable_fqdn_mode is off), so
# registration, --helper and the SQL probes all use the address.
resolve_master() {
    require_env FE_MASTER
    local deadline=$((SECONDS + START_TIMEOUT)) host ip
    while ((SECONDS < deadline)); do
        for host in ${FE_MASTER//,/ }; do
            ip="$(getent hosts "${host}" | awk '{print $1; exit}')" || true
            [[ -n "${ip}" ]] || continue
            if fe_up "${ip}"; then
                MASTER_IP="${ip}"
                info "using FE ${host} (${ip})"
                return 0
            fi
        done
        sleep 1
    done
    die "no FE in FE_MASTER=${FE_MASTER} answered within ${START_TIMEOUT}s"
}

# The local FE is up: the HTTP endpoint reports ready, and a metadata query
# proves the MySQL port serves. It has to be a metadata query -- `select 1`
# goes through Nereids, which picks a backend as its scan node and fails with
# "No backend available" until one is registered.
fe_alive_here() {
    kill -0 "${FE_PID}" 2>/dev/null || die "FE exited during startup, see ${FE_HOME}/log/fe.log"
    fe_up 127.0.0.1 && [[ "$(node_alive 127.0.0.1 frontends "${MY_IP}" "${FE_EDIT_LOG_PORT}")" == true ]]
}

# The local BE is up as far as the master FE is concerned, which is the only
# opinion that matters to a client.
be_alive_here() {
    kill -0 "${BE_PID}" 2>/dev/null || die "BE exited during startup, see ${BE_HOME}/log/be.INFO and ${BE_HOME}/log/be.out"
    [[ "$(node_alive "${MASTER_IP}" backends "${MY_IP}" "${BE_HEARTBEAT_PORT}")" == true ]]
}

tcp_open() { (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null; }

ms_alive_here() {
    kill -0 "${MS_PID}" 2>/dev/null || die "${DORIS_ROLE} exited during startup, see ${MS_HOME}/log/"
    curl -fsS --max-time 4 "http://127.0.0.1:${MS_PORT}/health" >/dev/null 2>&1
}

# A cloud instance created in storage-vault mode carries a built_in_storage_vault
# that nothing marks as default, and CREATE TABLE refuses to pick one on its own.
ensure_default_vault() {
    local vaults
    vaults="$(sql 127.0.0.1 'show storage vaults')" || return 0
    if awk -F'\t' '$NF == "true" { found = 1 } END { exit !found }' <<<"${vaults}"; then
        return 0
    fi
    if grep -q '^built_in_storage_vault' <<<"${vaults}"; then
        info "setting built_in_storage_vault as the default storage vault"
        sql 127.0.0.1 'set built_in_storage_vault as default storage vault' \
            || warn "could not set the default storage vault; CREATE TABLE will need an explicit one"
    fi
}

# park <name> <pid>: block until the process exits, then fail the container.
# `wait` also returns when a trap fires; the trap exits on its own, so getting
# past it means the process is really gone.
park() {
    local rc=0
    wait "$2" || rc=$?
    rm -f "${READY_FLAG}"
    die "$1 exited (rc=${rc})"
}

# ------------------------------------------------------------------ roles ---

start_fe() {
    info "starting FE${*:+ $*}"
    "${FE_HOME}/bin/start_fe.sh" "$@" --console &
    FE_PID=$!
}

start_be() {
    info "starting BE"
    "${BE_HOME}/bin/start_be.sh" --console &
    BE_PID=$!
}

register_be() {
    local stmt="alter system add backend '${MY_IP}:${BE_HEARTBEAT_PORT}'"
    if [[ "${DEPLOY_MODE}" == cloud ]]; then
        stmt+=" properties ('tag.compute_group_name' = '${COMPUTE_GROUP:-default_compute_group}')"
    fi
    # Idempotent: a container restarted on a mounted doris-meta already has it.
    if [[ -n "$(node_alive "${MASTER_IP}" backends "${MY_IP}" "${BE_HEARTBEAT_PORT}")" ]]; then
        info "backend ${MY_IP}:${BE_HEARTBEAT_PORT} already registered"
    else
        info "registering backend ${MY_IP}:${BE_HEARTBEAT_PORT}${COMPUTE_GROUP:+ in compute group ${COMPUTE_GROUP}}"
        sql "${MASTER_IP}" "${stmt}" || die "ALTER SYSTEM ADD BACKEND failed"
    fi
}

run_all() {
    [[ "${DEPLOY_MODE}" == local ]] || die "DORIS_ROLE=all only supports DEPLOY_MODE=local"
    MASTER_IP=127.0.0.1
    render_conf "${FE_HOME}/conf/fe.conf" "$(fe_conf_block)"
    render_conf "${BE_HOME}/conf/be.conf" "$(be_conf_block)"
    [[ -z "${FE_HEAP:-}" ]] || set_heap "${FE_HOME}/conf/fe.conf" "${FE_HEAP}"
    [[ -z "${BE_HEAP:-}" ]] || set_heap "${BE_HOME}/conf/be.conf" "${BE_HEAP}"
    start_fe
    wait_until "${START_TIMEOUT}" "FE to come up" fe_alive_here
    start_be
    register_be
    wait_until "${START_TIMEOUT}" "the backend to come alive" be_alive_here
    touch "${READY_FLAG}"
    info "cluster is ready -- mysql -uroot -h127.0.0.1 -P${FE_QUERY_PORT}"
    local rc=0
    wait -n "${FE_PID}" "${BE_PID}" || rc=$?
    rm -f "${READY_FLAG}"
    if ! kill -0 "${FE_PID}" 2>/dev/null; then
        die "FE exited (rc=${rc}), see ${FE_HOME}/log/fe.log"
    fi
    die "BE exited (rc=${rc}), see ${BE_HOME}/log/be.INFO and ${BE_HOME}/log/be.out"
}

run_fe() {
    [[ "${DEPLOY_MODE}" != cloud ]] || require_env INSTANCE_ID MS_ENDPOINT
    render_conf "${FE_HOME}/conf/fe.conf" "$(fe_conf_block)"
    [[ -z "${FE_HEAP:-}" ]] || set_heap "${FE_HOME}/conf/fe.conf" "${FE_HEAP}"
    if [[ -z "${FE_MASTER}" ]]; then
        start_fe
        wait_until "${START_TIMEOUT}" "FE to come up" fe_alive_here
        [[ "${DEPLOY_MODE}" != cloud ]] || ensure_default_vault
    else
        local role="${FE_ROLE:-follower}"
        case "${role}" in follower|observer) ;; *) die "bad FE_ROLE=${role}, expected follower|observer" ;; esac
        if [[ -d "${FE_HOME}/doris-meta/bdb" ]]; then
            # Already a member: the metadata knows the peers, so rejoin
            # without waiting on any other FE, which may well be down.
            info "metadata present, rejoining the cluster"
            start_fe
        else
            resolve_master
            # A node has to be in the master's metadata before it may join
            # with --helper.
            if [[ -z "$(node_alive "${MASTER_IP}" frontends "${MY_IP}" "${FE_EDIT_LOG_PORT}")" ]]; then
                info "registering as ${role} ${MY_IP}:${FE_EDIT_LOG_PORT}"
                sql "${MASTER_IP}" "alter system add ${role} '${MY_IP}:${FE_EDIT_LOG_PORT}'" \
                    || die "ALTER SYSTEM ADD ${role^^} failed"
            fi
            start_fe --helper "${MASTER_IP}:${FE_EDIT_LOG_PORT}"
        fi
        wait_until "${START_TIMEOUT}" "FE to join the cluster" fe_alive_here
    fi
    touch "${READY_FLAG}"
    info "FE is ready -- mysql -uroot -h${MY_IP} -P${FE_QUERY_PORT}"
    park FE "${FE_PID}"
}

run_be() {
    [[ "${DEPLOY_MODE}" != cloud ]] || require_env MS_ENDPOINT
    mkdir -p "${BE_HOME}/storage/file_cache" "${BE_HOME}/storage/tmp"
    render_conf "${BE_HOME}/conf/be.conf" "$(be_conf_block)"
    [[ -z "${BE_HEAP:-}" ]] || set_heap "${BE_HOME}/conf/be.conf" "${BE_HEAP}"
    resolve_master
    register_be
    start_be
    wait_until "${START_TIMEOUT}" "the backend to come alive" be_alive_here
    touch "${READY_FLAG}"
    info "BE is ready"
    park BE "${BE_PID}"
}

run_ms() {
    require_env FDB_CLUSTER
    [[ -x "${MS_HOME}/bin/start.sh" ]] || die "this image carries no meta-service payload (${MS_HOME})"
    local flag=--meta-service
    [[ "${DORIS_ROLE}" == recycler ]] && flag=--recycler
    render_conf "${MS_HOME}/conf/doris_cloud.conf" "$(ms_conf_block)"
    mkdir -p "${MS_HOME}/log"
    # doris_cloud gives up quickly when FoundationDB is not there yet; the
    # compose files order it after fdb, this covers a hand-run container.
    local fdb_addr="${FDB_CLUSTER##*@}"
    wait_until "${START_TIMEOUT}" "FoundationDB at ${fdb_addr}" tcp_open "${fdb_addr%:*}" "${fdb_addr##*:}"
    info "starting ${DORIS_ROLE}"
    (cd "${MS_HOME}" && bash bin/start.sh ${flag} --console) &
    MS_PID=$!
    wait_until "${START_TIMEOUT}" "${DORIS_ROLE} to come up" ms_alive_here
    touch "${READY_FLAG}"
    info "${DORIS_ROLE} is ready on ${MY_IP}:${MS_PORT}"
    park "${DORIS_ROLE}" "${MS_PID}"
}

run_cloud_init() {
    exec "${CI_HOME}/cloud_init.sh"
}

# Sit in the network as a ready-made client: `docker compose up --wait`
# returns once this container is healthy, i.e. once every expected node is
# alive, and `docker compose exec client mysql ...` reaches the cluster.
run_client() {
    resolve_master
    local want_fe="${EXPECT_FE:-1}" want_be="${EXPECT_BE:-1}"
    count_alive() {
        [[ "$(alive_count "${MASTER_IP}" frontends)" -ge "${want_fe}" ]] \
            && [[ "$(alive_count "${MASTER_IP}" backends)" -ge "${want_be}" ]]
    }
    wait_until "${START_TIMEOUT}" "${want_fe} FE and ${want_be} BE to be alive" count_alive
    touch "${READY_FLAG}"
    info "cluster is ready: $(alive_count "${MASTER_IP}" frontends) FE, $(alive_count "${MASTER_IP}" backends) BE"
    info "connect with: mysql -uroot -h${FE_MASTER%%,*} -P${FE_QUERY_PORT}"
    sleep infinity &
    wait $!
}

# --------------------------------------------------------------- lifecycle ---

shutdown() {
    trap - SIGTERM SIGINT
    rm -f "${READY_FLAG}"
    # BE first, so it stops reporting to an FE that is about to go away.
    stop_one BE "${BE_PID}"
    stop_one FE "${FE_PID}"
    stop_one "${DORIS_ROLE}" "${MS_PID}"
    exit 0
}

main() {
    trap shutdown SIGTERM SIGINT
    rm -f "${READY_FLAG}"
    case "${DEPLOY_MODE}" in local|cloud) ;; *) die "bad DEPLOY_MODE=${DEPLOY_MODE}, expected local|cloud" ;; esac
    info "role=${DORIS_ROLE} mode=${DEPLOY_MODE} ip=${MY_IP}"
    case "${DORIS_ROLE}" in
        all)         run_all ;;
        fe)          run_fe ;;
        be)          run_be ;;
        ms|recycler) run_ms ;;
        cloud-init)  run_cloud_init ;;
        client)      run_client ;;
        *) die "unknown DORIS_ROLE=${DORIS_ROLE}" ;;
    esac
}

main "$@"
