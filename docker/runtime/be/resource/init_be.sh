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

set -eo pipefail
shopt -s nullglob

DORIS_HOME="/opt/apache-doris"

# Obtain necessary and basic information to complete initialization

# logging functions
# usage: doris_[note|warn|error] $log_meg
#    ie: doris_warn "task may be risky!"
#   out: 2023-01-08T19:08:16+08:00 [Warn] [Entrypoint]: task may be risky!
doris_log() {
    local type="$1"
    shift
    # accept argument string or stdin
    local text="$*"
    if [ "$#" -eq 0 ]; then text="$(cat)"; fi
    local dt="$(date -Iseconds)"
    printf '%s [%s] [Entrypoint]: %s\n' "$dt" "$type" "$text"
}
doris_note() {
    doris_log Note "$@"
}
doris_warn() {
    doris_log Warn "$@" >&2
}
doris_error() {
    doris_log ERROR "$@" >&2
    exit 1
}

# check to see if this file is being run or sourced from another script
_is_sourced() {
    [ "${#FUNCNAME[@]}" -ge 2 ] &&
    [ "${FUNCNAME[0]}" = '_is_sourced' ] &&
    [ "${FUNCNAME[1]}" = 'source' ]
}

docker_setup_env() {
    declare -g DATABASE_ALREADY_EXISTS
    if [ -d "${DORIS_HOME}/be/storage/data" ]; then
        DATABASE_ALREADY_EXISTS='true'
    fi
}

add_priority_networks() {
    doris_note "add priority_networks ${1} to ${DORIS_HOME}/be/conf/be.conf"
    echo "priority_networks = ${1}" >>${DORIS_HOME}/be/conf/be.conf
}

show_be_args(){
    doris_note "============= init args ================"
    doris_note "MASTER_FE_IP " ${MASTER_FE_IP}
    doris_note "CURRENT_BE_IP " ${CURRENT_BE_IP}
    doris_note "CURRENT_BE_PORT " ${CURRENT_BE_PORT}
    doris_note "RUN_TYPE " ${RUN_TYPE}
    doris_note "PRIORITY_NETWORKS " ${PRIORITY_NETWORKS}
}

# Execute sql script, passed via stdin
# usage: docker_process_sql sql_script
docker_process_sql() {
    set +e
    mysql -uroot -P9030 -h${MASTER_FE_IP} --comments "$@" 2>/dev/null
}

node_role_conf(){
    if [[ ${NODE_ROLE} == 'computation' ]]; then
        doris_note "this node role is computation"
        echo "be_node_role=computation" >>${DORIS_HOME}/be/conf/be.conf
    else
        doris_note "this node role is mix"
    fi
}

register_be_to_fe() {
    set +e
    # check fe status
    local is_fe_start=false
    if [ -n "$DATABASE_ALREADY_EXISTS" ]; then
        check_be_status
        if [ -n "$BE_ALREADY_EXISTS" ]; then
            doris_warn "Same backend already exists! No need to register again！"
            return
        fi
    fi
    for i in {1..30}; do
        docker_process_sql <<<"alter system add backend '${CURRENT_BE_IP}:${CURRENT_BE_PORT}'"
        register_be_status=$?
        if [[ $register_be_status == 0 ]]; then
            doris_note "BE successfully registered to FE！"
            is_fe_start=true
            return
        fi
        if [[ $(( $i % 15 )) == 1 ]]; then
            doris_note "Register BE to FE is failed. retry."
        fi
        sleep 1
    done
    if ! [[ $is_fe_start ]]; then
        doris_error "Failed to register BE to FE！Tried 30 times！Maybe FE Start Failed！"
    fi
}

check_be_status() {
    set +e
    local is_fe_start=false
    for i in {1..30}; do
        if [[ $(($i % 15)) == 1 ]]; then
            doris_warn "start check be register status~"
        fi
        docker_process_sql <<<"show backends;" | grep "[[:space:]]${CURRENT_BE_IP}[[:space:]]" | grep "[[:space:]]${CURRENT_BE_PORT}[[:space:]]"
        be_join_status=$?
        if [[ "${be_join_status}" == 0 ]]; then
            doris_note "Verify that BE is registered to FE successfully"
            is_fe_start=true
            return
        else
            if [[ $(($i % 15)) == 1 ]]; then
                doris_note "register is failed, wait next~"
            fi
        fi
        sleep 1
    done
    if [[ ! $is_fe_start ]]; then
        doris_error "Failed to register BE to FE！Tried 30 times！Maybe FE Start Failed！"
    fi
}

cleanup() {
    doris_note "Container stopped, running stop_be script"
    ${DORIS_HOME}/be/bin/stop_be.sh
}

_main() {
    trap 'cleanup' SIGTERM SIGINT
    docker_setup_env
    if [ -z "$DATABASE_ALREADY_EXISTS" ]; then
        add_priority_networks $PRIORITY_NETWORKS
        node_role_conf
        show_be_args
        register_be_to_fe
    fi
    check_be_status
    doris_note "Ready to start BE！"
    export SKIP_CHECK_ULIMIT=true
    ${DORIS_HOME}/be/bin/start_be.sh --console &
    child_pid=$!
    wait $child_pid
    exec "$@"
}

if ! _is_sourced; then
    _main "$@"
fi
