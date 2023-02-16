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
#    ie: doris_warn "task may fe risky!"
#   out: 2023-01-08T19:08:16+08:00 [Warn] [Entrypoint]: task may fe risky!
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
    if [ -d "${DORIS_HOME}/fe/doris-meta/image" ]; then
        DATABASE_ALREADY_EXISTS='true'
    fi
}

# Check the variables required for startup
docker_required_variables_env() {
    if [[ $FE_SERVERS =~ ^.+:[1-2]{0,1}[0-9]{0,1}[0-9]{1}(\.[1-2]{0,1}[0-9]{0,1}[0-9]{1}){3}:[1-6]{0,1}[0-9]{1,4}(,.+:[1-2]{0,1}[0-9]{0,1}[0-9]{1}(\.[1-2]{0,1}[0-9]{0,1}[0-9]{1}){3}:[1-6]{0,1}[0-9]{1,4})*$ ]]; then
        doris_warn "FE_SERVERS" $FE_SERVERS
    else
        doris_error "FE_SERVERS rule error！example: \$FE_NAME:\$FE_HOST_IP:\$FE_EDIT_LOG_PORT[,\$FE_NAME:\$FE_HOST_IP:\$FE_EDIT_LOG_PORT]..."
    fi
    if [[ $FE_ID =~ ^[1-9]{1}$ ]]; then
        doris_warn "FE_ID" $FE_ID
    else
        doris_error "FE_ID rule error！If FE is the role of Master, please set FE_ID=1, and ensure that all IDs correspond to the IP of the current node."
    fi
}

get_doris_fe_args() {
    local feServerArray=($(echo "${FE_SERVERS}" | awk '{gsub (/,/," "); print $0}'))
    for i in "${feServerArray[@]}"; do
        val=${i}
        val=${val// /}
        tmpFeName=$(echo "${val}" | awk -F ':' '{ sub(/fe/, ""); sub(/ /, ""); print$1}')
        tmpFeIp=$(echo "${val}" | awk -F ':' '{ sub(/ /, ""); print$2}')
        tmpFeEditLogPort=$(echo "${val}" | awk -F ':' '{ sub(/ /, ""); print$3}')
        check_arg "TMP_FE_NAME" $tmpFeName
        feIpArray[$tmpFeName]=${tmpFeIp}
        check_arg "TMP_FE_EDIT_LOG_PORT" $tmpFeEditLogPort
        feEditLogPortArray[$tmpFeName]=${tmpFeEditLogPort}
    done

    declare -g MASTER_FE_IP CURRENT_FE_IP MASTER_FE_EDIT_PORT CURRENT_FE_EDIT_PORT PRIORITY_NETWORKS CURRENT_FE_IS_MASTER
    MASTER_FE_IP=${feIpArray[1]}
    check_arg "MASTER_FE_IP" $MASTER_FE_IP
    MASTER_FE_EDIT_PORT=${feEditLogPortArray[1]}
    check_arg "MASTER_FE_EDIT_PORT" $MASTER_FE_EDIT_PORT
    CURRENT_FE_IP=${feIpArray[FE_ID]}
    check_arg "CURRENT_FE_IP" $CURRENT_FE_IP
    CURRENT_FE_EDIT_PORT=${feEditLogPortArray[FE_ID]}
    check_arg "CURRENT_FE_EDIT_PORT" $CURRENT_FE_EDIT_PORT

    if [ ${MASTER_FE_IP} == ${CURRENT_FE_IP} ]; then
        CURRENT_FE_IS_MASTER=true
    else
        CURRENT_FE_IS_MASTER=false
    fi

    PRIORITY_NETWORKS=$(echo "${CURRENT_FE_IP}" | awk -F '.' '{print$1"."$2"."$3".0/24"}')
    check_arg "PRIORITY_NETWORKS" $PRIORITY_NETWORKS

    doris_note "FE_IP_ARRAY = ${feIpArray[*]}"
    doris_note "FE_EDIT_LOG_PORT_ARRAY = ${feEditLogPortArray[*]}"
    doris_note "MASTER_FE = ${feIpArray[1]}:${feEditLogPortArray[1]}"
    doris_note "CURRENT_FE = ${CURRENT_FE_IP}:${CURRENT_FE_EDIT_PORT}"
    doris_note "PRIORITY_NETWORKS = ${PRIORITY_NETWORKS}"
    # wait fe start
    check_fe_status true
}

add_priority_networks() {
    doris_note "add priority_networks ${1} to ${DORIS_HOME}/fe/conf/fe.conf"
    echo "priority_networks = ${1}" >>${DORIS_HOME}/fe/conf/fe.conf
}

# Execute sql script, passed via stdin
# usage: docker_process_sql sql_script
docker_process_sql() {
    set +e
    mysql -uroot -P9030 -h${MASTER_FE_IP} --comments "$@" 2>/dev/null
}

docker_setup_db() {
    set +e
    # check fe status
    local is_fe_start=false
    if [ ${CURRENT_FE_IS_MASTER} == true ]; then
        doris_note "Current FE is Master FE!  No need to register again！"
        return
    fi
    for i in {1..300}; do
        docker_process_sql <<<"alter system add FOLLOWER '${CURRENT_FE_IP}:${CURRENT_FE_EDIT_PORT}'"
        register_fe_status=$?
        if [[ $register_fe_status == 0 ]]; then
            doris_note "FE successfully registered！"
            is_fe_start=true
            break
        else
            check_fe_status
            if [ -n "$CURRENT_FE_ALREADY_EXISTS" ]; then
                doris_warn "Same frontend already exists! No need to register again！"
                break
            fi
            if [[ $(($i % 20)) == 1 ]]; then
                doris_warn "register_fe_status: ${register_fe_status}"
                doris_warn "FE failed registered!"
            fi
        fi
        if [[ $(($i % 20)) == 1 ]]; then
            doris_note "ADD FOLLOWER failed, retry."
        fi
        sleep 1
    done
    if ! [[ $is_fe_start ]]; then
        doris_error "Failed to register CURRENT_FE to FE！Tried 30 times！Maybe FE Start Failed！"
    fi
}

# Check whether the passed parameters are empty to avoid subsequent task execution failures. At the same time,
# enumeration checks can fe added, such as checking whether a certain parameter appears repeatedly, etc.
check_arg() {
    if [ -z $2 ]; then
        doris_error "$1 is null!"
    fi
}

check_fe_status() {
    set +e
    declare -g CURRENT_FE_ALREADY_EXISTS
    if [ ${CURRENT_FE_IS_MASTER} == true ]; then
        doris_note "Current FE is Master FE!  No need check fe status！"
        return
    fi
    for i in {1..300}; do
        if [[ $1 == true ]]; then
            docker_process_sql <<<"show frontends" | grep "[[:space:]]${MASTER_FE_IP}[[:space:]]" | grep "[[:space:]]${MASTER_FE_EDIT_PORT}[[:space:]]"
        else
            docker_process_sql <<<"show frontends" | grep "[[:space:]]${CURRENT_FE_IP}[[:space:]]" | grep "[[:space:]]${CURRENT_FE_EDIT_PORT}[[:space:]]"
        fi
        fe_join_status=$?
        if [[ "${fe_join_status}" == 0 ]]; then
            if [[ $1 == true ]]; then
                doris_note "Master FE is started!"
            else
                doris_note "Verify that CURRENT_FE is registered to FE successfully"
            fi
            CURRENT_FE_ALREADY_EXISTS=true
            break
        else
            if [[ $(($i % 20)) == 1 ]]; then
                if [[ $1 == true ]]; then
                    doris_note "Master FE is not started, retry."
                else
                    doris_warn "Verify that CURRENT_FE is registered to FE failed, retry."
                fi
            fi
        fi
        if [[ $(($i % 20)) == 1 ]]; then
            doris_note "try session Master FE."
        fi
        sleep 1
    done
}

_main() {
    docker_setup_env
    docker_required_variables_env
    get_doris_fe_args

    if [ -z "$DATABASE_ALREADY_EXISTS" ]; then
        add_priority_networks $PRIORITY_NETWORKS
    fi

    docker_setup_db
    check_fe_status
    doris_note "Ready to start CURRENT_FE！"

    if [ $CURRENT_FE_IS_MASTER == true ]; then
        start_fe.sh
    else
        start_fe.sh --helper ${MASTER_FE_IP}:${MASTER_FE_EDIT_PORT}
    fi

    exec "$@"
}

if ! _is_sourced; then
    _main "$@"
fi
