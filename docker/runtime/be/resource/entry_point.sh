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

get_doris_args() {
    local feServerArray=($(echo "${FE_SERVERS}" | awk '{gsub (/,/," "); print $0}'))
    for i in "${feServerArray[@]}"; do
        val=${i}
        val=${val// /}
        tmpFeId=$(echo "${val}" | awk -F ':' '{ sub(/fe/, ""); sub(/ /, ""); print$1}')
        tmpFeIp=$(echo "${val}" | awk -F ':' '{ sub(/ /, ""); print$2}')
        feIpArray[$tmpFeId]=${tmpFeIp}
    done

    declare -g MASTER_FE_IP BE_HOST_IP BE_HEARTBEAT_PORT
    MASTER_FE_IP=${feIpArray[1]}
    doris_note "masterFe = ${MASTER_FE_IP}"
    BE_HOST_IP=$(echo "${BE_ADDR}" | awk -F ':' '{ sub(/ /, ""); print$1}')
    BE_HEARTBEAT_PORT=$(echo "${BE_ADDR}" | awk -F ':' '{ sub(/ /, ""); print$2}')
    doris_note "be_addr = ${BE_HOST_IP}:${BE_HEARTBEAT_PORT}"
}

# Execute sql script, passed via stdin
# usage: docker_process_sql [mysql-cli-args]
#    ie: docker_process_sql --database=mydb <<<'INSERT ...'
#    ie: docker_process_sql --database=mydb <my-file.sql
docker_process_sql() {
    set +e
    mysql -uroot -P9030 -h${MASTER_FE_IP} --comments "$@" 2>/dev/null
}

check_be_status() {
    set +e
    local is_fe_start=false
    for i in {1..300}; do
        if [[ $(($i % 20)) == 1 ]]; then
            doris_warn "start check be status~"
        fi
        docker_process_sql <<<"show backends;" | grep "[[:space:]]${BE_HOST_IP}[[:space:]]" | grep "[[:space:]]${BE_HEARTBEAT_PORT}[[:space:]]" | grep "[[:space:]]true[[:space:]]"
        be_join_status=$?
        if [[ "${be_join_status}" == 0 ]]; then
            doris_note "Verify that BE is registered to FE successfully"
            is_fe_start=true
            break
        else
            if [[ $(($i % 20)) == 1 ]]; then
                doris_note "register is failed, wait next~"
            fi
        fi
        sleep 1
    done
    if ! [[ $is_fe_start ]]; then
        doris_error "Failed to register BE to FE！Tried 30 times！Maybe FE Start Failed！"
    fi
}

# usage: docker_process_init_files [file [file [...]]]
#    ie: docker_process_init_files /always-initdb.d/*
# process initializer files, based on file extensions
docker_process_init_files() {
    local f
    for f; do
        case "$f" in
        *.sh)
            if [ -x "$f" ]; then
                doris_note "$0: running $f"
                "$f"
            else
                doris_note "$0: sourcing $f"
                . "$f"
            fi
            ;;
        *.sql)
            doris_note "$0: running $f"
            docker_process_sql <"$f"
            echo
            ;;
        *.sql.bz2)
            doris_note "$0: running $f"
            bunzip2 -c "$f" | docker_process_sql
            echo
            ;;
        *.sql.gz)
            doris_note "$0: running $f"
            gunzip -c "$f" | docker_process_sql
            echo
            ;;
        *.sql.xz)
            doris_note "$0: running $f"
            xzcat "$f" | docker_process_sql
            echo
            ;;
        *.sql.zst)
            doris_note "$0: running $f"
            zstd -dc "$f" | docker_process_sql
            echo
            ;;
        *) doris_warn "$0: ignoring $f" ;;
        esac
        echo
    done
}

_main() {
    docker_setup_env
    # get init args
    get_doris_args
    # Start Doris BE
    {
        set +e
        bash init_be.sh 2>/dev/null
    } &
    # check BE started status
    check_be_status
    if [ -z ${DATABASE_ALREADY_EXISTS} ]; then
        # run script
        docker_process_init_files /docker-entrypoint-initdb.d/*
    fi

    # keep BE started status
    wait
    exec "$@"
}

if ! _is_sourced; then
    _main "$@"
fi
