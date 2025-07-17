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
BE_ALREADY_EXISTS=false

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

# Check the variables required for startup
docker_required_variables_env() {
    check_arg "MASTER_FE" $MASTER_FE
    check_arg "CURRENT_BE" $CURRENT_BE
}

get_doris_args() {

    export MASTER_FE_HOST=$(echo "${MASTER_FE}" | awk -F ':' '{ sub(/ /, ""); print$1}')
    export CURRENT_BE_HOST=$(echo "${CURRENT_BE}" | awk -F ':' '{ sub(/ /, ""); print$1}')
    export CURRENT_BE_PORT=$(echo "${CURRENT_BE}" | awk -F ':' '{ sub(/ /, ""); print$2}')

    if [[ -z $DORIS_PRIORITY_NETWORKS ]]; then
       DORIS_PRIORITY_NETWORKS=$(echo "${CURRENT_BE_HOST}" | awk -F '.' '{print$1"."$2"."$3".0/24"}')
       export DORIS_PRIORITY_NETWORKS=${DORIS_PRIORITY_NETWORKS}
    fi
    check_arg "PRIORITY_NETWORKS" $DORIS_PRIORITY_NETWORKS
    
    doris_note "MASTER_FE_HOST ${MASTER_FE_HOST}"
    doris_note "CURRENT_BE_HOST ${CURRENT_BE_HOST}"
    doris_note "CURRENT_BE_PORT ${CURRENT_BE_PORT}"
    doris_note "DORIS_PRIORITY_NETWORKS ${DORIS_PRIORITY_NETWORKS}"

    check_be_status true
}

# Execute sql script, passed via stdin
# usage: docker_process_sql [mysql-cli-args]
#    ie: docker_process_sql --database=mydb <<<'INSERT ...'
#    ie: docker_process_sql --database=mydb <my-file.sql
docker_process_sql() {
    set +e
    mysql -uroot -P9030 -h${MASTER_FE_HOST} --comments "$@" 2>&1
}

check_be_status() {
    set +e
    BE_ALREADY_EXISTS=false
    for i in {0..30}; do
        if [[ $1 == true ]]; then
            docker_process_sql <<<"show frontends" | grep "[[:space:]]${MASTER_FE_HOST}[[:space:]]"
        else
            docker_process_sql <<<"show backends" | grep "[[:space:]]${CURRENT_BE_HOST}[[:space:]]" | grep "[[:space:]]${CURRENT_BE_PORT}[[:space:]]"
        fi
        be_join_status=$?
        if [[ "${be_join_status}" == 0 ]]; then
            if [[ $1 == true ]]; then
                doris_note "MASTER FE is started!"
            else
                doris_note "EntryPoint Check - Verify that BE is registered to FE successfully"
                BE_ALREADY_EXISTS=true
            fi
            return
        fi
        if [[ $(( $i % 15 )) == 0 ]]; then
            if [[ $1 == true ]]; then
                doris_note "MASTER FE is not started. Tried ${i} times, retry..."
            else
                doris_note "BE has not been registered in cluster. Tried ${i} times, retry..."
            fi
        fi
        sleep 1
    done
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

# Check whether the passed parameters are empty to avoid subsequent task execution failures. At the same time,
# enumeration checks can be added, such as checking whether a certain parameter appears repeatedly, etc.
check_arg() {
    if [ -z $2 ]; then
        doris_error "$1 is null!"
    fi
}

_main() {
    docker_required_variables_env

    # get init args
    get_doris_args

    # Start Doris BE
    {
        set +e
        bash init_be.sh 2>/dev/null
    } &

    times=1
    while [[ $BE_ALREADY_EXISTS != "true" ]]; do
      times=$((times+1))
      # wait for 5min
      if [[ $times > 10 ]]; then
        doris_error "be is not registered in cluster! Byebye..."
      fi
      
      # check BE started status
      check_be_status
    done
    
    if [ -d "${DORIS_HOME}/be/storage/data" ]; then
        # run script
        sleep 15
        docker_process_init_files /docker-entrypoint-initdb.d/*
    fi

    # keep BE started status
    wait
    exec "$@"
}

if ! _is_sourced; then
    _main "$@"
fi
