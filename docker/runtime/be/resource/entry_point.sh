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

# Check the variables required for startup
docker_required_variables_env() {
  declare -g RUN_TYPE
  if [[ -n "$FE_SERVERS" && -n "$BE_ADDR" ]]; then
      RUN_TYPE="ELECTION"
      if [[ $FE_SERVERS =~ ^.+:[1-2]{0,1}[0-9]{0,1}[0-9]{1}(\.[1-2]{0,1}[0-9]{0,1}[0-9]{1}){3}:[1-6]{0,1}[0-9]{1,4}(,.+:[1-2]{0,1}[0-9]{0,1}[0-9]{1}(\.[1-2]{0,1}[0-9]{0,1}[0-9]{1}){3}:[1-6]{0,1}[0-9]{1,4})*$ || $FE_SERVERS =~ ^.+:([0-9a-fA-F]{1,4}:){7,7}([0-9a-fA-F]{1,4}|:)|([0-9a-fA-F]{1,4}:){1,6}(:[0-9a-fA-F]{1,4}|:)|([0-9a-fA-F]{1,4}:){1,5}((:[0-9a-fA-F]{1,4}){1,2}|:)|([0-9a-fA-F]{1,4}:){1,4}((:[0-9a-fA-F]{1,4}){1,3}|:)|([0-9a-fA-F]{1,4}:){1,3}((:[0-9a-fA-F]{1,4}){1,4}|:)|([0-9a-fA-F]{1,4}:){1,2}((:[0-9a-fA-F]{1,4}){1,5}|:)|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6}|:)|:((:[0-9a-fA-F]{1,4}){1,7}|:)$ ]]; then
          doris_warn "FE_SERVERS" $FE_SERVERS
      else
          doris_error "FE_SERVERS rule error！example: \$FE_NAME:\$FE_HOST_IP:\$FE_EDIT_LOG_PORT[,\$FE_NAME:\$FE_HOST_IP:\$FE_EDIT_LOG_PORT]..."
      fi
      if [[ $BE_ADDR =~ ^[1-2]{0,1}[0-9]{0,1}[0-9]{1}(\.[1-2]{0,1}[0-9]{0,1}[0-9]{1}){3}:[1-6]{0,1}[0-9]{1,4}$ || $BE_ADDR =~ ^([0-9a-fA-F]{1,4}:){7,7}([0-9a-fA-F]{1,4}|:)|([0-9a-fA-F]{1,4}:){1,6}(:[0-9a-fA-F]{1,4}|:)|([0-9a-fA-F]{1,4}:){1,5}((:[0-9a-fA-F]{1,4}){1,2}|:)|([0-9a-fA-F]{1,4}:){1,4}((:[0-9a-fA-F]{1,4}){1,3}|:)|([0-9a-fA-F]{1,4}:){1,3}((:[0-9a-fA-F]{1,4}){1,4}|:)|([0-9a-fA-F]{1,4}:){1,2}((:[0-9a-fA-F]{1,4}){1,5}|:)|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6}|:)|:((:[0-9a-fA-F]{1,4}){1,7}|:):[1-6]{0,1}[0-9]{1,4}$ ]]; then
        doris_warn "BE_ADDR" $BE_ADDR
      else
        doris_error "BE_ADDR rule error！example: \$BE_IP:\$HEARTBEAT_SERVICE_PORT"
      fi
      export RUN_TYPE=${RUN_TYPE}
      return
  fi

  if [[ -n "$FE_MASTER_IP"  && -n "$BE_IP" && -n "$BE_PORT" ]]; then
      RUN_TYPE="ASSIGN"
      if [[ $FE_MASTER_IP =~ ^[1-2]{0,1}[0-9]{0,1}[0-9]{1}(\.[1-2]{0,1}[0-9]{0,1}[0-9]{1}){3}$ || $FE_MASTER_IP =~ ^([0-9a-fA-F]{1,4}:){7,7}([0-9a-fA-F]{1,4}|:)|([0-9a-fA-F]{1,4}:){1,6}(:[0-9a-fA-F]{1,4}|:)|([0-9a-fA-F]{1,4}:){1,5}((:[0-9a-fA-F]{1,4}){1,2}|:)|([0-9a-fA-F]{1,4}:){1,4}((:[0-9a-fA-F]{1,4}){1,3}|:)|([0-9a-fA-F]{1,4}:){1,3}((:[0-9a-fA-F]{1,4}){1,4}|:)|([0-9a-fA-F]{1,4}:){1,2}((:[0-9a-fA-F]{1,4}){1,5}|:)|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6}|:)|:((:[0-9a-fA-F]{1,4}){1,7}|:)$ ]]; then
          doris_warn "FE_MASTER_IP" $FE_MASTER_IP
      else
          doris_error "FE_MASTER_IP rule error！example: \$FE_MASTER_IP"
      fi
      if [[ $BE_IP =~ ^[1-2]{0,1}[0-9]{0,1}[0-9]{1}(\.[1-2]{0,1}[0-9]{0,1}[0-9]{1}){3}$ || $BE_IP =~ ^([0-9a-fA-F]{1,4}:){7,7}([0-9a-fA-F]{1,4}|:)|([0-9a-fA-F]{1,4}:){1,6}(:[0-9a-fA-F]{1,4}|:)|([0-9a-fA-F]{1,4}:){1,5}((:[0-9a-fA-F]{1,4}){1,2}|:)|([0-9a-fA-F]{1,4}:){1,4}((:[0-9a-fA-F]{1,4}){1,3}|:)|([0-9a-fA-F]{1,4}:){1,3}((:[0-9a-fA-F]{1,4}){1,4}|:)|([0-9a-fA-F]{1,4}:){1,2}((:[0-9a-fA-F]{1,4}){1,5}|:)|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6}|:)|:((:[0-9a-fA-F]{1,4}){1,7}|:)$ ]]; then
          doris_warn "BE_IP" $BE_IP
      else
          doris_error "BE_IP rule error！example: \$BE_IP"
      fi
      if [[ $BE_PORT =~ ^[1-6]{0,1}[0-9]{1,4}$ ]]; then
          doris_warn "BE_PORT" $BE_PORT
      else
          doris_error "BE_PORT rule error！example: \$BE_PORT."
      fi
      export RUN_TYPE=${RUN_TYPE}
      return
  fi

  doris_error EOF "
               Note that you did not configure the required parameters!
               plan 1:
               FE_SERVERS & BE_ADDR
               plan 2:
               FE_MASTER_IP & FE_MASTER_PORT & BE_IP & BE_PORT"
              EOF
}

get_doris_args() {
  declare -g MASTER_FE_IP CURRENT_BE_IP CURRENT_BE_PORT PRIORITY_NETWORKS
  if [ $RUN_TYPE == "ELECTION" ]; then
      local feServerArray=($(echo "${FE_SERVERS}" | awk '{gsub (/,/," "); print $0}'))
      for i in "${feServerArray[@]}"; do
        val=${i}
        val=${val// /}
        tmpFeName=$(echo "${val}" | awk -F ':' '{ sub(/fe/, ""); sub(/ /, ""); print$1}')
        tmpFeIp=$(echo "${val}" | awk -F ':' '{ sub(/ /, ""); print$2}')
        tmpFeEditLogPort=$(echo "${val}" | awk -F ':' '{ sub(/ /, ""); print$3}')
        check_arg "TMP_FE_NAME" $tmpFeName
        feIpArray[$tmpFeName]=${tmpFeIp}
      done

      FE_MASTER_IP=${feIpArray[1]}
      check_arg "FE_MASTER_IP" $FE_MASTER_IP
      BE_IP=$(echo "${BE_ADDR}" | awk -F ':' '{ sub(/ /, ""); print$1}')
      check_arg "BE_IP" $BE_IP
      BE_PORT=$(echo "${BE_ADDR}" | awk -F ':' '{ sub(/ /, ""); print$2}')
      check_arg "BE_PORT" $BE_PORT

  elif [ $RUN_TYPE == "ASSIGN" ]; then
      check_arg "FE_MASTER_IP" $FE_MASTER_IP
      check_arg "BE_IP" $BE_IP
      check_arg "BE_PORT" $BE_PORT
  fi

  PRIORITY_NETWORKS=$(echo "${BE_IP}" | awk -F '.' '{print$1"."$2"."$3".0/24"}')
  check_arg "PRIORITY_NETWORKS" $PRIORITY_NETWORKS

  # export be args
  export MASTER_FE_IP=${FE_MASTER_IP}
  export CURRENT_BE_IP=${BE_IP}
  export CURRENT_BE_PORT=${BE_PORT}
  export PRIORITY_NETWORKS=${PRIORITY_NETWORKS}

  doris_note "MASTER_FE_IP ${MASTER_FE_IP}"
  doris_note "CURRENT_BE_IP ${CURRENT_BE_IP}"
  doris_note "CURRENT_BE_PORT ${CURRENT_BE_PORT}"
  doris_note "PRIORITY_NETWORKS ${PRIORITY_NETWORKS}"

  check_be_status true
}

# Execute sql script, passed via stdin
# usage: docker_process_sql [mysql-cli-args]
#    ie: docker_process_sql --database=mydb <<<'INSERT ...'
#    ie: docker_process_sql --database=mydb <my-file.sql
docker_process_sql() {
    set +e
    mysql -uroot -P9030 -h${MASTER_FE_IP} --comments "$@" 2>&1
}

check_be_status() {
  set +e
  for i in {1..300}; do
    if [[ $1 == true ]]; then
      docker_process_sql <<<"show frontends" | grep "[[:space:]]${MASTER_FE_IP}[[:space:]]"
    else
      docker_process_sql <<<"show backends" | grep "[[:space:]]${CURRENT_BE_IP}[[:space:]]" | grep "[[:space:]]${CURRENT_BE_PORT}[[:space:]]"
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
    if [[ $(( $i % 20 )) == 1 ]]; then
      if [[ $1 == true ]]; then
        doris_note "MASTER FE is not started. retry."
      else
        doris_note "BE is not register. retry."
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
    docker_setup_env
    # Start Doris BE
    {
        set +e
        bash init_be.sh 2>/dev/null
    } &
    # check BE started status
    check_be_status
    if [ -z ${DATABASE_ALREADY_EXISTS} ]; then
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
