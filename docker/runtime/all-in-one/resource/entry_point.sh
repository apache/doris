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
    declare -g METADATA_FAILURE_RECOVERY MASTER_FE_IP CURRENT_BE_IP \
               CURRENT_BE_PORT CURRENT_BROKER_IP CURRENT_BROKER_PORT DATABASE_ALREADY_EXISTS
    MASTER_FE_IP="127.0.0.1"
    CURRENT_BE_IP="127.0.0.1"
    CURRENT_BE_PORT=9050
    CURRENT_BROKER_IP="127.0.0.1"
    CURRENT_BROKER_PORT=8000
    if [[ $RECOVERY == "true" ]]; then
        METADATA_FAILURE_RECOVERY='true'
    fi
    if [ -d "${DORIS_HOME}/fe/doris-meta/image" ]; then
      DATABASE_ALREADY_EXISTS='true'
    fi
}

# Execute sql script, passed via stdin
docker_process_sql() {
    set +e
    mysql -uroot -P9030 -h127.0.0.1 --comments "$@" 2>&1
}

check_be_status() {
  set +e
  declare -g BE_ALREADY_EXISTS
  for i in {1..300}; do
    if [[ $1 == true ]]; then
      docker_process_sql <<<"show frontends" | grep "[[:space:]]${MASTER_FE_IP}[[:space:]]"
    else
      docker_process_sql <<<"show backends" | grep "[[:space:]]${CURRENT_BE_IP}[[:space:]]" | grep "[[:space:]]${CURRENT_BE_PORT}[[:space:]]" | grep "[[:space:]]true[[:space:]]"
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

check_broker_status() {
  set +e
  declare -g BROKER_ALREADY_EXISTS
  if [[ $1 == true ]]; then
    docker_process_sql <<<"show broker" | grep "[[:space:]]${MASTER_FE_IP}[[:space:]]"
  else
    docker_process_sql <<<"show broker" | grep "[[:space:]]${CURRENT_BROKER_IP}[[:space:]]" | grep "[[:space:]]${CURRENT_BROKER_PORT}[[:space:]]" | grep "[[:space:]]true[[:space:]]"
  fi
  broker_join_status=$?
  if [[ "${broker_join_status}" == 0 ]]; then
    if [[ $1 == true ]]; then
      doris_note "MASTER FE is started!"
    else
      doris_note "EntryPoint Check - Verify that Broker is registered to FE successfully"
      BROKER_ALREADY_EXISTS=true
    fi
    return
  fi
  sleep 1
}

add_priority_networks() {
  doris_note "add priority_networks ‘127.0.0.1/24’ to ${DORIS_HOME}/be/conf/be.conf"
  echo "priority_networks = 127.0.0.1/24" >>${DORIS_HOME}/be/conf/be.conf
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
  for i in {1..300}; do
    docker_process_sql <<<"alter system add backend '${CURRENT_BE_IP}:${CURRENT_BE_PORT}'"
    register_be_status=$?
    if [[ $register_be_status == 0 ]]; then
      doris_note "BE successfully registered to FE！"
      is_fe_start=true
      return
    fi
    if [[ $(( $i % 20 )) == 1 ]]; then
      doris_note "Register BE to FE is failed. retry."
    fi
    sleep 1
  done
  if ! [[ $is_fe_start ]]; then
    doris_error "Failed to register BE to FE！Tried 30 times！Maybe FE Start Failed！"
  fi
}

register_broker_to_fe() {
  set +e
  # check fe status
  local is_fe_start=false
  for i in {1..300}; do
    if [ -n "$BROKER_ALREADY_EXISTS" ]; then
      doris_warn "Same Broker already exists! No need to register again！"
      break
    fi
    docker_process_sql <<<"alter system add broker test '${CURRENT_BROKER_IP}:${CURRENT_BROKER_PORT}'"
    register_broker_status=$?
    if [[ $register_broker_status == 0 ]]; then
      doris_note "Broker successfully registered to FE！"
      is_fe_start=true
      break
    fi
    if [[ $(( $i % 20 )) == 1 ]]; then
      doris_note "Register Broker to FE is failed. retry."
    fi
    sleep 1
  done
  if ! [[ $is_fe_start ]]; then
    doris_error "Failed to register Broker to FE！Tried 30 times！Maybe FE Start Failed！"
  fi
}

start_doris() {
    declare -g child_pid
    if [[ $METADATA_FAILURE_RECOVERY == "true" ]]; then
        doris_warn "Because \$RECOVERY = True, So Doris FE start metadata_failure_recovery model."
        start_fe.sh --metadata_failure_recovery --console
    else
        doris_note "Start Doris FE."
        {
            set +e
            bash start_fe.sh --console 2>/dev/null
        } &
    fi
    if [[ $BROKER = "true" ]]; then
        sleep 20
        {
            set +e
            bash start_broker.sh 2>/dev/null
        } &
        check_broker_status
        register_broker_to_fe
    fi
    sleep 20
    doris_note "Start Doris BE."
    {
        set +e
        bash start_be.sh --console 2>/dev/null
    } &
    child_pid=$!
}

stop_doris() {
    doris_note "Container stopped, running stop_fe & stop_be script"
    stop_broker.sh
    sleep 2
    stop_be.sh
    sleep 10
    stop_fe.sh
}

_main() {
    trap 'stop_doris' SIGTERM SIGINT
    docker_setup_env
    # Check Already Exists
    if [ -z "$DATABASE_ALREADY_EXISTS" ]; then
      add_priority_networks
    fi
    # Start Doris
    start_doris
    # register BE
    register_be_to_fe
    # keep BE started status
    wait $child_pid
    doris_note "Apache Doris is Start Successfully!"
    exec "$@"
}

if ! _is_sourced; then
    _main "$@"
fi
