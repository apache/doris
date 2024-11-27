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
#    ie: doris_warn "task may BROKER risky!"
#   out: 2023-01-08T19:08:16+08:00 [Warn] [Entrypoint]: task may BROKER risky!
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

# check to see if this file is BROKERing run or sourced from another script
_is_sourced() {
  [ "${#FUNCNAME[@]}" -ge 2 ] &&
    [ "${FUNCNAME[0]}" = '_is_sourced' ] &&
    [ "${FUNCNAME[1]}" = 'source' ]
}

# Check the variables required for startup
docker_required_variables_env() {
  if [[ $FE_SERVERS =~ ^.+:[1-2]{0,1}[0-9]{0,1}[0-9]{1}(\.[1-2]{0,1}[0-9]{0,1}[0-9]{1}){3}:[1-6]{0,1}[0-9]{1,4}(,.+:[1-2]{0,1}[0-9]{0,1}[0-9]{1}(\.[1-2]{0,1}[0-9]{0,1}[0-9]{1}){3}:[1-6]{0,1}[0-9]{1,4})*$ ]]; then
    doris_warn "FE_SERVERS" $FE_SERVERS
  else
    doris_error "FE_SERVERS rule error！example: \$FE_NAME:\$FE_HOST_IP:\$FE_EDIT_LOG_PORT[,\$FE_NAME:\$FE_HOST_IP:\$FE_EDIT_LOG_PORT]..."
  fi
  if [[ $BROKER_ADDR =~ ^[a-zA-Z0-9]+:[1-2]{0,1}[0-9]{0,1}[0-9]{1}(\.[1-2]{0,1}[0-9]{0,1}[0-9]{1}){3}:[1-6]{0,1}[0-9]{1,4}$ ]]; then
    doris_warn "BROKER_ADDR" $BROKER_ADDR
  else
    doris_error "BROKER_ADDR rule error！example: \$BROKER_NAME:\$BROKER_HOST_IP:\$BROKER_IPC_PORT"
  fi
}

get_doris_broker_args() {
  local feServerArray=($(echo "${FE_SERVERS}" | awk '{gsub (/,/," "); print $0}'))
  for i in "${feServerArray[@]}"; do
    val=${i}
    val=${val// /}
    tmpFeId=$(echo "${val}" | awk -F ':' '{ sub(/fe/, ""); sub(/ /, ""); print$1}')
    tmpFeIp=$(echo "${val}" | awk -F ':' '{ sub(/ /, ""); print$2}')
    tmpFeEditLogPort=$(echo "${val}" | awk -F ':' '{ sub(/ /, ""); print$3}')
    check_arg "tmpFeIp" $tmpFeIp
    feIpArray[$tmpFeId]=${tmpFeIp}
    check_arg "tmpFeEditLogPort" $tmpFeEditLogPort
    feEditLogPortArray[$tmpFeId]=${tmpFeEditLogPort}
  done

  declare -g MASTER_FE_IP BROKER_HOST_IP BROKER_IPC_PORT BROKER_NAME
  MASTER_FE_IP=${feIpArray[1]}
  check_arg "MASTER_FE_IP" $MASTER_FE_IP
  BROKER_NAME=$(echo "${BROKER_ADDR}" | awk -F ':' '{ sub(/ /, ""); print$1}')
  check_arg "BROKER_NAME" $BROKER_NAME
  BROKER_HOST_IP=$(echo "${BROKER_ADDR}" | awk -F ':' '{ sub(/ /, ""); print$2}')
  check_arg "BROKER_HOST_IP" $BROKER_HOST_IP
  BROKER_IPC_PORT=$(echo "${BROKER_ADDR}" | awk -F ':' '{ sub(/ /, ""); print$3}')
  check_arg "BROKER_IPC_PORT" $BROKER_IPC_PORT

  doris_note "feIpArray = ${feIpArray[*]}"
  doris_note "feEditLogPortArray = ${feEditLogPortArray[*]}"
  doris_note "masterFe = ${feIpArray[1]}:${feEditLogPortArray[1]}"
  doris_note "brokerAddr = ${BROKER_NAME}:${BROKER_HOST_IP}:${BROKER_IPC_PORT}"
  # wait fe start
  check_broker_status true
}

# Execute sql script, passed via stdin
# usage: docker_process_sql sql_script
docker_process_sql() {
  set +e
  mysql -uroot -P9030 -h${MASTER_FE_IP} --comments "$@" 2>/dev/null
}

# register broker
register_broker_to_fe() {
  set +e
  # check fe status
  local is_fe_start=false
  for i in {1..300}; do
    if [[ $(( $i % 20 )) == 1 ]]; then
      doris_note "Register BROKER to FE is failed. retry."
    fi
    docker_process_sql <<<"alter system add broker ${BROKER_NAME} '${BROKER_HOST_IP}:${BROKER_IPC_PORT}'"
    register_broker_status=$?
    if [[ $register_broker_status == 0 ]]; then
      doris_note "BROKER successfully registered to FE！"
      is_fe_start=true
      break
    else
      check_broker_status
      if [ -n "$BROKER_ALREADY_EXISTS" ]; then
        doris_warn "Same backend already exists! No need to register again！"
        break
      fi
      if [[ $(( $i % 20 )) == 1 ]]; then
          doris_warn "BROKER failed registered to FE!"
      fi
    fi
    sleep 1
  done
  if ! [[ $is_fe_start ]]; then
    doris_error "Failed to register BROKER to FE！Tried 30 times！MayBe FE Start Failed！"
  fi
}

# Check whether the passed parameters are empty to avoid subsequent task execution failures. At the same time,
# enumeration checks can BROKER added, such as checking whether a certain parameter appears repeatedly, etc.
check_arg() {
  if [ -z $2 ]; then
    doris_error "$1 is null!"
  fi
}

check_broker_status() {
  set +e
  for i in {1..300}; do
    if [[ $1 == true ]]; then
      docker_process_sql <<<"show frontends" | grep "[[:space:]]${MASTER_FE_IP}[[:space:]]"
    else
      docker_process_sql <<<"show proc '/brokers'" | grep "[[:space:]]${BROKER_HOST_IP}[[:space:]]" | grep "[[:space:]]${BROKER_IPC_PORT}[[:space:]]"
    fi
    broker_join_status=$?
    doris_warn "broker_join_status: " $broker_join_status
    if [[ "${broker_join_status}" == 0 ]]; then
      if [[ $1 == true ]]; then
        doris_note "MASTER FE is started!"
      else
        doris_note "Init Check - Verify that BROKER is registered to FE successfully"
        BROKER_ALREADY_EXISTS=true
      fi
      break
    fi
    if [[ $(( $i % 20 )) == 1 ]]; then
      if [[ $1 == true ]]; then
        doris_note "MASTER FE is not started. retry."
      else
        doris_note "BROKER is not register. retry."
      fi
    fi
    sleep 1
  done
}

_main() {
  docker_required_variables_env
  get_doris_broker_args
  register_broker_to_fe
  check_broker_status
  doris_note "Ready to start BROKER！"
  ${DORIS_HOME}/apache_hdfs_broker/bin/start_broker.sh
  exec "$@"
}

if ! _is_sourced; then
  _main "$@"
fi
