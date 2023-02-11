#!/bin/env bash
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

# Check the variables required for startup
docker_required_variables_env() {
  if [[ $FE_SERVERS =~ ^.+:[1-2]{0,1}[0-9]{0,1}[0-9]{1}(\.[1-2]{0,1}[0-9]{0,1}[0-9]{1}){3}:[1-6]{0,1}[0-9]{1,4}(,.+:[1-2]{0,1}[0-9]{0,1}[0-9]{1}(\.[1-2]{0,1}[0-9]{0,1}[0-9]{1}){3}:[1-6]{0,1}[0-9]{1,4})*$ ]]; then
    doris_warn "FE_SERVERS" $FE_SERVERS
  else
    doris_error "FE_SERVERS rule error！example: \$FE_NAME:\$FE_HOST_IP:\$FE_EDIT_LOG_PORT[,\$FE_NAME:\$FE_HOST_IP:\$FE_EDIT_LOG_PORT]..."
  fi
  if [[ $BE_ADDR =~ ^[1-2]{0,1}[0-9]{0,1}[0-9]{1}(\.[1-2]{0,1}[0-9]{0,1}[0-9]{1}){3}:[1-6]{0,1}[0-9]{1,4}$ ]]; then
    doris_warn "BE_ADDR" $BE_ADDR
  else
    doris_error "BE_ADDR rule error！example: \$BE_HOST_IP:\$HEARTBEAT_SERVICE_PORT"
  fi
}

get_doris_be_args() {
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

  declare -g MASTER_FE_IP BE_HOST_IP BE_HEARTBEAT_PORT PRIORITY_NETWORKS
  MASTER_FE_IP=${feIpArray[1]}
  check_arg "MASTER_FE_IP" $MASTER_FE_IP
  BE_HOST_IP=$(echo "${BE_ADDR}" | awk -F ':' '{ sub(/ /, ""); print$1}')
  check_arg "BE_HOST_IP" $BE_HOST_IP
  BE_HEARTBEAT_PORT=$(echo "${BE_ADDR}" | awk -F ':' '{ sub(/ /, ""); print$2}')
  check_arg "BE_HEARTBEAT_PORT" $BE_HEARTBEAT_PORT

  PRIORITY_NETWORKS=$(echo "${BE_HOST_IP}" | awk -F '.' '{print$1"."$2"."$3".0/24"}')
  check_arg "priority_networks" $PRIORITY_NETWORKS

  doris_note "feIpArray = ${feIpArray[*]}"
  doris_note "feEditLogPortArray = ${feEditLogPortArray[*]}"
  doris_note "masterFe = ${feIpArray[1]}:${feEditLogPortArray[1]}"
  doris_note "be_addr = ${BE_HOST_IP}:${BE_HEARTBEAT_PORT}"
  doris_note "priority_networks = ${PRIORITY_NETWORKS}"
  # wait fe start
  check_be_status true
}

add_priority_networks() {
  doris_note "add priority_networks ${1} to ${DORIS_HOME}/be/conf/be.conf"
  echo "priority_networks = ${1}" >>${DORIS_HOME}/be/conf/be.conf
}

# Execute sql script, passed via stdin
# usage: docker_process_sql sql_script
docker_process_sql() {
  set +e
  mysql -uroot -P9030 -h${MASTER_FE_IP} --comments "$@" 2>/dev/null
}

register_be_to_fe() {
  set +e
  # check fe status
  local is_fe_start=false
  for i in {1..300}; do
    if [[ $(( $i % 20 )) == 1 ]]; then
      doris_note "Register BE to FE is failed. retry."
    fi
    docker_process_sql <<<"alter system add backend '${BE_HOST_IP}:${BE_HEARTBEAT_PORT}'"
    register_be_status=$?
    if [[ $register_be_status == 0 ]]; then
      doris_note "BE successfully registered to FE！"
      is_fe_start=true
      break
    else
      check_be_status
      if [ -n "$BE_ALREADY_EXISTS" ]; then
        doris_warn "Same backend already exists! No need to register again！"
        break
      fi
      if [[ $(( $i % 20 )) == 1 ]]; then
          doris_warn "register_be_status: ${register_be_status}"
          doris_warn "BE failed registered to FE!"
      fi
    fi
    sleep 1
  done
  if ! [[ $is_fe_start ]]; then
    doris_error "Failed to register BE to FE！Tried 30 times！Maybe FE Start Failed！"
  fi
}

# Check whether the passed parameters are empty to avoid subsequent task execution failures. At the same time,
# enumeration checks can be added, such as checking whether a certain parameter appears repeatedly, etc.
check_arg() {
  if [ -z $2 ]; then
    doris_error "$1 is null!"
  fi
}

# 这里可用 docker_process_sql() 函数封装，为了方便调试，暂未封装
check_be_status() {
  set +e
  for i in {1..300}; do
    if [[ $(( $i % 20 )) == 1 ]]; then
      if [[ $1 == true ]]; then
        doris_note "MASTER FE is not started. retry."
      else
        doris_note "BE is not register. retry."
      fi
    fi
    if [[ $1 == true ]]; then
      docker_process_sql <<<"show frontends" | grep "[[:space:]]${MASTER_FE_IP}[[:space:]]"
    else
      docker_process_sql <<<"show backends" | grep "[[:space:]]${BE_HOST_IP}[[:space:]]" | grep "[[:space:]]${BE_HEARTBEAT_PORT}[[:space:]]"
    fi
    be_join_status=$?
    if [[ "${be_join_status}" == 0 ]]; then
      if [[ $1 == true ]]; then
        doris_note "MASTER FE is started!"
      else
        doris_note "Init Check - Verify that BE is registered to FE successfully"
        BE_ALREADY_EXISTS=true
      fi
      break
    fi
    sleep 1
  done
}

_main() {
  docker_setup_env
  docker_required_variables_env
  get_doris_be_args

  if [ -z "$DATABASE_ALREADY_EXISTS" ]; then
    add_priority_networks $PRIORITY_NETWORKS
  fi

  register_be_to_fe
  check_be_status
  doris_note "Ready to start BE！"
  start_be.sh
  exec "$@"
}

if ! _is_sourced; then
  _main "$@"
fi
