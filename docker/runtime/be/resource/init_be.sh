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

FE_SERVERS=""
BE_ADDR=""

ARGS=$(getopt -o -h: --long fe_servers:,be_addr: -n "$0" -- "$@")

eval set -- "${ARGS}"

while [[ -n "$1" ]]; do
    case "$1" in
    --fe_servers)
        FE_SERVERS=$2
        shift
        ;;
    --be_addr)
        BE_ADDR=$2
        shift
        ;;
    --) ;;

    *)
        echo "Error option $1"
        break
        ;;
    esac
    shift
done

#echo FE_SERVERS = $FE_SERVERS
echo "DEBUG >>>>>> FE_SERVERS=[${FE_SERVERS}]"
echo "DEBUG >>>>>> BE_ADDR=[${BE_ADDR}]"

feIpArray=()
feEditLogPortArray=()

IFS=","
# shellcheck disable=SC2206
feServerArray=(${FE_SERVERS})

for i in "${!feServerArray[@]}"; do
    val=${feServerArray[i]}
    val=${val// /}
    tmpFeId=$(echo "${val}" | awk -F ':' '{ sub(/fe/, ""); sub(/ /, ""); print$1}')
    tmpFeIp=$(echo "${val}" | awk -F ':' '{ sub(/ /, ""); print$2}')
    tmpFeEditLogPort=$(echo "${val}" | awk -F ':' '{ sub(/ /, ""); print$3}')
    feIpArray[tmpFeId]=${tmpFeIp}
    feEditLogPortArray[tmpFeId]=${tmpFeEditLogPort}
done

be_ip=$(echo "${BE_ADDR}" | awk -F ':' '{ sub(/ /, ""); print$1}')
be_heartbeat_port=$(echo "${BE_ADDR}" | awk -F ':' '{ sub(/ /, ""); print$2}')

echo "DEBUG >>>>>> feIpArray = ${feIpArray[*]}"
echo "DEBUG >>>>>> feEditLogPortArray = ${feEditLogPortArray[*]}"
echo "DEBUG >>>>>> masterFe = ${feIpArray[1]}:${feEditLogPortArray[1]}"
echo "DEBUG >>>>>> be_addr = ${be_ip}:${be_heartbeat_port}"

priority_networks=$(echo "${be_ip}" | awk -F '.' '{print$1"."$2"."$3".0/24"}')
echo "DEBUG >>>>>> Append the configuration [priority_networks = ${priority_networks}] to /opt/apache-doris/be/conf/fe.conf"
echo "priority_networks = ${priority_networks}" >>/opt/apache-doris/be/conf/be.conf

registerMySQL="mysql -uroot -P9030 -h${feIpArray[1]} -e \"alter system add backend '${be_ip}:${be_heartbeat_port}'\""
echo "DEBUG >>>>>> registerMySQL = ${registerMySQL}"

registerShell="/opt/apache-doris/be/bin/start_be.sh &"
echo "DEBUG >>>>>> registerShell = ${registerShell}"

for ((i = 0; i <= 20; i++)); do

    ## check be register status
    echo "mysql -uroot -P9030 -h${feIpArray[1]} -e \"show backends\" | grep \" ${be_ip} \" | grep \" ${be_heartbeat_port} \""
    mysql -uroot -P9030 -h"${feIpArray[1]}" -e "show backends" | grep "[[:space:]]${be_ip}[[:space:]]" | grep "[[:space:]]${be_heartbeat_port}[[:space:]]"
    be_join_status=$?
    echo "DEBUG >>>>>> The " "${i}" "time to register BE node, be_join_status=${be_join_status}"
    if [[ "${be_join_status}" == 0 ]]; then
        ## be registe successfully
        echo "DEBUG >>>>>> run command ${registerShell}"
        eval "${registerShell}"
    else
        ## be doesn't registe
        echo "DEBUG >>>>>> run commnad ${registerMySQL}"
        eval "${registerMySQL}"
        if [[ "${i}" == 20 ]]; then
            echo "DEBUG >>>>>> BE Start Or Register FAILED!"
        fi
        sleep 5
    fi
done
