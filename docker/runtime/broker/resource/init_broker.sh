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
BROKER_ADDR=""

ARGS=$(getopt -o -h: --long fe_servers:,broker_addr: -n "$0" -- "$@")

eval set -- "${ARGS}"

while [[ -n "$1" ]]; do
    case "$1" in
    --fe_servers)
        FE_SERVERS=$2
        shift
        ;;
    --broker_addr)
        BROKER_ADDR=$2
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
echo "DEBUG >>>>>> BROKER_ADDR=[${BROKER_ADDR}]"

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

broker_name=$(echo "${BROKER_ADDR}" | awk -F ':' '{ sub(/ /, ""); print$1}')
broker_ip=$(echo "${BROKER_ADDR}" | awk -F ':' '{ sub(/ /, ""); print$2}')
broker_ipc_port=$(echo "${BROKER_ADDR}" | awk -F ':' '{ sub(/ /, ""); print$3}')

echo "DEBUG >>>>>> feIpArray = ${feIpArray[*]}"
echo "DEBUG >>>>>> feEditLogPortArray = ${feEditLogPortArray[*]}"
echo "DEBUG >>>>>> masterFe = ${feIpArray[1]}:${feEditLogPortArray[1]}"
echo "DEBUG >>>>>> broker_addr = ${broker_ip}:${broker_ipc_port}"

dropMySQL="/usr/bin/mysql -uroot -P9030 -h${feIpArray[1]} -e \"alter system drop broker ${broker_name} '${broker_ip}:${broker_ipc_port}'\""
echo "DEBUG >>>>>> dropMySQL = ${dropMySQL}"
eval "${dropMySQL}" && echo "DEBUG >>>>>> drop history registe SUCCESS!" || echo "DEBUG >>>>>> drop history registe FAILED!"

# register broker to FE through mysql
registerMySQL="/usr/bin/mysql -uroot -P9030 -h${feIpArray[1]} -e \"alter system add broker ${broker_name} '${broker_ip}:${broker_ipc_port}'\""
echo "DEBUG >>>>>> registerMySQL = ${registerMySQL}"
eval "${registerMySQL}" && echo "DEBUG >>>>>> mysql register is SUCCESS!" || echo "DEBUG >>>>>> mysql register is FAILED!"

# start broker
registerShell="/opt/apache-doris/broker/bin/start_broker.sh &"
echo "DEBUG >>>>>> registerShell = ${registerShell}"
eval "${registerShell}" && echo "DEBUG >>>>>> start_broker SUCCESS！" || echo "DEBUG >>>>>> start_broker FAILED!"

for ((i = 0; i <= 20; i++)); do
    sleep 10
    ## check broker register status
    echo "DEBUG >>>>>> run commnad mysql -uroot -P9030 -h${feIpArray[1]} -e \"show proc '/brokers'\" | grep \" ${broker_ip} \" | grep \" ${broker_ipc_port} \" | grep \" true \""
    mysql -uroot -P9030 -h"${feIpArray[1]}" -e "show proc '/brokers'" | grep "[[:space:]]${broker_ip}[[:space:]]" | grep "[[:space:]]${broker_ipc_port}[[:space:]]" | grep "[[:space:]]true[[:space:]]"
    broker_join_status=$?
    echo "DEBUG >>>>>> The " "${i}" "time to register Broker node, broker_join_status=${broker_join_status}"
    if [[ "${broker_join_status}" == 0 ]]; then
        ## broker registe successfully
        echo "BROKER START SUCCESS!!!"
        break
    else
        ## broker doesn't registe
        echo "DEBUG >>>>>> run commnad ${registerMySQL}"
        eval "${registerMySQL}"
        if [[ "${i}" == 20 ]]; then
            echo "DEBUG >>>>>> Broker Start Or Register FAILED!"
        fi
        sleep 3
    fi
done
