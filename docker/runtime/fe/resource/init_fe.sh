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

FE_ID=0
FE_SERVERS=""

ARGS=$(getopt -o -h: --long fe_id:,fe_servers: -n "$0" -- "$@")

eval set -- "${ARGS}"

while [[ -n "$1" ]]; do
    case "$1" in
    --fe_id)
        FE_ID=$2
        shift
        ;;
    --fe_servers)
        FE_SERVERS=$2
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

echo "DEBUG >>>>>> FE_ID = [${FE_ID}]"
echo "DEBUG >>>>>> FE_SERVERS = [${FE_SERVERS}]"

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
    echo "DEBUG >>>>>> tmpFeId = [${tmpFeId}]"
    echo "DEBUG >>>>>> tmpFeIp = [${tmpFeIp}]"
    echo "DEBUG >>>>>> tmpFeEditLogPort = [${tmpFeEditLogPort}]"

    feIpArray[tmpFeId]=${tmpFeIp}
    feEditLogPortArray[tmpFeId]=${tmpFeEditLogPort}

done

echo "DEBUG >>>>>> feIpArray = ${feIpArray[*]}"
echo "DEBUG >>>>>> feEditLogPortArray = ${feEditLogPortArray[*]}"
echo "DEBUG >>>>>> masterFe = ${feIpArray[1]}:${feEditLogPortArray[1]}"
echo "DEBUG >>>>>> currentFe = ${feIpArray[FE_ID]}:${feEditLogPortArray[FE_ID]}"

priority_networks=$(echo "${feIpArray[FE_ID]}" | awk -F '.' '{print$1"."$2"."$3".0/24"}')
echo "DEBUG >>>>>> Append the configuration [priority_networks = ${priority_networks}] to /opt/doris-fe/conf/fe.conf"
echo "priority_networks = ${priority_networks}" >>/opt/apache-doris/fe/conf/fe.conf

if [[ "${FE_ID}" != 1 ]]; then

    ## if current node is not master
    ## PREPARE1: registe follower from mysql client
    ## PREPARE2: call start_fe.sh using --help optional
    ## STEP1: check master fe service works
    ## STEP2: if feMasterStat == true; register PREPARE1 & PREPARE2 [retry 3 times, sleep 10s]

    ## PREPARE1: registe follower from mysql client
    registerMySQL="mysql -uroot -P9030 -h${feIpArray[1]} -e \"alter system add follower '${feIpArray[FE_ID]}:${feEditLogPortArray[FE_ID]}'\""

    ## PREPARE2: call start_fe.sh using --help optional
    registerShell="/opt/apache-doris/fe/bin/start_fe.sh --helper '${feIpArray[1]}:${feEditLogPortArray[1]}'"

    echo "DEBUG >>>>>> FE is follower, fe_id = ${FE_ID}"
    echo "DEBUG >>>>>> registerMySQL = 【${registerMySQL}】"
    echo "DEBUG >>>>>> registerShell = 【${registerShell}】"
    echo "DEBUG >>>>>> feMasterStat =  【mysql -uroot -P9030 -h ${feIpArray[1]} -e \"show frontends\" | grep \"${feIpArray[1]}_9010\" | grep -E \"true[[:space:]]*true\"】"

    ## STEP1: check FE master status

    for ((i = 0; i <= 2000; i++)); do

        ## run STEP1 & STEP2, and then break
        echo "Run registerShell command, [ registerMySQL = ${registerMySQL} ]"
        eval "${registerMySQL}"
        sleep 2

        ## followerJoined: Joined = 0, doesn't join = 1
        mysql -uroot -P9030 -h"${feIpArray[1]}" -e "show frontends" | grep "${feIpArray[FE_ID]}_9010" | grep -E "false[[:space:]]*false"
        followerJoined=$?

        if [[ "${followerJoined}" == 0 ]]; then
            echo "Run registerShell command, [ registerShell = ${registerShell} ]"
            eval "${registerShell}"
            echo "The resutl of run registerShell command, [ res = $? ]"
        fi
        sleep 5
    done

else
    registerShell="/opt/apache-doris/fe/bin/start_fe.sh"
    eval "${registerShell}"
    echo "DEBUG >>>>>> FE is master, fe_id = ${FE_ID}"
    echo "DEBUG >>>>>> registerShell = ${registerShell}"
fi
