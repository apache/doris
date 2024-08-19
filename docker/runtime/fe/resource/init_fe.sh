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
    declare -g DATABASE_ALREADY_EXISTS BUILD_TYPE_K8S PRIORITY_NETWORKS_EXISTS
    if [ -d "${DORIS_HOME}/fe/doris-meta/image" ]; then
        doris_note "the image is exsit!"
        DATABASE_ALREADY_EXISTS='true'
    fi
    if grep -q "$PRIORITY_NETWORKS" "${DORIS_HOME}/fe/conf/fe.conf" ; then
        doris_note "the priority_networks is exsit!"
        PRIORITY_NETWORKS_EXISTS='true'
    else
        doris_note "the priority_networks values is $PRIORITY_NETWORKS"
        doris_note "the conf file path is ${DORIS_HOME}/fe/conf/fe.conf"
    fi
}

# Check the variables required for   startup
docker_required_variables_env() {
    declare -g RUN_TYPE
    if [ -n "$BUILD_TYPE" ]; then
        RUN_TYPE="K8S"
        if [[ $BUILD_TYPE =~ ^([kK]8[sS])$ ]]; then
            doris_warn "BUILD_TYPE" $BUILD_TYPE
        else
            doris_error "BUILD_TYPE rule error！example: [k8s], Default Value: docker."
        fi
        return
    fi

    if [[ -n "$FE_SERVERS" && -n "$BE_SERVERS" && -n "$FE_ID"  && -n "$FQDN" ]]; then
        RUN_TYPE="FQDN"
        if [[ $FE_SERVERS =~ ^[a-zA-Z].+:[1-2]{0,1}[0-9]{0,1}[0-9]{1}(\.[1-2]{0,1}[0-9]{0,1}[0-9]{1}){3}:[1-6]{0,1}[0-9]{1,4}(,[a-zA-Z]+\w+:[1-2]{0,1}[0-9]{0,1}[0-9]{1}(\.[1-2]{0,1}[0-9]{0,1}[0-9]{1}){3}:[1-6]{0,1}[0-9]{1,4})*$ || $FE_SERVERS =~ ^([0-9a-fA-F]{1,4}:){7,7}([0-9a-fA-F]{1,4}|:)|([0-9a-fA-F]{1,4}:){1,6}(:[0-9a-fA-F]{1,4}|:)|([0-9a-fA-F]{1,4}:){1,5}((:[0-9a-fA-F]{1,4}){1,2}|:)|([0-9a-fA-F]{1,4}:){1,4}((:[0-9a-fA-F]{1,4}){1,3}|:)|([0-9a-fA-F]{1,4}:){1,3}((:[0-9a-fA-F]{1,4}){1,4}|:)|([0-9a-fA-F]{1,4}:){1,2}((:[0-9a-fA-F]{1,4}){1,5}|:)|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6}|:)|:((:[0-9a-fA-F]{1,4}){1,7}|:)$ ]]; then
            doris_warn "FE_SERVERS " $FE_SERVERS
        else
            doris_error "FE_SERVERS rule error！example: \$FE_NAME:\$FE_HOST_IP:\$FE_EDIT_LOG_PORT[,\$FE_NAME:\$FE_HOST_IP:\$FE_EDIT_LOG_PORT]... AND FE_NAME Ruler is '[a-zA-Z].+'!"
        fi
        if [[ $FE_ID =~ ^[1-9]{1}$ ]]; then
            doris_warn "FE_ID " $FE_ID
        else
            doris_error "FE_ID rule error！If FE is the role of Master, please set FE_ID=1, and ensure that all IDs correspond to the IP of the current node, ID start num is 1."
        fi
        if [[ $BE_SERVERS =~ ^[a-zA-Z].+:[1-2]{0,1}[0-9]{0,1}[0-9]{1}(\.[1-2]{0,1}[0-9]{0,1}[0-9]{1}){3}(,[a-zA-Z]+\w+:[1-2]{0,1}[0-9]{0,1}[0-9]{1}(\.[1-2]{0,1}[0-9]{0,1}[0-9]{1}){3})*$ || $FE_SERVERS =~ ^([0-9a-fA-F]{1,4}:){7,7}([0-9a-fA-F]{1,4}|:)|([0-9a-fA-F]{1,4}:){1,6}(:[0-9a-fA-F]{1,4}|:)|([0-9a-fA-F]{1,4}:){1,5}((:[0-9a-fA-F]{1,4}){1,2}|:)|([0-9a-fA-F]{1,4}:){1,4}((:[0-9a-fA-F]{1,4}){1,3}|:)|([0-9a-fA-F]{1,4}:){1,3}((:[0-9a-fA-F]{1,4}){1,4}|:)|([0-9a-fA-F]{1,4}:){1,2}((:[0-9a-fA-F]{1,4}){1,5}|:)|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6}|:)|:((:[0-9a-fA-F]{1,4}){1,7}|:)$ ]]; then
            doris_warn "BE_SERVERS " $BE_SERVERS
        else
            doris_error "BE_SERVERS rule error！example: \$BE_NODE_NAME:\$BE_HOST_IP[,\$BE_NODE_NAME:\$BE_HOST_IP:]... AND BE_NODE_NAME Ruler is '[a-zA-Z].+'!"
        fi
        return
    fi

    if [[ -n "$RECOVERY" && -n "$FE_SERVERS" && -n "$FE_ID" ]]; then
        RUN_TYPE="RECOVERY"
        if [[ $RECOVERY =~ true ]]; then
            doris_warn "RECOVERY " $RECOVERY
        else
            doris_error "RECOVERY value error! Only Support 'true'!"
        fi
        if [[ $FE_SERVERS =~ ^.+:[1-2]{0,1}[0-9]{0,1}[0-9]{1}(\.[1-2]{0,1}[0-9]{0,1}[0-9]{1}){3}:[1-6]{0,1}[0-9]{1,4}(,.+:[1-2]{0,1}[0-9]{0,1}[0-9]{1}(\.[1-2]{0,1}[0-9]{0,1}[0-9]{1}){3}:[1-6]{0,1}[0-9]{1,4})*$ || $FE_SERVERS =~ ^([0-9a-fA-F]{1,4}:){7,7}([0-9a-fA-F]{1,4}|:)|([0-9a-fA-F]{1,4}:){1,6}(:[0-9a-fA-F]{1,4}|:)|([0-9a-fA-F]{1,4}:){1,5}((:[0-9a-fA-F]{1,4}){1,2}|:)|([0-9a-fA-F]{1,4}:){1,4}((:[0-9a-fA-F]{1,4}){1,3}|:)|([0-9a-fA-F]{1,4}:){1,3}((:[0-9a-fA-F]{1,4}){1,4}|:)|([0-9a-fA-F]{1,4}:){1,2}((:[0-9a-fA-F]{1,4}){1,5}|:)|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6}|:)|:((:[0-9a-fA-F]{1,4}){1,7}|:)$ ]]; then
            doris_warn "FE_SERVERS " $FE_SERVERS
        else
            doris_error "FE_SERVERS rule error！example: \$FE_NAME:\$FE_HOST_IP:\$FE_EDIT_LOG_PORT[,\$FE_NAME:\$FE_HOST_IP:\$FE_EDIT_LOG_PORT]..."
        fi
        if [[ $FE_ID =~ ^[1-9]{1}$ ]]; then
            doris_warn "FE_ID" $FE_ID
        else
            doris_error "FE_ID rule error！If FE is the role of Master, please set FE_ID=1, and ensure that all IDs correspond to the IP of the current node."
        fi
        doris_warn "The Frontend MetaData Will Recovery."
        return
    fi

    if [[ -n "$FE_SERVERS" && -n "$FE_ID" ]]; then
        RUN_TYPE="ELECTION"
        if [[ $FE_SERVERS =~ ^.+:[1-2]{0,1}[0-9]{0,1}[0-9]{1}(\.[1-2]{0,1}[0-9]{0,1}[0-9]{1}){3}:[1-6]{0,1}[0-9]{1,4}(,.+:[1-2]{0,1}[0-9]{0,1}[0-9]{1}(\.[1-2]{0,1}[0-9]{0,1}[0-9]{1}){3}:[1-6]{0,1}[0-9]{1,4})*$ || $FE_SERVERS =~ ^([0-9a-fA-F]{1,4}:){7,7}([0-9a-fA-F]{1,4}|:)|([0-9a-fA-F]{1,4}:){1,6}(:[0-9a-fA-F]{1,4}|:)|([0-9a-fA-F]{1,4}:){1,5}((:[0-9a-fA-F]{1,4}){1,2}|:)|([0-9a-fA-F]{1,4}:){1,4}((:[0-9a-fA-F]{1,4}){1,3}|:)|([0-9a-fA-F]{1,4}:){1,3}((:[0-9a-fA-F]{1,4}){1,4}|:)|([0-9a-fA-F]{1,4}:){1,2}((:[0-9a-fA-F]{1,4}){1,5}|:)|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6}|:)|:((:[0-9a-fA-F]{1,4}){1,7}|:)$ ]]; then
            doris_warn "FE_SERVERS" $FE_SERVERS
        else
            doris_error "FE_SERVERS rule error！example: \$FE_NAME:\$FE_HOST_IP:\$FE_EDIT_LOG_PORT[,\$FE_NAME:\$FE_HOST_IP:\$FE_EDIT_LOG_PORT]..."
        fi
        if [[ $FE_ID =~ ^[1-9]{1}$ ]]; then
            doris_warn "FE_ID" $FE_ID
        else
            doris_error "FE_ID rule error！If FE is the role of Master, please set FE_ID=1, and ensure that all IDs correspond to the IP of the current node."
        fi
        return
    fi

    if [[ -n "$FE_MASTER_IP" && -n "$FE_MASTER_PORT" && -n "$FE_CURRENT_IP" && -n "$FE_CURRENT_PORT" ]]; then
        RUN_TYPE="ASSIGN"
        if [[ $FE_MASTER_IP =~ ^[1-2]{0,1}[0-9]{0,1}[0-9]{1}(\.[1-2]{0,1}[0-9]{0,1}[0-9]{1}){3}$ || $FE_MASTER_IP =~ ^([0-9a-fA-F]{1,4}:){7,7}([0-9a-fA-F]{1,4}|:)|([0-9a-fA-F]{1,4}:){1,6}(:[0-9a-fA-F]{1,4}|:)|([0-9a-fA-F]{1,4}:){1,5}((:[0-9a-fA-F]{1,4}){1,2}|:)|([0-9a-fA-F]{1,4}:){1,4}((:[0-9a-fA-F]{1,4}){1,3}|:)|([0-9a-fA-F]{1,4}:){1,3}((:[0-9a-fA-F]{1,4}){1,4}|:)|([0-9a-fA-F]{1,4}:){1,2}((:[0-9a-fA-F]{1,4}){1,5}|:)|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6}|:)|:((:[0-9a-fA-F]{1,4}){1,7}|:)$ ]]; then
            doris_warn "FE_MASTER_IP" $FE_MASTER_IP
        else
            doris_error "FE_MASTER_IP rule error！example: \$FE_MASTER_IP"
        fi
        if [[ $FE_MASTER_PORT =~ ^[1-6]{0,1}[0-9]{1,4}$ ]]; then
            doris_warn "FE_MASTER_PORT" $FE_MASTER_PORT
        else
            doris_error "FE_MASTER_PORT rule error！example: \$FE_MASTER_EDIT_LOG_HOST."
        fi
        if [[ $FE_CURRENT_IP =~ ^[1-2]{0,1}[0-9]{0,1}[0-9]{1}(\.[1-2]{0,1}[0-9]{0,1}[0-9]{1}){3}$ || $FE_CURRENT_IP =~ ^([0-9a-fA-F]{1,4}:){7,7}([0-9a-fA-F]{1,4}|:)|([0-9a-fA-F]{1,4}:){1,6}(:[0-9a-fA-F]{1,4}|:)|([0-9a-fA-F]{1,4}:){1,5}((:[0-9a-fA-F]{1,4}){1,2}|:)|([0-9a-fA-F]{1,4}:){1,4}((:[0-9a-fA-F]{1,4}){1,3}|:)|([0-9a-fA-F]{1,4}:){1,3}((:[0-9a-fA-F]{1,4}){1,4}|:)|([0-9a-fA-F]{1,4}:){1,2}((:[0-9a-fA-F]{1,4}){1,5}|:)|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6}|:)|:((:[0-9a-fA-F]{1,4}){1,7}|:)$ ]]; then
            doris_warn "FE_CURRENT_IP" $FE_CURRENT_IP
        else
            doris_error "FE_CURRENT_IP rule error！example: \$FE_CURRENT_IP"
        fi
        if [[ $FE_CURRENT_PORT =~ ^[1-6]{0,1}[0-9]{1,4}$ ]]; then
            doris_warn "FE_CURRENT_PORT" $FE_CURRENT_PORT
        else
            doris_error "FE_CURRENT_PORT rule error！example: \$FE_CURRENT_EDIT_LOG_HOST."
        fi
        return
    fi

    doris_error EOF "
                 Note that you did not configure the required parameters!
                 plan 1:
                 BUILD_TYPE
                 plan 2:
                 RECOVERY & FE_SERVERS & FE_ID
                 plan 3:
                 FE_SERVERS & FE_ID
                 plan 4:
                 FE_SERVERS & FE_ID & BE_SERVERS & FQDN
                 plan 5:
                 FE_MASTER_IP & FE_MASTER_PORT & FE_CURRENT_IP & FE_CURRENT_PORT"
                EOF

}

get_doris_fe_args() {
    declare -g MASTER_FE_IP CURRENT_FE_IP CURRENT_NODE_NAME MASTER_FE_EDIT_PORT MASTER_NODE_NAME CURRENT_FE_EDIT_PORT PRIORITY_NETWORKS CURRENT_FE_IS_MASTER FE_HOSTS_MSG BE_HOSTS_MSG
    if [[ $RUN_TYPE == "ELECTION" || $RUN_TYPE == "RECOVERY" ]]; then
        local feServerArray=($(echo "${FE_SERVERS}" | awk '{gsub (/,/," "); print $0}'))
        for i in "${feServerArray[@]}"; do
            val=${i}
            val=${val// /}
            tmpFeName=$(echo "${val}" | awk -F ':' '{ sub(/ /, ""); print$1}')
            tmpFeIp=$(echo "${val}" | awk -F ':' '{ sub(/ /, ""); print$2}')
            tmpFeEditLogPort=$(echo "${val}" | awk -F ':' '{ sub(/ /, ""); print$3}')
            check_arg "TMP_FE_IP" $tmpFeIp
            feIpArray+=("$tmpFeIp")
            check_arg "TMP_FE_EDIT_LOG_PORT" $tmpFeEditLogPort
            feEditLogPortArray+=("$tmpFeEditLogPort")
        done

        MASTER_FE_IP=${feIpArray[0]}
        check_arg "MASTER_FE_IP" $MASTER_FE_IP
        MASTER_FE_EDIT_PORT=${feEditLogPortArray[0]}
        check_arg "MASTER_FE_EDIT_PORT" $MASTER_FE_EDIT_PORT
        CURRENT_FE_IP=${feIpArray[$FE_ID-1]}
        check_arg "CURRENT_FE_IP" $CURRENT_FE_IP
        CURRENT_FE_EDIT_PORT=${feEditLogPortArray[$FE_ID-1]}
        check_arg "CURRENT_FE_EDIT_PORT" $CURRENT_FE_EDIT_PORT

        if [ ${MASTER_FE_IP} == ${CURRENT_FE_IP} ]; then
            CURRENT_FE_IS_MASTER=true
        else
            CURRENT_FE_IS_MASTER=false
        fi

        PRIORITY_NETWORKS=$(echo "${CURRENT_FE_IP}" | awk -F '.' '{print$1"."$2"."$3".0/24"}')
        check_arg "PRIORITY_NETWORKS" $PRIORITY_NETWORKS

        doris_note "FE_IP_ARRAY = [${feIpArray[*]}]"
        doris_note "FE_EDIT_LOG_PORT_ARRAY = [${feEditLogPortArray[*]}]"
        doris_note "MASTER_FE = ${MASTER_FE_IP}:${MASTER_FE_EDIT_PORT}"
        doris_note "CURRENT_FE = ${CURRENT_FE_IP}:${CURRENT_FE_EDIT_PORT}"
        doris_note "PRIORITY_NETWORKS = ${PRIORITY_NETWORKS}"

    elif [[ $RUN_TYPE == "FQDN" ]]; then
        local feServerArray=($(echo "${FE_SERVERS}" | awk '{gsub (/,/," "); print $0}'))
        local beServerArray=($(echo "${BE_SERVERS}" | awk '{gsub (/,/," "); print $0}'))
        feIpArray=()
        feEditLogPortArray=()
        feNameArray=()
        beIpArray=()
        beNameArray=()
        for ((i=0; i<${#feServerArray[@]}; i++)); do
            val=${feServerArray[i]}
            val=${val// /}
            tmpFeName=$(echo "${val}" | awk -F ':' '{ sub(/ /, ""); print$1}')
            tmpFeIp=$(echo "${val}" | awk -F ':' '{ sub(/ /, ""); print$2}')
            tmpFeEditLogPort=$(echo "${val}" | awk -F ':' '{ sub(/ /, ""); print$3}')
            check_arg "TMP_FE_NAME" $tmpFeName
            feNameArray+=("$tmpFeName")
            check_arg "TMP_FE_IP" $tmpFeIp
            feIpArray+=("$tmpFeIp")
            check_arg "TMP_FE_EDIT_LOG_PORT" $tmpFeEditLogPort
            feEditLogPortArray+=("$tmpFeEditLogPort")
            FE_HOSTS_MSG+=$(printf "%s\t%s" $tmpFeIp $tmpFeName)
            if [ $i -lt $((${#feServerArray[@]}-1)) ]; then
                FE_HOSTS_MSG+='\n'
            fi
        done
        for ((i=0; i<${#beServerArray[@]}; i++)); do
            val=${beServerArray[i]}
            val=${val// /}
            tmpBeName=$(echo "${val}" | awk -F ':' '{ sub(/fe/, ""); sub(/ /, ""); print$1}')
            tmpBeIp=$(echo "${val}" | awk -F ':' '{ sub(/ /, ""); print$2}')
            check_arg "TMP_BE_NAME" $tmpBeName
            beNameArray+=("$tmpBeName")
            check_arg "TMP_BE_IP" $tmpBeIp
            beIpArray+=("$tmpBeIp")
            BE_HOSTS_MSG+=$(printf "%s\t%s" $tmpBeIp $tmpBeName)
            if [ $i -lt $((${#beServerArray[@]}-1)) ]; then
                BE_HOSTS_MSG+='\n'
            fi
        done

        MASTER_FE_IP=${feIpArray[0]}
        check_arg "MASTER_FE_IP" $MASTER_FE_IP
        MASTER_FE_EDIT_PORT=${feEditLogPortArray[0]}
        check_arg "MASTER_FE_EDIT_PORT" $MASTER_FE_EDIT_PORT
        MASTER_NODE_NAME=${feNameArray[0]}
        check_arg "MASTER_NODE_NAME" $MASTER_NODE_NAME
        CURRENT_NODE_NAME=${feNameArray[$FE_ID-1]}
        check_arg "CURRENT_NODE_NAME" $CURRENT_NODE_NAME
        CURRENT_FE_IP=${feIpArray[$FE_ID-1]}
        check_arg "CURRENT_FE_IP" $CURRENT_FE_IP
        CURRENT_FE_EDIT_PORT=${feEditLogPortArray[$FE_ID-1]}
        check_arg "CURRENT_FE_EDIT_PORT" $CURRENT_FE_EDIT_PORT

        if [ "${MASTER_FE_IP}" == "${CURRENT_FE_IP}" ]; then
            CURRENT_FE_IS_MASTER=true
        else
            CURRENT_FE_IS_MASTER=false
        fi

        # Print arrays with desired format
        doris_note "FE_HOSTS_MSG = [\n${FE_HOSTS_MSG}\n]"
        doris_note "BE_HOSTS_MSG = [\n${BE_HOSTS_MSG}\n]"
        doris_note "FE_NAME_ARRAY = [${feNameArray[*]}]"
        doris_note "FE_IP_ARRAY = [${feIpArray[*]}]"
        doris_note "FE_EDIT_LOG_PORT_ARRAY = [${feEditLogPortArray[*]}]"
        doris_note "MASTER_FE = ${MASTER_FE_IP}:${MASTER_FE_EDIT_PORT}"
        doris_note "MASTEMASTER_NODE_NAMER_FE = ${MASTER_NODE_NAME}"
        doris_note "CURRENT_NODE_NAME = ${CURRENT_NODE_NAME}"
        doris_note "CURRENT_FE = ${CURRENT_FE_IP}:${CURRENT_FE_EDIT_PORT}"

    elif [[ $RUN_TYPE == "ASSIGN" ]]; then
        MASTER_FE_IP=${FE_MASTER_IP}
        check_arg "MASTER_FE_IP" $MASTER_FE_IP
        MASTER_FE_EDIT_PORT=${FE_MASTER_PORT}
        check_arg "MASTER_FE_EDIT_PORT" $MASTER_FE_EDIT_PORT
        CURRENT_FE_IP=${FE_CURRENT_IP}
        check_arg "CURRENT_FE_IP" $CURRENT_FE_IP
        CURRENT_FE_EDIT_PORT=${FE_CURRENT_PORT}
        check_arg "CURRENT_FE_EDIT_PORT" $CURRENT_FE_EDIT_PORT

        if [ ${MASTER_FE_IP} == ${CURRENT_FE_IP} ]; then
            CURRENT_FE_IS_MASTER=true
        else
            CURRENT_FE_IS_MASTER=false
        fi

        PRIORITY_NETWORKS=$(echo "${CURRENT_FE_IP}" | awk -F '.' '{print$1"."$2"."$3".0/24"}')
        check_arg "PRIORITY_NETWORKS" $PRIORITY_NETWORKS
    fi

    # check fe start
    if [[ $RUN_TYPE == "ELECTION" || $RUN_TYPE == "ASSIGN" ]]; then
        check_fe_status true
    fi
}

add_priority_networks() {
    doris_note "add priority_networks ${1} to ${DORIS_HOME}/fe/conf/fe.conf"
    echo "priority_networks = ${1}" >>${DORIS_HOME}/fe/conf/fe.conf
}

add_fqdn_conf() {
    doris_note "add ‘enable_fqdn_mode = true’ to ${DORIS_HOME}/fe/conf/fe.conf"
    echo "enable_fqdn_mode = true" >>${DORIS_HOME}/fe/conf/fe.conf
    doris_note "add 'FE hosts msg' \n${FE_HOSTS_MSG} to /etc/hosts"
    echo -e ${FE_HOSTS_MSG} >/etc/hosts
    doris_note "add 'BE hosts msg' \n${BE_HOSTS_MSG} to /etc/hosts"
    echo -e ${BE_HOSTS_MSG} >>/etc/hosts
    doris_note "add 'host_name = ${CURRENT_NODE_NAME}' to /etc/hostname"
    echo ${CURRENT_NODE_NAME} >/etc/hostname
}

# Execute sql script, passed via stdin
# usage: docker_process_sql sql_script
docker_process_sql() {
    set +e
    if [[ $RUN_TYPE == "ELECTION" || $RUN_TYPE == "ASSIGN" ]]; then
        mysql -uroot -P9030 -h${MASTER_FE_IP} --comments "$@" 2>/dev/null
    elif [[ $RUN_TYPE == "FQDN" ]]; then
        mysql -uroot -P9030 -h${MASTER_NODE_NAME} --comments "$@" 2>/dev/null
    fi
}

docker_setup_db() {
    set +e
    # check fe status
    local is_fe_start=false
    if [[ ${CURRENT_FE_IS_MASTER} == true ]]; then
        doris_note "Current FE is Master FE!  No need to register again！"
        return
    fi
    for i in {1..30}; do
        if [[ $RUN_TYPE == "ELECTION" || $RUN_TYPE == "ASSIGN" ]]; then
            docker_process_sql <<<"alter system add FOLLOWER '${CURRENT_FE_IP}:${CURRENT_FE_EDIT_PORT}'"
        elif [[ $RUN_TYPE == "FQDN" ]]; then
            docker_process_sql <<<"alter system add FOLLOWER '${CURRENT_NODE_NAME}:${CURRENT_FE_EDIT_PORT}'"
        fi
        register_fe_status=$?
        if [[ $register_fe_status == 0 ]]; then
            doris_note "FE successfully registered！"
            is_fe_start=true
            break
        else
            if [[ $RUN_TYPE == "ELECTION" || $RUN_TYPE == "ASSIGN" ]]; then
                check_fe_status
            fi
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
    if [[ ${CURRENT_FE_IS_MASTER} == true ]]; then
        doris_note "Current FE is Master FE!  No need check fe status！"
        return
    fi
    for i in {1..30}; do
        if [[ $1 == true ]]; then
            if [[ $RUN_TYPE == "ELECTION" || $RUN_TYPE == "ASSIGN" ]]; then
                docker_process_sql <<<"show frontends" | grep "[[:space:]]${MASTER_FE_IP}[[:space:]]" | grep "[[:space:]]${MASTER_FE_EDIT_PORT}[[:space:]]"
            elif [[ $RUN_TYPE == "FQDN" ]]; then
                docker_process_sql <<<"show frontends" | grep "[[:space:]]${MASTER_NODE_NAME}[[:space:]]" | grep "[[:space:]]${MASTER_FE_EDIT_PORT}[[:space:]]"
            fi
        else
            if [[ $RUN_TYPE == "ELECTION" || $RUN_TYPE == "ASSIGN" ]]; then
                docker_process_sql <<<"show frontends" | grep "[[:space:]]${CURRENT_FE_IP}[[:space:]]" | grep "[[:space:]]${CURRENT_FE_EDIT_PORT}[[:space:]]"
            elif [[ $RUN_TYPE == "FQDN" ]]; then
                docker_process_sql <<<"show frontends" | grep "[[:space:]]${CURRENT_NODE_NAME}[[:space:]]" | grep "[[:space:]]${CURRENT_FE_EDIT_PORT}[[:space:]]"
            fi
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

cleanup() {
    doris_note "Container stopped, running stop_fe script"
    ${DORIS_HOME}/fe/bin/stop_fe.sh
}

_main() {
    docker_required_variables_env
    trap 'cleanup' SIGTERM SIGINT
    if [[ $RUN_TYPE == "K8S" ]]; then
        ${DORIS_HOME}/fe/bin/start_fe.sh --console &
        child_pid=$!
    else
        get_doris_fe_args
        docker_setup_env
        if [[ $DATABASE_ALREADY_EXISTS == "true" && $PRIORITY_NETWORKS_EXISTS != "true" ]]; then
            if [[ $RUN_TYPE == "ELECTION" || $RUN_TYPE == "ASSIGN" || $RUN_TYPE == "RECOVERY" ]]; then
                doris_note "start add_priority_networks"
                add_priority_networks $PRIORITY_NETWORKS
            elif [[ $RUN_TYPE == "FQDN" ]]; then
                doris_note "start add_fqdn_conf"
                add_fqdn_conf
            fi
        fi
        docker_setup_db
        if [[ $RUN_TYPE == "ELECTION" || $RUN_TYPE == "ASSIGN" ]]; then
            check_fe_status
        fi
        doris_note "Ready to start CURRENT_FE！"
        if [ $RECOVERY == true ]; then
            start_fe.sh --console --metadata_failure_recovery &
            child_pid=$!
        fi
        if [[ $CURRENT_FE_IS_MASTER == true ]]; then
            ${DORIS_HOME}/fe/bin/start_fe.sh --console &
            child_pid=$!
        else
            if [[ $RUN_TYPE == "ELECTION" || $RUN_TYPE == "ASSIGN" ]]; then
                ${DORIS_HOME}/fe/bin/start_fe.sh --helper ${MASTER_FE_IP}:${MASTER_FE_EDIT_PORT} --console &
            elif [[ $RUN_TYPE == "FQDN" ]]; then
                ${DORIS_HOME}/fe/bin/start_fe.sh --helper ${MASTER_NODE_NAME}:${MASTER_FE_EDIT_PORT} --console &
            fi
            child_pid=$!
        fi
    fi
    wait $child_pid
    exec "$@"
}

if ! _is_sourced; then
    _main "$@"
fi
