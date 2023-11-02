#!/bin/bash

function get_doris_conf_value() {
    local conf_file="$1"
    local conf_key="$2"
    if [[ -z "${conf_key}" ]]; then return 1; fi

    local conf_value
    if line="$(grep "^${conf_key}" "${conf_file}")"; then
        conf_value="${line#*=}"                      #取第一个等号后面的子串为value
        conf_value="$(echo "${conf_value}" | xargs)" #去掉前导和尾随空格
        echo "${conf_value}"
        return 0
    else
        echo "ERROR: can not find ${conf_key} in ${conf_file}"
        return 1
    fi
}

function set_doris_conf_value() {
    local conf_file="$1"
    local conf_key="$2"
    local conf_value="$3"
    if [[ -z "${conf_value}" ]]; then return 1; fi

    local origin_conf_value
    if origin_conf_value="$(get_conf_value "${conf_file}" "${conf_key}")"; then
        echo "origin_conf_value is ${origin_conf_value}"
        sed -i "/^${conf_key}/d" "${conf_file}"
    fi
    echo "${conf_key}=${conf_value}" | tee -a "${conf_file}"
}

# set -x
# get_doris_conf_value "$1" "$2"
# set_doris_conf_value "$1" "$2" "$3"

function start_doris_fe() {
    local doris_home="$1"
    if [[ ! -d "${doris_home}" ]]; then return 1; fi
    if ! java -version >/dev/null; then sudo apt install openjdk-8-jdk -y >/dev/null; fi
    JAVA_HOME="$(find /usr/lib/jvm -maxdepth 1 -type d -name 'java-8-*' | sed -n '1p')"
    export JAVA_HOME
    "${doris_home}"/fe/bin/start_fe.sh --daemon

    if ! mysql --version >/dev/null; then sudo apt install -y mysql-client; fi
    query_port=$(get_doris_conf_value "${doris_home}"/fe/conf/fe.conf query_port)
    cl="mysql -h127.0.0.1 -P${query_port} -uroot "
    local i=1
    while [[ $((i++)) -lt 60 ]]; do
        fe_version=$(${cl} -e 'show frontends\G' 2>/dev/null | grep -i version | cut -d: -f2)
        if [[ -n "${fe_version}" ]] && [[ "${fe_version}" != "NULL" ]]; then
            echo "INFO: doris fe started, fe version: ${fe_version}" && return 0
        else
            echo "${i}/60, Wait for Frontend ready, sleep 2 seconds ..." && sleep 2
        fi
    done
    if [[ ${i} -ge 60 ]]; then echo "ERROR: Start Doris Frontend Failed after 2 mins wait..." && return 1; fi

}

function start_doris_be() {
    doris_home="$1"
    if [[ ! -d "${doris_home}" ]]; then return 1; fi
    if ! java -version >/dev/null; then sudo apt install openjdk-8-jdk -y >/dev/null; fi
    JAVA_HOME="$(find /usr/lib/jvm -maxdepth 1 -type d -name 'java-8-*' | sed -n '1p')"
    export JAVA_HOME
    sysctl -w vm.max_map_count=2000000 &&
        ulimit -n 200000 &&
        ulimit -c unlimited &&
        "${doris_home}"/be/bin/start_be.sh --daemon

    sleep 2
    local i=1
    while [[ $((i++)) -lt 5 ]]; do
        if ! pgrep -fia doris_be >/dev/null; then
            echo "ERROR: start doris be failed." && return 1
        else
            sleep 2
        fi
    done
    if [[ ${i} -ge 5 ]]; then
        echo "INFO: doris be started, be version: $("${doris_home}"/be/lib/doris_be --version)"
    fi
}

function add_doris_be_to_fe() {
    doris_home="$1"
    if [[ ! -d "${doris_home}" ]]; then return 1; fi
    if ! mysql --version >/dev/null; then sudo apt install -y mysql-client; fi
    query_port=$(get_doris_conf_value "${doris_home}"/fe/conf/fe.conf query_port)
    heartbeat_service_port=$(get_doris_conf_value "${doris_home}"/fe/conf/fe.conf heartbeat_service_port)
    cl="mysql -h127.0.0.1 -P${query_port} -uroot "
    ${cl} -e "ALTER SYSTEM ADD BACKEND '127.0.0.1:${heartbeat_service_port}';"

    i=1
    while [[ $((i++)) -lt 60 ]]; do
        be_ready_count=$(${cl} -e 'show backends\G' | grep -c 'Alive: true')
        if [[ ${be_ready_count} -eq 1 ]]; then
            echo -e "INFO: add doris be success, be version: \n$(${cl} -e 'show backends\G' | grep 'Version')" && break
        else
            echo 'Wait for Backends ready, sleep 2 seconds ...' && sleep 2
        fi
    done
    if [[ ${i} -eq 60 ]]; then echo "ERROR: Add Doris Backend Failed after 2 mins wait..." && return 1; fi
}
