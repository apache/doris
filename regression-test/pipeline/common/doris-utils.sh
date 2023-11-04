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
    if [[ ! -d "${DORIS_HOME:-}" ]]; then return 1; fi
    if ! java -version >/dev/null; then sudo apt install openjdk-8-jdk -y >/dev/null; fi
    JAVA_HOME="$(find /usr/lib/jvm -maxdepth 1 -type d -name 'java-8-*' | sed -n '1p')"
    export JAVA_HOME
    "${DORIS_HOME}"/fe/bin/start_fe.sh --daemon

    if ! mysql --version >/dev/null; then sudo apt install -y mysql-client; fi
    query_port=$(get_doris_conf_value "${DORIS_HOME}"/fe/conf/fe.conf query_port)
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
    if [[ ! -d "${DORIS_HOME:-}" ]]; then return 1; fi
    if ! java -version >/dev/null; then sudo apt install openjdk-8-jdk -y >/dev/null; fi
    JAVA_HOME="$(find /usr/lib/jvm -maxdepth 1 -type d -name 'java-8-*' | sed -n '1p')"
    export JAVA_HOME
    sysctl -w vm.max_map_count=2000000 &&
        ulimit -n 200000 &&
        ulimit -c unlimited &&
        swapoff -a &&
        "${DORIS_HOME}"/be/bin/start_be.sh --daemon

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
        echo "INFO: doris be started, be version: $("${DORIS_HOME}"/be/lib/doris_be --version)"
    fi
}

function add_doris_be_to_fe() {
    if [[ ! -d "${DORIS_HOME:-}" ]]; then return 1; fi
    if ! mysql --version >/dev/null; then sudo apt install -y mysql-client; fi
    query_port=$(get_doris_conf_value "${DORIS_HOME}"/fe/conf/fe.conf query_port)
    heartbeat_service_port=$(get_doris_conf_value "${DORIS_HOME}"/be/conf/be.conf heartbeat_service_port)
    cl="mysql -h127.0.0.1 -P${query_port} -uroot "
    if ${cl} -e "ALTER SYSTEM ADD BACKEND '127.0.0.1:${heartbeat_service_port}';"; then echo; else echo; fi

    i=1
    while [[ $((i++)) -lt 60 ]]; do
        if be_ready_count=$(${cl} -e 'show backends\G' | grep -c 'Alive: true') &&
            [[ ${be_ready_count} -eq 1 ]]; then
            echo -e "INFO: add doris be success, be version: \n$(${cl} -e 'show backends\G' | grep 'Version')" && break
        else
            echo 'Wait for Backends ready, sleep 2 seconds ...' && sleep 2
        fi
    done
    if [[ ${i} -eq 60 ]]; then echo "ERROR: Add Doris Backend Failed after 2 mins wait..." && return 1; fi
}

function stop_doris() {
    if [[ ! -d "${DORIS_HOME:-}" ]]; then return 1; fi
    if "${DORIS_HOME}"/fe/bin/stop_fe.sh &&
        "${DORIS_HOME}"/be/bin/start_be.sh; then
        echo "INFO: normally stoped doris"
    else
        pgrep -fi doris | xargs kill -9
        echo "WARNING: force stoped doris"
    fi
}

function check_tpch_table_rows() {
    if [[ ! -d "${DORIS_HOME:-}" ]]; then return 1; fi
    db_name="$1"
    scale_factor="$2"
    if [[ -z "${scale_factor}" ]]; then return 1; fi

    query_port=$(get_doris_conf_value "${DORIS_HOME}"/fe/conf/fe.conf query_port)
    cl="mysql -h127.0.0.1 -P${query_port} -uroot "
    declare -A table_rows
    if [[ "${scale_factor}" == "100" ]]; then
        table_rows=(['region']=5 ['nation']=25 ['supplier']=1000000 ['customer']=15000000 ['part']=20000000 ['partsupp']=80000000 ['orders']=150000000 ['lineitem']=600037902)
    else
        table_rows=(['region']=5 ['nation']=25 ['supplier']=10000 ['customer']=150000 ['part']=200000 ['partsupp']=800000 ['orders']=1500000 ['lineitem']=6001215)
    fi
    for table in ${!table_rows[*]}; do
        rows_actual=$(${cl} -D"${db_name}" -e"SELECT count(*) FROM ${table}" | sed -n '2p')
        rows_expect=${table_rows[${table}]}
        if [[ ${rows_actual} -ne ${rows_expect} ]]; then
            echo "WARNING: ${table} actual rows: ${rows_actual}, expect rows: ${rows_expect}" && return 1
        fi
    done
}

get_session_variable() {
    if [[ ! -d "${DORIS_HOME:-}" ]]; then return 1; fi
    usage="
    usage:
        get_session_variable SESSION_VARIABLE
        return the value of the SESSION_VARIABLE
    "
    if [[ -z "$1" ]]; then echo "${usage}" && return 1; else sv="$1"; fi

    query_port=$(get_doris_conf_value "${DORIS_HOME}"/fe/conf/fe.conf query_port)
    cl="mysql -h127.0.0.1 -P${query_port} -uroot "

    if ret=$(${cl} -e"show variables like '${sv}'\G" | grep "Value: "); then
        echo "${ret/*Value: /}"
    else
        return 1
    fi
}

set_session_variables_from_file() {
    usage="
    usage:
        set_session_variables_from_file FILE
        FILE content lile '
        session_variable_key session_variable_value
        ...
        '
    "
    if [[ ! -d "${DORIS_HOME:-}" ]]; then return 1; fi
    if [[ -z "$1" ]]; then echo "${usage}" && return 1; else sv_file="$1"; fi

    query_port=$(get_doris_conf_value "${DORIS_HOME}"/fe/conf/fe.conf query_port)
    cl="mysql -h127.0.0.1 -P${query_port} -uroot "

    ret=0
    while read -r sv; do
        if [[ "${sv}" == "#"* ]]; then continue; fi
        k=$(echo "${sv}" | awk '{print $1}')
        v=$(echo "${sv}" | awk '{print $2}' | tr '[:upper:]' '[:lower:]')
        if ${cl} -e"set global ${k}=${v};"; then
            if [[ "$(get_session_variable "${k}" | tr '[:upper:]' '[:lower:]')" == "${v}" ]]; then
                echo "INFO:      set global ${k}=${v};"
            else
                echo "ERROR:     set global ${k}=${v};" && ret=1
            fi
        else
            ret=1
        fi
    done <"${sv_file}"
    return "${ret}"
}

set_session_variable() {
    if [[ ! -d "${DORIS_HOME:-}" ]]; then return 1; fi
    k="$1"
    v="$2"
    if [[ -z "${v}" ]]; then return 1; fi
    query_port=$(get_doris_conf_value "${DORIS_HOME}"/fe/conf/fe.conf query_port)
    cl="mysql -h127.0.0.1 -P${query_port} -uroot "
    if ${cl} -e"set global ${k}=${v};"; then
        if [[ "$(get_session_variable "${k}" | tr '[:upper:]' '[:lower:]')" == "${v}" ]]; then
            echo "INFO:      set global ${k}=${v};"
        else
            echo "ERROR:     set global ${k}=${v};" && return 1
        fi
    else
        return 1
    fi
}
