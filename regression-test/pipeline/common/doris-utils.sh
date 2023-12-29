#!/usr/bin/env bash
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
    if ! java -version >/dev/null ||
        [[ -z "$(find /usr/lib/jvm -maxdepth 1 -type d -name 'java-8-*')" ]]; then
        sudo apt update && sudo apt install openjdk-8-jdk -y >/dev/null
    fi
    JAVA_HOME="$(find /usr/lib/jvm -maxdepth 1 -type d -name 'java-8-*' | sed -n '1p')"
    export JAVA_HOME
    "${DORIS_HOME}"/fe/bin/start_fe.sh --daemon

    if ! mysql --version >/dev/null; then sudo apt update && sudo apt install -y mysql-client; fi
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
    if ! java -version >/dev/null ||
        [[ -z "$(find /usr/lib/jvm -maxdepth 1 -type d -name 'java-8-*')" ]]; then
        sudo apt update && sudo apt install openjdk-8-jdk -y >/dev/null
    fi
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
    if ! mysql --version >/dev/null; then sudo sudo apt update && apt install -y mysql-client; fi
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
    if [[ ${i} -ge 60 ]]; then echo "ERROR: Add Doris Backend Failed after 2 mins wait..." && return 1; fi
}

function stop_doris() {
    if "${DORIS_HOME}"/fe/bin/stop_fe.sh &&
        "${DORIS_HOME}"/be/bin/stop_be.sh; then
        echo "INFO: normally stoped doris"
    else
        pgrep -fi doris | xargs kill -9
        echo "WARNING: force stoped doris"
    fi
}

function restart_doris() {
    if stop_doris; then echo; fi
    if ! start_doris_fe; then return 1; fi
    if ! start_doris_be; then return 1; fi
    # wait 10s for doris totally started, otherwize may encounter the error below,
    # ERROR 1105 (HY000) at line 102: errCode = 2, detailMessage = Failed to find enough backend, please check the replication num,replication tag and storage medium.
    sleep 10s
}

function check_tpch_table_rows() {
    if [[ ! -d "${DORIS_HOME:-}" ]]; then return 1; fi
    db_name="$1"
    scale_factor="$2"
    if [[ -z "${scale_factor}" ]]; then return 1; fi

    query_port=$(get_doris_conf_value "${DORIS_HOME}"/fe/conf/fe.conf query_port)
    cl="mysql -h127.0.0.1 -P${query_port} -uroot "
    declare -A table_rows
    if [[ "${scale_factor}" == "1" ]]; then
        table_rows=(['region']=5 ['nation']=25 ['supplier']=10000 ['customer']=150000 ['part']=200000 ['partsupp']=800000 ['orders']=1500000 ['lineitem']=6001215)
    elif [[ "${scale_factor}" == "100" ]]; then
        table_rows=(['region']=5 ['nation']=25 ['supplier']=1000000 ['customer']=15000000 ['part']=20000000 ['partsupp']=80000000 ['orders']=150000000 ['lineitem']=600037902)
    else
        echo "ERROR: unsupported scale_factor ${scale_factor} for tpch" && return 1
    fi
    for table in ${!table_rows[*]}; do
        rows_actual=$(${cl} -D"${db_name}" -e"SELECT count(*) FROM ${table}" | sed -n '2p')
        rows_expect=${table_rows[${table}]}
        if [[ ${rows_actual} -ne ${rows_expect} ]]; then
            echo "ERROR: ${table} actual rows: ${rows_actual}, expect rows: ${rows_expect}" && return 1
        fi
    done
}

function check_tpcds_table_rows() {
    if [[ ! -d "${DORIS_HOME:-}" ]]; then return 1; fi
    db_name="$1"
    scale_factor="$2"
    if [[ -z "${scale_factor}" ]]; then return 1; fi

    query_port=$(get_doris_conf_value "${DORIS_HOME}"/fe/conf/fe.conf query_port)
    cl="mysql -h127.0.0.1 -P${query_port} -uroot "
    declare -A table_rows
    if [[ "${scale_factor}" == "1" ]]; then
        table_rows=(['income_band']=20 ['ship_mode']=20 ['warehouse']=5 ['reason']=35 ['web_site']=30 ['call_center']=6 ['store']=12 ['promotion']=300 ['household_demographics']=7200 ['web_page']=60 ['catalog_page']=11718 ['time_dim']=86400 ['date_dim']=73049 ['item']=18000 ['customer_demographics']=1920800 ['customer_address']=50000 ['customer']=100000 ['web_returns']=71763 ['catalog_returns']=144067 ['store_returns']=287514 ['inventory']=11745000 ['web_sales']=719384 ['catalog_sales']=1441548 ['store_sales']=2880404)
    elif [[ "${scale_factor}" == "100" ]]; then
        table_rows=(['income_band']=20 ['ship_mode']=20 ['warehouse']=15 ['reason']=55 ['web_site']=24 ['call_center']=30 ['store']=402 ['promotion']=1000 ['household_demographics']=7200 ['web_page']=2040 ['catalog_page']=20400 ['time_dim']=86400 ['date_dim']=73049 ['item']=204000 ['customer_demographics']=1920800 ['customer_address']=1000000 ['customer']=2000000 ['web_returns']=7197670 ['catalog_returns']=14404374 ['store_returns']=28795080 ['inventory']=399330000 ['web_sales']=72001237 ['catalog_sales']=143997065 ['store_sales']=287997024)
    elif [[ "${scale_factor}" == "1000" ]]; then
        table_rows=(['income_band']=20 ['ship_mode']=20 ['warehouse']=20 ['reason']=65 ['web_site']=54 ['call_center']=42 ['store']=1002 ['promotion']=1500 ['household_demographics']=7200 ['web_page']=3000 ['catalog_page']=30000 ['time_dim']=86400 ['date_dim']=73049 ['item']=300000 ['customer_demographics']=1920800 ['customer_address']=6000000 ['customer']=12000000 ['web_returns']=71997522 ['catalog_returns']=143996756 ['store_returns']=287999764 ['inventory']=783000000 ['web_sales']=720000376 ['catalog_sales']=1439980416 ['store_sales']=2879987999)
    elif [[ "${scale_factor}" == "3000" ]]; then
        table_rows=(['income_band']=20 ['ship_mode']=20 ['warehouse']=22 ['reason']=67 ['web_site']=66 ['call_center']=48 ['store']=1350 ['promotion']=1800 ['household_demographics']=7200 ['web_page']=3600 ['catalog_page']=36000 ['time_dim']=86400 ['date_dim']=73049 ['item']=360000 ['customer_demographics']=1920800 ['customer_address']=15000000 ['customer']=30000000 ['web_returns']=216003761 ['catalog_returns']=432018033 ['store_returns']=863989652 ['inventory']=1033560000 ['web_sales']=2159968881 ['catalog_sales']=4320078880 ['store_sales']=8639936081)
    else
        echo "ERROR: unsupported scale_factor ${scale_factor} for tpcds" && return 1
    fi
    for table in ${!table_rows[*]}; do
        rows_actual=$(${cl} -D"${db_name}" -e"SELECT count(*) FROM ${table}" | sed -n '2p')
        rows_expect=${table_rows[${table}]}
        if [[ ${rows_actual} -ne ${rows_expect} ]]; then
            echo "ERROR: ${table} actual rows: ${rows_actual}, expect rows: ${rows_expect}" && return 1
        fi
    done
}

function check_clickbench_table_rows() {
    if [[ ! -d "${DORIS_HOME:-}" ]]; then return 1; fi
    db_name="$1"
    if [[ -z "${db_name}" ]]; then return 1; fi
    query_port=$(get_doris_conf_value "${DORIS_HOME}"/fe/conf/fe.conf query_port)
    cl="mysql -h127.0.0.1 -P${query_port} -uroot "
    declare -A table_rows
    table_rows=(['hits']=99997497)
    if ${DEBUG:-false}; then table_rows=(['hits']=10000); fi
    for table in ${!table_rows[*]}; do
        rows_actual=$(${cl} -D"${db_name}" -e"SELECT count(*) FROM ${table}" | sed -n '2p')
        rows_expect=${table_rows[${table}]}
        if [[ ${rows_actual} -ne ${rows_expect} ]]; then
            echo "ERROR: ${table} actual rows: ${rows_actual}, expect rows: ${rows_expect}" && return 1
        fi
    done
}

function check_tpch_result() {
    log_file="$1"
    if [[ -z "${log_file}" ]]; then return 1; fi
    if ! grep '^Total cold run time' "${log_file}" || ! grep '^Total hot run time' "${log_file}"; then
        echo "ERROR: can not find 'Total hot run time' in '${log_file}'"
        return 1
    else
        cold_run_time=$(grep '^Total cold run time' "${log_file}" | awk '{print $5}')
        hot_run_time=$(grep '^Total hot run time' "${log_file}" | awk '{print $5}')
    fi
    # 单位是毫秒
    cold_run_time_threshold=${cold_run_time_threshold:-50000}
    hot_run_time_threshold=${hot_run_time_threshold:-42000}
    if [[ ${cold_run_time} -gt ${cold_run_time_threshold} || ${hot_run_time} -gt ${hot_run_time_threshold} ]]; then
        echo "ERROR:
    cold_run_time ${cold_run_time} is great than the threshold ${cold_run_time_threshold},
    or, hot_run_time ${hot_run_time} is great than the threshold ${hot_run_time_threshold}"
        return 1
    else
        echo "INFO:
    cold_run_time ${cold_run_time} is less than the threshold ${cold_run_time_threshold},
    hot_run_time ${hot_run_time} is less than the threshold ${hot_run_time_threshold}"
    fi
}

function check_tpcds_result() {
    check_tpch_result "$1"
}

function check_clickbench_query_result() {
    echo "TODO"
}

function check_clickbench_performance_result() {
    result_file="$1"
    if [[ -z "${result_file}" ]]; then return 1; fi

    empty_query_time="$(awk -F ',' '{if( ($2=="") || ($3=="") || ($4=="") ){print $1}}' "${result_file}")"
    if [[ -n ${empty_query_time} ]]; then
        echo -e "ERROR: find empty query time of:\n${empty_query_time}" && return 1
    fi

    # 单位是秒
    cold_run_time_threshold=${cold_run_time_threshold:-200}
    hot_run_time_threshold=${hot_run_time_threshold:-55}
    cold_run_sum=$(awk -F ',' '{sum+=$2} END {print sum}' result.csv)
    hot_run_time=$(awk -F ',' '{if($3<$4){sum+=$3}else{sum+=$4}} END {print sum}' "${result_file}")
    if [[ $(echo "${hot_run_time} > ${hot_run_time_threshold}" | bc) -eq 1 ]] ||
        [[ $(echo "${cold_run_sum} > ${cold_run_time_threshold}" | bc) -eq 1 ]]; then
        echo "ERROR:
    cold_run_time ${cold_run_time} is great than the threshold ${cold_run_time_threshold},
    or, hot_run_time ${hot_run_time} is great than the threshold ${hot_run_time_threshold}"
        return 1
    else
        echo "INFO:
    cold_run_time ${cold_run_time} is less than the threshold ${cold_run_time_threshold},
    hot_run_time ${hot_run_time} is less than the threshold ${hot_run_time_threshold}"
    fi
}

function check_load_performance() {
    echo "TODO"
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

    if ret=$(${cl} -e"show variables like '${sv}'\G" | grep " Value: "); then
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

archive_doris_logs() {
    if [[ ! -d "${DORIS_HOME:-}" ]]; then return 1; fi
    archive_name="$1"
    if [[ -z ${archive_name} ]]; then echo "ERROR: archive file name required" && return 1; fi
    if tar -I pigz \
        --directory "${DORIS_HOME}" \
        -cf "${DORIS_HOME}/${archive_name}" \
        fe/conf \
        fe/log \
        be/conf \
        be/log; then
        echo "${DORIS_HOME}/${archive_name}"
    else
        return 1
    fi
}

print_doris_fe_log() {
    if [[ ! -d "${DORIS_HOME:-}" ]]; then return 1; fi
    echo "WARNING: --------------------tail -n 100 ${DORIS_HOME}/fe/log/fe.out--------------------"
    tail -n 100 "${DORIS_HOME}"/fe/log/fe.out
    echo "WARNING: --------------------tail -n 100 ${DORIS_HOME}/fe/log/fe.log--------------------"
    tail -n 100 "${DORIS_HOME}"/fe/log/fe.log
    echo "WARNING: ----------------------------------------"
}

print_doris_be_log() {
    if [[ ! -d "${DORIS_HOME:-}" ]]; then return 1; fi
    echo "WARNING: --------------------tail -n 100 ${DORIS_HOME}/be/log/be.out--------------------"
    tail -n 100 "${DORIS_HOME}"/be/log/be.out
    echo "WARNING: --------------------tail -n 100 ${DORIS_HOME}/be/log/be.INFO--------------------"
    tail -n 100 "${DORIS_HOME}"/be/log/be.INFO
    echo "WARNING: ----------------------------------------"
}
