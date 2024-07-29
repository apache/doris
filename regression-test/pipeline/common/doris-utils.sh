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

function start_doris_ms() {
    if [[ ! -d "${DORIS_HOME:-}" ]]; then return 1; fi
    cd "${DORIS_HOME}"/ms || return 1
    if ! ./bin/start.sh --meta-service --daemonized; then
        echo "ERROR: start doris meta-service failed." && return 1
    fi
    local i=1
    while [[ $((i++)) -lt 5 ]]; do
        if ! pgrep -fia 'doris_cloud --meta-service' >/dev/null; then
            echo "ERROR: start doris meta-service failed." && return 1
        else
            sleep 1
        fi
    done
    if [[ ${i} -ge 5 ]]; then
        echo -e "INFO: doris meta-service started,\n$("${DORIS_HOME}"/ms/lib/doris_cloud --version)"
    fi
}

function start_doris_recycler() {
    if [[ ! -d "${DORIS_HOME:-}" ]]; then return 1; fi
    cd "${DORIS_HOME}"/recycler || return 1
    if ! ./bin/start.sh --recycler --daemonized; then
        echo "ERROR: start doris recycler failed." && return 1
    fi
    local i=1
    while [[ $((i++)) -lt 5 ]]; do
        if ! pgrep -fia 'doris_cloud --recycler' >/dev/null; then
            echo "ERROR: start doris recycler failed." && return 1
        else
            sleep 1
        fi
    done
    if [[ ${i} -ge 5 ]]; then
        echo -e "INFO: doris recycler started,\n$("${DORIS_HOME}"/ms/lib/doris_cloud --version)"
    fi
}

function install_java() {
    if ! java -version >/dev/null ||
        [[ -z "$(find /usr/lib/jvm -maxdepth 1 -type d -name 'java-8-*')" ]]; then
        sudo apt update && sudo apt install openjdk-8-jdk -y >/dev/null
    fi
    # doris master branch use java-17
    if ! java -version >/dev/null ||
        [[ -z "$(find /usr/lib/jvm -maxdepth 1 -type d -name 'java-17-*')" ]]; then
        sudo apt update && sudo apt install openjdk-17-jdk -y >/dev/null
    fi
}

install_maven() {
    if ! mvn -v >/dev/null; then
        sudo apt update && sudo apt install maven -y >/dev/null
        PATH="/usr/share/maven/bin:${PATH}"
        export PATH
    fi
    if ! mvn -v >/dev/null; then
        wget -c -t3 -q "${MAVEN_DOWNLOAD_URL:-https://dlcdn.apache.org/maven/maven-3/3.9.8/binaries/apache-maven-3.9.8-bin.tar.gz}"
        tar -xf apache-maven-3.9.8-bin.tar.gz -C /usr/share/
        PATH="/usr/share/apache-maven-3.9.8/bin:${PATH}"
        export PATH
    fi
    if ! mvn -v >/dev/null; then
        echo "ERROR: install maven failed" && return 1
    fi
}

function start_doris_fe() {
    if [[ ! -d "${DORIS_HOME:-}" ]]; then return 1; fi
    if install_java && [[ -z "${JAVA_HOME}" ]]; then
        # default to use java-8
        JAVA_HOME="$(find /usr/lib/jvm -maxdepth 1 -type d -name 'java-8-*' | sed -n '1p')"
        export JAVA_HOME
    fi
    # export JACOCO_COVERAGE_OPT="-javaagent:/usr/local/jacoco/lib/jacocoagent.jar=excludes=org.apache.doris.thrift:org.apache.doris.proto:org.apache.parquet.format:com.aliyun*:com.amazonaws*:org.apache.hadoop.hive.metastore:org.apache.parquet.format,output=file,append=true,destfile=${DORIS_HOME}/fe/fe_cov.exec"
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
    if install_java && [[ -z "${JAVA_HOME}" ]]; then
        # default to use java-8
        JAVA_HOME="$(find /usr/lib/jvm -maxdepth 1 -type d -name 'java-8-*' | sed -n '1p')"
        export JAVA_HOME
    fi
    ASAN_SYMBOLIZER_PATH="$(command -v llvm-symbolizer)"
    if [[ -z "${ASAN_SYMBOLIZER_PATH}" ]]; then ASAN_SYMBOLIZER_PATH='/var/local/ldb-toolchain/bin/llvm-symbolizer'; fi
    export ASAN_SYMBOLIZER_PATH
    export ASAN_OPTIONS="symbolize=1:abort_on_error=1:disable_coredump=0:unmap_shadow_on_exit=1:use_sigaltstack=0:detect_leaks=0:fast_unwind_on_malloc=0:check_malloc_usable_size=0"
    export TCMALLOC_SAMPLE_PARAMETER=524288
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
    # try to add be, maybe Same backend already exists[127.0.0.1:9050], it's ok
    if ${cl} -e "ALTER SYSTEM ADD BACKEND '127.0.0.1:${heartbeat_service_port}';"; then echo; else echo; fi
    check_doris_ready
}

function check_doris_ready() {
    if [[ ! -d "${DORIS_HOME:-}" ]]; then return 1; fi
    i=1
    while [[ $((i++)) -lt 60 ]]; do
        if be_ready_count=$(${cl} -e 'show backends\G' | grep -c 'Alive: true') &&
            [[ ${be_ready_count} -eq 1 ]]; then
            echo -e "INFO: Doris cluster ready, be version: \n$(${cl} -e 'show backends\G' | grep 'Version')" && break
        else
            echo 'Wait for backends ready, sleep 2 seconds ...' && sleep 2
        fi
    done
    if [[ ${i} -ge 60 ]]; then echo "ERROR: Doris cluster not ready after 2 mins wait..." && return 1; fi

    # wait 10s for doris totally started, otherwize may encounter the error below,
    # ERROR 1105 (HY000) at line 102: errCode = 2, detailMessage = Failed to find enough backend, please check the replication num,replication tag and storage medium.
    sleep 10s
}

function stop_doris() {
    if [[ ! -d "${DORIS_HOME:-}" ]]; then return 1; fi
    if [[ -f "${DORIS_HOME}"/ms/bin/stop.sh ]]; then bash "${DORIS_HOME}"/ms/bin/stop.sh; fi
    if [[ -f "${DORIS_HOME}"/recycler/bin/stop.sh ]]; then bash "${DORIS_HOME}"/recycler/bin/stop.sh; fi
    if "${DORIS_HOME}"/be/bin/stop_be.sh && "${DORIS_HOME}"/fe/bin/stop_fe.sh; then
        echo "INFO: normally stoped doris"
    else
        pgrep -fi doris | xargs kill -9 &>/dev/null
        echo "WARNING: force stoped doris"
    fi
}

function clean_fdb() {
    instance_id="$1"
    if [[ -z "${instance_id:-}" ]]; then return 1; fi
    if fdbcli --exec "writemode on;clearrange \x01\x10instance\x00\x01\x10${instance_id}\x00\x01 \x01\x10instance\x00\x01\x10${instance_id}\x00\xff\x00\x01" &&
        fdbcli --exec "writemode on;clearrange \x01\x10meta\x00\x01\x10${instance_id}\x00\x01 \x01\x10meta\x00\x01\x10${instance_id}\x00\xff\x00\x01" &&
        fdbcli --exec "writemode on;clearrange \x01\x10txn\x00\x01\x10${instance_id}\x00\x01 \x01\x10txn\x00\x01\x10${instance_id}\x00\xff\x00\x01" &&
        fdbcli --exec "writemode on;clearrange \x01\x10version\x00\x01\x10${instance_id}\x00\x01 \x01\x10version\x00\x01\x10${instance_id}\x00\xff\x00\x01" &&
        fdbcli --exec "writemode on;clearrange \x01\x10stats\x00\x01\x10${instance_id}\x00\x01 \x01\x10stats\x00\x01\x10${instance_id}\x00\xff\x00\x01" &&
        fdbcli --exec "writemode on;clearrange \x01\x10recycle\x00\x01\x10${instance_id}\x00\x01 \x01\x10recycle\x00\x01\x10${instance_id}\x00\xff\x00\x01" &&
        fdbcli --exec "writemode on;clearrange \x01\x10job\x00\x01\x10${instance_id}\x00\x01 \x01\x10job\x00\x01\x10${instance_id}\x00\xff\x00\x01" &&
        fdbcli --exec "writemode on;clearrange \x01\x10copy\x00\x01\x10${instance_id}\x00\x01 \x01\x10copy\x00\x01\x10${instance_id}\x00\xff\x00\x01" &&
        rm -f /var/log/foundationdb/*; then
        echo "INFO: fdb cleaned."
    else
        echo "ERROR: failed to clean fdb" && return 1
    fi
}

function install_fdb() {
    if fdbcli --exec 'status' >/dev/null; then return; fi
    wget -c -t3 -q https://github.com/apple/foundationdb/releases/download/7.1.23/foundationdb-clients_7.1.23-1_amd64.deb
    wget -c -t3 -q https://github.com/apple/foundationdb/releases/download/7.1.23/foundationdb-server_7.1.23-1_amd64.deb
    sudo dpkg -i foundationdb-clients_7.1.23-1_amd64.deb foundationdb-server_7.1.23-1_amd64.deb
    # /usr/lib/foundationdb/fdbmonitor --daemonize
    # fdbcli --exec 'configure new single ssd'
    if fdbcli --exec 'status'; then
        echo "INFO: foundationdb installed."
    else
        return 1
    fi
}

function restart_doris() {
    if stop_doris; then echo; fi
    if ! start_doris_fe; then return 1; fi
    if ! start_doris_be; then return 1; fi
    i=1
    while [[ $((i++)) -lt 60 ]]; do
        if be_ready_count=$(${cl} -e 'show backends\G' | grep -c 'Alive: true') &&
            [[ ${be_ready_count} -eq 1 ]]; then
            echo -e "INFO: ${be_ready_count} Backends ready, version: \n$(${cl} -e 'show backends\G' | grep 'Version')" && break
        else
            echo 'Wait for Backends ready, sleep 2 seconds ...' && sleep 2
        fi
    done
    if [[ ${i} -ge 60 ]]; then echo "ERROR: Backend not ready after 2 mins wait..." && return 1; fi

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
    cold_run_time=$(awk -F ',' '{sum+=$2} END {print sum}' result.csv)
    hot_run_time=$(awk -F ',' '{if($3<$4){sum+=$3}else{sum+=$4}} END {print sum}' "${result_file}")
    if [[ $(echo "${hot_run_time} > ${hot_run_time_threshold}" | bc) -eq 1 ]] ||
        [[ $(echo "${cold_run_time} > ${cold_run_time_threshold}" | bc) -eq 1 ]]; then
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

show_session_variables() {
    if [[ ! -d "${DORIS_HOME:-}" ]]; then return 1; fi
    query_port=$(get_doris_conf_value "${DORIS_HOME}"/fe/conf/fe.conf query_port)
    if mysql -h127.0.0.1 -P"${query_port}" -uroot -e"show session variables;"; then
        return
    else
        return 1
    fi
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

function reset_doris_session_variables() {
    # reset all session variables to default
    if [[ ! -d "${DORIS_HOME:-}" ]]; then return 1; fi
    query_port=$(get_doris_conf_value "${DORIS_HOME}"/fe/conf/fe.conf query_port)
    cl="mysql -h127.0.0.1 -P${query_port} -uroot "
    # Variable_name    Value    Default_Value    Changed
    # "\x27" means single quote in awk
    if ${cl} -e'show variables' | awk '{if ($4 == 1){print "set global " $1 "=\x27" $3 "\x27;"}}' >reset_session_variables; then
        cat reset_session_variables
        if ${cl} <reset_session_variables; then
            echo "INFO: reset session variables to default, succeed"
            rm -f reset_session_variables
        else
            echo "ERROR: reset session variables failed" && return 1
        fi
    else
        echo "ERROR: reset session variables failed" && return 1
    fi
}

function set_doris_session_variables_from_file() {
    # set session variables from file
    if [[ ! -d "${DORIS_HOME:-}" ]]; then return 1; fi
    session_variables_file="$1"
    if [[ -z ${session_variables_file} ]]; then echo "ERROR: session_variables_file required" && return 1; fi
    query_port=$(get_doris_conf_value "${DORIS_HOME}"/fe/conf/fe.conf query_port)
    if mysql -h127.0.0.1 -P"${query_port}" -uroot -e"source ${session_variables_file};"; then
        echo "INFO: set session variables from file ${session_variables_file}, succeed"
    else
        echo "ERROR: set session variables from file ${session_variables_file}, failed" && return 1
    fi
}

archive_doris_logs() {
    if [[ ! -d "${DORIS_HOME:-}" ]]; then return 1; fi
    local archive_name="$1"
    if [[ -z ${archive_name} || ${archive_name} != *".tar.gz" ]]; then
        echo "USAGE: ${FUNCNAME[0]} xxxx.tar.gz" && return 1
    fi
    local archive_dir="${archive_name%.tar.gz}"
    rm -rf "${DORIS_HOME:?}/${archive_dir}"
    mkdir -p "${DORIS_HOME}/${archive_dir}"
    (
        cd "${DORIS_HOME}" || return 1
        cp --parents -rf "fe/conf" "${archive_dir}"/
        cp --parents -rf "fe/log" "${archive_dir}"/
        cp --parents -rf "be/conf" "${archive_dir}"/
        cp --parents -rf "be/log" "${archive_dir}"/
        if [[ -d "${DORIS_HOME}"/regression-test/log ]]; then
            # try to hide ak and sk
            if sed -i "s/${cos_ak:-}//g;s/${cos_sk:-}//g" regression-test/log/* &>/dev/null; then :; fi
            cp --parents -rf "regression-test/log" "${archive_dir}"/
        fi
        if [[ -d "${DORIS_HOME}"/../regression-test/conf ]]; then
            # try to hide ak and sk
            if sed -i "s/${cos_ak:-}//g;s/${cos_sk:-}//g" ../regression-test/conf/* &>/dev/null; then :; fi
            mkdir -p "${archive_dir}"/regression-test/conf
            cp -rf ../regression-test/conf/* "${archive_dir}"/regression-test/conf/
        fi
        if [[ -f "${DORIS_HOME}"/session_variables ]]; then
            cp --parents -rf "session_variables" "${archive_dir}"/
        fi
        if [[ -d "${DORIS_HOME}"/ms ]]; then
            mkdir -p "${archive_dir}"/foundationdb/log
            cp -rf /var/log/foundationdb/* "${archive_dir}"/foundationdb/log/
            cp --parents -rf "ms/conf" "${archive_dir}"/
            cp --parents -rf "ms/log" "${archive_dir}"/
        fi
        if [[ -d "${DORIS_HOME}"/recycler ]]; then
            cp --parents -rf "recycler/conf" "${archive_dir}"/
            cp --parents -rf "recycler/log" "${archive_dir}"/
        fi
        if [[ -d "${DORIS_HOME}"/be/storage/error_log ]]; then
            cp --parents -rf "be/storage/error_log" "${archive_dir}"/
        fi
    )

    if tar -I pigz \
        --directory "${DORIS_HOME}" \
        -cf "${DORIS_HOME}/${archive_name}" \
        "${archive_dir}"; then
        rm -rf "${DORIS_HOME:?}/${archive_dir}"
        echo "${DORIS_HOME}/${archive_name}"
    else
        return 1
    fi
}

wait_coredump_file_ready() {
    # if the size of coredump file does not changed in 5 seconds, we think it has generated done
    local coredump_file="$1"
    if [[ -z "${coredump_file}" ]]; then echo "ERROR: coredump_file is required" && return 1; fi
    initial_size=$(stat -c %s "${coredump_file}")
    while true; do
        sleep 5
        current_size=$(stat -c %s "${coredump_file}")
        if [[ ${initial_size} -eq ${current_size} ]]; then
            break
        else
            initial_size=${current_size}
        fi
    done
}

clear_coredump() {
    echo -e "INFO: clear coredump files \n$(ls /var/lib/apport/coredump/)"
    rm -rf /var/lib/apport/coredump/*
}

archive_doris_coredump() {
    if [[ ! -d "${DORIS_HOME:-}" ]]; then return 1; fi
    archive_name="$1"
    COREDUMP_SIZE_THRESHOLD="${COREDUMP_SIZE_THRESHOLD:-85899345920}" # if coredump size over 80G, do not archive"
    if [[ -z ${archive_name} ]]; then echo "ERROR: archive file name required" && return 1; fi
    local archive_dir="${archive_name%.tar.gz}"
    rm -rf "${DORIS_HOME:?}/${archive_dir}"
    mkdir -p "${DORIS_HOME}/${archive_dir}"
    declare -A pids
    pids['be']="$(cat "${DORIS_HOME}"/be/bin/be.pid)"
    pids['ms']="$(cat "${DORIS_HOME}"/ms/bin/doris_cloud.pid)"
    pids['recycler']="$(cat "${DORIS_HOME}"/recycler/bin/doris_cloud.pid)"
    local has_core=false
    for p in "${!pids[@]}"; do
        pid="${pids[${p}]}"
        if [[ -z "${pid}" ]]; then continue; fi
        if coredump_file=$(find /var/lib/apport/coredump/ -maxdepth 1 -type f -name "core.*${pid}.*") &&
            [[ -n "${coredump_file}" ]]; then
            wait_coredump_file_ready "${coredump_file}"
            file_size=$(stat -c %s "${coredump_file}")
            if ((file_size <= COREDUMP_SIZE_THRESHOLD)); then
                mkdir -p "${DORIS_HOME}/${archive_dir}/${p}"
                if [[ "${p}" == "be" ]]; then
                    mv "${DORIS_HOME}"/be/lib/doris_be "${DORIS_HOME}/${archive_dir}/${p}"
                elif [[ "${p}" == "ms" ]]; then
                    mv "${DORIS_HOME}"/ms/lib/doris_cloud "${DORIS_HOME}/${archive_dir}/${p}"
                elif [[ "${p}" == "recycler" ]]; then
                    mv "${DORIS_HOME}"/recycler/lib/doris_cloud "${DORIS_HOME}/${archive_dir}/${p}"
                fi
                mv "${coredump_file}" "${DORIS_HOME}/${archive_dir}/${p}"
                has_core=true
            else
                echo -e "\n\n\n\nERROR: --------------------tail -n 100 ${DORIS_HOME}/be/log/be.out--------------------"
                tail -n 100 "${DORIS_HOME}"/be/log/be.out
            fi
        fi
    done

    if ${has_core} && tar -I pigz \
        --directory "${DORIS_HOME}" \
        -cf "${DORIS_HOME}/${archive_name}" \
        "${archive_dir}"; then
        rm -rf "${DORIS_HOME:?}/${archive_dir}"
        echo "${DORIS_HOME}/${archive_name}"
    else
        return 1
    fi
}

print_doris_fe_log() {
    if [[ ! -d "${DORIS_HOME:-}" ]]; then return 1; fi
    echo -e "\n\n\n\nWARNING: --------------------tail -n 100 ${DORIS_HOME}/fe/log/fe.out--------------------"
    tail -n 100 "${DORIS_HOME}"/fe/log/fe.out
    echo -e "\n\n\n\nWARNING: --------------------tail -n 100 ${DORIS_HOME}/fe/log/fe.log--------------------"
    tail -n 100 "${DORIS_HOME}"/fe/log/fe.log
    echo -e "WARNING: ----------------------------------------\n\n\n\n"
}

print_doris_be_log() {
    if [[ ! -d "${DORIS_HOME:-}" ]]; then return 1; fi
    echo -e "\n\n\n\nWARNING: --------------------tail -n 100 ${DORIS_HOME}/be/log/be.out--------------------"
    tail -n 100 "${DORIS_HOME}"/be/log/be.out
    echo -e "\n\n\n\nWARNING: --------------------tail -n 100 ${DORIS_HOME}/be/log/be.INFO--------------------"
    tail -n 100 "${DORIS_HOME}"/be/log/be.INFO
    echo -e "WARNING: ----------------------------------------\n\n\n\n"
}

print_fdb_log() {
    echo
}

print_doris_conf() {
    if [[ ! -d "${DORIS_HOME:-}" ]]; then return 1; fi
    echo -e "\n\n\n\nINFO: --------------------cat ${DORIS_HOME}/fe/conf/fe.conf--------------------"
    cat "${DORIS_HOME}"/fe/conf/fe.conf
    if [[ -f ${DORIS_HOME}/fe/conf/fe_custom.conf ]]; then
        echo -e "\n\n\n\nINFO: --------------------cat ${DORIS_HOME}/fe/conf/fe_custom.conf--------------------"
        cat "${DORIS_HOME}"/fe/conf/fe_custom.conf
    fi
    echo -e "\n\n\n\nINFO: --------------------tail -n 100 ${DORIS_HOME}/be/conf/be.conf--------------------"
    tail -n 100 "${DORIS_HOME}"/be/conf/be.conf
    if [[ -f ${DORIS_HOME}/be/conf/be_custom.conf ]]; then
        echo -e "\n\n\n\nINFO: --------------------cat ${DORIS_HOME}/be/conf/be_custom.conf--------------------"
        cat "${DORIS_HOME}"/be/conf/be_custom.conf
    fi
    if [[ -f ${DORIS_HOME}/ms/conf/doris_cloud.conf ]]; then
        echo -e "\n\n\n\nINFO: --------------------cat ${DORIS_HOME}/ms/conf/doris_cloud.conf--------------------"
        cat "${DORIS_HOME}"/ms/conf/doris_cloud.conf
    fi
    if [[ -f ${DORIS_HOME}/recycler/conf/doris_cloud.conf ]]; then
        echo -e "\n\n\n\nINFO: --------------------cat ${DORIS_HOME}/recycler/conf/doris_cloud.conf--------------------"
        cat "${DORIS_HOME}"/recycler/conf/doris_cloud.conf
    fi
    echo -e "INFO: ----------------------------------------\n\n\n\n"
}

function create_warehouse() {
    if [[ -z ${oss_ak} || -z ${oss_sk} ]]; then
        echo "ERROR: env oss_ak and oss_sk are required." && return 1
    fi
    if curl "127.0.0.1:5000/MetaService/http/create_instance?token=greedisgood9999" -d "{
        \"instance_id\": \"cloud_instance_0\",
        \"name\":\"cloud_instance_0\",
        \"user_id\":\"user-id\",
        \"obj_info\": {
            \"provider\": \"OSS\",
            \"region\": \"oss-cn-hongkong\",
            \"bucket\": \"doris-community-test\",
            \"prefix\": \"cloud_regression\",
            \"endpoint\": \"oss-cn-hongkong-internal.aliyuncs.com\",
            \"external_endpoint\": \"oss-cn-hongkong-internal.aliyuncs.com\",
            \"ak\": \"${oss_ak}\",
            \"sk\": \"${oss_sk}\"
        }
    }"; then
        echo
    else
        return 1
    fi
}

function warehouse_add_fe() {
    local ret
    if curl "127.0.0.1:5000/MetaService/http/add_cluster?token=greedisgood9999" -d "{
        \"instance_id\": \"cloud_instance_0\",
        \"cluster\":{
            \"type\":\"SQL\",
            \"cluster_name\":\"RESERVED_CLUSTER_NAME_FOR_SQL_SERVER\",
            \"cluster_id\":\"RESERVED_CLUSTER_ID_FOR_SQL_SERVER\",
            \"nodes\":[
                {
                    \"cloud_unique_id\":\"cloud_unique_id_sql_server00\",
                    \"ip\":\"127.0.0.1\",
                    \"edit_log_port\":\"9010\",
                    \"node_type\":\"FE_MASTER\"
                }
            ]
        }
    }"; then
        # check
        if ret=$(curl "127.0.0.1:5000/MetaService/http/get_cluster?token=greedisgood9999" -d "{
            \"instance_id\": \"cloud_instance_0\",
            \"cloud_unique_id\":\"cloud_unique_id_sql_server00\",
            \"cluster_name\":\"RESERVED_CLUSTER_NAME_FOR_SQL_SERVER\",
            \"cluster_id\":\"RESERVED_CLUSTER_ID_FOR_SQL_SERVER\"
        }"); then
            echo -e "warehouse_add_fe:\n${ret}"
        fi
    else
        return 1
    fi

}

function warehouse_add_be() {
    local ret
    if curl "127.0.0.1:5000/MetaService/http/add_cluster?token=greedisgood9999" -d "{
        \"instance_id\": \"cloud_instance_0\",
        \"cluster\":{
            \"type\":\"COMPUTE\",
            \"cluster_name\":\"cluster_name0\",
            \"cluster_id\":\"cluster_id0\",
            \"nodes\":[
                {
                    \"cloud_unique_id\":\"cloud_unique_id_compute_node0\",
                    \"ip\":\"127.0.0.1\",
                    \"heartbeat_port\":\"9050\"
                }
            ]
        }
    }"; then
        # check
        if ret=$(curl "127.0.0.1:5000/MetaService/http/get_cluster?token=greedisgood9999" -d "{
            \"instance_id\": \"cloud_instance_0\",
            \"cloud_unique_id\":\"cloud_unique_id_compute_node0\",
            \"cluster_name\":\"cluster_name0\",
            \"cluster_id\":\"cluster_id0\"
        }"); then
            echo -e "warehouse_add_be:\n${ret}"
        fi
    else
        return 1
    fi
}

function check_if_need_gcore() {
    exit_flag="$1"
    if [[ ${exit_flag} == "124" ]]; then # 124 is from command timeout
        echo "INFO: run regression timeout, gcore to find out reason"
        be_pid=$(pgrep "doris_be")
        if [[ -n "${be_pid}" ]]; then
            kill -ABRT "${be_pid}"
            sleep 10
        fi
    else
        echo "ERROR: unknown exit_flag ${exit_flag}" && return 1
    fi
}

prepare_java_udf() {
    if [[ ! -d "${DORIS_HOME:-}" ]]; then return 1; fi
    # custom_lib相关的case需要在fe启动前把编译好的jar放到 $DORIS_HOME/fe/custom_lib/
    install_java
    install_maven
    OLD_JAVA_HOME=${JAVA_HOME}
    JAVA_HOME="$(find /usr/lib/jvm -maxdepth 1 -type d -name 'java-8-*' | sed -n '1p')"
    export JAVA_HOME
    if bash "${DORIS_HOME}"/../run-regression-test.sh --clean &&
        bash "${DORIS_HOME}"/../run-regression-test.sh --compile; then
        echo
    else
        echo "ERROR: failed to compile java udf"
    fi
    JAVA_HOME=${OLD_JAVA_HOME}
    export JAVA_HOME

    if ls "${DORIS_HOME}"/fe/custom_lib/*.jar &&
        ls "${DORIS_HOME}"/be/custom_lib/*.jar; then
        echo "INFO: java udf prepared."
    else
        echo "ERROR: failed to prepare java udf"
        return 1
    fi
}

function print_running_pipeline_tasks() {
    webserver_port=$(get_doris_conf_value "${DORIS_HOME}"/be/conf/be.conf webserver_port)
    mkdir -p "${DORIS_HOME}"/be/log/
    echo "------------------------${FUNCNAME[0]}--------------------------"
    echo "curl -m 10 http://127.0.0.1:${webserver_port}/api/running_pipeline_tasks/30"
    echo ""
    curl -m 10 "http://127.0.0.1:${webserver_port}/api/running_pipeline_tasks/30" 2>&1 | tee "${DORIS_HOME}"/be/log/running_pipeline_tasks_30
    echo "------------------------${FUNCNAME[0]}--------------------------"
}
