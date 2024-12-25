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

#
# 1. Run fdb_ctrl.sh deploy on each machine to deploy FoundationDB.
#    This will create the necessary directories, configuration files.
#
# 2. Run fdb_ctrl.sh start on each machine to start the fdb cluster
#    and get the cluster connection string.
#

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" &>/dev/null && pwd)"

if [[ -f "${ROOT_DIR}/fdb_vars.sh" ]]; then
    source "${ROOT_DIR}/fdb_vars.sh"
else
    echo "Please create fdb_vars.sh first"
    exit 1
fi

if [[ ! -d "${FDB_HOME}" ]]; then
    echo "Please set and create FDB_HOME first"
    exit 1
fi

if [[ ! "${FDB_HOME}" = /* ]]; then
    echo "${FDB_HOME} is not an absolute path."
    exit 1
fi

if [[ -z ${FDB_CLUSTER_ID} ]]; then
    echo "Please set FDB_CLUSTER_ID first"
    exit 1
fi

# TODO verify config

FDB_CLUSTER_DESC=${FDB_CLUSTER_DESC:-"doris-fdb"}

# A dir to provide FDB binary pkgs
FDB_PKG_DIR=${ROOT_DIR}/pkgs/${FDB_VERSION}

FDB_PORT=${FDB_PORT:-4500}

LOG_DIR=${LOG_DIR:-${FDB_HOME}/log}

mkdir -p "${LOG_DIR}"
mkdir -p "${FDB_HOME}"/conf
mkdir -p "${FDB_HOME}"/log

function ensure_port_is_listenable() {
    local component="$1"
    local port="$2"

    if lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null; then
        echo "The port ${port} of ${component} is occupied"
        exit 1
    fi
}

function download_fdb() {
    if [[ -d "${FDB_PKG_DIR}" ]]; then
        echo "FDB package for ${FDB_VERSION} already exists"
        return
    fi

    arch=$(uname -m)
    if [[ "${arch}" == "x86_64" ]]; then
        local URL="https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/"
        local TMP="${FDB_PKG_DIR}-tmp"

        rm -rf "${TMP}"
        mkdir -p "${TMP}"

        wget "${URL}/fdbbackup.x86_64" -O "${TMP}/fdbbackup"
        wget "${URL}/fdbserver.x86_64" -O "${TMP}/fdbserver"
        wget "${URL}/fdbcli.x86_64" -O "${TMP}/fdbcli"
        wget "${URL}/fdbmonitor.x86_64" -O "${TMP}/fdbmonitor"
        wget "${URL}/libfdb_c.x86_64.so" -O "${TMP}/libfdb_c.x86_64.so"
    elif [[ "${arch}" == "aarch64" ]]; then
        local URL="https://doris-build.oss-cn-beijing.aliyuncs.com/thirdparty/fdb/aarch64"
        local TMP="${FDB_PKG_DIR}-tmp"

        rm -rf "${TMP}"
        mkdir -p "${TMP}"

        wget "${URL}/fdbbackup" -O "${TMP}/fdbbackup"
        wget "${URL}/fdbserver" -O "${TMP}/fdbserver"
        wget "${URL}/fdbcli" -O "${TMP}/fdbcli"
        wget "${URL}/fdbmonitor" -O "${TMP}/fdbmonitor"
        wget "${URL}/libfdb_c.aarch64.so" -O "${TMP}/libfdb_c.aarch64.so"
    else
        echo "Unsupported architecture: ""${arch}"
    fi

    chmod +x "${TMP}"/fdb*

    mv "${TMP}" "${FDB_PKG_DIR}"
    echo "Download fdb binary pkgs success"
}

# Function to configure coordinators
get_coordinators() {
    local num_nodes
    local num_coordinators

    num_nodes=$(echo "${FDB_CLUSTER_IPS}" | tr ',' '\n' | wc -l)

    if [[ ${num_nodes} -le 2 ]]; then
        num_coordinators=1
    elif [[ ${num_nodes} -le 4 ]]; then
        num_coordinators=3
    else
        num_coordinators=5
    fi

    echo "${FDB_CLUSTER_IPS}" | cut -d',' -f1-"${num_coordinators}" | tr ',' '\n' | sed "s/$/:${FDB_PORT}/" | paste -sd ','
}

get_fdb_mode() {
    # Initialize a new database
    local num_nodes
    local fdb_mode

    num_nodes=$(echo "${FDB_CLUSTER_IPS}" | tr ',' '\n' | wc -l)
    if [[ ${num_nodes} -eq 1 ]]; then
        fdb_mode="single"
    elif [[ ${num_nodes} -le 4 ]]; then
        fdb_mode="double"
    else
        fdb_mode="triple"
    fi

    echo "${fdb_mode}"
}

# Function to calculate number of processes
calculate_process_numbers() {
    local memory_limit_gb=$1
    local cpu_cores_limit=$2

    local data_dir_count

    # Convert comma-separated DATA_DIRS into an array
    IFS=',' read -r -a DATA_DIR_ARRAY <<<"${DATA_DIRS}"
    data_dir_count=${#DATA_DIR_ARRAY[@]}

    # Parse the ratio input
    IFS=':' read -r num_storage num_stateless num_log <<<"${STORAGE_STATELESS_LOG_RATIO}"

    # Initialize process counts
    local storage_processes=0   # Storage processes
    local stateless_processes=0 # Stateless processes
    local log_processes=0       # Log processes

    local storage_process_num_limit=$((STORAGE_PROCESSES_NUM_PER_SSD * data_dir_count))
    local log_process_num_limit=$((LOG_PROCESSES_NUM_PER_SSD * data_dir_count))

    if [[ "#${MEDIUM_TYPE}" = "#HDD" ]]; then
        storage_process_num_limit=$((STORAGE_PROCESSES_NUM_PER_HDD * data_dir_count))
        log_process_num_limit=$((LOG_PROCESSES_NUM_PER_HDD * data_dir_count))
    fi

    # Find maximum number of processes while maintaining the specified ratio
    while true; do
        # Calculate process counts based on the ratio
        storage_processes=$((storage_processes + num_storage))
        stateless_processes=$((storage_processes * num_stateless / num_storage))
        log_processes=$((storage_processes * num_log / num_storage))

        # Calculate total CPUs used
        local total_cpu_used=$((storage_processes + stateless_processes + log_processes))

        # Check memory constraint
        local total_memory_used=$(((MEMORY_STORAGE_GB * storage_processes) + (MEMORY_STATELESS_GB * stateless_processes) + (MEMORY_LOG_GB * log_processes)))

        # Check datadir limits
        if ((storage_processes > storage_process_num_limit || log_processes > log_process_num_limit)); then
            break
        fi

        # Check overall constraints
        if ((total_memory_used <= memory_limit_gb && total_cpu_used <= cpu_cores_limit)); then
            continue
        else
            # If constraints are violated, revert back
            storage_processes=$((storage_processes - num_storage))
            stateless_processes=$((storage_processes * num_stateless / num_storage))
            log_processes=$((storage_processes * num_log / num_storage))
            break
        fi
    done

    # Return the values
    echo "${stateless_processes} ${storage_processes} ${log_processes}"
}

function check_vars() {
    IFS=',' read -r -a IPS <<<"${FDB_CLUSTER_IPS}"

    command -v ping || echo "ping is not available to check machines are available, please install ping."

    for IP_ADDRESS in "${IPS[@]}"; do
        if ping -c 1 "${IP_ADDRESS}" &>/dev/null; then
            echo "${IP_ADDRESS} is reachable"
        else
            echo "${IP_ADDRESS} is not reachable"
            exit 1
        fi
    done

    if [[ ${CPU_CORES_LIMIT} -gt $(nproc) ]]; then
        echo "CPU_CORES_LIMIT beyonds number of machine, which is $(nproc)"
        exit 1
    fi

    if [[ ${MEMORY_LIMIT_GB} -gt $(free -g | awk '/^Mem:/{print $2}') ]]; then
        echo "MEMORY_LIMIT_GB beyonds memory of machine, which is $(free -g | awk '/^Mem:/{print $2}')"
        exit 1
    fi
}

function deploy_fdb() {
    check_vars
    download_fdb
    check_fdb_running

    ln -sf "${FDB_PKG_DIR}/fdbserver" "${FDB_HOME}/fdbserver"
    ln -sf "${FDB_PKG_DIR}/fdbmonitor" "${FDB_HOME}/fdbmonitor"
    ln -sf "${FDB_PKG_DIR}/fdbbackup" "${FDB_HOME}/backup_agent"
    ln -sf "${FDB_PKG_DIR}/fdbcli" "${FDB_HOME}/fdbcli"

    CLUSTER_DESC="${FDB_CLUSTER_DESC:-${FDB_CLUSTER_ID}}"

    # Convert comma-separated DATA_DIRS into an array
    IFS=',' read -r -a DATA_DIR_ARRAY <<<"${DATA_DIRS}"
    for DIR in "${DATA_DIR_ARRAY[@]}"; do
        mkdir -p "${DIR}" || handle_error "Failed to create data directory ${DIR}"
        if [[ -n "$(ls -A "${DIR}")" ]]; then
            echo "Error: ${DIR} is not empty. DO NOT run deploy on a node running fdb. If you are sure that the node is not in a fdb cluster, run fdb_ctl.sh clean."
            exit 1
        fi
    done

    echo -e "\tCreate fdb.cluster, coordinator: $(get_coordinators)"
    echo -e "\tfdb.cluster content is: ${CLUSTER_DESC}:${FDB_CLUSTER_ID}@$(get_coordinators)"
    cat >"${FDB_HOME}/conf/fdb.cluster" <<EOF
${CLUSTER_DESC}:${FDB_CLUSTER_ID}@$(get_coordinators)
EOF

    cat >"${FDB_HOME}/conf/fdb.conf" <<EOF
[fdbmonitor]
user = ${USER}
group = ${USER}

[general]
restart-delay = 60
cluster-file = ${FDB_HOME}/conf/fdb.cluster

## Default parameters for individual fdbserver processes
[fdbserver]
command = ${FDB_HOME}/fdbserver
public-address = auto:\$ID
listen-address = public
logdir = ${LOG_DIR}
datadir = ${DATA_DIR_ARRAY[0]}/\$ID

EOF

    # Read configuration values
    MEMORY_LIMIT_GB=${MEMORY_LIMIT_GB:-8}
    CPU_CORES_LIMIT=${CPU_CORES_LIMIT:-1}

    # Calculate number of processes based on resources and data directories
    read -r stateless_processes storage_processes log_processes <<<"$(calculate_process_numbers "${MEMORY_LIMIT_GB}" "${CPU_CORES_LIMIT}")"
    echo "stateless process num : ${stateless_processes}, storage_processes : ${storage_processes}, log_processes : ${log_processes}"
    if [[ ${storage_processes} -eq 0 ]]; then
        # Add one process
        PORT=$((FDB_PORT))
        echo "[fdbserver.${PORT}]
" >>"${FDB_HOME}/conf/fdb.conf"
    fi

    # Add stateless processes
    for ((i = 0; i < stateless_processes; i++)); do
        PORT=$((FDB_PORT + i))
        echo "[fdbserver.${PORT}]
class = stateless" >>"${FDB_HOME}/conf/fdb.conf"
    done

    FDB_PORT=$((FDB_PORT + stateless_processes))

    # Add storage processes
    STORAGE_DIR_COUNT=${#DATA_DIR_ARRAY[@]}
    for ((i = 0; i < storage_processes; i++)); do
        PORT=$((FDB_PORT + i))
        DIR_INDEX=$((i % STORAGE_DIR_COUNT))
        echo "[fdbserver.${PORT}]
class = storage
datadir = ${DATA_DIR_ARRAY[${DIR_INDEX}]}/${PORT}" | tee -a "${FDB_HOME}/conf/fdb.conf" >/dev/null
    done

    FDB_PORT=$((FDB_PORT + storage_processes))

    # Add log processes
    for ((i = 0; i < log_processes; i++)); do
        PORT=$((FDB_PORT + i))
        DIR_INDEX=$((i % STORAGE_DIR_COUNT))
        echo "[fdbserver.${PORT}]
class = log
datadir = ${DATA_DIR_ARRAY[${DIR_INDEX}]}/${PORT}" | tee -a "${FDB_HOME}/conf/fdb.conf" >/dev/null
    done

    echo "[backup_agent]
command = ${FDB_HOME}/backup_agent
logdir = ${LOG_DIR}" >>"${FDB_HOME}/conf/fdb.conf"

    echo "Deploy FDB to: ${FDB_HOME}"
}

function start_fdb() {
    check_fdb_running

    if [[ ! -f "${FDB_HOME}/fdbmonitor" ]]; then
        echo 'Please run setup before start fdb server'
        exit 1
    fi

    ensure_port_is_listenable "fdbserver" "${FDB_PORT}"

    echo "Run FDB monitor ..."
    "${FDB_HOME}/fdbmonitor" \
        --conffile "${FDB_HOME}/conf/fdb.conf" \
        --lockfile "${FDB_HOME}/fdbmonitor.pid" \
        --daemonize
}

function stop_fdb() {
    fdb_pid_file="${FDB_HOME}/fdbmonitor.pid"
    if [[ -f "${fdb_pid_file}" ]]; then
        local fdb_pid
        fdb_pid=$(cat "${fdb_pid_file}")
        if ps -p "${fdb_pid}" >/dev/null; then
            echo "Stop fdbmonitor with pid ${fdb_pid}"
            kill -9 "${fdb_pid}"
            rm -f "${fdb_pid_file}"
        fi
    fi
}

function check_fdb_running() {
    if [[ -f "${FDB_HOME}/fdbmonitor.pid" ]]; then
        local fdb_pid

        fdb_pid=$(cat "${FDB_HOME}/fdbmonitor.pid")
        if ps -p "${fdb_pid}" >/dev/null; then
            echo "fdbmonitor with pid ${fdb_pid} is running, stop it first."
            exit 1
        fi
    fi
}

function clean_fdb() {
    if [[ -f "${FDB_HOME}/fdbmonitor.pid" ]]; then
        local fdb_pid

        fdb_pid=$(cat "${FDB_HOME}/fdbmonitor.pid")
        if ps -p "${fdb_pid}" >/dev/null; then
            echo "fdbmonitor with pid ${fdb_pid} is running, stop it first."
            exit 1
        fi
    fi

    sleep 1

    # Check if FDB_HOME is set and not root
    if [[ -z "${FDB_HOME}" || "${FDB_HOME}" == "/" ]]; then
        echo "Error: FDB_HOME is not set or is set to root directory. Aborting cleanup."
        exit 1
    fi

    # Check if FDB_HOME is empty
    if [[ -z "$(ls -A "${FDB_HOME}")" ]]; then
        echo "Error: FDB_HOME is empty. Nothing to clean."
        exit 1
    fi

    # Remove all directories and files under ${FDB_HOME}
    echo "Removing all directories and files under ${FDB_HOME}"
    rm -rf "${FDB_HOME:?}"/*
}

function deploy() {
    local job="$1"

    if [[ ${job} =~ ^(all|fdb)$ ]]; then
        deploy_fdb
    fi
}

function start() {
    local job="$1"
    local init="$2"

    if [[ ${job} =~ ^(all|fdb)$ ]]; then
        start_fdb
    fi

    if [[ ${init} =~ ^(all|fdb)$ ]]; then
        local fdb_mode

        fdb_mode=$(get_fdb_mode)

        echo "Try create database in fdb ${fdb_mode}"

        "${FDB_HOME}/fdbcli" -C "${FDB_HOME}/conf/fdb.cluster" \
            --exec "configure new ${fdb_mode} ssd" ||
            "${FDB_HOME}/fdbcli" -C "${FDB_HOME}/conf/fdb.cluster" --exec "status" ||
            (echo "failed to start fdb, please check that all nodes have same FDB_CLUSTER_ID" &&
                exit 1)
    fi

    echo "Start fdb success, and you can set conf for MetaService:"
    echo "fdb_cluster = $(cat "${FDB_HOME}"/conf/fdb.cluster)"
}

function stop() {
    local job="$1"

    if [[ ${job} =~ ^(all|fdb)$ ]]; then
        stop_fdb &
    fi
    wait
}

function clean() {
    local job="$1"

    if [[ ${job} =~ ^(all|fdb)$ ]]; then
        clean_fdb &
    fi
    wait
}

function status() {
    pgrep -f "${FDB_CLUSTER_DESC}"
}

function usage() {
    echo "Usage: $0 <CMD> "
    echo -e "\t deploy \t setup fdb env (dir, binary, conf ...)"
    echo -e "\t clean  \t clean fdb data"
    echo -e "\t start  \t start fdb"
    echo -e "\t stop   \t stop fdb"
    echo -e "\t fdbcli \t execute fdbcli"
    echo -e ""
    exit 1
}

function unknown_cmd() {
    local cmd="$1"

    printf "Unknown cmd: %s \n" "${cmd}"
    usage
}

if [[ $# -lt 1 ]]; then
    usage
fi

cmd="$1"
shift

job="fdb"

init="fdb"

case ${cmd} in
deploy)
    deploy "${job}"
    ;;
start)
    start "${job}" "${init}"
    ;;
stop)
    stop "${job}"
    ;;
clean)
    clean "${job}"
    ;;
fdbcli)
    "${FDB_HOME}/fdbcli" -C "${FDB_HOME}/conf/fdb.cluster" "$@"
    ;;
config)
    generate_regression_config true
    ;;
*)
    unknown_cmd "${cmd}"
    ;;
esac
