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
        echo "FDB ${FDB_VERSION} already exists"
        return
    fi

    local URL="https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/"
    local TMP="${FDB_PKG_DIR}-tmp"

    rm -rf "${TMP}"
    mkdir -p "${TMP}"

    wget "${URL}/fdbbackup.x86_64" -O "${TMP}/fdbbackup"
    wget "${URL}/fdbserver.x86_64" -O "${TMP}/fdbserver"
    wget "${URL}/fdbcli.x86_64" -O "${TMP}/fdbcli"
    wget "${URL}/fdbmonitor.x86_64" -O "${TMP}/fdbmonitor"
    wget "${URL}/libfdb_c.x86_64.so" -O "${TMP}/libfdb_c.x86_64.so"
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
    # local memory_gb=$1
    local cpu_cores=$2

    local min_processes=1
    local data_dir_count

    # Convert comma-separated DATA_DIRS into an array
    IFS=',' read -r -a DATA_DIR_ARRAY <<<"${DATA_DIRS}"
    data_dir_count=${#DATA_DIR_ARRAY[@]}

    # Stateless processes (at least 1, up to 1/4 of CPU cores)
    local stateless_processes=$((cpu_cores / 4))
    [[ ${stateless_processes} -lt ${min_processes} ]] && stateless_processes=${min_processes}

    # Storage processes (must be a multiple of the number of data directories)
    local storage_processes=$((cpu_cores / 4))
    [[ ${storage_processes} -lt ${data_dir_count} ]] && storage_processes=${data_dir_count}
    storage_processes=$(((storage_processes / data_dir_count) * data_dir_count))

    # Transaction processes (must be a multiple of the number of data directories)
    local transaction_processes=$((cpu_cores / 8))
    [[ ${transaction_processes} -lt ${min_processes} ]] && transaction_processes=${min_processes}
    [[ ${transaction_processes} -lt ${data_dir_count} ]] && transaction_processes=${data_dir_count}
    transaction_processes=$(((transaction_processes / data_dir_count) * data_dir_count))

    # Return the values
    echo "${stateless_processes} ${storage_processes} ${transaction_processes}"
}

function deploy_fdb() {
    download_fdb

    ln -sf "${FDB_PKG_DIR}/fdbserver" "${FDB_HOME}/fdbserver"
    ln -sf "${FDB_PKG_DIR}/fdbmonitor" "${FDB_HOME}/fdbmonitor"
    ln -sf "${FDB_PKG_DIR}/fdbbackup" "${FDB_HOME}/backup_agent"
    ln -sf "${FDB_PKG_DIR}/fdbcli" "${FDB_HOME}/fdbcli"

    CLUSTER_DESC="${FDB_CLUSTER_DESC:-${FDB_CLUSTER_ID}}"

    # Convert comma-separated DATA_DIRS into an array
    IFS=',' read -r -a DATA_DIR_ARRAY <<<"${DATA_DIRS}"
    for DIR in "${DATA_DIR_ARRAY[@]}"; do
        mkdir -p "${DIR}" || handle_error "Failed to create data directory ${DIR}"
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
    read -r stateless_processes storage_processes transaction_processes <<<"$(calculate_process_numbers "${MEMORY_LIMIT_GB}" "${CPU_CORES_LIMIT}")"

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

    # Add transaction processes
    for ((i = 0; i < transaction_processes; i++)); do
        PORT=$((FDB_PORT + i))
        DIR_INDEX=$((i % STORAGE_DIR_COUNT))
        echo "[fdbserver.${PORT}]
class = transaction
datadir = ${DATA_DIR_ARRAY[${DIR_INDEX}]}/${PORT}" | tee -a "${FDB_HOME}/conf/fdb.conf" >/dev/null
    done

    echo "[backup_agent]
command = ${FDB_HOME}/backup_agent
logdir = ${LOG_DIR}" >>"${FDB_HOME}/conf/fdb.conf"

    echo "Deploy FDB to: ${FDB_HOME}"
}

function start_fdb() {
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
    if [[ -f "${FDB_HOME}/fdbmonitor.pid" ]]; then
        local fdb_pid
        fdb_pid=$(cat "${FDB_HOME}/fdbmonitor.pid")
        if ps -p "${fdb_pid}" >/dev/null; then
            echo "Stop fdbmonitor with pid ${fdb_pid}"
            kill -9 "${fdb_pid}"
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
    local skip_pkg="$2"
    local skip_config="$3"

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
        echo "Try create database ..."
        local fdb_mode

        fdb_mode=$(get_fdb_mode)
        "${FDB_HOME}/fdbcli" -C "${FDB_HOME}/conf/fdb.cluster" \
            --exec "configure new ${fdb_mode} ssd" || true
    fi

    echo "Start fdb success, and the cluster is:"
    cat "${FDB_HOME}/conf/fdb.cluster"
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
    echo "Usage: $0 <CMD> [--skip-pkg] [--skip-config]"
    echo -e "\t deploy \t setup fdb env (dir, binary, conf ...)"
    echo -e "\t clean  \t clean fdb data"
    echo -e "\t start  \t start fdb"
    echo -e "\t stop   \t stop fdb"
    echo -e ""
    echo -e ""
    echo -e "Args:"
    echo -e "\t --skip-pkg    \t skip to update binary pkgs during deploy"
    echo -e "\t --skip-config \t skip to update config during deploy"
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
skip_pkg="false"
skip_config="false"

case ${cmd} in
deploy)
    deploy "${job}" "${skip_pkg}" "${skip_config}"
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
