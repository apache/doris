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

################################################################
# This script will restart all thirdparty containers
################################################################

set -eo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

. "${ROOT}/custom_settings.env"
. "${ROOT}/juicefs-helpers.sh"
. "${ROOT}/docker-health.sh"
. "${ROOT}/docker-compose/hive/scripts/bootstrap/bootstrap-groups.sh"

usage() {
    echo "
Usage: $0 <options>
  Optional options:
     [no option]        start all components
     --help,-h          show this usage
     -c mysql           start MySQL
     -c mysql,hive3     start MySQL and Hive3
     --stop             stop the specified components
     --reserve-ports    reserve host ports by setting 'net.ipv4.ip_local_reserved_ports' to avoid port already bind error
     --no-load-data     do not load data into the components
     --load-parallel <num>  set the parallel number to load data, default is the 50% of CPU cores
     --hive-mode <mode>     hive startup mode: fast, refresh, rebuild
     --hive-modules <list>  comma separated hive modules to refresh

  All valid components:
    mysql,pg,oracle,sqlserver,clickhouse,es,hive2,hive3,iceberg,iceberg-rest,hudi,kafka,mariadb,db2,oceanbase,lakesoul,kerberos,ranger,polaris
  "
    exit 1
}
DEFAULT_COMPONENTS="mysql,es,hive2,hive3,pg,oracle,sqlserver,clickhouse,mariadb,iceberg,hudi,db2,oceanbase,kerberos,minio"
ALL_COMPONENTS="${DEFAULT_COMPONENTS},kafka,lakesoul,ranger,polaris"
COMPONENTS=$2
HELP=0
STOP=0
NEED_RESERVE_PORTS=0
export NEED_LOAD_DATA=1
export LOAD_PARALLEL=$(( $(getconf _NPROCESSORS_ONLN) / 2 ))
export START_PROGRESS_INTERVAL="${START_PROGRESS_INTERVAL:-60}"
export HIVE_HQL_PARALLEL="${HIVE_HQL_PARALLEL:-4}"
export START_CLEANUP_ON_FAILURE="${START_CLEANUP_ON_FAILURE:-1}"
export HIVE_MODE="${HIVE_MODE:-refresh}"
export HIVE_MODULES="${HIVE_MODULES:-all}"
HIVE_SHARED_ID="doris-shared"
: "${HIVE_BASELINE_VERSION:?HIVE_BASELINE_VERSION must be set in custom_settings.env}"
: "${HIVE_BASELINE_TARBALL_CACHE:?HIVE_BASELINE_TARBALL_CACHE must be set in custom_settings.env}"

if [[ -z "${IP_HOST:-}" ]]; then
    if command -v ip >/dev/null 2>&1; then
        export IP_HOST=$(ip -4 addr show scope global | awk '/inet / {print $2}' | cut -d/ -f1 | head -n 1)
    elif command -v hostname >/dev/null 2>&1; then
        export IP_HOST=$(hostname -I 2>/dev/null | awk '{print $1}')
    fi
fi

if ! OPTS="$(getopt \
    -n "$0" \
    -o '' \
    -l 'help' \
    -l 'stop' \
    -l 'reserve-ports' \
    -l 'no-load-data' \
    -l 'load-parallel:' \
    -l 'hive-mode:' \
    -l 'hive-modules:' \
    -o 'hc:' \
    -- "$@")"; then
    usage
fi

eval set -- "${OPTS}"

if [[ "$#" == 1 ]]; then
    # default
    COMPONENTS="${DEFAULT_COMPONENTS}"
else
    while true; do
        case "$1" in
        -h)
            HELP=1
            shift
            ;;
        --help)
            HELP=1
            shift
            ;;
        --stop)
            STOP=1
            shift
            ;;
        -c)
            COMPONENTS=$2
            shift 2
            ;;
        --reserve-ports)
            NEED_RESERVE_PORTS=1
            shift
            ;;
        --no-load-data)
            export NEED_LOAD_DATA=0
            shift
            ;;
        --load-parallel)
            export LOAD_PARALLEL=$2
            shift 2
            ;;
        --hive-mode)
            export HIVE_MODE=$2
            shift 2
            ;;
        --hive-modules)
            export HIVE_MODULES=$2
            shift 2
            ;;
        --)
            shift
            break
            ;;
        *)
            echo "Internal error"
            exit 1
            ;;
        esac
    done
    if [[ "${COMPONENTS}"x == ""x ]]; then
        if [[ "${STOP}" -eq 1 ]]; then
            COMPONENTS="${ALL_COMPONENTS}"
        fi
        if [[ "${NEED_RESERVE_PORTS}" -eq 1 ]]; then
            COMPONENTS="${DEFAULT_COMPONENTS}"
        fi
    fi
fi

if [[ "${HELP}" -eq 1 ]]; then
    usage
fi

if [[ "${COMPONENTS}"x == ""x ]]; then
    echo "Invalid arguments"
    echo ${COMPONENTS}
    usage
fi

if [[ "${CONTAINER_UID}"x == "doris--"x ]]; then
    echo "Must set CONTAINER_UID to a unique name in custom_settings.env"
    exit 1
fi

if [[ -z "${HIVE_HOST_ALIAS:-}" ]]; then
    export HIVE_HOST_ALIAS="hadoop-master-${HIVE_SHARED_ID}"
fi

echo "Components are: ${COMPONENTS}"
echo "Container UID: ${CONTAINER_UID}"
echo "Stop: ${STOP}"
echo "Start progress interval: ${START_PROGRESS_INTERVAL}"
echo "Hive mode: ${HIVE_MODE}"
echo "Hive modules: ${HIVE_MODULES}"
echo "Hive HQL parallel: ${HIVE_HQL_PARALLEL}"
echo "Hive host alias: ${HIVE_HOST_ALIAS}"

if ! [[ "${START_PROGRESS_INTERVAL}" =~ ^[1-9][0-9]*$ ]]; then
    echo "Invalid start progress interval: ${START_PROGRESS_INTERVAL}"
    usage
fi

if ! [[ "${HIVE_HQL_PARALLEL}" =~ ^[1-9][0-9]*$ ]]; then
    echo "Invalid hive HQL parallel: ${HIVE_HQL_PARALLEL}"
    usage
fi

case "${START_CLEANUP_ON_FAILURE}" in
0|1)
    ;;
*)
    echo "Invalid start cleanup on failure: ${START_CLEANUP_ON_FAILURE}"
    usage
    ;;
esac

case "${HIVE_MODE}" in
fast|refresh|rebuild)
    ;;
*)
    echo "Invalid hive mode: ${HIVE_MODE}"
    usage
    ;;
esac

OLD_IFS="${IFS}"
IFS=','
read -r -a COMPONENTS_ARR <<<"${COMPONENTS}"
IFS="${OLD_IFS}"

RUN_MYSQL=0
RUN_PG=0
RUN_ORACLE=0
RUN_SQLSERVER=0
RUN_CLICKHOUSE=0
RUN_HIVE2=0
RUN_HIVE3=0
RUN_ES=0
RUN_ICEBERG=0
RUN_ICEBERG_REST=0
RUN_HUDI=0
RUN_KAFKA=0
RUN_MARIADB=0
RUN_DB2=0
RUN_OCEANBASE=0
RUN_LAKESOUL=0
RUN_KERBEROS=0
RUN_MINIO=0
RUN_RANGER=0
RUN_POLARIS=0

RESERVED_PORTS="65535"

for element in "${COMPONENTS_ARR[@]}"; do
    if [[ "${element}"x == "mysql"x ]]; then
        RUN_MYSQL=1
    elif [[ "${element}"x == "pg"x ]]; then
        RUN_PG=1
    elif [[ "${element}"x == "oracle"x ]]; then
        RUN_ORACLE=1
    elif [[ "${element}"x == "sqlserver"x ]]; then
        RUN_SQLSERVER=1
    elif [[ "${element}"x == "clickhouse"x ]]; then
        RUN_CLICKHOUSE=1
    elif [[ "${element}"x == "es"x ]]; then
        RUN_ES=1
    elif [[ "${element}"x == "hive2"x ]]; then
        RUN_HIVE2=1
        RESERVED_PORTS="${RESERVED_PORTS},50070,50075" # namenode and datanode ports
    elif [[ "${element}"x == "hive3"x ]]; then
        RUN_HIVE3=1
    elif [[ "${element}"x == "kafka"x ]]; then
        RUN_KAFKA=1
    elif [[ "${element}"x == "iceberg"x ]]; then
        RUN_ICEBERG=1
    elif [[ "${element}"x == "iceberg-rest"x ]]; then
        RUN_ICEBERG_REST=1
    elif [[ "${element}"x == "hudi"x ]]; then
        RUN_HUDI=1
        RESERVED_PORTS="${RESERVED_PORTS},19083,19100,19101,18080"
    elif [[ "${element}"x == "mariadb"x ]]; then
        RUN_MARIADB=1
    elif [[ "${element}"x == "db2"x ]]; then
        RUN_DB2=1
    elif [[ "${element}"x == "oceanbase"x ]];then
        RUN_OCEANBASE=1
    elif [[ "${element}"x == "lakesoul"x ]]; then
        RUN_LAKESOUL=1
    elif [[ "${element}"x == "kerberos"x ]]; then
        RUN_KERBEROS=1
    elif [[ "${element}"x == "minio"x ]]; then
        RUN_MINIO=1
    elif [[ "${element}"x == "ranger"x ]]; then
        RUN_RANGER=1
    elif [[ "${element}"x == "polaris"x ]]; then
        RUN_POLARIS=1
    else
        echo "Invalid component: ${element}"
        usage
    fi
done

hive_bootstrap_groups_for() {
    local hive_version="$1"
    case "${hive_version}" in
    hive2)
        echo "common,hive2_only"
        ;;
    hive3)
        echo "common,hive3_only"
        ;;
    *)
        echo "Unsupported hive version: ${hive_version}" >&2
        return 1
        ;;
    esac
}

hive_requires_mysql_component() {
    local hive_version="$1"
    local jfs_meta=""
    local settings_env="${ROOT}/docker-compose/hive/hive-${hive_version#hive}x_settings.env"

    # shellcheck disable=SC1090
    . "${settings_env}"
    jfs_meta="${JFS_CLUSTER_META:-}"
    [[ "${jfs_meta}" == mysql://* ]] || return 1
    [[ "${jfs_meta}" == *"@(127.0.0.1:3316)/"* || "${jfs_meta}" == *"@(localhost:3316)/"* ]]
}

if [[ "${RUN_HIVE2}" -eq 1 ]] && hive_requires_mysql_component "hive2"; then
    RUN_MYSQL=1
fi
if [[ "${RUN_HIVE3}" -eq 1 ]] && hive_requires_mysql_component "hive3"; then
    RUN_MYSQL=1
fi

reserve_ports() {
    if [[ "${NEED_RESERVE_PORTS}" -eq 0 ]]; then
        return
    fi

    if [[ "${RESERVED_PORTS}"x != ""x ]]; then
        echo "Reserve ports: ${RESERVED_PORTS}"
        sudo sysctl -w net.ipv4.ip_local_reserved_ports="${RESERVED_PORTS}"
    fi
}

JFS_META_FORMATTED=0
DORIS_ROOT="${DORIS_ROOT:-$(cd "${ROOT}/../.." &>/dev/null && pwd)}"
JUICEFS_RUNTIME_ROOT="${ROOT}/juicefs"
LOG_ROOT="${ROOT}/logs"

mkdir -p "${LOG_ROOT}"

JUICEFS_LOCAL_BIN="${JUICEFS_RUNTIME_ROOT}/bin/juicefs"

find_juicefs_hadoop_jar() {
    local -a jar_globs=(
        "${JUICEFS_RUNTIME_ROOT}/lib/juicefs-hadoop-[0-9]*.jar"
        "${DORIS_ROOT}/thirdparty/installed/juicefs_libs/juicefs-hadoop-[0-9]*.jar"
        "${DORIS_ROOT}/output/fe/lib/juicefs/juicefs-hadoop-[0-9]*.jar"
        "${DORIS_ROOT}/output/be/lib/java_extensions/juicefs/juicefs-hadoop-[0-9]*.jar"
        "${DORIS_ROOT}/../../../clusterEnv/*/Cluster*/fe/lib/juicefs/juicefs-hadoop-[0-9]*.jar"
        "${DORIS_ROOT}/../../../clusterEnv/*/Cluster*/be/lib/java_extensions/juicefs/juicefs-hadoop-[0-9]*.jar"
        "/mnt/ssd01/pipline/OpenSourceDoris/clusterEnv/*/Cluster*/fe/lib/juicefs/juicefs-hadoop-[0-9]*.jar"
        "/mnt/ssd01/pipline/OpenSourceDoris/clusterEnv/*/Cluster*/be/lib/java_extensions/juicefs/juicefs-hadoop-[0-9]*.jar"
    )
    juicefs_find_hadoop_jar_by_globs "${jar_globs[@]}"
}

detect_juicefs_version() {
    local juicefs_jar
    juicefs_jar=$(find_juicefs_hadoop_jar || true)
    juicefs_detect_hadoop_version "${juicefs_jar}" "${JUICEFS_DEFAULT_VERSION}"
}

download_juicefs_hadoop_jar() {
    local juicefs_version="$1"
    local cache_dir="${JUICEFS_RUNTIME_ROOT}/lib"
    juicefs_download_hadoop_jar_to_cache "${juicefs_version}" "${cache_dir}"
}

install_juicefs_cli() {
    local juicefs_version="$1"
    local cache_dir="${JUICEFS_RUNTIME_ROOT}/bin"
    local archive_name="juicefs-${juicefs_version}-linux-amd64.tar.gz"
    local download_url="https://github.com/juicedata/juicefs/releases/download/v${juicefs_version}/${archive_name}"
    local tmp_dir
    local extracted_bin

    mkdir -p "${cache_dir}"
    tmp_dir=$(mktemp -d "${cache_dir}/tmp.XXXXXX")

    echo "Downloading JuiceFS CLI ${juicefs_version} from ${download_url}" >&2
    if ! curl -fL --retry 3 --retry-delay 2 -o "${tmp_dir}/${archive_name}" "${download_url}"; then
        rm -rf "${tmp_dir}"
        echo "ERROR: failed to download JuiceFS CLI from ${download_url}" >&2
        return 1
    fi

    tar -xzf "${tmp_dir}/${archive_name}" -C "${tmp_dir}"
    extracted_bin=$(find "${tmp_dir}" -maxdepth 2 -type f -name juicefs | head -n 1)
    if [[ -z "${extracted_bin}" ]]; then
        rm -rf "${tmp_dir}"
        echo "ERROR: failed to locate extracted JuiceFS CLI in ${archive_name}" >&2
        return 1
    fi

    install -m 0755 "${extracted_bin}" "${JUICEFS_LOCAL_BIN}"
    rm -rf "${tmp_dir}"
}

resolve_juicefs_cli() {
    local juicefs_version

    if command -v juicefs >/dev/null 2>&1; then
        command -v juicefs
        return 0
    fi

    if [[ -x "${JUICEFS_LOCAL_BIN}" ]]; then
        echo "${JUICEFS_LOCAL_BIN}"
        return 0
    fi

    juicefs_version=$(detect_juicefs_version)
    install_juicefs_cli "${juicefs_version}" || return 1
    echo "${JUICEFS_LOCAL_BIN}"
}

ensure_juicefs_meta_database() {
    local jfs_meta="$1"
    local meta_db
    local mysql_container
    local pg_container
    local -a pg_candidates

    meta_db="${jfs_meta##*/}"
    meta_db="${meta_db%%\?*}"
    if [[ ! "${meta_db}" =~ ^[A-Za-z0-9_]+$ ]]; then
        echo "WARN: skip JuiceFS metadata database creation for unsafe database name '${meta_db}'." >&2
        return 0
    fi

    if [[ "${jfs_meta}" == mysql://* ]]; then
        if [[ "${jfs_meta}" != *"@(127.0.0.1:3316)/"* && "${jfs_meta}" != *"@(localhost:3316)/"* ]]; then
            return 0
        fi

        mysql_container=$(sudo docker ps --format '{{.Names}}' | grep -E "(^|-)${CONTAINER_UID}mysql_57(-[0-9]+)?$" | head -n 1 || true)
        if [[ -n "${mysql_container}" ]]; then
            if sudo docker exec "${mysql_container}" \
                mysql -uroot -p123456 -e "CREATE DATABASE IF NOT EXISTS \`${meta_db}\`;" >/dev/null 2>&1; then
                return 0
            fi
            echo "WARN: docker mysql ${mysql_container} is unavailable for JuiceFS metadata init." >&2
            return 0
        fi

        echo "WARN: docker mysql_57 is not running; skip eager JuiceFS metadata database creation for ${meta_db}." >&2
        return 0
    fi

    if [[ "${jfs_meta}" == postgres://* || "${jfs_meta}" == postgresql://* ]]; then
        if [[ "${jfs_meta}" != *"@127.0.0.1:"* && "${jfs_meta}" != *"@localhost:"* ]]; then
            return 0
        fi

        pg_candidates=(
            "hive3-metastore-postgresql"
            "hive2-metastore-postgresql"
            "${CONTAINER_UID}postgres"
            "postgres"
        )

        for pg_container in "${pg_candidates[@]}"; do
            if ! sudo docker ps --format '{{.Names}}' | grep -Fxq "${pg_container}"; then
                continue
            fi

            if sudo docker exec "${pg_container}" \
                psql -U postgres -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='${meta_db}'" | grep -q '^1$'; then
                return 0
            fi

            if sudo docker exec "${pg_container}" \
                psql -U postgres -d postgres -c "CREATE DATABASE \"${meta_db}\";" >/dev/null 2>&1; then
                return 0
            fi

            echo "WARN: docker postgres ${pg_container} is unavailable for JuiceFS metadata init." >&2
            return 0
        done

        echo "WARN: no local postgres container for JuiceFS metadata init; skip eager database creation for ${meta_db}." >&2
        return 0
    fi

    return 0
}

run_juicefs_cli() {
    local juicefs_cli
    if ! juicefs_cli=$(resolve_juicefs_cli); then
        echo "ERROR: JuiceFS CLI is not available (download failed or binary not found)" >&2
        return 1
    fi
    "${juicefs_cli}" "$@"
}

ensure_juicefs_hadoop_jar_for_hive() {
    local auxlib_dir="${ROOT}/docker-compose/hive/scripts/auxlib"
    local source_jar
    local juicefs_version

    source_jar=$(find_juicefs_hadoop_jar || true)
    if [[ -z "${source_jar}" ]]; then
        juicefs_version=$(detect_juicefs_version)
        source_jar=$(download_juicefs_hadoop_jar "${juicefs_version}" || true)
    fi

    if [[ -z "${source_jar}" ]]; then
        echo "WARN: skip syncing juicefs-hadoop jar for hive, not found and download failed."
        return 0
    fi

    mkdir -p "${auxlib_dir}"
    if [[ "${source_jar}" == "${auxlib_dir}/$(basename "${source_jar}")" ]]; then
        echo "JuiceFS Hadoop jar already exists in hive auxlib: $(basename "${source_jar}")"
        return 0
    fi
    cp -f "${source_jar}" "${auxlib_dir}/"
    echo "Synced JuiceFS Hadoop jar to hive auxlib: $(basename "${source_jar}")"
}

prepare_juicefs_meta_for_hive() {
    local jfs_meta="$1"
    local jfs_cluster_name="${2:-cluster}"
    if [[ -z "${jfs_meta}" ]]; then
        return 0
    fi
    if [[ "${jfs_meta}" != mysql://* && "${jfs_meta}" != postgres://* && "${jfs_meta}" != postgresql://* ]]; then
        return 0
    fi
    if [[ "${JFS_META_FORMATTED}" -eq 1 ]]; then
        return 0
    fi

    # JuiceFS CLI is required; if unavailable (e.g. no network), skip gracefully.
    if ! resolve_juicefs_cli >/dev/null 2>&1; then
        echo "WARN: JuiceFS CLI not available; skipping JuiceFS metadata init for ${jfs_meta}." >&2
        echo "WARN: JuiceFS-dependent tests will fail. Ensure juicefs binary is on PATH or network access to github.com is available." >&2
        return 0
    fi

    local bucket_dir="${JFS_BUCKET_DIR:-/tmp/jfs-bucket}"
    sudo mkdir -p "${bucket_dir}"
    sudo chmod 777 "${bucket_dir}"

    # For local docker metadata DSNs (mysql/postgresql), ensure metadata database exists.
    ensure_juicefs_meta_database "${jfs_meta}"

    if run_juicefs_cli status "${jfs_meta}" >/dev/null 2>&1; then
        echo "JuiceFS metadata is already formatted."
        JFS_META_FORMATTED=1
        return 0
    fi

    # Clean stale bucket data before formatting. When meta is not formatted,
    # any leftover data in the bucket directory is orphaned from a previous run
    # and will cause "juicefs format" to fail with "Storage ... is not empty".
    if [[ -d "${bucket_dir}" ]]; then
        echo "Cleaning stale JuiceFS bucket directory: ${bucket_dir}"
        sudo rm -rf "${bucket_dir:?}"/*
    fi

    if ! run_juicefs_cli \
        format --storage file --bucket "${bucket_dir}" "${jfs_meta}" "${jfs_cluster_name}"; then
        # If format reports conflict on rerun, verify by status and continue.
        run_juicefs_cli status "${jfs_meta}" >/dev/null 2>&1 || true
    fi

    JFS_META_FORMATTED=1
}

render_uid_template() {
    local template_file="$1"
    local output_file="$2"
    local replacement="${CONTAINER_UID//\\/\\\\}"

    replacement="${replacement//&/\\&}"
    replacement="${replacement//|/\\|}"

    sed "s|doris--|${replacement}|g" "${template_file}" >"${output_file}"
}

compose_cmd() {
    local compose_file="$1"
    local env_file="$2"
    shift 2

    if [[ -n "${env_file}" ]]; then
        sudo docker compose -f "${compose_file}" --env-file "${env_file}" "$@"
    else
        sudo docker compose -f "${compose_file}" "$@"
    fi
}

compose_down_stack() {
    local compose_file="$1"
    local env_file="$2"
    shift 2

    compose_cmd "${compose_file}" "${env_file}" down "$@"
}

compose_up_stack() {
    local compose_file="$1"
    local env_file="$2"
    shift 2

    compose_cmd "${compose_file}" "${env_file}" up "$@"
}

reset_data_dirs() {
    local data_dir

    for data_dir in "$@"; do
        sudo mkdir -p "${data_dir}"
        sudo rm -rf "${data_dir:?}"/*
    done
}

declare -A START_PIDS=()
declare -A START_LOGS=()
declare -A START_COMPOSE_FILES=()
declare -A START_ENV_FILES=()
declare -A START_DONE=()
START_ORDER=()

register_stack_metadata() {
    local component="$1"
    local compose_file="$2"
    local env_file="${3:-}"

    START_COMPOSE_FILES["${component}"]="${compose_file}"
    START_ENV_FILES["${component}"]="${env_file}"
}

start_rendered_compose_stack() {
    local component="$1"
    local template_file="$2"
    local compose_file="$3"
    local env_file="$4"
    shift 4

    local stage="up"
    local -a up_args=()
    local -a reset_dirs=()

    while (($#)); do
        if [[ "$1" == "--" ]]; then
            stage="reset"
            shift
            continue
        fi

        if [[ "${stage}" == "up" ]]; then
            up_args+=("$1")
        else
            reset_dirs+=("$1")
        fi
        shift
    done

    render_uid_template "${template_file}" "${compose_file}"
    register_stack_metadata "${component}" "${compose_file}" "${env_file}"
    compose_down_stack "${compose_file}" "${env_file}" --remove-orphans

    if [[ "${STOP}" -eq 1 ]]; then
        return 0
    fi

    if (( ${#reset_dirs[@]} > 0 )); then
        reset_data_dirs "${reset_dirs[@]}"
    fi

    if (( ${#up_args[@]} == 0 )); then
        up_args=(-d --wait)
    fi

    compose_up_stack "${compose_file}" "${env_file}" "${up_args[@]}"
}

register_job() {
    local component="$1"
    local pid="$2"
    local log_file="$3"

    START_PIDS["${component}"]="${pid}"
    START_LOGS["${component}"]="${log_file}"
    START_DONE["${component}"]=0
    START_ORDER+=("${component}")
}

cleanup_started_stacks() {
    local component
    local compose_file
    local env_file
    local i

    [[ "${START_CLEANUP_ON_FAILURE}" -eq 1 ]] || return 0

    for ((i = ${#START_ORDER[@]} - 1; i >= 0; --i)); do
        component="${START_ORDER[$i]}"
        compose_file="${START_COMPOSE_FILES["${component}"]:-}"
        env_file="${START_ENV_FILES["${component}"]:-}"
        [[ -n "${compose_file}" ]] || continue
        echo "Cleaning component '${component}' after startup failure" >&2
        compose_down_stack "${compose_file}" "${env_file}" --remove-orphans >/dev/null 2>&1 || true
    done
}

wait_remaining_jobs_quietly() {
    local component
    local pid

    for component in "${START_ORDER[@]}"; do
        [[ "${START_DONE["${component}"]:-0}" -eq 0 ]] || continue
        pid="${START_PIDS["${component}"]:-}"
        [[ -n "${pid}" ]] || continue
        wait "${pid}" >/dev/null 2>&1 || true
        START_DONE["${component}"]=1
    done
}

handle_start_failure() {
    local component="$1"
    local status="$2"

    dump_start_failure "${component}" "${status}"
    kill_running_jobs
    wait_remaining_jobs_quietly
    cleanup_started_stacks
}

collect_one_finished_job() {
    local component
    local pid
    local status

    for component in "${START_ORDER[@]}"; do
        [[ "${START_DONE["${component}"]:-0}" -eq 0 ]] || continue
        pid="${START_PIDS["${component}"]:-}"
        [[ -n "${pid}" ]] || continue
        if kill -0 "${pid}" >/dev/null 2>&1; then
            continue
        fi

        status=0
        wait "${pid}" || status=$?
        START_DONE["${component}"]=1
        if [[ "${status}" -ne 0 ]]; then
            handle_start_failure "${component}" "${status}"
            return 1
        fi
        return 0
    done

    return 2
}

launch_component() {
    local component="$1"
    local log_file="$2"
    shift 2

    echo "Launching ${component}, log => ${log_file}"
    "$@" >"${log_file}" 2>&1 &
    register_job "${component}" "$!" "${log_file}"
}

kill_running_jobs() {
    local component
    local pid

    for component in "${START_ORDER[@]}"; do
        [[ "${START_DONE["${component}"]:-0}" -eq 0 ]] || continue
        pid="${START_PIDS["${component}"]:-}"
        [[ -n "${pid}" ]] || continue
        kill "${pid}" >/dev/null 2>&1 || true
    done
}

print_wait_progress() {
    local component
    local pid
    local log_file

    echo "Still waiting for docker components:"
    for component in "${START_ORDER[@]}"; do
        [[ "${START_DONE["${component}"]:-0}" -eq 0 ]] || continue
        pid="${START_PIDS["${component}"]:-}"
        [[ -n "${pid}" ]] || continue
        if ! kill -0 "${pid}" >/dev/null 2>&1; then
            continue
        fi

        log_file="${START_LOGS["${component}"]:-}"
        echo "  ${component} (pid=${pid}, log=${log_file})"
        if [[ -n "${log_file}" && -f "${log_file}" ]]; then
            echo "  ----- ${component} log tail -----"
            tail -n 20 "${log_file}" || true
            echo "  ----- end ${component} log tail -----"
        fi
    done
}

dump_start_failure() {
    local component="$1"
    local status="$2"
    local log_file="${START_LOGS["${component}"]}"
    local compose_file="${START_COMPOSE_FILES["${component}"]:-}"
    local env_file="${START_ENV_FILES["${component}"]:-}"

    echo "ERROR: docker component '${component}' failed with exit code ${status}" >&2
    echo "ERROR: start log file: ${log_file}" >&2
    echo "===== ${component} start log (tail -200) =====" >&2
    tail -n 200 "${log_file}" >&2 || true

    if [[ -n "${compose_file}" ]]; then
        echo "===== ${component} docker compose ps =====" >&2
        compose_cmd "${compose_file}" "${env_file}" ps >&2 || true
        echo "===== ${component} docker compose logs (tail -200) =====" >&2
        compose_cmd "${compose_file}" "${env_file}" logs --no-color --tail 200 >&2 || true
    fi

    echo "===== unhealthy containers =====" >&2
    sudo docker ps -a --filter 'health=unhealthy' --format '{{.Names}} | {{.Image}} | {{.Status}}' >&2 || true
}

print_started_summary() {
    local component
    local compose_file
    local env_file
    local compose_ids
    local container_id

    echo "===== started components summary ====="
    for component in "${START_ORDER[@]}"; do
        compose_file="${START_COMPOSE_FILES["${component}"]:-}"
        env_file="${START_ENV_FILES["${component}"]:-}"

        echo "component: ${component}"
        echo "  log: ${START_LOGS["${component}"]}"
        echo "  compose: ${compose_file}"
        if [[ -n "${env_file}" ]]; then
            echo "  env: ${env_file}"
        fi
        echo "  compose ps:"
        compose_cmd "${compose_file}" "${env_file}" ps 2>/dev/null || true

        compose_ids="$(compose_cmd "${compose_file}" "${env_file}" ps -q 2>/dev/null || true)"
        if [[ -n "${compose_ids}" ]]; then
            echo "  containers:"
            while read -r container_id; do
                [[ -n "${container_id}" ]] || continue
                sudo docker inspect --format '{{.Name}} | {{.Config.Image}} | {{.State.Status}} | health={{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "${container_id}" \
                    2>/dev/null || true
            done <<<"${compose_ids}"
        fi
    done
}

wait_for_started_jobs() {
    local component
    local remaining_count
    local collect_status
    local last_progress_ts
    local now_ts

    last_progress_ts="$(date +%s)"

    while true; do
        remaining_count=0
        for component in "${START_ORDER[@]}"; do
            if [[ "${START_DONE["${component}"]:-0}" -eq 0 ]]; then
                remaining_count=$((remaining_count + 1))
            fi
        done

        if (( remaining_count == 0 )); then
            return 0
        fi

        collect_status=0
        collect_one_finished_job || collect_status=$?
        if [[ "${collect_status}" -eq 1 ]]; then
            return 1
        fi
        if [[ "${collect_status}" -eq 2 ]]; then
            now_ts="$(date +%s)"
            if (( now_ts - last_progress_ts >= START_PROGRESS_INTERVAL )); then
                print_wait_progress
                last_progress_ts="${now_ts}"
            fi
            sleep 1
        fi
    done
}

start_es() {
    # elasticsearch
    render_uid_template "${ROOT}/docker-compose/elasticsearch/es.yaml.tpl" "${ROOT}/docker-compose/elasticsearch/es.yaml"
    register_stack_metadata "es" "${ROOT}/docker-compose/elasticsearch/es.yaml" "${ROOT}/docker-compose/elasticsearch/es.env"
    compose_down_stack "${ROOT}/docker-compose/elasticsearch/es.yaml" "${ROOT}/docker-compose/elasticsearch/es.env" --remove-orphans
    if [[ "${STOP}" -ne 1 ]]; then
        sudo mkdir -p "${ROOT}"/docker-compose/elasticsearch/data/es6/
        sudo rm -rf "${ROOT}"/docker-compose/elasticsearch/data/es6/*
        sudo mkdir -p "${ROOT}"/docker-compose/elasticsearch/data/es7/
        sudo rm -rf "${ROOT}"/docker-compose/elasticsearch/data/es7/*
        sudo mkdir -p "${ROOT}"/docker-compose/elasticsearch/data/es8/
        sudo rm -rf "${ROOT}"/docker-compose/elasticsearch/data/es8/*
        sudo chmod -R 777 "${ROOT}"/docker-compose/elasticsearch/data
        sudo mkdir -p "${ROOT}"/docker-compose/elasticsearch/logs/es6/
        sudo rm -rf "${ROOT}"/docker-compose/elasticsearch/logs/es6/*
        sudo mkdir -p "${ROOT}"/docker-compose/elasticsearch/logs/es7/
        sudo rm -rf "${ROOT}"/docker-compose/elasticsearch/logs/es7/*
        sudo mkdir -p "${ROOT}"/docker-compose/elasticsearch/logs/es8/
        sudo rm -rf "${ROOT}"/docker-compose/elasticsearch/logs/es8/*
        sudo chmod -R 777 "${ROOT}"/docker-compose/elasticsearch/logs
        sudo chmod -R 777 "${ROOT}"/docker-compose/elasticsearch/config
        compose_cmd "${ROOT}/docker-compose/elasticsearch/es.yaml" "${ROOT}/docker-compose/elasticsearch/es.env" up -d --remove-orphans
    fi
}

start_mysql() {
    # mysql 5.7
    start_rendered_compose_stack "mysql" \
        "${ROOT}/docker-compose/mysql/mysql-5.7.yaml.tpl" \
        "${ROOT}/docker-compose/mysql/mysql-5.7.yaml" \
        "${ROOT}/docker-compose/mysql/mysql-5.7.env" \
        -d --wait -- \
        "${ROOT}/docker-compose/mysql/data"
}

start_pg() {
    # pg 14
    start_rendered_compose_stack "pg" \
        "${ROOT}/docker-compose/postgresql/postgresql-14.yaml.tpl" \
        "${ROOT}/docker-compose/postgresql/postgresql-14.yaml" \
        "${ROOT}/docker-compose/postgresql/postgresql-14.env" \
        -d --wait -- \
        "${ROOT}/docker-compose/postgresql/data/data"
}

start_oracle() {
    # oracle
    start_rendered_compose_stack "oracle" \
        "${ROOT}/docker-compose/oracle/oracle-11.yaml.tpl" \
        "${ROOT}/docker-compose/oracle/oracle-11.yaml" \
        "${ROOT}/docker-compose/oracle/oracle-11.env" \
        -d --wait -- \
        "${ROOT}/docker-compose/oracle/data"
}

start_db2() {
    # db2
    start_rendered_compose_stack "db2" \
        "${ROOT}/docker-compose/db2/db2.yaml.tpl" \
        "${ROOT}/docker-compose/db2/db2.yaml" \
        "${ROOT}/docker-compose/db2/db2.env" \
        -d --wait -- \
        "${ROOT}/docker-compose/db2/data"
}

start_oceanbase() {
    # oceanbase
    start_rendered_compose_stack "oceanbase" \
        "${ROOT}/docker-compose/oceanbase/oceanbase.yaml.tpl" \
        "${ROOT}/docker-compose/oceanbase/oceanbase.yaml" \
        "${ROOT}/docker-compose/oceanbase/oceanbase.env" \
        -d --wait -- \
        "${ROOT}/docker-compose/oceanbase/data"
}

start_sqlserver() {
    # sqlserver
    start_rendered_compose_stack "sqlserver" \
        "${ROOT}/docker-compose/sqlserver/sqlserver.yaml.tpl" \
        "${ROOT}/docker-compose/sqlserver/sqlserver.yaml" \
        "${ROOT}/docker-compose/sqlserver/sqlserver.env" \
        -d --wait -- \
        "${ROOT}/docker-compose/sqlserver/data"
}

start_clickhouse() {
    # clickhouse
    start_rendered_compose_stack "clickhouse" \
        "${ROOT}/docker-compose/clickhouse/clickhouse.yaml.tpl" \
        "${ROOT}/docker-compose/clickhouse/clickhouse.yaml" \
        "${ROOT}/docker-compose/clickhouse/clickhouse.env" \
        -d --wait -- \
        "${ROOT}/docker-compose/clickhouse/data"
}

start_kafka() {
    # kafka
    KAFKA_CONTAINER_ID="${CONTAINER_UID}kafka"
    render_uid_template "${ROOT}/docker-compose/kafka/kafka.yaml.tpl" "${ROOT}/docker-compose/kafka/kafka.yaml"
    sed -i "s/localhost/${IP_HOST}/g" "${ROOT}/docker-compose/kafka/kafka.yaml"
    register_stack_metadata "kafka" "${ROOT}/docker-compose/kafka/kafka.yaml" "${ROOT}/docker-compose/kafka/kafka.env"
    compose_down_stack "${ROOT}/docker-compose/kafka/kafka.yaml" "${ROOT}/docker-compose/kafka/kafka.env" --remove-orphans

    create_kafka_topics() {
        local container_id="$1"
        local ip_host="$2"
        local -a topics=("basic_data" "basic_array_data" "basic_data_with_errors" "basic_array_data_with_errors" "basic_data_timezone" "basic_array_data_timezone" "trino_kafka_basic_data")
        local topic

        for topic in "${topics[@]}"; do
            echo "Creating kafka topic '${topic}' for ${container_id}"
            sudo docker exec "${container_id}" bash -c "/opt/bitnami/kafka/bin/kafka-topics.sh --create --bootstrap-server '${ip_host}:19193' --topic '${topic}'"
        done

    }

    wait_for_kafka_ready() {
        local container_id="$1"
        local ip_host="$2"
        local attempt

        for attempt in {1..30}; do
            if sudo docker exec "${container_id}" bash -c "/opt/bitnami/kafka/bin/kafka-topics.sh --list --bootstrap-server '${ip_host}:19193'" >/dev/null 2>&1; then
                return 0
            fi
            sleep 2
        done

        echo "ERROR: kafka container '${container_id}' did not become ready on ${ip_host}:19193" >&2
        return 1
    }

    if [[ "${STOP}" -ne 1 ]]; then
        compose_up_stack "${ROOT}/docker-compose/kafka/kafka.yaml" "${ROOT}/docker-compose/kafka/kafka.env" --build --remove-orphans -d
        wait_for_kafka_ready "${KAFKA_CONTAINER_ID}" "${IP_HOST}"
        create_kafka_topics "${KAFKA_CONTAINER_ID}" "${IP_HOST}"
    fi
}

start_hive2() {
    start_hive_stack "hive2"
}

start_hive3() {
    start_hive_stack "hive3"
}

hive_volume_prefix_for() {
    local hive_version="$1"
    echo "${HIVE_SHARED_ID}-${hive_version}"
}

HIVE_VOLUME_SUFFIXES=(namenode datanode pgdata state)

log_hive_volumes() {
    local hive_version="$1"
    local prefix="$2"
    echo "[${hive_version}] volume_prefix=${prefix} volumes=$(IFS=,; echo "${HIVE_VOLUME_SUFFIXES[*]}")"
}

ensure_hive_volumes() {
    local prefix="$1"
    local suffix
    for suffix in "${HIVE_VOLUME_SUFFIXES[@]}"; do
        if ! sudo docker volume inspect "${prefix}-${suffix}" >/dev/null 2>&1; then
            sudo docker volume create "${prefix}-${suffix}" >/dev/null
        fi
    done
}

reset_hive_volumes() {
    local prefix="$1"
    local suffix
    for suffix in "${HIVE_VOLUME_SUFFIXES[@]}"; do
        sudo docker volume rm -f "${prefix}-${suffix}" >/dev/null 2>&1 || true
    done
}

hive_volume_is_populated() {
    local prefix="$1"
    sudo docker run --rm \
        -v "${prefix}-namenode:/vol:ro" \
        alpine test -f /vol/current/VERSION 2>/dev/null
}

maybe_restore_baseline_to_volumes() {
    local prefix="$1"
    local hive_version="${2:-hive3}"
    local baseline_cache="${HIVE_BASELINE_TARBALL_CACHE}"
    local cache_file="${baseline_cache}/${hive_version}-baseline-${HIVE_BASELINE_VERSION}.tar.gz"
    local extracted_dir="${baseline_cache}/${hive_version}-baseline-${HIVE_BASELINE_VERSION}"
    local extracted_ready_file="${extracted_dir}/.extract.ready"
    local remote_path="hive_baseline/${hive_version}-baseline-${HIVE_BASELINE_VERSION}.tar.gz"
    local download_url=""
    local tmp_cache_file=""
    local tmp_extract_dir=""

    HIVE_BASELINE_RESTORE_RESULT="missing"

    if [[ -n "${s3BucketName:-}" && -n "${s3Endpoint:-}" ]]; then
        download_url="https://${s3BucketName}.${s3Endpoint}/regression/datalake/pipeline_data/${remote_path}"
    fi

    # Nothing to do if the named volumes already hold a populated baseline.
    if hive_volume_is_populated "${prefix}"; then
        echo "[baseline] volumes already populated, skip restore"
        HIVE_BASELINE_RESTORE_RESULT="existing"
        return 0
    fi

    # Ensure a local tarball is available: prefer an intact cache, otherwise
    # download to a temporary file and atomically replace the cache. This avoids
    # persisting truncated tarballs when curl is interrupted on CI hosts.
    if [[ -f "${cache_file}" ]]; then
        if tar -tzf "${cache_file}" >/dev/null 2>&1; then
            echo "[baseline] using cached tarball: ${cache_file}"
        else
            echo "[baseline] cached tarball is corrupt, removing: ${cache_file}"
            rm -f "${cache_file}"
        fi
    fi

    if [[ ! -f "${cache_file}" ]]; then
        if [[ -z "${download_url}" ]]; then
            echo "[baseline] no baseline tarball available, will do full init"
            return 0
        fi
        mkdir -p "${baseline_cache}"
        tmp_cache_file="$(mktemp "${cache_file}.tmp.XXXXXX")"
        echo "[baseline] downloading baseline from ${download_url}"
        if ! curl -fSL -o "${tmp_cache_file}" "${download_url}"; then
            rm -f "${tmp_cache_file}"
            return 1
        fi
        if ! tar -tzf "${tmp_cache_file}" >/dev/null 2>&1; then
            echo "[baseline] downloaded tarball is corrupt: ${download_url}" >&2
            rm -f "${tmp_cache_file}"
            return 1
        fi
        mv -f "${tmp_cache_file}" "${cache_file}"
    fi

    # Cache the extracted baseline tree on disk so repeated refresh runs can
    # restore directly from files instead of paying the tar.gz decompression
    # cost every time.
    if [[ -f "${extracted_ready_file}" ]] \
        && [[ -d "${extracted_dir}/namenode" ]] \
        && [[ -d "${extracted_dir}/datanode" ]] \
        && [[ -d "${extracted_dir}/pgdata" ]] \
        && [[ -d "${extracted_dir}/state" ]]; then
        echo "[baseline] using cached extracted baseline: ${extracted_dir}"
    else
        if [[ -d "${extracted_dir}" ]]; then
            echo "[baseline] extracted baseline cache is incomplete, removing: ${extracted_dir}"
            rm -rf "${extracted_dir}"
        fi
        mkdir -p "${baseline_cache}"
        tmp_extract_dir="$(mktemp -d "${extracted_dir}.tmp.XXXXXX")"
        echo "[baseline] extracting baseline tarball to cache dir: ${extracted_dir}"
        if ! tar -xzf "${cache_file}" -C "${tmp_extract_dir}"; then
            rm -rf "${tmp_extract_dir}"
            return 1
        fi
        if [[ ! -d "${tmp_extract_dir}/namenode" ]] \
            || [[ ! -d "${tmp_extract_dir}/datanode" ]] \
            || [[ ! -d "${tmp_extract_dir}/pgdata" ]] \
            || [[ ! -d "${tmp_extract_dir}/state" ]]; then
            echo "[baseline] extracted baseline cache is incomplete: ${cache_file}" >&2
            rm -rf "${tmp_extract_dir}"
            return 1
        fi
        touch "${tmp_extract_dir}/.extract.ready"
        mv "${tmp_extract_dir}" "${extracted_dir}"
    fi

    # Restore into all 4 volumes in a single alpine container so data streams
    # directly from the extracted cache tree into the volume mounts.
    echo "[baseline] restoring volumes from extracted baseline cache..."
    local _t0
    _t0=$(date +%s)
    sudo docker run --rm \
        -v "${extracted_dir}:/baseline:ro" \
        -v "${prefix}-namenode:/restore/namenode" \
        -v "${prefix}-datanode:/restore/datanode" \
        -v "${prefix}-pgdata:/restore/pgdata" \
        -v "${prefix}-state:/restore/state" \
        alpine sh -c 'cd /baseline && tar cf - namenode datanode pgdata state | tar xf - -C /restore'
    HIVE_BASELINE_RESTORE_RESULT="restored"
    echo "[baseline] restore done took=$(( $(date +%s) - _t0 ))s"
}

hive_compose_file_for() {
    local hive_version="$1"
    echo "${ROOT}/docker-compose/hive/hive-${hive_version#hive}x.yaml"
}

hive_compose_template_for() {
    local hive_version="$1"
    echo "${ROOT}/docker-compose/hive/hive-${hive_version#hive}x.yaml.tpl"
}

hive_env_file_for() {
    local hive_version="$1"
    echo "${ROOT}/docker-compose/hive/hadoop-hive-${hive_version#hive}x.env"
}

hive_env_template_for() {
    local hive_version="$1"
    echo "${ROOT}/docker-compose/hive/hadoop-hive-${hive_version#hive}x.env.tpl"
}

hive_settings_env_for() {
    local hive_version="$1"
    echo "${ROOT}/docker-compose/hive/hive-${hive_version#hive}x_settings.env"
}

hive_metastore_container_for() {
    local hive_version="$1"
    echo "${hive_version}-metastore"
}

ensure_hosts_alias() {
    local alias_name="$1"
    local alias_ip="$2"
    local tmp_hosts
    local sudo_cmd=()

    if [[ "$(id -u)" -ne 0 ]]; then
        sudo_cmd=(sudo)
    fi

    tmp_hosts="$(mktemp)"
    "${sudo_cmd[@]}" chmod a+w /etc/hosts
    awk -v alias_name="${alias_name}" '
        {
            keep = 1
            for (i = 2; i <= NF; ++i) {
                if ($i == alias_name) {
                    keep = 0
                    break
                }
            }
            if (keep) {
                print
            }
        }
    ' /etc/hosts >"${tmp_hosts}"
    printf "%s %s\n" "${alias_ip}" "${alias_name}" >>"${tmp_hosts}"
    "${sudo_cmd[@]}" cp "${tmp_hosts}" /etc/hosts
    rm -f "${tmp_hosts}"
}

render_hive_compose() {
    local hive_version="$1"
    local compose_tpl
    local compose_file
    local env_file
    local env_tpl

    compose_tpl="$(hive_compose_template_for "${hive_version}")"
    compose_file="$(hive_compose_file_for "${hive_version}")"
    env_file="$(hive_env_file_for "${hive_version}")"
    env_tpl="$(hive_env_template_for "${hive_version}")"

    envsubst <"${compose_tpl}" >"${compose_file}"
    envsubst <"${ROOT}/docker-compose/hive/hadoop-hive.env.tpl" >"${env_file}"
    envsubst <"${env_tpl}" >>"${env_file}"
}

hive_compose_cmd() {
    local hive_version="$1"
    sudo docker compose -p "${CONTAINER_UID}${hive_version}" -f "$(hive_compose_file_for "${hive_version}")" --env-file "$(hive_env_file_for "${hive_version}")" "${@:2}"
}

exec_hive_script() {
    local hive_version="$1"
    local script_name="$2"
    local metastore_container

    metastore_container="$(hive_metastore_container_for "${hive_version}")"
    # -i: forward SIGINT/SIGTERM into container so Ctrl+C kills the in-container script
    #     instead of leaving an orphan that keeps mutating state.
    # stdbuf -oL -eL: line-buffer output so progress reaches the host log in real time.
    sudo docker exec -i \
        -e HIVE_BOOTSTRAP_GROUPS="${HIVE_BOOTSTRAP_GROUPS}" \
        -e LOAD_PARALLEL="${LOAD_PARALLEL}" \
        -e HIVE_HQL_PARALLEL="${HIVE_HQL_PARALLEL}" \
        -e HIVE_MODULES="${HIVE_MODULES}" \
        -e HIVE_BASELINE_VERSION="${HIVE_BASELINE_VERSION}" \
        -e HIVE_STATE_DIR="/mnt/state" \
        -e HS_PORT="${HS_PORT}" \
        -e DORIS_HS2_URL="jdbc:hive2://localhost:${HS_PORT}/default" \
        -e HIVE_DEBUG="${HIVE_DEBUG:-0}" \
        "${metastore_container}" \
        stdbuf -oL -eL bash --noprofile --norc "/mnt/scripts/${script_name}"
}

maybe_refresh_hive_data() {
    local hive_version="$1"
    local baseline_restore_result="${2:-missing}"

    if [[ "${NEED_LOAD_DATA}" -eq 0 ]]; then
        echo "Skip Hive data refresh because --no-load-data is set"
        return 0
    fi

    if [[ "${HIVE_MODE}" == "rebuild" || "${baseline_restore_result}" == "missing" ]]; then
        local _t_baseline
        _t_baseline=$(date +%s)
        echo "[$(date '+%H:%M:%S')] [${hive_version}] init-hive-baseline begin"
        exec_hive_script "${hive_version}" init-hive-baseline.sh
        echo "[$(date '+%H:%M:%S')] [${hive_version}] init-hive-baseline done took=$(( $(date +%s) - _t_baseline ))s"
    fi

    if [[ "${HIVE_MODE}" == "refresh" || "${HIVE_MODE}" == "rebuild" ]]; then
        local _t_modules
        _t_modules=$(date +%s)
        echo "[$(date '+%H:%M:%S')] [${hive_version}] refresh-hive-modules begin (mode=${HIVE_MODE} modules=${HIVE_MODULES})"
        exec_hive_script "${hive_version}" refresh-hive-modules.sh
        echo "[$(date '+%H:%M:%S')] [${hive_version}] refresh-hive-modules done took=$(( $(date +%s) - _t_modules ))s"
    fi
}

start_hive_stack() {
    local hive_version="$1"
    local volume_prefix
    local baseline_restore_result="missing"

    export HIVE_BOOTSTRAP_GROUPS="$(hive_bootstrap_groups_for "${hive_version}")"
    echo "${hive_version} selected bootstrap files: ${HIVE_BOOTSTRAP_GROUPS}"

    . "$(hive_settings_env_for "${hive_version}")"
    volume_prefix="$(hive_volume_prefix_for "${hive_version}")"
    export HIVE_VOLUME_PREFIX="${volume_prefix}"
    log_hive_volumes "${hive_version}" "${volume_prefix}"

    # Keep a stable hostname in metastore/HDFS metadata while allowing the
    # backing host IP to change across restarts.
    ensure_hosts_alias "${HIVE_HOST_ALIAS}" "${IP_HOST}"

    if [[ "${STOP}" -eq 1 ]]; then
        render_hive_compose "${hive_version}"
        hive_compose_cmd "${hive_version}" down
        return 0
    fi

    # refresh/rebuild: tear down the stack and clear volumes first.
    # fast: keep existing volumes and only restore the baseline when they are empty.
    if [[ "${HIVE_MODE}" == "rebuild" || "${HIVE_MODE}" == "refresh" ]]; then
        render_hive_compose "${hive_version}"
        hive_compose_cmd "${hive_version}" down || true
        reset_hive_volumes "${volume_prefix}"
    fi

    ensure_hive_volumes "${volume_prefix}"
    if [[ "${HIVE_MODE}" != "rebuild" ]]; then
        maybe_restore_baseline_to_volumes "${volume_prefix}" "${hive_version}"
        baseline_restore_result="${HIVE_BASELINE_RESTORE_RESULT}"
    fi
    if [[ "${HIVE_MODE}" == "fast" && "${baseline_restore_result}" == "missing" ]]; then
        echo "[baseline] ERROR: fast mode requires existing populated volumes or an available baseline tarball" >&2
        return 1
    fi
    render_hive_compose "${hive_version}"

    # fast mode is the only mode that reuses the current stack in place.
    if [[ "${HIVE_MODE}" == "fast" ]] && docker_hive_stack_healthy "${CONTAINER_UID}" "${hive_version}"; then
        echo "${hive_version} stack is already healthy, fast mode skips compose up"
    else
        local _t_up
        _t_up=$(date +%s)
        hive_compose_cmd "${hive_version}" up --build --remove-orphans -d --wait
        echo "[$(date '+%H:%M:%S')] [${hive_version}] compose up done took=$(( $(date +%s) - _t_up ))s"
    fi

    local _t_data
    _t_data=$(date +%s)
    maybe_refresh_hive_data "${hive_version}" "${baseline_restore_result}"
    echo "[$(date '+%H:%M:%S')] [${hive_version}] data refresh done took=$(( $(date +%s) - _t_data ))s"
}

start_iceberg() {
    # iceberg
    ICEBERG_DIR=${ROOT}/docker-compose/iceberg
    render_uid_template "${ROOT}/docker-compose/iceberg/iceberg.yaml.tpl" "${ROOT}/docker-compose/iceberg/iceberg.yaml"
    render_uid_template "${ROOT}/docker-compose/iceberg/entrypoint.sh.tpl" "${ROOT}/docker-compose/iceberg/entrypoint.sh"
    cp "${ROOT}/docker-compose/iceberg/entrypoint.sh" "${ROOT}/docker-compose/iceberg/scripts/entrypoint.sh"
    register_stack_metadata "iceberg" "${ROOT}/docker-compose/iceberg/iceberg.yaml" "${ROOT}/docker-compose/iceberg/iceberg.env"
    compose_down_stack "${ROOT}/docker-compose/iceberg/iceberg.yaml" "${ROOT}/docker-compose/iceberg/iceberg.env" --remove-orphans
    if [[ "${STOP}" -ne 1 ]]; then
        if [[ ! -d "${ICEBERG_DIR}/data" ]]; then
            echo "${ICEBERG_DIR}/data does not exist"
            (
                cd "${ICEBERG_DIR}" || exit 1
                rm -f iceberg_data*.zip
                wget -P "${ROOT}/docker-compose/iceberg" "https://${s3BucketName}.${s3Endpoint}/regression/datalake/pipeline_data/iceberg_data_spark40.zip"
                sudo unzip iceberg_data_spark40.zip
                sudo mv iceberg_data data
                sudo rm -rf iceberg_data_spark40.zip
            )
        else
            echo "${ICEBERG_DIR}/data exist, continue !"
        fi

        compose_up_stack "${ROOT}/docker-compose/iceberg/iceberg.yaml" "${ROOT}/docker-compose/iceberg/iceberg.env" -d --wait
    fi
}

start_hudi() {
    HUDI_DIR=${ROOT}/docker-compose/hudi
    export CONTAINER_UID=${CONTAINER_UID}
    export HUDI_BUNDLE_URL="${HUDI_BUNDLE_URL:-${MAVEN_REPOSITORY_URL}/org/apache/hudi/hudi-spark3.5-bundle_2.12/1.0.2/hudi-spark3.5-bundle_2.12-1.0.2.jar}"
    export HADOOP_AWS_URL="${HADOOP_AWS_URL:-${MAVEN_REPOSITORY_URL}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar}"
    export AWS_SDK_BUNDLE_URL="${AWS_SDK_BUNDLE_URL:-${MAVEN_REPOSITORY_URL}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar}"
    export POSTGRESQL_JDBC_URL="${POSTGRESQL_JDBC_URL:-${MAVEN_REPOSITORY_URL}/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar}"
    envsubst <"${HUDI_DIR}"/hudi.env.tpl >"${HUDI_DIR}"/hudi.env
    set -a
    . "${HUDI_DIR}"/hudi.env
    set +a
    envsubst <"${HUDI_DIR}"/hudi.yaml.tpl >"${HUDI_DIR}"/hudi.yaml
    sudo chmod +x "${HUDI_DIR}"/scripts/init.sh
    register_stack_metadata "hudi" "${HUDI_DIR}/hudi.yaml" "${HUDI_DIR}/hudi.env"
    compose_down_stack "${HUDI_DIR}/hudi.yaml" "${HUDI_DIR}/hudi.env" --remove-orphans
    if [[ "${STOP}" -ne 1 ]]; then
        compose_up_stack "${HUDI_DIR}/hudi.yaml" "${HUDI_DIR}/hudi.env" -d --wait
    fi
}

start_mariadb() {
    # mariadb
    start_rendered_compose_stack "mariadb" \
        "${ROOT}/docker-compose/mariadb/mariadb-10.yaml.tpl" \
        "${ROOT}/docker-compose/mariadb/mariadb-10.yaml" \
        "${ROOT}/docker-compose/mariadb/mariadb-10.env" \
        -d --wait -- \
        "${ROOT}/docker-compose/mariadb/data"
}

start_lakesoul() {
    echo "RUN_LAKESOUL"
    cp "${ROOT}"/docker-compose/lakesoul/lakesoul.yaml.tpl "${ROOT}"/docker-compose/lakesoul/lakesoul.yaml
    sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/lakesoul/lakesoul.yaml
    register_stack_metadata "lakesoul" "${ROOT}/docker-compose/lakesoul/lakesoul.yaml" ""
    compose_cmd "${ROOT}/docker-compose/lakesoul/lakesoul.yaml" "" down --remove-orphans
    sudo rm -rf "${ROOT}"/docker-compose/lakesoul/data
    if [[ "${STOP}" -ne 1 ]]; then
        echo "PREPARE_LAKESOUL_DATA"
        compose_cmd "${ROOT}/docker-compose/lakesoul/lakesoul.yaml" "" up -d
        ## import tpch data into lakesoul
        ## install rustup
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain none -y
        # shellcheck source=/dev/null
        . "${HOME}/.cargo/env"
        ## install rust nightly-2023-05-20
        rustup install nightly-2023-05-20
        ## download&generate tpch data
        mkdir -p lakesoul/test_files/tpch/data
        git clone https://github.com/databricks/tpch-dbgen.git
        (
            cd tpch-dbgen
            make
            ./dbgen -f -s 0.1
            mv *.tbl ../lakesoul/test_files/tpch/data
        )
        export TPCH_DATA=$(realpath lakesoul/test_files/tpch/data)
        ## import tpch data
        git clone https://github.com/lakesoul-io/LakeSoul.git
        #    git checkout doris_dev
        (
            cd LakeSoul/rust
            cargo test load_tpch_data --package lakesoul-datafusion --features=ci -- --nocapture
        )
    fi
}

start_kerberos() {
    echo "RUN_KERBEROS"
    export CONTAINER_UID=${CONTAINER_UID}
    envsubst <"${ROOT}"/docker-compose/kerberos/kerberos.yaml.tpl >"${ROOT}"/docker-compose/kerberos/kerberos.yaml
    sed -i "s/s3Endpoint/${s3Endpoint}/g" "${ROOT}"/docker-compose/kerberos/entrypoint-hive-master.sh
    sed -i "s/s3BucketName/${s3BucketName}/g" "${ROOT}"/docker-compose/kerberos/entrypoint-hive-master.sh
    for i in {1..2}; do
        . "${ROOT}"/docker-compose/kerberos/kerberos${i}_settings.env
        envsubst <"${ROOT}"/docker-compose/kerberos/hadoop-hive.env.tpl >"${ROOT}"/docker-compose/kerberos/hadoop-hive-${i}.env
        envsubst <"${ROOT}"/docker-compose/kerberos/conf/my.cnf.tpl > "${ROOT}"/docker-compose/kerberos/conf/kerberos${i}/my.cnf
        envsubst <"${ROOT}"/docker-compose/kerberos/conf/kerberos${i}/kdc.conf.tpl > "${ROOT}"/docker-compose/kerberos/conf/kerberos${i}/kdc.conf
        envsubst <"${ROOT}"/docker-compose/kerberos/conf/kerberos${i}/krb5.conf.tpl > "${ROOT}"/docker-compose/kerberos/conf/kerberos${i}/krb5.conf
    done
    sudo chmod a+w /etc/hosts
    if ! awk -v ip="${IP_HOST}" '$1 == ip && $2 == "hadoop-master" { found = 1 } END { exit !found }' /etc/hosts; then
        sudo sed -i "1i${IP_HOST} hadoop-master" /etc/hosts
    fi
    if ! awk -v ip="${IP_HOST}" '$1 == ip && $2 == "hadoop-master-2" { found = 1 } END { exit !found }' /etc/hosts; then
        sudo sed -i "1i${IP_HOST} hadoop-master-2" /etc/hosts
    fi
    register_stack_metadata "kerberos" "${ROOT}/docker-compose/kerberos/kerberos.yaml" ""
    compose_cmd "${ROOT}/docker-compose/kerberos/kerberos.yaml" "" down --remove-orphans
    sudo rm -rf "${ROOT}"/docker-compose/kerberos/data
    if [[ "${STOP}" -ne 1 ]]; then
        echo "PREPARE KERBEROS DATA"
        rm -rf "${ROOT}"/docker-compose/kerberos/two-kerberos-hives/*.keytab
        rm -rf "${ROOT}"/docker-compose/kerberos/two-kerberos-hives/*.jks
        rm -rf "${ROOT}"/docker-compose/kerberos/two-kerberos-hives/*.conf
        compose_cmd "${ROOT}/docker-compose/kerberos/kerberos.yaml" "" up --remove-orphans --wait -d
        sudo ln -sfn "${ROOT}/docker-compose/kerberos/two-kerberos-hives" /keytabs
        sudo cp "${ROOT}"/docker-compose/kerberos/common/conf/doris-krb5.conf /keytabs/krb5.conf
        sudo cp "${ROOT}"/docker-compose/kerberos/common/conf/doris-krb5.conf /etc/krb5.conf
        sleep 2
    fi
}

start_minio() {
    echo "RUN_MINIO"
    start_rendered_compose_stack "minio" \
        "${ROOT}/docker-compose/minio/minio-RELEASE.2024-11-07.yaml.tpl" \
        "${ROOT}/docker-compose/minio/minio-RELEASE.2024-11-07.yaml" \
        "${ROOT}/docker-compose/minio/minio-RELEASE.2024-11-07.env" \
        -d --wait
}

start_polaris() {
    echo "RUN_POLARIS"
    local POLARIS_DIR="${ROOT}/docker-compose/polaris"
    # Render compose with envsubst since settings is a bash export file
    export CONTAINER_UID=${CONTAINER_UID}
    . "${POLARIS_DIR}/polaris_settings.env"
    if command -v envsubst >/dev/null 2>&1; then
        envsubst <"${POLARIS_DIR}/docker-compose.yaml.tpl" >"${POLARIS_DIR}/docker-compose.yaml"
    else
        # Fallback: let docker compose handle variable substitution from current shell env
        cp "${POLARIS_DIR}/docker-compose.yaml.tpl" "${POLARIS_DIR}/docker-compose.yaml"
    fi
    register_stack_metadata "polaris" "${POLARIS_DIR}/docker-compose.yaml" ""
    compose_cmd "${POLARIS_DIR}/docker-compose.yaml" "" down --remove-orphans
    if [[ "${STOP}" -ne 1 ]]; then
        compose_cmd "${POLARIS_DIR}/docker-compose.yaml" "" up -d --wait --remove-orphans
    fi
}

start_ranger() {
    echo "RUN_RANGER"
    export CONTAINER_UID=${CONTAINER_UID}
    find "${ROOT}/docker-compose/ranger/script" -type f -exec sed -i "s/s3Endpoint/${s3Endpoint}/g" {} \;
    find "${ROOT}/docker-compose/ranger/script" -type f -exec sed -i "s/s3BucketName/${s3BucketName}/g" {} \;
    . "${ROOT}/docker-compose/ranger/ranger_settings.env"
    envsubst <"${ROOT}"/docker-compose/ranger/ranger.yaml.tpl >"${ROOT}"/docker-compose/ranger/ranger.yaml
    register_stack_metadata "ranger" "${ROOT}/docker-compose/ranger/ranger.yaml" "${ROOT}/docker-compose/ranger/ranger_settings.env"
    compose_down_stack "${ROOT}/docker-compose/ranger/ranger.yaml" "${ROOT}/docker-compose/ranger/ranger_settings.env" --remove-orphans
    if [[ "${STOP}" -ne 1 ]]; then
        compose_up_stack "${ROOT}/docker-compose/ranger/ranger.yaml" "${ROOT}/docker-compose/ranger/ranger_settings.env" -d --wait --remove-orphans
    fi
}

start_iceberg_rest() {
    echo "RUN_ICEBERG_REST"
    # iceberg-rest with multiple cloud storage backends
    ICEBERG_REST_DIR=${ROOT}/docker-compose/iceberg-rest
    
    # generate iceberg-rest.yaml
    export CONTAINER_UID=${CONTAINER_UID}
    . "${ROOT}"/docker-compose/iceberg-rest/iceberg-rest_settings.env
    envsubst <"${ICEBERG_REST_DIR}/docker-compose.yaml.tpl" >"${ICEBERG_REST_DIR}/docker-compose.yaml"
    register_stack_metadata "iceberg-rest" "${ICEBERG_REST_DIR}/docker-compose.yaml" ""
    compose_cmd "${ICEBERG_REST_DIR}/docker-compose.yaml" "" down --remove-orphans
    if [[ "${STOP}" -ne 1 ]]; then
        # Start all three REST catalogs (S3, OSS, COS)
        compose_cmd "${ICEBERG_REST_DIR}/docker-compose.yaml" "" up -d --remove-orphans --wait
    fi
}

echo "starting dockers in parallel"

reserve_ports

# Ensure hive data is downloaded before starting hive2/hive3, but only once
need_prepare_hive_data=0
if [[ "$NEED_LOAD_DATA" -eq 1 ]]; then
    if [[ "${RUN_HIVE2}" -eq 1 ]] || [[ "${RUN_HIVE3}" -eq 1 ]]; then
        if [[ "${HIVE_MODE}" == "refresh" || "${HIVE_MODE}" == "rebuild" ]]; then
            need_prepare_hive_data=1
        fi
    fi
fi

if [[ $need_prepare_hive_data -eq 1 ]]; then
    prepare_hive_bootstrap_groups=()
    if [[ "${RUN_HIVE2}" -eq 1 ]]; then
        prepare_hive_bootstrap_groups+=("$(hive_bootstrap_groups_for "hive2")")
    fi
    if [[ "${RUN_HIVE3}" -eq 1 ]]; then
        prepare_hive_bootstrap_groups+=("$(hive_bootstrap_groups_for "hive3")")
    fi
    export HIVE_BOOTSTRAP_GROUPS="$(bootstrap_merge_groups "${prepare_hive_bootstrap_groups[@]}")"
    echo "prepare hive2/hive3 data"
    echo "Prepare hive selected bootstrap files: ${HIVE_BOOTSTRAP_GROUPS}"
    bash "${ROOT}/docker-compose/hive/scripts/prepare-hive-data.sh"
fi

if [[ "${STOP}" -ne 1 ]]; then
    if [[ "${RUN_HIVE2}" -eq 1 ]] || [[ "${RUN_HIVE3}" -eq 1 ]]; then
        ensure_juicefs_hadoop_jar_for_hive
    fi
fi

if [[ "${RUN_ES}" -eq 1 ]]; then
    launch_component "es" "${LOG_ROOT}/start_es.log" start_es
fi

if [[ "${RUN_MYSQL}" -eq 1 ]]; then
    launch_component "mysql" "${LOG_ROOT}/start_mysql.log" start_mysql
fi

if [[ "${RUN_PG}" -eq 1 ]]; then
    launch_component "pg" "${LOG_ROOT}/start_pg.log" start_pg
fi

if [[ "${RUN_ORACLE}" -eq 1 ]]; then
    launch_component "oracle" "${LOG_ROOT}/start_oracle.log" start_oracle
fi

if [[ "${RUN_DB2}" -eq 1 ]]; then
    launch_component "db2" "${LOG_ROOT}/start_db2.log" start_db2
fi

if [[ "${RUN_OCEANBASE}" -eq 1 ]]; then
    launch_component "oceanbase" "${LOG_ROOT}/start_oceanbase.log" start_oceanbase
fi

if [[ "${RUN_SQLSERVER}" -eq 1 ]]; then
    launch_component "sqlserver" "${LOG_ROOT}/start_sqlserver.log" start_sqlserver
fi

if [[ "${RUN_CLICKHOUSE}" -eq 1 ]]; then
    launch_component "clickhouse" "${LOG_ROOT}/start_clickhouse.log" start_clickhouse
fi

if [[ "${RUN_KAFKA}" -eq 1 ]]; then
    launch_component "kafka" "${LOG_ROOT}/start_kafka.log" start_kafka
fi

if [[ "${RUN_HIVE2}" -eq 1 ]]; then
    launch_component "hive2" "${LOG_ROOT}/start_hive2.log" start_hive2
fi

if [[ "${RUN_HIVE3}" -eq 1 ]]; then
    launch_component "hive3" "${LOG_ROOT}/start_hive3.log" start_hive3
fi

if [[ "${RUN_ICEBERG}" -eq 1 ]]; then
    launch_component "iceberg" "${LOG_ROOT}/start_iceberg.log" start_iceberg
fi

if [[ "${RUN_ICEBERG_REST}" -eq 1 ]]; then
    launch_component "iceberg-rest" "${LOG_ROOT}/start_iceberg_rest.log" start_iceberg_rest
fi

if [[ "${RUN_HUDI}" -eq 1 ]]; then
    launch_component "hudi" "${LOG_ROOT}/start_hudi.log" start_hudi
fi

if [[ "${RUN_MARIADB}" -eq 1 ]]; then
    launch_component "mariadb" "${LOG_ROOT}/start_mariadb.log" start_mariadb
fi

if [[ "${RUN_LAKESOUL}" -eq 1 ]]; then
    launch_component "lakesoul" "${LOG_ROOT}/start_lakesoul.log" start_lakesoul
fi

if [[ "${RUN_MINIO}" -eq 1 ]]; then
    launch_component "minio" "${LOG_ROOT}/start_minio.log" start_minio
fi

if [[ "${RUN_POLARIS}" -eq 1 ]]; then
    launch_component "polaris" "${LOG_ROOT}/start_polaris.log" start_polaris
fi

if [[ "${RUN_KERBEROS}" -eq 1 ]]; then
    launch_component "kerberos" "${LOG_ROOT}/start_kerberos.log" start_kerberos
fi

if [[ "${RUN_RANGER}" -eq 1 ]]; then
    launch_component "ranger" "${LOG_ROOT}/start_ranger.log" start_ranger
fi
echo "waiting all dockers starting done"

if ! wait_for_started_jobs; then
    exit 1
fi

if [[ "${STOP}" -ne 1 ]]; then
    if [[ "${RUN_HIVE2}" -eq 1 ]]; then
        . "${ROOT}"/docker-compose/hive/hive-2x_settings.env
        prepare_juicefs_meta_for_hive "${JFS_CLUSTER_META}" "cluster"
    fi
    if [[ "${RUN_HIVE3}" -eq 1 ]]; then
        . "${ROOT}"/docker-compose/hive/hive-3x_settings.env
        prepare_juicefs_meta_for_hive "${JFS_CLUSTER_META}" "cluster"
    fi
fi

if [[ "${STOP}" -ne 1 ]]; then
    echo "docker started"
    print_started_summary
    echo "all requested dockers started successfully"
fi
