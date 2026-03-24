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
export IP_HOST=$(ip -4 addr show scope global | awk '/inet / {print $2}' | cut -d/ -f1 | head -n 1)

if ! OPTS="$(getopt \
    -n "$0" \
    -o '' \
    -l 'help' \
    -l 'stop' \
    -l 'reserve-ports' \
    -l 'no-load-data' \
    -l 'load-parallel:' \
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

echo "Components are: ${COMPONENTS}"
echo "Container UID: ${CONTAINER_UID}"
echo "Stop: ${STOP}"

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
RUN_OCENABASE=0
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

JUICEFS_LOCAL_BIN="${JUICEFS_RUNTIME_ROOT}/bin/juicefs"

find_juicefs_hadoop_jar() {
    local -a jar_globs=(
        "${JUICEFS_RUNTIME_ROOT}/lib/juicefs-hadoop-[0-9]*.jar"
        "${ROOT}/docker-compose/hive/scripts/auxlib/juicefs-hadoop-[0-9]*.jar"
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

    if [[ "${jfs_meta}" != *"@(127.0.0.1:3316)/"* && "${jfs_meta}" != *"@(localhost:3316)/"* ]]; then
        return 0
    fi

    meta_db="${jfs_meta##*/}"
    meta_db="${meta_db%%\?*}"

    if command -v mysql >/dev/null 2>&1; then
        mysql -h127.0.0.1 -P3316 -uroot -p123456 -e "CREATE DATABASE IF NOT EXISTS \`${meta_db}\`;"
        return 0
    fi

    mysql_container=$(sudo docker ps --format '{{.Names}}' | grep -E "(^|-)${CONTAINER_UID}mysql_57(-[0-9]+)?$" | head -n 1 || true)
    if [[ -n "${mysql_container}" ]]; then
        sudo docker exec "${mysql_container}" \
            mysql -uroot -p123456 -e "CREATE DATABASE IF NOT EXISTS \`${meta_db}\`;"
    fi
}

run_juicefs_cli() {
    local juicefs_cli
    juicefs_cli=$(resolve_juicefs_cli)
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
    cp -f "${source_jar}" "${auxlib_dir}/"
    echo "Synced JuiceFS Hadoop jar to hive auxlib: $(basename "${source_jar}")"
}

prepare_juicefs_meta_for_hive() {
    local jfs_meta="$1"
    local jfs_cluster_name="${2:-cluster}"
    if [[ -z "${jfs_meta}" || "${jfs_meta}" != mysql://* ]]; then
        return 0
    fi
    if [[ "${JFS_META_FORMATTED}" -eq 1 ]]; then
        return 0
    fi

    local bucket_dir="${JFS_BUCKET_DIR:-/tmp/jfs-bucket}"
    sudo mkdir -p "${bucket_dir}"
    sudo chmod 777 "${bucket_dir}"

    # For local mysql_57 metadata DSN, ensure metadata database exists.
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
        run_juicefs_cli status "${jfs_meta}" >/dev/null
    fi

    JFS_META_FORMATTED=1
}

start_es() {
    # elasticsearch
    cp "${ROOT}"/docker-compose/elasticsearch/es.yaml.tpl "${ROOT}"/docker-compose/elasticsearch/es.yaml
    sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/elasticsearch/es.yaml
    sudo docker compose -f "${ROOT}"/docker-compose/elasticsearch/es.yaml --env-file "${ROOT}"/docker-compose/elasticsearch/es.env down
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
        sudo docker compose -f "${ROOT}"/docker-compose/elasticsearch/es.yaml --env-file "${ROOT}"/docker-compose/elasticsearch/es.env up -d --remove-orphans
    fi
}

start_mysql() {
    # mysql 5.7
    cp "${ROOT}"/docker-compose/mysql/mysql-5.7.yaml.tpl "${ROOT}"/docker-compose/mysql/mysql-5.7.yaml
    sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/mysql/mysql-5.7.yaml
    sudo docker compose -f "${ROOT}"/docker-compose/mysql/mysql-5.7.yaml --env-file "${ROOT}"/docker-compose/mysql/mysql-5.7.env down
    if [[ "${STOP}" -ne 1 ]]; then
        sudo rm "${ROOT}"/docker-compose/mysql/data/* -rf
        sudo mkdir -p "${ROOT}"/docker-compose/mysql/data/
        sudo docker compose -f "${ROOT}"/docker-compose/mysql/mysql-5.7.yaml --env-file "${ROOT}"/docker-compose/mysql/mysql-5.7.env up -d --wait
    fi
}

start_pg() {
    # pg 14
    cp "${ROOT}"/docker-compose/postgresql/postgresql-14.yaml.tpl "${ROOT}"/docker-compose/postgresql/postgresql-14.yaml
    sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/postgresql/postgresql-14.yaml
    sudo docker compose -f "${ROOT}"/docker-compose/postgresql/postgresql-14.yaml --env-file "${ROOT}"/docker-compose/postgresql/postgresql-14.env down
    if [[ "${STOP}" -ne 1 ]]; then
        sudo rm "${ROOT}"/docker-compose/postgresql/data/* -rf
        sudo mkdir -p "${ROOT}"/docker-compose/postgresql/data/data
        sudo docker compose -f "${ROOT}"/docker-compose/postgresql/postgresql-14.yaml --env-file "${ROOT}"/docker-compose/postgresql/postgresql-14.env up -d --wait
    fi
}

start_oracle() {
    # oracle
    cp "${ROOT}"/docker-compose/oracle/oracle-11.yaml.tpl "${ROOT}"/docker-compose/oracle/oracle-11.yaml
    sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/oracle/oracle-11.yaml
    sudo docker compose -f "${ROOT}"/docker-compose/oracle/oracle-11.yaml --env-file "${ROOT}"/docker-compose/oracle/oracle-11.env down
    if [[ "${STOP}" -ne 1 ]]; then
        sudo rm "${ROOT}"/docker-compose/oracle/data/* -rf
        sudo mkdir -p "${ROOT}"/docker-compose/oracle/data/
        sudo docker compose -f "${ROOT}"/docker-compose/oracle/oracle-11.yaml --env-file "${ROOT}"/docker-compose/oracle/oracle-11.env up -d --wait
    fi
}

start_db2() {
    # db2
    cp "${ROOT}"/docker-compose/db2/db2.yaml.tpl "${ROOT}"/docker-compose/db2/db2.yaml
    sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/db2/db2.yaml
    sudo docker compose -f "${ROOT}"/docker-compose/db2/db2.yaml --env-file "${ROOT}"/docker-compose/db2/db2.env down
    if [[ "${STOP}" -ne 1 ]]; then
        sudo rm "${ROOT}"/docker-compose/db2/data/* -rf
        sudo mkdir -p "${ROOT}"/docker-compose/db2/data/
        sudo docker compose -f "${ROOT}"/docker-compose/db2/db2.yaml --env-file "${ROOT}"/docker-compose/db2/db2.env up -d --wait
    fi
}

start_oceanbase() {
    # oceanbase
    cp "${ROOT}"/docker-compose/oceanbase/oceanbase.yaml.tpl "${ROOT}"/docker-compose/oceanbase/oceanbase.yaml
    sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/oceanbase/oceanbase.yaml
    sudo docker compose -f "${ROOT}"/docker-compose/oceanbase/oceanbase.yaml --env-file "${ROOT}"/docker-compose/oceanbase/oceanbase.env down
    if [[ "${STOP}" -ne 1 ]]; then
        sudo rm "${ROOT}"/docker-compose/oceanbase/data/* -rf
        sudo mkdir -p "${ROOT}"/docker-compose/oceanbase/data/
        sudo docker compose -f "${ROOT}"/docker-compose/oceanbase/oceanbase.yaml --env-file "${ROOT}"/docker-compose/oceanbase/oceanbase.env up -d --wait
    fi
}

start_sqlserver() {
    # sqlserver
    cp "${ROOT}"/docker-compose/sqlserver/sqlserver.yaml.tpl "${ROOT}"/docker-compose/sqlserver/sqlserver.yaml
    sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/sqlserver/sqlserver.yaml
    sudo docker compose -f "${ROOT}"/docker-compose/sqlserver/sqlserver.yaml --env-file "${ROOT}"/docker-compose/sqlserver/sqlserver.env down
    if [[ "${STOP}" -ne 1 ]]; then
        sudo rm "${ROOT}"/docker-compose/sqlserver/data/* -rf
        sudo mkdir -p "${ROOT}"/docker-compose/sqlserver/data/
        sudo docker compose -f "${ROOT}"/docker-compose/sqlserver/sqlserver.yaml --env-file "${ROOT}"/docker-compose/sqlserver/sqlserver.env up -d --wait
    fi
}

start_clickhouse() {
    # clickhouse
    cp "${ROOT}"/docker-compose/clickhouse/clickhouse.yaml.tpl "${ROOT}"/docker-compose/clickhouse/clickhouse.yaml
    sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/clickhouse/clickhouse.yaml
    sudo docker compose -f "${ROOT}"/docker-compose/clickhouse/clickhouse.yaml --env-file "${ROOT}"/docker-compose/clickhouse/clickhouse.env down
    if [[ "${STOP}" -ne 1 ]]; then
        sudo rm "${ROOT}"/docker-compose/clickhouse/data/* -rf
        sudo mkdir -p "${ROOT}"/docker-compose/clickhouse/data/
        sudo docker compose -f "${ROOT}"/docker-compose/clickhouse/clickhouse.yaml --env-file "${ROOT}"/docker-compose/clickhouse/clickhouse.env up -d --wait
    fi
}

start_kafka() {
    # kafka
    KAFKA_CONTAINER_ID="${CONTAINER_UID}kafka"
    cp "${ROOT}"/docker-compose/kafka/kafka.yaml.tpl "${ROOT}"/docker-compose/kafka/kafka.yaml
    sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/kafka/kafka.yaml
    sed -i "s/localhost/${IP_HOST}/g" "${ROOT}"/docker-compose/kafka/kafka.yaml
    sudo docker compose -f "${ROOT}"/docker-compose/kafka/kafka.yaml --env-file "${ROOT}"/docker-compose/kafka/kafka.env down

    create_kafka_topics() {
        local container_id="$1"
        local ip_host="$2"
        local backup_dir=/home/work/pipline/backup_center

        declare -a topics=("basic_data" "basic_array_data" "basic_data_with_errors" "basic_array_data_with_errors" "basic_data_timezone" "basic_array_data_timezone" "trino_kafka_basic_data")

        for topic in "${topics[@]}"; do
            echo "docker exec "${container_id}" bash -c echo '/opt/bitnami/kafka/bin/kafka-topics.sh --create --bootstrap-server '${ip_host}:19193' --topic '${topic}'"
            docker exec "${container_id}" bash -c "/opt/bitnami/kafka/bin/kafka-topics.sh --create --bootstrap-server '${ip_host}:19193' --topic '${topic}'"
        done

    }

    if [[ "${STOP}" -ne 1 ]]; then
        sudo docker compose -f "${ROOT}"/docker-compose/kafka/kafka.yaml --env-file "${ROOT}"/docker-compose/kafka/kafka.env up --build --remove-orphans -d
        sleep 10s
        create_kafka_topics "${KAFKA_CONTAINER_ID}" "${IP_HOST}"
    fi
}

start_hive2() {
    # hive2
    # If the doris cluster you need to test is single-node, you can use the default values; If the doris cluster you need to test is composed of multiple nodes, then you need to set the IP_HOST according to the actual situation of your machine
    #default value
    export CONTAINER_UID=${CONTAINER_UID}
    . "${ROOT}"/docker-compose/hive/hive-2x_settings.env
    envsubst <"${ROOT}"/docker-compose/hive/hive-2x.yaml.tpl >"${ROOT}"/docker-compose/hive/hive-2x.yaml
    envsubst <"${ROOT}"/docker-compose/hive/hadoop-hive.env.tpl >"${ROOT}"/docker-compose/hive/hadoop-hive-2x.env
    envsubst <"${ROOT}"/docker-compose/hive/hadoop-hive-2x.env.tpl >> "${ROOT}"/docker-compose/hive/hadoop-hive-2x.env
    sudo docker compose -p ${CONTAINER_UID}hive2 -f "${ROOT}"/docker-compose/hive/hive-2x.yaml --env-file "${ROOT}"/docker-compose/hive/hadoop-hive-2x.env down
    if [[ "${STOP}" -ne 1 ]]; then
        sudo docker compose -p ${CONTAINER_UID}hive2 -f "${ROOT}"/docker-compose/hive/hive-2x.yaml --env-file "${ROOT}"/docker-compose/hive/hadoop-hive-2x.env up --build --remove-orphans -d --wait
    fi
}

start_hive3() {
    # hive3
    # If the doris cluster you need to test is single-node, you can use the default values; If the doris cluster you need to test is composed of multiple nodes, then you need to set the IP_HOST according to the actual situation of your machine
    export CONTAINER_UID=${CONTAINER_UID}
    . "${ROOT}"/docker-compose/hive/hive-3x_settings.env
    envsubst <"${ROOT}"/docker-compose/hive/hive-3x.yaml.tpl >"${ROOT}"/docker-compose/hive/hive-3x.yaml
    envsubst <"${ROOT}"/docker-compose/hive/hadoop-hive.env.tpl >"${ROOT}"/docker-compose/hive/hadoop-hive-3x.env
    envsubst <"${ROOT}"/docker-compose/hive/hadoop-hive-3x.env.tpl >> "${ROOT}"/docker-compose/hive/hadoop-hive-3x.env
    sudo docker compose -p ${CONTAINER_UID}hive3 -f "${ROOT}"/docker-compose/hive/hive-3x.yaml --env-file "${ROOT}"/docker-compose/hive/hadoop-hive-3x.env down
    if [[ "${STOP}" -ne 1 ]]; then
        sudo docker compose -p ${CONTAINER_UID}hive3 -f "${ROOT}"/docker-compose/hive/hive-3x.yaml --env-file "${ROOT}"/docker-compose/hive/hadoop-hive-3x.env up --build --remove-orphans -d --wait
    fi
}

start_iceberg() {
    # iceberg
    ICEBERG_DIR=${ROOT}/docker-compose/iceberg
    cp "${ROOT}"/docker-compose/iceberg/iceberg.yaml.tpl "${ROOT}"/docker-compose/iceberg/iceberg.yaml
    cp "${ROOT}"/docker-compose/iceberg/entrypoint.sh.tpl "${ROOT}"/docker-compose/iceberg/entrypoint.sh
    sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/iceberg/iceberg.yaml
    sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/iceberg/entrypoint.sh
    cp "${ROOT}"/docker-compose/iceberg/entrypoint.sh "${ROOT}"/docker-compose/iceberg/scripts/entrypoint.sh
    sudo docker compose -f "${ROOT}"/docker-compose/iceberg/iceberg.yaml --env-file "${ROOT}"/docker-compose/iceberg/iceberg.env down
    if [[ "${STOP}" -ne 1 ]]; then
        if [[ ! -d "${ICEBERG_DIR}/data" ]]; then
            echo "${ICEBERG_DIR}/data does not exist"
            cd "${ICEBERG_DIR}" \
            && rm -f iceberg_data*.zip \
            && wget -P "${ROOT}"/docker-compose/iceberg https://"${s3BucketName}.${s3Endpoint}"/regression/datalake/pipeline_data/iceberg_data_paimon_101.zip \
            && sudo unzip iceberg_data_paimon_101.zip \
            && sudo mv iceberg_data data \
            && sudo rm -rf iceberg_data_paimon_101.zip
            cd -
        else
            echo "${ICEBERG_DIR}/data exist, continue !"
        fi

        if [[ ! -f "${ICEBERG_DIR}/data/input/jars/iceberg-aws-bundle-1.10.0.jar" ]]; then 
            echo "iceberg 1.10.0 jars does not exist"
            cd "${ICEBERG_DIR}" \
            && rm -f iceberg_1_10_0*.jars.tar.gz\
            && wget -P "${ROOT}"/docker-compose/iceberg https://"${s3BucketName}.${s3Endpoint}"/regression/datalake/pipeline_data/iceberg_1_10_0.jars.tar.gz \
            && sudo tar xzvf iceberg_1_10_0.jars.tar.gz -C "data/input/jars" \
            && sudo rm -rf iceberg_1_10_0.jars.tar.gz
            cd -
        else 
            echo "iceberg 1.10.0 jars exist, continue !"
        fi        

        sudo docker compose -f "${ROOT}"/docker-compose/iceberg/iceberg.yaml --env-file "${ROOT}"/docker-compose/iceberg/iceberg.env up -d --wait
    fi
}

start_hudi() {
    HUDI_DIR=${ROOT}/docker-compose/hudi
    export CONTAINER_UID=${CONTAINER_UID}
    envsubst <"${HUDI_DIR}"/hudi.env.tpl >"${HUDI_DIR}"/hudi.env
    set -a
    . "${HUDI_DIR}"/hudi.env
    set +a
    envsubst <"${HUDI_DIR}"/hudi.yaml.tpl >"${HUDI_DIR}"/hudi.yaml
    sudo chmod +x "${HUDI_DIR}"/scripts/init.sh
    sudo docker compose -f "${HUDI_DIR}"/hudi.yaml --env-file "${HUDI_DIR}"/hudi.env down --remove-orphans
    if [[ "${STOP}" -ne 1 ]]; then
        sudo docker compose -f "${HUDI_DIR}"/hudi.yaml --env-file "${HUDI_DIR}"/hudi.env up -d --wait
    fi
}

start_mariadb() {
    # mariadb
    cp "${ROOT}"/docker-compose/mariadb/mariadb-10.yaml.tpl "${ROOT}"/docker-compose/mariadb/mariadb-10.yaml
    sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/mariadb/mariadb-10.yaml
    sudo docker compose -f "${ROOT}"/docker-compose/mariadb/mariadb-10.yaml --env-file "${ROOT}"/docker-compose/mariadb/mariadb-10.env down
    if [[ "${STOP}" -ne 1 ]]; then
        sudo mkdir -p "${ROOT}"/docker-compose/mariadb/data/
        sudo rm "${ROOT}"/docker-compose/mariadb/data/* -rf
        sudo docker compose -f "${ROOT}"/docker-compose/mariadb/mariadb-10.yaml --env-file "${ROOT}"/docker-compose/mariadb/mariadb-10.env up -d --wait
    fi
}

start_lakesoul() {
    echo "RUN_LAKESOUL"
    cp "${ROOT}"/docker-compose/lakesoul/lakesoul.yaml.tpl "${ROOT}"/docker-compose/lakesoul/lakesoul.yaml
    sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/lakesoul/lakesoul.yaml
    sudo docker compose -f "${ROOT}"/docker-compose/lakesoul/lakesoul.yaml down
    sudo rm -rf "${ROOT}"/docker-compose/lakesoul/data
    if [[ "${STOP}" -ne 1 ]]; then
        echo "PREPARE_LAKESOUL_DATA"
        sudo docker compose -f "${ROOT}"/docker-compose/lakesoul/lakesoul.yaml up -d
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
        cd tpch-dbgen
        make
        ./dbgen -f -s 0.1
        mv *.tbl ../lakesoul/test_files/tpch/data
        cd ..
        export TPCH_DATA=$(realpath lakesoul/test_files/tpch/data)
        ## import tpch data
        git clone https://github.com/lakesoul-io/LakeSoul.git
        #    git checkout doris_dev
        cd LakeSoul/rust
        cargo test load_tpch_data --package lakesoul-datafusion --features=ci -- --nocapture
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
    sudo sed -i "1i${IP_HOST} hadoop-master" /etc/hosts
    sudo sed -i "1i${IP_HOST} hadoop-master-2" /etc/hosts
    sudo docker compose -f "${ROOT}"/docker-compose/kerberos/kerberos.yaml down
    sudo rm -rf "${ROOT}"/docker-compose/kerberos/data
    if [[ "${STOP}" -ne 1 ]]; then
        echo "PREPARE KERBEROS DATA"
        rm -rf "${ROOT}"/docker-compose/kerberos/two-kerberos-hives/*.keytab
        rm -rf "${ROOT}"/docker-compose/kerberos/two-kerberos-hives/*.jks
        rm -rf "${ROOT}"/docker-compose/kerberos/two-kerberos-hives/*.conf
        sudo docker compose -f "${ROOT}"/docker-compose/kerberos/kerberos.yaml up --remove-orphans --wait -d
        sudo rm -df /keytabs
        sudo ln -s "${ROOT}"/docker-compose/kerberos/two-kerberos-hives /keytabs
        sudo cp "${ROOT}"/docker-compose/kerberos/common/conf/doris-krb5.conf /keytabs/krb5.conf
        sudo cp "${ROOT}"/docker-compose/kerberos/common/conf/doris-krb5.conf /etc/krb5.conf
        sleep 2
    fi
}

start_minio() {
    echo "RUN_MINIO"
    cp "${ROOT}"/docker-compose/minio/minio-RELEASE.2024-11-07.yaml.tpl "${ROOT}"/docker-compose/minio/minio-RELEASE.2024-11-07.yaml
    sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/minio/minio-RELEASE.2024-11-07.yaml
    sudo docker compose -f "${ROOT}"/docker-compose/minio/minio-RELEASE.2024-11-07.yaml --env-file "${ROOT}"/docker-compose/minio/minio-RELEASE.2024-11-07.env down
    if [[ "${STOP}" -ne 1 ]]; then
        sudo docker compose -f "${ROOT}"/docker-compose/minio/minio-RELEASE.2024-11-07.yaml --env-file "${ROOT}"/docker-compose/minio/minio-RELEASE.2024-11-07.env up -d
    fi
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
    sudo docker compose -f "${POLARIS_DIR}/docker-compose.yaml" down
    if [[ "${STOP}" -ne 1 ]]; then
        sudo docker compose -f "${POLARIS_DIR}/docker-compose.yaml" up -d --wait --remove-orphans
    fi
}

start_ranger() {
    echo "RUN_RANGER"
    export CONTAINER_UID=${CONTAINER_UID}
    find "${ROOT}/docker-compose/ranger/script" -type f -exec sed -i "s/s3Endpoint/${s3Endpoint}/g" {} \;
    find "${ROOT}/docker-compose/ranger/script" -type f -exec sed -i "s/s3BucketName/${s3BucketName}/g" {} \;
    . "${ROOT}/docker-compose/ranger/ranger_settings.env"
    envsubst <"${ROOT}"/docker-compose/ranger/ranger.yaml.tpl >"${ROOT}"/docker-compose/ranger/ranger.yaml
    sudo docker compose -f "${ROOT}"/docker-compose/ranger/ranger.yaml --env-file "${ROOT}"/docker-compose/ranger/ranger_settings.env down
    if [[ "${STOP}" -ne 1 ]]; then
        sudo docker compose -f "${ROOT}"/docker-compose/ranger/ranger.yaml --env-file "${ROOT}"/docker-compose/ranger/ranger_settings.env up -d --wait --remove-orphans
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
    
    sudo docker compose -f "${ICEBERG_REST_DIR}/docker-compose.yaml" down
    if [[ "${STOP}" -ne 1 ]]; then
        # Start all three REST catalogs (S3, OSS, COS)
        sudo docker compose -f "${ICEBERG_REST_DIR}/docker-compose.yaml" up -d --remove-orphans --wait
    fi
}

echo "starting dockers in parallel"

reserve_ports

# Ensure hive data is downloaded before starting hive2/hive3, but only once
need_prepare_hive_data=0
if [[ "$NEED_LOAD_DATA" -eq 1 ]]; then
    if [[ "${RUN_HIVE2}" -eq 1 ]] || [[ "${RUN_HIVE3}" -eq 1 ]]; then
        need_prepare_hive_data=1
    fi
fi

if [[ $need_prepare_hive_data -eq 1 ]]; then
    echo "prepare hive2/hive3 data"
    bash "${ROOT}/docker-compose/hive/scripts/prepare-hive-data.sh"
fi

if [[ "${STOP}" -ne 1 ]]; then
    if [[ "${RUN_HIVE2}" -eq 1 ]] || [[ "${RUN_HIVE3}" -eq 1 ]]; then
        ensure_juicefs_hadoop_jar_for_hive
    fi
fi

declare -A pids

if [[ "${RUN_ES}" -eq 1 ]]; then
    start_es > start_es.log  2>&1 &
    pids["es"]=$!
fi

if [[ "${RUN_MYSQL}" -eq 1 ]]; then
    start_mysql > start_mysql.log 2>&1 &
    pids["mysql"]=$!
fi

if [[ "${RUN_PG}" -eq 1 ]]; then
    start_pg > start_pg.log 2>&1 &
    pids["pg"]=$!
fi

if [[ "${RUN_ORACLE}" -eq 1 ]]; then
    start_oracle > start_oracle.log 2>&1 &
    pids["oracle"]=$!
fi

if [[ "${RUN_DB2}" -eq 1 ]]; then
    start_db2 > start_db2.log 2>&1 &
    pids["db2"]=$!
fi

if [[ "${RUN_OCEANBASE}" -eq 1 ]]; then
    start_oceanbase > start_oceanbase.log 2>&1 &
    pids["oceanbase"]=$!
fi

if [[ "${RUN_SQLSERVER}" -eq 1 ]]; then
    start_sqlserver > start_sqlserver.log 2>&1 &
    pids["sqlserver"]=$!
fi

if [[ "${RUN_CLICKHOUSE}" -eq 1 ]]; then
    start_clickhouse > start_clickhouse.log 2>&1 &
    pids["clickhouse"]=$!
fi

if [[ "${RUN_KAFKA}" -eq 1 ]]; then
    start_kafka > start_kafka.log 2>&1 &
    pids["kafka"]=$!
fi

if [[ "${RUN_HIVE2}" -eq 1 ]]; then
    start_hive2 > start_hive2.log 2>&1 &
    pids["hive2"]=$!
fi

if [[ "${RUN_HIVE3}" -eq 1 ]]; then
    start_hive3 > start_hive3.log 2>&1 &
    pids["hive3"]=$!
fi

if [[ "${RUN_ICEBERG}" -eq 1 ]]; then
    start_iceberg > start_iceberg.log 2>&1 &
    pids["iceberg"]=$!
fi

if [[ "${RUN_ICEBERG_REST}" -eq 1 ]]; then
    start_iceberg_rest > start_iceberg_rest.log 2>&1 &
    pids["iceberg-rest"]=$!
fi

if [[ "${RUN_HUDI}" -eq 1 ]]; then
    start_hudi > start_hudi.log 2>&1 &
    pids["hudi"]=$!
fi

if [[ "${RUN_MARIADB}" -eq 1 ]]; then
    start_mariadb > start_mariadb.log 2>&1 &
    pids["mariadb"]=$!
fi

if [[ "${RUN_LAKESOUL}" -eq 1 ]]; then
    start_lakesoul > start_lakesoule.log 2>&1 &
    pids["lakesoul"]=$!
fi

if [[ "${RUN_MINIO}" -eq 1 ]]; then
    start_minio > start_minio.log 2>&1 &
    pids["minio"]=$!
fi

if [[ "${RUN_POLARIS}" -eq 1 ]]; then
    start_polaris > start_polaris.log 2>&1 &
    pids["polaris"]=$!
fi

if [[ "${RUN_KERBEROS}" -eq 1 ]]; then
    start_kerberos > start_kerberos.log 2>&1 &
    pids["kerberos"]=$!
fi

if [[ "${RUN_RANGER}" -eq 1 ]]; then
    start_ranger > start_ranger.log 2>&1 &
    pids["ranger"]=$!
fi
echo "waiting all dockers starting done"

for compose in "${!pids[@]}"; do
    # prevent wait return 1 make the script exit
    status=0
    wait "${pids[$compose]}" || status=$?
    if [ $status -ne 0 ] && [ $compose != "db2" ]; then
        echo "docker $compose started failed with status $status"
        echo "print start_${compose}.log"
        cat start_${compose}.log || true

        echo ""
        echo "print last 100 logs of the latest unhealthy container"
        sudo docker ps -a --latest --filter 'health=unhealthy' --format '{{.ID}}' | xargs -I '{}' sh -c 'echo "=== Logs of {} ===" && docker logs -t --tail 100 "{}"'

        exit 1
    fi
done

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

echo "docker started"
sudo docker ps -a --format "{{.ID}} | {{.Image}} | {{.Status}}"
echo "all dockers started successfully"
