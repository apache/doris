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

dump_kerberos_container_state() {
    local container="$1"

    echo "===== ${container} state =====" >&2
    sudo docker inspect --format \
        'status={{.State.Status}} running={{.State.Running}} exitCode={{.State.ExitCode}} oomKilled={{.State.OOMKilled}} error={{.State.Error}}' \
        "${container}" >&2 || true
    echo "===== ${container} logs (tail -200) =====" >&2
    sudo docker logs --tail 200 "${container}" >&2 || true
}

cleanup_kerberos_ready_wait() {
    local wait_pid="$1"

    [[ -n "${wait_pid}" ]] || return 0
    sudo kill -TERM -- "-${wait_pid}" >/dev/null 2>&1 || true
    wait "${wait_pid}" >/dev/null 2>&1 || true
}

wait_for_kerberos_ready() {
    local container="$1"
    local wait_pid=""
    local wait_status=0

    echo "Waiting for ${container} readiness event"
    setsid sudo timeout --signal=TERM --kill-after=5s 20m bash -c '
        log_dir=$(mktemp -d)
        mkfifo "${log_dir}/container.log"
        docker logs --follow "$1" >"${log_dir}/container.log" 2>&1 &
        log_pid=$!
        cleanup() {
            kill -KILL "${log_pid}" 2>/dev/null || true
            wait "${log_pid}" 2>/dev/null || true
            rm -rf "${log_dir}"
        }
        trap cleanup EXIT
        while IFS= read -r line; do
            if [[ "${line}" == "DORIS_KERBEROS_READY" ]]; then
                echo "${line}"
                exit 0
            fi
        done <"${log_dir}/container.log"
        exit 1
    ' _ "${container}" &
    wait_pid=$!
    trap 'cleanup_kerberos_ready_wait "${wait_pid}"' EXIT
    trap 'exit 143' TERM
    trap 'exit 130' INT
    if ! wait "${wait_pid}"; then
        wait_status=1
    fi
    wait_pid=""
    trap - EXIT TERM INT
    if [[ "${wait_status}" -ne 0 ]]; then
        echo "ERROR: timed out or container exited before ${container} became ready" >&2
        dump_kerberos_container_state "${container}"
        return 1
    fi
}

cleanup_kerberos_readiness_jobs() {
    local pid

    for pid in "$@"; do
        kill "${pid}" >/dev/null 2>&1 || true
    done
    for pid in "$@"; do
        wait "${pid}" >/dev/null 2>&1 || true
    done
}

validate_kerberos_container() {
    local container="$1"

    echo "Running one-shot readiness validation for ${container}"
    if ! sudo docker exec "${container}" /opt/doris/health.sh; then
        echo "ERROR: one-shot readiness validation failed for ${container}" >&2
        dump_kerberos_container_state "${container}"
        return 1
    fi
}

start_kerberos() {
    echo "RUN_KERBEROS"
    local KERBEROS_DIR="${ROOT}/docker-compose/kerberos"
    local -a containers=(
        "doris-${CONTAINER_UID}-kerberos1"
        "doris-${CONTAINER_UID}-kerberos2"
    )
    local -a readiness_pids=()
    local readiness_status=0
    local container
    local pid

    export CONTAINER_UID=${CONTAINER_UID}
    envsubst <"${KERBEROS_DIR}/kerberos.yaml.tpl" >"${KERBEROS_DIR}/kerberos.yaml"
    mkdir -p "${KERBEROS_DIR}/conf/kerberos1" "${KERBEROS_DIR}/conf/kerberos2" \
        "${KERBEROS_DIR}/two-kerberos-hives"
    for i in {1..2}; do
        . "${KERBEROS_DIR}/kerberos${i}_settings.env"
        envsubst <"${KERBEROS_DIR}/hadoop-hive.env.tpl" >"${KERBEROS_DIR}/hadoop-hive-${i}.env"
        for config in kdc.conf krb5.conf core-site.xml hdfs-site.xml hive-site.xml; do
            envsubst <"${KERBEROS_DIR}/conf/${config}.tpl" >"${KERBEROS_DIR}/conf/kerberos${i}/${config}"
        done
    done
    sudo chmod a+w /etc/hosts
    if ! awk -v ip="${IP_HOST}" '$1 == ip && $2 == "hadoop-master" { found = 1 } END { exit !found }' /etc/hosts; then
        sudo sed -i "1i${IP_HOST} hadoop-master" /etc/hosts
    fi
    if ! awk -v ip="${IP_HOST}" '$1 == ip && $2 == "hadoop-master-2" { found = 1 } END { exit !found }' /etc/hosts; then
        sudo sed -i "1i${IP_HOST} hadoop-master-2" /etc/hosts
    fi
    register_stack_metadata "kerberos" "${KERBEROS_DIR}/kerberos.yaml" ""
    compose_cmd "${KERBEROS_DIR}/kerberos.yaml" "" down --remove-orphans
    sudo rm -rf "${KERBEROS_DIR}/data"
    if [[ "${STOP}" -ne 1 ]]; then
        echo "PREPARE KERBEROS DATA"
        rm -rf "${KERBEROS_DIR}"/two-kerberos-hives/*.keytab
        rm -rf "${KERBEROS_DIR}"/two-kerberos-hives/*.jks
        rm -rf "${KERBEROS_DIR}"/two-kerberos-hives/*.conf
        compose_cmd "${KERBEROS_DIR}/kerberos.yaml" "" up --build --remove-orphans -d
        trap 'cleanup_kerberos_readiness_jobs "${readiness_pids[@]}"' EXIT
        trap 'exit 143' TERM
        trap 'exit 130' INT
        for container in "${containers[@]}"; do
            wait_for_kerberos_ready "${container}" &
            readiness_pids+=("$!")
        done
        for pid in "${readiness_pids[@]}"; do
            if ! wait "${pid}"; then
                readiness_status=1
            fi
        done
        readiness_pids=()
        trap - EXIT TERM INT
        if [[ "${readiness_status}" -ne 0 ]]; then
            return 1
        fi
        for container in "${containers[@]}"; do
            validate_kerberos_container "${container}"
        done
        sudo ln -sfn "${KERBEROS_DIR}/two-kerberos-hives" /keytabs
        sudo cp "${KERBEROS_DIR}/common/conf/doris-krb5.conf" /keytabs/krb5.conf
        sudo cp "${KERBEROS_DIR}/common/conf/doris-krb5.conf" /etc/krb5.conf
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
juicefs_init_runtime_vars

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
        juicefs_ensure_hadoop_jar_for_hive
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
        . "${ROOT}/docker-compose/hive/hive-2x_settings.env"
        juicefs_prepare_meta_for_hive "${JFS_CLUSTER_META}" "cluster"
    fi
    if [[ "${RUN_HIVE3}" -eq 1 ]]; then
        . "${ROOT}/docker-compose/hive/hive-3x_settings.env"
        juicefs_prepare_meta_for_hive "${JFS_CLUSTER_META}" "cluster"
    fi
fi

echo "docker started"
sudo docker ps -a --format "{{.ID}} | {{.Image}} | {{.Status}}"
echo "all dockers started successfully"
