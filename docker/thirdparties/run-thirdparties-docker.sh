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

################################################################
# This script will restart all thirdparty containers
################################################################

set -eo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

. "${ROOT}/custom_settings.env"

usage() {
    echo "
Usage: $0 <options>
  Optional options:
     [no option]        start all components
     --help,-h          show this usage
     -c mysql           start MySQL
     -c mysql,hive      start MySQL and Hive
     --stop             stop the specified components

  All valid components:
    mysql,pg,oracle,sqlserver,clickhouse,es,hive,iceberg,hudi,trino
  "
    exit 1
}

if ! OPTS="$(getopt \
    -n "$0" \
    -o '' \
    -l 'help' \
    -l 'stop' \
    -o 'hc:' \
    -- "$@")"; then
    usage
fi

eval set -- "${OPTS}"

COMPONENTS=""
HELP=0
STOP=0

if [[ "$#" == 1 ]]; then
    # default
    COMPONENTS="mysql,pg,oracle,sqlserver,clickhouse,hive,iceberg,hudi,trino"
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
            COMPONENTS="mysql,es,pg,oracle,sqlserver,clickhouse,hive,iceberg,hudi,trino"
        fi
    fi
fi

if [[ "${HELP}" -eq 1 ]]; then
    usage
fi

if [[ "${COMPONENTS}"x == ""x ]]; then
    echo "Invalid arguments"
    usage
fi

if [[ "${CONTAINER_UID}"x == "doris--"x ]]; then
    echo "Must set CONTAINER_UID to a unique name in custom_settings.sh"
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
RUN_HIVE=0
RUN_ES=0
RUN_ICEBERG=0
RUN_HUDI=0
RUN_TRINO=0

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
    elif [[ "${element}"x == "hive"x ]]; then
        RUN_HIVE=1
    elif [[ "${element}"x == "iceberg"x ]]; then
        RUN_ICEBERG=1
    elif [[ "${element}"x == "hudi"x ]]; then
        RUN_HUDI=1
    elif [[ "${element}"x == "trino"x ]];then
        RUN_TRINO=1
    else
        echo "Invalid component: ${element}"
        usage
    fi
done

if [[ "${RUN_ES}" -eq 1 ]]; then
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
        sudo docker compose -f "${ROOT}"/docker-compose/elasticsearch/es.yaml --env-file "${ROOT}"/docker-compose/elasticsearch/es.env up -d --remove-orphans
    fi
fi

if [[ "${RUN_MYSQL}" -eq 1 ]]; then
    # mysql 5.7
    cp "${ROOT}"/docker-compose/mysql/mysql-5.7.yaml.tpl "${ROOT}"/docker-compose/mysql/mysql-5.7.yaml
    sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/mysql/mysql-5.7.yaml
    sudo docker compose -f "${ROOT}"/docker-compose/mysql/mysql-5.7.yaml --env-file "${ROOT}"/docker-compose/mysql/mysql-5.7.env down
    if [[ "${STOP}" -ne 1 ]]; then
        sudo mkdir -p "${ROOT}"/docker-compose/mysql/data/
        sudo rm "${ROOT}"/docker-compose/mysql/data/* -rf
        sudo docker compose -f "${ROOT}"/docker-compose/mysql/mysql-5.7.yaml --env-file "${ROOT}"/docker-compose/mysql/mysql-5.7.env up -d
    fi
fi

if [[ "${RUN_PG}" -eq 1 ]]; then
    # pg 14
    cp "${ROOT}"/docker-compose/postgresql/postgresql-14.yaml.tpl "${ROOT}"/docker-compose/postgresql/postgresql-14.yaml
    sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/postgresql/postgresql-14.yaml
    sudo docker compose -f "${ROOT}"/docker-compose/postgresql/postgresql-14.yaml --env-file "${ROOT}"/docker-compose/postgresql/postgresql-14.env down
    if [[ "${STOP}" -ne 1 ]]; then
        sudo mkdir -p "${ROOT}"/docker-compose/postgresql/data/data
        sudo rm "${ROOT}"/docker-compose/postgresql/data/* -rf
        sudo docker compose -f "${ROOT}"/docker-compose/postgresql/postgresql-14.yaml --env-file "${ROOT}"/docker-compose/postgresql/postgresql-14.env up -d
    fi
fi

if [[ "${RUN_ORACLE}" -eq 1 ]]; then
    # oracle
    cp "${ROOT}"/docker-compose/oracle/oracle-11.yaml.tpl "${ROOT}"/docker-compose/oracle/oracle-11.yaml
    sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/oracle/oracle-11.yaml
    sudo docker compose -f "${ROOT}"/docker-compose/oracle/oracle-11.yaml --env-file "${ROOT}"/docker-compose/oracle/oracle-11.env down
    if [[ "${STOP}" -ne 1 ]]; then
        sudo mkdir -p "${ROOT}"/docker-compose/oracle/data/
        sudo rm "${ROOT}"/docker-compose/oracle/data/* -rf
        sudo docker compose -f "${ROOT}"/docker-compose/oracle/oracle-11.yaml --env-file "${ROOT}"/docker-compose/oracle/oracle-11.env up -d
    fi
fi

if [[ "${RUN_SQLSERVER}" -eq 1 ]]; then
    # sqlserver
    cp "${ROOT}"/docker-compose/sqlserver/sqlserver.yaml.tpl "${ROOT}"/docker-compose/sqlserver/sqlserver.yaml
    sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/sqlserver/sqlserver.yaml
    sudo docker compose -f "${ROOT}"/docker-compose/sqlserver/sqlserver.yaml --env-file "${ROOT}"/docker-compose/sqlserver/sqlserver.env down
    if [[ "${STOP}" -ne 1 ]]; then
        sudo mkdir -p "${ROOT}"/docker-compose/sqlserver/data/
        sudo rm "${ROOT}"/docker-compose/sqlserver/data/* -rf
        sudo docker compose -f "${ROOT}"/docker-compose/sqlserver/sqlserver.yaml --env-file "${ROOT}"/docker-compose/sqlserver/sqlserver.env up -d
    fi
fi

if [[ "${RUN_CLICKHOUSE}" -eq 1 ]]; then
    # clickhouse
    cp "${ROOT}"/docker-compose/clickhouse/clickhouse.yaml.tpl "${ROOT}"/docker-compose/clickhouse/clickhouse.yaml
    sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/clickhouse/clickhouse.yaml
    sudo docker compose -f "${ROOT}"/docker-compose/clickhouse/clickhouse.yaml --env-file "${ROOT}"/docker-compose/clickhouse/clickhouse.env down
    if [[ "${STOP}" -ne 1 ]]; then
        sudo mkdir -p "${ROOT}"/docker-compose/clickhouse/data/
        sudo rm "${ROOT}"/docker-compose/clickhouse/data/* -rf
        sudo docker compose -f "${ROOT}"/docker-compose/clickhouse/clickhouse.yaml --env-file "${ROOT}"/docker-compose/clickhouse/clickhouse.env up -d
    fi
fi

if [[ "${RUN_HIVE}" -eq 1 ]]; then
    # hive
    # before start it, you need to download parquet file package, see "README" in "docker-compose/hive/scripts/"
    cp "${ROOT}"/docker-compose/hive/gen_env.sh.tpl "${ROOT}"/docker-compose/hive/gen_env.sh
    sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/hive/gen_env.sh
    cp "${ROOT}"/docker-compose/hive/hive-2x.yaml.tpl "${ROOT}"/docker-compose/hive/hive-2x.yaml
    cp "${ROOT}"/docker-compose/hive/hadoop-hive.env.tpl.tpl "${ROOT}"/docker-compose/hive/hadoop-hive.env.tpl
    sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/hive/hive-2x.yaml
    sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/hive/hadoop-hive.env.tpl
    sudo bash "${ROOT}"/docker-compose/hive/gen_env.sh
    sudo docker compose -f "${ROOT}"/docker-compose/hive/hive-2x.yaml --env-file "${ROOT}"/docker-compose/hive/hadoop-hive.env down
    sudo sed -i '/${CONTAINER_UID}namenode/d' /etc/hosts
    if [[ "${STOP}" -ne 1 ]]; then
        sudo docker compose -f "${ROOT}"/docker-compose/hive/hive-2x.yaml --env-file "${ROOT}"/docker-compose/hive/hadoop-hive.env up --build --remove-orphans -d
        sudo echo "127.0.0.1 ${CONTAINER_UID}namenode" >> /etc/hosts
    fi
fi

if [[ "${RUN_ICEBERG}" -eq 1 ]]; then
    # iceberg
    cp "${ROOT}"/docker-compose/iceberg/iceberg.yaml.tpl "${ROOT}"/docker-compose/iceberg/iceberg.yaml
    cp "${ROOT}"/docker-compose/iceberg/entrypoint.sh.tpl "${ROOT}"/docker-compose/iceberg/entrypoint.sh
    cp "${ROOT}"/docker-compose/iceberg/spark-defaults.conf.tpl "${ROOT}"/docker-compose/iceberg/spark-defaults.conf
    sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/iceberg/iceberg.yaml
    sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/iceberg/entrypoint.sh
    sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/iceberg/spark-defaults.conf
    sudo docker compose -f "${ROOT}"/docker-compose/iceberg/iceberg.yaml --env-file "${ROOT}"/docker-compose/iceberg/iceberg.env down
    if [[ "${STOP}" -ne 1 ]]; then
        sudo rm -rf "${ROOT}"/docker-compose/iceberg/notebooks
        sudo mkdir "${ROOT}"/docker-compose/iceberg/notebooks
        sudo rm -rf "${ROOT}"/docker-compose/iceberg/spark
        sudo mkdir "${ROOT}"/docker-compose/iceberg/spark
        sudo rm -rf "${ROOT}"/docker-compose/iceberg/warehouse
        sudo mkdir "${ROOT}"/docker-compose/iceberg/warehouse
        sudo docker compose -f "${ROOT}"/docker-compose/iceberg/iceberg.yaml --env-file "${ROOT}"/docker-compose/iceberg/iceberg.env up -d
    fi
fi

if [[ "${RUN_HUDI}" -eq 1 ]]; then
    # hudi
    cp "${ROOT}"/docker-compose/hudi/hudi.yaml.tpl "${ROOT}"/docker-compose/hudi/hudi.yaml
    sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/hudi/hudi.yaml
    sudo docker compose -f "${ROOT}"/docker-compose/hudi/hudi.yaml --env-file "${ROOT}"/docker-compose/hudi/hadoop.env down
    if [[ "${STOP}" -ne 1 ]]; then
        sudo rm -rf "${ROOT}"/docker-compose/hudi/historyserver
        sudo mkdir "${ROOT}"/docker-compose/hudi/historyserver
        sudo rm -rf "${ROOT}"/docker-compose/hudi/hive-metastore-postgresql
        sudo mkdir "${ROOT}"/docker-compose/hudi/hive-metastore-postgresql
        if [[ ! -d "${ROOT}/docker-compose/hudi/scripts/hudi_docker_compose_attached_file" ]]; then
            echo "Attached files does not exist, please download the https://doris-build-hk-1308700295.cos.ap-hongkong.myqcloud.com/regression/load/hudi/hudi_docker_compose_attached_file.zip file to the docker-compose/hudi/scripts/ directory and unzip it."
            exit 1
        fi
        sudo docker compose -f "${ROOT}"/docker-compose/hudi/hudi.yaml --env-file "${ROOT}"/docker-compose/hudi/hadoop.env up -d
        echo "sleep 15, wait server start"
        sleep 15
        docker exec -it adhoc-1 /bin/bash /var/scripts/setup_demo_container_adhoc_1.sh
        docker exec -it adhoc-2 /bin/bash /var/scripts/setup_demo_container_adhoc_2.sh
    fi
fi

if  [[ "${RUN_TRINO}" -eq 1 ]]; then
    # trino
    trino_docker="${ROOT}"/docker-compose/trino
    TRINO_CONTAINER_ID="${CONTAINER_UID}trino"
    NAMENODE_CONTAINER_ID="${CONTAINER_UID}namenode"
    HIVE_METASTORE_CONTAINER_ID=${CONTAINER_UID}hive-metastore
    for file in trino_hive.yaml trino_hive.env gen_env.sh hive.properties
    do
        cp "${trino_docker}/$file.tpl" "${trino_docker}/$file"
        if [[ $file != "hive.properties" ]]; then
            sed -i "s/doris--/${CONTAINER_UID}/g" "${trino_docker}/$file"
        fi
    done

    bash "${trino_docker}"/gen_env.sh
    sudo docker compose -f "${trino_docker}"/trino_hive.yaml --env-file "${trino_docker}"/trino_hive.env down
    if [[ "${STOP}" -ne 1 ]]; then
        sudo sed -i "/${NAMENODE_CONTAINER_ID}/d" /etc/hosts
        sudo docker compose -f "${trino_docker}"/trino_hive.yaml --env-file "${trino_docker}"/trino_hive.env up --build --remove-orphans -d
        sudo echo "127.0.0.1 ${NAMENODE_CONTAINER_ID}" >> /etc/hosts
        sleep 20s
        hive_metastore_ip=$(docker inspect --format='{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ${HIVE_METASTORE_CONTAINER_ID})

        if [ -z "$hive_metastore_ip" ]; then
            echo "Failed to get Hive Metastore IP address" >&2
            exit 1
        else
          echo "Hive Metastore IP address is: $hive_metastore_ip"
        fi

        sed -i "s/metastore_ip/${hive_metastore_ip}/g" "${trino_docker}"/hive.properties
        docker cp "${trino_docker}"/hive.properties "${CONTAINER_UID}trino":/etc/trino/catalog/

        # trino load hive catalog need restart server
        max_retries=3

        function control_container() {
            max_retries=3
            operation=$1
            expected_status=$2
            retries=0

            while [ $retries -lt $max_retries ]
            do
                status=$(docker inspect --format '{{.State.Running}}' ${TRINO_CONTAINER_ID})
                if [ "${status}" == "${expected_status}" ]; then
                    echo "Container ${TRINO_CONTAINER_ID} has ${operation}ed successfully."
                    break
                else
                    echo "Waiting for container ${TRINO_CONTAINER_ID} to ${operation}..."
                    sleep 5s
                    ((retries++))
                fi
                sleep 3s
            done

            if [ $retries -eq $max_retries ]; then
                echo "${operation} operation failed to complete after $max_retries attempts."
                exit 1
            fi
        }
        # Stop the container
        docker stop ${TRINO_CONTAINER_ID}
        sleep 5s
        control_container "stop" "false"

        # Start the container
        docker start ${TRINO_CONTAINER_ID}
        control_container "start" "true"

        # waite trino init
        sleep 20s
        # execute create table sql
        docker exec -it ${TRINO_CONTAINER_ID} /bin/bash -c 'trino -f /scripts/create_trino_table.sql'
    fi
fi
