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

##############################################################
# This script is used to create TPC-H tables
##############################################################

set -eo pipefail

ROOT=$(dirname "$0")
ROOT=$(
    cd "${ROOT}"
    pwd
)

CURDIR=${ROOT}

usage() {
    echo "
This script is used to create TPC-H tables, 
will use mysql client to connect Doris server which is specified in doris-cluster.conf file.
Usage: $0 
  "
    exit 1
}

OPTS=$(getopt \
    -n "$0" \
    -o '' \
    -o 'hs:' \
    -- "$@")

eval set -- "${OPTS}"
HELP=0
SCALE_FACTOR=100

if [[ $# == 0 ]]; then
    usage
fi

while true; do
    case "$1" in
    -h)
        HELP=1
        shift
        ;;
    -s)
        SCALE_FACTOR=$2
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

if [[ "${HELP}" -eq 1 ]]; then
    usage
fi

if [[ ${SCALE_FACTOR} -ne 1 ]] && [[ ${SCALE_FACTOR} -ne 100 ]] && [[ ${SCALE_FACTOR} -ne 1000 ]] && [[ ${SCALE_FACTOR} -ne 10000 ]]; then
    echo "${SCALE_FACTOR} scale is not supported"
    exit 1
fi

check_prerequest() {
    local CMD=$1
    local NAME=$2
    if ! ${CMD}; then
        echo "${NAME} is missing. This script depends on mysql to create tables in Doris."
        exit 1
    fi
}

check_prerequest "mysql --version" "mysql"

source "${CURDIR}/../conf/doris-cluster.conf"
export MYSQL_PWD=${PASSWORD}

echo "FE_HOST: ${FE_HOST}"
echo "FE_QUERY_PORT: ${FE_QUERY_PORT}"
echo "USER: ${USER}"
echo "DB: ${DB}"
echo "SF: ${SCALE_FACTOR}"

mysql -h"${FE_HOST}" -u"${USER}" -P"${FE_QUERY_PORT}" -e "CREATE DATABASE IF NOT EXISTS ${DB}"

if [[ ${SCALE_FACTOR} -eq 1 ]]; then
    echo "Run SQLs from ${CURDIR}/../ddl/create-tpch-tables.sql"
    mysql -h"${FE_HOST}" -u"${USER}" -P"${FE_QUERY_PORT}" -D"${DB}" <"${CURDIR}"/../ddl/create-tpch-tables-sf1.sql
elif [[ ${SCALE_FACTOR} -eq 100 ]]; then
    echo "Run SQLs from ${CURDIR}/../ddl/create-tpch-tables-sf100.sql"
    mysql -h"${FE_HOST}" -u"${USER}" -P"${FE_QUERY_PORT}" -D"${DB}" <"${CURDIR}"/../ddl/create-tpch-tables-sf100.sql
elif [[ ${SCALE_FACTOR} -eq 1000 ]]; then
    echo "Run SQLs from ${CURDIR}/../ddl/create-tpch-tables-sf1000.sql"
    mysql -h"${FE_HOST}" -u"${USER}" -P"${FE_QUERY_PORT}" -D"${DB}" <"${CURDIR}"/../ddl/create-tpch-tables-sf1000.sql
elif [[ ${SCALE_FACTOR} -eq 10000 ]]; then
    echo "Run SQLs from ${CURDIR}/../ddl/create-tpch-tables-sf10000.sql"
    mysql -h"${FE_HOST}" -u"${USER}" -P"${FE_QUERY_PORT}" -D"${DB}" <"${CURDIR}"/../ddl/create-tpch-tables-sf10000.sql
else
    echo "${SCALE_FACTOR} scale is NOT supported currently"
fi

echo "tpch tables has been created"
