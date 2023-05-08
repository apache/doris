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

CURDIR="${ROOT}"
QUERIES_DIR="${CURDIR}/../queries"

usage() {
    echo "
This script is used to run TPC-H 22queries, 
will use mysql client to connect Doris server which parameter is specified in doris-cluster.conf file.
Usage: $0 
  "
    exit 1
}

OPTS=$(getopt \
    -n "$0" \
    -o '' \
    -- "$@")

eval set -- "${OPTS}"
HELP=0

if [[ $# == 0 ]]; then
    usage
fi

while true; do
    case "$1" in
    -h)
        HELP=1
        shift
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

run_sql() {
    echo "$*"
    mysql -h"${FE_HOST}" -u"${USER}" -P"${FE_QUERY_PORT}" -D"${DB}" -e "$*"
}

echo '============================================'
run_sql "show variables;"
echo '============================================'
run_sql "show table status;"
echo '============================================'
echo "Time Unit: ms"

touch result.csv
cold_run_sum=0
best_hot_run_sum=0
for i in $(seq 1 22); do
    cold=0
    hot1=0
    hot2=0

    echo -ne "q${i}\t" | tee -a result.csv

    start=$(date +%s%3N)
    mysql -h"${FE_HOST}" -u "${USER}" -P"${FE_QUERY_PORT}" -D"${DB}" --comments <"${QUERIES_DIR}/q${i}.sql" >/dev/null
    end=$(date +%s%3N)
    cold=$((end - start))
    echo -ne "${cold}\t" | tee -a result.csv

    start=$(date +%s%3N)
    mysql -h"${FE_HOST}" -u "${USER}" -P"${FE_QUERY_PORT}" -D"${DB}" --comments <"${QUERIES_DIR}/q${i}.sql" >/dev/null
    end=$(date +%s%3N)
    hot1=$((end - start))
    echo -ne "${hot1}\t" | tee -a result.csv

    start=$(date +%s%3N)
    mysql -h"${FE_HOST}" -u "${USER}" -P"${FE_QUERY_PORT}" -D"${DB}" --comments <"${QUERIES_DIR}/q${i}.sql" >/dev/null
    end=$(date +%s%3N)
    hot2=$((end - start))
    echo -ne "${hot2}" | tee -a result.csv

    echo "" | tee -a result.csv

    cold_run_sum=$((cold_run_sum + cold))
    if [[ ${hot1} -lt ${hot2} ]]; then
        best_hot_run_sum=$((best_hot_run_sum + hot1))
    else
        best_hot_run_sum=$((best_hot_run_sum + hot2))
    fi
done

echo "Total cold run time: ${cold_run_sum} ms"
echo "Total hot run time: ${best_hot_run_sum} ms"
echo 'Finish tpch queries.'
