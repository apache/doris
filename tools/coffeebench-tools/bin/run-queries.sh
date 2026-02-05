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
# This script is used to run coffee-bench 17 queries
##############################################################

set -eo pipefail

ROOT=$(dirname "$0")
ROOT=$(
    cd "${ROOT}"
    pwd
)

CURDIR="${ROOT}"

usage() {
    echo "
This script is used to run coffee_benchmark 17 queries, 
will use mysql client to connect Doris server which parameter is specified in doris-cluster.conf file.
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
export MYSQL_PWD=${PASSWORD:-}

echo "FE_HOST: ${FE_HOST:='127.0.0.1'}"
echo "FE_QUERY_PORT: ${FE_QUERY_PORT:='9030'}"
echo "USER: ${USER:='root'}"
echo "DB: ${DB:='coffee_shop'}"
echo "Time Unit: ms"

run_sql() {
    echo "$*"
    mysql -h"${FE_HOST}" -u"${USER}" -P"${FE_QUERY_PORT}" -D"${DB}" -e "$*"
}

run_sql "set global parallel_pipeline_task_num = 30;"
echo '============================================'
run_sql "show variables;"
echo '============================================'
run_sql "show table status;"
echo '============================================'

RESULT_DIR="${CURDIR}/result"
QUERIES_DIR="${CURDIR}/../queries"

if [[ -d "${RESULT_DIR}" ]]; then
    rm -r "${RESULT_DIR}"
fi
mkdir -p "${RESULT_DIR}"
touch result.csv
best_run_sum=0
# run part of queries, set their index to query_array
query_array=$(seq 1 17)
# shellcheck disable=SC2068
for i in ${query_array[@]}; do
    cold=0
    hot1=0
    hot2=0
    hot3=0
    hot4=0
    echo -ne "q${i}\t" | tee -a result.csv

    # Cold run test (first execution)
    start=$(date +%s%3N)
    mysql -h"${FE_HOST}" -u "${USER}" -P"${FE_QUERY_PORT}" -D"${DB}" --comments <"${QUERIES_DIR}"/q"${i}".sql >"${RESULT_DIR}"/result"${i}".out 2>"${RESULT_DIR}"/result"${i}".log
    end=$(date +%s%3N)
    cold=$((end - start))
    echo -ne "${cold}\t" | tee -a result.csv

    # Hot run test 1 (second execution)
    start=$(date +%s%3N)
    mysql -h"${FE_HOST}" -u "${USER}" -P"${FE_QUERY_PORT}" -D"${DB}" --comments <"${QUERIES_DIR}"/q"${i}".sql >"${RESULT_DIR}"/result"${i}".out 2>"${RESULT_DIR}"/result"${i}".log
    end=$(date +%s%3N)
    hot1=$((end - start))
    echo -ne "${hot1}\t" | tee -a result.csv

    # Hot run test 2 (third execution)
    start=$(date +%s%3N)
    mysql -h"${FE_HOST}" -u "${USER}" -P"${FE_QUERY_PORT}" -D"${DB}" --comments <"${QUERIES_DIR}"/q"${i}".sql >"${RESULT_DIR}"/result"${i}".out 2>"${RESULT_DIR}"/result"${i}".log
    end=$(date +%s%3N)
    hot2=$((end - start))
    echo -ne "${hot2}\t" | tee -a result.csv

    # Hot run test 3 (fourth execution)
    start=$(date +%s%3N)
    mysql -h"${FE_HOST}" -u "${USER}" -P"${FE_QUERY_PORT}" -D"${DB}" --comments <"${QUERIES_DIR}"/q"${i}".sql >"${RESULT_DIR}"/result"${i}".out 2>"${RESULT_DIR}"/result"${i}".log
    end=$(date +%s%3N)
    hot3=$((end - start))
    echo -ne "${hot3}\t" | tee -a result.csv

    # Hot run test 4 (fifth execution)
    start=$(date +%s%3N)
    mysql -h"${FE_HOST}" -u "${USER}" -P"${FE_QUERY_PORT}" -D"${DB}" --comments <"${QUERIES_DIR}"/q"${i}".sql >"${RESULT_DIR}"/result"${i}".out 2>"${RESULT_DIR}"/result"${i}".log
    end=$(date +%s%3N)
    hot4=$((end - start))
    echo -ne "${hot4}\t" | tee -a result.csv

    # Find the minimum execution time from all runs
    min_time=${cold}
    if [[ ${hot1} -lt ${min_time} ]]; then min_time=${hot1}; fi
    if [[ ${hot2} -lt ${min_time} ]]; then min_time=${hot2}; fi
    if [[ ${hot3} -lt ${min_time} ]]; then min_time=${hot3}; fi
    if [[ ${hot4} -lt ${min_time} ]]; then min_time=${hot4}; fi

    # Update cumulative time counters
    best_run_sum=$((best_run_sum + min_time))

    # Output the best time for this query
    echo -ne "${min_time}" | tee -a result.csv
    echo "" | tee -a result.csv
done

echo "Total hot run time: ${best_run_sum} ms"
echo 'Finish coffee bench queries.'
