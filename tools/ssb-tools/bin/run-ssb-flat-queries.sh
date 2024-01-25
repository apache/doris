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
# This script is used to create ssb flat queries
##############################################################

set -eo pipefail

ROOT=$(dirname "$0")
ROOT=$(
    cd "${ROOT}"
    pwd
)

CURDIR="${ROOT}"
QUERIES_DIR="${CURDIR}/../ssb-flat-queries"

usage() {
    echo "
This script is used to run SSB flat 13queries, 
will use mysql client to connect Doris server which parameter is specified in 'doris-cluster.conf' file.
Usage: $0 
  "
    exit 1
}

OPTS=$(getopt \
    -n "$0" \
    -o '' \
    -o 'h' \
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

check_prerequest "mysqlslap --version" "mysqlslap"
check_prerequest "mysql --version" "mysql"
check_prerequest "bc --version" "bc"

source "${CURDIR}/../conf/doris-cluster.conf"
export MYSQL_PWD=${PASSWORD}

echo "FE_HOST: ${FE_HOST}"
echo "FE_QUERY_PORT: ${FE_QUERY_PORT}"
echo "USER: ${USER}"
echo "DB: ${DB}"

run_sql() {
    echo "$@"
    mysql -h"${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${USER}" -D"${DB}" -e "$@"
}

echo '============================================'
echo "optimize some session variables before run, and then restore it after run."
origin_parallel_fragment_exec_instance_num=$(
    set -e
    run_sql 'select @@parallel_fragment_exec_instance_num;' | sed -n '3p'
)
origin_exec_mem_limit=$(
    set -e
    run_sql 'select @@exec_mem_limit;' | sed -n '3p'
)
origin_batch_size=$(
    set -e
    run_sql 'select @@batch_size;' | sed -n '3p'
)
run_sql "set global parallel_fragment_exec_instance_num=8;"
run_sql "set global exec_mem_limit=8G;"
run_sql "set global batch_size=4096;"
run_sql "set global query_timeout=900;"
echo '============================================'
run_sql "show variables;"
echo '============================================'
run_sql "show table status;"
echo '============================================'
start=$(date +%s)
run_sql "analyze table lineorder_flat with sync;"
end=$(date +%s)
totalTime=$((end - start))
echo "analyze database ${DB} with sync total time: ${totalTime} s"
echo '============================================'

sum=0
for i in '1.1' '1.2' '1.3' '2.1' '2.2' '2.3' '3.1' '3.2' '3.3' '3.4' '4.1' '4.2' '4.3'; do
    # Each query is executed 3 times and takes the min time
    res1=$(mysql -vvv -h"${FE_HOST}" -u"${USER}" -P"${FE_QUERY_PORT}" -D"${DB}" -e "$(cat "${QUERIES_DIR}"/q"${i}".sql)" | perl -nle 'print $1 if /\((\d+\.\d+)+ sec\)/' || :)
    res2=$(mysql -vvv -h"${FE_HOST}" -u"${USER}" -P"${FE_QUERY_PORT}" -D"${DB}" -e "$(cat "${QUERIES_DIR}"/q"${i}".sql)" | perl -nle 'print $1 if /\((\d+\.\d+)+ sec\)/' || :)
    res3=$(mysql -vvv -h"${FE_HOST}" -u"${USER}" -P"${FE_QUERY_PORT}" -D"${DB}" -e "$(cat "${QUERIES_DIR}"/q"${i}".sql)" | perl -nle 'print $1 if /\((\d+\.\d+)+ sec\)/' || :)

    min_value=$(echo "${res1} ${res2} ${res3}" | tr ' ' '\n' | sort -n | head -n 1)
    echo -e "q${i}:\t${res1}\t${res2}\t${res3}\tfast:${min_value}"

    cost=$(echo "${min_value}" | cut -d' ' -f1)
    sum=$(echo "${sum} + ${cost}" | bc)
done
echo "total time: ${sum} seconds"

echo '============================================'
echo "restore session variables"
run_sql "set global parallel_fragment_exec_instance_num=${origin_parallel_fragment_exec_instance_num};"
run_sql "set global exec_mem_limit=${origin_exec_mem_limit};"
run_sql "set global batch_size=${origin_batch_size};"
echo '============================================'

echo 'Finish ssb-flat queries.'
