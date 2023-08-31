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
will use mysql client to connect Doris server which parameter is specified in 'cluster.conf' file.
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

source "${CURDIR}/../conf/cluster.conf"
export MYSQL_PWD=${PASSWORD}

echo "FE_HOST: ${FE_HOST}"
echo "FE_QUERY_PORT: ${FE_QUERY_PORT}"
echo "USER: ${USER}"
echo "DB: ${DB}"

touch $CURDIR/../ssb_flat_query_result.csv
truncate -s0 $CURDIR/../ssb_flat_query_result.csv

clt=""
exec_clt=""
if [ -z "${PASSWORD}" ];then
    clt="mysql -h${FE_HOST} -u${USER} -P${FE_QUERY_PORT} -D${DB} "
    exec_clt="mysql -vvv -h$FE_HOST -u$USER -P$FE_QUERY_PORT -D$DB "
else
    clt="mysql -h${FE_HOST} -u${USER} -p${PASSWORD} -P${FE_QUERY_PORT} -D${DB} "
    exec_clt="mysql -vvv -h$FE_HOST -u$USER -p${PASSWORD} -P$FE_QUERY_PORT -D$DB "
fi

run_sql() {
    echo "$@"
    $clt -e "$@"
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
echo "analyze tables with sync total time: ${totalTime} s"
echo '============================================'

TRIES=3
sum=0
total_time=0
for i in '1.1' '1.2' '1.3' '2.1' '2.2' '2.3' '3.1' '3.2' '3.3' '3.4' '4.1' '4.2' '4.3'; do
    # Each query is executed four times, taking the best of the next three
    ${exec_clt} < "${QUERIES_DIR}/q${i}.sql" >/dev/null 2>&1

    echo -n "q${i}.sql:" | tee -a $CURDIR/../ssb_flat_query_result.csv
    runtime=0.0000
    for ord in $(seq 1 $TRIES); do
       res=$(${exec_clt} < "${QUERIES_DIR}/q${i}.sql" | perl -nle 'print $1 if /\((\d+\.\d+)+ sec\)/' || :)
       sum=0.0000
       while IFS= read -r number; do
         sum=$(echo "$sum + $number" | bc)
       done <<< "$res"
       if [ $ord -eq 1 ];then
          runtime=$(echo "scale=6;0.0000+$sum" | bc)
       else
          if [ $( echo "$runtime > $sum" | bc ) -eq 1 ];then
              runtime=$(echo "scale=6;0.0000+$sum" | bc)
          fi
       fi
    done

    echo -n "${runtime}" | tee -a $CURDIR/../ssb_flat_query_result.csv
    echo "" | tee -a $CURDIR/../ssb_flat_query_result.csv

    total_time=$(echo "scale=6;$total_time+$runtime" | bc)
done
echo "total time: ${total_time} seconds"

echo '============================================'
echo "restore session variables"
run_sql "set global parallel_fragment_exec_instance_num=${origin_parallel_fragment_exec_instance_num};"
run_sql "set global exec_mem_limit=${origin_exec_mem_limit};"
run_sql "set global batch_size=${origin_batch_size};"
echo '============================================'

echo 'Finish ssb-flat queries.'
