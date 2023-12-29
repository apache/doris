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
# This script is used to run ClickBench queries
##############################################################

set -eo pipefail

ROOT=$(dirname "$0")
ROOT=$(
  cd "$ROOT"
  pwd
)

CURDIR=${ROOT}
QUERIES_FILE=$CURDIR/sql/queries.sql

usage() {
  echo "
This script is used to run ClickBench 43 queries, 
will use mysql client to connect Doris server which parameter is specified in conf/doris-cluster.conf file.
Usage: $0 
  "
  exit 1
}

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -o 'h' \
  -- "$@")

eval set -- "$OPTS"
HELP=0

if [ $# == 0 ]; then
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
  exit
fi

check_prerequest() {
  local CMD=$1
  local NAME=$2
  if ! $CMD; then
    echo "$NAME is missing. This script depends on mysql to create tables in Doris."
    exit 1
  fi
}

check_prerequest "mysql --version" "mysql"
check_prerequest "perl --version" "perl"

source $CURDIR/conf/doris-cluster.conf
export MYSQL_PWD=$PASSWORD

echo "FE_HOST: $FE_HOST"
echo "FE_QUERY_PORT: $FE_QUERY_PORT"
echo "USER: $USER"
echo "PASSWORD: $PASSWORD"
echo "DB: $DB"

run_sql() {
  echo $@
  mysql -h$FE_HOST -u$USER -P$FE_QUERY_PORT -D$DB -e "$@"
}

get_session_variable() {
  k="$1"
  v=$(mysql -h"${FE_HOST}" -u"${USER}" -P"${FE_QUERY_PORT}" -D"${DB}" -e"show variables like '${k}'\G" | grep " Value: ")
  echo "${v/*Value: /}"
}

_parallel_fragment_exec_instance_num="$(get_session_variable parallel_fragment_exec_instance_num)"
_exec_mem_limit="$(get_session_variable exec_mem_limit)"
_query_timeout="$(get_session_variable query_timeout)"

echo '============================================'
echo "Optimize session variables"
run_sql "set global parallel_fragment_exec_instance_num=16;"
run_sql "set global exec_mem_limit=32G;"
run_sql "set global query_timeout=900;"
echo '============================================'
run_sql "show variables"
echo '============================================'
run_sql "analyze table hits with sync;"

TRIES=3
QUERY_NUM=1
touch result.csv
truncate -s0 result.csv

cat ${QUERIES_FILE} | while read query; do
  if [[ ! $query == SELECT* ]]; then
    continue
  fi
  sync
  echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null

  echo -n "query${QUERY_NUM}," | tee -a result.csv
  for i in $(seq 1 $TRIES); do
    RES=$(mysql -vvv -h$FE_HOST -u$USER -P$FE_QUERY_PORT -D$DB -e "${query}" | perl -nle 'print $1 if /\((\d+\.\d+)+ sec\)/' || :)

    echo -n "${RES}" | tee -a result.csv
    [[ "$i" != $TRIES ]] && echo -n "," | tee -a result.csv
  done
  echo "" | tee -a result.csv

  QUERY_NUM=$((QUERY_NUM + 1))
done

cold_run_sum=$(awk -F ',' '{sum+=$2} END {print sum}' result.csv)
best_hot_run_sum=$(awk -F ',' '{if($3<$4){sum+=$3}else{sum+=$4}} END {print sum}' result.csv)
echo "Total cold run time: ${cold_run_sum} ms"
echo "Total hot run time: ${best_hot_run_sum} ms"
echo 'Finish ClickBench queries.'

echo "Restore session variables"
run_sql "set global parallel_fragment_exec_instance_num=${_parallel_fragment_exec_instance_num};"
run_sql "set global exec_mem_limit=${_exec_mem_limit};"
run_sql "set global query_timeout=${_query_timeout};"
