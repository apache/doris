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
QUERIES_FILE=$CURDIR/../queries/queries.sql

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

source $CURDIR/../conf/cluster.conf
export MYSQL_PWD=$PASSWORD

echo "FE_HOST: $FE_HOST"
echo "FE_QUERY_PORT: $FE_QUERY_PORT"
echo "USER: $USER"
echo "PASSWORD: $PASSWORD"
echo "DB: $DB"

clt=""
exec_clt=""
if [ -z "${PASSWORD}" ];then
    clt="mysql -h${FE_HOST} -u${USER} -P${FE_QUERY_PORT} -D${DB} "
    exec_clt="mysql -vvv -h$FE_HOST -u$USER -P$FE_QUERY_PORT -D$DB "
else
    clt="mysql -h${FE_HOST} -u${USER} -p${PASSWORD} -P${FE_QUERY_PORT} -D${DB} "
    exec_clt="mysql -vvv -h$FE_HOST -u$USER -p${PASSWORD} -P$FE_QUERY_PORT -D$DB "
fi

pre_set() {
  echo $@
  $clt -e "$@"
}

echo '============================================'
pre_set "set global parallel_fragment_exec_instance_num=8;"
pre_set "set global exec_mem_limit=32G;"
echo '============================================'
pre_set "show variables"
echo '============================================'
pre_set "analyze table hits with sync;"
echo '============================================'

TRIES=3
QUERY_NUM=1

echo "step:  execute queries"

touch $CURDIR/../clickbench_query_result.csv
truncate -s0 $CURDIR/../clickbench_query_result.csv
total_time=0
cat ${QUERIES_FILE} | while read query; do
  if [[ ! $query == SELECT* ]]; then
    continue
  fi
  sync
  echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null

  $exec_clt -e "${query}" >/dev/null 2>&1

  echo -n "q_${QUERY_NUM}.sql:" | tee -a $CURDIR/../clickbench_query_result.csv
  runtime=0.0000
  for i in $(seq 1 $TRIES); do
     res=$($exec_clt -e "${query}" | perl -nle 'print $1 if /\((\d+\.\d+)+ sec\)/' || :)
     sum=0.0000
     while IFS= read -r number; do
       sum=$(echo "$sum + $number" | bc)
     done <<< "$res"
     if [ $i -eq 1 ];then
        runtime=$(echo "scale=6;0.0000+$sum" | bc)
     else
        if [ $( echo "$runtime > $sum" | bc ) -eq 1 ];then
            runtime=$(echo "scale=6;0.0000+$sum" | bc)
        fi
     fi
  done

  echo -n "${runtime}" | tee -a $CURDIR/../clickbench_query_result.csv
  echo "" | tee -a $CURDIR/../clickbench_query_result.csv

  QUERY_NUM=$((QUERY_NUM + 1))
  total_time=$(echo "scale=6;$total_time+$runtime" | bc)
done
echo "total time: ${total_time} seconds"
