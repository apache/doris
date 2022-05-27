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
  cd "$ROOT"
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
  -n $0 \
  -o '' \
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

if [[ ${HELP} -eq 1 ]]; then
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

source $CURDIR/doris-cluster.conf
export MYSQL_PWD=$PASSWORD

echo "FE_HOST: $FE_HOST"
echo "FE_QUERY_PORT: $FE_QUERY_PORT"
echo "USER: $USER"
echo "PASSWORD: $PASSWORD"
echo "DB: $DB"

mysql -h$FE_HOST -u$USER -P$FE_QUERY_PORT -e "CREATE DATABASE IF NOT EXISTS $DB"

echo $CURDIR/create-tpch-tables.sql
mysql -h$FE_HOST -u$USER -P$FE_QUERY_PORT -D$DB <$CURDIR/create-tpch-tables.sql
