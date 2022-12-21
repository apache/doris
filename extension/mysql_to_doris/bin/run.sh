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

function usage() {
  echo "Usage: run.sh [option]"
  echo "    -e, --create-external-table: create doris external table"
  echo "    -o, --create-olap-table: create doris olap table"
  echo "    -i, --insert-data: insert data into doris olap table from doris external table"
  echo "    -d, --drop-external-table: drop doris external table"
  echo "    -a, --auto-external-table: create doris external table and auto check mysql schema change"
  echo "    --database: specify the database name to process all tables under the entire database, and separate multiple databases with \",\""
  echo "    -t, --type: specify external table type, valid options: ODBC(default), JDBC"
  echo "    -h, --help: show usage"
  exit 1
}

cur_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

if [[ $# -eq 0 ]]; then
  usage
fi

opts=$(getopt -o eaoidht: \
  -l create-external-table \
  -l create-olap-table \
  -l insert-datadrop-external-table \
  -l auto-external-table \
  -l database: \
  -l type: \
  -l help \
  -n "$0" \
  -- "$@")

eval set -- "${opts}"

CREATE_EXTERNAL_TABLE=0
CREATE_OLAP_TABLE=0
INSERT_DATA=0
DROP_EXTERNAL_TABLE=0
AUTO_EXTERNAL_TABLE=0
DATABASE=''
TYPE='ODBC'

while true; do
  case "$1" in
  -e | --create-external-table)
    CREATE_EXTERNAL_TABLE=1
    shift
    ;;
  -o | --create-olap-table)
    CREATE_OLAP_TABLE=1
    shift
    ;;
  -i | --insert-data)
    INSERT_DATA=1
    shift
    ;;
  -d | --drop-external-table)
    DROP_EXTERNAL_TABLE=1
    shift
    ;;
  -a | --auto-external-table)
    AUTO_EXTERNAL_TABLE=1
    shift
    ;;
  --database)
    DATABASE="$2"
    shift 2
    ;;
  -t | --type)
    TYPE="$2"
    shift 2
    ;;
  -h | --help)
    usage
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

home_dir=$(cd "${cur_dir}"/.. && pwd)

source "${home_dir}"/conf/env.conf

# when fe_password is not set or is empty, do not put -p option
use_passwd=$([ -z "${doris_password}" ] && echo "" || echo "-p${doris_password}")

if [ -n "${DATABASE}" ]; then
  sh "${home_dir}"/lib/get_tables.sh "${DATABASE}"
fi

# create doris jdbc catalog
if [[ "JDBC" == "${TYPE}" && "${CREATE_EXTERNAL_TABLE}" -eq 1 ]]; then
  echo "====================== start create doris jdbc catalog ======================"
  sh "${home_dir}"/lib/jdbc/create_jdbc_catalog.sh "${home_dir}"/result/mysql/jdbc_catalog.sql 2>>error.log
  echo "source ${home_dir}/result/mysql/jdbc_catalog.sql;" | mysql -h"${fe_master_host}" -P"${fe_master_port}" -u"${doris_username}" "${use_passwd}" 2>>error.log
  res=$?
  if [ "${res}" != 0 ]; then
      echo "====================== create doris jdbc catalog failed ======================"
      exit "${res}"
    fi
  echo "====================== create doris jdbc catalog finished ======================"
fi

# create doris external table
if [[ "ODBC" == "${TYPE}" && "${CREATE_EXTERNAL_TABLE}" -eq 1 ]]; then
  echo "====================== start create doris external table ======================"
  sh "${home_dir}"/lib/e_mysql_to_doris.sh "${home_dir}"/result/mysql/e_mysql_to_doris.sql 2>error.log
  echo "source ${home_dir}/result/mysql/e_mysql_to_doris.sql;" | mysql -h"${fe_master_host}" -P"${fe_master_port}" -u"${doris_username}" "${use_passwd}" 2>>error.log
  res=$?
  if [ "${res}" != 0 ]; then
    echo "====================== create doris external table failed ======================"
    exit "${res}"
  fi
  echo "====================== create doris external table finished ======================"
fi

# create doris olap table
if [[ "${CREATE_OLAP_TABLE}" -eq 1 ]]; then
  echo "====================== start create doris olap table ======================"
  sh "${home_dir}"/lib/mysql_to_doris.sh "${home_dir}"/result/mysql/mysql_to_doris.sql 2>>error.log
  echo "source ${home_dir}/result/mysql/mysql_to_doris.sql;" | mysql -h"${fe_master_host}" -P"${fe_master_port}" -u"${doris_username}" "${use_passwd}" 2>>error.log
  res=$?
  if [ "${res}" != 0 ]; then
    echo "====================== create doris olap table failed ======================"
    exit "${res}"
  fi
  echo "====================== create doris olap table finished ======================"
fi

# insert data into doris olap table
if [[ "${INSERT_DATA}" -eq 1 ]]; then
  echo "====================== start insert data ======================"
  if [[ "JDBC" == "${TYPE}" ]]; then
    sh "${home_dir}"/lib/jdbc/sync_to_doris.sh "${home_dir}"/result/mysql/sync_to_doris.sql 2>>error.log
  else
    sh "${home_dir}"/lib/sync_to_doris.sh "${home_dir}"/result/mysql/sync_to_doris.sql 2>>error.log
  fi
  echo "source ${home_dir}/result/mysql/sync_to_doris.sql;" | mysql -h"${fe_master_host}" -P"${fe_master_port}" -u"${doris_username}" "${use_passwd}" 2>>error.log
  res=$?
  if [ "${res}" != 0 ]; then
    echo "====================== insert data failed ======================"
    exit "${res}"
  fi
  echo "====================== insert data finished ======================"
  echo "====================== start sync check ======================"
  sh "${home_dir}"/lib/sync_check.sh "${home_dir}"/result/mysql/sync_check 2>>error.log
  res=$?
  if [ "${res}" != 0 ]; then
    echo "====================== sync check failed ======================"
    exit "${res}"
  fi
  echo "====================== sync check finished ======================"
fi

# drop doris external table
if [[ "ODBC" == "${TYPE}" && "${DROP_EXTERNAL_TABLE}" -eq 1 ]]; then
  echo "====================== start drop doris external table =========================="
  for table in $(cat ${home_dir}/conf/doris_external_tables | grep -v '#' | awk -F '\n' '{print $1}' | sed 's/\./`.`/g'); do
    echo "DROP TABLE IF EXISTS \`${table}\`;" | mysql -h"${fe_master_host}" -P"${fe_master_port}" -u"${doris_username}" "${use_passwd}" 2>>error.log
    res=$?
    if [ "${res}" != 0 ]; then
      echo "====================== drop doris external table failed ======================"
      exit "${res}"
    fi
  done
  echo "====================== create drop external table finished ======================"
fi

# create doris external table and auto check mysql schema change
if [[ "ODBC" == "${TYPE}" && "${AUTO_EXTERNAL_TABLE}" -eq 1 ]]; then
  echo "====================== start auto doris external table ======================"
  nohup sh ${home_dir}/lib/e_auto.sh &
  echo $! >e_auto.pid
  echo "====================== create doris external table started ======================"
fi
