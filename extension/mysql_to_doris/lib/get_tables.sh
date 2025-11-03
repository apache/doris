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

cur_dir="$(cd "$(dirname "$0")" && pwd)"
home_dir=$(cd "${cur_dir}"/.. && pwd)

source ${home_dir}/conf/env.conf

dbs=$1
if [ -z ${dbs} ]; then
    echo "database option is empty"
    exit 1
fi

# get mysql tables and write to ${home_dir}/conf/mysql_tables
echo "select concat(TABLE_SCHEMA, '.', TABLE_NAME) from information_schema.TABLES where TABLE_SCHEMA in ('$(echo ${dbs} | sed "s/,/','/g")') and TABLE_TYPE = 'BASE TABLE' order by TABLE_SCHEMA, TABLE_NAME;" | mysql -N -h"${mysql_host}" -P"${mysql_port}" -u"${mysql_username}" -p"${mysql_password}" > "${home_dir}"/conf/mysql_tables
# get mysql tables and generate external tables and write to ${home_dir}/conf/doris_external_tables
echo "select concat(TABLE_SCHEMA , '.e_', TABLE_NAME) from information_schema.TABLES where TABLE_SCHEMA in ('$(echo ${dbs} | sed "s/,/','/g")') and TABLE_TYPE = 'BASE TABLE' order by TABLE_SCHEMA, TABLE_NAME;" | mysql -N -h"${mysql_host}" -P"${mysql_port}" -u"${mysql_username}" -p"${mysql_password}" > "${home_dir}"/conf/doris_external_tables
# get mysql tables and generate olap tables and write to ${home_dir}/conf/doris_tables
echo "select concat(TABLE_SCHEMA , '.', TABLE_NAME) from information_schema.TABLES where TABLE_SCHEMA in ('$(echo ${dbs} | sed "s/,/','/g")') and TABLE_TYPE = 'BASE TABLE' order by TABLE_SCHEMA, TABLE_NAME;" | mysql -N -h"${mysql_host}" -P"${mysql_port}" -u"${mysql_username}" -p"${mysql_password}" > "${home_dir}"/conf/doris_tables