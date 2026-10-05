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

#mkdir files to store tables and tables.sql
mkdir -p ${home_dir}/result/mysql

#The default path is ../result/mysql_to_doris.sql for create table sql
path=${1:-${home_dir}/result/mysql/sync_to_doris.sql}

rm -f $path

# generate insert into select statement
idx=0
for table in $(cat ${home_dir}/conf/mysql_tables | grep -v '#' | awk -F '\n' '{print $1}' | sed 's/ //g' | sed '/^$/d'); do
        m_d=$(echo $table | awk -F '.' '{print $1}')
        m_t=$(echo $table | awk -F '.' '{print $2}')
        # get mysql table columns
        columns=$(echo "SELECT group_concat(COLUMN_NAME) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '$m_d' AND TABLE_NAME = '$m_t';" | mysql -N -h"${mysql_host}" -P"${mysql_port}" -u"${mysql_username}" -p"${mysql_password}")
        let idx++
        i_t=$(cat ${home_dir}/conf/doris_tables | grep -v '#' | awk "NR==$idx{print}" | sed 's/ //g' | sed 's/\./`.`/g')
        e_t=$(cat ${home_dir}/conf/doris_external_tables | grep -v '#' | awk "NR==$idx{print}" | sed 's/ //g' | sed 's/\./`.`/g')
        echo "INSERT INTO \`${i_t}\` (${columns}) SELECT ${columns} FROM \`${e_t}\`;" >> $path
done