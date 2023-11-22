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
path=${1:-${home_dir}/result/mysql/sync_check}

rm -f $path

# when fe_password is not set or is empty, do not put -p option
use_passwd=$([ -z "${fe_password}" ] && echo "" || echo "-p${fe_password}")

# generate insert into select statement
idx=0
for table in $(cat ${home_dir}/conf/mysql_tables | grep -v '#' | awk -F '\n' '{print $1}' | sed 's/ //g' | sed '/^$/d'); do
        m_d=$(echo $table | awk -F '.' '{print $1}')
        m_t=$(echo $table | awk -F '.' '{print $2}')
        let idx++
        i_t=$(cat ${home_dir}/conf/doris_tables | grep -v '#' | awk "NR==$idx{print}" | sed 's/ //g' | sed 's/\./`.`/g')
        # get mysql table count
        m_count=$(echo "SELECT count(*) FROM \`${m_d}\`.\`${m_t}\`;" | mysql -N -h"${mysql_host}" -P"${mysql_port}" -u"$mysql_username" -p"$mysql_password")
        d_count=$(echo "SELECT count(*) FROM \`${i_t}\`;" | mysql -N -h"${fe_master_host}" -P"${fe_master_port}" -u"${doris_username}" "${use_passwd}")
        check=''
        if [ $m_count -eq $d_count ]; then
            check="${m_d}.${m_t}: ${m_count} ==> ${d_count}, ok"
        else
            check="${m_d}.${m_t}: ${m_count} ==> ${d_count}, check failed"
        fi
        echo "${check}" >> $path
done
exit $(grep "check failed" "${path}" | wc -l)