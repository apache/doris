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

cur_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
home_dir=$(cd "${cur_dir}"/.. && pwd)

source ${home_dir}/conf/env.conf

# when fe_password is not set or is empty, do not put -p option
use_passwd=$([ -z "${doris_password}" ] && echo "" || echo "-p${doris_password}")

#Execute the create external table sql
echo "source ${home_dir}/result/mysql/e_mysql_to_doris.sql;" | mysql -h$fe_master_host -P$fe_master_port -u$doris_username "${use_passwd}" 2>>e_auto_error.log

#Circulation monitoring mysql or conf
while ((1 == 1)); do

    #Monitor interval
    sleep 30

    #get new create table sql
    sh ${home_dir}/lib/e_mysql_to_doris.sh ${home_dir}/result/mysql/new_e_mysql_to_doris.sql 2>>e_auto_error.log

    #get a md5 from old create table sql
    old=$(md5sum ${home_dir}/result/mysql/e_mysql_to_doris.sql | awk -F ' ' '{print $1}')

    #get a md5 from new create table sql
    new=$(md5sum ${home_dir}/result/mysql/new_e_mysql_to_doris.sql | awk -F ' ' '{print $1}')

    if [[ $old != $new ]]; then
        #table charges to drop old table and create new table
        for table in $(cat ${home_dir}/conf/doris_external_tables | grep -v '#' | awk -F '\n' '{print $1}'); do
            echo "DROP TABLE IF EXISTS ${table};" | mysql -h$fe_master_host -P$fe_master_port -u$doris_username "${use_passwd}" 2>>e_auto_error.log
        done
        echo "source ${home_dir}/result/mysql/new_e_mysql_to_doris.sql;" | mysql -h$fe_master_host -P$fe_master_port -u$doris_username "${use_passwd}" 2>>e_auto_error.log
        #delete old create table
        rm -rf ${home_dir}/result/mysql/e_mysql_to_doris.sql
        #alter new table sql name
        mv ${home_dir}/result/mysql/new_e_mysql_to_doris.sql ${home_dir}/result/mysql/e_mysql_to_doris.sql
    fi
    #if table no charge delete new create table
    rm -f ${home_dir}/result/mysql/new_e_mysql_to_doris.sql
done
