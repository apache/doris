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
#!/bin/bash
source ../conf/doris.conf
#Execute the create external table sql
echo "source ../result/mysql_to_doris.sql;" |mysql -h$master_host -P$master_port -u$doris_user -p$doris_password 2>error.log

#Circulation monitoring mysql or conf
while (( 1 == 1 ))
do

#Monitor interval
sleep 30

#get new create table sql
sh ./e_mysql_to_doris.sh ../result/new_mysql_to_doris.sql 2>error.log

#get a md5 from old create table sql
old=`md5sum ../result/mysql_to_doris.sql |awk -F ' ' '{print $1}'`

#get a md5 from new create table sql
new=`md5sum ../result/new_mysql_to_doris.sql |awk -F ' ' '{print $1}'`

        if [[ $old != $new ]];then
#table charges to drop old table and create new table
                for table in $(cat ../conf/doris_tables |grep -v '#' | awk -F '\n' '{print $1}')
                do
                       echo "drop table if exists ${table};" |mysql -h$master_host -P$master_port -u$doris_user -p$doris_password
                done
                echo "source ../result/new_mysql_to_doris.sql;" |mysql -h$master_host -P$master_port -u$doris_user -p$doris_password 2>error.log
#delete old create table
                rm -rf ../result/mysql_to_doris.sql
#alter new table sql name
                mv ../result/new_mysql_to_doris.sql ../result/mysql_to_doris.sql
                fi
#if table no charge delete new create table
        rm -f ../result/new_mysql_to_doris.sql
done
