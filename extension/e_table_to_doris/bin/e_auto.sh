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
source ../conf/e_table.conf

#input arg in (mysql pgsql oracle hive es)
type=$1

case $1 in
        mysql)
                echo "start to executor mysql tables to doris";;
        pgsql)
                echo "start to executor pgsql tables to doris";;
        oracle)
                echo "start to executor oracle tables to doris";;
        hive)
                echo "start to executor hive tables to doris";;
        es)
                echo "start to executor es tables to doris";;
        *)
        echo "please check input"
        exit;;
esac

#create doris external table statements
sh e_${type}_to_doris.sh

#Execute the create external table sql
echo "source ../result/${type}/${type}_to_doris.sql;" |mysql -h$master_host -P$master_port -uroot -p$doris_password

#Circulation monitoring pgsql or conf
while (( 1 == 1 ))
do

#Monitor interval
sleep 30

#get a md5 from old create table sql
old=`md5sum ../result/$type/${type}_to_doris.sql |awk -F ' ' '{print $1}'`

#get new create table sql
sh ./e_${type}_to_doris.sh ../result/$type/new_${type}_to_doris.sql

#get a md5 from new create table sql
new=`md5sum ../result/$type/new_${type}_to_doris.sql |awk -F ' ' '{print $1}'`

        if [[ $old != $new ]];then
#table charges to drop old table and create new table
                for table in $(cat ../conf/doris_tables |grep -v '#' | awk -F '\n' '{print $1}' |sed 's/ //g' | sed '/^$/d')

                do
                       echo "drop table if exists ${table};" |mysql -h$master_host -P$master_port -uroot -p$doris_password
                done

                echo "source ../result/$type/new_${type}_to_doris.sql;" |mysql -h$master_host -P$master_port -uroot -p$doris_password
#delete old create table
                rm -rf ../result/$type/${type}_to_doris.sql

#alter new table sql name
                mv ../result/$type/new_${type}_to_doris.sql ../result/$type/${type}_to_doris.sql

                fi
#if table no charge delete new create table
        rm -rf ../result/$type/new_${type}_to_doris.sql
done

