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


####################################################################
# This script is used to will mysql tables import doris by external and by user to define tables
####################################################################


#reference configuration file
source ./conf/mysql.conf
source ./conf/doris.conf

#define mysql database and doris database
d_mysql=$1
d_doris=$2

#check args
if [ ! -n "$1" ];then
        echo "please check source database"
        exit
fi
if [ ! -n "$2" ];then
        echo "please check sink database"
        exit
fi

#mkdir files to store tables and tables.sql
mkdir -p user_files
rm -rf ./user_files/tables
rm -rf ./user_files/tables.sql

#reference tables to create tables.sql
for table in $(cat ./conf/tables |grep -v '#' | awk -F '\n' '{print $1}')
        do
        sed -i "/${table}view/d" ./conf/tables
        echo "use $d_mysql; show create table ${table};" |mysql -h$mysql_host -uroot -p$mysql_password 2>/dev/null >> ./user_files/tables.sql
        echo "print ${table} sql to tables.sql in the user_file dir"
done

echo '==============================start to transform mysql table for doris extral table======================'

#adjust sql
awk -F '\t' '{print $2}' ./user_files/tables.sql |awk '!(NR%2)' |awk '{print $0 ";"}' > ./user_files/tables1.sql
sed -i 's/\\n/\n/g' ./user_files/tables1.sql
sed -n '/CREATE TABLE/,/ENGINE\=/p' ./user_files/tables1.sql > ./user_files/tables2.sql

#delete tables special struct
sed -i '/^  CON/d' ./user_files/tables2.sql
sed -i '/^  KEY/d' ./user_files/tables2.sql
rm -rf ./user_files/tables.sql
rm -rf ./user_files/tables1.sql
mv ./user_files/tables2.sql ./user_files/tables.sql
#start transform tables struct
sed -i '/ENGINE=/a) ENGINE=MYSQL\n COMMENT "MYSQL"\nPROPERTIES (\n"host" = "ApacheDorisHostIp",\n"port" = "3306",\n"user" = "root",\n"password" = "ApacheDorisHostPassword",\n"database" = "ApacheDorisDataBases",\n"table" = "ApacheDorisTables");' ./user_files/tables.sql

#delete match line
sed -i '/ENGINT=/d' ./user_files/tables.sql
sed -i '/PRIMARY KEY/d' ./user_files/tables.sql
sed -i '/UNIQUE KEY/d' ./user_files/tables.sql
#delete , at the beginning (
sed -i '/,\s*$/{:loop; N; /,\(\s*\|\n\))/! bloop; s/,\s*[\n]\?\s*)/\n)/}' ./user_files/tables.sql

#delete a line on keyword
sed -i -e '$!N;/\n.*ENGINE=MYSQL/!P;D' ./user_files/tables.sql

#replace mysql password、database、table、host
for t_name in $(cat ./conf/tables |grep -v '#' | awk -F '\n' '{print $1}')
        do
        sed -i "0,/ApacheDorisHostIp/s/ApacheDorisHostIp/${mysql_host}/" ./user_files/tables.sql
        sed -i "0,/ApacheDorisHostPassword/s/ApacheDorisHostPassword/${mysql_password}/" ./user_files/tables.sql
        sed -i "0,/ApacheDorisDataBases/s/ApacheDorisDataBases/${d_mysql}/" ./user_files/tables.sql
        sed -i "0,/ApacheDorisTables/s/ApacheDorisTables/${t_name}/" ./user_files/tables.sql

done
#replace mysql type with doris
sed -i 's/\<text\>/varchar(65533)/g' ./user_files/tables.sql
sed -i 's/\<tinyblob\>/varchar(65533)/g' ./user_files/tables.sql
sed -i 's/\<blob\>/varchar(65533)/g' ./user_files/tables.sql
sed -i 's/\<mediumblob\>/varchar(65533)/g' ./user_files/tables.sql
sed -i 's/\<longblob\>/varchar(65533)/g' ./user_files/tables.sql
sed -i 's/\<tinystring\>/varchar(65533)/g' ./user_files/tables.sql
sed -i 's/\<mediumstring\>/varchar(65533)/g' ./user_files/tables.sql
sed -i 's/\<longstring\>/varchar(65533)/g' ./user_files/tables.sql
sed -i 's/\<timestamp\>/datetime/g' ./user_files/tables.sql
sed -i 's/AUTO_INCREMENT//g' ./user_files/tables.sql
sed -i 's/\<unsigned\>//g' ./user_files/tables.sql
sed -i 's/\<zerofill\>//g' ./user_files/tables.sql
sed -i 's/\<json\>/varchar(65533)/g' ./user_files/tables.sql
sed -i 's/enum([^)]*)/varchar(65533)/g' ./user_files/tables.sql
sed -i 's/\<set\>/varchar(65533)/g' ./user_files/tables.sql
sed -i 's/\<bit\>/varchar(65533)/g' ./user_files/tables.sql
sed -i 's/CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci//g'  ./user_files/tables.sql
sed -i 's/CHARACTER SET utf8mb4 COLLATE utf8_general_ci//g' ./user_files/tables.sql
sed -i 's/CHARACTER SET utf8 COLLATE utf8_general_ci//g' ./user_files/tables.sql
sed -i 's/COLLATE utf8mb4_general_ci//g' ./user_files/tables.sql
sed -i 's/COLLATE utf8_general_ci//g'  ./user_files/tables.sql
sed -i 's/DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP//g' ./user_files/tables.sql
sed -i 's/DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP//g' ./user_files/tables.sql
sed -i 's/CHARACTER SET utf8 COLLATE utf8_bin//g' ./user_files/tables.sql
sed -i 's/COLLATE utf8_general_ci//g'  ./user_files/tables.sql
sed -i 's/datetime([0-9])/varchar(65533)/g' ./user_files/tables.sql
sed -i 's/CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci//g' ./user_files/tables.sql
sed -i 's/\<binary\>/varchar(65533)/g' ./user_files/tables.sql
sed -i 's/\<varbinary\>/varchar(65533)/g' ./user_files/tables.sql
sed -i 's/binary([0-9])/varchar(65533)/g' ./user_files/tables.sql
sed -i 's/varbinary([0-9])/varchar(65533)/g' ./user_files/tables.sql
sed -i 's/string([0-9])/varchar(65533)/g' ./user_files/tables.sql
sed -i 's/\<string\>/varchar(65533)/g' ./user_files/tables.sql
sed -i 's/string([0-9])/varchar(65533)/g' ./user_files/tables.sql
sed -i 's/binary([0-9][0-9])/varchar(65533)/g' ./user_files/tables.sql
sed -i 's/varbinary([0-9][0-9])/varchar(65533)/g' ./user_files/tables.sql
sed -i 's/string([0-9][0-9])/varchar(65533)/g' ./user_files/tables.sql
sed -i 's/binary([0-9][0-9][0-9])/varchar(65533)/g' ./user_files/tables.sql
sed -i 's/varbinary([0-9][0-9][0-9])/varchar(65533)/g' ./user_files/tables.sql
sed -i 's/string([0-9][0-9][0-9])/varchar(65533)/g' ./user_files/tables.sql
sed -i 's/CHARACTER SET utf8mb4 COLLATE utf8mb4_bin//g' ./user_files/tables.sql


#######################################
#import doris
for table in $(awk -F '\n' '{print $1}' ./conf/tables)
        do
        echo "use $d_doris; drop table if exists ${table};" |mysql -h$master_host -P$master_port -uroot -p$doris_password 2>/dev/null
done

echo '==========================================start to write database========================================'
echo "create database if not exists $d_doris; use $d_doris; source ./user_files/tables.sql;" |mysql -h$master_host -P$master_port -uroot -p$doris_password
echo '==========================================write database success========================================='
