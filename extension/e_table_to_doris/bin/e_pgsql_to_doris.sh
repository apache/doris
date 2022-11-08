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

#mkdir files to store tables and tables.sql
mkdir -p ../result/pgsql

#The default path is ../result/mysql_to_doris.sql for create table sql
path=${1:-../result/pgsql/pgsql_to_doris.sql}

#delete sql file if it is exists
rm -f $path

#get create table sql for mysql
for table in $(cat ../conf/pgsql_tables |grep -v '#' | awk -F '\n' '{print $1}' |sed 's/ //g' | sed '/^$/d')
        do
        db=`echo $table| awk -F '.' '{print $1}'`
        dt=`echo $table| awk -F '.' '{print $2}'`
        pg_dump "host=192.168.213.162 port=5432 user=postgres password=root dbname=$db " -s -t $dt | sed -n '/CREATE TABLE/,/);/p' >> $path
done
##start transform tables struct
sed -i '/);/i) ENGINE=ODBC\n COMMENT "ODBC"\nPROPERTIES (\n"host" = "ApacheDorisHostIp",\n"port" = "5432",\n"user" = "postgres",\n"password" = "ApacheDorisHostPassword",\n"database" = "ApacheDorisDataBases",\n"table" = "ApacheDorisTables",\n"driver" = "MySQL",\n"odbc_type" = "postgresql"' $path
sed -i "s/\"driver\" = \"MySQL\"/\"driver\" = \"$pgsql_odbc_name\"/g" $path


#alter conf dir info
for t_name in $(cat ../conf/pgsql_tables |grep -v '#' | awk -F '\n' '{print $1}' |sed 's/ //g' | sed '/^$/d')
        do
        d=`echo $t_name | awk -F '.' '{print $1}'`
        t=`echo $t_name | awk -F '.' '{print $2}'`
        sed -i "0,/ApacheDorisHostIp/s/ApacheDorisHostIp/${pgsql_host}/" $path
        sed -i "0,/ApacheDorisHostPassword/s/ApacheDorisHostPassword/${pgsql_password}/" $path
        sed -i "0,/ApacheDorisDataBases/s/ApacheDorisDataBases/$d/" $path
        sed -i "0,/ApacheDorisTables/s/ApacheDorisTables/$t/" $path
done


#do transfrom from mysql to doris external
sh ../lib/pgsql_to_doris.sh $path


#get an orderly table name and add if not exists statement
x=0
for table in $(cat ../conf/doris_tables |grep -v '#' | awk -F '\n' '{print $1}' |sed 's/ //g' | sed '/^$/d')
        do
        let x++
        d_t=`cat ../conf/pgsql_tables |grep -v '#' | awk "NR==$x{print}" |awk -F '.' '{print $2}' |sed 's/ //g' | sed '/^$/d'`
        sed -i "0,/.*\.$d_t ($/s/.*\.$d_t ($/CREATE TABLE IF NOT EXISTS $table /i" $path
        sed -i "s/$table/$table(/g" $path
done

#create database
for d_doris in $(cat ../conf/doris_tables |grep -v '#' | awk -F '\n' '{print $1}' |awk -F '.' '{print $1}' |sed 's/ //g' | sed '/^$/d'  |sort -u)
do
                sed -i "1icreate database if not exists $d_doris;" $path
done

#delete a , on key
sed -i '/,\s*$/{:loop; N; /,\(\s*\|\n\))/! bloop; s/,\s*[\n]\?\s*)/\n)/}' $path
