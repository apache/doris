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
source ../conf/mysql.conf
source ../conf/doris.conf

#mkdir files to store tables and tables.sql
mkdir -p ../result

#The default path is ../result/mysql_to_doris.sql for create table sql
path=${1:-../result/mysql_to_doris.sql}

#delete sql file if it is exists
rm -f $path


#get create table sql for mysql
for table in $(cat ../conf/mysql_tables |grep -v '#' | awk -F '\n' '{print $1}')
        do
        d_d=$(echo $table |awk -F '.' '{print $1}')
        d_t=$(echo $table |awk -F '.' '{print $2}')
        echo "show create table \`$d_d\`.\`$d_t\`;" |mysql -h$mysql_host -uroot -p$mysql_password  >> $path
done

#adjust sql
awk -F '\t' '{print $2}' $path |awk '!(NR%2)' |awk '{print $0 ";"}' > ../result/tmp111.sql
sed -i 's/\\n/\n/g' ../result/tmp111.sql
sed -n '/CREATE TABLE/,/ENGINE\=/p' ../result/tmp111.sql > ../result/tmp222.sql



#delete tables special struct
sed -i '/^  CON/d' ../result/tmp222.sql
sed -i '/^  KEY/d' ../result/tmp222.sql
rm -rf $path
rm -rf ../result/tmp111.sql
mv ../result/tmp222.sql $path

#start transform tables struct
sed -i '/ENGINE=/a) ENGINE=ODBC\n COMMENT "ODBC"\nPROPERTIES (\n"host" = "ApacheDorisHostIp",\n"port" = "3306",\n"user" = "root",\n"password" = "ApacheDorisHostPassword",\n"database" = "ApacheDorisDataBases",\n"table" = "ApacheDorisTables",\n"driver" = "MySQL",\n"odbc_type" = "mysql");' $path
sed -i "s/\"driver\" = \"MySQL\"/\"driver\" = \"$doris_odbc_name\"/g" $path


#delete match line
sed -i '/PRIMARY KEY/d' $path
sed -i '/UNIQUE KEY/d' $path
#delete , at the beginning (
sed -i '/,\s*$/{:loop; N; /,\(\s*\|\n\))/! bloop; s/,\s*[\n]\?\s*)/\n)/}' $path

#delete a line on keyword
sed -i -e '$!N;/\n.*ENGINE=ODBC/!P;D' $path
#replace mysql password、database、table、host




for t_name in $(cat ../conf/mysql_tables |grep -v '#' | awk -F '\n' '{print $1}')
        do
        d=`echo $t_name | awk -F '.' '{print $1}'`
        t=`echo $t_name | awk -F '.' '{print $2}'`
        sed -i "0,/ApacheDorisHostIp/s/ApacheDorisHostIp/${mysql_host}/" $path
        sed -i "0,/ApacheDorisHostPassword/s/ApacheDorisHostPassword/${mysql_password}/" $path
        sed -i "0,/ApacheDorisDataBases/s/ApacheDorisDataBases/$d/" $path
        sed -i "0,/ApacheDorisTables/s/ApacheDorisTables/$t/" $path
done



#do transfrom from mysql to doris external
sh ../lib/mysql_to_doris.sh $path

#get an orderly table name and add if not exists statement
x=0
for table in $(cat ../conf/doris_tables |grep -v '#' | awk -F '\n' '{print $1}')
        do
        let x++
        d_t=`cat ../conf/mysql_tables |grep -v '#' | awk "NR==$x{print}" |awk -F '.' '{print $2}'`
        sed -i "s/TABLE \`$d_t\`/TABLE if not exists $table/g" $path
done

#create database
for d_doris in $(cat ../conf/doris_tables |grep -v '#' | awk -F '\n' '{print $1}' |awk -F '.' '{print $1}' |sort -u)
do
                sed -i "1icreate database if not exists $d_doris;" $path
done
