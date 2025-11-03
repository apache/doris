#!/bin/sh
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

separator='\t'
# hdfs_data_path='hdfs://xxxxxx'
# broker_property="WITH BROKER 'hdfs' ('username'='root', 'password'='')"


mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "DROP DATABASE IF EXISTS ${FE_DB}"
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "create database ${FE_DB}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "create table ${FE_DB}.baseall(k1 tinyint, k2 smallint, k3 int, k4 bigint, k5 decimal(9,3), k6 char(5), k10 date, k11 datetime, k7 varchar(20), k8 double max, k9 float sum) engine=olap distributed by hash(k1) buckets 5 properties('storage_type'='column')"
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "create table ${FE_DB}.test(k1 tinyint, k2 smallint, k3 int, k4 bigint, k5 decimal(9,3), k6 char(5), k10 date, k11 datetime, k7 varchar(20), k8 double max, k9 float sum) engine=olap partition by range(k1) (partition p1 values less than ('-64'), partition p2 values less than ('0'), partition p3 values less than ('64'), partition p4 values less than maxvalue ) distributed by hash(k1) buckets 5 properties('storage_type'='column')"
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "create table ${FE_DB}.bigtable(k1 tinyint, k2 smallint, k3 int, k4 bigint, k5 decimal(9,3), k6 char(5), k10 date, k11 datetime, k7 varchar(20), k8 double max, k9 float sum) engine=olap distributed by hash(k1) buckets 5 properties('storage_type'='column')"
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "CREATE TABLE ${FE_DB}.all_type_table (tinyint_key tinyint(4) NULL, smallint_key smallint(6) NULL, int_key int(11) NOT NULL, bigint_key bigint(20) NULL, char_50_key char(50) NULL, character_key varchar(500) NOT NULL, char_key char(1) NULL, character_most_key varchar(65533) NULL, decimal_key decimal(20, 6) NULL, decimal_most_key decimal(27, 9) NULL, date_key date NULL, datetime_key datetime NULL, tinyint_value tinyint(4) SUM NULL, smallint_value smallint(6) SUM NULL, int_value int(11) SUM NULL, largeint_value largeint SUM NULL, char_50_value char(50) REPLACE NULL, character_value varchar(500) REPLACE NULL, char_value char(1) REPLACE NULL, character_most_value varchar(65533) REPLACE NULL, decimal_value decimal(20, 6) SUM NULL, decimal_most_value decimal(27, 9) SUM NULL, date_value_replace date REPLACE NULL, date_value_max date REPLACE NULL, date_value_min date REPLACE NULL, datetime_value_replace datetime REPLACE NULL, datetime_value_max datetime REPLACE NULL, datetime_value_min datetime REPLACE NULL, float_value float SUM NULL, double_value double SUM NULL) ENGINE=OLAP DISTRIBUTED BY HASH(tinyint_key) BUCKETS 5 PROPERTIES ('storage_type' = 'COLUMN')"


mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL ${FE_DB}.label_1 (DATA INFILE('${hdfs_data_path}/qe/baseall.txt') INTO TABLE baseall COLUMNS TERMINATED BY '${separator}') ${broker_property}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL ${FE_DB}.label_2 (DATA INFILE('${hdfs_data_path}/qe/baseall.txt') INTO TABLE test COLUMNS TERMINATED BY '${separator}') ${broker_property}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL ${FE_DB}.label_3 (DATA INFILE('${hdfs_data_path}/qe/xaaa') INTO TABLE test COLUMNS TERMINATED BY '${separator}') ${broker_property}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL ${FE_DB}.label_4 (DATA INFILE('${hdfs_data_path}/qe/baseall.txt') INTO TABLE bigtable COLUMNS TERMINATED BY '${separator}') ${broker_property}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL ${FE_DB}.label_5 (DATA INFILE('${hdfs_data_path}/sys/all_type.txt') INTO TABLE all_type_table COLUMNS TERMINATED BY '${separator}') ${broker_property} PROPERTIES('max_filter_ratio'='0.05')"

sleep 20
