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

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "create table ${FE_DB}.baseall(k1 tinyint, k2 smallint, k3 int, k4 bigint, k5 decimal(9,3), k6 char(5), k10 date, k11 datetime, k7 varchar(20), k8 double, k9 float ) engine=olap duplicate key (k1, k2, k3, k4, k5, k6, k10) distributed by hash(k1) buckets 5 properties('storage_type'='column')"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "create table ${FE_DB}.test(k1 tinyint, k2 smallint, k3 int, k4 bigint, k5 decimal(9,3), k6 char(5), k10 date, k11 datetime, k7 varchar(20), k8 double, k9 float) engine=olap duplicate key (k1, k2, k3, k4, k5, k6, k10) partition by range(k1) (partition p1 values less than ('-64'), partition p2 values less than ('0'), partition p3 values less than ('64'), partition p4 values less than maxvalue ) distributed by hash(k1) buckets 5 properties('storage_type'='column')"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "create table ${FE_DB}.bigtable(k1 tinyint, k2 smallint, k3 int, k4 bigint, k5 decimal(9,3), k6 char(5), k10 date, k11 datetime, k7 varchar(20), k8 double, k9 float) engine=olap duplicate key (k1, k2, k3, k4, k5, k6, k10) distributed by hash(k1) buckets 5 properties('storage_type'='column')"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL ${FE_DB}.label_1 (DATA INFILE('${hdfs_data_path}/qe/baseall.txt') INTO TABLE baseall COLUMNS TERMINATED BY '${separator}') ${broker_property}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL ${FE_DB}.label_2 (DATA INFILE('${hdfs_data_path}/qe/baseall.txt') INTO TABLE test COLUMNS TERMINATED BY '${separator}') ${broker_property}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL ${FE_DB}.label_3 (DATA INFILE('${hdfs_data_path}/qe/xaaa') INTO TABLE test COLUMNS TERMINATED BY '${separator}') ${broker_property}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL ${FE_DB}.label_4 (DATA INFILE('${hdfs_data_path}/qe/baseall.txt') INTO TABLE bigtable COLUMNS TERMINATED BY '${separator}') ${broker_property}"

sleep 20
