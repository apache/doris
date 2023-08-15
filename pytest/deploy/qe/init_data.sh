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

# FE_DB='test_query_qa'

cd ../../hdfs/data/qe/

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "DROP DATABASE IF EXISTS ${FE_DB}"
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "create database ${FE_DB}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" "${FE_DB}" -e 'create table baseall(k1 tinyint, k2 smallint, k3 int, k4 bigint, k5 decimal(9,3), k6 char(5), k10 date, k11 datetime, k7 varchar(20), k8 double max, k9 float sum) engine=olap distributed by hash(k1) buckets 5 properties("storage_type"="column")'
curl --location-trusted -T baseall.txt -u "${FE_USER}":"${FE_PASSWORD}" http://"${FE_HOST}":"${FE_WEB_PORT}"/api/"${FE_DB}"/baseall/_stream_load

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" "${FE_DB}" -e 'create table test(k1 tinyint, k2 smallint, k3 int, k4 bigint, k5 decimal(9,3), k6 char(5), k10 date, k11 datetime, k7 varchar(20), k8 double max, k9 float sum) engine=olap distributed by hash(k1) buckets 5 properties("storage_type"="column")'
curl --location-trusted -T baseall.txt -u "${FE_USER}":"${FE_PASSWORD}" http://"${FE_HOST}":"${FE_WEB_PORT}"/api/"${FE_DB}"/test/_stream_load
curl --location-trusted -T xaaa -u "${FE_USER}":"${FE_PASSWORD}" http://"${FE_HOST}":"${FE_WEB_PORT}"/api/"${FE_DB}"/test/_stream_load

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" "${FE_DB}" -e 'create table bigtable(k1 tinyint, k2 smallint, k3 int, k4 bigint, k5 decimal(9,3), k6 char(5), k10 date, k11 datetime, k7 varchar(20), k8 double max, k9 float sum) engine=olap distributed by hash(k1) buckets 5 properties("storage_type"="column")'
curl --location-trusted -T baseall.txt -u "${FE_USER}":"${FE_PASSWORD}" http://"${FE_HOST}":"${FE_WEB_PORT}"/api/"${FE_DB}"/bigtable/_stream_load

