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

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e 'DROP DATABASE IF EXISTS test_query_qa'
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e 'create database test_query_qa'
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e 'create EXTERNAL table test_query_qa.baseall(k1 tinyint, k2 smallint, k3 int, k4 bigint, k5 decimal(9,3), k6 char(5), k10 date, k11 datetime, k7 varchar(20), k8 double max, k9 float sum) engine=broker properties("broker_name" = "hdfs",  "path" = "hdfs://xxxxxxx/qe/baseall.txt", "column_separator" = "\t") BROKER PROPERTIES ("username"="root", "password"="")'
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e 'create EXTERNAL table test_query_qa.test(k1 tinyint, k2 smallint, k3 int, k4 bigint, k5 decimal(9,3), k6 char(5), k10 date, k11 datetime, k7 varchar(20), k8 double max, k9 float sum) engine=broker properties( "broker_name" = "hdfs",  "path" = "hdfs://xxxxxxxx/qe/baseall.txt, hdfs://xxxxxxxx/qe/xaaa", "column_separator" = "\t") BROKER PROPERTIES ("username"="root", "password"="")'
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e 'create EXTERNAL table test_query_qa.bigtable(k1 tinyint, k2 smallint, k3 int, k4 bigint, k5 decimal(9,3), k6 char(5), k10 date, k11 datetime, k7 varchar(20), k8 double max, k9 float sum) engine=broker properties( "broker_name" = "hdfs",  "path" = "hdfs://xxxxxxxx/qe/baseall.txt", "column_separator" = "\t") BROKER PROPERTIES ("username"="root", "password"="")'
