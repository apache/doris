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

#删库
mysql -h "${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" -e "DROP DATABASE IF EXISTS ${MYSQL_DB}"
#建库
mysql -h "${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" -e "CREATE DATABASE ${MYSQL_DB}"
#建表
mysql -h "${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" "${MYSQL_DB}" -e 'CREATE TABLE `baseall` ( `k1` tinyint(4) DEFAULT NULL, `k2` smallint(6) DEFAULT NULL, `k3` int(11) DEFAULT NULL, `k4` bigint(20) DEFAULT NULL, `k5` decimal(9,3) DEFAULT NULL, `k6` char(5) DEFAULT NULL, `k10` date DEFAULT NULL, `k11` datetime DEFAULT NULL, `k7` varchar(20) DEFAULT NULL, `k8` double DEFAULT NULL, `k9` float DEFAULT NULL)'
mysql -h "${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" "${MYSQL_DB}" -e 'CREATE TABLE `bigtable` ( `k1` tinyint(4) DEFAULT NULL, `k2` smallint(6) DEFAULT NULL, `k3` int(11) DEFAULT NULL, `k4` bigint(20) DEFAULT NULL, `k5` decimal(9,3) DEFAULT NULL, `k6` char(5) DEFAULT NULL, `k10` date DEFAULT NULL, `k11` datetime DEFAULT NULL, `k7` varchar(20) DEFAULT NULL, `k8` double DEFAULT NULL, `k9` float DEFAULT NULL)'
mysql -h "${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" "${MYSQL_DB}" -e 'CREATE TABLE `test` ( `k1` tinyint(4) DEFAULT NULL, `k2` smallint(6) DEFAULT NULL, `k3` int(11) DEFAULT NULL, `k4` bigint(20) DEFAULT NULL, `k5` decimal(9,3) DEFAULT NULL, `k6` char(5) DEFAULT NULL, `k10` date DEFAULT NULL, `k11` datetime DEFAULT NULL, `k7` varchar(20) DEFAULT NULL, `k8` double DEFAULT NULL, `k9` float DEFAULT NULL)'
#导数据
mysql -h "${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" "${MYSQL_DB}" -e 'LOAD DATA LOCAL INFILE "../../hdfs/data/qe/baseall.txt" INTO TABLE baseall'
mysql -h "${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" "${MYSQL_DB}" -e 'LOAD DATA LOCAL INFILE "../../hdfs/data/qe/xaaa" INTO TABLE test'
mysql -h "${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" "${MYSQL_DB}" -e 'LOAD DATA LOCAL INFILE "../../hdfs/data/qe/baseall.txt" INTO TABLE test'
mysql -h "${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" "${MYSQL_DB}" -e 'LOAD DATA LOCAL INFILE "../../hdfs/data/qe/baseall.txt" INTO TABLE bigtable'
