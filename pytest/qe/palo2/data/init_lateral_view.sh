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

export hdfs_path=${hdfs_data_path}/qe/lateral_view

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "DROP DATABASE IF EXISTS test_lateral_view"
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "create database test_lateral_view"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "create table test_lateral_view.lateral_view_data(k1 int, k2 int, k3 int, k4 varchar(500), k5 varchar(500), k6 varchar(500), k7 varchar(500), k8 varchar(500), k9 bitmap bitmap_union) DISTRIBUTED BY HASH(k1)"
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL test_lateral_view.label_1 (DATA INFILE('${hdfs_path}/lateral_view_data') INTO TABLE lateral_view_data (k1,k2,k3,k4,k5,k6,k7,k8) set (k9=bitmap_from_string(k8))) ${broker_property}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "create table test_lateral_view.lateral_view_check(k1 int, k2 int, k3 int, k4 varchar(50), k5 varchar(50), k6 bigint, k7 double, k8 bigint) DISTRIBUTED BY HASH(k1)"
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL test_lateral_view.label_2 (DATA INFILE('${hdfs_path}/lateral_view_check') INTO TABLE lateral_view_check (k1,k2,k3,k4,k5,k6,k7,k8)) ${broker_property}"
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "create table test_lateral_view.test_aggregate_key(k1 int, k2 int, k3 int, k4 varchar(500), k5 varchar(500), k6 varchar(500), k7 varchar(500), k8 varchar(500), k9 bitmap bitmap_union) AGGREGATE KEY(k1,k2,k3,k4,k5,k6,k7,k8) DISTRIBUTED BY HASH(k1)"
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL test_lateral_view.label_3 (DATA INFILE('${hdfs_path}/lateral_view_data') INTO TABLE test_aggregate_key (k1,k2,k3,k4,k5,k6,k7,k8) set (k9=bitmap_from_string(k8))) ${broker_property}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "create table test_lateral_view.test_aggregate_key_bucket(k1 int, k2 int, k3 int, k4 varchar(500), k5 varchar(500), k6 varchar(500), k7 varchar(500), k8 varchar(500), k9 bitmap bitmap_union) AGGREGATE KEY(k1,k2,k3,k4,k5,k6,k7,k8) DISTRIBUTED BY HASH(k1,k2,k3,k4,k5,k6,k7,k8)"
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL test_lateral_view.label_4 (DATA INFILE('${hdfs_path}/lateral_view_data') INTO TABLE test_aggregate_key_bucket (k1,k2,k3,k4,k5,k6,k7,k8) set (k9=bitmap_from_string(k8))) ${broker_property}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "create table test_lateral_view.test_aggregate_value_replace(k1 int, k2 int, k3 int, k4 varchar(500) replace, k5 varchar(500) replace, k6 varchar(500) replace, k7 varchar(500) replace, k8 varchar(500) replace, k9 bitmap bitmap_union) AGGREGATE KEY(k1,k2,k3) DISTRIBUTED BY HASH(k1)"
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL test_lateral_view.label_5 (DATA INFILE('${hdfs_path}/lateral_view_data') INTO TABLE test_aggregate_value_replace (k1,k2,k3,k4,k5,k6,k7,k8) set (k9=bitmap_from_string(k8))) ${broker_property}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "create table test_lateral_view.test_aggregate_value(k1 int, k2 int, k3 int, k4 varchar(500) max, k5 varchar(500) min, k6 varchar(500) max, k7 varchar(500) max, k8 varchar(500) min, k9 bitmap bitmap_union) AGGREGATE KEY(k1,k2,k3) DISTRIBUTED BY HASH(k1)"
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL test_lateral_view.label_6 (DATA INFILE('${hdfs_path}/lateral_view_data') INTO TABLE test_aggregate_value (k1,k2,k3,k4,k5,k6,k7,k8) set (k9=bitmap_from_string(k8))) ${broker_property}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "create table test_lateral_view.test_unique_key(k1 int, k2 int, k3 int, k4 varchar(500), k5 varchar(500), k6 varchar(500), k7 varchar(500), k8 varchar(500), k9 varchar(500)) UNIQUE KEY(k1,k2,k3,k4,k5,k6,k7,k8) DISTRIBUTED BY HASH(k1)"
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL test_lateral_view.label_7 (DATA INFILE('${hdfs_path}/lateral_view_data') INTO TABLE test_unique_key (k1,k2,k3,k4,k5,k6,k7,k8) set (k9=k8)) ${broker_property}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "create table test_lateral_view.test_unique_key_bucket(k1 int, k2 int, k3 int, k4 varchar(500), k5 varchar(500), k6 varchar(500), k7 varchar(500), k8 varchar(500), k9 varchar(500)) UNIQUE KEY(k1,k2,k3,k4,k5,k6,k7,k8) DISTRIBUTED BY HASH(k1,k2,k3,k4,k5,k6,k7,k8)"
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL test_lateral_view.label_8 (DATA INFILE('${hdfs_path}/lateral_view_data') INTO TABLE test_unique_key_bucket (k1,k2,k3,k4,k5,k6,k7,k8) set (k9=k8)) ${broker_property}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "create table test_lateral_view.test_unique_value(k1 int, k2 int, k3 int, k4 varchar(500), k5 varchar(500), k6 varchar(500), k7 varchar(500), k8 varchar(500), k9 varchar(500)) UNIQUE KEY(k1,k2,k3) DISTRIBUTED BY HASH(k1)"
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL test_lateral_view.label_9 (DATA INFILE('${hdfs_path}/lateral_view_data') INTO TABLE test_unique_value (k1,k2,k3,k4,k5,k6,k7,k8) set (k9=k8)) ${broker_property}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "create table test_lateral_view.test_duplicate_key(k1 int, k2 int, k3 int, k4 varchar(500), k5 varchar(500), k6 varchar(500), k7 varchar(500), k8 varchar(500), k9 varchar(500)) DUPLICATE KEY(k1,k2,k3,k4,k5,k6,k7,k8) DISTRIBUTED BY HASH(k1)"
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL test_lateral_view.label_10 (DATA INFILE('${hdfs_path}/lateral_view_data') INTO TABLE test_duplicate_key (k1,k2,k3,k4,k5,k6,k7,k8) set (k9=k8)) ${broker_property}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "create table test_lateral_view.test_duplicate_key_bucket(k1 int, k2 int, k3 int, k4 varchar(500), k5 varchar(500), k6 varchar(500), k7 varchar(500), k8 varchar(500), k9 varchar(500)) DUPLICATE KEY(k1,k2,k3,k4,k5,k6,k7,k8) DISTRIBUTED BY HASH(k1,k2,k3,k4,k5,k6,k7,k8)"
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL test_lateral_view.label_11 (DATA INFILE('${hdfs_path}/lateral_view_data') INTO TABLE test_duplicate_key_bucket (k1,k2,k3,k4,k5,k6,k7,k8) set (k9=k8)) ${broker_property}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "create table test_lateral_view.test_duplicate_value(k1 int, k2 int, k3 int, k4 varchar(500), k5 varchar(500), k6 varchar(500), k7 varchar(500), k8 varchar(500), k9 varchar(500)) DUPLICATE KEY(k1,k2,k3) DISTRIBUTED BY HASH(k1)"
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL test_lateral_view.label_12 (DATA INFILE('${hdfs_path}/lateral_view_data') INTO TABLE test_duplicate_value (k1,k2,k3,k4,k5,k6,k7,k8) set (k9=k8)) ${broker_property}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e 'create table test_lateral_view.test_range_partition(k1 int, k2 int, k3 int, k4 varchar(500), k5 varchar(500), k6 varchar(500), k7 varchar(500), k8 varchar(500), k9 bitmap bitmap_union) PARTITION BY RANGE(k1) (partition p1 values less than ("-32768"), partition p2 values less than ("0"), partition p3 values less than ("32768"), partition p4 values less than ("65536")) DISTRIBUTED BY HASH(k1)'
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL test_lateral_view.label_13 (DATA INFILE('${hdfs_path}/lateral_view_data') INTO TABLE test_range_partition (k1,k2,k3,k4,k5,k6,k7,k8) set (k9=bitmap_from_string(k8))) ${broker_property}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e 'create table test_lateral_view.test_range_partition_bucket(k1 int, k2 int, k3 int, k4 varchar(500), k5 varchar(500), k6 varchar(500), k7 varchar(500), k8 varchar(500), k9 bitmap bitmap_union) PARTITION BY RANGE(k1) (partition p1 values less than ("-32768"), partition p2 values less than ("0"), partition p3 values less than ("32768"), partition p4 values less than ("65536")) DISTRIBUTED BY HASH(k1,k2,k3,k4,k5,k6,k7,k8)'
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL test_lateral_view.label_14 (DATA INFILE('${hdfs_path}/lateral_view_data') INTO TABLE test_range_partition_bucket (k1,k2,k3,k4,k5,k6,k7,k8) set (k9=bitmap_from_string(k8))) ${broker_property}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e 'create table test_lateral_view.test_list_partition(k1 int, k2 int not null, k3 int, k4 varchar(500), k5 varchar(500), k6 varchar(500), k7 varchar(500), k8 varchar(500), k9 bitmap bitmap_union) PARTITION BY LIST(k2) (partition p1 values in ("1","2"), partition p2 values in ("3","4","5","6"), partition p3 values in ("7","8","9","10"), partition p4 values in ("11","12","13","14","15","16")) DISTRIBUTED BY HASH(k1)'
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL test_lateral_view.label_15 (DATA INFILE('${hdfs_path}/lateral_view_data') INTO TABLE test_list_partition (k1,k2,k3,k4,k5,k6,k7,k8) set (k9=bitmap_from_string(k8))) ${broker_property}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e 'create table test_lateral_view.test_list_partition_bucket(k1 int, k2 int not null, k3 int, k4 varchar(500), k5 varchar(500), k6 varchar(500), k7 varchar(500), k8 varchar(500), k9 bitmap bitmap_union) PARTITION BY LIST(k2) (partition p1 values in ("1","2"), partition p2 values in ("3","4","5","6"), partition p3 values in ("7","8","9","10"), partition p4 values in ("11","12","13","14","15","16")) DISTRIBUTED BY HASH(k1,k2,k3,k4,k5,k6,k7,k8)'
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL test_lateral_view.label_16 (DATA INFILE('${hdfs_path}/lateral_view_data') INTO TABLE test_list_partition_bucket (k1,k2,k3,k4,k5,k6,k7,k8) set (k9=bitmap_from_string(k8))) ${broker_property}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e 'create table test_lateral_view.test_multi_partition(k1 int, k2 int, k3 int, k4 varchar(500), k5 varchar(500), k6 varchar(500), k7 varchar(500), k8 varchar(500), k9 bitmap bitmap_union) PARTITION BY RANGE(k1,k2) (partition p1 values less than ("-32768","2"), partition p2 values less than ("0","4"),partition p3 values less than ("32768","8"),partition p4 values less than ("65536","18")) DISTRIBUTED BY HASH(k1)'
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL test_lateral_view.label_17 (DATA INFILE('${hdfs_path}/lateral_view_data') INTO TABLE test_multi_partition (k1,k2,k3,k4,k5,k6,k7,k8) set (k9=bitmap_from_string(k8))) ${broker_property}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e 'create table test_lateral_view.test_multi_partition_bucket(k1 int, k2 int, k3 int, k4 varchar(500), k5 varchar(500), k6 varchar(500), k7 varchar(500), k8 varchar(500), k9 bitmap bitmap_union) PARTITION BY RANGE(k1,k2) (partition p1 values less than ("-32768","-8"), partition p2 values less than ("0","0"),partition p3 values less than ("32768","8"),partition p4 values less than ("65536","18")) DISTRIBUTED BY HASH(k1,k2,k3,k4,k5,k6,k7,k8)'
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL test_lateral_view.label_18 (DATA INFILE('${hdfs_path}/lateral_view_data') INTO TABLE test_multi_partition_bucket (k1,k2,k3,k4,k5,k6,k7,k8) set (k9=bitmap_from_string(k8))) ${broker_property}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e  'create table test_lateral_view.test_explode_split_type(k1 int, k2 char(255), k3 varchar(500), k4 string) DISTRIBUTED BY HASH(k1)'
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL test_lateral_view.label_19 (DATA INFILE('${hdfs_path}/lateral_view_data') INTO TABLE test_explode_split_type (k1,v2,v3,v4,v5,v6,v7,v8) set (k2=v4,k3=v4,k4=v4)) ${broker_property}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e 'create table test_lateral_view.test_explode_split_type_check(k1 int, k2 varchar(50)) DISTRIBUTED BY HASH(k1)'
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL test_lateral_view.label_20 (DATA INFILE('${hdfs_path}/lateral_view_check') INTO TABLE test_explode_split_type_check (k1,v2,v3,v4,v5,v6,v7,v8) set (k2=v4)) ${broker_property}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e 'create table test_lateral_view.lateral_view_multi_data(k1 int, k2 int, k3 int, k4 varchar(2000), k5 varchar(2000), k6 varchar(2000), k7 varchar(2000), k8 varchar(2000)) DISTRIBUTED BY HASH(k1)'
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL test_lateral_view.label_21 (DATA INFILE('${hdfs_path}/lateral_view_multi_data') INTO TABLE lateral_view_multi_data (k1,k2,k3,k4,k5,k6,k7,k8)) ${broker_property}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e 'create table test_lateral_view.lateral_view_multi_check(k1 int, k2 int, k3 int, k4 varchar(200), k5 bigint, k6 bigint, k7 double) DISTRIBUTED BY HASH(k1)'
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL test_lateral_view.label_22 (DATA INFILE('${hdfs_path}/lateral_view_multi_check') INTO TABLE lateral_view_multi_check (k1,k2,k3,k4,k5,k6,k7)) ${broker_property}"

