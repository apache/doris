#!/bin/env python
# -*- coding: utf-8 -*-
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

"""
the data set for test load on palo
Date: 2015/03/25 11:48:41
"""
import sys
sys.path.append("../")
from lib import palo_client

#create
column_list = [("tinyint_key", "tinyint"), ("smallint_key", "smallint"), \
        ("int_key", "int"), ("bigint_key", "bigint"), \
        ("char_50_key", "char(50)"), ("varchar_key", "varchar(500)"), \
        ("char_key", "char"), ("varchar_most_key", "varchar(165533)"), \
        ("decimal_key", "decimal(20, 6)"), ("decimal_most_key", "decimal(27, 9)"), \
        ("date_key", "date"), ("datetime_key", "datetime"), \
        ("tinyint_value", "tinyint", "sum"), ("smallint_value", "smallint", "sum"), \
        ("int_value", "int", "sum"), ("bigint_value", "bigint", "sum"), \
        ("char_50_value", "char(50)", "replace"), \
        ("varchar_value", "varchar(500)", "replace"), \
        ("char_value", "char", "replace"), \
        ("varchar_most_value", "varchar(65533)", "replace"), \
        ("decimal_value", "decimal(20, 6)", "sum"), \
        ("decimal_most_value", "decimal(27, 9)", "sum"), \
        ("date_value_max", "date", "max"), \
        ("date_value_replace", "date", "replace"), \
        ("date_value_min", "date", "min"), \
        ("datetime_value_max", "datetime", "max"), \
        ("datetime_value_replace", "datetime", "replace"), \
        ("datetime_value_min", "datetime", "min"), \
        ("float_value", "float", "sum"), \
        ("double_value", "double", "sum")
    ]

random_distribution_info = palo_client.DistributionInfo("RANDOM", 15)
hash_distribution_info = palo_client.DistributionInfo("HASH(k1)", 15)

range_info = "PARTITION BY RANGE (k1)\
              (\
	          PARTITION p1 VALUES LESS THAN (\"0\"),\
	          PARTITION p2 VALUES LESS THAN (\"60\"),\
	          PARTITION p3 VALUES LESS THAN MAXVALUE\
	      )\
	      DISTRIBUTED BY HASH(k1) BUCKETS 32"

date_range_info = "PARTITION BY RANGE (k1)\
              (\
	          PARTITION p1 VALUES LESS THAN (\"2000-01-01\"),\
	          PARTITION p2 VALUES LESS THAN (\"2011-07-01\"),\
	          PARTITION p3 VALUES LESS THAN MAXVALUE\
	      )\
	      DISTRIBUTED BY HASH(k1) BUCKETS 32"

datetime_range_info = "PARTITION BY RANGE (k1)\
              (\
	      PARTITION p1 VALUES LESS THAN (\"2000-01-01 00:00:00\"),\
	      PARTITION p2 VALUES LESS THAN (\"2011-07-01 12:30:01\"),\
	          PARTITION p3 VALUES LESS THAN MAXVALUE\
	      )\
	      DISTRIBUTED BY HASH(k1) BUCKETS 32"

hash_partition_type = "hash(k1)"
#hash_partition_num = 15
#hash_partition_info = palo_client.PartitionInfo(partition_type = hash_partition_type, \
#	partition_num = hash_partition_num)
tinyint_column_list = [("k1", "TINYINT"), \
        ("v1", "TINYINT", "SUM"), \
        ("v2", "TINYINT", "MAX"), \
        ("v3", "TINYINT", "MIN"), \
        ("v4", "TINYINT", "REPLACE")]

hash_partition_type = "hash(k1)"
#hash_partition_num = 15
#hash_partition_info = palo_client.PartitionInfo(partition_type = hash_partition_type, \
#	partition_num = hash_partition_num)
tinyint_column_list = [("k1", "TINYINT"), \
        ("v1", "TINYINT", "SUM"), \
        ("v2", "TINYINT", "MAX"), \
        ("v3", "TINYINT", "MIN"), \
        ("v4", "TINYINT", "REPLACE")]

hash_partition_type = "hash(k1)"
#hash_partition_num = 15
#hash_partition_info = palo_client.PartitionInfo(partition_type = hash_partition_type, \
#	partition_num = hash_partition_num)
tinyint_column_list = [("k1", "TINYINT"), \
        ("v1", "TINYINT", "SUM"), \
        ("v2", "TINYINT", "MAX"), \
        ("v3", "TINYINT", "MIN"), \
        ("v4", "TINYINT", "REPLACE")]

tinyint_partition_info = palo_client.PartitionInfo("k1", \
        ["p1", "p2", "p3", "p4", "p5"], \
        ["-10", "0", "10", "100", "MAXVALUE"])

smallint_column_list = [("k1", "SMALLINT"), \
        ("v1", "SMALLINT", "SUM"), \
        ("v2", "SMALLINT", "MAX"), \
        ("v3", "SMALLINT", "MIN"), \
        ("v4", "SMALLINT", "REPLACE")]

int_column_list = [("k1", "INT"), \
        ("v1", "INT", "SUM"), \
        ("v2", "INT", "MAX"), \
        ("v3", "INT", "MIN"), \
        ("v4", "INT", "REPLACE")]

bigint_column_list = [("k1", "BIGINT"), \
        ("v1", "BIGINT", "SUM"), \
        ("v2", "BIGINT", "MAX"), \
        ("v3", "BIGINT", "MIN"), \
        ("v4", "BIGINT", "REPLACE")]

largeint_column_list = [("k1", "LARGEINT"), \
        ("v1", "LARGEINT", "SUM"), \
        ("v2", "LARGEINT", "MAX"), \
        ("v3", "LARGEINT", "MIN"), \
        ("v4", "LARGEINT", "REPLACE")]

float_column_list = [("k1", "INT"), \
        ("v1", "FLOAT", "SUM"), \
        ("v2", "FLOAT", "MAX"), \
        ("v3", "FLOAT", "MIN"), \
        ("v4", "FLOAT", "REPLACE")]

double_column_list = [("k1", "INT"), \
        ("v1", "DOUBLE", "SUM"), \
        ("v2", "DOUBLE", "MAX"), \
        ("v3", "DOUBLE", "MIN"), \
        ("v4", "DOUBLE", "REPLACE")]

decimal_least_column_list = [("k1", "DECIMAL(1, 0)"), \
        ("v1", "DECIMAL(1, 0)", "SUM"), \
        ("v2", "DECIMAL(1, 0)", "MAX"), \
        ("v3", "DECIMAL(1, 0)", "MIN"), \
        ("v4", "DECIMAL(1, 0)", "REPLACE")]

decimal_normal_column_list = [("k1", "DECIMAL(10, 5)"), \
        ("v1", "DECIMAL(10, 5)", "SUM"), \
        ("v2", "DECIMAL(10, 5)", "MAX"), \
        ("v3", "DECIMAL(10, 5)", "MIN"), \
        ("v4", "DECIMAL(10, 5)", "REPLACE")]

decimal_most_column_list = [("k1", "DECIMAL(27, 9)"), \
        ("v1", "DECIMAL(27, 9)", "SUM"), \
        ("v2", "DECIMAL(27, 9)", "MAX"), \
        ("v3", "DECIMAL(27, 9)", "MIN"), \
        ("v4", "DECIMAL(27, 9)", "REPLACE")]

date_column_list = [("k1", "DATE"), \
        ("v1", "DATE", "REPLACE"), \
        ("v2", "DATE", "MAX"), \
        ("v3", "DATE", "MIN")]

datetime_column_list = [("k1", "DATETIME"), \
        ("v1", "DATETIME", "REPLACE"), \
        ("v2", "DATETIME", "MAX"), \
        ("v3", "DATETIME", "MIN")]
        #("v2", "DATE", "MAX"), \
        #("v3", "DATE", "MIN")]

char_least_column_list = [("k1", "CHAR"), \
        ("v1", "CHAR", "REPLACE")]

char_normal_column_list = [("k1", "CHAR(20)"), \
        ("v1", "CHAR(20)", "REPLACE")]

char_most_column_list = [("k1", "CHAR(255)"), \
        ("v1", "CHAR(255)", "REPLACE")]

varchar_least_column_list = [("k1", "VARCHAR(1)"), \
        ("v1", "VARCHAR(1)", "REPLACE")]

varchar_normal_column_list = [("k1", "VARCHAR(200)"), \
        ("v1", "VARCHAR(200)", "REPLACE")]

varchar_most_column_list = [("k1", "VARCHAR(65533)"), \
        ("v1", "VARCHAR(65533)", "REPLACE")]

storage_type = "column"

