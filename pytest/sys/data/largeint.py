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
the data set for test largeint on palo
Date: 2015/05/28 17:24:39
"""
import sys
sys.path.append("../")
from lib import palo_client
from lib import palo_config

#create
column_list = [("tinyint_key", "tinyint"), ("smallint_key", "smallint"), \
        ("int_key", "int", None, "0"), ("bigint_key", "bigint"), ("largeint_key", "LARGEINT"), \
        ("char_50_key", "char(50)"), ("varchar_key", "varchar(500)"), \
        ("char_key", "char"), ("varchar_most_key", "varchar(65533)"), \
        ("decimal_key", "decimal(20, 6)"), ("decimal_most_key", "decimal(27, 9)"), \
        ("date_key", "date"), ("datetime_key", "datetime"), \
        ("tinyint_value", "tinyint", "sum"), ("smallint_value", "smallint", "sum"), \
        ("int_value", "int", "sum"), ("bigint_value", "bigint", "sum"), \
        ("char_50_value", "char(50)", "replace"), \
        ("varchar_value", "varchar(500)", "replace", ""), \
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
        ("double_value", "double", "sum"), \
        ("largeint_value", "LARGEINT", "SUM") 
    ]
column_name_list = [column[0] for column in column_list]
distribution_info = palo_client.DistributionInfo("hash(tinyint_key, largeint_key)", 100)

base_column_list = [("k1", "int"), ("k2", "largeint"), ("v", "largeint", "sum")]
base_distribution_info = palo_client.DistributionInfo("hash(k2)", 20)

int_column_list = [("k1", "int"), ("k2", "largeint"), \
        ("k3", "int", None, "0"), ("v", "largeint", "sum")]
int_column_name_list = ["k1", "k2", "v"]

char_column_list = [("k1", "char(50)"), ("k2", "largeint"), ("k3", "char(50)"), \
        ("v", "largeint", "sum")]
char_column_name_list = [column[0] for column in char_column_list]

datetime_column_list = [("k1", "DATETIME"), ("k2", "LARGEINT"), ("k3", "DATETIME"), \
        ("v", "largeint", "sum")]
datetime_column_name_list = [column[0] for column in datetime_column_list]
decimal_column_list = [("k1", "DECIMAL(20, 8)"), ("k2", "LARGEINT"), ("k3", "DECIMAL(20, 8)"), \
        ("v", "LARGEINT", "sum")]
decimal_column_name_list = [column[0] for column in decimal_column_list]

default_value_column_list = [("k1", "INT"), ("k2", "LARGEINT", None, "0"), \
        ("v", "LARGEINT", "SUM", str(2 ** 127 - 1))]
default_value_column_name_list = ["k1"]
default_value_distribution_info = palo_client.DistributionInfo("hash(k1, k2)", 100)

overflow_column_list = [("k1", "INT"), ("k2", "LARGEINT", None, "0"), \
        ("v", "LARGEINT", "SUM", str(2 ** 127 - 1))]
overflow_column_name_list = ["k1"]
overflow_distribution_info = palo_client.DistributionInfo("hash(k2)", 100)
overflow_new_column_name_list = ["k2", "v"]

add_column_list = [("k2", "largeint", None, "0")]
order_column_name_list = ["k2", "k1", "v"]

partition_info = palo_client.PartitionInfo("k2", \
        ["less_010", "less_0", "less_100", "max_partition"], \
        ["-10", "0", "100", "MAXVALUE"])

#batch
hdfs_path = palo_config.gen_remote_file_path('sys/largeint')

base_file_path = "%s/base.txt" % hdfs_path 
base_expected_file = "./data/largeint/base.expected"
int_expected_file = "./data/largeint/int.expected"
char_file_path = "%s/char_largeint_char_largeint.txt" % hdfs_path 
char_expected_file = "./data/largeint/char_largeint_char_largeint.expected" 
datetime_file_path = "%s/datetime_largeint_datetime_largeint.txt" % hdfs_path 
datetime_expected_file = "./data/largeint/datetime_largeint_datetime_largeint.expected"
decimal_file_path = "%s/decimal_largeint_decimal_largeint.txt" % hdfs_path 
decimal_expected_file = "./data/largeint/decimal_largeint_decimal_largeint.expected"
all_type_file_path = "%s/all_type.txt" % hdfs_path
all_type_expected_file = "./data/largeint/all_type.expected"
default_value_file_path = "%s/default_value.txt" % hdfs_path
default_value_expected_file = "./data/largeint/default_value.expected" 

local_base_file_path = "./data/largeint/base.txt"
local_datetime_file_path = "./data/largeint/datetime_largeint_datetime_largeint.txt"
local_all_type_file_path = "./data/largeint/all_type.txt"
local_default_value_file_path = "./data/largeint/default_value.txt"
local_overflow_file_path = "./data/largeint/overflow.txt"
recovery_expected_file = "./data/largeint/recovery.expected"

add_column_expected_file = "./data/largeint/add_column.expected"
order_column_expected_file = "./data/largeint/order_column.expected"
