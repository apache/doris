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
the data set for test master on palo
Date: 2015/05/14 17:37:21
"""
import sys
sys.path.append("../")
from lib import palo_client
from lib import palo_config

#create
column_list = [("tinyint_key", "tinyint"), ("smallint_key", "smallint"), \
        ("int_key", "int"), ("bigint_key", "bigint"), \
        ("char_50_key", "char(50)"), ("varchar_key", "varchar(500)"), \
        ("char_key", "char"), ("varchar_most_key", "varchar(65533)"), \
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

storage_type = "column"
partition_info = palo_client.PartitionInfo("tinyint_key", \
        ["less_n_100", "less_n_10", "less_0", "less_10", "less_100", "less_max"], \
        ["-100", "-10", "0", "10", "100", "MAXVALUE"])

distribution_info = palo_client.DistributionInfo("hash(tinyint_key, int_key)", 100)

random_partition_type="random"
random_partition_num = 13
#为增加随机性，对push/delete相关的case建立较多的tablet
push_partition_num = 103

range_partition_type = "range(tinyint_key, int_key)"
range_list = [("-1", "-4", )]

hash_partition_type = "hash(tinyint_key, int_key)"
hash_partition_num = 15

#load
file_path = palo_config.gen_remote_file_path('/all_type.txt')
local_file_path = "./data/all_type.txt"

file_path_list = list()
file_path_list.append(file_path)
column_name_list = [column[0] for column in column_list]
load_data_list = list()
load_data = palo_client.LoadDataInfo(file_path_list, None, column_name_list)
load_data_list.append(load_data)
expected_data_files = "./data/all_type_834"

#delete
delete_condition_list = [("tinyint_key", "=", "1")]
delete_conditions_list = [[("tinyint_key", "=", "1")], \
        [("tinyint_key", "=", "2")], \
        [("tinyint_key", "=", "3")], \
        [("tinyint_key", "=", "4")], \
        [("tinyint_key", "=", "5")], \
        [("tinyint_key", "=", "6")], \
        [("tinyint_key", "=", "7")], \
        [("tinyint_key", "=", "8")], \
        [("tinyint_key", "=", "9")]]

#rollup
rollup_column_name_list = ["tinyint_key", "int_key", "char_value", "tinyint_value"]

#schema change
drop_column_name_list = ["decimal_key", "bigint_value", "datetime_value_min"]
