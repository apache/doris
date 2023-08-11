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
the data set for test create table on palo
Date: 2015/03/09 15:23:31
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

random_partition_type="random"
random_partition_num = 13
random_partition_info = palo_client.PartitionInfo(partition_type=random_partition_type, \
        partition_num=random_partition_num)
#为增加随机性，对push/delete相关的case建立较多的tablet
push_partition_num = 103

range_partition_type = "range(tinyint_key, int_key)"
range_list=[("-1", "-4", )]
range_partition_info = palo_client.PartitionInfo(partition_type=range_partition_type, \
        range_list=range_list, set_max_partition=True)

hash_partition_type = "hash(tinyint_key, int_key)"
hash_partition_num = 15
hash_partition_info = palo_client.PartitionInfo(partition_type=hash_partition_type, \
        partition_num=hash_partition_num)

datetime_range_type = "range(datetime_key)"
datetime_range_list = [("1992-12-07 12:48:27", )]
datetime_range_info = palo_client.PartitionInfo(partition_type=datetime_range_type, \
        range_list=datetime_range_list, set_max_partition=True)

date_range_type = "range(date_key)"
date_range_list = [("1992-12-07", )]
date_range_info = palo_client.PartitionInfo(partition_type=date_range_type, \
        range_list=date_range_list, set_max_partition=True)


datetime_hash_type = "hash(date_key, datetime_key)"
datetime_hash_num = 13
datetime_hash_info = palo_client.PartitionInfo(partition_type=datetime_hash_type, \
        partition_num=datetime_hash_num)

decimal_range_type = "range(decimal_key)"
decimal_range_list = [("0", ), ("10.01", )]
decimal_range_info = palo_client.PartitionInfo(partition_type=decimal_range_type, \
        range_list=decimal_range_list, set_max_partition=True)

decimal_hash_type = "hash(decimal_key, decimal_most_key)"
decimal_hash_num = 13
decimal_hash_info = palo_client.PartitionInfo(partition_type=decimal_hash_type, \
        partition_num=decimal_hash_num)

column_storage_type = "column"
row_storage_type = "row"

#load

file_path = palo_config.gen_remote_file_path("all_type.txt")
file_path_list = list()
file_path_list.append(file_path)
column_name_list = [column[0] for column in column_list]
load_data_list = list()
load_data = palo_client.LoadDataInfo(file_path_list, None, column_name_list)
load_data_list.append(load_data)
expected_data_file = "./data/all_type_834"

#rollup
rollup_column_name_list = ["tinyint_key", "int_key", "char_value", "tinyint_value"]
