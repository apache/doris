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
change.py
"""
import sys
sys.path.append("../")
from lib import palo_config

hdfs_path = palo_config.hdfs_path

file_path_1 = '%s/load_case_data1.txt' % hdfs_path
file_path_2 = '%s/a_k2_k1_v3_v2_del-v1_add-v4' % hdfs_path

expected_data_file_list_1 = ['./data/CHANGE/a_raw']
expected_data_file_list_2 = ['./data/CHANGE/a_add_col_mid_key']
expected_data_file_list_3 = ['./data/CHANGE/a_add_col_after_key']
expected_data_file_list_4 = ['./data/CHANGE/a_add_col_in_value']
expected_data_file_list_5 = ['./data/CHANGE/a_add_col_v4_v5']
expected_data_file_list_6 = ['./data/CHANGE/a_add_col_v4_v5_v6']
expected_data_file_list_7 = ['./data/CHANGE/a_add_col_v4_v5_v6_v7']
expected_data_file_list_8 = ['./data/CHANGE/a_add_col_k3_v4_v5_v6_v7']
expected_data_file_list_9 = ['./data/CHANGE/a_drop_col_v1']
expected_data_file_list_10 = ['./data/CHANGE/a_drop_col_v1_v2']
expected_data_file_list_11 = ['./data/CHANGE/a_drop_col_k2']
expected_data_file_list_12 = ['./data/CHANGE/a_order_k1_k2_v2_v1_v3']
expected_data_file_list_13 = ['./data/CHANGE/a_order_k1_k2_v3_v2_v1']
expected_data_file_list_14 = ['./data/CHANGE/a_order_k1_k2_v2_v3_v1']
expected_data_file_list_15 = ['./data/CHANGE/a_order_k2_k1_v3_v2_v1']
expected_data_file_list_16 = ['./data/CHANGE/a_k2_k1_v3_v2_del-v1_add-v4']


schema_1 = [('k1', 'int'), \
            ('k2', 'int'), \
            ('v1', 'varchar(4096)', 'replace'), \
            ('v2', 'float', 'sum'), \
            ('v3', 'decimal(20,7)', 'sum')]


add_key_partition_type = 'range(k1)'
add_key_range_list = [("150")]
add_key_set_max_partition = True


