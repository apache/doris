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
the data set for test schema change on palo
Date: 2015/03/25 11:48:37
"""
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from lib import palo_config
file_dir = os.path.abspath(os.path.dirname(__file__))

schema = [("tinyint_key", "TINYINT"), \
          ("smallint_key", "SMALLINT"), \
          ("int_key", "INT"), \
          ("bigint_key", "BIGINT"), \
          ("char_50_key", "CHAR(50)"), \
          ("character_key", "VARCHAR(500)"), \
          ("char_key", "CHAR"), \
          ("character_most_key", "VARCHAR(65533)"), \
          ("decimal_key", "DECIMAL(20, 6)"), \
          ("decimal_most_key", "DECIMAL(27, 9)"), \
          ("date_key", "DATE"), \
          ("datetime_key", "DATETIME"), \
          ("tinyint_value", "TINYINT", "SUM"), \
          ("smallint_value", "SMALLINT", "SUM"), \
          ("int_value", "int", "SUM"), \
          ("bigint_value", "BIGINT", "SUM"), \
          ("char_50_value", "CHAR(50)", "REPLACE"), \
          ("character_value", "VARCHAR(500)", "REPLACE"), \
          ("char_value", "CHAR", "REPLACE"), \
          ("character_most_value", "VARCHAR(65533)", "REPLACE"), \
          ("decimal_value", "DECIMAL(20, 6)", "SUM"), \
          ("decimal_most_value", "DECIMAL(27, 9)", "SUM"), \
          ("date_value_max", "DATE", "max"), \
          ("date_value_replace", "DATE", "REPLACE"), \
          ("date_value_min", "DATE", "min"), \
          ("datetime_value_max", "DATETIME", "MAX"), \
          ("datetime_value_replace", "DATETIME", "REPLACE"), \
          ("datetime_value_min", "DATETIME", "MIN"), \
          ("float_value", "FLOAT", "SUM"), \
          ("double_value", "DOUBLE", "SUM")]

schema_dup = schema_uniq = [("tinyint_key", "TINYINT"), \
          ("smallint_key", "SMALLINT"), \
          ("int_key", "INT"), \
          ("bigint_key", "BIGINT"), \
          ("char_50_key", "CHAR(50)"), \
          ("character_key", "VARCHAR(500)"), \
          ("char_key", "CHAR"), \
          ("character_most_key", "VARCHAR(65533)"), \
          ("decimal_key", "DECIMAL(20, 6)"), \
          ("decimal_most_key", "DECIMAL(27, 9)"), \
          ("date_key", "DATE"), \
          ("datetime_key", "DATETIME"), \
          ("tinyint_value", "TINYINT"), \
          ("smallint_value", "SMALLINT"), \
          ("int_value", "int"), \
          ("bigint_value", "BIGINT"), \
          ("char_50_value", "CHAR(50)"), \
          ("character_value", "VARCHAR(500)"), \
          ("char_value", "CHAR"), \
          ("character_most_value", "VARCHAR(65533)"), \
          ("decimal_value", "DECIMAL(20, 6)"), \
          ("decimal_most_value", "DECIMAL(27, 9)"), \
          ("date_value_replace", "DATE"), \
          ("date_value_max", "DATE"), \
          ("date_value_min", "DATE"), \
          ("datetime_value_replace", "DATETIME"), \
          ("datetime_value_max", "DATETIME"), \
          ("datetime_value_min", "DATETIME"), \
          ("float_value", "FLOAT"), \
          ("double_value", "DOUBLE")]

key_dup = "DUPLICATE KEY(tinyint_key, smallint_key, int_key, bigint_key, char_50_key," \
          "character_key, char_key, character_most_key, decimal_key, decimal_most_key," \
          "date_key, datetime_key)"
key_uniq = "UNIQUE KEY(tinyint_key, smallint_key, int_key, bigint_key, char_50_key," \
          "character_key, char_key, character_most_key, decimal_key, decimal_most_key," \
          "date_key, datetime_key)"

file_path = palo_config.gen_remote_file_path('sys/all_type.txt')
expected_data_file_list_delete_key_1 = '%s/all_type_834' % file_dir
expected_data_file_list_delete_key_2 = '%s/SCHEMA_CHANGE/del_data' % file_dir
expected_data_file_list_delete_key_2_new = '%s/SCHEMA_CHANGE/del_data_new' % file_dir
expected_data_file_list_delete_key_2_new_agg = '%s/SCHEMA_CHANGE/del_data_new_agg' % file_dir
expected_data_file_list_delete_key_3 = '%s/SCHEMA_CHANGE/del_data_b' % file_dir
expected_data_file_list_delete_key_1_dup = '%s/all_type_834_dup' % file_dir


storage_type = "column"

random_partition_type="random"
random_partition_num = 13
push_partition_num = 103

range_partition_type = "range(tinyint_key, int_key)"
range_list = [("-1", "-4", )]

hash_partition_type = "hash(tinyint_key, int_key)"
hash_partition_num = 15

#load
column_name_list = [column[0] for column in schema]

#delete
delete_condition_list = [("tinyint_key", "=", "1")]
delete_condition_list_2 = [("tinyint_key", "=", "-128")]
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
drop_column_name_list_new = ["bigint_value", "datetime_value_min"]

#START FOR BASIC SCHEMA CHANGE CASES
file_path_1 = palo_config.gen_remote_file_path('sys/schema_change/a_raw')
file_path_2 = palo_config.gen_remote_file_path('sys/schema_change/a_k2_k1_v3_v2_del_v1_add_v4')

data_file_varchar_add_length100 = ['%s/SCHEMA_CHANGE/basemodify100' % file_dir]
expected_data_file_varchar_lenth_change1 = ['%s/SCHEMA_CHANGE/wjbase' % file_dir]
expected_data_file_varchar_lenth_change2 = ['%s/SCHEMA_CHANGE/baseadd100' % file_dir]
expected_data_file_varchar_lenth_change3 = ['%s/SCHEMA_CHANGE/baseadd100_2' % file_dir]

expected_data_file_list_1 = ['%s/SCHEMA_CHANGE/a_raw' % file_dir]
expected_data_file_list_2 = ['%s/SCHEMA_CHANGE/a_add_col_mid_key' % file_dir]
expected_data_file_list_3 = ['%s/SCHEMA_CHANGE/a_add_col_after_key' % file_dir]
expected_data_file_list_4 = ['%s/SCHEMA_CHANGE/a_add_col_in_value' % file_dir]
expected_data_file_list_5 = ['%s/SCHEMA_CHANGE/a_add_col_v4_v5' % file_dir]
expected_data_file_list_6 = ['%s/SCHEMA_CHANGE/a_add_col_v4_v5_v6' % file_dir]
expected_data_file_list_7 = ['%s/SCHEMA_CHANGE/a_add_col_v4_v5_v6_v7' % file_dir]
expected_data_file_list_8 = ['%s/SCHEMA_CHANGE/a_add_col_k3_v4_v5_v6_v7' % file_dir]
expected_data_file_list_9 = ['%s/SCHEMA_CHANGE/a_drop_col_v1' % file_dir]
expected_data_file_list_10 = ['%s/SCHEMA_CHANGE/a_drop_col_v1_v2' % file_dir]
expected_data_file_list_11 = ['%s/SCHEMA_CHANGE/a_drop_col_k2' % file_dir]
expected_data_file_list_12 = ['%s/SCHEMA_CHANGE/a_order_k1_k2_v2_v1_v3' % file_dir]
expected_data_file_list_13 = ['%s/SCHEMA_CHANGE/a_order_k1_k2_v3_v2_v1' % file_dir]
expected_data_file_list_14 = ['%s/SCHEMA_CHANGE/a_order_k1_k2_v2_v3_v1' % file_dir]
expected_data_file_list_15 = ['%s/SCHEMA_CHANGE/a_order_k2_k1_v3_v2_v1' % file_dir]
expected_data_file_list_16 = ['%s/SCHEMA_CHANGE/a_k2_k1_v3_v2_del_v1_add_v4' % file_dir]
expected_data_file_list_17 = ['%s/SCHEMA_CHANGE/all_type_after_delete' % file_dir]
expected_data_file_list_18 = ['%s/SCHEMA_CHANGE/all_type_833' % file_dir]
expected_data_file_list_19 = ['%s/SCHEMA_CHANGE/all_type_after_schema_change' % file_dir]
expected_data_file_list_20 = ['%s/SCHEMA_CHANGE/all_data_after_sc_then_del' % file_dir]
expected_data_file_list_21 = ['%s/SCHEMA_CHANGE/all_data_after_sc_then_delete' % file_dir]

expected_data_file_list_18_dup = ['%s/SCHEMA_CHANGE/all_type_833_dup' % file_dir]
expected_data_file_list_8_agg = ['%s/SCHEMA_CHANGE/a_add_col_k3_v4_v5_v6_v7_agg' % file_dir]
expected_data_file_list_4_agg = ['%s/SCHEMA_CHANGE/a_add_col_in_value_agg' % file_dir]
expected_data_file_list_5_agg = ['%s/SCHEMA_CHANGE/a_add_col_v4_v5_agg' % file_dir]
expected_data_file_list_6_agg = ['%s/SCHEMA_CHANGE/a_add_col_v4_v5_v6_agg' % file_dir]
expected_data_file_list_7_agg = ['%s/SCHEMA_CHANGE/a_add_col_v4_v5_v6_v7_agg' % file_dir]
expected_data_file_list_16_agg = ['%s/SCHEMA_CHANGE/a_k2_k1_v3_v2_del_v1_add_v4_agg' % file_dir]
expected_data_file_list_16_agg_new = ['%s/SCHEMA_CHANGE/a_k2_k1_v3_v2_del_v1_add_v4_agg_new' % file_dir]

schema_1 = [('k1', 'INT'), \
            ('k2', 'INT'), \
            ('v1', 'VARCHAR(4096)', 'REPLACE'), \
            ('v2', 'FLOAT', 'SUM'), \
            ('v3', 'DECIMAL(20,7)', 'SUM')]

schema_1_new = [('k1', 'INT'), \
            ('k2', 'INT'), \
            ('v1', 'VARCHAR(4096)'), \
            ('v2', 'FLOAT', 'SUM'), \
            ('v3', 'DECIMAL(20,7)', 'SUM')]

schema_1_alter_modify_date = [('k1', 'INT'), \
            ('k2', 'VARCHAR(4096)'), \
            ('k3', 'VARCHAR(4096)'), \
            ('v1', 'FLOAT', 'SUM'), \
            ('v2', 'DECIMAL(20,7)', 'SUM')]

schema_1_alter_modify_date_null = [('k1', 'INT'), \
            ('k2', 'VARCHAR(4096)' 'NULL'), \
            ('k3', 'VARCHAR(4096)'), \
            ('v1', 'FLOAT', 'SUM'), \
            ('v2', 'DECIMAL(20,7)', 'SUM')]

schema_1_alter_modify_number = [('k1', 'INT'), \
            ('k2', 'VARCHAR(4096)'), \
            ('k3', 'VARCHAR(4096)'), \
            ('k4', 'VARCHAR(4096)'), \
            ('k5', 'VARCHAR(4096)'), \
            ('k6', 'VARCHAR(4096)'), \
            ('k7', 'VARCHAR(4096)'), \
            ('k8', 'VARCHAR(4096)'), \
            ('k9', 'VARCHAR(4096)'), \
            ('v1', 'FLOAT', 'SUM'), \
            ('v2', 'DECIMAL(20,7)', 'SUM')]

schema_varchar_lenth_change = [('k1', 'INT'), \
	    ('k3', 'VARCHAR(20)'), \
            ('k2', 'INT'), \
	    ('v1', 'VARCHAR(50)', 'REPLACE'), \
            ('v2', 'FLOAT', 'SUM'), \
            ('v3', 'DECIMAL(20,7)', 'SUM')]

schema_1_dup = [('k1', 'INT'), \
            ('k2', 'INT'), \
            ('v1', 'VARCHAR(4096)'), \
            ('v2', 'FLOAT'), \
            ('v3', 'DECIMAL(20,7)')]

schema_1_uniq = [('k1', 'INT'), \
            ('k2', 'INT'), \
            ('v1', 'VARCHAR(4096)'), \
            ('v2', 'FLOAT'), \
            ('v3', 'DECIMAL(20,7)')]

schema_1_new_dup = [('k1', 'INT'), \
            ('k2', 'INT'), \
            ('v1', 'VARCHAR(4096)'), \
            ('v2', 'FLOAT'), \
            ('v3', 'DECIMAL(20,7)')]

schema_1_new_uniq = [('k1', 'INT'), \
            ('k2', 'INT'), \
            ('v1', 'VARCHAR(4096)'), \
            ('v2', 'FLOAT'), \
            ('v3', 'DECIMAL(20,7)')]

key_1_dup = "DUPLICATE KEY(k1,k2)"
key_1_uniq = "UNIQUE KEY(k1,k2)"

add_key_partition_type = 'range(k1)'
add_key_range_list = [("150")]
add_key_set_max_partition = True
#END FOR BASIC SCHEMA CHANGE CASES

schema_2 = [('k1', 'INT'), \
            ('k2', 'INT'), \
            ('k3', 'INT'), \
            ('v1', 'INT', 'SUM'), \
            ('v2', 'INT', 'SUM'), \
            ('v3', 'INT', 'SUM')]

schema_2_dup = schema_2_uniq = [('k1', 'INT'), \
            ('k2', 'INT'), \
            ('k3', 'INT'), \
            ('v1', 'INT'), \
            ('v2', 'INT'), \
            ('v3', 'INT')]

file_path_3 = palo_config.gen_remote_file_path('sys/schema_change/delete_k1')
file_path_4 = palo_config.gen_remote_file_path('sys/schema_change/delete_k1_delta')

rollup_field_list_1 = ['k1', 'v1']
rollup_field_list_2 = ['k1', 'k2', 'v1']
rollup_field_key_dul_1 = "duplicate key(k1)"

