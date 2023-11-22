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
"""load files"""

import sys
import os
sys.path.append('../')
from lib import palo_config

file_dir = os.path.abspath(os.path.dirname(__file__))

all_type_local_file = '%s/all_type.txt' % file_dir
all_type_hdfs_file = palo_config.gen_remote_file_path('sys/all_type.txt')

partition_local_file = '%s/PARTITION/partition_type' % file_dir
partition_hdfs_file = palo_config.gen_remote_file_path('sys/partition/partition_type')
partition_local_json_file = '%s/LOAD/partition_type.json' % file_dir
# json object
partition_local_json_object_file = '%s/LOAD/json_object_basic.json' % file_dir

baseall_local_file = '%s/../../hdfs/data/qe/baseall.txt' % file_dir
baseall_hdfs_file = palo_config.gen_remote_file_path('qe/baseall.txt')
test_local_file = '%s/../../hdfs/data/qe/xaaa' % file_dir
test_hdfs_file = palo_config.gen_remote_file_path('qe/xaaa')

all_null_data_local_file = '%s/../../hdfs/data/sys/null/data_all_null' % file_dir
all_null_data_hdfs_file = palo_config.gen_remote_file_path('sys/null/data_all_null')

parse_hdfs_file_path_empty = palo_config.gen_remote_file_path('sys/broker_load/k1=-1/k2=0/city=/partition_type')
parse_hdfs_file_path_normal = palo_config.gen_remote_file_path('sys/broker_load/k1=-1/k2=0/city=bj/partition_type')
parse_hdfs_file_path_float = palo_config.gen_remote_file_path('sys/broker_load/k1=-1/k2=0/k5=100.12345/partition_type')

export_to_hdfs_path = palo_config.gen_remote_file_path('export')
test_number_local_file = "%s/LOAD/test_number.data" % file_dir
test_number_hdfs_file = palo_config.gen_remote_file_path('sys/load/test_number.data')

test_char_local_file = "%s/LOAD/test_char.data" % file_dir
test_char_hdfs_file = palo_config.gen_remote_file_path('sys/load/test_char.data')

# schema.tinyint_column_list, schema.tinyint_column_no_agg_list
test_tinyint_file = '%s/STREAM_LOAD/test_hash_tinyint.data' % file_dir
test_tinyint_unique_file = '%s/STREAM_LOAD/expe_test_hash_tinyint_uniq.data' % file_dir

# schema.smallint_column_list, schema.smallint_column_no_agg_list
test_smallint_file = '%s/STREAM_LOAD/test_hash_smallint.data' % file_dir
expe_test_smallint_unique_file = '%s/STREAM_LOAD/expe_test_hash_smallint_uniq.data' % file_dir

# schema.int_column_list, schema.int_column_no_agg_list
test_int_file = '%s/STREAM_LOAD/test_hash_int.data' % file_dir
expe_test_int_unique_file = '%s/STREAM_LOAD/expe_test_hash_int_uniq.data' % file_dir

# schema.bigint_column_list, schema.bigint_column_no_agg_list
test_bigint_file = '%s/STREAM_LOAD/test_hash_bigint.data' % file_dir
expe_test_bigint_unique_file = '%s/STREAM_LOAD/expe_test_hash_bigint_uniq.data' % file_dir

# schema.largeint_column_list, schema.largeint_column_no_agg_list
test_largeint_file = '%s/STREAM_LOAD/test_hash_largeint.data' % file_dir
expe_test_largeint_unique_file = '%s/STREAM_LOAD/expe_test_hash_largeint_uniq.data' % file_dir

# schema.char_normal_column_list, schema.char_normal_column_no_agg_list
test_char_normal_file = '%s/STREAM_LOAD/test_hash_char_normal.data' % file_dir
expe_test_char_normal_file = '%s/STREAM_LOAD/expe_test_hash_char_normal.data' % file_dir

# schema.varchar_normal_column_list, schema.varchar_normal_column_no_agg_list
test_varchar_normal_file = '%s/STREAM_LOAD/test_hash_varchar_normal.data' % file_dir
expe_test_varchar_normal_file = '%s/STREAM_LOAD/expe_test_hash_varchar_normal.data' % file_dir

# schema.date_column_list, schema.date_column_no_agg_list
test_date_file = '%s/STREAM_LOAD/test_hash_date.data' % file_dir
expe_test_date_unique_file = '%s/STREAM_LOAD/expe_test_hash_date_uniq.data' % file_dir

# schema.datetime_column_list, schema.datetime_column_no_agg_list
test_datetime_file = '%s/STREAM_LOAD/test_hash_date.data' % file_dir
expe_test_datetime_unique_file = '%s/STREAM_LOAD/expe_test_hash_datetime_uniq.data' % file_dir

# schema.decimal_normal_column_list, schema.decimal_normal_column_no_agg_list
test_decimal_normal_file = '%s/STREAM_LOAD/test_hash_decimal_normal.data' % file_dir
expe_test_decimal_normal_unique_file = '%s/STREAM_LOAD/expe_test_decimal_normal_uniq.data' % file_dir

# schema.double_int_column_list, schema.double_int_column_no_agg_list
test_double_int_file = '%s/STREAM_LOAD/test_double_int.data' % file_dir
expe_test_double_int_file = '%s/STREAM_LOAD/expe_test_double_int.data' % file_dir

# schema.float_int_column_list, schema.float_int_column_no_agg_list
test_float_int_file = '%s/STREAM_LOAD/test_float_int.data' % file_dir
expe_test_float_int_file = '%s/STREAM_LOAD/expe_test_hash_float_int.data' % file_dir

empty_local_file = '%s/empty_file' % file_dir

# schema.array_boolean_list
test_array_boolean_local_file = '%s/LOAD/test_array_boolean.data' % file_dir
test_array_boolean_remote_file = palo_config.gen_remote_file_path('sys/load/test_array_boolean.data')
test_array_boolean_local_json = '%s/LOAD/test_array_boolean.json' % file_dir
expe_array_boolean_file = '%s/LOAD/expe_array_boolean.data' % file_dir

# schema.array_tinyint_list
test_array_tinyint_local_file = '%s/LOAD/test_array_tinyint.data' % file_dir
test_array_tinyint_remote_file = palo_config.gen_remote_file_path('sys/load/test_array_tinyint.data')
test_array_tinyint_local_json = '%s/LOAD/test_array_tinyint.json' % file_dir
expe_array_tinyint_file = '%s/LOAD/expe_array_tinyint.data' % file_dir

# schema.array_smallint_list
test_array_smallint_local_file = '%s/LOAD/test_array_smallint.data' % file_dir
test_array_smallint_remote_file = palo_config.gen_remote_file_path('sys/load/test_array_smallint.data')
test_array_smallint_local_json = '%s/LOAD/test_array_smallint.json' % file_dir
expe_array_smallint_file = '%s/LOAD/expe_array_smallint.data' % file_dir

# schema.array_int_list
test_array_int_local_file = '%s/LOAD/test_array_int.data' % file_dir
test_array_int_remote_file = palo_config.gen_remote_file_path('sys/load/test_array_int.data')
test_array_int_local_json = '%s/LOAD/test_array_int.json' % file_dir
expe_array_int_file = '%s/LOAD/expe_array_int.data' % file_dir

# schema.array_bigint_list
test_array_bigint_local_file = '%s/LOAD/test_array_bigint.data' % file_dir
test_array_bigint_remote_file = palo_config.gen_remote_file_path('sys/load/test_array_bigint.data')
test_array_bigint_local_json = '%s/LOAD/test_array_bigint.json' % file_dir
expe_array_bigint_file = '%s/LOAD/expe_array_bigint.data' % file_dir

# schema.array_largeint_list
test_array_largeint_local_file = '%s/LOAD/test_array_largeint.data' % file_dir
test_array_largeint_remote_file = palo_config.gen_remote_file_path('sys/load/test_array_largeint.data')
test_array_largeint_local_json = '%s/LOAD/test_array_largeint.json' % file_dir
expe_array_largeint_file = '%s/LOAD/expe_array_largeint.data' % file_dir

test_array_largeint_local_json_num = '%s/LOAD/test_array_largeint_num.json' % file_dir
expe_array_largeint_json_num_file_null = '%s/LOAD/expe_array_largeint_json_num1.data' % file_dir
expe_array_largeint_json_num_file_real = '%s/LOAD/expe_array_largeint_json_num2.data' % file_dir

# schema.array_decimal_list
test_array_decimal_local_file = '%s/LOAD/test_array_decimal.data' % file_dir
test_array_decimal_remote_file = palo_config.gen_remote_file_path('sys/load/test_array_decimal.data')
test_array_decimal_local_json = '%s/LOAD/test_array_decimal.json' % file_dir
expe_array_decimal_file = '%s/LOAD/expe_array_decimal.data' % file_dir

# schema.array_float_list
test_array_float_local_file = '%s/LOAD/test_array_float.data' % file_dir
test_array_float_remote_file = palo_config.gen_remote_file_path('sys/load/test_array_float.data')
test_array_float_local_json = '%s/LOAD/test_array_float.json' % file_dir
expe_array_float_file = '%s/LOAD/expe_array_float.data' % file_dir

# schema.array_double_list
test_array_double_local_file = '%s/LOAD/test_array_double.data' % file_dir
test_array_double_remote_file = palo_config.gen_remote_file_path('sys/load/test_array_double.data')
test_array_double_local_json = '%s/LOAD/test_array_double.json' % file_dir
expe_array_double_file = '%s/LOAD/expe_array_double.data' % file_dir

# schema.array_date_list
test_array_date_local_file = '%s/LOAD/test_array_date.data' % file_dir
test_array_date_remote_file = palo_config.gen_remote_file_path('sys/load/test_array_date.data')
test_array_date_local_json = '%s/LOAD/test_array_date.json' % file_dir
expe_array_date_file = '%s/LOAD/expe_array_date.data' % file_dir

# schema.array_datetime_list
test_array_datetime_local_file = '%s/LOAD/test_array_datetime.data' % file_dir
test_array_datetime_remote_file = palo_config.gen_remote_file_path('sys/load/test_array_datetime.data')
test_array_datetime_local_json = '%s/LOAD/test_array_datetime.json' % file_dir
expe_array_datetime_file = '%s/LOAD/expe_array_datetime.data' % file_dir

# schema.array_char_list
test_array_char_local_file = '%s/LOAD/test_array_char.data' % file_dir
test_array_char_remote_file = palo_config.gen_remote_file_path('sys/load/test_array_char.data')
test_array_char_local_json = '%s/LOAD/test_array_char.json' % file_dir
expe_array_char_file = '%s/LOAD/expe_array_char.data' % file_dir

# schema.array_varchar_list
test_array_varchar_local_file = '%s/LOAD/test_array_varchar.data' % file_dir
test_array_varchar_remote_file = palo_config.gen_remote_file_path('sys/load/test_array_varchar.data')
test_array_varchar_local_json = '%s/LOAD/test_array_varchar.json' % file_dir
expe_array_varchar_file = '%s/LOAD/expe_array_varchar.data' % file_dir

# schema.array_string_list
expe_array_string_file = '%s/LOAD/expe_array_string.data' % file_dir

# schema.array_table_list
test_array_table_local_file = '%s/LOAD/array_test.data' % file_dir
test_array_table_local_json = '%s/LOAD/array_test.json' % file_dir
test_array_table_remote_file = palo_config.gen_remote_file_path('sys/load/array_test.data')
expe_array_table_file = '%s/LOAD/expe_array_test.data' % file_dir

test_array_table_remote_parquet_string = palo_config.gen_remote_file_path('sys/load/array_test.parquet')
test_array_table_remote_parquet_hive = palo_config.gen_remote_file_path('sys/load/hive_array_test*.parquet')
test_array_table_remote_orc_hive = palo_config.gen_remote_file_path('sys/load/hive_array_test.orc')
test_array_table_remote_orc_string = palo_config.gen_remote_file_path('sys/load/array_test.orc')

# k1, k2 array
test_array_mix_file_local = '%s/LOAD/test_array_mix.data' % file_dir
