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
pull load
"""
import os
import sys
sys.path.append("../")
from lib import palo_config

file_dir = os.path.abspath(os.path.dirname(__file__))

data_1 = palo_config.gen_hdfs_file_path('sys/pull_load/pull_load_data')
bos_data_1 = palo_config.gen_hdfs_file_path('sys/pull_load/pull_load_data')
data_noexsit = palo_config.gen_hdfs_file_path('sys/pull_load/noexist')
data_1_gz = palo_config.gen_hdfs_file_path('sys/pull_load/pull_load_data.gz')
data_1_bz2 = palo_config.gen_hdfs_file_path('sys/pull_load/pull_load_data.bz2')
data_1_lzo = palo_config.gen_hdfs_file_path('sys/pull_load/pull_load_data.lzo')
data_1_lz4 = palo_config.gen_hdfs_file_path('sys/pull_load/pull_load_data.lz4')
data_1_parquet = palo_config.gen_hdfs_file_path('sys/pull_load/all_type_none.parquet')
data_1_orc = palo_config.gen_hdfs_file_path('sys/pull_load/pull_load_data.orc')
data_2 = palo_config.gen_hdfs_file_path('sys/pull_load/1g_data')
data_3 = palo_config.gen_hdfs_file_path('sys/pull_load/duo_s')
data_4 = palo_config.gen_hdfs_file_path('sys/pull_load/duo_data_1')
data_5 = palo_config.gen_hdfs_file_path('sys/pull_load/duo_data_3')
data_5_gz = palo_config.gen_hdfs_file_path('sys/pull_load/duo_data_3.gz')
data_6 = palo_config.gen_hdfs_file_path('sys/pull_load/duo_data_5')
data_6_lzo = palo_config.gen_hdfs_file_path('sys/pull_load/duo_data_5.lzo')
data_7 = palo_config.gen_hdfs_file_path('sys/pull_load/duo_data_7')
data_7_bz2 = palo_config.gen_hdfs_file_path('sys/pull_load/duo_data_7.bz2')
data_8 = palo_config.gen_hdfs_file_path('sys/pull_load/data-dir/*')
data_9 = palo_config.gen_hdfs_file_path('sys/pull_load/data-dir-b/*')
data_parquet_null = palo_config.gen_hdfs_file_path('sys/pull_load/data_parquet_null.parquet')
data_orc_null = palo_config.gen_hdfs_file_path('sys/pull_load/data_orc_null.orc')

verify_1 = ['%s/PULL_LOAD/pull_load_verify' % file_dir]
verify_2 = ['%s/PULL_LOAD/1g_verify' % file_dir]
verify_3 = ['%s/PULL_LOAD/duo_j' % file_dir]
verify_4 = ['%s/PULL_LOAD/duo_2' % file_dir, '%s/PULL_LOAD/duo_4' % file_dir]
verify_5 = ['%s/PULL_LOAD/duo_6' % file_dir, '%s/PULL_LOAD/duo_8' % file_dir]
verify_6 = ['%s/PULL_LOAD/parquet_pull_load_verify' % file_dir]
verify_7 = ['%s/PULL_LOAD/pull_load_null_verify' % file_dir]

schema_0 = [\
          ('tinyint_key', 'TINYINT'), \
          ('smallint_key', 'SMALLINT'), \
          ('int_key', 'INT'), \
          ('bigint_key', 'BIGINT'), \
          ('largeint_key', 'LARGEINT'), \
          ('char_key', 'CHAR(50)'), \
          ('varchar_key', 'VARCHAR(65533)'), \
          ('decimal_key', 'DECIMAL(27, 9)'), \
          ('date_key', 'DATE'), \
          ('datetime_key', 'DATETIME'), \
          ('tinyint_value_max', 'TINYINT', 'MAX'), \
          ('smallint_value_min', 'SMALLINT', 'MIN'), \
          ('int_value_sum', 'INT', 'SUM'), \
          ('bigint_value_sum', 'BIGINT', 'SUM'), \
          ('largeint_value_sum', 'LARGEINT', 'SUM'), \
          ('float_value_sum', 'FLOAT', 'SUM'), \
          ('double_value_sum', 'DOUBLE', 'SUM')]   

AGGREGATE_KEYS = 'AGGREGATE KEY(tinyint_key, smallint_key, int_key, \
        bigint_key, largeint_key, char_key, varchar_key, decimal_key, date_key, datetime_key)'

schema_1 = [\
          ('tinyint_key', 'TINYINT'), \
          ('smallint_key', 'SMALLINT'), \
          ('int_key', 'INT'), \
          ('bigint_key', 'BIGINT'), \
          ('largeint_key', 'LARGEINT'), \
          ('char_key', 'CHAR(50)'), \
          ('varchar_key', 'VARCHAR(65533)'), \
          ('decimal_key', 'DECIMAL(27, 9)'), \
          ('date_key', 'DATE'), \
          ('datetime_key', 'DATETIME'), \
          ('tinyint_value_max', 'TINYINT', 'MAX'), \
          ('smallint_value_min', 'SMALLINT', 'MIN'), \
          ('int_value_sum', 'INT', 'SUM'), \
          ('bigint_value_sum', 'BIGINT', 'SUM'), \
          ('largeint_value_sum', 'LARGEINT', 'SUM'), \
          ('largeint_value_replace', 'LARGEINT', 'replace'), \
          ('char_value_replace', 'CHAR(50)', 'REPLACE'), \
          ('varchar_value_replace', 'VARCHAR(65533)', 'REPLACE'), \
          ('decimal_value_replace', 'DECIMAL(27, 9)', 'REPLACE'), \
          ('date_value_replace', 'DATE', 'REPLACE'), \
          ('datetime_value_replace', 'DATETIME', 'REPLACE'), \
          ('float_value_sum', 'FLOAT', 'SUM'), \
          ('double_value_sum', 'DOUBLE', 'SUM')]   


schema_2 = [\
          ('tinyint_key', 'TINYINT'), \
          ('smallint_key', 'SMALLINT'), \
          ('int_key', 'INT'), \
          ('bigint_key', 'BIGINT'), \
          ('largeint_key', 'LARGEINT'), \
          ('char_key', 'CHAR(50)'), \
          ('varchar_key', 'VARCHAR(65533)'), \
          ('decimal_key', 'DECIMAL(27, 9)'), \
          ('date_key', 'DATE'), \
          ('datetime_key', 'DATETIME'), \
          ('tinyint_value_max', 'TINYINT', 'MAX'), \
          ('smallint_value_min', 'SMALLINT', 'MIN'), \
          ('int_value_sum', 'INT', 'SUM'), \
          ('bigint_value_sum', 'BIGINT', 'SUM', '5'), \
          ('largeint_value_sum', 'LARGEINT', 'SUM'), \
          ('float_value_sum', 'FLOAT', 'SUM'), \
          ('double_value_sum', 'DOUBLE', 'SUM')]   


