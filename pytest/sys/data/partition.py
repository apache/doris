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

"""partition """

import sys
import os
sys.path.append('../')
from lib import palo_config
file_dir = os.path.dirname(os.path.abspath(__file__))

schema_1 = [('k1', 'TINYINT'), \
            ('k2', 'SMALLINT'), \
            ('k3', 'INT'), \
            ('k4', 'BIGINT'), \
            ('k5', 'DATETIME'), \
            ('v1', 'DATE', 'REPLACE'), \
            ('v2', 'CHAR', 'REPLACE'), \
            ('v3', 'VARCHAR(4096)', 'REPLACE'), \
            ('v4', 'FLOAT', 'SUM'), \
            ('v5', 'DOUBLE', 'SUM'), \
            ('v6', 'DECIMAL(20,7)', 'SUM')]

schema_1_1 = [('k1', 'TINYINT'), \
            ('k2', 'SMALLINT'), \
            ('k3', 'INT'), \
            ('k4', 'BIGINT'), \
            ('k5', 'DATETIME'), \
            ('v1', 'DATE'), \
            ('v2', 'CHAR'), \
            ('v3', 'VARCHAR(4096)'), \
            ('v4', 'FLOAT', 'SUM'), \
            ('v5', 'DOUBLE', 'SUM'), \
            ('v6', 'DECIMAL(20,7)', 'SUM')]

file_path_1 = palo_config.gen_remote_file_path('sys/partition/partition_type')

expected_data_file_list_1 = ['%s/PARTITION/partition_type' % file_dir]
expected_file_1 = '%s/PARTITION/drop_partition_less_than_300_by_int' % file_dir
expected_file_2 = '%s/PARTITION/drop_partition_less_than_300_and_500_900_by_int' % file_dir
expected_file_3 = '%s/PARTITION/drop_partition_less_than_300_and_500_900_and_2200_2300_by_int' % file_dir
expected_file_4 = '%s/PARTITION/drop_partition_less_than_300_'\
        'and_500_900_and_2200_2300_and_tail_by_int' % file_dir
expected_file_5 = '%s/PARTITION/delete_k1_greater_than_26' % file_dir
expected_file_6 = '%s/PARTITION/delete_k1_lt_20_and_eq_30' % file_dir
expected_file_7 = '%s/PARTITION/delete_21_26' % file_dir
expected_file_8 = '%s/PARTITION/delete_21_25' % file_dir
expected_file_9 = '%s/PARTITION/delete_and_schema_change' % file_dir
expected_file_9_new = '%s/PARTITION/delete_and_schema_change_v3_v6' % file_dir
expected_file_10 = '%s/PARTITION/load_delete' % file_dir
expected_file_11 = '%s/PARTITION/delay_drop_recover' % file_dir

schema_2 = [('tinyint_key', 'TINYINT'), \
          ('smallint_key', 'SMALLINT'), \
          ('int_key', 'INT'), \
          ('bigint_key', 'BIGINT'), \
          ('char_50_key', 'CHAR(50)'), \
          ('character_key', 'VARCHAR(500)'), \
          ('char_key', 'CHAR'), \
          ('character_most_key', 'VARCHAR(65533)'), \
          ('decimal_key', 'DECIMAL(20, 6)'), \
          ('decimal_most_key', 'DECIMAL(27, 9)'), \
          ('date_key', 'DATE'), \
          ('datetime_key', 'DATETIME'), \
          ('tinyint_value', 'TINYINT', 'SUM'), \
          ('smallint_value', 'SMALLINT', 'SUM'), \
          ('int_value', 'int', 'SUM'), \
          ('bigint_value', 'BIGINT', 'SUM'), \
          ('char_50_value', 'CHAR(50)', 'REPLACE'), \
          ('character_value', 'VARCHAR(500)', 'REPLACE'), \
          ('char_value', 'CHAR', 'REPLACE'), \
          ('character_most_value', 'VARCHAR(65533)', 'REPLACE'), \
          ('decimal_value', 'DECIMAL(20, 6)', 'SUM'), \
          ('decimal_most_value', 'DECIMAL(27, 9)', 'SUM'), \
          ('date_value_max', 'DATE', 'MAX'), \
          ('date_value_replace', 'DATE', 'REPLACE'), \
          ('date_value_min', 'DATE', 'MIN'), \
          ('datetime_value_max', 'DATETIME', 'MAX'), \
          ('datetime_value_replace', 'DATETIME', 'REPLACE'), \
          ('datetime_value_min', 'DATETIME', 'MIN'), \
          ('float_value', 'FLOAT', 'SUM'), \
          ('double_value', 'DOUBLE', 'SUM')]   

file_path_2 = palo_config.gen_remote_file_path('sys/all_type.txt')
expected_data_file_list_2 = '%s/all_type_834' % file_dir
expected_data_file_list_3 = '%s/PARTITION/partition_type_del_k1' % file_dir
expected_data_file_list_4 = '%s/PARTITION/partition_type_delete_k3_eq_100' % file_dir

