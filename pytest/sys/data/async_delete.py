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
async_delete.py
"""

import sys
sys.path.append('../')
from lib import palo_config

schema_1 = [('largeint_key', 'LARGEINT'), \
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
          ('largeint_value', 'LARGEINT', 'SUM'), \
          ('smallint_value', 'SMALLINT', 'SUM'), \
          ('int_value', 'int', 'SUM'), \
          ('bigint_value', 'BIGINT', 'SUM'), \
          ('char_50_value', 'CHAR(50)', 'REPLACE'), \
          ('character_value', 'VARCHAR(500)', 'REPLACE'), \
          ('char_value', 'CHAR', 'REPLACE'), \
          ('character_most_value', 'VARCHAR(65533)', 'REPLACE'), \
          ('decimal_value', 'DECIMAL(20, 6)', 'SUM'), \
          ('decimal_most_value', 'DECIMAL(27, 9)', 'SUM'), \
          ('date_value_replace', 'DATE', 'REPLACE'), \
          ('date_value_max', 'DATE', 'REPLACE'), \
          ('date_value_min', 'DATE', 'REPLACE'), \
          ('datetime_value_replace', 'DATETIME', 'REPLACE'), \
          ('datetime_value_max', 'DATETIME', 'REPLACE'), \
          ('datetime_value_min', 'DATETIME', 'REPLACE'), \
          ('float_value', 'FLOAT', 'SUM'), \
          ('double_value', 'DOUBLE', 'SUM')]   

hdfs_file_1 = palo_config.gen_remote_file_path('sys/all_type.txt')
hdfs_file_2 = palo_config.gen_remote_file_path('sys/async_delete/data_1')
expected_file_list_1 = './data/all_type_834'
expected_file_list_2 = './data/ASYNC_DELETE/all_type_835_delete_largeintkey_eq_1'
expected_file_list_3 = './data/ASYNC_DELETE/all_type_835_delete_largeintkey_eq_1_and_eq_127'
expected_file_list_4 = './data/ASYNC_DELETE/all_type_835_delete_largeintkey_eq_1_and_load'


