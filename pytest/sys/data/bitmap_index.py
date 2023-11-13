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
the data set for test bitmap index on palo
Date: 2020/02/17 11:48:37
"""
import sys
sys.path.append("../")
from lib import palo_config
from lib import palo_client

schema_agg_table = [('k1', 'TINYINT'), \
          ('k2', 'SMALLINT'), \
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

schema_agg_table_index = [('index_tinyint', 'k1', 'BITMAP'), \
                          ('index_smallint', 'k2', 'BITMAP'), \
                          ('index_int', 'int_key', 'BITMAP'), \
                          ('index_bigint', 'bigint_key', 'BITMAP'), \
                          ('index_char_50_key', 'char_50_key', 'BITMAP'), \
                          ('index_character_key', 'character_key', 'BITMAP'), \
                          ('index_date_key', 'date_key', 'BITMAP'), \
                          ('index_datetime_key', 'datetime_key', 'BITMAP')]

schema_agg_table_with_index_col = [('k1', 'BIGINT'),
                                   ('k2', 'SMALLINT'),
                                   ('v1', 'FLOAT', 'SUM'),
                                   ('v2', 'DOUBLE', 'SUM')]
                                   
schema_agg_table_with_index = [('k1_idx', 'k1', 'BITMAP')]

schema_agg_table_simple = [('k1', 'INT'), \
            ('k2', 'INT'), \
            ('k3', 'INT'), \
            ('v1', 'DOUBLE', 'SUM'), \
            ('v2', 'FLOAT', 'SUM')]

schema_dup = [('k1', 'INT'), \
            ('k2', 'INT'), \
            ('k3', 'INT'), \
            ('v1', 'VARCHAR(4096)'), \
            ('v2', 'FLOAT'), \
            ('v3', 'DOUBLE'), \
            ('v4', 'DECIMAL(20,7)')]

schema_uniq = [('k1', 'INT'), \
            ('k2', 'INT'), \
            ('k3', 'INT'), \
            ('v1', 'VARCHAR(4096)'), \
            ('v2', 'FLOAT'), \
            ('v3', 'DECIMAL(20,7)')]

key_1_dup = "DUPLICATE KEY(k1,k2)"
key_1_uniq = "UNIQUE KEY(k1,k2)"

file_path = palo_config.gen_remote_file_path('sys/all_type.txt')
expected_data_file_list = './data/all_type_834'
