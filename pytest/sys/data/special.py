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
special
"""

import sys
sys.path.append("../")
from lib import palo_config

file_path_1 = palo_config.gen_remote_file_path('sys/special/all_int_valid')
schema_1 = [('tinyint_key', 'TINYINT'), \
            ('smallint_key', 'SMALLINT'), \
            ('int_value', 'INT', 'SUM'), \
            ('bigint_value', 'BIGINT', 'SUM')]
expected_data_file_list_1 = ['./data/SPECIAL/all_int_valid']

file_path_2 = palo_config.gen_remote_file_path('sys/special/decimal_a')
schema_2 = [('tinyint_key', 'TINYINT'), \
            ('v1', 'DECIMAL(27, 9)', 'SUM'), \
            ('v2', 'DECIMAL(27, 9)', 'SUM'), \
            ('v3', 'DECIMAL(27, 9)', 'SUM'), \
            ('v4', 'DECIMAL(27, 9)', 'SUM')]
expected_data_file_list_2 = ['./data/SPECIAL/decimal_a']

file_path_3 = palo_config.gen_remote_file_path('sys/special/decimal_b')
schema_3 = [('tinyint_key', 'TINYINT'), \
          ('v1', 'DECIMAL', 'SUM'), \
          ('v2', 'DECIMAL(20, 5)', 'SUM'), \
          ('v3', 'DECIMAL(27, 9)', 'SUM')]
expected_data_file_list_3 = ['./data/SPECIAL/decimal_b']


file_path_4 = palo_config.gen_remote_file_path('sys/special/char_type')
schema_4 = [('tinyint_key', 'TINYINT'), \
          ('v1', 'CHAR ', 'REPLACE'), \
          ('v2', 'CHAR(255)', 'REPLACE'), \
          ('v3', 'VARCHAR(65533)', 'REPLACE')]
expected_data_file_list_4 = ['./data/SPECIAL/char_type']


file_path_5 = palo_config.gen_remote_file_path('sys/special/char_type_invalid')
schema_5 = [('tinyint_key', 'TINYINT'), \
            ('v1', 'CHAR ', 'REPLACE'), \
            ('v2', 'CHAR(255)', 'REPLACE'), \
            ('v3', 'VARCHAR(65533)', 'REPLACE')]
expected_data_file_list_5 = ['./data/SPECIAL/char_type_invalid']


