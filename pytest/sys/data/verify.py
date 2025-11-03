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
p0 verify
"""

import sys
sys.path.append("../")
from lib import palo_config

file_path_1 = palo_config.gen_remote_file_path('sys/verify/data_a')
file_path_2 = palo_config.gen_remote_file_path('sys/verify/data_b')
file_path_3 = palo_config.gen_remote_file_path('sys/verify/data_0')

expected_data_file_list_1 = ['./data/VERIFY/data_a']
expected_data_file_list_2 = ['./data/VERIFY/data_a', './data/VERIFY/data_b']

expected_file_1 = './data/VERIFY/diff_len_1'
expected_file_2 = './data/VERIFY/diff_len_2'
expected_file_3 = './data/VERIFY/diff_len_3'

expected_data_file_list_schema_change_1 = ['../data/VERIFY/data_a']
expected_data_file_list_schema_change_2 = ['../data/VERIFY/data_a', '../data/VERIFY/data_b']

expected_file_schema_change_1 = '../data/VERIFY/diff_len_1'
expected_file_schema_change_2 = '../data/VERIFY/diff_len_2'
expected_file_schema_change_3 = '../data/VERIFY/diff_len_3'

schema_1 = [('k1', 'INT'), \
            ('v1', 'INT', 'SUM'), \
            ('v2', 'VARCHAR(4096)', 'REPLACE'), \
            ('v3', 'FLOAT', 'SUM'), \
            ('v4', 'DECIMAL(20,7)', 'SUM')]

schema_1_dup = [('k1', 'INT'), \
            ('v1', 'INT'), \
            ('v2', 'VARCHAR(4096)'), \
            ('v3', 'FLOAT'), \
            ('v4', 'DECIMAL(20,7)')]

schema_2 = [('k1', 'INT'), \
            ('v1', 'INT', 'MAX'), \
            ('v2', 'VARCHAR(4096)', 'REPLACE'), \
            ('v3', 'FLOAT', 'MAX'), \
            ('v4', 'DECIMAL(20,7)', 'MAX')]

schema_3 = [('k1', 'INT'), \
            ('v1', 'INT', 'MIN'), \
            ('v2', 'VARCHAR(4096)', 'REPLACE'), \
            ('v3', 'FLOAT', 'MIN'), \
            ('v4', 'DECIMAL(20,7)', 'MIN')]

schema_4 = [('k1', 'INT'), \
            ('v1', 'INT', 'REPLACE'), \
            ('v2', 'VARCHAR(4096)', 'REPLACE'), \
            ('v3', 'FLOAT', 'REPLACE'), \
            ('v4', 'DECIMAL(20,7)', 'REPLACE')]

schema_5 = [('k1', 'INT'), \
            ('k2', 'SMALLINT'), \
            ('k3', 'INT'), \
            ('k4', 'BIGINT'), \
            ('k5', 'LARGEINT'), \
            ('k6', 'DATE'), \
            ('k7', 'DATETIME'), \
            ('k8', 'CHAR(3)'), \
            ('k9', 'VARCHAR(10)'), \
            ('k10', 'DECIMAL(5,3)'), \
            ('v1', 'TINYINT', 'SUM'), \
            ('v2', 'SMALLINT', 'SUM'), \
            ('v3', 'INT', 'SUM'), \
            ('v4', 'BIGINT', 'SUM'), \
            ('v5', 'LARGEINT', 'SUM'), \
            ('v6', 'DATETIME', 'REPLACE'), \
            ('v7', 'DATE', 'REPLACE'), \
            ('v8', 'CHAR(10)', 'REPLACE'), \
            ('v9', 'VARCHAR(6)', 'REPLACE'), \
            ('v10', 'DECIMAL(27,9)', 'SUM')]

schema_6 = [('k1', 'TINYINT'), \
            ('k2', 'SMALLINT'), \
            ('k3', 'INT'), \
            ('k4', 'BIGINT'), \
            ('k5', 'LARGEINT'), \
            ('k6', 'DATE'), \
            ('k7', 'DATETIME'), \
            ('k8', 'CHAR(3)'), \
            ('k9', 'VARCHAR(1000)'), \
            ('k10', 'DECIMAL(5,3)'), \
            ('v1', 'TINYINT', 'SUM'), \
            ('v2', 'SMALLINT', 'SUM'), \
            ('v3', 'INT', 'SUM'), \
            ('v4', 'BIGINT', 'SUM'), \
            ('v5', 'LARGEINT', 'SUM'), \
            ('v6', 'DATETIME', 'REPLACE'), \
            ('v7', 'DATE', 'REPLACE'), \
            ('v8', 'CHAR(10)', 'REPLACE'), \
            ('v9', 'VARCHAR(6)', 'REPLACE'), \
            ('v10', 'DECIMAL(27,9)', 'SUM')]


