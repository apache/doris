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
rollup scenario data
"""

import sys
sys.path.append("../")
from lib import palo_config

file_path_1 = palo_config.gen_remote_file_path('sys/rollup_scenario/shootrollup_a')
schema_1 = [('k1', 'INT'), \
            ('k2', 'INT'), \
            ('v1', 'VARCHAR(4096)', 'REPLACE'), \
            ('v2', 'FLOAT', 'SUM'), \
            ('v3', 'DECIMAL(20,7)', 'SUM')]
expected_data_file_list_1 = ['./data/ROLLUP_SCENARIO/shootrollup_a']
expected_data_file_list_1_b = ['./data/ROLLUP_SCENARIO/verify_a']
rollup_field_list_1 = ['k2', 'v3']

file_path_2 = palo_config.gen_remote_file_path('sys/rollup_scenario/shootrollup_b')
schema_2 = [('k1', 'INT'), \
            ('k2', 'INT'), \
            ('k3', 'INT'), \
            ('v1', 'INT', 'SUM'), \
            ('v2', 'INT', 'SUM')]
expected_data_file_list_2 = ['./data/ROLLUP_SCENARIO/shootrollup_b']
expected_data_file_list_2_b = ['./data/ROLLUP_SCENARIO/verify_b']
expected_data_file_list_2_c = ['./data/ROLLUP_SCENARIO/verify_c']
expected_data_file_list_3 = ['./data/ROLLUP_SCENARIO/all_data_after_rollup_then_delete']
rollup_field_list_2 = ['k1', 'k3', 'v1']

schema_3 = [('k1', 'INT'), \
            ('k2', 'INT'), \
            ('v1', 'INT', 'SUM'), \
            ('v2', 'INT', 'SUM')]
rollup_field_list_3 = ['k1', 'k2', 'v1']
rollup_field_list_4 = ['k1', 'v1']
