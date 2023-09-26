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
timestamp data
"""

import sys
sys.path.append("../")
from lib import palo_config

file_path_1 = palo_config.gen_remote_file_path('sys/verify/datetime_data')
file_path_2 = palo_config.gen_remote_file_path('sys/verify/datetime_data_with_null')
file_path_3 = palo_config.gen_remote_file_path('sys/verify/datetime_data_with_out_of_range')
file_path_4 = palo_config.gen_remote_file_path('sys/verify/timestamp_data')
file_path_5 = palo_config.gen_remote_file_path('sys/verify/timestamp_data_with_out_range')
file_path_6 = palo_config.gen_remote_file_path('sys/verify/datetime_timestamp_data')
file_path_7 = palo_config.gen_remote_file_path('sys/verify/datetime_timestamp_data_with_invalid')
file_path_8 = palo_config.gen_remote_file_path('sys/verify/timestamp_special_data')

expected_data_file_list_1 = ['./data/VERIFY/datetime_file']
expected_data_file_list_2 = ['./data/VERIFY/timestamp_file']
expected_data_file_list_3 = ['./data/VERIFY/datetime_timestamp_file']
expected_data_file_list_4 = ['./data/VERIFY/timestamp_special_file']
expected_data_file_list_5 = ['./data/VERIFY/timestamp_one_to_many']

schema_1 = [('k1', 'DATETIME'), \
            ('k2', 'INT'), \
            ('k3', 'DATETIME'), \
            ('k4', 'DATETIME'), \
            ('v1', 'VARCHAR(4096)', 'REPLACE'), \
            ('v2', 'DATETIME', 'REPLACE'), \
            ('v3', 'FLOAT', 'REPLACE'), \
            ('v4', 'DATETIME', 'REPLACE'), \
            ('v5', 'DECIMAL(20,7)', 'REPLACE'), \
            ('v6', 'DATETIME', 'REPLACE'), \
            ('v7', 'DATETIME', 'REPLACE')]

schema_2 = [('k1', 'BIGINT'), \
            ('k2', 'BIGINT'), \
            ('k3', 'BIGINT'), \
            ('k4', 'BIGINT'), \
            ('k5', 'BIGINT'), \
            ('v1', 'VARCHAR(4096)', 'REPLACE'), \
            ('v2', 'BIGINT', 'REPLACE'), \
            ('v3', 'FLOAT', 'REPLACE'), \
            ('v4', 'BIGINT', 'REPLACE'), \
            ('v5', 'DECIMAL(20,7)', 'REPLACE'), \
            ('v6', 'BIGINT', 'REPLACE'), \
            ('v7', 'BIGINT', 'REPLACE')]

schema_3 = [('k1', 'DATETIME'), \
            ('k2', 'INT'), \
            ('k3', 'BIGINT'), \
            ('k4', 'BIGINT'), \
            ('k5', 'DATETIME'), \
            ('k6', 'BIGINT'), \
            ('k7', 'BIGINT'), \
            ('k8', 'DATETIME'), \
            ('v1', 'VARCHAR(4096)', 'REPLACE'), \
            ('v2', 'DATETIME', 'REPLACE'), \
            ('v3', 'BIGINT', 'REPLACE'), \
            ('v4', 'DATETIME', 'REPLACE'), \
            ('v5', 'BIGINT', 'REPLACE'), \
            ('v6', 'BIGINT', 'REPLACE'), \
            ('v7', 'DATETIME', 'REPLACE'), \
            ('v8', 'BIGINT', 'REPLACE'), \
            ('v9', 'DATETIME', 'REPLACE')]

schema_4 = [('k1', 'DATETIME'), \
            ('k2', 'INT'), \
            ('k3', 'BIGINT', None, '30'), \
            ('k4', 'BIGINT'), \
            ('k5', 'DATETIME', None, '1949-10-01 08:00:00'), \
            ('k6', 'BIGINT'), \
            ('k7', 'BIGINT'), \
            ('k8', 'DATETIME'), \
            ('v1', 'VARCHAR(4096)', 'REPLACE'), \
            ('v2', 'DATETIME', 'REPLACE'), \
            ('v3', 'BIGINT', 'REPLACE'), \
            ('v4', 'DATETIME', 'REPLACE'), \
            ('v5', 'BIGINT', 'REPLACE', '50'), \
            ('v6', 'BIGINT', 'REPLACE'), \
            ('v7', 'DATETIME', 'REPLACE', '2008-08-08 20:08:08'), \
            ('v8', 'BIGINT', 'REPLACE'), \
            ('v9', 'DATETIME', 'REPLACE')]


schema_5 = [('k1', 'DATETIME'), \
            ('k2', 'BIGINT'), \
            ('k3', 'BIGINT')]


schema_6 = [('k1', 'VARCHAR(1024)'), \
            ('k2', 'INT'), \
            ('k3', 'VARCHAR(1024)'), \
            ('k4', 'VARCHAR(1024)'), \
            ('k5', 'VARCHAR(1024)'), \
            ('k6', 'VARCHAR(1024)'), \
            ('k7', 'VARCHAR(1024)'), \
            ('k8', 'VARCHAR(1024)'), \
            ('v1', 'VARCHAR(4096)', 'REPLACE'), \
            ('v2', 'VARCHAR(1024)', 'REPLACE'), \
            ('v3', 'VARCHAR(1024)', 'REPLACE'), \
            ('v4', 'VARCHAR(1024)', 'REPLACE'), \
            ('v5', 'VARCHAR(1024)', 'REPLACE'), \
            ('v6', 'VARCHAR(1024)', 'REPLACE'), \
            ('v7', 'VARCHAR(1024)', 'REPLACE'), \
            ('v8', 'VARCHAR(1024)', 'REPLACE'), \
            ('v9', 'VARCHAR(1024)', 'REPLACE')]


schema_7 = [('k1', 'BIGINT'), \
            ('k2', 'BIGINT'), \
            ('k3', 'BIGINT'), \
            ('k4', 'BIGINT'), \
            ('k5', 'BIGINT'), \
            ('v1', 'VARCHAR(4096)', 'REPLACE'), \
            ('v2', 'BIGINT', 'REPLACE'), \
            ('v4', 'BIGINT', 'REPLACE'), \
            ('v6', 'BIGINT', 'REPLACE'), \
            ('v7', 'BIGINT', 'REPLACE')]


