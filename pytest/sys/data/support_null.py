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
null test
"""

import sys
sys.path.append('../')
from lib import palo_config

schema_1 = [('k1', 'TINYINT'), \
            ('k2', 'SMALLINT'), \
            ('k3', 'INT'), \
            ('k4', 'BIGINT'), \
            ('k5', 'DATETIME'), \
            ('v1', 'DATE', 'REPLACE'), \
            ('v2', 'CHAR', 'REPLACE'), \
            ('v3', 'VARCHAR(4096)', 'REPLACE'), \
            ('v4', 'DECIMAL(27,9)', 'SUM'), \
            ('v5', 'DECIMAL(27,9)', 'SUM'), \
            ('v6', 'DECIMAL(27,9)', 'SUM')]

schema_2 = [('k1', 'TINYINT'), \
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


schema_3 = [('k1', 'TINYINT'), 
            ('k2', 'SMALLINT'), 
            ('k3', 'INT'), 
            ('k4', 'BIGINT'), 
            ('k5', 'LARGEINT'), 
            ('k6', 'DATE'), 
            ('k7', 'DATETIME'), 
            ('k8', 'CHAR(3)'), 
            ('k9', 'VARCHAR(10)'), 
            ('k10', 'DECIMAL(5,3)'), 
            ('v1', 'TINYINT', 'SUM'), 
            ('v2', 'SMALLINT', 'SUM'), 
            ('v3', 'INT', 'SUM'), 
            ('v4', 'BIGINT', 'SUM'), 
            ('v5', 'LARGEINT', 'SUM'), 
            ('v6', 'DATETIME', 'REPLACE'), 
            ('v7', 'DATE', 'REPLACE'), 
            ('v8', 'CHAR(10)', 'REPLACE'), 
            ('v9', 'VARCHAR(6)', 'REPLACE'), 
            ('v10', 'DECIMAL(27,9)', 'SUM')]


schema_4 = [('k5', 'LARGEINT'), \
            ('k1', 'TINYINT'), \
            ('k2', 'SMALLINT'), \
            ('k3', 'INT'), \
            ('k4', 'BIGINT'), \
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



hdfs_file_1 = palo_config.gen_remote_file_path('sys/null/data_1')
hdfs_file_2 = palo_config.gen_remote_file_path('sys/null/data_2')
hdfs_file_3 = palo_config.gen_remote_file_path('sys/null/data_3')
hdfs_file_4 = palo_config.gen_remote_file_path('sys/null/data_4')
hdfs_file_5 = palo_config.gen_remote_file_path('sys/null/data_5')
hdfs_file_6 = palo_config.gen_remote_file_path('sys/null/data_6')
hdfs_file_7 = palo_config.gen_remote_file_path('sys/null/data_7')
hdfs_file_8_1 = palo_config.gen_remote_file_path('sys/null/x00data_8')
hdfs_file_8_2 = palo_config.gen_remote_file_path('sys/null/x01data_8')
hdfs_file_8_3 = palo_config.gen_remote_file_path('sys/null/x02data_8')
hdfs_file_8_4 = palo_config.gen_remote_file_path('sys/null/x03data_8')
hdfs_file_8_5 = palo_config.gen_remote_file_path('sys/null/x04data_8')
hdfs_file_9_1 = palo_config.gen_remote_file_path('sys/null/data_9_1')
hdfs_file_10_1 = palo_config.gen_remote_file_path('sys/null/data_10_1')
hdfs_file_12 = palo_config.gen_remote_file_path('sys/null/data_12')
hdfs_file_14_1 = palo_config.gen_remote_file_path('sys/null/data_14_1')
hdfs_file_14_2 = palo_config.gen_remote_file_path('sys/null/data_14_2')
hdfs_file_all_null = palo_config.gen_remote_file_path('sys/null/data_all_null')
hdfs_file_17 = palo_config.gen_remote_file_path('sys/null/data_17')

expected_file_1 = './data/NULL/verify_3'
expected_file_4 = './data/NULL/verify_4'
expected_file_5 = './data/NULL/verify_5'
expected_file_5_1 = './data/NULL/verify_5_1'
expected_file_6 = './data/NULL/verify_6'
expected_file_7 = './data/NULL/verify_7'
expected_file_8 = './data/NULL/verify_8'
expected_file_9 = './data/NULL/verify_9'
expected_file_9_1 = './data/NULL/verify_9_1'
expected_file_10 = './data/NULL/verify_10'
expected_file_n10 = './data/NULL/verify_n10'
expected_file_10_1 = './data/NULL/verify_10_1'
expected_file_11 = './data/NULL/verify_11'
expected_file_12 = './data/NULL/verify_12'
expected_file_13 = './data/NULL/verify_13'
expected_file_14 = './data/NULL/verify_14'
expected_file_all_null = './data/NULL/verify_all_null'
expected_file_15_1 = './data/NULL/verify_15_1'
expected_file_15_1_1 = './data/NULL/verify_15_1_1'
expected_file_15_2 = './data/NULL/verify_15_2'
expected_file_15_2_1 = './data/NULL/verify_15_2_1'
expected_file_15_3 = './data/NULL/verify_15_3'
expected_file_15_4 = './data/NULL/verify_15_4'
expected_file_16 = './data/NULL/verify_16'
expected_file_17 = './data/NULL/verify_17'
expected_file_ds_1 = './data/NULL/verify_ds_1'
expected_file_ds_2 = './data/NULL/verify_ds_2'
expected_file_external_1 = './data/NULL/verify_e1'
expected_file_external_2 = './data/NULL/verify_e2'

expected_file_list_data_1 = './data/NULL/verify_1'
expected_file_list_data_2 = './data/NULL/verify_2'
