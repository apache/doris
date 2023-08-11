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

"""load restrict"""

import sys
sys.path.append("../")
from lib import palo_config

schema_1 = [('k1', 'TINYINT'), \
            ('k2', 'SMALLINT'), \
            ('k3', 'INT'), \
            ('k4', 'BIGINT'), \
            ('k5', 'DATETIME'), \
            ('k6', 'DATE'), \
            ('v1', 'CHAR(255)', 'REPLACE'), \
            ('v2', 'VARCHAR(65533)' 'REPLACE'), \
            ('v3', 'DECIMAL(27, 9)', 'MAX'), \
            ('v4', 'FLOAT', 'REPLACE'), \
            ('v5', 'DOUBLE', 'MAX'), \
            ('v6', 'TINYINT', 'MAX'), \
            ('v7', 'SMALLINT', 'MIN'), \
            ('v8', 'INT', 'REPLACE'), \
            ('v9', 'BIGINT', 'MAX'), \
            ('v10', 'DATETIME', 'REPLACE'), \
            ('v11', 'DATE', 'REPLACE'), \
            ('v12', 'CHAR(255)', 'REPLACE'), \
            ('v13', 'DECIMAL(27, 9)', 'MAX'), \
            ('v14', 'VARCHAR(65533)', 'REPLACE')]

file_path_1 = palo_config.gen_remote_file_path('load_data/v3_after_v7_r_last')
column_name_list_1 = ['k1', 'k2', 'k3', 'k4', 'k5', 'k6', \
        'v1', 'v2', 'v4', 'v5', 'v6', 'v7', 'v3', 'v8', 'v9', 'v10', 'v11', \
        'v12', 'v13', 'v14', 'notexist']

file_path_list_2 = []
ix = 1
while ix < 200:
    file_path = palo_config.gen_remote_file_path('sys/restrict/23')
    file_path_list_2.append(file_path)
    ix = ix + 1

file_path_list_3 = []
ix = 1
while ix < 29:
    file_path = palo_config.gen_remote_file_path('sys/restrict/%d' % (ix))
    file_path_list_3.append(file_path)
    ix = ix + 1
 
