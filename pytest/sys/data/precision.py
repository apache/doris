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
precision
"""

import sys
sys.path.append("../")
from lib import palo_config


file_path = palo_config.gen_remote_file_path('sys/precision/float')
expected_data_file_list = ['./data/PRECISION/float']
schema_1 = [('k1', 'INT'), \
            ('v1', 'FLOAT', 'SUM'), \
            ('v2', 'FLOAT', 'SUM'), \
            ('v3', 'FLOAT', 'SUM'), \
            ('v4', 'FLOAT', 'SUM'), \
            ('v5', 'FLOAT', 'SUM'), \
            ('v6', 'FLOAT', 'SUM'), \
            ('v7', 'FLOAT', 'SUM'), \
            ('v8', 'FLOAT', 'SUM'), \
            ('v9', 'FLOAT', 'SUM'), \
            ('v10', 'FLOAT', 'SUM'), \
            ('v11', 'FLOAT', 'SUM')]

schema_2 = [('k1', 'INT'), \
            ('v1', 'DOUBLE', 'SUM'), \
            ('v2', 'DOUBLE', 'SUM'), \
            ('v3', 'DOUBLE', 'SUM'), \
            ('v4', 'DOUBLE', 'SUM'), \
            ('v5', 'DOUBLE', 'SUM'), \
            ('v6', 'DOUBLE', 'SUM'), \
            ('v7', 'DOUBLE', 'SUM'), \
            ('v8', 'DOUBLE', 'SUM'), \
            ('v9', 'DOUBLE', 'SUM'), \
            ('v10', 'DOUBLE', 'SUM'), \
            ('v11', 'DOUBLE', 'SUM')]

