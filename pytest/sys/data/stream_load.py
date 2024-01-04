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
stream load
"""

import sys
import os
sys.path.append('../')
file_dir = os.path.abspath(os.path.dirname(__file__))

partition_data = '%s/PARTITION/partition_type' % file_dir
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
expected_data_file_list_1 = ['%s/PARTITION/partition_type' % file_dir]
# this for insert streamint test
schema_join = [('ak1', 'TINYINT'),
               ('ak2', 'SMALLINT'),
               ('ak3', 'INT'),
               ('ak4', 'BIGINT'),
               ('ak5', 'DECIMAL(20, 7)'),
               ('ak6', 'CHAR(5)'),
               ('ak7', 'VARCHAR(20)'),
               ('ak8', 'FLOAT'),
               ('ak9', 'DOUBLE'),
               ('ak10', 'DATE'),
               ('ak11', 'DATETIME'),
               ('bk1', 'TINYINT'),
               ('bk2', 'SMALLINT'),
               ('bk3', 'INT'),
               ('bk4', 'BIGINT'),
               ('bk5', 'DECIMAL(20, 7)'),
               ('bk6', 'CHAR(5)'),
               ('bk7', 'VARCHAR(20)'),
               ('bk8', 'FLOAT'),
               ('bk9', 'DOUBLE'),
               ('bk10', 'DATE'),
               ('bk11', 'DATETIME'),
              ]
key_type = 'unique key (ak1, ak2, ak3, ak4, ak5, ak6, ak7, ak8, ak9, ak10, ak11, \
            bk1, bk2, bk3, bk4, bk5, bk6, bk7, bk8, bk9, bk10, bk11)'

