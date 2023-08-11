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
test binlog sync job schema & data
"""

import datetime
import decimal
import sys
import os
sys.path.append('../')
from lib import palo_config
file_dir = os.path.dirname(os.path.abspath(__file__))


column_1 = [('k1', 'int(11)'),
        ('k2', 'char(12)'),
        ('k3', 'date'),
        ('k4', 'decimal(27, 9)'),
        ('k5', 'varchar(50)'),
        ('k6', 'datetime'),
        ('k7', 'double')]

column_2 = [('k1', 'int(5)'),
        ('k2', 'int(10)'),
        ('k3', 'date'),
        ('k4', 'datetime'),
        ('k5', 'decimal(9,3)')]

column_3 = [('k1', 'int(11)'),
        ('k2', 'char(12)'),
        ('k3', 'date')]

column_4 = [('k1', 'int(11)'),
        ('k2', 'char(12)'),
        ('k3', 'date'),
        ('k4', 'decimal(27, 9)'),
        ('k5', 'varchar(50)'),
        ('k6', 'datetime'),
        ('k7', 'double'),
        ('k8', 'bigint'),
        ('k9', 'char')]

column_5 = [('k1', 'int(11)'),
        ('k2', 'varchar(50)'),
        ('k3', 'date'),
        ('k4', 'decimal(27, 9)'),
        ('k5', 'double')]

expected_file_1 = '%s/BINLOG/binlog_data_1' % file_dir
expected_file_2 = '%s/BINLOG/binlog_data_2' % file_dir
expected_file_3 = '%s/BINLOG/binlog_data_3' % file_dir
expected_file_4 = '%s/BINLOG/binlog_data_4' % file_dir
expected_file_5 = '%s/BINLOG/binlog_data_5' % file_dir
expected_file_6 = '%s/BINLOG/binlog_data_6' % file_dir
expected_file_7 = '%s/BINLOG/binlog_data_7' % file_dir
expected_file_8 = '%s/BINLOG/binlog_data_8' % file_dir
expected_file_9 = '%s/BINLOG/binlog_data_9' % file_dir
expected_file_10 = '%s/BINLOG/binlog_data_10' % file_dir
expected_file_11 = '%s/BINLOG/binlog_data_11' % file_dir
expected_file_12 = '%s/BINLOG/binlog_data_12' % file_dir

binlog_sql_1 = '%s/BINLOG/binlog_sql_1' % file_dir
binlog_sql_2 = '%s/BINLOG/binlog_sql_2' % file_dir
binlog_sql_3 = '%s/BINLOG/binlog_sql_3' % file_dir
binlog_sql_4 = '%s/BINLOG/binlog_sql_4' % file_dir
binlog_sql_5 = '%s/BINLOG/binlog_sql_5' % file_dir
binlog_sql_6 = '%s/BINLOG/binlog_sql_6' % file_dir
binlog_sql_7 = '%s/BINLOG/binlog_sql_7' % file_dir
binlog_sql_8 = '%s/BINLOG/binlog_sql_8' % file_dir
binlog_sql_9 = '%s/BINLOG/binlog_sql_9' % file_dir
binlog_sql_10 = '%s/BINLOG/binlog_sql_10' % file_dir
binlog_sql_11 = '%s/BINLOG/binlog_sql_11' % file_dir
binlog_sql_12 = '%s/BINLOG/binlog_sql_12' % file_dir

