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

############################################################################
#
#   @file test_broker_load_remote.py
#   @date 2022-11-23 11:52:36
#   @brief load bos & s3 data
#
#############################################################################
"""
test load bos & s3 data
"""
import sys
import os
file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(file_dir)
file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(file_dir)

from data import schema as DATA
from lib import palo_config
from lib import palo_client
from lib import util
from lib import common

config = palo_config.config


def test_s3_http():
    """
    {
    "title": "test_s3_http",
    "describe": "建表，导入s3 http数据，结果正确",
    "tag": "system"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = common.create_workspace(database_name)
    client.create_table(table_name, DATA.baseall_column_list,
                        set_null=True, distribution_info=DATA.baseall_distribution_info)
    assert client.show_tables(table_name)
    if 'https:' in config.s3_endpoint:
        endpoint = config.s3_endpoint.replace('https:', 'http:')
    else:
        endpoint = config.s3_endpoint
    s3_info = palo_config.S3Info({"AWS_ENDPOINT": endpoint,
                                  "AWS_ACCESS_KEY": config.bos_accesskey,
                                  "AWS_SECRET_KEY": config.bos_secret_accesskey,
                                  "AWS_REGION": config.bos_region})
    s3_file = palo_config.gen_s3_file_path('qe/baseall.txt')
    data_desc_list = palo_client.LoadDataInfo(s3_file, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=s3_info,
                            max_filter_ratio=0.05, strict_mode=True)
    assert ret, 's3 load failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall order by k1' % config.palo_db
    common.check2(client, sql1, sql2=sql2)
    client.clean(database_name)


def test_s3_https():
    """
    {
    "title": "test_s3_https",
    "describe": "建表，导入s3 https数据，验证结果正确",
    "tag": "system"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = common.create_workspace(database_name)
    client.create_table(table_name, DATA.baseall_column_list,
                        set_null=True, distribution_info=DATA.baseall_distribution_info)
    assert client.show_tables(table_name)
    if 'http:' in config.s3_endpoint:
        endpoint = config.s3_endpoint.replace('http:', 'https:')
    else:
        endpoint = config.s3_endpoint
    s3_info = palo_config.S3Info({"AWS_ENDPOINT": endpoint,
                                  "AWS_ACCESS_KEY": config.bos_accesskey,
                                  "AWS_SECRET_KEY": config.bos_secret_accesskey,
                                  "AWS_REGION": config.bos_region})
    s3_file = palo_config.gen_s3_file_path('qe/baseall.txt')
    data_desc_list = palo_client.LoadDataInfo(s3_file, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=s3_info,
                            max_filter_ratio=0.05, strict_mode=True)
    assert ret, 's3 load failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall order by k1' % config.palo_db
    common.check2(client, sql1, sql2=sql2)
    client.clean(database_name)


def test_bos_load():
    """
    {
    "title": "test_bos_load",
    "describe": "建表，导入bos数据，不支持bos开头的路径",
    "tag": "system"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = common.create_workspace(database_name)
    client.create_table(table_name, DATA.baseall_column_list,
                        set_null=True, distribution_info=DATA.baseall_distribution_info)
    assert client.show_tables(table_name)
    bos_info = palo_config.BrokerInfo('hdfs', 
                                      {"bos_endpoint": config.bos_endpoint,
                                       "bos_accesskey": config.bos_accesskey,
                                       "bos_secret_accesskey": config.bos_secret_accesskey})
    bos_file = palo_config.gen_bos_file_path('qe/baseall.txt')
    data_desc_list = palo_client.LoadDataInfo(bos_file, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=bos_info,
                            max_filter_ratio=0.05, strict_mode=True)
    assert ret, 'bos load failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall order by k1' % config.palo_db
    common.check2(client, sql1, sql2=sql2)
    client.clean(database_name)


def test_broker_load_err():
    """
    {
    "title": "test_broker_load_err",
    "describe": "建表，导入错误数据路径，不支持协议开头的路径",
    "tag": "system"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = common.create_workspace(database_name)
    client.create_table(table_name, DATA.baseall_column_list,
                        set_null=True, distribution_info=DATA.baseall_distribution_info)
    assert client.show_tables(table_name)
    bos_info = palo_config.BrokerInfo('hdfs',
                                      {"bos_endpoint": config.bos_endpoint,
                                       "bos_accesskey": config.bos_accesskey,
                                       "bos_secret_accesskey": config.bos_secret_accesskey})
    bos_file = palo_config.gen_bos_file_path('qe/baseall.txt')

    data_desc_list = palo_client.LoadDataInfo(bos_file.replace('bos', 'wrong'), table_name)
    msg = "Invalid broker path."
    util.assert_return(False, msg, client.batch_load, util.get_label(), data_desc_list, broker=bos_info)
    
