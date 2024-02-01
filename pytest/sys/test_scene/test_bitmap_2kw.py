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
#   @file test_bitmap_2kw.py
#   @date 2022-09-02 16:45:46
#   @brief bitmpa 2kw, be crash when join
#
#############################################################################
"""
bitmap query scene
"""
import sys
import os
file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(file_dir)
file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(file_dir)

from lib import palo_config
from lib import palo_client
from lib import util
from lib import common

config = palo_config.config
broker_info = palo_config.s3_info
database_name = 'test_scene_test_bitmap_2kw'


def setup_module():
    """setUp"""
    pass


def teardown_module():
    """tearDown"""
    pass


def test_bitmap_2kw():
    """
    {
    "title": "test_bitmap_2kw",
    "describe": "建表，导入s3数据，bitmap查询，验证结果正确",
    "tag": "system"
    } 
    """
    client = common.create_workspace(database_name)
    table_1 = 'table_1'
    table_2 = 'table_2'
    table_1_schema = [('label_id', 'bigint'), ('entity_ids', 'bitmap', 'bitmap_union')]
    ret = client.create_table(table_1, table_1_schema, replication_num=1)
    assert ret, 'create table failed'

    table_2_schema = [('entity', 'bigint'), ('account', "varchar(50)")]
    ret = client.create_table(table_2, table_2_schema, replication_num=1, keys_desc='UNIQUE KEY(`entity`)')
    assert ret, 'create table failed'

    file_list = list()
    for i in [1, 2, 3, 4]:
        file_list.append(palo_config.gen_s3_file_path('scene/data_20000000_%s' % i))
    load_data_info = palo_client.LoadDataInfo(file_list, table_1, column_terminator=',', 
                                              set_list=['entity_ids=to_bitmap(entity)'], 
                                              column_name_list=['entity', 'account', 'label_id'])
    ret = client.batch_load(util.get_label(), load_data_info, broker=broker_info, is_wait=True)
    assert ret, 'load table failed'
    load_data_info = palo_client.LoadDataInfo(file_list, table_2, column_terminator=',', 
                                              column_name_list=['entity', 'account', 'label_id'])
    ret = client.batch_load(util.get_label(), load_data_info, broker=broker_info, is_wait=True)
    assert ret, 'load table failed'
    query = """
            select /*+ SET_VAR(exec_mem_limit=8589934592) */
              account,
              count(*) as num
            from
              %s
            where
              entity in (
                select
                  bc
                from
                  %s
                   lateral view explode_bitmap(entity_ids) tmp_v as bc
                where label_id=20000000
              )
            group by account
            order by num desc, account limit 10
            """ % (table_2, table_1)
    ret = client.execute(query)
    expect_ret = ((u'1000', 10000), (u'1001', 10000), (u'1002', 10000),
                  (u'1003', 10000), (u'1004', 10000), (u'1005', 10000),
                  (u'1006', 10000), (u'1007', 10000), (u'1008', 10000),
                  (u'1009', 10000))
    util.check(ret, expect_ret)

