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
/***************************************************************************
  *
  * @file test_sys_alter_schema_change.py
  * @date 2019-11-01
  * @brief This file is a test file for Palo schema changing.
  * 
  **************************************************************************/
"""

import random
import time

from data import schema_change as DATA
from lib import palo_config
from lib import palo_client
from lib import util

config = palo_config.config
LOG = palo_client.LOG
L = palo_client.L


def setup_module():
    """
    setUp
    """
    global client
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)


def check_partition_list(table_name, partition_name_list):
    """
    验证分区是否创建成功
    """
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)


def partition_check(table_name, column_name, partition_name_list, \
        partition_value_list, distribution_type, bucket_num, storage_type):
    """
    检查，验证
    """
    partition_info = palo_client.PartitionInfo(column_name, \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo(distribution_type, bucket_num)
    client.create_table(table_name, DATA.schema_1, \
            partition_info, distribution_info)
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)


def check(table_name):
    """
    分区，检查
    """
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d',
                           'partition_e', 'partition_f', 'partition_g']
    partition_value_list = ['5', '30', '100', '500', '1000', '2000', 'MAXVALUE']
    partition_check(table_name, 'k1',
                    partition_name_list, partition_value_list,
                    'HASH(k1, k2)', random.randrange(1, 30), 'column')


def test_change_partition_actions():
    """
    {
    "title": "test_sys_alter_schema_change.test_change_partition_actions",
    "describe": "test_change_partition_actions,修改表的 bloom filter 列,修改表的Colocate 属性,表的分桶方式由 Random Distribution 改为 Hash Distribution",
    "tag": "function,p1"
    }
    """
    """
    test_change_partition_actions
    以下测试的schema change的方法与表的聚合结构无关，所以没有区分，统一放在这里
    修改表的 bloom filter 列
    修改表的Colocate 属性
    将表的分桶方式由 Random Distribution 改为 Hash Distribution
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    check(table_name)

    # 可以修改bloom filter列
    util.assert_return(True, '',
                       client.schema_change, table_name, bloom_filter_column_list=['k1', 'k2'],
                       is_wait=True)
    time.sleep(10)
    # 修改表的Colocate 属性
    # alter collocate_with 只修改元数据，不产生实际的alter job
    util.assert_return(True, '',
                       client.schema_change, table_name, colocate_with_list='k1',
                       # client.schema_change, table_name, colocate_with_list=['k1'],
                       is_wait=True)
    # 将表的分桶方式由 Random Distribution 改为 Hash Distribution
    # Random 已经不再支持了，这个功能只是为何把之前的random转换成hash用的,不做测试
    # util.assert_return(True, '',
    #                    client.schema_change, table_name, distribution_type=['hash'],
    #                    is_wait=True)

    client.clean(database_name)


def teardown_module():
    """
    tearDown
    """
    pass


if __name__ == '__main__':
    import pdb
    pdb.set_trace()
    setup_module()
    test_change_partition_actions()
