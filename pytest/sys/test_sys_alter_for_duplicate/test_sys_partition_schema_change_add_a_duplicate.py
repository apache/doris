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
  * @file test_sys_partition_schema_change.py
  * @date 2015/02/04 15:26:21
  * @brief This file is a test file for Palo schema changing.
  * 
  **************************************************************************/
"""

import random

from data import schema_change as DATA
from lib import palo_config
from lib import palo_client
from lib import util

config = palo_config.config
LOG = palo_client.LOG
L = palo_client.L
broker_info = palo_config.broker_info


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
    client.create_table(table_name, DATA.schema_1_dup, \
            partition_info, distribution_info, keys_desc=DATA.key_1_dup)
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)


def check(table_name):
    """
    分区，检查
    """
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d', \
            'partition_e', 'partition_f', 'partition_g']
    partition_value_list = ['5', '30', '100', '500', '1000', '2000', 'MAXVALUE']
    partition_check(table_name, 'k1', \
            partition_name_list, partition_value_list, \
            'HASH(k1, k2)', random.randrange(1, 30), 'column')


def test_add_key_mid():
    """
    {
    "title": "test_sys_partition_schema_change_add_a_duplicate.test_add_key_mid",
    "describe": "在key中间新加一列",
    "tag": "function,p1"
    }
    """
    """
    在key中间新加一列
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    check(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_1, table_name)
    assert ret

    column_list = [('k3', 'INT KEY', None, '5')]
    ret = client.schema_change_add_column(table_name, column_list, \
            after_column_name='k1', is_wait_job=True, is_wait_delete_old_schema=True)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_2, table_name)
    assert ret
    client.clean(database_name)


def test_add_key_tail():
    """
    {
    "title": "test_sys_partition_schema_change_add_a_duplicate.test_add_key_tail",
    "describe": "在key后边新加一列,schema change前后增减分区",
    "tag": "function,p1"
    }
    """
    """
    在key后边新加一列
    schema change前后增减分区
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d', \
            'partition_e', 'partition_f', 'partition_g']
    partition_value_list = ['5', '30', '100', '500', '1000', '2000', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(k1, k2)', 13)
    client.create_table(table_name, DATA.schema_1, \
            partition_info, distribution_info)
    assert client.show_tables(table_name)

    client.drop_partition(table_name, 'partition_g')
    client.add_partition(table_name, 'partition_h', '3500')

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_1, table_name)
    assert ret

    column_list = [('k3', 'INT KEY', None, '5')]
    ret = client.schema_change_add_column(table_name, column_list, \
            after_column_name='k2', is_wait_job=True, is_wait_delete_old_schema=True)
    assert ret

    client.add_partition(table_name, 'partition_i', '4500')
    client.drop_partition(table_name, 'partition_i')

    ret = client.verify(DATA.expected_data_file_list_3, table_name)
    assert ret
    client.clean(database_name)


def test_add_value():
    """
    {
    "title": "test_sys_partition_schema_change_add_a_duplicate.test_add_value",
    "describe": "新增加一个value列",
    "tag": "function,p1"
    }
    """
    """
    新增加一个value列
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    check(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_1, table_name)
    assert ret

    column_list = [('v4', 'INT', None, '5')]
    ret = client.schema_change_add_column(table_name, column_list,
                                          after_column_name='v2', is_wait_job=True,
                                          is_wait_delete_old_schema=True)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_4, table_name)
    assert ret
    client.clean(database_name)


def test_add_v4_v5():
    """
    {
    "title": "test_sys_partition_schema_change_add_a_duplicate.test_add_v4_v5",
    "describe": "增加多个value列, v4 v5",
    "tag": "function,p1"
    }
    """
    """
    新增加多个value列, v4 v5
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    check(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_1, table_name)
    assert ret

    column_list = [('v4', 'INT', None, '7'), ('v5', 'INT', None, '9')]
    ret = client.schema_change_add_column(table_name, column_list,
                                          is_wait_job=True, is_wait_delete_old_schema=True)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_5, table_name)
    assert ret
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

