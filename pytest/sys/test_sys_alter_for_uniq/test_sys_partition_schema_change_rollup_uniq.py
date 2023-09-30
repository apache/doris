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
    client.create_table(table_name, DATA.schema_1_uniq, \
            partition_info, distribution_info, keys_desc=DATA.key_1_uniq)
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


def test_change_partition_rollup():
    """
    {
    "title": "test_sys_partition_schema_change_rollup_uniq.test_change_partition_rollup",
    "describe": "即给任意 rollup 增加的列，都会自动加入到 Base 表中。同时，不允许向 Rollup 中加入 Base 表已经存在的列。如果用户需要这样做，可以重新建立一个包含新增列的 Rollup，之后再删除原 Rollup。",
    "tag": "function,p1,fuzz"
    }
    """
    """
    即给任意 rollup 增加的列，都会自动加入到 Base 表中。
    同时，不允许向 Rollup 中加入 Base 表已经存在的列。
    如果用户需要这样做，可以重新建立一个包含新增列的 Rollup，之后再删除原 Rollup。
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    index_name_b = index_name + '_b'
    index_name_c = index_name + '_c'
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    check(table_name)

    util.assert_return(False, 'Rollup should contains all unique keys in basetable',
                       client.create_rollup_table, table_name, index_name,
                       DATA.rollup_field_list_1, is_wait=True)

    idx_c = ['k2', 'k1', 'v1']
    util.assert_return(True, None,
                       client.create_rollup_table, table_name, index_name,
                       idx_c, is_wait=True)

    util.assert_return(True, '',
                       client.schema_change, table_name,
                       add_column_list=['v4 float NULL to ' + index_name],
                       is_wait=True)

    assert client.get_column('v4', table_name, database_name)

    util.assert_return(False, 'Can not add column which already exists in base table: v2',
                       client.schema_change, table_name,
                       add_column_list=['v2 float NULL to ' + index_name],
                       is_wait=True)
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

