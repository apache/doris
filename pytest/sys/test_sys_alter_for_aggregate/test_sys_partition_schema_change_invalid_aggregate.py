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

import sys
import random

sys.path.append("../")
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


def test_change_partition_invalid():
    """
    {
    "title": "test_sys_partition_schema_change_invalid_aggregate.test_change_partition_invalid",
    "describe": "一张表在同一时间只能有一个 Schema Change 作业在运行。分区列和分桶列不能修改。不支持修改列名称、聚合类型、Nullable 属性、默认值以及列注释。",
    "tag": "function,p1,fuzz"
    }
    """
    """
    一张表在同一时间只能有一个 Schema Change 作业在运行。
    分区列和分桶列不能修改。
    不支持修改列名称、聚合类型、Nullable 属性、默认值以及列注释。
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    check(table_name)

    # 分区列和分桶列不能修改
    util.assert_return(False, 'Can not modify partition column',
                       client.schema_change_modify_column, table_name, 'k1', 'BIGINT NULL',
                       is_wait_job=True, is_wait_delete_old_schema=True)

    # 不支持修改列名称、聚合类型、Nullable 属性、默认值
    util.assert_return(False, 'Cannot shorten string length',
                       client.schema_change_modify_column, table_name, 'v1', 'varchar(2048)',
                       aggtype='REPLACE', column_info='NOT NULL',
                       is_wait_job=True, is_wait_delete_old_schema=True)

    util.assert_return(True, '',
                       client.schema_change_modify_column, table_name, 'v2', 'float',
                       aggtype='SUM', column_info='NULL',
                       is_wait_job=True, is_wait_delete_old_schema=True)

    util.assert_return(False, 'Can not change from nullable to non-nullable',
                       client.schema_change_modify_column, table_name, 'v2', 'float',
                       aggtype='SUM', column_info='NOT NULL',
                       is_wait_job=True, is_wait_delete_old_schema=True)

    util.assert_return(False, 'Can not change aggregation type',
                       client.schema_change_modify_column, table_name, 'v2', 'float',
                       aggtype='REPLACE', column_info='NULL',
                       is_wait_job=True, is_wait_delete_old_schema=True)

    util.assert_return(False, 'Can not change default value',
                       client.schema_change_modify_column, table_name, 'v3', 'decimal(20, 7)',
                       aggtype='SUM', column_info='NOT NULL DEFAULT "0.1"',
                       is_wait_job=True, is_wait_delete_old_schema=True)

    # 一张表在同一时间只能有一个 Schema Change 作业在运行
    client.schema_change_modify_column(table_name, 'v1', 'varchar(8192)',
                                       aggtype='REPLACE', column_info='NOT NULL',
                                       is_wait_job=False, is_wait_delete_old_schema=False)
    util.assert_return(False, 'Do not allow doing ALTER ops',
                       client.schema_change_modify_column, table_name, 'v1', 'varchar(8196)',
                       aggtype='REPLACE', column_info='NOT NULL',
                       is_wait_job=True, is_wait_delete_old_schema=True)
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
