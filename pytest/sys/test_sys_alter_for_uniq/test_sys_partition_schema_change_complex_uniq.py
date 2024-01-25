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
import time
import random

sys.path.append("../")
from data import schema_change as DATA
from lib import palo_config
from lib import palo_client
from lib import palo_task
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
    partition_info = palo_client.PartitionInfo(column_name,
                                               partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo(distribution_type, bucket_num) 
    client.create_table(table_name, DATA.schema_1_uniq,
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
                    'HASH(K1, K2)', random.randrange(1, 30), 'column')


def test_selecting():
    """
    {
    "title": "test_sys_partition_schema_change_complex_uniq.test_selecting",
    "describe": "功能点：schema change不影响查询",
    "tag": "system,p1,stability"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)

    hash_distribution_info = palo_client.DistributionInfo(distribution_type=DATA.hash_partition_type, 
                                                          bucket_num=DATA.hash_partition_num)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d', 
                           'partition_e', 'partition_f', 'partition_g']
    partition_value_list = ['5', '30', '100', '500', '1000', '2000', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('bigint_key',
                                               partition_name_list, partition_value_list)
    ret = client.create_table(table_name, DATA.schema_uniq, partition_info, 
                              distribution_info=hash_distribution_info, keys_desc=DATA.key_uniq)
    assert ret

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path, table_name)
    label = "%s_1" % database_name
    ret = client.batch_load(label, data_desc_list, max_filter_ratio="0.5", is_wait=True, 
                            broker=broker_info)
    assert ret

    sql = "SELECT * FROM %s.%s" % (database_name, table_name)
    select_task = palo_task.SelectTask(config.fe_host, config.fe_query_port, sql, 
                                       database_name=database_name)
    select_thread = palo_task.TaskThread(select_task)
    select_thread.start()
    client.schema_change_drop_column(table_name, 
                                    column_name_list=DATA.drop_column_name_list_new)
    select_thread.stop()
    client.clean(database_name)

 
def test_loading():
    """
    {
    "title": "test_sys_partition_schema_change_complex_uniq.test_loading",
    "describe": "功能点：导入不影响schema change",
    "tag": "system,p1,stability"
    }
    """
    """
    功能点：导入不影响schema change
    测试步骤：
    1. 启动一个线程持续进行导入任务
    2. 等到有任务进入loading状态、做schema change
    验证：
    1. 导入一直正确
    2. schema change后数据正确
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d', 
                           'partition_e', 'partition_f', 'partition_g']
    partition_value_list = ['5', '30', '100', '500', '1000', '2000', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('bigint_key', 
                                               partition_name_list, partition_value_list)
    hash_distribution_info = palo_client.DistributionInfo(distribution_type=DATA.hash_partition_type, 
                                                          bucket_num=DATA.hash_partition_num)
    ret = client.create_table(table_name, DATA.schema_uniq, 
                              partition_info, distribution_info=hash_distribution_info, 
                              keys_desc=DATA.key_uniq)
    assert ret

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path, table_name)
    label = "%s_1" % database_name
    ret = client.batch_load(label, data_desc_list, max_filter_ratio="0.5", is_wait=True, 
                            broker=broker_info)
    assert ret

    load_task = palo_task.BatchLoadTask(config.fe_host, config.fe_query_port, database_name, 
                                        label, data_desc_list, max_filter_ratio="0.05", 
                                        is_wait=False, interval=10, broker=broker_info)
    load_thread = palo_task.TaskThread(load_task)
    load_thread.start()
    #等到有任务进入loading 状态
    timeout = 600
    while not client.get_load_job_list(state="LOADING") and timeout > 0:
        time.sleep(1)
        timeout -= 1
    client.schema_change_drop_column(table_name, 
                                     column_name_list=DATA.drop_column_name_list_new)
    time.sleep(10)
    load_thread.stop()
    #等到schema change完成
    client.wait_table_schema_change_job(table_name)
    #schema change完成后，没有未完成的导入任务
    timeout = 1200
    while client.get_unfinish_load_job_list() and timeout > 0:
        time.sleep(1)
        timeout -= 1
    client.clean(database_name)
 

def test_multi_schema_change_in_database():
    """
    {
    "title": "test_sys_partition_schema_change_complex_uniq.test_multi_schema_change_in_database",
    "describe": "功能点：相同的database中不同的table family同时进行schema change",
    "tag": "system,p1,stability"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d', 
                           'partition_e', 'partition_f', 'partition_g']
    partition_value_list = ['5', '30', '100', '500', '1000', '2000', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('bigint_key', 
                                                partition_name_list, partition_value_list)
    table_name_1 = "%s_1" % table_name
    hash_distribution_info = palo_client.DistributionInfo(distribution_type=DATA.hash_partition_type, 
                                                          bucket_num=DATA.hash_partition_num)
    ret = client.create_table(table_name_1, DATA.schema_uniq, 
                              partition_info, distribution_info=hash_distribution_info, 
                              keys_desc=DATA.key_uniq)
    assert ret

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path, table_name_1)
    label = table_name_1
    ret = client.batch_load(label, data_desc_list, max_filter_ratio="0.5", is_wait=True, 
                            broker=broker_info)
    assert ret

    table_name_2 = "%s_2" % table_name
    ret = client.create_table(table_name_2, DATA.schema_uniq,
                              partition_info, distribution_info=hash_distribution_info, 
                              keys_desc=DATA.key_uniq)
    assert ret

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path, table_name_2)
    label = table_name_2
    ret = client.batch_load(label, data_desc_list, max_filter_ratio="0.5", is_wait=True, 
                            broker=broker_info)
    assert ret

    timeout = 1200
    while client.get_unfinish_load_job_list() and timeout > 0:
        time.sleep(1)
        timeout -= 1

    assert client.schema_change_drop_column(table_name_1, DATA.drop_column_name_list_new)
    assert client.schema_change_drop_column(table_name_2, DATA.drop_column_name_list_new,
                                            is_wait_job=True)
    client.wait_table_schema_change_job(table_name_1)
    new_column_name_list = list(set(DATA.column_name_list).difference(set(DATA.drop_column_name_list))) 
    sql = "SELECT %s FROM %s.%s" % (", ".join(new_column_name_list), 
                                    database_name, table_name_1)

    data_1 = client.execute(sql)
    sql = "SELECT %s FROM %s.%s" % (", ".join(new_column_name_list), 
                                    database_name, table_name_1)
    data_2 = client.execute(sql)
    util.check(data_1, data_2, True)
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

