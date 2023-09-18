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
#   @file test_sys_partition_complex_without_restart_be.py
#   @date 2015/05/13 15:26:21
#   @brief This file is a test file for palo data loading and verifying.
#   
#############################################################################

"""
复合分区测试设计建表部分测试
"""

import time
import threading
import pytest

from data import partition as DATA
from lib import palo_config
from lib import palo_client
from lib import palo_task
from lib import util

LOG = palo_client.LOG
L = palo_client.L

config = palo_config.config
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


def __test_multi_tables_table_b(client):
    #创建表b，单分区表
    table_name_b = 'table_b'
    client.create_table(table_name_b, DATA.schema_1, \
            distribution_info=palo_client.DistributionInfo('HASH(k2)', 10)) 
    #验证
    assert client.show_tables(table_name_b)
    check_partition_list(table_name_b, [table_name_b])

    #向表b导入
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name_b)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    #数据校验
    assert client.verify(DATA.expected_data_file_list_1, table_name_b)

    #schema change
    column_name_list = ['k2', 'k3', 'k1', 'k5', 'k4', 'v5', 'v3', 'v2', 'v6', 'v4', 'v1']
    assert client.schema_change_order_column(table_name_b, \
            column_name_list, is_wait_job=True, is_wait_delete_old_schema=True)


def __test_multi_tables_table_c(client, partition_info, distribution_info, \
        partition_name_list, index_name):
    #创建表c，复合分区表
    table_name_c = 'table_c'
    client.create_table(table_name_c, DATA.schema_1, \
            partition_info, distribution_info)
    #验证
    assert client.show_tables(table_name_c)
    check_partition_list(table_name_c, partition_name_list)
    #创建上卷表, 非法
    ret = False
    try:
        ret = client.create_rollup_table(table_name_c, \
                index_name, ['k1', 'k2', 'k5', 'v1', 'v3'], is_wait=True)
    except Exception as e:
        print(str(e))
        pass
    assert not ret
    #创建上卷表
    assert client.create_rollup_table(table_name_c, \
            index_name, ['k1', 'k2', 'k5', 'v4', 'v5'], is_wait=True)

    #向表c导入
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name_c)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    #数据校验
    assert client.verify(DATA.expected_data_file_list_1, table_name_c)

    #delete
    assert client.delete(table_name_c, [('k2', '<', '300'),], \
            partition_name=partition_name_list[0])

    #schema change
    assert client.schema_change_drop_column(table_name_c, \
            ['v4', 'v5'], from_table_name=index_name, \
            is_wait_job=True, is_wait_delete_old_schema=True)


def __test_multi_tables_table_d(client):
    #创建表d，复合分区表
    table_name_d = 'table_d'
    partition_name_list_d = ['partition_e', 'partition_b', 'partition_g', 'partition_h']
    partition_value_list_d = ['100', '500', '1000', 'MAXVALUE']
    partition_info_d = palo_client.PartitionInfo('k3', \
            partition_name_list_d, partition_value_list_d)
    distribution_type_d = 'HASH(k1, k2, k5)'
    distribution_info_d = palo_client.DistributionInfo(distribution_type_d, 171) 
    client.create_table(table_name_d, DATA.schema_1, \
            partition_info_d, distribution_info_d)
    #验证
    assert client.show_tables(table_name_d)
    check_partition_list(table_name_d, partition_name_list_d)

    #向表d导入
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name_d)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    #数据校验
    assert client.verify(DATA.expected_data_file_list_1, table_name_d)


def __test_multi_tables_table_e(client):
    #创建表e，单分区表, schema不同
    table_name_e = 'table_e'
    client.create_table(table_name_e, DATA.schema_2)
    assert client.show_tables(table_name_e)
    check_partition_list(table_name_e, [table_name_e])

    #向表e导入
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_2, table_name_e)
    assert client.batch_load(util.get_label(), data_desc_list, max_filter_ratio='0.5', is_wait=True, 
                             broker=broker_info)
    #数据校验
    assert client.verify(DATA.expected_data_file_list_2, table_name_e, encoding='utf-8')


def test_multi_tables():
    """
    同一DB中，有多个表，有单分区表，有多分区表
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', partition_name_list, partition_value_list)
    distribution_type = 'HASH(k1)'
    distribution_info = palo_client.DistributionInfo(distribution_type, 13) 

    #创建表1，复合分区表
    client.create_table(table_name, DATA.schema_1, \
            partition_info, distribution_info)
    #验证
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    #向表1导入
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    #数据校验
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    #创建上卷表
    assert client.create_rollup_table(table_name, \
            index_name, ['k1', 'k2', 'v6'], is_wait=True)

    __test_multi_tables_table_b(client)

    __test_multi_tables_table_c(client, partition_info, distribution_info, \
            partition_name_list, index_name)

    __test_multi_tables_table_d(client)

    __test_multi_tables_table_e(client)
    client.clean(database_name)

 
@pytest.mark.skip()
def test_add_partition_while_loading():
    """
    增加分区时，并行线程进行导入操作
    测试步骤：
    1. 启动一个线程持续进行导入任务
    2. 等到有任务进入loading状态、add partition
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '25', '29']
    partition_info = palo_client.PartitionInfo('k1', partition_name_list, partition_value_list)
    client.create_table(table_name, DATA.schema_1, partition_info)
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, max_filter_ratio='0.5', is_wait=True,
                             broker=broker_info)

    load_label=table_name
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    load_task = palo_task.BatchLoadTask(config.fe_host, config.fe_query_port, database_name, \
            load_label, data_desc_list, max_filter_ratio='0.5', is_wait=False, interval=10)
    load_thread = palo_task.TaskThread(load_task)
    load_thread.start()

    #等到有任务进入loading 状态
    while not client.get_load_job_list(state='LOADING'):
        time.sleep(1)
    load_thread.stop()

    assert client.add_partition(table_name, 'partition_e', '30')
    assert client.add_partition(table_name, 'partition_f', '31')
    assert client.add_partition(table_name, 'partition_g', '32')
    assert client.add_partition(table_name, 'partition_h', '33')
    assert client.add_partition(table_name, 'partition_i', '34')
    client.clean(database_name)

 
def test_drop_partition_while_loading():
    """
    删除分区时，并行线程进行导入操作
    测试步骤：
    1. 启动一个线程持续进行导入任务
    2. 等到有任务进入loading状态、add partition
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d', \
            'partition_e', 'partition_f', 'partition_g', 'partition_h', 'partition_i']
    partition_value_list = ['5', '20', '25', '30', '50', '60', '70', '80', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', partition_name_list, partition_value_list)
    client.create_table(table_name, DATA.schema_1, partition_info)
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, max_filter_ratio='0.5', is_wait=True, 
                             broker=broker_info)

    load_label=table_name
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    load_task = palo_task.BatchLoadTask(config.fe_host, config.fe_query_port, database_name, \
            load_label, data_desc_list, max_filter_ratio='0.5', is_wait=False, interval=10)
    load_thread = palo_task.TaskThread(load_task)
    load_thread.start()

    #等到有任务进入loading 状态
    timeout = 300
    while not client.get_load_job_list(state='LOADING') and timeout > 0:
        time.sleep(1)
        timeout -= 1
    if timeout == 0:
        client.clean(database_name)
        raise pytest.skip('cant not get LOADING state')
    load_thread.stop()

    assert client.drop_partition(table_name, 'partition_e')
    assert client.drop_partition(table_name, 'partition_f')
    assert client.drop_partition(table_name, 'partition_g')
    assert client.drop_partition(table_name, 'partition_h')
    assert client.drop_partition(table_name, 'partition_i')
    client.clean(database_name)


def test_select_while_loading():
    """
    查询同时导入
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', partition_name_list, partition_value_list)
    distribution_type = 'RANDOM'
    distribution_info = palo_client.DistributionInfo(distribution_type, 13) 

    #创建复合分区表
    client.create_table(table_name, DATA.schema_1, \
            partition_info, distribution_info)
    #验证
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    #导入
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    label = util.get_label()
    client.batch_load(label, data_desc_list, broker=broker_info)
    timeout=300
    while not client.get_load_job_list(state='LOADING') and timeout > 0:
        time.sleep(1)
        timeout -= 1
    if timeout == 0:
        client.clean(database_name)
        raise pytest.skip('can not get LOADING state')
    #数据校验，load导入时间变短，最终导入结果不确认
    # assert client.verify(DATA.expected_data_file_list_1, table_name)
    client.clean(database_name)


def test_select_while_rollup():
    """
    查询同时rollup
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', partition_name_list, partition_value_list)
    distribution_type = 'RANDOM'
    distribution_info = palo_client.DistributionInfo(distribution_type, 13) 

    #创建复合分区表
    client.create_table(table_name, DATA.schema_1, \
            partition_info, distribution_info)
    #验证
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    #导入
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)

    #创建上卷表
    assert client.create_rollup_table(table_name, index_name, ['k1', 'k2', 'v6'])

    #数据校验
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    assert client.cancel_rollup(table_name)
    client.clean(database_name)


def test_select_while_delete():
    """
    查询同时delete
    """
    class DeleteThread(threading.Thread):
        """
        Delete线程
        """
        def __init__(self, database_name, table_name, \
                delete_condition_list, partition_name):
            threading.Thread.__init__(self)
            self.database_name = database_name
            self.table_name = table_name
            self.delete_condition_list = delete_condition_list
            self.partition_name = partition_name

        def run(self):
            """
            run
            """
            thread_client = palo_client.PaloClient(palo_config.config.fe_host, \
                    palo_config.config.fe_query_port, self.database_name)
            thread_client.init()
            count = 0
            while count < 100:
                thread_client.delete(self.table_name, \
                    self.delete_condition_list, self.partition_name)
                count = count + 1
                time.sleep(1)

    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', partition_name_list, partition_value_list)
    distribution_type = 'RANDOM'
    distribution_info = palo_client.DistributionInfo(distribution_type, 13) 

    #创建复合分区表
    client.create_table(table_name, DATA.schema_1, \
            partition_info, distribution_info)
    #验证
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    #导入
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)

    #创建并启动delete线程
    delete_thread = DeleteThread(database_name, table_name, \
            [('k1', '<', '-5'),], 'partition_a')
    delete_thread.start()

    #数据校验
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    delete_thread.join()
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

