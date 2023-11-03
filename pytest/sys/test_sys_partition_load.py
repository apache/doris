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
#   @file test_sys_partition_load.py
#   @date 2015/05/13 15:26:21
#   @brief This file is a test file for palo data loading and verifying.
#   
#############################################################################

"""
复合分区，导入操作测试
"""

import time
import threading
import pytest
from data import partition as DATA
from data import timestamp as DATA_TS
from lib import palo_config
from lib import palo_client
from lib import palo_task
from lib import util

LOG = palo_client.LOG
L = palo_client.L
config = palo_config.config
broker_info = palo_config.broker_info


class LoadThread(threading.Thread):
    """
    导入线程
    """
    def __init__(self, database_name, data_desc_list, label, max_filter_ratio=None):
        threading.Thread.__init__(self)
        self.database_name = database_name
        self.data_desc_list = data_desc_list
        self.label = label
        self.max_filter_ratio = max_filter_ratio

    def run(self):
        """
        run
        """
        thread_client = palo_client.get_client(config.fe_host, config.fe_query_port, self.database_name,
                                               user=config.fe_user, password=config.fe_password, 
                                               http_port=config.fe_http_port)
        thread_client.batch_load(self.label, self.data_desc_list, broker=broker_info,
                                 max_filter_ratio=self.max_filter_ratio, is_wait=True)


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


def check_load_and_verify(table_name, load_partition_list=None):
    """
    验证表是否创建成功，分区是否创建成功，导入数据，校验
    """
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, \
            table_name, load_partition_list)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(list(DATA.expected_data_file_list_1) * 2, table_name)


def partition_check(table_name, column_name, partition_name_list, \
        partition_value_list, distribution_type, bucket_num, storage_type):
    """
    检查，验证
    """
    partition_info = palo_client.PartitionInfo(column_name, \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo(distribution_type, bucket_num) 
    client.create_table(table_name, DATA.schema_1, partition_info, 
            distribution_info, keys_desc='AGGREGATE KEY (k1, k2, k3, k4, k5)')
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)


def test_load_without_partition():
    """
    {
    "title": "test_sys_partition_load.test_load_without_partition",
    "describe": "导入，不指定分区, 带rollup表",
    "tag": "system,p1"
    }
    """
    """
    导入，不指定分区, 带rollup表
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']

    partition_check(table_name, 'k1', \
            partition_name_list, partition_value_list, \
            'RANDOM', 13, 'row')

    column_name_list = ['k1', 'k5', 'v4', 'v5']
    assert client.create_rollup_table(table_name, \
            index_name, column_name_list, is_wait=True)

    check_load_and_verify(table_name)
    client.clean(database_name)


def test_load_with_partition():
    """
    {
    "title": "test_sys_partition_load.test_load_with_partition",
    "describe": "导入，指定分区",
    "tag": "system,p1"
    }
    """
    """
    导入，指定分区
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '31', 'MAXVALUE']

    partition_check(table_name, 'k1', \
            partition_name_list, partition_value_list, \
            'RANDOM', 13, 'row')
    check_load_and_verify(table_name, ['partition_a', 'partition_b', 'partition_c'])
    client.clean(database_name)


def test_multi_loading():
    """
    {
    "title": "test_sys_partition_load.test_multi_loading",
    "describe": "并行导入",
    "tag": "system,p1"
    }
    """
    """
    并行导入
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '31', 'MAXVALUE']

    partition_check(table_name, 'k1', \
            partition_name_list, partition_value_list, \
            'RANDOM', 13, 'row')

    data_desc_list_1 = palo_client.LoadDataInfo(DATA.file_path_1, \
            table_name, ['partition_a', 'partition_b', 'partition_c'])
    data_desc_list_2 = palo_client.LoadDataInfo(DATA.file_path_1, \
            table_name, ['partition_a', 'partition_b', 'partition_c', 'partition_d'])
    data_desc_list_3 = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    data_desc_list_4 = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    laod_thread_1 = LoadThread(database_name, data_desc_list_1, 'label_1')
    laod_thread_2 = LoadThread(database_name, data_desc_list_2, 'label_2')
    laod_thread_3 = LoadThread(database_name, data_desc_list_3, 'label_3')
    laod_thread_4 = LoadThread(database_name, data_desc_list_4, 'label_4')
    laod_thread_1.start()
    laod_thread_2.start()
    laod_thread_3.start()
    laod_thread_4.start()
    laod_thread_1.join()
    laod_thread_2.join()
    laod_thread_3.join()
    laod_thread_4.join()

    assert client.verify(list(DATA.expected_data_file_list_1) * 4, table_name)
    client.clean(database_name)


def test_multi_loading_for_one_database():
    """
    {
    "title": "test_sys_partition_load.test_multi_loading_for_one_database",
    "describe": "同一库中的单分区表和复合分区表同时进行导入, #相同label导两次，失败",
    "tag": "system,p1,fuzz"
    }
    """
    """
    1. 同一库中的单分区表和复合分区表同时进行导入
    2. #相同label导两次，失败
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    single_table_name = 'table_1'
    client.create_table(single_table_name, DATA.schema_1)
    assert client.show_tables(single_table_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '31', 'MAXVALUE']

    partition_check(table_name, 'k1', \
            partition_name_list, partition_value_list, \
            'RANDOM', 13, 'row')

    data_desc_list_1 = palo_client.LoadDataInfo(DATA.file_path_1, single_table_name)
    data_desc_list_2 = palo_client.LoadDataInfo(DATA.file_path_1, \
            table_name, ['partition_a', 'partition_b', 'partition_c', 'partition_d'])
    laod_thread_1 = LoadThread(database_name, data_desc_list_1, 'label_1')
    laod_thread_2 = LoadThread(database_name, data_desc_list_2, 'label_2')
    laod_thread_1.start()
    laod_thread_2.start()
    laod_thread_1.join()
    laod_thread_2.join()
    assert client.verify(list(DATA.expected_data_file_list_1), single_table_name)
    assert client.verify(list(DATA.expected_data_file_list_1), table_name)

    #相同label导两次，失败
    ret = False
    try:
        ret = client.batch_load('label_2', data_desc_list_2, is_wait=True, broker=broker_info)
    except:
        pass
    assert not ret
    client.clean(database_name)


def test_cancel_load():
    """
    {
    "title": "test_sys_partition_load.test_cancel_load",
    "describe": "取消导入",
    "tag": "system,p1,fuzz"
    }
    """
    """
    取消导入
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '31', 'MAXVALUE']

    partition_check(table_name, 'k1', \
            partition_name_list, partition_value_list, \
            'RANDOM', 13, 'row')

    data_desc_list_1 = palo_client.LoadDataInfo(DATA.file_path_1, \
            table_name, ['partition_a', 'partition_b', 'partition_c'])
    data_desc_list_2 = palo_client.LoadDataInfo(DATA.file_path_1, \
            table_name, ['partition_a', 'partition_b', 'partition_c', 'partition_d'])
    data_desc_list_3 = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    data_desc_list_4 = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    laod_thread_1 = LoadThread(database_name, data_desc_list_1, 'label_1')
    laod_thread_2 = LoadThread(database_name, data_desc_list_2, 'label_2')
    laod_thread_3 = LoadThread(database_name, data_desc_list_3, 'label_3')
    laod_thread_4 = LoadThread(database_name, data_desc_list_4, 'label_4')
    laod_thread_1.start()
    laod_thread_2.start()
    laod_thread_3.start()
    laod_thread_4.start()
    time.sleep(1)
    try:
        client.cancel_load('label_2')
        client.cancel_load('label_3')
    except Exception as e:
        print(str(e))
        raise pytest.skip('can not cancel load')
    finally:
        laod_thread_1.join()
        laod_thread_2.join()
        laod_thread_3.join()
        laod_thread_4.join()

    assert client.verify(list(DATA.expected_data_file_list_1) * 2, table_name)
    client.clean(database_name)

 
def test_add_partition_while_loading():
    """
    {
    "title": "test_sys_partition_load.test_add_partition_while_loading",
    "describe": "导入过程中，增加分区",
    "tag": "system,p1"
    }
    """
    """
    导入过程中，增加分区
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '31', '40']

    partition_check(table_name, 'k1', \
            partition_name_list, partition_value_list, \
            'RANDOM', 13, 'row')

    data_desc_list_1 = palo_client.LoadDataInfo(DATA.file_path_1, \
            table_name, ['partition_a', 'partition_b', 'partition_c'])
    data_desc_list_2 = palo_client.LoadDataInfo(DATA.file_path_1, \
            table_name, ['partition_a', 'partition_b', 'partition_c', 'partition_d'])
    data_desc_list_3 = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    data_desc_list_4 = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    laod_thread_1 = LoadThread(database_name, data_desc_list_1, 'label_1')
    laod_thread_2 = LoadThread(database_name, data_desc_list_2, 'label_2')
    laod_thread_3 = LoadThread(database_name, data_desc_list_3, 'label_3')
    laod_thread_4 = LoadThread(database_name, data_desc_list_4, 'label_4')
    laod_thread_1.start()
    client.add_partition(table_name, 'partition_e', '50')
    laod_thread_2.start()
    timeout = 30
    while not client.get_load_job_list(state="LOADING") and timeout > 0:
        time.sleep(1)
        timeout -= 1
    client.add_partition(table_name, 'partition_f', '60')
    laod_thread_3.start()
    client.add_partition(table_name, 'partition_g', '70')
    laod_thread_4.start()
    client.add_partition(table_name, 'partition_h', '80')
    laod_thread_1.join()
    laod_thread_2.join()
    laod_thread_3.join()
    laod_thread_4.join()

    assert client.verify(list(DATA.expected_data_file_list_1) * 4, table_name)
    add_partition_list = ['partition_e', 'partition_f', 'partition_g', 'partition_h']
    #FIXME
    #check_partition_list(table_name, partition_name_list.extend(add_partition_list))
    check_partition_list(table_name, partition_name_list + add_partition_list)
    client.clean(database_name)

 
def test_load_while_selecting():
    """
    {
    "title": "test_sys_partition_load.test_load_while_selecting",
    "describe": "查询的同时导入",
    "tag": "system,p1"
    }
    """
    """
    查询的同时导入
    """

    class SelectTask(palo_task.PaloTask):
        """
        导入线程
        """
        def __init__(self, database_name, table_name):
            self.database_name = database_name
            self.table_name = table_name
            self.thread_client = palo_client.PaloClient(palo_config.config.fe_host, \
                    palo_config.config.fe_query_port, database_name)
            self.thread_client.init()

        def do_task(self):
            """
            run
            """
            self.thread_client.select_all(self.table_name)

    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '31', 'MAXVALUE']

    partition_check(table_name, 'k1', \
            partition_name_list, partition_value_list, \
            'RANDOM', 13, 'row')

    data_desc_list_1 = palo_client.LoadDataInfo(DATA.file_path_1, \
            table_name, ['partition_a', 'partition_b', 'partition_c'])
    data_desc_list_2 = palo_client.LoadDataInfo(DATA.file_path_1, \
            table_name, ['partition_a', 'partition_b', 'partition_c', 'partition_d'])
    data_desc_list_3 = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    data_desc_list_4 = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    laod_thread_1 = LoadThread(database_name, data_desc_list_1, 'label_1')
    laod_thread_2 = LoadThread(database_name, data_desc_list_2, 'label_2')
    laod_thread_3 = LoadThread(database_name, data_desc_list_3, 'label_3')
    laod_thread_4 = LoadThread(database_name, data_desc_list_4, 'label_4')
    laod_thread_1.start()
    laod_thread_2.start()
    timeout = 30
    while not client.get_load_job_list(state="LOADING") and timeout > 0:
        time.sleep(1)
        timeout -= 1
    select_task = SelectTask(database_name, table_name)
    select_thread = palo_task.TaskThread(select_task)
    select_thread.start()
    laod_thread_3.start()
    laod_thread_4.start()
    laod_thread_1.join()
    laod_thread_2.join()
    laod_thread_3.join()
    laod_thread_4.join()
    select_thread.stop()

    assert client.verify(list(DATA.expected_data_file_list_1) * 4, table_name)
    client.clean(database_name)

 
def test_load_while_deleting():
    """
    {
    "title": "test_sys_partition_load.test_load_while_deleting",
    "describe": "同时向不同的分区导入和删除",
    "tag": "system,p1,fuzz"
    }
    """
    """
    同时向不同的分区导入和删除
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '31', 'MAXVALUE']

    partition_check(table_name, 'k1', \
            partition_name_list, partition_value_list, \
            'RANDOM', 13, 'row')

    data_desc_list_1 = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list_1, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    data_desc_list_2 = palo_client.LoadDataInfo(DATA.file_path_1, \
            table_name, ['partition_b', 'partition_c'])
    laod_thread_1 = LoadThread(database_name, data_desc_list_2, 'label_1', max_filter_ratio='0.5')
    laod_thread_1.start()
    timeout = 30
    while not client.get_load_job_list(state="LOADING") and timeout > 0:
        time.sleep(1)
        timeout -= 1
    client.delete(table_name, [('k1', '<', '5'),], 'partition_a')
    laod_thread_1.join()

    assert client.verify([DATA.expected_file_10] * 2, table_name)
    client.clean(database_name)

 
def test_load_while_deleting_to_same_partition():
    """
    {
    "title": "test_sys_partition_load.test_load_while_deleting_to_same_partition",
    "describe": "同时向相同的分区导入和删除",
    "tag": "system,p1,fuzz"
    }
    """
    """
    同时向相同的分区导入和删除
    流式导入一期：支持同时向相同的分区导入和删除
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '31', 'MAXVALUE']

    partition_check(table_name, 'k1', \
            partition_name_list, partition_value_list, \
            'RANDOM', 13, 'row')

    data_desc_list_1 = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list_1, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    data_desc_list_2 = palo_client.LoadDataInfo(DATA.file_path_1, \
            table_name, ['partition_b', 'partition_c'])
    load_thread_1 = LoadThread(database_name, data_desc_list_2, 'label_1', max_filter_ratio='0.5')
    load_thread_1.start()
    timeout = 30
    while not client.get_load_job_list(state="LOADING") and timeout > 0:
        time.sleep(1)
        timeout -= 1
    ret = False
    try:
        ret = client.delete(table_name, [('k1', '<', '20'),], 'partition_b')
    except:
        pass
    load_thread_1.join()
    # assert not ret
    assert ret
    client.clean(database_name)


def test_load_set_with_partition():
    """
    {
    "title": "test_sys_partition_load.test_load_set_with_partition",
    "describe": "SET, 导入，不指定分区",
    "tag": "system,p1,fuzz"
    }
    """
    """
    SET, 导入，不指定分区
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['1900-01-01 23:59:59', '2000-01-01 00:00:00', \
            '2099-12-21 23:59:59', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13) 
    client.create_table(table_name, DATA_TS.schema_1, \
                        partition_info, distribution_info)
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    set_list = ['k1 = strftime("%Y-%m-%d %H:%M:%S", tmp_k1)', \
                'k3 = strftime("%Y-%m-%d %H:%M:%S", tmp_k3)', \
                'k4 = strftime("%Y-%m-%d %H:%M:%S", tmp_k4)', \
                'v2 = strftime("%Y-%m-%d %H:%M:%S", tmp_v2)', \
                'v4 = strftime("%Y-%m-%d %H:%M:%S", tmp_v4)', \
                'v6 = strftime("%Y-%m-%d %H:%M:%S", tmp_v6)', \
                'v7 = strftime("%Y-%m-%d %H:%M:%S", tmp_v7)']

    column_name_list = ['tmp_k1', 'k2', 'tmp_k3', 'tmp_k4', \
            'v1', 'tmp_v2', 'v3', 'tmp_v4', 'v5', 'tmp_v6', 'tmp_v7']
    data_desc_list = palo_client.LoadDataInfo(DATA_TS.file_path_1, \
            table_name, column_name_list=column_name_list, set_list=set_list)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=0.05)
    assert client.verify(DATA_TS.expected_data_file_list_1, table_name)
    client.clean(database_name)


def test_load_set_with_null_data():
    """
    {
    "title": "test_sys_partition_load.test_load_set_with_null_data",
    "describe": "SET, 导入，有空数据",
    "tag": "system,p1,fuzz"
    }
    """
    """
    SET, 导入，有空数据
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['1900-01-01 23:59:59', '2000-01-01 00:00:00', \
            '2099-12-21 23:59:59', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13) 
    client.create_table(table_name, DATA_TS.schema_1, \
            partition_info, distribution_info)
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    set_list = ['k1 = strftime("%Y-%m-%d %H:%M:%S", tmp_k1)', \
                'k3 = strftime("%Y-%m-%d %H:%M:%S", tmp_k3)', \
                'k4 = strftime("%Y-%m-%d %H:%M:%S", tmp_k4)', \
                'v2 = strftime("%Y-%m-%d %H:%M:%S", tmp_v2)', \
                'v4 = strftime("%Y-%m-%d %H:%M:%S", tmp_v4)', \
                'v6 = strftime("%Y-%m-%d %H:%M:%S", tmp_v6)', \
                'v7 = strftime("%Y-%m-%d %H:%M:%S", tmp_v7)']

    column_name_list = ['tmp_k1', 'k2', 'tmp_k3', 'tmp_k4', \
            'v1', 'tmp_v2', 'v3', 'tmp_v4', 'v5', 'tmp_v6', 'tmp_v7']
    data_desc_list = palo_client.LoadDataInfo(DATA_TS.file_path_2, \
            table_name, column_name_list=column_name_list, set_list=set_list)
    assert client.batch_load(util.get_label(), data_desc_list, \
            max_filter_ratio='0.05', is_wait=True, broker=broker_info)
    assert client.verify(DATA_TS.expected_data_file_list_1, table_name)
    client.clean(database_name)


def test_load_set_out_of_range():
    """
    {
    "title": "test_sys_partition_load.test_load_set_out_of_range",
    "describe": "SET, 导入，有超过范围的数据",
    "tag": "system,p1,fuzz"
    }
    """
    """
    SET, 导入，有超过范围的数据
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['1900-01-01 23:59:59', '2000-01-01 00:00:00', \
            '2099-12-21 23:59:59', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13) 
    client.create_table(table_name, DATA_TS.schema_1, \
            partition_info, distribution_info)
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    set_list = ['k1 = strftime("%Y-%m-%d %H:%M:%S", tmp_k1)', \
                'k3 = strftime("%Y-%m-%d %H:%M:%S", tmp_k3)', \
                'k4 = strftime("%Y-%m-%d %H:%M:%S", tmp_k4)', \
                'v2 = strftime("%Y-%m-%d %H:%M:%S", tmp_v2)', \
                'v4 = strftime("%Y-%m-%d %H:%M:%S", tmp_v4)', \
                'v6 = strftime("%Y-%m-%d %H:%M:%S", tmp_v6)', \
                'v7 = strftime("%Y-%m-%d %H:%M:%S", tmp_v7)']

    column_name_list = ['tmp_k1', 'k2', 'tmp_k3', 'tmp_k4', \
            'v1', 'tmp_v2', 'v3', 'tmp_v4', 'v5', 'tmp_v6', 'tmp_v7']
    data_desc_list = palo_client.LoadDataInfo(DATA_TS.file_path_3, \
            table_name, column_name_list=column_name_list, set_list=set_list)
    assert client.batch_load(util.get_label(), data_desc_list, \
            max_filter_ratio='0.05', is_wait=True, broker=broker_info)
    assert client.verify(DATA_TS.expected_data_file_list_1, table_name)
    client.clean(database_name)


def test_load_set_align():
    """
    {
    "title": "test_sys_partition_load.test_load_set_align",
    "describe": "SET, 导入，对齐",
    "tag": "system,p1,fuzz"
    }
    """
    """
    SET, 导入，对齐
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['-300000', '0', '300000', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13) 
    assert client.create_table(table_name, DATA_TS.schema_2, \
            partition_info, distribution_info)
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    set_list = ['k1 = alignment_timestamp("hour", tmp_k1)', \
                'k3 = alignment_timestamp("day", tmp_k3)', \
                'k4 = alignment_timestamp("month", tmp_k4)', \
                'k5 = alignment_timestamp("year", tmp_k5)', \
                'v2 = alignment_timestamp("year", tmp_v2)', \
                'v4 = alignment_timestamp("month", tmp_v4)', \
                'v6 = alignment_timestamp("day", tmp_v6)', \
                'v7 = alignment_timestamp("hour", tmp_v7)']

    column_name_list = ['tmp_k1', 'k2', 'tmp_k3', 'tmp_k4', 'tmp_k5', \
            'v1', 'tmp_v2', 'v3', 'tmp_v4', 'v5', 'tmp_v6', 'tmp_v7']
    data_desc_list = palo_client.LoadDataInfo(DATA_TS.file_path_4, \
            table_name, column_name_list=column_name_list, set_list=set_list)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=0.05)
    assert client.verify(DATA_TS.expected_data_file_list_2, table_name)
    client.clean(database_name)


def load_set_align_out_range():
    """
    SET, 导入，对齐, 对于year month有超过范围数据
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['-300000', '0', '300000', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13) 
    assert client.create_table(table_name, DATA_TS.schema_2, \
            partition_info, distribution_info)
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    set_list = ['k1 = alignment_timestamp("hour", tmp_k1)', \
                'k3 = alignment_timestamp("day", tmp_k3)', \
                'k4 = alignment_timestamp("month", tmp_k4)', \
                'k5 = alignment_timestamp("year", tmp_k5)', \
                'v2 = alignment_timestamp("year", tmp_v2)', \
                'v4 = alignment_timestamp("month", tmp_v4)', \
                'v6 = alignment_timestamp("day", tmp_v6)', \
                'v7 = alignment_timestamp("hour", tmp_v7)']

    column_name_list = ['tmp_k1', 'k2', 'tmp_k3', 'tmp_k4', 'tmp_k5', \
            'v1', 'tmp_v2', 'v3', 'tmp_v4', 'v5', 'tmp_v6', 'tmp_v7']
    data_desc_list = palo_client.LoadDataInfo(DATA_TS.file_path_5, \
            table_name, column_name_list=column_name_list, set_list=set_list)
    assert client.batch_load(util.get_label(), data_desc_list, max_filter_ratio='0.5', is_wait=True, 
                             broker=broker_info)
    assert client.verify(DATA_TS.expected_data_file_list_2, table_name)
    client.clean(database_name)


def test_datetime_timestamp():
    """
    {
    "title": "test_sys_partition_load.test_datetime_timestamp",
    "describe": "datetime timestamp",
    "tag": "system,p1,fuzz"
    }
    """
    """
    datetime timestamp
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['1900-01-01 23:59:59', '2000-01-01 00:00:00', \
            '2099-12-21 23:59:59', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13) 
    assert client.create_table(table_name, DATA_TS.schema_3, \
            partition_info, distribution_info)
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    set_list = ['k1 = strftime("%Y-%m-%d %H:%M:%S", tmp_k1)', \
                'k3 = alignment_timestamp("hour", tmp_k3)', \
                'k4 = alignment_timestamp("day", tmp_k4)', \
                'k5 = strftime("%Y-%m-%d %H:%M:%S", tmp_k5)', \
                'k6 = alignment_timestamp("month", tmp_k6)', \
                'k7 = alignment_timestamp("year", tmp_k7)', \
                'k8 = strftime("%Y-%m-%d %H:%M:%S", tmp_k8)', \
                'v2 = strftime("%Y-%m-%d %H:%M:%S", tmp_v2)', \
                'v3 = alignment_timestamp("year", tmp_v3)', \
                'v4 = strftime("%Y-%m-%d %H:%M:%S", tmp_v4)', \
                'v5 = alignment_timestamp("month", tmp_v5)', \
                'v6 = alignment_timestamp("day", tmp_v6)', \
                'v7 = strftime("%Y-%m-%d %H:%M:%S", tmp_v7)', \
                'v8 = alignment_timestamp("hour", tmp_v8)', \
                'v9 = strftime("%Y-%m-%d %H:%M:%S", tmp_v9)']

    column_name_list = [\
            'tmp_k1', 'k2', 'tmp_k3', 'tmp_k4', 'tmp_k5', \
            'tmp_k6', 'tmp_k7', 'tmp_k8', \
            'v1', 'tmp_v2', 'tmp_v3', 'tmp_v4', 'tmp_v5', \
            'tmp_v6', 'tmp_v7', 'tmp_v8', 'tmp_v9']
    data_desc_list = palo_client.LoadDataInfo(DATA_TS.file_path_6, \
            table_name, column_name_list=column_name_list, set_list=set_list)
    assert client.batch_load(util.get_label(), data_desc_list,  \
            max_filter_ratio='1', is_wait=True, broker=broker_info)
    assert client.verify(DATA_TS.expected_data_file_list_3, table_name)
    client.clean(database_name)


def test_to_default_value_column():
    """
    {
    "title": "test_sys_partition_load.test_to_default_value_column",
    "describe": "函数的返回值列为schema中指定默认值的列",
    "tag": "system,p1,fuzz"
    }
    """
    """
    函数的返回值列为schema中指定默认值的列
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['1900-01-01 23:59:59', '2000-01-01 00:00:00', \
            '2099-12-21 23:59:59', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13) 
    assert client.create_table(table_name, DATA_TS.schema_4, \
            partition_info, distribution_info)
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    set_list = ['k1 = strftime("%Y-%m-%d %H:%M:%S", tmp_k1)', \
                'k3 = alignment_timestamp("hour", tmp_k3)', \
                'k4 = alignment_timestamp("day", tmp_k4)', \
                'k5 = strftime("%Y-%m-%d %H:%M:%S", tmp_k5)', \
                'k6 = alignment_timestamp("month", tmp_k6)', \
                'k7 = alignment_timestamp("year", tmp_k7)', \
                'k8 = strftime("%Y-%m-%d %H:%M:%S", tmp_k8)', \
                'v2 = strftime("%Y-%m-%d %H:%M:%S", tmp_v2)', \
                'v3 = alignment_timestamp("year", tmp_v3)', \
                'v4 = strftime("%Y-%m-%d %H:%M:%S", tmp_v4)', \
                'v5 = alignment_timestamp("month", tmp_v5)', \
                'v6 = alignment_timestamp("day", tmp_v6)', \
                'v7 = strftime("%Y-%m-%d %H:%M:%S", tmp_v7)', \
                'v8 = alignment_timestamp("hour", tmp_v8)', \
                'v9 = strftime("%Y-%m-%d %H:%M:%S", tmp_v9)']

    column_name_list = [\
            'tmp_k1', 'k2', 'tmp_k3', 'tmp_k4', 'tmp_k5', \
            'tmp_k6', 'tmp_k7', 'tmp_k8', \
            'v1', 'tmp_v2', 'tmp_v3', 'tmp_v4', 'tmp_v5', \
            'tmp_v6', 'tmp_v7', 'tmp_v8', 'tmp_v9']
    data_desc_list = palo_client.LoadDataInfo(DATA_TS.file_path_6, \
            table_name, column_name_list=column_name_list, set_list=set_list)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, 
                             max_filter_ratio=0.1)
    assert client.verify(DATA_TS.expected_data_file_list_3, table_name)
    client.clean(database_name)


def test_datetime_timestamp_invalid():
    """
    {
    "title": "test_sys_partition_load.test_datetime_timestamp_invalid",
    "describe": "数据文件中时间戳列含有无效字符串、八进制、十六进制字符串、算术表达式、小数",
    "tag": "system,p1,fuzz"
    }
    """
    """
    数据文件中时间戳列含有无效字符串、八进制、十六进制字符串、算术表达式、小数
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['1900-01-01 23:59:59', '2000-01-01 00:00:00', \
            '2099-12-21 23:59:59', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13) 
    assert client.create_table(table_name, DATA_TS.schema_3, \
            partition_info, distribution_info)
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    set_list = ['k1 = strftime("%Y-%m-%d %H:%M:%S", tmp_k1)', \
                'k3 = alignment_timestamp("hour", tmp_k3)', \
                'k4 = alignment_timestamp("day", tmp_k4)', \
                'k5 = strftime("%Y-%m-%d %H:%M:%S", tmp_k5)', \
                'k6 = alignment_timestamp("month", tmp_k6)', \
                'k7 = alignment_timestamp("year", tmp_k7)', \
                'k8 = strftime("%Y-%m-%d %H:%M:%S", tmp_k8)', \
                'v2 = strftime("%Y-%m-%d %H:%M:%S", tmp_v2)', \
                'v3 = alignment_timestamp("year", tmp_v3)', \
                'v4 = strftime("%Y-%m-%d %H:%M:%S", tmp_v4)', \
                'v5 = alignment_timestamp("month", tmp_v5)', \
                'v6 = alignment_timestamp("day", tmp_v6)', \
                'v7 = strftime("%Y-%m-%d %H:%M:%S", tmp_v7)', \
                'v8 = alignment_timestamp("hour", tmp_v8)', \
                'v9 = strftime("%Y-%m-%d %H:%M:%S", tmp_v9)']

    column_name_list = [\
            'tmp_k1', 'k2', 'tmp_k3', 'tmp_k4', 'tmp_k5', \
            'tmp_k6', 'tmp_k7', 'tmp_k8', \
            'v1', 'tmp_v2', 'tmp_v3', 'tmp_v4', 'tmp_v5', \
            'tmp_v6', 'tmp_v7', 'tmp_v8', 'tmp_v9']
    data_desc_list = palo_client.LoadDataInfo(DATA_TS.file_path_7, \
            table_name, column_name_list=column_name_list, set_list=set_list)
    assert client.batch_load(util.get_label(), data_desc_list,  \
            max_filter_ratio='1', is_wait=True, broker=broker_info)
    assert client.verify(DATA_TS.expected_data_file_list_3, table_name)
    client.clean(database_name)


def timestamp_special_data():
    """
    特殊数据
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['1900-01-01 23:59:59', '2000-01-01 00:00:00', \
            '2099-12-21 23:59:59', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13) 
    assert client.create_table(table_name, DATA_TS.schema_5, \
            partition_info, distribution_info)
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    set_list = ['k1 = strftime("%Y-%m-%d %H:%M:%S", tmp_k1)', \
                'k2 = alignment_timestamp("month", tmp_k2)', \
                'k3 = alignment_timestamp("year", tmp_k3)']

    column_name_list = ['tmp_k1', 'tmp_k2', 'tmp_k3']
    data_desc_list = palo_client.LoadDataInfo(DATA_TS.file_path_8, \
            table_name, column_name_list=column_name_list, set_list=set_list)
    assert client.batch_load(util.get_label(), data_desc_list,  \
            max_filter_ratio='1', is_wait=True, broker=broker_info)
    assert client.verify(DATA_TS.expected_data_file_list_4, table_name)
    client.clean(database_name)


def test_invalid_parameter_part_1():
    """
    {
    "title": "test_sys_partition_load.test_invalid_parameter_part_1",
    "describe": "函数缺少第1或2个参数，或同时缺少两个参数",
    "tag": "system,p1,fuzz"
    }
    """
    """
    函数缺少第1或2个参数，或同时缺少两个参数
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['1900-01-01 23:59:59', '2000-01-01 00:00:00', \
            '2099-12-21 23:59:59', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13) 
    assert client.create_table(table_name, DATA_TS.schema_5, \
            partition_info, distribution_info)
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    set_list_1 = ['k1 = strftime("%Y-%m-%d %H:%M:%S")', \
                'k2 = alignment_timestamp("month", tmp_k2)', \
                'k3 = alignment_timestamp("year", tmp_k3)']

    set_list_2 = ['k1 = strftime(tmp_k1)', \
                'k2 = alignment_timestamp("month", tmp_k2)', \
                'k3 = alignment_timestamp("year", tmp_k3)']

    set_list_3 = ['k1 = strftime("%Y-%m-%d %H:%M:%S", tmp_k1)', \
                'k2 = alignment_timestamp(tmp_k2)', \
                'k3 = alignment_timestamp("year", tmp_k3)']

    ret = False
    column_name_list = ['tmp_k1', 'tmp_k2', 'tmp_k3']
    data_desc_list = palo_client.LoadDataInfo(DATA_TS.file_path_8, \
            table_name, column_name_list=column_name_list, set_list=set_list_1)
    try:
        ret = client.batch_load(util.get_label(), data_desc_list, \
                max_filter_ratio='1', is_wait=True, broker=broker_info)
    except:
        pass
    assert not ret

    data_desc_list = palo_client.LoadDataInfo(DATA_TS.file_path_8, \
            table_name, column_name_list=column_name_list, set_list=set_list_2)
    try:
        ret = client.batch_load(util.get_label(), data_desc_list, \
                max_filter_ratio='1', is_wait=True, broker=broker_info)
    except:
        pass
    assert not ret

    data_desc_list = palo_client.LoadDataInfo(DATA_TS.file_path_8, \
            table_name, column_name_list=column_name_list, set_list=set_list_3)
    try:
        ret = client.batch_load(util.get_label(), data_desc_list, \
                max_filter_ratio='1', is_wait=True, broker=broker_info)
    except:
        pass
    assert not ret
    client.clean(database_name)


def test_invalid_parameter_part_2():
    """
    {
    "title": "test_sys_partition_load.test_invalid_parameter_part_2",
    "describe": "函数缺少第1或2个参数，或同时缺少两个参数",
    "tag": "system,p1,fuzz"
    }
    """
    """
    函数缺少第1或2个参数，或同时缺少两个参数
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['1900-01-01 23:59:59', '2000-01-01 00:00:00', \
            '2099-12-21 23:59:59', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13) 
    assert client.create_table(table_name, DATA_TS.schema_5, \
            partition_info, distribution_info)
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    set_list_4 = ['k1 = strftime("%Y-%m-%d %H:%M:%S", tmp_k1)', \
                'k2 = alignment_timestamp("month", tmp_k2)', \
                'k3 = alignment_timestamp("year")']

    set_list_5 = ['k1 = strftime()', \
                'k2 = alignment_timestamp("month", tmp_k2)', \
                'k3 = alignment_timestamp("year", tmp_k3)']

    set_list_6 = ['k1 = strftime("%Y-%m-%d %H:%M:%S", tmp_k1)', \
                'k2 = alignment_timestamp("month", tmp_k2)', \
                'k3 = alignment_timestamp()']

    ret = False
    column_name_list = ['tmp_k1', 'tmp_k2', 'tmp_k3']
    data_desc_list = palo_client.LoadDataInfo(DATA_TS.file_path_8, \
            table_name, column_name_list=column_name_list, set_list=set_list_4)
    try:
        ret = client.batch_load(util.get_label(), data_desc_list, \
                max_filter_ratio='1', is_wait=True, broker=broker_info)
    except:
        pass
    assert not ret

    data_desc_list = palo_client.LoadDataInfo(DATA_TS.file_path_8, \
            table_name, column_name_list=column_name_list, set_list=set_list_5)
    try:
        ret = client.batch_load(util.get_label(), data_desc_list, \
                max_filter_ratio='1', is_wait=True, broker=broker_info)
    except:
        pass
    assert not ret

    data_desc_list = palo_client.LoadDataInfo(DATA_TS.file_path_8, \
            table_name, column_name_list=column_name_list, set_list=set_list_6)
    try:
        ret = client.batch_load(util.get_label(), data_desc_list, \
                max_filter_ratio='1', is_wait=True, broker=broker_info)
    except:
        pass
    assert not ret
    client.clean(database_name)


def test_invalid_parameter_part_3():
    """
    {
    "title": "test_sys_partition_load.test_invalid_parameter_part_3",
    "describe": "函数传递空串""或"NULL"",
    "tag": "system,p1,fuzz"
    }
    """
    """
    函数传递空串""或"NULL"
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['1900-01-01 23:59:59', '2000-01-01 00:00:00', \
            '2099-12-21 23:59:59', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13) 
    assert client.create_table(table_name, DATA_TS.schema_5, \
            partition_info, distribution_info)
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    set_list_1 = ['k1 = strftime("", tmp_k1)', \
                'k2 = alignment_timestamp("month", tmp_k2)', \
                'k3 = alignment_timestamp("year", tmp_k3)']
    set_list_2 = ['k1 = strftime("%Y-%m-%d %H:%M:%S", "")', \
                'k2 = alignment_timestamp("month", tmp_k2)', \
                'k3 = alignment_timestamp("year", tmp_k3)']
    set_list_3 = ['k1 = strftime("%Y-%m-%d %H:%M:%S", tmp_k1)', \
                'k2 = alignment_timestamp("NULL", tmp_k2)', \
                'k3 = alignment_timestamp("year", tmp_k3)']

    ret = False
    column_name_list = ['tmp_k1', 'tmp_k2', 'tmp_k3']
    data_desc_list = palo_client.LoadDataInfo(DATA_TS.file_path_8, \
            table_name, column_name_list=column_name_list, set_list=set_list_1)
    try:
        ret = client.batch_load(util.get_label(), data_desc_list, \
                max_filter_ratio='1', is_wait=True, broker=broker_info)
    except:
        pass
    assert not ret

    data_desc_list = palo_client.LoadDataInfo(DATA_TS.file_path_8, \
            table_name, column_name_list=column_name_list, set_list=set_list_2)
    try:
        ret = client.batch_load(util.get_label(), data_desc_list, \
                max_filter_ratio='1', is_wait=True, broker=broker_info)
    except:
        pass
    assert not ret

    data_desc_list = palo_client.LoadDataInfo(DATA_TS.file_path_8, \
            table_name, column_name_list=column_name_list, set_list=set_list_3)
    try:
        ret = client.batch_load(util.get_label(), data_desc_list, \
                max_filter_ratio='1', is_wait=True, broker=broker_info)
    except:
        pass
    assert not ret
    client.clean(database_name)


def test_invalid_parameter_part_4():
    """
    {
    "title": "test_sys_partition_load.test_invalid_parameter_part_4",
    "describe": "函数传递空串""或"NULL", 或无效的时间格式字符串参数",
    "tag": "system,p1,fuzz"
    }
    """
    """
    函数传递空串""或"NULL", 或无效的时间格式字符串参数
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['1900-01-01 23:59:59', '2000-01-01 00:00:00', \
            '2099-12-21 23:59:59', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13) 
    assert client.create_table(table_name, DATA_TS.schema_5, \
            partition_info, distribution_info)
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    set_list_4 = ['k1 = strftime(None, tmp_k1)', \
                'k2 = alignment_timestamp("month", tmp_k2)', \
                'k3 = alignment_timestamp("year", tmp_k3)']
    set_list_5 = ['k1 = strftime("%Y-%m-%d %H:%M:%S", tmp_k1)', \
                'k2 = alignment_timestamp("month", tmp_k2)', \
                'k3 = alignment_timestamp(None, tmp_k3)']
    set_list_6 = ['k1 = strftime("%Y-%m-%d %H:%M:%S", tmp_k1)', \
                'k2 = alignment_timestamp("month", tmp_k2)', \
                'k3 = alignment_timestamp("year", None)']

    set_list_7 = ['k1 = strftime("%k-%m-%d %H:%M:%S", tmp_k1)', \
                'k2 = alignment_timestamp("month", tmp_k2)', \
                'k3 = alignment_timestamp("year", tmp_k3)']

    ret = False
    column_name_list = ['tmp_k1', 'tmp_k2', 'tmp_k3']
    data_desc_list = palo_client.LoadDataInfo(DATA_TS.file_path_8, \
            table_name, column_name_list=column_name_list, set_list=set_list_4)
    try:
        ret = client.batch_load(util.get_label(), data_desc_list, \
                max_filter_ratio='1', is_wait=True, broker=broker_info)
    except:
        pass
    assert not ret

    data_desc_list = palo_client.LoadDataInfo(DATA_TS.file_path_8, \
            table_name, column_name_list=column_name_list, set_list=set_list_5)
    try:
        ret = client.batch_load(util.get_label(), data_desc_list, \
                max_filter_ratio='1', is_wait=True, broker=broker_info)
    except:
        pass
    assert not ret

    data_desc_list = palo_client.LoadDataInfo(DATA_TS.file_path_8, \
            table_name, column_name_list=column_name_list, set_list=set_list_6)
    try:
        ret = client.batch_load(util.get_label(), data_desc_list, \
                max_filter_ratio='1', is_wait=True, broker=broker_info)
    except:
        pass
    assert not ret

    data_desc_list = palo_client.LoadDataInfo(DATA_TS.file_path_8, \
            table_name, column_name_list=column_name_list, set_list=set_list_7)
    try:
        ret = client.batch_load(util.get_label(), data_desc_list, \
                max_filter_ratio='1', is_wait=True, broker=broker_info)
    except:
        pass
    assert not ret
    client.clean(database_name)


def test_invalid_parameter_part_5():
    """
    {
    "title": "test_sys_partition_load.test_invalid_parameter_part_5",
    "describe": "不指定返回值或返回值列不存在",
    "tag": "system,p1,fuzz"
    }
    """
    """
    不指定返回值或返回值列不存在
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['1900-01-01 23:59:59', '2000-01-01 00:00:00', \
            '2099-12-21 23:59:59', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13) 
    assert client.create_table(table_name, DATA_TS.schema_5, \
            partition_info, distribution_info)
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    set_list_1 = ['strftime("%Y-%m-%d %H:%M:%S", tmp_k1)', \
                'k2 = alignment_timestamp("month", tmp_k2)', \
                'k3 = alignment_timestamp("year", tmp_k3)']

    set_list_2 = ['k1 = strftime("%Y-%m-%d %H:%M:%S", tmp_k1)', \
                'k2 = alignment_timestamp("month", tmp_k2)', \
                'k5 = alignment_timestamp("year", tmp_k3)']
    ret = False
    column_name_list = ['tmp_k1', 'tmp_k2', 'tmp_k3']
    data_desc_list = palo_client.LoadDataInfo(DATA_TS.file_path_8, \
            table_name, column_name_list=column_name_list, set_list=set_list_1)
    try:
        ret = client.batch_load(util.get_label(), data_desc_list, \
                max_filter_ratio='1', is_wait=True, broker=broker_info)
    except:
        pass
    assert not ret

    data_desc_list = palo_client.LoadDataInfo(DATA_TS.file_path_8, \
            table_name, column_name_list=column_name_list, set_list=set_list_2)
    try:
        ret = client.batch_load(util.get_label(), data_desc_list, \
                max_filter_ratio='1', is_wait=True, broker=broker_info)
    except:
        pass
    assert not ret
    client.clean(database_name)


def test_timestamp_invalid_format():
    """
    {
    "title": "test_sys_partition_load.test_timestamp_invalid_format",
    "describe": "函数返回值列为datetime类型，但是不是Palo支持的格式",
    "tag": "system,p1,fuzz"
    }
    """
    """
    函数返回值列为datetime类型，但是不是Palo支持的格式
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['1900-01-01 23:59:59', '2000-01-01 00:00:00', \
            '2099-12-21 23:59:59', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13) 
    assert client.create_table(table_name, DATA_TS.schema_5, \
            partition_info, distribution_info)
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    set_list = ['k1 = strftime("%Y:%m:%d %H:%M:%S", tmp_k1)', \
                'k2 = alignment_timestamp("month", tmp_k2)', \
                'k3 = alignment_timestamp("year", tmp_k3)']

    column_name_list = ['tmp_k1', 'tmp_k2', 'tmp_k3']
    data_desc_list = palo_client.LoadDataInfo(DATA_TS.file_path_8, \
            table_name, column_name_list=column_name_list, set_list=set_list)
    assert not client.batch_load(util.get_label(), data_desc_list,  \
            max_filter_ratio='0', is_wait=True, broker=broker_info)
    client.clean(database_name)


def test_timestamp_upper_lower():
    """
    {
    "title": "test_sys_partition_load.test_timestamp_upper_lower",
    "describe": "year|month|day|hour大小写",
    "tag": "system,p1,fuzz"
    }
    """
    """
    year|month|day|hour大小写
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['1900-01-01 23:59:59', '2000-01-01 00:00:00', \
            '2099-12-21 23:59:59', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13) 
    assert client.create_table(table_name, DATA_TS.schema_5, \
            partition_info, distribution_info)
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    set_list = ['k1 = strftime("%Y-%m-%d %H:%M:%S", tmp_k1)', \
                'k2 = alignment_timestamp("MONTH", tmp_k2)', \
                'k3 = alignment_timestamp("YEAR", tmp_k3)']

    column_name_list = ['tmp_k1', 'tmp_k2', 'tmp_k3']
    data_desc_list = palo_client.LoadDataInfo(DATA_TS.file_path_8, \
            table_name, column_name_list=column_name_list, set_list=set_list)
    assert not client.batch_load(util.get_label(), data_desc_list,  \
            max_filter_ratio='0', is_wait=True, broker=broker_info)
    client.clean(database_name)


def test_no_column_list():
    """
    {
    "title": "test_sys_partition_load.test_no_column_list",
    "describe": "不指定column_list",
    "tag": "system,p1,fuzz"
    }
    """
    """
    不指定column_list
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['1900-01-01 23:59:59', '2000-01-01 00:00:00', \
            '2099-12-21 23:59:59', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13) 
    assert client.create_table(table_name, DATA_TS.schema_3, \
            partition_info, distribution_info)
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    set_list = ['k1 = strftime("%Y-%m-%d %H:%M:%S", k1)', \
                'k3 = alignment_timestamp("hour", k3)', \
                'k4 = alignment_timestamp("day", k4)', \
                'k5 = strftime("%Y-%m-%d %H:%M:%S", k5)', \
                'k6 = alignment_timestamp("month", k6)', \
                'k7 = alignment_timestamp("year", k7)', \
                'k8 = strftime("%Y-%m-%d %H:%M:%S", k8)', \
                'v2 = strftime("%Y-%m-%d %H:%M:%S", v2)', \
                'v3 = alignment_timestamp("year", v3)', \
                'v4 = strftime("%Y-%m-%d %H:%M:%S", v4)', \
                'v5 = alignment_timestamp("month", v5)', \
                'v6 = alignment_timestamp("day", v6)', \
                'v7 = strftime("%Y-%m-%d %H:%M:%S", v7)', \
                'v8 = alignment_timestamp("hour", v8)', \
                'v9 = strftime("%Y-%m-%d %H:%M:%S", v9)']

    data_desc_list = palo_client.LoadDataInfo(DATA_TS.file_path_6, \
            table_name, set_list=set_list)
    assert client.batch_load(util.get_label(), data_desc_list,  \
            max_filter_ratio='1', is_wait=True, broker=broker_info)
    assert client.verify(DATA_TS.expected_data_file_list_3, table_name)
    client.clean(database_name)


def test_to_varchar():
    """
    {
    "title": "test_sys_partition_load.test_to_varchar",
    "describe": "时间戳转换成字符串类型",
    "tag": "system,p1,fuzz"
    }
    """
    """
    时间戳转换成字符串类型
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    assert client.create_table(table_name, DATA_TS.schema_6)
    assert client.show_tables(table_name)

    set_list = ['k1 = strftime("%Y-%m-%d %H:%M:%S", k1)', \
                'k3 = alignment_timestamp("hour", k3)', \
                'k4 = alignment_timestamp("day", k4)', \
                'k5 = strftime("%Y-%m-%d %H:%M:%S", k5)', \
                'k6 = alignment_timestamp("month", k6)', \
                'k7 = alignment_timestamp("year", k7)', \
                'k8 = strftime("%Y-%m-%d %H:%M:%S", k8)', \
                'v2 = strftime("%Y-%m-%d %H:%M:%S", v2)', \
                'v3 = alignment_timestamp("year", v3)', \
                'v4 = strftime("%Y-%m-%d %H:%M:%S", v4)', \
                'v5 = alignment_timestamp("month", v5)', \
                'v6 = alignment_timestamp("day", v6)', \
                'v7 = strftime("%Y-%m-%d %H:%M:%S", v7)', \
                'v8 = alignment_timestamp("hour", v8)', \
                'v9 = strftime("%Y-%m-%d %H:%M:%S", v9)']

    data_desc_list = palo_client.LoadDataInfo(DATA_TS.file_path_6, \
            table_name, set_list=set_list)
    assert client.batch_load(util.get_label(), data_desc_list,  \
            max_filter_ratio='1', is_wait=True, broker=broker_info)
    assert client.verify(DATA_TS.expected_data_file_list_3, table_name)
    client.clean(database_name)


def test_one_to_one():
    """
    {
    "title": "test_sys_partition_load.test_one_to_one",
    "describe": "同一列多次转换或对齐都对应到表中的同一列",
    "tag": "system,p1,fuzz"
    }
    """
    """
    同一列多次转换或对齐都对应到表中的同一列
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['-300000', '0', '300000', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13) 
    assert client.create_table(table_name, DATA_TS.schema_2, \
            partition_info, distribution_info)
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    set_list = ['k1 = alignment_timestamp("hour", tmp_k1)', \
                'k3 = alignment_timestamp("day", tmp_k3)', \
                'k4 = alignment_timestamp("month", tmp_k4)', \
                'k5 = alignment_timestamp("year", tmp_k5)', \
                'k5 = alignment_timestamp("year", tmp_k5)', \
                'v2 = alignment_timestamp("year", tmp_v2)', \
                'v4 = alignment_timestamp("month", tmp_v4)', \
                'v6 = alignment_timestamp("day", tmp_v6)', \
                'v7 = alignment_timestamp("hour", tmp_v7)']

    column_name_list = ['tmp_k1', 'k2', 'tmp_k3', 'tmp_k4', 'tmp_k5', \
            'v1', 'tmp_v2', 'v3', 'tmp_v4', 'v5', 'tmp_v6', 'tmp_v7']
    data_desc_list = palo_client.LoadDataInfo(DATA_TS.file_path_4, \
            table_name, column_name_list=column_name_list, set_list=set_list)
    ret = False
    try:
        ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    except:
        pass
    assert not ret
    client.clean(database_name)


def test_one_to_many():
    """
    {
    "title": "test_sys_partition_load.test_one_to_many",
    "describe": "同一列转换后对应到表中的多列，及多个函数具有相同的参数列，不同的返回值列",
    "tag": "system,p1,fuzz"
    }
    """
    """
    同一列转换后对应到表中的多列，及多个函数具有相同的参数列，不同的返回值列
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['-300000', '0', '300000', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13) 
    assert client.create_table(table_name, DATA_TS.schema_7, \
            partition_info, distribution_info)
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    set_list = ['k1 = alignment_timestamp("hour", tmp_k1)', \
                'k3 = alignment_timestamp("day", tmp_k3)', \
                'k4 = alignment_timestamp("month", tmp_k4)', \
                'k5 = alignment_timestamp("year", tmp_k5)', \
                'v2 = alignment_timestamp("year", tmp_v2)', \
                'v4 = alignment_timestamp("month", tmp_v4)', \
                'v6 = alignment_timestamp("day", tmp_v6)', \
                'v7 = alignment_timestamp("day", tmp_v6)']

    column_name_list = ['tmp_k1', 'k2', 'tmp_k3', 'tmp_k4', 'tmp_k5', \
            'v1', 'tmp_v2', 'v3', 'tmp_v4', 'v5', 'tmp_v6', 'tmp_v7']
    data_desc_list = palo_client.LoadDataInfo(DATA_TS.file_path_4, \
            table_name, column_name_list=column_name_list, set_list=set_list)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=0.05)
    assert client.verify(DATA_TS.expected_data_file_list_5, table_name)
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

