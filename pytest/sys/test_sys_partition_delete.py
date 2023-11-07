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
#   @file test_sys_partition_delete.py
#   @date 2015/05/13 15:26:21
#   @brief This file is a test file for palo data loading and verifying.
#   
#############################################################################

"""
复合分区，delete操作测试
"""

import time
import threading
from data import partition as DATA
from lib import palo_config
from lib import palo_client
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


def teardown_module():
    """
    tearDown
    """
    pass


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
    palo3.3 storage_type未使用，建表使用默认的column类型，不支持行存
    """
    partition_info = palo_client.PartitionInfo(column_name, \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo(distribution_type, bucket_num) 
    client.create_table(table_name, DATA.schema_1, \
            partition_info, distribution_info)
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)


def test_delete_verify():
    """
    {
    "title": "test_sys_partition_delete.test_delete_verify",
    "describe": "对复合分区表，执行删除操作，验证结果正确性",
    "tag": "system,p1"
    }
    """
    """
    对复合分区表，执行删除操作，验证结果正确性
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
    check_load_and_verify(table_name)

    client.delete(table_name, [('k1', '<', '-1'),], 'partition_a')
    client.delete(table_name, [('k1', '>', '110'),], 'partition_b')
    client.delete(table_name, [('k1', '>', '26'),], 'partition_c')
    client.delete(table_name, [('k1', '>', '26'),], 'partition_d')
    ret = False
    try:
        ret = client.delete(table_name, [('k1', '>', '26'),], 'partition_e')
    except:
        pass
    assert not ret

    assert client.verify(DATA.expected_file_5, table_name)
    client.clean(database_name)


def test_delete_partition_value_verify():
    """
    {
    "title": "test_sys_partition_delete.test_delete_partition_value_verify",
    "describe": "对复合分区表，执行删除操作，删除条件是分区值，验证结果正确性",
    "tag": "system,p1"
    }
    """
    """
    对复合分区表，执行删除操作，删除条件是分区值，验证结果正确性
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
    check_load_and_verify(table_name)

    client.delete(table_name, [('k1', '<', '5'),], 'partition_a')
    client.delete(table_name, [('k1', '<=', '20'),], 'partition_b')
    client.delete(table_name, [('k1', '=', '30'),], 'partition_c')
    client.delete(table_name, [('k1', '>', '26'),], 'partition_d')

    assert client.verify(DATA.expected_file_6, table_name)
    client.clean(database_name)


def test_delete_drop_and_partition_load():
    """
    {
    "title": "test_sys_partition_delete.test_delete_drop_and_partition_load",
    "describe": "删除后，删除分区，增加分区，导入",
    "tag": "system,p1"
    }
    """
    """
    删除后，删除分区，增加分区，导入
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
    check_load_and_verify(table_name)

    client.delete(table_name, [('k1', '<', '5'),], 'partition_a')
    client.delete(table_name, [('k1', '<=', '20'),], 'partition_b')
    client.delete(table_name, [('k1', '=', '30'),], 'partition_c')
    client.delete(table_name, [('k1', '>', '26'),], 'partition_d')

    assert client.drop_partition(table_name, 'partition_c')
    assert client.drop_partition(table_name, 'partition_d')
    assert client.add_partition(table_name, 'partition_e', '40', \
            distribute_type='RANDOM', bucket_num=5)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    client.clean(database_name)


def test_delete_after_drop_and_add_partition():
    """
    {
    "title": "test_sys_partition_delete.test_delete_after_drop_and_add_partition",
    "describe": "删除分区后delete, 增加分区后delete",
    "tag": "system,p1"
    }
    """
    """
    删除分区后delete, 增加分区后delete
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
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)

    #删除分区后delete, 校验
    assert client.drop_partition(table_name, 'partition_b')
    assert client.drop_partition(table_name, 'partition_d')
    client.delete(table_name, [('k1', '<', '5'),], 'partition_a')
    client.delete(table_name, [('k1', '=', '20'),], 'partition_c')
    client.delete(table_name, [('k1', '>', '26'),], 'partition_c')
    assert client.verify(DATA.expected_file_7, table_name)

    #增加分区后delete, 校验
    assert client.add_partition(table_name, 'partition_e', '40', \
            distribute_type='RANDOM', bucket_num=5)
    client.delete(table_name, [('k1', '<', '5'),], 'partition_a')
    client.delete(table_name, [('k1', '<=', '20'),], 'partition_a')
    client.delete(table_name, [('k1', '=', '30'),], 'partition_c')
    client.delete(table_name, [('k1', '>', '26'),], 'partition_e')
    client.delete(table_name, [('k1', '=', '26'),], 'partition_c')
    assert client.verify(DATA.expected_file_8, table_name)

    #delete, 删除分区，增加分区，导入，校验
    client.delete(table_name, [('k1', '<', '100'),], 'partition_c')
    assert client.drop_partition(table_name, 'partition_c')
    assert client.drop_partition(table_name, 'partition_e')
    assert client.add_partition(table_name, 'partition_f', 'MAXVALUE', \
            distribute_type='RANDOM', bucket_num=5)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    client.clean(database_name)


def test_multi_delete():
    """
    {
    "title": "test_sys_partition_delete.test_multi_delete",
    "describe": "多个delete并行",
    "tag": "system,p1"
    }
    """
    """
    多个delete并行
    如何是并行的线程的话
    Replica does not catch up with check version是正常的
    现已改为非并行线程
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
            thread_client.delete(self.table_name, \
                    self.delete_condition_list, self.partition_name)

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
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)

    delete_thread_1 = DeleteThread(database_name, table_name, \
            [('k1', '<', '5'),], 'partition_a')
    delete_thread_2 = DeleteThread(database_name, table_name, \
            [('k1', '<', '20'),], 'partition_b')
    delete_thread_3 = DeleteThread(database_name, table_name, \
            [('k1', '=', '20'),], 'partition_c')
    delete_thread_4 = DeleteThread(database_name, table_name, \
            [('k1', '>', '26'),], 'partition_c')

    delete_thread_1.start()
    delete_thread_1.join()
    delete_thread_2.start()
    delete_thread_2.join()
    delete_thread_3.start()
    delete_thread_3.join()
    delete_thread_4.start()
    delete_thread_4.join()

    assert client.verify(DATA.expected_file_7, table_name)
    client.clean(database_name)


def test_delete_and_rollup():
    """
    {
    "title": "test_sys_partition_delete.test_delete_and_rollup",
    "describe": "删除，然后rollup",
    "tag": "system,p1"
    }
    """
    """
    删除，然后rollup
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
    check_load_and_verify(table_name)

    client.delete(table_name, [('k1', '<', '5'),], 'partition_a')
    client.delete(table_name, [('k1', '<=', '20'),], 'partition_b')
    client.delete(table_name, [('k1', '=', '30'),], 'partition_c')
    client.delete(table_name, [('k1', '>', '26'),], 'partition_d')

    assert client.verify(DATA.expected_file_6, table_name)

    column_name_list = ['k1', 'k2', 'v4']
    assert client.create_rollup_table(table_name, \
            index_name, column_name_list, is_wait=True)

    assert client.verify(DATA.expected_file_6, table_name)
    client.clean(database_name)


def test_delete_and_schema_change():
    """
    {
    "title": "test_sys_partition_delete.test_delete_and_schema_change",
    "describe": "删除，然后schema change",
    "tag": "system,p1"
    }
    """
    """
    删除，然后schema change
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
            'HASH(k1)', 13, 'row')
    check_load_and_verify(table_name)

    client.delete(table_name, [('k1', '<', '5'),], 'partition_a')
    client.delete(table_name, [('k1', '<=', '20'),], 'partition_b')
    client.delete(table_name, [('k1', '=', '30'),], 'partition_c')
    client.delete(table_name, [('k1', '>', '26'),], 'partition_d')

    assert client.verify(DATA.expected_file_6, table_name)

    column_name_list = ['k1', 'k2', 'v6']
    assert client.create_rollup_table(table_name, \
            index_name, column_name_list, is_wait=True)

    assert client.schema_change_modify_column(table_name, 'k2', 'INT', \
            is_wait_job=True, is_wait_delete_old_schema=True)

    assert client.schema_change_drop_column(table_name, ['v3', 'v6'], \
            is_wait_job=True, is_wait_delete_old_schema=True)

    assert client.verify([DATA.expected_file_9_new] * 2, table_name) 
    client.clean(database_name)


def test_delete_while_schema_change():
    """
    {
    "title": "test_sys_partition_delete.test_delete_while_schema_change",
    "describe": "删除的同时schema change",
    "tag": "system,p1"
    }
    """
    """
    删除的同时schema change
    """
    class SchemaChangeThread(threading.Thread):
        """
        SchemaChange线程
        """
        def __init__(self, database_name, table_name, column_name, column_type):
            threading.Thread.__init__(self)
            self.database_name = database_name
            self.table_name = table_name
            self.column_name = column_name
            self.column_type = column_type

        def run(self):
            """
            run
            """
            thread_client = palo_client.PaloClient(palo_config.config.fe_host, \
                    palo_config.config.fe_query_port, self.database_name)
            thread_client.init()
            thread_client.schema_change_modify_column(table_name, \
                    self.column_name, self.column_type, \
                    is_wait_job=False, is_wait_delete_old_schema=False)

    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '31', 'MAXVALUE']

    partition_check(table_name, 'k1', \
            partition_name_list, partition_value_list, \
            'HASH(k1)', 13, 'row')
    check_load_and_verify(table_name)

    schema_change_thread = SchemaChangeThread(database_name, table_name, 'k2', 'INT')
    schema_change_thread.start()
    time.sleep(8)

    client.delete(table_name, [('k1', '<', '5'),], 'partition_a')
    client.delete(table_name, [('k1', '<=', '20'),], 'partition_b')
    client.delete(table_name, [('k1', '=', '30'),], 'partition_c')
    client.delete(table_name, [('k1', '>', '26'),], 'partition_d')
    schema_change_thread.join()
    assert client.verify(DATA.expected_file_6, table_name)
    client.clean(database_name)


def test_delete_while_rollup():
    """
    {
    "title": "test_sys_partition_delete.test_delete_while_rollup",
    "describe": "删除的同时rollup",
    "tag": "system,p1"
    }
    """
    """
    删除的同时rollup
    """
    class RollupThread(threading.Thread):
        """
        Rollup线程
        """
        def __init__(self, database_name, table_name, column_name, column_type):
            threading.Thread.__init__(self)
            self.database_name = database_name
            self.table_name = table_name
            self.index_name = index_name
            self.column_name_list = column_name_list

        def run(self):
            """
            run
            """
            thread_client = palo_client.PaloClient(palo_config.config.fe_host, \
                    palo_config.config.fe_query_port, self.database_name)
            thread_client.init()
            thread_client.create_rollup_table(self.table_name, \
                    self.index_name, self.column_name_list, is_wait=False)

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
    check_load_and_verify(table_name)

    column_name_list = ['k1', 'k2', 'v5']
    rollup_thread = RollupThread(database_name, table_name, 'k2', 'INT')
    rollup_thread.start()
    time.sleep(5)
    client.delete(table_name, [('k1', '<', '5'),], 'partition_a')
    client.delete(table_name, [('k1', '<=', '20'),], 'partition_b')
    client.delete(table_name, [('k1', '=', '30'),], 'partition_c')
    client.delete(table_name, [('k1', '>', '26'),], 'partition_d')
    rollup_thread.join()
    assert client.verify(DATA.expected_file_6, table_name)
    client.clean(database_name)


def test_delete_and_be():
    """
    {
    "title": "test_sys_partition_delete.test_delete_and_be",
    "describe": "delete后，BE后，校验",
    "tag": "system,p1"
    }
    """
    """
    delete后，BE后，校验
    delete_delta_expire_time = 0
    be_policy_cumulative_files_number = 3
    be_policy_start_time = 0
    be_policy_end_time = 24
    be_policy_be_interval_seconds = 0
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['1930-01-01', '2000-01-01', '2031-12-31', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('date_key', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(smallint_key)', 1) 
    client.create_table(table_name, DATA.schema_2, \
            partition_info, distribution_info)
    assert client.show_tables(table_name)
 
    file_path = palo_config.gen_remote_file_path('sys/delete_and_be/1')
    data_desc_list = palo_client.LoadDataInfo(file_path, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    client.delete(table_name, [('char_50_key', '=', '50string_make_you_smile'),], 'partition_a')

    expected_data_file_list = './data/DELETE_AND_BE/1'
    assert client.verify(expected_data_file_list, table_name)

    client.delete(table_name, [('tinyint_key', '=', '10'),], 'partition_a')
    client.delete(table_name, [('tinyint_key', '=', '10'),], 'partition_a')
    client.delete(table_name, [('tinyint_key', '=', '10'),], 'partition_a')

    time.sleep(20)
    assert client.verify(expected_data_file_list, table_name)
    client.clean(database_name)


if __name__ == '__main__':
    setup_module()
    database_name = 'database_2325154442_2983398330'
    table_name = 'table_2325154442_2983398330'
    client.use(database_name)
    assert client.verify(DATA.expected_file_9_new, table_name)

