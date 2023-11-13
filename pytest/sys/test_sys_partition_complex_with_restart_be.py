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
#   @file test_sys_partition_complex_with_restart_be.py
#   @date 2015/05/13 15:26:21
#   @brief This file is a test file for palo data loading and verifying.
#   
#############################################################################

"""
复合分区测试设计建表部分测试
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
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port)
    client.init()


def check_partition_list(table_name, partition_name_list):
    """
    验证分区是否创建成功
    """
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)


def test_select_while_drop_be():
    """
    {
    "title": "test_select_while_drop_be",
    "describe": "查询过程中drop BE",
    "tag": "system,p1,fuzz"
    }
    """
    class DropBeThread(threading.Thread):
        """
        Delete线程
        """
        def __init__(self, database_name, be_list):
            threading.Thread.__init__(self)
            self.database_name = database_name
            self.be_list = be_list

        def run(self):
            """
            run
            """
            thread_client = palo_client.PaloClient(palo_config.config.fe_host, \
                    palo_config.config.fe_query_port, self.database_name)
            thread_client.init()
            thread_client.drop_backend_list(self.be_list)


    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    retry = 10
    ret = None
    while retry > 0:
        try:
            ret = client.create_database(database_name)
            break
        except Exception as e:
            print(str(e))
            client.connect()
        time.sleep(3)
        retry -= 1
    assert ret
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', partition_name_list, partition_value_list)
    distribution_type = 'Hash(k2)'
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

    backend_host_port_list = client.get_backend_host_port_list()
    be = backend_host_port_list[0]
    drop_be_thread = DropBeThread(database_name, be)
    drop_be_thread.start()

    #数据校验
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    drop_be_thread.join()
    client.add_backend_list(be)
    time.sleep(30)
    client.clean(database_name)


def test_select_while_drop_add_be():
    """
    {
    "title": "test_select_while_drop_add_be",
    "describe": "查询过程中drop add BE",
    "tag": "system,p1,fuzz"
    }
    """
    class DropAddBeThread(threading.Thread):
        """
        Delete线程
        """
        def __init__(self, database_name, be_list):
            threading.Thread.__init__(self)
            self.database_name = database_name
            self.be_list = be_list

        def run(self):
            """
            run
            """
            thread_client = palo_client.PaloClient(palo_config.config.fe_host, \
                    palo_config.config.fe_query_port, self.database_name)
            thread_client.init()
            thread_client.drop_backend_list(self.be_list)
            time.sleep(10)
            thread_client.add_backend_list(self.be_list)

    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', partition_name_list, partition_value_list)
    distribution_type = 'Hash(k2)'
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

    backend_host_port_list = client.get_backend_host_port_list()
    be = backend_host_port_list[0]
    drop_add_be_thread = DropAddBeThread(database_name, be)
    drop_add_be_thread.start()

    #数据校验
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    drop_add_be_thread.join()
    time.sleep(30)
    client.clean(database_name)


def test_select_after_drop_be():
    """
    {
    "title": "test_select_after_drop_be",
    "describe": "drop BE后查询",
    "tag": "system,p1,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', partition_name_list, partition_value_list)
    distribution_type = 'Hash(k2)'
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

    backend_host_port_list = client.get_backend_host_port_list()
    be = backend_host_port_list[0]

    client.drop_backend_list(be)
    #数据校验
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    time.sleep(10)
    client.add_backend_list(be)
    time.sleep(30)
    client.clean(database_name)


def test_select_after_drop_add_be():
    """
    {
    "title": "test_select_after_drop_add_be",
    "describe": "drop add BE后查询",
    "tag": "system,p1,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', partition_name_list, partition_value_list)
    distribution_type = 'Hash(k2)'
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

    backend_host_port_list = client.get_backend_host_port_list()
    be = backend_host_port_list[0]

    client.drop_backend_list(be)
    time.sleep(10)
    client.add_backend_list(be)
    #数据校验
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    time.sleep(30)
    client.clean(database_name)


def teardown_module():
    """
    tearDown
    """
    pass


if __name__ == '__main__':
    import pdb
    # pdb.set_trace()
    setup_module()
    test_select_while_drop_be()
    test_select_while_drop_add_be()
    test_select_after_drop_be()
    test_select_after_drop_add_be()
    #restart_be_while_loading()

