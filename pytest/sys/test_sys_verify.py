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
#   @file test_sys_verify.py
#   @date 2015/02/04 15:26:21
#   @brief This file is a test file for palo data loading and verifying.
#   
#############################################################################

"""
测试各种数据类型和存储方式的数据正确性
"""
import pytest
from data import verify as VERIFY_DATA
from lib import palo_config
from lib import palo_client
from lib import util
from lib import common

config = palo_config.config
LOG = palo_client.LOG
L = palo_client.L


broker_name = config.broker_name
broker_info = palo_config.broker_info


def setup_module():
    """
    setUp
    """
    global client
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port, user=config.fe_user, password=config.fe_password)
    client.init()
    try:
        is_exist = False
        brokers = client.get_broker_list()
        for br in brokers:
            if broker_name == br[0]:
                is_exist = True
                break
        if not is_exist:
            raise pytest.skip('no broker')
    except:
        pass


def check_table_load_and_verify(table_name):
    """
    验证表是否创建成功，导入数据，校验
    """
    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(VERIFY_DATA.file_path_1, table_name)
    label_1 = util.get_label()
    client.batch_load(label_1, data_desc_list, is_wait=True, broker=broker_info)

    assert client.verify(VERIFY_DATA.expected_data_file_list_1, table_name)

    data_desc_list = palo_client.LoadDataInfo(VERIFY_DATA.file_path_2, table_name)
    label_2 = util.get_label()
    client.batch_load(label_2, data_desc_list, is_wait=True, broker=broker_info)

    assert client.verify(VERIFY_DATA.expected_data_file_list_2, table_name)


def test_column_sum():
    """
    {
    "title": "test_sys_verify.test_column_sum",
    "describe": "测试列存储sum聚合方式",
    "tag": "system,p0"
    }
    """
    """
    测试列存储sum聚合方式
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)
    client.create_table(table_name, \
            VERIFY_DATA.schema_1, storage_type='column', keys_desc='AGGREGATE KEY (K1)')
    check_table_load_and_verify(table_name)
    client.clean(database_name)


def test_column_max():
    """
    {
    "title": "test_sys_verify.test_column_max",
    "describe": "测试列存储max聚合方式",
    "tag": "system,p0"
    }
    """
    """
    测试列存储max聚合方式
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)
    client.create_table(table_name, \
            VERIFY_DATA.schema_2, storage_type='column', keys_desc='AGGREGATE KEY (K1)')
    check_table_load_and_verify(table_name)
    client.clean(database_name)


def test_column_min():
    """
    {
    "title": "test_sys_verify.test_column_min",
    "describe": "测试列存储min聚合方式",
    "tag": "system,p0"
    }
    """
    """
    测试列存储min聚合方式
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)
    client.create_table(table_name, \
            VERIFY_DATA.schema_3, storage_type='column', keys_desc='AGGREGATE KEY (K1)')
    check_table_load_and_verify(table_name)
    client.clean(database_name)


def test_column_replace():
    """
    {
    "title": "test_sys_verify.test_column_replace",
    "describe": "测试列存储replace聚合方式",
    "tag": "system,p0"
    }
    """
    """
    测试列存储replace聚合方式
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)
    client.create_table(table_name, \
            VERIFY_DATA.schema_4, storage_type='column', keys_desc='AGGREGATE KEY (k1)')
    check_table_load_and_verify(table_name)
    client.clean(database_name)


def test_same_name_diff_len():
    """
    {
    "title": "test_sys_verify.test_same_name_diff_len",
    "describe": "查询的数据越界，tinyint的大小为[-128, 127]你们可以查小于这个最小值，以及大于这个最大值的查询",
    "tag": "system,p0"
    }
    """
    """
    1.
    针对上次的nmga的core你们可以加个case，就是查询的数据越界
    就像tinyint的大小为[-128, 127]你们可以查小于这个最小值，以及大于这个最大值的查询
    都是字段f，一个是int，一个是tinyint；或者一个是varchar(10)，一个是varchar(20)
    2.
    char或varchar长度溢出时需要进行处理
    改写逻辑如下： 对于varchar(5)
    k1 < 'aaaaab' --> k1 <= 'aaaaa' 截断、同时改写
    k1 <= 'aaaaab' --> k1 <= 'aaaaa' 只截断，不改写
    k1 = 'aaaaab' --> 不下推
    k1 > 'aaaaab' --> k1 > 'aaaaa' 只截断，不改写 
    k1 >= 'aaaaab' --> k1 > 'aaaaa' 截断、同时改写
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    table_name_a = 'table_a'
    table_name_b = 'table_b'
    client.create_table(table_name_a, VERIFY_DATA.schema_5, storage_type='column', \
            keys_desc='AGGREGATE KEY (K1, k2, k3, k4, k5, k6, k7, k8, k9, k10)')
    client.create_table(table_name_b, VERIFY_DATA.schema_6, storage_type='column', \
            keys_desc='AGGREGATE KEY (K1, k2, k3, k4, k5, k6, k7, k8, k9, k10)')

    assert client.show_tables(table_name_a)
    assert client.show_tables(table_name_b)

    data_desc_list = palo_client.LoadDataInfo(VERIFY_DATA.file_path_3, table_name_a)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)

    data_desc_list = palo_client.LoadDataInfo(VERIFY_DATA.file_path_3, table_name_b)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)

    sql = 'select * from table_a where k1 > 130' 
    assert () == client.execute(sql)
    sql = 'select * from table_a where k1 < -130' 
    assert () == client.execute(sql)

    sql = 'select a.k1 as a_k1, b.k1 as b_k1, a.k9 as a_k9, b.k9 as b_k9 from table_a as a, ' \
            'table_b as b where a.k9 = b.k9 order by a_k1, b_k1, a_k9, b_k9'
    assert common.check_by_file(VERIFY_DATA.expected_file_1, sql=sql, client=client)

    sql = "select a.k1 as a_k1, b.k1 as b_k1, a.k9 as a_k9, b.k9 as b_k9 " \
            "from table_a as a, table_b as b " \
            "where a.k9 > b.k9 and a.k9 = 'vzb' and b.k9 = 'ddsc' order by a_k1, b_k1, a_k9, b_k9"
    assert common.check_by_file(VERIFY_DATA.expected_file_2, sql=sql, client=client)

    sql = "select a.k1 as a_k1, b.k1 as b_k1, a.k9 as a_k9, b.k9 as b_k9 " \
            "from table_a as a, table_b as b " \
    "where a.k9 < b.k9 and a.k9 = 'ddsc' and b.k9 = 'vzb' order by a_k1, b_k1, a_k9, b_k9"
    assert common.check_by_file(VERIFY_DATA.expected_file_3, sql=sql, client=client)

    #NOTICE K9 varchar(10)
    #TODO
    sql = "select k9 from table_a where k9 = 'aaaaaaaaaa'"
    assert () == client.execute(sql)
    sql = "select k9 from table_a where k9 = 'aaaaaaaaaaa'"
    print(client.execute(sql))
    sql = "select k9 from table_a where k9 > 'aaaaaaaaaaa'"
    print(client.execute(sql))
    sql = "select k9 from table_a where k9 < 'aaaaaaaaaaa'"
    print(client.execute(sql))
    client.clean(database_name)


def test_not_support_row():
    """
    {
    "title": "test_sys_verify.test_not_support_row",
    "describe": "不支持row存储，目前client中create_table，统一建column，不支持指定storage_type",
    "tag": "system,p0,fuzz"
    }
    """
    """
    不支持row存储
    目前client中create_table，统一建column，不支持指定storage_type
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    try:
        ret = client.create_table(table_name, 
                                  VERIFY_DATA.schema_4, storage_type='row', 
                                  keys_desc='AGGREGATE KEY (K1)')
        assert not ret
    except Exception as e:
        pass
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

