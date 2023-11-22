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
File: test_sys_special_data.py
Date: 2015/03/24 11:36:12
"""

import time
import sys
sys.path.append("../")
from data import special as DATA
from lib import palo_client
from lib import palo_config
from lib import util 

config = palo_config.config
LOG = palo_client.LOG
L = palo_client.L
broker_info = palo_config.broker_info


def setup_module():
    """
    set up
    """
    global client
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port, \
            user=config.fe_user, password=config.fe_password)
    client.init()


def test_special_int_valid_hash_column():
    """
    {
    "title": "test_sys_special_data.test_special_int_valid_hash_column",
    "describe": "int类型特殊值, hash分区, column存储",
    "tag": "system,p1,fuzz"
    }
    """
    """
    功能点：int类型特殊值, hash分区, column存储
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)
    client.create_table(table_name, DATA.schema_1, \
            keys_desc='AGGREGATE KEY (tinyint_key, smallint_key)')

    time.sleep(1)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_1, table_name)
    assert ret
    client.clean(database_name)


def test_special_decimal_valid_hash_column():
    """
    {
    "title": "test_sys_special_data.test_special_decimal_valid_hash_column",
    "describe": "decimal类型特殊值, hash分区, column存储",
    "tag": "system,p1,fuzz"
    }
    """
    """
    功能点：decimal类型特殊值, hash分区, column存储
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)
    client.create_table(table_name, DATA.schema_2, keys_desc='AGGREGATE KEY (tinyint_key)')

    time.sleep(1)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_2, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_2, table_name)
    assert ret
    client.clean(database_name)


def test_special_max_decimal_valid_hash_column():
    """
    {
    "title": "test_sys_special_data.test_special_max_decimal_valid_hash_column",
    "describe": "decimal类型特殊值, 最大值, hash分区, column存储",
    "tag": "system,p1,fuzz"
    }
    """
    """
    功能点：decimal类型特殊值, 最大值, hash分区, column存储
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)
    client.create_table(table_name, DATA.schema_3, keys_desc='AGGREGATE KEY (tinyint_key)')

    time.sleep(1)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_3, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_3, table_name)
    assert ret
    client.clean(database_name)


def test_special_char_valid_hash_column():
    """
    {
    "title": "test_sys_special_data.test_special_char_valid_hash_column",
    "describe": "char类型特殊值, hash分区, column存储",
    "tag": "system,p1,fuzz"
    }
    """
    """
    功能点：char类型特殊值, hash分区, column存储
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)
    client.create_table(table_name, DATA.schema_4, keys_desc='AGGREGATE KEY (tinyint_key)')

    time.sleep(1)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_4, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_4, table_name)
    assert ret
    client.clean(database_name)


def test_special_char_invalid_hash_column():
    """
    {
    "title": "test_sys_special_data.test_special_char_invalid_hash_column",
    "describe": "char类型特殊值, invalid, hash分区, column存储",
    "tag": "system,p1,fuzz"
    }
    """
    """
    功能点：char类型特殊值, invalid, hash分区, column存储
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)
    client.create_table(table_name, DATA.schema_5, keys_desc='AGGREGATE KEY (tinyint_key)')

    time.sleep(1)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_5, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert not ret

    ret = client.verify(DATA.expected_data_file_list_5, table_name)
    assert not ret
    client.clean(database_name)


def teardown_module():
    """
    tear down
    """
    pass
