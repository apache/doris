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
  * @file test_sys_schema_change.py
  * @date 2015/02/04 15:26:21
  * @brief This file is a test file for Palo schema changing.
  * 
  **************************************************************************/
"""

import sys
sys.path.append("../")
import time
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


def test_order_k1_k2_v2_v1_v3():
    """
    {
    "title": "test_sys_schema_change_order_aggregate.test_order_k1_k2_v2_v1_v3",
    "describe": "重新排序: k1 k2 v2 v1 v3",
    "tag": "function,p1"
    }
    """
    """
    重新排序: k1 k2 v2 v1 v3
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)
    distribution_info = palo_client.DistributionInfo('HASH(k1, k2)', 10) 
    client.create_table(table_name, DATA.schema_1, distribution_info=distribution_info, \
            keys_desc='AGGREGATE KEY (k1, k2)')

    time.sleep(1)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_1, table_name)
    assert ret

    column_name_list = ['k1', 'k2', 'v2', 'v1', 'v3']
    ret = client.schema_change_order_column(table_name, column_name_list, \
            is_wait_job=True, is_wait_delete_old_schema=True)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_12, table_name)
    assert ret
    client.clean(database_name)


def test_order_k1_k2_v3_v2_v1():
    """
    {
    "title": "test_sys_schema_change_order_aggregate.test_order_k1_k2_v3_v2_v1",
    "describe": "重新排序: k1 k2 v3 v2 v1",
    "tag": "function,p1"
    }
    """
    """
    重新排序: k1 k2 v3 v2 v1
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)
    distribution_info = palo_client.DistributionInfo('HASH(k1, k2)', 10) 
    client.create_table(table_name, DATA.schema_1, distribution_info=distribution_info, \
            keys_desc='AGGREGATE KEY (k1, k2)')

    time.sleep(1)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_1, table_name)
    assert ret

    column_name_list = ['k1', 'k2', 'v3', 'v2', 'v1']
    ret = client.schema_change_order_column(table_name, column_name_list, \
            is_wait_job=True, is_wait_delete_old_schema=True)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_13, table_name)
    assert ret
    client.clean(database_name)


def test_order_k1_k2_v2_v3_v1():
    """
    {
    "title": "test_sys_schema_change_order_aggregate.test_order_k1_k2_v2_v3_v1",
    "describe": "重新排序: k1 k2 v2 v3 v1",
    "tag": "function,p1"
    }
    """
    """
    重新排序: k1 k2 v2 v3 v1
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)
    distribution_info = palo_client.DistributionInfo('HASH(k1, k2)', 10) 
    client.create_table(table_name, DATA.schema_1, distribution_info=distribution_info, \
            keys_desc='AGGREGATE KEY (k1, k2)')

    time.sleep(1)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_1, table_name)
    assert ret

    column_name_list = ['k1', 'k2', 'v2', 'v3', 'v1']
    ret = client.schema_change_order_column(table_name, column_name_list, \
            is_wait_job=True, is_wait_delete_old_schema=True)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_14, table_name)
    assert ret
    client.clean(database_name)


def test_order_k2_k1_v3_v2_v1():
    """
    {
    "title": "test_sys_schema_change_order_aggregate.test_order_k2_k1_v3_v2_v1",
    "describe": "重新排序: k2 k1 v3 v2 v1",
    "tag": "function,p1"
    }
    """
    """
    重新排序: k2 k1 v3 v2 v1
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)
    distribution_info = palo_client.DistributionInfo('HASH(k1, k2)', 10) 
    client.create_table(table_name, DATA.schema_1, distribution_info=distribution_info, \
            keys_desc='AGGREGATE KEY (k1, k2)')

    time.sleep(1)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_1, table_name)
    assert ret

    column_name_list = ['k2', 'k1', 'v3', 'v2', 'v1']
    ret = client.schema_change_order_column(table_name, column_name_list, \
            is_wait_job=True, is_wait_delete_old_schema=True)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_15, table_name)
    assert ret
    client.clean(database_name)


def test_k2_k1_v3_v2_v4():
    """
    {
    "title": "test_sys_schema_change_order_aggregate.test_k2_k1_v3_v2_v4",
    "describe": "混合，增加 v4，删除 v1，排序 k2 k1 v3 v2 v4",
    "tag": "function,p1"
    }
    """
    """
    混合，增加 v4，删除 v1，排序 k2 k1 v3 v2 v4
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)
    distribution_info = palo_client.DistributionInfo('HASH(k1, k2)', 10) 
    client.create_table(table_name, DATA.schema_1, distribution_info=distribution_info, \
            keys_desc='AGGREGATE KEY (k1, k2)')

    time.sleep(1)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_1, table_name)
    assert ret

    sql = 'ALTER TABLE %s.%s' % (database_name, table_name)
    sql = '%s ADD COLUMN v4 INT SUM DEFAULT "0" AFTER v3,' % sql
    sql = '%s DROP COLUMN v1, ORDER BY(k2, k1, v3, v2, v4)' % sql
    ret = client.execute(sql)
    assert ret == ()
    client.wait_table_schema_change_job(table_name, database_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_2, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    ret = client.verify(DATA.expected_data_file_list_16_agg_new, table_name)
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

