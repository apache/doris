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
  * @file test_sys_null.py
  * @date 2016/04/01 15:26:21
  * @brief Test for function - "NULL".
  * 
  **************************************************************************/
"""
import pytest

from data import support_null as DATA
from lib import palo_client
from lib import util
from lib import palo_config
from lib import common

config = palo_config.config
LOG = palo_client.LOG
L = palo_client.L
broker_info = palo_config.broker_info


def setup_module():
    """
    setUp
    """
    pass


def test_shortkey_null_a():
    """
    {
    "title": "test_sys_null.test_shortkey_null_a",
    "describe": "shortkey列为NULL",
    "tag": "system,p1"
    }
    """
    """
    shortkey列为NULL
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, 
               table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user, 
                                    password=config.fe_password, http_port=config.fe_http_port)
    assert client
    client.use(database_name)
    client.clean(database_name)
    client.create_database(database_name)
    client.create_table(table_name, DATA.schema_1, storage_type='column', set_null=True)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.hdfs_file_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    sql = 'SELECT * FROM %s.%s ORDER BY k1 nulls last, k2 nulls last, k3, k4, k5' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_list_data_1, sql=sql, client=client)
    client.clean(database_name)


def test_shortkey_null_b():
    """
    {
    "title": "test_sys_null.test_shortkey_null_b",
    "describe": "shortkey列为not NULL",
    "tag": "system,p1,fuzz"
    }
    """
    """
    shortkey列为not NULL
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, 
               table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    assert client
    client.clean(database_name)
    client.create_database(database_name)
    client.create_table(table_name, DATA.schema_1, storage_type='column')

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.hdfs_file_2, table_name)
    assert not client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    client.clean(database_name)


@pytest.mark.skip()
def test_null_kv():
    """
    {
    "title": "test_sys_null.test_null_kv",
    "describe": "定长类型和变长类型共12种数据类型为key和value时，指定为NULL列，导入含有NULL值的数据，验证数据正确性",
    "tag": "autotest"
    }
    """
    """
    定长类型和变长类型共12种数据类型为key和value时，指定为NULL列，导入含有NULL值的数据，验证数据正确性
    """
    # not null列, 非Null数据导入，校验
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    assert client
    client.clean(database_name)
    client.create_database(database_name)
    client.create_table(table_name, DATA.schema_2, storage_type='column', )

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.hdfs_file_3, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)

    sql = 'SELECT * FROM %s.%s ORDER BY k1' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_list_data_2, sql=sql, client=client)
    client.clean(database_name)


def test_order_by():
    """
    {
    "title": "test_sys_null.test_order_by",
    "describe": "测试排序",
    "tag": "system,p1"
    }
    """
    """
    测试排序
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    assert client
    client.clean(database_name)
    client.create_database(database_name)
    client.create_table(table_name, DATA.schema_2, storage_type='column', set_null=True)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.hdfs_file_4, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)

    sql = 'SELECT * FROM %s.%s ORDER BY k2 nulls last, k1 nulls last' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_4, sql=sql, client=client)
    client.clean(database_name)


def test_null_all():
    """
    {
    "title": "test_sys_null.test_null_all",
    "describe": "一行数据中，多个连续key和value列为NULL值,所有key列为NULL，验证聚合正确，验证数据正确,导入多条数据，这些数据的key列部分相同且含有NULL，验证导入后的聚合结果是否正确",
    "tag": "system,p1"
    }
    """
    """
    一行数据中，多个连续key和value列为NULL值
    所有key列为NULL，验证聚合正确，验证数据正确
    导入多条数据，这些数据的key列部分相同且含有NULL，验证导入后的聚合结果是否正确
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    assert client
    client.clean(database_name)
    client.create_database(database_name)
    client.create_table(table_name, DATA.schema_2, storage_type='column', set_null=True)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.hdfs_file_5, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)

    sql = 'SELECT * FROM %s.%s ' \
            'ORDER BY k2 nulls last, k1, k3, k4, k5, k6, k7, k8, k9, k10' \
            % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_5, sql=sql, client=client)

    sql = 'SELECT * FROM %s.%s WHERE K5 is NULL' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_5_1, sql=sql, client=client)
    client.clean(database_name)


@pytest.mark.skip()
def test_empty_string():
    """
    {
    "title": "test_sys_null.test_empty_string",
    "describe": "导入empty field代表空字符串，验证数据正确,空串的replace类型，聚合结果不符合预期",
    "tag": "autotest"
    }
    """
    """
    导入empty field代表空字符串，验证数据正确
    空串的replace类型，聚合结果不符合预期
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    assert client
    client.clean(database_name)
    client.create_database(database_name)
    client.create_table(table_name, DATA.schema_2, storage_type='column', set_null=True)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.hdfs_file_6, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)

    sql = 'SELECT * FROM %s.%s ' \
            'ORDER BY k2, k1, k3, k4, k5, k6, k7, k8, k9, k10' \
            % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_6, sql=sql, client=client)


def test_partition_col_null_hash():
    """
    {
    "title": "test_sys_null.test_partition_col_null_hash",
    "describe": "partition列为NULL，分布方式为hash,hash列为NULL",
    "tag": "system,p1"
    }
    """
    """
    partition列为NULL，分布方式为hash
    hash列为NULL
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    assert client
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['-5000', '0', '3000', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('k5', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('hash(k1, k2, k3)', 13) 
    client.create_table(table_name, DATA.schema_2, \
            partition_info, distribution_info, storage_type='column', set_null=True)
    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.hdfs_file_4, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)

    sql = 'SELECT * FROM %s.%s ' \
            'ORDER BY k2 nulls last, k1 nulls last, k3, k4, k5, k6, k7, k8, k9, k10' \
            % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_4, sql=sql, client=client)
    client.clean(database_name)


def test_define_as():
    """
    {
    "title": "test_sys_null.test_define_as",
    "describe": "通过NULL DEFINE AS指定特定的字符代表NULL，导入含有特定字符的数据，验证数据正确性",
    "tag": "system,p1,fuzz"
    }
    """
    """
    通过NULL DEFINE AS指定特定的字符代表NULL，导入含有特定字符的数据，验证数据正确性
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    assert client
    client.clean(database_name)
    client.create_database(database_name)
    client.create_table(table_name, DATA.schema_2, storage_type='column', set_null=True)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    property_list = ["k1 = replace_value('\\\\0xU', NULL)", \
            "k2 = replace_value('\\\\0xU', NULL)", \
            "k3 = replace_value('\\\\0xU', NULL)", \
            "k4 = replace_value('\\\\0xN', NULL)"]

    data_desc_list = palo_client.LoadDataInfo(DATA.hdfs_file_7, table_name, set_list=property_list)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
            #property_list = property_list, 

    sql = 'SELECT * FROM %s.%s ORDER BY k2 nulls last, k1 nulls last' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_7, sql=sql, client=client)
    client.clean(database_name)


@pytest.mark.skip()
def test_null_be():
    """
    {
    "title": "test_sys_null.test_null_be",
    "describe": "导入后，触发BE/CE，验证数据正确性, disable",
    "tag": "autotest"
    }
    """
    """
    导入后，触发BE/CE，验证数据正确性
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    assert client
    client.clean(database_name)
    client.create_database(database_name)
    assert client.use(database_name)

    client.create_table(table_name, DATA.schema_2, storage_type='column', set_null=True)
    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    label_1 = 'label_1'
    label_2 = 'label_2'
    label_3 = 'label_3'
    label_4 = 'label_4'
    label_5 = 'label_5'

    data_desc_list_1 = palo_client.LoadDataInfo(DATA.hdfs_file_8_1, table_name)
    data_desc_list_2 = palo_client.LoadDataInfo(DATA.hdfs_file_8_2, table_name)
    data_desc_list_3 = palo_client.LoadDataInfo(DATA.hdfs_file_8_3, table_name)
    data_desc_list_4 = palo_client.LoadDataInfo(DATA.hdfs_file_8_4, table_name)
    data_desc_list_5 = palo_client.LoadDataInfo(DATA.hdfs_file_8_5, table_name)

    assert client.batch_load(label_1, data_desc_list_1, broker=broker_info)
    assert client.batch_load(label_2, data_desc_list_2, broker=broker_info)
    assert client.batch_load(label_3, data_desc_list_3, broker=broker_info)
    assert client.batch_load(label_4, data_desc_list_4, broker=broker_info)
    assert client.batch_load(label_5, data_desc_list_5, broker=broker_info)

    assert client.wait_load_job(label_1)
    assert client.wait_load_job(label_2)
    assert client.wait_load_job(label_3)
    assert client.wait_load_job(label_4)
    assert client.wait_load_job(label_5)

    tablet_id, host_name = client.get_tablet_id_and_be_host_list(table_name)[0]
    print(tablet_id, host_name)
    
    # assert env.wait_be(host_name, tablet_id)

    sql = 'SELECT * FROM %s.%s ORDER BY k2, k1' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_8, sql=sql, client=client)


def test_schema_change_add_null():
    """
    {
    "title": "test_sys_null.test_schema_change_add_null",
    "describe": "增加新列为NULL列",
    "tag": "system,p1"
    }
    """
    """
    增加新列为NULL列
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    assert client
    client.clean(database_name)
    client.create_database(database_name)
    client.create_table(table_name, DATA.schema_2, storage_type='column', set_null=True)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.hdfs_file_4, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)

    sql = 'SELECT * FROM %s.%s ORDER BY k2 nulls last, k1 nulls last' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_4, sql=sql, client=client)

    column_list = [('v11', 'INT', 'SUM'),]
    assert client.schema_change_add_column(table_name, column_list, \
            after_column_name='v10', is_wait_job=True, set_null=True)

    sql = 'SELECT * FROM %s.%s ORDER BY k2 nulls last, k1 nulls last' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_9, sql=sql, client=client)
    client.clean(database_name)
    """
    # disable check be/ce 
    tablet_id, host_name = client.get_tablet_id_and_be_host_list(table_name)[0]
    print(tablet_id, host_name)
    assert client.delete(table_name, [('k1', '>', '127'),], table_name)
    assert client.delete(table_name, [('k1', '>', '127'),], table_name)
    assert client.delete(table_name, [('k1', '>', '127'),], table_name)
    assert client.delete(table_name, [('k1', '>', '127'),], table_name)
    assert client.delete(table_name, [('k1', '>', '127'),], table_name)

    #assert env.restart_be(host_name)
    assert env.wait_be(host_name, tablet_id)

    assert client.verify_without_schema(sql, DATA.expected_file_9)

    data_desc_list = palo_client.LoadDataInfo(DATA.hdfs_file_9_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True)

    sql = 'SELECT * FROM %s.%s ORDER BY k2, k1' % (database_name, table_name)
    assert client.verify_without_schema(sql, DATA.expected_file_9_1)
    """

 
def test_schema_change_add_key_null():
    """
    {
    "title": "test_sys_null.test_schema_change_add_key_null",
    "describe": "增加新列为NULL列",
    "tag": "system,p1"
    }
    """
    """
    增加新列为NULL列
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    assert client
    client.clean(database_name)
    client.create_database(database_name)
    client.create_table(table_name, DATA.schema_2, storage_type='column', set_null=True)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.hdfs_file_4, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)

    sql = 'SELECT * FROM %s.%s ORDER BY k2 nulls last, k1 nulls last' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_4, sql=sql, client=client)

    column_list = [('add_key', 'INT', None),]
    assert client.schema_change_add_column(table_name, column_list,
            after_column_name='k2', is_wait_job=True, set_null=True)

    sql = 'SELECT * FROM %s.%s ORDER BY k2 nulls last, k1 nulls last' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_10, sql=sql, client=client)

    set_list = ['add_key = replace_value("None", NULL)']
    data_desc_list = palo_client.LoadDataInfo(DATA.hdfs_file_10_1, table_name, set_list=set_list)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)

    sql = 'SELECT * FROM %s.%s ORDER BY k2 nulls last, k1 nulls last' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_10_1, sql=sql, client=client)
    client.clean(database_name)
 

def test_schema_change_add_key_not_null():
    """
    {
    "title": "test_sys_null.test_schema_change_add_key_not_null",
    "describe": "增加新列为NULL列",
    "tag": "system,p1"
    }
    """
    """
    增加新列为NULL列
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name))
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    assert client
    client.clean(database_name)
    client.create_database(database_name)
    client.create_table(table_name, DATA.schema_2, storage_type='column', set_null=True)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.hdfs_file_4, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)

    sql = 'SELECT * FROM %s.%s ORDER BY k2 nulls last, k1 nulls last' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_4, sql=sql, client=client)

    column_list = [('add_key', 'INT', None, '1'),]
    assert client.schema_change_add_column(table_name, column_list,
            after_column_name='k2', is_wait_job=True, set_null=False)

    sql = 'SELECT * FROM %s.%s ORDER BY k2 nulls last, k1 nulls last' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_n10, sql=sql, client=client)

    # set_list = ['add_key = replace_value("None", NULL)']
    # data_desc_list = palo_client.LoadDataInfo(DATA.hdfs_file_10_1, table_name, set_list=set_list)
    column_list = ['k1', 'k2', 'add_key', 'k3', 'k4', 'k5', 'k6', 'k7', 'k8', 'k9', 'k10', 'v1', 
                   'v2', 'v3', 'v4', 'v5', 'v6', 'v7', 'v8', 'v9']
    data_desc_list = palo_client.LoadDataInfo(DATA.hdfs_file_10_1, table_name,
                                              column_name_list=column_list)
    assert not client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)

    sql = 'SELECT * FROM %s.%s ORDER BY k2 nulls last, k1 nulls last' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_n10, sql=sql, client=client)
    client.clean(database_name)


def test_not_null():
    """
    {
    "title": "test_sys_null.test_not_null",
    "describe": "not null约束, 删除NOT NULL约束，导入NULL数据",
    "tag": "system,p1,fuzz"
    }
    """
    """
    not null约束
    删除NOT NULL约束，导入NULL数据
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name))
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    assert client
    client.clean(database_name)
    client.create_database(database_name)
    distribution_info = palo_client.DistributionInfo('HASH(k3)', 5)
    client.create_table(table_name, DATA.schema_3, storage_type='column', 
                        distribution_info=distribution_info)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.hdfs_file_4, table_name)
    assert not client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.schema_change_modify_column(table_name, 'k1', 'TINYINT NULL', 
                                           is_wait_job=True, is_wait_delete_old_schema=True)
    assert client.schema_change_modify_column(table_name, 'k2', 'SMALLINT NULL', \
            is_wait_job=True, is_wait_delete_old_schema=True)
    assert client.schema_change_modify_column(table_name, 'k4', 'BIGINT NULL', is_wait_job=True)

    data_desc_list = palo_client.LoadDataInfo(DATA.hdfs_file_4, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)

    sql = 'SELECT * FROM %s.%s ORDER BY k2 nulls last, k1 nulls last' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_4, sql=sql, client=client)
    client.clean(database_name)


@pytest.mark.skip()
def test_schema_change_order_by():
    """
    {
    "title": "test_sys_null.test_schema_change_order_by",
    "describe": "重排序导致shortkey列变化，NULL列提前，导入数据，验证数据正确",
    "tag": "autotest"
    }
    """
    """
    重排序导致shortkey列变化，NULL列提前，导入数据，验证数据正确
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    assert client
    client.clean(database_name)
    client.create_database(database_name)
    client.create_table(table_name, DATA.schema_2, storage_type='column', set_null=True)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.hdfs_file_4, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)

    sql = 'SELECT * FROM %s.%s ORDER BY k2, k1' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_4, sql=sql, client=client)

#   column_list = [('add_key', 'INT', None),]
#   assert client.schema_change_add_column(table_name, column_list, \
#           after_column_name='k2', is_wait_job=True)

#   sql = 'SELECT * FROM %s.%s ORDER BY k2, k1' % (database_name, table_name)
#   assert client.verify_without_schema(sql, DATA.expected_file_10)

    column_name_list = ['k10', 'k5', 'k1', 'k2', 'k3', 'k4', \
            'k6', 'k7', 'k8', 'k9', 'v5', 'v1', 'v2', 'v3', 'v4', 'v7', 'v8', 'v6', 'v9', 'v10']
    assert client.schema_change_order_column(table_name, column_name_list, is_wait_job=True)

    sql = 'SELECT * FROM %s.%s ORDER BY k2, k1' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_11, sql=sql, client=client)
    
    """
    # disable check be
    tablet_id, host_name = client.get_tablet_id_and_be_host_list(table_name)[0]
    print(tablet_id, host_name)
    assert client.delete(table_name, [('k1', '>', '127'),], table_name)
    assert client.delete(table_name, [('k1', '>', '127'),], table_name)
    assert client.delete(table_name, [('k1', '>', '127'),], table_name)
    assert client.delete(table_name, [('k1', '>', '127'),], table_name)
    assert client.delete(table_name, [('k1', '>', '127'),], table_name)
    assert client.delete(table_name, [('k1', '>', '127'),], table_name)

    #assert env.restart_be(host_name)
    assert env.wait_be(host_name, tablet_id)

    sql = 'SELECT * FROM %s.%s ORDER BY k2, k1' % (database_name, table_name)
    assert client.verify_without_schema(sql, DATA.expected_file_11)
    """


def test_rollup():
    """
    {
    "title": "test_sys_null.test_rollup",
    "describe": "创建含有NULL列的rollup表，导入数据，验证命中rollup表的查询结果正确",
    "tag": "system,p1"
    }
    """
    """
    创建含有NULL列的rollup表，导入数据，验证命中rollup表的查询结果正确
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    assert client
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.schema_2, storage_type='column', set_null=True)
    rollup_field = ['k4', 'k1', 'k3', 'k2', 'v1']
    client.create_rollup_table(table_name, index_name, rollup_field, is_wait=True)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_index(table_name, index_name=index_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.hdfs_file_12, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)

    #sql = 'SELECT * FROM %s.%s ORDER BY k2, k1' % (database_name, table_name)
    #assert client.verify_without_schema(sql, DATA.expected_file_4)

    sql = 'SELECT k4, k1, k3, k2, SUM(v1) ' \
            'FROM %s GROUP BY k4, k1, k3, k2 ORDER BY k1' % (table_name)
    shoot_table = common.get_explain_rollup(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert index_name in shoot_table

    sql = 'SELECT k4, k1, k3, k2, SUM(v1) FROM %s ' \
            'GROUP BY k4, k1, k3, k2 ORDER BY k1 nulls last, k2 nulls last' % (table_name)
    # ORDER BY K1
    assert common.check_by_file(DATA.expected_file_12, sql=sql, client=client)
    client.clean(database_name)


@pytest.mark.skip()
def test_load_rollup():
    """
    {
    "title": "test_sys_null.test_load_rollup",
    "describe": "创建含有NULL列的rollup表，导入数据，验证命中rollup表的查询结果正确",
    "tag": "autotest"
    }
    """
    """
    创建含有NULL列的rollup表，导入数据，验证命中rollup表的查询结果正确
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    assert client
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.schema_2, storage_type='column', set_null=True)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.hdfs_file_12, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)

    rollup_field = ['k4', 'k1', 'k3', 'k2', 'v1']
    client.create_rollup_table(table_name, index_name, rollup_field, is_wait=True)
    assert client.get_index(table_name, index_name=index_name)
    #sql = 'SELECT * FROM %s.%s ORDER BY k2, k1' % (database_name, table_name)
    #assert client.verify_without_schema(sql, DATA.expected_file_4)

    sql = 'SELECT k4, k1, k3, k2, SUM(v1) FROM %s ' \
            'GROUP BY k4, k1, k3, k2 ORDER BY k1' % (table_name)
    shoot_table = common.get_explain_rollup(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert index_name in shoot_table, 'expect rollup %s, but %s' % (index_name, shoot_table)

    sql = 'SELECT k4, k1, k3, k2, SUM(v1) FROM %s ' \
            'GROUP BY k4, k1, k3, k2 ORDER BY k1, k2' % (table_name)
    assert common.check_by_file(DATA.expected_file_12, sql=sql, client=client)
    """
    # disable check be
    tablet_id, host_name = client.get_tablet_id_and_be_host_list(table_name)[0]
    print(tablet_id, host_name)
    time.sleep(15)
    assert client.delete(table_name, [('k1', '>', '126'),], table_name)
#   time.sleep(2)
#   assert client.delete(table_name, [('k1', '>', '127'),], table_name)
#   time.sleep(2)
#   assert client.delete(table_name, [('k1', '>', '127'),], table_name)
#   time.sleep(2)
#   assert client.delete(table_name, [('k1', '>', '127'),], table_name)
#   time.sleep(2)
#   assert client.delete(table_name, [('k1', '>', '127'),], table_name)
#   assert env.wait_be(host_name, tablet_id)

#   sql = 'SELECT k4, k1, k3, k2, SUM(v1) FROM %s ' \
#           'GROUP BY k4, k1, k3, k2 ORDER BY k1' % (table_name)
#   # ORDER BY K1
#   assert client.verify_without_schema(sql, DATA.expected_file_12)
    """


@pytest.mark.skip()
def test_delete():
    """
    {
    "title": "test_sys_null.test_delete",
    "describe": "测试delete",
    "tag": "autotest"
    }
    """
    """
    测试delete
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 

    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    assert client
    client.clean(database_name)
    client.create_database(database_name)
    assert client.use(database_name)

    client.create_table(table_name, DATA.schema_2, storage_type='column', set_null=True)
    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    label_1 = 'label_1'
    label_2 = 'label_2'
    label_3 = 'label_3'
    label_4 = 'label_4'
    label_5 = 'label_5'

    data_desc_list_1 = palo_client.LoadDataInfo(DATA.hdfs_file_8_1, table_name)
    data_desc_list_2 = palo_client.LoadDataInfo(DATA.hdfs_file_8_2, table_name)
    data_desc_list_3 = palo_client.LoadDataInfo(DATA.hdfs_file_8_3, table_name)
    data_desc_list_4 = palo_client.LoadDataInfo(DATA.hdfs_file_8_4, table_name)
    data_desc_list_5 = palo_client.LoadDataInfo(DATA.hdfs_file_8_5, table_name)

    assert client.batch_load(label_1, data_desc_list_1, broker=broker_info)
    assert client.wait_load_job(label_1)
    assert client.delete(table_name, [('k5', '=', '-90000'),], table_name)
    assert client.batch_load(label_2, data_desc_list_2, broker=broker_info)
    assert client.wait_load_job(label_2)
    assert client.delete(table_name, [('k5', '=', '-70000'),], table_name)
    assert client.batch_load(label_3, data_desc_list_3, broker=broker_info)
    assert client.batch_load(label_4, data_desc_list_4, broker=broker_info)
    assert client.batch_load(label_5, data_desc_list_5, broker=broker_info)

    assert client.wait_load_job(label_3)
    assert client.wait_load_job(label_4)
    assert client.wait_load_job(label_5)

    sql = 'SELECT * FROM %s.%s ORDER BY k2, k1' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_13, sql=sql, client=client)


def test_delete_and_schema_change():
    """
    {
    "title": "test_sys_null.test_delete_and_schema_change",
    "describe": "测试delete and schema change",
    "tag": "system,p1"
    }
    """
    """
    测试delete and schema change
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    assert client
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.schema_2, storage_type='column', set_null=True)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.hdfs_file_4, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    sql = 'SELECT * FROM %s.%s ORDER BY k2 nulls last, k1 nulls last' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_4, sql=sql, client=client)

    assert client.delete(table_name, [('k1', '>=', '127'),], table_name)
    sql = 'SELECT * FROM %s.%s ORDER BY k2 nulls last, k1 nulls last' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_ds_1, sql=sql, client=client)

    column_list = [('add_key', 'INT', None), ]
    assert client.schema_change_add_column(table_name, column_list, 
            after_column_name='k2', is_wait_job=True, set_null=True)
    sql = 'SELECT * FROM %s.%s ORDER BY k2 nulls last, k1 nulls last' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_list_data_2, sql=sql, client=client)

    assert client.delete(table_name, [('k1', '>', '127'),], table_name)
    assert client.delete(table_name, [('k1', '>', '127'),], table_name)
    assert client.delete(table_name, [('k1', '>', '127'),], table_name)
    assert client.delete(table_name, [('k1', '>', '127'),], table_name)
    assert client.delete(table_name, [('k1', '>', '127'),], table_name)
    assert client.delete(table_name, [('k1', '>', '127'),], table_name)

    sql = 'SELECT * FROM %s.%s ORDER BY k2 nulls last, k1 nulls last' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_ds_2, sql=sql, client=client)
    client.clean(database_name)


def test_largeint():
    """
    {
    "title": "test_sys_null.test_largeint",
    "describe": "测试largeint",
    "tag": "system,p1"
    }
    """
    """
    测试largeint
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    assert client
    client.clean(database_name)
    client.create_database(database_name)
    client.create_table(table_name, DATA.schema_4, storage_type='column', set_null=True)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.hdfs_file_14_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)

    data_desc_list = palo_client.LoadDataInfo(DATA.hdfs_file_14_2, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)

    sql = 'SELECT * FROM %s.%s ORDER BY k2 nulls last, k1 nulls last' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_14, sql=sql, client=client)
    client.clean(database_name)


def test_all_null():
    """
    {
    "title": "test_sys_null.test_all_null",
    "describe": "测试 all null",
    "tag": "system,p1"
    }
    """
    """
    测试 all null
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    assert client
    client.clean(database_name)
    client.create_database(database_name)
    client.create_table(table_name, DATA.schema_4, storage_type='column', set_null=True)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.hdfs_file_all_null, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)

    sql = 'SELECT * FROM %s.%s ORDER BY k2, k1' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_all_null, sql=sql, client=client)
    client.clean(database_name)


def test_bloom_filter():
    """
    {
    "title": "test_sys_null.test_bloom_filter",
    "describe": "测试bloom filter",
    "tag": "system,p1"
    }
    """
    """
    测试bloom filter
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    assert client
    client.clean(database_name)
    client.create_database(database_name)
    bloom_filter_column_list = ['k5', 'k7', 'k9']
    client.create_table(table_name, DATA.schema_2, storage_type='column', 
            bloom_filter_column_list=bloom_filter_column_list, set_null=True)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.hdfs_file_5, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)

    sql = 'SELECT * FROM %s.%s where ' \
            'k5 = -170141183460469231731687303715884105727' \
            % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_15_1, sql=sql, client=client)

    sql = 'SELECT * FROM %s.%s where k5 is NULL' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_15_1_1, sql=sql, client=client)

    sql = 'SELECT * FROM %s.%s where k7 = "2452-09-22 13:38:55"' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_15_2, sql=sql, client=client)

    sql = 'SELECT * FROM %s.%s where k7 in ' \
            '("2452-09-22 13:38:55", "2479-02-15 20:33:27") order by k1' \
            % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_15_2_1, sql=sql, client=client)

    sql = 'SELECT * FROM %s.%s where k9 = "中文"' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_15_3, sql=sql, client=client)

    sql = 'SELECT * FROM %s.%s where v7 = "2677-02-03"' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_15_4, sql=sql, client=client)

    sql = 'SELECT * FROM %s.%s where v9 = "d"' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_15_4, sql=sql, client=client)

    sql = 'SELECT * FROM %s.%s where k1 is NULL ' \
            'and k2 is NULL and k3 is NULL and k4 is NULL and k6 is NULL ' \
            'and k7 is NULL and k8 is NULL and k9 is NULL and k10 is NULL ' \
            'and v1 is NULL and v2 is NULL and v3 is NULL and v4 is NULL ' \
            'and v6 is NULL and v7 is NULL and v8 is NULL and v9 is NULL ' \
            'and v10 is NULL' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_15_1_1, sql=sql, client=client)
    client.clean(database_name)


def test_join():
    """
    {
    "title": "test_sys_null.test_join",
    "describe": "测试join",
    "tag": "system,p1"
    }
    """
    """
    测试join
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    assert client
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.schema_2, storage_type='column', set_null=True)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.hdfs_file_4, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)

    sql = 'SELECT a.v1, b.v2 FROM %s as a, %s as b ' \
            'WHERE a.k1=b.k1 ORDER BY a.k2 nulls last' % (table_name, table_name)
    assert common.check_by_file(DATA.expected_file_16, sql=sql, client=client)
    client.clean(database_name)

 
def test_varchar_hash_null():
    """
    {
    "title": "test_sys_null.test_varchar_hash_null",
    "describe": "varchar字段分桶字段多个版本全是NULL",
    "tag": "system,p1"
    }
    """
    """
    varchar字段分桶字段多个版本全是NULL，会出现core
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    assert client
    client.clean(database_name)
    client.create_database(database_name)
    distribution_info = palo_client.DistributionInfo('HASH(K9)', 13)
    client.create_table(table_name, DATA.schema_2, \
            distribution_info = distribution_info, storage_type='column', set_null=True)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.hdfs_file_17, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    sql = 'SELECT * FROM %s.%s ORDER BY k2 nulls last, k1 nulls last' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_17, sql=sql, client=client)
    client.clean(database_name)


def test_modify():
    """
    {
    "title": "test_sys_null.test_modify",
    "describe": "测试modify",
    "tag": "system,p1"
    }
    """
    """
    测试modify
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    assert client
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    distribution_info = palo_client.DistributionInfo('HASH(K2)', 13)
    client.create_table(table_name, DATA.schema_2, distribution_info = distribution_info, 
                        storage_type='column', set_null=True)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.hdfs_file_4, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)

    sql = 'ALTER TABLE %s.%s ' % (database_name, table_name) + \
            'MODIFY COLUMN k1 LARGEINT NULL, MODIFY COLUMN k9 VARCHAR(1024) NULL, ' + \
            'MODIFY COLUMN v2 LARGEINT SUM NULL, MODIFY COLUMN v9 VARCHAR(1024) REPLACE NULL'

    assert () == client.execute(sql)
    client.wait_table_schema_change_job(table_name, database_name)

    sql = 'SELECT * FROM %s.%s ORDER BY k2 nulls last, k1 nulls last' % (database_name, table_name)
    assert common.check_by_file(DATA.expected_file_4, sql=sql, client=client)
    client.clean(database_name)


if __name__ == '__main__':
    setup_module()
    test_null_be()
