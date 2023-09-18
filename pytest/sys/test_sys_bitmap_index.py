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
  * @file test_sys_alter_schema_change_modify.py
  * @date 2020-02-04
  * @brief This file is a test file for Palo bitmap index.
  * 
  **************************************************************************/
"""
import sys
import random
import time
sys.path.append("../")

from data import bitmap_index as DATA
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
    timeout = 30
    i = 0
    variable_value = ""
    # 设置default_rowset_type=beta
    # 检查10次variable，每次等待30s
    while i < 10:
        # 由于是global变量，必须每请求一次换一个client
        client_tmp = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
        set_variable_sql = "set global default_rowset_type=beta"
        r = client_tmp.execute(set_variable_sql)
        if variable_value == "beta":
            time.sleep(timeout)
            break
        time.sleep(timeout)
        variable_value = client_tmp.show_variables("default_rowset_type")[0][1]
        i = i + 1
    assert variable_value == "beta"
    print("alter global variable default_rowset_type from alpha to beta!")


def check_partition_list(table_name, partition_name_list):
    """
    验证分区是否创建成功
    """
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)


def partition_check(table_name, column_name, partition_name_list, \
        partition_value_list, distribution_type, bucket_num, storage_type, schema, \
        keys_desc=None, bitmap_index_list=None):
    """
    检查，验证
    """
    partition_info = palo_client.PartitionInfo(column_name, \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo(distribution_type, bucket_num)
    client.create_table(table_name, schema, \
            partition_info, distribution_info, keys_desc=keys_desc, bitmap_index_list=bitmap_index_list)
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)


def check(table_name, schema, keys_desc=None, bitmap_index_list=None):
    """
    分区，检查
    """
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '30', '100', 'MAXVALUE']
    partition_check(table_name, 'k1',
                    partition_name_list, partition_value_list,
                    'HASH(k1, k2)', random.randrange(1, 30), 'column', schema, keys_desc,\
                    bitmap_index_list)


def test_create_index_when_create_table():
    """
    {
    "title": "test_sys_bitmap_index.test_create_index_when_create_table",
    "describe": "test_create_index_when_create_table, 测试建表同时创建索引",
    "tag": "system,p1"
    }
    """
    """
    test_create_index_when_create_table
    测试建表同时创建索引
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client_exe = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                database_name = database_name, password=config.fe_password, http_port=config.fe_http_port)
    check(table_name, DATA.schema_agg_table_with_index_col, bitmap_index_list=DATA.schema_agg_table_with_index)            
    util.assert_return_flag(True,
                        client.is_exists_index_in_table, 'k1_idx', 'k1', table_name)
    client.clean(database_name)


def test_create_index_after_create_table():
    """
    {
    "title": "test_sys_bitmap_index.test_create_index_after_create_table",
    "describe": "test_create_index_after_create_table, 测试建表之后创建索引，使用两种语法创建：CREATE INDEX index_name ON table ... ;ALTER TABLE table ADD INDEX index_name ... ;",
    "tag": "system,p1"
    }
    """
    """
    test_create_index_after_create_table
    测试建表之后创建索引，使用两种语法创建：
    CREATE INDEX index_name ON table ...
    ALTER TABLE table ADD INDEX index_name ...
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client_exe = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                database_name = database_name, password=config.fe_password, http_port=config.fe_http_port)
    check(table_name, DATA.schema_agg_table)
    util.assert_return_flag(True,
                       client.create_bitmap_index_table, table_name, 'index_nameint', 'int_key',
                       is_wait=True)
    time.sleep(10)
    util.assert_return_flag(True,
                       client.create_bitmap_index_table, table_name, 'index_name_char', 'char_key',
                       create_format=2, is_wait=True)
    util.assert_return_flag(True,
                        client.is_exists_index_in_table, 'index_nameint', 'int_key', table_name)
    util.assert_return_flag(True,
                        client.is_exists_index_in_table, 'index_name_char', 'char_key', table_name)
    client.clean(database_name)


def test_create_index_with_data():
    """
    {
    "title": "test_sys_bitmap_index.test_create_index_with_data",
    "describe": "test_create_index_with_data, 测试表中有数据时创建索引",
    "tag": "system,p1"
    }
    """
    """
    test_create_index_with_data
    测试表中有数据时创建索引
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client_exe = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                database_name = database_name, password=config.fe_password, http_port=config.fe_http_port)
    check(table_name, DATA.schema_agg_table_simple)
    sql_insert = "insert into %s values(1, 1, 1, 1, 1),(2, 2, 2, 2, 2)" % table_name
    r = client_exe.execute(sql_insert)
    assert r == ()
    sql_select = "select * from %s" % table_name
    ret = client_exe.execute(sql_select)
    assert len(ret) == 2
    util.assert_return_flag(True,
                       client.create_bitmap_index_table, table_name, 'index_k1', 'k1',
                       is_wait=True)
    util.assert_return_flag(True,
                        client.is_exists_index_in_table, 'index_k1', 'k1', table_name)
    client.clean(database_name)


def test_delete_data_after_create_index():
    """
    {
    "title": "test_sys_bitmap_index.test_delete_data_after_create_index",
    "describe": "test_delete_data_after_create_index, 测试创建索引后删除数据",
    "tag": "system,p1"
    }
    """
    """
    test_delete_data_after_create_index
    测试创建索引后删除数据
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client_exe = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                database_name = database_name, password=config.fe_password, http_port=config.fe_http_port)
    check(table_name, DATA.schema_agg_table_simple)
    sql_insert = "insert into %s values(1, 1, 1, 1, 1),(2, 2, 2, 2, 2)" % table_name
    r = client_exe.execute(sql_insert)
    assert r == ()
    sql_select = "select * from %s" % table_name
    ret = client_exe.execute(sql_select)
    assert len(ret) == 2
    util.assert_return_flag(True,
                       client.create_bitmap_index_table, table_name, 'index_k1', 'k1',
                       is_wait=True)
    util.assert_return_flag(True,
                        client.is_exists_index_in_table, 'index_k1', 'k1', table_name)
    sql_insert = "delete from %s partition partition_a where k1 > 0" % table_name
    r = client_exe.execute(sql_insert)
    assert r == ()
    sql_select = "select * from %s" % table_name
    ret = client_exe.execute(sql_select)
    assert len(ret) == 0
    client.clean(database_name)


def test_drop_column_after_create_index():
    """
    {
    "title": "test_sys_bitmap_index.test_drop_column_after_create_index",
    "describe": "test_drop_column_after_create_index, 测试创建索引后删除列，预期索引一并删除",
    "tag": "system,p1,fuzz"
    }
    """
    """
    test_drop_column_after_create_index
    测试创建索引后删除列，预期索引一并删除
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client_exe = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                database_name = database_name, password=config.fe_password, http_port=config.fe_http_port)
    check(table_name, DATA.schema_agg_table_simple)
    util.assert_return_flag(True, 
                       client.create_bitmap_index_table, table_name, 'index_k3', 'k3',
                       is_wait=True)
    util.assert_return_flag(True, 
                        client.is_exists_index_in_table, 'index_k3', 'k3', table_name)
    time.sleep(10)
    # 删除列
    util.assert_return(True, '',
                       client.schema_change, table_name, drop_column_list = ["k3"],
                       is_wait=True)
    util.assert_return_flag(False,
                        client.is_exists_index_in_table, 'index_k3', 'k3', table_name)
    client.clean(database_name)


def test_create_index_with_alltype_data():
    """
    {
    "title": "test_sys_bitmap_index.test_create_index_with_alltype_data",
    "describe": "test_create_index_with_alltype_data, 测试各类支持的数据类型创建索引，包括TINYINT，SMALLINT，INT，BIGINT，CHAR，VARCHAR，DATE，DATETIME，LARGEINT，DECIMAL",
    "tag": "system,p1"
    }
    """
    """
    test_create_index_with_alltype_data
    测试各类支持的数据类型创建索引，包括TINYINT，SMALLINT，INT，
    BIGINT，CHAR，VARCHAR，DATE，DATETIME，LARGEINT，DECIMAL
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client_exe = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                database_name = database_name, password=config.fe_password, http_port=config.fe_http_port)
    check(table_name, DATA.schema_agg_table)
    util.assert_return_flag(True,
                       client.create_bitmap_index_table, table_name, 'index_tinyint', 'k1',
                       is_wait=True)
    util.assert_return_flag(True,
                        client.is_exists_index_in_table, 'index_tinyint', 'k1', table_name)
    time.sleep(10)
    util.assert_return_flag(True,
                       client.create_bitmap_index_table, table_name, 'index_smallint', 'k2',
                       is_wait=True)
    util.assert_return_flag(True,
                        client.is_exists_index_in_table, 'index_smallint', 'k2', table_name)
    time.sleep(10)
    util.assert_return_flag(True,
                       client.create_bitmap_index_table, table_name, 'index_int', 'int_key',
                       is_wait=True)
    util.assert_return_flag(True, 
                        client.is_exists_index_in_table, 'index_int', 'int_key', table_name)
    time.sleep(10)
    util.assert_return_flag(True, 
                       client.create_bitmap_index_table, table_name, 'index_bigint', 'bigint_key',
                       is_wait=True)
    util.assert_return_flag(True, 
                        client.is_exists_index_in_table, 'index_bigint', 'bigint_key', table_name)
    time.sleep(10)
    util.assert_return_flag(True,
                       client.create_bitmap_index_table, table_name, 'index_char_50_key', 'char_50_key',
                       is_wait=True)
    util.assert_return_flag(True, 
                        client.is_exists_index_in_table, 'index_char_50_key', 'char_50_key', table_name)
    time.sleep(10)
    util.assert_return_flag(True,
                       client.create_bitmap_index_table, table_name, 'index_character_key', 'character_key',
                       is_wait=True)
    util.assert_return_flag(True, 
                        client.is_exists_index_in_table, 'index_character_key', 'character_key', table_name)
    time.sleep(10)
    util.assert_return_flag(True, 
                       client.create_bitmap_index_table, table_name, 'index_date_key', 'date_key',
                       is_wait=True)
    util.assert_return_flag(True, 
                        client.is_exists_index_in_table, 'index_date_key', 'date_key', table_name)
    time.sleep(10)
    util.assert_return_flag(True,
                       client.create_bitmap_index_table, table_name, 'index_datetime_key', 'datetime_key',
                       is_wait=True)
    util.assert_return_flag(True, 
                        client.is_exists_index_in_table, 'index_datetime_key', 'datetime_key', table_name)
    client.clean(database_name)


def test_create_index_on_value_column():
    """
    {
    "title": "test_sys_bitmap_index.test_create_index_on_value_column",
    "describe": "test_create_index_on_value_column, 测试在agg表的非key列创建index",
    "tag": "system,p1,fuzz"
    }
    """
    """
    test_create_index_on_value_column
    测试在agg表的非key列创建index
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client_exe = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                database_name = database_name, password=config.fe_password, http_port=config.fe_http_port)
    check(table_name, DATA.schema_agg_table_simple)
    util.assert_return(False, '',
                       client.create_bitmap_index_table, table_name, 'index_v1', 'v1',
                       is_wait=True)
    client.clean(database_name)


def test_create_index_on_many_column():
    """
    {
    "title": "test_sys_bitmap_index.test_create_index_on_many_column",
    "describe": "test_create_index_on_many_column,测试在多列创建索引（联合索引，目前不支持）",
    "tag": "system,p1,fuzz"
    }
    """
    """
    test_create_index_on_many_column
    测试在多列创建索引（联合索引，目前不支持）
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client_exe = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                database_name = database_name, password=config.fe_password, http_port=config.fe_http_port)
    check(table_name, DATA.schema_agg_table_simple)
    util.assert_return(False, '',
                       client.create_bitmap_index_table, table_name, 'index_k1_k2', 'k1,k2',
                       is_wait=True)
    client.clean(database_name)


def test_create_index_repete():
    """
    {
    "title": "test_sys_bitmap_index.test_create_index_repete",
    "describe": "test_create_index_repete,测试重复创建同一索引",
    "tag": "system,p1,fuzz"
    }
    """
    """
    test_create_index_repete
    测试重复创建同一索引
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client_exe = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                database_name = database_name, password=config.fe_password, http_port=config.fe_http_port)
    check(table_name, DATA.schema_agg_table)
    util.assert_return_flag(True,
                       client.create_bitmap_index_table, table_name, 'index_tinyint', 'k1',
                       is_wait=True)
    util.assert_return_flag(True, 
                        client.is_exists_index_in_table, 'index_tinyint', 'k1', table_name)
    util.assert_return(False, '',
                       client.create_bitmap_index_table, table_name, 'index_tinyint_new', 'k1',
                       is_wait=True)
    client.clean(database_name)


def test_drop_index():
    """
    {
    "title": "test_sys_bitmap_index.test_drop_index",
    "describe": "test_drop_index,测试删除索引",
    "tag": "system,p1,fuzz"
    }
    """
    """
    test_drop_index
    测试删除索引
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client_exe = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                database_name = database_name, password=config.fe_password, http_port=config.fe_http_port)
    check(table_name, DATA.schema_agg_table)
    util.assert_return_flag(True, 
                       client.create_bitmap_index_table, table_name, 'index_tinyint', 'k1',
                       is_wait=True)
    util.assert_return_flag(True, 
                        client.is_exists_index_in_table, 'index_tinyint', 'k1', table_name)
    time.sleep(10)
    util.assert_return_flag(True, 
                        client.drop_bitmap_index_table, table_name, 'index_tinyint', is_wait=True)
    util.assert_return_flag(False, 
                       client.is_exists_index_in_table, 'index_tinyint', 'k1', table_name)
    client.clean(database_name)


def test_drop_index_not_exist():
    """
    {
    "title": "test_sys_bitmap_index.test_drop_index_not_exist",
    "describe": "test_drop_index_not_exist,测试删除不存在的索引",
    "tag": "system,p1,fuzz"
    }
    """
    """
    test_drop_index_not_exist
    测试删除不存在的索引
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client_exe = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                database_name = database_name, password=config.fe_password, http_port=config.fe_http_port)
    check(table_name, DATA.schema_agg_table)
    util.assert_return_flag(True,
                       client.create_bitmap_index_table, table_name, 'index_tinyint', 'k1',
                       is_wait=True)
    util.assert_return_flag(True,
                        client.is_exists_index_in_table, 'index_tinyint', 'k1', table_name)
    time.sleep(10)
    util.assert_return_flag(True, 
                        client.drop_bitmap_index_table, table_name, 'index_tinyint', is_wait=True)
    util.assert_return_flag(False, 
                       client.is_exists_index_in_table, 'index_tinyint', 'k1', table_name)
    time.sleep(10)
    util.assert_return(False, '',
                        client.drop_bitmap_index_table, table_name, 'index_tinyint', is_wait=True)
    client.clean(database_name)


def test_load_result_corr():
    """
    {
    "title": "test_sys_bitmap_index.test_load_result_corr",
    "describe": "test_load_result_corr,测试导入查询结果正确性",
    "tag": "system,p1"
    }
    """
    """
    test_load_result_corr
    测试导入查询结果正确性
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client_exe = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                database_name = database_name, password=config.fe_password, http_port=config.fe_http_port)
    check(table_name, DATA.schema_agg_table, bitmap_index_list=DATA.schema_agg_table_index)
    # 导入各类型数据
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path, table_name) 
    assert client.batch_load(util.get_label(), data_desc_list, max_filter_ratio=0.05, is_wait=True,
                             broker=broker_info)
    assert client.verify(DATA.expected_data_file_list, table_name)
    client.clean(database_name)


def test_create_index_on_dup_value_column():
    """
    {
    "title": "test_sys_bitmap_index.test_create_index_on_dup_value_column",
    "describe": "test_create_index_on_dup_value_column,测试在dup表的非key列创建index",
    "tag": "system,p1"
    }
    """
    """
    test_create_index_on_dup_value_column
    测试在dup表的非key列创建index
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client_exe = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                database_name = database_name, password=config.fe_password, http_port=config.fe_http_port)
    check(table_name, DATA.schema_dup, keys_desc=DATA.key_1_dup)
    util.assert_return_flag(True,
                       client.create_bitmap_index_table, table_name, 'index_v1', 'v1',
                       is_wait=True)
    util.assert_return_flag(True,
                        client.is_exists_index_in_table, 'index_v1', 'v1', table_name)
    client.clean(database_name)


def test_create_index_on_dup_float_double_column():
    """
    {
    "title": "test_sys_bitmap_index.test_create_index_on_dup_float_double_column",
    "describe": "test_create_index_on_dup_float_double_column, 测试在dup表的float，double列创建索引",
    "tag": "system,p1,fuzz"
    }
    """
    """
    test_create_index_on_dup_float_double_column
    测试在dup表的float，double列创建索引
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client_exe = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                database_name = database_name, password=config.fe_password, http_port=config.fe_http_port)
    check(table_name, DATA.schema_dup, keys_desc=DATA.key_1_dup)
    util.assert_return(False, '',
                       client.create_bitmap_index_table, table_name, 'index_v2', 'v2',
                       is_wait=True)
    util.assert_return(False, '', 
                       client.create_bitmap_index_table, table_name, 'index_v3', 'v3',
                       is_wait=True)
    client.clean(database_name)


def teardown_module():
    """
    tearDown
    """
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    timeout = 30
    i = 0
    variable_value = ""
    # 设置default_rowset_type=alpha
    # 检查10次variable，每次等待30s
    while i < 10:
        # 由于是global变量，必须每请求一次换一个client
        client_tmp = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
        set_variable_sql = "set global default_rowset_type=alpha"
        r = client_tmp.execute(set_variable_sql)
        if variable_value == "alpha":
            time.sleep(timeout)
            break
        time.sleep(timeout)
        variable_value = client_tmp.show_variables("default_rowset_type")[0][1]
        i = i + 1
    assert variable_value == "alpha"
    print("alter global variable default_rowset_type from beta to alpha!")
    print("End")


if __name__ == '__main__':
    import pdb
    pdb.set_trace()
    setup_module()
    test_create_index_when_create_table()
    test_create_index_after_create_table()
    test_create_index_with_data()
    test_delete_data_after_create_index()
    test_drop_column_after_create_index()
    test_create_index_with_alltype_data()
    test_create_index_on_value_column()
    test_create_index_on_many_column()
    test_create_index_repete()
    test_drop_index()
    test_drop_index_not_exist()
    test_load_result_corr()
    test_create_index_on_dup_value_column()
    test_create_index_on_dup_float_double_column()
    teardown_module()
