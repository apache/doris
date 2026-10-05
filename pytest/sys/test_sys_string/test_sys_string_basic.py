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
#   @file test_sys_string_basic.py
#   @date 2021-09-01 11:16:39
#   @brief This file is a test file for string type.
#
#############################################################################
"""
test_sys_string_basic.py
string不能作为key列，最大长度1M, 1048576
"""
import sys
import os
import time

file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(file_dir)
file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(file_dir)

from lib import palo_config
from lib import palo_client
from lib import util
from lib import common
from lib import palo_job
from data import schema
from data import load_file
from data import pull_load_apache as DATA
config = palo_config.config
broker_info = palo_config.broker_info


def setup_module():
    """setup"""
    client = common.get_client()


def teardown_module():
    """teardown"""
    pass


def test_string_key():
    """
    {
    "title": "",
    "describe": "string类型，不支持作为key列",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column = [("k1", "STRING"), ("v1", "STRING")]
    msg = 'The olap table first column could not be float, double, string or array'
    util.assert_return(False, msg, client.create_table, table_name, column)
    column = [("k1", "INT"), ("k2", "STRING")]
    msg = 'String Type should not be used in key column[k2].'
    util.assert_return(False, msg, client.create_table, table_name, column, keys_desc='DUPLICATE KEY(k1,k2)')
    client.clean(database_name)


def test_agg_table_replace():
    """
    {
    "title": "",
    "describe": "string类型，作为agg表value列，replace，min，max聚合，支持，导入成功，数据正确",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column = [("k1", "VARCHAR(65533)"), ("v1", "STRING", "REPLACE")]
    ret = client.create_table(table_name, column)
    assert ret, 'create table failed'
    insert_sql = 'INSERT INTO %s VALUES(1, %s)'
    values = ['"test"', 'repeat("test111111", 1000)',
              'repeat("test111111", 10000)', 
              'repeat("test111111", 100000)']
    for v in values:
        client.execute(insert_sql % (table_name, v))
    # check
    sql1 = 'select k1, md5(v1), length(v1) from %s' % table_name
    sql2 = 'select 1, "22bf05240aa95737fa343072fc612bf2", 1000000' # repeat("test111111", 100000)
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_agg_table_max():
    """
    {
    "title": "",
    "describe": "string类型，作为agg表value列，replace，min，max聚合，支持，导入成功，数据正确",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column = [("k1", "VARCHAR(65533)"), ("v1", "STRING", "MAX")]
    ret = client.create_table(table_name, column)
    assert ret, 'create table failed'
    insert_sql = 'INSERT INTO %s VALUES(1, %s)'
    values = ['"test"', 'repeat("test111111", 1000)',
              'repeat("test111111", 10000)',
              'repeat("test111111", 100000)']
    for v in values:
        client.execute(insert_sql % (table_name, v))
    # check
    sql1 = 'select k1, md5(v1), length(v1) from %s' % table_name
    sql2 = 'select 1, "22bf05240aa95737fa343072fc612bf2", 1000000' # repeat("test111111", 200000000)
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_agg_table_min():
    """
    {
    "title": "",
    "describe": "string类型，作为agg表value列，replace，min，max聚合，支持，导入成功，数据正确",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column = [("k1", "VARCHAR(65533)"), ("v1", "STRING", "MIN")]
    ret = client.create_table(table_name, column)
    assert ret, 'create table failed'
    insert_sql = 'INSERT INTO %s VALUES(1, %s)'
    values = ['"test"', 'repeat("test111111", 1000)',
              'repeat("test111111", 10000)',
              'repeat("test111111", 100000)']
    for v in values:
        client.execute(insert_sql % (table_name, v))
    sql1 = 'select k1, md5(v1), v1 from %s' % table_name
    sql2 = 'select 1, "098f6bcd4621d373cade4e832627b4f6", "test"' # test, 'test' < 'test1'
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_dup_table():
    """
    {
    "title": "",
    "describe": "string类型，作为dup表value列，支持，导入成功，数据正确",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column = [("k1", "VARCHAR(65533)"), ("v1", "STRING")]
    ret = client.create_table(table_name, column, keys_desc='DUPLICATE KEY(k1)')
    assert ret, 'create table failed'
    insert_sql = 'INSERT INTO %s VALUES(1, %s)'
    values = ['"test"', 'repeat("test111111", 1000)',
              'repeat("test111111", 10000)',
              'repeat("test111111", 100000)']
    for v in values:
        client.execute(insert_sql % (table_name, v))
    # check
    sql1 = 'select k1, md5(v1), length(v1) from %s' % table_name
    sql2 = 'select 1, "098f6bcd4621d373cade4e832627b4f6", 4 union ' \
           'select 1, "aca71b36752920f5812a4ecfe8807cd3", 10000 union ' \
           'select 1, "f25f126df9a9caa86c2ed90d3c0ee6ff", 100000 union ' \
           'select 1, "22bf05240aa95737fa343072fc612bf2", 1000000 ' 
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    client.clean(database_name)


def test_uniq_table():
    """
    {
    "title": "",
    "describe": "string类型，作为unique表value列，支持，导入成功，数据正确",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column = [("k1", "VARCHAR(65533)"), ("v1", "STRING")]
    ret = client.create_table(table_name, column, keys_desc='UNIQUE KEY(k1)')
    assert ret, 'create table failed'
    insert_sql = 'INSERT INTO %s VALUES(%s)'
    values = ['1, repeat("test111111", 10000)', '2, repeat("test111111", 100000)']
    for v in values:
        client.execute(insert_sql % (table_name, v))
    # check
    sql1 = 'select k1, md5(v1), length(v1) from %s' % table_name
    sql2 = 'select 1, "f25f126df9a9caa86c2ed90d3c0ee6ff", 100000 ' \
           'union select 2, "22bf05240aa95737fa343072fc612bf2", 1000000'
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    client.clean(database_name)


def test_insert_load():
    """
    {
    "title": "",
    "describe": "string类型，insert导入，空串，null，65536长度，1M长度，导入成功，数据正确，10M失败",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column = [("k1", "VARCHAR(65533)"), ("v1", "STRING")]
    client.set_variables('enable_insert_strict', 'False')
    ret = client.create_table(table_name, column, keys_desc='UNIQUE KEY(k1)', set_null=True)
    assert ret, 'create table failed'
    insert_sql = 'INSERT INTO %s VALUES%s'
    values = ['("", "")', '(NULL, NULL)',
              '(1, repeat("test1111", 8192))', # 65536
              '(2, repeat("test1111", 131072))' # 1M
             ]
    client.execute(insert_sql % (table_name, ','.join(values)))
    sql1 = 'select k1, md5(v1), length(v1) from %s' % table_name
    sql2 = 'select "", "d41d8cd98f00b204e9800998ecf8427e", 0 union ' \
           'select "1", "1f44fb91f47cab16f711973af06294a0", 65536 union ' \
           'select "2", "3c514d3b89e26e2f983b7bd4cbb82055", 1048576 union select NULL, NULL, NULL'
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    client.clean(database_name)


def test_insert_load_strict():
    """
    {
    "title": "",
    "describe": "string类型，insert导入, strict，空串，null，65536长度，1M长度导入成功，数据正确",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column = [("k1", "VARCHAR(65533)"), ("v1", "STRING")]
    client.set_variables('enable_insert_strict', 'True')
    ret = client.create_table(table_name, column, keys_desc='UNIQUE KEY(k1)', set_null=False)
    assert ret, 'create table failed'
    insert_sql = 'INSERT INTO %s VALUES%s'
    values = ['("", "")', '(NULL, NULL)',               
              '(1, repeat("test1111", 8192))', # 65536
              '(2, repeat("test1111", 131072))'] # 1M
    msg = 'error totally whack'
    util.assert_return(False, msg, client.execute, insert_sql % (table_name, ','.join(values)))
    # string is too long
    sql = 'INSERT INTO %s VALUES(5, repeat("test1111", 131073))' % table_name
    msg = ' '
    util.assert_return(False, msg, client.execute, sql)
    values.remove(values[1])
    client.execute(insert_sql % (table_name, ','.join(values)))
    sql1 = 'select k1 from %s' % table_name
    sql2 = 'select "" union select "1" union select "2"' 
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    client.clean(database_name)


def test_stream_load():
    """
    {
    "title": "",
    "describe": "string类型，stream导入，3种表类型，导入成功，数据正确",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column = [("k1", "VARCHAR(65533)"), ("v1", "STRING")]
    ret = client.create_table(table_name, column, keys_desc='UNIQUE KEY(k1)', set_null=True)
    assert ret, 'create table failed'
    file = '%s/data/LOAD/string_file.data' % file_dir
    ret = client.stream_load(table_name, file, strict_mode=False)
    assert ret, 'stream load failed'
    # check
    sql1 = 'select k1, md5(v1) from %s' % table_name
    sql2 = 'select NULL, NULL union select "0", "d41d8cd98f00b204e9800998ecf8427e" union ' \
           'select "1", "de649f2e18451033ed8e583e06cb4c67"' 
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    client.clean(database_name)


def test_broker_load_strict():
    """
    {
    "title": "",
    "describe": "string类型，strict模式，broker导入，3种表类型，导入成功，数据正确",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column = [("k1", "VARCHAR(65533)"), ("v1", "STRING")]
    client.set_variables('enable_insert_strict', 'True')
    ret = client.create_table(table_name, column, keys_desc='UNIQUE KEY(k1)', set_null=True)
    assert ret, 'create table failed'
    file = palo_config.gen_remote_file_path('sys/load/string_file.data')
    load_info = palo_client.LoadDataInfo(file, table_name)
    ret = client.batch_load(util.get_label(), load_info, strict_mode='true', is_wait=True, broker=broker_info)
    assert ret, 'broker load failed'
    # check
    sql1 = 'select k1, md5(v1) from %s' % table_name
    sql2 = 'select NULL, NULL union select "0", "d41d8cd98f00b204e9800998ecf8427e" union ' \
           'select "1", "de649f2e18451033ed8e583e06cb4c67"'
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    client.clean(database_name)


def test_string_bloom_filter():
    """
    {
    "title": "",
    "describe": "string类型，作为bloom filter，建表时指定，建表导入后alter创建，支持",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, schema.baseall_string_no_agg_column_list,
                              bloom_filter_column_list=['k7', 'k2'])
    assert ret, 'create table failed'
    load_info = palo_client.LoadDataInfo(load_file.baseall_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), load_info, is_wait=True, broker=broker_info)
    assert ret, 'load data failed'
    table_schema = client.get_index_schema(table_name)
    for column in table_schema:
        if column[0] in ['k7', 'k2']:
            assert column[5].find("BLOOM_FILTER") >= 0, "%s should be bloom filter" % str(column)
    assert client.verify(load_file.baseall_local_file, table_name)
    client.clean(database_name)


def test_string_prefix_index():
    """
    {
    "title": "",
    "describe": "string类型，作为前缀索引，建表时string为首列，建表导入后alter更新列顺序，支持",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_list = [("k1", "tinyint"),
                   ("k2", "string"),
                   ("k3", "string"),
                   ("k4", "string"),
                   ("k5", "string"),
                   ("k6", "string"),
                   ("k10", "date"),
                   ("k11", "datetime"),
                   ("k7", "string"),
                   ("k8", "double"),
                   ("k9", "float")]
    ret = client.create_table(table_name, column_list, keys_desc='DUPLICATE KEY(k1)',
                              bloom_filter_column_list=['k6', 'k7'])
    assert ret, 'create table failed'
    load_info = palo_client.LoadDataInfo(load_file.baseall_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), load_info, is_wait=True, broker=broker_info)
    assert ret, 'load data failed'
    schema = client.get_index_schema(table_name)
    for column in schema:
        if column[0] in ['k6', 'k7']:
            assert column[5].find("BLOOM_FILTER") >= 0, "%s should be bloom filter" % str(column)
    assert client.verify(load_file.baseall_local_file, table_name)
    client.clean(database_name)


def test_load_file_type():
    """
    {
    "title": "",
    "describe": "导入parquet，orc，json类型文件，映射到string类型",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_list = [
                  ('tinyint_key', 'TINYINT'), 
                  ('smallint_key', 'SMALLINT'), 
                  ('int_key', 'INT'), 
                  ('bigint_key', 'BIGINT'), 
                  ('largeint_key', 'LARGEINT'), 
                  ('char_key', 'CHAR(255)'), 
                  ('varchar_key', 'VARCHAR(65533)'), 
                  ('decimal_key', 'DECIMAL(27, 9)'), 
                  ('date_key', 'DATE'), 
                  ('datetime_key', 'DATETIME'), 
                  ('tinyint_value_max', 'TINYINT', 'MAX'), 
                  ('smallint_value_min', 'SMALLINT', 'MIN'), 
                  ('int_value_sum', 'INT', 'SUM'), 
                  ('bigint_value_sum', 'BIGINT', 'SUM'), 
                  ('largeint_value_sum', 'LARGEINT', 'SUM'), 
                  ('largeint_value_replace', 'LARGEINT', 'replace'), 
                  ('char_value_replace', 'STRING', 'REPLACE'), 
                  ('varchar_value_replace', 'STRING', 'REPLACE'), 
                  ('decimal_value_replace', 'DECIMAL(27, 9)', 'REPLACE'), 
                  ('date_value_replace', 'DATE', 'REPLACE'), 
                  ('datetime_value_replace', 'DATETIME', 'REPLACE'), 
                  ('float_value_sum', 'FLOAT', 'SUM'), 
                  ('double_value_sum', 'DOUBLE', 'SUM')]
    client.create_table(table_name, column_list, set_null=True)
    assert client.show_tables(table_name)
    # load parquet file
    data_desc_list_1 = palo_client.LoadDataInfo(DATA.data_1_parquet, table_name, format_as='parquet')
    assert client.batch_load(util.get_label(), data_desc_list_1, is_wait=True, broker=broker_info)
    assert client.verify(DATA.verify_6, table_name)
    assert client.truncate(table_name), 'truncate table'
    # load orc file
    data_desc_list_1 = palo_client.LoadDataInfo(DATA.data_1_orc, table_name, format_as='orc')
    assert client.batch_load(util.get_label(), data_desc_list_1, is_wait=True, broker=broker_info)
    assert client.verify(DATA.verify_6, table_name)
    client.clean(database_name)


def test_select_string_type():
    """
    {
    "title": "",
    "describe": "string类型查询，返回各种长度的string",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column = [("k1", "VARCHAR(65533)"), ("v1", "STRING")]
    client.set_variables('enable_insert_strict', 'False')
    ret = client.create_table(table_name, column, keys_desc='UNIQUE KEY(k1)', set_null=True)
    assert ret, 'create table failed'
    insert_sql = 'INSERT INTO %s VALUES%s'
    values = ['("", "")', '(NULL, NULL)',
              '(1, repeat("test1111", 8192))',  # 65536
              '(2, repeat("test1111", 131072))' # 1M
             ]
    client.execute(insert_sql % (table_name, ','.join(values)))
    sql1 = 'select v1 from %s where k1=""' % table_name
    sql2 = 'select ""'
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select v1 from %s where k1 is NULL' % table_name
    sql2 = 'select NULL'
    common.check2(client, sql1, sql2=sql2)
    select_verify = {'1':'1f44fb91f47cab16f711973af06294a0',
                     '2':'3c514d3b89e26e2f983b7bd4cbb82055',
                    }
    for k, v in select_verify.items():
        sql1 =  'select v1 from %s where k1="%s"' % (table_name, k)
        ret1 = client.execute(sql1)
        ret1_md5 = util.get_string_md5(ret1[0][0])
        assert ret1_md5 == v, '%s result check error' % sql1
    client.clean(database_name)


def test_select_string_function():
    """
    {
    "title": "",
    "describe": "string类型查询，字符串函数",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column = [("k1", "VARCHAR"), ("v1", "STRING")]
    client.set_variables('enable_insert_strict', 'False')
    ret = client.create_table(table_name, column, keys_desc='UNIQUE KEY(k1)', set_null=False)
    assert ret, 'create table failed'
    insert_sql = 'INSERT INTO %s VALUES%s'
    values = ['("", "")',
              '(1, repeat("test111", 8192))', # 65536
              '(2, repeat("test111", 131072))' # 1M
             ]
    client.execute(insert_sql % (table_name, ','.join(values)))
    # string函数
    sql1 = 'select bit_length(v1) from %s' % table_name
    sql2 = 'select 458752 union select 0 union select 7340032'
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    client.clean(database_name)


def test_select_string_operator():
    """
    {
    "title": "",
    "describe": "string类型查询，字符串操作符",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column = [("k1", "VARCHAR(65533)"), ("v1", "STRING")]
    client.set_variables('enable_insert_strict', 'False')
    ret = client.create_table(table_name, column, keys_desc='UNIQUE KEY(k1)', set_null=False)
    assert ret, 'create table failed'
    insert_sql = 'INSERT INTO %s VALUES%s'
    values = ['("", "")',
              '(1, repeat("test111", 8192))', # 65536
              '(2, repeat("test111", 131072))', # 1M
              '(4, "test111")'
             ]
    client.execute(insert_sql % (table_name, ','.join(values)))
    # string比较操作符
    sql1 = 'select k1 from %s where k1 > "3"' % table_name
    sql2 = 'select "4"'
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    sql1 = 'select k1 from %s where v1 = repeat("test111", 8192)' % table_name
    sql2 = 'select "1"'
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    sql1 = 'select k1 from %s where v1 != repeat("test111", 8192)' % table_name
    sql2 = 'select "" union select "2" union select "4"' 
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    sql1 = 'select k1 from %s where v1 >= repeat("test111", 131072)' % table_name
    sql2 = 'select "2"'
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    sql1 = 'select k1 from %s where v1 <= repeat("test111", 8192)' % table_name
    sql2 = 'select "" union select "1" union select "4"'
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    sql1 = 'select k1 from %s where v1 between "test" and repeat("test111", 8193)' % table_name
    sql2 = 'select "1" union select "4"'
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    client.clean(database_name)


def test_select_string_sql_mode():
    """
    {
    "title": "",
    "describe": "string类型查询，sql_mode对string类型生效",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column = [("k1", "VARCHAR(65533)"), ("v1", "STRING")]
    client.set_variables('sql_mode', 'PIPES_AS_CONCAT')
    ret = client.create_table(table_name, column, keys_desc='UNIQUE KEY(k1)', set_null=False)
    assert ret, 'create table failed'
    insert_sql = 'INSERT INTO %s VALUES%s'
    values = ['("", "")',
              '(1, repeat("test111", 8192))', # 65536
              '(2, repeat("test111", 131072))' # 1M
             ]
    client.execute(insert_sql % (table_name, ','.join(values)))
    # string函数
    sql1 = 'select k1 || k1 from %s where v1 like "%%11test11%%"' % table_name
    sql2 = 'select "11" union select "22"'
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    sql1 = 'select length(v1 || v1) from %s ' % table_name
    sql2 = 'select 0 union select 114688 union select 1835008'
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    client.clean(database_name)


def test_select_string_match():
    """
    {
    "title": "",
    "describe": "string类型查询，字符串匹配",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column = [("k1", "VARCHAR(65533)"), ("v1", "STRING")]
    client.set_variables('enable_insert_strict', 'False')
    ret = client.create_table(table_name, column, keys_desc='UNIQUE KEY(k1)', set_null=False)
    assert ret, 'create table failed'
    insert_sql = 'INSERT INTO %s VALUES%s'
    values = ['("", "")',
              '(1, repeat("test111", 8192))',
              '(2, repeat("test111", 131072))'
             ]
    client.execute(insert_sql % (table_name, ','.join(values)))
    # string函数
    sql1 = 'select k1 from %s where v1 like "%%test11%%"' % table_name
    sql2 = 'select "1" union select "2"'
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    sql = 'select k1 from %s where v1 like "%%111111%%"' % table_name
    ret = client.execute(sql)
    assert ret == ()
    sql1 = 'select k1 from %s where k1 in ("", "45", "44")' % table_name
    sql2 = 'select ""'
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    sql1 = 'select k1 from %s where k1 not in ("", "45", "44")' % table_name
    sql2 = 'select "1" union select "2"'
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    sql1 = 'select k1 from %s where v1 in (repeat("test111", 8192), "test", repeat("test111", 131072))' % table_name
    sql2 = 'select "1" union select 2'
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    client.clean(database_name)


def test_select_distinct():
    """
    {
    "title": "",
    "describe": "string类型查询，distinct",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column = [("k1", "VARCHAR(65533)"), ("v1", "STRING")]
    client.set_variables('enable_insert_strict', 'False')
    ret = client.create_table(table_name, column, keys_desc='UNIQUE KEY(k1)', set_null=False)
    assert ret, 'create table failed'
    insert_sql = 'INSERT INTO %s VALUES%s'
    values = ['("", "")',
              '(1, repeat("test111", 8192))', # 65536
              '(2, repeat("test111", 131072))', # 1M
              '(4, repeat("test111", 8192))',
              '(5, repeat("test111", 131072))',
              '(6, repeat("test111", 131072))'
             ]
    client.execute(insert_sql % (table_name, ','.join(values)))
    # select distinct
    sql1 = 'select count(v1) from %s' % table_name
    sql2 = 'select 6'
    common.check2(client, sql1=sql1, sql2=sql2)
    sql1 = 'select count(distinct v1) from %s' % table_name
    sql2 = 'select 3'
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_alter_char_to_string():
    """
    {
    "title": "",
    "describe": "alter将char/varchar类型修改为string类型，支持，结果正确",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    # 建表 & 导入
    ret = client.create_table(table_name, schema.baseall_column_no_agg_list)
    assert ret, 'create database failed'
    load_info = palo_client.LoadDataInfo(load_file.baseall_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), load_info, is_wait=True, broker=broker_info)
    assert ret, 'load data failed'
    assert client.verify(load_file.baseall_local_file, table_name), 'data check error'
    # modify & check
    ret = client.schema_change_modify_column(table_name, 'k6', 'string', is_wait_job=True)
    assert ret, 'modify char to string failed'
    ret = client.schema_change_modify_column(table_name, 'k7', 'string', is_wait_job=True)
    assert ret, 'modify varchar to string failed'
    ret = client.desc_table(table_name)
    assert util.get_attr_condition_value(ret, palo_job.DescInfo.Field, 'k6',
                                         palo_job.DescInfo.Type) == 'TEXT', 'expect k6 text'
    assert util.get_attr_condition_value(ret, palo_job.DescInfo.Field, 'k7',
                                         palo_job.DescInfo.Type) == 'TEXT', 'expect k7 text'
    # after modify load and check
    client.truncate(table_name)
    ret = client.batch_load(util.get_label(), load_info, is_wait=True, broker=broker_info)
    assert ret, 'load data failed'
    assert client.verify(load_file.baseall_local_file, table_name), 'data check error'
    client.clean(database_name)


def test_alter_date_to_string():
    """
    {
    "title": "",
    "describe": "alter将date/datetime类型修改为string类型，支持，结果正确",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    # 建表 & 导入
    ret = client.create_table(table_name, schema.baseall_column_no_agg_list)
    assert ret, 'create database failed'
    load_info = palo_client.LoadDataInfo(load_file.baseall_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), load_info, is_wait=True, broker=broker_info)
    assert ret, 'load data failed'
    assert client.verify(load_file.baseall_local_file, table_name), 'data check error'
    # modify & check
    msg = 'Can not change DATE to STRING'
    util.assert_return(False, msg, client.schema_change_modify_column, table_name, 'k10', 'string', is_wait_job=True)
    msg = 'Can not change DATETIME to STRING'
    util.assert_return(False, msg, client.schema_change_modify_column, table_name, 'k11', 'string', is_wait_job=True)
    client.clean(database_name)


def test_alter_int_to_string():
    """
    {
    "title": "",
    "describe": "alter将smallint/int/bigint类型修改为string类型，支持，结果正确",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    # 建表 & 导入
    ret = client.create_table(table_name, schema.baseall_column_no_agg_list, keys_desc="duplicate key(k1)",
                              distribution_info=palo_client.DistributionInfo('hash(k5)', 3))
    assert ret, 'create database failed'
    load_info = palo_client.LoadDataInfo(load_file.baseall_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), load_info, is_wait=True, broker=broker_info)
    assert ret, 'load data failed'
    assert client.verify(load_file.baseall_local_file, table_name), 'data check error'
    # modify & check
    ret = client.schema_change_modify_column(table_name, 'k2', 'string', is_wait_job=True)
    assert ret, 'modify smallint to string failed'
    ret = client.schema_change_modify_column(table_name, 'k3', 'string', is_wait_job=True)
    assert ret, 'modify int to string failed'
    ret = client.schema_change_modify_column(table_name, 'k4', 'string', is_wait_job=True)
    assert ret, 'modify bigint to string failed'
    ret = client.desc_table(table_name)
    assert util.get_attr_condition_value(ret, palo_job.DescInfo.Field, 'k2',
                                         palo_job.DescInfo.Type) == 'TEXT', 'expect k2 text'
    assert util.get_attr_condition_value(ret, palo_job.DescInfo.Field, 'k3',
                                         palo_job.DescInfo.Type) == 'TEXT', 'expect k3 text'
    assert util.get_attr_condition_value(ret, palo_job.DescInfo.Field, 'k4',
                                         palo_job.DescInfo.Type) == 'TEXT', 'expect k4 text'
    # after modify load and check
    client.truncate(table_name)
    ret = client.batch_load(util.get_label(), load_info, is_wait=True, broker=broker_info)
    assert ret, 'load data failed'
    assert client.verify(load_file.baseall_local_file, table_name), 'data check error'
    client.clean(database_name)


def test_alter_double_to_string():
    """
    {
    "title": "",
    "describe": "alter将float/double/decimal类型修改为string类型，支持，结果正确",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    # 建表 & 导入
    column_list = util.convert_agg_column_to_no_agg_column(schema.baseall_column_no_agg_list)
    ret = client.create_table(table_name, column_list)
    assert ret, 'create database failed'
    load_info = palo_client.LoadDataInfo(load_file.baseall_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), load_info, is_wait=True, broker=broker_info)
    assert ret, 'load data failed'
    assert client.verify(load_file.baseall_local_file, table_name), 'data check error'
    # modify & check
    ret = client.schema_change_modify_column(table_name, 'k5', 'string', is_wait_job=True)
    assert ret, 'modify decimal to string failed'
    ret = client.schema_change_modify_column(table_name, 'k8', 'string', is_wait_job=True)
    assert ret, 'modify float to string failed'
    ret = client.schema_change_modify_column(table_name, 'k9', 'string', is_wait_job=True)
    assert ret, 'modify double to string failed'
    ret = client.desc_table(table_name)
    assert util.get_attr_condition_value(ret, palo_job.DescInfo.Field, 'k5',
                                         palo_job.DescInfo.Type) == 'TEXT', 'expect k1 text'
    assert util.get_attr_condition_value(ret, palo_job.DescInfo.Field, 'k8',
                                         palo_job.DescInfo.Type) == 'TEXT', 'expect k8 text'
    assert util.get_attr_condition_value(ret, palo_job.DescInfo.Field, 'k9',
                                         palo_job.DescInfo.Type) == 'TEXT', 'expect k9 text'
    # after modify load and check
    client.truncate(table_name)
    ret = client.batch_load(util.get_label(), load_info, is_wait=True, broker=broker_info)
    assert ret, 'load data failed'
    assert client.verify(load_file.baseall_local_file, table_name), 'data check error'
    client.clean(database_name)


def test_alter_string_to_types():
    """
    {
    "title": "",
    "describe": "alter将string类型修改为其他类型，不支持",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column = [("k1", "VARCHAR(65533)"), ("v1", "STRING")]
    ret = client.create_table(table_name, column, keys_desc='DUPLICATE KEY(k1)', set_null=False)
    assert ret, 'create table failed'
    # change sting to char
    msg = 'Can not change STRING to CHAR'
    util.assert_return(False, msg, client.schema_change_modify_column, table_name, 'v1', 'char(5)')
    # change string to varchar
    msg = 'Can not change STRING to VARCHAR'
    util.assert_return(False, msg, client.schema_change_modify_column, table_name, 'v1', 'varchar(55)')
    # change string to date
    msg = 'Can not change STRING to DATE'
    util.assert_return(False, msg, client.schema_change_modify_column, table_name, 'v1', 'date')
    # change string to datetime
    msg = 'Can not change STRING to DATETIME'
    util.assert_return(False, msg, client.schema_change_modify_column, table_name, 'v1', 'datetime')
    # change string to tinying
    msg = 'Can not change STRING to TINYINT'
    util.assert_return(False, msg, client.schema_change_modify_column, table_name, 'v1', 'tinyint')
    # change string to smallint
    msg = 'Can not change STRING to SMALLINT'
    util.assert_return(False, msg, client.schema_change_modify_column, table_name, 'v1', 'smallint')
    # change string to int
    msg = 'Can not change STRING to INT'
    util.assert_return(False, msg, client.schema_change_modify_column, table_name, 'v1', 'int')
    # change string to bigint
    msg = 'Can not change STRING to BIGINT'
    util.assert_return(False, msg, client.schema_change_modify_column, table_name, 'v1', 'bigint')
    # change string to largeint
    msg = 'Can not change STRING to LARGEINT'
    util.assert_return(False, msg, client.schema_change_modify_column, table_name, 'v1', 'largeint')
    # change string to decimal
    msg = 'Can not change STRING to DECIMALV2'
    util.assert_return(False, msg, client.schema_change_modify_column, table_name, 'v1', 'decimal(27, 9)')
    # change string to double
    msg = 'Can not change STRING to DOUBLE'
    util.assert_return(False, msg, client.schema_change_modify_column, table_name, 'v1', 'double')
    # change string to float
    msg = 'Can not change STRING to FLOAT'
    util.assert_return(False, msg, client.schema_change_modify_column, table_name, 'v1', 'float')
    # change string to hll
    msg = 'Can not assign aggregation method on column in Duplicate data model table'
    util.assert_return(False, msg, client.schema_change_modify_column, table_name, 'v1', 'hll hll_union')
    # change string to bitmap
    msg = 'Can not assign aggregation method on column in Duplicate data model table'
    util.assert_return(False, msg, client.schema_change_modify_column, table_name, 'v1', 'bitmap bitmap_union')
    client.clean(database_name)


def test_add_string_column():
    """
    {
    "title": "",
    "describe": "alter，增加string key列/value列",
    "tag": "function,p1"
    }0
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    # 建表 & 导入
    ret = client.create_table(table_name, schema.baseall_string_column_list)
    assert ret, 'create database failed'
    load_info = palo_client.LoadDataInfo(load_file.baseall_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), load_info, is_wait=True, broker=broker_info)
    assert ret, 'load data failed'
    assert client.verify(load_file.baseall_local_file, table_name), 'data check error'
    sql = 'select k1, k2, k3, k4, k5, k6, k10, k11, k7, k8, "", k9 from %s' % table_name
    ret1 = client.execute(sql)
    # add value column
    ret = client.schema_change_add_column(table_name, [('k12', 'string', 'replace', "")], 
                                          after_column_name='k8', is_wait_job=True)
    assert ret, 'add column failed'
    # add key column failed
    msg = 'String Type should not be used in key column[k1_1]'
    util.assert_return(False, msg, client.schema_change_add_column, table_name,
                       [('k1_1', 'string', 'key', "")], after_column_name='k1')
    ret = client.desc_table(table_name)
    assert util.get_attr_condition_value(ret, palo_job.DescInfo.Field, 'k12'), 'can not get k12'
    ret2 = client.select_all(table_name)
    util.check(ret2, ret1, True)
    client.clean(database_name)


def test_drop_string_column():
    """
    {
    "title": "",
    "describe": "alter，删除string key列/value列，各种表类型",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    # 建表 & 导入
    column_list = [("k1", "tinyint"),
                   ("k2", "smallint"),
                   ("k3", "int"),
                   ("k4", "string"),
                   ("k5", "string"),
                   ("k6", "string"),
                   ("k10", "date"),
                   ("k11", "datetime"),
                   ("k7", "string"),
                   ("k8", "double"),
                   ("k9", "float")]
    ret = client.create_table(table_name, column_list, keys_desc="DUPLICATE KEY(k1, k2, k3)")
    assert ret, 'create database failed'
    load_info = palo_client.LoadDataInfo(load_file.baseall_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), load_info, is_wait=True, broker=broker_info)
    assert ret, 'load data failed'
    assert client.verify(load_file.baseall_local_file, table_name), 'data check error'
    sql = 'select k1, k2, k3, k5, k6, k10, k11, k8, k9 from %s' % table_name
    ret1 = client.execute(sql)
    # drop string column
    ret = client.schema_change_drop_column(table_name, ['k4', 'k7'], is_wait_job=True)
    assert ret, 'add column failed'
    ret = client.desc_table(table_name)
    assert util.get_attr_condition_value(ret, palo_job.DescInfo.Field, 'k4') is None, 'get k4'
    assert util.get_attr_condition_value(ret, palo_job.DescInfo.Field, 'k7') is None, 'get k7'
    ret2 = client.select_all(table_name)
    util.check(ret2, ret1, True)
    client.clean(database_name)


def test_rollup_string():
    """
    {
    "title": "",
    "describe": "alter，创建string value列的rollup，各种表类型",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    # 建表 & 导入
    baseall_string_column_list = [("k1", "tinyint"),
                                  ("k2", "smallint"),
                                  ("k3", "int"),
                                  ("k4", "bigint"),
                                  ("k5", "decimal(9, 3)"),
                                  ("k6", "char(5)"),
                                  ("k10", "date"),
                                  ("k11", "datetime"),
                                  ("k7", "string", "max"),
                                  ("k8", "double", "max"),
                                  ("k9", "float", "sum")]
    ret = client.create_table(table_name, baseall_string_column_list)
    assert ret, 'create database failed'
    load_info = palo_client.LoadDataInfo(load_file.baseall_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), load_info, is_wait=True, broker=broker_info)
    assert ret, 'load data failed'
    assert client.verify(load_file.baseall_local_file, table_name), 'data check error'
    # create rollup
    sql = 'select k2, k6, max(k7) from %s group by k2, k6' % table_name
    ret1 = client.execute(sql)
    ret = client.create_rollup_table(table_name, index_name, ['k2', 'k6', 'k7'], is_wait=True)
    assert ret, 'create rollup failed'
    ret2 = client.execute(sql)
    util.check(ret2, ret1, True)
    times = 100
    while times > 0:
        rollup = common.get_explain_rollup(client, sql)
        if index_name in rollup:
            break
        time.sleep(3) 
        times -= 1
    assert index_name in common.get_explain_rollup(client, sql), 'expect rollup: %s' % index_name
    client.clean(database_name)


def test_mv_string():
    """
    {
    "title": "",
    "describe": "alter，创建string value列的物化视图，各种表类型",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    # 建表 & 导入
    baseall_string_column_list = [("k1", "tinyint"),
                                  ("k2", "smallint"),
                                  ("k3", "int"),
                                  ("k4", "bigint"),
                                  ("k5", "decimal(9, 3)"),
                                  ("k6", "string"),
                                  ("k10", "date"),
                                  ("k11", "datetime"),
                                  ("k7", "string"),
                                  ("k8", "double"),
                                  ("k9", "float")]
    ret = client.create_table(table_name, baseall_string_column_list, keys_desc='DUPLICATE KEY(k1, k2)')
    assert ret, 'create database failed'
    load_info = palo_client.LoadDataInfo(load_file.baseall_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), load_info, is_wait=True, broker=broker_info)
    assert ret, 'load data failed'
    assert client.verify(load_file.baseall_local_file, table_name), 'data check error'
    # create rollup
    sql = 'select k1, k2, max(k7), count(k6) from %s group by k1, k2' % table_name
    ret1 = client.execute(sql)
    ret = client.create_materialized_view(table_name, index_name, sql, is_wait=True)
    assert ret, 'create mv failed'
    ret2 = client.execute(sql)
    util.check(ret2, ret1, True)
    times = 100
    while times > 0:
        rollup = common.get_explain_rollup(client, sql)
        if index_name in rollup:
            break
        time.sleep(3)
        times -= 1
    assert index_name in common.get_explain_rollup(client, sql), 'expect rollup: %s' % index_name
    client.clean(database_name)


def test_delete_string():
    """
    {
    "title": "",
    "describe": "删除数据，以string类型为where条件",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    # 建表 & 导入
    column = [("k1", "VARCHAR(65533)"), ("v1", "STRING")]
    client.set_variables('enable_insert_strict', 'False')
    ret = client.create_table(table_name, column, keys_desc='DUPLICATE KEY(k1)', set_null=False)
    assert ret, 'create table failed'
    insert_sql = 'INSERT INTO %s VALUES%s'
    values = ['("", "")', '(NULL, NULL)',
              '(1, repeat("test111", 8192))', # 65536
              '(2, repeat("test111", 131072))', # 1M
             ]
    client.execute(insert_sql % (table_name, ','.join(values)))
    # check
    ret = client.delete(table_name, [('k1', '=', '1')])
    assert ret, 'delete failed'
    ret = client.delete(table_name, [('v1', '=', 'NULL')])
    assert ret, 'delete failed'
    ret = client.delete(table_name, [('v1', '=', 'test')])
    assert ret, 'delete failed'
    msg = 'Where clause only supports compound predicate, binary predicate, is_null predicate or in predicate'
    util.assert_return(False, msg, client.delete, table_name, [('v1', 'like', '%%111test111%%')])
    sql1 = 'select k1 from %s' % table_name
    sql2 = 'select "" union select "2" '
    client.clean(database_name)

