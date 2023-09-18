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
#   @file test_array_ddl.py
#   @date 2022-08-15 11:09:53
#   @brief This file is a test file for array type.
#
#############################################################################
"""
test_array_ddl.py
"""
import sys
import os
import time
import pytest
file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(file_dir)
file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(file_dir)

from lib import palo_config
from lib import palo_client
from lib import util
from lib import common
from lib import palo_job
from data import schema as SCHEMA
from data import load_file as FILE

config = palo_config.config
broker_info = palo_config.broker_info


def setup_module():
    """setup"""
    client = common.get_client()
    ret = client.show_variables('enable_vectorized_engine')
    if len(ret) == 1 and ret[0][1] == 'false':
        raise pytest.skip('skip if enable_vectorized_engine is false')

    ret = client.admin_show_config('enable_array_type')
    assert len(ret) == 1, 'get enable_array_type config error'
    value = palo_job.AdminShowConfig(ret[0]).get_value()
    if value != 'true':
        client.set_frontend_config('enable_array_type', 'true')


def teardown_module():
    """teardown"""
    pass


def test_array_basic():
    """
    {
    "title": "test_array_basic",
    "describe": "array类型，基础使用，建表成功，导入成功，查询成功",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, SCHEMA.array_table_list, keys_desc=SCHEMA.duplicate_key)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.test_array_table_local_file, column_separator='|')
    assert ret, 'stream load failed'
    assert client.verify(FILE.expe_array_table_file, table_name), 'check data failed'
    client.clean(database_name)


def test_array_key():
    """
    {
    "title": "",
    "describe": "array类型，不支持作为key列",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column = [("k2", "ARRAY<INT>"), ("k1", "INT")]
    msg = 'The olap table first column could not be'
    # with duplicate key
    util.assert_return(False, msg, client.create_table, table_name, column)
    column = [("k1", "INT"), ("k2", "ARRAY<INT>")]
    msg = 'Array can only be used in the non-key column of the duplicate table at present.'
    # with duplicate key
    util.assert_return(False, msg, client.create_table, table_name, column, keys_desc='DUPLICATE KEY(k1,k2)')
    # default dupllicate key
    util.assert_return(True, '', client.create_table, table_name, column)
    ret = client.desc_table(table_name)
    key = util.get_attr_condition_value(ret, palo_job.DescInfo.Field, 'k2', palo_job.DescInfo.Key)
    assert key == 'false', 'array column can not be key'
    client.clean(database_name)


def test_array_table_model():
    """
    {
    "title": "",
    "describe": "array类型，仅支持duplicate模型，不支持unique表和agg表，不支持agg中任一聚合方式",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column = [("k1", "INT"), ("k2", "ARRAY<INT>")]
    # unique table
    msg = "ARRAY column can't support aggregation REPLACE"
    util.assert_return(False, msg, client.create_table, table_name, column, keys_desc='UNIQUE KEY(k1)')
    # duplicate table
    util.assert_return(True, '', client.create_table, table_name, column, keys_desc='DUPLICATE KEY(k1)')
    # aggregate table
    agg_types = ['MAX', 'MIN', 'SUM', 'BITMAP_UNION', 'HLL_UNION']
    msg = 'is not compatible with primitive type array<int(11)>'
    for agg in agg_types:
        column = [("k1", "INT"), ("k2", "ARRAY<INT> %s" % agg)]
        util.assert_return(False, msg, client.create_table, table_name, column)
    agg_types = ['REPLACE', 'REPLACE_IF_NOT_NULL']
    msg = "ARRAY column can't support aggregation"
    for agg in agg_types:
        column = [("k1", "INT"), ("k2", "ARRAY<INT> %s" % agg)]
        util.assert_return(False, msg, client.create_table, table_name, column)
    client.clean(database_name)


def test_array_tb_partition():
    """
    {
    "title": "",
    "describe": "array类型，分区表，导入、查询成功",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, SCHEMA.array_table_list,
                              partition_info=SCHEMA.baseall_tinyint_partition_info,
                              keys_desc=SCHEMA.duplicate_key)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.test_array_table_local_file, column_separator='|')
    assert ret, 'stream load failed'
    assert client.verify(FILE.expe_array_table_file, table_name), 'check data failed'
    client.clean(database_name)


def test_array_default():
    """
    {
    "title": "",
    "describe": "array类型，设置default支持，null的default值，not null的default值",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    # array default not null
    column = [("k1", "INT", "", "88"), ("k2", "ARRAY<INT>", "", "[1,2,3]")]
    msg = 'Array type column default value only support null'
    util.assert_return(False, msg, client.create_table, table_name, column,
                       keys_desc="DUPLICATE KEY(k1)", set_null=True)
    # not null array default null
    column = [("k1", "INT", "", "99"), ("k2", "ARRAY<INT>", "", None)]
    msg = 'Can not set null default value to non nullable column: k2'
    util.assert_return(False, msg, client.create_table, table_name, column,
                       keys_desc="DUPLICATE KEY(k1)", set_null=False)
    # null array defaylt null
    ret = client.create_table(table_name, column, keys_desc="DUPLICATE KEY(k1)",
                              set_null=True)
    assert ret, 'create table failed'
    sql = 'insert into %s values(1, [1,2,3])' % table_name
    client.execute(sql)
    sql = 'insert into %s(k1) values(2), (3), (4)' % table_name
    client.execute(sql)
    ret1 = client.select_all(table_name)
    ret2 = ((1, '[1, 2, 3]'), (2, None), (3, None), (4, None))
    util.check(ret1, ret2, True)
    client.clean(database_name)


def test_array_create_like():
    """
    {
    "title": "",
    "describe": "含有array类型的表，使用create table like创建一个空表，导入查询成功",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, SCHEMA.array_table_list,
                              partition_info=SCHEMA.baseall_tinyint_partition_info,
                              keys_desc=SCHEMA.duplicate_key)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.test_array_table_local_file, column_separator='|')
    assert ret, 'stream load failed'
    new_tb = 'new_%s' % table_name
    ret = client.create_table_like(new_tb, table_name)
    assert ret, 'create table failed'
    ret = client.select_all(new_tb)
    assert ret == (), 'expect empty table'
    ret1 = client.desc_table(table_name)
    ret2 = client.desc_table(new_tb)
    util.check(ret1, ret2)

    ret = client.stream_load(new_tb, FILE.test_array_table_local_file, column_separator='|')
    assert ret, 'stream load failed'
    time.sleep(10)
    assert client.verify(FILE.expe_array_table_file, new_tb), 'check data failed'
    client.clean(database_name)


def test_array_ctas():
    """
    {
    "title": "",
    "describe": "含有array类型的查询结果，创建表，建表成功，数据导入成功",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    sql1 = 'select 1, 2, 3, array_sort(collect_list(k6)), array_sort(collect_list(k7)), ' \
           'array_sort(collect_list(k5)) from test_query_qa.test'
    sql = 'create table %s as select 1 k1, 2 k2, 3 k3, collect_list(k6) a1, collect_list(k7) a2, ' \
          'collect_list(k5) a3 from test_query_qa.test' % table_name
    ret = client.execute(sql)
    sql2 = 'select k1, k2, k3, array_sort(a1), array_sort(a2), array_sort(a3) from %s.%s' % (database_name, table_name)
    common.check2(client, sql1, sql2=sql2, forced=True)
    ret = client.desc_table(table_name)
    assert "ARRAY<CHAR(5)>" == util.get_attr_condition_value(ret, palo_job.DescInfo.Field,
                                                             'a1', palo_job.DescInfo.Type), 'column type error'
    assert "ARRAY<VARCHAR(20)>" == util.get_attr_condition_value(ret, palo_job.DescInfo.Field, 
                                                                 'a2', palo_job.DescInfo.Type), 'column type error'
    assert "ARRAY<DECIMALV3(9, 3)>" == util.get_attr_condition_value(ret, palo_job.DescInfo.Field,
                                                                   'a3', palo_job.DescInfo.Type), 'column type error'
    
    client.clean(database_name)


def test_array_bucket_column():
    """
    {
    "title": "",
    "describe": "使用array作为bucket列，建表失败",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    distribute = palo_client.DistributionInfo('hash(a2)', 1)
    msg = 'Array Type should not be used in distribution column[a2]'
    util.assert_return(False, msg, client.create_table, table_name, SCHEMA.array_table_list,
                       partition_info=SCHEMA.baseall_tinyint_partition_info,
                       keys_desc=SCHEMA.duplicate_key, distribution_info=distribute)
    client.clean(database_name)


def test_array_nest():
    """
    {
    "title": "",
    "describe": "array嵌套，仅验证建表，导入查询",
    "tag": "function,p2"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column = [("k1", "INT"), ("k2", "ARRAY<ARRAY<INT>>")]
    ret = client.create_table(table_name, column, keys_desc='DUPLICATE KEY(k1)')
    assert ret, 'create table failed'
    sql = 'insert into %s values(1, [[1, 2, 3], [-1, -2, -3]])' % table_name
    client.execute(sql)
    sql = 'insert into %s values(2, [[3, 4, 5], [-1, 2, 3]])' % table_name
    client.execute(sql)
    ret1 = client.select_all(table_name)
    ret2 = ((1, '[[1, 2, 3], [-1, -2, -3]]'), (2, '[[3, 4, 5], [-1, 2, 3]]'))
    util.check(ret1, ret2, True)
    ret = client.desc_table(table_name)
    assert "ARRAY<ARRAY<INT(11)>>" == \
           util.get_attr_condition_value(ret, palo_job.DescInfo.Field, 'k2', palo_job.DescInfo.Type) 
    client.clean(database_name)


def test_array_external_table():
    """
    {
    "title": "",
    "describe": "创建含有array类型的外部表（hdfs csv），创建查询成功",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    property = {"broker_name": config.broker_name,
                "path": FILE.test_array_table_remote_file,
                "column_separator": "|",
                "format": "csv"}
    msg = 'Array can only be used in the non-key column of the duplicate table at present'
    util.assert_return(False, msg, client.create_external_table, table_name, SCHEMA.array_table_list, 
                       engine='broker', property=property, broker_property=config.broker_property)
    # ret = client.create_external_table(table_name, SCHEMA.array_table_list, engine='broker',
    #                                    property=property, broker_property=config.broker_property)
    # assert ret, 'create array external table failed'
    # assert client.verify(FILE.expe_array_table_file, table_name), 'check data failed'
    client.clean(database_name)


if __name__ == '__main__':
    setup_module()
