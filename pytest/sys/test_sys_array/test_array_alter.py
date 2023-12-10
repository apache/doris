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
#   @file test_array_alter.py
#   @date 2022-08-15 11:09:53
#   @brief This file is a test file for array type.
#
#############################################################################
"""
test_array_alter.py
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
from lib import palo_job
from lib import util
from lib import common
from data import schema as SCHEMA
from data import load_file as FILE

config = palo_config.config
broker_info = palo_config.broker_info

check_db = 'array_check_db'
check_tb = 'array_tb'


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
    if len(client.show_databases(check_db)) == 0:
        init_check()
    assert client.verify(FILE.expe_array_table_file, check_tb, database_name=check_db)


def init_check():
    """init check db & tb"""
    client = common.create_workspace(check_db)
    ret = client.create_table(check_tb, SCHEMA.array_table_list, keys_desc=SCHEMA.duplicate_key)
    assert ret, 'create table failed'
    ret = client.stream_load(check_tb, FILE.test_array_table_local_file, column_separator='|')
    assert ret, 'load chech table failed'


def teardown_module():
    """teardown"""
    pass


def test_add_array_column():
    """
    {
    "title": "test_add_array_column",
    "describe": "array类型，增加array类型列",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, SCHEMA.array_table_list, keys_desc=SCHEMA.duplicate_key)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.test_array_table_local_file, column_separator='|')
    assert ret, 'stream load failed'
    column_name_list = util.get_attr(SCHEMA.array_table_list, 0)

    column_list = [('add_arr', 'array<int>', None, "[]")]
    ret = client.schema_change_add_column(table_name, column_list, is_wait_job=True, set_null=True)
    assert ret, 'add column failed'

    column_list = [('add_arr', 'array<int>', None, None)]
    msg = 'Can not set null default value to non nullable column: add_arr'
    ret = util.assert_return(False, msg, client.schema_change_add_column, table_name, column_list, set_null=False)

    column_list = [('add_arr', 'array<int>', 'key', None)]
    msg = 'Array can only be used in the non-key column of the duplicate table at present.'
    ret = util.assert_return(False, msg, client.schema_change_add_column, table_name, column_list, set_null=True)

    column_list = [('add_arr1', 'array<int>', None, None)]    
    ret = client.schema_change_add_column(table_name, column_list, is_wait_job=True, set_null=True)
    assert ret, 'add column failed'

    ret = client.desc_table(table_name)
    assert 'ARRAY<INT(11)>' == util.get_attr_condition_value(ret, palo_job.DescInfo.Field, 'add_arr1',
                                                             palo_job.DescInfo.Type)
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select *, [], null from %s.%s order by k1' % (check_db, check_tb)
    common.check2(client, sql1, sql2=sql2)

    column_name_list.append('add_arr=a4')
    ret = client.stream_load(table_name, FILE.test_array_table_local_file, column_separator='|',
                             column_name_list=column_name_list)
    assert ret, 'stream load failed'
    time.sleep(5)
    sql1 = 'select count(*) from %s.%s' % (database_name, table_name)
    sql2 = 'select count(*) * 2 from %s.%s' % (check_db, check_tb)
    common.check2(client, sql1, sql2=sql2)

    client.clean(database_name)


def test_drop_array_column():
    """
    {
    "title": "test_drop_array_column",
    "describe": "array类型，删除array类型列",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, SCHEMA.array_table_list, keys_desc=SCHEMA.duplicate_key)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.test_array_table_local_file, column_separator='|')
    assert ret, 'stream load failed'
    column_name_list = util.get_attr(SCHEMA.array_table_list, 0)
    ret = client.schema_change_drop_column(table_name, ['a1', 'a2', 'a7', 'a11', 'a13'], is_wait_job=True)
    assert ret, 'drop column failed'
    # check
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select k1, a3, a4, a5, a6, a8, a9, a10, a12, a14 from %s.%s order by k1' % (check_db, check_tb)
    common.check2(client, sql1, sql2=sql2)
    ret = client.desc_table(table_name)
    assert util.get_attr_condition_value(ret, palo_job.DescInfo.Field, 'a1', palo_job.DescInfo.Type) is None
    assert util.get_attr_condition_value(ret, palo_job.DescInfo.Field, 'a2', palo_job.DescInfo.Type) is None
    assert util.get_attr_condition_value(ret, palo_job.DescInfo.Field, 'a7', palo_job.DescInfo.Type) is None
    assert util.get_attr_condition_value(ret, palo_job.DescInfo.Field, 'a11', palo_job.DescInfo.Type) is None
    assert util.get_attr_condition_value(ret, palo_job.DescInfo.Field, 'a13', palo_job.DescInfo.Type) is None

    ret = client.stream_load(table_name, FILE.test_array_table_local_file, column_separator='|',
                             column_name_list=column_name_list)
    assert ret, 'stream load failed'
    time.sleep(5)
    sql1 = 'select count(*) from %s.%s' % (database_name, table_name)
    sql2 = 'select count(*) * 2 from %s.%s' % (check_db, check_tb)
    common.check2(client, sql1, sql2=sql2)

    client.clean(database_name)


def test_modify_array_column_not_support():
    """
    {
    "title": "test_modify_array_column_not_support",
    "describe": "不支持修改array类型",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, SCHEMA.array_boolean_list, keys_desc=SCHEMA.duplicate_key)
    assert ret, 'create table failed'
    msg = 'Can not change ARRAY to STRING'
    util.assert_return(False, msg, client.schema_change_modify_column, table_name, 'k2', 'string')
    msg = 'Can not change ARRAY to ARRAY'
    util.assert_return(False, msg, client.schema_change_modify_column, table_name, 'k2', 'array<string>')
    client.clean(database_name)


@pytest.mark.skip()
def test_modify_array_column_type_array_boolean():
    """
    {
    "title": "test_modify_array_column_type_array_boolean",
    "describe": "array类型，修改array<boolean>类型列为其他array子类型列",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_type_list = ['tinyint', 'smallint', 'int', 'bigint', 'largeint',
                        'deical(9, 3)', 'double', 'float', 'date', 
                        'datetime', 'char(10)', 'varchar(10)', 'string']

    ret = client.create_table(table_name, SCHEMA.array_boolean_list, keys_desc=SCHEMA.duplicate_key)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.test_array_boolean_local_file, max_filter_ratio=0.01)
    assert ret, 'stream load failed'
    c_type = 'array<%s>'
    k = 'k2'
    for sub_type in column_type_list:
        tb = table_name[-50:] + '_' + sub_type.split('(')[0]
        ret = client.create_table(tb, SCHEMA.array_boolean_list, keys_desc=SCHEMA.duplicate_key)
        assert ret, 'create table failed'
        ret = client.stream_load(tb, FILE.test_array_boolean_local_file, max_filter_ratio=0.01)
        assert ret, 'stream load failed'
        ret = client.schema_change_modify_column(tb, k, c_type % sub_type, is_wait_job=True)
        assert ret, 'modify table failed'
        sql1 = 'select * from %s.%s order by k1' % (database_name, tb)
        sql2 = 'select k1, cast(%s as %s) from %s.%s order by k1' % (k, sub_type, database_name, table_name)
        common.check2(client, sql1, sql2=sql2)
        ret = client.stream_load(tb, FILE.test_array_boolean_local_file, max_filter_ratio=0.01)
        assert ret, 'stream load failed'
        sql1 = 'select count(*) from %s.%s' % (database_name, tb)
        sql2 = 'select count(*) * 2 from %s.%s' % (database_name, table_name)
        print(client.execute(sql1))
        # common.check2(client, sql1, sql2=sql2)
    # client.clean(database_name)


@pytest.mark.skip()
def test_modify_array_column_type_array_tinyint():
    """
    {
    "title": "test_modify_array_column_type_array_tinyint",
    "describe": "array类型，修改array<tinyint>类型列为其他array子类型列",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_type_list = ['boolean', 'smallint', 'int', 'bigint', 'largeint',
                        'deical(9, 3)', 'double', 'float', 'date',
                        'datetime', 'char(10)', 'varchar(10)', 'string']

    ret = client.create_table(table_name, SCHEMA.array_tinyint_list, keys_desc=SCHEMA.duplicate_key)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.test_array_tinyint_local_file, max_filter_ratio=0.01)
    assert ret, 'stream load failed'
    c_type = 'array<%s>'
    k = 'k2'
    for sub_type in column_type_list:
        tb = table_name[-50:] + '_' + sub_type.split('(')[0]
        ret = client.create_table(tb, SCHEMA.array_tinyint_list, keys_desc=SCHEMA.duplicate_key)
        assert ret, 'create table failed'
        ret = client.stream_load(tb, FILE.test_array_tinyint_local_file, max_filter_ratio=0.01)
        assert ret, 'stream load failed'
        ret = client.schema_change_modify_column(tb, k, c_type % sub_type, is_wait_job=True)
        assert ret, 'modify table failed'
        sql1 = 'select * from %s.%s order by k1' % (database_name, tb)
        sql2 = 'select k1, cast(%s as %s) from %s.%s order by k1' % (k, sub_type, database_name, table_name)
        common.check2(client, sql1, sql2=sql2)
        ret = client.stream_load(tb, FILE.test_array_tinyint_local_file, max_filter_ratio=0.01)
        assert ret, 'after modify stream load failed'
        sql1 = 'select count(*) from %s.%s' % (database_name, tb)
        sql2 = 'select count(*) * 2 from %s.%s' % (database_name, table_name)
        print(client.execute(sql1))
        # common.check2(client, sql1, sql2=sql2)
    # client.clean(database_name)


@pytest.mark.skip()
def test_modify_array_column_type_array_smallint():
    """
    {
    "title": "test_modify_array_column_type_array_smallint",
    "describe": "array类型，修改array<smallint>类型列为array其他子类型列",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_type_list = ['boolean', 'tinyint', 'int', 'bigint', 'largeint',
                        'deical(9, 3)', 'double', 'float', 'date',
                        'datetime', 'char(10)', 'varchar(10)', 'string']
    column_type_list = ['int', 'bigint']

    ret = client.create_table(table_name, SCHEMA.array_smallint_list, keys_desc=SCHEMA.duplicate_key)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.test_array_smallint_local_file, max_filter_ratio=0.01)
    assert ret, 'stream load failed'
    c_type = 'array<%s>'
    k = 'k2'
    for sub_type in column_type_list:
        tb = table_name[-50:] + '_' + sub_type.split('(')[0]
        ret = client.create_table(tb, SCHEMA.array_smallint_list, keys_desc=SCHEMA.duplicate_key)
        assert ret, 'create table failed'
        ret = client.stream_load(tb, FILE.test_array_smallint_local_file, max_filter_ratio=0.01)
        assert ret, 'stream load failed'
        ret = client.schema_change_modify_column(tb, k, c_type % sub_type, is_wait_job=True)
        assert ret, 'modify table failed'
        sql1 = 'select * from %s.%s order by k1' % (database_name, tb)
        sql2 = 'select k1, cast(%s as %s) from %s.%s order by k1' % (k, sub_type, database_name, table_name)
        common.check2(client, sql1, sql2=sql2)
        ret = client.stream_load(tb, FILE.test_array_smallint_local_file, max_filter_ratio=0.01)
        assert ret, 'after modify stream load failed'
        sql1 = 'select count(*) from %s.%s' % (database_name, tb)
        sql2 = 'select count(*) * 2 from %s.%s' % (database_name, table_name)
        print(client.execute(sql1))
        # common.check2(client, sql1, sql2=sql2)
    # client.clean(database_name)


@pytest.mark.skip()
def test_modify_array_column_type_array_int():
    """
    {
    "title": "test_modify_array_column_type_array_int",
    "describe": "array类型，修改array<int>类型列为array其他子类型列",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_type_list = ['boolean', 'tinyint', 'smallint', 'bigint', 'largeint',
                        'deical(9, 3)', 'double', 'float', 'date',
                        'datetime', 'char(10)', 'varchar(10)', 'string']

    ret = client.create_table(table_name, SCHEMA.array_int_list, keys_desc=SCHEMA.duplicate_key)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.test_array_int_local_file, max_filter_ratio=0.01)
    assert ret, 'stream load failed'
    c_type = 'array<%s>'
    k = 'k2'
    for sub_type in column_type_list:
        tb = table_name[-50:] + '_' + sub_type.split('(')[0]
        ret = client.create_table(tb, SCHEMA.array_int_list, keys_desc=SCHEMA.duplicate_key)
        assert ret, 'create table failed'
        ret = client.stream_load(tb, FILE.test_array_int_local_file, max_filter_ratio=0.01)
        assert ret, 'stream load failed'
        ret = client.schema_change_modify_column(tb, k, c_type % sub_type, is_wait_job=True)
        assert ret, 'modify table failed'
        sql1 = 'select * from %s.%s order by k1' % (database_name, tb)
        sql2 = 'select k1, cast(%s as %s) from %s.%s order by k1' % (k, sub_type, database_name, table_name)
        common.check2(client, sql1, sql2=sql2)
        ret = client.stream_load(tb, FILE.test_array_int_local_file, max_filter_ratio=0.01)
        assert ret, 'after modify stream load failed'
        sql1 = 'select count(*) from %s.%s' % (database_name, tb)
        sql2 = 'select count(*) * 2 from %s.%s' % (database_name, table_name)
        print(client.execute(sql1))
        # common.check2(client, sql1, sql2=sql2)
    # client.clean(database_name)


@pytest.mark.skip()
def test_modify_array_column_type_array_bigint():
    """
    {
    "title": "test_modify_array_column_type_array_bigint",
    "describe": "array类型，修改array<bigint>类型列为其他array子类型列",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_type_list = ['boolean', 'tinyint', 'smallint', 'int', 'largeint',
                        'deical(9, 3)', 'double', 'float', 'date',
                        'datetime', 'char(10)', 'varchar(10)', 'string']

    ret = client.create_table(table_name, SCHEMA.array_bigint_list, keys_desc=SCHEMA.duplicate_key)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.test_array_bigint_local_file, max_filter_ratio=0.01)
    assert ret, 'stream load failed'
    c_type = 'array<%s>'
    k = 'k2'
    for sub_type in column_type_list:
        tb = table_name[-50:] + '_' + sub_type.split('(')[0]
        ret = client.create_table(tb, SCHEMA.array_bigint_list, keys_desc=SCHEMA.duplicate_key)
        assert ret, 'create table failed'
        ret = client.stream_load(tb, FILE.test_array_bigint_local_file, max_filter_ratio=0.01)
        assert ret, 'stream load failed'
        ret = client.schema_change_modify_column(tb, k, c_type % sub_type, is_wait_job=True)
        assert ret, 'modify table failed'
        sql1 = 'select * from %s.%s order by k1' % (database_name, tb)
        sql2 = 'select k1, cast(%s as %s) from %s.%s order by k1' % (k, sub_type, database_name, table_name)
        common.check2(client, sql1, sql2=sql2)
        ret = client.stream_load(tb, FILE.test_array_bigint_local_file, max_filter_ratio=0.01)
        assert ret, 'after modify stream load failed'
        sql1 = 'select count(*) from %s.%s' % (database_name, tb)
        sql2 = 'select count(*) * 2 from %s.%s' % (database_name, table_name)
        print(client.execute(sql1))
        # common.check2(client, sql1, sql2=sql2)
    # client.clean(database_name)


@pytest.mark.skip()
def test_modify_array_column_type_array_largeint():
    """
    {
    "title": "test_modify_array_column_type_array_largeint",
    "describe": "array类型，修改array<largeint>类型列为其他子类型列",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_type_list = ['boolean', 'tinyint', 'smallint', 'int', 'bigint',
                        'deical(9, 3)', 'double', 'float', 'date',
                        'datetime', 'char(10)', 'varchar(10)', 'string']

    ret = client.create_table(table_name, SCHEMA.array_largeint_list, keys_desc=SCHEMA.duplicate_key)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.test_array_largeint_local_file, max_filter_ratio=0.01)
    assert ret, 'stream load failed'
    c_type = 'array<%s>'
    k = 'k2'
    for sub_type in column_type_list:
        tb = table_name[-50:] + '_' + sub_type.split('(')[0]
        ret = client.create_table(tb, SCHEMA.array_largeint_list, keys_desc=SCHEMA.duplicate_key)
        assert ret, 'create table failed'
        ret = client.stream_load(tb, FILE.test_array_largeint_local_file, max_filter_ratio=0.01)
        assert ret, 'stream load failed'
        ret = client.schema_change_modify_column(tb, k, c_type % sub_type, is_wait_job=True)
        assert ret, 'modify table failed'
        sql1 = 'select * from %s.%s order by k1' % (database_name, tb)
        sql2 = 'select k1, cast(%s as %s) from %s.%s order by k1' % (k, sub_type, database_name, table_name)
        common.check2(client, sql1, sql2=sql2)
        ret = client.stream_load(tb, FILE.test_array_largeint_local_file, max_filter_ratio=0.01)
        assert ret, 'after modify stream load failed'
        sql1 = 'select count(*) from %s.%s' % (database_name, tb)
        sql2 = 'select count(*) * 2 from %s.%s' % (database_name, table_name)
        print(client.execute(sql1))
        # common.check2(client, sql1, sql2=sql2)
    # client.clean(database_name)


@pytest.mark.skip()
def test_modify_array_column_type_array_decimal():
    """
    {
    "title": "test_modify_array_column_type_array_decimal",
    "describe": "array类型，修改array<decimal>类型列为array其他子类型列",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_type_list = ['boolean', 'tinyint', 'smallint', 'int', 'bigint', 'largeint',
                        'double', 'float', 'date',
                        'datetime', 'char(10)', 'varchar(10)', 'string']

    ret = client.create_table(table_name, SCHEMA.array_decimal_list, keys_desc=SCHEMA.duplicate_key)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.test_array_decimal_local_file, max_filter_ratio=0.01)
    assert ret, 'stream load failed'
    c_type = 'array<%s>'
    k = 'k2'
    for sub_type in column_type_list:
        tb = table_name[-50:] + '_' + sub_type.split('(')[0]
        ret = client.create_table(tb, SCHEMA.array_decimal_list, keys_desc=SCHEMA.duplicate_key)
        assert ret, 'create table failed'
        ret = client.stream_load(tb, FILE.test_array_decimal_local_file, max_filter_ratio=0.01)
        assert ret, 'stream load failed'
        ret = client.schema_change_modify_column(tb, k, c_type % sub_type, is_wait_job=True)
        assert ret, 'modify table failed'
        sql1 = 'select * from %s.%s order by k1' % (database_name, tb)
        sql2 = 'select k1, cast(%s as %s) from %s.%s order by k1' % (k, sub_type, database_name, table_name)
        common.check2(client, sql1, sql2=sql2)
        ret = client.stream_load(tb, FILE.test_array_decimal_local_file, max_filter_ratio=0.01)
        assert ret, 'after modify stream load failed'
        sql1 = 'select count(*) from %s.%s' % (database_name, tb)
        sql2 = 'select count(*) * 2 from %s.%s' % (database_name, table_name)
        print(client.execute(sql1))
        # common.check2(client, sql1, sql2=sql2)
    # client.clean(database_name)


@pytest.mark.skip()
def test_modify_array_column_type_array_double():
    """
    {
    "title": "test_modify_array_column_type_array_double",
    "describe": "array类型，修改array<double>类型列为array其他子类型列",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_type_list = ['boolean', 'tinyint', 'smallint', 'int', 'bigint', 'largeint',
                        'deical(9, 3)', 'float', 'date',
                        'datetime', 'char(10)', 'varchar(10)', 'string']

    ret = client.create_table(table_name, SCHEMA.array_double_list, keys_desc=SCHEMA.duplicate_key)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.test_array_double_local_file, max_filter_ratio=0.01)
    assert ret, 'stream load failed'
    c_type = 'array<%s>'
    k = 'k2'
    for sub_type in column_type_list:
        tb = table_name[-50:] + '_' + sub_type.split('(')[0]
        ret = client.create_table(tb, SCHEMA.array_double_list, keys_desc=SCHEMA.duplicate_key)
        assert ret, 'create table failed'
        ret = client.stream_load(tb, FILE.test_array_double_local_file, max_filter_ratio=0.01)
        assert ret, 'stream load failed'
        ret = client.schema_change_modify_column(tb, k, c_type % sub_type, is_wait_job=True)
        assert ret, 'modify table failed'
        sql1 = 'select * from %s.%s order by k1' % (database_name, tb)
        sql2 = 'select k1, cast(%s as %s) from %s.%s order by k1' % (k, sub_type, database_name, table_name)
        common.check2(client, sql1, sql2=sql2)
        ret = client.stream_load(tb, FILE.test_array_double_local_file, max_filter_ratio=0.01)
        assert ret, 'after modify stream load failed'
        sql1 = 'select count(*) from %s.%s' % (database_name, tb)
        sql2 = 'select count(*) * 2 from %s.%s' % (database_name, table_name)
        print(client.execute(sql1))
        # common.check2(client, sql1, sql2=sql2)
    # client.clean(database_name)


@pytest.mark.skip()
def test_modify_array_column_type_array_float():
    """
    {
    "title": "test_modify_array_column_type_array_float",
    "describe": "array类型，修改array<double>类型列为其他array子类型列",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_type_list = ['boolean', 'tinyint', 'smallint', 'int', 'bigint', 'largeint',
                        'deical(9, 3)', 'double', 'date',
                        'datetime', 'char(10)', 'varchar(10)', 'string']

    ret = client.create_table(table_name, SCHEMA.array_float_list, keys_desc=SCHEMA.duplicate_key)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.test_array_float_local_file, max_filter_ratio=0.01)
    assert ret, 'stream load failed'
    c_type = 'array<%s>'
    k = 'k2'
    for sub_type in column_type_list:
        tb = table_name[-50:] + '_' + sub_type.split('(')[0]
        ret = client.create_table(tb, SCHEMA.array_float_list, keys_desc=SCHEMA.duplicate_key)
        assert ret, 'create table failed'
        ret = client.stream_load(tb, FILE.test_array_float_local_file, max_filter_ratio=0.01)
        assert ret, 'stream load failed'
        ret = client.schema_change_modify_column(tb, k, c_type % sub_type, is_wait_job=True)
        assert ret, 'modify table failed'
        sql1 = 'select * from %s.%s order by k1' % (database_name, tb)
        sql2 = 'select k1, cast(%s as %s) from %s.%s order by k1' % (k, sub_type, database_name, table_name)
        common.check2(client, sql1, sql2=sql2)
        ret = client.stream_load(tb, FILE.test_array_float_local_file, max_filter_ratio=0.01)
        assert ret, 'after modify stream load failed'
        sql1 = 'select count(*) from %s.%s' % (database_name, tb)
        sql2 = 'select count(*) * 2 from %s.%s' % (database_name, table_name)
        print(client.execute(sql1))
        # common.check2(client, sql1, sql2=sql2)
    # client.clean(database_name)


@pytest.mark.skip()
def test_modify_array_column_type_array_date():
    """
    {
    "title": "test_modify_array_column_type_array_date",
    "describe": "array类型，修改array<date>类型列为array其他子类型列",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_type_list = ['boolean', 'tinyint', 'smallint', 'int', 'bigint', 'largeint',
                        'deical(9, 3)', 'double', 'float',
                        'datetime', 'char(10)', 'varchar(10)', 'string']

    ret = client.create_table(table_name, SCHEMA.array_date_list, keys_desc=SCHEMA.duplicate_key)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.test_array_date_local_file, max_filter_ratio=0.01)
    assert ret, 'stream load failed'
    c_type = 'array<%s>'
    k = 'k2'
    for sub_type in column_type_list:
        tb = table_name[-50:] + '_' + sub_type.split('(')[0]
        ret = client.create_table(tb, SCHEMA.array_date_list, keys_desc=SCHEMA.duplicate_key)
        assert ret, 'create table failed'
        ret = client.stream_load(tb, FILE.test_array_date_local_file, max_filter_ratio=0.01)
        assert ret, 'stream load failed'
        ret = client.schema_change_modify_column(tb, k, c_type % sub_type, is_wait_job=True)
        assert ret, 'modify table failed'
        sql1 = 'select * from %s.%s order by k1' % (database_name, tb)
        sql2 = 'select k1, cast(%s as %s) from %s.%s order by k1' % (k, sub_type, database_name, table_name)
        common.check2(client, sql1, sql2=sql2)
        ret = client.stream_load(tb, FILE.test_array_date_local_file, max_filter_ratio=0.01)
        assert ret, 'after modify stream load failed'
        sql1 = 'select count(*) from %s.%s' % (database_name, tb)
        sql2 = 'select count(*) * 2 from %s.%s' % (database_name, table_name)
        print(client.execute(sql1))
        # common.check2(client, sql1, sql2=sql2)
    # client.clean(database_name)


@pytest.mark.skip()
def test_modify_array_column_type_array_datetime():
    """
    {
    "title": "test_modify_array_column_type_array_datetime",
    "describe": "array类型，修改array<datetime>类型列为array其他子类型列",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_type_list = ['boolean', 'tinyint', 'smallint', 'int', 'bigint', 'largeint',
                        'deical(9, 3)', 'double', 'float', 'date',
                        'char(10)', 'varchar(10)', 'string']

    ret = client.create_table(table_name, SCHEMA.array_datetime_list, keys_desc=SCHEMA.duplicate_key)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.test_array_datetime_local_file, max_filter_ratio=0.01)
    assert ret, 'stream load failed'
    c_type = 'array<%s>'
    k = 'k2'
    for sub_type in column_type_list:
        tb = table_name[-50:] + '_' + sub_type.split('(')[0]
        ret = client.create_table(tb, SCHEMA.array_datetime_list, keys_desc=SCHEMA.duplicate_key)
        assert ret, 'create table failed'
        ret = client.stream_load(tb, FILE.test_array_datetime_local_file, max_filter_ratio=0.01)
        assert ret, 'stream load failed'
        ret = client.schema_change_modify_column(tb, k, c_type % sub_type, is_wait_job=True)
        assert ret, 'modify table failed'
        sql1 = 'select * from %s.%s order by k1' % (database_name, tb)
        sql2 = 'select k1, cast(%s as %s) from %s.%s order by k1' % (k, sub_type, database_name, table_name)
        common.check2(client, sql1, sql2=sql2)
        ret = client.stream_load(tb, FILE.test_array_datetime_local_file, max_filter_ratio=0.01)
        assert ret, 'after modify stream load failed'
        sql1 = 'select count(*) from %s.%s' % (database_name, tb)
        sql2 = 'select count(*) * 2 from %s.%s' % (database_name, table_name)
        print(client.execute(sql1))
        # common.check2(client, sql1, sql2=sql2)
    # client.clean(database_name)


@pytest.mark.skip()
def test_modify_array_column_type_array_char():
    """
    {
    "title": "test_modify_array_column_type_array_char",
    "describe": "array类型，修改array<char>类型列为array其他子类型列",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_type_list = ['boolean', 'tinyint', 'smallint', 'int', 'bigint', 'largeint',
                        'deical(9, 3)', 'double', 'float', 'date',
                        'datetime', 'varchar(10)', 'string']

    ret = client.create_table(table_name, SCHEMA.array_char_list, keys_desc=SCHEMA.duplicate_key)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.test_array_char_local_file, max_filter_ratio=0.01)
    assert ret, 'stream load failed'
    c_type = 'array<%s>'
    k = 'k2'
    for sub_type in column_type_list:
        tb = table_name[-50:] + '_' + sub_type.split('(')[0]
        ret = client.create_table(tb, SCHEMA.array_char_list, keys_desc=SCHEMA.duplicate_key)
        assert ret, 'create table failed'
        ret = client.stream_load(tb, FILE.test_array_char_local_file, max_filter_ratio=0.01)
        assert ret, 'stream load failed'
        ret = client.schema_change_modify_column(tb, k, c_type % sub_type, is_wait_job=True)
        assert ret, 'modify table failed'
        sql1 = 'select * from %s.%s order by k1' % (database_name, tb)
        sql2 = 'select k1, cast(%s as %s) from %s.%s order by k1' % (k, sub_type, database_name, table_name)
        common.check2(client, sql1, sql2=sql2)
        ret = client.stream_load(tb, FILE.test_array_char_local_file, max_filter_ratio=0.01)
        assert ret, 'after modify stream load failed'
        sql1 = 'select count(*) from %s.%s' % (database_name, tb)
        sql2 = 'select count(*) * 2 from %s.%s' % (database_name, table_name)
        print(client.execute(sql1))
        # common.check2(client, sql1, sql2=sql2)
    # client.clean(database_name)


@pytest.mark.skip()
def test_modify_array_column_type_array_varchar():
    """
    {
    "title": "test_modify_array_column_type_array_varchar",
    "describe": "array类型，修改array<varchar>类型列为array其他子类型列",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_type_list = ['boolean', 'tinyint', 'smallint', 'int', 'bigint', 'largeint',
                        'deical(9, 3)', 'double', 'float', 'date',
                        'datetime', 'char(10)', 'string']

    ret = client.create_table(table_name, SCHEMA.array_varchar_list, keys_desc=SCHEMA.duplicate_key)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.test_array_varchar_local_file, max_filter_ratio=0.01)
    assert ret, 'stream load failed'
    c_type = 'array<%s>'
    k = 'k2'
    for sub_type in column_type_list:
        tb = table_name[-50:] + '_' + sub_type.split('(')[0]
        ret = client.create_table(tb, SCHEMA.array_varchar_list, keys_desc=SCHEMA.duplicate_key)
        assert ret, 'create table failed'
        ret = client.stream_load(tb, FILE.test_array_varchar_local_file, max_filter_ratio=0.01)
        assert ret, 'stream load failed'
        ret = client.schema_change_modify_column(tb, k, c_type % sub_type, is_wait_job=True)
        assert ret, 'modify table failed'
        sql1 = 'select * from %s.%s order by k1' % (database_name, tb)
        sql2 = 'select k1, cast(%s as %s) from %s.%s order by k1' % (k, sub_type, database_name, table_name)
        common.check2(client, sql1, sql2=sql2)
        ret = client.stream_load(tb, FILE.test_array_varchar_local_file, max_filter_ratio=0.01)
        assert ret, 'after modify stream load failed'
        sql1 = 'select count(*) from %s.%s' % (database_name, tb)
        sql2 = 'select count(*) * 2 from %s.%s' % (database_name, table_name)
        print(client.execute(sql1))
        # common.check2(client, sql1, sql2=sql2)
    # client.clean(database_name)


@pytest.mark.skip()
def test_modify_array_column_type_array_string():
    """
    {
    "title": "test_modify_array_column_type_array_string",
    "describe": "array类型，修改array<string>类型列为array其他子类型列",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_type_list = ['boolean', 'tinyint', 'smallint', 'int', 'bigint', 'largeint',
                        'deical(9, 3)', 'double', 'float', 'date',
                        'datetime', 'char(10)', 'varchar(10)']

    ret = client.create_table(table_name, SCHEMA.array_string_list, keys_desc=SCHEMA.duplicate_key)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.test_array_string_local_file, max_filter_ratio=0.01)
    assert ret, 'stream load failed'
    c_type = 'array<%s>'
    k = 'k2'
    for sub_type in column_type_list:
        tb = table_name[-50:] + '_' + sub_type.split('(')[0]
        ret = client.create_table(tb, SCHEMA.array_string_list, keys_desc=SCHEMA.duplicate_key)
        assert ret, 'create table failed'
        ret = client.stream_load(tb, FILE.test_array_string_local_file, max_filter_ratio=0.01)
        assert ret, 'stream load failed'
        ret = client.schema_change_modify_column(tb, k, c_type % sub_type, is_wait_job=True)
        assert ret, 'modify table failed'
        sql1 = 'select * from %s.%s order by k1' % (database_name, tb)
        sql2 = 'select k1, cast(%s as %s) from %s.%s order by k1' % (k, sub_type, database_name, table_name)
        common.check2(client, sql1, sql2=sql2)
        ret = client.stream_load(tb, FILE.test_array_string_local_file, max_filter_ratio=0.01)
        assert ret, 'after modify stream load failed'
        sql1 = 'select count(*) from %s.%s' % (database_name, tb)
        sql2 = 'select count(*) * 2 from %s.%s' % (database_name, table_name)
        print(client.execute(sql1))
        # common.check2(client, sql1, sql2=sql2)
    # client.clean(database_name)


def test_modify_array_column_order():
    """
    {
    "title": "test_modify_array_column_order",
    "describe": "array类型，修改表的array类型列的顺序",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, SCHEMA.array_table_list, keys_desc=SCHEMA.duplicate_key)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.test_array_table_local_file, column_separator='|')
    assert ret, 'stream load failed'
    column_name_list = ['k1', 'a14', 'a13', 'a12', 'a11', 'a10', 'a9',
                        'a8', 'a7', 'a6', 'a5', 'a4', 'a3', 'a2', 'a1']
    ret = client.schema_change_order_column(table_name, column_name_list, is_wait_job=True)
    assert ret, 'modify column order failed'
    ret = client.desc_table(table_name)
    actual_column = util.get_attr(ret, palo_job.DescInfo.Field)
    assert actual_column == column_name_list, 'column order error, expect %s, actual %s' \
                                              % (column_name_list, actual_column)
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select %s from %s.%s order by k1' % (', '.join(column_name_list), check_db, check_tb)
    common.check2(client, sql1, sql2=sql2)

    column_name_list = util.get_attr(SCHEMA.array_table_list, 0)
    ret = client.stream_load(table_name, FILE.test_array_table_local_file, column_separator='|', 
                             column_name_list=column_name_list)
    assert ret, 'stream load failed'
    time.sleep(5)
    sql1 = 'select count(*) from %s.%s' % (database_name, table_name)
    sql2 = 'select count(*) * 2 from %s.%s' % (check_db, check_tb)
    common.check2(client, sql1, sql2=sql2)
    client.clean(database_name)


def test_array_bloom_filter():
    """
    {
    "title": "test_array_bloom_filter",
    "describe": "array类型列作为bloom filter, 报错",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    bf_column = ['k1', 'a2']
    msg = 'ARRAY is not supported in bloom filter index. invalid column: a2'
    util.assert_return(False, msg, client.create_table, table_name, SCHEMA.array_table_list, 
                       keys_desc=SCHEMA.duplicate_key, bloom_filter_column_list=bf_column)
    ret = client.create_table(table_name, SCHEMA.array_table_list, keys_desc=SCHEMA.duplicate_key)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.test_array_table_local_file, column_separator='|')
    assert ret, 'stream load failed'
    util.assert_return(False, msg, client.schema_change, table_name, bloom_filter_column_list=bf_column)
    client.clean(database_name)


def test_array_mv():
    """
    {
    "title": "test_array_mv",
    "describe": "array类型不支持创建物化视图, k1, a2, group by, order by, sum/min/max",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_list = [('k0', 'int')] + SCHEMA.array_table_list
    column_name_list = util.get_attr(SCHEMA.array_table_list, 0) + ['k0=k1+1']
    ret = client.create_table(table_name, column_list, keys_desc='DUPLICATE KEY(k0, k1)')
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.test_array_table_local_file, column_separator='|', 
                             column_name_list=column_name_list)
    assert ret, 'stream load failed'
    # array, without order by/group by 
    index_name = index_name + '1'
    sql = 'select k1, a2 from %s order by k1' % table_name
    msg = ' The ARRAY column[`mv_a2` array<tinyint(4)> NOT NULL] not support to create materialized view'
    util.assert_return(False, msg, client.create_materialized_view, table_name, index_name, sql, is_wait=True)
    index_name = index_name + '2'
    sql = 'select k1, a3 from %s' % table_name
    msg = 'The ARRAY column[`mv_a3` array<smallint(6)> NOT NULL] not support to create materialized view'
    util.assert_return(False, msg, client.create_materialized_view, table_name, index_name, sql, is_wait=True)

    index_name = 'err_idx'
    # order by array
    sql = 'select k1, a4 from %s order by k1, a4' % table_name
    msg = "must use with specific function, and don't support filter or group by."
    util.assert_return(False, msg, client.create_materialized_view, table_name, index_name, sql, is_wait=True)
    # group by array
    sql = 'select k1, a5 from %s group by k1, a5' % table_name
    util.assert_return(False, msg, client.create_materialized_view, table_name, index_name, sql, is_wait=True)
    # count array
    sql = 'select k1, count(a6) from %s group by k1' % table_name
    util.assert_return(False, msg, client.create_materialized_view, table_name, index_name, sql, is_wait=True)
    # sum array
    msg_s = 'sum requires a numeric parameter: sum(`a7`)'
    sql = 'select k1, sum(a7) from %s group by k1' % table_name
    util.assert_return(False, msg_s, client.create_materialized_view, table_name, index_name, sql, is_wait=True)
    # max array
    sql = 'select k1, max(a8) from %s group by k1' % table_name
    util.assert_return(False, msg, client.create_materialized_view, table_name, index_name, sql, is_wait=True)
    # min array
    sql = 'select k1, min(a9) from %s group by k1' % table_name
    util.assert_return(False, msg, client.create_materialized_view, table_name, index_name, sql, is_wait=True)
    # bitmap_union
    msg_b = 'No matching function with signature: to_bitmap_with_check(array<date>).'
    sql = 'select k1, bitmap_union(to_bitmap(a10)) from %s group by k1' % table_name
    util.assert_return(False, msg_b, client.create_materialized_view, table_name, index_name, sql, is_wait=True)
    # hll_union 
    msg_h = 'No matching function with signature: hll_hash(array<datetime>).'
    sql = 'select k1, hll_union_agg(hll_hash(a11)) from %s group by k1' % table_name
    util.assert_return(False, msg_h, client.create_materialized_view, table_name, index_name, sql, is_wait=True)
    client.clean(database_name)


def test_array_export():
    """
    {
    "title": "test_array_export",
    "describe": "array类型，导出成功",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, SCHEMA.array_table_list, keys_desc=SCHEMA.duplicate_key)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.test_array_table_local_file, column_separator='|')
    assert ret, 'stream load failed'
    property_dict = {'column_separator': '|'}
    ret = client.export(table_name, FILE.export_to_hdfs_path, broker_info=broker_info, property_dict=property_dict)
    assert ret, 'export failed'
    client.wait_export()
    ret = client.show_export(state="FINISHED")
    assert len(ret) == 1
    client.clean(database_name)


def test_array_outfile_csv():
    """
    {
    "title": "test_array_outfile_csv",
    "describe": "array类型，查询结果导出为csv",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, SCHEMA.array_table_list, keys_desc=SCHEMA.duplicate_key)
    assert ret, 'create table failed'
    load_data_info = palo_client.LoadDataInfo(FILE.test_array_table_remote_file, table_name, column_terminator='|')
    ret = client.batch_load(util.get_label(), load_data_info, broker=broker_info, max_filter_ratio=0.01, is_wait=True)
    assert ret, 'broker load failed'

    sql = 'select * from %s' % table_name
    csv_output_path = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name, util.get_label(),
                                                                             util.get_label())
    ret = client.select_into(sql, csv_output_path, broker_info, format_as='csv')
    print(ret)
    assert ret, 'select into csv failed'

    csv_check_table = table_name + '_csv'
    csv_load_file = str(palo_job.SelectIntoInfo(ret[0]).get_url() + '*')
    ret = client.create_table(csv_check_table, SCHEMA.array_table_list, keys_desc=SCHEMA.duplicate_key)
    assert ret, 'create table failed'
    load_data_list = palo_client.LoadDataInfo(csv_load_file, csv_check_table)
    ret = client.batch_load(util.get_label(), load_data_list, broker=broker_info, is_wait=True)
    assert ret, 'csv outfile load failed'

    for k in (1, 2, 3, 4, 5, 6, 7, 10, 11, 12, 13, 14):
        sql1 = 'select a%s from %s order by k1' % (k, table_name)
        sql2 = 'select a%s from %s order by k1' % (k, csv_check_table)
        common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    # array double & float 精度与原数据不一致，使用array_sum用作校验
    for k in (8, 9):
        sql1 = 'select size(a%s), array_sum(a%s) from %s order by k1' % (k, k, table_name)
        sql2 = 'select size(a%s), array_sum(a%s) from %s order by k1' % (k, k, csv_check_table)
        common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    client.clean(database_name)


def test_array_outfile_format():
    """
    {
    "title": "test_array_outfile_format",
    "describe": "array类型，导出格式测试，array不支持导出为parquet格式",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, SCHEMA.array_table_list, keys_desc=SCHEMA.duplicate_key)
    assert ret, 'create table failed'
    load_data_info = palo_client.LoadDataInfo(FILE.test_array_table_remote_file, table_name, column_terminator='|')
    ret = client.batch_load(util.get_label(), load_data_info, broker=broker_info, max_filter_ratio=0.01, is_wait=True)
    assert ret, 'broker load failed'

    sql = 'select * from %s' % table_name
    csv_output_path = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name, util.get_label(),
                                                                             util.get_label())
    ret = client.select_into(sql, csv_output_path, broker_info, format_as='csv_with_names')
    print(ret)
    assert ret, 'select into csv failed'

    csv_output_path = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name, util.get_label(),
                                                                             util.get_label())
    ret = client.select_into(sql, csv_output_path, broker_info, format_as='csv_with_names_and_types')
    print(ret)
    assert ret, 'select into csv failed'

    # not support outfile parquet
    parquet_output_path = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name, util.get_label(),
                                                                                util.get_label())
    msg = 'currently parquet do not support column type: ARRAY'
    util.assert_return(False, msg, client.select_into, sql, parquet_output_path, broker_info, format_as='parquet')
    client.clean(database_name)


def test_array_delete():
    """
    {
    "title": "test_array_delete",
    "describe": "array类型删除，删除后数据正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, SCHEMA.array_table_list, keys_desc=SCHEMA.duplicate_key)
    assert ret, 'create table failed'
    load_data_info = palo_client.LoadDataInfo(FILE.test_array_table_remote_file, table_name, column_terminator='|')
    ret = client.batch_load(util.get_label(), load_data_info, broker=broker_info, max_filter_ratio=0.01, is_wait=True)
    assert ret, 'broker load failed'
    ret = client.delete(table_name, 'k1 > 0')
    assert ret, 'delete failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.%s where k1<= 0 order by k1' % (check_db, check_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    ret = client.delete(table_name, 'a1 is null')
    assert ret, 'delete failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.%s where k1<= 0 order by k1' % (check_db, check_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    ret = client.delete(table_name, 'a1 is not null and k1 > -10')
    assert ret, 'delete failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.%s where k1<= -10 order by k1' % (check_db, check_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    client.truncate(table_name)
    ret = client.select_all(table_name)
    assert ret == (), 'expect empty table'
    client.clean(database_name)


if __name__ == '__main__': 
    setup_module()
    # todo modify
