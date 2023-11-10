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
  * @file test_sys_partition_multi_col.py
  * @brief Test for partition by multi column
  *
  **************************************************************************/
"""
import time
import pytest

from data import schema as DATA
from data import load_file as FILE
from lib import palo_client
from lib import util
from lib import common
from lib import palo_job
from lib import palo_task

LOG = palo_client.LOG
L = palo_client.L


def setup_module():
    """
    setUp
    """
    pass


def teardown_module():
    """
    tearDown
    """
    pass


def test_update_tinyint():
    """
    {
    "title": "test_update_tinyint",
    "describe": "update数据类型tinyint, 数据溢出，Null，数据校验正确",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.tinyint_column_no_agg_list, keys_desc=DATA.unique_key)
    assert ret, 'create table failed'
    sql = 'insert into %s values(1, 2, 3, 4, 5)' % table_name
    ret = client.execute(sql)
    assert ret == (), 'insert date failed'
    # null
    util.assert_return(False, '', client.update, table_name, ['v1=null'], 'k1=1')
    # 溢出
    msg = 'Number out of range[-129]. type: TINYINT'
    util.assert_return(False, msg, client.update, table_name, ['v1=-129'], 'k1=1')
    msg = 'Number out of range[128]. type: TINYINT'
    util.assert_return(False, msg, client.update, table_name, ['v1=128'], 'k1=1')

    # 多个数据导入
    assert client.truncate(table_name)
    assert len(client.select_all(table_name)) == 0
    ret = client.stream_load(table_name, FILE.test_tinyint_unique_file)
    assert ret, 'stream load failed'
    sql = 'select k1, -1, -1, -1, -1 from %s order by k1' % table_name
    ret1 = client.execute(sql)
    ret = client.update(table_name, ['v1=-1', 'v2=-1', 'v3=-1', 'v4=-1'], 'k1 is not null')
    assert ret, 'update table failed'
    sql = 'select k1, v1, v2, v3, v4 from %s order by k1' % table_name
    ret2 = client.execute(sql)
    util.check(ret1, ret2)
    client.clean(database_name)


def test_update_smallint():
    """
    {
    "title": "test_update_smallint",
    "describe": "update数据类型smallint，数据溢出，Null，数据校验正确",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.smallint_column_no_agg_list, keys_desc=DATA.unique_key, set_null=True)
    assert ret, 'create table failed'
    sql = 'insert into %s values(1, 2, 3, 4, 5)' % table_name
    ret = client.execute(sql)
    assert ret == (), 'insert date failed'
    # null
    ret = client.update(table_name, ['v1=null'], 'k1=1')
    assert ret, 'update table failed'
    common.check2(client, sql1='select * from %s' % table_name, sql2='select 1, null, 3, 4, 5')
    # 溢出
    msg = 'Number out of range[-32769]. type: SMALLINT'
    util.assert_return(False, msg, client.update, table_name, ['v1=-32769'], 'k1=1')
    msg = 'Number out of range[32768]. type: SMALLINT'
    util.assert_return(False, msg, client.update, table_name, ['v1=32768'], 'k1=1')
    # 多个数据导入
    assert client.truncate(table_name)
    assert len(client.select_all(table_name)) == 0
    ret = client.stream_load(table_name, FILE.test_smallint_file, max_filter_ratio=0.1)
    assert ret, 'stream load failed'
    sql = 'select k1, -1, -1, -1, -1 from %s order by k1' % table_name
    ret1 = client.execute(sql)
    ret = client.update(table_name, ['v1=-1, v2=-1, v3=-1, v4=-1'], 'k1 is not null or k1 is null')
    assert ret, 'update table failed'
    sql = 'select k1, v1, v2, v3, v4 from %s order by k1' % table_name
    ret2 = client.execute(sql)
    util.check(ret1, ret2)
    client.clean(database_name)


def test_update_int():
    """
    {
    "title": "test_update_int",
    "describe": "update数据类型int，数据溢出，Null，数据校验正确",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.int_column_no_agg_list, keys_desc=DATA.unique_key)
    assert ret, 'create table failed'
    sql = 'insert into %s values(1, 2, 3, 4, 5)' % table_name
    ret = client.execute(sql)
    assert ret == (), 'insert date failed'
    # null, not null列 update failed
    util.assert_return(False, '', client.update, table_name, ['v1=NULL'], 'k1=1')
    # 溢出
    msg = 'Number out of range[-2147483649]. type: INT'
    util.assert_return(False, msg, client.update, table_name, ['v1=-2147483649'], 'k1=1')
    msg = 'Number out of range[2147483648]. type: INT'
    util.assert_return(False, msg, client.update, table_name, ['v1=2147483648'], 'k1=1')
    # 多个数据导入
    assert client.truncate(table_name)
    assert len(client.select_all(table_name)) == 0
    ret = client.stream_load(table_name, FILE.test_int_file, max_filter_ratio=0.01)
    assert ret, 'stream load failed'
    sql = 'select k1, -1, -1, -1, -1 from %s order by k1' % table_name
    ret1 = client.execute(sql)
    ret = client.update(table_name, ['v1=-1, v2=-1, v3=-1, v4=-1'], 'k1 is not null')
    assert ret, 'update table failed'
    sql = 'select k1, v1, v2, v3, v4 from %s order by k1' % table_name
    ret2 = client.execute(sql)
    util.check(ret1, ret2)
    client.clean(database_name)


def test_update_bigint():
    """
    {
    "title": "test_update_bigint",
    "describe": "update数据类型bigint，数据溢出，Null，数据校验正确",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.bigint_column_no_agg_list, keys_desc=DATA.unique_key, set_null=True)
    assert ret, 'create table failed'
    sql = 'insert into %s values(1, 2, 3, 4, 5)' % table_name
    ret = client.execute(sql)
    assert ret == (), 'insert date failed'
    # null todo
    ret = client.update(table_name, ['v1=null'], 'k1=1')
    assert ret, 'update table failed'
    common.check2(client, sql1='select * from %s' % table_name, sql2='select 1, null, 3, 4, 5')
    # 溢出
    msg = 'Number out of range[-9223372036854775809]. type: BIGINT'
    util.assert_return(False, msg, client.update, table_name, ['v1=-9223372036854775809'], 'k1=1')
    msg = 'Number out of range[9223372036854775808]. type: BIGINT'
    util.assert_return(False, msg, client.update, table_name, ['v1=9223372036854775808'], 'k1=1')
    # 多个数据导入
    assert client.truncate(table_name)
    assert len(client.select_all(table_name)) == 0
    ret = client.stream_load(table_name, FILE.test_bigint_file, max_filter_ratio=0.01)
    assert ret, 'stream load failed'
    sql = 'select k1, -1, -1, -1, -1 from %s order by k1' % table_name
    ret1 = client.execute(sql)
    ret = client.update(table_name, ['v1=-1, v2=-1, v3=-1, v4=-1'], 'k1 is not null or k1 is null')
    assert ret, 'update table failed'
    sql = 'select k1, v1, v2, v3, v4 from %s order by k1' % table_name
    ret2 = client.execute(sql)
    util.check(ret1, ret2)
    client.clean(database_name)


def test_update_largeint():
    """
    {
    "title": "test_update_largeint",
    "describe": "update数据类型largeint，数据溢出，Null，数据校验正确",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.largeint_column_no_agg_list, keys_desc=DATA.unique_key, set_null=True)
    assert ret, 'create table failed'
    sql = 'insert into %s values(1, 2, 3, 4, 5)' % table_name
    ret = client.execute(sql)
    assert ret == (), 'insert date failed'
    # null
    ret = client.update(table_name, ['v1=null'], 'k1=1')
    assert ret, 'update table failed'
    common.check2(client, sql1='select * from %s' % table_name, sql2='select 1, null, 3, 4, 5')
    # 溢出
    msg = 'Numeric overflow'
    util.assert_return(False, msg, client.update, table_name, ['v1=-170141183460469231731687303715884105729'], 'k1=1')
    msg = 'Number Overflow. literal: 170141183460469231731687303715884105728'
    util.assert_return(False, msg, client.update, table_name, ['v1=170141183460469231731687303715884105728'], 'k1=1')
    # 多个数据导入
    assert client.truncate(table_name)
    assert len(client.select_all(table_name)) == 0
    ret = client.stream_load(table_name, FILE.test_largeint_file, max_filter_ratio=0.01)
    assert ret, 'stream load failed'
    sql = 'select k1, "-1", "-1", "-1", "-1" from %s order by k1' % table_name
    ret1 = client.execute(sql)
    ret = client.update(table_name, ['v1=-1, v2=-1, v3=-1, v4=-1'], 'k1 is not null or k1 is null')
    assert ret, 'update table failed'
    sql = 'select k1, v1, v2, v3, v4 from %s order by k1' % table_name
    ret2 = client.execute(sql)
    util.check(ret1, ret2)
    client.clean(database_name)


def test_update_char():
    """
    {
    "title": "test_update_char",
    "describe": "update数据类型char，长度溢出，Null，空串，数据校验正确",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.char_normal_column_no_agg_list, keys_desc=DATA.unique_key)
    assert ret, 'create table failed'
    sql = 'insert into %s values("1", "2")' % table_name
    ret = client.execute(sql)
    assert ret == (), 'insert date failed'
    # null
    util.assert_return(False, '', client.update, table_name, ['v1=null'], 'k1=1')
    # 溢出
    util.assert_return(False, '', client.update, table_name,
                       ['v1="-170141183460469231731687303715884105729"'], 'k1=1')
    # 空串
    ret = client.update(table_name, ['v1=""'], 'k1=1')
    assert ret, 'update table failed'
    common.check2(client, sql1='select * from %s' % table_name, sql2='select "1", ""')
    # 多个数据导入
    assert client.truncate(table_name)
    assert len(client.select_all(table_name)) == 0
    ret = client.stream_load(table_name, FILE.test_char_normal_file, max_filter_ratio=0.01)
    assert ret, 'stream load failed'
    sql = 'select k1, "-1" from %s order by k1' % table_name
    ret1 = client.execute(sql)
    ret = client.update(table_name, ['v1="-1"'], 'k1 is not null')
    assert ret, 'update table failed'
    sql = 'select k1, v1 from %s order by k1' % table_name
    ret2 = client.execute(sql)
    util.check(ret1, ret2)
    client.clean(database_name)


def test_update_varchar():
    """
    {
    "title": "test_update_varchar",
    "describe": "update数据类型varchar，长度溢出，Null，空串，数据校验正确",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.varchar_least_column_no_agg_list, keys_desc=DATA.unique_key)
    assert ret, 'create table failed'
    sql = 'insert into %s values("1", "2")' % table_name
    ret = client.execute(sql)
    assert ret == (), 'insert date failed'
    # null
    util.assert_return(False, '', client.update, table_name, ['v1=null'], 'k1=1')
    # 溢出
    util.assert_return(False, '', client.update, table_name,
                       ['v1="-170141183460469231731687303715884105729"'], 'k1=1')
    # 空串
    ret = client.update(table_name, ['v1=""'], 'k1=1')
    assert ret, 'update table failed'
    common.check2(client, sql1='select * from %s' % table_name, sql2='select "1", ""')
    # 多个数据导入
    assert client.drop_table(table_name)
    ret = client.create_table(table_name, DATA.varchar_normal_column_no_agg_list, keys_desc=DATA.unique_key)
    assert ret, 'create table failed'
    assert len(client.select_all(table_name)) == 0
    ret = client.stream_load(table_name, FILE.test_varchar_normal_file, max_filter_ratio=0.02)
    assert ret, 'stream load failed'
    sql = 'select k1, "-1" from %s order by k1' % table_name
    ret1 = client.execute(sql)
    ret = client.update(table_name, ['v1="-1"'], 'k1 is not null')
    assert ret, 'update table failed'
    sql = 'select k1, v1 from %s order by k1' % table_name
    ret2 = client.execute(sql)
    util.check(ret1, ret2)
    client.clean(database_name)


def test_update_date():
    """
    {
    "title": "test_update_date",
    "describe": "update数据类型date，各种日期格式，数据校验正确",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.date_column_no_agg_list, keys_desc=DATA.unique_key)
    assert ret, 'create table failed'
    sql = 'insert into %s values("2000-01-01", "2000-01-02", "2000-01-03", "2000-01-04")' % table_name
    ret = client.execute(sql)
    assert ret == (), 'insert date failed'
    # null
    util.assert_return(False, '', client.update, table_name, ['v1=null'], 'k1="2000-01-01"')
    # 正常数据
    ret = client.update(table_name, ['v1="2020-01-01"'], 'k1="2000-01-01"')
    assert ret, 'update table failed'
    sql1 = 'select * from %s' % table_name
    sql2 = 'select cast("2000-01-01" as date), cast("2020-01-01" as date), ' \
           'cast("2000-01-03" as date), cast("2000-01-04" as date)'
    common.check2(client, sql1, sql2=sql2)
    # int数据
    ret = client.update(table_name, ['v1="20200102"'], 'k1="2000-01-01"')
    assert ret, 'update table failed'
    sql1 = 'select * from %s' % table_name
    sql2 = 'select cast("2000-01-01" as date), cast("2020-01-02" as date), ' \
           'cast("2000-01-03" as date), cast("2000-01-04" as date)'
    common.check2(client, sql1, sql2=sql2)
    # 00数据
    util.assert_return(False, '', client.update, table_name, ['v1="0000-00-00"'], 'k1="2000-01-01"')
    # 不支持数据
    util.assert_return(False, '', client.update, table_name, ['v1="2000-13-01"'], 'k1="2000-01-01"')
    # 多个数据导入
    assert client.truncate(table_name)
    assert len(client.select_all(table_name)) == 0
    ret = client.stream_load(table_name, FILE.test_date_file, max_filter_ratio=0.015)
    assert ret, 'stream load failed'
    sql = 'select k1, "2021-01-01", "2021-01-03", "2021-01-02" from %s order by k1' % table_name
    ret1 = client.execute(sql)
    ret = client.update(table_name, ['v1="2021-01-01"', 'v2="2021-01-03"', 'v3="2021-01-02"'], 'k1 is not null')
    assert ret, 'update table failed'
    sql = 'select k1, v1, v2, v3 from %s order by k1' % table_name
    ret2 = client.execute(sql)
    util.check(ret1, ret2)
    client.clean(database_name)


def test_update_datetime():
    """
    {
    "title": "test_update_datetime",
    "describe": "update数据类型datetime，各种日期格式，数据校验正确",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.datetime_column_no_agg_list, keys_desc=DATA.unique_key)
    assert ret, 'create table failed'
    sql = 'insert into %s values("2000-01-01 00:00:00", "2000-01-02 01:00:00", ' \
          '"2000-01-03 00:00:00", "2000-01-04 00:00:01")' % table_name
    ret = client.execute(sql)
    assert ret == (), 'insert date failed'
    # null
    util.assert_return(False, '', client.update, table_name, ['v1=null'], 'k1="2000-01-01 00:00:00"')
    # 正常
    ret = client.update(table_name, ['v1="2020-01-01 00:00:00"'], 'k1="2000-01-01 00:00:00"')
    assert ret, 'update failed'
    sql1 = 'select * from %s' % table_name
    sql2 = 'select cast("2000-01-01 00:00:00" as datetime), cast("2020-01-01 00:00:00" as datetime), ' \
           'cast("2000-01-03 00:00:00" as datetime), cast("2000-01-04 00:00:01" as datetime)'
    common.check2(client, sql1, sql2=sql2)
    # 溢出
    util.assert_return(False, '', client.update, table_name, ['v1="10020-01-01 00:00:00"'], 'k1="2000-01-01 00:00:00"')
    # 00数据
    util.assert_return(False, '', client.update, table_name, ['v1="2020-01-00 00:00:00"'], 'k1="2000-01-01 00:00:00"')
    # 不支持datetime
    util.assert_return(False, '', client.update, table_name, ['v1="2020-13-01 00:00:00"'], 'k1="2000-01-01 00:00:00"')
    # 多个数据导入
    assert client.truncate(table_name)
    assert len(client.select_all(table_name)) == 0
    ret = client.stream_load(table_name, FILE.test_datetime_file, max_filter_ratio=0.015)
    assert ret, 'stream load failed'
    sql = 'select k1, "2021-01-01 08:00:00", "2021-01-03 08:00:00", "2021-01-02 08:00:00" ' \
          'from %s order by k1' % table_name
    ret1 = client.execute(sql)
    ret = client.update(table_name, ['v1="2021-01-01 08:00:00"',
                                     'v2="2021-01-03 08:00:00"',
                                     'v3="2021-01-02 08:00:00"'], 'k1 is not null')
    assert ret, 'update table failed'
    sql = 'select k1, v1, v2, v3 from %s order by k1' % table_name
    ret2 = client.execute(sql)
    util.check(ret1, ret2)
    client.clean(database_name)


def test_update_decimal():
    """
    {
    "title": "test_update_decimal",
    "describe": "update数据类型decimal，精度溢出，数据校验正确",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.decimal_normal_column_no_agg_list, 
                              keys_desc=DATA.unique_key, set_null=True)
    assert ret, 'create table failed'
    sql = 'insert into %s values(1.001, 2.002, 3.003, 4.004, 5.006)' % table_name
    ret = client.execute(sql)
    assert ret == (), 'insert date failed'
    # null
    ret = client.update(table_name, ['v1=null'], 'k1 > 0')
    assert ret, 'update failed'
    sql1 = 'select * from %s' % table_name
    sql2 = 'select 1.001, null, 3.003, 4.004, 5.006'
    common.check2(client, sql1=sql1, sql2=sql2)
    # 溢出
    ret = client.update(table_name, ['v1="0.00000001"'], 'k1 = 1.001')
    assert ret, 'update failed'
    sql1 = 'select * from %s' % table_name
    sql2 = 'select 1.001, 0, 3.003, 4.004, 5.006'
    common.check2(client, sql1=sql1, sql2=sql2)
    util.assert_return(False, '', client.update, table_name, ['v1=100000000000.001'], 'k1 = 1.001')
    # 格式不正确
    util.assert_return(False, '', client.update, table_name, ['v1=123456'], 'k1 = 1.001')
    msg = 'Unexpected exception: Character a is neither a decimal digit number, ' \
          'decimal point, nor "e" notation exponential mark'
    util.assert_return(False, msg, client.update, table_name, ['v1="aaa"'], 'k1 = 1.001')
    # 多个数据导入
    assert client.truncate(table_name)
    assert len(client.select_all(table_name)) == 0
    ret = client.stream_load(table_name, FILE.test_decimal_normal_file, max_filter_ratio=0.01)
    assert ret, 'stream load failed'
    sql = 'select k1, 2.002, 3.003, 4.004, 5.006 from %s order by k1' % table_name
    ret1 = client.execute(sql)
    ret = client.update(table_name, ['v1=2.002', 'v2=3.003', 'v3=4.004', 'v4=5.006'], 'k1 is not null')
    assert ret, 'update table failed'
    sql = 'select k1, v1, v2, v3, v4 from %s order by k1' % table_name
    ret2 = client.execute(sql)
    util.check(ret1, ret2)
    client.clean(database_name)


def test_update_double():
    """
    {
    "title": "test_update_double",
    "describe": "update数据类型double，精度，数据校验正确",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.double_column_no_agg_list, keys_desc=DATA.unique_key)
    assert ret, 'create table failed'
    sql = 'insert into %s values(1, 2.002, 3.003, 4.004, 5.006)' % table_name
    ret = client.execute(sql)
    assert ret == (), 'insert date failed'
    # null
    util.assert_return(False, '', client.update, table_name, ['v1=null'], 'k1=1')
    # 溢出
    ret = client.update(table_name, ['v2="1.99999999999999999"'], 'k1=1')
    assert ret, 'update table failed'
    sql1 = 'select * from %s' % table_name
    sql2 = 'select 1, 2.002, 2.0, 4.004, 5.006'
    common.check2(client, sql1, sql2=sql2)
    # 格式
    ret = client.update(table_name, ['v2="1.009e2"'], 'k1=1')
    assert ret, 'update table failed'
    sql2 = 'select 1, 2.002, 100.9, 4.004, 5.006'
    common.check2(client, sql1, sql2=sql2)
    ret = client.update(table_name, ['v2=2.0003e-2'], 'k1=1')
    assert ret, 'update table failed'
    sql2 = 'select 1, 2.002, 0.020003, 4.004, 5.006'
    common.check2(client, sql1, sql2=sql2)
    # msg = "'a3' is not a number"
    msg = 'error totally whack'
    util.assert_return(False, msg, client.update, table_name, ['v2="a3"'], 'k1=1')
    print(client.select_all(table_name))

    # 多个数据导入
    assert client.truncate(table_name)
    assert len(client.select_all(table_name)) == 0
    ret = client.stream_load(table_name, FILE.test_double_int_file, max_filter_ratio=0.01)
    assert ret, 'stream load failed'
    sql = 'select k1, 2.002, 3.003, 4.004, 5.006 from %s order by k1' % table_name
    ret1 = client.execute(sql)
    ret = client.update(table_name, ['v1=2.002', 'v2=3.003', 'v3=4.004', 'v4=5.006'], 'k1 is not null')
    assert ret, 'update table failed'
    sql = 'select k1, v1, v2, v3, v4 from %s order by k1' % table_name
    ret2 = client.execute(sql)
    util.check(ret1, ret2)
    client.clean(database_name)


def test_update_float():
    """
    {
    "title": "test_update_float",
    "describe": "update数据类型float，精度，数据校验正确",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.float_column_no_agg_list, keys_desc=DATA.unique_key)
    assert ret, 'create table failed'
    sql = 'insert into %s values(1, 2.002, 3.003, 4.004, 5.006)' % table_name
    ret = client.execute(sql)
    assert ret == (), 'insert date failed'
    # null
    util.assert_return(False, '', client.update, table_name, ['v1=null'], 'k1 = 1')
    # 溢出, todo
    ret = client.update(table_name, ['v2="1.99999999999999999"'], 'k1=1')
    assert ret, 'update table failed'
    sql1 = 'select * from %s' % table_name
    sql2 = 'select 1, 2.002, 2.0, 4.004, 5.006'
    common.check2(client, sql1, sql2=sql2)
    # 格式不正确
    ret = client.update(table_name, ['v2="1.009e2"'], 'k1=1')
    assert ret, 'update table failed'
    sql1 = 'select * from %s' % table_name
    sql2 = 'select 1, 2.002, 100.9, 4.004, 5.006'
    common.check2(client, sql1, sql2=sql2)
    ret = client.update(table_name, ['v2=2.0003e-2'], 'k1=1')
    assert ret, 'update table failed'
    sql1 = 'select * from %s' % table_name
    sql2 = 'select 1, 2.002, 0.020003, 4.004, 5.006'
    common.check2(client, sql1, sql2=sql2)
    # msg = "'a3' is not a number"
    msg = 'error totally whack'
    util.assert_return(False, msg, client.update, table_name, ['v2="a3"'], 'k1=1')
    print(client.select_all(table_name))
    # 多个数据导入
    assert client.truncate(table_name)
    assert len(client.select_all(table_name)) == 0
    ret = client.stream_load(table_name, FILE.test_float_int_file, max_filter_ratio=0.01)
    assert ret, 'stream load failed'
    sql = 'select k1, 2.002, 3.003, 4.004, 5.006 from %s order by k1' % table_name
    ret1 = client.execute(sql)
    ret = client.update(table_name, ['v1=2.002', 'v2=3.003', 'v3=4.004', 'v4=5.006'], 'k1 is not null')
    assert ret, 'update table failed'
    sql = 'select k1, v1, v2, v3, v4 from %s order by k1' % table_name
    ret2 = client.execute(sql)
    util.check(ret1, ret2)
    client.clean(database_name)


def test_update_other_table():
    """
    {
    "title": "test_update_other_table",
    "describe": "update只支持unique表，不支持duplicate表和aggregate表",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    # 创建不同类型的表，并导入数据
    agg_tb = table_name + '_agg'
    dup_tb = table_name + '_dup'
    uniq_tb = table_name + '_uniq'
    ret = client.create_table(agg_tb, DATA.partition_column_list)
    assert ret, 'create table failed'
    ret = client.create_table(dup_tb, DATA.partition_column_no_agg_list)
    assert ret, 'create table failed'
    ret = client.create_table(uniq_tb, DATA.partition_column_no_agg_list, keys_desc=DATA.unique_key)
    assert ret, 'create table failed'
    for tb in [agg_tb, dup_tb, uniq_tb]:
        ret = client.stream_load(tb, FILE.partition_local_file)
        assert ret, 'stream load failed'
    # update不同类型的表
    ret = client.update(uniq_tb, ['v2="2"'], 'k1=1')
    assert ret, 'update failed'
    msg = 'Only unique table could be updated'
    util.assert_return(False, msg, client.update, agg_tb, ['v2="2"'], 'k1=1')
    msg = 'Only unique table could be updated'
    util.assert_return(False, msg, client.update, dup_tb, ['v2="2"'], 'k1=1')
    client.clean(database_name)


def test_update_key():
    """
    {
    "title": "test_update_key",
    "describe": "update只支持value列，不支持key列，包括分区列，分桶列",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    partition_info = palo_client.PartitionInfo("k2",
                                       ["p1", "p2", "p3", "p4", "p5"],
                                       ["-10", "0", "10", "100", "MAXVALUE"])
    distribution_info = palo_client.DistributionInfo('hash(k1)', 10)
    keys_desc = "UNIQUE KEY(k1, k2, k3, k4, k5)"
    ret = client.create_table(table_name, DATA.partition_column_no_agg_list, keys_desc=keys_desc,
                              partition_info=partition_info, distribution_info=distribution_info)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.partition_local_file)
    assert ret, 'load data failed'
    # update key列/分桶列/分区列
    msg = 'Only value columns of unique table could be updated'
    util.assert_return(False, msg, client.update, table_name, ['k3=0'], 'k1=1')
    msg = 'Only value columns of unique table could be updated'
    util.assert_return(False, msg, client.update, table_name, ['k1=-10'], 'k4=1000')
    msg = 'Only value columns of unique table could be updated'
    util.assert_return(False, msg, client.update, table_name, ['k2=-10'], 'k4=1000')
    client.clean(database_name)


def test_update_multi_row():
    """
    {
    "title": "test_update_multi_row",
    "describe": "update多行数据，多个分区的多条数据",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    partition_info = palo_client.PartitionInfo("k2",
                                               ["p1", "p2", "p3", "p4", "p5"],
                                               ["0", "100", "200", "300", "MAXVALUE"])
    distribution_info = palo_client.DistributionInfo('hash(k1)', 10)
    keys_desc = "UNIQUE KEY(k1, k2, k3, k4, k5)"
    ret = client.create_table(table_name, DATA.partition_column_no_agg_list, keys_desc=keys_desc,
                              partition_info=partition_info, distribution_info=distribution_info)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.partition_local_file)
    assert ret, 'load data failed'
    sql1 = 'select k1, k2, k3, k4, k5, v1, v2, "after update" v3, v4, v5, v6 from %s order by k1' % table_name
    ret1 = client.execute(sql1)
    # update多个分区的多个值
    ret = client.update(table_name, ['v3="after update"'], 'k1 > 0')
    assert ret, 'update failed'
    sql2 = 'select * from %s order by k1' % table_name
    ret2 = client.execute(sql2)
    util.check(ret1, ret2)
    client.clean(database_name)


def test_update_multi_col():
    """
    {
    "title": "test_update_multi_col",
    "describe": "update多个列",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    partition_info = palo_client.PartitionInfo("k2",
                                               ["p1", "p2", "p3", "p4", "p5"],
                                               ["0", "100", "200", "300", "MAXVALUE"])
    distribution_info = palo_client.DistributionInfo('hash(k1)', 10)
    keys_desc = "UNIQUE KEY(k1, k2, k3, k4, k5)"
    ret = client.create_table(table_name, DATA.partition_column_no_agg_list, keys_desc=keys_desc,
                              partition_info=partition_info, distribution_info=distribution_info)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.partition_local_file)
    assert ret, 'load data failed'
    sql1 = 'select k1, k2, k3, k4, k5, "2020-01-20", "-", "after update" v3, 0, 0, 0 from %s order by k1' % table_name
    ret1 = client.execute(sql1)
    # update多个分区的多个值
    ret = client.update(table_name, ['v3="after update"', 
                                     'v2="-"', 
                                     'v1="2020-01-20"', 
                                     'v4=0', 
                                     'v5=0', 
                                     'v6=0'], 'k1 > 0')
    assert ret, 'update failed'
    sql2 = 'select * from %s order by k1' % table_name
    ret2 = client.execute(sql2)
    util.check(ret1, ret2)
    client.clean(database_name)


def test_update_empty():
    """
    {
    "title": "test_update_empty",
    "describe": "update空表",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    partition_info = palo_client.PartitionInfo("k2",
                                               ["p1", "p2", "p3", "p4", "p5"],
                                               ["0", "100", "200", "300", "MAXVALUE"])
    distribution_info = palo_client.DistributionInfo('hash(k1)', 10)
    keys_desc = "UNIQUE KEY(k1, k2, k3, k4, k5)"
    ret = client.create_table(table_name, DATA.partition_column_no_agg_list, keys_desc=keys_desc,
                              partition_info=partition_info, distribution_info=distribution_info)
    assert ret, 'create table failed'
    # update多个分区的多个值
    ret = client.update(table_name, ['v3="after update"',
                                     'v2="-"',
                                     'v5=0',
                                     'v6=0'], 'k1 > 0')
    assert ret, 'update failed'
    sql2 = 'select * from %s order by k1' % table_name
    ret2 = client.execute(sql2)
    assert len(ret2) == 0, 'check data error'
    ret = client.stream_load(table_name, FILE.partition_local_file)
    assert ret, 'load data failed'
    assert client.verify(FILE.partition_local_file, table_name)
    client.clean(database_name)


def test_update_condition():
    """
    {
    "title": "test_update_condition",
    "describe": "update中where条件，in，like，< > =，between...and，not，is null，is not null，函数，子查询，and，or, empty",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    partition_info = palo_client.PartitionInfo("k2",
                                               ["p1", "p2", "p3", "p4", "p5"],
                                               ["0", "100", "200", "300", "MAXVALUE"])
    distribution_info = palo_client.DistributionInfo('hash(k1)', 10)
    keys_desc = "UNIQUE KEY(k1, k2, k3, k4, k5)"
    ret = client.create_table(table_name, DATA.partition_column_no_agg_list, keys_desc=keys_desc,
                              partition_info=partition_info, distribution_info=distribution_info)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.partition_local_file, partition_list=["p5"], max_filter_ratio=1)
    assert ret, 'load data failed'
    sql1 = 'select * from %s' % table_name
    sql2 = 'select 30, 300, 3000, 30000, cast("2040-01-01 00:00:00" as datetime), cast("2010-01-30" as date), "d", ' \
           '"tbuvpobzluhbwkljlhwnrkrhowybk", 949.57562, 976.26649, 2579.668636'
    common.check2(client, sql1=sql1, sql2=sql2)

    # in
    ret = client.update(table_name, ['v6=0'], 'v2 in ("d", "a", "b")')
    assert ret, 'update failed'
    sql2 = 'select 30, 300, 3000, 30000, cast("2040-01-01 00:00:00" as datetime), cast("2010-01-30" as date), "d", ' \
           '"tbuvpobzluhbwkljlhwnrkrhowybk", 949.57562, 976.26649, 0'
    common.check2(client, sql1=sql1, sql2=sql2)

    # like
    ret = client.update(table_name, ['v6=1'], 'v3 like "%%wnrk"')
    assert ret, 'update failed'
    sql2 = 'select 30, 300, 3000, 30000, cast("2040-01-01 00:00:00" as datetime), cast("2010-01-30" as date), "d", ' \
           '"tbuvpobzluhbwkljlhwnrkrhowybk", 949.57562, 976.26649, 0'
    common.check2(client, sql1=sql1, sql2=sql2)
    ret = client.update(table_name, ['v6=1'], 'v3 like "%%wnrk%%"')
    assert ret, 'update failed'
    sql2 = 'select 30, 300, 3000, 30000, cast("2040-01-01 00:00:00" as datetime), cast("2010-01-30" as date), "d", ' \
           '"tbuvpobzluhbwkljlhwnrkrhowybk", 949.57562, 976.26649, 1'
    common.check2(client, sql1=sql1, sql2=sql2)

    # 比较
    ret = client.update(table_name, ['v6=2'], 'k1=30')
    assert ret, 'update failed'
    sql2 = 'select 30, 300, 3000, 30000, cast("2040-01-01 00:00:00" as datetime), cast("2010-01-30" as date), "d", ' \
           '"tbuvpobzluhbwkljlhwnrkrhowybk", 949.57562, 976.26649, 2'
    common.check2(client, sql1=sql1, sql2=sql2)
    ret = client.update(table_name, ['v6=3'], 'k1 > 30')
    assert ret, 'update failed'
    sql2 = 'select 30, 300, 3000, 30000, cast("2040-01-01 00:00:00" as datetime), cast("2010-01-30" as date), "d", ' \
           '"tbuvpobzluhbwkljlhwnrkrhowybk", 949.57562, 976.26649, 2'
    common.check2(client, sql1=sql1, sql2=sql2)
    ret = client.update(table_name, ['v6=3'], 'k1<=20')
    assert ret, 'update failed'
    sql2 = 'select 30, 300, 3000, 30000, cast("2040-01-01 00:00:00" as datetime), cast("2010-01-30" as date), "d", ' \
           '"tbuvpobzluhbwkljlhwnrkrhowybk", 949.57562, 976.26649, 2'
    common.check2(client, sql1=sql1, sql2=sql2)

    # and
    ret = client.update(table_name, ['v6=3'], 'k1 >= 30 and k2 < 300')
    assert ret, 'update failed'
    sql2 = 'select 30, 300, 3000, 30000, cast("2040-01-01 00:00:00" as datetime), cast("2010-01-30" as date), "d", ' \
           '"tbuvpobzluhbwkljlhwnrkrhowybk", 949.57562, 976.26649, 2'
    common.check2(client, sql1=sql1, sql2=sql2)

    ret = client.update(table_name, ['v6=3'], 'k1 in (3, 30) and k5="2040-01-01 00:00:00"')
    assert ret, 'update failed'
    sql2 = 'select 30, 300, 3000, 30000, cast("2040-01-01 00:00:00" as datetime), cast("2010-01-30" as date), "d", ' \
           '"tbuvpobzluhbwkljlhwnrkrhowybk", 949.57562, 976.26649, 3'
    # or
    ret = client.update(table_name, ['v6=4'], 'k1 >= 30 or k2 < 300')
    assert ret, 'update failed'
    sql2 = 'select 30, 300, 3000, 30000, cast("2040-01-01 00:00:00" as datetime), cast("2010-01-30" as date), "d", ' \
           '"tbuvpobzluhbwkljlhwnrkrhowybk", 949.57562, 976.26649, 4'
    common.check2(client, sql1=sql1, sql2=sql2)
    # is not null
    ret = client.update(table_name, ['v6=5'], 'v6 is not null')
    assert ret, 'update failed'
    sql2 = 'select 30, 300, 3000, 30000, cast("2040-01-01 00:00:00" as datetime), cast("2010-01-30" as date), "d", ' \
           '"tbuvpobzluhbwkljlhwnrkrhowybk", 949.57562, 976.26649, 5'
    common.check2(client, sql1=sql1, sql2=sql2)
    ret = client.update(table_name, ['v6=6'], 'v6 is null')
    assert ret, 'update failed'
    sql2 = 'select 30, 300, 3000, 30000, cast("2040-01-01 00:00:00" as datetime), cast("2010-01-30" as date), "d", ' \
           '"tbuvpobzluhbwkljlhwnrkrhowybk", 949.57562, 976.26649, 5'
    common.check2(client, sql1=sql1, sql2=sql2)
    # function
    ret = client.update(table_name, ['v6=6'], 'abs(v5) > 900')
    assert ret, 'update failed'
    sql2 = 'select 30, 300, 3000, 30000, cast("2040-01-01 00:00:00" as datetime), cast("2010-01-30" as date), "d", ' \
           '"tbuvpobzluhbwkljlhwnrkrhowybk", 949.57562, 976.26649, 6'
    common.check2(client, sql1=sql1, sql2=sql2)
    # 常量表达式
    ret = client.update(table_name, ['v6=7'], '1=3')
    assert ret, 'update failed'
    sql2 = 'select 30, 300, 3000, 30000, cast("2040-01-01 00:00:00" as datetime), cast("2010-01-30" as date), "d", ' \
           '"tbuvpobzluhbwkljlhwnrkrhowybk", 949.57562, 976.26649, 6'
    common.check2(client, sql1=sql1, sql2=sql2)
    ret = client.update(table_name, ['v6=7'], '1=1')
    assert ret, 'update failed'
    sql2 = 'select 30, 300, 3000, 30000, cast("2040-01-01 00:00:00" as datetime), cast("2010-01-30" as date), "d", ' \
           '"tbuvpobzluhbwkljlhwnrkrhowybk", 949.57562, 976.26649, 7'
    common.check2(client, sql1=sql1, sql2=sql2)
    ret = client.update(table_name, ['v6=8'], 'true')
    assert ret, 'update failed'
    sql2 = 'select 30, 300, 3000, 30000, cast("2040-01-01 00:00:00" as datetime), cast("2010-01-30" as date), "d", ' \
           '"tbuvpobzluhbwkljlhwnrkrhowybk", 949.57562, 976.26649, 8'
    common.check2(client, sql1=sql1, sql2=sql2)
    ret = client.update(table_name, ['v6=9'], 'null is not null')
    assert ret, 'update failed'
    sql2 = 'select 30, 300, 3000, 30000, cast("2040-01-01 00:00:00" as datetime), cast("2010-01-30" as date), "d", ' \
           '"tbuvpobzluhbwkljlhwnrkrhowybk", 949.57562, 976.26649, 8'
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_update_set():
    """
    {
    "title": "test_update_set",
    "describe": "update中的set设置，包含自增，函数，表达式等，",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    partition_info = palo_client.PartitionInfo("k2",
                                               ["p1", "p2", "p3", "p4", "p5"],
                                               ["0", "100", "200", "300", "MAXVALUE"])
    distribution_info = palo_client.DistributionInfo('hash(k1)', 10)
    keys_desc = "UNIQUE KEY(k1, k2, k3, k4, k5)"
    ret = client.create_table(table_name, DATA.partition_column_no_agg_list, keys_desc=keys_desc,
                              partition_info=partition_info, distribution_info=distribution_info)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.partition_local_file, partition_list=["p5"], max_filter_ratio=1)
    assert ret, 'load data failed'
    sql1 = 'select * from %s' % table_name
    sql2 = 'select 30, 300, 3000, 30000, cast("2040-01-01 00:00:00" as datetime), cast("2010-01-30" as date), "d", ' \
           '"tbuvpobzluhbwkljlhwnrkrhowybk", 949.57562, 976.26649, 2579.668636'
    common.check2(client, sql1=sql1, sql2=sql2)
    common.check2(client, sql1=sql1, sql2=sql2)
    # set计算表达式
    ret = client.update(table_name, 'v6 = v6 + v5', 'k1=30')
    assert ret, 'update failed'
    sql2 = 'select 30, 300, 3000, 30000, cast("2040-01-01 00:00:00" as datetime), cast("2010-01-30" as date), "d", ' \
           '"tbuvpobzluhbwkljlhwnrkrhowybk", 949.57562, 976.26649, 3555.935126'
    common.check2(client, sql1=sql1, sql2=sql2)
    # set自增
    ret = client.update(table_name, 'v5 = v5 + 1', 'k1=30')
    assert ret, 'update failed'
    sql2 = 'select 30, 300, 3000, 30000, cast("2040-01-01 00:00:00" as datetime), cast("2010-01-30" as date), "d", ' \
           '"tbuvpobzluhbwkljlhwnrkrhowybk", 949.57562, 977.26649, 3555.935126'
    common.check2(client, sql1=sql1, sql2=sql2)
    # set自增 & 函数
    ret = client.update(table_name, 'v1 = date_add(v1, interval 2 day)', 'k1=30')
    assert ret, 'update failed'
    sql2 = 'select 30, 300, 3000, 30000, cast("2040-01-01 00:00:00" as datetime), cast("2010-02-1" as date), "d", ' \
           '"tbuvpobzluhbwkljlhwnrkrhowybk", 949.57562, 977.26649, 3555.935126'
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_update_multi_table():
    """
    {
    "title": "test_update_multi_table",
    "describe": "同时对不同表执行update操作，update操作均可执行成功",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    # init table
    tb1 = table_name + '_1'
    tb2 = table_name + '_2'
    ret = client.create_table(tb1, DATA.double_column_no_agg_list, keys_desc=DATA.unique_key)
    assert ret, 'create table failed'
    sql = 'insert into %s values(1, 2, 3, 4, 5)' % tb1
    ret = client.execute(sql)
    assert ret == (), 'insert date failed'
    ret = client.create_table(tb2, DATA.double_column_no_agg_list, keys_desc=DATA.unique_key)
    assert ret, 'create table failed'
    sql = 'insert into %s values(1, 2, 3, 4, 5)' % tb2
    ret = client.execute(sql)
    assert ret == (), 'insert date failed'
    # update
    client1 = common.get_client()
    client2 = common.get_client()
    update_task_1 = palo_task.SyncTask(client1.update, tb1, ['v1=v1+1'], 'k1=1', database_name)
    update_thread_1 = palo_task.TaskThread(update_task_1)
    update_thread_1.start()
    update_task_2 = palo_task.SyncTask(client2.update, tb2, ['v2=v2+1'], 'k1=1', database_name)
    update_thread_2 = palo_task.TaskThread(update_task_2)
    update_thread_2.start()
    time.sleep(10)
    update_thread_1.stop()
    update_thread_1.join()
    update_thread_2.stop()
    update_thread_2.join()
    print(update_task_1.succ_count)
    print(update_task_2.succ_count)
    sql1 = 'select * from %s' % tb1
    sql2 = 'select 1, 2 + %s, 3, 4, 5' % update_task_1.succ_count
    common.check2(client, sql1=sql1, sql2=sql2)
    sql1 = 'select * from %s' % tb2
    sql2 = 'select 1, 2, 3 + %s, 4, 5' % update_task_2.succ_count
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_concurrent_update_table_1():
    """
    {
    "title": "test_update_multi_table_1",
    "describe": "enable_concurrent_update=false，对同一个表执行update操作，偶发报错：There is an update operation in progress for the current table. Please try again later, or set enable_concurrent_update in fe.conf to true",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    if client.user in ('root', 'admin'):
        ret = client.admin_show_config('enable_concurrent_update')
        fe_config = palo_job.AdminShowConfig(ret[0])
        if fe_config.get_value() == 'true':
            raise pytest.skip("need to set fe config: enable_concurrent_update=false")
    # init table
    ret = client.create_table(table_name, DATA.double_column_no_agg_list, keys_desc=DATA.unique_key)
    assert ret, 'create table failed'
    sql = 'insert into %s values(1, 2, 3, 4, 5)' % table_name
    ret = client.execute(sql)
    assert ret == (), 'insert date failed'
    print(client.select_all(table_name))
    # update
    client1 = common.get_client()
    client2 = common.get_client()
    update_task_1 = palo_task.SyncTask(client1.update, table_name, ['v1=v1+1'], 'k1=1', database_name)
    update_thread_1 = palo_task.TaskThread(update_task_1)
    update_thread_1.start()
    update_task_2 = palo_task.SyncTask(client2.update, table_name, ['v2=v2+1'], 'k1=1', database_name)
    update_thread_2 = palo_task.TaskThread(update_task_2)
    update_thread_2.start()
    time.sleep(10)
    update_thread_1.stop()
    update_thread_1.join()
    time.sleep(1)
    update_thread_2.stop()
    update_thread_2.join()
    time.sleep(1)
    LOG.info(L('there are %s update v1=v1+1 succ' % update_task_1.succ_count))
    LOG.info(L('there are %s update v2=v2+1 succ' % update_task_2.succ_count))
    sql1 = 'select * from %s' % table_name
    sql2 = 'select 1, 2 + %s, 3 + %s, 4, 5' % (update_task_1.succ_count, update_task_2.succ_count)
    #common.check2(client, sql1=sql1, sql2=sql2)
    # msg = 'There is an update operation in progress for the current table. Please try again later, or set enable_concurrent_update in fe.conf to true'


def test_concurrent_update_table_2():
    """
    {
    "title": "test_update_multi_table_2",
    "describe": "enable_concurrent_update=fasle，对同一个表执行update操作，update的数据不冲突，偶发报错：There is an update operation in progress for the current table. Please try again later, or set enable_concurrent_update in fe.conf to true",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    if client.user in ('root', 'admin'):
        ret = client.admin_show_config('enable_concurrent_update')
        fe_config = palo_job.AdminShowConfig(ret[0])
        if fe_config.get_value() == 'true':
            raise pytest.skip("need to set fe config: enable_concurrent_update=false")
    # init table
    ret = client.create_table(table_name, DATA.double_column_no_agg_list, keys_desc=DATA.unique_key)
    assert ret, 'create table failed'
    sql = 'insert into %s values(1, 2, 3, 4, 5), (2, 3, 4, 5, 6)' % table_name
    ret = client.execute(sql)
    assert ret == (), 'insert date failed'
    # update
    client1 = common.get_client()
    client2 = common.get_client()
    update_task_1 = palo_task.SyncTask(client1.update, table_name, ['v1=v1+1'], 'k1=1', database_name)
    update_thread_1 = palo_task.TaskThread(update_task_1)
    update_thread_1.start()
    update_task_2 = palo_task.SyncTask(client2.update, table_name, ['v2=v2+1'], 'k1=2', database_name)
    update_thread_2 = palo_task.TaskThread(update_task_2)
    update_thread_2.start()
    time.sleep(10)
    update_thread_1.stop()
    update_thread_1.join()
    update_thread_2.stop()
    update_thread_2.join()
    LOG.info(L('there are %s update k1=1 succ' % update_task_1.succ_count))
    LOG.info(L('there are %s update k1=2 succ' % update_task_2.succ_count))
    sql1 = 'select * from %s' % table_name
    sql2 = 'select 1, 2 + %s, 3, 4, 5 union select 2, 3, 4 + %s, 5, 6' \
           % (update_task_1.succ_count, update_task_2.succ_count)
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    client.clean(database_name)


def test_concurrent_update_table_3():
    """
    {
    "title": "test_update_multi_table_2",
    "describe": "enable_concurrent_update=true，对同一个表执行update操作，update的数据不冲突"
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    if client.user in ('root', 'admin'):
        ret = client.admin_show_config('enable_concurrent_update')
        fe_config = palo_job.AdminShowConfig(ret[0])
        if fe_config.get_value() == 'false':
            raise pytest.skip("need to set fe config: enable_concurrent_update=true")
    # init table
    ret = client.create_table(table_name, DATA.double_column_no_agg_list, keys_desc=DATA.unique_key)
    assert ret, 'create table failed'
    sql = 'insert into %s values(1, 2, 3, 4, 5), (2,3,4,5,6)' % table_name
    ret = client.execute(sql)
    assert ret == (), 'insert date failed'
    # update
    client1 = common.get_client()
    client2 = common.get_client()
    update_task_1 = palo_task.SyncTask(client1.update, table_name, ['v1=v1+1'], 'k1=1', database_name)
    update_thread_1 = palo_task.TaskThread(update_task_1)
    update_thread_1.start()
    update_task_2 = palo_task.SyncTask(client2.update, table_name, ['v2=v2+1'], 'k1=2', database_name)
    update_thread_2 = palo_task.TaskThread(update_task_2)
    update_thread_2.start()
    time.sleep(10)
    update_thread_1.stop()
    update_thread_1.join()
    update_thread_2.stop()
    update_thread_2.join()
    LOG.info(L('there are %s update k1=1 succ' % update_task_1.succ_count))
    LOG.info(L('there are %s update k1=2 succ' % update_task_2.succ_count))
    sql1 = 'select * from %s' % table_name
    sql2 = 'select 1, 2 + %s, 3, 4, 5 union select 2, 3, 4 + %s, 5, 6' \
           % (update_task_1.succ_count, update_task_2.succ_count)
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    client.clean(database_name)


def test_concurrent_update_table_4():
    """
    {
    "title": "test_update_multi_table_4",
    "describe": "enable_concurrent_update=true，对同一个表执行update操作，update的数据冲突"
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    if client.user in ('root', 'admin'):
        ret = client.admin_show_config('enable_concurrent_update')
        fe_config = palo_job.AdminShowConfig(ret[0])
        if fe_config.get_value() == 'false':
            raise pytest.skip("need to set fe config: enable_concurrent_update=true")
    # init table
    ret = client.create_table(table_name, DATA.double_column_no_agg_list, keys_desc=DATA.unique_key)
    assert ret, 'create table failed'
    sql = 'insert into %s values(1, 2, 3, 4, 5)' % table_name
    ret = client.execute(sql)
    assert ret == (), 'insert date failed'
    print(client.select_all(table_name))
    # update
    client1 = common.get_client()
    client2 = common.get_client()
    update_task_1 = palo_task.SyncTask(client1.update, table_name, ['v1=v1+1'], 'k1=1', database_name)
    update_thread_1 = palo_task.TaskThread(update_task_1)
    update_thread_1.start()
    update_task_2 = palo_task.SyncTask(client2.update, table_name, ['v2=v2+1'], 'k1=1', database_name)
    update_thread_2 = palo_task.TaskThread(update_task_2)
    update_thread_2.start()
    time.sleep(10)
    update_thread_1.stop()
    update_thread_1.join()
    time.sleep(1)
    update_thread_2.stop()
    update_thread_2.join()
    time.sleep(1)
    LOG.info(L('there are %s update v1=v1+1 succ' % update_task_1.succ_count))
    LOG.info(L('there are %s update v2=v2+1 succ' % update_task_2.succ_count))
    sql1 = 'select * from %s' % table_name
    sql2 = 'select 1, 2 + %s, 3 + %s, 4, 5' % (update_task_1.succ_count, update_task_2.succ_count)
    # common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    client.clean(database_name)


def test_update_table_with_idx():
    """
    {
    "title": "test_update_multi_table_2",
    "describe": "update带有索引的表，bloom filter，bitmap Index",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    partition_info = palo_client.PartitionInfo("k2",
                                               ["p1", "p2", "p3", "p4", "p5"],
                                               ["0", "100", "200", "300", "MAXVALUE"])
    distribution_info = palo_client.DistributionInfo('hash(k1)', 10)
    keys_desc = "UNIQUE KEY(k1, k2, k3)"
    ret = client.create_table(table_name, DATA.partition_column_no_agg_list, keys_desc=keys_desc,
                              partition_info=partition_info, distribution_info=distribution_info)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.partition_local_file)
    assert ret, 'load data failed'
    sql1 = 'select k1, k2, k3, k4, k5, v1, "-", v3, v4, v5, 0.01 from %s order by k1' % table_name
    ret1 = client.execute(sql1)
    sql2 = 'select k1, k2, k3, 0, k5, v1, "-", v3, v4, v5, 0.01 from %s order by k1' % table_name
    ret2 = client.execute(sql2)

    ret = client.schema_change(table_name, bloom_filter_column_list=['v2', 'v6'], is_wait=True)
    assert ret, 'alter add bloom filter failed'
    ret = client.update(table_name, ['v2="-"', 'v6=0.01'], 'k1 > 0')
    assert ret, 'update failed'
    ret1_sut = client.select_all(table_name)
    util.check(ret1, ret1_sut, True)
    time.sleep(10)
    ret = client.create_bitmap_index_table(table_name, index_name, 'k4', is_wait=True)
    assert ret
    ret = client.update(table_name, 'k4=0', 'k1 > 0')
    ret2_sut = client.select_all(table_name)
    util.check(ret2, ret2_sut, True)
    client.clean(database_name)


def test_update_table_with_batch_delete():
    """
    {
    "title": "test_update_table_with_batch_delete",
    "describe": "update开启batch delte的表 update成功",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    partition_info = palo_client.PartitionInfo("k2",
                                               ["p1", "p2", "p3", "p4", "p5"],
                                               ["0", "100", "200", "300", "MAXVALUE"])
    distribution_info = palo_client.DistributionInfo('hash(k1)', 10)
    keys_desc = "UNIQUE KEY(k1, k2, k3)"
    ret = client.create_table(table_name, DATA.partition_column_no_agg_list, keys_desc=keys_desc,
                              partition_info=partition_info, distribution_info=distribution_info)
    assert ret, 'create table failed'
    try:
        client.enable_feature_batch_delete(table_name)
    except Exception as e:
        pass
    ret = client.stream_load(table_name, FILE.partition_local_file)
    assert ret, 'stream load failed'
    sql1 = 'select k1, k2, k3, k4, k5, v1, "-", v3, v4, v5, 0.01 from %s order by k1' % table_name
    ret1 = client.execute(sql1)
    ret = client.update(table_name, ['v2="-"', 'v6=0.01'], 'k1 > 0')
    assert ret, 'update failed'
    ret1_sut = client.select_all(table_name)
    util.check(ret1, ret1_sut, True)
    client.clean(database_name)


def test_update_table_while_load():
    """
    {
    "title": "test_update_multi_table_2",
    "describe": "在update的同时，导入数据",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    partition_info = palo_client.PartitionInfo("k2",
                                               ["p1", "p2", "p3", "p4", "p5"],
                                               ["0", "100", "200", "300", "MAXVALUE"])
    distribution_info = palo_client.DistributionInfo('hash(k1)', 10)
    keys_desc = "UNIQUE KEY(k1, k2, k3)"
    ret = client.create_table(table_name, DATA.partition_column_no_agg_list, keys_desc=keys_desc,
                              partition_info=partition_info, distribution_info=distribution_info)
    assert ret, 'create table failed'
    client1 = common.get_client()
    update_task = palo_task.SyncTask(client1.update, table_name, ['v2="0"'], 'k1 > 0', database_name)
    update_thread = palo_task.TaskThread(update_task)
    update_thread.start()
    time.sleep(1)
    ret = client.stream_load(table_name, FILE.partition_local_file)
    assert ret, 'stream load failed'
    update_thread.stop()
    update_thread.join()
    ret = client.select_all(table_name)
    assert len(ret) == 30, 'check data error'
    client.clean(database_name)


def test_update_table_while_delete():
    """
    {
    "title": "test_update_multi_table_2",
    "describe": "在update的同时，删除数据，偶发：doris-1130",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    distribution_info = palo_client.DistributionInfo('hash(k1)', 10)
    keys_desc = "UNIQUE KEY(k1, k2, k3)"
    ret = client.create_table(table_name, DATA.partition_column_no_agg_list, keys_desc=keys_desc,
                              distribution_info=distribution_info)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.partition_local_file)
    assert ret, 'stream load failed'
    client1 = common.get_client()
    update_task = palo_task.SyncTask(client1.update, table_name, ['v2="0"'], 'k1 > 0', database_name)
    update_task.interval = 0
    update_thread = palo_task.TaskThread(update_task)
    update_thread.start()
    ret = client.delete(table_name, [("k1", ">", "20")])
    assert ret, 'delete failed'
    time.sleep(1)
    update_thread.stop()
    update_thread.join()
    ret = client.select_all(table_name)
    print(len(ret))
    # assert len(ret) == 20, 'check data error, there are %s rows' % len(ret)
    client.clean(database_name)


def test_update_table_while_add_column():
    """
    {
    "title": "test_update_table_while_add_column",
    "describe": "在update的同时，加列",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    partition_info = palo_client.PartitionInfo("k2",
                                               ["p1", "p2", "p3", "p4", "p5"],
                                               ["0", "100", "200", "300", "MAXVALUE"])
    distribution_info = palo_client.DistributionInfo('hash(k1)', 10)
    keys_desc = "UNIQUE KEY(k1, k2, k3)"
    ret = client.create_table(table_name, DATA.partition_column_no_agg_list, keys_desc=keys_desc,
                              partition_info=partition_info, distribution_info=distribution_info)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.partition_local_file)
    assert ret, 'stream load failed'
    client1 = common.get_client()
    update_task = palo_task.SyncTask(client1.update, table_name, ['v2="0"'], 'k1 > 0', database_name)
    update_thread = palo_task.TaskThread(update_task)
    update_thread.start()
    # add column
    ret = client.schema_change_add_column(table_name, [('v_add', 'INT', '', '0')], is_wait_job=True)
    assert ret, 'add column failed'
    update_thread.stop()
    update_thread.join()
    ret = client.select_all(table_name)
    client.clean(database_name)


def test_update_table_while_drop_column():
    """
    {
    "title": "test_update_table_while_drop_column",
    "describe": "在update的同时，减列",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    partition_info = palo_client.PartitionInfo("k2",
                                               ["p1", "p2", "p3", "p4", "p5"],
                                               ["0", "100", "200", "300", "MAXVALUE"])
    distribution_info = palo_client.DistributionInfo('hash(k1)', 10)
    keys_desc = "UNIQUE KEY(k1, k2, k3)"
    ret = client.create_table(table_name, DATA.partition_column_no_agg_list, keys_desc=keys_desc,
                              partition_info=partition_info, distribution_info=distribution_info)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.partition_local_file)
    assert ret, 'stream load failed'
    sql1 = 'select k1, k2, k3, k4, k5, v1, "0", v3, v4, v5 from %s order by k1' % table_name
    ret1 = client.execute(sql1)

    client1 = common.get_client()
    update_task = palo_task.SyncTask(client1.update, table_name, ['v2="0"'], 'k1 > 0', database_name)
    update_thread = palo_task.TaskThread(update_task)
    update_thread.start()
    # add column
    ret = client.schema_change_drop_column(table_name, ['v6'], is_wait_job=True)
    assert ret, 'add column failed'
    update_thread.stop()
    update_thread.join()
    ret = client.select_all(table_name)
    util.check(ret, ret1, True)
    client.clean(database_name)


def test_update_table_while_modify_column():
    """
    {
    "title": "test_update_table_while_modify_column",
    "describe": "在update的同时，修改列",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    partition_info = palo_client.PartitionInfo("k2",
                                               ["p1", "p2", "p3", "p4", "p5"],
                                               ["0", "100", "200", "300", "MAXVALUE"])
    distribution_info = palo_client.DistributionInfo('hash(k1)', 10)
    keys_desc = "UNIQUE KEY(k1, k2, k3)"
    ret = client.create_table(table_name, DATA.partition_column_no_agg_list, keys_desc=keys_desc,
                              partition_info=partition_info, distribution_info=distribution_info)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.partition_local_file)
    assert ret, 'stream load failed'
    client1 = common.get_client()
    update_task = palo_task.SyncTask(client1.update, table_name, ['v2="0"'], 'k1 > 0', database_name)
    update_thread = palo_task.TaskThread(update_task)
    update_thread.start()
    # modify column
    ret = client.schema_change_modify_column(table_name, 'v2', 'char(5)', is_wait_job=True, aggtype='-')
    assert ret, 'add column failed'
    update_thread.stop()
    update_thread.join()
    ret = client.select_all(table_name)
    client.clean(database_name)


def test_update_table_while_rollup():
    """
    {
    "title": "test_update_table_while_rollup",
    "describe": "在update的同时，创建rollup",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    partition_info = palo_client.PartitionInfo("k2",
                                               ["p1", "p2", "p3", "p4", "p5"],
                                               ["0", "100", "200", "300", "MAXVALUE"])
    distribution_info = palo_client.DistributionInfo('hash(k1)', 10)
    keys_desc = "UNIQUE KEY(k1, k2, k3)"
    ret = client.create_table(table_name, DATA.partition_column_no_agg_list, keys_desc=keys_desc,
                              partition_info=partition_info, distribution_info=distribution_info)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.partition_local_file)
    assert ret, 'stream load failed'
    client1 = common.get_client()
    update_task = palo_task.SyncTask(client1.update, table_name, ['v2="0"'], 'k1 > 0', database_name)
    update_thread = palo_task.TaskThread(update_task)
    update_thread.start()
    # create rollup
    ret = client.create_rollup_table(table_name, index_name, ['k1', 'k3', 'k2', 'v2', 'v6'], is_wait=True)
    assert ret, 'create rollup failed'
    update_thread.stop()
    update_thread.join()
    ret = client.select_all(table_name)
    client.clean(database_name)


if __name__ == '__main__':
    setup_module()
    #test_update_condition()
    test_update_table_while_add_column()
