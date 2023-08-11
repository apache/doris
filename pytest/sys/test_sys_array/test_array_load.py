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
# BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, DECIMAL, DATE, DATETIME, CHAR, VARCHAR, STRING
# 4种导入方式 insert/stream/broker/routine load
# 4种导入格式 csv，json，parquet，orc
# json导入参数
# max_filter_ratio
# set column
#############################################################################
"""
test_array_load.py
array routine load case in  test_routine_load_property.py test_array_load
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
from lib import palo_types

config = palo_config.config
broker_info = palo_client.BrokerInfo(config.broker_name, config.broker_property)
# BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, DECIMAL, DATE, DATETIME, CHAR, VARCHAR, STRING
# 4种导入方式 insert/stream/broker/routine load
# 4种导入格式 csv，json，parquet，orc
# json导入参数
# max_filter_ratio
# set column


def setup_module():
    """enable array"""
    client = common.get_client()
    ret = client.show_variables('enable_vectorized_engine')
    if len(ret) == 1 and ret[0][1] == 'false':
        raise pytest.skip('skip if enable_vectorized_engine is false')

    ret = client.admin_show_config('enable_array_type')
    assert len(ret) == 1, 'get enable_array_type config error'
    value = palo_job.AdminShowConfig(ret[0]).get_value()
    if value != 'true':
        client.set_frontend_config('enable_array_type', 'true')
    print(len(client.get_alive_backend_list()))


def teardown_module():
    """tearDown"""
    client = common.get_client()
    print(len(client.get_alive_backend_list()))


def load_check(db, tb, schema, local_file, hdfs_file, expect_file, **kwargs):
    """
    1. create db
    2. create tb & stream load, skip if no local file
    3. brokerte tb &  load, skip if no broker file
    4. check load result with expect file
    5. drop db
    **kwargs pass to stream load & broker load
    """
    client = common.create_workspace(db)
    stream_tb = tb + '_stream'
    broker_tb = tb + '_broker'
    k2_type = util.get_attr_condition_value(schema, 0, 'k2', 1)
    be_checked_table = list()
    # stream load
    if local_file is not None:
        ret = client.create_table(stream_tb, schema, keys_desc=SCHEMA.duplicate_key, set_null=True)
        assert ret, 'create table failed'
        ret = client.stream_load(stream_tb, local_file, max_filter_ratio=0.01, **kwargs)
        assert ret, 'stream load failed'
        be_checked_table.append(stream_tb)
        time.sleep(5)
    # broker load
    if hdfs_file is not None:
        ret = client.create_table(broker_tb, schema, keys_desc=SCHEMA.duplicate_key, set_null=True)
        assert ret, 'create table failed'
        load_data_info = palo_client.LoadDataInfo(hdfs_file, broker_tb, **kwargs)
        ret = client.batch_load(util.get_label(), load_data_info, broker=broker_info, 
                                max_filter_ratio=0.01, is_wait=True)
        assert ret, 'broker load failed'
        be_checked_table.append(broker_tb)
    # check
    if local_file and hdfs_file:
        sql1 = 'select * from %s order by k1' % stream_tb
        sql2 = 'select * from %s order by k1' % broker_tb
        common.check2(client, sql1, sql2=sql2)
    for tb in be_checked_table:
        if 'DOUBLE' in k2_type.upper() or 'FLOAT' in k2_type.upper():
            common.check_by_file(expect_file, table_name=tb, client=client, k2=palo_types.ARRAY_DOUBLE)
        elif 'DECIMAL' in k2_type.upper():
            common.check_by_file(expect_file, table_name=tb, client=client, k2=palo_types.ARRAY_DECIMAL)
        else:
            common.check_by_file(expect_file, table_name=tb, client=client)
    client.clean(db)


def test_array_boolean():
    """
    {
    "title": "test_array_boolean",
    "describe": "array类型中，子类型为boolean，stream、broker导入csv，导入成功，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_boolean_list
    local_file = FILE.test_array_boolean_local_file
    hdfs_file = FILE.test_array_boolean_remote_file
    expect_file = FILE.expe_array_boolean_file
    load_check(database_name, table_name, column_list, local_file, hdfs_file, expect_file)


def test_array_boolean_json():
    """
    {
    "title": "test_array_boolean_json",
    "describe": "array类型中，子类型为boolean，stream导入json，导入成功，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_boolean_list
    local_file = FILE.test_array_boolean_local_json
    expect_file = FILE.expe_array_boolean_file
    load_check(database_name, table_name, column_list, local_file, None, expect_file, 
               format='json', strip_outer_array=True)


def test_array_boolean_insert():
    """
    {
    "title": "test_array_boolean_insert",
    "describe": "array类型中，子类型为boolean，insert导入，导入成功，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_boolean_list
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, column_list, keys_desc=SCHEMA.duplicate_key, set_null=True)
    assert ret, 'create table failed'
    # insert array data
    sql_list = list()
    sql_list.append("insert into %s values (1000, ['1', '0'])" % table_name)
    sql_list.append("insert into %s values (1001, ['true', 'false'])" % table_name)
    sql_list.append("insert into %s values (1002, [2])" % table_name)
    sql_list.append("insert into %s values (1003, [NULL])" % table_name)
    sql_list.append("insert into %s values (1004, NULL)" % table_name)
    sql_list.append("insert into %s values (1005, ['Abc'])" % table_name)
    sql_list.append("insert into %s values (1006, [1.234])" % table_name)
    sql_list.append("insert into %s values (1007, '1,2,3')" % table_name)
    sql_list.append("insert into %s values (1008, '[1,2,3]')" % table_name)
    for i in sql_list:
        client.execute(i)
    sql = 'select * from %s order by k1' % table_name
    ret = client.execute(sql)
    expect = ((1000, u'[1, 0]'), (1001, u'[1, 0]'), (1002, u'[1]'),
              (1003, u'[NULL]'), (1004, None), (1005, u'[NULL]'),
              (1006, u'[1]'), (1007, None), (1008, u'[1, NULL, NULL]'))
    util.check(ret, expect)
    client.clean(database_name)


def test_array_tinyint():
    """
    {
    "title": "test_array_tinyint",
    "describe": "array类型中，子类型为tinyint，stream、broker导入csv，导入成功，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_tinyint_list
    local_file = FILE.test_array_tinyint_local_file
    hdfs_file = FILE.test_array_tinyint_remote_file
    expect_file = FILE.expe_array_tinyint_file
    load_check(database_name, table_name, column_list, local_file, hdfs_file, expect_file)


def test_array_tinyint_json():
    """
    {
    "title": "test_array_tinyint_json",
    "describe": "array类型中，子类型为tinyint，stream导入json，导入成功，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_tinyint_list
    local_file = FILE.test_array_tinyint_local_json
    expect_file = FILE.expe_array_tinyint_file
    load_check(database_name, table_name, column_list, local_file, None, expect_file,
               format='json', strip_outer_array=True)


def test_array_tinyint_insert():
    """
    {
    "title": "test_array_tinyint_insert",
    "describe": "array类型中，子类型为tinyint，insert导入，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_tinyint_list
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, column_list, keys_desc=SCHEMA.duplicate_key, set_null=True)
    assert ret, 'create table failed'
    sql_list = list()
    err_sql_list = list()
    sql_list.append("insert into %s values (1000, [])" % table_name)
    sql_list.append("insert into %s values (1001, [NULL])" % table_name)
    sql_list.append("insert into %s values (1002, [1, 0, -0])" % table_name)
    sql_list.append("insert into %s values (1003, [1.23, 2.34])" % table_name)
    sql_list.append("insert into %s values (1004, NULL)" % table_name)
    err_sql_list.append("insert into %s values (1005, [-129])" % table_name)
    err_sql_list.append("insert into %s values (1006, [128])" % table_name)
    sql_list.append("insert into %s values (1007, ['123', '23'])" % table_name)
    err_sql_list.append("insert into %s values (1008, ['ABC', '1'])" % table_name)
    sql_list.append("insert into %s values (1009, '1,2,3')" % table_name)
    sql_list.append("insert into %s values (1010, '[1,2,3]')" % table_name)
    for i in sql_list:
        client.execute(i)
    for sql in err_sql_list:
        util.assert_return(False, ' ', client.execute, sql)
    sql = 'select * from %s order by k1' % table_name
    ret = client.execute(sql)
    expect = ((1000, u'[]'), (1001, u'[NULL]'), (1002, u'[1, 0, 0]'), (1003, u'[1, 2]'),
              (1004, None),
              (1007, u'[123, 23]'), (1009, None), (1010, u'[1, 2, 3]'))
    util.check(ret, expect)
    client.clean(database_name)


def test_array_smallint():
    """
    {
    "title": "test_array_smallint",
    "describe": "array类型中，子类型为smallint，stream、broker导入csv，导入成功，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_smallint_list
    local_file = FILE.test_array_smallint_local_file
    hdfs_file = FILE.test_array_smallint_remote_file
    expect_file = FILE.expe_array_smallint_file
    load_check(database_name, table_name, column_list, local_file, hdfs_file, expect_file)


def test_array_smallint_json():
    """
    {
    "title": "test_array_smallint_json",
    "describe": "array类型中，子类型为smallint，stream导入json，导入成功，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_smallint_list
    local_file = FILE.test_array_smallint_local_json
    expect_file = FILE.expe_array_smallint_file
    load_check(database_name, table_name, column_list, local_file, None, expect_file,
               format='json', strip_outer_array=True)


def test_array_smallint_insert():
    """
    {
    "title": "test_array_smallint_insert",
    "describe": "array类型中，子类型为smallint，insert导入，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_smallint_list
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, column_list, keys_desc=SCHEMA.duplicate_key, set_null=True)
    assert ret, 'create table failed'
    # insert
    sql_list = list()
    err_sql_list = list()
    sql_list.append("insert into %s values (1000, [])" % table_name)
    sql_list.append("insert into %s values (1001, [NULL])" % table_name)
    sql_list.append("insert into %s values (1002, [1, 0, -0])" % table_name)
    sql_list.append("insert into %s values (1003, [1.23, 2.34])" % table_name)
    sql_list.append("insert into %s values (1004, NULL)" % table_name)
    err_sql_list.append("insert into %s values (1005, [-32769])" % table_name)
    err_sql_list.append("insert into %s values (1006, [32768])" % table_name)
    sql_list.append("insert into %s values (1007, ['123', '23'])" % table_name)
    err_sql_list.append("insert into %s values (1008, ['ABC', '1'])" % table_name)
    sql_list.append("insert into %s values (1009, '1,2,3')" % table_name)
    sql_list.append("insert into %s values (1010, '[1,2,3]')" % table_name)
    for i in sql_list:
        client.execute(i)
    for sql in err_sql_list:
        util.assert_return(False, ' ', client.execute, sql)
    sql = 'select * from %s order by k1' % table_name
    ret = client.execute(sql)
    expect = ((1000, u'[]'), (1001, u'[NULL]'), (1002, u'[1, 0, 0]'),
              (1003, u'[1, 2]'), (1004, None), (1007, u'[123, 23]'),
              (1009, None), (1010, u'[1, 2, 3]'))
    util.check(ret, expect)
    client.clean(database_name)


def test_array_int():
    """
    {
    "title": "test_array_int",
    "describe": "array类型中，子类型为int，stream、broker导入csv，导入成功，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_int_list
    local_file = FILE.test_array_int_local_file
    hdfs_file = FILE.test_array_int_remote_file
    expect_file = FILE.expe_array_int_file
    load_check(database_name, table_name, column_list, local_file, hdfs_file, expect_file)


def test_array_int_json():
    """
    {
    "title": "test_array_int_json",
    "describe": "array类型中，子类型为intt，stream导入json，导入成功，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_int_list
    local_file = FILE.test_array_int_local_json
    expect_file = FILE.expe_array_int_file
    load_check(database_name, table_name, column_list, local_file, None, expect_file,
               format='json', strip_outer_array=True)


def test_array_int_insert():
    """
    {
    "title": "test_array_int_insert",
    "describe": "array类型中，子类型为int，insert导入查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_int_list
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, column_list, keys_desc=SCHEMA.duplicate_key, set_null=True)
    assert ret, 'create table failed'
    # insert
    sql_list = list()
    err_sql_list = list()
    sql_list.append("insert into %s values (1000, [])" % table_name)
    sql_list.append("insert into %s values (1001, [NULL])" % table_name)
    sql_list.append("insert into %s values (1002, [1, 0, -0])" % table_name)
    sql_list.append("insert into %s values (1003, [1.23, 2.34])" % table_name)
    sql_list.append("insert into %s values (1004, NULL)" % table_name)
    err_sql_list.append("insert into %s values (1005, [-2147483649])" % table_name)
    err_sql_list.append("insert into %s values (1006, [2147483648])" % table_name)
    sql_list.append("insert into %s values (1007, ['123', '23'])" % table_name)
    err_sql_list.append("insert into %s values (1008, ['ABC', '1'])" % table_name)
    sql_list.append("insert into %s values (1009, '1,2,3')" % table_name)
    sql_list.append("insert into %s values (1010, '[1,2,3]')" % table_name)
    err_sql_list.append("insert into %s values (1005, [-9223372036854775809])" % table_name)
    for i in sql_list:
        client.execute(i)
    for sql in err_sql_list:
        util.assert_return(False, ' ', client.execute, sql)
    sql = 'select * from %s order by k1' % table_name
    ret = client.execute(sql)
    expect = ((1000, u'[]'), (1001, u'[NULL]'), (1002, u'[1, 0, 0]'),
              (1003, u'[1, 2]'), (1004, None), (1007, u'[123, 23]'),
              (1009, None), (1010, u'[1, 2, 3]'))
    util.check(ret, expect)
    client.clean(database_name)


def test_array_bigint():
    """
    {
    "title": "test_array_bigint",
    "describe": "array类型中，子类型为bigint，stream、broker导入csv，导入成功，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_bigint_list
    local_file = FILE.test_array_bigint_local_file
    hdfs_file = FILE.test_array_bigint_remote_file
    expect_file = FILE.expe_array_bigint_file
    load_check(database_name, table_name, column_list, local_file, hdfs_file, expect_file)


def test_array_bigint_json():
    """
    {
    "title": "test_array_bigint_json",
    "describe": "array类型中，子类型为bigint，stream导入json，导入成功，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_bigint_list
    local_file = FILE.test_array_bigint_local_json
    expect_file = FILE.expe_array_bigint_file
    load_check(database_name, table_name, column_list, local_file, None, expect_file, 
               format='json', strip_outer_array=True)


def test_array_bigint_insert():
    """
    {
    "title": "test_array_bigint_insert",
    "describe": "array类型中，子类型为bigint，insert导入，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_bigint_list
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, column_list, keys_desc=SCHEMA.duplicate_key, set_null=True)
    assert ret, 'create table failed'
    # insert
    sql_list = list()
    err_sql_list = list()
    sql_list.append("insert into %s values (1000, [])" % table_name)
    sql_list.append("insert into %s values (1001, [NULL])" % table_name)
    sql_list.append("insert into %s values (1002, ['1002', '1002'])" % table_name)
    err_sql_list.append("insert into %s values (1003, [1, 'abc'])" % table_name)
    sql_list.append("insert into %s values (1004, [1, 3.1415, -0])" % table_name)
    sql_list.append("insert into %s values (1005, NULL)" % table_name)
    sql_list.append("insert into %s values (1006, '1,2,3')" % table_name)
    err_sql_list.append("insert into %s values (1007, [-9223372036854775809])" % table_name)
    err_sql_list.append("insert into %s values (1008, [9223372036854775808])" % table_name)
    sql_list.append("insert into %s values (1009, [-9223372036854775808, 9223372036854775807])" % table_name)
    sql_list.append("insert into %s values (1010, '[1,2,3]')" % table_name)
    for i in sql_list:
        client.execute(i)
    for sql in err_sql_list:
        util.assert_return(False, ' ', client.execute, sql)
    sql = 'select * from %s order by k1' % table_name
    ret = client.execute(sql)
    expect = ((1000, u'[]'), (1001, u'[NULL]'), (1002, u'[1002, 1002]'),
              (1004, u'[1, 3, 0]'), (1005, None), (1006, None),
              (1009, u'[-9223372036854775808, 9223372036854775807]'), (1010, u'[1, 2, 3]'))
    util.check(ret, expect)
    client.clean(database_name)


def test_array_largeint():
    """
    {
    "title": "test_array_largeint_json",
    "describe": "array类型中，子类型为largeint，stream、broker导入csv，导入成功，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_largeint_list
    local_file = FILE.test_array_largeint_local_file
    hdfs_file = FILE.test_array_largeint_remote_file
    expect_file = FILE.expe_array_largeint_file
    load_check(database_name, table_name, column_list, local_file, hdfs_file, expect_file)


def test_array_largeint_json():
    """
    {
    "title": "test_array_largeint_json",
    "describe": "array类型中，子类型为boolean，stream导入json，导入成功，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_largeint_list
    local_file = FILE.test_array_largeint_local_json
    expect_file = FILE.expe_array_largeint_file
    load_check(database_name, table_name, column_list, local_file, None, expect_file, 
               format='json', strip_outer_array=True)


def test_array_largeint_json_num():
    """
    {
    "title": "test_array_largeint_json",
    "describe": "array类型中，子类型为largeint，stream导入json，largeint为数字非字符串，导入成功，结果为Null",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_largeint_list
    local_file = FILE.test_array_largeint_local_json_num
    expect_file = FILE.expe_array_largeint_json_num_file_null
    load_check(database_name, table_name, column_list, local_file, None, expect_file, format='json')


def test_array_largeint_json_num_as_string():
    """
    {
    "title": "test_array_largeint_json",
    "describe": "array类型中，子类型为largeint，stream导入json，设置num_as_string，largeint为数字非字符串，导入成功，结果为Null",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_largeint_list
    local_file = FILE.test_array_largeint_local_json_num
    expect_file = FILE.expe_array_largeint_json_num_file_real
    load_check(database_name, table_name, column_list, local_file, None, expect_file, 
               format='json', num_as_string='true')
    

def test_array_largeint_insert():
    """
    {
    "title": "test_array_largeint_insert",
    "describe": "array类型中，子类型为largeint，insert导入，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_largeint_list
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, column_list, keys_desc=SCHEMA.duplicate_key, set_null=True)
    assert ret, 'create table failed'
    # insert
    sql_list = list()
    err_sql_list = list()
    sql_list.append("insert into %s values (1000, [])" % table_name)
    sql_list.append("insert into %s values (1001, [NULL])" % table_name)
    sql_list.append("insert into %s values (1002, [1, 0, -0])" % table_name)
    sql_list.append("insert into %s values (1003, [1.23, 2.34])" % table_name)
    sql_list.append("insert into %s values (1004, NULL)" % table_name)
    err_sql_list.append("insert into %s values (1005, [-170141183460469231731687303715884105729])" % table_name)
    err_sql_list.append("insert into %s values (1006, [170141183460469231731687303715884105728])" % table_name)
    sql_list.append("insert into %s values (1007, ['123', '23'])" % table_name)
    err_sql_list.append("insert into %s values (1008, ['ABC', '1'])" % table_name)
    sql_list.append("insert into %s values (1009, '1,2,3')" % table_name)
    sql_list.append("insert into %s values (1010, '[1,2,3]')" % table_name)
    for i in sql_list:
        client.execute(i)
    for sql in err_sql_list:
        util.assert_return(False, ' ', client.execute, sql)
    sql = 'select * from %s order by k1' % table_name
    ret = client.execute(sql)
    expect = ((1000, u'[]'), (1001, u'[NULL]'), (1002, u'[1, 0, 0]'),
              (1003, u'[1, 2]'), (1004, None), (1007, u'[123, 23]'),
              (1009, None), (1010, u'[1, 2, 3]'))
    util.check(ret, expect)
    client.clean(database_name)


def test_array_decimal():
    """
    {
    "title": "test_array_decimal",
    "describe": "array类型中，子类型为decimal，stream、broker导入csv，导入成功，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_decimal_list
    local_file = FILE.test_array_decimal_local_file
    hdfs_file = FILE.test_array_decimal_remote_file
    expect_file = FILE.expe_array_decimal_file
    load_check(database_name, table_name, column_list, local_file, hdfs_file, expect_file)


def test_array_decimal_json():
    """
    {
    "title": "test_array_decimal_json",
    "describe": "array类型中，子类型为decimal，stream导入json，导入成功，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_decimal_list
    local_file = FILE.test_array_decimal_local_json
    expect_file = FILE.expe_array_decimal_file
    load_check(database_name, table_name, column_list, local_file, None, expect_file, 
               format='json', strip_outer_array=True)


def test_array_decimal_insert():
    """
    {
    "title": "test_array_decimal_insert",
    "describe": "array类型中，子类型为decimal，insert导入，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_decimal_list
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, column_list, keys_desc=SCHEMA.duplicate_key, set_null=True)
    assert ret, 'create table failed'
    # insert
    sql_list = list()
    err_sql_list = list()
    sql_list.append("insert into %s values (1000, [123, 3, 4])" % table_name)
    sql_list.append("insert into %s values (1001, [])" % table_name)
    sql_list.append("insert into %s values (1002, [NULL])" % table_name)
    sql_list.append("insert into %s values (1003, ['-1.234', '2.3456'])" % table_name)
    sql_list.append("insert into %s values (1004, [1.0000000001])" % table_name)
    sql_list.append("insert into %s values (1005, [10000000000000000000.1111111222222222])" % table_name)
    sql_list.append("insert into %s values (1006, '1,3,4')" % table_name)
    err_sql_list.append("insert into %s values (1007, ['ABCD'])" % table_name)
    sql_list.append("insert into %s values (1008, NULL)" % table_name)
    sql_list.append("insert into %s values (1009, [999999999999999999.9999999])" % table_name)
    sql_list.append("insert into %s values (1010, '[1,2,3]')" % table_name)
    for i in sql_list:
        client.execute(i)
    for sql in err_sql_list:
        util.assert_return(False, ' ', client.execute, sql)
    sql = 'select * from %s order by k1' % table_name
    ret = client.execute(sql)
    expect = ((1000, u'[123, 3, 4]'), (1001, u'[]'),
              (1002, u'[NULL]'), (1003, u'[-1.234, 2.3456]'),
              (1004, u'[1]'), (1005, u'[100000000000000000.001111111]'), # 1005 decimal is wrong, overflow
              (1006, None), (1008, None),
              (1009, u'[999999999999999999.9999999]'), (1010, u'[1, 2, 3]'))
    util.check(ret, expect)
    client.clean(database_name)


def test_array_float():
    """
    {
    "title": "test_array_float",
    "describe": "array类型中，子类型为float，stream、broker导入csv，导入成功，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_float_list
    local_file = FILE.test_array_float_local_file
    hdfs_file = FILE.test_array_float_remote_file
    expect_file = FILE.expe_array_float_file
    load_check(database_name, table_name, column_list, local_file, hdfs_file, expect_file)


def test_array_float_json():
    """
    {
    "title": "test_array_float_json",
    "describe": "array类型中，子类型为float，stream导入json，导入成功，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_float_list
    local_file = FILE.test_array_float_local_json
    expect_file = FILE.expe_array_float_file
    load_check(database_name, table_name, column_list, local_file, None, expect_file, 
               format='json', strip_outer_array=True)


def test_array_float_insert():
    """
    {
    "title": "test_array_float_insert",
    "describe": "array类型中，子类型为float，insert导入，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_float_list
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, column_list, keys_desc=SCHEMA.duplicate_key, set_null=True)
    assert ret, 'create table failed'
    # insert
    sql_list = list()
    err_sql_list = list()
    sql_list.append("insert into %s values (1000, [123, 3, 4])" % table_name)
    sql_list.append("insert into %s values (1001, [])" % table_name)
    sql_list.append("insert into %s values (1002, [NULL])" % table_name)
    sql_list.append("insert into %s values (1003, ['-1.234', '2.3456'])" % table_name)
    sql_list.append("insert into %s values (1004, [1.0000000001])" % table_name)
    sql_list.append("insert into %s values (1005, [10000000000000000000.1111111222222222])" % table_name)
    sql_list.append("insert into %s values (1006, '1,3,4')" % table_name)
    err_sql_list.append("insert into %s values (1007, ['ABCD'])" % table_name)
    sql_list.append("insert into %s values (1008, NULL)" % table_name)
    sql_list.append("insert into %s values (1009, [999999999999999999.9999999])" % table_name)
    sql_list.append("insert into %s values (1010, '[1,2,3]')" % table_name)
    for i in sql_list:
        client.execute(i)
    for sql in err_sql_list:
        util.assert_return(False, ' ', client.execute, sql)
    sql = 'select * from %s order by k1' % table_name
    ret = client.execute(sql)
    expect = ((1000, u'[123, 3, 4]'), (1001, u'[]'), (1002, u'[NULL]'),
              (1003, u'[-1.234, 2.3456]'), (1004, u'[1]'), (1005, u'[1e+19]'),
              (1006, None), (1008, None), (1009, u'[1e+18]'),
              (1010, u'[1, 2, 3]'))
    util.check(ret, expect)
    client.clean(database_name)


def test_array_double():
    """
    {
    "title": "test_array_double",
    "describe": "array类型中，子类型为double，stream、broker导入csv，导入成功，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_double_list
    local_file = FILE.test_array_double_local_file
    hdfs_file = FILE.test_array_double_remote_file
    expect_file = FILE.expe_array_double_file
    load_check(database_name, table_name, column_list, local_file, hdfs_file, expect_file)


def test_array_double_json():
    """
    {
    "title": "test_array_double_json",
    "describe": "array类型中，子类型为double，stream导入json，导入成功，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_double_list
    local_file = FILE.test_array_double_local_json
    expect_file = FILE.expe_array_double_file
    load_check(database_name, table_name, column_list, local_file, None, expect_file, 
               format='json', strip_outer_array=True)


def test_array_double_insert():
    """
    {
    "title": "test_array_double_insert",
    "describe": "array类型中，子类型为double，insert导入，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_double_list
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, column_list, keys_desc=SCHEMA.duplicate_key, set_null=True)
    assert ret, 'create table failed'
    # insert
    sql_list = list()
    err_sql_list = list()
    sql_list.append("insert into %s values (1000, [123, 3, 4])" % table_name)
    sql_list.append("insert into %s values (1001, [])" % table_name)
    sql_list.append("insert into %s values (1002, [NULL])" % table_name)
    sql_list.append("insert into %s values (1003, ['-1.234', '2.3456'])" % table_name)
    sql_list.append("insert into %s values (1004, [1.0000000001])" % table_name)
    sql_list.append("insert into %s values (1005, [10000000000000000000.1111111222222222])" % table_name)
    sql_list.append("insert into %s values (1006, '1,3,4')" % table_name)
    err_sql_list.append("insert into %s values (1007, ['ABCD'])" % table_name)
    sql_list.append("insert into %s values (1008, NULL)" % table_name)
    sql_list.append("insert into %s values (1009, [999999999999999999.9999999])" % table_name)
    sql_list.append("insert into %s values (1010, '[1,2,3]')" % table_name)
    for i in sql_list:
        client.execute(i)
    for sql in err_sql_list:
        util.assert_return(False, ' ', client.execute, sql)
    sql = 'select * from %s order by k1' % table_name
    ret = client.execute(sql)
    expect = ((1000, u'[123, 3, 4]'), (1001, u'[]'), (1002, u'[NULL]'),
              (1003, u'[-1.234, 2.3456]'), (1004, u'[1.0000000001]'),
              (1005, u'[1e+19]'), (1006, None), (1008, None),
              (1009, u'[1e+18]'), (1010, u'[1, 2, 3]'))
    util.check(ret, expect)
    client.clean(database_name)


def test_array_date():
    """
    {
    "title": "test_array_date",
    "describe": "array类型中，子类型为date，stream、broker导入csv，导入成功，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_date_list
    local_file = FILE.test_array_date_local_file
    hdfs_file = FILE.test_array_date_remote_file
    expect_file = FILE.expe_array_date_file
    load_check(database_name, table_name, column_list, local_file, hdfs_file, expect_file)


def test_array_date_json():
    """
    {
    "title": "test_array_date_json",
    "describe": "array类型中，子类型为date，stream导入json，导入成功，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_date_list
    local_file = FILE.test_array_date_local_json
    expect_file = FILE.expe_array_date_file
    load_check(database_name, table_name, column_list, local_file, None, expect_file,
               format='json', strip_outer_array=True)


def test_array_date_insert():
    """
    {
    "title": "test_array_date_insert",
    "describe": "array类型中，子类型为date，insert导入，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_date_list
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, column_list, keys_desc=SCHEMA.duplicate_key, set_null=True)
    assert ret, 'create table failed'
    # insert
    sql_list = list()
    err_sql_list =  list()
    sql_list.append("insert into %s values (1000, ['0001-01-01', '9999-12-31'])" % table_name)
    sql_list.append("insert into %s values (1001, ['10000-01-01'])" % table_name)
    sql_list.append("insert into %s values (1002, ['20200202'])" % table_name)
    sql_list.append("insert into %s values (1003, ['2020-01-01 12:23:12'])" % table_name)
    sql_list.append("insert into %s values (1004, ['2020/12/12'])" % table_name)
    err_sql_list.append("insert into %s values (1005, [2020-01-01])" % table_name)
    sql_list.append("insert into %s values (1006, [])" % table_name)
    sql_list.append("insert into %s values (1007, [NULL])" % table_name)
    sql_list.append("insert into %s values (1008, NULL)" % table_name)
    sql_list.append("insert into %s values (1009, ['ABC', 'DEF'])" % table_name)
    err_sql_list.append("insert into %s values (1010, 123)" % table_name)
    for i in sql_list:
        client.execute(i)
    for sql in err_sql_list:
        util.assert_return(False, ' ', client.execute, sql)
    sql = 'select * from %s order by k1' % table_name
    ret = client.execute(sql)
    expect = ((1000, u'[0001-01-01, 9999-12-31]'), (1001, u'[NULL]'),
              (1002, u'[2020-02-02]'), (1003, u'[2020-01-01]'),
              (1004, u'[2020-12-12]'), (1006, u'[]'),
              (1007, u'[NULL]'), (1008, None), (1009, u'[NULL, NULL]'))
    util.check(ret, expect)
    client.clean(database_name)


def test_array_datetime():
    """
    {
    "title": "test_array_datetime",
    "describe": "array类型中，子类型为datetime，stream、broker导入csv，导入成功，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_datetime_list
    local_file = FILE.test_array_datetime_local_file
    hdfs_file = FILE.test_array_datetime_remote_file
    expect_file = FILE.expe_array_datetime_file
    load_check(database_name, table_name, column_list, local_file, hdfs_file, expect_file)


def test_array_datetime_json():
    """
    {
    "title": "test_array_datetime_json",
    "describe": "array类型中，子类型为datetime，stream导入json，导入成功，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_datetime_list
    local_file = FILE.test_array_datetime_local_json
    expect_file = FILE.expe_array_datetime_file
    load_check(database_name, table_name, column_list, local_file, None, expect_file, 
               format='json', strip_outer_array=True)


def test_array_datetime_insert():
    """
    {
    "title": "test_array_datetime_insert",
    "describe": "array类型中，子类型为datetime，insert导入，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_datetime_list
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, column_list, keys_desc=SCHEMA.duplicate_key, set_null=True)
    assert ret, 'create table failed'
    # insert
    sql_list = list()
    err_sql_list = list()
    sql_list.append("insert into %s values (1000, [])" % table_name)
    sql_list.append("insert into %s values (1001, [NULL])" % table_name)
    sql_list.append("insert into %s values (1002, ['0001-01-01 00:00:00', '9999-12-31 23:59:58'])" % table_name)
    sql_list.append("insert into %s values (1003, ['2020-01-01'])" % table_name)
    sql_list.append("insert into %s values (1004, ['2020-13-01'])" % table_name)
    sql_list.append("insert into %s values (1005, ['0000-00-00 00:00:00'])" % table_name)
    sql_list.append("insert into %s values (1006, ['2020/01/01 00-00-00'])" % table_name)
    sql_list.append("insert into %s values (1007, ['20200101120000', 20200101120000])" % table_name)
    sql_list.append("insert into %s values (1008, NULL)" % table_name)
    sql_list.append("insert into %s values (1009, ['ABCD', 'AM'])" % table_name)
    sql_list.append("insert into %s values (1010, '1,2,3')" % table_name)
    sql_list.append("insert into %s values (1011, ['1975-06-18 01:47:17'])" % table_name)
    sql_list.append("insert into %s values (1012, '[0001-01-01 00:00:00, 9999-12-31 23:59:58]')" % table_name)
    for i in sql_list:
        client.execute(i)
    for sql in err_sql_list:
        util.assert_return(False, ' ', client.execute, sql)
    sql = 'select * from %s order by k1' % table_name
    ret = client.execute(sql)
    expect = ((1000, u'[]'), (1001, u'[NULL]'), (1002, u'[0001-01-01 00:00:00, 9999-12-31 23:59:58]'),
              (1003, u'[2020-01-01 00:00:00]'), (1004, u'[NULL]'), (1005, u'[NULL]'),
              (1006, u'[2020-01-01 00:00:00]'), (1007, u'[2020-01-01 12:00:00, 2020-01-01 12:00:00]'),
              (1008, None), (1009, u'[NULL, NULL]'), (1010, None), (1011, u'[1975-06-18 01:47:17]'),
              (1012, u'[0001-01-01 00:00:00, 9999-12-31 23:59:58]'))
    util.check(ret, expect)
    client.clean(database_name)



def test_array_char():
    """
    {
    "title": "test_array_char",
    "describe": "array类型中，子类型为char，stream、broker导入csv，导入成功，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_char_list
    local_file = FILE.test_array_char_local_file
    hdfs_file = FILE.test_array_char_remote_file
    expect_file = FILE.expe_array_char_file
    load_check(database_name, table_name, column_list, local_file, hdfs_file, expect_file)


@pytest.mark.skip()
def test_array_char_json():
    """
    {
    "title": "test_array_char_json",
    "describe": "array类型中，子类型为char，stream导入json，导入成功，查询结果正确.",
    "tag": "function,p0"
    }
    """
    # todo: 存在json转义字符问题
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_char_list
    local_file = FILE.test_array_char_local_json
    expect_file = FILE.expe_array_char_file
    load_check(database_name, table_name, column_list, local_file, None, expect_file,
               format='json', strip_outer_array=True)


@pytest.mark.skip()
def test_array_char_insert():
    """
    {
    "title": "test_array_char_insert",
    "describe": "array类型中，子类型为char，insert导入，查询结果正确",
    "tag": "function,p0"
    }
    """
    # todo: strict为true未过滤，不生效
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_char_list
    client = common.create_workspace(database_name)
    client.set_variables('enable_insert_strict', 'true')
    ret = client.create_table(table_name, column_list, keys_desc=SCHEMA.duplicate_key, set_null=True)
    assert ret, 'create table failed'
    # insert
    sql_list = list()
    err_sql_list = list()
    sql_list.append("insert into %s values (1000, [''])" % table_name)
    sql_list.append("insert into %s values (1001, [NULL])" % table_name)
    sql_list.append("insert into %s values (1002, NULL)" % table_name)
    sql_list.append("insert into %s values (1003, ['true', 'false', null])" % table_name)
    err_sql_list.append("insert into %s values (1004, [1, 2, 3])" % table_name)
    sql_list.append("insert into %s values (1005, ['中文验证', '测试', '1kdads', '空 格'])" % table_name)
    sql_list.append("insert into %s values (1006, ['!@#$^&*()', '#$<>:\\'{},'])" % table_name)
    sql_list.append("insert into %s values (1009, ['hello', 'word'])" % table_name)
    sql_list.append("insert into %s values (1010, '[]')" % table_name)
    err_sql_list.append("insert into %s values (1004, [1, 2, 3])" % table_name)
    err_sql_list.append("insert into %s values (1007, ['too long:%s', 'ttds'])" % (table_name, 'changdu' * 50))
    sql_list.append("insert into %s values (1008, 'ddate')" % table_name) # ??
    for i in sql_list:
        client.execute(i)
    sql = 'select * from %s order by k1' % table_name
    ret = client.execute(sql)
    expect = ((1000, u"['']"), (1001, u'[NULL]'), (1002, None), (1003, u"['true', 'false', NULL]"),
              (1005, u"['\u4e2d\u6587\u9a8c\u8bc1', '\u6d4b\u8bd5', '1kdads', '\u7a7a \u683c']"),
              (1006, u"['!@#$^&*()', '#$<>:'{},']"), (1008, None), (1009, u"['hello', 'word']"),
              (1010, u'[]'))
    util.check(ret, expect, True)
    for sql in err_sql_list:
        util.assert_return(False, ' ', client.execute, sql)
    # strict关闭，之前不支持的sql导入成功
    client.set_variables('enable_insert_strict', 'false')
    err_sql_list = list()
    err_sql_list.append("insert into %s values (1007, ['too long:%s', 'ttds'])" % (table_name, 'changdu' * 50))
    err_sql_list.append("insert into %s values (1008, 'ddate')" % table_name)
    for sql in err_sql_list:
        util.assert_return(True, ' ', client.execute, sql)
    sql = 'select * from %s where k1 in (1007, 1008)' % table_name
    ret = client.execute(sql)
    expect = ((1008, None),)
    util.check(ret, expect)
    client.clean(database_name)


def test_array_varchar():
    """
    {
    "title": "test_array_varchar",
    "describe": "array类型中，子类型为varchar，stream、broker导入csv，导入成功，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_varchar_list
    local_file = FILE.test_array_varchar_local_file
    hdfs_file = FILE.test_array_varchar_remote_file
    expect_file = FILE.expe_array_varchar_file
    load_check(database_name, table_name, column_list, local_file, hdfs_file, expect_file)


def test_array_varchar_json():
    """
    {
    "title": "test_array_varchar_json",
    "describe": "array类型中，子类型为varchar，stream导入json，导入成功，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_varchar_list
    local_file = FILE.test_array_varchar_local_json
    expect_file = FILE.expe_array_varchar_file
    load_check(database_name, table_name, column_list, local_file, None, expect_file, 
               format='json', strip_outer_array=True)


def test_array_varchar_insert():
    """
    {
    "title": "test_array_varchar_insert",
    "describe": "array类型中，子类型为varchar，insert导入，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_varchar_list
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, column_list, keys_desc=SCHEMA.duplicate_key, set_null=True)
    assert ret, 'create table failed'
    sql_list = list()
    err_sql_list = list()
    sql_list.append("insert into %s values (1000, [''])" % table_name)
    sql_list.append("insert into %s values (1001, [NULL])" % table_name)
    sql_list.append("insert into %s values (1002, NULL)" % table_name)
    sql_list.append("insert into %s values (1003, ['true', 'false', null])" % table_name)
    sql_list.append("insert into %s values (1004, [1, 2, 3])" % table_name)
    sql_list.append("insert into %s values (1005, ['中文验证', '测试', '1kdads', '空 格'])" % table_name)
    sql_list.append("insert into %s values (1006, ['!@#$^&*()', '#$<>:\\'{},'])" % table_name)
    err_sql_list.append("insert into %s values (1007, ['too long:%s']" % (table_name, 'changdu' * 50))
    sql_list.append("insert into %s values (1008, 'ddate')" % table_name)
    sql_list.append("insert into %s values (1009, ['hello', 'word'])" % table_name)
    sql_list.append("insert into %s values (1010, '[]')" % table_name)
    err_sql_list.append("insert into %s values (1011, {'test'})" % table_name)
    err_sql_list.append("insert into %s values (1012, [{},{}])" % table_name)
    err_sql_list.append("insert into %s values (1013, [[],[]])" % table_name)
    for i in sql_list:
        client.execute(i)
    for sql in err_sql_list:
        util.assert_return(False, ' ', client.execute, sql)
    sql = 'select * from %s order by k1' % table_name
    ret = client.execute(sql)
    expect = ((1000, u"['']"), (1001, u'[NULL]'), (1002, None), (1003, u"['true', 'false', NULL]"),
              (1004, u"['1', '2', '3']"),
              (1005, u"['\u4e2d\u6587\u9a8c\u8bc1', '\u6d4b\u8bd5', '1kdads', '\u7a7a \u683c']"),
              (1006, u"['!@#$^&*()', '#$<>:'{},']"), (1008, None),
              (1009, u"['hello', 'word']"), (1010, u'[]'))
    util.check(ret, expect)
    client.clean(database_name)


def test_array_string():
    """
    {
    "title": "test_array_string",
    "describe": "array类型中，子类型为string，stream、broker导入csv，导入成功，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_string_list
    local_file = FILE.test_array_varchar_local_file
    hdfs_file = FILE.test_array_varchar_remote_file
    expect_file = FILE.expe_array_string_file
    load_check(database_name, table_name, column_list, local_file, hdfs_file, expect_file)


def test_array_string_json():
    """
    {
    "title": "test_array_string_json",
    "describe": "array类型中，子类型为string，stream导入json，导入成功，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_string_list
    local_file = FILE.test_array_varchar_local_json
    expect_file = FILE.expe_array_string_file
    load_check(database_name, table_name, column_list, local_file, None, expect_file, 
               format='json', strip_outer_array=True)


def test_array_string_insert():
    """
    {
    "title": "test_array_string_insert",
    "describe": "array类型中，子类型为string，insert导入，查询结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    column_list = SCHEMA.array_string_list
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, column_list, keys_desc=SCHEMA.duplicate_key, set_null=True)
    assert ret, 'create table failed'
    # insert
    sql_list = list()
    err_sql_list = list()
    sql_list.append("insert into %s values (1000, [''])" % table_name)
    sql_list.append("insert into %s values (1001, [NULL])" % table_name)
    sql_list.append("insert into %s values (1002, NULL)" % table_name)
    sql_list.append("insert into %s values (1003, ['true', 'false', null])" % table_name)
    sql_list.append("insert into %s values (1004, [1, 2, 3])" % table_name)
    sql_list.append("insert into %s values (1005, ['中文验证', '测试', '1kdads', '空 格'])" % table_name)
    sql_list.append("insert into %s values (1006, ['!@#$^&*()', '#$<>:\\'{},'])" % table_name)
    sql_list.append("insert into %s values (1007, ['long:%s'])" % (table_name, 'changdu' * 50))
    sql_list.append("insert into %s values (1008, 'ddate')" % table_name)
    sql_list.append("insert into %s values (1009, ['hello', 'word'])" % table_name)
    sql_list.append("insert into %s values (1010, '[]')" % table_name)
    err_sql_list.append("insert into %s values (1011, {'test'})" % table_name)
    err_sql_list.append("insert into %s values (1012, [{},{}])" % table_name)
    err_sql_list.append("insert into %s values (1013, [[],[]])" % table_name)
    for i in sql_list:
        client.execute(i)
    for sql in err_sql_list:
        util.assert_return(False, ' ', client.execute, sql)
    sql = 'select * from %s order by k1' % table_name
    ret = client.execute(sql)
    expect = ((1000, u"['']"), (1001, u'[NULL]'), (1002, None),
              (1003, u"['true', 'false', NULL]"), (1004, u"['1', '2', '3']"),
              (1005, u"['\u4e2d\u6587\u9a8c\u8bc1', '\u6d4b\u8bd5', '1kdads', '\u7a7a \u683c']"),
              (1006, u"['!@#$^&*()', '#$<>:'{},']"),
              (1007, u"['long:%s']" % ('changdu' * 50)), (1008, None),
              (1009, u"['hello', 'word']"), (1010, u'[]'))
    util.check(ret, expect)
    client.clean(database_name)


def test_array_parquet():
    """
    {
    "title": "test_array_parquet",
    "describe": "parquet文件，列类型一个为string，一个是hive生成的array复杂类型列，分别导入，不支持hive生成的格式，但be不core",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, SCHEMA.array_table_list, keys_desc=SCHEMA.duplicate_key, set_null=True)
    assert ret, 'create table failed'
    load_data_info = palo_client.LoadDataInfo(FILE.test_array_table_remote_parquet_string, 
                                              table_name, format_as='parquet')
    ret = client.batch_load(util.get_label(), load_data_info, broker=broker_info, is_wait=True)
    assert ret, 'broker load failed'
    assert client.verify(FILE.expe_array_table_file, table_name)

    column_name_list = util.get_attr(SCHEMA.array_table_list, 0)
    load_data_info = palo_client.LoadDataInfo(FILE.test_array_table_remote_parquet_hive, 
                                              table_name, format_as='parquet', column_name_list=column_name_list)
    ret = client.batch_load(util.get_label(), load_data_info, broker=broker_info, is_wait=True)
    assert not ret, 'expect broker load failed'
    client.clean(database_name)


def test_array_orc():
    """
    {
    "title": "test_array_orc",
    "describe": "orc文件，列类型一个为string，一个是hive生成的array类型列，分别导入，支持hive生成的格式,todo check",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, SCHEMA.array_table_list, keys_desc=SCHEMA.duplicate_key, set_null=True)
    assert ret, 'create table failed'
    load_data_info = palo_client.LoadDataInfo(FILE.test_array_table_remote_orc_string, table_name, format_as='orc')
    ret = client.batch_load(util.get_label(), load_data_info, broker=broker_info, is_wait=True)
    assert ret, 'broker load faled'
    assert client.verify(FILE.expe_array_table_file, table_name)

    load_data_info = palo_client.LoadDataInfo(FILE.test_array_table_remote_parquet_hive,
                                              table_name, format_as='orc')
    ret = client.batch_load(util.get_label(), load_data_info, broker=broker_info, is_wait=True)
    assert not ret, 'expect broker load failed'

    assert client.truncate(table_name), 'truncate table failed'
    load_data_info = palo_client.LoadDataInfo(FILE.test_array_table_remote_orc_hive,
                                              table_name, format_as='orc')
    ret = client.batch_load(util.get_label(), load_data_info, broker=broker_info, is_wait=True)
    assert ret, 'expect broker load failed'
    # assert client.verify(FILE.expe_array_table_file, table_name)
    # client.clean(database_name)


def test_array_json():
    """
    {
    "title": "test_array_orc",
    "describe": "json文件导入，成功，结果正确",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, SCHEMA.array_table_list, keys_desc=SCHEMA.duplicate_key, 
                              set_null=True)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.test_array_table_local_json, format='json',
                             strip_outer_array=True, num_as_string='true')
    assert ret, 'stream load failed'
    common.check_by_file(FILE.expe_array_table_file, table_name, client=client, 
                         a7=palo_types.ARRAY_DECIMAL, a8=palo_types.ARRAY_FLOAT,
                         a9=palo_types.ARRAY_DOUBLE)
    client.clean(database_name)


def test_array_strict():
    """
    {
    "title": "test_array_strict",
    "describe": "验证strict模式，分别为not Null列，strict为true，strict为false，构造错误数据",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    schema = SCHEMA.array_int_list
    tb_n = table_name + '_n' # not null
    tb_s = table_name + '_s' # strict true
    tb_ns = table_name + '_ns' # strict false
    ret = client.create_table(tb_s, schema, keys_desc=SCHEMA.duplicate_key, set_null=True)
    assert ret, 'create table failed'
    ret = client.create_table(tb_ns, schema, keys_desc=SCHEMA.duplicate_key, set_null=True)
    assert ret, 'create table failed'
    ret = client.create_table(tb_n, schema, keys_desc=SCHEMA.duplicate_key, set_null=False)
    assert ret, 'create table failed'
    ret = client.stream_load(tb_n, FILE.test_array_mix_file_local, max_filter_ratio=1, column_separator='|')
    assert ret, 'stream load failed'
    ret = client.stream_load(tb_s, FILE.test_array_mix_file_local, strict_mode='true', 
                             max_filter_ratio=1, column_separator='|')
    assert ret, 'stream load failed'
    ret = client.stream_load(tb_ns, FILE.test_array_mix_file_local, strict_mode='false',
                             max_filter_ratio=1, column_separator='|')
    assert ret, 'stream load failed'

    tb_i = table_name + '_i' # insert test
    ret = client.create_table(tb_i, schema, keys_desc=SCHEMA.duplicate_key, set_null=True)
    assert ret, 'create table failed'
    insert_sql = 'insert into %s values(1, "abc")' % tb_i
    # todo
    # client.set_variables('enable_insert_strict', 'true')
    # util.assert_return(False, ' ', client.execute, insert_sql)
    client.set_variables('enable_insert_strict', 'false')
    util.assert_return(True, None, client.execute, insert_sql)
    # client.clean(database_name)    


def test_array_stream_param():
    """
    {
    "title": "test_array_stream_param",
    "describe": "导入时，设置的一些参数，如set，where，columns，partition",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    check_tb = 'check_tb'
    ret = client.create_table(check_tb, SCHEMA.array_table_list, keys_desc=SCHEMA.duplicate_key)
    assert ret, 'create table failed'
    ret = client.stream_load(check_tb, FILE.test_array_table_local_file, column_separator='|')
    assert ret, 'stream load failed'
    ret = client.create_table(table_name, SCHEMA.array_table_list, keys_desc=SCHEMA.duplicate_key, 
                              partition_info=SCHEMA.baseall_tinyint_partition_info)
    assert ret, 'create table failed'
    # column, where, partition
    columns = ['k1', 'b1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8',
               'a9', 'a10', 'a11', 'a12', 'a13', 'a14', 'a1=[]']
    ret = client.stream_load(table_name, FILE.test_array_table_local_file, column_separator='|', 
                             column_name_list=columns, where='size(a2) > 270', partitions='p1, p2')
    assert not ret, 'expect stream load failed'

    columns = ['k1', 'b1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8',
               'a9', 'a10', 'a11', 'a12', 'a13', 'a14', 'a1=[]']
    ret = client.stream_load(table_name, FILE.test_array_table_local_file, column_separator='|',
                             column_name_list=columns, where='size(a2) > 270', partitions='p1, p2', 
                             max_filter_ratio=0.8)
    assert ret, 'stream load failed'
    sql1 = 'select * from %s order by k1' % table_name
    sql2 = 'select k1, [], a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14 from %s ' \
           'where size(a2) > 270 and k1 < 0 order by k1' % check_tb
    common.check2(client, sql1=sql1, sql2=sql2)
    # array function    
    client.truncate(table_name)
    columns = ['k1', 'a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9',
               'a10', 'a11', 'a12', 'a13', 'b14', 
               'a14=array_sort(array_union(cast(a12 as array<string>), cast(a13 as array<string>)))']
    ret = client.stream_load(table_name, FILE.test_array_table_local_file, column_separator='|', 
                             column_name_list=columns, where='size(a2) > 270', partitions='p1, p2',
                             max_filter_ratio=0.8)
    assert ret, 'stream load failed'
    sql1 = 'select * from %s order by k1' % table_name
    sql2 = 'select k1, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, ' \
           'array_sort(array_union(a12, a13)) from %s where size(a2) > 270 and k1 < 0 order by k1' % check_tb
    common.check2(client, sql1=sql1, sql2=sql2)
    # array function, todo core
    client.truncate(table_name)
    columns = ['k1', 'a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9', 'a10',
               'a11', 'a12', 'a13', 'b14', 'a14=array_remove(cast(a5 as array<bigint>), 1)']
    ret = client.stream_load(table_name, FILE.test_array_table_local_file, column_separator='|', 
                             column_name_list=columns, where='size(a2) > 270', partitions='p1, p2',
                             max_filter_ratio=0.8)
    assert ret, 'stream load failed'
    sql1 = 'select * from %s order by k1' % table_name
    sql2 = 'select k1, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, array_sort(array_union(a12, a13)) ' \
           'from %s where size(a2) > 270 and k1 < 0 order by k1' % check_tb
    common.check2(client, sql1=sql1, sql2=sql2)
    # client.clean(database_name)


def test_array_broker_param():
    """
    {
    "title": "test_array_broker_param",
    "describe": "导入时，设置的一些参数，如set，where，columns，partition",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    check_tb = 'check_tb'
    ret = client.create_table(check_tb, SCHEMA.array_table_list, keys_desc=SCHEMA.duplicate_key)
    assert ret, 'create table failed'
    ret = client.stream_load(check_tb, FILE.test_array_table_local_file, column_separator='|')
    assert ret, 'stream load failed'
    ret = client.create_table(table_name, SCHEMA.array_table_list, keys_desc=SCHEMA.duplicate_key,
                              partition_info=SCHEMA.baseall_tinyint_partition_info)
    assert ret, 'create table failed'
    # broker load & check
    column_name_list =  ['k1', 'a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7',
                         'a8', 'a9', 'a10', 'a11', 'a12', 'a13', 'b14']
    set_list = ['a14=array_union(cast(b14 as array<string>), cast(a13 as array<string>))']
    load_data_info = palo_client.LoadDataInfo(FILE.test_array_table_remote_file, table_name, 
                                              partition_list=['p1', 'p2'], 
                                              column_name_list=column_name_list, set_list=set_list, 
                                              column_terminator='|', where_clause='size(a2) > 270')
    ret = client.batch_load(util.get_label(), load_data_info, max_filter_ratio=0.8, broker=broker_info, is_wait=True)
    assert ret
    sql1 = 'select * from %s order by k1' % table_name
    sql2 = 'select k1, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, array_union(a14, a13) from %s ' \
           'where size(a2) > 270 and k1 < 0 order by k1' % check_tb
    common.check2(client, sql1=sql1, sql2=sql2)

    client.truncate(table_name)
    set_list = ['a14=[]']
    load_data_info = palo_client.LoadDataInfo(FILE.test_array_table_remote_file, table_name, 
                                              partition_list=['p1', 'p2'],
                                              column_name_list=column_name_list, set_list=set_list,
                                              column_terminator='|', where_clause='size(a2) > 270')
    ret = client.batch_load(util.get_label(), load_data_info, max_filter_ratio=0.8, broker=broker_info, is_wait=True)
    assert ret
    sql1 = 'select * from %s order by k1' % table_name
    sql2 = 'select k1, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, [] from %s ' \
           'where size(a2) > 270 and k1 < 0 order by k1' % check_tb
    common.check2(client, sql1=sql1, sql2=sql2)
    client.truncate(table_name)

    set_list = ['a14=[]']
    load_data_info = palo_client.LoadDataInfo(FILE.test_array_table_remote_file, table_name,
                                              partition_list=['p1', 'p2'],
                                              column_name_list=column_name_list, set_list=set_list,
                                              column_terminator='|', where_clause='size(array_distinct(a2)) = 1')
    ret = client.batch_load(util.get_label(), load_data_info, max_filter_ratio=0.8, broker=broker_info, is_wait=True)
    assert ret
    sql1 = 'select * from %s order by k1' % table_name
    sql2 = 'select k1, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, [] from %s ' \
           'where size(array_distinct(a2)) = 1 and k1 < 0 order by k1' % check_tb
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


if __name__ == '__main__':
    setup_module()
    # test_array_decimal() # todo: 10000000000000000000.1111111222222222 vs 100000000000000000.001111111
    # test_array_char() # todo 逗号数据不正确问题,json转义字符问题
    # test_array_stream_param() # todo stream load core
    test_array_strict() # todo
