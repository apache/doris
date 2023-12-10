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
file: test_stream_load_json.py
测试stream导入json格式文件
"""
import os
import sys
import time
file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(file_dir)
file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(file_dir)

from data import schema as DATA
from data import partition as CHECK_DATA
from lib import palo_config
from lib import palo_client
from lib import util
from lib import common

config = palo_config.config


def setup_module():
    """setup"""
    pass


def teardown_module():
    """teardown"""
    pass


def check_date(client, check_file, table, column_list):
    """
    check date/datetime result
    python datetime不支持年为0000，会自动变为None，需要转成字符串进行校验
    """
    check_table = 'check_table'
    sql = 'drop table if exists %s' % check_table
    client.execute(sql)
    modify_col_list = list()
    cast_sql = list()
    for col in column_list:
        col = col[:1] + ('char(20)',) + col[2:]
        modify_col_list.append(col)
        sql = 'cast(%s as string)' % col[0]
        cast_sql.append(sql)
    print(modify_col_list)
    ret = client.create_table(check_table, modify_col_list, set_null=True,
                              distribution_info=DATA.hash_distribution_info, keys_desc=DATA.duplicate_key)
    assert ret
    sql = 'insert into %s select %s from %s' % (check_table, ','.join(cast_sql), table)
    ret = client.execute(sql)
    assert ret == ()
    assert client.verify(check_file, check_table)


def base(database_name, table_name, column_list, local_data_file, expect_data_file, sql=None, 
         load_column_name=None, convert_check=None, **kwarg):
    """datatype base test"""
    client = common.create_workspace(database_name)
    if sql is None:
        sql = 'select * from %s orer by 1, 2'
    tb_null = table_name + '_null'
    ret = client.create_table(tb_null, column_list, set_null=True,
                              distribution_info=DATA.hash_distribution_info,
                              keys_desc=DATA.duplicate_key)
    assert ret, 'create table failed'
    # stream load
    ret = client.stream_load(tb_null, local_data_file, column_name_list=load_column_name, 
                             max_filter_ratio=1, format='json', strip_outer_array='true', **kwarg)
    assert ret, 'stream load failed'
    tb_not_null = table_name + '_not_null'
    ret = client.create_table(tb_not_null, column_list, set_null=False,
                              distribution_info=DATA.hash_distribution_info,
                              keys_desc=DATA.duplicate_key)
    assert ret, 'create table failed'
    ret = client.stream_load(tb_not_null, local_data_file, max_filter_ratio=1, column_name_list=load_column_name,
                             format='json', strip_outer_array='true', **kwarg)
    assert ret, 'stream load failed'
    if convert_check is True:
        check_date(client, expect_data_file + '_null_ns.data', tb_null, column_list)
        check_date(client, expect_data_file + '_not_null.data', tb_not_null, column_list)
    else:
        assert client.verify(expect_data_file + '_null_ns.data', tb_null), 'check error'
        assert client.verify(expect_data_file + '_not_null.data', tb_not_null), 'check error'
    client.clean(database_name)


def test_json_tinyint():
    """
    {
    "title": "test_json_tinyint",
    "describe": "json array文件，TINYINT类型作HASH列, DUPLICATE KEY, test strict mode",
    "tag": "p1,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.tinyint_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_number.json" % file_dir
    expect_data_file = '%s/data/LOAD/expe_tinyint' % file_dir
    base(database_name, table_name, column_list, local_data_file, expect_data_file)


def test_json_smallint():
    """
    {
    "title": "test_json_smallint",
    "describe": "json array文件，SMALLINT类型作HASH列, DUPLICATE KEY, test strict mode",
    "tag": "p1,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.smallint_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_number.json" % file_dir
    expect_data_file = '%s/data/LOAD/expe_smallint' % file_dir
    base(database_name, table_name, column_list, local_data_file, expect_data_file)


def test_json_int():
    """
    {
    "title": "test_json_int",
    "describe": "json array文件，INT类型作HASH列, DUPLICATE KEY, test strict mode",
    "tag": "p1,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.int_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_number.json" % file_dir
    expect_data_file = '%s/data/LOAD/expe_int' % file_dir
    base(database_name, table_name, column_list, local_data_file, expect_data_file)


def test_json_bigint():
    """
    {
    "title": "test_json_bigint",
    "describe": "json array文件，BIGINT类型做HASH列, DUPLICATE KEY, test strict mode",
    "tag": "p1,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.bigint_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_number.json" % file_dir
    expect_data_file = '%s/data/LOAD/expe_bigint' % file_dir
    base(database_name, table_name, column_list, local_data_file, expect_data_file)


def test_json_largeint():
    """
    {
    "title": "test_json_largeint",
    "describe": "json array文件，LARGEINT类型作HASH列, DUPLICATE KEY, test strict mode",
    "tag": "p1,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.largeint_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_number.json" % file_dir
    expect_data_file = '%s/data/LOAD/expe_largeint' % file_dir
    base(database_name, table_name, column_list, local_data_file, expect_data_file)


def test_json_float_int():
    """
    {
    "title": "test_json_float_int",
    "describe": "json array文件，INT类型作AGGREGATE KEY，FLOAT类型作VALUE列, test strict mode",
    "tag": "p1,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.float_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_number.json" % file_dir
    expect_data_file = '%s/data/LOAD/expe_float_int' % file_dir
    jsonpaths = '["$.v1", "$.k1", "$.v2", "$.v3", "$.v4"]'
    # load_column_name = ['v1', 'k1', 'v2', 'v3', 'v4']
    base(database_name, table_name, column_list, local_data_file, expect_data_file,
         load_column_name=None, jsonpaths=jsonpaths)


def test_json_double_int():
    """
    {
    "title": "test_json_double_int",
    "describe": "json array文件，INT类型作AGGREGATE KEY，DOUBLE类型作VALUE列, test strict mode",
    "tag": "p1,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.double_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_number.json" % file_dir
    expect_data_file = '%s/data/LOAD/expe_double_int' % file_dir
    jsonpaths = '["$.v1", "$.k1", "$.v2", "$.v3", "$.v4"]'
    load_column_name = ['v1', 'k1', 'v2', 'v3', 'v4']
    base(database_name, table_name, column_list, local_data_file, expect_data_file,
         load_column_name=None, jsonpaths=jsonpaths)


def test_json_date():
    """
    {
    "title": "test_json_date",
    "describe": "json array文件，DATE类型作HASH列, DUPLICATE KEY, test strict mode",
    "tag": "p1,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.date_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_number.json" % file_dir
    expect_data_file = '%s/data/LOAD/expe_date' % file_dir
    load_column_name = ['k1', 'v1', 'v2', 'v3', 'v4']
    base(database_name, table_name, column_list, local_data_file, expect_data_file,
         load_column_name=load_column_name, convert_check=True)


def test_json_datetime():
    """
    {
    "title": "test_json_datetime",
    "describe": "json array文件，DATETIME类型作HASH列, DUPLICATE KEY, test strict mode",
    "tag": "p1,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.datetime_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_number.json" % file_dir
    expect_data_file = '%s/data/LOAD/expe_datetime' % file_dir
    load_column_name = ['k1', 'v1', 'v2', 'v3', 'v4']
    base(database_name, table_name, column_list, local_data_file, expect_data_file,
         load_column_name=load_column_name, convert_check=True)


def test_json_decimal_least():
    """
    {
    "title": "test_json_decimal_least",
    "describe": "json array文件，DECIMAL(1,0)类型作HASH列, DUPLICATE KEY, test strict mode",
    "tag": "p1,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.decimal_least_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_number.json" % file_dir
    expect_data_file = "%s/data/LOAD/expe_decimal_least" % file_dir
    base(database_name, table_name, column_list, local_data_file, expect_data_file)


def test_json_decimal_normal():
    """
    {
    "title": "test_json_decimal_normal",
    "describe": "DECIMAL(10,5)类型作HASH列, DUPLICATE KEY, test strick mode",
    "tag": "p1,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.decimal_normal_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_number.json" % file_dir
    expect_data_file = "%s/data/LOAD/expe_decimal_normal" % file_dir
    base(database_name, table_name, column_list, local_data_file, expect_data_file)


def test_json_decimal_most():
    """
    {
    "title": "test_json_decimal_most",
    "describe": "json array文件，DECIMAL(27,9)类型作HASH列, DUPLICATE KEY, test strict mode",
    "tag": "p1,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.decimal_most_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_number.json" % file_dir
    expect_data_file = "%s/data/LOAD/expe_decimal_most" % file_dir
    base(database_name, table_name, column_list, local_data_file, expect_data_file)


def test_json_char_least():
    """
    {
    "title": "test_json_char_least",
    "describe": "json array文件，CHAR类型作HASH列, DUPLICATE KEY, test strict mode",
    "tag": "p1,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.char_least_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_char.json" % file_dir
    expect_data_file = '%s/data/LOAD/expe_char_least' % file_dir
    base(database_name, table_name, column_list, local_data_file, expect_data_file)


def test_json_char_normal():
    """
    {
    "title": "test_json_char_normal",
    "describe": "json array文件，CHAR_NORMAL类型做HASH列，考虑特殊字符, DUPLICATE KEY, test strict mode",
    "tag": "p1,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.char_normal_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_char.json" % file_dir
    expect_data_file = '%s/data/LOAD/expe_char_normal' % file_dir
    base(database_name, table_name, column_list, local_data_file, expect_data_file)


def test_json_varchar_least():
    """
    {
    "title": "test_json_varchar_least",
    "describe": "json array文件，VARCHAR_LEAST类型做HASH列, DUPLICATE KEY, test strict mode",
    "tag": "p1,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.varchar_least_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_char.json" % file_dir
    expect_data_file = '%s/data/LOAD/expe_varchar_least' % file_dir
    base(database_name, table_name, column_list, local_data_file, expect_data_file)
    
    
def test_json_varchar_normal():
    """
    {
    "title": "test_json_varchar_normal",
    "describe": "json array文件，VARCHAR_NORMAL类型做HASH列, DUPLICATE KEY, test strict mode",
    "tag": "p1,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.varchar_normal_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_char.json" % file_dir
    expect_data_file = '%s/data/LOAD/expe_varchar_normal' % file_dir
    base(database_name, table_name, column_list, local_data_file, expect_data_file)


def test_json_array_error_format():
    """
    {
    "title": "test_json_array_error_format",
    "describe": "json array文件最后一行含有逗号，整个json文件识别错误，Parse json data for JsonDoc failed. code = 3, error-info:Invalid value.",
    "tag": "p1,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.partition_column_list,
                              distribution_info=DATA.hash_distribution_info)
    assert ret, 'create table failed'
    local_data_file = '%s/data/LOAD/json_array_wrong1.json' % file_dir
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=0,
                             format='json', strip_outer_array='true')
    assert ret == 0, 'expect stream load failed'
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=1,
                             format='json', strip_outer_array='true')
    assert ret == 1, 'expect stream load success'
    client.clean(database_name)


def test_json_array_empty():
    """
    {
    "title": "test_json_array_empty",
    "describe": "json arra为空的array[]，max_filter_ratio=0时导入失败，Reason: Empty json line. src line: [[]];",
    "tag": "p1,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.partition_column_list, 
                              distribution_info=DATA.hash_distribution_info)
    assert ret, 'create table failed'
    local_data_file = '%s/data/LOAD/json_array_wrong2.json' % file_dir
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=0,
                             format='json', strip_outer_array='true')
    assert ret == 0, 'expect stream load failed'
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=1,
                             format='json', strip_outer_array='true')
    assert ret == 1, 'expect stream load success'
    client.clean(database_name)


def test_csv_load_as_json():
    """
    {
    "title": "test_csv_load_as_json",
    "describe": "stream load设置format为json，而本地文件为csv格式，max_filter_ratio=0时预期导入失败",
    "tag": "p1,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.partition_column_list, 
                              distribution_info=DATA.hash_distribution_info)
    assert ret, 'create table failed'
    local_data_file = '%s/data/PARTITION/partition_type' % file_dir
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=0,
                             format='json', strip_outer_array='true')
    # Parse json data for JsonDoc failed. code = 2, 
    # error-info:The document root must not be followed by other values
    assert ret == 0, 'expect stream load failed'
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=1,
                             format='json', strip_outer_array='true')
    assert ret == 1, 'expect stream load success'
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=0,
                             format='json')
    assert ret == 0, 'expect stream load failed'
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=1,
                             format='json')
    assert ret == 1, 'expect stream load success'
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=0)
    assert ret == 1, 'expect stream load success'
    client.clean(database_name)


def test_json_array_record_wrong_format():
    """
    {
    "title": "test_json_array_record_wrong_format",
    "describe": "json array，某一行的格式不对最后一个元素后面带有逗号，json文件解析错误，整个导入失败，报错：Parse json data for JsonDoc failed. code = 4, error-info:Missing a name for object member. src line给出了格式不对的行",
    "tag": "p1,fuzz"
    }
    """
    """解析失败的，都认为只load 1行数据"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.partition_column_list,
                              distribution_info=DATA.hash_distribution_info)
    assert ret, 'create table failed'
    local_data_file = '%s/data/LOAD/json_array_wrong4.json' % file_dir
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=0,
                             format='json', strip_outer_array='true')
    assert ret == 0, 'expect stream load failed'
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=1,
                             format='json', strip_outer_array='true')
    assert ret == 1, 'expect stream load success'
    client.clean(database_name)


def test_json_array_with_strip_false():
    """
    {
    "title": "test_json_array_with_strip_false",
    "describe": "json array，stream load导入设置strip_array_out为false，导入失败，提示:JSON data is array-object, `strip_outer_array` must be TRUE.",
    "tag": "p1,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.partition_column_list, 
                              distribution_info=DATA.hash_distribution_info)
    assert ret, 'create table failed'
    local_data_file = '%s/data/LOAD/partition_type.json' % file_dir
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=0,
                             format='json', strip_outer_array='false')
    assert ret == 0, 'expect stream load failed'
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=0,
                             format='json')
    assert ret == 0, 'expect stream load failed'
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=1,
                             format='json', strip_outer_array='false')
    assert ret == 1, 'expect stream load success'
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=1,
                             format='json')
    assert ret == 1, 'expect stream load success'
    client.clean(database_name)


def test_json_array_with_strip_True():
    """
    {
    "title": "test_json_array_with_strip_True",
    "describe": "json array格式正确，stream load导入设置strip_array_out为True，大小写不影响，导入成功",
    "tag": "p1,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.partition_column_list,
                              distribution_info=DATA.hash_distribution_info)
    assert ret, 'create table failed'
    local_data_file = '%s/data/LOAD/partition_type.json' % file_dir
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=1,
                             format='json', strip_outer_array='True')
    assert ret, 'stream load failed'
    client.clean(database_name)


def test_json_object_basic():
    """
    {
    "title": "test_json_object_basic",
    "describe": "导入json object，文件正确，参数正确，验证结果",
    "tag": "p1,fuzz"
    }
    """
    """导入object，文件正确，参数正确，验证结果"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.partition_column_list, 
                              distribution_info=DATA.hash_distribution_info)
    assert ret, 'create table failed'
    local_data_file = '%s/data/LOAD/json_object_basic.json' % file_dir
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=1,
                             format='json')
    assert ret, 'expect stream load succ'
    # 校验
    sql1 = 'select * from %s' % table_name
    sql2 = 'select 1, 10, 100, 1000, cast("2011-01-01 00:00:00" as datetime), ' \
           'cast("2010-01-01" as date), "t", "ynqnzeowymt", 38.638843, 180.998031, 7395.231067'
    common.check2(client, sql1, sql2=sql2)
    client.clean(database_name)


def test_json_object_wrong_format():
    """
    {
    "title": "test_json_object_wrong_format",
    "describe": "导入object，文件格式不正确(最后一个元素后面有逗号)，导入失败, Parse json data for JsonDoc failed. code = 4, error-info:Missing a name for object member.",
    "tag": "p1,fuzz"
    }
    """
    """
    测试object
    1. array格式是否导入
    2. object中含有复杂类型
    3. 数据类型不匹配（由strict决定是否导入）
    """
    """导入object，文件格式不正确"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.partition_column_list,
                              distribution_info=DATA.hash_distribution_info)
    assert ret, 'create table failed'
    local_data_file = '%s/data/LOAD/json_object_wrong1.json' % file_dir
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=0,
                             format='json')
    assert ret == 0, 'expect stream load failed'
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=1,
                             format='json')
    assert ret == 1, 'expect stream load success'
    client.clean(database_name)


def test_json_object_wrong_format_2():
    """
    {
    "title": "test_json_object_wrong_format_2",
    "describe": "导入object，文件格式不正确(某一个key的value缺失)， 导入失败, Parse json data for JsonDoc failed. code = 3, error-info:Invalid value.",
    "tag": "p1,fuzz"
    }
    """
    """导入object，文件格式不正确"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.partition_column_list,
                              distribution_info=DATA.hash_distribution_info)
    assert ret, 'create table failed'
    local_data_file = '%s/data/LOAD/json_object_wrong2.json' % file_dir
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=0,
                             format='json')
    assert ret == 0, 'expect stream load failed'
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=1,
                             format='json')
    assert ret == 1, 'expect stream load success'
    client.clean(database_name)


def test_json_object_empty():
    """
    {
    "title": "test_json_object_empty",
    "describe": "导入object为空{}，预期导入失败,  All fields is null, this is a invalid row. max_filter_ratio=1时导入成功",
    "tag": "p1,fuzz"
    }
    """
    """导入object为空{}"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.partition_column_list, 
                              distribution_info=DATA.hash_distribution_info)
    assert ret, 'create table failed'
    local_data_file = '%s/data/LOAD/json_object_empty.json' % file_dir
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=0,
                             format='json')
    assert ret == 0, 'expect stream load failed'
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=1,
                             format='json')
    assert ret == 1, 'expect stream load success'
    client.clean(database_name)


def test_json_empty_file():
    """
    {
    "title": "test_json_empty_file",
    "describe": "导入空文件, 导入成功",
    "tag": "p1,fuzz"
    }
    """
    """导入object为空文件"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.partition_column_list,
                              distribution_info=DATA.hash_distribution_info)
    assert ret, 'create table failed'
    local_data_file = '%s/data/empty_file' % file_dir
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=0,
                             format='json')
    assert ret == 1, 'expect stream load success'
    client.clean(database_name)


def test_json_column_more_than_schema_without_jsonpath():
    """
    {
    "title": "test_json_column_more_than_schema_without_jsonpath",
    "describe": "json文件中的列全部属于schema中的列，并多于schema中的列，导入成功，不属于shema的列忽略",
    "tag": "p1,fuzz"
    }
    """
    """
    json文件中的列全部属于schema中的列，并多于schema中的列，导入成功，不属于shema的列忽略
    eg json: k1, k2, k3, k4, k5, v1, v2, schema: k1, k2, v1
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_list = [DATA.partition_column_list[0], DATA.partition_column_list[5]]
    ret = client.create_table(table_name, column_list, distribution_info=DATA.hash_distribution_info)
    assert ret, 'create table failed'
    local_data_file = local_data_file = '%s/data/LOAD/json_object_basic.json' % file_dir
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=1,
                             format='json')
    assert ret, 'stream load failed'
    sql1 = 'select * from %s' % table_name
    sql2 = 'select 1, cast("2010-01-01" as date)'
    common.check2(client, sql1, sql2=sql2)
    client.clean(database_name)


def test_json_column_less_than_schema_without_jsonpath():
    """
    {
    "title": "test_json_column_less_than_schema_without_jsonpath",
    "describe": "json文件中的列全部属于schema中的列，并少于schema中的列，导入成功，缺少的列使用null值填充，设置的default值不生效",
    "tag": "p1,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_list = [('k_add', 'INT')] + DATA.partition_column_list + [('v_add', 'INT', "REPLACE_IF_NOT_NULL", "100")]
    ret = client.create_table(table_name, column_list, distribution_info=DATA.hash_distribution_info, set_null=False)
    assert ret, 'create table failed'
    local_data_file = local_data_file = '%s/data/LOAD/json_object_basic.json' % file_dir
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=0,
                             format='json')
    assert ret == 0, 'expect stream load failed'
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=1,
                             format='json')
    assert ret == 1, 'expect stream load success'
    table_name = table_name + '_1'
    column_list = [('k_add', 'INT')] + DATA.partition_column_list + [('v_add', 'INT', "REPLACE_IF_NOT_NULL", "100")]
    ret = client.create_table(table_name, column_list, distribution_info=DATA.hash_distribution_info, set_null=True)
    assert ret, 'create table failed'
    local_data_file = local_data_file = '%s/data/LOAD/json_object_basic.json' % file_dir
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=0,
                             format='json')
    assert ret == 1, 'expect steam load success'
    # 校验
    sql1 = 'select * from %s' % table_name
    sql2 = 'select null, 1, 10, 100, 1000, cast("2011-01-01 00:00:00" as datetime), ' \
           'cast("2010-01-01" as date), "t", "ynqnzeowymt", 38.638844, 180.998031, 7395.231067, null'
    common.check2(client, sql1, sql2=sql2)
    client.clean(database_name)


def test_json_column_diff_with_schema_without_jsonpath():
    """
    {
    "title": "test_json_column_diff_with_schema_without_jsonpath",
    "describe": "json文件中的列部分属于于schema中的列，部分不属于，导入成功，缺少的列使用null值填充，设置的default值不生效",
    "tag": "p1,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_list = [('k_add', 'INT')] + DATA.partition_column_list[3:7] + \
                  [('v_add', 'INT', "REPLACE_IF_NOT_NULL", "100")]
    distribution_info = palo_client.DistributionInfo('HASH(k4)', 13)
    ret = client.create_table(table_name, column_list, distribution_info=distribution_info, set_null=False)
    assert ret, 'create table failed'
    local_data_file = local_data_file = '%s/data/LOAD/json_object_basic.json' % file_dir
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=0,
                             format='json')
    assert ret == 0, 'expect stream load failed'
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=1,
                             format='json')
    assert ret == 1, 'expect stream load success'
    table_name = table_name + '_1'
    column_list = [('k_add', 'INT')] + DATA.partition_column_list + [('v_add', 'INT', "REPLACE_IF_NOT_NULL", "100")]
    ret = client.create_table(table_name, column_list, distribution_info=DATA.hash_distribution_info, set_null=True)
    assert ret, 'create table failed'
    local_data_file = local_data_file = '%s/data/LOAD/json_object_basic.json' % file_dir
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=0,
                             format='json')
    # 校验
    sql1 = 'select * from %s' % table_name
    sql2 = 'select null, 1, 10, 100, 1000, cast("2011-01-01 00:00:00" as datetime), ' \
           'cast("2010-01-01" as date), "t", "ynqnzeowymt", 38.638844, 180.998031, 7395.231067, null'
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_json_column_exclude_schema_without_jsonpath():
    """
    {
    "title": "test_json_column_exclude_schema_without_jsonpath",
    "describe": "json文件中的列全部不属于schema中的列，object导入失败，提示：All fields is null, this is a invalid row.",
    "tag": "p1,fuzz"
    }
    """
    """
    json文件中的列全部不属于schema中的列，object导入失败
    eg. json：k1, k2, k3, schema: v1, v2, v3
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.all_type_column_list)
    assert ret, 'create table failed'
    local_data_file = local_data_file = '%s/data/LOAD/json_object_basic.json' % file_dir
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=0,
                             format='json')
    assert ret == 0, 'expect stream load failed'
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=1,
                             format='json')
    assert ret == 1, 'expect stream load success'
    client.clean(database_name)


def test_json_column_exclude_schema_without_jsonpath_2():
    """
    {
    "title": "test_json_column_exclude_schema_without_jsonpath_2",
    "describe": "json array，文件中一些行的列全部不属于schema中的列，认为是错误行，另一些列正确，array考虑max_filter_rati",
    "tag": "p1,fuzz"
    }
    """
    """
    json文件中的列全部不属于schema中的列，认为是错误行，array考虑max_filter_ratio
    eg. json：k1, k2, k3, schema: v1, v2, v3
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.partition_column_list, set_null=True)
    assert ret, 'create table failed'
    local_data_file = local_data_file = '%s/data/LOAD/json_column_match.json' % file_dir
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=0.4,
                             format='json', strip_outer_array='true')
    assert not ret, 'expect stream load failed'
    ret = client.stream_load(table_name, local_data_file, max_filter_ratio=0.5,
                             format='json', strip_outer_array='true')
    assert ret
    assert client.verify(CHECK_DATA.expected_data_file_list_1, table_name)
    client.clean(database_name)


def test_json_column_all_null_without_path():
    """
    {
    "title": "test_json_column_all_null_without_path",
    "describe": "json object中所有的key都是null，导入全部为Null，导入成功",
    "tag": "p1,fuzz"
    }
    """
    """json导入全部为Null"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.partition_column_list, 
                              distribution_info=DATA.hash_distribution_info, set_null=True)
    assert ret, 'create table failed'
    local_data_file = local_data_file = '%s/data/LOAD/json_object_null.json' % file_dir
    ret = client.stream_load(table_name, local_data_file, format='json')
    assert ret, 'stream load failed'
    # check
    sql1 = 'select * from %s' % table_name
    sql2 = 'select null, null, null, null, null, null, null, null, null, null, null'
    common.check2(client, sql1, sql2=sql2)
    client.clean(database_name)


def test_with_jsonpath_basic():
    """
    {
    "title": "test_with_jsonpath_basic",
    "describe": "json object导入指定jsonpath",
    "tag": "p1,fuzz"
    }
    """
    """导入指定jsonpath"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.partition_column_list, distribution_info=DATA.hash_distribution_info)
    assert ret, 'create table failed'
    local_data_file = '%s/data/LOAD/json_object_basic.json' % file_dir
    jsonpaths = '["$.k1", "$.k2", "$.k3", "$.k4", "$.k5", "$.v1", "$.v2", "$.v3", "$.v4", "$.v5", "$.v6"]'
    ret = client.stream_load(table_name, local_data_file,
                             format='json', jsonpaths=jsonpaths)
    assert ret, 'stream load failed'
    # 校验
    sql1 = 'select * from %s' % table_name
    sql2 = 'select 1, 10, 100, 1000, cast("2011-01-01 00:00:00" as datetime), ' \
           'cast("2010-01-01" as date), "t", "ynqnzeowymt", 38.638843, 180.998031, 7395.231067'
    common.check2(client, sql1, sql2=sql2)
    client.clean(database_name)


def test_jsonpath_without_columns():
    """
    {
    "title": "test_with_jsonpath_without_columns",
    "describe": "json object导入指定jsonpath，不指定columns，json文件中不存在的列填充Null，导入成功",
    "tag": "p1,fuzz"
    }
    """
    """不指定columns，提取json文件中不存在的列，不存在的列填充Null？还是应该default值？todo"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_list = [('k_add', 'INT')] + DATA.partition_column_list + [('v_add', 'INT', "REPLACE_IF_NOT_NULL", "100")]
    ret = client.create_table(table_name, column_list, distribution_info=DATA.hash_distribution_info, set_null=True)
    assert ret, 'create table failed'
    local_data_file = local_data_file = '%s/data/LOAD/json_object_basic.json' % file_dir
    jsonpaths = '["$.k_add", "$.k1", "$.k2", "$.k3", "$.k4", "$.k5", "$.v1", "$.v2", "$.v3", ' \
                 '"$.v4", "$.v5", "$.v6", "$.v_add"]'
    ret = client.stream_load(table_name, local_data_file,
                             format='json', jsonpaths=jsonpaths)
    assert ret, 'stream load failed'
    sql1 = 'select * from %s' % table_name
    sql2 = 'select null, 1, 10, 100, 1000, cast("2011-01-01 00:00:00" as datetime), ' \
           'cast("2010-01-01" as date), "t", "ynqnzeowymt", 38.638843, 180.998031, 7395.231067, null'
    common.check2(client, sql1, sql2=sql2)
    client.clean(database_name)


def test_jsonpath_less_than_schema_without_columns():
    """
    {
    "title": "test_jsonpath_less_than_schema_without_columns",
    "describe": "json object导入，指定的jsonpath中的列比schema中的列少，不指定columns，执行成功，使用null值填充
    "tag": "p1,fuzz"
    }
    """
    """
    指定的jsonpath中的列比schema中的列少，不指定columns（当不指定columns时，默认使用列的schema的列顺序和数量）
    报错：The column name of table is not foud in jsonpath: v6
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_list = [('k_add', 'INT')] + DATA.partition_column_list + [('v_add', 'INT', "REPLACE_IF_NOT_NULL", "100")]
    ret = client.create_table(table_name, column_list, distribution_info=DATA.hash_distribution_info, set_null=True)
    assert ret, 'create table failed'
    local_data_file = local_data_file = '%s/data/LOAD/json_object_basic.json' % file_dir
    jsonpaths = '["$.k_add", "$.k1", "$.k2", "$.k3", "$.k4", "$.k5", "$.v1", "$.v2", "$.v3", "$.v4", "$.v5"]'
    ret = client.stream_load(table_name, local_data_file,
                             format='json', jsonpaths=jsonpaths)
    assert ret
    sql1 = 'select * from %s' % table_name
    sql2 = 'select null, 1, 10, 100, 1000, cast("2011-01-01 00:00:00" as datetime), ' \
           'cast("2010-01-01" as date), "t", "ynqnzeowymt", 38.638843, 180.998031, null, null'
    common.check2(client, sql1, sql2=sql2)
    client.clean(database_name)   


def test_jsonpath_more_than_schema_without_columns():
    """
    {
    "title": "test_jsonpath_more_than_schema_without_columns",
    "describe": "json object导入，指定的jsonpath中的列比schema中的列多，不指定columns，导入成功",
    "tag": "p1,fuzz"
    }
    """
    """指定的jsonpath中的列比schema中的列少，不指定columns（当不指定columns时，默认使用列的schema的列顺序和数量）"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_list = DATA.partition_column_list
    ret = client.create_table(table_name, column_list, distribution_info=DATA.hash_distribution_info, set_null=True)
    assert ret, 'create table failed'
    local_data_file = local_data_file = '%s/data/LOAD/json_object_basic.json' % file_dir
    jsonpaths = '["$.k_add", "$.k1", "$.k2", "$.k3", "$.k4", "$.k5", "$.v1", "$.v2", "$.v3", ' \
                 '"$.v4", "$.v5", "$.v6", "$.v_add"]'
    ret = client.stream_load(table_name, local_data_file,
                             format='json', jsonpaths=jsonpaths)
    assert ret == 0, 'expect stream load failed'
    jsonpaths = '["$.k1", "$.k2", "$.k3", "$.k4", "$.k5", "$.v1", "$.v2", "$.v3", "$.v4", "$.v5", "$.v6", "$.v_add"]'
    ret = client.stream_load(table_name, local_data_file,
                             format='json', jsonpaths=jsonpaths)
    assert ret, 'stream load failed'
    sql1 = 'select * from %s' % table_name
    sql2 = 'select 1, 10, 100, 1000, cast("2011-01-01 00:00:00" as datetime), ' \
           'cast("2010-01-01" as date), "t", "ynqnzeowymt", 38.638843, 180.998031, 7395.231067'
    common.check2(client, sql1, sql2=sql2)
    client.clean(database_name)


def test_jsonpath_less_than_schema_with_columns():
    """
    {
    "title": "test_jsonpath_less_than_schema_without_columns",
    "describe": "json object导入，指定的jsonpath中的列比schema中的列少，指定columns正确(jsonpath中的列与columns中相同)，导入成功",
    "tag": "p1,fuzz"
    }
    """
    """指定的jsonpath中的列比schema中的列少，指定columns正确"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_list = [('k_add', 'INT')] + DATA.partition_column_list + [('v_add', 'INT', "REPLACE_IF_NOT_NULL", "100")]
    ret = client.create_table(table_name, column_list, distribution_info=DATA.hash_distribution_info, set_null=True)
    assert ret, 'create table failed'
    local_data_file = local_data_file = '%s/data/LOAD/json_object_basic.json' % file_dir
    jsonpaths = '["$.k_add", "$.k1", "$.k2", "$.k3", "$.k4", "$.k5", "$.v1", "$.v2", "$.v3", "$.v4", "$.v5"]'
    columns = ['k_add', 'k1', 'k2', 'k3', 'k4', 'k5', 'v1', 'v2', 'v3', 'v4', 'v5']
    ret = client.stream_load(table_name, local_data_file, column_name_list=columns, 
                             format='json', jsonpaths=jsonpaths)
    assert ret, 'stream load failed'
    sql1 = 'select * from %s' % table_name
    sql2 = 'select null, 1, 10, 100, 1000, cast("2011-01-01 00:00:00" as datetime), ' \
           'cast("2010-01-01" as date), "t", "ynqnzeowymt", 38.638843, 180.998031, null, 100'
    common.check2(client, sql1, sql2=sql2)
    client.clean(database_name)


def test_jsonpath_more_than_schema_with_columns():
    """
    {
    "title": "test_jsonpath_more_than_schema_with_columns",
    "describe": "json object导入，指定的jsonpath中的列比schema中的列多，指定columns正确(jsonpath中的列与columns相同)，导入成功",
    "tag": "p1,fuzz"
    }
    """
    """指定的jsonpath中的列比schema中的列多，指定columns正确"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_list = DATA.partition_column_list
    ret = client.create_table(table_name, column_list, distribution_info=DATA.hash_distribution_info, set_null=True)
    assert ret, 'create table failed'
    local_data_file = local_data_file = '%s/data/LOAD/json_object_basic.json' % file_dir
    columns = 'k_add, k1, k2, k3, k4, k5, v1, v2, v3, v4, v5, v6, v_add'
    jsonpaths = '["$.k_add", "$.k1", "$.k2", "$.k3", "$.k4", "$.k5", "$.v1", "$.v2", ' \
                 '"$.v3", "$.v4", "$.v5", "$.v6", "$.v_add"]'
    ret = client.stream_load(table_name, local_data_file,
                             format='json', jsonpaths=jsonpaths, columns=columns)
    assert ret
    sql1 = 'select * from %s' % table_name
    sql2 = 'select 1, 10, 100, 1000, cast("2011-01-01 00:00:00" as datetime), ' \
           'cast("2010-01-01" as date), "t", "ynqnzeowymt", 38.638843, 180.998031, 7395.231067'
    common.check2(client, sql1, sql2=sql2)
    client.clean(database_name)


def test_jsonpath_diff_with_columns():
    """
    {
    "title": "test_jsonpath_diff_with_columns",
    "describe": "json object导入，指定的jsonpath中的列比columns中的列多或少，导入失败",
    "tag": "p1,fuzz"
    }
    """
    """指定的jsonpath中的列比schema中的列少，指定columns正确"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_list = DATA.partition_column_list
    ret = client.create_table(table_name, column_list, distribution_info=DATA.hash_distribution_info, set_null=True)
    assert ret, 'create table failed'
    local_data_file = local_data_file = '%s/data/LOAD/json_object_basic.json' % file_dir
    columns = 'k1, k2, k3, k4, k5, v1, v2, v3, v4, v5, v6'
    jsonpaths = '["$.k_add", "$.k1", "$.k2", "$.k3", "$.k4", "$.k5", "$.v1", "$.v2", "$.v3", "$.v4", "$.v5", "$.v6"]'
    ret = client.stream_load(table_name, local_data_file,
                             format='json', jsonpaths=jsonpaths, columns=columns)
    assert ret == 0
    local_data_file = local_data_file = '%s/data/LOAD/json_object_basic.json' % file_dir
    columns = 'k1, k2, k3, k4, k5, v1, v2, v3, v4, v5, v6'
    jsonpaths = '["$.k1", "$.k2", "$.k3", "$.k4", "$.k5", "$.v1", "$.v2", "$.v3", "$.v4", "$.v5"]'
    ret = client.stream_load(table_name, local_data_file,
                             format='json', jsonpaths=jsonpaths, columns=columns)
    assert ret
    sql1 = 'select * from %s' % table_name
    sql2 = 'select 1, 10, 100, 1000, cast("2011-01-01 00:00:00" as datetime), ' \
           'cast("2010-01-01" as date), "t", "ynqnzeowymt", 38.638843, 180.998031, null'
    common.check2(client, sql1, sql2=sql2)
    client.clean(database_name)


def test_columns_without_jsonpath():
    """
    {
    "title": "test_columns_without_jsonpath",
    "describe": "json object导入，不指定jsonpath中，指定columns，columns中的列含有表达式，导入失败",
    "tag": "p1,fuzz"
    }
    """
    """指定的jsonpath中的列比schema中的列少，指定columns正确"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_list = DATA.partition_column_list
    ret = client.create_table(table_name, column_list, distribution_info=DATA.hash_distribution_info, set_null=True)
    assert ret, 'create table failed'
    local_data_file = local_data_file = '%s/data/LOAD/json_object_basic.json' % file_dir
    columns = 'k_add, k1, k2, k3, k4, k5, v1, v2, v3, v4, v5, v6'
    jsonpaths = '["$.k_add", "$.k1", "$.k2", "$.k3", "$.k4", "$.k5", "$.v1", "$.v2", ' \
                 '"$.v3", "$.v4", "$.v5", "$.v6", "$.v_add"]'
    ret = client.stream_load(table_name, local_data_file,
                             format='json', jsonpaths=jsonpaths)
    assert ret == 0
    local_data_file = local_data_file = '%s/data/LOAD/json_object_basic.json' % file_dir
    columns = 'k1, k2, k3, k4, k5, v1, v2, v3, v4, v5, v6'
    jsonpaths = '["$.k1", "$.k2", "$.k3", "$.k4", "$.k5", "$.v1", "$.v2", "$.v3", "$.v4", "$.v5"]'
    ret = client.stream_load(table_name, local_data_file,
                             format='json', jsonpaths=jsonpaths)
    assert ret
    sql1 = 'select * from %s' % table_name
    sql2 = 'select 1, 10, 100, 1000, cast("2011-01-01 00:00:00" as datetime), ' \
           'cast("2010-01-01" as date), "t", "ynqnzeowymt", 38.638843, 180.998031, null'
    common.check2(client, sql1, sql2=sql2)
    client.clean(database_name)


def test_json_object_complicate_type_1():
    """
    {
    "title": "test_json_object_complicate_type_1",
    "describe": "导入object中含有复杂类型，如[], {}，对应schema中的char类型，预期导入成功",
    "tag": "p1,fuzz"
    }
    """
    """
    导入object中含有复杂类型，如[], {}，对应schema中的char类型，预期导入成功
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_list = [('k1', 'CHAR(20)'), ('k2', 'CHAR(100)'), ('k3', 'CHAR(50)'), ('k4', 'INT')]
    ret = client.create_table(table_name, column_list, distribution_info=DATA.hash_distribution_info)
    assert ret, 'create table failed'
    local_data_file = '%s/data/LOAD/object.json' % file_dir
    columns = ['k1', 'k2', 'k3', 'c1', 'c2', 'k4=c1+c2']
    jsonpaths = '["$.data_file_type", "$.delta[0].segment_group", "$.selectivity", ' \
                 '"$.selectivity[0]", "$.selectivity[1]"]'
    ret = client.stream_load(table_name, local_data_file, column_name_list=columns,
                             format='json', jsonpaths=jsonpaths)
    assert ret
    sql1 = 'select * from %s' % table_name
    sql2 = 'select "COLUMN_ORIENTED_FILE",' \
           ' \'[{"segment_group_id":0,"num_segments":0,"index_size":0,"data_size":0,"num_rows":0,"empty":true}]\', ' \
           '"[1,1,1,1,1,1,1,1,1]", 2'
    common.check2(client, sql1, sql2=sql2)
    client.clean(database_name)


def test_json_object_complicate_type_2():
    """
    {
    "title": "test_json_object_complicate_type_2",
    "describe": "导入object中含有复杂类型，如[], {}，对应schema中的varchar类型，预期导入成功",
    "tag": "p1,fuzz"
    }
    """
    """
    导入object中含有复杂类型，如[], {}，对应schema中的varchar类型，预期导入成功
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_list = [('k1', 'VARCHAR(20)'), ('k2', 'VARCHAR(200)'), ('k3', 'VARCHAR(50)'), ('k4', 'INT')]
    ret = client.create_table(table_name, column_list, distribution_info=DATA.hash_distribution_info)
    assert ret, 'create table failed'
    local_data_file = '%s/data/LOAD/object.json' % file_dir
    columns = ['k1', 'k2', 'k3', 'k4']
    jsonpaths = '["$.column[0].is_allow_null", "$.column[1]", "$.schema_hash", "$.tablet_id"]'
    ret = client.stream_load(table_name, local_data_file, column_name_list=columns,
                             format='json', jsonpaths=jsonpaths)
    assert ret
    sql1 = 'select * from %s' % table_name
    sql2 = 'select "1", \'{"name":"k2","type":"SMALLINT","aggregation":"NONE","length":2,"is_key":true,' \
           '"index_length":2,"is_allow_null":true,"unique_id":1,"is_root_column":true}\', "368169781", 15007'
    common.check2(client, sql1, sql2=sql2)
    client.clean(database_name)


def test_jsonroot():
    """
    {
    "title": "test_jsonroot",
    "describe": "导入object中含有复杂类型，验证jsonroot生效",
    "tag": "p1,fuzz"
    }
    """
    """验证jsonroot"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_list  = [('name', 'CHAR(5)'), ('type', 'VARCHAR(20)'), ('aggregation', 'CHAR(5)'), 
                    ('length', 'INT'), ('is_root_column', 'BOOLEAN')]
    ret = client.create_table(table_name, column_list, 
                              distribution_info=palo_client.DistributionInfo('Hash(name)', 5))
    assert ret, 'create table failed'
    local_data_file = '%s/data/LOAD/object.json' % file_dir
    json_root = "$.column"
    strip_outer_array = 'true'
    ret = client.stream_load(table_name, local_data_file,
                             format='json', json_root=json_root, strip_outer_array=strip_outer_array)
    assert ret
    sql1 = 'select * from %s' % table_name
    sql2 = 'select "k1", "INT", "NONE", 4, 1 union select "k2", "SMALLINT", "NONE", ' \
           '2, 1 union select "k9", "FLOAT", "SUM", 4, 1'
    common.check2(client, sql1, sql2=sql2, forced=True)
    client.clean(database_name)


def test_json_all_param():
    """
    {
    "title": "test_json_all_param",
    "describe": "尽量覆盖json所有的参数，导入验证正确性",
    "tag": "p1,fuzz"
    }
    """
    """尽量覆盖json所有的参数"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    column_list  = [('start_version', 'INT'), ('end_version', 'INT'), ('version_hash', 'LARGEINT'),
                    ('create_time', 'DATETIME'), ('segment_group', 'VARCHAR(100)'), ('num_size', 'INT')]
    partition_info = palo_client.PartitionInfo('start_version', ['p1', 'p2', 'p3'], ['1', '3', '5'])
    ret = client.create_table(table_name, column_list, 
                              distribution_info=palo_client.DistributionInfo('Hash(start_version)', 5),
                              partition_info=partition_info)
    assert ret, 'create table failed'
    local_data_file = '%s/data/LOAD/object.json' % file_dir
    json_root = "$.delta"
    strip_outer_array = 'true'
    jsonpaths = '["$.start_version", "$.end_version", "$.version_hash", "$.creation_time", ' \
                '"$.segment_group[0].column_pruning[0]", "$.segment_group[0].num_rows"]'
    timezone = '+00:00'
    timeout = '300'
    columns = 'start_version, end_version, version_hash, create_timestamp, segment_group, ' \
              'num_size, create_time=from_unixtime(create_timestamp)'
    partitions = 'p1,p2'
    where = 'version_hash=6029593056193292005'
    max_filter_ratio = '0.5'
    ret = client.stream_load(table_name, local_data_file, format='json', max_filter_ratio=max_filter_ratio, 
                             jsonpaths=jsonpaths, json_root=json_root, strip_outer_array=strip_outer_array,
                             timezone=timezone, timeout=timeout, columns=columns, partitions=partitions,
                             where=where)
    assert ret
    sql1 = 'select * from %s' % table_name
    sql2 = 'select 2, 2, "6029593056193292005", cast("2019-03-21 07:10:55" as datetime), ' \
           '\'{"min":"LTEyOA==","max":"MTI2","null_flag":false}\', 3315'
    common.check2(client, sql1, sql2=sql2, forced=True)
    client.clean(database_name)


if __name__ == '__main__':
    setup_module()
