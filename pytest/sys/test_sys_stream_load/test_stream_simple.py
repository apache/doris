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
file: stream_test_simple.py
测试stream load的基本数据类型
"""

import os
import sys
import time
import pytest
file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(file_dir)
file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(file_dir)

from data import schema as DATA
import palo_config
import palo_client
from lib import util

client = None

config = palo_config.config
fe_http_port = config.fe_http_port
storage_type = DATA.storage_type


def setup_module():
    """
    setUp
    """
    global client
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port, user=config.fe_user, 
                                    password=config.fe_password)
    client.init()
    client.set_variables('enable_insert_strict', 'false')


def base(database_name, table_name, column_list, data_file, expected_data_file, key_type, 
         distribution_info, sql1=None, sql2=None):
    """
    清理环境、创建数据库，建表，导数据，校验数据
    """
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)
    
    ret = client.create_table(table_name, column_list, 
                              distribution_info=distribution_info, keys_desc=key_type)
    assert ret
    ret = client.stream_load(table_name, data_file, max_filter_ratio=0.1, port=fe_http_port)
    assert ret
    retry_times = 60
    ret = False
    while not ret and retry_times > 0:
        ret = client.verify(expected_data_file, table_name)
        retry_times -= 1
        if ret:
            break
        time.sleep(1)
    assert ret

    insert_table_name = 'insert_' + table_name
    ret = client.create_table(insert_table_name, column_list, 
                              distribution_info=distribution_info, keys_desc=key_type)
    assert ret
    insert_sql = 'insert into %s select * from %s' % (insert_table_name, table_name)
    ret = client.execute(insert_sql)
    assert ret == ()
    if sql1 is None:
        sql1 = 'select * from %s' % table_name
    if sql2 is None:
        sql2 = 'select * from %s' % insert_table_name
    ret1 = client.execute(sql1)
    ret2 = client.execute(sql2)
    retry_times = 60
    while ret2 == () and retry_times > 0:
        print('select streaming insert retry...')
        ret2 = client.execute(sql2)
        retry_times -= 1
        time.sleep(1)
    util.check(ret1, ret2, True)
    client.clean(database_name)


def base_special_str(database_name, table_name, column_list, data_file, distribution_info):
    """
    针对全角字符等特殊字符，清理环境、创建数据库，建表，导数据，校验数据
    """
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)
    ret = client.create_table(table_name, column_list,
                              distribution_info=distribution_info)
    assert ret
    load_label = str(time.time()).replace('.', '')
    ret = client.stream_load(table_name, data_file, max_filter_ratio=0.1, port=fe_http_port)
    assert ret
    time.sleep(5)


def test_hash_tinyint():
    """
    {
    "title": "test_stream_simple.test_hash_tinyint",
    "describe": "TINYINT类型作HASH列",
    "tag": "function,p1,fuzz"
    }
    """
    """
    TINYINT类型作HASH列
    """
    database_name, table_name, rollup_table_name = util.gen_name_list() 
    column_list = DATA.tinyint_column_list
    distribution_info = DATA.hash_distribution_info
    data_file = "%s/data/STREAM_LOAD/test_hash_tinyint.data" % file_dir
    expected_data_file = '%s/data/STREAM_LOAD/expe_test_hash_tinyint.data' % file_dir
    key_type = DATA.aggregate_key
    base(database_name, table_name, column_list, data_file, expected_data_file, key_type, 
         distribution_info)
    column_list = DATA.tinyint_column_no_agg_list
    key_type = DATA.duplicate_key
    expect_file = "%s/data/STREAM_LOAD/expe_test_hash_tinyint_dup.data" % file_dir
    base(database_name, table_name, column_list, data_file, expect_file, key_type, 
         distribution_info)

    # key_type = DATA.unique_key
    # expect_file = "%s/data/STREAM_LOAD/expe_test_hash_tinyint_uniq.data" % file_dir
    # base(database_name, table_name, column_list, data_file, expect_file, key_type, 
    #      distribution_info)


def test_hash_smallint():
    """
    {
    "title": "test_stream_simple.test_hash_smallint",
    "describe": "SMALLINT类型作HASH列",
    "tag": "function,p1,fuzz"
    }
    """
    """
    SMALLINT类型作HASH列
    """
    database_name, table_name, rollup_table_name = util.gen_name_list() 
    column_list = DATA.smallint_column_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.aggregate_key
    data_file = "%s/data/STREAM_LOAD/test_hash_smallint.data" % file_dir
    expected_data_file = '%s/data/STREAM_LOAD/expe_test_hash_smallint.data' % file_dir
    base(database_name, table_name, column_list, data_file, expected_data_file, key_type, 
         distribution_info)

    column_list = DATA.smallint_column_no_agg_list
    expect_file = "%s/data/STREAM_LOAD/expe_test_hash_smallint_dup.data" % file_dir
    key_type = DATA.duplicate_key
    base(database_name, table_name, column_list, data_file, expect_file, key_type, 
         distribution_info)

    # expect_file = "%s/data/STREAM_LOAD/expe_test_hash_smallint_uniq.data" % file_dir
    # key_type = DATA.unique_key
    # base(database_name, table_name, column_list, data_file, expect_file, key_type, 
    #      distribution_info)


def test_hash_int():
    """
    {
    "title": "test_stream_simple.test_hash_int",
    "describe": "INT类型作HASH列",
    "tag": "function,p1,fuzz"
    }
    """
    """
    INT类型作HASH列
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.int_column_list
    distribution_info = DATA.hash_distribution_info
    data_file = "%s/data/STREAM_LOAD/test_hash_int.data" % file_dir
    key_type = DATA.aggregate_key
    expected_data_file = '%s/data/STREAM_LOAD/expe_test_hash_int.data' % file_dir
    base(database_name, table_name, column_list, data_file, expected_data_file, key_type, 
         distribution_info)

    column_list = DATA.int_column_no_agg_list
    expect_file = "%s/data/STREAM_LOAD/expe_test_hash_int_dup.data" % file_dir
    key_type = DATA.duplicate_key
    base(database_name, table_name, column_list, data_file, expect_file, key_type, 
         distribution_info)

    # expect_file = "%s/data/STREAM_LOAD/expe_test_hash_int_uniq.data" % file_dir
    # key_type = DATA.unique_key
    # base(database_name, table_name, column_list, data_file, expect_file, key_type, 
    #      distribution_info)


def test_hash_bigint():
    """
    {
    "title": "test_stream_simple.test_hash_bigint",
    "describe": "BIGINT类型做HASH列",
    "tag": "function,p1,fuzz"
    }
    """
    """
    BIGINT类型做HASH列
    """
    database_name, table_name, rollup_table_name = util.gen_name_list() 
    column_list = DATA.bigint_column_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.aggregate_key
    data_file = "%s/data/STREAM_LOAD/test_hash_bigint.data" % file_dir
    expected_data_file = '%s/data/STREAM_LOAD/expe_test_hash_bigint.data' % file_dir
    base(database_name, table_name, column_list, data_file, expected_data_file, key_type, 
         distribution_info)

    column_list = DATA.bigint_column_no_agg_list
    expect_file = "%s/data/STREAM_LOAD/expe_test_hash_bigint_dup.data" % file_dir
    key_type = DATA.duplicate_key
    base(database_name, table_name, column_list, data_file, expect_file, key_type, 
         distribution_info)

    # expect_file = "%s/data/STREAM_LOAD/expe_test_hash_bigint_uniq.data" % file_dir
    # key_type = DATA.unique_key
    # base(database_name, table_name, column_list, data_file, expect_file, key_type, 
    #      distribution_info)


def test_hash_largeint():
    """
    {
    "title": "test_stream_simple.test_hash_largeint",
    "describe": "LARGEINT类型作HASH列",
    "tag": "function,p1,fuzz"
    }
    """
    """
    LARGEINT类型作HASH列
    """
    database_name, table_name, rollup_table_name = util.gen_name_list() 
    column_list = DATA.largeint_column_list
    distribution_info = DATA.hash_distribution_info
    data_file = "%s/data/STREAM_LOAD/test_hash_largeint.data" % file_dir
    expected_data_file = '%s/data/STREAM_LOAD/expe_test_hash_largeint.data' % file_dir
    
    base_special_str(database_name, table_name, column_list, data_file, distribution_info)   
    client_1 = palo_client.get_client(config.fe_host, config.fe_query_port,
               database_name=database_name, user=config.fe_user, password=config.fe_password)
    sql = "select * from %s.%s" % (database_name, table_name)
    results = client_1.execute(sql)
    sorted_results = sorted(results, key=lambda t:t[0])
    
    expected_file = open(expected_data_file, "r")
    assert str(sorted_results) == expected_file.readline().replace('\n', '')
    client.execute('drop database if exists %s' % database_name)

    column_list = DATA.largeint_column_no_agg_list
    # expect_file = '%s/data/STREAM_LOAD/expe_test_hash_largeint_uniq.data' % file_dir
    # key_type = DATA.unique_key
    # base(database_name, table_name, column_list, data_file, expect_file, key_type, 
    #      distribution_info)

    expect_file = '%s/data/STREAM_LOAD/expe_test_hash_largeint_dup.data' % file_dir
    key_type = DATA.duplicate_key
    base(database_name, table_name, column_list, data_file, expect_file, key_type, 
         distribution_info)


def test_float_int():
    """
    {
    "title": "test_stream_simple.test_float_int",
    "describe": "INT类型作KEY，FLOAT类型作VALUE列",
    "tag": "function,p1,fuzz"
    }
    """
    """
    INT类型作KEY，FLOAT类型作VALUE列
    """
    database_name, table_name, rollup_table_name = util.gen_name_list() 
    column_list = DATA.float_column_list
    key_type = DATA.aggregate_key
    distribution_info = DATA.hash_distribution_info
    data_file = "%s/data/STREAM_LOAD/test_float_int.data" % file_dir
    expected_data_file = '%s/data/STREAM_LOAD/expe_test_float_int.data' % file_dir
    base(database_name, table_name, column_list, data_file, expected_data_file, key_type, 
         distribution_info)


def test_double_int():
    """
    {
    "title": "test_stream_simple.test_double_int",
    "describe": "INT类型作KEY，DOUBLE类型作VALUE列",
    "tag": "function,p1,fuzz"
    }
    """
    """
    INT类型作KEY，DOUBLE类型作VALUE列
    """
    database_name, table_name, rollup_table_name = util.gen_name_list() 
    column_list = DATA.double_column_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.aggregate_key
    data_file = "%s/data/STREAM_LOAD/test_double_int.data" % file_dir
    expected_data_file = '%s/data/STREAM_LOAD/expe_test_double_int.data' % file_dir
    base(database_name, table_name, column_list, data_file, expected_data_file, key_type, 
         distribution_info)
    

def test_hash_date():
    """
    {
    "title": "test_stream_simple.test_hash_date",
    "describe": "DATE类型作HASH列",
    "tag": "function,p1,fuzz"
    }
    """
    """
    DATE类型作HASH列
    """
    database_name, table_name, rollup_table_name = util.gen_name_list() 
    column_list = DATA.date_column_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.aggregate_key
    data_file = "%s/data/STREAM_LOAD/test_hash_date.data" % file_dir
    expected_data_file = '%s/data/STREAM_LOAD/expe_test_hash_date.data' % file_dir
    # 如果返回的日期数据为0000-01-01，则ret中为None，datetime.date的年溢出
    # 使用cast转为string进行校验
    sql1 = 'select cast(k1 as string), cast(v1 as string), cast(v2 as string), \
            cast(v3 as string) from %s' % table_name
    sql2 = 'select cast(k1 as string), cast(v1 as string), cast(v2 as string), \
            cast(v3 as string) from insert_%s' % table_name
    base(database_name, table_name, column_list, data_file, expected_data_file, key_type, 
         distribution_info, sql1=sql1, sql2=sql2)

    column_list = DATA.date_column_no_agg_list
    expect_file = "%s/data/STREAM_LOAD/expe_test_hash_date_dup.data" % file_dir
    key_type = DATA.duplicate_key
    base(database_name, table_name, column_list, data_file, expect_file, key_type, 
         distribution_info, sql1=sql1, sql2=sql2)
    
    # expect_file = "%s/data/STREAM_LOAD/expe_test_hash_date_uniq.data" % file_dir
    # key_type = DATA.duplicate_key
    # base(database_name, table_name, column_list, data_file, expect_file, key_type, 
    #     distribution_info)


def test_hash_datetime():
    """
    {
    "title": "test_stream_simple.test_hash_datetime",
    "describe": "DATETIME类型作HASH列",
    "tag": "function,p1,fuzz"
    }
    """
    """
    DATETIME类型作HASH列
    """
    database_name, table_name, rollup_table_name = util.gen_name_list() 
    column_list = DATA.datetime_column_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.aggregate_key
    data_file = "%s/data/STREAM_LOAD/test_hash_datetime.data" % file_dir
    expected_data_file = '%s/data/STREAM_LOAD/expe_test_hash_datetime.data' % file_dir
    
    # 如果返回的日期数据为0000-01-01，则ret中为None，datetime.datetime的年溢出
    # 使用cast转为string进行校验
    sql1 = 'select cast(k1 as string), cast(v1 as string), cast(v2 as string), \
            cast(v3 as string) from %s' % table_name
    sql2 = 'select cast(k1 as string), cast(v1 as string), cast(v2 as string), \
            cast(v3 as string) from insert_%s' % table_name
    base(database_name, table_name, column_list, data_file, expected_data_file, key_type, 
         distribution_info, sql1=sql1, sql2=sql2)

    column_list = DATA.datetime_column_no_agg_list
    expect_file = "%s/data/STREAM_LOAD/expe_test_hash_datetime_dup.data" % file_dir
    key_type = DATA.duplicate_key
    base(database_name, table_name, column_list, data_file, expect_file, key_type, 
         distribution_info, sql1=sql1, sql2=sql2)

    # expect_file = "%s/data/STREAM_LOAD/expe_test_hash_datetime_uniq.data" % file_dir
    # key_type = DATA.unique_key
    # base(database_name, table_name, column_list, data_file, expect_file, key_type, 
    #      distribution_info)


def test_hash_decimal_least():
    """
    {
    "title": "test_stream_simple.test_hash_decimal_least",
    "describe": "DECIMAL(1,0)类型作HASH列",
    "tag": "function,p1,fuzz"
    }
    """
    """
    DECIMAL(1,0)类型作HASH列
    """
    database_name, table_name, rollup_table_name = util.gen_name_list() 
    column_list = DATA.decimal_least_column_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.aggregate_key
    data_file = "%s/data/STREAM_LOAD/test_hash_decimal_least.data" % file_dir
    expected_data_file = '%s/data/STREAM_LOAD/expe_test_hash_decimal_least.data' % file_dir

    sql1 = 'select * from %s where k1 < 0' % table_name
    base(database_name, table_name, column_list, data_file, expected_data_file, key_type, 
         distribution_info, sql1=sql1)

    column_list = DATA.decimal_least_column_no_agg_list
    expect_file = "%s/data/STREAM_LOAD/expe_test_hash_decimal_least_dup.data" % file_dir
    key_type = DATA.duplicate_key
    base(database_name, table_name, column_list, data_file, expect_file, key_type, 
         distribution_info)

    # expect_file = "%s/data/STREAM_LOAD/expe_test_hash_decimal_least.data" % file_dir
    # key_type = DATA.unique_key
    # base(database_name, table_name, column_list, data_file, expect_file, key_type, 
    #      distribution_info)
    

def test_hash_decimal_normal():
    """
    {
    "title": "test_stream_simple.test_hash_decimal_normal",
    "describe": "DECIMAL(10,5)类型作HASH列",
    "tag": "function,p1,fuzz"
    }
    """
    """
    DECIMAL(10,5)类型作HASH列
    """
    database_name, table_name, rollup_table_name = util.gen_name_list() 
    column_list = DATA.decimal_normal_column_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.aggregate_key
    data_file = "%s/data/STREAM_LOAD/test_hash_decimal_normal.data" % file_dir
    expected_data_file = '%s/data/STREAM_LOAD/expe_test_hash_decimal_normal.data' % file_dir
    base(database_name, table_name, column_list, data_file, expected_data_file, key_type, 
         distribution_info)
    
    column_list = DATA.decimal_normal_column_no_agg_list
    expect_file = "%s/data/STREAM_LOAD/expe_test_hash_decimal_normal_dup.data" % file_dir
    key_type = DATA.duplicate_key
    base(database_name, table_name, column_list, data_file, expect_file, key_type, 
         distribution_info)

    # expect_file = "%s/data/STREAM_LOAD/expe_test_hash_decimal_normal_uniq.data" % file_dir
    # key_type = DATA.unique_key
    # base(database_name, table_name, column_list, data_file, expect_file, key_type, 
    #     distribution_info)


def test_hash_decimal_most():
    """
    {
    "title": "test_stream_simple.test_hash_decimal_most",
    "describe": "DECIMAL(27,9)类型作HASH列",
    "tag": "function,p1,fuzz"
    }
    """
    """
    DECIMAL(27,9)类型作HASH列
    """
    database_name, table_name, rollup_table_name = util.gen_name_list() 
    column_list = DATA.decimal_most_column_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.aggregate_key
    data_file = "%s/data/STREAM_LOAD/test_hash_decimal_most.data" % file_dir
    expected_data_file = '%s/data/STREAM_LOAD/expe_test_hash_decimal_most.data' % file_dir
    base(database_name, table_name, column_list, data_file, expected_data_file, key_type, 
         distribution_info)

    column_list = DATA.decimal_most_column_no_agg_list
    expect_file = "%s/data/STREAM_LOAD/expe_test_hash_decimal_most_dup.data" % file_dir
    key_type = DATA.duplicate_key
    base(database_name, table_name, column_list, data_file, expect_file, key_type, 
         distribution_info)

    # expect_file = "%s/data/STREAM_LOAD/expe_test_hash_decimal_most_uniq.data" % file_dir
    # key_type = DATA.unique_key
    # base(database_name, table_name, column_list, data_file, expect_file, key_type, 
    #      distribution_info)
    

def test_hash_char_least():
    """
    {
    "title": "test_stream_simple.test_hash_char_least",
    "describe": "CHAR类型作HASH列",
    "tag": "function,p1,fuzz"
    }
    """
    """
    CHAR类型作HASH列
    """
    database_name, table_name, rollup_table_name = util.gen_name_list() 
    column_list = DATA.char_least_column_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.aggregate_key
    data_file = "%s/data/STREAM_LOAD/test_hash_char_least.data" % file_dir
    expected_data_file = '%s/data/STREAM_LOAD/expe_test_hash_char_least.data' % file_dir
    base(database_name, table_name, column_list, data_file, expected_data_file, key_type, 
         distribution_info)

    column_list = DATA.char_least_column_no_agg_list
    expect_file = "%s/data/STREAM_LOAD/expe_test_hash_char_least.data" % file_dir
    key_type = DATA.duplicate_key
    base(database_name, table_name, column_list, data_file, expect_file, key_type, 
         distribution_info)

    # expect_file = "%s/data/STREAM_LOAD/expe_test_hash_char_least.data" % file_dir
    # key_type = DATA.unique_key
    # base(database_name, table_name, column_list, data_file, expect_file, key_type, 
    #      distribution_info)
    

def test_hash_char_normal():
    """
    {
    "title": "test_stream_simple.test_hash_char_normal",
    "describe": "CHAR_NORMAL类型做HASH列，考虑特殊字符",
    "tag": "function,p1,fuzz"
    }
    """
    """
    CHAR_NORMAL类型做HASH列，考虑特殊字符
    """
    database_name, table_name, rollup_table_name = util.gen_name_list() 
    column_list = DATA.char_normal_column_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.aggregate_key
    data_file = "%s/data/STREAM_LOAD/test_hash_char_normal.data" % file_dir
    expected_data_file = '%s/data/STREAM_LOAD/expe_test_hash_char_normal.data' % file_dir
    base(database_name, table_name, column_list, data_file, expected_data_file, key_type, 
         distribution_info)

    column_list = DATA.char_normal_column_no_agg_list
    expect_file = "%s/data/STREAM_LOAD/expe_test_hash_char_normal.data" % file_dir
    key_type = DATA.duplicate_key
    base(database_name, table_name, column_list, data_file, expect_file, key_type, 
         distribution_info)

    # expect_file = "%s/data/STREAM_LOAD/expe_test_hash_char_normal.data" % file_dir
    # key_type = DATA.unique_key
    # base(database_name, table_name, column_list, data_file, expect_file, key_type, 
    #      distribution_info)
    
    #导入petl不支持的中文、全角字符、"等字符
    special_str_file = "%s/data/STREAM_LOAD/special_str.data" % file_dir
    base_special_str(database_name, table_name, column_list, special_str_file, distribution_info)   
    
    client_1 = palo_client.get_client(config.fe_host, config.fe_query_port, 
               database_name=database_name, user=config.fe_user, password=config.fe_password)
    sql = "select * from %s.%s" % (database_name, table_name)
    results = client_1.execute(sql)
    sorted_results = sorted(results, key=lambda t:t[0])
    assert sorted_results == [(u'!!!!', u'!!!!'), (u'"""""', u'"""""'),
           (u'<<<<<', u'\u300b\u300b\u300b\u300b\u300b\u300b'), (u'\u3001t(((((', u')))))))'),
           (u'\u3001\u3001\u3001\u3001\u3001\u3001', u'$$$$$$$'),
           (u'\u7b80\u4f53', u'\u4e2d\u6587'), (u'\u7b80\u4f53', u'\u4e2d\u6587'),
           (u'\u8f38\u5165\u7c21\u9ad4\u5b57', u'\u8defi\u5728\u7dda\u8f49\u63db'),
           (u'\uff07\uff07\uff07\uff07', u'\uff02\uff02\uff02\uff02\uff3c'),
           (u'\uff0b\uff0b\uff0b\uff0d\uff0d\uff0d', u'\uff09\uff08\uff05\uff05\uff04\uff03'),
           (u'\uff1b\uff07\uff5d\uff5b\uff3b\uff3d', u'\uff0d\uff1d\uff3f\uff02\uff1f'),
           (u'\uff44\uff46\uff41\uff53\uff46\uff41', u'\uff46\uff41\uff53\uff53\uff44\uff46'),
           (u'\uff47\uff41\uff10\uff18', u'\uff11\uff12\uff13\uff14\uff13\uff15'),
           (u'\uff5e\uff01\uff20\uff03\uff04', u'\uff05\uff3e\uff06\uff0a'),
           (u'\uff5e\uff01\uff20\uff03\uff04', u'\uff05\uff3e\uff06\uff0a')]
    client_1.execute('drop database if exists %s' % database_name)


def test_hash_char_most():
    """
    {
    "title": "test_stream_simple.test_hash_char_most",
    "describe": "CHAR_MOST 类型做HASH列",
    "tag": "function,p1,fuzz"
    }
    """
    """
    CHAR_MOST 类型做HASH列
    """
    database_name, table_name, rollup_table_name = util.gen_name_list() 
    column_list = DATA.char_most_column_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.aggregate_key
    data_file = "%s/data/STREAM_LOAD/test_hash_char_most.data" % file_dir
    expected_data_file = '%s/data/STREAM_LOAD/expe_test_hash_char_most.data' % file_dir
    base(database_name, table_name, column_list, data_file, expected_data_file, key_type, 
         distribution_info)

    column_list = DATA.char_most_column_no_agg_list
    expect_file = "%s/data/STREAM_LOAD/expe_test_hash_char_most_dup.data" % file_dir
    key_type = DATA.duplicate_key
    base(database_name, table_name, column_list, data_file, expect_file, key_type, 
         distribution_info)

    # expect_file = "%s/data/STREAM_LOAD/expe_test_hash_char_most.data" % file_dir
    # key_type = DATA.unique_key
    # base(database_name, table_name, column_list, data_file, expect_file, key_type, 
    #      distribution_info)

    #导入petl不支持的中文、全角字符、"等字符
    special_str_file = "%s/data/STREAM_LOAD/special_str.data" % file_dir
    base_special_str(database_name, table_name, column_list, special_str_file, distribution_info)   
    client_1 = palo_client.get_client(config.fe_host, config.fe_query_port,
               database_name=database_name, user=config.fe_user, password=config.fe_password)
    sql = "select * from %s.%s" % (database_name, table_name)
    results = client_1.execute(sql)
    sorted_results = sorted(results, key=lambda t:t[0])
    assert sorted_results == [(u'!!!!', u'!!!!'), (u'"""""', u'"""""'),
           (u'############', u'\uffe5\uffe5\uffe5\uffe5\uffe5\uffe5\uffe5'),
           (u'<<<<<', u'\u300b\u300b\u300b\u300b\u300b\u300b'), (u'\u3001t(((((', u')))))))'),
           (u'\u3001\u3001\u3001\u3001\u3001\u3001', u'$$$$$$$'),
           (u'\u7b80\u4f53', u'\u4e2d\u6587'), (u'\u7b80\u4f53', u'\u4e2d\u6587'),
           (u'\u8f38\u5165\u7c21\u9ad4\u5b57', u'\u8defi\u5728\u7dda\u8f49\u63db'),
           (u'\uff07\uff07\uff07\uff07', u'\uff02\uff02\uff02\uff02\uff3c'),
           (u'\uff0b\uff0b\uff0b\uff0d\uff0d\uff0d', u'\uff09\uff08\uff05\uff05\uff04\uff03'),
           (u'\uff1b\uff07\uff5d\uff5b\uff3b\uff3d', u'\uff0d\uff1d\uff3f\uff02\uff1f'),
           (u'\uff44\uff46\uff41\uff53\uff46\uff41', u'\uff46\uff41\uff53\uff53\uff44\uff46'),
           (u'\uff47\uff41\uff10\uff18', u'\uff11\uff12\uff13\uff14\uff13\uff15'),
           (u'\uff5e\uff01\uff20\uff03\uff04', u'\uff05\uff3e\uff06\uff0a'),
           (u'\uff5e\uff01\uff20\uff03\uff04', u'\uff05\uff3e\uff06\uff0a')]
    client_1.execute('drop database if exists %s' % database_name)
    

def test_hash_varchar_least():
    """
    {
    "title": "test_stream_simple.test_hash_varchar_least",
    "describe": "VARCHAR_LEAST类型做HASH列",
    "tag": "function,p1,fuzz"
    }
    """
    """
    VARCHAR_LEAST类型做HASH列
    """
    database_name, table_name, rollup_table_name = util.gen_name_list() 
    column_list = DATA.varchar_least_column_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.aggregate_key
    data_file = "%s/data/STREAM_LOAD/test_hash_varchar_least.data" % file_dir
    expected_data_file = '%s/data/STREAM_LOAD/expe_test_hash_varchar_least.data' % file_dir
    base(database_name, table_name, column_list, data_file, expected_data_file, key_type, 
         distribution_info)

    column_list = DATA.varchar_least_column_no_agg_list
    expect_file = "%s/data/STREAM_LOAD/expe_test_hash_varchar_least.data" % file_dir
    key_type = DATA.duplicate_key
    base(database_name, table_name, column_list, data_file, expect_file, key_type, 
         distribution_info)

    # expect_file = "%s/data/STREAM_LOAD/expe_test_hash_varchar_least.data" % file_dir
    # key_type = DATA.unique_key
    # base(database_name, table_name, column_list, data_file, expect_file, key_type, 
    #      distribution_info)


def test_hash_varchar_normal():
    """
    {
    "title": "test_stream_simple.test_hash_varchar_normal",
    "describe": "VARCHAR_NORMAL类型做HASH列",
    "tag": "function,p1,fuzz"
    }
    """
    """
    VARCHAR_NORMAL类型做HASH列
    """
    database_name, table_name, rollup_table_name = util.gen_name_list() 
    column_list = DATA.varchar_normal_column_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.aggregate_key
    data_file = "%s/data/STREAM_LOAD/test_hash_varchar_normal.data" % file_dir
    expected_data_file = '%s/data/STREAM_LOAD/expe_test_hash_varchar_normal.data' % file_dir
    base(database_name, table_name, column_list, data_file, expected_data_file, key_type, 
         distribution_info)

    column_list = DATA.varchar_normal_column_no_agg_list
    expect_file = "%s/data/STREAM_LOAD/expe_test_hash_varchar_normal.data" % file_dir
    key_type = DATA.duplicate_key
    base(database_name, table_name, column_list, data_file, expect_file, key_type, 
         distribution_info)

    # expect_file = "%s/data/STREAM_LOAD/expe_test_hash_varchar_normal.data" % file_dir
    # key_type = DATA.unique_key
    # base(database_name, table_name, column_list, data_file, expect_file, key_type, 
    #      distribution_info)

    #导入petl不支持的中文、全角字符、"等字符
    special_str_file = "%s/data/STREAM_LOAD/special_str.data" % file_dir
    base_special_str(database_name, table_name, column_list, special_str_file, distribution_info)   
    
    client_1 = palo_client.get_client(config.fe_host, config.fe_query_port,
               database_name=database_name, user=config.fe_user, password=config.fe_password)
    sql = "select * from %s.%s" % (database_name, table_name)
    results = client_1.execute(sql)
    sorted_results = sorted(results, key=lambda t:t[0])
    assert sorted_results == [(u'!!!!', u'!!!!'), (u'"""""', u'"""""'),
           (u'############', u'\uffe5\uffe5\uffe5\uffe5\uffe5\uffe5\uffe5'),
           (u'<<<<<', u'\u300b\u300b\u300b\u300b\u300b\u300b'), (u'\u3001t(((((', u')))))))'),
           (u'\u3001\u3001\u3001\u3001\u3001\u3001', u'$$$$$$$'),
           (u'\u7b80\u4f53', u'\u4e2d\u6587'), (u'\u7b80\u4f53', u'\u4e2d\u6587'),
           (u'\u8f38\u5165\u7c21\u9ad4\u5b57', u'\u8defi\u5728\u7dda\u8f49\u63db'),
           (u'\uff07\uff07\uff07\uff07', u'\uff02\uff02\uff02\uff02\uff3c'),
           (u'\uff0b\uff0b\uff0b\uff0d\uff0d\uff0d', u'\uff09\uff08\uff05\uff05\uff04\uff03'),
           (u'\uff1b\uff07\uff5d\uff5b\uff3b\uff3d', u'\uff0d\uff1d\uff3f\uff02\uff1f'),
           (u'\uff44\uff46\uff41\uff53\uff46\uff41', u'\uff46\uff41\uff53\uff53\uff44\uff46'),
           (u'\uff47\uff41\uff10\uff18', u'\uff11\uff12\uff13\uff14\uff13\uff15'),
           (u'\uff5e\uff01\uff20\uff03\uff04', u'\uff05\uff3e\uff06\uff0a'),
           (u'\uff5e\uff01\uff20\uff03\uff04', u'\uff05\uff3e\uff06\uff0a')]
    client_1.execute('drop database if exists %s' % database_name)


def test_hash_varchar_most():
    """
    {
    "title": "test_stream_simple.test_hash_varchar_most",
    "describe": "VARCHAR_MOST类型做HASH列",
    "tag": "function,p1,fuzz"
    }
    """
    """
    VARCHAR_MOST类型做HASH列
    """
    database_name, table_name, rollup_table_name = util.gen_name_list() 
    column_list = DATA.varchar_most_column_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.aggregate_key
    #数据文件
    data_part_1 = open("%s/data/STREAM_LOAD/test_hash_varchar_most_1.data" % file_dir).read()
    data_part_2 = open("%s/data/STREAM_LOAD/test_hash_varchar_most_2.data" % file_dir).read()
    str_data = str(data_part_1) + str(data_part_2)
    data = open("%s/data/STREAM_LOAD/test_hash_varchar_most.data" % file_dir, "w")
    data.write("%s" % str_data)
    data.close()    
    
    #校验文件
    expe_part_1 = open("%s/data/STREAM_LOAD/expe_test_hash_varchar_most_1.data" % file_dir).read()
    expe_part_2 = open("%s/data/STREAM_LOAD/expe_test_hash_varchar_most_2.data" % file_dir).read()
    str_expe_data = str(expe_part_1) + str(expe_part_2)
    expe_data = open("%s/data/STREAM_LOAD/expe_test_hash_varchar_most.data" % file_dir, "w")
    expe_data.write("%s" % str_expe_data)
    expe_data.close()
    
    data_file = "%s/data/STREAM_LOAD/test_hash_varchar_most.data" % file_dir
    expected_data_file = '%s/data/STREAM_LOAD/expe_test_hash_varchar_most.data' % file_dir
    # bulk load error urloperror
    base(database_name, table_name, column_list, data_file, expected_data_file, key_type, 
         distribution_info)

    column_list = DATA.varchar_most_column_no_agg_list
    key_type = DATA.duplicate_key
    expected_data_file = '%s/data/STREAM_LOAD/expe_test_hash_varchar_most.data' % file_dir
    base(database_name, table_name, column_list, data_file, expected_data_file, key_type, 
         distribution_info)

    # key_type = DATA.unique_key
    # base(database_name, table_name, column_list, data_file, expected_data_file, key_type, 
    #      distribution_info)
    
    special_str_file = "%s/data/STREAM_LOAD/special_str.data" % file_dir
    base_special_str(database_name, table_name, column_list, special_str_file, distribution_info)   
    client_1 = palo_client.get_client(config.fe_host, config.fe_query_port,
               database_name=database_name, user=config.fe_user, password=config.fe_password)
    sql = "select * from %s.%s" % (database_name, table_name)
    results = client_1.execute(sql)
    sorted_results = sorted(results, key=lambda t:t[0])
    assert sorted_results == [(u'!!!!', u'!!!!'), (u'"""""', u'"""""'), 
           (u'############', u'\uffe5\uffe5\uffe5\uffe5\uffe5\uffe5\uffe5'), 
           (u'<<<<<', u'\u300b\u300b\u300b\u300b\u300b\u300b'), (u'\u3001t(((((', u')))))))'), 
           (u'\u3001\u3001\u3001\u3001\u3001\u3001', u'$$$$$$$'), 
           (u'\u7b80\u4f53', u'\u4e2d\u6587'), (u'\u7b80\u4f53', u'\u4e2d\u6587'), 
           (u'\u8f38\u5165\u7c21\u9ad4\u5b57', u'\u8defi\u5728\u7dda\u8f49\u63db'), 
           (u'\uff07\uff07\uff07\uff07', u'\uff02\uff02\uff02\uff02\uff3c'), 
           (u'\uff0b\uff0b\uff0b\uff0d\uff0d\uff0d', u'\uff09\uff08\uff05\uff05\uff04\uff03'), 
           (u'\uff1b\uff07\uff5d\uff5b\uff3b\uff3d', u'\uff0d\uff1d\uff3f\uff02\uff1f'), 
           (u'\uff44\uff46\uff41\uff53\uff46\uff41', u'\uff46\uff41\uff53\uff53\uff44\uff46'), 
           (u'\uff47\uff41\uff10\uff18', u'\uff11\uff12\uff13\uff14\uff13\uff15'), 
           (u'\uff5e\uff01\uff20\uff03\uff04', u'\uff05\uff3e\uff06\uff0a'), 
           (u'\uff5e\uff01\uff20\uff03\uff04', u'\uff05\uff3e\uff06\uff0a')]
    client_1.execute('drop database if exists %s' % database_name)

def time_zone_stream_load(database_name, table_name, stream_cloumn_map,\
                          sql, table_schema=None, data_file=None, zone="+00:00"):
    """更改时区验证对stream load的影响,包含建表，导入数据，查询sql，再次导入数据，查询sql
    stream_cloumn_map： stream_cloumn mapping
    sql：stream load完毕要执行的sql语句
    """
    if not data_file:
        data_file = "%s/data/TIME_ZONE/test_time.data" % file_dir
    if not table_schema:
        table_schema = DATA.baseall_column_list
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    ##建表
    # 建表
    partition_name_list = ['partition_a']
    partition_value_list = ['MAXVALUE']

    partition_info = palo_client.PartitionInfo('k2',
                                               partition_name_list, partition_value_list)
    distribution_type_d = 'HASH(k1, k2, k5)'
    distribution_info_d = palo_client.DistributionInfo(distribution_type_d, 3)
    client.create_table(table_name, table_schema, partition_info, distribution_info_d)
    assert client.show_tables(table_name)
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)

    ##默认时区导入数据
    ret = client.stream_load(table_name, data_file, column_name_list=stream_cloumn_map, \
                                  max_filter_ratio=0.1, port=fe_http_port)
    assert ret
    client.use(database_name)
    sql_count = "select count(*) from %s" % table_name
    palo_result = client.execute(sql_count)
    assert palo_result[0][0] > 0, "palo result %s > 0 failed" % palo_result[0][0]
    ###第一次获取数据
    res_before = client.execute(sql)
    assert res_before
    ##设置时区, 第二次获取数据
    ret = client.stream_load(table_name, data_file, column_name_list=stream_cloumn_map, \
                                  max_filter_ratio=0.1, port=fe_http_port, time_zone=zone)
    assert ret
    time.sleep(10)
    res_after = client.execute(sql)
    assert res_after
    return res_before, res_after

def test_set_time_zone_stream_load_now():
    """
    {
    "title": "test_stream_simple.test_set_time_zone_stream_load_now",
    "describe": "设置time_zone变量,验证now() 函数对stream load任务的影响",
    "tag": "function,p1"
    }
    """
    """
    设置time_zone变量,验证now() 函数对stream load任务的影响。
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    set_column_list = ['col', 'k1=year(col)', 'k2=month(col)', "k3=month(col)", \
                        'k4=day(col)', 'k5=7.7', "k6='a'", 'k10=date(col)', "k11=now()", \
                        "k7='k7'", 'k8=month(col)', 'k9=day(col)']
    sql = "select k11 from %s order by k11 limit 1" % table_name
    zone = "+00:00"
    gap = 28800
    res_before, res_after = time_zone_stream_load(database_name, table_name, set_column_list, sql)
    print(res_before, res_after)
    ##两者差值
    util.check2_time_zone(res_before, res_after, gap)
    client.clean(database_name)


def test_set_time_zone_stream_load_FROM_UNIXTIME():
    """
    {
    "title": "test_stream_simple.test_set_time_zone_stream_load_FROM_UNIXTIME",
    "describe": "设置time_zone变量,验证FROM_UNIXTIME() 函数对stream load任务的影响。",
    "tag": "function,p1"
    }
    """
    """
    设置time_zone变量,验证FROM_UNIXTIME() 函数对stream load任务的影响。
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    set_column_list = ['col', 'k1=year(col)', 'k2=month(col)', "k3=month(col)", \
                        'k4=day(col)', 'k5=7.7', "k6='a'", 'k10=date(col)',\
                        "k11=FROM_UNIXTIME(2019, '%Y-%m-%d %H:%i:%s')", \
                        "k7='k7'", 'k8=month(col)', 'k9=day(col)']
    sql = "select k11 from %s order by k11 limit 1" % table_name
    zone = "+00:00"
    gap = 28800
    res_before, res_after = time_zone_stream_load(database_name, table_name, set_column_list, sql)
    print(res_before, res_after)
    ##两者差值
    util.check2_time_zone(res_before, res_after, gap)
    client.clean(database_name)


def test_set_time_zone_stream_load_CONVERT_TZ():
    """
    {
    "title": "test_stream_simple.test_set_time_zone_stream_load_CONVERT_TZ",
    "describe": "设置time_zone变量,验证CONVERT_TZ() 函数对stream load任务的影响。",
    "tag": "function,p1"
    }
    """
    """
    设置time_zone变量,验证CONVERT_TZ() 函数对stream load任务的影响。
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    set_column_list = ['col', 'k1=year(col)', 'k2=month(col)', "k3=month(col),"\
                        "k11=CONVERT_TZ('2019-01-01 12:00:00','+01:00','+10:00')", \
                        'k4=day(col)', 'k5=7.7', "k6='a'", 'k10=date(col)', \
                        "k7='k7'", 'k8=month(col)', 'k9=day(col)']
    sql = "select k11 from %s order by k11 limit 1" % table_name
    zone = "+00:00"
    res_before, res_after = time_zone_stream_load(database_name, table_name, set_column_list, sql)
    print(res_before, res_after)
    ##两者差值
    util.check2_time_zone(res_before, res_after)
    client.clean(database_name)

def test_set_time_zone_stream_load_UNIX_TIMESTAMP():
    """
    {
    "title": "test_stream_simple.test_set_time_zone_stream_load_UNIX_TIMESTAMP",
    "describe": "设置time_zone变量,验证UNIX_TIMESTAMP() 函数对stream load任务的影响, 数值不会有影响。",
    "tag": "function,p1"
    }
    """
    """
    设置time_zone变量,验证UNIX_TIMESTAMP() 函数对stream load任务的影响, 数值不会有影响。
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    set_column_list = ['col', 'k1=year(col)', 'k2=month(col)', "k3=UNIX_TIMESTAMP('2019-01-01 08:00:00')", \
                        'k4=day(col)', 'k5=7.7', "k6='a'", 'k10=date(col)',\
                         "k11='2019-01-01 08:00:00'", \
                        "k7='k7'", 'k8=month(col)', 'k9=day(col)']
    sql = "select k3 from %s order by k3 limit 1" % table_name
    res_before, res_after = time_zone_stream_load(database_name, table_name, set_column_list, sql)
    print(res_before, res_after)
    ##两者差值
    util.check(res_before, res_after)
    client.clean(database_name)


def teardown_module():
    """
    tearDown
    """
    pass


if __name__ == '__main__':
    setup_module()
    test_hash_datetime()
    teardown_module()
