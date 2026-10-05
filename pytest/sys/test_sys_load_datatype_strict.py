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
#   @file test_sys_load_datatype_strict.py
#   @date 2019-05-29 16:12:47
#   @brief This file is a test file for load data
#
#############################################################################

"""
测试load
"""

import os
import sys
file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(file_dir)
file_dir = os.path.abspath(os.path.dirname(__file__))
from data import schema as DATA
from lib import palo_config
from lib import palo_client
from lib import util

client = None

config = palo_config.config
fe_http_port = config.fe_http_port
broker_info = palo_config.broker_info


def setup_module():
    """
    setUp
    """
    global client
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password)
    client.init()


def check_date(check_file, table, column_list):
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
    

def base(database_name, table_name, column_list, local_data_file, hdfs_data_file, 
         expect_data_file='a', sql=None, load_column_name=None, convert_check=None):
    """
    清理环境、创建数据库，建表，导数据，校验数据
    """
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)
    if sql is None:
        sql = 'select * from %s order by 1, 2'
    # todo: 流式导入
    #stream_table = table_name + '_stream'
    #ret = client.create_table(stream_table, column_list, set_null=True,
    #                        distribution_info=distribution_info, keys_desc=key_type)
    #assert ret
    #ret = client.stream_load(stream_table, local_data_file, max_filter_ratio=0.1, port=fe_http_port)
    #assert ret
    # broker 导入 null and strict mode true
    broker_table = table_name + '_null_s'
    ret = client.create_table(broker_table, column_list, set_null=True,
                              distribution_info=DATA.hash_distribution_info, keys_desc=DATA.duplicate_key)
    assert ret
    load_data_file = palo_client.LoadDataInfo(hdfs_data_file, broker_table, column_name_list=load_column_name)
    broker_label = util.get_label()
    ret = client.batch_load(broker_label, load_data_file, max_filter_ratio=1, broker=broker_info,
                            strict_mode=True)
    assert ret
    # broker导入 null and strict mode false
    broker_table_ns = table_name + '_null_ns'
    ret = client.create_table(broker_table_ns, column_list, set_null=True,
                              distribution_info=DATA.hash_distribution_info, keys_desc=DATA.duplicate_key)
    assert ret
    load_data_file = palo_client.LoadDataInfo(hdfs_data_file, broker_table_ns, column_name_list=load_column_name)
    broker_label = util.get_label()
    ret = client.batch_load(broker_label, load_data_file, max_filter_ratio=1, broker=broker_info,
                            strict_mode=False)
    assert ret
    # broker导入 not null
    broker_table_not_null = table_name + '_not_null'
    ret = client.create_table(broker_table_not_null, column_list, set_null=False,
                              distribution_info=DATA.hash_distribution_info, keys_desc=DATA.duplicate_key)
    assert ret
    load_data_file = palo_client.LoadDataInfo(hdfs_data_file, broker_table_not_null, column_name_list=load_column_name)
    broker_label = util.get_label()
    ret = client.batch_load(broker_label, load_data_file, max_filter_ratio=1, broker=broker_info,
                            strict_mode=False)
    assert ret
    # 结果校验
    client.wait_table_load_job()
    if convert_check is True:
        check_date(expect_data_file + '_null_s.data', broker_table, column_list)
        check_date(expect_data_file + '_null_ns.data', broker_table_ns, column_list)
        check_date(expect_data_file + '_not_null.data', broker_table_not_null,column_list)
    else:
        assert client.verify(expect_data_file + '_null_s.data', broker_table)
        assert client.verify(expect_data_file + '_null_ns.data', broker_table_ns)
        assert client.verify(expect_data_file + '_not_null.data', broker_table_not_null)
    print('checkok\n')
    client.clean(database_name)


def test_tinyint():
    """
    {
    "title": "test_sys_load_datatype_strict.test_tinyint",
    "describe": "TINYINT类型作HASH列, DUPLICATE KEY, test strict_mode",
    "tag": "p1,fuzz,function"
    }
    """
    """
    TINYINT类型作HASH列, DUPLICATE KEY, test strict_mode
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.tinyint_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_number.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/load/test_number.data')
    expect_data_file = '%s/data/LOAD/expe_tinyint' % file_dir
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, expect_data_file)


def test_smallint():
    """
    {
    "title": "test_sys_load_datatype_strict.test_smallint",
    "describe": "SMALLINT类型作HASH列, DUPLICATE KEY, test strict mode",
    "tag": "p1,fuzz,function"
    }
    """
    """
    SMALLINT类型作HASH列, DUPLICATE KEY, test strict mode
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.smallint_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_number.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/load/test_number.data')
    expect_data_file = '%s/data/LOAD/expe_smallint' % file_dir
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, expect_data_file)


def test_int():
    """
    {
    "title": "test_sys_load_datatype_strict.test_int",
    "describe": "INT类型作HASH列, DUPLICATE KEY, test strict mode",
    "tag": "p1,fuzz,function"
    }
    """
    """
    INT类型作HASH列, DUPLICATE KEY, test strict mode
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.int_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_number.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/load/test_number.data')
    expect_data_file = '%s/data/LOAD/expe_int' % file_dir
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, expect_data_file)


def test_bigint():
    """
    {
    "title": "test_sys_load_datatype_strict.test_bigint",
    "describe": "BIGINT类型做HASH列, DUPLICATE KEY, test strict mode",
    "tag": "p1,fuzz,function"
    }
    """
    """
    BIGINT类型做HASH列, DUPLICATE KEY, test strict mode
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.bigint_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_number.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/load/test_number.data')
    expect_data_file = '%s/data/LOAD/expe_bigint' % file_dir
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, expect_data_file)


def test_largeint():
    """
    {
    "title": "test_sys_load_datatype_strict.test_largeint",
    "describe": "LARGEINT类型作HASH列, DUPLICATE KEY, test strict mode",
    "tag": "p1,fuzz,function"
    }
    """
    """
    LARGEINT类型作HASH列, DUPLICATE KEY, test strict mode
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.largeint_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_number.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/load/test_number.data')
    expect_data_file = '%s/data/LOAD/expe_largeint' % file_dir
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, expect_data_file)


def test_float_int():
    """
    {
    "title": "test_sys_load_datatype_strict.test_float_int",
    "describe": "INT类型作AGGREGATE KEY，FLOAT类型作VALUE列, test strict mode",
    "tag": "p1,fuzz,function"
    }
    """
    """
    INT类型作AGGREGATE KEY，FLOAT类型作VALUE列, test strict mode
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.float_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_number.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/load/test_number.data')
    expect_data_file = '%s/data/LOAD/expe_float_int' % file_dir
    load_column_name = ['v1', 'k1', 'v2', 'v3', 'v4']
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, expect_data_file, 
         load_column_name=load_column_name)


def test_double_int():
    """
    {
    "title": "test_sys_load_datatype_strict.test_double_int",
    "describe": "INT类型作AGGREGATE KEY，DOUBLE类型作VALUE列, test strict mode",
    "tag": "p1,fuzz,function"
    }
    """
    """
    INT类型作AGGREGATE KEY，DOUBLE类型作VALUE列, test strict mode
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.double_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_number.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/load/test_number.data')
    expect_data_file = '%s/data/LOAD/expe_double_int' % file_dir
    load_column_name = ['v1', 'k1', 'v2', 'v3', 'v4']
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, expect_data_file, 
         load_column_name=load_column_name)


def test_date():
    """
    {
    "title": "test_sys_load_datatype_strict.test_date",
    "describe": "DATE类型作HASH列, DUPLICATE KEY, test strict mode",
    "tag": "p1,fuzz,function"
    }
    """
    """
    DATE类型作HASH列, DUPLICATE KEY, test strict mode
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.date_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_number.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/load/test_number.data')
    expect_data_file = '%s/data/LOAD/expe_date' % file_dir
    load_column_name = ['k1', 'v1', 'v2', 'v3', 'v4']
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, expect_data_file,
         load_column_name=load_column_name, convert_check=True)


def test_datetime():
    """
    {
    "title": "test_sys_load_datatype_strict.test_datetime",
    "describe": "DATETIME类型作HASH列, DUPLICATE KEY, test strict mode",
    "tag": "p1,fuzz,function"
    }
    """
    """
    DATETIME类型作HASH列, DUPLICATE KEY, test strict mode
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.datetime_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_number.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/load/test_number.data')
    expect_data_file = '%s/data/LOAD/expe_datetime' % file_dir
    load_column_name = ['k1', 'v1', 'v2', 'v3', 'v4']
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, expect_data_file,
         load_column_name=load_column_name, convert_check=True)


def test_decimal_least():
    """
    {
    "title": "test_sys_load_datatype_strict.test_decimal_least",
    "describe": "DECIMAL(1,0)类型作HASH列, DUPLICATE KEY, test strict mode",
    "tag": "p1,fuzz,function"
    }
    """
    """
    DECIMAL(1,0)类型作HASH列, DUPLICATE KEY, test strict mode
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.decimal_least_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_number.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/load/test_number.data')
    expect_data_file = "%s/data/LOAD/expe_decimal_least" % file_dir
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, expect_data_file)


def test_decimal_normal():
    """
    {
    "title": "test_sys_load_datatype_strict.test_decimal_normal",
    "describe": "DECIMAL(10,5)类型作HASH列, DUPLICATE KEY, test strick mode",
    "tag": "p1,fuzz,function"
    }
    """
    """
    DECIMAL(10,5)类型作HASH列, DUPLICATE KEY, test strick mode
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.decimal_normal_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_number.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/load/test_number.data')
    expect_data_file = "%s/data/LOAD/expe_decimal_normal" % file_dir
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, expect_data_file)


def test_decimal_most():
    """
    {
    "title": "test_sys_load_datatype_strict.test_decimal_most",
    "describe": "DECIMAL(27,9)类型作HASH列, DUPLICATE KEY, test strict mode",
    "tag": "p1,fuzz,function"
    }
    """
    """
    DECIMAL(27,9)类型作HASH列, DUPLICATE KEY, test strict mode
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.decimal_most_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_number.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/load/test_number.data')
    expect_data_file = "%s/data/LOAD/expe_decimal_most" % file_dir
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, expect_data_file)


def test_char_least():
    """
    {
    "title": "test_sys_load_datatype_strict.test_char_least",
    "describe": "CHAR类型作HASH列, DUPLICATE KEY, test strict mode",
    "tag": "p1,fuzz,function"
    }
    """
    """
    CHAR类型作HASH列, DUPLICATE KEY, test strict mode
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.char_least_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_char.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/load/test_char.data')
    expect_data_file = '%s/data/LOAD/expe_char_least' % file_dir
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, expect_data_file)


def test_char_normal():
    """
    {
    "title": "test_sys_load_datatype_strict.test_char_normal",
    "describe": "CHAR_NORMAL类型做HASH列，考虑特殊字符, DUPLICATE KEY, test strict mode",
    "tag": "p1,fuzz,function"
    }
    """
    """
    CHAR_NORMAL类型做HASH列，考虑特殊字符, DUPLICATE KEY, test strict mode
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.char_normal_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_char.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/load/test_char.data')
    expect_data_file = '%s/data/LOAD/expe_char_normal' % file_dir
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, expect_data_file)


def test_char_most():
    """
    {
    "title": "test_sys_load_datatype_strict.test_char_most",
    "describe": "CHAR_MOST 类型做HASH列, DUPLICATE KEY, test strict mode",
    "tag": "p1,fuzz,function"
    }
    """
    """
    CHAR_MOST 类型做HASH列, DUPLICATE KEY, test strict mode
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.char_most_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_char.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/load/test_char.data')
    expect_data_file = '%s/data/LOAD/expe_char_most' % file_dir
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, expect_data_file)


def test_varchar_least():
    """
    {
    "title": "test_sys_load_datatype_strict.test_varchar_least",
    "describe": "VARCHAR_LEAST类型做HASH列, DUPLICATE KEY, test strict mode",
    "tag": "p1,fuzz,function"
    }
    """
    """
    VARCHAR_LEAST类型做HASH列, DUPLICATE KEY, test strict mode
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.varchar_least_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_char.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/load/test_char.data')
    expect_data_file = '%s/data/LOAD/expe_varchar_least' % file_dir
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, expect_data_file)


def test_varchar_normal():
    """
    {
    "title": "test_sys_load_datatype_strict.test_varchar_normal",
    "describe": "VARCHAR_NORMAL类型做HASH列, DUPLICATE KEY, test strict mode",
    "tag": "p1,fuzz,function"
    }
    """
    """
    VARCHAR_NORMAL类型做HASH列, DUPLICATE KEY, test strict mode
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.varchar_normal_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_char.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/load/test_char.data')
    expect_data_file = '%s/data/LOAD/expe_varchar_normal' % file_dir
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, expect_data_file)


def test_varchar_most():
    """
    {
    "title": "test_sys_load_datatype_strict.test_varchar_most",
    "describe": "VARCHAR_MOST类型做HASH列, DUPLICATE KEY, test strict mode",
    "tag": "p1,fuzz,function"
    }
    """
    """
    VARCHAR_MOST类型做HASH列, DUPLICATE KEY, test strict mode
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.varchar_most_column_no_agg_list
    local_data_file = "%s/data/LOAD/test_char.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/load/test_char.data')
    expect_data_file = '%s/data/LOAD/expe_varchar_most' % file_dir
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, expect_data_file)
    
    
def teardown_module():
    """
    tearDown
    """
    print('end')


if __name__ == '__main__':
    setup_module()
    test_datetime()

