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
#   @file test_sys_load.py
#   @date 2019-05-29 16:12:47
#   @brief This file is a test file for load data
#
#############################################################################

"""
测试load
"""

import os
import sys
import pytest
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


def base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info, sql=None):
    """
    清理环境、创建数据库，建表，导数据，校验数据
    """
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)
    if sql is None:
        sql = 'select * from %s order by 1, 2'
    # 流式导入
    stream_table = table_name + '_stream'
    ret = client.create_table(stream_table, column_list,
                            distribution_info=distribution_info, keys_desc=key_type)
    assert ret
    ret = client.stream_load(stream_table, local_data_file, max_filter_ratio=0.1, port=fe_http_port)
    assert ret
    # 小批量导入
    mini_table = table_name + '_mini'
    ret = client.create_table(mini_table, column_list,
                            distribution_info=distribution_info, keys_desc=key_type)
    assert ret
    ret = client.stream_load(mini_table, local_data_file, max_filter_ratio=0.1)
    assert ret
    # broker 导入
    broker_table = table_name + '_broker'
    ret = client.create_table(broker_table, column_list,
                              distribution_info=distribution_info, keys_desc=key_type)
    assert ret
    load_data_file = palo_client.LoadDataInfo(hdfs_data_file, broker_table)
    broker_label = util.get_label()
    ret = client.batch_load(broker_label, load_data_file, max_filter_ratio=0.1, broker=broker_info)
    assert ret
    client.wait_table_load_job(database_name)
    # 结果校验，不考虑dpp导入结果
    stream_ret = client.execute(sql % stream_table)
    mini_ret = client.execute(sql % mini_table)
    broker_ret = client.execute(sql % broker_table)
    assert len(broker_ret) != 0
    util.check(mini_ret, stream_ret, True)
    util.check(broker_ret, stream_ret, True)
    if len(mini_ret) != 0:
        util.check(broker_ret, mini_ret, True)
    else:
        print('\nbroker load cancelled\n')
    print('checkok')
    client.clean(database_name)


def test_tinyint_agg():
    """
    {
    "title": "test_sys_load.test_tinyint_agg",
    "describe": "TINYINT类型作HASH列, AGGREGATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    TINYINT类型作HASH列, AGGREGATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.tinyint_column_list
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/STREAM_LOAD/test_hash_tinyint.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_tinyint.data')
    key_type = DATA.aggregate_key
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_tinyint_dup():
    """
    {
    "title": "test_sys_load.test_tinyint_dup",
    "describe": "TINYINT类型作HASH列, DUPLICATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    TINYINT类型作HASH列, DUPLICATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/STREAM_LOAD/test_hash_tinyint.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_tinyint.data')
    column_list = DATA.tinyint_column_no_agg_list
    key_type = DATA.duplicate_key
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


@pytest.mark.skip()
def test_tinyint_uniq():
    """
    {
    "title": "test_sys_load.test_tinyint_uniq",
    "describe": "TINYINT类型作HASH列, UNIQUE KEY, uniq使用broker和dpp导入方式,导入的结果每次不一定完全一致,因此忽略",
    "tag": "autotest"
    }
    """
    """
    TINYINT类型作HASH列, UNIQUE KEY
    uniq使用broker和dpp导入方式,导入的结果每次不一定完全一致,因此忽略
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/LOAD/test_hash_tinyint.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_tinyint.data')
    key_type = DATA.unique_key
    column_list = DATA.tinyint_column_no_agg_list
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_smallint_agg():
    """
    {
    "title": "test_sys_load.test_smallint_agg",
    "describe": "SMALLINT类型作HASH列, AGGREGATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    SMALLINT类型作HASH列, AGGREGATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.smallint_column_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.aggregate_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_smallint.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_smallint.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_smallint_dup():
    """
    {
    "title": "test_sys_load.test_smallint_dup",
    "describe": "SMALLINT类型作HASH列, DUPLICATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    SMALLINT类型作HASH列, DUPLICATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.smallint_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.duplicate_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_smallint.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_smallint.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


@pytest.mark.skip()
def test_smallint_uniq():
    """
    {
    "title": "test_sys_load.test_smallint_uniq",
    "describe": "SMALLINT类型作HASH列, UNIQUE KEY",
    "tag": "autotest"
    }
    """
    """
    SMALLINT类型作HASH列, UNIQUE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.smallint_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.unique_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_smallint.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_smallint.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_int_agg():
    """
    {
    "title": "test_sys_load.test_int_agg",
    "describe": "INT类型作HASH列, AGGREGATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    INT类型作HASH列, AGGREGATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.int_column_list
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/STREAM_LOAD/test_hash_int.data" % file_dir
    key_type = DATA.aggregate_key
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_int.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_int_dup():
    """
    {
    "title": "test_sys_load.test_int_dup",
    "describe": "INT类型作HASH列, DUPLICATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    INT类型作HASH列, DUPLICATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.int_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/STREAM_LOAD/test_hash_int.data" % file_dir
    key_type = DATA.duplicate_key
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_int.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


@pytest.mark.skip()
def test_int_uniq():
    """
    {
    "title": "test_sys_load.test_int_uniq",
    "describe": "INT类型作HASH列, UNIQUE KEY",
    "tag": "autotest"
    }
    """
    """
    INT类型作HASH列, UNIQUE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.int_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/STREAM_LOAD/test_hash_int.data" % file_dir
    key_type = DATA.unique_key
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_int.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_bigint_agg():
    """
    {
    "title": "test_sys_load.test_bigint_agg",
    "describe": "BIGINT类型做HASH列, AGGREGATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    BIGINT类型做HASH列, AGGREGATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.bigint_column_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.aggregate_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_bigint.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_bigint.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_bigint_dup():
    """
    {
    "title": "test_sys_load.test_bigint_dup",
    "describe": "BIGINT类型做HASH列, DUPLICATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    BIGINT类型做HASH列, DUPLICATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.bigint_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.duplicate_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_bigint.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_bigint.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


@pytest.mark.skip()
def test_bigint_uniq():
    """
    {
    "title": "test_sys_load.test_bigint_uniq",
    "describe": "BIGINT类型做HASH列, UNIQUE KEY",
    "tag": "autotest"
    }
    """
    """
    BIGINT类型做HASH列, UNIQUE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.bigint_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.unique_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_bigint.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_bigint.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_largeint_agg():
    """
    {
    "title": "test_sys_load.test_largeint_agg",
    "describe": "LARGEINT类型作HASH列, AGGREGATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    LARGEINT类型作HASH列, AGGREGATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.largeint_column_list
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/STREAM_LOAD/test_hash_largeint.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_largeint.data')
    key_type = DATA.aggregate_key
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_largeint_dup():
    """
    {
    "title": "test_sys_load.test_largeint_dup",
    "describe": "LARGEINT类型作HASH列, DUPLICATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    LARGEINT类型作HASH列, DUPLICATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.largeint_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/STREAM_LOAD/test_hash_largeint.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_largeint.data')
    key_type = DATA.duplicate_key
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


@pytest.mark.skip()
def test_largeint_uniq():
    """
    {
    "title": "test_sys_load.test_largeint_uniq",
    "describe": "LARGEINT类型作HASH列, UNIQUE KEY",
    "tag": "autotest"
    }
    """
    """
    LARGEINT类型作HASH列, UNIQUE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.largeint_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/STREAM_LOAD/test_hash_largeint.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_largeint.data')
    key_type = DATA.unique_key
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_float_int_agg():
    """
    {
    "title": "test_sys_load.test_float_int_agg",
    "describe": "INT类型作AGGREGATE KEY，FLOAT类型作VALUE列",
    "tag": "system,p1,fuzz"
    }
    """
    """
    INT类型作AGGREGATE KEY，FLOAT类型作VALUE列
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.float_column_list
    key_type = DATA.aggregate_key
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/STREAM_LOAD/test_float_int.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_float_int.data')
    sql = 'select k1, v1, v2, v3 from %s order by 1, 2'
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info, sql=sql)


def test_float_int_dup():
    """
    {
    "title": "test_sys_load.test_float_int_dup",
    "describe": "INT类型作DUPLICATE KEY，FLOAT类型作VALUE列",
    "tag": "system,p1,fuzz"
    }
    """
    """
    INT类型作DUPLICATE KEY，FLOAT类型作VALUE列
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.float_column_no_agg_list
    key_type = DATA.duplicate_key
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/STREAM_LOAD/test_float_int.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_float_int.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


@pytest.mark.skip()
def test_float_int_uniq():
    """
    {
    "title": "test_sys_load.test_float_int_uniq",
    "describe": "INT类型作UNIQUE KEY，FLOAT类型作VALUE列",
    "tag": "autotest"
    }
    """
    """
    INT类型作UNIQUE KEY，FLOAT类型作VALUE列
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.float_column_no_agg_list
    key_type = DATA.unique_key
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/STREAM_LOAD/test_float_int.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_float_int.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_double_int_agg():
    """
    {
    "title": "test_sys_load.test_double_int_agg",
    "describe": "INT类型作AGGREGATE KEY，DOUBLE类型作VALUE列",
    "tag": "system,p1,fuzz"
    }
    """
    """
    INT类型作AGGREGATE KEY，DOUBLE类型作VALUE列
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.double_column_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.aggregate_key
    local_data_file = "%s/data/STREAM_LOAD/test_double_int.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_double_int.data')
    sql = 'select k1, v1, v2, v3 from %s order by 1, 2 limit 10'
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info, sql=sql)


def test_double_int_dup():
    """
    {
    "title": "test_sys_load.test_double_int_dup",
    "describe": "INT类型作DUPLICATE KEY，DOUBLE类型作VALUE列",
    "tag": "system,p1,fuzz"
    }
    """
    """
    INT类型作DUPLICATE KEY，DOUBLE类型作VALUE列
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.double_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.duplicate_key
    local_data_file = "%s/data/STREAM_LOAD/test_double_int.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_double_int.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


@pytest.mark.skip()
def test_double_int_uniq():
    """
    {
    "title": "test_sys_load.test_double_int_uniq",
    "describe": "INT类型作UNIQUE KEY，DOUBLE类型作VALUE列",
    "tag": "system,p1"
    }
    """
    """
    INT类型作UNIQUE KEY，DOUBLE类型作VALUE列
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.double_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.unique_key
    local_data_file = "%s/data/STREAM_LOAD/test_double_int.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_double_int.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_date_agg():
    """
    {
    "title": "test_sys_load.test_date_agg",
    "describe": "DATE类型作HASH列, AGGREGATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    DATE类型作HASH列, AGGREGATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.date_column_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.aggregate_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_date.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_date.data')
    sql = 'select cast(k1 as string), cast(v1 as string), cast(v2 as string), cast(v3 as string)' \
          'from %s order by 1, 2'
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info, sql=sql)


def test_date_dup():
    """
    {
    "title": "test_sys_load.test_date_dup",
    "describe": "DATE类型作HASH列, DUPLICATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    DATE类型作HASH列, DUPLICATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.date_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.duplicate_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_date.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_date.data')
    sql = 'select cast(k1 as string), cast(v1 as string), cast(v2 as string), cast(v3 as string)' \
          'from %s order by 1, 2'
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info, sql=sql)


@pytest.mark.skip()
def test_date_uniq():
    """
    {
    "title": "test_sys_load.test_date_uniq",
    "describe": "DATE类型作HASH列, UNIQUE KEY",
    "tag": "autotest"
    }
    """
    """
    DATE类型作HASH列, UNIQUE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.date_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.unique_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_date.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_date.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_datetime_agg():
    """
    {
    "title": "test_sys_load.test_datetime_agg",
    "describe": "DATETIME类型作HASH列, AGGREGATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    DATETIME类型作HASH列, AGGREGATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.datetime_column_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.aggregate_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_datetime.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_datetime.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_datetime_dup():
    """
    {
    "title": "test_sys_load.test_datetime_dup",
    "describe": "DATETIME类型作HASH列, DUPLICATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    DATETIME类型作HASH列, DUPLICATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.datetime_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.duplicate_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_datetime.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_datetime.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


@pytest.mark.skip()
def test_datetime_uniq():
    """
    {
    "title": "test_sys_load.test_datetime_uniq",
    "describe": "DATETIME类型作HASH列, UNIQUE KEY",
    "tag": "autotest"
    }
    """
    """
    DATETIME类型作HASH列, UNIQUE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.datetime_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.unique_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_datetime.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_datetime.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_decimal_least_agg():
    """
    {
    "title": "test_sys_load.test_decimal_least_agg",
    "describe": "DECIMAL(1,0)类型作HASH列, AGGREGATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    DECIMAL(1,0)类型作HASH列, AGGREGATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.decimal_least_column_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.aggregate_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_decimal_least.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_decimal_least.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_decimal_least_dup():
    """
    {
    "title": "test_sys_load.test_decimal_least_dup",
    "describe": "DECIMAL(1,0)类型作HASH列, DUPLICATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    DECIMAL(1,0)类型作HASH列, DUPLICATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.decimal_least_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.duplicate_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_decimal_least.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_decimal_least.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


@pytest.mark.skip()
def test_decimal_least_uniq():
    """
    {
    "title": "test_sys_load.test_decimal_least_uniq",
    "describe": "DECIMAL(1,0)类型作HASH列, UNIQUE KEY",
    "tag": "autotest"
    }
    """
    """
    DECIMAL(1,0)类型作HASH列, UNIQUE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.decimal_least_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.unique_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_decimal_least.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_decimal_least.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_decimal_normal_agg():
    """
    {
    "title": "test_sys_load.test_decimal_normal_agg",
    "describe": "DECIMAL(10,5)类型作HASH列, AGGREGATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    DECIMAL(10,5)类型作HASH列, AGGREGATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.decimal_normal_column_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.aggregate_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_decimal_normal.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_decimal_normal.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_decimal_normal_dup():
    """
    {
    "title": "test_sys_load.test_decimal_normal_dup",
    "describe": "DECIMAL(10,5)类型作HASH列, DUPLICATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    DECIMAL(10,5)类型作HASH列, DUPLICATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.decimal_normal_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.duplicate_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_decimal_normal.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_decimal_normal.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


@pytest.mark.skip()
def test_decimal_normal_uniq():
    """
    {
    "title": "test_sys_load.test_decimal_normal_uniq",
    "describe": "DECIMAL(10,5)类型作HASH列, UNIQUE KEY",
    "tag": "autotest"
    }
    """
    """
    DECIMAL(10,5)类型作HASH列, UNIQUE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.decimal_normal_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.unique_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_decimal_normal.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_decimal_normal.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_decimal_most_agg():
    """
    {
    "title": "test_sys_load.test_decimal_most_agg",
    "describe": "DECIMAL(27,9)类型作HASH列, AGGREGATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    DECIMAL(27,9)类型作HASH列, AGGREGATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.decimal_most_column_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.aggregate_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_decimal_most.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_decimal_most.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_decimal_most_dup():
    """
    {
    "title": "test_sys_load.test_decimal_most_dup",
    "describe": "DECIMAL(27,9)类型作HASH列, DUPLICATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    DECIMAL(27,9)类型作HASH列, DUPLICATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.decimal_most_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.duplicate_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_decimal_most.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_decimal_most.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


@pytest.mark.skip()
def test_decimal_most_uniq():
    """
    {
    "title": "test_sys_load.test_decimal_most_uniq",
    "describe": "DECIMAL(27,9)类型作HASH列, UNIQUE KEY",
    "tag": "autotest"
    }
    """
    """
    DECIMAL(27,9)类型作HASH列, UNIQUE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.decimal_most_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.unique_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_decimal_most.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_decimal_most.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_char_least_agg():
    """
    {
    "title": "test_sys_load.test_char_least_agg",
    "describe": "CHAR类型作HASH列, AGGREGATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    CHAR类型作HASH列, AGGREGATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.char_least_column_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.aggregate_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_char_least.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_char_least.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_char_least_dup():
    """
    {
    "title": "test_sys_load.test_char_least_dup",
    "describe": "CHAR类型作HASH列, DUPLICATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    CHAR类型作HASH列, DUPLICATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.char_least_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.duplicate_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_char_least.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_char_least.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


@pytest.mark.skip()
def test_char_least_uniq():
    """
    {
    "title": "test_sys_load.test_char_least_uniq",
    "describe": "CHAR类型作HASH列, UNIQUE KEY",
    "tag": "autotest"
    }
    """
    """
    CHAR类型作HASH列, UNIQUE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.char_least_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.unique_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_char_least.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_char_least.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_char_normal_agg():
    """
    {
    "title": "test_sys_load.test_char_normal_agg",
    "describe": "CHAR_NORMAL类型做HASH列，考虑特殊字符, AGGREGATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    CHAR_NORMAL类型做HASH列，考虑特殊字符, AGGREGATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.char_normal_column_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.aggregate_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_char_normal.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_char_normal.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_char_normal_dup():
    """
    {
    "title": "test_sys_load.test_char_normal_dup",
    "describe": "CHAR_NORMAL类型做HASH列，考虑特殊字符, DUPLICATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    CHAR_NORMAL类型做HASH列，考虑特殊字符, DUPLICATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.char_normal_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.duplicate_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_char_normal.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_char_normal.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


@pytest.mark.skip()
def test_char_normal_uniq():
    """
    {
    "title": "test_sys_load.test_char_normal_uniq",
    "describe": "CHAR_NORMAL类型做HASH列，考虑特殊字符, UNIQUE KEY",
    "tag": "autotest"
    }
    """
    """
    CHAR_NORMAL类型做HASH列，考虑特殊字符, UNIQUE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.char_normal_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.unique_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_char_normal.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_char_normal.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_char_most_agg():
    """
    {
    "title": "test_sys_load.test_char_most_agg",
    "describe": "CHAR_MOST 类型做HASH列, AGGREGATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    CHAR_MOST 类型做HASH列, AGGREGATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.char_most_column_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.aggregate_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_char_most.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_char_most.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_char_most_dup():
    """
    {
    "title": "test_sys_load.test_char_most_dup",
    "describe": "CHAR_MOST 类型做HASH列, DUPLICATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    CHAR_MOST 类型做HASH列, DUPLICATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.char_most_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.duplicate_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_char_most.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_char_most.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


@pytest.mark.skip()
def test_char_most_uniq():
    """
    {
    "title": "test_sys_load.test_char_most_uniq",
    "describe": "CHAR_MOST 类型做HASH列, UNIQUE KEY",
    "tag": "autotest"
    }
    """
    """
    CHAR_MOST 类型做HASH列, UNIQUE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.char_most_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.unique_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_char_most.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_char_most.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_varchar_least_agg():
    """
    {
    "title": "test_sys_load.test_varchar_least_agg",
    "describe": "VARCHAR_LEAST类型做HASH列, AGGREGATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    VARCHAR_LEAST类型做HASH列, AGGREGATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.varchar_least_column_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.aggregate_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_varchar_least.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_varchar_least.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_varchar_least_dup():
    """
    {
    "title": "test_sys_load.test_varchar_least_dup",
    "describe": "VARCHAR_LEAST类型做HASH列, DUPLICATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    VARCHAR_LEAST类型做HASH列, DUPLICATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.varchar_least_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.duplicate_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_varchar_least.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_varchar_least.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


@pytest.mark.skip()
def test_varchar_least_uniq():
    """
    {
    "title": "test_sys_load.test_varchar_least_uniq",
    "describe": "VARCHAR_LEAST类型做HASH列, UNIQUE KEY",
    "tag": "autotest"
    }
    """
    """
    VARCHAR_LEAST类型做HASH列, UNIQUE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.varchar_least_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.unique_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_varchar_least.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_varchar_least.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_varchar_normal_agg():
    """
    {
    "title": "test_sys_load.test_varchar_normal_agg",
    "describe": "VARCHAR_NORMAL类型做HASH列, AGGREGATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    VARCHAR_NORMAL类型做HASH列, AGGREGATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.varchar_normal_column_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.aggregate_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_varchar_normal.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_varchar_normal.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_varchar_normal_dup():
    """
    {
    "title": "test_sys_load.test_varchar_normal_dup",
    "describe": "VARCHAR_NORMAL类型做HASH列, DUPLICATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    VARCHAR_NORMAL类型做HASH列, DUPLICATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.varchar_normal_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.duplicate_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_varchar_normal.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_varchar_normal.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


@pytest.mark.skip()
def test_varchar_normal_uniq():
    """
    {
    "title": "test_sys_load.test_varchar_normal_uniq",
    "describe": "VARCHAR_NORMAL类型做HASH列, UNIQUE KEY",
    "tag": "autotest"
    }
    """
    """
    VARCHAR_NORMAL类型做HASH列, UNIQUE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.varchar_normal_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.unique_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_varchar_normal.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_varchar_normal.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def test_varchar_most_agg():
    """
    {
    "title": "test_sys_load.test_varchar_most_agg",
    "describe": "VARCHAR_MOST类型做HASH列, AGGREGATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    VARCHAR_MOST类型做HASH列, AGGREGATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.varchar_most_column_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.aggregate_key
    #数据文件
    # local_data_file = '%s/data/STREAM_LOAD/test_hash_varchar_most_*.data' % file_dir
    # hdfs_data_file = gen_remote_file_path('STREAM_LOAD/test_hash_varchar_most_*.data')
    local_data_file = "%s/data/STREAM_LOAD/test_hash_varchar_most.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_varchar_most.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)
    
    
def test_varchar_most_dup():
    """
    {
    "title": "test_sys_load.test_varchar_most_dup",
    "describe": "VARCHAR_MOST类型做HASH列, DUPLICATE KEY",
    "tag": "system,p1,fuzz"
    }
    """
    """
    VARCHAR_MOST类型做HASH列, DUPLICATE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.varchar_most_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.duplicate_key
    #数据文件
    # local_data_file = '%s/data/STREAM_LOAD/test_hash_varchar_most_*.data' % file_dir
    # hdfs_data_file = gen_remote_file_path('STREAM_LOAD/test_hash_varchar_most_*.data')
    local_data_file = "%s/data/STREAM_LOAD/test_hash_varchar_most.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_varchar_most.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


@pytest.mark.skip()
def test_varchar_most_uniq():
    """
    {
    "title": "test_sys_load.test_varchar_most_uniq",
    "describe": "VARCHAR_MOST类型做HASH列, UNIQUE KEY",
    "tag": "autotest"
    }
    """
    """
    VARCHAR_MOST类型做HASH列, UNIQUE KEY
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    column_list = DATA.varchar_most_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    key_type = DATA.unique_key
    local_data_file = "%s/data/STREAM_LOAD/test_hash_varchar_most.data" % file_dir
    hdfs_data_file = palo_config.gen_remote_file_path('sys/STREAM_LOAD/test_hash_varchar_most.data')
    base(database_name, table_name, column_list, local_data_file, hdfs_data_file, key_type,
         distribution_info)


def teardown_module():
    """
    tearDown
    """
    print('end')


if __name__ == '__main__':
    setup_module()
    test_tinyint_agg()


