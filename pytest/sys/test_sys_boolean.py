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
#   @file test_sys_boolean.py
#   @date 2020-07-08 15:24:04
#   @brief This file is a test file for boolean data type
#
#############################################################################

"""
测试boolean类型
"""

import os
from data import schema as DATA
from lib import palo_config
from lib import palo_client
from lib import util
from lib import common

config = palo_config.config
fe_http_port = config.fe_http_port
broker_info = palo_config.broker_info
file_dir = os.path.abspath(os.path.dirname(__file__))


def setup_module():
    """
    setUp
    """
    pass


def test_bool_create_table_wrong_1():
    """
    {
    "title": "test_bool_create_table_wrong_1",
    "describe": "验证boolean类型不支持max, min, sum三种聚合方式",
    "tag": "fuzz,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    agg_type = ['MAX', 'MIN', 'SUM']
    for type in agg_type:
        flag = True
        try:
            column_list = [('k1', 'BOOLEAN'), ('k2', 'INT'), ('v1', 'BOOLEAN', type)]
            distribution = palo_client.DistributionInfo('Hash(k2)', 1)
            client.create_table(table_name, column_list, distribution_info=distribution)
            flag = False
        except Exception as e:
            print(str(e))
            pass
        assert flag, 'expect create table faile'
    client.clean(database_name)


def test_bool_create_table_wrong_2():
    """
    {
    "title": "test_bool_create_table_wrong_2",
    "describe": "验证boolean类型不支持作为分区列",
    "tag": "fuzz,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    partition_name_list = ['p1', 'p2', 'p3']
    partition_column = ['k1', 'k2']
    partition_value_list = [('0', 'True'), ('10', 'True'), ('100', 'False')]
    partition_info = palo_client.PartitionInfo(partition_column, partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(k2)', 10)
    flag = True
    try:
        client.create_table(table_name, DATA.boolean_column_list, partition_info,
                            distribution_info)
        flag = False
    except Exception as e:
        print(str(e))
    assert flag
    client.clean(database_name)


def test_bool_create_table_wrong_3():
    """
    {
    "title": "test_bool_create_table_wrong_3",
    "describe": "验证不支持创建boolean类型的bloom filter",
    "tag": "fuzz,p1"
    }
    """
    """bug: bool类型作为bloom filter"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    distribution_info = palo_client.DistributionInfo('HASH(k2)', 10)
    flag = True
    try:
        client.create_table(table_name, DATA.boolean_column_list, distribution_info=distribution_info, 
                            bloom_filter_column_list=['k1', 'k2'])
        flag = False
    except Exception as e:
        print(str(e))
    assert flag
    client.clean(database_name)


def test_bool_bucket_key():
    """
    {
    "title": "test_bool_create_table_wrong_1",
    "describe": "验证支持bool类型的hash分桶，导入，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.boolean_column_list, distribution_info=DATA.hash_distribution_info,
                              keys_desc=DATA.boolean_aggregate_key, set_null=True)
    assert ret
    bool_hdfs = palo_config.gen_remote_file_path('sys/load/test_bool.data')
    column_name_list = ['k1', 'k2', 'v1', 'v2']
    load_data_info = palo_client.LoadDataInfo(bool_hdfs, table_name, column_name_list=column_name_list)
    ret = client.batch_load(util.get_label(), load_data_info, max_filter_ratio=0.01, broker=broker_info, is_wait=True)
    assert ret
    assert client.verify('%s/data/LOAD/expe_agg_bool_null_ns.data' % file_dir, table_name)
    client.clean(database_name)


def test_bool_broker_load():
    """
    {
    "title": "test_bool_broker_load",
    "describe": "验证broker导入bool类型，结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    # 建表
    agg_tb = table_name + '_agg'
    duplicate_tb = table_name + "_dup"
    unique_tb = table_name + "_uniq"
    ret = client.create_table(agg_tb, DATA.boolean_column_list, distribution_info=DATA.boolean_distribution_info, 
                              keys_desc=DATA.boolean_aggregate_key, set_null=True)
    assert ret
    ret = client.create_table(duplicate_tb, DATA.boolean_column_no_agg_list, 
                              distribution_info=DATA.boolean_distribution_info, keys_desc=DATA.boolean_duplicate_key,
                              set_null=True)
    assert ret
    ret = client.create_table(unique_tb, DATA.boolean_column_no_agg_list,
                              distribution_info=DATA.boolean_distribution_info, keys_desc=DATA.boolean_unique_key, 
                              set_null=True)
    assert ret
    # 导入
    bool_hdfs = palo_config.gen_remote_file_path('sys/load/test_bool.data')
    column_name_list = ['k1', 'k2', 'v1', 'v2']
    load_data_info = palo_client.LoadDataInfo(bool_hdfs, agg_tb, column_name_list=column_name_list)
    ret = client.batch_load(util.get_label(), load_data_info, max_filter_ratio=0.01, broker=broker_info, is_wait=True)
    assert ret
    load_data_info = palo_client.LoadDataInfo(bool_hdfs, duplicate_tb, column_name_list=column_name_list)
    ret = client.batch_load(util.get_label(), load_data_info, max_filter_ratio=0.01, broker=broker_info, is_wait=True)
    assert ret
    load_data_info = palo_client.LoadDataInfo(bool_hdfs, unique_tb, column_name_list=column_name_list)
    ret = client.batch_load(util.get_label(), load_data_info, max_filter_ratio=0.01, broker=broker_info, is_wait=True)
    assert ret
    # 查询&验证结果
    assert client.verify('%s/data/LOAD/expe_agg_bool_null_ns.data' % file_dir, agg_tb)
    assert client.verify('%s/data/LOAD/expe_dup_bool_null_ns.data' % file_dir, duplicate_tb)
    assert client.verify('%s/data/LOAD/expe_uniq_bool_null_ns.data' % file_dir, unique_tb)
    client.clean(database_name)


def test_bool_stream_load():
    """
    {
    "title": "test_bool_stream_load",
    "describe": "验证stream导入bool类型，结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    # 建表
    agg_tb = table_name + '_agg'
    duplicate_tb = table_name + "_dup"
    unique_tb = table_name + "_uniq"
    ret = client.create_table(agg_tb, DATA.boolean_column_list, distribution_info=DATA.boolean_distribution_info,
                              keys_desc=DATA.boolean_aggregate_key, set_null=True)
    assert ret
    ret = client.create_table(duplicate_tb, DATA.boolean_column_no_agg_list,
                              distribution_info=DATA.boolean_distribution_info, keys_desc=DATA.boolean_duplicate_key,
                               set_null=True)
    assert ret
    ret = client.create_table(unique_tb, DATA.boolean_column_no_agg_list, 
                              distribution_info=DATA.boolean_distribution_info, keys_desc=DATA.boolean_unique_key,
                              set_null=True)
    assert ret
    # 导入
    local_file = '%s/data/LOAD/test_bool.data' % file_dir
    print(file_dir)
    ret = client.stream_load(agg_tb, local_file, max_filter_ratio=0.1)
    assert ret
    column_name_list = ['k1', 'k2', 'v1', 'v2']
    ret = client.stream_load(duplicate_tb, local_file, max_filter_ratio=0.1, column_name_list=column_name_list)
    assert ret
    ret = client.stream_load(unique_tb, local_file, max_filter_ratio=0.1, column_name_list=column_name_list)
    assert ret
    # 查询 & 验证
    assert client.verify('%s/data/LOAD/expe_agg_bool_null_ns.data' % file_dir, agg_tb)
    assert client.verify('%s/data/LOAD/expe_dup_bool_null_ns.data' % file_dir, duplicate_tb)
    assert client.verify('%s/data/LOAD/expe_uniq_bool_null_ns.data' % file_dir, unique_tb)
    client.clean(database_name)


def test_bool_insert():
    """
    {
    "title": "test_bool_insert",
    "describe": "验证insert导入bool类型，结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    # 建表
    agg_tb = table_name + '_agg'
    duplicate_tb = table_name + "_dup"
    unique_tb = table_name + "_uniq"
    sql = "SET enable_insert_strict = false;"
    client.execute(sql)
    ret = client.create_table(agg_tb, DATA.boolean_column_list, distribution_info=DATA.boolean_distribution_info,
                              keys_desc=DATA.boolean_aggregate_key, set_null=True)
    assert ret
    ret = client.create_table(duplicate_tb, DATA.boolean_column_no_agg_list,
                              distribution_info=DATA.boolean_distribution_info, keys_desc=DATA.boolean_duplicate_key, 
                              set_null=True)
    assert ret
    ret = client.create_table(unique_tb, DATA.boolean_column_no_agg_list, 
                              distribution_info=DATA.boolean_distribution_info, keys_desc=DATA.boolean_unique_key,
                              set_null=True)
    assert ret
    # 导入
    sql_list = list()
    sql_list.append("insert into %s(k1, k2, v1, v2) values (1, 1, 1, 1), (0, 0, 0, 0)")
    sql_list.append("insert into %s(k1, k2, v1, v2) values ('true', 2, 'true', 'true'), ('false', 3, 'false', 'false')")
    sql_list.append("insert into %s(k1, k2, v1, v2) values (true, 4, true, true), (false, 5, false, false)")
    sql_list.append("insert into %s(k1, k2, v1, v2) values ('0', 6, '0', '0'), ('1', 7, '1', '8')")
    sql_list.append("insert into %s(k1, k2, v1, v2) values ('hello', 8, 'a', 'b'), (null, 9, null, null)")
    sql_list.append("insert into %s(k1, k2, v1, v2) values ('TRue', 2, 'TRUe', 'FALSE'), ('False', 3, 'TRUE', 'FALSe')")
    for insert_sql in sql_list:
        sql = insert_sql % agg_tb
        ret = client.execute(sql)
        assert ret == (), 'expect insert ok'
    sql_list = list()
    sql_list.append("insert into %s(k1, k2, v1) values (1, 1, 1), (0, 0, 0)")
    sql_list.append("insert into %s(k1, k2, v1) values ('true', 2, 'true'), ('false', 3, 'false')")
    sql_list.append("insert into %s(k1, k2, v1) values (true, 4, true), (false, 5, false)")
    sql_list.append("insert into %s(k1, k2, v1) values ('0', 6, '0'), ('1', 7, '1')")
    sql_list.append("insert into %s(k1, k2, v1) values ('hello', 8, 'a'), (null, 9, null)")
    sql_list.append("insert into %s(k1, k2, v1) values ('TRue', 2, 'TRUe'), ('False', 3, 'TRUE')")
    for insert_sql in sql_list:
        for tb in [duplicate_tb, unique_tb]:
            sql = insert_sql % tb
            ret = client.execute(sql)
            assert ret == (), 'expect insert ok'
    # 验证 
    assert client.verify('%s/data/LOAD/expe_bool_agg_insert_null_ns.data' % file_dir, agg_tb)
    assert client.verify('%s/data/LOAD/expe_bool_dup_insert_null_ns.data' % file_dir, duplicate_tb)
    assert client.verify('%s/data/LOAD/expe_bool_agg_insert_null_ns.data' % file_dir, unique_tb)
    client.clean(database_name)


def test_bool_load_strict_null():
    """
    {
    "title": "test_bool_load_strict_null",
    "describe": "验证broker导入bool类型，设置strict为true，列为Null，验证导入结果正确",
    "tag": "fuzz,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    # 建表
    agg_tb = table_name + '_agg'
    duplicate_tb = table_name + "_dup"
    unique_tb = table_name + "_uniq"
    ret = client.create_table(agg_tb, DATA.boolean_column_list, distribution_info=DATA.boolean_distribution_info, 
                              keys_desc=DATA.boolean_aggregate_key, set_null=True)
    assert ret
    ret = client.create_table(duplicate_tb, DATA.boolean_column_no_agg_list,
                              distribution_info=DATA.boolean_distribution_info, keys_desc=DATA.boolean_duplicate_key, 
                              set_null=True)
    assert ret
    ret = client.create_table(unique_tb, DATA.boolean_column_no_agg_list,
                              distribution_info=DATA.boolean_distribution_info, keys_desc=DATA.boolean_unique_key, 
                              set_null=True)
    assert ret
    # 导入
    bool_hdfs = palo_config.gen_remote_file_path('sys/load/test_bool.data')
    column_name_list = ['k1', 'k2', 'v1', 'v2']
    load_data_info = palo_client.LoadDataInfo(bool_hdfs, agg_tb, column_name_list=column_name_list)
    ret = client.batch_load(util.get_label(), load_data_info, max_filter_ratio=0.01, 
                            broker=broker_info, is_wait=True, strict_mode=True)
    assert ret
    load_data_info = palo_client.LoadDataInfo(bool_hdfs, duplicate_tb, column_name_list=column_name_list)
    ret = client.batch_load(util.get_label(), load_data_info, max_filter_ratio=0.01,
                            broker=broker_info, is_wait=True, strict_mode=True)
    assert ret
    load_data_info = palo_client.LoadDataInfo(bool_hdfs, unique_tb, column_name_list=column_name_list)
    ret = client.batch_load(util.get_label(), load_data_info, max_filter_ratio=0.01,
                            broker=broker_info, is_wait=True, strict_mode=True)
    assert ret
    # 查询&验证结果
    assert client.verify('%s/data/LOAD/expe_agg_bool_null_s.data' % file_dir, agg_tb)
    assert client.verify('%s/data/LOAD/expe_dup_bool_null_s.data' % file_dir, duplicate_tb)
    assert client.verify('%s/data/LOAD/expe_uniq_bool_null_s.data' % file_dir, unique_tb)
    client.clean(database_name)


def test_bool_load_strict_not_null():
    """
    {
    "title": "test_bool_load_strict_not_null",
    "describe": "验证broker导入bool类型，设置strict为true，列为Null，验证导入结果正确",
    "tag": "fuzz,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    # 建表
    agg_tb = table_name + '_agg'
    duplicate_tb = table_name + "_dup"
    unique_tb = table_name + "_uniq"
    ret = client.create_table(agg_tb, DATA.boolean_column_list, distribution_info=DATA.boolean_distribution_info, 
                              keys_desc=DATA.boolean_aggregate_key, set_null=False)
    assert ret
    ret = client.create_table(duplicate_tb, DATA.boolean_column_no_agg_list,
                              distribution_info=DATA.boolean_distribution_info, keys_desc=DATA.boolean_duplicate_key,
                              set_null=False)
    assert ret
    ret = client.create_table(unique_tb, DATA.boolean_column_no_agg_list,
                              distribution_info=DATA.boolean_distribution_info, keys_desc=DATA.boolean_unique_key,
                              set_null=False)
    assert ret
    # 导入
    bool_hdfs = palo_config.gen_remote_file_path('sys/load/test_bool.data')
    column_name_list = ['k1', 'k2', 'v1', 'v2']
    load_data_info = palo_client.LoadDataInfo(bool_hdfs, agg_tb, column_name_list=column_name_list)
    ret = client.batch_load(util.get_label(), load_data_info, max_filter_ratio=0.5,
                            broker=broker_info, is_wait=True, strict_mode=True)
    assert ret
    load_data_info = palo_client.LoadDataInfo(bool_hdfs, duplicate_tb, column_name_list=column_name_list)
    ret = client.batch_load(util.get_label(), load_data_info, max_filter_ratio=0.5,
                            broker=broker_info, is_wait=True, strict_mode=True)
    assert ret
    load_data_info = palo_client.LoadDataInfo(bool_hdfs, unique_tb, column_name_list=column_name_list)
    ret = client.batch_load(util.get_label(), load_data_info, max_filter_ratio=0.5,
                            broker=broker_info, is_wait=True, strict_mode=True)
    assert ret
    # 查询&验证结果
    assert client.verify('%s/data/LOAD/expe_agg_bool_not_null_s.data' % file_dir, agg_tb)
    assert client.verify('%s/data/LOAD/expe_dup_bool_not_null_s.data' % file_dir, duplicate_tb)
    assert client.verify('%s/data/LOAD/expe_uniq_bool_not_null_s.data' % file_dir, unique_tb)
    client.clean(database_name)


def test_add_bool_key_column():
    """
    {
    "title": "test_add_bool_key_column",
    "describe": "验证增加boolean类型的key列成功",
    "tag": "system,p1"
    }
    """
    """bug: 增加bool类型的key列"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.boolean_column_list, distribution_info=DATA.hash_distribution_info,
                              keys_desc=DATA.boolean_aggregate_key, set_null=True)
    assert ret
    bool_hdfs = palo_config.gen_remote_file_path('sys/load/test_bool.data')
    column_name_list = ['k1', 'k2', 'v1', 'v2']
    load_data_info = palo_client.LoadDataInfo(bool_hdfs, table_name, column_name_list=column_name_list)
    ret = client.batch_load(util.get_label(), load_data_info, max_filter_ratio=0.01, broker=broker_info, is_wait=True)
    assert ret
    assert client.verify('%s/data/LOAD/expe_agg_bool_null_ns.data' % file_dir, table_name)
    sql = 'select k1, k2, 1, v1, v2 from %s order by 1, 2' % table_name
    ret1 = client.execute(sql)
    add_column = ['k3 boolean default "1"']
    ret = client.schema_change(table_name, add_column_list=add_column, is_wait=True)
    assert ret
    sql = "select * from %s order by k1, k2" % table_name
    ret2 = client.execute(sql)
    util.check(ret1, ret2)
    client.clean(database_name)


def test_add_bool_value_column():
    """
    {
    "title": "test_add_bool_value_column",
    "describe": "验证增加boolean类型的value列成功",
    "tag": "system,p1"
    }
    """
    """增加bool类型的value列"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.boolean_column_list, distribution_info=DATA.hash_distribution_info, 
                              keys_desc=DATA.boolean_aggregate_key, set_null=True)
    assert ret
    bool_hdfs = palo_config.gen_remote_file_path('sys/load/test_bool.data')
    column_name_list = ['k1', 'k2', 'v1', 'v2']
    load_data_info = palo_client.LoadDataInfo(bool_hdfs, table_name, column_name_list=column_name_list)
    ret = client.batch_load(util.get_label(), load_data_info, max_filter_ratio=0.01, broker=broker_info, is_wait=True)
    assert ret
    assert client.verify('%s/data/LOAD/expe_agg_bool_null_ns.data' % file_dir, table_name)
    sql = 'select k1, k2, v1, v2, null from %s order by 1, 2' % table_name
    ret1 = client.execute(sql)
    add_column = ['v3 boolean replace_if_not_null']
    ret = client.schema_change(table_name, add_column_list=add_column, is_wait=True)
    assert ret
    sql = "select * from %s order by k1, k2" % table_name
    ret2 = client.execute(sql)
    util.check(ret1, ret2)
    client.clean(database_name)


def test_drop_bool_column():
    """
    {
    "title": "test_drop_bool_column",
    "describe": "验证删除boolean类型的value列成功，key列失败因为有replace列",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.boolean_column_list, distribution_info=DATA.hash_distribution_info, 
                              keys_desc=DATA.boolean_aggregate_key, set_null=True)
    assert ret
    bool_hdfs = palo_config.gen_remote_file_path('sys/load/test_bool.data')
    column_name_list = ['k1', 'k2', 'v1', 'v2']
    load_data_info = palo_client.LoadDataInfo(bool_hdfs, table_name, column_name_list=column_name_list)
    ret = client.batch_load(util.get_label(), load_data_info, max_filter_ratio=0.01, broker=broker_info, is_wait=True)
    assert ret
    assert client.verify('%s/data/LOAD/expe_agg_bool_null_ns.data' % file_dir, table_name)
    sql = 'select k1, k2, v1 from %s order by 1, 2' % table_name
    ret1 = client.execute(sql)
    # 删除bool key列失败，因为有replace聚合
    flag = True
    try:
        drop_column = ['k1']
        client.schema_change(table_name, drop_column_list=drop_column, is_wait=False)
        flag = False
    except Exception as e:
        print(str(e))
    assert flag

    drop_column = ['v2']
    ret = client.schema_change(table_name, drop_column_list=drop_column, is_wait=True)
    assert ret
    sql = "select * from %s order by k1, k2" % table_name
    ret2 = client.execute(sql)
    util.check(ret1, ret2)
    client.clean(database_name)


def test_alter_to_bool():
    """
    {
    "title": "test_alter_to_bool",
    "describe": "验证不支持其他列（tinyint/int/char/varchar/date/float）转为boolean类型列",
    "tag": "fuzz,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list, distribution_info=DATA.hash_distribution_info,
                              set_null=True)
    assert ret
    flag = True
    try:
        modify_column = ['k1 boolean' ]
        ret = client.schema_change(table_name, modify_column_list=modify_column, is_wait=True)
        flag = False
    except Exception as e:
        print(str(e))
    assert flag, 'expcet alter modify failed, can not convert tinyint to boolean'
    flag = True
    try:
        modify_column = ['k4 boolean' ]
        ret = client.schema_change(table_name, modify_column_list=modify_column, is_wait=True)
        flag = False
    except Exception as e:
        print(str(e))
    assert flag, 'expcet alter modify failed, can not convert bigint to boolean'
    flag = True
    try:
        modify_column = ['k6 boolean' ]
        ret = client.schema_change(table_name, modify_column_list=modify_column, is_wait=True)
        flag = False
    except Exception as e:
        print(str(e))
    assert flag, 'expcet alter modify failed, can not convert char to boolean'
    flag = True
    try:
        modify_column = ['k7 boolean' ]
        ret = client.schema_change(table_name, modify_column_list=modify_column, is_wait=True)
        flag = False
    except Exception as e:
        print(str(e))
    assert flag, 'expcet alter modify failed, can not convert varchar to boolean'
    flag = True
    try:
        modify_column = ['k10 boolean' ]
        ret = client.schema_change(table_name, modify_column_list=modify_column, is_wait=True)
        flag = False
    except Exception as e:
        print(str(e))
    assert flag, 'expcet alter modify failed, can not convert date to boolean'
    flag = True
    try:
        modify_column = ['k9 boolean' ]
        ret = client.schema_change(table_name, modify_column_list=modify_column, is_wait=True)
        flag = False
    except Exception as e:
        print(str(e))
    assert flag, 'expcet alter modify failed, can not convert float to boolean'
    client.clean(database_name)


def test_alter_bool_to_other_type():
    """
    {
    "title": "test_alter_bool_to_other_type",
    "describe": "验证不支持boolean类型列转为其他列（tinyint/int/char/varchar/date/float）",
    "tag": "fuzz,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.boolean_column_list, distribution_info=DATA.hash_distribution_info,
                              keys_desc=DATA.boolean_aggregate_key, set_null=True)
    assert ret
    bool_hdfs = palo_config.gen_remote_file_path('sys/load/test_bool.data')
    column_name_list = ['k1', 'k2', 'v1', 'v2']
    load_data_info = palo_client.LoadDataInfo(bool_hdfs, table_name, column_name_list=column_name_list)
    ret = client.batch_load(util.get_label(), load_data_info, max_filter_ratio=0.01, broker=broker_info, is_wait=True)
    assert ret
    assert client.verify('%s/data/LOAD/expe_agg_bool_null_ns.data' % file_dir, table_name)
    flag = True
    try:
        modify_column = ['v1 varchar(2) replace', 'v2 varchar(1) replace_if_not_null']
        ret = client.schema_change(table_name, modify_column_list=modify_column, is_wait=True)
        flag = False
    except Exception as e:
        print(str(e))
    assert flag, 'expcet alter modify failed, can not convert boolean to varchar'
    try:
        modify_column = ['v1 char(2) replace', 'v2 char(1) replace_if_not_null']
        ret = client.schema_change(table_name, modify_column_list=modify_column, is_wait=True)
        flag = False
    except Exception as e:
        print(str(e))
    assert flag, 'expcet alter modify failed, can not convert boolean to char'
    try:
        modify_column = ['v1 tinyint replace', 'v2 tinyint replace_if_not_null']
        ret = client.schema_change(table_name, modify_column_list=modify_column, is_wait=True)
        flag = False
    except Exception as e:
        print(str(e))
    assert flag, 'expcet alter modify failed, can not convert boolean to tinyint'
    try:
        modify_column = ['v1 float replace', 'v2 float replace_if_not_null']
        ret = client.schema_change(table_name, modify_column_list=modify_column, is_wait=True)
        flag = False
    except Exception as e:
        print(str(e))
    assert flag, 'expcet alter modify failed, can not convert boolean to float'
    try:
        modify_column = ['v1 date replace', 'v2 date replace_if_not_null']
        ret = client.schema_change(table_name, modify_column_list=modify_column, is_wait=True)
        flag = False
    except Exception as e:
        print(str(e))
    assert flag, 'expcet alter modify failed, can not convert boolean to date'
    client.clean(database_name)


def test_rollup_bool():
    """
    {
    "title": "test_rollup_bool",
    "describe": "验证试用boolean类型创建rollup",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.boolean_column_list, distribution_info=DATA.hash_distribution_info, 
                              keys_desc=DATA.boolean_aggregate_key, set_null=True)
    assert ret
    bool_hdfs = palo_config.gen_remote_file_path('sys/load/test_bool.data')
    column_name_list = ['k1', 'k2', 'v1', 'v2']
    load_data_info = palo_client.LoadDataInfo(bool_hdfs, table_name, column_name_list=column_name_list)
    ret = client.batch_load(util.get_label(), load_data_info, max_filter_ratio=0.01, broker=broker_info, is_wait=True)
    assert ret
    assert client.verify('%s/data/LOAD/expe_agg_bool_null_ns.data' % file_dir, table_name)
    int_tb_name = 'int_' + table_name
    int_4_column_list = [('k1', 'BOOLEAN'),
                         ('k2', 'INT'),
                         ('k3', 'BOOLEAN', 'REPLACE'),
                         ('k4', 'INT', 'SUM')]
    ret = client.create_table(int_tb_name, int_4_column_list, distribution_info=DATA.hash_distribution_info,
                              keys_desc=DATA.boolean_aggregate_key, set_null=True)
    assert ret
    insert_sql = 'insert into %s select * from %s' % (int_tb_name, table_name)
    ret = client.execute(insert_sql)
    assert ret == ()
    assert client.verify('%s/data/LOAD/expe_agg_bool_null_ns.data' % file_dir, int_tb_name)
    sql = 'select k1, sum(k4) from %s group by k1 order by k1' % int_tb_name
    ret1 = client.execute(sql)
    rollup_column = ['k1', 'k4']
    ret = client.create_rollup_table(int_tb_name, rollup_table_name, rollup_column, is_wait=True)
    assert ret
    assert client.get_index(int_tb_name, rollup_table_name)
    ret2 = client.execute(sql)
    util.check(ret1, ret2)
    client.clean(database_name)


def test_material_view_bool():
    """
    {
    "title": "test_material_view_bool",
    "describe": "验证支持试用boolean类型创建物化视图",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.boolean_column_no_agg_list, 
                              distribution_info=DATA.hash_distribution_info,
                              keys_desc=DATA.boolean_duplicate_key, set_null=True)
    assert ret
    bool_hdfs = palo_config.gen_remote_file_path('sys/load/test_bool.data')
    column_name_list = ['k1', 'k2', 'v1', 'v2']
    load_data_info = palo_client.LoadDataInfo(bool_hdfs, table_name, column_name_list=column_name_list)
    ret = client.batch_load(util.get_label(), load_data_info, max_filter_ratio=0.01, broker=broker_info, is_wait=True)
    assert ret
    assert client.verify('%s/data/LOAD/expe_dup_bool_null_ns.data' % file_dir, table_name)
    int_tb_name = 'int_' + table_name
    int_4_column_list = [('k1', 'BOOLEAN'),
                         ('k2', 'INT'),
                         ('k3', 'INT')]
    ret = client.create_table(int_tb_name, int_4_column_list, distribution_info=DATA.hash_distribution_info,
                              keys_desc=DATA.boolean_duplicate_key, set_null=True)
    assert ret
    insert_sql = 'insert into %s select * from %s' % (int_tb_name, table_name)
    ret = client.execute(insert_sql)
    assert ret == ()
    assert client.verify('%s/data/LOAD/expe_dup_bool_null_ns.data' % file_dir, int_tb_name)
    sql = 'select k1, sum(k3) from %s group by k1 order by k1' % int_tb_name
    ret1 = client.execute(sql)
    ret = client.create_materialized_view(int_tb_name, rollup_table_name, sql, is_wait=True)
    assert ret
    assert client.get_index(int_tb_name, rollup_table_name)
    ret2 = client.execute(sql)
    util.check(ret1, ret2)
    client.clean(database_name)


if __name__ == '__main__':
    setup_module()
    test_bool_create_table_wrong_3()
    test_add_bool_key_column()
    test_add_bool_value_column()



