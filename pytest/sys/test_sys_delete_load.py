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
  * @file test_sys_partition_schema_change.py
  * @date 2015/02/04 15:26:21
  * @brief This file is a test file for Palo schema changing.
  *
  **************************************************************************/
  对于unique表的导入来说，每条数据都是有全部的key的，相当于是按照全key进行数据delete的
"""

import os
import sys
import time

sys.path.append("../")
sys.path.append("../../")
from data import schema as DATA
from data import load_file as FILE
from lib import palo_config
from lib import palo_client
from lib import util
from lib import common
from lib import palo_job
from lib import kafka_config

config = palo_config.config
LOG = palo_client.LOG
L = palo_client.L
broker_info = palo_config.broker_info
TOPIC = 'routine-load-delete-%s' % config.fe_query_port


def setup_module():
    """set up"""
    global check_db, baseall_tb
    baseall_tb = 'baseall'
    if 'FE_DB' in os.environ.keys():
        check_db = os.environ['FE_DB']
    else:
        check_db = 'test_query_qa'


def teardown_module():
    """tear down"""
    pass


def test_delete_broker_basic():
    """
    {
    "title": "test_delete_broker_basic",
    "describe": "验证broker load的delete的基本功能",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_no_agg_list,
                              distribution_info=DATA.baseall_distribution_info,
                              keys_desc=DATA.baseall_unique_key)
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'can not get table: %s' % table_name
    try:
        ret = client.enable_feature_batch_delete(table_name)
        assert ret, 'enable batch delete feature failed'
    except Exception as e:
        pass
    assert client.set_variables('show_hidden_columns', 1)
    ret = client.desc_table(table_name, is_all=True)
    assert util.get_attr_condition_value(ret, palo_job.DescInfoAll.Field, '__DORIS_DELETE_SIGN__')
    assert client.set_variables('show_hidden_columns', 0)
    # delete load, 一个空表，表中的数据仍然为空
    load_data_desc = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, merge_type='DELETE')
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True)
    assert ret, 'broker load failed'
    ret = client.select_all(table_name)
    assert ret == (), 'check failed'
    # 向表中导入数据，再delete load，预期表为空
    load_data_desc = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True)
    assert ret, 'broker failed'
    sql = 'select * from %s.%s order by k1'
    common.check2(client, sql1=sql % (database_name, table_name), sql2=sql % (check_db, baseall_tb))
    load_data_desc = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, merge_type='DELETE')
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True)
    assert ret, 'broker load failed'
    ret = client.select_all(table_name)
    assert ret == (), 'check failed'
    client.clean(database_name)


def test_delete_broker_column_set():
    """
    {
    "title": "test_delete_broker_column_set",
    "describe": "验证broker load的delete的列设置",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list,
                              distribution_info=DATA.baseall_distribution_info,
                              keys_desc=DATA.datatype_column_uniq_key)
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'can not get table: %s' % table_name
    try:
        ret = client.enable_feature_batch_delete(table_name)
        assert ret, 'enable batch delete feature failed'
    except Exception as e:
        pass
    assert client.set_variables('show_hidden_columns', 1)
    ret = client.desc_table(table_name, is_all=True)
    assert util.get_attr_condition_value(ret, palo_job.DescInfoAll.Field, '__DORIS_DELETE_SIGN__')
    assert client.set_variables('show_hidden_columns', 0)
    # 带有column和set，导入一张空表
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k6', 'k7', 'k8', 'k9', 'k10', 'k11', 'k12']
    set_list = ['k0=k7', 'k5=k4']
    load_data_desc = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, merge_type='DELETE',
                                              column_name_list=column_name_list, set_list=set_list)
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True)
    assert ret, 'broker load failed'
    ret = client.select_all(table_name)
    assert ret == (), 'check failed'
    # 向表中导入数据，再delete load，预期表为空
    load_data_desc = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name,
                                              column_name_list=column_name_list, set_list=set_list)
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True)
    assert ret, 'broker failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select case k6 when "true" then 1 when "false" then 0 end as k0, k1, k2, k3, k4, ' \
           'k4, k5, k6, k10, k11, k7, k8, k9 from %s.%s order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    load_data_desc = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, merge_type='DELETE',
                                              column_name_list=column_name_list, set_list=set_list)
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True)
    assert ret, 'broker load failed'
    ret = client.select_all(table_name)
    assert ret == (), 'check failed'
    client.clean(database_name)


def test_delete_broker_filter_ratio():
    """
    {
    "title": "test_delete_broker_column_set",
    "describe": "验证broker load的delete的数据过滤",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list, 
                              partition_info=DATA.baseall_tinyint_partition_info,
                              distribution_info=DATA.baseall_distribution_info,
                              keys_desc=DATA.datatype_column_uniq_key)
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'can not get table: %s' % table_name
    try:
        ret = client.enable_feature_batch_delete(table_name)
        assert ret, 'enable batch delete feature failed'
    except Exception as e:
        pass
    assert client.set_variables('show_hidden_columns', 1)
    ret = client.desc_table(table_name, is_all=True)
    assert util.get_attr_condition_value(ret, palo_job.DescInfoAll.Field, '__DORIS_DELETE_SIGN__')
    assert client.set_variables('show_hidden_columns', 0)
    # 带有column和set，导入一张空表
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k6', 'k7', 'k8', 'k9', 'k10', 'k11', 'k12']
    set_list = ['k0=k7', 'k5=k4']
    where = 'k1 > 8'
    partitions = ['p3']
    load_data_desc1 = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, merge_type='DELETE',
                                              column_name_list=column_name_list, set_list=set_list,
                                              where_clause=where, partition_list=partitions)
    ret = client.batch_load(util.get_label(), load_data_desc1, broker=broker_info, is_wait=True, max_filter_ratio=1)
    assert ret, 'broker load failed'
    ret = client.select_all(table_name)
    assert ret == (), 'check failed'
    # 向表中导入数据，再delete load
    load_data_desc2 = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name,
                                              column_name_list=column_name_list, set_list=set_list)
    ret = client.batch_load(util.get_label(), load_data_desc2, broker=broker_info, is_wait=True)
    assert ret, 'broker failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select case k6 when "true" then 1 when "false" then 0 end as k0, k1, k2, k3, k4, ' \
           'k4, k5, k6, k10, k11, k7, k8, k9 from %s.%s order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    ret = client.batch_load(util.get_label(), load_data_desc1, broker=broker_info, is_wait=True, max_filter_ratio=1)
    assert ret, 'broker load failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select case k6 when "true" then 1 when "false" then 0 end as k0, k1, k2, k3, k4, ' \
           'k4, k5, k6, k10, k11, k7, k8, k9 from %s.%s where k1 != 9 order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)

    client.clean(database_name)


def test_merge_broker_basic():
    """
    {
    "title": "test_merge_broker_basic",
    "describe": "验证broker load的merge的基本功能",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_no_agg_list,
                              distribution_info=DATA.baseall_distribution_info,
                              keys_desc=DATA.baseall_unique_key)
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'can not get table: %s' % table_name
    try:
        ret = client.enable_feature_batch_delete(table_name)
        assert ret, 'enable batch delete feature failed'
    except Exception as e:
        pass
    assert client.set_variables('show_hidden_columns', 1)
    ret = client.desc_table(table_name, is_all=True)
    assert util.get_attr_condition_value(ret, palo_job.DescInfoAll.Field, '__DORIS_DELETE_SIGN__')
    assert client.set_variables('show_hidden_columns', 0)
    # merge, 一个空表，delete on 条件命中全部数据，todo set show_hidden_columns产看表的隐藏删除数据
    load_data_desc = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, merge_type='MERGE',
                                              delete_on_predicates='k1 > 0')
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True)
    assert ret, 'broker load failed'
    ret = client.select_all(table_name)
    assert ret == (), 'check failed'
    # 向表中导入数据，再merge，delete on条件未命中数据，数据全部导入
    load_data_desc = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, merge_type='MERGE',
                                              delete_on_predicates='k1 = 0')
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True)
    assert ret, 'broker failed'
    sql = 'select * from %s.%s order by k1'
    common.check2(client, sql1=sql % (database_name, table_name), sql2=sql % (check_db, baseall_tb))
    # 再导入，delete on条件命中部分数据，命中数据被删除，其他数据保持不变
    load_data_desc = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, merge_type='MERGE', 
                                              delete_on_predicates='k2 > 0')
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True)
    assert ret, 'broker load failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.%s where k2 <= 0 order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_merge_broker_set_columns():
    """
    {
    "title": "test_merge_broker_set_columns",
    "describe": "验证broker load的merge列设置",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list,
                              distribution_info=DATA.baseall_distribution_info,
                              keys_desc=DATA.datatype_column_uniq_key)
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'can not get table: %s' % table_name
    try:
        ret = client.enable_feature_batch_delete(table_name)
        assert ret, 'enable batch delete feature failed'
    except Exception as e:
        pass
    assert client.set_variables('show_hidden_columns', 1)
    ret = client.desc_table(table_name, is_all=True)
    assert util.get_attr_condition_value(ret, palo_job.DescInfoAll.Field, '__DORIS_DELETE_SIGN__')
    assert client.set_variables('show_hidden_columns', 0)
    # 带有column和set，导入一张空表
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k6', 'k7', 'k8', 'k9', 'k10', 'k11', 'k12']
    set_list = ['k0=k7', 'k5=k4']
    load_data_desc = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, merge_type='MERGE',
                                              column_name_list=column_name_list, set_list=set_list,
                                              delete_on_predicates='k1 > 0')
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True)
    assert ret, 'broker load failed'
    ret = client.select_all(table_name)
    assert ret == (), 'check failed'
    # 向表中导入数据，再delete load，预期表为空
    load_data_desc = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, merge_type='MERGE',
                                              column_name_list=column_name_list, set_list=set_list,
                                              delete_on_predicates='k1=0')
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True)
    assert ret, 'broker failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select case k6 when "true" then 1 when "false" then 0 end as k0, k1, k2, k3, k4, ' \
           'k4, k5, k6, k10, k11, k7, k8, k9 from %s.%s order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    load_data_desc = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, merge_type='MERGE',
                                              column_name_list=column_name_list, set_list=set_list,
                                              delete_on_predicates='k7="false"')
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True)
    assert ret, 'broker load failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select case k6 when "true" then 1 when "false" then 0 end as k0, k1, k2, k3, k4, ' \
           'k4, k5, k6, k10, k11, k7, k8, k9 from %s.%s where k6 != "false" order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_merge_broker_filter_ratio():
    """
    {
    "title": "test_merge_broker_filter_ratio",
    "describe": "验证broker load的merge数据导入",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list, 
                              partition_info=DATA.baseall_tinyint_partition_info,
                              distribution_info=DATA.baseall_distribution_info,
                              keys_desc=DATA.datatype_column_uniq_key)
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'can not get table: %s' % table_name
    try:
        ret = client.enable_feature_batch_delete(table_name)
        assert ret, 'enable batch delete feature failed'
    except Exception as e:
        pass
    assert client.set_variables('show_hidden_columns', 1)
    ret = client.desc_table(table_name, is_all=True)
    assert util.get_attr_condition_value(ret, palo_job.DescInfoAll.Field, '__DORIS_DELETE_SIGN__')
    assert client.set_variables('show_hidden_columns', 0)
    # 带有column和set，导入一张空表
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k6', 'k7', 'k8', 'k9', 'k10', 'k11', 'k12']
    set_list = ['k0=k7', 'k5=k4']
    where = 'k1 > 8'
    partitions = ['p3']
    load_data_desc1 = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, merge_type='MERGE',
                                               column_name_list=column_name_list, set_list=set_list,
                                               where_clause=where, partition_list=partitions,
                                               delete_on_predicates='k1 > 0')
    ret = client.batch_load(util.get_label(), load_data_desc1, broker=broker_info, is_wait=True, max_filter_ratio=1)
    assert ret, 'broker load failed'
    ret = client.select_all(table_name)
    assert ret == (), 'check failed'
    # 向表中导入数据，再delete load
    load_data_desc2 = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, merge_type='MERGE',
                                               column_name_list=column_name_list, set_list=set_list,
                                               delete_on_predicates='k1=0')
    ret = client.batch_load(util.get_label(), load_data_desc2, broker=broker_info, is_wait=True)
    assert ret, 'broker failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select case k6 when "true" then 1 when "false" then 0 end as k0, k1, k2, k3, k4, ' \
           'k4, k5, k6, k10, k11, k7, k8, k9 from %s.%s order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    load_data_desc1 = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, merge_type='MERGE',
                                               column_name_list=column_name_list, set_list=set_list,
                                               where_clause='k5 is not null', partition_list=['p1', 'p2', 'p3', 'p4'],
                                               delete_on_predicates='k8 > "2000-01-01"')
    ret = client.batch_load(util.get_label(), load_data_desc1, broker=broker_info, is_wait=True, max_filter_ratio=1)
    assert ret, 'broker load failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select case k6 when "true" then 1 when "false" then 0 end as k0, k1, k2, k3, k4, ' \
           'k4, k5, k6, k10, k11, k7, k8, k9 from %s.%s where k10 <= "20000101" order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)

    client.clean(database_name)


def test_delete_stream_basic():
    """
    {
    "title": "test_delete_stream_basic",
    "describe": "验证stream load的delete的基本功能",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_no_agg_list,
                              distribution_info=DATA.baseall_distribution_info,
                              keys_desc=DATA.baseall_unique_key)
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'can not get table: %s' % table_name
    try:
        ret = client.enable_feature_batch_delete(table_name)
        assert ret, 'enable batch delete feature failed'
    except Exception as e:
        pass
    assert client.set_variables('show_hidden_columns', 1)
    ret = client.desc_table(table_name, is_all=True)
    assert util.get_attr_condition_value(ret, palo_job.DescInfoAll.Field, '__DORIS_DELETE_SIGN__')
    assert client.set_variables('show_hidden_columns', 0)
    # delete load, 一个空表，表中的数据仍然为空
    ret = client.stream_load(table_name, FILE.baseall_local_file, merge_type='DELETE')
    assert ret, 'stream load failed'
    ret = client.select_all(table_name)
    assert ret == (), 'check failed'
    # 向表中导入数据，再delete load，预期表为空
    ret = client.stream_load(table_name, FILE.baseall_local_file, merge_type='APPEND')
    assert ret, 'stream failed'
    sql = 'select * from %s.%s order by k1'
    common.check2(client, sql1=sql % (database_name, table_name), sql2=sql % (check_db, baseall_tb))
    ret = client.stream_load(table_name, FILE.baseall_local_file, merge_type='DELETE')
    assert ret, 'stream load failed'
    ret = client.select_all(table_name)
    assert ret == (), 'check failed'
    client.clean(database_name)


def test_delete_stream_column_set():
    """
    {
    "title": "test_delete_stream_column_set",
    "describe": "验证stream load的delete的列设置",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list,
                              distribution_info=DATA.baseall_distribution_info,
                              keys_desc=DATA.datatype_column_uniq_key)
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'can not get table: %s' % table_name
    try:
        ret = client.enable_feature_batch_delete(table_name)
        assert ret, 'enable batch delete feature failed'
    except Exception as e:
        pass
    assert client.set_variables('show_hidden_columns', 1)
    ret = client.desc_table(table_name, is_all=True)
    assert util.get_attr_condition_value(ret, palo_job.DescInfoAll.Field, '__DORIS_DELETE_SIGN__')
    assert client.set_variables('show_hidden_columns', 0)
    # 带有column和set，导入一张空表
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k6', 'k7', 'k8', 'k9',
                        'k10', 'k11', 'k12', 'k0=k7', 'k5=k4']
    ret = client.stream_load(table_name, FILE.baseall_local_file, column_name_list=column_name_list, 
                             merge_type='DELETE')
    assert ret, 'stream load failed'
    ret = client.select_all(table_name)
    assert ret == (), 'check failed'
    # 向表中导入数据，再delete load，预期表为空
    ret = client.stream_load(table_name, FILE.baseall_local_file, column_name_list=column_name_list,
                             merge_type='APPEND')
    assert ret, 'stream failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select case k6 when "true" then 1 when "false" then 0 end as k0, k1, k2, k3, k4, ' \
           'k4, k5, k6, k10, k11, k7, k8, k9 from %s.%s order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    ret = client.stream_load(table_name, FILE.baseall_local_file, column_name_list=column_name_list, 
                             merge_type='DELETE')
    assert ret, 'stream load failed'
    ret = client.select_all(table_name)
    assert ret == (), 'check failed'
    client.clean(database_name)


def test_delete_stream_filter_ratio():
    """
    {
    "title": "test_delete_stream_filter_ratio",
    "describe": "验证stream load的delete的数据过滤",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list,
                              partition_info=DATA.baseall_tinyint_partition_info,
                              distribution_info=DATA.baseall_distribution_info,
                              keys_desc=DATA.datatype_column_uniq_key)
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'can not get table: %s' % table_name
    try:
        ret = client.enable_feature_batch_delete(table_name)
        assert ret, 'enable batch delete feature failed'
    except Exception as e:
        pass
    assert client.set_variables('show_hidden_columns', 1)
    ret = client.desc_table(table_name, is_all=True)
    assert util.get_attr_condition_value(ret, palo_job.DescInfoAll.Field, '__DORIS_DELETE_SIGN__')
    assert client.set_variables('show_hidden_columns', 0)
    # 带有column和set，导入一张空表
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k6', 'k7', 'k8', 'k9',
                        'k10', 'k11', 'k12', 'k0=k7', 'k5=k4']
    where = 'k1 > 8'
    partitions = ['p3']

    ret = client.stream_load(table_name, FILE.baseall_local_file, column_name_list=column_name_list,
                             where_filter=where, partition_list=partitions, merge_type='DELETE', max_filter_ratio=1)
    assert ret, 'stream load failed'
    ret = client.select_all(table_name)
    assert ret == (), 'check failed'
    # 向表中导入数据，再delete load
    ret = client.stream_load(table_name, FILE.baseall_local_file, column_name_list=column_name_list, 
                             merge_type='APPEND')
    assert ret, 'stream load failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select case k6 when "true" then 1 when "false" then 0 end as k0, k1, k2, k3, k4, ' \
           'k4, k5, k6, k10, k11, k7, k8, k9 from %s.%s order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    ret = client.stream_load(table_name, FILE.baseall_local_file, column_name_list=column_name_list,
                             where_filter=where, partition_list=partitions, merge_type='DELETE', max_filter_ratio=1)
    assert ret, 'stream load failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select case k6 when "true" then 1 when "false" then 0 end as k0, k1, k2, k3, k4, ' \
           'k4, k5, k6, k10, k11, k7, k8, k9 from %s.%s where k1 != 9 order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)

    client.clean(database_name)


def test_merge_stream_basic():
    """
    {
    "title": "test_merge_stream_basic",
    "describe": "验证stream load的merge的基本功能",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_no_agg_list,
                              distribution_info=DATA.baseall_distribution_info,
                              keys_desc=DATA.baseall_unique_key)
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'can not get table: %s' % table_name
    try:
        ret = client.enable_feature_batch_delete(table_name)
        assert ret, 'enable batch delete feature failed'
    except Exception as e:
        pass
    assert client.set_variables('show_hidden_columns', 1)
    ret = client.desc_table(table_name, is_all=True)
    assert util.get_attr_condition_value(ret, palo_job.DescInfoAll.Field, '__DORIS_DELETE_SIGN__')
    assert client.set_variables('show_hidden_columns', 0)
    # merge, 一个空表，delete on 条件命中全部数据，todo set show_hidden_columns产看表的隐藏删除数据
    ret = client.stream_load(table_name, FILE.baseall_local_file, merge_type='MERGE', delete='k1>0')
    assert ret, 'stream load failed'
    ret = client.select_all(table_name)
    assert ret == (), 'check failed'
    # 向表中导入数据，再merge，delete on条件未命中数据，数据全部导入
    ret = client.stream_load(table_name, FILE.baseall_local_file, merge_type='MERGE', delete='k1=0')
    assert ret, 'stream load failed'
    sql = 'select * from %s.%s order by k1'
    common.check2(client, sql1=sql % (database_name, table_name), sql2=sql % (check_db, baseall_tb))
    # 再导入，delete on条件命中部分数据，命中数据被删除，其他数据保持不变
    ret = client.stream_load(table_name, FILE.baseall_local_file, merge_type='MERGE', delete='k2>0')
    assert ret, 'stream load failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.%s where k2 <= 0 order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_merge_stream_set_columns():
    """
    {
    "title": "test_merge_stream_set_columns",
    "describe": "验证broker load的merge导入",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list,
                              distribution_info=DATA.baseall_distribution_info,
                              keys_desc=DATA.datatype_column_uniq_key)
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'can not get table: %s' % table_name
    try:
        ret = client.enable_feature_batch_delete(table_name)
        assert ret, 'enable batch delete feature failed'
    except Exception as e:
        pass
    assert client.set_variables('show_hidden_columns', 1)
    ret = client.desc_table(table_name, is_all=True)
    assert util.get_attr_condition_value(ret, palo_job.DescInfoAll.Field, '__DORIS_DELETE_SIGN__')
    assert client.set_variables('show_hidden_columns', 0)
    # 带有column和set，导入一张空表
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k6', 'k7', 'k8', 'k9', 
                        'k10', 'k11', 'k12', 'k0=k7', 'k5=k4']
    ret = client.stream_load(table_name, FILE.baseall_local_file, column_name_list=column_name_list, 
                             merge_type='MERGE', delete='k1>0')
    assert ret, 'stream load failed'
    ret = client.select_all(table_name)
    assert ret == (), 'check failed'
    # 向表中导入数据，再delete load，预期表为空
    ret = client.stream_load(table_name, FILE.baseall_local_file, column_name_list=column_name_list, 
                             merge_type='MERGE', delete='k1=0')
    assert ret, 'stream load failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select case k6 when "true" then 1 when "false" then 0 end as k0, k1, k2, k3, k4, ' \
           'k4, k5, k6, k10, k11, k7, k8, k9 from %s.%s order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    ret = client.stream_load(table_name, FILE.baseall_local_file, column_name_list=column_name_list, 
                             merge_type='MERGE', delete='k7="false"')
    assert ret, 'broker load failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select case k6 when "true" then 1 when "false" then 0 end as k0, k1, k2, k3, k4, ' \
           'k4, k5, k6, k10, k11, k7, k8, k9 from %s.%s where k6 != "false" order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_merge_stream_filter_ratio():
    """
    {
    "title": "test_merge_stream_filter_ratio",
    "describe": "验证stream load的merge数据过滤",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list,
                              partition_info=DATA.baseall_tinyint_partition_info,
                              distribution_info=DATA.baseall_distribution_info,
                              keys_desc=DATA.datatype_column_uniq_key)
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'can not get table: %s' % table_name
    try:
        ret = client.enable_feature_batch_delete(table_name)
        assert ret, 'enable batch delete feature failed'
    except Exception as e:
        pass
    assert client.set_variables('show_hidden_columns', 1)
    ret = client.desc_table(table_name, is_all=True)
    assert util.get_attr_condition_value(ret, palo_job.DescInfoAll.Field, '__DORIS_DELETE_SIGN__')
    assert client.set_variables('show_hidden_columns', 0)
    # 带有column和set，导入一张空表
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k6', 'k7', 'k8', 'k9', 
                        'k10', 'k11', 'k12', 'k0=k7', 'k5=k4']
    where = 'k1 > 8'
    partitions = ['p3']
    ret = client.stream_load(table_name, FILE.baseall_local_file, column_name_list=column_name_list, 
                             where_filter=where, partition_list=partitions, max_filter_ratio=1, 
                             merge_type='MERGE', delete='k1>0')
    assert ret, 'stream load failed'
    ret = client.select_all(table_name)
    assert ret == (), 'check failed'
    # 向表中导入数据，再delete load
    ret = client.stream_load(table_name, FILE.baseall_local_file, column_name_list=column_name_list, 
                             merge_type='MERGE', delete='k1=0')
    assert ret, 'stream load failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select case k6 when "true" then 1 when "false" then 0 end as k0, k1, k2, k3, k4, ' \
           'k4, k5, k6, k10, k11, k7, k8, k9 from %s.%s order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    ret = client.stream_load(table_name, FILE.baseall_local_file, column_name_list=column_name_list, 
                             where_filter='k5 is not null', partition_list=['p1', 'p2', 'p3', 'p4'],
                             merge_type='MERGE', delete='k8 > "2000-01-01"')
    assert ret, 'broker load failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select case k6 when "true" then 1 when "false" then 0 end as k0, k1, k2, k3, k4, ' \
           'k4, k5, k6, k10, k11, k7, k8, k9 from %s.%s where k10 <= "20000101" order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)

    client.clean(database_name)


def test_delete_routine_basic():
    """
    {
    "title": "test_delete_routine_basic",
    "describe": "验证routine load的delete的基本功能",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_no_agg_list,
                              distribution_info=DATA.baseall_distribution_info,
                              keys_desc=DATA.baseall_unique_key)
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'can not get table: %s' % table_name
    # enable batch delete & check
    try:
        ret = client.enable_feature_batch_delete(table_name)
        assert ret, 'enable batch delete feature failed'
    except Exception as e:
        pass
    assert client.set_variables('show_hidden_columns', 1)
    ret = client.desc_table(table_name, is_all=True)
    assert util.get_attr_condition_value(ret, palo_job.DescInfoAll.Field, '__DORIS_DELETE_SIGN__')
    assert client.set_variables('show_hidden_columns', 0)
    # 1.delete load, 一个空表，表中的数据仍然为空
    # create routine load
    routine_load_job_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    routine_load_property.set_merge_type('DELETE')
    ret = client.routine_load(table_name, routine_load_job_name, routine_load_property=routine_load_property)
    assert ret, 'routine load create failed'
    routine_load_job = palo_job.RoutineLoadJob(client.show_routine_load(routine_load_job_name)[0])
    ret = (routine_load_job.get_merge_type() == 'DELETE')
    common.assert_stop_routine_load(ret, client, routine_load_job_name, 'expect delete merge type')
    client.wait_routine_load_state(routine_load_job_name)
    # send kafka data & check, expect empty table
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    client.wait_routine_load_commit(routine_load_job_name, 15)
    ret = client.select_all(table_name)
    common.assert_stop_routine_load(ret == (), client, routine_load_job_name, 'check error')
    # 2.向表中导入数据，再delete load，预期表为空
    # stream load and check
    ret = client.stream_load(table_name, FILE.baseall_local_file, merge_type='APPEND')
    common.assert_stop_routine_load(ret, client, routine_load_job_name, 'stream load failed')
    sql = 'select * from %s.%s order by k1'
    common.check2(client, sql1=sql % (database_name, table_name), sql2=sql % (check_db, baseall_tb))
    # send kafka data & check
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    client.wait_routine_load_commit(routine_load_job_name, 30)
    ret = client.select_all(table_name)
    client.stop_routine_load(routine_load_job_name)
    assert ret == (), 'check failed'
    client.clean(database_name)


def test_delete_routine_column_set():
    """
    {
    "title": "test_delete_routine_column_set",
    "describe": "验证routine load的delete，设置column",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list,
                              distribution_info=DATA.baseall_distribution_info,
                              keys_desc=DATA.datatype_column_uniq_key)
    assert ret, 'create table failed'
    try:
        ret = client.enable_feature_batch_delete(table_name)
        assert ret, 'enable batch delete feature failed'
    except Exception as e:
        pass
    assert client.set_variables('show_hidden_columns', 1)
    ret = client.desc_table(table_name, is_all=True)
    assert util.get_attr_condition_value(ret, palo_job.DescInfoAll.Field, '__DORIS_DELETE_SIGN__')
    assert client.set_variables('show_hidden_columns', 0)
    # 带有column和set，导入一张空表
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k6', 'k7', 'k8', 'k9',
                        'k10', 'k11', 'k12', 'k0=k7', 'k5=k4']
    routine_load_job_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    routine_load_property.set_merge_type('DELETE')
    routine_load_property.set_column_mapping((column_name_list))
    ret = client.routine_load(table_name, routine_load_job_name, routine_load_property=routine_load_property)
    assert ret, 'routine load create failed'
    client.wait_routine_load_state(routine_load_job_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    client.wait_routine_load_commit(routine_load_job_name, 15)
    ret = client.select_all(table_name)
    assert ret == (), 'check failed'
    # 向表中导入数据，再delete load，预期表为空
    ret = client.stream_load(table_name, FILE.baseall_local_file, column_name_list=column_name_list, 
                             merge_type='APPEND')
    common.assert_stop_routine_load(ret, client, routine_load_job_name, 'stream load failed')
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select case k6 when "true" then 1 when "false" then 0 end as k0, k1, k2, k3, k4, ' \
           'k4, k5, k6, k10, k11, k7, k8, k9 from %s.%s order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    client.wait_routine_load_commit(routine_load_job_name, 30)
    ret = client.select_all(table_name)
    client.stop_routine_load(routine_load_job_name)
    assert ret == (), 'check failed'
    client.clean(database_name)


def test_delete_routine_filter_ratio():
    """
    {
    "title": "test_delete_routine_filter_ratio",
    "describe": "验证routine load的delete的数据过滤",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list,
                              partition_info=DATA.baseall_tinyint_partition_info,
                              distribution_info=DATA.baseall_distribution_info,
                              keys_desc=DATA.datatype_column_uniq_key)
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'can not get table: %s' % table_name
    try:
        ret = client.enable_feature_batch_delete(table_name)
        assert ret, 'enable batch delete feature failed'
    except Exception as e:
        pass
    assert client.set_variables('show_hidden_columns', 1)
    ret = client.desc_table(table_name, is_all=True)
    assert util.get_attr_condition_value(ret, palo_job.DescInfoAll.Field, '__DORIS_DELETE_SIGN__')
    assert client.set_variables('show_hidden_columns', 0)
    # 带有column和set，导入一张空表
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k6', 'k7', 'k8', 'k9',
                        'k10', 'k11', 'k12', 'k0=k7', 'k5=k4']
    routine_load_job_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    routine_load_property.set_merge_type('DELETE')
    routine_load_property.set_column_mapping(column_name_list)
    routine_load_property.set_where_predicates('k1 > 8')
    routine_load_property.set_partitions(['p3'])
    routine_load_property.set_max_error_number(15)
    ret = client.routine_load(table_name, routine_load_job_name, routine_load_property=routine_load_property)
    assert ret, 'routine load create failed'
    client.wait_routine_load_state(routine_load_job_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    client.wait_routine_load_commit(routine_load_job_name, 1)
    ret = client.select_all(table_name)
    common.assert_stop_routine_load(ret == (), client, routine_load_job_name, 'check failed')
    # 向表中导入数据，再delete load
    ret = client.stream_load(table_name, FILE.baseall_local_file, column_name_list=column_name_list,
                             merge_type='APPEND')
    common.assert_stop_routine_load(ret, client, routine_load_job_name, 'stream load failed')

    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select case k6 when "true" then 1 when "false" then 0 end as k0, k1, k2, k3, k4, ' \
           'k4, k5, k6, k10, k11, k7, k8, k9 from %s.%s order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    client.wait_routine_load_commit(routine_load_job_name, 2)
    ret = client.show_routine_load(routine_load_job_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    error_rows = routine_load_job.get_error_rows()
    state = routine_load_job.get_state()
    client.stop_routine_load(routine_load_job_name)
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select case k6 when "true" then 1 when "false" then 0 end as k0, k1, k2, k3, k4, ' \
           'k4, k5, k6, k10, k11, k7, k8, k9 from %s.%s where k1 != 9 order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_merge_routine_basic():
    """
    {
    "title": "test_merge_routine_basic",
    "describe": "验证routine load的merge基本功能",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_no_agg_list,
                              distribution_info=DATA.baseall_distribution_info,
                              keys_desc=DATA.baseall_unique_key)
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'can not get table: %s' % table_name
    try:
        ret = client.enable_feature_batch_delete(table_name)
        assert ret, 'enable batch delete feature failed'
    except Exception as e:
        pass
    assert client.set_variables('show_hidden_columns', 1)
    ret = client.desc_table(table_name, is_all=True)
    assert util.get_attr_condition_value(ret, palo_job.DescInfoAll.Field, '__DORIS_DELETE_SIGN__')
    assert client.set_variables('show_hidden_columns', 0)
    # merge, 一个空表，delete on 条件命中全部数据，todo set show_hidden_columns产看表的隐藏删除数据
    routine_load_job_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    routine_load_property.set_merge_type('MERGE')
    routine_load_property.set_delete_on_predicates('k1 % 3 = 0')
    ret = client.routine_load(table_name, routine_load_job_name, routine_load_property=routine_load_property)
    assert ret, 'routine load create failed'
    client.wait_routine_load_state(routine_load_job_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    client.wait_routine_load_commit(routine_load_job_name, 15)
    client.stop_routine_load(routine_load_job_name)
    ret = client.select_all(table_name)
    assert len(ret) == 10, 'check failed'
    # 向表中导入数据，再merge，delete on条件未命中数据，数据全部导入
    ret = client.stream_load(table_name, FILE.baseall_local_file, merge_type='MERGE', delete='k1=0')
    assert ret, 'stream load failed'
    sql = 'select * from %s.%s order by k1'
    common.check2(client, sql1=sql % (database_name, table_name), sql2=sql % (check_db, baseall_tb))
    # 再导入，delete on条件命中部分数据，命中数据被删除，其他数据保持不变
    routine_load_job_name = util.get_label()
    routine_load_property.set_delete_on_predicates('k9 > 0')
    # 设置了offset和分区
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    ret = client.routine_load(table_name, routine_load_job_name, routine_load_property=routine_load_property)
    assert ret, 'routine load create failed'
    client.wait_routine_load_state(routine_load_job_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    client.wait_routine_load_commit(routine_load_job_name, 15)
    client.stop_routine_load(routine_load_job_name)
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.%s where k9 <= 0 order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_merge_routine_set_columns():
    """
    {
    "title": "test_merge_routine_set_columns",
    "describe": "验证routine load的merge的列设置",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list,
                              distribution_info=DATA.baseall_distribution_info,
                              keys_desc=DATA.datatype_column_uniq_key)
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'can not get table: %s' % table_name
    try:
        ret = client.enable_feature_batch_delete(table_name)
        assert ret, 'enable batch delete feature failed'
    except Exception as e:
        pass
    assert client.set_variables('show_hidden_columns', 1)
    ret = client.desc_table(table_name, is_all=True)
    assert util.get_attr_condition_value(ret, palo_job.DescInfoAll.Field, '__DORIS_DELETE_SIGN__')
    assert client.set_variables('show_hidden_columns', 0)
    # 带有column和set，导入一张空表
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k6', 'k7', 'k8', 'k9',
                        'k10', 'k11', 'k12', 'k0=k7', 'k5=k4']
    routine_load_job_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    routine_load_property.set_merge_type('MERGE')
    routine_load_property.set_delete_on_predicates('k1 > 0')
    routine_load_property.set_column_mapping(column_name_list)
    ret = client.routine_load(table_name, routine_load_job_name, routine_load_property=routine_load_property)
    assert ret, 'routine load create failed'
    client.wait_routine_load_state(routine_load_job_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    client.wait_routine_load_commit(routine_load_job_name, 15)
    client.stop_routine_load(routine_load_job_name)
    ret = client.select_all(table_name)
    assert ret == (), 'check failed'
    # 向表中导入数据，再delete load
    ret = client.stream_load(table_name, FILE.baseall_local_file, column_name_list=column_name_list, 
                             merge_type='APPEND')
    assert ret, 'stream load failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select case k6 when "true" then 1 when "false" then 0 end as k0, k1, k2, k3, k4, ' \
           'k4, k5, k6, k10, k11, k7, k8, k9 from %s.%s order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    routine_load_job_name = util.get_label()
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    routine_load_property.set_delete_on_predicates('k11<0')
    ret = client.routine_load(table_name, routine_load_job_name, routine_load_property=routine_load_property)
    assert ret, 'routine load create failed'
    client.wait_routine_load_state(routine_load_job_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    client.wait_routine_load_commit(routine_load_job_name, 15)
    client.stop_routine_load(routine_load_job_name)
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select case k6 when "true" then 1 when "false" then 0 end as k0, k1, k2, k3, k4, ' \
           'k4, k5, k6, k10, k11, k7, k8, k9 from %s.%s where k8 >= 0 order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_merge_routine_filter_ratio():
    """
    {
    "title": "test_merge_routine_filter_ratio",
    "describe": "验证routine load的merge的数据过滤",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list, 
                              partition_info=DATA.baseall_tinyint_partition_info,
                              distribution_info=DATA.baseall_distribution_info,
                              keys_desc=DATA.datatype_column_uniq_key)
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'can not get table: %s' % table_name
    try:
        ret = client.enable_feature_batch_delete(table_name)
        assert ret, 'enable batch delete feature failed'
    except Exception as e:
        pass
    assert client.set_variables('show_hidden_columns', 1)
    ret = client.desc_table(table_name, is_all=True)
    assert util.get_attr_condition_value(ret, palo_job.DescInfoAll.Field, '__DORIS_DELETE_SIGN__')
    assert client.set_variables('show_hidden_columns', 0)
    # 带有column和set，导入一张空表
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k6', 'k7', 'k8', 'k9',
                        'k10', 'k11', 'k12', 'k0=k7', 'k5=k4']
    where = 'k1 > 8'
    partitions = ['p3']
    routine_load_job_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    routine_load_property.set_merge_type('MERGE')
    routine_load_property.set_delete_on_predicates('k1 > 0')
    routine_load_property.set_column_mapping(column_name_list)
    routine_load_property.set_where_predicates(where)
    routine_load_property.set_partitions(partitions)
    routine_load_property.set_max_error_number(15)
    ret = client.routine_load(table_name, routine_load_job_name, routine_load_property=routine_load_property)
    assert ret, 'routine load create failed'
    client.wait_routine_load_state(routine_load_job_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    client.wait_routine_load_commit(routine_load_job_name, 1)
    client.stop_routine_load(routine_load_job_name)
    ret = client.select_all(table_name)
    assert ret == (), 'check failed'
    # 向表中导入数据，再delete load
    ret = client.stream_load(table_name, FILE.baseall_local_file, column_name_list=column_name_list,
                             merge_type='APPEND')
    assert ret, 'stream load failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select case k6 when "true" then 1 when "false" then 0 end as k0, k1, k2, k3, k4, ' \
           'k4, k5, k6, k10, k11, k7, k8, k9 from %s.%s order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    # create routine delete load
    routine_load_job_name = util.get_label()
    routine_load_property.set_delete_on_predicates('k9 > "2000-01-01 00:00:00"')
    routine_load_property.set_where_predicates('k5 is not null')
    routine_load_property.set_partitions(['p1', 'p2', 'p3', 'p4'])
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    ret = client.routine_load(table_name, routine_load_job_name, routine_load_property=routine_load_property)
    assert ret, 'routine load create failed'
    client.wait_routine_load_state(routine_load_job_name)

    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    client.wait_routine_load_commit(routine_load_job_name, 15)
    client.stop_routine_load(routine_load_job_name)

    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select case k6 when "true" then 1 when "false" then 0 end as k0, k1, k2, k3, k4, ' \
           'k4, k5, k6, k10, k11, k7, k8, k9 from %s.%s where k11 <= "20000101" order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_delete_with_delete_on():
    """
    {
    "title": "test_delete_with_delete_on",
    "describe": "验证当delete与delete on条件连用时报错",
    "tag": "p1,function,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_no_agg_list,
                              distribution_info=DATA.baseall_distribution_info,
                              keys_desc=DATA.baseall_unique_key)
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'can not get table: %s' % table_name
    try:
        ret = client.enable_feature_batch_delete(table_name)
        assert ret, 'enable batch delete feature failed'
    except Exception as e:
        pass
    assert client.set_variables('show_hidden_columns', 1)
    ret = client.desc_table(table_name, is_all=True)
    assert util.get_attr_condition_value(ret, palo_job.DescInfoAll.Field, '__DORIS_DELETE_SIGN__')
    assert client.set_variables('show_hidden_columns', 0)
    # broker load failed
    load_data_desc = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, merge_type='DELETE', 
                                              delete_on_predicates='k1 > 0')
    msg = 'not support DELETE ON clause when merge type is not MERGE'
    util.assert_return(False, msg, client.batch_load, util.get_label(), load_data_desc, broker=broker_info)
    # stream load failed
    ret = client.stream_load(table_name, FILE.baseall_local_file, merge_type='DELETE', delete='k1>0')
    assert not ret, 'expect stream load failed'
    # routine load failed
    routine_load_job_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    routine_load_property.set_merge_type('DELETE')
    routine_load_property.set_delete_on_predicates('k1 > 0')
    ret = client.routine_load(table_name, routine_load_job_name, routine_load_property=routine_load_property)
    assert not ret, 'expect create routine load failed'
    client.clean(database_name)


def test_merge_with_delete_on():
    """
    {
    "title": "test_merge_with_delete_on",
    "describe": "验证merge必须带有delete on条件，否则报错；测试delete on条件，尤其是和set连用时",
    "tag": "p1,function,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list,
                              partition_info=DATA.baseall_tinyint_partition_info,
                              distribution_info=DATA.baseall_distribution_info,
                              keys_desc=DATA.datatype_column_uniq_key)
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'can not get table: %s' % table_name
    try:
        ret = client.enable_feature_batch_delete(table_name)
        assert ret, 'enable batch delete feature failed'
    except Exception as e:
        pass
    assert client.set_variables('show_hidden_columns', 1)
    ret = client.desc_table(table_name, is_all=True)
    assert util.get_attr_condition_value(ret, palo_job.DescInfoAll.Field, '__DORIS_DELETE_SIGN__')
    assert client.set_variables('show_hidden_columns', 0)
    # load fail without delete on
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k6', 'k7', 'k8', 'k9', 'k10', 'k11', 'k12']
    set_list = ['k0=k7', 'k5=k4']
    stream_column_list = ['k1', 'k2', 'k3', 'k4', 'k6', 'k7', 'k8', 'k9',
                          'k10', 'k11', 'k12', 'k0=k7', 'k5=k4']
    load_data_desc = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, merge_type='MERGE',
                                              column_name_list=column_name_list, set_list=set_list)
    msg = 'Excepted DELETE ON clause when merge type is MERGE'
    util.assert_return(False, msg, client.batch_load, util.get_label(), load_data_desc, broker=broker_info)
    ret = client.stream_load(table_name, FILE.baseall_local_file, merge_type='MERGE', 
                             column_name_list=stream_column_list)
    assert not ret, 'expect stream load failed'
    # set: k0=k6, delete: k0 = true -> fail
    load_data_desc = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, 
                                              merge_type='MERGE', column_name_list=column_name_list, 
                                              set_list=set_list, delete_on_predicates='k0=true')
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True)
    assert not ret, 'expect failed. unknown reference column, column=__DORIS_DELETE_SIGN__, reference=k0'
    ret = client.stream_load(table_name, FILE.baseall_local_file, merge_type='MERGE', 
                             column_name_list=stream_column_list, delete='k0=true')
    assert not ret, 'expect failed. unknown reference column, column=__DORIS_DELETE_SIGN__, reference=k0'
    # set: k2=k2/2+1, delete: k2 & k1 -> succ
    set_list = ['k0=k7', 'k5=k4', 'k2=k2/2+1']
    load_data_desc = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name,
                                              merge_type='MERGE', column_name_list=column_name_list,
                                              set_list=set_list, delete_on_predicates='k2>0 and abs(k1) = 1')
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True)
    assert ret, 'broker load failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select case k6 when "true" then 1 when "false" then 0 end as k0, k1, cast(k2/2 + 1 as int), k3, k4, ' \
           'k4, k5, k6, k10, k11, k7, k8, k9 from %s.%s where k1!=1 order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    stream_column_list = ['k1', 'k2', 'k3', 'k4', 'k6', 'k7', 'k8', 'k9',
                          'k10', 'k11', 'k12', 'k0=k7', 'k5=k4', 'k2=k2/2 + 1']
    ret = client.stream_load(table_name, FILE.baseall_local_file, merge_type='MERGE',
                             column_name_list=stream_column_list, delete='k2 <= 0 or k1 not in (1)')
    assert ret, 'stream load failed'
    sql1 = 'select * from %s.%s' % (database_name, table_name)
    sql2 = 'select case k6 when "true" then 1 when "false" then 0 end as k0, k1, cast(k2/2 + 1 as int), k3, k4, ' \
           'k4, k5, k6, k10, k11, k7, k8, k9 from %s.%s where k1=1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_delete_merge_special():
    """
    {
    "title": "test_delete_merge_special",
    "describe": "delete & merge 特殊值",
    "tag": "p1,system,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.char_normal_column_no_agg_list,
                              distribution_info=DATA.hash_distribution_info,
                              keys_desc=DATA.unique_key, set_null=True)
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'can not get table: %s' % table_name
    try:
        ret = client.enable_feature_batch_delete(table_name)
        assert ret, 'enable batch delete feature failed'
    except Exception as e:
        pass
    assert client.set_variables('show_hidden_columns', 1)
    ret = client.desc_table(table_name, is_all=True)
    assert util.get_attr_condition_value(ret, palo_job.DescInfoAll.Field, '__DORIS_DELETE_SIGN__')
    assert client.set_variables('show_hidden_columns', 0)
    # load
    load_data_desc = palo_client.LoadDataInfo(FILE.test_char_hdfs_file, table_name, merge_type='DELETE')
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True)
    assert ret, 'broker load failed'
    ret = client.select_all(table_name)
    assert ret == (), 'check failed'

    load_data_desc = palo_client.LoadDataInfo(FILE.test_char_hdfs_file, table_name, merge_type='MERGE', 
                                              delete_on_predicates='k1 is not null')
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True)
    assert ret, 'broker failed'
    ret = client.select_all(table_name)
    assert ret == ((None, None),)
    
    load_data_desc = palo_client.LoadDataInfo(FILE.test_char_hdfs_file, table_name, merge_type='MERGE', 
                                              delete_on_predicates='k1 is null')
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True)
    assert ret, 'broker load failed'
    ret1 = client.select_all(table_name)
    ret2 = ((u'hello', u'hello'), (u'H', u'H'), (u'hello,hello', u'hello,hello'), (u'h', u'h'),
            (u'\u4ed3\u5e93', u'\u5b89\u5168'), (u'', u''))
    util.check(ret1, ret2, True)
    # 当k1文件中的值为Null时，delete on条件k1="仓库"返回null，该条数据认为是错误数据被过滤，需设置max_filter_ratio
    load_data_desc = palo_client.LoadDataInfo(FILE.test_char_hdfs_file, table_name, merge_type='MERGE', 
                                              delete_on_predicates='k1="仓库"')
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True)
    assert not ret, 'expect broker load failed'
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True, max_filter_ratio=0.2)
    assert ret, 'broker load failed'
    ret1 = client.select_all(table_name)
    ret2 = ((u'H', u'H'), (u'hello,hello', u'hello,hello'), (u'h', u'h'), (u'', u''), (u'hello', u'hello'))
    util.check(ret1, ret2, True)

    load_data_desc = palo_client.LoadDataInfo(FILE.test_char_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True)
    assert ret, 'broker load failed'
    load_data_desc = palo_client.LoadDataInfo(FILE.test_char_hdfs_file, table_name, merge_type='DELETE')
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True)
    assert ret, 'broker load failed'
    ret = client.select_all(table_name)
    assert ret == (), 'check failed'
    client.clean(database_name)


def test_enable_batch_delete():
    """
    {
    "title": "test_enable_batch_delete",
    "describe": "多次执行enable，结果正确。enable后，执行drop column，然后导入验证。未enable的时候delete load。agg和duplicate表agg",
    "tag": "p1,system,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_no_agg_list,
                              distribution_info=DATA.baseall_distribution_info,
                              keys_desc=DATA.baseall_unique_key)
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'can not get table: %s' % table_name
    try:
        client.set_variables('show_hidden_columns', 1)
        ret = client.schema_change_drop_column(table_name, ['__DORIS_DELETE_SIGN__'], is_wait_job=True)
        assert ret
    except Exception as e:
        pass
    # 不enable，执行delete load报错
    msg = 'load by MERGE or DELETE need to upgrade table to support batch delete'
    load_data_desc = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, merge_type='MERGE',
                                              delete_on_predicates='k1 > 0')
    util.assert_return(False, msg, client.batch_load, util.get_label(), load_data_desc, broker=broker_info)
    # enable，导入成功
    try:
        ret = client.enable_feature_batch_delete(table_name)
        assert ret, 'enable batch delete feature failed'
    except Exception as e:
        pass
    assert client.set_variables('show_hidden_columns', 1)
    ret = client.desc_table(table_name, is_all=True)
    assert util.get_attr_condition_value(ret, palo_job.DescInfoAll.Field, '__DORIS_DELETE_SIGN__')
    assert util.get_attr_condition_value(ret, palo_job.DescInfoAll.Field, '__DORIS_VERSION_COL__')
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True)
    assert ret
    print(client.show_variables('show_hidden_columns'))
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select k1, k2, k3, k4, k5, k6, k10, k11, k7, k8, k9, 1, 2 from %s.%s order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    # 再次enable，enable失败
    msg = 'Can not enable batch delete support, already supported batch delete.'
    util.assert_return(False, msg, client.enable_feature_batch_delete, table_name)
    # drop column隐藏列成功
    assert client.set_variables('show_hidden_columns', 1)
    msg = 'Nothing is changed. please check your alter stmt.'
    ret = client.schema_change_drop_column(table_name, ['__DORIS_DELETE_SIGN__', '__DORIS_VERSION_COL__'],
                                           is_wait_job=True)
    assert ret
    ret = client.desc_table(table_name, is_all=True)
    assert util.get_attr_condition_value(ret, palo_job.DescInfoAll.Field, '__DORIS_DELETE_SIGN__') is None
    assert util.get_attr_condition_value(ret, palo_job.DescInfoAll.Field, '__DORIS_VERSION_COL__') is None
    msg = 'load by MERGE or DELETE need to upgrade table to support batch delete'
    load_data_desc = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, merge_type='MERGE',
                                              delete_on_predicates='k1 > 0')
    util.assert_return(False, msg, client.batch_load, util.get_label(), load_data_desc, broker=broker_info)
    client.clean(database_name)


def test_delete_merge_rollup_1():
    """
    {
    "title": "test_delete_merge_rollup_1",
    "describe": "enable，导入，创建rollup，导入",
    "tag": "p1,system"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_no_agg_list,
                              distribution_info=DATA.baseall_distribution_info,
                              keys_desc=DATA.baseall_unique_key,
                              enable_unique_key_merge_on_write="false")
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'can not get table: %s' % table_name
    try:
        ret = client.enable_feature_batch_delete(table_name)
        assert ret, 'enable batch delete feature failed'
    except Exception as e:
        pass
    assert client.set_variables('show_hidden_columns', 1)
    ret = client.desc_table(table_name, is_all=True)
    assert util.get_attr_condition_value(ret, palo_job.DescInfoAll.Field, '__DORIS_DELETE_SIGN__')
    assert util.get_attr_condition_value(ret, palo_job.DescInfoAll.Field, '__DORIS_VERSION_COL__')
    load_data_desc = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, merge_type='MERGE',
                                              delete_on_predicates='k1 = 1')
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True)
    assert ret
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select k1, k2, k3, k4, k5, k6, k10, k11, k7, k8, k9, k1 = 1, 2 from %s.%s order by k1' \
           % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    rollup_list = ['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k10', 'k11']
    ret = client.create_rollup_table(table_name, index_name, rollup_list, is_wait=True)
    assert ret
    ret = client.desc_table(table_name, is_all=True)
    hidden_column = util.get_attr_condition_list(ret, palo_job.DescInfoAll.Field, '__DORIS_DELETE_SIGN__')
    assert len(hidden_column) == 2, 'expect base table & mv have __DORIS_DELETE_SIGN__'
    hidden_column = util.get_attr_condition_list(ret, palo_job.DescInfoAll.Field, '__DORIS_VERSION_COL__')
    assert len(hidden_column) == 1, 'expect base table has __DORIS_VERSION_COL__'
    sql1 = 'select %s from %s.%s order by k1' % (','.join(rollup_list), database_name, table_name)
    sql2 = 'select %s from %s.%s order by k1' % (','.join(rollup_list), check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    idx = common.get_explain_rollup(client, sql1)
    assert index_name in idx
    assert client.set_variables('show_hidden_columns', 0)
    sql1 = 'select %s from %s.%s order by k1' % (','.join(rollup_list), database_name, table_name)
    sql2 = 'select %s from %s.%s where k1 != 1 order by k1' % (','.join(rollup_list), check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    idx = common.get_explain_rollup(client, sql1)
    assert index_name in idx
    load_data_desc = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, merge_type='MERGE', 
                                              delete_on_predicates='k1 != 1')
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True)
    assert ret
    sql1 = 'select %s from %s.%s' % (','.join(rollup_list), database_name, table_name)
    sql2 = 'select %s from %s.%s WHERE k1 = 1' % (','.join(rollup_list), check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    idx = common.get_explain_rollup(client, sql1)
    assert index_name in idx
    sql1 = 'select %s from %s.%s' % (','.join(rollup_list), database_name, table_name)
    sql2 = 'select %s from %s.%s WHERE k1 = 1' % (','.join(rollup_list), check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    assert client.set_variables('show_hidden_columns', 1)
    sql1 = 'select %s from %s.%s order by k1' % (','.join(rollup_list), database_name, table_name)
    sql2 = 'select %s from %s.%s order by k1' % (','.join(rollup_list), check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    idx = common.get_explain_rollup(client, sql1)
    assert index_name in idx
    client.clean(database_name)


def test_delete_merge_rollup_2():
    """
    {
    "title": "test_delete_merge_rollup_2",
    "describe": "建表，创建rollup，导入，enable，导入",
    "tag": "p1,system"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    # creat tb, create rollup & load
    ret = client.create_table(table_name, DATA.baseall_column_no_agg_list,
                              distribution_info=DATA.baseall_distribution_info,
                              keys_desc=DATA.baseall_unique_key,
                              enable_unique_key_merge_on_write="false")
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'can not get table: %s' % table_name
    rollup_list = ['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k10', 'k11']
    ret = client.create_rollup_table(table_name, index_name, rollup_list, is_wait=True)
    assert ret
    assert client.get_index(table_name, index_name)
    load_data_desc = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, merge_type='APPEND')
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True)
    assert ret
    sql1 = 'select * from %s.%s order by k1'
    common.check2(client, sql1=sql1 % (database_name, table_name), sql2=sql1 % (check_db, baseall_tb))
    sql1 = 'select %s from %s.%s order by k1' % (','.join(rollup_list), database_name, table_name)
    sql2 = 'select %s from %s.%s order by k1' % (','.join(rollup_list), check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    idx = common.get_explain_rollup(client, sql1)
    assert index_name in idx
    # enable
    try:
        ret = client.enable_feature_batch_delete(table_name)
        assert ret, 'enable batch delete feature failed'
    except Exception as e:
        pass
    assert client.set_variables('show_hidden_columns', 1)
    ret = client.desc_table(table_name, is_all=True)
    hidden_column = util.get_attr_condition_list(ret, palo_job.DescInfoAll.Field, '__DORIS_DELETE_SIGN__')
    assert len(hidden_column) == 2, 'expect base table & mv have __DORIS_DELETE_SIGN__'
    hidden_column = util.get_attr_condition_list(ret, palo_job.DescInfoAll.Field, '__DORIS_VERSION_COL__')
    assert len(hidden_column) == 1, 'expect base table has __DORIS_VERSION_COL__'

    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select k1, k2, k3, k4, k5, k6, k10, k11, k7, k8, k9, 0, 2 from %s.%s order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    sql1 = 'select %s from %s.%s order by k1' % (','.join(rollup_list), database_name, table_name)
    sql2 = 'select %s from %s.%s order by k1' % (','.join(rollup_list), check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    idx = common.get_explain_rollup(client, sql1)
    assert index_name in idx

    assert client.set_variables('show_hidden_columns', 0)
    common.check2(client, sql1=sql1, sql2=sql2)
    idx = common.get_explain_rollup(client, sql1)
    assert index_name in idx

    load_data_desc = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, merge_type='MERGE', 
                                              delete_on_predicates='k1 > 10')
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True)
    assert ret
    sql1 = 'select %s from %s.%s order by k1' % (','.join(rollup_list), database_name, table_name)
    sql2 = 'select %s from %s.%s where k1 <= 10 order by k1' % (','.join(rollup_list), check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    idx = common.get_explain_rollup(client, sql1)
    assert index_name in idx

    assert client.set_variables('show_hidden_columns', 1)
    sql2 = 'select %s from %s.%s order by k1' % (','.join(rollup_list), check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    idx = common.get_explain_rollup(client, sql1)
    assert index_name in idx
    client.clean(database_name)


def test_add_column_delete_load():
    """
    {
    "title": "test_add_column_delete_load",
    "describe": "带rollup的表，加列不影响delete load",
    "tag": "p1,system"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    # 建表
    ret = client.create_table(table_name, DATA.baseall_column_no_agg_list,
                              distribution_info=DATA.baseall_distribution_info,
                              keys_desc=DATA.baseall_unique_key,
                              enable_unique_key_merge_on_write="false")
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'can not get table: %s' % table_name
    # 创建物化视图
    rollup_list = ['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k10', 'k11']
    ret = client.create_rollup_table(table_name, index_name, rollup_list, is_wait=True)
    assert ret
    assert client.get_index(table_name, index_name)
    # 导入 & 验证
    load_data_desc = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, merge_type='APPEND')
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True)
    assert ret
    sql1 = 'select * from %s.%s order by k1'
    common.check2(client, sql1=sql1 % (database_name, table_name), sql2=sql1 % (check_db, baseall_tb))
    # enable批量删除，并验证
    try:
        ret = client.enable_feature_batch_delete(table_name)
        assert ret, 'enable batch delete feature failed'
    except Exception as e:
        pass
    assert client.set_variables('show_hidden_columns', 1)
    ret = client.desc_table(table_name, is_all=True)
    hidden_column = util.get_attr_condition_list(ret, palo_job.DescInfoAll.Field, '__DORIS_DELETE_SIGN__')
    assert len(hidden_column) == 2, 'expect base table & mv have __DORIS_DELETE_SIGN__'
    hidden_column = util.get_attr_condition_list(ret, palo_job.DescInfoAll.Field, '__DORIS_VERSION_COL__')
    assert len(hidden_column) == 1, 'expect base table has __DORIS_VERSION_COL__'
    # 加列并验证
    v_add = [('k_add', 'INT', '', '0')]
    ret = client.schema_change_add_column(table_name, v_add, is_wait_job=True)
    assert ret
    ret = client.desc_table(table_name, is_all=True)
    hidden_column = util.get_attr_condition_list(ret, palo_job.DescInfoAll.Field, '__DORIS_DELETE_SIGN__')
    assert len(hidden_column) == 2, 'expect base table & mv have __DORIS_DELETE_SIGN__'
    hidden_column = util.get_attr_condition_list(ret, palo_job.DescInfoAll.Field, '__DORIS_VERSION_COL__')
    assert len(hidden_column) == 1, 'expect base table has __DORIS_VERSION_COL__'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select k1, k2, k3, k4, k5, k6, k10, k11, k7, k8, k9, 0, 0, 2 from %s.%s order by k1' \
           % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    # 导入并验证
    column_list = DATA.baseall_column_name_list
    load_data_desc = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, column_name_list=column_list,
                                              merge_type='MERGE', delete_on_predicates='k1 > 0')
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True)
    assert ret
    assert client.set_variables('show_hidden_columns', 0)
    ret = client.select_all(table_name)
    assert ret == ()
    client.clean(database_name)


def test_drop_column_delete_load():
    """
    {
    "title": "test_drop_column_delete_load",
    "describe": "带rollup的表，减列不影响delete load，删除隐藏列，预期失败",
    "tag": "p1,system,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    # creat tb, create rollup & load
    ret = client.create_table(table_name, DATA.baseall_column_no_agg_list,
                              distribution_info=DATA.baseall_distribution_info,
                              keys_desc=DATA.baseall_unique_key,
                              enable_unique_key_merge_on_write="false")
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'can not get table: %s' % table_name
    # load
    load_data_desc = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, merge_type='APPEND')
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True)
    assert ret
    sql1 = 'select * from %s.%s order by k1'
    common.check2(client, sql1=sql1 % (database_name, table_name), sql2=sql1 % (check_db, baseall_tb))
    # enable
    try:
        ret = client.enable_feature_batch_delete(table_name)
        assert ret, 'enable batch delete feature failed'
    except Exception as e:
        pass
    assert client.set_variables('show_hidden_columns', 1)
    ret = client.desc_table(table_name, is_all=True)
    assert util.get_attr_condition_value(ret, palo_job.DescInfoAll.Field, '__DORIS_DELETE_SIGN__')
    assert util.get_attr_condition_value(ret, palo_job.DescInfoAll.Field, '__DORIS_VERSION_COL__')
    # rollup
    rollup_list = ['k1', 'k2', 'k3', 'k4', 'k5', 'k7', 'k6', 'k10', 'k11']
    ret = client.create_rollup_table(table_name, index_name, rollup_list, is_wait=True)
    assert ret
    assert client.get_index(table_name, index_name)
    # drop column. Can not drop key column in Unique data model table
    ret = client.schema_change_drop_column(table_name, ['k9'], is_wait_job=True)
    assert ret
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select k1, k2, k3, k4, k5, k6, k10, k11, k7, k8, 0, 2 from %s.%s order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    # merge load
    load_data_desc = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, 
                                              column_name_list=DATA.baseall_column_name_list, 
                                              merge_type='MERGE', delete_on_predicates='k6 in ("false")')
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True)
    assert ret
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select k1, k2, k3, k4, k5, k6, k10, k11, k7, k8, k6 in ("false"), 3 ' \
           'from %s.%s order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_add_drop_partition_delete_load():
    """
    {
    "title": "test_delete_merge_duplicate_data",
    "describe": "加减分区不影响批量删除功能",
    "tag": "p1,system"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    partition_info = palo_client.PartitionInfo("k1",
                                               ["p1", "p2", "p3"],
                                               ["-10", "0", "10"])
    ret = client.create_table(table_name, DATA.baseall_column_no_agg_list,
                              distribution_info=DATA.baseall_distribution_info,
                              partition_info=partition_info,
                              keys_desc=DATA.baseall_unique_key)
    assert ret, 'create table failed'
    # merge, 一个空表，delete on 条件命中全部数据，todo set show_hidden_columns产看表的隐藏删除数据
    load_data_desc = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True, max_filter_ratio=0.5)
    assert ret
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.%s where k1 < 10 order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    try:
        ret = client.enable_feature_batch_delete(table_name)
        assert ret, 'enable batch delete feature failed'
    except Exception as e:
        pass
    assert client.add_partition(table_name, 'p4', 20), 'add partition failed'
    assert client.get_partition(table_name, 'p4')
    load_data_desc = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, merge_type='MERGE',
                                              delete_on_predicates='k1 < 10',
                                              partition_list=['p1', 'p2', 'p3', 'p4'])
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True)
    assert ret, 'broker load failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.%s where k1 >= 10 order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    ret = client.drop_partition(table_name, 'p3')
    load_data_desc = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, merge_type='delete')
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True, max_filter_ratio=0.8)
    assert ret, 'broker load failed'
    ret = client.select_all(table_name)
    assert ret == ()
    client.clean(database_name)


def test_delete_and_delete_load():
    """
    {
    "title": "test_delete_and_delete_load",
    "describe": "delete & truncate table，关注show_hiden_column模式下，数据的正确性",
    "tag": "p1,system"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    partition_info = palo_client.PartitionInfo("k1",
                                               ["p1", "p2", "p3", "p4"],
                                               ["-10", "0", "10", "20"])
    ret = client.create_table(table_name, DATA.baseall_column_no_agg_list,
                              distribution_info=DATA.baseall_distribution_info,
                              partition_info=partition_info,
                              keys_desc=DATA.baseall_unique_key)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.baseall_local_file, merge_type='MERGE', delete="k1 = 1")
    assert ret, 'stream load failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.%s where k1 != 1 order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    assert client.set_variables('show_hidden_columns', 1)
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select k1, k2, k3, k4, k5, k6, k10, k11, k7, k8, k9, k1=1, 2 from %s.%s order by k1' \
           % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    ret = client.delete(table_name, 'k1=1', 'p3')
    assert ret, 'delete failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select k1, k2, k3, k4, k5, k6, k10, k11, k7, k8, k9, k1=1, 2 from %s.%s ' \
           'where k1 != 1 order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    ret = client.delete(table_name, 'k1>1', 'p4')
    assert ret, 'delete failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select k1, k2, k3, k4, k5, k6, k10, k11, k7, k8, k9, k1=1, 2 from %s.%s ' \
           'where k1 != 1 and k1 < 10 order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    ret = client.stream_load(table_name, FILE.baseall_local_file, merge_type='delete')
    assert ret
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select k1, k2, k3, k4, k5, k6, k10, k11, k7, k8, k9, 1, 4 from %s.%s order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    assert client.truncate(table_name), 'truncate table failed'
    ret = client.select_all(table_name)
    assert ret == ()
    ret = client.stream_load(table_name, FILE.baseall_local_file, merge_type='MERGE', delete="k1 = 1")
    assert ret, 'stream load failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select *, k1=1, 2 from %s.%s order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_batch_delete_insert():
    """
    {
    "title": "test_batch_delete_insert",
    "describe": "开启删除导入，insert select & insert value数据成功，数据正确",
    "tag": "p1,system"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    partition_info = palo_client.PartitionInfo("k1",
                                               ["p1", "p2", "p3", "p4"],
                                               ["-10", "0", "10", "20"])
    ret = client.create_table(table_name, DATA.baseall_column_no_agg_list,
                              distribution_info=DATA.baseall_distribution_info,
                              partition_info=partition_info,
                              keys_desc=DATA.baseall_unique_key, set_null=True)
    assert ret, 'create table failed'
    try:
        ret = client.enable_feature_batch_delete(table_name)
        assert ret, 'enable batch delete feature failed'
    except Exception as e:
        pass
    assert client.set_variables('show_hidden_columns', 1)
    ret = client.insert_select(table_name, 'select * from %s.%s' % (check_db, baseall_tb))
    assert ret
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select k1, k2, k3, k4, k5, k6, k10, k11, k7, k8, k9, 0, 2 from %s.%s order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2)
    sql = 'insert into %s values(null, null, null, null, null, null, null, null, null, null, null)' % table_name
    ret = client.execute(sql)
    assert ret == (), 'insert values failed'
    ret = client.stream_load(table_name, FILE.baseall_local_file, merge_type='merge', delete='k1 is null')
    assert ret, 'stream load failed'
    sql2 = 'select null, null, null, null, null, null, null, null, null, null, null, 0, 2 ' \
           'union select k1, k2, k3, k4, k5, k6, k10, k11, k7, k8, k9, 0, 3 from %s.%s' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    ret = client.stream_load(table_name, FILE.baseall_local_file, merge_type='delete')
    assert ret, 'stream load failed'
    sql2 = 'select null, null, null, null, null, null, null, null, null, null, null, 0, 2 ' \
           'union select k1, k2, k3, k4, k5, k6, k10, k11, k7, k8, k9, 1, 4 from %s.%s' % (check_db, baseall_tb)
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    client.clean(database_name)
  

def test_batch_delete_some_times():
    """
    {
    "title": "test_batch_delete_some_times",
    "describe": "多次执行导入删除，验证最后结果正确",
    "tag": "p1,system,stability"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    partition_info = palo_client.PartitionInfo("k1",
                                               ["p1", "p2", "p3", "p4"],
                                               ["-10", "0", "10", "20"])
    ret = client.create_table(table_name, DATA.baseall_column_no_agg_list,
                              distribution_info=DATA.baseall_distribution_info,
                              partition_info=partition_info,
                              keys_desc=DATA.baseall_unique_key)
    assert ret, 'create table failed'
    load_data_desc = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), load_data_desc, broker=broker_info, is_wait=True, max_filter_ratio=0.5)
    assert ret
    sq11 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.%s order by k1' % (check_db, baseall_tb)
    common.check2(client, sql1=sq11, sql2=sql2)
    try:
        ret = client.enable_feature_batch_delete(table_name)
        assert ret, 'enable batch delete feature failed'
    except Exception as e:
        pass
    for i in range(20):
        ret = client.stream_load(table_name, FILE.baseall_local_file, merge_type='merge', delete="k1 > %s" % i)
        assert ret, 'stream load failed'
        time.sleep(3)
    common.check2(client, sql1=sq11, sql2=sql2)
    client.clean(database_name)


def test_delete_merge_limitation():
    """
    {
    "title": "test_delete_merge_limitation",
    "describe": "验证delete load的限制",
    "tag": "p1,function,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_no_agg_list,
                              distribution_info=DATA.baseall_distribution_info,
                              keys_desc=DATA.baseall_duplicate_key)
    assert ret, 'create table failed'
    
    ret = client.stream_load(table_name, FILE.baseall_local_file, merge_type='DELETE')
    assert not ret
    msg = 'Batch delete only supported in unique tables.'
    util.assert_return(False, msg, client.enable_feature_batch_delete, table_name)
    client.clean(database_name)


def test_delete_merge_duplicate_data():
    """
    {
    "title": "test_delete_merge_duplicate_data",
    "describe": "验证delete load的文件中，当key相同的时候以最后出现的key为准，delete on为value，验证结果正确",
    "tag": "p1,function,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.tinyint_column_no_agg_list, 
                              distribution_info=DATA.hash_distribution_info,
                              keys_desc='UNIQUE KEY(k1)')
    assert ret, 'create table failed'
    assert client.show_tables(table_name)
    try:
        ret = client.enable_feature_batch_delete(table_name)
        assert ret, 'enable batch delete feature failed'
    except Exception as e:
        pass
    assert client.set_variables('show_hidden_columns', 1)
    ret = client.desc_table(table_name, is_all=True)
    assert util.get_attr_condition_value(ret, palo_job.DescInfoAll.Field, '__DORIS_DELETE_SIGN__')
    ret = client.stream_load(table_name, FILE.test_tinyint_file, max_filter_ratio=0.1, 
                             merge_type='merge', delete='v1=1')
    assert ret, 'stream load failed'
    ret = client.stream_load(table_name, FILE.test_tinyint_file, max_filter_ratio=0.1,
                             merge_type='merge', delete='v1=3')
    assert ret, 'stream load failed'
    assert client.set_variables('show_hidden_columns', 0)
    ret = client.select_all(table_name)
    assert ret == ()
    client.clean(database_name)


if __name__ == '__main__':
    setup_module()


