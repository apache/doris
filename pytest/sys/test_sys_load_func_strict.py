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
#   @file test_sys_load_func_strict.py
#   @date 2019-06-13 10:32:26
#   @brief This file is a test file for palo data loading function in different strict mode.
#
#############################################################################

"""
导入函数的测试,考虑不同的导入方式
"""

import pytest
from data import schema as DATA
from lib import palo_config
from lib import palo_client
from lib import util

LOG = palo_client.LOG
L = palo_client.L
config = palo_config.config
broker_info = palo_config.broker_info


def setup_module():
    """
    setUp
    """
    global client
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)


def check_partition_list(table_name, partition_name_list):
    """
    验证分区是否创建成功
    """
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)


def test_alignment():
    """
    {
    "title": "test_sys_load_func_strict.test_alignment",
    "describe": "test set function alignment_timestamp()",
    "tag": "function,p1,fuzz"
    }
    """
    """
    test set function alignment_timestamp()
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    # create table
    table_s = table_name + '_s'
    table_ns = table_name + '_ns'
    table_not = table_name + '_not'
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = [100000, 1000000000, 10000000000, 'MAXVALUE']
    duplicate_key = 'DUPLICATE KEY(k1, k2, k3)'
    partition_info = palo_client.PartitionInfo('k1', partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13)
    client.create_table(table_s, DATA.timestamp_convert_column_list,
                        partition_info, distribution_info, set_null=True, keys_desc=duplicate_key)
    client.create_table(table_ns, DATA.timestamp_convert_column_list,
                        partition_info, distribution_info, set_null=True, keys_desc=duplicate_key)
    client.create_table(table_not, DATA.timestamp_convert_column_list,
                        partition_info, distribution_info, set_null=False, keys_desc=duplicate_key)
    assert client.show_tables(table_s)
    assert client.show_tables(table_ns)
    assert client.show_tables(table_not)
    check_partition_list(table_s, partition_name_list)
    check_partition_list(table_ns, partition_name_list)
    check_partition_list(table_not, partition_name_list)
    # broker load
    set_list = ['k1 = alignment_timestamp("day", tmp_k1)',
                'k2 = alignment_timestamp("month", tmp_k2)',
                'k3 = alignment_timestamp("year", tmp_k3)',
                'k4 = alignment_timestamp("hour", tmp_k4)']
    column_name_list = ['tmp_k1', 'tmp_k2', 'tmp_k3', 'tmp_k4']
    hdfs_file = palo_config.gen_remote_file_path('sys/verify/timestamp_load_file')
    data_desc_list = palo_client.LoadDataInfo(hdfs_file, table_s,
                                              partition_list=partition_name_list,
                                              column_name_list=column_name_list, column_terminator=',',
                                              set_list=set_list)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.05, strict_mode=True)
    data_desc_list.table_name = table_ns
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.05, strict_mode=False)
    data_desc_list.table_name = table_not
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.5, strict_mode=False)
    # check result
    check_file = './data/LOAD/expe_alignment.data'
    assert client.verify(check_file, table_s)
    assert client.verify(check_file, table_ns)
    sql1 = 'select * from %s' % table_not
    sql2 = 'select * from %s where k1 is not null and k2 is not null and k3 is not null and ' \
           'k4 is not null' % table_s
    ret1 = client.execute(sql1)
    ret2 = client.execute(sql2)
    util.check(ret1, ret2, True)
    client.clean(database_name)


def test_strftime():
    """
    {
    "title": "test_sys_load_func_strict.test_strftime",
    "describe": "test set funtion strftime()",
    "tag": "function,p1,fuzz"
    }
    """
    """
    test set funtion strftime()
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    # create table
    table_s = table_name + '_s'
    table_ns = table_name + '_ns'
    table_not = table_name + '_not'
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['1900-01-01 23:59:59', '2000-01-01 00:00:00',
                            '2099-12-21 23:59:59', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13)
    duplicate_key = 'DUPLICATE KEY(k1, k2, k3)'
    assert client.create_table(table_s, DATA.date_format_column_list,
                            partition_info, distribution_info, set_null=True, keys_desc=duplicate_key)
    assert client.create_table(table_ns, DATA.date_format_column_list,
                        partition_info, distribution_info, set_null=True, keys_desc=duplicate_key)
    assert client.create_table(table_not, DATA.date_format_column_list,
                        partition_info, distribution_info, set_null=False, keys_desc=duplicate_key)
    assert client.show_tables(table_s)
    assert client.show_tables(table_ns)
    assert client.show_tables(table_not)
    check_partition_list(table_s, partition_name_list)
    check_partition_list(table_ns, partition_name_list)
    check_partition_list(table_not, partition_name_list)

    set_list = ['k1 = strftime("%Y-%m-%d %H:%M:%S", tmp_k1)',
                'k2 = strftime("%Y-%m-%d %H:%M:%S", tmp_k2)',
                'k3 = strftime("%Y-%m-%d %H:%M:%S", tmp_k3)',
                'k4 = strftime("%Y-%m-%d %H:%M:%S", tmp_k4)']

    column_name_list = ['tmp_k1', 'tmp_k2', 'tmp_k3', 'tmp_k4']
    hdfs_file = palo_config.gen_remote_file_path('sys/verify/timestamp_load_file')
    data_desc_list = palo_client.LoadDataInfo(hdfs_file, table_s,
                                              partition_list=partition_name_list,
                                              column_name_list=column_name_list, column_terminator=',',
                                              set_list=set_list)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.05, strict_mode=True)
    data_desc_list.table_name = table_ns
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.05, strict_mode=False)
    data_desc_list.table_name = table_not
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.5, strict_mode=False)
    check_file = './data/LOAD/expe_strftime.data'
    assert client.verify(check_file, table_s)
    assert client.verify(check_file, table_ns)
    sql1 = 'select * from %s' % table_not
    sql2 = 'select * from %s where k1 is not null and k2 is not null and k3 is not null ' \
           'and k4 is not null' % table_s
    ret1 = client.execute(sql1)
    ret2 = client.execute(sql2)
    util.check(ret1, ret2, True)
    client.clean(database_name)


def test_time_format():
    """
    {
    "title": "test_sys_load_func_strict.test_time_format",
    "describe": "test set function time_format()",
    "tag": "function,p1,fuzz"
    }
    """
    """test set function time_format()"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    # create table
    table_s = table_name + '_s'
    table_ns = table_name + '_ns'
    table_not = table_name + '_not'
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['1900-01-01 23:59:59', '2000-01-01 00:00:00',
                            '2099-12-21 23:59:59', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13)
    duplicate_key = 'DUPLICATE KEY(k1, k2, k3)'
    assert client.create_table(table_s, DATA.date_format_column_list,
                               partition_info, distribution_info, set_null=True, keys_desc=duplicate_key)

    assert client.create_table(table_ns, DATA.date_format_column_list,
                               partition_info, distribution_info, set_null=True, keys_desc=duplicate_key)
    assert client.create_table(table_not, DATA.date_format_column_list,
                               partition_info, distribution_info, set_null=False, keys_desc=duplicate_key)
    assert client.show_tables(table_s)
    assert client.show_tables(table_ns)
    assert client.show_tables(table_not)
    check_partition_list(table_s, partition_name_list)
    check_partition_list(table_ns, partition_name_list)
    check_partition_list(table_not, partition_name_list)
    # broker load
    set_list = ['k1 = time_format("%Y-%m-%d %H:%i:%S", "%m/%d/%Y %H:%i:%S", tmp_k1)',
                'k2 = time_format("%Y-%m-%d %H:%i:%S", "%m/%d/%Y %H:%i:%S", tmp_k2)',
                'k3 = time_format("%Y-%m-%d %H:%i:%S", "%m/%d/%Y %H:%i:%S", tmp_k3)',
                'k4 = time_format("%Y-%m-%d %H:%i:%S", "%m/%d/%Y %H:%i:%S", tmp_k4)']

    column_name_list = ['tmp_k1', 'tmp_k2', 'tmp_k3', 'tmp_k4']
    hdfs_file = palo_config.gen_remote_file_path('sys/verify/time_format_file')
    data_desc_list = palo_client.LoadDataInfo(hdfs_file, table_s,
                                              partition_list=partition_name_list,
                                              column_name_list=column_name_list, column_terminator=',',
                                              set_list=set_list)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.5, strict_mode=True)
    data_desc_list.table_name = table_ns
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.5, strict_mode=False)
    data_desc_list.table_name = table_not
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.5, strict_mode=False)
    # check
    check_file = './data/LOAD/expe_time_format.data'
    client.verify(check_file, table_s)
    client.verify(check_file, table_ns)
    sql1 = 'select * from %s' % table_not
    sql2 = 'select * from %s where k1 is not null and k2 is not null and k3 is not null ' \
           'and k4 is not null' % table_s
    ret1 = client.execute(sql1)
    ret2 = client.execute(sql2)
    util.check(ret1, ret2, True)
    client.clean(database_name)


def test_default_value():
    """
    {
    "title": "test_sys_load_func_strict.test_default_value",
    "describe": "test set function defualt_value()",
    "tag": "function,p1,fuzz"
    }
    """
    """test set function defualt_value()"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    # create table
    table_s = table_name + '_s'
    table_ns = table_name + '_ns'
    table_not = table_name + '_not'
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = [100000, 1000000000, 10000000000, 'MAXVALUE']
    duplicate_key = 'DUPLICATE KEY(k1, k2, k3)'
    partition_info = palo_client.PartitionInfo('k1', partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13)
    client.create_table(table_s, DATA.timestamp_convert_column_list,
                        partition_info, distribution_info, set_null=True, keys_desc=duplicate_key)
    client.create_table(table_ns, DATA.timestamp_convert_column_list,
                        partition_info, distribution_info, set_null=True, keys_desc=duplicate_key)
    client.create_table(table_not, DATA.timestamp_convert_column_list,
                        partition_info, distribution_info, set_null=False, keys_desc=duplicate_key)
    assert client.show_tables(table_s)
    assert client.show_tables(table_ns)
    assert client.show_tables(table_not)
    check_partition_list(table_s, partition_name_list)
    check_partition_list(table_ns, partition_name_list)
    check_partition_list(table_not, partition_name_list)
    # broker load
    set_list = ['k4 = default_value("31")']
    column_name_list = ['k1', 'k2', 'k3', 'tmp_k4']
    hdfs_file = palo_config.gen_remote_file_path('sys/verify/timestamp_load_file')
    data_desc_list = palo_client.LoadDataInfo(hdfs_file, table_s,
                                              partition_list=partition_name_list,
                                              column_name_list=column_name_list, column_terminator=',',
                                              set_list=set_list)


    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.05, strict_mode=True)
    data_desc_list.table_name = table_ns
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.05, strict_mode=False)
    data_desc_list.table_name = table_not
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.5, strict_mode=False)
    # check result
    check_file = './data/LOAD/expe_default_value.data'
    assert client.verify(check_file, table_s)
    assert client.verify(check_file, table_ns)
    sql1 = 'select * from %s' % table_not
    sql2 = 'select * from %s where k1 is not null and k2 is not null and k3 is not null and k4 is not null' % table_s
    ret1 = client.execute(sql1)
    ret2 = client.execute(sql2)
    util.check(ret1, ret2, True)
    client.clean(database_name)


def test_md5sum():
    """
    {
    "title": "test_sys_load_func_strict.test_md5sum",
    "describe": "test set function md5sum()",
    "tag": "function,p1,fuzz"
    }
    """
    """test set function md5sum()"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    # create table
    table_s = table_name + '_s'
    table_ns = table_name + '_ns'
    table_not = table_name + '_not'
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = [100000, 1000000000, 10000000000, 'MAXVALUE']
    duplicate_key = 'DUPLICATE KEY(k1, k2, k3)'
    partition_info = palo_client.PartitionInfo('k1', partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13)
    client.create_table(table_s, DATA.md5_colmn_list,
                        partition_info, distribution_info, set_null=True, keys_desc=duplicate_key)
    client.create_table(table_ns, DATA.md5_colmn_list,
                        partition_info, distribution_info, set_null=True, keys_desc=duplicate_key)
    client.create_table(table_not, DATA.md5_colmn_list,
                        partition_info, distribution_info, set_null=False, keys_desc=duplicate_key)
    assert client.show_tables(table_s)
    assert client.show_tables(table_ns)
    assert client.show_tables(table_not)
    check_partition_list(table_s, partition_name_list)
    check_partition_list(table_ns, partition_name_list)
    check_partition_list(table_not, partition_name_list)
    # broker load
    set_list = ['k5 = md5sum(k1)']
    column_name_list = ['k1', 'k2', 'k3', 'k4']
    hdfs_file = palo_config.gen_remote_file_path('sys/verify/timestamp_load_file')
    data_desc_list = palo_client.LoadDataInfo(hdfs_file, table_s,
                                              partition_list=partition_name_list,
                                              column_name_list=column_name_list, column_terminator=',',
                                              set_list=set_list)

    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.05, strict_mode=True)
    data_desc_list.table_name = table_ns
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.05, strict_mode=False)
    data_desc_list.table_name = table_not
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.5, strict_mode=False)
    # check result
    check_file = './data/LOAD/expe_md5sum.data'
    assert client.verify(check_file, table_s)
    assert client.verify(check_file, table_ns)
    sql1 = 'select * from %s' % table_not
    sql2 = 'select * from %s where k1 is not null and k2 is not null and k3 is not null and k4 is not null' % table_s
    ret1 = client.execute(sql1)
    ret2 = client.execute(sql2)
    util.check(ret1, ret2, True)
    client.clean(database_name)


def test_replace_value():
    """
    {
    "title": "test_sys_load_func_strict.test_replace_value",
    "describe": "test set function replace_value()",
    "tag": "function,p1,fuzz"
    }
    """
    """test set function replace_value()"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    # create table
    table_s = table_name + '_s'
    table_ns = table_name + '_ns'
    table_not = table_name + '_not'
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = [100000, 1000000000, 10000000000, 'MAXVALUE']
    duplicate_key = 'DUPLICATE KEY(k1, k2, k3)'
    partition_info = palo_client.PartitionInfo('k1', partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13)
    client.create_table(table_s, DATA.timestamp_convert_column_list,
                        partition_info, distribution_info, set_null=True, keys_desc=duplicate_key)
    client.create_table(table_ns, DATA.timestamp_convert_column_list,
                        partition_info, distribution_info, set_null=True, keys_desc=duplicate_key)
    client.create_table(table_not, DATA.timestamp_convert_column_list,
                        partition_info, distribution_info, set_null=False, keys_desc=duplicate_key)
    assert client.show_tables(table_s)
    assert client.show_tables(table_ns)
    assert client.show_tables(table_not)
    check_partition_list(table_s, partition_name_list)
    check_partition_list(table_ns, partition_name_list)
    check_partition_list(table_not, partition_name_list)
    # broker load
    set_list = ['k1 = replace_value("0", "-101")']
    column_name_list = ['k1', 'k2', 'k3', 'k4']
    hdfs_file = palo_config.gen_remote_file_path('sys/verify/timestamp_load_file')
    data_desc_list = palo_client.LoadDataInfo(hdfs_file, table_s,
                                              partition_list=partition_name_list,
                                              column_name_list=column_name_list, column_terminator=',',
                                              set_list=set_list)

    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.05, strict_mode=True)
    data_desc_list.table_name = table_ns
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.05, strict_mode=False)
    data_desc_list.table_name = table_not
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.5, strict_mode=False)
    # check result
    check_file = './data/LOAD/expe_replace_value.data'
    assert client.verify(check_file, table_s)
    assert client.verify(check_file, table_ns)
    sql1 = 'select * from %s' % table_not
    sql2 = 'select * from %s where k1 is not null and k2 is not null and k3 is not null and k4 is not null' % table_s
    ret1 = client.execute(sql1)
    ret2 = client.execute(sql2)
    util.check(ret1, ret2, True)
    client.clean(database_name)


def test_now():
    """
    {
    "title": "test_sys_load_func_strict.test_now",
    "describe": "test set function now()",
    "tag": "function,p1,fuzz"
    }
    """
    """test set function now()"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    # create table
    table_s = table_name + '_s'
    table_ns = table_name + '_ns'
    table_not = table_name + '_not'
    distribution_info = palo_client.DistributionInfo('RANDOM', 13)
    client.create_table(table_s, DATA.timestamp2timestamp_column_list,
                        distribution_info=distribution_info, set_null=True)
    client.create_table(table_ns, DATA.timestamp2timestamp_column_list,
                        distribution_info=distribution_info, set_null=True)
    client.create_table(table_not, DATA.timestamp2timestamp_column_list,
                        distribution_info=distribution_info, set_null=False)
    assert client.show_tables(table_s)
    assert client.show_tables(table_ns)
    assert client.show_tables(table_not)
    # broker load
    set_list = ['k1 = now()']
    column_name_list = ['tmp_k1', 'k2', 'k3', 'k4']
    hdfs_file = palo_config.gen_remote_file_path('sys/verify/timestamp_load_file')
    data_desc_list = palo_client.LoadDataInfo(hdfs_file, table_s,
                                              column_name_list=column_name_list, column_terminator=',',
                                              set_list=set_list)

    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.05, strict_mode=True)
    data_desc_list.table_name = table_ns
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.05, strict_mode=False)
    data_desc_list.table_name = table_not
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.5, strict_mode=False)
    # check result
    sql1 = 'select * from %s'
    ret1 = client.execute(sql1 % table_s)
    ret2 = client.execute(sql1 % table_ns)
    ret3 = client.execute(sql1 % table_not)
    assert len(ret1) == 1
    assert len(ret2) == 1
    assert len(ret3) == 1
    client.clean(database_name)


def teardown_module():
    """
    tearDown
    """
    pass


if __name__ == '__main__':
    setup_module()
    test_time_format()

