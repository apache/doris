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
#   @file test_sys_pull_load.py
#   @date 2015/05/13 15:26:21
#   @brief This file is a test file for palo data loading and verifying.
#
#############################################################################

"""
复合分区，导入操作测试
"""

import pytest
from data import pull_load_apache as DATA
from data import special as SPECIAL_DATA
from data import partition as PARTITION_DATA
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


def test_all_types():
    """
    {
    "title": "test_sys_pull_load_hdfs.test_all_types",
    "describe": "所有所有类型的随机生成数据的正确性测试,largeint类型测试,largeint是一种比较特殊的类型,4种聚合方式测试,所有支持类型的sum、replace、max、min聚合方式测试,key、value测试,支持的类型做key列和所有类型做value列的情况测试",
    "tag": "function,p1"
    }
    """
    """
    所有所有类型的随机生成数据的正确性测试
    largeint类型测试：largeint是一种比较特殊的类型
    4种聚合方式测试：所有支持类型的sum、replace、max、min聚合方式测试
    key、value测试：支持的类型做key列和所有类型做value列的情况测试
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.use(database_name)
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('tinyint_key',
                                               partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(largeint_key, decimal_key)', 13)
    client.create_table(table_name, DATA.schema_1, 
                        partition_info, distribution_info)
    assert client.show_tables(table_name)
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)

    data_desc_list_1 = palo_client.LoadDataInfo(DATA.data_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list_1, is_wait=True, broker=broker_info)
    assert client.verify(DATA.verify_1, table_name)
    client.clean(database_name)


def test_all_noexsit():
    """
    {
    "title": "test_sys_pull_load_hdfs.test_all_noexsit",
    "describe": "测试当指定不存在的文件时，导入失败",
    "tag": "function,p1,fuzz"
    }
    """
    """
    测试当指定不存在的文件时，导入失败
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.use(database_name)
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('tinyint_key',
                                               partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(largeint_key, decimal_key)', 13)
    client.create_table(table_name, DATA.schema_1, 
                        partition_info, distribution_info)
    assert client.show_tables(table_name)
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)

    data_desc_list_1 = palo_client.LoadDataInfo(DATA.data_noexsit, table_name)
    assert not client.batch_load(util.get_label(), data_desc_list_1,
                                 is_wait=True, broker=broker_info)
    client.clean(database_name)


def test_all_types_gz():
    """
    {
    "title": "test_sys_pull_load_hdfs.test_all_types_gz",
    "describe": "所有类型的随机生成数据的正确性测试，导入文件格式为gz",
    "tag": "function,p1"
    }
    """
    """
    所有类型的随机生成数据的正确性测试，导入文件格式为gz
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.use(database_name)
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('tinyint_key',
                                               partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(largeint_key, decimal_key)', 13)
    client.create_table(table_name, DATA.schema_1, 
                        partition_info, distribution_info)
    assert client.show_tables(table_name)
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)

    data_desc_list_1 = palo_client.LoadDataInfo(DATA.data_1_gz, table_name)
    assert client.batch_load(util.get_label(), data_desc_list_1, is_wait=True, broker=broker_info)
    assert client.verify(DATA.verify_1, table_name)
    client.clean(database_name)


def test_all_types_bz2():
    """
    {
    "title": "test_sys_pull_load_hdfs.test_all_types_bz2",
    "describe": "所有类型的随机生成数据的正确性测试，导入文件格式为bz2",
    "tag": "function,p1"
    }
    """
    """
    所有类型的随机生成数据的正确性测试，导入文件格式为bz2
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.use(database_name)
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('tinyint_key',
                                               partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(largeint_key, decimal_key)', 13)
    client.create_table(table_name, DATA.schema_1, 
                        partition_info, distribution_info)
    assert client.show_tables(table_name)
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)

    data_desc_list_1 = palo_client.LoadDataInfo(DATA.data_1_bz2, table_name)
    assert client.batch_load(util.get_label(), data_desc_list_1, is_wait=True, broker=broker_info)
    assert client.verify(DATA.verify_1, table_name)
    client.clean(database_name)


def test_all_types_lzo():
    """
    {
    "title": "test_sys_pull_load_hdfs.test_all_types_lzo",
    "describe": "所有类型的随机生成数据的正确性测试，导入文件格式为lzo",
    "tag": "function,p1"
    }
    """
    """
    所有类型的随机生成数据的正确性测试，导入文件格式为lzo
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.use(database_name)
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('tinyint_key',
                                               partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(largeint_key, decimal_key)', 13)
    client.create_table(table_name, DATA.schema_1, 
                        partition_info, distribution_info)
    assert client.show_tables(table_name)
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)

    data_desc_list_1 = palo_client.LoadDataInfo(DATA.data_1_lzo, table_name)
    assert client.batch_load(util.get_label(), data_desc_list_1, is_wait=True, broker=broker_info)
    assert client.verify(DATA.verify_1, table_name)
    client.clean(database_name)


def test_all_types_lz4():
    """
    {
    "title": "test_sys_pull_load_hdfs.test_all_types_lz4",
    "describe": "所有类型的随机生成数据的正确性测试，导入文件格式为lz4",
    "tag": "function,p1"
    }
    """
    """
    所有类型的随机生成数据的正确性测试，导入文件格式为lz4
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.use(database_name)
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('tinyint_key',
                                               partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(largeint_key, decimal_key)', 13)
    client.create_table(table_name, DATA.schema_1, 
                        partition_info, distribution_info)
    assert client.show_tables(table_name)
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)

    data_desc_list_1 = palo_client.LoadDataInfo(DATA.data_1_lz4, table_name)
    assert client.batch_load(util.get_label(), data_desc_list_1, is_wait=True, broker=broker_info)
    assert client.verify(DATA.verify_1, table_name)
    client.clean(database_name)


def test_all_types_parquet():
    """
    {
    "title": "test_sys_pull_load_hdfs.test_all_types_parquet",
    "describe": "test parquet file load",
    "tag": "function,p1"
    }
    """
    """
    test parquet file loading
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.use(database_name)
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('tinyint_key',
                                               partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(largeint_key, decimal_key)', 13)
    client.create_table(table_name, DATA.schema_1,
                        partition_info, distribution_info, set_null=True)
    assert client.show_tables(table_name)
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)

    data_desc_list_1 = palo_client.LoadDataInfo(DATA.data_1_parquet, table_name, format_as='parquet')
    assert client.batch_load(util.get_label(), data_desc_list_1, is_wait=True, broker=broker_info)
    assert client.verify(DATA.verify_6, table_name)
    client.clean(database_name)


def test_all_types_parquet_multi():
    """
    {
    "title": "test_sys_pull_load_hdfs.test_all_types_parquet_multi",
    "describe": "test parquet multi load",
    "tag": "function,p1"
    }
    """
    """test parquet multi load"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.use(database_name)
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('tinyint_key',
                                               partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(largeint_key, decimal_key)', 13)
    client.create_table(table_name, DATA.schema_1,
                        partition_info, distribution_info, set_null=True)
    assert client.show_tables(table_name)
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)

    data_desc_list_1 = palo_client.LoadDataInfo(DATA.data_1_parquet, table_name, format_as='parquet')
    assert client.batch_load(util.get_label(), data_desc_list_1, is_wait=True, broker=broker_info)
    assert client.batch_load(util.get_label(), data_desc_list_1, is_wait=True, broker=broker_info)
    assert client.verify(DATA.verify_6 * 2, table_name)
    client.clean(database_name)


def test_all_types_parquet_format():
    """
    {
    "title": "test_sys_pull_load_hdfs.test_all_types_parquet_format",
    "describe": "test parquet file loading",
    "tag": "function,p1,fuzz"
    }
    """
    """
    test parquet file loading
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.use(database_name)
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('tinyint_key',
                                               partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(largeint_key, decimal_key)', 13)
    client.create_table(table_name, DATA.schema_1,
                        partition_info, distribution_info, set_null=True)
    assert client.show_tables(table_name)
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)
    # error format
    data_desc_list_1 = palo_client.LoadDataInfo(DATA.data_1_parquet, table_name, format_as='parqut')
    try:
        client.batch_load(util.get_label(), data_desc_list_1, is_wait=True, broker=broker_info)
        assert 0 == 1
    except Exception as e:
        print(str(e))
    # default .parquet
    data_desc_list_1 = palo_client.LoadDataInfo(DATA.data_1_parquet, table_name)
    assert client.batch_load(util.get_label(), data_desc_list_1, is_wait=True, broker=broker_info)
    assert client.verify(DATA.verify_6, table_name)
    client.clean(database_name)


def test_all_types_parquet_null():
    """
    {
    "title": "test_sys_pull_load_hdfs.test_all_types_parquet_null",
    "describe": "test parquet file load",
    "tag": "function,p1"
    }
    """
    """
    test parquet file loading
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.use(database_name)
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('tinyint_key',
                                               partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(largeint_key, decimal_key)', 13)
    client.create_table(table_name, DATA.schema_1,
                        partition_info, distribution_info, set_null=True)
    assert client.show_tables(table_name)
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)

    data_desc_list_1 = palo_client.LoadDataInfo(DATA.data_parquet_null, table_name, format_as='parquet')
    assert client.batch_load(util.get_label(), data_desc_list_1, is_wait=True, broker=broker_info)
    assert client.verify(DATA.verify_7, table_name)
    client.clean(database_name)


def test_all_types_orc():
    """
    {
    "title": "test_all_types_orc",
    "describe": "test orc file load",
    "tag": "function,p1"
    }
    """
    """
    test orc file loading
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.use(database_name)
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('tinyint_key',
                                               partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(largeint_key, decimal_key)', 13)
    client.create_table(table_name, DATA.schema_1,
                        partition_info, distribution_info, set_null=True)
    assert client.show_tables(table_name)
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)

    data_desc_list_1 = palo_client.LoadDataInfo(DATA.data_1_orc, table_name, format_as='orc')
    assert client.batch_load(util.get_label(), data_desc_list_1, is_wait=True, broker=broker_info)
    assert client.verify(DATA.verify_6, table_name)
    client.clean(database_name)


def test_all_types_orc_null():
    """
    {
    "title": "test_sys_pull_load_hdfs.test_all_types_orc_null",
    "describe": "test orc file load null",
    "tag": "function,p1"
    }
    """
    """
    test orc file load null
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.use(database_name)
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('tinyint_key',
                                               partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(largeint_key, decimal_key)', 13)
    client.create_table(table_name, DATA.schema_1,
                        partition_info, distribution_info, set_null=True)
    assert client.show_tables(table_name)
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)

    data_desc_list_1 = palo_client.LoadDataInfo(DATA.data_orc_null, table_name, format_as='orc')
    assert client.batch_load(util.get_label(), data_desc_list_1, is_wait=True, broker=broker_info)
    assert client.verify(DATA.verify_7, table_name)
    client.clean(database_name)


def test_all_column_list_duo():
    """
    {
    "title": "test_sys_pull_load_hdfs.test_all_column_list_duo",
    "describe": "所有类型的随机生成数据的正确性测试，导入指定列名",
    "tag": "function,p1"
    }
    """
    """
    所有类型的随机生成数据的正确性测试，导入指定列名
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.use(database_name)
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('tinyint_key',
                                               partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(largeint_key, decimal_key)', 13)
    client.create_table(table_name, DATA.schema_1, 
                        partition_info, distribution_info)
    assert client.show_tables(table_name)
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)

    column_name_list = ['tinyint_key', 'smallint_key', 'int_key',
                        'bigint_key', 'largeint_key', 'char_key', 'varchar_key',
                        'decimal_key', 'date_key', 'datetime_key', 'tinyint_value_max',
                        'smallint_value_min', 'int_value_sum', 'bigint_value_sum',
                        'largeint_value_sum', 'largeint_value_replace', 'char_value_replace',
                        'varchar_value_replace', 'duo', 'decimal_value_replace',
                        'date_value_replace', 'datetime_value_replace',
                        'float_value_sum', 'double_value_sum']

    data_desc_list_1 = palo_client.LoadDataInfo(DATA.data_3, table_name,
                                                column_name_list=column_name_list)
    assert client.batch_load(util.get_label(), data_desc_list_1, is_wait=True, broker=broker_info)
    assert client.verify(DATA.verify_3, table_name)
    client.clean(database_name)


def test_more_files():
    """
    {
    "title": "test_sys_pull_load_hdfs.test_more_files",
    "describe": "多数据文件导入一张表测试,多数据文件导入多张表测试,分隔符测试：测试分隔符为x01的情况,column list与数据文件比较少列、表设置默认值的情况测试",
    "tag": "function,p1"
    }
    """
    """
    多数据文件导入一张表测试
    多数据文件导入多张表测试
    分隔符测试：测试分隔符为\x01的情况
    column list与数据文件比较少列、表设置默认值的情况测试
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    table_name_a = table_name + '_a'
    table_name_b = table_name + '_b'
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.use(database_name)
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('tinyint_key',
                                               partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(largeint_key, decimal_key)', 13)
    client.create_table(table_name_a, DATA.schema_2, 
                        partition_info, distribution_info)
    assert client.show_tables(table_name_a)
    for partition_name in partition_name_list:
        assert client.get_partition(table_name_a, partition_name)
    client.create_table(table_name_b, DATA.schema_2, 
                        partition_info, distribution_info)
    assert client.show_tables(table_name_b)

    column_name_list = ['tinyint_key', 'smallint_key', 'int_key',
                        'bigint_key', 'largeint_key', 'char_key', 'varchar_key',
                        'decimal_key', 'date_key', 'datetime_key', 'tinyint_value_max',
                        'smallint_value_min', 'int_value_sum', 'largeint_value_sum',
                        'float_value_sum', 'double_value_sum']

    data_desc_list_1 = palo_client.LoadDataInfo([DATA.data_4, DATA.data_5],
                                                table_name_a, column_name_list=column_name_list)
    data_desc_list_2 = palo_client.LoadDataInfo([DATA.data_6, DATA.data_7],
                                                table_name_b, column_name_list=column_name_list, 
                                                column_terminator='\x01')
    assert client.batch_load(util.get_label(), [data_desc_list_1,
                                                data_desc_list_2], is_wait=True, broker=broker_info)
    assert client.verify(DATA.verify_4, table_name_a)
    vf_a = palo_client.VerifyFile(DATA.verify_5[0], '\x01')
    vf_b = palo_client.VerifyFile(DATA.verify_5[1], '\x01')
    assert client.verify([vf_a, vf_b], table_name_b)
    client.clean(database_name)


def test_automic():
    """
    {
    "title": "test_sys_pull_load_hdfs.test_automic",
    "describe": "原子导入",
    "tag": "function,p1,fuzz"
    }
    """
    """
    原子导入
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    table_name_a = table_name + '_a'
    table_name_b = table_name + '_b'
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.use(database_name)
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('tinyint_key',
                                               partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(largeint_key, decimal_key)', 13)
    client.create_table(table_name_a, DATA.schema_2, 
                        partition_info, distribution_info)
    assert client.show_tables(table_name_a)
    for partition_name in partition_name_list:
        assert client.get_partition(table_name_a, partition_name)
    client.create_table(table_name_b, DATA.schema_2, 
                        partition_info, distribution_info)
    assert client.show_tables(table_name_b)

    column_name_list = ['tinyint_key', 'smallint_key', 'int_key',
                        'bigint_key', 'largeint_key', 'char_key', 'varchar_key',
                        'decimal_key', 'date_key', 'datetime_key', 'tinyint_value_max',
                        'smallint_value_min', 'int_value_sum', 'largeint_value_sum',
                        'float_value_sum', 'double_value_sum']

    data_desc_list_1 = palo_client.LoadDataInfo([DATA.data_4, DATA.data_5],
                                                table_name_a, column_name_list=column_name_list)
    data_desc_list_2 = palo_client.LoadDataInfo([DATA.data_6, DATA.data_1],
                                                table_name_b, column_name_list=column_name_list, 
                                                column_terminator='\x01')
    assert not client.batch_load(util.get_label(), [data_desc_list_1,
                                                    data_desc_list_2], is_wait=True, 
                                                    broker=broker_info)
    client.clean(database_name)


def test_dir():
    """
    {
    "title": "test_sys_pull_load_hdfs.test_dir",
    "describe": "测试导入指定目录，与通配符一起使用,多数据文件导入一张表测试,多数据文件导入多张表测试,分隔符测试：测试分隔符为x01的情况,column list与数据文件比较少列、表设置默认值的情况测试",
    "tag": "function,p1"
    }
    """
    """
    测试导入指定目录，与通配符一起使用
    多数据文件导入一张表测试
    多数据文件导入多张表测试
    分隔符测试：测试分隔符为\x01的情况
    column list与数据文件比较少列、表设置默认值的情况测试
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    table_name_a = table_name + '_a'
    table_name_b = table_name + '_b'
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.use(database_name)
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('tinyint_key',
                                               partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(largeint_key, decimal_key)', 13)
    client.create_table(table_name_a, DATA.schema_2, 
                        partition_info, distribution_info)
    assert client.show_tables(table_name_a)
    for partition_name in partition_name_list:
        assert client.get_partition(table_name_a, partition_name)
    client.create_table(table_name_b, DATA.schema_2, 
                        partition_info, distribution_info)
    assert client.show_tables(table_name_b)

    column_name_list = ['tinyint_key', 'smallint_key', 'int_key',
                        'bigint_key', 'largeint_key', 'char_key', 'varchar_key',
                        'decimal_key', 'date_key', 'datetime_key', 'tinyint_value_max',
                        'smallint_value_min', 'int_value_sum', 'largeint_value_sum',
                        'float_value_sum', 'double_value_sum']

    data_desc_list_1 = palo_client.LoadDataInfo(
        DATA.data_8, table_name_a, column_name_list=column_name_list)
    data_desc_list_2 = palo_client.LoadDataInfo(
        DATA.data_9, table_name_b, column_name_list=column_name_list, column_terminator='\x01')
    assert client.batch_load(
        util.get_label(), [data_desc_list_1, data_desc_list_2], is_wait=True, broker=broker_info)
#   assert client.batch_load(util.get_label(), [data_desc_list_1], is_wait=True, broker=broker_info)
    assert client.verify(DATA.verify_4, table_name_a)
    vf_a = palo_client.VerifyFile(DATA.verify_5[0], '\x01')
    vf_b = palo_client.VerifyFile(DATA.verify_5[1], '\x01')
    assert client.verify([vf_a, vf_b], table_name_b)
    client.clean(database_name)


# 接下来的几个case是：特殊值、边界值测试：各种int类型的最大最小值、varchar的空串、最大长度串的测试
def test_special_int_valid_hash_column():
    """
    {
    "title": "test_sys_pull_load_hdfs.test_special_int_valid_hash_column",
    "describe": "功能点：int类型特殊值, hash分区, column存储",
    "tag": "function,p1,fuzz"
    }
    """
    """
    功能点：int类型特殊值, hash分区, column存储
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.create_table(table_name, SPECIAL_DATA.schema_1,
                        keys_desc='AGGREGATE KEY (tinyint_key, smallint_key)')

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(SPECIAL_DATA.file_path_1, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    ret = client.verify(SPECIAL_DATA.expected_data_file_list_1, table_name)
    assert ret
    client.clean(database_name)


def test_special_decimal_valid_hash_column():
    """
    {
    "title": "test_sys_pull_load_hdfs.test_special_decimal_valid_hash_column",
    "describe": "功能点：decimal类型特殊值, hash分区, column存储",
    "tag": "function,p1,fuzz"
    }
    """
    """
    功能点：decimal类型特殊值, hash分区, column存储
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.create_table(table_name, SPECIAL_DATA.schema_2, keys_desc='AGGREGATE KEY (tinyint_key)')

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(SPECIAL_DATA.file_path_2, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    ret = client.verify(SPECIAL_DATA.expected_data_file_list_2, table_name)
    assert ret
    client.clean(database_name)


def test_special_max_decimal_valid_hash_column():
    """
    {
    "title": "test_sys_pull_load_hdfs.test_special_max_decimal_valid_hash_column",
    "describe": "功能点：decimal类型特殊值, 最大值, hash分区, column存储",
    "tag": "function,p1,fuzz"
    }
    """
    """
    功能点：decimal类型特殊值, 最大值, hash分区, column存储
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.create_table(table_name, SPECIAL_DATA.schema_3, keys_desc='AGGREGATE KEY (tinyint_key)')

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(SPECIAL_DATA.file_path_3, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    ret = client.verify(SPECIAL_DATA.expected_data_file_list_3, table_name)
    assert ret
    client.clean(database_name)


def test_special_char_valid_hash_column():
    """
    {
    "title": "test_sys_pull_load_hdfs.test_special_char_valid_hash_column",
    "describe": "功能点：char类型特殊值, hash分区, column存储",
    "tag": "function,p1,fuzz"
    }
    """
    """
    功能点：char类型特殊值, hash分区, column存储
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.create_table(table_name, SPECIAL_DATA.schema_4, keys_desc='AGGREGATE KEY (tinyint_key)')

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(SPECIAL_DATA.file_path_4, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    ret = client.verify(SPECIAL_DATA.expected_data_file_list_4, table_name)
    assert ret
    client.clean(database_name)


def test_special_char_invalid_hash_column():
    """
    {
    "title": "test_sys_pull_load_hdfs.test_special_char_invalid_hash_column",
    "describe": "功能点：char类型特殊值, invalid, hash分区, column存储",
    "tag": "function,p1,fuzz"
    }
    """
    """
    功能点：char类型特殊值, invalid, hash分区, column存储
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.use(database_name)
    client.clean(database_name)
    client.create_database(database_name)
    client.create_table(table_name, SPECIAL_DATA.schema_5, keys_desc='AGGREGATE KEY (tinyint_key)')

    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(SPECIAL_DATA.file_path_5, table_name)
    assert not client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)

    assert not client.verify(SPECIAL_DATA.expected_data_file_list_5, table_name)

    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True,
                             max_filter_ratio=0.5, broker=broker_info)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True,
                             max_filter_ratio=0.51, broker=broker_info)
    assert not client.batch_load(util.get_label(), data_desc_list,
                                 is_wait=True, max_filter_ratio=0.49, broker=broker_info)
    client.clean(database_name)


def test_load_with_partition():
    """
    {
    "title": "test_sys_pull_load_hdfs.test_load_with_partition",
    "describe": "指定partition：多分区表，测试指定partition进行导入",
    "tag": "function,p1"
    }
    """
    """
    指定partition：多分区表，测试指定partition进行导入
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '31', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('k1', partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(k1, k5)', 13)
    client.create_table(table_name, PARTITION_DATA.schema_1, keys_desc='AGGREGATE KEY (k1, k2, k3, k4, k5)',
                        partition_info=partition_info, distribution_info=distribution_info)
    assert client.show_tables(table_name)
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)

    data_desc_list = palo_client.LoadDataInfo(PARTITION_DATA.file_path_1,
                     table_name, ['partition_a', 'partition_b', 'partition_c'])
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(PARTITION_DATA.expected_data_file_list_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(list(PARTITION_DATA.expected_data_file_list_1) * 2, table_name)
    client.clean(database_name)


def test_partition_timeout_and_cancel():
    """
    {
    "title": "test_sys_pull_load_hdfs.test_partition_timeout_and_cancel",
    "describe": "test timeout and cancel",
    "tag": "function,p1,fuzz"
    }
    """
    """
    多分区表，大数据量
    并行导入
    数据量大于1G，并行ETL
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.use(database_name)
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b',
                           'partition_c', 'partition_d', 'partition_e']
    partition_value_list = ['1000000', '2000000000', '3000000000', '500000000000000', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('largeint_key',
                                               partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(largeint_key, decimal_key)', 13)
    client.create_table(table_name, DATA.schema_1, 
                        partition_info, distribution_info)
    assert client.show_tables(table_name)
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)

    data_desc_list_1 = palo_client.LoadDataInfo(
        DATA.data_2, table_name, ['partition_a', 'partition_b', 'partition_c'])
    data_desc_list_2 = palo_client.LoadDataInfo(
        DATA.data_2, table_name, ['partition_d', 'partition_e'])
    load_label_1 = util.get_label()
    client.batch_load(load_label_1, data_desc_list_1,
                      broker=broker_info, max_filter_ratio=1, timeout=5)
    load_label_2 = util.get_label()
    client.batch_load(load_label_2, data_desc_list_2, broker=broker_info, max_filter_ratio=1)
    client.cancel_load(load_label_2, database_name)
    assert not client.wait_load_job(load_label_1, database_name, cluster_name='test_cluster')
    assert not client.wait_load_job(load_label_2, database_name, cluster_name='test_cluster')
    client.clean(database_name)


def test_hunhe():
    """
    {
    "title": "test_sys_pull_load_hdfs.test_hunhe",
    "describe": "d多数据文件导入一张表测试,多数据文件导入多张表测试,column list与数据文件比较少列、表设置默认值的情况测试",
    "tag": "function,p1"
    }
    """
    """
    多数据文件导入一张表测试
    多数据文件导入多张表测试
    分隔符测试：测试分隔符为\x01的情况
    column list与数据文件比较少列、表设置默认值的情况测试
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    table_name_a = table_name + '_a'
    table_name_b = table_name + '_b'
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.use(database_name)
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('tinyint_key',
                                               partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(largeint_key, decimal_key)', 13)
    client.create_table(table_name_a, DATA.schema_2, 
                        partition_info, distribution_info)
    assert client.show_tables(table_name_a)
    for partition_name in partition_name_list:
        assert client.get_partition(table_name_a, partition_name)
    client.create_table(table_name_b, DATA.schema_2, 
                        partition_info, distribution_info)
    assert client.show_tables(table_name_b)

    column_name_list = ['tinyint_key', 'smallint_key', 'int_key', \
            'bigint_key', 'largeint_key', 'char_key', 'varchar_key', \
            'decimal_key', 'date_key', 'datetime_key', 'tinyint_value_max', \
            'smallint_value_min', 'int_value_sum', 'largeint_value_sum', \
            'float_value_sum', 'double_value_sum']

    data_desc_list_1 = palo_client.LoadDataInfo(
        [DATA.data_4, DATA.data_5_gz], table_name_a, column_name_list=column_name_list)
    data_desc_list_2 = palo_client.LoadDataInfo(
        [DATA.data_6_lzo, DATA.data_7_bz2], table_name_b, 
        column_name_list=column_name_list, column_terminator='\x01')
    assert client.batch_load(
        util.get_label(), [data_desc_list_1, data_desc_list_2], is_wait=True, broker=broker_info)
    assert client.verify(DATA.verify_4, table_name_a)
    vf_a = palo_client.VerifyFile(DATA.verify_5[0], '\x01')
    vf_b = palo_client.VerifyFile(DATA.verify_5[1], '\x01')
    assert client.verify([vf_a, vf_b], table_name_b)
    client.clean(database_name)


def teardown_module():
    """
    tearDown
    """
    pass


if __name__ == '__main__':
    setup_module()
    # test_all_types_orc()
    test_all_types_orc_null()
    test_all_types_parquet_null()
