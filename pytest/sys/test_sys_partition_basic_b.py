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
#   @file test_sys_partition_basic.py
#   @date 2015/05/13 15:26:21
#   @brief This file is a test file for palo data loading and verifying.
#   
#############################################################################

"""
按照所有支持分区的数据类型进行分区建表，对查询结果进行正确性校验
"""

from data import partition as DATA
from lib import palo_config
from lib import palo_client
from lib import util
import random

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


def check_load_and_verify(table_name, partition_name_list):
    """
    验证表是否创建成功，分区是否创建成功，导入数据，校验
    """
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(list(DATA.expected_data_file_list_1) * 2, table_name)


def partition_check(table_name, column_name, partition_name_list, \
        partition_value_list, distribution_type, bucket_num, storage_type):
    """
    检查，验证
    """
    partition_info = palo_client.PartitionInfo(column_name, \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo(distribution_type, bucket_num) 
    client.create_table(table_name, DATA.schema_1, \
            partition_info, distribution_info)
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)
    check_load_and_verify(table_name, partition_name_list)


def test_partition_by_datetime_1():
    """
    {
    "title": "test_sys_partition_basic_b.test_partition_by_datetime_1",
    "describe": "datetime分区",
    "tag": "system,p1"
    }
    """
    """
    datetime分区
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['2017-01-01 00:00:00', '2027-01-01 00:00:00', \
            '2039-01-01 00:00:00', 'MAXVALUE']

    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, 'k5', partition_name_list, \
            partition_value_list, 'RANDOM', random_bucket_num, 'row')
    client.clean(database_name)


def test_partition_by_datetime_2():
    """
    {
    "title": "test_sys_partition_basic_b.test_partition_by_datetime_2",
    "describe": "datetime分区，两个相同的分区值",
    "tag": "system,p1,fuzz"
    }
    """
    """
    datetime分区，两个相同的分区值
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['2017-01-01 00:00:00', '2017-01-01 00:00:00', \
            '2039-01-01 00:00:00', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('k5', partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13) 

    ret = False
    try:
        ret = client.create_table(table_name, DATA.schema_1, \
                partition_info, distribution_info)
    except:
        pass

    assert not ret
    client.clean(database_name)


def test_partition_by_datetime_3():
    """
    {
    "title": "test_sys_partition_basic_b.test_partition_by_datetime_3",
    "describe": "datetime分区，边界值",
    "tag": "system,p1,fuzz"
    }
    """
    """
    datetime分区，边界值
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['1900-01-01 00:00:00', '2027-01-01 23:59:59', \
            '2039-01-01 00:00:00', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('k5', partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13) 

    ret = False
    try:
        ret = client.create_table(table_name, DATA.schema_1, \
                partition_info, distribution_info)
    except:
        pass

    assert ret
    client.clean(database_name)


def test_partition_by_datetime_4():
    """
    {
    "title": "test_sys_partition_basic_b.test_partition_by_datetime_4",
    "describe": "datetime分区，越界",
    "tag": "system,p1,fuzz"
    }
    """
    """
    datetime分区，越界
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['1899-12-31 23:59:59', '2027-01-01 00:00:00', \
            '2039-01-01 00:00:00', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('k5', partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13) 

    ret = False
    try:
        ret = client.create_table(table_name, DATA.schema_1, \
                partition_info, distribution_info)
    except:
        pass
    assert ret
    client.clean(database_name)


def test_partition_by_datetime_5():
    """
    {
    "title": "test_sys_partition_basic_b.test_partition_by_datetime_5",
    "describe": "datetime分区，HASH(分区列)",
    "tag": "system,p1"
    }
    """
    """
    datetime分区，HASH(分区列)
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['1999-12-31 23:59:59', '2027-01-01 00:00:00', \
            '2039-01-01 00:00:00', 'MAXVALUE']

    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, 'k5', partition_name_list, \
            partition_value_list, 'HASH(k5)', random_bucket_num, 'column')
    client.clean(database_name)


def test_partition_by_datetime_6():
    """
    {
    "title": "test_sys_partition_basic_b.test_partition_by_datetime_6",
    "describe": "datetime分区，HASH(非分区列)",
    "tag": "system,p1"
    }
    """
    """
    datetime分区，HASH(非分区列)
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['1999-12-31 23:59:59', '2027-01-01 00:00:00', \
            '2039-01-01 00:00:00', 'MAXVALUE']

    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, 'k5', partition_name_list, \
            partition_value_list, 'HASH(k4, k1, k2)', random_bucket_num, 'column')
    client.clean(database_name)


def test_partition_by_datetime_7():
    """
    {
    "title": "test_sys_partition_basic_b.test_partition_by_datetime_7",
    "describe": "datetime分区，一个分区",
    "tag": "system,p1"
    }
    """
    """
    datetime分区，一个分区
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a']
    partition_value_list = ['MAXVALUE']

    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, 'k5', partition_name_list, \
            partition_value_list, 'HASH(k5)', random_bucket_num, 'column')
    client.clean(database_name)


def test_illegal_two_same_partition_value():
    """
    {
    "title": "test_sys_partition_basic_b.test_illegal_two_same_partition_value",
    "describe": "非法输入：指定两个相同的range分区值",
    "tag": "system,p1,fuzz"
    }
    """
    """
    非法输入：指定两个相同的range分区值
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['1000', '1000', '3000', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k4', partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13) 

    ret = False
    try:
        ret = client.create_table(table_name, DATA.schema_1, \
                partition_info, distribution_info)
    except:
        pass

    assert not ret
    client.clean(database_name)


def test_illegal_partition_value_bigger_first():
    """
    {
    "title": "test_sys_partition_basic_b.test_illegal_partition_value_bigger_first",
    "describe": "非法输入：分区值先大后小",
    "tag": "system,p1,fuzz"
    }
    """
    """
    非法输入：分区值先大后小
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['2000', '1000', '3000', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k4', partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13) 

    ret = False
    try:
        ret = client.create_table(table_name, DATA.schema_1, \
                partition_info, distribution_info)
    except:
        pass

    assert not ret
    client.clean(database_name)


def test_illegal_partition_value_out_range_1():
    """
    {
    "title": "test_sys_partition_basic_b.test_illegal_partition_value_out_range_1",
    "describe": "非法输入：分区值超过范围",
    "tag": "system,p1,fuzz"
    }
    """
    """
    非法输入：分区值超过范围
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['20', '100', '101', '3000']
    partition_info = palo_client.PartitionInfo('k1', partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13) 

    ret = False
    try:
        ret = client.create_table(table_name, DATA.schema_1, \
                partition_info, distribution_info)
    except:
        pass

    assert not ret
    client.clean(database_name)


def test_illegal_partition_value_out_range_2():
    """
    {
    "title": "test_sys_partition_basic_b.test_illegal_partition_value_out_range_2",
    "describe": "非法输入：分区值超过范围",
    "tag": "system,p1,fuzz"
    }
    """
    """
    非法输入：分区值超过范围
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['-150', '20', '100', '101']
    partition_info = palo_client.PartitionInfo('k1', partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13) 

    ret = False
    try:
        ret = client.create_table(table_name, DATA.schema_1, \
                partition_info, distribution_info)
    except:
        pass

    assert not ret
    client.clean(database_name)


def test_illegal_partition_value_not_int():
    """
    {
    "title": "test_sys_partition_basic_b.test_illegal_partition_value_not_int",
    "describe": "非法输入：分区值小数",
    "tag": "system,p1,fuzz"
    }
    """
    """
    非法输入：分区值小数
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['-50.1', '20.5', '100.0', '101']
    partition_info = palo_client.PartitionInfo('k1', partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13) 

    ret = False
    try:
        ret = client.create_table(table_name, DATA.schema_1, \
                partition_info, distribution_info)
    except:
        pass

    assert not ret
    client.clean(database_name)


def test_add_partition_1():
    """
    {
    "title": "test_sys_partition_basic_b.test_add_partition_1",
    "describe": "增加分区",
    "tag": "system,p1"
    }
    """
    """
    增加分区
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['50', '200', '1000', '65535000']
    partition_info = palo_client.PartitionInfo('k3', partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13) 
    client.create_table(table_name, DATA.schema_1, \
            partition_info, distribution_info)

    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    client.add_partition(table_name, 'add_1', '65537000')
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret
    ret = client.verify(DATA.expected_data_file_list_1, table_name)
    assert ret

    client.add_partition(table_name, 'add_2', '65538000')
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret
    ret = client.verify(list(DATA.expected_data_file_list_1) * 2, table_name)
    assert ret
    client.clean(database_name)


def test_add_partition_2():
    """
    {
    "title": "test_sys_partition_basic_b.test_add_partition_2",
    "describe": "非法输入：增加分区, 分区值位于中间和最前",
    "tag": "system,p1,fuzz"
    }
    """
    """
    非法输入：增加分区, 分区值位于中间和最前
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['50', '200', '1000', '65535000']
    partition_info = palo_client.PartitionInfo('k3', partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13) 
    client.create_table(table_name, DATA.schema_1, \
            partition_info, distribution_info)

    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    ret = False
    try:
        ret = client.add_partition(table_name, 'add_1', '100')
    except:
        pass
    assert not ret
    
    try:
        ret = client.add_partition(table_name, 'add_1', '50')
    except:
        pass

    assert not ret
    client.clean(database_name)


def test_add_partition_3():
    """
    {
    "title": "test_sys_partition_basic_b.test_add_partition_3",
    "describe": "增加分区, 修改桶数",
    "tag": "system,p1"
    }
    """
    """
    增加分区, 修改桶数
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['50', '200', '1000', '65535000']
    partition_info = palo_client.PartitionInfo('k3', partition_name_list, partition_value_list)
    distribution_type = 'RANDOM'
    distribution_info = palo_client.DistributionInfo(distribution_type, 13) 
    client.create_table(table_name, DATA.schema_1, \
            partition_info, distribution_info)

    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    client.add_partition(table_name, 'add_1', '65537000', distribution_type, 20)
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret
    ret = client.verify(DATA.expected_data_file_list_1, table_name)
    assert ret

    client.add_partition(table_name, 'add_2', '65538000', distribution_type, 37)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret
    ret = client.verify(list(DATA.expected_data_file_list_1) * 2, table_name)
    assert ret
    client.clean(database_name)


def test_add_partition_4():
    """
    {
    "title": "test_sys_partition_basic_b.test_add_partition_4",
    "describe": "增加分区, 修改桶数, 变小",
    "tag": "system,p1"
    }
    """
    """
    增加分区, 修改桶数, 变小
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['50', '200', '1000', '65535000']
    partition_info = palo_client.PartitionInfo('k3', partition_name_list, partition_value_list)
    distribution_type = 'RANDOM'
    distribution_info = palo_client.DistributionInfo(distribution_type, 13) 
    client.create_table(table_name, DATA.schema_1, \
            partition_info, distribution_info)

    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    client.add_partition(table_name, 'add_1', '65537000', distribution_type, 20)
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret
    ret = client.verify(DATA.expected_data_file_list_1, table_name)
    assert ret

    client.add_partition(table_name, 'add_2', str(2 ** 31 - 1), distribution_type, 7)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret
    ret = client.verify(list(DATA.expected_data_file_list_1) * 2, table_name)
    assert ret
    client.clean(database_name)


def test_add_partition_5():
    """
    {
    "title": "test_sys_partition_basic_b.test_add_partition_5",
    "describe": "增加分区, 修改distribution方式",
    "tag": "system,p1,fuzz"
    }
    """
    """
    增加分区, 修改distribution方式
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['50', '200', '1000', '65535000']
    partition_info = palo_client.PartitionInfo('k3', partition_name_list, partition_value_list)
    distribution_type = 'RANDOM'
    distribution_info = palo_client.DistributionInfo(distribution_type, 13) 
    client.create_table(table_name, DATA.schema_1, \
            partition_info, distribution_info)

    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    ret = False
    try:
        ret = client.add_partition(table_name, 'add_1', \
                str(2 ** 31 - 1), 'HASH(k1, k2)', 13)
    except:
        pass
    assert not ret
    client.clean(database_name)
    

def test_add_partition_6():
    """
    {
    "title": "test_sys_partition_basic_b.test_add_partition_6",
    "describe": "单分区表，增加分区",
    "tag": "system,p1,fuzz"
    }
    """
    """
    单分区表，增加分区
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    distribution_type = 'RANDOM'
    distribution_info = palo_client.DistributionInfo(distribution_type, 13) 
    client.create_table(table_name, \
            DATA.schema_1, distribution_info=distribution_info)

    assert client.show_tables(table_name)

    ret = False
    try:
        ret = client.add_partition(table_name, 'add_1', \
                str(2 ** 31 - 1), 'HASH(k1, k2)', 13)
    except:
        pass
    assert not ret
    client.clean(database_name)
    

def test_drop_partition_1():
    """
    {
    "title": "test_sys_partition_basic_b.test_drop_partition_1",
    "describe": "删除不存在的分区",
    "tag": "system,p1,fuzz"
    }
    """
    """
    删除不存在的分区
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['50', '200', '1000', '65535000']
    partition_info = palo_client.PartitionInfo('k3', partition_name_list, partition_value_list)
    distribution_type = 'RANDOM'
    distribution_info = palo_client.DistributionInfo(distribution_type, 13) 
    client.create_table(table_name, DATA.schema_1, \
            partition_info, distribution_info)

    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    ret = False
    try:
        ret = client.drop_partition(table_name, 'add_1')
    except:
        pass
    assert not ret
    client.clean(database_name)
 
   
def test_drop_partition_2():
    """
    {
    "title": "test_sys_partition_basic_b.test_drop_partition_2",
    "describe": "删除分区到一个分区也不留",
    "tag": "system,p1,fuzz"
    }
    """
    """
    删除分区到只剩一个分区
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['50', '200', '1000', '65535000']
    partition_info = palo_client.PartitionInfo('k3', partition_name_list, partition_value_list)
    distribution_type = 'RANDOM'
    distribution_info = palo_client.DistributionInfo(distribution_type, 13) 
    client.create_table(table_name, DATA.schema_1, \
            partition_info, distribution_info)

    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    assert client.drop_partition(table_name, partition_name_list[0])
    assert client.drop_partition(table_name, partition_name_list[1])
    assert client.drop_partition(table_name, partition_name_list[2])
    assert client.drop_partition(table_name, partition_name_list[3])
    ret = False
    try:
        ret = client.drop_partition(table_name, partition_name_list[3])
    except:
        pass
    assert not ret
    client.clean(database_name)
    

def test_drop_partition_3():
    """
    {
    "title": "test_sys_partition_basic_b.test_drop_partition_3",
    "describe": "删除，导入，数据校验",
    "tag": "system,p1"
    }
    """
    """
    删除，导入，数据校验
    """
    # TODO 删分区，加分区，导入过滤, 导入到指定分区
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', \
            'partition_d', 'partition_e', 'partition_f', 'partition_g']
    partition_value_list = ['300', '500', '900', '2200', '2300', '2800', '4000']
    partition_info = palo_client.PartitionInfo('k3', partition_name_list, partition_value_list)
    distribution_type = 'RANDOM'
    distribution_info = palo_client.DistributionInfo(distribution_type, 13) 
    client.create_table(table_name, DATA.schema_1, \
            partition_info, distribution_info)

    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    #导入
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    #校验
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    #删分区
    client.drop_partition(table_name, 'partition_a')
    #加分区
    assert client.add_partition(table_name, 'add_1', '5000')
    #校验
    assert client.verify([DATA.expected_file_1], table_name)
    #加分区
    assert client.add_partition(table_name, 'add_2', '6000')
    #删分区
    client.drop_partition(table_name, 'partition_c')
    #校验
    assert client.verify([DATA.expected_file_2], table_name)
    #删分区
    client.drop_partition(table_name, 'partition_e')
    #校验
    assert client.verify([DATA.expected_file_3], table_name)
    #加分区
    assert client.add_partition(table_name, 'add_3', 'MAXVALUE')
    #删分区
    client.drop_partition(table_name, 'partition_g')
    #校验
    assert client.verify([DATA.expected_file_4], table_name)
    client.clean(database_name)


def test_loop_add_drop():
    """
    {
    "title": "test_sys_partition_basic_b.test_loop_add_drop",
    "describe": "增删分区反复",
    "tag": "system,p1"
    }
    """
    """
    增删分区反复
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', \
            'partition_d', 'partition_e', 'partition_f', 'partition_g']
    partition_value_list = ['300', '500', '900', '2200', '2300', '3800', '4000']
    partition_info = palo_client.PartitionInfo('k3', partition_name_list, partition_value_list)
    distribution_type = 'RANDOM'
    distribution_info = palo_client.DistributionInfo(distribution_type, 13) 
    client.create_table(table_name, DATA.schema_1, \
            partition_info, distribution_info)

    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    #导入
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    #校验
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    count = 0
    #while count < 500:
    #ext 32000
    while count < 10:
        #删分区
        client.drop_partition(table_name, 'partition_g')
        #加分区
        assert client.add_partition(table_name, 'partition_g', '4000', \
                'RANDOM', random.randrange(1, 300))
        count = count + 1
    #校验
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    client.clean(database_name)


def test_add_partition_times_then_drop_times():
    """
    {
    "title": "test_sys_partition_basic_b.test_add_partition_times_then_drop_times",
    "describe": "连续增加分区，连续删除分区",
    "tag": "system,p1"
    }
    """
    """
    连续增加分区，连续删除分区
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', \
            'partition_d', 'partition_e', 'partition_f', 'partition_g']
    partition_value_list = ['300', '500', '900', '2200', '2300', '3800', '4000']
    partition_info = palo_client.PartitionInfo('k3', partition_name_list, partition_value_list)
    distribution_type = 'RANDOM'
    distribution_info = palo_client.DistributionInfo(distribution_type, 13) 
    client.create_table(table_name, DATA.schema_1, \
            partition_info, distribution_info)

    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    #导入
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    #校验
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    count = 1
    while count < 30:
        #加分区
        assert client.add_partition(table_name, \
                'partition_add_%d' % (count), str(4000 + count * 10), \
                'RANDOM', random.randrange(1, 300))
        count = count + 1
    #校验
    assert client.verify(DATA.expected_data_file_list_1, table_name)

    count = 1
    while count < 30:
        #删分区
        assert client.drop_partition(table_name, 'partition_add_%d' % (count))
        count = count + 1
    #校验
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    #导入
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    #校验
    assert client.verify(list(DATA.expected_data_file_list_1) * 2, table_name)
    client.clean(database_name)


def test_drop_partition_key():
    """
    {
    "title": "test_sys_partition_basic_b.test_drop_partition_key",
    "describe": "删除分区列",
    "tag": "system,p1,fuzz"
    }
    """
    """
    删除分区列
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', '50']

    partition_info = palo_client.PartitionInfo('k1', \
            partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', 13) 
    client.create_table(table_name, DATA.schema_1, \
            partition_info, distribution_info)
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)

    column_name_list = ['k1',]
    ret = False
    try:
        ret = client.schema_change_drop_column(table_name, column_name_list, \
            is_wait_job=True, is_wait_delete_old_schema=True)
    except:
        pass

    assert not ret
    client.clean(database_name)


def test_partition_by_value():
    """
    {
    "title": "test_sys_partition_basic_b.test_partition_by_value",
    "describe": "用value列分区",
    "tag": "system,p1"
    }
    """
    """
    用value列分区
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']

    partition_info = palo_client.PartitionInfo('v1', \
            partition_name_list, partition_value_list)
    ret = False
    try:
        ret = client.create_table(table_name, DATA.schema_1, partition_info)
    except:
        pass
    assert not ret
    client.clean(database_name)
 

def teardown_module():
    """
    tearDown
    """
    pass


if __name__ == '__main__':
    import pdb
    pdb.set_trace()
    setup_module()

