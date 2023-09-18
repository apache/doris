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


def test_partition_by_tinyint_random_column_1():
    """
    {
    "title": "test_sys_partition_basic_a.test_partition_by_tinyint_random_column_1",
    "describe": "tinyint分区, 边界值，random, random_bucket_num, column",
    "tag": "system,p0,fuzz"
    }
    """
    """
    tinyint分区, 边界值，random, random_bucket_num, column
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', \
            'partition_d', 'partition_e', 'partition_f']
    partition_value_list = ['-127', '-1', '0', '1', '126', '127']
    
    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, 'k1', \
            partition_name_list, partition_value_list, \
            'RANDOM', random_bucket_num, 'column')
    client.clean(database_name)


def test_partition_by_tinyint_random_hash_1():
    """
    {
    "title": "test_sys_partition_basic_a.test_partition_by_tinyint_random_hash_1",
    "describe": "tinyint分区, 边界值，hash(分区列), random_bucket_num, column",
    "tag": "system,p1,fuzz"
    }
    """
    """
    tinyint分区, 边界值，hash(分区列), random_bucket_num, column
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', \
            'partition_d', 'partition_e', 'partition_f']
    partition_value_list = ['-127', '-1', '0', '1', '126', '127']
    
    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, 'k1', \
            partition_name_list, partition_value_list, \
            'HASH(k1)', random_bucket_num, 'column')
    client.clean(database_name)


def test_partition_by_tinyint_random_hash_2():
    """
    {
    "title": "test_sys_partition_basic_a.test_partition_by_tinyint_random_hash_2",
    "describe": "tinyint分区, 边界值，hash(非分区列), random_bucket_num, column",
    "tag": "system,p0,fuzz"
    }
    """
    """
    tinyint分区, 边界值，hash(非分区列), random_bucket_num, column
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', \
            'partition_d', 'partition_e', 'partition_f']
    partition_value_list = ['-127', '-1', '0', '1', '126', '127']
    
    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, 'k1', \
            partition_name_list, partition_value_list, \
            'HASH(k2)', random_bucket_num, 'column')
    client.clean(database_name)


def test_partition_by_tinyint_random_hash_3():
    """
    {
    "title": "test_sys_partition_basic_a.test_partition_by_tinyint_random_hash_3",
    "describe": "tinyint分区, 边界值，hash(所有非分区列), random_bucket_num, column",
    "tag": "system,p0,fuzz"
    }
    """
    """
    tinyint分区, 边界值，hash(所有非分区列), random_bucket_num, column
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', \
            'partition_d', 'partition_e', 'partition_f']
    partition_value_list = ['-127', '-1', '0', '1', '126', '127']
    
    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, 'k1', \
            partition_name_list, partition_value_list, \
            'HASH(k2, k3, k4, k5)', random_bucket_num, 'column')
    client.clean(database_name)


def test_partition_by_tinyint_random_hash_4():
    """
    {
    "title": "test_sys_partition_basic_a.test_partition_by_tinyint_random_hash_4",
    "describe": "tinyint分区, 边界值，hash(所有列), random_bucket_num, column",
    "tag": "system,p0,fuzz"
    }
    """
    """
    tinyint分区, 边界值，hash(所有列), random_bucket_num, column
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', \
            'partition_d', 'partition_e', 'partition_f']
    partition_value_list = ['-127', '-1', '0', '1', '126', '127']
    
    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, 'k1', \
            partition_name_list, partition_value_list, \
            'HASH(k1, k2, k3, k4, k5)', random_bucket_num, 'column')
    client.clean(database_name)


def test_partition_by_tinyint_random_hash_5():
    """
    {
    "title": "test_sys_partition_basic_a.test_partition_by_tinyint_random_hash_5",
    "describe": " tinyint分区, 边界值，hash(部分非分区列), random_bucket_num, column",
    "tag": "system,p1,fuzz"
    }
    """
    """
    tinyint分区, 边界值，hash(部分非分区列), random_bucket_num, column
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', \
            'partition_d', 'partition_e', 'partition_f']
    partition_value_list = ['-127', '-1', '0', '1', '126', '127']
    
    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, 'k1', \
            partition_name_list, partition_value_list, \
            'HASH(k3, k4, k5)', random_bucket_num, 'column')
    client.clean(database_name)


def test_partition_by_smallint():
    """
    {
    "title": "test_sys_partition_basic_a.test_partition_by_smallint",
    "describe": "smallint分区",
    "tag": "system,p1"
    }
    """
    """
    smallint分区
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['10', '20', '30', 'MAXVALUE']

    partition_check(table_name, 'k2', partition_name_list, \
            partition_value_list, 'RANDOM', 13, 'column')
    client.clean(database_name)


def test_partition_by_smallint_over_range():
    """
    {
    "title": "test_sys_partition_basic_a.test_partition_by_smallint_over_range",
    "describe": "smallint分区, 边界值，hash(部分非分区列)，乱序，random_bucket_num, column",
    "tag": "system,p1,fuzz"
    }
    """
    """
    smallint分区, 边界值，hash(部分非分区列)，乱序，random_bucket_num, column
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', \
            'partition_d', 'partition_e', 'partition_f']
    partition_value_list = [str(-2 ** 15 + 1), str(-2 ** 15 + 2), \
            '0', '1', str(2 ** 15 - 2), str(2 ** 15 - 1)]
    
    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, 'k2', \
            partition_name_list, partition_value_list, \
            'HASH(k4, k5, k1)', random_bucket_num, 'column')
    client.clean(database_name)


def test_partition_by_int():
    """
    {
    "title": "test_sys_partition_basic_a.test_partition_by_int",
    "describe": "int分区",
    "tag": "system,p1"
    }
    """
    """
    int分区
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['100', '200', '300', 'MAXVALUE']

    partition_check(table_name, 'k3', partition_name_list, \
            partition_value_list, 'RANDOM', 13, 'column')
    client.clean(database_name)


def test_partition_by_int_over_range():
    """
    {
    "title": "test_sys_partition_basic_a.test_partition_by_int_over_range",
    "describe": "int分区, 边界值，hash(部分非分区列)，乱序，random_bucket_num, column",
    "tag": "system,p1,fuzz"
    }
    """
    """
    int分区, 边界值，hash(部分非分区列)，乱序，random_bucket_num, column
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', \
            'partition_d', 'partition_e', 'partition_f']
    partition_value_list = [str(-2 ** 31 + 1), str(-2 ** 31 + 2), \
            '0', '1', str(2 ** 31 - 2), str(2 ** 31 - 1)]
    
    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, 'k3', \
            partition_name_list, partition_value_list, \
            'HASH(k2, k4, k5, k1)', random_bucket_num, 'column')
    client.clean(database_name)


def test_partition_by_bigint():
    """
    {
    "title": "test_sys_partition_basic_a.test_partition_by_bigint",
    "describe": "bigint分区",
    "tag": "system,p1"
    }
    """
    """
    bigint分区
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['1000', '2000', '3000', 'MAXVALUE']

    partition_check(table_name, 'k4', partition_name_list, \
            partition_value_list, 'RANDOM', 13, 'column')
    client.clean(database_name)


def test_partition_by_bigint_over_range():
    """
    {
    "title": "test_sys_partition_basic_a.test_partition_by_bigint_over_range",
    "describe": "bigint分区, 边界值，hash(部分非分区列)，乱序，random_bucket_num, column",
    "tag": "system,p1,fuzz"
    }
    """
    """
    bigint分区, 边界值，hash(部分非分区列)，乱序，random_bucket_num, column
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', \
            'partition_d', 'partition_e', 'partition_f']
    partition_value_list = [str(-2 ** 63 + 1), str(-2 ** 63 + 2), \
            '0', '1', str(2 ** 63 - 2), str(2 ** 63 - 1)]
    
    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, 'k4', \
            partition_name_list, partition_value_list, \
            'HASH(k2, k5, k1)', random_bucket_num, 'column')
    client.clean(database_name)


def test_partition_by_bigint_one_partition():
    """
    {
    "title": "test_sys_partition_basic_a.test_partition_by_bigint_one_partition",
    "describe": "bigint分区, 一个分区",
    "tag": "system,p1"
    }
    """
    """
    bigint分区, 一个分区
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a']
    partition_value_list = [str(2 ** 63 - 1)]
    
    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, 'k4', \
            partition_name_list, partition_value_list, \
            'HASH(k2, k5, k1)', random_bucket_num, 'column')
    client.clean(database_name)


def teardown_module():
    """
    tearDown
    """
    pass


if __name__ == '__main__':
    setup_module()
    print(broker_info)
    test_partition_by_int()

