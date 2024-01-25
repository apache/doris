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
#   @file test_sys_temp_partition_function.py
#   @date 2020/04/28
#   @brief This file is a test file for palo temp partition function
#
#############################################################################

"""
对分区表建立临时分区，并进行导入、删除、修改等操作，对结果进行正确性校验
"""

import sys
sys.path.append("../")
from data import partition as DATA
from lib import palo_config
from lib import palo_client
from lib import util
import random
from data import schema_change as SCHEMA_CHANGE_DATA


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


def check2_palo(line1, line2):
    """
    check2_palo
    :param ret1:
    :param ret2:
    :return:
    """
    ret1 = execute(line1)
    ret2 = execute(line2)
    util.check(ret1, ret2)


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
    # assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    # assert client.verify(list(DATA.expected_data_file_list_1) * 2, table_name)


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


def execute(line):
    """execute palo sql and return reuslt"""
    print(line)
    palo_result = client.execute(line)
    print(palo_result)
    return palo_result


def test_temp_partition_basic():
    """
    {
    "title": "test_sys_temp_partition_function.test_temp_partition",
    "describe": "验证临时分区基本功能",
    "tag": "function,P0"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c',
                           'partition_d', 'partition_e', 'partition_f']
    partition_value_list = ['-127', '-1', '0', '1', '126', '127']

    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, 'k1',
                    partition_name_list, partition_value_list,
                    'RANDOM', random_bucket_num, 'column')

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)

    # 向临时分区导入数据后，查询正确
    temp_partition_name = 'partition_g'
    client.add_temp_partition(table_name, temp_partition_name, '20', database_name=database_name)
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name, temp_partition_list=[temp_partition_name])
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=0.5)
    line1 = 'select * from %s.%s where k1 < 20 order by k1' % (database_name, table_name)
    line2 = 'select * from %s.%s TEMPORARY PARTITION %s order by k1' % (database_name, table_name, temp_partition_name)
    check2_palo(line1, line2)

    # 新建临时分区后不影响原表数据
    assert client.verify(DATA.expected_data_file_list_1, table_name)

    # 重复向临时分区导入数据后，查询正确，且不影响原表数据
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(list(DATA.expected_data_file_list_1) * 2, table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name, temp_partition_list=[temp_partition_name])
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=0.5)
    line1 = 'select * from %s.%s where k1 < 20 order by k1' % (database_name, table_name)
    line2 = 'select * from %s.%s TEMPORARY PARTITION %s order by k1' % (database_name, table_name, temp_partition_name)
    check2_palo(line1, line2)

    # 删除临时分区不影响原表数据
    assert client.drop_temp_partition(database_name, table_name, temp_partition_name)
    assert client.verify(list(DATA.expected_data_file_list_1) * 2, table_name)

    client.clean(database_name)


def test_create_temp_partition():
    """
    {
    "title": "test_sys_temp_partition_function.test_create_temp_partition",
    "describe": "验证新建临时分区功能",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c',
                           'partition_d', 'partition_e', 'partition_f']
    partition_value_list = ['-127', '-1', '0', '1', '126', '127']

    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, 'k1',
                    partition_name_list, partition_value_list,
                    'RANDOM', random_bucket_num, 'column')

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)

    temp_partition_name = 'partition_g'
    assert client.add_temp_partition(table_name, temp_partition_name, '20', database_name=database_name)
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name, temp_partition_list=[temp_partition_name])
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=0.5)
    line1 = 'select * from %s.%s where k1 < 20 order by k1' % (database_name, table_name)
    line2 = 'select * from %s.%s TEMPORARY PARTITION %s order by k1' % (database_name, table_name, temp_partition_name)
    check2_palo(line1, line2)

    util.assert_return(False, 'Duplicate partition name',
                       client.add_temp_partition, table_name, temp_partition_name, '20', database_name=database_name)
    util.assert_return(False, 'Duplicate partition name',
                       client.add_temp_partition, table_name, 'partition_a', '20', database_name=database_name)
    partition_name = temp_partition_name + '_1'
    assert client.add_temp_partition(table_name, partition_name, '30', database_name=database_name)

    partition_name = temp_partition_name + '_2'
    util.assert_return(False, 'is intersected with range',
                       client.add_temp_partition, table_name, partition_name, (('25',), ('35',)),
                       database_name=database_name)

    partition_name = temp_partition_name + '_3'
    client.add_temp_partition(table_name, partition_name, '40',
                              distribute_type="RANDOM", bucket_num=5,
                              replication_num=1, database_name=database_name)

    client.clean(database_name)


def test_select_from_temp_partition():
    """
    {
    "title": "test_sys_temp_partition_function.test_select_from_temp_partition",
    "describe": "验证临时分区数据查询功能",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c',
                           'partition_d', 'partition_e', 'partition_f']
    partition_value_list = ['-127', '-1', '0', '1', '126', '127']

    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, 'k1',
                    partition_name_list, partition_value_list,
                    'RANDOM', random_bucket_num, 'column')

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)

    temp_partition_name_1 = 'partition_20'
    assert client.add_temp_partition(table_name, temp_partition_name_1, '20', database_name=database_name)
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name, temp_partition_list=[temp_partition_name_1])
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=0.5)
    line1 = 'select * from %s.%s where k1 < 20 order by k1' % (database_name, table_name)
    line2 = 'select * from %s.%s TEMPORARY PARTITION %s order by k1' \
            % (database_name, table_name, temp_partition_name_1)
    check2_palo(line1, line2)

    temp_partition_name_2 = 'partition_20_126'
    assert client.add_temp_partition(table_name, temp_partition_name_2, (('20',), ('126',)), 
                                     database_name=database_name)
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name, temp_partition_list=[temp_partition_name_2])
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=1)
    line1 = 'select * from %s.%s where k1 >=20 and k1 < 126 order by k1' \
            % (database_name, table_name)
    line2 = 'select * from %s.%s TEMPORARY PARTITION %s order by k1' % (
    database_name, table_name, temp_partition_name_2)
    check2_palo(line1, line2)

    line1 = 'select * from {0}.{1} a join {0}.{1} b on a.{2}=b.{2} where a.{3} < 20' \
            ' and b.{3} >= 20 and b.{3} < 126 order by a.{3}, b.{3}'.format(database_name, table_name, 'v2', 'k1')
    line2 = 'select * from {0}.{1} TEMPORARY PARTITION partition_20 a join' \
            ' {0}.{1} TEMPORARY PARTITION partition_20_126 b on a.{2}=b.{2}' \
            ' order by a.{3}, b.{3}'.format(database_name, table_name, 'v2', 'k1')
    check2_palo(line1, line2)

    line1 = 'select * from {0}.{1} a join {0}.{1} b on a.{2}=b.{2} where a.{3} < 20' \
            ' and b.{3} >= 1 and b.{3} < 126 order by a.{3}, b.{3}'.format(database_name, table_name, 'v2', 'k1')
    line2 = 'select * from {0}.{1} TEMPORARY PARTITION partition_20 a join' \
            ' {0}.{1} PARTITION partition_e b on a.{2}=b.{2}' \
            ' order by a.{3}, b.{3}'.format(database_name, table_name, 'v2', 'k1')
    check2_palo(line1, line2)

    line1 = 'select * from {0}.{1} a join {0}.{1} b on a.{2}=b.{2} where a.{3} < 20' \
            ' order by a.{3}, b.{3}'.format(database_name, table_name, 'v2', 'k1')
    line2 = 'select * from {0}.{1} TEMPORARY PARTITION partition_20 a join' \
            ' {0}.{1} b on a.{2}=b.{2}' \
            ' order by a.{3}, b.{3}'.format(database_name, table_name, 'v2', 'k1')
    check2_palo(line1, line2)

    client.clean(database_name)


def test_create_temp_partition_with_no_partition_tb():
    """
    {
    "title": "test_sys_temp_partition_function.test_create_temp_partition_with_no_partition_tb",
    "describe": "对没有分区的表，验证新建临时分区的功能",
    "tag": "function,fuzz,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    client.create_table(table_name, DATA.schema_1)
    assert client.show_tables(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)

    temp_partition_name = 'partition_g'
    # after list partiton
    msg = "Only support adding partition to range and list partitioned table"
    util.assert_return(False, msg,
                       client.add_temp_partition, table_name, temp_partition_name, '20', database_name=database_name)

    client.clean(database_name)


def test_drop_temp_partition():
    """
    {
    "title": "test_sys_temp_partition_function.test_drop_temp_partition",
    "describe": "验证删除临时分区功能",
    "tag": "function,fuzz,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c',
                           'partition_d', 'partition_e', 'partition_f']
    partition_value_list = ['-127', '-1', '0', '1', '126', '127']

    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, 'k1',
                    partition_name_list, partition_value_list,
                    'RANDOM', random_bucket_num, 'column')

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)

    temp_partition_name = 'partition_g'
    assert client.add_temp_partition(table_name, temp_partition_name, '20', database_name=database_name)
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name, temp_partition_list=[temp_partition_name])
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=0.5)
    line1 = 'select * from %s.%s where k1 < 20 order by k1' % (database_name, table_name)
    line2 = 'select * from %s.%s TEMPORARY PARTITION %s order by k1' % (database_name, table_name, temp_partition_name)
    check2_palo(line1, line2)

    util.assert_return(True, '',
                       client.drop_temp_partition, database_name, table_name, temp_partition_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(list(DATA.expected_data_file_list_1) * 2, table_name)

    partition_name = temp_partition_name + '_1'
    assert client.add_temp_partition(table_name, partition_name, '20', database_name=database_name)
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name, temp_partition_list=[partition_name])
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=0.5)

    util.assert_return(True, '',
                       client.drop_temp_partition, database_name, table_name, partition_name)

    sql = 'recover partition %s from %s.%s' % (partition_name, database_name, table_name)
    util.assert_return(False, '', client.execute, sql)

    partition_name = temp_partition_name + '_2'
    util.assert_return(False, '',
                       client.drop_temp_partition, database_name, table_name, partition_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(list(DATA.expected_data_file_list_1) * 3, table_name)

    client.clean(database_name)


def test_alter_temp_partition():
    """
    {
    "title": "test_sys_temp_partition_function.test_alter_temp_partition",
    "describe": "验证修改临时分区功能",
    "tag": "function,fuzz,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c',
                           'partition_d', 'partition_e', 'partition_f']
    partition_value_list = ['-127', '0', '10', '20', '126', '127']

    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, 'k1',
                    partition_name_list, partition_value_list,
                    'RANDOM', random_bucket_num, 'column')

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)

    temp_partition_name = 'partition_n2'
    assert client.add_temp_partition(table_name, temp_partition_name, '-2', database_name=database_name)
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name, temp_partition_list=[temp_partition_name])
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=1)
    assert ret, "expect batch load success"
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=0)
    assert not ret, "expect batch load failed"
    line1 = 'select * from %s.%s where k1 < -2 order by k1' % (database_name, table_name)
    line2 = 'select * from %s.%s TEMPORARY PARTITION %s order by k1' % (database_name, table_name, temp_partition_name)
    check2_palo(line1, line2)

    util.assert_return(False, 'range lists are not stricly matched',
                       client.modify_temp_partition, database_name, table_name, ['partition_a'], [temp_partition_name])

    util.assert_return(False, '',
                       client.modify_temp_partition, database_name, table_name,
                       ['partition_a', 'partition_b'], [temp_partition_name])
    util.assert_return(True, '',
                       client.modify_temp_partition, database_name, table_name,
                       ['partition_a', 'partition_b'], [temp_partition_name], strict_range="false")

    ret = client.get_partition(table_name, temp_partition_name, database_name=database_name)
    assert ret

    temp_partition_name = 'partition_n1'
    assert client.add_temp_partition(table_name, temp_partition_name, '0', database_name=database_name)
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name, temp_partition_list=[temp_partition_name])
    client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=0.9)
    line1 = 'select * from %s.%s where k1 >= -1 and k1 < 0 order by k1' % (database_name, table_name)
    line2 = 'select * from %s.%s TEMPORARY PARTITION %s order by k1' % (database_name, table_name, temp_partition_name)
    check2_palo(line1, line2)

    util.assert_return(False, '',
                       client.modify_temp_partition, database_name, table_name, ['partition_b'], [temp_partition_name])

    temp_partition_name = 'partition_20'
    assert client.add_temp_partition(table_name, temp_partition_name, '20', database_name=database_name)
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name, temp_partition_list=[temp_partition_name])
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=0.9)
    line1 = 'select * from %s.%s where k1 >= 0 and k1 < 20 order by k1' % (database_name, table_name)
    line2 = 'select * from %s.%s TEMPORARY PARTITION %s order by k1' % (database_name, table_name, temp_partition_name)
    check2_palo(line1, line2)

    util.assert_return(False, 'range lists are not stricly matched',
                       client.modify_temp_partition, database_name, table_name, ['partition_c'], [temp_partition_name])

    temp_partition_name = 'partition_20_126'
    assert client.add_temp_partition(table_name, temp_partition_name, (('20',), ('126',)), database_name=database_name)
    util.assert_return(True, '',
                       client.modify_temp_partition, database_name, table_name, ['partition_e'], [temp_partition_name])
    ret = client.get_partition(table_name, 'partition_e', database_name=database_name)
    assert ret
    temp_partition_name = 'partition_126_127'
    assert client.add_temp_partition(table_name, temp_partition_name, (('126',), ('127',)), database_name=database_name)
    util.assert_return(True, '',
                       client.modify_temp_partition, database_name, table_name, ['partition_f'], [temp_partition_name],
                       use_temp_partition_name='True')
    ret = client.get_partition(table_name, temp_partition_name, database_name=database_name)
    assert ret

    client.clean(database_name)


def test_truncate_temp_partition():
    """
    {
    "title": "test_sys_temp_partition_function.test_truncate_temp_partition",
    "describe": "验证truncate命令对临时分区的影响",
    "tag": "function,p1"
    }
    """

    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c',
                           'partition_d', 'partition_e', 'partition_f']
    partition_value_list = ['-127', '0', '10', '20', '126', '127']

    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, 'k1',
                    partition_name_list, partition_value_list,
                    'RANDOM', random_bucket_num, 'column')

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)

    temp_partition_name = 'partition_20'
    assert client.add_temp_partition(table_name, temp_partition_name, '20', database_name=database_name)
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name, temp_partition_list=[temp_partition_name])
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=1)
    assert ret
    line1 = 'select * from %s.%s where k1 < 20 order by k1' % (database_name, table_name)
    line2 = 'select * from %s.%s TEMPORARY PARTITION %s order by k1' % (database_name, table_name, temp_partition_name)
    check2_palo(line1, line2)

    # case1: 不可使用 Truncate 命令清空临时分区
    sql = 'TRUNCATE TABLE %s.%s PARTITION (%s)' % (database_name, table_name, temp_partition_name)
    util.assert_return(False, 'does not exist',
                       client.execute, sql)
    sql = 'TRUNCATE TABLE %s.%s TEMPORARY PARTITION (%s)' % (database_name, table_name, temp_partition_name)
    util.assert_return(False, 'Not support truncate temp partitions',
                       client.execute, sql)

    # case2: 使用 Truncate 命令清空正式分区时，不影响临时分区。
    sql = 'TRUNCATE TABLE %s.%s PARTITION (%s)' % (database_name, table_name, 'partition_c')
    util.assert_return(True, '', client.execute, sql)

    line1 = 'select * from %s.%s where k1 < 20 order by k1' % (database_name, table_name)
    line2 = 'select * from %s.%s TEMPORARY PARTITION %s where k1 >= 10 order by k1' % \
            (database_name, table_name, temp_partition_name)
    check2_palo(line1, line2)

    # case3: 使用 Truncate 命令清空表，表的临时分区会被删除，且不可恢复。
    sql = 'TRUNCATE TABLE %s.%s' % (database_name, table_name)
    util.assert_return(True, '', client.execute, sql)

    line2 = 'select * from %s.%s TEMPORARY PARTITION %s where k1 >= 10 order by k1' % (
    database_name, table_name, temp_partition_name)
    util.assert_return(False, 'doesn\'t exist', client.execute, line2)

    client.clean(database_name)


def test_alter_temp_partition_limit1():
    """
    {
    "title": "test_sys_temp_partition_function.test_alter_temp_partition_limit1",
    "describe": "当表存在临时分区时，无法使用 Alter 命令对表进行 Schema Change、Rollup 等变更操作",
    "tag": "function,fuzz,p1"
    }
    """
    # 当表存在临时分区时，无法使用 Alter 命令对表进行 Schema Change、Rollup 等变更操作
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c',
                           'partition_d', 'partition_e', 'partition_f']
    partition_value_list = ['-127', '0', '10', '20', '126', '127']

    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, 'k1',
                    partition_name_list, partition_value_list,
                    'RANDOM', random_bucket_num, 'column')

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)

    temp_partition_name = 'partition_n2'
    assert client.add_temp_partition(table_name, temp_partition_name, '-2', database_name=database_name)
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name, temp_partition_list=[temp_partition_name])
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=1)
    assert ret, "expect batch load success"
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=0)
    assert not ret, "expect batch load failed"
    line1 = 'select * from %s.%s where k1 < -2 order by k1' % (database_name, table_name)
    line2 = 'select * from %s.%s TEMPORARY PARTITION %s order by k1' % (database_name, table_name, temp_partition_name)
    check2_palo(line1, line2)

    column_name_list = ['k2', 'k1', 'v3', 'v2', 'v1']
    util.assert_return(False, 'Can not alter table when there are temp partitions in table',
                       client.schema_change_order_column, table_name, column_name_list,
                       is_wait_job=True, is_wait_delete_old_schema=True)

    util.assert_return(False, 'Can not alter table when there are temp partitions in table',
                       client.create_rollup_table, table_name, index_name,
                       SCHEMA_CHANGE_DATA.rollup_field_list_2, is_wait=True)
    client.clean(database_name)


def test_alter_temp_partition_limit2():
    """
    {
    "title": "test_sys_temp_partition_function.test_alter_temp_partition_limit2",
    "describe": "当表在进行变更操作时，无法对表添加临时分区",
    "tag": "function,fuzz,p1"
    }
    """
    # 当表在进行变更操作时，无法对表添加临时分区
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c',
                           'partition_d', 'partition_e', 'partition_f']
    partition_value_list = ['-127', '0', '10', '20', '126', '127']

    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, 'k1',
                    partition_name_list, partition_value_list,
                    'RANDOM', random_bucket_num, 'column')

    client.create_rollup_table(table_name, index_name, ['k1', 'k2', 'k4', 'k3', 'k5', 'v1'], is_wait=False)

    temp_partition_name = 'partition_n2'
    util.assert_return(False, 'Do not allow doing ALTER ops',
                       client.add_temp_partition, table_name, temp_partition_name, '-2', database_name=database_name)
    client.clean(database_name)


def teardown_module():
    """
    tearDown
    """
    pass


if __name__ == '__main__':
    # import pdb
    # pdb.set_trace()
    setup_module()
    print(broker_info)
    test_temp_partition_basic()
