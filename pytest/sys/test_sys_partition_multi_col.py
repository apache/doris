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
  * @file test_sys_partition_multi_col.py
  * @brief Test for partition by multi column
  *
  **************************************************************************/
"""

import random
import pytest

from data import partition as DATA
from lib import palo_client
from lib import util
from lib import palo_config

LOG = palo_client.LOG
L = palo_client.L
config = palo_config.config
broker_name = config.broker_name
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
    to check if partition ok
    验证分区是否创建成功
    """
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)


def check_load_and_verify(table_name, partition_name_list):
    """
    load and check data
    验证表是否创建成功，分区是否创建成功，导入数据，校验
    """
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(list(DATA.expected_data_file_list_1) * 2, table_name)


def partition_check(table_name, column_name, partition_name_list,
                    partition_value_list, distribution_type, bucket_num):
    """
    create table, check table, check partition, load, check data
    检查，验证
    """
    partition_info = palo_client.PartitionInfo(column_name,
                                               partition_name_list, partition_value_list)
    print(partition_info)
    distribution_info = palo_client.DistributionInfo(distribution_type, bucket_num)
    print(distribution_info)
    client.create_table(table_name, DATA.schema_1,
                        partition_info, distribution_info, set_null=True)
    assert client.show_tables(table_name)
    check_partition_list(table_name, partition_name_list)
    check_load_and_verify(table_name, partition_name_list)


def test_partition_multi_col_agg():
    """
    {
    "title": "test_sys_partition_multi_col.test_partition_multi_col_agg",
    "describe": "test aggregate key with multi partition columns, 测试聚合模型进行多列分区",
    "tag": "function,p1"
    }
    """
    """
    test aggregate key with multi partition columns
    测试聚合模型进行多列分区
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c',
                           'partition_d', 'partition_e', 'partition_f']
    partition_value_list = [('-127', '-127'), ('-1', '-1'), ('0', '-1'), 
                            ('0', '0'), ('126', '126'), ('127')]

    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, ['k1', 'k2'],
                    partition_name_list, partition_value_list,
                    'HASH(k2)', random_bucket_num,)
    client.clean(database_name)


def test_partition_multi_dataype():
    """
    {
    "title": "test_sys_partition_multi_col.test_partition_multi_dataype",
    "describe": "partition columns are int & datetime, 分区列类型为int/datetime组合",
    "tag": "function,p1,fuzz"
    }
    """
    """
    partition columns are int & datetime
    分区列类型为int/datetime组合
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_column_name = ['k1', 'k5']
    partition_name_list = ['partition_a', 'partition_b', 'partition_c',
                           'partition_d', 'partition_e', 'partition_f']
    partition_value_list = [('-127', '2010-01-01'), ('1', '2010-01-02'), ('2', '2010-01-03'),
                            ('3', '2010-01-04'), ('4', '2010-01-01'), 'MAXVALUE']
    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_info = palo_client.PartitionInfo(partition_column_name,
                                               partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(k1)', random_bucket_num)
    try:
        client.create_table(table_name, DATA.schema_1,
                              partition_info, distribution_info)
        ret = True
    except Exception as e:
        print(str(e))
        ret = False
    assert not ret
    client.clean(database_name)


def test_partition_multi_col_value():
    """
    {
    "title": "test_sys_partition_multi_col.test_partition_multi_col_value",
    "describe": "partiton columns should be key, 分区列只能是key列",
    "tag": "function,p1,fuzz"
    }
    """
    """
    partiton columns should be key
    分区列只能是key列
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_column_name = ['k1', 'v1']
    partition_name_list = ['partition_a', 'partition_b', 'partition_c',
                           'partition_d', 'partition_e', 'partition_f']
    partition_value_list = [('-127', '2010-01-01'), ('1', '2010-01-02'), ('2', '2010-01-03'),
                            ('3', '2010-01-04'), ('4', '2010-01-01'), 'MAXVALUE']
    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_info = palo_client.PartitionInfo(partition_column_name,
                                               partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(k1)', random_bucket_num)
    try:
        client.create_table(table_name, DATA.schema_1,
                              partition_info, distribution_info)
        ret = True
    except Exception as e:
        print(str(e))
        ret = False
    assert not ret
    client.clean(database_name)


def test_partiton_multi_col_range_1():
    """
    {
    "title": "test_sys_partition_multi_col.test_partiton_multi_col_range_1",
    "describe": "test the range of partition column, 多列分区的范围",
    "tag": "function,p1"
    }
    """
    """
    test the range of partition column
    多列分区的范围
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c',
                           'partition_d', 'partition_e', 'partition_f']
    partition_value_list = [('-127', '2010-01-01 00:00:00'), ('1', '2010-01-02 00:00:00'), 
                            ('2', '2010-01-03 00:00:00'), 
                            ('3', '2010-01-04 00:00:00'), ('4', '2010-01-01 00:00:00'), 'MAXVALUE']
    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, ['k1', 'k5'],
                    partition_name_list, partition_value_list,
                    'HASH(k2)', random_bucket_num,)
    sql = 'select * from %s partition (%s)'
    ret = client.execute(sql % (table_name, partition_name_list[0]))
    assert len(ret) == 0
    sql = 'select * from %s partition (%s)'
    ret = client.execute(sql % (table_name, partition_name_list[1]))
    assert len(ret) == 0
    sql = 'select * from %s partition (%s)'
    ret = client.execute(sql % (table_name, partition_name_list[2]))
    assert len(ret) == 1
    sql = 'select * from %s partition (%s)'
    ret = client.execute(sql % (table_name, partition_name_list[3]))
    assert len(ret) == 1
    sql = 'select * from %s partition (%s)'
    ret = client.execute(sql % (table_name, partition_name_list[4]))
    assert len(ret) == 1
    sql = 'select * from %s partition (%s)'
    ret = client.execute(sql % (table_name, partition_name_list[5]))
    assert len(ret) == 27
    client.clean(database_name)


def test_partition_multi_col_range_default():
    """
    {
    "title": "test_sys_partition_multi_col.test_partition_multi_col_range_default",
    "describe": "test the default value of partiton value, 分区列的缺省值是minvalue",
    "tag": "function,p1"
    }
    """
    """
    test the default value of partiton value
    分区列的缺省值是minvalue
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_name_list = ['partition_a', 'partition_b', 'partition_c',
                           'partition_d', 'partition_e', 'partition_f']
    partition_value_list = [('-127', '-127'), ('-1', '-1'), ('0'), ('0', '0'), 
                            ('126', '126'), ('500')]

    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, ['k2', 'k1'],
                    partition_name_list, partition_value_list,
                    'HASH(k1)', random_bucket_num)
    client.clean(database_name)


def test_partition_multi_illegle_1():
    """
    {
    "title": "test_sys_partition_multi_col.test_partition_multi_illegle_1",
    "describe": "partition columns error, 不合法的分区",
    "tag": "function,p1,fuzz"
    }
    """
    """
    partition columns error
    不合法的分区
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_column_name = ['k1', 'k1']
    partition_name_list = ['partition_a', 'partition_b', 'partition_c',
                           'partition_d', 'partition_e', 'partition_f']
    partition_value_list = [('-127', '2010-01-01'), ('1', '2010-01-02'), ('2', '2010-01-03'),
                            ('3', '2010-01-04'), ('4', '2010-01-01'), 'MAXVALUE']
    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_info = palo_client.PartitionInfo(partition_column_name,
                                               partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(k1)', random_bucket_num)
    try:
        client.create_table(table_name, DATA.schema_1,
                              partition_info, distribution_info)
        ret = True
    except Exception as e:
        print(str(e))
        ret = False
    assert not ret
    client.clean(database_name)


def test_partition_multi_illegle_2():
    """
    {
    "title": "test_sys_partition_multi_col.test_partition_multi_illegle_2",
    "describe": "partition range insersection, 不合法的分区",
    "tag": "function,p1,fuzz"
    }
    """
    """
    partition range insersection
    不合法的分区
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_column_name = ['k1', 'k2']
    partition_name_list = ['partition_a', 'partition_b', 'partition_c',
                           'partition_d', 'partition_e', 'partition_f']
    partition_value_list = [('-127'), ('5', '0'), ('5', '100'),
                            ('5', '10'), ('5'), 'MAXVALUE']
    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_info = palo_client.PartitionInfo(partition_column_name,
                                               partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(k1)', random_bucket_num)
    try:
        client.create_table(table_name, DATA.schema_1,
                            partition_info, distribution_info)
        ret = True
    except Exception as e:
        print(str(e))
        ret = False
    assert not ret
    client.clean(database_name)


def test_partition_multi_illegle_3():
    """
    {
    "title": "test_sys_partition_multi_col.test_partition_multi_illegle_3",
    "describe": "partition range error, 不合法的分区,分区范围有相交",
    "tag": "function,p1,fuzz"
    }
    """
    """
    partition range error
    不合法的分区,分区范围有相交
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_column_name = ['k1', 'k2']
    partition_name_list = ['partition_a', 'partition_b', 'partition_c',
                           'partition_d', 'partition_e', 'partition_f']
    partition_value_list = [('-127'), ('5', '0'), ('5', '100'),
                            ('5', '300'), ('5'), 'MAXVALUE']
    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_info = palo_client.PartitionInfo(partition_column_name,
                                               partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(k1)', random_bucket_num)
    try:
        client.create_table(table_name, DATA.schema_1,
                              partition_info, distribution_info)
        ret = True
    except Exception as e:
        print(str(e))
        ret = False
    assert not ret
    client.clean(database_name)


def test_partition_add():
    """
    {
    "title": "test_sys_partition_multi_col.test_partition_add",
    "describe": "partitioned by multi columns and add partition, 多个分区列,增加分区",
    "tag": "function,p1,fuzz"
    }
    """
    """
    partitioned by multi columns and add partition 
    多个分区列,增加分区
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c',
                           'partition_d', 'partition_e', 'partition_f']
    partition_value_list = [('-127', '-127'), ('-1', '-1'), ('0', '-1'), 
                            ('0', '0'), ('30'), ('30', '500')]

    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, ['k1', 'k2'],
                    partition_name_list, partition_value_list,
                    'HASH(k1)', random_bucket_num)
    add_partition_name = 'partition_add'
    add_partition_value = ('30', '1000')
    ret = client.add_partition(table_name, add_partition_name, add_partition_value,
                               'hash(k1)', 5)
    assert ret
    assert client.get_partition(table_name, add_partition_name)
    add_partition_name = 'add_partition_wrong'
    add_partition_value = ('30', '800')
    ret = None
    try:
        client.add_partition(table_name, add_partition_name, add_partition_value,
                               'hash(k1)', 5)
        ret = True
    except Exception as e:
        print(str(e))
        ret = False
    assert not ret
    client.clean(database_name)


def test_partition_drop():
    """
    {
    "title": "test_sys_partition_multi_col.test_partition_drop",
    "describe": " partitioned by multi columns and drop partition column, 多个分区列,删除分区",
    "tag": "function,p1"
    }
    """
    """
    partitioned by multi columns and drop partition column
    多个分区列,删除分区
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c',
                           'partition_d', 'partition_e', 'partition_f']
    partition_value_list = [('-127', '-127'), ('-1', '-1'), ('0', '-1'), 
                            ('0', '0'), ('30'), ('30', '500')]

    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, ['k1', 'k2'],
                    partition_name_list, partition_value_list,
                    'HASH(k1)', random_bucket_num)
    drop_partition_name = 'partition_d'
    ret = client.drop_partition(table_name, drop_partition_name)
    assert ret
    assert not client.get_partition(table_name, drop_partition_name)
    add_partition_name = 'partition_add'
    add_partition_value = ('0', '0')
    ret = client.add_partition(table_name, add_partition_name, add_partition_value,
                               'hash(k1)', 5)
    assert ret
    assert client.get_partition(table_name, add_partition_name)
    client.clean(database_name)


def test_partition_null():
    """
    {
    "title": "test_sys_partition_multi_col.test_partition_null",
    "describe": "null value in the lowest partition, if drop the partition null is deleted. 测试null值的分区",
    "tag": "function,p1,fuzz"
    }
    """
    """
    null value in the lowest partition, if drop the partition null is deleted.
    测试null值的分区
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c',
                           'partition_d', 'partition_e', 'partition_f']
    partition_value_list = [('-127', '-127'), ('-1', '-1'), ('0'), 
                            ('0', '0'), ('126', '126'), ('500')]

    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, ['k2', 'k1'],
                    partition_name_list, partition_value_list,
                    'HASH(k3)', random_bucket_num)
    isql = 'insert into %s values(0, NULL, 0, 0, "2000-01-01 00:00:00", "2000-01-01", ' \
           '"a", "a", 0.001, -0.001, 0.001)' % table_name
    ret = client.execute(isql)
    assert ret == ()
    qsql = 'select k1 from %s partition(%s) where k2 is null' % (table_name, 
                                                                 partition_name_list[0])
    ret = client.execute(qsql)
    print(ret)
    assert ret == ((0,),)
    print(client.execute('show partitions from %s' % table_name))
    drop_partition_name = 'partition_a'
    ret = client.drop_partition(table_name, drop_partition_name)
    assert ret
    qsql = 'select k1 from %s where k2 is null' % (table_name)
    ret = client.execute(qsql)
    assert ret == ()
    try:
        ret = client.execute(isql)
        assert 0 == 1, 'expect insert error'
    except Exception as e:
        LOG.info(L('insert ret', msg=str(e)))
        print(str(e))
    ret = client.execute(qsql)
    assert ret == ()
    client.clean(database_name)


def test_partition_add_column():
    """
    {
    "title": "test_sys_partition_multi_col.test_partition_add_column",
    "describe": "partition columns is not adjacent, 分区列之间增加key列",
    "tag": "function,p1"
    }
    """
    """
    partition columns is not adjacent
    分区列之间增加key列
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c',
                           'partition_d', 'partition_e', 'partition_f']
    partition_value_list = [('-127', '-127'), ('-1', '-1'), ('0'), 
                            ('0', '0'), ('126', '126'), ('500')]

    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, ['k2', 'k1'],
                    partition_name_list, partition_value_list,
                    'HASH(k3)', random_bucket_num)
    add_column = [('add_key', 'int', '', '0')]
    ret = client.schema_change_add_column(table_name, add_column, after_column_name='k1', 
                                          is_wait_job=True)
    assert ret
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k5', 'v1', 'v2', 'v3', 'v4', 'v5', 'v6']
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name, 
                                              column_name_list=column_name_list)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    client.clean(database_name)


def test_partition_drop_column():
    """
    {
    "title": "test_sys_partition_multi_col.test_partition_drop_column",
    "describe": "can not drop partition column, 多列分区，删除列，不能删除分区列",
    "tag": "function,p1,fuzz"
    }
    """
    """
    can not drop partition column
    多列分区，删除列，不能删除分区列
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c',
                           'partition_d', 'partition_e', 'partition_f']
    partition_value_list = [('-127', '-127'), ('-1', '-1'), ('0'), 
                            ('0', '0'), ('126', '126'), ('500')]

    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, ['k2', 'k1'],
                    partition_name_list, partition_value_list,
                    'HASH(k3)', random_bucket_num)
    drop_column = ['k1']
    ret = None
    try:
        client.schema_change_drop_column(table_name, drop_column, is_wait_job=True)
        ret = False
    except Exception as e:
        print(str(e))
        ret = True
    assert ret
    drop_column = ['v1']
    ret = client.schema_change_drop_column(table_name, drop_column, is_wait_job=True)
    assert ret
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k5', 'v1', 'v2', 'v3', 'v4', 'v5', 'v6']
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name, 
                                              column_name_list=column_name_list)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    client.clean(database_name)


def test_partition_delete():
    """
    {
    "title": "test_sys_partition_multi_col.test_partition_delete",
    "describe": "test partition delete, 多列分区，删除分区中的数据",
    "tag": "function,p1"
    }
    """
    """
    test partition delete
    多列分区，删除分区中的数据
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c',
                           'partition_d', 'partition_e', 'partition_f']
    partition_value_list = [('-127', '-127'), ('-1', '-1'), ('10'), 
                            ('10', '2'), ('126', '126'), ('500')]

    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, ['k2', 'k1'],
                    partition_name_list, partition_value_list,
                    'HASH(k3)', random_bucket_num)
    delete_condition = [('k2', '=', '10'), ('k1', '<', '10')]
    ret = client.delete(table_name, delete_condition, 'partition_d')
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k5', 'v1', 'v2', 'v3', 'v4', 'v5', 'v6']
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name, 
                                              column_name_list=column_name_list)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    sql = 'select * from %s partition(%s)' % (table_name, 'partition_d')
    ret = client.execute(sql)
    assert len(ret) == 1
    client.clean(database_name)


def test_partition_rollup():
    """
    {
    "title": "test_sys_partition_multi_col.test_partition_rollup",
    "describe": "test partition rollup, 多列分区，创建rollup",
    "tag": "function,p1"
    }
    """
    """
    test partition rollup
    多列分区，创建rollup
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c',
                           'partition_d', 'partition_e', 'partition_f']
    partition_value_list = [('-127', '-127'), ('-1', '-1'), ('10'), 
                            ('10', '1'), ('126', '126'), ('500')]

    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, ['k2', 'k1'],
                    partition_name_list, partition_value_list,
                    'HASH(k3)', random_bucket_num)
    column_list = ['k1', 'k3', 'k4', 'v5']
    ret = client.create_rollup_table(table_name, index_name, column_list, is_wait=True)
    assert ret
    assert client.get_index(table_name, index_name)
    client.clean(database_name)


def test_partition_drop_rollup():
    """
    {
    "title": "test_sys_partition_multi_col.test_partition_drop_rollup",
    "describe": "test partiton drop rollup, 多列分区，删除rollup",
    "tag": "function,p1"
    }
    """
    """
    test partiton drop rollup
    多列分区，删除rollup
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c',
                           'partition_d', 'partition_e', 'partition_f']
    partition_value_list = [('-127', '-127'), ('-1', '-1'), ('10'), 
                            ('10', '1'), ('126', '126'), ('500')]

    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, ['k2', 'k1'],
                    partition_name_list, partition_value_list,
                    'HASH(k3)', random_bucket_num)
    column_list = ['k1', 'k2', 'v4', 'v5']
    ret = client.create_rollup_table(table_name, index_name, column_list, is_wait=True)
    assert ret
    assert client.get_index(table_name, index_name)
    client.drop_rollup_table(table_name, index_name)
    assert not client.get_index(table_name, index_name)
    client.clean(database_name)


def test_partition_modify():
    """
    {
    "title": "test_sys_partition_multi_col.test_partition_modify",
    "describe": "test partition modify, 修改分区列类型",
    "tag": "function,p1"
    }
    """
    """
    test partition modify
    修改分区列类型
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c',
                           'partition_d', 'partition_e', 'partition_f']
    partition_value_list = [('-127', '-127'), ('-1', '-1'), ('10'), 
                            ('10', '1'), ('126', '126'), ('500')]

    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, ['k2', 'k1'],
                    partition_name_list, partition_value_list,
                    'HASH(k3)', random_bucket_num)
    ret = client.modify_partition(table_name, 'partition_a', replication_num=1)
    assert ret
    num = client.get_partition_replication_num(table_name, 'partition_a')
    print('replication num is ' + num)
    assert int(num) == 1
    client.clean(database_name)


def test_partition_fixed_range():
    """
    {
    "title": "test_sys_partition_multi_col.test_partition_fixed_range",
    "describe": "create table with range partition, 使用区间的方式定义分区表",
    "tag": "function,p1"
    }
    """
    """
    create table with range partition
    使用区间的方式定义分区表
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c']
    partition_value_list = [(('-127', '-127'), ('10', '-1')), (('10', '-1'), ('40', '0')), 
                            (('126', '126'), ('127', ))]

    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, ['k1', 'k2'],
                    partition_name_list, partition_value_list,
                    'HASH(k1)', random_bucket_num,)
    client.clean(database_name)


def test_partition_fixed_range_1():
    """
    {
    "title": "test_sys_partition_multi_col.test_partition_fixed_range_1",
    "describe": "partition range intersection, 区间有重叠",
    "tag": "function,p1"
    }
    """
    """
    partition range intersection
    区间有重叠
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c']
    partition_value_list = [(('-127', '-127'), ('10', '10')), (('10', '100'), ('40', '0')), 
                            (('126', '126'), ('127',))]

    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, ['k1', 'k2'],
                    partition_name_list, partition_value_list,
                    'HASH(k2)', random_bucket_num,)
    client.clean(database_name)


def test_partition_fixed_range_2():
    """
    {
    "title": "test_sys_partition_multi_col.test_partition_fixed_range_2",
    "describe": "partiton has hole, 区间不连续",
    "tag": "function,p1"
    }
    """
    """
    partiton has hole
    区间不连续
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c']
    partition_value_list = [(('-127', '-127'), ('-1', '-1')), (('0', '-1'), ('40', '0')), 
                            (('126', '126'), ('127',))]

    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, ['k1', 'k2'],
                    partition_name_list, partition_value_list,
                    'HASH(k1)', random_bucket_num,)
    client.clean(database_name)


def test_partition_fixed_range_3():
    """
    {
    "title": "test_sys_partition_multi_col.test_partition_fixed_range_3",
    "describe": "partition range maxvalue and min value, 区间定义的最大最小值",
    "tag": "function,p1"
    }
    """
    """
    partition range maxvalue and min value
    区间定义的最大最小值
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c']
    partition_value_list = [(('-127', '-127'), ('-1', '-1')), (('0', '-1'), ('40', '0')), 
                            (('126', '126'), ('127',))]

    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, ['k1', 'k2'],
                    partition_name_list, partition_value_list,
                    'HASH(k3)', random_bucket_num,)
    client.clean(database_name)


def test_fixed_range_add_partition():
    """
    {
    "title": "test_sys_partition_multi_col.test_fixed_range_add_partition",
    "describe": "add partition with range, alter增加区间的分区",
    "tag": "function,p1,fuzz"
    }
    """
    """
    add partition with range
    alter增加区间的分区
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c']
    partition_value_list = [(('-127', '-127'), ('-1', '-1')), (('0', '-1'), ('40', '0')), 
                            (('126', '126'), ('127',))]

    random_bucket_num = random.randrange(1, 300)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_check(table_name, ['k1', 'k2'],
                    partition_name_list, partition_value_list,
                    'RANDOM', random_bucket_num,)
    
    add_partition_name = 'partition_add'
    add_partition_value = ('50', '1000')
    ret = client.add_partition(table_name, add_partition_name, add_partition_value,
                               'hash(k1)', 5)
    assert ret
    assert client.get_partition(table_name, add_partition_name)
    add_partition_name = 'add_partition_wrong'
    add_partition_value = ('50', '800')
    ret = None
    try:
        client.add_partition(table_name, add_partition_name, add_partition_value,
                               'hash(k1)', 5)
        ret = True
    except Exception as e:
        print(str(e))
        ret = False
    assert not ret
    client.clean(database_name)


def teardown_module():
    """tear down"""
    pass


if __name__ == '__main__':
    setup_module()
    test_partition_fixed_range_1()
