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
#   @file test_sys_load_parse_from_path.py
#   @date 2019-09-10 16:02:05
#   @brief This file is a test file for broker load parse columns from file path
#
#############################################################################

"""
测试broker load，从文件路径中解析数据
"""

import os
import sys
file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(file_dir)
file_dir = os.path.abspath(os.path.dirname(__file__))
from data import schema as DATA
from data import load_file as FILE
from lib import palo_config
from lib import palo_client
from lib import util

client = None
LOG = palo_client.LOG
L = palo_client.L

config = palo_config.config
broker_info = palo_config.broker_info


def setup_module():
    """
    setUp
    """
    global client
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password)
    client.init()


def init_check_table(db_name, check_table='check_tb'):
    """init check table for data verify"""
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', partition_name_list,
                                               partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(k2)', 5)
    ret = client.create_table(check_table, DATA.partition_column_list, partition_info,
                              distribution_info, keys_desc='AGGREGATE KEY (k1, k2, k3, k4, k5)', 
                              database_name=db_name, set_null=True)
    assert ret
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k5', 'v1', 'v2', 'v3', 'v4', 'v5', 'v6']
    data_desc = palo_client.LoadDataInfo(FILE.parse_hdfs_file_path_normal, check_table,
                                         column_name_list=column_name_list)
    ret = client.batch_load(util.get_label(), data_desc, is_wait=True, broker=broker_info,
                            database_name=db_name)
    assert ret


def check2(sql1, sql2):
    """check select result"""
    ret1 = client.execute(sql1)
    ret2 = client.execute(sql2)
    LOG.info(L('sql', sql=sql1))
    LOG.info(L('check sql', check_sql=sql2))
    util.check(ret1, ret2)


def test_parse_key_from_path():
    """
    {
    "title": "test_sys_load_parse_from_path.test_parse_key_from_path",
    "describe": "parse partition key from file path, 从路径中解析key列，分区列",
    "tag": "system,p1"
    }
    """
    """
    parse partition key from file path
    从路径中解析key列，分区列
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', partition_name_list,
                                               partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(k2)', 5)
    ret = client.create_table(table_name, DATA.partition_column_list, partition_info,
                              distribution_info, keys_desc='AGGREGATE KEY (k1, k2, k3, k4, k5)')
    assert ret
    column_name_list = ['tmp_k1', 'tmp_k2', 'k3', 'k4', 'k5', 'v1', 'v2', 'v3', 'v4', 'v5', 'v6']
    column_from_path = ['k1', 'k2', 'city']
    data_desc = palo_client.LoadDataInfo(FILE.parse_hdfs_file_path_normal, table_name, 
                                         column_name_list=column_name_list, columns_from_path=column_from_path)
    ret = client.batch_load(util.get_label(), data_desc, is_wait=True, broker=broker_info)
    assert ret
    check_table = 'check_tb'
    init_check_table(database_name)
    sql1 = 'select * from %s order by 1, 2, 3' % table_name
    sql2 = 'select -1, 0, k3, k4, k5, v1, v2, v3, v4, v5, v6 from %s order by k3' % check_table
    check2(sql1, sql2)
    client.clean(database_name)
    

def test_parse_from_path_set():
    """
    {
    "title": "test_sys_load_parse_from_path.test_parse_from_path_set",
    "describe": "parse value column from file path and set, 从路径中解析value列, 使用路径中解析的列进行set函数",
    "tag": "system,p1"
    }
    """
    """
    parse value column from file path and set
    从路径中解析value列
    使用路径中解析的列进行set函数
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', partition_name_list,
                                               partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(k2)', 5)
    ret = client.create_table(table_name, DATA.partition_column_list, partition_info,
                              distribution_info, keys_desc='AGGREGATE KEY (k1, k2, k3, k4, k5)')
    assert ret
    column_name_list = ['tmp_k1', 'tmp_k2', 'k3', 'k4', 'k5', 'v1', 'v2', 'tmp_v3', 'v4', 'v5', 'v6']
    column_from_path = ['k1', 'k2', 'city']
    set_list = ['v3=city']
    data_desc = palo_client.LoadDataInfo(FILE.parse_hdfs_file_path_normal, table_name, set_list=set_list,
                                         column_name_list=column_name_list, columns_from_path=column_from_path)
    ret = client.batch_load(util.get_label(), data_desc, is_wait=True, broker=broker_info)
    assert ret
    check_table = 'check_tb'
    init_check_table(database_name)
    sql1 = 'select * from %s order by 1, 2, 3' % table_name
    sql2 = 'select -1, 0, k3, k4, k5, v1, v2, "bj", v4, v5, v6 from %s order by k3' % check_table
    check2(sql1, sql2)
    client.clean(database_name)


def test_parse_special_from_path():
    """
    {
    "title": "test_sys_load_parse_from_path.test_parse_special_from_path",
    "describe": "parse special char empty from path to char column, 从路径中解析空",
    "tag": "system,p1,fuzz"
    }
    """
    """
    parse special char empty from path to char column
    从路径中解析空
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', partition_name_list,
                                               partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(k2)', 5)
    ret = client.create_table(table_name, DATA.partition_column_list, partition_info,
                              distribution_info, keys_desc='AGGREGATE KEY (k1, k2, k3, k4, k5)')
    assert ret
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k5', 'v1', 'v2', 'tmp_v3', 'v4', 'v5', 'v6']
    column_from_path = ['city']
    set_list = ['v3=city']
    data_desc = palo_client.LoadDataInfo(FILE.parse_hdfs_file_path_empty, table_name, set_list=set_list,
                                         column_name_list=column_name_list, columns_from_path=column_from_path)
    ret = client.batch_load(util.get_label(), data_desc, is_wait=True, broker=broker_info)
    assert ret
    check_table = 'check_tb'
    init_check_table(database_name)
    sql1 = 'select * from %s order by 1, 2, 3' % table_name
    sql2 = 'select k1, k2, k3, k4, k5, v1, v2, "", v4, v5, v6 from %s order by k3' % check_table
    check2(sql1, sql2)
    client.clean(database_name)


def test_parse_special_from_path_1():
    """
    {
    "title": "test_sys_load_parse_from_path.test_parse_special_from_path_1",
    "describe": "parse special char empty from path to int column, 从路径中解析空到数字列，报错",
    "tag": "system,p1,fuzz"
    }
    """
    """
    parse special char empty from path to int column
    从路径中解析空到数字列，报错
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', partition_name_list,
                                               partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(k2)', 5)
    ret = client.create_table(table_name, DATA.partition_column_list, partition_info,
                              distribution_info, keys_desc='AGGREGATE KEY (k1, k2, k3, k4, k5)',
                              set_null=True)
    assert ret
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k5', 'v1', 'v2', 'v3', 'v4', 'v5', 'v6']
    column_from_path = ['city']
    set_list = ['k1=city']
    data_desc = palo_client.LoadDataInfo(FILE.parse_hdfs_file_path_empty, table_name, set_list=set_list,
                                         column_name_list=column_name_list, columns_from_path=column_from_path)
    ret = client.batch_load(util.get_label(), data_desc, is_wait=True, broker=broker_info)
    assert ret
    check_table = 'check_tb'
    init_check_table(database_name)
    sql1 = 'select * from %s order by 1, 2, 3' % table_name
    sql2 = 'select NULL, k2, k3, k4, k5, v1, v2, v3, v4, v5, v6 from %s order by k3' % check_table
    check2(sql1, sql2)
    client.clean(database_name)


def test_parse_column_wrong():
    """
    {
    "title": "test_sys_load_parse_from_path.test_parse_column_wrong",
    "describe": "parse duplicate column from load file, 重复列报错",
    "tag": "system,p1,fuzz"
    }
    """
    """
    parse duplicate column from load file
    重复列报错
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', partition_name_list,
                                               partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(k2)', 5)
    ret = client.create_table(table_name, DATA.partition_column_list, partition_info,
                              distribution_info, keys_desc='AGGREGATE KEY (k1, k2, k3, k4, k5)')
    assert ret
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k5', 'v1', 'v2', 'v3', 'v4', 'v5', 'v6']
    column_from_path = ['k1', 'k2', 'city']
    data_desc = palo_client.LoadDataInfo(FILE.parse_hdfs_file_path_normal, table_name,
                                         column_name_list=column_name_list, columns_from_path=column_from_path)

    try:
        ret = client.batch_load(util.get_label(), data_desc, is_wait=True, broker=broker_info)
        assert 0 == 1
    except Exception as e:
        print(str(e))
        assert 'Duplicate column' in str(e)
    client.clean(database_name)


def test_parse_column_wrong_1():
    """
    {
    "title": "test_sys_load_parse_from_path.test_parse_column_wrong_1",
    "describe": "parse a not exist column from file path, 从路径中解析个不存在的列，失败",
    "tag": "system,p1,fuzz"
    }
    """
    """
    parse a not exist column from file path
    从路径中解析个不存在的列，失败
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', partition_name_list,
                                               partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(k2)', 5)
    ret = client.create_table(table_name, DATA.partition_column_list, partition_info,
                              distribution_info, keys_desc='AGGREGATE KEY (k1, k2, k3, k4, k5)',
                              set_null=True)
    assert ret
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'tmp_k5', 'v1', 'v2', 'v3', 'v4', 'v5', 'v6']
    column_from_path = ['k5']
    data_desc = palo_client.LoadDataInfo(FILE.parse_hdfs_file_path_empty, table_name,
                                         column_name_list=column_name_list,
                                         columns_from_path=column_from_path)
    ret = client.batch_load(util.get_label(), data_desc, is_wait=True, broker=broker_info)
    assert not ret
    client.clean(database_name)


def test_parse_column_filtered():
    """
    {
    "title": "test_sys_load_parse_from_path.test_parse_column_filtered",
    "describe": "parse a column to a not exist partition, 解析的列不存在相应的分区，被过滤",
    "tag": "system,p1,fuzz"
    }
    """
    """
    parse a column to a not exist partition
    解析的列不存在相应的分区，被过滤
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['-50', '-10', '-5', '-2']
    partition_info = palo_client.PartitionInfo('k1', partition_name_list,
                                               partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(k2)', 5)
    ret = client.create_table(table_name, DATA.partition_column_list, partition_info,
                              distribution_info, keys_desc='AGGREGATE KEY (k1, k2, k3, k4, k5)',
                              set_null=True)
    assert ret
    column_name_list = ['tmp_k1', 'k2', 'k3', 'k4', 'k5', 'v1', 'v2', 'v3', 'v4', 'v5', 'v6']
    column_from_path = ['k1', 'city']
    data_desc = palo_client.LoadDataInfo(FILE.parse_hdfs_file_path_empty, table_name,
                                         column_name_list=column_name_list, columns_from_path=column_from_path)
    ret = client.batch_load(util.get_label(), data_desc, is_wait=True, broker=broker_info)
    assert not ret
    client.clean(database_name)


def test_parse_wrong_type():
    """
    {
    "title": "test_sys_load_parse_from_path.test_parse_wrong_type",
    "describe": " test parse wrong type from file path, 解析出来的数据类型不符合，由strict mode参数控制是否导入成功，默认strict mode为FALSE",
    "tag": "system,p1,fuzz"
    }
    """
    """
    test parse wrong type from file path
    解析出来的数据类型不符合，由strict mode参数控制是否导入成功，默认strict mode为FALSE
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', partition_name_list,
                                               partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(k2)', 5)
    ret = client.create_table(table_name, DATA.partition_column_list, partition_info,
                              distribution_info, keys_desc='AGGREGATE KEY (k1, k2, k3, k4, k5)',
                              set_null=True)
    assert ret
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'tmp_k5', 'v1', 'v2', 'v3', 'v4', 'v5', 'v6']
    column_from_path = ['k5']
    data_desc = palo_client.LoadDataInfo(FILE.parse_hdfs_file_path_float, table_name, 
                                         column_name_list=column_name_list, 
                                         columns_from_path=column_from_path)
    ret = client.batch_load(util.get_label(), data_desc, is_wait=True, broker=broker_info, strict_mode=True)
    assert not ret
    ret = client.batch_load(util.get_label(), data_desc, is_wait=True, broker=broker_info)
    assert ret
    client.clean(database_name)


def test_parse_float():
    """
    {
    "title": "test_sys_load_parse_from_path.test_parse_float",
    "describe": "parse a float from file path, 解析小数类型",
    "tag": "system,p1"
    }
    """
    """
    parse a float from file path
    解析小数类型
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '30', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('k1', partition_name_list,
                                               partition_value_list)
    distribution_info = palo_client.DistributionInfo('HASH(k2)', 5)
    ret = client.create_table(table_name, DATA.partition_column_list_parse, partition_info,
                              distribution_info, keys_desc='AGGREGATE KEY (k1, k2, k3, k4, k5_t)',
                              set_null=True)
    assert ret
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'tmp_k5', 'v1', 'v2', 'v3', 'v4', 'v5', 'tmp_v6']
    column_from_path = ['k5']
    set_list = ['k5_t=tmp_k5, v6=k5']
    data_desc = palo_client.LoadDataInfo(FILE.parse_hdfs_file_path_float, table_name,
                                         column_name_list=column_name_list, set_list=set_list,
                                         columns_from_path=column_from_path)
    ret = client.batch_load(util.get_label(), data_desc, is_wait=True, broker=broker_info)
    assert ret
    check_table = 'check_tb'
    init_check_table(database_name)
    sql1 = 'select * from %s order by 1, 2, 3' % table_name
    sql2 = 'select k1, k2, k3, k4, k5, v1, v2, v3, v4, v5, 100.12345 from %s order by k3' % check_table
    check2(sql1, sql2)
    client.clean(database_name)
    

def teardown_module():
    """teardown"""
    pass


if __name__ == '__main__':
    setup_module()
    test_parse_float()
