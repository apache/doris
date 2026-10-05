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
#   @file test_sys_modify_partition_property.py
#   @date 2020-07-29 15:07:59
#   @brief This file is a test file for modify partitions property
#
#############################################################################

"""
测试修改分区表多分区属性
"""

import pytest

from data import partition as DATA
from lib import palo_config
from lib import palo_client
from lib import util
from lib import common
from lib.palo_job import PartitionInfo
from lib.palo_job import DescInfo

client = None

config = palo_config.config
broker_info = palo_config.broker_info


def setup_module():
    """
    setUp
    """
    pass


def test_modify_table_replication():
    """
    {
    "title": "test_modify_table_replication",
    "describe": "修改表属性，set replication_num错误，使用default.replication_num",
    "tag": "p1,system"
    }
    """
    """增加表的副本数量"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    partition_a = palo_client.Partition('partition_a', '100', storage_medium='SSD')
    partition_b = palo_client.Partition('partition_b', '200', storage_medium='SSD')
    partition_c = palo_client.Partition('partition_c', '300', storage_medium='SSD')
    partition_d = palo_client.Partition('partition_d', 'MAXVALUE', storage_medium='SSD')
    partition_list = [partition_a, partition_b, partition_c, partition_d]
    partition_info = palo_client.PartitionInfo('k3', partition_list=partition_list)
    distribution_info = palo_client.DistributionInfo('Hash(k1)', 3)

    assert client.create_table(table_name, DATA.schema_1, partition_info, distribution_info)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_partition_storage_medium(table_name, 'partition_a') == 'SSD'
    assert client.get_partition_storage_medium(table_name, 'partition_b') == 'SSD'
    assert client.get_partition_storage_medium(table_name, 'partition_c') == 'SSD'
    assert client.get_partition_storage_medium(table_name, 'partition_d') == 'SSD'

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    ret = client.show_tablet(table_name)
    flag = True
    try:
        ret = client.modify_partition(table_name, replication_num=2)
        flag = False
    except Exception as e:
        # This is a range partitioned table, you should specify partitions with MODIFY PARTITION clause. 
        # If you want to set default replication number, please use 'default.replication_num' instead of 'replication_num' to escape misleading.
        print(str(e))
    assert flag, 'expect modfiy partition failed'
    client.clean(database_name)


def test_modify_tb_reduce_replication():
    """
    {
    "title": "test_modify_tb_reduce_replication",
    "describe": "修改表属性，将replication_num由3变为2，show partition查看RelicationNum为不变仍为3，增加新的分区副本数为2",
    "tag": "p1,system"
    }
    """
    """减少表的副本数量，原有分区的副本数不变，新加分区的副本数使用修改后的副本数"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    partition_a = palo_client.Partition('partition_a', '100', storage_medium='SSD')
    partition_b = palo_client.Partition('partition_b', '200', storage_medium='SSD')
    partition_c = palo_client.Partition('partition_c', '300', storage_medium='SSD')
    partition_d = palo_client.Partition('partition_d', '5000', storage_medium='SSD')
    partition_list = [partition_a, partition_b, partition_c, partition_d]
    partition_info = palo_client.PartitionInfo('k3', partition_list=partition_list)
    distribution_info = palo_client.DistributionInfo('Hash(k1)', 3)

    assert client.create_table(table_name, DATA.schema_1, partition_info, distribution_info)
    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    ret = client.show_partitions(table_name)
    ret = client.modify_partition(table_name, None, None, None, None, None, **{"default.replication_num": "2"})
    assert ret
    ret = client.show_partitions(table_name)
    part_replication_list = util.get_attr(ret, PartitionInfo.ReplicationNum)
    print(part_replication_list)
    assert part_replication_list == [u'3'] * len(part_replication_list)
    assert '3' == util.get_attr_condition_value(ret, PartitionInfo.PartitionName, 
                                                'partition_a', PartitionInfo.ReplicationNum)
    ret = client.add_partition(table_name, 'partition_e', '10000')
    assert ret
    ret = client.show_partitions(table_name)
    assert '2' == util.get_attr_condition_value(ret, PartitionInfo.PartitionName, 
                                                'partition_e', PartitionInfo.ReplicationNum)
    part_replication_list = util.get_attr(ret, PartitionInfo.ReplicationNum)
    print(part_replication_list)
    print([u'3'] * len(part_replication_list) + [u'2'])
    assert part_replication_list == [u'3'] * (len(part_replication_list) - 1) + [u'2']
    client.clean(database_name)


def test_modify_tb_add_replication():
    """
    {
    "title": "test_modify_tb_add_replication",
    "describe": "修改表属性replication_num，将由3变为4，show partition查看RelicationNum为不变仍为3，增加新的分区副本数为4",
    "tag": "p1,system"
    }
    """
    """增加表的副本数量，原有分区的副本数不变，新加分区的副本数使用修改后的副本数"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    partition_a = palo_client.Partition('partition_a', '100', storage_medium='SSD')
    partition_b = palo_client.Partition('partition_b', '200', storage_medium='SSD')
    partition_c = palo_client.Partition('partition_c', '300', storage_medium='SSD')
    partition_d = palo_client.Partition('partition_d', '5000', storage_medium='SSD')
    partition_list = [partition_a, partition_b, partition_c, partition_d]
    partition_info = palo_client.PartitionInfo('k3', partition_list=partition_list)
    distribution_info = palo_client.DistributionInfo('Hash(k1)', 3)

    assert client.create_table(table_name, DATA.schema_1, partition_info, distribution_info)
    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    ret = client.show_partitions(table_name)
    ret = client.modify_partition(table_name, None, None, None, None, None, **{"default.replication_num": "4"})
    assert ret
    ret = client.show_partitions(table_name)
    part_replication_list = util.get_attr(ret, PartitionInfo.ReplicationNum)
    print(part_replication_list)
    assert part_replication_list == [u'3'] * len(part_replication_list)
    assert '3' == util.get_attr_condition_value(ret, PartitionInfo.PartitionName,
                                                'partition_a', PartitionInfo.ReplicationNum)
    ret = client.add_partition(table_name, 'partition_e', '10000')
    assert ret
    ret = client.show_partitions(table_name)
    assert '4' == util.get_attr_condition_value(ret, PartitionInfo.PartitionName, 
                                                'partition_e', PartitionInfo.ReplicationNum)
    part_replication_list = util.get_attr(ret, PartitionInfo.ReplicationNum)
    print(part_replication_list)
    print([u'3'] * len(part_replication_list) + [u'4'])
    assert part_replication_list == [u'3'] * (len(part_replication_list) - 1) + [u'4']
    client.clean(database_name)


def test_modify_tb_bloom_filter():
    """
    {
    "title": "test_modify_tb_bloom_filter",
    "describe": "修改表属性bloom filter，show alter table查看alter任务状态，finished后desc查看bloom filter列验证",
    "tag": "p1,system"
    }
    """
    """修改表的bloom filter"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    partition_a = palo_client.Partition('partition_a', '100', storage_medium='SSD')
    partition_b = palo_client.Partition('partition_b', '200', storage_medium='SSD')
    partition_c = palo_client.Partition('partition_c', '300', storage_medium='SSD')
    partition_d = palo_client.Partition('partition_d', 'MAXVALUE', storage_medium='SSD')
    partition_list = [partition_a, partition_b, partition_c, partition_d]
    partition_info = palo_client.PartitionInfo('k3', partition_list=partition_list)
    distribution_info = palo_client.DistributionInfo('Hash(k1)', 3)

    assert client.create_table(table_name, DATA.schema_1, partition_info, distribution_info)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_partition_storage_medium(table_name, 'partition_a') == 'SSD'
    assert client.get_partition_storage_medium(table_name, 'partition_b') == 'SSD'
    assert client.get_partition_storage_medium(table_name, 'partition_c') == 'SSD'
    assert client.get_partition_storage_medium(table_name, 'partition_d') == 'SSD'

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    ret = client.desc_table(table_name)
    print(util.get_attr_condition_list(ret, DescInfo.Extra, 'BLOOM_FILTER', DescInfo.Field))
    assert util.get_attr_condition_list(ret, DescInfo.Extra, 'BLOOM_FILTER', DescInfo.Field) is None
    # todo check show tablet_
    bloom_filter_columns_list = ['k2', 'k4']
    ret = client.modify_partition(table_name, bloom_filter_columns=','.join(bloom_filter_columns_list))
    assert ret
    assert client.wait_table_schema_change_job(table_name)
    ret = client.desc_table(table_name)
    assert bloom_filter_columns_list == util.get_attr_condition_list(ret, DescInfo.Extra, 
                                                                     'BLOOM_FILTER', DescInfo.Field) 
    client.clean(database_name)


def test_modify_table_storage_medium():
    """
    {
    "title": "test_modify_table_storage_medium",
    "describe": "修改表的storage_medium，storage_medium和storage_cooldown_time不是表属性，不支持修改",
    "tag": "p1,system"
    }
    """
    """表属性不支持storage medium"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    partition_a = palo_client.Partition('partition_a', '100', storage_medium='SSD')
    partition_b = palo_client.Partition('partition_b', '200', storage_medium='SSD')
    partition_c = palo_client.Partition('partition_c', '300', storage_medium='SSD')
    partition_d = palo_client.Partition('partition_d', 'MAXVALUE', storage_medium='SSD')
    partition_list = [partition_a, partition_b, partition_c, partition_d]
    partition_info = palo_client.PartitionInfo('k3', partition_list=partition_list)
    distribution_info = palo_client.DistributionInfo('Hash(k1)', 3)

    assert client.create_table(table_name, DATA.schema_1, partition_info, distribution_info)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_partition_storage_medium(table_name, 'partition_a') == 'SSD'
    assert client.get_partition_storage_medium(table_name, 'partition_b') == 'SSD'
    assert client.get_partition_storage_medium(table_name, 'partition_c') == 'SSD'
    assert client.get_partition_storage_medium(table_name, 'partition_d') == 'SSD'

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    ret = client.show_tablet(table_name)
    flag = True
    try:
        ret = client.modify_partition(table_name, storage_medium='ssd')
        flag = False
    except Exception as e:
        print(str(e))
    assert flag, 'expect: Unknown table property: [storage_medium]'
    client.clean(database_name)


def test_modify_table_colocate():
    """
    {
    "title": "test_modify_table_colocate",
    "describe": "修改表属性colocate_with，修改成功",
    "tag": "p1,system"
    }
    """
    """修改表的colocate_with属性"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    partition_a = palo_client.Partition('partition_a', '100', storage_medium='SSD')
    partition_b = palo_client.Partition('partition_b', '200', storage_medium='SSD')
    partition_c = palo_client.Partition('partition_c', '300', storage_medium='SSD')
    partition_d = palo_client.Partition('partition_d', 'MAXVALUE', storage_medium='SSD')
    partition_list = [partition_a, partition_b, partition_c, partition_d]
    partition_info = palo_client.PartitionInfo('k3', partition_list=partition_list)
    distribution_info = palo_client.DistributionInfo('Hash(k1)', 3)

    assert client.create_table(table_name, DATA.schema_1, partition_info, distribution_info)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_partition_storage_medium(table_name, 'partition_a') == 'SSD'
    assert client.get_partition_storage_medium(table_name, 'partition_b') == 'SSD'
    assert client.get_partition_storage_medium(table_name, 'partition_c') == 'SSD'
    assert client.get_partition_storage_medium(table_name, 'partition_d') == 'SSD'

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    ret = client.modify_partition(table_name, colocate_with='k2')
    assert ret
    client.clean(database_name)


@pytest.mark.skip()
def test_modify_table_dynamic_parition():
    """
    {
    "title": "test_modify_table_dynamic_parition",
    "describe": "修改表属性dynamic_partition，预期修改成功，目前失败，bug",
    "tag": "p1,system"
    }
    """
    """bug:修改表的动态分区属性"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    partition_a = palo_client.Partition('partition_a', '2010-01-01 00:00:00', storage_medium='SSD')
    partition_b = palo_client.Partition('partition_b', '2030-01-01 00:00:00', storage_medium='SSD')
    partition_c = palo_client.Partition('partition_c', '2050-02-01 00:00:00', storage_medium='SSD')
    partition_d = palo_client.Partition('partition_d', '2100-03-01 00:00:00', storage_medium='SSD')
    partition_list = [partition_a, partition_b, partition_c, partition_d]
    partition_info = palo_client.PartitionInfo('k5', partition_list=partition_list)
    distribution_info = palo_client.DistributionInfo('Hash(k1)', 3)

    assert client.create_table(table_name, DATA.schema_1, partition_info, distribution_info)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_partition_storage_medium(table_name, 'partition_a') == 'SSD'
    assert client.get_partition_storage_medium(table_name, 'partition_b') == 'SSD'
    assert client.get_partition_storage_medium(table_name, 'partition_c') == 'SSD'
    assert client.get_partition_storage_medium(table_name, 'partition_d') == 'SSD'

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    # todo: bug，非master代码报错，master代码待确认
    #ret = client.modify_partition(table_name, **{'dynamic_partition.enable': 'false'})
    #assert ret 
    dynamic_prition_property = {'dynamic_partition.enable': 'true', 'dynamic_partition.time_unit': 'DAY', 
                                'dynamic_partition.end': '3', 'dynamic_partition.prefix': 'partition_'}
    ret = client.modify_partition(table_name, **dynamic_prition_property)
    assert ret


def test_modify_s_tb_reduce_replication():
    """
    {
    "title": "test_modify_s_tb_reduce_replication",
    "describe": "修改单分区表分区属性replication_num，由3变为2，修改成功，show partitons，replication num为2",
    "tag": "p1,system"
    }
    """
    """修改单分区表的replication_num, storage_medium, storage_cooldown_time, in_memory属性"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    distribution_info = palo_client.DistributionInfo('Hash(k1)', 3)
    assert client.create_table(table_name, DATA.schema_1, distribution_info=distribution_info) 
    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    ret = client.modify_partition(table_name, table_name, replication_num=2)
    assert ret
    ret = client.show_partitions(table_name)
    part_replication_list = util.get_attr(ret, PartitionInfo.ReplicationNum)
    assert part_replication_list == [u'2'] * len(part_replication_list)
    client.clean(database_name)


def test_modify_s_tb_add_replication():
    """
    {
    "title": "test_modify_s_tb_add_replication",
    "describe": "修改单分区表分区属性replication_num，由3变为4，修改成功，show partitons，replication num为4",
    "tag": "p1,system"
    }
    """
    """修改单分区表的replication_num, storage_medium, storage_cooldown_time, in_memory属性"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    distribution_info = palo_client.DistributionInfo('Hash(k1)', 3)
    assert client.create_table(table_name, DATA.schema_1, distribution_info=distribution_info)
    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    ret = client.modify_partition(table_name, table_name, replication_num=4)
    assert ret
    ret = client.show_partitions(table_name)
    part_replication_list = util.get_attr(ret, PartitionInfo.ReplicationNum)
    assert part_replication_list == [u'4'] * len(part_replication_list)
    client.clean(database_name)


def test_modfiy_s_tb_cooldown_time():
    """
    {
    "title": "test_modfiy_s_tb_in_memory",
    "describe": "修改单分区表分区属性storage_cooldown_time，成功",
    "tag": "p1,system"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    distribution_info = palo_client.DistributionInfo('Hash(k1)', 3)
    assert client.create_table(table_name, DATA.schema_1, distribution_info=distribution_info, storage_medium='SSD')
    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    ret = client.modify_partition(table_name, table_name, storage_cooldown_time='9999-01-01 00:00:00')
    assert True
    ret = client.show_partitions(table_name)
    assert util.get_attr(ret, PartitionInfo.CooldownTime) == [u'9999-01-01 00:00:00']
    client.clean(database_name)
    
    
# 单分区表修改storage_medium和storage_cooldown_time在test_sys_storage_medium.py中，此处不再验证


def test_modify_p_tb_reduce_replication():
    """
    {
    "title": "test_modify_p_tb_reduce_replication",
    "describe": "修改分区表多个分区属性replication_num，由3变为2，修改成功，show partition验证被修改分区为2副本，增加分区，新分区的副本数为3",
    "tag": "p1,system"
    }
    """

    """减少表的副本数量，原有分区的副本数不变，新加分区的副本数使用修改后的副本数"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    partition_a = palo_client.Partition('partition_a', '100', storage_medium='SSD')
    partition_b = palo_client.Partition('partition_b', '200', storage_medium='SSD')
    partition_c = palo_client.Partition('partition_c', '300', storage_medium='SSD')
    partition_d = palo_client.Partition('partition_d', '5000', storage_medium='SSD')
    partition_list = [partition_a, partition_b, partition_c, partition_d]
    partition_info = palo_client.PartitionInfo('k3', partition_list=partition_list)
    distribution_info = palo_client.DistributionInfo('Hash(k1)', 3)

    assert client.create_table(table_name, DATA.schema_1, partition_info, distribution_info)
    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    ret = client.show_partitions(table_name)
    ret = client.modify_partition(table_name, ['partition_a', 'partition_d'], replication_num=2)
    assert ret
    ret = client.show_partitions(table_name)
    assert '2' == util.get_attr_condition_value(ret, PartitionInfo.PartitionName, 
                                                'partition_a', PartitionInfo.ReplicationNum)
    assert '2' == util.get_attr_condition_value(ret, PartitionInfo.PartitionName, 
                                                'partition_d', PartitionInfo.ReplicationNum)
    assert '3' == util.get_attr_condition_value(ret, PartitionInfo.PartitionName, 
                                                'partition_b', PartitionInfo.ReplicationNum)
    assert '3' == util.get_attr_condition_value(ret, PartitionInfo.PartitionName, 
                                                'partition_c', PartitionInfo.ReplicationNum)

    ret = client.add_partition(table_name, 'partition_e', '10000')
    assert ret
    ret = client.show_partitions(table_name)
    assert '3' == util.get_attr_condition_value(ret, PartitionInfo.PartitionName, 
                                                'partition_e', PartitionInfo.ReplicationNum)
    client.clean(database_name)   


def test_modify_p_tb_add_replication():
    """
    {
    "title": "test_modify_p_tb_add_replication",
    "describe": "修改分区表多个分区属性replication_num，由3变为4，修改成功，show partition验证被修改分区为4副本，增加分区，新分区的副本数为3",
    "tag": "p1,system"
    }
    """
    """减少表的副本数量，原有分区的副本数不变，新加分区的副本数使用修改后的副本数"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    partition_a = palo_client.Partition('partition_a', '100', storage_medium='SSD')
    partition_b = palo_client.Partition('partition_b', '200', storage_medium='SSD')
    partition_c = palo_client.Partition('partition_c', '300', storage_medium='SSD')
    partition_d = palo_client.Partition('partition_d', '5000', storage_medium='SSD')
    partition_list = [partition_a, partition_b, partition_c, partition_d]
    partition_info = palo_client.PartitionInfo('k3', partition_list=partition_list)
    distribution_info = palo_client.DistributionInfo('Hash(k1)', 3)

    assert client.create_table(table_name, DATA.schema_1, partition_info, distribution_info)
    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    ret = client.show_partitions(table_name)
    ret = client.modify_partition(table_name, ['partition_a', 'partition_d'], replication_num=4)
    assert ret
    ret = client.show_partitions(table_name)
    part_replication_list = util.get_attr(ret, PartitionInfo.ReplicationNum)
    assert part_replication_list == [u'4', u'3', u'3', u'4']

    ret = client.add_partition(table_name, 'partition_e', '10000')
    assert ret
    ret = client.show_partitions(table_name)
    assert '3' == util.get_attr_condition_value(ret, PartitionInfo.PartitionName, 
                                                'partition_e', PartitionInfo.ReplicationNum) 
    part_replication_list = util.get_attr(ret, PartitionInfo.ReplicationNum)
    assert part_replication_list == [u'4', u'3', u'3', u'4', u'3']
    client.clean(database_name)


def test_modify_p_tb_storage_medium():
    """
    {
    "title": "test_modify_p_tb_storage_medium",
    "describe": "修改分区表分区属性storage_medium，修改多个分区pa, pb的storage medium，验证，修改所有分区 *的storage medium，验证",
    "tag": "p1,system"
    }
    """
    """减少表的副本数量，原有分区的副本数不变，新加分区的副本数使用修改后的副本数"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    partition_a = palo_client.Partition('partition_a', '100', storage_medium='SSD')
    partition_b = palo_client.Partition('partition_b', '200', storage_medium='SSD')
    partition_c = palo_client.Partition('partition_c', '300', storage_medium='SSD')
    partition_d = palo_client.Partition('partition_d', '5000', storage_medium='SSD')
    partition_list = [partition_a, partition_b, partition_c, partition_d]
    partition_info = palo_client.PartitionInfo('k3', partition_list=partition_list)
    distribution_info = palo_client.DistributionInfo('Hash(k1)', 3)

    assert client.create_table(table_name, DATA.schema_1, partition_info, distribution_info)
    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_partition_storage_medium(table_name, 'partition_a') == 'SSD'
    assert client.get_partition_storage_medium(table_name, 'partition_b') == 'SSD'
    assert client.get_partition_storage_medium(table_name, 'partition_c') == 'SSD'
    assert client.get_partition_storage_medium(table_name, 'partition_d') == 'SSD'

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    ret = client.modify_partition(table_name, ['partition_c', 'partition_d'], storage_medium='HDD')
    assert ret
    assert client.get_partition_storage_medium(table_name, 'partition_c') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_d') == 'HDD'
    ret = client.modify_partition(table_name, ['*'], storage_medium='HDD')    
    assert client.get_partition_storage_medium(table_name, 'partition_a') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_b') == 'HDD'
    client.clean(database_name)


def test_modify_p_tb_all_reduce_replication():
    """
    {
    "title": "test_modify_p_tb_all_reduce_replication",
    "describe": "修改分区表所有分区属性replication_num，由3变为2，show partition验证所有分区变为2，增加分区，新增分区副本数为3",
    "tag": "p1,system"
    }
    """
    """减少表的副本数量，原有分区的副本数不变，新加分区的副本数使用修改后的副本数"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    partition_a = palo_client.Partition('partition_a', '100', storage_medium='SSD')
    partition_b = palo_client.Partition('partition_b', '200', storage_medium='SSD')
    partition_c = palo_client.Partition('partition_c', '300', storage_medium='SSD')
    partition_d = palo_client.Partition('partition_d', '5000', storage_medium='SSD')
    partition_list = [partition_a, partition_b, partition_c, partition_d]
    partition_info = palo_client.PartitionInfo('k3', partition_list=partition_list)
    distribution_info = palo_client.DistributionInfo('Hash(k1)', 3)

    assert client.create_table(table_name, DATA.schema_1, partition_info, distribution_info)
    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    ret = client.show_partitions(table_name)
    ret = client.modify_partition(table_name, ['*'], replication_num=2)
    assert ret
    ret = client.show_partitions(table_name)
    part_replication_list = util.get_attr(ret, PartitionInfo.ReplicationNum)
    assert part_replication_list == [u'2'] * len(part_replication_list)

    ret = client.add_partition(table_name, 'partition_e', '10000')
    assert ret
    ret = client.show_partitions(table_name)
    assert '3' == util.get_attr_condition_value(ret, PartitionInfo.PartitionName, 'partition_e', 
                                                PartitionInfo.ReplicationNum)
    part_replication_list = util.get_attr(ret, PartitionInfo.ReplicationNum)
    print(part_replication_list)
    assert part_replication_list == [u'2'] * (len(part_replication_list) - 1) + [u'3']
    client.clean(database_name)


def test_modify_p_tb_all_add_replication():
    """
    {
    "title": "test_modify_p_tb_all_add_replication",
    "describe": "修改分区表所有分区属性replication_num，由3变为4，show partition验证所有分区变为4，增加分区，新增分区副本数为4",
    "tag": "p1,system"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    partition_a = palo_client.Partition('partition_a', '100', storage_medium='SSD')
    partition_b = palo_client.Partition('partition_b', '200', storage_medium='SSD')
    partition_c = palo_client.Partition('partition_c', '300', storage_medium='SSD')
    partition_d = palo_client.Partition('partition_d', '5000', storage_medium='SSD')
    partition_list = [partition_a, partition_b, partition_c, partition_d]
    partition_info = palo_client.PartitionInfo('k3', partition_list=partition_list)
    distribution_info = palo_client.DistributionInfo('Hash(k1)', 3)

    assert client.create_table(table_name, DATA.schema_1, partition_info, distribution_info)
    assert client.show_tables(table_name)
    assert client.get_index(table_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    ret = client.show_partitions(table_name)
    ret = client.modify_partition(table_name, ['*'], replication_num=4)
    assert ret
    ret = client.show_partitions(table_name)
    part_replication_list = util.get_attr(ret, PartitionInfo.ReplicationNum)
    assert part_replication_list == [u'4'] * len(part_replication_list)

    ret = client.add_partition(table_name, 'partition_e', '10000')
    assert ret
    ret = client.show_partitions(table_name)
    assert '3' == util.get_attr_condition_value(ret, PartitionInfo.PartitionName, 'partition_e',
                                                PartitionInfo.ReplicationNum)
    part_replication_list = util.get_attr(ret, PartitionInfo.ReplicationNum)
    print(part_replication_list)
    assert part_replication_list == [u'4'] * (len(part_replication_list) - 1) + [u'3']
    client.clean(database_name)


def test_modify_table_default_buckets():
    """
    {
    "title": "test_modify_table_default_buckets",
    "describe": "修改表默认分桶数,github issue 6024",
    "tag": "p1,system"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    partition_name_list = ['partition_a', 'partition_b']
    partition_value_list = ['100', '200']
    partition_info = palo_client.PartitionInfo('k3', partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('Hash(k1)', 8)

    assert client.create_table(table_name, DATA.schema_1, partition_info, distribution_info), "create table failed"
    assert client.show_tables(table_name), "create table failed"
    assert client.get_partition_buckets(table_name, partition_name_list[0], database_name) == '8', "buckets num false"
    assert client.get_partition_buckets(table_name, partition_name_list[1], database_name) == '8', "buckets num false"
    sql = "alter table %s modify distribution distributed by hash(k1) buckets 12" % table_name
    ret = client.execute(sql)
    assert ret == ()
    ret = client.add_partition(table_name, 'partition_c', '300')
    assert ret, "add partition failed"
    assert client.get_partition_buckets(table_name, 'partition_a', database_name) == '8', "buckets num false"
    assert client.get_partition_buckets(table_name, 'partition_b', database_name) == '8', "buckets num false"
    assert client.get_partition_buckets(table_name, 'partition_c', database_name) == '12', "buckets num false"
    sql = "alter table %s modify distribution distributed by hash(k1) buckets 10" % table_name
    ret = client.execute(sql)
    assert ret == ()
    ret = client.add_partition(table_name, 'partition_d', '500')
    assert ret, "add partition failed"
    assert client.get_partition_buckets(table_name, 'partition_a', database_name) == '8', "buckets num false"
    assert client.get_partition_buckets(table_name, 'partition_b', database_name) == '8', "buckets num false"
    assert client.get_partition_buckets(table_name, 'partition_c', database_name) == '12', "buckets num false"
    assert client.get_partition_buckets(table_name, 'partition_d', database_name) == '10', "buckets num false"
    client.clean(database_name)


def teardown_module():
    """tear down"""
    pass


if __name__ == '__main__':
   setup_module()

