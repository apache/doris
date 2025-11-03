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
#   @file test_sys_storage_medium.py
#   @date 2020-05-07 14:48:30
#   @brief 存储介质基本case。需要使用env_config.py中的be数据目录及机器登录信息
#
#############################################################################

"""
分级存储测试
"""
import time
import datetime
import pytest

from data import partition as DATA
from data import schema as SCHEMA
from lib import palo_config
from lib import palo_client
from lib import palo_job
from lib import util

config = palo_config.config
LOG = palo_client.LOG
L = palo_client.L
broker_info = palo_config.broker_info
BE_DATA_PATH_EXISTS=False


def setup_module():
    """
    setUp
    """
    global client, BE_DATA_PATH_EXISTS
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user, \
            password=config.fe_password)
    be_host = client.get_backend_host_list()
    for host in be_host:
        cmd = 'ls %s' % ' '.join(config.be_data_path)
        status, output = util.exec_cmd(cmd, config.host_username, config.host_password, host)
        if status == 0:
            BE_DATA_PATH_EXISTS = True


def check_tablet(table_tablet_info, storage_medium):
    """
    检查tablet是否在be数据目录的ssd中
    table_tablet_info:show tablet result
    """
    if not BE_DATA_PATH_EXISTS:
        return 0
    storage_path_list = list()
    for data_path in config.be_data_path:
        if data_path.endswith(storage_medium.upper()):
            storage_path_list.append(data_path + '/data/')
    for tablet in table_tablet_info:
        tablet_id = tablet[0]
        host = client.get_backend(tablet[2])[palo_job.BackendProcInfo.Host]
        for data_path in storage_path_list:
            cmd = 'find %s -type d -name %s ' % (data_path, tablet_id)
            status, output = util.exec_cmd(cmd, config.host_username, config.host_password, host)
            print(cmd)
            print(status, output)
            LOG.info(L('exec cmd on remote host', cmd=cmd, username=config.host_username, host=host))
            LOG.info(L('exec cmd on remote host ret', status=status, output=output))
            assert status == 0
            assert output.find(tablet_id) != -1


def test_ssd_all():
    """
    {
    "title": "存储介质SSD测试",
    "describe": "复合分区表建表时指定全部分区到SSD,验证BE在SSD上建立了存储分片,导入数据后, 验证数据正确分布到了SSD，并对数据进行正确性验证",
    "tag": "p1,system"
    }
    """
    """
    复合分区表建表时指定全部分区到SSD
    验证BE在SSD上建立了存储分片
    导入数据后, 验证数据正确分布到了SSD，并对数据进行正确性验证
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
                table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
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
    retry_times = 10
    while retry_times > 0:
        try:
            retry_times -= 1
            check_tablet(ret, 'ssd')
            break
        except Exception as e:
            time.sleep(10)
            print(e)
    if retry_times == 0:
        assert 0 == 1, 'storage medium check error'

    client.clean(database_name)


def test_hdd_all():
    """
    {
    "title": "建表指定HDD",
    "describe": "复合分区表建表时指定全部分区到HDD，验证BE在HDD正确建立了存储分片，导入数据后，验证相应分区的数据正确分布到了HDD，并对数据进行正确性验证",
    "tag": "p1,system"
    }
    """
    """
    复合分区表建表时指定全部分区到HDD
    验证BE在HDD正确建立了存储分片
    导入数据后，验证相应分区的数据正确分布到了HDD，并对数据进行正确性验证
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_a = palo_client.Partition('partition_a', '100', storage_medium='HDD')
    partition_b = palo_client.Partition('partition_b', '200', storage_medium='HDD')
    partition_c = palo_client.Partition('partition_c', '300', storage_medium='HDD')
    partition_d = palo_client.Partition('partition_d', 'MAXVALUE', storage_medium='HDD')
    partition_list = [partition_a, partition_b, partition_c, partition_d]
    partition_info = palo_client.PartitionInfo('k3', partition_list=partition_list)
    distribution_info = palo_client.DistributionInfo('Hash(k2)', 3)

    assert client.create_table(table_name, DATA.schema_1,
                               partition_info, distribution_info)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_partition_storage_medium(table_name, 'partition_a') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_b') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_c') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_d') == 'HDD'

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    ret = client.show_tablet(table_name)
    retry_times = 10
    while retry_times > 0:
        try:
            retry_times -= 1
            check_tablet(ret, 'hdd')
            break
        except Exception as e:
            time.sleep(10)
            print(e)
    if retry_times == 0:
        assert 0 == 1, 'storage medium check error'

    client.clean(database_name)


def test_ssd_hdd():
    """
    {
    "title": "建分区表，ssd和hdd混合",
    "describe": "复合分区表建表时指定部分分区到SSD，指定其余分区到HDD",
    "tag": "p1,system"
    }
    """
    """
    复合分区表建表时指定部分分区到SSD，指定其余分区到HDD
    并且保证所有BE的SSD和HDD空间充足
    验证BE在SSD和HDD建立了存储分片
    导入数据后，验证相应分区的数据正确分布到了SSD和HDD，并对数据进行正确性验证
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_a = palo_client.Partition('partition_a', '100', storage_medium='SSD')
    partition_b = palo_client.Partition('partition_b', '200', storage_medium='HDD')
    partition_c = palo_client.Partition('partition_c', '300', storage_medium='SSD')
    partition_d = palo_client.Partition('partition_d', 'MAXVALUE', storage_medium='HDD')
    partition_list = [partition_a, partition_b, partition_c, partition_d]
    partition_info = palo_client.PartitionInfo('k3', partition_list=partition_list)
    distribution_info = palo_client.DistributionInfo('Hash(k3)', 3)

    assert client.create_table(table_name, DATA.schema_1,
                               partition_info, distribution_info)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_partition_storage_medium(table_name, 'partition_a') == 'SSD'
    assert client.get_partition_storage_medium(table_name, 'partition_b') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_c') == 'SSD'
    assert client.get_partition_storage_medium(table_name, 'partition_d') == 'HDD'

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    ret = client.show_tablet(table_name, partition_list=['partition_a', 'partition_c'])
    retry_times = 10
    while retry_times > 0:
        try:
            retry_times -= 1
            check_tablet(ret, 'ssd')
            break
        except Exception as e:
            time.sleep(10)
            print(e)
    if retry_times == 0:
        assert 0 == 1, 'storage medium check error'

    ret = client.show_tablet(table_name, partition_list=['partition_b', 'partition_d'])
    retry_times = 10
    while retry_times > 0:
        try:
            retry_times -= 1
            check_tablet(ret, 'hdd')
            break
        except Exception as e:
            time.sleep(10)
            print(e)
    if retry_times == 0:
        assert 0 == 1, 'storage medium check error'

    client.clean(database_name)


def test_ssd_table():
    """
    {
    "title": "建立ssd单分区表",
    "describe": "单分区表建表时指定到SSD，验证BE在SSD上建立了存储分片，导入数据后，验证数据正确分布到了SSD，并对数据进行正确性验证",
    "tag": "p1,system"
    }
    """
    """
    单分区表建表时指定到SSD，
    验证BE在SSD上建立了存储分片
    导入数据后，验证数据正确分布到了SSD，并对数据进行正确性验证
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    distribution_info = palo_client.DistributionInfo('Hash(k1)', 3)

    assert client.create_table(table_name, DATA.schema_1,
                               distribution_info=distribution_info, storage_medium='SSD')

    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_partition_storage_medium(table_name, table_name) == 'SSD'

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    ret = client.show_tablet(table_name)
    retry_times = 10
    while retry_times > 0:
        try:
            retry_times -= 1
            check_tablet(ret, 'ssd')
            break
        except Exception as e:
            time.sleep(10)
            print(e)
    if retry_times == 0:
        assert 0 == 1, 'storage medium check error'

    assert client.verify(DATA.expected_data_file_list_1, table_name)
    client.clean(database_name)


def test_hdd_table():
    """
    {
    "title": "建立hdd单分区表",
    "describe": "单分区表建表时指定到HDD，验证BE在HDD上建立了存储分片，导入数据后，验证数据正确分布到了HDD，并对数据进行正确性验证",
    "tag": "p1,system"
    }
    """
    """
    单分区表建表时指定到HDD，
    验证BE在HDD上建立了存储分片
    导入数据后，验证数据正确分布到了HDD，并对数据进行正确性验证
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    distribution_info = palo_client.DistributionInfo('Hash(k1)', 3)
    assert client.create_table(table_name, DATA.schema_1,
                               distribution_info=distribution_info, storage_medium='HDD')

    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_partition_storage_medium(table_name, table_name) == 'HDD'

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    ret = client.show_tablet(table_name)
    retry_times = 10
    while retry_times > 0:
        try:
            retry_times -= 1
            check_tablet(ret, 'hdd')
            break
        except Exception as e:
            time.sleep(10)
            print(e)
    if retry_times == 0:
        assert 0 == 1, 'storage medium check error'

    client.clean(database_name)


def test_add_ssd_partition():
    """
    {
    "title": "增加ssd分区",
    "describe": "复合分区表，增加分区指定到SSD，同时修改BUCKETS个数，验证BE在SSD上建立了新的分区的存储分片，导入新增分区的数据后，验证数据分布到SSD，正确性验证",
    "tag": "p1,system"
    }
    """
    """
    复合分区表，增加分区指定到SSD，同时修改BUCKETS个数
    验证BE在SSD上建立了新的分区的存储分片
    导入新增分区的数据后，验证数据正确分布到了SSD，并对数据进行正确性验证
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
        table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_a = palo_client.Partition('partition_a', '100', storage_medium='HDD')
    partition_b = palo_client.Partition('partition_b', '200', storage_medium='HDD')
    partition_c = palo_client.Partition('partition_c', '300', storage_medium='HDD')
    partition_d = palo_client.Partition('partition_d', '50000', storage_medium='HDD')
    partition_list = [partition_a, partition_b, partition_c, partition_d]
    partition_info = palo_client.PartitionInfo('k3', partition_list=partition_list)
    distribution_info = palo_client.DistributionInfo('Hash(k2)', 3)

    assert client.create_table(table_name, DATA.schema_1,
                               partition_info, distribution_info)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_partition_storage_medium(table_name, 'partition_a') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_b') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_c') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_d') == 'HDD'

    ret = client.show_tablet(table_name)
    retry_times = 10
    while retry_times > 0:
        try:
            retry_times -= 1
            check_tablet(ret, 'hdd')
            break
        except Exception as e:
            time.sleep(10)
            print(e)
    if retry_times == 0:
        assert 0 == 1, 'storage medium check error'


    assert client.add_partition(table_name, 'partition_add', 'MAXVALUE', 
                                storage_medium='SSD', distribute_type='Hash(k2)', 
                                bucket_num=13)
    assert client.get_partition_storage_medium(table_name, 'partition_add') == 'SSD'

    ret = client.show_tablet(table_name, partition_list=['partition_add'])
    retry_times = 10
    while retry_times > 0:
        try:
            retry_times -= 1
            check_tablet(ret, 'ssd')
            break
        except Exception as e:
            time.sleep(10)
            print(e)
    if retry_times == 0:
        assert 0 == 1, 'storage medium check error'


    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    client.clean(database_name)


def test_add_hdd_partition():
    """
    {
    "title": "增加hdd分区",
    "describe": "复合分区表，增加分区指定到HDD，同时修改BUCKETS个数,验证BE在SSD上建立了新的分区的存储分片,导入新增分区的数据后，验证数据正确分布到了HDD，并对数据进行正确性验证",
    "tag": "autotest"
    }
    """
    """
    复合分区表，增加分区指定到HDD，同时修改BUCKETS个数
    验证BE在SSD上建立了新的分区的存储分片
    导入新增分区的数据后，验证数据正确分布到了HDD，并对数据进行正确性验证
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_a = palo_client.Partition('partition_a', '100', storage_medium='SSD')
    partition_b = palo_client.Partition('partition_b', '200', storage_medium='SSD')
    partition_c = palo_client.Partition('partition_c', '300', storage_medium='SSD')
    partition_d = palo_client.Partition('partition_d', '50000', storage_medium='SSD')
    partition_list = [partition_a, partition_b, partition_c, partition_d]
    partition_info = palo_client.PartitionInfo('k3', partition_list=partition_list)
    distribution_info = palo_client.DistributionInfo('Hash(k1)', 1)
    assert client.create_table(table_name, DATA.schema_1,
                               partition_info, distribution_info)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_partition_storage_medium(table_name, 'partition_a') == 'SSD'
    assert client.get_partition_storage_medium(table_name, 'partition_b') == 'SSD'
    assert client.get_partition_storage_medium(table_name, 'partition_c') == 'SSD'
    assert client.get_partition_storage_medium(table_name, 'partition_d') == 'SSD'
    ret = client.show_tablet(table_name)
    retry_times = 10
    while retry_times > 0:
        try:
            retry_times -= 1
            check_tablet(ret, 'ssd')
            break
        except Exception as e:
            time.sleep(10)
            print(e)
    if retry_times == 0:
        assert 0 == 1, 'storage medium check error'
    assert client.add_partition(table_name, 'partition_add', 'MAXVALUE',
                                storage_medium='HDD', distribute_type='Hash(k1)', bucket_num=13)
    assert client.get_partition_storage_medium(table_name, 'partition_add') == 'HDD'
    ret = client.show_tablet(table_name, partition_list=['partition_add'])
    retry_times = 10
    while retry_times > 0:
        try:
            retry_times -= 1
            check_tablet(ret, 'hdd')
            break
        except Exception as e:
            time.sleep(10)
            print(e)
    if retry_times == 0:
        assert 0 == 1, 'storage medium check error'
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    client.clean(database_name)


def test_modify_to_hdd_partition():
    """
    {
    "title": "修改ssd分区为hdd",
    "describe": "主动修改分区存储属性，SSD改为HDD，HDD充足，验证BE汇报后迁移正确，数据正确",
    "tag": "p1,system"
    }
    """
    """
    主动修改分区存储属性，SSD改为HDD，HDD充足，验证BE汇报后迁移正确，数据正确
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_a = palo_client.Partition('partition_a', '100', storage_medium='HDD')
    partition_b = palo_client.Partition('partition_b', '200', storage_medium='HDD')
    partition_c = palo_client.Partition('partition_c', '300', storage_medium='HDD')
    partition_d = palo_client.Partition('partition_d', '50000', storage_medium='SSD')
    partition_list = [partition_a, partition_b, partition_c, partition_d]
    partition_info = palo_client.PartitionInfo('k3', partition_list=partition_list)
    distribution_info = palo_client.DistributionInfo('Hash(k3)', 3)

    assert client.create_table(table_name, DATA.schema_1,
                               partition_info, distribution_info)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_partition_storage_medium(table_name, 'partition_a') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_b') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_c') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_d') == 'SSD'

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    assert client.modify_partition(table_name, 'partition_d', storage_medium='HDD')
    assert client.get_partition_storage_medium(table_name, 'partition_d') == 'HDD'
    ret = client.show_tablet(table_name, partition_list=['partition_d'])
    retry_times = 10
    while retry_times > 0:
        try:
            time.sleep(30)
            retry_times -= 1
            check_tablet(ret, 'hdd')
            break
        except Exception as e:
            print(e)
    if retry_times == 0:
       raise pytest.skip('timeout, cannot get tablet from hdd')
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    client.clean(database_name)


def test_modify_to_ssd_partition():
    """
    {
    "title": "修改hdd分区为ssd",
    "describe": "主动修改分区存储属性，HDD改为SSD，SSD充足，验证BE汇报后迁移正确，数据正确",
    "tag": "p1,system"
    }
    """
    """
    主动修改分区存储属性，HDD改为SSD，SSD充足，验证BE汇报后迁移正确，数据正确
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_a = palo_client.Partition('partition_a', '100', storage_medium='SSD')
    partition_b = palo_client.Partition('partition_b', '200', storage_medium='SSD')
    partition_c = palo_client.Partition('partition_c', '300', storage_medium='SSD')
    partition_d = palo_client.Partition('partition_d', '50000', storage_medium='HDD')
    partition_list = [partition_a, partition_b, partition_c, partition_d]
    partition_info = palo_client.PartitionInfo('k3', partition_list=partition_list)
    distribution_info = palo_client.DistributionInfo('Hash(k2)', 3)

    assert client.create_table(table_name, DATA.schema_1,
                               partition_info, distribution_info)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_partition_storage_medium(table_name, 'partition_a') == 'SSD'
    assert client.get_partition_storage_medium(table_name, 'partition_b') == 'SSD'
    assert client.get_partition_storage_medium(table_name, 'partition_c') == 'SSD'
    assert client.get_partition_storage_medium(table_name, 'partition_d') == 'HDD'
    ret = client.show_tablet(table_name, partition_list=['partition_d'])
    check_tablet(ret, 'hdd')
    assert client.modify_partition(table_name, 'partition_d', storage_medium='SSD')
    assert client.get_partition_storage_medium(table_name, 'partition_d') == 'SSD'
    ret = client.show_tablet(table_name, partition_list=['partition_d'])
    retry_times = 10
    while retry_times > 0:
        try:
            retry_times -= 1
            time.sleep(30)
            check_tablet(ret, 'ssd')
            break
        except Exception as e:
            print(e)
    if retry_times == 0:
       raise pytest.skip('timeout, cannot get tablet from hdd')

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    client.clean(database_name)


def test_modify_ssd_to_ssd_partition():
    """
    {
    "title": "将ssd分区改为ssd",
    "describe": "主动修改分区存储属性，SSD改为SSD，验证不迁移",
    "tag": "p1,system,fuzz"
    }
    """
    """
    主动修改分区存储属性，SSD改为SSD，验证不迁移
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_a = palo_client.Partition('partition_a', '100', storage_medium='SSD')
    partition_b = palo_client.Partition('partition_b', '200', storage_medium='SSD')
    partition_c = palo_client.Partition('partition_c', '300', storage_medium='SSD')
    partition_d = palo_client.Partition('partition_d', '50000', storage_medium='SSD')
    partition_list = [partition_a, partition_b, partition_c, partition_d]
    partition_info = palo_client.PartitionInfo('k3', partition_list=partition_list)
    distribution_info = palo_client.DistributionInfo('Hash(k1)', 3)

    assert client.create_table(table_name, DATA.schema_1,
                               partition_info, distribution_info)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_partition_storage_medium(table_name, 'partition_a') == 'SSD'
    assert client.get_partition_storage_medium(table_name, 'partition_b') == 'SSD'
    assert client.get_partition_storage_medium(table_name, 'partition_c') == 'SSD'
    assert client.get_partition_storage_medium(table_name, 'partition_d') == 'SSD'

    assert client.modify_partition(table_name, 'partition_d', storage_medium='SSD')
    assert client.get_partition_storage_medium(table_name, 'partition_d') == 'SSD'
    ret = client.show_tablet(table_name, partition_list=['partition_d'])
    retry_times = 10
    while retry_times > 0:
        try:
            time.sleep(30)
            retry_times -= 1
            check_tablet(ret, 'ssd')
            break
        except Exception as e:
            print(e)
    if retry_times == 0:
       raise pytest.skip('timeout, cannot get tablet from ssd')

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    client.clean(database_name)


def test_modify_hdd_to_hdd_partition():
    """
    {
    "title": "修改hdd分区为hdd",
    "describe": "主动修改分区存储属性，HDD改为HDD，验证不迁移",
    "tag": "p1,system,fuzz"
    }
    """
    """
    主动修改分区存储属性，HDD改为HDD，验证不迁移
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_a = palo_client.Partition('partition_a', '100', storage_medium='HDD')
    partition_b = palo_client.Partition('partition_b', '200', storage_medium='HDD')
    partition_c = palo_client.Partition('partition_c', '300', storage_medium='HDD')
    partition_d = palo_client.Partition('partition_d', '50000', storage_medium='HDD')
    partition_list = [partition_a, partition_b, partition_c, partition_d]
    partition_info = palo_client.PartitionInfo('k3', partition_list=partition_list)
    distribution_info = palo_client.DistributionInfo('Hash(k1)', 2)
    assert client.create_table(table_name, DATA.schema_1,
                               partition_info, distribution_info)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_partition_storage_medium(table_name, 'partition_a') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_b') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_c') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_d') == 'HDD'

    assert client.modify_partition(table_name, 'partition_d', storage_medium='HDD')
    assert client.get_partition_storage_medium(table_name, 'partition_d') == 'HDD'
    ret = client.show_tablet(table_name, partition_list=['partition_d'])
    retry_times = 10
    while retry_times > 0:
        try:
            time.sleep(30)
            retry_times -= 1
            check_tablet(ret, 'hdd')
            break
        except Exception as e:
            print(e)
    if retry_times == 0:
       raise pytest.skip('timeout, cannot get tablet from hdd')

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    client.clean(database_name)


def test_modify_to_hdd_partition_at_cooldown_time():
    """
    {
    "title": "修改hdd分区冷却时间",
    "describe": "主动修改分区storage_cooldown_time属性，HDD充足，验证属性修改正确，验证过期时间达到后BE汇报后迁移正确",
    "tag": "autotest"
    }
    """
    """
    主动修改分区storage_cooldown_time属性，HDD充足
    验证属性修改正确，验证过期时间达到后BE汇报后迁移正确
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
        table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_a = palo_client.Partition('partition_a', '100', storage_medium='HDD')
    partition_b = palo_client.Partition('partition_b', '200', storage_medium='HDD')
    partition_c = palo_client.Partition('partition_c', '300', storage_medium='HDD')
    partition_d = palo_client.Partition('partition_d', '50000', storage_medium='SSD')
    partition_list = [partition_a, partition_b, partition_c, partition_d]
    partition_info = palo_client.PartitionInfo('k3', partition_list=partition_list)
    distribution_info = palo_client.DistributionInfo('Hash(k3)', 3)

    assert client.create_table(table_name, DATA.schema_1,
                               partition_info, distribution_info)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_partition_storage_medium(table_name, 'partition_a') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_b') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_c') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_d') == 'SSD'

    storage_cooldown_delta_time = 150
    delta = datetime.timedelta(seconds=storage_cooldown_delta_time)
    storage_cooldown_time = (datetime.datetime.now() + delta).strftime('%Y-%m-%d %H:%M:%S')

    assert client.modify_partition(table_name, 'partition_d', storage_medium='SSD',
                                   storage_cooldown_time=storage_cooldown_time)
    time.sleep(storage_cooldown_delta_time + 30)
    ret = client.show_tablet(table_name, partition_list=['partition_d'])
    retry_times = 10
    while retry_times > 0:
        try:
            time.sleep(30)
            retry_times -= 1
            check_tablet(ret, 'hdd')
            break
        except Exception as e:
            print(e)
    if retry_times == 0:
       raise pytest.skip('timeout, cannot get tablet from hdd')

    assert client.get_partition_storage_medium(table_name, 'partition_d') == 'HDD'
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    client.clean(database_name)


def test_modify_medium_cooldown_time():
    """
    {
    "title": "修改hdd分区为ssd，增加冷却时间",
    "describe": "单分区表同时修改分区storage_medium、storage_cooldown_time属性，HDD改为SSD，SSD充足，验证属性修改正确，验证迁移正确",
    "tag": "autotest"
    }
    """
    """
    单分区表同时修改分区storage_medium、storage_cooldown_time属性
    HDD改为SSD，SSD充足，验证属性修改正确，验证迁移正确
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    distribution_info = palo_client.DistributionInfo('Hash(k1)', 3)

    assert client.create_table(table_name, DATA.schema_1,
                               distribution_info=distribution_info, storage_medium='HDD')

    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_partition_storage_medium(table_name, table_name) == 'HDD'

    storage_cooldown_delta_time = 150
    delta = datetime.timedelta(seconds=storage_cooldown_delta_time)
    storage_cooldown_time = (datetime.datetime.now() + delta).strftime('%Y-%m-%d %H:%M:%S')

    assert client.modify_partition(table_name, table_name, storage_medium='SSD',
                                   storage_cooldown_time=storage_cooldown_time)

    time.sleep(storage_cooldown_delta_time + 30)
    ret = client.show_tablet(table_name)
    retry_times = 10
    while retry_times > 0:
        try:
            time.sleep(30)
            retry_times -= 1
            check_tablet(ret, 'hdd')
            break
        except Exception as e:
            print(e)
    if retry_times == 0:
        raise pytest.skip('timeout, cannot get tablet from hdd')
    assert client.get_partition_storage_medium(table_name, table_name) == 'HDD'
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    client.clean(database_name)


def test_modify_medium_cooldown_time_for_partitions():
    """
    {
    "title": "修改分区为ssd，修改冷却时间",
    "describe": "复合分区表对多个分区同时修改分区storage_medium、storage_cooldown_time属性,HDD改为SSD，SSD充足，验证属性修改正确，验证迁移正确",
    "tag": "p1,system"
    }
    """
    """
    复合分区表对多个分区同时修改分区storage_medium、storage_cooldown_time属性
    HDD改为SSD，SSD充足，验证属性修改正确，验证迁移正确
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_a = palo_client.Partition('partition_a', '100', storage_medium='HDD')
    partition_b = palo_client.Partition('partition_b', '200', storage_medium='HDD')
    partition_c = palo_client.Partition('partition_c', '300', storage_medium='HDD')
    partition_d = palo_client.Partition('partition_d', '50000', storage_medium='SSD')
    partition_list = [partition_a, partition_b, partition_c, partition_d]
    partition_info = palo_client.PartitionInfo('k3', partition_list=partition_list)
    distribution_info = palo_client.DistributionInfo('Hash(k3)', 3)

    assert client.create_table(table_name, DATA.schema_1,
                               partition_info, distribution_info)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_partition_storage_medium(table_name, 'partition_a') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_b') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_c') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_d') == 'SSD'

    storage_cooldown_delta_time = 150
    delta = datetime.timedelta(seconds=storage_cooldown_delta_time)
    storage_cooldown_time = (datetime.datetime.now() + delta).strftime('%Y-%m-%d %H:%M:%S')

    assert client.modify_partition(table_name, 'partition_c', storage_medium='SSD',
                                   storage_cooldown_time=storage_cooldown_time)
    assert client.modify_partition(table_name, 'partition_d', storage_medium='SSD',
                                   storage_cooldown_time=storage_cooldown_time)
    time.sleep(storage_cooldown_delta_time + 30)
    ret = client.show_tablet(table_name)
    retry_times = 10
    while retry_times > 0:
        try:
            time.sleep(30)
            retry_times -= 1
            check_tablet(ret, 'hdd')
            break
        except Exception as e:
            print(e)
    if retry_times == 0:
        raise pytest.skip('timeout, cannot get tablet from hdd')
    assert client.get_partition_storage_medium(table_name, 'partition_c') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_d') == 'HDD'
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    client.clean(database_name)


def test_modify_cooldown_time():
    """
    {
    "title": "单分区表增加冷却时间",
    "describe": "单分区表SSD指定过期时间，验证过期时间达到后BE汇报后数据迁移到HDD，验证数据正确",
    "tag": "autotest"
    }
    """
    """
    单分区表SSD指定过期时间，验证过期时间达到后BE汇报后数据迁移到HDD，验证数据正确
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    distribution_info = palo_client.DistributionInfo('Hash(k1)', 3)

    assert client.create_table(table_name, DATA.schema_1,
                               distribution_info=distribution_info, storage_medium='SSD')

    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_partition_storage_medium(table_name, table_name) == 'SSD'

    storage_cooldown_delta_time = 150
    delta = datetime.timedelta(seconds=storage_cooldown_delta_time)
    storage_cooldown_time = (datetime.datetime.now() + delta).strftime('%Y-%m-%d %H:%M:%S')

    assert client.modify_partition(table_name, table_name, storage_medium='SSD',
                                   storage_cooldown_time=storage_cooldown_time)

    time.sleep(storage_cooldown_delta_time + 30)
    ret = client.show_tablet(table_name)
    retry_times = 10
    while retry_times > 0:
        try:
            time.sleep(30)
            retry_times -= 1
            check_tablet(ret, 'hdd')
            break
        except Exception as e:
            print(e)
    if retry_times == 0:
        raise pytest.skip('timeout, cannot get tablet from hdd')
    assert client.get_partition_storage_medium(table_name, table_name) == 'HDD'
    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    client.clean(database_name)


def test_modify_invalid_cooldown_time_a():
    """
    {
    "title": "无效的冷却时间",
    "describe": "单分区表SSD指定过去时间为过期时间，验证不迁移，报错",
    "tag": "p1,system,fuzz"
    }
    """
    """
    单分区表SSD指定过去时间为过期时间，验证不迁移，报错
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    distribution_info = palo_client.DistributionInfo('Hash(k1)', 3)
    assert client.create_table(table_name, DATA.schema_1,
                               distribution_info=distribution_info, storage_medium='SSD')
    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_partition_storage_medium(table_name, table_name) == 'SSD'

    storage_cooldown_delta_time = 150
    delta = datetime.timedelta(seconds=storage_cooldown_delta_time)
    storage_cooldown_time = (datetime.datetime.now() - delta).strftime('%Y-%m-%d %H:%M:%S')

    ret = False
    try:
        ret = client.modify_partition(table_name, table_name, storage_medium='SSD',
                                      storage_cooldown_time=storage_cooldown_time)
    except:
        pass
    assert not ret
    client.clean(database_name)


def test_modify_invalid_cooldown_time_b():
    """
    {
    "title": "无效的冷却时间",
    "describe": "单分区表SSD指定当前时间为过期时间，验证报错",
    "tag": "p1,system,fuzz"
    }
    """
    """
    单分区表SSD指定当前时间为过期时间，验证报错
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    distribution_info = palo_client.DistributionInfo('Hash(k1)', 3)
    assert client.create_table(table_name, DATA.schema_1, 
                               distribution_info=distribution_info, storage_medium='SSD')
    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_partition_storage_medium(table_name, table_name) == 'SSD'

    storage_cooldown_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    ret = False
    try:
        ret = client.modify_partition(table_name, table_name, storage_medium='SSD',
                                      storage_cooldown_time=storage_cooldown_time)
    except:
        pass
    assert not ret
    client.clean(database_name)


def test_cooldown_while_select():
    """
    {
    "title": "test_cooldown_while_select",
    "describe": "复合分区表全部分区指定到SSD，部分分区指定过期时间，部分分区不指定过期时间过期时间达到前后1分钟内，持续进行查询操作",
    "tag": "p1,system,stability"
    }
    """
    """
    复合分区表全部分区指定到SSD，SSD充足
    部分分区指定过期时间，部分分区不指定过期时间
    过期时间达到前后1分钟内，持续进行查询操作
    验证查询结果一直正确，验证指定过期时间的分区的数据迁移到HDD
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    storage_cooldown_delta_time = 5 * 60
    delta = datetime.timedelta(seconds=storage_cooldown_delta_time)
    storage_cooldown_time = (datetime.datetime.now() + delta).strftime('%Y-%m-%d %H:%M:%S')

    partition_a = palo_client.Partition('partition_a', '100', storage_medium='HDD')
    partition_b = palo_client.Partition('partition_b', '200', storage_medium='HDD')
    partition_c = palo_client.Partition('partition_c', '300', storage_medium='HDD')
    partition_d = palo_client.Partition('partition_d', '50000', storage_medium='SSD')
    partition_list = [partition_a, partition_b, partition_c, partition_d]
    partition_info = palo_client.PartitionInfo('k3', partition_list=partition_list)
    distribution_info = palo_client.DistributionInfo('Hash(k1)', 3)

    assert client.create_table(table_name, DATA.schema_1, 
                               partition_info, distribution_info)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_partition_storage_medium(table_name, 'partition_a') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_b') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_c') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_d') == 'SSD'

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert client.verify(DATA.expected_data_file_list_1, table_name)

    storage_cooldown_delta_time = 150
    delta = datetime.timedelta(seconds=storage_cooldown_delta_time)
    storage_cooldown = (datetime.datetime.now() + delta)
    storage_cooldown_time = storage_cooldown.strftime('%Y-%m-%d %H:%M:%S')

    assert client.modify_partition(table_name, 'partition_a', storage_medium='SSD',
                                   storage_cooldown_time=storage_cooldown_time)
    assert client.modify_partition(table_name, 'partition_c', storage_medium='SSD',
                                   storage_cooldown_time=storage_cooldown_time)

    while datetime.datetime.now() < storage_cooldown + delta:
        assert client.verify(DATA.expected_data_file_list_1, table_name)
        time.sleep(5)
    
    ret = client.show_tablet(table_name, partition_list=['partition_a', 'partition_c'])
    retry_times = 10
    while retry_times > 0:
        try:
            time.sleep(30)
            retry_times -= 1
            check_tablet(ret, 'ssd')
            break
        except Exception as e:
            print(e)
    if retry_times == 0:
        raise pytest.skip('timeout, cannot get tablet from hdd')
    assert client.verify(DATA.expected_data_file_list_1, table_name)
    client.clean(database_name)


def test_migrate_while_loading():
    """
    {
    "title": "test_migrate_while_loading",
    "describe": "迁移过程的同时导入，验证迁移正确，验证导入正确",
    "tag": "p1,system,stability"
    }
    """
    """
    迁移过程的同时导入，验证迁移正确，验证导入正确
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    partition_a = palo_client.Partition('partition_a', '100', storage_medium='HDD')
    partition_b = palo_client.Partition('partition_b', '200', storage_medium='HDD')
    partition_c = palo_client.Partition('partition_c', '300', storage_medium='HDD')
    partition_d = palo_client.Partition('partition_d', '50000', storage_medium='SSD')
    partition_list = [partition_a, partition_b, partition_c, partition_d]
    partition_info = palo_client.PartitionInfo('k3', partition_list=partition_list)
    distribution_info = palo_client.DistributionInfo('Hash(k1)', 3)
    assert client.create_table(table_name, DATA.schema_1,
                               partition_info, distribution_info)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_partition_storage_medium(table_name, 'partition_a') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_b') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_c') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_d') == 'SSD'

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    label = util.get_label()
    assert client.batch_load(label, data_desc_list, broker=broker_info)
    state = client.get_load_job_state(label)
    while state != 'LOADING':
        if state == 'FINISHED' or state == 'CANCELLED':
            raise pytest.skip('can not get loading state')
        time.sleep(1)
        state = client.get_load_job_state(label)

    assert client.modify_partition(table_name, 'partition_d', storage_medium='HDD')
    time.sleep(30 * 2)

    assert client.verify(DATA.expected_data_file_list_1, table_name)
    assert client.get_partition_storage_medium(table_name, 'partition_d') == 'HDD'
    client.clean(database_name)


def test_migrate_before_schema_change():
    """
    {
    "title": "test_migrate_before_schema_change",
    "describe": "迁移过程的同时schema change，验证迁移正确，验证schema change正确",
    "tag": "p1,system,stability"
    }
    """
    """
    迁移过程的同时schema change，验证迁移正确，验证schema change正确
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_a = palo_client.Partition('partition_a', '100', storage_medium='HDD')
    partition_b = palo_client.Partition('partition_b', '200', storage_medium='HDD')
    partition_c = palo_client.Partition('partition_c', '300', storage_medium='HDD')
    partition_d = palo_client.Partition('partition_d', '50000', storage_medium='SSD')
    partition_list = [partition_a, partition_b, partition_c, partition_d]
    partition_info = palo_client.PartitionInfo('k3', partition_list=partition_list)
    distribution_info = palo_client.DistributionInfo('Hash(k2)', 3)
    keys_desc = 'DUPLICATE KEY(k1,k2,k3,k4,k5)'
    assert client.create_table(table_name, SCHEMA.partition_column_no_agg_list,
                               partition_info, distribution_info, keys_desc=keys_desc)

    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_partition_storage_medium(table_name, 'partition_a') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_b') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_c') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_d') == 'SSD'

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)

    assert client.modify_partition(table_name, 'partition_d', storage_medium='HDD')

    column_name_list = ['k1',]
    assert client.schema_change_drop_column(table_name, column_name_list, is_wait_job=True)

    assert client.verify(DATA.expected_data_file_list_3, table_name)
    assert client.get_partition_storage_medium(table_name, 'partition_d') == 'HDD'
    ret = client.show_tablet(table_name, partition_list=['partition_d'])
    retry_times = 10
    while retry_times > 0:
        try:
            time.sleep(30)
            retry_times -= 1
            check_tablet(ret, 'ssd')
            break
        except Exception as e:
            print(e)
    if retry_times == 0:
        raise pytest.skip('timeout, cannot get tablet from hdd')
    client.clean(database_name)


def test_migrate_after_schema_change():
    """
    {
    "title": "test_migrate_after_schema_change",
    "describe": "schema change的同时迁移，验证迁移正确，验证schema change正确",
    "tag": "p1,system,stability"
    }
    """
    """
    schema change的同时迁移，验证迁移正确，验证schema change正确
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_a = palo_client.Partition('partition_a', '100', storage_medium='HDD')
    partition_b = palo_client.Partition('partition_b', '200', storage_medium='HDD')
    partition_c = palo_client.Partition('partition_c', '300', storage_medium='HDD')
    partition_d = palo_client.Partition('partition_d', '50000', storage_medium='SSD')
    partition_list = [partition_a, partition_b, partition_c, partition_d]
    partition_info = palo_client.PartitionInfo('k3', partition_list=partition_list)
    distribution_info = palo_client.DistributionInfo('Hash(k2)', 3)
    keys_desc = 'DUPLICATE KEY(k1,k2,k3,k4,k5)'
    assert client.create_table(table_name, SCHEMA.partition_column_no_agg_list,
                               partition_info, distribution_info, keys_desc=keys_desc)
    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_partition_storage_medium(table_name, 'partition_a') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_b') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_c') == 'HDD'
    assert client.get_partition_storage_medium(table_name, 'partition_d') == 'SSD'

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)

    column_name_list = ['k1',]
    assert client.schema_change_drop_column(table_name, column_name_list,
                                            is_wait_job=True, is_wait_delete_old_schema=True)

    assert client.modify_partition(table_name, 'partition_d', storage_medium='HDD')

    time.sleep(60)
    assert client.verify(DATA.expected_data_file_list_3, table_name)
    assert client.get_partition_storage_medium(table_name, 'partition_d') == 'HDD'
    ret = client.show_tablet(table_name, partition_list=['partition_d'])
    retry_times = 10
    while retry_times > 0:
        try:
            time.sleep(30)
            retry_times -= 1
            check_tablet(ret, 'ssd')
            break
        except Exception as e:
            print(e)
    if retry_times == 0:
        raise pytest.skip('timeout, cannot get tablet from hdd')
    client.clean(database_name)



if __name__ == '__main__':
    setup_module()
    # test_ssd_all()
    # test_hdd_all()
    # test_ssd_table()
    # test_hdd_table()
    # test_ssd_hdd()
    # test_add_ssd_partition()
    # test_add_hdd_partition()
    # test_modify_to_hdd_partition()
    # test_modify_to_ssd_partition()
    # test_modify_ssd_to_ssd_partition()
    # test_modify_hdd_to_hdd_partition()
    # test_modify_to_hdd_partition_at_cooldown_time()
    # test_modify_medium_cooldown_time()
    # test_modify_medium_cooldown_time_for_partitions()
    # test_modify_cooldown_time()
    # test_modify_invalid_cooldown_time_a()
    # test_modify_invalid_cooldown_time_b()
    # test_cooldown_while_select()
    # test_migrate_while_loading()
    test_migrate_before_schema_change()
    test_migrate_after_schema_change()
