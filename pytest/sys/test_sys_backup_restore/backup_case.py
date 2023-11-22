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
#   @file backup_case.py
#   @date 2018-05-08 14:42:46
#   @brief backup
#
#############################################################################

"""
备份恢复测试基类
"""
import os
import sys
import time
import pytest

file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(file_dir)
file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../sys"))
sys.path.append(file_dir)

from data import partition as DATA
from lib import palo_config
from lib import palo_client
from lib import util

LOG = palo_client.LOG
L = palo_client.L

config = palo_config.config
broker_name = config.broker_name
broker_property = config.broker_property
broker_info = palo_config.broker_info


class BackupBaseCase(object):
    """backup test class"""
    repo_name = 'backup_repo'
    db_name = 'backup_restart_db'
    snapshot_name = 'restart_backup_restart'

    def __init__(self, client):
        self.client = client
        self.single_partition_table = 'single_partition_table'
        self.table_with_rollup = 'table_with_rollup'
        self.multi_partitions_table = 'multi_partitions_table'
        self.all_type_table = 'all_type_table'

    def check_partition_list(self, table_name, partition_name_list):
        """
        验证分区是否创建成功
        """
        for partition_name in partition_name_list:
            assert self.client.get_partition(table_name, partition_name)

    def check2(self, sql1, sql2):
        """check result are same"""
        ret1 = self.client.execute(sql1)
        ret2 = self.client.execute(sql2)
        util.check(ret1, ret2)

    def init_single_partition_table(self):
        """init single partition table"""
        # 创建sting_partition_table，单分区表
        table_name_b = self.single_partition_table
        self.client.create_table(table_name_b, DATA.schema_1)
        # 验证
        assert self.client.show_tables(table_name_b)
        self.check_partition_list(table_name_b, [table_name_b])
        # 导入
        data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name_b)
        assert self.client.batch_load(util.get_label(), data_desc_list, broker=broker_info)

    def init_multi_partition_with_rollup(self, partition_info, distribution_info,
                                           index_name):
        """init multi partition whit rollup table"""
        # 创建table_with_rollup，复合分区表
        table_name_c = self.table_with_rollup
        self.client.create_table(table_name_c, DATA.schema_1,
                            partition_info, distribution_info)
        # 验证
        assert self.client.show_tables(table_name_c)
        self.check_partition_list(table_name_c, partition_info.partition_name_list)
        # 创建上卷表
        assert self.client.create_rollup_table(table_name_c,
                                               index_name, ['k1', 'k2', 'k5', 'v4', 'v6'], is_wait=True)
        # 导入
        data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name_c)
        assert self.client.batch_load(util.get_label(), data_desc_list, broker=broker_info)

    def init_multi_partitions_table(self):
        """init multi partitions table"""
        # 创建multi_partitions_table，复合分区表
        table_name_d = self.multi_partitions_table
        partition_name_list_d = ['partition_e', 'partition_b', 'partition_g', 'partition_h']
        partition_value_list_d = ['100', '500', '1000', 'MAXVALUE']
        partition_info_d = palo_client.PartitionInfo('k3',
                                                     partition_name_list_d, partition_value_list_d)
        distribution_type_d = 'HASH(k1, k2, k5)'
        distribution_info_d = palo_client.DistributionInfo(distribution_type_d, 31)
        self.client.create_table(table_name_d, DATA.schema_1,
                            partition_info_d, distribution_info_d)
        # 验证
        assert self.client.show_tables(table_name_d)
        self.check_partition_list(table_name_d, partition_name_list_d)
        # 导入
        data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name_d)
        assert self.client.batch_load(util.get_label(), data_desc_list, broker=broker_info)

    def init_all_type_table(self):
        """init all type table"""
        # 创建all_type_table，单分区表, all type schema
        table_name_e = self.all_type_table
        self.client.create_table(table_name_e, DATA.schema_2)
        assert self.client.show_tables(table_name_e)
        self.check_partition_list(table_name_e, [table_name_e])
        # 导入
        data_desc_list = palo_client.LoadDataInfo(DATA.file_path_2, table_name_e)
        assert self.client.batch_load(util.get_label(), data_desc_list, broker=broker_info,
                                      max_filter_ratio='0.5')

    def init_partition_table(self, table_name, partition_info):
        """init partition table"""
        # 复合分区表
        table_name_d = table_name
        partition_info_d = partition_info
        distribution_type_d = 'HASH(k1, k2, k5)'
        distribution_info_d = palo_client.DistributionInfo(distribution_type_d, 31)
        self.client.create_table(table_name_d, DATA.schema_1,
                                 partition_info_d, distribution_info_d)
        # 验证
        assert self.client.show_tables(table_name_d)
        self.check_partition_list(table_name_d, partition_info.partition_name_list)
        # 向表d导入
        data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name_d)
        load_label = util.get_label()
        assert self.client.batch_load(load_label, data_desc_list, broker=broker_info)
        return load_label

    def init_multi_all_type_table(self, table_name, partition_info):
        """init multi all type table"""
        # all_type_table, 复合分区表
        table_name_e = table_name
        self.client.create_table(table_name_e, DATA.schema_2, partition_info)
        assert self.client.show_tables(table_name_e)
        self.check_partition_list(table_name_e, [table_name_e])
        # 向表e导入
        data_desc_list = palo_client.LoadDataInfo(DATA.file_path_2, table_name_e)
        assert self.client.batch_load(util.get_label(), data_desc_list, broker=broker_info,
                                      max_filter_ratio='0.5')

    def wait_load(self, database_name):
        """wait load job end"""
        job_list = self.client.get_unfinish_load_job_list(database_name)
        while len(job_list) != 0:
            time.sleep(1)
            job_list = self.client.get_unfinish_load_job_list(database_name)

    def init_database(self):
        """init backup_test_db"""
        ret = self.client.show_databases(self.db_name)
        if len(ret) == 0:
            try:
                self.client.create_database(self.db_name)
                self.client.use(self.db_name)
                partition_name_list_d = ['partition_e', 'partition_b', 'partition_g', 'partition_h']
                partition_value_list_d = ['100', '500', '1000', 'MAXVALUE']
                partition_info_d = palo_client.PartitionInfo('k3',
                                                             partition_name_list_d,
                                                             partition_value_list_d)
                distribution_type_d = 'HASH(k1, k2, k5)'
                distribution_info_d = palo_client.DistributionInfo(distribution_type_d, 5)

                self.init_multi_partitions_table()
                self.init_single_partition_table()
                self.init_all_type_table()
                self.init_multi_partition_with_rollup(partition_info_d, distribution_info_d,
                                                      'rollup_in_table')
                self.wait_load(self.db_name)
                assert len(self.client.select_all(self.single_partition_table)) != 0
                assert len(self.client.select_all(self.table_with_rollup)) != 0
                assert len(self.client.select_all(self.multi_partitions_table)) != 0
                assert len(self.client.select_all(self.all_type_table)) != 0
            except Exception as e:
                self.client.clean(self.db_name)
                raise pytest.skip("init_database backup/restore case data failed")


