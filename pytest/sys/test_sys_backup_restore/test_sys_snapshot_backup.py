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
#   @file test_sys_backup.py
#   @date 2018-05-08 14:42:46
#   @brief backup
#
#############################################################################

"""
备份恢复测试
"""
import os
import sys
import time
import pytest

file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(file_dir)
file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../sys"))
sys.path.append(file_dir)
import backup_case
from data import partition as DATA
from lib import palo_config
from lib import palo_client
from lib import util
from lib import palo_job

LOG = palo_client.LOG
L = palo_client.L

config = palo_config.config
broker_name = config.broker_name
broker_property = config.broker_property
broker_info = palo_config.broker_info
repo_location = config.repo_location + '/backup_repo'


class TestBackup(object):
    """test backup class"""
    repo_name = 'repo_backup'
    db_name = 'backup_test_db'

    def setup_class(self):
        """setup class"""
        client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                        password=config.fe_password)
        ret = client.get_repository(self.repo_name)
        if ret:
            pass
        else:
            ret = client.create_repository(self.repo_name, broker_name, repo_location=repo_location,
                                           repo_properties=broker_property)
            assert ret
        self.base_case = backup_case.BackupBaseCase(client)
        self.client = client
        self.base_case.init_database()

    def test_backup_table(self):
        """
        {
        "title": "test_sys_snapshot_backup.test_backup_table",
        "describe": "创建DB，创建多分区表，导入数据，全表数据备份，备份完成后，执行恢复rename为新表，验证数据",
        "tag": "function,p1"
        }
        """
        """创建DB，创建多分区表，导入数据，全表数据备份，备份完成后，执行恢复rename为新表，验证数据"""
        database_name, table_name, index_name = util.gen_name_list()
        self.client.clean(database_name)
        self.client.create_database(database_name)
        self.client.use(database_name)
        self.base_case.init_multi_partitions_table()
        self.base_case.wait_load(database_name)
        table_name = self.base_case.multi_partitions_table
        assert self.client.verify(DATA.expected_data_file_list_1, table_name)
        self.client.wait_backup_job()
        self.client.wait_restore_job()
        snapshot_label = util.get_snapshot_label('backup_single_partition')
        ret = self.client.backup(snapshot_label=snapshot_label, backup_list=[table_name], 
                                 repo_name=self.repo_name, database_name=database_name, 
                                 is_wait=True)
        assert ret
        new_table_name = 'new_' + table_name
        restore_list = list()
        restore_list.append('%s AS %s' % (table_name, new_table_name))
        ret = self.client.restore(snapshot_name=snapshot_label, repo_name=self.repo_name,
                                  restore_list=restore_list, database_name=database_name, 
                                  is_wait=True)
        assert ret
        # check
        line1 = 'select * from %s order by k1' % table_name
        line2 = 'select * from %s order by k1' % new_table_name
        self.base_case.check2(line1, line2)
        self.client.drop_database(database_name)

    def test_backup_partitions(self):
        """
        {
        "title": "test_sys_snapshot_backup.test_backup_partitions",
        "describe": "创建DB，创建多分区表，导入数据，备份时指定某几个分区，备份完成后，执行恢复rename为新表，验证数据",
        "tag": "function,p1"
        }
        """
        """创建DB，创建多分区表，导入数据，备份时指定某几个分区，备份完成后，执行恢复rename为新表，验证数据"""
        database_name, table_name, index_name = util.gen_name_list()
        self.client.clean(database_name)
        self.client.create_database(database_name)
        self.client.use(database_name)
        self.base_case.init_multi_partitions_table()
        self.base_case.wait_load(database_name)
        table_name = self.base_case.multi_partitions_table
        assert self.client.verify(DATA.expected_data_file_list_1, table_name)
        self.client.wait_backup_job()
        self.client.wait_restore_job()
        snapshot_name = util.get_snapshot_label('backup_partitions_snapshot')
        ret = self.client.backup(snapshot_label=snapshot_name, 
                                backup_list=['%s PARTITION(partition_b, partition_g)' % table_name],
                                repo_name=self.repo_name, database_name=database_name, 
                                is_wait=True)
        assert ret
        new_table_name = 'new_' + table_name
        restore_list = list()
        restore_list.append('%s AS %s' % (table_name, new_table_name))
        ret = self.client.restore(snapshot_name=snapshot_name, repo_name=self.repo_name,
                                  restore_list=restore_list, database_name=database_name, 
                                  is_wait=True)
        assert ret
        # check
        line1 = 'select * from %s where k3 < 1000 and k3 >= 100 order by k1' % table_name
        line2 = 'select * from %s order by k1' % new_table_name
        self.base_case.check2(line1, line2)
        self.client.drop_database(database_name)

    def test_backup_all_type_table(self):
        """
        {
        "title": "test_sys_snapshot_backup.test_backup_all_type_table",
        "describe": "创建DB，创建单分区表和各种分区类型的多分区表，并导入数据，备份多个表，多个分区，show备份任务，备份完成后，进行恢复rename为新表，验证数据正确，可查询到本次备份",
        "tag": "function,p1"
        }
        """
        """
        创建DB，创建单分区表和各种分区类型的多分区表，并导入数据，备份多个表，多个分区，
        show备份任务，备份完成后，进行恢复rename为新表，验证数据正确，可查询到本次备份
        """
        database_name, table_name, index_name = util.gen_name_list()
        self.client.clean(database_name)
        self.client.create_database(database_name)
        self.client.use(database_name)
        partition_name_list_d = ['partition_e', 'partition_b', 'partition_g', 'partition_h']
        partition_value_list_d = ['100', '500', '1000', 'MAXVALUE']
        partition_info_d = palo_client.PartitionInfo('k3',
                                                     partition_name_list_d, partition_value_list_d)
        distribution_type_d = 'HASH(k1, k2, k5)'
        distribution_info_d = palo_client.DistributionInfo(distribution_type_d, 5)

        self.base_case.init_multi_partitions_table()
        self.base_case.init_single_partition_table()
        self.base_case.init_all_type_table()
        self.base_case.init_multi_partition_with_rollup(partition_info_d, distribution_info_d, index_name)
        self.base_case.wait_load(database_name)
        table_name = self.base_case.all_type_table
        # assert self.client.verify(DATA.expected_data_file_list_2, table_name, encoding='utf8')
        self.client.wait_backup_job()
        self.client.wait_restore_job()
        backup_list = list()
        backup_list.append(self.base_case.single_partition_table)
        backup_list.append('%s PARTITION(partition_b)' % self.base_case.table_with_rollup)
        backup_list.append('%s PARTITION(partition_e, partition_g)' % self.base_case.multi_partitions_table)
        backup_list.append(self.base_case.all_type_table)
        snapshot_label = util.get_snapshot_label('all_type_table')
        ret = self.client.backup(snapshot_label=snapshot_label, backup_list=backup_list, 
                                 repo_name=self.repo_name, database_name=database_name, 
                                 is_wait=True)
        assert ret

        restore_list = list()
        restore_list.append('{tb_name} as {tb_name}_new'.format(tb_name=self.base_case.all_type_table))
        restore_list.append('{tb_name} as {tb_name}_new'.format(tb_name=self.base_case.single_partition_table))
        restore_list.append('{tb_name} as {tb_name}_new'.format(tb_name=self.base_case.multi_partitions_table))
        restore_list.append('{tb_name} as {tb_name}_new'.format(tb_name=self.base_case.table_with_rollup))

        ret = self.client.restore(snapshot_name=snapshot_label, repo_name=self.repo_name,
                                  restore_list=restore_list, is_wait=True,
                                  database_name=database_name)
        assert ret
        # check
        line1 = 'select * from %s order by k1' % self.base_case.single_partition_table
        line2 = 'select * from %s_new order by k1' % self.base_case.single_partition_table
        self.base_case.check2(line1, line2)

        line1 = 'select * from %s order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14' \
                % self.base_case.all_type_table
        line2 = 'select * from %s_new order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14' \
                % self.base_case.all_type_table
        self.base_case.check2(line1, line2)

        line1 = 'select * from %s where k3 < 100 or (k3 >= 500 and k3 < 1000) order by k1' \
                % self.base_case.multi_partitions_table
        line2 = 'select * from %s_new order by k1' % self.base_case.multi_partitions_table
        self.base_case.check2(line1, line2)

        line1 = 'select * from %s where k3 >= 100 and k3 < 500 order by k1' % self.base_case.table_with_rollup
        line2 = 'select * from %s_new order by k1' % self.base_case.table_with_rollup
        self.base_case.check2(line1, line2)
        self.client.clean(database_name)

    def test_backup_rollup_table(self):
        """
        {
        "title": "test_sys_snapshot_backup.test_backup_rollup_table",
        "describe": " 创建DB，创建单分区表和各种分区类型的多分区表，并导入数据，对每个表都创建rollup，执行备份操作",
        "tag": "function,p1"
        }
        """
        """
        创建DB，创建单分区表和各种分区类型的多分区表，并导入数据，对每个表都创建rollup，执行备份操作，
        备份完成后，执行恢复rename为新表，验证数据及rollup。
        """
        database_name, table_name, index_name = util.gen_name_list()
        self.client.clean(database_name)
        self.client.create_database(database_name)
        self.client.use(database_name)

        partition_name_list_d = ['partition_e', 'partition_b', 'partition_g', 'partition_h']
        partition_value_list_d = ['100', '500', '1000', 'MAXVALUE']
        partition_info_d = palo_client.PartitionInfo('k3',
                                                     partition_name_list_d, partition_value_list_d)
        distribution_type_d = 'HASH(k1, k2, k5)'
        distribution_info_d = palo_client.DistributionInfo(distribution_type_d, 31)

        self.base_case.init_multi_partition_with_rollup(partition_info_d, distribution_info_d, index_name)
        self.base_case.wait_load(database_name)
        table_name = self.base_case.table_with_rollup
        assert self.client.verify(DATA.expected_data_file_list_1, table_name)
        self.client.wait_backup_job()
        self.client.wait_restore_job()
        snapshot_label = util.get_snapshot_label('backup_rollup_table')
        ret = self.client.backup(snapshot_label=snapshot_label, backup_list=[table_name], 
                                 repo_name=self.repo_name, database_name=database_name, 
                                 is_wait=True)
        assert ret
        new_table_name = 'new_' + table_name
        restore_list = list()
        restore_list.append('%s AS %s' % (table_name, new_table_name))
        ret = self.client.restore(snapshot_name=snapshot_label, repo_name=self.repo_name,
                                  restore_list=restore_list, database_name=database_name, 
                                  is_wait=True)
        assert ret
        # check
        line1 = 'select * from %s order by k1' % table_name
        line2 = 'select * from %s order by k1' % new_table_name
        self.base_case.check2(line1, line2)
        ret = self.client.get_index_schema(new_table_name, index_name)
        assert ret
        self.client.drop_database(database_name)

    def test_backup_while_load(self):
        """
        {
        "title": "test_sys_snapshot_backup.test_backup_while_load",
        "describe": "初始化数据，在数据导入时，执行备份操作，备份任务正确执行，数据恢复正确",
        "tag": "system,stability,p1"
        }
        """
        """
        初始化数据，在数据导入时，执行备份操作，备份任务正确执行，数据恢复正确
        数据导入完成时，数据仍旧在执行备份操作
        """
        self.client.use(self.db_name)
        self.client.wait_backup_job()
        self.client.wait_restore_job()
        table_name = 'backup_while_load_table'
        self.client.execute('drop table if exists %s' % table_name)
        partition_name_list_d = ['partition_e', 'partition_b', 'partition_g', 'partition_h']
        partition_value_list_d = ['100', '500', '1000', 'MAXVALUE']
        partition_info_d = palo_client.PartitionInfo('k3',
                                                     partition_name_list_d, partition_value_list_d)
        label = self.base_case.init_partition_table(table_name, partition_info_d)
        assert label
        state = self.client.get_load_job_state(label, self.db_name)
        if state == 'CANCELLED' or state == 'FINISHED':
            raise pytest.skip('load already finished')
        snapshot_label = util.get_snapshot_label('backup_while_load')
        ret = self.client.backup(snapshot_label=snapshot_label, backup_list=[table_name], 
                                 repo_name=self.repo_name,
                                 database_name=self.db_name, is_wait=True)
        assert ret
        restore_list = list()
        new_table_name = '%s_new' % table_name
        self.client.execute("DROP TABLE IF EXISTS %s" % new_table_name)
        restore_list.append('%s AS %s' % (table_name, new_table_name))
        ret = self.client.restore(snapshot_name=snapshot_label, repo_name=self.repo_name,
                                  restore_list=restore_list, database_name=self.db_name, 
                                  is_wait=True)
        assert ret
        # 结果可能为空，也可能为导入后的数据
        line1 = 'select * from %s order by k1' % table_name
        line2 = 'select * from %s order by k1' % new_table_name
        try:
            self.base_case.check2(line1, line2)
        except Exception as e:
            ret = self.client.execute(line2)
            assert ret == ()

    def test_backup_while_schema_change(self):
        """
        {
        "title": "test_sys_snapshot_backup.test_backup_while_schema_change",
        "describe": "初始化数据，在shema change任务执行过程中，执行备份操作，备份任务正确执行，数据恢复正确",
        "tag": "system,stability,p1"
        }
        """
        """初始化数据，在shema change任务执行过程中，执行备份操作，备份任务正确执行，数据恢复正确"""
        self.client.use(self.db_name)
        self.client.wait_backup_job()
        self.client.wait_restore_job()
        table_name = 'backup_while_sc_table'
        self.client.execute("drop table if exists %s" % table_name)
        partition_name_list_d = ['partition_e', 'partition_b', 'partition_g', 'partition_h']
        partition_value_list_d = ['100', '500', '1000', 'MAXVALUE']
        partition_info_d = palo_client.PartitionInfo('k3',
                                                     partition_name_list_d, partition_value_list_d)
        label = self.base_case.init_partition_table(table_name, partition_info_d)
        assert label
        self.base_case.wait_load(self.db_name)
        line1 = 'select * from %s order by k1' % table_name
        ret1 = self.client.execute(line1)
        ret = self.client.schema_change_add_column(table_name, 
                                                   [('v_add', 'char(6)', 'replace', 'HELLO')])
        assert ret
        job_info = self.client.get_table_schema_change_job_list(table_name, self.db_name)
        # assert len(job_info) == 1
        state = palo_job.SchemaChangeJob(job_info[-1]).get_state()
        if state == 'CANCELLED' or state == 'FINISHED':
            raise pytest.skip('schema_change already finished')
        snapshot_label = util.get_snapshot_label('backup_while_sc')
        ret = self.client.backup(snapshot_label=snapshot_label, backup_list=[table_name], 
                                 repo_name=self.repo_name,
                                 database_name=self.db_name, is_wait=True)
        assert ret
        restore_list = list()
        new_table_name = '%s_new' % table_name
        self.client.execute("drop table if exists %s" % new_table_name)
        restore_list.append('%s AS %s' % (table_name, new_table_name))
        ret = self.client.restore(snapshot_name=snapshot_label, repo_name=self.repo_name,
                                  restore_list=restore_list, database_name=self.db_name, 
                                  is_wait=True)
        assert ret
        # check
        line2 = 'select * from %s order by k1' % new_table_name
        ret2 = self.client.execute(line2)
        util.check(ret1, ret2)

    def test_backup_while_rollup(self):
        """
        {
        "title": "test_sys_snapshot_backup.test_backup_while_rollup",
        "describe": "初始化数据，在rollup任务执行过程中，执行备份操作，备份任务正确执行，数据恢复正确",
        "tag": "system,stability,p1"
        }
        """
        """初始化数据，在rollup任务执行过程中，执行备份操作，备份任务正确执行，数据恢复正确"""
        self.client.use(self.db_name)
        self.client.wait_backup_job()
        self.client.wait_restore_job()
        table_name = 'backup_while_rollup_table'
        index_name = 'backup_while_rollup_index'
        self.client.execute("drop table if exists %s" % table_name)
        partition_name_list_d = ['partition_e', 'partition_b', 'partition_g', 'partition_h']
        partition_value_list_d = ['100', '500', '1000', 'MAXVALUE']
        partition_info_d = palo_client.PartitionInfo('k3',
                                                     partition_name_list_d, partition_value_list_d)
        label = self.base_case.init_partition_table(table_name, partition_info_d)
        assert label
        self.base_case.wait_load(self.db_name)
        ret = self.client.create_rollup_table(table_name, index_name, ['k1', 'k4', 'v6'])
        assert ret
        job_info = self.client.get_table_rollup_job_list(table_name, self.db_name)
        # assert len(job_info) == 1
        state = palo_job.RollupJob(job_info[-1]).get_state()
        if state == 'CANCELLED' or state == 'FINISHED':
            raise pytest.skip('rollup already finished')
        snapshot_label = util.get_snapshot_label('backup_while_rollup')
        ret = self.client.execute('SHOW BACKUP')
        print(ret)
        LOG.info(L('SHOW BACKUP.', database_name=self.db_name))
        ret = self.client.execute('SHOW RESTORE')
        print(ret)
        LOG.info(L('SHOW RESTORE.', database_name=self.db_name))

        ret = self.client.backup(snapshot_label=snapshot_label, backup_list=[table_name], 
                                 repo_name=self.repo_name,
                                 database_name=self.db_name, is_wait=True)
        assert ret
        restore_list = list()
        new_table_name = '%s_new' % table_name
        self.client.execute("DROP TABLE IF EXISTS %s" % new_table_name)
        restore_list.append('%s AS %s' % (table_name, new_table_name))
        ret = self.client.restore(snapshot_name=snapshot_label, repo_name=self.repo_name,
                                  restore_list=restore_list, database_name=self.db_name, 
                                  is_wait=True)
        assert ret
        # check
        line1 = 'select * from %s order by k1' % table_name
        line2 = 'select * from %s order by k1' % new_table_name
        self.base_case.check2(line1, line2)

    def test_backup_while_restore(self):
        """
        {
        "title": "test_sys_snapshot_backup.test_backup_while_restore",
        "describe": "初始化数据，在恢复任务执行过程中，执行备份操作，备份任务失败",
        "tag": "function,fuzz,p1"
        }
        """
        """初始化数据，在恢复任务执行过程中，执行备份操作，备份任务失败"""
        self.client.use(self.db_name)
        self.client.wait_backup_job()
        self.client.wait_restore_job()
        table_name = self.base_case.single_partition_table
        assert self.client.verify(DATA.expected_data_file_list_1, table_name)
        snapshot_label = util.get_snapshot_label('backup_while_restore')
        ret = self.client.backup(snapshot_label=snapshot_label, backup_list=[table_name], 
                                 repo_name=self.repo_name, database_name=self.db_name, 
                                 is_wait=True)
        assert ret
        new_table_name = 'new_' + table_name
        restore_list = list()
        restore_list.append('%s AS %s' % (table_name, new_table_name))
        ret = self.client.restore(snapshot_name=snapshot_label, repo_name=self.repo_name,
                                  restore_list=restore_list, database_name=self.db_name)
        assert ret
        job_info = self.client.show_restore(self.db_name)
        # assert len(job_info) == 1
        # show restore支持返回多个restore任务
        # state = palo_job.RestoreJob(job_info[-1]).get_state()
        state = util.get_attr_condition_value(job_info, palo_job.RestoreJob.Label,
                                              snapshot_label, palo_job.RestoreJob.State)
        if state == 'FINISHED' or state == 'CANCELLED':
            raise pytest.skip('restore already finished')
        snapshot_label = util.get_snapshot_label('backup_while_restore')
        ret = self.client.backup(snapshot_label=snapshot_label, backup_list=[new_table_name], 
                                 repo_name=self.repo_name,
                                 database_name=self.db_name, is_wait=True)
        assert not ret
        ret = self.client.backup(snapshot_label=snapshot_label, backup_list=[table_name],
                                 repo_name=self.repo_name, database_name=self.db_name, is_wait=True)

        assert not ret

    @pytest.mark.skip()
    def test_backup_while_backup_diff_db(self):
        """
        {
        "title": "test_sys_snapshot_backup.test_backup_while_backup_diff_db",
        "describe": "初始化数据，不同DB中，DB1中执行备份同时DB2中执行备份，验证备份任务均执行成功",
        "tag": "autotest"
        }
        """
        """初始化数据，不同DB中，DB1中执行备份同时DB2中执行备份，验证备份任务均执行成功"""
        pass

    def test_backup_while_backup_same_db(self):
        """
        {
        "title": "test_sys_snapshot_backup.test_backup_while_backup_same_db",
        "describe": "初始化数据，一个DB中，备份sting_partition_table，在执行过程中，再次备份sting_partition_table，验证执行失败",
        "tag": "function,p1,fuzz"
        }
        """
        """
        初始化数据，一个DB中，备份sting_partition_table，在执行过程中，再次备份sting_partition_table，验证执行失败；
        在备份sting_partition_table的过程中备份table_with_rollup验证table_with_rollup备份失败
        """
        self.client.use(self.db_name)
        self.client.wait_backup_job()
        self.client.wait_restore_job()
        table_name = self.base_case.single_partition_table
        assert self.client.verify(DATA.expected_data_file_list_1, table_name)
        snapshot_label = util.get_snapshot_label('backup_same_db_1')
        ret = self.client.backup(snapshot_label=snapshot_label, backup_list=[table_name],
                                 repo_name=self.repo_name, database_name=self.db_name)
        assert ret
        table_name = self.base_case.multi_partitions_table
        snapshot_label = util.get_snapshot_label('backup_same_db_2')
        ret = self.client.backup(snapshot_label=snapshot_label, backup_list=[table_name], 
                                 repo_name=self.repo_name, database_name=self.db_name)
        assert not ret
        assert self.client.wait_backup_job()

    def test_cancel_backup_pending(self):
        """
        {
        "title": "test_sys_snapshot_backup.test_cancel_backup_pending",
        "describe": "初始化数据并备份，执行恢复操作，在恢复操作的各个阶段执行cancel，验证任务cancel成功",
        "tag": "function,p1"
        }
        """
        """初始化数据并备份，执行恢复操作，在恢复操作的各个阶段执行cancel，验证任务cancel成功"""
        assert self._cancel_backup_while_state('PENDING')

    def test_cancel_backup_snapshoting(self):
        """
        {
        "title": "test_sys_snapshot_backup.test_cancel_backup_snapshoting",
        "describe": "cancel backup while snaphshoting",
        "tag": "function,p1"
        }
        """
        """cancel backup while snaphshoting"""
        assert self._cancel_backup_while_state('SNAPSHOTING')

    def test_cancel_backup_upload_snapshot(self):
        """
        {
        "title": "test_sys_snapshot_backup.test_cancel_backup_upload_snapshot",
        "describe": "cancel backup while upload_snapshot",
        "tag": "function,p1"
        }
        """
        """cancel backup while upload_snapshot"""
        assert self._cancel_backup_while_state('UPLOAD_SNAPSHOT')

    def test_cancel_backup_uploading(self):
        """
        {
        "title": "test_sys_snapshot_backup.test_cancel_backup_uploading",
        "describe": "test cancel backup while uploading",
        "tag": "function,p1"
        }
        """
        """test cancel backup while uploading"""
        assert self._cancel_backup_while_state('UPLOADING')

    def test_cancel_backup_save_meta(self):
        """
        {
        "title": "test_sys_snapshot_backup.test_cancel_backup_save_meta",
        "describe": "test cancel backup while save_meta",
        "tag": "function,p1"
        }
        """
        """test cancel backup while save_meta"""
        assert self._cancel_backup_while_state('SAVE_META')
    
    def test_cancel_backup_upload_info(self):
        """
        {
        "title": "test_sys_snapshot_backup.test_cancel_backup_upload_info",
        "describe": "test cancel backup while upload_info",
        "tag": "function,p1"
        }
        """
        """test cancel backup while upload_info"""
        assert self._cancel_backup_while_state('UPLOAD_INFO')

    def _cancel_backup_while_state(self, state):
        """"""
        self.client.use(self.db_name)
        self.client.wait_backup_job()
        self.client.wait_restore_job()
        table_name = self.base_case.table_with_rollup
        assert self.client.verify(DATA.expected_data_file_list_1, table_name)
        snapshot_label = util.get_snapshot_label('backup_cancel_' + state)
        ret = self.client.backup(snapshot_label=snapshot_label, backup_list=[table_name], 
                                 repo_name=self.repo_name, database_name=self.db_name)
        assert ret
        s = None
        flag = False
        while s != 'FINISHED' and s != 'CANCELLED':
            job_info = self.client.show_backup(self.db_name)
            s = palo_job.BackupJob(job_info[-1]).get_state()
            if s == state:
                flag = True
                ret = self.client.cancel_backup(self.db_name)
                assert ret
                job_info = self.client.show_backup(self.db_name)
                s = palo_job.BackupJob(job_info[-1]).get_state()
                assert s == 'CANCELLED'
                return True
            time.sleep(1)
        if flag is False:
            raise pytest.skip('can not get backup state')

    def test_backup_show_info(self):
        """
        {
        "title": "test_sys_snapshot_backup.test_backup_show_info",
        "describe": "初始化数据，多次执行备份操作，验证能够查到备份操作的历史信息show snapshot",
        "tag": "function,p1"
        }
        """
        """初始化数据，多次执行备份操作，验证能够查到备份操作的历史信息show snapshot"""
        ret = self.client.show_snapshot(self.repo_name)
        if len(ret) == 0:
            raise pytest.skip('have no snapshot')
        snapshot_info = palo_job.SnapshotInfo(ret[-1])
        ret = self.client.show_snapshot(self.repo_name, snapshot=snapshot_info.get_snapshot_name(),
                                        timestamp=snapshot_info.get_timestamp())
        assert ret

    @pytest.mark.skip()
    def test_delete_snapshot(self):
        """
        {
        "title": "test_sys_snapshot_backup.test_delete_snapshot",
        "describe": "todo:初始化数据，进行备份，完成后，执行备份删除，验证删除成功，show。",
        "tag": "autotest"
        }
        """
        """初始化数据，进行备份，完成后，执行备份删除，验证删除成功，show。"""
        # todo, can not support now
        pass

    def test_backup_privilege(self):
        """
        {
        "title": "test_sys_snapshot_backup.test_backup_privilege",
        "describe": "读写权限/只读权限用户，分别执行备份操作，验证备份成功。可查看到备份历史",
        "tag": "function,p1,fuzz"
        }
        """
        """
        读写权限/只读权限用户，分别执行备份操作，验证备份成功。可查看到备份历史。
        执行备份删除，只读权限用户和读写权限用户删除失败，root用户删除备份成功
        note: 只读用户没有backup权限
        """
        read_user = 'read_user'
        write_user = 'write_user'
        try:
            self.client.drop_user(read_user)
            self.client.drop_user(write_user)
        except Exception as e:
            pass
        self.client.create_user(read_user, read_user)
        self.client.create_user(write_user, write_user)
        self.client.grant(read_user, 'READ_ONLY', self.db_name)
        self.client.grant(write_user, 'CREATE_PRIV', self.db_name)
        read_client = palo_client.get_client(config.fe_host, config.fe_query_port, user=read_user, 
                                             password=read_user, database_name=self.db_name)
        write_client = palo_client.get_client(config.fe_host, config.fe_query_port, 
                                              user=write_user, password=write_user, 
                                              database_name=self.db_name)
        read_snapshot_label = util.get_snapshot_label('read_snapshot_label')
        ret = read_client.backup(snapshot_label=read_snapshot_label, 
                                 backup_list=[self.base_case.table_with_rollup], repo_name=self.repo_name,
                                 database_name=self.db_name, is_wait=True)
        assert not ret
        write_snapshot_label = util.get_snapshot_label('write_snapshot_label')
        ret = write_client.backup(snapshot_label=write_snapshot_label, 
                                  backup_list=[self.base_case.table_with_rollup], repo_name=self.repo_name,
                                  database_name=self.db_name, is_wait=True)
        assert not ret

    @pytest.mark.skip()
    def test_backup_timeout(self):
        """
        {
        "title": "test_sys_snapshot_backup.test_backup_timeout",
        "describe": "初始化数据，设置备份任务的timeout时间，执行备份，验证备份任务执行超过timeout时间后，执行失败。",
        "tag": "autotest"
        }
        """
        """初始化数据，设置备份任务的timeout时间，执行备份，验证备份任务执行超过timeout时间后，执行失败。
        timeout must be at least 10 min
        todo
        """
        self.client.use(self.db_name)
        table_name = self.base_case.table_with_rollup
        snapshot_label = util.get_snapshot_label('backup_timeout')
        ret = self.client.backup(snapshot_label=snapshot_label, backup_list=[table_name], 
                                 repo_name=self.repo_name,
                                 database_name=self.db_name, is_wait=True, timeout=5)
        assert not ret
        ret = self.client.backup(snapshot_label=snapshot_label, backup_list=[table_name], 
                                 repo_name=self.repo_name, 
                                 database_name=self.db_name, is_wait=True)
        assert ret

    @pytest.mark.skip()
    def test_backup_auto(self):
        """
        {
        "title": "test_sys_snapshot_backup.test_backup_auto",
        "describe": "自动备份 Todo",
        "tag": "autotest"
        }
        """
        """自动备份 Todo"""
        # todo can not support now
        pass

    @pytest.mark.skip()
    def test_backup_when_space_not_enough(self):
        """
        {
        "title": "test_sys_snapshot_backup.test_backup_when_space_not_enough",
        "describe": "HDFS中存储空间不足时,备份失败",
        "tag": "autotest"
        }
        """
        """HDFS中存储空间不足时,备份失败"""
        # todo
        pass

    @pytest.mark.skip()
    def test_wrong_backup_sql(self):
        """
        {
        "title": "test_sys_snapshot_backup.test_wrong_backup_sql",
        "describe": "错误的backup",
        "tag": "autotest"
        }
        """
        """错误的backup sql
        BACKUP SNAPSHOT example_db.snapshot_label2
        TO example_repo
        ON
        (
            example_tbl PARTITION (p1,p2),
            example_tbl2
        ) PROPERTIES ("type" = "full", "timeout" = "3600");
        """
        pass


