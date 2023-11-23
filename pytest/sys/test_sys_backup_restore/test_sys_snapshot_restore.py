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
#   @file test_sys_restore.py
#   @date 2018-05-08 14:42:27
#   @brief restore
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

from data import partition as DATA
from lib import palo_config
from lib import palo_client
from lib import util
from lib import palo_job
import backup_case

LOG = palo_client.LOG
L = palo_client.L

config = palo_config.config
broker_name = config.broker_name
broker_property = config.broker_property
broker_info = palo_config.broker_info
repo_location = config.repo_location + '/restore_repo'

class TestRestore(object):
    """test restore class"""
    repo_name = 'repo_restore'
    db_name = 'restore_test_db'
    snapshot_name = util.get_snapshot_label('restore_test_snapshot')

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
        self.init_database()

    def init_database(self):
        """init restore_test_db"""
        self.base_case.init_database()
        self.client.use(self.db_name)
        ret = self.client.show_snapshot(self.repo_name, self.snapshot_name)
        status = ret[0][palo_job.SnapshotInfo.Status]
        LOG.info(L('', status=status))
        if "OK" not in str(status):
            backup_list = [self.base_case.single_partition_table, self.base_case.multi_partitions_table,
                           self.base_case.table_with_rollup, self.base_case.all_type_table]
            ret = self.client.backup(self.snapshot_name, backup_list, repo_name=self.repo_name, 
                                     database_name=self.db_name, is_wait=True)
            assert ret
            
    def tearDown(self):
        """tear down"""
        print("tearDown")
        print(self.client)
        del self.client

    def test_restore_dropped_table(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_restore_dropped_table",
        "describe": "初始化数据并备份table1，drop table1，恢复表table1，验证恢复成功，数据正确",
        "tag": "system,p1"
        }
        """
        """初始化数据并备份table1，drop table1，恢复表table1，验证恢复成功，数据正确"""
        db_name, table_name, index_name = util.gen_name_list()
        self.client.clean(db_name)
        self.client.create_database(db_name)
        self.client.use(db_name)
        self.base_case.init_multi_partitions_table()
        self.base_case.wait_load(db_name)
        sql = 'select * from %s order by k1' % self.base_case.multi_partitions_table
        r1 = self.client.execute(sql)
        snapshot_name = util.get_snapshot_label('restore_dropped_table_snapshot')
        ret = self.client.backup(snapshot_name, [self.base_case.multi_partitions_table],
                                 repo_name=self.repo_name, database_name=db_name, is_wait=True)
        assert ret
        self.client.drop_table(self.base_case.multi_partitions_table)
        ret = self.client.restore(snapshot_name, self.repo_name, [self.base_case.multi_partitions_table],
                                  database_name=db_name, is_wait=True)
        assert ret
        r2 = self.client.execute(sql)
        util.check(r1, r2)

    def test_restore_partitions(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_restore_partitions",
        "describe": "初始化数据并备份table1，drop table1，恢复表table1中的某几个分区，验证恢复成功，数据正确",
        "tag": "system,p1"
        }
        """
        """初始化数据并备份table1，drop table1，恢复表table1中的某几个分区，验证恢复成功，数据正确"""
        db_name, table_name, index_name = util.gen_name_list()
        self.client.clean(db_name)
        self.client.create_database(db_name)
        self.client.use(db_name)
        self.base_case.init_multi_partitions_table()
        self.base_case.wait_load(db_name)
        sql = 'select * from %s partition (partition_e, partition_g) order by k1' \
              % self.base_case.multi_partitions_table
        r1 = self.client.execute(sql)
        snapshot_name = util.get_snapshot_label('restore_partition_snapshot')
        ret = self.client.backup(snapshot_name, [self.base_case.multi_partitions_table],
                                 repo_name=self.repo_name, database_name=db_name, is_wait=True)
        assert ret
        self.client.drop_table(self.base_case.multi_partitions_table)
        restore_list = ['%s partition(partition_e, partition_g)' % self.base_case.multi_partitions_table]
        ret = self.client.restore(snapshot_name, self.repo_name, restore_list, 
                                  database_name=db_name, is_wait=True)
        assert ret
        sql = 'select * from %s order by k1' % self.base_case.multi_partitions_table
        r2 = self.client.execute(sql)
        util.check(r1, r2)
        self.client.clean(db_name)

    @pytest.mark.skip()
    def test_restore_table_rename(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_restore_table_rename",
        "describe": "todo 初始化数据并备份table1，恢复并将表rename 为table2，验证恢复正成功，数据正确",
        "tag": "autotest"
        }
        """
        """初始化数据并备份table1，恢复并将表rename 为table2，验证恢复正成功，数据正确"""
        pass

    def test_restore_tables(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_restore_tables",
        "describe": "初始化数据，恢复一个db中的所有表，验证恢复正确,各种类型的表",
        "tag": "system,p1"
        }
        """
        """初始化数据，恢复一个db中的所有表，验证恢复正确,各种类型的表"""

        db_name, table_name, index_name = util.gen_name_list()
        self.client.clean(db_name)
        self.client.create_database(db_name)
        self.client.use(db_name)
        snapshot_name = self.snapshot_name
        restore_list = [self.base_case.single_partition_table, self.base_case.multi_partitions_table,
                        self.base_case.table_with_rollup, self.base_case.all_type_table]
        ret = self.client.restore(snapshot_name, self.repo_name, restore_list, 
                                  database_name=db_name, is_wait=True)
        assert ret
        sql = 'select * from {db}.{tb} order by k1'
        line1 = sql.format(db=self.db_name, tb=self.base_case.table_with_rollup)
        line2 = sql.format(db=db_name, tb=self.base_case.table_with_rollup)
        self.base_case.check2(line1, line2)
        line1 = sql.format(db=self.db_name, tb=self.base_case.single_partition_table)
        line2 = sql.format(db=db_name, tb=self.base_case.single_partition_table)
        self.base_case.check2(line1, line2)
        line1 = sql.format(db=self.db_name, tb=self.base_case.multi_partitions_table)
        line2 = sql.format(db=db_name, tb=self.base_case.multi_partitions_table)
        self.base_case.check2(line1, line2)
        sql = 'select * from {db}.{tb} order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,\
               20,21,22,23,24,25,26,27,28,29,30'
        line1 = sql.format(db=self.db_name, tb=self.base_case.all_type_table)
        line2 = sql.format(db=db_name, tb=self.base_case.all_type_table)
        self.base_case.check2(line1, line2)
        self.client.clean(db_name)

    def test_restore_after_load_and_delete(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_restore_after_load_and_delete",
        "describe": "初始化数据并备份table1，对table1进行数据导入和删除，恢复表中的所有分区，验证恢复正确，数据正确",
        "tag": "system,p1"
        }
        """
        """初始化数据并备份table1，对table1进行数据导入和删除，恢复表中的所有分区，验证恢复正确，数据正确"""
        db_name, table_name, index_name = util.gen_name_list()
        self.client.clean(db_name)
        self.client.create_database(db_name)
        self.client.use(db_name)
        self.base_case.init_multi_partitions_table()
        self.base_case.wait_load(db_name)
        sql = 'select * from %s order by k1' % self.base_case.multi_partitions_table
        r1 = self.client.execute(sql)
        snapshot_name = util.get_snapshot_label('restore_op_table_snapshot')
        ret = self.client.backup(snapshot_name, [self.base_case.multi_partitions_table],
                                 repo_name=self.repo_name, database_name=db_name, is_wait=True)
        assert ret
        sql = 'insert into %s select k1 + 100, k2, k3, k4, k5, v1, v2, v3, v4, v5, v6 from %s.%s' \
              % (self.base_case.multi_partitions_table, self.db_name, self.base_case.single_partition_table)
        self.client.execute(sql)
        time.sleep(20)
        self.base_case.wait_load(db_name)
        ret = self.client.restore(snapshot_name, self.repo_name, [self.base_case.multi_partitions_table],
                                  db_name, is_wait=True)
        assert ret
        sql = 'select * from %s order by k1' % self.base_case.multi_partitions_table
        r2 = self.client.execute(sql)
        util.check(r1, r2)

        sql = 'delete from %s partition partition_b where k1 > 10' % self.base_case.multi_partitions_table
        self.client.execute(sql)
        flag = True
        while flag:
            ret = self.client.show_delete(db_name)
            state = palo_job.DeleteJob(ret[-1]).get_state()
            if state == 'FINISHED':
                flag = False
            elif state == 'CANCELLED':
                raise pytest.skip('delete failed, pass')
            else:
                time.sleep(1)
        ret = self.client.restore(snapshot_name, self.repo_name, [self.base_case.multi_partitions_table],
                                  db_name, is_wait=True)
        assert ret
        sql = 'select * from %s order by k1' % self.base_case.multi_partitions_table
        r2 = self.client.execute(sql)
        util.check(r1, r2)
        self.client.clean(db_name)

    def test_restore_after_drop_partition_1(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_restore_after_drop_partition_1",
        "describe": "初始化数据并备份table1（partition a,b,c,d,e），drop partition c,d，恢复partition c, d，验证执行成功，数据正确",
        "tag": "system,p1"
        }
        """
        """
        初始化数据并备份table1（partition a,b,c,d,e），drop partition c,d，
        恢复partition c, d，验证执行成功，数据正确
        """
        db_name, table_name, index_name = util.gen_name_list()
        self.client.clean(db_name)
        self.client.create_database(db_name)
        self.client.use(db_name)

        partition_name_list_d = ['a', 'b', 'c', 'd', 'e']
        partition_value_list_d = ['0', '10', '20', '30', '40']
        partition_info_d = palo_client.PartitionInfo('k1',
                                                     partition_name_list_d, partition_value_list_d)

        self.base_case.init_partition_table(table_name, partition_info_d)
        self.base_case.wait_load(db_name)
        sql = 'select * from %s order by k1' % table_name
        r1 = self.client.execute(sql)
        snapshot_name = util.get_snapshot_label('restore_dropped_p_snapshot')
        ret = self.client.backup(snapshot_name, [table_name], repo_name=self.repo_name,
                                 database_name=db_name, is_wait=True)
        assert ret
        assert self.client.drop_partition(table_name, 'b', db_name)
        assert self.client.drop_partition(table_name, 'd', db_name)
        restore_list = ['%s partition (b, d)' % table_name]
        ret = self.client.restore(snapshot_name, self.repo_name, restore_list, db_name, 
                                  is_wait=True)
        assert ret
        r2 = self.client.execute(sql)
        util.check(r1, r2)
        self.client.clean(db_name)

    def test_restore_after_drop_partition_2(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_restore_after_drop_partition_2",
        "describe": "初始化数据并备份table1（partition a,b,c,d,e），drop partition c,d，恢复partition b，c，d，e，验证恢复成功",
        "tag": "system,p1"
        }
        """
        """
        初始化数据并备份table1（partition a,b,c,d,e），drop partition c,d，
        恢复partition b，c，d，e，验证恢复成功
        """
        db_name, table_name, index_name = util.gen_name_list()
        self.client.clean(db_name)
        self.client.create_database(db_name)
        self.client.use(db_name)

        partition_name_list_d = ['a', 'b', 'c', 'd', 'e']
        partition_value_list_d = ['0', '10', '20', '30', '40']
        partition_info_d = palo_client.PartitionInfo('k1',
                                                     partition_name_list_d, partition_value_list_d)

        self.base_case.init_partition_table(table_name, partition_info_d)
        self.base_case.wait_load(db_name)
        sql = 'select * from %s order by k1' % table_name
        r1 = self.client.execute(sql)
        snapshot_name = util.get_snapshot_label('restore_dropped_2p_snapshot')
        ret = self.client.backup(snapshot_name, [table_name], repo_name=self.repo_name,
                                 database_name=db_name, is_wait=True)
        assert ret
        assert self.client.drop_partition(table_name, 'b', db_name)
        assert self.client.drop_partition(table_name, 'd', db_name)
        restore_list = ['%s partition (b, d, c, e)' % table_name]
        ret = self.client.restore(snapshot_name, self.repo_name, restore_list, db_name,
                                  is_wait=True)
        assert ret
        r2 = self.client.execute(sql)
        util.check(r1, r2)
        self.client.clean(db_name)

    def test_restore_after_drop_partition_3(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_restore_after_drop_partition_3",
        "describe": "初始化数据并备份table1（partition a,b,c,d,e），drop partition d，e，新建parition f,f覆盖分区e和d的范围，执行恢复并验证恢复失败",
        "tag": "system,p1,fuzz"
        }
        """
        """
        初始化数据并备份table1（partition a,b,c,d,e），drop partition d，e，
        新建parition f,f覆盖分区e和d的范围，执行恢复并验证恢复失败
        """
        db_name, table_name, index_name = util.gen_name_list()
        self.client.clean(db_name)
        self.client.create_database(db_name)
        self.client.use(db_name)

        partition_name_list_d = ['a', 'b', 'c', 'd', 'e']
        partition_value_list_d = ['0', '10', '20', '30', '40']
        partition_info_d = palo_client.PartitionInfo('k1',
                                                     partition_name_list_d, partition_value_list_d)

        self.base_case.init_partition_table(table_name, partition_info_d)
        self.base_case.wait_load(db_name)
        snapshot_name = util.get_snapshot_label('restore_dropped_3p_snapshot')
        ret = self.client.backup(snapshot_name, [table_name], repo_name=self.repo_name,
                                 database_name=db_name, is_wait=True)
        assert ret
        assert self.client.drop_partition(table_name, 'b', db_name)
        assert self.client.drop_partition(table_name, 'c', db_name)
        self.client.add_partition(table_name, 'f', '20')
        restore_list = ['%s partition (b, c)' % table_name]
        ret = self.client.restore(snapshot_name, self.repo_name, restore_list, db_name, 
                                  is_wait=True)
        assert not ret
        self.client.clean(db_name)

    def test_restore_after_add_partition_1(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_restore_after_add_partition_1",
        "describe": "初始化数据并备份table1（partition a,b,c,d,e），新增分区f，并导入数据，执行恢复分区c,d,e,验证恢复成功，数据正确",
        "tag": "system,p1"
        }
        """
        """
        初始化数据并备份table1（partition a,b,c,d,e），新增分区f，并导入数据，
        执行恢复分区c,d,e,验证恢复成功，数据正确
        """
        db_name, table_name, index_name = util.gen_name_list()
        self.client.clean(db_name)
        self.client.create_database(db_name)
        self.client.use(db_name)

        partition_name_list_d = ['a', 'b', 'c', 'd', 'e']
        partition_value_list_d = ['0', '10', '20', '30', '40']
        partition_info_d = palo_client.PartitionInfo('k1',
                                                     partition_name_list_d, partition_value_list_d)

        self.base_case.init_partition_table(table_name, partition_info_d)
        self.base_case.wait_load(db_name)
        sql = 'select * from %s order by k1' % table_name
        r1 = self.client.execute(sql)
        snapshot_name = util.get_snapshot_label('restore_add_p1_snapshot')
        ret = self.client.backup(snapshot_name, [table_name], repo_name=self.repo_name,
                                 database_name=db_name, is_wait=True)
        assert ret
        self.client.add_partition(table_name, 'f', '50')
        self.client.delete(table_name, [('k1', '>', '0')], 'c')
        self.client.delete(table_name, [('k1', '>', '0')], 'd')
        self.client.delete(table_name, [('k1', '>', '0')], 'e')
        restore_list = ['%s partition (c, d, e)' % table_name]
        ret = self.client.restore(snapshot_name, self.repo_name, restore_list, db_name, 
                                  is_wait=True)
        assert ret
        times = 30
        while times > 0:
            r2 = self.client.execute(sql)
            print(len(r2))
            times -= 1
        util.check(r1, r2)
        self.client.clean(db_name)

    def test_restore_after_add_partition_2(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_restore_after_add_partition_2",
        "describe": "初始化数据并备份table1（partition a,b,c,d,e），新增分区f，并导入数据，执行全表恢复，验证恢复成功，数据正确，分区f任然存在",
        "tag": "system,p1"
        }
        """
        """
        初始化数据并备份table1（partition a,b,c,d,e），新增分区f，并导入数据，
        执行全表恢复，验证恢复成功，数据正确，分区f任然存在
        """
        db_name, table_name, index_name = util.gen_name_list()
        self.client.clean(db_name)
        self.client.create_database(db_name)
        self.client.use(db_name)

        partition_name_list_d = ['a', 'b', 'c', 'd', 'e']
        partition_value_list_d = ['0', '10', '20', '30', '40']
        partition_info_d = palo_client.PartitionInfo('k1',
                                                     partition_name_list_d, partition_value_list_d)

        self.base_case.init_partition_table(table_name, partition_info_d)
        self.base_case.wait_load(db_name)
        sql = 'select * from %s order by k1' % table_name
        r1 = self.client.execute(sql)
        snapshot_name = util.get_snapshot_label('restore_add_p2_snapshot')
        ret = self.client.backup(snapshot_name, [table_name], repo_name=self.repo_name,
                                 database_name=db_name, is_wait=True)
        assert ret
        self.client.add_partition(table_name, 'f', '50')
        self.client.delete(table_name, [('k1', '>', '0')], 'c')
        self.client.delete(table_name, [('k1', '>', '0')], 'd')
        self.client.delete(table_name, [('k1', '>', '0')], 'e')
        restore_list = ['%s' % table_name]
        ret = self.client.restore(snapshot_name, self.repo_name, restore_list, db_name, 
                                  is_wait=True)
        assert ret
        r2 = self.client.execute(sql)
        util.check(r1, r2)
        self.client.clean(db_name)

    def test_restore_after_rename_partition(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_restore_after_rename_partition",
        "describe": "初始化数据并备份table1（partition a,b,c,d,e），将分区e执行rename操作，恢复该分区，验证恢复失",
        "tag": "system,p1,fuzz"
        }
        """
        """初始化数据并备份table1（partition a,b,c,d,e），将分区e执行rename操作，恢复该分区，验证恢复失"""
        db_name, table_name, index_name = util.gen_name_list()
        self.client.clean(db_name)
        self.client.create_database(db_name)
        self.client.use(db_name)

        partition_name_list_d = ['a', 'b', 'c', 'd', 'e']
        partition_value_list_d = ['0', '10', '20', '30', '40']
        partition_info_d = palo_client.PartitionInfo('k1',
                                                     partition_name_list_d, partition_value_list_d)

        self.base_case.init_partition_table(table_name, partition_info_d)
        self.base_case.wait_load(db_name)
        snapshot_name = util.get_snapshot_label('restore_rename_p_snapshot')
        ret = self.client.backup(snapshot_name, [table_name], repo_name=self.repo_name,
                                 database_name=db_name, is_wait=True)
        assert ret
        ret = self.client.rename_partition('new_d', 'd', table_name)
        assert ret
        self.client.delete(table_name, [('k1', '>', '0')], 'new_d')
        restore_list = ['%s partition (d)' % table_name]
        ret = self.client.restore(snapshot_name, self.repo_name, restore_list, db_name, 
                                  is_wait=True)
        assert not ret
        self.client.clean(db_name)

    @pytest.mark.skip()
    def test_restore_after_delete_data(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_restore_after_delete_data",
        "describe": "todo:初始化数据并备份，delete分区c和d的数据，并恢复这两个分区的数据，验证恢复成功，数据正确",
        "tag": "autotest"
        }
        """
        """初始化数据并备份，delete分区c和d的数据，并恢复这两个分区的数据，验证恢复成功，数据正确"""
        pass

    def test_restore_after_column_change(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_restore_after_column_change",
        "describe": "初始化数据并备份table1，执行add column/drop column操作，执行恢复，验证恢复失败",
        "tag": "system,p1,fuzz"
        }
        """
        """初始化数据并备份table1，执行add column/drop column操作，执行恢复，验证恢复失败"""
        db_name, table_name, index_name = util.gen_name_list()
        self.client.clean(db_name)
        self.client.create_database(db_name)
        self.client.use(db_name)

        partition_name_list_d = ['a', 'b', 'c', 'd', 'e']
        partition_value_list_d = ['0', '10', '20', '30', '40']
        partition_info_d = palo_client.PartitionInfo('k1',
                                                     partition_name_list_d, partition_value_list_d)

        self.base_case.init_partition_table(table_name, partition_info_d)
        self.base_case.wait_load(db_name)
        snapshot_name = util.get_snapshot_label('restore_column_change_snapshot')
        ret = self.client.backup(snapshot_name, [table_name], repo_name=self.repo_name,
                                 database_name=db_name, is_wait=True)
        assert ret
        column_list = [('add_v', 'int', 'sum', '0')]
        ret = self.client.schema_change_add_column(table_name, column_list, is_wait_job=True)
        assert ret
        restore_list = ['%s' % table_name]
        ret = self.client.restore(snapshot_name, self.repo_name, restore_list, db_name, 
                                  is_wait=True)
        assert not ret
        self.client.clean(db_name)

    def test_restore_after_column_modify_order(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_restore_after_column_modify_order",
        "describe": "初始化数据并备份table1，将table1中的列的顺序进行修改，执行恢复全表/某几个分区操作，并验证失败",
        "tag": "system,p1,fuzz"
        }
        """
        """初始化数据并备份table1，将table1中的列的顺序进行修改，执行恢复全表/某几个分区操作，并验证失败"""
        db_name, table_name, index_name = util.gen_name_list()
        self.client.clean(db_name)
        self.client.create_database(db_name)
        self.client.use(db_name)

        partition_name_list_d = ['a', 'b', 'c', 'd', 'e']
        partition_value_list_d = ['0', '10', '20', '30', '40']
        partition_info_d = palo_client.PartitionInfo('k1',
                                                     partition_name_list_d, partition_value_list_d)

        self.base_case.init_partition_table(table_name, partition_info_d)
        self.base_case.wait_load(db_name)
        snapshot_name = util.get_snapshot_label('restore_order_modify_snapshot')
        ret = self.client.backup(snapshot_name, [table_name], repo_name=self.repo_name,
                                 database_name=db_name, is_wait=True)
        assert ret
        column_list = ['k1', 'k2', 'k3', 'k4', 'k5', 'v6', 'v5', 'v4', 'v3', 'v2', 'v1']
        ret = self.client.schema_change_order_column(table_name, column_list, is_wait_job=True)
        assert ret
        restore_list = ['%s' % table_name]
        ret = self.client.restore(snapshot_name, self.repo_name, restore_list, db_name,
                                  is_wait=True)
        assert not ret
        self.client.clean(db_name)

    def test_restore_after_column_modify_type(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_restore_after_column_modify_type",
        "describe": "初始化数据并备份table1，将table1中的列的类型进行修改，执行恢复全表/某几个分区操作，并验证恢复失败",
        "tag": "system,p1,fuzz"
        }
        """
        """初始化数据并备份table1，将table1中的列的类型进行修改，执行恢复全表/某几个分区操作，并验证恢复失败"""
        db_name, table_name, index_name = util.gen_name_list()
        self.client.clean(db_name)
        self.client.create_database(db_name)
        self.client.use(db_name)

        partition_name_list_d = ['a', 'b', 'c', 'd', 'e']
        partition_value_list_d = ['0', '10', '20', '30', '40']
        partition_info_d = palo_client.PartitionInfo('k1',
                                                     partition_name_list_d, partition_value_list_d)

        self.base_case.init_partition_table(table_name, partition_info_d)
        self.base_case.wait_load(db_name)
        snapshot_name = util.get_snapshot_label('restore_type_modify_snapshot')
        ret = self.client.backup(snapshot_name, [table_name], repo_name=self.repo_name,
                                 database_name=db_name, is_wait=True)
        assert ret
        ret = self.client.schema_change_modify_column(table_name, 'v3', 'varchar(5000)')
        assert ret
        restore_list = ['%s' % table_name]
        ret = self.client.restore(snapshot_name, self.repo_name, restore_list, db_name, 
                                  is_wait=True)
        assert not ret
        self.client.clean(db_name)

    def test_restore_after_rollup_rename(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_restore_after_rollup_rename",
        "describe": "初始化数据并备份table1，将table1中的rollup进行rename，执行恢复全表/某几个分区操作，并验证恢复失败。",
        "tag": "system,p1,fuzz"
        }
        """
        """初始化数据并备份table1，将table1中的rollup进行rename，执行恢复全表/某几个分区操作，并验证恢复失败。"""
        db_name, table_name, index_name = util.gen_name_list()
        self.client.clean(db_name)
        self.client.create_database(db_name)
        self.client.use(db_name)
        table_name = self.base_case.table_with_rollup
        partition_name_list_d = ['a', 'b', 'c', 'd', 'e']
        partition_value_list_d = ['0', '10', '20', '30', '40']
        partition_info_d = palo_client.PartitionInfo('k1',
                                                     partition_name_list_d, partition_value_list_d)
        distribution_type_d = 'HASH(k1, k2, k5)'
        distribution_info_d = palo_client.DistributionInfo(distribution_type_d, 5)
        self.base_case.init_multi_partition_with_rollup(partition_info_d, distribution_info_d, index_name)
        self.base_case.wait_load(db_name)
        snapshot_name = util.get_snapshot_label('restore_rename_rollup_snapshot')
        ret = self.client.backup(snapshot_name, [table_name], repo_name=self.repo_name,
                                 database_name=db_name, is_wait=True)
        assert ret
        ret = self.client.rename_rollup('new_rollup_name', index_name, table_name)
        assert ret
        restore_list = ['%s' % table_name]
        ret = self.client.restore(snapshot_name, self.repo_name, restore_list, db_name, 
                                  is_wait=True)
        assert not ret
        self.client.clean(db_name)

    def test_restore_after_rollup_create(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_restore_after_rollup_create",
        "describe": "初始化数据并备份table1，执行rollup操作，执行完成后，执行恢复全表/某几个分区,验证恢复失败;删除rollup,执行恢复,验证恢复成功",
        "tag": "system,p1,fuzz"
        }
        """
        """初始化数据并备份table1，执行rollup操作，执行完成后，执行恢复全表/某几个分区，
           验证恢复失败;删除rollup,执行恢复,验证恢复成功"""
        db_name, table_name, index_name = util.gen_name_list()
        self.client.clean(db_name)
        self.client.create_database(db_name)
        self.client.use(db_name)

        partition_name_list_d = ['a', 'b', 'c', 'd', 'e']
        partition_value_list_d = ['0', '10', '20', '30', '40']
        partition_info_d = palo_client.PartitionInfo('k1',
                                                     partition_name_list_d, partition_value_list_d)

        self.base_case.init_partition_table(table_name, partition_info_d)
        self.base_case.wait_load(db_name)
        sql = 'select * from %s order by k1' % table_name
        r1 = self.client.execute(sql)
        snapshot_name = util.get_snapshot_label('restore_create_rollup_snapshot')
        ret = self.client.backup(snapshot_name, [table_name], repo_name=self.repo_name,
                                 database_name=db_name, is_wait=True)
        assert ret
        self.client.delete(table_name, [('k1', '>', '0')], 'c')
        self.client.delete(table_name, [('k1', '>', '0')], 'd')
        self.client.delete(table_name, [('k1', '>', '0')], 'e')
        column_list = ['k2', 'k4', 'v4']
        ret = self.client.create_rollup_table(table_name, index_name, column_list, is_wait=True)
        assert ret
        restore_list = ['%s' % table_name]
        ret = self.client.restore(snapshot_name, self.repo_name, restore_list, db_name, 
                                  is_wait=True)
        assert not ret
        self.client.drop_rollup_table(table_name, index_name)
        time.sleep(10)
        ret = self.client.restore(snapshot_name, self.repo_name, restore_list, db_name, 
                                  is_wait=True)
        assert ret
        time.sleep(30)
        r2 = self.client.execute(sql)
        util.check(r1, r2)
        self.client.clean(db_name)

    @pytest.mark.skip()
    def test_restore_allow_load_true(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_restore_allow_load_true",
        "describe": "todo allow_load暂时只支持false，true的要等流式导入一起",
        "tag": "autotest"
        }
        """
        """
        初始化数据并备份table1，恢复任务中设allow_load为TRUE，执行恢复任务，在恢复过程中，执行导入，验证均执行成功，数据正确
        TODO: allow_load暂时只支持false，true的要等流式导入一起
        """
        pass

    @pytest.mark.skip()
    def test_restore_allow_load_false(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_restore_allow_load_false",
        "describe": "todo:初始化数据并备份table1，恢复任务中设allow_load为FALSE",
        "tag": "autotest"
        }
        """
        """
        初始化数据并备份table1，恢复任务中设allow_load为FALSE，执行恢复任务，在恢复过程中，执行导入，验证导入失败，恢复成功，数据正确。
        """
        pass

    def test_restore_while_other_job(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_restore_while_other_job",
        "describe": "初始化数据并备份table1，在导入/delete/schema change/rollup任务执行的时候执行恢复，验证恢复失败",
        "tag": "system,p1,fuzz"
        }
        """
        """
        初始化数据并备份table1，在导入/delete/schema change/rollup任务执行的时候执行恢复，验证恢复失败。
        """
        db_name, table_name, index_name = util.gen_name_list()
        self.client.clean(db_name)
        self.client.create_database(db_name)
        self.client.use(db_name)

        snapshot_name = util.get_snapshot_label('restore_while_op_snapshot')
        restore_list = ['%s' % table_name]
        partition_name_list_d = ['a', 'b', 'c', 'd', 'e']
        partition_value_list_d = ['0', '10', '20', '30', '40']
        partition_info_d = palo_client.PartitionInfo('k1',
                                                     partition_name_list_d, partition_value_list_d)

        distribution_type_d = 'HASH(k1, k2, k5)'
        distribution_info_d = palo_client.DistributionInfo(distribution_type_d, 11)
        self.client.create_table(table_name, DATA.schema_1,
                                 partition_info_d, distribution_info_d)
        ret = self.client.backup(snapshot_name, [table_name], repo_name=self.repo_name,
                                 database_name=db_name, is_wait=True)
        assert ret
        # 导入 的时候restore
        data_desc_list = palo_client.LoadDataInfo(DATA.file_path_1, table_name)
        load_label = util.get_label()
        assert self.client.batch_load(load_label, data_desc_list, broker=broker_info)
        ret = self.client.restore(snapshot_name, self.repo_name, restore_list, db_name,
                                  is_wait=True)
        self.base_case.wait_load(db_name)
        ret1 = self.client.get_load_job_state(load_label)
        print(ret, ret1)
        # ret,ret1: True FINISHED
        # assert not ret or ret1 == 'CANCELLED'
        # add column的时候 restore
        column_list = [('add_v', 'int', 'sum', '0')]
        ret = self.client.schema_change_add_column(table_name, column_list)
        assert ret
        ret = self.client.restore(snapshot_name, self.repo_name, restore_list, db_name,
                                  is_wait=True)
        assert not ret
        ret1 = self.client.wait_table_schema_change_job(table_name, db_name)
        ret2 = self.client.restore(snapshot_name, self.repo_name, restore_list, db_name,
                                  is_wait=True)
        assert not ret1 or not ret2
        self.client.clean(db_name)

    def test_other_job_while_restore(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_other_job_while_restore",
        "describe": "初始化数据并备份table1,执行恢复的同时,执行导入/delete/schema change/rollup任务,其他任务执行失败",
        "tag": "system,p1,fuzz"
        }
        """
        """
        初始化数据并备份table1,执行恢复的同时,执行导入/delete/schema change/rollup任务,其他任务执行失败
        """
        db_name, table_name, index_name = util.gen_name_list()
        self.client.clean(db_name)
        self.client.create_database(db_name)
        self.client.use(db_name)
        self.base_case.init_multi_partitions_table()
        self.base_case.wait_load(db_name)
        sql = 'select * from %s order by k1' % self.base_case.multi_partitions_table
        r1 = self.client.execute(sql)
        snapshot_name = util.get_snapshot_label('op_while_restore_snapshot')
        ret = self.client.backup(snapshot_name, [self.base_case.multi_partitions_table],
                                 repo_name=self.repo_name, database_name=db_name, is_wait=True)
        assert ret
        self.client.drop_table(self.base_case.multi_partitions_table)
        ret = self.client.restore(snapshot_name, self.repo_name, [self.base_case.multi_partitions_table],
                                  database_name=db_name)
        assert ret
        flag = True
        while flag:
            job = self.client.show_restore(db_name)
            state = palo_job.RestoreJob(job[0]).get_state()
            if state == 'FINISHED' or state == 'CANCELLED':
                flag = False
            else:
                try:
                    self.client.delete(table_name, [('k1', '>', '0')], 'partition_g')
                    assert 0 == 1
                except Exception as e:
                    pass
                try:
                    column_list = [('add_v', 'int', 'sum', '0')]
                    ret = self.client.schema_change_add_column(table_name, column_list)
                    assert 0 == 1
                except Exception as e:
                    pass
                time.sleep(5)
        self.client.clean(db_name)

    def test_restore_privilege(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_restore_privilege",
        "describe": "初始化数据并备份，只读用户执行恢复操作，验证执行失败；读写用户执行恢复操作，验证执行成功",
        "tag": "system,p1,fuzz"
        }
        """
        """初始化数据并备份，只读用户执行恢复操作，验证执行失败；读写用户执行恢复操作，验证执行成功"""
        read_user = 'read_user'
        write_user = 'write_user'
        self.client.drop_user(read_user)
        self.client.drop_user(write_user)
        self.client.create_user(read_user, read_user)
        self.client.create_user(write_user, write_user)
        self.client.grant(read_user, 'READ_ONLY', self.db_name)
        self.client.grant(write_user, 'READ_WRITE', self.db_name)
        read_client = palo_client.get_client(config.fe_host, config.fe_query_port, 
                                             user=read_user, password=read_user, 
                                             database_name=self.db_name)
        write_client = palo_client.get_client(config.fe_host, config.fe_query_port, 
                                              user=write_user, password=write_user, 
                                              database_name=self.db_name)

        ret = read_client.restore(self.snapshot_name, restore_list=[self.base_case.table_with_rollup],
                                  repo_name=self.repo_name, database_name=self.db_name, 
                                  is_wait=True)
        assert not ret
        ret = write_client.restore(self.snapshot_name, restore_list=[self.base_case.table_with_rollup],
                                   repo_name=self.repo_name, database_name=self.db_name, 
                                   is_wait=True)
        assert ret
        self.client.drop_user(read_user)
        self.client.drop_user(write_user)

    def test_cancel_restore_pending(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_cancel_restore_pending",
        "describe": "初始化数据并备份，执行恢复操作，在恢复操作的各个阶段执行cancel，验证任务cancel成功",
        "tag": "system,p1"
        }
        """
        """初始化数据并备份，执行恢复操作，在恢复操作的各个阶段执行cancel，验证任务cancel成功"""
        assert self._cancel_restore_while_state('PENDING')

    def test_cancel_restore_snapshoting(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_cancel_restore_snapshoting",
        "describe": "cancel restore job in snapshoting state",
        "tag": "system,p1"
        }
        """
        """cancel restore job in snapshoting state"""
        assert self._cancel_restore_while_state('SNAPSHOTING')

    def test_cancel_restore_download(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_cancel_restore_download",
        "describe": "cancel restore job in download state",
        "tag": "system,p1"
        }
        """
        """cancel restore job in download state"""
        assert self._cancel_restore_while_state('DOWNLOAD')

    def test_cancel_restore_downloading(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_cancel_restore_downloading",
        "describe": "cancel restore job in downloading state",
        "tag": "system,p1"
        }
        """
        """cancel restore job in downloading state"""
        assert self._cancel_restore_while_state('DOWNLOADING')

    def test_cancel_restore_commit(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_cancel_restore_commit",
        "describe": "cancel restore job in commit state",
        "tag": "system,p1"
        }
        """
        """cancel restore job in commit state"""
        assert self._cancel_restore_while_state('COMMIT')

    def test_cancel_restore_commiting(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_cancel_restore_commiting",
        "describe": "cancel restore job in commting state",
        "tag": "system,p1"
        }
        """
        """cancel restore job in commting state"""
        assert self._cancel_restore_while_state('COMMITING')

    def _cancel_restore_while_state(self, state):
        """cancel restore job in the state"""
        self.client.use(self.db_name)
        table_name = self.base_case.table_with_rollup
        assert self.client.verify(DATA.expected_data_file_list_1, table_name)
        snapshot_label = self.snapshot_name
        ret = self.client.restore(snapshot_label, restore_list=[table_name], 
                                  repo_name=self.repo_name, database_name=self.db_name)
        assert ret
        s = None
        flag = False
        while s != 'FINISHED' and s != 'CANCELLED':
            job_info = self.client.show_restore(self.db_name)
            s = palo_job.RestoreJob(job_info[-1]).get_state()
            if s == state:
                flag = True
                ret = self.client.cancel_restore(self.db_name)
                assert ret
                job_info = self.client.show_restore(self.db_name)
                s = palo_job.RestoreJob(job_info[-1]).get_state()
                assert s == 'CANCELLED'
                return True
            time.sleep(1)
        if flag is False:
            raise pytest.skip('can not get restore state')
        return True

    @pytest.mark.skip()
    def test_restore_from_readonly_repo(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_restore_from_readonly_repo",
        "describe": "todo:从read_only仓库中恢复数据，验证恢复正确。",
        "tag": "autotest"
        }
        """
        """从read_only仓库中恢复数据，验证恢复正确。"""
        pass

    @pytest.mark.skip()
    def test_restore_timeout(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_restore_timeout",
        "describe": "初始化数据并备份，设置恢复操作的timeout时间，验证超过timeout时间后，恢复任务失败",
        "tag": "autotest"
        }
        """
        """初始化数据并备份，设置恢复操作的timeout时间，验证超过timeout时间后，恢复任务失败"""
        pass

    def test_restore_replication_num_1(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_restore_replication_num_1",
        "describe": "初始化数据并备份table1（table1中的replication_num=3），drop table1，恢复表table1，设置副本数为1,replication_num）验证恢复正确，数据正确",
        "tag": "system,p1"
        }
        """
        """初始化数据并备份table1（table1中的replication_num=3），drop table1，恢复表table1，设置副本数为1,
          （replication_num）验证恢复正确，数据正确"""
        db_name, table_name, index_name = util.gen_name_list()
        self.client.clean(db_name)
        self.client.create_database(db_name)
        self.client.use(db_name)
        self.base_case.init_multi_partitions_table()
        self.base_case.wait_load(db_name)
        sql = 'select * from %s order by k1' % self.base_case.multi_partitions_table
        r1 = self.client.execute(sql)
        snapshot_name = util.get_snapshot_label('restore_replication_1_snapshot')
        ret = self.client.backup(snapshot_name, [self.base_case.multi_partitions_table],
                                 repo_name=self.repo_name, database_name=db_name, is_wait=True)
        assert ret
        self.client.drop_table(self.base_case.multi_partitions_table)
        restore_list = [self.base_case.multi_partitions_table]
        ret = self.client.restore(snapshot_name, self.repo_name, restore_list, 
                                  database_name=db_name, replication_num=1, is_wait=True)
        assert ret
        r2 = self.client.execute(sql)
        util.check(r1, r2)
        self.client.clean(db_name)

    def test_restore_replication_num_2(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_restore_replication_num_2",
        "describe": "初始化数据并备份table1（table1中的replication_num=3），执行导入和删除操作，恢复表table1，设置副本数为1（replication_num），验证恢复失败。",
        "tag": "system,p1,fuzz"
        }
        """
        """初始化数据并备份table1（table1中的replication_num=3），执行导入和删除操作，
           恢复表table1，设置副本数为1（replication_num），验证恢复失败。"""
        db_name, table_name, index_name = util.gen_name_list()
        self.client.clean(db_name)
        self.client.create_database(db_name)
        self.client.use(db_name)
        self.base_case.init_multi_partitions_table()
        self.base_case.wait_load(db_name)
        snapshot_name = util.get_snapshot_label('restore_replication_2_snapshot')
        ret = self.client.backup(snapshot_name, [self.base_case.multi_partitions_table],
                                 repo_name=self.repo_name, database_name=db_name, is_wait=True)
        assert ret
        self.client.delete(self.base_case.multi_partitions_table, [('k1', '>', '0')], 'partition_g')
        restore_list = [self.base_case.multi_partitions_table]
        ret = self.client.restore(snapshot_name, self.repo_name, restore_list,
                                  database_name=db_name, replication_num=1, is_wait=True)
        assert not ret
        self.client.clean(db_name)

    def test_after_snapshot(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_after_snapshot",
        "describe": "对snapshot后的表进行delete, load, add partition, drop partition,add rollup, drop rollup, modify column",
        "tag": "system,p1,fuzz"
        }
        """
        """对snapshot后的表进行delete, load, add partition, drop partition, 
           add rollup, drop rollup, modify column"""
        db_name, table_name, index_name = util.gen_name_list()
        self.client.clean(db_name)
        self.client.create_database(db_name)
        self.client.use(db_name)
        self.base_case.init_multi_partitions_table()
        self.base_case.wait_load(db_name)
        ret1 = self.client.execute('select * from %s order by k1' % self.base_case.multi_partitions_table)
        snapshot_name = util.get_snapshot_label('after_restore_snapshot')
        ret = self.client.backup(snapshot_name, [self.base_case.multi_partitions_table],
                                 repo_name=self.repo_name, database_name=db_name, is_wait=True)
        assert ret
        self.client.drop_table(self.base_case.multi_partitions_table)
        restore_list = [self.base_case.multi_partitions_table]
        ret = self.client.restore(snapshot_name, self.repo_name, restore_list,
                                  database_name=db_name, is_wait=True)
        restore_list = []
        rollup_after_restore = '%s_rollup' % self.base_case.multi_partitions_table
        restore_list.append('%s as %s' % (self.base_case.multi_partitions_table, rollup_after_restore))
        ret = self.client.restore(snapshot_name, self.repo_name, restore_list,
                                  database_name=db_name, is_wait=True)
        restore_list = []

        partition_after_restore = '%s_partition' % self.base_case.multi_partitions_table
        restore_list.append("%s as %s" % (self.base_case.multi_partitions_table, partition_after_restore))
        ret = self.client.restore(snapshot_name, self.repo_name, restore_list,
                                  database_name=db_name, is_wait=True)
        restore_list = []

        delete_after_restore = '%s_delete' % self.base_case.multi_partitions_table
        restore_list.append("%s as %s" % (self.base_case.multi_partitions_table, delete_after_restore))
        ret = self.client.restore(snapshot_name, self.repo_name, restore_list,
                                  database_name=db_name, is_wait=True)
        restore_list = []

        column_after_restore = '%s_column' % self.base_case.multi_partitions_table
        restore_list.append("%s as %s" % (self.base_case.multi_partitions_table, column_after_restore))
        ret = self.client.restore(snapshot_name, self.repo_name, restore_list, 
                                  database_name=db_name, is_wait=True)
        assert ret
        # rollup
        ret = self.client.create_rollup_table(rollup_after_restore, index_name, ['k1, k2'], 
                                              is_wait=True)
        assert ret
        assert self.client.get_index_schema(rollup_after_restore, index_name)
        ret2 = self.client.execute('select * from %s order by k1' % rollup_after_restore)
        util.check(ret1, ret2)
        # add partition
        ret = self.client.drop_partition(partition_after_restore, 'partition_g')
        assert ret
        assert not self.client.get_partition(partition_after_restore, 'partition_g')
        ret = self.client.add_partition(partition_after_restore, 'add_partition', '999')
        assert ret
        assert self.client.get_partition(partition_after_restore, 'add_partition')
        
        # delete
        assert self.client.delete(delete_after_restore, [('k1', '>', '0')], 'partition_g')
        result = self.client.execute('select * from %s' % delete_after_restore)
        print(result)
        
        # column
        assert self.client.schema_change_add_column(column_after_restore, 
                                                    [('add_v1', 'int', 'replace', '-1')], 
                                                    is_wait_job=True)
        line1 = 'select * from %s order by k1' % column_after_restore
        line2 = 'select k1, k2, k3, k4, k5, v1, v2, v3, v4, v5, v6, -1 from %s order by k1' \
                % self.base_case.multi_partitions_table
        self.base_case.check2(line1, line2)
        self.client.clean(db_name)

    @pytest.mark.skip()
    def test_wrong_sql(self):
        """
        {
        "title": "test_sys_snapshot_restore.test_wrong_sql",
        "describe": "todo:错误的restore SQL",
        "tag": "autotest"
        }
        """
        """错误的restore SQL"""
        pass


