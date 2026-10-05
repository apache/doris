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
#   @file test_sys_snapshot_repo.py
#   @date 2016/01/23 15:26:21
#   @brief backup restore
#
#############################################################################


"""
仓库测试
"""
import os
import random
import sys
import time
import threading
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
LOG = palo_client.LOG
L = palo_client.L

config = palo_config.config
broker_name = config.broker_name
broker_property = config.broker_property
broker_info = palo_config.broker_info
repo_location = config.repo_location
repo_location1 = repo_location + '1'


class TestRepo(object):
    """test repo class"""
    db = "repo_test_database"
    table = "repo_test_table"

    def setUp(self):
        """setUp"""
        self.client = palo_client.get_client(config.fe_host, config.fe_query_port,
                      database_name="repo_test_database", user=config.fe_user, password=config.fe_password)
        self.client.init()
        print("setup")

    def tearDown(self):
        """tearDown"""
        print("tearDown")
        print(self.client)
        del self.client

    def setup_class(self):
        """setup class"""
        client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                        password=config.fe_password)
        self.init(client, self.db, self.table)
        self.setUp()

    def init(self, client, db, table):
        """
        init db and table, load data
        Args:
            db: string, database name
            table: string, table name

        Returns:
        """
        client.clean(db)
        client.create_database(db)
        partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
        partition_value_list = ['5', '20', '31', 'MAXVALUE']
        partition_info = palo_client.PartitionInfo('k1', partition_name_list, partition_value_list)
        client.create_table(table, DATA.schema_1, partition_info,
                            keys_desc='AGGREGATE KEY (k1, k2, k3, k4, k5)')
        assert client.show_tables(table, db)
        data_desc_list_1 = palo_client.LoadDataInfo(DATA.file_path_1,
                                                table,
                                                ['partition_a', 'partition_b', 'partition_c'])
        client.batch_load(util.get_label(), data_desc_list_1, is_wait=True, broker=broker_info)
        assert client.verify(DATA.expected_data_file_list_1, table)

    def test_create_repo_basic(self):
        """
        {
        "title": "test_sys_snapshot_repo.test_create_repo_basic",
        "describe": "仓库创建。验证创建成功，可执行备份和恢复",
        "tag": "function,p1,fuzz"
        }
        """
        """仓库创建。验证创建成功，可执行备份和恢复"""
        database_name, table_name = self.db, self.table
        # 创建仓库,并验证
        repo_name = 'repo_create_basic_test'
        self.client.drop_repository(repo_name)
        ret = self.client.create_repository(repo_name, broker_name, repo_location, broker_property)
        assert ret
        assert self.client.get_repository(repo_name)
        # 备份
        snapshot_label = util.get_snapshot_label(table_name)
        backup_list = [table_name]
        ret = self.client.backup(snapshot_label, backup_list, repo_name, 
                                 database_name=database_name, is_wait=True)
        assert ret
        # 恢复并验证
        restore_list = list()
        new_table_name = 'create_repo_basic_table'
        restore_list.append("%s as %s" % (table_name, new_table_name))
        ret = self.client.restore(snapshot_label, repo_name, restore_list, 
                                  database_name=database_name, is_wait=True)
        assert ret
        assert self.client.verify(DATA.expected_data_file_list_1, new_table_name)
        # 删除仓库
        ret = self.client.drop_repository(repo_name)
        # assert ret
        # self.client.drop_table(new_table_name, database_name)
        """创建read_only仓库。验证创建成功，仅可执行恢复操作，不能备份到该仓库中"""
        # 创建read_only仓库
        repo_name = 'repo_create_basic_test'
        self.client.drop_repository(repo_name)
        ret = self.client.create_repository(repo_name, broker_name, repo_location, 
                                            broker_property, is_read_only=True)
        assert ret
        repo_info = self.client.get_repository(repo_name, True)
        assert repo_info
        assert repo_info.get_isReadOnly()
        # 备份失败
        snapshot_label_f = util.get_snapshot_label("failed")
        backup_list = [table_name]
        ret = self.client.backup(snapshot_label_f, backup_list, repo_name, 
                                 database_name=database_name)
        assert not ret

        # 恢复并验证
        restore_list = list()
        new_table_name = 'create_repo_readonly_table'
        restore_list.append("%s as %s" % (table_name, new_table_name))
        ret = self.client.restore(snapshot_label, repo_name, restore_list, 
                                  database_name=database_name, is_wait=True)
        assert ret
        assert self.client.verify(DATA.expected_data_file_list_1, new_table_name)
        # 删除仓库
        ret = self.client.drop_repository(repo_name)
        assert ret
        self.client.drop_table(new_table_name, database_name)

    @pytest.mark.skip()
    def test_create_repo_on_bos(self):
        """
        {
        "title": "test_sys_snapshot_repo.test_create_repo_on_bos",
        "describe": "todo:bos上仓库创建。验证创建成功，可执行备份和恢复",
        "tag": "autotest"
        }
        """
        """bos上仓库创建。验证创建成功，可执行备份和恢复"""
        pass

    @pytest.mark.skip()
    def test_create_repo_on_hdfs(self):
        """
        {
        "title": "test_sys_snapshot_repo.test_create_repo_on_hdfs",
        "describe": "todo:hdfs上仓库创建。验证创建成功，可执行备份和恢复",
        "tag": "autotest"
        }
        """
        """hdfs上仓库创建。验证创建成功，可执行备份和恢复"""
        pass

    def test_create_repo_when_already_exist(self):
        """
        {
        "title": "test_sys_snapshot_repo.test_create_repo_when_already_exist",
        "describe": "创建repo时，有同名的repo存在，验证创建成功（相当于复用）",
        "tag": "function,p1,fuzz"
        }
        """
        """创建repo时，有同名的repo存在，验证创建成功（相当于复用）"""
        repo_name = 'repo_alread_exist'
        self.client.drop_repository(repo_name)
        ret = self.client.create_repository(repo_name, broker_name, repo_location, broker_property)
        assert ret
        self.client.get_repository(repo_name)
        ret = self.client.create_repository(repo_name, broker_name, repo_location, broker_property)
        assert not ret, 'repository already exist'
        assert self.client.get_repository(repo_name)
        self.client.drop_repository(repo_name)

    def test_create_repo_path_not_exist(self):
        """
        {
        "title": "test_sys_snapshot_repo.test_create_repo_path_not_exist",
        "describe": "创建repo时，指定的路径不存在，验证创建失败",
        "tag": "function,p1,fuzz"
        }
        """
        """创建repo时，指定的路径不存在，验证创建失败"""
        repo_name = 'repo_path_not_exist'
        repo_path = '/user/palo/path_not_exist'
        ret = self.client.create_repository(repo_name, broker_name, repo_path, broker_property)
        assert not ret

    def test_repo_create_drop_privilege(self):
        """
        {
        "title": "test_sys_snapshot_repo.test_repo_create_drop_privilege",
        "describe": "验证root有权限创建/删除repo，一般普通用户没有权限创建/删除repo",
        "tag": "function,p1,fuzz"
        }
        """
        """验证root有权限创建/删除repo，一般普通用户没有权限创建/删除repo"""
        # 创建只读用户/读写用户并连接
        read_user = 'read_user'
        rw_user = 'read_write_user'
        database_name, table_name = self.db, self.table
        self.client.drop_user(read_user)
        self.client.drop_user(rw_user)
        assert self.client.create_user(read_user, read_user)
        assert self.client.create_user(rw_user, rw_user)
        assert self.client.grant(read_user, 'READ_ONLY', database_name)
        assert self.client.grant(rw_user, 'ALL', database_name)
        read_client = palo_client.get_client(config.fe_host, config.fe_query_port, 
                                             database_name=database_name, user=read_user, 
                                             password=read_user)
        rw_client = palo_client.get_client(config.fe_host, config.fe_query_port, 
                                           database_name=database_name, user=rw_user, 
                                           password=rw_user)
        # 只读用户/读写用户创建仓库失败
        repo_name = 'repo_privilege'
        ret = rw_client.create_repository(repo_name, broker_name, repo_location, broker_property)
        assert not ret
        ret = read_client.create_repository(repo_name, broker_name, repo_location, broker_property)
        assert not ret
        # 初始化仓库
        repo_name1 = 'repo_test'
        ret = self.client.create_repository(repo_name1, broker_name, repo_location, 
                                            broker_property)
        assert ret
        assert rw_client.get_repository(repo_name1)
        # 只读用户/读写用户删除仓库失败
        ret = read_client.drop_repository(repo_name1)
        assert not ret
        ret = rw_client.drop_repository(repo_name1)
        assert not ret
        ret = self.client.drop_repository(repo_name1)
        assert ret
        # 删除用户
        self.client.drop_user(read_user)
        self.client.drop_user(rw_user)

    def test_drop_and_create_repo(self):
        """
        {
        "title": "test_sys_snapshot_repo.test_drop_and_create_repo",
        "describe": "删除repo后，再重新创建同名repo/不同名repo",
        "tag": "function,p1"
        }
        """
        """删除repo后，再重新创建同名repo/不同名repo"""
        repo_name = 'drop_and_create_repo'
        repo_name_1 = 'drop_and_create_repo_1'
        ret = self.client.create_repository(repo_name, broker_name, repo_location, broker_property)
        assert ret
        assert self.client.get_repository(repo_name)
        ret = self.client.drop_repository(repo_name)
        assert ret
        ret = self.client.create_repository(repo_name, broker_name, repo_location, broker_property)
        assert ret
        assert self.client.get_repository(repo_name)
        ret = self.client.drop_repository(repo_name)
        assert ret
        ret = self.client.create_repository(repo_name, broker_name, repo_location1, 
                                            broker_property)
        assert ret
        assert self.client.get_repository(repo_name)
        assert self.client.drop_repository(repo_name)
        ret = self.client.create_repository(repo_name_1, broker_name, repo_location, 
                                            broker_property)
        assert ret
        assert self.client.get_repository(repo_name_1)
        assert self.client.drop_repository(repo_name_1)
        times = 10
        while times > 0:
            ret = self.client.create_repository(repo_name, broker_name, repo_location, 
                                                broker_property)
            assert ret
            assert self.client.get_repository(repo_name)
            ret = self.client.drop_repository(repo_name)
            assert ret
            times -= 1

    def test_drop_repo_when_backup_restore(self):
        """
        {
        "title": "test_sys_snapshot_repo.test_drop_repo_when_backup_restore",
        "describe": "当有备份/恢复任务执行时，删除repo，验证删除失败",
        "tag": "function,p1,fuzz"
        }
        """
        """当有备份/恢复任务执行时，删除repo，验证删除失败"""
        database_name, table_name = self.db, self.table
        # 创建仓库,并验证
        repo_name = 'repo_drop_test'
        self.client.drop_repository(repo_name)
        ret = self.client.create_repository(repo_name, broker_name, repo_location, broker_property)
        assert ret
        assert self.client.get_repository(repo_name)
        # 备份
        snapshot_label = util.get_snapshot_label(table_name)
        backup_list = [table_name]
        ret = self.client.backup(snapshot_label, backup_list, repo_name, 
                                 database_name=database_name, is_wait=False)
        assert ret
        # 备份时删除repo失败
        backup_jobs = self.client.show_backup(database_name)
        backup_job = palo_job.BackupJob(backup_jobs[-1])
        state = backup_job.get_state()
        if state != "FINISHED" and state != "CANCELLED":
            ret = self.client.drop_repository(repo_name)
            assert not ret
        self.client.wait_backup_job()
        # 恢复时删除repo失败,验证恢复成功
        restore_list = list()
        new_table_name = 'drop_repo_restore_table'
        restore_list.append("%s as %s" % (table_name, new_table_name))
        ret = self.client.restore(snapshot_label, repo_name, restore_list, 
                                  database_name=database_name)
        assert ret
        restore_jobs = self.client.show_restore(database_name)
        restore_job = palo_job.RestoreJob(restore_jobs[-1])
        state = restore_job.get_state()
        if state != "FINISHED" and state != "CANCELLED":
            ret = self.client.drop_repository(repo_name)
            assert not ret
        self.client.wait_restore_job()

        assert self.client.verify(DATA.expected_data_file_list_1, new_table_name)
        # 删除仓库
        time.sleep(5)
        ret = self.client.drop_repository(repo_name)
        assert ret
        self.client.drop_table(new_table_name, database_name)

