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
import time

from data import schema as DATA
from data import load_file as FILE
from lib import palo_client
from lib import util
from lib import palo_config
from lib import common
from lib import palo_job
from lib import palo_task
from lib import node_op

LOG = palo_client.LOG
L = palo_client.L
config = palo_config.config
broker_info = palo_config.broker_info


def setup_module():
    """
    setUp
    """
    global node_operator
    node_operator = node_op.Node()
    node_operator.check_cluster()


def teardown_module():
    """
    tearDown
    """
    node_operator.check_cluster()


def test_update_on_observer():
    """
    {
    "title": "test_update_on_observer",
    "describe": "在非fe master上执行update操作，预期执行成功",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    partition_info = palo_client.PartitionInfo("k2",
                                               ["p1", "p2", "p3", "p4", "p5"],
                                               ["0", "100", "200", "300", "MAXVALUE"])
    distribution_info = palo_client.DistributionInfo('hash(k1)', 10)
    keys_desc = "UNIQUE KEY(k1, k2, k3)"
    ret = client.create_table(table_name, DATA.partition_column_no_agg_list, keys_desc=keys_desc,
                              partition_info=partition_info, distribution_info=distribution_info)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.partition_local_file)
    assert ret, 'stream load failed'
    ret1 = client.execute('select k1, k2, v6+1 from %s order by k1' % table_name)
    observer = node_operator.get_observer()
    observer_client = common.get_client(observer)
    observer_client.use(database_name)
    retry_times = 10
    while retry_times > 0:
        try:
            ret = observer_client.update(table_name, 'v6=v6+1', 'k1 > 0')
            break
        except Exception as e:
            LOG.warning(L('retry', msg=str(e)))
        retry_times -= 1
        time.sleep(3)
    assert ret, 'update failed'
    ret2 = observer_client.execute('select k1, k2, v6 from %s order by k1' % table_name)
    util.check(ret1, ret2, True)
    follower = node_operator.get_follower()
    follower_client = common.get_client(follower)
    follower_client.use(database_name)
    ret = follower_client.update(table_name, 'v6=1.0', 'k1 > 0')
    assert ret, 'update failed'
    sql1 = 'select 1.0 from %s' % table_name
    sql2 = 'select v6 from %s order by k1' % table_name
    common.check2(follower_client, sql1, sql2=sql2)
    client.clean(database_name)


def test_update_when_switch_fe_master():
    """
    {
    "title": "test_update_when_switch_fe_master",
    "describe": "在执行update操作的时候，fe进行切主",
    "tag": "function,p0,fuzz"
    }
    """
    # init
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    partition_info = palo_client.PartitionInfo("k2",
                                               ["p1", "p2", "p3", "p4", "p5"],
                                               ["0", "100", "200", "300", "MAXVALUE"])
    distribution_info = palo_client.DistributionInfo('hash(k1)', 10)
    keys_desc = "UNIQUE KEY(k1, k2, k3)"
    ret = client.create_table(table_name, DATA.partition_column_no_agg_list, keys_desc=keys_desc,
                              partition_info=partition_info, distribution_info=distribution_info)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.partition_local_file)
    assert ret, 'stream load failed'
    # 持续执行update
    observer = node_operator.get_observer()
    client1 = common.get_client(observer)
    update_task = palo_task.SyncTask(client1.update, table_name, ['v6=v6+1'], 'k1 > 0', database_name)
    update_thread = palo_task.TaskThread(update_task)
    update_thread.start()
    time.sleep(5)
    # upadte的过程中切主
    master = node_operator.get_master()
    node_operator.restart_fe(master)
    time.sleep(30)
    # 停止update
    time.sleep(5)
    update_thread.stop()
    update_thread.join()
    # 校验
    assert node_operator.is_fe_alive(master), 'fe restart failed'
    assert client1.select_all(table_name, database_name)
    client1.connect()
    assert client1.get_master()
    client1.clean(database_name)
    retry_times = 10
    while retry_times > 0:
        try:
            client.connect()
            client.get_alive_backend_list()[0][palo_job.BackendProcInfo.Host]
            break
        except Exception as e:
            retry_times -= 1
            time.sleep(3)
            LOG.info(L("show proc failed"))


def test_update_load_fe_image():
    """
    {
    "title": "test_update_when_switch_fe_master",
    "describe": "在执行update操作的时候生成image，fe宕机，持续执行update操作到有新的image生成，fe切主加载image，数据正确",
    "tag": "function,p0,fuzz"
    }
    """
    # check cluster status
    # init
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    partition_info = palo_client.PartitionInfo("k2",
                                               ["p1", "p2", "p3", "p4", "p5"],
                                               ["0", "100", "200", "300", "MAXVALUE"])
    distribution_info = palo_client.DistributionInfo('hash(k1)', 10)
    keys_desc = "UNIQUE KEY(k1, k2, k3)"
    ret = client.create_table(table_name, DATA.partition_column_no_agg_list, keys_desc=keys_desc,
                              partition_info=partition_info, distribution_info=distribution_info)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.partition_local_file)
    assert ret, 'stream load failed'
    # 获取fe image
    master = node_operator.get_master()
    version = node_operator.get_image_version(master)
    assert version, 'get fe version failed.version is %s' % version
    print(version)
    node_operator.restart_fe(master)
    time.sleep(30)
    new_master = node_operator.get_master()
    # 持续执行update
    observer = node_operator.get_observer()
    client1 = common.get_client(observer)
    update_task = palo_task.SyncTask(client1.update, table_name, ['v6=v6+1'], 'k1 > 0', database_name)
    update_thread = palo_task.TaskThread(update_task)
    update_thread.start()
    # 检测新master上是否有新image生成，如果有则停止update
    timeout = 300
    while timeout > 0:
        new_version = node_operator.get_image_version(new_master)
        print(new_version)
        if new_version and max(new_version) > max(version):
            break
        time.sleep(3)
        timeout -= 3
    # 停止update
    update_thread.stop()
    update_thread.join()
    time.sleep(5)
    # 再次切主
    node_operator.restart_fe(new_master)
    new_master = node_operator.get_master()
    time.sleep(30)
    # 查询表
    client.connect()
    assert client.select_all(table_name)
    client.clean(database_name)
    retry_times = 10
    while retry_times > 0:
        try:
            client.get_alive_backend_list()[0][palo_job.BackendProcInfo.Host]
            break
        except Exception as e:
            retry_times -= 1
            time.sleep(3)
            LOG.info(L("show proc failed"))


def test_update_when_one_be_down():
    """
    {
    "title": "test_update_when_one_be_down",
    "describe": "在执行update操作的时候，有一个be宕机",
    "tag": "function,p0,fuzz"
    }
    """
    # init
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    partition_info = palo_client.PartitionInfo("k2",
                                               ["p1", "p2", "p3", "p4", "p5"],
                                               ["0", "100", "200", "300", "MAXVALUE"])
    distribution_info = palo_client.DistributionInfo('hash(k1)', 10)
    keys_desc = "UNIQUE KEY(k1, k2, k3)"
    ret = client.create_table(table_name, DATA.partition_column_no_agg_list, keys_desc=keys_desc,
                              partition_info=partition_info, distribution_info=distribution_info)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.partition_local_file)
    assert ret, 'stream load failed'
    # 持续执行update
    update_task = palo_task.SyncTask(client.update, table_name, ['v6=v6+1'], 'k1 > 0', database_name)
    update_thread = palo_task.TaskThread(update_task)
    update_thread.start()
    time.sleep(5)
    # update的过程中be宕机
    be_host = client.get_alive_backend_list()[0][palo_job.BackendProcInfo.Host]
    print('stop be ', be_host)
    assert node_operator.stop_be(be_host)
    update_succ_count = int(update_task.succ_count)
    print(update_succ_count)
    # 停止update
    time.sleep(30)
    update_thread.stop()
    update_thread.join()
    print(update_task.succ_count)
    assert update_task.succ_count > update_succ_count
    assert node_operator.start_be(be_host)
    time.sleep(5)
    client.clean(database_name)


def test_update_when_be_restart():
    """
    {
    "title": "test_update_when_multi_be_down",
    "describe": "在执行update操作的时候，有be重启",
    "tag": "function,p0,fuzz"
    }
    """
    # init
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    partition_info = palo_client.PartitionInfo("k2",
                                               ["p1", "p2", "p3", "p4", "p5"],
                                               ["0", "100", "200", "300", "MAXVALUE"])
    distribution_info = palo_client.DistributionInfo('hash(k1)', 10)
    keys_desc = "UNIQUE KEY(k1, k2, k3)"
    ret = client.create_table(table_name, DATA.partition_column_no_agg_list, keys_desc=keys_desc,
                              partition_info=partition_info, distribution_info=distribution_info)
    assert ret, 'create table failed'
    ret = client.stream_load(table_name, FILE.partition_local_file)
    assert ret, 'stream load failed'
    # 持续执行update
    update_task = palo_task.SyncTask(client.update, table_name, ['v6=v6+1'], 'k1 > 0', database_name)
    update_thread = palo_task.TaskThread(update_task)
    update_thread.start()
    time.sleep(5)
    # upadte的过程中be宕机
    be_host = node_operator.get_be_list()[-1]
    print('stop be ', be_host)
    node_operator.restart_be(be_host)
    update_succ_count = int(update_task.succ_count)
    print(update_succ_count)
    time.sleep(10)
    assert node_operator.is_be_alive(be_host)
    print(update_succ_count)
    # 停止update
    time.sleep(30)
    update_thread.stop()
    update_thread.join()
    print(update_task.succ_count)
    print(update_succ_count)
    assert update_task.succ_count > update_succ_count
    client.clean(database_name)


