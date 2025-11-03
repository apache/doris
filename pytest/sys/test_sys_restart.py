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
Date:    2015/11/17 17:23:06
"""
import sys
import time
import pytest

from lib import node_op
from data import rollup_scenario as DATA
from data import schema_change as DATA1
from lib import palo_client
from lib import palo_config
from lib import util
from lib import palo_task
from lib import palo_job
from lib import common

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
    global fe_observer
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user, \
            password=config.fe_password)
    fe_list = client.get_fe_list()
    fe_observer = util.get_attr_condition_value(fe_list, palo_job.FrontendInfo.Role,
                                                'OBSERVER', palo_job.FrontendInfo.Host)


def check_fe_be():
    """检查fe和be，是否有false，如果有则拉起来"""
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user, \
            password=config.fe_password)
    ret = client.get_backend_list()
    be_list = util.get_attr_condition_list(ret, palo_job.BackendProcInfo.Alive, 
                                           'false', palo_job.BackendProcInfo.Host)
    if be_list is not None:
        for be_host in be_list:
            node_operator.start_be(be_host)
            assert node_operator.is_be_alive(be_host)
  
    ret = client.get_fe_list()
    fe_list = util.get_attr_condition_list(ret, palo_job.FrontendInfo.Alive,
                                           'false', palo_job.FrontendInfo.Host)
    if fe_list is not None:
        for fe_host in fe_list:
            node_operator.start_fe(fe_host)
            assert node_operator.is_fe_alive(fe_host)


def test_rollup_restart_schema_change():
    """
    {
    "title": "test_rollup_restart_schema_change",
    "describe": "对A表进行rollup操作生成B表,再次对A表进行rollup操作生成C,C表未完成生成时，重启BE",
    "tag": "system,p1,fuzz"
    }
    """
    """
    1. 对A表进行rollup操作生成B表；
    2. B表生成完成后，再次对A表进行rollup操作生成C，此时会清除A-B间的关系链（内存）；
    3. C表未完成生成时，重启BE；
    4. 所有表重新加载后，B表头中再次出现B->A的单向关系链，而A中存储的实际是A->C的关系链；
    5. B导入数据，清除B中的关系链同时，误清除了A中A->C的关系链，导致生成C的过程中，A不能继续给C转数据。
    """
    check_fe_be()
    database_name, table_name, index_name = util.gen_num_format_name_list()
    user = 'jack'
    LOG.info(L('', database_name=database_name,
             table_name=table_name, index_name=index_name))
    root_client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user, \
            password=config.fe_password)
    root_client.clean(database_name)
    root_client.clean_user(user)
    assert root_client.create_database(database_name)
    assert root_client.use(database_name)
    assert root_client.create_user(user, is_superuser=False)
    assert root_client.grant(user, "ALL", database_name)
    normal_client = palo_client.get_client(config.fe_host, config.fe_query_port, user=user, password='')
    assert normal_client.use(database_name)
    assert normal_client.create_table(table_name, DATA.schema_2) 
    assert root_client.create_rollup_table(table_name, index_name,
                                           DATA.rollup_field_list_2, is_wait=True)

    assert root_client.show_tables(table_name)
    assert root_client.get_index(table_name)
    assert root_client.get_index(table_name, index_name=index_name)

    data_desc_list = palo_client.LoadDataInfo(DATA.file_path_2, table_name)
    assert normal_client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert root_client.verify(DATA.expected_data_file_list_2, table_name)

    sql_1 = 'SELECT k1, k3, SUM(v1) FROM %s GROUP BY k1, k3' % (table_name)
    shoot_table = common.get_explain_rollup(normal_client, sql_1)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert index_name in shoot_table
    verify_schema_1 = [('k1', 'INT'), ('k3', 'INT'), ('v1', 'INT', 'SUM')]
    assert root_client.verify_by_sql(DATA.expected_data_file_list_2_b, sql_1, verify_schema_1)

    index_name_2 = 'index_2'
    assert normal_client.create_rollup_table(table_name, index_name_2, ['k1', 'v1'])
    time.sleep(5)
    ret = root_client.get_backend_list()
    be_list = util.get_attr(ret, palo_job.BackendProcInfo.Host)
    for be_host in be_list:
        node_operator.restart_be(be_host, 1)
        assert node_operator.is_be_alive(be_host)
    ret = normal_client.wait_table_rollup_job(table_name)
    sql_2 = 'SELECT k1, SUM(v1) FROM %s GROUP BY k1' % (table_name)
    if ret:
        shoot_table = common.get_explain_rollup(normal_client, sql_2)
        LOG.info(L('shoot table:', shoot_table=shoot_table))
        assert index_name_2 in shoot_table
    verify_schema_2 = [('k1', 'INT'), ('v1', 'INT', 'SUM')]
    assert root_client.verify_by_sql(DATA.expected_data_file_list_2_c, sql_2, verify_schema_2)

    assert normal_client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)

    assert root_client.verify_by_sql(DATA.expected_data_file_list_2_b * 2, sql_1, verify_schema_1)
    assert root_client.verify_by_sql(DATA.expected_data_file_list_2_c * 2, sql_2, verify_schema_2)
    root_client.clean(database_name)


def test_fe_load_image():
    """
    {
    "title": "test_fe_load_image",
    "describe": "fe检测image，然后重启，执行操作，生成新的image，然后fe重启，加载新生成的image，验证集群状态正常",
    "tag": "system,p1"
    }
    """
    """fe检测image，然后重启，执行操作，生成新的image，然后fe重启，加载新生成的image，验证集群状态正常"""
    check_fe_be()
    # 获取fe master以及fe元数据的image version
    retry_times = 10
    while retry_times > 0:
        try:
            client = palo_client.get_client(fe_observer, config.fe_query_port, user=config.fe_user, \
                password=config.fe_password)
            fe_list = client.get_fe_list()
            master1 = util.get_attr_condition_value(fe_list, palo_job.FrontendInfo.IsMaster, 
                                                    'true', palo_job.FrontendInfo.Host)
            LOG.info(L('get fe master', master=master1))
            break
        except Exception as e:
            pass
        retry_times -= 1
        time.sleep(3)
    version_list = node_operator.get_image_version(master1)
    retry_times = 30
    while retry_times > 0 and version_list is None:
        time.sleep(10)
        retry_times -= 1
        version_list = node_operator.get_image_version(master1)
    version1 = max(version_list)
    LOG.info(L('get master image version', version=version1))
    assert version1
    # 初始化db
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
        table_name=table_name, index_name=index_name))
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d',
            'partition_e', 'partition_f', 'partition_g']
    partition_value_list = ['5', '30', '100', '500', '1000', '2000', 'MAXVALUE']
    partition_info = palo_client.PartitionInfo('bigint_key',
            partition_name_list, partition_value_list)
    hash_distribution_info = palo_client.DistributionInfo(
                             distribution_type=DATA1.hash_partition_type,
                             bucket_num=DATA1.hash_partition_num)
    ret = client.create_table(table_name, DATA1.schema,
                              partition_info, distribution_info=hash_distribution_info,
                              storage_type=DATA1.storage_type)
    assert ret

    data_desc_list = palo_client.LoadDataInfo(DATA1.file_path, table_name)
    label = "%s_1" % database_name
    ret = client.batch_load(label, data_desc_list, max_filter_ratio="0.5", is_wait=True,
                            broker=broker_info)
    assert ret
    # 重启fe master，获取新的master
    node_operator.restart_fe(master1)
    time.sleep(5)
    assert node_operator.is_fe_alive(master1)
    LOG.info(L('restart fe master succ', master=master1))
    timeout = 20
    while timeout > 0:
        try:
            fe_list = client.get_fe_list()
            break
        except Exception as e:
            LOG.info(L('get fe master failed...'))
        time.sleep(3)
        timeout -= 1
    master2 = util.get_attr_condition_value(fe_list, palo_job.FrontendInfo.IsMaster,
                                            'true', palo_job.FrontendInfo.Host)
    LOG.info(L('get new master', master=master2))
    client.connect()
    # 不停的进行导入操作，以产生新的image version
    load_task = palo_task.BatchLoadTask(fe_observer, config.fe_query_port, database_name,
                                        label, data_desc_list, max_filter_ratio="0.05",
                                        is_wait=False, interval=5, broker=broker_info)
    load_thread = palo_task.TaskThread(load_task)
    load_thread.start()
    # 检测master是否产生新的image version
    retry_times = 30
    while retry_times > 0:
        retry_times -= 1
        version_list = node_operator.get_image_version(master2)
        version2 = max(version_list)
        try:
            if version2 and int(version2) > int(version1):
                LOG.info(L('get new master image version', master=master2, version=version2))
                break
        except Exception as e:
                LOG.info(L('get new master image version error', msg=str(e)))
        time.sleep(10)
    # 停止导入，如果没有产生新的image version则skip
    load_thread.stop()
    if retry_times == 0:
        raise pytest.skip('can not get new image version')
    # 重启fe master
    node_operator.restart_fe(master2)
    time.sleep(50)
    assert node_operator.is_fe_alive(master2)
    LOG.info(L('restart fe master succ', master=master2))
    time.sleep(50)
    # 验证新master结果与旧master结果与observer结果一致
    client1 = palo_client.get_client(master2, config.fe_query_port, user=config.fe_user, \
            password=config.fe_password)
    client1.use(database_name)
    client1.wait_table_load_job(database_name)
    ret1 = client1.select_all(table_name)
    client2 = palo_client.get_client(fe_observer, config.fe_query_port, user=config.fe_user, \
            password=config.fe_password)
    client2.use(database_name)
    retry_times = 10
    while retry_times > 10:
        retry_times -= 1
        try:
            client2.wait_table_load_job(database_name)
            break
        except Exception as e:
            client2.connect()
            time.sleep(3)
            print(str(e))
    ret2 = client2.select_all(table_name)
    util.check(ret1, ret2, True)
    client3 = palo_client.get_client(master1, config.fe_query_port, user=config.fe_user, \
            password=config.fe_password)
    client3.use(database_name)
    client3.wait_table_load_job(database_name)
    ret3 = client3.select_all(table_name)
    util.check(ret1, ret3, True)
    client1.clean(database_name)


