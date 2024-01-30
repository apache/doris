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
file:test_routine_load_property.py
测试routine load的load property
"""
import os
import sys
import time
import pytest
file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(file_dir)
file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(file_dir)

from lib import palo_config
from lib import palo_client
from lib import palo_job
from lib import kafka_config as kafka_config
from lib import util
from lib import common
from data import schema as DATA
from data import load_file as FILE

config = palo_config.config
# single partition
TOPIC = 'single-partition-3-%s' % config.fe_query_port
WAIT_TIME = 30
LOG = palo_client.LOG
L = palo_client.L


def setup_module():
    """
    init config
    """
    global query_db
    if 'FE_DB' in os.environ.keys():
        query_db = os.environ["FE_DB"]
    else:
        query_db = "test_query_qa"


def create_workspace(database_name):
    """get client and create db"""
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password)

    client.init()
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    return client


def check2(client, sql1, sql2, forced=False):
    """check 2 sql same result"""
    ret = False
    retry_times = 300
    while ret is False and retry_times > 0:
        try:
            ret1 = client.execute(sql1)
            ret2 = client.execute(sql2)
            util.check(ret1, ret2, forced)
            ret = True
        except Exception as e:
            print(str(e))
            ret = False
            time.sleep(1)
            retry_times -= 1
    assert ret, 'check data error'


def assertStop(ret, client, stop_job=None, info=''):
    """assert, if not stop routine load """
    if not ret and stop_job is not None:
        try:
            show_ret = client.show_routine_load(stop_job)
            LOG.info(L('routine load job info', ret=show_ret))
            client.stop_routine_load(stop_job)
        except Exception as e:
            print(str(e))
    assert ret, info


def wait_commit(client, routine_load_job_name, committed_expect_num, timeout=60):
    """wait task committed"""
    LOG.info(L('', expect_commited_rows=committed_expect_num))
    print('expect commited rows: %s\n' % committed_expect_num)
    while timeout > 0:
        ret = client.show_routine_load(routine_load_job_name,)
        routine_load_job = palo_job.RoutineLoadJob(ret[0])
        loaded_rows = routine_load_job.get_loaded_rows()
        print(loaded_rows)
        LOG.info(L('get loaded_rows', LoadedRows=loaded_rows))
        if str(loaded_rows) == str(committed_expect_num):
            LOG.info(L('all rows are loaded. check abort task num'))
            aborted_task_num = int(routine_load_job.get_task_aborted_task_num())
            LOG.info(L('get aborted task num', abortedTaskNum=aborted_task_num))
            print('aborted_task_num is ', aborted_task_num)
            while timeout > 0:
                ret = client.show_routine_load(routine_load_job_name)
                routine_load_job = palo_job.RoutineLoadJob(ret[0])
                task_num = int(routine_load_job.get_task_aborted_task_num())
                LOG.info(L('check aborted task num', orgin_aborted_task_num=aborted_task_num, 
                                                     now_aborted_task_num=task_num))
                print('origin task num is', aborted_task_num, ', now is ', task_num)
                if aborted_task_num < task_num:
                    break
                time.sleep(3)
                timeout -= 3
            return True
        state = routine_load_job.get_state()
        if state != 'RUNNING':
            print('routine load job\' state is not running, it\'s %s' % state)
            return False
        timeout -= 3
        time.sleep(3)
    return False


def test_db_name():
    """
    {
    "title": "test_routine_load_property.test_db_name",
    "describe": "导入指定db name",
    "tag": "function,p1"
    }
    """
    """导入指定db name"""
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password)
    client.init()
    # create db and table
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list, 
                              distribution_info=DATA.baseall_distribution_info)
    assert ret, 'create table failed'
    # create routine load
    routine_load_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    # check data
    sql = 'select * from %s' % table_name
    ret = client.execute(sql)
    ret = (len(ret) == 0)
    assertStop(ret, client, routine_load_name)
    # wait routine load turn running
    client.wait_routine_load_state(routine_load_name)
    state = client.get_routine_load_state(routine_load_name)
    ret = (state == 'RUNNING')
    assertStop(ret, client, routine_load_name, 'cat not get routine load running')
    # send data to kafka topic
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    # defualt maxBatchIntervalS is 10
    wait_commit(client, routine_load_name, 15)
    # check data
    sql = 'select * from %s' % table_name
    ret = client.execute(sql)
    ret = (len(ret) == 15)
    assertStop(ret, client, routine_load_name, 'check select result error')
    # check routine load job
    ret = client.show_routine_load(routine_load_name)
    assert_ret = (len(ret) == 1)
    assertStop(assert_ret, client, routine_load_name, 'expect 1 routine load job')
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    total_rows = routine_load_job.get_total_rows()
    ret = (total_rows == 15)
    assertStop(ret, client, routine_load_name, 'expect routine load total rows 15')
    ret = (routine_load_job.get_state() == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    # stop routine load
    ret = client.stop_routine_load(routine_load_name)
    assert ret, 'expect stop routine load succ'
    ret = client.show_routine_load(routine_load_name, is_all=True)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    state = routine_load_job.get_state()
    assert state == 'STOPPED', 'expect routine load stopped'
    # clean
    client.clean(database_name)


def test_routine_load_name():
    """
    {
    "title": "test_routine_load_property.test_routine_load_name",
    "describe": "一个db下,已有一个running job, 创建同名的routine load name失败",
    "tag": "function,p1,fuzz"
    }
    """
    """一个db下,已有一个running job, 创建同名的routine load name失败"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password)
    client.init()
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list,
                              distribution_info=DATA.baseall_distribution_info)
    assert ret, 'create table failed'
    routine_load_name = 'test_routine_load_name_job'
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    state = client.get_routine_load_state(routine_load_name)
    assert state != "STOPPED"
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)

    assert not ret, 'expect create routine load failed'
    client.stop_routine_load(routine_load_name)
    client.clean(database_name)


def test_routine_load_name_1():
    """
    {
    "title": "test_routine_load_property.test_routine_load_name_1",
    "describe": "一个db下,已有一个stopped job,创建同名的routine load name成功",
    "tag": "function,p1"
    }
    """
    """一个db下,已有一个stopped job,创建同名的routine load name成功"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password)
    client.init()
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list,
                              distribution_info=DATA.baseall_distribution_info)
    assert ret, 'expect create table failed'
    routine_load_name = 'test_routine_load_name_job'
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load'
    client.wait_routine_load_state(routine_load_name)
    state = client.get_routine_load_state(routine_load_name)
    ret = (state == 'RUNNING')
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    client.stop_routine_load(routine_load_name)
    client.wait_routine_load_state(routine_load_name, state="STOPPED")
    state = client.get_routine_load_state(routine_load_name)
    assert state == "STOPPED", 'expect routine load stopped'
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)

    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    state = client.get_routine_load_state(routine_load_name)
    assert state != "STOPPED", 'expect routine load not stopped'
    client.clean(database_name)


def test_dbs_routine_load_jobs():
    """
    {
    "title": "test_routine_load_property.test_dbs_routine_load_jobs",
    "describe": "不同db下的routine load job 同名",
    "tag": "function,p1"
    }
    """
    """不同db下的routine load job 同名"""
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password)
    client.init()
    database_name, table_name, rollup_table_name = util.gen_name_list()
    db_name_1 = database_name + "_1"
    db_name_2 = database_name + "_2"
    client.clean(db_name_1)
    client.clean(db_name_2)
    client.create_database(db_name_1)
    client.create_database(db_name_2)
    ret = client.create_table(table_name, DATA.baseall_column_list, database_name=db_name_1,
                              distribution_info=DATA.baseall_distribution_info)
    assert ret, 'create table failed'
    ret = client.create_table(table_name, DATA.baseall_column_list, database_name=db_name_2,
                              distribution_info=DATA.baseall_distribution_info)
    assert ret, 'create table failed'
    routine_load_name = "rt_" + database_name
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))

    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=db_name_1)
    assert ret, 'create routine load failed'
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=db_name_2)

    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name, database_name=db_name_1)
    client.wait_routine_load_state(routine_load_name, database_name=db_name_2)
    state = client.get_routine_load_state(routine_load_name, database_name=db_name_1)
    assert state == 'RUNNING', 'expect routine load running'
    state = client.get_routine_load_state(routine_load_name, database_name=db_name_2)
    assert state == "RUNNING", 'expect routine load running'
    client.stop_routine_load(routine_load_name, db_name_1)
    client.stop_routine_load(routine_load_name, db_name_2)
    client.clean(db_name_1)
    client.clean(db_name_2)


def test_multi_routine_job_in_same_db():
    """
    {
    "title": "test_routine_load_property.test_multi_routine_job_in_same_db",
    "describe": "一个db下创建多个routine load job",
    "tag": "function,p1"
    }
    """
    """一个db下创建多个routine load job"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password)
    client.init()
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    for i in range(0, 10):
        tb_name = "%s_%s" % (table_name, i)
        ret = client.create_table(tb_name, DATA.baseall_column_list,
                                  distribution_info=DATA.baseall_distribution_info)
        assert ret, 'create table failed'
    routine_load_name = 'test_routine_load_name_job'
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))

    for i in range(0, 10):
        routine_load_name_job = "%s_%s" % (routine_load_name, i)
        tb_name = "%s_%s" % (table_name, i)
        ret = client.routine_load(tb_name, routine_load_name_job, routine_load_property,
                                  database_name=database_name)
        assert ret, 'create routine load failed'
        client.wait_routine_load_state(routine_load_name_job)
        state = client.get_routine_load_state(routine_load_name_job)
        assert state == "RUNNING", 'expect routine load running'

    time.sleep(60)
    for i in range(0, 10):
        routine_load_name_job = "%s_%s" % (routine_load_name, i)
        client.stop_routine_load(routine_load_name_job)
        client.wait_routine_load_state(routine_load_name_job, state="STOPPED")
        state = client.get_routine_load_state(routine_load_name_job)
        assert state == "STOPPED", 'expect routine load stopped'

    client.clean(database_name)


def test_column_separator():
    """
    {
    "title": "test_routine_load_property.test_column_separator",
    "describe": "测试column separator, url编码",
    "tag": "function,p1"
    }
    """
    """测试column separator, url编码 """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list,
                              distribution_info=DATA.baseall_distribution_info)
    assert ret, 'expect create table failed'
    routine_load_name = 'test_routine_load_name_job'
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    routine_load_property.set_column_separator("\x09")

    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'expect routine load failed'
    client.wait_routine_load_state(routine_load_name)
    assert client.get_routine_load_state(routine_load_name) == "RUNNING", 'expect routine load running'
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 15)
    sql = 'select count(*) from %s.%s' % (database_name, table_name)
    ret = client.execute(sql)
    ret = (ret[0][0] == 15)
    assertStop(ret, client, routine_load_name, 'check select result error')
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    ret = client.stop_routine_load(routine_load_name)
    assert ret, 'expect stop routine load succ'
    client.clean(database_name)


def test_column_mapping_more_column():
    """
    {
    "title": "test_routine_load_property.test_column_mapping_more_column",
    "describe": "测试column mapping,比kafka中的数据多一列pause; 指定个不存在的列正确",
    "tag": "function,p1,fuzz"
    }
    """
    """测试column mapping,比kafka中的数据多一列pause; 指定个不存在的列正确"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list,
                              distribution_info=DATA.baseall_distribution_info)
    assert ret, 'create table failed'
    routine_load_name = 'test_routine_load_name_job'
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    column_mapping = DATA.baseall_column_name_list + ['v0']
    routine_load_property.set_column_mapping(column_mapping)

    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    time.sleep(WAIT_TIME)
    client.wait_routine_load_state(routine_load_name, 'PAUSED')
    ret = (client.get_routine_load_state(routine_load_name) == "PAUSED")
    assertStop(ret, client, routine_load_name, 'expect routine load paused')
    ret = client.stop_routine_load(routine_load_name)
    assert ret, 'expect stop routine load succ'
    sql = 'select count(*) from %s.%s' % (database_name, table_name)
    ret = client.execute(sql)
    assert ret[0][0] == 0, 'expect %s' % ret
    client.clean(database_name)


def test_column_mapping_lack_column():
    """
    {
    "title": "test_routine_load_property.test_column_mapping_lack_column",
    "describe": "测试column mapping,kafka中的数据少一列pause",
    "tag": "function,p1,fuzz"
    }
    """
    """测试column mapping,kafka中的数据少一列pause"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list,
                              distribution_info=DATA.baseall_distribution_info, set_null=True)
    assert ret, 'create table failed'
    routine_load_name = 'test_routine_load_name_job'
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    column_mapping = DATA.baseall_column_name_list[0:-1]
    routine_load_property.set_column_mapping(column_mapping)

    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    time.sleep(WAIT_TIME)
    ret = (client.get_routine_load_state(routine_load_name) == "PAUSED")
    assertStop(ret, client, routine_load_name, 'expect routine load paused')
    ret = client.stop_routine_load(routine_load_name)
    assert ret, 'expect stop routine load succ'
    sql = 'select count(*) from %s.%s' % (database_name, table_name)
    ret = client.execute(sql)
    assert ret[0][0] == 0, 'expect %s' % ret
    client.clean(database_name)


def test_column_mapping_expr():
    """
    {
    "title": "test_routine_load_property.test_column_mapping_expr",
    "describe": "测试column mapping, 导入时,指定列的表达式",
    "tag": "function,p1"
    }
    """
    """测试column mapping, 导入时,指定列的表达式"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list,
                              distribution_info=DATA.baseall_distribution_info, set_null=True)
    assert ret, 'create table failed'
    routine_load_name = 'test_routine_load_name_job'
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    column_mapping = DATA.baseall_column_name_list[0:-1] + ['no_use', 'k9=(k1+k8+k2-k3-k5)/k2']
    routine_load_property.set_column_mapping(column_mapping)

    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 15)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    ret = client.stop_routine_load(routine_load_name)
    assert ret, 'expect stop routine load succ'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select k1, k2, k3, k4, k5, k6, k10, k11, k7, k8, (k1+k8+k2-k3-k5)/k2 from %s.baseall ' \
           'order by k1' % query_db
    check2(client, sql1, sql2)
    client.clean(database_name)


def test_column_mapping_hll():
    """
    {
    "title": "test_routine_load_property.test_column_mapping_hll",
    "describe": "测试column mapping, 导入hll列",
    "tag": "function,p1"
    }
    """
    """测试column mapping, 导入hll列"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.hll_varchar_column_list,
                              distribution_info=DATA.hash_distribution_info)
    assert ret, 'create table failed'
    routine_load_name = 'test_routine_load_name_job'
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    routine_load_property.set_max_error_number(13)
    column_mapping = ['k1', 'v1=hll_hash(k1)', 'v2=1', 'dirty']
    routine_load_property.set_column_mapping(column_mapping)
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'routine load state is not running')
    kafka_config.send_to_kafka(TOPIC, '../STREAM_LOAD/test_hash_varchar_least.data')
    wait_commit(client, routine_load_name, 1034)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'routine load state is not running')
    ret = client.stop_routine_load(routine_load_name)
    assert ret
    sql = 'select count(k1) from %s.%s ' % (database_name, table_name)
    ret = client.execute(sql)
    assert ret[0][0] == 60, 'sql result error'
    client.clean(database_name)


def test_column_mapping_null():
    """
    {
    "title": "test_routine_load_property.test_column_mapping_null",
    "describe": "测试column mapping,导入Null值",
    "tag": "function,p1"
    }
    """
    """测试column mapping,导入Null值"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.types_kv_column_list,
                              distribution_info=DATA.baseall_distribution_info,
                              set_null=True)
    assert ret, 'create table failed'
    routine_load_name = 'test_routine_load_name_job'
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    routine_load_property.set_column_mapping(['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k8', 
                                              'k9', 'k10', 'v1', 'v2', 'v3', 'v4', 'v5', 'v6',
                                              'v7', 'v8', 'v9', 'tmp', 'v10=ifnull(tmp, "0")'])
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'routine load state is not running')
    kafka_config.send_to_kafka(TOPIC, '../NULL/data_5')
    wait_commit(client, routine_load_name, 12)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    ret = client.stop_routine_load(routine_load_name)
    assert ret
    expect_file = '%s/data/NULL/verify_5_3' % file_dir
    assert client.verify(expect_file, table_name), 'verify without schema failed'
    client.clean(database_name)


def test_column_mapping_func():
    """
    {
    "title": "test_routine_load_property.test_column_mapping_func",
    "describe": "测试column mapping,表达式中有函数,包括替换字符,数字,日期的函数",
    "tag": "function,p1"
    }
    """
    """测试column mapping,表达式中有函数,包括替换字符,数字,日期的函数"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list, set_null=True,
                              distribution_info=DATA.baseall_distribution_info)
    assert ret, 'create table failed'
    routine_load_name = 'test_routine_load_name_job'
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    column_mapping = ['v1', 'v2', 'v3', 'v4', 'v5', 'v6', 'v10', 'v11','v7', 'v8', 'v9', 
                      'k1=v1*2', 'k2=round(v2/10)', 'k3=ceil(v5)', 'k4=round(ln(abs(v4)))', 
                      'k5=sin(v3)', 'k6=ucase(v6)', 'k7=substr(v7,3, 8)', 
                      'k10=date_add(v10, interval "13" week)', 'k11=adddate(v11, interval 31 day)',
                      'k8=floor(v8)', 'k9=abs(v9)']
    routine_load_property.set_column_mapping(column_mapping)

    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 15)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    ret = client.stop_routine_load(routine_load_name)
    assert ret, 'expect stop routine load succ'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select 2*k1, round(k2/10), ceil(k5), round(ln(abs(k4))), sin(k3), ucase(k6), ' \
           'date_add(k10, interval "13" week), adddate(k11, interval 31 day), substr(k7, 3, 8),' \
           'floor(k8), abs(k9) from %s.baseall order by k1' % query_db
    check2(client, sql1, sql2)
    client.clean(database_name)


def test_column_mapping_error_func():
    """
    {
    "title": "test_routine_load_property.test_column_mapping_error_func",
    "describe": "测试column mapping,表达式中有错误函数",
    "tag": "function,p1,fuzz"
    }
    """
    """测试column mapping,表达式中有错误函数"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list, set_null=True,
                              distribution_info=DATA.baseall_distribution_info)
    assert ret, 'create table failed'
    routine_load_name = 'test_routine_load_name_job'
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    column_mapping = ['v1', 'v2', 'v3', 'v4', 'v5', 'v6', 'v10', 'v11','v7', 'v8', 'v9',
                      'k1=v1*2', 'k2=round(v2/10)', 'k3=ceil(v5)', 'k4=round(ln(abs(v4)))',
                      'k5=sin(v3)', 'k6=ucase(v6)', 'k7=substr(v7,3, 8)',
                      'k10=date_add(v10, interval "13" week)', 'k11=adddate(v11, interval 31 day)',
                      'k8=zt(v8)', 'k9=abs(v9)']
    routine_load_property.set_column_mapping(column_mapping)

    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create rotuine load failed'
    client.wait_routine_load_state(routine_load_name)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    client.wait_routine_load_state(routine_load_name, 'PAUSED')
    assert client.get_routine_load_state(routine_load_name) == 'PAUSED', \
           'expect routine load paused'
    client.clean(database_name)


def test_column_mapping_case_when():
    """
    {
    "title": "test_routine_load_property.test_column_mapping_case_when",
    "describe": "测试column mapping,表达式为case when",
    "tag": "function,p1"
    }
    """
    """测试column mapping,表达式为case when"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list,
                              distribution_info=DATA.baseall_distribution_info)
    assert ret, 'create table failed'
    routine_load_name = 'test_routine_load_name_job'
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    column_mapping = ['v1', 'v2', 'v3', 'v4', 'v5', 'v6', 'v10', 'v11', 'v7', 'v8', 'v9', 
                      'k1=case when cast(v1 as int) > 10 then cast((v1 + "100") as varchar) ' \
                      'when cast(v1 as int) > 1 and cast(v1 as int) < 5 then cast(-v1 as varchar)' \
                      ' else cast(v1 as varchar) end', 
                      'k2=v2', 'k3=v3', 'k4=v4', 'k5=v5', 
                      'k6=case v6 when "true" then "True" when "false" then "False" else "None" end', 
                      'k7=case when v7 like "%wang%" then "wang" else v7 end', 'k10=v10', 
                      'k11=v11', 'k8=v8', 'k9=v9']
    routine_load_property.set_column_mapping(column_mapping)

    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 15)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    ret = client.stop_routine_load(routine_load_name)
    assert ret, 'expect stop routine load succ'
    sql1 = 'select * from %s.%s order by 1, 2, 3, 4' % (database_name, table_name)
    sql2 = 'select case when k1 > 10 then k1 + 100 when k1 > 1 and k1 < 5 then -k1 else k1 end a,' \
           ' k2, k3, k4, k5, case k6 when "true" then "True" when "false" then "False" else "None" end b, ' \
           'k10, k11, case when k7 like "%%wang%%" then "wang" else k7 end c, k8, k9 ' \
           'from %s.baseall order by 1, 2, 3, 4' % query_db
    check2(client, sql1, sql2)
    client.clean(database_name)


def test_column_mapping_if():
    """
    {
    "title": "test_routine_load_property.test_column_mapping_if",
    "describe": "测试column mapping,表达式为if, ifnull, nullif",
    "tag": "function,p1"
    }
    """
    """测试column mapping,表达式为if, ifnull, nullif"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list,
                              distribution_info=DATA.baseall_distribution_info, set_null=True)
    assert ret, 'create table failed'
    routine_load_name = 'test_routine_load_name_job'
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    column_mapping = ['v1', 'v2', 'v3', 'v4', 'v5', 'v6', 'v10', 'v11', 'v7', 'v8', 'v9',
                      'k1=nullif(v1, "100")', 'k2=if(v2 > "0", v2, cast(-v2 as varchar))',
                      'k3=v3', 'k4=v4', 'k5=v5',
                      'k6=nullif(v6, "false")',
                      'k7=v7', 'k10=v10', 'k11=v11', 'k8=v8', 'k9=v9']
    routine_load_property.set_column_mapping(column_mapping)

    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine loaf failed'
    client.wait_routine_load_state(routine_load_name)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 15)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    ret = client.stop_routine_load(routine_load_name)
    assert ret, 'expect stop routine load succ'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select nullif(k1, 100), if(k2>0, k2, -k2), k3, k4, k5, nullif(k6,"false"),' \
           ' k10, k11, k7, k8, k9 from %s.baseall order by k1' % query_db
    check2(client, sql1, sql2)
    client.clean(database_name)


def test_where_predicate_and():
    """
    {
    "title": "test_routine_load_property.test_where_predicate_and",
    "describe": "测试where predicates, and复合谓词",
    "tag": "function,p1"
    }
    """
    """测试where predicates, and复合谓词"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list,
                              distribution_info=DATA.baseall_distribution_info, set_null=True)
    assert ret, 'create table failed'
    routine_load_name = 'test_routine_load_name_job'
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    column_mapping = DATA.baseall_column_name_list
    routine_load_property.set_column_mapping(column_mapping)
    where_predicate = 'k1 between 1 and 13 and k2 is not null and k6 = "true" and ' \
                      'k10 < "2019-01-01" and k11 < "2019-01-01"'
    routine_load_property.set_where_predicates(where_predicate)

    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    ret = client.get_routine_load_state(routine_load_name) == "RUNNING"
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 5)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    ret = client.stop_routine_load(routine_load_name)
    assert ret, 'expect stop routine load succ'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall where k1 between 1 and 13 and k2 is not null ' \
           'and k6 = "true" and k10 < "2019-01-01" and k11 < "2019-01-01" order by k1' % query_db
    check2(client, sql1, sql2)
    client.clean(database_name)


def test_where_predicate_or():
    """
    {
    "title": "test_routine_load_property.test_where_predicate_or",
    "describe": "测试where predcates, or复合谓词",
    "tag": "function,p1"
    }
    """
    """测试where predcates, or复合谓词"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list,
                              distribution_info=DATA.baseall_distribution_info, set_null=True)
    assert ret, 'create table failed'
    routine_load_name = 'test_routine_load_name_job'
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    column_mapping = DATA.baseall_column_name_list
    routine_load_property.set_column_mapping(column_mapping)
    where_predicate = 'k1 between 10 and 13 or k2 is null or lower(k7) regexp".*04$" or ' \
                      'k10 > "2019-01-01" or k11 > "2019-01-01"'
    routine_load_property.set_where_predicates(where_predicate)

    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 9)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    ret = client.stop_routine_load(routine_load_name)
    assert ret, 'expect stop routine load succ'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall where k1 between 10 and 13 or k2 is null or ' \
           'lower(k7) regexp".*04$" or k10 > "2019-01-01" or k11 > "2019-01-01" order by k1' \
           % query_db
    check2(client, sql1, sql2)
    client.clean(database_name)


def test_where_predicate_cmp():
    """
    {
    "title": "test_routine_load_property.test_where_predicate_cmp",
    "describe": "测试where predicates, 比较过滤",
    "tag": "function,p1"
    }
    """
    """测试where predicates, 比较过滤"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list,
                              distribution_info=DATA.baseall_distribution_info, set_null=True)
    assert ret, 'create table failed'
    routine_load_name = 'test_routine_load_name_job'
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    column_mapping = DATA.baseall_column_name_list
    routine_load_property.set_column_mapping(column_mapping)
    where_predicate = 'k1 > 10 and k6 >= "false" and k1 != 1 and k10 < "2019-01-01"'
    routine_load_property.set_where_predicates(where_predicate)

    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 4)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    ret = client.stop_routine_load(routine_load_name)
    assert ret, 'expect stop routine load succ'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall where k1 > 10 and k6 >= "false" and k1 != 1 and ' \
           'k10 < "2019-01-01" order by k1' % query_db
    check2(client, sql1, sql2)
    client.clean(database_name)


def test_where_predicate_in():
    """
    {
    "title": "test_routine_load_property.test_where_predicate_in",
    "describe": "测试where predicates, in / not in",
    "tag": "function,p1"
    }
    """
    """测试where predicates, in / not in"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list,
                              distribution_info=DATA.baseall_distribution_info, set_null=True)
    assert ret, 'create table failed'
    routine_load_name = 'test_routine_load_name_job'
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    column_mapping = DATA.baseall_column_name_list
    routine_load_property.set_column_mapping(column_mapping)
    where_predicate = 'k1 in (1, 2, 3, 4, 5, 6) and k6 not in ("true", "hello")'
    routine_load_property.set_where_predicates(where_predicate)

    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 3)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    ret = client.stop_routine_load(routine_load_name)
    assert ret
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall where k1 in (1, 2, 3, 4, 5, 6) and ' \
           'k6 not in ("true", "hello") order by k1' % query_db
    check2(client, sql1, sql2)
    client.clean(database_name)


def test_where_predicate_like():
    """
    {
    "title": "test_routine_load_property.test_where_predicate_like",
    "describe": "测试where predicate, like",
    "tag": "function,p1"
    }
    """
    """测试where predicate, like"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list,
                              distribution_info=DATA.baseall_distribution_info, set_null=True)
    assert ret, 'create table failed'
    routine_load_name = 'test_routine_load_name_job'
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    column_mapping = DATA.baseall_column_name_list
    routine_load_property.set_column_mapping(column_mapping)
    where_predicate = 'k6 like "true" and k7 like "wang%%"'
    routine_load_property.set_where_predicates(where_predicate)

    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 3)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    ret = client.stop_routine_load(routine_load_name)
    assert ret
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall where k6 like "true" and k7 like "wang%%" order by k1' \
           % query_db
    check2(client, sql1, sql2)
    client.clean(database_name)


def test_partitions():
    """
    {
    "title": "test_routine_load_property.test_partitions",
    "describe": "测试指定palo表的partitions",
    "tag": "function,p1"
    }
    """
    """测试指定partitions"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list,
                              distribution_info=DATA.baseall_distribution_info,
                              partition_info=DATA.baseall_tinyint_partition_info, set_null=True)
    assert ret, 'create table failed'
    routine_load_name = 'test_routine_load_name_job'
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    routine_load_property.set_partitions(['p1', 'p2', 'p3'])
    routine_load_property.set_max_error_number(6)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 9)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    ret = client.stop_routine_load(routine_load_name)
    assert ret, 'expect stop routine load succ'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall where k1 < 10 order by k1' % query_db
    check2(client, sql1, sql2)
    client.clean(database_name)


def test_json_load_object():
    """
    {
    "title": "test_routine_load_property.test_json_load_object",
    "describe": "json导入object",
    "tag": "function,p1"
    }
    """
    """测试指定partitions"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.partition_column_list,
                              distribution_info=DATA.baseall_distribution_info,
                              partition_info=DATA.baseall_tinyint_partition_info, set_null=True)
    assert ret, 'create table failed'
    routine_load_name = 'test_routine_load_name_job'
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    routine_load_property.set_partitions(['p1', 'p2', 'p3'])
    routine_load_property.set_max_error_number(6)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    routine_load_property.set_job_property('format', 'json')
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    kafka_config.send_to_kafka(TOPIC, 'json_object_basic.json')
    wait_commit(client, routine_load_name, 1)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    ret = client.stop_routine_load(routine_load_name)
    assert ret, 'expect stop routine load succ'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select 1, 10, 100, 1000, cast("2011-01-01 00:00:00" as datetime), ' \
           'cast("2010-01-01" as date), "t", "ynqnzeowymt", 38.638843, 180.998031, 7395.231067'
    check2(client, sql1, sql2)
    client.clean(database_name)


def test_json_load_array():
    """
    {
    "title": "test_routine_load_property.test_json_load",
    "describe": "json导入object",
    "tag": "function,p1"
    }
    """
    """测试指定partitions"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.partition_column_list,
                              distribution_info=DATA.baseall_distribution_info,
                              partition_info=DATA.baseall_tinyint_partition_info, set_null=True)
    assert ret, 'create table failed'
    routine_load_name = 'test_routine_load_name_job'
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    routine_load_property.set_partitions(['p1', 'p2', 'p3', 'p4', 'p5'])
    routine_load_property.set_max_error_number(1)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    routine_load_property.set_job_property('format', 'json')
    routine_load_property.set_job_property('strip_outer_array', 'true')
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    kafka_config.send_to_kafka(TOPIC, 'partition_type.json')
    wait_commit(client, routine_load_name, 15)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    print(routine_load_job.get_error_rows())
    
    error_rows = routine_load_job.get_error_rows()
    assertStop(error_rows == 1, client, routine_load_name, 'expect error rows: 1')
    ret = client.stop_routine_load(routine_load_name)
    assert ret, 'expect stop routine load succ'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    ret = client.execute(sql1)
    assert len(ret) == 15
    client.clean(database_name)


def test_json_load_error_num():
    """
    {
    "title": "test_routine_load_property.test_json_load",
    "describe": "json导入object",
    "tag": "function,p1"
    }
    """
    """测试指定partitions"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.partition_column_list,
                              distribution_info=DATA.baseall_distribution_info,
                              partition_info=DATA.baseall_tinyint_partition_info, set_null=True)
    assert ret, 'create table failed'
    routine_load_name = 'test_routine_load_name_job'
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    routine_load_property.set_partitions(['p1', 'p2', 'p3'])
    routine_load_property.set_max_error_number(5)
    routine_load_property.set_job_property('format', 'json')
    routine_load_property.set_job_property('strip_outer_array', 'true')
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    kafka_config.send_to_kafka(TOPIC, 'partition_type.json')
    wait_commit(client, routine_load_name, 9)
    ret = (client.get_routine_load_state(routine_load_name) == "PAUSED")
    assertStop(ret, client, routine_load_name, 'expect routine load paused')
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    error_rows = routine_load_job.get_error_rows()
    assertStop(error_rows == 7, client, routine_load_name, 'expect error rows: 7')
    ret = client.stop_routine_load(routine_load_name)
    assert ret, 'expect stop routine load succ'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    ret = client.execute(sql1)
    assert len(ret) == 9
    client.clean(database_name)


def test_json_load_jsonpath():
    """
    {
    "title": "test_routine_load_property.test_json_load",
    "describe": "json导入object",
    "tag": "function,p1"
    }
    """
    """测试指定partitions"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.partition_column_list,
                              distribution_info=DATA.baseall_distribution_info,
                              partition_info=DATA.baseall_tinyint_partition_info, set_null=True)
    assert ret, 'create table failed'
    routine_load_name = 'test_routine_load_name_job'
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    routine_load_property.set_partitions(['p1', 'p2', 'p3'])
    routine_load_property.set_max_error_number(1)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    routine_load_property.set_job_property('format', 'json')
    routine_load_property.set_job_property('strip_outer_array', 'true')
    jsonpaths = "[\\\"$.k1\\\", \\\"$.k2\\\", \\\"$.k3\\\", \\\"$.k4\\\", \\\"$.k5\\\", \\\"$.v1\\\", \\\"$.v2\\\", " \
                 "\\\"$.v3\\\", \\\"$.v4\\\", \\\"$.v5\\\", \\\"$.v6\\\"]"
    routine_load_property.set_job_property('jsonpaths', jsonpaths)
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    kafka_config.send_to_kafka(TOPIC, 'partition_type.json')
    wait_commit(client, routine_load_name, 9)
    ret = (client.get_routine_load_state(routine_load_name) == "PAUSED")
    assertStop(ret, client, routine_load_name, 'expect routine load paused')
    ret = client.stop_routine_load(routine_load_name)
    assert ret, 'expect stop routine load succ'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    ret = client.execute(sql1)
    assert len(ret) == 9, ret
    client.clean(database_name)


def test_json_load_jsonroot():
    """
    {
    "title": "test_routine_load_property.test_json_load",
    "describe": "json导入object",
    "tag": "function,p1"
    }
    """
    """测试指定partitions"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    column_list  = [('name', 'CHAR(5)'), ('type', 'VARCHAR(20)'), ('aggregation', 'CHAR(5)'),
                    ('length', 'INT'), ('is_root_column', 'BOOLEAN')]
    ret = client.create_table(table_name, column_list,
                              distribution_info=palo_client.DistributionInfo('Hash(name)', 5),
                              set_null=True)
    assert ret, 'create table failed'
    routine_load_name = 'test_routine_load_name_job'
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    routine_load_property.set_max_error_number(6)
    routine_load_property.set_job_property('format', 'json')
    routine_load_property.set_job_property('json_root', '$.column')
    routine_load_property.set_job_property('strip_outer_array', 'true')
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    kafka_config.send_to_kafka(TOPIC, 'object.json')
    wait_commit(client, routine_load_name, 3)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    ret = client.stop_routine_load(routine_load_name)
    assert ret, 'expect stop routine load succ'
    sql1 = 'select * from %s' % table_name
    sql2 = 'select "k1", "INT", "NONE", 4, 1 union select "k2", "SMALLINT", "NONE", ' \
           '2, 1 union select "k9", "FLOAT", "SUM", 4, 1'
    check2(client, sql1, sql2, True)
    client.clean(database_name)


def test_array_load():
    """
    {
    "title": "test_routine_load_property.test_array_load",
    "describe": "导入array类型",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.show_variables('enable_vectorized_engine')
    if len(ret) == 1 and ret[0][1] == 'false':
        raise pytest.skip('skip if enable_vectorized_engine is false')

    ret = client.create_table(table_name, DATA.array_table_list, keys_desc=DATA.duplicate_key,
                              partition_info=DATA.baseall_tinyint_partition_info)
    assert ret, 'create table failed'
    routine_load_name = table_name + '_job'
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    routine_load_property.set_column_separator("|")
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    kafka_config.send_to_kafka(TOPIC, 'array_test.data')
    wait_commit(client, routine_load_name, 255)
    ret = (client.get_routine_load_state(routine_load_name) == "RUNNING")
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    ret = client.stop_routine_load(routine_load_name)
    assert ret, 'expect stop routine load succ'
    assert client.verify(FILE.expe_array_table_file, table_name), 'check data failed'
    client.clean(database_name)


if __name__ == '__main__':
    setup_module()
    test_db_name()
    # test_json_load_object()
    # test_json_load_array()
    # test_json_load_error_num()
    # test_json_load_jsonpath()
    #test_json_load_jsonroot()

