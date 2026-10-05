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
file:test_routine_load_with_alter.py
测试routine load 和schema change交叉场景
"""
import os
import sys
import time
file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(file_dir)
file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(file_dir)

from data import schema as DATA
from lib import palo_config
from lib import palo_client
from lib import palo_job
import kafka_config as kafka_config
from lib import util
from lib import common

config = palo_config.config
# single partition
TOPIC = 'single-partition-4-%s' % config.fe_query_port
WAIT_TIME = 46
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


def check2(client, sql1, sql2):
    """check 2 sql same result"""
    ret = False
    retry_times = 300
    while ret is False and retry_times > 0:
        try:
            ret1 = client.execute(sql1)
            ret2 = client.execute(sql2)
            util.check(ret1, ret2)
            ret = True
        except Exception as e:
            print(str(e))
            ret = False
            time.sleep(1)
            retry_times -= 1
    return ret


def assertStop(ret, client, stop_job=None, info=''):
    """assert, if not stop routine load """
    if not ret and stop_job is not None:
        try:
            show_ret = client.show_routine_load(stop_job)
            LOG.info(L('routine load job info', ret=show_ret))
            client.stop_routine_load(stop_job)
        except Exception as e:
            print(str(e))
            LOG.info(L('stop routine load error', msg=str(e)))
    assert ret, info


def wait_commit(client, routine_load_job_name, committed_expect_num, timeout=60):
    """wait task committed"""
    print('expect commited loaded rows: %s\n' % committed_expect_num)
    LOG.info(L('', expect_commited_rows=committed_expect_num))
    while timeout > 0:
        ret = client.show_routine_load(routine_load_job_name)
        if len(ret) == 0:
            return False
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
                time.sleep(3)
                timeout -= 3
                ret = client.show_routine_load(routine_load_job_name)
                routine_load_job = palo_job.RoutineLoadJob(ret[0])
                task_num = int(routine_load_job.get_task_aborted_task_num())
                LOG.info(L('check aborted task num', orgin_aborted_task_num=aborted_task_num, 
                                                     now_aborted_task_num=task_num))
                print('origin task num is', aborted_task_num, ', now is ', task_num)
                if aborted_task_num < task_num:
                    break
            return True
        timeout -= 3
        time.sleep(3)
    return False


def create_workspace(database_name):
    """get client and create db"""
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password)

    client.init()
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    return client


def test_rename_partition():
    """
    {
    "title": "test_routine_load_with_alter.test_rename_partition",
    "describe": "routine load指定partition name, 修改partition name, 导入正确",
    "tag": "system,p1,stability"
    }
    """
    """routine load指定partition name, 修改partition name, 导入正确"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list, 
                              distribution_info=DATA.baseall_distribution_info, 
                              partition_info=DATA.baseall_tinyint_partition_info)
    assert ret, 'create table failed'
    routine_load_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    routine_load_property.set_partitions(['p1', 'p2', 'p3', 'p4'])
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 15)
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall order by k1' % query_db
    ret = check2(client, sql1, sql2)
    assertStop(ret, client, routine_load_name, 'check data error')
    rename = 'rename_p1'
    ret = client.rename_partition(rename,  'p1', table_name)
    assertStop(ret, client, routine_load_name, 'rename partition failed')
    ret = client.get_partition(table_name, rename)
    assertStop(ret, client, routine_load_name, 'cannot get partition %s' % rename)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    # wait_commit(client, routine_load_name, 30)
    time.sleep(30)
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'PAUSED')
    assertStop(ret, client, routine_load_name, 'expect routine load paused')
    print(routine_load_job.get_reason_of_state_changed())
    client.stop_routine_load(routine_load_name)
    # avoid:There are still some transactions in the COMMITTED state waiting to be completed. [db] cannot be dropped.
    #       If you want to forcibly drop(cannot be recovered), please use "DROP database FORCE
    common.execute_ignore_error(client.clean, database_name)


def test_rename_table():
    """
    {
    "title": "test_routine_load_with_alter.test_rename_table",
    "describe": "rename routine load的表的名称",
    "tag": "system,p1,stability"
    }
    """
    """rename routine load的表的名称"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list, 
                              distribution_info=DATA.baseall_distribution_info, 
                              partition_info=DATA.baseall_tinyint_partition_info)
    assert ret, 'create table failed'
    routine_load_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    routine_load_property.set_partitions(['p1', 'p2', 'p3', 'p4'])
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 15)
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall order by k1' % query_db
    ret = check2(client, sql1, sql2)
    assertStop(ret, client, routine_load_name, 'check data error')
    rename = 'rename_tb'
    ret = client.rename_table(rename, table_name)
    assertStop(ret, client, routine_load_name, 'rename table failed')
    ret = client.show_tables(rename)
    assertStop(ret, client, routine_load_name, 'failed get table %s' % rename)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 30)
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'RUNNING')
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    ret = (routine_load_job.get_table_name() == rename)
    assertStop(ret, client, routine_load_name, 'routine load table name wrong')
    client.stop_routine_load(routine_load_name)
    sql1 = 'select * from %s.%s order by k1' % (database_name, rename)
    sql2 = 'select k1, k2, k3, k4, k5, k6, k10, k11, k7, k8, k9 * 2 from %s.baseall ' \
           'order by k1' % query_db
    assert check2(client, sql1, sql2)
    common.execute_ignore_error(client.clean, database_name)


def test_rename_db():
    """
    {
    "title": "test_routine_load_with_alter.test_rename_db",
    "describe": "rename database name",
    "tag": "system,p1,stability"
    }
    """
    """rename database name"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    client.clean('rename_db')
    ret = client.create_table(table_name, DATA.baseall_column_list, 
                              distribution_info=DATA.baseall_distribution_info, 
                              partition_info=DATA.baseall_tinyint_partition_info)
    assert ret, 'create table failed'
    routine_load_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    routine_load_property.set_partitions(['p1', 'p2', 'p3', 'p4'])
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 15)
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall order by k1' % query_db
    ret = check2(client, sql1, sql2)
    assertStop(ret, client, routine_load_name, 'check data error')
    rename = 'rename_db'
    ret = client.rename_database(rename, database_name)
    client.use(rename)
    ret = client.get_database_id(rename)
    assertStop(ret, client, routine_load_name, 'can not get db %s' % rename)
    time.sleep(10)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    time.sleep(10)
    ret = client.show_routine_load(routine_load_name, database_name)
    assertStop(not ret, client, routine_load_name, 'expect show routine load failed')
    client.use(rename)
    wait_commit(client, routine_load_name, 30)
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'RUNNING')
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    ret = (rename in routine_load_job.get_db_name())
    assertStop(ret, client, routine_load_name, 'expect get db %s' % rename)
    sql1 = 'select * from %s.%s order by k1' % (rename, table_name)
    sql2 = 'select k1, k2, k3, k4, k5, k6, k10, k11, k7, k8, k9 * 2 from %s.baseall ' \
           'order by k1' % query_db
    ret = check2(client, sql1, sql2)
    client.stop_routine_load(routine_load_name)
    assert ret, 'check data error'
    common.execute_ignore_error(client.clean, rename)


def test_modify_column():
    """
    {
    "title": "test_routine_load_with_alter.test_modify_column",
    "describe": "修改列的类型, tinyint → smallint → int → bigint → largeint, char/varchar的长度",
    "tag": "system,p1,stability"
    }
    """
    """
    修改列的类型, tinyint → smallint → int → bigint → largeint, char/varchar的长度
    列的顺序,指定column  mapping
    decimal 精度不支持修改,不能修改distributed列
    """
    # init db & table
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
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
    routine_load_property.set_column_mapping(DATA.baseall_column_name_list)
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    # wait routine load running & send data to kafka
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 15)
    # schema change modify column & wait sc job finished
    new_column = ['k2 int', 'k3 bigint', 'k4 largeint', 'k6 char(10)', 'k7 varchar(50)']
    ret = client.schema_change(table_name, modify_column_list=new_column, is_wait=True)
    assertStop(ret, client, routine_load_name, 'schema change failed')
    ret = client.get_database_schema_change_job_list(database_name)
    ret = (palo_job.SchemaChangeJob(ret[0]).get_state() == 'FINISHED')
    assertStop(ret, client, routine_load_name, 'expect schema change finished')
    # after schema change, send data to kafka, load data
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 30)
    # check routine load job state and check result
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'RUNNING')
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    client.stop_routine_load(routine_load_name)
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select k1, k2, k3, cast(k4 as string), k5, k6, k10, k11, k7, k8, k9 * 2 from %s.baseall ' \
           'order by k1' % query_db
    assert check2(client, sql1, sql2)
    # stop routine load & clean db
    common.execute_ignore_error(client.clean, database_name)


def test_add_column():
    """
    {
    "title": "test_routine_load_with_alter.test_add_column",
    "describe": "without column mapping, routine load with default value",
    "tag": "system,p1,stability"
    }
    """
    """without column mapping, routine load with default value"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list, 
                              distribution_info=DATA.baseall_distribution_info)
    assert ret, 'create table failed'
    routine_load_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    routine_load_property.set_column_mapping(DATA.baseall_column_name_list)
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 15)
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall order by k1' % query_db
    ret = check2(client, sql1, sql2)
    assertStop(ret, client, routine_load_name, 'check data error')
    new_column = ['add_v int sum default "0"']
    ret = client.schema_change(table_name, add_column_list=new_column, is_wait=True)
    assertStop(ret, client, routine_load_name, 'schema change failed')
    ret = client.get_database_schema_change_job_list(database_name)
    ret = (palo_job.SchemaChangeJob(ret[0]).get_state() == 'FINISHED')
    assertStop(ret, client, routine_load_name, 'expect sc job finished')
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 30)
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'RUNNING')
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    ret = client.desc_table(table_name)
    ret = len(ret) == 12
    assertStop(ret, client, routine_load_name, 'sc job not work')
    client.stop_routine_load(routine_load_name)
    common.execute_ignore_error(client.clean, database_name)


def test_drop_column():
    """
    {
    "title": "test_routine_load_with_alter.test_drop_column",
    "describe": "with column mapping, routine load is ok",
    "tag": "system,p1,stability"
    }
    """
    """with column mappinp, routine load is ok"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list, 
                              distribution_info=DATA.baseall_distribution_info)
    assert ret, 'create table failed'
    routine_load_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    routine_load_property.set_column_mapping(DATA.baseall_column_name_list)
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 15)
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall order by k1' % query_db
    ret = check2(client, sql1, sql2)
    assertStop(ret, client, routine_load_name, 'check data error')
    drop_column = ['k11', 'k9']
    ret = client.schema_change(table_name, drop_column_list=drop_column, is_wait=True)
    assertStop(ret, client, routine_load_name, 'schema change failed')
    ret = client.get_database_schema_change_job_list(database_name)
    ret = (palo_job.SchemaChangeJob(ret[0]).get_state())
    assertStop(ret, client, routine_load_name, 'expect sc job finished')
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 30)
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'RUNNING')
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select k1, k2, k3, k4, k5, k6, k10, k7, k8 from %s.baseall ' \
           'order by k1' % query_db
    ret = check2(client, sql1, sql2)
    client.stop_routine_load(routine_load_name)
    assert ret
    common.execute_ignore_error(client.clean, database_name)


def test_create_rollup():
    """
    {
    "title": "test_routine_load_with_alter.test_create_rollup",
    "describe": "create rollup and drop rollup",
    "tag": "system,p1,stability"
    }
    """
    """create rollup and drop rollup"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list, 
                              distribution_info=DATA.baseall_distribution_info)
    assert ret, 'create table failed'
    routine_load_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    routine_load_property.set_column_mapping(DATA.baseall_column_name_list)
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 15)
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall order by k1' % query_db
    ret = check2(client, sql1, sql2)
    assertStop(ret, client, routine_load_name, 'check data error')
    rollup_column = ['k1', 'k9']
    ret = client.create_rollup_table(table_name, rollup_table_name, column_name_list=rollup_column, 
                                     is_wait=True)
    assertStop(ret, client, routine_load_name, 'create rollup table failed')
    ret = client.get_table_rollup_job_list(table_name)
    assertStop(ret, client, routine_load_name, 'get rollup job failed')
    ret = (palo_job.RollupJob(ret[0]).get_state() == 'FINISHED')
    assertStop(ret, client, routine_load_name, 'expect rollup job finished')
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 30)
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'RUNNING')
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select k1, k2, k3, k4, k5, k6, k10, k11, k7, k8, k9 * 2 from %s.baseall ' \
           'order by k1' % query_db
    ret = check2(client, sql1, sql2)
    client.stop_routine_load(routine_load_name)
    assert ret
    common.execute_ignore_error(client.clean, database_name)


def test_delete():
    """
    {
    "title": "test_routine_load_with_alter.test_delete",
    "describe": "delete data",
    "tag": "system,p1,stability"
    }
    """
    """delete data"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list,
                              distribution_info=DATA.baseall_distribution_info)
    assert ret, 'create table failed'
    routine_load_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    routine_load_property.set_column_mapping(DATA.baseall_column_name_list)
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 15)
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall order by k1' % query_db
    ret = check2(client, sql1, sql2)
    assertStop(ret, client, routine_load_name, 'check data error')

    ret = client.delete(table_name, [('k1', '=', '1')])
    if not ret:
        client.pause_routine_load(routine_load_name)
        time.sleep(1)
        ret = (client.get_routine_load_state(routine_load_name) == 'PAUSED')
        assertStop(ret, client, routine_load_name, 'expect routine load paused')
        ret = client.delete(table_name, [('k1', '=', '1')])
        client.resume_routine_load(routine_load_name)
    assertStop(ret, client, routine_load_name, 'delete failed')
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select k1, k2, k3, k4, k5, k6, k10, k11, k7, k8, k9 from %s.baseall ' \
           'where k1 != 1 order by k1' % query_db
    ret = check2(client, sql1, sql2)
    assertStop(ret, client, routine_load_name, 'check data error')
    ret = (client.get_routine_load_state(routine_load_name) == 'RUNNING')
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 30)
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'RUNNING')
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select k1, k2, k3, k4, k5, k6, k10, k11, k7, k8, k9 * 2 from %s.baseall ' \
           'where k1 != 1 union (select k1, k2, k3, k4, k5, k6, k10, k11, k7, k8, k9 ' \
           'from %s.baseall where k1 = 1) order by k1' % (query_db, query_db)
    ret = check2(client, sql1, sql2)
    client.stop_routine_load(routine_load_name)
    assert ret
    common.execute_ignore_error(client.clean, database_name)


def test_drop_table():
    """
    {
    "title": "test_routine_load_with_alter.test_drop_table",
    "describe": "while routine load is running, drop table",
    "tag": "system,p1,stability"
    }
    """
    """while routine load is running, drop table"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list, 
                              distribution_info=DATA.baseall_distribution_info)
    assert ret, 'create table failed'
    routine_load_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    routine_load_property.set_column_mapping(DATA.baseall_column_name_list)
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 15)
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall order by k1' % query_db
    ret = check2(client, sql1, sql2)
    assertStop(ret, client, routine_load_name, 'check data error')
    ret = client.drop_table(table_name)
    assert ret, 'drop table failed'

    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    client.wait_routine_load_state(routine_load_name, 'CANCELLED')
    ret = client.show_routine_load(routine_load_name, is_all=True)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    print(routine_load_job.get_state())
    assert routine_load_job.get_state() == 'CANCELLED'
    common.execute_ignore_error(client.clean, database_name)


def test_drop_database():
    """
    {
    "title": "test_routine_load_with_alter.test_drop_database",
    "describe": "while routine load is running, drop database",
    "tag": "system,p1,stability"
    }
    """
    """while routine load is running, drop database"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list, 
                              distribution_info=DATA.baseall_distribution_info)
    assert ret, 'create table failed'
    routine_load_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    routine_load_property.set_column_mapping(DATA.baseall_column_name_list)
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 15)
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall order by k1' % query_db
    assert check2(client, sql1, sql2)
    ret = client.clean(database_name)
    client = create_workspace(database_name)
    assert client, 'get client failed'
    common.execute_ignore_error(client.clean, database_name)


def test_drop_partition():
    """
    {
    "title": "test_routine_load_with_alter.test_drop_partition",
    "describe": "while routine load is running, drop partition,导入指定partition",
    "tag": "system,p1,stability"
    }
    """
    """while routine load is running, drop partition,导入指定partition"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list, 
                              distribution_info=DATA.baseall_distribution_info, 
                              partition_info=DATA.baseall_tinyint_partition_info)
    assert ret, 'create table failed'
    routine_load_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    routine_load_property.set_partitions(['p1', 'p2', 'p3', 'p4'])
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 15)
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall order by k1' % query_db
    ret = check2(client, sql1, sql2)
    assertStop(ret, client, routine_load_name, 'check data error')
    ret = client.drop_partition(table_name, 'p1')
    assertStop(ret, client, routine_load_name, 'drop partition failed')
    ret = client.get_partition(table_name, 'p1')
    assertStop(not ret, client, routine_load_name, 'expect not get p1 partition')

    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    client.wait_routine_load_state(routine_load_name, 'PAUSED')
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    assert routine_load_job.get_state() == 'PAUSED', 'expect routine load paused'
    print(routine_load_job.get_reason_of_state_changed())
    client.stop_routine_load(routine_load_name)
    common.execute_ignore_error(client.clean, database_name)


if __name__ == '__main__':
    setup_module()
    test_delete()
