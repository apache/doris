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
file:test_routine_load_job_property.py
测试routine load的导入控制
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

config = palo_config.config
TOPIC_50 = 'multi-partitions-50-%s' % config.fe_query_port
TOPIC_10 = 'first_test-%s' % config.fe_query_port
TOPIC_3 = 'three-partition-%s' % config.fe_query_port
TOPIC_1 = 'single-partition-%s' % config.fe_query_port
TOPIC_TIME_ZONE = 'time-zone-%s' % config.fe_query_port

WAIT_TIME = 10
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
        ret = client.show_routine_load(routine_load_job_name)
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
        timeout -= 3
        time.sleep(3)
    return False


def test_without_concurrent_num():
    """
    {
    "title": "test_routine_load_job_property.test_without_concurrent_num",
    "describe": "导入不指定并发数,使用默认的并发数,通过show查看并发度, 50个分区,10个be",
    "tag": "system,p1"
    }
    """
    """
    导入不指定并发数,使用默认的并发数,通过show查看并发度
    50个分区,10个be
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    # create table
    ret = client.create_table(table_name, DATA.baseall_column_list, 
                              distribution_info=DATA.baseall_distribution_info)
    assert ret, 'create table failed'
    # create routine load
    routine_load_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC_50)
    partition_offset = kafka_config.get_topic_offset(TOPIC_50)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    # wait routine load from NEED_SCHEDULE to RUNNING, send msg to kafka topic
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC_50, '../qe/baseall.txt')
    # wait routine load job task commit, defualt maxBatchIntervalS is 10s, check resule
    print(routine_load_name)
    wait_commit(client, routine_load_name, 15)
    sql = 'select * from %s' % table_name
    ret = client.execute(sql)
    ret_assert = (len(ret) == 15)
    assertStop(ret_assert, client, routine_load_name, 'select result error')
    # check routine load state and task number
    ret_assert = (client.get_routine_load_state(routine_load_name) == 'RUNNING')
    assertStop(ret_assert, client, routine_load_name, 'expect routine load running')
    ret = client.show_routine_load(routine_load_name)
    ret_assert = (len(ret) == 1)
    assertStop(ret_assert, client, routine_load_name, 'expect 1 routine load job')
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    task_num = routine_load_job.get_current_task_num()
    print(task_num)
    # 5 is default max task num
    ret_assert = (int(task_num) == 5)
    assertStop(ret_assert, client, routine_load_name, task_num)
    # stop routine load and clean
    client.stop_routine_load(routine_load_name)
    client.clean(database_name)


def test_with_concurrent_num():
    """
    {
    "title": "test_routine_load_job_property.test_with_concurrent_num",
    "describe": "导入指定并发数2,通过show 查看并发度,50个分区,10个be",
    "tag": "system,p1"
    }
    """
    """
    导入指定并发数2,通过show 查看并发度
    50个分区,10个be
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list, 
                              distribution_info=DATA.baseall_distribution_info)
    assert ret, 'create table failed'
    routine_load_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC_50)
    routine_load_property.set_desired_concurrent_number(2)
    partition_offset = kafka_config.get_topic_offset(TOPIC_50)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC_50, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 15)
    sql = 'select * from %s' % table_name
    ret = client.execute(sql)
    ret_assert = (len(ret) == 15)
    assertStop(ret_assert, client, routine_load_name, 'check select result error')
    ret_assert = (client.get_routine_load_state(routine_load_name) == 'RUNNING')
    assertStop(ret_assert, client, routine_load_name, 'expect routine load running')
    ret = client.show_routine_load(routine_load_name)
    ret_assert = (len(ret) == 1)
    assertStop(ret_assert, client, routine_load_name, 'expect 1 routine load job')
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    task_num = routine_load_job.get_current_task_num()
    ret_assert = (int(task_num) == 2)
    assertStop(ret_assert, client, routine_load_name, 'expect current task num 2')
    client.stop_routine_load(routine_load_name)
    client.clean(database_name)


def test_with_concurrent_num_partition():
    """
    {
    "title": "test_routine_load_job_property.test_with_concurrent_num_partition",
    "describe": "导入不指定并发数,通过show 查看并发度, 3个分区,10个be",
    "tag": "system,p1"
    }
    """
    """
    导入不指定并发数,通过show 查看并发度
    3个分区,10个be
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list, 
                              distribution_info=DATA.baseall_distribution_info)
    assert ret, 'create table failed'
    routine_load_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC_3)
    partition_offset = kafka_config.get_topic_offset(TOPIC_3)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC_3, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 15)
    sql = 'select * from %s' % table_name
    ret = client.execute(sql)
    ret_assert = (len(ret) == 15)
    assertStop(ret_assert, client, routine_load_name, 'check select result error')
    ret_assert = (client.get_routine_load_state(routine_load_name) == 'RUNNING')
    assertStop(ret_assert, client, routine_load_name, 'expect routine load running')
    ret = client.show_routine_load(routine_load_name)
    ret_assert = (len(ret) == 1)
    assertStop(ret_assert, client, routine_load_name, 'expect 1 routine load job')
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    task_num = routine_load_job.get_current_task_num()
    print(task_num)
    ret_assert = (int(task_num) == 3)
    assertStop(ret_assert, client, routine_load_name, 'expect current task num 3')
    client.stop_routine_load(routine_load_name)
    client.clean(database_name)


def test_max_error_rows():
    """
    {
    "title": "test_routine_load_job_property.test_max_error_rows",
    "describe": "max_error_number=0，第1条数据错误，第一个task停止，job暂停，当前导入数据量为20w-1条数据",
    "tag": "system,p1,fuzz"
    }
    """
    """
    max_error_number=0，第1条数据错误，第一个task停止，job暂停，当前导入数据量为20w-1条数据
    max_batch_rows=20w， 只一个partition，max_batch_interval和max_batch_size足够大，
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.sandbox_column_list, 
                              distribution_info=DATA.sandbox_distribution_info)
    assert ret, 'create table failed'
    routine_load_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC_1)
    partition_offset = kafka_config.get_topic_offset(TOPIC_1)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    routine_load_property.set_max_batch_rows(200000)
    routine_load_property.set_max_error_number(0)
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC_1, '../f_0_00')
    wait_commit(client, routine_load_name, 199999)
    sql = 'select count(*) from %s' % table_name
    result = client.execute(sql)
    ret = client.show_routine_load(routine_load_name)
    ret_assert = (len(ret) == 1)
    assertStop(ret_assert, client, routine_load_name, 'expect 1 routine load job')
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    print(routine_load_job.get_state())
    print(routine_load_job.get_error_rows())
    print(routine_load_job.get_loaded_rows())
    print(result[0][0])
    ret_assert = (routine_load_job.get_state() == "PAUSED")
    assertStop(ret_assert, client, routine_load_name, 'expect routine load paused')
    ret_assert = (routine_load_job.get_error_rows() == 1)
    assertStop(ret_assert, client, routine_load_name, 'expect error rows 1')
    ret_assert = (routine_load_job.get_loaded_rows() == result[0][0])
    assertStop(ret_assert, client, routine_load_name, 'expect loaded rows right')
    client.stop_routine_load(routine_load_name)
    client.clean(database_name)


def test_max_error_rows_1():
    """
    {
    "title": "test_routine_load_job_property.test_max_error_rows_1",
    "describe": "max_error_number=1，第1条数据错误，在一个partition中固定每隔20万条数据有一个error数据，第2个task停止，offset为40万，job暂停",
    "tag": "system,p1,fuzz"
    }
    """
    """
    max_error_number=1，第1条数据错误，在一个partition中固定每隔20万条数据有一个error数据，第2个task停止，offset为40万，job暂停
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.sandbox_column_list, 
                              distribution_info=DATA.sandbox_distribution_info)
    assert ret, 'create table failed'
    routine_load_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC_1)
    partition_offset = kafka_config.get_topic_offset(TOPIC_1)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    routine_load_property.set_max_batch_rows(200000)
    routine_load_property.set_max_error_number(1)
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC_1, '../f_0_00')
    wait_commit(client, routine_load_name, 199999 * 2)
    sql = 'select count(*) from %s' % table_name
    result = client.execute(sql)
    ret = client.show_routine_load(routine_load_name)
    ret_assert = (len(ret) == 1)
    assertStop(ret_assert, client, routine_load_name, 'expect 1 routine load job')
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    print(routine_load_job.get_state())
    print(routine_load_job.get_error_rows())
    print(routine_load_job.get_loaded_rows())
    print(result[0][0])
    ret_assert = (routine_load_job.get_state() == "PAUSED")
    assertStop(ret_assert, client, routine_load_name, 'expect routine load paused')
    ret_assert = (routine_load_job.get_error_rows() == 2)
    assertStop(ret_assert, client, routine_load_name, 'expect error rows 2')
    ret_assert = (routine_load_job.get_loaded_rows() == result[0][0])
    assertStop(ret_assert, client, routine_load_name, 'expect loaded rows right')
    client.stop_routine_load(routine_load_name)
    client.clean(database_name)


def test_max_error_rows_2():
    """
    {
    "title": "test_routine_load_job_property.test_max_error_rows_2",
    "describe": "max_error_number=10，第1条数据错误，在一个partition中固定每隔20万条数据有一个error数据，job正常运行，不暂停。",
    "tag": "system,p1,fuzz"
    }
    """
    """
    max_error_number=10，第1条数据错误，在一个partition中固定每隔20万条数据有一个error数据，job正常运行，不暂停。
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.sandbox_column_list, 
                              distribution_info=DATA.sandbox_distribution_info)
    assert ret, 'create table failed'
    routine_load_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC_1)
    partition_offset = kafka_config.get_topic_offset(TOPIC_1)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    routine_load_property.set_max_batch_rows(200000)
    routine_load_property.set_max_error_number(10)
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC_1, '../f_0_00')
    kafka_config.send_to_kafka(TOPIC_1, '../f_0_02')
    wait_commit(client, routine_load_name, 1999990, 60)
    sql = 'select count(*) from %s' % table_name
    result = client.execute(sql)
    ret = client.show_routine_load(routine_load_name)
    ret_assert = (len(ret) == 1)
    assertStop(ret_assert, client, routine_load_name, 'expect 1 routine load job')
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    print(routine_load_job.get_state())
    print(routine_load_job.get_error_rows())
    print(routine_load_job.get_loaded_rows())
    print(result[0][0])
    ret_assert = (routine_load_job.get_state() == "RUNNING")
    assertStop(ret_assert, client, routine_load_name, 'expect routine load running')
    ret_assert = (routine_load_job.get_error_rows() == 10)
    assertStop(ret_assert, client, routine_load_name, 'expect error rows 10')
    ret_assert = (routine_load_job.get_loaded_rows() == result[0][0])
    assertStop(ret_assert, client, routine_load_name, 'expect loaded rows right')
    client.stop_routine_load(routine_load_name)
    client.clean(database_name)


def test_max_error_rows_3():
    """
    {
    "title": "test_routine_load_job_property.test_max_error_rows_3",
    "describe": "max_error_number=10，第100条数据错误，在一个partition中固定每隔20万条数据有一个error数据，第10个task中有多个错误行数，job 暂停，验证offset",
    "tag": "system,p1,fuzz"
    }
    """
    """
    max_error_number=10，第100条数据错误，在一个partition中固定每隔20万条数据有一个error数据，第10个task中有多个错误行数，job 暂停，验证offset
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.sandbox_column_list, 
                              distribution_info=DATA.sandbox_distribution_info)
    assert ret, 'create table failed'
    routine_load_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC_1)
    partition_offset = kafka_config.get_topic_offset(TOPIC_1)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    routine_load_property.set_max_batch_rows(200000)
    routine_load_property.set_max_error_number(10)
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC_1, '../f_0_00')
    time.sleep(WAIT_TIME)
    kafka_config.send_to_kafka(TOPIC_1, '../f_0_01')
    wait_commit(client, routine_load_name, 1999989, 60)
    sql = 'select count(*) from %s' % table_name
    result = client.execute(sql)
    ret = client.show_routine_load(routine_load_name)
    ret_assert = (len(ret) == 1)
    assertStop(ret_assert, client, routine_load_name, 'expect 1 routine load job')
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    print(routine_load_job.get_state())
    print(routine_load_job.get_error_rows())
    print(routine_load_job.get_loaded_rows())
    print(result[0][0])
    ret_assert = (routine_load_job.get_state() == "PAUSED")
    assertStop(ret_assert, client, routine_load_name, 'expect routine load paused')
    ret_assert = (routine_load_job.get_error_rows() == 11)
    assertStop(ret_assert, client, routine_load_name, 'expect error rows 11')
    ret_assert = (routine_load_job.get_loaded_rows() == result[0][0])
    assertStop(ret_assert, client, routine_load_name, 'expect loaded rows right')
    client.stop_routine_load(routine_load_name)
    client.clean(database_name)


def time_zone_routine_load(database_name, table_name, routine_cloumn_map,\
    sql, client, table_schema=None, zone="+00:00"):
    """"
    routine load的影响,包含建表，导入数据，查询sql，再次导入数据，查询sql
    broker_cloumn_map： routine_cloumn mapping
    sql：routine load完毕要执行的sql语句
    """
    if not table_schema:
        table_schema = DATA.baseall_column_list
    # 建表
    distribution_type_d = 'HASH(k1, k2, k5)'
    distribution_info_d = palo_client.DistributionInfo(distribution_type_d, 3)
    client.create_table(table_name, table_schema, distribution_info=distribution_info_d)
    assert client.show_tables(table_name, database_name)
    ###导入数据
    # create routine load
    routine_load_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    column_mapping = ['k11=now()']
    routine_load_property.set_column_mapping(column_mapping)
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC_TIME_ZONE)
    partition_offset = kafka_config.get_topic_offset(TOPIC_TIME_ZONE)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))

    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                                   database_name=database_name)
    assert ret, 'create routine load failed'
    # wait routine load from NEED_SCHEDULE to RUNNING, send msg to kafka topic
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC_TIME_ZONE, '../qe/baseall.txt')
    wait_commit(client, routine_load_name, 15)
    sql_count = 'select count(*) from %s' % table_name
    ret = client.execute(sql_count)
    assert ret[0] == (15,), "excepetd %s == 15" % ret[0]
    assert (client.get_routine_load_state(routine_load_name) == 'RUNNING')
    ###第一次获取数据
    res_before = client.execute(sql)
    client.stop_routine_load(routine_load_name)
    ##设置时区,第二次获取数据
    zone = '+00:00'
    routine_load_property.set_timezone(zone)
    routine_load_name = util.get_label()
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                                   database_name=database_name)
    assert ret, 'create routine load failed'
    # wait routine load from NEED_SCHEDULE to RUNNING, send msg to kafka topic
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC_TIME_ZONE, '../qe/baseall.txt')
    # wait routine load job task commit, defualt maxBatchIntervalS is 10s, check resule
    wait_commit(client, routine_load_name, 30)
    assert (client.get_routine_load_state(routine_load_name) == 'RUNNING')
    client.stop_routine_load(routine_load_name)
    res_after = client.execute(sql)
    return res_before, res_after


def test_set_time_zone_routine_load_now():
    """
    {
    "title": "test_routine_load_job_property.test_set_time_zone_routine_load_now",
    "describe": "设置time_zone变量,验证now() routine load任务的影响。",
    "tag": "system,p1"
    }
    """
    """
    设置time_zone变量,验证now() routine load任务的影响。
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    set_column_list = ["k11=now()"]
    sql = "select k11 from %s order by k11 limit 1" % table_name
    zone = "+00:00"
    gap = 28800 + WAIT_TIME*2
    res_before, res_after = time_zone_routine_load(database_name, table_name, \
       set_column_list, sql, client=client)
    print(res_before, res_after)
    ##两者差值
    util.check2_time_zone(res_before, res_after, gap)
    client.clean(database_name)


def test_set_time_zone_routine_load_FROM_UNIXTIME():
    """
    {
    "title": "test_routine_load_job_property.test_set_time_zone_routine_load_FROM_UNIXTIME",
    "describe": "设置time_zone变量,验证FROM_UNIXTIME() 函数对routine_load任务的影响。",
    "tag": "system,p1"
    }
    """
    """
    设置time_zone变量,验证FROM_UNIXTIME() 函数对routine_load任务的影响。
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    set_column_list = ["k11=FROM_UNIXTIME(2019, '%Y-%m-%d %H:%i:%s')"]
    sql = "select k11 from %s order by k11 limit 1" % table_name
    zone = "+00:00"
    gap = 28800 + WAIT_TIME*2
    res_before, res_after = time_zone_routine_load(database_name, table_name, \
       set_column_list, sql, client=client)
    print(res_before, res_after)
    ##两者差值
    util.check2_time_zone(res_before, res_after, gap)
    client.clean(database_name)


def test_set_time_zone_routine_load_CONVERT_TZ():
    """
    {
    "title": "test_routine_load_job_property.test_set_time_zone_routine_load_CONVERT_TZ",
    "describe": "设置time_zone变量,验证CONVERT_TZ() 函数对routine load任务的影响。",
    "tag": "system,p1"
    }
    """
    """
    设置time_zone变量,验证CONVERT_TZ() 函数对routine load任务的影响。
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    set_column_list = ["k11=CONVERT_TZ('2019-01-01 12:00:00','+01:00','+10:00')"]
    sql = "select k11 from %s order by k11 limit 1" % table_name
    zone = "+00:00"
    gap = 28800 + WAIT_TIME*2
    res_before, res_after = time_zone_routine_load(database_name, table_name,\
       set_column_list, sql, client=client)
    print(res_before, res_after)
    ##两者差值
    util.check2_time_zone(res_before, res_after, gap)
    client.clean(database_name)


def test_set_time_zone_routine_load_UNIX_TIMESTAMP():
    """
    {
    "title": "test_routine_load_job_property.test_set_time_zone_routine_load_UNIX_TIMESTAMP",
    "describe": "设置time_zone变量,验证UNIX_TIMESTAMP() 函数对routine load任务的影响, 数值不会有影响。",
    "tag": "system,p1"
    }
    """
    """
    设置time_zone变量,验证UNIX_TIMESTAMP() 函数对routine load任务的影响, 数值不会有影响。
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    set_column_list = ["k3=UNIX_TIMESTAMP('2019-01-01 08:00:00')"]
    sql = "select k3 from %s order by k3 limit 1" % table_name
    res_before, res_after = time_zone_routine_load(database_name, table_name,\
       set_column_list, sql, client=client)
    print(res_before, res_after)
    ##两者差值
    util.check(res_before, res_after)
    client.clean(database_name)


if __name__ == '__main__':
    setup_module()
    test_set_time_zone_routine_load_now()
    test_set_time_zone_routine_load_FROM_UNIXTIME()
    test_set_time_zone_routine_load_CONVERT_TZ()
    test_set_time_zone_routine_load_UNIX_TIMESTAMP()



