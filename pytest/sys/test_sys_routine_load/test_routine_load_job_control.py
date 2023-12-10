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
file:test_routine_load_job_control.py
测试routine load job的状态转换
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
# single partition
TOPIC = 'single-partition-1-%s' % config.fe_query_port
WAIT_TIME = 45
LOG = palo_client.LOG
L = palo_client.L


def setup_module():
    """
    init config
    """
    pass


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
            show_ret = client.show_routine_load(stop_job, is_all=True)
            LOG.info(L('routine load job info', ret=show_ret))
            client.stop_routine_load(stop_job)
        except Exception as e:
            print(str(e))
    assert ret, info


def test_pause_running_job():
    """
    {
    "title": "test_routine_load_job_control.test_pause_running_job",
    "describe": "暂停一个running job",
    "tag": "function,p1"
    }
    """
    """暂停一个running job"""
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
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    # wait routine load from NEED_SCHEDULE to RUNNING
    client.wait_routine_load_state(routine_load_name)
    # when routine load job running, send data to kafka topic
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    # check routine load state, should be running
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'RUNNING')
    assertStop(ret, client, routine_load_name, 'expect routine load is running')
    # pause routine load job
    ret = client.pause_routine_load(routine_load_name)
    assertStop(ret, client, routine_load_name, 'pause routine load failed')
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'PAUSED')
    assertStop(ret, client, routine_load_name, 'expect routine load is paused')
    # stop routine load and clean db
    client.stop_routine_load(routine_load_name)
    client.clean(database_name)


def test_pause_paused_job():
    """
    {
    "title": "test_routine_load_job_control.test_pause_paused_job",
    "describe": "暂停一个paused job",
    "tag": "function,p1,fuzz"
    }
    """
    """暂停一个paused job"""
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
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'RUNNING')
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    ret = client.pause_routine_load(routine_load_name)
    assert ret
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'PAUSED')
    assertStop(ret, client, routine_load_name, 'expect routine load paused')
    ret = client.pause_routine_load(routine_load_name)
    assertStop(not ret, client, routine_load_name, 'expect pause routine load failed')
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'PAUSED')
    assertStop(ret, client, routine_load_name, 'expect routine load paused')
    client.stop_routine_load(routine_load_name)
    client.clean(database_name)


def test_pause_stopped_job():
    """
    {
    "title": "test_routine_load_job_control.test_pause_stopped_job",
    "describe": "暂停一个stopped job",
    "tag": "function,p1,fuzz"
    }
    """
    """暂停一个stopped job"""
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
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'RUNNING')
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    ret = client.stop_routine_load(routine_load_name) 
    assert ret, 'expect stop routine load succ'
    ret = client.show_routine_load(routine_load_name, is_all=True)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    assert routine_load_job.get_state() == 'STOPPED', 'expect routine load stopped'
    ret = client.pause_routine_load(routine_load_name)
    assert not ret, 'expect pause routine load failed'
    ret = client.show_routine_load(routine_load_name, is_all=True)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    assert routine_load_job.get_state() == 'STOPPED', 'expect routine load stopped'
    client.clean(database_name)


def test_pause_cancelled_job():
    """
    {
    "title": "test_routine_load_job_control.test_pause_cancelled_job",
    "describe": "暂停一个cancelled job",
    "tag": "function,p1,fuzz"
    }
    """
    """暂停一个cancelled job"""
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
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'RUNNING')
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    ret = client.drop_table(table_name)
    assertStop(ret, client, routine_load_name, 'drop table failed')
    time.sleep(WAIT_TIME)
    ret = client.show_routine_load(routine_load_name, is_all=True)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    assert routine_load_job.get_state() == 'CANCELLED', 'expect routine load cancelled'
    ret = client.pause_routine_load(routine_load_name)
    assert not ret, 'expect pause routine load failed'
    ret = client.show_routine_load(routine_load_name, is_all=True)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    assert routine_load_job.get_state() == 'CANCELLED', 'expect routine load cancelled'
    client.clean(database_name)


def test_resume_running_job():
    """
    {
    "title": "test_routine_load_job_control.test_resume_running_job",
    "describe": "恢复一个running job",
    "tag": "function,p1,fuzz"
    }
    """
    """恢复一个running job"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list, distribution_info=DATA.baseall_distribution_info)
    assert ret, 'create table failed'
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
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'RUNNING')
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    ret = client.resume_routine_load(routine_load_name)
    assertStop(not ret, client, routine_load_name, 'expect resume failed')
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'RUNNING')
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    client.stop_routine_load(routine_load_name)
    client.clean(database_name)


def test_resume_paused_job():
    """
    {
    "title": "test_routine_load_job_control.test_resume_paused_job",
    "describe": "恢复一个paused job",
    "tag": "function,p1"
    }
    """
    """恢复一个paused job"""
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
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'RUNNING')
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    ret = client.pause_routine_load(routine_load_name)
    assertStop(ret, client, routine_load_name, 'expect pause routine load succ')
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'PAUSED')
    assertStop(ret, client, routine_load_name, 'expect routine load paused')
    ret = client.resume_routine_load(routine_load_name)
    assertStop(ret, client, routine_load_name, 'expect resume routine load succ')
    client.wait_routine_load_state(routine_load_name)
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'RUNNING')
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    client.stop_routine_load(routine_load_name)
    client.clean(database_name)


def test_resume_stopped_job():
    """
    {
    "title": "test_routine_load_job_control.test_resume_stopped_job",
    "describe": "恢复一个stopped job",
    "tag": "function,p1,fuzz"
    }
    """
    """恢复一个stopped job"""
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
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'RUNNING')
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    ret = client.stop_routine_load(routine_load_name)
    assertStop(ret, client, routine_load_name, 'expect stop routine load succ')
    ret = client.show_routine_load(routine_load_name, is_all=True)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    assert routine_load_job.get_state() == 'STOPPED', 'expect routine load stopped'
    ret = client.resume_routine_load(routine_load_name)
    assert not ret, 'expect resume routine load failed'
    ret = client.show_routine_load(routine_load_name, is_all=True)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    assert routine_load_job.get_state() == 'STOPPED', 'expect routine load stopped'
    client.stop_routine_load(routine_load_name)
    client.clean(database_name)


def test_resume_cancelled_job():
    """
    {
    "title": "test_routine_load_job_control.test_resume_cancelled_job",
    "describe": "恢复一个cancelled job",
    "tag": "function,p1,fuzz"
    }
    """
    """恢复一个cancelled job"""
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
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'RUNNING')
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    ret = client.drop_table(table_name)
    assert ret, 'expect drop table succ'
    time.sleep(WAIT_TIME)
    ret = client.show_routine_load(routine_load_name, is_all=True)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    assert routine_load_job.get_state() == 'CANCELLED', 'expect routine load cancelled'
    ret = client.resume_routine_load(routine_load_name)
    assert not ret, 'expect resume routine load failed'
    ret = client.show_routine_load(routine_load_name, is_all=True)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    assert routine_load_job.get_state() == 'CANCELLED', 'expect routine load cancelled'
    client.stop_routine_load(routine_load_name)
    client.clean(database_name)


def test_stop_running_job():
    """
    {
    "title": "test_routine_load_job_control.test_stop_running_job",
    "describe": "停止一个running job",
    "tag": "function,p1"
    }
    """
    """停止一个running job"""
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
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'RUNNING')
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    ret = client.stop_routine_load(routine_load_name)
    assert ret, 'expect stop routine load succ'
    ret = client.show_routine_load(routine_load_name, is_all=True)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    assert routine_load_job.get_state() == 'STOPPED', 'expect routine load stopped'
    client.clean(database_name)


def test_stop_paused_job():
    """
    {
    "title": "test_routine_load_job_control.test_stop_paused_job",
    "describe": "停止一个paused job",
    "tag": "function,p1"
    }
    """
    """停止一个paused job"""
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
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'RUNNING')
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    ret = client.pause_routine_load(routine_load_name)
    assertStop(ret, client, routine_load_name, 'expect pause routine load succ')
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'PAUSED')
    assertStop(ret, client, routine_load_name, 'expect routine load paused')
    ret = client.stop_routine_load(routine_load_name)
    assert ret, 'expect stop routine load succ'
    ret = client.show_routine_load(routine_load_name, is_all=True)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    assert routine_load_job.get_state() == 'STOPPED', 'expect routine load stopped'
    client.clean(database_name)


def test_stop_stopped_job():
    """
    {
    "title": "test_routine_load_job_control.test_stop_stopped_job",
    "describe": "停止一个stopped job",
    "tag": "function,p1,fuzz"
    }
    """
    """停止一个stopped job"""
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
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'RUNNING')
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    ret = client.stop_routine_load(routine_load_name)
    assert ret, 'expect stop routine load succ'
    ret = client.show_routine_load(routine_load_name, is_all=True)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    assert routine_load_job.get_state() == 'STOPPED', 'expect routine load stopped'
    ret = client.stop_routine_load(routine_load_name)
    assert not ret, 'expect stop routine load failed'
    ret = client.show_routine_load(routine_load_name, is_all=True)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    assert routine_load_job.get_state() == 'STOPPED', 'expect routine load stopped'
    client.clean(database_name)


def test_stop_cancelled_job():
    """
    {
    "title": "test_routine_load_job_control.test_stop_cancelled_job",
    "describe": "停止一个cancelled job",
    "tag": "function,p1,fuzz"
    }
    """
    """停止一个cancelled job"""
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
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'RUNNING')
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    ret = client.drop_table(table_name)
    assert ret, 'expect drop table succ'
    time.sleep(WAIT_TIME)
    ret = client.show_routine_load(routine_load_name, is_all=True)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    assert routine_load_job.get_state() == 'CANCELLED', 'expect routine load cancelled'
    ret = client.stop_routine_load(routine_load_name)
    assert not ret, 'expect stop routine load failed'
    ret = client.show_routine_load(routine_load_name, is_all=True)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    assert routine_load_job.get_state() == 'CANCELLED', 'expect routine load cancelled'
    client.clean(database_name)


def test_privilege():
    """
    {
    "title": "test_routine_load_job_control.test_privilege",
    "describe": "routine load的权限",
    "tag": "function,p1"
    }
    """
    """routine load的权限"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    root_client = create_workspace(database_name)
    ret = root_client.create_table(table_name, DATA.baseall_column_list, 
                                   distribution_info=DATA.baseall_distribution_info)
    assert ret, 'create table failed'
    user = 'load_user'
    root_client.drop_user(user)
    ret = root_client.create_user(user, user)
    assert ret, 'create user failed'
    ret = root_client.grant(user, 'LOAD_PRIV', database_name)
    assert ret, 'grant failed'
    user_client = palo_client.PaloClient(config.fe_host, config.fe_query_port, user=user,
                                    password=user)
    user_client.init()
    user_client.use(database_name)
    routine_load_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    ret = user_client.routine_load(table_name, routine_load_name, routine_load_property,
                                   database_name=database_name)
    assert ret, 'create routine load failed'
    user_client.wait_routine_load_state(routine_load_name)
    ret = user_client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'RUNNING')
    assertStop(ret, user_client, routine_load_name, 'expect routine load running')
    ret = user_client.pause_routine_load(routine_load_name)
    assertStop(ret, user_client, routine_load_name, 'expect routine load running')
    ret = user_client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'PAUSED')
    assertStop(ret, user_client, routine_load_name, 'expect routine load paused')
    ret = user_client.resume_routine_load(routine_load_name)
    assertStop(ret, user_client, routine_load_name, 'expect resume routine load succ')
    user_client.wait_routine_load_state(routine_load_name)
    ret = user_client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'RUNNING')
    assertStop(ret, user_client, routine_load_name, 'expect routine load running')
    ret = user_client.stop_routine_load(routine_load_name)
    assert ret, 'expect stop routine load succ'
    ret = user_client.show_routine_load(routine_load_name, is_all=True)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    assert routine_load_job.get_state() == 'STOPPED', 'expect routine load stopped'
    root_client.drop_user(user)
    root_client.clean(database_name)


def test_pause_all_routine_job():
    """
    {
    "title": "test_routine_load_job_control.test_pause_all_routine_job",
    "describe": "暂停所有routine load job, github issue #6394",
    "tag": "function,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    table_name_2 = table_name + '_2'
    table_name_3 = table_name + '_3'
    table_list = [table_name, table_name_2, table_name_3]
    # create table
    for table in table_list:
        assert client.create_table(table, DATA.baseall_column_list, \
                distribution_info=DATA.baseall_distribution_info), "create table failed"
    # create routine load
    routine_load_name = util.get_label()
    routine_load_name_2 = routine_load_name + '_2'
    routine_load_name_3 = routine_load_name + '_3'
    routine_list = [routine_load_name, routine_load_name_2, routine_load_name_3]
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    for i in range(3):
        assert client.routine_load(table_list[i], routine_list[i], routine_load_property,
                              database_name=database_name), "create routine load failed"
        # wait routine load from NEED_SCHEDULE to RUNNING
        client.wait_routine_load_state(routine_list[i])
        # when routine load job running, send data to kafka topic
        kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
        # check routine load state, should be running
        ret = client.show_routine_load(routine_list[i])
        routine_load_job = palo_job.RoutineLoadJob(ret[0])
        ret = (routine_load_job.get_state() == 'RUNNING')
        assertStop(ret, client, routine_list[i], 'expect routine load is running')
    # pause all routine load job
    sql = "PAUSE ALL ROUTINE LOAD"
    client.execute(sql)
    for routine in routine_list:
        assertStop(ret, client, routine, 'pause routine load failed')
        ret = client.show_routine_load(routine)
        routine_load_job = palo_job.RoutineLoadJob(ret[0])
        ret = (routine_load_job.get_state() == 'PAUSED')
        assertStop(ret, client, routine, 'expect routine load is paused')
    # stop routine load and clean db
    for routine in routine_list:
        client.stop_routine_load(routine)
    client.clean(database_name)


def test_resume_all_routine_job():
    """
    {
    "title": "test_routine_load_job_control.test_resume_all_routine_job",
    "describe": "resume所有routine load job, github issue #6394",
    "tag": "function,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    table_name_2 = table_name + '_2'
    table_name_3 = table_name + '_3'
    table_list = [table_name, table_name_2, table_name_3]
    # create table
    for table in table_list:
        assert client.create_table(table, DATA.baseall_column_list, \
                distribution_info=DATA.baseall_distribution_info), "create table failed"
    # create routine load
    routine_load_name = util.get_label()
    routine_load_name_2 = routine_load_name + '_2'
    routine_load_name_3 = routine_load_name + '_3'
    routine_list = [routine_load_name, routine_load_name_2, routine_load_name_3]
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    partition_offset = kafka_config.get_topic_offset(TOPIC)
    routine_load_property.set_kafka_partitions(','.join(partition_offset.keys()))
    routine_load_property.set_kafka_offsets(','.join(partition_offset.values()))
    for i in range(3):
        assert client.routine_load(table_list[i], routine_list[i], routine_load_property,
                              database_name=database_name), "create routine load failed"
        # wait routine load from NEED_SCHEDULE to RUNNING
        client.wait_routine_load_state(routine_list[i])
        # when routine load job running, send data to kafka topic
        kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
        # check routine load state, should be running
        ret = client.show_routine_load(routine_list[i])
        routine_load_job = palo_job.RoutineLoadJob(ret[0])
        ret = (routine_load_job.get_state() == 'RUNNING')
        assertStop(ret, client, routine_list[i], 'expect routine load is running')
    # pause all routine load job
    for routine in routine_list: 
        ret = client.pause_routine_load(routine)
        assertStop(ret, client, routine, 'pause routine load failed')
        ret = client.show_routine_load(routine)
        routine_load_job = palo_job.RoutineLoadJob(ret[0])
        ret = (routine_load_job.get_state() == 'PAUSED')
        assertStop(ret, client, routine, 'expect routine load is paused')
    # resume all routine load job
    sql = "RESUME ALL ROUTINE LOAD"
    client.execute(sql)
    for routine in routine_list:
        client.wait_routine_load_state(routine)
        ret = client.show_routine_load(routine)
        routine_load_job = palo_job.RoutineLoadJob(ret[0])
        ret = (routine_load_job.get_state() == 'RUNNING')
        assertStop(ret, client, routine, 'expect routine load running')
    # stop routine load and clean db
    for routine in routine_list:
        client.stop_routine_load(routine)
    client.clean(database_name)


def teardown_module():
    """tear down"""
    print('End')

