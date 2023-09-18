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
test_routine_load_kafka_property.py
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
TOPIC = 'single-partition-2-%s' % config.fe_query_port
WAIT_TIME = 15


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
    if not ret and stop_job != None:
        client.stop_routine_load(stop_job)
    assert ret, info


def test_kafka_topic_not_exist():
    """
    kafka topic不存在
    如果kafka集群配置支持自动创建Topic,则会创建Topic;否则,load job会paused
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list, 
                              distribution_info=DATA.baseall_distribution_info)
    assert ret, 'create table failed'
    routine_load_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC+'error')
    routine_load_property.set_kafka_partitions('0')
    routine_load_property.set_kafka_offsets('0')
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name, 'RUNNING')
    time.sleep(15)
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'RUNNING')
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    print(routine_load_job.get_reason_of_state_changed())
    client.stop_routine_load(routine_load_name)
    client.clean(database_name)


def test_kafka_offset_error():
    """kafka offset错误,超出范围,load job paused"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list, 
                              distribution_info=DATA.baseall_distribution_info)
    assert ret, 'create table failed'
    routine_load_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    routine_load_property.set_kafka_partitions('0')
    routine_load_property.set_kafka_offsets('0')
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name, 'PAUSED')
    time.sleep(15)
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'PAUSED')
    assertStop(ret, client, routine_load_name, 'expect routine load paused')
    print(routine_load_job.get_reason_of_state_changed())
    client.stop_routine_load(routine_load_name)
    client.clean(database_name)


def test_kafka_partition_error():
    """kafka partition错误, load job创建失败"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list, 
                              distribution_info=DATA.baseall_distribution_info)
    assert ret, 'create table failed'
    routine_load_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list(kafka_config.kafka_broker_list)
    routine_load_property.set_kafka_topic(TOPIC)
    routine_load_property.set_kafka_partitions('100')
    routine_load_property.set_kafka_offsets('OFFSET_BEGINNING')
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert not ret, 'expect create routine load failed'
    client.clean(database_name)


def test_kafka_broker_list_error():
    """kafka broker list错误, load job创建失败"""
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = create_workspace(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list, 
                              distribution_info=DATA.baseall_distribution_info)
    assert ret, 'create table failed'
    routine_load_name = util.get_label()
    routine_load_property = palo_client.RoutineLoadProperty()
    routine_load_property.set_kafka_broker_list('127.0.0.1:9092')
    routine_load_property.set_kafka_topic(TOPIC)
    routine_load_property.set_kafka_partitions('100')
    routine_load_property.set_kafka_offsets('OFFSET_BEGINNING')
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert not ret, 'expect create routine load failed'
    client.clean(database_name)


def test_alter_broker_list():
    """
    {
    "title": "test_routine_load_kafka_property.test_alter_broker_list",
    "describe": "alter broker list for kafka routine load, github issue #6335",
    "tag": "function,p1"
    }
    """
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
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
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
    # alter routine load
    sql = "alter routine load for %s from kafka('kafka_broker_list' = '10.114.101.12:9094')" % routine_load_name
    client.execute(sql)
    sql = "show create routine load for %s" % routine_load_name
    ret = client.execute(sql)
    create_msg = ret[0][2]
    assert "10.114.101.12:9094" in create_msg
    assert "kafka_partitions" in create_msg
    assert "kafka_offsets" in create_msg
    # resume routine load job
    ret = client.resume_routine_load(routine_load_name)
    assertStop(ret, client, routine_load_name, 'expect resume routine load succ')
    client.wait_routine_load_state(routine_load_name)
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'RUNNING')
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    client.stop_routine_load(routine_load_name)
    client.clean(database_name)


def test_alter_kafka_topic():
    """
    {
    "title": "test_routine_load_kafka_property.test_alter_kafka_topic",
    "describe": "alter topic for kafka routine load, github issue #6335",
    "tag": "function,p1"
    }
    """
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
    ret = client.routine_load(table_name, routine_load_name, routine_load_property,
                              database_name=database_name)
    assert ret, 'create routine load failed'
    client.wait_routine_load_state(routine_load_name)
    kafka_config.send_to_kafka(TOPIC, '../qe/baseall.txt')
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
    # alter routine load
    sql = "alter routine load for %s from kafka('kafka_topic' = 'new_topic')" % routine_load_name
    client.execute(sql)
    sql = "show create routine load for %s" % routine_load_name
    ret = client.execute(sql)
    create_msg = ret[0][2]
    assert "new_topic" in create_msg
    assert "kafka_partitions" in create_msg
    assert "kafka_offsets" in create_msg
    # resume routine load job
    ret = client.resume_routine_load(routine_load_name)
    assertStop(ret, client, routine_load_name, 'expect resume routine load succ')
    client.wait_routine_load_state(routine_load_name)
    ret = client.show_routine_load(routine_load_name)
    routine_load_job = palo_job.RoutineLoadJob(ret[0])
    ret = (routine_load_job.get_state() == 'RUNNING')
    assertStop(ret, client, routine_load_name, 'expect routine load running')
    client.stop_routine_load(routine_load_name)
    client.clean(database_name)


def teardown_module():
    """teardown"""
    pass


