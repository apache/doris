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
#   @file test_create_dynamic_partition_parameter.py
#   @date 2021/06/02 10:28:20
#   @brief This file is a test file for doris dynamic partition parameters.
#
#############################################################################

"""
测试动态分区功能参数
"""

import time
import datetime
import sys
from dateutil.relativedelta import relativedelta
sys.path.append("../")
from lib import palo_config
from lib import palo_client
from lib import palo_job
from lib import util

client = None
config = palo_config.config
LOG = palo_client.LOG
L = palo_client.L


def setup_module():
    """
    setUp
    """
    global client
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user, 
                                    password=config.fe_password, http_port=config.fe_http_port)


def create(table_name, datetype, info, column=None, keys_desc=None, partition_info=None):
    """
    建表
    info:动态分区建表参数
    """
    if partition_info is None:
        partition_info = palo_client.PartitionInfo('k1', [], [])
    distribution_info = palo_client.DistributionInfo('HASH(k1)', 3)
    dynamic_info = palo_client.DynamicPartitionInfo(info)
    dynamic = dynamic_info.to_string()
    if column is None:
        column = [('k1', datetype), ('k2', 'varchar(20)'), ('k3', 'int', 'sum')]
    client.create_table(table_name, column, partition_info, distribution_info, keys_desc=keys_desc, 
                dynamic_partition_info=dynamic)
    assert client.show_tables(table_name), 'fail to create table'


def get_partition_name_list(start, end, prefix):
    """
    分区名
    """
    partition_name_list = []
    for i in range(-start, -end - 1, -1):
        p_name = (datetime.datetime.now() - datetime.timedelta(days=i))
        partition_name_list.append(str(prefix + p_name.strftime("%Y%m%d")))
    return partition_name_list


def get_partition_value_list(start, end):
    """
    分区范围
    """
    partition_value_list = []
    for i in range(-start, -end - 1, -1):
        p_value = (datetime.datetime.now() - datetime.timedelta(days=i))
        p_value2 = (datetime.datetime.now() - datetime.timedelta(days=(i - 1)))
        p_value = p_value.strftime("%Y-%m-%d")
        p_value2 = p_value2.strftime("%Y-%m-%d")
        tmp = '[types: [DATEV2]; keys: [%s]; ..types: [DATEV2]; keys: [%s]; )' % (p_value, p_value2)
        partition_value_list.append(tmp)
    return partition_value_list 


def check_partition_list(table_name, partition_name_list):
    """
    验证分区是否创建成功
    """
    ret = client.get_partition_list(table_name)
    partitions_list = util.get_attr(ret, palo_job.PartitionInfo.PartitionName)
    expect_plist_sorted = sorted(partition_name_list)
    
    actural_plist_sorted = sorted(partitions_list)
    assert expect_plist_sorted == actural_plist_sorted, \
           'expect: %s, actural: %s' % (expect_plist_sorted, actural_plist_sorted)


def check_partition_range(table_name, partition_value_list):
    """
    验证分区范围是否正确
    """
    ret = client.get_partition_list(table_name)
    LOG.info(L('', PARTITION_RANGE=util.get_attr(ret, palo_job.PartitionInfo.Range), EXPECT=partition_value_list))
    assert util.get_attr(ret, palo_job.PartitionInfo.Range) == partition_value_list


def test_dynamic_partition_enable():
    """
    {
    "title": "test_sys_dynamic_partition_parameter.test_dynamic_partition_enable",
    "describe": "验证dynamic_partition.enable参数功能enable=true开启动态分区功能",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'start': -3, 'end': 3, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true'}
    create(table_name, 'date', dynamic_partition_info)
    partition_name_list = get_partition_name_list(-3, 3, 'p')
    partition_value_list = get_partition_value_list(-3, 3)
    check_partition_list(table_name, partition_name_list)
    check_partition_range(table_name, partition_value_list)    
    client.drop_table(table_name, database_name)
    dynamic_partition_info = {'enable': 'false', 'time_unit': 'DAY', 'start': -3, 'end': 3, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true'}
    create(table_name, 'date', dynamic_partition_info)
    check_partition_list(table_name, [])
    client.clean(database_name)


def test_dynamic_partition_hour():
    """
    {
    "title": "test_sys_dynamic_partition_parameter.test_dynamic_partition_hour",
    "describe": "time_unit=HOUR建表，仅支持datetime格式",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    now_time = datetime.datetime.now()
    partition_name_list = []
    for i in range(30, -31, -1):
        p_name = (now_time - datetime.timedelta(hours=i))
        partition_name_list.append(str("p" + p_name.strftime("%Y%m%d%H")))
    partition_value_list = []
    for i in range(30, -31, -1):
        p_value = (now_time - datetime.timedelta(hours=i))
        p_value2 = (now_time - datetime.timedelta(hours=(i - 1)))
        p_value = p_value.strftime("%Y-%m-%d %H:00:00")
        p_value2 = p_value2.strftime("%Y-%m-%d %H:00:00")
        partition = '[types: [DATETIMEV2]; keys: [%s]; ..types: [DATETIMEV2]; keys: [%s]; )' % (p_value, p_value2)
        partition_value_list.append(partition)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'HOUR', 'start': -30, 'end': 30, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true'}
    create(table_name, 'datetime', dynamic_partition_info)
    check_partition_list(table_name, partition_name_list)
    check_partition_range(table_name, partition_value_list)
    client.clean(database_name)


def test_dynamic_partition_day_date():
    """
    {
    "title": "test_sys_dynamic_partition_parameter.test_dynamic_partition_day_date",
    "describe": "time_unit=DAY建表，datetype=date",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_name_list = get_partition_name_list(-3, 3, 'p')
    partition_value_list = get_partition_value_list(-3, 3)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'start': -3, 'end': 3, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true'}
    create(table_name, 'date', dynamic_partition_info)
    check_partition_list(table_name, partition_name_list)
    check_partition_range(table_name, partition_value_list)
    client.clean(database_name)


def test_dynamic_partition_day_int():
    """
    {
    "title": "test_sys_dynamic_partition_parameter.test_dynamic_partition_day_int",
    "describe": "time_unit=DAY建表，datetype=int",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_name_list = get_partition_name_list(-3, 3, 'p')
    partition_value_list = []
    for i in range(3, -4, -1):
        p_value = (datetime.datetime.now() - datetime.timedelta(days=i))
        p_value2 = (datetime.datetime.now() - datetime.timedelta(days=(i - 1)))
        p_value = p_value.strftime("%Y%m%d")
        p_value2 = p_value2.strftime("%Y%m%d")
        partition = '[types: [INT]; keys: [%s]; ..types: [INT]; keys: [%s]; )' % (p_value, p_value2)
        partition_value_list.append(partition)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'start': -3, 'end': 3, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true'}
    create(table_name, 'int', dynamic_partition_info)
    check_partition_list(table_name, partition_name_list)
    check_partition_range(table_name, partition_value_list)
    client.clean(database_name)


def test_dynamic_partition_day_datetime():
    """
    {
    "title": "test_sys_dynamic_partition_parameter.test_dynamic_partition_day_datetime",
    "describe": "time_unit=DAY建表,datetype=datetime",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_name_list = get_partition_name_list(-3, 3, 'p')
    partition_value_list = []
    for i in range(3, -4, -1):
        p_value = (datetime.datetime.now() - datetime.timedelta(days=i))
        p_value2 = (datetime.datetime.now() - datetime.timedelta(days=(i - 1)))
        p_value = p_value.strftime("%Y-%m-%d 00:00:00")
        p_value2 = p_value2.strftime("%Y-%m-%d 00:00:00")
        partition = '[types: [DATETIMEV2]; keys: [%s]; ..types: [DATETIMEV2]; keys: [%s]; )' % (p_value, p_value2)
        partition_value_list.append(partition)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'start': -3, 'end': 3, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true'}
    create(table_name, 'datetime', dynamic_partition_info)
    check_partition_list(table_name, partition_name_list)
    check_partition_range(table_name, partition_value_list)
    client.clean(database_name)


def test_dynamic_partition_week_date():
    """
    {
    "title": "test_sys_dynamic_partition_parameter.test_dynamic_partition_week_date",
    "describe": "time_unit=WEEK建表，datetype=date"
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_name_list = []
    today = datetime.datetime.now().weekday()
    start_day = datetime.datetime.now() - datetime.timedelta(days=today)
    # todo: 2022-2023 week name not same
    for i in range(30, -31, -1):
        p_name = start_day - datetime.timedelta(days=7 * i)
        week = str("0" + str(int(p_name.strftime("%U")) + 1))
        partition_name_list.append(str("p" + p_name.strftime("%Y_") + week[-2:]))
    partition_value_list = []
    for i in range(30, -31, -1):
        p_value = start_day - datetime.timedelta(days=7 * i)
        p_value2 = start_day - datetime.timedelta(days=(7 * (i - 1)))
        p_value = p_value.strftime("%Y-%m-%d")
        p_value2 = p_value2.strftime("%Y-%m-%d")
        partition = '[types: [DATEV2]; keys: [%s]; ..types: [DATEV2]; keys: [%s]; )' % (p_value, p_value2)
        partition_value_list.append(partition)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'WEEK', 'start': -30, 'end': 30, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true'}
    create(table_name, 'date', dynamic_partition_info)
    # check_partition_list(table_name, partition_name_list)
    check_partition_range(table_name, partition_value_list)
    client.clean(database_name)


def test_dynamic_partition_week_datetime():
    """
    {
    "title": "test_sys_dynamic_partition_parameter.test_dynamic_partition_week_datetime",
    "describe": "time_unit=WEEK建表, datetype=datetime"
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_name_list = []
    today = datetime.datetime.now().weekday()
    start_day = datetime.datetime.now() - datetime.timedelta(days=today)
    for i in range(30, -31, -1):
        p_name = start_day - datetime.timedelta(days=7 * i)
        week = str("0" + str(int(p_name.strftime("%U")) + 1))
        partition_name_list.append(str("p" + p_name.strftime("%Y_") + week[-2:]))
    partition_value_list = []
    for i in range(30, -31, -1):
        p_value = start_day - datetime.timedelta(days=7 * i)
        p_value2 = start_day - datetime.timedelta(days=(7 * (i - 1)))
        p_value = p_value.strftime("%Y-%m-%d 00:00:00")
        p_value2 = p_value2.strftime("%Y-%m-%d 00:00:00")
        partition = '[types: [DATETIMEV2]; keys: [%s]; ..types: [DATETIMEV2]; keys: [%s]; )' % (p_value, p_value2)
        partition_value_list.append(partition)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'WEEK', 'start': -30, 'end': 30, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true'}
    create(table_name, 'datetime', dynamic_partition_info)
    # check_partition_list(table_name, partition_name_list)
    check_partition_range(table_name, partition_value_list)
    client.drop_table(table_name, database_name)
    client.clean(database_name)


def test_dynamic_partition_week_int():
    """
    {
    "title": "test_sys_dynamic_partition_parameter.test_dynamic_partition_week_int",
    "describe": "time_unit=WEEK建表, datetype=int"
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_name_list = []
    today = datetime.datetime.now().weekday()
    start_day = datetime.datetime.now() - datetime.timedelta(days=today)
    for i in range(30, -31, -1):
        p_name = start_day - datetime.timedelta(days=7 * i)
        week = str("0" + str(int(p_name.strftime("%U")) + 1))
        partition_name_list.append(str("p" + p_name.strftime("%Y_") + week[-2:]))
    partition_value_list = []
    for i in range(30, -31, -1):
        p_value = start_day - datetime.timedelta(days=7 * i)
        p_value2 = start_day - datetime.timedelta(days=(7 * (i - 1)))
        p_value = p_value.strftime("%Y%m%d")
        p_value2 = p_value2.strftime("%Y%m%d")
        partition = '[types: [INT]; keys: [%s]; ..types: [INT]; keys: [%s]; )' % (p_value, p_value2)
        partition_value_list.append(partition)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'WEEK', 'start': -30, 'end': 30, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true'}
    create(table_name, 'int', dynamic_partition_info)
    # check_partition_list(table_name, partition_name_list)
    check_partition_range(table_name, partition_value_list)
    client.clean(database_name)


def test_dynamic_partition_month_date():
    """
    {
    "title": "test_sys_dynamic_partition_parameter.test_dynamic_partition_month_date",
    "describe": "time_unit=MONTH建表，datetype=date",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_name_list = []
    for i in range(10, -11, -1):
        p_name = datetime.datetime.now() - relativedelta(months=i)
        partition_name_list.append(str("p" + p_name.strftime("%Y%m")))
    partition_value_list = []
    start_day = datetime.datetime.now()
    for i in range(10, -11, -1):
        p_value = start_day - relativedelta(months=i)
        p_value2 = start_day - relativedelta(months=(i - 1))
        p_value = p_value.strftime("%Y-%m") + "-01"
        p_value2 = p_value2.strftime("%Y-%m") + "-01"
        partition = '[types: [DATEV2]; keys: [%s]; ..types: [DATEV2]; keys: [%s]; )' % (p_value, p_value2)
        partition_value_list.append(partition)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'MONTH', 'start': -10, 'end': 10, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true'}
    create(table_name, 'date', dynamic_partition_info)
    check_partition_list(table_name, partition_name_list)
    check_partition_range(table_name, partition_value_list)
    client.clean(database_name)


def test_dynamic_partition_month_datetime():
    """
    {
    "title": "test_sys_dynamic_partition_parameter.test_dynamic_partition_month_datetime",
    "describe": "time_unit=MONTH建表，datetype=datetime",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_name_list = []
    for i in range(10, -11, -1):
        p_name = datetime.datetime.now() - relativedelta(months=i)
        partition_name_list.append(str("p" + p_name.strftime("%Y%m")))
    partition_value_list = []
    start_day = datetime.datetime.now()
    for i in range(10, -11, -1):
        p_value = start_day - relativedelta(months=i)
        p_value2 = start_day - relativedelta(months=(i - 1))
        p_value = p_value.strftime("%Y-%m") + "-01 00:00:00"
        p_value2 = p_value2.strftime("%Y-%m") + "-01 00:00:00"
        partition = '[types: [DATETIMEV2]; keys: [%s]; ..types: [DATETIMEV2]; keys: [%s]; )' % (p_value, p_value2)
        partition_value_list.append(partition)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'MONTH', 'start': -10, 'end': 10, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true'}
    create(table_name, 'datetime', dynamic_partition_info)
    check_partition_list(table_name, partition_name_list)
    check_partition_range(table_name, partition_value_list)
    client.clean(database_name)


def test_dynamic_partition_month_int():
    """
    {
    "title": "test_sys_dynamic_partition_parameter.test_dynamic_partition_month_int",
    "describe": "time_unit=MONTH建表，datetype=int",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_name_list = []
    for i in range(10, -11, -1):
        p_name = datetime.datetime.now() - relativedelta(months=i)
        partition_name_list.append(str("p" + p_name.strftime("%Y%m")))
    partition_value_list = []
    start_day = datetime.datetime.now()
    for i in range(10, -11, -1):
        p_value = start_day - relativedelta(months=i)
        p_value2 = start_day - relativedelta(months=(i - 1))
        p_value = p_value.strftime("%Y%m") + "01"
        p_value2 = p_value2.strftime("%Y%m") + "01"
        partition = '[types: [INT]; keys: [%s]; ..types: [INT]; keys: [%s]; )' % (p_value, p_value2)
        partition_value_list.append(partition)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'MONTH', 'start': -10, 'end': 10, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true'}
    create(table_name, 'int', dynamic_partition_info)
    check_partition_list(table_name, partition_name_list)
    check_partition_range(table_name, partition_value_list)
    client.clean(database_name)


def test_dynamic_partition_time_zone():
    """
    {
    "title": "test_sys_dynamic_partition_parameter.test_dynamic_partition_time_zone",
    "describe": "验证dynamic_partition.time_zone参数功能",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_name_list = []
    for i in range(11, 4, -1):
        p_name = (datetime.datetime.now() - datetime.timedelta(hours=i))
        partition_name_list.append(str("p" + p_name.strftime("%Y%m%d%H")))
    partition_value_list = []
    for i in range(11, 4, -1):
        p_value = (datetime.datetime.now() - datetime.timedelta(hours=i))
        p_value2 = (datetime.datetime.now() - datetime.timedelta(hours=(i - 1)))
        p_value = p_value.strftime("%Y-%m-%d %H:00:00")
        p_value2 = p_value2.strftime("%Y-%m-%d %H:00:00")
        partition = '[types: [DATETIMEV2]; keys: [%s]; ..types: [DATETIMEV2]; keys: [%s]; )' % (p_value, p_value2)
        partition_value_list.append(partition)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'HOUR', 'start': -3, 'end': 3, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true', 'time_zone': 'Etc/GMT'}
    create(table_name, 'datetime', dynamic_partition_info)
    check_partition_list(table_name, partition_name_list)
    check_partition_range(table_name, partition_value_list)
    client.clean(database_name)


def test_dynamic_partition_start():
    """
    {
    "title": "test_sys_dynamic_partition_parameter.test_dynamic_partition_start",
    "describe": "验证dynamic_partition.start参数功能",
    "tag": "p1,function,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    start_list = [-5, -3, -10, -1, -100]
    for start in start_list:
        dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'start': start, 'end': 3, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true'}
        create(table_name, 'date', dynamic_partition_info)
        partition_name_list = get_partition_name_list(start, 3, 'p')
        partition_value_list = get_partition_value_list(start, 3)
        check_partition_list(table_name, partition_name_list)
        check_partition_range(table_name, partition_value_list)
        client.drop_table(table_name, database_name)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'start': 0, 'end': 3, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true'}
    util.assert_return(False, 'Dynamic partition start must less than 0', create, table_name, 'date', \
                        dynamic_partition_info)
    client.clean(database_name)   


def test_dynamic_partition_end():
    """
    {
    "title": "test_sys_dynamic_partition_parameter.test_dynamic_partition_end",
    "describe": "验证dynamic_partition.end参数功能",
    "tag": "p1,function,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    end_list = [3, 5, 10, 100, 1]
    for end in end_list:
        dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'start': -3, 'end': end, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true'}
        create(table_name, 'date', dynamic_partition_info)
        partition_name_list = get_partition_name_list(-3, end, 'p')
        partition_value_list = get_partition_value_list(-3, end)
        check_partition_list(table_name, partition_name_list)
        check_partition_range(table_name, partition_value_list)
        client.drop_table(table_name, database_name)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'start': -3, 'end': 0, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true'}
    util.assert_return(False, 'Dynamic partition end must greater than 0', create, table_name, 'date', \
                        dynamic_partition_info)
    client.clean(database_name) 


def test_dynamic_partition_prefix():
    """
    {
    "title": "test_sys_dynamic_partition_parameter.test_dynamic_partition_prefix",
    "describe": "验证dynamic_partition.prefix参数功能",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    prefix_list = ['snfkjds', 'a', 'vhklskhsfdkksdv', 'fds', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa']
    for prefix in prefix_list:
        partition_name_list = get_partition_name_list(-3, 3, prefix)
        partition_value_list = get_partition_value_list(-3, 3)
        dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'start': -3, 'end': 3, 'prefix': prefix, 
            'buckets': 10, 'create_history_partition': 'true'}
        create(table_name, 'date', dynamic_partition_info)
        check_partition_list(table_name, partition_name_list)
        check_partition_range(table_name, partition_value_list)
        client.drop_table(table_name, database_name)
    client.clean(database_name) 


def test_dynamic_partition_buckets():
    """
    {
    "title": "test_sys_dynamic_partition_parameter.test_dynamic_partition_buckets",
    "describe": "验证dynamic_partition.buckets参数功能",
    "tag": "p1,function,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    buckets_list = [1, 2, 5, 10, 100]
    for buckets in buckets_list:
        partition_name_list = get_partition_name_list(-3, 3, 'p')
        partition_value_list = get_partition_value_list(-3, 3)
        dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'start': -3, 'end': 3, 'prefix': 'p', 
            'buckets': buckets, 'create_history_partition': 'true'}
        create(table_name, 'date', dynamic_partition_info)
        check_partition_list(table_name, partition_name_list)
        check_partition_range(table_name, partition_value_list) 
        ret = client.get_partition_list(table_name)
        assert util.get_attr(ret, palo_job.PartitionInfo.Buckets) == [str(buckets) for i in range(7)]
        client.drop_table(table_name, database_name)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'start': -3, 'end': 3, 'prefix': 'p', 
            'buckets': -10, 'create_history_partition': 'true'}
    util.assert_return(False, 'Dynamic partition buckets must greater than 0', create, table_name, 'date', \
                        dynamic_partition_info)
    client.clean(database_name)


def test_dynamic_partition_replication_num():
    """
    {
    "title": "test_sys_dynamic_partition_parameter.test_dynamic_partition_replication_num",
    "describe": "验证dynamic_partition.replication_num参数功能",
    "tag": "p1,function,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    replication_num_list = [1, 2, 3]
    for replication_num in replication_num_list:
        partition_name_list = get_partition_name_list(-3, 3, 'p')
        partition_value_list = get_partition_value_list(-3, 3)
        dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'start': -3, 'end': 3, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true', 'replication_num': replication_num}
        create(table_name, 'date', dynamic_partition_info)
        check_partition_list(table_name, partition_name_list)
        check_partition_range(table_name, partition_value_list) 
        ret = client.get_partition_list(table_name)
        assert util.get_attr(ret, palo_job.PartitionInfo.ReplicationNum) == [str(replication_num) for i in range(7)]
        client.drop_table(table_name, database_name)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'start': -3, 'end': 3, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true', 'replication_num': -3}
    util.assert_return(False, 'Dynamic partition replication num must greater than 0', create, table_name, 'date', \
                        dynamic_partition_info)
    client.clean(database_name)


def test_dynamic_partition_start_day_of_week():
    """
    {
    "title": "test_sys_dynamic_partition_parameter.test_dynamic_partition_start_day_of_week",
    "describe": "验证dynamic_partition.start_day_of_week参数功能",
    "tag": "p1,function,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    start_day_of_week_list = [1, 2, 3, 4, 5, 6, 7]
    for start_day_of_week in start_day_of_week_list:
        today = datetime.datetime.now().weekday()
        start_day = datetime.datetime.now() - datetime.timedelta(days=(today - start_day_of_week + 1))
        partition_name_list = []
        for i in range(3, -4, -1):
            p_name = start_day - datetime.timedelta(days=7 * i)
            week = str("0" + str(p_name.strftime("%U")))
            partition_name_list.append(str("p" + p_name.strftime("%Y_") + week[-2:])) 
        partition_value_list = []
        for i in range(3, -4, -1):
            p_value = start_day - datetime.timedelta(days=7 * i)
            p_value2 = start_day - datetime.timedelta(days=(7 * (i - 1)))
            p_value = p_value.strftime("%Y-%m-%d")
            p_value2 = p_value2.strftime("%Y-%m-%d")
            partition = '[types: [DATEV2]; keys: [%s]; ..types: [DATEV2]; keys: [%s]; )' % (p_value, p_value2)
            partition_value_list.append(partition)
        dynamic_partition_info = {'enable': 'true', 'time_unit': 'WEEK', 'start': -3, 'end': 3, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true', 'start_day_of_week': start_day_of_week}
        create(table_name, 'date', dynamic_partition_info)
        check_partition_list(table_name, partition_name_list)
        check_partition_range(table_name, partition_value_list)
        client.drop_table(table_name, database_name)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'WEEK', 'start': -3, 'end': 3, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true', 'start_day_of_week': 8}
    util.assert_return(False, 'between 1 and 7', create, table_name, 'date', dynamic_partition_info)
    client.clean(database_name)


def test_dynamic_partition_start_day_of_month():
    """
    {
    "title": "test_sys_dynamic_partition_parameter.test_dynamic_partition_start_day_of_month",
    "describe": "验证dynamic_partition.start_day_of_month参数功能",
    "tag": "p1,function,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    start_day_of_month_list = [1, 2, 5, 10, 28]
    for start_day_of_month in start_day_of_month_list:
        partition_name_list = []
        today = datetime.datetime.now().strftime("%d")
        if int(today) - start_day_of_month >= 0:
            month_count = 0
        else:
            month_count = 1
        for i in range(3 + month_count, -4 + month_count, -1):
            p_name = datetime.datetime.now() - relativedelta(months=i)
            partition_name_list.append(str("p" + p_name.strftime("%Y%m")))
        partition_value_list = []
        start_day = datetime.datetime.now()
        for i in range(3 + month_count, -4 + month_count, -1):
            p_value = start_day - relativedelta(months=i)
            p_value2 = start_day - relativedelta(months=(i - 1))
            if start_day_of_month >= 10:
                p_value = p_value.strftime("%Y-%m") + "-" + str(start_day_of_month)
                p_value2 = p_value2.strftime("%Y-%m") + "-" + str(start_day_of_month)
            else:
                p_value = p_value.strftime("%Y-%m") + "-0" + str(start_day_of_month)
                p_value2 = p_value2.strftime("%Y-%m") + "-0" + str(start_day_of_month)
            partition = '[types: [DATEV2]; keys: [%s]; ..types: [DATEV2]; keys: [%s]; )' % (p_value, p_value2)
            partition_value_list.append(partition)
        dynamic_partition_info = {'enable': 'true', 'time_unit': 'MONTH', 'start': -3, 'end': 3, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true', 'start_day_of_month': start_day_of_month}
        create(table_name, 'date', dynamic_partition_info)
        check_partition_list(table_name, partition_name_list)
        check_partition_range(table_name, partition_value_list)
        client.drop_table(table_name, database_name)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'MONTH', 'start': -3, 'end': 3, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true', 'start_day_of_month': 29}
    util.assert_return(False, 'between 1 and 28', create, table_name, 'date', dynamic_partition_info)
    client.clean(database_name)


def test_dynamic_partition_create_history_partition():
    """
    {
    "title": "test_sys_dynamic_partition_parameter.test_dynamic_partition_create_history_partition",
    "describe": "验证dynamic_partition.create_history_partition参数功能",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'start': -3, 'end': 3, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'false'}
    create(table_name, 'date', dynamic_partition_info)
    partition_name_list = get_partition_name_list(0, 3, 'p')
    partition_value_list = get_partition_value_list(0, 3)
    check_partition_list(table_name, partition_name_list)
    check_partition_range(table_name, partition_value_list)
    client.clean(database_name) 


def test_dynamic_partition_hot_partition_num():
    """
    {
    "title": "test_sys_dynamic_partition_parameter.test_dynamic_partition_hot_partition_num",
    "describe": "验证dynamic_partition.hot_partition_num参数功能",
    "tag": "p1,function,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_name_list = get_partition_name_list(-3, 3, 'p')
    partition_value_list = get_partition_value_list(-3, 3)
    hot_partition_num_list = [None, 0, 1, 2, 3, 4]
    for hot_partition_num in hot_partition_num_list:
        dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'start': -3, 'end': 3, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true', 'hot_partition_num': hot_partition_num}
        create(table_name, 'date', dynamic_partition_info)
        if hot_partition_num is None or hot_partition_num == 0:
            storage_medium = ['HDD' for i in range(7)]
        else:
            storage_medium = ['HDD' for i in range(4 - hot_partition_num)] + \
                ['SSD' for i in range(hot_partition_num + 3)]
        ret = client.get_partition_list(table_name)
        assert util.get_attr(ret, palo_job.PartitionInfo.StorageMedium) == storage_medium
        client.drop_table(table_name, database_name)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'start': -3, 'end': 3, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true', 'hot_partition_num': -1}
    util.assert_return(False, 'larger than 0', create, table_name, 'date', dynamic_partition_info)
    client.clean(database_name) 


def test_dynamic_partition_history_partition_num():
    """
    {
    "title": "test_sys_dynamic_partition_parameter.test_dynamic_partition_history_partition_num",
    "describe": "验证dynamic_partition.history_partition_num参数功能",
    "tag": "p1,function,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    history_partition_num_list = [5, 3, -1, 1, 0, None]
    for num in history_partition_num_list:
        dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'start': -3, 'end': 3, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true', 'history_partition_num': num}
        create(table_name, 'date', dynamic_partition_info)
        if num is None or num == -1:
            start = -3
        else:
            start = -min(3, num)
        partition_name_list = get_partition_name_list(start, 3, 'p')
        partition_value_list = get_partition_value_list(start, 3)
        check_partition_list(table_name, partition_name_list)
        check_partition_range(table_name, partition_value_list)
        client.drop_table(table_name, database_name)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'start': -3, 'end': 3, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true', 'history_partition_num': -2}
    util.assert_return(False, 'Dynamic history partition num must greater than 0', create, table_name, 'date', \
                        dynamic_partition_info)
    client.clean(database_name)
    

def test_composite_partition():
    """
    {
    "title": "test_sys_dynamic_partition_parameter.test_composite_partition",
    "describe": "验证复合分区创建动态分区表，建表失败",
    "tag": "p1,function,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    column = [('k1', 'date'), ('k2', 'date'), ('k3', 'int')]
    partition_info = palo_client.PartitionInfo(['k1', 'k2'], [], [])
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'start': -3, 'end': 3, 'prefix': 'p',
            'buckets': 10, 'create_history_partition': 'true'}
    util.assert_return(False, 'only support single-column range partition', create, table_name, 'date', 
            dynamic_partition_info, column, partition_info=partition_info)
    client.clean(database_name)


def test_list_partition():
    """
    {
    "title": "test_sys_dynamic_partition_parameter.test_list_partition",
    "describe": "验证list分区创建动态分区表，建表失败",
    "tag": "p1,function,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_info = palo_client.PartitionInfo(['k1'], [], [], partition_type='LIST')
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'start': -3, 'end': 3, 'prefix': 'p',
            'buckets': 10, 'create_history_partition': 'true'}
    util.assert_return(False, 'Only support dynamic partition properties on range partition table', 
            create, table_name, 'date', dynamic_partition_info, partition_info=partition_info)
    client.clean(database_name)


def test_range_partition():
    """
    {
    "title": "test_sys_dynamic_partition_parameter.test_range_partition",
    "describe": "验证range分区创建动态分区表，建表成功",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_info = palo_client.PartitionInfo(['k1'], [], [], partition_type='RANGE')
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'start': -3, 'end': 3, 'prefix': 'p',
            'buckets': 10, 'create_history_partition': 'true'}
    util.assert_return(True, None, create, table_name, 'date', dynamic_partition_info, partition_info=partition_info)
    client.clean(database_name)


def test_type():
    """
    {
    "title": "test_sys_dynamic_partition_parameter.test_type",
    "describe": "分区数据格式不为date/datetime/int时创建动态分区表失败",
    "tag": "p1,function,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'start': -3, 'end': 3, 'prefix': 'p',
            'buckets': 10, 'create_history_partition': 'true'}
    typelist = ['float', 'double', 'char', 'varchar', 'hll', 'bitmap']
    for type in typelist:
        column = [('k1', type), ('k2', 'date'), ('k3', 'int')]
        util.assert_return(False, 'errCode', create, table_name, 'date', dynamic_partition_info, column=column)
    client.clean(database_name)


def test_aggregation_aggregate():
    """
    {
    "title": "test_sys_dynamic_partition_parameter.test_aggregation_aggregate",
    "describe": "AGGREGATE聚合方式创建动态分区表",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    column = [('k1', 'date'), ('k2', 'int', 'sum'), ('k3', 'int', 'max')]
    keys_desc = 'AGGREGATE KEY(k1)'
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'start': -3, 'end': 3, 'prefix': 'p',
            'buckets': 10, 'create_history_partition': 'true'}
    util.assert_return(True, None, create, table_name, 'date', dynamic_partition_info, column, keys_desc=keys_desc)
    client.clean(database_name)


def test_aggregation_unique():
    """
    {
    "title": "test_sys_dynamic_partition_parameter.test_aggregation_unique",
    "describe": "UNIQUE聚合方式创建动态分区表",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    column = [('k1', 'date'), ('k2', 'int'), ('k3', 'int')]
    keys_desc = 'UNIQUE KEY(k1)'
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'start': -3, 'end': 3, 'prefix': 'p',
            'buckets': 10, 'create_history_partition': 'true'}
    util.assert_return(True, None, create, table_name, 'date', dynamic_partition_info, column, keys_desc=keys_desc)
    client.clean(database_name)


def test_aggregation_duplicate():
    """
    {
    "title": "test_sys_dynamic_partition_parameter.test_aggregation_duplicate",
    "describe": "DUPLICATE聚合方式创建动态分区表",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    column = [('k1', 'date'), ('k2', 'int'), ('k3', 'int')]
    keys_desc = 'DUPLICATE KEY(k1)'
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'start': -3, 'end': 3, 'prefix': 'p',
            'buckets': 10, 'create_history_partition': 'true'}
    util.assert_return(True, None, create, table_name, 'date', dynamic_partition_info, column, keys_desc=keys_desc)
    client.clean(database_name)


def test_create_history_partition_with_no_start():
    """
    {
    "title": "test_sys_dynamic_partition_parameter.test_create_history_partition_with_no_start",
    "describe": "不设置start创建历史分区,issues #5995",
    "tag": "p1,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'end': 3, 'prefix': 'p',
            'buckets': 10, 'create_history_partition': 'true'}
    util.assert_return(False, 'Provide start or history_partition_num property when creating history partition', \
            create, table_name, 'date', dynamic_partition_info)
    client.clean(database_name)


if __name__ == '__main__':
    setup_module()
    test_dynamic_partition_enable()
