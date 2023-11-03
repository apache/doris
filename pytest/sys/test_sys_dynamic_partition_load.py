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
#   @file test_create_dynamic_partition.py
#   @date 2021/06/23 10:28:20
#   @brief This file is a test file for doris dynamic partition parameters.
#
#############################################################################

"""
测试动态分区导入
"""

import time
import datetime
import sys
import os
sys.path.append("../")
from dateutil.relativedelta import relativedelta
from lib import palo_config
from lib import palo_client
from lib import util

client = None
config = palo_config.config
LOG = palo_client.LOG
L = palo_client.L
broker_info = palo_config.broker_info
file_path = os.path.split(os.path.realpath(__file__))[0]
random_time = [0, 1, 3, 5, 10, 3, 5, 20, 70, 40, -5, -3, -1, -1, -2, 0, 3, 2, 0, 9, 11, 10, -9, -10,
                300, 400, -300, -400, 100, -100, -100, 365, -365, 120, 0, -50, 50, 30, 31, 101]

random_char = ['fnjl', 'fsd', 'afd', 'dsf', 'afl', 'ore', 'etou', 'jor', 'fljf', 'fjlk', \
                'hro', 'af', 'fef', 'poet', 'nvfk', 'aln', 'rio', 'ro', 'aaa', 'rou', \
                'orto', 'aeor', 'pjp', 'tuhi', 'mvkl', 'fnl', 'gor', 'roo', 'froh', 'roc', \
                'tqie', 'cz', 'fl', 'ajpf', 'vmjl', 'ep', 'vml', 'pjo', 'nk', 'sss']


def setup_module():
    """
    setUp
    """
    global client
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user, \
                                    password=config.fe_password, http_port=config.fe_http_port)


def create(table_name, datetype, info, keys_desc=None, column=None):
    """
    建表
    info:动态分区建表参数
    """
    partition_info = palo_client.PartitionInfo('k2', [], [])
    distribution_info = palo_client.DistributionInfo('HASH(k2)', 3)
    dynamic_info = palo_client.DynamicPartitionInfo(info)
    dynamic = dynamic_info.to_string()
    if column is None:
        column = [('k1', 'int'), ('k2', datetype), ('k3', 'int'), ('k4', 'varchar(16)')]
    client.create_table(table_name, column, partition_info, distribution_info, dynamic_partition_info=dynamic, 
            keys_desc=keys_desc)
    assert client.show_tables(table_name), 'fail to create table'


def load_value(time_unit, table_name, database_name):
    """
    按照不同的time_unit生成insert语句和验证结果
    """
    now = datetime.datetime.now()
    value_list = ''
    result = []
    count = 1
    if time_unit == 'DAY':
        for i in range(40):
            date_value = (now + datetime.timedelta(days=random_time[i])).strftime('%Y-%m-%d')
            value_list = '%s (%s, "%s", %s, "%s"),' % (value_list, count, date_value, random_time[i], random_char[i])
            if -10 <= random_time[i] <= 10:
                year, month, day = time.strptime(date_value, '%Y-%m-%d')[:3]
                result = [(count, datetime.date(year, month, day), random_time[i], random_char[i])] + result
            count += 1
    if time_unit == 'HOUR':
        for i in range(40):
            date_value = (now + datetime.timedelta(hours=random_time[i])).strftime('%Y-%m-%d %H:00:00')
            value_list = '%s (%s, "%s", %s, "%s"),' % (value_list, count, date_value, random_time[i], random_char[i])
            if -10 <= random_time[i] <= 10:
                year, month, day, hour = time.strptime(date_value, '%Y-%m-%d %H:%M:%S')[:4]
                result = [(count, datetime.datetime(year, month, day, hour), random_time[i], random_char[i])] + result
            count += 1
    if time_unit == 'WEEK':
        for i in range(40):
            date_value = (now + datetime.timedelta(days=random_time[i])).strftime('%Y-%m-%d')
            value_list = '%s (%s, "%s", %s, "%s"),' % (value_list, count, date_value, random_time[i], random_char[i])
            today = datetime.datetime.now().weekday()
            if -70 - today <= random_time[i] <= 77 - today:
                year, month, day = time.strptime(date_value, '%Y-%m-%d')[:3]
                result = [(count, datetime.date(year, month, day), random_time[i], random_char[i])] + result
            count += 1
    if time_unit == 'MONTH':
        for i in range(40):
            date_value = (now + datetime.timedelta(days=random_time[i])).strftime('%Y-%m-%d')
            value_list = '%s (%s, "%s", %s, "%s"),' % (value_list, count, date_value, random_time[i], random_char[i])
            start_day = (now - relativedelta(months=10)).strftime('%Y-%m-01')
            end_day = (now + relativedelta(months=11)).strftime('%Y-%m-01')
            if start_day <= date_value < end_day:
                year, month, day = time.strptime(date_value, '%Y-%m-%d')[:3]
                result = [(count, datetime.date(year, month, day), random_time[i], random_char[i])] + result
            count += 1
    sql = 'insert into %s values %s' % (table_name, value_list[:-1])
    return sql, tuple(sorted(result))


def check_insert(datetype, time_unit, table_name, database_name=None):
    """
    insert导入数据，查询结果
    eg: database_name = test_sys_dynamic_partition_load_test_day_db
        table_name = test_sys_dynamic_partition_load_test_day_tb
    """
    sql, result = load_value(time_unit, table_name, database_name)
    client.set_variables('enable_insert_strict', 'false')
    ret = client.execute(sql)
    assert ret == (), "Failed to insert data"
    sql = "SELECT * FROM %s.%s ORDER BY k1" % (database_name, table_name)
    ret = client.execute(sql)
    util.check(ret, result)
    client.drop_table(table_name)


def check_broker_load(datetype, time_unit, table_name, database_name=None):
    """
    broker load 导入数据及结果验证
    """
    load_file_path = palo_config.gen_remote_file_path("sys/dynamic_partition/test_dynamic_partition_load.data")
    column_name_list = ['k1', 'k3', 'k4']
    if time_unit == 'HOUR':
        set_list_value = ['k2=hours_add(now(),k3)']
    else:
        set_list_value = ['k2=date_add(now(),k3)']
    load_data_file = palo_client.LoadDataInfo(load_file_path, table_name, column_name_list=column_name_list, 
                            column_terminator=' ', set_list=set_list_value)
    assert client.batch_load(util.get_label(), load_data_file, is_wait=True, broker=broker_info,
                             max_filter_ratio=0.7, strict_mode=False)
    tmp, result = load_value(time_unit, table_name, database_name)
    if time_unit != 'HOUR':
        sql = 'select * from %s order by k1' % (table_name)
    elif time_unit == 'HOUR':
        sql = 'select k1,str_to_date(date_format(k2, "%%Y-%%m-%%d %%H"),"%%Y-%%m-%%d %%H") \
                as k2,k3,k4 from %s order by k1' % (table_name)
    ret = client.execute(sql)
    util.check(ret, result)
    client.drop_table(table_name)


def check_stream_load(datetype, time_unit, table_name, database_name=None):
    """
    stream load 导入数据及结果验证
    """
    data_file = "%s/data/STREAM_LOAD/test_dynamic_partition_load.data" % file_path
    if time_unit != 'HOUR':
        column = ["c1,c2,c3,k1=c1,k2=date_add(now(),c2),k3=c2,k4=c3"]
    else:
        column = ["c1,c2,c3,k1=c1,k2=hours_add(now(),c2),k3=c2,k4=c3"]
    assert client.stream_load(table_name, data_file, max_filter_ratio=0.7, column_name_list=column, 
                column_separator=',')
    tmp, result = load_value(time_unit, table_name, database_name)
    if time_unit != 'HOUR':
        sql = 'select * from %s order by k1' % (table_name)
    else:
        sql = 'select k1,str_to_date(date_format(k2, "%%Y-%%m-%%d %%H"),"%%Y-%%m-%%d %%H") \
            as k2,k3,k4 from %s order by k1' % (table_name)
    ret = client.execute(sql)
    util.check(ret, result)
    client.drop_table(table_name)


def test_day():
    """
    {
    "title": "test_sys_dynamic_partition_load.test_day",
    "describe": "导入数据到动态分区，time_unit=DAY",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'start': -10, 'end': 10, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true'}
    #insert
    create(table_name, 'date', dynamic_partition_info)
    check_insert('date', 'DAY', table_name, database_name)
    #broker load
    create(table_name, 'date', dynamic_partition_info)
    check_broker_load('date', 'DAY', table_name, database_name)
    #stream load
    create(table_name, 'date', dynamic_partition_info)
    check_stream_load('date', 'DAY', table_name, database_name)
    client.clean(database_name)


def test_hour():
    """
    {
    "title": "test_sys_dynamic_partition_load.test_hour",
    "describe": "导入数据到动态分区，time_unit=HOUR",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'HOUR', 'start': -10, 'end': 10, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true'}
    #insert
    if time.strftime("%M", time.localtime()) == '59':
        time.sleep(90)
    create(table_name, 'datetime', dynamic_partition_info)
    check_insert('datetime', 'HOUR', table_name, database_name)
    #broker load
    create(table_name, 'datetime', dynamic_partition_info)
    check_broker_load('datetime', 'HOUR', table_name, database_name)
    #stream load
    create(table_name, 'datetime', dynamic_partition_info)
    check_stream_load('datetime', 'HOUR', table_name, database_name)
    client.clean(database_name)


def test_week():
    """
    {
    "title": "test_sys_dynamic_partition_load.test_week",
    "describe": "导入数据到动态分区，time_unit=WEEK",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'WEEK', 'start': -10, 'end': 10, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true'}
    #insert
    create(table_name, 'date', dynamic_partition_info)
    check_insert('date', 'WEEK', table_name, database_name)
    #broker load
    create(table_name, 'date', dynamic_partition_info)
    check_broker_load('date', 'WEEK', table_name, database_name)
    #stream load
    create(table_name, 'date', dynamic_partition_info)
    check_stream_load('date', 'WEEK', table_name, database_name)
    client.clean(database_name)


def test_month():
    """
    {
    "title": "test_sys_dynamic_partition_load.test_month",
    "describe": "导入数据到动态分区，time_unit=MONTH",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'MONTH', 'start': -10, 'end': 10, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true'}
    #insert
    create(table_name, 'date', dynamic_partition_info)
    check_insert('date', 'MONTH', table_name, database_name)
    #broker load
    create(table_name, 'date', dynamic_partition_info)
    check_broker_load('date', 'MONTH', table_name, database_name)
    #stream load
    create(table_name, 'date', dynamic_partition_info)
    check_stream_load('date', 'MONTH', table_name, database_name)
    client.clean(database_name)


def test_aggregate_load():
    """
    {
    "title": "test_sys_dynamic_partition_load.test_aggregate__load",
    "describe": "AGGREGATE聚合表导入数据",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'start': -10, 'end': 10, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true'}
    column = [('k1', 'int'), ('k2', 'date'), ('k3', 'int'), ('k4', 'varchar(16)', 'replace')]
    keys_desc = 'AGGREGATE KEY(k1, k2, k3)'
    #insert
    create(table_name, 'date', dynamic_partition_info, keys_desc, column)
    check_insert('date', 'DAY', table_name, database_name)
    #broker load
    create(table_name, 'date', dynamic_partition_info, keys_desc, column)
    check_broker_load('date', 'DAY', table_name, database_name)
    #stream load
    create(table_name, 'date', dynamic_partition_info, keys_desc, column)    
    check_stream_load('date', 'DAY', table_name, database_name)
    client.clean(database_name)


def test_unique_load():
    """
    {
    "title": "test_sys_dynamic_partition_load.test_unique_load",
    "describe": "UNIQUE聚合表导入数据",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'start': -10, 'end': 10, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true'}
    keys_desc = 'UNIQUE KEY(k1, k2)'
    #insert
    create(table_name, 'date', dynamic_partition_info, keys_desc)
    check_insert('date', 'DAY', table_name, database_name)
    #broker load
    create(table_name, 'date', dynamic_partition_info, keys_desc)
    check_broker_load('date', 'DAY', table_name, database_name)
    #stream load
    create(table_name, 'date', dynamic_partition_info, keys_desc)
    check_stream_load('date', 'DAY', table_name, database_name)
    client.clean(database_name)


def test_duplicate_load():
    """
    {
    "title": "test_sys_dynamic_partition_load.test_duplicate_load",
    "describe": "DUPLICATE聚合表导入数据",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'start': -10, 'end': 10, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true'}
    keys_desc = 'DUPLICATE KEY(k1, k2)'
    #insert
    create(table_name, 'date', dynamic_partition_info, keys_desc)
    check_insert('date', 'DAY', table_name, database_name)
    #broker load
    create(table_name, 'date', dynamic_partition_info, keys_desc)
    check_broker_load('date', 'DAY', table_name, database_name)
    #stream load
    create(table_name, 'date', dynamic_partition_info, keys_desc)
    check_stream_load('date', 'DAY', table_name, database_name)
    client.clean(database_name)


if __name__ == '__main__':
    setup_module()
    test_day()
                                              

