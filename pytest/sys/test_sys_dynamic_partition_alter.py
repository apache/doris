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
#   @file test_sys_dynamic_partition_alter.py
#   @date 2021/06/23 10:28:20
#   @brief This file is a test file for alter doris dynamic partition parameters.
#
#############################################################################

"""
测试动态分区功能参数修改
"""

import time
import datetime
import sys
sys.path.append("../")
from dateutil.relativedelta import relativedelta
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


def create(table_name, datetype, info):
    """
    建表
    info:dic类型,动态分区建表参数
    """
    partition_info = palo_client.PartitionInfo('k1', [], [])
    distribution_info = palo_client.DistributionInfo('HASH(k1)', 3)
    dynamic_info = palo_client.DynamicPartitionInfo(info)
    dynamic = dynamic_info.to_string()
    column = [('k1', datetype), ('k2', 'varchar(20)'), ('k3', 'int', 'sum')]
    client.create_table(table_name, column, partition_info, distribution_info, dynamic_partition_info=dynamic)
    assert client.show_tables(table_name), 'fail to create table'


def get_partition_name_list(start, end, prefix):
    """
    生成分区名
    """
    partition_name_list = []
    for i in range(-start, -end - 1, -1):
        p_name = (datetime.datetime.now() - datetime.timedelta(days=i))
        partition_name_list.append(str(prefix + p_name.strftime("%Y%m%d")))
    return partition_name_list


def get_partition_value_list(start, end):
    """
    生成分区范围
    """
    partition_value_list = []
    for i in range(-start, -end - 1, -1):
        p_value = (datetime.datetime.now() - datetime.timedelta(days=i))
        p_value2 = (datetime.datetime.now() - datetime.timedelta(days=(i - 1)))
        p_value = p_value.strftime("%Y-%m-%d")
        p_value2 = p_value2.strftime("%Y-%m-%d")
        tmp = '[types: [DATE]; keys: [%s]; ..types: [DATE]; keys: [%s]; )' % (p_value, p_value2)
        partition_value_list.append(tmp) 
    return partition_value_list  


def check_partition_list(table_name, partition_name_list):
    """
    验证分区是否创建成功
    """
    for partition_name in partition_name_list:
        assert client.get_partition(table_name, partition_name)


def check_partition_range(table_name, partition_value_list):
    """
    验证分区范围是否正确
    """
    ret = client.get_partition_list(table_name)
    LOG.info(L('', PARTITION_RANGE=util.get_attr(ret, palo_job.PartitionInfo.Range), EXPECT=partition_value_list))
    assert util.get_attr(ret, palo_job.PartitionInfo.Range) == partition_value_list, 'Partition range is incorrect'


def check_parameter(condition_col_idx, parameter, database_name=None):
    """
    验证指定的show dynamic partition tables中的参数值
    condition_col_idx:int类型，为验证参数在palo_job.DynamicPartitionInfo中对应的值
    """
    ret = client.show_dynamic_partition_tables(database_name)
    assert util.get_attr(ret, condition_col_idx) == parameter, 'Parameter value is incorrect'


def test_alter_enable():
    """
    {
    "title": "test_sys_dynamic_partition_alter.test_alter_enable",
    "describe": "验证修改dynamic_partition.enable参数",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    dynamic_partition_info = {'enable': 'false', 'time_unit': 'DAY', 'start': -3, 'end': 3, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true'}
    create(table_name, 'date', dynamic_partition_info)
    ret = client.modify_partition(table_name, **{"dynamic_partition.enable":"true"})
    assert ret, "Change dynamic_partition.enable failed"
    partition_name_list = get_partition_name_list(-3, 3, 'p')
    partition_value_list = get_partition_value_list(-3, 3)
    check_partition_list(table_name, partition_name_list)
    check_partition_range(table_name, partition_value_list)
    check_parameter(palo_job.DynamicPartitionInfo.Enable, ['true'], database_name) 

    ret = client.modify_partition(table_name, **{"dynamic_partition.enable":"false"})
    assert ret, "Change dynamic_partition.enable failed"
    check_parameter(1, ['false'], database_name)
    client.clean(database_name) 


def test_alter_time_unit():
    """
    {
    "title": "test_sys_dynamic_partition_alter.test_alter_time_unit",
    "describe": "验证修改dynamic_partition.time_unit参数",
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
    create(table_name, 'datetime', dynamic_partition_info)
    check_parameter(palo_job.DynamicPartitionInfo.TimeUnit, ['DAY'], database_name)
    ret = client.modify_partition(table_name, **{"dynamic_partition.time_unit":"WEEK"})
    assert ret, "Change dynamic_partition.time_unit failed"
    check_parameter(palo_job.DynamicPartitionInfo.TimeUnit, ['WEEK'], database_name)
    ret = client.modify_partition(table_name, **{"dynamic_partition.time_unit":"MONTH"})
    assert ret, "Change dynamic_partition.time_unit failed"
    check_parameter(palo_job.DynamicPartitionInfo.TimeUnit, ['MONTH'], database_name)
    ret = client.modify_partition(table_name, **{"dynamic_partition.time_unit":"DAY"})
    assert ret, "Change dynamic_partition.time_unit failed"
    check_parameter(palo_job.DynamicPartitionInfo.TimeUnit, ['DAY'], database_name)
    ret = client.modify_partition(table_name, **{"dynamic_partition.time_unit":"HOUR"})
    assert ret, "Change dynamic_partition.time_unit failed"
    check_parameter(palo_job.DynamicPartitionInfo.TimeUnit, ['HOUR'], database_name)
    client.clean(database_name)


def test_alter_time_zone():
    """
    {
    "title": "test_sys_dynamic_partition_alter.test_alter_time_zone",
    "describe": "验证修改dynamic_partition.time_zone参数",
    "tag": "p1,function"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'HOUR', 'start': -3, 'end': 3, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true'}
    create(table_name, 'datetime', dynamic_partition_info)
    #向前修改时区
    ret = client.modify_partition(table_name, **{"dynamic_partition.time_zone":"Asia/Tokyo"})
    assert ret, "Change dynamic_partition.time_zone failed"
    partition_name_list = []
    for i in range(2, -5, -1):
        p_name = (datetime.datetime.now() - datetime.timedelta(hours=i))
        partition_name_list.append(str("p" + p_name.strftime("%Y%m%d%H")))
    partition_value_list = []
    for i in range(2, -5, -1):
        p_value = (datetime.datetime.now() - datetime.timedelta(hours=i))
        p_value2 = (datetime.datetime.now() - datetime.timedelta(hours=(i - 1)))
        p_value = p_value.strftime("%Y-%m-%d %H:00:00")
        p_value2 = p_value2.strftime("%Y-%m-%d %H:00:00")
        partition = '[types: [DATETIME]; keys: [%s]; ..types: [DATETIME]; keys: [%s]; )' % (p_value, p_value2)
        partition_value_list.append(partition)
    check_partition_list(table_name, partition_name_list)
    check_partition_range(table_name, partition_value_list)
    #向后修改时区
    ret = client.modify_partition(table_name, **{"dynamic_partition.time_zone":"Asia/Jakarta"})
    assert ret, "Change dynamic_partition.time_zone failed"
    partition_name_list = []
    for i in range(4, -3, -1):
        p_name = (datetime.datetime.now() - datetime.timedelta(hours=i))
        partition_name_list.append(str("p" + p_name.strftime("%Y%m%d%H")))
    partition_value_list = []
    for i in range(4, -3, -1):
        p_value = (datetime.datetime.now() - datetime.timedelta(hours=i))
        p_value2 = (datetime.datetime.now() - datetime.timedelta(hours=(i - 1)))
        p_value = p_value.strftime("%Y-%m-%d %H:00:00")
        p_value2 = p_value2.strftime("%Y-%m-%d %H:00:00")
        partition = '[types: [DATETIME]; keys: [%s]; ..types: [DATETIME]; keys: [%s]; )' % (p_value, p_value2)
        partition_value_list.append(partition)
    check_partition_list(table_name, partition_name_list)
    check_partition_range(table_name, partition_value_list)
    client.clean(database_name)


def test_alter_start():
    """
    {
    "title": "test_sys_dynamic_partition_alter.test_alter_start",
    "describe": "验证修改dynamic_partition.start参数",
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
    create(table_name, 'date', dynamic_partition_info)
    check_parameter(palo_job.DynamicPartitionInfo.Start, ['-3'], database_name)
    
    ret = client.modify_partition(table_name, **{"dynamic_partition.start":"-5"})
    assert ret, "Change dynamic_partition.start failed"
    partition_name_list = get_partition_name_list(-5, 3, 'p')
    partition_value_list = get_partition_value_list(-5, 3)
    check_partition_list(table_name, partition_name_list)
    check_partition_range(table_name, partition_value_list)
    check_parameter(palo_job.DynamicPartitionInfo.Start, ['-5'], database_name)
    
    ret = client.modify_partition(table_name, **{"dynamic_partition.start":"-1"})
    assert ret, "Change dynamic_partition.start failed"
    partition_name_list = get_partition_name_list(-1, 3, 'p')
    partition_value_list = get_partition_value_list(-1, 3)
    check_partition_list(table_name, partition_name_list)
    check_partition_range(table_name, partition_value_list)
    check_parameter(palo_job.DynamicPartitionInfo.Start, ['-1'], database_name)

    sql = 'ALTER table %s set ("dynamic_partition.start" = "0")' % table_name
    util.assert_return(False, 'Dynamic partition start must less than 0', client.execute, sql)
    client.clean(database_name)


def test_alter_end():
    """
    {
    "title": "test_sys_dynamic_partition_alter.test_alter_end",
    "describe": "验证修改dynamic_partition.end参数",
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
    create(table_name, 'date', dynamic_partition_info)
    check_parameter(palo_job.DynamicPartitionInfo.End, ['3'], database_name)
    
    ret = client.modify_partition(table_name, **{"dynamic_partition.end":"5"})
    assert ret, "Change dynamic_partition.end failed"
    partition_name_list = get_partition_name_list(-3, 5, 'p')
    partition_value_list = get_partition_value_list(-3, 5)
    check_partition_list(table_name, partition_name_list)
    check_partition_range(table_name, partition_value_list)
    check_parameter(palo_job.DynamicPartitionInfo.End, ['5'], database_name)
    
    ret = client.modify_partition(table_name, **{"dynamic_partition.end":"1"})
    assert ret, "Change dynamic_partition.end failed"
    partition_name_list = get_partition_name_list(-3, 1, 'p')
    partition_value_list = get_partition_value_list(-3, 1)
    check_partition_list(table_name, partition_name_list)
    check_partition_range(table_name, partition_value_list)
    check_parameter(palo_job.DynamicPartitionInfo.End, ['1'], database_name)

    sql = 'ALTER table %s set ("dynamic_partition.end" = "0")' % table_name
    util.assert_return(False, 'Dynamic partition end must greater than 0', client.execute, sql)
    client.clean(database_name)


def test_alter_prefix():
    """
    {
    "title": "test_sys_dynamic_partition_alter.test_alter_prefix",
    "describe": "验证修改dynamic_partition.prefix参数",
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
    create(table_name, 'date', dynamic_partition_info)
    
    ret = client.modify_partition(table_name, **{"dynamic_partition.prefix":"abc"})
    assert ret, "Change dynamic_partition.prefix failed"
    ret = client.modify_partition(table_name, **{"dynamic_partition.end":"4"})
    assert ret, "Change dynamic_partition.end failed"
    partition_name_list = get_partition_name_list(-3, 3, 'p')
    new_partition_name = str('abc' + (datetime.datetime.now() - datetime.timedelta(days=-4)).strftime("%Y%m%d"))
    partition_name_list.append(new_partition_name)
    partition_value_list = get_partition_value_list(-3, 4)
    check_partition_list(table_name, partition_name_list)
    check_partition_range(table_name, partition_value_list)
    check_parameter(palo_job.DynamicPartitionInfo.Prefix, ['abc'], database_name)
    
    sql = 'ALTER table %s set ("dynamic_partition.prefix" = "0")' % table_name
    util.assert_return(False, 'Invalid dynamic partition prefix', client.execute, sql)
    client.clean(database_name)


def test_alter_buckets():
    """
    {
    "title": "test_sys_dynamic_partition_alter.test_alter_buckets",
    "describe": "验证修改dynamic_partition.buckets参数",
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
    create(table_name, 'date', dynamic_partition_info)
    check_parameter(palo_job.DynamicPartitionInfo.Buckets, ['10'], database_name)

    ret = client.modify_partition(table_name, **{"dynamic_partition.buckets":"1"})
    assert ret, "Change dynamic_partition.buckets failed"
    ret = client.modify_partition(table_name, **{"dynamic_partition.end":"4"})
    assert ret, "Change dynamic_partition.end failed"
    partition_name_list = get_partition_name_list(-3, 4, 'p')
    partition_value_list = get_partition_value_list(-3, 4)
    check_partition_list(table_name, partition_name_list)
    check_partition_range(table_name, partition_value_list)
    check_parameter(palo_job.DynamicPartitionInfo.Buckets, ['1'], database_name)
    assert client.get_partition_buckets(table_name, partition_name_list[-1], database_name) == '1'
    
    ret = client.modify_partition(table_name, **{"dynamic_partition.buckets":"12"})
    assert ret, "Change dynamic_partition.buckets failed"
    ret = client.modify_partition(table_name, **{"dynamic_partition.end":"5"})
    assert ret, "Change dynamic_partition.end failed"
    partition_name_list = get_partition_name_list(-3, 5, 'p')
    partition_value_list = get_partition_value_list(-3, 5)
    check_partition_list(table_name, partition_name_list)
    check_partition_range(table_name, partition_value_list)
    check_parameter(palo_job.DynamicPartitionInfo.Buckets, ['12'], database_name)
    assert client.get_partition_buckets(table_name, partition_name_list[-1], database_name) == '12'
    
    sql = 'ALTER table %s set ("dynamic_partition.buckets" = "0")' % table_name
    util.assert_return(False, 'Dynamic partition buckets must greater than 0', client.execute, sql)
    client.clean(database_name)


def test_alter_replication_num():
    """
    {
    "title": "test_sys_dynamic_partition_alter.test_alter_replication_num",
    "describe": "验证修改dynamic_partition.replication_num参数",
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
    create(table_name, 'date', dynamic_partition_info)
    check_parameter(palo_job.DynamicPartitionInfo.ReplicationNum, ['3'], database_name)
    
    ret = client.modify_partition(table_name, **{"dynamic_partition.replication_num":"1"})
    assert ret, "Change dynamic_partition.replication_num failed"
    ret = client.modify_partition(table_name, **{"dynamic_partition.end":"4"})
    assert ret, "Change dynamic_partition.end failed"
    partition_name_list = get_partition_name_list(-3, 4, 'p')
    partition_value_list = get_partition_value_list(-3, 4)
    check_partition_list(table_name, partition_name_list)
    check_partition_range(table_name, partition_value_list)
    check_parameter(palo_job.DynamicPartitionInfo.ReplicationNum, ['1'], database_name)
    assert client.get_partition_replication_num(table_name, partition_name_list[-1], database_name) == '1'
    
    sql = 'ALTER table %s set ("dynamic_partition.replication_num" = "0")' % table_name
    util.assert_return(False, 'Total replication num should between 1 and 32767', client.execute, sql)
    client.clean(database_name)


def test_alter_start_day_of_week():
    """
    {
    "title": "test_sys_dynamic_partition_alter.test_alter_start_day_of_week",
    "describe": "验证修改dynamic_partition.start_day_of_week参数",
    "tag": "p1,function,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'WEEK', 'start': -3, 'end': 3, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true'}
    create(table_name, 'date', dynamic_partition_info)
    check_parameter(palo_job.DynamicPartitionInfo.StartOf, ['MONDAY'], database_name)
    
    ret = client.modify_partition(table_name, **{"dynamic_partition.start_day_of_week":"2"})
    assert ret, "Change dynamic_partition.start_day_of_week failed"
    ret = client.modify_partition(table_name, **{"dynamic_partition.end":"4"})
    assert ret, "Change dynamic_partition.end failed"
    partition_name_list = []
    today = datetime.datetime.now().weekday()
    start_day = datetime.datetime.now() - datetime.timedelta(days=today)
    for i in range(3, -5, -1):
        p_name = start_day - datetime.timedelta(days=7 * i)
        week = str("0" + str(p_name.strftime("%U")))
        partition_name_list.append(str("p" + p_name.strftime("%Y_") + week[-2:]))
    partition_value_list = []
    for i in range(3, -4, -1):
        p_value = start_day - datetime.timedelta(days=7 * i)
        p_value2 = start_day - datetime.timedelta(days=(7 * (i - 1)))
        p_value = p_value.strftime("%Y-%m-%d")
        p_value2 = p_value2.strftime("%Y-%m-%d")
        tmp = '[types: [DATE]; keys: [%s]; ..types: [DATE]; keys: [%s]; )' % (p_value, p_value2)
        partition_value_list.append(tmp)
    p_value1 = (start_day + datetime.timedelta(days=7 * 4 + 1)).strftime("%Y-%m-%d")
    p_value2 = (start_day + datetime.timedelta(days=7 * 5 + 1)).strftime("%Y-%m-%d")
    new_partition = '[types: [DATE]; keys: [%s]; ..types: [DATE]; keys: [%s]; )' % (p_value1, p_value2)
    partition_value_list.append(new_partition)
    check_partition_list(table_name, partition_name_list[:-1])
    check_partition_range(table_name, partition_value_list)
    check_parameter(palo_job.DynamicPartitionInfo.StartOf, ['TUESDAY'], database_name)
    
    ret = client.modify_partition(table_name, **{"dynamic_partition.start_day_of_week":"7"})
    assert ret, "Change dynamic_partition.start_day_of_week failed"
    ret = client.modify_partition(table_name, **{"dynamic_partition.end":"5"})
    assert ret, "Change dynamic_partition.end failed"
    p_value1 = (start_day + datetime.timedelta(days=7 * 5 + 6)).strftime("%Y-%m-%d")
    p_value2 = (start_day + datetime.timedelta(days=7 * 6 + 6)).strftime("%Y-%m-%d")
    new_partition = '[types: [DATE]; keys: [%s]; ..types: [DATE]; keys: [%s]; )' % (p_value1, p_value2)
    partition_value_list.append(new_partition)
    check_partition_list(table_name, partition_name_list)
    check_partition_range(table_name, partition_value_list)
    check_parameter(palo_job.DynamicPartitionInfo.StartOf, ['SUNDAY'], database_name)

    sql = 'ALTER table %s set ("dynamic_partition.start_day_of_week" = "8")' % table_name
    util.assert_return(False, 'start_day_of_week should between 1 and 7', client.execute, sql)
    client.clean(database_name)


def test_alter_start_day_of_month():
    """
    {
    "title": "test_sys_dynamic_partition_alter.test_alter_start_day_of_month",
    "describe": "验证修改dynamic_partition.start_day_of_month参数",
    "tag": "p1,function,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'MONTH', 'start': -3, 'end': 3, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true'}
    create(table_name, 'date', dynamic_partition_info)
    check_parameter(palo_job.DynamicPartitionInfo.StartOf, ['1st'], database_name)

    ret = client.modify_partition(table_name, **{"dynamic_partition.start_day_of_month":"28"})
    assert ret, "Change dynamic_partition.start_day_of_month failed"
    ret = client.modify_partition(table_name, **{"dynamic_partition.end":"5"})
    assert ret, "Change dynamic_partition.end failed"
    partition_name_list = []
    for i in range(3, -8, -1):
        p_name = datetime.datetime.now() - relativedelta(months=i)
        partition_name_list.append(str("p" + p_name.strftime("%Y%m")))
    partition_value_list = []
    start_day = datetime.datetime.now()
    for i in range(3, -4, -1):
        p_value1 = (start_day - relativedelta(months=i)).strftime("%Y-%m-01")
        p_value2 = (start_day - relativedelta(months=(i - 1))).strftime("%Y-%m-01")
        tmp = '[types: [DATE]; keys: [%s]; ..types: [DATE]; keys: [%s]; )' % (p_value1, p_value2)
        partition_value_list.append(tmp)
    p_value1 = (start_day + relativedelta(months=4)).strftime("%Y-%m-28")
    p_value2 = (start_day + relativedelta(months=5)).strftime("%Y-%m-28")
    new_partition = '[types: [DATE]; keys: [%s]; ..types: [DATE]; keys: [%s]; )' % (p_value1, p_value2)
    partition_value_list.append(new_partition)
    if int(start_day.strftime('%d')) >= 28:
        p_value3 = (start_day + relativedelta(months=6)).strftime("%Y-%m-28")
        new_partition = '[types: [DATE]; keys: [%s]; ..types: [DATE]; keys: [%s]; )' % (p_value2, p_value3)
        partition_value_list.append(new_partition)
        check_partition_list(table_name, partition_name_list[:-2])
    else:
        check_partition_list(table_name, partition_name_list[:-3])
    check_partition_range(table_name, partition_value_list)
    check_parameter(palo_job.DynamicPartitionInfo.StartOf, ['28th'], database_name)
    
    ret = client.modify_partition(table_name, **{"dynamic_partition.start_day_of_month":"2"})
    assert ret, "Change dynamic_partition.start_day_of_month failed"
    ret = client.modify_partition(table_name, **{"dynamic_partition.end":"7"})
    assert ret, "Change dynamic_partition.end failed"
    p_value1 = (start_day + relativedelta(months=6)).strftime("%Y-%m-02")
    p_value2 = (start_day + relativedelta(months=7)).strftime("%Y-%m-02")
    p_value3 = (start_day + relativedelta(months=8)).strftime("%Y-%m-02")
    if int(start_day.strftime('%d')) < 28:
        new_partition = '[types: [DATE]; keys: [%s]; ..types: [DATE]; keys: [%s]; )' % (p_value1, p_value2)
        partition_value_list.append(new_partition)
        if int(start_day.strftime('%d')) < 2:
            check_partition_list(table_name, partition_name_list[:-3] + partition_name_list[-2:-1])
        else:
            check_partition_list(table_name, partition_name_list[:-3] + partition_name_list[-2:])
    else:
        check_partition_list(table_name, partition_name_list[:-2] + partition_name_list[-1:])
    new_partition = '[types: [DATE]; keys: [%s]; ..types: [DATE]; keys: [%s]; )' % (p_value2, p_value3)
    if int(start_day.strftime('%d')) >= 2:
        partition_value_list.append(new_partition)
    check_partition_range(table_name, partition_value_list)
    check_parameter(palo_job.DynamicPartitionInfo.StartOf, ['2nd'], database_name)

    sql = 'ALTER table %s set ("dynamic_partition.start_day_of_month" = "29")' % table_name
    util.assert_return(False, 'start_day_of_month should between 1 and 28', client.execute, sql)
    client.clean(database_name)


def test_alter_hot_partition_num():
    """
    {
    "title": "test_sys_dynamic_partition_alter.test_alter_hot_partition_num",
    "describe": "验证修改dynamic_partition.hot_partition_num参数",
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
    create(table_name, 'date', dynamic_partition_info)
    partition_name_list = get_partition_name_list(-5, 5, 'p')
    partition_value_list = get_partition_value_list(-5, 5)
    
    ret = client.modify_partition(table_name, **{"dynamic_partition.hot_partition_num":"5"})
    assert ret, "Change dynamic_partition.hot_partition_num failed"
    ret = client.modify_partition(table_name, **{"dynamic_partition.start":"-5"})
    assert ret, "Change dynamic_partition.start failed"
    ret = client.modify_partition(table_name, **{"dynamic_partition.end":"4"})
    assert ret, "Change dynamic_partition.end failed"
    check_partition_list(table_name, partition_name_list[:-1])
    check_partition_range(table_name, partition_value_list[:-1])
    assert client.get_partition_storage_medium(table_name, partition_name_list[0], database_name) == 'HDD'
    assert client.get_partition_storage_medium(table_name, partition_name_list[1], database_name) == 'SSD'
    assert client.get_partition_storage_medium(table_name, partition_name_list[-2], database_name) == 'SSD'

    ret = client.modify_partition(table_name, **{"dynamic_partition.hot_partition_num":"0"})
    assert ret, "Change dynamic_partition.hot_partition_num failed"
    ret = client.modify_partition(table_name, **{"dynamic_partition.end":"5"})
    assert ret, "Change dynamic_partition.end failed"
    check_partition_list(table_name, partition_name_list)
    check_partition_range(table_name, partition_value_list)
    assert client.get_partition_storage_medium(table_name, partition_name_list[-1], database_name) == 'HDD'
    
    sql = 'ALTER table %s set ("dynamic_partition.hot_partition_num" = "-1")' % table_name
    util.assert_return(False, 'hot_partition_num must larger than 0', client.execute, sql)
    client.clean(database_name)


def test_alter_history_partition_num():
    """
    {
    "title": "test_sys_dynamic_partition_alter.test_alter_history_partition_num",
    "describe": "验证修改dynamic_partition.history_partition_num参数",
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
    create(table_name, 'date', dynamic_partition_info)
    partition_name_list = get_partition_name_list(-3, 3, 'p')
    partition_value_list = get_partition_value_list(-3, 3)

    ret = client.modify_partition(table_name, **{"dynamic_partition.history_partition_num":"5"})
    assert ret, "Change dynamic_partition.history_partition_num failed"
    check_partition_list(table_name, partition_name_list)
    check_partition_range(table_name, partition_value_list)

    ret = client.modify_partition(table_name, **{"dynamic_partition.history_partition_num":"1"})
    assert ret, "Change dynamic_partition.history_partition_num failed"
    check_partition_list(table_name, partition_name_list)
    check_partition_range(table_name, partition_value_list)

    sql = 'ALTER table %s set ("dynamic_partition.history_partition_num" = "-2")' % table_name
    util.assert_return(False, 'Dynamic history partition num must greater than 0', client.execute, sql)
    client.clean(database_name)


def test_add_partition():
    """
    {
    "title": "test_sys_dynamic_partition_alter.test_add_partition",
    "describe": "对动态分区表手动增加和删除分区，关闭动态分区后验证增加和删除分区",
    "tag": "p1,function,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, \
        index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    dynamic_partition_info = {'enable': 'true', 'time_unit': 'DAY', 'start': -3, 'end': 3, 'prefix': 'p', 
            'buckets': 10, 'create_history_partition': 'true'}
    create(table_name, 'date', dynamic_partition_info)
    partition_name_list = get_partition_name_list(-3, 4, 'p')
    day1 = (datetime.datetime.now() + datetime.timedelta(days=4)).strftime("%Y-%m-%d")
    day2 = (datetime.datetime.now() + datetime.timedelta(days=5)).strftime("%Y-%m-%d")
    p_name = 'p' + (datetime.datetime.now() + datetime.timedelta(days=4)).strftime("%Y%m%d")
    sql = "ALTER TABLE %s ADD PARTITION %s VALUES [('%s'), ('%s'))" % (table_name, p_name, day1, day2)
    util.assert_return(False, 'Cannot add/drop partition on a Dynamic Partition Table', client.execute, sql)
    
    sql = "ALTER TABLE %s DROP PARTITION %s" % (table_name, partition_name_list[0])
    util.assert_return(False, 'Cannot add/drop partition on a Dynamic Partition Table', client.execute, sql)

    ret = client.modify_partition(table_name, **{"dynamic_partition.enable":"false"})
    assert ret, "Change dynamic_partition.enable failed"
    sql = "ALTER TABLE %s ADD PARTITION %s VALUES [('%s'), ('%s'))" % (table_name, p_name, day1, day2)
    ret = client.execute(sql)
    assert ret == (), "Add partition failed"
    ret = client.drop_partition(table_name, partition_name_list[0])
    assert ret, "Drop partition failed"
    check_partition_list(table_name, partition_name_list[1:])
    client.clean(database_name) 


def test_add_schema():
    """
    {
    "title": "test_sys_dynamic_partition_alter.test_add_schema",
    "describe": "对动态分区表增加和删除列操作"
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
    ret = client.schema_change(table_name, add_column_list=["k4 int"], is_wait=True)
    assert ret
    day = datetime.datetime.now().strftime("%Y-%m-%d")
    sql = "INSERT INTO %s VALUES('%s', 'a', 20210704, 10)" % (table_name, day)
    ret = client.execute(sql)
    assert ret == (), "Load dmic_partition_info = client.DynamicPartitionInfoata failed"
    ret = client.schema_change(table_name, modify_column_list=["k4 date"], is_wait=True)
    assert ret, "Modify column failed"
    ret = client.schema_change(table_name, order_column_list=["k4", "k1", "k2", "k3"], is_wait=True)
    assert ret, "Order column failed"
    ret = client.schema_change(table_name, drop_column_list=["k2"], is_wait=True)    
    assert ret, "Drop column falied"
    ret = client.select_all(table_name)
    sql = "SELECT cast('2021-07-04' as date), cast('%s' as date), 10" % day
    assert client.execute(sql) == ret, "Schema is incorrect"
    client.clean(database_name)


def test_alter_storage_medium():
    """
    {
    "title": "test_sys_dynamic_partition_alter.test_alter_storage_medium",
    "describe": "修改动态分区表的storage_medium参数"
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
    ret = client.modify_partition(table_name, partition_name_list, storage_medium='SSD')
    assert ret, "Alter storage medium failed"
    assert client.get_partition_storage_medium(table_name, partition_name_list[0], database_name) == 'SSD'
    ret = client.modify_partition(table_name, partition_name=partition_name_list[0], storage_medium='HDD')
    assert ret, "Alter storage medium failed"
    assert client.get_partition_storage_medium(table_name, partition_name_list[0], database_name) == 'HDD'
    check_partition_range(table_name, partition_value_list)
    client.clean(database_name)


def test_alter_cooldown_time():
    """
    {
    "title": "test_sys_dynamic_partition_alter.test_alter_cooldown_time",
    "describe": "修改动态分区的cooldown_time,报错Invalid data property. storage medium property is not found"
    "tag": "p1,fuzz"
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
    util.assert_return(False, 'Unknown table property', client.modify_partition, table_name, 
                    storage_cooldown_time='2100-01-01 00:00:00')
    client.clean(database_name)


if __name__ == '__main__':
    setup_module()
    test_alter_enable()
