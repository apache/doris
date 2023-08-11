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

###########################################################################
#   @file common.py
#   @date 2020-07-14 19:11:39
#   @brief common function
############################################################################
"""
common test tool
"""
import re
import time

import palo_client
import palo_config
import palo_logger
import palo_job
import palo_types
import util

config = palo_config.config
LOG = palo_logger.Logger.getLogger()
L = palo_logger.StructedLogMessage


def get_client(host=None):
    """get a new client"""
    if host is None:
        host = config.fe_host
    client = palo_client.get_client(host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    return client


def create_workspace(database_name):
    """get new palo client connect, and create db, and use and return client"""
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    return client


def check2(client1, sql1, client2=None, sql2=None, forced=False):
    """
    check2
    client can be PaloClient or mysql.cursor
    """
    if client2 is None and sql2 is None:
        assert 0 == 1, 'the input of check2() is wrong'
    elif client2 is None:
        client2 = client1
    elif sql2 is None:
        sql2 = sql1
    ret1 = client1.execute(sql1)
    ret2 = client2.execute(sql2)
    util.check(ret1, ret2, forced)
    return True


def get_explain_rollup(client, sql):
    """
    Get explain table
    """
    result = client.execute('EXPLAIN ' + sql)
    if result is None:
        return None
    rollup_flag = 'TABLE: '
    explain_rollup = list()
    for element in result:
        message = element[0].lstrip()
        profile = message.split(',')
        for msg in profile:
            if msg.startswith(rollup_flag):
                LOG.info(L('explain', msg=msg))
                table = msg[len(rollup_flag):].rstrip(' ')
                pattern = re.compile(r'[(](.*?)[)]', re.S)
                rollup = re.findall(pattern, table)
                explain_rollup.append(rollup[0])
    return explain_rollup


def assert_stop_routine_load(ret, client, stop_job=None, info=''):
    """assert, if not stop routine load """
    ret = client.show_routine_load(stop_job)
    if not ret and stop_job is not None:
        try:
            client.stop_routine_load(stop_job)
        except Exception as e:
            print(str(e))
    assert ret, info


def wait_commit(client, routine_load_job_name, committed_expect_num, timeout=600):
    """wait task committed"""
    print('expect commited rows: %s\n' % committed_expect_num)
    while timeout > 0:
        ret = client.show_routine_load(routine_load_job_name,)
        routine_load_job = palo_job.RoutineLoadJob(ret[0])
        loaded_rows = routine_load_job.get_loaded_rows()
        print(loaded_rows)
        if str(loaded_rows) == str(committed_expect_num):
            return True
        state = routine_load_job.get_state()
        if state != 'RUNNING':
            print('routine load job\' state is not running, it\'s %s' % state)
            return False
        timeout -= 3
        time.sleep(3)
    return False


def execute_ignore_error(func, *argc, **kwargs):
    """execute ignore sql error"""
    try:
        func(*argc, **kwargs)
    except Exception as e:
        LOG.info(L('execute ignore error', msg=str(e)))
        print(e)


def execute_retry_when_msg(msg, func, *argc, **kwargs):
    """当SQL返回的结果包含msg，则重试"""
    retry = 20
    while retry > 0:
        try:
            ret = func(*argc, **kwargs)
            return ret
        except Exception as e:
            if msg in str(e):
                time.sleep(3)
                retry -= 1
            LOG.info(L("get error msg", msg=str(e)))
    return False


def check_by_file(expect_file, table_name=None, sql=None,
                  client=None, database_name=None, **kwargs):
    """
    通过csv校验文件，验证表或sql的结果。
    根据sql执行返回的列的类型，读取csv文件转化为相应的结构，进行校验
    expect_file: 校验文件
    table_name：表名，被校验的表
    sql: sql，被校验的sql查询，表和sql不能同时为None
    client: 执行sql的client，如果为None则重新创建连接
    database_name: 数据库名称
    **kwargs：可以指定sql或表中的类型，可在复杂类型中使用，例： k2=palo_types.ARRAY_INT
              如array返回为字符串类型，对于array<double/fload>等可单独指定类型校验
    """
    LOG.info(L('check file', file=expect_file))
    if table_name is None and sql is None:
        assert 0 == 1, 'there is no table and query to be checked'
    if client is None:
        client = get_client()
        if database_name:
            client.use(database_name)
    if table_name:
        if database_name:
            table_name = '%s.%s' % (database_name, table_name)
        sql = 'select * from %s' % table_name
    cursor, ret = client.execute(sql, return_cursor=True)
    result_info = cursor.description
    column_name_list = util.get_attr(result_info, 0)
    column_type_list = util.get_attr(result_info, 1)
    for col_name, col_type in kwargs.items():
        try:
            idx = column_name_list.index(col_name)
            column_type_list[idx] = col_type
        except Exception as e:
            print(type(col_name), type(column_name_list[1]))
            print("%s is not the column in %s" % (col_name, column_name_list))
    expect_ret = palo_types.convert_csv_to_ret(expect_file, column_type_list)
    # 判断sql结果是否含有复杂类型，如果有，则需要对复杂类型进行处理
    if max(column_type_list) > 1000:
        ret = palo_types.convert_ret_complex_type(ret, column_type_list)
    util.check(ret, expect_ret, True)
    return True


def check_by_sql(tested_sql, expect_sql, client=None, database_name=None, **kwargs):
    """
    适用于复杂类型的两个sql结果校验。将两个sql分别执行，格式化处理，保存到文件中进行校验
    通过**kwargs指定列的类型
    tested_sql: 被测sql
    expect_sql: 校验sql，预期正确的结果
    """
    if client is None:
        client = get_client()
        if database_name:
            client.use(database_name)
    
    cursor, expect_ret = client.execute(expect_sql, return_cursor=True)
    # 根据cursor获取返回结果的列名和类型code
    result_info = cursor.description
    column_name_list = util.get_attr(result_info, 0)
    column_type_list = util.get_attr(result_info, 1)
    tested_ret = client.execute(tested_sql)
    
    for col_name, col_type in kwargs.items():
        try:
            idx = column_name_list.index(col_name)
            column_type_list[idx] = col_type
        except Exception as e:
            print(type(col_name), type(column_name_list[1]))
            print("%s is not the column in %s" % (col_name, column_name_list))

    if len(kwargs) != 0:
        processed_expect_ret = palo_types.convert_ret_complex_type(expect_ret, column_type_list)
        processed_tested_ret = palo_types.convert_ret_complex_type(tested_ret, column_type_list)
        util.check(processed_tested_ret, processed_expect_ret, True)
    else:
        util.check(tested_ret, expect_ret, True)
    return True

