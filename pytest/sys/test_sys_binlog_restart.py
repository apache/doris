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
#   @file test_sys_binlog_restart.py
#   @date 2021/11/02 14:37:00
#   @brief This file is a test file for doris binlog load.
#
#############################################################################

"""
binlog load be/fe 异常测试
MySQL需开启binlog功能
"""

import os
import sys
import time
import pymysql
sys.path.append("../")
from lib import node_op
from lib import palo_config
from lib import palo_client
from lib import palo_job
from lib import util
from data import binlog as DATA
client = None
config = palo_config.config
LOG = palo_client.LOG
L = palo_client.L

canal_ip = config.canal_ip
WAIT_TIME = 20


def setup_module():
    """
    setUp
    """
    global node_operator
    node_operator = node_op.Node()
    global client
    master = node_operator.get_master()
    client = palo_client.get_client(master, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    global connect
    connect = pymysql.connect(host=config.mysql_host, port=config.mysql_port, user=config.canal_user, \
                                passwd=config.canal_password)
    # client.set_frontend_config('enable_create_sync_job', 'true')
    global destination
    destination = 'example_' + str(config.fe_query_port)


def check_fe_be():
    """检查fe和be，是否有false，如果有则拉起来"""
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


def mysql_execute(sql):
    """
    连接mysql执行语句
    """
    cursor = connect.cursor()
    try:
        LOG.info(L('mysql check sql', sql=sql))
        cursor.execute(sql)
        cursor.close()
        connect.commit()
    except Exception as error:
        assert False, "execute error. %s" % str(error)
        LOG.error(L("mysql execute error", error=str(error)))
    time.sleep(1)
    return cursor.fetchall()


def create_mysql_table(mysql_table_name, mysql_database_name, columns, key='PRIMARY KEY (k1)', new_database=True):
    """
    创建MySQL数据库和表
    """
    if new_database:
        mysql_clean(mysql_database_name)
        sql = "CREATE DATABASE %s" % mysql_database_name
        try:
            mysql_execute(sql)
        except Exception as error:
            LOG.error(L("CREATE database error", host=config.canal_host, database_name=mysql_database_name, \
                    error=error))
    connect.select_db(mysql_database_name)
    sql = "DROP TABLE IF EXISTS %s" % mysql_table_name
    mysql_execute(sql)
    sql = ''
    for column in columns:
        column_sql = '%s %s' % (column[0], column[1])
        if len(column) > 2:
            if column[2]:
                column_sql = '%s %s' % (column_sql, column[2])
        if len(column) > 3:
            column_sql = '%s DEFAULT "%s"' % (column_sql, column[3])
        sql = '%s %s,' % (sql, column_sql)
    sql = "CREATE table %s (%s %s)" % (mysql_table_name, sql, key)
    LOG.info(L('mysql check sql', sql=sql))
    try:
        mysql_execute(sql)
    except Exception as error:
        LOG.error(L("CREATE TABLE error", host=config.canal_host, table_name=mysql_table_name, error=error))
        return False
    return True


def mysql_clean(mysql_database_name):
    """
    mysql drop database
    """
    sql = "DROP DATABASE IF EXISTS %s" % mysql_database_name
    mysql_execute(sql)


def test_restart_be():
    """
    {
    "title": "test_sys_binlog_restart:test_restart_be",
    "describe": "创建binlog load任务，be宕机，mysql执行导入语句，验证数据导入到doris",
    "tag": "p0,system,fuzz"
    }
    """
    check_fe_be()
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + '_' + str(config.fe_query_port)
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, 
                                 job_name, canal_ip, destination=destination)
    assert ret, 'create sync job failed'
    assert client.get_sync_job_state(job_name) == 'RUNNING', 'sync job state error'
    be_id = client.get_backend_id_list()[0]
    be_host = client.get_be_hostname_by_id(be_id)
    ret = node_operator.stop_be(be_host)
    assert ret, 'stop be failed'
    time.sleep(WAIT_TIME)
    sql = open(DATA.binlog_sql_4, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    time.sleep(WAIT_TIME)
    assert client.get_sync_job_state(job_name) == 'RUNNING', 'sync job state error'
    assert client.verify(DATA.expected_file_3, table_name)
    ret = node_operator.start_be(be_host)
    assert ret, 'start be failed'
    client.stop_sync_job(job_name)
    client.clean(database_name)
    mysql_clean(mysql_database_name)
    check_fe_be()


def test_restart_fe():
    """
    {
    "title": "test_sys_binlog_restart:test_restart_fe",
    "describe": "创建binlog load任务，fe master宕机，mysql执行导入语句，验证数据导入到doris",
    "tag": "p0,system,fuzz"
    }
    """
    check_fe_be()
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + '_' + str(config.fe_query_port)
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, 
                                 job_name, canal_ip, destination=destination)
    assert ret, 'create sync job failed'
    assert client.get_sync_job_state(job_name) == 'RUNNING', 'sync job state error'
    master_port = client.get_master()
    master = master_port.split(':')[0]
    ret = node_operator.stop_fe(master)
    assert ret, 'stop fe failed'
    time.sleep(WAIT_TIME)
    sql = open(DATA.binlog_sql_4, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    time.sleep(WAIT_TIME)
    ret = node_operator.start_fe(master)
    assert ret, 'start fe failed'
    time.sleep(WAIT_TIME)
    client.connect()
    client.use(database_name)
    assert client.get_sync_job_state(job_name) == 'RUNNING', 'sync job state error'
    assert client.verify(DATA.expected_file_3, table_name)
    client.stop_sync_job(job_name)
    client.clean(database_name)
    mysql_clean(mysql_database_name)
    check_fe_be()
