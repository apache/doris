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
#   @file test_sys_binlog.py
#   @date 2021/11/02 14:37:00
#   @brief This file is a test file for doris binlog load.
#
#############################################################################

"""
测试binlog load
MySQL需开启binlog功能
"""

import os
import sys
import time
import pymysql
sys.path.append("../")
from lib import palo_config
from lib import palo_client
from lib import palo_job
from lib import util
from data import binlog as DATA
client = None
config = palo_config.config
LOG = palo_client.LOG
L = palo_client.L

port = str(config.fe_query_port)
canal_ip = config.canal_ip
WAIT_TIME = 15

def setup_module():
    """
    setUp
    """
    global client
    global connect
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    connect = pymysql.connect(host=config.mysql_host, port=config.mysql_port, user=config.canal_user, \
                                passwd=config.canal_password)
    # client.set_frontend_config('enable_create_sync_job', 'true')
    global destination
    destination = 'example_' + str(config.fe_query_port)


def mysql_execute(sql):
    """
    连接mysql执行导入语句
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
    cursor = connect.cursor()
    if new_database:
        mysql_clean(mysql_database_name)
        sql = "CREATE DATABASE %s" % mysql_database_name
        try:
            cursor.execute(sql)
            LOG.info(L('CREATE MYSQL db succ', database=mysql_database_name))
        except Exception as error:
            LOG.error(L("CREATE MYSQL db error", host=config.mysql_host, database_name=mysql_database_name, \
                    error=error))
    connect.select_db(mysql_database_name)
    sql = "DROP TABLE IF EXISTS %s" % mysql_table_name
    cursor.execute(sql)
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
    LOG.info(L('mysql table sql', sql=sql))
    try:
        cursor.execute(sql)
    except Exception as error:
        LOG.error(L("CREATE TABLE error", host=config.mysql_host, table_name=mysql_table_name, error=error))
        return False
    return True


def mysql_clean(mysql_database_name):
    """
    mysql drop database
    """
    sql = "DROP DATABASE IF EXISTS %s" % mysql_database_name
    cursor = connect.cursor()
    cursor.execute(sql)
    LOG.info(L("DROP MYSQL db", db=mysql_database_name))


def check(table_name, mysql_table_name):
    """验证doris与mysql数据同步"""
    sql_1 = "select * from %s order by k1" % table_name
    sql_2 = "select * from %s order by k1" % mysql_table_name
    cursor = connect.cursor()
    cursor.execute(sql_2)
    ret_2 = cursor.fetchall()
    time_limit = 0
    while time_limit < 60:
        ret_1 = client.execute(sql_1)
        if ret_1 == ret_2:
            return True
            break
        else:
            time_limit += 1
            time.sleep(2)
    return ret_1 == ret_2


def assertStop(ret, client, stop_job=None, info=''):
    """assert, if not stop binlog load"""
    if not ret and stop_job is not None:
        try:
            show_ret = client.show_sync_job()
            LOG.info(L('binlog load job info', ret=show_ret))
            client.stop_sync_job(stop_job)
        except Exception as e:
            print(str(e))
    assert ret, info


def check_data(data, table_name, job_name, fail_info, client=client):
    """验证binlog导入数据"""
    time_limit = 0
    try:
        while time_limit < 90:
            ret = client.verify(data, table_name)
            if ret:
                break
            else:
                time.sleep(2)
                time_limit += 1
    except Exception as e:
        LOG.info(L('get an error when check verify', msg=str(e)))
        client.stop_sync_job(job_name)
    assertStop(ret, client, job_name, fail_info)


def test_binlog():
    """
    {
    "title": "test_sys_binlog:test_binlog",
    "describe": "创建binlog load任务，mysql执行insert, delete, update操作，验证doris中导入数据成功",
    "tag": "p0,system"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name 
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
                                 destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    time.sleep(WAIT_TIME)
    #insert
    sql = open(DATA.binlog_sql_1, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_1, table_name, job_name, 'insert data failed', client)
    #delete
    sql = open(DATA.binlog_sql_2, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_2, table_name, job_name, 'delete data failed', client)
    #update
    sql = open(DATA.binlog_sql_3, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_3, table_name, job_name, 'update data failed', client)
    client.stop_sync_job(job_name)
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_binlog_function():
    """
    {
    "title": "test_sys_binlog:test_binlog_function",
    "describe": "创建binlog load任务，mysql执行导入语句中包含函数，验证doris中导入数据成功",
    "tag": "p0,system"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    time.sleep(WAIT_TIME)
    sql = open(DATA.binlog_sql_5, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_3, table_name, job_name, 'data load fail', client)
    client.stop_sync_job(job_name)
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_binlog_select():
    """
    {
    "title": "test_sys_binlog:test_binlog_select",
    "describe": "binlog load 过程中执行查询操作，验证查询结果正确",
    "tag": "p0,system"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    time.sleep(WAIT_TIME)
    sql = "select * from %s order by k1" % table_name
    ret = (client.execute(sql) == ())
    assertStop(ret, client, job_name, 'query data error')
    sql = open(DATA.binlog_sql_4, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    ret = check(table_name, mysql_table_name)
    assertStop(ret, client, job_name, 'select data error')
    client.stop_sync_job(job_name)
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_binlog_special():
    """
    {
    "title": "test_sys_binlog:test_binlog_special",
    "describe": "创建binlog load任务，mysql导入特殊值：NULL,随机数(支持decimal精度),时间，验证doris导入数据成功",
    "tag": "p0,system"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_2, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_2)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    time.sleep(WAIT_TIME)
    sql = 'insert into %s values (NULL, NULL, NULL, NULL, NULL)' % mysql_table_name
    util.assert_return(False, "Column 'k1' cannot be null", mysql_execute, sql)

    sql = 'insert into %s values (1, NULL, NULL, NULL, NULL)' % mysql_table_name
    mysql_execute(sql)
    ret = check(table_name, mysql_table_name)
    assertStop(ret, client, job_name, 'data load fail')

    sql = 'insert into %s values (2, FLOOR(RAND() * 10), "2021-08-18", "2021-08-18 00:05:10", 3.22), \
                (3, FLOOR(RAND() * 100), "2021-08-18", "2021-08-18 00:05:10", 3.22), \
                (4, FLOOR(RAND() * 1000), "2021-08-18", "2021-08-18 00:05:10", 3.22)' % mysql_table_name
    mysql_execute(sql)
    ret = check(table_name, mysql_table_name)
    assertStop(ret, client, job_name, 'data load fail')

    sql = 'insert into %s values (5, 20, "2021-08-18", "2021-08-18 00:05:10", RAND()), \
                (6, 20, "2021-08-18", "2021-08-18 00:05:10", RAND() * 10), \
                (7, 20, "2021-08-18", "2021-08-18 00:05:10", RAND() * 100)' % mysql_table_name
    mysql_execute(sql)
    ret = check(table_name, mysql_table_name)
    assertStop(ret, client, job_name, 'data load fail')

    sql = 'insert into %s values (8, 20, current_date, "2021-08-18 00:05:10", 3.22)' % mysql_table_name
    mysql_execute(sql)
    ret = check(table_name, mysql_table_name)
    assertStop(ret, client, job_name, 'data load fail')

    sql = 'insert into %s values (9, 20, "2021-08-18", now(), 3.22)' % mysql_table_name
    mysql_execute(sql)
    ret = check(table_name, mysql_table_name)
    assertStop(ret, client, job_name, 'data load fail')
    client.stop_sync_job(job_name)
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_partition():
    """
    {
    "title": "test_sys_binlog:test_partition",
    "describe": "开启binlog load，doris增加、删除分区，mysql执行导入语句，验证数据可导入到已有分区",
    "tag": "p0,system"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['20', '40', '60', '80']
    partition_info = palo_client.PartitionInfo('k1', partition_name_list, partition_value_list)
    client.create_table(table_name, DATA.column_1, partition_info, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    time.sleep(WAIT_TIME)
    sql = open(DATA.binlog_sql_4, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_3, table_name, job_name, 'data load fail', client)
    #导入数据在doris中无分区，无法导入
    sql = open(DATA.binlog_sql_12, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_3, table_name, job_name, 'data load fail', client)
    #add partition
    ret = client.add_partition(table_name, 'partition_e', '100')
    assertStop(ret, client, job_name, 'add partition failed')
    ret = client.verify(DATA.expected_file_3, table_name)
    assertStop(ret, client, job_name, 'data load fail')
    #drop partition without data
    ret = client.drop_partition(table_name, 'partition_d')
    assertStop(ret, client, job_name, 'Drop partition failed')
    check_data(DATA.expected_file_3, table_name, job_name, 'data load fail', client)
    #drop partition with data
    ret = client.drop_partition(table_name, 'partition_a')
    assertStop(ret, client, job_name, 'drop partition failed')
    ret = not client.verify(DATA.expected_file_3, table_name)
    assertStop(ret, client, job_name, 'data load fail')
    client.stop_sync_job(job_name)
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_mysql_transaction():
    """
    {
    "title": "test_sys_binlog:test_mysql_transaction",
    "describe": "开启binlog load，mysql提交事务，验证doris中数据导入成功",
    "tag": "p1,system"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql' + table_name
    transaction_table = mysql_table_name + '_t'
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    create_mysql_table(transaction_table, mysql_database_name, DATA.column_1, new_database=False)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    time.sleep(WAIT_TIME)
    sql_1 = open(DATA.binlog_sql_1, 'r').read().format(mysql_table_name)
    sql_2 = open(DATA.binlog_sql_1, 'r').read().format(transaction_table)
    sql_3 = open(DATA.binlog_sql_2, 'r').read().format(mysql_table_name)
    sql_4 = open(DATA.binlog_sql_3, 'r').read().format(mysql_table_name)
    sql_5 = open(DATA.binlog_sql_2, 'r').read().format(transaction_table)
    sql = "begin; %s; %s; %s; %s; %s; commit;" % (sql_1, sql_2, sql_3, sql_4, sql_5)
    try:
        mysql_execute(sql)
        ret = True
    except:
        ret = False
    assertStop(ret, client, job_name, 'mysql transaction failed')
    check_data(DATA.expected_file_3, table_name, job_name, 'data load fail', client)
    client.stop_sync_job(job_name)
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_diff_schema_1():
    """
    {
    "title": "test_sys_binlog:test_diff_schema_1",
    "describe": "mysql与doris表结构不同，doris表有mysql表不存在的列，任务进行，但数据无法导入到doris",
    "tag": "p1,system,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_3, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    time.sleep(WAIT_TIME)
    sql = open(DATA.binlog_sql_1, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    time.sleep(WAIT_TIME)
    sql = 'select * from %s' % table_name
    ret = (client.execute(sql) == ())
    assertStop(ret, client, job_name, 'query data error')
    ret = client.get_sync_job_state(job_name) == 'RUNNING'
    assertStop(ret, client, job_name, 'sync job is not running')
    client.stop_sync_job(job_name)
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_diff_schema_2():
    """
    {
    "title": "test_sys_binlog:test_diff_schema_2",
    "describe": "mysql与doris表结构不同，mysql表有doris表不存在的列，任务进行，但数据无法导入到doris",
    "tag": "p1,system,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_4, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    time.sleep(WAIT_TIME)
    sql = open(DATA.binlog_sql_1, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    time.sleep(WAIT_TIME)
    sql = 'select * from %s' % table_name
    ret = (client.execute(sql) == ())
    assertStop(ret, client, job_name, 'query data error')
    ret = client.get_sync_job_state(job_name) == 'RUNNING'
    assertStop(ret, client, job_name, 'sync job is not running')
    client.stop_sync_job(job_name)
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_alter_mysql_schema():
    """
    {
    "title": "test_sys_binlog:test_alter_mysql_schema",
    "describe": "创建binlog load任务，修改mysql表结构，mysql执行导入语句，验证数据按doris列导入",
    "tag": "p1,system"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name + str(int(time.time()))
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    time.sleep(WAIT_TIME)
    sql = open(DATA.binlog_sql_6, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_5, table_name, job_name, 'data load fail', client)
    #add column
    sql = "alter table %s add column k8 int(10)" % mysql_table_name
    mysql_execute(sql)
    sql = open(DATA.binlog_sql_9, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    time.sleep(WAIT_TIME)
    ret = client.verify(DATA.expected_file_5, table_name)
    assertStop(ret, client, job_name, 'data load fail')
    #drop new column
    sql = "alter table %s drop column k8" % mysql_table_name
    mysql_execute(sql)
    sql = open(DATA.binlog_sql_8, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data([DATA.expected_file_5, DATA.expected_file_7], table_name, job_name, 'data load fail', client)
    #drop column
    sql = "alter table %s drop column k7" % mysql_table_name
    mysql_execute(sql)
    sql = open(DATA.binlog_sql_10, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    time.sleep(WAIT_TIME)
    ret = client.verify([DATA.expected_file_5, DATA.expected_file_7], table_name)
    assertStop(ret, client, job_name, 'data load fail')
    client.stop_sync_job(job_name)
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_alter_doris_schema():
    """
    {
    "title": "test_sys_binlog:test_alter_doris_schema",
    "describe": "创建binlog load任务，修改doris表结构，mysql执行导入语句，验证数据按doris列导入",
    "tag": "p1,system"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    time.sleep(WAIT_TIME)
    sql = open(DATA.binlog_sql_6, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_5, table_name, job_name, 'data load fail', client)
    #add column
    client.pause_sync_job(job_name)
    client.schema_change(table_name, add_column_list=["k8 int"], is_wait=True)
    client.resume_sync_job(job_name)
    sql = open(DATA.binlog_sql_7, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_8, table_name, job_name, 'data load fail', client)
    #drop new column
    client.schema_change(table_name, drop_column_list=["k8"], is_wait=True)
    sql = open(DATA.binlog_sql_8, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data([DATA.expected_file_5, DATA.expected_file_6, DATA.expected_file_7], table_name, job_name, \
                'data load fail', client)
    #drop column
    client.schema_change(table_name, drop_column_list=["k7"], is_wait=True)
    sql = open(DATA.binlog_sql_11, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_9, table_name, job_name, 'data load fail', client)
    client.stop_sync_job(job_name)
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_mysql_primary_key():
    """
    {
    "title": "test_sys_binlog:test_mysql_promary_key",
    "describe": "创建binlog load任务，mysql操作primary索引表导入数据，验证支持binlog导入数据到doris",
    "tag": "p1,system"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    time.sleep(WAIT_TIME)
    sql = open(DATA.binlog_sql_4, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_3, table_name, job_name, 'data load fail', client)
    client.stop_sync_job(job_name)
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_mysql_unique_key():
    """
    {
    "title": "test_sys_binlog:test_mysql_unique_key",
    "describe": "创建binlog load任务，mysql操作unique索引表导入数据，验证支持binlog导入数据到doris",
    "tag": "p1,system"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1, 'UNIQUE (k1)')
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    time.sleep(WAIT_TIME)
    sql = open(DATA.binlog_sql_4, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_3, table_name, job_name, 'data load fail', client)
    client.stop_sync_job(job_name)
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_mysql_index_key():
    """
    {
    "title": "test_sys_binlog:test_mysql_index_key",
    "describe": "创建binlog load任务，mysql操作index索引表导入数据，验证支持binlog导入数据到doris",
    "tag": "p1,system"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1, 'INDEX (k1)')
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    time.sleep(WAIT_TIME)
    sql = open(DATA.binlog_sql_4, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_3, table_name, job_name, 'data load fail', client)
    client.stop_sync_job(job_name)
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_pause_resume():
    """
    {
    "title": "test_sys_binlog:test_pause_resume",
    "describe": "创建binlog load任务，验证暂停任务后重启任务成功",
    "tag": "p0,system"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    ret =  (client.get_sync_job_state(job_name) == 'RUNNING')
    assertStop(ret, client, job_name, 'sync job state error')
    time.sleep(WAIT_TIME)
    sql = open(DATA.binlog_sql_6, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_5, table_name, job_name, 'data load fail', client)
    #pause sync job
    ret = client.pause_sync_job(job_name)
    assertStop(ret, client, job_name, 'pause sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'PAUSED')
    assertStop(ret, client, job_name, 'sync job state error')
    sql = open(DATA.binlog_sql_7, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_5, table_name, job_name, 'data load fail', client)
    #resume sync job
    ret = client.resume_sync_job(job_name)
    assertStop(ret, client, job_name, 'resume sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'RUNNING')
    assertStop(ret, client, job_name, 'sync job state error')
    sql = open(DATA.binlog_sql_8, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    expected_file_list = [DATA.expected_file_5, DATA.expected_file_6, DATA.expected_file_7]
    check_data(expected_file_list, table_name, job_name, 'data load fail', client)
    client.stop_sync_job(job_name)
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_create_same_job():
    """
    {
    "title": "test_sys_binlog:test_create_same_job",
    "describe": "验证创建相同的binlog load任务失败",
    "tag": "p0,system,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    time.sleep(WAIT_TIME)
    ret = (client.get_sync_job_state(job_name) == 'RUNNING')
    assertStop(ret, client, job_name, 'sync job state error')
    util.assert_return(False, 'already exists', client.create_sync_job, table_name, database_name, mysql_table_name, \
            mysql_database_name, job_name, canal_ip, destination=destination)
    ret = (client.get_sync_job_state(job_name) == 'RUNNING')
    assertStop(ret, client, job_name, 'sync job state error')
    sql = open(DATA.binlog_sql_4, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_3, table_name, job_name, 'data load fail', client)
    client.stop_sync_job(job_name)
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_resume_after_create():
    """
    {
    "title": "test_sys_binlog:test_resume_after_create",
    "describe": "创建binlog load任务后，重启任务失败",
    "tag": "p1,system,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    time.sleep(WAIT_TIME)
    ret = (client.get_sync_job_state(job_name) == 'RUNNING')
    assertStop(ret, client, job_name, 'sync job state error')
    util.assert_return(False, 'There is no paused job', client.resume_sync_job, job_name)
    ret = client.get_sync_job_state(job_name) == 'RUNNING'
    assertStop(ret, client, job_name, 'sync job state error')
    sql = open(DATA.binlog_sql_4, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_3, table_name, job_name, 'data load fail', client)
    client.stop_sync_job(job_name)
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_pause_same_job():
    """
    {
    "title": "test_sys_binlog:test_pause_same_job",
    "describe": "创建binlog load任务，验证暂停同一个任务失败",
    "tag": "p1,system,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'RUNNING')
    assertStop(ret, client, job_name, 'sync job state error')
    ret = client.pause_sync_job(job_name)
    assertStop(ret, client, job_name, 'pause sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'PAUSED')
    assertStop(ret, client, job_name, 'sync job state error')
    util.assert_return(False, "There is no running job", client.pause_sync_job, job_name)
    ret = (client.get_sync_job_state(job_name) == 'PAUSED')
    assertStop(ret, client, job_name, 'sync job state error')
    sql = open(DATA.binlog_sql_6, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    time.sleep(WAIT_TIME)
    sql = "select * from %s" % table_name
    ret = (client.execute(sql) == ())
    assertStop(ret, client, job_name, 'query data error')
    client.stop_sync_job(job_name)
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_create_after_pause():
    """
    {
    "title": "test_sys_binlog:test_create_after_pause",
    "describe": "创建binlog load任务，暂停任务后再次创建该任务失败",
    "tag": "p1,system,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'RUNNING')
    assertStop(ret, client, job_name, 'sync job state error')
    ret = client.pause_sync_job(job_name)
    assertStop(ret, client, job_name, 'pause sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'PAUSED')
    assertStop(ret, client, job_name, 'sync job state error')
    util.assert_return(False, "already exists", client.create_sync_job, table_name, database_name, mysql_table_name, \
                    mysql_database_name, job_name, canal_ip)
    ret = (client.get_sync_job_state(job_name) == 'PAUSED')
    assertStop(ret, client, job_name, 'sync job state error')
    sql = open(DATA.binlog_sql_6, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    time.sleep(WAIT_TIME)
    sql = "select * from %s" % table_name
    ret = (client.execute(sql) == ())
    assertStop(ret, client, job_name, 'query data error')
    client.stop_sync_job(job_name)
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_resume_same_job():
    """
    {
    "title": "test_sys_binlog:test_resume_same_job",
    "describe": "创建binlog load任务，暂停任务，重启任务，验证再次重启失败",
    "tag": "p1,system,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'RUNNING')
    assertStop(ret, client, job_name, 'sync job state error')
    ret = client.pause_sync_job(job_name)
    assertStop(ret, client, job_name, 'pause sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'PAUSED')
    assertStop(ret, client, job_name, 'sync job state error')
    ret = client.resume_sync_job(job_name)
    assertStop(ret, client, job_name, 'resume sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'RUNNING')
    assertStop(ret, client, job_name, 'sync job state error')
    util.assert_return(False, "There is no paused job", client.resume_sync_job, job_name)
    ret = (client.get_sync_job_state(job_name) == 'RUNNING')
    assertStop(ret, client, job_name, 'sync job state error')
    time.sleep(WAIT_TIME)
    sql = open(DATA.binlog_sql_4, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_3, table_name, job_name, 'data load fail', client)
    client.stop_sync_job(job_name)
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_create_after_resume():
    """
    {
    "title": "test_sys_binlog:test_create_after_resume",
    "describe": "创建binlog load任务，暂停任务，重启任务，验证再次创建该任务失败",
    "tag": "p1,system,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'RUNNING')
    assertStop(ret, client, job_name, 'sync job state error')
    ret = client.pause_sync_job(job_name)
    assertStop(ret, client, job_name, 'pause sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'PAUSED')
    assertStop(ret, client, job_name, 'sync job state error')
    ret = client.resume_sync_job(job_name)
    assertStop(ret, client, job_name, 'resume sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'RUNNING')
    assertStop(ret, client, job_name, 'sync job state error')
    util.assert_return(False, "already exists", client.create_sync_job, table_name, database_name, mysql_table_name, \
            mysql_database_name, job_name, canal_ip, destination=destination)
    ret = (client.get_sync_job_state(job_name) == 'RUNNING')
    assertStop(ret, client, job_name, 'sync job state error')
    time.sleep(WAIT_TIME)
    sql = open(DATA.binlog_sql_4, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_3, table_name, job_name, 'data load fail', client)
    client.stop_sync_job(job_name)
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_pause_and_resume():
    """
    {
    "title": "test_sys_binlog:test_pause_and_resume",
    "describe": "创建binlog load任务，验证多次暂停、重启任务成功",
    "tag": "p1,system"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'RUNNING')
    assertStop(ret, client, job_name, 'sync job state error')
    count = 10
    while count > 0:
        ret = client.pause_sync_job(job_name)
        assertStop(ret, client, job_name, 'pause sync job failed')
        ret = (client.get_sync_job_state(job_name) == 'PAUSED')
        assertStop(ret, client, job_name, 'sync job state error')
        ret = client.resume_sync_job(job_name)
        assertStop(ret, client, job_name, 'resume sync job failed')
        ret = (client.get_sync_job_state(job_name) == 'RUNNING')
        assertStop(ret, client, job_name, 'sync job state error')
        time.sleep(1)
        count -= 1
    time.sleep(WAIT_TIME)
    sql = open(DATA.binlog_sql_4, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_3, table_name, job_name, 'data load fail', client)
    client.stop_sync_job(job_name)
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_sync_job_info():
    """
    {
    "title": "test_sys_binlog:test_sync_job_info",
    "describe": "创建binlog load任务，验证show sync job info",
    "tag": "p1,system"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    canal_port = '11111'
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    sync_job_list = client.show_sync_job()
    sync_channel = '%s.%s->%s' % (mysql_database_name, mysql_table_name, table_name)
    sync_job_config = 'address:%s:%s,destination:%s,batchSize:8192' % (canal_ip, canal_port, destination)
    for sync_job in sync_job_list:
        sync_job_info = palo_job.SyncJobInfo(sync_job)
        if sync_job_info.get_job_name() == job_name:
            ret = (sync_job_info.get_state() == 'RUNNING')
            assertStop(ret, client, job_name, 'sync job state error')
            ret = (sync_job_info.get_channel() == sync_channel)
            assertStop(ret, client, job_name, 'sync channel error')
            ret = (sync_job_info.get_job_config() == sync_job_config)
            assertStop(ret, client, job_name, 'sync job config error')
    client.stop_sync_job(job_name)
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_tables_in_one_job():
    """
    {
    "title": "test_sys_binlog:test_tables_in_one_job",
    "describe": "创建单个binlog load任务，任务中包括多对表的对应关系，mysql执行导入语句，验证数据导入doris成功",
    "tag": "p1,system"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    mysql_database_name = 'm_' + database_name + port
    table_names = []
    mysql_table_names = []
    mysql_database_names = [mysql_database_name for i in range(3)]
    for i in ['_1', '_2', '_3']:
        table_name_s = table_name + i
        client.create_table(table_name_s, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
        table_names.append(table_name_s)
        mysql_table_name = 'mysql_' + table_name_s
        if i == '_1':
            create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
        else:
            create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1, new_database=False)
        mysql_table_names.append(mysql_table_name)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_names, database_name, mysql_table_names, mysql_database_names, job_name, \
            canal_ip, destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'RUNNING')
    assertStop(ret, client, job_name, 'sync job state error')
    time.sleep(WAIT_TIME)
    sql = open(DATA.binlog_sql_4, 'r').read().format(mysql_table_names[0])
    mysql_execute(sql)
    sql = open(DATA.binlog_sql_4, 'r').read().format(mysql_table_names[1])
    mysql_execute(sql)
    sql = open(DATA.binlog_sql_1, 'r').read().format(mysql_table_names[2])
    mysql_execute(sql)
    time.sleep(WAIT_TIME)
    ret = (client.get_sync_job_state(job_name) == 'RUNNING')
    assertStop(ret, client, job_name, 'sync job state error')
    check_data(DATA.expected_file_3, table_names[0], job_name, 'data load fail', client)
    check_data(DATA.expected_file_3, table_names[1], job_name, 'data load fail', client)
    check_data(DATA.expected_file_1, table_names[2], job_name, 'data load fail', client)
    client.stop_sync_job(job_name)
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_tables_in_jobs():
    """
    {
    "title": "test_sys_binlog:test_tables_in_jobs",
    "describe": "创建多个binlog load任务，任务中包含多对表的对应关系，mysql执行导入语句，验证数据导入doris成功",
    "tag": "p1,system"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    mysql_database_name = 'm_' + database_name + port
    destination_list = [destination + '_1', destination + '_2', destination + '_3']
    count = 0
    job_name_list = []
    mysql_table_name_list = []
    table_name_list = []
    for i in ['_1', '_2', '_3']:
        table_name_s = table_name + i
        table_name_list.append(table_name_s)
        client.create_table(table_name_s, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
        mysql_table_name = 'mysql_' + table_name_s
        if i == '_1':
            create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
        else:
            create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1, new_database=False)
        mysql_table_name_list.append(mysql_table_name)
        job_name = 'job_' + table_name_s
        job_name_list.append(job_name)
        ret = client.create_sync_job(table_name_s, database_name, mysql_table_name, mysql_database_name, job_name, \
                canal_ip, destination=destination_list[count])
        assertStop(ret, client, job_name, 'create sync job failed')
        ret = (client.get_sync_job_state(job_name) == 'RUNNING')
        assertStop(ret, client, job_name, 'sync job state error')
        count += 1
    time.sleep(WAIT_TIME)
    sql = open(DATA.binlog_sql_4, 'r').read().format(mysql_table_name_list[0])
    mysql_execute(sql)
    sql = open(DATA.binlog_sql_4, 'r').read().format(mysql_table_name_list[1])
    mysql_execute(sql)
    sql = open(DATA.binlog_sql_1, 'r').read().format(mysql_table_name_list[2])
    mysql_execute(sql)
    for i in range(3):
        ret = (client.get_sync_job_state(job_name_list[i]) == 'RUNNING')
        assertStop(ret, client, job_name_list[i], 'sync job state error')
    check_data(DATA.expected_file_3, table_name_list[0], job_name_list[0], 'data load fail', client)
    check_data(DATA.expected_file_3, table_name_list[1], job_name_list[1], 'data load fail', client)
    check_data(DATA.expected_file_1, table_name_list[2], job_name_list[2], 'data load fail', client)
    for i in range(3):
        client.stop_sync_job(job_name_list[i])
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_stop_after_create():
    """
    {
    "title": "test_sys_binlog:test_stop_after_create",
    "describe": "创建binlog load任务，验证终止任务成功",
    "tag": "p0,system"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'RUNNING')
    assertStop(ret, client, job_name, 'sync job state error')
    time.sleep(WAIT_TIME)
    sql = open(DATA.binlog_sql_6, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_5, table_name, job_name, 'data load fail', client)
    #stop sync job
    ret = client.stop_sync_job(job_name)
    assertStop(ret, client, job_name, 'stop sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'CANCELLED')
    assertStop(ret, client, job_name, 'sync job state error')
    sql = open(DATA.binlog_sql_7, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    time.sleep(WAIT_TIME)
    ret = client.verify(DATA.expected_file_5, table_name)
    assertStop(ret, client, job_name, 'data load error')
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_stop_after_pause():
    """
    {
    "title": "test_sys_binlog:test_stop_after_pause",
    "describe": "创建binlog load任务，验证暂停任务后终止任务成功",
    "tag": "p0,system"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'RUNNING')
    assertStop(ret, client, job_name, 'sync job state error')
    time.sleep(WAIT_TIME)
    sql = open(DATA.binlog_sql_6, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_5, table_name, job_name, 'data load fail', client)
    #pause sync job
    ret = client.pause_sync_job(job_name)
    assertStop(ret, client, job_name, 'pause sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'PAUSED')
    assertStop(ret, client, job_name, 'sync job state error')
    sql = open(DATA.binlog_sql_7, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    time.sleep(WAIT_TIME)
    ret = client.verify(DATA.expected_file_5, table_name)
    assertStop(ret, client, job_name, 'data load error')
    #stop sync job
    ret = client.stop_sync_job(job_name)
    assertStop(ret, client, job_name, 'stop sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'CANCELLED')
    assertStop(ret, client, job_name, 'sync job state error')
    sql = open(DATA.binlog_sql_8, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    time.sleep(WAIT_TIME)
    ret = client.verify(DATA.expected_file_5, table_name)
    assertStop(ret, client, job_name, 'data load error')
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_stop_after_resume():
    """
    {
    "title": "test_sys_binlog:test_stop_after_resume",
    "describe": "创建binlog load任务，暂停任务，重启任务，验证终止任务成功",
    "tag": "p0,system"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'RUNNING')
    assertStop(ret, client, job_name, 'sync job state error')
    time.sleep(WAIT_TIME)
    sql = open(DATA.binlog_sql_6, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_5, table_name, job_name, 'data load fail', client)
    #pause sync job
    ret = client.pause_sync_job(job_name)
    assertStop(ret, client, job_name, 'pause sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'PAUSED')
    assertStop(ret, client, job_name, 'sync job state error')
    sql = open(DATA.binlog_sql_7, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    time.sleep(WAIT_TIME)
    ret = client.verify(DATA.expected_file_5, table_name)
    assertStop(ret, client, job_name, 'data load error')
    #resume sync job
    ret = client.resume_sync_job(job_name)
    assertStop(ret, client, job_name, 'resume sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'RUNNING')
    assertStop(ret, client, job_name, 'sync job state error')
    sql = open(DATA.binlog_sql_8, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    expected_file_list = [DATA.expected_file_5, DATA.expected_file_6, DATA.expected_file_7]
    check_data(expected_file_list, table_name, job_name, 'data load fail', client)
    #stop sync job
    ret = client.stop_sync_job(job_name)
    assertStop(ret, client, job_name, 'stop sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'CANCELLED')
    assertStop(ret, client, job_name, 'sync job state error')
    sql = open(DATA.binlog_sql_11, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    time.sleep(WAIT_TIME)
    ret = client.verify(expected_file_list, table_name)
    assertStop(ret, client, job_name, 'data load error')
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_stop_after_stop():
    """
    {
    "title": "test_sys_binlog:test_stop_after_stop",
    "describe": "创建binlog load任务，终止任务，验证再次终止任务失败",
    "tag": "p1,system,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'RUNNING')
    assertStop(ret, client, job_name, 'sync job state error')
    time.sleep(WAIT_TIME)
    sql = open(DATA.binlog_sql_6, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_5, table_name, job_name, 'data load fail', client)
    #stop sync job
    ret = client.stop_sync_job(job_name)
    assertStop(ret, client, job_name, 'stop sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'CANCELLED')
    assertStop(ret, client, job_name, 'sync job state error')
    sql = open(DATA.binlog_sql_7, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    time.sleep(WAIT_TIME)
    ret = client.verify(DATA.expected_file_5, table_name)
    assertStop(ret, client, job_name, 'data load error')
    #stop sync job again
    util.assert_return(False, 'There is no uncompleted job', client.stop_sync_job, job_name)
    ret = (client.get_sync_job_state(job_name) == 'CANCELLED')
    assertStop(ret, client, job_name, 'sync job state error')
    sql = open(DATA.binlog_sql_8, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    time.sleep(WAIT_TIME)
    ret = client.verify(DATA.expected_file_5, table_name)
    assertStop(ret, client, job_name, 'data load error')
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_create_same_job_after_stop():
    """
    {
    "title": "test_sys_binlog:test_create_same_job_after_stop",
    "describe": "创建binlog load任务，终止任务，验证创建不同名但表对应与之前相同的任务成功，数据能够继续导入",
    "tag": "p1,system"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'RUNNING')
    assertStop(ret, client, job_name, 'sync job state error')
    time.sleep(WAIT_TIME)
    sql = open(DATA.binlog_sql_6, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_5, table_name, job_name, 'data load fail', client)
    #stop sync job
    ret = client.stop_sync_job(job_name)
    assertStop(ret, client, job_name, 'stop sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'CANCELLED')
    assertStop(ret, client, job_name, 'sync job state error')
    sql = open(DATA.binlog_sql_7, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    time.sleep(WAIT_TIME)
    ret = client.verify(DATA.expected_file_5, table_name)
    assertStop(ret, client, job_name, 'data load error')
    #create same sync
    job_name_2 = job_name + '_2'
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name_2, \
            canal_ip, destination=destination)
    assertStop(ret, client, job_name_2, 'create sync job failed')
    ret = (client.get_sync_job_state(job_name_2) == 'RUNNING')
    assertStop(ret, client, job_name_2, 'sync job state error')
    time.sleep(WAIT_TIME)
    sql = open(DATA.binlog_sql_8, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    expected_file_list = [DATA.expected_file_5, DATA.expected_file_6, DATA.expected_file_7]
    check_data(expected_file_list, table_name, job_name_2, 'data load fail', client)
    client.stop_sync_job(job_name_2)
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_create_same_name_job_after_stop():
    """
    {
    "title": "test_sys_binlog:test_create_same_name_job_after_stop",
    "describe": "创建binlog load任务，终止任务，验证创建同名任务成功，执行mysql导入语句，验证doris数据导入正确",
    "tag": "p1,system"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'RUNNING')
    assertStop(ret, client, job_name, 'sync job state error')
    time.sleep(WAIT_TIME)
    sql = open(DATA.binlog_sql_6, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_5, table_name, job_name, 'data load fail', client)
    #stop sync job
    ret = client.stop_sync_job(job_name)
    assertStop(ret, client, job_name, 'stop sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'CANCELLED')
    assertStop(ret, client, job_name, 'sync job state error')
    sql = open(DATA.binlog_sql_7, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    time.sleep(WAIT_TIME)
    ret = client.verify(DATA.expected_file_5, table_name)
    assertStop(ret, client, job_name, 'data load error')
    #create same sync job
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, \
            canal_ip, destination=destination, is_wait=False)
    assertStop(ret, client, job_name, 'create sync job failed')
    time.sleep(WAIT_TIME)
    sql = open(DATA.binlog_sql_8, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    expected_file_list = [DATA.expected_file_5, DATA.expected_file_6, DATA.expected_file_7]
    check_data(expected_file_list, table_name, job_name, 'data load fail', client)
    client.stop_sync_job(job_name)
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_pause_after_stop():
    """
    {
    "title": "test_sys_binlog:test_pause_after_stop",
    "describe": "创建binlog load任务，终止任务，验证暂停任务失败",
    "tag": "p1,system,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'RUNNING')
    assertStop(ret, client, job_name, 'sync job state error')
    time.sleep(WAIT_TIME)
    sql = open(DATA.binlog_sql_6, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_5, table_name, job_name, 'data load fail', client)
    #stop sync job
    ret = client.stop_sync_job(job_name)
    assertStop(ret, client, job_name, 'stop sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'CANCELLED')
    assertStop(ret, client, job_name, 'sync job state error')
    sql = open(DATA.binlog_sql_7, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    time.sleep(WAIT_TIME)
    ret = client.verify(DATA.expected_file_5, table_name)
    assertStop(ret, client, job_name, 'data load error')
    #pause sync job
    util.assert_return(False, 'There is no running job', client.pause_sync_job, job_name)
    ret = (client.get_sync_job_state(job_name) == 'CANCELLED')
    assertStop(ret, client, job_name, 'sync job state error')
    sql = open(DATA.binlog_sql_8, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    time.sleep(WAIT_TIME)
    ret = client.verify(DATA.expected_file_5, table_name)
    assertStop(ret, client, job_name, 'data load error')
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_resume_after_stop():
    """
    {
    "title": "test_sys_binlog:test_resume_after_stop",
    "describe": "创建binlog load任务，终止任务，验证重启任务失败",
    "tag": "p1,system,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'RUNNING')
    assertStop(ret, client, job_name, 'sync job state error')
    time.sleep(WAIT_TIME)
    sql = open(DATA.binlog_sql_6, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_5, table_name, job_name, 'data load fail', client)
    #stop sync job
    ret = client.stop_sync_job(job_name)
    assertStop(ret, client, job_name, 'stop sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'CANCELLED')
    assertStop(ret, client, job_name, 'sync job state error')
    sql = open(DATA.binlog_sql_7, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    time.sleep(WAIT_TIME)
    ret = client.verify(DATA.expected_file_5, table_name)
    assertStop(ret, client, job_name, 'data load error')
    #resume sync job
    util.assert_return(False, 'There is no paused job', client.resume_sync_job, job_name)
    ret = (client.get_sync_job_state(job_name) == 'CANCELLED')
    assertStop(ret, client, job_name, 'sync job state error')
    sql = open(DATA.binlog_sql_8, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    time.sleep(WAIT_TIME)
    ret = client.verify(DATA.expected_file_5, table_name)
    assertStop(ret, client, job_name, 'data load error')
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_column():
    """
    {
    "title": "test_sys_binlog:test_column",
    "describe": "创建binlog load任务，指定列映射，mysql执行导入命令，验证doris数据导入",
    "tag": "p0,system"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_5, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    columns = ['k1', 'k6', 'k3', 'k4', 'k2', 'k7', 'k5']
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            columns=columns, destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'RUNNING')
    assertStop(ret, client, job_name, 'sync job state error')
    time.sleep(WAIT_TIME)
    sql = open(DATA.binlog_sql_4, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_10, table_name, job_name, 'data load fail', client)
    client.stop_sync_job(job_name)
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_add_nullable_column():
    """
    {
    "title": "test_sys_binlog:test_add_nullable_column",
    "describe": "创建binlog load任务，指定列映射，doris增加nullable列，mysql执行导入命令，验证数据导入到doris",
    "tag": "p1,system"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_5, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    columns = ['k1', 'k6', 'k3', 'k4', 'k2', 'k7', 'k5']
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            columns=columns, destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'RUNNING')
    assertStop(ret, client, job_name, 'sync job state error')
    time.sleep(WAIT_TIME)
    client.schema_change(table_name, add_column_list=["k8 int after k1"], is_wait=True)
    sql = open(DATA.binlog_sql_4, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_11, table_name, job_name, 'data load fail', client)
    client.stop_sync_job(job_name)
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_same_destination():
    """
    {
    "title": "test_sys_binlog:test_same_destination",
    "describe": "canal中每一个的destination仅对应一个binlog load任务，重复使用destination，验证创建binlog load任务失败",
    "tag": "p1,system,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    ret = client.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, canal_ip, \
            destination=destination)
    assertStop(ret, client, job_name, 'create sync job failed')
    ret = (client.get_sync_job_state(job_name) == 'RUNNING')
    assertStop(ret, client, job_name, 'sync job state error')
    #使用重复的destination创建binlog load任务失败
    table_name_s = table_name + '_s'
    mysql_table_name_s = mysql_table_name + '_s'
    client.create_table(table_name_s, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    create_mysql_table(mysql_table_name_s, mysql_database_name, DATA.column_1, new_database=False)
    job_name_s = job_name + '_s'
    util.assert_return(False, 'conflict destination', client.create_sync_job, table_name_s, database_name, \
                    mysql_table_name_s, mysql_database_name, job_name_s, canal_ip, destination=destination)
    client.stop_sync_job(job_name)
    client.clean(database_name)
    mysql_clean(mysql_database_name)


def test_observer_fe():
    """
    {
    "title": "test_sys_binlog:test_observer_fe",
    "describe": "连接observer fe，创建binlog load 任务，mysql执行导入语句，验证doris导入数据",
    "tag": "p1,system"
    }
    """
    client_observer = palo_client.get_client(config.fe_observer_list[0], config.fe_query_port, user=config.fe_user, \
                    password=config.fe_password, http_port=config.fe_http_port)
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client_observer.clean(database_name)
    client_observer.create_database(database_name)
    client_observer.use(database_name)
    client_observer.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    ret = client_observer.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, \
            canal_ip, destination=destination)
    assertStop(ret, client_observer, job_name, 'create sync job failed')
    time.sleep(WAIT_TIME)
    sql = open(DATA.binlog_sql_4, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_3, table_name, job_name, 'data load fail', client_observer)
    client_observer.stop_sync_job(job_name)
    client_observer.clean(database_name)
    mysql_clean(mysql_database_name)


def test_follower_fe():
    """
    {
    "title": "test_sys_binlog:test_follower_fe",
    "describe": "连接follower fe，创建binlog load 任务，mysql执行导入语句，验证doris导入数据",
    "tag": "p1,system"
    }
    """
    client_follower = palo_client.get_client(config.fe_follower_list[0], config.fe_query_port, user=config.fe_user, \
                    password=config.fe_password, http_port=config.fe_http_port)
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client_follower.clean(database_name)
    client_follower.create_database(database_name)
    client_follower.use(database_name)
    client_follower.create_table(table_name, DATA.column_1, set_null=True, keys_desc='UNIQUE KEY(k1)')
    mysql_database_name = 'm_' + database_name + port
    mysql_table_name = 'mysql_' + table_name
    create_mysql_table(mysql_table_name, mysql_database_name, DATA.column_1)
    job_name = 'job_' + table_name
    ret = client_follower.create_sync_job(table_name, database_name, mysql_table_name, mysql_database_name, job_name, \
            canal_ip, destination=destination)
    assertStop(ret, client_follower, job_name, 'create sync job failed')
    time.sleep(WAIT_TIME)
    sql = open(DATA.binlog_sql_4, 'r').read().format(mysql_table_name)
    mysql_execute(sql)
    check_data(DATA.expected_file_3, table_name, job_name, 'data load fail', client_follower)
    client_follower.stop_sync_job(job_name)
    client_follower.clean(database_name)
    mysql_clean(mysql_database_name)


def teardown_module():
    """
    tearDown
    """
    connect.close()
