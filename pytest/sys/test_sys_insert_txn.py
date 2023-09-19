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
test insert into value txn
Date: 2021-01-07 14:54:38
"""

import time
import sys
import random
import pytest
sys.path.append("../")
from lib import palo_client
from lib import palo_config
from lib import util
from lib import common
from data import schema as DATA

client = None
config = palo_config.config
LOG = palo_client.LOG
L = palo_client.L


def setup_module():
    """
    set up
    """
    pass


def test_txn_basic():
    """
    {
    "title": "test_txn_basic",
    "describe": "事务基本测试，commit & rollback",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.int_column_list)
    assert ret, 'create table failed'
    # begin insert commit check
    assert client.begin(), 'begin failed'
    sql = 'insert into %s.%s values(0, 1, 1, 1, 1)' % (database_name, table_name)
    assert client.execute(sql) == (), 'insert failed'
    assert client.commit(), 'commit failed'
    sql1 = 'select * from %s.%s' % (database_name, table_name)
    sql2 = 'select 0, 1, 1, 1, 1'
    common.check2(client, sql1=sql1, sql2=sql2)
    # begin insert rollback check
    assert client.begin(), 'begin failed'
    sql = 'insert into %s.%s values(1, 1, 1, 1, 1)' % (database_name, table_name)
    assert client.execute(sql) == (), 'insert failed'
    assert client.rollback(), 'rollback failed'
    sql1 = 'select * from %s.%s' % (database_name, table_name)
    sql2 = 'select 0, 1, 1, 1, 1'
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_txn_commit():
    """
    {
    "title": "test_txn_commit",
    "describe": "多次连续执行commit，状态正确",
    "tag": "fuzz,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.int_column_list)
    assert ret, 'create table failed'
    # begin insert commit check
    assert client.begin(), 'begin failed'
    sql = 'insert into %s.%s values(0, 1, 1, 1, 1)' % (database_name, table_name)
    assert client.execute(sql) == (), 'insert failed'
    assert client.commit(), 'commit failed'
    sql1 = 'select * from %s.%s' % (database_name, table_name)
    sql2 = 'select 0, 1, 1, 1, 1'
    common.check2(client, sql1=sql1, sql2=sql2)
    # commit again & check
    retry = 10
    while retry > 0:
        assert client.commit(), 'commit failed'
        common.check2(client, sql1=sql1, sql2=sql2)
        retry -= 1
        time.sleep(1)
    # new txn begin insert rollback check
    assert client.begin(), 'begin failed'
    sql = 'insert into %s.%s values(1, 1, 1, 1, 1)' % (database_name, table_name)
    assert client.execute(sql) == (), 'insert failed'
    assert client.commit(), 'commit failed'
    sql1 = 'select * from %s.%s' % (database_name, table_name)
    sql2 = 'select 0, 1, 1, 1, 1 union select 1, 1, 1, 1, 1'
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    client.clean(database_name)


def test_txn_rollback():
    """
    {
    "title": "test_txn_rollback",
    "describe": "多次连续执行rollback，状态正确",
    "tag": "fuzz,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.int_column_list)
    assert ret, 'create table failed'
    # begin insert rollback check
    assert client.begin(), 'begin failed'
    sql = 'insert into %s.%s values(0, 1, 1, 1, 1)' % (database_name, table_name)
    assert client.execute(sql) == (), 'insert failed'
    assert client.rollback(), 'rollback failed'
    sql1 = 'select * from %s.%s' % (database_name, table_name)
    ret = client.execute(sql1)
    assert len(ret) == 0, 'check data failed'
    # retry rollback
    retry = 10
    while retry > 0:
        client.rollback()
        sql1 = 'select * from %s.%s' % (database_name, table_name)
        ret = client.execute(sql1)
        assert len(ret) == 0, 'check data failed'
        retry -= 1
        time.sleep(1)
    # new txn begin insert commit check
    assert client.begin(), 'begin failed'
    sql = 'insert into %s.%s values(1, 1, 1, 1, 1)' % (database_name, table_name)
    assert client.execute(sql) == (), 'insert failed'
    assert client.commit(), 'commit failed'
    sql1 = 'select * from %s.%s' % (database_name, table_name)
    sql2 = 'select 1, 1, 1, 1, 1'
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_txn_begin():
    """
    {
    "title": "test_txn_begin",
    "describe": "多次连续执行begin，状态正确",
    "tag": "fuzz,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.int_column_list)
    assert ret, 'create table failed'
    # begin insert commit check
    assert client.begin(), 'begin failed'
    sql = 'insert into %s.%s values(0, 1, 1, 1, 1)' % (database_name, table_name)
    assert client.execute(sql) == (), 'insert failed'
    assert client.commit(), 'commit failed'
    sql1 = 'select * from %s.%s' % (database_name, table_name)
    sql2 = 'select 0, 1, 1, 1, 1'
    common.check2(client, sql1=sql1, sql2=sql2)
    # begin again & check
    retry = 10
    while retry > 0:
        assert client.begin(), 'begin failed'
        retry -= 1
        time.sleep(1)
    # new txn begin insert commit check
    assert client.begin(), 'begin failed'
    sql = 'insert into %s.%s values(1, 1, 1, 1, 1)' % (database_name, table_name)
    assert client.execute(sql) == (), 'insert failed'
    assert client.commit(), 'commit failed'
    sql1 = 'select * from %s.%s' % (database_name, table_name)
    sql2 = 'select 0, 1, 1, 1, 1 union select 1, 1, 1, 1, 1'
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    client.clean(database_name)


def test_empty_txn():
    """
    {
    "title": "test_empty_txn",
    "describe": "begin后直接执行rollback/commit，状态正确",
    "tag": "fuzz,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.int_column_list)
    assert ret, 'create table failed'
    assert client.begin(), 'begin failed'
    assert client.commit(), 'commit failed'
    assert client.begin(), 'begin failed'
    assert client.rollback(), 'rollback failed'
    ret = client.select_all(table_name)
    assert ret == (), 'check data error'
    client.clean(database_name)


def test_txn_not_support():
    """
    {
    "title": "test_txn_not_support",
    "describe": "事务中不支持执行select，delete，show，alter，load等操作",
    "tag": "fuzz,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.int_column_list)
    assert ret, 'create table failed'
    # begin insert commit check
    assert client.begin(), 'begin failed'
    sql = 'insert into %s.%s values(0, 1, 1, 1, 1)' % (database_name, table_name)
    assert client.execute(sql) == (), 'insert failed'
    sql = 'select * from %s.%s' % (database_name, table_name)
    msg = 'This is in a transaction, only insert, commit, rollback is acceptable'
    util.assert_return(False, msg, client.execute, sql)
    util.assert_return(False, msg, client.delete, table_name, [("k1", "=", "0")])
    sql = 'show tables'
    util.assert_return(False, msg, client.execute, sql)
    sql = 'insert into %s.%s values(0, 1, 1, 1, 1)'
    msg = ' '
    util.assert_return(False, msg, client.execute, sql)
    sql = 'insert into %s.%s values(0, 1, 2, 3)'
    util.assert_return(False, msg, client.execute, sql)
    sql = ''
    assert client.commit(), 'commit failed'
    sql1 = 'select * from %s.%s' % (database_name, table_name)
    sql2 = 'select 0, 1, 1, 1, 1'
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    client.clean(database_name)


def test_txn_db_table():
    """
    {
    "title": "test_txn_db_table",
    "describe": "不执行use db，直接执行insert",
    "tag": "fuzz,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.get_client()
    client.clean(database_name)
    assert client.create_database(database_name), 'create database failed'
    ret = client.create_table(table_name, DATA.int_column_list, database_name=database_name)
    assert ret, 'create table failed'
    assert client.begin(), 'begin failed'
    sql = 'insert into %s.%s values(0, 1, 1, 1, 1)' % (database_name, table_name)
    assert client.execute(sql) == (), 'insert failed'
    assert client.commit(), 'commit failed'
    sql1 = 'select * from %s.%s' % (database_name, table_name)
    sql2 = 'select 0, 1, 1, 1, 1'
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_txn_insert_fail():
    """
    {
    "title": "test_txn_insert_fail",
    "describe": "事务中，有的insert成功，有的insert失败，事务提交正确，状态正确",
    "tag": "fuzz,p1"
    }
    """
    """
    当事务中，有的insert成功，有的insert失败，事务提交正确，状态正确
    not Null列插入Null值，无报错信息，Null值未插入
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    assert client.set_variables('enable_insert_strict', 'false')
    ret = client.create_table(table_name, DATA.int_column_list, set_null=False)
    assert ret, 'create table failed'

    assert client.begin(), 'begin failed'
    sql = 'insert into %s.%s values(1, null, null, null, null),' \
          '(1, 1, 1, 1, 1)' % (database_name, table_name)
    assert client.execute(sql) == ()
    #msg = 'Column cat not be null, rowIdx = 0, column = v1'
    #util.assert_return(False, msg, client.execute, sql)
    assert client.commit(), 'commit failed'
    sql1 = 'select * from %s.%s' % (database_name, table_name)
    ret = client.execute(sql1)
    assert len(ret) == 1, 'check data error'
    assert client.begin(), 'begin failed'
    sql = 'insert into %s.%s values(null, 1, 1, 1, 1)' % (database_name, table_name)
    assert client.execute(sql) == (), 'insert failed'
    #msg = 'Column cat not be null, rowIdx = 0, column = k1'
    #util.assert_return(False, msg, client.execute, sql)
    sql = 'insert into %s.%s values(1, 1, 1, 1, 1)' % (database_name, table_name)
    assert client.execute(sql) == (), 'insert failed'
    client.commit()
    sql1 = 'select * from %s.%s' % (database_name, table_name)
    sql2 = 'select 1, 2, 1, 1, 1'
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_txn_insert_fail_1():
    """
    {
    "title": "test_txn_insert_fail",
    "describe": "事务中，有的insert成功，有的insert失败，事务提交正确，状态正确",
    "tag": "fuzz,p1"
    }
    """
    """
    当事务中，有的insert成功，有的insert失败，事务提交正确，状态正确
    not Null列插入Null值，无报错信息，Null值未插入
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    assert client.set_variables('enable_insert_strict', 'true')
    ret = client.create_table(table_name, DATA.int_column_list, set_null=False)
    assert ret, 'create table failed'

    assert client.begin(), 'begin failed'
    sql = 'insert into %s.%s values(1, null, null, null, null),' \
          '(1, 1, 1, 1, 1)' % (database_name, table_name)
    ret = client.execute(sql)
    assert client.execute(sql) == (), 'insert failed'
    msg = 'too many filtered rows'
    util.assert_return(False, msg, client.commit)
    sql1 = 'select * from %s.%s' % (database_name, table_name)
    ret = client.execute(sql1)
    assert len(ret) == 0, 'check data error'
    assert client.begin(), 'begin failed'
    sql = 'insert into %s.%s values(null, 1, 1, 1, 1)' % (database_name, table_name)
    assert client.execute(sql) == (), 'insert failed'
    sql = 'insert into %s.%s values(1, 1, 1, 1, 1)' % (database_name, table_name)
    assert client.execute(sql) == (), 'insert failed'
    msg = 'too many filtered rows'
    util.assert_return(False, msg, client.commit)
    sql1 = 'select * from %s.%s' % (database_name, table_name)
    ret = client.execute(sql1)
    assert len(ret) == 0, 'check data error'
    client.clean(database_name)


def test_txn_insert_multi_table():
    """
    {
    "title": "test_txn_insert_multi_table",
    "describe": "一个事务中，向多个表插入值",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    tb1 = table_name + '_1'
    tb2 = table_name + '_2'
    ret = client.create_table(tb1, DATA.int_column_list, set_null=False)
    assert ret, 'create table failed'
    ret = client.create_table(tb2, DATA.int_column_list, set_null=False)
    assert ret, 'create table failed'
    assert client.begin(), 'begin failed'
    sql = 'insert into %s.%s values(0, 1, 1, 1, 1)' % (database_name, tb1)
    assert client.execute(sql) == (), 'insert failed'
    sql = 'insert into %s.%s values(0, 1, 1, 1, 1)' % (database_name, tb2)
    msg = 'Only one table can be inserted in one transaction.'
    util.assert_return(False, msg, client.execute, sql)
    assert client.commit(), 'commit failed'
    sql1 = 'select * from %s.%s'
    sql2 = 'select 0, 1, 1, 1, 1'
    common.check2(client, sql1=sql1 % (database_name, tb1), sql2=sql2)
    ret = client.execute(sql1 % (database_name, tb2))
    print(ret)
    assert len(ret) == 0, 'check data failed'
    client.clean(database_name)


def test_txn_multi_con():
    """
    {
    "title": "test_txn_multi_con",
    "describe": "多个连接同时执行事务，结果正确",
    "tag": "function,p1"
    }
    """
    """多个连接同时执行事务，结果正确, unique表"""
    database_name, table_name, index_name = util.gen_name_list()
    db1 = database_name + '_1'
    db2 = database_name + '_2'
    client1 = common.create_workspace(db1)
    client2 = common.create_workspace(db2)
    assert client1.create_table(table_name, DATA.int_column_list), 'create table failed'
    assert client2.create_table(table_name, DATA.int_column_list), 'create table failed'
    assert client1.begin(), 'begin failed'
    sql = 'insert into %s.%s values(0, 1, 1, 1, 1), (1, 1, 1, 1, 1)'
    assert client1.execute(sql % (db1, table_name)) == (), 'insert failed'
    assert client2.begin(), 'begin failed'
    assert client2.execute(sql % (db2, table_name)) == (), 'insert failed'
    assert client2.commit(), 'commit failed'
    assert client1.commit(), 'commit failed'
    sql1 = 'select * from %s.%s order by k1'
    sql2 = 'select 0, 1, 1, 1, 1 union select 1, 1, 1, 1, 1'
    common.check2(client1, sql1=sql1 % (db1, table_name), sql2=sql2, forced=True)
    common.check2(client2, sql1=sql1 % (db2, table_name), sql2=sql2, forced=True)
    client1.clean(db1)
    client2.clean(db2)


def test_txn_multi_con_1():
    """
    {
    "title": "test_txn_multi_con_1",
    "describe": "多个事务并发执行，插入同一张表",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    assert client.create_table(table_name, DATA.int_column_no_agg_list), 'create table failed'
    client2 = common.get_client()
    client2.use(database_name)
    assert client.begin(), 'begin failed'
    sql = 'insert into %s.%s values(0, 1, 1, 1, 1), (1, 1, 1, 1, 1)' % (database_name, table_name)
    assert client.execute(sql) == (), 'insert failed'
    assert client2.begin(), 'begin failed'
    assert client2.execute(sql) == (), 'insert failed'
    assert client.commit(), 'commit failed'
    assert client2.commit(), 'commit failed'
    sql = 'select * from %s.%s order by k1' % (database_name, table_name)
    ret = client.execute(sql)
    ret2 = ((0, 1, 1, 1, 1), (0, 1, 1, 1, 1), (1, 1, 1, 1, 1), (1, 1, 1, 1, 1))
    util.check(ret, ret2)
    client.clean(database_name)


def test_txn_multi_fe():
    """
    {
    "title": "test_txn_multi_fe",
    "describe": "不同fe上执行事务，预期均执行成功，状态正确",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    other_fe = random.choice(client.get_fe_host())
    assert client.create_table(table_name, DATA.int_column_no_agg_list), 'create table failed'
    client2 = palo_client.get_client(other_fe, config.fe_query_port, user=config.fe_user,
                                     password=config.fe_password, http_port=config.fe_http_port)
    client2.use(database_name)
    assert client.begin(), 'begin failed'
    sql = 'insert into %s.%s values(0, 1, 1, 1, 1), (1, 1, 1, 1, 1)' % (database_name, table_name)
    assert client.execute(sql) == (), 'insert failed'
    assert client2.begin(), 'begin failed'
    assert client2.execute(sql) == (), 'insert failed'
    assert client.commit(), 'commit failed'
    assert client2.commit(), 'commit failed'
    sql = 'select * from %s.%s order by k1' % (database_name, table_name)
    ret = client.execute(sql)
    ret2 = ((0, 1, 1, 1, 1), (0, 1, 1, 1, 1), (1, 1, 1, 1, 1), (1, 1, 1, 1, 1))
    util.check(ret, ret2)
    client.clean(database_name)


def test_txn_strict():
    """
    {
    "title": "test_txn_strict",
    "describe": "insert的数据不符合规范，对事务正确执行",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    assert client.create_table(table_name, DATA.int_column_no_agg_list), 'create table failed'
    assert client.begin(), 'begin failed'
    sql = 'insert into %s.%s values("a", "b", "1", "1", "1")' % (database_name, table_name)
    msg = 'Invalid number format: a'
    util.assert_return(False, msg, client.execute, sql)
    sql = 'insert into %s.%s values("a", "b", "1", "1", "1"), (1, 1, 1, 1, 1)' % (database_name, table_name)
    msg = 'Invalid number format: a'
    util.assert_return(False, msg, client.execute, sql)
    sql = 'insert into %s.%s values("0", "1", "1", "1", "1")' % (database_name, table_name)
    assert client.execute(sql) == (), 'insert failed'
    assert client.commit(), 'commit failed'
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select 0, 1, 1, 1, 1'
    common.check2(client, sql1=sql1, sql2=sql2)
    assert client.begin(), 'begin failed'
    sql = 'insert into %s.%s values(2147483649, 1, 1, 1, 1)' % (database_name, table_name)
    msg = 'Number out of range[2147483649]. type: INT'
    util.assert_return(False, msg, client.execute, sql)
    client.commit()
    client.clean(database_name)


def test_txn_drop_db():
    """
    {
    "title": "test_txn_drop_db",
    "describe": "在事务提交前，drop db，状态正常",
    "tag": "fuzz,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    assert client.create_table(table_name, DATA.int_column_no_agg_list), 'create table failed'
    client2 = common.get_client()
    client2.use(database_name)
    assert client.begin(), 'begin failed'
    sql = 'insert into %s.%s values(0, 1, 1, 1, 1), (1, 1, 1, 1, 1)' % (database_name, table_name)
    assert client.execute(sql) == (), 'insert failed'
    assert client2.drop_database(database_name), 'drop db failed'
    msg = 'Unknown database'
    util.assert_return(False, msg, client.commit)
    sql = 'select * from test_query_qa.baseall order by k1'
    client.execute(sql)
    client.begin()
    util.assert_return(True, ' ', client.commit)
    util.assert_return(True, ' ', client.rollback)
    client.clean(database_name)


def test_txn_drop_tb():
    """
    {
    "title": "test_txn_drop_tb",
    "describe": "在事务提交前，drop tb",
    "tag": "fuzz,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    assert client.create_table(table_name, DATA.int_column_no_agg_list), 'create table failed'
    client2 = common.get_client()
    client2.use(database_name)
    assert client.begin(), 'begin failed'
    sql = 'insert into %s.%s values(0, 1, 1, 1, 1), (1, 1, 1, 1, 1)' % (database_name, table_name)
    assert client.execute(sql) == (), 'insert failed'
    assert client2.drop_table(table_name), 'drop table failed'
    msg = 'unknown table, tableName='
    util.assert_return(False, msg, client.commit)
    sql = 'select * from test_query_qa.baseall order by k1'
    assert client.execute(sql), 'select failed'
    client.clean(database_name)


def test_txn_insert_select():
    """
    {
    "title": "test_txn_insert_select",
    "describe": "在事务中，不支持insert select操作",
    "tag": "fuzz,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    assert client.create_table(table_name, DATA.int_column_no_agg_list), 'create table failed'
    assert client.begin(), 'begin failed'
    sql = 'insert into %s.%s select k1, k1+1, k1+2, k1+3, k1+4 from %s.baseall' % \
          (database_name, table_name, 'test_query_qa')
    msg = 'Insert into ** select is not supported in a transaction'
    util.assert_return(False, msg, client.execute, sql)
    assert client.commit(), 'commit failed'
    client.clean(database_name)


@pytest.mark.skip()
def test_txn_timeout():
    """
    skip: txn timeout change to 14400000ms
    {
    "title": "test_txn_timeout",
    "describe": "在事务中，insert超时失败",
    "tag": "fuzz,p2"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    assert client.create_table(table_name, DATA.int_column_no_agg_list), 'create table failed'
    assert client.begin(), 'begin failed'
    sql = 'insert into %s.%s values(1, 2, 3, 4, 5)' % (database_name, table_name)
    assert client.execute(sql) == (), 'insert failed'
    time.sleep(605)
    print('sleep 605')
    sql = 'insert into %s.%s values(2, 2, 3, 4, 5)' % (database_name, table_name)
    assert client.execute(sql) == (), 'insert failed'
    # msg = 'commit failed, rollback.'
    msg = 'timeout by txn manager'
    util.assert_return(False, msg, client.commit)
    client.select_all(table_name)
    client.rollback()
    client.clean(database_name)


def test_txn_after_batch_delete():
    """
    {
    "title": "test_txn_after_batch_delete",
    "describe": "开启batch delete后，执行insert事务",
    "tag": "fuzz,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    assert client.create_table(table_name, DATA.int_column_no_agg_list, keys_desc=DATA.unique_key)
    assert client.begin(), 'begin failed'
    sql = 'insert into %s.%s values(1, 2, 3, 4, 5)' % (database_name, table_name)
    assert client.execute(sql) == (), 'insert failed'
    assert client.commit()
    ret = client.select_all(table_name)
    assert ret == ((1, 2, 3, 4, 5),)
    client.clean(database_name)


def gen_values(file):
    """
    get values from file
    """
    value_list = list()
    with open(file) as f:
        line = f.readline()
        while line:
            value_list.append('(%s)' % line.replace('\t', ','))
            line = f.readline()
    return value_list


def test_txn_multi_insert():
    """
    {
    "title": "test_txn_multi_insert",
    "describe": "在事务中，执行多个insert",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    client = common.create_workspace(database_name)
    assert client.create_table(table_name, DATA.int_column_no_agg_list), 'create table failed'
    assert client.begin(), 'begin error'
    # 读取文件，生成相应insert sql
    file = './data/SMALL_LOAD_SIMPLE/expe_test_hash_int.data'
    values = gen_values(file)
    for v in values:
        sql = 'insert into %s.%s values %s' % (database_name, table_name, v)
        try:
            client.execute(sql)
        except Exception as e:
            print(values.index(v))
            print(e)
            LOG.info(L('insert failed', msg=str(e)))
    try:
        assert client.commit(), 'commit error'
    except Exception as e:
        print(str(e))
    expect_file = './data/SMALL_LOAD_SIMPLE/expe_test_hash_int.data'
    assert client.verify(expect_file, table_name), 'data check error'
    assert client.truncate(table_name), 'truncate table failed'
    assert client.begin(), 'begin error'
    sql =  'insert into %s.%s values %s' % (database_name, table_name, ','.join(values))
    client.execute(sql)
    assert client.commit(), 'commit error'
    assert client.verify(expect_file, table_name), 'data check error'
    client.clean(database_name,)


if __name__ == '__main__':
    test_txn_insert_fail()
