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
测试sys view相关
"""

import sys

sys.path.append("../")
from lib import palo_config
from lib import palo_client
from lib import util
from lib import palo_logger
from lib import common
from data import schema as DATA

client = None
LOG = palo_logger.Logger.getLogger()
L = palo_logger.StructedLogMessage

config = palo_config.config
baseall_tb = 'test_query_qa.baseall'
qe_db = 'test_query_qa'
qe_base_tb = 'baseall'


def setup_module():
    """
    setUp
    """
    global client
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password)
    client.init()


def execute(line):
    """execte sql"""
    print(line)
    palo_result = client.execute(line)
    print(palo_result)
    return palo_result


def check2_palo(line1, line2):
    """
    check2_palo
    :param ret1:
    :param ret2:
    :return:
    """
    ret1 = execute(line1)
    ret2 = execute(line2)
    util.check(ret1, ret2)


def check_column_list(tb, db, expected_column):
    """
    check_column_list
    验证desc中的column是不是与期望的一致
    :param tb:
    :param db:
    :param expected_column:
    :return:
    """
    column_list = client.get_all_columns(tb, db)
    assert len(column_list) == len(expected_column)
    for column in column_list:
        assert column in expected_column


def restart_palo_and_reconnect():
    """
    todo
    restart_palo_and_reconnect
    重启操作暂时不可用，所以先不放在自动化case里
    fe的持久化测试分为image和journal加载两种形式，暂时是手工执行
    :return:
    """
    pass


def test_create_and_alter_view():
    """
    {
    "title": "test_sys_view.test_create_and_alter_view",
    "describe": "test_create_and_alter_view",
    "tag": "function,p1,fuzz"
    }
    """
    """test_create_and_alter_view"""
    db_name, table_name, invalied_name_1 = util.gen_name_list()
    line = 'DROP DATABASE IF EXISTS %s' % db_name
    client.execute(line)
    client.create_database(db_name)
    client.use(db_name)

    view_name = 'baseall_view'
    view_name2 = 'baseall_view_2'

    # case: view中指定数据库，使用另一个数据库
    line = 'drop view if exists %s' % view_name
    execute(line)
    line = 'create view %s as (select k1,k2 from %s)' % (view_name, baseall_tb)
    execute(line)
    check_column_list(view_name, db_name, ['k1', 'k2'])

    line = 'drop view if exists %s' % view_name2
    execute(line)
    line = 'create view %s as (select k1,k2 from %s)' % (view_name2, view_name)
    execute(line)
    check_column_list(view_name2, db_name, ['k1', 'k2'])

    line = 'alter view %s as (select k1,k2,k3 from %s)' % (view_name2, view_name)
    util.assert_return(False, 'Unknown column \'k3\' in \'table list\'',
                       execute, line)

    line1 = 'select * from %s order by k1' % view_name
    line2 = 'select k1,k2 from %s order by k1' % baseall_tb
    check2_palo(line1, line2)

    line = 'alter view %s as (select k1 from %s)' % (view_name, baseall_tb)
    execute(line)
    check_column_list(view_name, db_name, ['k1'])
    line = 'alter view %s as (select k1 from %s)' % (view_name2, view_name)
    execute(line)
    check_column_list(view_name2, db_name, ['k1'])
    line = 'alter view %s as (select k1,k2 from %s)' % (view_name2, view_name)
    util.assert_return(False, 'Unknown column \'k2\' in \'table list\'', execute, line)

    line1 = 'select * from %s order by k1' % view_name
    line2 = 'select k1 from %s order by k1' % baseall_tb
    check2_palo(line1, line2)

    # case: view中不指定数据库，使用当前数据库的其他表
    client.use(qe_db)
    line = 'drop view if exists %s' % view_name
    execute(line)
    line = 'create view %s as (select k1,k2 from %s)' % (view_name, qe_base_tb)
    execute(line)
    check_column_list(view_name, qe_db, ['k1', 'k2'])

    line1 = 'select * from %s order by k1' % view_name
    line2 = 'select k1,k2 from %s order by k1' % qe_base_tb
    check2_palo(line1, line2)

    line = 'alter view %s as (select k1 from %s)' % (view_name, qe_base_tb)
    execute(line)
    check_column_list(view_name, qe_db, ['k1'])

    line1 = 'select * from %s order by k1' % view_name
    line2 = 'select k1 from %s order by k1' % qe_base_tb
    check2_palo(line1, line2)

    # restart_palo_and_reconnect()

    check2_palo(line1, line2)
    check_column_list(view_name, db_name, ['k1'])

    check2_palo(line1, line2)
    check_column_list(view_name, qe_db, ['k1'])

    line = 'drop view if exists {0}.{1}'.format(qe_db, view_name)
    execute(line)

    client.clean(db_name)


def test_create_and_alter_view_with_cte():
    """
    {
    "title": "test_create_and_alter_view_with_cte",
    "describe": "test_create_and_alter_view_with_cte",
    "tag": "function,p1"
    }
    """
    """test_create_and_alter_view_with_cte"""
    db_name, table_name, invalied_name_1 = util.gen_name_list()
    line = 'DROP DATABASE IF EXISTS %s' % db_name
    client.execute(line)
    client.create_database(db_name)

    view_name = 'baseall_view'
    cte_tb_name = 'test_baseall_cte'

    # case: 测试没有指定db情况下，使用cte语句建view的情况
    # 目前palo client一定会默认指定一个数据库连接，所以新建一个数据库再drop，来模拟没有指定db连接的情况
    line = 'create database tmp_db'
    execute(line)
    line = 'use tmp_db'
    execute(line)
    line = 'drop database tmp_db'
    execute(line)

    line = 'create view {0}.{1} as with {2} as (select k1,k2 from {3}) select k1,k2 from {2}'.format(
        db_name, view_name, cte_tb_name, baseall_tb)
    execute(line)

    line1 = 'select * from {0}.{1} order by k1'.format(db_name, view_name)
    line2 = 'select k1,k2 from {0} order by k1'.format(baseall_tb)
    check2_palo(line1, line2)
    check_column_list(view_name, db_name, ['k1', 'k2'])

    # restart_palo_and_reconnect()
    check2_palo(line1, line2)
    check_column_list(view_name, db_name, ['k1', 'k2'])

    line = 'alter view {0}.{1} as (select k1 from {2})'.format(db_name, view_name, baseall_tb)
    execute(line)
    check_column_list(view_name, db_name, ['k1'])

    line1 = 'select * from {0}.{1} order by k1'.format(db_name, view_name)
    line2 = 'select k1 from {0} order by k1'.format(baseall_tb)
    check2_palo(line1, line2)

    # restart_palo_and_reconnect()

    check2_palo(line1, line2)
    check_column_list(view_name, db_name, ['k1'])

    # case: 测试指定db情况下，使用cte语句建view的情况: cte中指定另一个数据库
    client.use(db_name)
    line = 'drop view if exists %s' % view_name
    execute(line)
    line = 'create view {0} as with {1} as (select k1,k2 from {2}) select k1,k2 from {1};'.format(
        view_name, cte_tb_name, baseall_tb)
    execute(line)

    line1 = 'select * from %s order by k1' % view_name
    line2 = 'select k1,k2 from %s order by k1' % baseall_tb
    check2_palo(line1, line2)
    sql = 'select database()'
    execute(sql)
    check_column_list(view_name, db_name, ['k1', 'k2'])

    line = 'alter view %s as (select k1 from %s)' % (view_name, baseall_tb)
    execute(line)
    check_column_list(view_name, db_name, ['k1'])

    line1 = 'select * from %s order by k1' % view_name
    line2 = 'select k1 from %s order by k1' % baseall_tb
    check2_palo(line1, line2)

    # restart_palo_and_reconnect()

    check2_palo(line1, line2)
    check_column_list(view_name, db_name, ['k1'])

    # case: 测试指定db情况下，使用cte语句建view的情况: cte中不指定数据库，使用当前数据库
    client.use(qe_db)
    line = 'drop view if exists %s' % view_name
    execute(line)
    line = 'create view {0} as with {1} as (select k1,k2 from {2}) select k1,k2 from {1};'.format(
        view_name, cte_tb_name, qe_base_tb)
    execute(line)

    line1 = 'select * from %s order by k1' % view_name
    line2 = 'select k1,k2 from %s order by k1' % qe_base_tb
    check2_palo(line1, line2)
    sql = 'select database()'
    execute(sql)
    check_column_list(view_name, qe_db, ['k1', 'k2'])

    line = 'alter view %s as (select k1 from %s)' % (view_name, qe_base_tb)
    execute(line)
    check_column_list(view_name, qe_db, ['k1'])

    line1 = 'select * from %s order by k1' % view_name
    line2 = 'select k1 from %s order by k1' % qe_base_tb
    check2_palo(line1, line2)

    # restart_palo_and_reconnect()

    check2_palo(line1, line2)
    check_column_list(view_name, qe_db, ['k1'])

    line = 'drop view if exists {0}.{1}'.format(qe_db, view_name)
    execute(line)

    client.clean(db_name)


def test_create_and_alter_view_with_sql_mode():
    """
    {
    "title": "test_create_and_alter_view_with_sql_mode",
    "describe": "test_create_and_alter_view_with_sql_mode, 测试在不同sql mode下新建/修改view，并在不同的sql mode下进行查询, ||的转义结果应该由新建/修改时的sql mode来确定，而不是查询时的sql mode",
    "tag": "function,p1"
    }
    """
    """
    test_create_and_alter_view_with_sql_mode
    测试在不同sql mode下新建/修改view，并在不同的sql mode下进行查询
    ||的转义结果应该由新建/修改时的sql mode来确定，而不是查询时的sql mode
    """
    db_name, table_name, invalied_name_1 = util.gen_name_list()
    line = 'DROP DATABASE IF EXISTS %s' % db_name
    client.execute(line)
    client.create_database(db_name)
    client.use(db_name)

    view_name = 'baseall_view'
    view_name_1 = 'baseall_view_1'

    # case: 在sql mode=0的时候新建view，这时候的||等价于or
    line = 'set sql_mode=0;'
    execute(line)
    line = 'create view {0} as select (True || False ) from {1}'.format(view_name, baseall_tb)
    execute(line)
    line1 = 'select * from {0}.{1}'.format(db_name, view_name)
    line2 = 'select ((TRUE) OR (FALSE)) from {0}'.format(baseall_tb)
    check2_palo(line1, line2)

    line = 'create view {0} as select k1, (k1>10) || (k2>2) from {1}'.format(view_name_1, baseall_tb)
    execute(line)
    line1 = 'select * from {0}.{1} order by k1'.format(db_name, view_name_1)
    line2 = 'select k1,((k1>10) OR (k2>2)) from {0} order by k1'.format(baseall_tb)
    check2_palo(line1, line2)

    # case: 由于view是在sql mode=0的时候建的，即使sql mode切换成2，这时候的view的查询结果仍等价于or
    line = 'set sql_mode=2'
    execute(line)

    line1 = 'select * from {0}.{1}'.format(db_name, view_name)
    line2 = 'select ((TRUE) OR (FALSE)) from {0}'.format(baseall_tb)
    check2_palo(line1, line2)

    line1 = 'select * from {0}.{1} order by k1'.format(db_name, view_name_1)
    line2 = 'select k1,((k1>10) OR (k2>2)) from {0} order by k1'.format(baseall_tb)
    check2_palo(line1, line2)

    # case: 在sql mode=0的时候修改view，这时候的||等价于字符串连接
    line = 'alter view {0} as select (True || False ) from {1}'.format(view_name, baseall_tb)
    execute(line)
    line1 = 'select * from {0}.{1}'.format(db_name, view_name)
    line2 = 'select (concat(TRUE, FALSE)) from {0}'.format(baseall_tb)
    check2_palo(line1, line2)

    line = 'alter view {0} as select k1,(k1>10) || (k2>2) from {1}'.format(view_name_1, baseall_tb)
    execute(line)
    line1 = 'select * from {0}.{1} order by k1'.format(db_name, view_name_1)
    line2 = 'select k1, (concat((k1>10), (k2>2))) from {0} order by k1'.format(baseall_tb)
    check2_palo(line1, line2)

    # case: 由于view是在sql mode=2的时候修改的，即使sql mode切换成0，这时候的view的查询结果仍等价于字符串连接
    line = 'set sql_mode=0'
    execute(line)

    line1 = 'select * from {0}.{1}'.format(db_name, view_name)
    line2 = 'select (concat(TRUE, FALSE)) from {0}'.format(baseall_tb)
    check2_palo(line1, line2)

    line1 = 'select * from {0}.{1} order by k1'.format(db_name, view_name_1)
    line2 = 'select k1,(concat((k1>10), (k2>2))) from {0} order by k1'.format(baseall_tb)
    check2_palo(line1, line2)

    # restart_palo_and_reconnect()

    line1 = 'select * from {0}.{1}'.format(db_name, view_name)
    line2 = 'select (concat(TRUE, FALSE)) from {0}'.format(baseall_tb)
    check2_palo(line1, line2)

    line1 = 'select * from {0}.{1} order by k1'.format(db_name, view_name_1)
    line2 = 'select k1,(concat((k1>10), (k2>2))) from {0} order by k1'.format(baseall_tb)
    check2_palo(line1, line2)

    client.clean(db_name)


def test_show_view_issue_5812():
    """
    {
    "title": "test_show_view",
    "describe": "show view正确",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, DATA.tinyint_column_no_agg_list, keys_desc=DATA.unique_key)
    assert ret, 'create table failed'
    sql = 'insert into %s values(1, 2, 3, 4, 5)' % table_name
    ret = client.execute(sql)
    assert ret == (), 'insert date failed'
    view_name = 'test_view'
    sql = 'create view %s as select * from %s' % (view_name, table_name)
    ret = client.execute(sql)
    assert ret == (), 'create view failed'
    sql = 'show view from %s.%s' % (database_name, table_name)
    ret = client.execute(sql)
    assert len(ret) == 1
    client.clean(database_name)


if __name__ == '__main__':
    setup_module()
