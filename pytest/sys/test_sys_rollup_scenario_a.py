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

################################################################################
#
#   @file test_sys_rollup_scenario_a.py
#   @date 2019-04-29 15:07:20
#   @brief test sql shoot rollup policy
################################################################################

"""
test sql shoot rollup policy
"""

import time
import sys
import os
sys.path.append("../")
from data import schema as DATA
from data import load_file as FILE
from lib import palo_client
from lib import palo_config
from lib import util
from lib import common
config = palo_config.config
LOG = palo_client.LOG
L = palo_client.L

broker_info = palo_config.broker_info


def check2(client, sql1, sql2):
    """check 2 sql same result"""
    ret1 = client.execute(sql1)
    ret2 = client.execute(sql2)
    util.check(ret1, ret2)


def setup_module():
    """
    Set up
    """
    global query_db, database_name, table_name, index_name1, index_name2, index_name3, \
           index_name4, index_name5
    if 'FE_DB' in os.environ.keys():
        query_db = os.environ["FE_DB"]
    else:
        query_db = "test_query_qa"
    database_name = 'test_sys_rollup_scenario_a_test_shoot_db'
    table_name = 'test_sys_rollup_scenario_a_test_shoot_tb'
    index_name1 = 'rollup1'
    index_name2 = 'rollup2'
    index_name3 = 'rollup3'
    index_name4 = 'rollup4'
    index_name5 = 'rollup5'
    init()
    

def init():
    """init db, table and rollup"""
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port)
    client.init()
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    ret = client.create_table(table_name, DATA.baseall_column_list, 
                              distribution_info=DATA.baseall_distribution_info)

    rollup_list = ['k1', 'k2', 'k3', 'k4', 'k8', 'k9']
    client.create_rollup_table(table_name, index_name1, rollup_list, is_wait=True)
    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_index(table_name, index_name=index_name1)
    
    rollup_list = ['k1', 'k3', 'k8', 'k9']
    client.create_rollup_table(table_name, index_name2, rollup_list, is_wait=True)
    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_index(table_name, index_name=index_name2)
    
    rollup_list = ['k1', 'k2', 'k4', 'k8', 'k9']
    client.create_rollup_table(table_name, index_name3, rollup_list, is_wait=True)
    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_index(table_name, index_name=index_name3)
    
    rollup_list = ['k2', 'k1', 'k8', 'k9']
    client.create_rollup_table(table_name, index_name4, rollup_list, is_wait=True)
    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_index(table_name, index_name=index_name4)
    
    rollup_list = ['k2', 'k1', 'k3', 'k4', 'k8', 'k9']
    client.create_rollup_table(table_name, index_name5, rollup_list, is_wait=True)
    assert client.show_tables(table_name)
    assert client.get_index(table_name)
    assert client.get_index(table_name, index_name=index_name5)

    data_desc_list = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    sql1 = 'select count(*) from %s.%s' % (database_name, table_name)
    sql2 = 'select count(*) from %s.baseall' % query_db
    check2(client, sql1, sql2)


def test_shoot_1():
    """
    {
    "title": "test_sys_rollup_scenario_a.test_shoot_1",
    "describe": "without where, sum命中rollup",
    "tag": "system,p1"
    }
    """
    """
    without where
    """
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port)
    client.init()
    client.use(database_name)

    sql = 'select sum(k9) from %s.%s' % (database_name, table_name)
    shoot_table = common.get_explain_rollup(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert index_name2 in shoot_table or index_name4 in shoot_table
    check_sql = 'select sum(k9) from %s.baseall' % query_db
    check2(client, sql, check_sql)


def test_shoot_2():
    """
    {
    "title": "test_sys_rollup_scenario_a.test_shoot_2",
    "describe": "where k1 = 1,点查询命中rollup",
    "tag": "system,p1"
    }
    """
    """where k1 = 1"""
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port)
    client.init()
    client.use(database_name)
    sql = 'select sum(k9) from %s.%s where k1 = 1' % (database_name, table_name)
    shoot_table = common.get_explain_rollup(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert index_name2 in shoot_table
    check_sql = 'select sum(k9) from %s.baseall where k1 = 1' % query_db
    check2(client, sql, check_sql)


def test_shoot_3():
    """
    {
    "title": "test_sys_rollup_scenario_a.test_shoot_3",
    "describe": "where k1 > 1，范围查询命中rollup",
    "tag": "system,p1"
    }
    """
    """
    where k1 > 1
    """
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port)
    client.init()
    client.use(database_name)
    sql = 'select sum(k9) from %s.%s where k1 > 1' % (database_name, table_name)
    shoot_table = common.get_explain_rollup(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert index_name2 in shoot_table
    check_sql = 'select sum(k9) from %s.baseall where k1 > 1' % query_db
    check2(client, sql, check_sql)


def test_shoot_4():
    """
    {
    "title": "test_sys_rollup_scenario_a.test_shoot_4",
    "describe": "where k1=1 and k2=1，and 命中rollup",
    "tag": "system,p1"
    }
    """
    """
    where k1=1 and k2=1
    """
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port)
    client.init()
    client.use(database_name)
    sql = 'select sum(k9) from %s.%s where k1 = 1 and k2 = 1' % (database_name, table_name)
    shoot_table = common.get_explain_rollup(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert index_name4 in shoot_table
    check_sql = 'select sum(k9) from %s.baseall where k1 = 1 and k2 = 1' % query_db
    check2(client, sql, check_sql)


def test_shoot_5():
    """
    {
    "describe": "where k1>1 and k2=1，and命中rollup",
    "tag": "system,p1"
    }
    """
    """
    where k1>1 and k2=1
    """
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port)
    client.init()
    client.use(database_name)
    sql = 'select sum(k9) from %s.%s where k1 > 1 and k2 = 1' % (database_name, table_name)
    shoot_table = common.get_explain_rollup(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert index_name4 in shoot_table
    check_sql = 'select sum(k9) from %s.baseall where k1 > 1 and k2 = 1' % query_db
    check2(client, sql, check_sql)


def test_shoot_6():
    """
    {
    "describe": "where k4>1，命中rollup",
    "tag": "system,p1"
    }
    """
    """
    where k4>1
    """
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port)
    client.init()
    client.use(database_name)
    sql = 'select sum(k9) from %s.%s where k4 > 1' % (database_name, table_name)
    shoot_table = common.get_explain_rollup(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert index_name3 in shoot_table
    check_sql = 'select sum(k9) from %s.baseall where k4 > 1' % query_db
    check2(client, sql, check_sql)


def test_shoot_7():
    """
    {
    "title": "test_sys_rollup_scenario_a.test_shoot_7",
    "describe": "where k1=1 and k2>1，命中rollup",
    "tag": "system,p1"
    }
    """
    """
    where k1=1 and k2>1
    """
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port)
    client.init()
    client.use(database_name)
    sql = 'select sum(k9) from %s.%s where k1 = 1 and k2 > 1' % (database_name, table_name)
    shoot_table = common.get_explain_rollup(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert index_name3 in shoot_table
    check_sql = 'select sum(k9) from %s.baseall where k1 = 1 and k2 > 1' % query_db
    check2(client, sql, check_sql)


def test_shoot_8():
    """
    {
    "title": "test_sys_rollup_scenario_a.test_shoot_8",
    "describe": "where k1=1 and k2=1 and k3=1命中中rollup",
    "tag": "system,p1"
    }
    """
    """
    where k1=1 and k2=1 and k3=1
    """
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port)
    client.init()
    client.use(database_name)
    sql = 'select sum(k9) from %s.%s where k1 = 1 and k2 = 1 and k3 > 1' \
          % (database_name, table_name)
    shoot_table = common.get_explain_rollup(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert index_name1 in shoot_table
    check_sql = 'select sum(k9) from %s.baseall where k1 = 1 and k2 = 1 and k3 > 1' % query_db
    check2(client, sql, check_sql)
  

def test_shoot_9():
    """
    {
    "title": "test_sys_rollup_scenario_a.test_shoot_9",
    "describe": " where cast，命中rollup",
    "tag": "system,p1"
    }
    """
    """
    where cast
    """
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port)
    client.init()
    client.use(database_name)
    sql = 'select sum(k9) from %s.%s where cast(k2 as int) < 10000' % (database_name, table_name)
    shoot_table = common.get_explain_rollup(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert index_name4 in shoot_table
    check_sql = 'select sum(k9) from %s.baseall where cast(k2 as int) < 10000' % query_db
    check2(client, sql, check_sql)


def test_shoot_10():
    """
    {
    "title": "test_sys_rollup_scenario_a.test_shoot_10",
    "describe": "where join，命中rollup",
    "tag": "system,p1"
    }
    """
    """
    where join 
    """
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port)
    client.init()
    client.use(database_name)
    # 左表
    sql = 'select sum(a.k9) from %s.%s a join %s.%s b on a.k1 = b.k1' \
          % (database_name, table_name, database_name, table_name)
    shoot_table = common.get_explain_rollup(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert index_name2 in shoot_table
    check_sql = 'select sum(a.k9) from %s.baseall a join %s.baseall b on a.k1 = b.k1' \
                % (query_db, query_db)
    check2(client, sql, check_sql)


def test_shoot_11():
    """
    {
    "title": "test_sys_rollup_scenario_a.test_shoot_11",
    "describe": "where join on，命中rollup",
    "tag": "system,p1"
    }
    """
    """
    where join on 
    """
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port)
    client.init()
    client.use(database_name)
    sql = 'select sum(a.k9) from %s.%s a join %s.%s b on a.k1 = b.k1 and a.k2 = 2' \
          % (database_name, table_name, database_name, table_name)
    shoot_table = common.get_explain_rollup(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert index_name4 in shoot_table
    check_sql = 'select sum(a.k9) from %s.baseall a join %s.baseall b on a.k1 = b.k1 ' \
                'and a.k2 = 2' % (query_db, query_db)
    check2(client, sql, check_sql)


def test_shoot_12():
    """
    {
    "title": "test_sys_rollup_scenario_a.test_shoot_12",
    "describe": "where join on cas，命中rollup",
    "tag": "system,p1"
    }
    """
    """
    where join on cast
    """
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port)
    client.init()
    client.use(database_name)
    sql = 'select sum(a.k9) from %s.%s a join %s.%s b on a.k1 = cast(hex(b.k1) as int) ' \
          'and a.k2 = 2' % (database_name, table_name, database_name, table_name)
    shoot_table = common.get_explain_rollup(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert index_name4 in shoot_table
    check_sql = 'select sum(a.k9) from %s.baseall a join %s.baseall b on ' \
                'a.k1 = cast(hex(b.k1) as int) and a.k2 = 2' % (query_db, query_db)




