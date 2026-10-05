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
#   @file: test_sys_materialized_view_shoot.py
#   @date: 2020-09-03 11:12:43
#   @brief: 验证duplicate表的物化视图的命中
################################################################################

"""
新增hll_union, count, bitmap_union聚合的物化视图
"""

import sys
import os
sys.path.append("../")
from data import schema as DATA
from data import load_file as FILE
from lib import palo_client
from lib import palo_config
from lib import util

config = palo_config.config
LOG = palo_client.LOG
L = palo_client.L

broker_info = palo_config.broker_info


def get_explain_table(client, sql):
    """
    Get explain table
    """
    result = client.execute('EXPLAIN ' + sql)
    if result is None:
        return None
    rollup_flag = 'rollup: '
    explain_table = list()
    for element in result:
        message = element[0].lstrip()
        if message.startswith(rollup_flag):
            explain_table.append(message[len(rollup_flag):].rstrip(' '))
    return explain_table


def check2(client, sql1, sql2):
    """check 2 sql same result"""
    ret1 = client.execute(sql1)
    ret2 = client.execute(sql2)
    util.check(ret1, ret2)


def setup_module():
    """
    Set up
    """
    global query_db, database_name, tb_dup, tb_dup
    global rollup_name1, rollup_name2, rollup_name3, rollup_name4, rollup_name5
    global mv_name1, mv_name2, mv_name3, mv_name4, mv_name5, mv_name6, mv_name7, mv_name8, mv_name_9, mv_name10
    if 'FE_DB' in os.environ.keys():
        query_db = os.environ["FE_DB"]
    else:
        query_db = "test_query_qa"
    database_name = 'test_sys_materialized_view_shoot_test_shoot_db'
    tb_dup = 'test_shoot_tb_dup'
    mv_name1 = 'mv1'
    mv_name2 = 'mv2'
    mv_name3 = 'mv3'
    mv_name4 = 'mv4'
    mv_name5 = 'mv5'
    mv_name6 = 'mv6'
    mv_name7 = 'mv7'
    mv_name8 = 'mv8'
    mv_name9 = 'mv9'
    mv_name10 = 'mv10'
    init_mv()
    

def init_mv():
    """init db, table and rollup"""
    client = palo_client.get_client(config.fe_host, config.fe_query_port)
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    ret = client.create_table(tb_dup, DATA.baseall_column_no_agg_list,
                              distribution_info=palo_client.DistributionInfo("HASH(k2)", 5), set_null=True)

    sql = 'select k1, k2, k3, k4, max(k8), sum(k9) from %s group by k1, k2, k3, k4' % tb_dup
    client.create_materialized_view(tb_dup, mv_name1, sql, is_wait=True)
    assert client.show_tables(tb_dup)
    assert client.get_index(tb_dup, index_name=mv_name1)

    sql = 'select k1, k3, max(k8), sum(k9) from %s group by k1, k3' % tb_dup
    client.create_materialized_view(tb_dup, mv_name2, sql, is_wait=True)
    assert client.show_tables(tb_dup)
    assert client.get_index(tb_dup, index_name=mv_name2)

    sql = 'select k1, k2, k4, max(k8), sum(k9) from %s group by k1, k2, k4' % tb_dup
    client.create_materialized_view(tb_dup, mv_name3, sql, is_wait=True)
    assert client.show_tables(tb_dup)
    assert client.get_index(tb_dup, index_name=mv_name3)

    sql = 'select k2, k1, max(k8), sum(k9) from %s group by k2, k1' % tb_dup
    client.create_materialized_view(tb_dup, mv_name4, sql, is_wait=True)
    assert client.show_tables(tb_dup)
    assert client.get_index(tb_dup, index_name=mv_name4)

    sql = 'select k2, k1, k3, k4, max(k8), sum(k9) from %s group by k2, k1, k3, k4' % tb_dup
    client.create_materialized_view(tb_dup, mv_name5, sql, is_wait=True)
    assert client.show_tables(tb_dup)
    assert client.get_index(tb_dup, index_name=mv_name5)

    sql = 'select k2, k1, k3, k4, count(k6), count(k7) from %s group by k2, k1, k3, k4' % tb_dup
    client.create_materialized_view(tb_dup, mv_name6, sql, is_wait=True)
    assert client.show_tables(tb_dup)
    assert client.get_index(tb_dup, index_name=mv_name6)

    sql = 'select k2, k1, k3, k4, hll_union(hll_hash(k10)), hll_union(hll_hash(k11)) from %s ' \
          'group by k2, k1, k3, k4' % tb_dup
    client.create_materialized_view(tb_dup, mv_name7, sql, is_wait=True)
    assert client.show_tables(tb_dup)
    assert client.get_index(tb_dup, index_name=mv_name7)

    sql = 'select k2, k3, k4, bitmap_union(to_bitmap(k1)) from %s group by k2, k3, k4' % tb_dup
    client.create_materialized_view(tb_dup, mv_name8, sql, is_wait=True)
    assert client.show_tables(tb_dup)
    assert client.get_index(tb_dup, index_name=mv_name8)

    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k10', 'k11', 'k7', 'k8', 'k9']
    set_list = ['']
    data_desc_list = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, tb_dup)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret

    sql1 = 'select count(*) from %s.%s' % (database_name, tb_dup)
    sql2 = 'select count(*) from %s.baseall' % query_db
    check2(client, sql1, sql2)


def test_shoot_1():
    """
    {
    "title": "test_shoot_1",
    "describe": "without where, sum命中rollup",
    "tag": "system,p1"
    }
    """
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port)
    client.init()
    client.use(database_name)
    client.set_variables('test_materialized_view', 1)
    assert client.show_variable('test_materialized_view')[0][1] == 'true'

    sql = 'select sum(k9) from %s.%s' % (database_name, tb_dup)
    shoot_table = get_explain_table(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert mv_name2 in shoot_table or mv_name4 in shoot_table
    check_sql = 'select sum(k9) from %s.baseall' % query_db
    check2(client, sql, check_sql)


def test_shoot_2():
    """
    {
    "title": "test_shoot_2",
    "describe": "where k1 = 1,点查询命中rollup",
    "tag": "system,p1"
    }
    """
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port)
    client.init()
    client.use(database_name)

    sql = 'select sum(k9) from %s.%s where k1 = 1' % (database_name, tb_dup)
    shoot_table = get_explain_table(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert mv_name2 in shoot_table
    check_sql = 'select sum(k9) from %s.baseall where k1 = 1' % query_db
    check2(client, sql, check_sql)


def test_shoot_3():
    """
    {
    "title": "test_shoot_3",
    "describe": "where k1 > 1，范围查询命中rollup",
    "tag": "system,p1"
    }
    """
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port)
    client.init()
    client.use(database_name)

    sql = 'select sum(k9) from %s.%s where k1 > 1' % (database_name, tb_dup)
    shoot_table = get_explain_table(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert mv_name2 in shoot_table
    check_sql = 'select sum(k9) from %s.baseall where k1 > 1' % query_db
    check2(client, sql, check_sql)


def test_shoot_4():
    """
    {
    "title": "test_shoot_4",
    "describe": "where k1=1 and k2=1，and 命中rollup",
    "tag": "system,p1"
    }
    """
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port)
    client.init()
    client.use(database_name)

    sql = 'select sum(k9) from %s.%s where k1 = 1 and k2 = 1' % (database_name, tb_dup)
    shoot_table = get_explain_table(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert mv_name4 in shoot_table
    check_sql = 'select sum(k9) from %s.baseall where k1 = 1 and k2 = 1' % query_db
    check2(client, sql, check_sql)


def test_shoot_5():
    """
    {
    "title": "test_shoot_5",
    "describe": "where k1>1 and k2=1，and命中rollup",
    "tag": "system,p1"
    }
    """
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port)
    client.init()
    client.use(database_name)

    sql = 'select sum(k9) from %s.%s where k1 > 1 and k2 = 1' % (database_name, tb_dup)
    shoot_table = get_explain_table(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert mv_name4 in shoot_table
    check_sql = 'select sum(k9) from %s.baseall where k1 > 1 and k2 = 1' % query_db
    check2(client, sql, check_sql)


def test_shoot_6():
    """
    {
    "title": "test_shoot_6",
    "describe": "where k4>1，命中rollup",
    "tag": "system,p1"
    }
    """
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port)
    client.init()
    client.use(database_name)

    sql = 'select sum(k9) from %s.%s where k4 > 1' % (database_name, tb_dup)
    shoot_table = get_explain_table(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert mv_name3 in shoot_table
    check_sql = 'select sum(k9) from %s.baseall where k4 > 1' % query_db
    check2(client, sql, check_sql)


def test_shoot_7():
    """
    {
    "title": "test_shoot_7",
    "describe": "where k1=1 and k2>1，命中rollup",
    "tag": "system,p1"
    }
    """
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port)
    client.init()
    client.use(database_name)

    sql = 'select sum(k9) from %s.%s where k1 = 1 and k2 > 1' % (database_name, tb_dup)
    shoot_table = get_explain_table(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert mv_name3 in shoot_table
    check_sql = 'select sum(k9) from %s.baseall where k1 = 1 and k2 > 1' % query_db
    check2(client, sql, check_sql)


def test_shoot_8():
    """
    {
    "title": "test_shoot_8",
    "describe": "where k1=1 and k2=1 and k3=1命中中rollup",
    "tag": "system,p1"
    }
    """
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port)
    client.init()
    client.use(database_name)

    sql = 'select sum(k9) from %s.%s where k1 = 1 and k2 = 1 and k3 > 1' \
          % (database_name, tb_dup)
    shoot_table = get_explain_table(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert mv_name1 in shoot_table
    check_sql = 'select sum(k9) from %s.baseall where k1 = 1 and k2 = 1 and k3 > 1' % query_db
    check2(client, sql, check_sql)
  

def test_shoot_9():
    """
    {
    "title": "test_shoot_9",
    "describe": " where cast，命中rollup",
    "tag": "system,p1"
    }
    """
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port)
    client.init()
    client.use(database_name)

    sql = 'select sum(k9) from %s.%s where cast(k2 as int) < 10000' % (database_name, tb_dup)
    shoot_table = get_explain_table(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert mv_name4 in shoot_table
    check_sql = 'select sum(k9) from %s.baseall where cast(k2 as int) < 10000' % query_db
    check2(client, sql, check_sql)


def test_shoot_10():
    """
    {
    "title": "test_shoot_10",
    "describe": "where join，命中rollup",
    "tag": "system,p1"
    }
    """
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port)
    client.init()
    client.use(database_name)

    # 左表

    sql = 'select sum(a.k9) from %s.%s a join %s.%s b on a.k1 = b.k1' \
          % (database_name, tb_dup, database_name, tb_dup)
    shoot_table = get_explain_table(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert mv_name2 in shoot_table
    check_sql = 'select sum(a.k9) from %s.baseall a join %s.baseall b on a.k1 = b.k1' \
                % (query_db, query_db)
    check2(client, sql, check_sql)


def test_shoot_11():
    """
    {
    "title": "test_shoot_11",
    "describe": "where join on，命中rollup",
    "tag": "system,p1"
    }
    """
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port)
    client.init()
    client.use(database_name)

    sql = 'select sum(a.k9) from %s.%s a join %s.%s b on a.k1 = b.k1 and a.k2 = 2' \
          % (database_name, tb_dup, database_name, tb_dup)
    shoot_table = get_explain_table(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert mv_name4 in shoot_table
    check_sql = 'select sum(a.k9) from %s.baseall a join %s.baseall b on a.k1 = b.k1 ' \
                'and a.k2 = 2' % (query_db, query_db)
    check2(client, sql, check_sql)


def test_shoot_12():
    """
    {
    "title": "test_shoot_12",
    "describe": "where join on cas，命中rollup",
    "tag": "system,p1"
    }
    """
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port)
    client.init()
    client.use(database_name)
    
    sql = 'select sum(a.k9) from %s.%s a join %s.%s b on a.k1 = cast(hex(b.k1) as int) ' \
          'and a.k2 = 2' % (database_name, tb_dup, database_name, tb_dup)
    shoot_table = get_explain_table(client, sql)
    LOG.info(L('shoot table:', shoot_table=shoot_table))
    assert mv_name4 in shoot_table
    check_sql = 'select sum(a.k9) from %s.baseall a join %s.baseall b on ' \
                'a.k1 = cast(hex(b.k1) as int) and a.k2 = 2' % (query_db, query_db)
    check2(client, sql, check_sql)

 
def test_shoot_13():
    """
    {
    "title": "test_shoot_13",
    "describe": "count, hll_union, bitmap_union聚合查询测试",
    "tag": "system,p1"
    }
    """
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port)
    client.init()
    client.use(database_name)

    sql1 = 'select bitmap_union_count(to_bitmap(%s)) from %s where 1=1'
    sql2 = 'select bitmap_union(to_bitmap(%s)) from %s where 1=1'
    sql3 = 'select count(distinct %s) from %s where 1=1'
    sql4 = 'select count(%s) from %s where 1=1'
    sql5 = 'select hll_cardinality(hll_raw_agg(hll_hash(%s))) from %s where 1=1'
    sql6 = 'select hll_union_agg(hll_hash(%s)) from %s where 1=1'
    sql7 = 'select ndv(%s) from %s where 1=1'
    sql8 = 'select approx_count_distinct(%s) from %s where 1=1'
    check(client, sql1, 'k1', mv_name8)
    check(client, sql2, 'k1', mv_name8)
    check(client, sql3, 'k1', mv_name8)
    check(client, sql4, 'k1', tb_dup)
    check(client, sql5, 'k1', tb_dup)
    check(client, sql6, 'k1', tb_dup)
    check(client, sql7, 'k1', tb_dup)
    check(client, sql8, 'k1', tb_dup)
    check(client, sql3, 'k6', tb_dup)
    check(client, sql4, 'k6', mv_name6)
    check(client, sql5, 'k6', tb_dup)
    check(client, sql6, 'k6', tb_dup)
    check(client, sql7, 'k6', tb_dup)
    check(client, sql8, 'k6', tb_dup)
    check(client, sql3, 'k11', tb_dup)
    check(client, sql4, 'k11', tb_dup)
    check(client, sql5, 'k11', mv_name7)
    check(client, sql6, 'k11', mv_name7)
    check(client, sql7, 'k11', mv_name7)
    check(client, sql8, 'k11', mv_name7)


def check(client, sql, col, mv):
    """check explain rollup and result"""
    sql1 = sql % (col, tb_dup)
    sql2 = sql % (col, 'test_query_qa.baseall')
    shoot_mv = get_explain_table(client, sql1)
    print(mv, shoot_mv)
    check2(client, sql1, sql2)
    assert mv in shoot_mv


def teardown_module():
    """tear down"""
    pass



