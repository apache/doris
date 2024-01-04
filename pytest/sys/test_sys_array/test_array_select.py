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
#   @file test_array_select.py
#   @date 2022-08-15 11:09:53
#   @brief This file is a test file for array type.
#
#############################################################################
"""
test_array_select.py
"""
import sys
import os
import time
import pytest
file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(file_dir)
file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(file_dir)

from lib import palo_config
from lib import palo_client
from lib import palo_job
from lib import palo_types
from lib import util
from lib import common
from data import schema as SCHEMA
from data import load_file as FILE

config = palo_config.config
broker_info = palo_client.BrokerInfo(config.broker_name, config.broker_property)
db = 'test_array_select_test_table_db'
table = 'test_array_select_test_table_tb'
table_2 = 'test_array_select_test_table_tb_1'


def setup_module():
    """enable array"""
    client = common.get_client()
    ret = client.show_variables('enable_vectorized_engine')
    if len(ret) == 1 and ret[0][1] == 'false':
        raise pytest.skip('skip if enable_vectorized_engine is false')

    ret = client.admin_show_config('enable_array_type')
    assert len(ret) == 1, 'get enable_array_type config error'
    value = palo_job.AdminShowConfig(ret[0]).get_value()
    if value != 'true':
        client.set_frontend_config('enable_array_type', 'true')
    if len(client.show_databases(db)) == 0:
        print(len(client.show_databases(db)))
        print(client.show_databases(db))
        init(db, table, True)


def init(database_name, table_name, sort=False):
    """init test data"""
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, SCHEMA.array_table_list, keys_desc=SCHEMA.duplicate_key,
                              set_null=True)
    assert ret, 'create table failed'
    
    sql = 'insert into %s select k1, [0, 1, 1, 0] as a1, collect_list(k1) as a2, collect_list(k2) as a3, ' \
          'collect_list(k3) as a4, collect_list(k4) as a5, collect_list(cast(k4 as largeint)*10) as a6, ' \
          'collect_list(k5) as a7, collect_list(k9) as a8, collect_list(k8) as a9, collect_list(k10) as a10, ' \
          'collect_list(k11) as a11, collect_list(k6) as a12, collect_list(k7) as a12, collect_list(k7) as a13 ' \
          'from test_query_qa.test group by k1' % table_name
    sql_s = 'insert into %s select k1, [0, 1, 1, 0] as a1, array_sort(collect_list(k1)) as a2, ' \
            'array_sort(collect_list(k2)) as a3, array_sort(collect_list(k3)) as a4, ' \
            'array_sort(collect_list(k4)) as a5, array_sort(collect_list(cast(k4 as largeint) * 10)) as a6, ' \
            'array_sort(collect_list(k5)) as a7, array_sort(collect_list(k9)) as a8, ' \
            'array_sort(collect_list(k8)) as a9, array_sort(collect_list(k10)) as a10, ' \
            'array_sort(collect_list(k11)) as a11, array_sort(collect_list(k6)) as a12, ' \
            'array_sort(collect_list(k7)) as a13, array_sort(collect_list(k7)) as a14 ' \
            'from test_query_qa.test group by k1' % table_name
    if sort:
        insert_sql = sql_s
    else:
        insert_sql = sql
    ret = client.execute(insert_sql)
    table_name_1 = table_name + "_1"
    ret = client.create_table(table_name_1, SCHEMA.array_table_list, keys_desc=SCHEMA.duplicate_key,
                              set_null=True)
    assert ret, 'create table failed'
    sql = 'insert into %s select 0, [0, 1, 1, 0], collect_list(k1), collect_list(k2), ' \
          'collect_list(k3), collect_list(k4), collect_list(cast(k4 as largeint)*10), collect_list(k5), ' \
          'collect_list(k9), collect_list(k8), collect_list(k10), collect_list(k11), ' \
          'collect_list(k6), collect_list(k7), collect_list(k7) from ' \
          '(select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from test_query_qa.baseall ' \
          'union select null, null, null, null, null, null, null, null, null, null, null) tmp' % table_name_1
    sql_s = 'insert into %s select 0, [0, 1, 1, 0], array_sort(collect_list(k1)), array_sort(collect_list(k2)), ' \
            'array_sort(collect_list(k3)), array_sort(collect_list(k4)), ' \
            'array_sort(collect_list(cast(k4 as largeint) * 10)), array_sort(collect_list(k5)), ' \
            'array_sort(collect_list(k9)), array_sort(collect_list(k8)), array_sort(collect_list(k10)), ' \
            'array_sort(collect_list(k11)), array_sort(collect_list(k6)), array_sort(collect_list(k7)), ' \
            'array_sort(collect_list(k7)) from test_query_qa.baseall' % table_name_1
    if sort:
        insert_sql = sql_s
    else:
        insert_sql = sql
    client.execute(insert_sql)
    insert_sql = "insert into %s values(1, [null, 1], [1, null, 3, 4], [0, null, 32767, -32767],  " \
                 "[4, null, 65534, 65535],  [65534, null, 65535, 5, 65536, 6553600], " \
                 "[65534, 6, 65535, 6553600, null], [1.12, 3.45, 4.23, null], [3.1234,1.2, null], " \
                 "[123.00001, null, 1.1],  ['2022-07-13', null, '2000-01-01'], " \
                 "[null, '2022-07-13 12:30:00', '2022-07-13 12:30:00'], " \
                 "['', 'hello char', 'tds', null], [null, 'hello varchar', ''], [null, '', 'jedsd'])" % table_name_1
    ret = client.execute(insert_sql)
    sql = 'insert into %s select 2, [], [], [], [], [], [], [], [], [], [], [], [], [], []' % table_name_1
    ret = client.execute(sql)
    sql = 'insert into %s select 3, [null], [null], [null], [null], [null], [null], [null], ' \
          '[null], [null], [null], [null], [null], [null], [null]' % table_name_1
    ret = client.execute(sql)
    sql = 'insert into %s select 4, null, null, null, null, null, null, null, ' \
          'null, null, null, null, null, null, null' % table_name_1
    ret = client.execute(sql)


def test_array_avg():
    """
    {
    "title": "test_array_avg",
    "describe": "array_avg函数测试, ARRAY_AVG(ARRAY<T> a) 求array中元素的平均值",
    "tag": "function,p1"
    }
    """
    client = common.get_client()
    # types
    support_k = ['a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9']
    not_support_k = ['a10', 'a11', 'a12', 'a13', 'a14']
    sql = 'select array_avg({0}) from {1}.{2}'
    for c in support_k:
        ret = client.execute(sql.format(c, db, table_2))
        assert ret, 'sql: %s, error' % sql.format(c, db, table)
    msg = 'No matching function with signature'
    for c in not_support_k:
        util.assert_return(False, msg, client.execute, sql.format(c, db, table))
    sql1 = 'select k1, array_avg(a2), array_avg(a3), array_avg(a4), array_avg(a5), array_avg(a6), ' \
           'array_avg(a7), array_avg(a8), array_avg(a9) from %s.%s order by k1' % (db, table)
    sql2 = 'select k1, avg(k1), avg(k2), avg(k3), avg(cast(k4 as largeint)), avg(cast(k4 as largeint) * 10), ' \
           'avg(k5), avg(k9), avg(k8) from test_query_qa.test group by k1 order by k1'
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select array_avg(a2), array_avg(a3), array_avg(a4), array_avg(a5), array_avg(a6), ' \
           'array_avg(a7), array_avg(a8), array_avg(a9) from %s.%s where k1 = 0' % (db, table_2)
    sql2 = 'select avg(k1), avg(k2), avg(k3), avg(k4), avg(cast(k4 as largeint) * 10), avg(k5), ' \
           'avg(k9), avg(k8) from test_query_qa.baseall'
    common.check2(client, sql1, sql2=sql2)

    sql2 = 'select null, null, null, null, null, null, null, null'
    for k in (2, 3, 4):
        sql1 = 'select array_avg(a2), array_avg(a3), array_avg(a4), array_avg(a5), array_avg(a6), ' \
               'array_avg(a7), array_avg(a8), array_avg(a9) from %s.%s where k1 = %s' % (db, table_2, k)
        common.check2(client, sql1, sql2=sql2)
    sql1 = 'select array_avg(a2), array_avg(a3), array_avg(a4), array_avg(a5), array_avg(a6), ' \
           'array_avg(a7), array_avg(a8), array_avg(a9) from %s.%s where k1 = 1' % (db, table_2)
    sql2 = 'select 2.6666666666666665, 0, 43691, 1350042, 1671168.75, 2.933333333, 2.1617000102996826, 62.050005'
    common.check2(client, sql1, sql2=sql2)
    # where
    sql1 = 'select k1, array_avg(a3) from %s.%s where array_avg(a3) > 0 order by k1' % (db, table)
    sql2 = 'select k1, avg(k2) from test_query_qa.test group by k1 having avg(k2) > 0 order by k1'
    common.check2(client, sql1, sql2=sql2)


def test_array_max():
    """
    {
    "title": "test_array_max",
    "describe": "array_max函数测试, T ARRAY_MAX(ARRAY<T> a) 求array中元素的最大值，字符串类型不支持该函数",
    "tag": "function,p1"
    }
    """
    client = common.get_client()
    db = 'test_array_select_test_table_db'
    table = 'test_array_select_test_table_tb'
    table_2 = 'test_array_select_test_table_tb_1'
    # types
    support_k = ['a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9', 'a10', 'a11']
    # not_support_k = ['a10', 'a11', 'a12', 'a13', 'a14']
    not_support_k = ['a12', 'a13', 'a14']
    sql = 'select array_max({0}) from {1}.{2}'
    for c in support_k:
        ret = client.execute(sql.format(c, db, table_2))
        assert ret
    msg = 'No matching function with signature'
    for c in not_support_k:
        util.assert_return(False, msg, client.execute, sql.format(c, db, table_2))
    # query
    sql1 = 'select k1, array_max(a1), array_max(a2), array_max(a3), array_max(a4), array_max(a5), ' \
           'array_max(a6), array_max(a7), array_max(a8), array_max(a9), array_max(a10), array_max(a11) ' \
           'from %s.%s order by k1' % (db, table)
    sql2 = 'select k1, 1, max(k1), max(k2), max(k3), max(k4), max(cast(k4 as largeint) * 10), ' \
           'max(k5), max(k9), max(k8), max(k10), max(k11) from test_query_qa.test group by k1 order by k1'
    common.check2(client, sql1, sql2=sql2)

    sql1 = 'select array_max(a1), array_max(a2), array_max(a3), array_max(a4), array_max(a5), array_max(a6), ' \
           'array_max(a7), array_max(a8), array_max(a9), array_max(a10), array_max(a11) ' \
           'from %s.%s where k1 = 0' % (db, table_2)
    sql2 = 'select 1, max(k1), max(k2), max(k3), max(k4), max(cast(k4 as largeint) * 10), max(k5), ' \
           'max(k9), max(k8), max(k10), max(k11) from test_query_qa.baseall'
    common.check2(client, sql1, sql2=sql2)
    
    sql2 = 'select null, null, null, null,null, null, null, null, null, null, null'
    for k in (2, 3, 4):
        sql1 = 'select array_max(a1), array_max(a2), array_max(a3), array_max(a4), array_max(a5), array_max(a6), ' \
               'array_max(a7), array_max(a8), array_max(a9), array_max(a10), array_max(a11) ' \
               'from %s.%s where k1 = %s' % (db, table_2, k)
        common.check2(client, sql1, sql2=sql2)

    sql1 = 'select array_max(a1), array_max(a2), array_max(a3), array_max(a4), array_max(a5), array_max(a6), ' \
           'array_max(a7), array_max(a8), array_max(a9), array_max(a10), array_max(a11) ' \
           'from %s.%s where k1 = 1' % (db, table_2)
    sql2 = 'select 1, 4, 32767, 65535, 6553600, 6553600, 4.23, 3.1234, 123.00001, ' \
           'cast("2022-07-13" as date), cast("2022=-07-13 12:30:00" as datetime)'
    common.check2(client, sql1, sql2=sql2)

    sql1 = 'select array_max([1, 2, 3, null, 0, 3, -3]), array_max([]), array_max([null, null])'
    sql2 = 'select 3, null, null'
    common.check2(client, sql1, sql2=sql2)

    # where
    sql1 = 'select k1, array_max(a3) from %s.%s where array_max(a3) > 1000000 order by k1' % (db, table)
    sql2 = 'select k1, max(k2) from test_query_qa.test group by k1 having max(k2) > 1000000 order by k1'
    common.check2(client, sql1, sql2=sql2)
        

def test_array_min():
    """
    {
    "title": "test_array_min",
    "describe": "array_min函数测试, T ARRAY_MIN(ARRAY<T> a) 求array中元素的最小值，不支持字符串类型",
    "tag": "function,p1"
    }
    """
    client = common.get_client()
    # types
    support_k = ['a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9', 'a10', 'a11']
    not_support_k = ['a12', 'a13', 'a14']
    sql = 'select array_min({0}) from {1}.{2}'
    for c in support_k:
        ret = client.execute(sql.format(c, db, table))
        assert ret
    msg = 'No matching function with signature'
    for c in not_support_k:
        util.assert_return(False, msg, client.execute, sql.format(c, db, table))
    # query
    sql1 = 'select k1, array_min(a1), array_min(a2), array_min(a3), array_min(a4), array_min(a5), array_min(a6), ' \
           'array_min(a7), array_min(a8), array_min(a9), array_min(a10), array_min(a11) ' \
           'from %s.%s order by k1' % (db, table)
    sql2 = 'select k1, 0, min(k1), min(k2), min(k3), min(k4), min(cast(k4 as largeint) * 10), min(k5), ' \
           'min(k9), min(k8), min(k10), min(k11) from test_query_qa.test group by k1 order by k1'
    common.check2(client, sql1, sql2=sql2)

    sql1 = 'select array_min(a1), array_min(a2), array_min(a3), array_min(a4), array_min(a5), array_min(a6), ' \
           'array_min(a7), array_min(a8), array_min(a9), array_min(a10), array_min(a11) ' \
           'from %s.%s where k1 = 0' % (db, table_2)
    sql2 = 'select 0, min(k1), min(k2), min(k3), min(k4), min(cast(k4 as largeint) * 10), ' \
           'min(k5), min(k9), min(k8), min(k10), min(k11) from test_query_qa.baseall'
    common.check2(client, sql1, sql2=sql2)

    sql2 = 'select null, null, null, null,null, null, null, null, null, null, null'
    for k in (2, 3, 4):
        sql1 = 'select array_min(a1), array_min(a2), array_min(a3), array_min(a4), array_min(a5), array_min(a6), ' \
               'array_min(a7), array_min(a8), array_min(a9), array_min(a10), array_min(a11) ' \
               'from %s.%s where k1 = %s' % (db, table_2, k)
        common.check2(client, sql1, sql2=sql2)
    sql1 = 'select array_min(a1), array_min(a2), array_min(a3), array_min(a4), array_min(a5), array_min(a6), ' \
           'array_min(a7), array_min(a8), array_min(a9), array_min(a10), array_min(a11) ' \
           'from %s.%s where k1 = 1' % (db, table_2)
    sql2 = 'select 1, 1, -32767, 4, 5, 6, 1.12, 1.2, 1.1, ' \
           'cast("2000-01-01" as date), cast("2022-07-13 12:30:00" as datetime)'
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select array_min([1, 2, 3, null, 0, 3, -3]), array_min([]), array_min([null, null]), ' \
           'array_min([null, 1, null]);'
    sql2 = 'select -3, null, null, 1'
    common.check2(client, sql1, sql2=sql2)

    # where
    sql1 = 'select k1, array_min(a3) from %s.%s where array_min(a3) > -1000000 order by k1' % (db, table)
    sql2 = 'select k1, min(k2) from test_query_qa.test group by k1 having min(k2) > -1000000 order by k1'
    common.check2(client, sql1, sql2=sql2)


def test_array_sum():
    """
    {
    "title": "test_array_sum",
    "describe": "array_sum函数测试, ARRAY_SUM(ARRAY<T> a) 求array中所有元素的和",
    "tag": "function,p1"
    }
    """
    client = common.get_client()
    # types
    support_k = ['a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9']
    not_support_k = ['a10', 'a11', 'a12', 'a13', 'a14']
    sql = 'select array_sum({0}) from {1}.{2}'
    for c in support_k:
        util.assert_return(True, None, client.execute, sql.format(c, db, table))
    msg = 'No matching function with signature'
    for c in not_support_k:
        util.assert_return(False, msg, client.execute, sql.format(c, db, table))
    sql1 = 'select k1, array_sum(a1), array_sum(a2), array_sum(a3), array_sum(a4), array_sum(a5), ' \
           'array_sum(a6), array_sum(a7), array_sum(a8), array_sum(a9) from %s.%s order by k1' % (db, table)
    sql2 = 'select k1, 2, sum(k1), sum(k2), sum(k3), sum(k4), sum(cast(k4 as largeint) * 10), ' \
           'sum(k5), sum(k9), sum(k8) from test_query_qa.test group by k1 order by k1'
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select array_sum(a1), array_sum(a2), array_sum(a3), array_sum(a4), array_sum(a5), ' \
           'array_sum(a6), array_sum(a7), array_sum(a8), array_sum(a9) from %s.%s where k1 = 0' % (db, table_2)
    sql2 = 'select 2, sum(k1), sum(k2), sum(k3), sum(k4), sum(cast(k4 as largeint) * 10), sum(k5), ' \
           'sum(k9), sum(k8) from test_query_qa.baseall'
    common.check2(client, sql1, sql2=sql2)
    sql2 = 'select null, null, null, null,null, null, null, null, null'
    for k in (2, 3, 4):
        sql1 = 'select array_sum(a1), array_sum(a2), array_sum(a3), array_sum(a4), array_sum(a5), ' \
               'array_sum(a6), array_sum(a7), array_sum(a8), array_sum(a9) from %s.%s where k1 = %s' \
               % (db, table_2, k)
        common.check2(client, sql1, sql2=sql2)
    sql1 = 'select array_sum(a1), array_sum(a2), array_sum(a3), array_sum(a4), array_sum(a5), ' \
           'array_sum(a6), array_sum(a7), array_sum(a8), array_sum(a9) from %s.%s where k1 = 1' % (db, table_2)
    sql2 = 'select 1, 8, 0, 131073, 6750210, 6684675, 8.8, 4.3234000205993652, 124.10001'
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select array_sum([1, 2, 3.1, 0, null, -1]), array_sum([null, null]), array_sum([]), array_sum([-1, 1.0])'
    sql2 = 'select 5.1, null, null, 0'
    common.check2(client, sql1, sql2=sql2)
    # where
    sql1 = 'select k1, array_sum(a3) from %s.%s where array_sum(a3) > 0 order by k1' % (db, table)
    sql2 = 'select k1, sum(k2) from test_query_qa.test group by k1 having sum(k2) > 0 order by k1'
    common.check2(client, sql1, sql2=sql2)


def test_array_product():
    """
    {
    "title": "test_array_product",
    "describe": "array_product函数测试, ARRAY_PRODUCT(ARRAY<T> a) 求array中所有元素的乘积值，仅支持向量化",
    "tag": "function,p1"
    }
    """
    client = common.get_client()
    ret = client.show_variables('enable_vectorized_engine')
    if len(ret) == 1 and ret[0][1] != 'true':
        raise pytest.skip("enable_vectorized_engine is false, skip array_product case")
    # types
    support_k = ['a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9']
    not_support_k = ['a10', 'a11', 'a12', 'a13', 'a14']
    sql = 'select array_product({0}) from {1}.{2}'
    for c in support_k:
        util.assert_return(True, None, client.execute, sql.format(c, db, table))
    msg = 'No matching function with signature'
    for c in not_support_k:
        util.assert_return(False, msg, client.execute, sql.format(c, db, table))

    sql1 = 'select array_product(a1), array_product(a2), array_product(a3), array_product(a4), ' \
           'array_product(a5), array_product(a6), array_product(a7), array_product(a8), ' \
           'array_product(a9) from %s.%s where k1 = 1' % (db, table_2)
    sql2 = 'select 1, 12.0, 0, 17179082760.0, 9.222949828684677e+21, 1.68877255163904e+17, ' \
           '16.34472, 3.748080116434096, 135.300011'
    common.check2(client, sql1, sql2=sql2)

    sql1 = 'select array_product([1.12, 3.45, 4.23])'
    sql2 = 'select 1.12 * 3.45 * 4.23'
    common.check2(client, sql1, sql2=sql2)

    sql1 = 'select k1, array_product(a1) from %s.%s order by k1' % (db, table_2)
    sql2 = 'select 0, 0 union select 1, 1 union select 2, null union select 3, null ' \
           'union select 3, null union select 4, null'
    common.check2(client, sql1, sql2=sql2, forced=True)

    sql1 = 'select array_product([0,1, -0, 1]), array_product([1, null, 3.1, -1]), ' \
           'array_product([null]), array_product([])'
    sql2 = 'select 0, -3.1, null, null'
    common.check2(client, sql1, sql2=sql2)

    # where
    sql1 = 'select k1, array_product(a2) from %s.%s where array_product(a2) > 0 order by k1' % (db, table_2)
    sql2 = 'select 0, 1307674368000 union select 1, 12'
    common.check2(client, sql1, sql2=sql2, forced=True)

    # null
    sql = 'select array_product(null)'
    msg = 'No matching function with signature: array_product(boolean)'
    util.assert_return(False, msg, client.execute, sql)


def test_array_sort():
    """
    {
    "title": "test_array_sort",
    "describe": "array_sort函数测试 ARRAY<T> array_sort(ARRAY<T> arr) 对array中所有元素排序并返回array",
    "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    init(database_name, table_name)
    client = common.get_client()
    table_2 = table_name + '_1'
    # types
    support_k = ['a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9', 'a10', 'a11', 'a12', 'a13', 'a14']
    sql = 'select array_sort({0}) from {1}.{2}'
    for c in support_k:
        util.assert_return(True, None, client.execute, sql.format(c, database_name, table_name))
    sql1 = 'select k1, array_sort(a1), array_sort(a2), array_sort(a3), array_sort(a4), array_sort(a5), ' \
          'array_sort(a6), array_sort(a7), array_sort(a8), array_sort(a9), array_sort(a10), ' \
           'array_sort(a11), array_sort(a12), array_sort(a13), array_sort(a14) ' \
           'from %s.%s order by k1' % (database_name, table_name)
    sql2 = "select k1, [0, 0, 1, 1], concat('[', group_concat(CAST(`k1` AS CHARACTER) ORDER BY `k1`), ']') a2, " \
           "concat('[', group_concat(CAST(`k2` AS CHARACTER) ORDER BY `k2`), ']') a3," \
           "concat('[', group_concat(CAST(`k3` AS CHARACTER) ORDER BY `k3`), ']') a4," \
           "concat('[', group_concat(CAST(`k4` AS CHARACTER) ORDER BY `k4`), ']') a5," \
           "concat('[', group_concat(CAST(cast(k4 as largeint) * 10 AS CHARACTER) ORDER BY `k4`), ']') a6," \
           "concat('[', group_concat(CAST(`k5` AS CHARACTER) ORDER BY `k5`), ']') a7," \
           "concat('[', group_concat(CAST(`k9` AS CHARACTER) ORDER BY `k9`), ']') a8," \
           "concat('[', group_concat(CAST(`k8` AS CHARACTER) ORDER BY `k8`), ']') a9," \
           "concat('[', group_concat(CAST(`k10` AS CHARACTER) ORDER BY `k10`), ']') a10," \
           "concat('[', group_concat(CAST(`k11` AS CHARACTER) ORDER BY `k11`), ']') a11," \
           "concat(\"['\", group_concat(k6, \"', '\" ORDER BY `k6`), \"']\") a12," \
           "concat(\"['\", group_concat(k7, \"', '\" ORDER BY `k7`), \"']\") a13," \
           "concat(\"['\", group_concat(k7, \"', '\" ORDER BY `k7`), \"']\") a14 " \
           "from test_query_qa.test group by k1 order by k1"
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select array_sort(a1) s1, array_sort(a2) s2, array_sort(a3) s3, array_sort(a4) s4, array_sort(a5) s5,' \
           ' array_sort(a6) s6, array_sort(a7) s7, array_sort(a8) s8, array_sort(a9) s9, array_sort(a10) s10, ' \
           'array_sort(a11) s11, array_sort(a12) s12, array_sort(a13) s13, array_sort(a14) s14 ' \
           'from %s.%s where k1 = 1 ' % (database_name, table_2)
    sql2 = 'select [NULL, 1] s1, [NULL, 1, 3, 4] s2, [NULL, -32767, 0, 32767] s3, [NULL, 4, 65534, 65535] s4, ' \
           '[NULL, 5, 65534, 65535, 65536, 6553600] s5, [NULL, 6, 65534, 65535, 6553600] s6, ' \
           '[NULL, 1.12, 3.45, 4.23] s7, [NULL, 1.2, 3.1234] s8, [NULL, 1.1, 123.00001] s9, ' \
           '[NULL, "2000-01-01", "2022-07-13"] s10, [NULL, "2022-07-13 12:30:00", "2022-07-13 12:30:00"] s11, ' \
           '[NULL, "", "hello char", "tds"] s12, [NULL, "", "hello varchar"] s13, [NULL, "", "jedsd"] s14'
    common.check_by_sql(sql1, sql2, client=client, s10=palo_types.ARRAY_DATE, s11=palo_types.ARRAY_DATETIME)
    sql1 = 'select array_sort([]), array_sort([1, null, 2, null, 0, -0]), array_sort([null]), array_sort(null)'
    sql2 = 'select [], [null, null, 0, 0, 1, 2], [null], null'
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_size():
    """
    {
    "title": "test_size",
    "describe": "size函数测试， BIGINT size(ARRAY<T> arr), BIGINT cardinality(ARRAY<T> arr) 求array中元素数量",
    "tag": "function,p1"
    }
    """
    client = common.get_client()
    # types
    support_k = ['a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9', 'a10', 'a11', 'a12', 'a13', 'a14']
    sql = 'select size({0}) from {1}.{2}'
    for c in support_k:
        util.assert_return(True, None, client.execute, sql.format(c, db, table))
    sql1 = 'select k1, size(a1), size(a2), size(a3), size(a4), size(a5), size(a6), ' \
           'size(a7), size(a8), size(a9), size(a10), size(a11), size(a12), size(a13), ' \
           'size(a14) from %s.%s order by k1' % (db, table)
    sql2 = 'select k1, 4, count(k1), count(k2), count(k3), count(k4), count(k4), count(k5), ' \
           'count(k9), count(k8), count(k10), count(k11), count(k6), ' \
           'count(k7), count(k7) from test_query_qa.test group by k1 order by k1'
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select size(a1), size(a2), size(a3), size(a4), size(a5), size(a6), size(a7), ' \
           'size(a8), size(a9), size(a10), size(a11), size(a12), size(a13), size(a14) ' \
           'from %s.%s where k1 = 0' % (db, table_2)
    sql2 = 'select 4, count(k1), count(k2), count(k3), count(k4), count(k4), count(k5), ' \
           'count(k9), count(k8), count(k10), count(k11), count(k6), count(k7), count(k7) ' \
           'from test_query_qa.baseall'
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select size(a1), size(a2), size(a3), size(a4), size(a5), size(a6), size(a7), ' \
           'size(a8), size(a9), size(a10), size(a11), size(a12), size(a13), size(a14) ' \
           'from %s.%s where k1 = 1' % (db, table_2)
    sql2 = 'select 2, 4, 4, 4, 6, 5, 4, 3, 3, 3, 3, 4, 3, 3'
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select size(a1), size(a2), size(a3), size(a4), size(a5), size(a6), size(a7), ' \
           'size(a8), size(a9), size(a10), size(a11), size(a12), size(a13), size(a14) ' \
           'from %s.%s where k1 = 2' % (db, table_2)
    sql2 = 'select 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0'
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select size(a1), size(a2), size(a3), size(a4), size(a5), size(a6), size(a7), ' \
           'size(a8), size(a9), size(a10), size(a11), size(a12), size(a13), size(a14) ' \
           'from %s.%s where k1 = 3' % (db, table_2)
    sql2 = 'select 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1'
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select size(a1), size(a2), size(a3), size(a4), size(a5), size(a6), size(a7), ' \
           'size(a8), size(a9), size(a10), size(a11), size(a12), size(a13), size(a14) ' \
           'from %s.%s where k1 = 4' % (db, table_2)
    sql2 = 'select null, null, null, null, null, null, null, null, null, null, null, null, null, null'
    common.check2(client, sql1, sql2=sql2)

    sql1 = 'select k1 from %s.%s where size(a2) = 0' % (db, table_2)
    sql2 = 'select 2'
    common.check2(client, sql1, sql2=sql2) 

    sql1 = 'select size([]), size(null), size([null]), size([1, 2, null, null, 1, 2])'
    sql2 = 'select 0, null, 1, 6'
    common.check2(client, sql1, sql2=sql2)


def test_array_distinct():
    """
    {
    "title": "test_array_distinct",
    "describe": "array_distinct函数测试，ARRAY<T> array_distinct(ARRAY<T> arr) 去除array中的重复元素并返回array",
    "tag": "function,p1"
    }
    """
    client = common.get_client()
    # types
    support_k = ['a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9', 'a10', 'a11', 'a12', 'a13', 'a14']
    sql = 'select array_distinct({0}) from {1}.{2}'
    for c in support_k:
        util.assert_return(True, None, client.execute, sql.format(c, db, table))
        
    sql1 = 'select k1, array_distinct(a1), array_distinct(a2), array_distinct(a3), array_distinct(a4), ' \
           'array_distinct(a5), array_distinct(a6), array_distinct(a7), array_distinct(a8), ' \
           'array_distinct(a9), array_distinct(a10), array_distinct(a11), array_distinct(a12), ' \
           'array_distinct(a13), array_distinct(a14) from %s.%s order by k1' % (db, table)
    sql2 = 'select k1, [0, 1] as a1, array_sort(collect_set(k1)) as a2, array_sort(collect_set(k2)) as a3, ' \
           'array_sort(collect_set(k3)) as a4, array_sort(collect_set(k4)) as a5, ' \
           'array_sort(collect_set(cast(k4 as largeint) * 10)) as a6, array_sort(collect_set(k5)) as a7, ' \
           'array_sort(collect_set(k9)) as a8, array_sort(collect_list(k8)) as a9, ' \
           'array_sort(collect_set(k10)) as a10, array_sort(collect_set(k11)) as a11, ' \
           'array_sort(collect_set(k6)) as a12, array_sort(collect_set(k7)) as a13, ' \
           'array_sort(collect_set(k7)) as a14 from test_query_qa.test group by k1 order by k1'
    common.check2(client, sql1, sql2=sql2)

    sql1 = 'select array_distinct(a1), array_distinct(a2), array_distinct(a3), array_distinct(a4), ' \
           'array_distinct(a5), array_distinct(a6), array_distinct(a7), array_distinct(a8), ' \
           'array_distinct(a10), array_distinct(a11), array_distinct(a12), ' \
           'array_distinct(a13), array_distinct(a14) from %s.%s where k1 = 0' % (db, table_2)
    sql2 = 'select [0, 1] as a1, array_sort(collect_set(k1)) as a2, array_sort(collect_set(k2)) as a3, ' \
           'array_sort(collect_set(k3)) as a4, array_sort(collect_set(k4)) as a5, ' \
           'array_sort(collect_set(cast(k4 as largeint) * 10)) as a6, array_sort(collect_set(k5)) as a7, ' \
           'array_sort(collect_set(k9)) as a8, array_sort(collect_set(k10)) as a10, ' \
           'array_sort(collect_set(k11)) as a11, array_sort(collect_set(k6)) as a12, ' \
           'array_sort(collect_set(k7)) as a13, array_sort(collect_set(k7)) as a14 from test_query_qa.baseall'
    common.check2(client, sql1, sql2=sql2)

    sql1 = 'select e1 from %s.%s lateral view explode(array_distinct(a10)) tmp as e1 where k1 = 0 order by e1' \
           % (db, table_2)
    sql2 = ' select distinct k10 from test_query_qa.baseall order by 1' 
    common.check2(client, sql1, sql2=sql2)

    sql1 = 'select array_distinct(a1), array_distinct(a2), array_distinct(a3), array_distinct(a4), ' \
           'array_distinct(a5), array_distinct(a6), array_distinct(a7), array_distinct(a8), ' \
           'array_distinct(a9), array_distinct(a10), array_distinct(a11), array_distinct(a12), ' \
           'array_distinct(a13), array_distinct(a14) from %s.%s where k1 = 1' % (db, table_2)
    sql2 = "select [NULL, 1] a1, [1, NULL, 3, 4] a2, [0, NULL, 32767, -32767] a3, " \
           "[4, NULL, 65534, 65535] a4, [65534, NULL, 65535, 5, 65536, 6553600] a5, " \
           "[65534, 6, 65535, 6553600, NULL] a6, [1.12, 3.45, 4.23, NULL] a7, " \
           "[3.1234, 1.2, NULL] a8, [123.00001, NULL, 1.1] a9, " \
           "cast(['2022-07-13', NULL, '2000-01-01'] as array<date>) a10, " \
           "cast([NULL, '2022-07-13 12:30:00'] as array<datetime>) a11, " \
           "['', 'hello char', 'tds', NULL] a12, [NULL, 'hello varchar', ''] a13, [NULL, '', 'jedsd'] a14"
    common.check2(client, sql1, sql2=sql2)
 
    sql1 = "select array_distinct([]), array_distinct([null, null]), " \
           "array_distinct([4, 1, null, 1, 2, 3, 4, null, null])"
    sql2 = "select [], [null], [4, 1, null, 2, 3]"
    common.check2(client, sql1, sql2=sql2)
    sql1 = "select array_distinct(a2) from %s.%s where  k1 > 0 order by k1" % (db, table_2)
    sql2 = "select a2 from %s.%s where k1 > 0 order by k1" % (db, table_2)
    common.check2(client, sql1, sql2=sql2)


def test_array_contains():
    """
    {
    "title": "test_array_contains",
    "describe": "array_contains函数测试，BOOLEAN array_contains(ARRAY<T> arr, T value) 判断数字中是否包含某个元素",
    "tag": "function,p1"
    }
    """
    client = common.get_client()
    db = 'test_array_select_test_table_db'
    table = 'test_array_select_test_table_tb'
    table_2 = 'test_array_select_test_table_tb_1'
    # types
    support_k = ['a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9', 'a12', 'a13', 'a14', 'a10', 'a11']
    sql = 'select array_contains({0}, null) from {1}.{2}'
    for c in support_k:
        sql1 = sql.format(c, db, table)
        sql2 = 'select 0 from test_query_qa.test group by k1'
        common.check2(client, sql1, sql2=sql2)
    sql1 = 'select array_contains(a1, 0), array_contains(a2, 1), array_contains(a3, 255), ' \
           'array_contains(a4, -2147483647), array_contains(a5, -9223372036854775807), ' \
           'array_contains(a6, 1234560), array_contains(a7, 0), array_contains(a8, 6.333), ' \
           'array_contains(a9, -0), array_contains(a10, "1989-03-21"), ' \
           'array_contains(a11, cast("2015-04-02 00:00:00" as datetime)), ' \
           'array_contains(a12, "false"), array_contains(a13, "wangjuoo4"), ' \
           'array_contains(a14, "yanvjldjlll") from %s.%s' % (db, table_2)
    sql2 = 'select 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 union all ' \
           'select 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 union all ' \
           'select 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 union all ' \
           'select 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 union all ' \
           'select null, null, null, null, null, null, null, null, null, null, null, null, null, null' 
    common.check2(client, sql1, sql2=sql2, forced=True)

    sql1 = 'select k1 from %s.%s where array_contains(a2, 10)' % (db, table)
    sql2 = 'select 10'
    common.check2(client, sql1, sql2=sql2)

    sql1 = 'select array_contains(a10, array_max(a10)),  array_contains(a8, array_min(a8)), ' \
           'array_contains(a7, a7[0]) from %s.%s order by k1' % (db, table)
    sql2 = 'select 1, 1, 0 from test_query_qa.test group by k1'
    common.check2(client, sql1, sql2=sql2)

    sql1 = "select array_contains(a, '1'), array_contains(a, null), array_contains(a, '44') " \
           "from (select [1, 2, 3, null] a) tmk"
    sql2 = "select 1, 1, 0"
    common.check2(client, sql1, sql2=sql2)
    sql1 = "select array_contains([], 1), array_contains([null], 1), array_contains(null, 1), " \
           "array_contains([1, 2, 3], null)"
    sql2 = "select 0, 0, null, 0"
    common.check2(client, sql1, sql2=sql2)
    sql1 = "select array_contains(a10, null) from %s.%s limit 1" % (db, table)
    sql2 = 'select 0'
    common.check2(client, sql1, sql2=sql2)
    sql1 = "select array_contains(a11, null) from %s.%s" % (db, table_2)
    sql2 = "select 1 union all select 1 union all select null union all select 0 union all select 0"
    common.check2(client, sql1, sql2=sql2, forced=True)


def test_array_position():
    """
    {
    "title": "test_array_position",
    "describe": "array_position函数测试",
    "tag": "function,p1"
    }
    """
    # BIGINT array_position(ARRAY<T> arr, T value) 返回arr中第一次出现元素value的位置
    # 从1开始计算
    client = common.get_client()
    # types
    support_k = ['a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9', 'a10', 'a11', 'a12', 'a13', 'a14'] 
    sql = 'select array_position({0}, null) from {1}.{2}'
    for c in support_k:
        sql1 = sql.format(c, db, table)
        sql2 = 'select 0 from test_query_qa.test group by k1'
        common.check2(client, sql1, sql2=sql2)

    sql1 = 'select array_position(a1, 0), array_position(a2, 1), array_position(a3, 255), ' \
           'array_position(a4, -2147483647), array_position(a5, -9223372036854775807), ' \
           'array_position(a6, 1234560), array_position(a7, 0), array_position(a8, 6.333), ' \
           'array_position(a9, -0), array_position(a10, cast("1989-03-21" as date)), ' \
           'array_position(a11, cast("2015-04-02 00:00:00" as datetime)), ' \
           'array_position(a12, "false"), array_position(a13, "wangjuoo4"), ' \
           'array_position(a14, "yanvjldjlll") from %s.%s' % (db, table_2)
    sql2 = 'select 1, 1, 3, 1, 1, 5, 4, 8, 4, 3, 12, 1, 6, 13 union all ' \
           'select 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 union all ' \
           'select 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 union all ' \
           'select 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 union all ' \
           'select null, null, null, null, null, null, null, null, null, null, null, null, null, null'      
    common.check2(client, sql1, sql2=sql2, forced=True)

    sql1 = 'select k1 from %s.%s where array_position(a2, 1) in (1, 2, 3, 4, 5)' % (db, table)
    sql2 = 'select 1'
    common.check2(client, sql1, sql2=sql2, forced=True)
    sql1 = 'select array_position(a, 1), array_position(a, null), array_position(a, -10) ' \
           'from (select [1, 1, null, -10, null, -10] a) tmp'
    sql2 = 'select 1, 3, 4'
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select array_position([], 1), array_position([null], 1), array_position([1, 2, 3], null), ' \
           'array_position([1, 2, 3], 4), array_position([null, null], null)'
    sql2 = 'select 0, 0, 0, 0, 1'
    common.check2(client, sql1, sql2=sql2)
    sql1 = "select array_position(a13, null) from %s.%s" % (db, table_2)
    sql2 = "select 1 union all select 1 union all select null union all select 0 union all select 0"
    common.check2(client, sql1, sql2=sql2, forced=True)


def test_element_at():
    """
    {
    "title": "test_element_at",
    "describe": "element_at函数测试",
    "tag": "function,p1"
    }
    """
    # T element_at(ARRAY<T> arr, BIGINT position), 返回arr中位置为position的值，同arr[position]
    # enable_vectorized_engine为true
    client = common.get_client()
    # types
    support_k = ['a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9', 'a10', 'a11', 'a12', 'a13', 'a14']
    sql1 = 'select k1, element_at({0}, 1) from {1}.{2} order by k1'
    sql2 = 'select k1, {0}[1] from {1}.{2} order by k1'
    for c in support_k:
        common.check2(client, sql1=sql1.format(c, db, table), 
                      sql2=sql2.format(c, db, table))
    sql1 = 'select element_at(a1, 0), element_at(a2, 1), element_at(a3, 2), element_at(a4, 3), ' \
           'element_at(a5, 4), element_at(a6, 5), element_at(a7, 6), element_at(a8, 7), ' \
           'element_at(a9, 8), element_at(a10, 9), element_at(a11, 10), element_at(a12, 11), ' \
           'element_at(a13, 12), element_at(a14, 13) from %s.%s order by k1' % (db, table_2)
    sql2 = 'select null, 1, -32767, 103, -11011903, 1234560, 0.666, 4.336, 0.1, ' \
           'cast("2015-01-01" as date), cast("2015-03-13 12:36:38" as datetime), ' \
           '"true", "yanavnd", "yanvjldjlll" union all ' \
           'select null, 1, null, 65534, 5, null, null, null, null, null, null, null, null, null union all ' \
           'select null, null, null, null, null, null, null, null, null, null, null, null, null, null union all ' \
           'select null, null, null, null, null, null, null, null, null, null, null, null, null, null union all ' \
           'select null, null, null, null, null, null, null, null, null, null, null, null, null, null'
    common.check2(client, sql1, sql2=sql2, forced=True)
    sql1 = 'select k1 from %s.%s where element_at(a2, 1) is not null' % (db, table_2)
    sql2 = 'select 1 union select 0'
    common.check2(client, sql1, sql2=sql2, forced=True)
    sql1 = 'select k1 from %s.%s where a2[1] is null' % (db, table_2)
    sql2 = 'select 4 union select 2 union select 3'
    common.check2(client, sql1, sql2=sql2, forced=True)
    sql1 = 'select element_at(a, 0), element_at(a, 1), element_at(a, 100), element_at(a, -1), ' \
           'element_at(a, null) from (select [1, 2, 3] a) tmp' 
    sql2 = 'select null, 1, null, 3, null'
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select a[0], a[1], a[100], a[-1], a[null] from (select [1, 2, 3] a) tmp'
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select element_at([], 1), element_at([null], 1)'
    sql2 = 'select null, null'
    common.check2(client, sql1=sql1, sql2=sql2)
    sql = 'select element_at(null, 1)'
    msg = 'No matching function with signature: element_at(null_type, tinyint(4))'
    util.assert_return(False, msg, client.execute, sql)


def test_array_slice():
    """
    {
    "title": "test_array_slice",
    "describe": "array_slice函数测试",
    "tag": "function,p1"
    }
    """
    # ARRAY<T> array_slice(ARRAY<T> arr, BIGINT off, BIGINT len)，返回从指定位置开始的执行长度的子数组
    # enable_vectorized_engine
    client = common.get_client()
    # types
    support_k = ['a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9', 'a10', 'a11', 'a12', 'a13', 'a14']
    sql1 = 'select array_slice({0}, 1, 2) from {1}.{2} order by k1'
    sql2 = 'select {0}[1:2] from {1}.{2} order by k1'
    for c in support_k:
        common.check2(client, sql1=sql1.format(c, db, table), sql2=sql2.format(c, db, table))
    sql1 = 'select array_slice(a1, 1, 1) b1, array_slice(a2, 2, 2) b2, ' \
           'array_slice(a3, 1, 1) b3, array_slice(a4, 1, 2) b4, ' \
           'array_slice(a5, 1, 1) b5, array_slice(a6, 2, 1) b6, ' \
           'array_slice(a7, 2, 2) b7, array_slice(a8, 1, 1) b8, ' \
           'array_slice(a9, 1, 1) b9, array_slice(a10, 2, 1) b10, ' \
           'array_slice(a11, 1, 1) b11, array_slice(a12, 2, 2) b12, ' \
           'array_slice(a13, 1, 1) b13, array_slice(a14, 1, 2) b14 from %s.%s order by k1' % (db, table_2)
    sql2 = 'select [0] b1, [2, 3] b2, [-32767] b3, [-2147483647, -2147483647] b4, ' \
           '[-9223372036854775807] b5, [-92233720368547758070] b6, [-258.369, -0.123] b7, ' \
           '[-365] b8, [-123456.54] b9,  ["1988-03-21"] b10, ["1901-01-01 00:00:00"] b11, ' \
           '["false", "false"] b12, [""] b13, ["", " "] b14 union all ' \
           'select [null], [null, 3], [0], [4, null], [65534], [6], [3.45, 4.23], [3.1234], ' \
           '[123.00001], [null], [null], ["hello char", "tds"], [null], [null, ""] union all ' \
           'select [], [], [], [], [], [], [], [], [], [], [], [], [], [] union all ' \
           'select [null], [], [null], [null], [null], [], [], [null], [null], [], [null], ' \
           '[], [null], [null] union all ' \
           'select null, null, null, null, null, null, null, null, null, null, null, null, null, null'
    common.check_by_sql(sql1, sql2, client=client, b10=palo_types.ARRAY_DATE, b11=palo_types.ARRAY_DATETIME)

    sql1 = 'select array_slice([1, 2, 3], 1, 1), array_slice([1, 2, 3], 1, 0), array_slice([1, 2, 3], 1), ' \
           'array_slice([1, 2, 3], null, null), array_slice([1, 2, 3], 1, 100), array_slice([1, 2, 3], 1, -2), ' \
           'array_slice([1, 2, 3], -1, 2)'
    sql2 = 'select [1], [], [1, 2, 3], null, [1, 2, 3], [], [3]'
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select array_slice([null], 1, 1), array_slice([1, null, 2], -2, 3), array_slice([], 1, 2), ' \
           'array_slice([], -1, 1), array_slice([null], -1, 2), array_slice([null], 100)'
    sql2 = ' select [null], [null, 2], [], [], [null], []'
    common.check2(client, sql1, sql2=sql2)


def test_array_remove():
    """
    {
    "title": "test_array_remove",
    "describe": "array_remove函数测试",
    "tag": "function,p1"
    }
    """
    #  ARRAY<T> array_remove(ARRAY<T> arr, T val)，返回arr中删除所有val元素的子数组
    client = common.get_client()
    # types
    support_k = ['a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9', 'a10', 'a11', 'a12', 'a13', 'a14']
    sql = 'select array_remove({0}, {0}[1]) from {1}.{2}'
    for c in support_k:
        util.assert_return(True, None, client.execute, sql.format(c, db, table))
    sql1 = 'select array_remove(a1, 0), array_remove(a2, k1), array_remove(a3, 4), ' \
           'array_remove(a4, 4), array_remove(a5, 4), array_remove(a6, 4), ' \
           'array_remove(a7, 4), array_remove(a8, 4), array_remove(a9, 4), ' \
           'array_remove(a10, date("2022-11-11")), array_remove(a11, date("2022-11-11")), ' \
           'array_remove(a12, 100), array_remove(a13, 100), array_remove(a14, 200) ' \
           'from %s.%s order by k1' % (db, table)
    sql2 = 'select [1, 1], [], a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14 from %s.%s order by k1' \
           % (db, table)
    common.check2(client, sql1, sql2=sql2)

    sql1 = 'select array_remove(a1, 1), array_remove(a2, 1), array_remove(a3, 32767), ' \
           'array_remove(a4, 65535), array_remove(a5, 6553600), array_remove(a6, 6553600), ' \
           'array_remove(a7, 4.23), array_remove(a8, 3.1234), array_remove(a9, 123.00001), ' \
           'array_remove(a10, cast("2000-01-01" as date)), array_remove(a11, "2022-07-13 12:30:00"), ' \
           'array_remove(a12, "hello char"), array_remove(a13, ""), array_remove(a14, "jedsd") ' \
           'from %s.%s where k1 > 0' % (db, table_2)
    sql2 = 'select [null] a1, [null, 3, 4] a2, [0, NULL, -32767] a3, [4, NULL, 65534] a4, ' \
           '[65534, NULL, 65535, 5, 65536] a5, [65534, 6, 65535, NULL] a6, ' \
           '[1.12, 3.45, NULL] a7, [1.2, NULL] a8, [NULL, 1.1] a9, ' \
           '["2022-07-13", NULL] a10, [NULL] a11, ' \
           '["", "tds", NULL] a12, [NULL, "hello varchar"] a13, [NULL, ""] a14 union all ' \
           'select [], [], [], [], [], [], [], [], [], [], [], [], [], [] union all ' \
           'select [null], [null], [null], [null], [null], [null], [null], [null], [null], ' \
           '[null], [null], [null], [null], [null] union all ' \
           'select null, null, null, null, null, null, null, null, null, null, null, null, null, null'
    common.check_by_sql(sql1, sql2, client=client, a10=palo_types.ARRAY_DATE)
    sql1 = 'select array_remove(["", "abc", "dfef"], null), array_remove([1, 2, 3], "1"), ' \
           'array_remove(["", "abc", "dfef"], "abcd")'
    sql2 = 'select null, [2, 3], ["", "abc", "dfef"]'
    common.check2(client, sql1, sql2=sql2)
    # 类型匹配case
    sql1 = "select array_remove([1, 2, 3, 4], 1.3), array_remove([1, 2, 3, 4.0], 1.3)"
    sql2 = "select [2, 3, 4], [1, 2, 3, 4]"
    common.check2(client, sql1, sql2=sql2)


def test_array_join():
    """
    {
    "title": "test_array_join",
    "describe": "array_join函数测试",
    "tag": "function,p1"
    }
    """
    # VARCHAR array_join(ARRAY<T> arr, VARCHAR sep[, VARCHAR null_replace])
    client = common.get_client()
    # types
    support_k = ['a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9', 'a10', 'a11', 'a12', 'a13', 'a14']
    sql = 'select array_join({0}, "_") from {1}.{2}'
    for c in support_k:
        util.assert_return(True, None, client.execute, sql.format(c, db, table))
    sql1 = 'select k1, array_join(a12, "_"), array_join(a13, "_"), array_join(a14, "_") ' \
           'from %s.%s order by k1' % (db, table)
    sql2 = 'select k1, group_concat(k6, "_" order by k6), group_concat(k7, "_" order by k7), ' \
           'group_concat(k7, "_" order by k7) from test_query_qa.test group by k1 order by k1'
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select k1, array_join(a1, "_"), array_join(a2, "_"), array_join(a3, "_"), ' \
           'array_join(a4, "_"), array_join(a5, "_"), array_join(a6, "_"), ' \
           'array_join(a7, "_"), array_join(a10, "_"), array_join(a11, "_") ' \
           'from %s.%s order by k1' % (db, table)
    sql2 = 'select k1, "0_1_1_0", group_concat(cast(k1 as string), "_" order by k1) a2, ' \
           'group_concat(cast(k2 as string), "_" order by k2) a3, ' \
           'group_concat(cast(k3 as string), "_" order by k3) a4, ' \
           'group_concat(cast(k4 as string), "_" order by k4) a5, ' \
           'group_concat(cast(cast(k4 as largeint) * 10 as string), "_" order by k4) a6, ' \
           'group_concat(cast(k5 as string), "_" order by k5) a7, ' \
           'group_concat(cast(k10 as string), "_" order by k10) a10, ' \
           'group_concat(cast(k11 as string), "_" order by k11) a11 ' \
           'from test_query_qa.test group by k1 order by k1'
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select array_join(a12, "_"), array_join(a13, "_"), array_join(a14, "_") ' \
           'from %s.%s where k1 = 0' % (db, table_2)
    sql2 = 'select group_concat(k6, "_" order by k6), group_concat(k7, "_" order by k7), ' \
           'group_concat(k7, "_" order by k7) from test_query_qa.baseall'
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select array_join(a12, "_", ">.<"), array_join(a13, "_"), array_join(a14, "_") ' \
           'from %s.%s where k1 > 0' % (db, table_2)
    sql2 = 'select "", "", "" union all select null, null, null union all ' \
           'select "_hello char_tds_>.<", "hello varchar_", "_jedsd" union all ' \
           'select ">.<", "", ""'
    common.check2(client, sql1, sql2=sql2, forced=True)
    sql1 = 'select count(*) from %s.%s where array_join(a1, ",") = "0,1,1,0"' % (db, table)
    sql2 = 'select count(distinct k1) from test_query_qa.test'
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select array_join(a, 1, 2), array_join(a, ",", "dd"), array_join(a, null, "ppt") ' \
           'from (select [1, 2, null, 3] a) tmp'
    sql2 = 'select "1121213", "1,2,dd,3", null'
    common.check2(client, sql1, sql2=sql2)
    sql1 = "select array_join([null, null], '\"', 'd'), array_join([null], '\"', 'd'), " \
           "array_join([], '\"', 'd'), array_join([null, null, 1, null, null], '\"', 'd'), " \
           "array_join([null, null, 1, null, null], '-')"
    sql2 = "select 'd\"d', 'd', '', 'd\"d\"1\"d\"d', '1'"
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select array_join(["", "1", "2"], "_")'
    sql2 = 'select "_1_2"'
    common.check2(client, sql1, sql2=sql2)


def test_array_except():
    """
    {
    "title": "test_array_except",
    "describe": "array_except函数测试",
    "tag": "function,p1"
    }
    """
    # ARRAY<T> array_except(ARRAY<T> array1, ARRAY<T> array2) 所有在array1内但不在array2内的元素，不包含重复项
    # enable_vectorized_engine
    client = common.get_client()
    # types
    support_k = ['a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9', 'a10', 'a11', 'a12', 'a13', 'a14']
    line1 = 'select array_except({0}, {0}) from {1}.{2} order by k1'
    line2 = 'select [] from test_query_qa.test group by k1'
    for c in support_k:
        common.check2(client, sql1=line1.format(c, db, table), sql2=line2)
    sql1 = 'select k1, array_except(a1, [1]), array_except(a2, [null]), array_except(a3, [null]), ' \
           'array_except(a4, [null]), array_except(a5, [null]), array_except(a6, [null]), ' \
           'array_except(a7, [null]), array_except(a8, [null]), array_except(a9, [null]), ' \
           'array_except(a10, [null]), array_except(a11, [null]), array_except(a12, [null]), ' \
           'array_except(a13, [null]), array_except(a14, [null]) from %s.%s where k1 = 0 order by k1' % (db, table_2)
    sql2 = 'select k1, [0], array_distinct(a2), array_distinct(a3), array_distinct(a4), ' \
           'array_distinct(a5), array_distinct(a6), array_distinct(a7), array_distinct(a8), ' \
           'array_distinct(a9), array_distinct(a10), array_distinct(a11), array_distinct(a12), ' \
           'array_distinct(a13), array_distinct(a14) from %s.%s where k1=0 order by k1' % (db, table_2)
    common.check2(client, sql1=sql1, sql2=sql2)
    sql1 = 'select array_except([null], [1])'
    sql2 = 'select [null]'
    common.check2(client, sql1=sql1, sql2=sql2)
    sql1 = 'select array_except(a3, [null]) from %s.%s where k1 != 0 order by k1' % (db, table_2)
    sql2 = 'select "[]" union all select "[]" union all select null union all select "[0, 32767, -32767]"'
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    # array不同子类型的array_except，如array<int> vs array<date>， not support
    sql = 'select array_except({0}, {1}) from {2}.{3} where k1 = 0 order by k1'
    msg = 'No matching function with signature: array_except(array<smallint(6)>, array<date>).'
    util.assert_return(False, msg, client.execute, sql.format('a3', 'a10', db, table))
    sql1 = "select array_except(['1', '2', '3', null], [1, 1, 1, 'hek']) from test_query_qa.baseall"
    sql2 = "select ['2', '3', NULL] from test_query_qa.baseall;"
    common.check2(client, sql1, sql2=sql2)
    sql1 = "select array_except(['2022-07-13', null], a10) from %s.%s order by k1" % (db, table_2)
    sql2 = "select '[2022-07-13, NULL]' a union all select '[]' union all " \
           "select '[2022-07-13, NULL]' union all select '[2022-07-13]' union all select null"
    common.check_by_sql(sql1, sql2, client=client, a=palo_types.ARRAY_DATE)
    sql1 = "select array_except([], []), array_except([null], []), array_except([], [null]), " \
           "array_except([1, 2, null, 3, 4, 3, 2, null], [null, 3]), " \
           "array_except([1, 2, null, 3, 4, 3, 2, null], [null, 5, 6]), array_except([1, 2, 3], null)"
    sql2 = "select [], [null], [], [1, 2, 4], [1, 2, 3, 4], null"
    common.check2(client, sql1, sql2=sql2)
    sql1 = "select array_except([null], [1])"
    sql2 = "select [null]"
    common.check2(client, sql1, sql2=sql2)
    sql1 = "select array_except(a3, a7) from %s.%s where k1 = 0 order by k1" % (db, table_2)
    sql2 = "select [-32767, 255, 1985, 1986, 1989, 1991, 1992, 32767]"
    common.check2(client, sql1, sql2=sql2)


def test_array_intersect():
    """
    {
    "title": "test_array_intersect",
    "describe": "array_intersect函数测试",
    "tag": "function,p1"
    }
    """
    # ARRAY<T> array_intersect(ARRAY<T> array1, ARRAY<T> array2) array1和array2的交集中的所有元素，不包含重复项
    # enable_vectorized_engine
    client = common.get_client()
    # types
    support_k = ['a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9', 'a10', 'a11', 'a12', 'a13', 'a14']
    sql1 = 'select array_intersect({0}, []) from {1}.{2}'
    sql2 = 'select [] from test_query_qa.test group by k1'
    for c in support_k:
        common.check2(client, sql1=sql1.format(c, db, table), sql2=sql2)
    sql1 = 'select k1, array_intersect(a1, [1]), array_intersect(a2, [null]), array_intersect(a3, [null]), ' \
           'array_intersect(a4, [null]), array_intersect(a5, [null]), array_intersect(a6, [null]), ' \
           'array_intersect(a7, [null]), array_intersect(a8, [null]), array_intersect(a9, [null]), ' \
           'array_intersect(a9, [null]), array_intersect(a10, [null]), array_intersect(a11, [null]), ' \
           'array_intersect(a12, [null]), array_intersect(a13, [null]), array_intersect(a14, [null]) ' \
           'from %s.%s order by k1' % (db, table)
    sql2 = 'select k1, [1], [], [], [], [], [], [], [], [], [], [], [], [], [], [] ' \
           'from test_query_qa.test group by k1 order by k1'
    common.check2(client, sql1=sql1, sql2=sql2)
    
    sql0 = 'select t1.{0}, t2.{0}, array_intersect(t1.{0}, t2.{0}) from {1}.{2} t1 cross join {1}.{2} t2 ' \
           'where t1.k1=1 and t2.k1 > 0'
    sql1 = 'select array_intersect(t1.{0}, t2.{0}) from {1}.{2} t1 cross join {1}.{2} t2 ' \
           'where t1.k1=1 and t2.k1 > 0'
    sql2 = 'select array_distinct({0}) from {1}.{2} where k1 > 0'
    for c in support_k:
        print(sql0.format(c, db, table_2))
        common.check2(client, sql1=sql1.format(c, db, table_2), sql2=sql2.format(c, db, table_2), forced=True)
    
    # array不同子类型的array_except，如array<int> vs array<date>， not support
    sql = 'select array_intersect({0}, {1}) from {2}.{3} where k1 = 0 order by k1'
    msg = 'No matching function with signature: array_intersect(array<smallint(6)>, array<date>).'
    util.assert_return(False, msg, client.execute, sql.format('a3', 'a10', db, table))

    sql1 = "select array_intersect(['1', '2', 'a', 'b', null], ['b,a', 'a', '3', '4', null])"
    sql2 = "select ['a', NULL]"
    common.check2(client, sql1=sql1, sql2=sql2)
    sql1 = "select array_intersect(['1', '2', 'a', 'b', null], ['b,a', 'a', '3', '4', null]) from test_query_qa.baseall"
    sql2 = "select ['a', NULL] from test_query_qa.baseall"
    common.check2(client, sql1=sql1, sql2=sql2)
    sql1 = "select k1 from %s.%s where size(array_intersect(a11, ['1989-03-21 13:11:00'])) = 0 " \
           "order by k1" % (db, table_2)
    sql2 = "select 1 union select 2 union select 3"
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    sql1 = "select array_intersect([1, 2, 3, 1, 2, 3], [3, 3, 3])"
    sql2 = "select [3]"
    common.check2(client, sql1=sql1, sql2=sql2)
    err = "select array_intersect([1, 2, 3, 1, 2, 3], '1,[3, 2, 5]')" 
    util.assert_return(False, 'No matching function with signature', client.execute, err)


def test_array_union():
    """
    {
    "title": "test_array_union",
    "describe": "array_union函数测试",
    "tag": "function,p1"
    }
    """
    # ARRAY<T> array_union(ARRAY<T> array1, ARRAY<T> array2) array1和array2的并集中的所有元素，不包含重复项
    # enable_vectorized_engine
    client = common.get_client()
    # types
    support_k = ['a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9', 'a10', 'a11', 'a12', 'a13', 'a14']
    sql1 = 'select array_union({0}, {0}) from {1}.{2} order by k1'
    sql2 = 'select array_distinct({0}) from {1}.{2} order by k1'
    for c in support_k:
        common.check2(client, sql1=sql1.format(c, db, table), sql2=sql2.format(c, db, table))

    sql0 = 'select t1.{0}, t2.{0}, array_union(t1.{0}, t2.{0}) from {1}.{2} t1 cross join {1}.{2} t2 ' \
           'where t1.k1=1 and t2.k1 > 0'
    sql1 = 'select array_union(t1.{0}, t2.{0}) from {1}.{2} t1 cross join {1}.{2} t2 ' \
           'where t1.k1=1 and t2.k1 > 0'
    sql2 = 'select "[1.12, 3.45, 4.23, NULL]" union all select "[1.12, 3.45, 4.23, NULL]" ' \
           'union all select "[1.12, 3.45, 4.23, NULL]" union all select null'
    c = 'a7'
    print(sql0.format(c, db, table_2))
    # select t1.a7, t2.a7, array_union(t1.a7, t2.a7) from test_array_select_test_table_db.test_array_select_test_table_tb_1 t1 cross join test_array_select_test_table_db.test_array_select_test_table_tb_1 t2 where t1.k1=1 and t2.k1 > 0;
    common.check2(client, sql1=sql1.format(c, db, table_2), sql2=sql2.format(c, db, table_2), forced=True)

    sql = "select array_union(a2, a10) from %s.%s" % (db, table)
    msg = "No matching function with signature"
    util.assert_return(False, msg, client.execute, sql)
    sql = "select array_union([1, 2, 3, 1, 2], '[1, 2, 4]')"
    util.assert_return(False, msg, client.execute, sql)

    sql1 = "select array_union([1, 2, 3, 1, 2], [1, 2, 4]), array_union([1, 2, 4], [1, 2, 3, 1, 2])"
    sql2 = "select [1, 2, 3, 4], [1, 2, 4, 3]"
    common.check2(client, sql1=sql1, sql2=sql2)
    sql1 = "select array_union([1, 2, 3, 1, 2, 'true'], [null]), array_union([1, 2, 3, 1, 2, 'true'], null)"
    sql2 = "select ['1', '2', '3', 'true', null], null"
    common.check2(client, sql1=sql1, sql2=sql2)
    sql1 = "select array_union(['a', 'abc', ''], [' ', 'abcd', 'a', 'b', ''])"
    sql2 = "select ['a', 'abc', '', ' ', 'abcd', 'b']"
    common.check2(client, sql1=sql1, sql2=sql2)
    sql1 = "select array_union(['a', 'abc', '', null, 'a'], [' ', 'abcd', 'a', null, 'b', ''])"
    sql2 = "select ['a', 'abc', '', NULL, ' ', 'abcd', 'b']"
    common.check2(client, sql1=sql1, sql2=sql2)


def test_arrays_overlap():
    """
    {
    "title": "test_array_overlap",
    "describe": "arrays_overlap函数测试",
    "tag": "function,p1"
    }
    """
    # BOOLEAN arrays_overlap(ARRAY<T> left, ARRAY<T> right), 判断left和right数组中是否包含公共元素，返回如
    # 1    - left和right数组存在公共元素；
    # 0    - left和right数组不存在公共元素；
    # NULL - left或者right数组为NULL；或者left和right数组中，任意元素为NULL；
    # enable_vectorized_engine
    client = common.get_client()
    # types
    support_k = ['a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9', 'a10', 'a11', 'a12', 'a13', 'a14']
    sql1 = 'select arrays_overlap({0}, {0}) from {1}.{2}'
    sql2 = 'select 1 from {0}.{1}'
    for c in support_k:
        common.check2(client, sql1=sql1.format(c, db, table), sql2=sql2.format(db, table))
    sql1 = 'select k1, arrays_overlap(a1, [1]), arrays_overlap(a2, [null]), arrays_overlap(a3, [null]), ' \
           'arrays_overlap(a4, [null]), arrays_overlap(a5, [null]), arrays_overlap(a6, [null]), ' \
           'arrays_overlap(a7, [null]), arrays_overlap(a8, [null]), arrays_overlap(a9, [null]), ' \
           'arrays_overlap(a9, [null]), arrays_overlap(a10, [null]), arrays_overlap(a11, [null]), ' \
           'arrays_overlap(a12, [null]), arrays_overlap(a13, [null]), arrays_overlap(a14, [null]) ' \
           'from %s.%s order by k1' % (db, table)
    sql2 = 'select k1, 1, null, null, null, null, null, null, null, null, null, null, null, null, null, null ' \
           'from test_query_qa.test group by k1 order by k1'
    common.check2(client, sql1, sql2=sql2)

    sql0 = 'select t1.{0}, t2.{0}, arrays_overlap(t1.{0}, t2.{0}) from {1}.{2} t1 cross join {1}.{2} t2 ' \
           'where t1.k1=0 and t2.k1 > 0'
    sql1 = 'select arrays_overlap(t1.{0}, t2.{0}) from {1}.{2} t1 cross join {1}.{2} t2 ' \
           'where t1.k1=0 and t2.k1 > 0'
    sql2 = 'select 0 union all select null union all select null union all select null'
    c = 'a7'
    print(sql0.format(c, db, table_2))
    common.check2(client, sql1=sql1.format(c, db, table_2), sql2=sql2.format(c, db, table_2), forced=True)

    sql1 = "select arrays_overlap([0, 1, 2, 3], a1) from %s.%s order by k1" % (db, table_2)
    sql2 = "select 1 union all select 0 union all select null union all select null union all select null"
    common.check2(client, sql1, sql2=sql2, forced=True)

    sql1 = 'select arrays_overlap([1, 2, 3, null], [1, 2, 3, 4]), arrays_overlap([1, 2, 3], [1, 2, 3, 4, null])'
    sql2 = 'select null, null'
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select arrays_overlap([1, 2, 3, 1], [1, 2, 3, 4]), arrays_overlap([1, 2, 3, null], null)'
    sql2 = 'select 1, null'
    common.check2(client, sql1, sql2=sql2)
    sql1 = "select arrays_overlap(['a', 'b', 'c'], ['bb', 'd', 'c'])"
    sql2 = "select 1"
    common.check2(client, sql1, sql2=sql2)


def test_collect_list():
    """
    {
    "title": "test_collect_list",
    "describe": "collect_list函数测试，ARRAY<T> collect_list(expr) 将行聚合成array类型",
    "tag": "function,p1"
    }
    """
    # ARRAY<T> collect_list(expr) 
    # 将行聚合成array类型, 返回一个包含 expr 中所有元素(不包括NULL)的数组，数组中元素顺序是不确定的
    client = common.get_client()
    sql1 = 'select array_sort(collect_list(k1+k2)) from test_query_qa.baseall where k1 > 5'
    sql2 = "select concat('[', group_concat(CAST( k1+k2 AS CHARACTER) ORDER BY k2+k1), ']') " \
           "from test_query_qa.baseall where k1 > 5"
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select array_sort(collect_list(k1 * k5)) from test_query_qa.baseall group by k2'
    sql2 = "select concat('[', group_concat(CAST( k1 * k5 AS CHARACTER) ORDER BY k1 * k5), ']') " \
           "from test_query_qa.baseall group by k2"
    common.check2(client, sql1, sql2=sql2, forced=True)
    sql = 'select collect_list(k5) a1, collect_list(k6) a2 from test_query_qa.baseall ' \
          'where k1 > 5 group by k2 having size(a2) > 1'
    util.assert_return(True, None, client.execute, sql)
    # select collect_list(k10), collect_list(k2) from (select k10, k2 from test_query_qa.baseall union select null, null) t;
    sql1 = 'select array_sort(collect_list(k10)), array_sort(collect_list(k2)) from ' \
           '(select k10, k2 from test_query_qa.baseall union select null, null) t'
    sql2 = "select concat('[', group_concat(CAST( k10 AS CHARACTER) ORDER BY k10 ), ']'), " \
           "concat('[', group_concat(CAST( k2 AS CHARACTER) ORDER BY k2), ']')" \
           "from test_query_qa.baseall"
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select array_sort(a1) oo from (select collect_list(k4) a1 from test_query_qa.baseall) tmp ' \
           'order by size(oo)'
    sql2 = "select concat('[', group_concat(CAST( k4 AS CHARACTER) ORDER BY k4), ']') from test_query_qa.baseall"
    common.check2(client, sql1, sql2=sql2)
    sql = 'select collect_set(a1) from %s.%s' % (db, table_2)
    msg = 'No matching function with signature: collect_set(array<boolean>)'
    util.assert_return(False, msg, client.execute, sql)
    sql1 = 'select collect_list(a1) from ' \
           '(select 1 a1 union all select 1 union all select 2 union all select null union all select 3) tmp'
    sql2 = 'select [1, 1, 2, 3]'
    common.check2(client, sql1, sql2=sql2)
    # empty
    sql1 = 'select collect_list(k1) from test_query_qa.baseall where k1 < 0'
    sql2 = 'select null'
    common.check2(client, sql1, sql2=sql2)
    # all null
    sql1 = 'select collect_list(k1 + null) from test_query_qa.baseall'
    sql2 = 'select null'
    common.check2(client, sql1, sql2=sql2)


def test_collect_set():
    """
    {
    "title": "test_collect_set",
    "describe": "acollect_set函数测试",
    "tag": "function,p1"
    }
    """
    # ARRAY<T> collect_set(expr)
    # 返回一个包含 expr 中所有去重后元素(不包括NULL)的数组，数组中元素顺序是不确定的
    client = common.get_client()
    sql1 = 'select array_sort(collect_set(k1)) a1 from test_query_qa.baseall where k1 > 5'
    sql2 = "select concat('[', group_concat(distinct CAST( k1 AS CHARACTER) ORDER BY k1), ']') " \
           "from test_query_qa.baseall where k1 > 5"
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select array_sort(collect_set(k4 + k3)) a2 from test_query_qa.baseall ' \
           'where k1 > 5 group by k6 order by k6'
    sql2 = "select concat('[', group_concat(distinct CAST(k4 + k3 AS CHARACTER) ORDER BY k4 + k3), ']')" \
           "from test_query_qa.baseall where k1 > 5 group by k6 order by k6"
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select array_sort(collect_set(k5)) a1 from test_query_qa.baseall ' \
           'where k1 > 5 group by k2 having size(a1) > 1'
    sql2 = "select concat('[', group_concat(distinct CAST( k5 AS CHARACTER) ORDER BY k5), ']') " \
           "from test_query_qa.baseall where k1 > 5 group by k2 having count(k5) > 1"
    common.check2(client, sql1, sql2=sql2, forced=True)
    # select array_sort(collect_set(k11)) from (select k10, k11 from test_query_qa.baseall union select null, null) t;
    sql1 = 'select array_sort(collect_set(k11)) ' \
           'from (select k10, k11 from test_query_qa.baseall union select null, null) t'
    sql2 = "select concat('[', group_concat(distinct CAST( k11 AS CHARACTER) ORDER BY k11 ), ']') " \
           "from test_query_qa.baseall"
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select array_sort(a1) oo ' \
           'from (select collect_set(k2) a1 from test_query_qa.baseall) tmp order by size(oo)'
    sql2 = "select concat('[', group_concat(distinct CAST( k2 AS CHARACTER) ORDER BY k2), ']')" \
           "from test_query_qa.baseall"
    common.check2(client, sql1, sql2=sql2)
    sql = 'select collect_set(a1) from %s.%s' % (db, table_2)
    msg = 'No matching function with signature: collect_set(array<boolean>)'
    util.assert_return(False, msg, client.execute, sql)
    sql1 = 'select collect_set(a1) from (select 1 a1 union all select null union all select 1 union all select 2) tmp'
    sql2 = 'select [2, 1]'
    common.check2(client, sql1, sql2=sql2)
    # empty
    sql1 = 'select collect_list(k1) from test_query_qa.baseall where k1 < 0'
    sql2 = 'select null'
    common.check2(client, sql1, sql2=sql2)
    # all null
    sql1 = 'select collect_list(k1 + null) from test_query_qa.baseall'
    sql2 = 'select null'
    common.check2(client, sql1, sql2=sql2)


def test_explode():
    """
    {
    "title": "test_explode",
    "describe": "array行转列, outer的区别在于Null",
    "tag": "function,p1"
    }
    """
    # explode(expr)
    # 表函数，需配合 Lateral View 使用
    # 将 array 列展开成多行。当 array 为NULL或者为空时，explode_outer 返回NULL， explode返回empty。
    client = common.get_client()
    # types
    support_k = ['a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9', 'a10', 'a11', 'a12', 'a13', 'a14']
    sql1 = 'select k1, t1 from {0}.{1} lateral view explode({2}) k2_t as t1 ' \
           'where k1 = 1 order by t1'
    sql2 = 'select k1, t1 from {0}.{1} lateral view explode_outer({2}) k2_t as t1 ' \
           'where k1 = 1 order by t1'
    for c in support_k:
        common.check2(client, sql1=sql1.format(db, table, c), sql2=sql2.format(db, table, c))
    

    sql = 'select e1 from (select 1 k1) as t lateral view explode(null) tmp1 as e1'
    ret = client.execute(sql)
    assert len(ret) == 0, 'expect empty set'
    sql1 = 'select e1 from (select 1 k1) as t lateral view explode_outer(null) tmp1 as e1'
    sql2 = 'select null'
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select e1 from %s.%s as t lateral view explode_outer(a1) tmp1 as e1 where k1 > 0' % (db, table_2)
    sql2 = 'select null union all select null union all select null union all select null union all select 1'
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    sql1 = 'select e1 from %s.%s as t lateral view explode(a1) tmp1 as e1 where k1 > 0' % (db, table_2)
    sql2 = 'select null union all select null union all select 1'
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)

    sql1 = 'select k1, e1, e2 from %s.%s as t lateral view explode_outer(a2) tmp1 as e1 ' \
           'lateral view explode_outer(a1) tmp2 as e2 where k1=0 order by k1, e1, e2' % (db, table_2)
    sql2 = 'select 0, t1.k1, t2.k2 from test_query_qa.baseall t1 cross join ' \
           '(select 0 k2 union all select 1 union all select 1 union all select 0 ) t2 order by 1, 2, 3'
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)

    client.use(db)
    sql = 'drop view if exists t1'
    client.execute(sql)
    sql = 'create view t1 as select k1, a1 from %s.%s where k1 > 0' % (db, table_2)
    client.execute(sql)
    sql1 = 'select k1, e1 from (select k1, a1, e1 from t1 lateral view explode_outer(a1) tt as e1) tmp ' \
           'where e1 is not null order by k1, e1'
    sql2 = 'select 1, 1'
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select collect_list(e1) from (select k1, a1, e1 from t1 lateral view explode_outer(a1) tt as e1) tmp ' \
           'where e1 > 10 or size(a1) = 3'
    sql2 = 'select null'
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select k1 from t1 where t1.k1 in (select e1 from t1 lateral view explode_outer(a1) tt as e1)'
    sql2 = 'select 1'
    common.check2(client, sql1, sql2=sql2)
    sql = 'drop view if exists t1'
    client.execute(sql)


def test_array_cast():
    """
    {
    "title": "test_explode",
    "describe": "array类型转换, todo后续可能不支持array子类型之间的cast，需跟进",
    "tag": "function,p1"
    }
    """
    client = common.get_client()
    sql = 'select cast(1 as array<largeint>)'
    msg = 'Invalid type cast of 1 from TINYINT to ARRAY<LARGEINT(40)>'
    util.assert_return(False, msg, client.execute, sql)
    sql1 = 'select cast("1" as array<largeint>)'
    sql2 = 'select null'
    common.check2(client, sql1, sql2=sql2)
    sql = "select cast(['a','b','c','',''] as string)"
    msg = "Invalid type cast of ARRAY('a', 'b', 'c', '', '') from ARRAY<VARCHAR(-1)> to TEXT"
    util.assert_return(False, msg, client.execute, sql)
    array_types = ['array<boolean>', 'array<tinyint>', 'array<smallint>', 'array<int>',
                    'array<bigint>', 'array<largeint>', 'array<decimal>', 'array<double>', 
                    'array<float>', 'array<date>', 'array<datetime>', 'array<char>',
                    'array<varchar>', 'array<string>']
    support_k = ['a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9', 'a10', 'a11', 'a12', 'a13', 'a14']
    sql2 = 'select {0} from {1}.{2}'    
    for at in array_types:
        for c in support_k:
            if c == 'a7' and at in ('array<date>', 'array<datetime>'):
                continue
            if c in support_k[0:11] and at in ('array<char>'):
                continue
            sql1 = "select cast({0} as {1}) from {2}.{3} where k1 = 1 limit 1".format(c, at, db, table_2)
            util.assert_return(True, None, client.execute, sql1)
            print(client.execute(sql1))
    sql = 'select cast(a7 as array<date>) from %s.%s' % (db, table)
    msg = 'Invalid type cast of `a7` from ARRAY<DECIMAL(27, 7)> to ARRAY<DATE>'
    util.assert_return(False, msg, client.execute, sql)
    sql = 'select cast(a1 as array<char(5)>) from %s.%s' % (db, table)
    msg = 'Invalid type cast of `a1` from ARRAY<BOOLEAN> to ARRAY<CHAR(5)>'
    util.assert_return(False, msg, client.execute, sql)


def test_array_query_1():
    """
    {
    "title": "test_array_query_1",
    "describe": "array类型查询",
    "tag": "function,p1"
    }
    """
    client = common.get_client()
    client.use(db)
    sql = "select * from %s where a4[1] = a3[1] or a4[1]=a5[1]" % table
    assert () == client.execute(sql), 'expect empty set'
    sql1 = 'select collect_list(k1) from %s group by k1 having size(collect_list(k1)) = 1' % table_2
    sql2 = 'select "[0]" union select "[1]" union select "[2]" union select "[3]" union select "[4]"'
    common.check2(client, sql1, sql2=sql2, forced=True)
    sql1 = 'with z as (select * from %s) select a2 from z' % table_2
    sql2 = 'select a2 from %s' % table_2
    common.check2(client, sql1, sql2=sql2, forced=True)
    sql = 'drop view if exists arr_v'
    client.execute(sql)
    sql = 'create view arr_v as select * from %s' % table
    client.execute(sql)
    sql1 = 'select * from %s order by k1' % table
    sql2 = 'select * from arr_v order by k1' 
    common.check2(client, sql2, sql2=sql1)
    sql = 'drop view if exists arr_v'
    client.execute(sql)
    sql = 'select k1 from %s where k1 > (select array_avg(a2) from %s where array_avg(a2) > 0 limit 1)' \
          % (table_2, table_2)
    util.assert_return(True, None, client.execute, sql)
    sql1 = 'select k1 from (select * from %s) b where array_contains(b.a2, 1) and not array_contains(b.a3, 32767)' \
           % table
    sql2 = 'select 1'
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select a2[1] + a3[1] from %s order by k1' % table
    sql2 = 'select min(k1) + min(k2) from test_query_qa.test group by k1 order by k1'
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select a2[k1+1] + a3[1] from %s where k1 > 0 and k1 < 100 order by k1' % table
    sql2 = 'select min(k1) + min(k2) from test_query_qa.test where k1 > 0 and k1 < 100 group by k1 order by k1'
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select k1 from %s where upper(element_at(array_sort(a13), -1)) in ("YUNLJ8@NK", "HELLO VARCHAR")' \
           % table_2
    sql2 = 'select 0 union select 1'
    common.check2(client, sql1, sql2=sql2, forced=True)
    sql1 = 'select l.k1, b.a2[-1], l.a3[1:2] from %s l, %s b where l.k1=b.k1 order by k1' % (table, table_2)
    sql2 = 'select 0, 15, "[-32550, -32506]" union select 1, 4, "[-32532, -31936]" union ' \
           'select 2, null, "[-32743, -32583]" union select 3, null, "[-32017, -31809]" union ' \
           'select 4, null, "[-32611, -32497]"'
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select l.k1, array_except(l.a1, b.a1) from %s l, %s b where size(array_except(l.a1, b.a1)) > 0 ' \
           'order by k1' % (table, table_2)
    sql2 = 'select k1, "[0]" from test_query_qa.test group by k1 union all ' \
           'select k1, "[0, 1]" from test_query_qa.test group by k1 union all ' \
           'select k1, "[0, 1]" from test_query_qa.test group by k1'
    common.check2(client, sql1, sql2=sql2, forced=True)
    sql1 = 'select count(*) from (select l.k1, array_except(l.a1, b.a1), array_except(l.a3, b.a2) ' \
           'from %s l, %s b where size(array_except(l.a1, b.a1)) > 0 or array_except(l.a3, b.a2) is null) tmp ' \
           % (table, table_2)
    sql2 = 'select 1020'
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select l.k1, array_except(l.a1, b.a1) from %s l join %s b on size(array_except(l.a1, b.a1)) = 0 ' \
           'order by k1' % (table, table_2)
    sql2 = 'select k1, [] from test_query_qa.test group by k1 order by k1'
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select l.k1, array_min(b.a2) from %s l join %s b on array_max(l.a2) = 0' % (table, table_2)
    sql2 = 'select 0, 1 union all select 0,1 union all select 0, null union all select 0, null union all select 0, null'
    common.check2(client, sql1, sql2=sql2, forced=True)


def test_array_query_2():
    """
    {
    "title": "test_array_query_2",
    "describe": "array类型查询",
    "tag": "function,p1"
    }
    """
    client = common.get_client()
    client.use(db)
    client.drop_table('tb1', if_exist=True)
    client.drop_table('tb2', if_exist=True)
    schema = [('k1', 'int'), ('a1', 'array<int>')]
    ret = client.create_table('tb1', schema)
    assert ret
    assert client.create_table('tb2', schema)
    client.execute('insert into tb1 values(1, [1,2,3]),(3,[3,2,1]),(3,[3,2,1,NULL]),(2,[3,4,5])')
    client.execute('insert into tb2 values(1,[2]),(2,[3])')
    sql1 = 'select t1.k1, t1.a1, array_except(t1.a1, t2.a1) from tb1 t1 join tb2 t2 ' \
           'where size(array_except(t1.a1, t2.a1)) = size(t1.a1)'
    sql2 = 'select 2, [3, 4, 5], [3, 4, 5]'
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select array_sort(collect_list(k1)) from (select array_max(a1) k1 from tb1 ' \
           'union all select array_min(a1) k1 from tb2) k1'
    sql2 = 'select [2, 3, 3, 3, 3, 5]'
    common.check2(client, sql1, sql2=sql2)
    sql1 = "select array_sort(collect_list(k1)) from (select array_join(a1, 'hh', '\\N') k1 from tb1 " \
           "union select array_join(a1, 'hhd', '\\N') k1 from tb2) k1;"
    sql2 = "select ['1hh2hh3', '2', '3', '3hh2hh1', '3hh2hh1hh\\N', '3hh4hh5']"
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'with w1 as (select k1 ds from tb1), w2 as (select k1 as dd from tb2) ' \
           'select array_sort(collect_list(ds)), array_sort(collect_set(ds)) from w1,  w2'
    sql2 = 'select [1, 1, 2, 2, 3, 3, 3, 3], [1, 2, 3]'
    common.check2(client, sql1, sql2=sql2)
    sql1 = 'select array_union(t1.a1, t2.a1) from tb1 t1 inner join tb2 t2 on t1.k1=t2.k1;'
    sql2 = 'select "[3, 4, 5]" union select "[1, 2, 3]"'
    common.check2(client, sql1, sql2=sql2, forced=True)
    sql1 = 'select array_union(t1.a1, t2.a1) from tb1 t1 inner join tb2 t2 on t1.k1 > t2.k1;'
    sql2 = 'select "[3, 4, 5, 2]" union all select "[3, 2, 1, NULL]" union all ' \
           'select "[3, 2, 1, NULL]" union all select "[3, 2, 1]" union all ' \
           'select "[3, 2, 1]"'
    common.check2(client, sql1, sql2=sql2, forced=True)
    sql1 = 'select t1.k1, array_min(t2.a1) from tb1 t1 join tb2 t2 where array_max(t1.a1) = array_max(t2.a1)'
    sql2 = 'select 1, 3 union all select 3, 3 union all select 3, 3'
    common.check2(client, sql1, sql2=sql2, forced=True)
    sql1 = 'select array_min(t2.a1) from tb1 t1 join tb2 t2 where array_max(t1.a1) = array_max(t2.a1)'
    sql2 = 'select 3 union all select 3 union all select 3'
    common.check2(client, sql1, sql2=sql2, forced=True)
    client.drop_table('tb1', if_exist=True)
    client.drop_table('tb2', if_exist=True)


def test_array_query_not_support():
    """
    {
    "title": "test_explode",
    "describe": "array类型查询，不支持的场景",
    "tag": "function,p1"
    }
    """
    client = common.get_client()
    client.use(db)
    # where
    sql = "select count(a1) from %s" % table
    msg = "must use with specific function"
    util.assert_return(False, msg, client.execute, sql)
    # =, > , <
    sql = "select k1 from %s where a1 = []" % table
    msg = "Array type dose not support operand"
    util.assert_return(False, msg, client.execute, sql)
    sql = "select k1 from %s where a1 = cast('[]' as array<boolean>)" % table
    util.assert_return(False, msg, client.execute, sql)
    # order by 
    sql = 'select k1 from %s order by a1' % table
    msg = "don't support"
    util.assert_return(False, msg, client.execute, sql)
    # group by
    sql = 'select count(*) from %s group by a1' % table
    msg = "don\'t support"
    util.assert_return(False, msg, client.execute, sql)
    # union
    sql = "select k1, a2 from test_array_select_test_table_tb union select k1, a2 from %s" % table_2
    msg = "don\'t support"
    util.assert_return(False, msg, client.execute, sql)
    # distinct
    sql = "select distinct a2 from test_array_select_test_table_tb_1;"
    msg = 'don\'t support'
    util.assert_return(False, msg, client.execute, sql)


def test_array_function_nest():
    """
    {
    "title": "test_explode",
    "describe": "array类型查询",
    "tag": "function,p1"
    }
    """
    client = common.get_client()
    sql1 = "select size(array_remove(array_distinct((array_sort(cast('[4, 5, null, 2, null, 2]' as array<int>)))), 5))"
    sql2 = "select 3"
    common.check2(client, sql1, sql2=sql2)


if __name__ == '__main__':
    setup_module()
    init(db, table, True)

