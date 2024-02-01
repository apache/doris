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
file: test_constant_assigned.py
date: 2019-05-10
"""
import sys
sys.path.append("../lib/")
from palo_qe_client import QueryBase

table_name = "test"
join_name = "baseall"


def setup_module():
    """
    init config
    """
    global runner
    runner = QueryBase()


def test_query_constant_assigned_basic():
    """
    {
    "title": "test_query_constant_assigned.test_query_constant_assigned",
    "describe": "测试常量谓词的分配，以及表达式的计算",
    "tag": "p1,system"
    }
    """
    """
    测试常量谓词的分配，以及表达式的计算
    """
    line = 'select * from %s where 1 = 2' % table_name
    runner.check(line)
    line = 'select * from %s where 1=2 and 3=4' % table_name
    runner.check(line)
    line = 'select * from %s where !false' % table_name
    runner.check(line, force_order=True)
    line = 'select * from %s where 1=2 or 3=4' % table_name
    runner.check(line)
    line = 'select * from %s where 1=2 or 4=4' % table_name
    runner.check(line, force_order=True)
    line = 'select * from %s where 1=2 and hex(3) = 4' % table_name
    runner.check(line, force_order=True)
    line = 'select * from %s where 1=2 or hex(3) = 4' % table_name
    runner.check(line, force_order=True)
    line = 'select * from %s a join %s b on a.k1 = b.k1 where 1=3' % (table_name, join_name)
    runner.check(line, force_order=True)
    line = 'select sum(a.k1) from %s a join %s b on a.k1 = b.k1 where 1=3' % (table_name, join_name)
    runner.check(line, force_order=True)
    line = 'select * from %s a left join %s b on a.k1 = b.k1 where 1=3' % (table_name, join_name)
    runner.check(line, force_order=True)
    line = 'select count(*) from %s a left join %s b on a.k1 = b.k1 and 1=3' % (table_name, join_name)
    runner.check(line)
    line = 'select a.k1, b.k1, c.k1 from %s a left join %s b on a.k1 = b.k1 and 1=3 ' \
           'join %s c on b.k1 = c.k1' % (table_name, table_name, join_name)
    runner.check(line)
    line = 'select a.k1, b.k1, c.k1 from %s a left join %s b on a.k1 = b.k1 and 1=3 ' \
           'right join %s c on b.k1 = c.k1' % (table_name, table_name, join_name)
    runner.check(line, force_order=True)
    line = 'select a.k1, b.k1, c.k1 from %s a left join %s b on a.k1 = b.k1 and 1=3 ' \
           'right join %s c on b.k1 = c.k1 and 2=4' % (table_name, table_name, join_name)
    runner.check(line, force_order=True)
    line = 'select a.k1, b.k1, c.k1 from %s a left join %s b on a.k1 = b.k1 and 1=3 ' \
           'join %s c on b.k1 = c.k1 and 2=4' % (table_name, table_name, join_name)
    runner.check(line)
    # return empty set
    line1 = 'select a.k1, b.k1, c.k1 from %s a join %s b on a.k1 = b.k1 and 1=3 ' \
            'full join %s c on b.k1 = c.k1 and 2=4' % (table_name, table_name, join_name)
    line2 = 'select * from %s where 1=2' % table_name
    runner.check2(line1, line2)


def test_query_constant_assigned_view():
    """
    {
    "title": "test_query_constant_assigned.test_query_constant_assigned_view",
    "describe": "基于view测试常量谓词的分配，以及表达式的计算",
    "tag": "p1,system"
    }
    """
    """
    基于view测试常量谓词的分配，以及表达式的计算
    """
    line = 'drop view if exists constant_assigned_test_view'
    runner.init(line)
    line = 'create view constant_assigned_test_view as select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11' \
           ' from %s union all select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s'\
           % (table_name, join_name)
    runner.init(line)
    line = 'select * from constant_assigned_test_view where 1=3'
    runner.check(line, force_order=True)
    line = 'select * from constant_assigned_test_view where 1=2 and 3=hex(4)'
    runner.check(line, force_order=True)
    line = 'select * from constant_assigned_test_view where 1=2 or 3=hex(4)'
    runner.check(line, force_order=True)
    line = 'select * from constant_assigned_test_view where 1=2 or 4=4'
    runner.check(line, force_order=True)
    line = 'select * from constant_assigned_test_view where 1=2 and 4=4'
    runner.check(line, force_order=True)
    line = 'select k1 from constant_assigned_test_view where 1=2 or 3=hex(4) group by k1 order by k1'
    runner.check(line, force_order=True)
    line = 'select aa.k1 from constant_assigned_test_view aa join %s b where 1=2 or 3=hex(4) ' \
           'group by aa.k1 order by aa.k1' % table_name
    runner.check(line, force_order=True)
    line = 'select aa.k1, bb.k1 from constant_assigned_test_view aa join %s bb where 1=2 or 3=hex(4) ' \
           'group by aa.k1, bb.k1 order by aa.k1' % table_name
    runner.check(line, force_order=True)
    line = 'select aa.k1, bb.k1 from constant_assigned_test_view aa join %s bb where 1=2 and 3=hex(4) ' \
           'group by aa.k1, bb.k1 order by aa.k1' % table_name
    runner.check(line, force_order=True)
    line = 'select * from (select sum(k1) as s from %s)a where 2=2' % table_name
    runner.check(line, force_order=True)
    line = 'select * from (select sum(k1) as s from %s)a where 2=1' % table_name
    runner.check(line, force_order=True)


def teardown_module():
    """tear down"""
    pass

