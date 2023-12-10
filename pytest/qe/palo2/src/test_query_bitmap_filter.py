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
test_query_bitmap_filter.py
支持如下in子查询，k1为非largeint的整型数字，bitmap_column为bitmap类型的列
k1 in (select bitmap_column from xxx)
"""
import sys

sys.path.append("../lib/")
from palo_qe_client import QueryBase

dup_table = "bm_dup_data"
k1_tb = "test_k1_bitmap"
k1_k2_k3_tb = "test_k1_k2_k3_bitmap"
baseall = "baseall"
test_tb = "test"
bigtable = "bigtable"


def setup_module():
    """
    init config
    """
    global runner
    runner = QueryBase()
    ret = runner.get_sql_result('show variables like "runtime_filter_type"')
    if len(ret) == 1 and str(ret[0][1]).find('BITMAP_FILTER') == -1:
        runner.checkok("set global runtime_filter_type='IN_OR_BLOOM_FILTER, BITMAP_FILTER'")
    try:
        runner.checkok('select count(*) from %s' % dup_table)
        runner.checkok('select count(*) from %s' % k1_tb)
        runner.checkok("select count(*) from %s" % k1_k2_k3_tb)
    except Exception as e:
        print(e)
        init_tb()


def init_tb():
    """
    init test data
    """
    runner.checkok('drop table if exists %s' % dup_table)
    sql = "create table %s (k1 int, k2 int, k3 int, k4 bigint) distributed by hash(k1)" % dup_table
    runner.checkok(sql)
    sql = "insert into %s select k1, k2, k3, abs(k4) as k4 from test" % dup_table
    runner.checkok(sql)
    sql = "insert into %s select null, null, null, null" % dup_table
    runner.checkok(sql)
    runner.checkok('drop table if exists %s' % k1_tb)
    sql = 'create table %s (k1 int, bm_k4 bitmap bitmap_union) ' \
          'PARTITION BY RANGE(`k1`) (' \
          'PARTITION p1 VALUES LESS THAN ("-64"), ' \
          'PARTITION p2 VALUES LESS THAN ("0"), ' \
          'PARTITION p3 VALUES LESS THAN ("64"), ' \
          'PARTITION p4 VALUES LESS THAN MAXVALUE) distributed by hash(k1)' % k1_tb
    runner.checkok(sql)
    sql = "insert into %s select k1, to_bitmap(k4) from %s" % (k1_tb, dup_table)
    runner.checkok(sql)
    runner.checkok('drop table if exists %s' % k1_k2_k3_tb)
    sql = "create table %s (k1 int, k2 int, k3 int, bm_k1_k2_k3 bitmap bitmap_union) distributed by hash(k1)" \
          % k1_k2_k3_tb
    runner.checkok(sql)
    sql = "insert into %s select k1, k2, k3, to_bitmap(abs(k1)) from %s" % (k1_k2_k3_tb, dup_table)
    runner.checkok(sql)
    sql = "insert into %s select k1, k2, k3, to_bitmap(abs(k2)) from %s" % (k1_k2_k3_tb, dup_table)
    runner.checkok(sql)
    sql = "insert into %s select k1, k2, k3, to_bitmap(abs(k3)) from %s" % (k1_k2_k3_tb, dup_table)
    runner.checkok(sql)
    sql = "insert into %s select k1, k2, k3, to_bitmap(abs(k4)) from %s" % (k1_k2_k3_tb, dup_table)
    runner.checkok(sql)


def test_bitmap_filter_uncorrelated_1():
    """
    {
    "title": "test_bitmap_filter_uncorrelated",
    "describe": "不相关子查询",
    "tag": "function,p0"
    }
    """
    line1 = 'select k1 from %s where k4 in (select bm_k4 from %s) and k4 = 2575082496149092063 order by k1' \
            % (dup_table, k1_tb)
    line2 = 'select k1 from %s where k4 = 2575082496149092063 order by k1' % dup_table
    runner.check2_palo(line1, line2)
    line1 = 'select k1 from %s where k4 in (select bm_k4 from %s) and k4 = -2575082496149092063 order by k1' \
            % (dup_table, k1_tb)
    line2 = 'select k1 from %s where k4 = -2575082496149092063 order by k1' % dup_table
    runner.check2_palo(line1, line2)
    line1 = 'select k1 from %s where 2575082496149092063 in (select bm_k4 from %s) order by k1' % (dup_table, k1_tb)
    line2 = 'select k1 from %s order by k1' % dup_table
    runner.check2_palo(line1, line2)
    line1 = 'select k1 from %s where k4 in (select bm_k4 from %s order by k1) order by k1' % (dup_table, k1_tb)
    line2 = 'select k1 from %s where k4 is not null order by k1' % dup_table
    runner.check2_palo(line1, line2)
    line1 = 'select k1 from %s where k4 in (select bm_k4 from %s order by k1 limit 10) order by k1' \
            % (dup_table, k1_tb)
    line2 = 'select k1 from %s where k1 in (select distinct k1 from %s order by k1 limit 10) order by k1' \
            % (dup_table, dup_table)
    runner.check2_palo(line1, line2)
    line1 = 'select k1 from %s where k4 in (select bitmap_union(bm_k4) from %s) order by k1' % (dup_table, k1_tb)
    line2 = 'select k1 from %s where k4 is not null order by k1' % dup_table
    runner.check2_palo(line1, line2)
    line1 = 'select k1 from %s where k4 in (select bitmap_union(bm_k4) from %s where k1 > 0) order by k1' \
            % (dup_table, k1_tb)
    line2 = 'select k1 from %s where k1 > 0 order by k1' % dup_table
    runner.check2_palo(line1, line2)
    line1 = 'select k1 from %s where k4 not in (select bm_k4 from %s) and k4 != -11011903 order by k1' \
            % (baseall, k1_tb)
    line2 = 'select k1 from %s where k4 not in (select k4 from %s where k4 is not null) ' \
            'and k4 != -11011903 order by k1' % (baseall, dup_table)
    runner.check2_palo(line1, line2)
    line1 = 'select k1 from %s where k4 not in (select bm_k4 from %s) and k4 = -11011903 order by k1' \
            % (baseall, k1_tb)
    line2 = 'select k1 from %s where k4 not in (select k4 from %s where k4 is not null) ' \
            'and k4 = -11011903 order by k1' % (baseall, dup_table)
    runner.check2_palo(line1, line2)
    
    line1 = 'select k1 from %s where -11011903 not in (select bm_k4 from %s) order by k1' % (baseall, k1_tb)
    line2 = 'select k1 from %s order by k1' % baseall
    runner.check2_palo(line1, line2)
    line1 = 'select k1 from %s where k4 not in (select bm_k4 from %s order by k1) order by k1' % (baseall, k1_tb)
    line2 = 'select k1 from %s where k4 not in (select k4 from %s where k4 is not null) order by k1' \
            % (baseall, dup_table)
    runner.check2_palo(line1, line2)


def test_bitmap_filter_uncorrelated_2():
    """
    {
    "title": "test_bitmap_filter_uncorrelated_2",
    "describe": "不相关子查询",
    "tag": "function,p0"
    }
    """
    line1 = 'select k1 from %s where k4 not in (select bm_k4 from %s order by k1 limit 10) order by k1' \
            % (baseall, k1_tb)
    line2 = 'select k1 from %s where k4 not in ' \
            '(select k4 from %s where k1 in (select k1 from %s order by k1 limit 10)) ' \
            'order by k1' % (baseall, dup_table, dup_table)
    runner.check2_palo(line1, line2)
    line1 = 'select k1 from %s where k4 not in (select bitmap_union(bm_k4) from %s) order by k1' \
            % (baseall, k1_tb)
    line2 = 'select k1 from %s where k4 not in (select k4 from %s where k4 is not null) order by k1' \
            % (baseall, dup_table)
    runner.check2_palo(line1, line2)
    line1 = 'select k1 from %s where k4 not in (select bitmap_union(bm_k4) from %s where k1 > 10) ' \
            'order by k1' % (baseall, k1_tb)
    line2 = 'select k1 from %s where k4 not in (select k4 from %s where k1 > 10) order by k1' \
            % (baseall, dup_table)
    runner.check2_palo(line1, line2)

    line1 = 'select count(*) from %s a where k4 in (select bm_k4 from %s b)' % (baseall, k1_tb)
    line2 = 'select count(*) from %s a where k4 in (select k4 from %s)' % (baseall, dup_table)
    runner.check2_palo(line1, line2)
    line1 = 'select count(*) from %s a where k4 in (select bm_k4 from %s b ) group by k2' % (baseall, k1_tb)
    line2 = 'select count(*) from %s a where k4 in (select k4 from %s) group by k2' % (baseall, dup_table)
    runner.check2_palo(line1, line2, True)
    line1 = 'select count(*) from %s a where k4 in (select bm_k4 from %s b where k1 > 0) ' \
            'and k4 in (select bm_k4 from %s b where k1 < 10);' % (baseall, k1_tb, k1_tb)
    line2 = 'select count(*) from %s a where k4 in (select k4 from %s b where k1 > 0) ' \
            'and k4 in (select k4 from %s where k1 < 10)' % (baseall, dup_table, dup_table)
    runner.check2_palo(line1, line2)
    line1 = 'select k1 from %s a where k4 in (select bm_k4 from %s b where bitmap_count(bm_k4) > 250)' \
            % (baseall, k1_tb)
    line2 = 'select k1 from %s a where k4 in (select k4 from %s where ' \
            'k1 in (select k1 from %s group by k1 having count(k4) > 250))' \
             % (baseall, dup_table, dup_table)
    runner.check2_palo(line1, line2, True)
    line1 = 'select count(k1) from %s a where k4 in ' \
            '(select bm_k4 from %s b where bitmap_count(bm_k4) > 100) ' \
            'group by k4 order by 1 desc limit 10' % (dup_table, k1_tb)
    line2 = 'select count(k1) from %s a where k4 in ' \
            '(select k4 from %s where k1 in (select k1 from %s group by k1 having count(k4) > 100)) ' \
            'group by k4 order by 1 desc limit 10' % (dup_table, dup_table, dup_table)
    runner.check2_palo(line1, line2)


def test_bitmap_filter_correlated():
    """
    {
    "title": "test_bitmap_filter_correlated",
    "describe": "相关子查询，不支持",
    "tag": "function,p0"
    }
    """
    msg = 'In bitmap does not support correlated subquery'
    line1 = 'select k1 from %s a where k4 in (select bm_k4 from %s b where a.k1 = b.k1 order by k1) ' \
            'order by k1' % (baseall, k1_tb)
    runner.checkwrong(line1, msg)
    line1 = 'select k1 from %s a where k4 in (select bm_k4 from %s b where a.k1 > b.k1 order by k1) ' \
            'order by k1' % (baseall, k1_tb)
    runner.checkwrong(line1, msg)
    line1 = 'select k1 from %s a where k4 in (select bm_k4 from %s b where a.k1 < b.k1 order by k1) ' \
            'order by k1' % (baseall, k1_tb)
    runner.checkwrong(line1, msg)


def test_bitmap_filter_and_or():
    """
    {
    "title": "test_bitmap_filter_and_or",
    "describe": "子查询符合谓词",
    "tag": "function,p0"
    }
    """
    line1 = 'select count(k1) from %s a where k4 in (select bm_k4 from %s b where bitmap_count(bm_k4) > 100) ' \
            'and k1 in (1, 2, 3, 4)' % (dup_table, k1_tb)
    line2 = 'select count(k1) from %s a where k1 in (select k1 from %s b group by k1 having count(k4) > 100) ' \
            'and k1 in (1, 2, 3, 4)' % (dup_table, dup_table)
    runner.check2_palo(line1, line2, True)
    line1 = 'select count(k1) from %s a where k4 in (select bm_k4 from %s b where bitmap_count(bm_k4) > 100) ' \
            'and k1 is not null' % (dup_table, k1_tb)
    line2 = 'select count(k1) from %s a where k1 in (select k1 from %s b group by k1 having count(k4) > 100) ' \
            'and k1 is not null' % (dup_table, dup_table)
    runner.check2_palo(line1, line2, True)
    line1 = 'select count(k1) from %s a where k4 in (select bm_k4 from %s b where bitmap_count(bm_k4) > 100) ' \
            'and k4 > 123456' % (dup_table, k1_tb)
    line2 = 'select count(k1) from %s a where k1 in (select k1 from %s b group by k1 having count(k4) > 100) ' \
            'and k4 > 123456;' % (dup_table, dup_table)
    runner.check2_palo(line1, line2, True)
    line1 = 'select count(k1) from %s a where k4 in (select bm_k4 from %s b where bitmap_count(bm_k4) > 100) ' \
            'and k4 between 123456 and 23456789' % (dup_table, k1_tb)
    line2 = 'select count(k1) from %s a where k1 in (select k1 from %s b group by k1 having count(k4) > 100) ' \
            'and k4 between 123456 and 23456789' % (dup_table, dup_table)
    runner.check2_palo(line1, line2, True)
    line1 = 'select count(k1) from %s a where k4 in (select bm_k4 from %s b where bitmap_count(bm_k4) > 100) ' \
            'or k4 = 123456' % (dup_table, k1_tb)
    # msg = 'Subqueries in OR predicates are not supported'
    msg = "Unsupported"
    # runner.checkwrong(line1, msg)
    line1 = 'select count(*) from %s where k4 in (select bm_k4 from %s) and ' \
            'k4 not in (select bm_k4 from %s where bitmap_count(bm_k4) = 0)' % (test_tb, k1_tb, k1_tb)
    line2 = 'select count(*) from %s where k4 in (select k4 from %s)' % (test_tb, dup_table)
    runner.check2_palo(line1, line2)
    line1 = 'select count(*) from %s where k4 not in (select bm_k4 from %s b) ' \
            'and k4 not in (select bm_k4 from %s b where bitmap_count(bm_k4) = 0)' % (test_tb, k1_tb, k1_tb)
    line2 = 'select count(*) from %s where k4 not in (select k4 from %s where k4 is not null)' \
            % (test_tb, dup_table)
    runner.check2_palo(line1, line2)
    line1 = 'select * from %s where k1 in (select bm_k1_k2_k3 from %s) and ' \
            'k2 in (select bm_k1_k2_k3 from %s) and k3 in (select bm_k1_k2_k3 from %s) ' \
            'order by k1' % (baseall, k1_k2_k3_tb, k1_k2_k3_tb, k1_k2_k3_tb)
    line2 = 'select * from %s where k1 in (select k1 from %s) and k2 in (select k2 from %s) ' \
            'and k3 in (select k3 from %s) order by k1' % (baseall, dup_table, dup_table, dup_table)
    runner.check2_palo(line1, line2)


def test_bitmap_filter_datatype():
    """
    {
    "title": "test_bitmap_filter_datatype",
    "describe": "常数和特殊值和数据类型",
    "tag": "function,p0"
    }
    """
    line1 = "select k1, k2 from %s where k2 in (select bitmap_union(to_bitmap(k2)) from %s) " \
            "order by k1, k2" % (test_tb, baseall)
    line2 = "select k1, k2 from %s where k2 in (select k2 from %s) and k2 > 0 order by k1, k2" \
            % (test_tb, baseall)
    runner.check2_palo(line1, line2)
    line1 = "select k1, k2 from (select 1 k1, 2 k2) tmp where k1 in (select bm_k1_k2_k3 from %s)" \
            % k1_k2_k3_tb
    line2 = "select 1, 2"
    runner.check2_palo(line1, line2)
    line1 = "select k1, k2 from (select null, null) tmp where k1 in (select bm_k4 from %s) " \
            "and k1 is null" % k1_tb
    for col in ['k5', 'k6', 'k7', 'k8', 'k9', 'k10', 'k11']:
        line1 = "select count(*) from %s where %s in  (select bm_k4 from %s)" % (baseall, col, k1_tb)
        msg = "Incompatible return types"
        # runner.checkwrong(line1, msg)
    line = "select count(*) from %s where cast(k1 as largeint) in  (select bm_k4 from %s)" \
           % (baseall, k1_tb)
    # runner.checkwrong(line, msg)
    for col in ['k1', 'k2', 'k3', 'k4']:
        line1 = "select count(*) from %s where %s in  (select bm_k4 from %s)" % (baseall, col, k1_tb)
        runner.checkok(line1)
    line1 = "select k1, k2 from %s where k1 in (select bm_k4 from %s) and k1 is null" % (dup_table, k1_tb)
    line2 = "select k1, k2 from %s where null in (select k4 from %s)" % (dup_table, dup_table)
    runner.check2_palo(line1, line2)
    # empty table
    empty_table = "bm_empty_tb"
    runner.checkok("drop table if exists %s" % empty_table)
    runner.checkok("create table %s like test_k1_bitmap" % empty_table)
    line1 = "select * from %s where k1 in (select bm_k4 from %s)" % (empty_table, k1_tb)
    line2 = "select * from %s" % empty_table
    runner.check2_palo(line1, line2)
    line1 = "select * from %s where k1 not in (select bm_k4 from %s)" % (empty_table, k1_tb)
    line2 = "select * from %s" % empty_table
    runner.check2_palo(line1, line2)
    line1 = "select * from %s where k4 in (select bm_k4 from %s)" % (baseall, empty_table)
    line2 = "select * from %s where k1 < 0" % baseall
    runner.check2_palo(line1, line2)
    line1 = "select * from %s where k4 not in (select bm_k4 from %s) order by k1" % (baseall, empty_table)
    line2 = "select * from %s order by k1" % baseall
    runner.check2_palo(line1, line2)
    runner.checkok("drop table if exists %s" % empty_table)
    line1 = "select * from %s where k1 in (select to_bitmap(10))" % baseall
    line2 = "select * from %s where k1 in (10)" % baseall
    runner.check2_palo(line1, line2)
    line1 = "select * from %s where k1 in (select to_bitmap(10) from %s)" % (baseall, baseall)
    line2 = "select * from %s where k1 in (10)" % baseall
    runner.check2_palo(line1, line2)
    line1 = "select * from %s where k1 in (select bitmap_empty())" % baseall
    line2 = "select * from %s where k1 in (null)" % baseall
    runner.check2_palo(line1, line2)


def test_bitmap_filter_scalar():
    """
    {
    "title": "test_bitmap_filter_scalar",
    "describe": "子查询结果返回多行结果，一行结果，空",
    "tag": "function,p0"
    }
    """
    line1 = "select * from %s where k4 in (select bm_k4 from %s where k1 > 10)" % (baseall, k1_tb)
    line2 = "select * from %s where k4 in (select k4 from %s where k1 > 10)" % (baseall, dup_table)
    runner.check2_palo(line1, line2, True)
    line1 = "select * from %s where k4 in (select bm_k4 from %s where k1 = 6)" % (baseall, k1_tb)
    line2 = "select * from %s where k4 in (select k4 from %s where k1 = 6)" % (baseall, dup_table)
    runner.check2_palo(line1, line2, True)
    line1 = "select * from %s where k4 in (select bm_k4 from %s where k1 = 200)" % (baseall, k1_tb)
    line2 = "select * from %s where k4 in (select k4 from %s where k1 = 200)" % (baseall, dup_table)
    runner.check2_palo(line1, line2, True)
    line1 = "select count(*) from %s where k4 in (select bitmap_union(bm_k4) from %s)" % (test_tb, k1_tb)
    line2 = "select count(*) from %s where k4 in (select k4 from %s)" % (test_tb, dup_table)
    runner.check2_palo(line1, line2)
    line1 = "select * from %s where k4 not in (select bm_k4 from %s where k1 > 10)" % (baseall, k1_tb)
    line2 = "select * from %s where k4 not in (select k4 from %s where k1 > 10)" % (baseall, dup_table)
    runner.check2_palo(line1, line2, True)
    line1 = "select * from %s where k4 not in (select bm_k4 from %s where k1 = 6)" % (baseall, k1_tb)
    line2 = "select * from %s where k4 not in (select k4 from %s where k1 = 6)" % (baseall, dup_table)
    runner.check2_palo(line1, line2, True)
    line1 = "select * from %s where k4 not in (select bm_k4 from %s where k1 = 200)" % (baseall, k1_tb)
    line2 = "select * from %s where k4 not in (select k4 from %s where k1 = 200)" % (baseall, dup_table)
    runner.check2_palo(line1, line2, True)
    line1 = "select count(*) from %s where k4 not in (select bitmap_union(bm_k4) from %s)" % (test_tb, k1_tb)
    line2 = "select count(*) from %s where k4 not in (select k4 from %s where k4 is not null)" \
            % (test_tb, dup_table)
    runner.check2_palo(line1, line2, True)
    line1 = "select count(*) from %s where exists (select bm_k4 from %s)" % (test_tb, k1_tb)
    line2 = "select count(*) from %s where exists (select k4 from %s)" % (test_tb, dup_table)
    runner.check2_palo(line1, line2)
    line1 = "select count(*) from %s where exists (select bm_k4 from %s where k1 = 200)" % (test_tb, k1_tb)
    line2 = "select count(*) from %s where exists (select k4 from %s where k1 = 200)" % (test_tb, dup_table)
    runner.check2_palo(line1, line2)
    msg = "Unsupported uncorrelated NOT EXISTS subquery"
    line1 = "select count(*) from %s where not exists (select bm_k4 from %s)" % (test_tb, k1_tb)
    # runner.checkwrong(line1, msg)
    line1 = "select count(*) from %s where not exists (select bm_k4 from %s where k1 = 200)" % (test_tb, k1_tb)
    # runner.checkwrong(line1, msg)
     

def test_bitmap_filter_expression():
    """
    {
    "title": "test_bitmap_filter_expression",
    "describe": "函数，表达式",
    "tag": "function,p0"
    }
    """
    line1 = "select * from %s a where " \
            "k1 in (select bitmap_or(bm_k4, to_bitmap(k1)) from %s b where k1 > 10)" % (baseall, k1_tb)
    line2 = "select * from %s a where " \
            "k1 in (select * from (select k4 from %s where k1 > 10 union select k1 from %s b where k1 > 10) tmp)" \
            % (baseall, dup_table, dup_table)
    runner.check2_palo(line1, line2, True)
    line1 = "select * from %s a where k1 + 1 in (select bitmap_or(bm_k4, to_bitmap(k1)) from %s b where k1 > 10)" \
            % (baseall, k1_tb)
    line2 = "select * from %s a where k1 + 1 in " \
            "(select * from (select k4 from %s where k1 > 10 union select k1 from %s b where k1 > 10) tmp)" \
            % (baseall, dup_table, dup_table)
    runner.check2_palo(line1, line2, True)
    line1 = "select k1, count(*) from %s a where k1 + 1 in " \
            "(select bitmap_or(bm_k4, to_bitmap(k1)) from %s b where k1 > 100) group by k1 order by k1" \
            % (test_tb, k1_tb)
    line2 = "select k1, count(*) from %s a where k1 + 1 in " \
            "(select * from (select k4 from %s where k1 > 100 union " \
            "select k1 from %s b where k1 > 100) tmp) group by k1 order by k1" \
            % (test_tb, dup_table, dup_table)
    runner.check2_palo(line1, line2, True)
    line1 = "select k1, count(*) from %s a where k1 + 1 in " \
            "(select bitmap_or(bm_k4, to_bitmap(k1)) from %s b where k1 > 100) " \
            "group by k1 having count(*) < 200 order by k1" % (test_tb, k1_tb)
    line2 = "select k1, count(*) from %s a where k1 + 1 in " \
            "(select * from (select k4 from %s where k1 > 100 union " \
            "select k1 from %s b where k1 > 100) tmp) group by k1 having count(*) < 200 order by k1" \
            % (test_tb, dup_table, dup_table)
    runner.check2_palo(line1, line2, True)
    line1 = "select * from %s where k1 in (select bitmap_and(to_bitmap(k2), bm_k1_k2_k3) from %s) " \
            "order by k1" % (baseall, k1_k2_k3_tb)
    line2 = "select * from %s where k1 in (select k2 from %s) order by k1" % (baseall, dup_table)
    runner.check2_palo(line1, line2)
    line1 = "select count(*) from %s a where cast(k10 as bigint) in " \
            "(select bitmap_or(bm_k4, to_bitmap(k1)) from %s b) group by k1 order by k1" % (test_tb, k1_tb)
    line2 = "select count(*) from %s a where cast(k10 as bigint) in " \
            "(select * from (select k4 from %s union select k1 from %s b) tmp) group by k1 order by k1" \
            % (test_tb, dup_table, dup_table)
    runner.check2_palo(line1, line2, True)
    

def test_bitmap_filter_query_1():
    """
    {
    "title": "test_bitmap_filter_query_1",
    "describe": "子查询与join/union/with/view等联合使用",
    "tag": "function,p0"
    }
    """
    # join
    line1 = 'select * from %s a join (select k1 from %s where k4 in (select bm_k4 from %s)) b ' \
            'order by a.k4, a.k1, b.k1 limit 1000' % (test_tb, baseall, k1_tb)
    line2 = 'select * from %s a join (select k1 from %s where k4 in (select k4 from %s)) b ' \
            'order by a.k4, a.k1, b.k1 limit 1000' % (test_tb, baseall, dup_table)
    runner.check2_palo(line1, line2)
    line1 = 'select * from %s a join (select k1 from %s where k4 not in (select bm_k4 from %s)) b ' \
            'on a.k1 = b.k1 order by a.k4, b.k1' % (test_tb, baseall, k1_tb)
    line2 = 'select * from %s a join (select k1 from %s where k4 not in ' \
            '(select k4 from %s where k4 is not null)) b on a.k1 = b.k1 order by a.k4, b.k1' \
            % (test_tb, baseall, dup_table)
    runner.check2_palo(line1, line2)
    line1 = 'select * from (select k1 from %s where k4 not in (select bm_k4 from %s)) a join ' \
            '(select k1 from %s where k4 in (select bm_k4 from %s)) b on a.k1 = b.k1 order by a.k1, b.k1' \
            % (test_tb, k1_tb, baseall, k1_tb)
    line2 = 'select * from (select k1 from %s where k4 not in (select k4 from %s where k4 is not null)) a ' \
            'join (select k1 from %s where k4 in (select k4 from %s)) b on a.k1 = b.k1 order by a.k1, b.k1' \
            % (test_tb, dup_table, baseall, dup_table)
    runner.check2_palo(line1, line2)
    line1 = 'select * from (select k1 from %s where k4 not in (select bm_k4 from %s)) a left join ' \
            '(select k1 from %s where k4 in (select bm_k4 from %s)) b on a.k1 = b.k1 order by 1' \
            % (test_tb, k1_tb, baseall, k1_tb)
    line2 = 'select * from (select k1 from %s where k4 not in (select k4 from %s where k4 is not null)) a ' \
            'left join (select k1 from %s where k4 in (select k4 from %s)) b on a.k1 = b.k1 order by 1' \
            % (test_tb, dup_table, baseall, dup_table)
    runner.check2_palo(line1, line2)
    # union/except/intersect
    line1 = 'select k1 from %s a where k1 in (select bitmap_or(bm_k4, to_bitmap(k1)) from %s b where k1 > 10) union ' \
            'select k1 from %s a where not k1 in (select bitmap_or(bm_k4, to_bitmap(k1)) from %s b where k1 > 10)' \
            % (baseall, k1_tb, baseall, k1_tb)
    line2 = 'select k1 from %s a where k1 in (select k4 from ' \
            '(select k4 from %s b where k1 > 10 union select k1 from %s where k1 > 10) tmp) union ' \
            'select k1 from %s a where not k1 in (select k4 from ' \
            '(select k4 from %s b where k1 > 10 union select k1 from %s where k1 > 10) tmp)' \
            % (baseall, dup_table, dup_table, baseall, dup_table, dup_table)
    runner.check2_palo(line1, line2, True)
    line1 = 'select k1 from %s a where k1 in (select bitmap_or(bm_k4, to_bitmap(k1)) from %s b where k1 > 5) except ' \
            'select k1 from %s a where not k1 in (select bitmap_or(bm_k4, to_bitmap(k1)) from %s b where k1 > 10)' \
            % (baseall, k1_tb, baseall, k1_tb)
    line2 = 'select k1 from %s a where k1 in (select k4 from ' \
            '(select k4 from %s b where k1 > 10 union select k1 from %s where k1 > 5) tmp) except ' \
            'select k1 from %s a where not k1 in (select k4 from ' \
            '(select k4 from %s b where k1 > 10 union select k1 from %s where k1 > 10) tmp)' \
            % (baseall, dup_table, dup_table, baseall, dup_table, dup_table)
    runner.check2_palo(line1, line2, True)
    line1 = 'select k1 from %s a where k1 in (select bitmap_or(bm_k4, to_bitmap(k1)) from %s b where k1 > 5) intersect ' \
            'select k1 from %s a where not k1 in (select bitmap_or(bm_k4, to_bitmap(k1)) from %s b where k1 > 10)' \
            % (baseall, k1_tb, baseall, k1_tb)
    line2 = 'select k1 from %s a where k1 in (select k4 from ' \
            '(select k4 from %s b where k1 > 10 union select k1 from %s where k1 > 5) tmp) intersect ' \
            'select k1 from %s a where not k1 in (select k4 from ' \
            '(select k4 from %s b where k1 > 10 union select k1 from %s where k1 > 10) tmp)' \
            % (baseall, dup_table, dup_table, baseall, dup_table, dup_table)
    runner.check2_palo(line1, line2, True)


def test_bitmap_filter_query_2():
    """
    {
    "title": "test_bitmap_filter_query_2",
    "describe": "子查询与join/union/with/view等联合使用",
    "tag": "function,p0"
    }
    """
    # group by/having/order by/limit/
    line1 = 'select distinct k1 from %s a where k1 in ' \
            '(select bitmap_or(bm_k4, to_bitmap(k1)) from %s b where k1 > 100) order by k1 limit 10' % (test_tb, k1_tb)
    line2 = 'select distinct k1 from %s a where k1 in (select k4 from ' \
            '(select k4 from %s b where k1 > 100 union select k1 from %s where k1 > 100) tmp ) ' \
            'order by k1 limit 10' % (test_tb, dup_table, dup_table)
    runner.check2_palo(line1, line2)
    line1 = 'select count(distinct k1) from %s a where k1 in ' \
            '(select bitmap_or(bm_k4, to_bitmap(k1)) from %s b where k1 > 100)' % (test_tb, k1_tb)
    line2 = 'select count(distinct k1) from %s a where k1 in ' \
            '(select k4 from (select k4 from %s b where k1 > 100 union select k1 from %s where k1 > 100) tmp)' \
            % (test_tb, dup_table, dup_table)
    runner.check2_palo(line1, line2)
    line1 = 'select k1, count(*) from %s a where k1 in ' \
            '(select bitmap_or(bm_k4, to_bitmap(k1)) from %s b where k1 > 100) ' \
            'group by k1 having count(*) > 200 order by k1 desc limit 10 offset 20' % (test_tb, k1_tb)
    line2 = 'select k1, count(*) from %s a where k1 in ' \
            '(select k4 from (select k4 from %s b where k1 > 100 union select k1 from %s where k1 > 100) tmp) ' \
            'group by k1 having count(*) > 200 order by k1 desc limit 10 offset 20' % (test_tb, dup_table, dup_table)
    runner.check2_palo(line1, line2)
    line1 = 'select count(*) from %s a where k4 in ' \
            '(select bitmap_union(bm_k4) from %s b group by k1 order by k1 limit 10)' % (test_tb, k1_tb)
    line2 = 'select count(*) from %s a where k4 in ' \
            '(select k4 from %s where k1 in ' \
            '(select k1 from %s b group by k1 order by k1 limit 10))' % (test_tb, dup_table, dup_table)
    runner.check2_palo(line1, line2)
    # subquery
    line1 = 'select k1, count(*) from %s group by k1 having k1 in ' \
            '(select bitmap_union(bm_k4) from %s b group by k1 order by k1 limit 10)' % (test_tb, k1_tb)
    msg = 'HAVING clause dose not support in bitmap syntax'
    # runner.checkwrong(line1, msg)
    line1 = 'select count(*) from ' \
            '(select k1, count(*) from %s where k4 in (select bitmap_union(bm_k4) from %s) group by k1) tmp' \
            % (test_tb, k1_tb)
    line2 = 'select count(*) from (select k1, count(*) from %s where k4 in (select k4 from %s) group by k1) tmp' \
            % (test_tb, dup_table)
    runner.check2_palo(line1, line2)
    line1 = 'select case (select k1 from %s where k4 in (select bitmap_union(bm_k4) from %s) order by k1 limit 1) ' \
            'when 1.5 then k1 else "no" end a from %s b order by a' % (baseall, k1_tb, bigtable)
    line2 = 'select case (select k1 from %s where k4 in (select k4 from %s) order by k1 limit 1) ' \
            'when 1.5 then k1 else "no" end a from %s b order by a' % (baseall, dup_table, bigtable)
    runner.check2_palo(line1, line2)
    line1 = 'select case (select max(k1) from %s where k4 in (select bitmap_union(bm_k4) from %s)) ' \
            'when 1.5 then k1 else "no" end a from %s b order by a' % (baseall, k1_tb, bigtable)
    line2 = 'select case (select max(k1) from %s where k4 in (select k4 from %s)) ' \
            'when 1.5 then k1 else "no" end a from %s b order by a' % (baseall, dup_table, bigtable)
    runner.check2_palo(line1, line2)


def test_bitmap_filter_query_3():
    """
    {
    "title": "test_bitmap_filter_query_3",
    "describe": "子查询与join/union/with/view等联合使用",
    "tag": "function,p0"
    }
    """
    # view
    view_name = 'bmv'
    line1 = 'drop view if exists %s' % view_name
    runner.checkok(line1)
    line1 = "CREATE VIEW `%s` AS SELECT k1 FROM %s WHERE k4 IN ((SELECT bm_k4 FROM %s))" \
            % (view_name, baseall, k1_tb)
    runner.checkok(line1)
    line1 = "select a.k1, count(*) from %s a join %s b on a.k1=b.k1 group by a.k1 order by a.k1" \
            % (test_tb, view_name)
    line2 = "select a.k1, count(*) from %s a join (select k1 from %s where k4 in (select k4 from %s)) b " \
            "on a.k1=b.k1 group by a.k1 order by a.k1" % (test_tb, baseall, dup_table)
    runner.check2_palo(line1, line2)
    line1 = "select count(*) from %s a join %s b on a.k1=b.k1" % (test_tb, view_name)
    line2 = "select count(*) from %s a join (select k1 from %s where k4 in (select k4 from %s)) b " \
            "on a.k1=b.k1" % (test_tb, baseall, dup_table)
    runner.check2_palo(line1, line2)
    line1 = 'drop view if exists %s' % view_name
    runner.checkok(line1)
    # with
    line1 = "with w as (select k1 from %s where k4 in (select bm_k4 from %s)) select * from w order by k1" \
            % (baseall, k1_tb)
    line2 = "select k1 from %s where k4 in (select k4 from %s) order by k1" % (baseall, dup_table)
    runner.check2_palo(line1, line2)
    line1 = "with w1 as (select k1 from %s where k4 in (select bm_k4 from %s)), " \
            "w2 as (select k2 from %s where k1 in (select bm_k1_k2_k3 from %s)) " \
            "select * from w1 union all select * from w2 order by 1" % (baseall, k1_tb, baseall, k1_k2_k3_tb)
    line2 = "select k1 from %s where k4 in (select k4 from %s) union all " \
            "select k2 from %s where k1 in (select k1 from %s)" % (baseall, dup_table, baseall, dup_table)
    runner.check2_palo(line1, line2, True)
    

if __name__ == '__main__':
    setup_module()

