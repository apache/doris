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
file : test_query_with.py
test with sql
"""
import os
import sys
import time
sys.path.append("../lib/")
from palo_qe_client import QueryBase
import query_util as util

sys.path.append("../../../lib/")
import palo_logger
LOG = palo_logger.Logger.getLogger()
L = palo_logger.StructedLogMessage

table_name = "test"
join_name = "baseall"

if 'FE_DB' in os.environ.keys():
    db = os.environ["FE_DB"]
else:
    db = "test_query_qa"

def setup_module():
    """
    init config
    """
    global runner
    runner = QueryExecuter()


class QueryExecuter(QueryBase):
    """query executer"""
    def __init__(self):
        self.get_clients()

    def check2(self, line1, line2, force_order=False):
        """check """
        print(line1)
        print(line2)
        times = 0
        flag = 0
        while (times <= 10 and flag == 0):
            try:
                LOG.info(L('palo sql 1', palo_sql_1=line1))
                palo_result_1 = self.query_palo.do_sql(line1)
                LOG.info(L('palo sql 2', palo_sql_2=line2))
                palo_result_2 = self.query_palo.do_sql(line2)
                util.check_same(palo_result_1, palo_result_2, force_order)
                flag = 1
            except Exception as e:
                print(Exception, ":", e)
                print("hello")
                LOG.error(L('err', error=e))
                time.sleep(1)
                times += 1
                if (times == 3):
                    assert 0 == 1


def test_with_basic():
    """
    {
    "title": "test_query_with.test_with_basic",
    "describe": "test with basic use",
    "tag": "function,p1,fuzz"
    }
    """
    """test with basic use"""
    line1 = 'select 1 from (with w as (select 1 from baseall where exists (select 1 from baseall)) \
            select 1 from w ) tt;'
    line2 = 'select 1 from baseall'
    runner.check2(line1, line2)

    line1 = '(with t2 as (select "c", "d") select * from t2) union all (with t3 as (select "e", "f") \
            select * from t3);'
    line2 = 'select "c", "d" union all select "e", "f"'
    runner.check2(line1, line2, True)

    line1 = 'with t1 as (select "a", "b") (with t2 as (select "c", "d") select * from t2) union all \
            (with t3 as (select "e", "f") select * from t3);'
    runner.check2(line1, line2, True)

    line1 = 'with t1(a, b) as (select k1, k2 from baseall order by k1), \
            t2(c, d) as (select k1+100 k1, k2+100 k2 from baseall order by k1), \
            t3(e, f) as (select 100, 10) select * from t1 j full outer join t2 d on j.a + 100 = d.c'
    line2 = 'select * from (select k1 a, k2 b from baseall order by a) j full outer join \
            (select k1+100 c, k2+100 d from baseall order by c) d on j.a + 100 = d.c'
    runner.check2(line1, line2, True)

    line1 = 'with t1(a, b) as (select k1, k2 from baseall order by k1), \
            t2(c, d) as (select k1+100 k1, k2+100 k2 from baseall order by k1), \
            t3(e, f) as (select 100, 10) select t1.a, t1.b, t2.c, t2.d, t3.e, t3.f from t1, t2, t3;'
    line2 = 'select t1.a, t1.b, t2.c, t2.d, t3.e, t3.f \
            from (select k1 a, k2 b from baseall order by a) t1, \
            (select k1+100 c, k2+100 d from baseall order by c) t2, (select 100 e, 10 f) t3;'
    runner.check2(line1, line2, True)

    line1 = 'with t1(a, b) as (select k1, k2 from baseall order by k1), \
            t2(c, d) as (select k1+100 k1, k2+100 k2 from baseall order by k1), \
            t3(e, f) as (select 100, 10) \
            select t1.a, t1.b, t2.c, t2.d, t3.e, t3.f \
            from t1, t2, t3 where t1.a = t2.c-100 and t1.a = t3.f'
    line2 = 'select t1.a, t1.b, t2.c, t2.d, t3.e, t3.f \
            from (select k1 a, k2 b from baseall order by a) t1, \
            (select k1+100 c, k2+100 d from baseall order by c) t2, (select 100 e, 10 f) t3 \
            where t1.a = t2.c - 100 and t1.a = t3.f'
    runner.check2(line1, line2, True)

    line1 = 'with t1(a, b) as (select k1, k2 from baseall order by k1), \
            t2(c, d) as (select k1+100 k1, k2+100 k2 from baseall order by k1), \
            t3(e, f) as (select 100, 10) select * from t1, t2, t3 where t1.a = 1 and t2.c = 101;'
    line2 = 'select t1.a, t1.b, t2.c, t2.d, t3.e, t3.f \
            from (select k1 a, k2 b from baseall order by a) t1, \
            (select k1+100 c, k2+100 d from baseall order by c) t2, (select 100 e, 10 f) t3 \
            where t1.a = 1 and t2.c = 101'
    runner.check2(line1, line2, True)

    line1 = 'with t1(a, b) as (select k1, k2 from baseall), \
            t2(c, d) as (select k1+100, k2+100 from baseall), \
            t3(e, f) as (select 100, 10) select * from t2;'
    line2 = 'select k1+100, k2+100 from baseall'
    runner.check2(line1, line2, True)

    line1 = 'with t3 as (select 100, 10) select * from t3;'
    line2 = 'select 100, 10'
    runner.check2(line1, line2, True)

    line1 = 'with t1(a, b) as (select k1, k2 from baseall), \
            t2(c, d) as (select k1+100, k2+100 from baseall), \
            t3 as (select 100, 10) select * from t1 union select * from t2 union select * from t3;'
    line2 = 'select k1, k2 from baseall union select k1+100, k2+100 \
             from baseall union select 100, 10'
    runner.check2(line1, line2, True)

    line1 = 'with t(n) as (select k1 x, k2 y from %s.baseall) select * from t;' % db
    line2 = 'select k1 x, k2 y from %s.baseall' % db
    runner.check2(line1, line2, True)

    line1 = 'with t(m, n) as (select k1 x, k2 y from %s.baseall) select * from t;' % db
    runner.check2(line1, line2, True)

    # expect false
    line = 'with t(n, m, g) as (select k1 x, k2 y from %s.baseall) select * from t ;' % db
    runner.checkwrong(line)

    line1 = 'with t as (select k1 x, k2 y from %s.baseall) \
            select count(x), count(y) from t;' % db
    line2 = 'select count(k1), count(k2) from baseall'
    runner.check2(line1, line2)


def test_with_impala():
    """
    {
    "title": "test_query_with.test_with_impala",
    "describe": "impala with test case",
    "tag": "function,p1,fuzz"
    }
    """
    """impala with test case"""
    line1 = 'with t as (select k3 x, k4 y from %s.test) \
            select count(x), count(y) from t' % db
    line2 = 'select count(k3), count(k4) from test'
    runner.check2(line1, line2)

    line = 'drop view if exists a'
    runner.checkok(line)
    line = 'create view a as select * from baseall'
    runner.checkok(line)
    line1 = 'with t as (select k5 x, k6 y from %s.a) \
            select x, y from t order by x, y limit 10' % db
    line2 = 'select k5, k6 from baseall order by k5, k6 limit 10'
    runner.check2(line1, line2)
    line = 'drop view a'
    print(line)
    runner.checkok(line)

    # Basic tests with a single with-clause view with column labels.
    line1 = 'with t(c1, c2) as (select k10, k11 y from %s.test) \
            select * from t order by c1 limit 1' % db
    line2 = 'select k10, k11 y from %s.test order by k10 limit 1' % db
    runner.check2(line1, line2)

    line1 = 'with t(c1) as (select k7, k8 from %s.test) \
            select * from t order by c1 limit 1' % db
    line2 = 'select k7, k8 from %s.test order by k7 limit 1' % db
    runner.check2(line1, line2)

    line = 'with t(c1, c2) as (select k7 from %s.baseall) \
            select * from t limit 1' % db
    runner.checkwrong(line)

    line1 = 'with t1 as (select k1 x, k2 y from test), \
            t2 as (select 1 x, 10 y), t3 as (select 2 x, 20 y) \
            select x, y from t2'
    line2 = 'select 1 x, 10 y'
    runner.check2(line1, line2)

    line1 = 'with t1 as (select k1 x, k2 y from test), \
            t2 as (select 1 x, 10 y), t3 as (select 2 x, 20 y union select 3, 30) \
            select * from t1 union all select * from t2 union all (select * from t3) \
            order by x, y limit 20'
    line2 = 'select k1 x, k2 y from test union all select 1 x, 10 \
            union all (select 2 x, 20 y union select 3, 30) order by x, y limit 20'
    runner.check2(line1, line2)

    line1 = 'with t1(c1, c2) as (select k8 x, k9 y from baseall), \
            t2(c3, c4) as (select 1 x, 10 y) \
            select * from t1 order by c1 limit 1 union all select * from t2 limit 1'
    line2 = 'select k8 x, k9 y from baseall order by k8 limit 1 union all select 1 x, 10 y'
    # bug
    runner.check2(line1, line2)

    line1 = 'with t1 as (select k1 x, k2 y from baseall order by k1 limit 2), \
            t2 as (select k1 x, k2 y from baseall order by k1 limit 2), \
            t3 as (select k1 x, k2 y from baseall order by k1 limit 2) \
            select * from t1, t2, t3 where t1.x = t2.x and t2.x = t3.x'
    line2 = 'select * from (select k1 x, k2 y from baseall order by k1 limit 2) t1, \
            (select k1 x, k2 y from baseall order by k1 limit 2) t2, \
            (select k1 x, k2 y from baseall order by k1 limit 2) t3 \
            where t1.x = t2.x and t2.x = t3.x'
    runner.check2(line1, line2)

    line1 = 'with t as (select k1 x, k4 y from baseall order by k1 limit 2) \
            select * from t t1 left outer join t t2 on t1.y = t2.x full outer join t t3 on t2.y = t3.x \
            order by t1.x limit 10'
    line = 'show tables'

    line = 'create view a as select k1 x, k4 y from baseall order by k1 limit 2'
    runner.checkok(line)
    line2 = 'select * from a t1 left outer join a t2 on t1.y = t2.x full outer join a t3 on t2.y = t3.x \
            order by t1.x limit 10'
    runner.check2(line1, line2)
    line = 'drop view a'
    runner.checkok(line)

    line1 = 'with t1 as (select k1, count(*) from test group by 1 \
            order by 1 desc limit 10) select * from t1;'
    line2 = 'select k1, count(*) from test group by 1 order by 1 desc limit 10'
    runner.check2(line1, line2)


def test_query_with_join_1():
    """
    {
    "title": "test_query_with.test_query_with_join_1",
    "describe": "test for function join",
    "tag": "function,p1"
    }
    """
    """
    test for function join
    """
    line1 = "with t1 as (select * from %s), t2 as (select * from %s) \
            select j.*, d.* from t1 j full outer join t2 d on (j.k1=d.k1) \
            order by j.k1, j.k2, j.k3, j.k4, d.k1, d.k2 limit 100" % (join_name, table_name)
    line2 = "select * from (select j.k1 j1, j.k2 j2, j.k3 j3, j.k4 j4, j.k5 j5, j.k6 j6, j.k10 j10, j.k11 j11, \
             j.k7 j7, j.k8 j8, j.k9 j9, d.k1 d1, d.k2 d2, d.k3 d3, d.k4 d4, d.k5 d5, d.k6 d6, d.k10 d10, \
             d.k11 d11, d.k7 d7, d.k8 d8, d.k9 d9 from %s j left join %s d on (j.k1=d.k1) \
             union select j.k1 j1, j.k2 j2, j.k3 j3, j.k4 j4, j.k5 j5, j.k6 j6, j.k10 j10, j.k11 j11, \
             j.k7 j7, j.k8 j8, j.k9 j9, d.k1 d1, d.k2 d2, d.k3 d3, d.k4 d4, d.k5 d5, d.k6 d6, d.k10 d10, d.k11 d11, \
             d.k7 d7, d.k8 d8, d.k9 d9 from %s j right join %s d on (j.k1=d.k1) ) a order by j1, j2, j3, j4, d1, d2 \
             limit 100"  % (join_name, table_name, join_name, table_name)
    runner.check2(line1, line2)

    line1 = "with a1 as (select * from %s), a2 as (select * from %s) \
            select sum(t1.k1), sum(t1.k3), max(t1.k5), max(t2.k4) from a1 t1 inner join a2 t2 \
            on t1.k1 = t2.k1 and t1.k6 is not null and t2.k6 is not null" % (table_name, join_name)
    line2 = "select sum(t1.k1), sum(t1.k3), max(t1.k5), max(t2.k4) from %s t1 inner join %s t2 on t1.k1 = t2.k1 and \
            t1.k6 is not null and t2.k6 is not null" % (table_name, join_name)
    runner.check2(line1, line2)

    line1 = 'with a1 as (select * from %s)\
            select k1, k2, k3 from a1 where k7 is not null order by 1 desc, 2 desc, 3 desc \
            limit 10' % table_name
    line2 = "select k1, k2, k3 from %s where k7 is not null order by 1 desc, 2 desc, 3 desc \
            limit 10" % table_name
    runner.check2(line1, line2)

    line1 = "with a1 as (select * from %s), a2 as (select * from %s) \
            select c.k1, c.k8 from a1 d join (select a.k1 as k1, a.k8 as k8 from a2 a join a1 b on (a.k1=b.k1)) c\
            on c.k1 = d.k1 order by 1, 2" % (join_name, table_name)
    line2 = "select c.k1, c.k8 from %s d join (select a.k1 as k1, a.k8 as k8 from %s a join %s b on (a.k1=b.k1)) c\
            on c.k1 = d.k1 order by 1, 2" % (join_name, table_name, join_name)
    runner.check2(line1, line2)

    line1 = "with a1 as (select * from %s), a2 as (select * from %s) \
            select a.k1, b.k1 from a1 a join (select k1, k2 from a2 order by k1 limit 10) b \
            on a.k2=b.k2 order by 1, 2" % (join_name, table_name)
    line2 = "select a.k1, b.k1 from %s a join (select k1, k2 from %s order by k1 limit 10) b \
            on a.k2=b.k2 order by 1, 2" % (join_name, table_name)
    runner.check2(line1, line2)

    line1 = "with a1 as (select * from %s), a2 as (select * from %s) \
            select a.k1, b.k2 from a1 as a join (select k1, k2 from a2) as b\
            where a.k1 = b.k1 order by a.k1, b.k2" % (join_name, table_name)
    line2 = "select a.k1, b.k2 from %s as a join (select k1, k2 from %s) as b\
            where a.k1 = b.k1 order by a.k1, b.k2" % (join_name, table_name)
    runner.check2(line1, line2)

    line1 = "with a1 as (select * from %s), a2 as (select * from %s) \
            select A.k1,B.k1 from a1 as A join a2 as B where A.k1=B.k1+1 \
            order by A.k1, A.k2, A.k3, A.k4" % (join_name, table_name)
    line2 = "select A.k1,B.k1 from %s as A join %s as B where A.k1=B.k1+1 \
            order by A.k1, A.k2, A.k3, A.k4" % (join_name, table_name)
    runner.check2(line1, line2)

    line1 = "with a1 as (select * from %s), a2 as (select * from %s) \
            select A.k1, B.k2 from a1 as A join a2 as B \
            order by A.k1, B.k2 limit 10" % (join_name, table_name)
    line2 = "select A.k1, B.k2 from %s as A join %s as B \
            order by A.k1, B.k2 limit 10" % (join_name, table_name)
    runner.check2(line1, line2)

    line1 = "with a1 as (select * from %s), a2 as (select * from %s) \
            select a.k4 from a1 a inner join a2 b on (a.k1=b.k1) \
            where a.k2>0 and b.k1=1 and a.k1=1 order by 1" % (table_name, join_name)
    line2 = "select a.k4 from %s a inner join %s b on (a.k1=b.k1) \
            where a.k2>0 and b.k1=1 and a.k1=1 order by 1" %(table_name, join_name)
    runner.check2(line1, line2)


def test_query_with_join_2():
    """
    {
    "title": "test_query_with.test_query_with_join_2",
    "describe": "test join with",
    "tag": "function,p1"
    }
    """
    """test join with"""
    line1 = "with a1 as (select * from %s), a2 as (select * from %s) \
            select j.*, d.* from a1 j inner join a2 d on (j.k1=d.k1) \
            order by j.k1, j.k2, j.k3, j.k4" % (table_name, join_name)
    line2 = "select j.*, d.* from %s j inner join %s d on (j.k1=d.k1) \
            order by j.k1, j.k2, j.k3, j.k4" % (table_name, join_name)
    runner.check2(line1, line2)

    line1 = "with a1 as (select * from %s), a2 as (select * from %s) \
            select a.k1, b.k2, c.k3 \
            from a1 a join a2 b on (a.k1=b.k1) join a1 c on (a.k1 = c.k1)\
            where a.k2>0 and a.k1+50<0" % (table_name, join_name)
    line2 = "select a.k1, b.k2, c.k3 \
            from %s a join %s b on (a.k1=b.k1) join %s c on (a.k1 = c.k1)\
            where a.k2>0 and a.k1+50<0" % (table_name, join_name, table_name)
    runner.check2(line1, line2)

    line1 = "with a1 as (select * from %s), a2 as (select * from %s) \
            select t1.k1, t2.k1 from a1 t1 join a2 t2 where (t1.k1<3 and t2.k1<3)\
            order by t1.k1, t2.k1 limit 100" % (table_name, join_name)
    line2 = "select t1.k1, t2.k1 from %s t1 join %s t2 where (t1.k1<3 and t2.k1<3)\
            order by t1.k1, t2.k1 limit 100" % (table_name, join_name)
    runner.check2(line1, line2)

    line1 = "with a1 as (select * from %s), a2 as (select * from %s) \
            select a.k1, b.k1, a.k2, b.k2 from\
                (select k1, k2 from a1 where k9>0 and k6='false' union all\
            select k1, k2 from a2 where k6='true' union all\
            select 0, 0) a inner join\
             a1 b on a.k1=b.k1 and b.k1<5 order by 1, 2, 3, 4" \
            % (table_name, join_name)
    line2 = "select a.k1, b.k1, a.k2, b.k2 from\
            (select k1, k2 from %s where k9>0 and k6='false' union all\
            select k1, k2 from %s where k6='true' union all\
            select 0, 0) a inner join\
            %s b on a.k1=b.k1 and b.k1<5 order by 1, 2, 3, 4"\
            % (table_name, join_name, table_name)
    runner.check2(line1, line2)


def test_query_with_join_3():
    """
    {
    "title": "test_query_with.test_query_with_join_3",
    "describe": "test_query_with_join_3",
    "tag": "function,p1"
    }
    """
    """test_query_with_join_3"""
    line1 = "with t(ak1, ak2, bk1, bk2) as (select a.k1, b.k1, a.k2, b.k2 from\
            %s b left outer join\
            (select k1, k2 from %s where k9>0 and k6='false' union all\
            select k1, k2 from %s where k6='true' union all select 0, 0) a \
             on a.k1=b.k1 where b.k1<5 and a.k1 is not NULL order by 1, 2, 3, 4) select * from t \
            order by ak1, ak2, bk1, bk2" \
           % (table_name, join_name, table_name)
    line2 = "select a.k1, b.k1, a.k2, b.k2 from\
             %s b left outer join\
            (select k1, k2 from %s where k9>0 and k6='false' union all\
            select k1, k2 from %s where k6='true' union all select 0, 0) a \
            on a.k1=b.k1 where b.k1<5 and a.k1 is not NULL order by 1, 2, 3, 4"\
            % (table_name, join_name, table_name)
    runner.check2(line1, line2)

    line1 = "with t(ak1, ak2, bk1, bk2) as (select a.k1, b.k1, a.k2, b.k2 from\
            (select k1, k2 from %s where k1=1 and lower(k6) like '%%w%%' union all\
            select k1, k2 from %s where k1=2  union all\
            select 0, 1) a  join\
            (select k1, k2 from %s where k1=1 and lower(k6) like '%%w%%' union all\
            select k1, k2 from %s where k1>0  union all\
            select 1, 2) b on a.k1 = b.k1 where b.k1<5 order by 1, 2, 3, 4) select * from t \
            order by ak1, ak2, bk1, bk2" \
           % (table_name, join_name, join_name, table_name)
    line2 = "select a.k1, b.k1, a.k2, b.k2 from\
            (select k1, k2 from %s where k1=1 and lower(k6) like '%%w%%' union all\
            select k1, k2 from %s where k1=2  union all\
            select 0, 1) a  join\
            (select k1, k2 from %s where k1=1 and lower(k6) like '%%w%%' union all\
            select k1, k2 from %s where k1>0  union all\
            select 1, 2) b on a.k1 = b.k1 where b.k1<5 order by 1, 2, 3, 4"\
            % (table_name, join_name, join_name, table_name)
    runner.check2(line1, line2)

    line1 = "with t as (select count(*) from \
            (select k1 from %s union distinct\
            select k1 from %s) a inner join\
            (select k2 from %s union distinct\
            select k2 from %s) b on a.k1+1000=b.k2 inner join\
            (select distinct k1 from %s) c on a.k1=c.k1) select * from t" \
            % (table_name, join_name, join_name, table_name, table_name)
    line2 = "select count(*) from \
            (select k1 from %s union distinct\
            select k1 from %s) a inner join\
            (select k2 from %s union distinct\
            select k2 from %s) b on a.k1+1000=b.k2 inner join\
            (select distinct k1 from %s) c on a.k1=c.k1" \
            % (table_name, join_name, join_name, table_name, table_name)
    runner.check2(line1, line2)

    line1 = "with t as (select count(t1.k1) as wj from %s t1 left join\
            %s t2 on t1.k10=t2.k10 left join\
            %s t3 on t2.k1 = t3.k1) select * from t" \
            % (join_name, join_name, join_name)
    line2 = "select count(t1.k1) as wj from %s t1 left join\
            %s t2 on t1.k10=t2.k10 left join\
            %s t3 on t2.k1 = t3.k1"\
            % (join_name, join_name, join_name)
    runner.check2(line1, line2)

    line1 = "with t(jk1, jk2, jk3, jk4, jk5, jk6, jk10, jk11, jk7, jk8, jk9, \
            dk1, dk2, dk3, dk4, dk5, dk6, dk10, dk11, dk7, dk8, dk9 \
            ) as (select j.*, d.* from %s j left join %s d on (lower(j.k6) = lower(d.k6)) \
            order by j.k1, j.k2, j.k3, j.k4, d.k2, d.k3, d.k4) select * from t \
            order by jk1, jk2, jk3, jk4, dk2, dk3, dk4" % (table_name, join_name)
    line2 = "select j.*, d.* from %s j left join %s d on (lower(j.k6) = lower(d.k6)) \
            order by j.k1, j.k2, j.k3, j.k4, d.k2, d.k3, d.k4" % (table_name, join_name)
    runner.check2(line1, line2)

    line1 = "with t(jk1, jk2, jk3, jk4, jk5, jk6, jk10, jk11, jk7, jk8, jk9, \
            dk1, dk2, dk3, dk4, dk5, dk6, dk10, dk11, dk7, dk8, dk9 \
            ) as (select j.*, d.* from %s j right join %s d on (lower(j.k6) = lower(d.k6)) \
            order by j.k1, j.k2, j.k3, j.k4, d.k2, d.k3, d.k4) select * from t \
            order by jk1, jk2, jk3, jk4, dk2, dk3, dk4" % (table_name, join_name)
    line2 = "select j.*, d.* from %s j right join %s d on (lower(j.k6) = lower(d.k6)) \
            order by j.k1, j.k2, j.k3, j.k4, d.k2, d.k3, d.k4" % (table_name, join_name)
    runner.check2(line1, line2)

    line1 = "with t(a, b) as (select k1, v.k2 from %s c, (select k2 from %s order by k2 limit 2) v \
            where k1 in (1, 2, 3) order by 1, 2) select * from t order by a, b" \
            % (join_name, table_name)
    line2 = "select k1, v.k2 from %s c, (select k2 from %s order by k2 limit 2) v \
            where k1 in (1, 2, 3) order by 1, 2" % (join_name, table_name)
    runner.check2(line1, line2)

    line1 = "with t(a, b) as (select k1, v.k2 from %s c, (select k2 from %s order by k2 limit 2)v \
            where k1 in (1, 2, 3) and v.k2%%2=0 order by 1, 2) select * from t order by a, b" \
            % (join_name, table_name)
    line2 = "select k1, v.k2 from %s c, (select k2 from %s order by k2 limit 2) v \
            where k1 in (1, 2, 3) and v.k2%%2=0 order by 1, 2" % (join_name, table_name)
    runner.check2(line1, line2)


def test_query_with_join_4():
    """
    {
    "title": "test_query_with.test_query_with_join_4",
    "describe": "test for join",
    "tag": "function,p1"
    }
    """
    """
    test for join
    """
    line1 = "with t as (select k1, k2, cnt, avp from %s c, (select count(k1) cnt, avg(k2) avp from %s) v where k1 <3 \
            order by 1, 2, 3, 4) select * from t order by k1, k2, cnt, avp" % (table_name, join_name)
    line2 = "with t1 as (select * from %s), t2 as (select * from %s) \
            select k1, k2, cnt, avp from t1 c, (select count(k1) cnt, avg(k2) avp from t2) v where k1 <3 \
            order by 1, 2, 3, 4" % (table_name, join_name)
    runner.check2(line1, line2)

    line1 = "with t(k1, k2, cnt, avp) as (select k1, v.k3, cnt, avp from %s c,(select count(k1) cnt, avg(k9) avp, k3\
            from %s group by k3) v where k1<0 order by 1, 2, 3, 4) \
            select * from t order by k1, k2, cnt, avp" % (join_name, join_name)
    line2 = "with t1 as (select * from %s), t2 as (select * from %s) \
            select k1, v.k3, cnt, avp from t1 c, (select count(k1) cnt, avg(k9) avp, \
            k3 from t2 group by k3) v\
            where k1<0 order by 1, 2, 3, 4" % (join_name, join_name)
    runner.check2(line1, line2)

    line1 = "with t(a, b) as (select count(k5), k2 from %s c, (select ca.k1 okey, cb.k2 opr from %s ca, %s cb where \
             ca.k1=cb.k1 and ca.k2+cb.k2>2) v group by k2 order by 1, 2) select * from t order by a, b" \
           % (table_name, join_name, table_name)
    line2 = "with t1 as (select * from %s), t2 as (select * from %s) \
            select count(k5), k2 from t1 c, (select ca.k1 okey, cb.k2 opr from t2 ca, t1 cb where \
            ca.k1=cb.k1 and ca.k2+cb.k2>2) v group by k2 order by 1, 2" \
            % (table_name, join_name)
    runner.check2(line1, line2)

    line1 = "with t(a, b) as (select count(k6), k1 from %s c, \
            (select ca.k1 wj, ca.k2 opr from %s ca left outer join %s cb on ca.k1 = cb.k1) v\
            group by k1 order by 1, 2) select * from t order by a, b" % (table_name, join_name, table_name)
    line2 = "with t1 as (select * from %s), t2 as (select * from %s) \
            select count(k6), k1 from t1 c, \
            (select ca.k1 wj, ca.k2 opr from t2 ca left outer join t1 cb on ca.k1 = cb.k1) v\
            group by k1 order by 1, 2" % (table_name, join_name)
    runner.check2(line1, line2)


def test_query_with_union_1():
    """
    {
    "title": "test_query_with.test_query_with_union_1",
    "describe": "test for function union",
    "tag": "function,p1"
    }
    """
    """
    test for union
    """
    line1 = "(select * from %s) union (select * from %s) order by k1, k2, k3, k4 limit 4" % \
           (table_name, table_name)
    line2 = "with t as ((select * from %s) union (select * from %s) order by k1, k2, k3, k4 limit 4) \
            select * from t" % (table_name, table_name)
    line3 = "with t as (select * from %s) \
            (select * from t) union (select * from t) order by k1, k2, k3, k4 limit 4" % table_name
    runner.check2(line1, line2)
    runner.check2(line1, line3)

    line1 = "(select * from %s) union all (select * from %s) \
            order by k1, k2, k3, k4 limit 4" % (table_name, table_name)
    line2 = "with t as ((select * from %s) union all (select * from %s) \
            order by k1, k2, k3, k4 limit 4) select * from t order by k1, k2, k3, k4 " \
            % (table_name, table_name)
    line3 = "with t as (select * from %s) (select * from t) union all (select * from t) \
            order by k1, k2, k3, k4 limit 4" % table_name
    runner.check2(line1, line2)
    runner.check2(line1, line3)

    line1 = "(select * from %s where k1<10) union all \
            (select * from %s where k5<0) order by k1,k2,k3 limit 40" % \
           (table_name, table_name)
    line2 = "with t as ((select * from %s where k1<10) union all \
            (select * from %s where k5<0) order by k1,k2,k3 limit 40) select * from t \
            order by k1, k2, k3, k4" % (table_name, table_name)
    line3 = "with t as (select * from %s) (select * from t where k1<10) union all \
            (select * from t where k5<0) order by k1,k2,k3 limit 40" % table_name
    runner.check2(line1, line2)
    runner.check2(line1, line3)

    line1 = "(select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k1>0)\
            union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k2>0)\
            union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k3>0)\
            order by k1, k2, k3, k4" % (table_name, join_name, join_name)
    line2 = "with t as ((select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k1>0)\
            union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k2>0)\
            union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k3>0)\
            order by k1, k2, k3, k4) select * from t order by k1, k2, k3, k4" \
            % (table_name, join_name, join_name)
    line3 = "with t1 as (select * from %s), t2 as (select * from %s) \
            (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from t1 where k1>0)\
            union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from t2 where k2>0)\
            union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from t2 where k3>0)\
            order by k1, k2, k3, k4" % (table_name, join_name)
    runner.check2(line1, line2)
    runner.check2(line1, line3)


def test_query_with_union_2():
    """
    {
    "title": "test_query_with.test_query_with_union_2",
    "describe": "test_query_with_union_2",
    "tag": "function,p1"
    }
    """
    """test_query_with_union_2"""
    line1 = "(select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s \
            where k1>0 order by k1, k2, k3, k4 limit 1)\
            union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s \
            where k2>0 order by k1, k2, k3, k4 limit 1)\
            union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s \
            where k3>0  order by k1, k2, k3, k4 limit 1)\
            order by k1, k2, k3, k4" % (table_name, join_name, join_name)
    line2 = "with t as ((select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s \
            where k1>0 order by k1, k2, k3, k4 limit 1)\
            union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s \
            where k2>0 order by k1, k2, k3, k4 limit 1)\
            union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s \
            where k3>0  order by k1, k2, k3, k4 limit 1)\
            order by k1, k2, k3, k4) select * from t order by k1, k2, k3, k4" \
            % (table_name, join_name, join_name)
    line3 = "with t1 as (select * from %s), t2 as (select * from %s) \
            (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from t1 \
            where k1>0 order by k1, k2, k3, k4 limit 1)\
            union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from t2 \
            where k2>0 order by k1, k2, k3, k4 limit 1)\
            union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from t2 \
            where k3>0  order by k1, k2, k3, k4 limit 1)" % (table_name, join_name)
    runner.check2(line1, line2)
    runner.check2(line1, line3)

    line1 = "(select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k1>0)\
            union distinct (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k2>0)\
            union distinct (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k3>0)\
            order by k1, k2, k3, k4" % (table_name, join_name, join_name)
    line2 = "with t as ((select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k1>0)\
            union distinct (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k2>0)\
            union distinct (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k3>0)\
            order by k1, k2, k3, k4) select * from t order by k1, k2, k3, k4" \
            % (table_name, join_name, join_name)
    line3 = "with t1 as (select * from %s), t2 as (select * from %s) \
            (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from t1 where k1>0)\
            union distinct (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from t2 where k2>0)\
            union distinct (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from t2 where k3>0)\
            order by k1, k2, k3, k4" % (table_name, join_name)
    runner.check2(line1, line2)
    runner.check2(line1, line3)

    line1 = "(select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k1>0)\
            union distinct (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k2>0)\
            union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k3>0)\
            order by k1, k2, k3, k4" % (table_name, join_name, join_name)
    line2 = "with t as ((select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k1>0)\
            union distinct (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k2>0)\
            union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k3>0))\
            select * from t order by k1, k2, k3, k4" % (table_name, join_name, join_name)
    line3 = "with t1 as (select * from %s), t2 as (select * from %s) \
            (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from t1 where k1>0)\
            union distinct (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from t2 where k2>0)\
            union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from t2 where k3>0)\
            order by k1, k2, k3, k4" % (table_name, join_name)
    runner.check2(line1, line2)
    runner.check2(line1, line3)

    line1 = "(select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s \
            where k1>0 order by k1, k2, k3, k4 limit 1)\
            union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s \
            where k2>0 order by k1, k2, k3, k4 limit 1)\
            union distinct (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s \
            where k3>0  order by k1, k2, k3, k4 limit 1)\
            order by k1, k2, k3, k4" % (table_name, join_name, join_name)
    line2 = "with t as ((select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s \
            where k1>0 order by k1, k2, k3, k4 limit 1)\
            union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s \
            where k2>0 order by k1, k2, k3, k4 limit 1)\
            union distinct (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s \
            where k3>0  order by k1, k2, k3, k4 limit 1)) select * from t order by k1, k2, k3, k4" \
            % (table_name, join_name, join_name)
    line3 = "with t1 as (select * from %s), t2 as (select * from %s) \
            (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from t1 \
            where k1>0 order by k1, k2, k3, k4 limit 1)\
            union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from t2 \
            where k2>0 order by k1, k2, k3, k4 limit 1)\
            union distinct (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from t2 \
            where k3>0  order by k1, k2, k3, k4 limit 1) order by k1, k2, k3, k4" \
            % (table_name, join_name)
    runner.check2(line1, line3)
    runner.check2(line1, line2)


def test_query_with_union_3():
    """
    {
    "title": "test_query_with.test_query_with_union_3",
    "describe": "test_query_with_union_3",
    "tag": "function,p1"
    }
    """
    """test_query_with_union_3"""
    line1 = "(select count(k1), sum(k2) from %s)\
            union all (select k1, k2 from %s order by k1, k2 limit 10)\
            union all (select sum(k1), max(k3) from %s group by k2)\
            union all (select k1, k2 from %s)\
            union all (select a.k1, b.k2 from %s a join %s b on (a.k1=b.k1))\
            union all (select 1000, 2000) order by 1, 2" \
           % (table_name, table_name, table_name, join_name, table_name, join_name)
    line2 = "with t(k1, k2) as ((select count(k1), sum(k2) from %s)\
            union all (select k1, k2 from %s order by k1, k2 limit 10)\
            union all (select sum(k1), max(k3) from %s group by k2)\
            union all (select k1, k2 from %s)\
            union all (select a.k1, b.k2 from %s a join %s b on (a.k1=b.k1))\
            union all (select 1000, 2000)) select * from t order by k1, k2" \
            % (table_name, table_name, table_name, join_name, table_name, join_name)
    line3 = "with t1 as (select * from %s), t2 as (select * from %s) \
            (select count(k1), sum(k2) from t1)\
            union all (select k1, k2 from t1 order by k1, k2 limit 10)\
            union all (select sum(k1), max(k3) from t1 group by k2)\
            union all (select k1, k2 from t2)\
            union all (select a.k1, b.k2 from t1 a join t2 b on (a.k1=b.k1))\
            union all (select 1000, 2000) order by 1, 2" % (table_name, join_name)
    runner.check2(line1, line2)
    runner.check2(line1, line3)

    line1 = "select * from (select 1 a, 2 b \
            union all select 3, 4 \
            union all select 10, 20) t where a<b order by a, b"
    line2 = "with t as (select * from (select 1 a, 2 b union all select 3, 4 \
            union all select 10, 20) t where a<b order by a, b) select * from t order by a, b "
    runner.check2(line1, line2)

    line1 = "(select k1, count(*) from %s where k1=1 group by k1)\
            union distinct (select 2,3) order by 1,2" % table_name
    line2 = "with t as ((select k1, count(*) from %s where k1=1 group by k1)\
            union distinct (select 2,3)) select * from t order by 1, 2" % table_name
    line3 = "with t as (select * from %s) (select k1, count(*) from t where k1=1 group by k1)\
            union distinct (select 2,3) order by 1, 2" % table_name
    runner.check2(line1, line2)
    runner.check2(line1, line3)

    line1 = "(select 1, 'a', NULL, 10.0)\
            union all (select 2, 'b', NULL, 20.0)\
            union all (select 1, 'a', NULL, 10.0) order by 1, 2"
    line2 = "with t as ((select 1, 'a', NULL, 10.0)\
            union all (select 2, 'b', NULL, 20.0)\
            union all (select 1, 'a', NULL, 10.0)) select * from t order by 1, 2"
    runner.check2(line1, line2)

    line1 = "select count(*) from (\
            (select k1, k2, k3, k4, k5, k6, k7, k8, k9, 10, k11 from %s where k1>0)\
            union distinct (select k1, k2, k3, k4, k5, k6, k7, k8, k9, 10, k11 from %s where k2>0)\
            union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, 10, k11 from %s where k3>0)) x" \
           % (table_name, table_name, join_name)
    line2 = "with t as (select count(*) from (\
            (select k1, k2, k3, k4, k5, k6, k7, k8, k9, 10, k11 from %s where k1>0)\
            union distinct (select k1, k2, k3, k4, k5, k6, k7, k8, k9, 10, k11 from %s where k2>0)\
            union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, 10, k11 from %s where k3>0)) x) \
            select * from t" % (table_name, table_name, join_name)
    line3 = "with t1 as (select * from %s), t2 as (select * from %s) select count(*) from (\
            (select k1, k2, k3, k4, k5, k6, k7, k8, k9, 10, k11 from t1 where k1>0)\
            union distinct (select k1, k2, k3, k4, k5, k6, k7, k8, k9, 10, k11 from t1 where k2>0)\
            union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, 10, k11 from t2 where k3>0)) x" \
            % (table_name, join_name)
    runner.check2(line1, line2)
    runner.check2(line1, line3)


def test_query_with_union_4():
    """
    {
    "title": "test_query_with.test_query_with_union_4",
    "describe": "test_query_with_union_4",
    "tag": "function,p1"
    }
    """
    """test_query_with_union_4"""
    line1 = "(select 10, 10.0, 'hello', 'world') union all\
            (select k1, k5, k6, k7 from %s where k1=1) union all\
            (select 20, 20.0, 'wangjuoo4', 'beautiful') union all\
            (select k2, k8, k6, k7 from %s where k2>0) order by 1, 2, 3, 4" \
           % (join_name, join_name)
    line2 = "with t as ((select 10, 10.0, 'hello', 'world') union all\
            (select k1, k5, k6, k7 from %s where k1=1) union all\
            (select 20, 20.0, 'wangjuoo4', 'beautiful') union all\
            (select k2, k8, k6, k7 from %s where k2>0)) select * from t order by 1, 2, 3, 4" \
            % (join_name, join_name)
    line3 = "with t as (select * from %s) (select 10, 10.0, 'hello', 'world') union all\
            (select k1, k5, k6, k7 from t where k1=1) union all\
            (select 20, 20.0, 'wangjuoo4', 'beautiful') union all\
            (select k2, k8, k6, k7 from t where k2>0) order by 1, 2, 3, 4" \
            % join_name
    runner.check2(line1, line2)
    runner.check2(line1, line3)

    line1 = "select x.k1, k2, k3, k4, k5 from \
            ((select k1, k2, k3, k4, k5 from %s where k1>0) union distinct\
            (select k1, k2, k3, k4, k5 from %s where k2>0)) x \
             where x.k1<5 and x.k3>0 order by 1, 2, 3, 4" \
           % (table_name, join_name)
    line2 = "with t as (select x.k1, k2, k3, k4, k5 from \
            ((select k1, k2, k3, k4, k5 from %s where k1>0) union distinct\
            (select k1, k2, k3, k4, k5 from %s where k2>0)) x \
             where x.k1<5 and x.k3>0 order by 1, 2, 3, 4) select * from t order by 1, 2, 3, 4" \
            % (table_name, join_name)
    line3 = "with t1 as (select * from %s), t2 as (select * from %s) \
            select x.k1, k2, k3, k4, k5 from \
            ((select k1, k2, k3, k4, k5 from t1 s where k1>0) union distinct\
            (select k1, k2, k3, k4, k5 from t2 where k2>0)) x \
            where x.k1<5 and x.k3>0 order by 1, 2, 3, 4" % (table_name, join_name)
    runner.check2(line1, line2)
    runner.check2(line1, line3)

    line1 = "select x.k1, k2, k3, k4, k5 from \
            ((select k1, k2, k3, k4, k5 from %s where k1>0) union all\
            (select k1, k2, k3, k4, k5 from %s where k2>0)) x \
            where x.k1<5 and x.k3>0 order by 1, 2, 3, 4" \
            % (table_name, join_name)
    line2 = "with t as (select x.k1, k2, k3, k4, k5 from \
            ((select k1, k2, k3, k4, k5 from %s where k1>0) union all\
            (select k1, k2, k3, k4, k5 from %s where k2>0)) x \
            where x.k1<5 and x.k3>0 order by 1, 2, 3, 4) select * from t order by 1, 2, 3, 4" \
            % (table_name, join_name)
    line3 = "with t1 as (select * from %s), t2 as (select * from %s) \
            select x.k1, k2, k3, k4, k5 from \
            ((select k1, k2, k3, k4, k5 from t1 where k1>0) union all\
            (select k1, k2, k3, k4, k5 from t2 where k2>0)) x \
            where x.k1<5 and x.k3>0 order by 1, 2, 3, 4" % (table_name, join_name)
    runner.check2(line1, line2)
    runner.check2(line1, line3)

    line1 = "select x.k1, k6, k7, k8, k9, k10 from \
            ((select k1, k6, k7, k8, k9, k10 from %s where k1=1) union distinct\
            (select k1, k6, k7, k8, k9, k10 from %s where k9>0)) x union distinct\
            (select k1, k6, k7, k8, k9, k10 from %s) order by 1, 4, 5, 6 limit 10" \
            % (table_name, table_name, join_name)
    line2 = "with t as (select x.k1, k6, k7, k8, k9, k10 from \
            ((select k1, k6, k7, k8, k9, k10 from %s where k1=1) union distinct\
            (select k1, k6, k7, k8, k9, k10 from %s where k9>0)) x union distinct\
            (select k1, k6, k7, k8, k9, k10 from %s) order by 1, 4, 5, 6 limit 10) \
            select * from t order by 1, 4, 5, 6" % (table_name, table_name, join_name)
    line3 = "with t1 as (select * from %s), t2 as (select * from %s) \
            select x.k1, k6, k7, k8, k9, k10 from \
            ((select k1, k6, k7, k8, k9, k10 from t1 where k1=1) union distinct\
            (select k1, k6, k7, k8, k9, k10 from t1 where k9>0)) x union distinct\
            (select k1, k6, k7, k8, k9, k10 from t2) order by 1, 4, 5, 6 limit 10" \
            % (table_name, join_name)
    runner.check2(line1, line2)
    runner.check2(line1, line3)

    line1 = "select x.k1, k6, k7, k8, k9, k10 from \
            ((select k1, k6, k7, k8, k9, k10 from %s where k1=1) union all\
            (select k1, k6, k7, k8, k9, k10 from %s where k9>0)) x union all\
            (select k1, k6, k7, k8, k9, k10 from %s) order by 1, 4, 5, 6 limit 10" \
            % (table_name, table_name, join_name)
    line2 = "with t as (select x.k1, k6, k7, k8, k9, k10 from \
            ((select k1, k6, k7, k8, k9, k10 from %s where k1=1) union all\
            (select k1, k6, k7, k8, k9, k10 from %s where k9>0)) x union all\
            (select k1, k6, k7, k8, k9, k10 from %s) order by 1, 4, 5, 6 limit 10) \
            select * from t order by 1, 4, 5, 6" % (table_name, table_name, join_name)
    line3 = "with t1 as (select * from %s), t2 as (select * from %s) \
            select x.k1, k6, k7, k8, k9, k10 from \
            ((select k1, k6, k7, k8, k9, k10 from t1 where k1=1) union all\
            (select k1, k6, k7, k8, k9, k10 from t1 where k9>0)) x union all\
            (select k1, k6, k7, k8, k9, k10 from t2) order by 1, 4, 5, 6 limit 10" \
            % (table_name, join_name)
    runner.check2(line1, line2)
    runner.check2(line1, line3)


def test_query_with_other():
    """
    {
    "title": "test_query_with.test_query_with_other",
    "describe": "test_query_with_other multi union and union all in with",
    "tag": "function,p1"
    }
    """
    """test_query_with_other multi union and union all in with"""
    line = 'drop view if exists a_join'
    runner.checkok(line)
    line = 'create view a_join as select k1, k2, k3, k4 from %s' % join_name
    runner.checkok(line)

    sql1 = 'with t as (select k1, k2 from {table} where k1 = 1), %s select * from t'
    sql2 = 'with %s %s'
    sub1 = 't%s as (select k1, k2 from {table} where k1 = 1)'
    sub2 = 'select k1, k2 from t%s'

    # table multi with
    l = list()
    for i in range(0, 100):
        l.append(sub1 % i)
    a = ', '.join(l)
    sql = sql1 % a
    sql = sql.format(table=join_name)
    runner.checkok(sql)

    # table multi with union all & union
    l = list()
    m = list()
    for i in range(0, 100):
        l.append(sub1 % i)
        m.append(sub2 % i)
    c = ', '.join(l)
    b = ' union all '.join(m)
    sql = sql2 % (c, b)
    sql = sql.format(table=join_name)
    runner.checkok(sql)

    d = ' union '.join(m)
    sql = sql2 % (c, b)
    sql = sql.format(table=join_name)
    runner.checkok(sql)

    # view multi with select & uninon all & union
    sql = sql1 % a
    sql = sql.format(table='a_join')
    runner.checkok(sql)

    sql = sql2 % (c, b)
    sql = sql.format(table='a_join')
    runner.checkok(sql)

    sql = sql2 % (c, d)
    sql = sql.format(table='a_join')
    runner.checkok(sql)


def test_with_bug():
    """
    {
    "title": "test_query_with.test_with_bug",
    "describe": "test with bug ",
    "tag": "function,p1"
    }
    """
    """test with bug"""

    line = 'with t1(a, b) as (select k1, k2 from baseall), \
            t2(c, d) as (select k1+100, k2+100 from baseall), \
            t3 as (select 100, 10) select * from t1 union select * from t2 union select * from t3;'
    runner.checkok(line)

    line = 'select k1, k2 from baseall union select k1, k2 from baseall order by k1;'
    runner.check(line, True)
    line = '(select k1, k2 from baseall) union (select k1, k2 from baseall order by k1);'
    runner.check(line, True)

    line = ' (select k8 , k9 from baseall) union (select 1, 10);'
    runner.check(line, True)

    line = 'drop view if exists a'
    runner.check(line)
    line = 'create view a as select * from baseall;'
    runner.check(line)
    line = 'select k1 from a;'
    runner.check(line, True)

    line = 'with t1 as (select * from baseall), t2 as (select * from test) select j.*, d.* \
            from t1 j full outer join t2 d on (j.k1=d.k1) order by j.k1;'
    runner.checkok(line)

    line = 'drop view if exists a'
    runner.check(line)
    line = 'create view a as select k1 x, k4 y from baseall order by k1 limit 2;'
    runner.check(line)
    line = 'select * from a m where m.x= 1;'
    runner.check(line)
    line = 'select * from a t1 left outer join a t2 on t1.y = t2.x full outer join a t3 \
            on t2.y = t3.x order by t1.x limit 10;'
    runner.checkok(line)

    line = 'drop view if exists b'
    runner.checkok(line)
    line = 'create view b(x, y) as select k1 x, k4 y from baseall;'
    runner.checkok(line)

    line = '(select k1, k2 from baseall where k1 = 1) union all \
            (select k1, k2 from baseall where k1 = 1) union all \
            (select k1, k2 from baseall where k1 = 1) union all \
            (select k1, k2 from baseall where k1 = 1) union all \
            (select k1, k2 from baseall where k1 = 1) union all \
            (select k1, k2 from baseall where k1 = 1) union all \
            (select k1, k2 from baseall where k1 = 1);'
    runner.check(line)

    line1 = 'with t as (select k1, sum(k2) v from baseall group by k1 union all \
            select k1, count(k2) v from test group by k1) select k1 from t ;'
    line2 = 'select k1 from (select k1, sum(k2) v from baseall group by k1 union all \
            select k1, count(k2) v from test group by k1) a'
    runner.check2(line1, line2, True)

    line = 'with a as(select k1, k2, k10 from baseall), b as (select * from a), \
            c as (select t1.k10, row_number() over (partition by t1.k1 order by t1.k1) rn \
            from a t1 full outer join b t2 on (t1.k1 = t2.k1 and t1.k2 = t2.k2)) select * from c;'
    runner.checkok(line)

    line = 'with t as (select k1 from (select k1, k2, k3 from baseall where k6 in \
            (select bigtable.k6 from test, bigtable where test.k1 = bigtable.k1 \
            and test.k1 = 10)) a) select * from t;'
    runner.checkok(line)

    line = 'with t as (select * from baseall where k1 in (select k1 from baseall \
            where k1 = (select max(k1) from baseall))) select * from t;'
    runner.checkok(line)


def test_query_with_subquery_1():
    """
    {
    "title": "test_query_with.test_query_with_subquery_1",
    "describe": "test simple subquery",
    "tag": "function,p1"
    }
    """
    """
    test simple subquery
    """
    line1 = 'with t as (select c1, c3, m2 from ( select c1, c3, max(c2) m2 from (select c1, c2, c3 from (\
            select k3 c1, k2 c2, max(k1) c3 from %s group by 1, 2 order by 1 desc, 2 desc limit 5) x ) x2\
            group by c1, c3 limit 10) t where c1>0 order by 2 , 1 limit 3) \
            select * from t' % table_name
    line2 = "select c1, c3, m2 from ( select c1, c3, max(c2) m2 from (select c1, c2, c3 from (\
            select k3 c1, k2 c2, max(k1) c3 from %s group by 1, 2 order by 1 desc, 2 desc limit 5) x ) x2\
            group by c1, c3 limit 10) t where c1>0 order by 2 , 1 limit 3" % (table_name)
    runner.check2(line1, line2, True)

    line1 = 'with t as (select c1, c2 from (select k3 c1, k1 c2, min(k8) c3 from %s group by 1, 2) x\
            order by 1, 2) select * from t order by c1, c2' % table_name
    line2 = "select c1, c2 from (select k3 c1, k1 c2, min(k8) c3 from %s group by 1, 2) x\
            order by 1, 2" % (table_name)
    runner.check2(line1, line2)


def test_query_with_subquery_in():
    """
    {
    "title": "test_query_with.test_query_with_subquery_in",
    "describe": "subquery query with in",
    "tag": "function,p1"
    }
    """
    """
    subquery query with in
    """
    line1 = 'with t as (select sum(k1), k2 from %s where k2 in (select distinct k1 from %s) group by k2 \
            ) select * from t order by 1, 2' % (table_name, join_name)
    line2 = "select sum(k1), k2 from %s where k2 in (select distinct k1 from %s) group by k2 \
            order by sum(k1), k2" % (table_name, join_name)
    runner.check2(line1, line2)

    for i in range(1, 12):
        line1 = 'with t as (select * from %s where k%s in (select k%s from %s)) select * from t \
                order by k1, k2, k3, k4' % (join_name, i, i, join_name)
        line2 = "select * from %s where k%s in (select k%s from %s) order by k1, k2, k3, k4" \
                % (join_name, i, i, join_name)
        line3 = 'with t1 as (select * from %s), t2 as (select * from %s) \
                select * from t1 where k%s in (select k%s from t2) order by k1, k2, k3, k4' \
                % (join_name, join_name, i, i)
        runner.check2(line1, line2)
        runner.check2(line2, line3)

        line1 = 'with t as (select * from %s where k%s in (select k%s from %s)) select * from t \
                order by k1, k2, k3, k4' % (join_name, i, i, table_name)
        line2 = "select * from %s where k%s in (select k%s from %s) order by k1, k2, k3, k4" \
                % (join_name, i, i, table_name)
        line3 = 'with t1 as (select * from %s), t2 as (select * from %s) \
                    select * from t1 where k%s in (select k%s from t2) order by k1, k2, k3, k4' \
                % (join_name, table_name, i, i)
        runner.check2(line1, line2)
        runner.check2(line2, line3)

        line1 = 'with t as (select * from %s where k%s not in (select k%s from %s)) \
                select * from t order by k1, k2, k3, k4' % (join_name, i, i, table_name)
        line2 = "select * from %s where k%s not in (select k%s from %s) order by k1, k2, k3, k4" \
                % (join_name, i, i, table_name)
        line3 = 'with t1 as (select * from %s), t2 as (select * from %s) \
                select * from t1 where k%s not in (select k%s from t2) order by k1, k2, k3, k4' \
                % (join_name, table_name, i, i)
        runner.check2(line1, line2)
        runner.check2(line2, line3)

        line1 = 'with t as (select * from %s where k%s not in (select k%s from %s)) \
                select * from t order by k1, k2, k3, k4' % (join_name, i, i, join_name)
        line2 = "select * from %s where k%s not in (select k%s from %s) order by k1, k2, k3, k4" \
                % (join_name, i, i, join_name)
        line3 = 'with t1 as (select * from %s), t2 as (select * from %s) \
                    select * from t1 where k%s not in (select k%s from t2) order by k1, k2, k3, k4' \
                % (join_name, join_name, i, i)
        runner.check2(line1, line2)
        runner.check2(line2, line3)


def test_query_with_subquery_exist():
    """
    {
    "title": "test_query_with.test_query_with_subquery_exist",
    "describe": "subquery query with exist",
    "tag": "function,p1"
    }
    """
    """
    subquery query with exist
    """
    for i in range(1, 12):
        line1 = 'with t as (select * from %s as a where \
                exists (select * from %s as b where a.k%s = b.k2)) select * from t \
                order by k1, k2, k3, k4' % (join_name, table_name, i)
        line2 = "select * from %s as a where \
                exists (select * from %s as b where a.k%s = b.k2) order by k1, k2, k3, k4" \
                 % (join_name, table_name, i)
        line3 = 'with t1 as (select * from %s), t2 as (select * from %s) select * from t1 as a where \
                exists (select * from t2 as b where a.k%s = b.k2) order by k1, k2, k3, k4' \
                % (join_name, table_name, i)
        runner.check2(line1, line2)
        runner.check2(line2, line3)

        line1 = 'with t as (select * from %s as a where \
                exists (select * from %s as b where a.k%s = b.k2)) select * from t \
                order by k1, k2, k3, k4' % (join_name, join_name, i)
        line2 = "select * from %s as a where \
                exists (select * from %s as b where a.k%s = b.k2) order by k1, k2, k3, k4" \
                % (join_name, join_name, i)
        line3 = 'with t1 as (select * from %s), t2 as (select * from %s) select * from t1 as a where \
                exists (select * from t2 as b where a.k%s = b.k2) order by k1, k2, k3, k4' \
                % (join_name, join_name, i)
        runner.check2(line1, line2)
        runner.check2(line2, line3)


def test_subquery_with_typedata():
    """
    {
    "title": "test_query_with.test_subquery_with_typedata",
    "describe": "different types in comparion between the subqury and out query",
    "tag": "function,p1"
    }
    """
    """
    different types in comparion between the subqury and out query
    """
    for i in (2, 3, 4, 5, 8, 9):
        for j in (1, 2, 3, 4, 5, 8, 9):
            line1 = 'with t as (select k%s from %s where k%s < (select max(k%s) from %s)) \
                    select * from t order by k%s' % (i, table_name, i, j, join_name, i)
            line2 = "select k%s from %s where k%s < (select max(k%s) from %s) order by k%s" \
                    % (i, table_name, i, j, join_name, i)
            line3 = 'with t1 as (select * from %s), t2 as (select * from %s) \
                    select k%s from t1 where k%s < (select max(k%s) from t2) order by k%s' \
                    % (table_name, join_name, i, i, j, i)
            runner.check2(line1, line2)
            runner.check2(line2, line3)

            line1 = 'with t as (select a.k%s from %s as a where a.k%s < (select max(b.k%s) \
                    from %s as b  where a.k%s = b.k1)) select * from t order by k%s' \
                    % (i, join_name, i, j, join_name, i, i)
            line2 = "select a.k%s from %s as a where a.k%s < (select max(b.k%s) from %s as b \
                    where a.k%s = b.k1) order by k%s" % (i, join_name, i, j, join_name, i, i)
            line3 = 'with t1 as (select * from %s), t2 as (select * from %s) select a.k%s \
                    from t1 as a where a.k%s < (select max(b.k%s) from t2 as b where a.k%s = b.k1) \
                    order by k%s' % (join_name, join_name, i, i, j, i, i)
            runner.check2(line1, line2)
            runner.check2(line2, line3)

    for i in (10, 11):
        for j in (10, 11):
            line1 = 'with t as (select k%s from %s where k%s < (select max(k%s) from %s)) \
                    select * from t order by k%s' % (i, table_name, i, j, join_name, i)
            line2 = "select k%s from %s where k%s < (select max(k%s) from %s) \
                    order by k%s" % (i, table_name, i, j, join_name, i)
            line3 = 'with t1 as (select * from %s), t2 as (select * from %s) \
                    select k%s from t1 where k%s < (select max(k%s) from t2) \
                    order by k%s' % (table_name, join_name, i, i, j, i)
            runner.check2(line1, line2)
            runner.check2(line2, line3)


def test_subquery_with_compute():
    """
    {
    "title": "test_query_with.test_subquery_with_compute",
    "describe": "test for calculation with subquery result",
    "tag": "function,p1"
    }
    """
    """
    test for calculation with subquery result
    """
    for i in (1, 2, 3, 4, 5, 8, 9):
        for j in(1, 2, 3, 4, 5, 8, 9):
            for k in ("+", "-", "*", "/"):
                line1 = 'with t as (select k%s from %s where \
                        k%s < (select min(k%s) from %s) %s 100.0123) select * from t order by k%s' \
                        % (i, table_name, i, j, join_name, k, i)
                line2 = "select k%s from %s where k%s < (select min(k%s) from %s) %s 100.00123 \
                        order by k%s" % (i, table_name, i, j, join_name, k, i)
                line3 = 'with t1 as (select * from %s), t2 as (select * from %s) \
                        select k%s from t1 where k%s < (select min(k%s) from t2) %s 100.00123 \
                        order by k%s' % (table_name, join_name, i, i, j, k, i)
                runner.check2(line1, line2)
                runner.check2(line2, line3)

    # uncorrelated subquery with different comparision
    for k in ("<", ">", "<=", ">=", "=", "!=", "<>"):
        for func in ('max', 'min', 'count', 'sum', 'avg'):
            line1 = 'with t as (select k1 from %s where k2 %s (select %s(k2) from %s where k3 < k2)) \
                    select * from t order by k1' % (table_name, k, func, table_name)
            line2 = "select k1 from %s where k2 %s (select %s(k2) from %s where k3 < k2) \
                    order by k1" % (table_name, k, func, table_name)
            line3 = 'with t as (select * from %s) select k1 from t \
                    where k2 %s (select %s(k2) from t where k3 < k2) order by k1' \
                    % (table_name, k, func)
            runner.check2(line1, line2)
            runner.check2(line2, line3)


def test_subquery_complicate():
    """
    {
    "title": "test_query_with.test_subquery_complicate",
    "describe": "the complicated subquery",
    "tag": "function,p1"
    }
    """
    """
    the complicated subquery
    """

    line1 = 'with t as (select t1.* from %s t1 where (select count(*) from %s t3,%s t2 where\
            t3.k2=t2.k2 and t2.k1="1") > 0) select * from t order by k1, k2, k3, k4' \
            % (table_name, join_name, join_name)
    line2 = "select t1.* from %s t1 where (select count(*) from %s t3,%s t2 where\
            t3.k2=t2.k2 and t2.k1='1') > 0 order by k1,k2,k3,k4" \
            % (table_name, join_name, join_name)
    line3 = 'with a1 as (select * from %s), a2 as (select * from %s) \
            select t1.* from a1 t1 where (select count(*) from a2 t3,a2 t2 where\
            t3.k2=t2.k2 and t2.k1="1") > 0 order by k1,k2,k3,k4' % (table_name, join_name)
    runner.check2(line1, line2)
    runner.check2(line2, line3)

    line1 = 'with t as (select k6 from %s where k1 = (select max(t1.k1) from %s t1 join %s t2 on \
            t1.k1=t2.k1)) select * from t order by k6' % (table_name, table_name, join_name)
    line2 = "select k6 from %s where k1 = (select max(t1.k1) from %s t1 join %s t2 on t1.k1=t2.k1) \
             order by k6" % (table_name, table_name, join_name)
    line3 = 'with a1 as (select * from %s), a2 as (select * from %s) \
            select k6 from a1 where k1 = (select max(t1.k1) from a1 t1 join a2 t2 on t1.k1=t2.k1) \
            order by k6' % (table_name, join_name)
    runner.check2(line1, line2)
    runner.check2(line2, line3)

    line1 = 'with t as (select * from %s where k1 in (select max(k1) from %s  \
            where 0<(select max(k9) from %s\
            where 0<(select max(k8) from %s  where 0<(select max(k5) from %s\
            where 0<(select max(k4) from %s  where 0<(select sum(k4) from %s\
            where 0<(select max(k3) from %s  where 0 < (select max(k2) from %s\
            where 0<(select max(k1) from %s  where -12<(select min(k1) from %s\
            where 10 > (select avg(k1) from %s  where 5 > (select sum(k1) from %s)))))))))) ))) \
            select * from t' \
            % (table_name, table_name, table_name, table_name, table_name, table_name, table_name,
            table_name, table_name, table_name, table_name, table_name, table_name)
    line2 = "select * from %s where k1 in (select max(k1) from %s  where 0<(select max(k9) from %s\
            where 0<(select max(k8) from %s  where 0<(select max(k5) from %s\
            where 0<(select max(k4) from %s  where 0<(select sum(k4) from %s\
            where 0<(select max(k3) from %s  where 0 < (select max(k2) from %s\
            where 0<(select max(k1) from %s  where -12<(select min(k1) from %s\
            where 10 > (select avg(k1) from %s  where 5 > (select sum(k1) from %s)))))))))) ))"\
            % (table_name, table_name, table_name, table_name, table_name, table_name, table_name,\
                table_name, table_name, table_name, table_name, table_name, table_name)
    runner.check2(line1, line2)

    temp_name = "bigtable"
    line1 = 'with t as (SELECT ot1.k1 FROM %s AS ot1, %s AS nt3 \
            WHERE ot1.k1 IN (SELECT it2.k1 FROM %s AS it2 \
            JOIN %s AS it4 ON it2.k1=it4.k1)) select * from t order by k1' \
            % (join_name, temp_name, table_name, temp_name)
    line2 = "SELECT ot1.k1 FROM %s AS ot1, %s AS nt3 \
            WHERE ot1.k1 IN (SELECT it2.k1 FROM %s AS it2 \
            JOIN %s AS it4 ON it2.k1=it4.k1) order by ot1.k1" \
            % (join_name, temp_name, table_name, temp_name)
    line3 = 'with t1 as (select * from %s), t2 as (select * from %s), t3 as (select * from %s) \
            SELECT ot1.k1 FROM t1 AS ot1, t2 AS nt3 \
            WHERE ot1.k1 IN (SELECT it2.k1 FROM t3 AS it2 \
            JOIN t2 AS it4 ON it2.k1=it4.k1) order by ot1.k1' % (join_name, temp_name, table_name)
    runner.check2(line1, line2)


if __name__ == '__main__':
    setup_module()
    # test_with_basic()
    # test_with_impala()
    # test_query_with_join_1()
    # test_query_with_union_4()
    # test_query_with_other()
    # test_query_with_subquery_in()
    # test_query_with_subquery_exist()
    # test_subquery_with_typedata()
    # test_subquery_complicate()
    test_with_bug()
