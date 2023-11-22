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


def test_query_order():
    """
    {
    "title": "test_query_order_group.test_query_order",
    "describe": "query contains order",
    "tag": "function,p0"
    }
    """
    """
    query contains order
    """
    line = "select k1, k10 from %s order by 1, 2 limit 1000" % (table_name)
    runner.check(line)
    line = "select k1, k8 from %s order by 1, 2 desc limit 1000" % (table_name)
    runner.check(line)
    line = "select k4, k10 from (select k4, k10 from %s order by 1, 2 limit 1000000) as i \
		    order by 1, 2 limit 1000" % (table_name)
    runner.check(line)
    line = "select * from %s where k1<-1000 order by k1" % (table_name)
    runner.check(line)
    for i in range(1, 12):
        for j in range(1, 12):
            if i != j and j != 7 and i != 7 and i != 6 and j != 6:
                line = "select k%s, k%s from %s order by k%s, k%s" % (i, j, table_name, i, j) 
                runner.check(line)
                line = "select k%s, k%s from %s order by k%s, k%s asc" % (i, j, table_name, i, j)
                runner.check(line)
                line = "select k%s, k%s from %s order by k%s, k%s desc" % (i, j, table_name, i, j) 
                runner.check(line)


def test_query_group_1():
    """
    {
    "title": "test_query_order_group.test_query_group_1",
    "describe": "test for function group",
    "tag": "function,p0"
    }
    """
    """
    test for function group
    """
    line = "select min(k5) from %s" % (table_name)
    runner.check(line)
    line = "select max(k5) from %s" % (table_name)
    runner.check(line)
    line = "select avg(k5) from %s" % (table_name)
    runner.check(line)
    line = "select sum(k5) from %s" % (table_name)
    runner.check(line)
    line = "select count(k5) from %s" % (table_name)
    runner.check(line)
    
    line = "select min(k5) from %s group by k2 order by min(k5)" % (table_name)
    runner.check(line)
    line = "select max(k5) from %s group by k1 order by max(k5)" % (table_name)
    runner.check(line)
    line = "select avg(k5) from %s group by k1 order by avg(k5)" % (table_name)
    runner.check(line)
    line = "select sum(k5) from %s group by k1 order by sum(k5)" % (table_name)
    runner.check(line)
    line = "select count(k5) from %s group by k1 order by count(k5)" % (table_name)
    runner.check(line)
    line = "select lower(k6), avg(k8), sum(k8),count(k8),  min(k8), max(k8)\
		    from %s group by lower(k6) \
		    order by avg(k8), sum(k8),count(k8),  min(k8), max(k8)" % (table_name) 
    runner.check(line)

    line = "select k2, avg(k8) from %s group by k2 \
		    order by k2, avg(k8)" % (table_name) 
    runner.check(line)
    line = "select k2, sum(k8) from %s group by k2 \
		    order by k2, sum(k8)" % (table_name)
    runner.check(line)
    line = "select k2, count(k8) from %s group by k2 \
		    order by k2, count(k8)" % (table_name)
    runner.check(line)
    line = "select k2, min(k8) from %s group by k2 \
		    order by k2, min(k8)" % (table_name)
    runner.check(line)
    line = "select k2, max(k8) from %s group by k2 \
		    order by k2, max(k8)" % (table_name)
    runner.check(line)

    line = "select k6, avg(k8) from %s group by k6 having k6=\"true\"\
		    order by k6, avg(k8)" % (table_name) 
    runner.check(line)
    line = "select k6, sum(k8) from %s group by k6 having k6=\"true\" \
		    order by k6, sum(k8)" % (table_name)
    runner.check(line)
    line = "select k6, count(k8) from %s group by k6 having k6=\"true\" \
		    order by k6, count(k8)" % (table_name)
    runner.check(line)
    line = "select k6, min(k8) from %s group by k6 having k6=\"true\" \
		    order by k6, min(k8)" % (table_name)
    runner.check(line)
    line = "select k6, max(k8) from %s group by k6 having k6=\"true\" \
		    order by k6, max(k8)" % (table_name)
    runner.check(line)

    line = "select k2, avg(k8) from %s group by k2 having k2<=1989 \
		    order by k2, avg(k8)" % (table_name) 
    runner.check(line)
    line = "select k2, sum(k8) from %s group by k2 having k2<=1989 \
		    order by k2, sum(k8)" % (table_name)
    runner.check(line)
    line = "select k2, count(k8) from %s group by k2 having k2<=1989 \
		    order by k2, count(k8)" % (table_name)
    runner.check(line)
    line = "select k2, min(k8) from %s group by k2 having k2<=1989 \
		    order by k2, min(k8)" % (table_name)
    runner.check(line)
    line = "select k2, max(k8) from %s group by k2 having k2<=1989 \
		    order by k2, max(k8)" % (table_name)
    runner.check(line)
    line = "select count(ALL *) from %s where k5 is not null group by k1%%10 order by 1" \
		    % (table_name)
    line = "select k5, k5*2, count(*) from %s group by 1, 2 order by 1, 2,3" % (table_name)
    runner.check(line)
    line = "select k1%%3, k2%%3, count(*) from %s where k4>0 group by 2, 1 order by 1, 2 ,3" \
		    % (table_name)
    runner.check(line)
    line = "select k1%%2, k2%%2, k3%%3, k4%%3, k11, count(*) from %s \
		    where (k11='2015-03-13 12:36:38' or k11 = '2000-01-01 00:00:00')\
		    and k5 is not null group by 1, 2, 3, 4, 5 order by 1, 2, 3, 4, 5" % (table_name)
    runner.check(line)
    line = "select count(*) from %s where (k11='2015-03-13 12:36:38' or k11 = '2000-01-01 00:00:00')\
		    and k5 is not null group by k1%%2, k2%%2, k3%%3, k4%%3, k11%%2 order by 1" % (table_name)
    runner.check(line)
    line = "select count(*), min(k1), max(k1), sum(k1), avg(k1) from %s where k1=10000 order by 1" \
		    % (table_name)
    runner.check(line)
    line = "select k1 %% 7, count(*), avg(k1) from %s where k4>0 group by 1 having avg(k1) > 2 or count(*)>5\
		    order by 1, 2, 3" % (table_name)
    runner.check(line)


def test_query_group_2():
    """
    {
    "title": "test_query_order_group.test_query_group_2",
    "describe": "test for function group",
    "tag": "function,p0"
    }
    """
    """
    test for function group
    """
    line = "select k10, count(*) from %s where k5 is not null group by k10 \
		    having k10<cast('2010-01-01 01:05:20' as datetime) order by 1, 2" % (table_name)
    runner.check(line)
    '''
    bug
    line = " select count(NULL), min(NULL), max(NULL), sum(NULL), avg(NULL) from %s" % (table_name)
    runner.check(line)
    line = "select k4, group_concat(cast(k1 as char), cast((10-k1) as char)) from %s\
		    where k1<0 and k4>0 group by k4 order by 1" % (table_name)
    runner.check(line)
    '''
    line = "select k1*k1, k1+k1 as c from %s group by k1*k1, k1+k1, k1*k1 having (c)<5\
		    order by 1, 2 limit 10" % (table_name)
    runner.check(line)
    line = "select 1 from (select count(k4) c from %s having min(k1) is not null) as t \
		    where c is not null" % (table_name)
    runner.check(line)
    line = "select count(k1), sum(k1*k2) from %s order by 1, 2" % (table_name)
    runner.check(line)
    line = "select k1%%2, k2+1, k3 from %s where k3>10000 group by 1,2,3 order by 1,2,3" \
		    % (table_name)
    runner.check(line)
    line = "select extract(year from k10) as wj, extract(month from k10) as dyk, sum(k1)\
		    from %s group by 1, 2 order by 1,2,3" % (table_name)
    runner.check(line)
    """
    line = "select t2.k10, t1.col_dyk from\
		    (select coalesce(t1.k1, t1.k5, t1.k6) as col_wj, \
		            (count(t1.k1)) <= (coalesce(t1.k1, t1.k5, t1.k6)) as bool_wj,\
			    (t1.k3) + (t1.k2) as col_dyk \
	             from %s t1 group by coalesce(t1.k1, t1.k5, t1.k6), (t1.k3) + (t1.k2)\
		     having col_dyk <> (count(col_dyk))) t1\
		     inner join %s t2 on (t2.k1=t1.col_wj)\
		     where t2.k1 in (t1.col_wj, col_dyk) order by 1, 2" % (table_name, join_name)
    runner.check(line)
    """


def test_query_having():
    """
    {
    "title": "test_query_order_group.test_query_having",
    "describe": "test for having and aggregation",
    "tag": "function,p0"
    }
    """
    """
    test for having and aggregation
    """
    line = "select avg(k1) as a from %s group by k2 having a > 10 order by a" % table_name
    runner.check(line)
    line = "select avg(k5) as a from %s group by k1 having a > 100 order by a" % table_name
    runner.check(line)
    line = "select sum(k5) as a from %s group by k1 having a < 100.0 order by a" % table_name
    runner.check(line)
    line = "select sum(k8) as a from %s group by k1 having a > 100 order by a" % table_name
    runner.check(line)
    line = "select avg(k9) as a from %s group by k1 having a < 100.0 order by a" % table_name
    runner.check(line)


def test_query_order_2():
    """
    {
    "title": "test_query_order_group.test_query_order_2",
    "describe": "order test",
    "tag": "function,p0"
    }
    """
    """order test"""
    line = 'select k1, k2 from (select k1, max(k2) as k2 from %s where k1 > 0 group by k1 \
            order by k1)a where k1 > 0 and k1 < 10 order by k1' % table_name
    runner.check(line)
    line = 'select k1, k2 from (select k1, max(k2) as k2 from %s where k1 > 0 group by k1 \
            order by k1)a left join (select k1 as k3, k2 as k4 from %s) b on a.k1 = b.k3 \
            where k1 > 0 and k1 < 10 order by k1, k2' % (table_name, join_name)
    runner.check(line)
    line = 'select k1, count(*) from %s group by 1 order by 1 limit 10' % table_name
    runner.check(line)
    line = 'select a.k1, b.k1, a.k6 from %s a join %s b on a.k1 = b.k1 where a.k2 > 0 \
            and a.k1 + b.k1 > 20 and b.k6 = "false" order by a.k1' % (join_name, table_name)
    runner.check(line)
    line = 'select k1 from baseall order by k1 % 5, k1'
    runner.check(line)
    line = 'select k1 from (select k1, k2 from %s order by k1 limit 10) a where k1 > 5 \
            order by k1 limit 10' % join_name
    runner.check(line)
    line = 'select k1 from (select k1, k2 from %s order by k1) a where k1 > 5 \
            order by k1 limit 10' % join_name
    runner.check(line)
    line = 'select k1 from (select k1, k2 from %s order by k1 limit 10 offset 3) a \
            where k1 > 5 order by k1 limit 5 offset 2' % join_name
    runner.check(line)
    line = 'select a.k1, a.k2, b.k1 from %s a join (select * from %s where k6 = "false" \
            order by k1 limit 3 offset 2) b on a.k1 = b.k1 where a.k2 > 0 order by 1' \
            % (join_name, table_name)
    runner.check(line)


def test_query_nulls_first():
    """
    {
    "title": "test_query_order_group.test_query_nulls_first",
    "describe": "test nulls_first,测试点：对NULL值和非NULL值进行计算，排序、非排序、窗户等函数",
    "tag": "function,p0,fuzz"
    }
    """
    """test nulls_first
    测试点：对NULL值和非NULL值进行计算，排序、非排序、窗户等函数
    """
    table_name = "test"
    # 非NULL结果
    line = 'select k4 + k5 from %s nulls first' % table_name
    runner.checkwrong(line)
    line1 = "select k4 + k5 as sum, k5, k5 + k6 as nu from %s where k6 not like 'na%%' and\
       k6 not like 'INf%%' order by sum nulls first" % table_name
    line2 = "select k4 + k5 as sum, k5,  k5 + k7 as nu from %s where k6 not like 'na%%' and\
        k6 not like 'INf%%' order by sum nulls first" % table_name
    runner.check2_palo(line1, line2)
    line1 = 'select k4 + k5 from %s order by 1 nulls first' % table_name
    line2 = 'select k4 + k5 from %s order by 1' % table_name
    runner.check2(line1, line2)
    # NULL结果
    line1 = "select k5, k5 + k6 from %s where lower(k6) not like 'na%%' and\
        upper(k6) not like 'INF%%' order by k5 nulls first" % table_name
    line2 = "select k5, NULL from %s where lower(k6) not like 'na%%' and\
        upper(k6) not like 'INF%%' order by k5" % table_name
    runner.check2(line1, line2)
    # null 和非null
    line1 = " select a.k1 ak1, b.k1 bk1 from %s a \
       right join %s b on a.k1=b.k1 and b.k1>10 \
       order by ak1 desc nulls first, bk1" % (table_name, join_name)
    line2 = " select a.k1 ak1, b.k1 bk1 from %s a \
           right join %s b on a.k1=b.k1 and b.k1>10 \
           order by isnull(ak1) desc, ak1 desc, bk1" % (table_name, join_name)
    runner.check2(line1, line2)

    # NULL列group by
    line1 = "select k5 + k4 as nu, sum(k1) from %s group by nu order by nu\
        nulls first" % table_name
    line2 = "select k4 + k5 as nu, sum(k1) from %s group by nu order by nu" % table_name
    runner.check2(line1, line2)
    line3 = "select k6 + k5 as nu from test group by nu"
    line4 = "select NULL"
    runner.check2(line3, line4)
    line1 = "select k6 + k5 as nu, sum(1) from test  group by nu order by nu  desc limit 5"
    line2 = "select NULL, count(1) from test"
    runner.check2(line1, line2)
    line3 = "select k6 + k5 as nu, sum(1) from test  group by nu order by nu limit 5"
    runner.check2(line3, line2)
    # 其他列排序，NULL列也排序，以NULL高优
    line1 = "select ak1, sum(bk1) as s from (select a.k1 ak1, b.k1 bk1 from \
           %s a right join %s b on a.k1=b.k1 and b.k1>10)s group by ak1 \
           order by s nulls first" % (table_name, join_name)
    line2 = "select ak1, sum(bk1) as s from (select a.k1 ak1, b.k1 bk1 from \
           %s a right join %s b on a.k1=b.k1 and b.k1>10)s group by ak1 \
           order by s" % (table_name, join_name)
    # runner.check2(line1, line2)
    # 窗口函数对NULL的处理
    line1 = "select k1, k2, nu from (select k1, k2, k5, k5 + k6 as nu,\
         sum(k2) over (partition by k5 + k6)\
        as ss from %s)s  where s.k5 > 2000 order by k1 nulls first" %  join_name
    line2 = "select k1, k2, nu from (select k1, k2, k5, k5 + k6 as nu,\
        sum(k2) over (partition by k5 + k6)\
        as ss from %s  where k5 > 2000 )s order by k1" % join_name
    runner.check2_palo(line1, line2)


def test_query_nulls_last():
    """
    {
    "title": "test_query_order_group.test_query_nulls_last",
    "describe": "test nulls_last,测试点：对NULL值和非NULL值进行计算，排序、非排序、窗户等函数",
    "tag": "function,p0,fuzz"
    }
    """
    """test nulls_last
    测试点：对NULL值和非NULL值进行计算，排序、非排序、窗户等函数
    """
    table_name = "test"
    # 非NULL结果
    line = 'select k4 + k5 from %s nulls last' % table_name
    runner.checkwrong(line)
    line1 = "select k4 + k5 as sum, k5 + k6 as nu from %s  where lower(k6) not like 'na%%' and\
       upper(k6) not like 'INF%%' order by sum nulls last" % table_name
    line2 = "select k4 + k5 as sum, NULL as nu from %s where lower(k6) not like 'na%%' and\
       upper(k6) not like 'INF%%' order by sum" % table_name
    runner.check2(line1, line2)
    line1 = 'select k4 + k5 as nu from %s order by nu nulls last' % table_name
    line2 = 'select k4 + k5 as nu from %s order by nu' % table_name
    runner.check2(line1, line2)
    # null 和非null
    line1 = " select a.k1 ak1, b.k1 bk1 from %s a \
       right join %s b on a.k1=b.k1 and b.k1>10 \
       order by ak1 nulls last, bk1" % (table_name, join_name)
    line2 = " select a.k1 ak1, b.k1 bk1 from %s a \
           right join %s b on a.k1=b.k1 and b.k1>10 \
           order by isnull(ak1), ak1, bk1" % (table_name, join_name)
    runner.check2(line1, line2)

    # NULL列group by
    line1 = "select k5 + k6 as nu, sum(k1) from %s group by nu order by nu,\
        sum(k1) nulls last" % table_name
    line2 = "select k6 + k5 as nu, sum(k1) from %s group by nu order by nu, sum(k1)" % table_name
    runner.check2_palo(line1, line2)
    #issue https://github.com/apache/incubator-doris/issues/2142
    # 其他列排序，NULL列也排序，以NULL高优
    line1 = "select ak1, sum(bk1) as s from (select a.k1 ak1, b.k1 bk1 from \
       %s a right join %s b on a.k1=b.k1 and b.k1>10)s group by ak1 \
       order by s nulls last" % (table_name, join_name)
    line2 = "select ak1, sum(bk1) as s from (select a.k1 ak1, b.k1 bk1 from \
           %s a right join %s b on a.k1=b.k1 and b.k1>10)s group by ak1 \
           order by s" % (table_name, join_name)
    # todo
    # runner.check2(line1, line2)
    # 窗口函数对NULL的处理
    line1 = "select k1, k2, nu from (select k1, k2, k5, k5 + k6 as nu,\
         sum(k2) over (partition by k5 + k6)\
        as ss from %s)s  where s.k5 > 2000 order by k1,k2 nulls last" %  join_name
    line2 = "select k1, k2, nu from (select k1, k2, k5, k5 + k6 as nu,\
        sum(k2) over (partition by k5 + k6)\
        as ss from %s  where k5 > 2000 )s order by k1,k2 " % join_name
    runner.check2_palo(line1, line2)


def teardown_module():
    """
    end 
    """
    print("End")
    # mysql_cursor.close()
    # mysql_con.close()


if __name__ == "__main__":
    print("test")
    setup_module()
    test_query_nulls_first()
    test_query_nulls_last()
