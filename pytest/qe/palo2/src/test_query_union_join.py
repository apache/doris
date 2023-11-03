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
    def __init__(self):
        self.get_clients()

    def check(self, line, timeout=None):
        """
        check if result of mysql and palo is same
        """
        print(line)
        times = 0
        flag = 0
        while (times <= 10 and flag == 0):
            try:
                if timeout is None:
                    LOG.info(L('palo sql', palo_sql=line))
                    palo_result = self.query_palo.do_sql(line)
                else:
                    palo_result = self.query_palo.do_set_properties_sql(line, 
                                                               ['set query_timeout=%s' % timeout])
                LOG.info(L('mysql sql', mysql_sql=line))
                self.mysql_cursor.execute(line)
                mysql_result = self.mysql_cursor.fetchall()
                util.check_same(palo_result, mysql_result)
                flag = 1
            except Exception as e:
                print(Exception, ":", e)
                LOG.error(L('err', error=e))
                time.sleep(1)
                times += 1
                if (times == 3):
                    assert 0 == 1


def test_query_join_1():
    """
    {
    "title": "test_query_union_join.test_query_join_1",
    "describe": "test for function join",
    "tag": "system,p0"
    }
    """
    """
    test for function join
    """
    line1 = "select j.*, d.* from %s j full outer join %s d on (j.k1=d.k1) order by j.k1, j.k2, j.k3, j.k4, d.k1, d.k2\
            limit 100"  % (join_name, table_name)
    line2 = "select * from (select j.k1 j1, j.k2 j2, j.k3 j3, j.k4 j4, j.k5 j5, j.k6 j6, j.k10 j10, j.k11 j11, \
             j.k7 j7, j.k8 j8, j.k9 j9, d.k1 d1, d.k2 d2, d.k3 d3, d.k4 d4, d.k5 d5, d.k6 d6, d.k10 d10, \
             d.k11 d11, d.k7 d7, d.k8 d8, d.k9 d9 from %s j left join %s d on (j.k1=d.k1) \
             union select j.k1 j1, j.k2 j2, j.k3 j3, j.k4 j4, j.k5 j5, j.k6 j6, j.k10 j10, j.k11 j11, \
             j.k7 j7, j.k8 j8, j.k9 j9, d.k1 d1, d.k2 d2, d.k3 d3, d.k4 d4, d.k5 d5, d.k6 d6, d.k10 d10, d.k11 d11, \
             d.k7 d7, d.k8 d8, d.k9 d9 from %s j right join %s d on (j.k1=d.k1) ) a order by j1, j2, j3, j4, d1, d2 \
             limit 100"  % (join_name, table_name, join_name, table_name)
    runner.check2(line1, line2)
    #todo
    line = "select sum(t1.k1), sum(t1.k3), max(t1.k5), max(t2.k4) from %s t1 inner join %s t2 on t1.k1 = t2.k1 and \
		    t1.k6 is not null and t2.k6 is not null" % (table_name, join_name)
    runner.check(line)
    line = "select k1, k2, k3 from %s where k7 is not null order by 1 desc, 2 desc, 3 desc limit 10" \
		    % (table_name)
    runner.check(line)
    line = "select c.k1, c.k8 from %s d join (select a.k1 as k1, a.k8 from %s a join %s b on (a.k1=b.k1)) c\
		    on c.k1 = d.k1 order by 1, 2" % (join_name, table_name, join_name)
    runner.check(line)
    line = "select a.k1, b.k1 from %s a join (select k1, k2 from %s order by k1 limit 10) b \
		    on a.k2=b.k2 order by 1, 2" % (join_name, table_name)
    runner.check(line)
    line = "select a.k1, b.k2 from %s as a join (select k1, k2 from %s) as b\
		    where a.k1 = b.k1 order by a.k1, b.k2" % (join_name, table_name)
    runner.check(line)
    line = "select A.k1,B.k1 from %s as A join %s as B where A.k1=B.k1+1 \
		    order by A.k1, A.k2, A.k3, A.k4" % (join_name, table_name)
    runner.check(line)
    line = "select A.k1, B.k2 from %s as A join %s as B \
    		    order by A.k1, B.k2 limit 10" % (join_name, table_name)
    runner.check(line)
    line = "select A.k1 from %s as A join %s as B order by A.k1 limit 10" % (join_name, join_name)
    runner.check(line)
    line = "select a.k4 from %s a inner join %s b on (a.k1=b.k1) \
		    where a.k2>0 and b.k1=1 and a.k1=1 order by 1" %(table_name, join_name)
    runner.check(line)
    line = "select j.*, d.* from %s j inner join %s d on (j.k1=d.k1) \
		    order by j.k1, j.k2, j.k3, j.k4" % (table_name, join_name)
    runner.check(line)
    line = "select a.k1, b.k2, c.k3 \
		    from %s a join %s b on (a.k1=b.k1) join %s c on (a.k1 = c.k1)\
		    where a.k2>0 and a.k1+50<0" % (table_name, join_name, table_name)
    runner.check(line)
    line = "select t1.k1, t2.k1 from %s t1 join %s t2 where (t1.k1<3 and t2.k1<3)\
		    order by t1.k1, t2.k1 limit 100" % (table_name, join_name)
    runner.check(line)
    line = "select a.k1, b.k1, a.k2, b.k2 from\
            (select k1, k2 from %s where k9>0 and k6='false' union all\
	     select k1, k2 from %s where k6='true' union all\
	     select 0, 0) a inner join\
	     %s b on a.k1=b.k1 and b.k1<5 order by 1, 2, 3, 4"\
	     % (table_name, join_name, table_name)
    runner.check(line)
    line = "select a.k1, b.k1, a.k2, b.k2 from\
             %s b left outer join\
            (select k1, k2 from %s where k9>0 and k6='false' union all\
	     select k1, k2 from %s where k6='true' union all\
	     select 0, 0) a \
	     on a.k1=b.k1 where b.k1<5 and a.k1 is not NULL order by 1, 2, 3, 4"\
	     % (table_name, join_name, table_name)
    runner.check(line)
    line = "select a.k1, b.k1, a.k2, b.k2 from\
            (select k1, k2 from %s where k1=1 and lower(k6) like '%%w%%' union all\
	     select k1, k2 from %s where k1=2  union all\
	     select 0, 1) a  join\
            (select k1, k2 from %s where k1=1 and lower(k6) like '%%w%%' union all\
	     select k1, k2 from %s where k1>0  union all\
	     select 1, 2) b on a.k1 = b.k1 where b.k1<5 order by 1, 2, 3, 4"\
	     % (table_name, join_name, join_name, table_name)
    runner.check(line)
    line = "select count(*) from \
	    (select k1 from %s union distinct\
	     select k1 from %s) a inner join\
	    (select k2 from %s union distinct\
	     select k2 from %s) b on a.k1+1000=b.k2 inner join\
	    (select distinct k1 from %s) c on a.k1=c.k1" \
	     % (table_name, join_name, join_name, table_name, table_name)
    runner.check(line)
    line = "select count(t1.k1) as wj from %s t1 left join\
		    %s t2 on t1.k10=t2.k10 left join\
		    %s t3 on t2.k1 = t3.k1"\
		    % (join_name, join_name, join_name)
    runner.check(line)
    line = "select j.*, d.* from %s j left join %s d on (lower(j.k6) = lower(d.k6)) \
		    order by j.k1, j.k2, j.k3, j.k4, d.k2, d.k3, d.k4" % (table_name, join_name)
    runner.check(line)
    line = "select j.*, d.* from %s j right join %s d on (lower(j.k6) = lower(d.k6)) \
		    order by j.k1, j.k2, j.k3, j.k4, d.k2, d.k3, d.k4" % (table_name, join_name)
    runner.check(line)
    line = "select k1, v.k2 from %s c, (select k2 from %s order by k2 limit 2) v \
		    where k1 in (1, 2, 3) order by 1, 2" % (join_name, table_name)
    runner.check(line)
    line = "select k1, v.k2 from %s c, (select k2 from %s order by k2 limit 2) v \
    	    where k1 in (1, 2, 3) and v.k2%%2=0 order by 1, 2" % (join_name, table_name)
    runner.check(line)


def test_query_join_2():
    """
    {
    "title": "test_query_union_join.test_query_join_2",
    "describe": "test for function join",
    "tag": "system,p0"
    }
    """
    """
    test for function join
    """
    line = "select k1, k2, cnt, avp from %s c, (select count(k1) cnt, avg(k2) avp from %s) v where k1 <3 \
		    order by 1, 2, 3, 4" % (table_name, join_name)
    runner.check(line)
    line = "select k1, avg(maxp) from %s c, (select max(k8) maxp from %s group by k1) v where k1<3 group by k1\
		    order by 1, 2" % (table_name, table_name)
    runner.check(line)
    line = "select k1, v.k3, cnt, avp from %s c, (select count(k1) cnt, avg(k9) avp, k3 from %s group by k3) v\
		    where k1<0 order by 1, 2, 3, 4" % (join_name, join_name)
    runner.check(line)
    line = "select count(k5), k2 from %s c, (select ca.k1 okey, cb.k2 opr from %s ca, %s cb where \
		    ca.k1=cb.k1 and ca.k2+cb.k2>2) v group by k2 order by 1, 2" \
		    % (table_name, join_name, table_name)
    runner.check(line)
    line = "select count(k6), k1 from %s c, \
		    (select ca.k1 wj, ca.k2 opr from %s ca left outer join %s cb on ca.k1 = cb.k1) v\
		    group by k1 order by 1, 2" % (table_name, join_name, table_name)
    runner.check(line)
    line = "select count(k6), k1 from %s c, \
		    (select ca.k1 wj, ca.k2 opr from %s ca right outer join %s cb \
		    on ca.k1 = cb.k1 and ca.k2+cb.k2>2) v\
		    group by k1 order by 1, 2" % (table_name, join_name, table_name)
    # Ocurrs time out with specfied time 299969 MILLISECONDS
    runner.check(line, 600)


def test_query_complex():
    """
    {
    "title": "test_query_union_join.test_query_complex",
    "describe": "test for complex query",
    "tag": "system,p0"
    }
    """
    """
    test for complex query
    """
    line = "(select A.k2 as wj1,count(*) as wj2, case A.k2 when 1989 then \"wj\" \
		    when 1992 then \"dyk\" when 1985 then \"wcx\" else \"mlx\" end \
		    from %s as A join %s as B where A.k1=B.k1+1 \
		    group by A.k2 having sum(A.k3)> 1989) union all \
		    (select C.k5, C.k8, C.k6 from %s as C where lower(C.k6) like \"tr%%\")\
		    order by wj1,wj2"%(table_name, table_name, table_name)
    runner.check(line)
    line = "(select A.k2 as wj1,count(*) as wj2, case A.k2 when 1989 then \"wj\" \
		    when 1992 then \"dyk\" when 1985 then \"wcx\" else \"mlx\" end,\
		    if (A.k2<>255,\"hello\",\"world\") \
		    from %s as A join %s as B where A.k1=B.k1+1 \
		    group by A.k2 having sum(A.k3)> 1989) union all \
		    (select C.k5, C.k8, C.k6, if (C.k8<0,\"hello\",\"world\") \
		    from %s as C where lower(C.k6) like \"tr%%\")\
		    order by wj1,wj2"%(table_name, table_name, table_name)
    runner.check(line)
    line = " select A.k2,count(*) from %s as A join %s as B \
		    where A.k1=B.k1+1 group by A.k2 having sum(A.k3)> 1989 order by A.k2 desc"\
		    % (table_name, table_name)
    runner.check(line)
    line = "(select A.k2 as wj1,count(*) as wj2 from %s as A join %s as B \
		    where A.k1=B.k1+1 group by A.k2 having sum(A.k3)> 1989)\
		    union all (select C.k5, C.k8 from %s as C where C.k6 like \"tr%%\")\
		    order by wj1,wj2"%(table_name, table_name, join_name)
    runner.check(line)


def test_query_union_1():
    """
    {
    "title": "test_query_union_join.test_query_union_1",
    "describe": "test for function union",
    "tag": "system,p0"
    }
    """
    """
    test for function union
    """
    line = "(select * from %s) union (select * from %s) order by k1, k2, k3, k4 limit 4" %\
		    (table_name, table_name)
    runner.check(line)
    line = "(select * from %s) union all (select * from %s) \
		    order by k1, k2, k3, k4 limit 4" % (table_name, table_name)
    runner.check(line)
    line = "(select * from %s where k1<10) union all \
		    (select * from %s where k5<0) order by k1,k2,k3 limit 40" % \
		    (table_name, table_name)
    runner.check(line)
    line = "(select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k1>0)\
      union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k2>0)\
      union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k3>0)\
		      order by k1, k2, k3, k4" % (table_name, join_name, join_name)
    runner.check(line)
    line = "(select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s \
		    where k1>0 order by k1, k2, k3, k4 limit 1)\
      union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s \
                    where k2>0 order by k1, k2, k3, k4 limit 1)\
      union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s \
		    where k3>0  order by k1, k2, k3, k4 limit 1)\
      order by k1, k2, k3, k4" % (table_name, join_name, join_name)
    runner.check(line)
    line = "(select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k1>0)\
      union distinct (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k2>0)\
      union distinct (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k3>0)\
		      order by k1, k2, k3, k4" % (table_name, join_name, join_name)
    runner.check(line)
    line = "(select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k1>0)\
      union distinct (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k2>0)\
      union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k3>0)\
		      order by k1, k2, k3, k4" % (table_name, join_name, join_name)
    runner.check(line)
    line = "(select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s \
		    where k1>0 order by k1, k2, k3, k4 limit 1)\
      union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s \
                    where k2>0 order by k1, k2, k3, k4 limit 1)\
      union distinct (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s \
		    where k3>0  order by k1, k2, k3, k4 limit 1)\
      order by k1, k2, k3, k4" % (table_name, join_name, join_name)
    runner.check(line)
    line = "(select count(k1), sum(k2) from %s)\
            union all (select k1, k2 from %s order by k1, k2 limit 10)\
	    union all (select sum(k1), max(k3) from %s group by k2)\
	    union all (select k1, k2 from %s)\
	    union all (select a.k1, b.k2 from %s a join %s b on (a.k1=b.k1)\
	    union all (select 1000, 2000) order by k1, k2"\
	    % (table_name, table_name, table_name, join_name, table_name, join_name)
    line = "select * from (select 1 a, 2 b \
		    union all select 3, 4 \
		    union all select 10, 20) t where a<b order by a, b"
    runner.check(line)
    line = "select count(*) from (select 1 from %s as t1 join %s as t2 on t1.k1 = t2.k1\
		        union all select 1 from %s as t1) as t3" % (table_name, join_name, table_name)
    runner.check(line)
    line = "(select k1, count(*) from %s where k1=1 group by k1)\
		    union distinct (select 2,3) order by 1,2" % (table_name)
    runner.check(line)
    line = "(select 1, 'a', NULL, 10.0)\
            union all (select 2, 'b', NULL, 20.0)\
	    union all (select 1, 'a', NULL, 10.0) order by 1, 2"
    runner.check(line)
    line = "select count(*) from (\
             (select k1, k2, k3, k4, k5, k6, k7, k8, k9, 10, k11 from %s where k1>0)\
   union distinct (select k1, k2, k3, k4, k5, k6, k7, k8, k9, 10, k11 from %s where k2>0)\
   union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, 10, k11 from %s where k3>0)) x" \
           % (table_name, table_name, join_name)
    runner.check(line)
    line = "(select 10, 10.0, 'hello', 'world') union all\
            (select k1, k5, k6, k7 from %s where k1=1) union all\
	    (select 20, 20.0, 'wangjuoo4', 'beautiful') union all\
	    (select k2, k8, k6, k7 from %s where k2>0) order by 1, 2, 3, 4"\
			    %(join_name, join_name)
    runner.check(line)
    line = "select x.k1, k2, k3, k4, k5 from \
             ((select k1, k2, k3, k4, k5 from %s where k1>0) union distinct\
	     (select k1, k2, k3, k4, k5 from %s where k2>0)) x \
	     where x.k1<5 and x.k3>0 order by 1, 2, 3, 4"\
	     %(table_name, join_name)
    runner.check(line)
    line = "select x.k1, k2, k3, k4, k5 from \
             ((select k1, k2, k3, k4, k5 from %s where k1>0) union all\
	     (select k1, k2, k3, k4, k5 from %s where k2>0)) x \
	     where x.k1<5 and x.k3>0 order by 1, 2, 3, 4"\
	     %(table_name, join_name)
    runner.check(line)


def test_query_union_2():
    """
    {
    "title": "test_query_union_join.test_query_union_2",
    "describe": "test for function union",
    "tag": "system,p0"
    }
    """
    """
    test for function union
    """
    line = "select x.k1, k6, k7, k8, k9, k10 from \
          ((select k1, k6, k7, k8, k9, k10 from %s where k1=1) union distinct\
	   (select k1, k6, k7, k8, k9, k10 from %s where k9>0)) x union distinct\
	  (select k1, k6, k7, k8, k9, k10 from %s) order by 1, 4, 5, 6 limit 10"\
	  % (table_name, table_name, join_name)
    runner.check(line)
    line = "select x.k1, k6, k7, k8, k9, k10 from \
          ((select k1, k6, k7, k8, k9, k10 from %s where k1=1) union all\
	   (select k1, k6, k7, k8, k9, k10 from %s where k9>0)) x union all\
	  (select k1, k6, k7, k8, k9, k10 from %s) order by 1, 4, 5, 6 limit 10"\
	  % (table_name, table_name, join_name)
    runner.check(line)
    line = "(select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k1>0)\
           union all (select 1, 2, 3, 4, 3.14, 'hello', 'world', 0.0, 1.1, cast('1989-03-21' as date), \
           cast('1989-03-21 13:00:00' as datetime))\
           union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k3>0)\
	       order by k1, k2, k3, k4" % (table_name, join_name)
    runner.check(line)
    line = "(select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k1>0)\
            union distinct (select 1, 2, 3, 4, 3.14, 'hello', 'world', 0.0, 1.1, cast('1989-03-21' as date), \
            cast('1989-03-21 13:00:00' as datetime))\
            union distinct (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s where k3>0)\
	        order by k1, k2, k3, k4" % (table_name, join_name)
    runner.check(line)


def test_union_basic():
    """
    {
    "title": "test_query_union_join.test_union_basic",
    "describe": "test union basic",
    "tag": "system,p0"
    }
    """
    """test union basic"""
    line = 'select 1, 2  union select 1.01, 2.0 union (select 0.0001, 0.0000001) order by 1, 2'
    runner.check(line)
    line = 'select 1, 2 union (select "hell0", "") order by 1, 2'
    runner.check(line)
    line = 'select 1, 2  union select 1.0, 2.0 union (select 1.00000000, 2.00000) order by 1, 2'
    runner.check(line)
    line = 'select 1, 2  union all select 1.0, 2.0 union (select 1.00000000, 2.00000) order by 1, 2'
    runner.check(line)
    line = 'select 1, 2  union all select 1.0, 2.0 union all (select 1.00000000, 2.00000) order by 1, 2'
    runner.check(line)
    line = 'select 1, 2  union select 1.0, 2.0 union all (select 1.00000000, 2.00000) order by 1, 2'
    runner.check(line)
    line = 'select 1, 2  union distinct select 1.0, 2.0 union distinct (select 1.00000000, 2.00000) order by 1, 2'
    runner.check(line)
    line = 'select cast("2016-07-01" as date) union (select "2016-07-02") order by 1'
    runner.check(line)
    line = 'select "2016-07-01" union (select "2016-07-02") order by 1'
    runner.check(line)
    line = 'select cast("2016-07-01" as date) union (select cast("2016-07-02 1:10:0" as date)) order by 1'
    runner.check(line)
    line1 = 'select cast(1 as decimal), cast(2 as double) union distinct select 1.0, 2.0 \
             union distinct (select 1.00000000, 2.00000) order by 1, 2'
    line2 = 'select cast(1 as decimal), cast(2 as decimal) union distinct select 1.0, 2.0 \
             union distinct (select 1.00000000, 2.00000) order by 1, 2'
    runner.check2(line1, line2)


def test_union_multi():
    """
    {
    "title": "test_query_union_join.test_union_multi",
    "describe": "test union multi",
    "tag": "system,p0"
    }
    """
    """test union multi"""
    sub_sql = ['(select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from baseall where k1 % 3 = 0)'] * 10
    sql = ' union '.join(sub_sql)
    runner.check(sql + 'order by 1, 2, 3, 4')
    runner.check(sql + 'order by 1, 2, 3, 4')
    runner.check(sql + 'order by 1, 2, 3, 4')
    runner.check(sql + 'order by 1, 2, 3, 4')
    runner.check(sql + 'order by 1, 2, 3, 4')
    sql = ' union all '.join(sub_sql)
    runner.check(sql + 'order by 1, 2, 3, 4')
    runner.check(sql + 'order by 1, 2, 3, 4')
    runner.check(sql + 'order by 1, 2, 3, 4')
    runner.check(sql + 'order by 1, 2, 3, 4')
    runner.check(sql + 'order by 1, 2, 3, 4')
    sql = ' union distinct '.join(sub_sql)
    runner.check(sql + 'order by 1, 2, 3, 4')
    runner.check(sql + 'order by 1, 2, 3, 4')
    runner.check(sql + 'order by 1, 2, 3, 4')
    runner.check(sql + 'order by 1, 2, 3, 4')
    runner.check(sql + 'order by 1, 2, 3, 4')


def test_union_bug():
    """
    {
    "title": "test_query_union_join.test_union_bug",
    "describe": "bug补充",
    "tag": "system,p0"
    }
    """
    line = 'select * from (select 1 as a, 2 as b union select 3, 3) c where a = 1'
    runner.check(line)
    line = 'drop view nullable'
    line = 'CREATE VIEW `nullable` AS SELECT `a`.`k1` AS `n1`, `b`.`k2` AS `n2` ' \
           'FROM `default_cluster:%s`.`baseall` a LEFT OUTER JOIN ' \
           '`default_cluster:%s`.`bigtable` b ON `a`.`k1` = `b`.`k1` + 10 ' \
           'WHERE `b`.`k2` IS NULL' % (db, db)
    line = 'select n1 from nullable union all select n2 from nullable'
    line = '(select n1 from nullable) union all (select n2 from nullable order by n1) order by n1'
    line = '(select n1 from nullable) union all (select n2 from nullable) order by n1'


def test_union_different_column():
    """
    {
    "title": "test_query_union_join.test_union_different_column",
    "describe": "2个select 的列个数 或 字段类型不相同,列个数会报错；大类型（数值或字符或日期）不同的会报错，大类型相同的成功",
    "tag": "system,p0,fuzz"
    }
    """
    """2个select 的列个数 或 字段类型不相同
    列个数会报错；大类型（数值或字符或日期）不同的会报错，大类型相同的成功
    """
    line = "select k1, k2 from %s union select k2 from %s order by k1, k2" \
       % (join_name, table_name)
    runner.checkwrong(line)
    line = "select k1, k1 from %s union select k2 from %s limit 3" %\
       (join_name, table_name)
    runner.checkwrong(line)
    line = "(select k1, k1 from %s) union (select k2, 1 from %s) order by k1" \
       % (join_name, table_name)
    runner.checkwrong(line)
    line = "(select k1+k1 from %s) union (select k2 from %s) order by k1+k1" %\
       (join_name, table_name)
    runner.checkwrong(line)

    ##不同类型的列
    for index in range(2, 11):
        line1 = "(select k1 from %s) union all (select k%s from %s order by k%s)\
           order by k1 limit 30" % (join_name, index, table_name, index)
        line2 = "select k1 from %s union all (select k%s from %s order by k%s) \
           order by k1 limit 30" % (join_name, index, table_name, index)
        if index in [6, 7, 10]:
            ##todo,int uninon date报错；issue：https://github.com/apache/incubator-doris/issues/2180
            continue
        runner.check2(line1, line2)
    line = "(select k1, k2 from %s) union (select k2, k10 from %s order by k10)\
       order by k1, k2" % (join_name, table_name)
    runner.checkwrong(line)
    ##cast类型
    line1 = "(select k1, k2 from %s) union (select k2, cast(k11 as int) from %s) \
       order by k1, k2" % (join_name, table_name)
    line2 = "(select k1, k2 from %s) union (select k2, cast(k11 as int) from %s order by k2)\
       order by k1, k2" % (join_name, table_name)
    runner.check2_palo(line1, line2)
    line1 = "(select k1, k2 from %s) union (select k2, cast(k10 as int) from %s) order by k1, k2"\
        % (join_name, table_name)
    line2 = "(select k1, k2 from %s) union (select k2, cast(k10 as int) from %s order by k2) order\
        by k1, k2" % (join_name, table_name)
    runner.check2_palo(line1, line2)
    ##不同类型不同个数
    line = "select k1, k2 from %s union selectk11, k10, k9  from %s order by k1, k2"\
       % (join_name, table_name)
    runner.checkwrong(line)


def test_union_different_schema():
    """
    {
    "title": "test_query_union_join.test_union_different_schema",
    "describe": "两个表的schema信息相同或不同,schame相同时都支持；schema不同时select出的是相同大类型是ok的",
    "tag": "system,p0,fuzz"
    }
    """
    """两个表的schema信息相同或不同
    schame相同时都支持；schema不同时select出的是相同大类型是ok的
    """
    new_union_table = "union_different_schema_table"
    sql = 'create table %s(k1 tinyint, k2 decimal(9,3) NULL, k3 char(5) NULL,\
        k4 date NULL, k5 datetime NULL, \
        k6 double sum) engine=olap \
        distributed by hash(k1) buckets 2 properties("storage_type"="column")' % new_union_table
    msql = 'create table %s(k1 tinyint, k2 decimal(9,3), k3 char(5), k4 date,\
         k5 datetime, k6 double)' % new_union_table
    runner.init(sql, msql)
    ##不同schema 不同列报错
    line = "select * from %s union select * from %s order by k1, k2" % (new_union_table, table_name)
    runner.checkwrong(line)
    for index in range(1, 5):
        line = "(select k1 from %s) union (select k%s from %s) order by k1" %\
           (new_union_table, index, table_name)
        runner.check(line)
    line = 'drop table %s' % new_union_table
    runner.init(line)


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
    test_union_different_column()    
    test_union_different_schema()
