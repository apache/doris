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
test datatype
"""

import sys
sys.path.append("../lib/")
from palo_qe_client import QueryBase

sys.path.append("../../../lib/")
import palo_logger
LOG = palo_logger.Logger.getLogger()
L = palo_logger.StructedLogMessage

table_name = "test"
join_name = "baseall"


def setup_module():
    """
    init config
    """
    global runner
    runner = QueryBase()


def test_query_base():
    """
    {
    "title": "test_query_datatype.test_query_base",
    "describe": "base query, 1.just select *** from ***, 2.contains types tinyint, smallint, int, bigint, decimal, float,double, date, datetime",
    "tag": "function,p0"
    }
    """
    """
    base query
            1.just select *** from ***
            2.contains types tinyint, smallint, int, bigint, decimal, float, double, date, datetime
    """
    line = "select * from %s order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select k1 from %s order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select k2 from %s order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select k3 from %s order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select k4 from %s order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select k5 from %s order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select k6 from %s order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select k7 from %s order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select k8 from %s order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select k9 from %s order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select k10 from %s order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select k11 from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select %s.* from %s order by k1, k2, k3, k4" % (table_name, table_name)
    runner.check(line)


def test_query_bigint():
    """
    {
    "title": "test_query_datatype.test_query_bigint",
    "describe": "test for type bigint",
    "tag": "function,p0"
    }
    """
    """
    test for type bigint
    """
    line = "select * from %s where k4=9223372036854775807 order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select * from %s where k4=-9223372036854775808 order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select * from %s where k4=9223372036854775808 order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select * from %s where k4=-9223372036854775808 order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select * from %s where k4>-9223372036854775808 order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select * from %s where k4<9223372036854775808 order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)


def test_query_int():
    """
    {
    "title": "test_query_datatype.test_query_int",
    "describe": "test for type int",
    "tag": "function,p0"
    }
    """
    """
    test for type int
    """
    line = "select * from %s where k3=2147483647 order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select * from %s where k3=-2147483648 order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select * from %s where k3=2147483648 order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select * from %s where k3=-2147483649 order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select * from %s where k3>-2147483649 order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select * from %s where k3<2147483648 order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)


def test_query_smallint():
    """
    {
    "title": "test_query_datatype.test_query_smallint",
    "describe": "test for type smallint",
    "tag": "function,p0"
    }
    """
    """
    test for type smallint
    """
    line = "select * from %s where k2=32767 order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select * from %s where k2=-32768 order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select * from %s where k2=32768 order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select * from %s where k2=-32769 order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select * from %s where k2>-32769 order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select * from %s where k2<32768 order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)


def test_query_tinyint():
    """
    {
    "title": "test_query_datatype.test_query_tinyint",
    "describe": "test for type tinyint",
    "tag": "function,p0"
    }
    """
    """
    test for type tinyint
    """
    line = "select * from %s where k1='1' order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select * from %s where k1='1.0' order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select * from %s where k1=127 order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select * from %s where k1=-128 order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select * from %s where k1=128 order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select * from %s where k1=-129 order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select * from %s where k1>-129 order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select * from %s where k1<128 order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    

def test_query_string_simple():
    """
    {
    "title": "test_query_datatype.test_query_string_simple",
    "describe": "test for type string simple query",
    "tag": "function,p0"
    }
    """
    """
    test for type string simple query
    """
    line = "select * from %s where k6=\"wangjuoo4\" order by k1, k2, k3, k4" % (table_name) 
    runner.check(line)
    line = "select * from %s where lower(k6)<\"wangjuoo4\" order by k1, k2, k3, k4" % (join_name) 
    runner.check(line)
    line = "select * from %s where lower(k6)>\"wangjuoo4\" order by k1, k2, k3, k4" % (join_name) 
    runner.check(line)
    line = "select * from %s where k7=\"wangjuoo4\" order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where k6 = cast('true' as char(5)) order by k1, k2, k3, k4" \
           % (table_name)
    runner.check(line)
   

def test_query_decimal():
    """
    {
    "title": "test_query_datatype.test_query_decimal",
    "describe": "test for type decimal, double, float",
    "tag": "function,p0"
    }
    """
    """
    test for type decimal, double, float
    """
    line = "select * from %s where k5=123.123 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where k5>=0 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where k5=-0.123 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where k5>=0 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where k1<10.001 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where k2<1989.001 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where k3<1989.001 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where k4<1989000.001 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where k5<1989000.001 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where k8<1989000.001 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where k9<1989000.001 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where k5=123.123000001 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where k8>0.001 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where k9<1989000.001 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where k8>=20.268 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where k8=0.00 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where k9>=-0.001 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select count(*), count(k1), count(k2), count(k3), count(k4), count(k5), count(k6), count(k7), " \
           "count(k8), count(k9), count(k10), count(k11) from %s" % (table_name)
    runner.check(line)
    line1 = "select * from %s where k5=cast(243.325 as decimal) order by k1, k2, k3, k4" \
            % (table_name)
    line2 = "select * from %s where k5=cast(243.325 as decimal(9,0)) order by k1, k2, k3, k4" \
            % (table_name)
    runner.check2(line1, line2)
    line1 = "select count(*) from %s where k5=cast(243.325 as decimal)" % (table_name)
    line2 = "select count(*) from %s where k5=cast(243.325 as decimal(9,0))" % (table_name)
    runner.check2(line1, line2)
    line = "select count(*), sum(k1), sum(k2), sum(k5), sum(k8), sum(k9) from %s where k5>0" % (table_name)
    runner.check(line)
    line1 = "select k5, max(k1+k2) from %s group by k5 having max(k1+k2)>cast(12 as decimal)" \
            "order by k5, max(k1+k2)" % (table_name)
    line2 = "select k5, max(k1+k2) from %s group by k5 having max(k1+k2)>cast(12 as decimal(9,3)) " \
            "order by k5, max(k1+k2)" % (table_name)
    runner.check2(line1, line2)
    line1 = "select k1+k2 from %s order by 1 nulls first limit 5" % (table_name)
    line2 = "select k1+k2 from %s order by 1 limit 5" % (table_name) 
    runner.check2(line1, line2)
    line1 = "select k1+k2 from %s order by 1 desc nulls last limit 5" % (table_name)
    line2 = "select k1+k2 from %s order by 1 desc limit 5" % (table_name)
    runner.check2(line1, line2)
    line1 = "select t1.*, t2.* from %s t1 join %s t2 on t1.k1 = t2.k1 order by \
		    t1.k1,t1.k2,t1.k3,t1.k4 desc nulls last limit 3" \
            % (table_name, join_name)
    line2 = "select t1.*, t2.* from %s t1 join %s t2 on t1.k1 = t2.k1 order by \
            t1.k1,t1.k2,t1.k3,t1.k4 desc limit 3" \
            % (table_name, join_name)
    runner.check2(line1, line2)
    line = "select avg(k1), avg(k2), avg(k5), avg(k8), avg(k9) from %s" % (table_name)
    runner.check(line)
    # todo: palo result is different from mysql result, verify decimal function, then update the case
    line1 = "select cast(avg(k1) as decimal) as c from %s group by k5 having c>0 order by 1" \
            % (table_name)
    line2 = "select cast(avg(k1) as decimal(9,0)) as c from %s group by k5 having c>0 order by 1" \
            % (table_name)
    runner.check2(line1, line2)
    
    
def test_query_constant():
    """
    {
    "title": "test_query_datatype.test_query_constant",
    "describe": "query for functions using constant",
    "tag": "function,p0"
    }
    """
    """
    query for functions using constant
    """
    line = "SELECT 1 = 0"
    runner.check(line)
    line = "SELECT '0' = 0"
    runner.check(line)
    line = "SELECT '0.0' = 0"
    runner.check(line)
    line = "SELECT '.01' = 0.01"
    runner.check(line)
    line = "SELECT RPAD('hi',5,'?')"
    runner.check(line)
    line = "SELECT RPAD('hi',1,'?')"
    runner.check(line)
    line = "SELECT RTRIM('barbar   ')"
    runner.check(line)
    line = "SELECT REVERSE('abc')"
    runner.check(line)
    line = "SELECT LOCATE('bar', 'foobarbar')"
    runner.check(line)
    line = "SELECT LOCATE('xbar', 'foobar')"
    runner.check(line)
    line = "SELECT LOCATE('bar', 'foobarbar',5)"
    runner.check(line)
    line = "SELECT LOWER('QUADRATICALLY')"
    runner.check(line)
    line = "SELECT LPAD('hi',4,'??')"
    runner.check(line)
    line = "SELECT LPAD('hi',1,'??')"
    runner.check(line)
    line = "SELECT LTRIM('  barbar')"
    runner.check(line)
    line = "SELECT CONV('a',16,2)"
    runner.check(line)
    line = "SELECT CONV('6E',18,8)"
    runner.check(line)
    line = "SELECT CONV(-17,10,-18)"
    runner.check(line)
    line = "SELECT FIND_IN_SET('b','a,b,c,d')"
    runner.check(line)
    line = "SELECT INSTR('foobarbar', 'bar')"
    runner.check(line)
    line = "SELECT INSTR('xbar', 'foobar')"
    runner.check(line)
    line = "SELECT CONCAT(14.3)"
    runner.check(line)
    line = "SELECT CONCAT('My', 'S', 'QL')"
    runner.check(line)
    line = "SELECT CONCAT_WS(',','First name','Second name','Last Name')"
    runner.check(line)
    line = "SELECT SUBSTRING('Quadratically',5)"
    runner.check(line)
    line = "SELECT SUBSTRING('Quadratically',5,6)"
    runner.check(line)
    line = "SELECT SUBSTRING('Sakila', -3)"
    runner.check(line)
    line = "SELECT SUBSTRING('Sakila', -5, 3)"
    runner.check(line)
    line = "SELECT TRIM('  bar   ')"
    runner.check(line)
    line = "SELECT 1+'1'"
    runner.check(line)
    line = "SELECT CONCAT('hello you ',2)"
    runner.check(line)
    line = "SELECT 29 | 15, 29 & 15, 11 ^ 3, 5 & ~1"
    runner.check(line)
    line = "SELECT PASSWORD('badpwd')"
    runner.check(line)


def test_query_date_constant():
    """
    {
    "title": "test_query_datatype.test_query_date_constant",
    "describe": "query for date functions using constant",
    "tag": "function,p0"
    }
    """
    """
    query for date functions using constant
    """
    line = "SELECT DATE('2003-12-31 01:02:03')"
    runner.check(line)
    line = "SELECT DATE_FORMAT('1997-10-04 22:23:00', '%W %M %Y')"
    runner.check(line)
    line = "SELECT DATE_FORMAT('1997-10-04 22:23:00', '%H:%i:%s')"
    runner.check(line)
    line = "SELECT DATE_FORMAT('1997-10-04 22:23:00', '%D %y %a %d %m %b %j')"
    runner.check(line)
    line = "SELECT DATE_FORMAT('1997-10-04 22:23:00', '%D %y %a %d %m %b %j')"
    runner.check(line)
    line = "SELECT DATE_FORMAT('1997-10-04 22:23:00', '%H %k %I %r %T %S %w')"
    runner.check(line)
    line = "SELECT DATE_FORMAT('1999-01-01', '%X %V')"
    runner.check(line)
    line = "select dayofmonth('1998-02-03'), dayofyear('1998-02-03')"
    runner.check(line)
    line = "select now() + 0"
    runner.checkok(line)
    line = "SELECT DAYOFYEAR('1998-02-03')"
    runner.check(line)
    line = "SELECT FROM_DAYS(729669)"
    runner.check(line)
    line = "SELECT HOUR('1998-02-03 10:05:03')"
    runner.check(line)
    line = "SELECT HOUR('98-02-03 272:59:59')"
    runner.check(line)
    line = "SELECT MINUTE('98-02-03 10:05:03')"
    runner.check(line)
    line = "SELECT MONTH('1998-02-03')"
    runner.check(line)
    line = "SELECT MONTHNAME('1998-02-05')"
    runner.check(line)
    line = "SELECT SECOND('98-02-03 10:05:03')"
    runner.check(line)
    line = "SELECT STR_TO_DATE('00/00/0000', '%m/%d/%Y')"
    runner.check(line)
    line = "SELECT STR_TO_DATE('04/31/2004', '%m/%d/%Y')"
    runner.check(line)
    line = "SELECT TO_DAYS(950501)"
    runner.check(line)
    line = "SELECT TO_DAYS('1997-10-07'), TO_DAYS('97-10-07')"
    runner.check(line)
    line = "SELECT YEAR('2000-01-01')"
    runner.check(line)
    line = "SELECT WEEKOFYEAR('1998-02-20')"
    runner.check(line)
    line = "SELECT DAYNAME('1998-02-05')"
    runner.check(line)


def test_query_nullfunction():
    """
    {
    "title": "test_query_datatype.test_query_nullfunction",
    "describe": "query for nullif, ifnull, isnull",
    "tag": "function,p0"
    }
    """
    """
    query for nullif, ifnull, isnull
    """
    line = "select k1, k2, nullif(k2/k1, 0) from %s order by k1, k2" % (table_name)
    runner.check(line)
    line = "select k1, k2, nullif(k2/k1, k2+k3) from %s order by k1, k2" % (table_name)
    runner.check(line)
    line = "select k1, k2, nullif(k2/k1, k8) from %s order by k1, k2" % (table_name)
    runner.check(line)
    line = "select k1, k2, ifnull(k2/k1, 0) from %s order by k1, k2" % (table_name)
    runner.check(line)
    line = "select k1, k2, ifnull(k2/k1, k2+k3) from %s order by k1, k2" % (table_name)
    runner.check(line)
    line = "select k1, k2, isnull(k2/k1) from %s order by k1, k2" % (table_name)
    runner.check(line)


def test_query_if():
    """
    query for if
    """
    line = "select if (k6='true', k1, k2) as wj from %s order by wj" % (table_name)
    runner.check(line)
    line = "select if (k6='true' or k6='false', if (k1>5.1, k3, k4*0.1), k2) as wj " \
           "from %s order by wj" % (table_name)
    runner.check(line)


def test_query_impalad_empty():
    """
    {
    "title": "test_query_datatype.test_query_impalad_empty",
    "describe": "impalad empty.test",
    "tag": "function,p0"
    }
    """
    """
    impalad empty.test
    """
    line = "select 794.67 from %s t1 where 5=6 union all \
            select coalesce(10.4, k3) from %s where false" % (table_name, join_name)
    runner.check(line)
    line = "select * from (select 10 as i, 2 as j, '2013' as s) as t where t.i<10"
    runner.check(line)
    line = "select sum(T.k1), count(T.k3) from (select k1, k3, k4 from %s) T where false" % (table_name)
    runner.check(line)
    line = "select t1.k1, t2.k1 from %s t1 left outer join %s t2 on t1.k1 = t2.k1 limit 0" % (table_name, join_name)
    runner.check(line)
    line = "select count(k3), avg(k5), count(*) from %s limit 0" % (table_name)
    runner.check(line)
    line = "select e.k1, f.k1 from %s f inner join ( " \
           "select t1.k1 from %s t1 left outer join %s t2 on t1.k1=t2.k1 limit 0) e " \
           "on e.k1 = f.k1" % (table_name, table_name, join_name)
    runner.check(line)
    line = "select e.k1, f.k1 from %s f inner join ( " \
           "select t1.k1 from %s t1 left outer join %s t2 on t1.k1=t2.k1 where 1+3>10) e " \
           "on e.k1 = f.k1" % (table_name, table_name, join_name)
    runner.check(line)
    line = "select k3 from %s where k1>200" % (table_name)
    runner.check(line)
    line = "select t1.k1, t2.k1 from %s t1 left outer join %s t2 on t1.k1=t2.k1 " \
           "where false order by 1, 2" % (table_name, join_name)
    runner.check(line)
    line = "select count(k3), avg(k5), count(*) from %s where null" % (table_name)
    runner.check(line)
    
    
def test_query_null():
    """
    {
    "title": "test_query_datatype.test_query_null",
    "describe": "the null case",
    "tag": "function,p0"
    }
    """
    """
    the null case
    """
    line = "select NULL or NULL,NULL or true,NULL or false"
    runner.check(line)
    line = "select NULL and NULL,NULL and true,NULL and false"
    runner.check(line)
    line = "select 1 | NULL,1 & NULL,1 + NULL,1 - NULL "
    runner.check(line)
    line = "select NULL=NULL,NULL<>NULL"
    runner.check(line)
    line = "select NULL > 5, NULL < 5, NULL = 5, NULL >= 5,NULL <= 5"
    runner.check(line)
    line = "select NULL + 2,NULL - 2,NULL * 2,NULL / 2"
    runner.check(line)
    line = "select 2 between NULL and 1,2 between 3 AND NULL,NULL between 1 and 2,\
            2 between NULL and 3, 2 between 1 AND NULL"
    runner.check(line)
    line = "select abs(NULL),cos(NULL),sin(NULL)"
    runner.check(line)
    line = "select repeat(\"a\",0),repeat(\"ab\",5+5),repeat(\"ab\",-1),reverse(NULL)"
    runner.check(line)
    line = "select count(null),max(null),min(null),sum(null),avg(null) from baseall"
    runner.check(line)
    line = "select concat(\"a\",NULL)"
    runner.check(line)
    line = "select 1 from baseall where (select max(k1) from %s where k1 = 16) is NULL" \
            % (join_name)
    runner.check(line)
    line = "select 1 from baseall where (select max(k1) from %s where k1 = 16) is not NULL" \
            % (join_name)
    runner.check(line)
    line = "select 1 from baseall where (select max(k1) from %s where k1 = 15) is NULL" \
            % (join_name)
    runner.check(line)
    line = "select 1 from baseall where (select max(k10) from %s where k1 = 15) is not NULL" \
            % (join_name)
    runner.check(line)
    line = "select 1 from baseall where exists(select k1 from %s where k1 = 16)" % (join_name)
    runner.check(line)
    line = "select 1 from baseall b where not exists(select k1 from %s a where \
            a.k1 = 16 and b.k1 = a.k1)" % (join_name)
    runner.check(line)
    line = "select 1 from baseall where 1 >= (select k1 from %s where k1 = 16 limit 1)" \
             % (join_name)
    runner.check(line)
    line = "select 1 from baseall where 1 > (select k1 from %s where k1 = 16 limit 1)" % (join_name)
    runner.check(line)
    line = "select 1 from baseall where 1 = (select k1 from %s where k1 = 16 limit 1)" % (join_name)
    runner.check(line)
    line = "select 1 from baseall where 1 < (select k1 from %s where k1 = 16 limit 1)" % (join_name)
    runner.check(line)
    line = "select 1 from baseall where 1 <= (select k1 from %s where k1 = 16 limit 1)" \
            % (join_name)
    runner.check(line)
    line = "select 1 from baseall b where 1 in (select k1 from %s a where a.k1 = 16 \
            and a.k1 = b.k1 )" % (join_name)
    runner.check(line)
    line = "select max(k1) from %s where k1 > 16" % (join_name)
    runner.check(line)
    line = "select 1 from %s where NULL" % (join_name)
    runner.check(line)
    line = "select a.k1, a.k2, b.k2 from %s a left join %s b on a.k1 = b.k1 where b.k2 = NULL \
            order by a.k1, a.k2" % (table_name, join_name)
    runner.check(line)
    line = "select a.k1, a.k2, b.k2 from %s a left join %s b on a.k1 = b.k1 where b.k2 is NULL \
            order by a.k1, a.k2" % (table_name, join_name)
    runner.check(line)
    line = "select a.k1, a.k2, b.k2 from %s a left join %s b on a.k1 = b.k1 where b.k2 != NULL \
            order by a.k1, a.k2" % (table_name, join_name)
    runner.check(line)
    line = "select a.k1, a.k2, b.k2 from %s a left join %s b on a.k1 = b.k1 where b.k2 is not NULL \
            order by a.k1, a.k2" % (table_name, join_name)
    runner.check(line)
    
    
def test_query_cast():
    """
    {
    "title": "test_query_datatype.test_query_cast",
    "describe": "test for cast type",
    "tag": "function,p0"
    }
    """
    """
    test for cast type 
    """
    line1 = "select cast(k1 as int), cast(k2 as int), cast(k3 as int) from %s order by 1, 2, 3" \
            % (table_name)
    line2 = "select cast(k1 as signed), cast(k2 as signed), cast(k3 as signed) from %s " \
            "order by 1, 2, 3" % (table_name)
    runner.check2(line1, line2)
    line = "select k2, k3, (cast(\"1971-01-01\" as date) + interval k2 day) from %s " \
           "order by 1, 2, 3" % (table_name)
    runner.check(line)
    line = "select cast(k1+k10 as date),k10,k1 from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line1 = "select 1.1*1.1 + cast(1.1 as decimal)"
    line2 = "select 1.1*1.1 + cast(1.1 as decimal(2,0))"
    runner.check2(line1, line2)
    line1 = "select k3, k4 from %s where abs(cast(k4 as decimal)) > negative(abs(cast(k4 as decimal))) " \
            "order by 1, 2" % (table_name)
    line2 = "select k3, k4 from %s where abs(cast(k4 as decimal(8, 0))) > -(abs(cast(k4 as decimal(20, 0)))) " \
            "order by 1, 2" % (table_name)
    runner.check2(line1, line2)
    for i in (1, 2, 3, 4, 10, 11):
        line = "select cast(k%s as char),k1 from %s order by k1, k2, k3, k4" % (i, table_name)
        runner.check(line)
    for i in (1, 2, 3, 4, 10, 11):
        line1 = "select cast(k%s as bigint),k1 from %s order by k1, k2, k3, k4" % (i, table_name)
        line2 = "select cast(k%s as signed),k1 from %s order by k1, k2, k3, k4" % (i, table_name)
        runner.check2(line1, line2)
    # need more cases
    # Palo: 9.24795174e-09 mysql: 0.00000000924795; 存在精度问题
    line = "select cast(k9 as char) from %s where k2 = 23406 order by 1" % (table_name)
    runner.checkok(line)
    line = "select cast(k9 as char) from %s where k2 = -32729 order by 1" % (table_name)
    runner.checkok(line)


def verify(sql, result):
    """verify sql result, result only has one data"""
    LOG.info(L('palo sql', palo_sql=sql))
    ret = runner.query_palo.do_sql(sql)
    print(sql)
    print(ret, result)
    LOG.info(L('palo result', palo_result=ret))
    if result is None:
        assert ret[0][0] is None
    else:
        print(str(ret[0][0]), result)
        LOG.info(L('palo excepted result', palo_excepted_result=result))
        assert str(ret[0][0]) == result, '%s vs %s' % (str(ret[0][0]), result)


def test_query_cast_tinyint():
    """
    {
    "title": "test_query_datatype.test_query_cast_tinyint",
    "describe": "test basic cast function",
    "tag": "function,p0,fuzz"
    }
    """
    """test basic cast function"""
    line1 = 'select cast("hello" as tinyint)'
    verify(line1, None)
    line1 = 'select cast("123.123" as tinyint)'
    verify(line1, "123")
    line1 = 'select cast("" as tinyint)'
    verify(line1, None)
    line1 = 'select cast("2000-01-01" as tinyint)'
    verify(line1, None)
    line1 = 'select cast(20001230 as tinyint)'
    verify(line1, "-50")
    line1 = 'select cast(1.234 as tinyint)'
    verify(line1, "1")
    line1 = 'select cast(a as tinyint) from (select cast("2000-01-01" as date) a) ta'
    verify(line1, "101")
    line1 = 'select cast(a as tinyint) from (select cast("2000-01-01 00:00:00" as datetime) a) ta'
    verify(line1, "64")
    line1 = 'select cast(true as tinyint)'
    verify(line1, "1")
    line1 = 'select cast(false as tinyint)'
    verify(line1, "0")


def test_query_cast_smallint():
    """
    {
    "title": "test_query_datatype.test_query_cast_smallint",
    "describe": "test cast as smallint",
    "tag": "function,p0,fuzz"
    }
    """
    """test cast as smallint"""
    line1 = 'select cast("hello" as smallint)'
    verify(line1, None)
    line1 = 'select cast("123.123" as smallint)'
    verify(line1, "123")
    line1 = 'select cast("" as smallint)'
    verify(line1, None)
    line1 = 'select cast("2000-01-01" as smallint)'
    verify(line1, None)
    line1 = 'select cast(20001230 as smallint)'
    verify(line1, "12750")
    line1 = 'select cast(1.234 as smallint)'
    verify(line1, "1")
    line1 = 'select cast(a as smallint) from (select cast("2000-01-01" as date) a) ta'
    verify(line1, "11621")
    line1 = 'select cast(a as smallint) from (select cast("2000-01-01 00:00:00" as datetime) a) ta'
    verify(line1, "25408")
    line1 = 'select cast(true as smallint)'
    verify(line1, "1")
    line1 = 'select cast(false as smallint)'
    verify(line1, "0")


def test_query_cast_int():
    """
    {
    "title": "test_query_datatype.test_query_cast_int",
    "describe": "test cast as int",
    "tag": "function,p0,fuzz"
    }
    """
    """test cast as int"""
    line1 = 'select cast("hello" as int)'
    verify(line1, None)
    line1 = 'select cast("123.123" as int)'
    verify(line1, "123")
    line1 = 'select cast("" as int)'
    verify(line1, None)
    line1 = 'select cast("2000-01-01" as int)'
    verify(line1, None)
    line1 = 'select cast(20001230 as int)'
    verify(line1, "20001230")
    line1 = 'select cast(1.234 as int)'
    verify(line1, "1")
    line1 = 'select cast(a as int) from (select cast("2000-01-01" as date) a) ta'
    verify(line1, "20000101")
    line1 = 'select cast(a as int) from (select cast("2000-01-01 00:00:00" as datetime) a) ta'
    verify(line1, "-1561697472")
    line1 = 'select cast(true as int)'
    verify(line1, "1")
    line1 = 'select cast(false as int)'
    verify(line1, "0")


def test_query_cast_bigint():
    """
    {
    "title": "test_query_datatype.test_query_cast_bigint",
    "describe": "test cast as bigint",
    "tag": "function,p0,fuzz"
    }
    """
    """test cast as bigint"""
    line1 = 'select cast("hello" as bigint)'
    verify(line1, None)
    line1 = 'select cast("123.123" as bigint)'
    verify(line1, "123")
    line1 = 'select cast("" as bigint)'
    verify(line1, None)
    line1 = 'select cast("2000-01-01" as bigint)'
    verify(line1, None)
    line1 = 'select cast(20001230 as bigint)'
    verify(line1, "20001230")
    line1 = 'select cast(1.234 as bigint)'
    verify(line1, "1")
    line1 = 'select cast(a as bigint) from (select cast("2000-01-01" as date) a) ta'
    verify(line1, "20000101")
    line1 = 'select cast(a as bigint) from (select cast("2000-01-01 00:00:00" as datetime) a) ta'
    verify(line1, "20000101000000")
    line1 = 'select cast(true as bigint)'
    verify(line1, "1")
    line1 = 'select cast(false as bigint)'
    verify(line1, "0")


def test_query_cast_largeint():
    """
    {
    "title": "test_query_datatype.test_query_cast_largeint",
    "describe": "test cast as largeint",
    "tag": "function,p0,fuzz"
    }
    """
    """test cast as largeint"""
    line1 = 'select cast("hello" as largeint)'
    verify(line1, None)
    line1 = 'select cast("123.123" as largeint)'
    verify(line1, "123")
    line1 = 'select cast("" as largeint)'
    verify(line1, None)
    line1 = 'select cast("2000-01-01" as largeint)'
    verify(line1, None)
    line1 = 'select cast(20001230 as largeint)'
    verify(line1, "20001230")
    line1 = 'select cast(1.234 as largeint)'
    verify(line1, "1")
    line1 = 'select cast(a as largeint) from (select cast("2000-01-01" as date) a) ta'
    verify(line1, "20000101")
    line1 = 'select cast(a as largeint) from (select cast("2000-01-01 00:00:00" as datetime) a) ta'
    verify(line1, "20000101000000")
    line1 = 'select cast(true as largeint)'
    verify(line1, "1")
    line1 = 'select cast(false as largeint)'
    verify(line1, "0")


def test_query_cast_double():
    """
    {
    "title": "test_query_datatype.test_query_cast_double",
    "describe": "test cast as double",
    "tag": "function,p0,fuzz"
    }
    """
    """test cast as double"""
    line1 = 'select cast("hello" as double)'
    verify(line1, None)
    line1 = 'select cast("123.123" as double)'
    verify(line1, "123.123")
    line1 = 'select cast("" as double)'
    verify(line1, None)
    line1 = 'select cast("2000-01-01" as double)'
    verify(line1, None)
    line1 = 'select cast(20001230 as double)'
    verify(line1, "20001230.0")
    line1 = 'select cast(1.234 as double)'
    verify(line1, "1.234")
    line1 = 'select cast(a as double) from (select cast("2000-01-01" as date) a) ta'
    verify(line1, "20000101.0")
    line1 = 'select cast(a as double) from (select cast("2000-01-01 00:00:00" as datetime) a) ta'
    line2 = 'select 2.0000101e+13'
    runner.check2(line1, line2)
    line1 = 'select cast(true as double)'
    verify(line1, "1.0")
    line1 = 'select cast(false as double)'
    verify(line1, "0.0")


def test_query_cast_float():
    """
    {
    "title": "test_query_datatype.test_query_cast_float",
    "describe": "test cast as float",
    "tag": "function,p0,fuzz"
    }
    """
    """test cast as float"""
    line1 = 'select cast("hello" as float)'
    verify(line1, None)
    line1 = 'select cast("123.123" as float)'
    verify(line1, "123.123")
    line1 = 'select cast("" as float)'
    verify(line1, None)
    line1 = 'select cast("2000-01-01" as float)'
    verify(line1, None)
    line1 = 'select cast(20001230 as float)'
    verify(line1, "20001230.0")
    line1 = 'select cast(1.234 as float)'
    verify(line1, "1.234")
    line1 = 'select cast(a as float) from (select cast("2000-01-01" as date) a) ta'
    verify(line1, "20000100.0")
    line1 = 'select cast(a as float) from (select cast("2000-01-01 00:00:00" as datetime) a) ta'
    line2 = 'select 2.00001e+13'
    runner.check2(line1, line2)
    line1 = 'select cast(true as float)'
    verify(line1, "1.0")
    line1 = 'select cast(false as float)'
    verify(line1, "0.0")


def test_query_cast_decimal():
    """
    {
    "title": "test_query_datatype.test_query_cast_decimal",
    "describe": "test cast as decimal",
    "tag": "function,p0,fuzz"
    }
    """
    """test cast as decimal"""
    line1 = 'select cast("hello" as decimal)'
    verify(line1, None)
    line1 = 'select cast("123.123" as decimal)'
    verify(line1, "123")
    line1 = 'select cast("" as decimal)'
    verify(line1, None)
    line1 = 'select cast("2000-01-01" as decimal)'
    verify(line1, "2000")
    line1 = 'select cast(20001230 as decimal)'
    verify(line1, "20001230")
    line1 = 'select cast(1.234 as decimal)'
    verify(line1, "1")
    line1 = 'select cast(a as decimal) from (select cast("2000-01-01" as date) a) ta'
    verify(line1, "20000101")
    line1 = 'select cast(a as decimal) from (select cast("2000-01-01 00:00:00" as datetime) a) ta'
    verify(line1, "20000101000000")
    line1 = 'select cast(true as decimal)'
    runner.check(line1)
    line1 = 'select cast(false as decimal)'
    runner.check(line1)


def test_query_cast_decimal_in_table():
    """
    {
    "title": "test_query_datatype.test_query_cast_decimal_in_table",
    "describe": "create table,load,query, compare the accuracy",
    "tag": "function,p0,fuzz"
    }
    """
    """create table,load,query, compare the accuracy"""
    test_cast_decimal = "test_cast_decimal_in_table"
    sql = 'create table %s(k1 tinyint, k2 decimal(6,3) NULL, k3 char(5) NULL,\
            k4 date NULL, k5 datetime NULL, \
            k6 double sum) engine=olap \
            distributed by hash(k1) buckets 2 properties\
            ("storage_type"="column")' % test_cast_decimal
    runner.query_palo.do_sql(sql)
    # insert data
    insert_pre = "insert into %s values" % test_cast_decimal
    low_accuracy = insert_pre + "(1, 123.23, 'a', '2019-01-01', '2019-01-01 01:00:00', 6.6)"
    runner.query_palo.do_sql(low_accuracy)
    high_accuracy = insert_pre + "(1, 1.1267, 'a', '2019-01-01', '2019-01-01 01:00:00', 6.6)"
    runner.query_palo.do_sql(high_accuracy)
    err_accuracy = insert_pre + "(1, 123.1234, 'a', '2019-01-01', '2019-01-01 01:00:00', 6.6)"
    try:
        runner.query_palo.do_sql(high_accuracy)
        assert 1 == 0, "insert decimal err_accuracy"
    except Exception as err:
        assert 1 == 1, "insert decimal err_accuracy"
    # select
    line1 = 'select k2 from %s order by k2 limit 1' % test_cast_decimal
    verify(line1, "1.127")
    line1 = 'select cast(k2 as decimal(6, 4))from %s order by k2 limit 1' % test_cast_decimal
    line2 = 'select 1.127'
    runner.check2(line1, line2)
    line1 = 'select cast(k2 as decimal(6, 1))from %s order by k2 limit 1' % test_cast_decimal
    line2 = 'select 1.1'
    runner.check2(line1, line2)
    line1 = 'select cast(k2 as decimal(9, 1))from %s order by k2 limit 1' % test_cast_decimal
    runner.check2(line1, line2)

    # 其他操作
    line1 = 'select cast(k2+k1 as decimal(9, 1))from %s order by k2 limit 1' % test_cast_decimal
    line2 = 'select 2.1'
    runner.check2(line1, line2)
    line1 = 'select cast(k2 as decimal(9-1, 1))from %s order by k2 limit 1' % test_cast_decimal
    runner.checkwrong(line1)
     
    sql = "drop table %s" % test_cast_decimal
    runner.query_palo.do_sql(sql)


def test_query_cast_char():
    """
    {
    "title": "test_query_datatype.test_query_cast_char",
    "describe": "test cast as char",
    "tag": "function,p0,fuzz"
    }
    """
    """test cast as char"""
    line1 = 'select cast("hello" as char)'
    verify(line1, "hello")
    line1 = 'select cast("123.123" as char)'
    verify(line1, "123.123")
    line1 = 'select cast("" as char)'
    verify(line1, "")
    line1 = 'select cast("2000-01-01" as char)'
    verify(line1, "2000-01-01")
    line1 = 'select cast(20001230 as char)'
    verify(line1, "20001230")
    line1 = 'select cast(1.234 as char)'
    verify(line1, "1.234")
    line1 = 'select cast(a as char) from (select cast("2000-01-01" as date) a) ta'
    verify(line1, "2000-01-01")
    line1 = 'select cast(a as char) from (select cast("2000-01-01 00:00:00" as datetime) a) ta'
    verify(line1, "2000-01-01 00:00:00")
    line1 = 'select cast(true as char)'
    verify(line1, "1")
    line1 = 'select cast(false as char)'
    verify(line1, "0")
    line1 = 'select cast(round(26107/1232, 2) as string)'
    verify(line1, "21.19")
    line1 = 'select cast(3285/18 as string)'
    verify(line1, "182.5")
    line1 = 'select cast(cast(10000.00001 as double) as string)'
    verify(line1, "10000.00001")


def test_query_cast_varchar():
    """
    {
    "title": "test_query_datatype.test_query_cast_varchar",
    "describe": "test cast as varchar",
    "tag": "function,p0,fuzz"
    }
    """
    """test cast as varchar"""
    line1 = 'select cast("hello" as varchar)'
    verify(line1, "hello")
    line1 = 'select cast("123.123" as varchar)'
    verify(line1, "123.123")
    line1 = 'select cast("" as varchar)'
    verify(line1, "")
    line1 = 'select cast("2000-01-01" as varchar)'
    verify(line1, "2000-01-01")
    line1 = 'select cast(20001230 as varchar)'
    verify(line1, "20001230")
    line1 = 'select cast(1.234 as varchar)'
    verify(line1, "1.234")
    line1 = 'select cast(a as varchar) from (select cast("2000-01-01" as date) a) ta'
    verify(line1, "2000-01-01")
    line1 = 'select cast(a as varchar) from (select cast("2000-01-01 00:00:00" as datetime) a) ta'
    verify(line1, "2000-01-01 00:00:00")
    line1 = 'select cast(true as varchar)'
    verify(line1, "true")
    line1 = 'select cast(false as varchar)'
    verify(line1, "false")


def test_query_cast_string():
    """
    {
    "title": "test_query_datatype.test_query_cast_string",
    "describe": "test cast as string",
    "tag": "function,p0,fuzz"
    }
    """
    """test cast as string"""
    line1 = 'select cast("hello" as string)'
    verify(line1, "hello")
    line1 = 'select cast("123.123" as string)'
    verify(line1, "123.123")
    line1 = 'select cast("" as string)'
    verify(line1, "")
    line1 = 'select cast("2000-01-01" as string)'
    verify(line1, "2000-01-01")
    line1 = 'select cast(20001230 as string)'
    verify(line1, "20001230")
    line1 = 'select cast(1.234 as string)'
    verify(line1, "1.234")
    line1 = 'select cast(a as string) from (select cast("2000-01-01" as date) a) ta'
    verify(line1, "2000-01-01")
    line1 = 'select cast(a as string) from (select cast("2000-01-01 00:00:00" as datetime) a) ta'
    verify(line1, "2000-01-01 00:00:00")
    line1 = 'select cast(true as string)'
    verify(line1, "true")
    line1 = 'select cast(false as string)'
    verify(line1, "false")


def test_query_cast_date():
    """
    {
    "title": "test_query_datatype.test_query_cast_date",
    "describe": "test cast as date",
    "tag": "function,p0,fuzz"
    }
    """
    """test cast as date"""
    line1 = 'select cast("hello" as date)'
    verify(line1, None)
    line1 = 'select cast("123.123" as date)'
    verify(line1, "2012-03-12")
    line1 = 'select cast("" as date)'
    verify(line1, None)
    line1 = 'select cast("2000-01-01" as date)'
    verify(line1, "2000-01-01")
    line1 = 'select cast(20001230 as date)'
    verify(line1, "2000-12-30")
    line1 = 'select cast(1.234 as date)'
    verify(line1, None)
    line1 = 'select cast(a as date) from (select cast("2000-01-01" as date) a) ta'
    verify(line1, "2000-01-01")
    line1 = 'select cast(a as date) from (select cast("2000-01-01 00:00:00" as datetime) a) ta'
    verify(line1, "2000-01-01")
    line1 = 'select cast(true as date)'
    # runner.checkwrong(line1)
    verify(line1, None)
    line1 = 'select cast(false as date)'
    verify(line1, None)
    # runner.checkwrong(line1)


def test_query_cast_datetime():
    """
    {
    "title": "test_query_datatype.test_query_cast_datetime",
    "describe": "test cast as datetime",
    "tag": "function,p0,fuzz"
    }
    """
    """test cast as datetime"""
    line1 = 'select cast("hello" as datetime)'
    verify(line1, None)
    line1 = 'select cast("123.123" as datetime)'
    verify(line1, "2012-03-12 03:00:00")
    line1 = 'select cast("" as datetime)'
    verify(line1, None)
    line1 = 'select cast("2000-01-01" as datetime)'
    verify(line1, "2000-01-01 00:00:00")
    line1 = 'select cast(20001230 as datetime)'
    verify(line1, "2000-12-30 00:00:00")
    line1 = 'select cast(1.234 as datetime)'
    verify(line1, None)
    line1 = 'select cast(a as datetime) from (select cast("2000-01-01" as date) a) ta'
    verify(line1, "2000-01-01 00:00:00")
    line1 = 'select cast(a as datetime) from (select cast("2000-01-01 00:00:00" as datetime) a) ta'
    verify(line1, "2000-01-01 00:00:00")
    line1 = 'select cast(true as datetime)'
    # runner.checkwrong(line1)
    verify(line1, None)
    line1 = 'select cast(false as datetime)'
    # runner.checkwrong(line1)
    verify(line1, None)


def teardown_module():
    """
    end 
    """
    print("End")


if __name__ == "__main__":
    print("test")
    setup_module()

