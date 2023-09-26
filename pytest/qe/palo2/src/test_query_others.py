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

import pymysql
import sys
import time
sys.path.append("../lib/")
from palo_qe_client import PaloQE
from palo_qe_client import QueryBase
import query_util as util

table_name = "test"
join_name = "baseall"


def setup_module():
    """
    init config
    """
    global runner
    runner = QueryBase()
    
        
def test_query_sys():
    """
    {
    "title": "test_query_others.test_query_sys",
    "describe": "system query",
    "tag": "function,p1"
    }
    """
    """
    system query
    """
    line = "SELECT DATABASE()"
    runner.checkok(line)
    line = "SELECT \"welecome to my blog!\""
    runner.check(line)
    line = "describe %s" % (table_name)
    runner.checkok(line)
    line = "select version()"
    runner.checkok(line)
    line = "select rand()"
    runner.checkok(line)
    line = "select rand(20)"
    runner.checkok(line)
    line = "select random()"
    runner.checkok(line)
    line = "select random(20)"
    runner.checkok(line)
    line = "SELECT CONNECTION_ID()"
    runner.checkok(line)
    line = "SELECT CURRENT_USER()"
    runner.checkok(line)
    line = "select now()"
    runner.checkok(line)
    line = "select localtime()"
    runner.checkok(line)
    line = "select localtimestamp()"
    runner.checkok(line)
    line = "select pi()"
    runner.check(line)
    line = "select e()"
    runner.checkok(line)
    line = "select sleep(2)"
    runner.check2(line, "select 1")


def test_bug_buchong():
    """
    {
    "title": "test_query_others.test_bug_buchong",
    "describe": "describe",
    "tag": "function,p1"
    }
    """
    """
    todo
    """
    # query_palo1 = PaloQE(query_host, query_port, query_user, query_passwd, "")
    mysql_con1 = pymysql.connect(host=runner.mysql_host, user=runner.mysql_user,
                                 passwd=runner.mysql_passwd, port=runner.mysql_port, db="")
    palo_con1 = pymysql.connect(host=runner.query_host, user=runner.query_user,
                                passwd=runner.query_passwd, port=runner.query_port, db="")
    mysql_cursor1 = mysql_con1.cursor()
    palo_cursor1 = palo_con1.cursor()
    line1 = "select * from %s.%s t where k1=1 order by k1, k2, k3, k4" % (runner.query_db, table_name)
    line2 = "select * from %s.%s t where k1=1 order by k1, k2, k3, k4" % (runner.mysql_db, table_name)
    print(line1)
    print(line2)
    palo_cursor1.execute(line1)
    palo_result = palo_cursor1.fetchall()
    print('down')
    mysql_cursor1.execute(line2)
    mysql_result = mysql_cursor1.fetchall()
    util.check_same(palo_result, mysql_result)
    palo_cursor1.close()
    palo_con1.close()
    mysql_cursor1.close()
    mysql_con1.close()


def test_query_limit():
    """
    {
    "title": "test_query_others.test_query_limit",
    "describe": "test for limit",
    "tag": "function,p1"
    }
    """
    """
    test for limit
    """
    line = "select * from %s order by k1, k2, k3, k4 limit 2" % (table_name)
    runner.check(line)
    line = "select * from %s order by k1, k2, k3, k4 limit 0" % (table_name)
    runner.check(line)
    line = "select * from %s where k6 = 'true' limit 0" % (table_name)
    runner.check(line)
    line = "select * from %s order by k1, k2, k3, k4 limit 100" % (table_name)
    runner.check(line)
    line = "select * from %s order by k1, k2, k3, k4 limit 2, 2" % (table_name)
    runner.check(line)
    line = "select * from %s order by k1, k2, k3, k4 limit 2, 20" % (table_name)
    runner.check(line)
    line = "select * from %s order by k1, k2, k3, k4 desc limit 2" % (table_name)
    runner.check(line)
    line = "select * from %s order by k1, k2, k3, k4 desc limit 0" % (table_name)
    runner.check(line)
    line = "select * from %s order by k1, k2, k3, k4 desc limit 100" % (table_name)
    runner.check(line)
    line = "select k3, sum(k9) from %s where k1<5 group by 1 order by 2 limit 3" % (table_name)
    runner.check(line)
    line = "select * from (select * from %s union all select * from %s) b limit 0" \
            % (table_name, join_name)
    runner.check(line)


def test_query_offset():
    """
    {
    "title": "test_query_others.test_query_offset",
    "describe": "test for offset",
    "tag": "function,p1"
    }
    """
    """
    test for offset
    """
    line = "select * from %s order by k1, k2, k3, k4 limit 2 offset 0" % (table_name)
    runner.check(line)
    line = "select * from %s order by k1, k2, k3, k4 limit 0 offset 10" % (table_name)
    runner.check(line)
    line = "select * from %s order by k1, k2, k3, k4 limit 100 offset 10" % (table_name)
    runner.check(line)
    line = "select * from %s order by k1, k2, k3, k4 limit 2 offset 20" % (table_name)
    runner.check(line)


def test_query_all():
    """
    {
    "title": "test_query_others.test_query_all",
    "describe": "test query all",
    "tag": "function,p1"
    }
    """
    """
    test query all
    """
    line = "select all k1 from %s order by k1, k2, k3, k4" % table_name
    runner.check(line)
    line = "select all * from %s order by k1, k2, k3, k4" % table_name
    runner.check(line)


def test_query_distinct():
    """
    {
    "title": "test_query_others.test_query_distinct",
    "describe": "test for distinct",
    "tag": "function,p1"
    }
    """
    """
    test for distinct
    """
    line = "select distinct k1 from %s order by k1" % (table_name)
    runner.check(line)
    line = "select distinct k2 from %s order by k2" % (table_name)
    runner.check(line)
    line = "select distinct k3 from %s order by k3" % (table_name)
    runner.check(line)
    line = "select distinct k4 from %s order by k4" % (table_name)
    runner.check(line)
    line = "select distinct k5 from %s order by k5" % (table_name)
    runner.check(line)
    line = "select distinct upper(k6) from %s order by upper(k6)" % (table_name)
    runner.check(line)
    line = "select distinct k8 from %s order by k8" % (table_name)
    runner.check(line)
    line = "select distinct k9 from %s order by k9" % (table_name)
    runner.check(line)
    line = "select distinct k10 from %s order by k10" % (table_name)
    runner.check(line)
    line = "select distinct k11 from %s order by k11" % (table_name)
    runner.check(line)
    line = "select distinct k1, upper(k6), k9 from %s order by k1, upper(k6), k9" % (table_name)
    runner.check(line)
    line = "select count(distinct k1, k5) from %s" % (table_name)
    runner.check(line)
    line = "select k1, count(distinct k3), sum(distinct k2), count(k6) from %s \
		    group by 1 order by 1, 2, 3" % (table_name)
    runner.check(line)
    line = "select count(distinct k1) from %s order by max(distinct k1) limit 100" % (table_name)
    runner.check(line)
    line = "select distinct * from %s where k1<20 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select distinct * from %s order by k1, k2, k3, k4" % table_name
    runner.check(line)
    line = "select count(distinct NULL) from %s" % (table_name)
    runner.check(line)
    line = "select count(distinct k1, NULL) from %s" % (table_name)
    runner.check(line)
    '''
    + group by
    '''
    line = "select t1.c, t2.c from (select count(distinct k1) as c from %s) t1 join \
		                     (select count(distinct k1) as c from %s) t2 on\
				     (t1.c = t2.c) order by t1.c, t2.c" % (table_name, join_name)
    runner.check(line)
    line = "select count(distinct k1) from %s having count(k1)>60000" % (table_name)
    runner.check(line)
    line = "select count(distinct k1) from %s having count(k1)>70000" % (table_name)
    runner.check(line)
    line = "select count(*), COUNT(distinct 1) from %s where false" % (table_name)
    runner.check(line)
    line = "select avg(distinct k1), avg(k1) from %s" % (table_name)
    runner.check(line)
    line = "select count(*) from (select count(distinct k1) from %s group by k2) v \
		    order by count(*)" % (table_name)
    runner.check(line)


def test_query_as_simple():
    """
    {
    "title": "test_query_others.test_query_as_simple",
    "describe": "test for as",
    "tag": "function,p1"
    }
    """
    """
    test for as
    """
    line = "select * from %s as a, %s as b where a.k1 = b.k1 \
		    order by a.k1, a.k2, a.k3, a.k4, b.k1, b.k2, b.k3, b.k4" \
		    % (join_name, table_name)
    runner.check(line)

    line = "select k1 as k2 from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select date_format(b.k10, '%%Y%%m%%d') as k10 from %s a left join (select k10 from %s) b \
            on a.k10 = b.k10 group by k10 order by k10" % (join_name, table_name)
    runner.check(line)


def test_query_having():
    """
    {
    "title": "test_query_others.test_query_having",
    "describe": "test for having clause",
    "tag": "function,p1"
    }
    """
    """test for having clause"""
    line = 'select k1, k2 from ((select * from baseall) union all (select * from bigtable)) a \
            having k1 = 1 order by 1, 2'
    runner.check(line)
    line = 'select k1, k2 from baseall having k1 % 3 = 0 order by k1, k2'
    runner.check(line)
    line = 'select count(k1) b from baseall where k2 = 1989 having b >= 2 order by b'
    runner.check(line)
    line = 'select count(k1) b from baseall where k2 = 1989 having b > 2 order by b'
    runner.check(line)
    line = 'select k2, 0 as x from baseall group by k2 having k2 > 0 and x > 1 order by k2'
    runner.check(line)
    line = 'select k2, 0 as x from baseall group by k2 having k2 > 0 order by k2'
    runner.check(line)
    line = 'select k2, count(k1) b from baseall group by k2 having max(k1) > 2 order by k2'
    runner.check(line)

    line = 'select a.k1, a.k2, a.k3, b.k2 from baseall a left join baseall b on a.k1 = b.k1 + 5 \
            having b.k2 < 0 order by a.k1'
    runner.check(line)
    line = 'select a.k1, a.k2, a.k3, b.k2 from baseall a left outer join baseall b on a.k1 = b.k1 + 5 \
            having b.k2 is not null order by a.k1'
    runner.check(line)
    line = 'select a.k1, a.k2, a.k3, b.k2 from baseall a join baseall b on a.k1 = b.k1 + 5 \
            having b.k2 < 0 order by a.k1'
    runner.check(line)
    line = ' select k2, count(*) from baseall group by k2 having k2 > 1000 order by k2'
    runner.check(line)


def test_query_information_schema():
    """
    {
    "title": "test_query_others.test_query_information_schema",
    "describe": "test for information_schema",
    "tag": "function,p1"
    }
    """
    """test for information_schema"""
    query_palo1 = PaloQE(runner.query_host, runner.query_port, runner.query_user, 
                         runner.query_passwd, None)
    mysql_con1 = pymysql.connect(host=runner.mysql_host, user=runner.mysql_user,
                                 passwd=runner.mysql_passwd, port=runner.mysql_port)
    mysql_cursor1 = mysql_con1.cursor()
    line1 = 'SELECT table_name FROM INFORMATION_SCHEMA.TABLES ' \
        'WHERE table_schema = "%s" ' \
        'and TABLE_TYPE = "BASE TABLE" order by table_name' % (runner.query_db)
    line2 = 'SELECT table_name FROM INFORMATION_SCHEMA.TABLES ' \
        'WHERE table_schema = "%s" ' \
        'and TABLE_TYPE = "BASE TABLE" order by table_name' % (runner.mysql_db)
    print(line1)
    print(line2)
    palo_result = query_palo1.do_sql(line1)
    mysql_cursor1.execute(line2)
    mysql_result = mysql_cursor1.fetchall()
    # 可能与MySQL的表不一样，验证Palo查询正确即可
    # util.check_same(palo_result, mysql_result)
    line3 = 'SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT ' \
        'FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = "test" ' \
        'AND table_schema = "%s" AND column_name LIKE "k%%"' % (runner.query_db)
    line4 = 'SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT ' \
        'FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = "test" ' \
        'AND table_schema = "%s" AND column_name LIKE "k%%"' % (runner.mysql_db)
    print(line3)
    print(line4)
    palo_result = query_palo1.do_sql(line3)
    mysql_cursor1.execute(line4)
    mysql_result = mysql_cursor1.fetchall()
    # 与MySQL的结果不一致，此处验证Palo执行正确即可
    # util.check_same(palo_result, mysql_result)
    mysql_cursor1.close()
    mysql_con1.close()


def test_query_after_add_schema():
    """
    {
    "title": "test_query_others.test_query_after_add_schema",
    "describe": "验证add column后，向量化查询",
    "tag": "function,p1"
    }
    """
    """
    验证add column后，向量化查询
    """
    test_table = 'p_test'
    runner.query_palo.do_sql('drop table if exists %s' % test_table)
    sql = 'CREATE TABLE %s ( \
           `k1` int(11) NULL COMMENT "", \
           `k2` int(11) NULL COMMENT "", \
           `v1` int(11) SUM NULL COMMENT "" \
           ) ENGINE=OLAP \
           AGGREGATE KEY(`k1`, `k2`) \
           DISTRIBUTED BY HASH(`k1`) BUCKETS 1 \
           PROPERTIES ( \
           "storage_type" = "COLUMN" \
           );' % test_table
    result = runner.query_palo.do_sql(sql)
    assert result == ()
    sql = 'insert into %s select k1, k2, k3 from %s' % (test_table, join_name)
    runner.query_palo.do_sql(sql)
    runner.wait_end('load')
    time.sleep(30)
    line1 = 'select * from %s order by k1' % test_table
    line2 = 'select k1, k2, k3 from %s order by k1' % join_name
    runner.check2(line1, line2)
    sql = 'alter table %s add column v2 int sum NULL' % test_table
    runner.query_palo.do_sql(sql)
    runner.wait_end('schema_change')
    line2 = 'select k1, k2, k3, null from %s order by k1' % join_name
    runner.check2(line1, line2)


def test_query_as_1():
    """
    {
    "title": "test_query_others.test_query_as_1",
    "describe": "test as and derived table",
    "tag": "function,p1,fuzz"
    }
    """
    """test as and derived table"""
    line = "select * from (select k1 from baseall) b order by 1"
    runner.check(line)
    line = "select * from (select k1 from baseall) order by 1"
    runner.checkwrong(line)
    line = "select baseall.k1, t3.t from baseall, (select k2 as t from test where k2 = 1989) as t3 where \
            baseall.k1 > 0 and t3.t > 0 order by 1, 2"
    runner.check(line)
    line = "select baseall.k1, t3.k1 from baseall, (select k1 from test where k2 = 1989) as t3 where \
            baseall.k1 > 0 and t3.k1 > 0 order by 1, 2;"
    runner.check(line)
    line = "SELECT a FROM (SELECT 1 FROM (SELECT 1) a HAVING a=1) b"
    runner.checkwrong(line)
    line = "SELECT a,b as a FROM (SELECT '1' as a,'2' as b) b  HAVING a=1;"
    runner.checkwrong(line)
    line = "SELECT a,2 as a FROM (SELECT '1' as a) b HAVING a=1;"
    runner.checkwrong(line)
    line = "SELECT 1 FROM (SELECT 1) a WHERE a=2;"
    runner.checkwrong(line)
    line = "select * from baseall as x1, bigtable as x2;"
    runner.check(line, True)
    line = "select * from (select 1) as a;"
    runner.check(line)
    line = "select a from (select 1 as a) as b;"
    runner.check(line)
    line = "select 1 from (select 1) as a;"
    runner.check(line)
    line = "select * from (select * from baseall union select * from baseall) a order by k1;"
    runner.check(line)
    line = "select * from (select * from baseall union all select * from baseall) a order by k1;"
    runner.check(line)
    line = "select * from (select * from baseall union all \
            (select * from baseall order by k1 limit 2)) a order by k1"
    runner.check(line)
    line = "SELECT * FROM (SELECT k1 FROM test) as b ORDER BY k1  ASC LIMIT 0,20;"
    runner.check(line)
    line = "select * from (select 1 as a) b  left join (select 2 as a) c using(a);"
    # this is a bug.
    # runner.check(line)
    line = "select 1 from  (select 2) a order by 0;"
    runner.checkwrong(line)
    line = "select * from (select k1 from test group by k1) bar order by k1;"
    runner.check(line)
    line = "SELECT a.x FROM (SELECT 1 AS x) AS a HAVING a.x = 1;"
    runner.check(line)
    line = "select k1 as a, k2 as b, k3 as c from baseall t where a > 0;"
    runner.checkwrong(line)
    line = "select k1 as a, k2 as b, k3 as c from baseall t group by a, b, c order by a, b, c;"
    runner.check(line)
    line = "select k1 as a, k2 as b, k3 as c from baseall t group by a, b, c having a > 5 order by a, b, c;"
    runner.check(line)
    line = "select k1 as k7, k2 as k8, k3 as k9 from baseall t group by k7, k8, k9 having k7 > 5 \
            order by k7;"
    # this sql is empty and get warning in mysql, because of k7,k8,k9 is ambiguous
    try:
        runner.query_palo.do_set_properties_sql(line, ["set group_by_and_having_use_alias_first=true"])
    except Exception as e:
        assert 0 == 1, 'expect excute ok'
    line = "select k1 as k7, k2 as k8, k3 as k9 from baseall t where k8 > 0 group by k7, k8, k9 having k7 > 5 order by k7;"
    # same as above sql
    try:
        runner.query_palo.do_set_properties_sql(line, ["set group_by_and_having_use_alias_first=true"])
    except Exception as e:
        assert 0 == 1, 'expect excute ok'


def test_issue_6345():
    """
    {
    "title": "test_query_others.test_issue_6345",
    "describe": "test set runtime_filter parameter query,github issue #6345",
    "tag": "function,p1"
    }
    """
    runner.query_palo.do_sql("set runtime_filter_type=2")
    runner.query_palo.do_sql("set runtime_filter_mode='GLOBAL'")
    line = "SELECT DISTINCT k5 FROM baseall where k7 <> 'wang' AND k5 in (SELECT k5 FROM test WHERE k3 < 0) ORDER BY k5"
    runner.check(line)
    

def teardown_module():
    """
    end
    """
    print("End")
    # mysql_cursor.close()
    # mysql_con.close()
