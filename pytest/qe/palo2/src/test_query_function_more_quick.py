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
test_query_function_more_quick.py
"""
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


def setup_module():
    """
    todo
    """
    global runner
    runner = QueryExecuter()


class QueryExecuter(QueryBase):
    def __init__(self):
        self.get_clients()

    def excutesql(self, line):
        """
        mysql excute
        palo excute
        """
        print(line)
        times = 0
        flag = 0
        while (times <= 10 and flag == 0):
            try:
                LOG.info(L('palo&mysql both sql', sql=line))
                palo_result = self.query_palo.do_sql(line)
                self.mysql_cursor.execute(line)
                mysql_result = self.mysql_cursor.fetchall()
                util.check_same(palo_result, mysql_result)
                flag = 1
            except Exception as e:
                print(Exception, ":", e)
                print("hello")
                LOG.error(L('err', error=e))
                time.sleep(1)
                times += 1
                if (times == 3):
                    print("====================================================")
                    print(Exception, ":", e)
                    print("====================================================")
                    assert 0 == 1

    def drop_view(self, viewname="wj", viewtype="view"):
        """
        drop view
        """
        line = "drop %s if exists %s" % (viewtype, viewname)
        print(line)
        times = 0
        flag = 0
        while (times <= 10 and flag == 0):
            try:
                LOG.info(L('palo sql', sql=line))
                palo_result = self.query_palo.do_sql(line)
                flag = 1
            except Exception as e:
                if ("is not 'VIEW'" not in str(e)) and ("Unknown table" not in str(e)) \
                        and ("database doesn\'t exist" not in str(e)):
                    print(Exception, ":", e)
                    LOG.error(L('err', error=e))
                    time.sleep(1)
                    times += 1
                    if (times == 3):
                        assert 0 == 1
                else:
                    flag = 1
        times = 0
        flag = 0
        while (times <= 10 and flag == 0):
            try:
                LOG.info(L('mysql sql', sql=line))
                self.mysql_cursor.execute(line)
                mysql_result = self.mysql_cursor.fetchall()
                flag = 1
            except Exception as e:
                if ("Unknown table" not in str(e)) and \
                        ("database doesn\'t exist" not in str(e) and "No database selected" not in str(e)):
                    print(Exception, ":", e)
                    LOG.error(L('err', error=e))
                    time.sleep(1)
                    times += 1
                    if (times == 3):
                        assert 0 == 1
                else:
                    flag = 1

    def wait_end(self, job='LOAD'):
        """wait load/schema change job end"""
        timeout = 1200
        while timeout > 0:
            if job.upper() == 'LOAD':
                sql = 'show load'
                LOG.info(L('palo sql', palo_sql=sql))
                job_list = self.query_palo.do_sql(sql)
                state = job_list[-1][2]
                LOG.info(L('palo result', palo_result=state))
            elif job.upper() == 'SCHEMA_CHANGE':
                sql = 'show alter table column'
                LOG.info(L('palo sql', palo_sql=sql))
                job_list = self.query_palo.do_sql(sql)
                state = job_list[-1][7]
                LOG.info(L('palo result', palo_result=state))

            if state == "FINISHED" or state == "CANCELLED":
                #return state == "FINISHED"
                timeout = 0
            time.sleep(1)
            timeout -= 1
        assert state == 'FINISHED'


def test_query_create_view_many_times():
    """
    {
    "title": "test_query_function_more_quick.test_query_create_view_many_times",
    "describe": "create view on view",
    "tag": "function,p1"
    }
    """
    """
    create view on view
    """
    runner.drop_view("wj0")
    line = "create view wj0 as select * from %s" % (join_name)
    runner.excutesql(line)
    line = "select * from wj0 order by k1, k2, k3, k4"
    runner.check(line)

    for i in range(1, 15):
        viewname = "wj" + str(i)
        runner.drop_view(viewname)
        
    for i in range(1, 10):
        viewbase = "wj" + str(i - 1)
        viewnow = "wj" + str(i)
        line = "create view %s as select * from %s" % (viewnow, viewbase)
        runner.excutesql(line)
        line = "select * from %s order by k1, k2, k3, k4" % (viewnow)
        runner.check(line)

    for i in range(10, 15):
        viewbase = "wj" + str(i - 1)
        viewnow = "wj" + str(i)
        line = "create view %s as select a.k1 as k1, a.k2 as k2, a.k3 as k3, a.k4 as k4 \
                    from %s as a join %s as b where a.k1 = b.k1 order by k1, k2, k3, k4 limit 1000" \
                    % (viewnow, viewbase, table_name)
        runner.excutesql(line)
        line = "select * from %s order by k1, k2, k3, k4" % (viewnow)
        runner.check(line)
    
    for i in range(0, 15):
        viewname = "wj" + str(i)
        runner.drop_view(viewname)


def test_query_create_view_insert():
    """
    {
    "title": "test_query_function_more_quick.test_query_create_view_insert",
    "describe": "create view first and insert then",
    "tag": "function,p1"
    }
    """
    """
    create view first and insert then
    """
    runner.drop_view()
    runner.drop_view("wjview", "table")
    line1 = "create table wjview(\
		    k1 tinyint, k2 smallint, k3 int, k4 bigint, k5 decimal(9,3), \
		    k6 char(5), k10 date, k11 datetime, k7 varchar(20), \
		    k8 double max, k9 float sum)\
		    engine=olap distributed by hash(k1) buckets 5 \
		    properties(\"storage_type\"=\"column\")"
    line2 = "CREATE TABLE wjview select * from %s where k1<0" % (join_name)
    # line1 create on palo, line2 create on mysql
    runner.init(line1, line2)
    line = "select * from %s order by k1, k2, k3, k4" % (join_name)
    runner.check(line)

    line = "create view wj as select * from wjview" 
    runner.excutesql(line)
    line = "select * from wj order by k1, k2, k3, k4"
    runner.check(line)
    
    line = "insert into wjview select * from %s" % (join_name)
    runner.init(line)
    line = "select * from wj order by k1, k2, k3, k4" 
    runner.check(line)
    
    runner.drop_view()
    runner.drop_view("wjview", "table")


def create_table_tmp():
    """
    create table in mysql and palo
    """
    runner.drop_view("wjview", "table")
    line = "create table wjview(\
		    k1 tinyint, k2 smallint, k3 int, k4 bigint, k5 decimal(9,3), \
		    k6 char(5), k10 date, k11 datetime, k7 varchar(20), \
		    k8 double max, k9 float sum)\
		    engine=olap distributed by hash(k1) buckets 5 \
		    properties(\"storage_type\"=\"column\")"
    print(line)
    palo_result = runner.query_palo.do_sql(line)
    line = "insert into wjview select * from %s" % (join_name)  
    print(line)
    palo_result = runner.query_palo.do_sql(line)
    time.sleep(15)
    runner.wait_end()
    line = "CREATE TABLE wjview SELECT * FROM %s" % (join_name)
    print(line)
    runner.mysql_cursor.execute(line)
    mysql_result = runner.mysql_cursor.fetchall()
    line = "select * from %s order by k1, k2, k3, k4" % (join_name)
    runner.check(line)


def test_query_create_view_drop():
    """
    {
    "title": "test_query_function_more_quick.test_query_create_view_drop",
    "describe": "create view first and drop view then",
    "tag": "function,p1"
    }
    """
    """
    create view first and drop view then
    """
    runner.drop_view("wj0")
    line = "create view wj0 as select * from %s" % (join_name)
    runner.excutesql(line)
    line = "select * from wj0 order by k1, k2, k3, k4"
    runner.check(line)

    for i in range(1, 15):
        viewname = "wj" + str(i)
        runner.drop_view(viewname)
        
    for i in range(1, 10):
        viewbase = "wj" + str(i - 1)
        viewnow = "wj" + str(i)
        line = "create view %s as select * from %s" % (viewnow, viewbase)
        runner.excutesql(line)
        line = "select * from %s order by k1, k2, k3, k4" % (viewnow)
        runner.check(line)

    runner.drop_view("wj5")
    for i in range(6, 10):
        viewnow = "wj" + str(i)
        line = "select * from %s order by k1, k2, k3, k4" % (viewnow)
        flag = 0
        try:
            palo_result = runner.query_palo.do_sql(line)
        except Exception as e:
            flag = 1
        assert flag == 1
   
    for i in range(1, 5):
        viewnow = "wj" + str(i)
        line = "select * from %s order by k1, k2, k3, k4" % (viewnow)
        runner.check(line)
    for i in range(0, 10):
        viewname = "wj" + str(i)
        runner.drop_view(viewname)
    
    create_table_tmp()
    runner.drop_view("wj")
    line = "create view wj as select * from wjview" 
    runner.excutesql(line)
    line = "select * from wj order by k1, k2, k3, k4"
    runner.check(line)
    
    line = "create view wj1(k1, k2, k3, k4, k5, k6) as select a.k1, b.k1, a.k3, a.k3,\
		    a.k5, b.k5 from %s as a join wjview as b where a.k1 = b.k1" % (table_name)
    runner.excutesql(line)
    line = "select * from wj1 order by k1, k2, k3, k4"
    runner.check(line)
   
    runner.drop_view("wjview", "table")
    line = "select * from wj order by k1, k2, k3, k4"
    print(line)
    flag = 0
    try:
        palo_result = runner.query_palo.do_sql(line)
    except Exception as e:
        flag = 1
        print(e)
    assert flag == 1
    
    line = "select * from wj1 order by k1, k2, k3, k4" 
    print(line)
    flag = 0
    try:
        palo_result = runner.query_palo.do_sql(line)
    except Exception as e:
        flag = 1
        print(e)
    assert flag == 1

    runner.drop_view()
    runner.drop_view("wj1")


def test_query_create_view_double_tables():
    """
    {
    "title": "test_query_function_more_quick.test_query_create_view_double_tables",
    "describe": "create view on more than two tables",
    "tag": "function,p1"
    }
    """
    """
    create view on more than two tables
    """
    runner.drop_view()
    
    line = "create view wj as select a.k1 as k1, b.k2 as k2, a.k3 as k3, b.k4 as k4,\
		    a.k5 as k5, b.k6 as k6, a.k7 as k7, b.k8 as k8, a.k9 as k9, b.k10 as k10,\
		    a.k11 as k11 from %s as a join %s as b where a.k1 = b.k1" % (table_name, join_name)
    runner.excutesql(line)
    line = "select * from wj order by k1, k2, k3, k4"
    runner.check(line)
    runner.drop_view()
    
    line = "create view wj(k1, k2, k3, k4, k5, k6) as select a.k1, b.k1, a.k3, a.k3,\
		    a.k5, b.k5 from %s as a join %s as b where a.k1 = b.k1" % (table_name, join_name)
    runner.excutesql(line)
    line = "select * from wj order by k1, k2, k3, k4"
    runner.check(line)
    runner.drop_view()
    
    line = "create view wj as select a.k1 as k1, a.k2 * b.k1 as k2, a.k5 as k3, b.k6 as k4\
		    from %s as a join %s as b where a.k1 = b.k1" \
		    % (table_name, join_name)
    runner.excutesql(line)
    line = "select * from wj order by k1, k2, k3, k4"
    runner.check(line)
    runner.drop_view()
    
    line = "create view wj as select count(a.k1) as k1, sum(a.k2) as k2, min(b.k3) as k3\
		    from %s as a join %s as b where a.k1 = b.k1 group by b.k5" \
		    % (table_name, join_name)
    runner.excutesql(line)
    line = "select * from wj order by k1, k2, k3"
    runner.check(line)
    runner.drop_view()

    line = "create view wj(k1, k2, k3, k4, k5, k6) as select a.k1, b.k1, a.k3, a.k3,\
		    a.k5, b.k1+c.k1  from %s as a join %s as b join %s as c where a.k1 = b.k1" \
		    % (table_name, join_name, join_name)
    runner.excutesql(line)
    line = "select * from wj order by k1, k2, k3, k4, k5, k6"
    runner.check(line)
    runner.drop_view()


def test_query_create_view_single_table():
    """
    {
    "title": "test_query_function_more_quick.test_query_create_view_single_table",
    "describe": "query for create view",
    "tag": "function,p1"
    }
    """
    """
    query for create view
    """
    # 1
    runner.drop_view()
    
    # avg(k4) --> avg(k5)
    line = "create view wj as select count(k1) as k1, sum(k2) as k2, \
		    min(k3) as k3, avg(k5) as k4, max(k5) as k5 from %s" % (table_name)
    runner.excutesql(line)
    line = "select * from wj order by k1, k2, k3, k4, k5"
    runner.check(line)
    runner.drop_view()

    line = "create view wj as select count(k1) as k1, sum(k2) as k2, min(k3) as k3, avg(k5) as k4, \
		    max(k5) as k5 from %s group by k10" % (table_name)
    runner.excutesql(line)
    line = "select * from wj order by k1, k2, k3, k4, k5"
    runner.check(line)
    runner.drop_view()
    
    line = "create view wj as select k6 as k1, avg(k8) as k2, min(k8) as k3, max(k8) as k4, \
		    count(k8) as k5, sum(k8) as k7 from %s group by k6 \
		    order by k6, avg(k8)" % (join_name) 
    runner.excutesql(line)
    line = "select * from wj order by k2, k3, k4, k5, k1"
    runner.check(line)
    runner.drop_view()
    
    line = "create view wj as select k2 as a1, avg(k8) as a2, count(k8) as a3, sum(k8) as a4, \
		    min(k8) as a5, max(k8) as a6 from %s group by k2 \
		    order by k2, avg(k8)" % (table_name) 
    runner.excutesql(line)
    line = "select * from wj order by a1, a2, a3, a4, a5, a6"
    runner.check(line)
    runner.drop_view()

    line = "create view wj as select  k6 as k1, avg(k8) as k2, min(k8) as k3, max(k8) as k4, \
		    count(k8) as k5, sum(k8) as k7 from %s group by k6 having k6=\"true\"\
		    order by k6, avg(k8)" % (join_name) 
    runner.excutesql(line)
    line = "select * from wj order by k1, k2, k3, k4, k5, k7"
    runner.check(line)
    runner.drop_view()

    line = "create view wj as select  k2 as a1, avg(k8) as a2, count(k8) as a3, sum(k8) as a4, \
		    min(k8) as a5, max(k8) as a6 from %s group by k2 having k2<=1989 \
		    order by k2, avg(k8)" % (table_name) 
    runner.excutesql(line)
    line = "select * from wj order by a1, a2, a3, a4, a5, a6"
    runner.check(line)
    runner.drop_view()
    
    line = "create view wj as select * from %s" % table_name
    runner.excutesql(line)
    line = "select * from wj order by k1, k2, k3, k4"
    runner.check(line)
    runner.drop_view()
    
    line = "create view wj as \
		    select k1+100 as k1, k2-2000 as k2, k3+30000 as k3, k4 as k4, \
		    k5-k8 as k5, concat(k6, space(65535), \"\") as k6, concat(k7, space(65535), \"wj\")  as k7, k8 as k8, k9 as k9, k10 as k10, k11 as k11 \
		    from %s" % join_name
    runner.excutesql(line)
    line = "select * from wj order by k1, k2, k3, k4"
    runner.check(line)
    runner.drop_view()


def test_query_597_bieming():
    """
    {
    "title": "test_query_function_more_quick.test_query_597_bieming",
    "describe": "test column like "select" "desc"",
    "tag": "function,p1"
    }
    """
    """
    test column like "select" "desc"
    """

    sql = "drop table if exists test597"
    runner.mysql_cursor.execute(sql)
    runner.query_palo.do_sql(sql)

    create_table_sql_palo = "create table test597(\
		    k1 tinyint, `desc` smallint, `select` int, `from` bigint, k5 decimal(9,3), k6 char(5), \
		    k10 date, k11 datetime, k7 varchar(20), k8 double max, k9 float sum\
		    ) \
		    engine=olap distributed by hash(k1) buckets 5 \
		    properties(\"storage_type\"=\"column\")" 
    create_table_result = runner.query_palo.do_sql(create_table_sql_palo)
    assert create_table_result == ()

    create_table_sql_mysql = " CREATE TABLE test597( \
		    k1 tinyint(4), `desc` smallint(6), `select` int(11), `from` bigint(20), k5 decimal(9,3),\
		    k6 char(5), k10 date, k11 datetime, k7 varchar(20), k8 double, k9 float)"
    runner.mysql_cursor.execute(create_table_sql_mysql)

    line = "insert into test597 select * from baseall"
    insert_table_result = runner.query_palo.do_sql(line)
    assert insert_table_result == () 
    runner.mysql_cursor.execute(line)

    time.sleep(30)
    runner.wait_end()

    line = "select k1, `desc`, `select`, `from` from test597 order by k1, `desc`, `select`, `from`"
    runner.check(line)

    sql = "drop table if exists test597"
    runner.mysql_cursor.execute(sql)
    runner.query_palo.do_sql(sql)


def test_query_table_daxiaoxie():
    """
    {
    "title": "test_query_function_more_quick.test_query_table_daxiaoxie",
    "describe": "est table name daxiaoxie",
    "tag": "function,p1,fuzz"
    }
    """
    """
    test table name daxiaoxie
    """
    runner.drop_view("baseALL", "table")

    create_table_sql_palo = "create table baseALL(\
		    k1 tinyint, k2 smallint, k3 int, k4 bigint, k5 decimal(9,3), k6 char(5), \
		    k10 date, k11 datetime, k7 varchar(20), k8 double max, k9 float sum\
		    ) \
		    engine=olap distributed by hash(k1) buckets 5 \
		    properties(\"storage_type\"=\"column\")" 
    create_table_result = runner.query_palo.do_sql(create_table_sql_palo)
    assert create_table_result == ()

    create_table_sql_mysql = " CREATE TABLE baseALL( \
		    k1 tinyint(4), k2 smallint(6), k3 int(11), k4 bigint(20), k5 decimal(9,3),\
		    k6 char(5), k10 date, k11 datetime, k7 varchar(20), k8 double, k9 float)"
    runner.mysql_cursor.execute(create_table_sql_mysql)

    line = "insert into baseALL select * from test"
    insert_table_result = runner.query_palo.do_sql(line)
    assert insert_table_result == () 
    runner.mysql_cursor.execute(line)

    time.sleep(30)
    runner.wait_end()

    line = "select * from baseALL order by k1, k2, k3, k4"
    runner.check(line)
    line = "select * from baseall order by k1, k2, k3, k4"
    runner.check(line)
    line = "select baseALL.*, baseall.* from baseALL join baseall where baseALL.k1 = baseall.k2"

    line = "select * from baseaLL"
    runner.checkwrong(line)

    runner.drop_view("baseALL", "table")


def test_query_database_daxiaoxie():
    """
    {
    "title": "test_query_function_more_quick.test_query_database_daxiaoxie",
    "describe": "test db name daxiaoxie",
    "tag": "function,p1,fuzz"
    }
    """
    """
    test db name daxiaoxie
    """
    runner.drop_view("test_query_QA", "database")

    line = "create database test_query_QA"
    
    create_table_result = runner.query_palo.do_sql(line)
    assert create_table_result == ()
    runner.mysql_cursor.execute(line)

    line = "use test_query_QA"
    runner.checkok(line)

    line = "select * from test_query_Qa.baseall order by k1, k2, k3, k4"
    runner.checkwrong(line)
    line = "select * from test_query_QA.baseall order by k1, k2, k3, k4"
    runner.checkwrong(line)

    runner.drop_view("test_query_QA", "database")

    line = "use %s" % runner.query_db
    runner.query_palo.do_sql(line)
    line = "use %s" % (runner.mysql_db)
    runner.mysql_cursor.execute(line)


def create_partition_table(partition_column, parition_name_list, partition_value_list):
    """
    create partition table
    """
    num = 0
    palo_partition_info = "("
    mysql_partition_info = "("
    flag = 0
    for item in parition_name_list:
        if flag == 1:
            palo_partition_info += ","
            mysql_partition_info += ","
        if (partition_value_list[num] == "MAXVALUE"):
            palo_partition_info += "partition %s values less than %s" \
			    % (item, partition_value_list[num])
            mysql_partition_info += "partition %s values less than %s" \
			    % (item, partition_value_list[num])
        else:
            palo_partition_info += "partition %s values less than (\"%s\")" \
			    % (item, partition_value_list[num])
            mysql_partition_info += "partition %s values less than (%s)" \
			    % (item, partition_value_list[num])
        num += 1
        flag = 1
    palo_partition_info += ")"
    mysql_partition_info += ")"
    create_table_sql_palo = "create table test_partition(\
		    k1 tinyint, k2 smallint, k3 int, k4 bigint, k5 decimal(9,3), k6 char(5), \
		    k10 date, k11 datetime, k7 varchar(20), k8 double max, k9 float sum\
		    ) engine=olap partition by range(%s) %s\
		    distributed by hash(k1) buckets 5" % (partition_column, palo_partition_info)
    print(create_table_sql_palo)
    '''
    create_table_sql_mysql = " CREATE TABLE test_partition( \
		    k1 tinyint(4), k2 smallint(6), k3 int(11), k4 bigint(20), k5 decimal(9,3),\
		    k6 char(5), k10 date, k11 datetime, k7 varchar(20), k8 double, k9 float)\
		    partition by range(%s) %s" % (partition_column, mysql_partition_info)
    '''
    create_table_sql_mysql = " CREATE TABLE test_partition( \
		    k1 tinyint(4), k2 smallint(6), k3 int(11), k4 bigint(20), k5 decimal(9,3),\
		    k6 char(5), k10 date, k11 datetime, k7 varchar(20), k8 double, k9 float)"
    print(create_table_sql_mysql)
    create_table_result = runner.query_palo.do_sql(create_table_sql_palo)
    assert create_table_result == ()
    runner.mysql_cursor.execute(create_table_sql_mysql)

    line = "insert into test_partition select * from baseall"
    print(line)
    insert_table_result = runner.query_palo.do_sql(line)
    assert insert_table_result == () 
    runner.mysql_cursor.execute(line)
    runner.wait_end()
    time.sleep(30)


def test_query_parition_tinyint():
    """
    {
    "title": "test_query_function_more_quick.test_query_parition_tinyint",
    "describe": "partition tinyint",
    "tag": "function,p1"
    }
    """
    """
    partition tinyint
    """
    runner.drop_view("test_partition", "table")
    parition_name_list = ["p1", "p2", "p3", "p4", "p5"]
    partition_value_list = ["-50", "0", "10", "13", "MAXVALUE"]

    create_partition_table("k1", parition_name_list, partition_value_list)

    line1 = "select * from test_partition  partition (p1, p3) order by k1, k2, k3, k4"
    line2 = "select * from test_partition where k1<-50 or (k1 >=0 and k1 < 10) \
		    order by k1, k2, k3, k4"
    runner.check2(line1, line2)
    line1 = "select * from test_partition  partition (p5) order by k1, k2, k3, k4"
    line2 = "select * from test_partition where k1>=13 order by k1, k2, k3, k4"
    runner.check2(line1, line2)
    line1 = "select * from test_partition  partition (p5, p4, p3, p2, p1) \
		    order by k1, k2, k3, k4"
    line2 = "select * from test_partition order by k1, k2, k3, k4"
    runner.check2(line1, line2)
    '''
    branch ok trunk bug
    line = "select * from test_partition where 10 <= k1 order by k1, k2, k3, k4" 
    runner.check(line)
    line = "select * from test_partition where -1 < k1 order by k1, k2, k3, k4" 
    runner.check(line)
    '''
    line = "select * from test_partition where k1 in (-2, 11, 12, 13, 14) order by k1, k2, k3, k4"
    runner.check(line)
    line = "select * from test_partition where k1 not in (-2, 11, 12, 13, 14) \
		    order by k1, k2, k3, k4"
    runner.check(line)
    runner.drop_view("test_partition", "table")


def test_query_parition_smallint():
    """
    {
    "title": "test_query_function_more_quick.test_query_parition_smallint",
    "describe": "partition smallint",
    "tag": "function,p1"
    }
    """
    """
    partition smallint
    """
    runner.drop_view("test_partition", "table")
    parition_name_list = ["p1", "p2", "p3", "p4", "p5"]
    partition_value_list = ["-50", "0", "10", "80", "MAXVALUE"]
 
    create_partition_table("k2", parition_name_list, partition_value_list)
 
    line1 = "select * from test_partition  partition (p1, p3) order by k1, k2, k3, k4"
    line2 = "select * from test_partition where k2<-50 or (k2 >=0 and k2 < 10) \
		    order by k1, k2, k3, k4"
    runner.check2(line1, line2)
    line1 = "select * from test_partition  partition (p2, p4, p5) order by k1, k2, k3, k4"
    line2 = "select * from test_partition where k2>=10 or (k2 >=-50 and k2 < 0) \
		    order by k1, k2, k3, k4"
    runner.check2(line1, line2)
    '''
    branch ok trunk bug
    line = "select * from test_partition where 10 <= k2 order by k1, k2, k3, k4" 
    runner.check(line)
    line = "select * from test_partition where -1 < k2 order by k1, k2, k3, k4" 
    runner.check(line)
    '''
    line = "select * from test_partition where k2 in (-2, 11, 12, 13, 14) order by k1, k2, k3, k4"
    runner.check(line)
    line = "select * from test_partition where k2 not in (-2, 11, 12, 13, 14) \
		    order by k1, k2, k3, k4"
    runner.check(line)

    runner.drop_view("test_partition", "table")


def test_query_parition_int():
    """
    {
    "title": "test_query_function_more_quick.test_query_parition_int",
    "describe": "partition init",
    "tag": "function,p1"
    }
    """
    """
    partition int
    """
    runner.drop_view("test_partition", "table")
    parition_name_list = ["p1", "p2", "p3", "p4", "p5"]
    partition_value_list = ["-50", "0", "10", "80", "MAXVALUE"]

    create_partition_table("k3", parition_name_list, partition_value_list)

    line1 = "select * from test_partition  partition (p1, p3) order by k1, k2, k3, k4"
    line2 = "select * from test_partition where k3<-50 or (k3 >=0 and k3 < 10) \
		    order by k1, k2, k3, k4"
    runner.check2(line1, line2)
    line1 = "select * from test_partition  partition (p2, p4, p5) order by k1, k2, k3, k4"
    line2 = "select * from test_partition where k3>=10 or (k3 >=-50 and k3 < 0) \
		    order by k1, k2, k3, k4"
    runner.check2(line1, line2)
    '''
    branch ok trunk bug
    line = "select * from test_partition where 10 <= k3 order by k1, k2, k3, k4" 
    runner.check(line)
    line = "select * from test_partition where -1 < k3 order by k1, k2, k3, k4" 
    runner.check(line)
    '''
    line = "select * from test_partition where k3 in (-2, 11, 12, 13, 14) order by k1, k2, k3, k4"
    runner.check(line)
    line = "select * from test_partition where k3 not in (-2, 11, 12, 13, 14) \
		    order by k1, k2, k3, k4"
    runner.check(line)
    runner.drop_view("test_partition", "table")


def test_query_parition_bigint():
    """
    {
    "title": "test_query_function_more_quick.test_query_parition_bigint",
    "describe": "partition bigint",
    "tag": "function,p1"
    }
    """
    """
    partition bigint
    """
    runner.drop_view("test_partition", "table")
    parition_name_list = ["p1", "p2", "p3", "p4", "p5"]
    partition_value_list = ["-50", "0", "10", "80", "MAXVALUE"]

    create_partition_table("k4", parition_name_list, partition_value_list)

    line1 = "select * from test_partition partition (p1, p3) where k6='true' \
		    order by k1, k2, k3, k4"
    line2 = "select * from test_partition where (k4<-50 or (k4 >=0 and k4 < 10)) and k6='true' \
		    order by k1, k2, k3, k4"
    runner.check2(line1, line2)
    line1 = "select * from test_partition  partition (p1, p3) order by k1, k2, k3, k4"
    line2 = "select * from test_partition where k4<-50 or (k4 >=0 and k4 < 10) \
		    order by k1, k2, k3, k4"
    runner.check2(line1, line2)
    line1 = "select * from test_partition  partition (p2, p4, p5) order by k1, k2, k3, k4"
    line2 = "select * from test_partition where k4>=10 or (k4 >=-50 and k4 < 0) \
		    order by k1, k2, k3, k4"
    runner.check2(line1, line2)
    '''
    branch ok trunk bug
    line = "select * from test_partition where 10 <= k4 order by k1, k2, k3, k4" 
    runner.check(line)
    line = "select * from test_partition where -1 < k4 order by k1, k2, k3, k4" 
    runner.check(line)
    '''
    line = "select * from test_partition where k4 in (-2, 11, 12, 13, 14) order by k1, k2, k3, k4"
    runner.check(line)
    line = "select * from test_partition where k4 not in (-2, 11, 12, 13, 14) \
		    order by k1, k2, k3, k4"
    runner.check(line)
    runner.drop_view("test_partition", "table")


def test_query_parition_date_only():
    """
    {
    "title": "test_query_function_more_quick.test_query_parition_date_only",
    "describe": "partition date",
    "tag": "function,p1"
    }
    """
    """
    partition date
    """
    runner.drop_view("test_partition", "table")
    parition_name_list = ["p1", "p2", "p3", "p4", "p5"]
    partition_value_list = ["1989-03-21", "1991-08-11", "2014-11-11", "3124-10-10", "MAXVALUE"]

    create_partition_table("k10", parition_name_list, partition_value_list)

    line1 = "select * from test_partition  partition (p1, p3) order by k1, k2, k3, k4"
    line2 = "select * from test_partition \
		    where k10<'1989-03-21' or (k10 >='1991-08-11' and k10 < '2014-11-11') \
		    order by k1, k2, k3, k4"
    runner.check2(line1, line2)
    line1 = "select k6, sum(k1) as wj from test_partition  partition (p2, p4, p5) \
		    group by k6 order by k6, wj"
    line2 = "select k6, sum(k1) as wj from test_partition \
 		   where k10>='2014-11-11' or (k10>='1989-03-21' and k10 < '1991-08-11') \
		   group by k6 order by k6, wj"
    runner.check2(line1, line2)
    line1 = "select * from test_partition  partition (p2, p4, p5) order by k1, k2, k3, k4"
    line2 = "select * from test_partition \
		    where k10>='2014-11-11' or (k10>='1989-03-21' and k10 < '1991-08-11') \
		    order by k1, k2, k3, k4"
    runner.check2(line1, line2)
    '''
    branch ok trunk bug
    line = "select * from test_partition where '2014-11-11' <= k10 order by k1, k2, k3, k4" 
    runner.check(line)
    line = "select * from test_partition where '2013-11-11' < k10 order by k1, k2, k3, k4" 
    runner.check(line)
    '''
    line = "select * from test_partition where k10 in ('2013-11-11') order by k1, k2, k3, k4"
    runner.check(line)
    line = "select * from test_partition where k10 not in ('2013-11-11') order by k1, k2, k3, k4"
    runner.check(line)
    runner.drop_view("test_partition", "table")  


def test_query_parition_datetime():
    """
    {
    "title": "test_query_function_more_quick.test_query_parition_datetime",
    "describe": "partition int",
    "tag": "function,p1"
    }
    """
    """
    partition int
    """
    runner.drop_view("test_partition", "table")
    parition_name_list = ["p1", "p2", "p3", "p4", "p5"]
    partition_value_list = ["1989-03-21 13:00:01", "1999-01-01 00:00:00", \
		    "2013-04-02 15:16:52", "9999-11-11 12:12:00", "MAXVALUE"]

    create_partition_table("k11", parition_name_list, partition_value_list)

    line1 = "select * from test_partition  partition (p1, p3) order by k1, k2, k3, k4"
    line2 = "select * from test_partition \
		    where k11<'1989-03-21 13:00:01' or (k11 >='1999-01-01 00:00:00' and k11 < '2013-04-02 15:16:52') \
		    order by k1, k2, k3, k4"
    runner.check2(line1, line2)
    line1 = "select * from test_partition  partition (p2, p4, p5) order by k1, k2, k3, k4"
    line2 = "select * from test_partition \
		    where k11>='2013-04-02 15:16:52' or (k11>='1989-03-21 13:00:01' and k11 < '1999-01-01 00:00:00') \
		    order by k1, k2, k3, k4"
    runner.check2(line1, line2)
    line1 = "select * from test_partition  partition (p2) order by k1, k2, k3, k4"
    line2 = "select * from test_partition \
		    where (k11>='1989-03-21 13:00:01' and k11 < '1999-01-01 00:00:00') order by k1, k2, k3, k4"
    runner.check2(line1, line2)
    '''
    branch ok trunk bug
    line = "select * from test_partition where '2013-04-02 15:16:52' <= k11 order by k1, k2, k3, k4" 
    runner.check(line)
    line = "select * from test_partition where ''2010-03-21 13:00:00 < k11 order by k1, k2, k3, k4" 
    runner.check(line)
    '''
    line = "select * from test_partition where k11 in ('2013-11-11') order by k1, k2, k3, k4"
    runner.check(line)
    line = "select * from test_partition where k11 not in ('2013-11-11') order by k1, k2, k3, k4"
    runner.check(line)
    runner.drop_view("test_partition", "table")  


def test_query_join_empty():
    """
    {
    "title": "test_query_function_more_quick.test_query_join_empty",
    "describe": "join empty",
    "tag": "function,p1"
    }
    """
    """
    join empty
    """
    runner.drop_view("test_join", "table")
    
    create_table_sql_palo = "create table test_join(\
		    k1 tinyint not null, k2 smallint not null, k3 int not null, k4 bigint not null, k5 decimal(9,3) not null, k6 char(5) not null, \
		    k10 date not null, k11 datetime not null, k7 varchar(20) not null, k8 double max not null, k9 float sum not null\
		    ) \
		    engine=olap distributed by hash(k1) buckets 5 \
		    properties(\"storage_type\"=\"column\")" 
    print(create_table_sql_palo)
    create_table_result = runner.query_palo.do_sql(create_table_sql_palo)
    assert create_table_result == ()

    create_table_sql_mysql = " CREATE TABLE test_join( \
		    k1 tinyint(4), k2 smallint(6), k3 int(11), k4 bigint(20), k5 decimal(9,3),\
		    k6 char(5), k10 date, k11 datetime, k7 varchar(20), k8 double, k9 float)"
    runner.mysql_cursor.execute(create_table_sql_mysql)
    
    line = "select * from baseall join test_join"
    
    runner.drop_view("test_join", "table")


def test_query_join_mysql_table():
    """
    {
    "title": "test_query_function_more_quick.test_query_join_mysql_table",
    "describe": "test_query_function_more_quick.test_query_join_mysql_table",
    "tag": "function,p1"
    }
    """
    """
    join mysql table
    """
    global mysql_host, mysql_port, mysql_user, mysql_passwd, mysql_db
    runner.drop_view("test_join", "table")
    # mysql engine table cannot set aggregation type
    create_table_sql_palo = "create table test_join(\
		    k1 tinyint, k2 smallint, k3 int, k4 bigint, k5 decimal(9,3), k6 char(5), \
		    k10 date, k11 datetime, k7 varchar(20), k8 double, k9 float\
		    ) engine=mysql properties (\
		    \"host\" = \"%s\",\
		    \"port\" = \"%d\",\
                    \"user\" = \"%s\",\
                    \"password\" = \"%s\",\
                    \"database\" = \"%s\",\
                    \"table\" = \"baseall\"\
		    )" % (runner.mysql_host, runner.mysql_port, runner.mysql_user, 
                          runner.mysql_passwd, runner.mysql_db)
    print(create_table_sql_palo)
    create_table_result = runner.query_palo.do_sql(create_table_sql_palo)
    assert create_table_result == ()
   
    line1 = "select A.k1,B.k1 from %s as A join %s as B where A.k1=B.k1+1 \
		   order by A.k1, A.k2, A.k3, A.k4" % ("test_join", "test")
    line2 = "select A.k1,B.k1 from %s as A join %s as B where A.k1=B.k1+1 \
		   order by A.k1, A.k2, A.k3, A.k4" % ("baseall", "test")

    runner.check2(line1, line2)

    runner.drop_view("test_join", "table")


def test_select_for_create_view():
    """
    {
    "title": "test_query_function_more_quick.test_select_for_create_view",
    "describe": "test select for create view",
    "tag": "function,p1"
    }
    """
    """
    test select for create view
    """
    runner.drop_view("wj1", "view")
    line = "create view wj1 as select * from (select distinct k1 from %s)t;" % (table_name)
    runner.checkok(line)
    line1 = "select * from wj1 order by k1"
    line2 = "select * from (select distinct k1 from %s)t order by k1;" % (table_name)
    runner.check2(line1, line2)
    runner.drop_view("wj1", "view")

    runner.drop_view("wj2", "view")
    line = "create view wj2 as select * from (select sum(k1) as c1 from %s group by k1)t;" \
            %(table_name)
    runner.checkok(line)
    line1 = "select * from wj2 order by c1;"
    line2 = "select * from (select sum(k1) as c1 from %s group by k1)t order by c1" % (table_name)
    runner.check2(line1, line2)
    runner.drop_view("wj2", "view")

    runner.drop_view("wj3", "view")
    line = "create view wj3 as select * from (select (k1+3) as c1, k2*2 as c2, k3-100 as c3, \
            k8/2 as c4 from %s) t " % (table_name)
    runner.checkok(line)
    line1 = "select * from wj3 order by c1,c2,c3,c4 "
    line2 = "select * from (select (k1+3) as c1, k2*2 as c2, k3-100 as c3, \
            k8/2 as c4 from %s) t order by c1, c2, c3, c4" %(table_name)
    runner.check2(line1, line2)
    runner.drop_view("wj3", "view")
    line = 'drop view if exists test_view'
    runner.checkok(line)
    runner.mysql_cursor.execute(line)
    line = 'create view test_view as select * from test where k1 = 1'
    runner.checkok(line)
    runner.mysql_cursor.execute(line)
    line = 'select * from baseall left join test_view on' \
            ' baseall.k1 = test_view.k1 order by baseall.k1, ' \
            'baseall.k2, baseall.k3, baseall.k4, test_view.k1, ' \
            'test_view.k2, test_view.k3, test_view.k4'
    runner.check2(line, line)
    line = 'select * from baseall where 1 order by k1, k2, k3, k4;'
    runner.check2(line, line)
    runner.drop_view("z", "view")
    line = 'create view z(k1, k2, k3,k4,k5,k6,k7,k8,k9,k10,k11) as select * from %s' % join_name
    runner.checkok(line)
    runner.drop_view("z", "view")


def test_view_with_join_predicate():
    """
    {
    "title": "test_query_function_more_quick.test_view_with_join_predicate",
    "describe": "for bug https://github.com/apache/incubator-doris/issues/645",
    "tag": "function,p1"
    }
    """
    """for bug https://github.com/apache/incubator-doris/issues/645"""
    runner.drop_view("v2", "view")
    sql = 'create view v2 as (select t1.k1 from baseall t1 left outer join baseall as t11 on t1.k1 = t11.k1)'
    runner.init(sql)
    sql = 'select count(*) from (select 1 as nn from v2 union all select 2 as nn from v2) a where 1=2'
    result = runner.check(sql)
    runner.drop_view("v2", "view")


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
    test_query_create_view_many_times()
