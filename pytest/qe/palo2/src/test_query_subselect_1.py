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


def test_query_subquery_simple():
    """
    {
    "title": "test_query_select.test_query_subquery_simple",
    "describe": "test simple subquery",
    "tag": "function,p0"
    }
    """
    """
    test simple subquery
    """
    line = "select c1, c3, m2 from ( select c1, c3, max(c2) m2 from (select c1, c2, c3 from (\
		    select k3 c1, k2 c2, max(k1) c3 from %s group by 1, 2 order by 1 desc, 2 desc limit 5) x ) x2\
		    group by c1, c3 limit 10) t where c1>0 order by 2 , 1 limit 3" % (table_name)
    runner.check(line)		   
    line = "select c1, c2 from (select k3 c1, k1 c2, min(k8) c3 from %s group by 1, 2) x\
		    order by 1, 2" % (table_name)
    runner.check(line)


def test_query_subquery_in():
    """
    {
    "title": "test_query_select.test_query_subquery_in",
    "describe": "subquery query with in",
    "tag": "function,p0"
    }
    """
    """
    subquery query with in
    """
    line = "select * from %s where k2 in (select distinct k1 from %s) and k4>0 and \
    		    round(k5) in (select k2 from %s) order by k1, k2, k3, k4" % (table_name, join_name, join_name)
    runner.check(line)
    line = "select sum(k1), k2 from %s where k2 in (select distinct k1 from %s) group by k2 \
		    order by sum(k1), k2" % (table_name, join_name)
    runner.check(line)
    for i in range(1, 5):
        line = "select * from %s where k%s in (select k%s from %s) order by k1, k2, k3, k4" \
	    	    % (join_name, i, i, table_name)
        runner.check(line)
        line = "select * from %s where k%s in (select k%s from %s) order by k1, k2, k3, k4" \
		    % (join_name, i, i, join_name)
        runner.check(line)
        line = "select * from %s where k%s not in (select k%s from %s) order by k1, k2, k3, k4" \
	    	    % (join_name, i, i, table_name)
        runner.check(line)
        line = "select * from %s where k%s not in (select k%s from %s) order by k1, k2, k3, k4" \
		    % (join_name, i, i, join_name)
        runner.check(line)
    line = "select * from %s where k6 in (select k7 from %s) order by k1, k2, k3, k4" \
	    	    % (join_name, join_name)
    runner.check(line)
    line = "select * from %s where k6  not in (select k7 from %s) order by k1, k2, k3, k4" \
	    	    % (join_name, join_name)
    runner.check(line)
    line = "select * from %s where k7 in (select k6 from %s) order by k1, k2, k3, k4" \
	    	    % (join_name, join_name)
    runner.check(line)
    line = "select * from %s where k7 not in (select k6 from %s) order by k1, k2, k3, k4" \
	    	    % (join_name, join_name)
    runner.check(line)
    line = "select * from %s where k8 in (select abs(k8) from %s where k8<>0) \
		    order by k1, k2, k3, k4" % (join_name, join_name)
    runner.check(line)
    line = "select * from %s where k8 not in (select abs(k8) from %s where k8<>0 order by 1) \
		    order by k1, k2, k3, k4" % (join_name, join_name)
    runner.check(line)
    line = "select * from %s where k9 in (select abs(k9) from %s) order by k1, k2, k3, k4" \
	    	    % (join_name, join_name)
    runner.check(line)
    line = "select * from %s where k9 not in (select abs(k9) from %s) order by k1, k2, k3, k4" \
	    	    % (join_name, join_name)
    runner.check(line)
    line = "select * from %s where k10 in (select k10 from %s) order by k1, k2, k3, k4" \
	    	    % (join_name, table_name)
    runner.check(line)
    line = "select * from %s where k10 not in (select k10 from %s) order by k1, k2, k3, k4" \
	    	    % (join_name, table_name)
    runner.check(line)
    line = "select * from %s where k11 in (select k11 from %s) order by k1, k2, k3, k4" \
	    	    % (join_name, table_name)
    runner.check(line)
    line = "select * from %s where k11 in (\"1957-04-01\",\"1957-05-01\",NULL) order by k1, k2, k3, k4" \
                % join_name
    runner.check(line)
    line = "select * from %s where k11 not in (select k11 from %s) order by k1, k2, k3, k4" \
	    	    % (join_name, table_name)
    runner.check(line)
    
    line = "select * from %s where k1 in \
		    (select k1 from %s where k1=0 and k1<>0) order by k1, k2, k3, k4" \
	    	    % (join_name, join_name)
    runner.check(line)


def test_query_subquery_exist():
    """
    {
    "title": "test_query_select.test_query_subquery_exist",
    "describe": "subquery query with exist",
    "tag": "function,p0,fuzz"
    }
    """
    """
    subquery query with exist
    """
    for i in range(1, 4):
        line = "select * from %s as a where \
			exists (select * from %s as b where a.k%s = b.k2) order by k1, k2, k3, k4" \
	    	    % (join_name, table_name, i)
        runner.check(line)
        line = "select * from %s as a where \
			exists (select * from %s as b where a.k%s = b.k2) order by k1, k2, k3, k4" \
		    % (join_name, join_name, i)
        runner.check(line)
    line = "select * from %s as a where \
		    exists (select * from %s as b where round(a.k5) = b.k2) order by k1, k2, k3, k4" \
	    	    % (join_name, join_name)
    runner.check(line)
    line = "select * from %s as a where \
		    exists (select * from %s as b where a.k6 = b.k6 and b.k6=\"true\") order by k1, k2, k3, k4" \
	    	    % (join_name, join_name)
    runner.check(line)
    # do not support: Unsupported predicate with subquery: EXISTS, exists的相关子查询不支持
    line = "select * from %s as a where \
		    exists (select * from %s as b where a.k7 = concat(b.k6, b.k6, b.k6, b.k6)) order by k1, k2, k3, k4" \
	    	    % (table_name, table_name)
    runner.checkwrong(line)
    line = "select * from %s as a where \
		    exists (select * from %s as b where round(a.k8) = b.k5) order by k1, k2, k3, k4" \
	    	    % (join_name, table_name)
    runner.check(line)
    line = "select * from %s as a where \
		    exists (select * from %s as b where a.k9 = b.k8) order by k1, k2, k3, k4" \
	    	    % (join_name, table_name)
    runner.check(line)
    line = "select * from %s as a where \
		    exists (select * from %s as b where a.k10 = b.k11) order by k1, k2, k3, k4" \
	    	    % (join_name, table_name)
    runner.check(line)
    line = "select * from %s as a where exists (select * from %s as b where a.k10 = DATE_SUB(k11, INTERVAL 1 YEAR)) \
		    order by k1, k2, k3, k4" \
	    	    % (join_name, table_name)
    runner.check(line)
    
    line = "select * from %s as a where \
		    exists (select * from %s as b where a.k1=b.k5 and a.k2=b.k1) order by k1, k2, k3, k4" \
	    	    % (join_name, join_name)
    runner.check(line)
    # todo bug
    line = "select count(distinct k1, k2) from test a where" \
           "exists(select * from test b where a.k1=b.k1 and a.k2 <> b.k2)"
    # runner.check(line)


def test_subquery_typedata():
    """
    {
    "title": "test_query_select.test_subquery_typedata",
    "describe": "different types in comparion between the subquery and out query",
    "tag": "function,p0"
    }
    """
    """
    different types in comparion between the subquery and out query
    """
    for i in (2, 3, 4, 5, 8, 9):
        for j in (1, 2, 3, 4, 5, 8, 9):
            line = "select k%s from %s where k%s < (select max(k%s) from %s) order by k%s" \
            % (i,table_name,i,j,join_name,i)
            runner.check(line)    
             
            line = "select a.k%s from %s as a where a.k%s < (select max(b.k%s) from %s as b \
            		where a.k%s = b.k1) order by k%s" % (i,join_name,i,j,join_name,i,i)
            runner.check(line)

    for i in (10,11):
        for j in (10,11):
            line = "select k%s from %s where k%s < (select max(k%s) from %s) \
            		order by k%s" % (i,table_name,i,j,join_name,i)
            runner.check(line)

   
def test_subquery_compute():
    """
    {
    "title": "test_query_select.test_subquery_compute",
    "describe": "test for calculation with subquery result",
    "tag": "function,p0"
    }
    """
    """
    test for calculation with subquery result
    """
    for i in (1, 2, 3, 4, 5, 8, 9):
        for j in(1, 2, 3, 4, 5, 8, 9):
            for k in ("+", "-", "*", "/"):
                line = "select k%s from %s where k%s < (select min(k%s) from %s) %s 100.00123 \
                		order by k%s" % (i,table_name,i,j,join_name,k,i)
                runner.check(line)
                
    for k in ("+", "-", "*", "/"):
        line = "select k1 from %s where k1 > 100.000 %s (select min(k2) from %s \
        		where k1 = 16) order by k1;" % (table_name,k,join_name)
        runner.check(line)
    """
    line = "select k1 from %s where k1 > 100 / (select min(k1)-1 from %s)" % (table_name,join_name)
    runner.check(line)
    """


def test_subquery_aggregate():
    """
    {
    "title": "test_subquery_aggregate",
    "describe": "test for the aggregation function min max sum avg count",
    "tag": "function,p0"
    }
    """
    """
    test for the aggregation function min max sum avg count
    """
    line = "select k4 from %s where k1 < (select min(k1) from %s where k2 < k3) \
            order by k4" % (table_name,table_name)
    runner.check(line)
    line = "select k4 from %s where k1 < (select max(k1) from %s where k2 < k3) \
            order by k4" % (table_name,table_name)
    runner.check(line)
    line = "select k4 from %s where k1 < (select sum(k1) from %s where k2 < k3) \
            order by k4" % (table_name,table_name)
    runner.check(line)
    line = "select k4 from %s where k1 < (select sum(distinct k1) from %s where k2 < k3) \
            order by k4" % (table_name,table_name)
    runner.check(line)
    line = "select k4 from %s where k1 < (select count(k1) from %s where k2 < k3) \
            order by k4" % (table_name,table_name)
    runner.check(line)
    line = "select k4 from %s where k1 < (select count(distinct k1) from %s where k2 < k3) \
           order by k4" % (table_name,table_name)
    runner.check(line)
    line = "select k4 from %s where k1 < (select avg(k1) from %s where k2 < k3) \
            order by k4" % (table_name,table_name)
    runner.check(line)
    line = "select k4 from %s where k1 < (select avg(distinct k1) from %s where k2 < k3) \
            order by k4" % (table_name,table_name)
    runner.check(line)


def test_subquery_outeroperater():
    """
    {
    "title": "test_query_select.test_subquery_outeroperater",
    "describe": "uncorrelated subquery with different comparision",
    "tag": "function,p0"
    }
    """
    """
    uncorrelated subquery with different comparision
    """
    for k in ("<", ">", "<=", ">=", "=", "!=", "<>"):
        line = "select k1 from %s where k2 %s (select max(k2) from %s where k3 < k2) order by k1" \
                % (table_name,k,table_name)
        runner.check(line)
        line = "select k1 from %s where k2 %s (select avg(k2) from %s where k3 < k2) order by k1" \
               % (table_name,k,table_name)
        runner.check(line)
        line = "select k1 from %s where k2 %s (select count(k2) from %s where k3 < k2) order by k1" \
               % (table_name,k,table_name)
        runner.check(line)
        line = "select k1 from %s where k2 %s (select sum(k2) from %s where k3 < k2) order by k1" \
               % (table_name,k,table_name)
        runner.check(line)
        line = "select k1 from %s where k2 %s (select min(k2) from %s where k3 < k2) order by k1" \
               % (table_name,k,table_name)
        runner.check(line)


def test_subquery_function():
    """
    {
    "title": "test_query_select.test_subquery_function",
    "describe": "the function result compared with subquery result,and the subqury result with function",
    "tag": "function,p0"
    }
    """
    """
    the function result compared with subquery result,and the subqury result with function
    """
    line = "select k1,k7 from %s where k7 like (select Upper(k6) from %s limit 1)  order by k1,k7" \
            % (table_name,table_name)
    runner.check(line)
    line = "select k1 from %s where length(k6) > (select avg(k2) from %s where length(7) > k1) order by k1" \
            % (table_name,table_name)
    runner.check(line)
    line = "select k6 from %s where day(k11) >= (select max(day(k11)) from %s where month(k10) = month(k11)) order by lower(k6)" \
            % (table_name,table_name)
    runner.check(line)
    line = "select distinct k6 from %s where day(k11) > (select avg(day(k11)) from %s \
            where month(k10) = month(k11) and year(k10) = 2015) order by lower(k6) limit 5" % (table_name,table_name)
    runner.check(line)


def test_subquery_complicate():
    """
    {
    "title": "test_query_select.test_subquery_complicate",
    "describe": "the complicated subquery",
    "tag": "function,p0"
    }
    """
    """
    the complicated subquery    
    """
    line = "select t1.* from %s t1 where (select count(*) from %s t3,%s t2 where\
            t3.k2=t2.k2 and t2.k1='1') > 0 order by k1,k2,k3,k4" % (table_name,join_name,join_name)
    runner.check(line)
  
    line = " select k6 from %s where k1 = (select max(t1.k1) from %s t1 join %s t2 on t1.k1=t2.k1) order by k2" \
            % (table_name,table_name,join_name)
    runner.check(line)
    line = "select k2,k6,k7,k10 from %s where k1 = 4 union (select k2,k6,k7,k10 from %s where k2=5) order by k2,k6" \
            % (table_name,table_name)
    runner.check(line)
    
    line = "select * from %s where k1 in (select max(k1) from %s  where 0<(select max(k9) from %s\
            where 0<(select max(k8) from %s  where 0<(select max(k5) from %s\
            where 0<(select max(k4) from %s  where 0<(select sum(k4) from %s\
            where 0<(select max(k3) from %s  where 0 < (select max(k2) from %s\
            where 0<(select max(k1) from %s  where -12<(select min(k1) from %s\
            where 10 > (select avg(k1) from %s  where 5 > (select sum(k1) from %s)))))))))) ))"\
            % (table_name,table_name,table_name,table_name,table_name,table_name,table_name,\
                table_name,table_name,table_name,table_name,table_name,table_name)
    runner.check(line)
    temp_name = "bigtable"
    
    line = "SELECT * FROM %s AS nt2 WHERE k1 IN (SELECT it1.k1 \
            FROM %s AS it1 JOIN %s AS it3 ON it1.k1=it3.k1) \
            order by k1,k2,k3,k4"\
            % (join_name,join_name,table_name)
    runner.check(line)
    
    line = "SELECT ot1.k1 FROM %s AS ot1, %s AS nt3 \
            WHERE ot1.k1 IN (SELECT it2.k1 FROM %s AS it2 \
            JOIN %s AS it4 ON it2.k1=it4.k1) order by ot1.k1" \
            % (join_name,temp_name,table_name,temp_name)
    runner.check(line)
    line = "select k1 from %s where k2 in (select k1 from %s ) order by k1" \
            % (join_name,table_name)
    runner.check(line)
    line = "select k2 from %s as a where exists (select * from %s b where a.k1 = b.k1)\
            order by k1, k2, k3, k4" % (join_name,table_name)
    runner.check(line)
    

def test_subquery_nest():
    """
    {
    "title": "test_query_select.test_subquery_nest",
    "describe": "test for palo suported subquery",
    "tag": "function,p0"
    }
    """
    """
    test for palo suported subquery
	"""
    line = "select b.k1 from %s b where b.k1 < (select max(a.k1) \
			from %s a where a.k2 = b.k2) order by b.k1" % (join_name, table_name)
    runner.check(line)
    line = "select k1 from %s where k2 > (select max(k1) from %s where k2 = 1) order by k1" \
			% (table_name, join_name)
    runner.check(line)
    line = "select a.k1,a.k2 from %s a where a.k1 in (select k1 \
			from %s b where a.k2 = b.k2) order by a.k1,a.k2" % (join_name, table_name)
    runner.check(line)
    line = "select a.k1,a.k2 from %s a where a.k1 not in (select k1 \
			from %s b where a.k2 = b.k2) order by a.k1,a.k2" %(join_name, table_name)
    runner.check(line)
    line = "select k1,k2 from %s where k1 in (select k1 from %s where k2 > 3) \
			order by k1,k2" % (join_name, table_name)
    runner.check(line)
    line = "select k1,k2 from %s where k1 not in (select k1 from %s where k2 > 3) \
			order by k1,k2" % (join_name, table_name)
    runner.check(line)
    line = "select a.k1,a.k2 from %s a where exists (select b.k1,b.k2 \
			from %s b where a.k1 = b.k1) order by a.k1,a.k2" % (join_name, table_name)
    runner.check(line)
    line = "select a.k1,a.k2 from %s a where not exists (select b.k1,b.k2 \
			from %s b where a.k1 = b.k1) order by a.k1,a.k2" % (join_name, table_name)
    runner.check(line)
    line = "select a.k2,a.k1 from %s a where exists (select b.k1 \
			from %s b where a.k1 = b.k1 and a.k2 = b.k2) order by a.k1" \
			% (join_name, table_name)
    runner.check(line)
    line = "select a.k2,a.k1 from %s a where not exists (select b.k1 \
			from %s b where a.k1 = b.k1 and a.k2 = b.k2) order by a.k1" \
			% (join_name, table_name)
    runner.check(line)
    line = "select k1,k2 from %s where exists (select k1 from %s where k1 = 1) order by k1, k2" \
			% (join_name, table_name)
    runner.check(line)


def test_select_buchong():
    """
    {
    "title": "test_query_select.test_select_buchong",
    "describe": "补充简单case",
    "tag": "function,p0"
    }
    """
    """补充简单case
    """
    line = "select 1.1 from %s" % (join_name)
    runner.check(line)
    line = "select 1.1 from %s limit 1" % (join_name)
    runner.check(line)
    line = "select 1.1 as num from %s" % (join_name) 
    runner.check(line)
    line = "select 1.1 as num, k1 from %s order by k1 limit 1" % (join_name)
    runner.check(line)

    line = 'select k1 from (select k2 as k1 from %s where k2 in (select k2 from %s)) a order by k1' \
           % (join_name, table_name)
    runner.check(line)
    line = 'select k1 from (select k1 from %s where k3 in (select k2 from %s group by k2)) a order by k1' \
           % (join_name, table_name)
    runner.check(line)
    line = 'select k1 from (select k1 from %s where k3 in (select k2 from %s)) a order by k1' \
           % (join_name, table_name)
    runner.check(line)
    """execute fail
    line = 'select k1 from (select k1 from %s where exists (select k2 from %s)) a order by k1' \
           % (join_name, table_name)
    runner.check(line)
    """
 

def test_subquery_related_non_scalar():
    """
    {
    "title": "test_query_select.test_subquery_related_non_scalar",
    "describe": "related non-scalar subquery",
    "tag": "function,p0,fuzz"
    }
    """
    """related non-scalar subquery"""
    line = 'select * from baseall a where k1 in (select k1 from bigtable b where a.k2 = b.k2) \
            order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from baseall a where k1 in (select k1 from bigtable b where a.k2 > b.k1) \
            order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from baseall a where k1 not in (select k1 from bigtable b where a.k2 = b.k2) \
            order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from baseall a where k1 not in (select k1 from bigtable b where a.k2 > b.k1) \
            order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from baseall a where exists (select k1 from bigtable b where a.k2 = b.k2) \
            order by k1, k2, k3, k4'
    runner.check(line)
    # supported predicate with subquery: EXISTS
    line = 'select * from baseall a where exists (select k1 from bigtable b where a.k2 > b.k1) \
            order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from baseall a where not exists (select k1 from bigtable b where a.k2 = b.k2)\
            order by k1, k2, k3, k4'
    runner.check(line)
    # supported predicate with subquery: NOT EXISTS
    line = 'select * from baseall a where not exists (select k1 from bigtable b where a.k2 > b.k1)\
            order by k1, k2, k3, k4'
    runner.check(line)
    # The select item in correlated subquery of binary predicate should only be sum, min, max, avg and count.以下同
    line = 'select * from baseall a where k2 > (select k1 from bigtable b where a.k2 = b.k2) \
            order by k1, k2, k3, k4'
    runner.checkwrong(line)
    line = 'select * from baseall a where k2 = (select k1 from bigtable b where a.k2 > b.k1) \
            order by k1, k2, k3, k4'
    runner.checkwrong(line)
    line = 'select * from baseall as a where k3 between (select k1 from test as b where a.k2 > b.k2) \
            and(select distinct(baseall.k2) from baseall,test where test.k1 = 2 and baseall.k1 = 1)'
    runner.checkwrong(line)


def test_subquery_unrelated_non_scalar():
    """
    {
    "title": "test_query_select.test_subquery_unrelated_non_scalar",
    "describe": "unrelated non-scalar subquery",
    "tag": "function,p0,fuzz"
    }
    """
    """unrelated non-scalar subquery"""
    line = 'select * from baseall a where k1 in (select k1 from bigtable b where k2 > 0) \
            order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from baseall a where k1 in (select k1 from bigtable) order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from baseall a where k1 not in (select k1 from bigtable b where k2 > 0) \
            order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from baseall a where k1 not in (select k1 from bigtable) \
            order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from baseall a where exists (select k1 from bigtable b where k2 > 0) \
            order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from baseall a where exists (select k1 from bigtable) order by k1, k2, k3, k4'
    runner.check(line)
    # Unsupported uncorrelated NOT EXISTS subquery: 下同
    line = 'select * from baseall a where not exists (select k1 from bigtable b where k2 > 0) \
            order by k1, k2, k3, k4'
    runner.checkwrong(line)
    line = 'select * from baseall a where not exists (select k1 from bigtable) \
            order by k1, k2, k3, k4'
    runner.checkwrong(line)
    # Expected no more than 1 to be returned by expression. 下同
    line = 'select * from baseall a where k2 = (select k1 from bigtable b where k2 > 0) \
            order by k1, k2, k3, k4'
    runner.checkwrong(line)
    line = 'select * from baseall a where k2 > (select k1 from bigtable) order by k1, k2, k3, k4'
    runner.checkwrong(line)
    line = 'select * from baseall where k3 between (select k1 from test ) and \
            (select k2 from bigtable where bigtable.k1 = 1) order by k1, k2, k3, k4'
    runner.checkwrong(line)
    line = 'select * from baseall where k3 between (select k1 from test ) and \
            (select k2 from bigtable ) order by k1, k2, k3, k4'
    runner.checkwrong(line)


def test_subquery_related_scalar():
    """
    {
    "title": "test_query_select.test_subquery_related_scalar",
    "describe": "related scalar subquery",
    "tag": "function,p0,fuzz"
    }
    """
    """related scalar subquery"""
    line = 'select * from baseall a where k1 in (select k1 from bigtable b where a.k2 = b.k2 \
            and b.k1 = 1) order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from baseall a where k1 in (select k1 from bigtable b where a.k2 > b.k1 \
            and b.k1 = 1) order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from baseall a where k1 not in (select k1 from bigtable b where a.k2 = b.k2 \
            and b.k1 = 1) order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from baseall a where k1 not in (select k1 from bigtable b where a.k2 > b.k1 \
            and b.k1 = 1) order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from baseall a where exists (select k1 from bigtable b where a.k2 = b.k2 \
            and b.k1 = 1) order by k1, k2, k3, k4'
    runner.check(line)
    # supported predicate with subquery: EXISTS
    line = 'select * from baseall a where exists (select k1 from bigtable b where a.k2 > b.k1 \
            and b.k1 = 1) order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from baseall a where not exists (select k1 from bigtable b where a.k2 = b.k2 \
            and b.k1 = 1) order by k1, k2, k3, k4'
    runner.check(line)
    # supported predicate with subquery: NOT EXISTS
    line = 'select * from baseall a where not exists (select k1 from bigtable b where a.k2 > b.k1 \
            and b.k1 = 1) order by k1, k2, k3, k4'
    runner.check(line)
    # The select item in correlated subquery of binary predicate should only be sum, min, max, avg and count.
    line = 'select * from baseall a where k1 = (select k1 from bigtable b where a.k2 = b.k2 \
            and b.k1 = 1) order by k1, k2, k3, k4'
    runner.checkwrong(line)
    line = 'select * from baseall a where k2 > (select k1 from bigtable b where a.k2 > b.k1 \
            and b.k1 = 1) order by k1, k2, k3, k4'
    runner.checkwrong(line)
    line = 'select * from test as a where k3 between (select distinct(k3) from baseall as b where a.k1 = b.k1 \
            and a.k1 < 2 ) and (select distinct(baseall.k2) from baseall,test where test.k1 = 2 and baseall.k1 = 1)'
    runner.checkwrong(line)
    line = 'select * from test as a where k3 > (select distinct(k3) from baseall as b \
            where a.k1 = b.k1 and a.k1 = 2 ) order by k1, k2, k3, k4'
    runner.checkwrong(line)
    line = 'select * from test as a where k3 = (select distinct(k3) from baseall as b \
            where a.k1 = b.k1 and a.k1 < 2 ) order by k1, k2, k3, k4'
    runner.checkwrong(line)
    line = 'select * from test as a where k3 > (select sum(k3) from baseall as b where a.k1 = b.k1 \
            and a.k1 = 2 ) order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from test as a where k3 > (select min(k3) from baseall as b where a.k1 = b.k1 \
            and a.k1 = 2 ) order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from test as a where k3 > (select max(k3) from baseall as b where a.k1 = b.k1 \
            and a.k1 = 2 ) order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from test as a where k3 < (select avg(k3) from baseall as b where a.k1 = b.k1 \
            and a.k1 = 2 ) order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from test as a where k1 = (select count(k3) from baseall as b where a.k1 = b.k1 \
            and a.k1 = 2 ) order by k1, k2, k3, k4'
    runner.check(line)
    # scalar subquery's CorrelatedPredicates's operator must be EQ
    line = 'select * from test as a where k1 = (select count(k3) from baseall as b where a.k1 > b.k1)'
    runner.checkwrong(line)


def test_subquery_unrelated_scalar():
    """
    {
    "title": "test_query_select.test_subquery_unrelated_scalar",
    "describe": "unrelated scalar subquery",
    "tag": "function,p0,fuzz"
    }
    """
    """unrelated scalar subquery"""
    line = 'select * from baseall a where k1 in (select k1 from bigtable b where k2 > 0 and k1 = 1)\
            order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from baseall a where k1 in (select k1 from bigtable where k1 = 1) \
            order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from baseall a where k1 not in (select k1 from bigtable b where k2 > 0 \
            and k1 = 1) order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from baseall a where k1 not in (select k1 from bigtable where k1 = 1) \
            order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from baseall a where exists (select k1 from bigtable b where k2 > 0 \
            and k1 = 1) order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from baseall a where exists (select k1 from bigtable where k1 = 1) \
            order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from baseall a where not exists (select k1 from bigtable b where k2 > 0 \
            and k1 = 1) order by k1, k2, k3, k4'
    runner.checkwrong(line)
    line = 'select * from baseall a where not exists (select k1 from bigtable where k1 = 1) \
            order by k1, k2, k3, k4'
    runner.checkwrong(line)
    line = 'select * from baseall a where k1 = (select k1 from bigtable b where k2 > 0 and k1 = 1)\
            order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from baseall a where k2 > (select k1 from bigtable where k1 = 1) \
            order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from baseall where k3 between (select k1 from bigtable where bigtable.k1 = 1) \
            and (select k2 from bigtable where bigtable.k1 = 1) order by k1, k2, k3, k4'
    runner.check(line)
    line = 'select * from baseall where k1 > (select b from (select count(*) b from baseall) a)'
    runner.check(line)


def test_subquery_supply():
    """
    {
    "title": "test_query_select.test_subquery_supply",
    "describe": "subquery in from",
    "tag": "function,p0"
    }
    """
    """subquery in from"""
    line = 'select k1, k2 from (select k1, max(k2) as k2 from test where k1 > 0 group by k1 \
            order by k1)a where k1 > 0 and k1 < 10'
    runner.check(line, True)
    line = 'select k1, k2 from (select k1, max(k2) as k2 from test where k1 > 0 group by k1 \
            order by k1)a left join (select k1 as k3, k2 as k4 from baseall) b on a.k1 = b.k3 \
            where k1 > 0 and k1 < 10'
    runner.check(line, True)
    # PALO-3748, couldn't resolve slot descriptor 3
    sql = 'drop table if exists p_3748'
    runner.checkok(sql)
    sql = 'CREATE TABLE `p_3748` (`k1` int(11) COMMENT "",`k2` int(11) COMMENT "") ENGINE=OLAP \
           AGGREGATE KEY(`k1`, `k2`) DISTRIBUTED BY HASH(`k1`) BUCKETS 1 PROPERTIES \
           ("storage_type" = "COLUMN")'
    runner.checkok(sql)
    line = 'select k1 from (select k1, k2 from p_3748 order by k1) a where k2=1;'
    runner.checkok(line)
    line = 'drop table if exists p_3748'
    runner.checkok(line)


def test_subquery_binary_cmp():
    """
    {
    "title": "test_query_select.test_subquery_binary_cmp",
    "describe": "binary subquery",
    "tag": "function,p0,fuzz"
    }
    """
    line = 'select * from baseall where k1 < (select count(*) from test) order by k1'
    runner.check(line)
    line = 'select * from baseall where k1 < (select * from (select count(*) from baseall)a ) order by k1'
    runner.check(line)
    line = 'select * from baseall where k1 < (select * from (select count(*), 1 from baseall)a ) order by k1'
    runner.checkwrong(line)
    line = 'select * from baseall where k1 != (select k1 from baseall where k1 = 16) order by k1'
    runner.check(line)
    line = 'select * from baseall where k1 = (select null) order by k1'
    runner.check(line)
    # todo
    line = 'select * from baseall where (select null) is null order by k1'
    # runner.check(line)
    line = 'select * from baseall where k1 = (select * from baseall where k1 = 1) order by k1'
    runner.checkwrong(line)
    line = 'select * from baseall where k1 = (select count(k1) from baseall group by k1 having k1 = 1) order by k1'
    runner.check(line)
    line = 'select * from baseall where k1 = (select k1 from test where k1 = 1 limit 1) order by k1'
    runner.check(line)
    line = 'select * from baseall where k1 = (select k1 from baseall) order by k1'
    runner.checkwrong(line)


def teardown_module():
    """
    end 
    """
    print("End")
    # mysql_cursor.close()
    # mysql_con.close()

