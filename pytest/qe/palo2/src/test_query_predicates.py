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


def test_query_between():
    """
    {
    "title": "test_query_predicates.test_query_between",
    "describe": "between",
    "tag": "function,p0"
    }
    """
    """
    between
    """
    line = "select if(k1 between 1 and 2, 2, 0) as wj from %s order by wj" % (table_name)
    runner.check(line)
    line = "select k1 from %s where k1 between 3 and 4 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k2 from %s where k2 between 1980 and 1990 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k3 from %s where k3 between 1000 and 2000 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k4 from %s where k4 between -100000000 and 0 \
		    order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k6 from %s where lower(k6) between 'f' and 'false' \
		    order by k1, k2, k3, k4" % (join_name)
    runner.check(line)
    line = "select k7 from %s where lower(k7) between 'a' and 'g' order by k1, k2, k3, k4"\
		    % (table_name)
    runner.check(line)
    line = "select k8 from %s where k8 between -2 and 0 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k10 from %s where k10 between \"2015-04-02 00:00:00\" \
		    and \"9999-12-31 12:12:12\" order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k11 from %s where k11 between \"2015-04-02 00:00:00\" \
            and \"9999-12-31 12:12:12\" order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k10 from %s where k10 between \"2015-04-02\" \
            and \"9999-12-31\" order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k9 from %s where k9 between -1 and 6.333 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k5 from %s where k5 between 0 and 1243.5 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)


def test_query_cmp():
    """
    {
    "title": "test_query_predicates.test_query_cmp",
    "describe": "query contains where, where contains =, like, <>, <, <=, >, >=, !=",
    "tag": "function,p0"
    }
    """
    """
    query contains where
    where contains =, like, <>, <, <=, >, >=, !=

    """
    
    line = "select k1 from %s where k1 < 4 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k2 from %s where k2 < 1990 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k3 from %s where k3 < 2000 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k4 from %s where k4 < 0 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k5 from %s where k5 < 1243.5 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select lower(k6) from %s where lower(k6) < 'false' order by lower(k6)" % (join_name)
    runner.check(line)
    line = "select lower(k7) from %s where lower(k7) < 'g' order by lower(k7)" % (join_name)
    runner.check(line)
    line = "select k8 from %s where k8 < 0 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k9 from %s where k9 < 6.333 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1 from %s where k1 <> 4 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k2 from %s where k2 <> 1989 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k3 from %s where k3 <> 1001 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k4 from %s where k4 <> -11011903 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k5 from %s where k5 <> 1243.5 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k6 from %s where k6 <> 'false' order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k7 from %s where k7 <> 'f' order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k8 from %s where k8 <> 0 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k8 from %s where k8 <> 0.1 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k9 from %s where k9 <> 6.333 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k9 from %s where k9 <> -365 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)

    line = "select k1 from %s where k1 != 4 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k2 from %s where k2 != 1989 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k3 from %s where k3 != 1001 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k4 from %s where k4 != -11011903 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k5 from %s where k5 != 1243.5 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k6 from %s where k6 != 'false' order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k7 from %s where k7 != 'f' order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k8 from %s where k8 != 0 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k8 from %s where k8 != 0.1 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k9 from %s where k9 != 6.333 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k9 from %s where k9 != -365 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where k1<10000000000000000000000000 \
		    order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where k5=123.123000001" % (table_name)
    runner.check(line)
    line = "select * from %s where k1=1 or k1>=10 and k6=\"true\" order by k1, k2, k3, k4"\
		    % (table_name)
    runner.check(line)


def test_query_in():
    """
    {
    "title": "test_query_predicates.test_query_in",
    "describe": "test for query function in",
    "tag": "function,p0"
    }
    """
    """
    test for query function in 
    """
    line = "select * from %s where k1 in (1, -1, 5, 0.1, 3.000) order by k1, k2, k3, k4" \
		    % (table_name)
    runner.check(line)
    line = "select * from %s where k6 in (\"true\") order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where k7 in (\"wangjuoo4\") order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where k7 in (\"wjj\") order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where k8 in (1, -1, 0.100, 0) order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where k9 in (-365, 100) order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where k5 in \
		    (123.123, 1243.5, 100, -654,6540, \"0\", \"-0.1230\") \
		    order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from test where k4 in \
		    (-9016414291091581975, -1, 100000000000000000000000000000000000) order by k1, k2, k3, k4"
    runner.check(line)
    line = "select * from %s where k1 not in (1, -1, 5, 0.1, 3.000) order by k1, k2, k3, k4" \
		    % (table_name)
    runner.check(line)
    line = "select * from %s where k6 not in (\"true\") order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where k7 not in (\"wangjuoo4\") order by k1, k2, k3, k4" \
		    % (table_name)
    runner.check(line)
    line = "select * from %s where k7 not in (\"wjj\") order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where k8 not in (1, -1, 0.100, 0) order by k1, k2, k3, k4" \
		    % (table_name)
    runner.check(line)
    line = "select * from %s where k9 not in (-365, 100) order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where k5 not in \
		    (123.123, 1243.5, 100, -654,6540, \"0\", \"-0.1230\") \
		    order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from test where k4 not in \
		    (-9016414291091581975, -1, 100000000000000000000000000000000000) order by k1, k2, k3, k4"
    runner.check(line)
    line = "select NULL in (1, 2, 3)"
    runner.check(line)
    line = "select NULL in (1, NULL, 3)"
    runner.check(line)
    line = "select 1 in (2, NULL, 1)"
    runner.check(line)
    line = "select 1 in (1, NULL, 2)"
    runner.check(line)
    line = "select 1 in (2, NULL, 3)"
    runner.check(line)
    line = "select 1 in (2, 3, 4)"
    runner.check(line)
    line = "select NULL not in (1, 2, 3)"
    runner.check(line)
    line = "select NULL not in (1, NULL, 3)"
    runner.check(line)
    line = "select 1 not in (2, NULL, 1)"
    runner.check(line)
    line = "select 1 not in (1, NULL, 2)"
    runner.check(line)
    line = "select 1 not in (2, NULL, 3)"
    runner.check(line)
    line = "select 1 not in (2, 3, 4)"
    runner.check(line)
    line = "select * from %s where k1 in (1,2,3,4) and k1 in (1)" % join_name
    runner.check(line)
    line = "select * from (select 'jj' as kk1, sum(k2) from baseall where k10 = '2015-04-02' group by kk1)tt " \
           "where kk1 in ('jj')"
    runner.check(line)
    line = "select * from (select 'jj' as kk1, sum(k2) from baseall where k10 = '2015-04-02' group by kk1)tt " \
           "where kk1 = 'jj'"
    runner.check(line)
    

def test_query_like():
    """
    {
    "title": "test_query_predicates.test_query_like",
    "describe": "test for function like",
    "tag": "function,p0"
    }
    """
    """
    test for function like
    """
    line = "select * from %s where k6 like \"%%\" order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    #todo "%%"
    line = "select * from %s where k6 like \"____\" order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where lower(k7) like \"%%lnv%%\" order by k1, k2, k3, k4" \
		    % (table_name)
    runner.check(line)
    line = "select * from %s where lower(k7) like \"%%lnv%%\" order by k1, k2, k3, k4"\
		    % (table_name)
    runner.check(line)
    line = "select * from %s where lower(k7) like \"wangjuoo4\" order by k1, k2, k3, k4" \
		    % (table_name)
    runner.check(line)
    line = "select * from %s where lower(k6) like \"%%t_u%%\" order by k1, k2, k3, k4" \
		    % (table_name)
    runner.check(line)

    line = "select * from %s where k6 not like \"%%\" order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where k6 not like \"____\" order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where lower(k7) not like \"%%lnv%%\" order by k1, k2, k3, k4" \
		    % (table_name)
    runner.check(line)
    line = "select * from %s where lower(k7) not like \"%%lnv%%\" order by k1, k2, k3, k4" \
		    % (table_name)
    runner.check(line)
    line = "select * from %s where lower(k7) not like \"wangjuoo4\" \
		    order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where lower(k6) not like \"%%t_u%%\" order by k1, k2, k3, k4" \
		    % (table_name)
    runner.check(line)
    line = 'select "abcd%%1" like "abcd%", "abcd%%1" not like "abcd%"'
    runner.check(line)
    line = 'select "abcd%%1" like "abcd%1", "abcd%%1" not like "abcd%1"'
    runner.check(line)
    line = 'select "abcd%%1" like "abcd%1%", "abcd%%1" not like "abcd%1%"'
    runner.check(line)
    line = 'select "abcd%%1" like "abcd\%%", "abcd%%1" not like "abcd\%%"'
    runner.check(line)
    line = 'select "abcd%%1" like "abcd\%\%%", "abcd%%1" not like "abcd\%\%%"'
    runner.check(line)
    line = 'select "abcd%%1" like "abcd\%\%1", "abcd%%1" not like "abcd\%\%1"'
    runner.check(line)
    line = 'select "abcd%%1" like "abcd\%\%\1%", "abcd%%1" not like "abcd\%\%\1%"'
    runner.check(line)
    line = 'select "abcd%%1" like "abcd_%1", "abcd%%1" not like "abcd_%1"'
    runner.check(line)
    line = 'select "abcd%%1" like "abcd_\%1", "abcd%%1" not like "abcd_\%1"'
    runner.check(line)
    line = 'select "abcd%%1" like "abcd__1", "abcd%%1" not like "abcd__1"'
    runner.check(line)
    line = 'select "abcd%%1" like "abcd_%_", "abcd%%1" not like "abcd_%_"'
    runner.check(line)
    line = 'select "abcd%%1" like "abcd\_%1", "abcd%%1" not like "abcd\_%1"'
    runner.check(line)


def test_query_operate():
    """
    {
    "title": "test_query_predicates.test_query_operate",
    "describe": "test for +,-,*,/",
    "tag": "function,p0"
    }
    """
    """
    test for +,-,*,/
    """
    line = "select k1, k4 div k1, k4 div k2, k4 div k3, k4 div k4 \
		    from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1, k1+ '1', k5,100000*k5 from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1,k5,k2*k5 from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1,k5,k8*k5,k5*k9,k2*k9,k2*k8 from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1, k5*0.1, k8*0.1, k9*0.1 from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1, k2*(-0.1), k3*(-0.1), k4*(-0.1), \
		    k5*(-0.1), k8*(-0.1), k9*(-0.1) from  %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1, k5*(9223372036854775807/100), k8*9223372036854775807, \
		    k9*9223372036854775807 from  %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1, k2/9223372036854775807, k3/9223372036854775807, \
		    k4/9223372036854775807,k5/9223372036854775807, \
		    k8/9223372036854775807,k9/9223372036854775807 \
		    from  %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1, k5+9223372036854775807/100, k8+9223372036854775807, \
		    k9+9223372036854775807 from  %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1, k5-9223372036854775807/100, k8-9223372036854775807, \
		    k9-9223372036854775807 from  %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1, k5/0.000001, k8/0.000001, \
		    k9/0.000001 from  %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1, k1*0.1, k2*0.1, k3*0.1, k4*0.1, k5*0.1, k8*0.1, k9*0.1 \
		    from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1, k1/10, k2/10, k3/10, k4/10, k5/10, k8/10, k9/10 \
		    from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1, k1-0.1, k2-0.1, k3-0.1, k4-0.1, k5-0.1, k8-0.1, k9-0.1 \
		    from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1, k1+0.1, k2+0.1, k3+0.1, k4+0.1, k5+0.1, k8+0.1, k9+0.1 \
		    from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1+10, k2+10.0, k3+1.6, k4*1, k5-6, k8-234.66, k9-0 \
		    from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where k1+k9<0 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1*k2*k3*k5 from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1*k2*k3*k5*k8*k9 from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1*10000/k4/k8/k9 from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    for i in (1, 2, 3, 5, 8, 9):
        for j in (1, 2, 3, 5, 8, 9):
            line = "select k%s*k%s, k%s+k%s, k%s-k%s, k%s/k%s from %s \
			    where abs(k%s)<9223372036854775807 and k%s<>0 and\
			    abs(k%s)<922337203685477580 order by k1, k2, k3, k4"%\
			    (i, j, i, j, i, j, i, j, table_name, i, j, j)
            runner.check(line)
    line = "select 1.1*1.1 + k2 from %s order by 1 limit 10" % (table_name)
    runner.check(line)
    line = "select 1.1*1.1 + k5 from %s order by 1 limit 10" % (table_name)
    runner.check(line)
    line = "select 1.1*1.1+1.1"
    runner.check(line)


def test_query_reg():
    """
    {
    "title": "test_query_predicates.test_query_reg",
    "describe": "test for function reg",
    "tag": "function,p0"
    }
    """
    """
    test for function reg
    """
    line = "select * from %s where lower(k7) regexp'.*o4$' order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where lower(k7) regexp'[yun]+nk' order by k1, k2, k3, k4"\
		    % (table_name)
    runner.check(line)
    line = "select * from %s where lower(k7) regexp'wang(juoo|yu)[0-9]+$' order by k1, k2, k3, k4"\
		    % (table_name)
    runner.check(line)
    line = "select * from %s where lower(k7) regexp'^[a-z]+[0-9]?$' \
		    order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where lower(k7) regexp'^[a-z]+[0-9]+[a-z]+$' order by k1, k2, k3, k4"\
		    % (table_name)
    runner.check(line)
    line = "select * from %s where lower(k7) regexp'^[a-o]+[0-9]+[a-z]?$' order by k1, k2, k3, k4" \
		    % (table_name)
    runner.check(line)
    line = "select count(*) from %s where k1<10 and lower(k6) regexp '^t'" % (table_name)
    runner.check(line)


def test_query_not_reg():
    """
    {
    "title": "test_query_predicates.test_query_not_reg",
    "describe": "test for function not regexp",
    "tag": "function,p0"
    }
    """
    line = "select * from %s where lower(k7) not regexp'.*o4$' order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where lower(k7) not regexp'[yun]+nk' order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where lower(k7) not regexp'wang(juoo|yu)[0-9]+$' order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where lower(k7) not regexp'^[a-z]+[0-9]?$' order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where lower(k7) not regexp'^[a-z]+[0-9]+[a-z]+$' order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select * from %s where lower(k7) not regexp'^[a-o]+[0-9]+[a-z]?$' order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select count(*) from %s where k1<10 and lower(k6) not regexp '^t'" % (table_name)
    runner.check(line)


def test_query_and_or_xor_mod():
    """
    {
    "title": "test_query_predicates.test_query_and_or_xor_mod",
    "describe": "&, |, ^, %",
    "tag": "function,p0"
    }
    """
    """
    &, |, ^, %
    """
    #fushu unable
    for i in range(1, 6):
        line = "select k%s %%2 from %s order by k1, k2, k3, k4" % (i, table_name)
        runner.check(line)
        line = "select k%s %%-2 from %s order by k1, k2, k3, k4" % (i, table_name)
        runner.check(line)
        if (i != 5):
            line = "select k%s %%0 from %s order by k1, k2, k3, k4" % (i, table_name)
            runner.check(line)
        line = "select k%s %%2.1 from %s order by k1, k2, k3, k4" % (i, table_name)
        runner.check(line)
        line = "select k%s %%-2.1 from %s order by k1, k2, k3, k4" % (i, table_name)
        runner.check(line)
    for i in range(1, 5):
        for j in range(1, 5):
            line  = "select k%s^k%s from %s where k%s>=0 and k%s>=0 order by k1, k2, k3, k4"\
			    % (i, j, table_name, i, j)
            runner.check(line)
            line  = "select k%s|k%s from %s where k%s>=0 and k%s>=0 order by k1, k2, k3, k4" \
			    % (i, j, table_name, i, j) 
            runner.check(line)
            line  = "select k%s&k%s from %s where k%s>=0 and k%s>=0 order by k1, k2, k3, k4" \
			    % (i, j, table_name, i, j)
            runner.check(line)
            """
            line  = "select k%s, ~k%s from %s where k%s>=0 and k%s>=0 order by k1, k2, k3, k4" \
			    % (i, j, table_name, i, j)
            runner.check(line)
	    """
    line = "select k8, k9, k8%%k9, k9%%NULL, NULL%%k9 from %s order by 1, 2" % (table_name)
    runner.check(line)
    line = 'select * from baseall where (k1 = 1) or (k1 = 1 and k2 = 2)'
    runner.check(line)


def test_query_case():
    """
    {
    "title": "test_query_predicates.test_query_case",
    "describe": "query for case",
    "tag": "function,p0"
    }
    """
    """
    query for case
    """
    '''
    line = "select (case\
		    when k6='true' or k6='false' then\
		    (select k10 from %s where %s.k1 = %s.k1 order by k1 limit 1)\
		    when k7 like '%%w%%' then\
		    (select k10 from %s where %s.k1 = %s.k1 order by k1 limit 1)\
		    else\
		    (select k10 from %s where %s.k1 = 0 and %s.k1 <> 0 order by k1 limit 1)\
		    end) as wj from %s order by wj"\
		    % (join_name, join_name, table_name, \
		       join_name, join_name, table_name,\
		       join_name, join_name, join_name, table_name)
    runner.check(line)
    '''
    line = "select 'number', count(*) from %s group by\
		    case\
		    when k1=10 then 'zero'\
		    when k1>10 then '+'\
		    when k1<10 then '-' end order by 1, 2" % (join_name)
    runner.check(line)
    line = "select case when k1=0 then 'zero'\
		        when k1>0 then '+'\
		        when k1<0 then '-' end as wj,\
		   count(*) from %s\
		   group by\
		    case when k1=0 then 'zero'\
		         when k1>0 then '+'\
		         when k1<0 then '-' end\
		   order by\
		    case when k1=0 then 'zero'\
		         when k1>0 then '+'\
		         when k1<0 then '-' end" % (table_name)
    runner.check(line)
    line = "select a.k1, case\
		       when b.wj is not null and b.k1>0 then 'wangj'\
		       when b.wj is null then b.wj\
		       end as wjtest\
		       from (select k1, k2, case when k6='true' then 'ok' end as wj\
		                        from %s) as b \
		       join %s as a where a.k1=b.k1 and a.k2=b.k2 order by k1, wjtest "\
				    % (table_name, join_name)
    runner.check(line)
    line = "select case when k1<0 then 'zhengshu' when k10='1989-03-21' then 'birthday' \
		    when k2<0 then 'fu' when k7 like '%%wang%%' then 'wang' else 'other' end \
		    as wj from %s order by wj" % (table_name)
    runner.check(line)
    line = "select case k6 when 'true' then 1 when 'false' then -1 else 0 end \
		    as wj from %s order by wj" % (table_name)
    runner.check(line)
    line = "select k1, case k1 when 1 then 'one' when 2 then 'two' \
		    end as wj from %s order by k1, wj" % (table_name)
    runner.check(line)
    line = "select k1, case when k2<0 then -1 when k2=0 then 0 when k2>0 then 1 end \
		    as wj from %s order by k1, wj" % (table_name)
    runner.check(line)


def test_query_if():
    """
    {
    "title": "test_query_predicates.test_query_if",
    "describe": "if/nullif/ifnull",
    "tag": "function,p0"
    }
    """
    """if/nullif/ifnull"""
    line = 'select if(null, -1, 10) a, if(null, "hello", "worlk") b'
    runner.check(line)   
    line = 'select if(k1 > 5, true, false) a from baseall order by k1'
    runner.check(line)
    line = 'select if(k1, 10, -1) a from baseall order by k1'
    # runner.check(line)
    line = 'select if(length(k6) >= 5, true, false) a from baseall order by k1'
    runner.check(line)
    line = 'select if(k6 like "fa%", -1, 10) a from baseall order by k6'
    runner.check(line)
    line = 'select if(k6 like "%e", "hello", "world") a from baseall order by k6'
    runner.check(line)
    line = 'select if(k6, -1, 0) a from baseall order by k6'
    # runner.check(line)
    line = 'select ifnull(b.k1, -1) k1 from baseall a left join bigtable b on a.k1 = b.k1 + 5 \
            order by a.k1'
    runner.check(line)
    line = 'select ifnull(b.k6, "hll") k1 from baseall a left join bigtable b on a.k1 = b.k1 + 5 \
            order by k1'
    runner.check(line)
    line = 'select ifnull(b.k10, "2017-06-06") k1 from baseall a left join bigtable b on \
            a.k1 = b.k1 + 5 order by k1'
    # runner.check(line)
    line = 'select ifnull(b.k10, cast("2017-06-06" as date)) k1 from baseall a left join bigtable \
            b on a.k1 = b.k1 + 5 order by k1'
    runner.check(line)
    line = 'select ifnull(b.k1, "-1") k1 from baseall a left join bigtable b on a.k1 = b.k1 + 5 \
            order by a.k1'
    # runner.check(line)
    line = 'select ifnull(b.k6, 1001) k1 from baseall a left join bigtable b on a.k1 = b.k1 + 5 \
            order by k1'
    # runner.check(line)
    line = 'select nullif(k1, 100) k1 from baseall order by k1'
    runner.check(line)
    line = 'select nullif(k6, "false") k from baseall order by k1'
    runner.check(line)
    line = 'select cast(nullif(k10, cast("2012-03-14" as date)) as date) from baseall order by k1'
    runner.check(line)
    line = 'select cast(nullif(k11, cast("2000-01-01 00:00:00" as datetime)) as datetime) from baseall order by k1'
    runner.check(line)
    line = 'select nullif(b.k1, null) k1 from baseall a left join bigtable b on a.k1 = b.k1 \
            order by k1'
    runner.check(line)


def test_query_ifnull():
    """
    {
    "title": "test_query_predicates.test_query_ifnull",
    "describe": "ifnull test case",
    "tag": "function,p0,fuzz"
    }
    """
    """ifnull test case"""
    line = "select ifnull(null,2,3)"
    runner.checkwrong(line)
    line = "select ifnull(1234567890123456789012345678901234567890,2)"
    runner.checkwrong(line)
    line = "select ifnull(123456789.5678901234567890,2),\
        ifnull('1234567890123456789012345678901234567890',2)"
    runner.check(line)
    line = "select IFNULL('hello', 'doris'), IFNULL(NULL,0)"
    runner.check(line)
    line = "select ifnull('null',2), ifnull('NULL',2), ifnull('null','2019-09-09 00:00:00'),\
        ifnull(NULL, concat('NUL', 'LL'))"
    runner.check(line)
    for index in range(1, 12):
        if index in [7, 10]:
            continue
        line = "select ifnull(k%s, NULL) from %s order by k%s" % (index, join_name, index)
        runner.check(line)
        line = "select ifnull(NULL, k%s) from %s order by k%s" % (index, join_name, index)
        runner.check(line)
    ##操作符
    line = "select ifnull('null',2+3*5), ifnull(NULL,concat(1,2)), ifnull(NULL, ifnull(1,3)),\
           ifnull(NULL,NULL) <=> NULL"
    runner.check(line)
    #不支持子查询，mysql支持
    line = "select concat((select k2 from baseall order by k2 limit 1), 2)"
    runner.checkwrong(line)
    ##表达式
    line = "select ifnull(length('null'), 2), ifnull(concat(NULL, 0), 2), ifnull('1.0' + '3.3','2019-09-09 00:00:00'),\
        ifnull(ltrim('  NULL'), concat('NUL', 'LL'))"
    runner.check(line)
    line = "select ifnull(2+3, 2), ifnull((3*1 > 1 || 1>0), 2), ifnull((3*1 > 1 or 1>0), 2),\
        ifnull(upper('null'), concat('NUL', 'LL'))"
    runner.check(line)
    line = "select ifnull(date(substring('2020-02-09', 1, 1024)), null)"
    runner.check(line)


def test_colsesce():
    """
    {
    "title": "test_query_predicates.test_colsesce",
    "describe": "colsesce(e1, e2, e3...) if there is null, return null, or return e1",
    "tag": "function,p0"
    }
    """
    """colsesce(e1, e2, e3...) 返回第一个不是Null的"""
    for k in range(1, 12):
        line = 'select k1, coalesce(k%s) from %s order by 1' % (k, join_name)
        runner.check(line)
        line = 'select k1, coalesce(k%s, k%s) from %s order by 1' % (k, k, join_name)
        runner.check(line)
        line = 'select k1, coalesce(k%s, null) from %s order by 1' % (k, join_name)
        runner.check(line)
        line = 'select k1, coalesce(null, k%s) from %s order by 1' % (k, join_name)
        runner.check(line)
    line = 'select coalesce(k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, null) from %s order by 1' % table_name
    runner.check(line)
    line = 'select coalesce(k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11) from %s order by 1' % table_name
    runner.check(line)
    line = 'select * from (select coalesce("string", "")) a'
    runner.check(line)
    line = 'select * from %s where coalesce(k1, k2) in (1, null) order by 1, 2, 3, 4' % join_name
    runner.check(line)
    line = 'select * from %s where coalesce(k1, null) in (1, null) order by 1, 2, 3, 4, 5, 6' % table_name
    runner.check(line)
    line = 'select  coalesce(1, null)'
    runner.check(line)


def test_query_divide_mod_zero():
    """
    {
    "title": "test_query_predicates.test_query_divide_mod_zero",
    "describe": "test for decimal value divide,mod zero,result should be NULL, github issue #6049",
    "tag": "function,p0"
    }
    """
    line = "select 10.2 / 0.0, 10.2 / 0, 10.2 % 0.0, 10.2 % 0"
    runner.check(line)
    line = "select 0.0 / 0.0, 0.0 / 0, 0.0 % 0.0, 0.0 % 0"
    runner.check(line)
    line = "select -10.2 / 0.0, -10.2 / 0, -10.2 % 0.0, -10.2 % 0"
    runner.check(line) 
    line = "select k5 / 0, k8 / 0, k9 / 0 from %s order by k1,k2,k3,k4" % table_name
    runner.check(line)
    line = "select k5 %% 0, k8 %% 0, k9 %% 0 from %s order by k1,k2,k3,k4" % table_name
    runner.check(line)


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
    test_query_ifnull()
