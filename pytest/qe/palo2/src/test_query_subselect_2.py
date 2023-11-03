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
add the case about subquery in having clause and in select case when
"""
import sys
import pytest
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


def test_having_subquery():
    """
    {
    "title": "having子查询测试",
    "describe": "test simple having subquery",
    "tag": "p0,fuzz,function"
    }
    """
    # Only constant expr could be supported in having clause when no aggregation in stmt
    line = 'SELECT * FROM baseall tb1, test tb2 WHERE tb1.k1 = 1 AND tb2.k1 = 1 ' \
           'HAVING tb2.k2 = (SELECT MAX(k2) FROM baseall)'
    runner.checkwrong(line)
    line = 'select t000.k1, count(*) `C` FROM test t000 GROUP BY t000.k1 ' \
           'HAVING count(*) > (SELECT count(*) FROM baseall t001)'
    runner.check(line, True)
    # Unsupported correlated subquery，having子查询不支持相关子查询
    line = 'SELECT k1, k2 FROM test GROUP BY k1, k2 ' \
           'HAVING COUNT(*) >= (SELECT count(k1) FROM baseall WHERE baseall.k1 = test.k1)'
    runner.checkwrong(line)
    # only support one subquery in xxx，支持一个子查询
    line = 'SELECT k1 FROM test GROUP BY k1 HAVING ' \
           'IFNULL((SELECT max(k1) FROM baseall WHERE k1 > 2), (SELECT max(k1) FROM bigtable)) > 3'
    runner.checkwrong(line)
    line = 'SELECT k1 FROM test GROUP BY k1 HAVING k1 IN (SELECT k1 FROM baseall where k1 > 5) ' \
           'ORDER BY k1'
    runner.check(line)
    line = 'SELECT k1 FROM test GROUP BY k1 HAVING ' \
           'k1 NOT IN (SELECT k1 FROM baseall where k1 > 5) ORDER BY k1'
    runner.check(line)
    line = 'SELECT k1 FROM test GROUP BY k1 HAVING ' \
           'k1 IN (SELECT k1 FROM baseall WHERE k2<k3) ORDER BY k1'
    runner.check(line)
    # type，和MySQL处理不一致，MySQL：0 in ('true', 'false')为true，Palo报错不支持
    line = 'SELECT k1 FROM test GROUP BY k1 HAVING ' \
           'k1 IN (SELECT k6 FROM baseall WHERE k2<k3) ORDER BY k1'
    runner.checkok(line)
    # 测试点：子查询结果为empty set
    line = 'SELECT k1 FROM test GROUP BY k1 HAVING ' \
           'k1 IN (SELECT k1 FROM baseall WHERE k2=k3)'
    runner.check(line)
    # todo: 当前提示Unknown column 'k2' in 'baseall'，暂时保持
    line = 'SELECT k1 FROM test GROUP BY k1 HAVING ' \
           'k1 IN (SELECT k1 FROM baseall WHERE k2 >= (' \
           'SELECT min(k3) FROM bigtable WHERE k1 < baseall.k2))'
    runner.checkwrong(line)
    line = 'SELECT k1 FROM test GROUP BY k1 HAVING k1 IN ' \
           '(SELECT k1 FROM baseall WHERE ' \
           'EXISTS(SELECT k1 FROM bigtable WHERE k1 is not null)) ORDER BY k1'
    runner.check(line)
    # Unsupported uncorrelated NOT EXISTS subquery
    line = 'SELECT k1 FROM test GROUP BY k1 HAVING k1 IN ' \
           '(SELECT k1 FROM baseall WHERE ' \
           'NOT EXISTS(SELECT k1 FROM bigtable WHERE k1 is not null)) ORDER BY k1'
    runner.checkwrong(line)
    line1 = 'SELECT k1, k2 FROM test GROUP BY k1,k2 HAVING ' \
            'count(k1) IN (SELECT k1 FROM baseall WHERE ' \
            'k5 > (SELECT count(*) FROM bigtable WHERE k2 > 0))'
    line2 = 'SELECT k1, k2 FROM test GROUP BY k1,k2 HAVING count(k1) IN (1,2,3,4,5,6,12,13)'
    runner.check2(line1, line2, True)
    line = 'SELECT k1, k2 FROM test GROUP BY k1,k2 HAVING k2 IN ' \
           '(SELECT k1 FROM baseall WHERE k5 > (SELECT count(*) FROM bigtable WHERE k2 > 0))'
    runner.check(line, True)
    line = 'SELECT k1, k2 FROM test GROUP BY k1,k2 HAVING count(k1) > 1 ORDER BY k1, k2'
    runner.check(line)
    line = 'SELECT k1 FROM test GROUP BY k1 HAVING k1 IN (SELECT k1 FROM baseall WHERE ' \
           'EXISTS(SELECT k1 FROM bigtable WHERE k1 > 10 or k3 < 5)) ORDER BY k1'
    runner.check(line)
    line = 'SELECT k1 FROM test GROUP BY k1 HAVING k1 IN (SELECT k1 FROM baseall WHERE k2 < k3 ' \
           'AND EXISTS(SELECT k1 FROM bigtable WHERE k3 < 5)) ORDER BY k1'
    runner.check(line)
    line = 'SELECT k1 FROM test WHERE EXISTS(SELECT k1 FROM baseall GROUP BY k1 ' \
           'HAVING SUM(k1) = 5) GROUP BY k1 ORDER BY k1'
    runner.check(line)
    # The correlated having clause is not supported
    line = 'SELECT k1 FROM test GROUP BY k1 HAVING ' \
           'EXISTS(SELECT k1 FROM baseall GROUP BY k1 HAVING SUM(test.k1) = k1)'
    runner.checkwrong(line)
    line = 'SELECT k1 FROM test WHERE k1 < 3 AND ' \
           'EXISTS(SELECT k2 FROM baseall GROUP BY k2 HAVING SUM(k1) != k2) ORDER BY k1'
    runner.check(line)
    # todo: ERROR 1064 (HY000): Unknown error
    line = 'SELECT test.k1 FROM test GROUP BY test.k1 HAVING ' \
           'test.k1 < (SELECT max(baseall.k1) FROM baseall GROUP BY baseall.k1 ' \
           'HAVING EXISTS(SELECT bigtable.k1 FROM bigtable GROUP BY bigtable.k1))'
    # runner.check(line)
    line = 'SELECT test.k1 FROM test GROUP BY test.k1 HAVING ' \
           'test.k1 > (SELECT max(baseall.k1) FROM baseall WHERE ' \
           'EXISTS(SELECT sum(bigtable.k1) FROM bigtable GROUP BY bigtable.k2)) ORDER BY test.k1'
    runner.check(line)
    # aggregate function cannot contain aggregate parameters
    line = 'SELECT test.k1 from test GROUP BY test.k1 HAVING AVG(SUM(test.k2)) > 20'
    runner.checkwrong(line)
    line = 'SELECT test.k1, SUM(test.k2) from test GROUP BY test.k1 ' \
           'HAVING count(test.k5) > 260 ORDER BY test.k1'
    runner.check(line)
    line = 'SELECT test.k1 FROM test GROUP BY test.k1 HAVING test.k1 IN ' \
           '(SELECT baseall.k1 FROM baseall GROUP BY baseall.k1 HAVING SUM(test.k2) > 20)'
    runner.checkwrong(line)
    line = 'SELECT test.k1, SUM(k2) AS sum  FROM test GROUP BY test.k1 HAVING test.k1 IN ' \
           '(SELECT baseall.k1 FROM baseall GROUP BY baseall.k1 HAVING baseall.k1+sum > 20)'
    runner.checkwrong(line)
    line = 'SELECT COUNT(*), k1 FROM test GROUP BY k1 HAVING ' \
           '(SELECT MIN(k1) FROM baseall WHERE k1 = count(*)) > 1'
    runner.checkwrong(line)


def test_having_subquery_1():
    """
    {
    "title": "having子查询测试",
    "describe": "test simple having subquery",
    "tag": "p0,fuzz,function"
    }
    """
    line = 'select k1, count(*) from test group by k1 having ' \
           'count(*) > (select k1 from baseall where k1 = 1) order by k1'
    runner.check(line)
    line = 'select k1, count(*) from test group by k1 having ' \
           'count(*) > (select k1 from baseall) order by k1'
    runner.checkwrong(line)
    line = 'select k1, count(*) cnt from test group by k1 having ' \
           'cnt > (select k1 from baseall where k1 = 1) order by k1'
    runner.check(line)
    line = 'select k1, count(*) cnt from test group by k1 having ' \
           'cnt > (select k1 from baseall where k1 < 1) order by k1'
    runner.check(line)
    line = 'select k1, count(*) cnt from test group by k1 having ' \
           'cnt > (select NULL from baseall where k1 = 1) order by k1'
    runner.check(line)
    line = 'select k1, count(*) cnt from test where k1 > 1000 group by k1 having' \
           ' count(*) > (select k1 from baseall where k1=1) order by k1'
    runner.check(line)
    line = 'select k1, count(*) cnt from test group by k1 having ' \
           'count(*) > (select max(k1) from baseall) order by k1'
    runner.check(line)
    line = 'select k1, count(*) cnt from test group by k1 having ' \
           'count(*) in (select k1 from baseall) order by k1'
    runner.check(line)
    # 不相关子查询
    line = 'select a.k1, sum(a.k2), sum(b.k2) from test a join baseall b on a.k1=b.k1 ' \
           'group by k1 having sum(a.k2) > (select max(k1) from baseall)'
    runner.check(line, True)
    line = 'select a.k1, sum(a.k2), sum(b.k2) from test a join baseall b on a.k1=b.k1 ' \
           'group by k1 having a.k1 = (select max(k1) from baseall)'
    runner.check(line)
    # Subqueries in OR predicates
    line = 'select a.k1, sum(a.k2), sum(b.k2) from test a join baseall b on a.k1=b.k1 ' \
           'group by k1 having a.k1 = (select max(k1) from baseall) or sum(a.k2) > 0 order by 1'
    runner.check(line)
    line = 'select a.k1, sum(a.k2), sum(b.k2) from test a join baseall b on a.k1=b.k1 ' \
           'group by k1 having a.k1 = (select max(k1) from baseall) and sum(a.k2) < 0'
    runner.check(line)
    # Subquery of binary predicate must return a single column
    line = 'select a.k1, sum(a.k2), sum(b.k2) from test a join baseall b on a.k1=b.k1 ' \
           'group by k1 having a.k1 = (select max(k1),max(k2) from baseall) and sum(a.k2) > 0'
    runner.checkwrong(line)
    line = 'select a.k1, sum(a.k2), sum(b.k2) from test a join baseall b where a.k1=b.k1 ' \
           'group by k1 having a.k1 = (select max(k1) from baseall) and sum(a.k2) < 0'
    runner.check(line)
    # A subquery must contain a single select block: (SELECT  FROM `baseall` UNION SELECT )  maybe bug
    line = 'select b.k1, sum(a.k2), sum(b.k2), count(a.k1) from test a left join baseall b ' \
           'on a.k1=b.k1 group by b.k1 having b.k1 in ' \
           '(select max(k1) from baseall union select null) and sum(a.k2) < 0'
    runner.checkwrong(line)
    line = 'select b.k1, sum(a.k2), sum(b.k2), count(a.k1) from test a left join baseall b ' \
           'on a.k1=b.k1 group by b.k1 having b.k1 is null'
    runner.check(line)
    line = 'select b.k1, sum(a.k2), sum(b.k2), count(a.k1) from test a left join baseall b ' \
           'on a.k1=b.k1 group by b.k1 having ' \
           'exists(select * from baseall where baseall.k1 = b.k1)'
    runner.checkwrong(line)
    line = 'select b.k1, sum(a.k2), sum(b.k2), count(a.k1) from test a left join baseall b ' \
           'on a.k1=b.k1 group by b.k1 having exists(select * from baseall where k1 = 1)'
    runner.check(line, True)
    # exists vs null
    line = 'SELECT k1 FROM test GROUP BY k1 HAVING not ' \
           'exists(select sum(k1) from baseall where k1 = 0) ORDER BY k1'
    runner.check(line)
    line = 'SELECT k1 FROM test GROUP BY k1 HAVING ' \
           'exists(select sum(k1) from baseall where k1 = 0) ORDER BY k1'
    runner.check(line)
    # order by
    # mysql 5.6: This version of MySQL doesn't yet support 'LIMIT & IN/ALL/ANY/SOME subquery'
    line1 = 'select k1, count(*) cnt from test group by k1 having k1 in ' \
            '(select k1 from baseall order by k1 limit 2) order by k1'
    line2 = 'select k1, count(*) cnt from test group by k1 having k1 in ' \
            '(1, 2) order by k1'
    runner.check2(line1, line2)
    line1 = 'select k1, count(*) cnt from test group by k1 having k1 in ' \
            '(select k1 from baseall order by k1 limit 2) order by k1 limit 5 offset 3'
    line2 = 'select k1, count(*) cnt from test group by k1 having k1 in ' \
            '(1, 2) order by k1 limit 5 offset 3'
    runner.check2(line1, line2)
    line1 = 'select k1, count(*) cnt from test group by k1 having k1 in ' \
            '(select k1 from baseall order by k1 desc limit 2 offset 3) order by k1'
    line2 = 'select k1, count(*) cnt from test group by k1 having k1 in (11, 12) order by k1'
    runner.check2(line1, line2)
    # as & window
    # HAVING clause must not contain analytic expressions
    line = 'select k6, k3, sum(k3) over (partition by k6 order by k3 ' \
           'range between unbounded preceding and unbounded following) win ' \
           'from baseall having win > (select sum(k3) from baseall where k1 % 2 = 0)' \
           ' order by k6, k3'
    runner.checkwrong(line)
    line = 'select k6, k3, sum(k3) win from baseall group by k6, k3 having ' \
           'win > (select sum(k3) from baseall where k1 % 2 = 0) order by k6, k3'
    runner.check(line)
    line = 'select a.k1 ak1, a.k2, b.k1 bk1 from baseall b join test a on a.k1 = b.k1 ' \
           'where a.k2 > 0 group by a.k1, a.k2, b.k1 having ' \
           'a.k1 = (select max(k1) from bigtable) and bk1 > 0'
    runner.check(line, True)
    # from
    line = 'select * from (select k1 from baseall group by k1 having ' \
           'count(*) >= (select min(k1) from baseall)) t, test a where t.k1 > 5 and t.k1 = a.k1'
    runner.check(line, True)
    # with
    line1 = 'select k6, k3, sum(k3) win from baseall group by k6, k3 having ' \
            'win > (with t as (select * from baseall) select sum(k3) from t where k1 % 2 = 0) ' \
            'order by k6, k3'
    line2 = 'select k6, k3, sum(k3) win from baseall group by k6, k3 having ' \
            'win > (select sum(k3) from baseall where k1 % 2 = 0) order by k6, k3'
    runner.check2(line1, line2)


@pytest.mark.skip()
def test_view_subselect():
    """
    {
    "title": "view中的子查询",
    "describe": "test view with subselect",
    "tag": "p1,function"
    }
    """
    # view
    # todo failed to init view stmt
    line = 'drop view if exists tt'
    runner.init(line)
    line = 'create view tt as select k1, count(*) cnt from test group by k1 ' \
           'having k1 in (select k1 from baseall order by k1 limit 2) order by k1'
    runner.init(line)
    line = 'select * from tt'
    runner.check(line)
    line = 'drop view if exists tt'
    runner.init(line)
    line = 'create view t as select case(select count(distinct b.k1) from test b)' \
           ' when 255 then k1 else "no" end a from bigtable b'
    runner.init(line)
    line = 'select * from tt'
    runner.check(line)
    line = 'drop view if exists tt'
    runner.init(line)


def test_case_when():
    """
    {
    "title": "test select case when + subselect",
    "describe": "test basic select case when + subselect",
    "tag": "p1,fuzz,function"
    }
    """
    # Only support subquery in binary predicate in case statement.
    line = 'select case k1 when (select max(k1) from baseall) then null else k1 end as a ' \
           'from baseall order by a'
    runner.check(line, True)
    # Subquery is not supported in the select list.
    line = 'select *, (select case when k1=1 then -1 else null end) as a from baseall'
    runner.checkwrong(line)
    # Multiple subqueries are not supported in expression
    line = "select * from baseall where " \
           "(case when k1 in (select k1 from test a) then k1 else null end) " \
           "in (select k1 from test b)"
    runner.checkwrong(line)
    # Unsupported IN predicate with a subquery
    line = 'select * from baseall where ' \
           '(case when k1 in (select k1 from test a) then k1 else null end) in (1,2,3)'
    runner.checkwrong(line)
    # WHERE clause requires return type 'BOOLEAN'. Actual type is 'TINYINT'.
    line = 'select * from baseall where case when k1 in ' \
           '(select k1 from test a) then k1 else null end'
    runner.checkwrong(line)
    # Non-scalar subquery is not supported in expression
    line = 'select * from baseall where case when k1 in ' \
           '(select k1 from test a) then true else false end'
    runner.checkwrong(line)
    # Subquery is not supported in the select list
    line = """SELECT (SELECT 1)"""
    runner.checkwrong(line)
    line = "select case(select count(distinct k6) from baseall) " \
           "when 2 then 'yes' else 'no' end a"
    runner.check(line)
    line = 'select case(select count(distinct k6) from baseall) ' \
           'when 2 then "yes" else "no" end a from baseall'
    runner.check(line)
    line = "select case(select count(*) from baseall) when 2 " \
           "then 'yes' else 'no' end a from bigtable"
    runner.check(line)
    # Unknown column 'k1' in 'baseall'
    line = 'select case(select count(distinct k6) from baseall) ' \
           'when 2 then "yes" else "no" end a from bigtable where bigtable.k1=baseall.k1'
    runner.checkwrong(line)
    line = 'select case(select count(distinct b.k1) from test b) ' \
           'when 255 then k1 else "no" end a from bigtable b order by a'
    runner.check(line)
    # Incompatible return types 'bigint(20)' and 'ARRAY<tinyint(4)>' of exprs
    # -- when的子查询非标量
    line = 'select case(select count(distinct b.k1) from test b) when ' \
           '(select k1 from baseall) then k1 else "no" end a from bigtable b'
    runner.checkwrong(line)
    # Subquery in case-when must return scala type
    line = 'select case(select count(distinct b.k1) from test b group by k1 having k1=1) ' \
           'when 255 then k1 else "no" end a from bigtable b order by a'
    runner.checkwrong(line)
    # Subquery in case-when must return scala type
    line = 'select case(select k1 from test b group by k1 having k1=1) ' \
           'when 255 then k1 else "no" end a from bigtable b order by a'
    runner.checkwrong(line)
    # Subquery in case-when must return scala type
    # -- 子查询返回空集
    line = 'select case(select k1 from baseall where k1 = 16) ' \
           'when 0 then k1 else "no" end a from bigtable b order by a'
    runner.checkwrong(line)


def test_case_when_1():
    """
    {
    "title": "test select case when + subselect",
    "describe": "test basic select case when + subselect",
    "tag": "p1,fuzz,function"
    }
    """
    # Expected EQ 1 to be returned by expression
    line = 'select case(select k1 from baseall where k1 = 16 limit 1) ' \
           'when 0 then k1 else "no" end a from bigtable b order by a'
    runner.checkwrong(line)
    # Incompatible return types 'ARRAY<tinyint(4)>' and 'decimal(9, 0)' of exprs
    line = 'select case (select k1 from baseall where k1 between 1 and 2) ' \
           'when 1.5 then k1 else "no" end a from bigtable b order by a'
    runner.checkwrong(line)
    line = 'select case (select k1 from baseall where k1 between 1 and 2 limit 1) ' \
           'when 1.5 then k1 else "no" end a from bigtable b order by a'
    runner.check(line)
    # Only support subquery in binary predicate in case statement.
    line = 'select case when k1 in (select k1 from baseall) then "true" ' \
           'else k1 end a from baseall order by a'
    runner.checkwrong(line)
    # 不支持in
    # Only support subquery in binary predicate in case statement.
    line = 'select case when k1 in (select k1 + 5 from baseall) then "true" ' \
           'else k1 end a from baseall order by a'
    runner.checkwrong(line)
    # 不支持not in，Only support subquery in binary predicate in case statement.
    line = 'select case when k1 not in (select k1 + 5 from baseall) then "true" ' \
           'else k1 end a from baseall order by a'
    runner.checkwrong(line)
    # Subquery in case-when must return scala type(修改后新增)??
    line = 'select case when k1 > (select k1 + 5 from baseall where k1 = 1) then "true" ' \
           'when k1 > (select k1 + 10 from baseall where k1 = 1) then "really true" ' \
           'else k1 end a from baseall order by a'
    # runner.checkwrong(line)
    line = 'select case when k1 > (select k1 + 5 from baseall where k1 = 1) then "true" ' \
           'when k1 > (select k1 + 2 from baseall where k1 = 1) then "really true" ' \
           'else k1 end a from baseall order by a'
    runner.checkwrong(line)
    line = 'select case k1 when (select null) then "empty" else "p_test" end a ' \
           'from (select k1 from bigtable union select null) c order by a'
    runner.check(line)
    line = 'select case k1 when (select null) then "empty" else "p_test" end a ' \
           'from bigtable order by a'
    runner.check(line)
    line = 'select case k1 when (select 1) then "empty" else "p_test" end a ' \
           'from bigtable order by a'
    runner.check(line)
    # 不支持exists，Only support subquery in binary predicate in case statement.
    line = 'select case k1 when exists (select 1) then "empty" else "p_test" end a ' \
           'from bigtable order by a'
    runner.checkwrong(line)
    # 不支持exists子查询
    line = 'select case when exists (select 1) then "empty" else "p_test" end a ' \
           'from bigtable'
    runner.checkwrong(line)
    line = "SELECT CASE WHEN (SELECT 1) = 1 THEN (SELECT 'a') ELSE (SELECT 'a') END AS c1, " \
           "CASE WHEN (SELECT 1) = 1 THEN (SELECT 'a') ELSE (SELECT 'a') END AS c2, " \
           "CASE WHEN (SELECT 1) = 1 THEN (SELECT 'a') ELSE (SELECT 1)  END AS c3," \
           "CASE WHEN (SELECT 1) = 1 THEN (SELECT 1) ELSE (SELECT 'a') END AS c4, " \
           "CASE WHEN (SELECT 1) = 1 THEN (SELECT 'a') ELSE (SELECT 1.0) END AS c5, " \
           "CASE WHEN (SELECT 1) = 1 THEN (SELECT 1.0) ELSE (SELECT 'a') END AS c6, " \
           "CASE WHEN (SELECT 1) = 1 THEN (SELECT 1) ELSE (SELECT 1.0) END AS c7, " \
           "CASE WHEN (SELECT 1) = 1 THEN (SELECT 1.0) ELSE (SELECT 1) END AS c8, " \
           "CASE WHEN (SELECT 1) = 1 THEN (SELECT 1.0) END AS c9, " \
           "CASE WHEN (SELECT 1) = 1 THEN (SELECT 0.1e1) else (SELECT 0.1) END AS c10, " \
           "CASE WHEN (SELECT 1) = 1 THEN (SELECT 0.1e1) else (SELECT 1) END AS c11, " \
           "CASE WHEN (SELECT 1) = 1 THEN (SELECT 0.1e1) else (SELECT '1') END AS c12"
    line1 = 'select "a", "a", "a", "1", "a", "1.000000000", 1, 1, 1, 1, 1, "1.000000000"'
    # runner.check(line)
    runner.check2(line, line1)
    line = 'select CASE (select max(k10) from baseall) when (select max(k10) from test) then k10 ' \
           'when (select min(k10) from test) then null END a from baseall order by a'
    runner.check(line)
    line = 'select CASE (select max(k10) from baseall) when (select max(k10) from test) then k10 ' \
           'when (select min(k10) from test) then null END'
    runner.checkwrong(line)
    line = 'select CASE (select k10 from baseall) when (select max(k10) from test) then k10 ' \
           'when (select min(k10) from test) then null END cs from baseall'
    runner.checkwrong(line)
    line = 'select CASE (select k10 from baseall limit 1) ' \
           'when (select max(k10) from test) then k10 ' \
           'when (select min(k10) from test) then null else "1010-01-01" END cs from baseall'
    runner.check(line)
    line = 'select case (select k1,k2 from baseall) when k1 then "true" end a from bigtable'
    runner.checkwrong(line)
    line = 'select case (select null limit 1) when k1 then "true" end a from bigtable'
    runner.check(line)


def test_case_when_2():
    """
    {
    "title": "test select case when + subselect",
    "describe": "test basic select case when + subselect",
    "tag": "p1,fuzz,function"
    }
    """
    # Expected EQ 1 to be returned by expression
    line = 'select case when k1 > (select k1 from baseall where k1 = 16 limit 1) ' \
           'then "true" end a from bigtable'
    runner.checkwrong(line)
    line = 'select case when (select sum(k1) from baseall where k1 = 16) is null ' \
           'then "true" else "false" end a from bigtable'
    runner.check(line)
    line = 'select case when (select count(*) from baseall where k1 = 16) is null ' \
           'then "true" else "false" end a from bigtable'
    runner.check(line)
    line = 'select case when (select sum(k1) from baseall where k1 = 16) is not null ' \
           'then "true" else "false" end a from bigtable'
    runner.check(line)
    line = 'select case when (select count(*) from baseall where k1 = 16) is not null ' \
           'then k1 else "false" end a from bigtable order by a'
    runner.check(line)
    line = 'select case when (select count(*) from baseall where k1 > 3) >= k1 then k1 ' \
           'else "false" end a from bigtable order by a'
    runner.check(line)
    line = 'select case when (select count(*) from baseall b where a.k1 = b.k1) >= k1 then k1 ' \
           'else "false" end a from bigtable a'
    runner.checkwrong(line)
    line = 'select case when (select count(*) from baseall b, bigtable a where a.k1 != b.k1) > k1' \
           ' then k1 else "false" end a from test order by a'
    runner.check(line)
    line = 'select case when (select count(*) cnt from baseall b, test a where a.k1 = b.k1 ' \
           'group by a.k1 order by cnt limit 1 offset 3 ) >= k1 then k1 else "false" end a ' \
           'from test order by a'
    # Expected EQ 1 to be returned by expression
    line = 'select case when (select sum(a.k1) sum from baseall b left join test a ' \
           'on a.k1 = b.k1 order by sum limit 1 offset 3 ) >= k1 then k1 ' \
           'else "false" end a from test order by a'
    runner.checkwrong(line)
    line = 'select case when (select count(*) from ' \
           '(select k1 from baseall union select null) sub) >= k2 then k1 ' \
           'else "false" end a from baseall order by a'
    runner.check(line)
    line = 'select case when (select count(*) from ' \
           '(select * from (select * from (select * from baseall) a) b) c) >= k2 then k1 ' \
           'else "false" end a from baseall order by a'
    runner.check(line)
    line = 'select case when (select max(k1) from baseall) < k1 then "plus a" ' \
           'when k1 < (select min(k1) from baseall) then "minus a" ' \
           'when k1 between (select max(k1) from baseall) and (select min(k1) from baseall) ' \
           'then "normal" end cs from test'
    runner.check(line, True)
    line = 'select count(*) from (select case when (select max(k1) from baseall) < k1 ' \
           'then "plus a" when k1 < (select min(k1) from baseall) then "minus a" ' \
           'when k1 between (select max(k1) from baseall) and (select min(k1) from baseall) ' \
           'then "normal" end cs from test) sub group by cs order by 1'
    runner.check(line)
    line = 'select k1, count(*) from test group by k1 having ' \
           'k1 > (select case when (select count(*) from baseall) > 10 then 107 end) order by k1'
    runner.check(line)
    line = 'select * from baseall where ' \
           'k1 > (select case when (select count(*) from bigtable) > 10 then 10 end) order by k1'
    runner.check(line)
    # type
    for i in [3, 4, 5, 6, 7, 8, 9, 10, 11]:
        line = 'select CASE (select max(k%s) from baseall) when (select max(k%s) from test) ' \
               'then k%s when (select min(k%s) from test) then null END a from baseall ' \
               'order by a' % (i, i, i, i)
        runner.check(line)


if __name__ == '__main__':
    setup_module()
    test_case_when()
    test_case_when_1()
    test_case_when_2()
    # test_having_subquery()
    # test_having_subquery_1()


