// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("nereids_cte") {

    sql """
        DROP TABLE IF EXISTS t1
    """

    sql """
        CREATE TABLE t1 (col1 varchar(11451) not null, col2 int not null, col3 int not null, col4 int not null)
        UNIQUE KEY(`col1`)
        DISTRIBUTED BY HASH(col1)
        BUCKETS 3
        PROPERTIES(
            "replication_num"="1"
        );
    """

    sql """insert into t1 values('21',5,1,7)    """
    sql """insert into t1 values('7',9,1,5);    """
    sql """insert into t1 values('3',5,3,4);    """
    sql """insert into t1 values('9',8,0,7);    """

    sql """insert into t1 values('1',5,1,7);"""        
    sql """insert into t1 values('51',9,1,5);"""
    sql """insert into t1 values('6',5,3,4);"""
    sql """insert into t1 values('933',8,0,7);"""

    sql """set enable_nereids_planner=true"""

    // test cte inline
    qt_sql """explain shape plan with  cte as (select col2, col1 from t1) select * from cte,t1;"""

    // test cte materialize : use cte in inline view
    qt_sql """explain shape plan with  cte1 as (select col2, col1 from t1) select * from (SELECT * FROM cte1) v1, cte1"""

    // use cte in scalar query
    qt_sql """explain shape plan with  cte as (select col2, col1 from t1)  SELECT col1 FROM cte group by col1 having sum(col2) > (select 0.05 * avg(col2) from cte )"""

    // test filter push down
    qt_sql """explain shape plan with  cte as (select col2, col1 from t1) select * from cte t1,  cte t2 where t1.col2 = 3 and t2.col1 = 2 """

    // test filter of inline view push down
    qt_sql """explain shape plan with  cte1 as (select col2, col1 from t1) select * from (SELECT * FROM cte1 WHERE col2 = 3) v1, cte1 WHERE cte1.col2 = 4"""

    // test filter of scalar query push down
    qt_sql """explain shape plan with  cte as (select col2, col1 from t1)  SELECT col1 FROM cte group by col1 having sum(col2) > (select 0.05 * avg(col2) from cte where col2 > 1 )"""

    // test filter of scalar query  push down and column prune
    qt_sql """explain shape plan with  cte as (select col2, col3, col4, col1 from t1)  SELECT col1 FROM cte group by col1 having sum(col2) > (select 0.05 * avg(col4) from cte where col2 > 1 )"""

    // test project prune of inline vew
    qt_sql """explain shape plan with  cte1 as (select col2, col1, col3, col4 from t1) select v1.col1, v2.col2 from (select col1 FROM cte1) v1, cte1 v2"""

    // test cte reference another cte, partial inline
    qt_sql """explain rewritten plan WITH cte1 AS (select * from t1), 
                cte2 AS (select * from cte1),
                cte3 AS (  select col1, col2, col3 from cte1
                UNION  select col2 + 1, col3 + 2 , col4 from cte1)
                select * from cte3;"""

    // define cte in sub query
    qt_sql """
        explain select * from
                (with cte1 as (select * from t1)
                select t1.* from cte1 t1 join cte1 t2 on t1.col2 = t2.col2) v
                where v.col3 = 2;
    """

    // use cte in union
    qt_sql """
        explain WITH cte1 AS (SELECT * FROM t1) SELECT * FROM cte1 UNION SELECT * FROM cte1;
    """

    // test cte reference another cte first cte should be materialized
    qt_sql """
         explain WITH cte1 AS (SELECT * FROM t1), cte2 AS ( SELECT * FROM cte1 UNION SELECT * FROM cte1) SELECT * FROM cte2;
    """

    // test useless cte
    qt_sql """
         explain WITH cte1 AS (SELECT * FROM t1) SELECT * FROM t1;
    """

    // test nested cte, all inline
    qt_sql """
        explain
        WITH cte1 AS (WITH cte2 AS (SELECT * FROM t1 UNION ALL SELECT * FROM t1) SELECT * FROM cte2) 
        SELECT * FROM cte1;
    """

    // test multiple nested cte, all inline
    qt_sql """
        explain
        WITH cte0 AS (SELECT * FROM t1 UNION ALL SELECT * FROM t1), 
        cte1 AS (WITH cte2 AS (WITH cte3 AS (SELECT * FROM cte0)
        SELECT * FROM cte3) SELECT * FROM cte0) 
        SELECT * FROM cte1;
    """

    // test multiple nested cte, partial inline
    qt_sql """
        explain
        WITH cte0 AS (SELECT * FROM t1 UNION ALL SELECT * FROM t1), 
        cte1 AS (WITH cte2 AS (WITH cte3 AS (SELECT * FROM cte0)
        SELECT * FROM cte3) SELECT * FROM cte0) 
        SELECT * FROM cte1 UNION SELECT * FROM cte1;
    """

    // test predicate that couldn't be putted down
    qt_sql """
        explain
        WITH cte0 AS (SELECT * FROM t1)
        SELECT * FROM cte0 WHERE col3 < 5 UNION ALL  SELECT * FROM cte0 WHERE col2 > 1
    """

    // test project that couldn't be pruned
    qt_sql """
        explain 
        WITH cte0 AS (SELECT * FROM t1)
        SELECT col2 FROM cte0 UNION ALL SELECT col3 FROM cte0
    """

    // use cte in join, slots in join condition shouldn't be pruned
    qt_sql """
        explain 
        WITH cte0 AS (SELECT * FROM t1)
        SELECT c1.col3 FROM cte0 c1, cte0 c2 WHERE c1.col3 = c2.col3
    """

    // nested cte defined in scalar query
    qt_sql """
    explain with cte as (select col2, col1 from t1), cte2 AS (SELECT * FROM cte WHERE cte.col1 > (WITH cte3 AS (SELECT * FROM cte) SELECT sum(c1.col2) FROM cte3 c1, cte3 c2)
     LIMIT 3) SELECT c1.* FROM cte2 c1, cte2 c2;
    """
}