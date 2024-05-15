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

suite("window_function") {
    sql "SET enable_nereids_planner=true"

    sql "DROP TABLE IF EXISTS window_test"

    sql "DROP TABLE IF EXISTS adj_nullable_1"

    sql "DROP TABLE IF EXISTS adj_nullable_2"

    sql """
        CREATE TABLE `window_test` (
            `c1` int NULL,
            `c2` int NULL,
            `c3` double NULL
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`c1`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        create table adj_nullable_1 (
          c1 int,
          c2 int,
          c3 int
        ) distributed by hash(c1)
        properties('replication_num'='1');
    """

    sql """
        create table adj_nullable_2 (
          c4 int not null,
          c5 int not null,
          c6 int not null
        ) distributed by hash(c4)
        properties('replication_num'='1');
    """

    sql """INSERT INTO window_test VALUES(1, 1, 1)"""
    sql """INSERT INTO window_test VALUES(1, 2, 1)"""
    sql """INSERT INTO window_test VALUES(1, 3, 1)"""
    sql """INSERT INTO window_test VALUES(2, 1, 1)"""
    sql """INSERT INTO window_test VALUES(2, 2, 1)"""
    sql """INSERT INTO window_test VALUES(2, 3, 1)"""
    sql """INSERT INTO window_test VALUES(1, 1, 2)"""
    sql """INSERT INTO window_test VALUES(1, 2, 2)"""
    sql """INSERT INTO window_test VALUES(2, 1, 2)"""
    sql """INSERT INTO window_test VALUES(2, 2, 2)"""
    sql """INSERT INTO window_test VALUES(1, 2, null)"""
    sql """INSERT INTO window_test VALUES(1, 3, null)"""
    sql """INSERT INTO window_test VALUES(1, null, 3)"""
    sql """INSERT INTO window_test VALUES(2, null, 3)"""

    sql """insert into adj_nullable_1 values(1, 1, 1);"""
    sql """insert into adj_nullable_2 values(1, 1, 1);"""

    sql "SET enable_fallback_to_original_planner=false"

    order_qt_empty_over "SELECT rank() over() FROM window_test"
    order_qt_with_star "SELECT *, rank() over(partition by c3 order by c2) FROM window_test"
    order_qt_multi_window_1 "SELECT rank() over(), row_number() over() FROM window_test"
    order_qt_multi_window_2 """
        SELECT sum(c1) over(), count(c2) over(), rank() over(partition by c3   order by c2) 
        FROM window_test
    """

    order_qt_dense_rank "SELECT dense_rank() over(partition by c3 order by c2) FROM window_test"
    order_qt_ntile "SELECT ntile(5) over(partition by c3 order by c2) FROM window_test"
    order_qt_rank "SELECT rank() over(partition by c3 order by c2) FROM window_test"
    order_qt_row_number "SELECT row_number() over(partition by c3 order by c2) FROM window_test"
    order_qt_sum "SELECT sum(c1) over(partition by c3 order by c2) FROM window_test"
    order_qt_avg "SELECT avg(c1) over(partition by c3 order by c2) FROM window_test"
    order_qt_count "SELECT count(c1) over(partition by c3 order by c2) FROM window_test"
    sql "SELECT first_value(c1) over(partition by c3 order by c2) FROM window_test"
    sql "SELECT last_value(c1) over(partition by c3 order by c2) FROM window_test"
    sql "SELECT lead(c1, 1, 111) over(partition by c3 order by c2) FROM window_test"
    sql "SELECT lag(c1, 1, 222) over(partition by c3 order by c2) FROM window_test"
    sql "SELECT lead(c1, 3, null) over(partition by c3 order by c2) FROM window_test"
    sql "SELECT lag(c1, 2, null) over(partition by c3 order by c2) FROM window_test"
    order_qt_max "SELECT max(c1) over(partition by c3 order by c2) FROM window_test"
    order_qt_min "SELECT min(c1) over(partition by c3 order by c2) FROM window_test"

    order_qt_agg_1 """
        SELECT c1+1, c2, sum(c1), sum(c1) over(partition by c2 order by c1) 
        FROM window_test 
        GROUP BY c1, c2
    """

    order_qt_agg_2 """
        SELECT c1, c2, sum(c1+1), sum(c1+1) over(partition by c2 order by c1+1) 
        FROM window_test 
        GROUP BY c1, c2
    """

    order_qt_agg_3 """
        SELECT row_number() over(partition by (grouping(c2) + grouping(c3)))
        FROM window_test
        GROUP BY ROLLUP (c2, c3)
    """

    order_qt_subquery_1 """
        SELECT *, row_number() over(partition by c1 order by c3, c2, c1, r1) as r2
        FROM (
            SELECT *, row_number() over(partition by c2 order by c3, c2, c1) as r1
            FROM window_test
        ) t
    """

    order_qt_subquery_2 """
        SELECT * 
        FROM (   
            SELECT 
                count(1) OVER (PARTITION BY c2 ORDER BY c3) + sum(c1) OVER (PARTITION BY c2 ORDER BY c3) AS total,     
                count(1) OVER (PARTITION BY c2 ORDER BY c3) AS fourcount, 
                sum(c1) OVER (PARTITION BY c2 ORDER BY c3) AS twosum 
            FROM window_test
        ) sub 
    """

    sql """ set batch_size = 3; """
    sql """ set parallel_pipeline_task_num = 8; """

    order_qt_range """
        SELECT c1, (sum(c1) over  (ORDER BY c1 range between UNBOUNDED preceding and CURRENT ROW)) FROM window_test;
    """

    order_qt_cte """
        WITH cte as (select c1 as x from window_test) 
        SELECT x, (sum(x) over  (ORDER BY x range between UNBOUNDED preceding and CURRENT ROW)) FROM cte;
    """

    order_qt_window_use_agg """
        SELECT sum(sum(c1)) over(partition by avg(c2))
        FROM window_test
    """

    order_qt_winExpr_not_agg_expr """
        select sum(c1+1), sum(c1+1) over (partition by avg(c2))
        from window_test
        group by c1, c2
    """

    order_qt_on_notgroupbycolumn """
        select sum(sum(c3)) over (partition by avg(c2) order by c1)
        from window_test
        group by c1, c2
    """

    order_qt_orderby """
        select c1, sum(c1+1), sum(c1+1) over (partition by avg(c2) order by c1)
        from window_test
        group by c1, c2
    """

    order_qt_winExpr_with_others """
        select sum(c1)/sum(c1+1) over (partition by c2 order by c1)
        from window_test
        group by c1, c2
    """

    order_qt_winExpr_with_others2"""
        select sum(c1)/sum(c1+1) over (partition by c2 order by c1)
        from window_test
        group by c1, c2
    """

    // test adjust nullable on window
    sql """
        select 
          count(c1) over (partition by c4),
          coalesce(c5, sum(c2) over (partition by c3))
        from
          adj_nullable_1
          left join adj_nullable_2 on c1 = c4
        where c6 is not null;
    """

    // distinct with window
    sql """
        SELECT
              DISTINCT dt,
              count(dt2) over (partition by dt) as num
              from
              (select 1 as dt,2 as dt2)t
        group by dt,dt2"""
}
