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

suite("window_skew_rewrite") {
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "drop table if exists test_skew_window"
    sql """create table test_skew_window(a int, c varchar(100), b int, d varchar(20)) distributed by hash(a) buckets 32 properties("replication_num"="1");"""
    sql """
    INSERT INTO test_skew_window VALUES
    (1, 'value1', 100, 'd001'),
    (2, 'value2', 200, 'd002'),
    (3, 'value3', 300, 'd003'),
    (4, 'value4', 400, 'd004'),
    (5, 'value5', 500, 'd005'),
    (6, 'value6', 600, 'd006'),
    (7, 'value7', 700, 'd007'),
    (8, 'value8', 800, 'd008'),
    (9, 'value9', 900, 'd009'),
    (10, 'value10', 1000, 'd010');
    """

    // 1.only one window expression
    qt_one_window_expr """select sum(w) from (select sum(a) over(partition by [skew] b order by d rows between unbounded PRECEDING and current row) w from  test_skew_window ) t;"""
    // 2.two window expression，belong to same same windowFrameGroup
    qt_two_window_expr_in_same_windowFrameGroup """select sum(w1), sum(w3) from (select 
        sum(a) over(partition by [skew] b order by d) w1,
        rank() over(partition by [skew] b order by d) w3
        from  test_skew_window ) t; """
    qt_two_window_expr_in_same_windowFrameGroup_first_one_has_skew_hint """
        select sum(w1), sum(w3) from (select
        sum(a) over(partition by [skew] b order by d) w1,
        rank() over(partition by  b order by d) w3
        from  test_skew_window ) t;"""
    qt_two_window_expr_in_same_windowFrameGroup_last_one_has_skew_hint """
        select sum(w1), sum(w3) from (select
        sum(a) over(partition by b order by d) w1,
        rank() over(partition by [skew] b order by d) w3
        from  test_skew_window ) t;"""

    // 3.two window expression，belong to same orderGroup
    qt_two_window_expr_in_same_orderGroup """
        select sum(w1), sum(w2) from (select sum(a) over(partition by [skew] b order by d rows between unbounded PRECEDING and 1 FOLLOWING) w1,
        min(a) over(partition by [skew] b order by d rows between unbounded PRECEDING and 2 FOLLOWING) w2
        from  test_skew_window) t;"""

    qt_two_window_expr_in_same_orderGroup_first_one_has_skew_hint """
        select sum(w1), sum(w2) from (select sum(a) over(partition by [skew] b order by d rows between unbounded PRECEDING and 1 FOLLOWING) w1,
        min(a) over(partition by b order by d rows between unbounded PRECEDING and 2 FOLLOWING) w2
        from  test_skew_window) t;"""

    qt_two_window_expr_in_same_orderGroup_last_one_has_skew_hint """
        select sum(w1), sum(w2) from (select sum(a) over(partition by b order by d rows between unbounded PRECEDING and 1 FOLLOWING) w1,
        min(a) over(partition by [skew] b order by d rows between unbounded PRECEDING and 2 FOLLOWING) w2
        from  test_skew_window) t;"""

    // 4.two orderGroup，belong to same partitionGroup,
    qt_two_orderGroup_in_same_partitionGroup"""
        select sum(w1), sum(w2) from (select sum(a) over(partition by [skew] b order by d rows between unbounded PRECEDING and 1 FOLLOWING) w1,
        min(a) over(partition by [skew] b order by a rows between unbounded PRECEDING and 2 FOLLOWING) w2
        from  test_skew_window) t;
    """

    qt_two_orderGroup_in_same_partitionGroup_first_has_hint"""
    select sum(w1), sum(w2) from (select sum(a) over(partition by [skew] b order by d rows between unbounded PRECEDING and 1 FOLLOWING) w1,
    min(a) over(partition by b order by a rows between unbounded PRECEDING and 2 FOLLOWING) w2
    from  test_skew_window) t;
    """

    qt_two_orderGroup_in_same_partitionGroup_last_has_hint"""
    select sum(w1), sum(w2) from (select sum(a) over(partition by b order by d rows between unbounded PRECEDING and 1 FOLLOWING) w1,
    min(a) over(partition by [skew] b order by a rows between unbounded PRECEDING and 2 FOLLOWING) w2
    from  test_skew_window) t;
    """

    // 5. not belong to same partitionGroup
    qt_two_partitionGroup """
    select sum(w1),sum(w2) from (select sum(a) over(partition by [skew] b order by d rows between unbounded PRECEDING and 1 FOLLOWING) w1,
    min(a) over(partition by [skew] c order by a rows between unbounded PRECEDING and 2 FOLLOWING) w2
    from  test_skew_window) t;
    """
    qt_two_partitionGroup_first_has_hint"""
    select sum(w1),sum(w2) from (select sum(a) over(partition by [skew] b order by d rows between unbounded PRECEDING and 1 FOLLOWING) w1,
    min(a) over(partition by c order by a rows between unbounded PRECEDING and 2 FOLLOWING) w2
    from  test_skew_window) t;
    """
    qt_two_partitionGroup_last_has_hint"""
    select sum(w1),sum(w2) from (select sum(a) over(partition by b order by d rows between unbounded PRECEDING and 1 FOLLOWING) w1,
    min(a) over(partition by [skew] c order by a rows between unbounded PRECEDING and 2 FOLLOWING) w2
    from  test_skew_window) t;
    """

    qt_without_order_key """
    SELECT 
    b,
    SUM(a) OVER(PARTITION BY [skew] b) AS w,
    MAX(a) OVER(PARTITION BY [skew] c) as m
    FROM test_skew_window order by 1,2,3 limit 20
    """

    // shape
    qt_one_window_expr_shape """explain shape plan select sum(w) from (select sum(a) over(partition by [skew] b order by d rows between unbounded PRECEDING and current row) w from  test_skew_window ) t;"""
    qt_two_window_expr_in_same_windowFrameGroup_shape """explain shape plan select sum(w1), sum(w3) from (select 
        sum(a) over(partition by [skew] b order by d) w1,
        rank() over(partition by [skew] b order by d) w3
        from  test_skew_window ) t; """
    qt_two_window_expr_in_same_windowFrameGroup_first_one_has_skew_hint_shape """
        explain shape plan select sum(w1), sum(w3) from (select
        sum(a) over(partition by [skew] b order by d) w1,
        rank() over(partition by  b order by d) w3
        from  test_skew_window ) t;"""
    qt_two_window_expr_in_same_windowFrameGroup_last_one_has_skew_hint_shape """
        explain shape plan select sum(w1), sum(w3) from (select
        sum(a) over(partition by b order by d) w1,
        rank() over(partition by [skew] b order by d) w3
        from  test_skew_window ) t;"""

    qt_two_window_expr_in_same_orderGroup_shape """
        explain shape plan select sum(w1), sum(w2) from (select sum(a) over(partition by [skew] b order by d rows between unbounded PRECEDING and 1 FOLLOWING) w1,
        min(a) over(partition by [skew] b order by d rows between unbounded PRECEDING and 2 FOLLOWING) w2
        from  test_skew_window) t;"""

    qt_two_window_expr_in_same_orderGroup_first_one_has_skew_hint_shape """
        explain shape plan select sum(w1), sum(w2) from (select sum(a) over(partition by [skew] b order by d rows between unbounded PRECEDING and 1 FOLLOWING) w1,
        min(a) over(partition by b order by d rows between unbounded PRECEDING and 2 FOLLOWING) w2
        from  test_skew_window) t;"""

    qt_two_window_expr_in_same_orderGroup_last_one_has_skew_hint_shape """
        explain shape plan select sum(w1), sum(w2) from (select sum(a) over(partition by b order by d rows between unbounded PRECEDING and 1 FOLLOWING) w1,
        min(a) over(partition by [skew] b order by d rows between unbounded PRECEDING and 2 FOLLOWING) w2
        from  test_skew_window) t;"""

    qt_two_orderGroup_in_same_partitionGroup_shape"""
        explain shape plan select sum(w1), sum(w2) from (select sum(a) over(partition by [skew] b order by d rows between unbounded PRECEDING and 1 FOLLOWING) w1,
        min(a) over(partition by [skew] b order by a rows between unbounded PRECEDING and 2 FOLLOWING) w2
        from  test_skew_window) t;
    """

    qt_two_orderGroup_in_same_partitionGroup_first_has_hint_shape"""
    explain shape plan select sum(w1), sum(w2) from (select sum(a) over(partition by [skew] b order by d rows between unbounded PRECEDING and 1 FOLLOWING) w1,
    min(a) over(partition by b order by a rows between unbounded PRECEDING and 2 FOLLOWING) w2
    from  test_skew_window) t;
    """

    qt_two_orderGroup_in_same_partitionGroup_last_has_hint_shape"""
    explain shape plan select sum(w1), sum(w2) from (select sum(a) over(partition by b order by d rows between unbounded PRECEDING and 1 FOLLOWING) w1,
    min(a) over(partition by [skew] b order by a rows between unbounded PRECEDING and 2 FOLLOWING) w2
    from  test_skew_window) t;
    """

    qt_two_partitionGroup_shape """
    explain shape plan select sum(w1),sum(w2) from (select sum(a) over(partition by [skew] b order by d rows between unbounded PRECEDING and 1 FOLLOWING) w1,
    min(a) over(partition by [skew] c order by a rows between unbounded PRECEDING and 2 FOLLOWING) w2
    from  test_skew_window) t;
    """
    qt_two_partitionGroup_first_has_hint_shape"""
    explain shape plan select sum(w1),sum(w2) from (select sum(a) over(partition by [skew] b order by d rows between unbounded PRECEDING and 1 FOLLOWING) w1,
    min(a) over(partition by c order by a rows between unbounded PRECEDING and 2 FOLLOWING) w2
    from  test_skew_window) t;
    """
    qt_two_partitionGroup_last_has_hint_shape"""
    explain shape plan select sum(w1),sum(w2) from (select sum(a) over(partition by b order by d rows between unbounded PRECEDING and 1 FOLLOWING) w1,
    min(a) over(partition by [skew] c order by a rows between unbounded PRECEDING and 2 FOLLOWING) w2
    from  test_skew_window) t;
    """

    sql """drop table if exists test_skew_join"""
    sql """CREATE TABLE test_skew_join (
            a INT,
                    join_key INT,
            e VARCHAR(100)
    ) DISTRIBUTED BY HASH(a) BUCKETS 32 PROPERTIES("replication_num"="1");"""

    sql """INSERT INTO test_skew_join VALUES
    (1, 1, 'join1'),
    (2, 1, 'join2'),
    (3, 1, 'join3'),
    (4, 2, 'join4'),
    (5, 2, 'join5'),
    (6, 3, 'join6'),
    (7, 3, 'join7'),
    (8, 3, 'join8'),
    (9, 3, 'join9'),
    (10, 4, 'join10');"""

    qt_skew_agg_group_by """
    SELECT b, SUM(w) AS total_sum
    FROM (
        SELECT 
            b,
            SUM(a) OVER(PARTITION BY [skew] b ORDER BY d ROWS UNBOUNDED PRECEDING) AS w
        FROM test_skew_window
    ) t
    GROUP BY b
    ORDER BY 1,2;
    """
    qt_skew_subquery_agg """
    SELECT AVG(w) AS avg_window
    FROM (
        SELECT 
            SUM(a) OVER(PARTITION BY [skew] b ORDER BY d) AS w
        FROM test_skew_window
    ) t;
    """
    qt_skew_join_window """
    SELECT SUM(t1.w) AS total
    FROM (
        SELECT 
            j.join_key,
            SUM(t.a) OVER(PARTITION BY [skew] j.a ORDER BY t.d) AS w
        FROM test_skew_window t
        JOIN test_skew_join j ON t.a = j.a
    ) t1;
    """

    qt_skew_multi_window_agg """
    SELECT 
        join_key,
        SUM(w1) AS sum_win1,
        AVG(w2) AS avg_win2
    FROM (
        SELECT 
            j.join_key,
            SUM(t.a) OVER(PARTITION BY [skew] j.join_key ORDER BY t.d) AS w1,
            RANK() OVER(PARTITION BY [skew] j.join_key ORDER BY t.b) AS w2
        FROM test_skew_window t
        JOIN test_skew_join j ON t.a = j.a
    ) t
    GROUP BY join_key
    ORDER BY 1,2,3;
    """

    qt_skew_window_join_condition """
    SELECT 
        t1.a,
        t2.e
    FROM (
        SELECT 
            a,
            SUM(b) OVER(PARTITION BY [skew] b ORDER BY d) AS sum_b
        FROM test_skew_window
    ) t1
    JOIN test_skew_join t2 ON t1.a = t2.a
    WHERE t1.sum_b > 0
    ORDER BY 1,2;
    """
    qt_skew_having_agg """
    SELECT 
        b,
        MAX(w) AS max_window
    FROM (
        SELECT 
            b,
            SUM(a) OVER(PARTITION BY [skew] b ORDER BY d) AS w
        FROM test_skew_window
    ) t
    GROUP BY b
    HAVING MAX(w) > 1000
    ORDER BY 1,2;
    """
}