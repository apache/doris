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
    sql """create table test_skew_window(a int, c varchar(100), b int, d varchar(20)) distributed by hash(a) buckets 32 properties("replication_num"="1");"""
    sql """insert into test_skew_window select random(1,100),'abc', 1, cast(seconds_add('0000-01-01', number) as varchar(20)) from numbers("number" = "80");"""
    sql """insert into test_skew_window select random(1,100),'abc',number,cast(seconds_add('5000-01-01', number) as varchar(20)) from numbers("number" = "20");"""

    // 1.只有一个window expression
    qt_one_window_expr """select sum(w) from (select sum(a) over(partition by [skew] b order by d rows between unbounded PRECEDING and current row) w from  test_skew_window ) t;"""
    // 2.有两个window expression，属于一个windowFrameGroup
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

    // 3.有两个window expression，属于一个orderGroup
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

    // 4.有两个orderGroup，属于一个partitionGroup,
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

    // 5.不属于一个partitionGroup
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
}