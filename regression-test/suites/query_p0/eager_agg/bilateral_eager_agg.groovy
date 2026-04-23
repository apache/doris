
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

suite("bilateral_eager_agg") {
    sql """
        drop table if exists t_pdajos_1;
        CREATE TABLE `t_pdajos_1` (
          `k` int NOT NULL COMMENT "join key",
          `v` int NOT NULL COMMENT "agg column on left"
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        drop table if exists t_pdajos_2;
        CREATE TABLE `t_pdajos_2` (
          `k` int NOT NULL COMMENT "join key",
          `v` int NOT NULL COMMENT "agg column on right"
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        insert into t_pdajos_1 values(1,10),(1,20),(2,30);
        insert into t_pdajos_2 values(1,100),(1,200),(2,300);
        
        drop table if exists pdagg_proj_t1;
        drop table if exists pdagg_proj_t2;
        create table pdagg_proj_t1 (
            id1 int not null,
            x   int,
            y   int,
            z   int,
            g1  int,
            g2  int,
            flag int
        )
        duplicate key(id1)
        distributed by hash(id1) buckets 1
        properties ("replication_num" = "1");
        
        create table pdagg_proj_t2 (
            id2 int not null,
            k   int,
            v   int
        )
        duplicate key(id2)
        distributed by hash(id2) buckets 1
        properties ("replication_num" = "1");
        
        insert into pdagg_proj_t1 values
            (1, 10, 1, 100, 7, 3, 1),
            (2, 20, 2, 100, 7, 4, 0),
            (3, 30, 3, 200, 8, 5, 1),
            (4, 40, 4, 200, 8, 6, 0);
        
        insert into pdagg_proj_t2 values
            (1, 10, 1000),
            (2, 10, 2000),
            (3, 20, 3000),
            (5, 30, 5000);
    """

//    sql "set disable_nereids_rules='PUSH_DOWN_AGG_THROUGH_JOIN';"
//    sql "SET eager_aggregation_mode = -1;"
    order_qt_2_join """
        SELECT
          t1.k,
          count(t1.v) AS lcount,
          sum(t2.v) AS rsum,
          sum(t3.v) as 3sum
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        inner join t_pdajos_2 t3 on t2.k=t3.k
        GROUP BY t1.k;
    """

    order_qt_2_join_count_star"""
        SELECT
          t1.k,
          count(t1.v) AS lcount,
          sum(t2.v) AS rsum,
          sum(t3.v) as 3sum,
          count(*) as cntstar
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        inner join t_pdajos_2 t3 on t2.k=t3.k
        GROUP BY t1.k;
    """

    order_qt_push_one_side"""
        SELECT
        t1.k,
        count(t1.v) AS lcount,
        sum(t1.v) as lsum,
        min(t1.v) as lmin
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        GROUP BY t1.k;
    """


    order_qt_sum_to_2_side """
        select t2.k, sum(if(t1.x==0,t1.y,t2.v)),sum(t1.y)
        from pdagg_proj_t1 t1
        inner join pdagg_proj_t2 t2 
        group by t2.k;
        select * from pdagg_proj_t2;
    """


    // test session variables force_eager_agg_hint

    sql "set force_eager_agg_hint='count:t1.v=push;sum:t2.v=nopush;sum:t3.v=nopush';"
    order_qt_push_one_nopush_two """
        SELECT
          t1.k,
          count(t1.v) AS lcount,
          sum(t2.v) AS rsum,
          sum(t3.v) as 3sum
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        inner join t_pdajos_2 t3 on t2.k=t3.k
        GROUP BY t1.k;
    """

    sql "set force_eager_agg_hint='count:t1.v=push;sum:t2.v=push;sum:t3.v=nopush';"
    order_qt_push_two_nopush_one """
        SELECT
          t1.k,
          count(t1.v) AS lcount,
          sum(t2.v) AS rsum,
          sum(t3.v) as 3sum
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        inner join t_pdajos_2 t3 on t2.k=t3.k
        GROUP BY t1.k;
    """

    sql "set force_eager_agg_hint='count:t1.v=nopush;sum:t2.v=push;sum:t3.v=nopush';"
    order_qt_count_nopush_other_push """
        SELECT /*+ NO_USE_CBO_RULE(PUSH_DOWN_AGG_THROUGH_JOIN_ONE_SIDE) */
          t1.k,
          count(t1.v) AS lcount,
          sum(t2.v) AS rsum,
          sum(t3.v) as 3sum
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        inner join t_pdajos_2 t3 on t2.k=t3.k
        GROUP BY t1.k;
    """

    sql "set force_eager_agg_hint='count:t1.v=push;sum:t1.k=nopush;sum:t2.v=push;sum:t3.v=nopush';"
    order_qt_one_table_has_push_and_nopush_choose_nopush"""
        SELECT /*+ NO_USE_CBO_RULE(PUSH_DOWN_AGG_THROUGH_JOIN_ONE_SIDE) */
          t1.k,
          count(t1.v) AS lcount,
          sum(t1.k) as lsum,
          sum(t2.v) AS rsum,
          sum(t3.v) as 3sum
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        inner join t_pdajos_2 t3 on t2.k=t3.k
        GROUP BY t1.k;
    """

    // ================================================================
    // Extended correctness tests for bilateral eager aggregation
    // ================================================================

    // Setup helper tables for extended tests
    sql """
        drop table if exists t_bilateral_null;
        CREATE TABLE `t_bilateral_null` (
          `k` int NOT NULL COMMENT "join key",
          `v` int COMMENT "nullable agg column"
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 4
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");

        drop table if exists t_bilateral_mg1;
        CREATE TABLE `t_bilateral_mg1` (
          `k1` int NOT NULL,
          `k2` int NOT NULL,
          `v`  int NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`, `k2`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 4
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");

        drop table if exists t_bilateral_mg2;
        CREATE TABLE `t_bilateral_mg2` (
          `k1` int NOT NULL,
          `k2` int NOT NULL,
          `w`  int NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`, `k2`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 4
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");

        drop table if exists t_bilateral_union_t1;
        CREATE TABLE `t_bilateral_union_t1` (
          `k` int NOT NULL,
          `a` int NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 4
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");

        drop table if exists t_bilateral_union_t2;
        CREATE TABLE `t_bilateral_union_t2` (
          `k` int NOT NULL,
          `a` int NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 4
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");

        drop table if exists t_bilateral_union_t3;
        CREATE TABLE `t_bilateral_union_t3` (
          `k` int NOT NULL,
          `a1` int NOT NULL,
          `a2` int NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 4
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");

        drop table if exists t_bilateral_union_t4;
        CREATE TABLE `t_bilateral_union_t4` (
          `k` int NOT NULL,
          `a` int NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 4
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");

        drop table if exists t_bilateral_outer_l;
        CREATE TABLE `t_bilateral_outer_l` (
          `k` int NOT NULL,
          `v` int NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");

        drop table if exists t_bilateral_outer_r;
        CREATE TABLE `t_bilateral_outer_r` (
          `k` int NOT NULL,
          `v` int NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");

        insert into t_bilateral_null values(1,10),(1,null),(2,30),(2,null),(3,null);
        insert into t_bilateral_mg1 values(1,1,10),(1,1,20),(1,2,30),(2,1,40);
        insert into t_bilateral_mg2 values(1,1,100),(1,2,200),(2,1,300);
        insert into t_bilateral_union_t1 values(1,10),(1,20),(2,30);
        insert into t_bilateral_union_t2 values(1,100),(1,200),(2,300);
        insert into t_bilateral_union_t3 values(1,1000,10000),(2,2000,20000);
        insert into t_bilateral_union_t4 values(1,7),(1,8),(2,9);
        insert into t_bilateral_outer_l values(1,10),(1,20),(2,30),(4,40);
        insert into t_bilateral_outer_r values(1,100),(1,200),(2,300),(3,400);
    """

    // ================================================================
    // Section B: mode=1 bilateral push-down — two-table INNER JOIN
    // ================================================================
    sql "SET eager_aggregation_mode = 1;"
    sql "SET force_eager_agg_hint = '';"

    // B1: sum from both sides
    order_qt_mode1_bilateral_sum """
        SELECT t1.k, sum(t1.v) AS lsum, sum(t2.v) AS rsum
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        GROUP BY t1.k;
    """

    // B2: count from both sides
    order_qt_mode1_bilateral_count """
        SELECT t1.k, count(t1.v) AS lcnt, count(t2.v) AS rcnt
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        GROUP BY t1.k;
    """

    // B3: count(*) — pushed to both sides with bilateral multiplier
    order_qt_mode1_bilateral_count_star """
        SELECT t1.k, count(*) AS cnt
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        GROUP BY t1.k;
    """

    // B4: min/max from both sides — multiplier ignored, correct extremes
    order_qt_mode1_bilateral_minmax """
        SELECT t1.k, min(t1.v) AS lmin, max(t1.v) AS lmax,
               min(t2.v) AS rmin, max(t2.v) AS rmax
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        GROUP BY t1.k;
    """

    // B5: mixed agg from both sides (sum + count + min + max)
    order_qt_mode1_bilateral_mixed """
        SELECT t1.k, sum(t1.v) AS lsum, count(t1.v) AS lcnt,
               min(t1.v) AS lmin, max(t1.v) AS lmax, sum(t2.v) AS rsum
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        GROUP BY t1.k;
    """

    // B6: expressions inside agg functions
    order_qt_mode1_bilateral_expr """
        SELECT t1.k, sum(t1.v * 2) AS lexpr, sum(t2.v + 1) AS rexpr
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        GROUP BY t1.k;
    """

    // B7: literal in agg — sum(1) counts rows from left subtree
    order_qt_mode1_bilateral_literal """
        SELECT t1.k, sum(1) AS lcnt_lit, sum(t2.v) AS rsum
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        GROUP BY t1.k;
    """

    // B8: group key taken from right-side table
    order_qt_mode1_bilateral_sum_rkey """
        SELECT t2.k, sum(t1.v) AS lsum, sum(t2.v) AS rsum
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        GROUP BY t2.k;
    """

    // B9: sum of join key column from left side
    order_qt_mode1_bilateral_sum_key """
        SELECT t1.k, sum(t1.k) AS lksum, sum(t2.v) AS rsum
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        GROUP BY t1.k;
    """

    // ================================================================
    // Section C: mode=1 three-way inner join
    // ================================================================

    // C1: sum from all three aliased sides
    order_qt_mode1_threeway_sum """
        SELECT t1.k, sum(t1.v) AS lsum, sum(t2.v) AS msum, sum(t3.v) AS rsum
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        INNER JOIN t_pdajos_2 t3 ON t2.k = t3.k
        GROUP BY t1.k;
    """

    // C2: count + count(*) in three-way join
    order_qt_mode1_threeway_count_star """
        SELECT t1.k, count(t1.v) AS lcnt, count(t2.v) AS mcnt, count(*) AS cnt
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        INNER JOIN t_pdajos_2 t3 ON t2.k = t3.k
        GROUP BY t1.k;
    """

    // C3: min/max across three sides
    order_qt_mode1_threeway_minmax """
        SELECT t1.k, min(t1.v) AS lmin, max(t2.v) AS mmax, min(t3.v) AS rmin
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        INNER JOIN t_pdajos_2 t3 ON t2.k = t3.k
        GROUP BY t1.k;
    """

    // ================================================================
    // Section D: mode=1 cross join
    // ================================================================

    // D1: cross join sum — each row multiplied by full opposite table
    order_qt_mode1_cross_sum """
        SELECT t1.k, sum(t1.v) AS lsum, sum(t2.v) AS rsum
        FROM t_pdajos_1 t1, t_pdajos_2 t2
        GROUP BY t1.k;
    """

    // D2: cross join count
    order_qt_mode1_cross_count """
        SELECT t1.k, count(t1.v) AS lcnt, count(t2.v) AS rcnt
        FROM t_pdajos_1 t1, t_pdajos_2 t2
        GROUP BY t1.k;
    """

    // ================================================================
    // Section E: mode=1 NULL value handling
    // ================================================================

    // E1: nullable left-side column — count/sum skip NULLs
    order_qt_mode1_null_left_count_sum """
        SELECT t1n.k, count(t1n.v) AS lcnt, sum(t1n.v) AS lsum,
               count(t2.v) AS rcnt, sum(t2.v) AS rsum
        FROM t_bilateral_null t1n
        INNER JOIN t_pdajos_2 t2 ON t1n.k = t2.k
        GROUP BY t1n.k;
    """

    // E2: nullable left-side min/max — NULLs ignored
    order_qt_mode1_null_left_minmax """
        SELECT t1n.k, min(t1n.v) AS lmin, max(t1n.v) AS lmax,
               min(t2.v) AS rmin, max(t2.v) AS rmax
        FROM t_bilateral_null t1n
        INNER JOIN t_pdajos_2 t2 ON t1n.k = t2.k
        GROUP BY t1n.k;
    """

    // E3: left join with unmatched rows — NULLs propagate for right side
    order_qt_mode1_null_left_join """
        SELECT t1n.k, sum(t1n.v) AS lsum, sum(t2.v) AS rsum
        FROM t_bilateral_null t1n
        LEFT JOIN t_pdajos_2 t2 ON t1n.k = t2.k
        GROUP BY t1n.k;
    """

    // E4: nullable right-side column in inner join
    order_qt_mode1_null_right_count_sum """
        SELECT t1.k, sum(t1.v) AS lsum, count(t2n.v) AS rcnt, sum(t2n.v) AS rsum
        FROM t_pdajos_1 t1
        INNER JOIN t_bilateral_null t2n ON t1.k = t2n.k
        GROUP BY t1.k;
    """

    // ================================================================
    // Section F: mode=1 multi-column group key
    // ================================================================

    // F1: sum with composite join key (k1, k2)
    order_qt_mode1_multi_groupkey_sum """
        SELECT t1m.k1, t1m.k2, sum(t1m.v) AS lsum, sum(t2m.w) AS rsum
        FROM t_bilateral_mg1 t1m
        INNER JOIN t_bilateral_mg2 t2m ON t1m.k1 = t2m.k1 AND t1m.k2 = t2m.k2
        GROUP BY t1m.k1, t1m.k2;
    """

    // F2: count with composite join key
    order_qt_mode1_multi_groupkey_count """
        SELECT t1m.k1, t1m.k2, count(t1m.v) AS lcnt, count(t2m.w) AS rcnt
        FROM t_bilateral_mg1 t1m
        INNER JOIN t_bilateral_mg2 t2m ON t1m.k1 = t2m.k1 AND t1m.k2 = t2m.k2
        GROUP BY t1m.k1, t1m.k2;
    """

    // F3: min/max with composite join key
    order_qt_mode1_multi_groupkey_minmax """
        SELECT t1m.k1, t1m.k2, min(t1m.v) AS lmin, max(t1m.v) AS lmax, sum(t2m.w) AS rsum
        FROM t_bilateral_mg1 t1m
        INNER JOIN t_bilateral_mg2 t2m ON t1m.k1 = t2m.k1 AND t1m.k2 = t2m.k2
        GROUP BY t1m.k1, t1m.k2;
    """

    // ================================================================
    // Section G: mode=1 HAVING clause
    // ================================================================

    // G1: HAVING filter on left-side aggregate
    order_qt_mode1_having_lsum """
        SELECT t1.k, sum(t1.v) AS lsum, sum(t2.v) AS rsum
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        GROUP BY t1.k
        HAVING sum(t1.v) > 50;
    """

    // G2: HAVING filter on count
    order_qt_mode1_having_count """
        SELECT t1.k, count(t1.v) AS lcnt, sum(t2.v) AS rsum
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        GROUP BY t1.k
        HAVING count(t1.v) > 3;
    """

    // G3: HAVING filter on right-side aggregate (both groups survive)
    order_qt_mode1_having_rsum """
        SELECT t1.k, sum(t1.v) AS lsum, sum(t2.v) AS rsum
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        GROUP BY t1.k
        HAVING sum(t2.v) >= 300;
    """

    // ================================================================
    // Section H: mode=1 pdagg_proj tables (projected join key)
    // ================================================================

    // H1: sum via projected join key (t1.x = t2.k)
    order_qt_mode1_proj_sum """
        SELECT t2.k, sum(t1.x) AS lsum, sum(t2.v) AS rsum
        FROM pdagg_proj_t1 t1
        INNER JOIN pdagg_proj_t2 t2 ON t1.x = t2.k
        GROUP BY t2.k;
    """

    // H2: count via projected join key
    order_qt_mode1_proj_count """
        SELECT t2.k, count(t1.x) AS lcnt, count(t2.v) AS rcnt
        FROM pdagg_proj_t1 t1
        INNER JOIN pdagg_proj_t2 t2 ON t1.x = t2.k
        GROUP BY t2.k;
    """

    // H3: min/max from both sides via projected join key
    order_qt_mode1_proj_minmax """
        SELECT t2.k, min(t1.x) AS lmin, max(t1.y) AS lmax_y,
               min(t2.v) AS rmin, max(t2.v) AS rmax
        FROM pdagg_proj_t1 t1
        INNER JOIN pdagg_proj_t2 t2 ON t1.x = t2.k
        GROUP BY t2.k;
    """

    // ================================================================
    // Section I: mode=1 empty result set
    // ================================================================

    // I1: WHERE predicate eliminates all rows before join
    order_qt_mode1_empty """
        SELECT t1.k, sum(t1.v) AS lsum, sum(t2.v) AS rsum
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        WHERE t1.k = 99
        GROUP BY t1.k;
    """

    // ================================================================
    // Section J: force_eager_agg_hint variations (mode=1)
    // Results must match non-hint equivalents — only optimization path changes
    // ================================================================

    // J1: force push sum from both sides
    sql "SET force_eager_agg_hint = 'sum:t1.v=push;sum:t2.v=push';"
    order_qt_hint_push_both_sum """
        SELECT t1.k, sum(t1.v) AS lsum, sum(t2.v) AS rsum
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        GROUP BY t1.k;
    """

    // J2: force nopush sum from both sides
    sql "SET force_eager_agg_hint = 'sum:t1.v=nopush;sum:t2.v=nopush';"
    order_qt_hint_nopush_both_sum """
        SELECT t1.k, sum(t1.v) AS lsum, sum(t2.v) AS rsum
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        GROUP BY t1.k;
    """

    // J3: push left, nopush right
    sql "SET force_eager_agg_hint = 'sum:t1.v=push;sum:t2.v=nopush';"
    order_qt_hint_push_left_nopush_right """
        SELECT t1.k, sum(t1.v) AS lsum, sum(t2.v) AS rsum
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        GROUP BY t1.k;
    """

    // J4: nopush left, push right
    sql "SET force_eager_agg_hint = 'sum:t1.v=nopush;sum:t2.v=push';"
    order_qt_hint_nopush_left_push_right """
        SELECT t1.k, sum(t1.v) AS lsum, sum(t2.v) AS rsum
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        GROUP BY t1.k;
    """

    // J5: force push count from both sides
    sql "SET force_eager_agg_hint = 'count:t1.v=push;count:t2.v=push';"
    order_qt_hint_push_both_count """
        SELECT t1.k, count(t1.v) AS lcnt, count(t2.v) AS rcnt
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        GROUP BY t1.k;
    """

    // J6: force nopush count from both sides
    sql "SET force_eager_agg_hint = 'count:t1.v=nopush;count:t2.v=nopush';"
    order_qt_hint_nopush_both_count """
        SELECT t1.k, count(t1.v) AS lcnt, count(t2.v) AS rcnt
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        GROUP BY t1.k;
    """

    // J7: force push min/max from both sides
    sql "SET force_eager_agg_hint = 'min:t1.v=push;max:t2.v=push';"
    order_qt_hint_push_minmax """
        SELECT t1.k, min(t1.v) AS lmin, max(t2.v) AS rmax
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        GROUP BY t1.k;
    """

    // J8: force nopush min/max
    sql "SET force_eager_agg_hint = 'min:t1.v=nopush;max:t2.v=nopush';"
    order_qt_hint_nopush_minmax """
        SELECT t1.k, min(t1.v) AS lmin, max(t2.v) AS rmax
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        GROUP BY t1.k;
    """

    // J9: count(*) with push hint
    sql "SET force_eager_agg_hint = 'count:*=push';"
    order_qt_hint_count_star_push """
        SELECT t1.k, count(*) AS cnt
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        GROUP BY t1.k;
    """

    // J10: count(*) with nopush hint
    sql "SET force_eager_agg_hint = 'count:*=nopush';"
    order_qt_hint_count_star_nopush """
        SELECT t1.k, count(*) AS cnt
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        GROUP BY t1.k;
    """

    // J11: mixed funcs with all pushed
    sql "SET force_eager_agg_hint = 'sum:t1.v=push;count:t1.v=push;sum:t2.v=push;min:t2.v=push';"
    order_qt_hint_mixed_funcs """
        SELECT t1.k, sum(t1.v) AS lsum, count(t1.v) AS lcnt,
               sum(t2.v) AS rsum, min(t2.v) AS rmin
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        GROUP BY t1.k;
    """

    // J12: three-way join, partial push (t3.v nopush)
    sql "SET force_eager_agg_hint = 'sum:t1.v=push;sum:t2.v=push;sum:t3.v=nopush';"
    order_qt_hint_threeway_partial """
        SELECT t1.k, sum(t1.v) AS lsum, sum(t2.v) AS msum, sum(t3.v) AS rsum
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        INNER JOIN t_pdajos_2 t3 ON t2.k = t3.k
        GROUP BY t1.k;
    """

    // J13: three-way join, different agg funcs per table
    sql "SET force_eager_agg_hint = 'count:t1.v=push;sum:t2.v=push;min:t3.v=push';"
    order_qt_hint_threeway_mixed """
        SELECT t1.k, count(t1.v) AS lcnt, sum(t2.v) AS msum, min(t3.v) AS rmin
        FROM t_pdajos_1 t1
        INNER JOIN t_pdajos_2 t2 ON t1.k = t2.k
        INNER JOIN t_pdajos_2 t3 ON t2.k = t3.k
        GROUP BY t1.k;
    """

    // J14: semi join count correctness with forced left push
    sql "SET force_eager_agg_hint = 'count:t1.v=push';"
    order_qt_hint_left_semi_count_push_left """
        SELECT /*+SET_VAR(disable_join_reorder = true) */
            t1.k, count(t1.v) AS lcnt
        FROM t_bilateral_outer_l t1
        LEFT SEMI JOIN t_bilateral_outer_r t2 ON t1.k = t2.k
        GROUP BY t1.k;
    """

    // J15: left outer join sum correctness with forced left push
    sql "SET force_eager_agg_hint = 'sum:t1.v=push';"
    order_qt_hint_left_outer_sum_push_left """
        SELECT /*+SET_VAR(disable_join_reorder = true) */
            t2.k, sum(t1.v) AS lsum
        FROM t_bilateral_outer_l t1
        LEFT OUTER JOIN t_bilateral_outer_r t2 ON t1.k = t2.k
        GROUP BY t2.k;
    """

    // J16: right outer join sum correctness with forced left push
    sql "SET force_eager_agg_hint = 'sum:t1.v=push';"
    order_qt_hint_right_outer_sum_push_left """
        SELECT /*+SET_VAR(disable_join_reorder = true) */
            t2.k, sum(t1.v) AS lsum
        FROM t_bilateral_outer_l t1
        RIGHT OUTER JOIN t_bilateral_outer_r t2 ON t1.k = t2.k
        GROUP BY t2.k;
    """

    sql "SET force_eager_agg_hint = 'count:t1.v=push;sum:p1.z=push';"
    order_qt_semi_join_output_cnt """SELECT /*+SET_VAR(disable_join_reorder = true) */
    t1.k, count(t1.v) AS lcnt, sum(p1.z) as s
    FROM  pdagg_proj_t1 p1 inner join (t_bilateral_outer_l t1
            LEFT SEMI JOIN t_bilateral_outer_r t2 ON t1.k = t2.k) on p1.y=t1.k
    GROUP BY t1.k;"""

    order_qt_left_outer_join_output_cnt """SELECT /*+SET_VAR(disable_join_reorder = true) */
    t1.k, count(t1.v) AS lcnt, sum(p1.z) as s
    FROM  pdagg_proj_t1 p1 inner join (t_bilateral_outer_l t1
            LEFT outer JOIN t_bilateral_outer_r t2 ON t1.k = t2.k) on p1.y=t1.k
    GROUP BY t1.k;"""

    order_qt_right_outer_join_output_cnt """SELECT /*+SET_VAR(disable_join_reorder = true) */
    t1.k, count(t1.v) AS lcnt, sum(p1.z) as s
    FROM  pdagg_proj_t1 p1 inner join (t_bilateral_outer_l t1
            right outer JOIN t_bilateral_outer_r t2 ON t1.k = t2.k) on p1.y=t2.k
    GROUP BY t1.k;"""

    // ================================================================
    // Section K: mode=1 union all with bilateral multi-level join
    // ================================================================

    def union_all_sql = """
        SELECT 
               t4.k, sum(u.a1), sum(u.a2), sum(t4.a)
        FROM (
            SELECT t1.k AS k, t1.a AS a1, t2.a AS a2
            FROM t_bilateral_union_t1 t1
            INNER JOIN t_bilateral_union_t2 t2 ON t1.k = t2.k
            UNION ALL
            SELECT t3.k AS k, t3.a1 AS a1, t3.a2 AS a2
            FROM t_bilateral_union_t3 t3
        ) u
        INNER JOIN t_bilateral_union_t4 t4 ON u.k = t4.k
        GROUP BY t4.k
        ORDER BY t4.k;
    """
    sql "SET force_eager_agg_hint = 'sum:u.a1=push;sum:u.a2=push;sum:t4.a=push';"
    order_qt_all_push union_all_sql
    sql "SET force_eager_agg_hint = 'sum:u.a1=nopush;sum:u.a2=nopush;sum:t4.a=push';"
    order_qt_union_all_no_push union_all_sql
    sql "SET force_eager_agg_hint = 'sum:u.a1=push;sum:u.a2=push;sum:t4.a=nopush';"
    order_qt_union_all_push union_all_sql
    sql "SET force_eager_agg_hint = 'sum:u.a1=nopush;sum:u.a2=push;sum:t4.a=nopush';"
    //should not rewrite
    order_qt_union_all_partial_push union_all_sql

    // Reset session variables to defaults
    sql "SET eager_aggregation_mode = -1;"
    sql "SET force_eager_agg_hint = '';"
}
