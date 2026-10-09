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

suite("test_ivm_agg_expr_over_agg_1") {
    // Transparent rewrite could answer a query from one of the views under test, which would stop the
    // base-table comparisons in this suite from being an oracle.
    sql """set enable_materialized_view_rewrite = false"""

    // =========================================================
    // A scalar expression wrapped around an aggregate result, as
    // in SELECT k, SUM(v) * 100 FROM t GROUP BY k, must stay
    // incrementally maintainable.
    //
    // Apply merges the old MV state in the state domain and then
    // re-applies the outer expression:
    //     new.s100 = f(apply(old_mv.sum_v, delta.sum_v))
    // so the MV must persist a column carrying SUM(v) itself. That
    // column is the visible aggregate output when the select list
    // projects it (SELECT SUM(v) AS s, SUM(v) * 100) and a
    // materialized hidden column when an upper expression consumes
    // it without projecting it (SELECT SUM(v) * 100).
    //
    // These cases verify both halves of the invariant:
    //   * the hidden layout, via DESC (the dropped state column is
    //     materialized, once per aggregate state, reusing existing
    //     columns when they already carry it);
    //   * the merged values, through INSERT/UPDATE/DELETE and
    //     incremental refreshes.
    //
    // NOTE: set show_hidden_columns=true right before a DESC only —
    // enabling it earlier puts the session in debug mode and blocks
    // CREATE MATERIALIZED VIEW.
    // =========================================================

    def refreshIncremental = { mv ->
        sql """REFRESH MATERIALIZED VIEW ${mv} INCREMENTAL"""
        waitingMTMVTaskFinishedByMvName(mv)
    }

    sql """drop materialized view if exists test_ivm_expr_over_agg_sum;"""
    sql """drop materialized view if exists test_ivm_expr_over_agg_cnt;"""
    sql """drop materialized view if exists test_ivm_expr_over_agg_min;"""
    sql """drop materialized view if exists test_ivm_expr_over_agg_max;"""
    sql """drop materialized view if exists test_ivm_expr_over_agg_list;"""
    sql """drop materialized view if exists test_ivm_expr_over_agg_div;"""
    sql """drop materialized view if exists test_ivm_expr_over_agg_cast;"""
    sql """drop materialized view if exists test_ivm_expr_over_agg_scalar;"""
    sql """drop materialized view if exists test_ivm_expr_over_agg_sum_avg;"""
    sql """drop materialized view if exists test_ivm_expr_over_agg_sum_avg_mul;"""
    sql """drop materialized view if exists test_ivm_expr_over_agg_floor_sum_div;"""
    sql """drop materialized view if exists test_ivm_expr_over_agg_plain;"""
    sql """drop materialized view if exists test_ivm_expr_over_agg_cnt_star;"""
    sql """drop materialized view if exists test_ivm_expr_over_agg_avg_round;"""
    sql """drop materialized view if exists test_ivm_expr_over_agg_agg_arg;"""
    sql """drop materialized view if exists test_ivm_expr_over_agg_key_expr;"""
    sql """drop table if exists test_ivm_expr_over_agg_base;"""

    sql """
        CREATE TABLE test_ivm_expr_over_agg_base (
            id INT,
            k INT,
            v INT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    // =========================================================
    // Part 1: hidden layout of the wrapped-aggregate shapes
    // =========================================================

    // SUM(v) * 100: SUM's own value is its mergeable state and the visible column is
    // consumed by the outer expression, so the state is materialized as _0_SUM_COL__
    // next to the hidden non-NULL count.
    sql """
        CREATE MATERIALIZED VIEW test_ivm_expr_over_agg_sum
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS SELECT k, SUM(v) * 100 AS s100 FROM test_ivm_expr_over_agg_base GROUP BY k;
    """
    sql """set show_hidden_columns=true"""
    qt_sum_desc """DESC test_ivm_expr_over_agg_sum"""
    sql """set show_hidden_columns=false"""

    // COUNT(v) + 1: same for COUNT(expr), whose visible column is the count state.
    sql """
        CREATE MATERIALIZED VIEW test_ivm_expr_over_agg_cnt
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS SELECT k, COUNT(v) + 1 AS c1 FROM test_ivm_expr_over_agg_base GROUP BY k;
    """
    sql """set show_hidden_columns=true"""
    qt_cnt_desc """DESC test_ivm_expr_over_agg_cnt"""
    sql """set show_hidden_columns=false"""

    // MIN(v) * 2 and MAX(v) + 1.
    sql """
        CREATE MATERIALIZED VIEW test_ivm_expr_over_agg_min
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS SELECT k, MIN(v) * 2 AS m2 FROM test_ivm_expr_over_agg_base GROUP BY k;
    """
    sql """set show_hidden_columns=true"""
    qt_min_desc """DESC test_ivm_expr_over_agg_min"""
    sql """set show_hidden_columns=false"""

    sql """
        CREATE MATERIALIZED VIEW test_ivm_expr_over_agg_max
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS SELECT k, MAX(v) + 1 AS m1 FROM test_ivm_expr_over_agg_base GROUP BY k;
    """
    sql """set show_hidden_columns=true"""
    qt_max_desc """DESC test_ivm_expr_over_agg_max"""
    sql """set show_hidden_columns=false"""

    // ARRAY_SIZE(COLLECT_LIST(v)): the visible array is the whole aggregate state.
    sql """
        CREATE MATERIALIZED VIEW test_ivm_expr_over_agg_list
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS SELECT k, ARRAY_SIZE(COLLECT_LIST(v)) AS n FROM test_ivm_expr_over_agg_base GROUP BY k;
    """
    sql """set show_hidden_columns=true"""
    qt_list_desc """DESC test_ivm_expr_over_agg_list"""
    sql """set show_hidden_columns=false"""

    // SUM(v) / COUNT(v): two states, both consumed by one expression. The visible COUNT
    // column of the COUNT target is also SUM's hidden non-NULL count (column pool), so
    // the shared count state is materialized exactly once.
    sql """
        CREATE MATERIALIZED VIEW test_ivm_expr_over_agg_div
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS SELECT k, SUM(v) / COUNT(v) AS d FROM test_ivm_expr_over_agg_base GROUP BY k;
    """
    sql """set show_hidden_columns=true"""
    qt_div_desc """DESC test_ivm_expr_over_agg_div"""
    sql """set show_hidden_columns=false"""

    // CAST(SUM(v) AS DOUBLE) and a scalar (no GROUP BY) variant.
    sql """
        CREATE MATERIALIZED VIEW test_ivm_expr_over_agg_cast
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS SELECT k, CAST(SUM(v) AS DOUBLE) AS d FROM test_ivm_expr_over_agg_base GROUP BY k;
    """
    sql """set show_hidden_columns=true"""
    qt_cast_desc """DESC test_ivm_expr_over_agg_cast"""
    sql """set show_hidden_columns=false"""

    sql """
        CREATE MATERIALIZED VIEW test_ivm_expr_over_agg_scalar
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS SELECT SUM(v) * 100 AS s100 FROM test_ivm_expr_over_agg_base;
    """
    sql """set show_hidden_columns=true"""
    qt_scalar_desc """DESC test_ivm_expr_over_agg_scalar"""
    sql """set show_hidden_columns=false"""

    // SUM(v) * 100 next to AVG(v) * 200: AVG's hidden SUM state reuses the visible SUM
    // column, so it must follow that column onto the single materialized carrier instead
    // of adding a second SUM column.
    sql """
        CREATE MATERIALIZED VIEW test_ivm_expr_over_agg_sum_avg
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS SELECT k, SUM(v) * 100 AS s100, AVG(v) * 200 AS a200 FROM test_ivm_expr_over_agg_base GROUP BY k;
    """
    sql """set show_hidden_columns=true"""
    qt_sum_avg_desc """DESC test_ivm_expr_over_agg_sum_avg"""
    sql """set show_hidden_columns=false"""

    // SUM(v) * AVG(v): both aggregate values are consumed by one expression. SUM's state is
    // materialized, and AVG's hidden SUM state (which the column pool shares with the visible SUM
    // column) follows it onto that single carrier, so AVG adds no second state column.
    sql """
        CREATE MATERIALIZED VIEW test_ivm_expr_over_agg_sum_avg_mul
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS SELECT k, SUM(v) * AVG(v) AS p FROM test_ivm_expr_over_agg_base GROUP BY k;
    """
    sql """set show_hidden_columns=true"""
    qt_sum_avg_mul_desc """DESC test_ivm_expr_over_agg_sum_avg_mul"""
    sql """set show_hidden_columns=false"""

    // FLOOR((SUM(v) + 0.7) / 0.5) + 20: one expression tree consumes the SUM result, so the state is
    // materialized and the whole tree is re-applied over the merged state on every refresh.
    sql """
        CREATE MATERIALIZED VIEW test_ivm_expr_over_agg_floor_sum_div
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS SELECT k, FLOOR((SUM(v) + 0.7) / 0.5) + 20 AS fs FROM test_ivm_expr_over_agg_base GROUP BY k;
    """
    sql """set show_hidden_columns=true"""
    qt_floor_sum_div_desc """DESC test_ivm_expr_over_agg_floor_sum_div"""
    sql """set show_hidden_columns=false"""

    // =========================================================
    // Part 2: shapes that already worked must keep their exact
    // layout — no extra column is materialized when the visible
    // column itself carries the state.
    // =========================================================

    // The visible SUM column is projected, so it carries the state itself.
    sql """
        CREATE MATERIALIZED VIEW test_ivm_expr_over_agg_plain
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS SELECT k, SUM(v) AS s, SUM(v) * 100 AS s100 FROM test_ivm_expr_over_agg_base GROUP BY k;
    """
    sql """set show_hidden_columns=true"""
    qt_plain_desc """DESC test_ivm_expr_over_agg_plain"""
    sql """set show_hidden_columns=false"""

    // COUNT(*) reads the group count, AVG and BITMAP_UNION_COUNT derive their visible value
    // from hidden state: wrapping them needs no state materialization.
    sql """
        CREATE MATERIALIZED VIEW test_ivm_expr_over_agg_cnt_star
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS SELECT k, COUNT(*) * 2 AS c2 FROM test_ivm_expr_over_agg_base GROUP BY k;
    """
    sql """set show_hidden_columns=true"""
    qt_cnt_star_desc """DESC test_ivm_expr_over_agg_cnt_star"""
    sql """set show_hidden_columns=false"""

    sql """
        CREATE MATERIALIZED VIEW test_ivm_expr_over_agg_avg_round
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS SELECT k, ROUND(AVG(v), 2) AS a FROM test_ivm_expr_over_agg_base GROUP BY k;
    """
    sql """set show_hidden_columns=true"""
    qt_avg_round_desc """DESC test_ivm_expr_over_agg_avg_round"""
    sql """set show_hidden_columns=false"""





    // The expression sits inside the aggregate, so the visible column is the state.
    sql """
        CREATE MATERIALIZED VIEW test_ivm_expr_over_agg_agg_arg
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS SELECT k, SUM(v * 100) AS s FROM test_ivm_expr_over_agg_base GROUP BY k;
    """
    sql """set show_hidden_columns=true"""
    qt_agg_arg_desc """DESC test_ivm_expr_over_agg_agg_arg"""
    sql """set show_hidden_columns=false"""

    // The expression wraps the GROUP BY key, not an aggregate.
    sql """
        CREATE MATERIALIZED VIEW test_ivm_expr_over_agg_key_expr
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS SELECT UPPER(CAST(k AS VARCHAR)) AS uk, SUM(v) AS s FROM test_ivm_expr_over_agg_base GROUP BY k;
    """
    sql """set show_hidden_columns=true"""
    qt_key_expr_desc """DESC test_ivm_expr_over_agg_key_expr"""
    sql """set show_hidden_columns=false"""

    // =========================================================
    // Part 3: values. Every wrapped-aggregate MV above is
    // incrementally maintainable, so a mixed INSERT / UPDATE /
    // DELETE window per step must keep the MV equal to the same
    // query over the base table.
    // =========================================================

    // k=1: 10, 20; k=2: 30, 40; k=3: a single NULL row.
    sql """INSERT INTO test_ivm_expr_over_agg_base VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30), (4, 2, 40), (5, 3, NULL);"""
    refreshIncremental("test_ivm_expr_over_agg_sum")
    refreshIncremental("test_ivm_expr_over_agg_cnt")
    refreshIncremental("test_ivm_expr_over_agg_min")
    refreshIncremental("test_ivm_expr_over_agg_max")
    refreshIncremental("test_ivm_expr_over_agg_list")
    refreshIncremental("test_ivm_expr_over_agg_div")
    refreshIncremental("test_ivm_expr_over_agg_cast")
    refreshIncremental("test_ivm_expr_over_agg_scalar")
    refreshIncremental("test_ivm_expr_over_agg_sum_avg")
    refreshIncremental("test_ivm_expr_over_agg_sum_avg_mul")
    refreshIncremental("test_ivm_expr_over_agg_floor_sum_div")
    refreshIncremental("test_ivm_expr_over_agg_plain")
    refreshIncremental("test_ivm_expr_over_agg_cnt_star")
    refreshIncremental("test_ivm_expr_over_agg_avg_round")
    refreshIncremental("test_ivm_expr_over_agg_agg_arg")
    refreshIncremental("test_ivm_expr_over_agg_key_expr")

    order_qt_initial_sum """SELECT k, s100 FROM test_ivm_expr_over_agg_sum"""
    order_qt_initial_sum_source """SELECT k, SUM(v) * 100 AS s100 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_initial_cnt """SELECT k, c1 FROM test_ivm_expr_over_agg_cnt"""
    order_qt_initial_cnt_source """SELECT k, COUNT(v) + 1 AS c1 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_initial_min """SELECT k, m2 FROM test_ivm_expr_over_agg_min"""
    order_qt_initial_min_source """SELECT k, MIN(v) * 2 AS m2 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_initial_max """SELECT k, m1 FROM test_ivm_expr_over_agg_max"""
    order_qt_initial_max_source """SELECT k, MAX(v) + 1 AS m1 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_initial_list """SELECT k, n FROM test_ivm_expr_over_agg_list"""
    order_qt_initial_list_source """SELECT k, ARRAY_SIZE(COLLECT_LIST(v)) AS n FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_initial_div """SELECT k, d FROM test_ivm_expr_over_agg_div"""
    order_qt_initial_div_source """SELECT k, SUM(v) / COUNT(v) AS d FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_initial_scalar """SELECT s100 FROM test_ivm_expr_over_agg_scalar"""
    order_qt_initial_scalar_source """SELECT SUM(v) * 100 AS s100 FROM test_ivm_expr_over_agg_base"""
    order_qt_initial_sum_avg """SELECT k, s100, a200 FROM test_ivm_expr_over_agg_sum_avg"""
    order_qt_initial_sum_avg_source """
        SELECT k, SUM(v) * 100 AS s100, AVG(v) * 200 AS a200 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_initial_sum_avg_mul """SELECT k, p FROM test_ivm_expr_over_agg_sum_avg_mul"""
    order_qt_initial_sum_avg_mul_source """
        SELECT k, SUM(v) * AVG(v) AS p FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_initial_floor_sum_div """SELECT k, fs FROM test_ivm_expr_over_agg_floor_sum_div"""
    order_qt_initial_floor_sum_div_source """
        SELECT k, FLOOR((SUM(v) + 0.7) / 0.5) + 20 AS fs FROM test_ivm_expr_over_agg_base GROUP BY k"""


    // Insert a new non-extremal value per group so later MIN/MAX merges stay incremental.
    sql """INSERT INTO test_ivm_expr_over_agg_base VALUES (6, 1, 30), (7, 2, 35);"""
    refreshIncremental("test_ivm_expr_over_agg_sum")
    refreshIncremental("test_ivm_expr_over_agg_cnt")
    refreshIncremental("test_ivm_expr_over_agg_min")
    refreshIncremental("test_ivm_expr_over_agg_max")
    refreshIncremental("test_ivm_expr_over_agg_list")
    refreshIncremental("test_ivm_expr_over_agg_div")
    refreshIncremental("test_ivm_expr_over_agg_scalar")
    refreshIncremental("test_ivm_expr_over_agg_sum_avg")
    refreshIncremental("test_ivm_expr_over_agg_sum_avg_mul")
    refreshIncremental("test_ivm_expr_over_agg_floor_sum_div")

    order_qt_after_insert_sum """SELECT k, s100 FROM test_ivm_expr_over_agg_sum"""
    order_qt_after_insert_sum_source """SELECT k, SUM(v) * 100 AS s100 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_insert_cnt """SELECT k, c1 FROM test_ivm_expr_over_agg_cnt"""
    order_qt_after_insert_cnt_source """SELECT k, COUNT(v) + 1 AS c1 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_insert_min """SELECT k, m2 FROM test_ivm_expr_over_agg_min"""
    order_qt_after_insert_min_source """SELECT k, MIN(v) * 2 AS m2 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_insert_max """SELECT k, m1 FROM test_ivm_expr_over_agg_max"""
    order_qt_after_insert_max_source """SELECT k, MAX(v) + 1 AS m1 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_insert_list """SELECT k, n FROM test_ivm_expr_over_agg_list"""
    order_qt_after_insert_list_source """
        SELECT k, ARRAY_SIZE(COLLECT_LIST(v)) AS n FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_insert_div """SELECT k, d FROM test_ivm_expr_over_agg_div"""
    order_qt_after_insert_div_source """SELECT k, SUM(v) / COUNT(v) AS d FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_insert_scalar """SELECT s100 FROM test_ivm_expr_over_agg_scalar"""
    order_qt_after_insert_scalar_source """SELECT SUM(v) * 100 AS s100 FROM test_ivm_expr_over_agg_base"""
    order_qt_after_insert_sum_avg """SELECT k, s100, a200 FROM test_ivm_expr_over_agg_sum_avg"""
    order_qt_after_insert_sum_avg_source """
        SELECT k, SUM(v) * 100 AS s100, AVG(v) * 200 AS a200 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_insert_sum_avg_mul """SELECT k, p FROM test_ivm_expr_over_agg_sum_avg_mul"""
    order_qt_after_insert_sum_avg_mul_source """
        SELECT k, SUM(v) * AVG(v) AS p FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_insert_floor_sum_div """SELECT k, fs FROM test_ivm_expr_over_agg_floor_sum_div"""
    order_qt_after_insert_floor_sum_div_source """
        SELECT k, FLOOR((SUM(v) + 0.7) / 0.5) + 20 AS fs FROM test_ivm_expr_over_agg_base GROUP BY k"""

    // The bitmap MV is refreshed in this insert-only window: deleting a non-NULL bitmap element is
    // not incrementally maintainable (the delete guard requires COMPLETE), which is a pre-existing
    // bitmap limitation covered by test_ivm_bitmap_runtime_fallback, not a wrapped-expression one.


    // Update id=2 (k=1: 20 -> 25), a non-extremal value of k=1.
    sql """INSERT INTO test_ivm_expr_over_agg_base VALUES (2, 1, 25);"""
    refreshIncremental("test_ivm_expr_over_agg_sum")
    refreshIncremental("test_ivm_expr_over_agg_cnt")
    refreshIncremental("test_ivm_expr_over_agg_min")
    refreshIncremental("test_ivm_expr_over_agg_max")
    refreshIncremental("test_ivm_expr_over_agg_list")
    refreshIncremental("test_ivm_expr_over_agg_div")
    refreshIncremental("test_ivm_expr_over_agg_scalar")
    refreshIncremental("test_ivm_expr_over_agg_sum_avg")
    refreshIncremental("test_ivm_expr_over_agg_sum_avg_mul")
    refreshIncremental("test_ivm_expr_over_agg_floor_sum_div")

    order_qt_after_update_sum """SELECT k, s100 FROM test_ivm_expr_over_agg_sum"""
    order_qt_after_update_sum_source """SELECT k, SUM(v) * 100 AS s100 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_update_cnt """SELECT k, c1 FROM test_ivm_expr_over_agg_cnt"""
    order_qt_after_update_cnt_source """SELECT k, COUNT(v) + 1 AS c1 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_update_min """SELECT k, m2 FROM test_ivm_expr_over_agg_min"""
    order_qt_after_update_min_source """SELECT k, MIN(v) * 2 AS m2 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_update_max """SELECT k, m1 FROM test_ivm_expr_over_agg_max"""
    order_qt_after_update_max_source """SELECT k, MAX(v) + 1 AS m1 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_update_list """SELECT k, n FROM test_ivm_expr_over_agg_list"""
    order_qt_after_update_list_source """
        SELECT k, ARRAY_SIZE(COLLECT_LIST(v)) AS n FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_update_div """SELECT k, d FROM test_ivm_expr_over_agg_div"""
    order_qt_after_update_div_source """SELECT k, SUM(v) / COUNT(v) AS d FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_update_scalar """SELECT s100 FROM test_ivm_expr_over_agg_scalar"""
    order_qt_after_update_scalar_source """SELECT SUM(v) * 100 AS s100 FROM test_ivm_expr_over_agg_base"""
    order_qt_after_update_sum_avg """SELECT k, s100, a200 FROM test_ivm_expr_over_agg_sum_avg"""
    order_qt_after_update_sum_avg_source """
        SELECT k, SUM(v) * 100 AS s100, AVG(v) * 200 AS a200 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_update_sum_avg_mul """SELECT k, p FROM test_ivm_expr_over_agg_sum_avg_mul"""
    order_qt_after_update_sum_avg_mul_source """
        SELECT k, SUM(v) * AVG(v) AS p FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_update_floor_sum_div """SELECT k, fs FROM test_ivm_expr_over_agg_floor_sum_div"""
    order_qt_after_update_floor_sum_div_source """
        SELECT k, FLOOR((SUM(v) + 0.7) / 0.5) + 20 AS fs FROM test_ivm_expr_over_agg_base GROUP BY k"""

    // Delete the middle value of k=2 (id=7, v=35) together with a dirty insert, so the
    // group keeps at least one live extreme and MIN/MAX stay incremental.
    sql """DELETE FROM test_ivm_expr_over_agg_base WHERE id = 7;"""
    sql """INSERT INTO test_ivm_expr_over_agg_base VALUES (8, 1, 40);"""
    refreshIncremental("test_ivm_expr_over_agg_sum")
    refreshIncremental("test_ivm_expr_over_agg_cnt")
    refreshIncremental("test_ivm_expr_over_agg_min")
    refreshIncremental("test_ivm_expr_over_agg_max")
    refreshIncremental("test_ivm_expr_over_agg_list")
    refreshIncremental("test_ivm_expr_over_agg_div")
    refreshIncremental("test_ivm_expr_over_agg_scalar")
    refreshIncremental("test_ivm_expr_over_agg_sum_avg")
    refreshIncremental("test_ivm_expr_over_agg_sum_avg_mul")
    refreshIncremental("test_ivm_expr_over_agg_floor_sum_div")

    order_qt_after_delete_sum """SELECT k, s100 FROM test_ivm_expr_over_agg_sum"""
    order_qt_after_delete_sum_source """SELECT k, SUM(v) * 100 AS s100 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_delete_cnt """SELECT k, c1 FROM test_ivm_expr_over_agg_cnt"""
    order_qt_after_delete_cnt_source """SELECT k, COUNT(v) + 1 AS c1 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_delete_min """SELECT k, m2 FROM test_ivm_expr_over_agg_min"""
    order_qt_after_delete_min_source """SELECT k, MIN(v) * 2 AS m2 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_delete_max """SELECT k, m1 FROM test_ivm_expr_over_agg_max"""
    order_qt_after_delete_max_source """SELECT k, MAX(v) + 1 AS m1 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_delete_list """SELECT k, n FROM test_ivm_expr_over_agg_list"""
    order_qt_after_delete_list_source """
        SELECT k, ARRAY_SIZE(COLLECT_LIST(v)) AS n FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_delete_div """SELECT k, d FROM test_ivm_expr_over_agg_div"""
    order_qt_after_delete_div_source """SELECT k, SUM(v) / COUNT(v) AS d FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_delete_scalar """SELECT s100 FROM test_ivm_expr_over_agg_scalar"""
    order_qt_after_delete_scalar_source """SELECT SUM(v) * 100 AS s100 FROM test_ivm_expr_over_agg_base"""
    order_qt_after_delete_sum_avg """SELECT k, s100, a200 FROM test_ivm_expr_over_agg_sum_avg"""
    order_qt_after_delete_sum_avg_source """
        SELECT k, SUM(v) * 100 AS s100, AVG(v) * 200 AS a200 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_delete_sum_avg_mul """SELECT k, p FROM test_ivm_expr_over_agg_sum_avg_mul"""
    order_qt_after_delete_sum_avg_mul_source """
        SELECT k, SUM(v) * AVG(v) AS p FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_delete_floor_sum_div """SELECT k, fs FROM test_ivm_expr_over_agg_floor_sum_div"""
    order_qt_after_delete_floor_sum_div_source """
        SELECT k, FLOOR((SUM(v) + 0.7) / 0.5) + 20 AS fs FROM test_ivm_expr_over_agg_base GROUP BY k"""

    // Delete the only row of k=3 (a NULL row) so the group empties, and insert a new group
    // k=4 in the same window: an emptied group must be removed, a new one created.
    sql """DELETE FROM test_ivm_expr_over_agg_base WHERE id = 5;"""
    sql """INSERT INTO test_ivm_expr_over_agg_base VALUES (9, 4, 50);"""
    refreshIncremental("test_ivm_expr_over_agg_sum")
    refreshIncremental("test_ivm_expr_over_agg_cnt")
    refreshIncremental("test_ivm_expr_over_agg_min")
    refreshIncremental("test_ivm_expr_over_agg_max")
    refreshIncremental("test_ivm_expr_over_agg_list")
    refreshIncremental("test_ivm_expr_over_agg_div")
    refreshIncremental("test_ivm_expr_over_agg_scalar")
    refreshIncremental("test_ivm_expr_over_agg_sum_avg")
    refreshIncremental("test_ivm_expr_over_agg_sum_avg_mul")
    refreshIncremental("test_ivm_expr_over_agg_floor_sum_div")

    order_qt_after_group_empty_sum """SELECT k, s100 FROM test_ivm_expr_over_agg_sum"""
    order_qt_after_group_empty_sum_source """SELECT k, SUM(v) * 100 AS s100 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_group_empty_cnt """SELECT k, c1 FROM test_ivm_expr_over_agg_cnt"""
    order_qt_after_group_empty_cnt_source """SELECT k, COUNT(v) + 1 AS c1 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_group_empty_min """SELECT k, m2 FROM test_ivm_expr_over_agg_min"""
    order_qt_after_group_empty_min_source """SELECT k, MIN(v) * 2 AS m2 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_group_empty_max """SELECT k, m1 FROM test_ivm_expr_over_agg_max"""
    order_qt_after_group_empty_max_source """SELECT k, MAX(v) + 1 AS m1 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_group_empty_list """SELECT k, n FROM test_ivm_expr_over_agg_list"""
    order_qt_after_group_empty_list_source """
        SELECT k, ARRAY_SIZE(COLLECT_LIST(v)) AS n FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_group_empty_div """SELECT k, d FROM test_ivm_expr_over_agg_div"""
    order_qt_after_group_empty_div_source """
        SELECT k, SUM(v) / COUNT(v) AS d FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_group_empty_scalar """SELECT s100 FROM test_ivm_expr_over_agg_scalar"""
    order_qt_after_group_empty_scalar_source """SELECT SUM(v) * 100 AS s100 FROM test_ivm_expr_over_agg_base"""
    order_qt_after_group_empty_sum_avg """SELECT k, s100, a200 FROM test_ivm_expr_over_agg_sum_avg"""
    order_qt_after_group_empty_sum_avg_source """
        SELECT k, SUM(v) * 100 AS s100, AVG(v) * 200 AS a200 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_group_empty_sum_avg_mul """SELECT k, p FROM test_ivm_expr_over_agg_sum_avg_mul"""
    order_qt_after_group_empty_sum_avg_mul_source """
        SELECT k, SUM(v) * AVG(v) AS p FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_after_group_empty_floor_sum_div """SELECT k, fs FROM test_ivm_expr_over_agg_floor_sum_div"""
    order_qt_after_group_empty_floor_sum_div_source """
        SELECT k, FLOOR((SUM(v) + 0.7) / 0.5) + 20 AS fs FROM test_ivm_expr_over_agg_base GROUP BY k"""

    // Resurrect the emptied group k=3.
    sql """INSERT INTO test_ivm_expr_over_agg_base VALUES (10, 3, 70);"""
    refreshIncremental("test_ivm_expr_over_agg_sum")
    refreshIncremental("test_ivm_expr_over_agg_cnt")
    refreshIncremental("test_ivm_expr_over_agg_min")
    refreshIncremental("test_ivm_expr_over_agg_max")
    refreshIncremental("test_ivm_expr_over_agg_list")
    refreshIncremental("test_ivm_expr_over_agg_div")
    refreshIncremental("test_ivm_expr_over_agg_scalar")
    refreshIncremental("test_ivm_expr_over_agg_sum_avg")
    refreshIncremental("test_ivm_expr_over_agg_sum_avg_mul")
    refreshIncremental("test_ivm_expr_over_agg_floor_sum_div")
    refreshIncremental("test_ivm_expr_over_agg_cast")
    refreshIncremental("test_ivm_expr_over_agg_plain")
    refreshIncremental("test_ivm_expr_over_agg_cnt_star")
    refreshIncremental("test_ivm_expr_over_agg_avg_round")
    refreshIncremental("test_ivm_expr_over_agg_agg_arg")
    refreshIncremental("test_ivm_expr_over_agg_key_expr")

    order_qt_final_sum """SELECT k, s100 FROM test_ivm_expr_over_agg_sum"""
    order_qt_final_sum_source """SELECT k, SUM(v) * 100 AS s100 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_final_cnt """SELECT k, c1 FROM test_ivm_expr_over_agg_cnt"""
    order_qt_final_cnt_source """SELECT k, COUNT(v) + 1 AS c1 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_final_min """SELECT k, m2 FROM test_ivm_expr_over_agg_min"""
    order_qt_final_min_source """SELECT k, MIN(v) * 2 AS m2 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_final_max """SELECT k, m1 FROM test_ivm_expr_over_agg_max"""
    order_qt_final_max_source """SELECT k, MAX(v) + 1 AS m1 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_final_list """SELECT k, n FROM test_ivm_expr_over_agg_list"""
    order_qt_final_list_source """SELECT k, ARRAY_SIZE(COLLECT_LIST(v)) AS n FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_final_div """SELECT k, d FROM test_ivm_expr_over_agg_div"""
    order_qt_final_div_source """SELECT k, SUM(v) / COUNT(v) AS d FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_final_cast """SELECT k, d FROM test_ivm_expr_over_agg_cast"""
    order_qt_final_cast_source """SELECT k, CAST(SUM(v) AS DOUBLE) AS d FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_final_scalar """SELECT s100 FROM test_ivm_expr_over_agg_scalar"""
    order_qt_final_scalar_source """SELECT SUM(v) * 100 AS s100 FROM test_ivm_expr_over_agg_base"""
    order_qt_final_sum_avg """SELECT k, s100, a200 FROM test_ivm_expr_over_agg_sum_avg"""
    order_qt_final_sum_avg_source """
        SELECT k, SUM(v) * 100 AS s100, AVG(v) * 200 AS a200 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_final_sum_avg_mul """SELECT k, p FROM test_ivm_expr_over_agg_sum_avg_mul"""
    order_qt_final_sum_avg_mul_source """
        SELECT k, SUM(v) * AVG(v) AS p FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_final_floor_sum_div """SELECT k, fs FROM test_ivm_expr_over_agg_floor_sum_div"""
    order_qt_final_floor_sum_div_source """
        SELECT k, FLOOR((SUM(v) + 0.7) / 0.5) + 20 AS fs FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_final_avg_round """SELECT k, a FROM test_ivm_expr_over_agg_avg_round"""
    order_qt_final_avg_round_source """SELECT k, ROUND(AVG(v), 2) AS a FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_final_cnt_star """SELECT k, c2 FROM test_ivm_expr_over_agg_cnt_star"""
    order_qt_final_cnt_star_source """SELECT k, COUNT(*) * 2 AS c2 FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_final_agg_arg """SELECT k, s FROM test_ivm_expr_over_agg_agg_arg"""
    order_qt_final_agg_arg_source """SELECT k, SUM(v * 100) AS s FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_final_key_expr """SELECT uk, s FROM test_ivm_expr_over_agg_key_expr"""
    order_qt_final_key_expr_source """
        SELECT UPPER(CAST(k AS VARCHAR)) AS uk, SUM(v) AS s FROM test_ivm_expr_over_agg_base GROUP BY k"""
    order_qt_final_plain """SELECT k, s, s100 FROM test_ivm_expr_over_agg_plain"""
    order_qt_final_plain_source """
        SELECT k, SUM(v) AS s, SUM(v) * 100 AS s100 FROM test_ivm_expr_over_agg_base GROUP BY k"""

    // =========================================================
    // Part 4: a complete refresh of every MV must produce the
    // same values as the incremental path, so the layout created
    // at CREATE time (the materialized state columns) is verified
    // independently of the merge path.
    // =========================================================

    sql """REFRESH MATERIALIZED VIEW test_ivm_expr_over_agg_sum COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_expr_over_agg_sum")
    order_qt_complete_sum """SELECT k, s100 FROM test_ivm_expr_over_agg_sum"""
    order_qt_complete_sum_source """SELECT k, SUM(v) * 100 AS s100 FROM test_ivm_expr_over_agg_base GROUP BY k"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_expr_over_agg_cnt COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_expr_over_agg_cnt")
    order_qt_complete_cnt """SELECT k, c1 FROM test_ivm_expr_over_agg_cnt"""
    order_qt_complete_cnt_source """SELECT k, COUNT(v) + 1 AS c1 FROM test_ivm_expr_over_agg_base GROUP BY k"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_expr_over_agg_min COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_expr_over_agg_min")
    order_qt_complete_min """SELECT k, m2 FROM test_ivm_expr_over_agg_min"""
    order_qt_complete_min_source """SELECT k, MIN(v) * 2 AS m2 FROM test_ivm_expr_over_agg_base GROUP BY k"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_expr_over_agg_max COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_expr_over_agg_max")
    order_qt_complete_max """SELECT k, m1 FROM test_ivm_expr_over_agg_max"""
    order_qt_complete_max_source """SELECT k, MAX(v) + 1 AS m1 FROM test_ivm_expr_over_agg_base GROUP BY k"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_expr_over_agg_list COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_expr_over_agg_list")
    order_qt_complete_list """SELECT k, n FROM test_ivm_expr_over_agg_list"""
    order_qt_complete_list_source """
        SELECT k, ARRAY_SIZE(COLLECT_LIST(v)) AS n FROM test_ivm_expr_over_agg_base GROUP BY k"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_expr_over_agg_div COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_expr_over_agg_div")
    order_qt_complete_div """SELECT k, d FROM test_ivm_expr_over_agg_div"""
    order_qt_complete_div_source """SELECT k, SUM(v) / COUNT(v) AS d FROM test_ivm_expr_over_agg_base GROUP BY k"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_expr_over_agg_cast COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_expr_over_agg_cast")
    order_qt_complete_cast """SELECT k, d FROM test_ivm_expr_over_agg_cast"""
    order_qt_complete_cast_source """
        SELECT k, CAST(SUM(v) AS DOUBLE) AS d FROM test_ivm_expr_over_agg_base GROUP BY k"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_expr_over_agg_scalar COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_expr_over_agg_scalar")
    order_qt_complete_scalar """SELECT s100 FROM test_ivm_expr_over_agg_scalar"""
    order_qt_complete_scalar_source """SELECT SUM(v) * 100 AS s100 FROM test_ivm_expr_over_agg_base"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_expr_over_agg_sum_avg COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_expr_over_agg_sum_avg")
    order_qt_complete_sum_avg """SELECT k, s100, a200 FROM test_ivm_expr_over_agg_sum_avg"""
    order_qt_complete_sum_avg_source """
        SELECT k, SUM(v) * 100 AS s100, AVG(v) * 200 AS a200 FROM test_ivm_expr_over_agg_base GROUP BY k"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_expr_over_agg_sum_avg_mul COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_expr_over_agg_sum_avg_mul")
    order_qt_complete_sum_avg_mul """SELECT k, p FROM test_ivm_expr_over_agg_sum_avg_mul"""
    order_qt_complete_sum_avg_mul_source """
        SELECT k, SUM(v) * AVG(v) AS p FROM test_ivm_expr_over_agg_base GROUP BY k"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_expr_over_agg_floor_sum_div COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_expr_over_agg_floor_sum_div")
    order_qt_complete_floor_sum_div """SELECT k, fs FROM test_ivm_expr_over_agg_floor_sum_div"""
    order_qt_complete_floor_sum_div_source """
        SELECT k, FLOOR((SUM(v) + 0.7) / 0.5) + 20 AS fs FROM test_ivm_expr_over_agg_base GROUP BY k"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_expr_over_agg_plain COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_expr_over_agg_plain")
    order_qt_complete_plain """SELECT k, s, s100 FROM test_ivm_expr_over_agg_plain"""
    order_qt_complete_plain_source """
        SELECT k, SUM(v) AS s, SUM(v) * 100 AS s100 FROM test_ivm_expr_over_agg_base GROUP BY k"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_expr_over_agg_cnt_star COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_expr_over_agg_cnt_star")
    order_qt_complete_cnt_star """SELECT k, c2 FROM test_ivm_expr_over_agg_cnt_star"""
    order_qt_complete_cnt_star_source """
        SELECT k, COUNT(*) * 2 AS c2 FROM test_ivm_expr_over_agg_base GROUP BY k"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_expr_over_agg_avg_round COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_expr_over_agg_avg_round")
    order_qt_complete_avg_round """SELECT k, a FROM test_ivm_expr_over_agg_avg_round"""
    order_qt_complete_avg_round_source """
        SELECT k, ROUND(AVG(v), 2) AS a FROM test_ivm_expr_over_agg_base GROUP BY k"""




    sql """REFRESH MATERIALIZED VIEW test_ivm_expr_over_agg_agg_arg COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_expr_over_agg_agg_arg")
    order_qt_complete_agg_arg """SELECT k, s FROM test_ivm_expr_over_agg_agg_arg"""
    order_qt_complete_agg_arg_source """SELECT k, SUM(v * 100) AS s FROM test_ivm_expr_over_agg_base GROUP BY k"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_expr_over_agg_key_expr COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_expr_over_agg_key_expr")
    order_qt_complete_key_expr """SELECT uk, s FROM test_ivm_expr_over_agg_key_expr"""
    order_qt_complete_key_expr_source """
        SELECT UPPER(CAST(k AS VARCHAR)) AS uk, SUM(v) AS s FROM test_ivm_expr_over_agg_base GROUP BY k"""
}
