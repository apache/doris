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

/**
 * Regression for DORIS-25865: FE local-shuffle planner used to insert a
 * LocalExchangeNode directly under RecursiveCteNode, which collided with two
 * RecursiveCte invariants:
 *
 *   1. ThriftPlansBuilder locates the recursive sender fragment via
 *      `recursiveCteNode.getChild(1).getChild(0).getFragment()`.  An extra LE
 *      wrapper shifted that path off the cross-fragment ExchangeNode and
 *      pulled the RecCTE producer fragment itself into `fragmentsToReset`.
 *      BE then rejected with `[INTERNAL_ERROR]Fragment N contains a recursive
 *      CTE node` during `RecCTESourceOperatorX::prepare()`.
 *
 *   2. BE's `RecCTESourceOperatorX::is_serial_operator()` always returns true,
 *      but `RecursiveCteNode.isSerialNode()` on the FE side defaulted to
 *      false.  Without the serial marker, the FE planner left the producer
 *      fragment with parallel=N sender pipelines while RecCte actually emits
 *      data from a single instance — the cross-fragment Exchange receiver
 *      waited forever on the N-1 silent senders and the query hung.
 *
 * Fix lives in `RecursiveCteNode`:
 *   - override `isSerialNode()` to return true (mirrors BE),
 *   - override `enforceAndDeriveLocalExchange` to bypass the framework's
 *     `enforceRequire` so no LE is inserted between RecCte and its Exchange
 *     children (children's own subtrees still get LE planning).
 *
 * This test asserts:
 *   - planner=true succeeds (no "Fragment N contains a recursive CTE node");
 *   - results between planner=true and planner=false are identical for the
 *     three downstream-consumer shapes the JIRA listed: aggregate, window,
 *     grouping sets;
 *   - the negative control (RecCte directly consumed by SELECT) still works
 *     in both modes — it would pass even with the original bug, but covers
 *     the simple path so a regression there is caught immediately.
 */
suite("test_local_shuffle_recursive_cte", "nereids_p0") {

    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_sql_cache=false"
    sql "SET enable_local_shuffle=true"
    sql "SET parallel_pipeline_task_num=4"
    sql "SET runtime_filter_mode=off"

    // For each SQL, run it twice — once with FE planner, once with BE planner —
    // and assert result rows are identical.  Plan shape intentionally not asserted:
    // the two planners legitimately differ on LE placement.
    def checkConsistency = { String tag, String testSql ->
        def sqlOn = """SELECT /*+SET_VAR(enable_local_shuffle_planner=true)*/""" + testSql.replaceFirst(/(?i)^\s*select/, "")
        def sqlOff = """SELECT /*+SET_VAR(enable_local_shuffle_planner=false)*/""" + testSql.replaceFirst(/(?i)^\s*select/, "")
        check_sql_equal(sqlOn, sqlOff)
    }

    // ============================================================
    //  Case 1 — Recursive CTE consumed by aggregate
    //
    //  Original error with FE planner:
    //    errCode = 2, detailMessage = [INTERNAL_ERROR]
    //    Fragment N contains a recursive CTE node
    // ============================================================
    checkConsistency("rec_cte_agg", """
        SELECT n_mod, count(*) AS c, sum(s) AS total
        FROM (
            WITH RECURSIVE cte(n, s) AS (
                SELECT CAST(1 AS INT), CAST(1 AS BIGINT)
                UNION ALL
                SELECT CAST(n + 1 AS INT), CAST(s + n + 1 AS BIGINT)
                FROM cte WHERE n < 30
            )
            SELECT n % 7 AS n_mod, s FROM cte
        ) t
        GROUP BY n_mod
        ORDER BY n_mod
    """)

    // ============================================================
    //  Case 2 — Recursive CTE consumed by window function
    //
    //  Original failure with FE planner: hung indefinitely because the
    //  producer fragment had parallel=N sender pipelines but only one of
    //  them actually emits data.  Fixed by marking RecursiveCteNode serial
    //  so AddLocalExchange wraps the root with a PASSTHROUGH LE that fans
    //  the single producer out to N parallel sinks.
    // ============================================================
    checkConsistency("rec_cte_window", """
        SELECT n, sum(n) OVER (PARTITION BY n % 5) AS sum_n
        FROM (
            WITH RECURSIVE cte(n) AS (
                SELECT CAST(1 AS INT)
                UNION ALL
                SELECT CAST(n + 1 AS INT) FROM cte WHERE n < 30
            )
            SELECT n FROM cte
        ) t
        ORDER BY n
    """)

    // ============================================================
    //  Case 3 — Recursive CTE consumed by GROUPING SETS
    //
    //  Suggested by the JIRA reporter as a third "downstream operator that
    //  introduces additional fragments / local exchanges" — together with
    //  aggregate and window it covers the original failure pattern.
    // ============================================================
    checkConsistency("rec_cte_grouping_sets", """
        SELECT n_mod, n_bucket, count(*) AS c, sum(s) AS total
        FROM (
            WITH RECURSIVE cte(n, s) AS (
                SELECT CAST(1 AS INT), CAST(1 AS BIGINT)
                UNION ALL
                SELECT CAST(n + 1 AS INT), CAST(s + n + 1 AS BIGINT)
                FROM cte WHERE n < 30
            )
            SELECT n % 7 AS n_mod, n % 3 AS n_bucket, s FROM cte
        ) t
        GROUP BY GROUPING SETS ((n_mod), (n_bucket), (n_mod, n_bucket))
        ORDER BY n_mod NULLS LAST, n_bucket NULLS LAST
    """)

    // ============================================================
    //  Negative control — RecCte directly consumed by SELECT.
    //  This path didn't generate the extra fragments needed to trigger the
    //  original bug, but exercising it ensures the fix doesn't regress the
    //  simple consumer shape.
    // ============================================================
    checkConsistency("rec_cte_select", """
        SELECT n
        FROM (
            WITH RECURSIVE cte(n) AS (
                SELECT CAST(1 AS INT)
                UNION ALL
                SELECT CAST(n + 1 AS INT) FROM cte WHERE n < 5
            )
            SELECT n FROM cte
        ) t
        ORDER BY n
    """)

    // ============================================================
    //  Case 4 — RecCte feeding a hash JOIN
    //
    //  Another downstream consumer that introduces an extra fragment via a
    //  shuffle join.  Verifies the serial-RecCte → PASSTHROUGH LE wrap also
    //  works when the consumer requires hash distribution.
    // ============================================================
    checkConsistency("rec_cte_join", """
        SELECT a.n, b.n AS m
        FROM (
            WITH RECURSIVE cte(n) AS (
                SELECT CAST(1 AS INT)
                UNION ALL
                SELECT CAST(n + 1 AS INT) FROM cte WHERE n < 10
            )
            SELECT n FROM cte
        ) a JOIN (
            WITH RECURSIVE cte(n) AS (
                SELECT CAST(2 AS INT)
                UNION ALL
                SELECT CAST(n + 2 AS INT) FROM cte WHERE n < 10
            )
            SELECT n FROM cte
        ) b ON a.n = b.n
        ORDER BY a.n
    """)
}
