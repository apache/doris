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

// Regression for GroupJoin fusion + runtime filter interaction.
//
// When a fused GROUP JOIN node produces a runtime filter for a PARTITIONED
// (shuffle) inner join, the filter is only correct if it is treated as a
// global (non-broadcast) filter that merges the per-instance partial hash
// tables. The fused node was previously classified as a broadcast join
// unconditionally, so each instance published only its own partition's keys
// and the probe-side scan over-pruned rows, returning an empty result (or a
// partial one) instead of the join groups. This suite reproduces that shape
// (shuffle join + GLOBAL runtime filter + group-by key == equi-join key) and
// asserts the fused plan returns exactly the reference result.
suite("test_group_join_fusion_rf") {
    sql "DROP TABLE IF EXISTS gj_rf_left"
    sql "DROP TABLE IF EXISTS gj_rf_right"

    sql """
        CREATE TABLE gj_rf_left (
            k1 INT NULL,
            k2 INT NULL,
            v BIGINT NULL,
            s VARCHAR(64) NULL,
            ts INT NULL
        ) DUPLICATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 8
        PROPERTIES ("replication_num" = "1")
        """

    sql """
        CREATE TABLE gj_rf_right (
            k1 INT NULL,
            k2 INT NULL,
            v BIGINT NULL,
            s VARCHAR(64) NULL,
            ts INT NULL
        ) DUPLICATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k2) BUCKETS 8
        PROPERTIES ("replication_num" = "1")
        """

    sql """INSERT INTO gj_rf_left VALUES
        (1,10,5,'A',1), (1,10,15,'a',2),
        (2,20,7,'B',1), (2,21,NULL,'b',2),
        (3,30,100,'C',1), (4,40,1,'D',1),
        (NULL,50,9,'N',1), (11,10,3,'X',3)
        """

    sql """INSERT INTO gj_rf_right VALUES
        (1,10,6,'a',3), (1,10,20,'A',4),
        (2,20,8,'B',5), (2,21,5,'b',6),
        (3,30,50,'c',7), (5,50,9,'E',8),
        (NULL,50,9,'N',9), (21,10,4,'X',10)
        """

    // Runtime filters are only planned when column statistics are known, so
    // analyze synchronously before comparing plans.
    sql "ANALYZE TABLE gj_rf_left WITH SYNC"
    sql "ANALYZE TABLE gj_rf_right WITH SYNC"

    def query = """
        SELECT l.k1, COUNT(*) AS cnt, SUM(r.v) AS total_v
        FROM gj_rf_left l
        JOIN [shuffle] gj_rf_right r ON l.k1 = r.k1
        GROUP BY l.k1
        ORDER BY l.k1
        """

    // Reference result computed with the fusion operator disabled.
    sql "SET experimental_enable_group_join_fusion = false"
    sql "SET runtime_filter_mode = 'GLOBAL'"
    def reference = sql query

    sql "SET experimental_enable_group_join_fusion = true"

    // The planner must fuse join + aggregate AND generate a runtime filter on
    // the fused node, otherwise this suite does not exercise the fix.
    def plan = sql "EXPLAIN " + query
    assertTrue(plan.toString().contains("VGROUP JOIN"),
            "GroupJoin fusion did not fire, plan: " + plan)
    assertTrue(plan.toString().contains("runtime filters"),
            "Runtime filter not generated on the fused plan, plan: " + plan)

    // Before the fix the probe side was over-pruned by the per-instance
    // partial filter and the fused query returned no rows at all.
    def fused = sql query
    assertEquals(reference, fused)

    order_qt_fused_result query

    // Aggregates with GROUP BY keys but no aggregate functions (e.g. SELECT
    // DISTINCT over an inner join, or a pure GROUP BY over the join keys) are
    // fused as well: the fused GroupJoin operator groups rows by the shared
    // hash key and materializes the grouping-key columns, so it does not need
    // any aggregate function (aggregate_functions stays empty on the node).
    sql "SET runtime_filter_mode = 'OFF'"
    def distinctQuery = """
        SELECT DISTINCT l.k1
        FROM gj_rf_left l
        JOIN [shuffle] gj_rf_right r ON l.k1 = r.k1
        ORDER BY l.k1
        """
    sql "SET experimental_enable_group_join_fusion = true"
    def distinctPlan = sql "EXPLAIN " + distinctQuery
    assertTrue(distinctPlan.toString().contains("VGROUP JOIN"),
            "DISTINCT over the join must be fused into a GroupJoin, plan: " + distinctPlan)
    sql "SET experimental_enable_group_join_fusion = false"
    def distinctReference = sql distinctQuery
    sql "SET experimental_enable_group_join_fusion = true"
    def distinctFused = sql distinctQuery
    assertEquals(distinctReference, distinctFused)

    // The same no-aggregate-function shape written as a plain GROUP BY (a pure
    // deduplication query, the QA repro form) must equally be fused and return
    // exactly the reference rows.
    def pureGroupByQuery = """
        SELECT l.k1
        FROM gj_rf_left l
        JOIN [shuffle] gj_rf_right r ON l.k1 = r.k1
        GROUP BY l.k1
        ORDER BY l.k1
        """
    def pureGroupByPlan = sql "EXPLAIN " + pureGroupByQuery
    assertTrue(pureGroupByPlan.toString().contains("VGROUP JOIN"),
            "Pure GROUP BY over the join must be fused into a GroupJoin, plan: " + pureGroupByPlan)
    sql "SET experimental_enable_group_join_fusion = false"
    def pureGroupByReference = sql pureGroupByQuery
    sql "SET experimental_enable_group_join_fusion = true"
    def pureGroupByFused = sql pureGroupByQuery
    assertEquals(pureGroupByReference, pureGroupByFused)

    // Restore defaults so other suites are not affected.
    sql "SET experimental_enable_group_join_fusion = false"
    sql "SET runtime_filter_mode = 'GLOBAL'"
}
