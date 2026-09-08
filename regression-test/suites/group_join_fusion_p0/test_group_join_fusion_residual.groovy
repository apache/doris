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

// Regression for GroupJoin fusion + residual non-equi ON conjuncts.
//
// The fused GROUP JOIN operator matches rows purely by the equi-join key: it keeps
// per-key row counts and per-side aggregation states and has no per-pair filtering
// stage. Fusing a join that still carries a residual ON conjunct (e.g.
// l.v < r.v) used to silently drop that conjunct, over-expanding the match set:
// the fused result equaled the equi-key-only join (k1=1 returned 4 pairs instead
// of the 3 pairs where l.v < r.v holds) and the aggregates were wrong. Joins with
// residual conjuncts must stay on the regular HashJoinNode + AggregationNode path,
// which evaluates other join conjuncts per matched pair. This suite reproduces that
// shape (shuffle INNER join + group-by key == equi-join key + residual <) and
// asserts the residual query is not fused and returns exactly the reference result,
// while a pure equi-key query of the same shape is still fused.
suite("test_group_join_fusion_residual") {
    sql "DROP TABLE IF EXISTS gj_res_left"
    sql "DROP TABLE IF EXISTS gj_res_right"

    sql """
        CREATE TABLE gj_res_left (
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
        CREATE TABLE gj_res_right (
            k1 INT NULL,
            k2 INT NULL,
            v BIGINT NULL,
            s VARCHAR(64) NULL,
            ts INT NULL
        ) DUPLICATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k2) BUCKETS 8
        PROPERTIES ("replication_num" = "1")
        """

    sql """INSERT INTO gj_res_left VALUES
        (1,10,5,'A',1), (1,10,15,'a',2),
        (2,20,7,'B',1), (2,21,NULL,'b',2),
        (3,30,100,'C',1), (4,40,1,'D',1),
        (NULL,50,9,'N',1), (11,10,3,'X',3)
        """

    sql """INSERT INTO gj_res_right VALUES
        (1,10,6,'a',3), (1,10,20,'A',4),
        (2,20,8,'B',5), (2,21,5,'b',6),
        (3,30,50,'c',7), (5,50,9,'E',8),
        (NULL,50,9,'N',9), (21,10,4,'X',10)
        """

    // Keep the plan shape deterministic: shuffle INNER join, no runtime filters,
    // no sql cache so the run always exercises the same execution path.
    sql "SET runtime_filter_mode = 'OFF'"
    sql "SET enable_sql_cache = false"

    def residualQuery = """
        SELECT l.k1, COUNT(*) AS cnt, SUM(l.v) AS sum_l, SUM(r.v) AS sum_r
        FROM gj_res_left l
        JOIN [shuffle] gj_res_right r
          ON l.k1 = r.k1 AND l.v < r.v
        GROUP BY l.k1
        ORDER BY l.k1
        """

    // Reference result computed with the fusion operator disabled.
    sql "SET experimental_enable_group_join_fusion = false"
    def reference = sql residualQuery

    // For this data the residual query must return exactly two groups:
    // k1=1 matches pairs (5<6, 5<20, 15<20) -> cnt 3, sum_l 25, sum_r 46;
    // k1=2 matches only (7<8) -> cnt 1, sum_l 7, sum_r 8. Before the fix the
    // fused plan dropped l.v < r.v and returned k1=1/4/40/52, k1=2/4/14/26
    // plus a spurious k1=3 group.
    assertTrue(reference.toString().contains("[1, 3, 25, 46]"),
            "unexpected reference result: " + reference)
    assertTrue(reference.toString().contains("[2, 1, 7, 8]"),
            "unexpected reference result: " + reference)

    sql "SET experimental_enable_group_join_fusion = true"

    // A join with a residual non-equi ON conjunct must NOT be fused: the fused
    // operator cannot evaluate the per-pair predicate, so the query must fall
    // back to the regular hash join + aggregation path.
    def residualPlan = sql "EXPLAIN " + residualQuery
    assertFalse(residualPlan.toString().contains("VGROUP JOIN"),
            "residual query must not be fused, plan: " + residualPlan)

    // The residual conjunct must be applied: fused result == reference result.
    def residualFused = sql residualQuery
    assertEquals(reference, residualFused)

    // Positive control: a pure equi-key query of the same shape must still be
    // fused, so the residual gate does not disable the fusion feature entirely.
    def equiQuery = """
        SELECT l.k1, COUNT(*) AS cnt, SUM(r.v) AS total_v
        FROM gj_res_left l
        JOIN [shuffle] gj_res_right r ON l.k1 = r.k1
        GROUP BY l.k1
        ORDER BY l.k1
        """
    def equiPlan = sql "EXPLAIN " + equiQuery
    assertTrue(equiPlan.toString().contains("VGROUP JOIN"),
            "equi-key query should still fuse, plan: " + equiPlan)

    // Restore defaults so other suites are not affected.
    sql "SET experimental_enable_group_join_fusion = false"
    sql "SET runtime_filter_mode = 'GLOBAL'"
}
