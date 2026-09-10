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

// Regression for GroupJoin fusion + non-final aggregate phases (agg_phase=2 etc.).
//
// The fused GroupJoin node materializes FINAL_RESULT and finalizes per-key aggregate
// state directly, so only the final one-phase aggregate (GLOBAL + INPUT_TO_RESULT) may
// fuse. When aggregation is forced to two phases (agg_phase=2), the plan puts a LOCAL
// (INPUT_TO_BUFFER) aggregate directly above the join (no exchange needed between them),
// and a merge-finalize GLOBAL aggregate above it. Fusing the LOCAL node hard-coded
// FINAL_RESULT with finalize-on evaluators while the merge above still consumes the
// partial buffer, so the BE aborted with
//   Aggregate function count result type check failed:
//   Column type String is not compatible with data type BIGINT
// The eligibility gate now rejects any aggregate whose node (or any output function)
// phase is not (GLOBAL, INPUT_TO_RESULT); such shapes stay on the ordinary
// HashJoinNode + AggregationNode path and return correct rows.
suite("test_group_join_fusion_agg_phase") {
    sql "DROP TABLE IF EXISTS gj_phase_left"
    sql "DROP TABLE IF EXISTS gj_phase_right"

    sql """
        CREATE TABLE gj_phase_left (
            k INT NOT NULL,
            v INT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 4
        PROPERTIES ("replication_num" = "1")
        """

    sql "CREATE TABLE gj_phase_right LIKE gj_phase_left"

    sql """INSERT INTO gj_phase_left VALUES (1,10), (1,20), (2,30)"""
    sql """INSERT INTO gj_phase_right VALUES (1,7), (1,11), (2,13)"""

    def query = """
        SELECT l.k, COUNT(*), SUM(l.v), SUM(r.v)
        FROM gj_phase_left l
        JOIN [shuffle] gj_phase_right r ON l.k = r.k
        GROUP BY l.k
        ORDER BY l.k
        """

    def setupPhase2 = { ->
        sql "SET agg_phase = 2"
        sql "SET eager_aggregation_mode = -1"
        sql "SET enable_bucketed_hash_agg = false"
        sql "SET enable_bucket_shuffle_join = false"
        sql "SET experimental_use_serial_exchange = false"
        sql "SET runtime_filter_mode = 'OFF'"
        sql "SET parallel_pipeline_task_num = 1"
        sql "SET enable_spill = false"
        sql "SET enable_sql_cache = false"
        sql "SET query_cache_force_refresh = true"
    }

    // Reference computed with fusion disabled.
    sql "SET experimental_enable_group_join_fusion = false"
    setupPhase2()
    def reference = sql query

    // 1. agg_phase=2 + fusion on: the LOCAL node above the join must NOT fuse (no VGROUP
    //    JOIN), and the result must match the reference (previously: hard BE error).
    sql "SET experimental_enable_group_join_fusion = true"
    def plan = sql "EXPLAIN " + query
    assertFalse(plan.toString().contains("VGROUP JOIN"),
            "two-phase LOCAL aggregate must not be fused, plan: " + plan)
    def fusedPhase2 = sql query
    assertEquals(reference, fusedPhase2)

    // 2. DISTINCT shapes under agg_phase=2 also stay on the ordinary path.
    def distinctQuery = """
        SELECT DISTINCT l.k
        FROM gj_phase_left l
        JOIN [shuffle] gj_phase_right r ON l.k = r.k
        ORDER BY l.k
        """
    sql "SET experimental_enable_group_join_fusion = false"
    def distinctRef = sql distinctQuery
    sql "SET experimental_enable_group_join_fusion = true"
    plan = sql "EXPLAIN " + distinctQuery
    assertFalse(plan.toString().contains("VGROUP JOIN"),
            "two-phase DISTINCT must not be fused, plan: " + plan)
    def fusedDistinct = sql distinctQuery
    assertEquals(distinctRef, fusedDistinct)

    def countDistinctQuery = """
        SELECT l.k, COUNT(DISTINCT l.v)
        FROM gj_phase_left l
        JOIN [shuffle] gj_phase_right r ON l.k = r.k
        GROUP BY l.k
        ORDER BY l.k
        """
    sql "SET experimental_enable_group_join_fusion = false"
    def countDistinctRef = sql countDistinctQuery
    sql "SET experimental_enable_group_join_fusion = true"
    plan = sql "EXPLAIN " + countDistinctQuery
    assertFalse(plan.toString().contains("VGROUP JOIN"),
            "two-phase COUNT(DISTINCT) must not be fused, plan: " + plan)
    def fusedCountDistinct = sql countDistinctQuery
    assertEquals(countDistinctRef, fusedCountDistinct)

    // 3. Positive control: at the default/one-phase agg the same query still fuses.
    sql "SET experimental_enable_group_join_fusion = false"
    sql "SET agg_phase = 0"
    sql "SET eager_aggregation_mode = -1"
    def onePhaseRef = sql query
    sql "SET experimental_enable_group_join_fusion = true"
    plan = sql "EXPLAIN " + query
    assertTrue(plan.toString().contains("VGROUP JOIN"),
            "one-phase aggregate should still fuse, plan: " + plan)
    def fusedOnePhase = sql query
    assertEquals(onePhaseRef, fusedOnePhase)

    // Restore defaults so other suites are not affected.
    sql "SET experimental_enable_group_join_fusion = false"
    sql "SET agg_phase = 0"
    sql "SET runtime_filter_mode = 'GLOBAL'"
}
