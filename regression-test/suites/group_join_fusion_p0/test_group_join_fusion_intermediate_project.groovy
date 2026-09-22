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

// Regression for GroupJoin fusion + an intermediate computing Project between the
// aggregate and the join (Scheme A: the Project must be a pure passthrough).
//
// With eager aggregation (eager_aggregation_mode=1) on a group-by-key shuffle join, both
// join children are pre-aggregated streams, and a Project between the top-level aggregate
// and the join re-multiplies the per-side counts/sums (count#11 = cntL*cntR,
// sum#12 = sR*cntL, sum#13 = sL*cntR) to restore the join multiplicity. The fused GroupJoin
// operator keeps per-key ROW counts per side; it has no per-join-row expression stage, so it
// can never evaluate that Project. Previously the eligibility gate only tested whether the
// aggregate's input slots existed on the join children — under eager-agg the Project outputs
// reuse the pre-aggregate columns' ExprIds, so the gate passed and fusion silently dropped the
// Project, returning COUNT 2 / SUM 30 instead of COUNT 4 / SUM 60.
//
// Fix: the intermediate Project between the aggregate and the join is only fusable when every
// one of its outputs is a bare slot already produced by a join child (pure passthrough /
// column pruning). Any computing Project makes the shape fall back to the ordinary
// HashJoinNode + AggregationNode path, which evaluates the Project and keeps its semantics.
suite("test_group_join_fusion_intermediate_project") {
    sql "DROP TABLE IF EXISTS gj_weight_left"
    sql "DROP TABLE IF EXISTS gj_weight_right"

    sql """
        CREATE TABLE gj_weight_left (
            id INT NOT NULL,
            k INT NULL,
            v INT NULL,
            s SMALLINT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES ("replication_num" = "1")
        """

    sql "CREATE TABLE gj_weight_right LIKE gj_weight_left"

    sql """INSERT INTO gj_weight_left VALUES
        (1,1,10,10), (2,1,20,20), (3,2,30,30)"""
    sql """INSERT INTO gj_weight_right VALUES
        (1,1,7,7), (2,1,11,11), (3,2,13,13)"""

    sql "ANALYZE TABLE gj_weight_left WITH SYNC"
    sql "ANALYZE TABLE gj_weight_right WITH SYNC"

    def query = """
        SELECT l.k, COUNT(*), SUM(l.v), SUM(r.v)
        FROM gj_weight_left l
        JOIN [shuffle] gj_weight_right r ON l.k = r.k
        GROUP BY l.k
        ORDER BY l.k
        """

    // Session configuration that reproduces the eager pre-aggregation shape.
    def setupEager = { ->
        sql "SET agg_phase = 1"
        sql "SET eager_aggregation_mode = 1"
        sql "SET eager_agg_broadcast_row_count = 0"
        sql "SET enable_bucketed_hash_agg = false"
        sql "SET enable_bucket_shuffle_join = false"
        sql "SET experimental_use_serial_exchange = false"
        sql "SET runtime_filter_mode = 'OFF'"
        sql "SET parallel_pipeline_task_num = 1"
        sql "SET enable_spill = false"
        sql "SET enable_aggregate_cse = true"
        sql "SET enable_sql_cache = false"
        sql "SET query_cache_force_refresh = true"
        sql "SET query_timeout = 15"
        sql "SET exec_mem_limit = 268435456"
    }

    // Reference computed with the fusion operator disabled.
    sql "SET experimental_enable_group_join_fusion = false"
    setupEager()
    def reference = sql query

    // 1. agg_phase=1 + eager aggregation: the intermediate Project computes the weights, so
    //    the plan must NOT fuse, and the result must match the reference exactly.
    sql "SET experimental_enable_group_join_fusion = true"
    def plan = sql "EXPLAIN " + query
    assertFalse(plan.toString().contains("VGROUP JOIN"),
            "computing intermediate Project must not be fused, plan: " + plan)
    def fusedPhase1 = sql query
    assertEquals(reference, fusedPhase1)

    // 2. Default agg_phase (0, Nereids auto) + eager aggregation also produces the shape and
    //    must fall back with correct results.
    sql "SET agg_phase = 0"
    sql "SET experimental_enable_group_join_fusion = false"
    def referenceDefaultPhase = sql query
    sql "SET experimental_enable_group_join_fusion = true"
    plan = sql "EXPLAIN " + query
    assertFalse(plan.toString().contains("VGROUP JOIN"),
            "computing intermediate Project (default agg_phase) must not be fused, plan: " + plan)
    def fusedDefaultPhase = sql query
    assertEquals(referenceDefaultPhase, fusedDefaultPhase)

    // 3. count(*)-only / SUM-only variants stay correct regardless of the fusion decision.
    def countOnlyQuery = """
        SELECT l.k, COUNT(*)
        FROM gj_weight_left l
        JOIN [shuffle] gj_weight_right r ON l.k = r.k
        GROUP BY l.k
        ORDER BY l.k
        """
    def sumOnlyQuery = """
        SELECT l.k, SUM(l.v)
        FROM gj_weight_left l
        JOIN [shuffle] gj_weight_right r ON l.k = r.k
        GROUP BY l.k
        ORDER BY l.k
        """
    sql "SET agg_phase = 1"
    sql "SET experimental_enable_group_join_fusion = false"
    def countOnlyRef = sql countOnlyQuery
    def sumOnlyRef = sql sumOnlyQuery
    sql "SET experimental_enable_group_join_fusion = true"
    def fusedCountOnly = sql countOnlyQuery
    assertEquals(countOnlyRef, fusedCountOnly)
    def fusedSumOnly = sql sumOnlyQuery
    assertEquals(sumOnlyRef, fusedSumOnly)

    // 4. Positive control: a plain (non-eager) one-phase aggregate over the join still fuses
    //    (no intermediate Project, or only a pure passthrough one).
    sql "SET experimental_enable_group_join_fusion = false"
    sql "SET eager_aggregation_mode = -1"
    sql "SET agg_phase = 0"
    def plainRef = sql query
    sql "SET experimental_enable_group_join_fusion = true"
    plan = sql "EXPLAIN " + query
    assertTrue(plan.toString().contains("VGROUP JOIN"),
            "plain (non-eager) shape should still fuse, plan: " + plan)
    def fusedPlain = sql query
    assertEquals(plainRef, fusedPlain)

    // Restore defaults so other suites are not affected.
    sql "SET experimental_enable_group_join_fusion = false"
    sql "SET agg_phase = 0"
    sql "SET eager_aggregation_mode = 0"
    sql "SET runtime_filter_mode = 'GLOBAL'"
}
