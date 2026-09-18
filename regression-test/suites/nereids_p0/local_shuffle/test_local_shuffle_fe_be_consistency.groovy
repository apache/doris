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
 * Verify FE-planned local exchange (enable_local_shuffle_planner=true) produces
 * the same query results as BE-native local exchange (enable_local_shuffle_planner=false)
 * across a wide set of operator/plan shapes.
 *
 * Only data correctness is asserted — the two planners legitimately differ on the
 * exact exchange counts/types they emit, so plan-shape comparison is intentionally
 * not done here. (Earlier iterations of this suite did that for diagnosis; those
 * comparisons are flaky in practice and are not appropriate for a regression net.)
 */
suite("test_local_shuffle_fe_be_consistency") {

    def setVarBase = "disable_join_reorder=true,disable_colocate_plan=true,ignore_storage_data_distribution=false,parallel_pipeline_task_num=4,auto_broadcast_join_threshold=-1,broadcast_row_count_limit=0"

    // Run the given SQL twice — once with enable_local_shuffle_planner=true (FE planner)
    // and once with =false (BE-native) — and assert the result rows are identical.
    // knownDiff is accepted for source compatibility but has no effect anymore.
    def checkConsistencyWithSql = { String tag, String testSql, boolean knownDiff = false ->
        def sqlOn  = testSql.replaceFirst(/(?i)\/\*\+SET_VAR\(([^)]*)\)\s*\*\//, "/*+SET_VAR(enable_local_shuffle_planner=true,\$1)*/")
        def sqlOff = testSql.replaceFirst(/(?i)\/\*\+SET_VAR\(([^)]*)\)\s*\*\//, "/*+SET_VAR(enable_local_shuffle_planner=false,\$1)*/")
        if (!testSql.contains("/*+SET_VAR")) {
            sqlOn  = testSql.replaceFirst(/(?i)^\s*(SELECT)\s+/, "SELECT /*+SET_VAR(enable_local_shuffle_planner=true,${setVarBase})*/ ")
            sqlOff = testSql.replaceFirst(/(?i)^\s*(SELECT)\s+/, "SELECT /*+SET_VAR(enable_local_shuffle_planner=false,${setVarBase})*/ ")
        }
        check_sql_equal(sqlOn, sqlOff)
    }

    // ============================================================
    //  Common settings
    // ============================================================
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET runtime_filter_mode=off"
    sql "SET parallel_pipeline_task_num=4"
    sql "SET enable_sql_cache=false"
    // Keep local shuffle feature globally enabled; only toggle the planner flag
    sql "SET enable_local_shuffle=true"
    // Disable ignore_storage_data_distribution to get predictable plans from scans
    sql "SET ignore_storage_data_distribution=false"

    // ============================================================
    //  Table setup
    //  ls_t1: HASH(k1) 8 buckets
    //  ls_t2: HASH(k1) 8 buckets  (same distribution → colocate-eligible)
    //  ls_t3: HASH(k4) 5 buckets  (different distribution)
    //  ls_serial: HASH(k1) 2 buckets (for serial-scan tests: 2 < parallel_pipeline_task_num=4)
    // ============================================================
    sql "DROP TABLE IF EXISTS ls_t1"
    sql "DROP TABLE IF EXISTS ls_t2"
    sql "DROP TABLE IF EXISTS ls_t3"
    sql "DROP TABLE IF EXISTS ls_serial"

    sql """
        CREATE TABLE ls_t1 (
            k1 INT NOT NULL,
            k2 INT,
            v1 INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 8
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        CREATE TABLE ls_t2 (
            k1 INT NOT NULL,
            k3 INT,
            v2 INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1, k3)
        DISTRIBUTED BY HASH(k1) BUCKETS 8
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        CREATE TABLE ls_t3 (
            k1 INT NOT NULL,
            k4 INT,
            v3 INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1, k4)
        DISTRIBUTED BY HASH(k4) BUCKETS 5
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        INSERT INTO ls_t1 VALUES
            (1, 10, 2), (1, 11, 3), (2, 20, 4), (2, 21, 1),
            (3, 30, 5), (4, 40, 6), (5, 50, 7), (6, 60, 8),
            (7, 70, 9), (8, 80, 10), (9, 90, 11), (10, 100, 12)
    """

    sql """
        INSERT INTO ls_t2 VALUES
            (1, 100, 7), (1, 101, 1), (2, 200, 2), (3, 300, 3),
            (4, 400, 4), (5, 500, 5), (6, 600, 6), (7, 700, 7)
    """

    sql """
        INSERT INTO ls_t3 VALUES
            (1, 1001, 5), (1, 1001, 6), (2, 1002, 7),
            (3, 1003, 8), (4, 1004, 9), (5, 1005, 10)
    """

    sql """
        CREATE TABLE ls_serial (
            k1 INT NOT NULL,
            k2 INT,
            v1 INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO ls_serial VALUES
            (1, 10, 2), (2, 20, 4), (3, 30, 5), (4, 40, 6)
    """

    // SET_VAR prefix used in most test SQLs (disables plan reorder/colocate for deterministic plans)
    def sv = "/*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,ignore_storage_data_distribution=false,parallel_pipeline_task_num=4,auto_broadcast_join_threshold=-1,broadcast_row_count_limit=0)*/"
    // Same as sv but forces serial source path (default in many environments)
    def svSerialSource = "/*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,ignore_storage_data_distribution=true,parallel_pipeline_task_num=4,auto_broadcast_join_threshold=-1,broadcast_row_count_limit=0)*/"

    // ================================================================
    // Section 1: AggSink / StreamingAgg scenarios
    // BE operator: AggSinkOperatorX / StreamingAggOperatorX
    // ================================================================

    // 1-1: AggSink finalize, no group key
    //      Finalize phase: FE uses noRequire() (needsFinalize && groupingExprs.isEmpty()) → NOOP
    //      Streaming pre-agg phase: FE uses requirePassthrough() (else branch, groupingExprs.isEmpty())
    //      BE: does not add exchanges for no-group-key agg (NOOP)
    //      Known diff: FE inserts PASSTHROUGH for streaming pre-agg; BE skips exchanges entirely
    checkConsistencyWithSql("agg_finalize_no_group_key",
        "SELECT ${sv} count(*) FROM ls_t1")

    // 1-2: AggSink 1-phase, bucket key (k1) → no mismatch (distribution matches)
    //      BE: BUCKET_HASH_SHUFFLE for sink, scan already provides BUCKET_HASH_SHUFFLE
    //      → need_to_local_exchange returns false (both hash types match)
    checkConsistencyWithSql("agg_1phase_bucket_key",
        "SELECT ${sv} k1, count(*) AS cnt FROM ls_t1 GROUP BY k1 ORDER BY k1")

    // 1-2b: Same SQL under serial-source mode (ignore_storage_data_distribution=true)
    //       This explicitly validates FE/BE consistency under serial-source planning path.
    checkConsistencyWithSql("agg_1phase_bucket_key_serial_source",
        "SELECT ${svSerialSource} k1, count(*) AS cnt FROM ls_t1 GROUP BY k1 ORDER BY k1")

    // 1-2c: Finalize agg, serial/pooling scan, bucket key (k1), ls_serial (2 buckets).
    //       Pooling scan + bucket-key colocate agg: BE inserts PASSTHROUGH fan-out (heavy_ops
    //       bottleneck avoidance before LOCAL_HASH_SHUFFLE) + LOCAL_HASH_SHUFFLE.
    //       FE mirrors with heavy_ops check in enforceChild.
    checkConsistencyWithSql("agg_finalize_serial_pooling_bucket",
        "SELECT ${svSerialSource} k1, count(*) AS cnt FROM ls_serial GROUP BY k1 ORDER BY k1")

    // 1-2c2: Same finalize agg with bucket key, but non-pooling (ignore_storage_data_distribution=false).
    //        No serial source → no heavy_ops PASSTHROUGH fan-out needed.
    //        Known mismatch on clusters where ls_serial (2 BUCKETS) tablets land on different BEs:
    //        FE sees global instanceCount=2 → inserts LE; each BE sees local _num_instances=1 → skips.
    //        This is a pre-existing FE/BE instanceCount discrepancy, not a planner bug.
    checkConsistencyWithSql("agg_finalize_non_pooling_bucket",
        "SELECT ${sv} k1, count(*) AS cnt FROM ls_serial GROUP BY k1 ORDER BY k1")

    // 1-2d: Agg, serial/pooling scan, non-bucket key (k2), ls_serial.
    checkConsistencyWithSql("agg_finalize_serial_pooling_non_bucket",
        "SELECT ${svSerialSource} k2, count(*) AS cnt FROM ls_serial GROUP BY k2 ORDER BY k2")

    // 1-3: AggSink 1-phase, non-bucket key (k2)
    //      BE: GLOBAL_EXECUTION_HASH_SHUFFLE vs BUCKET_HASH_SHUFFLE from scan
    //      → inserts GLOBAL_HASH_SHUFFLE (or LOCAL_HASH_SHUFFLE for local execution)
    checkConsistencyWithSql("agg_1phase_non_bucket_key",
        "SELECT ${sv} k2, count(*) AS cnt FROM ls_t1 GROUP BY k2 ORDER BY k2")

    // 1-4: AggSink 1-phase, multi-column non-bucket key (k1,k2)
    //      Even though k1 is the bucket key, (k1,k2) is not → GLOBAL_HASH_SHUFFLE
    checkConsistencyWithSql("agg_1phase_multi_key_non_bucket",
        "SELECT ${sv} k1, k2, count(*) AS cnt FROM ls_t1 GROUP BY k1, k2 ORDER BY k1, k2")

    // 1-5: Two-phase agg (pre-agg + finalize), non-bucket key
    //      Tests that both streaming agg pre-phase and finalize phases are handled correctly
    checkConsistencyWithSql("agg_two_phase_non_bucket",
        "SELECT ${sv} k2, sum(v1) AS s FROM ls_t1 GROUP BY k2 ORDER BY k2")

    // ================================================================
    // Section 2: DistinctStreamingAgg scenarios
    // BE operator: DistinctStreamingAggOperatorX
    // ================================================================

    // 2-1: DISTINCT on bucket key → no extra exchange needed (distribution matches)
    checkConsistencyWithSql("distinct_bucket_key",
        "SELECT ${sv} DISTINCT k1 FROM ls_t1 ORDER BY k1")

    // 2-2: DISTINCT on non-bucket key
    checkConsistencyWithSql("distinct_non_bucket_key",
        "SELECT ${sv} DISTINCT k2 FROM ls_t1 ORDER BY k2")

    // 2-3: DISTINCT on multiple non-bucket keys
    checkConsistencyWithSql("distinct_multi_non_bucket",
        "SELECT ${sv} DISTINCT k1, k2 FROM ls_t1 ORDER BY k1, k2")

    // ================================================================
    // Section 3: AnalyticSink / SortSink (analytic) scenarios
    // BE operators: AnalyticSinkOperatorX, SortSinkOperatorX
    // ================================================================

    // 3-1: Analytic window, no PARTITION BY → serial path, no exchange needed
    checkConsistencyWithSql("analytic_no_partition",
        "SELECT ${sv} k1, sum(v1) OVER() AS s FROM ls_t1 ORDER BY k1, s")

    // 3-2: Analytic window, PARTITION BY non-bucket key → GLOBAL_HASH_SHUFFLE
    //      Also triggers SortSink (analytic sort) → GLOBAL_HASH_SHUFFLE
    checkConsistencyWithSql("analytic_partition_non_bucket",
        "SELECT ${sv} k1, k2, row_number() OVER(PARTITION BY k2 ORDER BY k1) AS rn FROM ls_t1 ORDER BY k2, k1, rn")

    // 3-3: Analytic window, PARTITION BY bucket key → BUCKET_HASH_SHUFFLE (or no extra exchange)
    //      SortSink(analytic): if colocate+bucket → BUCKET_HASH_SHUFFLE
    checkConsistencyWithSql("analytic_partition_bucket_key",
        "SELECT ${sv} k1, sum(v1) OVER(PARTITION BY k1) AS s FROM ls_t1 ORDER BY k1, s")

    // 3-4: ORDER BY sort (SortSink._merge_by_exchange=true) → PASSTHROUGH
    checkConsistencyWithSql("sort_order_by",
        "SELECT ${sv} * FROM ls_t1 ORDER BY k1, k2 LIMIT 10")

    // ================================================================
    // Section 4: PartitionSortSink scenarios
    // BE operator: PartitionSortSinkOperatorX
    // ================================================================

    // 4-1: PartitionSort TWO_PHASE_GLOBAL (triggered by QUALIFY / ROW_NUMBER with LIMIT)
    //      → GLOBAL_EXECUTION_HASH_SHUFFLE
    checkConsistencyWithSql("partition_sort_two_phase_global",
        """SELECT ${sv} k1, k2, v1
           FROM (
               SELECT k1, k2, v1,
                      ROW_NUMBER() OVER(PARTITION BY k2 ORDER BY v1 DESC) AS rn
               FROM ls_t1
           ) t
           WHERE rn <= 2
           ORDER BY k1, k2""")

    // 4-2: PartitionSort single phase (TWO_PHASE_LOCAL or ONE_PHASE) → PASSTHROUGH
    //      Note: This depends on the optimizer's choice; TopN on non-partitioned window
    checkConsistencyWithSql("partition_sort_single_phase",
        """SELECT ${sv} k1, k2, v1
           FROM (
               SELECT k1, k2, v1,
                      ROW_NUMBER() OVER(PARTITION BY k2 ORDER BY v1 DESC) AS rn
               FROM ls_t1
           ) t
           WHERE rn = 1
           ORDER BY k1, k2""")

    // ================================================================
    // Section 5: HashJoinProbe / HashJoinBuildSink scenarios
    // BE operators: HashJoinProbeOperatorX, HashJoinBuildSinkOperatorX
    // ================================================================

    // 5-1: Broadcast join — probe NOOP (or PASSTHROUGH if serial), build PASS_TO_ONE (if serial)
    checkConsistencyWithSql("hash_join_broadcast",
        """SELECT ${sv} a.k1, a.v1, b.v2
           FROM ls_t1 a JOIN [broadcast] ls_t2 b ON a.k1 = b.k1
           ORDER BY a.k1, a.k2""")

    // 5-2: Shuffle (PARTITIONED) join → probe GLOBAL_HASH_SHUFFLE, build GLOBAL_HASH_SHUFFLE
    checkConsistencyWithSql("hash_join_shuffle",
        """SELECT ${sv} a.k1, a.v1, b.v2
           FROM ls_t1 a JOIN [shuffle] ls_t2 b ON a.k1 = b.k1
           ORDER BY a.k1, a.k2""")

    // 5-3: LEFT OUTER shuffle join
    //      Known diff: In single-BE environments, BE's pipeline-level need_to_local_exchange()
    //      may skip exchanges when num_tasks_of_parent<=1, while FE still inserts PASSTHROUGH
    //      because it lacks pipeline-level task count information.
    checkConsistencyWithSql("hash_join_left_outer_shuffle",
        """SELECT ${sv} a.k1, a.v1, b.v2
           FROM ls_t1 a LEFT OUTER JOIN [shuffle] ls_t2 b ON a.k1 = b.k1
           ORDER BY a.k1, a.k2""")

    // 5-4: RIGHT OUTER shuffle join
    checkConsistencyWithSql("hash_join_right_outer_shuffle",
        """SELECT ${sv} a.k1, a.v1, b.v2
           FROM ls_t1 a RIGHT OUTER JOIN [shuffle] ls_t2 b ON a.k1 = b.k1
           ORDER BY a.k1, b.k3""")

    // 5-5: FULL OUTER shuffle join
    checkConsistencyWithSql("hash_join_full_outer_shuffle",
        """SELECT ${sv} a.k1, a.v1, b.v2
           FROM ls_t1 a FULL OUTER JOIN [shuffle] ls_t2 b ON a.k1 = b.k1
           ORDER BY a.k1, b.k3""")

    // 5-6: LEFT SEMI shuffle join
    checkConsistencyWithSql("hash_join_left_semi_shuffle",
        """SELECT ${sv} a.k1, a.v1
           FROM ls_t1 a LEFT SEMI JOIN [shuffle] ls_t2 b ON a.k1 = b.k1
           ORDER BY a.k1""")

    // 5-7: LEFT ANTI shuffle join
    //      Known diff: Same as LEFT OUTER — BE pipeline-level num_tasks_of_parent<=1 check
    //      skips exchanges in single-BE environments; FE cannot replicate this.
    checkConsistencyWithSql("hash_join_left_anti_shuffle",
        """SELECT ${sv} a.k1, a.v1
           FROM ls_t1 a LEFT ANTI JOIN [shuffle] ls_t2 b ON a.k1 = b.k1
           ORDER BY a.k1""")

    // 5-8: NULL_AWARE_LEFT_ANTI_JOIN (NOT IN subquery with nullable) → NOOP
    //      BE: both probe and build NOOP
    checkConsistencyWithSql("hash_join_null_aware_left_anti",
        """SELECT ${sv} k1, v1 FROM ls_t1
           WHERE k1 NOT IN (SELECT k1 FROM ls_t2)
           ORDER BY k1""")

    // ================================================================
    // Section 6: NestedLoopJoin scenarios
    // BE operators: NestedLoopJoinProbeOperatorX, NestedLoopJoinBuildSinkOperatorX
    // ================================================================

    // 6-1: NLJ INNER (cross/theta join) → probe ADAPTIVE_PASSTHROUGH, build BROADCAST (serial)
    //      FE: requireAdaptivePassthrough for probe; BE: ADAPTIVE_PASSTHROUGH for probe
    checkConsistencyWithSql("nlj_inner_theta",
        """SELECT ${sv} a.k1, b.k1 AS bk1
           FROM ls_t1 a, ls_t2 b WHERE a.k1 > b.k1
           ORDER BY a.k1, bk1 LIMIT 20""")

    // 6-2: NLJ LEFT OUTER
    checkConsistencyWithSql("nlj_left_outer",
        """SELECT ${sv} a.k1, b.k1 AS bk1
           FROM ls_t1 a LEFT OUTER JOIN ls_t2 b ON a.k1 > b.k1
           ORDER BY a.k1, bk1 LIMIT 20""")

    // 6-3: NLJ RIGHT OUTER → BE probe: NOOP; FE: ADAPTIVE_PASSTHROUGH (known difference)
    //      FE uses requireAdaptivePassthrough unconditionally for non-NULL_AWARE NLJ
    //      BE uses NOOP for RIGHT_OUTER/RIGHT_SEMI/RIGHT_ANTI/FULL_OUTER
    checkConsistencyWithSql("nlj_right_outer", """
        SELECT ${sv} a.k1, b.k1 AS bk1
        FROM ls_t1 a RIGHT OUTER JOIN ls_t2 b ON a.k1 < b.k1
        ORDER BY a.k1, bk1 LIMIT 20
    """)

    // 6-4: NLJ FULL OUTER → same known difference as RIGHT_OUTER
    checkConsistencyWithSql("nlj_full_outer", """
        SELECT ${sv} a.k1, b.k1 AS bk1
        FROM ls_t1 a FULL OUTER JOIN ls_t2 b ON a.k1 > b.k1
        ORDER BY a.k1, bk1 LIMIT 20
    """)

    // ================================================================
    // Section 7: Set operations (INTERSECT / EXCEPT)
    // BE operators: SetSinkOperatorX, SetProbeSinkOperatorX, SetSourceOperatorX
    // ================================================================

    // 7-1: INTERSECT → GLOBAL_HASH_SHUFFLE for both set sink and set probe sink
    checkConsistencyWithSql("set_intersect",
        """SELECT ${sv} k1 FROM ls_t1
           INTERSECT
           SELECT k1 FROM ls_t2
           ORDER BY k1""")

    // 7-2: EXCEPT → GLOBAL_HASH_SHUFFLE
    checkConsistencyWithSql("set_except",
        """SELECT ${sv} k1 FROM ls_t1
           EXCEPT
           SELECT k1 FROM ls_t2
           ORDER BY k1""")

    // 7-3: Three-way INTERSECT
    //      FE and BE are consistent:
    //      - ls_t1/ls_t2 (colocated on k1): DISTINCT_STREAMING_AGG with needsFinalize=true
    //        → BE returns NOOP/HASH (already satisfied), FE requireHash (already satisfied) → no exchange
    //      - ls_t3 (non-colocated, partition on k4): DISTINCT_STREAMING_AGG with needsFinalize=false
    //        → BE returns PASSTHROUGH (enable_distinct_streaming_agg_force_passthrough=true),
    //           FE enableDistinctStreamingAggForcePassthrough=true → requirePassthrough → insert PASSTHROUGH
    checkConsistencyWithSql("set_intersect_three_way",
        """SELECT ${sv} k1 FROM ls_t1
           INTERSECT
           SELECT k1 FROM ls_t2
           INTERSECT
           SELECT k1 FROM ls_t3
           ORDER BY k1""")

    // ================================================================
    // Section 8: UNION scenarios
    // BE operators: UnionSinkOperatorX, UnionSourceOperatorX
    // ================================================================

    // 8-1: UNION ALL (no downstream shuffled op → base default)
    checkConsistencyWithSql("union_all_simple",
        """SELECT ${sv} k1, v1 FROM ls_t1
           UNION ALL
           SELECT k1, v2 FROM ls_t2
           ORDER BY k1, v1""")

    // 8-2: UNION ALL feeding into GROUP BY (union followed by shuffled agg)
    checkConsistencyWithSql("union_all_followed_by_agg",
        """SELECT ${sv} k1, count(*) AS cnt
           FROM (
               SELECT k1, v1 AS v FROM ls_t1
               UNION ALL
               SELECT k1, v2 AS v FROM ls_t2
           ) u
           GROUP BY k1
           ORDER BY k1""")

    // 8-3: UNION followed by analytic window
    checkConsistencyWithSql("union_all_followed_by_window",
        """SELECT ${sv} k1, SUM(v) OVER(PARTITION BY k1) AS sv
           FROM (
               SELECT k1, v1 AS v FROM ls_t1
               UNION ALL
               SELECT k1, v2 AS v FROM ls_t2
           ) u
           ORDER BY k1, sv""")

    // ================================================================
    // Section 9: TableFunction and AssertNumRows
    // BE operators: TableFunctionOperatorX, AssertNumRowsOperatorX
    // ================================================================

    // 9-1: TableFunction (non-pooling) → PASSTHROUGH×2
    //      BE TableFunctionOperatorX overrides required_data_distribution() to always return
    //      PASSTHROUGH; need_to_local_exchange Step 4 always inserts non-hash exchanges.
    //      So: OlapScan → PT → TableFunc → PT → Sort. Total: 2 PASSTHROUGH.
    //      FE mirrors: TableFunctionNode requires PASSTHROUGH from child (outputs NOOP),
    //      SortNode independently inserts PASSTHROUGH for mergeByExchange.
    checkConsistencyWithSql("table_function",
        """SELECT ${sv} k1, e1 FROM ls_t1
           LATERAL VIEW explode_numbers(v1) tmp AS e1
           ORDER BY k1, e1 LIMIT 20""")

    // 9-1b: TableFunction (pooling scan) → PASSTHROUGH×2
    //       Same as 9-1: TableFunctionOperatorX always requires PASSTHROUGH regardless of child.
    //       Pooling scan (serial) → PT fan-out → TableFunc → PT → Sort. Total: 2 PASSTHROUGH.
    //       FE mirrors: TableFunctionNode requires PASSTHROUGH (outputs NOOP),
    //       SortNode independently inserts PASSTHROUGH for mergeByExchange.
    checkConsistencyWithSql("table_function_pooling",
        """SELECT ${svSerialSource} k1, e1 FROM ls_t1
           LATERAL VIEW explode_numbers(v1) tmp AS e1
           ORDER BY k1, e1 LIMIT 20""")

    // 9-2: AssertNumRows (scalar subquery) → PASSTHROUGH
    //      Known diff: In single-BE environments, FE and BE may disagree on instance counts
    //      for fragments containing AssertNumRows, leading to different exchange decisions.
    checkConsistencyWithSql("assert_num_rows",
        """SELECT ${sv} k1, (SELECT count(*) FROM ls_t2 WHERE ls_t2.k1 = ls_t1.k1) AS cnt
           FROM ls_t1
           ORDER BY k1""")

    // ================================================================
    // Section 10: Mixed / multi-level scenarios
    // ================================================================

    // 10-1: Agg after shuffle join (k1 is bucket key → no extra exchange after join)
    checkConsistencyWithSql("agg_after_shuffle_join_bucket_key",
        """SELECT ${sv} a.k1, count(*) AS cnt
           FROM ls_t1 a JOIN [shuffle] ls_t2 b ON a.k1 = b.k1
           GROUP BY a.k1
           ORDER BY a.k1""")

    // 10-2: Agg after shuffle join on non-bucket column
    //      GROUP BY k2 ≠ join key k1. BE's StreamingAggOperatorX sees child is_hash_join_probe()
    //      and returns PASSTHROUGH (enable_streaming_agg_hash_join_force_passthrough=true by default),
    //      splitting the pipeline at the streaming pre-agg/join boundary.
    //      FE replicates this: AggregationNode detects useStreamingPreagg && child is HashJoinNode
    //      → requirePassthrough. Both FE and BE produce 18 PASSTHROUGH exchanges.
    checkConsistencyWithSql("agg_after_shuffle_join_non_bucket_key",
        """SELECT ${sv} a.k2, count(*) AS cnt
           FROM ls_t1 a JOIN [shuffle] ls_t2 b ON a.k1 = b.k1
           GROUP BY a.k2
           ORDER BY a.k2""")

    // 10-3: Agg after broadcast join
    //      ScanNode returns BUCKET_HASH_SHUFFLE (mirroring BE's ScanOperator). HashJoinNode
    //      (broadcast, non-serial probe) propagates probe side's distribution as its own
    //      output type (instead of hardcoding NOOP). AggNode sees BUCKET_HASH_SHUFFLE,
    //      RequireHash.satisfy(BUCKET_HASH_SHUFFLE)=true → no redundant hash exchange.
    //      Mirrors BE's !(hash && hash) check in need_to_local_exchange.
    checkConsistencyWithSql("agg_after_broadcast_join",
        """SELECT ${sv} a.k1, count(*) AS cnt
           FROM ls_t1 a JOIN [broadcast] ls_t2 b ON a.k1 = b.k1
           GROUP BY a.k1
           ORDER BY a.k1""")

    // 10-4: Window after UNION
    //      Known diff: In single-BE environments, FE instance-count-based skipping and
    //      BE pipeline-level num_tasks checks can diverge for union+window fragments.
    checkConsistencyWithSql("window_after_union",
        """SELECT ${sv} k1, SUM(v) OVER(PARTITION BY k1) AS sv
           FROM (
               SELECT k1, v1 AS v FROM ls_t1
               UNION ALL
               SELECT k1, v2 AS v FROM ls_t2
           ) u
           ORDER BY k1, sv""")

    // 10-5: Multi-level join + agg + window
    checkConsistencyWithSql("join_agg_window_multilevel",
        """SELECT ${sv} t.k1, t.cnt,
                  row_number() OVER(ORDER BY t.cnt DESC, t.k1 ASC) AS rn
           FROM (
               SELECT a.k1, count(*) AS cnt
               FROM ls_t1 a JOIN [shuffle] ls_t2 b ON a.k1 = b.k1
               GROUP BY a.k1
           ) t
           ORDER BY t.k1""")

    // 10-6: Two shuffle joins chained
    checkConsistencyWithSql("two_shuffle_joins",
        """SELECT ${sv} a.k1, b.k3, c.k4
           FROM ls_t1 a
           JOIN [shuffle] ls_t2 b ON a.k1 = b.k1
           JOIN [shuffle] ls_t3 c ON a.k1 = c.k1
           ORDER BY a.k1, b.k3 LIMIT 20""")

    // 10-7: Complex: join → agg → agg (double-layer aggregation after join)
    //      Known diff: Multi-level agg fragments may have single instances in single-BE
    //      environments, causing FE/BE exchange decision divergence.
    checkConsistencyWithSql("complex_join_double_agg",
        """SELECT ${sv} z.k1, SUM(z.metric) AS total
           FROM (
               SELECT y.k1, SUM(y.metric) AS metric
               FROM (
                   SELECT a.k1, SUM(a.v1 + b.v2) AS metric
                   FROM ls_t1 a JOIN [shuffle] ls_t2 b ON a.k1 = b.k1
                   GROUP BY a.k1
               ) y
               GROUP BY y.k1
           ) z
           GROUP BY z.k1
           ORDER BY z.k1""")

    // 10-8: Agg then INTERSECT
    checkConsistencyWithSql("agg_then_intersect",
        """SELECT ${sv} k1
           FROM (SELECT k1, count(*) AS cnt FROM ls_t1 GROUP BY k1 HAVING cnt > 0) a
           INTERSECT
           SELECT k1
           FROM (SELECT k1, count(*) AS cnt FROM ls_t2 GROUP BY k1 HAVING cnt > 0) b
           ORDER BY k1""")

    // 10-9: Shuffle join then DISTINCT
    checkConsistencyWithSql("shuffle_join_then_distinct",
        """SELECT ${sv} DISTINCT a.k1
           FROM ls_t1 a JOIN [shuffle] ls_t2 b ON a.k1 = b.k1
           ORDER BY a.k1""")

    // ================================================================
    // Section 11: AggSink LOCAL_HASH_SHUFFLE scenarios
    // Scenarios where BE's need_to_local_exchange() inserts GLOBAL/BUCKET
    // hash exchange because the source distribution is not hash-compatible.
    //
    // Key rule in pipeline.cpp need_to_local_exchange():
    //   If source is BUCKET_HASH and sink requires GLOBAL_HASH → both are hash
    //   → need_to_local_exchange returns false → NO local exchange.
    //   But if source is PASSTHROUGH/NOOP → not both hash → insert GLOBAL_HASH.
    // ================================================================

    // 11-1: force_to_local_shuffle=true + non-bucket finalize agg
    //       With force_to_local_shuffle, OlapScanNode.isSerialOperator()=true even with 8 tablets.
    //       Optimizer puts agg in a separate finalize fragment receiving hash-partitioned data,
    //       so no LOCAL_HASH_SHUFFLE is generated — only PASSTHROUGH for the NLJ/scan boundary.
    checkConsistencyWithSql("agg_finalize_force_local_shuffle_non_bucket",
        """SELECT /*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,
                             ignore_storage_data_distribution=false,parallel_pipeline_task_num=4,
                             force_to_local_shuffle=true,enable_local_shuffle=true)*/ k2, count(*) AS cnt
           FROM ls_t1 GROUP BY k2 ORDER BY k2""")

    // 11-2: force_to_local_shuffle=true + bucket-key finalize agg
    //       GROUP BY k1 (bucket key of ls_t1): colocate agg stays in same fragment as scan.
    //       FE: AggNode is colocate → requireHash. BE: AggSink returns BUCKET_HASH.
    //       Result MATCH: [PASSTHROUGH:9], consistent with other bucket-key colocate cases.
    checkConsistencyWithSql("agg_finalize_force_local_shuffle_bucket_key",
        """SELECT /*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,
                             ignore_storage_data_distribution=false,parallel_pipeline_task_num=4,
                             force_to_local_shuffle=true,enable_local_shuffle=true)*/ k1, count(*) AS cnt
           FROM ls_t1 GROUP BY k1 ORDER BY k1""")

    // 11-3: NLJ (theta join) → finalize agg on non-bucket key
    //       GROUP BY k2 (non-bucket): optimizer puts agg in a separate finalize fragment
    //       receiving data via a hash-partitioned inter-fragment exchange on k2.
    //       Within each fragment, the distributions are compatible → no LOCAL_HASH_SHUFFLE.
    //       Result MATCH: [ADAPTIVE_PASSTHROUGH:5, PASSTHROUGH:5]
    checkConsistencyWithSql("agg_after_nlj_non_bucket",
        """SELECT /*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,
                             ignore_storage_data_distribution=false,parallel_pipeline_task_num=4,
                             auto_broadcast_join_threshold=-1,broadcast_row_count_limit=0)*/ a.k2, count(*) AS cnt
           FROM ls_t1 a, ls_t2 b WHERE a.k1 > b.k1
           GROUP BY a.k2 ORDER BY a.k2""")

    // 11-4: NLJ (theta join) → finalize agg on bucket key (LOCAL_HASH_SHUFFLE test)
    //       GROUP BY k1 (bucket key): colocate agg stays in same pipeline as NLJ probe.
    //       The NLJ probe requires ADAPTIVE_PASSTHROUGH → local exchange inserted.
    //       After that exchange, the next pipeline has LocalExchangeSource (PASSTHROUGH distribution)
    //       feeding into AggSink (BUCKET_HASH_SHUFFLE for colocate k1 agg).
    //       PASSTHROUGH source ≠ BUCKET_HASH target, not both-hash → need_to_local_exchange=true
    //       → BE inserts LOCAL_HASH_SHUFFLE (BUCKET type).
    //       FE: AggNode isColocated=true → requireHash → inserts LocalExchangeNode.
    //       Result MATCH: [ADAPTIVE_PASSTHROUGH:9, LOCAL_HASH_SHUFFLE:9, PASSTHROUGH:9]
    //       This is a primary test for regular AggSink generating LOCAL_HASH_SHUFFLE.
    checkConsistencyWithSql("agg_after_nlj_bucket_key",
        """SELECT /*+SET_VAR(disable_join_reorder=true,disable_colocate_plan=true,
                             ignore_storage_data_distribution=false,parallel_pipeline_task_num=4,
                             auto_broadcast_join_threshold=-1,broadcast_row_count_limit=0)*/ a.k1, count(*) AS cnt
           FROM ls_t1 a, ls_t2 b WHERE a.k1 > b.k1
           GROUP BY a.k1 ORDER BY a.k1""")

    // ================================================================
    // Section 12: Nested NLJ with pooling scan
    // Tests that FE correctly inserts local exchange on ALL NLJ build sides,
    // even when the NLJ's direct children are not ScanNodes (e.g., nested NLJ
    // or ExchangeNode). Without the fix, the serial Exchange on NLJ(outer)'s
    // build side would reduce num_tasks to 1, causing "must set shared state,
    // in CROSS_JOIN_OPERATOR" for instances 1+.
    // ================================================================

    // 12-1: Nested NLJ with pooling scan — the regression case from RQG.
    //       Two LEFT JOINs with non-equi conditions → two nested NLJ operators.
    //       The outer NLJ's build side is an Exchange (UNPARTITIONED, serial).
    //       FE must insert a BROADCAST local exchange there to fan out to all instances.
    //       Without the fix in NestedLoopJoinNode (removing instanceof ScanNode check),
    //       FE wouldn't insert the local exchange → "must set shared state" error.
    //       BE-native also fails on this query with "_num_remaining_senders: -N",
    //       so we only verify FE mode produces correct results (skip profile comparison).
    //       knownDiff=true to tolerate the BE failure in profile comparison.
    checkConsistencyWithSql("nested_nlj_pooling_scan",
        """SELECT ${svSerialSource} count(a.k1) AS cnt, a.v1
           FROM ls_serial a
           LEFT JOIN ls_serial b ON b.k2 >= b.k2
           LEFT JOIN ls_serial c ON b.k1 >= b.k1
           WHERE a.k1 IS NOT NULL
           GROUP BY a.v1
           ORDER BY cnt, a.v1""", true)

    // 12-2: Same nested NLJ but non-pooling (ignore_storage_data_distribution=false).
    //       FE uses manual force-enforce to always insert ADAPTIVE_PASSTHROUGH
    //       on NLJ probe side, matching BE's need_to_local_exchange Step 4 behavior.
    //       Known mismatch on clusters where ls_serial (2 BUCKETS) tablets span multiple BEs:
    //       same FE/BE instanceCount discrepancy as agg_finalize_non_pooling_bucket.
    checkConsistencyWithSql("nested_nlj_non_pooling",
        """SELECT ${sv} count(a.k1) AS cnt, a.v1
           FROM ls_serial a
           LEFT JOIN ls_serial b ON b.k2 >= b.k2
           LEFT JOIN ls_serial c ON b.k1 >= b.k1
           WHERE a.k1 IS NOT NULL
           GROUP BY a.v1
           ORDER BY cnt, a.v1""")

    // ================================================================
    // Section 13: Pooling scan + operators requiring shared state
    // Regression cases from RQG build 183677 — serial Exchange on build
    // side of various operators (Agg, Sort, Union/Repeat) reduced pipeline
    // num_tasks, causing "must set shared state" errors.
    // Fixed by restoring the num_tasks raise in _create_deferred_local_exchangers
    // for non-scan serial operators.
    // ================================================================

    // 13-1: NLJ + AGG with pooling scan.
    //       NLJ creates pipeline boundary; serial Exchange on build side
    //       needs raise to _num_instances for AGG shared state injection.
    //       knownDiff=true: pooling scan + NLJ has FE/BE exchange count
    //       differences (same root cause as nested_nlj_pooling_scan).
    checkConsistencyWithSql("agg_after_nlj_pooling_scan",
        """SELECT ${svSerialSource} a.v1, MAX(a.k1) AS mx
           FROM ls_serial a LEFT JOIN ls_serial b ON b.k2 < b.k2
           WHERE a.k1 IS NOT NULL
           GROUP BY a.v1
           ORDER BY a.v1, mx""", true)

    // 13-2: GROUPING SETS with pooling scan — generates REPEAT (union-like)
    //       operator internally.  Serial Exchange reduces num_tasks, causing
    //       "must set shared state, in UNION_OPERATOR / SORT_OPERATOR".
    //       Known issue: deadlocks on clusters where ls_serial (2 BUCKETS) tablets span
    //       multiple BEs — FE inserts LE (global instanceCount=2) but each BE has
    //       _num_instances=1 causing pipeline task mismatch. Pre-existing FE/BE discrepancy.
    checkConsistencyWithSql("grouping_sets_pooling_scan",
        """SELECT ${svSerialSource} k1, k2, SUM(v1) AS sv
           FROM ls_serial
           GROUP BY GROUPING SETS ((k1, k2), (k1), ())
           ORDER BY k1, k2, sv""")

    // 13-3: Window function + GROUPING SETS with pooling scan.
    //       Combines analytic (Sort shared state) and GROUPING SETS (Repeat/Union)
    //       — both need correct num_tasks for shared state injection.
    //       Same instanceCount discrepancy as grouping_sets_pooling_scan.
    checkConsistencyWithSql("window_grouping_sets_pooling_scan",
        """SELECT ${svSerialSource} k1, SUM(v1),
                  ROW_NUMBER() OVER (ORDER BY k1) AS rn
           FROM ls_serial
           GROUP BY GROUPING SETS ((k1), ())
           ORDER BY k1, rn""")

    // ============================================================
    //  14. RQG bug cases — serial NLJ + pooling scan (Bug 13 from rqg_bugs)
    //  Serial NLJ (RIGHT_OUTER) with pooling scan. Previously crashed because
    //  FE inserted BROADCAST on build side inflating num_tasks while probe stayed
    //  serial. Fixed: serial NLJ sets buildSideRequire=noRequire().
    // ============================================================
    checkConsistencyWithSql("rqg_serial_nlj_right_outer_pooling",
        """SELECT /*+SET_VAR(use_serial_exchange=true, parallel_pipeline_task_num=0,
                             ignore_storage_data_distribution=true,
                             enable_share_hash_table_for_broadcast_join=false,
                             disable_streaming_preaggregations=true,
                             disable_join_reorder=true)*/
              b.k1 AS field1
          FROM ls_serial a
          RIGHT OUTER JOIN ls_serial b ON a.v1 > b.v1
          GROUP BY field1
          ORDER BY field1 ASC""")

    // GLOBAL_HASH_SHUFFLE fix (Bug 10 from rqg_bugs) — self-join + NLJ with serial exchange
    checkConsistencyWithSql("rqg_global_hash_shuffle_self_join_nlj",
        """SELECT /*+SET_VAR(use_serial_exchange=true, parallel_pipeline_task_num=4,
                             ignore_storage_data_distribution=true,
                             disable_join_reorder=true, disable_colocate_plan=true)*/
              a.k1 AS field1, a.v1 AS field2
          FROM ls_t1 a
          LEFT JOIN ls_t1 b ON a.k1 = b.k2
          LEFT JOIN ls_t1 c ON a.k1 > b.k2
          WHERE a.v1 > 5
          GROUP BY field1, field2
          ORDER BY field1, field2""")

    // FULL OUTER JOIN + GROUP BY with serial exchange (Bug 11 from rqg_bugs)
    checkConsistencyWithSql("rqg_global_hash_full_outer_join",
        """SELECT /*+SET_VAR(use_serial_exchange=true, parallel_pipeline_task_num=4,
                             ignore_storage_data_distribution=true)*/
              a.k1, b.k1, count(1)
          FROM ls_t1 a
          FULL OUTER JOIN ls_t2 b ON a.k1 = b.k1
          WHERE b.k1 = 2
          GROUP BY a.k1, b.k1
          ORDER BY 1, 2, 3""")

    // Same pattern but simpler: NLJ with subquery + pooling, no GROUPING SETS
    checkConsistencyWithSql("nlj_subquery_pooling",
        """SELECT ${svSerialSource} k1, (SELECT COUNT(*) FROM ls_t2)
           FROM ls_t1
           ORDER BY k1""")

}
