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
 * Regression tests for bugs discovered by RQG testing on the local-exchange2 branch.
 *
 * These queries triggered "must set shared state" errors or incorrect results
 * in RQG build 183992.  Common conditions:
 *   - use_serial_exchange=true  (makes ALL Exchanges serial, not just UNPARTITIONED)
 *   - enable_local_shuffle_planner=true (FE-planned local exchange)
 *   - parallel_pipeline_task_num > 1
 *
 * Error types reproduced:
 *   1. must set shared state, in AGGREGATION_OPERATOR
 *   2. must set shared state, in SORT_OPERATOR
 *   3. incorrect results with GROUPING SETS + scalar subquery + window function
 */
suite("test_local_shuffle_rqg_bugs") {

    // ============================================================
    //  Table setup — mirrors RQG table structure
    //  10 buckets to match RQG (replication_num=1 for single-BE testing)
    // ============================================================
    sql "DROP TABLE IF EXISTS rqg_t1"
    sql "DROP TABLE IF EXISTS rqg_t2"
    sql "DROP TABLE IF EXISTS rqg_t3"
    sql "DROP TABLE IF EXISTS rqg_t4"

    sql """
        CREATE TABLE rqg_t1 (
            pk INT NOT NULL,
            col_int_undef_signed INT,
            col_int_undef_signed2 INT,
            col_int_undef_signed_not_null INT NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(pk)
        DISTRIBUTED BY HASH(pk) BUCKETS 10
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        CREATE TABLE rqg_t2 (
            pk INT NOT NULL,
            col_int_undef_signed INT,
            col_int_undef_signed2 INT,
            col_bigint_undef_signed_not_null BIGINT NOT NULL,
            col_decimal_38_10__undef_signed_not_null DECIMAL(38,10) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(pk)
        DISTRIBUTED BY HASH(pk) BUCKETS 10
        PROPERTIES ("replication_num" = "1")
    """

    // Table for build 184181 GLOBAL_HASH_SHUFFLE bugs — needs varchar + bigint columns
    sql """
        CREATE TABLE rqg_t3 (
            pk INT NOT NULL,
            col_bigint_undef_signed BIGINT,
            col_varchar_10__undef_signed VARCHAR(10),
            col_varchar_64__undef_signed VARCHAR(64)
        ) ENGINE=OLAP
        DUPLICATE KEY(pk)
        DISTRIBUTED BY HASH(pk) BUCKETS 10
        PROPERTIES ("replication_num" = "1")
    """

    // Second table for FULL OUTER JOIN case (col_bigint_undef_signed_not_null)
    sql """
        CREATE TABLE rqg_t4 (
            pk INT NOT NULL,
            col_bigint_undef_signed_not_null BIGINT NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(pk)
        DISTRIBUTED BY HASH(pk) BUCKETS 10
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        INSERT INTO rqg_t3 VALUES
            (0, -94, 'Abc', 'hello world'),
            (1, 672609, 'Xyz', null),
            (2, -3766684, 'Pqr', 'test string'),
            (3, 5070261, 'abc', 'another row'),
            (4, null, 'def', 'value four'),
            (5, -86, 'XgpxlHBLEM', null),
            (6, 21910, 'abc', 'they'),
            (7, -63, 'zzzz', 'some text'),
            (8, -8276281, 'AHlvNtoGLO', 'longer string here'),
            (9, -101, 'mid', 'final row')
    """

    sql """
        INSERT INTO rqg_t4 VALUES
            (0, 0), (1, 1), (2, 2), (3, 3), (4, 4),
            (5, 5), (6, 6), (7, 7), (8, 8), (9, 9),
            (10, 2), (11, 2), (12, 2), (13, 3), (14, 4),
            (15, 5), (16, 2), (17, 2), (18, 2), (19, 9)
    """

    // Insert enough rows to exercise multiple pipeline tasks
    sql """
        INSERT INTO rqg_t1 VALUES
            (0, 0, 10, 0), (1, 1, 11, 1), (2, 2, 12, 2), (3, 3, 13, 3),
            (4, 4, 14, 4), (5, 5, 15, 5), (6, 6, 16, 6), (7, 7, 17, 7),
            (8, 8, 18, 8), (9, 9, 19, 9), (10, 0, 20, 10), (11, 1, 21, 11),
            (12, 2, 22, 12), (13, 3, 23, 13), (14, 4, 24, 14), (15, 5, 25, 15),
            (16, 6, 26, 16), (17, 7, 27, 17), (18, 8, 28, 18), (19, 9, 29, 19)
    """

    sql """
        INSERT INTO rqg_t2 VALUES
            (0, 0, 10, 100, 1.5), (1, 1, 11, 101, 2.5), (2, 2, 12, 102, 3.5),
            (3, 3, 13, 103, 4.5), (4, 4, 14, 104, 5.5), (5, 5, 15, 105, 6.5),
            (6, 6, 16, 106, 7.5), (7, 7, 17, 107, 8.5), (8, 8, 18, 108, 9.5),
            (9, 9, 19, 109, 10.5), (10, 0, 20, 110, 11.5), (11, 1, 21, 111, 12.5),
            (12, 2, 22, 112, 13.5), (13, 3, 23, 113, 14.5), (14, 4, 24, 114, 15.5),
            (15, 5, 25, 115, 16.5), (16, 6, 26, 116, 17.5), (17, 7, 27, 117, 18.5),
            (18, 8, 28, 118, 19.5), (19, 9, 29, 119, 20.5)
    """

    // Wait for the inserted data to be visible — poll the actual row counts instead of a fixed sleep.
    for (int i = 0; i < 60; i++) {
        def c1 = sql "SELECT COUNT(*) FROM rqg_t1"
        def c2 = sql "SELECT COUNT(*) FROM rqg_t2"
        if (c1[0][0] == 20 && c2[0][0] == 20) {
            break
        }
        sleep(200)
    }

    // ============================================================
    //  Common settings
    // ============================================================
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET runtime_filter_mode=off"
    sql "SET enable_profile=true"
    sql "SET enable_sql_cache=false"
    sql "SET enable_local_shuffle=true"

    // ============================================================
    //  Bug 1: must set shared state, in AGGREGATION_OPERATOR
    //  RQG case: eliminate_group_by_uniform.case_id_11007680713
    //  Key conditions: use_serial_exchange=true, parallel_pipeline_task_num=3
    //  SQL: EXCEPT with count(*) GROUP BY on both sides
    // ============================================================

    // Test with FE planner (the buggy path)
    logger.info("=== Bug 1a: AGG shared state - EXCEPT with serial exchange (FE planner) ===")
    try {
        sql """
            SELECT /*+SET_VAR(use_serial_exchange=true,parallel_pipeline_task_num=3,
                              enable_local_shuffle_planner=true,
                              ignore_storage_data_distribution=true,
                              enable_common_expr_pushdown=false,
                              disable_streaming_preaggregations=true)*/
                col_int_undef_signed_not_null as col1,
                col_int_undef_signed_not_null as col2,
                0 as col3, count(1)
            FROM rqg_t1
            GROUP BY col1, col2, col3
            EXCEPT
            SELECT col_bigint_undef_signed_not_null as col1,
                   col_decimal_38_10__undef_signed_not_null as col2,
                   5 as col3, count(1)
            FROM rqg_t2
            GROUP BY col1, col2, col3
        """
        logger.info("Bug 1a: PASSED (no crash)")
    } catch (Throwable t) {
        logger.error("Bug 1a FAILED: ${t.message}")
        assertTrue(false, "Bug 1a: must set shared state in AGGREGATION_OPERATOR: ${t.message}")
    }

    // Compare with BE native planner
    logger.info("=== Bug 1b: AGG shared state - EXCEPT with serial exchange (BE native) ===")
    try {
        sql """
            SELECT /*+SET_VAR(use_serial_exchange=true,parallel_pipeline_task_num=3,
                              enable_local_shuffle_planner=false,
                              ignore_storage_data_distribution=true,
                              enable_common_expr_pushdown=false,
                              disable_streaming_preaggregations=true)*/
                col_int_undef_signed_not_null as col1,
                col_int_undef_signed_not_null as col2,
                0 as col3, count(1)
            FROM rqg_t1
            GROUP BY col1, col2, col3
            EXCEPT
            SELECT col_bigint_undef_signed_not_null as col1,
                   col_decimal_38_10__undef_signed_not_null as col2,
                   5 as col3, count(1)
            FROM rqg_t2
            GROUP BY col1, col2, col3
        """
        logger.info("Bug 1b: PASSED (no crash)")
    } catch (Throwable t) {
        logger.error("Bug 1b FAILED: ${t.message}")
        assertTrue(false, "Bug 1b: BE native also fails: ${t.message}")
    }

    // ============================================================
    //  Bug 2: must set shared state, in SORT_OPERATOR
    //  RQG case: grouping_set.case_id_5308471751
    //  Key conditions: use_serial_exchange=true, parallel_pipeline_task_num=5
    //  SQL: GROUPING SETS + window function (PERCENT_RANK)
    // ============================================================

    logger.info("=== Bug 2a: SORT shared state - GROUPING SETS + window (FE planner) ===")
    try {
        sql """
            SELECT /*+SET_VAR(use_serial_exchange=true,parallel_pipeline_task_num=5,
                              enable_local_shuffle_planner=true,
                              ignore_storage_data_distribution=true,
                              enable_share_hash_table_for_broadcast_join=false,
                              disable_streaming_preaggregations=true)*/
                SUM(PERCENT_RANK() OVER (PARTITION BY col_int_undef_signed2 ORDER BY col_int_undef_signed2))
            FROM rqg_t1
            GROUP BY GROUPING SETS ((col_int_undef_signed2),(pk, pk),(col_int_undef_signed))
        """
        logger.info("Bug 2a: PASSED (no crash)")
    } catch (Throwable t) {
        logger.error("Bug 2a FAILED: ${t.message}")
        assertTrue(false, "Bug 2a: must set shared state in SORT_OPERATOR: ${t.message}")
    }

    logger.info("=== Bug 2b: SORT shared state - GROUPING SETS + window (BE native) ===")
    try {
        sql """
            SELECT /*+SET_VAR(use_serial_exchange=true,parallel_pipeline_task_num=5,
                              enable_local_shuffle_planner=false,
                              ignore_storage_data_distribution=true,
                              enable_share_hash_table_for_broadcast_join=false,
                              disable_streaming_preaggregations=true)*/
                SUM(PERCENT_RANK() OVER (PARTITION BY col_int_undef_signed2 ORDER BY col_int_undef_signed2))
            FROM rqg_t1
            GROUP BY GROUPING SETS ((col_int_undef_signed2),(pk, pk),(col_int_undef_signed))
        """
        logger.info("Bug 2b: PASSED (no crash)")
    } catch (Throwable t) {
        logger.error("Bug 2b FAILED: ${t.message}")
        assertTrue(false, "Bug 2b: BE native also fails: ${t.message}")
    }

    // ============================================================
    //  Bug 3: incorrect results with GROUPING SETS + scalar subquery + window
    //  RQG case: grouping_set.case_id_5694495756
    //  Key conditions: parallel_pipeline_task_num=2, disable_streaming_preaggregations=true
    //  Expected: all rows same value; Actual: values split proportionally (1/3, 2/3)
    // ============================================================

    logger.info("=== Bug 3: incorrect results - GROUPING SETS + subquery + window ===")
    // FE planner
    def result_fe = sql """
        SELECT /*+SET_VAR(parallel_pipeline_task_num=2,
                          enable_local_shuffle_planner=true,
                          disable_streaming_preaggregations=true,
                          enable_share_hash_table_for_broadcast_join=true)*/
            SUM((SELECT MAX(col_int_undef_signed2) FROM rqg_t1))
                OVER (PARTITION BY pk ORDER BY pk)
        FROM rqg_t1
        GROUP BY GROUPING SETS ((col_int_undef_signed2, pk),(pk), (pk))
    """
    // BE native
    def result_be = sql """
        SELECT /*+SET_VAR(parallel_pipeline_task_num=2,
                          enable_local_shuffle_planner=false,
                          disable_streaming_preaggregations=true,
                          enable_share_hash_table_for_broadcast_join=true)*/
            SUM((SELECT MAX(col_int_undef_signed2) FROM rqg_t1))
                OVER (PARTITION BY pk ORDER BY pk)
        FROM rqg_t1
        GROUP BY GROUPING SETS ((col_int_undef_signed2, pk),(pk), (pk))
    """
    logger.info("Bug 3 FE result rows: ${result_fe.size()}, first few: ${result_fe.take(5)}")
    logger.info("Bug 3 BE result rows: ${result_be.size()}, first few: ${result_be.take(5)}")

    // FE planner and BE native must produce identical results (the bug was values split
    // proportionally instead of equal). Assert row count and order-insensitive content so a
    // recurrence fails the suite.
    assertEquals(result_be.size(), result_fe.size(), "Bug 3: FE/BE row count mismatch")
    assertEquals(result_be.collect { it.toString() }.sort(), result_fe.collect { it.toString() }.sort(),
            "Bug 3: FE/BE result mismatch")

    // ============================================================
    //  Bug 4: Simplified AGG shared state — single table GROUP BY with serial exchange
    //  Minimal reproduction attempt
    // ============================================================

    logger.info("=== Bug 4: Simplified AGG shared state ===")
    for (int ppt : [2, 3, 4, 5]) {
        try {
            sql """
                SELECT /*+SET_VAR(use_serial_exchange=true,parallel_pipeline_task_num=${ppt},
                                  enable_local_shuffle_planner=true,
                                  ignore_storage_data_distribution=true)*/
                    col_int_undef_signed, count(*)
                FROM rqg_t1
                GROUP BY col_int_undef_signed
                UNION ALL
                SELECT col_int_undef_signed, count(*)
                FROM rqg_t2
                GROUP BY col_int_undef_signed
            """
            logger.info("Bug 4 ppt=${ppt}: PASSED")
        } catch (Throwable t) {
            logger.error("Bug 4 ppt=${ppt} FAILED: ${t.message}")
            assertTrue(false, "Bug 4 ppt=${ppt}: AGG shared state crash with serial exchange: ${t.message}")
        }
    }

    // ============================================================
    //  Bug 5: GROUPING SETS + window variations with serial exchange
    //  More variations to find minimal repro
    // ============================================================

    logger.info("=== Bug 5: GROUPING SETS + window variations ===")
    for (int ppt : [2, 3, 4, 5]) {
        try {
            sql """
                SELECT /*+SET_VAR(use_serial_exchange=true,parallel_pipeline_task_num=${ppt},
                                  enable_local_shuffle_planner=true,
                                  ignore_storage_data_distribution=true)*/
                    pk, col_int_undef_signed,
                    ROW_NUMBER() OVER (ORDER BY pk)
                FROM rqg_t1
                GROUP BY GROUPING SETS ((pk, col_int_undef_signed), (pk), ())
                ORDER BY pk
            """
            logger.info("Bug 5 ppt=${ppt}: PASSED")
        } catch (Throwable t) {
            logger.error("Bug 5 ppt=${ppt} FAILED: ${t.message}")
            assertTrue(false, "Bug 5 ppt=${ppt}: GROUPING SETS + window crash with serial exchange: ${t.message}")
        }
    }

    // ============================================================
    //  Bug 6: must set shared state, in CROSS_JOIN_OPERATOR
    //  Root cause: nested NLJ + pooling scan — FE planner skipped BROADCAST
    //  local exchange on outer NLJ's build side because child was NLJ (not ScanNode).
    //  Fixed in NestedLoopJoinNode.enforceAndDeriveLocalExchange by using
    //  fragment.useSerialSource() instead of instanceof ScanNode check.
    //  This was the root cause of 989 RQG test failures (build 183677).
    // ============================================================

    logger.info("=== Bug 6: CROSS_JOIN shared state - nested NLJ + pooling scan (FE planner) ===")
    try {
        sql """
            SELECT /*+SET_VAR(ignore_storage_data_distribution=true,
                              parallel_pipeline_task_num=4,
                              enable_local_shuffle_planner=true,
                              disable_join_reorder=true,
                              disable_colocate_plan=true,
                              auto_broadcast_join_threshold=-1,
                              broadcast_row_count_limit=0,
                              query_timeout=60)*/
                count(a.pk) AS cnt, a.col_int_undef_signed
            FROM rqg_t1 a
            LEFT JOIN rqg_t1 b ON b.col_int_undef_signed >= b.col_int_undef_signed
            LEFT JOIN rqg_t1 c ON b.pk >= b.pk
            WHERE a.pk IS NOT NULL
            GROUP BY a.col_int_undef_signed
            ORDER BY cnt, a.col_int_undef_signed
        """
        logger.info("Bug 6: PASSED (no CROSS_JOIN_OPERATOR shared state error)")
    } catch (Throwable t) {
        logger.error("Bug 6 FAILED: ${t.message}")
        assertTrue(false, "Bug 6: must set shared state in CROSS_JOIN_OPERATOR: ${t.message}")
    }

    // ============================================================
    //  Bug 7: DataStreamSink hang — sender fragment with pooling scan
    //  Root cause: FE planner did not insert PASSTHROUGH at the root of pooling scan
    //  sender fragments. With pooling scan, only instance 0 creates pipeline tasks,
    //  so only 1 EOS is sent. The downstream ExchangeNode expects _num_instances EOSes
    //  and hangs indefinitely.
    //  Fixed in AddLocalExchange.addLocalExchangeForFragment: insert PASSTHROUGH
    //  when isLocalShuffle && newRoot.isSerialOperator().
    //  Any NLJ + pooling scan query triggers this via the UNPARTITIONED sender fragments.
    // ============================================================

    logger.info("=== Bug 7: DataStreamSink hang - NLJ + pooling scan sender (FE planner) ===")
    try {
        sql """
            SELECT /*+SET_VAR(ignore_storage_data_distribution=true,
                              parallel_pipeline_task_num=4,
                              enable_local_shuffle_planner=true,
                              disable_join_reorder=true,
                              disable_colocate_plan=true,
                              auto_broadcast_join_threshold=-1,
                              broadcast_row_count_limit=0,
                              query_timeout=60)*/
                a.col_int_undef_signed, MAX(a.pk) AS mx
            FROM rqg_t1 a
            LEFT JOIN rqg_t1 b ON b.col_int_undef_signed < b.col_int_undef_signed
            WHERE a.pk IS NOT NULL
            GROUP BY a.col_int_undef_signed
            ORDER BY a.col_int_undef_signed, mx
        """
        logger.info("Bug 7: PASSED (no hang)")
    } catch (Throwable t) {
        logger.error("Bug 7 FAILED: ${t.message}")
        assertTrue(false, "Bug 7: DataStreamSink hang (query timed out or crashed): ${t.message}")
    }

    // ============================================================
    //  Bug 8: must set shared state, in SORT_OPERATOR / UNION_OPERATOR
    //  Root cause: FE planner + pooling scan + GROUPING SETS. Serial UNPARTITIONED
    //  Exchange reduces downstream pipeline num_tasks to 1. SORT and UNION operators
    //  need _num_instances tasks to inject shared state for all instances.
    //  Fixed by: (1) restoring num_tasks raise for non-scan serial operators in BE
    //  deferred exchanger creation (commit 920d43d), and (2) FE inserting PASSTHROUGH
    //  after serial ExchangeNode in pooling scan fragments (commit d2e7fa2).
    // ============================================================

    logger.info("=== Bug 8a: SORT/UNION shared state - GROUPING SETS + pooling scan (FE planner) ===")
    try {
        sql """
            SELECT /*+SET_VAR(ignore_storage_data_distribution=true,
                              parallel_pipeline_task_num=4,
                              enable_local_shuffle_planner=true,
                              disable_streaming_preaggregations=true,
                              query_timeout=60)*/
                pk, col_int_undef_signed, SUM(col_int_undef_signed_not_null) AS sv
            FROM rqg_t1
            GROUP BY GROUPING SETS ((pk, col_int_undef_signed), (pk), ())
            ORDER BY pk, col_int_undef_signed, sv
        """
        logger.info("Bug 8a: PASSED (no SORT/UNION_OPERATOR shared state error)")
    } catch (Throwable t) {
        logger.error("Bug 8a FAILED: ${t.message}")
        assertTrue(false, "Bug 8a: must set shared state in SORT/UNION_OPERATOR: ${t.message}")
    }

    logger.info("=== Bug 8b: SORT shared state - window + GROUPING SETS + pooling scan (FE planner) ===")
    try {
        sql """
            SELECT /*+SET_VAR(ignore_storage_data_distribution=true,
                              parallel_pipeline_task_num=4,
                              enable_local_shuffle_planner=true,
                              disable_streaming_preaggregations=true,
                              query_timeout=60)*/
                pk, SUM(col_int_undef_signed_not_null) AS sv,
                ROW_NUMBER() OVER (ORDER BY pk) AS rn
            FROM rqg_t1
            GROUP BY GROUPING SETS ((pk), ())
            ORDER BY pk, sv, rn
        """
        logger.info("Bug 8b: PASSED (no SORT_OPERATOR shared state error)")
    } catch (Throwable t) {
        logger.error("Bug 8b FAILED: ${t.message}")
        assertTrue(false, "Bug 8b: must set shared state in SORT_OPERATOR (window+grouping_sets): ${t.message}")
    }

    // ============================================================
    //  Bug 9: FE/BE result inconsistency — agg after NLJ + pooling scan
    //  Root cause: StreamingAgg used fragment.useSerialSource()=true to require
    //  PASSTHROUGH from child, but when child is NLJ (not directly a serial scan),
    //  NLJ outputs ADAPTIVE_PASSTHROUGH. FE wrongly inserted an extra PASSTHROUGH
    //  exchange between StreamingAgg and NLJ (5 extra LOCAL_EXCHANGE_SINK_OPERATOR
    //  entries vs BE native).
    //  Fixed in AggregationNode: only requirePassthrough when
    //  children.get(0).isSerialOperator()=true, mirroring BE _child->is_serial_operator().
    // ============================================================

    logger.info("=== Bug 9: FE/BE result consistency - agg after NLJ + pooling scan ===")
    def bug9_fe = sql """
        SELECT /*+SET_VAR(ignore_storage_data_distribution=true,
                          parallel_pipeline_task_num=4,
                          enable_local_shuffle_planner=true,
                          disable_join_reorder=true,
                          disable_colocate_plan=true,
                          auto_broadcast_join_threshold=-1,
                          broadcast_row_count_limit=0)*/
            a.col_int_undef_signed, MAX(a.pk) AS mx
        FROM rqg_t1 a LEFT JOIN rqg_t1 b ON b.col_int_undef_signed < b.col_int_undef_signed
        WHERE a.pk IS NOT NULL
        GROUP BY a.col_int_undef_signed
        ORDER BY a.col_int_undef_signed, mx
    """
    def bug9_be = sql """
        SELECT /*+SET_VAR(ignore_storage_data_distribution=true,
                          parallel_pipeline_task_num=4,
                          enable_local_shuffle_planner=false,
                          disable_join_reorder=true,
                          disable_colocate_plan=true,
                          auto_broadcast_join_threshold=-1,
                          broadcast_row_count_limit=0)*/
            a.col_int_undef_signed, MAX(a.pk) AS mx
        FROM rqg_t1 a LEFT JOIN rqg_t1 b ON b.col_int_undef_signed < b.col_int_undef_signed
        WHERE a.pk IS NOT NULL
        GROUP BY a.col_int_undef_signed
        ORDER BY a.col_int_undef_signed, mx
    """
    logger.info("Bug 9 FE rows: ${bug9_fe.size()}, BE rows: ${bug9_be.size()}")
    assertEquals(bug9_be.size(), bug9_fe.size(), "Bug 9: FE/BE row count mismatch")
    assertEquals(bug9_be, bug9_fe, "Bug 9: FE/BE result mismatch for agg after NLJ + pooling scan")
    logger.info("Bug 9: PASSED (FE/BE results match)")

    // ============================================================
    //  Bug 10: GLOBAL_HASH_SHUFFLE Rows mismatched — self-join + NLJ
    //  RQG case: 906784672 (build 184181)
    //  Root cause: HashJoinNode used requireGlobalExecutionHash() → GLOBAL local exchange
    //  inserted when use_serial_exchange=true; shuffle_idx_to_instance_idx map has only
    //  4 entries (1/BE) but GLOBAL hash needs N*dop entries → most rows unrouted (0 actual rows).
    //  Fixed: changed to requireHash() so resolveExchangeType() downgrades to LOCAL hash.
    //  SQL: self-join (table1 LEFT JOIN table1 table2 ON pk=col_bigint_undef_signed)
    //       then NLJ (LEFT JOIN table1 table3 ON pk > col_bigint_undef_signed)
    // ============================================================

    logger.info("=== Bug 10: GLOBAL_HASH_SHUFFLE Rows mismatched - self-join + NLJ (build 184181 case 906784672) ===")
    def bug10_fe = sql """
        SELECT /*+SET_VAR(use_serial_exchange=true, parallel_pipeline_task_num=4,
                          enable_local_shuffle_planner=true,
                          ignore_storage_data_distribution=true,
                          enable_sql_cache=false,
                          disable_join_reorder=true, disable_colocate_plan=true)*/
            table1.pk AS field1, table1.col_bigint_undef_signed AS field2
        FROM rqg_t3 AS table1
        LEFT JOIN rqg_t3 AS table2 ON table1.pk = table2.col_bigint_undef_signed
        LEFT JOIN rqg_t3 AS table3 ON table1.pk > table2.col_bigint_undef_signed
        WHERE (table1.col_varchar_10__undef_signed > 'AHlvNtoGLO'
               AND table1.col_varchar_10__undef_signed < 'zzzz')
           OR (table1.col_bigint_undef_signed = table1.pk AND table1.col_varchar_64__undef_signed IS NULL)
           OR (table1.pk != table1.pk AND table1.pk <> 2)
        GROUP BY field1, field2
        ORDER BY field1, field2
    """
    def bug10_be = sql """
        SELECT /*+SET_VAR(use_serial_exchange=true, parallel_pipeline_task_num=4,
                          enable_local_shuffle_planner=false,
                          ignore_storage_data_distribution=true,
                          enable_sql_cache=false,
                          disable_join_reorder=true, disable_colocate_plan=true)*/
            table1.pk AS field1, table1.col_bigint_undef_signed AS field2
        FROM rqg_t3 AS table1
        LEFT JOIN rqg_t3 AS table2 ON table1.pk = table2.col_bigint_undef_signed
        LEFT JOIN rqg_t3 AS table3 ON table1.pk > table2.col_bigint_undef_signed
        WHERE (table1.col_varchar_10__undef_signed > 'AHlvNtoGLO'
               AND table1.col_varchar_10__undef_signed < 'zzzz')
           OR (table1.col_bigint_undef_signed = table1.pk AND table1.col_varchar_64__undef_signed IS NULL)
           OR (table1.pk != table1.pk AND table1.pk <> 2)
        GROUP BY field1, field2
        ORDER BY field1, field2
    """
    logger.info("Bug 10 FE rows: ${bug10_fe.size()}, BE rows: ${bug10_be.size()}")
    assertEquals(bug10_be.size(), bug10_fe.size(), "Bug 10: FE/BE row count mismatch (GLOBAL_HASH_SHUFFLE Rows mismatched)")
    assertEquals(bug10_be, bug10_fe, "Bug 10: FE/BE result mismatch for self-join + NLJ")
    logger.info("Bug 10: PASSED")

    // ============================================================
    //  Bug 11: GLOBAL_HASH_SHUFFLE Rows mismatched — FULL OUTER JOIN + GROUP BY
    //  RQG case: 11007681241 (build 184181)
    //  Same root cause as Bug 10.
    //  SQL: FULL OUTER JOIN on col_bigint_undef_signed_not_null with WHERE + GROUP BY
    // ============================================================

    logger.info("=== Bug 11: GLOBAL_HASH_SHUFFLE Rows mismatched - FULL OUTER JOIN + GROUP BY (build 184181 case 11007681241) ===")
    def bug11_fe = sql """
        SELECT /*+SET_VAR(use_serial_exchange=true, parallel_pipeline_task_num=4,
                          enable_local_shuffle_planner=true,
                          ignore_storage_data_distribution=true,
                          enable_sql_cache=false)*/
            t1.col_bigint_undef_signed_not_null, t2.col_bigint_undef_signed_not_null, count(1)
        FROM rqg_t4 t1
        FULL OUTER JOIN rqg_t2 t2
          ON t1.col_bigint_undef_signed_not_null = t2.col_bigint_undef_signed_not_null
        WHERE t2.col_bigint_undef_signed_not_null = 2
        GROUP BY t1.col_bigint_undef_signed_not_null, t2.col_bigint_undef_signed_not_null
        ORDER BY 1, 2, 3
    """
    def bug11_be = sql """
        SELECT /*+SET_VAR(use_serial_exchange=true, parallel_pipeline_task_num=4,
                          enable_local_shuffle_planner=false,
                          ignore_storage_data_distribution=true,
                          enable_sql_cache=false)*/
            t1.col_bigint_undef_signed_not_null, t2.col_bigint_undef_signed_not_null, count(1)
        FROM rqg_t4 t1
        FULL OUTER JOIN rqg_t2 t2
          ON t1.col_bigint_undef_signed_not_null = t2.col_bigint_undef_signed_not_null
        WHERE t2.col_bigint_undef_signed_not_null = 2
        GROUP BY t1.col_bigint_undef_signed_not_null, t2.col_bigint_undef_signed_not_null
        ORDER BY 1, 2, 3
    """
    logger.info("Bug 11 FE rows: ${bug11_fe.size()}, BE rows: ${bug11_be.size()}")
    assertEquals(bug11_be.size(), bug11_fe.size(), "Bug 11: FE/BE row count mismatch (GLOBAL_HASH_SHUFFLE Rows mismatched)")
    assertEquals(bug11_be, bug11_fe, "Bug 11: FE/BE result mismatch for FULL OUTER JOIN + GROUP BY")
    logger.info("Bug 11: PASSED")

    // ============================================================
    //  Bug 12: GLOBAL_HASH_SHUFFLE Rows mismatched — LEFT JOIN + VARCHAR predicates + MIN()
    //  RQG case: 906784662 (build 184181)
    //  Same root cause as Bug 10/11.
    //  SQL: LEFT JOIN on pk with VARCHAR NOT IN / BETWEEN / IN predicates, MIN() aggregate
    // ============================================================

    logger.info("=== Bug 12: GLOBAL_HASH_SHUFFLE Rows mismatched - LEFT JOIN + VARCHAR predicates (build 184181 case 906784662) ===")
    def bug12_fe = sql """
        SELECT /*+SET_VAR(use_serial_exchange=true, parallel_pipeline_task_num=4,
                          enable_local_shuffle_planner=true,
                          ignore_storage_data_distribution=true,
                          enable_sql_cache=false,
                          disable_join_reorder=true, disable_colocate_plan=true)*/
            table1.pk AS field1, MIN(table1.pk) AS field2
        FROM rqg_t3 AS table1
        LEFT JOIN rqg_t1 AS table2 ON table2.pk = table1.pk
        WHERE table1.col_varchar_64__undef_signed NOT IN ('they')
          AND table1.col_varchar_10__undef_signed BETWEEN 'AHlvNtoGLO' AND 'z'
          AND table1.pk IN (3, 6, 8, 9, 2)
        GROUP BY field1
        ORDER BY field1, field2 ASC
    """
    def bug12_be = sql """
        SELECT /*+SET_VAR(use_serial_exchange=true, parallel_pipeline_task_num=4,
                          enable_local_shuffle_planner=false,
                          ignore_storage_data_distribution=true,
                          enable_sql_cache=false,
                          disable_join_reorder=true, disable_colocate_plan=true)*/
            table1.pk AS field1, MIN(table1.pk) AS field2
        FROM rqg_t3 AS table1
        LEFT JOIN rqg_t1 AS table2 ON table2.pk = table1.pk
        WHERE table1.col_varchar_64__undef_signed NOT IN ('they')
          AND table1.col_varchar_10__undef_signed BETWEEN 'AHlvNtoGLO' AND 'z'
          AND table1.pk IN (3, 6, 8, 9, 2)
        GROUP BY field1
        ORDER BY field1, field2 ASC
    """
    logger.info("Bug 12 FE rows: ${bug12_fe.size()}, BE rows: ${bug12_be.size()}")
    assertEquals(bug12_be.size(), bug12_fe.size(), "Bug 12: FE/BE row count mismatch (GLOBAL_HASH_SHUFFLE Rows mismatched)")
    assertEquals(bug12_be, bug12_fe, "Bug 12: FE/BE result mismatch for LEFT JOIN + VARCHAR predicates")
    logger.info("Bug 12: PASSED")

    // ============================================================
    //  Bug 13: NLJ COREDUMP — serial NLJ + pooling scan + BROADCAST build side
    //  RQG build 184430, query c0dafc1bed0f4910
    //  Root cause: serial NLJ (RIGHT_OUTER) with pooling scan inserted BROADCAST
    //  local exchange on build side, inflating build pipeline num_tasks to _num_instances
    //  while probe pipeline stayed at 1 task. Instance 1+ created build tasks without
    //  corresponding probe tasks → source_deps empty → set_ready_to_read() crash.
    //  Fixed: serial NLJ sets buildSideRequire=noRequire() to match BE-native
    //  num_tasks_of_parent()<=1 skip logic.
    // ============================================================

    logger.info("=== Bug 13: NLJ COREDUMP - serial NLJ + pooling scan (FE planner) ===")
    try {
        def bug13_fe = sql """
            SELECT /*+SET_VAR(use_serial_exchange=true, parallel_pipeline_task_num=0,
                              enable_local_shuffle_planner=true,
                              ignore_storage_data_distribution=true,
                              enable_sql_cache=false,
                              enable_share_hash_table_for_broadcast_join=false,
                              disable_streaming_preaggregations=true,
                              disable_join_reorder=true)*/
                t2.col_bigint_undef_signed_not_null AS field1
            FROM rqg_t4 AS t1
            RIGHT OUTER JOIN rqg_t2 AS t2 ON t1.col_bigint_undef_signed_not_null > t2.col_bigint_undef_signed_not_null
            GROUP BY field1
            ORDER BY field1 ASC
        """
        def bug13_be = sql """
            SELECT /*+SET_VAR(use_serial_exchange=true, parallel_pipeline_task_num=0,
                              enable_local_shuffle_planner=false,
                              ignore_storage_data_distribution=true,
                              enable_sql_cache=false,
                              enable_share_hash_table_for_broadcast_join=false,
                              disable_streaming_preaggregations=true,
                              disable_join_reorder=true)*/
                t2.col_bigint_undef_signed_not_null AS field1
            FROM rqg_t4 AS t1
            RIGHT OUTER JOIN rqg_t2 AS t2 ON t1.col_bigint_undef_signed_not_null > t2.col_bigint_undef_signed_not_null
            GROUP BY field1
            ORDER BY field1 ASC
        """
        logger.info("Bug 13 FE rows: ${bug13_fe.size()}, BE rows: ${bug13_be.size()}")
        assertEquals(bug13_be.size(), bug13_fe.size(), "Bug 13: FE/BE row count mismatch (NLJ COREDUMP)")
        assertEquals(bug13_be, bug13_fe, "Bug 13: FE/BE result mismatch for serial NLJ + pooling scan")
        logger.info("Bug 13: PASSED (no crash, results match)")
    } catch (Throwable t) {
        logger.error("Bug 13 FAILED: ${t.message}")
        assertTrue(false, "Bug 13: NLJ COREDUMP (serial NLJ + pooling scan): ${t.message}")
    }

    // ============================================================
    //  Bug 14: BUCKET_SHUFFLE join + serial build Exchange — must set shared state
    //  RQG build 184563, cases 906784706/906784783/906784987/906785006
    //  Root cause: BUCKET_SHUFFLE join build side ExchangeNode marked serial in
    //  pooling scan fragment → build pipeline num_tasks reduced to 1 →
    //  instance 1+ have probe tasks without build tasks → shared state injection
    //  fails. Fixed: isBucketShuffle() branch checks buildChildSerial and uses
    //  requirePassToOne() to restore num_tasks, matching BE-native behavior.
    //  Requires replication_num=3 + [shuffle] hint to force BUCKET_SHUFFLE plan.
    // ============================================================

    logger.info("=== Bug 14: BUCKET_SHUFFLE join + serial build Exchange (FE planner) ===")
    // Need replication_num=3 for BUCKET_SHUFFLE. Check if allow_replica_on_same_host is enabled.
    def allowSameHost = sql "ADMIN SHOW FRONTEND CONFIG LIKE 'allow_replica_on_same_host'"
    if (allowSameHost[0][1].toString() == "true") {
        sql "DROP TABLE IF EXISTS rqg_t5_rep3"
        sql "DROP TABLE IF EXISTS rqg_t6_rep3"
        try {
            sql """
                CREATE TABLE rqg_t5_rep3 (
                    pk INT NULL,
                    col_varchar_10__undef_signed VARCHAR(10) NULL,
                    col_bigint_undef_signed BIGINT NULL,
                    col_varchar_64__undef_signed VARCHAR(64) NULL
                ) DUPLICATE KEY(pk, col_varchar_10__undef_signed)
                DISTRIBUTED BY HASH(pk) BUCKETS 10
                PROPERTIES ("replication_num" = "3")
            """
            sql """
                CREATE TABLE rqg_t6_rep3 (
                    pk INT NULL,
                    col_varchar_10__undef_signed VARCHAR(10) NULL,
                    col_bigint_undef_signed BIGINT NULL,
                    col_varchar_64__undef_signed VARCHAR(64) NULL
                ) DUPLICATE KEY(pk, col_varchar_10__undef_signed)
                DISTRIBUTED BY HASH(pk) BUCKETS 10
                PROPERTIES ("replication_num" = "3")
            """
            sql """
                INSERT INTO rqg_t5_rep3 VALUES
                    (0,'abc',-94,'hello'),(1,'xyz',672609,null),(2,'pqr',-3766684,'test'),
                    (3,'abc',5070261,'another'),(4,'def',null,'value'),(5,'so',-86,null),
                    (6,'abc',21910,'they'),(7,'zzzz',-63,'some'),(8,'xPLflvBEcW',-8276281,'longer'),
                    (9,'mid',-101,'final')
            """
            sql """
                INSERT INTO rqg_t6_rep3 VALUES
                    (0,'aaa',100,'world'),(1,'bbb',200,null),(2,'ccc',300,'foo'),
                    (3,'ddd',400,'bar'),(4,'eee',500,'baz'),(5,'fff',600,null),
                    (6,'ggg',700,'qux'),(7,'hhh',800,'quux'),(8,'iii',900,'corge'),
                    (9,'jjj',1000,'grault')
            """
            Thread.sleep(3000)

            def bug14_fe = sql """
                SELECT /*+SET_VAR(enable_local_shuffle_planner=true,
                                  parallel_pipeline_task_num=3,
                                  disable_streaming_preaggregations=true,
                                  enable_sql_cache=false,
                                  disable_join_reorder=true)*/
                    table1.pk AS field1
                FROM rqg_t5_rep3 AS table1
                LEFT OUTER JOIN [shuffle] rqg_t6_rep3 AS table2 ON table1.pk = table2.pk
                WHERE table1.col_varchar_10__undef_signed >= 'so'
                GROUP BY field1
                ORDER BY field1
            """
            def bug14_be = sql """
                SELECT /*+SET_VAR(enable_local_shuffle_planner=false,
                                  parallel_pipeline_task_num=3,
                                  disable_streaming_preaggregations=true,
                                  enable_sql_cache=false,
                                  disable_join_reorder=true)*/
                    table1.pk AS field1
                FROM rqg_t5_rep3 AS table1
                LEFT OUTER JOIN [shuffle] rqg_t6_rep3 AS table2 ON table1.pk = table2.pk
                WHERE table1.col_varchar_10__undef_signed >= 'so'
                GROUP BY field1
                ORDER BY field1
            """
            logger.info("Bug 14 FE rows: ${bug14_fe.size()}, BE rows: ${bug14_be.size()}")
            assertEquals(bug14_be.size(), bug14_fe.size(), "Bug 14: FE/BE row count mismatch (BUCKET_SHUFFLE must set shared state)")
            assertEquals(bug14_be, bug14_fe, "Bug 14: FE/BE result mismatch for BUCKET_SHUFFLE + serial build Exchange")
            logger.info("Bug 14: PASSED (no crash, results match)")
        } catch (Throwable t) {
            logger.error("Bug 14 FAILED: ${t.message}")
            assertTrue(false, "Bug 14: BUCKET_SHUFFLE must set shared state: ${t.message}")
        }
    } else {
        logger.info("Bug 14: SKIPPED (allow_replica_on_same_host not enabled, cannot create replication_num=3 tables)")
    }

    // ============================================================
    //  Bug 15: BUCKET_SHUFFLE join wrong results with serial exchange — PASS_TO_ONE data loss
    //  Root cause: When serial exchange feeds BUCKET_SHUFFLE join build side,
    //  PASS_TO_ONE routes ALL build data to task 0. Unlike BROADCAST joins,
    //  BUCKET_SHUFFLE has no shared hash table mechanism — tasks 1..N-1 build
    //  empty hash tables and lose rows during probe. Fixed by using
    //  BUCKET_HASH_SHUFFLE instead of PASS_TO_ONE for BUCKET_SHUFFLE build side.
    //  Tables use 3 buckets so pptn=4 triggers serial scan on single BE (3 < 4*1).
    // ============================================================

    logger.info("=== Bug 15: BUCKET_SHUFFLE join wrong results with serial PASS_TO_ONE ===")

    sql "DROP TABLE IF EXISTS rqg_t7_3bucket"
    sql "DROP TABLE IF EXISTS rqg_t8_3bucket"

    sql """
        CREATE TABLE rqg_t7_3bucket (
            pk INT NOT NULL,
            col_int INT NULL,
            col_varchar VARCHAR(64) NULL
        ) DUPLICATE KEY(pk)
        DISTRIBUTED BY HASH(pk) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        CREATE TABLE rqg_t8_3bucket (
            pk INT NOT NULL,
            col_int INT NULL,
            col_varchar VARCHAR(64) NULL
        ) DUPLICATE KEY(pk)
        DISTRIBUTED BY HASH(pk) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        INSERT INTO rqg_t7_3bucket VALUES
            (0, 10, 'aaa'), (1, 20, 'bbb'), (2, 30, 'ccc'),
            (3, 40, 'ddd'), (4, 50, 'eee'), (5, 60, 'fff'),
            (6, 70, 'ggg'), (7, 80, 'hhh'), (8, 90, 'iii'), (9, 100, 'jjj')
    """

    sql """
        INSERT INTO rqg_t8_3bucket VALUES
            (0, 10, 'aaa'), (1, 20, 'bbb'), (2, 30, 'ccc'),
            (3, 40, 'ddd'), (4, 50, 'eee'), (5, 60, 'fff'),
            (6, 70, 'ggg'), (7, 80, 'hhh'), (8, 90, 'iii'), (9, 100, 'jjj')
    """

    Thread.sleep(3000)

    try {
        // pptn=4 with 3 buckets on 1 BE: 3 < 4*1 → serial scan → serial exchange
        // This triggers the PASS_TO_ONE bug for BUCKET_SHUFFLE build side.
        // Also test with higher pptn values to cover more cases.
        for (int ppt : [4, 6, 8]) {
            def bug15_fe = sql """
                SELECT /*+SET_VAR(use_serial_exchange=true,
                                  parallel_pipeline_task_num=${ppt},
                                  enable_local_shuffle_planner=true,
                                  ignore_storage_data_distribution=true,
                                  enable_share_hash_table_for_broadcast_join=false,
                                  enable_sql_cache=false,
                                  disable_join_reorder=true)*/
                    t1.pk, t1.col_int, t2.col_varchar
                FROM rqg_t7_3bucket t1
                INNER JOIN [shuffle] rqg_t8_3bucket t2 ON t1.pk = t2.pk
                ORDER BY t1.pk
            """
            def bug15_be = sql """
                SELECT /*+SET_VAR(use_serial_exchange=true,
                                  parallel_pipeline_task_num=${ppt},
                                  enable_local_shuffle_planner=false,
                                  ignore_storage_data_distribution=true,
                                  enable_share_hash_table_for_broadcast_join=false,
                                  enable_sql_cache=false,
                                  disable_join_reorder=true)*/
                    t1.pk, t1.col_int, t2.col_varchar
                FROM rqg_t7_3bucket t1
                INNER JOIN [shuffle] rqg_t8_3bucket t2 ON t1.pk = t2.pk
                ORDER BY t1.pk
            """
            logger.info("Bug 15 ppt=${ppt}: FE rows=${bug15_fe.size()}, BE rows=${bug15_be.size()}")
            assertEquals(10, bug15_fe.size(), "Bug 15 ppt=${ppt}: expected 10 rows from FE planner, got ${bug15_fe.size()}")
            assertEquals(bug15_be, bug15_fe, "Bug 15 ppt=${ppt}: FE/BE result mismatch for BUCKET_SHUFFLE + serial exchange")
        }

        // Also test LEFT OUTER JOIN to verify no rows lost on probe side
        def bug15_left_fe = sql """
            SELECT /*+SET_VAR(use_serial_exchange=true,
                              parallel_pipeline_task_num=6,
                              enable_local_shuffle_planner=true,
                              ignore_storage_data_distribution=true,
                              enable_share_hash_table_for_broadcast_join=false,
                              enable_sql_cache=false,
                              disable_join_reorder=true)*/
                t1.pk, t2.col_int
            FROM rqg_t7_3bucket t1
            LEFT OUTER JOIN [shuffle] rqg_t8_3bucket t2 ON t1.pk = t2.pk
            ORDER BY t1.pk
        """
        def bug15_left_be = sql """
            SELECT /*+SET_VAR(use_serial_exchange=true,
                              parallel_pipeline_task_num=6,
                              enable_local_shuffle_planner=false,
                              ignore_storage_data_distribution=true,
                              enable_share_hash_table_for_broadcast_join=false,
                              enable_sql_cache=false,
                              disable_join_reorder=true)*/
                t1.pk, t2.col_int
            FROM rqg_t7_3bucket t1
            LEFT OUTER JOIN [shuffle] rqg_t8_3bucket t2 ON t1.pk = t2.pk
            ORDER BY t1.pk
        """
        assertEquals(10, bug15_left_fe.size(), "Bug 15 LEFT JOIN: expected 10 rows from FE planner")
        assertEquals(bug15_left_be, bug15_left_fe, "Bug 15 LEFT JOIN: FE/BE result mismatch")

        logger.info("Bug 15: PASSED (no wrong results, all pptn values correct)")
    } catch (Throwable t) {
        logger.error("Bug 15 FAILED: ${t.message}")
        assertTrue(false, "Bug 15: BUCKET_SHUFFLE wrong results with serial PASS_TO_ONE: ${t.message}")
    }

    // ============================================================
    //  Bug 16 & 17: Serial AnalyticEval crash and DataStreamSink hang
    //  with LocalShuffleAssignedJob (multiple instances on one BE)
    //
    //  Bug 16 (crash): Exchange wraps itself with PASSTHROUGH LocalExchange.
    //  This restores AnalyticSink pipeline to _num_instances tasks while
    //  serial AnalyticSource stays at 1 task. For instance_idx > 0,
    //  source_deps is empty → DCHECK crash.
    //
    //  Bug 17 (hang): After fixing the crash, serial AnalyticSource reduces
    //  all downstream pipeline tasks to 1 via add_pipeline() inheritance.
    //  Only instance 0 runs DataStreamSink → receiver expects _num_instances
    //  EOSes → hang.
    //
    //  Both triggered by: OVER() with no PARTITION BY + GROUPING SETS +
    //  pptn=0 (auto-parallel) + disable_streaming_preaggregations=true
    //  RQG build 186195, query IDs: 7f3178a77c2c4b6b, 71887f7bf804c0c, 5dd9fcad234c4484
    // ============================================================
    sql "DROP TABLE IF EXISTS rqg_analytic_t1"
    sql """
        CREATE TABLE rqg_analytic_t1 (
            pk INT NOT NULL,
            col_int_undef_signed INT
        ) ENGINE=OLAP
        DUPLICATE KEY(pk)
        DISTRIBUTED BY HASH(pk) BUCKETS 10
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO rqg_analytic_t1 VALUES
        (1, 10), (2, 20), (3, 30), (4, 40), (5, 50),
        (6, 60), (7, 70), (8, 80), (9, 90), (10, 100),
        (11, 10), (12, 20), (13, 30), (14, 40), (15, 50),
        (16, 60), (17, 70), (18, 80), (19, 90), (20, 100)
    """

    try {
        logger.info("Bug 16+17: Testing serial AnalyticEval with GROUPING SETS")

        // Baseline: pptn=1 (no multi-instance, no local shuffle)
        def bug16_baseline = sql """
            SELECT /*+SET_VAR(parallel_pipeline_task_num=1,
                              enable_sql_cache=false,
                              disable_streaming_preaggregations=true)*/
                COUNT(MIN(col_int_undef_signed) OVER())
            FROM rqg_analytic_t1
            GROUP BY GROUPING SETS ((col_int_undef_signed, pk), (), (pk))
            ORDER BY 1
        """
        assertEquals(41, bug16_baseline.size(), "Bug 16 baseline: expected 41 rows")

        // Test with pptn=0 (auto-parallel, triggers LocalShuffleAssignedJob)
        for (int ppt : [0, 2, 4, 8]) {
            def bug16_result = sql """
                SELECT /*+SET_VAR(parallel_pipeline_task_num=${ppt},
                                  enable_local_shuffle_planner=true,
                                  ignore_storage_data_distribution=true,
                                  enable_sql_cache=false,
                                  disable_streaming_preaggregations=true)*/
                    COUNT(MIN(col_int_undef_signed) OVER())
                FROM rqg_analytic_t1
                GROUP BY GROUPING SETS ((col_int_undef_signed, pk), (), (pk))
                ORDER BY 1
            """
            assertEquals(bug16_baseline, bug16_result,
                "Bug 16+17 pptn=${ppt}: result mismatch with serial AnalyticEval")
        }

        // Also test with use_serial_exchange=true (makes ALL exchanges serial)
        def bug16_serial = sql """
            SELECT /*+SET_VAR(use_serial_exchange=true,
                              parallel_pipeline_task_num=0,
                              enable_local_shuffle_planner=true,
                              ignore_storage_data_distribution=true,
                              enable_sql_cache=false,
                              disable_streaming_preaggregations=true)*/
                COUNT(MIN(col_int_undef_signed) OVER())
            FROM rqg_analytic_t1
            GROUP BY GROUPING SETS ((col_int_undef_signed, pk), (), (pk))
            ORDER BY 1
        """
        assertEquals(bug16_baseline, bug16_serial,
            "Bug 16+17 serial_exchange: result mismatch")

        logger.info("Bug 16+17: PASSED (no crash, no hang, correct results)")
    } catch (Throwable t) {
        logger.error("Bug 16+17 FAILED: ${t.message}")
        assertTrue(false, "Bug 16+17: Serial AnalyticEval crash/hang: ${t.message}")
    }

    //  Bug 18: DCHECK crash in Pipeline::set_num_tasks when PASSTHROUGH LE is inserted
    //  between serial NLJ and its child Exchange.
    //  Root cause: ExchangeNode.enforceAndDeriveLocalExchange wraps UNPARTITIONED serial
    //  Exchange with PASSTHROUGH LE. On BE, NLJ_PROBE (serial) sets pipeline num_tasks=1,
    //  then the LE handler's set_num_tasks(_num_instances) overrides it to N, triggering
    //  DCHECK (serial operator in pipeline with num_tasks > 1).
    //  Fix: skip PASSTHROUGH wrapping when hasSerialAncestorInPipeline is true.
    //  Query: LEFT JOIN with always-true self-ref condition (table.pk = table.pk) creates
    //  RIGHT_OUTER NLJ (serial). With pptn>1 and ignore_data_distribution, the fragment
    //  gets N instances but NLJ forces 1 task.
    try {
        logger.info("Bug 18: Testing serial NLJ with PASSTHROUGH LE crash")
        // Use existing rqg_t1 table (10 rows, 10 buckets)
        def bug18_baseline = sql """
            SELECT /*+SET_VAR(enable_local_shuffle_planner=false,
                              enable_sql_cache=false)*/
                table1.col_int_undef_signed AS field1
            FROM rqg_t1 AS table1
            LEFT JOIN rqg_t1 AS table2
                ON table2.pk = table2.pk
            WHERE table1.pk BETWEEN 2 AND 11
            GROUP BY field1
            ORDER BY 1
        """

        // Test with various pptn values — crash requires pptn > 1
        for (int ppt : [4, 7]) {
            def bug18_result = sql """
                SELECT /*+SET_VAR(use_serial_exchange=false,
                                  parallel_pipeline_task_num=${ppt},
                                  enable_local_shuffle_planner=true,
                                  ignore_storage_data_distribution=true,
                                  enable_sql_cache=false,
                                  enable_share_hash_table_for_broadcast_join=false,
                                  enable_broadcast_join_force_passthrough=true,
                                  enable_parallel_scan=false)*/
                    table1.col_int_undef_signed AS field1
                FROM rqg_t1 AS table1
                LEFT JOIN rqg_t1 AS table2
                    ON table2.pk = table2.pk
                WHERE table1.pk BETWEEN 2 AND 11
                GROUP BY field1
                ORDER BY 1
            """
            assertEquals(bug18_baseline, bug18_result,
                "Bug 18 pptn=${ppt}: result mismatch with serial NLJ + local exchange")
        }
        logger.info("Bug 18: PASSED (no crash, correct results)")
    } catch (Throwable t) {
        logger.error("Bug 18 FAILED: ${t.message}")
        assertTrue(false, "Bug 18: Serial NLJ PASSTHROUGH LE crash: ${t.message}")
    }

    //  Bug 19: source_deps.size()=0 crash in NLJ build sink.
    //  Root cause: serial NLJ (RIGHT_OUTER) resets serial ancestor flag for build side.
    //  Exchange(UNPARTITIONED) on build side sees hasSerialAncestorInPipeline=false and
    //  inserts PASSTHROUGH LE. This restores build pipeline num_tasks to _num_instances
    //  while probe pipeline stays at 1. The extra build tasks have NLJ shared state with
    //  empty source_deps → crash in set_ready_to_read().
    //  Fix: shouldResetSerialFlagForChild(1) returns false when NLJ is serial.
    //  Differs from Bug 18 in fuzzy vars: enable_share_hash_table=true, broadcast_passthrough=false.
    try {
        logger.info("Bug 19: Testing serial NLJ build-side source_deps crash")
        def bug19_baseline = sql """
            SELECT /*+SET_VAR(enable_local_shuffle_planner=false,
                              enable_sql_cache=false)*/
                table1.col_int_undef_signed AS field1
            FROM rqg_t1 AS table1
            LEFT JOIN rqg_t1 AS table2
                ON table2.pk = table2.pk
            WHERE table1.pk BETWEEN 2 AND 11
            GROUP BY field1
            ORDER BY 1
        """

        for (int ppt : [2, 4]) {
            def bug19_result = sql """
                SELECT /*+SET_VAR(use_serial_exchange=false,
                                  parallel_pipeline_task_num=${ppt},
                                  enable_local_shuffle_planner=true,
                                  ignore_storage_data_distribution=true,
                                  enable_sql_cache=false,
                                  enable_share_hash_table_for_broadcast_join=true,
                                  enable_broadcast_join_force_passthrough=false,
                                  enable_parallel_scan=true,
                                  disable_streaming_preaggregations=true)*/
                    table1.col_int_undef_signed AS field1
                FROM rqg_t1 AS table1
                LEFT JOIN rqg_t1 AS table2
                    ON table2.pk = table2.pk
                WHERE table1.pk BETWEEN 2 AND 11
                GROUP BY field1
                ORDER BY 1
            """
            assertEquals(bug19_baseline, bug19_result,
                "Bug 19 pptn=${ppt}: result mismatch with serial NLJ build side crash")
        }
        logger.info("Bug 19: PASSED (no crash, correct results)")
    } catch (Throwable t) {
        logger.error("Bug 19 FAILED: ${t.message}")
        assertTrue(false, "Bug 19: Serial NLJ build-side source_deps crash: ${t.message}")
    }

    //  Bug 20: Hang (ASAN: COREDUMP source_deps.size()=0 in AggSinkOperatorX) when
    //  use_serial_exchange=true + RIGHT JOIN + GROUP BY in non-pooling fragment.
    //  Root cause: serial HASH Exchange in non-pooling fragment returned NOOP, causing FE
    //  to insert LOCAL_EXECUTION_HASH_SHUFFLE LE. On BE, serial Exchange pipeline has 1 task
    //  but LE downstream has _num_instances tasks. AggSink on instances 1+ has empty source_deps.
    //  Fix: ExchangeNode.enforceAndDeriveLocalExchange() returns actual distribution type
    //  (GLOBAL_EXECUTION_HASH_SHUFFLE/BUCKET_HASH_SHUFFLE) for serial Exchange in non-pooling
    //  fragments, preventing LE insertion.
    //  Requires 3+ BEs to reproduce (single BE has _num_instances=1, no hang).
    try {
        logger.info("Bug 20: Testing serial exchange + agg hang in non-pooling fragment")
        // Baseline uses same fuzzy vars but with planner=false (BE-native).
        // This way we compare FE-planned vs BE-native under identical conditions,
        // not against "correct" results — use_serial_exchange=true itself may have
        // pre-existing BE bugs with certain pptn values.
        def bug20_baseline_sql = { int ppt -> """
            SELECT /*+SET_VAR(use_serial_exchange=true,
                              parallel_pipeline_task_num=${ppt},
                              enable_local_shuffle_planner=false,
                              ignore_storage_data_distribution=true,
                              enable_sql_cache=false,
                              enable_share_hash_table_for_broadcast_join=false,
                              disable_streaming_preaggregations=true)*/
                table1.pk AS field1
            FROM rqg_t1 AS table1
            RIGHT OUTER JOIN rqg_t1 AS table2 ON table1.pk = table2.pk
            LEFT JOIN rqg_t1 AS table3 ON table3.pk = table1.pk
            WHERE table1.col_int_undef_signed IS NOT NULL OR table1.pk <> 10
            GROUP BY field1
            ORDER BY 1
        """ }
        for (int ppt : [3, 4, 7]) {
            def bug20_baseline = sql bug20_baseline_sql(ppt)
            def bug20_result = sql """
                SELECT /*+SET_VAR(use_serial_exchange=true,
                                  parallel_pipeline_task_num=${ppt},
                                  enable_local_shuffle_planner=true,
                                  ignore_storage_data_distribution=true,
                                  enable_sql_cache=false,
                                  enable_share_hash_table_for_broadcast_join=false,
                                  disable_streaming_preaggregations=true)*/
                    table1.pk AS field1
                FROM rqg_t1 AS table1
                RIGHT OUTER JOIN rqg_t1 AS table2 ON table1.pk = table2.pk
                LEFT JOIN rqg_t1 AS table3 ON table3.pk = table1.pk
                WHERE table1.col_int_undef_signed IS NOT NULL OR table1.pk <> 10
                GROUP BY field1
                ORDER BY 1
            """
            assertEquals(bug20_baseline, bug20_result,
                "Bug 20 pptn=${ppt}: result mismatch with serial exchange + agg hang")
        }
        logger.info("Bug 20: PASSED (no hang, correct results)")
    } catch (Throwable t) {
        logger.error("Bug 20 FAILED: ${t.message}")
        assertTrue(false, "Bug 20: Serial exchange + agg hang: ${t.message}")
    }

    // ============================================================
    //  Bug 21: Multi-distinct COUNT on many-bucket table → COREDUMP
    //  RQG build 186737/186929/186952: AggSinkOperatorX::sink → set_ready_to_read
    //  with empty source_deps.
    //
    //  Root cause: AGG operators (streaming, distinct-streaming, serialize) requested
    //  PASSTHROUGH from non-ScanNode serial children (Exchange, AGG), inserting a
    //  PASSTHROUGH LE that created a pipeline split disconnecting AggSink↔AggSource
    //  shared state.
    //
    //  Fix: restrict AGG PASSTHROUGH requests to ScanNode children only.
    //  Triggered by: multi-distinct COUNT/MIN with MultiCastDataSinks feeding
    //  serial UNPARTITIONED Exchanges into streaming AGG fragments.
    // ============================================================
    sql "DROP TABLE IF EXISTS rqg_t5_many_buckets"
    sql """
        CREATE TABLE rqg_t5_many_buckets (
            pk INT NOT NULL,
            col_int_undef_signed INT,
            col_date_undef_signed DATE,
            col_date_undef_signed2 DATE,
            col_varchar_1024__undef_signed VARCHAR(1024)
        ) ENGINE=OLAP
        DUPLICATE KEY(pk)
        DISTRIBUTED BY HASH(pk) BUCKETS 56
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO rqg_t5_many_buckets VALUES
        (1,1,'2023-12-09','2024-06-01','s1'),(2,2,'2023-03-15','2024-01-20','s2'),
        (3,3,'2023-07-22','2024-03-10',NULL),(4,4,'2023-12-09','2024-06-01','s4'),
        (5,5,'2023-01-05','2024-09-15','s5'),(6,6,'2023-08-11','2024-02-28','s6'),
        (7,7,'2023-04-18','2024-07-04',NULL),(8,8,'2023-11-25','2024-05-12','s8'),
        (9,9,'2023-06-30','2024-11-19','s9'),(10,10,'2023-02-14','2024-08-07','s10')
    """

    try {
        logger.info("Bug 21: Testing multi-distinct COUNT on many-bucket table (COREDUMP fix)")
        for (int ppt : [4, 6]) {
            // Test without use_serial_exchange
            def bug21_baseline = sql """
                SELECT /*+SET_VAR(enable_local_shuffle_planner=false,
                                  parallel_pipeline_task_num=${ppt},
                                  ignore_storage_data_distribution=true,
                                  enable_sql_cache=false,
                                  query_timeout=60)*/
                    MIN(distinct col_date_undef_signed),
                    COUNT(distinct col_date_undef_signed2),
                    COUNT(distinct col_int_undef_signed)
                FROM rqg_t5_many_buckets
                WHERE col_int_undef_signed = col_int_undef_signed
                LIMIT 1000
            """
            def bug21_result = sql """
                SELECT /*+SET_VAR(enable_local_shuffle_planner=true,
                                  parallel_pipeline_task_num=${ppt},
                                  ignore_storage_data_distribution=true,
                                  enable_sql_cache=false,
                                  query_timeout=60)*/
                    MIN(distinct col_date_undef_signed),
                    COUNT(distinct col_date_undef_signed2),
                    COUNT(distinct col_int_undef_signed)
                FROM rqg_t5_many_buckets
                WHERE col_int_undef_signed = col_int_undef_signed
                LIMIT 1000
            """
            assertEquals(bug21_baseline, bug21_result,
                "Bug 21 pptn=${ppt}: multi-distinct COUNT result mismatch (was COREDUMP)")
        }
        logger.info("Bug 21: PASSED (no crash, correct results)")
    } catch (Throwable t) {
        // Timeout/hang is a real failure mode for this bug: an EOS/close-count mismatch in the
        // coupled pipelines can hang instead of crashing, so a timeout here may be the very
        // regression we are testing. Let it fail too — do not mask it as SKIPPED.
        logger.error("Bug 21 FAILED: ${t.message}")
        assertTrue(false, "Bug 21: Multi-distinct COUNT COREDUMP/hang: ${t.message}")
    }

    // ============================================================
    //  Bug 22: AGG/SORT above FE-planned LOCAL_EXCHANGE → COREDUMP
    //  (set_ready_to_read DCHECK failure with empty source_deps)
    //
    //  Root cause: when FE inserts LOCAL_EXCHANGE_NODE below a pipeline-
    //  splitting operator (AGG, SORT), LOCAL_EXCHANGE restores its immediate
    //  pipeline to _num_instances tasks, but ancestor pipelines (e.g.,
    //  AggSource) still carry the reduced num_tasks from the serial operator.
    //  This causes instance 1+ to create AggSink tasks but not AggSource
    //  tasks, leaving source_deps uninitialized → DCHECK in set_ready_to_read.
    //
    //  Fix: _propagate_local_exchange_num_tasks() walks the DAG upward from
    //  LOCAL_EXCHANGE and raises ancestor pipeline num_tasks to _num_instances.
    // ============================================================
    try {
        logger.info("Bug 22: Testing AGG/SORT above LOCAL_EXCHANGE num_tasks propagation")
        for (int ppt : [4, 6]) {
            // 22a: Simple AGG with GROUP BY over pooling scan
            def bug22a_baseline = sql """
                SELECT /*+SET_VAR(enable_local_shuffle_planner=false,
                                  parallel_pipeline_task_num=${ppt},
                                  ignore_storage_data_distribution=true,
                                  enable_sql_cache=false)*/
                    col_int_undef_signed, COUNT(*), SUM(col_int_undef_signed2)
                FROM rqg_t1
                GROUP BY col_int_undef_signed
                ORDER BY 1
            """
            def bug22a_result = sql """
                SELECT /*+SET_VAR(enable_local_shuffle_planner=true,
                                  parallel_pipeline_task_num=${ppt},
                                  ignore_storage_data_distribution=true,
                                  enable_sql_cache=false)*/
                    col_int_undef_signed, COUNT(*), SUM(col_int_undef_signed2)
                FROM rqg_t1
                GROUP BY col_int_undef_signed
                ORDER BY 1
            """
            assertEquals(bug22a_baseline, bug22a_result,
                "Bug 22a pptn=${ppt}: AGG GROUP BY result mismatch (was COREDUMP)")

            // 22b: SORT + AGG (two pipeline splits above LOCAL_EXCHANGE)
            def bug22b_baseline = sql """
                SELECT /*+SET_VAR(enable_local_shuffle_planner=false,
                                  parallel_pipeline_task_num=${ppt},
                                  ignore_storage_data_distribution=true,
                                  enable_sql_cache=false)*/
                    col_int_undef_signed, COUNT(*) AS cnt
                FROM rqg_t1
                GROUP BY col_int_undef_signed
                ORDER BY cnt DESC, col_int_undef_signed
            """
            def bug22b_result = sql """
                SELECT /*+SET_VAR(enable_local_shuffle_planner=true,
                                  parallel_pipeline_task_num=${ppt},
                                  ignore_storage_data_distribution=true,
                                  enable_sql_cache=false)*/
                    col_int_undef_signed, COUNT(*) AS cnt
                FROM rqg_t1
                GROUP BY col_int_undef_signed
                ORDER BY cnt DESC, col_int_undef_signed
            """
            assertEquals(bug22b_baseline, bug22b_result,
                "Bug 22b pptn=${ppt}: SORT+AGG result mismatch (was COREDUMP)")

            // 22c: JOIN + AGG (join probe pipeline also needs num_tasks propagation)
            def bug22c_baseline = sql """
                SELECT /*+SET_VAR(enable_local_shuffle_planner=false,
                                  parallel_pipeline_task_num=${ppt},
                                  ignore_storage_data_distribution=true,
                                  enable_sql_cache=false)*/
                    t1.col_int_undef_signed, COUNT(*)
                FROM rqg_t1 t1 JOIN rqg_t2 t2 ON t1.pk = t2.pk
                GROUP BY t1.col_int_undef_signed
                ORDER BY 1
            """
            def bug22c_result = sql """
                SELECT /*+SET_VAR(enable_local_shuffle_planner=true,
                                  parallel_pipeline_task_num=${ppt},
                                  ignore_storage_data_distribution=true,
                                  enable_sql_cache=false)*/
                    t1.col_int_undef_signed, COUNT(*)
                FROM rqg_t1 t1 JOIN rqg_t2 t2 ON t1.pk = t2.pk
                GROUP BY t1.col_int_undef_signed
                ORDER BY 1
            """
            assertEquals(bug22c_baseline, bug22c_result,
                "Bug 22c pptn=${ppt}: JOIN+AGG result mismatch (was COREDUMP)")

            // 22d: AGG without GROUP BY (scalar agg, PASS_TO_ONE exchange)
            def bug22d_baseline = sql """
                SELECT /*+SET_VAR(enable_local_shuffle_planner=false,
                                  parallel_pipeline_task_num=${ppt},
                                  ignore_storage_data_distribution=true,
                                  enable_sql_cache=false)*/
                    COUNT(*), SUM(col_int_undef_signed), AVG(col_int_undef_signed2)
                FROM rqg_t1
            """
            def bug22d_result = sql """
                SELECT /*+SET_VAR(enable_local_shuffle_planner=true,
                                  parallel_pipeline_task_num=${ppt},
                                  ignore_storage_data_distribution=true,
                                  enable_sql_cache=false)*/
                    COUNT(*), SUM(col_int_undef_signed), AVG(col_int_undef_signed2)
                FROM rqg_t1
            """
            assertEquals(bug22d_baseline, bug22d_result,
                "Bug 22d pptn=${ppt}: scalar AGG result mismatch")
        }
        logger.info("Bug 22: PASSED (no crash, correct results)")
    } catch (Throwable t) {
        logger.error("Bug 22 FAILED: ${t.message}")
        assertTrue(false, "Bug 22: AGG/SORT num_tasks propagation: ${t.message}")
    }

    // ==================== Bug 23 ====================
    // canUseDistinctStreamingAgg + GROUPING SETS + serial scan → missing LE
    // When enable_distinct_streaming_aggregation=true, AggregationNode's
    // canUseDistinctStreamingAgg path set requireChild=noRequire() without
    // checking child serial status → serial RepeatNode feeds directly into
    // non-serial AggregationNode → shared_state mismatch on multi-BE.
    // Fix: add isSerialOperatorOnBe check in the noRequire branch.
    try {
        for (def pptn : [2, 4]) {
            def bug23_baseline = sql """
                SELECT /*+SET_VAR(enable_local_shuffle_planner=false,
                                   parallel_pipeline_task_num=${pptn},
                                   ignore_storage_data_distribution=true,
                                   enable_sql_cache=false,
                                   disable_streaming_preaggregations=false,
                                   enable_distinct_streaming_aggregation=true)*/
                    col_int_undef_signed, count(*)
                FROM rqg_t1
                GROUP BY GROUPING SETS ((col_int_undef_signed), (pk), ())
                ORDER BY 1, 2
            """
            def bug23_result = sql """
                SELECT /*+SET_VAR(enable_local_shuffle_planner=true,
                                   parallel_pipeline_task_num=${pptn},
                                   ignore_storage_data_distribution=true,
                                   enable_sql_cache=false,
                                   disable_streaming_preaggregations=false,
                                   enable_distinct_streaming_aggregation=true)*/
                    col_int_undef_signed, count(*)
                FROM rqg_t1
                GROUP BY GROUPING SETS ((col_int_undef_signed), (pk), ())
                ORDER BY 1, 2
            """
            assertEquals(bug23_baseline, bug23_result,
                "Bug 23 pptn=${pptn}: GROUPING SETS + distinct streaming agg result mismatch")
        }
        logger.info("Bug 23: PASSED (no crash, correct results)")
    } catch (Throwable t) {
        logger.error("Bug 23 FAILED: ${t.message}")
        assertTrue(false, "Bug 23: canUseDistinctStreamingAgg + GROUPING SETS: ${t.message}")
    }

    // ============================================================
    //  Bug 24: BUCKET_SHUFFLE join + pooling scan + local shuffle
    //          causes data loss when destination routing uses all
    //          instances instead of firstInstancePerWorker.
    //
    //  Root cause: filterInstancesWhichCanReceiveDataFromRemote() had
    //  a special branch for BUCKET_SHUFFLE that returned all instances
    //  (40) as destinations, but BE native local exchange creates only
    //  4 receiver tasks (one per BE). Destination mismatch causes rows
    //  sent to non-existent receivers to be lost.
    //
    //  Trigger conditions:
    //    - BUCKET_SHUFFLE join plan (requires multi-BE + specific pptn)
    //    - ignore_storage_data_distribution=true (pooling scan)
    //    - enable_local_shuffle=true
    //    - pptn that makes scan serial (scanRanges < pptn * numBE)
    // ============================================================
    try {
        logger.info("Bug 24: BUCKET_SHUFFLE + pooling scan destination routing")
        sql "DROP TABLE IF EXISTS bug24_t1"
        sql "DROP TABLE IF EXISTS bug24_t2"

        sql """
            CREATE TABLE bug24_t1 (
                pk INT NOT NULL,
                val VARCHAR(64),
                INDEX idx_val (val) USING INVERTED
            ) ENGINE=OLAP
            DUPLICATE KEY(pk)
            DISTRIBUTED BY HASH(pk) BUCKETS 10
            PROPERTIES ("replication_num" = "1")
        """

        sql """
            CREATE TABLE bug24_t2 (
                pk INT NOT NULL,
                val VARCHAR(64),
                INDEX idx_val (val) USING INVERTED
            ) ENGINE=OLAP
            DUPLICATE KEY(pk)
            DISTRIBUTED BY HASH(pk) BUCKETS 10
            PROPERTIES ("replication_num" = "1")
        """

        // Insert 20 rows into t1, 50 into t2
        for (int i = 1; i <= 20; i++) {
            sql "INSERT INTO bug24_t1 VALUES (${i}, 'row_${i}')"
        }
        for (int i = 1; i <= 50; i++) {
            sql "INSERT INTO bug24_t2 VALUES (${i}, 'row_${i}')"
        }

        // Baseline: no local shuffle
        def bug24_baseline = sql """
            SELECT /*+SET_VAR(enable_local_shuffle=false,enable_sql_cache=false)*/
            count(*) FROM (
                SELECT * FROM bug24_t1 AS t1
                LEFT JOIN (SELECT * FROM bug24_t2) AS t2 ON t1.pk = t2.pk
                ORDER BY t2.pk DESC, t1.pk DESC LIMIT 21
            ) t
        """

        // Test with multiple pptn values to catch the specific trigger
        for (int pptn : [1, 2, 3, 4, 5, 8, 10]) {
            def result = sql """
                SELECT /*+SET_VAR(
                    parallel_pipeline_task_num=${pptn},
                    ignore_storage_data_distribution=true,
                    enable_local_shuffle=true,
                    enable_local_shuffle_planner=false,
                    enable_sql_cache=false
                )*/ count(*) FROM (
                    SELECT * FROM bug24_t1 AS t1
                    LEFT JOIN (SELECT * FROM bug24_t2) AS t2 ON t1.pk = t2.pk
                    ORDER BY t2.pk DESC, t1.pk DESC LIMIT 21
                ) t
            """
            assertEquals(bug24_baseline, result,
                "Bug 24 pptn=${pptn} planner=false: BUCKET_SHUFFLE+pooling result mismatch")

            def result2 = sql """
                SELECT /*+SET_VAR(
                    parallel_pipeline_task_num=${pptn},
                    ignore_storage_data_distribution=true,
                    enable_local_shuffle=true,
                    enable_local_shuffle_planner=true,
                    enable_sql_cache=false
                )*/ count(*) FROM (
                    SELECT * FROM bug24_t1 AS t1
                    LEFT JOIN (SELECT * FROM bug24_t2) AS t2 ON t1.pk = t2.pk
                    ORDER BY t2.pk DESC, t1.pk DESC LIMIT 21
                ) t
            """
            assertEquals(bug24_baseline, result2,
                "Bug 24 pptn=${pptn} planner=true: BUCKET_SHUFFLE+pooling result mismatch")
        }
        logger.info("Bug 24: PASSED")
    } catch (Throwable t) {
        logger.error("Bug 24 FAILED: ${t.message}")
        assertTrue(false, "Bug 24: BUCKET_SHUFFLE+pooling destination routing: ${t.message}")
    }

    // Bug 25: COLOCATE JOIN + NLJ CROSS JOIN probe side → wrong BUCKET_HASH_SHUFFLE
    // isColocated() traverses subtree and returns false when NLJ is in the probe side,
    // causing COLOCATE JOIN to fall into generic requireHash() → LOCAL_EXECUTION_HASH_SHUFFLE
    // which breaks bucket distribution → result mismatch.
    // Fix: use isColocate() directly on the HashJoinNode instead of subtree check.
    logger.info("Bug 25: COLOCATE JOIN with NLJ CROSS JOIN probe side")
    try {
        sql """
            CREATE TABLE IF NOT EXISTS bug25_t20 (
                pk INT, col1 INT
            ) DISTRIBUTED BY HASH(pk) BUCKETS 10
            PROPERTIES ("replication_num" = "1")
        """
        sql """
            CREATE TABLE IF NOT EXISTS bug25_t24 (
                pk INT, col1 INT
            ) DISTRIBUTED BY HASH(pk) BUCKETS 10
            PROPERTIES ("replication_num" = "1")
        """
        sql """
            CREATE TABLE IF NOT EXISTS bug25_t7 (
                pk INT, col1 INT
            ) DISTRIBUTED BY HASH(pk) BUCKETS 10
            PROPERTIES ("replication_num" = "1")
        """
        sql "TRUNCATE TABLE bug25_t20"
        sql "TRUNCATE TABLE bug25_t24"
        sql "TRUNCATE TABLE bug25_t7"
        (1..20).each { i -> sql "INSERT INTO bug25_t20 VALUES ($i, $i)" }
        (1..24).each { i -> sql "INSERT INTO bug25_t24 VALUES ($i, $i)" }
        (1..7).each { i -> sql "INSERT INTO bug25_t7 VALUES ($i, $i)" }

        def query = """
            WITH cte1 AS (
                SELECT t1.pk FROM bug25_t20 AS t1 CROSS JOIN bug25_t24 AS alias1
            ),
            cte2 AS (
                SELECT t1.pk FROM bug25_t20 AS t1
                INNER JOIN bug25_t7 AS alias2 ON t1.pk = alias2.pk
            )
            SELECT cte1.pk AS pk1 FROM cte1
            RIGHT OUTER JOIN cte2 AS alias3 ON cte1.pk = alias3.pk
            LIMIT 66666666
        """

        for (int pptn : [0, 1, 2, 4]) {
            def feResult = sql """
                /*+SET_VAR(enable_local_shuffle_planner=true,
                           ignore_storage_data_distribution=true,
                           parallel_pipeline_task_num=${pptn},
                           enable_sql_cache=false)*/ ${query}
            """
            def beResult = sql """
                /*+SET_VAR(enable_local_shuffle_planner=false,
                           ignore_storage_data_distribution=true,
                           parallel_pipeline_task_num=${pptn},
                           enable_sql_cache=false)*/ ${query}
            """
            assertEquals(beResult.size(), feResult.size(),
                "Bug 25 pptn=${pptn}: FE rows=${feResult.size()}, BE rows=${beResult.size()}")
        }
        logger.info("Bug 25: PASSED")
    } catch (Throwable t) {
        logger.error("Bug 25 FAILED: ${t.message}")
        assertTrue(false, "Bug 25: COLOCATE+NLJ CROSS probe: ${t.message}")
    }

    logger.info("=== All RQG bug reproduction tests completed ===")
}
