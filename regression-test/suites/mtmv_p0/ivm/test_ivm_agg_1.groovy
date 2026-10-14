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

import org.awaitility.Awaitility
import static java.util.concurrent.TimeUnit.SECONDS

suite("test_ivm_agg_1") {

    // =========================================================
    // Part 1: Grouped aggregate MV (COUNT + SUM)
    // =========================================================

    sql """drop materialized view if exists test_ivm_agg_mtmv_mv;"""
    sql """drop table if exists test_ivm_agg_mtmv_base;"""

    // 1. Create MOW base table
    sql """
        CREATE TABLE test_ivm_agg_mtmv_base (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    // 2. Insert initial rows: groups k1=1,2,3
    sql """
        INSERT INTO test_ivm_agg_mtmv_base VALUES
            (1, 10),
            (2, 20),
            (3, 30);
    """

    // 3. Create IVM MV with grouped aggregate (COUNT(*) + SUM)
    sql """
        CREATE MATERIALIZED VIEW test_ivm_agg_mtmv_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT k1, COUNT(*) AS cnt, SUM(v1) AS sum_v1 FROM test_ivm_agg_mtmv_base GROUP BY k1;
    """

    // 4. Verify MV metadata
    def mvInfos = sql """select State from mv_infos('database'='${context.dbName}') where Name = 'test_ivm_agg_mtmv_mv'"""
    assertTrue(mvInfos.toString().contains("INIT"))

    // 5. Verify MV is UNIQUE_KEYS
    def descResult = sql """desc test_ivm_agg_mtmv_mv all"""
    assertTrue(descResult.toString().contains("UNIQUE_KEYS"))

    // 6. Initial INCREMENTAL refresh consumes the historical binlog and establishes the stream offset.
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_mv")

    // Each k1 maps to 1 row, so COUNT(*)=1, SUM(v1)=v1
    order_qt_agg_after_initial_incremental """SELECT k1, cnt, sum_v1 FROM test_ivm_agg_mtmv_mv"""

    // 7. Insert new rows: new group k1=4 and update existing k1=1 (MOW upsert replaces v1)
    sql """
        INSERT INTO test_ivm_agg_mtmv_base VALUES
            (4, 40),
            (1, 15);
    """

    // 8. INCREMENTAL refresh consumes only the binlog records written after the initial refresh.
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_mv")

    order_qt_agg_after_first_update_incremental """SELECT k1, cnt, sum_v1 FROM test_ivm_agg_mtmv_mv"""

    // 9. Update another row: k1=2 gets new value
    sql """
        INSERT INTO test_ivm_agg_mtmv_base VALUES
            (2, 25);
    """

    // 10. Second INCREMENTAL refresh — also assert no error and check output
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_mv")

    order_qt_agg_after_second_update_incremental """SELECT k1, cnt, sum_v1 FROM test_ivm_agg_mtmv_mv"""

    // 11. COMPLETE refresh — produces correct results (full recomputation)
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_mv")

    order_qt_agg_after_complete """SELECT k1, cnt, sum_v1 FROM test_ivm_agg_mtmv_mv"""

    // =========================================================
    // Part 2: Scalar aggregate MV (no GROUP BY)
    // =========================================================

    sql """drop materialized view if exists test_ivm_agg_mtmv_scalar_mv;"""
    sql """drop table if exists test_ivm_agg_mtmv_scalar_base;"""

    sql """
        CREATE TABLE test_ivm_agg_mtmv_scalar_base (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        INSERT INTO test_ivm_agg_mtmv_scalar_base VALUES
            (1, 10),
            (2, 20),
            (3, 30);
    """

    // Scalar agg MV: COUNT(*), SUM, AVG, COUNT(expr) over the whole table
    sql """
        CREATE MATERIALIZED VIEW test_ivm_agg_mtmv_scalar_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT COUNT(*) AS total_cnt, SUM(v1) AS total_sum, AVG(v1) AS avg_v1, COUNT(v1) AS cnt_v1
           FROM test_ivm_agg_mtmv_scalar_base;
    """

    def scalarMvInfos = sql """select State from mv_infos('database'='${context.dbName}') where Name = 'test_ivm_agg_mtmv_scalar_mv'"""
    assertTrue(scalarMvInfos.toString().contains("INIT"))

    def scalarDescResult = sql """desc test_ivm_agg_mtmv_scalar_mv all"""
    assertTrue(scalarDescResult.toString().contains("UNIQUE_KEYS"))

    // Initial INCREMENTAL refresh: 3 rows, total_cnt=3, total_sum=60, avg=20, cnt_v1=3
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_scalar_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_scalar_mv")

    order_qt_scalar_after_initial_incremental """
        SELECT total_cnt, total_sum, avg_v1, cnt_v1 FROM test_ivm_agg_mtmv_scalar_mv
    """

    // Upsert k1=1 (v1: 10 → 15)
    sql """INSERT INTO test_ivm_agg_mtmv_scalar_base VALUES (1, 15);"""

    // INCREMENTAL refresh consumes the update after the initial stream offset.
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_scalar_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_scalar_mv")

    order_qt_scalar_after_first_update_incremental """
        SELECT total_cnt, total_sum, avg_v1, cnt_v1 FROM test_ivm_agg_mtmv_scalar_mv
    """

    // Insert new row k1=4
    sql """INSERT INTO test_ivm_agg_mtmv_scalar_base VALUES (4, 40);"""

    // Second INCREMENTAL refresh — also assert no error
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_scalar_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_scalar_mv")

    order_qt_scalar_after_second_update_incremental """
        SELECT total_cnt, total_sum, avg_v1, cnt_v1 FROM test_ivm_agg_mtmv_scalar_mv
    """

    // Final COMPLETE refresh: k1=1(15),2(20),3(30),4(40)
    // total_cnt=4, total_sum=105, avg=26.25, cnt_v1=4
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_scalar_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_scalar_mv")

    order_qt_scalar_after_final_complete """
        SELECT total_cnt, total_sum, avg_v1, cnt_v1 FROM test_ivm_agg_mtmv_scalar_mv
    """

    // =========================================================
    // Part 3: MIN / MAX aggregate MV (grouped by non-key column so each group has multiple rows)
    // =========================================================

    sql """drop materialized view if exists test_ivm_agg_mtmv_minmax_mv;"""
    sql """drop table if exists test_ivm_agg_mtmv_minmax_base;"""

    sql """
        CREATE TABLE test_ivm_agg_mtmv_minmax_base (
            k1 INT,
            grp INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    // Group grp=1: three rows (k1=10→v1=100, k1=11→v1=200, k1=12→v1=150) → MIN=100, MAX=200
    // Group grp=2: three rows (k1=20→v1=50,  k1=21→v1=80,  k1=22→v1=70)  → MIN=50,  MAX=80
    sql """
        INSERT INTO test_ivm_agg_mtmv_minmax_base VALUES
            (10, 1, 100),
            (11, 1, 200),
            (12, 1, 150),
            (20, 2, 50),
            (21, 2, 80),
            (22, 2, 70);
    """

    // MV: GROUP BY grp, MIN(v1), MAX(v1)
    sql """
        CREATE MATERIALIZED VIEW test_ivm_agg_mtmv_minmax_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(grp) BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT grp, MIN(v1) AS min_v1, MAX(v1) AS max_v1
           FROM test_ivm_agg_mtmv_minmax_base
           GROUP BY grp;
    """

    def minmaxMvInfos = sql """select State from mv_infos('database'='${context.dbName}') where Name = 'test_ivm_agg_mtmv_minmax_mv'"""
    assertTrue(minmaxMvInfos.toString().contains("INIT"))

    // Verify schema includes hidden MIN/MAX columns
    def minmaxDescResult = sql """desc test_ivm_agg_mtmv_minmax_mv all"""
    assertTrue(minmaxDescResult.toString().contains("UNIQUE_KEYS"))

    // Initial INCREMENTAL refresh: grp=1 → min=100,max=200; grp=2 → min=50,max=80
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_minmax_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_minmax_mv")

    order_qt_minmax_after_initial_incremental """
        SELECT grp, min_v1, max_v1 FROM test_ivm_agg_mtmv_minmax_mv ORDER BY grp
    """

    // Test 1: Update k1=10 (grp=1) from v1=100 to v1=90 → new MIN should be 90.
    //   Group grp=1 has other rows (200, 150), so MAX=200 is safe.
    //   MIN guard: old_min=100, deltaDel=100, deltaInsert=90.
    //   delete 命中旧 MIN (100), but insert 提供更优值 90 ≤ 100 → guard passes ✓
    sql """INSERT INTO test_ivm_agg_mtmv_minmax_base VALUES (10, 1, 90);"""

    // Test 2: Update k1=21 (grp=2) from v1=80 to v1=90 → new MAX should be 90.
    //   Group grp=2 has other rows (50, 70), so MIN=50 is safe.
    //   MAX guard: old_max=80, deltaDel=80, deltaInsert=90.
    //   delete 命中旧 MAX (80), but insert 提供更优值 90 ≥ 80 → guard passes ✓
    sql """INSERT INTO test_ivm_agg_mtmv_minmax_base VALUES (21, 2, 90);"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_minmax_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_minmax_mv")

    // grp=1: min=90, max=200; grp=2: min=50, max=90
    order_qt_minmax_after_incremental """
        SELECT grp, min_v1, max_v1 FROM test_ivm_agg_mtmv_minmax_mv ORDER BY grp
    """

    // Final COMPLETE refresh to verify ground truth
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_minmax_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_minmax_mv")

    order_qt_minmax_after_final_complete """
        SELECT grp, min_v1, max_v1 FROM test_ivm_agg_mtmv_minmax_mv ORDER BY grp
    """

    // =========================================================
    // Part 4: Agg MV with MIN + binlog_op=delete hitting MIN boundary
    //         → INCREMENTAL should fail (assert_true guard), then COMPLETE recovers
    // =========================================================
    // When an INCREMENTAL refresh encounters a delete (binlog_op=1) whose value equals
    // the current MIN, the assert_true guard fires:
    //   assert_true(deltaDelMin IS NULL OR old_min IS NULL OR deltaDelMin > old_min)
    // Because deltaDelMin == old_min, the guard fails and the execution throws an exception.
    // Since we use `REFRESH ... INCREMENTAL` (explicit), the task does NOT fall back to
    // COMPLETE — it fails with status FAILED. We verify this, then run COMPLETE to recover.

    sql """drop materialized view if exists test_ivm_agg_mtmv_minmax_op_mv;"""
    sql """drop table if exists test_ivm_agg_mmop_base;"""

    sql """
        CREATE TABLE test_ivm_agg_mmop_base (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    // Initial data: group by v1 ranges via k1 grouping
    // k1=1 (v1=10, op=0), k1=2 (v1=20, op=0), k1=3 (v1=30, op=0)
    sql """
        INSERT INTO test_ivm_agg_mmop_base VALUES
            (1, 10),
            (2, 20),
            (3, 30);
    """

    // Scalar agg MV with MIN/MAX (no GROUP BY) — the single group makes boundary detection simple
    sql """
        CREATE MATERIALIZED VIEW test_ivm_agg_mtmv_minmax_op_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT MIN(v1) AS min_v1, MAX(v1) AS max_v1, COUNT(*) AS cnt
           FROM test_ivm_agg_mmop_base;
    """

    def minmaxOpMvInfos = sql """select State from mv_infos('database'='${context.dbName}') where Name = 'test_ivm_agg_mtmv_minmax_op_mv'"""
    assertTrue(minmaxOpMvInfos.toString().contains("INIT"))

    // Step 1: Initial INCREMENTAL refresh — min=10, max=30, cnt=3
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_minmax_op_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_minmax_op_mv")

    order_qt_minmax_op_after_initial_incremental """
        SELECT min_v1, max_v1, cnt FROM test_ivm_agg_mtmv_minmax_op_mv
    """

    // Step 2: Safe INCREMENTAL — insert a new row (op=0) that does NOT touch MIN boundary
    // This writes one new insert after the initial stream offset.
    sql """INSERT INTO test_ivm_agg_mmop_base VALUES (4, 25);"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_minmax_op_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_minmax_op_mv")

    // Step 3: Now delete k1=1 (the MIN value v1=10 was deleted),
    // and insert a dirty row to ensure the partition is stale for INCREMENTAL.
    sql """DELETE FROM test_ivm_agg_mmop_base WHERE k1 = 1;"""

    // Dirty the partition to ensure INCREMENTAL actually runs
    sql """INSERT INTO test_ivm_agg_mmop_base VALUES (5, 35);"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_minmax_op_mv INCREMENTAL"""

    // Wait for the task to reach a terminal state (expected FAILED)
    def showTasksSql = """
        select TaskId, Status, ErrorMsg from tasks('type'='mv')
        where MvDatabaseName = '${context.dbName}' and MvName = 'test_ivm_agg_mtmv_minmax_op_mv'
        order by CreateTime DESC limit 1
    """
    def taskResult
    Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
        taskResult = sql(showTasksSql)
        if (taskResult.isEmpty()) return false
        def st = taskResult[0][1].toString()
        return st != 'PENDING' && st != 'RUNNING'
    })
    def taskStatus = taskResult[0][1].toString()
    logger.info("MIN boundary delete INCREMENTAL task status: " + taskStatus
            + ", error: " + taskResult[0][2])
    // The task should be FAILED because assert_true guard detected MIN boundary deletion
    assertTrue(taskStatus == "FAILED",
            "Expected INCREMENTAL to fail when deleting MIN value, but got: " + taskStatus)
    def errorMsg = taskResult[0][2].toString()
    assertTrue(errorMsg.contains("MIN_MAX_BOUNDARY") || errorMsg.contains("IVM")
                    || errorMsg.contains("assert_true") || errorMsg.contains("fallback"),
            "Error should mention MIN_MAX_BOUNDARY/IVM/assert_true/fallback but got: " + errorMsg)

    // Step 4: COMPLETE refresh recovers from the failed task. The snapshot excludes deleted k1=1,
    // so min=20, max=35, cnt=4.
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_minmax_op_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_minmax_op_mv")

    order_qt_minmax_op_after_recovery """
        SELECT min_v1, max_v1, cnt FROM test_ivm_agg_mtmv_minmax_op_mv
    """

    // Step 5: Fix the data — upsert k1=1 back to op=0, then safe INCREMENTAL should succeed
    sql """INSERT INTO test_ivm_agg_mmop_base VALUES (1, 10);"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_minmax_op_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_minmax_op_mv")

    // After safe INCREMENTAL, output remains queryable.
    order_qt_minmax_op_after_safe_incremental """
        SELECT min_v1, max_v1, cnt FROM test_ivm_agg_mtmv_minmax_op_mv
    """

    // =========================================================
    // Part 5: Grouped agg MV with NULL values + binlog_op
    //         Tests that SUM/AVG/COUNT(expr)/MIN/MAX handle NULLs correctly
    // =========================================================

    sql """drop materialized view if exists test_ivm_agg_mtmv_null_mv;"""
    sql """drop table if exists test_ivm_agg_mtmv_null_base;"""

    sql """
        CREATE TABLE test_ivm_agg_mtmv_null_base (
            k1 INT,
            grp INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    // Initial data: 3 groups
    // grp=1: two rows, both non-NULL (v1=10, v1=20)
    // grp=2: two rows, one NULL (v1=NULL, v1=30)
    // grp=3: one row, all NULL (v1=NULL)
    sql """
        INSERT INTO test_ivm_agg_mtmv_null_base VALUES
            (1, 1, 10),
            (2, 1, 20),
            (3, 2, NULL),
            (4, 2, 30),
            (5, 3, NULL);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_agg_mtmv_null_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT grp,
                  SUM(v1) AS sum_v1,
                  AVG(v1) AS avg_v1,
                  COUNT(v1) AS cnt_v1,
                  MIN(v1) AS min_v1,
                  MAX(v1) AS max_v1,
                  COUNT(*) AS cnt_all
           FROM test_ivm_agg_mtmv_null_base
           GROUP BY grp;
    """

    // Step 1: Initial INCREMENTAL refresh
    // grp=1: sum=30, avg=15, cnt_v1=2, min=10, max=20, cnt_all=2
    // grp=2: sum=30, avg=30, cnt_v1=1, min=30, max=30, cnt_all=2
    // grp=3: sum=NULL, avg=NULL, cnt_v1=0, min=NULL, max=NULL, cnt_all=1
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_null_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_null_mv")

    order_qt_null_after_initial_incremental """
        SELECT grp, sum_v1, avg_v1, cnt_v1, min_v1, max_v1, cnt_all
        FROM test_ivm_agg_mtmv_null_mv ORDER BY grp
    """

    // Step 2: INCREMENTAL — insert a NULL value into grp=1 and a non-NULL into grp=3
    // The incremental input here contains only new inserts (all op=0), so INCREMENTAL should succeed.
    sql """INSERT INTO test_ivm_agg_mtmv_null_base VALUES (6, 1, NULL);"""
    sql """INSERT INTO test_ivm_agg_mtmv_null_base VALUES (7, 3, 50);"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_null_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_null_mv")

    // After INCREMENTAL, only the two rows written after the initial stream offset are applied.
    order_qt_null_after_update_incremental """
        SELECT grp, sum_v1, avg_v1, cnt_v1, min_v1, max_v1, cnt_all
        FROM test_ivm_agg_mtmv_null_mv ORDER BY grp
    """

    // Step 3: COMPLETE to verify correct final state after NULL-aware incremental
    // grp=1: k1=1(10),2(20),6(NULL) → sum=30, avg=15, cnt_v1=2, min=10, max=20, cnt_all=3
    // grp=2: k1=3(NULL),4(30) → sum=30, avg=30, cnt_v1=1, min=30, max=30, cnt_all=2
    // grp=3: k1=5(NULL),7(50) → sum=50, avg=50, cnt_v1=1, min=50, max=50, cnt_all=2
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_null_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_null_mv")

    order_qt_null_after_final_complete """
        SELECT grp, sum_v1, avg_v1, cnt_v1, min_v1, max_v1, cnt_all
        FROM test_ivm_agg_mtmv_null_mv ORDER BY grp
    """

    // Step 4: Delete a NULL value row (binlog_op=1) — tests caseWhenExprNotNull with delete+NULL
    // Mark k1=6 (grp=1, v1=NULL) as deleted
    sql """DELETE FROM test_ivm_agg_mtmv_null_base WHERE k1 = 6;"""
    // Dirty partition for INCREMENTAL
    sql """INSERT INTO test_ivm_agg_mtmv_null_base VALUES (8, 2, 40);"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_null_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_null_mv")

    // The stream applies the deletion of k1=6 (grp=1, NULL) and insertion of k1=8 (grp=2, 40).
    order_qt_null_after_delete_incremental """
        SELECT grp, sum_v1, avg_v1, cnt_v1, min_v1, max_v1, cnt_all
        FROM test_ivm_agg_mtmv_null_mv ORDER BY grp
    """

    // Step 5: COMPLETE refresh verifies the snapshot after NULL-row deletion.
    // grp=1 excludes deleted k1=6, so sum=30, avg=15, cnt_v1=2, min=10, max=20, cnt_all=2
    // grp=2: k1=3(NULL),4(30),8(40) → sum=70, avg=35, cnt_v1=2, min=30, max=40, cnt_all=3
    // grp=3: k1=5(NULL),7(50) → sum=50, avg=50, cnt_v1=1, min=50, max=50, cnt_all=2
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_null_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_null_mv")

    order_qt_null_after_delete_complete """
        SELECT grp, sum_v1, avg_v1, cnt_v1, min_v1, max_v1, cnt_all
        FROM test_ivm_agg_mtmv_null_mv ORDER BY grp
    """

    // =========================================================
    // Part 6: Decimal AVG incremental refresh
    // =========================================================

    sql """drop materialized view if exists test_ivm_agg_mtmv_decimal_mv;"""
    sql """drop table if exists test_ivm_agg_mtmv_decimal_base;"""

    sql """
        CREATE TABLE test_ivm_agg_mtmv_decimal_base (
            k1 INT,
            grp INT,
            v1 DECIMAL(18, 4)
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        INSERT INTO test_ivm_agg_mtmv_decimal_base VALUES
            (1, 1, 1.2500),
            (2, 1, 1.7500),
            (3, 2, NULL);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_agg_mtmv_decimal_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT grp, AVG(v1) AS avg_v1
           FROM test_ivm_agg_mtmv_decimal_base
           GROUP BY grp;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_decimal_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_decimal_mv")

    order_qt_decimal_avg_after_initial_incremental """
        SELECT grp, avg_v1 FROM test_ivm_agg_mtmv_decimal_mv ORDER BY grp
    """

    sql """INSERT INTO test_ivm_agg_mtmv_decimal_base VALUES (4, 2, 2.5000);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_decimal_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_decimal_mv")

    order_qt_decimal_avg_after_incremental """
        SELECT grp, avg_v1 FROM test_ivm_agg_mtmv_decimal_mv ORDER BY grp
    """
}
