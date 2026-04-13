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

suite("test_ivm_agg_mtmv") {
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

    // 6. First COMPLETE refresh
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_mv")

    // Each k1 maps to 1 row, so COUNT(*)=1, SUM(v1)=v1
    order_qt_agg_after_first_complete """SELECT k1, cnt, sum_v1 FROM test_ivm_agg_mtmv_mv"""

    // 7. Insert new rows: new group k1=4 and update existing k1=1 (MOW upsert replaces v1)
    sql """
        INSERT INTO test_ivm_agg_mtmv_base VALUES
            (4, 40),
            (1, 15);
    """

    // 8. INCREMENTAL refresh — verify it completes without a type-mismatch error.
    //    NOTE: The current IVM mock reads the full base table as delta, so aggregate values
    //    after an INCREMENTAL refresh are not semantically correct (group counts may be inflated).
    //    We only assert that the task finishes in SUCCESS state (no BE crash / type error).
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_mv")

    // 9. Update another row: k1=2 gets new value
    sql """
        INSERT INTO test_ivm_agg_mtmv_base VALUES
            (2, 25);
    """

    // 10. Second INCREMENTAL refresh — also assert no error
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_mv")

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

    // First COMPLETE refresh: 3 rows, total_cnt=3, total_sum=60, avg=20, cnt_v1=3
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_scalar_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_scalar_mv")

    order_qt_scalar_after_first_complete """
        SELECT total_cnt, total_sum, avg_v1, cnt_v1 FROM test_ivm_agg_mtmv_scalar_mv
    """

    // Upsert k1=1 (v1: 10 → 15)
    sql """INSERT INTO test_ivm_agg_mtmv_scalar_base VALUES (1, 15);"""

    // INCREMENTAL refresh — only asserts SUCCESS (no crash / type-mismatch),
    // since mock IVM reads the full base table so scalar agg values may not be correct.
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_scalar_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_scalar_mv")

    // Insert new row k1=4
    sql """INSERT INTO test_ivm_agg_mtmv_scalar_base VALUES (4, 40);"""

    // Second INCREMENTAL refresh — also assert no error
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_scalar_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_scalar_mv")

    // Final COMPLETE refresh: k1=1(15),2(20),3(30),4(40)
    // total_cnt=4, total_sum=105, avg=26.25, cnt_v1=4
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_scalar_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_scalar_mv")

    order_qt_scalar_after_final_complete """
        SELECT total_cnt, total_sum, avg_v1, cnt_v1 FROM test_ivm_agg_mtmv_scalar_mv
    """

    // =========================================================
    // Part 3: MIN / MAX aggregate MV (grouped)
    // =========================================================

    sql """drop materialized view if exists test_ivm_agg_mtmv_minmax_mv;"""
    sql """drop table if exists test_ivm_agg_mtmv_minmax_base;"""

    sql """
        CREATE TABLE test_ivm_agg_mtmv_minmax_base (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        INSERT INTO test_ivm_agg_mtmv_minmax_base VALUES
            (1, 10),
            (1, 10),
            (2, 20),
            (3, 30);
    """

    // MV: GROUP BY k1, MIN(v1), MAX(v1)
    sql """
        CREATE MATERIALIZED VIEW test_ivm_agg_mtmv_minmax_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT k1, MIN(v1) AS min_v1, MAX(v1) AS max_v1
           FROM test_ivm_agg_mtmv_minmax_base
           GROUP BY k1;
    """

    def minmaxMvInfos = sql """select State from mv_infos('database'='${context.dbName}') where Name = 'test_ivm_agg_mtmv_minmax_mv'"""
    assertTrue(minmaxMvInfos.toString().contains("INIT"))

    // Verify schema includes hidden MIN/MAX columns
    def minmaxDescResult = sql """desc test_ivm_agg_mtmv_minmax_mv all"""
    assertTrue(minmaxDescResult.toString().contains("UNIQUE_KEYS"))

    // First COMPLETE refresh: k1=1 → min=10,max=10; k1=2 → min=20,max=20; k1=3 → min=30,max=30
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_minmax_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_minmax_mv")

    order_qt_minmax_after_first_complete """
        SELECT k1, min_v1, max_v1 FROM test_ivm_agg_mtmv_minmax_mv ORDER BY k1
    """

    // Safe INCREMENTAL: upsert k1=1 with v1=5 (new min) and insert k1=4 (v1=40).
    // NOTE: The current IVM mock reads the full base table as delta (all rows have dml_factor=+1,
    // i.e. no delete stream), so the assert_true guard for boundary deletion is never triggered.
    // Boundary-deletion fallback can only be tested once real binlog-based delta streams are available.
    sql """INSERT INTO test_ivm_agg_mtmv_minmax_base VALUES (1, 5);"""
    sql """INSERT INTO test_ivm_agg_mtmv_minmax_base VALUES (4, 40);"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_minmax_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_minmax_mv")

    // Verify INCREMENTAL refresh completed and produced queryable data (values may not be
    // semantically correct due to the mock delta reading the full base table).
    order_qt_minmax_after_incremental """
        SELECT k1, min_v1, max_v1 FROM test_ivm_agg_mtmv_minmax_mv ORDER BY k1
    """

    // Final COMPLETE refresh to get ground truth
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_minmax_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_minmax_mv")

    // k1=1 → v1=5 (MOW upsert); k1=2 → v1=20; k1=3 → v1=30; k1=4 → v1=40
    order_qt_minmax_after_final_complete """
        SELECT k1, min_v1, max_v1 FROM test_ivm_agg_mtmv_minmax_mv ORDER BY k1
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
    sql """drop table if exists test_ivm_agg_mtmv_minmax_op_base;"""

    sql """
        CREATE TABLE test_ivm_agg_mtmv_minmax_op_base (
            k1 INT,
            v1 INT,
            binlog_op TINYINT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    // Initial data: group by v1 ranges via k1 grouping
    // k1=1 (v1=10, op=0), k1=2 (v1=20, op=0), k1=3 (v1=30, op=0)
    sql """
        INSERT INTO test_ivm_agg_mtmv_minmax_op_base VALUES
            (1, 10, 0),
            (2, 20, 0),
            (3, 30, 0);
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
           FROM test_ivm_agg_mtmv_minmax_op_base;
    """

    def minmaxOpMvInfos = sql """select State from mv_infos('database'='${context.dbName}') where Name = 'test_ivm_agg_mtmv_minmax_op_mv'"""
    assertTrue(minmaxOpMvInfos.toString().contains("INIT"))

    // Step 1: COMPLETE refresh — min=10, max=30, cnt=3
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_minmax_op_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_minmax_op_mv")

    order_qt_minmax_op_after_complete """
        SELECT min_v1, max_v1, cnt FROM test_ivm_agg_mtmv_minmax_op_mv
    """

    // Step 2: Safe INCREMENTAL — insert a new row (op=0) that does NOT touch MIN boundary
    // After this, mock delta reads all 4 rows (all op=0), dml_factor=1 for all → no deletes
    sql """INSERT INTO test_ivm_agg_mtmv_minmax_op_base VALUES (4, 25, 0);"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_minmax_op_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_minmax_op_mv")

    // Step 3: Now upsert k1=1 to binlog_op=1 (mark the MIN value v1=10 as deleted),
    // and insert a dirty row to ensure the partition is stale for INCREMENTAL.
    // The mock delta will read all rows. k1=1 has op=1 → dml_factor=-1,
    // and deleteOnlyExpr picks up v1=10. Since old_min=10 and deltaDelMin=10,
    // the guard (deltaDelMin > old_min) is false → assert_true fires → task FAILS.
    sql """INSERT INTO test_ivm_agg_mtmv_minmax_op_base VALUES (1, 10, 1);"""

    // Dirty the partition to ensure INCREMENTAL actually runs
    sql """INSERT INTO test_ivm_agg_mtmv_minmax_op_base VALUES (5, 35, 0);"""

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
    assertTrue(errorMsg.contains("IVM") || errorMsg.contains("assert_true") || errorMsg.contains("fallback"),
            "Error should mention IVM/assert_true/fallback but got: " + errorMsg)

    // Step 4: COMPLETE refresh to recover — now k1=1(op=1) is physically present,
    // and k1=2(v1=20), k1=3(v1=30), k1=4(v1=25), k1=5(v1=35) all have op=0.
    // COMPLETE ignores binlog_op semantics, so min=10, max=35, cnt=5
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_minmax_op_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_minmax_op_mv")

    order_qt_minmax_op_after_recovery """
        SELECT min_v1, max_v1, cnt FROM test_ivm_agg_mtmv_minmax_op_mv
    """

    // Step 5: Fix the data — upsert k1=1 back to op=0, then safe INCREMENTAL should succeed
    sql """INSERT INTO test_ivm_agg_mtmv_minmax_op_base VALUES (1, 10, 0);"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_minmax_op_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_minmax_op_mv")

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
            v1 INT,
            binlog_op TINYINT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    // Initial data: 3 groups
    // grp=1: two rows, both non-NULL (v1=10, v1=20)
    // grp=2: two rows, one NULL (v1=NULL, v1=30)
    // grp=3: one row, all NULL (v1=NULL)
    sql """
        INSERT INTO test_ivm_agg_mtmv_null_base VALUES
            (1, 1, 10, 0),
            (2, 1, 20, 0),
            (3, 2, NULL, 0),
            (4, 2, 30, 0),
            (5, 3, NULL, 0);
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

    // Step 1: COMPLETE refresh
    // grp=1: sum=30, avg=15, cnt_v1=2, min=10, max=20, cnt_all=2
    // grp=2: sum=30, avg=30, cnt_v1=1, min=30, max=30, cnt_all=2
    // grp=3: sum=NULL, avg=NULL, cnt_v1=0, min=NULL, max=NULL, cnt_all=1
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_null_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_null_mv")

    order_qt_null_after_complete """
        SELECT grp, sum_v1, avg_v1, cnt_v1, min_v1, max_v1, cnt_all
        FROM test_ivm_agg_mtmv_null_mv ORDER BY grp
    """

    // Step 2: INCREMENTAL — insert a NULL value into grp=1 and a non-NULL into grp=3
    // Mock delta reads all rows (all op=0), no deletes, so INCREMENTAL should succeed.
    sql """INSERT INTO test_ivm_agg_mtmv_null_base VALUES (6, 1, NULL, 0);"""
    sql """INSERT INTO test_ivm_agg_mtmv_null_base VALUES (7, 3, 50, 0);"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_null_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_null_mv")

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
    sql """INSERT INTO test_ivm_agg_mtmv_null_base VALUES (6, 1, NULL, 1);"""
    // Dirty partition for INCREMENTAL
    sql """INSERT INTO test_ivm_agg_mtmv_null_base VALUES (8, 2, 40, 0);"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_null_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_null_mv")

    // Step 5: COMPLETE to get ground truth after NULL-row deletion
    // grp=1: k1=1(10),2(20),6(NULL,deleted) → effectively k1=1(10),2(20) → sum=30, avg=15, cnt_v1=2, min=10, max=20, cnt_all=2
    //         BUT with mock delta, COMPLETE still sees all physical rows including k1=6 with op=1
    //         COMPLETE refresh ignores binlog_op, so k1=6 is still counted:
    //         k1=1(10),2(20),6(NULL) → sum=30, avg=15, cnt_v1=2, min=10, max=20, cnt_all=3
    // grp=2: k1=3(NULL),4(30),8(40) → sum=70, avg=35, cnt_v1=2, min=30, max=40, cnt_all=3
    // grp=3: k1=5(NULL),7(50) → sum=50, avg=50, cnt_v1=1, min=50, max=50, cnt_all=2
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_null_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_null_mv")

    order_qt_null_after_delete_complete """
        SELECT grp, sum_v1, avg_v1, cnt_v1, min_v1, max_v1, cnt_all
        FROM test_ivm_agg_mtmv_null_mv ORDER BY grp
    """

    // =========================================================
    // Part 6: Scalar agg MV with all-NULL values + binlog_op
    //         Tests SUM/AVG/COUNT(expr)/MIN/MAX when every value is NULL
    // =========================================================

    sql """drop materialized view if exists test_ivm_agg_mtmv_allnull_mv;"""
    sql """drop table if exists test_ivm_agg_mtmv_allnull_base;"""

    sql """
        CREATE TABLE test_ivm_agg_mtmv_allnull_base (
            k1 INT,
            v1 INT,
            binlog_op TINYINT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    // All values are NULL
    sql """
        INSERT INTO test_ivm_agg_mtmv_allnull_base VALUES
            (1, NULL, 0),
            (2, NULL, 0);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_agg_mtmv_allnull_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT SUM(v1) AS sum_v1,
                  AVG(v1) AS avg_v1,
                  COUNT(v1) AS cnt_v1,
                  MIN(v1) AS min_v1,
                  MAX(v1) AS max_v1,
                  COUNT(*) AS cnt_all
           FROM test_ivm_agg_mtmv_allnull_base;
    """

    // COMPLETE: all NULLs → sum=NULL, avg=NULL, cnt_v1=0, min=NULL, max=NULL, cnt_all=2
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_allnull_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_allnull_mv")

    order_qt_allnull_after_complete """
        SELECT sum_v1, avg_v1, cnt_v1, min_v1, max_v1, cnt_all
        FROM test_ivm_agg_mtmv_allnull_mv
    """

    // Insert a non-NULL value → transitions from all-NULL to mixed
    sql """INSERT INTO test_ivm_agg_mtmv_allnull_base VALUES (3, 100, 0);"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_allnull_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_allnull_mv")

    // COMPLETE to get ground truth: k1=1(NULL),2(NULL),3(100)
    // sum=100, avg=100, cnt_v1=1, min=100, max=100, cnt_all=3
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_allnull_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_allnull_mv")

    order_qt_allnull_after_insert_complete """
        SELECT sum_v1, avg_v1, cnt_v1, min_v1, max_v1, cnt_all
        FROM test_ivm_agg_mtmv_allnull_mv
    """

    // Insert another NULL (should not change aggregates)
    sql """INSERT INTO test_ivm_agg_mtmv_allnull_base VALUES (4, NULL, 0);"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_allnull_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_allnull_mv")

    // COMPLETE: k1=1(NULL),2(NULL),3(100),4(NULL)
    // sum=100, avg=100, cnt_v1=1, min=100, max=100, cnt_all=4
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_allnull_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_allnull_mv")

    order_qt_allnull_after_null_insert_complete """
        SELECT sum_v1, avg_v1, cnt_v1, min_v1, max_v1, cnt_all
        FROM test_ivm_agg_mtmv_allnull_mv
    """
}
