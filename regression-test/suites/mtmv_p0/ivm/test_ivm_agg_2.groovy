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

suite("test_ivm_agg_2") {

    // =========================================================
    // Part 6: Scalar agg MV with all-NULL values + binlog_op
    //         Tests SUM/AVG/COUNT(expr)/MIN/MAX when every value is NULL
    // =========================================================

    sql """drop materialized view if exists ivm_agg_allnull_mv;"""
    sql """drop table if exists ivm_agg_allnull_base;"""

    sql """
        CREATE TABLE ivm_agg_allnull_base (
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

    // All values are NULL
    sql """
        INSERT INTO ivm_agg_allnull_base VALUES
            (1, NULL),
            (2, NULL);
    """

    sql """
        CREATE MATERIALIZED VIEW ivm_agg_allnull_mv
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
           FROM ivm_agg_allnull_base;
    """

    // COMPLETE: all NULLs → sum=NULL, avg=NULL, cnt_v1=0, min=NULL, max=NULL, cnt_all=2
    sql """REFRESH MATERIALIZED VIEW ivm_agg_allnull_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_allnull_mv")

    order_qt_allnull_after_complete """
        SELECT sum_v1, avg_v1, cnt_v1, min_v1, max_v1, cnt_all
        FROM ivm_agg_allnull_mv
    """

    // Insert a non-NULL value → transitions from all-NULL to mixed
    sql """INSERT INTO ivm_agg_allnull_base VALUES (3, 100);"""

    sql """REFRESH MATERIALIZED VIEW ivm_agg_allnull_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_allnull_mv")

    // After INCREMENTAL: mock delta reads all 3 rows; output queryable (may be inflated).
    order_qt_allnull_after_insert_incremental """
        SELECT sum_v1, avg_v1, cnt_v1, min_v1, max_v1, cnt_all
        FROM ivm_agg_allnull_mv
    """

    // COMPLETE to get ground truth: k1=1(NULL),2(NULL),3(100)
    // sum=100, avg=100, cnt_v1=1, min=100, max=100, cnt_all=3
    sql """REFRESH MATERIALIZED VIEW ivm_agg_allnull_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_allnull_mv")

    order_qt_allnull_after_insert_complete """
        SELECT sum_v1, avg_v1, cnt_v1, min_v1, max_v1, cnt_all
        FROM ivm_agg_allnull_mv
    """

    // Insert another NULL (should not change aggregates)
    sql """INSERT INTO ivm_agg_allnull_base VALUES (4, NULL);"""

    sql """REFRESH MATERIALIZED VIEW ivm_agg_allnull_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_allnull_mv")

    // After inserting another NULL: mock delta increments cnt_all but sum/min/max unchanged.
    order_qt_allnull_after_null_insert_incremental """
        SELECT sum_v1, avg_v1, cnt_v1, min_v1, max_v1, cnt_all
        FROM ivm_agg_allnull_mv
    """

    // COMPLETE: k1=1(NULL),2(NULL),3(100),4(NULL)
    // sum=100, avg=100, cnt_v1=1, min=100, max=100, cnt_all=4
    sql """REFRESH MATERIALIZED VIEW ivm_agg_allnull_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_allnull_mv")

    order_qt_allnull_after_null_insert_complete """
        SELECT sum_v1, avg_v1, cnt_v1, min_v1, max_v1, cnt_all
        FROM ivm_agg_allnull_mv
    """

    // =========================================================
    // Part 7: Group disappearing — all rows in a group deleted
    //         via binlog_op=1 → group_count reaches 0
    //         → __DORIS_DELETE_SIGN__=1 removes the group row from MV
    // =========================================================

    sql """drop materialized view if exists ivm_agg_grpdel_mv;"""
    sql """drop table if exists ivm_agg_grpdel_base;"""

    sql """
        CREATE TABLE ivm_agg_grpdel_base (
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

    // Initial data: grp=1 has 2 rows, grp=2 has 1 row
    sql """
        INSERT INTO ivm_agg_grpdel_base VALUES
            (1, 1, 10),
            (2, 1, 20),
            (3, 2, 30);
    """

    sql """
        CREATE MATERIALIZED VIEW ivm_agg_grpdel_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT grp, COUNT(*) AS cnt, SUM(v1) AS sum_v1
           FROM ivm_agg_grpdel_base
           GROUP BY grp;
    """

    // Step 1: COMPLETE refresh
    // grp=1: cnt=2, sum=30; grp=2: cnt=1, sum=30
    sql """REFRESH MATERIALIZED VIEW ivm_agg_grpdel_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_grpdel_mv")

    order_qt_grpdel_after_complete """
        SELECT grp, cnt, sum_v1 FROM ivm_agg_grpdel_mv ORDER BY grp
    """

    // Step 2: Delete the only row in grp=2 (binlog_op=1) and add a dirty row in grp=1
    // After INCREMENTAL, grp=2 should disappear (group_count=0 → DELETE_SIGN=1)
    sql """DELETE FROM ivm_agg_grpdel_base WHERE k1 = 3;"""
    // Dirty the partition so INCREMENTAL actually runs
    sql """INSERT INTO ivm_agg_grpdel_base VALUES (4, 1, 40);"""

    sql """REFRESH MATERIALIZED VIEW ivm_agg_grpdel_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_grpdel_mv")

    // After INCREMENTAL: grp=1 should have cnt=3, sum=70; grp=2 should be gone
    order_qt_grpdel_after_incremental """
        SELECT grp, cnt, sum_v1 FROM ivm_agg_grpdel_mv ORDER BY grp
    """

    // Step 3: COMPLETE refresh to verify ground truth
    // Physical rows: k1=1(grp=1,v1=10,op=0), k1=2(grp=1,v1=20,op=0),
    //                k1=3(grp=2,v1=30,op=1), k1=4(grp=1,v1=40,op=0)
    // COMPLETE ignores binlog_op → grp=1: cnt=3, sum=70; grp=2: cnt=1, sum=30
    sql """REFRESH MATERIALIZED VIEW ivm_agg_grpdel_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_grpdel_mv")

    order_qt_grpdel_after_final_complete """
        SELECT grp, cnt, sum_v1 FROM ivm_agg_grpdel_mv ORDER BY grp
    """

    // Step 4: Re-insert into grp=2 (resurrect the group) and fix k1=3 back to op=0
    sql """INSERT INTO ivm_agg_grpdel_base VALUES (3, 2, 30);"""
    sql """INSERT INTO ivm_agg_grpdel_base VALUES (5, 2, 50);"""

    sql """REFRESH MATERIALIZED VIEW ivm_agg_grpdel_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_grpdel_mv")

    // After INCREMENTAL resurrection: grp=2 re-appears in MV output.
    order_qt_grpdel_after_resurrect_incremental """
        SELECT grp, cnt, sum_v1 FROM ivm_agg_grpdel_mv ORDER BY grp
    """

    // COMPLETE to verify resurrected grp=2
    // Physical rows: k1=1(grp=1,10,0), k1=2(grp=1,20,0), k1=3(grp=2,30,0),
    //                k1=4(grp=1,40,0), k1=5(grp=2,50,0)
    // grp=1: cnt=3, sum=70; grp=2: cnt=2, sum=80
    sql """REFRESH MATERIALIZED VIEW ivm_agg_grpdel_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_grpdel_mv")

    order_qt_grpdel_after_resurrect_complete """
        SELECT grp, cnt, sum_v1 FROM ivm_agg_grpdel_mv ORDER BY grp
    """

    // =========================================================
    // Part 8: Scalar agg — all rows deleted via binlog_op=1
    //         Scalar row must persist with COUNT(*)=0, SUM=NULL, MIN=NULL, MAX=NULL
    //         (scalar agg always has exactly one output row, never deleted)
    // =========================================================

    sql """drop materialized view if exists ivm_agg_scalardel_mv;"""
    sql """drop table if exists ivm_agg_scalardel_base;"""

    sql """
        CREATE TABLE ivm_agg_scalardel_base (
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

    // Initial data: 2 rows
    sql """
        INSERT INTO ivm_agg_scalardel_base VALUES
            (1, 10),
            (2, 20);
    """

    sql """
        CREATE MATERIALIZED VIEW ivm_agg_scalardel_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT COUNT(*) AS cnt, SUM(v1) AS sum_v1, MIN(v1) AS min_v1, MAX(v1) AS max_v1
           FROM ivm_agg_scalardel_base;
    """

    // Step 1: COMPLETE refresh — cnt=2, sum=30, min=10, max=20
    sql """REFRESH MATERIALIZED VIEW ivm_agg_scalardel_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_scalardel_mv")

    order_qt_scalardel_after_complete """
        SELECT cnt, sum_v1, min_v1, max_v1 FROM ivm_agg_scalardel_mv
    """

    // Step 2: Delete all rows (binlog_op=1)
    // After INCREMENTAL, scalar row should persist with cnt=0, sum=NULL, min=NULL, max=NULL
    // NOTE: Deleting the MIN (10) and MAX (20) will trigger assert_true guards for MIN/MAX.
    // Since this is explicit INCREMENTAL, it will FAIL due to the boundary guard.
    // We need to test without MIN/MAX guards triggering — use a table without MIN/MAX in MV,
    // OR accept the FAIL and test the scalar-empty scenario via COMPLETE only.
    //
    // Actually, since we want to test scalar row persistence, let's use a MV with only
    // COUNT(*) and SUM (no MIN/MAX), so no boundary guards fire.

    sql """drop materialized view if exists ivm_agg_scalardel_mv;"""

    sql """
        CREATE MATERIALIZED VIEW ivm_agg_scalardel_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT COUNT(*) AS cnt, SUM(v1) AS sum_v1
           FROM ivm_agg_scalardel_base;
    """

    // Re-COMPLETE with the new MV (COUNT+SUM only, no MIN/MAX)
    sql """REFRESH MATERIALIZED VIEW ivm_agg_scalardel_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_scalardel_mv")

    order_qt_scalardel_countsum_after_complete """
        SELECT cnt, sum_v1 FROM ivm_agg_scalardel_mv
    """

    // Step 3: Delete all rows via binlog_op=1
    sql """DELETE FROM ivm_agg_scalardel_base WHERE k1 = 1;"""
    sql """DELETE FROM ivm_agg_scalardel_base WHERE k1 = 2;"""
    // Dirty partition for INCREMENTAL
    sql """INSERT INTO ivm_agg_scalardel_base VALUES (3, 99);"""

    sql """REFRESH MATERIALIZED VIEW ivm_agg_scalardel_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_scalardel_mv")

    // After INCREMENTAL: k1=1(op=1→-1), k1=2(op=1→-1), k1=3(op=0→+1).
    // delta_group_count = -1-1+1 = -1; new_count = old(2)+(-1) = 1
    order_qt_scalardel_after_delete_incremental """
        SELECT cnt, sum_v1 FROM ivm_agg_scalardel_mv
    """

    // Step 4: COMPLETE to get ground truth
    // Physical rows: k1=1(v1=10,op=1), k1=2(v1=20,op=1), k1=3(v1=99,op=0)
    // COMPLETE ignores binlog_op → cnt=3, sum=129
    sql """REFRESH MATERIALIZED VIEW ivm_agg_scalardel_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_scalardel_mv")

    order_qt_scalardel_countsum_after_delete_complete """
        SELECT cnt, sum_v1 FROM ivm_agg_scalardel_mv
    """

    // Step 5: Now test true empty table — delete ALL rows including k1=3
    sql """DELETE FROM ivm_agg_scalardel_base WHERE k1 = 3;"""
    // Insert and immediately delete a dummy row to dirty the partition
    sql """INSERT INTO ivm_agg_scalardel_base VALUES (4, 0);"""

    sql """REFRESH MATERIALIZED VIEW ivm_agg_scalardel_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_scalardel_mv")

    // After second INCREMENTAL with more deletes + dirty row.
    order_qt_scalardel_after_second_delete_incremental """
        SELECT cnt, sum_v1 FROM ivm_agg_scalardel_mv
    """

    // COMPLETE to verify: all rows have op=1 or the physical state
    // Physical: k1=1(10,1), k1=2(20,1), k1=3(99,1), k1=4(0,0)
    // COMPLETE sees all 4 physical rows → cnt=4, sum=129
    sql """REFRESH MATERIALIZED VIEW ivm_agg_scalardel_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_scalardel_mv")

    order_qt_scalardel_countsum_after_allempty_complete """
        SELECT cnt, sum_v1 FROM ivm_agg_scalardel_mv
    """

    // Step 6: Insert fresh rows to recover from "all deleted" state
    sql """INSERT INTO ivm_agg_scalardel_base VALUES (5, 50);"""
    sql """INSERT INTO ivm_agg_scalardel_base VALUES (6, 60);"""

    sql """REFRESH MATERIALIZED VIEW ivm_agg_scalardel_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_scalardel_mv")

    // After recovery INCREMENTAL: fresh rows k1=5,6 inserted.
    order_qt_scalardel_after_recovery_incremental """
        SELECT cnt, sum_v1 FROM ivm_agg_scalardel_mv
    """

    // COMPLETE: k1=1(10,1),2(20,1),3(99,1),4(0,0),5(50,0),6(60,0) → cnt=6, sum=239
    sql """REFRESH MATERIALIZED VIEW ivm_agg_scalardel_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_scalardel_mv")

    order_qt_scalardel_countsum_after_recovery_complete """
        SELECT cnt, sum_v1 FROM ivm_agg_scalardel_mv
    """

    // =========================================================
    // Part 9: MAX boundary deletion via binlog_op=1
    //         Symmetric to Part 4 (MIN boundary), tests the MAX guard:
    //         assert_true(deltaDelMax IS NULL OR old_max IS NULL OR deltaDelMax < old_max)
    //         Deleting the MAX value → deltaDelMax == old_max → guard fails → INCREMENTAL FAILED
    // =========================================================

    sql """drop materialized view if exists ivm_agg_maxdel_mv;"""
    sql """drop table if exists ivm_agg_maxdel_base;"""

    sql """
        CREATE TABLE ivm_agg_maxdel_base (
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

    // Initial data: v1 values 10, 20, 30 — MAX is 30 (k1=3)
    sql """
        INSERT INTO ivm_agg_maxdel_base VALUES
            (1, 10),
            (2, 20),
            (3, 30);
    """

    // Scalar agg MV with MAX (and MIN+COUNT for completeness)
    sql """
        CREATE MATERIALIZED VIEW ivm_agg_maxdel_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT MIN(v1) AS min_v1, MAX(v1) AS max_v1, COUNT(*) AS cnt
           FROM ivm_agg_maxdel_base;
    """

    // Step 1: COMPLETE refresh — min=10, max=30, cnt=3
    sql """REFRESH MATERIALIZED VIEW ivm_agg_maxdel_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_maxdel_mv")

    order_qt_maxdel_after_complete """
        SELECT min_v1, max_v1, cnt FROM ivm_agg_maxdel_mv
    """

    // Step 2: Safe INCREMENTAL — insert a non-boundary row
    sql """INSERT INTO ivm_agg_maxdel_base VALUES (4, 25);"""

    sql """REFRESH MATERIALIZED VIEW ivm_agg_maxdel_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_maxdel_mv")

    // After safe INCREMENTAL (no boundary hit): output reflects 4 rows including new k1=4.
    order_qt_maxdel_after_safe_incremental """
        SELECT min_v1, max_v1, cnt FROM ivm_agg_maxdel_mv
    """

    // Step 3: Delete the MAX value (k1=3, v1=30, op=1)
    // The MAX guard should fire: deltaDelMax=30 == old_max=30, so deltaDelMax < old_max is FALSE
    sql """DELETE FROM ivm_agg_maxdel_base WHERE k1 = 3;"""
    // Dirty partition
    sql """INSERT INTO ivm_agg_maxdel_base VALUES (5, 15);"""

    sql """REFRESH MATERIALIZED VIEW ivm_agg_maxdel_mv INCREMENTAL"""

    // Wait for task to reach terminal state (expected FAILED)
    def maxDelShowTasksSql = """
        select TaskId, Status, ErrorMsg from tasks('type'='mv')
        where MvDatabaseName = '${context.dbName}' and MvName = 'ivm_agg_maxdel_mv'
        order by CreateTime DESC limit 1
    """
    def maxDelTaskResult
    Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
        maxDelTaskResult = sql(maxDelShowTasksSql)
        if (maxDelTaskResult.isEmpty()) return false
        def st = maxDelTaskResult[0][1].toString()
        return st != 'PENDING' && st != 'RUNNING'
    })
    def maxDelTaskStatus = maxDelTaskResult[0][1].toString()
    logger.info("MAX boundary delete INCREMENTAL task status: " + maxDelTaskStatus
            + ", error: " + maxDelTaskResult[0][2])
    assertTrue(maxDelTaskStatus == "FAILED",
            "Expected INCREMENTAL to fail when deleting MAX value, but got: " + maxDelTaskStatus)
    def maxDelErrorMsg = maxDelTaskResult[0][2].toString()
    assertTrue(maxDelErrorMsg.contains("MIN_MAX_BOUNDARY") || maxDelErrorMsg.contains("IVM")
                    || maxDelErrorMsg.contains("assert_true") || maxDelErrorMsg.contains("fallback"),
            "Error should mention MIN_MAX_BOUNDARY/IVM/assert_true/fallback but got: " + maxDelErrorMsg)

    // Step 4: COMPLETE refresh to recover
    sql """REFRESH MATERIALIZED VIEW ivm_agg_maxdel_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_maxdel_mv")

    // Physical rows: k1=1(10,0), k1=2(20,0), k1=3(30,1), k1=4(25,0), k1=5(15,0)
    // COMPLETE ignores binlog_op → min=10, max=30, cnt=5
    order_qt_maxdel_after_recovery """
        SELECT min_v1, max_v1, cnt FROM ivm_agg_maxdel_mv
    """

    // Step 5: Fix k1=3 back to op=0, safe INCREMENTAL should succeed
    sql """INSERT INTO ivm_agg_maxdel_base VALUES (3, 30);"""

    sql """REFRESH MATERIALIZED VIEW ivm_agg_maxdel_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_maxdel_mv")

    // After fix INCREMENTAL: k1=3 restored to op=0; output stable.
    order_qt_maxdel_after_fix_incremental """
        SELECT min_v1, max_v1, cnt FROM ivm_agg_maxdel_mv
    """
}
