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

suite("test_ivm_agg_previous_commit_not_visible", "nonConcurrent") {
    // The finish-transaction debug point only fires on the local (shared-nothing) txn
    // path; the cloud transaction manager never hits it.
    if (isCloudMode()) {
        logger.info("skip test_ivm_agg_previous_commit_not_visible on cloud mode: " +
                "finishTransaction debug point only fires on the local txn path")
        return
    }
    def blockVisible = "DatabaseTransactionMgr.finishTransaction.block_visible"
    def mvName = "ivm_prev_commit_mv"
    def baseName = "ivm_prev_commit_t"

    def disableDebugPoints = {
        GetDebugPoint().disableDebugPointForAllFEs(blockVisible)
    }
    def enableBlockMvVisible = {
        GetDebugPoint().enableDebugPointForAllFEs(blockVisible, [value: mvName])
    }

    def latestTask = {
        // Let the newly submitted refresh task row appear so the poll below cannot
        // latch onto the previous (already terminal) task of the same MV.
        Thread.sleep(2000)
        def taskResult
        Awaitility.await().atMost(180, SECONDS).pollInterval(2, SECONDS).until({
            taskResult = sql_return_maparray("""
                SELECT Status, RefreshMode, IvmFallbackReason, ErrorMsg
                FROM tasks('type'='mv')
                WHERE MvDatabaseName = '${context.dbName}'
                  AND MvName = '${mvName}'
                ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
            """)
            return !taskResult.isEmpty()
                    && taskResult[0].Status.toString() != 'PENDING'
                    && taskResult[0].Status.toString() != 'RUNNING'
        })
        return taskResult[0]
    }

    try {
        disableDebugPoints()
        sql """drop materialized view if exists ${mvName}"""
        sql """drop table if exists ${baseName}"""

        sql """
            CREATE TABLE ${baseName} (
                k1 INT,
                v1 INT
            )
            UNIQUE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "binlog.enable" = "true",
                "binlog.format" = "ROW",
                "binlog.need_historical_value" = "true",
                "enable_unique_key_merge_on_write" = "true"
            )
        """
        sql """INSERT INTO ${baseName} VALUES (1, 10), (2, 20)"""

        sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES ('replication_num' = '1')
            AS SELECT k1, COUNT(*) AS cnt, SUM(v1) AS sum_v1
               FROM ${baseName} GROUP BY k1
        """

        // Initial INCREMENTAL refresh (no debug point): consumes the historical binlog
        // and establishes the baseline snapshot.
        sql """REFRESH MATERIALIZED VIEW ${mvName} INCREMENTAL"""
        waitingMTMVTaskFinishedByMvName(mvName)
        order_qt_prev_commit_baseline """
            SELECT k1, cnt, sum_v1 FROM ${mvName} ORDER BY k1
        """

        // Batch B is visible before the debug point blocks the next MV refresh.
        sql """INSERT INTO ${baseName} VALUES (3, 30)"""
        enableBlockMvVisible()

        // R1: the delta txn commits but its finish (VISIBLE) is blocked by the debug
        // point, so the refresh txn stays COMMITTED. The insert times out waiting for
        // publish and the task still reports SUCCESS (committed mode). The refresh task
        // runs in an internal ConnectContext that clones the global session variables,
        // so shorten insert_visible_timeout_ms globally for the wait.
        setGlobalVarTemporary([insert_visible_timeout_ms: 3000], {
            sql """REFRESH MATERIALIZED VIEW ${mvName} INCREMENTAL"""
            waitingMTMVTaskFinishedByMvName(mvName)
        })
        order_qt_prev_commit_after_blocked_refresh """
            SELECT k1, cnt, sum_v1 FROM ${mvName} ORDER BY k1
        """

        // EXPLAIN REFRESH only produces a plan (no execution and no MV data read), so it
        // must still succeed while the previous refresh txn is committed but not visible;
        // this holds for INCREMENTAL and COMPLETE alike.
        sql """EXPLAIN REFRESH MATERIALIZED VIEW ${mvName} INCREMENTAL"""
        sql """EXPLAIN REFRESH MATERIALIZED VIEW ${mvName} COMPLETE"""

        // Batch C commits and publishes normally (the debug point only matches the MV),
        // so the next incremental refresh has a real delta while the previous refresh
        // txn on the MV is still not visible. The aggregate delta would join stale old
        // MV rows, so the refresh must fail instead of corrupting the MV.
        sql """INSERT INTO ${baseName} VALUES (4, 40)"""
        sql """REFRESH MATERIALIZED VIEW ${mvName} INCREMENTAL"""
        def failedTask = latestTask()
        assertEquals("FAILED", failedTask.Status.toString())
        assertTrue(failedTask.ErrorMsg.toString().contains("MV_COMMIT_NOT_VISIBLE"))
        assertTrue(failedTask.ErrorMsg.toString().contains("not visible yet"))
        order_qt_prev_commit_unchanged_after_failed_refresh """
            SELECT k1, cnt, sum_v1 FROM ${mvName} ORDER BY k1
        """

        // A COMPLETE refresh recomputes from the base tables and never joins old MV rows,
        // so it is not stopped by the guard. But the debug point is still on, so its own
        // write txn can not turn VISIBLE either: the task reports SUCCESS (committed
        // mode) while readers still see none of the new data.
        setGlobalVarTemporary([insert_visible_timeout_ms: 3000], {
            sql """REFRESH MATERIALIZED VIEW ${mvName} COMPLETE"""
            waitingMTMVTaskFinishedByMvName(mvName)
        })
        def completeStillInvisibleRows = sql """SELECT COUNT(*) FROM ${mvName} WHERE k1 >= 3"""
        assertEquals("0", completeStillInvisibleRows.get(0).get(0).toString())

        // The COMPLETE refresh above reported SUCCESS while its txn is still COMMITTED,
        // so a subsequent strict incremental with a real delta is refused by the guard
        // just like after a stuck incremental: readers must never see MV state computed
        // on top of rows that are committed but not visible.
        sql """INSERT INTO ${baseName} VALUES (5, 50)"""
        sql """REFRESH MATERIALIZED VIEW ${mvName} INCREMENTAL"""
        def failedAfterCompleteTask = latestTask()
        assertEquals("FAILED", failedAfterCompleteTask.Status.toString())
        assertTrue(failedAfterCompleteTask.ErrorMsg.toString().contains("MV_COMMIT_NOT_VISIBLE"))
        def stillInvisibleRows = sql """SELECT COUNT(*) FROM ${mvName} WHERE k1 >= 3"""
        assertEquals("0", stillInvisibleRows.get(0).get(0).toString())

        // Drop the debug point: the committed-but-unpublished txns auto-publish (their
        // rowsets are already on the BE, only the FE finish was held back), the MV
        // partition commit and visible versions converge, and reads now return the full
        // aggregate — the deferred failure lost nothing.
        disableDebugPoints()

        // The stuck txns auto-publish (their rowsets are already on the BE, only the FE
        // finish was held back), so the MV partition commit and visible versions converge
        // and readers see the COMPLETE content (base rows through (4,40)).
        Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until {
            sql("SELECT COUNT(*) FROM ${mvName}").get(0).get(0).toString() == "4"
        }
        order_qt_prev_commit_converged_after_publish """
            SELECT k1, cnt, sum_v1 FROM ${mvName} ORDER BY k1
        """

        // (5,50) was inserted after the stuck COMPLETE, so it needs one more successful
        // INCREMENTAL refresh; the guard is clear now that the stuck txns have published.
        sql """REFRESH MATERIALIZED VIEW ${mvName} INCREMENTAL"""
        waitingMTMVTaskFinishedByMvName(mvName)
        order_qt_prev_commit_converged """
            SELECT k1, cnt, sum_v1 FROM ${mvName} ORDER BY k1
        """
    } finally {
        disableDebugPoints()
        sql """drop materialized view if exists ${mvName}"""
        sql """drop table if exists ${baseName}"""
    }
}
