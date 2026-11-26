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

suite("test_ivm_cloud_partial_snapshot_version", "nonConcurrent") {
    // The failure case requires the Cloud FE version cache to lag behind Meta Service.
    if (!isCloudMode()) {
        return
    }
    sql "SET enable_materialized_view_rewrite = false"
    sql "SET enable_dml_materialized_view_rewrite = false"
    sql "SET enable_sql_cache = false"
    sql "SET cloud_partition_version_cache_ttl_ms = 3600000"

    def mvName = "ivm_cloud_partial_snapshot_mv"
    def forcedFallbackDebugPoint = "IvmIncrRefreshManager.doRefresh.force_fallback_reason"
    // The failure the strict path stops on: a reason strict mode does not fall back for.
    def forcedReason = "CHANGELOG_BROKEN"

    def waitForNewTask = { previousTaskId ->
        def taskResult
        Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
            taskResult = sql_return_maparray("""
                SELECT TaskId, Status
                FROM tasks('type'='mv')
                WHERE MvDatabaseName = '${context.dbName}'
                  AND MvName = '${mvName}'
                ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
            """)
            return !taskResult.isEmpty()
                    && taskResult[0].TaskId.toString() != previousTaskId
                    && taskResult[0].Status.toString() != 'PENDING'
                    && taskResult[0].Status.toString() != 'RUNNING'
        })
        return taskResult[0]
    }

    def taskOutcome = { taskId ->
        "SELECT Status, IvmRebuiltPartitions FROM tasks('type'='mv') WHERE TaskId = '${taskId}'"
    }

    setFeConfigTemporary([cloud_enable_version_syncer: false]) {
        sql """DROP MATERIALIZED VIEW IF EXISTS ivm_cloud_partial_snapshot_mv"""
        sql """DROP TABLE IF EXISTS ivm_cloud_partial_snapshot_f"""
        sql """DROP TABLE IF EXISTS ivm_cloud_partial_snapshot_d"""

        sql """
            CREATE TABLE ivm_cloud_partial_snapshot_f (
                k BIGINT NOT NULL,
                dt DATE NOT NULL,
                amount INT
            )
            UNIQUE KEY(k, dt)
            PARTITION BY RANGE(dt) ()
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "changelog.enable" = "true",
                "changelog.need_historical_value" = "true"
            )
        """
        sql """ALTER TABLE ivm_cloud_partial_snapshot_f ADD PARTITION p202601 VALUES [('2026-01-01'), ('2026-02-01'))"""
        sql """ALTER TABLE ivm_cloud_partial_snapshot_f ADD PARTITION p202602 VALUES [('2026-02-01'), ('2026-03-01'))"""

        sql """
            CREATE TABLE ivm_cloud_partial_snapshot_d (
                k BIGINT NOT NULL,
                v INT
            )
            UNIQUE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "changelog.enable" = "true",
                "changelog.need_historical_value" = "true"
            )
        """

        sql """INSERT INTO ivm_cloud_partial_snapshot_f VALUES
                (1, '2026-01-10', 100),
                (2, '2026-02-10', 200)"""

        sql """
            CREATE MATERIALIZED VIEW ivm_cloud_partial_snapshot_mv
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            KEY(k, dt)
            PARTITION BY(dt)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
            AS SELECT f.k, f.dt, f.amount, d.v
               FROM ivm_cloud_partial_snapshot_f f JOIN ivm_cloud_partial_snapshot_d d ON f.k = d.k
        """

        sql """REFRESH MATERIALIZED VIEW ivm_cloud_partial_snapshot_mv COMPLETE"""
        def task = waitForNewTask(null)
        order_qt_initial_refresh taskOutcome(task.TaskId)

        def partitionVersion = {
            sql_return_maparray("SHOW PARTITIONS FROM ivm_cloud_partial_snapshot_d").first().VisibleVersion
        }
        def dimensionCaughtUp = """
            SELECT LAG = '0' FROM information_schema.table_stream_consumption
            WHERE DB_NAME = '${context.dbName}' AND UNIT = 'ivm_cloud_partial_snapshot_d'
        """
        order_qt_empty_dimension_version "SELECT ${partitionVersion()}"

        // Replacing a fact partition forces a rebuild. The dimension's first write is visible
        // to Meta Service, but the FE cache still says empty and the Stream has no consumed baseline.
        sql "TRUNCATE TABLE ivm_cloud_partial_snapshot_f PARTITION(p202601)"
        sql "INSERT INTO ivm_cloud_partial_snapshot_f VALUES (1, '2026-01-10', 100)"
        def skipCacheUpdate = "FE.CloudGlobalTransactionMgr.updateVersion.disabled"
        try {
            GetDebugPoint().enableDebugPointForAllFEs(skipCacheUpdate)
            sql "INSERT INTO ivm_cloud_partial_snapshot_d VALUES (1, 20), (2, 20)"
        } finally {
            GetDebugPoint().disableDebugPointForAllFEs(skipCacheUpdate)
        }
        order_qt_stale_dimension_before_rebuild "SELECT ${partitionVersion()}"
        order_qt_stream_pending dimensionCaughtUp

        try {
            GetDebugPoint().enableDebugPointForAllFEs(forcedFallbackDebugPoint, [reason: forcedReason])
            sql "REFRESH MATERIALIZED VIEW ivm_cloud_partial_snapshot_mv INCREMENTAL"
            task = waitForNewTask(task.TaskId.toString())
        } finally {
            GetDebugPoint().disableDebugPointForAllFEs(forcedFallbackDebugPoint)
        }
        order_qt_failed_delta taskOutcome(task.TaskId)
        order_qt_rows_before_catch_up "SELECT k, dt, amount, v FROM ivm_cloud_partial_snapshot_mv"
        order_qt_stream_still_pending dimensionCaughtUp
        order_qt_stale_dimension_after_rebuild "SELECT ${partitionVersion()}"

        sql "REFRESH MATERIALIZED VIEW ivm_cloud_partial_snapshot_mv INCREMENTAL"
        task = waitForNewTask(task.TaskId.toString())
        order_qt_retry taskOutcome(task.TaskId)
        order_qt_rows_after_catch_up "SELECT k, dt, amount, v FROM ivm_cloud_partial_snapshot_mv"
        order_qt_stream_caught_up dimensionCaughtUp

        sql "REFRESH MATERIALIZED VIEW ivm_cloud_partial_snapshot_mv INCREMENTAL"
        task = waitForNewTask(task.TaskId.toString())
        order_qt_follow_up taskOutcome(task.TaskId)
        sql "SET cloud_partition_version_cache_ttl_ms = 0"
        order_qt_ms_version "SELECT ${partitionVersion()}"
    }
}
