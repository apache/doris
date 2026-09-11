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
 * partition_sync_limit and ivm_partition_window_limit together: the base partitions the
 * incremental delta may read are the INTERSECTION of the two.
 *
 * <p>The MV keeps the partitions above the start of the current year (partition_sync_limit=1 with
 * YEAR), which is p_mid and p_new, while p_dropped is filtered out. The compute window keeps the
 * last partition by value, which is p_new alone.
 *
 * <p>So only p_new may be read: p_dropped must stay unread, or the delta would emit rows for a
 * date the MV has no partition for, and p_mid must stay unread too, or the window would be
 * ignored and the MV would be maintained outside it. A dimension change touches one fact row in
 * each partition, so the expectation separates the three: p_new is repaired, p_mid keeps the
 * value the window says not to maintain, p_dropped is not part of the MV at all.
 *
 * <p>All dates are literals and every partition is created by hand: no current_date() and no
 * dynamic partition scheduler. The YEAR unit and the far-future range upper bounds keep the
 * kept/dropped split stable for any run date in this century.
 */
suite("test_ivm_partition_sync_limit_with_window") {
    def factTable = "ivm_pslw_f"
    def dimTable = "ivm_pslw_d"
    def mvName = "ivm_pslw_mv"

    sql """DROP MATERIALIZED VIEW IF EXISTS ${mvName}"""
    sql """DROP TABLE IF EXISTS ${factTable}"""
    sql """DROP TABLE IF EXISTS ${dimTable}"""

    sql """
        CREATE TABLE ${factTable} (
            order_id BIGINT NOT NULL,
            dt DATE NOT NULL,
            dimension_id INT,
            amount INT
        )
        UNIQUE KEY(order_id, dt)
        PARTITION BY RANGE(dt) ()
        DISTRIBUTED BY HASH(order_id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """
    sql """ALTER TABLE ${factTable} ADD PARTITION p_dropped VALUES [('2019-01-01'), ('2020-01-01'))"""
    sql """ALTER TABLE ${factTable} ADD PARTITION p_mid VALUES [('2026-01-01'), ('2099-01-01'))"""
    sql """ALTER TABLE ${factTable} ADD PARTITION p_new VALUES [('2099-01-01'), ('2199-01-01'))"""

    sql """
        CREATE TABLE ${dimTable} (
            dimension_id INT NOT NULL,
            dimension_name VARCHAR(32)
        )
        UNIQUE KEY(dimension_id)
        DISTRIBUTED BY HASH(dimension_id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """

    sql """INSERT INTO ${dimTable} VALUES (10, 'known')"""
    // One fact row per partition, all on dimension key 99, which has no dimension row yet.
    sql """INSERT INTO ${factTable} VALUES
            (1, '2019-06-01', 99, 10),
            (2, '2026-06-15', 99, 20),
            (3, '2099-06-15', 99, 30)"""

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        KEY(order_id, dt)
        PARTITION BY(dt)
        DISTRIBUTED BY HASH(order_id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "partition_sync_limit" = "1",
            "partition_sync_time_unit" = "YEAR",
            "ivm_partition_window_limit" = "${factTable}:1"
        )
        AS SELECT f.order_id, f.dt, f.amount, d.dimension_name
           FROM ${factTable} f
           LEFT JOIN ${dimTable} d ON f.dimension_id = d.dimension_id
    """

    // Waiting through the framework helper rather than by reading the newest row: tasks() can
    // briefly miss a task that just finished, and this suite reuses the MV name across runs, so
    // the newest row can be another run's task. Only the id is taken here; the row the .out
    // compares comes from taskQuery below.
    def refreshAndGetTaskId = { String mode ->
        sql """REFRESH MATERIALIZED VIEW ${mvName} ${mode}"""
        waitingMTMVTaskFinishedByMvName(mvName)
        def rows = sql_return_maparray("""
            SELECT TaskId FROM tasks('type'='mv')
            WHERE MvDatabaseName = '${context.dbName}' AND MvName = '${mvName}'
            ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
        """)
        assert !rows.isEmpty(): "no refresh task for ${mode} on ${mvName}"
        return rows[0].TaskId.toString()
    }

    // Unset RefreshMode / IvmFallbackReason come back as the literal two-character string "\N",
    // which does not survive the .out round trip, so fold the unset value into a printable token.
    def taskQuery = { String taskId ->
        """
            SELECT Status,
                   CASE WHEN RefreshMode IN ('COMPLETE', 'PARTIAL', 'NOT_REFRESH')
                        THEN RefreshMode ELSE 'NONE' END,
                   CASE WHEN IvmFallbackReason = 'BINLOG_BROKEN'
                        THEN IvmFallbackReason ELSE 'NONE' END
            FROM tasks('type'='mv')
            WHERE TaskId = '${taskId}'
        """
    }

    // The MV keeps p_mid and p_new, so order_id 1 is never part of it. The window is an
    // incremental-path property, so the complete refresh still builds both MV partitions.
    sql """REFRESH MATERIALIZED VIEW ${mvName} COMPLETE"""
    def taskId = refreshAndGetTaskId("COMPLETE")
    qt_complete_task taskQuery(taskId)
    order_qt_complete_mv """
        SELECT order_id, amount, dimension_name FROM ${mvName} ORDER BY order_id
    """

    // A late-arriving dimension row for key 99 touches the fact row in every partition.
    sql """INSERT INTO ${dimTable} VALUES (99, 'late-arriving')"""
    taskId = refreshAndGetTaskId("INCREMENTAL")
    qt_incremental_task taskQuery(taskId)
    order_qt_incremental_mv """
        SELECT order_id, amount, dimension_name FROM ${mvName} ORDER BY order_id
    """

    sql """DROP MATERIALIZED VIEW IF EXISTS ${mvName}"""
    sql """DROP TABLE IF EXISTS ${factTable}"""
    sql """DROP TABLE IF EXISTS ${dimTable}"""
}
