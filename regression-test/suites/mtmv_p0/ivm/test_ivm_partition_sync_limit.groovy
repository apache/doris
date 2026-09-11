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
 * An MV that keeps only part of the base table's partitions must not break the incremental
 * refresh when a dimension change touches base rows that live outside that window.
 *
 * <p>The MV below is partitioned by the fact table's dt and keeps only partitions whose range
 * upper bound is greater than the start of the current year (partition_sync_limit=1 with
 * partition_sync_time_unit=YEAR), so p_dropped is filtered out of the MV while p_kept stays.
 * Its partition set is therefore a strict subset of the base table's.
 *
 * <p>A late-arriving dimension row for key 99 produces a delta that joins the dimension events
 * against the fact snapshot; the snapshot covers every fact partition, so it also matches the
 * fact row stored in p_dropped and emits a delta row for a date the MV has no partition for.
 * The insert then fails with "no partition for this tuple", and because the write is atomic the
 * in-window repair is lost as well. The refresh must instead ignore the parts of the delta that
 * fall outside the MV's partition set and still repair the partition it does keep.
 *
 * <p>All dates are literals and every partition is created by hand: no current_date() and no
 * dynamic partition scheduler, so the expectation does not depend on the run date. Only the
 * YEAR unit is used to keep the kept/dropped split stable for any run date in this century.
 */
suite("test_ivm_partition_sync_limit") {
    def factTable = "ivm_pwld_f"
    def dimTable = "ivm_pwld_d"
    def mvName = "ivm_pwld_mv"

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
    // Empty PARTITION BY range plus manual ADD PARTITION: the partition layout does not depend
    // on the date the suite runs.
    sql """ALTER TABLE ${factTable} ADD PARTITION p_dropped VALUES [('2019-01-01'), ('2020-01-01'))"""
    sql """ALTER TABLE ${factTable} ADD PARTITION p_kept VALUES [('2026-01-01'), ('2099-01-01'))"""

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
    // order_id 1 and 2 share dimension key 99, which has no dimension row yet: order_id 1 sits in
    // the partition the MV does not keep, order_id 2 in the one it does.
    sql """INSERT INTO ${factTable} VALUES
            (1, '2019-06-01', 99, 10),
            (2, '2026-06-15', 99, 20),
            (3, '2026-06-16', 10, 30)"""

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        KEY(order_id, dt)
        PARTITION BY(dt)
        DISTRIBUTED BY HASH(order_id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "partition_sync_limit" = "1",
            "partition_sync_time_unit" = "YEAR"
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

    // The MV keeps p_kept only, so order_id 1 is never part of it.
    def taskId = refreshAndGetTaskId("COMPLETE")
    qt_complete_task taskQuery(taskId)
    order_qt_complete_mv """
        SELECT order_id, amount, dimension_name FROM ${mvName} ORDER BY order_id
    """

    // A late-arriving dimension row for key 99. Its delta must repair order_id 2 (in p_kept) and
    // must not try to write order_id 1 into a partition the MV does not have.
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
