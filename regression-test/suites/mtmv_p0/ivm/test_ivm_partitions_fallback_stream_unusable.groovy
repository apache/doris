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

/**
 * What a partition-based refresh does when the MV's base table streams are gone.
 *
 * <p>A partition refresh reads the stream of every base table it realigns from -- the PCT tables of the
 * partitions it rebuilds, and the non-PCT tables the MV joins -- so a missing stream makes it fail,
 * and only the COMPLETE attempt reconciles streams. A request that allows falling back must therefore
 * end up doing COMPLETE rather than failing the task; a request that does not allow it must keep
 * failing, because COMPLETE would refresh more than it was asked to.
 *
 * <p>Driven by dropping the streams by hand: that is the state the fallback exists for and the one a
 * unit test can only mock.
 */
suite("test_ivm_partitions_fallback_stream_unusable") {
    def factTable = "ivm_stream_f"
    def dimTable = "ivm_stream_d"
    def mvName = "ivm_stream_mv"

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
        return taskResult[0].TaskId.toString()
    }

    // Unset IvmFallbackReason comes back as the literal two-character string "\N", which does not
    // survive a .out round trip, so fold it into a printable token.
    def taskOutcome = { String taskId ->
        def rows = sql_return_maparray("""
            SELECT Status,
                   CASE WHEN RefreshMode IN ('COMPLETE', 'PARTIAL', 'NOT_REFRESH')
                        THEN RefreshMode ELSE 'NONE' END AS Mode,
                   CASE WHEN IvmFallbackReason = 'BINLOG_BROKEN' OR IvmFallbackReason = 'STREAM_UNSUPPORTED'
                        THEN IvmFallbackReason ELSE 'NONE' END AS Fallback
            FROM tasks('type'='mv')
            WHERE TaskId = '${taskId}'
        """)
        // toString: a GString never equals a String, and these are compared against literals.
        return "${rows[0].Status}\t${rows[0].Mode}\t${rows[0].Fallback}".toString()
    }

    def taskStatus = { String taskId ->
        def rows = sql_return_maparray("""
            SELECT Status FROM tasks('type'='mv') WHERE TaskId = '${taskId}'
        """)
        return rows[0].Status.toString()
    }

    def streamNames = {
        return sql("SHOW STREAMS FROM ${context.dbName}").collect { it[0].toString() }
    }

    def dropStreams = {
        def names = streamNames()
        assertTrue(!names.isEmpty(), "the MV should have created its streams")
        names.each { name ->
            sql """DROP STREAM ${name} FORCE"""
        }
    }

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
    sql """ALTER TABLE ${factTable} ADD PARTITION p202601 VALUES [('2026-01-01'), ('2026-02-01'))"""
    sql """ALTER TABLE ${factTable} ADD PARTITION p202602 VALUES [('2026-02-01'), ('2026-03-01'))"""

    // Partitioned as well, but joined on a non-partition column: a base table of the MV that is not one
    // of its PCT tables, and therefore read through its stream by a partition refresh.
    sql """
        CREATE TABLE ${dimTable} (
            dimension_id INT NOT NULL,
            dt DATE NOT NULL,
            dimension_name VARCHAR(32)
        )
        UNIQUE KEY(dimension_id, dt)
        PARTITION BY RANGE(dt) ()
        DISTRIBUTED BY HASH(dimension_id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """
    sql """ALTER TABLE ${dimTable} ADD PARTITION d202601 VALUES [('2026-01-01'), ('2026-02-01'))"""
    sql """ALTER TABLE ${dimTable} ADD PARTITION d202602 VALUES [('2026-02-01'), ('2026-03-01'))"""

    sql """INSERT INTO ${dimTable} VALUES (10, '2026-01-15', 'dim-a'), (20, '2026-02-15', 'dim-b')"""
    sql """INSERT INTO ${factTable} VALUES
            (1, '2026-01-10', 10, 100),
            (2, '2026-02-10', 20, 200)"""

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        KEY(order_id, dt)
        PARTITION BY(dt)
        DISTRIBUTED BY HASH(order_id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT f.order_id, f.dt, f.amount, d.dimension_name
           FROM ${factTable} f
           LEFT JOIN ${dimTable} d ON f.dimension_id = d.dimension_id
    """

    sql """REFRESH MATERIALIZED VIEW ${mvName} COMPLETE"""
    def taskId = waitForNewTask(null)
    assertEquals("SUCCESS\tCOMPLETE\tNONE", taskOutcome(taskId))
    order_qt_baseline_mv """SELECT order_id, dt, amount, dimension_name FROM ${mvName}"""

    // With the streams gone, a partition refresh cannot read anything. Falling back is what makes it
    // reach the COMPLETE attempt, which reconciles the streams and rebuilds the MV; without it the
    // refresh would fail and stay failed.
    dropStreams()
    sql """REFRESH MATERIALIZED VIEW ${mvName} PARTITIONS FALLBACK"""
    taskId = waitForNewTask(taskId)
    assertEquals("SUCCESS\tCOMPLETE\tSTREAM_UNSUPPORTED", taskOutcome(taskId))
    assertTrue(!streamNames().isEmpty(), "the complete attempt should have reconciled the streams")
    order_qt_reconciled_mv """SELECT order_id, dt, amount, dimension_name FROM ${mvName}"""

    // The same request without fallback keeps failing: COMPLETE refreshes more than the request asked
    // for, so it may only be reached when the user allowed it. Its failure leaves the MV as it was.
    //
    // The base table has to be out of sync for the partition refresh to do anything at all: an MV that
    // is already up to date refreshes no partition, and therefore never reads a stream.
    sql """INSERT INTO ${factTable} VALUES (3, '2026-01-20', 10, 300)"""
    dropStreams()
    sql """REFRESH MATERIALIZED VIEW ${mvName} PARTITIONS"""
    taskId = waitForNewTask(taskId)
    assertEquals("FAILED", taskStatus(taskId))
    order_qt_after_strict_failure_mv """SELECT order_id, dt, amount, dimension_name FROM ${mvName}"""
}
