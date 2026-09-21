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
 * Which streams decide that a refresh needs the complete attempt.
 *
 * <p>An IVM MV is created with a stream for every base table in its relation, and for a chained MV the
 * relation carries more than the plan reads: mv2 reads mv1, while the table behind mv1 is in mv2's
 * relation only through the closure. Both rewrites that read streams -- the incremental delta rewriter
 * and the full refresh -- take the streams of the plan's scans, so mv2's stream on that table is never
 * read, and its absence must not turn mv2's incremental refresh into a rebuild of the whole MV.
 *
 * <p>Pinned here:
 * <ol>
 *   <li>the unused stream exists, so the setup is the one the case is about;</li>
 *   <li>an incremental refresh of the upstream MV succeeds with it dropped, which is what makes the
 *       refresh below able to stay incremental;</li>
 *   <li>the downstream MV still refreshes incrementally rather than falling back to a complete
 *       refresh for a stream nothing reads.</li>
 * </ol>
 */
suite("test_ivm_chained_stream_scope") {
    def baseTable = "ivm_chained_scope_base"
    def upstreamMv = "ivm_chained_scope_up"
    def downstreamMv = "ivm_chained_scope_down"

    def waitForNewTask = { String mvName, String previousTaskId ->
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

    def taskRecord = { String taskId ->
        // Unset RefreshMode comes back as the literal two-character string "\N", which does not survive
        // the .out round trip, so fold the unset value into a printable token.
        return sql_return_maparray("""
            SELECT Status,
                   CASE WHEN RefreshMode IN ('COMPLETE', 'PARTIAL', 'NOT_REFRESH')
                        THEN RefreshMode ELSE 'NONE' END AS RefreshMode
            FROM tasks('type'='mv')
            WHERE TaskId = '${taskId}'
        """)[0]
    }

    def mvId = { String mvName ->
        def rows = sql_return_maparray("""
            SELECT Id FROM mv_infos('database'='${context.dbName}') WHERE Name = '${mvName}'
        """)
        return rows[0].Id.toString()
    }

    // Streams are named after the MV that owns them, so the ones of the downstream MV on the upstream's
    // base table can be told apart from the ones the upstream MV has on it.
    def streamNamesOf = { String mvName, String baseTableName ->
        def rows = sql_return_maparray("""
            SELECT STREAM_NAME FROM information_schema.table_streams
            WHERE DB_NAME = '${context.dbName}' AND BASE_TABLE_NAME = '${baseTableName}'
        """)
        def prefix = "__doris_ivm_stream_${mvId(mvName)}_"
        return rows.collect { it.STREAM_NAME.toString() }.findAll { it.startsWith(prefix) }
    }

    sql """DROP MATERIALIZED VIEW IF EXISTS ${downstreamMv}"""
    sql """DROP MATERIALIZED VIEW IF EXISTS ${upstreamMv}"""
    sql """DROP TABLE IF EXISTS ${baseTable}"""

    sql """
        CREATE TABLE ${baseTable} (
            k1 INT NOT NULL,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """
    sql """INSERT INTO ${baseTable} VALUES (1, 10), (2, 20)"""

    // The upstream MV carries row binlog, which is what the downstream MV reads its changes from.
    sql """
        CREATE MATERIALIZED VIEW ${upstreamMv}
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
        AS SELECT * FROM ${baseTable}
    """
    sql """REFRESH MATERIALIZED VIEW ${upstreamMv} COMPLETE"""
    def upstreamTaskId = waitForNewTask(upstreamMv, null)

    sql """
        CREATE MATERIALIZED VIEW ${downstreamMv}
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT k1, v1 FROM ${upstreamMv}
    """
    sql """REFRESH MATERIALIZED VIEW ${downstreamMv} COMPLETE"""
    def downstreamTaskId = waitForNewTask(downstreamMv, null)

    // The downstream MV's plan scans the upstream MV only, but its relation also carries the upstream's
    // base table, and the MV was created with a stream for it.
    def unusedStreams = streamNamesOf(downstreamMv, baseTable)
    assertEquals(1, unusedStreams.size())
    sql """DROP STREAM ${context.dbName}.${unusedStreams[0]} FORCE"""
    assertEquals(0, streamNamesOf(downstreamMv, baseTable).size())

    // One change reaches the downstream MV through the upstream MV, so the refresh below has something
    // to apply and cannot pass as an empty incremental refresh.
    sql """INSERT INTO ${baseTable} VALUES (3, 30)"""
    sql """REFRESH MATERIALIZED VIEW ${upstreamMv} INCREMENTAL"""
    upstreamTaskId = waitForNewTask(upstreamMv, upstreamTaskId)
    assertEquals("SUCCESS", taskRecord(upstreamTaskId).Status.toString())

    sql """REFRESH MATERIALIZED VIEW ${downstreamMv} AUTO"""
    downstreamTaskId = waitForNewTask(downstreamMv, downstreamTaskId)
    def downstreamTask = taskRecord(downstreamTaskId)
    // An incremental refresh of an IVM MV records no refresh mode, so an unset mode is the incremental
    // attempt succeeding; COMPLETE would say the whole MV was rebuilt.
    assertEquals("SUCCESS", downstreamTask.Status.toString())
    assertEquals("NONE", downstreamTask.RefreshMode.toString())
    order_qt_downstream_mv """SELECT k1, v1 FROM ${downstreamMv}"""
}
