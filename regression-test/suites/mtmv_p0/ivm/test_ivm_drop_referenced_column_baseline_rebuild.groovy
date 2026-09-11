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

// Dropping a column that an IVM references, then re-adding a column with the same name
// (schema ABA), used to let a strict INCREMENTAL refresh report SUCCESS while silently
// keeping the rows computed under the old column epoch.
//
// The base table change is metadata-only (light schema change) and emits no binlog, so the
// delta is empty and the refresh has nothing to apply -- the MV baseline is simply stale.
//
// Expected: dropping a referenced column invalidates the IVM baseline, so a strict
// INCREMENTAL refresh is rejected and the user is told to run a COMPLETE refresh.
// Dropping an unreferenced column must still leave the incremental path untouched.
suite("test_ivm_drop_referenced_column_baseline_rebuild", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    def tableName = "ivm_drop_ref_col_t"
    def mvName = "ivm_drop_ref_col_mv"

    sql """DROP MATERIALIZED VIEW IF EXISTS ${mvName}"""
    sql """DROP TABLE IF EXISTS ${tableName}"""

    sql """
        CREATE TABLE ${tableName} (
            id BIGINT NOT NULL,
            grp INT NULL,
            amount BIGINT NULL,
            spare INT NULL
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """
    sql """INSERT INTO ${tableName} VALUES (1, 10, 100, 7), (2, 10, 200, 8), (3, 20, 300, 9)"""

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        KEY(grp)
        DISTRIBUTED BY HASH(grp) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT grp, COUNT(*) AS cnt, SUM(amount) AS total
        FROM ${tableName} GROUP BY grp
    """

    def ddlJobCount = { String table ->
        return sql("""SHOW ALTER TABLE COLUMN WHERE TableName = '${table}'""").size()
    }

    // `SHOW ALTER TABLE COLUMN` keeps finished jobs, so wait for a *new* job that is FINISHED.
    def waitDdlFinished = { String table, int previousJobCount ->
        Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
            def jobs = sql """SHOW ALTER TABLE COLUMN WHERE TableName = '${table}'"""
            return jobs.size() > previousJobCount
                    && jobs.every({ row -> row[9].toString() == 'FINISHED' })
        })
    }

    def lastTaskId = null
    // tasks('type'='mv') can briefly miss the just-finished task, so require a *new* TaskId.
    def waitTerminalTask = { String mv ->
        def taskResult
        Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
            taskResult = sql_return_maparray("""
                SELECT TaskId, Status, RefreshMode, IvmFallbackReason, ErrorMsg
                FROM tasks('type'='mv')
                WHERE MvDatabaseName = '${context.dbName}' AND MvName = '${mv}'
                ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
            """)
            return !taskResult.isEmpty()
                    && taskResult[0].TaskId.toString() != lastTaskId
                    && taskResult[0].Status.toString() != 'PENDING'
                    && taskResult[0].Status.toString() != 'RUNNING'
        })
        lastTaskId = taskResult[0].TaskId.toString()
        return taskResult[0]
    }

    def mvRows = {
        return sql("""SELECT grp, cnt, total FROM ${mvName} ORDER BY grp""")
    }

    // ---------------------------------------------------------------- 1. baseline
    sql """REFRESH MATERIALIZED VIEW ${mvName} COMPLETE"""
    def task = waitTerminalTask(mvName)
    assertEquals("SUCCESS", task.Status.toString(), "baseline COMPLETE refresh: " + task.ErrorMsg)
    assertEquals("[[10, 2, 300], [20, 1, 300]]", mvRows().toString())

    // ------------------------------------- 2. unreferenced column: no baseline invalidation
    def before = ddlJobCount(tableName)
    sql """ALTER TABLE ${tableName} DROP COLUMN spare"""
    waitDdlFinished(tableName, before)

    sql """REFRESH MATERIALIZED VIEW ${mvName} INCREMENTAL"""
    task = waitTerminalTask(mvName)
    assertEquals("SUCCESS", task.Status.toString(),
            "dropping an unreferenced column must not invalidate the IVM baseline: " + task.ErrorMsg)

    // ---------------------------------------- 3. referenced column: strict INCREMENTAL rejected
    before = ddlJobCount(tableName)
    sql """ALTER TABLE ${tableName} DROP COLUMN grp"""
    waitDdlFinished(tableName, before)

    sql """REFRESH MATERIALIZED VIEW ${mvName} INCREMENTAL"""
    task = waitTerminalTask(mvName)
    assertEquals("FAILED", task.Status.toString(),
            "dropping a referenced column must reject a strict INCREMENTAL refresh")

    // -------------------------------- 4. same-name re-add (schema ABA) is still rejected
    before = ddlJobCount(tableName)
    sql """ALTER TABLE ${tableName} ADD COLUMN grp INT NULL DEFAULT '0'"""
    waitDdlFinished(tableName, before)

    sql """REFRESH MATERIALIZED VIEW ${mvName} INCREMENTAL"""
    task = waitTerminalTask(mvName)
    assertEquals("FAILED", task.Status.toString(),
            "schema ABA must not be silently accepted by a strict INCREMENTAL refresh")
    assertTrue(task.ErrorMsg.toString().contains("baseline rebuild is pending"),
            "expected a pending baseline rebuild hint, got: " + task.ErrorMsg)

    // ------------------------------------ 5. COMPLETE rebuild reflects current base semantics
    // Every pre-existing row now reads the re-added column's default value.
    sql """REFRESH MATERIALIZED VIEW ${mvName} COMPLETE"""
    task = waitTerminalTask(mvName)
    assertEquals("SUCCESS", task.Status.toString(), "COMPLETE rebuild after ABA: " + task.ErrorMsg)
    assertEquals("[[0, 3, 600]]", mvRows().toString())
}
