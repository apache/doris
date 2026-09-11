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

// A materialized view re-validates its schema against a fresh analysis of its own query whenever a
// base table changes (MTMVPlanUtil.ensureMTMVQueryUsable -> checkColumnIfChange). A table created
// with row binlog carries hidden columns that the query never produces, and an MV carries them too
// when its own properties enable row binlog (which cascade IVM requires). The analyzed schema has to
// contain them as well, otherwise every refresh of such an MV fails with
// "column length not equals, please check whether columns of base table have changed" -- including a
// COMPLETE refresh, which is the only way out of a stale baseline.
//
// Covered here: the four MV shapes, and the DORIS-28306 shape where a cascade L1 could not recover
// through COMPLETE after a schema ABA on a referenced column.
suite("test_ivm_row_binlog_schema_validation") {
    def rowBinlogProps = "'replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW'"
    def plainProps = "'replication_num' = '1'"
    // A cascade source keeps historical values so its own downstream MV can read the binlog.
    def cascadeProps = rowBinlogProps + ", 'binlog.need_historical_value' = 'true'"

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
                SELECT TaskId, Status, RefreshMode, ErrorMsg
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

    def refresh = { String mv, String mode ->
        sql """REFRESH MATERIALIZED VIEW ${mv} ${mode}"""
        return waitTerminalTask(mv)
    }

    // ---------------------------------------------------------------- 1. four MV shapes
    // Same query and same base table for all four; what differs is whether the MV itself enables
    // row binlog, and (for the IVM ones) whether the MV is a MOW unique table.
    def cases = [
            [name: "rb_ivm", mvProps: rowBinlogProps, ivm: true],
            [name: "rb_ivm_no_binlog", mvProps: plainProps, ivm: true],
            [name: "rb_dup", mvProps: rowBinlogProps, ivm: false],
            [name: "rb_dup_no_binlog", mvProps: plainProps, ivm: false],
    ]

    for (def c : cases) {
        def table = c.name + "_base"
        def mv = c.name + "_mv"

        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv}"""
        sql """DROP TABLE IF EXISTS ${table}"""
        sql """
            CREATE TABLE ${table} (
                k1 INT NOT NULL,
                v1 INT NULL,
                spare INT NULL
            )
            UNIQUE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES ('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'true',
                        'binlog.enable' = 'true', 'binlog.format' = 'ROW',
                        'binlog.need_historical_value' = 'true')
        """
        sql """INSERT INTO ${table} VALUES (1, 10, 1), (2, 20, 1)"""

        sql """
            CREATE MATERIALIZED VIEW ${mv}
            BUILD DEFERRED REFRESH ${c.ivm ? 'INCREMENTAL' : 'COMPLETE'} ON MANUAL
            KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (${c.mvProps})
            AS SELECT k1, SUM(v1) AS total FROM ${table} GROUP BY k1
        """
        def task = refresh(mv, "COMPLETE")
        assertEquals("SUCCESS", task.Status.toString(), "${c.name} baseline refresh: " + task.ErrorMsg)

        // A schema change that leaves the query intact: the MV must stay refreshable. The dropped
        // column is not referenced, so nothing about the MV's result changes.
        def before = ddlJobCount(table)
        sql """ALTER TABLE ${table} DROP COLUMN spare"""
        waitDdlFinished(table, before)

        task = refresh(mv, c.ivm ? "INCREMENTAL" : "COMPLETE")
        assertEquals("SUCCESS", task.Status.toString(),
                "${c.name} refresh after an unrelated DROP COLUMN: " + task.ErrorMsg)
    }

    // The dropped column was never referenced, so all four MVs must still hold their baseline rows.
    order_qt_rb_ivm_rows "SELECT k1, total FROM rb_ivm_mv"
    order_qt_rb_ivm_no_binlog_rows "SELECT k1, total FROM rb_ivm_no_binlog_mv"
    order_qt_rb_dup_rows "SELECT k1, total FROM rb_dup_mv"
    order_qt_rb_dup_no_binlog_rows "SELECT k1, total FROM rb_dup_no_binlog_mv"

    // ------------------------------------- 2. DORIS-28306: cascade L1 must recover via COMPLETE
    // A cascade forces row binlog onto L1, so L1 is exactly the shape that used to make every
    // refresh of L1 fail after a schema change. An ABA on a referenced column must still leave the
    // explicit COMPLETE recovery path usable.
    def cTable = "cascade_aba_base"
    sql """DROP MATERIALIZED VIEW IF EXISTS cascade_l2"""
    sql """DROP MATERIALIZED VIEW IF EXISTS cascade_l1"""
    sql """DROP TABLE IF EXISTS ${cTable}"""
    sql """
        CREATE TABLE ${cTable} (
            id BIGINT NOT NULL,
            grp INT NULL,
            amount BIGINT NULL
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'true',
                    'binlog.enable' = 'true', 'binlog.format' = 'ROW',
                    'binlog.need_historical_value' = 'true')
    """
    sql """INSERT INTO ${cTable} VALUES (1, 10, 100), (2, 10, 200), (3, 20, 300)"""

    sql """
        CREATE MATERIALIZED VIEW cascade_l1
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL KEY(id)
        PROPERTIES (${cascadeProps})
        AS SELECT id, grp, amount FROM ${cTable}
    """
    def cTask = refresh("cascade_l1", "COMPLETE")
    assertEquals("SUCCESS", cTask.Status.toString(), "cascade L1 baseline: " + cTask.ErrorMsg)

    sql """
        CREATE MATERIALIZED VIEW cascade_l2
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL KEY(grp)
        PROPERTIES (${cascadeProps})
        AS SELECT grp, COUNT(*) AS row_count, SUM(amount) AS total_amount FROM cascade_l1 GROUP BY grp
    """
    cTask = refresh("cascade_l2", "COMPLETE")
    assertEquals("SUCCESS", cTask.Status.toString(), "cascade L2 baseline: " + cTask.ErrorMsg)
    order_qt_cascade_l2_baseline "SELECT grp, row_count, total_amount FROM cascade_l2"

    def cBefore = ddlJobCount(cTable)
    sql """ALTER TABLE ${cTable} DROP COLUMN grp"""
    waitDdlFinished(cTable, cBefore)
    cBefore = ddlJobCount(cTable)
    sql """ALTER TABLE ${cTable} ADD COLUMN grp INT NULL DEFAULT '0'"""
    waitDdlFinished(cTable, cBefore)

    // COMPLETE is the recovery path: it has to rebuild L1 from the current base-table semantics,
    // where every pre-existing row now reads the re-added column's default value. Whether the
    // strict INCREMENTAL in between is rejected is a separate contract, owned by the suite for the
    // baseline invalidation itself, so it is deliberately not asserted here -- this suite has to
    // hold with or without that change.
    cTask = refresh("cascade_l1", "COMPLETE")
    assertEquals("SUCCESS", cTask.Status.toString(), "COMPLETE after schema ABA: " + cTask.ErrorMsg)
    order_qt_cascade_l1_after_aba "SELECT grp, COUNT(*) AS c, SUM(amount) AS s FROM cascade_l1 GROUP BY grp"

    // ... and the downstream level stays refreshable afterwards.
    cTask = refresh("cascade_l2", "COMPLETE")
    assertEquals("SUCCESS", cTask.Status.toString(), "cascade L2 after L1 recovery: " + cTask.ErrorMsg)
    order_qt_cascade_l2_after_aba "SELECT grp, row_count, total_amount FROM cascade_l2"
}
