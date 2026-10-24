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
 * An IVM COMPLETE refresh reads a PCT input only from the base partitions its batch is
 * recorded with. A merged LIST component can leave one join input out of a batch's mapping
 * whole: A and C keep 2020, 2038 and 2039 in partitions that meet, so their keys form one
 * MV partition, while B's 2020 partition is expired by the sync window and its 2040 one
 * meets no other table. B is therefore absent from that MV partition's mapping, and its
 * ordinary scan -- restricted to the MV partition's own key range -- would read the 2020
 * rows the batch records nowhere. The rows would enter the MV partition, and a later DELETE
 * of them would be invisible to the sync check: the current and recorded B sets are both
 * empty, so the incremental refresh exits at its all-synchronized early exit and the
 * deleted row stays behind.
 *
 * <p>With the read scoped to the mapping, B is read as no rows for that component, so the
 * join produces nothing there. The 2045 component, which all three tables name, shows that
 * the MV is still built: it keeps exactly that row before and after the DELETE.
 *
 * <p>Dates are literals and every partition is created by hand, so the kept/expired split
 * does not depend on the run date (the 2 YEAR window ends in the current year, which no
 * literal here falls in).
 */
suite("test_ivm_absent_pct_input_read_scope") {
    // The MV is checked by reading it, not through a rewrite of the base query.
    sql "SET enable_materialized_view_rewrite = false"
    sql "SET enable_dml_materialized_view_rewrite = false"

    def tableA = "ivm_aps_a"
    def tableB = "ivm_aps_b"
    def tableC = "ivm_aps_c"
    def mvName = "ivm_aps_mv"

    sql """DROP MATERIALIZED VIEW IF EXISTS ${mvName}"""
    sql """DROP TABLE IF EXISTS ${tableA}"""
    sql """DROP TABLE IF EXISTS ${tableB}"""
    sql """DROP TABLE IF EXISTS ${tableC}"""

    sql """
        CREATE TABLE ${tableA} (
            d DATE NOT NULL,
            k INT NOT NULL,
            v INT
        )
        UNIQUE KEY(d, k)
        PARTITION BY LIST(d) (
            PARTITION a_kept VALUES IN (("2020-01-01"), ("2038-01-01")),
            PARTITION a_shared VALUES IN (("2045-01-01"))
        )
        DISTRIBUTED BY HASH(d) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true"
        )
    """
    sql """
        CREATE TABLE ${tableB} (
            d DATE NOT NULL,
            k INT NOT NULL,
            v INT
        )
        UNIQUE KEY(d, k)
        PARTITION BY LIST(d) (
            PARTITION b_old VALUES IN (("2020-01-01")),
            PARTITION b_future VALUES IN (("2040-01-01")),
            PARTITION b_shared VALUES IN (("2045-01-01"))
        )
        DISTRIBUTED BY HASH(d) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true"
        )
    """
    sql """
        CREATE TABLE ${tableC} (
            d DATE NOT NULL,
            k INT NOT NULL,
            v INT
        )
        UNIQUE KEY(d, k)
        PARTITION BY LIST(d) (
            PARTITION c_kept VALUES IN (("2020-01-01"), ("2038-01-01"), ("2039-01-01")),
            PARTITION c_shared VALUES IN (("2045-01-01"))
        )
        DISTRIBUTED BY HASH(d) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true"
        )
    """
    sql """INSERT INTO ${tableA} VALUES ("2020-01-01", 1, 10), ("2045-01-01", 1, 100)"""
    sql """INSERT INTO ${tableB} VALUES ("2020-01-01", 1, 20), ("2045-01-01", 1, 200)"""
    sql """INSERT INTO ${tableC} VALUES ("2020-01-01", 1, 30), ("2045-01-01", 1, 300)"""
    // The rows the MV has to work with, so that the expectations below are not read as empty
    // base tables.
    order_qt_base_rows """SELECT (SELECT COUNT(*) FROM ${tableA}), (SELECT COUNT(*) FROM ${tableB}),
        (SELECT COUNT(*) FROM ${tableC})"""

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        PARTITION BY(d)
        DISTRIBUTED BY HASH(d) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "partition_sync_limit" = "2",
            "partition_sync_time_unit" = "YEAR",
            "refresh_partition_num" = "1"
        )
        AS SELECT a.d AS d, COUNT(*) AS cnt
           FROM ${tableA} a JOIN ${tableB} b ON a.d = b.d JOIN ${tableC} c ON a.d = c.d
           GROUP BY a.d
    """

    // Waiting through the framework helper rather than by reading the newest row: tasks() can
    // briefly miss a task that just finished, and the suite reuses the MV name across runs.
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
    def taskQuery = { String taskId ->
        """
            SELECT Status,
                   CASE WHEN RefreshMode IN ('COMPLETE', 'PARTIAL', 'NOT_REFRESH')
                        THEN RefreshMode ELSE 'NONE' END,
                   CASE WHEN IvmFallbackReason = 'CHANGELOG_BROKEN'
                        THEN IvmFallbackReason ELSE 'NONE' END
            FROM tasks('type'='mv')
            WHERE TaskId = '${taskId}'
        """
    }

    // The 2020 rows are named by no MV partition's mapping of B, so the join has no rows for
    // them; only the 2045 component, named by all three tables, is in the MV.
    def taskId = refreshAndGetTaskId("COMPLETE")
    qt_complete_task taskQuery(taskId)
    order_qt_complete_mv """SELECT d, cnt FROM ${mvName} ORDER BY d"""

    // The DELETE only touches B's expired partition. A/C and B's retained partitions are
    // unchanged, so no partition is left unsynchronized: the sync check must not be what
    // keeps the deleted row out -- it must never have entered the MV.
    sql """DELETE FROM ${tableB} WHERE d = '2020-01-01'"""
    taskId = refreshAndGetTaskId("INCREMENTAL")
    qt_incremental_task taskQuery(taskId)
    order_qt_incremental_mv """SELECT d, cnt FROM ${mvName} ORDER BY d"""

    sql """DROP MATERIALIZED VIEW IF EXISTS ${mvName}"""
    sql """DROP TABLE IF EXISTS ${tableA}"""
    sql """DROP TABLE IF EXISTS ${tableB}"""
    sql """DROP TABLE IF EXISTS ${tableC}"""
}
