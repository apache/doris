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

import org.junit.Assert;

/**
 * Which base-table column changes invalidate a materialized view.
 *
 * <p>A dropped column is judged by re-analysing the MV's own query -- but only where that question has an
 * answer. The query is analysed against the table as it is when the alter reaches the MV hook, and unless
 * the change is a light one it has not been applied yet at that point: it was submitted as a job, the
 * column is still there, and every query still analyses. An invalidation decided on that answer would be
 * about the table from before the change, which is exactly where a dropped and re-added column leaves the
 * ABA the invalidation exists for. So a change that is not in place keeps invalidating, and the two halves
 * of this suite pin the two answers:
 * <ol>
 *   <li>a merge-on-write table, where dropping a value column is a light change: the MV whose query does
 *       not name the column is left alone, and the MV whose query names it is invalidated;</li>
 *   <li>a duplicate table, where the same drop is not a light one and a job does the data rewrite: the
 *       column is out of the table's schema before the hook runs all the same, so the same answer holds.
 *       What the gate is for is the other side of that -- a change a job has not applied yet, where the
 *       query would be analysed against the table from before it. There the MV is invalidated, which is
 *       what every column change did before the queries were asked at all.</li>
 * </ol>
 *
 * <p>The second half is also where the consequence is observable: the rewrite reaches the MV on that table
 * and not on a merge-on-write one, so the state the change records is what is left to report on the first.
 * The IVM side of the same change, where the state is what escalates the next refresh to a whole-MV
 * COMPLETE, is pinned in the ivm directory.
 */
suite("test_drop_unreferenced_column_mtmv", "mtmv") {
    String dbName = context.config.getDbNameByFile(context.file)
    String suiteName = "test_drop_unreferenced_column_mtmv"

    // ------------------------------------------------- 1. the change is in place: the query decides
    String mowTable = "${suiteName}_mow_table"
    String mowMv = "${suiteName}_mow_mv"

    sql """drop materialized view if exists ${mowMv}"""
    sql """drop table if exists ${mowTable}"""
    sql """
        CREATE TABLE ${mowTable}
        (
            k1 INT NOT NULL,
            amount BIGINT,
            spare BIGINT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true")
    """
    sql """INSERT INTO ${mowTable} VALUES (1, 100, 7), (2, 200, 8)"""
    sql """
        CREATE MATERIALIZED VIEW ${mowMv}
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES ("replication_num" = "1")
        AS SELECT k1, SUM(amount) AS total FROM ${mowTable} GROUP BY k1
    """
    sql """REFRESH MATERIALIZED VIEW ${mowMv} COMPLETE"""
    waitingMTMVTaskFinishedByMvName(mowMv)
    order_qt_mow_baseline "SELECT k1, total FROM ${mowMv}"

    // A column the query does not name. Dropping it gives this MV nothing to recompute, so it is not
    // invalidated: it stays a refresh candidate, and the state is where that shows.
    sql """ALTER TABLE ${mowTable} DROP COLUMN spare"""
    assertEquals("FINISHED", getAlterColumnFinalState("${mowTable}"))
    order_qt_mow_state_after_unreferenced_drop "select Name,State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mowMv}'"

    // A column the query names. The MV cannot be computed from the table any more, so it is invalidated.
    sql """ALTER TABLE ${mowTable} DROP COLUMN amount"""
    assertEquals("FINISHED", getAlterColumnFinalState("${mowTable}"))
    order_qt_mow_state_after_referenced_drop "select Name,State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mowMv}'"
    // Neither column was one the rows the MV holds depended on: the invalidation above is not about the
    // data being wrong, it is about the query the data stands for.
    order_qt_mow_rows_after_both_drops "SELECT k1, total FROM ${mowMv}"

    // ------------------------- 2. the same drop on a table whose data a job rewrites: same answer
    String dupTable = "${suiteName}_dup_table"
    String dupMv = "${suiteName}_dup_mv"
    String dupQuery = "SELECT k1, SUM(amount) AS total FROM ${dupTable} GROUP BY k1"

    sql """drop materialized view if exists ${dupMv}"""
    sql """drop table if exists ${dupTable}"""
    sql """
        CREATE TABLE ${dupTable}
        (
            k1 INT,
            amount BIGINT,
            spare BIGINT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO ${dupTable} VALUES (1, 100, 7), (2, 200, 8)"""
    sql """
        CREATE MATERIALIZED VIEW ${dupMv}
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES ("replication_num" = "1")
        AS SELECT k1, SUM(amount) AS total FROM ${dupTable} GROUP BY k1
    """
    sql """REFRESH MATERIALIZED VIEW ${dupMv} COMPLETE"""
    waitingMTMVTaskFinishedByMvName(dupMv)
    order_qt_dup_baseline "SELECT k1, total FROM ${dupMv}"
    // This MV is reachable by the rewrite, which is what makes the answer below observable: the MV takes
    // part in it before the change, and a change that invalidated the MV would take it out.
    mv_rewrite_success_without_check_chosen(dupQuery, dupMv)

    // The same shape as the first half, on a table where dropping a column with data in it is not a light
    // change and a job does the data rewrite. The column is out of the table's schema before the hook runs
    // all the same -- measured, and it is what makes the same answer the right one here: the query is
    // analysed against a table that has the change, and a column it does not name leaves it analysable.
    sql """ALTER TABLE ${dupTable} DROP COLUMN spare"""
    assertEquals("FINISHED", getAlterColumnFinalState("${dupTable}"))
    order_qt_dup_state_after_unreferenced_drop "select Name,State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${dupMv}'"
    mv_rewrite_success_without_check_chosen(dupQuery, dupMv)
    order_qt_dup_rows_after_unreferenced_drop "SELECT k1, total FROM ${dupMv}"
}
