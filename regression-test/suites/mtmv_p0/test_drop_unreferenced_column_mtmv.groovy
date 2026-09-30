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
 * <p>The rest of the suite is about which column a name in the query answers for, because that is what
 * makes a change to a column nothing reads. A name is a column of the table the change is about only where
 * the query reads it from there, and it is the scopes' to answer for where the query resolves it across a
 * scope boundary: what is left is a name bound to another table inside the query's own scope, which no
 * later change can move.
 *
 * <p>The second half is also where the consequence is observable: the rewrite reaches the MV on that table
 * and not on a merge-on-write one, so the state the change records is what is left to report on the first.
 * The IVM side of the same change, where the state is what escalates the next refresh to a whole-MV
 * COMPLETE, is pinned in the ivm directory.
 */
suite("test_drop_unreferenced_column_mtmv") {
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

    String scopeDropOuter = "${suiteName}_scope_drop_outer"
    String scopeDropInner = "${suiteName}_scope_drop_inner"
    String scopeDropMv = "${suiteName}_scope_drop_mv"
    String scopeAddOuter = "${suiteName}_scope_add_outer"
    String scopeAddInner = "${suiteName}_scope_add_inner"
    String scopeAddMv = "${suiteName}_scope_add_mv"

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
        PROPERTIES ("replication_num" = "1", "light_schema_change" = "false")
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

    // The other half of the answer, on a table where a job applies the change rather than the statement.
    // The hook runs where that job has not run yet, so the table still holds the column and every query
    // analyses against it: nothing can be concluded about the column from a query, and the MV is
    // invalidated the way it was before the queries were asked at all.
    sql """ALTER TABLE ${dupTable} DROP COLUMN spare"""
    assertEquals("FINISHED", getAlterColumnFinalState("${dupTable}"))
    order_qt_dup_state_after_unreferenced_drop "select Name,State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${dupMv}'"
    mv_not_part_in(dupQuery, dupMv)
    order_qt_dup_rows_after_unreferenced_drop "SELECT k1, total FROM ${dupMv}"

    // ---- the name a query reaches a column by can move, and that is the query's to judge ----
    // `flag` below is unqualified inside the subquery, so it is the inner table's column while that table
    // has one: the column nearest to a name in the query's scopes answers for it. Taking that column away
    // leaves the name to the outer table and the query still analyses with the columns it always produced,
    // while its rows are the ones of the column that went away.
    sql """drop materialized view if exists ${scopeDropMv}"""
    sql """drop table if exists ${scopeDropOuter}"""
    sql """drop table if exists ${scopeDropInner}"""
    sql """
        CREATE TABLE ${scopeDropOuter} (id INT, flag INT) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE ${scopeDropInner} (id INT, flag INT) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO ${scopeDropOuter} VALUES (1, 0)"""
    sql """INSERT INTO ${scopeDropInner} VALUES (1, 1)"""
    sql """
        CREATE MATERIALIZED VIEW ${scopeDropMv}
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")
        AS SELECT o.id FROM ${scopeDropOuter} o
        WHERE EXISTS (SELECT 1 FROM ${scopeDropInner} i WHERE i.id = o.id AND flag = 1)
    """
    waitingMTMVTaskFinishedByMvName(scopeDropMv)
    order_qt_scope_drop_baseline "SELECT id FROM ${scopeDropMv}"
    sql """ALTER TABLE ${scopeDropInner} DROP COLUMN flag"""
    order_qt_scope_drop_state "select Name,State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${scopeDropMv}'"

    // And the same name can be taken over by a column that arrives. The inner table has no `flag` here, so
    // the name is the outer table's to answer; a column added inside the subquery's scope answers for it
    // from then on. A column added that no query reaches a name by leaves every MV alone.
    sql """drop materialized view if exists ${scopeAddMv}"""
    sql """drop table if exists ${scopeAddOuter}"""
    sql """drop table if exists ${scopeAddInner}"""
    sql """
        CREATE TABLE ${scopeAddOuter} (id INT, flag INT) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE ${scopeAddInner} (id INT) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO ${scopeAddOuter} VALUES (1, 1)"""
    sql """INSERT INTO ${scopeAddInner} VALUES (1)"""
    sql """
        CREATE MATERIALIZED VIEW ${scopeAddMv}
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")
        AS SELECT o.id FROM ${scopeAddOuter} o
        WHERE EXISTS (SELECT 1 FROM ${scopeAddInner} i WHERE i.id = o.id AND flag = 1)
    """
    waitingMTMVTaskFinishedByMvName(scopeAddMv)
    order_qt_scope_add_baseline "SELECT id FROM ${scopeAddMv}"
    sql """ALTER TABLE ${scopeAddInner} ADD COLUMN unrelated BIGINT"""
    order_qt_scope_add_unreached "select Name,State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${scopeAddMv}'"
    sql """ALTER TABLE ${scopeAddInner} ADD COLUMN flag INT DEFAULT 0"""
    order_qt_scope_add_state "select Name,State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${scopeAddMv}'"

    // A rename takes a name over the same way an add gives one, and it does it with two names at once: the
    // one the query spelled, which the column no longer answers for, and the one that answers for it from
    // then on. Neither of the two says on its own whether the query moved -- the old name is one no query
    // reaches any more, and the new one is only a move where the old name was the query's before -- so the
    // rename reports both. Here the inner table has `x` and no `flag`, so `flag` is the outer table's, and
    // renaming `x` to `flag` hands the name to the inner scope: the query goes on producing `o.id` out of
    // the rows of a column it never read, and the MV is invalidated.
    String scopeRenameOuter = "${suiteName}_scope_rename_outer"
    String scopeRenameInner = "${suiteName}_scope_rename_inner"
    String scopeRenameMv = "${suiteName}_scope_rename_mv"
    sql """drop materialized view if exists ${scopeRenameMv}"""
    sql """drop table if exists ${scopeRenameOuter}"""
    sql """drop table if exists ${scopeRenameInner}"""
    sql """
        CREATE TABLE ${scopeRenameOuter} (id INT, flag INT) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE ${scopeRenameInner} (id INT, x INT) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO ${scopeRenameOuter} VALUES (1, 1)"""
    sql """INSERT INTO ${scopeRenameInner} VALUES (1, 0)"""
    sql """
        CREATE MATERIALIZED VIEW ${scopeRenameMv}
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")
        AS SELECT o.id FROM ${scopeRenameOuter} o
        WHERE EXISTS (SELECT 1 FROM ${scopeRenameInner} i WHERE i.id = o.id AND flag = 1)
    """
    waitingMTMVTaskFinishedByMvName(scopeRenameMv)
    order_qt_scope_rename_baseline "SELECT id FROM ${scopeRenameMv}"
    sql """ALTER TABLE ${scopeRenameInner} RENAME COLUMN x flag"""
    order_qt_scope_rename_state "select Name,State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${scopeRenameMv}'"

    // ---- a name another table answers for is not this one's to give or take ----
    // `flag` here is written with the qualifier of the second table, so what the query reads does not turn
    // on anything the first table does with a `flag` of its own: a name bound where it is written is one no
    // later change can move. The first table's `flag` is 0 and the second's is 1, so the row the MV holds is
    // the one the qualified binding produces -- a query that had read the first table's column would hold
    // none -- and taking that column away, or giving it back, leaves this query and this MV alone.
    String qualOuter = "${suiteName}_qual_outer"
    String qualInner = "${suiteName}_qual_inner"
    String qualMv = "${suiteName}_qual_mv"

    sql """drop materialized view if exists ${qualMv}"""
    sql """drop table if exists ${qualOuter}"""
    sql """drop table if exists ${qualInner}"""
    sql """
        CREATE TABLE ${qualOuter} (id INT, flag INT) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE ${qualInner} (id INT, flag INT) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO ${qualOuter} VALUES (1, 0)"""
    sql """INSERT INTO ${qualInner} VALUES (1, 1)"""
    sql """
        CREATE MATERIALIZED VIEW ${qualMv}
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")
        AS SELECT a.id FROM ${qualOuter} a JOIN ${qualInner} b ON a.id = b.id WHERE b.flag = 1
    """
    waitingMTMVTaskFinishedByMvName(qualMv)
    order_qt_qualified_baseline "SELECT id FROM ${qualMv}"
    sql """ALTER TABLE ${qualOuter} DROP COLUMN flag"""
    order_qt_qualified_drop_state "select Name,State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${qualMv}'"
    sql """ALTER TABLE ${qualOuter} ADD COLUMN flag INT DEFAULT 0"""
    order_qt_qualified_add_state "select Name,State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${qualMv}'"
    order_qt_qualified_rows "SELECT id FROM ${qualMv}"

    // ---- an add a job applies: there is no query to ask ----
    // The hook of a change that is not a light one runs before the job has applied it, so the table is the one
    // from before the change and every query is analysed against that. A column that is not in the table yet
    // is one no query can be looked at for, and the MV is invalidated -- for a column added on its own and for
    // a batch of them, which is how an add is written either way. What the add would do once the job lands is
    // not readable here, and a name it may answer for is one the rows of these views were not computed under.
    String jobAddTable = "${suiteName}_job_add_table"
    String jobAddMv = "${suiteName}_job_add_mv"
    String jobAddColsTable = "${suiteName}_job_add_cols_table"
    String jobAddColsMv = "${suiteName}_job_add_cols_mv"
    sql """drop materialized view if exists ${jobAddMv}"""
    sql """drop materialized view if exists ${jobAddColsMv}"""
    sql """drop table if exists ${jobAddTable}"""
    sql """drop table if exists ${jobAddColsTable}"""
    sql """
        CREATE TABLE ${jobAddTable} (id INT, amount INT) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "light_schema_change" = "false")
    """
    sql """
        CREATE TABLE ${jobAddColsTable} (id INT, amount INT) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "light_schema_change" = "false")
    """
    sql """INSERT INTO ${jobAddTable} VALUES (1, 100)"""
    sql """INSERT INTO ${jobAddColsTable} VALUES (1, 100)"""
    sql """
        CREATE MATERIALIZED VIEW ${jobAddMv}
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")
        AS SELECT id, SUM(amount) AS total FROM ${jobAddTable} GROUP BY id
    """
    sql """
        CREATE MATERIALIZED VIEW ${jobAddColsMv}
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")
        AS SELECT id, SUM(amount) AS total FROM ${jobAddColsTable} GROUP BY id
    """
    waitingMTMVTaskFinishedByMvName(jobAddMv)
    waitingMTMVTaskFinishedByMvName(jobAddColsMv)
    order_qt_job_add_baseline "select Name,State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${jobAddMv}'"
    order_qt_job_add_cols_baseline "select Name,State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${jobAddColsMv}'"
    sql """ALTER TABLE ${jobAddTable} ADD COLUMN spare INT"""
    order_qt_job_add_state "select Name,State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${jobAddMv}'"
    sql """ALTER TABLE ${jobAddColsTable} ADD COLUMN (spare INT, other INT)"""
    order_qt_job_add_cols_state "select Name,State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${jobAddColsMv}'"

    // ---- a name a subquery's own projection answers for ----
    // The subquery below projects `flag` while the inner table has no column of that name, so the name is
    // the outer table's to answer for and the subquery projects the outer column: the MV holds the row.
    // Giving the inner table a column of that name hands the name to it, so the projection reads the inner
    // column from then on and the query returns no row -- while every column the MV produces is the one it
    // always produced. Nothing is read here that the rows of the view are computed from, and the name is
    // still the change's to answer for: a column of a subquery's own output is where a name can move
    // without moving an output of the view.
    String projOuter = "${suiteName}_proj_outer"
    String projInner = "${suiteName}_proj_inner"
    String projMv = "${suiteName}_proj_mv"
    sql """drop materialized view if exists ${projMv}"""
    sql """drop table if exists ${projOuter}"""
    sql """drop table if exists ${projInner}"""
    sql """
        CREATE TABLE ${projOuter} (id INT, flag INT) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE ${projInner} (x INT) DUPLICATE KEY(x)
        DISTRIBUTED BY HASH(x) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO ${projOuter} VALUES (1, 1)"""
    sql """INSERT INTO ${projInner} VALUES (1)"""
    sql """
        CREATE MATERIALIZED VIEW ${projMv}
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")
        AS SELECT o.id FROM ${projOuter} o WHERE o.flag IN (SELECT flag FROM ${projInner} i)
    """
    waitingMTMVTaskFinishedByMvName(projMv)
    order_qt_projection_baseline "SELECT id FROM ${projMv}"
    sql """ALTER TABLE ${projInner} ADD COLUMN flag INT DEFAULT 0"""
    order_qt_projection_state "select Name,State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${projMv}'"

    // ---- a name a subquery names itself ----
    // `flag` here is written unqualified inside the subquery and the subquery names a `flag` of its own, so
    // the name is the table's column while the table has one and the subquery's own value once it does not:
    // the group by and the having then read the constant, the subquery has a row, and the query returns a
    // row the view's rows do not have. Nothing of the view's is computed from a column of the changed table
    // afterwards, so the check has to read what the subquery itself names.
    String aliasOuter = "${suiteName}_alias_outer"
    String aliasInner = "${suiteName}_alias_inner"
    String aliasMv = "${suiteName}_alias_mv"
    sql """drop materialized view if exists ${aliasMv}"""
    sql """drop table if exists ${aliasOuter}"""
    sql """drop table if exists ${aliasInner}"""
    sql """
        CREATE TABLE ${aliasOuter} (id INT, flag INT) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")
    """
    sql """
        CREATE TABLE ${aliasInner} (id INT, flag INT) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")
    """
    sql """INSERT INTO ${aliasOuter} VALUES (1, 1)"""
    sql """INSERT INTO ${aliasInner} VALUES (1, 0)"""
    sql """
        CREATE MATERIALIZED VIEW ${aliasMv}
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")
        AS SELECT o.id FROM ${aliasOuter} o WHERE EXISTS (
            SELECT 1 AS flag, COUNT(*) AS n FROM ${aliasInner} i
            WHERE i.id = o.id GROUP BY flag HAVING flag = 1)
    """
    waitingMTMVTaskFinishedByMvName(aliasMv)
    order_qt_alias_baseline "SELECT COUNT(*) FROM ${aliasMv}"
    sql """ALTER TABLE ${aliasInner} DROP COLUMN flag"""
    order_qt_alias_state "select Name,State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${aliasMv}'"

    // ---- a name a subquery projects and never compares ----
    // The subquery below projects `spare`, which is only the outer table's while the outer table is the one
    // that has it; an EXISTS tests the rows of its subquery and not what it projects, so the same rows come
    // back after the inner table is given a column of that name. Reading the subquery's scope has to leave
    // that case alone, or a change this view does not depend on takes its rewrite snapshot with it.
    String ignoredOuter = "${suiteName}_ignored_outer"
    String ignoredInner = "${suiteName}_ignored_inner"
    String ignoredMv = "${suiteName}_ignored_mv"
    sql """drop materialized view if exists ${ignoredMv}"""
    sql """drop table if exists ${ignoredOuter}"""
    sql """drop table if exists ${ignoredInner}"""
    sql """
        CREATE TABLE ${ignoredOuter} (id INT, spare INT) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")
    """
    sql """
        CREATE TABLE ${ignoredInner} (id INT) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")
    """
    sql """INSERT INTO ${ignoredOuter} VALUES (1, 7)"""
    sql """INSERT INTO ${ignoredInner} VALUES (1)"""
    sql """
        CREATE MATERIALIZED VIEW ${ignoredMv}
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")
        AS SELECT o.id FROM ${ignoredOuter} o
        WHERE EXISTS (SELECT spare FROM ${ignoredInner} i WHERE i.id = o.id)
    """
    waitingMTMVTaskFinishedByMvName(ignoredMv)
    sql """ALTER TABLE ${ignoredInner} ADD COLUMN spare INT DEFAULT 0"""
    order_qt_ignored_projection_state "select Name,State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${ignoredMv}'"
    order_qt_ignored_projection_rows "SELECT id FROM ${ignoredMv}"

    // ---- a name a subquery names and never reads ----
    // The subquery below names a `flag` of its own and no reference in it resolves to that name: the compared
    // `flag` is the outer table's, written with its qualifier, and giving the inner table a column of that
    // name changes the subquery's own projection and nothing the query reads. Naming a value is not reading
    // it, so this MV is left where it is.
    String namedOuter = "${suiteName}_named_outer"
    String namedInner = "${suiteName}_named_inner"
    String namedMv = "${suiteName}_named_mv"
    sql """drop materialized view if exists ${namedMv}"""
    sql """drop table if exists ${namedOuter}"""
    sql """drop table if exists ${namedInner}"""
    sql """
        CREATE TABLE ${namedOuter} (id INT, flag INT) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")
    """
    sql """
        CREATE TABLE ${namedInner} (id INT) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")
    """
    sql """INSERT INTO ${namedOuter} VALUES (1, 1)"""
    sql """INSERT INTO ${namedInner} VALUES (1)"""
    sql """
        CREATE MATERIALIZED VIEW ${namedMv}
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")
        AS SELECT o.id FROM ${namedOuter} o WHERE o.flag IN (SELECT 1 AS flag FROM ${namedInner} i)
    """
    waitingMTMVTaskFinishedByMvName(namedMv)
    sql """ALTER TABLE ${namedInner} ADD COLUMN flag INT DEFAULT 0"""
    order_qt_named_not_read_state "select Name,State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${namedMv}'"
}
