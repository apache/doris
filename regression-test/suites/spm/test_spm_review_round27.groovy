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

suite("test_spm_review_round27", "spm") {

    // Replay side stability and the SHOW / CREATE surfaces.
    //
    //  - #1: a baseline is produced under the SPM rule whitelist (every MV rewrite
    //    excluded), so its fingerprint freezes the SOURCE table. The replay used to be
    //    planned by the ordinary planner with the session's full rule set: an async MTMV
    //    that became eligible AFTER the baseline was created substituted its storage table
    //    for the source table, the post-plan fingerprint guard rejected its own replay and
    //    the SELECT failed (default enable_spm_fallback=false) although the source table
    //    had not changed. The replay now installs the same whitelist mask.
    //  - #2: scan selectors are compared per table in STATEMENT order: the ambiguous swap
    //    of PARTITION pins between self-join occurrences is rejected at CREATE.
    //  - #4: SHOW BASELINE PLANS WHERE 1 = 1 used to become pattern = '1' and searched
    //    SQL / status / source text for 1: it now raises the advertised analysis error.
    // #3 (authoritative GLOBAL rows on a follower) needs two FEs: it is covered by
    // BaselineManagerConcurrencyTest.

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    def ownBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r27_") }
    }
    def dropOwnBaselines = {
        ownBaselines().each { row ->
            sql """DROP BASELINE PLAN ${row[0]}"""
        }
    }
    dropOwnBaselines()

    def explainOf = { String query -> sql("""EXPLAIN ${query}""").toString() }
    def createBaseline = { String bind, String plan ->
        (sql('CREATE GLOBAL BASELINE PLAN "' + bind + '" WITH "' + plan + '"')[0][0] as Long)
    }

    // ==================== #4: no supported column -> analysis error ====================
    // (each statement must stay on ONE line: a test{} block treats every line as its own
    // statement)
    test {
        sql 'SHOW BASELINE PLANS WHERE 1 = 1'
        exception "only supports"
    }
    test {
        sql 'SHOW BASELINE PLANS WHERE id + 0 = 1'
        exception "only supports"
    }
    // the supported shapes keep working
    assertEquals(0, sql("""SHOW BASELINE PLANS WHERE id = -1""").size())
    assertTrue(sql("""SHOW BASELINE PLANS LIKE '%'""").size() >= 0)

    // ==================== #1: an MTMV appears after the baseline ====================
    sql """DROP TABLE IF EXISTS spm_r27_mv_t"""
    sql """
        CREATE TABLE spm_r27_mv_t (k INT, v INT)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r27_mv_t VALUES (1, 1), (1, 2), (2, 3), (3, 4)"""

    sql """DROP MATERIALIZED VIEW IF EXISTS spm_r27_mv"""
    // the baseline is frozen while NO eligible MV exists: the fingerprint pins the source
    // table and the frozen plan reads it. ORDER BY is part of the query text (and of the
    // baseline), so the pinned replay results below are deterministic.
    String agg = "SELECT k, SUM(v) FROM spm_r27_mv_t GROUP BY k ORDER BY k"
    long aggId = createBaseline(agg, agg)
    assertTrue(explainOf(agg).contains("SPM baseline hit: id=${aggId}"),
            "the aggregate baseline must be hit: " + explainOf(agg))
    order_qt_r27_frozen_agg """SELECT k, SUM(v) FROM spm_r27_mv_t GROUP BY k ORDER BY k"""

    // ... an eligible async MTMV appears afterwards and is refreshed
    sql """
        CREATE MATERIALIZED VIEW spm_r27_mv
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT k, SUM(v) AS sv FROM spm_r27_mv_t GROUP BY k
    """
    sql """REFRESH MATERIALIZED VIEW spm_r27_mv COMPLETE"""

    // The ordinary planner would substitute the MTMV for the source table: prove the
    // candidate is really eligible, otherwise the rest of this section would pass
    // vacuously. A refreshed MTMV republishes its rewrite eligibility asynchronously, so
    // poll (the same session becomes eligible a moment after the refresh).
    sql """set enable_spm_rewrite = false"""
    boolean mvEligible = false
    for (int i = 0; i < 60 && !mvEligible; i++) {
        if (explainOf(agg).contains("spm_r27_mv chose")) {
            mvEligible = true
        } else {
            Thread.sleep(1000L)
        }
    }
    assertTrue(mvEligible,
            "precondition: the MTMV must become an eligible rewrite candidate for the"
                    + " frozen query: " + explainOf(agg))
    sql """set enable_spm_rewrite = true"""

    // ... but the REPLAY keeps reading the FROZEN source table: the baseline still hits,
    // the plan does not switch to the MTMV, and the result is unchanged (before the fix
    // the MTMV was substituted, the post-plan fingerprint guard rejected its own replay
    // and, with enable_spm_fallback=false, the SELECT failed)
    assertTrue(explainOf(agg).contains("SPM baseline hit: id=${aggId}"),
            "the baseline must keep hitting after the MTMV appeared: " + explainOf(agg))
    assertTrue(explainOf(agg).contains("spm_r27_mv_t(spm_r27_mv_t)"),
            "the replay must keep scanning the frozen source table: " + explainOf(agg))
    assertFalse(explainOf(agg).contains("spm_r27_mv chose"),
            "the MTMV must not be substituted into the replay: " + explainOf(agg))
    order_qt_r27_frozen_agg_with_mv """SELECT k, SUM(v) FROM spm_r27_mv_t GROUP BY k ORDER BY k"""

    // ==================== #2: the ambiguous per-occurrence swap is rejected ============
    sql """DROP TABLE IF EXISTS spm_r27_sj"""
    sql """
        CREATE TABLE spm_r27_sj (k INT)
        PARTITION BY RANGE(k) (
            PARTITION p1 VALUES LESS THAN (10),
            PARTITION p2 VALUES LESS THAN (20)
        )
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r27_sj VALUES (1), (11)"""

    test {
        sql 'CREATE GLOBAL BASELINE PLAN "SELECT a.k, b.k FROM spm_r27_sj PARTITION(p1) a CROSS JOIN spm_r27_sj PARTITION(p2) b" WITH "SELECT a.k, b.k FROM spm_r27_sj PARTITION(p2) a CROSS JOIN spm_r27_sj PARTITION(p1) b"'
        exception "DIFFERENT occurrences"
    }
    // the aligned pair keeps working and keeps reading exactly its own partitions
    String aligned = "SELECT a.k, b.k FROM spm_r27_sj PARTITION(p1) a CROSS JOIN spm_r27_sj" +
            " PARTITION(p2) b ORDER BY a.k, b.k"
    long alignedId = createBaseline(aligned, aligned)
    assertTrue(explainOf(aligned).contains("SPM baseline hit: id=${alignedId}"),
            "the aligned self-join baseline must be hit: " + explainOf(aligned))
    order_qt_r27_pinned_self_join """SELECT a.k, b.k FROM spm_r27_sj PARTITION(p1) a CROSS JOIN spm_r27_sj PARTITION(p2) b ORDER BY a.k, b.k"""

    // a pinned join of two DIFFERENT tables whose column names collide: every column is
    // referenced through the occurrence alias, so both pinned scans are wrapped
    sql """DROP TABLE IF EXISTS spm_r27_a"""
    sql """DROP TABLE IF EXISTS spm_r27_b"""
    sql """
        CREATE TABLE spm_r27_a (k INT)
        PARTITION BY RANGE(k) (PARTITION p1 VALUES LESS THAN (10))
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        CREATE TABLE spm_r27_b (k INT)
        PARTITION BY RANGE(k) (PARTITION p1 VALUES LESS THAN (10))
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r27_a VALUES (1), (2)"""
    sql """INSERT INTO spm_r27_b VALUES (1), (3)"""
    String pinnedJoin = "SELECT a.k, b.k FROM spm_r27_a PARTITION(p1) a CROSS JOIN spm_r27_b" +
            " PARTITION(p1) b ORDER BY a.k, b.k"
    long pinnedJoinId = createBaseline(pinnedJoin, pinnedJoin)
    assertTrue(explainOf(pinnedJoin).contains("SPM baseline hit: id=${pinnedJoinId}"),
            "the pinned two-table baseline must be hit: " + explainOf(pinnedJoin))
    order_qt_r27_pinned_join """SELECT a.k, b.k FROM spm_r27_a PARTITION(p1) a CROSS JOIN spm_r27_b PARTITION(p1) b ORDER BY a.k, b.k"""

    assertEquals(3, ownBaselines().size(),
            "only the two aligned pairs and the aggregate may exist: " + ownBaselines())
}
