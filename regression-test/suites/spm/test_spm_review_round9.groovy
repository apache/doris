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

suite("test_spm_review_round9", "spm") {

    // Ninth review round: end-to-end checks for
    //  - the one-row LIMIT sibling rule (LIMIT 1 / LIMIT 0 replay)
    //  - replay-time context expressions being rejected at CREATE time
    //  - the persisted creation sql_mode (PIPES_AS_CONCAT)
    //  - reserved identifiers being quoted in the frozen SQL

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    sql """DROP TABLE IF EXISTS spm_r9_t"""
    sql """DROP TABLE IF EXISTS spm_r9_rsv"""
    sql """
        CREATE TABLE spm_r9_t (k INT, s1 VARCHAR(10), s2 VARCHAR(10))
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r9_t VALUES (3, 'x', 'y'), (2, 'a', 'c'), (1, 'a', 'b')"""
    sql """
        CREATE TABLE spm_r9_rsv (k INT, `from` INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r9_rsv VALUES (2, 8), (1, 7)"""

    def ownBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r9_") }
    }
    def dropOwnBaselines = {
        ownBaselines().each { row ->
            sql """DROP BASELINE PLAN ${row[0]}"""
        }
    }
    dropOwnBaselines()
    assertEquals(0, ownBaselines().size(), "no spm_r9_ baseline should be left after cleanup")

    def explainOf = { String query -> sql("""EXPLAIN ${query}""").toString() }
    def createBaseline = { String text ->
        (sql('CREATE GLOBAL BASELINE PLAN "' + text + '" WITH "' + text + '"')[0][0] as Long)
    }

    // ==================== #1: the one-row LIMIT must survive the freeze ====================
    // ELIMINATE_LIMIT_ON_ONE_ROW_RELATION is registered in the same rule class as
    // ELIMINATE_LIMIT; excluding only the latter froze "SELECT 1 LIMIT 1" as the bare
    // one-row child, and "SELECT 1 LIMIT 0" (same no-offset LIMIT ? digest) then replayed
    // that plan and returned one row instead of none.
    String limitBindSql = "SELECT 1 AS x LIMIT 1"
    long limitId = createBaseline(limitBindSql)
    String limitPlanSql = sql(
            """SELECT plan_sql FROM __internal_schema.spm_baselines WHERE id = ${limitId}""")[0][0].toString()
    assertTrue(limitPlanSql.toUpperCase().contains("LIMIT"),
            "the frozen one-row query must keep its LIMIT: " + limitPlanSql)

    String zeroSql = "SELECT 1 AS x LIMIT 0"
    assertTrue(explainOf(zeroSql).contains("SPM baseline hit: id=${limitId}"),
            "LIMIT 0 shares the digest and must hit the LIMIT 1 baseline: " + explainOf(zeroSql))
    List<List<Object>> zeroRows = sql(zeroSql)
    assertTrue(zeroRows.isEmpty(),
            "the replay must honor the user's LIMIT 0 instead of returning one row: " + zeroRows)

    order_qt_limit_zero_replay """SELECT 1 AS x LIMIT 0"""
    order_qt_limit_one_replay """SELECT 1 AS x LIMIT 1"""

    String limitTableSql = "SELECT k FROM spm_r9_t ORDER BY k LIMIT 1"
    long limitTableId = createBaseline(limitTableSql)
    order_qt_limit_table_replay """SELECT k FROM spm_r9_t ORDER BY k LIMIT 1"""
    assertTrue(explainOf(limitTableSql).contains("SPM baseline hit: id=${limitTableId}"),
            "the table LIMIT baseline must hit: " + explainOf(limitTableSql))

    // ==================== #12: replay-time context expressions are rejected ====================
    // current_user() / database() / ... survive parameterization and the creator-context
    // optimization resolves them to LITERALS that the frozen SQL persists, while matching
    // compares the original unbound tree - a global baseline would serve every other user
    // the creator's identity.
    test {
        sql 'CREATE GLOBAL BASELINE PLAN "SELECT current_user() AS u, k FROM spm_r9_t WHERE k = 1" WITH "SELECT current_user() AS u, k FROM spm_r9_t WHERE k = 1"'
        exception "replay-time context expressions"
    }
    test {
        sql 'CREATE GLOBAL BASELINE PLAN "SELECT database() AS d, k FROM spm_r9_t WHERE k = 1" WITH "SELECT database() AS d, k FROM spm_r9_t WHERE k = 1"'
        exception "replay-time context expressions"
    }
    test {
        sql 'CREATE GLOBAL BASELINE PLAN "SELECT connection_id() AS c, k FROM spm_r9_t WHERE k = 1" WITH "SELECT connection_id() AS c, k FROM spm_r9_t WHERE k = 1"'
        exception "replay-time context expressions"
    }

    // ==================== #13: the creation sql_mode is persisted and reused ====================
    // CREATE under PIPES_AS_CONCAT parses "a || b" as concat(a, b) and stores that digest.
    // The reload path re-parses the stored bindSql with the CREATION mode, so a CONCAT-mode
    // query still matches structurally after a refresh / restart (a default-mode rebuild
    // would produce a boolean Or and silently stop applying).
    sql """set sql_mode = 'PIPES_AS_CONCAT'"""
    try {
        String concatSql = "SELECT k FROM spm_r9_t WHERE (s1 || s2) = 'ac' ORDER BY k"
        long concatId = createBaseline(concatSql)
        assertTrue(explainOf(concatSql).contains("SPM baseline hit: id=${concatId}"),
                "the CONCAT-mode query must hit its baseline: " + explainOf(concatSql))
        List<List<Object>> concatRows = sql(concatSql)
        assertTrue(concatRows.size() == 1 && (concatRows.get(0).get(0) as int) == 2,
                "|| must stay concat(a, b) in the replayed plan: " + concatRows)
        order_qt_pipes_concat_replay """SELECT k FROM spm_r9_t WHERE (s1 || s2) = 'ac' ORDER BY k"""

        String concatMode = sql("""SELECT sql_mode FROM __internal_schema.spm_baselines WHERE id = ${concatId}""")[0][0].toString()
        assertTrue(!concatMode.isEmpty() && concatMode != "0",
                "the creation sql_mode must be persisted: " + concatMode)
    } finally {
        sql """set sql_mode = ''"""
    }

    // ==================== #11: reserved identifiers are quoted ====================
    String reservedSql = "SELECT `from` FROM spm_r9_rsv WHERE k = 1"
    long reservedId = createBaseline(reservedSql)
    String reservedPlanSql = sql(
            """SELECT plan_sql FROM __internal_schema.spm_baselines WHERE id = ${reservedId}""")[0][0].toString()
    assertTrue(reservedPlanSql.contains("`from`"),
            "a reserved keyword used as a column must stay quoted: " + reservedPlanSql)
    assertTrue(explainOf(reservedSql).contains("SPM baseline hit: id=${reservedId}"),
            "the reserved-column query must hit: " + explainOf(reservedSql))
    order_qt_reserved_column_replay """SELECT `from` FROM spm_r9_rsv WHERE k = 1"""

    // leave no baselines behind for other runs
    dropOwnBaselines()
    assertEquals(0, ownBaselines().size(), "all spm_r9_ baselines must be dropped")
}
