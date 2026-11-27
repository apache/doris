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

suite("test_spm_review_round22", "spm") {

    // End-to-end coverage for the frozen-SQL output-name
    // parsing and the runtime-context replay.
    //
    // Covered here:
    //  - a legal column named `a as b` (the name CONTAINS the alias token) must survive
    //    the decompiler's pass-through layers: an upper layer otherwise references the
    //    truncated "b`" and the frozen planSql no longer reparses at replay
    //  - a derived table's ORDER BY ... LIMIT (LogicalTopN) is part of the match: a
    //    variant asking for a DIFFERENT slice must not replay the captured limit
    //  - CREATE preserves user() as a call, rather than persisting the
    //    CREATOR's identity.
    //
    // #1/#2/#3/#5 are covered by BaselineManagerConcurrencyTest / SPMRound18SafetyTest /

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    def ownBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r22_") }
    }
    def dropOwnBaselines = {
        ownBaselines().each { row ->
            sql """DROP BASELINE PLAN ${row[0]}"""
        }
    }
    dropOwnBaselines()

    def explainOf = { String query -> sql("""EXPLAIN ${query}""").toString() }
    def planSqlOf = { long id ->
        sql("""SELECT plan_sql FROM __internal_schema.spm_baselines WHERE id = ${id}""")[0][0]
                .toString()
    }
    def createBaseline = { String text ->
        (sql('CREATE GLOBAL BASELINE PLAN "' + text + '" WITH "' + text + '"')[0][0] as Long)
    }
    def rowsWithRewriteOff = { String query ->
        sql """set enable_spm_rewrite = false"""
        List<List<Object>> rows = sql(query)
        sql """set enable_spm_rewrite = true"""
        rows
    }

    // ==================== setup ====================
    sql """DROP TABLE IF EXISTS spm_r22_t"""
    sql """
        CREATE TABLE spm_r22_t (k INT, `a as b` INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r22_t VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)"""

    sql """DROP TABLE IF EXISTS spm_r22_d"""
    sql """
        CREATE TABLE spm_r22_d (k INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r22_d VALUES (1), (2), (3)"""

    // ==================== #4: a quoted name holding the alias token ====================
    String winBind = "SELECT `a as b`, ROW_NUMBER() OVER (ORDER BY k) FROM spm_r22_t" +
            " WHERE `a as b` > 5 ORDER BY k"
    long winId = createBaseline(winBind)
    String winPlanSql = planSqlOf(winId)
    assertTrue(winPlanSql.contains("`a as b`"),
            "the frozen SQL must carry the quoted name itself: " + winPlanSql)

    String winReplay = "SELECT `a as b`, ROW_NUMBER() OVER (ORDER BY k) FROM spm_r22_t" +
            " WHERE `a as b` > 15 ORDER BY k"
    assertTrue(explainOf(winReplay).contains("SPM baseline hit: id=${winId}"),
            "the value-variant window query must replay the baseline: " + explainOf(winReplay))
    List<List<Object>> winRows = sql(winReplay)
    assertTrue(winRows == rowsWithRewriteOff(winReplay),
            "the replayed window output must equal the direct one: " + winRows)

    // ==================== #6: a derived-table TopN limit is part of the match ====================
    String topNBind = "SELECT x.k FROM (SELECT k FROM spm_r22_d ORDER BY k LIMIT 2) x" +
            " JOIN spm_r22_d y ON x.k = y.k ORDER BY x.k"
    long topNId = createBaseline(topNBind)
    assertTrue(explainOf(topNBind).contains("SPM baseline hit: id=${topNId}"),
            "the identical derived-table query must still match: " + explainOf(topNBind))

    String topNVariant = "SELECT x.k FROM (SELECT k FROM spm_r22_d ORDER BY k LIMIT 3) x" +
            " JOIN spm_r22_d y ON x.k = y.k ORDER BY x.k"
    assertFalse(explainOf(topNVariant).contains("SPM baseline hit: id=${topNId}"),
            "a different derived-table LIMIT must NOT replay the captured slice: "
                    + explainOf(topNVariant))
    List<List<Object>> topNRows = sql(topNVariant)
    assertTrue(topNRows == rowsWithRewriteOff(topNVariant) && topNRows == [[1], [2], [3]],
            "the variant must return its own (3-row) slice: " + topNRows)

    // USER() remains a runtime function in frozen SQL.
    String userQuery = "SELECT user() AS u FROM spm_r22_t"
    long userId = sql("CREATE GLOBAL BASELINE PLAN '${userQuery}' WITH '${userQuery}'")[0][0] as Long
    assertTrue(sql("SELECT plan_frozen FROM __internal_schema.spm_baselines WHERE id = ${userId}")[0][0]
            .toString() in ["1", "true"])
    assertTrue(explainOf(userQuery).contains("SPM baseline hit: id=${userId}"))
    assertEquals(rowsWithRewriteOff(userQuery), sql(userQuery))

    // leave no baselines behind for other runs
    // The DROP is durable and fenced, but a PARALLEL SPM suite can still observe a just-deleted
    // row through a snapshot read that started before the DROP published (the internal table's
    // delete visibility lags its commit), so retry the cleanup for a bounded time instead of
    // asserting on a single instant: this is a cleanliness guard, not a functional assertion.
    long cleanupDeadline = System.currentTimeMillis() + 10000
    while (!ownBaselines().isEmpty() && System.currentTimeMillis() < cleanupDeadline) {
        dropOwnBaselines()
        Thread.sleep(200)
    }
    assertEquals(0, ownBaselines().size(), "all spm_r22_ baselines must be dropped")
}
