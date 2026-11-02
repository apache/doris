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

suite("test_spm_review_round66", "spm") {

    // Round-66 review comments on the planner side:
    //
    // #1 a PROJECTED all-NULL CASE reaches its Boolean consumer through a derived-table
    //    output slot. Its NULL markers used to stay NullType, the derived slot stayed
    //    NullType, and the consumer's OR binding replaced the whole slot with a typed
    //    constant NULL while the frozen plan was optimized - the matching ELSE TRUE
    //    variant then returned NULL instead of TRUE (pinned end to end below).
    // #2 a QUALIFIED EXCEPT entry (`* EXCEPT(u.k)`) names a RELATION; the name-based
    //    removal deleted the derived prefix's same-named column and the replay reported
    //    [z, z, q]. Covered exactly by SPMMatchingSafetyTest; the SQL surface below
    //    replays the shape (hit + direct-equal values, fallback CLOSED).
    // #3 a WITH nested inside a derived relation shadows the outer binding for ITS OWN
    //    subtree; the name-keyed recursion set declared the different inner binding
    //    recursive and the baseline could not be used at all.
    // #4 a bare integer GROUP BY key is an ORDINAL, not the SELECT item's literal value;
    //    parameterizing it also made the SELECT literal reuse the same placeholder, so
    //    `SELECT 2 AS c ... GROUP BY 1` never hit the `SELECT 1 AS c ...` baseline.
    // The five BaselineManager items are cache-miss / ordering protocols without a SQL
    // surface: they are covered by SPMRound72ManagerTest and SPMRound71ManagerTest.

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    sql """DROP TABLE IF EXISTS spm_r66_t"""
    sql """
        CREATE TABLE spm_r66_t (k INT, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    // k = -1 makes k > 0 false: the CASE row decides the OR.
    sql """INSERT INTO spm_r66_t VALUES (-1, 10), (0, 20), (2, 30)"""

    sql """DROP TABLE IF EXISTS spm_r66_u"""
    sql """
        CREATE TABLE spm_r66_u (k INT, q INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r66_u VALUES (1, 70), (2, 80)"""

    def dropOwnBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r66_") }
                .each { row -> sql """DROP BASELINE PLAN ${row[0]}""" }
    }
    dropOwnBaselines()

    def createBaseline = { String text ->
        (sql('CREATE GLOBAL BASELINE PLAN "' + text + '" WITH "' + text + '"')[0][0] as Long)
    }
    def explainOf = { String query -> sql("""EXPLAIN ${query}""").toString() }
    def directRows = { String query ->
        sql """set enable_spm_rewrite = false"""
        try {
            return sql(query)
        } finally {
            sql """set enable_spm_rewrite = true"""
        }
    }

    // ==================== #1: the projected all-NULL CASE keeps its markers ====================
    String caseBind = "SELECT k, k > 0 OR n AS c" +
            " FROM (SELECT k, CASE WHEN k = 0 THEN NULL ELSE NULL END AS n FROM spm_r66_t) s" +
            " ORDER BY k"
    long caseId = createBaseline(caseBind)
    assertTrue(explainOf(caseBind).contains("SPM baseline hit: id=${caseId}"),
            "the identical query must replay its baseline: " + explainOf(caseBind))
    // The ELSE TRUE variant: for k = -1 the captured ELSE NULL gave NULL, the user's
    // TRUE must win. The pre-fix frozen projection was `k > ... OR NULL` (the derived
    // slot had been replaced), so the replay returned NULL.
    String caseVariant = "SELECT k, k > 0 OR n AS c" +
            " FROM (SELECT k, CASE WHEN k = 0 THEN NULL ELSE TRUE END AS n FROM spm_r66_t) s" +
            " ORDER BY k"
    assertTrue(explainOf(caseVariant).contains("SPM baseline hit: id=${caseId}"),
            "the CASE variant must replay the baseline: " + explainOf(caseVariant))
    List<List<Object>> caseReplay = sql(caseVariant)
    assertTrue(caseReplay[0][1] != null,
            "k = -1: the ELSE TRUE variant must report TRUE, not NULL: " + caseReplay)
    List<List<Object>> caseDirect = directRows(caseVariant)
    assertTrue(caseReplay == caseDirect,
            "the replay must return exactly the direct result: replay=" + caseReplay
                    + " direct=" + caseDirect)
    order_qt_r66_case_variant """SELECT k, k > 0 OR n AS c
                FROM (SELECT k, CASE WHEN k = 0 THEN NULL ELSE TRUE END AS n FROM spm_r66_t) s
                ORDER BY k"""

    // ==================== #2: a qualified EXCEPT keeps the derived prefix ====================
    String exceptBind = "SELECT * EXCEPT(u.k)" +
            " FROM (SELECT k, v + 1 AS z FROM spm_r66_t) d CROSS JOIN spm_r66_u u" +
            " ORDER BY 1, 2, 3"
    long exceptId = createBaseline(exceptBind)
    assertTrue(explainOf(exceptBind).contains("SPM baseline hit: id=${exceptId}"),
            "the qualified EXCEPT replay must hit its baseline: " + explainOf(exceptBind))
    List<List<Object>> exceptReplay = sql(exceptBind)
    List<List<Object>> exceptDirect = directRows(exceptBind)
    assertTrue(exceptReplay == exceptDirect,
            "the replay must return exactly the direct result: replay=" + exceptReplay
                    + " direct=" + exceptDirect)
    order_qt_r66_except """SELECT * EXCEPT(spm_r66_u.k)
                FROM (SELECT k, v + 1 AS z FROM spm_r66_t) d CROSS JOIN spm_r66_u spm_r66_u
                ORDER BY 1, 2, 3"""

    // ==================== #3: nested WITH shadowing keeps its own binding ====================
    String nestedWith = "WITH c AS (WITH c AS (SELECT k + 1 AS x FROM spm_r66_t)" +
            " SELECT * FROM c) SELECT * FROM c ORDER BY 1"
    long nestedId = createBaseline(nestedWith)
    assertTrue(explainOf(nestedWith).contains("SPM baseline hit: id=${nestedId}"),
            "the nested shadowing query must replay its own baseline: " + explainOf(nestedWith))
    List<List<Object>> nestedReplay = sql(nestedWith)
    List<List<Object>> nestedDirect = directRows(nestedWith)
    assertTrue(nestedReplay == nestedDirect,
            "the nested WITH replay must return exactly the direct result: replay="
                    + nestedReplay + " direct=" + nestedDirect)
    order_qt_r66_nested_with """WITH c AS (WITH c AS (SELECT k + 1 AS x FROM spm_r66_t)
                SELECT * FROM c) SELECT * FROM c ORDER BY 1"""

    // ==================== #4: a GROUP BY ordinal stays positional ====================
    String ordinalBind = "SELECT 1 AS c, count(*) AS n FROM spm_r66_t GROUP BY 1"
    long ordinalId = createBaseline(ordinalBind)
    String ordinalVariant = "SELECT 2 AS c, count(*) AS n FROM spm_r66_t GROUP BY 1"
    assertTrue(explainOf(ordinalVariant).contains("SPM baseline hit: id=${ordinalId}"),
            "the GROUP BY ordinal variant must hit the baseline: " + explainOf(ordinalVariant))
    List<List<Object>> ordinalReplay = sql(ordinalVariant)
    List<List<Object>> ordinalDirect = directRows(ordinalVariant)
    assertTrue(ordinalReplay == ordinalDirect,
            "the replay must return the variant's SELECT literal: replay=" + ordinalReplay
                    + " direct=" + ordinalDirect)
    order_qt_r66_group_by_ordinal """SELECT 2 AS c, count(*) AS n FROM spm_r66_t GROUP BY 1"""

    dropOwnBaselines()
    sql """DROP TABLE IF EXISTS spm_r66_t"""
    sql """DROP TABLE IF EXISTS spm_r66_u"""
}
