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

suite("test_spm_review_round65", "spm") {

    // Round-65 review comments.
    //
    // #1 SpmConstList was declared AlwaysNotNullable: a NOT NULL comparand against a
    //    NULL-CONTAINING IN list is NULLABLE (k IN (1, NULL) evaluates to NULL for a
    //    non-matching k), so the enabled NullableDependentExpressionRewrite folded a
    //    surrounding COALESCE(k IN (1, NULL), false) away while the frozen plan was
    //    decompiled - even the IDENTICAL replay returned NULL instead of false. The
    //    frozen plan text is pinned and the replayed result is compared against the
    //    direct execution (COALESCE and NVL shapes, plus a substituted list variant).
    //
    // #2 (declared derived-table column list: the parser does not accept the
    //    positional `FROM (...) s(x, y)` syntax, so the tree shape is pinned by
    //    SPMMatchingSafetyTest), #3 (nested WITH scope) and #5 (stars before an
    //    open-tail star) are HEADER-realignment fixes: the caller's JDBC labels are
    //    not part of the order_qt output, so the exact labels are pinned by
    //    SPMMatchingSafetyTest. The SQL surface below still replays every affected
    //    shape (hit + the same values as the direct execution with the fallback switch
    //    CLOSED, so a declined alignment surfaces as an error instead of silently
    //    re-running the original plan).
    //
    // #4 (cross-id same-key twins of an evicted hash) is a cache-miss ordering without
    //    a SQL surface; it is covered by BaselineManagerCacheTest.

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    sql """DROP TABLE IF EXISTS spm_r65_t"""
    sql """
        CREATE TABLE spm_r65_t (k INT, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r65_t VALUES (1, 10), (2, 20), (3, 30)"""
    sql """DROP TABLE IF EXISTS spm_r65_u"""
    sql """
        CREATE TABLE spm_r65_u (w INT)
        DUPLICATE KEY(w)
        DISTRIBUTED BY HASH(w) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r65_u VALUES (7), (8)"""

    def dropOwnBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r65_") }
                .each { row -> sql """DROP BASELINE PLAN ${row[0]}""" }
    }
    dropOwnBaselines()

    def createBaseline = { String text ->
        (sql('CREATE GLOBAL BASELINE PLAN "' + text + '" WITH "' + text + '"')[0][0] as Long)
    }
    def explainOf = { String query -> sql("""EXPLAIN ${query}""").toString() }
    def planSqlOf = { long id ->
        sql("""SELECT plan_sql FROM __internal_schema.spm_baselines WHERE id = ${id}""")[0][0].toString()
    }
    def directRows = { String query ->
        sql """set enable_spm_rewrite = false"""
        try {
            return sql(query)
        } finally {
            sql """set enable_spm_rewrite = true"""
        }
    }

    // ==================== #1: COALESCE over a NULL-containing IN list ====================
    String coalesceBind = "SELECT k, coalesce(k IN (1, NULL), false) AS c FROM spm_r65_t ORDER BY k"
    long coalesceId = createBaseline(coalesceBind)
    String coalescePlan = planSqlOf(coalesceId)
    assertTrue(coalescePlan.toLowerCase().contains("coalesce"),
            "the frozen projection must keep COALESCE: the IN list is nullable: " + coalescePlan)

    assertTrue(explainOf(coalesceBind).contains("SPM baseline hit: id=${coalesceId}"),
            "the identical query must replay its baseline: " + explainOf(coalesceBind))
    List<List<Object>> coalesceReplay = sql(coalesceBind)
    // k=2 and k=3 do not match the list, so k IN (1, NULL) is NULL and COALESCE must
    // report false - the pre-fix frozen plan reported NULL
    assertTrue(coalesceReplay[1][1] != null && coalesceReplay[2][1] != null,
            "COALESCE(k IN (1, NULL), false) must report false, not NULL: " + coalesceReplay)
    List<List<Object>> coalesceDirect = directRows(coalesceBind)
    assertTrue(coalesceReplay == coalesceDirect,
            "the replay must return exactly the direct result: replay=" + coalesceReplay
                    + " direct=" + coalesceDirect)
    order_qt_r65_coalesce_replay """SELECT k, coalesce(k IN (1, NULL), false) AS c FROM spm_r65_t ORDER BY k"""

    // the substituted list must decide the result (k=2 matches, k=1 and k=3 do not)
    String coalesceVariant = "SELECT k, coalesce(k IN (2, NULL), false) AS c FROM spm_r65_t ORDER BY k"
    assertTrue(explainOf(coalesceVariant).contains("SPM baseline hit: id=${coalesceId}"),
            "the list variant must replay the baseline: " + explainOf(coalesceVariant))
    List<List<Object>> variantReplay = sql(coalesceVariant)
    List<List<Object>> variantDirect = directRows(coalesceVariant)
    assertTrue(variantReplay == variantDirect,
            "the substituted list must return the direct result: replay=" + variantReplay
                    + " direct=" + variantDirect)
    order_qt_r65_coalesce_variant """SELECT k, coalesce(k IN (2, NULL), false) AS c FROM spm_r65_t ORDER BY k"""

    // the NVL shape of the same contract (NVL(k IN (1, NULL), false))
    String nvlBind = "SELECT k, nvl(k IN (1, NULL), false) AS c FROM spm_r65_t ORDER BY k"
    long nvlId = createBaseline(nvlBind)
    String nvlPlan = planSqlOf(nvlId).toLowerCase()
    assertTrue(nvlPlan.contains("nvl") || nvlPlan.contains("ifnull")
                    || nvlPlan.contains("coalesce"),
            "the frozen projection must keep the null-fallback function: " + nvlPlan)
    assertTrue(explainOf(nvlBind).contains("SPM baseline hit: id=${nvlId}"),
            "the identical NVL query must replay its baseline: " + explainOf(nvlBind))
    List<List<Object>> nvlReplay = sql(nvlBind)
    assertTrue(nvlReplay[1][1] != null && nvlReplay[2][1] != null,
            "NVL(k IN (1, NULL), false) must report false, not NULL: " + nvlReplay)
    List<List<Object>> nvlDirect = directRows(nvlBind)
    assertTrue(nvlReplay == nvlDirect,
            "the NVL replay must return exactly the direct result: replay=" + nvlReplay
                    + " direct=" + nvlDirect)
    order_qt_r65_nvl_replay """SELECT k, nvl(k IN (1, NULL), false) AS c FROM spm_r65_t ORDER BY k"""

    // ==================== #3/#5: replay shapes of the header realignment ====================
    // (#3) a nested WITH whose inner alias shadows the outer one for its own subtree only
    String nestedWith = "WITH c AS (SELECT k AS a FROM spm_r65_t)" +
            " SELECT * FROM (WITH c AS (SELECT v AS b FROM spm_r65_t) SELECT * FROM c) s" +
            " CROSS JOIN c ORDER BY 1, 2"
    long nestedWithId = createBaseline(nestedWith)
    assertTrue(explainOf(nestedWith).contains("SPM baseline hit: id=${nestedWithId}"),
            "the nested WITH must replay its baseline: " + explainOf(nestedWith))
    List<List<Object>> nestedReplay = sql(nestedWith)
    List<List<Object>> nestedDirect = directRows(nestedWith)
    assertTrue(nestedReplay == nestedDirect,
            "the nested WITH replay must return exactly the direct result: replay="
                    + nestedReplay + " direct=" + nestedDirect)
    order_qt_r65_nested_with """WITH c AS (SELECT k AS a FROM spm_r65_t)
                SELECT * FROM (WITH c AS (SELECT v AS b FROM spm_r65_t) SELECT * FROM c) s
                CROSS JOIN c ORDER BY 1, 2"""

    // (#5) two stars before the unknown-width star of a base table
    String multiStarBind = "SELECT s.*, spm_r65_u.*, 9 AS z" +
            " FROM (SELECT k + 1, v + 1 FROM spm_r65_t) s CROSS JOIN spm_r65_u ORDER BY 1, 2, 3, 4"
    long multiStarId = createBaseline(multiStarBind)
    String multiStarVariant = "SELECT s.*, spm_r65_u.*, 9 AS z" +
            " FROM (SELECT k + 2, v + 2 FROM spm_r65_t) s CROSS JOIN spm_r65_u ORDER BY 1, 2, 3, 4"
    assertTrue(explainOf(multiStarVariant).contains("SPM baseline hit: id=${multiStarId}"),
            "the multi-star variant must replay the baseline: " + explainOf(multiStarVariant))
    List<List<Object>> multiStarReplay = sql(multiStarVariant)
    List<List<Object>> multiStarDirect = directRows(multiStarVariant)
    assertTrue(multiStarReplay == multiStarDirect,
            "the multi-star replay must compute the variant values: replay=" + multiStarReplay
                    + " direct=" + multiStarDirect)
    order_qt_r65_multi_star """SELECT s.*, spm_r65_u.*, 9 AS z
                FROM (SELECT k + 2, v + 2 FROM spm_r65_t) s CROSS JOIN spm_r65_u ORDER BY 1, 2, 3, 4"""

    dropOwnBaselines()
    sql """DROP TABLE IF EXISTS spm_r65_t"""
    sql """DROP TABLE IF EXISTS spm_r65_u"""
}
