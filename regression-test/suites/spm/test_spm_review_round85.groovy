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

suite("test_spm_review_round85", "spm") {

    // End-to-end checks for the round-85 review comments:
    //
    // #1 GROUPING SETS is a consumer of the block's OUTPUT aliases exactly like a GROUP BY, so a
    //    manual plan that permutes the labels while both texts spell the same sets subtotals
    //    ANOTHER expression - CREATE must refuse it.
    // #2 a MATCH's ANALYZER decides how the pattern is tokenized, so it belongs to the
    //    resolved-consumer identity: the same permutation must be refused.
    // #3 an ALIAS FUNCTION body is inlined during analysis, so a context expression inside it
    //    (current_user() here) has to be rejected before the capture folds the CREATOR's value.
    // #4 a multi-column COUNT(DISTINCT a, b) mixed with a plain SUM keeps the user's tuple
    //    deduplication; the frozen SQL must return the direct result.
    // #5 an EMPTY array container is ARRAY<NULL_TYPE> as well, so `element_at([], 1)` keeps its
    //    markers and the `element_at([TRUE], 1)` variant replays TRUE instead of NULL.
    // #6 the ASSERT_ROWS relation survives the result-sink relabelling, so the single-row
    //    assertion is still raised on replay.

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    sql """DROP TABLE IF EXISTS spm_r85_t"""
    sql """
        CREATE TABLE spm_r85_t (k INT, a INT, b INT, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    // (1,1,7) twice and (1,2,7): count(DISTINCT a, b) = 2 while a plain count of the rows is 3,
    // and the shared b = 7 makes a SCALAR distinct count return 1.
    sql """INSERT INTO spm_r85_t VALUES (1, 1, 7, 10), (1, 1, 7, 20), (1, 2, 7, 30)"""

    // k <= 0 makes the left operand false, so the element_at value decides the OR.
    sql """DROP TABLE IF EXISTS spm_r85_arr"""
    sql """
        CREATE TABLE spm_r85_arr (k INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r85_arr VALUES (-1), (0), (2)"""

    sql """DROP FUNCTION IF EXISTS spm_r85_ctx(VARCHAR(10))"""
    sql """CREATE ALIAS FUNCTION spm_r85_ctx(VARCHAR(10)) WITH PARAMETER(x)
        AS concat(current_user(), x)"""

    def dropOwnBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r85_") }
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

    // ==================== #1: the GROUPING SETS consumer ====================
    test {
        sql '''CREATE GLOBAL BASELINE PLAN
            "SELECT k AS x, v AS y, count(*) AS n FROM spm_r85_t GROUP BY GROUPING SETS((x, y), (x))"
            WITH
            "SELECT k AS y, v AS x, count(*) AS n FROM spm_r85_t GROUP BY GROUPING SETS((x, y), (x))"'''
        exception "GROUPING SETS"
    }

    // ==================== #2: the MATCH analyzer ====================
    test {
        sql '''CREATE GLOBAL BASELINE PLAN
            "SELECT k, k MATCH_ANY \'hello\' USING ANALYZER standard AS x,
                    k MATCH_ANY \'hello\' USING ANALYZER none AS y FROM spm_r85_t ORDER BY x"
            WITH
            "SELECT k, k MATCH_ANY \'hello\' USING ANALYZER standard AS y,
                    k MATCH_ANY \'hello\' USING ANALYZER none AS x FROM spm_r85_t ORDER BY x"'''
        exception "resolve to different expressions"
    }

    // ==================== #3: the resolved ALIAS body ====================
    test {
        sql '''CREATE GLOBAL BASELINE PLAN
            "SELECT spm_r85_ctx(k) AS c, k FROM spm_r85_t WHERE k = 1"
            WITH
            "SELECT spm_r85_ctx(k) AS c, k FROM spm_r85_t WHERE k = 1"'''
        exception "replay-time context expressions"
    }

    // ==================== #4: the multi-key distinct count ====================
    String multiKeyBind = "SELECT k, count(DISTINCT a, b) AS c, sum(v) AS s FROM spm_r85_t" +
            " GROUP BY k"
    long multiKeyId = createBaseline(multiKeyBind)
    assertTrue(explainOf(multiKeyBind).contains("SPM baseline hit: id=${multiKeyId}"),
            "the identical multi-key query must replay its baseline: " + explainOf(multiKeyBind))
    List<List<Object>> multiKeyReplay = sql(multiKeyBind)
    List<List<Object>> multiKeyDirect = directRows(multiKeyBind)
    assertTrue(multiKeyReplay == multiKeyDirect,
            "count(DISTINCT a, b) must replay the tuple count and the plain SUM: replay=" +
                    multiKeyReplay + " direct=" + multiKeyDirect)
    order_qt_r85_multi_key_count """SELECT k, count(DISTINCT a, b) AS c, sum(v) AS s
        FROM spm_r85_t GROUP BY k ORDER BY k"""

    // ==================== #5: the EMPTY array container ====================
    String emptyArrayBind = "SELECT k, k > 0 OR element_at([], 1) AS c FROM spm_r85_arr ORDER BY k"
    long emptyArrayId = createBaseline(emptyArrayBind)
    assertTrue(explainOf(emptyArrayBind).contains("SPM baseline hit: id=${emptyArrayId}"),
            "the identical empty-array query must replay its baseline: " +
                    explainOf(emptyArrayBind))
    String emptyArrayVariant = "SELECT k, k > 0 OR element_at([TRUE], 1) AS c" +
            " FROM spm_r85_arr ORDER BY k"
    List<List<Object>> emptyArrayReplay = sql(emptyArrayVariant)
    List<List<Object>> emptyArrayDirect = directRows(emptyArrayVariant)
    assertTrue(emptyArrayReplay == emptyArrayDirect,
            "the [TRUE] variant must replay its own value, not the stripped NULL: replay=" +
                    emptyArrayReplay + " direct=" + emptyArrayDirect)
    order_qt_r85_empty_array_variant """SELECT k, k > 0 OR element_at([TRUE], 1) AS c
        FROM spm_r85_arr ORDER BY k"""

    // ==================== #6: the ASSERT_ROWS boundary ====================
    String assertText = "SELECT * FROM ASSERT_ROWS" +
            " (SELECT k + 1 AS x FROM spm_r85_t WHERE k > 0) a"
    long assertId = createBaseline(assertText)
    String assertPlanSql = sql(
            """SELECT plan_sql FROM __internal_schema.spm_baselines WHERE id = ${assertId}""")[0][0]
            .toString()
    assertTrue(assertPlanSql.contains("ASSERT_ROWS"),
            "the frozen SQL must keep the single-row assertion: " + assertPlanSql)
    assertTrue(explainOf(assertText).contains("SPM baseline hit: id=${assertId}"),
            "the identical assert query must replay its baseline: " + explainOf(assertText))
    // the three rows violate the assertion in BOTH paths
    test {
        sql """SELECT * FROM ASSERT_ROWS
            (SELECT k + 1 AS x FROM spm_r85_t WHERE k > 0) a"""
        exception "AssertOperator"
    }
}
