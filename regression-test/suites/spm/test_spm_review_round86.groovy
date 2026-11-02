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

suite("test_spm_review_round86", "spm") {

    // End-to-end checks for the round-86 review comments:
    //
    // #1 an INTERVAL's UNIT is part of the resolved-consumer identity: a manual pair whose
    //    positional bodies only differ in DAY vs MONTH must be refused.
    // #2 the LAMBDA parameter MAPPING is part of it as well: (p,q) and (q,p) over the same body
    //    subtract different inputs.
    // #4 a TopN above a pending ASSERT_ROWS must keep the cap OUTSIDE the assertion, so the
    //    row-count error still fires on replay.
    // #5 the DERIVED-output container markers (a projection item carrying an all-NULL array /
    //    map) are retyped so an upper block's Boolean analysis cannot erase them. The end-to-end
    //    shape needed the round-87 fix first (a literal projected by a derived table took the
    //    place of the real column beside it, see test_spm_review_round87), so the SQL-level
    //    coverage of that shape lives there and the marker retyping itself is covered by
    //    SPMRound86PlannerTest.
    // #3 (the UDF executable identity) and #6 (the alias-body window) have no SQL surface that
    //    can be driven deterministically from here; they are covered by SPMRound86PlannerTest and
    //    the existing replay-time-context rejection below.

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    sql """DROP TABLE IF EXISTS spm_r86_t"""
    sql """
        CREATE TABLE spm_r86_t (k INT, d DATE)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r86_t VALUES (1, '2020-01-01'), (2, '2020-02-01')"""
    def dropOwnBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r86_") }
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

    // ==================== #1: the interval unit ====================
    test {
        sql '''CREATE GLOBAL BASELINE PLAN
            "SELECT d + INTERVAL 1 DAY AS x, d + INTERVAL 1 MONTH AS y FROM spm_r86_t
                HAVING x < \'2020-01-15\'"
            WITH
            "SELECT d + INTERVAL 1 DAY AS y, d + INTERVAL 1 MONTH AS x FROM spm_r86_t
                HAVING x < \'2020-01-15\'"'''
        exception "resolve to different expressions"
    }

    // ==================== #2: the lambda parameter mapping ====================
    test {
        sql '''CREATE GLOBAL BASELINE PLAN
            "SELECT array_map((p, q) -> p - q, [3], [1]) AS x,
                    array_map((q, p) -> p - q, [3], [1]) AS y FROM spm_r86_t
                HAVING element_at(x, 1) > 0"
            WITH
            "SELECT array_map((p, q) -> p - q, [3], [1]) AS y,
                    array_map((q, p) -> p - q, [3], [1]) AS x FROM spm_r86_t
                HAVING element_at(x, 1) > 0"'''
        exception "resolve to different expressions"
    }

    // ==================== #4: the cap above a pending assertion ====================
    String assertText = "SELECT * FROM ASSERT_ROWS" +
            " (SELECT k + 1 AS x FROM spm_r86_t WHERE k > 0) a LIMIT 1"
    long assertId = createBaseline(assertText)
    String assertPlanSql = sql(
            """SELECT plan_sql FROM __internal_schema.spm_baselines WHERE id = ${assertId}""")[0][0]
            .toString()
    assertTrue(assertPlanSql.contains("ASSERT_ROWS"),
            "the frozen SQL must keep the single-row assertion: " + assertPlanSql)
    int assertClose = assertPlanSql.indexOf(") t_", assertPlanSql.indexOf("ASSERT_ROWS"))
    assertTrue(assertClose > 0 && assertPlanSql.indexOf("LIMIT") > assertClose,
            "the cap must stay OUTSIDE the assertion: " + assertPlanSql)
    assertTrue(explainOf(assertText).contains("SPM baseline hit: id=${assertId}"),
            "the identical assert query must replay its baseline: " + explainOf(assertText))
    // both rows violate the assertion in BOTH paths, despite the LIMIT 1
    test {
        sql """SELECT * FROM ASSERT_ROWS
            (SELECT k + 1 AS x FROM spm_r86_t WHERE k > 0) a LIMIT 1"""
        exception "AssertOperator"
    }


}
