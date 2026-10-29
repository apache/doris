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

suite("test_spm_review_round87", "spm") {

    // Round-87 defect: a LITERAL projected by a derived table lost its own column and took the
    // place of the REAL column beside it, so the frozen plan replayed the captured constant for
    // every row.
    //
    // LogicalPlanBuilder builds a BOUND Alias for a literal select item (`SELECT 7 AS b`), and
    // that alias carries an ExprId from the statement context the text was PARSED under. The SPM
    // capture plans the parsed tree on a FRESH nested statement context, whose own generators
    // restart at 0: the table columns bound below got the same small ids, and since
    // SlotReference#equals compares ONLY the exprId, every Map<Slot, Expression> substitution
    // (projection merging / column pruning) conflated the derived constant with the real column.
    // `SELECT k, k > 0 OR b = 7 AS c FROM (SELECT k, 7 AS b FROM t) x` froze as
    // `SELECT _spm_const_var(1) AS k, ...` and replayed `7` for every k.
    //
    // The same mechanism erased a derived CONTAINER literal: `(SELECT k, [NULL] AS a FROM t) x`
    // froze with the array in the position of the element_at result, and the replay failed with
    // "Cannot compare two values with different data types: [NULL] (ARRAY<NULL>) vs 0 (INT)".
    //
    // Both shapes are exercised here end to end, together with a VALUE VARIANT of each text: the
    // placeholders must still be substituted (the freeze keeps working), while the real columns
    // keep their own values.

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    sql """DROP TABLE IF EXISTS spm_r87_t"""
    sql """
        CREATE TABLE spm_r87_t (k INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r87_t VALUES (-1), (0), (2)"""

    def dropOwnBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r87_") }
                .each { row -> sql """DROP BASELINE PLAN ${row[0]}""" }
    }
    dropOwnBaselines()

    def createBaseline = { String text ->
        (sql('CREATE GLOBAL BASELINE PLAN "' + text + '" WITH "' + text + '"')[0][0] as Long)
    }
    def planSqlOf = { long id ->
        sql("""SELECT plan_sql FROM __internal_schema.spm_baselines WHERE id = ${id}""")[0][0]
                .toString()
    }

    // ==================== #1: a derived SCALAR literal ====================
    String scalarText = "SELECT k, k > 0 OR b = 7 AS c" +
            " FROM (SELECT k, 7 AS b FROM spm_r87_t) x ORDER BY k"
    long scalarId = createBaseline(scalarText)
    String scalarPlan = planSqlOf(scalarId)
    // The k output must stay the TABLE column. A placeholder may appear only where the user's
    // literal was: the frozen text that put `_spm_const_var(..) AS k` there replayed the captured
    // constant as the whole k column.
    assertTrue(!scalarPlan.matches("(?s).*_spm_const_var\\(\\d+\\) AS `?k`?.*"),
            "the k output must not be a placeholder: " + scalarPlan)
    // the constant really is a placeholder (the freeze did happen, not a raw-planSql fallback)
    assertTrue(scalarPlan.contains("_spm_const_var("),
            "the frozen SQL must carry the placeholder: " + scalarPlan)
    // baseline: b = 7 => "k > 0 OR 7 = 7" is TRUE for every row, and k keeps its own values
    order_qt_r87_derived_scalar """SELECT k, k > 0 OR b = 7 AS c
        FROM (SELECT k, 7 AS b FROM spm_r87_t) x ORDER BY k"""
    // the SAME baseline with a DIFFERENT comparison value: 7 = 9 is FALSE, so c follows "k > 0"
    // (the derived 7 is still the SAME placeholder id, the comparison 9 is a different one)
    order_qt_r87_derived_scalar_variant """SELECT k, k > 0 OR b = 9 AS c
        FROM (SELECT k, 7 AS b FROM spm_r87_t) x ORDER BY k"""

    // ==================== #2: a derived CONTAINER literal ====================
    String containerText = "SELECT k, k > 0 OR element_at(a, 1) AS c" +
            " FROM (SELECT k, [NULL] AS a FROM spm_r87_t) x ORDER BY k"
    long containerId = createBaseline(containerText)
    String containerPlan = planSqlOf(containerId)
    // the container literal stays a placeholder in the element_at argument, and the k output is
    // still the TABLE column: the capture used to fold the array into k's position, and the
    // replay then compared an ARRAY<NULL> against 0.
    assertTrue(containerPlan.contains("element_at("),
            "the frozen SQL must read the derived container through element_at: " + containerPlan)
    assertTrue(!containerPlan.matches("(?s).*_spm_const_var\\(\\d+\\) AS `?k`?.*"),
            "the k output must not be a placeholder: " + containerPlan)
    // element_at([NULL], 1) is NULL, so c follows "k > 0 OR NULL"
    order_qt_r87_derived_container """SELECT k, k > 0 OR element_at(a, 1) AS c
        FROM (SELECT k, [NULL] AS a FROM spm_r87_t) x ORDER BY k"""
    // the variant substitutes a NON-NULL element into the same placeholder
    order_qt_r87_derived_container_variant """SELECT k, k > 0 OR element_at(a, 1) AS c
        FROM (SELECT k, [TRUE] AS a FROM spm_r87_t) x ORDER BY k"""

    dropOwnBaselines()
}
