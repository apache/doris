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

suite("test_spm_review_round88", "spm") {

    // Round-88 review #1: every ARRAY scalar function declaring `retArgType(0)` forwards its
    // input's element type, so an all-NULL container UNDER it keeps a NullType operand. Only
    // array_sort / array_distinct were recognized, so
    //
    //   SELECT k, k > 0 OR element_at(array_reverse_sort(array(NULL)), 1) AS c FROM t
    //
    // froze as `SELECT k, ((k > _spm_const_var(1)) OR NULL) AS c ...`: the Boolean consumer's
    // analysis replaced the whole wrapper call (and every marker inside it) with a NULL literal,
    // and the matching `array(TRUE)` query - which extracts TRUE - still replayed NULL for
    // k <= 0. The classifier now covers the whole family (array_reverse_sort, the slice / set
    // family, reverse, ...), see TYPE_PRESERVING_ARRAY_CALLS.
    //
    // Each case is driven end to end: the baseline freezes the wrapper, and a VALUE VARIANT of
    // the same text (the container element TRUE instead of NULL) must be substituted into the
    // SAME placeholder and change the result.

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    sql """DROP TABLE IF EXISTS spm_r88_t"""
    sql """
        CREATE TABLE spm_r88_t (k INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r88_t VALUES (-1), (0), (2)"""

    def dropOwnBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r88_") }
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

    // ==================== #1: array_reverse_sort ====================
    String reverseText = "SELECT k, k > 0 OR element_at(array_reverse_sort(array(NULL)), 1) AS c" +
            " FROM spm_r88_t ORDER BY k"
    String reversePlan = planSqlOf(createBaseline(reverseText))
    // the frozen SQL must keep the wrapper call and its container placeholder, not a bare NULL
    assertTrue(reversePlan.contains("array_reverse_sort("),
            "the frozen SQL must keep the type-preserving wrapper: " + reversePlan)
    assertTrue(!reversePlan.contains(") OR NULL)"),
            "the Boolean consumer must not erase the wrapper call: " + reversePlan)
    // element_at([NULL], 1) is NULL, so c follows "k > 0 OR NULL"
    order_qt_r88_reverse_sort """SELECT k, k > 0 OR element_at(array_reverse_sort(array(NULL)), 1) AS c
        FROM spm_r88_t ORDER BY k"""
    // the variant substitutes TRUE into the same placeholder: every row is TRUE
    order_qt_r88_reverse_sort_variant """SELECT k, k > 0 OR element_at(array_reverse_sort(array(TRUE)), 1) AS c
        FROM spm_r88_t ORDER BY k"""

    // ==================== #1: the slice family ====================
    String sliceText = "SELECT k, k > 0 OR element_at(array_slice(array(NULL), 1, 2), 1) AS c" +
            " FROM spm_r88_t ORDER BY k"
    String slicePlan = planSqlOf(createBaseline(sliceText))
    assertTrue(slicePlan.contains("array_slice("),
            "the frozen SQL must keep the slice wrapper: " + slicePlan)
    order_qt_r88_array_slice """SELECT k, k > 0 OR element_at(array_slice(array(NULL), 1, 2), 1) AS c
        FROM spm_r88_t ORDER BY k"""
    order_qt_r88_array_slice_variant """SELECT k, k > 0 OR element_at(array_slice(array(TRUE), 1, 2), 1) AS c
        FROM spm_r88_t ORDER BY k"""

    // ==================== #1: a second wrapper of the family ====================
    // array_compact declares the same retArgType(0), so the classifier must cover it too (the
    // round-85/86 code only knew array_sort / array_distinct).
    String compactText = "SELECT k, k > 0 OR element_at(array_compact(array(NULL)), 1) AS c" +
            " FROM spm_r88_t ORDER BY k"
    String compactPlan = planSqlOf(createBaseline(compactText))
    assertTrue(compactPlan.contains("array_compact("),
            "the frozen SQL must keep the compact wrapper: " + compactPlan)
    order_qt_r88_array_compact """SELECT k, k > 0 OR element_at(array_compact(array(NULL)), 1) AS c
        FROM spm_r88_t ORDER BY k"""
    order_qt_r88_array_compact_variant """SELECT k, k > 0 OR element_at(array_compact(array(TRUE)), 1) AS c
        FROM spm_r88_t ORDER BY k"""

    dropOwnBaselines()
}
