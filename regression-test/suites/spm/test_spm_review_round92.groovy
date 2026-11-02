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

suite("test_spm_review_round92", "spm") {

    // Round-92 review #1: `array_repeat(x, n)` declares ARRAY<FollowToAnyDataType(0)> - its
    // ELEMENT type comes from its FIRST argument - so `array_repeat(NULL, 1)` is an all-NULL
    // array producer. It was missing from the all-NULL container classifier, so the `element_at`
    // operand stayed a NullType producer and the Boolean consumer replaced it with a typed NULL
    // and DROPPED its markers. The value variant `array_repeat(TRUE, 1)` still matched the
    // value-free digest, but the frozen replay had no marker left to substitute and returned NULL
    // where the user's own query returns TRUE.
    //
    // So the frozen SQL must keep one placeholder per literal (four here: the 0 of `k > 0`, the
    // NULL element, the repeat count 1 and the array index 1), and the replay of the value
    // variant must follow the substituted TRUE instead of NULL.

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    sql """DROP TABLE IF EXISTS spm_r92_t"""
    sql """
        CREATE TABLE spm_r92_t (k INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    // 0 and a negative row matter: "k > 0 OR NULL" is NULL there, while "k > 0 OR TRUE" is TRUE.
    sql """INSERT INTO spm_r92_t VALUES (-1), (0), (1), (2)"""

    def dropOwnBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r92_") }
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

    String nullElementText = "SELECT k, k > 0 OR element_at(array_repeat(NULL, 1), 1) AS c" +
            " FROM spm_r92_t ORDER BY k"
    String nullElementPlan = planSqlOf(createBaseline(nullElementText))
    // One placeholder per literal, INCLUDING the NULL element: the erased marker is exactly the
    // replay that returned NULL for the value variant.
    assertTrue(nullElementPlan.count("_spm_const_var(") == 4,
            "every literal must keep its placeholder: " + nullElementPlan)
    assertTrue(nullElementPlan.contains("array_repeat("),
            "the frozen SQL must keep the array_repeat producer: " + nullElementPlan)

    // element_at(array_repeat(NULL, 1), 1) is NULL, so c follows "k > 0 OR NULL"
    order_qt_r92_array_repeat_null """SELECT k, k > 0 OR element_at(array_repeat(NULL, 1), 1) AS c
        FROM spm_r92_t ORDER BY k"""
    // the VALUE variant of the SAME value-free digest: it must replay TRUE for every row
    order_qt_r92_array_repeat_value """SELECT k, k > 0 OR element_at(array_repeat(TRUE, 1), 1) AS c
        FROM spm_r92_t ORDER BY k"""

    dropOwnBaselines()
}
