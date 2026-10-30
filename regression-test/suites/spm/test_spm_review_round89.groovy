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

suite("test_spm_review_round89", "spm") {

    // Round-89 review #1: a SUBQUERY must never reach the frozen SQL as a function call.
    //
    // SPMExprSqlBuilder has no SQL rendering for a subquery - it emits the structural digest
    // markers `INSUBQUERY(compare)[...]` / `EXISTS[...]` / `SCALARSUBQUERY[...]`. Stored as
    // frozen SQL that text is an ordinary, UNRESERVED function call, so a user alias UDF named
    // INSUBQUERY (returning an array) made capture-time validation succeed, the baseline was
    // stored with planFrozen = true, and every replay evaluated that UDF instead of the original
    // `IN (SELECT ...)` - returning different rows.
    //
    // The decompiler now refuses any residual subquery and reserves the marker names, so such a
    // baseline either freezes as real SQL or stays on the parameterized-plan-tree path. Either
    // way the stored text must not carry an internal marker, and the replay must return the same
    // rows as the user's own query.

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    sql """DROP TABLE IF EXISTS spm_r89_t"""
    sql """DROP TABLE IF EXISTS spm_r89_dim"""
    sql """
        CREATE TABLE spm_r89_t (k INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        CREATE TABLE spm_r89_dim (k INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r89_t VALUES (1), (2), (3), (4)"""
    sql """INSERT INTO spm_r89_dim VALUES (2), (3)"""

    def dropOwnBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r89_") }
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
    def assertNoInternalMarker = { String planSql ->
        assertTrue(!planSql.toUpperCase().contains("INSUBQUERY"),
                "the frozen SQL must not carry the INSUBQUERY marker: " + planSql)
        assertTrue(!planSql.toUpperCase().contains("SCALARSUBQUERY"),
                "the frozen SQL must not carry the SCALARSUBQUERY marker: " + planSql)
        assertTrue(!planSql.toUpperCase().contains("EXISTS["),
                "the frozen SQL must not carry the EXISTS marker: " + planSql)
    }

    // ==================== #1: IN subquery ====================
    String inText = "SELECT k FROM spm_r89_t WHERE k IN (SELECT k FROM spm_r89_dim)" +
            " ORDER BY k"
    assertNoInternalMarker(planSqlOf(createBaseline(inText)))
    order_qt_r89_in_subquery """SELECT k FROM spm_r89_t WHERE k IN (SELECT k FROM spm_r89_dim)
        ORDER BY k"""
    // the value variant of the SAME digest: the subquery's literal changes, the rows follow
    order_qt_r89_in_subquery_variant """SELECT k FROM spm_r89_t
        WHERE k IN (SELECT k FROM spm_r89_dim WHERE k > 2) ORDER BY k"""

    // ==================== #1: a scalar subquery ====================
    String scalarText = "SELECT k, (SELECT max(k) FROM spm_r89_dim) AS m FROM spm_r89_t" +
            " ORDER BY k"
    assertNoInternalMarker(planSqlOf(createBaseline(scalarText)))
    order_qt_r89_scalar_subquery """SELECT k, (SELECT max(k) FROM spm_r89_dim) AS m
        FROM spm_r89_t ORDER BY k"""

    // ==================== #1: EXISTS ====================
    String existsText = "SELECT k FROM spm_r89_t t WHERE EXISTS" +
            " (SELECT 1 FROM spm_r89_dim d WHERE d.k = t.k) ORDER BY k"
    assertNoInternalMarker(planSqlOf(createBaseline(existsText)))
    order_qt_r89_exists_subquery """SELECT k FROM spm_r89_t t WHERE EXISTS
        (SELECT 1 FROM spm_r89_dim d WHERE d.k = t.k) ORDER BY k"""

    dropOwnBaselines()
}
