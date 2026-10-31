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

suite("test_spm_r67_review", "spm") {

    // Round-67 review fixes with a SQL surface. Every block captures a baseline and
    // replays it with the SPM fallback switch pinned CLOSED, so a broken frozen text or a
    // dropped rewrite surfaces as an ERROR instead of silently re-running the original
    // query.
    //
    //  - #1  an explicit CAST around a substitutable placeholder survives parameterization:
    //        CAST(1 AS TINYINT) froze WITHOUT the conversion (the placeholder reported the
    //        captured literal's type, so SimplifyCastRule removed it) and the matching
    //        CAST(128 AS TINYINT) - whose literal is a SMALLINT - substituted a wider
    //        value, losing both the conversion and the result type. The frozen plan SQL
    //        must keep CAST(_spm_const_var(...)) and the replay must equal the direct run.
    //  - #3  the GROUP BY mirror records end at the query-block boundary: a variant that
    //        changes only the OUTER literal of
    //        SELECT k+1 AS c FROM (SELECT k+1 AS k FROM t GROUP BY k+1) s used to share
    //        one placeholder id with the inner block's group-by expression and never hit
    //        its baseline.
    //  - #5  an EXCEPT name is resolved with the analyzer's identifier-case rules
    //        (EXCEPT(k) removes the column aliased K); the SQL surface replays the shape
    //        (the exact header realignment is pinned by SPMRound67PlannerTest).
    //
    // #2 (the retired same-key twins ride in the DROP's durable confirmation / retained
    // retry obligation) and #4 (a status-only refresh adopts the persisted scalars instead
    // of reparsing the row under the cache monitor) are manager-internal: they are covered
    // by SPMRound67ManagerTest.

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    // ==================== setup: tables (drop before use, keep after) ====================
    sql """DROP TABLE IF EXISTS spm_r67_t"""
    sql """
        CREATE TABLE spm_r67_t (k INT, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r67_t VALUES (1, 10), (2, 20)"""

    sql """DROP TABLE IF EXISTS spm_r67_u"""
    sql """
        CREATE TABLE spm_r67_u (v INT)
        DUPLICATE KEY(v)
        DISTRIBUTED BY HASH(v) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r67_u VALUES (7)"""

    // Global baselines are cluster-wide state and other SPM suites may run their own in
    // parallel: every SHOW here is scoped to THIS suite's tables.
    def ownBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { row ->
            row[1].toString().contains("spm_r67_")
        }
    }
    def dropOwnBaselines = {
        ownBaselines().each { row ->
            sql """DROP BASELINE PLAN ${row[0]}"""
        }
    }
    def explainOf = { String stmt ->
        sql("""EXPLAIN ${stmt}""").toString()
    }
    def frozenPlanSql = { long id ->
        sql("""SELECT plan_sql FROM __internal_schema.spm_baselines WHERE id = ${id}""")[0][0].toString()
    }
    def createGlobal = { String bind, String plan ->
        List<List<Object>> created = sql("""CREATE GLOBAL BASELINE PLAN '${bind}' WITH '${plan}'""")
        assertEquals(1, created.size(), "CREATE should return one row, got: ${created}")
        return Long.parseLong(created[0][0].toString())
    }
    // the replay result must equal the DIRECT execution of the same statement
    def assertReplayEqualsDirect = { String stmt ->
        sql """set enable_spm_rewrite = false"""
        def direct = sql(stmt)
        sql """set enable_spm_rewrite = true"""
        def replayed = sql(stmt)
        assertEquals(direct.toString(), replayed.toString(),
                "the replayed result must equal the direct execution: ${stmt}")
    }

    dropOwnBaselines()

    // ==================== #1: an explicit CAST over a substituted value ====================
    String castBind = "SELECT CAST(1 AS TINYINT) AS c FROM spm_r67_t"
    long castId = createGlobal(castBind, castBind)
    assertTrue(frozenPlanSql(castId).contains("CAST(_spm_const_var"),
            "the explicit cast must survive parameterization: " + frozenPlanSql(castId))
    // the matching literal is a SMALLINT: only the retained cast keeps the semantics
    String castVariant = "SELECT CAST(128 AS TINYINT) AS c FROM spm_r67_t"
    assertTrue(explainOf(castVariant).contains("SPM baseline hit: id=${castId}"),
            "the wide-literal variant must hit the baseline: " + explainOf(castVariant))
    assertReplayEqualsDirect(castVariant)
    order_qt_r67_tinyint_cast """SELECT CAST(128 AS TINYINT) AS c FROM spm_r67_t ORDER BY 1"""

    // ==================== #3: GROUP BY mirror scope across a derived table ====================
    String mirrorBind = "SELECT k + 1 AS c" +
            " FROM (SELECT k + 1 AS k FROM spm_r67_t GROUP BY k + 1) s"
    long mirrorId = createGlobal(mirrorBind, mirrorBind)
    String mirrorVariant = "SELECT k + 2 AS c" +
            " FROM (SELECT k + 1 AS k FROM spm_r67_t GROUP BY k + 1) s"
    assertTrue(explainOf(mirrorVariant).contains("SPM baseline hit: id=${mirrorId}"),
            "the outer-literal variant must hit its baseline: " + explainOf(mirrorVariant))
    assertReplayEqualsDirect(mirrorVariant)
    order_qt_r67_mirror_variant """SELECT k + 2 AS c
        FROM (SELECT k + 1 AS k FROM spm_r67_t GROUP BY k + 1) s
        ORDER BY 1"""

    // ==================== #5: EXCEPT with identifier-case rules ====================
    String exceptSql = "SELECT * EXCEPT(k) FROM (SELECT k AS K FROM spm_r67_t) s" +
            " CROSS JOIN spm_r67_u"
    long exceptId = createGlobal(exceptSql, exceptSql)
    assertTrue(explainOf(exceptSql).contains("SPM baseline hit: id=${exceptId}"),
            "the EXCEPT query must hit its baseline: " + explainOf(exceptSql))
    assertReplayEqualsDirect(exceptSql)
    order_qt_r67_except """SELECT * EXCEPT(k) FROM (SELECT k AS K FROM spm_r67_t) s
        CROSS JOIN spm_r67_u ORDER BY 1"""
}
