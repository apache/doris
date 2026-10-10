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

suite("test_spm_review_round20", "spm") {

    // End-to-end coverage for the scope-aware guards.
    //
    // Covered here:
    //  - a WITH alias shadowing a same-named CATALOG VIEW must not make the view guard
    //    reject the statement: CREATE freezes normally and the matching query hits the
    //    baseline (before: raw fallback + replay exit at viewReferenced)
    //  - the * REPLACE payload must be namespace-qualified in the matching key: a
    //    baseline created in db1 for "SELECT * REPLACE((SELECT max(v) FROM u) AS k)
    //    FROM t" must not match the same text under db2 (the frozen SQL reads db1.u)
    //  - a replay-context expression inside a SUBQUERY plan (@v) must be rejected at
    //    CREATE instead of freezing the creator's value
    //
    // The cache-miss / status-probe items (#4/#5/#6), the pre-lock resolver (#1), the
    // hidden payload walks (#3/#9/#12 internals) and the fallback planner-state resets
    // (#7/#10) are covered by SPMRound20SafetyTest / BaselineManagerConcurrencyTest.

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    def ownBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r20_") }
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
    sql """DROP VIEW IF EXISTS spm_r20_c"""
    sql """DROP TABLE IF EXISTS spm_r20_t1"""
    sql """
        CREATE TABLE spm_r20_t1 (k INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r20_t1 VALUES (1), (2), (3)"""
    // the view shadowed by the WITH alias below: its body would return NO rows, so a
    // statement wrongly bound to it cannot produce the expected rows either
    sql """CREATE VIEW spm_r20_c AS SELECT k FROM spm_r20_t1 WHERE k > 100"""

    // ==================== #2: WITH alias shadows a catalog view ====================
    String cteBind = "WITH spm_r20_c AS (SELECT k FROM spm_r20_t1 WHERE k > 1)" +
            " SELECT k FROM spm_r20_c"
    long cteId = createBaseline(cteBind)
    assertTrue(planSqlOf(cteId) != cteBind,
            "the builder must freeze the CTE statement (not the self planSql fallback): "
                    + planSqlOf(cteId))

    String cteRun = "WITH spm_r20_c AS (SELECT k FROM spm_r20_t1 WHERE k > 2)" +
            " SELECT k FROM spm_r20_c"
    assertTrue(explainOf(cteRun).contains("SPM baseline hit: id=${cteId}"),
            "the matching CTE query must apply the baseline: " + explainOf(cteRun))
    List<List<Object>> cteRows = sql(cteRun)
    assertTrue(cteRows == rowsWithRewriteOff(cteRun)
                    && cteRows.size() == 1 && cteRows[0][0] == 3,
            "the replay must return the CTE's rows (k > 2): " + cteRows)

    // ==================== #8: * REPLACE payload is namespace-qualified ====================
    sql """DROP DATABASE IF EXISTS spm_r20_db1"""
    sql """DROP DATABASE IF EXISTS spm_r20_db2"""
    sql """CREATE DATABASE spm_r20_db1"""
    sql """CREATE DATABASE spm_r20_db2"""
    for (String db : ["spm_r20_db1", "spm_r20_db2"]) {
        sql """CREATE TABLE ${db}.spm_r20_t2 (k INT) DUPLICATE KEY(k)
                DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES("replication_num" = "1")"""
        sql """CREATE TABLE ${db}.spm_r20_u (v INT) DUPLICATE KEY(v)
                DISTRIBUTED BY HASH(v) BUCKETS 1 PROPERTIES("replication_num" = "1")"""
    }
    sql """INSERT INTO spm_r20_db1.spm_r20_t2 VALUES (1), (2)"""
    sql """INSERT INTO spm_r20_db1.spm_r20_u VALUES (10)"""
    sql """INSERT INTO spm_r20_db2.spm_r20_t2 VALUES (1), (2)"""
    sql """INSERT INTO spm_r20_db2.spm_r20_u VALUES (20)"""

    String starText = "SELECT * REPLACE((SELECT max(v) FROM spm_r20_u) AS k)" +
            " FROM spm_r20_t2"
    sql """USE spm_r20_db1"""
    long starId = createBaseline(starText)
    assertTrue(explainOf(starText).contains("SPM baseline hit: id=${starId}"),
            "the same text in the creating database must hit: " + explainOf(starText))
    List<List<Object>> db1Rows = sql(starText)
    assertTrue(db1Rows == rowsWithRewriteOff(starText)
                    && db1Rows == [[10], [10]],
            "db1 must read db1.u: " + db1Rows)

    sql """USE spm_r20_db2"""
    assertFalse(explainOf(starText).contains("SPM baseline hit: id=${starId}"),
            "the same text under db2 must NOT replay the db1 baseline: " + explainOf(starText))
    List<List<Object>> db2Rows = sql(starText)
    assertTrue(db2Rows == rowsWithRewriteOff(starText) && db2Rows == [[20], [20]],
            "db2 must read its own u (no db1.u leak): " + db2Rows)
    sql """USE regression_test_spm"""

    // ==================== #12: replay-context expression inside a subquery ====================
    test {
        sql """CREATE GLOBAL BASELINE PLAN
                'SELECT (SELECT max(@v) FROM spm_r20_t1) AS m FROM spm_r20_t1'
                WITH 'SELECT (SELECT max(@v) FROM spm_r20_t1) AS m FROM spm_r20_t1'"""
        exception "replay-time context"
    }

    // leave no baselines behind for other runs
    dropOwnBaselines()
    assertEquals(0, ownBaselines().size(), "all spm_r20_ baselines must be dropped")
}
