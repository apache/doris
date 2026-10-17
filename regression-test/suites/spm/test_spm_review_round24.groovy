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

suite("test_spm_review_round24", "spm") {

    // The frozen scalar-subquery replay and the namespace-qualified bind-table pinning.
    //
    //  - a plan SQL with /*+ LEADING(...) */ and a NON-aggregated scalar subquery freezes
    //    the decompiled text, which carries the PhysicalAssertNumRows as the ASSERT_ROWS
    //    relation prefix; the frozen=1 assertion below pins that path and the plan_sql
    //    check pins the assertion text. (The plan-selection hint of the FALLBACK replay
    //    is covered by SPMRound24SafetyTest.)
    //  - a cross-DATABASE pair of same-named tables must keep the bind baseline of
    //    db1.t working (and never leak it onto db2.t).
    // #1/#4 (master-handoff fencing of a status flip) are covered by
    // BaselineManagerConcurrencyTest: a single sandbox FE cannot interleave a handoff
    // with an in-flight ALTER.
    // #3 (catalog-aware bind-table lookup) and #5 (linear subquery walk) are covered by
    // SPMRound24SafetyTest.

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    def ownBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r24_") }
    }
    def dropOwnBaselines = {
        ownBaselines().each { row ->
            sql """DROP BASELINE PLAN ${row[0]}"""
        }
    }
    dropOwnBaselines()

    def explainOf = { String query -> sql("""EXPLAIN ${query}""").toString() }
    def createBaseline = { String text ->
        (sql('CREATE GLOBAL BASELINE PLAN "' + text + '" WITH "' + text + '"')[0][0] as Long)
    }
    def rowsWithRewriteOff = { String query ->
        sql """set enable_spm_rewrite = false"""
        List<List<Object>> rows = sql(query)
        sql """set enable_spm_rewrite = true"""
        rows
    }
    def planFrozen = { long id ->
        sql("""SELECT plan_frozen FROM __internal_schema.spm_baselines WHERE id = ${id}""")[0][0]
                .toString().toLowerCase()
    }

    // ==================== setup ====================
    sql """DROP TABLE IF EXISTS spm_r24_a"""
    sql """
        CREATE TABLE spm_r24_a (k INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r24_a VALUES (1), (2), (3)"""
    sql """DROP TABLE IF EXISTS spm_r24_b"""
    sql """
        CREATE TABLE spm_r24_b (k INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r24_b VALUES (1), (2), (3)"""
    sql """DROP DATABASE IF EXISTS spm_r24_db1"""
    sql """DROP DATABASE IF EXISTS spm_r24_db2"""
    sql """CREATE DATABASE spm_r24_db1"""
    sql """CREATE DATABASE spm_r24_db2"""
    sql """CREATE TABLE spm_r24_db1.t (k INT) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES("replication_num" = "1")"""
    sql """CREATE TABLE spm_r24_db2.t (k INT) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES("replication_num" = "1")"""
    sql """INSERT INTO spm_r24_db1.t VALUES (1), (2)"""
    sql """INSERT INTO spm_r24_db2.t VALUES (10), (20)"""

    // ==================== #2: a scalar-subquery plan freezes its ASSERT_ROWS text ====================
    // the scalar subquery is NOT aggregated and is filtered to ONE row at run time: the
    // optimizer inserts PhysicalAssertNumRows, and the frozen text renders it as the
    // ASSERT_ROWS relation prefix, which re-parses into the assertion on replay
    String leadingQuery = "SELECT /*+ LEADING(b, a) */ a.k FROM spm_r24_a a" +
            " JOIN spm_r24_b b ON a.k = b.k" +
            " WHERE a.k = (SELECT k FROM spm_r24_b b2 WHERE b2.k = 1)"
    long leadingId = createBaseline(leadingQuery)
    assertTrue("1".equals(planFrozen(leadingId)) || "true".equals(planFrozen(leadingId)),
            "the CREATE must freeze the decompiled text (plan_frozen=1), got: "
                    + planFrozen(leadingId))
    String leadingPlanSql = sql("""SELECT plan_sql FROM __internal_schema.spm_baselines WHERE id = ${leadingId}""")[0][0].toString()
    assertTrue(leadingPlanSql.contains("ASSERT_ROWS"),
            "the frozen text must keep the scalar-subquery assertion: " + leadingPlanSql)
    String leadingExplain = explainOf(leadingQuery)
    assertTrue(leadingExplain.contains("SPM baseline hit: id=${leadingId}"),
            "the frozen baseline must be hit: " + leadingExplain)
    assertTrue(sql(leadingQuery) == rowsWithRewriteOff(leadingQuery),
            "the frozen replay must return the original rows")

    // ==================== #3: the bind baseline stays pinned to its own database ====================
    String db1Query = "SELECT k FROM spm_r24_db1.t"
    long db1Id = createBaseline(db1Query)
    assertTrue(explainOf(db1Query).contains("SPM baseline hit: id=${db1Id}"),
            "the db1 baseline must hit its own query: " + explainOf(db1Query))
    assertTrue(sql(db1Query) == rowsWithRewriteOff(db1Query),
            "the replay must return db1's rows")
    String db2Query = "SELECT k FROM spm_r24_db2.t"
    assertFalse(explainOf(db2Query).contains("SPM baseline hit: id=${db1Id}"),
            "the db1 baseline must never serve the same-named table of db2: "
                    + explainOf(db2Query))
    assertTrue(sql(db2Query) == rowsWithRewriteOff(db2Query),
            "db2's own query must return db2's rows")

    // leave no baselines behind for other runs
    dropOwnBaselines()
    assertEquals(0, ownBaselines().size(), "all spm_r24_ baselines must be dropped")
}
