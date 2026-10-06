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

suite("test_spm_review_round18", "spm") {

    // Eighteenth review round: SQL-level regression for the create-input / reload fixes.
    //
    // Covered here (end-to-end through CREATE BASELINE + EXPLAIN hit + replay result):
    //  - EXPLAIN must revalidate the frozen baseline AFTER planning (like a query does):
    //    an ALTER on a PLAN-side table between the pre-match fingerprint check and the
    //    planning must make EXPLAIN reject the stale replay instead of describing a plan
    //    the equivalent query would not use
    //  - the RAW-PLAN fallback (a three-argument LIKE ... ESCAPE '!' makes the
    //    parameterized plan unplannable) must store the ORIGINAL planSql: a transformed
    //    text would re-parameterize to placeholder ids the bind tree never extracts, and
    //    every replay after the periodic reload failed
    //  - the temporary-table marker inside an ordinary LITERAL ('_#TEMP#_') must not be
    //    mistaken for a temporary relation: the reload used to drop the durable row from
    //    the cache and the baseline silently disappeared
    //  - SHOW BASELINE PLANS LIKE must honor the ESCAPED wildcards \_ and \% of a SQL
    //    literal

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    // ==================== setup: tables (drop before use, keep after) ====================
    sql """DROP TABLE IF EXISTS spm_r18_t1"""
    sql """DROP TABLE IF EXISTS spm_r18_p1"""
    sql """
        CREATE TABLE spm_r18_t1 (k INT, k2 VARCHAR(20))
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r18_t1 VALUES (1, 'ab'), (2, 'cd'), (3, 'ae')"""
    sql """
        CREATE TABLE spm_r18_p1 (v INT)
        DUPLICATE KEY(v)
        DISTRIBUTED BY HASH(v) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r18_p1 VALUES (1), (3)"""

    // ==================== cleanup: drop this suite's leftover baselines ====================
    def ownBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r18_") }
    }
    def dropOwnBaselines = {
        ownBaselines().each { row ->
            sql """DROP BASELINE PLAN ${row[0]}"""
        }
    }
    dropOwnBaselines()

    def explainOf = { String query -> sql("""EXPLAIN ${query}""").toString() }
    def createBaseline = { String bind, String plan ->
        (sql('CREATE GLOBAL BASELINE PLAN "' + bind + '" WITH "' + plan + '"')[0][0] as Long)
    }

    // ==================== EXPLAIN revalidates the replayed baseline (comment 5) ====================
    // round-44 #11: a manual plan may only read tables the BIND text reads, so the old
    // cross-table pair (bind spm_r18_t1, plan spm_r18_p1) is now rejected at CREATE -
    // the reviewer's "SELECT k FROM t / SELECT k FROM u" case. The equivalent
    // same-table scenario keeps the coverage: the baseline pins the table's schema and
    // a later DDL must stop the replay instead of letting EXPLAIN describe a plan the
    // query would not use.
    String explainBind = "SELECT k FROM spm_r18_t1"
    long explainId = createBaseline(explainBind, explainBind)
    assertTrue(explainOf(explainBind).contains("SPM baseline hit: id=${explainId}"),
            "control: the baseline must hit before the DDL: " + explainOf(explainBind))

    // the cross-table manual plan is rejected where it is authored; no row may appear
    test {
        sql """CREATE GLOBAL BASELINE PLAN '${explainBind}' WITH 'SELECT v AS k FROM spm_r18_p1'"""
        exception "never reads"
    }
    assertEquals(0, sql("""SHOW BASELINE PLANS WHERE bind_sql = '${explainBind}'""")
                    .findAll { it[4].toString().contains("spm_r18_p1") }.size(),
            "the rejected cross-table CREATE must leave no row behind")

    // the stored fingerprint was bound to the OLD schema: after the DDL the candidate
    // must be skipped and the query / EXPLAIN must run their own plan
    sql """ALTER TABLE spm_r18_t1 ADD COLUMN extra INT"""
    assertFalse(explainOf(explainBind).contains("SPM baseline hit: id=${explainId}"),
            "after the schema change the stale replay must not be used: " + explainOf(explainBind))
    sql """DROP BASELINE PLAN ${explainId}"""

    // ==================== the raw-plan fallback stores the ORIGINAL text (comment 10) ====================
    // the three-argument LIKE makes the parameterized plan unplannable, so CREATE falls
    // back to optimizing the raw SQL; the stored planSql must stay the ORIGINAL text
    String escapeBind = "SELECT k FROM spm_r18_t1 WHERE k2 LIKE '%a%' ESCAPE '!' AND k BETWEEN 1 AND 2"
    long escapeId = createBaseline(escapeBind, escapeBind)
    String storedPlanSql = sql("""SELECT plan_sql FROM __internal_schema.spm_baselines
            WHERE id = ${escapeId}""")[0][0].toString()
    assertTrue(storedPlanSql == escapeBind,
            "the raw-plan fallback must keep the user's text (a decompiled BETWEEN becomes"
                    + " k >= 1 AND k <= 2, whose reloaded placeholders never line up): "
                    + storedPlanSql)

    String escapeReplay = "SELECT k FROM spm_r18_t1 WHERE k2 LIKE '%a%' ESCAPE '!' AND k BETWEEN 2 AND 3"
    assertTrue(explainOf(escapeReplay).contains("SPM baseline hit: id=${escapeId}"),
            "the fallback baseline must hit before the reload: " + explainOf(escapeReplay))

    // ==================== the temporary marker inside a literal (comment 4) ====================
    String tempBind = "SELECT k FROM spm_r18_t1 WHERE k2 = '_#TEMP#_' OR k = 1"
    long tempId = createBaseline(tempBind, tempBind)

    // ==================== SHOW LIKE honors escaped wildcards (comment 3) ====================
    // the SQL literal keeps the backslash of \_ / \%; the matcher must consume it with
    // the next character instead of treating the underscore as a wildcard
    List<List<Object>> escapedShow = sql(
            """SHOW BASELINE PLANS LIKE '%spm\\_r18\\_t1%'""")
    assertTrue(escapedShow.any { it[0].toString() == tempId.toString() },
            "an escaped '_' must match the literal underscore of the stored SQL: " + escapedShow)
    List<List<Object>> escapedMiss = sql(
            """SHOW BASELINE PLANS LIKE '%spm\\_r18\\_t9%'""")
    assertTrue(escapedMiss.isEmpty(),
            "an escaped '_' must not act as a wildcard: " + escapedMiss)

    // ==================== one periodic reload: both rows must survive it ====================
    // the refresh daemon re-parses every persisted row (spm_baseline_refresh_interval_seconds
    // defaults to 60s) and the snapshot is AUTHORITATIVE: a row the reload cannot rebuild
    // disappears from the cache, and the next query silently stops using it
    Thread.sleep(65000)
    assertTrue(explainOf(escapeReplay).contains("SPM baseline hit: id=${escapeId}"),
            "the raw-fallback baseline must replay through the RELOADED trees: "
                    + explainOf(escapeReplay))
    List<List<Object>> escapeRows = sql(escapeReplay)
    sql """set enable_spm_rewrite = false"""
    List<List<Object>> escapeDirect = sql(escapeReplay)
    sql """set enable_spm_rewrite = true"""
    assertTrue(escapeRows == escapeDirect && escapeRows.collect { it[0] as int } == [3],
            "the reloaded replay must return the direct result: "
                    + escapeRows + " vs " + escapeDirect)

    assertTrue(explainOf(tempBind).contains("SPM baseline hit: id=${tempId}"),
            "the '_#TEMP#_' literal must not be mistaken for a temporary relation: "
                    + explainOf(tempBind))
    List<List<Object>> tempRows = sql(tempBind)
    assertTrue(tempRows.collect { it[0] as int } == [1],
            "the literal-marker baseline must return its rows: " + tempRows)

    // leave no baselines behind for other runs
    dropOwnBaselines()
    assertEquals(0, ownBaselines().size(), "all spm_r18_ baselines must be dropped")
}
