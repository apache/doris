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

import org.apache.doris.regression.util.JdbcUtils

suite("test_spm_review_round25", "spm") {

    // Twenty-fifth review round: the output-label contract of a manual plan and the
    // LIMIT contract of a replay whose own limit the positional merge cannot reach.
    //
    //  - #3: a manual plan that renders a BARE bind column under another label
    //    (bind 'SELECT k FROM t' WITH 'SELECT k AS other FROM t') pinned that alias in
    //    the frozen sink; the alignment returned no caller label for a bare column, so
    //    the manual plan's alias leaked out as the JDBC column label.
    //  - #4: top-level LIMIT / OFFSET VALUES are deliberately ignored by the match (a
    //    limit variant reuses the baseline), but the transfer is POSITIONAL. A manual
    //    plan may keep its own limit below a node the merge cannot align - DISTINCT over
    //    an inner limit - and replaying it for a caller asking a LARGER limit returned
    //    the captured row count. Such a candidate must be skipped instead.
    //  - #6: one 0/1 nullability digit per column grew a fingerprint entry without
    //    bound; the reviewer's ten 500-column example exceeded the column's
    //    VARCHAR(4096) and could not be persisted at all.
    // #1 (post-forwarded-DDL journal sync), #2 (fatal collision repair) and #5
    // (checkpoint leadership fence / promotion reload) need multi-FE interleavings: they
    // are covered by BaselineManagerConcurrencyTest and PlanCaptureCycleHandoffTest.

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    def ownBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r25_") }
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
    def labelOf = { String query ->
        def (rows, meta) = JdbcUtils.executeToList(context.getConnection(), query)
        [meta.getColumnLabel(1), rows]
    }
    def fingerprintOf = { long id ->
        sql("""SELECT schema_fingerprint FROM __internal_schema.spm_baselines
            WHERE id = ${id}""")[0][0].toString()
    }

    // ==================== setup ====================
    sql """DROP TABLE IF EXISTS spm_r25_x"""
    sql """
        CREATE TABLE spm_r25_x (k INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r25_x VALUES (1)"""
    sql """DROP TABLE IF EXISTS spm_r25_l"""
    sql """
        CREATE TABLE spm_r25_l (k INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r25_l VALUES (1), (2)"""

    // ==================== #3: a bare bind column keeps its own JDBC label ====================
    long labelId = createBaseline("SELECT k FROM spm_r25_x",
            "SELECT k AS other FROM spm_r25_x")
    def labelQuery = "SELECT k FROM spm_r25_x"
    assertTrue(explainOf(labelQuery).contains("SPM baseline hit: id=${labelId}"),
            "the manual baseline must be hit: " + explainOf(labelQuery))
    def (String label, List<List<Object>> labelRows) = labelOf(labelQuery)
    assertEquals("k", label,
            "the caller's own column name is the visible label (the manual plan's alias"
                    + " must not leak through the frozen sink)")
    assertEquals([[1]], labelRows, "the replayed rows stay the manual plan's rows")

    // ==================== #4: a caller limit the replay cannot carry is not served ====================
    // round-44 #10 pins the top-level ORDER BY contract at CREATE time and a matching
    // caller must share the bind's digest, so the bind carries no ORDER BY; the manual
    // plan keeps its own LIMIT below the DISTINCT, where the positional merge cannot
    // align it
    String limitedBind = "SELECT k FROM spm_r25_l LIMIT 1"
    String distinctPlan =
            "SELECT DISTINCT k FROM (SELECT k FROM spm_r25_l ORDER BY k LIMIT 1) s"
    long limitedId = createBaseline(limitedBind, distinctPlan)
    // the captured limit itself is served by the plan's own placement
    assertTrue(explainOf(limitedBind).contains("SPM baseline hit: id=${limitedId}"),
            "the exact bind must hit: " + explainOf(limitedBind))
    assertEquals([[1]], sql(limitedBind))

    // a larger caller limit: the DISTINCT above the inner LIMIT 1 cannot receive the
    // transferred value, so the replay would return the CAPTURED one row instead of two
    String largerLimit = "SELECT k FROM spm_r25_l LIMIT 2"
    String largerExplain = explainOf(largerLimit)
    assertTrue(!largerExplain.contains("SPM baseline hit: id=${limitedId}"),
            "the candidate must be skipped: its plan cannot carry the caller's LIMIT: "
                    + largerExplain)
    def largerRows = sql(largerLimit)
    assertEquals(2, largerRows.size(),
            "the caller's own limit must be honored (two rows, not the captured one)")
    assertEquals([1, 2], largerRows*.get(0).sort(),
            "both rows must come from the caller's own plan: " + largerRows)

    // ==================== #6: a wide multi-table fingerprint stays persistable ====================
    // the reviewer's example: ten 500-column tables. One 0/1 digit per column per table
    // added ~5000 characters - more than the fingerprint column (VARCHAR(4096)) - and the
    // CREATE failed even though the query is perfectly valid.
    def wideColumns = { int count -> (0..<count).collect { "c${it} INT" }.join(", ") }
    for (int table = 0; table < 10; table++) {
        sql """DROP TABLE IF EXISTS spm_r25_w${table}"""
        sql """CREATE TABLE spm_r25_w${table} (${wideColumns(500)})
            DUPLICATE KEY(c0)
            DISTRIBUTED BY HASH(c0) BUCKETS 1
            PROPERTIES("replication_num" = "1")"""
    }
    String wideQuery = "SELECT 1 FROM " + (0..<10).collect { "spm_r25_w${it}" }.join(", ")
    long wideId = createBaseline(wideQuery, wideQuery)
    String wideFingerprint = fingerprintOf(wideId)
    assertTrue(wideFingerprint.length() < 4096,
            "the fingerprint of ten 500-column tables must fit VARCHAR(4096), got "
                    + wideFingerprint.length())
    assertEquals(10, wideFingerprint.split(";").findAll { it.startsWith("spm_r25_w") }.size(),
            "one entry per table: " + wideFingerprint)
    assertTrue(wideFingerprint.contains("nullable:"),
            "the nullability digest section is persisted: " + wideFingerprint)
    assertTrue(explainOf(wideQuery).contains("SPM baseline hit: id=${wideId}"),
            "the wide baseline must be usable: " + explainOf(wideQuery))
}
