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

suite("test_spm_review_round21", "spm") {

    // Twenty-first review round: end-to-end coverage for the statement
    // re-parse safety and the join distribute-hint handling.
    //
    // Covered here:
    //  - a bind SQL carrying a JOIN [shuffle] distribute hint must freeze,
    //    re-parse and replay normally, and the hint must stay out of the
    //    matching key (a query without the hint hits the same baseline)
    //  - replaying that baseline must return the join rows (the parameterized
    //    rebuild of the frozen plan runs without a session attached to the
    //    re-parse in some paths; before the fix that re-parse threw an NPE
    //    inside the distribute-hint registration)
    //
    // The insert-ambiguity status probe (#1), the completion-aware audit
    // lower bound (#3), the pre-lock resolver (#4), the EXPLAIN routing (#5)
    // and the checkpoint visibility handshake (#6) are covered by
    // SPMRound21SafetyTest / BaselineManagerConcurrencyTest /
    // AuditLogScannerCursorTest / PlanCaptureCycleHandoffTest.

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    def ownBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r21_") }
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

    // ==================== setup ====================
    sql """DROP TABLE IF EXISTS spm_r21_t1"""
    sql """DROP TABLE IF EXISTS spm_r21_t2"""
    sql """
        CREATE TABLE spm_r21_t1 (k INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        CREATE TABLE spm_r21_t2 (k INT, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r21_t1 VALUES (1), (2), (3)"""
    sql """INSERT INTO spm_r21_t2 VALUES (1, 10), (2, 20), (4, 40)"""

    // ==================== #2: JOIN [shuffle] bind SQL ====================
    String hintBind = "SELECT t1.k, t2.v FROM spm_r21_t1 t1 JOIN [shuffle] spm_r21_t2 t2" +
            " ON t1.k = t2.k WHERE t1.k > 0"
    long hintId = createBaseline(hintBind)

    String hintRun = "SELECT t1.k, t2.v FROM spm_r21_t1 t1 JOIN [shuffle] spm_r21_t2 t2" +
            " ON t1.k = t2.k WHERE t1.k > 0"
    assertTrue(explainOf(hintRun).contains("SPM baseline hit: id=${hintId}"),
            "the hinted join query must hit the baseline: " + explainOf(hintRun))
    List<List<Object>> hintRows = sql(hintRun)
    // a [shuffle] join does not guarantee row order, so compare sorted rows
    assertTrue(hintRows.sort() == rowsWithRewriteOff(hintRun).sort() && hintRows.sort() == [[1, 10], [2, 20]],
            "the replayed hinted join must return the join rows: " + hintRows)

    // the distribute hint is not part of the matching key: the same join
    // without the hint must hit the same baseline and replay correctly
    String plainRun = "SELECT t1.k, t2.v FROM spm_r21_t1 t1 JOIN spm_r21_t2 t2" +
            " ON t1.k = t2.k WHERE t1.k > 1"
    assertTrue(explainOf(plainRun).contains("SPM baseline hit: id=${hintId}"),
            "the hintless variant must stay on the same baseline: " + explainOf(plainRun))
    List<List<Object>> plainRows = sql(plainRun)
    assertTrue(plainRows.sort() == rowsWithRewriteOff(plainRun).sort() && plainRows == [[2, 20]],
            "the replayed variant must return its own rows: " + plainRows)

    // an explicitly different distribute hint is free to miss the baseline
    // (the frozen hint is part of the join shape), but it must never corrupt
    // the replay of other statements: it executes normally and returns its rows
    String broadcastRun = "SELECT t1.k, t2.v FROM spm_r21_t1 t1 JOIN [broadcast] spm_r21_t2 t2" +
            " ON t1.k = t2.k WHERE t1.k >= 2"
    List<List<Object>> broadcastRows = sql(broadcastRun)
    assertTrue(broadcastRows.sort() == rowsWithRewriteOff(broadcastRun).sort() && broadcastRows == [[2, 20]],
            "the replayed broadcast variant must return its own rows: " + broadcastRows)

    // leave no baselines behind for other runs
    dropOwnBaselines()
    assertEquals(0, ownBaselines().size(), "all spm_r21_ baselines must be dropped")
}
