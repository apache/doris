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

suite("test_spm_review_round3", "spm") {

    // Third review round:
    //  - capture interval / batch size must reject non-positive values through the SQL
    //    SET path (zero disables scanning forever / LIMIT 0 skips every eligible row)
    //  - a LATERAL VIEW over a WRAPPED child (derived table with WHERE + LIMIT) must keep
    //    the child's complete query block INSIDE the lateral-view input: attaching the
    //    lateral view to the bare FROM fragment moves LIMIT after the explode and frozen
    //    replay then returns a truncated result

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    // ==================== capture interval / batch size range validation ====================
    def showVar = { String name -> sql("""SHOW VARIABLES LIKE '${name}'""")[0][1].toString() }
    String intervalDefault = showVar("plan_capture_interval_seconds")
    String batchDefault = showVar("plan_capture_max_batch_size")

    // an interval of 0 makes every cycle compute an empty window (scanStart >= now)
    test {
        sql """SET GLOBAL plan_capture_interval_seconds = 0"""
        exception "must be a positive"
    }
    test {
        sql """SET plan_capture_interval_seconds = -10"""
        exception "must be a positive"
    }
    // a batch size of 0 produces LIMIT 0, marks the window exhausted and advances the
    // watermark over every eligible row
    test {
        sql """SET GLOBAL plan_capture_max_batch_size = 0"""
        exception "must be positive"
    }
    test {
        sql """SET plan_capture_max_batch_size = -1"""
        exception "must be positive"
    }
    assertEquals(intervalDefault, showVar("plan_capture_interval_seconds"),
            "a rejected interval must never be written")
    assertEquals(batchDefault, showVar("plan_capture_max_batch_size"),
            "a rejected batch size must never be written")

    // valid values still go through SQL SET, and are restored afterwards
    sql """SET GLOBAL plan_capture_interval_seconds = 600"""
    assertEquals("600", showVar("plan_capture_interval_seconds"))
    sql """SET GLOBAL plan_capture_interval_seconds = ${intervalDefault}"""
    sql """SET GLOBAL plan_capture_max_batch_size = 100"""
    assertEquals("100", showVar("plan_capture_max_batch_size"))
    sql """SET GLOBAL plan_capture_max_batch_size = ${batchDefault}"""

    // ==================== LATERAL VIEW over a wrapped child ====================
    sql """DROP TABLE IF EXISTS spm_r3_t"""
    sql """
        CREATE TABLE spm_r3_t (k INT, arr ARRAY<INT>)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r3_t VALUES (1, [1, 2]), (2, [3, 4, 5])"""

    // cleanup: drop this suite's leftover baselines (ids are dynamic)
    def ownBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r3_") }
    }
    ownBaselines().each { row ->
        sql """DROP BASELINE PLAN ${row[0]}"""
    }
    assertEquals(0, ownBaselines().size(), "no spm_r3_ baseline should be left after cleanup")

    // the Generate child is a derived table with WHERE + LIMIT: its whole query block must
    // stay inside the lateral-view input; ORDER BY x makes the replayed result rows
    // deterministic for the qt baseline
    String lateralSql = "select s.k as k, x from (select k, arr from spm_r3_t where k = 1 limit 1) s" +
            " lateral view explode(s.arr) t as x order by x"
    long lateralId = sql("""CREATE GLOBAL BASELINE PLAN '${lateralSql}' WITH '${lateralSql}'""")[0][0] as long

    String explainText = sql("""EXPLAIN ${lateralSql}""").toString()
    assertTrue(explainText.contains("SPM baseline hit: id=${lateralId}"),
            "the lateral-view query must replay from its baseline: " + explainText)

    // the FROZEN SQL must be the SPM-decompiled (placeholder-bearing) text, with the
    // child's WHERE + LIMIT INSIDE the lateral-view input
    String frozenPlanSql = sql("""SELECT plan_sql FROM __internal_schema.spm_baselines WHERE id = ${lateralId}""")[0][0].toString()
    assertTrue(frozenPlanSql.contains("_spm_const_var"),
            "the frozen SQL must come from the SPM decompiler, not the raw plan text: " + frozenPlanSql)
    assertTrue(frozenPlanSql.contains("LATERAL VIEW"),
            "the frozen SQL must keep the lateral view: " + frozenPlanSql)
    int lateralIdx = frozenPlanSql.indexOf("LATERAL VIEW")
    int whereIdx = frozenPlanSql.indexOf("WHERE")
    int limitIdx = frozenPlanSql.indexOf("LIMIT")
    assertTrue(whereIdx > 0 && whereIdx < lateralIdx,
            "the child WHERE must stay inside the lateral-view input: " + frozenPlanSql)
    assertTrue(limitIdx > 0 && limitIdx < lateralIdx,
            "the child LIMIT must stay inside the lateral-view input (after the explode"
                    + " it would truncate the exploded rows): " + frozenPlanSql)

    // the replay must return BOTH exploded values of the single limited row: applying the
    // LIMIT after the lateral view would return only one of them
    order_qt_lateral_wrapped_child """select s.k as k, x from (select k, arr from spm_r3_t where k = 1 limit 1) s lateral view explode(s.arr) t as x order by x"""
    List<List<Object>> rows = sql(lateralSql)
    assertEquals(2, rows.size(),
            "both exploded values of the limited row must survive the replay: " + rows)
    def pairs = rows.collect { [ (it[0] as int), (it[1] as int) ] }.sort { a, b -> a[1] <=> b[1] }
    assertEquals([[1, 1], [1, 2]], pairs, "unexpected replay result: " + rows)

    // leave no baselines behind for other runs
    ownBaselines().each { row ->
        sql """DROP BASELINE PLAN ${row[0]}"""
    }
    assertEquals(0, ownBaselines().size(), "all spm_r3_ baselines must be dropped")
}
