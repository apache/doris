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

suite("test_spm_review_round8", "spm") {

    // Eighth review round: SQL-level regression for the matching / frozen-SQL fixes.
    //
    // Covered here (end-to-end through CREATE BASELINE + EXPLAIN hit + replay result):
    //  - a MIXED IN list (a IN (1, b)) keeps the parameterized literal arm, so a replay
    //    whose literal differs still returns the rows the user asked for
    //  - ASOF ... MATCH_CONDITION joins: the temporal boundary takes part in matching,
    //    a variant with another boundary must NOT replay the captured one
    //  - SELECT ... INTO OUTFILE is rejected by CREATE BASELINE (its destination lives
    //    outside the plan, so a frozen wrapper would write to the captured file)
    //  - a three-argument LIKE keeps its ESCAPE child through freeze and replay
    //  - scalar-subquery statements no longer freeze ASSERT_ROWS text (unparseable);
    //    the baseline still hits and returns exactly the direct result
    //  - a SET_VAR-hint baseline survives the periodic refresh (the daemon parses the
    //    persisted bind SQL without a ConnectContext; a throw used to drop the baseline)
    //  - the durable capture checkpoint table exists in the internal schema

    // SPM regression pins the fallback switch CLOSED: a rewritten-plan failure must
    // surface as an error, never silently re-run the original query.
    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    // ==================== setup: tables (drop before use, keep after) ====================
    sql """DROP TABLE IF EXISTS spm_r8_t1"""
    sql """DROP TABLE IF EXISTS spm_r8_l1"""
    sql """DROP TABLE IF EXISTS spm_r8_r1"""

    sql """
        CREATE TABLE spm_r8_t1 (k INT, a INT, b INT, s VARCHAR(20))
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    // row meanings for the IN-list case (a IN (2, b) under replay):
    //   k=1 a=1  b=99 -> only the captured literal arm mattered at bind time
    //   k=2 a=99 b=99 -> satisfies a = b (the degenerate predicate the bug kept)
    //   k=3 a=2  b=99 -> satisfies the USER's literal arm; the bug silently lost it
    //   k=4 a=5  b=5  -> satisfies a = b
    sql """INSERT INTO spm_r8_t1 VALUES
        (1, 1, 99, 'abc'), (2, 99, 99, 'a!bc'), (3, 2, 99, 'axc'), (4, 5, 5, 'abb')"""

    sql """
        CREATE TABLE spm_r8_l1 (k INT, d DATETIME, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r8_l1 VALUES (1, '2024-01-02 00:00:00', 7), (2, '2024-01-03 00:00:00', 8)"""
    sql """
        CREATE TABLE spm_r8_r1 (k INT, d DATETIME, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    // for k=1 the right side has d=...-01 (v=10) and d=...-02 (v=20): l.d >= r.d picks
    // v=20 while l.d > r.d picks v=10, so a boundary swap is observable in the result
    sql """INSERT INTO spm_r8_r1 VALUES
        (1, '2024-01-01 00:00:00', 10), (1, '2024-01-02 00:00:00', 20), (2, '2024-01-01 00:00:00', 30)"""

    // ==================== cleanup: drop this suite's leftover baselines ====================
    def ownBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r8_") }
    }
    def dropOwnBaselines = {
        ownBaselines().each { row ->
            sql """DROP BASELINE PLAN ${row[0]}"""
        }
    }
    dropOwnBaselines()
    assertEquals(0, ownBaselines().size(), "no spm_r8_ baseline should be left after cleanup")

    def explainOf = { String query -> sql("""EXPLAIN ${query}""").toString() }
    def createBaseline = { String text ->
        (sql('CREATE GLOBAL BASELINE PLAN "' + text + '" WITH "' + text + '"')[0][0] as Long)
    }

    // ==================== mixed IN list: the literal arm survives (comment 1) ====================
    String inBindSql = "SELECT k FROM spm_r8_t1 WHERE a IN (1, b) ORDER BY k"
    long inId = createBaseline(inBindSql)

    // the frozen SQL must keep the parameterized literal arm: rewrites that drop it
    // (a IN (1, b) -> a = b) lose every row the user's literal selects
    String inPlanSql = sql("""SELECT plan_sql FROM __internal_schema.spm_baselines WHERE id = ${inId}""")[0][0].toString()
    assertTrue(inPlanSql.contains("IN (_spm_const_var"),
            "the frozen IN list must keep the parameterized literal arm: " + inPlanSql)
    assertTrue(inPlanSql.contains(", b)"),
            "the frozen IN list must keep the column arm as well: " + inPlanSql)

    // the replay uses a DIFFERENT literal: it satisfies k=3 (a=2) but not the captured
    // 1; the buggy rewrite (a = b) would return k=2 and k=4 instead
    String inReplaySql = "SELECT k FROM spm_r8_t1 WHERE a IN (2, b) ORDER BY k"
    assertTrue(explainOf(inReplaySql).contains("SPM baseline hit: id=${inId}"),
            "the literal-variant query must replay the baseline: " + explainOf(inReplaySql))

    List<List<Object>> inReplayRows = sql(inReplaySql)
    assertTrue(inReplayRows.collect { it[0] as int } == [2, 3, 4],
            "the replay must keep the user's literal arm (k=3) instead of a = b (k=2, k=4): " + inReplayRows)
    sql """set enable_spm_rewrite = false"""
    List<List<Object>> inDirectRows = sql(inReplaySql)
    sql """set enable_spm_rewrite = true"""
    assertTrue(inReplayRows == inDirectRows,
            "the replay must return exactly the direct result: replay=" + inReplayRows + " direct=" + inDirectRows)
    order_qt_in_mixed_list_replay """SELECT k FROM spm_r8_t1 WHERE a IN (2, b) ORDER BY k"""

    // ==================== ASOF MATCH_CONDITION takes part in matching (comment 2) ====================
    String asofSql = "SELECT l.k, r.v FROM spm_r8_l1 l ASOF JOIN spm_r8_r1 r" +
            " MATCH_CONDITION(l.d >= r.d) USING(k) WHERE l.k > 1 ORDER BY l.k, r.v"
    long asofId = createBaseline(asofSql)

    // same boundary, different literal: hits and returns the captured >= semantics
    String asofHitSql = "SELECT l.k, r.v FROM spm_r8_l1 l ASOF JOIN spm_r8_r1 r" +
            " MATCH_CONDITION(l.d >= r.d) USING(k) WHERE l.k > 0 ORDER BY l.k, r.v"
    assertTrue(explainOf(asofHitSql).contains("SPM baseline hit: id=${asofId}"),
            "the same boundary must hit: " + explainOf(asofHitSql))
    order_qt_asof_same_boundary """SELECT l.k, r.v FROM spm_r8_l1 l ASOF JOIN spm_r8_r1 r MATCH_CONDITION(l.d >= r.d) USING(k) WHERE l.k > 0 ORDER BY l.k, r.v"""

    // a different boundary must NOT match: replaying the captured >= would select the
    // right-side row at d=...-02 (v=20) although the user asked for > (v=10)
    String asofOtherSql = "SELECT l.k, r.v FROM spm_r8_l1 l ASOF JOIN spm_r8_r1 r" +
            " MATCH_CONDITION(l.d > r.d) USING(k) WHERE l.k > 1 ORDER BY l.k, r.v"
    assertFalse(explainOf(asofOtherSql).contains("SPM baseline hit"),
            "a different MATCH_CONDITION must not replay the captured boundary: " + explainOf(asofOtherSql))
    List<List<Object>> asofOtherRows = sql(asofOtherSql)
    sql """set enable_spm_rewrite = false"""
    List<List<Object>> asofOtherDirect = sql(asofOtherSql)
    sql """set enable_spm_rewrite = true"""
    assertTrue(asofOtherRows == asofOtherDirect,
            "the boundary-variant result must match the direct run: " + asofOtherRows + " vs " + asofOtherDirect)
    assertTrue(asofOtherRows[0][1] as int == 30,
            "the boundary-variant must keep its own > semantics: " + asofOtherRows)
    order_qt_asof_other_boundary """SELECT l.k, r.v FROM spm_r8_l1 l ASOF JOIN spm_r8_r1 r MATCH_CONDITION(l.d > r.d) USING(k) WHERE l.k > 1 ORDER BY l.k, r.v"""

    // ==================== OUTFILE statements are rejected (comment 4) ====================
    test {
        sql 'CREATE GLOBAL BASELINE PLAN "SELECT k FROM spm_r8_t1 INTO OUTFILE \'/tmp/spm_r8_outfile\' FORMAT AS csv" WITH "SELECT k FROM spm_r8_t1 INTO OUTFILE \'/tmp/spm_r8_outfile\' FORMAT AS csv"'
        exception "SPM does not support SELECT ... INTO OUTFILE"
    }

    // ==================== LIKE ESCAPE survives freeze + replay (comment 8) ====================
    String escapeBindSql = "SELECT k FROM spm_r8_t1 WHERE s LIKE 'a!_%' ESCAPE '!' ORDER BY k"
    long escapeId = createBaseline(escapeBindSql)
    String escapePlanSql = sql("""SELECT plan_sql FROM __internal_schema.spm_baselines WHERE id = ${escapeId}""")[0][0].toString()
    assertTrue(escapePlanSql.contains("ESCAPE"),
            "the frozen LIKE must keep the ESCAPE child: " + escapePlanSql)

    // same escape character, another pattern: hits and matches only 'a!bc'
    String escapeReplaySql = "SELECT k FROM spm_r8_t1 WHERE s LIKE 'a!b%' ESCAPE '!' ORDER BY k"
    assertTrue(explainOf(escapeReplaySql).contains("SPM baseline hit: id=${escapeId}"),
            "the escape-replay query must hit: " + explainOf(escapeReplaySql))
    List<List<Object>> escapeRows = sql(escapeReplaySql)
    sql """set enable_spm_rewrite = false"""
    List<List<Object>> escapeDirect = sql(escapeReplaySql)
    sql """set enable_spm_rewrite = true"""
    assertTrue(escapeRows == escapeDirect && escapeRows.collect { it[0] as int } == [2],
            "the escape-replay must treat '!' literally: " + escapeRows + " vs " + escapeDirect)
    order_qt_like_escape_replay """SELECT k FROM spm_r8_t1 WHERE s LIKE 'a!b%' ESCAPE '!' ORDER BY k"""

    // ==================== scalar subquery: no ASSERT_ROWS text, replay proves it (comment 13) ====================
    String scalarBindSql = "SELECT k FROM spm_r8_t1 WHERE a = (SELECT a FROM spm_r8_t1 WHERE k = 3) AND k > 1 ORDER BY k"
    long scalarId = createBaseline(scalarBindSql)
    String scalarPlanSql = sql("""SELECT plan_sql FROM __internal_schema.spm_baselines WHERE id = ${scalarId}""")[0][0].toString()
    assertFalse(scalarPlanSql.toUpperCase().contains("ASSERT_ROWS"),
            "the frozen row must not contain ASSERT_ROWS (the parser cannot consume it): " + scalarPlanSql)

    // frozen replay: the literal variant must hit and return the direct result
    String scalarReplaySql = "SELECT k FROM spm_r8_t1 WHERE a = (SELECT a FROM spm_r8_t1 WHERE k = 3) AND k > 2 ORDER BY k"
    assertTrue(explainOf(scalarReplaySql).contains("SPM baseline hit: id=${scalarId}"),
            "the scalar-subquery baseline must hit (not only exist by id): " + explainOf(scalarReplaySql))
    List<List<Object>> scalarRows = sql(scalarReplaySql)
    sql """set enable_spm_rewrite = false"""
    List<List<Object>> scalarDirect = sql(scalarReplaySql)
    sql """set enable_spm_rewrite = true"""
    assertTrue(scalarRows == scalarDirect && scalarRows.collect { it[0] as int } == [3],
            "the scalar-subquery replay must return the direct result: " + scalarRows + " vs " + scalarDirect)
    order_qt_scalar_subquery_replay """SELECT k FROM spm_r8_t1 WHERE a = (SELECT a FROM spm_r8_t1 WHERE k = 3) AND k > 2 ORDER BY k"""

    // ==================== SET_VAR-hint baseline survives the periodic refresh (comment 14) ====================
    String hintBindSql = "SELECT /*+ SET_VAR(parallel_pipeline_task_num=4) */ k FROM spm_r8_t1 WHERE k > 1 ORDER BY k"
    long hintId = createBaseline(hintBindSql)
    String hintReplaySql = "SELECT /*+ SET_VAR(parallel_pipeline_task_num=4) */ k FROM spm_r8_t1 WHERE k > 2 ORDER BY k"
    assertTrue(explainOf(hintReplaySql).contains("SPM baseline hit: id=${hintId}"),
            "the hint baseline must hit before the refresh: " + explainOf(hintReplaySql))

    // wait for at least one periodic refresh cycle (spm_baseline_refresh_interval_seconds
    // defaults to 60s): the daemon re-parses the persisted bind SQL (which carries the
    // SET_VAR hint) WITHOUT a ConnectContext. If that parse throws, the authoritative
    // diff drops this baseline from the cache and the hit disappears.
    Thread.sleep(65000)
    assertTrue(explainOf(hintReplaySql).contains("SPM baseline hit: id=${hintId}"),
            "the hint baseline must survive the periodic refresh: " + explainOf(hintReplaySql))

    List<List<Object>> hintRows = sql(hintReplaySql)
    sql """set enable_spm_rewrite = false"""
    List<List<Object>> hintDirect = sql(hintReplaySql)
    sql """set enable_spm_rewrite = true"""
    assertTrue(hintRows == hintDirect && hintRows.collect { it[0] as int } == [3, 4],
            "the hint replay must return the direct result: " + hintRows + " vs " + hintDirect)
    order_qt_setvar_hint_replay """SELECT /*+ SET_VAR(parallel_pipeline_task_num=4) */ k FROM spm_r8_t1 WHERE k > 2 ORDER BY k"""

    // ==================== durable capture checkpoint table (comment 16) ====================
    List<List<Object>> checkpointTables = sql("""SHOW TABLES FROM __internal_schema LIKE 'spm_capture_checkpoint'""")
    assertTrue(checkpointTables.size() == 1,
            "the durable capture checkpoint table must exist in __internal_schema: " + checkpointTables)
    List<List<Object>> checkpointCols = sql("""DESC __internal_schema.spm_capture_checkpoint""")
    List<String> colNames = checkpointCols.collect { it[0].toString() }
    assertTrue(colNames.containsAll(["last_scan_timestamp", "pending_window_start", "pending_window_end",
            "cursor_query_time", "cursor_time", "failed_attempts", "retry_queue"]),
            "the checkpoint row must carry the whole pending window and retry state: " + colNames)

    // leave no baselines behind for other runs
    dropOwnBaselines()
    assertEquals(0, ownBaselines().size(), "all spm_r8_ baselines must be dropped")
}
