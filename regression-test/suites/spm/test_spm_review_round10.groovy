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

suite("test_spm_review_round10", "spm") {

    // Tenth review round: end-to-end checks for
    //  - the two-phase LIMIT (SplitLimit) being collapsed before freezing, so a smaller
    //    captured LIMIT still honours a larger LIMIT / OFFSET of a matching query
    //  - GLOBAL baselines over temporary tables being rejected (the frozen SQL would carry
    //    the creator session's internal sessionId_#TEMP#_name)
    //  - the schema-identity fingerprint: a frozen SELECT * stops matching after
    //    ALTER TABLE ... ADD COLUMN instead of returning the creator-time columns
    //  - the PK/FK aggregate push down being excluded from frozen plans: after the
    //    constraint is dropped the replay still matches the un-frozen (direct) result

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    sql """DROP TABLE IF EXISTS spm_r10_lim"""
    sql """DROP TABLE IF EXISTS spm_r10_drift"""
    sql """DROP TABLE IF EXISTS spm_r10_temp"""
    // the child (FK holder) first: a leftover FK CONSTRAINT from an earlier run blocks
    // dropping the referenced table (p) with "primary key is referenced by foreign key"
    sql """DROP TABLE IF EXISTS spm_r10_c"""
    sql """DROP TABLE IF EXISTS spm_r10_p"""
    sql """
        CREATE TABLE spm_r10_lim (k INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r10_lim SELECT number FROM numbers("number" = "30")"""
    sql """
        CREATE TABLE spm_r10_drift (k INT, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r10_drift VALUES (1, 10), (2, 20)"""
    sql """
        CREATE TABLE spm_r10_p (k INT)
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r10_p VALUES (1), (2)"""
    sql """
        CREATE TABLE spm_r10_c (k INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r10_c VALUES (1), (1), (2)"""

    def ownBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r10_") }
    }
    def dropOwnBaselines = {
        ownBaselines().each { row ->
            sql """DROP BASELINE PLAN ${row[0]}"""
        }
    }
    dropOwnBaselines()
    assertEquals(0, ownBaselines().size(), "no spm_r10_ baseline should be left after cleanup")

    def explainOf = { String query -> sql("""EXPLAIN ${query}""").toString() }
    def createBaseline = { String text ->
        (sql('CREATE GLOBAL BASELINE PLAN "' + text + '" WITH "' + text + '"')[0][0] as Long)
    }

    // ==================== #6: the two-phase LIMIT must be collapsed ====================
    // SplitLimit turns LIMIT 10 into GLOBAL(10, 0) -> LOCAL(10, 0); serializing both
    // phases froze an inner AND an outer LIMIT 10, and the rewrite-time LIMIT merge only
    // reaches the outer block - a matching user query asking for LIMIT 20 kept returning
    // at most 10 rows.
    String limSql = "SELECT k FROM spm_r10_lim ORDER BY k LIMIT 10"
    long limId = createBaseline(limSql)
    String limPlanSql = sql(
            """SELECT plan_sql FROM __internal_schema.spm_baselines WHERE id = ${limId}""")[0][0].toString()
    int limitCount = (limPlanSql.toUpperCase() =~ /LIMIT/).count
    assertEquals(1, limitCount,
            "the frozen plan must carry ONE semantic LIMIT block (the two-phase pair is"
                    + " execution-only): " + limPlanSql)

    String growSql = "SELECT k FROM spm_r10_lim ORDER BY k LIMIT 20"
    assertTrue(explainOf(growSql).contains("SPM baseline hit: id=${limId}"),
            "the larger LIMIT shares the digest and must hit the baseline: " + explainOf(growSql))
    List<List<Object>> grown = sql(growSql)
    assertEquals(20, grown.size(),
            "the replay must adopt the USER's larger LIMIT instead of the frozen one: " + grown)
    order_qt_spm10_limit_grow """SELECT k FROM spm_r10_lim ORDER BY k LIMIT 20"""

    String offsetSql = "SELECT k FROM spm_r10_lim ORDER BY k LIMIT 20 OFFSET 5"
    assertTrue(explainOf(offsetSql).contains("SPM baseline hit: id=${limId}"),
            "the OFFSET variant must hit the baseline too: " + explainOf(offsetSql))
    List<List<Object>> offsetRows = sql(offsetSql)
    assertEquals(20, offsetRows.size(),
            "LIMIT 20 OFFSET 5 must return 20 rows (rows 6..25 of the 0-based numbers"
                    + " table = k 5..24): " + offsetRows)
    assertEquals(5, offsetRows.get(0).get(0) as int,
            "the replay must adopt the user's OFFSET: " + offsetRows)
    assertEquals(24, offsetRows.get(offsetRows.size() - 1).get(0) as int,
            "the replay must adopt the user's OFFSET: " + offsetRows)

    // ==================== #5: GLOBAL baselines over temporary tables ====================
    // The physical relation of a temp table carries the creator session's internal
    // sessionId_#TEMP#_name; freezing it would let ANOTHER session's identical text
    // resolve the creator's still-live temporary table. GLOBAL create must reject it
    // (the SESSION scope may keep its own temp table).
    sql """CREATE TEMPORARY TABLE spm_r10_temp (k INT)"""
    sql """INSERT INTO spm_r10_temp VALUES (1)"""
    test {
        sql 'CREATE GLOBAL BASELINE PLAN "SELECT k FROM spm_r10_temp WHERE k = 1" WITH "SELECT k FROM spm_r10_temp WHERE k = 1"'
        exception "temporary tables"
    }
    List<List<Object>> sessionRes = sql '''CREATE SESSION BASELINE PLAN
        "SELECT k FROM spm_r10_temp WHERE k = 1" WITH "SELECT k FROM spm_r10_temp WHERE k = 1"'''
    long sessionTempId = Long.parseLong(sessionRes[0][0].toString())
    assertTrue(sessionTempId >= (1L << 62),
            "a SESSION baseline over the session's own temp table stays allowed, got: ${sessionTempId}")
    assertEquals(1, sql("""SELECT k FROM spm_r10_temp WHERE k = 1""").size(),
            "the session-scope replay over the temp table must return the row")

    // ==================== #11: schema-identity fingerprint ====================
    // The bind key is built from the still-unbound query, so SELECT * keeps the same
    // digest / Level-3 tree after ALTER TABLE ... ADD COLUMN; without the fingerprint the
    // frozen plan (creator-time outputs) would silently keep matching and return the OLD
    // column set.
    String driftSql = "SELECT * FROM spm_r10_drift WHERE k = 1"
    long driftId = createBaseline(driftSql)
    assertTrue(explainOf(driftSql).contains("SPM baseline hit: id=${driftId}"),
            "the pre-ALTER query must hit its baseline: " + explainOf(driftSql))
    List<List<Object>> beforeAlter = sql(driftSql)
    assertEquals(2, beforeAlter.get(0).size(), "the frozen SELECT * returns the two columns")

    sql """ALTER TABLE spm_r10_drift ADD COLUMN extra INT DEFAULT 100"""
    assertFalse(explainOf(driftSql).contains("SPM baseline hit: id=${driftId}"),
            "after the ADD COLUMN the stale schema identity must fail closed: "
                    + explainOf(driftSql))
    List<List<Object>> afterAlter = sql(driftSql)
    assertEquals(3, afterAlter.get(0).size(),
            "the drifted query must expand the CURRENT star (including extra): " + afterAlter)
    assertEquals(100, afterAlter.get(0).get(2) as int,
            "the new column's value must come from the current table: " + afterAlter)
    order_qt_spm10_schema_drift """SELECT * FROM spm_r10_drift WHERE k = 1"""

    // ==================== #8: PK/FK aggregate push down stays out of frozen plans =====
    // PUSH_DOWN_AGG_THROUGH_JOIN_ON_PKFK derives its rewrite from the (mutable) FK
    // constraint; after DROP CONSTRAINT the pushed-down pre-aggregate would replay
    // duplicate undoubled rows while the original query aggregates above the join. The
    // rule is excluded, so the frozen plan and the direct plan agree.
    sql """ALTER TABLE spm_r10_p ADD CONSTRAINT spm_r10_p_pk PRIMARY KEY (k)"""
    sql """ALTER TABLE spm_r10_c ADD CONSTRAINT spm_r10_c_fk FOREIGN KEY (k) REFERENCES spm_r10_p(k)"""
    String aggSql = "SELECT p.k, count(*) AS c FROM spm_r10_p p JOIN spm_r10_c c ON p.k = c.k" +
            " GROUP BY p.k ORDER BY p.k"
    long aggId = createBaseline(aggSql)
    assertTrue(explainOf(aggSql).contains("SPM baseline hit: id=${aggId}"),
            "the constrained query must hit its baseline: " + explainOf(aggSql))

    sql """ALTER TABLE spm_r10_c DROP CONSTRAINT spm_r10_c_fk"""
    sql """INSERT INTO spm_r10_c VALUES (1), (2), (2)"""
    sql """set enable_spm_rewrite = false"""
    List<List<Object>> directRows = sql(aggSql)
    sql """set enable_spm_rewrite = true"""
    List<List<Object>> replayedRows = sql(aggSql)
    assertEquals(directRows.toString(), replayedRows.toString(),
            "the constraint-change replay must match the direct (un-frozen) result:"
                    + " direct=${directRows}, replayed=${replayedRows}")
}
