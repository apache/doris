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

suite("test_spm_review_round11", "spm") {

    // Eleventh review round: end-to-end checks for
    //  - the distributed TopN fold: a captured LIMIT 200 OFFSET 1 must freeze as ONE
    //    semantic LIMIT (MERGE(200,1) -> LOCAL(201,0) is one plan), so a later matched
    //    LIMIT 300 OFFSET 1 really returns 300 rows instead of being capped at 201
    //  - set operations (UNION / EXCEPT / INTERSECT): the operands must be rendered as
    //    unaliased query terms, otherwise the persisted frozen text cannot be re-parsed
    //    on refresh / restart ("Every derived table must have its own alias" / a
    //    trailing alias is not a legal set operand)
    //  - the temporary-partition namespace: PARTITION(p1) and TEMPORARY PARTITION(...)
    //    are different scan identities, a formal selection must not replay a baseline
    //    frozen against the temp namespace (and vice versa)
    //  - literal-free frozen joins: a decompile without any placeholder call is STILL
    //    frozen (plan_frozen = true, no more dependence on placeholder presence), so an
    //    immediate replay uses the stored join structure instead of falling back to the
    //    original pre-optimization tree

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    sql """DROP TABLE IF EXISTS spm_r11_topn"""
    sql """DROP TABLE IF EXISTS spm_r11_ua"""
    sql """DROP TABLE IF EXISTS spm_r11_ub"""
    sql """DROP TABLE IF EXISTS spm_r11_pt"""
    sql """DROP TABLE IF EXISTS spm_r11_ja"""
    sql """DROP TABLE IF EXISTS spm_r11_jb"""
    sql """
        CREATE TABLE spm_r11_topn (k INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r11_topn SELECT number FROM numbers("number" = "500")"""
    sql """
        CREATE TABLE spm_r11_ua (k INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r11_ua VALUES (0), (1), (2), (3), (4)"""
    sql """
        CREATE TABLE spm_r11_ub (k INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r11_ub VALUES (2), (3), (4), (5), (6)"""
    sql """
        CREATE TABLE spm_r11_pt (k INT)
        DUPLICATE KEY(k)
        PARTITION BY RANGE(k)
        (
            PARTITION p1 VALUES [("0"), ("100")),
            PARTITION p2 VALUES [("100"), ("200"))
        )
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r11_pt VALUES (1), (2), (150)"""
    sql """
        CREATE TABLE spm_r11_ja (k INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r11_ja VALUES (1), (2), (3)"""
    sql """
        CREATE TABLE spm_r11_jb (k INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r11_jb VALUES (1), (2), (3), (4)"""

    def ownBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r11_") }
    }
    def dropOwnBaselines = {
        ownBaselines().each { row ->
            sql """DROP BASELINE PLAN ${row[0]}"""
        }
    }
    dropOwnBaselines()
    assertEquals(0, ownBaselines().size(), "no spm_r11_ baseline should be left after cleanup")

    def explainOf = { String query -> sql("""EXPLAIN ${query}""").toString() }
    def createBaseline = { String text ->
        (sql('CREATE GLOBAL BASELINE PLAN "' + text + '" WITH "' + text + '"')[0][0] as Long)
    }
    def planSqlOf = { long id ->
        sql("""SELECT plan_sql FROM __internal_schema.spm_baselines WHERE id = ${id}""")[0][0].toString()
    }

    // ==================== #5: the distributed TopN continuation folds ====================
    // Nereids builds MERGE(limit=200, offset=1) -> LOCAL(limit=201, offset=0). The old
    // `inner.limit <= outer.limit` test was false for every positive offset, so the pair
    // was frozen as a semantic inner LIMIT 201 + outer LIMIT 1, 200: a matching query
    // (matching ignores the top-level values) stayed capped at 201 inputs.
    String topnSql = "SELECT k FROM spm_r11_topn ORDER BY k LIMIT 200 OFFSET 1"
    long topnId = createBaseline(topnSql)
    String topnPlanSql = planSqlOf(topnId)
    int limitCount = (topnPlanSql.toUpperCase() =~ /LIMIT/).count
    assertEquals(1, limitCount,
            "the frozen plan must carry ONE semantic LIMIT block (MERGE+LOCAL is one"
                    + " plan): " + topnPlanSql)

    String grownTopnSql = "SELECT k FROM spm_r11_topn ORDER BY k LIMIT 300 OFFSET 1"
    assertTrue(explainOf(grownTopnSql).contains("SPM baseline hit: id=${topnId}"),
            "the larger LIMIT / OFFSET variant must hit the baseline: " + explainOf(grownTopnSql))
    List<List<Object>> grownTopn = sql(grownTopnSql)
    assertEquals(300, grownTopn.size(),
            "the replay must return 300 rows (not cap at the captured 201 inputs): "
                    + grownTopn.size())
    assertEquals(1, grownTopn.get(0).get(0) as int,
            "OFFSET 1 skips k = 0: " + grownTopn.get(0))
    assertEquals(300, grownTopn.get(299).get(0) as int,
            "the 300th row is k = 300: " + grownTopn.get(299))
    order_qt_spm11_topn_grow """SELECT k FROM spm_r11_topn ORDER BY k LIMIT 300 OFFSET 1"""

    // ==================== #6: set operands are unaliased query terms ====================
    // "(SELECT ...) t_N" on either side of UNION / EXCEPT / INTERSECT is not a legal set
    // operand; the frozen text could not be re-parsed by a refresh / restart. Every
    // captured set-plan text must EXPLAIN (i.e. re-parse) successfully, and the replay
    // must agree with the direct execution.
    [
            [ "UNION ALL", "SELECT k FROM spm_r11_ua UNION ALL SELECT k FROM spm_r11_ub" ],
            [ "EXCEPT", "SELECT k FROM spm_r11_ua EXCEPT SELECT k FROM spm_r11_ub" ],
            [ "INTERSECT", "SELECT k FROM spm_r11_ua INTERSECT SELECT k FROM spm_r11_ub" ]
    ].each { pair ->
        String keyword = pair[0]
        String setSql = pair[1]
        long setId = createBaseline(setSql)
        String setPlanSql = planSqlOf(setId)
        // re-parse check: the old aliased-operand text failed here
        def reparse = sql("""EXPLAIN ${setPlanSql}""")
        assertTrue(reparse != null && reparse.size() > 0,
                "${keyword}: the frozen plan text must re-parse: " + setPlanSql)
        assertFalse((setPlanSql =~ /\)\s+t_\d+\s+${keyword.replace(" ", "\\s+")}/).find(),
                "${keyword}: a set operand must carry no trailing alias: " + setPlanSql)
        assertTrue(explainOf(setSql).contains("SPM baseline hit: id=${setId}"),
                "${keyword}: the capture text must hit its baseline: " + explainOf(setSql))
        sql """set enable_spm_rewrite = false"""
        List<List<Object>> directRows = sql(setSql)
        sql """set enable_spm_rewrite = true"""
        List<List<Object>> replayedRows = sql(setSql)
        assertEquals(directRows.sort(), replayedRows.sort(),
                "${keyword}: the replay must agree with the direct execution (row order is"
                        + " not part of the contract): direct=${directRows}, replayed=${replayedRows}")
    }
    order_qt_spm11_union_all """SELECT k FROM spm_r11_ua UNION ALL SELECT k FROM spm_r11_ub"""
    order_qt_spm11_except """SELECT k FROM spm_r11_ua EXCEPT SELECT k FROM spm_r11_ub"""
    order_qt_spm11_intersect """SELECT k FROM spm_r11_ua INTERSECT SELECT k FROM spm_r11_ub"""

    // ==================== #7: temporary-partition namespace ====================
    // PARTITION(p1) and TEMPORARY PARTITION(tp1) are different scan identities even
    // when a name is reused: a formal selection must not replay a baseline frozen
    // against another namespace (the frozen SQL would keep reading the wrong data).
    String formalSql = "SELECT k FROM spm_r11_pt PARTITION(p1)"
    long formalId = createBaseline(formalSql)
    assertTrue(explainOf(formalSql).contains("SPM baseline hit: id=${formalId}"),
            "the formal partition query must hit its own baseline: " + explainOf(formalSql))
    assertEquals(2, sql(formalSql).size(), "p1 holds k = 1, 2")
    order_qt_spm11_formal_partition """SELECT k FROM spm_r11_pt PARTITION(p1)"""

    sql """ALTER TABLE spm_r11_pt ADD TEMPORARY PARTITION tp1 VALUES [("0"), ("100"))"""
    String tempSql = "SELECT k FROM spm_r11_pt TEMPORARY PARTITION(tp1)"
    assertFalse(explainOf(tempSql).contains("SPM baseline hit: id=${formalId}"),
            "a TEMPORARY PARTITION query must NOT match the PARTITION baseline: "
                    + explainOf(tempSql))
    // the temp partition is new (and empty): the direct query returns no rows and must
    // never silently read p1 through the baseline
    assertEquals(0, sql(tempSql).size(),
            "the temp partition holds no rows: " + sql(tempSql))
    // the formal query still hits its own baseline
    assertTrue(explainOf(formalSql).contains("SPM baseline hit: id=${formalId}"),
            "the formal query must keep matching after the temp partition was added: "
                    + explainOf(formalSql))
    sql """ALTER TABLE spm_r11_pt DROP TEMPORARY PARTITION tp1"""

    // ==================== #8: literal-free frozen plans ====================
    // A literal-free optimized join contains no spm_const* call, yet the decompile
    // SUCCEEDED: the persisted provenance must be plan_frozen = true (the old marker
    // check recorded false), otherwise immediate / refreshed replays reject the frozen
    // text and lose the stored join / distribution choice.
    String joinSql = "SELECT a.k FROM spm_r11_ja a JOIN spm_r11_jb b ON a.k = b.k"
    long joinId = createBaseline(joinSql)
    String joinFrozen = sql(
            """SELECT plan_frozen FROM __internal_schema.spm_baselines WHERE id = ${joinId}""")[0][0]
                    .toString()
    String joinFrozenText = joinFrozen?.toString()?.toLowerCase()
    assertEquals("true", joinFrozenText == "1" ? "true" : joinFrozenText,
            "a successful decompile is frozen even without any placeholder: " + joinFrozen)
    assertTrue(explainOf(joinSql).contains("SPM baseline hit: id=${joinId}"),
            "the literal-free join must replay its frozen structure: " + explainOf(joinSql))
    sql """set enable_spm_rewrite = false"""
    List<List<Object>> directJoin = sql(joinSql)
    sql """set enable_spm_rewrite = true"""
    List<List<Object>> replayedJoin = sql(joinSql)
    assertEquals(directJoin.sort(), replayedJoin.sort(),
            "the frozen join replay must agree with the direct execution (row order is not"
                    + " part of the contract): direct=${directJoin}, replayed=${replayedJoin}")
    order_qt_spm11_join """SELECT a.k FROM spm_r11_ja a JOIN spm_r11_jb b ON a.k = b.k ORDER BY a.k"""

    // the SESSION scope must behave identically (an immediate replay of the same text)
    List<List<Object>> sessionRes = sql('''CREATE SESSION BASELINE PLAN
        "SELECT a.k FROM spm_r11_ja a JOIN spm_r11_jb b ON a.k = b.k"
        WITH "SELECT a.k FROM spm_r11_ja a JOIN spm_r11_jb b ON a.k = b.k"''')
    long sessionJoinId = Long.parseLong(sessionRes[0][0].toString())
    assertTrue(sessionJoinId >= (1L << 62),
            "a SESSION baseline id lives in the session range, got: ${sessionJoinId}")
    assertTrue(explainOf(joinSql).contains("SPM baseline hit: id=${sessionJoinId}"),
            "the SESSION baseline must serve the immediate replay: " + explainOf(joinSql))
    assertEquals(directJoin.sort(), sql(joinSql).sort(),
            "the SESSION replay must agree with the direct execution")
}
