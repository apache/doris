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

suite("test_spm_r65_review", "spm") {

    // Round-65 review fixes with a SQL surface. Every block captures a baseline and
    // replays it with the SPM fallback switch pinned CLOSED, so a broken frozen text or a
    // dropped rewrite surfaces as an ERROR instead of silently re-running the original
    // query.
    //
    //  - #1  the checker's internal LAG frame must not be serialized: the frozen text is
    //        re-planned on replay and rejects an explicit LAG frame; the IN list also
    //        proves the retained SpmConstList values are replannable (the old probe
    //        skipped validation and the replay failed with "WindowFrame for LAG() must
    //        be null").
    //  - #3  COUNT(DISTINCT a,b) keeps the tuple identity: the frozen SQL must not add a
    //        second DISTINCT over the already-deduplicated stage (the replay returned 1
    //        instead of 2 for two tuples sharing one component value).
    //  - #5  the LATERAL VIEW generator name collides with the source column `X`
    //        case-insensitively: the frozen projection must not export two ambiguous
    //        bare references.
    //  - #6  GROUPING_ID is registered from its virtual expression: a congruent grouping
    //        key (a_b) must not make the frozen SQL return the grand-total value.
    //  - #7  SET_VAR hints carry in analyzer order (CTE definitions before the consuming
    //        query): the replay must end in the caller's effective time zone.
    //  - #8  every NULL-aware hash key survives decompilation (the frozen NOT IN must not
    //        drop a secondary key's NULL semantics).
    //  - #9  a nonmovable function above a child LIMIT stays in place (the replay must
    //        not evaluate the discarded row).
    //  - #12 a TVF-only baseline (EMPTY schema fingerprint) can be DROPPED: the
    //        IFNULL(...)='' predicate used to be rejected by DeleteFromCommand.
    //  - #15 inherited CTEs expand in their defining scope (the replay header must not
    //        come from the nested shadowing definition).
    //  - #16 a GROUPING SETS output list is aligned like an aggregate's (the replay must
    //        expose the caller's headers with the caller's values).
    //  - #18 keep-all CTE consumers below a UNION are recorded (a supported
    //        materialized-CTE shape must still hit and replay).

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    // ==================== setup: tables (drop before use, keep after) ====================
    sql """DROP TABLE IF EXISTS spm_r65b_t"""
    sql """
        CREATE TABLE spm_r65b_t (k INT, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r65b_t VALUES (1, 10), (2, 20), (3, 30)"""

    sql """DROP TABLE IF EXISTS spm_r65b_d"""
    sql """
        CREATE TABLE spm_r65b_d (a INT, b INT)
        DUPLICATE KEY(a)
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r65b_d VALUES (1, 7), (2, 7)"""

    sql """DROP TABLE IF EXISTS spm_r65b_lv"""
    sql """
        CREATE TABLE spm_r65b_lv (X INT, arr ARRAY<INT>, k INT)
        DUPLICATE KEY(X)
        DISTRIBUTED BY HASH(X) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r65b_lv VALUES (5, [1, 2], 1)"""

    sql """DROP TABLE IF EXISTS spm_r65b_g"""
    sql """
        CREATE TABLE spm_r65b_g (a INT, b INT, a_b INT)
        DUPLICATE KEY(a)
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r65b_g VALUES (1, 2, 3)"""

    sql """DROP TABLE IF EXISTS spm_r65b_tz"""
    sql """
        CREATE TABLE spm_r65b_tz (k BIGINT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r65b_tz VALUES (0)"""

    sql """DROP TABLE IF EXISTS spm_r65b_l"""
    sql """
        CREATE TABLE spm_r65b_l (a INT, b INT, z INT)
        DUPLICATE KEY(a)
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r65b_l VALUES (1, 2, 1)"""

    sql """DROP TABLE IF EXISTS spm_r65b_r"""
    sql """
        CREATE TABLE spm_r65b_r (c INT, d INT, f INT)
        DUPLICATE KEY(c)
        DISTRIBUTED BY HASH(c) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r65b_r VALUES (NULL, NULL, 2)"""

    sql """DROP TABLE IF EXISTS spm_r65b_bt"""
    sql """
        CREATE TABLE spm_r65b_bt (k INT, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r65b_bt VALUES (1, 1), (2, -1)"""

    sql """DROP TABLE IF EXISTS spm_r65b_u"""
    sql """
        CREATE TABLE spm_r65b_u (q INT)
        DUPLICATE KEY(q)
        DISTRIBUTED BY HASH(q) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r65b_u VALUES (3), (4)"""

    sql """DROP TABLE IF EXISTS spm_r65b_c"""
    sql """
        CREATE TABLE spm_r65b_c (k INT, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r65b_c VALUES (1, 10), (2, 20), (3, 30)"""

    // Global baselines are cluster-wide state and other SPM suites may run their own in
    // parallel: every SHOW here is scoped to THIS suite's tables.
    def ourTables = ["spm_r65b_t", "spm_r65b_d", "spm_r65b_lv", "spm_r65b_g", "spm_r65b_tz",
                     "spm_r65b_bt", "spm_r65b_c"]
    def ownBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { row ->
            ourTables.any { table -> row[1].toString().contains(table) }
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
    def createGlobal = { String bind, String plan ->
        List<List<Object>> created = sql("""CREATE GLOBAL BASELINE PLAN '${bind}' WITH '${plan}'""")
        assertEquals(1, created.size(), "CREATE should return one row, got: ${created}")
        return Long.parseLong(created[0][0].toString())
    }

    dropOwnBaselines()

    // ==================== #1: LAG() with an IN-list placeholder ====================
    String lagBind = "SELECT lag(k) OVER (ORDER BY k) AS p FROM spm_r65b_t WHERE k IN (1, 2)"
    long lagId = createGlobal(lagBind, lagBind)
    assertTrue(explainOf(lagBind).contains("SPM baseline hit: id=${lagId}"),
            "the LAG query must hit its baseline: " + explainOf(lagBind))
    order_qt_r65_lag_identical """
        SELECT lag(k) OVER (ORDER BY k) AS p FROM spm_r65b_t WHERE k IN (1, 2) ORDER BY p
    """
    // the substituted list variant replans the frozen text with the caller's values: an
    // explicit LAG frame in the frozen SQL used to fail here with "WindowFrame for LAG()
    // must be null"
    String lagVariant = "SELECT lag(k) OVER (ORDER BY k) AS p FROM spm_r65b_t WHERE k IN (1, 3)"
    assertTrue(explainOf(lagVariant).contains("SPM baseline hit: id=${lagId}"),
            "the substituted list variant must hit the same baseline: " + explainOf(lagVariant))
    order_qt_r65_lag_variant """
        SELECT lag(k) OVER (ORDER BY k) AS p FROM spm_r65b_t WHERE k IN (1, 3) ORDER BY p
    """

    // ==================== #3: COUNT(DISTINCT a, b) tuple identity ====================
    String distinctSql = "SELECT COUNT(DISTINCT a, b) AS c FROM spm_r65b_d"
    long distinctId = createGlobal(distinctSql, distinctSql)
    assertTrue(explainOf(distinctSql).contains("SPM baseline hit: id=${distinctId}"),
            "the tuple distinct query must hit its baseline: " + explainOf(distinctSql))
    order_qt_r65_distinct_tuple """SELECT COUNT(DISTINCT a, b) AS c FROM spm_r65b_d"""

    // ==================== #5: LATERAL VIEW generator name vs source column X ====================
    String lateralBind = "SELECT t.X AS a, lv.x AS b FROM spm_r65b_lv t" +
            " LATERAL VIEW explode(t.arr) lv AS x WHERE t.k IN (1, 2)"
    long lateralId = createGlobal(lateralBind, lateralBind)
    assertTrue(explainOf(lateralBind).contains("SPM baseline hit: id=${lateralId}"),
            "the lateral view query must hit its baseline: " + explainOf(lateralBind))
    order_qt_r65_lateral_view """
        SELECT t.X AS a, lv.x AS b FROM spm_r65b_lv t
        LATERAL VIEW explode(t.arr) lv AS x WHERE t.k IN (1, 2)
        ORDER BY a, b
    """

    // ==================== #6: GROUPING_ID with a congruent grouping key ====================
    String groupingSql = "SELECT a, b, a_b, GROUPING_ID(a, b) AS g FROM spm_r65b_g" +
            " GROUP BY GROUPING SETS ((a, b, a_b), ())"
    long groupingId = createGlobal(groupingSql, groupingSql)
    assertTrue(explainOf(groupingSql).contains("SPM baseline hit: id=${groupingId}"),
            "the GROUPING_ID query must hit its baseline: " + explainOf(groupingSql))
    order_qt_r65_grouping_id """
        SELECT a, b, a_b, GROUPING_ID(a, b) AS g FROM spm_r65b_g
        GROUP BY GROUPING SETS ((a, b, a_b), ())
        ORDER BY g, a, b, a_b
    """

    // ==================== #7: SET_VAR order across CTE blocks ====================
    // the hint literals need escaped quotes inside the CREATE's own quoted strings.
    // The rewrite path DECLINES statements carrying session-affecting SET_VAR hints (it
    // must not execute the captured payload while rewriting), so no hit is asserted here:
    // the ORDERING of the carried hints is pinned by
    // SPMRound65PlannerTest#testSetVarHintsCarryInAnalysisOrder (carrySetVarHints), and
    // this case keeps the SQL shape replayable end to end with the caller's effective
    // zone (00:00, the consuming query's assignment).
    String tzSql = "WITH c AS (SELECT /*+ SET_VAR(time_zone=\\'+08:00\\') */ k FROM spm_r65b_tz)" +
            " SELECT /*+ SET_VAR(time_zone=\\'+00:00\\') */ from_unixtime(k) AS t FROM c"
    // the CAPTURE still has to succeed (the stored SET_VAR payload must not leak into
    // the session of the CREATE either)
    createGlobal(tzSql, tzSql)
    order_qt_r65_set_var_order """
        WITH c AS (SELECT /*+ SET_VAR(time_zone='+08:00') */ k FROM spm_r65b_tz)
        SELECT /*+ SET_VAR(time_zone='+00:00') */ from_unixtime(k) AS t FROM c
        ORDER BY t
    """

    // ==================== #8: multi-key NULL-aware anti join ====================
    String naajSql = "SELECT l.a FROM spm_r65b_l l LEFT NULL_AWARE ANTI JOIN spm_r65b_r r" +
            " ON l.a = r.c AND l.b = r.d AND r.f > l.z"
    long naajId = createGlobal(naajSql, naajSql)
    assertTrue(explainOf(naajSql).contains("SPM baseline hit: id=${naajId}"),
            "the NULL-aware anti join must hit its baseline: " + explainOf(naajSql))
    // the NULL key row of r marks the probe matched: the anti join returns NO row
    order_qt_r65_null_aware_anti """SELECT l.a FROM spm_r65b_l l LEFT NULL_AWARE ANTI JOIN spm_r65b_r r
        ON l.a = r.c AND l.b = r.d AND r.f > l.z"""

    // ==================== #9: nonmovable evaluation above a child LIMIT ====================
    sql """set disable_nereids_rules = 'PUSH_DOWN_PROJECT_THROUGH_LIMIT'"""
    String nonMovableSql = "SELECT bitmap_to_string(to_bitmap_with_check(v)) AS b" +
            " FROM (SELECT k, v FROM spm_r65b_bt ORDER BY k LIMIT 1) s"
    long nonMovableId = createGlobal(nonMovableSql, nonMovableSql)
    assertTrue(explainOf(nonMovableSql).contains("SPM baseline hit: id=${nonMovableId}"),
            "the nonmovable query must hit its baseline: " + explainOf(nonMovableSql))
    // only the FIRST row (v = 1) may be evaluated; hoisting the projection would check
    // the discarded v = -1 and fail
    order_qt_r65_nonmovable_above_limit """
        SELECT bitmap_to_string(to_bitmap_with_check(v)) AS b
        FROM (SELECT k, v FROM spm_r65b_bt ORDER BY k LIMIT 1) s
    """
    sql """set disable_nereids_rules = ''"""

    // ==================== #12: TVF-only baseline (empty fingerprint) can be DROPPED ====================
    String tvfSql = "SELECT number FROM numbers(\"number\"=\"5\")"
    long tvfId = createGlobal(tvfSql, tvfSql)
    assertTrue(explainOf(tvfSql).contains("SPM baseline hit: id=${tvfId}"),
            "the TVF-only query must hit its baseline: " + explainOf(tvfSql))
    order_qt_r65_tvf_replay """SELECT number FROM numbers("number"="5") ORDER BY number"""
    sql """DROP BASELINE PLAN ${tvfId}"""
    assertFalse(sql("""SHOW BASELINE PLANS""").any { Long.parseLong(it[0].toString()) == tvfId },
            "the TVF-only baseline must be gone after DROP")

    // ==================== #15: inherited CTE expanded in its defining scope ====================
    String inheritedSql = "WITH c AS (SELECT k AS a FROM spm_r65b_t), d AS (SELECT * FROM c)" +
            " SELECT * FROM (WITH c AS (SELECT q AS b FROM spm_r65b_u) SELECT * FROM d) s"
    long inheritedId = createGlobal(inheritedSql, inheritedSql)
    assertTrue(explainOf(inheritedSql).contains("SPM baseline hit: id=${inheritedId}"),
            "the inherited CTE query must hit its baseline: " + explainOf(inheritedSql))
    order_qt_r65_inherited_cte """
        WITH c AS (SELECT k AS a FROM spm_r65b_t), d AS (SELECT * FROM c)
        SELECT * FROM (WITH c AS (SELECT q AS b FROM spm_r65b_u) SELECT * FROM d) s
        ORDER BY a
    """

    // ==================== #16: GROUPING SETS (ROLLUP) output list alignment ====================
    String rollupBind = "SELECT sum(v + 1) AS s FROM spm_r65b_t GROUP BY ROLLUP(k)"
    long rollupId = createGlobal(rollupBind, rollupBind)
    String rollupVariant = "SELECT sum(v + 2) AS s FROM spm_r65b_t GROUP BY ROLLUP(k)"
    assertTrue(explainOf(rollupVariant).contains("SPM baseline hit: id=${rollupId}"),
            "the rollup variant must hit its baseline: " + explainOf(rollupVariant))
    order_qt_r65_rollup_variant """
        SELECT sum(v + 2) AS s FROM spm_r65b_t GROUP BY ROLLUP(k) ORDER BY s
    """

    // ==================== #18: supported materialized-CTE shape below a UNION ====================
    String unionCteBind = "WITH c AS (SELECT k + 1 AS x, v + 1 AS y FROM spm_r65b_c)" +
            " SELECT a.x FROM c a JOIN (SELECT y FROM c UNION ALL SELECT q FROM spm_r65b_u) b" +
            " ON a.x = b.y WHERE a.x IN (1, 2)"
    long unionCteId = createGlobal(unionCteBind, unionCteBind)
    String unionCteVariant = "WITH c AS (SELECT k + 1 AS x, v + 1 AS y FROM spm_r65b_c)" +
            " SELECT a.x FROM c a JOIN (SELECT y FROM c UNION ALL SELECT q FROM spm_r65b_u) b" +
            " ON a.x = b.y WHERE a.x IN (2, 3)"
    assertTrue(explainOf(unionCteVariant).contains("SPM baseline hit: id=${unionCteId}"),
            "the union CTE variant must hit its baseline: " + explainOf(unionCteVariant))
    order_qt_r65_union_cte """
        WITH c AS (SELECT k + 1 AS x, v + 1 AS y FROM spm_r65b_c)
        SELECT a.x FROM c a JOIN (SELECT y FROM c UNION ALL SELECT q FROM spm_r65b_u) b
        ON a.x = b.y WHERE a.x IN (2, 3)
        ORDER BY a.x
    """

    // ==================== #2: an open star followed by another star ====================
    String starBind = "SELECT u.*, s.* FROM spm_r65b_u u" +
            " CROSS JOIN (SELECT k + 1 AS c1, v + 1 AS c2 FROM spm_r65b_t) s"
    long starId = createGlobal(starBind, starBind)
    String starVariant = "SELECT u.*, s.* FROM spm_r65b_u u" +
            " CROSS JOIN (SELECT k + 2 AS c1, v + 2 AS c2 FROM spm_r65b_t) s"
    assertTrue(explainOf(starVariant).contains("SPM baseline hit: id=${starId}"),
            "the star variant must hit its baseline: " + explainOf(starVariant))
    order_qt_r65_following_star """
        SELECT u.*, s.* FROM spm_r65b_u u
        CROSS JOIN (SELECT k + 2 AS c1, v + 2 AS c2 FROM spm_r65b_t) s
        ORDER BY 1, 2, 3
    """

    // ==================== #13: literal template syntax in persisted SQL ====================
    String templateSql = "SELECT k AS `\${id}` FROM spm_r65b_t WHERE k IN (1, 2)"
    long templateId = createGlobal(templateSql, templateSql)
    assertTrue(explainOf(templateSql).contains("SPM baseline hit: id=${templateId}"),
            "the template-alias query must hit its baseline: " + explainOf(templateSql))
    order_qt_r65_template_alias """SELECT k AS `\${id}` FROM spm_r65b_t WHERE k IN (1, 2) ORDER BY 1"""
}
