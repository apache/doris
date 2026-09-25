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

suite("test_spm_review_round2", "spm") {

    // Second review round: SQL-level regression for the frozen-SQL / matching fixes.
    //
    // Covered here (end-to-end through CREATE BASELINE + EXPLAIN hit + replay result):
    //  - constant UNION branches survive the freeze (a dropped SELECT 1 row)
    //  - FROM-less projection aliases survive (SELECT 1 AS a keeps its header)
    //  - SUM(DISTINCT x) restores DISTINCT through the stage fold
    //  - a different explicit alias / derived-table LIMIT / partition selection must
    //    NOT match (no replay of the captured slice / partition)
    //  - special-character column identifiers stay quoted (no subtraction at replay)
    //  - case-only join-column collisions stay qualified (frozen SQL stays analyzable)
    //  - LIKE on SHOW BASELINE PLANS has real % / _ wildcard semantics
    //  - SET-ing an invalid capture regex fails at SET time and never persists

    // SPM regression pins the fallback switch CLOSED: a rewritten-plan failure must
    // surface as an error, never silently re-run the original query.
    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    // ==================== setup: tables (drop before use, keep after) ====================
    sql """DROP TABLE IF EXISTS spm_r2_t1"""
    sql """DROP TABLE IF EXISTS spm_r2_t2"""
    sql """DROP TABLE IF EXISTS spm_r2_part"""
    sql """DROP TABLE IF EXISTS spm_r2_special"""
    sql """DROP TABLE IF EXISTS spm_r2_case1"""
    sql """DROP TABLE IF EXISTS spm_r2_case2"""

    sql """
        CREATE TABLE spm_r2_t1 (k1 INT, k2 INT)
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    // k1 = 1 appears twice so SUM(DISTINCT k1) and SUM(k1) differ
    sql """INSERT INTO spm_r2_t1 VALUES (1, 10), (1, 10), (2, 20), (3, 30)"""
    sql """
        CREATE TABLE spm_r2_t2 (k1 INT, k2 INT)
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r2_t2 VALUES (1, 100), (2, 200)"""
    sql """
        CREATE TABLE spm_r2_part (k1 INT)
        PARTITION BY RANGE(k1)
        (PARTITION p1 VALUES [("-2147483648"), ("0")), PARTITION p2 VALUES [("0"), ("100")))
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r2_part VALUES (5), (6)"""
    sql """
        CREATE TABLE spm_r2_special (`a-b` INT, a INT, b INT, k INT)
        DUPLICATE KEY(`a-b`)
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r2_special VALUES (100, 1, 2, 5)"""
    sql """
        CREATE TABLE spm_r2_case1 (a INT)
        DUPLICATE KEY(a)
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r2_case1 VALUES (1)"""
    sql """
        CREATE TABLE spm_r2_case2 (A INT)
        DUPLICATE KEY(A)
        DISTRIBUTED BY HASH(A) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r2_case2 VALUES (1)"""

    // ==================== cleanup: drop this suite's leftover baselines ====================
    // Global baselines are cluster-wide state: every SHOW / drop is scoped to this
    // suite's tables (spm_r2_), so concurrent SPM suites are never touched.
    def ownBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r2_") }
    }
    def dropOwnBaselines = {
        ownBaselines().each { row ->
            sql """DROP BASELINE PLAN ${row[0]}"""
        }
    }
    dropOwnBaselines()
    assertEquals(0, ownBaselines().size(), "no spm_r2_ baseline should be left after cleanup")

    def explainOf = { String query -> sql("""EXPLAIN ${query}""").toString() }
    def createBaseline = { String text ->
        (sql("""CREATE GLOBAL BASELINE PLAN '${text}' WITH '${text}'""")[0][0] as Long)
    }

    // ==================== explicit output alias: preserved / not matched (review 13) ====================
    String aliasSql = "select k1 as x from spm_r2_t1 where k1 = 1"
    long aliasId = createBaseline(aliasSql)
    assertTrue(explainOf(aliasSql).contains("SPM baseline hit: id=${aliasId}"),
            "the same explicit alias must hit the baseline: " + explainOf(aliasSql))
    // the replay keeps the captured result header (SELECT 1 AS x)
    order_qt_alias_same """select k1 as x from spm_r2_t1 where k1 = 1"""

    String aliasMissSql = "select k1 as y from spm_r2_t1 where k1 = 1"
    assertFalse(explainOf(aliasMissSql).contains("SPM baseline hit"),
            "a different explicit alias must not replay the captured header: " + explainOf(aliasMissSql))
    order_qt_alias_other """select k1 as y from spm_r2_t1 where k1 = 1"""

    // ==================== derived-table LIMIT: not matched (review 2) ====================
    String derivedBind = "select * from (select k1 from spm_r2_t1 where k1 < 100 limit 1) x" +
            " join spm_r2_t2 on x.k1 = spm_r2_t2.k1"
    long derivedId = createBaseline(derivedBind)
    assertTrue(explainOf(derivedBind).contains("SPM baseline hit: id=${derivedId}"),
            "the identical derived-table LIMIT must hit: " + explainOf(derivedBind))

    String derivedMiss = "select * from (select k1 from spm_r2_t1 where k1 < 100 limit 2) x" +
            " join spm_r2_t2 on x.k1 = spm_r2_t2.k1"
    assertFalse(explainOf(derivedMiss).contains("SPM baseline hit"),
            "a different derived-table LIMIT must not replay the captured slice: " + explainOf(derivedMiss))
    // correct result slice: LIMIT 2 (a replayed LIMIT 1 would return a single row)
    order_qt_derived_limit_miss """select x.k1 from (select k1 from spm_r2_t1 where k1 < 100 limit 2) x
        join spm_r2_t2 on x.k1 = spm_r2_t2.k1 order by x.k1"""

    // ==================== partition selection: not matched (review 10) ====================
    String partBind = "select * from spm_r2_part partition(p2) where k1 = 5"
    long partId = createBaseline(partBind)
    assertTrue(explainOf(partBind).contains("SPM baseline hit: id=${partId}"),
            "the identical partition selection must hit: " + explainOf(partBind))
    order_qt_partition_hit """select * from spm_r2_part partition(p2) where k1 = 5"""

    String partMiss = "select * from spm_r2_part partition(p1) where k1 = 5"
    assertFalse(explainOf(partMiss).contains("SPM baseline hit"),
            "a different partition selection must not replay the captured partition: " + explainOf(partMiss))
    // p1 is empty: a wrongly replayed p2 baseline would return the row 5 here
    order_qt_partition_miss """select * from spm_r2_part partition(p1) where k1 = 5"""

    // ==================== constant UNION branch survives the freeze (review 1) ====================
    String unionSql = "select 1 as c union all select k1 as c from spm_r2_t1 where k1 = 3"
    long unionId = createBaseline(unionSql)
    assertTrue(explainOf(unionSql).contains("SPM baseline hit: id=${unionId}"),
            "the UNION query must hit its baseline: " + explainOf(unionSql))
    // both rows must come back: a dropped constant branch would return only k1 = 3
    order_qt_union_constant """select c from (select 1 as c union all select k1 as c from spm_r2_t1 where k1 = 3) t
        order by c"""

    // ==================== SUM(DISTINCT x) keeps DISTINCT through the fold (review 12) ====================
    String distinctSql = "select k2, sum(distinct k1) as s, max(k1) as m from spm_r2_t1 group by k2"
    long distinctId = createBaseline(distinctSql)
    assertTrue(explainOf(distinctSql).contains("SPM baseline hit: id=${distinctId}"),
            "the distinct aggregate query must hit its baseline: " + explainOf(distinctSql))
    // k2 = 10 has k1 = 1 twice: a lost DISTINCT would report 2 instead of 1
    order_qt_distinct_sum """select k2, sum(distinct k1) as s, max(k1) as m from spm_r2_t1 group by k2 order by k2"""

    // ==================== special-character column identifier (review 20) ====================
    String specialSql = "select `a-b` from spm_r2_special where k = 5"
    long specialId = createBaseline(specialSql)
    assertTrue(explainOf(specialSql).contains("SPM baseline hit: id=${specialId}"),
            "the special-character column query must hit its baseline: " + explainOf(specialSql))
    // 100 (the column value); an unquoted frozen SELECT a-b would re-parse as a - b = -1
    order_qt_special_column """select `a-b` from spm_r2_special where k = 5"""

    // ==================== case-only join-column collision (review 18) ====================
    String caseSql = "select c1.a from spm_r2_case1 c1 join spm_r2_case2 c2 on c1.a = c2.A where c1.a = 1"
    long caseId = createBaseline(caseSql)
    assertTrue(explainOf(caseSql).contains("SPM baseline hit: id=${caseId}"),
            "the case-collision join must hit its baseline: " + explainOf(caseSql))
    // an unqualified frozen ON (a = A) would be rejected as ambiguous and - with the
    // fallback pinned closed - fail the query instead of returning the row
    order_qt_case_collision """select c1.a from spm_r2_case1 c1 join spm_r2_case2 c2 on c1.a = c2.A where c1.a = 1"""

    // ==================== SHOW BASELINE PLANS LIKE wildcard semantics (review 6) ====================
    def likeMatch = sql("""SHOW BASELINE PLANS LIKE '%spm_r2_t1%'""")
    assertTrue(likeMatch.size() >= 2,
            "LIKE '%spm_r2_t1%' must use % as a wildcard and find the t1 baselines, got: ${likeMatch}")
    likeMatch.each { row ->
        assertTrue(row[1].toString().contains("spm_r2_t1"),
                "every LIKE-matched row must contain the pattern fragment: ${row}")
    }
    // _ matches exactly one character: ENABL_D has to match the whole status value
    // ENABLED (the pattern syntax forbids regex-significant characters, so the
    // wildcard test uses the status column rather than a bind_sql containing '=')
    def likeSingleChar = sql("""SHOW BASELINE PLANS LIKE 'ENABL_D'""")
    assertTrue(likeSingleChar.any { (it[0] as Long) == aliasId },
            "LIKE with _ must match the single-character wildcard, got: ${likeSingleChar}")
    // the pattern must match the WHOLE value: a fragment-only pattern finds nothing
    assertTrue(sql("""SHOW BASELINE PLANS LIKE 'spm_r2_t1'""").isEmpty(),
            "LIKE must match the whole value, not a substring")

    // ==================== capture regex is validated at SET time (review 7) ====================
    String patternBefore = sql("""SHOW VARIABLES LIKE 'plan_capture_include_pattern'""")[0][1].toString()
    test {
        sql """SET GLOBAL plan_capture_include_pattern = '[unclosed'"""
        exception "Invalid plan capture table regex"
    }
    test {
        sql """SET plan_capture_exclude_pattern = '('"""
        exception "Invalid plan capture table regex"
    }
    assertEquals(patternBefore,
            sql("""SHOW VARIABLES LIKE 'plan_capture_include_pattern'""")[0][1].toString(),
            "an invalid pattern must never be persisted")
    // a valid value still goes through SQL SET, and is restored afterwards
    sql """SET GLOBAL plan_capture_include_pattern = 'spm_r2_.*'"""
    assertEquals("spm_r2_.*",
            sql("""SHOW VARIABLES LIKE 'plan_capture_include_pattern'""")[0][1].toString())
    sql """SET GLOBAL plan_capture_include_pattern = '${patternBefore}'"""
    assertEquals(patternBefore,
            sql("""SHOW VARIABLES LIKE 'plan_capture_include_pattern'""")[0][1].toString())

    // ==================== set-operation quantifier survives the freeze ====================
    // The frozen keyword must come from the PHYSICAL qualifier: the parser maps an
    // omitted quantifier (and explicit DISTINCT) to DISTINCT, so a DISTINCT union frozen
    // as UNION ALL returns duplicate rows, while EXCEPT ALL / INTERSECT ALL lose their
    // multiplicity when ALL is dropped.
    sql """DROP TABLE IF EXISTS spm_r2_setop"""
    sql """
        CREATE TABLE spm_r2_setop (k INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r2_setop VALUES (1), (2), (2), (3)"""
    sql """DROP TABLE IF EXISTS spm_r2_setop2"""
    sql """
        CREATE TABLE spm_r2_setop2 (k INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r2_setop2 VALUES (2), (2), (3)"""

    // UNION (DISTINCT): {1,2,3} - a wrongly frozen UNION ALL would return the 7 raw rows
    String unionDistinctSetopSql = "select k from spm_r2_setop union select k from spm_r2_setop2 order by k"
    long unionDistinctSetopId = createBaseline(unionDistinctSetopSql)
    assertTrue(explainOf(unionDistinctSetopSql).contains("SPM baseline hit: id=${unionDistinctSetopId}"),
            "the UNION query must hit its baseline: " + explainOf(unionDistinctSetopSql))
    order_qt_setop_union_distinct """select k from spm_r2_setop union select k from spm_r2_setop2 order by k"""

    // UNION ALL: all 7 rows - a wrongly frozen DISTINCT union would collapse them
    String unionAllSetopSql = "select k from spm_r2_setop union all select k from spm_r2_setop2 order by k"
    long unionAllSetopId = createBaseline(unionAllSetopSql)
    assertTrue(explainOf(unionAllSetopSql).contains("SPM baseline hit: id=${unionAllSetopId}"),
            "the UNION ALL query must hit its baseline: " + explainOf(unionAllSetopSql))
    order_qt_setop_union_all """select k from spm_r2_setop union all select k from spm_r2_setop2 order by k"""

    // EXCEPT (DISTINCT): {1,3}
    String exceptSetopSql = "select k from spm_r2_setop except select 2 as k order by k"
    long exceptSetopId = createBaseline(exceptSetopSql)
    assertTrue(explainOf(exceptSetopSql).contains("SPM baseline hit: id=${exceptSetopId}"),
            "the EXCEPT query must hit its baseline: " + explainOf(exceptSetopSql))
    order_qt_setop_except """select k from spm_r2_setop except select 2 as k order by k"""

    // INTERSECT (DISTINCT): {2,3}
    String intersectSetopSql = "select k from spm_r2_setop intersect select k from spm_r2_setop2 order by k"
    long intersectSetopId = createBaseline(intersectSetopSql)
    assertTrue(explainOf(intersectSetopSql).contains("SPM baseline hit: id=${intersectSetopId}"),
            "the INTERSECT query must hit its baseline: " + explainOf(intersectSetopSql))
    order_qt_setop_intersect """select k from spm_r2_setop intersect select k from spm_r2_setop2 order by k"""

    // The engine currently rejects ALL-qualified EXCEPT / INTERSECT at analysis time, so
    // such a shape can never reach the decompiler today; the frozen keyword is still
    // derived from the PHYSICAL qualifier (unit-tested: EXCEPT ALL / INTERSECT ALL keep
    // their ALL) instead of being hardcoded, so the rendering stays correct if that
    // restriction is ever lifted.
    test {
        sql """select k from spm_r2_setop except all select 2 as k order by k"""
        exception "does not support ALL"
    }
    test {
        sql """select k from spm_r2_setop intersect all select k from spm_r2_setop2 order by k"""
        exception "does not support ALL"
    }

    // ==================== the view guard covers NESTED statements ====================
    // A view behind a CTE body or a subquery must neither be frozen nor replayed: SPM
    // would replay the expanded base-table plan, authorizing those base tables instead of
    // the view (a view-only user is denied, a base-table user skips the view check).
    sql """DROP VIEW IF EXISTS spm_r2_v"""
    sql """CREATE VIEW spm_r2_v AS SELECT k1 AS k FROM spm_r2_t1"""

    String cteViewSql = "with c as (select k from spm_r2_v) select k from c order by k"
    long cteViewId = createBaseline(cteViewSql)
    assertFalse(explainOf(cteViewSql).contains("SPM baseline hit"),
            "a CTE over a view must not be replayed: " + explainOf(cteViewSql))
    def cteViewPlanSql = sql """SELECT plan_sql FROM __internal_schema.spm_baselines WHERE id = ${cteViewId}"""
    assertTrue(cteViewPlanSql[0][0].toString().contains("spm_r2_v"),
            "a view baseline must keep the user plan_sql text: ${cteViewPlanSql}")
    assertFalse(cteViewPlanSql[0][0].toString().contains("internal."),
            "a view baseline must not freeze a decompiled base-table plan: ${cteViewPlanSql}")
    // the query still runs correctly through normal planning (view resolved + authorized)
    order_qt_view_cte """with c as (select k from spm_r2_v) select k from c order by k"""

    String subqueryViewSql = "select k1 from spm_r2_t2 where k1 in (select k from spm_r2_v)"
    long subqueryViewId = createBaseline(subqueryViewSql)
    assertFalse(explainOf(subqueryViewSql).contains("SPM baseline hit"),
            "an IN-subquery over a view must not be replayed: " + explainOf(subqueryViewSql))
    order_qt_view_subquery """select k1 from spm_r2_t2 where k1 in (select k from spm_r2_v)"""

    // control: the same shapes over BASE tables still rewrite
    String cteBaseSql = "with c as (select k1 from spm_r2_t1) select k1 from c order by k1"
    long cteBaseId = createBaseline(cteBaseSql)
    assertTrue(explainOf(cteBaseSql).contains("SPM baseline hit: id=${cteBaseId}"),
            "a CTE over base tables must still hit its baseline: " + explainOf(cteBaseSql))
    order_qt_cte_base """with c as (select k1 from spm_r2_t1) select k1 from c order by k1"""

    // leave no baselines behind for other runs
    dropOwnBaselines()
    assertEquals(0, ownBaselines().size(), "all spm_r2_ baselines must be dropped")
}
