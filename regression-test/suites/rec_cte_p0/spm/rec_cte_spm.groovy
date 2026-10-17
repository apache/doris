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

suite("spm_rec_cte", "spm") {

    // SPM baseline DDL + match verification on recursive CTE (WITH RECURSIVE)
    // queries. A recursive CTE parses to a LogicalCTE whose body self-references the
    // CTE name; the SPM decompiler supports the recursive-union physical plan, so the
    // CREATE path must produce a frozen planSql that keeps the WITH RECURSIVE shape
    // AND parameterizes every literal (_spm_const_var) instead of falling back to the
    // user-supplied planSql. The rewrite path must replay that frozen text with the
    // user's literals inside the anchor, the recursive branch and any outer filter.
    //
    // 1. CREATE BASELINE PLAN from the query text (bind + plan); the returned
    //    baseline id locates the own row in SHOW BASELINE PLANS (its plan_sql is
    //    asserted below to prove the decompile happened) and is used to drop the
    //    baseline afterwards.
    // 2. Match verification: the query itself and a similar query (same structure,
    //    different literal values) run through the SPM rewrite path
    //    (enable_spm_rewrite=true); each must produce the same result as without SPM
    //    (rewrite hit or safe fallback - never an error or a different result).
    // 3. Every case also emits the three stored baseline columns into the OUT file
    //    (spm_bind_sql / spm_bind_digest / spm_plan_sql), exactly like the TPCDS /
    //    TPCH SPM suites, so the frozen bind and plan text of the recursive CTE is
    //    reviewable from the .out file.

    if (isCloudMode()) {
        return
    }
    // the suite runs in the framework-provided context db (created per suite directory)
    sql 'set enable_nereids_planner=true'
    // SPM regression pins the fallback switch CLOSED: a rewritten-plan failure must
    // surface as an error, never silently re-run the original query.
    sql 'set enable_spm_fallback=false'
    sql 'set enable_fallback_to_original_planner=false'

    // Reads the stored plan_sql of a baseline (SHOW BASELINE PLANS column index 4), so
    // the tests can assert that CREATE actually decompiled the recursive CTE instead
    // of keeping the user-supplied planSql as the legacy fallback.
    def fetchPlanSql = { long baselineId ->
        def rows = sql "SHOW BASELINE PLANS WHERE id = ${baselineId}"
        return rows[0][4].toString()
    }

    // ==================== self-contained table for a table-driven recursive CTE ====================
    sql "DROP TABLE IF EXISTS spm_rec_edge"
    sql """
        CREATE TABLE spm_rec_edge (
            node1id int,
            node2id int
        ) DUPLICATE KEY (node1id)
        DISTRIBUTED BY HASH(node1id) BUCKETS 1 PROPERTIES ('replication_num' = '1');
    """
    sql """
        INSERT INTO spm_rec_edge VALUES
        (1, 3), (1, 5), (2, 4), (2, 5), (2, 10), (3, 1),
        (3, 5), (3, 8), (3, 10), (5, 3), (5, 4), (5, 8),
        (6, 3), (6, 4), (7, 4), (8, 1), (9, 4);
    """

    // ==================== case 1: constant anchor + constant recursive branch ====================
    def bindSql1 = """WITH RECURSIVE t1(k1, k2) AS (
        SELECT 1, 2
        UNION
        SELECT 3, 4
        FROM t1 GROUP BY k1
    ) SELECT * FROM t1 ORDER BY 1, 2"""
    def createRes1 = sql ("CREATE GLOBAL BASELINE PLAN \"" + bindSql1.replace('"', '\\"') + "\" WITH \"" + bindSql1.replace('"', '\\"') + "\"")
    long id1 = Long.parseLong(createRes1[0][0].toString())

    try {
        // the recursive CTE must be decompiled into a frozen planSql (a fallback
        // would keep the raw bindSql text verbatim, with real literals and no placeholders)
        def planSql1 = fetchPlanSql(id1)
        assertTrue(planSql1.contains("_spm_const_var"),
                "recursive CTE plan_sql must be decompiled with placeholders, got: " + planSql1)
        assertTrue(planSql1.toUpperCase().contains("WITH RECURSIVE"),
                "recursive CTE plan_sql must keep the WITH RECURSIVE shape, got: " + planSql1)
        assertTrue(planSql1 != bindSql1,
                "recursive CTE plan_sql must be the decompiled text, not the raw bindSql")

        // original query: SPM rewrite must keep the result
        sql 'set enable_spm_rewrite=true'
        def origWithSpm = sql """WITH RECURSIVE t1(k1, k2) AS (
            SELECT 1, 2
            UNION
            SELECT 3, 4
            FROM t1 GROUP BY k1
        ) SELECT * FROM t1 ORDER BY 1, 2"""
        sql 'set enable_spm_rewrite=false'
        def origWithoutSpm = sql """WITH RECURSIVE t1(k1, k2) AS (
            SELECT 1, 2
            UNION
            SELECT 3, 4
            FROM t1 GROUP BY k1
        ) SELECT * FROM t1 ORDER BY 1, 2"""
        assertEquals(origWithSpm, origWithoutSpm,
                "SPM rewrite must preserve the result of the original recursive CTE query")

        // similar query (different anchor / recursive literals) must hit and rewrite
        sql 'set enable_spm_rewrite=true'
        def similarWithSpm = sql """WITH RECURSIVE t1(k1, k2) AS (
            SELECT 10, 20
            UNION
            SELECT 30, 40
            FROM t1 GROUP BY k1
        ) SELECT * FROM t1 ORDER BY 1, 2"""
        sql 'set enable_spm_rewrite=false'
        def similarWithoutSpm = sql """WITH RECURSIVE t1(k1, k2) AS (
            SELECT 10, 20
            UNION
            SELECT 30, 40
            FROM t1 GROUP BY k1
        ) SELECT * FROM t1 ORDER BY 1, 2"""
        assertEquals(similarWithSpm, similarWithoutSpm,
                "SPM rewrite must preserve the result of a similar recursive CTE query")

        // both original and similar must actually hit the baseline
        sql 'set enable_spm_rewrite=true'
        def explainOrig = sql """EXPLAIN WITH RECURSIVE t1(k1, k2) AS (
            SELECT 1, 2
            UNION
            SELECT 3, 4
            FROM t1 GROUP BY k1
        ) SELECT * FROM t1 ORDER BY 1, 2"""
        def explainSimilar = sql """EXPLAIN WITH RECURSIVE t1(k1, k2) AS (
            SELECT 10, 20
            UNION
            SELECT 30, 40
            FROM t1 GROUP BY k1
        ) SELECT * FROM t1 ORDER BY 1, 2"""
        sql 'set enable_spm_rewrite=false'
        assertTrue(explainOrig.toString().contains("SPM baseline hit: id=" + id1),
                "EXPLAIN of the original recursive CTE should report SPM baseline hit id " + id1 + ", got: " + explainOrig)
        assertTrue(explainSimilar.toString().contains("SPM baseline hit: id=" + id1),
                "EXPLAIN of the similar recursive CTE should report SPM baseline hit id " + id1 + ", got: " + explainSimilar)

        // emit the stored baseline columns into the OUT file, same blocks as the
        // TPCDS/TPCH SPM suites (spm_bind_sql / spm_bind_digest / spm_plan_sql)
        order_qt_spm_bind_sql """SELECT bind_sql FROM __internal_schema.spm_baselines WHERE id = ${id1}"""
        order_qt_spm_bind_digest """SELECT bind_sql_digest FROM __internal_schema.spm_baselines WHERE id = ${id1}"""
        order_qt_spm_plan_sql """SELECT plan_sql FROM __internal_schema.spm_baselines WHERE id = ${id1}"""
    } finally {
        sql """DROP BASELINE PLAN IF EXISTS ${id1}"""
    }

    // ==================== case 2: recursion guard literal (WHERE k1 < N) ====================
    def bindSql2 = """WITH RECURSIVE t1(k1) AS (
        SELECT cast(1 as bigint) k1
        UNION ALL
        SELECT k1 + 1 FROM t1 WHERE k1 < 5
    ) SELECT * FROM t1 ORDER BY 1"""
    def createRes2 = sql ("CREATE GLOBAL BASELINE PLAN \"" + bindSql2.replace('"', '\\"') + "\" WITH \"" + bindSql2.replace('"', '\\"') + "\"")
    long id2 = Long.parseLong(createRes2[0][0].toString())

    try {
        // the recursion guard literal must be parameterized in the frozen planSql
        def planSql2 = fetchPlanSql(id2)
        assertTrue(planSql2.contains("_spm_const_var"),
                "recursive CTE guard literal must be parameterized in plan_sql, got: " + planSql2)
        assertTrue(planSql2.toUpperCase().contains("WITH RECURSIVE"),
                "recursive CTE guard plan_sql must keep the WITH RECURSIVE shape, got: " + planSql2)

        sql 'set enable_spm_rewrite=true'
        def guardWithSpm = sql """WITH RECURSIVE t1(k1) AS (
            SELECT cast(1 as bigint) k1
            UNION ALL
            SELECT k1 + 1 FROM t1 WHERE k1 < 10
        ) SELECT * FROM t1 ORDER BY 1"""
        sql 'set enable_spm_rewrite=false'
        def guardWithoutSpm = sql """WITH RECURSIVE t1(k1) AS (
            SELECT cast(1 as bigint) k1
            UNION ALL
            SELECT k1 + 1 FROM t1 WHERE k1 < 10
        ) SELECT * FROM t1 ORDER BY 1"""
        assertEquals(guardWithSpm, guardWithoutSpm,
                "SPM rewrite must preserve the result of a recursive CTE with a different guard literal")

        sql 'set enable_spm_rewrite=true'
        def explainGuard = sql """EXPLAIN WITH RECURSIVE t1(k1) AS (
            SELECT cast(1 as bigint) k1
            UNION ALL
            SELECT k1 + 1 FROM t1 WHERE k1 < 10
        ) SELECT * FROM t1 ORDER BY 1"""
        sql 'set enable_spm_rewrite=false'
        assertTrue(explainGuard.toString().contains("SPM baseline hit: id=" + id2),
                "EXPLAIN of the recursive CTE guard variant should report SPM baseline hit id " + id2 + ", got: " + explainGuard)

        // stored baseline columns in the OUT file (bind_sql / bind_sql_digest / plan_sql)
        order_qt_spm_bind_sql """SELECT bind_sql FROM __internal_schema.spm_baselines WHERE id = ${id2}"""
        order_qt_spm_bind_digest """SELECT bind_sql_digest FROM __internal_schema.spm_baselines WHERE id = ${id2}"""
        order_qt_spm_plan_sql """SELECT plan_sql FROM __internal_schema.spm_baselines WHERE id = ${id2}"""
    } finally {
        sql """DROP BASELINE PLAN IF EXISTS ${id2}"""
    }

    // ==================== case 3: table-driven recursive CTE (graph reachability) ====================
    def bindSql3 = """WITH RECURSIVE t1(k1, k2) AS (
        SELECT node1id AS k1, node2id AS k2 FROM spm_rec_edge
        UNION
        SELECT k1, cast(sum(k2) as int)
        FROM t1 GROUP BY k1
    ) SELECT * FROM t1 ORDER BY 1, 2"""
    def createRes3 = sql ("CREATE GLOBAL BASELINE PLAN \"" + bindSql3.replace('"', '\\"') + "\" WITH \"" + bindSql3.replace('"', '\\"') + "\"")
    long id3 = Long.parseLong(createRes3[0][0].toString())

    try {
        // case 3 has no literals: the decompiled text is proven by the frozen
        // WITH RECURSIVE shape plus the real table reference (a fallback would keep
        // the raw bindSql text verbatim)
        def planSql3 = fetchPlanSql(id3)
        assertTrue(planSql3.toUpperCase().contains("WITH RECURSIVE"),
                "table-driven recursive CTE plan_sql must keep the WITH RECURSIVE shape, got: " + planSql3)
        assertTrue(planSql3.contains("spm_rec_edge"),
                "table-driven recursive CTE plan_sql must reference spm_rec_edge, got: " + planSql3)
        assertTrue(planSql3 != bindSql3,
                "table-driven recursive CTE plan_sql must be the decompiled text, not the raw bindSql")

        sql 'set enable_spm_rewrite=true'
        def edgeWithSpm = sql """WITH RECURSIVE t1(k1, k2) AS (
            SELECT node1id AS k1, node2id AS k2 FROM spm_rec_edge
            UNION
            SELECT k1, cast(sum(k2) as int)
            FROM t1 GROUP BY k1
        ) SELECT * FROM t1 ORDER BY 1, 2"""
        sql 'set enable_spm_rewrite=false'
        def edgeWithoutSpm = sql """WITH RECURSIVE t1(k1, k2) AS (
            SELECT node1id AS k1, node2id AS k2 FROM spm_rec_edge
            UNION
            SELECT k1, cast(sum(k2) as int)
            FROM t1 GROUP BY k1
        ) SELECT * FROM t1 ORDER BY 1, 2"""
        assertEquals(edgeWithSpm, edgeWithoutSpm,
                "SPM rewrite must preserve the result of a table-driven recursive CTE")

        sql 'set enable_spm_rewrite=true'
        def explainEdge = sql """EXPLAIN WITH RECURSIVE t1(k1, k2) AS (
            SELECT node1id AS k1, node2id AS k2 FROM spm_rec_edge
            UNION
            SELECT k1, cast(sum(k2) as int)
            FROM t1 GROUP BY k1
        ) SELECT * FROM t1 ORDER BY 1, 2"""
        sql 'set enable_spm_rewrite=false'
        assertTrue(explainEdge.toString().contains("SPM baseline hit: id=" + id3),
                "EXPLAIN of the table-driven recursive CTE should report SPM baseline hit id " + id3 + ", got: " + explainEdge)

        // stored baseline columns in the OUT file (bind_sql / bind_sql_digest / plan_sql)
        order_qt_spm_bind_sql """SELECT bind_sql FROM __internal_schema.spm_baselines WHERE id = ${id3}"""
        order_qt_spm_bind_digest """SELECT bind_sql_digest FROM __internal_schema.spm_baselines WHERE id = ${id3}"""
        order_qt_spm_plan_sql """SELECT plan_sql FROM __internal_schema.spm_baselines WHERE id = ${id3}"""
    } finally {
        sql """DROP BASELINE PLAN IF EXISTS ${id3}"""
    }

    // ==================== case 4: recursive CTE + outer literal filter ====================
    // The outer predicate literal must be parameterized together with the anchor and
    // the recursion guard (whole-tree parameterization), and the frozen text must keep
    // the outer filter so the replay uses the user's own bound.
    def bindSql4 = """WITH RECURSIVE t1(k1) AS (
        SELECT cast(1 as bigint) k1
        UNION ALL
        SELECT k1 + 1 FROM t1 WHERE k1 < 5
    ) SELECT * FROM t1 WHERE k1 > 2 ORDER BY 1"""
    def createRes4 = sql ("CREATE GLOBAL BASELINE PLAN \"" + bindSql4.replace('"', '\\"') + "\" WITH \"" + bindSql4.replace('"', '\\"') + "\"")
    long id4 = Long.parseLong(createRes4[0][0].toString())

    try {
        def planSql4 = fetchPlanSql(id4)
        assertTrue(planSql4.contains("_spm_const_var"),
                "recursive CTE with an outer literal filter must be decompiled with placeholders, got: " + planSql4)
        assertTrue(planSql4.toUpperCase().contains("WITH RECURSIVE"),
                "recursive CTE with an outer filter plan_sql must keep the WITH RECURSIVE shape, got: " + planSql4)

        // similar query: different anchor-adjacent / guard / outer literals (same shape)
        sql 'set enable_spm_rewrite=true'
        def outerWithSpm = sql """WITH RECURSIVE t1(k1) AS (
            SELECT cast(1 as bigint) k1
            UNION ALL
            SELECT k1 + 1 FROM t1 WHERE k1 < 8
        ) SELECT * FROM t1 WHERE k1 > 3 ORDER BY 1"""
        sql 'set enable_spm_rewrite=false'
        def outerWithoutSpm = sql """WITH RECURSIVE t1(k1) AS (
            SELECT cast(1 as bigint) k1
            UNION ALL
            SELECT k1 + 1 FROM t1 WHERE k1 < 8
        ) SELECT * FROM t1 WHERE k1 > 3 ORDER BY 1"""
        assertEquals(outerWithSpm, outerWithoutSpm,
                "SPM rewrite must preserve the result of a recursive CTE with a different outer filter literal")

        sql 'set enable_spm_rewrite=true'
        def explainOuter = sql """EXPLAIN WITH RECURSIVE t1(k1) AS (
            SELECT cast(1 as bigint) k1
            UNION ALL
            SELECT k1 + 1 FROM t1 WHERE k1 < 8
        ) SELECT * FROM t1 WHERE k1 > 3 ORDER BY 1"""
        sql 'set enable_spm_rewrite=false'
        assertTrue(explainOuter.toString().contains("SPM baseline hit: id=" + id4),
                "EXPLAIN of the recursive CTE with an outer filter variant should report SPM baseline hit id " + id4 + ", got: " + explainOuter)

        // stored baseline columns in the OUT file (bind_sql / bind_sql_digest / plan_sql)
        order_qt_spm_bind_sql """SELECT bind_sql FROM __internal_schema.spm_baselines WHERE id = ${id4}"""
        order_qt_spm_bind_digest """SELECT bind_sql_digest FROM __internal_schema.spm_baselines WHERE id = ${id4}"""
        order_qt_spm_plan_sql """SELECT plan_sql FROM __internal_schema.spm_baselines WHERE id = ${id4}"""
    } finally {
        sql """DROP BASELINE PLAN IF EXISTS ${id4}"""
    }
}
