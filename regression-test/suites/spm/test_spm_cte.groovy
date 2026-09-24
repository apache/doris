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

suite("test_spm_cte", "spm") {

    // SPM baseline DDL + match verification on NON-recursive CTE (WITH) queries.
    // The SPM decompiler renders PhysicalCTEAnchor / PhysicalCTEProducer /
    // PhysicalCTEConsumer as a real WITH clause (one shared definition, referenced
    // by alias, like StarRocks) instead of inlining the body at every consumer:
    //
    //  1. CREATE BASELINE PLAN from the query text; the frozen plan_sql must start
    //     with WITH and reference the body table exactly once (a multi-consumer CTE
    //     must not duplicate the body).
    //  2. Match verification: the query itself and a similar query (same structure,
    //     different literal values) run through the SPM rewrite path
    //     (enable_spm_rewrite=true); each must produce the same result as without SPM.
    //  3. Every case also emits the three stored baseline columns into the OUT file
    //     (spm_bind_sql / spm_bind_digest / spm_plan_sql), like the TPCDS / TPCH SPM
    //     suites, so the frozen bind and plan text is reviewable from the .out file.

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
    // the tests can assert that CREATE actually decompiled the CTE instead of keeping
    // the user-supplied planSql as the legacy fallback.
    def fetchPlanSql = { long baselineId ->
        def rows = sql "SHOW BASELINE PLANS WHERE id = ${baselineId}"
        return rows[0][4].toString()
    }

    // Counts the occurrences of a literal fragment inside a string.
    def countOccurrences = { String text, String fragment ->
        int count = 0
        int index = text.indexOf(fragment)
        while (index >= 0) {
            count++
            index = text.indexOf(fragment, index + fragment.length())
        }
        return count
    }

    // ==================== self-contained table for the CTE bodies ====================
    sql "DROP TABLE IF EXISTS spm_cte_t"
    sql """
        CREATE TABLE spm_cte_t (
            k1 int,
            v1 int
        ) DUPLICATE KEY (k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ('replication_num' = '1');
    """
    sql "INSERT INTO spm_cte_t VALUES (1, 10), (2, 20), (3, 30)"

    // ==================== case 1: single-consumer CTE ====================
    def bindSql1 = """WITH c1 AS (SELECT k1, v1 FROM spm_cte_t WHERE v1 > 5)
SELECT k1, sum(v1) AS s FROM c1 GROUP BY k1 ORDER BY k1"""
    def createRes1 = sql ("CREATE GLOBAL BASELINE PLAN \"" + bindSql1.replace('"', '\\"') + "\" WITH \"" + bindSql1.replace('"', '\\"') + "\"")
    long id1 = Long.parseLong(createRes1[0][0].toString())

    try {
        // the frozen planSql must keep the WITH structure (a fallback would keep the
        // raw bindSql text verbatim, and the old inlining decompiler would emit a
        // single SELECT without WITH)
        def planSql1 = fetchPlanSql(id1)
        assertTrue(planSql1.trim().toUpperCase().startsWith("WITH"),
                "single-consumer CTE plan_sql must start with WITH, got: " + planSql1)
        assertTrue(countOccurrences(planSql1, "spm_cte_t") == 1,
                "the CTE body must be defined exactly once, got: " + planSql1)
        assertTrue(planSql1 != bindSql1,
                "CTE plan_sql must be the decompiled text, not the raw bindSql")

        // original query: SPM rewrite must keep the result
        sql 'set enable_spm_rewrite=true'
        def origWithSpm = sql """WITH c1 AS (SELECT k1, v1 FROM spm_cte_t WHERE v1 > 5)
SELECT k1, sum(v1) AS s FROM c1 GROUP BY k1 ORDER BY k1"""
        sql 'set enable_spm_rewrite=false'
        def origWithoutSpm = sql """WITH c1 AS (SELECT k1, v1 FROM spm_cte_t WHERE v1 > 5)
SELECT k1, sum(v1) AS s FROM c1 GROUP BY k1 ORDER BY k1"""
        assertEquals(origWithSpm, origWithoutSpm,
                "SPM rewrite must preserve the result of the original CTE query")

        // similar query (different body literal) must hit the baseline and rewrite
        sql 'set enable_spm_rewrite=true'
        def similarWithSpm = sql """WITH c1 AS (SELECT k1, v1 FROM spm_cte_t WHERE v1 > 6)
SELECT k1, sum(v1) AS s FROM c1 GROUP BY k1 ORDER BY k1"""
        sql 'set enable_spm_rewrite=false'
        def similarWithoutSpm = sql """WITH c1 AS (SELECT k1, v1 FROM spm_cte_t WHERE v1 > 6)
SELECT k1, sum(v1) AS s FROM c1 GROUP BY k1 ORDER BY k1"""
        assertEquals(similarWithSpm, similarWithoutSpm,
                "SPM rewrite must preserve the result of a similar CTE query")

        sql 'set enable_spm_rewrite=true'
        def explainOrig = sql """EXPLAIN WITH c1 AS (SELECT k1, v1 FROM spm_cte_t WHERE v1 > 5)
SELECT k1, sum(v1) AS s FROM c1 GROUP BY k1 ORDER BY k1"""
        def explainSimilar = sql """EXPLAIN WITH c1 AS (SELECT k1, v1 FROM spm_cte_t WHERE v1 > 6)
SELECT k1, sum(v1) AS s FROM c1 GROUP BY k1 ORDER BY k1"""
        sql 'set enable_spm_rewrite=false'
        assertTrue(explainOrig.toString().contains("SPM baseline hit: id=" + id1),
                "EXPLAIN of the original CTE query should report SPM baseline hit id " + id1 + ", got: " + explainOrig)
        assertTrue(explainSimilar.toString().contains("SPM baseline hit: id=" + id1),
                "EXPLAIN of the similar CTE query should report SPM baseline hit id " + id1 + ", got: " + explainSimilar)

        // stored baseline columns in the OUT file (bind_sql / bind_sql_digest / plan_sql)
        order_qt_spm_bind_sql """SELECT bind_sql FROM __internal_schema.spm_baselines WHERE id = ${id1}"""
        order_qt_spm_bind_digest """SELECT bind_sql_digest FROM __internal_schema.spm_baselines WHERE id = ${id1}"""
        order_qt_spm_plan_sql """SELECT plan_sql FROM __internal_schema.spm_baselines WHERE id = ${id1}"""
    } finally {
        sql """DROP BASELINE PLAN IF EXISTS ${id1}"""
    }

    // ==================== case 2: multi-consumer CTE (self join) ====================
    // The body has two consumers: the frozen planSql must share ONE definition
    // (the old inlining decompiler emitted the body twice) and give each consumer
    // reference its own relation alias, so replay stays unambiguous.
    def bindSql2 = """WITH c2 AS (SELECT k1, v1 FROM spm_cte_t WHERE v1 > 5)
SELECT a.k1 FROM c2 a JOIN c2 b ON a.k1 = b.k1 ORDER BY 1"""
    def createRes2 = sql ("CREATE GLOBAL BASELINE PLAN \"" + bindSql2.replace('"', '\\"') + "\" WITH \"" + bindSql2.replace('"', '\\"') + "\"")
    long id2 = Long.parseLong(createRes2[0][0].toString())

    try {
        def planSql2 = fetchPlanSql(id2)
        assertTrue(planSql2.trim().toUpperCase().startsWith("WITH"),
                "multi-consumer CTE plan_sql must start with WITH, got: " + planSql2)
        assertTrue(countOccurrences(planSql2, "spm_cte_t") == 1,
                "a multi-consumer CTE must share one definition, got: " + planSql2)

        sql 'set enable_spm_rewrite=true'
        def joinWithSpm = sql """WITH c2 AS (SELECT k1, v1 FROM spm_cte_t WHERE v1 > 6)
SELECT a.k1 FROM c2 a JOIN c2 b ON a.k1 = b.k1 ORDER BY 1"""
        sql 'set enable_spm_rewrite=false'
        def joinWithoutSpm = sql """WITH c2 AS (SELECT k1, v1 FROM spm_cte_t WHERE v1 > 6)
SELECT a.k1 FROM c2 a JOIN c2 b ON a.k1 = b.k1 ORDER BY 1"""
        assertEquals(joinWithSpm, joinWithoutSpm,
                "SPM rewrite must preserve the result of a self-joined CTE query")

        sql 'set enable_spm_rewrite=true'
        def explainJoin = sql """EXPLAIN WITH c2 AS (SELECT k1, v1 FROM spm_cte_t WHERE v1 > 6)
SELECT a.k1 FROM c2 a JOIN c2 b ON a.k1 = b.k1 ORDER BY 1"""
        sql 'set enable_spm_rewrite=false'
        assertTrue(explainJoin.toString().contains("SPM baseline hit: id=" + id2),
                "EXPLAIN of the self-joined CTE should report SPM baseline hit id " + id2 + ", got: " + explainJoin)

        order_qt_spm_bind_sql """SELECT bind_sql FROM __internal_schema.spm_baselines WHERE id = ${id2}"""
        order_qt_spm_bind_digest """SELECT bind_sql_digest FROM __internal_schema.spm_baselines WHERE id = ${id2}"""
        order_qt_spm_plan_sql """SELECT plan_sql FROM __internal_schema.spm_baselines WHERE id = ${id2}"""
    } finally {
        sql """DROP BASELINE PLAN IF EXISTS ${id2}"""
    }

    // ==================== case 3: CTE referencing another CTE ====================
    // The WITH definitions must keep a valid dependency order (the referenced CTE is
    // defined before the CTE that uses it), otherwise the frozen text cannot be
    // re-parsed at replay time.
    def bindSql3 = """WITH c3 AS (SELECT k1, v1 FROM spm_cte_t WHERE v1 > 5),
c4 AS (SELECT k1 FROM c3 WHERE k1 > 1)
SELECT k1 FROM c4 ORDER BY 1"""
    def createRes3 = sql ("CREATE GLOBAL BASELINE PLAN \"" + bindSql3.replace('"', '\\"') + "\" WITH \"" + bindSql3.replace('"', '\\"') + "\"")
    long id3 = Long.parseLong(createRes3[0][0].toString())

    try {
        def planSql3 = fetchPlanSql(id3)
        assertTrue(planSql3.trim().toUpperCase().startsWith("WITH"),
                "nested CTE plan_sql must start with WITH, got: " + planSql3)
        assertTrue(countOccurrences(planSql3, "spm_cte_t") == 1,
                "the base CTE body must be defined exactly once, got: " + planSql3)

        sql 'set enable_spm_rewrite=true'
        def nestedWithSpm = sql """WITH c3 AS (SELECT k1, v1 FROM spm_cte_t WHERE v1 > 6),
c4 AS (SELECT k1 FROM c3 WHERE k1 > 1)
SELECT k1 FROM c4 ORDER BY 1"""
        sql 'set enable_spm_rewrite=false'
        def nestedWithoutSpm = sql """WITH c3 AS (SELECT k1, v1 FROM spm_cte_t WHERE v1 > 6),
c4 AS (SELECT k1 FROM c3 WHERE k1 > 1)
SELECT k1 FROM c4 ORDER BY 1"""
        assertEquals(nestedWithSpm, nestedWithoutSpm,
                "SPM rewrite must preserve the result of a CTE referencing another CTE")

        sql 'set enable_spm_rewrite=true'
        def explainNested = sql """EXPLAIN WITH c3 AS (SELECT k1, v1 FROM spm_cte_t WHERE v1 > 6),
c4 AS (SELECT k1 FROM c3 WHERE k1 > 1)
SELECT k1 FROM c4 ORDER BY 1"""
        sql 'set enable_spm_rewrite=false'
        assertTrue(explainNested.toString().contains("SPM baseline hit: id=" + id3),
                "EXPLAIN of the nested CTE should report SPM baseline hit id " + id3 + ", got: " + explainNested)

        order_qt_spm_bind_sql """SELECT bind_sql FROM __internal_schema.spm_baselines WHERE id = ${id3}"""
        order_qt_spm_bind_digest """SELECT bind_sql_digest FROM __internal_schema.spm_baselines WHERE id = ${id3}"""
        order_qt_spm_plan_sql """SELECT plan_sql FROM __internal_schema.spm_baselines WHERE id = ${id3}"""
    } finally {
        sql """DROP BASELINE PLAN IF EXISTS ${id3}"""
    }
}
