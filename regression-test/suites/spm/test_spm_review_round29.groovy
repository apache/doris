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

suite("test_spm_review_round29", "spm") {

    // Twenty-ninth review round.
    //
    // SQL-visible fixes covered here:
    //  - #1: a TABLESAMPLE clause is rendered from its FIELDS, never from Object#toString.
    //    TableSample overrides equals / hashCode by value but not toString, so its default
    //    text is the object's IDENTITY hash: with that text the bind / plan mismatch guard
    //    saw two DIFFERENT selectors for the SAME clause whenever the two texts were
    //    parsed separately (bind text != plan text), and CREATE failed with "their scan
    //    selectors of table ... differ". The sample is also part of the match key, so a
    //    different sample must still NOT hit the baseline.
    //  - #5: the LIKE chain of SHOW BASELINE PLANS now covers every column the WHERE form
    //    filters on (bind SQL, plan SQL, source, status, SCOPE), so
    //    `SHOW BASELINE PLANS LIKE 'SESSION'` finds a session baseline whose SQL text does
    //    not spell "SESSION" (and `LIKE 'GLOBAL'` no longer depends on a baseline's SQL
    //    containing the word).
    //
    // #2 (the audit dedup gate for FOR VERSION AS OF / FOR TIME AS OF / @scan-params), #3
    // (the threshold snapshot of a pending capture window), #4 (bounded page drain +
    // prompt resume) and #6 (clearing the abandoned pass's hints before the fallback)
    // have no SQL surface a single-node suite can drive deterministically - the audit
    // capture needs real audit rows and daemon cycles, and #6 needs a plan-side hint of a
    // frozen baseline that the fallback must drop. They are covered by
    // AuditDedupIdentityTest, PlanCaptureCycleHandoffTest and SPMRound29SafetyTest.

    // SPM regression pins the fallback switch CLOSED: a rewritten-plan failure must
    // surface as an error, never silently re-run the original query.
    sql """set enable_spm_fallback = false"""
    sql """set enable_spm_rewrite = true"""

    // ==================== setup: table (drop before use, keep after) ====================
    sql """DROP TABLE IF EXISTS spm_r29_t1"""
    sql """
        CREATE TABLE spm_r29_t1 (
            k1 INT,
            k2 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r29_t1 VALUES (1, 10), (2, 20), (3, 30)"""

    // Global baselines are cluster-wide state and other SPM suites may run their own in
    // parallel: every SHOW here is scoped to this suite's table and only baselines
    // matching it are dropped or asserted on.
    def ownBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r29_t1") }
    }
    def dropOwnBaselines = {
        ownBaselines().each { row ->
            sql """DROP BASELINE PLAN ${row[0]}"""
        }
    }
    def explainOf = { String stmt ->
        sql("""EXPLAIN ${stmt}""").toString()
    }

    dropOwnBaselines()
    try {
        // ==================== #1: TABLESAMPLE identity ====================
        // The bind and plan texts differ only in case, which forces TWO parses - the case
        // the identity-hash rendering of the sample used to break.
        String sampleBind = "SELECT k1 FROM spm_r29_t1 TABLESAMPLE(100 PERCENT) WHERE k1 >= 1"
        String samplePlan = "select k1 from spm_r29_t1 TABLESAMPLE(100 PERCENT) where k1 >= 1"
        List<List<Object>> sampled = sql(
                """CREATE GLOBAL BASELINE PLAN '${sampleBind}' WITH '${samplePlan}'""")
        assertEquals(1, sampled.size(), "CREATE should return one row, got: ${sampled}")
        long sampleId = Long.parseLong(sampled[0][0].toString())
        assertTrue(explainOf(sampleBind).contains("SPM baseline hit: id=${sampleId}"),
                "the sampled query must hit its baseline: " + explainOf(sampleBind))
        order_qt_tablesample_replay """SELECT k1 FROM spm_r29_t1 TABLESAMPLE(100 PERCENT) WHERE k1 >= 1 ORDER BY k1"""

        // the sample stays part of the match key: a different seed is a DIFFERENT sample
        // and must not be served by this baseline
        String otherSample =
                "SELECT k1 FROM spm_r29_t1 TABLESAMPLE(100 PERCENT) REPEATABLE 7 WHERE k1 >= 1"
        assertFalse(explainOf(otherSample).contains("SPM baseline hit: id=${sampleId}"),
                "a different sample must not hit the sample's baseline: " + explainOf(otherSample))

        // ==================== #5: LIKE covers the metadata columns ====================
        String sessionSql = "SELECT k2 FROM spm_r29_t1 WHERE k2 = 20"
        List<List<Object>> sessionCreate = sql(
                """CREATE SESSION BASELINE PLAN '${sessionSql}' WITH '${sessionSql}'""")
        assertEquals(1, sessionCreate.size(), "CREATE should return one row, got: ${sessionCreate}")
        long sessionId = Long.parseLong(sessionCreate[0][0].toString())

        List<List<Object>> bySession = sql("""SHOW BASELINE PLANS LIKE 'SESSION'""")
        assertTrue(bySession.any { Long.parseLong(it[0].toString()) == sessionId },
                "LIKE 'SESSION' must find a session baseline whose SQL does not spell"
                        + " SESSION: ${bySession}")
        assertFalse(bySession.any { Long.parseLong(it[0].toString()) == sampleId },
                "a GLOBAL baseline must not match LIKE 'SESSION': ${bySession}")

        List<List<Object>> byGlobal = sql("""SHOW BASELINE PLANS LIKE 'GLOBAL'""")
        assertTrue(byGlobal.any { Long.parseLong(it[0].toString()) == sampleId },
                "LIKE 'GLOBAL' must find the global baseline: ${byGlobal}")
        assertFalse(byGlobal.any { Long.parseLong(it[0].toString()) == sessionId },
                "a SESSION baseline must not match LIKE 'GLOBAL': ${byGlobal}")

        // the SQL text and the other metadata columns stay searchable
        List<List<Object>> bySql = sql("""SHOW BASELINE PLANS LIKE '%spm_r29_t1%'""")
        assertTrue(bySql.any { Long.parseLong(it[0].toString()) == sampleId }
                        && bySql.any { Long.parseLong(it[0].toString()) == sessionId },
                "LIKE '%...%' must keep matching the stored SQL text: ${bySql}")
        List<List<Object>> bySource = sql("""SHOW BASELINE PLANS LIKE 'user'""")
        assertTrue(bySource.any { Long.parseLong(it[0].toString()) == sessionId },
                "LIKE 'USER' must match the source column: ${bySource}")
    } finally {
        dropOwnBaselines()
    }
}
