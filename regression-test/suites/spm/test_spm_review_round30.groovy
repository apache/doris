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

suite("test_spm_review_round30", "spm") {

    // Thirtieth .
    //
    // SQL-visible fix covered here:
    //  - #8: a clock function is a replay-time context expression like @v /
    //    current_user(). FE constant folding evaluates now() / current_timestamp() from
    //    the CREATE statement's start time, so a baseline for
    //    'SELECT now() AS ts FROM t' served the CREATE timestamp to every later matching
    //    query - the frozen planSql persisted the creator's value. CREATE GLOBAL BASELINE
    //    PLAN now preserves those function calls, while a deterministic column function
    //    (unix_timestamp(k)) stays allowed and still hits its own baseline.
    //
    // #1 (audit window predicate for a pre-window statement completed inside the window),
    // #2 (no compensating delete after an unconfirmed status move), #3 (fail-closed
    // visibility probes), #4 (a timestamp-only refresh keeps the object identity), #5
    // (include / exclude patterns pinned in the capture checkpoint), #6 (a replayed
    // failure keeps its eligibility), #7 (a pending CREATE adopts its own row instead of
    // allocating a second id) and #9 (the retry queue budget pauses the page drain) have
    // no SQL surface a single-node suite can drive deterministically - they need real
    // audit rows, daemon cycles or a metadata outage mid-create. They are covered by

    // SPM regression pins the fallback switch CLOSED: a rewritten-plan failure must
    // surface as an error, never silently re-run the original query.
    sql """set enable_spm_fallback = false"""
    sql """set enable_spm_rewrite = true"""
    sql """set enable_sql_cache = false"""
    // The unix_timestamp(k2) assertion below converts DATETIME values using the
    // SESSION time_zone, and this suite's .out file pins the +08:00 epochs. Without
    // this the FE default comes from the runner's HOST zone: on a UTC runner the same
    // query yields values 28,800 seconds later and the suite fails although SPM behaved
    // correctly. A fixed offset (not 'Asia/Shanghai') keeps the expectation independent
    // of the tzdata installed on the runner as well.
    sql """set time_zone = '+08:00'"""

    // ==================== setup: table (drop before use, keep after) ====================
    sql """DROP TABLE IF EXISTS spm_r30_t1"""
    sql """
        CREATE TABLE spm_r30_t1 (
            k1 INT,
            k2 DATETIME
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r30_t1 VALUES (1, '2026-01-02 03:04:05'), (2, '2026-01-03 04:05:06')"""

    // Global baselines are cluster-wide state and other SPM suites may run their own in
    // parallel: every SHOW here is scoped to this suite's table and only baselines
    // matching it are dropped or asserted on.
    def ownBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r30_t1") }
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
        // Direct statement clocks must survive capture as real function calls.
        for (String call : ["now()", "current_timestamp()", "localtime()", "unix_timestamp()"]) {
            String query = "SELECT ${call} AS ts FROM spm_r30_t1"
            long id = sql("CREATE GLOBAL BASELINE PLAN '${query}' WITH '${query}'")[0][0] as Long
            def stored = sql("SELECT plan_frozen, plan_sql FROM __internal_schema.spm_baselines WHERE id = ${id}")[0]
            assertTrue(stored[0].toString() in ["1", "true"], stored.toString())
            assertTrue(explainOf(query).contains("SPM baseline hit: id=${id}"))
            sql(query)
        }

        // the deterministic form stays allowed: CREATE succeeds and the baseline replays
        String clockOk = "SELECT unix_timestamp(k2) AS ts FROM spm_r30_t1"
        List<List<Object>> created = sql(
                """CREATE GLOBAL BASELINE PLAN '${clockOk}' WITH '${clockOk}'""")
        assertEquals(1, created.size(), "CREATE should return one row, got: ${created}")
        long clockId = Long.parseLong(created[0][0].toString())
        assertTrue(explainOf(clockOk).contains("SPM baseline hit: id=${clockId}"),
                "unix_timestamp with arguments must stay allowed: " + explainOf(clockOk))
        order_qt_clock_function_allowed clockOk

        // leave no baselines behind for other runs
        dropOwnBaselines()
        assertEquals(0, ownBaselines().size(), "all spm_r30_ baselines must be dropped")
    } finally {
        dropOwnBaselines()
    }
}
