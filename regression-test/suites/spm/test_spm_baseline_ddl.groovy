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

suite("test_spm_baseline_ddl", "spm") {

    // SPM Phase 1 DDL regression: CREATE / SHOW / ALTER / DROP BASELINE PLAN.
    //
    // SHOW BASELINE PLANS carries auto-increment ids and create/update timestamps, so
    // non-empty SHOW output is verified with the dynamic columns (id, create_time,
    // update_time) stripped. CREATE BASELINE PLAN returns the created baseline id as a
    // single-row result set; the id is auto-increment and therefore dynamic, so creates
    // are asserted in code against the SHOW output, while result-less DDL and the
    // empty-state SHOW are asserted through qt / order_qt against the generated .out file.

    // SPM regression pins the fallback switch CLOSED: a rewritten-plan failure must
    // surface as an error, never silently re-run the original query.
    sql """set enable_spm_fallback = false"""

    // ==================== setup: tables (drop before use, keep after) ====================
    sql """DROP TABLE IF EXISTS spm_t1"""
    sql """
        CREATE TABLE spm_t1 (
            k1 INT,
            k2 INT,
            k3 STRING
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_t1 VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c')"""

    // ==================== cleanup: drop this suite's leftover baselines (ids are dynamic) ====================
    // Global baselines are cluster-wide state and other SPM suites may run their own
    // baselines in parallel: every SHOW here is scoped to this suite's table (spm_t1),
    // and only baselines matching it are ever dropped or counted.
    def ownBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_t1") }
    }
    def dropOwnBaselines = {
        ownBaselines().each { row ->
            sql """DROP BASELINE PLAN ${row[0]}"""
        }
    }
    dropOwnBaselines()

    // empty state is deterministic (scoped to this suite's bind: foreign baselines are ignored)
    order_qt_show_empty """SHOW BASELINE PLANS WHERE bind_sql = 'select * from spm_t1 where k1 = 1'"""
    assertEquals(0, ownBaselines().size(), "no spm_t1 baseline should be left after cleanup")

    // ==================== CREATE ====================
    // CREATE returns the created baseline id as a single-row result set; the id is
    // auto-increment and therefore dynamic - capture it and cross-check with SHOW
    // instead of comparing it through a qt .out entry.
    List<List<Object>> createSimpleRes = sql """CREATE GLOBAL BASELINE PLAN
        'select * from spm_t1 where k1 = 1'
        WITH 'select * from spm_t1 where k1 = 1'"""
    assertEquals(1, createSimpleRes.size(), "CREATE should return one row, got: ${createSimpleRes}")
    long simpleId = Long.parseLong(createSimpleRes[0][0].toString())

    List<List<Object>> createMultiRes = sql """CREATE SESSION BASELINE PLAN
        'select * from spm_t1 where k1 = 1 and k2 = 2'
        WITH 'select * from spm_t1 where k1 = 1 and k2 = 2'"""
    assertEquals(1, createMultiRes.size(), "CREATE should return one row, got: ${createMultiRes}")
    long multiId = Long.parseLong(createMultiRes[0][0].toString())

    // Scheme 1 (scope-partitioned id ranges): GLOBAL ids stay below 2^62, SESSION ids
    // start at 2^62, so the scope of an id is exact (BaselineScope.ofId) and a session id
    // can never collide with a global one
    assertTrue(simpleId < (1L << 62),
            "a GLOBAL baseline id must stay below the session id range, got: ${simpleId}")
    assertTrue(multiId >= (1L << 62),
            "a SESSION baseline id must start at 2^62, got: ${multiId}")

    // ==================== SHOW: 2 baselines, content verified without dynamic columns ====================
    List<List<Object>> rowsAfterCreate = ownBaselines()
    assertEquals(2, rowsAfterCreate.size(),
            "two baselines should be shown, got: ${rowsAfterCreate}")
    assertTrue(rowsAfterCreate.any { (it[0] as Long) == simpleId },
            "id returned by CREATE must be visible in SHOW, got: ${rowsAfterCreate}")
    assertTrue(rowsAfterCreate.any { (it[0] as Long) == multiId },
            "id returned by CREATE must be visible in SHOW, got: ${rowsAfterCreate}")

    // columns: 0 id, 1 bind_sql, 2 bind_sql_digest, 3 bind_sql_hash, 4 plan_sql,
    //          5 query_id (audit_log correlation), 6 cost, 7 query_time_ms, 8 source,
    //          9 status, 10 create_time, 11 update_time, 12 scope (synthesized: GLOBAL / SESSION)
    // bind_sql keeps the original bind text; bind_sql_digest is the value-free digest
    // (every literal rendered as ?); plan_sql is the SPM-optimized frozen text carrying
    // the _spm_const_var(id) placeholders.
    List<List<Object>> simple = rowsAfterCreate.findAll { it[1].contains("k1 = 1") && !it[1].contains("k2") }
    assertEquals(1, simple.size(), "simple baseline should be visible, got: ${rowsAfterCreate}")
    assertEquals("USER", simple[0][8])
    assertEquals("ENABLED", simple[0][9])
    assertEquals("GLOBAL", simple[0][12], "a GLOBAL baseline must report scope=GLOBAL")
    assertEquals("-1", simple[0][7].toString(), "user-created baseline query_time_ms should be -1")
    // the CREATE statement's audit query id is stored on the baseline (query_id column),
    // so the audit_log row of the CREATE - carrying the bindSql literal in its stmt text -
    // can be located by query id; SHOW must surface the persisted value
    assertTrue(simple[0][5].toString().length() > 0,
            "a created baseline must carry the audit query id, got: ${simple[0][5]}")
    assertEquals(simple[0][5].toString(),
            sql("""SELECT query_id FROM __internal_schema.spm_baselines WHERE id = ${simpleId}""")[0][0].toString(),
            "SHOW query_id must match the persisted query_id")
    assertTrue(simple[0][1].toString().contains("k1 = 1"),
            "bind_sql should keep the original bind text: ${simple[0][1]}")
    assertTrue(simple[0][2].toString().contains("k1 = ?"),
            "digest should be value-independent: ${simple[0][2]}")
    assertTrue(simple[0][4].toString().contains("_spm_const_var"),
            "plan_sql should carry the frozen placeholder: ${simple[0][4]}")

    List<List<Object>> multi = rowsAfterCreate.findAll { it[1].contains("k2") }
    assertEquals(1, multi.size(), "multi-condition baseline should be visible, got: ${rowsAfterCreate}")
    assertEquals("USER", multi[0][8])
    assertEquals("ENABLED", multi[0][9])
    assertEquals("SESSION", multi[0][12], "a SESSION baseline must report scope=SESSION")
    assertTrue(multi[0][4].toString().contains("spm_t1"),
            "plan_sql should reference spm_t1: ${multi[0][4]}")

    // the scope column doubles as a SHOW filter: one SESSION + one GLOBAL baseline here
    // (the SESSION filter only ever sees this connection's own session baselines; the
    // GLOBAL filter potentially also sees other suites' rows, so scope it afterwards)
    assertEquals(1, sql("""SHOW BASELINE PLANS WHERE scope = 'SESSION'""").size(),
            "WHERE scope = 'SESSION' must isolate the session baseline")
    assertEquals(1, sql("""SHOW BASELINE PLANS WHERE scope = 'GLOBAL'""")
                    .findAll { it[1].toString().contains("spm_t1") }.size(),
            "WHERE scope = 'GLOBAL' must isolate this suite's global baseline")

    // ==================== duplicate create: same bind+plan returns existing id (no new row) ====================
    List<List<Object>> createDupRes = sql """CREATE GLOBAL BASELINE PLAN
        'select * from spm_t1 where k1 = 1'
        WITH 'select * from spm_t1 where k1 = 1'"""
    assertEquals(simpleId, Long.parseLong(createDupRes[0][0].toString()),
            "duplicate create must return the existing id, got: ${createDupRes}")

    assertEquals(2, ownBaselines().size(),
            "duplicate create must not add a baseline")

    // same bind, different plan -> allowed to coexist
    List<List<Object>> createAltPlanRes = sql """CREATE GLOBAL BASELINE PLAN
        'select * from spm_t1 where k1 = 1'
        WITH 'select * from spm_t1 where k1 = 1 and k2 = 99'"""
    long altPlanId = Long.parseLong(createAltPlanRes[0][0].toString())
    assertTrue(altPlanId != simpleId,
            "a different planSql must create a new baseline id, got: ${altPlanId}")

    assertEquals(3, ownBaselines().size(),
            "different planSql for the same bind is allowed")

    // ==================== ALTER: disable / enable ====================
    def targetId = (ownBaselines()[0][0] as Long)
    sql """ALTER BASELINE PLAN ${targetId} DISABLE"""

    List<List<Object>> rowsAfterDisable = ownBaselines()
    assertEquals(1, rowsAfterDisable.count { it[9] == "DISABLED" },
            "one baseline should be DISABLED, got: ${rowsAfterDisable}")

    qt_alter_enable """ALTER BASELINE PLAN ${targetId} ENABLE"""

    List<List<Object>> rowsAfterEnable = ownBaselines()
    assertEquals(0, rowsAfterEnable.count { it[9] == "DISABLED" },
            "no baseline should stay DISABLED after re-enable, got: ${rowsAfterEnable}")

    // ==================== SESSION scope: connection-local, never persisted ====================
    // the SESSION-scope baseline lives only in this connection's SessionBaselineStore:
    // it is usable (SHOW / EXPLAIN / ALTER / DROP) in this session but is never written
    // to the shared internal table, while the GLOBAL one is persisted
    assertEquals(0L,
            sql("""SELECT COUNT(*) FROM __internal_schema.spm_baselines WHERE id = ${multiId}""")[0][0] as Long,
            "SESSION baseline must not be persisted to the internal table")
    assertEquals(1L,
            sql("""SELECT COUNT(*) FROM __internal_schema.spm_baselines WHERE id = ${simpleId}""")[0][0] as Long,
            "GLOBAL baseline must be persisted to the internal table")

    // the SESSION baseline takes precedence over the structurally identical GLOBAL
    // baseline (altPlanId, k2 = 99) for this connection
    sql """set enable_spm_rewrite = true"""
    String sessionHit = sql("""EXPLAIN SELECT * FROM spm_t1 WHERE k1 = 1 AND k2 = 2""").toString()
    assertTrue(sessionHit.contains("SPM baseline hit: id=${multiId}, scope=SESSION"),
            "the SESSION baseline must win the rewrite for this session, got: ${sessionHit}")

    // ALTER / DROP address the SESSION baseline directly (shared id space)
    sql """ALTER BASELINE PLAN ${multiId} DISABLE"""
    assertEquals(1, sql("""SHOW BASELINE PLANS WHERE status = 'DISABLED'""")
                    .findAll { it[1].toString().contains("spm_t1") }.size(),
            "the SESSION baseline must be disableable through ALTER")
    sql """ALTER BASELINE PLAN ${multiId} ENABLE"""
    sql """DROP BASELINE PLAN ${multiId}"""
    List<List<Object>> rowsAfterSessionDrop = ownBaselines()
    assertTrue(rowsAfterSessionDrop.every { (it[0] as Long) != multiId },
            "the dropped SESSION baseline must disappear from SHOW, got: ${rowsAfterSessionDrop}")

    // with the SESSION baseline gone the two-predicate shape no longer matches anything
    // in this session, while the GLOBAL baselines keep rewriting their own shape
    String afterDropHit = sql("""EXPLAIN SELECT * FROM spm_t1 WHERE k1 = 1 AND k2 = 2""").toString()
    assertFalse(afterDropHit.contains("SPM baseline hit"),
            "the dropped SESSION baseline must no longer rewrite this session, got: ${afterDropHit}")
    String globalHit = sql("""EXPLAIN SELECT * FROM spm_t1 WHERE k1 = 3""").toString()
    assertTrue(globalHit.contains("SPM baseline hit: id=${simpleId}")
                    || globalHit.contains("SPM baseline hit: id=${altPlanId}"),
            "a GLOBAL baseline must keep rewriting after the session baseline is dropped, got: ${globalHit}")
    sql """set enable_spm_rewrite = false"""

    // ==================== DROP ====================
    dropOwnBaselines()

    // back to the deterministic empty state (scoped to this suite's bind)
    order_qt_show_after_drop """SHOW BASELINE PLANS WHERE bind_sql = 'select * from spm_t1 where k1 = 1'"""

    // drop a missing baseline without IF EXISTS -> error
    test {
        sql """DROP BASELINE PLAN 999999"""
        exception "does not exist"
    }

    // drop a missing baseline with IF EXISTS -> ok
    qt_drop_if_exists """DROP BASELINE PLAN IF EXISTS 999999"""
}
