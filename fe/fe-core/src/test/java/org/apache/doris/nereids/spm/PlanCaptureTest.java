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

package org.apache.doris.nereids.spm;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.nereids.spm.capture.CapturedQuery;
import org.apache.doris.nereids.spm.capture.PlanCaptureFilter;
import org.apache.doris.nereids.spm.capture.PlanCaptureManager;
import org.apache.doris.nereids.spm.manager.BaselineManager;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.statistics.repository.ResultRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Phase 2 tests: SPM auto capture.
 *
 * Verifies:
 *
 * 1. PlanCaptureFilter table extraction (parse a SQL, get its distinct table names)
 * 2. The pure filter chain: multi-table requirement, performance thresholds, include /
 *    exclude table-name regex
 * 3. BaselineManager duplicate-detection helpers used by capture (design doc 7.2.5)
 * 4. PlanCaptureManager failure handling: a non-plannable candidate increments the
 *    failure counter and never propagates (transparent capture)
 */
public class PlanCaptureTest {

    private PlanCaptureFilter filter;
    private BaselineManager manager;

    @BeforeEach
    public void setUp() {
        filter = new PlanCaptureFilter("", "");
        manager = BaselineManager.getInstance();
        manager.clearForTest();
        PlanCaptureManager.getInstance().resetForTest();
    }

    // ==================== table extraction ====================

    @Test
    public void testExtractTableNamesMultiJoin() {
        List<String> tables = PlanCaptureFilter.extractTableNames(
                "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.a = 1");
        Assertions.assertTrue(tables.size() >= 2, "multi-table join must yield >= 2 tables: " + tables);
        Assertions.assertTrue(tables.stream().anyMatch(t -> t.equals("t1") || t.endsWith(".t1")),
                "t1 must be extracted: " + tables);
        Assertions.assertTrue(tables.stream().anyMatch(t -> t.equals("t2") || t.endsWith(".t2")),
                "t2 must be extracted: " + tables);
    }

    @Test
    public void testExtractTableNamesSingleTable() {
        List<String> tables = PlanCaptureFilter.extractTableNames(
                "SELECT * FROM t1 WHERE a = 1");
        Assertions.assertEquals(1, tables.size(), "single-table query must yield one table");
    }

    /**
     * #5: SELECT * REPLACE((SELECT MAX(v) FROM t2) AS k) FROM t1 stores the replacement in
     * UnboundStar.getReplacedAlias(), OUTSIDE children(): the table count saw only t1, so
     * the two-table query failed the capture gate and the candidate was marked terminal
     * while the audit cursor advanced.
     */
    @Test
    public void testExtractTableNamesStarReplaceScalarSubquery() {
        List<String> tables = PlanCaptureFilter.extractTableNames(
                "SELECT * REPLACE((SELECT MAX(v) FROM t2) AS k) FROM t1");
        Assertions.assertEquals(2, tables.size(),
                "the * REPLACE payload's subquery must contribute its table: " + tables);
        Assertions.assertTrue(tables.stream().anyMatch(t -> t.endsWith("t1")), tables.toString());
        Assertions.assertTrue(tables.stream().anyMatch(t -> t.endsWith("t2")), tables.toString());
    }

    @Test
    public void testExtractTableNamesInvalidSql() {
        // unparseable SQL -> empty list (not an exception)
        List<String> tables = PlanCaptureFilter.extractTableNames("SELECT FROM WHERE");
        Assertions.assertTrue(tables.isEmpty());
    }

    @Test
    public void testExtractTableNamesExcludesCteAliases() {
        // at parse time a CTE consumer is also an UnboundRelation: the alias must not
        // count as a physical table, otherwise the >= 2-table gate admits the
        // single-table workload it is meant to reject
        List<String> onePhysical = PlanCaptureFilter.extractTableNames(
                "WITH c AS (SELECT * FROM t1) SELECT * FROM c");
        Assertions.assertEquals(1, onePhysical.size(),
                "only t1 is a physical table: " + onePhysical);
        Assertions.assertTrue(onePhysical.get(0).endsWith("t1"), onePhysical.toString());

        List<String> twoPhysical = PlanCaptureFilter.extractTableNames(
                "WITH c AS (SELECT * FROM t1 JOIN t2 ON t1.a = t2.a) SELECT * FROM c");
        Assertions.assertEquals(2, twoPhysical.size(),
                "t1 and t2 are physical, c is not: " + twoPhysical);
        Assertions.assertFalse(twoPhysical.stream()
                        .anyMatch(t -> t.endsWith("c") || t.endsWith(".c")),
                "the CTE alias must be excluded: " + twoPhysical);

        // the same alias consumed twice is still ONE physical table
        List<String> reused = PlanCaptureFilter.extractTableNames(
                "WITH c AS (SELECT * FROM t1) SELECT * FROM c x JOIN c y ON x.a = y.a");
        Assertions.assertEquals(1, reused.size(),
                "two consumers of one CTE alias: " + reused);
    }

    /**
     * round-22 #3: with lower_case_table_names != 0 the analyzer resolves `t` and `T` to
     * the SAME physical table, so a self-join of one table must not be counted as a
     * two-table workload - the documented Level 3 filter excludes it, and a
     * case-sensitive set made the capturer create an unnecessary GLOBAL baseline.
     */
    @Test
    public void testTableNamesAreDeduplicatedUnderLowerCaseTableNames() {
        String sql = "SELECT * FROM spm_case_t a JOIN SPM_CASE_T b ON a.k = b.k";
        int original = GlobalVariable.lowerCaseTableNames;
        try {
            GlobalVariable.lowerCaseTableNames = 1;
            List<String> tables = PlanCaptureFilter.extractTableNames(sql);
            Assertions.assertEquals(1, tables.size(),
                    "both spellings resolve to ONE physical table: " + tables);
            AuditEvent event = new AuditEvent();
            event.isQuery = true;
            event.isNereids = true;
            event.isInternal = false;
            event.queryTime = 5000;
            event.scanRows = 100000;
            Assertions.assertFalse(filter.shouldCapture(event, tables),
                    "a self-join of one physical table is not a two-table workload");
        } finally {
            GlobalVariable.lowerCaseTableNames = original;
        }
        // control: under the default case-sensitive rule both spellings stay distinct
        Assertions.assertEquals(2, PlanCaptureFilter.extractTableNames(sql).size());
    }

    // ==================== quoted dotted names / audit-mode prefilter (round-11) ====================

    @Test
    public void testQuotedDottedTableNameKeepsComponentBoundaries() {
        // `t.a` is ONE identifier component (legal under enable_unicode_name_support):
        // the flattened name must keep that boundary so the existence check does not
        // read it as db "t" + table "a" and reject a valid join
        List<String> tables = PlanCaptureFilter.extractTableNames(
                "SELECT * FROM `t.a` JOIN u ON `t.a`.k = u.k");
        Assertions.assertEquals(2, tables.size(), "both tables must be extracted: " + tables);
        Assertions.assertTrue(tables.contains("`t.a`"),
                "the dotted component must stay quoted: " + tables);
    }

    @Test
    public void testQuotedDottedTableResolvesThroughExistenceCheck() throws Exception {
        PlanCaptureFilter captureFilter = new PlanCaptureFilter("", "");
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
            CatalogIf catalog = Mockito.mock(CatalogIf.class);
            DatabaseIf db = Mockito.mock(DatabaseIf.class);
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
            Mockito.when(catalogMgr.getCatalog("internal")).thenReturn(catalog);
            Mockito.when(catalog.getDbNullable("db1")).thenReturn(db);
            Mockito.when(db.getTableNullable("u")).thenReturn(Mockito.mock(TableIf.class));
            // `t.a` resolves as ONE plain component (cannot be split further); the old
            // unquoted "t.a" would have been read as db "t" + table "a" and rejected
            Assertions.assertTrue(captureFilter.allTablesExist(
                    List.of("`t.a`", "u"), "internal", "db1"),
                    "a quoted dotted component must resolve as one table name");
        }
    }

    @Test
    public void testPrefilterExtractionUsesAuditRowSqlMode() {
        org.apache.doris.qe.ConnectContext ctx = new org.apache.doris.qe.ConnectContext();
        ctx.setSessionVariable(new org.apache.doris.qe.SessionVariable());
        ctx.setThreadLocalInfo();
        try {
            ctx.getSessionVariable().setSqlMode(
                    org.apache.doris.qe.SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES);
            String stmt = "SELECT * FROM t1 JOIN t2 ON t1.a = t2.a WHERE t2.s = 'a\\'b'";
            // the daemon thread's global mode would fail this parse ...
            Assertions.assertTrue(PlanCaptureFilter.extractTableNames(stmt).size() < 2,
                    "control: NO_BACKSLASH_ESCAPES breaks the escaped literal");
            // ... the audited statement's own mode must be applied around the extraction
            Assertions.assertEquals(2, org.apache.doris.qe.SqlModeHelper.withSqlMode(
                    org.apache.doris.qe.SqlModeHelper.MODE_DEFAULT,
                    () -> PlanCaptureFilter.extractTableNames(stmt)).size(),
                    "extraction under the audit row's mode must see both tables");
        } finally {
            org.apache.doris.qe.ConnectContext.remove();
        }
    }

    // ==================== table existence resolves in the CAPTURED namespace ====================

    @Test
    public void testAllTablesExistUsesCapturedNamespace() throws Exception {
        PlanCaptureFilter captureFilter = new PlanCaptureFilter("", "");
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);

            CatalogIf external = Mockito.mock(CatalogIf.class);
            Mockito.when(catalogMgr.getCatalog("ext_cat")).thenReturn(external);
            DatabaseIf db = Mockito.mock(DatabaseIf.class);
            Mockito.when(external.getDbNullable("ext_db")).thenReturn(db);
            Mockito.when(db.getTableNullable("t1")).thenReturn(Mockito.mock(TableIf.class));

            // a three-part name resolves through the catalog manager
            Assertions.assertTrue(captureFilter.allTablesExist(
                    List.of("ext_cat.ext_db.t1"), "", ""));
            // a two-part name resolves in the CAPTURED catalog, never against the
            // internal catalog the audit row did not run in
            Assertions.assertTrue(captureFilter.allTablesExist(
                    List.of("ext_db.t1"), "ext_cat", ""));
            // ... and a missing table in that namespace fails the gate
            Assertions.assertFalse(captureFilter.allTablesExist(
                    List.of("ext_db.nope"), "ext_cat", ""));
            // an unresolvable catalog fails as well
            Assertions.assertFalse(captureFilter.allTablesExist(
                    List.of("no_cat.ext_db.t1"), "", ""));
            // a plain one-part name cannot be verified and is treated as existing
            Assertions.assertTrue(captureFilter.allTablesExist(
                    List.of("whatever"), "ext_cat", ""));
            // an empty table list trivially passes
            Assertions.assertTrue(captureFilter.allTablesExist(List.of(), "ext_cat", ""));
        }
    }

    // ==================== pure filter chain ====================

    @Test
    public void testShouldCaptureBasicAndThresholds() {
        AuditEvent event = new AuditEvent();
        event.isQuery = true;
        event.isNereids = true;
        event.isInternal = false;
        event.queryTime = 5000;
        event.scanRows = 100000;

        List<String> tables = List.of("internal.testdb.t1", "internal.testdb.t2");
        Assertions.assertTrue(filter.shouldCapture(event, tables),
                "slow multi-table nereids query must be capturable");
    }

    @Test
    public void testShouldCaptureSkipSingleTable() {
        AuditEvent event = new AuditEvent();
        event.isQuery = true;
        event.isNereids = true;
        event.queryTime = 5000;
        event.scanRows = 100000;

        List<String> tables = List.of("internal.testdb.t1");
        Assertions.assertFalse(filter.shouldCapture(event, tables),
                "single-table query must not be captured");
    }

    @Test
    public void testShouldCaptureSkipFastSmall() {
        AuditEvent event = new AuditEvent();
        event.isQuery = true;
        event.isNereids = true;
        event.queryTime = 10;      // below min query time
        event.scanRows = 10;       // below min scan rows

        List<String> tables = List.of("internal.testdb.t1", "internal.testdb.t2");
        Assertions.assertFalse(filter.shouldCapture(event, tables),
                "fast small query must not be captured");
    }

    @Test
    public void testShouldCaptureSkipLegacyPlanner() {
        AuditEvent event = new AuditEvent();
        event.isQuery = true;
        event.isNereids = false;  // legacy planner
        event.queryTime = 5000;
        event.scanRows = 100000;

        List<String> tables = List.of("internal.testdb.t1", "internal.testdb.t2");
        Assertions.assertFalse(filter.shouldCapture(event, tables),
                "legacy-planner query must not be captured");
    }

    @Test
    public void testIncludeExcludeRegex() {
        PlanCaptureFilter includeOnly = new PlanCaptureFilter("orders", "");
        List<String> tables = List.of("internal.tpch.lineitem", "internal.tpch.orders");
        Assertions.assertTrue(includeOnly.shouldCapture(baseEvent(), tables),
                "query with a matching table must pass the include regex");

        PlanCaptureFilter excludeOrders = new PlanCaptureFilter("", "orders");
        Assertions.assertFalse(excludeOrders.shouldCapture(baseEvent(), tables),
                "query with an excluded table must be skipped");

        PlanCaptureFilter includeOther = new PlanCaptureFilter("^not_there$", "");
        Assertions.assertFalse(includeOther.shouldCapture(baseEvent(), tables),
                "query without a matching table must be skipped");
    }

    private static AuditEvent baseEvent() {
        AuditEvent event = new AuditEvent();
        event.isQuery = true;
        event.isNereids = true;
        event.queryTime = 5000;
        event.scanRows = 100000;
        return event;
    }

    // ==================== BaselineManager capture dedup (design doc 7.2.5) ====================

    @Test
    public void testExistsBaseline() throws Exception {
        SPMPlanner planner = new SPMPlanner();
        String planSql = "SELECT * FROM t1 WHERE a = 100";
        BaselinePlan plan = planner.buildBaseline("SELECT * FROM t1 WHERE a = 100", planSql);
        long id = manager.createBaseline(plan);

        // same digest exists
        Assertions.assertTrue(manager.existsBaselineByDigest(plan.getBindSqlDigest()));
        // same digest + plan exists
        Assertions.assertTrue(manager.existsBaseline(plan.getBindSqlDigest(), planSql));
        // different plan -> not an identical baseline
        Assertions.assertFalse(manager.existsBaseline(plan.getBindSqlDigest(),
                "SELECT * FROM t1 WHERE a = 200"));
        // unknown digest -> false
        Assertions.assertFalse(manager.existsBaselineByDigest("no_such_digest"));
        Assertions.assertTrue(id > 0);
    }

    // ==================== PlanCaptureManager failure handling ====================

    @Test
    public void testProcessCandidateFailureIsSwallowed() {
        // An unplannable statement: the capture pipeline must not throw; the failure
        // counter is incremented and the rest of the cycle continues.
        CapturedQuery bad = new CapturedQuery("SELECT nonsense from nowhere", 5000, 100000, 0,
                "digest", "hash", "db", "internal", "qid-bad");
        // no assert throws
        PlanCaptureManager.getInstance().processCandidateForTest(bad);
        Assertions.assertTrue(PlanCaptureManager.getInstance().getStats().failed >= 0);
    }

    /**
     * A FAILED capture must stay retryable for the next overlapping scan: marking the
     * query id before processCandidate() would make a transient failure permanent (the
     * dedup map only evicts after 10,000 ids, long after the watermark passed the row).
     * Retries are bounded so a permanently broken row is given up on instead of burning
     * every cycle.
     */
    @Test
    public void testFailedCaptureStaysRetryableThenGivesUp() {
        PlanCaptureManager captureManager = PlanCaptureManager.getInstance();
        captureManager.resetForTest();
        // two one-part table names pass the existence gate (unverifiable names are
        // assumed to exist), but the query itself cannot be planned in this environment
        // -> a transient capture failure
        CapturedQuery transientFailure = new CapturedQuery(
                "SELECT t1.a FROM t1 JOIN t2 ON t1.a = t2.a WHERE t1.b = 1",
                5000, 100000, 0, "digest-retry", "hash", "db", "internal", "qid-retry");

        // attempts 1 + 2: the id stays retryable (NOT consumed) and the attempt counter
        // advances
        Assertions.assertFalse(captureManager.processCandidateForTest(transientFailure),
                "a candidate that cannot be planned must report a retryable failure");
        captureManager.handleCandidateForTest(transientFailure);
        Assertions.assertFalse(captureManager.isQueryIdTrackedForTest("qid-retry"),
                "a failed capture must not consume the query id");
        Assertions.assertEquals(1, captureManager.failedAttemptsForTest("qid-retry"));
        captureManager.handleCandidateForTest(transientFailure);
        Assertions.assertFalse(captureManager.isQueryIdTrackedForTest("qid-retry"),
                "the second failure is still retryable");
        Assertions.assertEquals(2, captureManager.failedAttemptsForTest("qid-retry"));

        // attempt 3: bounded retry gives up and marks the id terminal
        captureManager.handleCandidateForTest(transientFailure);
        Assertions.assertTrue(captureManager.isQueryIdTrackedForTest("qid-retry"),
                "a permanently failing row must be given up after bounded attempts");
        Assertions.assertEquals(0, captureManager.failedAttemptsForTest("qid-retry"),
                "the attempt counter is cleared once the id is terminal");

        // a terminal id is skipped WITHOUT another attempt
        long failuresBefore = captureManager.getStats().failed;
        captureManager.handleCandidateForTest(transientFailure);
        Assertions.assertEquals(failuresBefore, captureManager.getStats().failed,
                "a consumed id must be skipped by the next overlapping scan");
    }

    /**
     * A failed capture must receive its retry attempts even when the keyset cursor has
     * already moved past the audit row: keyset pagination and the five-minute overlap
     * window can no longer reach the row, so the queued candidate is replayed by the next
     * cycle instead. Attempts stay bounded and one id is never retried twice per cycle.
     */
    @Test
    public void testFailedCandidateIsReplayedWithoutOverlap() {
        PlanCaptureManager captureManager = PlanCaptureManager.getInstance();
        captureManager.resetForTest();
        CapturedQuery transientFailure = new CapturedQuery(
                "SELECT t1.a FROM t1 JOIN t2 ON t1.a = t2.a WHERE t1.b = 1",
                5000, 100000, 0, "digest-queue", "hash", "db", "internal", "qid-queue");

        captureManager.handleCandidateForTest(transientFailure);
        Assertions.assertEquals(1, captureManager.failedAttemptsForTest("qid-queue"));
        Assertions.assertTrue(captureManager.isQueuedForTest("qid-queue"),
                "a transient failure must be queued for a later retry attempt");

        // next cycle: the page does NOT contain the row (the cursor moved past it)
        captureManager.replayQueuedFailuresForTest(Set.of("qid-other"));
        Assertions.assertEquals(2, captureManager.failedAttemptsForTest("qid-queue"),
                "the queued failure must be retried without the overlap window");

        // an id the page already processed this cycle is not retried a second time
        captureManager.replayQueuedFailuresForTest(Set.of("qid-queue"));
        Assertions.assertEquals(2, captureManager.failedAttemptsForTest("qid-queue"),
                "the page's own attempt must not be duplicated by the replay");

        // third attempt: bounded retry gives up and dequeues the id
        captureManager.replayQueuedFailuresForTest(Set.of());
        Assertions.assertTrue(captureManager.isQueryIdTrackedForTest("qid-queue"),
                "a permanently failing row is given up after bounded attempts");
        Assertions.assertFalse(captureManager.isQueuedForTest("qid-queue"));
        Assertions.assertEquals(0, captureManager.failedAttemptsForTest("qid-queue"));
    }

    @Test
    public void testSuccessfulCandidateIsConsumedImmediately() {
        PlanCaptureManager captureManager = PlanCaptureManager.getInstance();
        captureManager.resetForTest();
        // single-table candidate: terminally filtered (never retried) and consumed
        CapturedQuery singleTable = new CapturedQuery("SELECT k FROM t1", 5000, 100000, 0,
                "digest-one", "hash", "db", "internal", "qid-one");
        captureManager.handleCandidateForTest(singleTable);
        Assertions.assertTrue(captureManager.isQueryIdTrackedForTest("qid-one"),
                "a terminally filtered candidate is consumed");
        Assertions.assertEquals(0, captureManager.failedAttemptsForTest("qid-one"));
    }

    @Test
    public void testCapturedQueryToAuditEvent() {
        CapturedQuery query = new CapturedQuery("SELECT 1", 5000, 100000, 0,
                "digest", "hash", "db", "internal", "qid-1");
        AuditEvent event = query.toAuditEvent();
        Assertions.assertTrue(event.isQuery);
        Assertions.assertTrue(event.isNereids);
        Assertions.assertEquals(5000, event.queryTime);
        Assertions.assertEquals(100000, event.scanRows);
        Assertions.assertEquals("SELECT 1", event.stmt);
        // the audit identity travels with the candidate, so the captured baseline can
        // store it and the source audit row stays reachable by query id
        Assertions.assertEquals("qid-1", query.getQueryId());
        Assertions.assertEquals("qid-1", event.queryId);
    }

    // ==================== durable capture checkpoint (#16) ====================

    /**
     * The window / cursor / retry state must survive a leader handoff: the checkpoint row
     * is encoded and decoded field by field, and the decoded queue keeps a retryable
     * candidate complete enough to retry without re-reading the audit row.
     */
    @Test
    public void testCheckpointRoundTrip() {
        Map<String, Integer> attempts = new LinkedHashMap<>();
        attempts.put("qid-a", 2);
        attempts.put("qid-b", 1);
        Assertions.assertEquals(attempts,
                PlanCaptureManager.decodeFailedAttempts(
                        PlanCaptureManager.encodeFailedAttempts(attempts)));
        Assertions.assertTrue(PlanCaptureManager.decodeFailedAttempts("not-json").isEmpty(),
                "a broken payload must decode to empty, never fail the cycle");

        Map<String, CapturedQuery> queue = new LinkedHashMap<>();
        queue.put("qid-a", new CapturedQuery("SELECT a FROM t1 JOIN t2 ON t1.a = t2.a",
                5000, 100000, 0, "digest", "hash", "db", "internal", "qid-a"));
        Map<String, CapturedQuery> decoded = PlanCaptureManager.decodeRetryQueue(
                PlanCaptureManager.encodeRetryQueue(queue));
        Assertions.assertEquals(1, decoded.size());
        Assertions.assertEquals("SELECT a FROM t1 JOIN t2 ON t1.a = t2.a",
                decoded.get("qid-a").getStmt());
        Assertions.assertEquals(5000, decoded.get("qid-a").getQueryTimeMs());
        Assertions.assertEquals("db", decoded.get("qid-a").getDb());

        // a checkpoint row installs the SAME state the daemon would have kept; the row
        // carries the FULL cursor including its tail (the appended cursor_tail column)
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        String tail = "[\"10.0.0.1\",\"h1\",\"100\",\"10\",\"m1\"]";
        manager.applyCheckpointRow(new ResultRow(List.of(
                "123456", "100", "200", "7", "2026-01-01 00:00:00", "qid-cursor",
                PlanCaptureManager.encodeFailedAttempts(attempts),
                PlanCaptureManager.encodeRetryQueue(queue), tail)));
        Assertions.assertEquals(2, manager.failedAttemptsForTest("qid-a"));
        Assertions.assertTrue(manager.isQueuedForTest("qid-a"),
                "the retry queue must survive the checkpoint");
        Object[] fields = manager.checkpointFieldsForTest();
        Assertions.assertEquals(123456L, fields[0]);
        Assertions.assertEquals(100L, fields[1]);
        Assertions.assertEquals(200L, fields[2]);
        Assertions.assertEquals(7L, fields[3]);
        Assertions.assertEquals("2026-01-01 00:00:00", fields[4]);
        Assertions.assertEquals("qid-cursor", fields[5]);
        Assertions.assertEquals(tail, fields[6],
                "the cursor tail must survive the checkpoint round-trip");
    }

    /**
     * The checkpoint may be read BEFORE the asynchronous internal-schema initializer has
     * created the table / while the BE is not ready. A failed first read must NOT consume
     * the checkpoint: with the flag already set this process would start from the default
     * window and later OVERWRITE the only record of the previous leader's unconsumed tail.
     */
    @Test
    public void testFailedFirstCheckpointReadIsRetried() {
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        AtomicInteger reads = new AtomicInteger();
        manager.setCheckpointReaderForTest(() -> {
            if (reads.incrementAndGet() == 1) {
                throw new RuntimeException("internal table not created yet");
            }
            return List.of(new ResultRow(List.of(
                    "123456", "100", "200", "7", "2026-01-01 00:00:00", "qid-cursor",
                    "{}", "{}")));
        });

        manager.loadCheckpointForTest();
        Assertions.assertFalse(manager.isCheckpointLoadedForTest(),
                "a failed read must keep the checkpoint retryable");

        manager.loadCheckpointForTest();
        Assertions.assertTrue(manager.isCheckpointLoadedForTest(),
                "the successful retry must load the checkpoint");
        Object[] fields = manager.checkpointFieldsForTest();
        Assertions.assertEquals(123456L, fields[0],
                "the retry must apply the previous leader's pending window");
        Assertions.assertEquals(200L, fields[2]);
        // the row predates the cursor_tail column (8 values, cursor present): the
        // partial cursor is NOT trusted - the pending window is re-scanned from the top
        // (captures are idempotent), because a prefix-only cursor loops / skips rows
        // whose (time, query_time, query_id) keys collide
        Assertions.assertEquals(org.apache.doris.nereids.spm.capture.AuditLogScanner.CURSOR_ABSENT,
                fields[3], "a cursor without its tail must be reset, not trusted");
        manager.resetForTest();
    }

    /**
     * Rows whose audit query id is unusable (null / empty / literal "NaN") still have to
     * be retried: the page cursor has already moved past their audit rows, so a transient
     * failure with an UNTRACKED candidate would be silently abandoned after one attempt.
     * A stable synthetic retry key, derived from the row identity, keeps them queued and
     * survives the checkpoint (the queue is keyed by it).
     */
    @Test
    public void testUnusableQueryIdStillRetriesAndRoundTrips() {
        PlanCaptureManager captureManager = PlanCaptureManager.getInstance();
        captureManager.resetForTest();
        String[] unusableIds = {null, "", "NaN"};
        for (int i = 0; i < unusableIds.length; i++) {
            CapturedQuery candidate = new CapturedQuery(
                    "SELECT t1.a FROM t1 JOIN t2 ON t1.a = t2.a WHERE t1.b = " + i,
                    5000, 100000, 0, "digest-noid-" + i, "hash", "db", "internal",
                    unusableIds[i]);
            String key = PlanCaptureManager.retryKeyOf(candidate);
            Assertions.assertTrue(key.startsWith("spm-retry:"),
                    "an unusable query id maps to the synthetic key: " + key);
            Assertions.assertFalse(captureManager.isQueryIdTrackedForTest(usefulKey(candidate)),
                    "nothing consumed yet");

            captureManager.handleCandidateForTest(candidate);
            Assertions.assertFalse(captureManager.isQueryIdTrackedForTest(key),
                    "a failed capture with an unusable id must stay retryable");
            Assertions.assertEquals(1, captureManager.failedAttemptsForTest(key));
            Assertions.assertTrue(captureManager.isQueuedForTest(key),
                    "the candidate must be queued under the synthetic key");

            // the checkpoint round-trip preserves the synthetic key (the queue is encoded
            // as a map, so decode returns the SAME key and the retry survives a handoff)
            Map<String, CapturedQuery> queue = new LinkedHashMap<>();
            queue.put(key, candidate);
            Map<String, CapturedQuery> decoded = PlanCaptureManager.decodeRetryQueue(
                    PlanCaptureManager.encodeRetryQueue(queue));
            Assertions.assertEquals(1, decoded.size());
            Assertions.assertTrue(decoded.containsKey(key),
                    "the synthetic retry key must round-trip: " + decoded.keySet());
            Assertions.assertEquals(candidate.getStmt(), decoded.get(key).getStmt());

            // the same audit row read again (overlap) maps onto the SAME key: skipped
            long failures = captureManager.getStats().failed;
            captureManager.handleCandidateForTest(candidate);
            Assertions.assertEquals(failures + 1, captureManager.getStats().failed,
                    "the overlap re-read must be processed once (retried), not duplicated");
            Assertions.assertEquals(2, captureManager.failedAttemptsForTest(key));
        }
        // the synthetic key is content-derived: two different rows never collide, the
        // same row is stable
        CapturedQuery rowA = new CapturedQuery("SELECT x FROM a1 JOIN a2 ON a1.x = a2.x",
                1, 1, 1, "d", "h", "db", "internal", "NaN");
        CapturedQuery rowB = new CapturedQuery("SELECT y FROM b1 JOIN b2 ON b1.y = b2.y",
                1, 1, 1, "d", "h", "db", "internal", "NaN");
        Assertions.assertNotEquals(PlanCaptureManager.retryKeyOf(rowA),
                PlanCaptureManager.retryKeyOf(rowB), "different rows get different keys");
        Assertions.assertEquals(PlanCaptureManager.retryKeyOf(rowA),
                PlanCaptureManager.retryKeyOf(new CapturedQuery(rowA.getStmt(), 1, 1, 1,
                        "d", "h", "db", "internal", "NaN")),
                "the same row is stable across re-reads");
        captureManager.resetForTest();
    }

    /** Helper: the key a USABLE id would have (identity for non-synthetic ids). */
    private static String usefulKey(CapturedQuery candidate) {
        return candidate.getQueryId() == null ? "" : candidate.getQueryId();
    }

    /**
     * The checkpoint replacement used to be two separately committed statements
     * (DELETE, then INSERT): a crash / leadership loss / timeout / failed INSERT after the
     * DELETE left NO row and the next leader permanently skipped the deleted pending
     * window's tail. It must be ONE upserted statement on the UNIQUE-key table now.
     */
    @Test
    public void testCheckpointReplacementIsOneUpsertStatement() {
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        List<String> statements = new ArrayList<>();
        manager.setCheckpointWriterForTest((sql, params) -> statements.add(sql));

        manager.persistCheckpointForTest();
        Assertions.assertEquals(1, statements.size(),
                "the checkpoint must be replaced by exactly one statement: " + statements);
        Assertions.assertTrue(statements.get(0).startsWith("INSERT INTO"), statements.get(0));
        Assertions.assertFalse(statements.get(0).contains("DELETE"),
                "the only checkpoint row must never be deleted before its replacement is durable: "
                        + statements.get(0));
        // the UPSERT must address its columns by NAME: the physical order of an upgraded
        // table can differ from the canonical schema order (see
        // InternalSchemaInitializerTest#testCheckpointUpgradeRestoresCanonicalColumnOrder),
        // and a positional VALUES would shift the tail JSON into failed_attempts there
        Assertions.assertTrue(
                statements.get(0).contains("(`id`, `last_scan_timestamp`, `pending_window_start`"),
                "the checkpoint UPSERT must carry its explicit target column list: "
                        + statements.get(0));
        manager.resetForTest();
    }
    // ==================== unavailable external metadata stays retryable (round-13) ====================

    /**
     * An EXTERNAL catalog that has not finished (or failed) initializing answers null
     * from getDbNullable although the database may well exist. The old code could not
     * distinguish that from a confirmed missing table, made the audit row TERMINAL and
     * advanced the keyset cursor past it - an otherwise eligible external query was lost
     * permanently during a transient outage on the capturing FE.
     */
    @Test
    public void testUnavailableExternalMetadataIsRetryable() throws Exception {
        PlanCaptureFilter captureFilter = new PlanCaptureFilter("", "");
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
            org.apache.doris.datasource.ExternalCatalog external =
                    Mockito.mock(org.apache.doris.datasource.ExternalCatalog.class);
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
            Mockito.when(catalogMgr.getCatalog("ext_cat")).thenReturn(external);
            Mockito.when(external.isInitialized()).thenReturn(false);
            Mockito.when(external.getDbNullable("ext_db")).thenReturn(null);

            Assertions.assertEquals(PlanCaptureFilter.TableLookup.UNAVAILABLE,
                    captureFilter.checkAllTablesExist(
                            List.of("ext_cat.ext_db.t1", "ext_cat.ext_db.t2"),
                            "ext_cat", "ext_db"),
                    "an uninitialized external catalog answers null for a db that may"
                            + " well exist: the row must stay eligible");

            // once initialized, an absent db is definitive
            Mockito.when(external.isInitialized()).thenReturn(true);
            Assertions.assertEquals(PlanCaptureFilter.TableLookup.MISSING,
                    captureFilter.checkAllTablesExist(
                            List.of("ext_cat.ext_db.t1"), "ext_cat", "ext_db"),
                    "an initialized catalog that reports no db is a definitive miss");
        }
    }

    @Test
    public void testMetadataFetchFailureIsRetryable() throws Exception {
        PlanCaptureFilter captureFilter = new PlanCaptureFilter("", "");
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
            org.apache.doris.datasource.ExternalCatalog external =
                    Mockito.mock(org.apache.doris.datasource.ExternalCatalog.class);
            org.apache.doris.datasource.ExternalDatabase<?> db =
                    Mockito.mock(org.apache.doris.datasource.ExternalDatabase.class);
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
            Mockito.when(catalogMgr.getCatalog("ext_cat")).thenReturn(external);
            Mockito.when(external.isInitialized()).thenReturn(true);
            // doReturn (Object-typed) sidesteps the covariantly narrowed return type of
            // ExternalCatalog#getDbNullable (a DatabaseIf mock would fail its runtime
            // return-type check)
            Mockito.doReturn(db).when(external).getDbNullable("ext_db");
            // the metastore is briefly unreachable
            Mockito.when(db.getTableNullable("t1"))
                    .thenThrow(new RuntimeException("metastore unavailable"));

            Assertions.assertEquals(PlanCaptureFilter.TableLookup.UNAVAILABLE,
                    captureFilter.checkAllTablesExist(
                            List.of("ext_cat.ext_db.t1"), "ext_cat", "ext_db"),
                    "a metadata fetch failure must be a retryable outcome, not a"
                            + " confirmed missing table");
        }
    }

    /**
     * End to end at the manager level: a candidate whose table metadata is unavailable
     * must report a NON-terminal failure (kept retryable) instead of being marked
     * processed and stepped over by the keyset cursor.
     */
    @Test
    public void testUnavailableMetadataKeepsCandidateRetryable() throws Exception {
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
            org.apache.doris.datasource.ExternalCatalog external =
                    Mockito.mock(org.apache.doris.datasource.ExternalCatalog.class);
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
            Mockito.when(catalogMgr.getCatalog("ext_cat")).thenReturn(external);
            Mockito.when(external.isInitialized()).thenReturn(false);
            Mockito.when(external.getDbNullable("ext_db")).thenReturn(null);

            CapturedQuery candidate = new CapturedQuery(
                    "SELECT t1.a FROM ext_cat.ext_db.t1 t1 JOIN ext_cat.ext_db.t2 t2"
                            + " ON t1.a = t2.a",
                    5000, 100000, 0, "digest-lookup", "hash", "ext_db", "ext_cat",
                    "qid-lookup");
            Assertions.assertFalse(manager.processCandidateForTest(candidate),
                    "unavailable metadata must be a RETRYABLE failure, not a permanent"
                            + " terminal decision that makes the row unreachable");
            manager.handleCandidateForTest(candidate);
            Assertions.assertTrue(manager.isQueuedForTest("qid-lookup"),
                    "the candidate must be queued for a later retry");
            Assertions.assertFalse(manager.isQueryIdTrackedForTest("qid-lookup"),
                    "a deferred candidate must not be consumed");
        } finally {
            manager.resetForTest();
        }
    }

    /**
     * The synthetic retry key must include the ORIGINATING parser mode: the scanner keeps
     * same-text default / PIPES_AS_CONCAT executions separate (a || b has different
     * semantics), and without the mode the first capture consumed the shared key - the
     * second row was skipped as already processed while the cursor advanced, or two
     * failures overwrote each other in the retry queue.
     */
    @Test
    public void testSyntheticRetryKeyIncludesSqlMode() {
        String stmt = "SELECT t1.a FROM t1 JOIN t2 ON t1.a = t2.a WHERE t1.b = 7";
        CapturedQuery defaultMode = new CapturedQuery(stmt, 5000, 100000, 0, "digest-mode",
                "hash", "db", "internal", "NaN");
        CapturedQuery concatMode = new CapturedQuery(stmt, 5000, 100000, 0, "digest-mode",
                "hash", "db", "internal", "NaN", false,
                org.apache.doris.qe.SqlModeHelper.MODE_PIPES_AS_CONCAT);
        Assertions.assertNotEquals(PlanCaptureManager.retryKeyOf(defaultMode),
                PlanCaptureManager.retryKeyOf(concatMode),
                "same-text executions under different parser modes must get different"
                        + " synthetic retry keys");

        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try {
            manager.handleCandidateForTest(defaultMode);
            manager.handleCandidateForTest(concatMode);
            Assertions.assertTrue(manager.isQueuedForTest(
                    PlanCaptureManager.retryKeyOf(defaultMode)));
            Assertions.assertTrue(manager.isQueuedForTest(
                    PlanCaptureManager.retryKeyOf(concatMode)),
                    "the second mode's failure must not be skipped as already processed");
            Assertions.assertEquals(1, manager.failedAttemptsForTest(
                    PlanCaptureManager.retryKeyOf(concatMode)));
        } finally {
            manager.resetForTest();
        }
    }

    /**
     * round-30 #6: a QUEUED failure must be retried with the eligibility decision of the
     * window that queued it. A transient failure (unavailable external metadata) leaves
     * the row behind the keyset cursor, so re-judging it against a configuration that
     * changed in between (raised thresholds, a new include pattern) would mark it
     * TERMINAL and drop its retry - the row is then neither captured nor inside the next
     * window's overlap.
     */
    @Test
    public void testQueuedFailureKeepsItsEligibilityAcrossAConfigChange() throws Exception {
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
            org.apache.doris.datasource.ExternalCatalog external =
                    Mockito.mock(org.apache.doris.datasource.ExternalCatalog.class);
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
            Mockito.when(catalogMgr.getCatalog("ext_cat")).thenReturn(external);
            Mockito.when(external.isInitialized()).thenReturn(false);
            Mockito.when(external.getDbNullable("ext_db")).thenReturn(null);

            CapturedQuery queued = new CapturedQuery(
                    "SELECT t1.a FROM ext_cat.ext_db.t1 t1 JOIN ext_cat.ext_db.t2 t2"
                            + " ON t1.a = t2.a",
                    5000, 100000, 0, "digest-keep", "hash", "ext_db", "ext_cat",
                    "qid-keep");
            Assertions.assertFalse(manager.processCandidateForTest(queued),
                    "precondition: unavailable metadata is a retryable failure");
            manager.handleCandidateForTest(queued);
            Assertions.assertTrue(manager.isQueuedForTest("qid-keep"),
                    "precondition: the failure is queued for a later cycle");

            // the admin raises the thresholds and adds an include pattern while the failure
            // waits: the retry completes the WINDOW's decision instead of re-judging the row
            manager.setFilterForTest(new PlanCaptureFilter("only_this_table", "",
                    10_000_000L, 10_000_000L));
            manager.replayQueuedFailuresForTest(Set.of());
            Assertions.assertTrue(manager.isQueuedForTest("qid-keep"),
                    "the queued failure must stay retryable despite the new thresholds -"
                            + " its audit row is behind the keyset cursor");
            Assertions.assertFalse(manager.isQueryIdTrackedForTest("qid-keep"),
                    "and it must not be marked as terminally processed");

            // a candidate of the CURRENT window is still judged by the current config
            CapturedQuery fresh = new CapturedQuery(queued.getStmt(), 5000, 100000, 0,
                    "digest-fresh", "hash", "ext_db", "ext_cat", "qid-fresh");
            manager.handleCandidateForTest(fresh);
            Assertions.assertFalse(manager.isQueuedForTest("qid-fresh"),
                    "a page candidate is filtered by the CURRENT configuration");
            Assertions.assertTrue(manager.isQueryIdTrackedForTest("qid-fresh"),
                    "a filtered-out page candidate is terminal: the cursor consumed its row");
        } finally {
            manager.resetForTest();
        }
    }
}
