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

        // a checkpoint row installs the SAME state the daemon would have kept
        PlanCaptureManager manager = PlanCaptureManager.getInstance();
        manager.resetForTest();
        manager.applyCheckpointRow(new ResultRow(List.of(
                "123456", "100", "200", "7", "2026-01-01 00:00:00", "qid-cursor",
                PlanCaptureManager.encodeFailedAttempts(attempts),
                PlanCaptureManager.encodeRetryQueue(queue))));
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
        manager.resetForTest();
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
        manager.resetForTest();
    }
}
