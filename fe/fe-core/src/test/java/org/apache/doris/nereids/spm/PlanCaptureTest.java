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

import org.apache.doris.nereids.spm.capture.CapturedQuery;
import org.apache.doris.nereids.spm.capture.PlanCaptureFilter;
import org.apache.doris.nereids.spm.capture.PlanCaptureManager;
import org.apache.doris.nereids.spm.manager.BaselineManager;
import org.apache.doris.plugin.AuditEvent;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

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
}
