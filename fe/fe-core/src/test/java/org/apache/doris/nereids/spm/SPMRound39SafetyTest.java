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

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.spm.manager.BaselineManager;
import org.apache.doris.nereids.spm.placeholder.SPMPlaceholderBuilder;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Review fixes without their own regression suite:
 *
 * - #1 (SPMPlanner#limitContractPreserved): an UNBOUNDED caller must never replay a
 *   frozen plan that retains a row cap (a manual planSql 'SELECT k FROM t LIMIT 1' over
 *   the unbounded bind 'SELECT k FROM t'), because the early return skipped the retained-
 *   cap check and the replay silently returned one row instead of all rows.
 * - #15 (SPMPlanTreeSupport#rejectScanSelectorMismatch): scan selectors are compared per
 *   OCCURRENCE (alias-attached), not by the per-table walk-order list: a reversed self
 *   join produced the identical list 't -> [p1, p2]' and every replay then returned the
 *   other pairing.
 */
public class SPMRound39SafetyTest {

    private BaselineManager manager;

    @BeforeEach
    public void setUp() {
        manager = BaselineManager.getInstance();
        manager.clearForTest();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }

    private static void installConnectContext() {
        ConnectContext ctx = new ConnectContext();
        ctx.setSessionVariable(new SessionVariable());
        ctx.setThreadLocalInfo();
        ctx.setStatementContext(new StatementContext(ctx, new OriginStatement("SELECT 1", 0)));
    }

    /**
     * Hand-builds a baseline whose planSql is a frozen plan text (the equivalent of what
     * the CREATE path stores after a successful decompile), exactly like
     * SPMFrozenTreeReplayTest#frozenBaseline.
     */
    private static BaselinePlan frozenBaseline(String bindSql, String frozenPlanSql)
            throws Exception {
        LogicalPlan bindPlan = parse(bindSql);
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        LogicalPlan parameterizedBind = SPMPlanTreeSupport.transform(
                bindPlan, expr -> expr.accept(builder, null));

        BaselinePlan baseline = new BaselinePlan();
        baseline.setBindSql(bindSql);
        baseline.setBindSqlDigest(bindPlan.toSpmDigest());
        baseline.setBindSqlHash(SPMUtils.hashOf(bindPlan.toSpmDigest()));
        baseline.setPlanSql(frozenPlanSql);
        baseline.setParameterizedBindPlan(parameterizedBind);
        baseline.setCost(0.0);
        return baseline;
    }

    // ==================== #1: unbounded callers and retained plan caps ====================

    /**
     * The reviewer's case: CREATE BASELINE PLAN 'SELECT k FROM t' WITH 'SELECT k FROM t
     * LIMIT 1' passes creation (the bind is unbounded), and the later unbounded SELECT
     * matches that bind. The frozen plan RETAINS LIMIT 1; the old early return
     * (userLimit == null -> true) skipped the retained-cap check, so the replay
     * silently returned ONE row instead of all rows. The candidate must be skipped.
     */
    @Test
    public void testUnboundedCallerNeverReplaysACappedFrozenPlan() throws Exception {
        installConnectContext();
        SPMPlanner planner = new SPMPlanner();
        manager.createBaseline(frozenBaseline("SELECT k FROM t", "SELECT k FROM t LIMIT 1"));

        LogicalPlan rewritten = planner.tryRewritePlan(parse("SELECT k FROM t"),
                System.currentTimeMillis() + 5000);
        Assertions.assertNull(rewritten,
                "a caller without a top-level LIMIT must not replay a plan that retains"
                        + " LIMIT 1 - the captured cap is NOT the caller's contract and the"
                        + " result would be truncated to one row");
    }

    /** Control: an unbounded replay from an UNBOUNDED frozen plan stays usable. */
    @Test
    public void testUnboundedReplayWithoutRetainedCapsStaysUsable() throws Exception {
        installConnectContext();
        SPMPlanner planner = new SPMPlanner();
        manager.createBaseline(frozenBaseline(
                "SELECT * FROM t1 WHERE a > 100",
                "SELECT * FROM t1 WHERE (a > CAST(_spm_const_var(1) AS INT))"));

        LogicalPlan rewritten = planner.tryRewritePlan(parse("SELECT * FROM t1 WHERE a > 42"),
                System.currentTimeMillis() + 5000);
        Assertions.assertNotNull(rewritten,
                "an unbounded caller must keep matching a frozen plan without caps");
        Assertions.assertTrue(planner.getUsedBaselineId() > 0, "the baseline must be used");
    }

    // ==================== #15: scan selectors are per-occurrence ====================

    /**
     * The reviewer's reversed self join: bind t PARTITION(p1) a CROSS JOIN
     * t PARTITION(p2) b and a manual plan t PARTITION(p1) b CROSS JOIN
     * t PARTITION(p2) a both produced t -> [p1, p2] in walk order, so CREATE
     * accepted them - and with k=1 in p1 and k=11 in p2 the bind returned (1,11) while the
     * frozen plan returned (11,1). The alias-attached comparison must reject the pair.
     */
    @Test
    public void testReversedSelfJoinIsRejectedEvenWithIdenticalSelectorLists() {
        LogicalPlan bind = parse("SELECT a.k, b.k FROM t PARTITION(p1) a"
                + " CROSS JOIN t PARTITION(p2) b");
        LogicalPlan plan = parse("SELECT b.k, a.k FROM t PARTITION(p1) b"
                + " CROSS JOIN t PARTITION(p2) a");
        AnalysisException failure = Assertions.assertThrows(AnalysisException.class,
                () -> SPMPlanTreeSupport.rejectScanSelectorMismatch(bind, plan,
                        "SELECT a.k, b.k FROM t PARTITION(p1) a CROSS JOIN t PARTITION(p2) b"),
                "the same selector list attached to SWAPPED aliases must be rejected");
        Assertions.assertTrue(failure.getMessage().contains("DIFFERENT occurrences"),
                "the error must name the occurrence ambiguity: " + failure.getMessage());
    }

    /** The SAME aliases carry the SAME pins: accepted (statement-order alignment). */
    @Test
    public void testAlignedSelfJoinOccurrencesAreAccepted() {
        LogicalPlan bind = parse("SELECT a.k, b.k FROM t PARTITION(p1) a"
                + " CROSS JOIN t PARTITION(p2) b");
        LogicalPlan plan = parse("SELECT b.k, a.k FROM t PARTITION(p1) a"
                + " CROSS JOIN t PARTITION(p2) b");
        Assertions.assertDoesNotThrow(
                () -> SPMPlanTreeSupport.rejectScanSelectorMismatch(bind, plan, "bind"));
    }

    /**
     * A single occurrence is compared by its SELECTOR alone: the alias cannot disambiguate
     * anything, and a manual plan may name its relation differently.
     */
    @Test
    public void testSingleOccurrenceAliasMayDiffer() {
        LogicalPlan bind = parse("SELECT x.k FROM t PARTITION(p1) x");
        LogicalPlan plan = parse("SELECT y.k FROM t PARTITION(p1) y2");
        Assertions.assertDoesNotThrow(
                () -> SPMPlanTreeSupport.rejectScanSelectorMismatch(bind, plan, "bind"),
                "one occurrence: only the selector matters");
    }

    /** A genuinely different selector still fails with the selector-mismatch message. */
    @Test
    public void testDifferentSelectorsAreStillRejected() {
        LogicalPlan bind = parse("SELECT x.k FROM t PARTITION(p1) x");
        LogicalPlan plan = parse("SELECT y.k FROM t PARTITION(p2) y");
        AnalysisException failure = Assertions.assertThrows(AnalysisException.class,
                () -> SPMPlanTreeSupport.rejectScanSelectorMismatch(bind, plan, "bind"));
        Assertions.assertTrue(failure.getMessage().contains("selectors"),
                failure.getMessage());
    }
}
