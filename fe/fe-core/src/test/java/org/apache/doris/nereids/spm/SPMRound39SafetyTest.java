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

    // ==================== CTE alias rename in the LIMIT-contract keys ====================

    /**
     * The frozen planSql replayed for an identical-text baseline is the DECOMPILED
     * plan: the decompiler regenerates every WITH alias (the caller's `ws_wh` becomes
     * `t_4`) while preserving the WITH structure and its order. The caller's cap key
     * (`...:ws_wh#1`) could therefore never be contained in the replay's
     * (`...:t_4#1`), the LIMIT contract of the identical query failed and 31 tpcds
     * suites could not hit their own baselines. A CTE reference must be identified by
     * its CTE's DEFINITION ORDER index, exactly like relation ordinals already replace
     * table aliases.
     */
    @Test
    public void testCteAliasRenameKeepsTheCapContractOfTheIdenticalQuery() {
        LogicalPlan user = parse("WITH ws_wh AS (SELECT k FROM t)"
                + " SELECT k FROM ws_wh ORDER BY k LIMIT 100");
        LogicalPlan replay = parse("WITH t_4 AS (SELECT k FROM t)"
                + " SELECT k FROM t_4 ORDER BY k LIMIT 100");
        Assertions.assertTrue(SPMPlanTreeSupport.rowLimitsWithin(replay, user),
                "the replayed cap must be recognized as the caller's own although the"
                        + " CTE alias was regenerated");
        Assertions.assertTrue(SPMPlanTreeSupport.rowLimitsSurviveReplay(replay, user),
                "every caller cap must survive in the replay although the CTE alias"
                        + " was regenerated");
        Assertions.assertEquals(SPMPlanTreeSupport.rowLimitKeysForTest(user),
                SPMPlanTreeSupport.rowLimitKeysForTest(replay),
                "the cap keys of the identical query must be equal modulo the"
                        + " decompiler-regenerated CTE alias");
    }

    /** Distinct CTEs keep distinct identities - a cap moved between them still differs. */
    @Test
    public void testDifferentCtesStillGetDifferentCapIdentities() {
        LogicalPlan first = parse("WITH a AS (SELECT k FROM t), b AS (SELECT k FROM t)"
                + " SELECT k FROM a ORDER BY k LIMIT 100");
        LogicalPlan second = parse("WITH a AS (SELECT k FROM t), b AS (SELECT k FROM t)"
                + " SELECT k FROM b ORDER BY k LIMIT 100");
        Assertions.assertNotEquals(SPMPlanTreeSupport.rowLimitKeysForTest(first),
                SPMPlanTreeSupport.rowLimitKeysForTest(second),
                "caps over DIFFERENT CTEs of the same table must not collapse into one"
                        + " identity");
    }

    // ==================== optimizer CSE collapse in the TOP cap ====================

    /**
     * The optimizer collapses repeated scans (common-subexpression elimination): the RAW
     * caller joins three occurrences of t while the frozen optimal plan reads t once
     * (tpcds q76 folds three date_dim scans into one). The TOP cap truncates the same
     * ordered result, so its occurrence-tagged keys must compare equal modulo the
     * ordinals - while a different input set or an inner cap keeps the strict identity.
     */
    @Test
    public void testTopCapToleratesOccurrenceCollapseFromCse() {
        LogicalPlan user = parse("SELECT a1.k FROM t a1 CROSS JOIN t a2 CROSS JOIN t a3"
                + " ORDER BY 1 LIMIT 100");
        LogicalPlan replay = parse("SELECT a1.k FROM t a1 ORDER BY 1 LIMIT 100");
        Assertions.assertFalse(SPMPlanTreeSupport.rowLimitsSurviveReplay(replay, user),
                "the strict occurrence-tagged comparison cannot hold across the"
                        + " collapse - the relaxation below is what fixes the suite");
        Assertions.assertTrue(SPMPlanTreeSupport.topCapContractPreserved(replay, user),
                "the top cap truncates the same result; only the occurrence ordinals"
                        + " differ");
        LogicalPlan otherTable = parse("SELECT a1.k FROM u a1 ORDER BY 1 LIMIT 100");
        Assertions.assertFalse(SPMPlanTreeSupport.topCapContractPreserved(otherTable, user),
                "a DIFFERENT top-level input set must stay rejected");
        LogicalPlan innerCap = parse("SELECT a1.k FROM (SELECT k FROM t LIMIT 5) a1"
                + " ORDER BY 1 LIMIT 100");
        Assertions.assertFalse(SPMPlanTreeSupport.topCapContractPreserved(replay, innerCap),
                "a caller whose own tree carries an inner cap must keep the strict"
                        + " comparison (the replay lacks it)");
    }
}
