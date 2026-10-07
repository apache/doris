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

import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * The manual-baseline plan contract (review fixes without their own regression
 * suite). A CREATE GLOBAL BASELINE PLAN binds one query shape to a frozen plan text,
 * and the earlier guards only compared the SCAN SELECTIONS and the row caps: a plan that
 * dropped the caller's row filter, read another table, changed the output columns / arity
 * or re-ordered the caller's result was stored and replayed silently.
 * The repair compares the two PARSED trees at CREATE - same sources, same output
 * expressions (labels may differ), bind filters contained in the plan, same top-level
 * ORDER BY contract - and re-checks the ordering at replay.
 */
public class SPMRound44SafetyTest {

    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }

    private static RuntimeException buildFails(String bindSql, String planSql) {
        return Assertions.assertThrows(RuntimeException.class,
                () -> new SPMPlanner().buildBaseline(bindSql, planSql));
    }

    // ==================== #1: bind-side filters ====================

    /**
     * The reviewer's case: bind SELECT k FROM t WHERE k = 1 to plan
     * SELECT k FROM t introduces no unmatched placeholder and passed the scan
     * guard, but a later WHERE k = 2 query matched the bind tree while the
     * frozen plan returned every row of t ({1,2} instead of {2}).
     */
    @Test
    public void testDroppedBindFilterIsRejected() {
        RuntimeException failure = buildFails(
                "SELECT k FROM t WHERE k = 1", "SELECT k FROM t");
        Assertions.assertTrue(failure.getMessage() != null
                        && failure.getMessage().contains("filter"),
                failure.getMessage());
    }

    /**
     * The same filter in both statements stays accepted; the texts may differ in case /
     * formatting, the PARSED conjuncts must be the same.
     */
    @Test
    public void testIdenticalFiltersStayAccepted() throws Exception {
        BaselinePlan baseline = new SPMPlanner().buildBaseline(
                "select k from t where k = 1", "SELECT k FROM t WHERE k = 1");
        Assertions.assertEquals("select k from t where k = 1", baseline.getBindSql());
    }

    // ==================== #9: output expressions / arity ====================

    /**
     * Binding SELECT k to plan SELECT v renames the replayed v column to
     * k while KEEPING v's expression: on t(k=1,v=9) a matching caller received k=9. The
     * output expressions must be equivalent; only the LABEL may differ.
     */
    @Test
    public void testOutputColumnDivergenceIsRejected() {
        RuntimeException failure = buildFails(
                "SELECT k FROM t", "SELECT v FROM t");
        Assertions.assertTrue(failure.getMessage() != null
                        && failure.getMessage().contains("output column"),
                failure.getMessage());
    }

    /** A plan with another arity bypassed the label alignment and changed the result arity. */
    @Test
    public void testOutputArityChangeIsRejected() {
        RuntimeException failure = buildFails(
                "SELECT k FROM t", "SELECT k, v FROM t");
        Assertions.assertTrue(failure.getMessage() != null
                        && failure.getMessage().contains("arity"),
                failure.getMessage());
    }

    /** A LABEL-only difference stays accepted (explicit aliases are the author's choice). */
    @Test
    public void testLabelOnlyDifferenceStaysAccepted() throws Exception {
        BaselinePlan baseline = new SPMPlanner().buildBaseline(
                "SELECT k FROM t", "SELECT k AS kk FROM t");
        Assertions.assertEquals("SELECT k FROM t", baseline.getBindSql());
    }

    // ==================== #11: plan-side sources ====================

    /**
     * The reviewer's case: bind SELECT k FROM t, plan SELECT k FROM u.
     * With t={1} and u={9}, a matching query of t replayed u and returned 9; the create
     * / replay schema fingerprints both include the stable union of t and u, so they
     * never caught it.
     */
    @Test
    public void testUnrelatedPlanSideScanIsRejected() {
        RuntimeException failure = buildFails(
                "SELECT k FROM t", "SELECT k FROM u");
        Assertions.assertTrue(failure.getMessage() != null
                        && failure.getMessage().contains("never reads"),
                failure.getMessage());
    }

    /** Both statements reading the SAME table set stay accepted. */
    @Test
    public void testSameSourceSetStaysAccepted() throws Exception {
        BaselinePlan baseline = new SPMPlanner().buildBaseline(
                "SELECT t1.k FROM t1 JOIN t2 ON t1.k = t2.k",
                "SELECT t1.k FROM t1 JOIN t2 ON t1.k = t2.k");
        Assertions.assertEquals("SELECT t1.k FROM t1 JOIN t2 ON t1.k = t2.k",
                baseline.getBindSql());
    }

    // ==================== #10: ordering ====================

    /**
     * A manual baseline could bind ... ORDER BY k ASC to plan
     * ... ORDER BY k DESC: CREATE accepted the same scan, both trees have no row
     * cap, and the replay passed the limit contract without comparing the sort - for
     * t={1,2} the caller requesting (1,2) received (2,1).
     */
    @Test
    public void testTopLevelOrderDivergenceIsRejected() {
        RuntimeException failure = buildFails(
                "SELECT k FROM t ORDER BY k ASC", "SELECT k FROM t ORDER BY k DESC");
        Assertions.assertTrue(failure.getMessage() != null
                        && failure.getMessage().contains("ORDER BY"),
                failure.getMessage());
    }

    /**
     * The replay-side half: an UNCAPPED caller sort must survive the replay. A replayed
     * tree without the caller's sort (or with the opposite direction) is not a valid
     * rewrite although the row-cap checks pass; a caller without a top-level sort has no
     * ordering contract to preserve.
     */
    @Test
    public void testOrderContractPreservedChecksTheUncappedCallerSort() {
        LogicalPlan callerAsc = parse("SELECT k FROM t ORDER BY k ASC");
        LogicalPlan replayedAsc = parse("SELECT k FROM t ORDER BY k ASC");
        LogicalPlan replayedDesc = parse("SELECT k FROM t ORDER BY k DESC");
        LogicalPlan replayedNone = parse("SELECT k FROM t");
        Assertions.assertTrue(SPMPlanTreeSupport.orderContractPreserved(replayedAsc, callerAsc),
                "the same sort survives the replay");
        Assertions.assertFalse(SPMPlanTreeSupport.orderContractPreserved(replayedDesc, callerAsc),
                "a flipped direction must not serve the caller's (1,2) request with (2,1)");
        Assertions.assertFalse(SPMPlanTreeSupport.orderContractPreserved(replayedNone, callerAsc),
                "the caller's uncapped sort must not be dropped by the replay");
        Assertions.assertTrue(SPMPlanTreeSupport.orderContractPreserved(replayedNone,
                        parse("SELECT k FROM t")),
                "an unordered caller has no ordering contract to preserve");
        // the frozen (decompiled) optimal plan appends deterministic TIE-BREAKER keys
        // after the caller's own (tpch q18: two caller keys plus c_name / c_custkey /
        // o_orderkey): those refine unspecified ties and must not reject the replay,
        // while a caller key dropped from or flipped in the replay must
        LogicalPlan callerTwoKeys = parse("SELECT k FROM t ORDER BY k ASC, k + 1 DESC");
        LogicalPlan replayedMoreKeys = parse(
                "SELECT k FROM t ORDER BY k ASC, k + 1 DESC, k + 2 ASC, k + 3 ASC");
        LogicalPlan replayedFewerKeys = parse("SELECT k FROM t ORDER BY k ASC");
        LogicalPlan replayedSecondFlipped = parse("SELECT k FROM t ORDER BY k ASC, k + 1 ASC");
        Assertions.assertTrue(
                SPMPlanTreeSupport.orderContractPreserved(replayedMoreKeys, callerTwoKeys),
                "the frozen tie-breaker keys after the caller's own must not reject the replay");
        Assertions.assertFalse(
                SPMPlanTreeSupport.orderContractPreserved(replayedFewerKeys, callerTwoKeys),
                "a caller key dropped from the replay breaks the visible ordering");
        Assertions.assertFalse(
                SPMPlanTreeSupport.orderContractPreserved(replayedSecondFlipped, callerTwoKeys),
                "a flipped second caller key must not be accepted");
        List<String> contract = SPMPlanTreeSupport.rootOrderContractForTest(replayedDesc);
        Assertions.assertEquals(1, contract.size(), "one order key, got: " + contract);
        Assertions.assertTrue(contract.get(0).startsWith("DESC/"),
                "the contract renders the direction: " + contract);
    }

    // ==================== #2: root-star labels over a join ====================

    /**
     * The reviewer's case: a baseline on
     * SELECT * FROM (SELECT k + 1 FROM t) s CROSS JOIN u matched the
     * k + 2 caller, but the label derivation returned null at the join and the
     * captured frozen sink label k + 1 stayed - the caller received the wrong
     * result-column header. The caller's expansion derives the DERIVED leading side
     * (k + 2) and leaves the underivable trailing side's real column names alone.
     */
    @Test
    public void testStarOverJoinAlignsTheDerivableLeadingSide() {
        LogicalPlan rewritten = parse(
                "SELECT `s`.`k + 1` AS `k + 1`, u.k FROM"
                        + " (SELECT k + 1 AS `k + 1` FROM t) s CROSS JOIN u");
        LogicalPlan caller = parse("SELECT * FROM (SELECT k + 2 FROM t) s CROSS JOIN u");
        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(rewritten, caller);
        Assertions.assertNotSame(rewritten, aligned,
                "the captured header must be realigned for the caller's star");
        UnboundAlias alignedItem = findUnboundAlias(aligned);
        Assertions.assertNotNull(alignedItem, "the aligned tree exposes the renamed item");
        Assertions.assertEquals("k + 2", alignedItem.getAlias().orElse(null),
                "the caller's expanded header must replace the captured one: " + aligned);
    }

    /**
     * An UNDERIVABLE side BEFORE the derivable one cannot be aligned positionally: the
     * derivable prefix has no position, so the rewrite must be SKIPPED (the caller keeps
     * its own plan). The alignment raises the skip signal instead of handing back a tree
     * whose later positions still expose the frozen captured names.
     */
    @Test
    public void testStarOverJoinWithLeadingUnknowableSideSkipsTheRewrite() {
        LogicalPlan rewritten = parse(
                "SELECT u.k, `s`.`k + 1` AS `k + 1` FROM u CROSS JOIN"
                        + " (SELECT k + 1 AS `k + 1` FROM t) s");
        LogicalPlan caller = parse("SELECT * FROM u CROSS JOIN (SELECT k + 2 FROM t) s");
        Assertions.assertThrows(
                SPMPlanTreeSupport.UnalignableOutputLabelsException.class,
                () -> SPMPlanTreeSupport.alignRootOutputLabels(rewritten, caller),
                "an unpositionable derivable prefix must skip the rewrite, not leak names");
    }

    /** First UnboundAlias reachable through the plan's expressions (null when none). */
    private static UnboundAlias findUnboundAlias(LogicalPlan plan) {
        final UnboundAlias[] found = new UnboundAlias[1];
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, (Plan node) -> {
            for (Expression expression : node.getExpressions()) {
                findUnboundAlias(expression, found);
            }
        });
        return found[0];
    }

    private static void findUnboundAlias(Expression expression, UnboundAlias[] found) {
        if (found[0] != null) {
            return;
        }
        if (expression instanceof UnboundAlias) {
            found[0] = (UnboundAlias) expression;
            return;
        }
        for (Expression child : expression.children()) {
            findUnboundAlias(child, found);
        }
    }
}
