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
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCheckPolicy;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.BitSet;

/**
 * Review fixes without their own regression suite:
 *
 * - #1 (SPMPlanTreeSupport, stripCheckPolicy): the policy markers of a relation inside a
 *   SUBQUERY plan live in SubqueryExpr.queryPlan, which the children-only walk
 *   never visited. CREATE's nested analyzer therefore expanded the CREATOR's row policy
 *   on that relation into an ordinary filter INSIDE the frozen SQL - a GLOBAL baseline
 *   authored by a non-root ADMIN with a policy on u served every other user the creator's
 *   filter on u. The strip now also walks the expression-owned plans (and fails closed if
 *   a marker survives).
 * - #5 (StatementContext#resetPlannerStateForReplan): the SPM replay rule mask
 *   (spmExcludedRules plus the cached disableRules) stayed installed when
 *   the fallback re-planned the ORIGINAL statement, so an original aggregate with an
 *   eligible refreshed MTMV scanned base tables instead of using ordinary MV planning.
 * - #8 (same reset): the abandoned pass's session-variable changes (a plan-side ORDERED
 *   hint sets disable_join_reorder, the planner assigns
 *   runtime_filter_wait_time_ms, a plan-side SET_VAR sets its key) are recorded as
 *   "single set var" originals and were only reverted at statement END - the fallback ran
 *   with them, and a leaked disable_join_reorder marks the CALLER's own
 *   LEADING(a b) hint UNUSED.
 */
public class SPMRound28SafetyTest {

    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }

    /** Counts the policy markers reachable from the tree (expression plans included). */
    private static int countPolicyMarkers(Plan plan) {
        int[] count = {0};
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, (Plan node) -> {
            if (node instanceof LogicalCheckPolicy) {
                count[0]++;
            }
        });
        return count[0];
    }

    // ==================== #1: subquery relations are stripped too ====================

    /**
     * The reviewer's shape: the subquery's relation u carries the parse-time marker inside
     * SubqueryExpr.queryPlan. Stripping only children()/CTE bodies left it in
     * place, and the nested analyzer then expanded the CREATOR's policy on u into the
     * frozen SQL.
     */
    @Test
    public void testPolicyMarkersInsideSubqueryPlansAreStripped() {
        LogicalPlan plan = parse("SELECT k FROM t1 WHERE k IN (SELECT k FROM t2)");
        Assertions.assertTrue(countPolicyMarkers(plan) >= 2,
                "the parser marks every relation, including the subquery's: "
                        + plan.treeString());

        LogicalPlan stripped = SPMPlanTreeSupport.stripCheckPolicy(plan);
        Assertions.assertEquals(0, countPolicyMarkers(stripped),
                "no policy marker may reach the SPM optimizer: " + stripped.treeString());
        Assertions.assertTrue(stripped.treeString().contains("UnboundRelation"),
                "the relations themselves must stay: " + stripped.treeString());

        // an EXISTS subquery and a scalar subquery in the select list are the other
        // expression-owned carriers
        Assertions.assertEquals(0, countPolicyMarkers(SPMPlanTreeSupport.stripCheckPolicy(
                parse("SELECT k FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.k = t1.k)"))));
        Assertions.assertEquals(0, countPolicyMarkers(SPMPlanTreeSupport.stripCheckPolicy(
                parse("SELECT (SELECT max(k) FROM t2) AS m, k FROM t1"))));

        // ... and stripping an already stripped tree is still a no-op
        Assertions.assertEquals(0, countPolicyMarkers(
                SPMPlanTreeSupport.stripCheckPolicy(stripped)));
    }

    /**
     * SELECT * REPLACE(...) payloads own their subquery plans OUTSIDE children():
     * a policy on the payload's relation must not be frozen either.
     */
    @Test
    public void testPolicyMarkersInsideStarReplacePayloadsAreStripped() {
        LogicalPlan plan = parse("SELECT * REPLACE((SELECT max(k) FROM t2) AS k) FROM t1");
        Assertions.assertTrue(countPolicyMarkers(plan) >= 2, plan.treeString());
        Assertions.assertEquals(0,
                countPolicyMarkers(SPMPlanTreeSupport.stripCheckPolicy(plan)));
    }

    // ==================== #5: the replay mask is a per-pass state ====================

    /**
     * The fallback re-plans the ORIGINAL statement on the same StatementContext: leaving
     * the replay mask (all MATERIALIZED_VIEW rewrites forbidden) installed made the
     * "ordinary" plan skip MV planning entirely.
     */
    @Test
    public void testReplayMaskIsClearedBeforeTheFallbackReplan() {
        StatementContext statementContext = new StatementContext();
        SessionVariable session = new SessionVariable();
        SPMOptimizer.installSpmReplayRuleMask(statementContext);
        BitSet beforeReset = statementContext.getOrCacheDisableRules(session);
        Assertions.assertTrue(beforeReset.get(RuleType.MATERIALIZED_VIEW_PROJECT_JOIN.ordinal()),
                "precondition: the replay forbids the MV family");

        statementContext.resetPlannerStateForReplan();
        BitSet afterReset = statementContext.getOrCacheDisableRules(session);
        Assertions.assertFalse(
                afterReset.get(RuleType.MATERIALIZED_VIEW_PROJECT_JOIN.ordinal()),
                "the fallback must plan the original statement under the session's own"
                        + " rules (an eligible MTMV stays usable)");
        Assertions.assertEquals(session.getDisableNereidsRules(), afterReset,
                "only the session's own disable list remains");
    }

    // ==================== #8: abandoned session changes are restored ====================

    /**
     * A plan-side ORDERED hint switches disable_join_reorder on "for this
     * statement"; the fallback must not inherit it, otherwise the CALLER's own
     * LEADING(a b) hint is marked UNUSED and the ordinary fallback picks another
     * join order.
     */
    @Test
    public void testAbandonedSessionChangesAreRestoredBeforeReplan() {
        SessionVariable session = new SessionVariable();
        Assertions.assertFalse(session.isDisableJoinReorder(), "precondition: default off");

        ConnectContext connectContext = new ConnectContext();
        connectContext.setSessionVariable(session);
        StatementContext statementContext = new StatementContext(connectContext,
                new OriginStatement("SELECT 1", 0));
        connectContext.setStatementContext(statementContext);

        // the abandoned pass's plan-side ORDERED hint (EliminateLogicalSelectHint)
        Assertions.assertTrue(session.setVarOnce(SessionVariable.DISABLE_JOIN_REORDER, "true"));
        Assertions.assertTrue(session.isDisableJoinReorder(),
                "precondition: the hint switched the variable on for this statement");
        Assertions.assertFalse(session.getSessionOriginValue().isEmpty(),
                "precondition: the original value is recorded for the statement-end revert");

        statementContext.resetPlannerStateForReplan();
        Assertions.assertFalse(session.isDisableJoinReorder(),
                "the fallback must run with the pre-statement value");
        Assertions.assertTrue(session.getSessionOriginValue().isEmpty(),
                "the revert is complete: the statement-end revert has nothing left to do");
        Assertions.assertFalse(session.getIsSingleSetVar());
    }
}
