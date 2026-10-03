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
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.BitSet;
import java.util.List;

/**
 * Round-27 review fixes without their own regression suite:
 *
 * - #1 / #4 (SPMPlanTreeSupport, LogicalPlanBuilder): the REPLAY of a frozen plan used to
 *   be planned by the ordinary planner with the session's FULL rule set, while the frozen
 *   plan was produced under the SPM rule whitelist (every MV rewrite excluded). A rule
 *   excluded at CREATE - an async MTMV that became eligible afterwards is the reported
 *   case - then rewrote the frozen text onto the MV's storage table, and the post-plan
 *   fingerprint guard rejected its own replay (with the default enable_spm_fallback=false
 *   the SELECT failed although the source table had not changed). The replay now installs
 *   the SAME whitelist mask (SPMOptimizer#installSpmReplayRuleMask), so the mechanism is
 *   pinned here: the MV family is forbidden, the engine essentials and the ordinary
 *   binding / implementation rules stay enabled, and an unbuildable session whitelist
 *   degrades to the previous (session-rules) behavior instead of failing the query.
 *   The SHOW BASELINE PLANS WHERE matrix lives in SPMMatchingSafetyTest (it gained the
 *   "no supported column" shapes), and the authoritative-SHOW refresh lives in
 *   BaselineManagerConcurrencyTest.
 * - #2 (SPMPlanTreeSupport): scan selectors are compared per table IN STATEMENT ORDER: a
 *   bind over {@code t PARTITION(p1) a CROSS JOIN t PARTITION(p2) b} with a manual plan
 *   that swaps p1 / p2 between the occurrences was accepted by the per-table multiset
 *   comparison; the two texts disagree as soon as a partition content changes, so the
 *   ambiguous swap is rejected.
 */
public class SPMRound27SafetyTest {

    // ==================== #1: the replay refuses MV substitution ====================

    /**
     * The mask the replay installs forbids every MATERIALIZED_VIEW rule - the fix for the
     * reported MTMV substitution - while the ordinary planning rules stay enabled.
     */
    @Test
    public void testReplayMaskForbidsTheMaterializedViewFamily() {
        StatementContext statementContext = new StatementContext();
        SPMOptimizer.installSpmReplayRuleMask(statementContext);
        BitSet forbidden = statementContext.getOrCacheDisableRules(new SessionVariable());

        List<String> mvRules = SPMOptimizer.getMaterializedViewRuleNames();
        Assertions.assertFalse(mvRules.isEmpty());
        for (String ruleName : mvRules) {
            Assertions.assertTrue(forbidden.get(RuleType.valueOf(ruleName).ordinal()),
                    "the replay must not apply the MV rule that would substitute another"
                            + " table for the frozen source tables: " + ruleName);
        }
        Assertions.assertFalse(forbidden.get(RuleType.BINDING_RELATION.ordinal()),
                "binding must stay enabled: the frozen plan is re-built, not re-read");
        Assertions.assertFalse(
                forbidden.get(RuleType.LOGICAL_PROJECT_TO_PHYSICAL_PROJECT_RULE.ordinal()),
                "implementation rules must stay enabled");
        // the engine-essential checks are never gated, even through this mask
        Assertions.assertFalse(forbidden.get(RuleType.CHECK_PRIVILEGES.ordinal()));
        Assertions.assertFalse(forbidden.get(RuleType.CHECK_ROW_POLICY.ordinal()));
    }

    /**
     * The mask is exactly the MV family: the other SPM-excluded rules stay available for
     * the replay. Forbidding the WHOLE CREATE-side whitelist complement broke the
     * caller's LIMIT contract - ELIMINATE_LIMIT ("limit = 0" -> empty relation) is
     * SPM-excluded, so a replayed LIMIT 0 returned one row (regression suite
     * test_spm_review_round9).
     */
    @Test
    public void testReplayMaskKeepsTheNonSubstitutingRules() {
        StatementContext statementContext = new StatementContext();
        SPMOptimizer.installSpmReplayRuleMask(statementContext);
        BitSet forbidden = statementContext.getOrCacheDisableRules(new SessionVariable());

        for (RuleType ruleType : RuleType.values()) {
            Assertions.assertEquals(ruleType.isMaterializedViewRule(),
                    forbidden.get(ruleType.ordinal()),
                    "only the MV family may be forbidden while a baseline is replayed: "
                            + ruleType);
        }
        Assertions.assertFalse(forbidden.get(RuleType.ELIMINATE_LIMIT.ordinal()),
                "the caller's LIMIT 0 must still become an empty relation");
        Assertions.assertFalse(
                forbidden.get(RuleType.ELIMINATE_LIMIT_ON_ONE_ROW_RELATION.ordinal()));
    }

    /**
     * The session's own disable list keeps applying on top of the replay mask, and the
     * mask does not touch the session's rule cache for any other statement.
     */
    @Test
    public void testReplayMaskCombinesWithTheSessionRules() {
        StatementContext statementContext = new StatementContext();
        SessionVariable session = new SessionVariable();
        session.setDisableNereidsRules(RuleType.SALT_JOIN.name());
        SPMOptimizer.installSpmReplayRuleMask(statementContext);
        BitSet forbidden = statementContext.getOrCacheDisableRules(session);
        Assertions.assertTrue(forbidden.get(RuleType.SALT_JOIN.ordinal()),
                "the session's own disable list keeps applying");
        Assertions.assertTrue(
                forbidden.get(RuleType.valueOf(SPMOptimizer.getMaterializedViewRuleNames().get(0))
                        .ordinal()),
                "the MV family stays forbidden");
    }

    // ==================== #2: pins stay attached to their occurrence ====================

    /**
     * The reviewer's scenario: the bind pins p1 on the first occurrence and p2 on the
     * second, the manual plan swaps them. The two texts agree on the initial content
     * (both yield the same pairing only while p2 is empty) and diverge afterwards, while
     * the bind/caller match and the table fingerprint still pass - so the pair must be
     * rejected at CREATE.
     */
    @Test
    public void testSwappedPartitionPinsBetweenOccurrencesAreRejected() {
        RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
                () -> new SPMPlanner().buildBaseline(
                        "SELECT a.k, b.k FROM t PARTITION(p1) a CROSS JOIN t PARTITION(p2) b",
                        "SELECT a.k, b.k FROM t PARTITION(p2) a CROSS JOIN t PARTITION(p1) b"));
        Assertions.assertTrue(failure.getMessage() != null
                        && failure.getMessage().contains("DIFFERENT occurrences"),
                failure.getMessage());
    }

    /**
     * Same occurrence, same order: the pins are aligned and the pair stays accepted.
     */
    @Test
    public void testAlignedPartitionPinsAreAccepted() throws Exception {
        BaselinePlan baseline = new SPMPlanner().buildBaseline(
                "SELECT a.k, b.k FROM t PARTITION(p1) a CROSS JOIN t PARTITION(p2) b",
                "SELECT a.k, b.k FROM t PARTITION(p1) a CROSS JOIN t PARTITION(p2) b");
        Assertions.assertNotNull(baseline.getBindSql());
    }

    /**
     * A divergence on the SAME occurrence keeps the original message (it is a mismatch,
     * not an ambiguous swap).
     */
    @Test
    public void testDivergentPinOnTheSameOccurrenceIsAMismatch() {
        RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
                () -> new SPMPlanner().buildBaseline(
                        "SELECT a.k, b.k FROM t PARTITION(p1) a CROSS JOIN t b",
                        "SELECT a.k, b.k FROM t PARTITION(p2) a CROSS JOIN t b"));
        Assertions.assertTrue(failure.getMessage() != null
                        && failure.getMessage().contains("scan selectors")
                        && !failure.getMessage().contains("DIFFERENT occurrences"),
                failure.getMessage());
    }

    /**
     * Both occurrences pinned identically on both sides: the lists are equal in statement
     * order, so the pair is accepted even though the multiset comparison would also accept
     * a swap.
     */
    @Test
    public void testIdenticalPinsOnBothOccurrencesAreAccepted() throws Exception {
        BaselinePlan baseline = new SPMPlanner().buildBaseline(
                "SELECT a.k, b.k FROM t PARTITION(p1) a CROSS JOIN t PARTITION(p1) b",
                "SELECT a.k, b.k FROM t PARTITION(p1) a CROSS JOIN t PARTITION(p1) b");
        Assertions.assertEquals(
                "SELECT a.k, b.k FROM t PARTITION(p1) a CROSS JOIN t PARTITION(p1) b",
                baseline.getBindSql());
    }
}
