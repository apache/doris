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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The round-48 contracts:
 *
 * - a column literally named `a.b` and a qualified reference a.b must not compare
 *   equal in the create-time output / projection checks: the replay then reads the
 *   wrong column for callers selecting the dotted name;
 * - a caller SET_VAR hint is applied by EliminateLogicalSelectHint, which the SPM
 *   replay bypasses: the hint sets a STATEMENT-scoped session variable, so every
 *   SET_VAR hint of the caller is carried onto the replay, wherever its block sits;
 * - a derivable caller label behind an unknown-width join input cannot be aligned
 *   positionally: the rewrite must be SKIPPED instead of exposing captured headers;
 * - a set-operation root aligns its first branch's labels onto the replayed output,
 *   including a replay that wraps the set in a projection over a subquery alias.
 */
public class SPMRound48SafetyTest {

    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }

    /** A quoted single part `a.b` must not pass as a qualified reference a.b. */
    @Test
    public void testDottedIdentifierBoundaryIsRejected() {
        Assertions.assertThrows(RuntimeException.class,
                () -> new SPMPlanner().buildBaseline(
                        "SELECT `a.b` FROM t a", "SELECT a.b FROM t a"),
                "a single part named a.b and a qualified reference read different columns");
    }

    /** Every SET_VAR hint is carried: its effect is statement-scoped, block-independent. */
    @Test
    public void testSetVarHints() {
        LogicalPlan rootHint = parse(
                "SELECT /*+ SET_VAR(time_zone='+08:00') */ k FROM t");
        LogicalPlan carriedRoot = SPMPlanTreeSupport.carrySetVarHints(rootHint, rootHint);
        Assertions.assertTrue(carriedRoot instanceof LogicalSelectHint,
                "the replay must re-attach the caller's root SET_VAR hint: " + carriedRoot);
        LogicalPlan nestedHint = parse(
                "SELECT k FROM (SELECT /*+ SET_VAR(time_zone='+08:00') */ k FROM t) s");
        LogicalPlan carriedNested = SPMPlanTreeSupport.carrySetVarHints(nestedHint, nestedHint);
        Assertions.assertTrue(carriedNested instanceof LogicalSelectHint,
                "a nested SET_VAR writes the same STATEMENT-scoped session variable, so it"
                        + " must be carried as well: " + carriedNested);
        Assertions.assertSame(nestedHint,
                SPMPlanTreeSupport.carrySetVarHints(nestedHint, parse("SELECT k FROM t")),
                "a hint-free caller leaves the replay untouched");
    }

    /** A derivable label behind an unknown-width join input cannot be aligned. */
    @Test
    public void testUnknownWidthJoinInputSkipsAlignment() {
        LogicalPlan plan = parse(
                "SELECT * FROM t1 CROSS JOIN (SELECT k + 1 FROM t2) s");
        Assertions.assertThrows(
                SPMPlanTreeSupport.UnalignableOutputLabelsException.class,
                () -> SPMPlanTreeSupport.alignRootOutputLabels(plan, plan),
                "the caller's k + 1 label sits behind the unknown-width t1");
    }

    /** A set-operation root realigns the first branch's labels (the header). */
    @Test
    public void testSetRootLabelsAlign() {
        LogicalPlan user = parse(
                "SELECT k + 2 FROM t UNION ALL SELECT k + 2 FROM u");
        LogicalPlan rewritten = parse(
                "SELECT k + 1 FROM t UNION ALL SELECT k + 1 FROM u");
        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(rewritten, user);
        Assertions.assertNotSame(rewritten, aligned,
                "the captured k + 1 header must be replaced by the caller's k + 2");
        LogicalProject<?> firstBranch = firstBranchOf(aligned);
        Assertions.assertTrue(firstBranch.getProjects().get(0).toString().contains("k + 2"),
                "the caller-visible header comes from the first branch: "
                        + firstBranch.getProjects());
    }

    /**
     * The frozen text of a set baseline may wrap the set in a projection over a
     * SUBQUERY ALIAS. The alignment must descend through those wrappers instead of
     * skipping the rewrite (an ordinary UNION ALL baseline stopped hitting when only a
     * direct Project-over-SetOp wrapper was recognized).
     */
    @Test
    public void testWrappedSetThroughAliasAligns() {
        LogicalPlan user = parse("SELECT k FROM t UNION ALL SELECT k FROM u");
        LogicalPlan rewritten = parse(
                "SELECT `s`.`k` AS `k` FROM (SELECT k FROM t UNION ALL"
                        + " SELECT k FROM u) s");
        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(rewritten, user);
        Assertions.assertSame(rewritten, aligned,
                "the replayed labels already equal the caller's first-branch labels: "
                        + aligned);
    }

    /**
     * A CONSTANT first branch (SELECT 1 AS c) has no projection to rebuild: the labels
     * can only be compared, and an equal label set leaves the replay untouched (the
     * ordinary constant-UNION baseline must keep hitting).
     */
    @Test
    public void testWrappedSetWithConstantBranchHits() {
        LogicalPlan user = parse("SELECT 1 AS c UNION ALL SELECT k AS c FROM t");
        LogicalPlan rewritten = parse(
                "SELECT `s`.`c` AS `c` FROM (SELECT 1 AS `c` UNION ALL"
                        + " SELECT k AS `c` FROM t) s");
        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(rewritten, user);
        Assertions.assertSame(rewritten, aligned,
                "the labels already agree, so the constant-branch set replay stays: "
                        + aligned);
    }

    /** The frozen text wraps the set and PINS derived labels that must be realigned. */
    @Test
    public void testWrappedSetThroughAliasRealigns() {
        LogicalPlan user = parse(
                "SELECT k + 2 FROM t UNION ALL SELECT k + 2 FROM u");
        LogicalPlan rewritten = parse(
                "SELECT `s`.`k + 1` AS `k + 1` FROM (SELECT k + 1 AS `k + 1` FROM t"
                        + " UNION ALL SELECT k + 1 AS `k + 1` FROM u) s");
        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(rewritten, user);
        Assertions.assertNotSame(rewritten, aligned,
                "the captured k + 1 header must be replaced by the caller's k + 2");
        LogicalProject<?> wrapper = firstProjectOf(aligned);
        Assertions.assertEquals("k + 2",
                ((UnboundAlias) wrapper.getProjects().get(0)).getAlias().orElse(null),
                "the outer projection pins the caller-visible header: "
                        + wrapper.getProjects());
    }

    private static LogicalProject<?> firstBranchOf(Plan plan) {
        Plan node = plan;
        while (!(node instanceof LogicalSetOperation)) {
            node = node.child(0);
        }
        return (LogicalProject<?>) node.child(0);
    }

    /** The outermost projection of the tree (the caller-visible output list). */
    private static LogicalProject<?> firstProjectOf(Plan plan) {
        Plan node = plan;
        while (!(node instanceof LogicalProject)) {
            node = node.child(0);
        }
        return (LogicalProject<?>) node;
    }

    /**
     * The frozen text of a UNION DISTINCT baseline wraps the set in
     * SQAlias + GROUP BY + SORT + SQAlias + ORDER BY layers - the DISTINCT re-encoding.
     * The alignment must accept the shape and keep the equal labels untouched (a
     * single-child search that stopped at the first output-carrying node rejected the
     * whole replay of the live regression).
     */
    @Test
    public void testFrozenUnionDistinctShapeHits() {
        LogicalPlan user = parse("select k from a union select k from b order by k");
        LogicalPlan rewritten = parse(
                "SELECT k FROM (SELECT k FROM ((SELECT k FROM a) UNION ALL"
                        + " (SELECT k FROM b)) t_0 GROUP BY k ORDER BY k ASC NULLS FIRST)"
                        + " t_1 ORDER BY k ASC NULLS FIRST");
        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(rewritten, user);
        Assertions.assertSame(rewritten, aligned,
                "equal labels leave the union-distinct replay untouched: " + aligned);
    }

    /** The same for a UNION ALL baseline's frozen text. */
    @Test
    public void testFrozenUnionAllShapeHits() {
        LogicalPlan user = parse(
                "select k from a union all select k from b order by k");
        LogicalPlan rewritten = parse(
                "SELECT k FROM ((SELECT k FROM a) UNION ALL (SELECT k FROM b)) t_0"
                        + " ORDER BY k ASC NULLS FIRST");
        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(rewritten, user);
        Assertions.assertSame(rewritten, aligned,
                "equal labels leave the union-all replay untouched: " + aligned);
    }
}
