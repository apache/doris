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
import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.spm.manager.SessionBaselineStore;
import org.apache.doris.nereids.spm.placeholder.SPMPlaceholderBuilder;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Round-32 review fixes without their own regression suite:
 *
 * - #8 (SPMPlanner#limitContractPreserved): a LIMIT VARIANT replay must not expose a
 *   row-limiting cap the caller's own tree does not have. The transfer is positional, so
 *   an inner cap of a MANUAL plan (below a node the merge cannot align) kept the CAPTURED
 *   value and silently truncated the variant (the reviewer's DISTINCT over an inner
 *   ORDER BY ... LIMIT 1 case).
 * - #9 (SPMPlaceholderBuilder / PlaceholderExpr): the placeholder identity must include
 *   the SELECT-list item POSITION and the query BLOCK, or independent literals share one
 *   id - "SELECT 1 AS x, 1 AS y" (same value, same child position, same parent signature)
 *   and equal filter literals of an outer block and a derived table (both block 0) could
 *   never be varied independently.
 * - #10 (SPMPlanTreeSupport#alignRootOutputLabels): a root SELECT * keeps the star as ONE
 *   caller item while the frozen sink pins the EXPANDED, capture-time items - the label of
 *   the value variant was left on the replayed result.
 */
public class SPMRound32SafetyTest {

    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }

    // ==================== #8: inner caps of a limit variant ====================

    /**
     * The replayed (manual) plan keeps an inner cap the caller's tree does not have - the
     * exact shape of a limit variant that must be REJECTED.
     */
    @Test
    public void testInnerCapOfAManualPlanIsNotJustifiedByTheCaller() {
        LogicalPlan replayed = parse(
                "SELECT DISTINCT k FROM (SELECT k FROM t ORDER BY k LIMIT 1) s"
                        + " ORDER BY k LIMIT 2");
        LogicalPlan user = parse("SELECT k FROM t ORDER BY k LIMIT 2");
        Assertions.assertFalse(SPMPlanTreeSupport.rowLimitsWithin(replayed, user),
                "the inner LIMIT 1 was inherited from the captured plan, not from the"
                        + " caller's LIMIT 2");
        // the same caps on BOTH sides are the caller's own contract
        Assertions.assertTrue(SPMPlanTreeSupport.rowLimitsWithin(replayed, parse(
                "SELECT k FROM (SELECT k FROM t ORDER BY k LIMIT 1) s ORDER BY k LIMIT 2")),
                "an inner cap the caller itself wrote is justified");
    }

    /** Only the TOP-LEVEL limit is transferred: a variant without inner caps stays valid. */
    @Test
    public void testTopLevelOnlyVariantIsStillAccepted() {
        LogicalPlan replayed = parse("SELECT k FROM t ORDER BY k LIMIT 2");
        LogicalPlan user = parse("SELECT k FROM t ORDER BY k LIMIT 2");
        Assertions.assertTrue(SPMPlanTreeSupport.rowLimitsWithin(replayed, user));
        Assertions.assertArrayEquals(new long[] {2L, 0L},
                SPMPlanTreeSupport.topLevelLimitOf(replayed));
    }

    // ==================== #9: placeholder identity ====================

    /**
     * Two literals of one SELECT list must be distinguishable by their ITEM POSITION:
     * {@code SELECT 1 AS x, 1 AS y} has the same value, the same child position (0 inside
     * the Alias) and the same parent signature (which omits the alias name), so they used
     * to share one placeholder and {@code SELECT 2 AS x, 3 AS y} could never match.
     */
    @Test
    public void testIndependentProjectionLiteralsGetDistinctPlaceholderIds() {
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        LogicalPlan bind = SPMPlanTreeSupport.transform(parse("SELECT 1 AS x, 1 AS y"), builder);
        Assertions.assertEquals(2, builder.getPlaceholderExprs().size(),
                "the two SELECT-list literals must not share one placeholder id");
        long firstId = idOfProjectItem(bind, 0);
        long secondId = idOfProjectItem(bind, 1);
        Assertions.assertNotEquals(firstId, secondId,
                "each SELECT-list item keeps its own placeholder id");

        // the value variant resolves each item to its OWN literal (the user tree is the RAW
        // parse: the matcher extracts the USER's literals against the bind placeholders)
        Map<Long, Expression> values = new HashMap<>();
        LogicalPlan user = parse("SELECT 2 AS x, 3 AS y");
        Assertions.assertTrue(SPMPlanTreeSupport.check(bind, user, values),
                "a variant with different values per column must match");
        Assertions.assertEquals("2", values.get(firstId).toSql());
        Assertions.assertEquals("3", values.get(secondId).toSql());
    }

    /**
     * Equal literals of an OUTER block and a DERIVED table must not share one id either:
     * both blocks used to be numbered 0, and the parent signatures (a comparison against
     * a column of the same name) coincide.
     */
    @Test
    public void testDerivedTableLiteralsGetTheirOwnBlock() {
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        LogicalPlan bind = SPMPlanTreeSupport.transform(
                parse("SELECT * FROM (SELECT k FROM t WHERE k = 1) s WHERE k = 1"), builder);
        Assertions.assertEquals(2, builder.getPlaceholderExprs().size(),
                "the inner and the outer literal must get independent ids");

        // the variant changing only the INNER value still matches and extracts both
        LogicalPlan user = parse("SELECT * FROM (SELECT k FROM t WHERE k = 2) s WHERE k = 1");
        Map<Long, Expression> values = new HashMap<>();
        Assertions.assertTrue(SPMPlanTreeSupport.check(bind, user, values),
                "changing one of the two equal literals must stay matchable");
        Assertions.assertEquals(2, values.size(), "both literals are extracted: " + values);
    }

    /**
     * The two trees of ONE baseline must still agree: the bind tree and the (separately
     * parsed) plan tree traverse the same structure, so corresponding literals keep
     * corresponding ids - a shared builder reuses the id of the first tree in the second.
     */
    @Test
    public void testPositionedIdentityStaysAlignedAcrossTheTwoTrees() {
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        LogicalPlan bind = SPMPlanTreeSupport.transform(parse("SELECT 1 AS x, 1 AS y"), builder);
        LogicalPlan plan = SPMPlanTreeSupport.transform(parse("SELECT 1 AS x, 1 AS y"), builder);
        Assertions.assertEquals(2, builder.getPlaceholderExprs().size(),
                "the second tree must REUSE the ids of the first, not allocate new ones");
        Assertions.assertEquals(idOfProjectItem(bind, 0), idOfProjectItem(plan, 0));
        Assertions.assertEquals(idOfProjectItem(bind, 1), idOfProjectItem(plan, 1));
    }

    // ==================== #10: root SELECT star labels ====================

    /**
     * {@code SELECT * FROM (SELECT k + 1 FROM t) s} keeps the star as ONE item, while the
     * frozen sink pins the captured expansion (the {@code k + 1} header). Aligning with
     * the EXPANDED caller labels exposes the caller's own column name ({@code k + 2}).
     */
    @Test
    public void testRootStarExpandsToTheCallersDerivedLabels() {
        LogicalPlan rewritten = parse("SELECT k AS `k + 1` FROM t");
        LogicalPlan user = parse("SELECT * FROM (SELECT k + 2 FROM t) s");
        // the caller's own label of the derived column: the item of the project BELOW the
        // outer star (the star itself carries no label)
        LogicalProject<?> outer = firstProject(user);
        Assertions.assertNotNull(outer, "the caller's tree must contain a projection");
        LogicalProject<?> derived = firstProject(outer.child(0));
        Assertions.assertNotNull(derived, "the derived relation must contain a projection");
        String expected = ((UnboundAlias) derived.getProjects().get(0)).getAlias().orElse(null);
        Assertions.assertNotNull(expected, "the derived label must be derivable");

        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(rewritten, user);
        Assertions.assertNotSame(rewritten, aligned,
                "the captured header must be replaced by the caller's derived label");
        UnboundAlias item = (UnboundAlias) firstItemOfFirstProject(aligned);
        Assertions.assertEquals(expected, item.getAlias().orElse(null),
                "the replayed result must expose the caller's own column name: " + aligned);
        Assertions.assertEquals("k", item.child().toSql(),
                "the substituted expression itself stays untouched");
    }

    /** A star whose expansion is not derivable (a base relation) must stay untouched. */
    @Test
    public void testStarOverABaseRelationIsLeftAlone() {
        LogicalPlan rewritten = parse("SELECT k AS other FROM t");
        LogicalPlan user = parse("SELECT * FROM t");
        Assertions.assertSame(rewritten,
                SPMPlanTreeSupport.alignRootOutputLabels(rewritten, user),
                "nothing to realign: the star expands to real column names");
    }

    // ==================== #1: the forwarded session baseline payload ====================

    /**
     * round-32 #1 / round-34 #3: the SESSION baselines of a forwarding connection ride in
     * one session variable, part of a Thrift request whose default message limit is
     * 100 MiB. The budget is now enforced where the rows are CREATED - the store rejects a
     * baseline that would not fit - instead of letting serialize drop rows: a dropped row
     * still participated in LOCAL matching, so the same connection rewrote a statement
     * with its SESSION baseline yet planned the FORWARDED statement without it (falling
     * through to a GLOBAL baseline or to none). Every accepted row is therefore always
     * carried.
     */
    @Test
    public void testForwardedSessionPayloadBudgetIsEnforcedAtCreation() {
        SessionBaselineStore store = new SessionBaselineStore();
        String filler = "x".repeat(2 * 1024 * 1024);
        int accepted = 0;
        String rejection = null;
        for (int i = 0; i < 8; i++) {
            BaselinePlan plan = new BaselinePlan();
            plan.setBindSql("select " + i + " /* " + filler + " */");
            plan.setPlanSql("select " + i + " /* " + filler + " */");
            plan.setBindSqlDigest("digest-" + i);
            plan.setBindSqlHash(i + 1);
            plan.setStatus(BaselineStatus.ENABLED);
            try {
                store.createBaseline(plan);
                accepted++;
            } catch (IllegalStateException e) {
                rejection = e.getMessage();
                break;
            }
        }
        Assertions.assertTrue(accepted >= 1, "the budget must accept the first rows");
        Assertions.assertTrue(accepted < 8, "the budget must reject the rows over it");
        Assertions.assertNotNull(rejection, "over-budget creations must be rejected");
        Assertions.assertTrue(rejection.contains("payload"), rejection);

        String payload = SPMForwardedSession.serialize(store);
        Assertions.assertTrue(payload.length() <= SPMForwardedSession.MAX_PAYLOAD_CHARS,
                "the payload stays inside the transport budget: " + payload.length());
        for (int i = 0; i < accepted; i++) {
            Assertions.assertTrue(payload.contains("select " + i + " "),
                    "EVERY accepted row must be carried - a silently skipped row changed the"
                            + " rewrite context of the forwarded statement: row " + i);
        }

        // dropping a row frees its share of the budget for a new creation
        store.dropBaseline(store.getAllBaselines().get(0).getId());
        BaselinePlan replacement = new BaselinePlan();
        replacement.setBindSql("select 99 /* " + filler + " */");
        replacement.setPlanSql("select 99 /* " + filler + " */");
        replacement.setBindSqlDigest("digest-99");
        replacement.setBindSqlHash(99);
        store.createBaseline(replacement);
        Assertions.assertTrue(SPMForwardedSession.serialize(store).contains("select 99 "),
                "the freed budget must be usable again");
    }

    /** An empty / disabled store carries nothing at all. */
    @Test
    public void testForwardedSessionPayloadEmptyWithoutEnabledRows() {
        SessionBaselineStore store = new SessionBaselineStore();
        Assertions.assertEquals("", SPMForwardedSession.serialize(store));
        Assertions.assertEquals("", SPMForwardedSession.serialize(null));
        BaselinePlan plan = new BaselinePlan();
        plan.setBindSql("select 1");
        plan.setPlanSql("select 1");
        plan.setBindSqlDigest("d");
        plan.setBindSqlHash(1);
        plan.setStatus(BaselineStatus.DISABLED);
        store.createBaseline(plan);
        Assertions.assertEquals("", SPMForwardedSession.serialize(store),
                "a disabled row can never match on the master");
    }

    // ==================== helpers ====================

    /** The placeholder id of the first literal of SELECT-list item {@code index}. */
    private static long idOfProjectItem(LogicalPlan plan, int index) {
        List<NamedExpression> items = firstOutputItems(plan);
        Assertions.assertNotNull(items, "the parsed tree must carry an output list");
        return ((SpmConstVar) childOfOutputItem(items.get(index))).getId();
    }

    /** The (parameterized) child of one SELECT-list item, whichever Alias kind carries it. */
    private static Expression childOfOutputItem(NamedExpression item) {
        if (item instanceof UnboundAlias) {
            return ((UnboundAlias) item).child();
        }
        if (item instanceof Alias) {
            return ((Alias) item).child();
        }
        return (Expression) item;
    }

    /**
     * The output list of the first projection-like node (a projection, or the one-row
     * relation of a FROM-less SELECT): the root of a parse is a result-sink wrapper.
     */
    private static List<NamedExpression> firstOutputItems(Plan plan) {
        if (plan instanceof LogicalProject) {
            return ((LogicalProject<?>) plan).getProjects();
        }
        if (plan instanceof LogicalOneRowRelation) {
            return ((LogicalOneRowRelation) plan).getProjects();
        }
        if (plan instanceof UnboundOneRowRelation) {
            return ((UnboundOneRowRelation) plan).getProjects();
        }
        for (Plan child : plan.children()) {
            List<NamedExpression> items = firstOutputItems(child);
            if (items != null) {
                return items;
            }
        }
        return null;
    }

    /** The first project of the tree (the root is wrapped in a result sink). */
    private static LogicalProject<?> firstProject(Plan plan) {
        if (plan instanceof LogicalProject) {
            return (LogicalProject<?>) plan;
        }
        for (Plan child : plan.children()) {
            LogicalProject<?> found = firstProject(child);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    /** The first item of the first projection of the tree. */
    private static NamedExpression firstItemOfFirstProject(Plan plan) {
        List<NamedExpression> items = firstOutputItems(plan);
        Assertions.assertNotNull(items, "the tree must carry an output list");
        return items.get(0);
    }
}
