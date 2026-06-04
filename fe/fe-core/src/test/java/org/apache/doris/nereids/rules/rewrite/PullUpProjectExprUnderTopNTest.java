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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Score;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

class PullUpProjectExprUnderTopNTest implements MemoPatternMatchSupported {
    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);

    @Test
    void testPullUpAddExpression() {
        List<NamedExpression> exprs = ImmutableList.of(
                scan1.getOutput().get(0),
                new Alias(new Add(scan1.getOutput().get(1), new IntegerLiteral((byte) 1)), "b")
        );
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .projectExprs(exprs)
                .topN(3, 0, ImmutableList.of(0))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                .matches(
                        logicalProject(
                                logicalTopN(
                                        logicalProject(
                                                logicalOlapScan()
                                        )
                                )
                        )
                )
                .getPlan();
    }

    @Test
    void testNotPullUpSimpleAlias() {
        List<NamedExpression> exprs = ImmutableList.of(
                scan1.getOutput().get(0).alias("a"),
                scan1.getOutput().get(1)
        );
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .projectExprs(exprs)
                .topN(3, 0, ImmutableList.of(0))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                .matches(
                        logicalTopN(
                                logicalProject(
                                        logicalOlapScan()
                                )
                        )
                )
                .getPlan();
    }

    @Test
    void testNotPullUpScoreExpression() {
        List<NamedExpression> exprs = ImmutableList.of(
                new Alias(new Score(), "score"),
                scan1.getOutput().get(0)
        );
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .projectExprs(exprs)
                .topN(3, 0, ImmutableList.of(1))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                .matchesFromRoot(
                        logicalTopN(
                                logicalProject(
                                        logicalOlapScan()
                                )
                        )
                )
                .getPlan();
    }

    @Test
    void testBlockedByFilterGoesToInnerTopN() {
        // project(y) -> topn1 -> filter(x>1) -> topn2 -> project(x, y) -> scan
        // x should stay below topn1 (blocked by filter and topn2 order key),
        // y should be pulled up past both topn1 and topn2.
        Slot a = scan1.getOutput().get(1);
        Slot b = scan1.getOutput().get(0);

        Alias x = new Alias(new Add(a, new IntegerLiteral((byte) 1)), "x");
        Alias y = new Alias(new Add(b, new IntegerLiteral((byte) 1)), "y");
        GreaterThan filter = new GreaterThan(x.toSlot(), new IntegerLiteral((byte) 1));

        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .projectExprs(ImmutableList.of(x, y))
                .topN(10, 0, ImmutableList.of(0))
                .filter(filter)
                .topN(3, 0, ImmutableList.of(0))
                .projectExprs(ImmutableList.of(y.toSlot()))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                // outer pull-up exists (project above topn1)
                .matches(
                        logicalProject(
                                logicalTopN(
                                        logicalFilter(
                                                logicalProject(
                                                        logicalTopN(
                                                                logicalProject(
                                                                        logicalOlapScan()
                                                                )
                                                        )
                                                )
                                        )
                                )
                        )
                )
                // inner pull-up also exists (project above topn2)
                .matches(
                        logicalProject(
                                logicalTopN(
                                        logicalProject(
                                                logicalOlapScan()
                                        )
                                )
                        )
                )
                .getPlan();
    }

    @Test
    void testDeduplicatePullUpToOutermostTopN() {
        // topn1(order by id) -> filter(x>1) -> topn2(order by id) -> project(id, x, y) -> scan
        // topn2 can pull up both x and y (order key does not reference them).
        // topn1 can only pull up y (x is blocked by filter).
        // After deduplication, y should only be pulled up to topn1 (outermost),
        // and x should only be pulled up to topn2.
        Slot id = scan1.getOutput().get(0);
        Slot a = scan1.getOutput().get(1);
        Slot b = scan1.getOutput().get(0);

        Alias x = new Alias(new Add(a, new IntegerLiteral((byte) 1)), "x");
        Alias y = new Alias(new Add(b, new IntegerLiteral((byte) 1)), "y");
        GreaterThan filter = new GreaterThan(x.toSlot(), new IntegerLiteral((byte) 1));

        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .projectExprs(ImmutableList.of(id, x, y))
                .topN(10, 0, ImmutableList.of(0))
                .filter(filter)
                .topN(3, 0, ImmutableList.of(0))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                // Verify the overall shape: project -> topn1 -> filter -> project -> topn2 -> project -> scan
                .matchesFromRoot(
                        logicalProject(
                                logicalTopN(
                                        logicalFilter(
                                                logicalProject(
                                                        logicalTopN(
                                                                logicalProject(
                                                                        logicalOlapScan()
                                                                )
                                                        )
                                                )
                                        )
                                )
                        )
                )
                // Verify inner topn2 has an upper project (x pulled up)
                .matches(
                        logicalProject(
                                logicalTopN(
                                        logicalProject(
                                                logicalOlapScan()
                                        )
                                )
                        )
                )
                .getPlan();
    }

    @Test
    void testPullUpThroughJoin() {
        // topn -> project(x, y) -> join -> [scan1, scan2]
        // x and y should be pulled up above topn.
        Slot a = scan1.getOutput().get(1);
        Slot b = scan1.getOutput().get(0);
        Alias x = new Alias(new Add(a, new IntegerLiteral((byte) 1)), "x");
        Alias y = new Alias(new Add(b, new IntegerLiteral((byte) 1)), "y");

        LogicalPlan join = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .build();

        LogicalPlan plan = new LogicalPlanBuilder(join)
                .projectExprs(ImmutableList.of(x, y))
                .topN(3, 0, ImmutableList.of(0))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                .matches(
                        logicalProject(
                                logicalTopN(
                                        logicalProject(
                                                logicalJoin(
                                                        logicalOlapScan(),
                                                        logicalOlapScan()
                                                )
                                        )
                                )
                        )
                )
                .getPlan();
    }

    @Test
    void testPullUpThroughForwardedSlotFromLowerProject() {
        // This is the minimal trigger pattern:
        // TopN -> Project(id, x) -> Project(id, a + 1 as x) -> Scan.
        // The lower Project can remove x after pull-up, so the forwarding Project
        // must pass through x's input slot a instead of keeping the unavailable x.
        Slot id = scan1.getOutput().get(0);
        Slot a = scan1.getOutput().get(1);
        Alias x = new Alias(new Add(a, new IntegerLiteral((byte) 1)), "x");

        LogicalProject<LogicalOlapScan> lowerProject = new LogicalProject<>(ImmutableList.of(id, x), scan1);
        LogicalProject<LogicalProject<LogicalOlapScan>> upperProject = new LogicalProject<>(
                ImmutableList.of(id, x.toSlot()), lowerProject);
        LogicalPlan plan = new LogicalPlanBuilder(upperProject)
                .topN(3, 0, ImmutableList.of(0))
                .build();

        LogicalPlan rewritten = (LogicalPlan) PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                .matchesFromRoot(
                        logicalProject(
                                logicalTopN(
                                        logicalProject(
                                                logicalProject(
                                                        logicalOlapScan()
                                                )
                                        )
                                )
                        )
                )
                .getPlan();

        LogicalProject<?> topProject = (LogicalProject<?>) rewritten;
        Assertions.assertEquals(x.getExprId(), topProject.getProjects().get(1).getExprId());

        LogicalTopN<?> topN = (LogicalTopN<?>) topProject.child(0);
        LogicalProject<?> rewrittenForwardingProject = (LogicalProject<?>) topN.child(0);
        Assertions.assertTrue(rewrittenForwardingProject.getProjects().stream()
                .anyMatch(expr -> expr.getExprId().equals(a.getExprId())));
        Assertions.assertFalse(rewrittenForwardingProject.getProjects().stream()
                .anyMatch(expr -> expr.getExprId().equals(x.getExprId())));
    }

    @Test
    void testPullUpThroughProjectSlotAboveJoinProject() {
        Slot id1 = scan1.getOutput().get(0);
        Slot a = scan1.getOutput().get(1);
        Slot id2 = scan2.getOutput().get(0);
        Alias x = new Alias(new Add(a, new IntegerLiteral((byte) 1)), "x");

        LogicalPlan join = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .build();
        LogicalProject<LogicalPlan> lowerProject = new LogicalProject<>(ImmutableList.of(id1, x, id2), join);
        LogicalProject<LogicalProject<LogicalPlan>> upperProject = new LogicalProject<>(
                ImmutableList.of(id1, x.toSlot(), id2), lowerProject);
        LogicalPlan plan = new LogicalPlanBuilder(upperProject)
                .topN(3, 0, ImmutableList.of(0))
                .build();

        LogicalPlan rewritten = (LogicalPlan) PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                .matchesFromRoot(
                        logicalProject(
                                logicalTopN(
                                        logicalProject(
                                                logicalProject(
                                                        logicalJoin(
                                                                logicalOlapScan(),
                                                                logicalOlapScan()
                                                        )
                                                )
                                        )
                                )
                        )
                )
                .getPlan();

        LogicalProject<?> topProject = (LogicalProject<?>) rewritten;
        Assertions.assertEquals(x.getExprId(), topProject.getProjects().get(1).getExprId());

        LogicalTopN<?> topN = (LogicalTopN<?>) topProject.child(0);
        LogicalProject<?> rewrittenUpperProject = (LogicalProject<?>) topN.child(0);
        Assertions.assertTrue(rewrittenUpperProject.getProjects().stream()
                .anyMatch(expr -> expr.getExprId().equals(a.getExprId())));
        Assertions.assertFalse(rewrittenUpperProject.getProjects().stream()
                .anyMatch(expr -> expr.getExprId().equals(x.getExprId())));
    }

    @Test
    void testBlockedByJoinCondition() {
        // topn -> project(id, x, y) -> join on x = scan2.id -> project(id, x, y) -> scan
        // x is referenced by join condition, so it should be blocked.
        Slot id = scan1.getOutput().get(0);
        Slot a = scan1.getOutput().get(1);
        Alias x = new Alias(new Add(a, new IntegerLiteral((byte) 1)), "x");
        Alias y = new Alias(new Add(id, new IntegerLiteral((byte) 1)), "y");

        EqualTo joinCond = new EqualTo(x.toSlot(), scan2.getOutput().get(0));
        LogicalProject<LogicalOlapScan> lowerProject = new LogicalProject<>(ImmutableList.of(id, x, y), scan1);
        LogicalJoin<LogicalPlan, LogicalPlan> join = new LogicalJoin<>(
                JoinType.INNER_JOIN,
                ImmutableList.of(joinCond),
                lowerProject, (LogicalPlan) scan2, null);

        LogicalPlan plan = new LogicalPlanBuilder(join)
                .projectExprs(ImmutableList.of(id, x.toSlot(), y.toSlot()))
                .topN(3, 0, ImmutableList.of(0))
                .build();

        LogicalPlan rewritten = (LogicalPlan) PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                // y is pulled up, x stays in the bottom project because it's blocked by join condition
                .matchesFromRoot(
                        logicalProject(
                                logicalTopN(
                                        logicalProject(
                                                logicalJoin(
                                                        logicalProject(logicalOlapScan()),
                                                        logicalOlapScan()
                                                )
                                        )
                                )
                        )
                )
                .getPlan();

        LogicalProject<?> topProject = (LogicalProject<?>) rewritten;
        Assertions.assertEquals(y.getExprId(), topProject.getProjects().get(2).getExprId());

        LogicalTopN<?> topN = (LogicalTopN<?>) topProject.child(0);
        LogicalProject<?> rewrittenProject = (LogicalProject<?>) topN.child(0);
        Assertions.assertTrue(rewrittenProject.getProjects().stream()
                .anyMatch(expr -> expr.getExprId().equals(x.getExprId())));
        Assertions.assertFalse(rewrittenProject.getProjects().stream()
                .anyMatch(expr -> expr.getExprId().equals(y.getExprId())));
    }

    @Test
    void testPullUpFromChainedProjects() {
        // topn -> project1(y) -> project2(x, y) -> scan
        // Both projects should be visited and both x and y pulled up.
        Slot a = scan1.getOutput().get(1);
        Slot b = scan1.getOutput().get(0);
        Alias x = new Alias(new Add(a, new IntegerLiteral((byte) 1)), "x");
        Alias y = new Alias(new Add(b, new IntegerLiteral((byte) 1)), "y");

        LogicalPlan innerProject = new LogicalProject<>(ImmutableList.of(x, y), scan1);
        LogicalPlan outerProject = new LogicalProject<>(ImmutableList.of(x.toSlot(), y.toSlot()), innerProject);
        LogicalPlan plan = new LogicalTopN<>(
                ImmutableList.of(new OrderKey(scan1.getOutput().get(0), false, false)),
                3, 0, outerProject);

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                .matches(
                        logicalProject(
                                logicalTopN(
                                        logicalProject(
                                                logicalProject(
                                                        logicalOlapScan()
                                                )
                                        )
                                )
                        )
                );
    }

    @Test
    void testBlockedByAggregate() {
        // topn -> project(x, y) -> agg -> scan
        // Aggregate is a boundary node, so nothing below it is pulled up.
        Slot a = scan1.getOutput().get(1);
        Slot b = scan1.getOutput().get(0);
        Alias x = new Alias(new Add(a, new IntegerLiteral((byte) 1)), "x");
        Alias y = new Alias(new Add(b, new IntegerLiteral((byte) 1)), "y");

        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .aggGroupUsingIndex(ImmutableList.of(0), ImmutableList.of(x, y))
                .topN(3, 0, ImmutableList.of(0))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                .matches(
                        logicalTopN(
                                logicalAggregate(
                                        logicalOlapScan()
                                )
                        )
                );
    }

    @Test
    void testBlockedByWindow() {
        // topn -> project(x, y) -> window -> scan
        // Window is a boundary node, so nothing below it is pulled up.
        Slot a = scan1.getOutput().get(1);
        Slot b = scan1.getOutput().get(0);
        Alias x = new Alias(new Add(a, new IntegerLiteral((byte) 1)), "x");
        Alias y = new Alias(new Add(b, new IntegerLiteral((byte) 1)), "y");

        List<NamedExpression> windowExprs = ImmutableList.of(
                new Alias(new WindowExpression(
                        new org.apache.doris.nereids.trees.expressions.functions.window.RowNumber(),
                        ImmutableList.of(),
                        ImmutableList.of(new OrderExpression(
                                new OrderKey(scan1.getOutput().get(0), false, false)))
                ), "rn")
        );
        LogicalWindow<LogicalOlapScan> window = new LogicalWindow<>(windowExprs, scan1);
        LogicalPlan plan = new LogicalPlanBuilder(window)
                .projectExprs(ImmutableList.of(x, y))
                .topN(3, 0, ImmutableList.of(0))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                .matches(
                        logicalTopN(
                                logicalProject(
                                        logicalWindow(
                                                logicalOlapScan()
                                        )
                                )
                        )
                );
    }

    @Test
    void testPullUpMultipleExpressions() {
        // topn -> project(x, y) -> scan
        // Both x and y should be pulled up.
        Slot a = scan1.getOutput().get(1);
        Slot b = scan1.getOutput().get(0);
        Alias x = new Alias(new Add(a, new IntegerLiteral((byte) 1)), "x");
        Alias y = new Alias(new Add(b, new IntegerLiteral((byte) 1)), "y");

        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .projectExprs(ImmutableList.of(x, y))
                .topN(3, 0, ImmutableList.of(0))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                .matches(
                        logicalProject(
                                logicalTopN(
                                        logicalProject(
                                                logicalOlapScan()
                                        )
                                )
                        )
                );
    }

    @Test
    void testPullUpDoesNotExposeInternalPassThroughSlotInUpperProject() {
        // This checks the schema boundary restored by the upper Project above TopN.
        // The same invariant is required even when this Project is not the query root:
        //
        // Project(y = x + 1, id)
        //   TopN(order by id)
        //     Project(x = a + 1, b, id)
        //       Scan(a, b, id)
        //
        // After pulling up x = a + 1, TopN still needs to carry a internally, but the
        // upper Project output must stay as the original TopN output [x, b, id] rather
        // than leaking internal pass-through slot a as [x, a, b, id].
        LogicalOlapScan scan = new LogicalOlapScan(
                PlanConstructor.getNextRelationId(), PlanConstructor.student, ImmutableList.of("db"));
        Slot id = scan.getOutput().get(0);
        Slot a = scan.getOutput().get(1);
        Slot b = scan.getOutput().get(3);
        Alias x = new Alias(new Add(a, new IntegerLiteral((byte) 1)), "x");

        LogicalPlan plan = new LogicalPlanBuilder(scan)
                .projectExprs(ImmutableList.of(x, b, id))
                .topN(10, 0, ImmutableList.of(2))
                .build();

        LogicalPlan rewritten = (LogicalPlan) PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                .getPlan();

        LogicalProject<?> upperProject = (LogicalProject<?>) rewritten;
        Assertions.assertEquals(3, upperProject.getProjects().size());
        Assertions.assertEquals(x.getExprId(), upperProject.getProjects().get(0).getExprId());
        Assertions.assertEquals(b.getExprId(), upperProject.getProjects().get(1).getExprId());
        Assertions.assertEquals(id.getExprId(), upperProject.getProjects().get(2).getExprId());
        Assertions.assertFalse(upperProject.getProjects().stream()
                .anyMatch(expr -> expr.getExprId().equals(a.getExprId())));

        LogicalTopN<?> topN = (LogicalTopN<?>) upperProject.child(0);
        Assertions.assertTrue(topN.getOutput().stream()
                .anyMatch(slot -> slot.getExprId().equals(a.getExprId())));
    }

    @Test
    void testRestoreNonPulledSlotsByExprIdAfterPullUp() {
        LogicalOlapScan scan = new LogicalOlapScan(
                PlanConstructor.getNextRelationId(), PlanConstructor.student, ImmutableList.of("db"));
        Slot id = scan.getOutput().get(0);
        Slot a = scan.getOutput().get(1);
        Slot c = scan.getOutput().get(3);
        Alias x = new Alias(new Add(a, new IntegerLiteral((byte) 1)), "x");

        LogicalPlan plan = new LogicalPlanBuilder(scan)
                .projectExprs(ImmutableList.of(id, x, c))
                .topN(3, 0, ImmutableList.of(0))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                .matchesFromRoot(
                        logicalProject(
                                logicalTopN(
                                        logicalProject(
                                                logicalOlapScan()
                                        )
                                )
                        ).when(project -> project.getProjects().size() == 3
                                && project.getProjects().get(0).getExprId().equals(id.getExprId())
                                && project.getProjects().get(1).getExprId().equals(x.getExprId())
                                && project.getProjects().get(2).getExprId().equals(c.getExprId()))
                );
    }

    @Test
    void testDeduplicatedPullUpDoesNotExposePassThroughInputSlots() {
        LogicalOlapScan scan = new LogicalOlapScan(
                PlanConstructor.getNextRelationId(), PlanConstructor.student, ImmutableList.of("db"));
        Slot id = scan.getOutput().get(0);
        Slot a = scan.getOutput().get(1);
        Slot b = scan.getOutput().get(3);
        Alias x = new Alias(new Add(a, new IntegerLiteral((byte) 1)), "x");
        Alias y = new Alias(new Add(b, new IntegerLiteral((byte) 1)), "y");
        GreaterThan filter = new GreaterThan(x.toSlot(), new IntegerLiteral((byte) 1));

        LogicalPlan plan = new LogicalPlanBuilder(scan)
                .projectExprs(ImmutableList.of(x, y, id))
                .topN(10, 0, ImmutableList.of(2))
                .filter(filter)
                .topN(3, 0, ImmutableList.of(2))
                .build();

        LogicalPlan rewritten = (LogicalPlan) PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                .getPlan();

        LogicalProject<?> topProject = (LogicalProject<?>) rewritten;
        Assertions.assertEquals(3, topProject.getProjects().size());
        Assertions.assertEquals(x.getExprId(), topProject.getProjects().get(0).getExprId());
        Assertions.assertEquals(y.getExprId(), topProject.getProjects().get(1).getExprId());
        Assertions.assertEquals(id.getExprId(), topProject.getProjects().get(2).getExprId());

        LogicalTopN<?> topN = (LogicalTopN<?>) topProject.child(0);
        Assertions.assertTrue(topN.getOutput().stream()
                .anyMatch(slot -> slot.getExprId().equals(b.getExprId())));
    }

    @Test
    void testDeduplicatedPullUpPassesThroughTransitiveInputSlots() {
        LogicalOlapScan scan = new LogicalOlapScan(
                PlanConstructor.getNextRelationId(), PlanConstructor.student, ImmutableList.of("db"));
        Slot id = scan.getOutput().get(0);
        Slot a = scan.getOutput().get(1);
        Slot b = scan.getOutput().get(3);
        Alias x = new Alias(new Add(a, b), "x");
        Alias y = new Alias(new Add(x.toSlot(), new IntegerLiteral((byte) 1)), "y");

        LogicalPlan plan = new LogicalPlanBuilder(scan)
                .projectExprs(ImmutableList.of(x, id))
                .topN(30, 0, ImmutableList.of(1))
                .projectExprs(ImmutableList.of(y, id))
                .topN(20, 0, ImmutableList.of(1))
                .topN(10, 0, ImmutableList.of(1))
                .build();

        LogicalPlan rewritten = (LogicalPlan) PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                .getPlan();

        LogicalProject<?> topN1UpperProject = (LogicalProject<?>) rewritten;
        Assertions.assertEquals(y.getExprId(), topN1UpperProject.getProjects().get(0).getExprId());
        Set<ExprId> topN1InputExprIds = topN1UpperProject.getProjects().get(0).child(0).getInputSlotExprIds();
        Assertions.assertTrue(topN1InputExprIds.contains(a.getExprId()));
        Assertions.assertTrue(topN1InputExprIds.contains(b.getExprId()));
        Assertions.assertFalse(topN1InputExprIds.contains(x.getExprId()));

        LogicalTopN<?> topN1 = (LogicalTopN<?>) topN1UpperProject.child(0);
        LogicalProject<?> topN2UpperProject = (LogicalProject<?>) topN1.child(0);
        Assertions.assertEquals(3, topN2UpperProject.getProjects().size());
        Assertions.assertEquals(a.getExprId(), topN2UpperProject.getProjects().get(0).getExprId());
        Assertions.assertEquals(b.getExprId(), topN2UpperProject.getProjects().get(1).getExprId());
        Assertions.assertEquals(id.getExprId(), topN2UpperProject.getProjects().get(2).getExprId());

        LogicalTopN<?> topN2 = (LogicalTopN<?>) topN2UpperProject.child(0);
        LogicalProject<?> yProject = (LogicalProject<?>) topN2.child(0);
        Assertions.assertEquals(3, yProject.getProjects().size());
        Assertions.assertEquals(id.getExprId(), yProject.getProjects().get(0).getExprId());
        Assertions.assertEquals(a.getExprId(), yProject.getProjects().get(1).getExprId());
        Assertions.assertEquals(b.getExprId(), yProject.getProjects().get(2).getExprId());
    }

    @Test
    void testDeduplicatedPullUpKeepsInputSlotRestoredByLowerTopN() {
        LogicalOlapScan scan = new LogicalOlapScan(
                PlanConstructor.getNextRelationId(), PlanConstructor.student, ImmutableList.of("db"));
        Slot id = scan.getOutput().get(0);
        Slot a = scan.getOutput().get(1);
        Slot b = scan.getOutput().get(3);
        Alias x = new Alias(new Add(a, b), "x");
        Alias y = new Alias(new Add(x.toSlot(), new IntegerLiteral((byte) 1)), "y");

        LogicalPlan plan = new LogicalPlanBuilder(scan)
                .projectExprs(ImmutableList.of(x, id))
                .topN(30, 0, ImmutableList.of(1))
                .projectExprs(ImmutableList.of(y, id, x.toSlot()))
                .topN(20, 0, ImmutableList.of(2))
                .topN(10, 0, ImmutableList.of(1))
                .build();

        LogicalPlan rewritten = (LogicalPlan) PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                .getPlan();

        LogicalProject<?> topN1UpperProject = (LogicalProject<?>) rewritten;
        Assertions.assertEquals(y.getExprId(), topN1UpperProject.getProjects().get(0).getExprId());
        Set<ExprId> topN1InputExprIds = topN1UpperProject.getProjects().get(0).child(0).getInputSlotExprIds();
        Assertions.assertTrue(topN1InputExprIds.contains(x.getExprId()));
        Assertions.assertFalse(topN1InputExprIds.contains(a.getExprId()));
        Assertions.assertFalse(topN1InputExprIds.contains(b.getExprId()));

        LogicalTopN<?> topN1 = (LogicalTopN<?>) topN1UpperProject.child(0);
        LogicalProject<?> topN2UpperProject = (LogicalProject<?>) topN1.child(0);
        Assertions.assertEquals(2, topN2UpperProject.getProjects().size());
        Assertions.assertEquals(x.getExprId(), topN2UpperProject.getProjects().get(0).getExprId());
        Assertions.assertEquals(id.getExprId(), topN2UpperProject.getProjects().get(1).getExprId());

        LogicalTopN<?> topN2 = (LogicalTopN<?>) topN2UpperProject.child(0);
        LogicalProject<?> yProject = (LogicalProject<?>) topN2.child(0);
        Assertions.assertEquals(2, yProject.getProjects().size());
        Assertions.assertEquals(id.getExprId(), yProject.getProjects().get(0).getExprId());
        Assertions.assertEquals(x.getExprId(), yProject.getProjects().get(1).getExprId());
    }

    @Test
    void testNotPullUpNoneMovableFunction() {
        // topn -> project(assert_true(a+1, "msg") as x) -> scan
        // NoneMovableFunction should not be pulled up.
        Slot a = scan1.getOutput().get(1);
        Alias x = new Alias(
                new AssertTrue(
                        new GreaterThan(new Add(a, new IntegerLiteral((byte) 1)), new IntegerLiteral((byte) 0)),
                        new StringLiteral("msg")
                ),
                "x"
        );

        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .projectExprs(ImmutableList.of(x))
                .topN(3, 0, ImmutableList.of(0))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                .matches(
                        logicalTopN(
                                logicalProject(
                                        logicalOlapScan()
                                )
                        )
                );
    }

    @Test
    void testBlockedBySort() {
        // topn -> project(id, x, y) -> sort(by x) -> project(id, x, y) -> scan
        // x is used by sort order key, so x is blocked.
        // y is not blocked and should be pulled up.
        Slot id = scan1.getOutput().get(0);
        Slot a = scan1.getOutput().get(1);
        Alias x = new Alias(new Add(a, new IntegerLiteral((byte) 1)), "x");
        Alias y = new Alias(new Add(id, new IntegerLiteral((byte) 1)), "y");

        LogicalProject<LogicalOlapScan> lowerProject = new LogicalProject<>(ImmutableList.of(id, x, y), scan1);
        LogicalSort<LogicalPlan> sort = new LogicalSort<>(
                ImmutableList.of(new OrderKey(x.toSlot(), false, false)),
                lowerProject);
        LogicalPlan plan = new LogicalPlanBuilder(sort)
                .projectExprs(ImmutableList.of(id, x.toSlot(), y.toSlot()))
                .topN(3, 0, ImmutableList.of(0))
                .build();

        LogicalPlan rewritten = (LogicalPlan) PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                // y is pulled up, x stays in bottom project due to sort order key
                .matchesFromRoot(
                        logicalProject(
                                logicalTopN(
                                        logicalProject(
                                                logicalSort(
                                                        logicalProject(logicalOlapScan())
                                                )
                                        )
                                )
                        )
                )
                .getPlan();

        LogicalProject<?> topProject = (LogicalProject<?>) rewritten;
        Assertions.assertEquals(y.getExprId(), topProject.getProjects().get(2).getExprId());

        LogicalTopN<?> topN = (LogicalTopN<?>) topProject.child(0);
        LogicalProject<?> rewrittenProject = (LogicalProject<?>) topN.child(0);
        Assertions.assertTrue(rewrittenProject.getProjects().stream()
                .anyMatch(expr -> expr.getExprId().equals(x.getExprId())));
        Assertions.assertFalse(rewrittenProject.getProjects().stream()
                .anyMatch(expr -> expr.getExprId().equals(y.getExprId())));
    }

    @Test
    void testSetOperationIsBoundary() {
        // topn -> union all -> [project(a+1 as x, a+1 as y) -> scan1,
        //                       project(a+1 as x, a+1 as y) -> scan2]
        // Set operations are a boundary: expressions below them are NOT
        // collected for the current TopN because UNION ALL children may
        // compute the same output column differently.
        Slot a = scan1.getOutput().get(1);
        Slot b = scan1.getOutput().get(0);
        Alias x = new Alias(new Add(a, new IntegerLiteral((byte) 1)), "x");
        Alias y = new Alias(new Add(b, new IntegerLiteral((byte) 1)), "y");

        LogicalProject<LogicalPlan> project1 = new LogicalProject<>(ImmutableList.of(x, y), scan1);
        LogicalProject<LogicalPlan> project2 = new LogicalProject<>(ImmutableList.of(x, y), scan2);

        List<NamedExpression> outputs = ImmutableList.of(x, y);
        List<List<org.apache.doris.nereids.trees.expressions.SlotReference>> regularChildrenOutputs
                = ImmutableList.of(
                        ImmutableList.of((org.apache.doris.nereids.trees.expressions.SlotReference) x.toSlot(),
                                (org.apache.doris.nereids.trees.expressions.SlotReference) y.toSlot()),
                        ImmutableList.of((org.apache.doris.nereids.trees.expressions.SlotReference) x.toSlot(),
                                (org.apache.doris.nereids.trees.expressions.SlotReference) y.toSlot())
                );
        LogicalUnion union = new LogicalUnion(
                Qualifier.ALL, outputs, regularChildrenOutputs,
                ImmutableList.of(), false,
                ImmutableList.of(project1, project2));

        LogicalPlan plan = new LogicalTopN<>(
                ImmutableList.of(new OrderKey(scan1.getOutput().get(0), false, false)),
                3, 0, union);

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                // Set operation is a boundary: no pull-up, plan unchanged
                .matchesFromRoot(
                        logicalTopN(
                                logicalUnion(
                                        logicalProject(
                                                logicalOlapScan()
                                        ),
                                        logicalProject(
                                                logicalOlapScan()
                                        )
                                )
                        )
                );
    }

    @Test
    void testUnionAllBoundaryWithDifferentChildExpressions() {
        // Reproduce the exact scenario from the review:
        //   SELECT x, id FROM (
        //     SELECT a + 1 AS x, id FROM t1
        //     UNION ALL
        //     SELECT a + 2 AS x, id FROM t2
        //   ) u ORDER BY id LIMIT 10
        //
        // UNION ALL children compute x differently (a+1 vs a+2).
        // A single pull-up Project above TopN cannot represent both
        // branch-specific expressions, so the union must be a boundary.
        Slot id1 = scan1.getOutput().get(0);
        Slot a1 = scan1.getOutput().get(1);
        Slot id2 = scan2.getOutput().get(0);
        Slot a2 = scan2.getOutput().get(1);

        Alias x1 = new Alias(new Add(a1, new IntegerLiteral((byte) 1)), "x");
        Alias x2 = new Alias(new Add(a2, new IntegerLiteral((byte) 2)), "x");
        Alias y1 = new Alias(new Add(id1, new IntegerLiteral((byte) 1)), "y");
        Alias y2 = new Alias(new Add(id2, new IntegerLiteral((byte) 1)), "y");

        LogicalProject<LogicalPlan> project1 = new LogicalProject<>(ImmutableList.of(x1, y1), scan1);
        LogicalProject<LogicalPlan> project2 = new LogicalProject<>(ImmutableList.of(x2, y2), scan2);

        // Union outputs use x1, y1 as the representative output schema
        List<NamedExpression> outputs = ImmutableList.of(x1, y1);
        List<List<SlotReference>> regularChildrenOutputs = ImmutableList.of(
                ImmutableList.of((SlotReference) x1.toSlot(), (SlotReference) y1.toSlot()),
                ImmutableList.of((SlotReference) x2.toSlot(), (SlotReference) y2.toSlot())
        );
        LogicalUnion union = new LogicalUnion(
                Qualifier.ALL, outputs, regularChildrenOutputs,
                ImmutableList.of(), false,
                ImmutableList.of(project1, project2));

        LogicalPlan plan = new LogicalTopN<>(
                ImmutableList.of(new OrderKey(id1, false, false)),
                10, 0, union);

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                // Union is a boundary even though x, y are not order keys:
                // plan should be unchanged — no project above topn
                .matchesFromRoot(
                        logicalTopN(
                                logicalUnion(
                                        logicalProject(logicalOlapScan()),
                                        logicalProject(logicalOlapScan())
                                )
                        )
                );
    }

    @Test
    void testNestedTopNInsideUnionAllIsHandledIndependently() {
        // topn(outer, order by id limit 10) -> union all
        //   -> topn(inner, order by id limit 3) -> project(a+1 as z, id) -> scan1
        //   -> project(a+2 as x, id) -> scan2
        //
        // The outer TopN stops at the union boundary — it does NOT pull up
        // expressions from inside union children.
        // The inner TopN independently pulls up z above itself.
        Slot id1 = scan1.getOutput().get(0);
        Slot a1 = scan1.getOutput().get(1);
        Slot id2 = scan2.getOutput().get(0);
        Slot a2 = scan2.getOutput().get(1);

        // Below inner TopN: project(a1+1 as z, id1)
        Alias z = new Alias(new Add(a1, new IntegerLiteral((byte) 1)), "z");
        LogicalProject<LogicalOlapScan> projectBelow
                = new LogicalProject<>(ImmutableList.of(z, id1), scan1);

        // Inner TopN: order by id1 limit 3
        LogicalTopN<LogicalProject<LogicalOlapScan>> innerTopN = new LogicalTopN<>(
                ImmutableList.of(new OrderKey(id1, false, false)),
                3, 0, projectBelow);

        // child1: project(a2+2 as x, id2)
        Alias x = new Alias(new Add(a2, new IntegerLiteral((byte) 2)), "x");
        LogicalProject<LogicalOlapScan> project2
                = new LogicalProject<>(ImmutableList.of(x, id2), scan2);

        // Union outputs: use z and id1 as representative output schema
        List<NamedExpression> outputs = ImmutableList.of(z, id1);
        List<List<SlotReference>> regularChildrenOutputs = ImmutableList.of(
                ImmutableList.of((SlotReference) z.toSlot(), (SlotReference) id1),
                ImmutableList.of((SlotReference) x.toSlot(), (SlotReference) id2)
        );
        LogicalUnion union = new LogicalUnion(
                Qualifier.ALL, outputs, regularChildrenOutputs,
                ImmutableList.of(), false,
                ImmutableList.of(innerTopN, project2));

        // Outer TopN: order by id1 limit 10
        LogicalTopN<LogicalUnion> outerTopN = new LogicalTopN<>(
                ImmutableList.of(new OrderKey(id1, false, false)),
                10, 0, union);

        PlanChecker.from(MemoTestUtils.createConnectContext(), outerTopN)
                .applyCustom(new PullUpProjectExprUnderTopN())
                // Outer TopN: no pull-up (union is boundary)
                .matchesFromRoot(
                        logicalTopN(
                                logicalUnion(
                                        logicalProject(logicalTopN(logicalProject(logicalOlapScan()))),
                                        logicalProject(logicalOlapScan())
                                )
                        )
                )
                // Inner TopN: independently pulls up z above itself
                .matches(
                        logicalProject(
                                logicalTopN(
                                        logicalProject(logicalOlapScan())
                                )
                        )
                );
    }

    @Test
    void testDeduplicatePullUpEffect() {
        // Verifies that deduplicatePullUps() correctly removes y from the
        // inner TopN's PullUpInfo, so y appears ONLY in the outer TopN's
        // upper project.
        //
        // Plan: topn1(order by id, limit 10) -> filter(x>1)
        //         -> topn2(order by id, limit 3) -> project(id, x, y) -> scan
        //
        // Without dedup: y would be pulled up to BOTH topn1 and topn2.
        // With dedup:    y is only above topn1, x is only above topn2.
        Slot id = scan1.getOutput().get(0);
        Slot a = scan1.getOutput().get(1);
        Slot b = scan1.getOutput().get(0); // b == id, so y = id + 1

        Alias x = new Alias(new Add(a, new IntegerLiteral((byte) 1)), "x");
        Alias y = new Alias(new Add(b, new IntegerLiteral((byte) 1)), "y");
        GreaterThan filter = new GreaterThan(x.toSlot(), new IntegerLiteral((byte) 1));

        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .projectExprs(ImmutableList.of(id, x, y))
                .topN(10, 0, ImmutableList.of(0))
                .filter(filter)
                .topN(3, 0, ImmutableList.of(0))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                // Root shape: project -> topn1 -> filter -> project -> topn2 -> project -> scan
                .matchesFromRoot(
                        logicalProject(
                                logicalTopN(
                                        logicalFilter(
                                                logicalProject(
                                                        logicalTopN(
                                                                logicalProject(
                                                                        logicalOlapScan()
                                                                )
                                                        )
                                                )
                                        )
                                )
                        )
                )
                // Inner project (above topn2): must contain "x"
                .matches(
                        logicalProject(
                                logicalTopN(
                                        logicalProject(logicalOlapScan())
                                )
                        ).when(proj -> proj.getProjects().stream()
                                .anyMatch(e -> "x".equals(e.getName())))
                )
                // Inner project (above topn2): must NOT contain "y"
                // (this is the core dedup verification)
                .nonMatch(
                        logicalProject(
                                logicalTopN(
                                        logicalProject(logicalOlapScan())
                                )
                        ).when(proj -> proj.getProjects().stream()
                                .anyMatch(e -> "y".equals(e.getName())))
                )
                // Outer project (above topn1): must contain "y"
                .matches(
                        logicalProject(
                                logicalTopN(
                                        logicalFilter(
                                                logicalProject(
                                                        logicalTopN(
                                                                logicalProject(
                                                                        logicalOlapScan()
                                                                )
                                                        )
                                                )
                                        )
                                )
                        ).when(proj -> proj.getProjects().stream()
                                .anyMatch(e -> "y".equals(e.getName())))
                );
    }

    /**
     * Regression test for correlated scalar subquery + LEFT OUTER JOIN + nested TopN.
     * When a LIMIT is pushed down to the left side of a LEFT JOIN, a nested TopN is
     * created. The inner TopN's collector must inherit the outer blocked slots
     * (from join conditions) so that pass-through aggregate slots like AVG are not
     * incorrectly removed during project simplification.
     */
    @Test
    void testCorrelatedSubqueryWithNestedTopN() {
        // Simulate: OuterTopN → Project[C1] → LeftOuterJoin → [
        //            InnerTopN → Project[AVG, elem_at, C1] → Join → [Project[elem_at] → Scan1, Scan3],
        //            Scan2]
        // (Simplified: use Scan3 instead of Aggregate to avoid memo duplication)
        Slot id1 = scan1.getOutput().get(0);
        Slot col1 = scan1.getOutput().get(1);
        // x = pull-up eligible expression (simulates element_at(var, 'col'))
        Alias x = new Alias(new Add(col1, new IntegerLiteral((byte) 1)), "x");

        LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);
        Slot avgSlot = scan3.getOutput().get(0);  // simulates AVG result from correlated subquery

        // Inner left: Project[elem_at=x, C1=id1] → Scan1
        LogicalPlan innerLeft = new LogicalPlanBuilder(scan1)
                .projectExprs(ImmutableList.of(x, id1))
                .build();

        // Inner Join (simulates Apply decomposition): [Project] JOIN Scan3
        LogicalPlan innerJoin = new LogicalPlanBuilder(innerLeft)
                .join(scan3, JoinType.INNER_JOIN, Pair.of(1, 0))
                .build();

        // Project[AVG=avgSlot, elem_at=x, C1=id1] above inner join, then InnerTopN
        LogicalPlan innerTopN = new LogicalPlanBuilder(innerJoin)
                .projectExprs(ImmutableList.of(avgSlot, x, id1))
                .topN(1, 0, ImmutableList.of(2))
                .build();

        // Outer: LeftOuterJoin between [InnerTopN side] and [Scan2]
        LogicalPlan outerJoin = new LogicalPlanBuilder(innerTopN)
                .join(scan2, JoinType.LEFT_OUTER_JOIN, Pair.of(2, 0))
                .build();

        // OuterTopN → Project[C1]
        LogicalPlan plan = new LogicalPlanBuilder(outerJoin)
                .projectExprs(ImmutableList.of(id1.alias("C1")))
                .topN(1, 0, ImmutableList.of(0))
                .build();

        // Must not crash with "Original slot ... should be restored or passed through"
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                .getPlan();
    }

    /**
     * Expressions on the nullable side of an outer join should NOT be pulled up.
     * The nullable side is protected by join null-extension: unmatched rows get
     * NULL for all nullable-side columns. Pulling an expression above the join
     * would break this — e.g. ifnull(r.b, 0) would turn NULLs into 0s.
     *
     * Plan: TopN → Project[l.id, x] → LEFT JOIN → [Scan(l), Project[x = r.b+1] → Scan(r)]
     * x is on the nullable (right) side → must stay below TopN.
     */
    @Test
    void testBlockedByNullableSideOfOuterJoin() {
        LogicalOlapScan scanL = PlanConstructor.newLogicalOlapScan(0, "l", 0);
        LogicalOlapScan scanR = PlanConstructor.newLogicalOlapScan(1, "r", 0);
        Slot lId = scanL.getOutput().get(0);
        Slot rId = scanR.getOutput().get(0);
        Slot rB = scanR.getOutput().get(1);
        Alias x = new Alias(new Add(rB, new IntegerLiteral((byte) 1)), "x");

        // Right side Project [x = r.b+1, r.id] → Scan(r)
        LogicalPlan rightSide = new LogicalPlanBuilder(scanR)
                .projectExprs(ImmutableList.of(x, rId))
                .build();

        // LEFT JOIN between Scan(l) and the right-side Project
        LogicalPlan join = new LogicalPlanBuilder(scanL)
                .join(rightSide, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 1))
                .build();

        // Project [l.id, x] above the join, then TopN
        LogicalPlan plan = new LogicalPlanBuilder(join)
                .projectExprs(ImmutableList.of(lId, x.toSlot()))
                .topN(3, 0, ImmutableList.of(0))
                .build();

        // x is on the nullable side — must NOT be pulled up above TopN
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                .matches(
                        logicalTopN(
                                logicalProject(
                                        logicalJoin()
                                )
                        )
                )
                .nonMatch(
                        logicalProject(
                                logicalTopN()
                        ).when(proj -> proj.getProjects().stream()
                                .anyMatch(e -> "x".equals(e.getName())))
                );
    }
}
