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
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Abs;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Score;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
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
        // With the stop-at-inner-TopN change, outer TopN no longer collects expressions
        // from under the inner TopN. topn2 handles its own subtree: pulls up both x and y.
        // topn1 has no pullable expressions between itself and topn2 (Filter is not a Project).
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
                // Root: topn(3) -> filter -> project -> topn(10) -> project -> scan
                // No addUpperProject for topn(3) — no pullable expressions between it and topn(10)
                .matchesFromRoot(
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
                // Inner topn(10) has an upper project with x and y pulled up
                .matches(
                        logicalProject(
                                logicalTopN(
                                        logicalProject(
                                                logicalOlapScan()
                                        )
                                )
                        ).when(proj -> proj.getProjects().stream()
                                .anyMatch(e -> "x".equals(e.getName())))
                )
                .matches(
                        logicalProject(
                                logicalTopN(
                                        logicalProject(
                                                logicalOlapScan()
                                        )
                                )
                        ).when(proj -> proj.getProjects().stream()
                                .anyMatch(e -> "y".equals(e.getName())))
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
        // topn(3) -> filter(x>1) -> topn(10) -> project(x=a+1, y=b+1, id) -> scan
        // With stop-at-inner-TopN, topn(10) handles its own subtree:
        // pulls up x and y, restores them above itself.
        // topn(3) has no pullable expressions → no addUpperProject.
        // Root is topn(3), not a Project.
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

        // Root is topn(3) — no addUpperProject (no pullable expressions)
        LogicalTopN<?> rootTopN = (LogicalTopN<?>) rewritten;
        LogicalFilter<?> midFilter = (LogicalFilter<?>) rootTopN.child(0);
        LogicalProject<?> topN10UpperProject = (LogicalProject<?>) midFilter.child(0);
        Assertions.assertEquals(3, topN10UpperProject.getProjects().size());
        Assertions.assertEquals(x.getExprId(), topN10UpperProject.getProjects().get(0).getExprId());
        Assertions.assertEquals(y.getExprId(), topN10UpperProject.getProjects().get(1).getExprId());
        Assertions.assertEquals(id.getExprId(), topN10UpperProject.getProjects().get(2).getExprId());

        LogicalTopN<?> topN10 = (LogicalTopN<?>) topN10UpperProject.child(0);
        // topN(10)'s output is [x, y, id]; base slot b is inside x and y expressions
        Assertions.assertTrue(topN10.getOutput().stream()
                .anyMatch(slot -> slot.getExprId().equals(b.getExprId())));
    }

    @Test
    void testDeduplicatedPullUpPassesThroughTransitiveInputSlots() {
        // topn(10) -> topn(20) -> project(y, id) -> topn(30) -> project(x, id) -> scan
        // Each TopN handles its own subtree independently.
        // topn(30): pulls up x from project(x, id), restores above itself
        // topn(20): has project(y=x+1, id) between it and topn(30), y is pullable,
        //           restores y above itself
        // topn(10): no pullable expressions → no addUpperProject
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

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                // Root: topn(10) → project(y, id) → topn(20) → project(x, id) → project(x) → topn(30) → project → scan
                .matchesFromRoot(
                        logicalTopN(
                                logicalProject(
                                        logicalTopN(
                                                logicalProject(
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
                )
                // topn(20)'s upper project contains y
                .matches(
                        logicalProject(
                                logicalTopN(
                                        logicalProject(
                                                logicalProject(
                                                        logicalTopN(
                                                                logicalProject(logicalOlapScan())
                                                        )
                                                )
                                        )
                                )
                        ).when(proj -> proj.getProjects().stream()
                                .anyMatch(e -> "y".equals(e.getName())))
                )
                // topn(30)'s upper project contains x
                .matches(
                        logicalProject(
                                logicalTopN(
                                        logicalProject(logicalOlapScan())
                                )
                        ).when(proj -> proj.getProjects().stream()
                                .anyMatch(e -> "x".equals(e.getName())))
                );
    }

    @Test
    void testDeduplicatedPullUpKeepsInputSlotRestoredByLowerTopN() {
        // topn(10) -> topn(20) -> project(y, id, x) -> topn(30) -> project(x, id) -> scan
        // Each TopN handles its own subtree independently.
        // topn(30): pulls up x from project(x, id), restores above itself
        // topn(20): has project(y, id, x) between it and topn(30), but x is a Slot (no pullup),
        //           y=x+1 is pullable, restores above itself
        // topn(10): no pullable expressions → no addUpperProject
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

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                // Root: topn(10) — no addUpperProject (no pullable expressions)
                .matchesFromRoot(logicalTopN().when(t -> t.getLimit() == 10))
                // topn(20)'s upper project contains y
                .matches(
                        logicalProject(
                                logicalTopN(logicalProject())
                        ).when(proj -> proj.getProjects().stream()
                                .anyMatch(e -> "y".equals(e.getName())))
                )
                // topn(30)'s upper project contains x
                .matches(
                        logicalProject(
                                logicalTopN(logicalProject(logicalOlapScan()))
                        ).when(proj -> proj.getProjects().stream()
                                .anyMatch(e -> "x".equals(e.getName())))
                );
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
        // Each TopN independently handles its own subtree — no cross-TopN dedup.
        // topn(10) pulls up both x and y from the Project below it.
        // topn(3) has no pullable expressions (stops at topn(10) boundary,
        // Filter is not a Project). No addUpperProject for topn(3).
        //
        // Plan: topn(3) -> filter(x>1) -> topn(10) -> project(id, x, y) -> scan
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
                // Root shape: topn(3) -> filter -> project -> topn(10) -> project -> scan
                .matchesFromRoot(
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
                // Inner project (above topn(10)): must contain both x and y
                .matches(
                        logicalProject(
                                logicalTopN(
                                        logicalProject(logicalOlapScan())
                                )
                        ).when(proj -> proj.getProjects().stream()
                                .anyMatch(e -> "x".equals(e.getName()))
                                && proj.getProjects().stream()
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

    /**
     * Simulates e.sql: JOIN with chained element_at-like expressions below TopN.
     * The lower projects compute non-trivial expressions (v1 from a, v3 from b),
     * an intermediate project forwards them as Slots, and the TopN has both a
     * pass-through expression and a blocked ORDER BY expression.
     *
     * Before pullup:
     *   TopN(order by id_a, v2)
     *     Project(v1#slot, (v1+1) as v2, v3#slot, ...)   ← wraps intermediate Slots
     *       Join
     *         Project(id_a, v1, v3')            ← computes v1=a+1, v3=b+1
     *           Scan(ta: id_a, a, b)
     *
     * After pullup:
     *   Project(v1#slot, v2#slot, v3=..., ...)  ← upper: only v3 is pulled up
     *     TopN
     *       Project(v1#slot, v2, b, id_a)       ← v1 kept for blocked v2
     *         Join (simplified children)
     */
    @Test
    void testPullUpChainedExpressionsThroughJoinAndTopN() {
        LogicalOlapScan scanTa = PlanConstructor.newLogicalOlapScan(0, "ta", 0);
        LogicalOlapScan scanTb = PlanConstructor.newLogicalOlapScan(1, "tb", 0);
        Slot idA = scanTa.getOutput().get(0);
        Slot a = scanTa.getOutput().get(1);     // simulates sa (base struct)
        Slot idB = scanTb.getOutput().get(0);
        Slot c = scanTb.getOutput().get(1);     // simulates sb (base struct)

        // v1 = a + 1 (simulates element_at(sa, 'fa'))
        Alias v1 = new Alias(new Add(a, new IntegerLiteral((byte) 1)), "v1");
        // v3 = c + 1 (simulates element_at(sb, 'fc'))
        Alias v3 = new Alias(new Add(c, new IntegerLiteral((byte) 1)), "v3");
        // v2 = v1 + 1 (COALESCE-like, depends on v1, in ORDER BY)
        Alias v2 = new Alias(new Add(v1.toSlot(), new IntegerLiteral((byte) 1)), "v2");

        // Lower project on ta side: computes v1
        LogicalProject<LogicalOlapScan> taProj
                = new LogicalProject<>(ImmutableList.of(idA, v1), scanTa);
        // Lower project on tb side: computes v3
        LogicalProject<LogicalOlapScan> tbProj
                = new LogicalProject<>(ImmutableList.of(idB, v3), scanTb);

        // Join
        LogicalJoin<LogicalPlan, LogicalPlan> join = new LogicalJoin<>(
                JoinType.INNER_JOIN, ImmutableList.of(new EqualTo(idA, idB)),
                taProj, tbProj, null);

        // Intermediate project: forwards v1, v3 as Slots, adds v2 (ORDER BY dep)
        LogicalProject<LogicalPlan> midProject = new LogicalProject<>(
                ImmutableList.of(v1.toSlot(), v2, v3.toSlot(), idA, idB), join);

        // TopN with v2 in ORDER BY. v2 is blocked, and its dependency v1 is blocked too.
        LogicalTopN<LogicalProject<LogicalPlan>> topN = new LogicalTopN<>(
                ImmutableList.of(
                        new OrderKey(idA, false, false),
                        new OrderKey(v2.toSlot(), false, false)),
                10, 0, midProject);

        LogicalPlan rewritten = (LogicalPlan) PlanChecker
                .from(MemoTestUtils.createConnectContext(), topN)
                .applyCustom(new PullUpProjectExprUnderTopN())
                .getPlan();

        // Root should be a Project (upper) → TopN → ...
        Assertions.assertTrue(rewritten instanceof LogicalProject,
                "Root should be LogicalProject (upper project above TopN)");
        LogicalProject<?> upperProject = (LogicalProject<?>) rewritten;

        // Upper project must contain v1 as pass-through and v3 as a pulled-up Alias.
        Assertions.assertTrue(upperProject.getProjects().stream()
                .anyMatch(e -> e.getExprId().equals(v1.getExprId())),
                "v1 should pass through TopN because v2 depends on it");
        Assertions.assertTrue(upperProject.getProjects().stream()
                .anyMatch(e -> "v3".equals(e.getName())),
                "v3 should be pulled above TopN");
        Assertions.assertTrue(upperProject.getProjects().stream()
                .filter(e -> "v3".equals(e.getName()))
                .anyMatch(e -> e.getInputSlots().stream()
                        .anyMatch(s -> s.getExprId().equals(c.getExprId()))),
                "Pulled-up v3 should reference its base slot");

        // TopN is child of upper project
        Assertions.assertTrue(upperProject.child(0) instanceof LogicalTopN);
        LogicalTopN<?> rewrittenTopN = (LogicalTopN<?>) upperProject.child(0);

        // Below TopN: v1 must stay because v2 (an order key) depends on it.
        // v3 should be removed and replaced by its base slot c for lazy materialization.
        LogicalProject<?> lowerProject = (LogicalProject<?>) rewrittenTopN.child(0);
        Assertions.assertTrue(lowerProject.getProjects().stream()
                .anyMatch(e -> e.getExprId().equals(v1.getExprId())),
                "Lower project must contain v1 because order key v2 depends on it");
        Assertions.assertTrue(lowerProject.getProjects().stream()
                .anyMatch(e -> "v2".equals(e.getName())),
                "Lower project must contain v2 for TopN order key");
        Assertions.assertTrue(lowerProject.getProjects().stream()
                .anyMatch(e -> e.getExprId().equals(c.getExprId())),
                "Lower project must contain base slot c for pulled-up v3");
        Assertions.assertFalse(lowerProject.getProjects().stream()
                .anyMatch(e -> "v3".equals(e.getName())),
                "Lower project must NOT contain v3 because it is pulled above TopN");
        Assertions.assertFalse(lowerProject.getProjects().stream()
                .anyMatch(e -> e.getExprId().equals(a.getExprId())),
                "Lower project should not expose base slot a when v1 is blocked");
    }

    /**
     * Verifies that addUpperProject does not leak base slots (injected by
     * simplifyProject for lazy materialization) into the upper project's visible
     * output. The upper project must expose only what the original TopN output had.
     *
     * Before pullup:
     *   TopN(order by idA, limit 10, output: [v1, v2, idA, idB])
     *     Project(v1 = a + 1, v2 = b + 1, idA, idB)
     *       Scan(a, b, idA, idB)
     *
     * After pullup:
     *   Project(v1 = a + 1, v2 = b + 1, idA, idB)   ← upper: must NOT leak a, b
     *     TopN(order by idA)                          ← carries a, b internally
     *       Project(a, b, idA, idB)                   ← base slots for lazy mat
     *         Scan(a, b, idA, idB)
     */
    @Test
    void testUpperProjectDoesNotLeakBaseSlots() {
        LogicalOlapScan scan = new LogicalOlapScan(
                PlanConstructor.getNextRelationId(), PlanConstructor.student, ImmutableList.of("db"));
        Slot idA = scan.getOutput().get(0);
        Slot a = scan.getOutput().get(1);
        Slot idB = scan.getOutput().get(2);
        Slot b = scan.getOutput().get(3);
        Alias v1 = new Alias(new Add(a, new IntegerLiteral((byte) 1)), "v1");
        Alias v2 = new Alias(new Add(b, new IntegerLiteral((byte) 1)), "v2");

        LogicalPlan plan = new LogicalPlanBuilder(scan)
                .projectExprs(ImmutableList.of(v1, v2, idA, idB))
                .topN(10, 0, ImmutableList.of(2))
                .build();

        LogicalPlan rewritten = (LogicalPlan) PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                .getPlan();

        LogicalProject<?> upperProject = (LogicalProject<?>) rewritten;
        // Must expose exactly the original TopN output: [v1, v2, idA, idB]
        Assertions.assertEquals(4, upperProject.getProjects().size(),
                "Upper project output must match original TopN output size");
        Assertions.assertEquals(v1.getExprId(), upperProject.getProjects().get(0).getExprId());
        Assertions.assertEquals(v2.getExprId(), upperProject.getProjects().get(1).getExprId());
        Assertions.assertEquals(idA.getExprId(), upperProject.getProjects().get(2).getExprId());
        Assertions.assertEquals(idB.getExprId(), upperProject.getProjects().get(3).getExprId());
        // Base slots must NOT leak into upper project output
        Assertions.assertFalse(upperProject.getProjects().stream()
                .anyMatch(e -> e.getExprId().equals(a.getExprId())),
                "Base slot a must not leak into upper project output");
        Assertions.assertFalse(upperProject.getProjects().stream()
                .anyMatch(e -> e.getExprId().equals(b.getExprId())),
                "Base slot b must not leak into upper project output");

        // Base slots must still pass through TopN for lazy materialization
        LogicalTopN<?> topN = (LogicalTopN<?>) upperProject.child(0);
        Assertions.assertTrue(topN.getOutput().stream()
                .anyMatch(s -> s.getExprId().equals(a.getExprId())),
                "Base slot a must pass through TopN internally");
        Assertions.assertTrue(topN.getOutput().stream()
                .anyMatch(s -> s.getExprId().equals(b.getExprId())),
                "Base slot b must pass through TopN internally");
    }

    /**
     * Tests pass-through slot handling when an upper project forwards a bare
     * Slot whose underlying expression was pulled up from a lower project.
     *
     * Before pullup:
     *   TopN(order by id, limit 10)
     *     Project1(id, x)              ← x is bare Slot (pass-through from child)
     *       Project2(id, a+b as x)     ← computes x = a+b
     *         Scan(id, a, b)
     *
     * After pullup:
     *   Project(x = a + b, id)         ← upper (pulled above TopN)
     *     TopN(order by id)
     *       Project(id, a, b)          ← base slots only, x replaced by a, b
     *         Project(id, a, b)        ← simplified lower project
     *           Scan(id, a, b)
     *
     * The key mechanism: x in Project1 is a bare Slot that becomes unavailable
     * after Project2 is simplified (x is removed). It is detected as an
     * "unavailable pull-up slot" via isUnavailablePullUpSlot, and its
     * replacement expression (a+b) is added to passThroughExprs. The base
     * slots a, b from passThroughExprs are then retained in the simplified
     * Project1 so they can flow through TopN for lazy materialization.
     */
    @Test
    void testPassThroughSlotWithPulledUpExpressionFromLowerProject() {
        LogicalOlapScan scan = new LogicalOlapScan(
                PlanConstructor.getNextRelationId(), PlanConstructor.student, ImmutableList.of("db"));
        Slot id = scan.getOutput().get(0);
        Slot a = scan.getOutput().get(1);   // gender column, used as "a"
        Slot b = scan.getOutput().get(2);   // name column, used as "b"

        // Lower project: computes x = a + b
        Alias x = new Alias(new Add(a, b), "x");
        LogicalProject<LogicalOlapScan> lowerProject
                = new LogicalProject<>(ImmutableList.of(id, x), scan);

        // Upper project: passes through x as a bare Slot, plus id
        LogicalProject<LogicalProject<LogicalOlapScan>> upperProject
                = new LogicalProject<>(ImmutableList.of(x.toSlot(), id), lowerProject);

        // TopN(order by id)
        LogicalTopN<LogicalProject<LogicalProject<LogicalOlapScan>>> topN = new LogicalTopN<>(
                ImmutableList.of(new OrderKey(id, false, false)),
                10, 0, upperProject);

        LogicalPlan rewritten = (LogicalPlan) PlanChecker
                .from(MemoTestUtils.createConnectContext(), topN)
                .applyCustom(new PullUpProjectExprUnderTopN())
                .getPlan();

        // Root should be a Project (upper) → TopN → ...
        Assertions.assertTrue(rewritten instanceof LogicalProject,
                "Root should be LogicalProject (upper project above TopN)");
        LogicalProject<?> rewrittenUpper = (LogicalProject<?>) rewritten;

        // Upper project must contain Alias(x, a+b) — the pulled-up expression
        Assertions.assertTrue(rewrittenUpper.getProjects().stream()
                .anyMatch(e -> "x".equals(e.getName())),
                "x should be pulled above TopN in upper project");

        // Upper project must retain id
        Assertions.assertTrue(rewrittenUpper.getProjects().stream()
                .anyMatch(e -> e.getExprId().equals(id.getExprId())),
                "id should be in upper project output");

        // Base slots a, b must NOT leak into upper project output
        Assertions.assertFalse(rewrittenUpper.getProjects().stream()
                .anyMatch(e -> e.getExprId().equals(a.getExprId())),
                "Base slot a must not leak into upper project output");
        Assertions.assertFalse(rewrittenUpper.getProjects().stream()
                .anyMatch(e -> e.getExprId().equals(b.getExprId())),
                "Base slot b must not leak into upper project output");

        // TopN is child of upper project
        Assertions.assertTrue(rewrittenUpper.child(0) instanceof LogicalTopN);
        LogicalTopN<?> rewrittenTopN = (LogicalTopN<?>) rewrittenUpper.child(0);

        // Below TopN: must contain base slots a and b (from passThroughExprs)
        LogicalProject<?> midProject = (LogicalProject<?>) rewrittenTopN.child(0);
        Assertions.assertTrue(midProject.getProjects().stream()
                .anyMatch(e -> e.getExprId().equals(a.getExprId())),
                "Project below TopN must contain base slot a (for lazy mat)");
        Assertions.assertTrue(midProject.getProjects().stream()
                .anyMatch(e -> e.getExprId().equals(b.getExprId())),
                "Project below TopN must contain base slot b (for lazy mat)");

        // Must NOT contain Alias x in the lower projects (expression pulled up)
        Assertions.assertFalse(midProject.getProjects().stream()
                .anyMatch(e -> "x".equals(e.getName())),
                "Project below TopN must NOT contain x (pulled above)");

        // The lowest project (original lowerProject after simplification)
        // should also only contain id, a, b
        LogicalProject<?> lowestProject = (LogicalProject<?>) midProject.child(0);
        Assertions.assertFalse(lowestProject.getProjects().stream()
                .anyMatch(e -> "x".equals(e.getName())),
                "Lowest project must NOT contain x (pulled above)");
        Assertions.assertTrue(lowestProject.getProjects().stream()
                .anyMatch(e -> e.getExprId().equals(a.getExprId())),
                "Lowest project must contain base slot a");
        Assertions.assertTrue(lowestProject.getProjects().stream()
                .anyMatch(e -> e.getExprId().equals(b.getExprId())),
                "Lowest project must contain base slot b");
    }

    /**
     * Tests chain resolution when an upper project computes an expression
     * (abs(x)) that references a slot (x) defined as a pulled-up expression
     * (a+b) in a lower project. Verifies that both expressions are resolved
     * through the chain and the intermediate slot disappears.
     *
     * Before pullup:
     *   TopN(order by id, limit 10)
     *     Project1(id, abs(x) as y)       ← y = abs(x), x from child
     *       Project2(id, a+b as x)        ← x = a+b
     *         Scan(id, a, b)
     *
     * After pullup:
     *   Project(y = abs(a + b), id)       ← upper: chain resolved to a+b
     *     TopN(order by id)
     *       Project(id, a, b)             ← only base slots
     *         Project(id, a, b)           ← simplified lower project
     *           Scan(id, a, b)
     *
     * Key mechanism: y = abs(x) is pulled up (canPullUp=true, child is not Slot).
     * x = a+b is also pulled up. In the replacer, resolveExpression replaces
     * SlotRef(x) with (a+b) via pullUpExprReplaceMap, so y becomes abs(a+b).
     * The intermediate slot x disappears entirely — it is neither in the upper
     * project output (originalTopNOutput only has y, id) nor in the lower
     * projects (pulled up).
     */
    @Test
    void testExpressionChainResolvedThroughIntermediateSlot() {
        LogicalOlapScan scan = new LogicalOlapScan(
                PlanConstructor.getNextRelationId(), PlanConstructor.student, ImmutableList.of("db"));
        Slot id = scan.getOutput().get(0);
        Slot a = scan.getOutput().get(1);   // gender column, used as "a"
        Slot b = scan.getOutput().get(2);   // name column, used as "b"

        // Lower project: computes x = a + b
        Alias x = new Alias(new Add(a, b), "x");
        LogicalProject<LogicalOlapScan> lowerProject
                = new LogicalProject<>(ImmutableList.of(id, x), scan);

        // Upper project: computes y = abs(x)
        Alias y = new Alias(new Abs(x.toSlot()), "y");
        LogicalProject<LogicalProject<LogicalOlapScan>> upperProject
                = new LogicalProject<>(ImmutableList.of(y, id), lowerProject);

        // TopN(order by id)
        LogicalTopN<LogicalProject<LogicalProject<LogicalOlapScan>>> topN = new LogicalTopN<>(
                ImmutableList.of(new OrderKey(id, false, false)),
                10, 0, upperProject);

        LogicalPlan rewritten = (LogicalPlan) PlanChecker
                .from(MemoTestUtils.createConnectContext(), topN)
                .applyCustom(new PullUpProjectExprUnderTopN())
                .getPlan();

        // Root should be a Project (upper)
        Assertions.assertTrue(rewritten instanceof LogicalProject,
                "Root should be LogicalProject (upper project above TopN)");
        LogicalProject<?> project3 = (LogicalProject<?>) rewritten;

        // === Verify project3 (upper) output ===
        // Must contain y (the pulled-up expression, now resolved to abs(a+b))
        Assertions.assertTrue(project3.getProjects().stream()
                .anyMatch(e -> "y".equals(e.getName())),
                "Upper project must contain y");

        // Must contain id
        Assertions.assertTrue(project3.getProjects().stream()
                .anyMatch(e -> e.getExprId().equals(id.getExprId())),
                "Upper project must contain id");

        // Must NOT contain x (intermediate slot, fully resolved away)
        Assertions.assertFalse(project3.getProjects().stream()
                .anyMatch(e -> "x".equals(e.getName())),
                "Upper project must NOT contain x (intermediate slot resolved away)");

        // Base slots a, b must NOT leak into upper project output
        Assertions.assertFalse(project3.getProjects().stream()
                .anyMatch(e -> e.getExprId().equals(a.getExprId())),
                "Base slot a must not leak into upper project output");
        Assertions.assertFalse(project3.getProjects().stream()
                .anyMatch(e -> e.getExprId().equals(b.getExprId())),
                "Base slot b must not leak into upper project output");

        // === Verify TopN ===
        Assertions.assertTrue(project3.child(0) instanceof LogicalTopN);
        LogicalTopN<?> rewrittenTopN = (LogicalTopN<?>) project3.child(0);

        // === Verify project4 (below TopN) output ===
        LogicalProject<?> project4 = (LogicalProject<?>) rewrittenTopN.child(0);
        // Must contain base slots a, b (for lazy mat)
        Assertions.assertTrue(project4.getProjects().stream()
                .anyMatch(e -> e.getExprId().equals(a.getExprId())),
                "Project below TopN must contain base slot a");
        Assertions.assertTrue(project4.getProjects().stream()
                .anyMatch(e -> e.getExprId().equals(b.getExprId())),
                "Project below TopN must contain base slot b");
        // Must contain id
        Assertions.assertTrue(project4.getProjects().stream()
                .anyMatch(e -> e.getExprId().equals(id.getExprId())),
                "Project below TopN must contain id");
        // Must NOT contain x or y (both pulled up)
        Assertions.assertFalse(project4.getProjects().stream()
                .anyMatch(e -> "x".equals(e.getName())),
                "Project below TopN must NOT contain x (pulled above)");
        Assertions.assertFalse(project4.getProjects().stream()
                .anyMatch(e -> "y".equals(e.getName())),
                "Project below TopN must NOT contain y (pulled above)");

        // === Verify project5 (lowest) output ===
        LogicalProject<?> project5 = (LogicalProject<?>) project4.child(0);
        Assertions.assertFalse(project5.getProjects().stream()
                .anyMatch(e -> "x".equals(e.getName())),
                "Lowest project must NOT contain x");
        Assertions.assertTrue(project5.getProjects().stream()
                .anyMatch(e -> e.getExprId().equals(a.getExprId())),
                "Lowest project must contain base slot a");
        Assertions.assertTrue(project5.getProjects().stream()
                .anyMatch(e -> e.getExprId().equals(b.getExprId())),
                "Lowest project must contain base slot b");
    }

    /**
     * Tests that when a TopN order key (z = abs(x)) depends on an intermediate
     * slot (x), the blocked order key also blocks the dependency x from being
     * pulled up. The non-blocked expression (y = abs(x)) can still be pulled up
     * and should reuse x passed through TopN.
     *
     * Before pullup:
     *   TopN(order by z, limit 10)       blocked={z}
     *     Project0(abs(x) as z, id, y)
     *       Project1(abs(x) as y, id)
     *         Project2(a+b as x, id)
     *           Scan(id, a, b)
     *
     * After pullup:
     *   Project(z, id, y = abs(x))       ← upper: y reuses x passed through TopN
     *     TopN(order by z)
     *       Project(z = abs(x), id, x)
     *         Project(id, x)
     *           Project(id, x = a+b)
     *             Scan(id, a, b)
     *
     * Key observations:
     * 1. z = abs(x) stays below TopN because z is the order key.
     * 2. Blocking z propagates to its input x, so x = a+b also stays below TopN.
     * 3. y = abs(x) is pulled up because y itself is not blocked.
     * 4. The pulled-up y must reference x, not expand x to a+b.
     */
    @Test
    void testBlockedOrderKeySharesDependencyWithPulledUpExpression() {
        LogicalOlapScan scan = new LogicalOlapScan(
                PlanConstructor.getNextRelationId(), PlanConstructor.student, ImmutableList.of("db"));
        Slot id = scan.getOutput().get(0);
        Slot a = scan.getOutput().get(1);
        Slot b = scan.getOutput().get(2);

        // Project2: x = a + b
        Alias x = new Alias(new Add(a, b), "x");
        LogicalProject<LogicalOlapScan> project2
                = new LogicalProject<>(ImmutableList.of(id, x), scan);

        // Project1: y = abs(x)
        Alias y = new Alias(new Abs(x.toSlot()), "y");
        LogicalProject<LogicalProject<LogicalOlapScan>> project1
                = new LogicalProject<>(ImmutableList.of(y, id), project2);

        // Project0: z = abs(x), plus pass-through id, y
        Alias z = new Alias(new Abs(x.toSlot()), "z");
        LogicalProject<LogicalProject<LogicalProject<LogicalOlapScan>>> project0
                = new LogicalProject<>(ImmutableList.of(z, id, y.toSlot()), project1);

        // TopN(order by z)
        LogicalTopN<LogicalProject<LogicalProject<LogicalProject<LogicalOlapScan>>>> topN
                = new LogicalTopN<>(
                        ImmutableList.of(new OrderKey(z.toSlot(), false, false)),
                        10, 0, project0);

        LogicalPlan rewritten = (LogicalPlan) PlanChecker
                .from(MemoTestUtils.createConnectContext(), topN)
                .applyCustom(new PullUpProjectExprUnderTopN())
                .getPlan();

        // Root should be a Project (upper)
        Assertions.assertTrue(rewritten instanceof LogicalProject,
                "Root should be LogicalProject (upper project above TopN)");
        LogicalProject<?> upperProject = (LogicalProject<?>) rewritten;

        // === Verify upper project output ===
        // Must contain y (pulled up, still depending on x passed through TopN)
        Assertions.assertTrue(upperProject.getProjects().stream()
                .anyMatch(e -> "y".equals(e.getName())),
                "Upper project must contain y (pulled up)");
        Assertions.assertTrue(upperProject.getProjects().stream()
                .filter(e -> "y".equals(e.getName()))
                .anyMatch(e -> e.getInputSlots().stream()
                        .anyMatch(s -> s.getExprId().equals(x.getExprId()))),
                "Pulled-up y should still reference x");
        // Must contain z (order key, passed through from TopN output)
        Assertions.assertTrue(upperProject.getProjects().stream()
                .anyMatch(e -> "z".equals(e.getName())),
                "Upper project must contain z (order key, pass-through)");
        // Must contain id
        Assertions.assertTrue(upperProject.getProjects().stream()
                .anyMatch(e -> e.getExprId().equals(id.getExprId())),
                "Upper project must contain id");
        // Must NOT expose x as final output.
        Assertions.assertFalse(upperProject.getProjects().stream()
                .anyMatch(e -> "x".equals(e.getName())),
                "Upper project must NOT expose x");
        // Base slots must NOT leak
        Assertions.assertFalse(upperProject.getProjects().stream()
                .anyMatch(e -> e.getExprId().equals(a.getExprId())),
                "Base slot a must not leak into upper project");
        Assertions.assertFalse(upperProject.getProjects().stream()
                .anyMatch(e -> e.getExprId().equals(b.getExprId())),
                "Base slot b must not leak into upper project");

        // === Verify TopN still has z as order key ===
        Assertions.assertTrue(upperProject.child(0) instanceof LogicalTopN);
        LogicalTopN<?> rewrittenTopN = (LogicalTopN<?>) upperProject.child(0);
        Assertions.assertEquals(1, rewrittenTopN.getOrderKeys().size());
        Assertions.assertEquals(z.getExprId(),
                ((NamedExpression) rewrittenTopN.getOrderKeys().get(0).getExpr()).getExprId(),
                "TopN order key should still be z");

        // === Verify project below TopN ===
        // Must contain z (order key), id and x. x is kept below TopN because z depends on it.
        LogicalProject<?> lowerProject = (LogicalProject<?>) rewrittenTopN.child(0);
        Assertions.assertTrue(lowerProject.getProjects().stream()
                .anyMatch(e -> "z".equals(e.getName())),
                "Lower project must contain z (order key expression)");
        Assertions.assertTrue(lowerProject.getProjects().stream()
                .anyMatch(e -> e.getExprId().equals(x.getExprId())),
                "Lower project must contain x because order key z depends on it");
        Assertions.assertFalse(lowerProject.getProjects().stream()
                .anyMatch(e -> "y".equals(e.getName())),
                "Lower project must NOT contain y (pulled above)");

        LogicalProject<?> middleProject = (LogicalProject<?>) lowerProject.child(0);
        Assertions.assertTrue(middleProject.getProjects().stream()
                .anyMatch(e -> e.getExprId().equals(x.getExprId())),
                "Middle project must pass through x");

        LogicalProject<?> lowestProject = (LogicalProject<?>) middleProject.child(0);
        Assertions.assertTrue(lowestProject.getProjects().stream()
                .anyMatch(e -> "x".equals(e.getName())),
                "Lowest project must still compute x");
        Assertions.assertFalse(lowestProject.getProjects().stream()
                .anyMatch(e -> e.getExprId().equals(a.getExprId())),
                "Lowest project should not expose base slot a when x is blocked");
        Assertions.assertFalse(lowestProject.getProjects().stream()
                .anyMatch(e -> e.getExprId().equals(b.getExprId())),
                "Lowest project should not expose base slot b when x is blocked");
    }

}
