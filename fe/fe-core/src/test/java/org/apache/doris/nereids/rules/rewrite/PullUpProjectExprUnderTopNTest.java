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
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
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
                );
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
                );
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
                );
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
                );
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
                );
    }

    @Test
    void testBlockedByJoinCondition() {
        // topn -> project(x, y) -> join on x = scan2.id
        // x is referenced by join condition, so it should be blocked.
        Slot a = scan1.getOutput().get(1);
        Slot b = scan1.getOutput().get(0);
        Alias x = new Alias(new Add(a, new IntegerLiteral((byte) 1)), "x");
        Alias y = new Alias(new Add(b, new IntegerLiteral((byte) 1)), "y");

        EqualTo joinCond = new EqualTo(x.toSlot(), scan2.getOutput().get(0));
        LogicalJoin<LogicalPlan, LogicalPlan> join = new LogicalJoin<>(
                JoinType.INNER_JOIN,
                ImmutableList.of(joinCond),
                (LogicalPlan) scan1, (LogicalPlan) scan2, null);

        LogicalPlan plan = new LogicalPlanBuilder(join)
                .projectExprs(ImmutableList.of(x, y))
                .topN(3, 0, ImmutableList.of(0))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                // y is pulled up, x stays in the bottom project because it's blocked by join condition
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
                );
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
        // topn -> project(x, y) -> sort(by x) -> scan
        // x is used by sort order key, so x is blocked.
        // y is not blocked and should be pulled up.
        Slot a = scan1.getOutput().get(1);
        Slot b = scan1.getOutput().get(0);
        Alias x = new Alias(new Add(a, new IntegerLiteral((byte) 1)), "x");
        Alias y = new Alias(new Add(b, new IntegerLiteral((byte) 1)), "y");

        LogicalSort<LogicalPlan> sort = new LogicalSort<>(
                ImmutableList.of(new OrderKey(x.toSlot(), false, false)),
                scan1);
        LogicalPlan plan = new LogicalPlanBuilder(sort)
                .projectExprs(ImmutableList.of(x, y))
                .topN(3, 0, ImmutableList.of(0))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyCustom(new PullUpProjectExprUnderTopN())
                // y is pulled up, x stays in bottom project due to sort order key
                .matches(
                        logicalProject(
                                logicalTopN(
                                        logicalProject(
                                                logicalSort(
                                                        logicalOlapScan()
                                                )
                                        )
                                )
                        )
                );
    }

    @Test
    void testSetOperatorDoesNotBlockPullUp() {
        // topn -> union -> [project(x, y) -> scan1, project(x, y) -> scan2]
        // Set operator itself does not block expression pull-up.
        // Expressions in children are collected as long as they are not blocked.
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
                // Union does not block: project should be inserted above topn
                .matchesFromRoot(
                        logicalProject(
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
                        )
                );
    }
}
