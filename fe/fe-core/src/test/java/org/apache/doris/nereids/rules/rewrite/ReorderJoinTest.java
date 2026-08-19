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
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.util.List;

class ReorderJoinTest implements MemoPatternMatchSupported {

    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    private final LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);
    private final LogicalOlapScan scan4 = PlanConstructor.newLogicalOlapScan(3, "t4", 0);

    @Test
    public void testLeftOuterJoin() {
        testLeftOuterJoinHelper(JoinType.LEFT_OUTER_JOIN);
    }

    private void testLeftOuterJoinHelper(JoinType joinType) {
        ImmutableList<LogicalPlan> plans = ImmutableList.of(
                new LogicalPlanBuilder(scan1)
                        .join(scan2, joinType, Pair.of(0, 0))
                        .joinEmptyOn(scan3, JoinType.CROSS_JOIN)
                        .filter(new EqualTo(scan3.getOutput().get(0), scan1.getOutput().get(0)))
                        .build(),
                new LogicalPlanBuilder(scan1)
                        .joinEmptyOn(scan3, JoinType.CROSS_JOIN)
                        .join(scan2, joinType, Pair.of(0, 0))
                        .filter(new EqualTo(scan3.getOutput().get(0), scan1.getOutput().get(0)))
                        .build()
        );

        check(plans);
    }

    @Test
    public void testRightOuterJoin() {
        testRightOuterJoinHelper(JoinType.RIGHT_OUTER_JOIN);
    }

    @Test
    public void testSemiJoinCommuteInRewrite() {
        for (JoinType joinType : ImmutableList.of(
                JoinType.RIGHT_OUTER_JOIN, JoinType.RIGHT_SEMI_JOIN, JoinType.RIGHT_ANTI_JOIN)) {
            ConnectContext connectContext = MemoTestUtils.createConnectContext();
            connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
            PlanChecker checker = PlanChecker.from(connectContext)
                    .analyze(new LogicalPlanBuilder(scan1)
                            .join(scan2, joinType, Pair.of(0, 0))
                            .build())
                    .matches(logicalJoin().when(join -> join.getJoinType() == joinType));

            checker.rewrite()
                    .matches(logicalJoin().when(join -> join.getJoinType() == joinType.swap()));
        }
    }

    @Test
    public void testDisableJoinReorderBeforeRewrite() {
        for (JoinType joinType : ImmutableList.of(
                JoinType.RIGHT_OUTER_JOIN, JoinType.RIGHT_SEMI_JOIN, JoinType.RIGHT_ANTI_JOIN)) {
            ConnectContext connectContext = MemoTestUtils.createConnectContext();
            connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
            PlanChecker checker = PlanChecker.from(connectContext)
                    .analyze(new LogicalPlanBuilder(scan1)
                            .join(scan2, joinType, Pair.of(0, 0))
                            .build());

            connectContext.getSessionVariable().setDisableJoinReorder(true);
            checker.rewrite()
                    .matches(logicalJoin().when(join -> join.getJoinType() == joinType));
        }
    }

    @Test
    public void testSemiJoinCommuteInMvPreRewrite() {
        for (JoinType joinType : ImmutableList.of(
                JoinType.RIGHT_OUTER_JOIN, JoinType.RIGHT_SEMI_JOIN, JoinType.RIGHT_ANTI_JOIN)) {
            ConnectContext connectContext = MemoTestUtils.createConnectContext();
            connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
            PlanChecker checker = PlanChecker.from(connectContext)
                    .analyze(new LogicalPlanBuilder(scan1)
                            .join(scan2, joinType, Pair.of(0, 0))
                            .build());
            CascadesContext cascadesContext = checker.getCascadesContext();

            Rewriter.getCteChildrenRewriter(
                    cascadesContext, Rewriter.CTE_CHILDREN_REWRITE_JOBS_MV_REWRITE_USED, false).execute();
            MemoTestUtils.initMemoAndValidState(cascadesContext);
            checker.matches(logicalJoin().when(join -> join.getJoinType() == joinType.swap()));
        }
    }

    private void testRightOuterJoinHelper(JoinType joinType) {
        ImmutableList<LogicalPlan> plans = ImmutableList.of(
                new LogicalPlanBuilder(scan1)
                        .join(scan2, joinType, Pair.of(0, 0))
                        .joinEmptyOn(scan3, JoinType.CROSS_JOIN)
                        .filter(new EqualTo(scan3.getOutput().get(0), scan1.getOutput().get(0)))
                        .build(),
                new LogicalPlanBuilder(scan1)
                        .joinEmptyOn(scan3, JoinType.CROSS_JOIN)
                        .join(scan2, joinType, Pair.of(0, 0))
                        .filter(new EqualTo(scan3.getOutput().get(0), scan1.getOutput().get(0)))
                        .build()
        );

        check(plans);
    }

    @Test
    public void testLeftSemiJoin() {
        ImmutableList<LogicalPlan> plans = ImmutableList.of(
                new LogicalPlanBuilder(scan1)
                        .join(scan2, JoinType.LEFT_SEMI_JOIN, Pair.of(0, 0))
                        .joinEmptyOn(scan3, JoinType.CROSS_JOIN)
                        .filter(new EqualTo(scan3.getOutput().get(0), scan1.getOutput().get(0)))
                        .build()
        );
        check(plans);

        LogicalPlan plan2 = new LogicalPlanBuilder(scan1)
                .joinEmptyOn(scan3, JoinType.CROSS_JOIN)
                .join(scan2, JoinType.LEFT_SEMI_JOIN, Pair.of(0, 0))
                .filter(new EqualTo(scan3.getOutput().get(0), scan1.getOutput().get(0)))
                .build();
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        PlanChecker.from(connectContext, plan2)
                .rewrite()
                .matchesFromRoot(
                        logicalProject(logicalJoin(
                            logicalProject(logicalJoin().whenNot(join -> join.getJoinType().isCrossJoin())),
                            logicalOlapScan()
                        ).whenNot(join -> join.getJoinType().isCrossJoin()))
                );
    }

    @Test
    public void testLeftSemiMarkJoin() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .markJoin(scan2, JoinType.LEFT_SEMI_JOIN, Pair.of(0, 0))
                .join(scan3, JoinType.INNER_JOIN, Pair.of(0, 0))
                .filter(new EqualTo(scan3.getOutput().get(0), scan1.getOutput().get(0)))
                .build();
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        PlanChecker.from(connectContext, plan)
                .applyBottomUp(new ReorderJoin())
                .matchesFromRoot(
                        logicalJoin(
                                logicalJoin().when(join -> join.isMarkJoin()),
                                logicalOlapScan()
                        ).whenNot(join -> join.getJoinType().isCrossJoin())
                );
    }

    @Test
    public void testRightSemiJoin() {
        LogicalPlan plan1 = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.RIGHT_SEMI_JOIN, Pair.of(0, 0))
                .joinEmptyOn(scan3, JoinType.CROSS_JOIN)
                .filter(new EqualTo(scan3.getOutput().get(0), scan2.getOutput().get(0)))
                .build();
        check(ImmutableList.of(plan1));

        LogicalPlan plan2 = new LogicalPlanBuilder(scan2)
                .join(
                        new LogicalPlanBuilder(scan1)
                                .joinEmptyOn(scan3, JoinType.CROSS_JOIN)
                                .build(),
                        JoinType.RIGHT_SEMI_JOIN, Pair.of(0, 0)
                )
                .filter(new EqualTo(scan3.getOutput().get(0), scan1.getOutput().get(0)))
                .build();
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        PlanChecker.from(connectContext, plan2)
                .rewrite()
                .matchesFromRoot(
                        logicalProject(innerLogicalJoin(
                            logicalProject(leftSemiLogicalJoin()),
                            logicalOlapScan()
                        ))
                );
    }

    @Test
    public void testFullOuterJoin() {
        ImmutableList<LogicalPlan> plans = ImmutableList.of(
                new LogicalPlanBuilder(scan1)
                        .join(scan2, JoinType.FULL_OUTER_JOIN, Pair.of(0, 0))
                        .joinEmptyOn(scan3, JoinType.CROSS_JOIN)
                        .filter(new EqualTo(scan3.getOutput().get(0), scan1.getOutput().get(0)))
                        .build(),
                new LogicalPlanBuilder(scan1)
                        .joinEmptyOn(scan3, JoinType.CROSS_JOIN)
                        .join(scan2, JoinType.FULL_OUTER_JOIN, Pair.of(0, 0))
                        .filter(new EqualTo(scan3.getOutput().get(0), scan1.getOutput().get(0)))
                        .build()
        );

        check(plans);
    }

    @Test
    public void testCrossJoin() {
        ImmutableList<LogicalPlan> plans = ImmutableList.of(
                new LogicalPlanBuilder(scan1)
                        .joinEmptyOn(scan2, JoinType.CROSS_JOIN)
                        .joinEmptyOn(scan3, JoinType.CROSS_JOIN)
                        .filter(new EqualTo(scan1.getOutput().get(0), scan3.getOutput().get(0)))
                        .build(),
                new LogicalPlanBuilder(scan1)
                        .joinEmptyOn(scan2, JoinType.CROSS_JOIN)
                        .joinEmptyOn(scan3, JoinType.CROSS_JOIN)
                        .filter(new EqualTo(scan1.getOutput().get(0), scan2.getOutput().get(0)))
                        .build()
        );

        for (LogicalPlan plan : plans) {
            ConnectContext connectContext = MemoTestUtils.createConnectContext();
            connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
            PlanChecker.from(connectContext, plan)
                    .applyBottomUp(new ReorderJoin())
                    .matchesFromRoot(
                            logicalJoin(
                                    logicalJoin().whenNot(join -> join.getJoinType().isCrossJoin()),
                                    leafPlan()
                            ).when(join -> join.getJoinType().isCrossJoin())
                    );
        }
    }

    public void check(List<LogicalPlan> plans) {
        for (LogicalPlan plan : plans) {
            ConnectContext connectContext = MemoTestUtils.createConnectContext();
            connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
            PlanChecker.from(connectContext, plan)
                    .rewrite()
                    .printlnTree()
                    .matchesFromRoot(
                            logicalProject(logicalJoin(
                                logicalProject(logicalJoin().whenNot(join -> join.getJoinType().isCrossJoin())),
                                leafPlan()
                            ).whenNot(join -> join.getJoinType().isCrossJoin()))
                    );
        }
    }

    /*
     *                                  join
     *      crossjoin                   /  \
     *       /     \                  join  D
     * innerjoin  innerjoin  ──►      /  \
     *   /   \     /   \            join  C
     *  A     B   C     D           /  \
     *                             A    B
     */
    @Test
    public void testInnerOrCrossJoin() {
        LogicalPlan leftJoin = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .build();
        LogicalPlan rightJoin = new LogicalPlanBuilder(scan3)
                .join(scan4, JoinType.INNER_JOIN, Pair.of(0, 0))
                .build();

        LogicalPlan plan = new LogicalPlanBuilder(leftJoin)
                .joinEmptyOn(rightJoin, JoinType.CROSS_JOIN)
                .filter(new EqualTo(scan1.getOutput().get(0), scan3.getOutput().get(0)))
                .build();
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        PlanChecker.from(connectContext, plan)
                .applyBottomUp(new ReorderJoin())
                .matchesFromRoot(
                        logicalJoin(
                                logicalJoin(
                                        logicalJoin().whenNot(join -> join.getJoinType().isCrossJoin()),
                                        leafPlan()).whenNot(join -> join.getJoinType().isCrossJoin()),
                                leafPlan()).whenNot(join -> join.getJoinType().isCrossJoin()))
                .printlnTree();
    }

    @Test
    public void testAsofJoin() {
        testAsofJoinHelper(JoinType.ASOF_LEFT_INNER_JOIN);
        testAsofJoinHelper(JoinType.ASOF_RIGHT_INNER_JOIN);
        testAsofJoinHelper(JoinType.ASOF_LEFT_OUTER_JOIN);
        testAsofJoinHelper(JoinType.ASOF_RIGHT_OUTER_JOIN);
    }

    private void testAsofJoinHelper(JoinType joinType) {
        LogicalPlan leftJoin = new LogicalPlanBuilder(scan1)
                .join(scan2, joinType, Pair.of(0, 0))
                .build();
        LogicalPlan rightJoin = new LogicalPlanBuilder(scan3)
                .join(scan4, joinType, Pair.of(0, 0))
                .build();

        LogicalPlan plan = new LogicalPlanBuilder(leftJoin)
                .joinEmptyOn(rightJoin, JoinType.CROSS_JOIN)
                .filter(new EqualTo(scan1.getOutput().get(0), scan3.getOutput().get(0)))
                .build();
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        PlanChecker.from(connectContext, plan)
                .applyBottomUp(new ReorderJoin())
                .matchesFromRoot(
                        logicalJoin(
                                logicalJoin(
                                        leafPlan(),
                                        leafPlan()).whenNot(join -> join.getJoinType().isCrossJoin()),
                                logicalJoin(
                                        leafPlan(),
                                        leafPlan()).whenNot(join -> join.getJoinType().isCrossJoin()))
                                .whenNot(join -> join.getJoinType().isCrossJoin()))
                .printlnTree();
    }

    /**
     * A filter containing a NoneMovableFunction (assert_true) must prevent ReorderJoin from
     * collecting the filter conjuncts into the join and redistributing them below the join:
     * the child would evaluate assert_true on a superset of rows.
     */
    @Test
    public void testNotReorderJoinWithNoneMovableFunction() {
        Expression assertTrueExpr = new AssertTrue(
                new GreaterThan(scan1.getOutput().get(0), new IntegerLiteral(0)), new StringLiteral("msg"));
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .filter(assertTrueExpr)
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new ReorderJoin())
                .matchesFromRoot(
                        logicalFilter(
                                logicalJoin(logicalOlapScan(), logicalOlapScan())
                        ).when(filter -> filter.getConjuncts().equals(ImmutableSet.of(assertTrueExpr)))
                );
    }

    /**
     * A NoneMovableFunction (assert_true) stored on an inner join's own ON predicate must make
     * that join a boundary: the reorder must not flatten it and move the assertion onto a
     * different edge, where it would be evaluated on a superset of rows.
     * <pre>
     * Join(other: assert_true(A.x = C.x))
     *   Join(hash: A.k = B.k)
     *     A
     *     B
     *   C
     * </pre>
     */
    @Test
    public void testNotReorderJoinWithNoneMovableOnJoinEdge() {
        Expression assertTrueExpr = new AssertTrue(
                new GreaterThan(scan1.getOutput().get(0), scan3.getOutput().get(0)), new StringLiteral("msg"));
        LogicalPlan leftJoin = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .build();
        LogicalPlan plan = new LogicalPlanBuilder(leftJoin)
                .join(scan3, JoinType.INNER_JOIN,
                        ImmutableList.of(), ImmutableList.of(assertTrueExpr))
                .filter(new EqualTo(scan1.getOutput().get(0), scan3.getOutput().get(0)))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new ReorderJoin())
                .matchesFromRoot(
                        logicalFilter(
                                logicalJoin(
                                        logicalJoin(logicalOlapScan(), logicalOlapScan()),
                                        logicalOlapScan()
                                ).when(join -> join.getOtherJoinConjuncts()
                                        .equals(ImmutableList.of(assertTrueExpr)))
                        )
                );
    }
}
