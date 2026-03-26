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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PushDownJoinOtherConditionTest implements MemoPatternMatchSupported {

    private LogicalOlapScan rStudent;
    private LogicalOlapScan rScore;

    private List<Slot> rStudentSlots;

    private List<Slot> rScoreSlots;

    /**
     * ut before.
     */
    @BeforeAll
    final void beforeAll() {
        rStudent = new LogicalOlapScan(PlanConstructor.getNextRelationId(), PlanConstructor.student,
                ImmutableList.of(""));
        rScore = new LogicalOlapScan(PlanConstructor.getNextRelationId(), PlanConstructor.score, ImmutableList.of(""));
        rStudentSlots = rStudent.getOutput();
        rScoreSlots = rScore.getOutput();
    }

    @Test
    void oneSide() {
        oneSidePush(JoinType.CROSS_JOIN, false);
        oneSidePush(JoinType.INNER_JOIN, false);
        oneSidePush(JoinType.LEFT_OUTER_JOIN, true);
        oneSidePush(JoinType.LEFT_SEMI_JOIN, true);
        oneSidePush(JoinType.LEFT_ANTI_JOIN, true);
        oneSidePush(JoinType.RIGHT_OUTER_JOIN, false);
        oneSidePush(JoinType.RIGHT_SEMI_JOIN, false);
        oneSidePush(JoinType.RIGHT_ANTI_JOIN, false);
        oneSideNoPush(JoinType.ASOF_LEFT_INNER_JOIN);
        oneSideNoPush(JoinType.ASOF_RIGHT_INNER_JOIN);
        oneSideNoPush(JoinType.ASOF_LEFT_OUTER_JOIN);
        oneSideNoPush(JoinType.ASOF_RIGHT_OUTER_JOIN);
    }

    private void oneSideNoPush(JoinType joinType) {
        Expression pushSide1 = new GreaterThan(rStudentSlots.get(1), Literal.of(18));
        Expression pushSide2 = new GreaterThan(rStudentSlots.get(1), Literal.of(50));
        List<Expression> condition = ImmutableList.of(pushSide1, pushSide2);

        LogicalOlapScan left = rStudent;
        LogicalOlapScan right = rScore;

        LogicalPlan root = new LogicalPlanBuilder(left)
                .join(right, joinType, ExpressionUtils.EMPTY_CONDITION, condition)
                .project(Lists.newArrayList())
                .build();

        PlanChecker planChecker = PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new PushDownJoinOtherCondition());

        planChecker.matches(
                logicalJoin(
                        logicalOlapScan(),
                        logicalOlapScan()).when(join -> join.getOtherJoinConjuncts().equals(condition)));
    }

    private void oneSidePush(JoinType joinType, boolean testRight) {
        Expression pushSide1 = new GreaterThan(rStudentSlots.get(1), Literal.of(18));
        Expression pushSide2 = new GreaterThan(rStudentSlots.get(1), Literal.of(50));
        List<Expression> condition = ImmutableList.of(pushSide1, pushSide2);

        LogicalOlapScan left = rStudent;
        LogicalOlapScan right = rScore;
        if (testRight) {
            left = rScore;
            right = rStudent;
        }

        LogicalPlan root = new LogicalPlanBuilder(left)
                .join(right, joinType, ExpressionUtils.EMPTY_CONDITION, condition)
                .project(Lists.newArrayList())
                .build();

        PlanChecker planChecker = PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new PushDownJoinOtherCondition());

        if (testRight) {
            planChecker.matches(
                    logicalJoin(
                            logicalOlapScan(),
                            logicalFilter()
                                    .when(filter -> ImmutableList.copyOf(filter.getConjuncts()).equals(condition))));
        } else {
            planChecker.matches(
                    logicalJoin(
                            logicalFilter().when(
                                    filter -> ImmutableList.copyOf(filter.getConjuncts()).equals(condition)),
                            logicalOlapScan()));

        }
    }

    @Test
    void bothSideToBothSide() {
        bothSidePush(JoinType.CROSS_JOIN);
        bothSidePush(JoinType.INNER_JOIN);
        bothSidePush(JoinType.LEFT_SEMI_JOIN);
        bothSidePush(JoinType.RIGHT_SEMI_JOIN);
        bothSideNoPush(JoinType.ASOF_LEFT_INNER_JOIN);
        bothSideNoPush(JoinType.ASOF_RIGHT_INNER_JOIN);
    }

    private void bothSidePush(JoinType joinType) {

        Expression leftSide = new GreaterThan(rStudentSlots.get(1), Literal.of(18));
        Expression rightSide = new GreaterThan(rScoreSlots.get(2), Literal.of(60));
        List<Expression> condition = ImmutableList.of(leftSide, rightSide);

        LogicalPlan root = new LogicalPlanBuilder(rStudent)
                .join(rScore, joinType, ExpressionUtils.EMPTY_CONDITION, condition)
                .project(Lists.newArrayList())
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new PushDownJoinOtherCondition())
                .matches(
                        logicalJoin(
                                logicalFilter().when(left -> left.getConjuncts().equals(ImmutableSet.of(leftSide))),
                                logicalFilter().when(right -> right.getConjuncts().equals(ImmutableSet.of(rightSide)))
                        ));
    }

    private void bothSideNoPush(JoinType joinType) {
        Expression leftSide = new GreaterThan(rStudentSlots.get(1), Literal.of(18));
        Expression rightSide = new GreaterThan(rScoreSlots.get(2), Literal.of(60));
        List<Expression> condition = ImmutableList.of(leftSide, rightSide);

        LogicalPlan root = new LogicalPlanBuilder(rStudent)
                .join(rScore, joinType, ExpressionUtils.EMPTY_CONDITION, condition)
                .project(Lists.newArrayList())
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new PushDownJoinOtherCondition())
                .matches(
                        logicalJoin(
                                logicalOlapScan(),
                                logicalOlapScan()));
    }

    @Test
    void bothSideToOneSide() {
        bothSideToOneSidePush(JoinType.LEFT_OUTER_JOIN, true);
        bothSideToOneSidePush(JoinType.LEFT_ANTI_JOIN, true);
        bothSideToOneSidePush(JoinType.RIGHT_OUTER_JOIN, false);
        bothSideToOneSidePush(JoinType.RIGHT_ANTI_JOIN, false);
        bothSideToOneSideNoPush(JoinType.ASOF_LEFT_OUTER_JOIN);
        bothSideToOneSideNoPush(JoinType.ASOF_RIGHT_OUTER_JOIN);
    }

    private void bothSideToOneSidePush(JoinType joinType, boolean testRight) {
        Expression pushSide = new GreaterThan(rStudentSlots.get(1), Literal.of(18));
        Expression reserveSide = new GreaterThan(rScoreSlots.get(2), Literal.of(60));
        List<Expression> condition = ImmutableList.of(pushSide, reserveSide);

        LogicalOlapScan left = rStudent;
        LogicalOlapScan right = rScore;
        if (testRight) {
            left = rScore;
            right = rStudent;
        }

        LogicalPlan root = new LogicalPlanBuilder(left)
                .join(right, joinType, ExpressionUtils.EMPTY_CONDITION, condition)
                .project(Lists.newArrayList())
                .build();

        PlanChecker planChecker = PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new PushDownJoinOtherCondition());

        if (testRight) {
            planChecker.matches(
                    logicalJoin(
                            logicalOlapScan(),
                            logicalFilter().when(filter -> filter.getConjuncts().equals(ImmutableSet.of(pushSide)))
                    ));
        } else {
            planChecker.matches(
                    logicalJoin(
                            logicalFilter().when(filter -> filter.getConjuncts().equals(ImmutableSet.of(pushSide))),
                            logicalOlapScan()
                    ));
        }
    }

    private void bothSideToOneSideNoPush(JoinType joinType) {
        Expression pushSide = new GreaterThan(rStudentSlots.get(1), Literal.of(18));
        Expression reserveSide = new GreaterThan(rScoreSlots.get(2), Literal.of(60));
        List<Expression> condition = ImmutableList.of(pushSide, reserveSide);

        LogicalOlapScan left = rStudent;
        LogicalOlapScan right = rScore;

        LogicalPlan root = new LogicalPlanBuilder(left)
                .join(right, joinType, ExpressionUtils.EMPTY_CONDITION, condition)
                .project(Lists.newArrayList())
                .build();

        PlanChecker planChecker = PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new PushDownJoinOtherCondition());

        planChecker.matches(
                logicalJoin(
                        logicalOlapScan(),
                        logicalOlapScan()));
    }
}
