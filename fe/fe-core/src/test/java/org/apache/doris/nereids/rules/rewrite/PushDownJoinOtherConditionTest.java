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
        oneSide(JoinType.CROSS_JOIN, false);
        oneSide(JoinType.INNER_JOIN, false);
        oneSide(JoinType.LEFT_OUTER_JOIN, true);
        oneSide(JoinType.LEFT_SEMI_JOIN, true);
        oneSide(JoinType.LEFT_ANTI_JOIN, true);
        oneSide(JoinType.RIGHT_OUTER_JOIN, false);
        oneSide(JoinType.RIGHT_SEMI_JOIN, false);
        oneSide(JoinType.RIGHT_ANTI_JOIN, false);
    }

    private void oneSide(JoinType joinType, boolean testRight) {
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
        bothSideToBothSide(JoinType.CROSS_JOIN);
        bothSideToBothSide(JoinType.INNER_JOIN);
        bothSideToBothSide(JoinType.LEFT_SEMI_JOIN);
        bothSideToBothSide(JoinType.RIGHT_SEMI_JOIN);
    }

    private void bothSideToBothSide(JoinType joinType) {

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

    @Test
    void bothSideToOneSide() {
        bothSideToOneSide(JoinType.LEFT_OUTER_JOIN, true);
        bothSideToOneSide(JoinType.LEFT_ANTI_JOIN, true);
        bothSideToOneSide(JoinType.RIGHT_OUTER_JOIN, false);
        bothSideToOneSide(JoinType.RIGHT_ANTI_JOIN, false);
    }

    private void bothSideToOneSide(JoinType joinType, boolean testRight) {
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
}
