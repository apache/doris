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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * PushdownFilterThroughJoinTest UT.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PushdownFilterThroughJoinTest implements PatternMatchSupported {

    private LogicalPlan rStudent;
    private LogicalPlan rScore;

    /**
     * ut before.
     */
    @BeforeAll
    public final void beforeAll() {
        rStudent = new LogicalOlapScan(PlanConstructor.getNextRelationId(), PlanConstructor.student,
                ImmutableList.of(""));
        rScore = new LogicalOlapScan(PlanConstructor.getNextRelationId(), PlanConstructor.score, ImmutableList.of(""));
    }

    public void testLeft(JoinType joinType) {
        Expression whereCondition1 = new GreaterThan(rStudent.getOutput().get(1), Literal.of(18));
        Expression whereCondition2 = new GreaterThan(rStudent.getOutput().get(1), Literal.of(50));
        Expression whereCondition = ExpressionUtils.and(whereCondition1, whereCondition2);

        LogicalPlan plan = new LogicalPlanBuilder(rStudent)
                .hashJoinEmptyOn(rScore, joinType)
                .filter(whereCondition)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(PushdownFilterThroughJoin.INSTANCE)
                .matchesFromRoot(
                        logicalJoin(
                                logicalFilter(logicalOlapScan())
                                        .when(filter -> filter.getPredicates().equals(whereCondition)),
                                logicalOlapScan()
                        )
                );
    }

    public void testRight(JoinType joinType) {
        Expression whereCondition1 = new GreaterThan(rStudent.getOutput().get(1), Literal.of(18));
        Expression whereCondition2 = new GreaterThan(rStudent.getOutput().get(1), Literal.of(50));
        Expression whereCondition = ExpressionUtils.and(whereCondition1, whereCondition2);

        LogicalPlan plan = new LogicalPlanBuilder(rScore)
                .hashJoinEmptyOn(rStudent, joinType)
                .filter(whereCondition)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(PushdownFilterThroughJoin.INSTANCE)
                .matchesFromRoot(
                        logicalJoin(
                                logicalOlapScan(),
                                logicalFilter(logicalOlapScan())
                                        .when(filter -> filter.getPredicates().equals(whereCondition))
                        )
                );
    }

    @Test
    public void oneSide() {
        testLeft(JoinType.CROSS_JOIN);
        testLeft(JoinType.INNER_JOIN);
        testLeft(JoinType.LEFT_OUTER_JOIN);
        testLeft(JoinType.LEFT_SEMI_JOIN);
        testLeft(JoinType.LEFT_ANTI_JOIN);
        testRight(JoinType.RIGHT_OUTER_JOIN);
        testRight(JoinType.RIGHT_SEMI_JOIN);
        testRight(JoinType.RIGHT_ANTI_JOIN);
    }

    @Test
    public void bothSideToBothSide() {
        bothSideToBothSide(JoinType.INNER_JOIN);
        bothSideToBothSide(JoinType.CROSS_JOIN);
    }

    private void bothSideToBothSide(JoinType joinType) {

        Expression bothSideEqualTo = new EqualTo(new Add(rStudent.getOutput().get(0), Literal.of(1)),
                new Subtract(rScore.getOutput().get(0), Literal.of(2)));
        Expression leftSide = new GreaterThan(rStudent.getOutput().get(1), Literal.of(18));
        Expression rightSide = new GreaterThan(rScore.getOutput().get(2), Literal.of(60));
        Expression whereCondition = ExpressionUtils.and(bothSideEqualTo, leftSide, rightSide);

        LogicalPlan plan = new LogicalPlanBuilder(rStudent)
                .hashJoinEmptyOn(rScore, joinType)
                .filter(whereCondition)
                .build();

        if (joinType.isInnerJoin()) {
            PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                    .applyTopDown(PushdownFilterThroughJoin.INSTANCE)
                    .printlnTree()
                    .matchesFromRoot(
                            logicalJoin(
                                    logicalFilter(logicalOlapScan())
                                            .when(filter -> filter.getPredicates().equals(leftSide)),
                                    logicalFilter(logicalOlapScan())
                                            .when(filter -> filter.getPredicates().equals(rightSide))
                            ).when(join -> join.getOtherJoinConjuncts().get(0).equals(bothSideEqualTo))
                    );
        }
        if (joinType.isCrossJoin()) {
            PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                    .applyTopDown(PushdownFilterThroughJoin.INSTANCE)
                    .printlnTree()
                    .matchesFromRoot(
                            logicalFilter(
                                    logicalJoin(
                                            logicalFilter(logicalOlapScan())
                                                    .when(filter -> filter.getPredicates().equals(leftSide)),
                                            logicalFilter(logicalOlapScan())
                                                    .when(filter -> filter.getPredicates().equals(rightSide))
                                    )
                            ).when(filter -> filter.getPredicates().equals(bothSideEqualTo))
                    );
        }
    }

    @Test
    public void bothSideToOneSide() {
        bothSideToLeft(JoinType.LEFT_OUTER_JOIN);
        bothSideToLeft(JoinType.LEFT_ANTI_JOIN);
        bothSideToLeft(JoinType.LEFT_SEMI_JOIN);
        bothSideToRight(JoinType.RIGHT_OUTER_JOIN);
        bothSideToRight(JoinType.RIGHT_ANTI_JOIN);
        bothSideToRight(JoinType.RIGHT_SEMI_JOIN);
    }

    private void bothSideToLeft(JoinType joinType) {
        Expression pushSide = new GreaterThan(rStudent.getOutput().get(1), Literal.of(18));
        Expression reserveSide = new GreaterThan(rScore.getOutput().get(2), Literal.of(60));
        Expression whereCondition = ExpressionUtils.and(pushSide, reserveSide);

        LogicalPlan plan = new LogicalPlanBuilder(rStudent)
                .hashJoinEmptyOn(rScore, joinType)
                .filter(whereCondition)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(PushdownFilterThroughJoin.INSTANCE)
                .matchesFromRoot(
                        logicalFilter(
                                logicalJoin(
                                        logicalFilter(logicalOlapScan())
                                                .when(filter -> filter.getPredicates().equals(pushSide)),
                                        logicalOlapScan()
                                )
                        )
                );
    }

    private void bothSideToRight(JoinType joinType) {
        Expression pushSide = new GreaterThan(rStudent.getOutput().get(1), Literal.of(18));
        Expression reserveSide = new GreaterThan(rScore.getOutput().get(2), Literal.of(60));
        Expression whereCondition = ExpressionUtils.and(pushSide, reserveSide);

        LogicalPlan plan = new LogicalPlanBuilder(rScore)
                .hashJoinEmptyOn(rStudent, joinType)
                .filter(whereCondition)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(PushdownFilterThroughJoin.INSTANCE)
                .matchesFromRoot(
                        logicalFilter(
                                logicalJoin(
                                        logicalOlapScan(),
                                        logicalFilter(logicalOlapScan()).when(
                                                filter -> filter.getPredicates().equals(pushSide))
                                )
                        )
                );
    }
}
