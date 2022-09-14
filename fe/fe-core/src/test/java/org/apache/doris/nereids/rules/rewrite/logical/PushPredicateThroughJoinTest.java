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

import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.nereids.util.PlanRewriter;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.Optional;

/**
 * plan rewrite ut.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PushPredicateThroughJoinTest {

    private Plan rStudent;
    private Plan rScore;

    /**
     * ut before.
     */
    @BeforeAll
    public final void beforeAll() {
        rStudent = new LogicalOlapScan(PlanConstructor.getNextRelationId(), PlanConstructor.student, ImmutableList.of(""));
        rScore = new LogicalOlapScan(PlanConstructor.getNextRelationId(), PlanConstructor.score, ImmutableList.of(""));
    }

    @Test
    public void oneSide() {
        oneSide(JoinType.CROSS_JOIN, false);
        oneSide(JoinType.INNER_JOIN, false);
        oneSide(JoinType.LEFT_OUTER_JOIN, false);
        oneSide(JoinType.LEFT_SEMI_JOIN, false);
        oneSide(JoinType.LEFT_ANTI_JOIN, false);
        oneSide(JoinType.RIGHT_OUTER_JOIN, true);
        oneSide(JoinType.RIGHT_SEMI_JOIN, true);
        oneSide(JoinType.RIGHT_ANTI_JOIN, true);
    }

    private void oneSide(JoinType joinType, boolean testRight) {

        Expression whereCondition1 = new GreaterThan(rStudent.getOutput().get(1), Literal.of(18));
        Expression whereCondition2 = new GreaterThan(rStudent.getOutput().get(1), Literal.of(50));
        Expression whereCondition = ExpressionUtils.and(whereCondition1, whereCondition2);

        Plan left = rStudent;
        Plan right = rScore;
        if (testRight) {
            left = rScore;
            right = rStudent;
        }

        Plan join = new LogicalJoin<>(joinType, new ArrayList<>(), Optional.empty(), left, right);
        Plan filter = new LogicalFilter<>(whereCondition, join);
        Plan root = new LogicalProject<>(Lists.newArrayList(), filter);

        Memo memo = rewrite(root);
        Group rootGroup = memo.getRoot();

        Plan shouldJoin = rootGroup.getLogicalExpression().child(0).getLogicalExpression().getPlan();
        Plan shouldFilter = rootGroup.getLogicalExpression().child(0).getLogicalExpression()
                .child(0).getLogicalExpression().getPlan();
        Plan shouldScan = rootGroup.getLogicalExpression().child(0).getLogicalExpression()
                .child(1).getLogicalExpression().getPlan();
        if (testRight) {
            shouldFilter = rootGroup.getLogicalExpression().child(0).getLogicalExpression()
               .child(1).getLogicalExpression().getPlan();
            shouldScan = rootGroup.getLogicalExpression().child(0).getLogicalExpression()
                    .child(0).getLogicalExpression().getPlan();
        }

        Assertions.assertTrue(shouldJoin instanceof LogicalJoin);
        Assertions.assertTrue(shouldFilter instanceof LogicalFilter);
        Assertions.assertTrue(shouldScan instanceof LogicalOlapScan);
        LogicalFilter<Plan> actualFilter = (LogicalFilter<Plan>) shouldFilter;

        Assertions.assertEquals(whereCondition, actualFilter.getPredicates());
    }

    @Test
    public void bothSideToBothSide() {
        bothSideToBothSide(JoinType.CROSS_JOIN);
        bothSideToBothSide(JoinType.INNER_JOIN);
    }

    private void bothSideToBothSide(JoinType joinType) {

        Expression bothSideEqualTo = new EqualTo(new Add(rStudent.getOutput().get(0), Literal.of(1)),
                new Subtract(rScore.getOutput().get(0), Literal.of(2)));
        Expression leftSide = new GreaterThan(rStudent.getOutput().get(1), Literal.of(18));
        Expression rightSide = new GreaterThan(rScore.getOutput().get(2), Literal.of(60));
        Expression whereCondition = ExpressionUtils.and(bothSideEqualTo, leftSide, rightSide);

        Plan join = new LogicalJoin<>(joinType, new ArrayList<>(), Optional.empty(), rStudent, rScore);
        Plan filter = new LogicalFilter<>(whereCondition, join);
        Plan root = new LogicalProject<>(Lists.newArrayList(), filter);

        Memo memo = rewrite(root);
        Group rootGroup = memo.getRoot();

        Plan shouldJoin = rootGroup.getLogicalExpression().child(0).getLogicalExpression().getPlan();
        Plan leftFilter = rootGroup.getLogicalExpression().child(0).getLogicalExpression()
                .child(0).getLogicalExpression().getPlan();
        Plan rightFilter = rootGroup.getLogicalExpression().child(0).getLogicalExpression()
                .child(1).getLogicalExpression().getPlan();

        Assertions.assertTrue(shouldJoin instanceof LogicalJoin);
        Assertions.assertTrue(leftFilter instanceof LogicalFilter);
        Assertions.assertTrue(rightFilter instanceof LogicalFilter);
        LogicalJoin<Plan, Plan> actualJoin = (LogicalJoin<Plan, Plan>) shouldJoin;
        LogicalFilter<Plan> actualLeft = (LogicalFilter<Plan>) leftFilter;
        LogicalFilter<Plan> actualRight = (LogicalFilter<Plan>) rightFilter;
        Assertions.assertEquals(bothSideEqualTo, actualJoin.getOtherJoinCondition().get());
        Assertions.assertEquals(leftSide, actualLeft.getPredicates());
        Assertions.assertEquals(rightSide, actualRight.getPredicates());
    }

    @Test
    public void bothSideToOneSide() {
        bothSideToOneSide(JoinType.LEFT_OUTER_JOIN, false);
        bothSideToOneSide(JoinType.LEFT_ANTI_JOIN, false);
        bothSideToOneSide(JoinType.LEFT_SEMI_JOIN, false);
        bothSideToOneSide(JoinType.RIGHT_OUTER_JOIN, true);
        bothSideToOneSide(JoinType.RIGHT_ANTI_JOIN, true);
        bothSideToOneSide(JoinType.RIGHT_SEMI_JOIN, true);
    }

    private void bothSideToOneSide(JoinType joinType, boolean testRight) {

        Expression pushSide = new GreaterThan(rStudent.getOutput().get(1), Literal.of(18));
        Expression reserveSide = new GreaterThan(rScore.getOutput().get(2), Literal.of(60));
        Expression whereCondition = ExpressionUtils.and(pushSide, reserveSide);

        Plan left = rStudent;
        Plan right = rScore;
        if (testRight) {
            left = rScore;
            right = rStudent;
        }

        Plan join = new LogicalJoin<>(joinType, new ArrayList<>(), Optional.empty(), left, right);
        Plan filter = new LogicalFilter<>(whereCondition, join);
        Plan root = new LogicalProject<>(Lists.newArrayList(), filter);

        Memo memo = rewrite(root);
        Group rootGroup = memo.getRoot();

        Plan shouldJoin = rootGroup.getLogicalExpression().child(0).getLogicalExpression()
                .child(0).getLogicalExpression().getPlan();
        Plan shouldFilter = rootGroup.getLogicalExpression().child(0).getLogicalExpression()
                .child(0).getLogicalExpression().child(0).getLogicalExpression().getPlan();
        Plan shouldScan = rootGroup.getLogicalExpression().child(0).getLogicalExpression()
                .child(0).getLogicalExpression().child(1).getLogicalExpression().getPlan();
        if (testRight) {
            shouldFilter = rootGroup.getLogicalExpression().child(0).getLogicalExpression()
                    .child(0).getLogicalExpression().child(1).getLogicalExpression().getPlan();
            shouldScan = rootGroup.getLogicalExpression().child(0).getLogicalExpression()
                    .child(0).getLogicalExpression().child(0).getLogicalExpression().getPlan();
        }

        Assertions.assertTrue(shouldJoin instanceof LogicalJoin);
        Assertions.assertTrue(shouldFilter instanceof LogicalFilter);
        Assertions.assertTrue(shouldScan instanceof LogicalOlapScan);
        LogicalFilter<Plan> actualFilter = (LogicalFilter<Plan>) shouldFilter;
        Assertions.assertEquals(pushSide, actualFilter.getPredicates());
    }

    private Memo rewrite(Plan plan) {
        return PlanRewriter.topDownRewriteMemo(plan, new ConnectContext(), new PushPredicatesThroughJoin());
    }
}
