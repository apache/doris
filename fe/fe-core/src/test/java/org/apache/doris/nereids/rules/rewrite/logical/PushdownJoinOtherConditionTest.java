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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PushdownJoinOtherConditionTest implements PatternMatchSupported {

    private final LogicalPlan rStudent = new LogicalOlapScan(PlanConstructor.getNextRelationId(),
            PlanConstructor.student);
    private final LogicalPlan rScore = new LogicalOlapScan(PlanConstructor.getNextRelationId(), PlanConstructor.score);
    private final List<JoinType> joinTypes = ImmutableList.of(JoinType.CROSS_JOIN, JoinType.INNER_JOIN);

    @Test
    void oneSide() {
        for (JoinType joinType : joinTypes) {
            Expression pushSide1 = new GreaterThan(rStudent.getOutput().get(1), Literal.of(18));
            Expression pushSide2 = new GreaterThan(rStudent.getOutput().get(1), Literal.of(50));
            List<Expression> condition = ImmutableList.of(pushSide1, pushSide2);

            Plan join = new LogicalJoin<>(joinType, ExpressionUtils.EMPTY_CONDITION, condition, rStudent, rScore);
            Plan root = new LogicalProject<>(Lists.newArrayList(), join);

            PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                    .applyTopDown(new PushdownJoinOtherCondition())
                    .printlnTree()
                    .matches(
                            logicalJoin(
                                    logicalFilter()
                                            .when(filter -> filter.getPredicates().toString()
                                                    .equals("((gender#1 > 18) AND (gender#1 > 50))")),
                                    leafPlan()
                            ).when(j -> j.getJoinType().isInnerOrCrossJoin())
                    );
        }
    }

    @Test
    void bothSideToBothSide() {
        for (JoinType joinType : joinTypes) {
            Expression leftSide = new GreaterThan(rStudent.getOutput().get(1), Literal.of(18));
            Expression rightSide = new GreaterThan(rScore.getOutput().get(2), Literal.of(60));
            List<Expression> condition = ImmutableList.of(leftSide, rightSide);

            Plan join = new LogicalJoin<>(joinType, ExpressionUtils.EMPTY_CONDITION, condition, rStudent, rScore);
            Plan root = new LogicalProject<>(Lists.newArrayList(), join);

            PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                    .applyTopDown(new PushdownJoinOtherCondition())
                    .printlnTree()
                    .matches(
                            logicalJoin(
                                    logicalFilter()
                                            .when(filter -> filter.getPredicates().toString()
                                                    .equals("(gender#1 > 18)")),
                                    logicalFilter()
                                            .when(filter -> filter.getPredicates().toString()
                                                    .equals("(grade#6 > 60)"))
                            ).when(j -> j.getJoinType().isInnerOrCrossJoin())
                    );
        }
    }
}
