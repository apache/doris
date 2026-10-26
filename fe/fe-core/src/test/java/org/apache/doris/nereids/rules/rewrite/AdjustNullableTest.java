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

import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

class AdjustNullableTest {

    @Test
    void testAsofMatchConditionUsesChildNullability() {
        assertOtherConjunctNullability(JoinType.ASOF_LEFT_INNER_JOIN, false);
        assertOtherConjunctNullability(JoinType.ASOF_RIGHT_INNER_JOIN, false);
        assertOtherConjunctNullability(JoinType.ASOF_LEFT_OUTER_JOIN, false);
        assertOtherConjunctNullability(JoinType.ASOF_RIGHT_OUTER_JOIN, false);
    }

    @Test
    void testRegularOuterJoinOtherConditionUsesJoinOutputNullability() {
        assertOtherConjunctNullability(JoinType.LEFT_OUTER_JOIN, true);
    }

    private void assertOtherConjunctNullability(JoinType joinType, boolean rightNullable) {
        LogicalOlapScan left = new LogicalOlapScan(PlanConstructor.getNextRelationId(), PlanConstructor.student,
                ImmutableList.of(""));
        LogicalOlapScan right = new LogicalOlapScan(PlanConstructor.getNextRelationId(), PlanConstructor.student,
                ImmutableList.of(""));
        Expression hashCondition = new EqualTo(left.getOutput().get(0), right.getOutput().get(0));
        Expression matchCondition = new GreaterThan(left.getOutput().get(1), right.getOutput().get(1));
        LogicalJoin<?, ?> join = (LogicalJoin<?, ?>) new LogicalPlanBuilder(left)
                .join(right, joinType, ImmutableList.of(hashCondition), ImmutableList.of(matchCondition))
                .build();

        LogicalJoin<?, ?> adjustedJoin = AdjustNullable.doVisitLogicalJoin(join, new HashMap<>(), false, false);
        GreaterThan adjustedCondition = (GreaterThan) adjustedJoin.getOtherJoinConjuncts().get(0);

        Assertions.assertFalse(((SlotReference) adjustedCondition.left()).nullable());
        Assertions.assertEquals(rightNullable, ((SlotReference) adjustedCondition.right()).nullable());
    }
}
