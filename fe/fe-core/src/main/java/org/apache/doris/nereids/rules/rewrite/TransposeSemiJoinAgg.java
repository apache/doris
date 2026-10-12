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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.NoneMovableFunction;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.qe.ConnectContext;

import java.util.Set;

/**
 * Pushdown semi-join through agg
 */
public class TransposeSemiJoinAgg extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalJoin(logicalAggregate(), any())
                .whenNot(join -> ConnectContext.get().getSessionVariable().isDisableJoinReorder())
                .when(join -> join.getJoinType().isLeftSemiOrAntiJoin())
                .whenNot(join -> join.isMarkJoin())
                .then(join -> {
                    LogicalAggregate<Plan> aggregate = join.left();
                    if (!canTranspose(aggregate, join)) {
                        return null;
                    }
                    return aggregate.withChildren(join.withChildren(aggregate.child(), join.right()));
                }).toRule(RuleType.TRANSPOSE_LOGICAL_SEMI_JOIN_AGG);
    }

    /**
     * check if we can transpose agg and semi join
     */
    public static boolean canTranspose(LogicalAggregate<? extends Plan> aggregate,
            LogicalJoin<? extends Plan, ? extends Plan> join) {
        // Transposition changes the rows or the number of times these expressions are evaluated.
        if (containsUnsafeExpression(join) || containsUnsafeExpression(aggregate)) {
            return false;
        }
        Set<Slot> canPushDownSlots = PushDownFilterThroughAggregation.getCanPushDownSlots(aggregate);
        // avoid push down scalar agg.
        if (canPushDownSlots.isEmpty()) {
            return false;
        }
        Set<Slot> leftConditionSlot = join.getLeftConditionSlot();
        return canPushDownSlots.containsAll(leftConditionSlot);
    }

    /** Check the intermediate project as well as the aggregate and join. */
    public static boolean canTranspose(LogicalAggregate<? extends Plan> aggregate,
            LogicalJoin<? extends Plan, ? extends Plan> join, LogicalProject<? extends Plan> project) {
        return !containsUnsafeExpression(project) && canTranspose(aggregate, join);
    }

    private static boolean containsUnsafeExpression(Plan plan) {
        return plan.getExpressions().stream().anyMatch(expression -> expression.containsVolatileExpression()
                || expression.containsType(NoneMovableFunction.class));
    }
}
