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

import org.apache.doris.nereids.annotation.DependsRules;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Set;

/**
 * Push the predicate in the LogicalFilter to the join children.
 */
@DependsRules({
    InferPredicates.class,
    EliminateOuterJoin.class
})
public class PushFilterInsideJoin extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalFilter(logicalJoin())
                // TODO: current just handle cross/inner join.
                .when(filter -> filter.child().getJoinType().isCrossJoin()
                        || filter.child().getJoinType().isInnerJoin())
                .then(filter -> {
                    List<Expression> otherConditions = Lists.newArrayList(filter.getConjuncts());
                    LogicalJoin<Plan, Plan> join = filter.child();
                    Set<Slot> childOutput = join.getOutputSet();
                    if (ExpressionUtils.getInputSlotSet(otherConditions).stream()
                            .filter(MarkJoinSlotReference.class::isInstance)
                            .anyMatch(slot -> childOutput.contains(slot))) {
                        return null;
                    }
                    otherConditions.addAll(join.getOtherJoinConjuncts());
                    return new LogicalJoin<>(join.getJoinType(), join.getHashJoinConjuncts(),
                            otherConditions, join.getDistributeHint(), join.getMarkJoinSlotReference(),
                            join.children(), join.getJoinReorderContext());
                }).toRule(RuleType.PUSH_FILTER_INSIDE_JOIN);
    }
}
