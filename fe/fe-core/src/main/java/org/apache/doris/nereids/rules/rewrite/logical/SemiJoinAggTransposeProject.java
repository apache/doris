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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Pushdown semi-join through agg
 */
public class SemiJoinAggTransposeProject extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalJoin(logicalProject(logicalAggregate()), any())
                .when(join -> join.getJoinType().isLeftSemiOrAntiJoin())
                .when(join -> join.left().getProjects().stream().allMatch(n -> n instanceof Slot))
                .then(join -> {
                    LogicalProject<LogicalAggregate<Plan>> project = join.left();
                    LogicalAggregate<Plan> aggregate = project.child();
                    Set<Slot> canPushDownSlots = new HashSet<>();
                    if (aggregate.hasRepeat()) {
                        // When there is a repeat, the push-down condition is consistent with the repeat
                        canPushDownSlots.addAll(aggregate.getSourceRepeat().get().getCommonGroupingSetExpressions());
                    } else {
                        for (Expression groupByExpression : aggregate.getGroupByExpressions()) {
                            if (groupByExpression instanceof Slot) {
                                canPushDownSlots.add((Slot) groupByExpression);
                            }
                        }
                    }
                    Set<Slot> leftOutputSet = join.left().getOutputSet();
                    Set<Slot> conditionSlot = join.getConditionSlot()
                            .stream()
                            .filter(leftOutputSet::contains)
                            .collect(Collectors.toSet());
                    if (!canPushDownSlots.containsAll(conditionSlot)) {
                        return null;
                    }
                    Plan newPlan = aggregate.withChildren(join.withChildren(aggregate.child(), join.right()));
                    return project.withChildren(newPlan);
                }).toRule(RuleType.LOGICAL_SEMI_JOIN_AGG_TRANSPOSE_PROJECT);
    }
}
