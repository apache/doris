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
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Set;

/**
 * Push the predicate in the LogicalFilter to the join children.
 */
public class PushFilterInsideJoin extends OneRewriteRuleFactory {
    public static final PushFilterInsideJoin INSTANCE = new PushFilterInsideJoin();

    @Override
    public Rule build() {
        return logicalFilter(logicalJoin())
                // TODO: current just handle cross/inner join.
                .when(filter -> filter.child().getJoinType().isCrossJoin() || filter.child().getJoinType()
                        .isInnerJoin())
                .then(filter -> {
                    LogicalJoin<GroupPlan, GroupPlan> join = filter.child();
                    List<Expression> predicates = ExpressionUtils.extractConjunction(filter.getPredicates());
                    Set<Slot> leftOutputs = join.left().getOutputSet();
                    Set<Slot> rightOutputs = join.right().getOutputSet();

                    // TODO: current just handle col1 > col2 (ComparisonPredicate but not equal).
                    List<Expression> otherConditions = Lists.newArrayList();
                    join.getOtherJoinCondition().ifPresent(otherConditions::add);
                    List<Expression> remainingPredicates = Lists.newArrayList();
                    for (Expression predicate : predicates) {
                        if (predicate instanceof ComparisonPredicate) {
                            if (predicate instanceof EqualTo) {
                                remainingPredicates.add(predicate);
                                continue;
                            }
                        }
                        ComparisonPredicate comparisonPredicate = (ComparisonPredicate) predicate;
                        Set<Slot> leftSlots = comparisonPredicate.left().collect(SlotReference.class::isInstance);
                        Set<Slot> rightSlots = comparisonPredicate.right().collect(SlotReference.class::isInstance);

                        if (leftSlots.size() == 0 || rightSlots.size() == 0) {
                            remainingPredicates.add(predicate);
                            continue;
                        }

                        if ((leftOutputs.containsAll(leftSlots) && rightOutputs.containsAll(rightSlots))
                                || (leftOutputs.containsAll(rightSlots) && rightOutputs.containsAll(leftSlots))) {
                            otherConditions.add(predicate);
                        }
                    }

                    if (predicates.size() == remainingPredicates.size()) {
                        return null;
                    }

                    return PlanUtils.filterOrSelf(remainingPredicates,
                            new LogicalJoin<>(join.getJoinType(), join.getHashJoinConjuncts(),
                                    ExpressionUtils.optionalAnd(otherConditions), join.left(), join.right()));
                }).toRule(RuleType.PUSH_FILTER_INSIDE_JOIN);
    }
}
