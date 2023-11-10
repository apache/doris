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

package org.apache.doris.nereids.rules.exploration.join;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.OneExplorationRuleFactory;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.JoinUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Rule for inner join LeftAssociate.
 */
public class InnerJoinLeftAssociate extends OneExplorationRuleFactory {
    /*
     *    topJoin                  newTopJoin
     *    /     \                  /        \
     *   A    bottomJoin  ->  newBottomJoin  C
     *           /    \        /    \
     *          B      C      A      B
     */
    public static final InnerJoinLeftAssociate INSTANCE = new InnerJoinLeftAssociate();

    @Override
    public Rule build() {
        return innerLogicalJoin(group(), innerLogicalJoin())
                .when(InnerJoinLeftAssociate::checkReorder)
                .whenNot(join -> join.hasJoinHint() || join.right().hasJoinHint())
                .whenNot(join -> join.isMarkJoin() || join.right().isMarkJoin())
                .then(topJoin -> {
                    LogicalJoin<GroupPlan, GroupPlan> bottomJoin = topJoin.right();
                    GroupPlan a = topJoin.left();
                    GroupPlan b = bottomJoin.left();
                    GroupPlan c = bottomJoin.right();

                    // Split condition
                    Set<ExprId> abOutputExprIdSet = JoinUtils.getJoinOutputExprIdSet(a, b);
                    Map<Boolean, List<Expression>> hashConjunctsSplit = Stream.concat(
                                    topJoin.getHashJoinConjuncts().stream(),
                                    bottomJoin.getHashJoinConjuncts().stream())
                            .collect(Collectors.partitioningBy(condition -> {
                                Set<ExprId> usedSlotExprIds = condition.getInputSlotExprIds();
                                return abOutputExprIdSet.containsAll(usedSlotExprIds);
                            }));

                    Map<Boolean, List<Expression>> otherConjunctsSplit = Stream.concat(
                                    topJoin.getOtherJoinConjuncts().stream(),
                                    bottomJoin.getOtherJoinConjuncts().stream())
                            .collect(Collectors.partitioningBy(condition -> {
                                Set<ExprId> usedSlotExprIds = condition.getInputSlotExprIds();
                                return abOutputExprIdSet.containsAll(usedSlotExprIds);
                            }));
                    List<Expression> newBottomHashJoinConjuncts = hashConjunctsSplit.get(true);
                    List<Expression> newTopHashJoinConjuncts = hashConjunctsSplit.get(false);
                    List<Expression> newBottomOtherJoinConjuncts = otherConjunctsSplit.get(true);
                    List<Expression> newTopOtherJoinConjuncts = otherConjunctsSplit.get(false);
                    if (newBottomHashJoinConjuncts.isEmpty() && newBottomOtherJoinConjuncts.isEmpty()) {
                        return null;
                    }

                    // new join.
                    LogicalJoin<Plan, Plan> newBottomJoin = topJoin.withConjunctsChildren(
                            newBottomHashJoinConjuncts, newBottomOtherJoinConjuncts, a, b);
                    LogicalJoin<Plan, Plan> newTopJoin = bottomJoin.withConjunctsChildren(
                            newTopHashJoinConjuncts, newTopOtherJoinConjuncts, newBottomJoin, c);
                    newTopJoin.getJoinReorderContext().setHasLeftAssociate(true);

                    return newTopJoin;
                }).toRule(RuleType.LOGICAL_INNER_JOIN_LEFT_ASSOCIATIVE);
    }

    /** Check JoinReorderContext. */
    public static boolean checkReorder(LogicalJoin<GroupPlan, ? extends Plan> topJoin) {
        return !topJoin.getJoinReorderContext().hasCommute()
                && !topJoin.getJoinReorderContext().hasLeftAssociate()
                && !topJoin.getJoinReorderContext().hasRightAssociate()
                && !topJoin.getJoinReorderContext().hasExchange();
    }
}
