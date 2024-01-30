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
import org.apache.doris.nereids.rules.exploration.CBOUtils;
import org.apache.doris.nereids.rules.exploration.OneExplorationRuleFactory;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import com.google.common.collect.ImmutableList;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Rule for inner join RightAssociate.
 */
public class InnerJoinRightAssociateProject extends OneExplorationRuleFactory {
    //       topJoin        newTopJoin
    //       /     \         /     \
    //  bottomJoin  C  ->   A   newBottomJoin
    //   /    \                     /    \
    //  A      B                   B      C
    public static final InnerJoinRightAssociateProject INSTANCE = new InnerJoinRightAssociateProject();

    @Override
    public Rule build() {
        return innerLogicalJoin(logicalProject(innerLogicalJoin()), group())
                .when(InnerJoinRightAssociate::checkReorder)
                .whenNot(join -> join.hasJoinHint() || join.left().child().hasJoinHint())
                .whenNot(join -> join.isMarkJoin() || join.left().child().isMarkJoin())
                .when(join -> join.left().isAllSlots())
                .then(topJoin -> {
                    LogicalJoin<GroupPlan, GroupPlan> bottomJoin = topJoin.left().child();
                    GroupPlan a = bottomJoin.left();
                    GroupPlan b = bottomJoin.right();
                    GroupPlan c = topJoin.right();
                    Set<ExprId> aExprIdSet = a.getOutputExprIdSet();

                    // Split condition
                    Map<Boolean, List<Expression>> splitHashConjuncts = CBOUtils.splitConjuncts(
                            topJoin.getHashJoinConjuncts(), bottomJoin.getHashJoinConjuncts(), aExprIdSet);
                    List<Expression> newTopHashConjuncts = splitHashConjuncts.get(true);
                    List<Expression> newBottomHashConjuncts = splitHashConjuncts.get(false);
                    Map<Boolean, List<Expression>> splitOtherConjuncts = CBOUtils.splitConjuncts(
                            topJoin.getOtherJoinConjuncts(), bottomJoin.getOtherJoinConjuncts(), aExprIdSet);
                    List<Expression> newTopOtherConjuncts = splitOtherConjuncts.get(true);
                    List<Expression> newBottomOtherConjuncts = splitOtherConjuncts.get(false);

                    if (newBottomOtherConjuncts.isEmpty() && newBottomHashConjuncts.isEmpty()) {
                        return null;
                    }

                    LogicalJoin<Plan, Plan> newBottomJoin = topJoin.withConjunctsChildren(
                            newBottomHashConjuncts, newBottomOtherConjuncts, b, c);

                    // new Project.
                    Set<ExprId> topUsedExprIds = new HashSet<>(topJoin.getOutputExprIdSet());
                    newTopHashConjuncts.forEach(expr -> topUsedExprIds.addAll(expr.getInputSlotExprIds()));
                    newTopOtherConjuncts.forEach(expr -> topUsedExprIds.addAll(expr.getInputSlotExprIds()));
                    Plan left = CBOUtils.newProject(topUsedExprIds, a);
                    Plan right = CBOUtils.newProject(topUsedExprIds, newBottomJoin);

                    LogicalJoin<Plan, Plan> newTopJoin = bottomJoin.withConjunctsChildren(
                            newTopHashConjuncts, newTopOtherConjuncts, left, right);
                    setNewBottomJoinReorder(newBottomJoin, bottomJoin);
                    setNewTopJoinReorder(newTopJoin, topJoin);

                    return CBOUtils.projectOrSelf(ImmutableList.copyOf(topJoin.getOutput()), newTopJoin);
                }).toRule(RuleType.LOGICAL_INNER_JOIN_RIGHT_ASSOCIATIVE_PROJECT);
    }

    /**
     * Check JoinReorderContext
     */
    public static boolean checkReorder(LogicalJoin<? extends Plan, GroupPlan> topJoin) {
        return !topJoin.getJoinReorderContext().hasCommute()
                && !topJoin.getJoinReorderContext().hasRightAssociate()
                && !topJoin.getJoinReorderContext().hasLeftAssociate()
                && !topJoin.getJoinReorderContext().hasExchange();
    }

    /**
     * Set JoinReorderContext
     */
    public static void setNewTopJoinReorder(LogicalJoin newTopJoin, LogicalJoin topJoin) {
        newTopJoin.getJoinReorderContext().copyFrom(topJoin.getJoinReorderContext());
        newTopJoin.getJoinReorderContext().setHasRightAssociate(true);
        newTopJoin.getJoinReorderContext().setHasCommute(false);
    }

    /**
     * Set JoinReorderContext
     */
    public static void setNewBottomJoinReorder(LogicalJoin newBottomJoin, LogicalJoin bottomJoin) {
        newBottomJoin.getJoinReorderContext().copyFrom(bottomJoin.getJoinReorderContext());
        newBottomJoin.getJoinReorderContext().setHasCommute(false);
        newBottomJoin.getJoinReorderContext().setHasRightAssociate(false);
        newBottomJoin.getJoinReorderContext().setHasLeftAssociate(false);
        newBottomJoin.getJoinReorderContext().setHasExchange(false);
    }
}
