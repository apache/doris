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
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

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
        return logicalProject(innerLogicalJoin(logicalProject(innerLogicalJoin()), group())
                .when(topJoin -> checkReorder(topJoin))
                .whenNot(join -> join.hasDistributeHint() || join.left().child().hasDistributeHint())
                .when(join -> join.left().isAllSlots()))
                .then(topProject -> {
                    LogicalJoin<LogicalProject<LogicalJoin<GroupPlan, GroupPlan>>, GroupPlan> topJoin
                            = topProject.child();
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
                            newBottomHashConjuncts, newBottomOtherConjuncts, b, c, null);

                    // new Project.
                    Set<ExprId> topUsedExprIds = new HashSet<>();
                    topProject.getProjects().forEach(expr -> topUsedExprIds.addAll(expr.getInputSlotExprIds()));
                    newTopHashConjuncts.forEach(expr -> topUsedExprIds.addAll(expr.getInputSlotExprIds()));
                    newTopOtherConjuncts.forEach(expr -> topUsedExprIds.addAll(expr.getInputSlotExprIds()));
                    Plan left = CBOUtils.newProject(topUsedExprIds, a);
                    Plan right = CBOUtils.newProject(topUsedExprIds, newBottomJoin);

                    LogicalJoin<Plan, Plan> newTopJoin = bottomJoin.withConjunctsChildren(
                            newTopHashConjuncts, newTopOtherConjuncts, left, right, null);
                    newTopJoin.getJoinReorderContext().setHasRightAssociate(true);

                    return topProject.withChildren(newTopJoin);
                }).toRule(RuleType.LOGICAL_INNER_JOIN_RIGHT_ASSOCIATIVE_PROJECT);
    }

    /**
     * Check JoinReorderContext
     */
    public static boolean checkReorder(LogicalJoin<? extends Plan, GroupPlan> topJoin) {
        if (topJoin.isLeadingJoin()
                || ((LogicalJoin) topJoin.left().child(0)).isLeadingJoin()) {
            return false;
        }
        return !topJoin.getJoinReorderContext().hasCommute()
                && !topJoin.getJoinReorderContext().hasRightAssociate()
                && !topJoin.getJoinReorderContext().hasLeftAssociate()
                && !topJoin.getJoinReorderContext().hasExchange();
    }
}
