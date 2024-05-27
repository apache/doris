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
 * Rule for inner join LeftAssociate.
 */
public class InnerJoinLeftAssociateProject extends OneExplorationRuleFactory {
    /*
     *    topJoin                  newTopJoin
     *    /     \                  /        \
     *   A    bottomJoin  ->  newBottomJoin  C
     *           /    \        /    \
     *          B      C      A      B
     */
    public static final InnerJoinLeftAssociateProject INSTANCE = new InnerJoinLeftAssociateProject();

    @Override
    public Rule build() {
        return logicalProject(innerLogicalJoin(group(), logicalProject(innerLogicalJoin()))
                .when(topJoin -> checkReorder(topJoin))
                .whenNot(join -> join.hasDistributeHint() || join.right().child().hasDistributeHint())
                .when(join -> join.right().isAllSlots()))
                .then(topProject -> {
                    LogicalJoin<GroupPlan, LogicalProject<LogicalJoin<GroupPlan, GroupPlan>>> topJoin
                            = topProject.child();
                    LogicalJoin<GroupPlan, GroupPlan> bottomJoin = topJoin.right().child();
                    GroupPlan a = topJoin.left();
                    GroupPlan b = bottomJoin.left();
                    GroupPlan c = bottomJoin.right();
                    Set<ExprId> cExprIdSet = c.getOutputExprIdSet();

                    // Split condition
                    Map<Boolean, List<Expression>> splitHashConjuncts = CBOUtils.splitConjuncts(
                            topJoin.getHashJoinConjuncts(), bottomJoin.getHashJoinConjuncts(), cExprIdSet);
                    List<Expression> newTopHashConjuncts = splitHashConjuncts.get(true);
                    List<Expression> newBottomHashConjuncts = splitHashConjuncts.get(false);
                    Map<Boolean, List<Expression>> splitOtherConjuncts = CBOUtils.splitConjuncts(
                            topJoin.getOtherJoinConjuncts(), bottomJoin.getOtherJoinConjuncts(), cExprIdSet);
                    List<Expression> newTopOtherConjuncts = splitOtherConjuncts.get(true);
                    List<Expression> newBottomOtherConjuncts = splitOtherConjuncts.get(false);

                    if (newBottomOtherConjuncts.isEmpty() && newBottomHashConjuncts.isEmpty()) {
                        return null;
                    }

                    // new join.
                    LogicalJoin<Plan, Plan> newBottomJoin = topJoin.withConjunctsChildren(
                            newBottomHashConjuncts, newBottomOtherConjuncts, a, b, null);

                    // new Project.
                    Set<ExprId> topUsedExprIds = new HashSet<>();
                    topProject.getProjects().forEach(expr -> topUsedExprIds.addAll(expr.getInputSlotExprIds()));
                    newTopHashConjuncts.forEach(expr -> topUsedExprIds.addAll(expr.getInputSlotExprIds()));
                    newTopOtherConjuncts.forEach(expr -> topUsedExprIds.addAll(expr.getInputSlotExprIds()));
                    Plan left = CBOUtils.newProject(topUsedExprIds, newBottomJoin);
                    Plan right = CBOUtils.newProject(topUsedExprIds, c);

                    LogicalJoin<Plan, Plan> newTopJoin = bottomJoin.withConjunctsChildren(
                            newTopHashConjuncts, newTopOtherConjuncts, left, right, null);
                    newTopJoin.getJoinReorderContext().setHasLeftAssociate(true);

                    return topProject.withChildren(newTopJoin);
                }).toRule(RuleType.LOGICAL_INNER_JOIN_LEFT_ASSOCIATIVE_PROJECT);
    }

    /** Check JoinReorderContext. */
    public static boolean checkReorder(LogicalJoin<GroupPlan, ? extends Plan> topJoin) {
        if (topJoin.isLeadingJoin()
                || JoinExchangeBothProject.isChildLeadingJoin(topJoin.right())) {
            return false;
        }
        return !topJoin.getJoinReorderContext().hasCommute()
                && !topJoin.getJoinReorderContext().hasLeftAssociate()
                && !topJoin.getJoinReorderContext().hasRightAssociate()
                && !topJoin.getJoinReorderContext().hasExchange();
    }
}
