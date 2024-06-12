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

import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.CBOUtils;
import org.apache.doris.nereids.rules.exploration.OneExplorationRuleFactory;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.JoinUtils;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * rule factory for exchange without inside-project.
 */
public class JoinExchangeBothProject extends OneExplorationRuleFactory {
    public static final JoinExchangeBothProject INSTANCE = new JoinExchangeBothProject();

    /*
     *        topJoin                      newTopJoin
     *        /      \                      /      \
     *   leftJoin  rightJoin   -->   newLeftJoin newRightJoin
     *    /    \    /    \            /    \        /    \
     *   A      B  C      D          A      C      B      D
     */
    @Override
    public Rule build() {
        return logicalProject(innerLogicalJoin(logicalProject(innerLogicalJoin()), logicalProject(innerLogicalJoin()))
                .when(JoinExchangeBothProject::checkReorder)
                .when(join -> join.left().isAllSlots() && join.right().isAllSlots())
                .whenNot(join -> join.hasDistributeHint()
                        || join.left().child().hasDistributeHint() || join.right().child().hasDistributeHint()))
                .then(topProject -> {
                    LogicalJoin<LogicalProject<LogicalJoin<GroupPlan, GroupPlan>>,
                            LogicalProject<LogicalJoin<GroupPlan, GroupPlan>>> topJoin = topProject.child();
                    LogicalJoin<GroupPlan, GroupPlan> leftJoin = topJoin.left().child();
                    LogicalJoin<GroupPlan, GroupPlan> rightJoin = topJoin.right().child();
                    GroupPlan a = leftJoin.left();
                    GroupPlan b = leftJoin.right();
                    GroupPlan c = rightJoin.left();
                    GroupPlan d = rightJoin.right();

                    Set<ExprId> acOutputExprIdSet = JoinUtils.getJoinOutputExprIdSet(a, c);
                    Set<ExprId> bdOutputExprIdSet = JoinUtils.getJoinOutputExprIdSet(b, d);

                    List<Expression> newLeftJoinHashJoinConjuncts = Lists.newArrayList();
                    List<Expression> newRightJoinHashJoinConjuncts = Lists.newArrayList();
                    List<Expression> newTopJoinHashJoinConjuncts = new ArrayList<>(leftJoin.getHashJoinConjuncts());
                    newTopJoinHashJoinConjuncts.addAll(rightJoin.getHashJoinConjuncts());
                    JoinExchangeBothProject.splitTopCondition(topJoin.getHashJoinConjuncts(), acOutputExprIdSet,
                            bdOutputExprIdSet,
                            newLeftJoinHashJoinConjuncts, newRightJoinHashJoinConjuncts, newTopJoinHashJoinConjuncts);

                    List<Expression> newLeftJoinOtherJoinConjuncts = Lists.newArrayList();
                    List<Expression> newRightJoinOtherJoinConjuncts = Lists.newArrayList();
                    List<Expression> newTopJoinOtherJoinConjuncts = new ArrayList<>(leftJoin.getOtherJoinConjuncts());
                    newTopJoinOtherJoinConjuncts.addAll(rightJoin.getOtherJoinConjuncts());
                    JoinExchangeBothProject.splitTopCondition(topJoin.getOtherJoinConjuncts(), acOutputExprIdSet,
                            bdOutputExprIdSet, newLeftJoinOtherJoinConjuncts, newRightJoinOtherJoinConjuncts,
                            newTopJoinOtherJoinConjuncts);

                    if (newLeftJoinHashJoinConjuncts.size() == 0 || newRightJoinHashJoinConjuncts.size() == 0) {
                        return null;
                    }

                    LogicalJoin<GroupPlan, GroupPlan> newLeftJoin = new LogicalJoin<>(JoinType.INNER_JOIN,
                            newLeftJoinHashJoinConjuncts, newLeftJoinOtherJoinConjuncts,
                            new DistributeHint(DistributeType.NONE), a, c, null);
                    LogicalJoin<GroupPlan, GroupPlan> newRightJoin = new LogicalJoin<>(JoinType.INNER_JOIN,
                            newRightJoinHashJoinConjuncts, newRightJoinOtherJoinConjuncts,
                            new DistributeHint(DistributeType.NONE), b, d, null);
                    Set<ExprId> topUsedExprIds = new HashSet<>();
                    topProject.getProjects().forEach(expr -> topUsedExprIds.addAll(expr.getInputSlotExprIds()));
                    newTopJoinHashJoinConjuncts.forEach(expr -> topUsedExprIds.addAll(expr.getInputSlotExprIds()));
                    newTopJoinOtherJoinConjuncts.forEach(expr -> topUsedExprIds.addAll(expr.getInputSlotExprIds()));
                    Plan left = CBOUtils.newProject(topUsedExprIds, newLeftJoin);
                    Plan right = CBOUtils.newProject(topUsedExprIds, newRightJoin);
                    LogicalJoin newTopJoin = new LogicalJoin<>(JoinType.INNER_JOIN,
                            newTopJoinHashJoinConjuncts, newTopJoinOtherJoinConjuncts,
                            new DistributeHint(DistributeType.NONE),
                            left, right, null);
                    newTopJoin.getJoinReorderContext().setHasExchange(true);

                    return topProject.withChildren(newTopJoin);
                }).toRule(RuleType.LOGICAL_JOIN_EXCHANGE_BOTH_PROJECT);
    }

    /**
     * check reorder masks.
     */
    public static boolean checkReorder(LogicalJoin<? extends Plan, ? extends Plan> topJoin) {
        if (topJoin.isLeadingJoin()
                || ((LogicalJoin) topJoin.left().child(0)).isLeadingJoin()
                || ((LogicalJoin) topJoin.right().child(0)).isLeadingJoin()) {
            return false;
        }
        if (topJoin.getJoinReorderContext().hasCommute()
                || topJoin.getJoinReorderContext().hasLeftAssociate()
                || topJoin.getJoinReorderContext().hasRightAssociate()
                || topJoin.getJoinReorderContext().hasExchange()) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * check whether a child plan is generate by leading
     *
     * @param child input plan by rule
     * @return boolean value if child is generate by leading
     */
    public static boolean isChildLeadingJoin(Plan child) {
        if (child instanceof LogicalProject) {
            if (((LogicalJoin) (child.child(0))).isLeadingJoin()) {
                return true;
            }
        } else if (child instanceof LogicalJoin) {
            if (((LogicalJoin) child).isLeadingJoin()) {
                return true;
            }
        }
        return false;
    }

    /**
     * split condition.
     */
    public static void splitTopCondition(List<Expression> topCondition,
            Set<ExprId> acOutputExprIdSet, Set<ExprId> bdOutputExprIdSet,
            List<Expression> newLeftCondition, List<Expression> newRightCondition,
            List<Expression> remainTopCondition) {
        for (Expression hashJoinConjunct : topCondition) {
            Set<ExprId> inputSlotExprIdSet = hashJoinConjunct.getInputSlotExprIds();
            if (acOutputExprIdSet.containsAll(inputSlotExprIdSet)) {
                newLeftCondition.add(hashJoinConjunct);
            } else if (bdOutputExprIdSet.containsAll(inputSlotExprIdSet)) {
                newRightCondition.add(hashJoinConjunct);
            } else {
                remainTopCondition.add(hashJoinConjunct);
            }
        }
    }
}
