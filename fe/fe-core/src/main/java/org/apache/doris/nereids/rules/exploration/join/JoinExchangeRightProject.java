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
import org.apache.doris.nereids.trees.plans.JoinHint;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.JoinUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * rule factory for exchange without inside-project.
 */
public class JoinExchangeRightProject extends OneExplorationRuleFactory {
    public static final JoinExchangeRightProject INSTANCE = new JoinExchangeRightProject();

    /*
     *        topJoin                      newTopJoin
     *        /      \                      /      \
     *   leftJoin  rightJoin   -->   newLeftJoin newRightJoin
     *    /    \    /    \            /    \        /    \
     *   A      B  C      D          A      C      B      D
     */
    @Override
    public Rule build() {
        return innerLogicalJoin(innerLogicalJoin(), logicalProject(innerLogicalJoin()))
                .when(JoinExchange::checkReorder)
                .when(join -> join.right().isAllSlots())
                .whenNot(join -> join.hasJoinHint()
                        || join.left().hasJoinHint() || join.right().child().hasJoinHint())
                .whenNot(join -> join.isMarkJoin() || join.left().isMarkJoin() || join.right().child().isMarkJoin())
                .then(topJoin -> {
                    LogicalJoin<GroupPlan, GroupPlan> leftJoin = topJoin.left();
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
                    JoinExchange.splitTopCondition(topJoin.getHashJoinConjuncts(), acOutputExprIdSet, bdOutputExprIdSet,
                            newLeftJoinHashJoinConjuncts, newRightJoinHashJoinConjuncts, newTopJoinHashJoinConjuncts);

                    List<Expression> newLeftJoinOtherJoinConjuncts = Lists.newArrayList();
                    List<Expression> newRightJoinOtherJoinConjuncts = Lists.newArrayList();
                    List<Expression> newTopJoinOtherJoinConjuncts = new ArrayList<>(leftJoin.getOtherJoinConjuncts());
                    newTopJoinOtherJoinConjuncts.addAll(rightJoin.getOtherJoinConjuncts());
                    JoinExchange.splitTopCondition(topJoin.getOtherJoinConjuncts(), acOutputExprIdSet,
                            bdOutputExprIdSet, newLeftJoinOtherJoinConjuncts, newRightJoinOtherJoinConjuncts,
                            newTopJoinOtherJoinConjuncts);

                    if (newLeftJoinHashJoinConjuncts.size() == 0 || newRightJoinHashJoinConjuncts.size() == 0) {
                        return null;
                    }

                    LogicalJoin<GroupPlan, GroupPlan> newLeftJoin = new LogicalJoin<>(JoinType.INNER_JOIN,
                            newLeftJoinHashJoinConjuncts, newLeftJoinOtherJoinConjuncts, JoinHint.NONE, a, c);
                    LogicalJoin<GroupPlan, GroupPlan> newRightJoin = new LogicalJoin<>(JoinType.INNER_JOIN,
                            newRightJoinHashJoinConjuncts, newRightJoinOtherJoinConjuncts, JoinHint.NONE, b, d);
                    Set<ExprId> topUsedExprIds = new HashSet<>(topJoin.getOutputExprIdSet());
                    newTopJoinHashJoinConjuncts.forEach(expr -> topUsedExprIds.addAll(expr.getInputSlotExprIds()));
                    newTopJoinOtherJoinConjuncts.forEach(expr -> topUsedExprIds.addAll(expr.getInputSlotExprIds()));
                    Plan left = CBOUtils.newProject(topUsedExprIds, newLeftJoin);
                    Plan right = CBOUtils.newProject(topUsedExprIds, newRightJoin);
                    LogicalJoin newTopJoin = new LogicalJoin<>(JoinType.INNER_JOIN,
                            newTopJoinHashJoinConjuncts, newTopJoinOtherJoinConjuncts, JoinHint.NONE,
                            left, right);
                    newTopJoin.getJoinReorderContext().setHasExchange(true);

                    return CBOUtils.projectOrSelf(ImmutableList.copyOf(topJoin.getOutput()), newTopJoin);
                }).toRule(RuleType.LOGICAL_JOIN_EXCHANGE_RIGHT_PROJECT);
    }

    /**
     * check reorder masks.
     */
    public static boolean checkReorder(LogicalJoin<? extends Plan, ? extends Plan> topJoin) {
        if (topJoin.getJoinReorderContext().hasCommute()
                || topJoin.getJoinReorderContext().hasLeftAssociate()
                || topJoin.getJoinReorderContext().hasRightAssociate()
                || topJoin.getJoinReorderContext().hasExchange()) {
            return false;
        } else {
            return true;
        }
    }
}
