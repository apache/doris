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

import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.OneExplorationRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.JoinUtils;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * rule factory for exchange without inside-project.
 */
@Developing
public class JoinExchange extends OneExplorationRuleFactory {
    public static final JoinExchange INSTANCE = new JoinExchange();

    /*
     *        topJoin                      newTopJoin
     *        /      \                      /      \
     *   leftJoin  rightJoin   -->   newLeftJoin newRightJoin
     *    /    \    /    \            /    \        /    \
     *   A      B  C      D          A      C      B      D
     */
    @Override
    public Rule build() {
        return innerLogicalJoin(innerLogicalJoin(), innerLogicalJoin())
                .when(JoinExchange::checkReorder)
                .then(topJoin -> {
                    LogicalJoin<GroupPlan, GroupPlan> leftJoin = topJoin.left();
                    LogicalJoin<GroupPlan, GroupPlan> rightJoin = topJoin.right();
                    GroupPlan a = leftJoin.left();
                    GroupPlan b = leftJoin.right();
                    GroupPlan c = rightJoin.left();
                    GroupPlan d = rightJoin.right();

                    Set<Slot> acOutputSet = JoinUtils.getJoinOutputSet(a, c);
                    Set<Slot> bdOutputSet = JoinUtils.getJoinOutputSet(b, d);

                    List<Expression> newLeftJoinHashJoinConjuncts = Lists.newArrayList();
                    List<Expression> newRightJoinHashJoinConjuncts = Lists.newArrayList();
                    List<Expression> newTopJoinHashJoinConjuncts = new ArrayList<>(leftJoin.getHashJoinConjuncts());
                    newTopJoinHashJoinConjuncts.addAll(rightJoin.getHashJoinConjuncts());
                    splitTopConditon(topJoin.getHashJoinConjuncts(), acOutputSet, bdOutputSet,
                            newLeftJoinHashJoinConjuncts, newRightJoinHashJoinConjuncts, newTopJoinHashJoinConjuncts);

                    List<Expression> newLeftJoinOtherJoinConjuncts = Lists.newArrayList();
                    List<Expression> newRightJoinOtherJoinConjuncts = Lists.newArrayList();
                    List<Expression> newTopJoinOtherJoinConjuncts = new ArrayList<>(leftJoin.getOtherJoinConjuncts());
                    newTopJoinOtherJoinConjuncts.addAll(rightJoin.getOtherJoinConjuncts());
                    splitTopConditon(topJoin.getOtherJoinConjuncts(), acOutputSet, bdOutputSet,
                            newLeftJoinOtherJoinConjuncts, newRightJoinOtherJoinConjuncts,
                            newTopJoinOtherJoinConjuncts);

                    if (newLeftJoinHashJoinConjuncts.size() == 0 || newRightJoinHashJoinConjuncts.size() == 0) {
                        return null;
                    }
                    LogicalJoin<GroupPlan, GroupPlan> newLeftJoin = new LogicalJoin<>(JoinType.INNER_JOIN,
                            newLeftJoinHashJoinConjuncts, newLeftJoinOtherJoinConjuncts,
                            a, c, leftJoin.getJoinReorderContext());
                    newLeftJoin.getJoinReorderContext().setHasCommute(false);
                    newLeftJoin.getJoinReorderContext().setHasLeftAssociate(false);
                    newLeftJoin.getJoinReorderContext().setHasRightAssociate(false);
                    newLeftJoin.getJoinReorderContext().setHasExchange(false);

                    LogicalJoin<GroupPlan, GroupPlan> newRightJoin = new LogicalJoin<>(JoinType.INNER_JOIN,
                            newRightJoinHashJoinConjuncts, newRightJoinOtherJoinConjuncts,
                            b, d, rightJoin.getJoinReorderContext());
                    newRightJoin.getJoinReorderContext().setHasCommute(false);
                    newRightJoin.getJoinReorderContext().setHasLeftAssociate(false);
                    newRightJoin.getJoinReorderContext().setHasRightAssociate(false);
                    newRightJoin.getJoinReorderContext().setHasExchange(false);

                    LogicalJoin<LogicalJoin<GroupPlan, GroupPlan>, LogicalJoin<GroupPlan, GroupPlan>>
                            newTopJoin = new LogicalJoin<>(JoinType.INNER_JOIN,
                            newTopJoinHashJoinConjuncts, newTopJoinOtherJoinConjuncts,
                            newLeftJoin, newRightJoin, topJoin.getJoinReorderContext());
                    newTopJoin.getJoinReorderContext().setHasExchange(true);

                    return newTopJoin;
                }).toRule(RuleType.LOGICAL_JOIN_EXCHANGE);
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

    private void splitTopConditon(List<Expression> topCondition, Set<Slot> acOutputSet, Set<Slot> bdOutputSet,
            List<Expression> newLeftCondition, List<Expression> newRightCondition,
            List<Expression> remainTopCondition) {
        for (Expression hashJoinConjunct : topCondition) {
            Set<Slot> inputSlots = hashJoinConjunct.getInputSlots();
            if (acOutputSet.containsAll(inputSlots)) {
                newLeftCondition.add(hashJoinConjunct);
            } else if (bdOutputSet.containsAll(inputSlots)) {
                newRightCondition.add(hashJoinConjunct);
            } else {
                remainTopCondition.add(hashJoinConjunct);
            }
        }
    }
}
