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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Rule for change inner join LAsscom (associative and commutive).
 */
public class InnerJoinLAsscom extends OneExplorationRuleFactory {
    public static final InnerJoinLAsscom INSTANCE = new InnerJoinLAsscom();

    /*
     *      topJoin                newTopJoin
     *      /     \                 /     \
     * bottomJoin  C   -->  newBottomJoin  B
     *  /    \                  /    \
     * A      B                A      C
     */
    @Override
    public Rule build() {
        return innerLogicalJoin(innerLogicalJoin(), group())
                .when(topJoin -> checkReorder(topJoin, topJoin.left()))
                .then(topJoin -> {
                    LogicalJoin<GroupPlan, GroupPlan> bottomJoin = topJoin.left();
                    GroupPlan a = bottomJoin.left();
                    GroupPlan b = bottomJoin.right();
                    GroupPlan c = topJoin.right();

                    // split HashJoinConjuncts.
                    Map<Boolean, List<Expression>> splitHashConjunts = splitConjuncts(topJoin.getHashJoinConjuncts(),
                            bottomJoin, bottomJoin.getHashJoinConjuncts());
                    List<Expression> newTopHashConjuncts = splitHashConjunts.get(true);
                    List<Expression> newBottomHashConjuncts = splitHashConjunts.get(false);
                    Preconditions.checkState(!newTopHashConjuncts.isEmpty(),
                            "LAsscom newTopHashJoinConjuncts join can't empty");
                    if (newBottomHashConjuncts.size() == 0) {
                        return null;
                    }

                    // split OtherJoinConjuncts.
                    Map<Boolean, List<Expression>> splitOtherConjunts = splitConjuncts(topJoin.getOtherJoinConjuncts(),
                            bottomJoin, bottomJoin.getOtherJoinConjuncts());
                    List<Expression> newTopOtherConjuncts = splitOtherConjunts.get(true);
                    List<Expression> newBottomOtherConjuncts = splitOtherConjunts.get(false);

                    LogicalJoin<GroupPlan, GroupPlan> newBottomJoin = new LogicalJoin<>(JoinType.INNER_JOIN,
                            newBottomHashConjuncts, newBottomOtherConjuncts,
                            a, c, bottomJoin.getJoinReorderContext());
                    newBottomJoin.getJoinReorderContext().setHasLAsscom(false);
                    newBottomJoin.getJoinReorderContext().setHasCommute(false);

                    LogicalJoin<LogicalJoin<GroupPlan, GroupPlan>, GroupPlan> newTopJoin = new LogicalJoin<>(
                            JoinType.INNER_JOIN, newTopHashConjuncts, newTopOtherConjuncts,
                            newBottomJoin, b, topJoin.getJoinReorderContext());
                    newTopJoin.getJoinReorderContext().setHasLAsscom(true);

                    return newTopJoin;
                }).toRule(RuleType.LOGICAL_INNER_JOIN_LASSCOM);
    }

    public static boolean checkReorder(LogicalJoin<? extends Plan, GroupPlan> topJoin,
            LogicalJoin<GroupPlan, GroupPlan> bottomJoin) {
        return !bottomJoin.getJoinReorderContext().hasCommuteZigZag()
                && !topJoin.getJoinReorderContext().hasLAsscom();
    }

    /**
     * Split onCondition into two part.
     * True: contains B.
     * False: just contains A C.
     */
    private static Map<Boolean, List<Expression>> splitConjuncts(List<Expression> topConjuncts,
            LogicalJoin<GroupPlan, GroupPlan> bottomJoin, List<Expression> bottomConjuncts) {
        // top: (A B)(error) (A C) (B C) (A B C)
        // Split topJoin hashCondition to two part according to include B.
        Map<Boolean, List<Expression>> splitOn = topConjuncts.stream()
                .collect(Collectors.partitioningBy(topHashOn -> {
                    Set<Slot> usedSlot = topHashOn.getInputSlots();
                    Set<Slot> bOutputSet = bottomJoin.right().getOutputSet();
                    return ExpressionUtils.isIntersecting(bOutputSet, usedSlot);
                }));
        // * don't include B, just include (A C)
        // we add it into newBottomJoin HashJoinConjuncts.
        // * include B, include (A B C) or (A B)
        // we add it into newTopJoin HashJoinConjuncts.
        List<Expression> newTopHashJoinConjuncts = splitOn.get(true);
        newTopHashJoinConjuncts.addAll(bottomConjuncts);

        return splitOn;
    }
}
