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
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Rule for change inner join LAsscom (associative and commutive).
 */
public class InnerJoinLAsscomProject extends OneExplorationRuleFactory {
    public static final InnerJoinLAsscomProject INSTANCE = new InnerJoinLAsscomProject();

    /*
     *        topJoin                   newTopJoin
     *        /     \                   /        \
     *    project    C          newLeftProject newRightProject
     *      /            ──►          /            \
     * bottomJoin                newBottomJoin      B
     *    /   \                     /   \
     *   A     B                   A     C
     */
    @Override
    public Rule build() {
        return innerLogicalJoin(logicalProject(innerLogicalJoin()), group())
                .when(topJoin -> InnerJoinLAsscom.checkReorder(topJoin, topJoin.left().child()))
                .whenNot(join -> join.hasJoinHint() || join.left().child().hasJoinHint())
                .whenNot(join -> join.isMarkJoin() || join.left().child().isMarkJoin())
                .when(join -> join.left().isAllSlots())
                .then(topJoin -> {
                    /* ********** init ********** */
                    LogicalJoin<GroupPlan, GroupPlan> bottomJoin = topJoin.left().child();
                    GroupPlan a = bottomJoin.left();
                    GroupPlan b = bottomJoin.right();
                    GroupPlan c = topJoin.right();
                    Set<ExprId> bExprIdSet = b.getOutputExprIdSet();

                    /* ********** split Conjuncts ********** */
                    Map<Boolean, List<Expression>> splitHashConjuncts = splitConjuncts(
                            topJoin.getHashJoinConjuncts(), bottomJoin.getHashJoinConjuncts(), bExprIdSet);
                    List<Expression> newTopHashConjuncts = splitHashConjuncts.get(true);
                    List<Expression> newBottomHashConjuncts = splitHashConjuncts.get(false);
                    Map<Boolean, List<Expression>> splitOtherConjuncts = splitConjuncts(
                            topJoin.getOtherJoinConjuncts(), bottomJoin.getOtherJoinConjuncts(), bExprIdSet);
                    List<Expression> newTopOtherConjuncts = splitOtherConjuncts.get(true);
                    List<Expression> newBottomOtherConjuncts = splitOtherConjuncts.get(false);

                    if (newBottomOtherConjuncts.isEmpty() && newBottomHashConjuncts.isEmpty()) {
                        return null;
                    }

                    /* ********** new Plan ********** */
                    LogicalJoin<Plan, Plan> newBottomJoin = topJoin.withConjunctsChildren(newBottomHashConjuncts,
                            newBottomOtherConjuncts, a, c);

                    // merge newTopHashConjuncts newTopOtherConjuncts topJoin.getOutputExprIdSet()
                    Set<ExprId> topUsedExprIds = new HashSet<>(topJoin.getOutputExprIdSet());
                    newTopHashConjuncts.forEach(expr -> topUsedExprIds.addAll(expr.getInputSlotExprIds()));
                    newTopOtherConjuncts.forEach(expr -> topUsedExprIds.addAll(expr.getInputSlotExprIds()));
                    Plan left = CBOUtils.newProject(topUsedExprIds, newBottomJoin);
                    Plan right = CBOUtils.newProject(topUsedExprIds, b);

                    LogicalJoin<Plan, Plan> newTopJoin = bottomJoin.withConjunctsChildren(newTopHashConjuncts,
                            newTopOtherConjuncts, left, right);
                    newTopJoin.getJoinReorderContext().copyFrom(topJoin.getJoinReorderContext());
                    newTopJoin.getJoinReorderContext().setHasLAsscom(true);

                    return CBOUtils.projectOrSelf(ImmutableList.copyOf(topJoin.getOutput()), newTopJoin);
                }).toRule(RuleType.LOGICAL_INNER_JOIN_LASSCOM_PROJECT);
    }

    /**
     * Split Condition into two part.
     * True: contains B.
     * False: just contains A C.
     */
    private Map<Boolean, List<Expression>> splitConjuncts(List<Expression> topConjuncts,
            List<Expression> bottomConjuncts, Set<ExprId> bExprIdSet) {
        // top: (A B)(error) (A C) (B C) (A B C)
        // Split topJoin Condition to two part according to include B.
        Map<Boolean, List<Expression>> splitOn = topConjuncts.stream()
                .collect(Collectors.partitioningBy(topHashOn -> {
                    Set<ExprId> usedExprIds = topHashOn.getInputSlotExprIds();
                    return Utils.isIntersecting(usedExprIds, bExprIdSet);
                }));
        // * don't include B, just include (A C)
        // we add it into newBottomJoin HashConjuncts.
        // * include B, include (A B C) or (A B)
        // we add it into newTopJoin HashConjuncts.
        List<Expression> newTopHashConjuncts = splitOn.get(true);
        newTopHashConjuncts.addAll(bottomConjuncts);

        return splitOn;
    }
}
