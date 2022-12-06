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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.OneExplorationRuleFactory;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableSet;

import java.util.Set;
import java.util.stream.Stream;

/**
 * OuterJoinAssoc.
 */
public class OuterJoinAssoc extends OneExplorationRuleFactory {
    /*
     *        topJoin        newTopJoin
     *        /     \         /     \
     *   bottomJoin  C  ->   A   newBottomJoin
     *    /    \                     /    \
     *   A      B                   B      C
     */
    public static final OuterJoinAssoc INSTANCE = new OuterJoinAssoc();

    public static Set<Pair<JoinType, JoinType>> VALID_TYPE_PAIR_SET = ImmutableSet.of(
            Pair.of(JoinType.INNER_JOIN, JoinType.LEFT_OUTER_JOIN),
            Pair.of(JoinType.LEFT_OUTER_JOIN, JoinType.LEFT_OUTER_JOIN));

    @Override
    public Rule build() {
        return logicalJoin(logicalJoin(), group())
                .when(join -> VALID_TYPE_PAIR_SET.contains(Pair.of(join.left().getJoinType(), join.getJoinType())))
                .when(topJoin -> OuterJoinLAsscom.checkReorder(topJoin, topJoin.left()))
                .when(topJoin -> checkCondition(topJoin, topJoin.left().left().getOutputSet()))
                .then(topJoin -> {
                    LogicalJoin<GroupPlan, GroupPlan> bottomJoin = topJoin.left();
                    GroupPlan a = bottomJoin.left();
                    GroupPlan b = bottomJoin.right();
                    GroupPlan c = topJoin.right();

                    /*
                     * p23 need to rejects nulls on A(e2) (Eqv. 1)
                     * see paper `On the Correct and Complete Enumeration of the Core Search Space`.
                     * But because we have added eliminate_outer_rule, we don't need to consider this.
                     */

                    LogicalJoin<GroupPlan, GroupPlan> newBottomJoin = new LogicalJoin<>(topJoin.getJoinType(),
                            topJoin.getHashJoinConjuncts(), topJoin.getOtherJoinConjuncts(),
                            b, c, bottomJoin.getJoinReorderContext());
                    newBottomJoin.getJoinReorderContext().setHasCommute(false);
                    newBottomJoin.getJoinReorderContext().setHasRightAssociate(false);
                    newBottomJoin.getJoinReorderContext().setHasLeftAssociate(false);
                    newBottomJoin.getJoinReorderContext().setHasExchange(false);

                    LogicalJoin<GroupPlan, LogicalJoin<GroupPlan, GroupPlan>> newTopJoin
                            = new LogicalJoin<>(
                            bottomJoin.getJoinType(),
                            bottomJoin.getHashJoinConjuncts(), bottomJoin.getOtherJoinConjuncts(),
                            a, newBottomJoin, topJoin.getJoinReorderContext());
                    newTopJoin.getJoinReorderContext().setHasRightAssociate(true);
                    newTopJoin.getJoinReorderContext().setHasCommute(false);

                    return newTopJoin;
                }).toRule(RuleType.LOGICAL_OUTER_JOIN_LASSCOM);
    }

    /**
     * if is LOJ-LOJ:
     * just allow: top (B C), bottom (A B), we can exchange HashConjunct directly.
     * <p>
     * Same with OtherJoinConjunct.
     * TODO:
     * if is LOJ-INNER
     * When top (A B C), we can split top.
     * Top can (A C) and (B C)
     * Bottom just (A B)
     */
    private boolean checkCondition(LogicalJoin<? extends Plan, GroupPlan> topJoin, Set<Slot> aOutputSet) {
        return Stream.concat(
                        topJoin.getHashJoinConjuncts().stream(),
                        topJoin.getOtherJoinConjuncts().stream())
                .allMatch(expr -> {
                    Set<Slot> usedSlot = expr.collect(SlotReference.class::isInstance);
                    return !ExpressionUtils.isIntersecting(usedSlot, aOutputSet);
                });
    }
}
