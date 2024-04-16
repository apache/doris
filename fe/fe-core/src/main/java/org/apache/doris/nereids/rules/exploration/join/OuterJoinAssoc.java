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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

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
                .thenApply(ctx -> {
                    LogicalJoin<LogicalJoin<GroupPlan, GroupPlan>, GroupPlan> topJoin = ctx.root;
                    LogicalJoin<GroupPlan, GroupPlan> bottomJoin = topJoin.left();
                    GroupPlan a = bottomJoin.left();
                    GroupPlan b = bottomJoin.right();
                    GroupPlan c = topJoin.right();

                    /*
                     * Paper `On the Correct and Complete Enumeration of the Core Search Space`.
                     * p23 need to reject nulls on A(e2) (Eqv. 1).
                     * It means that when slot is null, condition must return false or unknown.
                     */
                    if (bottomJoin.getJoinType().isLeftOuterJoin() && topJoin.getJoinType().isLeftOuterJoin()) {
                        Set<Slot> conditionSlot = topJoin.getConditionSlot();
                        Set<Expression> on = ImmutableSet.<Expression>builder()
                                .addAll(topJoin.getHashJoinConjuncts())
                                .addAll(topJoin.getOtherJoinConjuncts()).build();
                        Set<Slot> notNullSlots = ExpressionUtils.inferNotNullSlots(on,
                                ctx.cascadesContext);
                        if (conditionSlot.isEmpty() || !conditionSlot.equals(notNullSlots)) {
                            return null;
                        }
                    }

                    LogicalJoin newBottomJoin = topJoin.withChildrenNoContext(b, c, null);
                    newBottomJoin.getJoinReorderContext().copyFrom(bottomJoin.getJoinReorderContext());
                    LogicalJoin newTopJoin = bottomJoin.withChildrenNoContext(a, newBottomJoin, null);
                    newTopJoin.getJoinReorderContext().copyFrom(topJoin.getJoinReorderContext());
                    setReorderContext(newTopJoin, newBottomJoin);
                    return newTopJoin;
                }).toRule(RuleType.LOGICAL_OUTER_JOIN_ASSOC);
    }

    /**
     * just allow: top (B C), bottom (A B), we can exchange HashConjunct directly.
     * <p>
     * Same with OtherJoinConjunct.
     */
    public static boolean checkCondition(LogicalJoin<? extends Plan, GroupPlan> topJoin, Set<Slot> aOutputSet) {
        return Stream.concat(
                        topJoin.getHashJoinConjuncts().stream(),
                        topJoin.getOtherJoinConjuncts().stream())
                .allMatch(expr -> {
                    Set<Slot> usedSlot = expr.collect(SlotReference.class::isInstance);
                    return !Utils.isIntersecting(usedSlot, aOutputSet);
                });
    }

    /**
     * Set the reorder context for the new join.
     */
    public static void setReorderContext(LogicalJoin topJoin, LogicalJoin bottomJoin) {
        bottomJoin.getJoinReorderContext().setHasCommute(false);
        bottomJoin.getJoinReorderContext().setHasRightAssociate(false);
        bottomJoin.getJoinReorderContext().setHasLeftAssociate(false);
        bottomJoin.getJoinReorderContext().setHasExchange(false);

        topJoin.getJoinReorderContext().setHasRightAssociate(true);
        topJoin.getJoinReorderContext().setHasCommute(false);
    }
}
