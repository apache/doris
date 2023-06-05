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
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableSet;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Rule for change inner join LAsscom (associative and commutive).
 * TODO Future:
 * LeftOuter-LeftOuter can allow topHashConjunct (A B) and (AC)
 */
public class OuterJoinLAsscom extends OneExplorationRuleFactory {
    public static final OuterJoinLAsscom INSTANCE = new OuterJoinLAsscom();

    // Pair<bottomJoin, topJoin>
    // newBottomJoin Type = topJoin Type, newTopJoin Type = bottomJoin Type
    public static Set<Pair<JoinType, JoinType>> VALID_TYPE_PAIR_SET = ImmutableSet.of(
            Pair.of(JoinType.LEFT_OUTER_JOIN, JoinType.INNER_JOIN),
            Pair.of(JoinType.INNER_JOIN, JoinType.LEFT_OUTER_JOIN),
            Pair.of(JoinType.LEFT_OUTER_JOIN, JoinType.LEFT_OUTER_JOIN));

    /*
     *      topJoin                newTopJoin
     *      /     \                 /     \
     * bottomJoin  C   -->  newBottomJoin  B
     *  /    \                  /    \
     * A      B                A      C
     */
    @Override
    public Rule build() {
        return logicalJoin(logicalJoin(), group())
                .when(join -> VALID_TYPE_PAIR_SET.contains(Pair.of(join.left().getJoinType(), join.getJoinType())))
                .when(topJoin -> checkReorder(topJoin, topJoin.left()))
                .whenNot(join -> join.hasJoinHint() || join.left().hasJoinHint())
                .when(topJoin -> checkCondition(topJoin, topJoin.left().right().getOutputExprIdSet()))
                .whenNot(LogicalJoin::isMarkJoin)
                .then(topJoin -> {
                    LogicalJoin<GroupPlan, GroupPlan> bottomJoin = topJoin.left();
                    GroupPlan a = bottomJoin.left();
                    GroupPlan b = bottomJoin.right();
                    GroupPlan c = topJoin.right();

                    LogicalJoin newBottomJoin = topJoin.withChildrenNoContext(a, c);
                    newBottomJoin.getJoinReorderContext().copyFrom(bottomJoin.getJoinReorderContext());
                    newBottomJoin.getJoinReorderContext().setHasLAsscom(false);
                    newBottomJoin.getJoinReorderContext().setHasCommute(false);

                    LogicalJoin newTopJoin = bottomJoin.withChildrenNoContext(newBottomJoin, b);
                    newTopJoin.getJoinReorderContext().copyFrom(topJoin.getJoinReorderContext());
                    newTopJoin.getJoinReorderContext().setHasLAsscom(true);

                    return newTopJoin;
                }).toRule(RuleType.LOGICAL_OUTER_JOIN_LASSCOM);
    }

    /**
     * topHashConjunct possibility: (A B) (A C) (B C) (A B C).
     * (A B) is forbidden, because it should be in bottom join.
     * (B C) (A B C) check failed, because it contains B.
     * So, just allow: top (A C), bottom (A B), we can exchange HashConjunct directly.
     * <p>
     * Same with OtherJoinConjunct.
     */
    public static boolean checkCondition(LogicalJoin<? extends Plan, GroupPlan> topJoin, Set<ExprId> bOutputExprIdSet) {
        return Stream.concat(
                        topJoin.getHashJoinConjuncts().stream(),
                        topJoin.getOtherJoinConjuncts().stream())
                .allMatch(expr -> {
                    Set<ExprId> usedExprIdSet = expr.<Set<SlotReference>>collect(SlotReference.class::isInstance)
                            .stream()
                            .map(SlotReference::getExprId)
                            .collect(Collectors.toSet());
                    return !Utils.isIntersecting(usedExprIdSet, bOutputExprIdSet);
                });
    }

    /**
     * check join reorder masks.
     */
    public static boolean checkReorder(LogicalJoin<? extends Plan, GroupPlan> topJoin,
            LogicalJoin<GroupPlan, GroupPlan> bottomJoin) {
        // hasCommute will cause to lack of OuterJoinAssocRule:Left
        return !topJoin.getJoinReorderContext().hasLAsscom()
                && !topJoin.getJoinReorderContext().hasLeftAssociate()
                && !topJoin.getJoinReorderContext().hasRightAssociate()
                && !topJoin.getJoinReorderContext().hasExchange()
                && !bottomJoin.getJoinReorderContext().hasCommute();
    }
}
