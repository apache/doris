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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.nereids.util.TypeUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Eliminate outer join.
 */
public class EliminateOuterJoin extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalFilter(
                logicalJoin().when(join -> join.getJoinType().isOuterJoin())
        ).then(filter -> {
            LogicalJoin<Plan, Plan> join = filter.child();

            Builder<Expression> conjunctsBuilder = ImmutableSet.builder();
            Set<Slot> notNullSlots = new HashSet<>();
            for (Expression predicate : filter.getConjuncts()) {
                Optional<Slot> notNullSlot = TypeUtils.isNotNull(predicate);
                if (notNullSlot.isPresent()) {
                    notNullSlots.add(notNullSlot.get());
                } else {
                    conjunctsBuilder.add(predicate);
                }
            }
            boolean canFilterLeftNull = Utils.isIntersecting(join.left().getOutputSet(), notNullSlots);
            boolean canFilterRightNull = Utils.isIntersecting(join.right().getOutputSet(), notNullSlots);
            if (!canFilterRightNull && !canFilterLeftNull) {
                return null;
            }

            JoinType newJoinType = tryEliminateOuterJoin(join.getJoinType(), canFilterLeftNull, canFilterRightNull);
            Set<Expression> conjuncts = Sets.newHashSet();
            conjuncts.addAll(filter.getConjuncts());
            boolean conjunctsChanged = false;
            if (!notNullSlots.isEmpty()) {
                for (Slot slot : notNullSlots) {
                    Not isNotNull = new Not(new IsNull(slot), true);
                    conjunctsChanged |= conjuncts.add(isNotNull);
                }
            }
            if (newJoinType.isInnerJoin()) {
                /*
                 * for example: (A left join B on A.a=B.b) join C on B.x=C.x
                 * inner join condition B.x=C.x implies 'B.x is not null',
                 * by which the left outer join could be eliminated. Finally, the join transformed to
                 * (A join B on A.a=B.b) join C on B.x=C.x.
                 * This elimination can be processed recursively.
                 *
                 * TODO: is_not_null can also be inferred from A < B and so on
                 */
                conjunctsChanged |= join.getEqualToConjuncts().stream()
                        .map(EqualTo.class::cast)
                        .map(equalTo -> JoinUtils.swapEqualToForChildrenOrder(equalTo, join.left().getOutputSet()))
                        .anyMatch(equalTo -> createIsNotNullIfNecessary(equalTo, conjuncts));

                JoinUtils.JoinSlotCoverageChecker checker = new JoinUtils.JoinSlotCoverageChecker(
                        join.left().getOutput(),
                        join.right().getOutput());
                conjunctsChanged |= join.getOtherJoinConjuncts().stream()
                        .filter(EqualTo.class::isInstance)
                        .filter(equalTo -> checker.isHashJoinCondition((EqualPredicate) equalTo))
                        .map(equalTo -> JoinUtils.swapEqualToForChildrenOrder((EqualPredicate) equalTo,
                                join.left().getOutputSet()))
                        .anyMatch(equalTo -> createIsNotNullIfNecessary(equalTo, conjuncts));
            }
            if (conjunctsChanged) {
                return filter.withConjuncts(conjuncts.stream().collect(ImmutableSet.toImmutableSet()))
                        .withChildren(join.withJoinTypeAndContext(newJoinType, join.getJoinReorderContext()));
            }
            return filter.withChildren(join.withJoinTypeAndContext(newJoinType, join.getJoinReorderContext()));
        }).toRule(RuleType.ELIMINATE_OUTER_JOIN);
    }

    private JoinType tryEliminateOuterJoin(JoinType joinType, boolean canFilterLeftNull, boolean canFilterRightNull) {
        if (joinType.isRightOuterJoin() && canFilterLeftNull) {
            return JoinType.INNER_JOIN;
        }
        if (joinType.isLeftOuterJoin() && canFilterRightNull) {
            return JoinType.INNER_JOIN;
        }
        if (joinType.isFullOuterJoin() && canFilterLeftNull && canFilterRightNull) {
            return JoinType.INNER_JOIN;
        }
        if (joinType.isFullOuterJoin() && canFilterLeftNull) {
            return JoinType.LEFT_OUTER_JOIN;
        }
        if (joinType.isFullOuterJoin() && canFilterRightNull) {
            return JoinType.RIGHT_OUTER_JOIN;
        }
        return joinType;
    }

    private boolean createIsNotNullIfNecessary(EqualPredicate swapedEqualTo, Collection<Expression> container) {
        boolean containerChanged = false;
        if (swapedEqualTo.left().nullable()) {
            Not not = new Not(new IsNull(swapedEqualTo.left()), true);
            containerChanged |= container.add(not);
        }
        if (swapedEqualTo.right().nullable()) {
            Not not = new Not(new IsNull(swapedEqualTo.right()), true);
            containerChanged |= container.add(not);
        }
        return containerChanged;
    }
}
