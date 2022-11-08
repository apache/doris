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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Set;

/**
 * Push the predicate in the LogicalFilter to the join children.
 */
public class PushdownFilterThroughJoin extends OneRewriteRuleFactory {
    public static final PushdownFilterThroughJoin INSTANCE = new PushdownFilterThroughJoin();

    private static final ImmutableList<JoinType> COULD_PUSH_THROUGH_LEFT = ImmutableList.of(
            JoinType.INNER_JOIN,
            JoinType.LEFT_OUTER_JOIN,
            JoinType.LEFT_SEMI_JOIN,
            JoinType.LEFT_ANTI_JOIN,
            JoinType.CROSS_JOIN
    );

    private static final ImmutableList<JoinType> COULD_PUSH_THROUGH_RIGHT = ImmutableList.of(
            JoinType.INNER_JOIN,
            JoinType.RIGHT_OUTER_JOIN,
            JoinType.RIGHT_SEMI_JOIN,
            JoinType.RIGHT_ANTI_JOIN,
            JoinType.CROSS_JOIN
    );

    private static final ImmutableList<JoinType> COULD_PUSH_INSIDE = ImmutableList.of(
            JoinType.INNER_JOIN
    );

    /*
     * For example:
     * select a.k1, b.k1 from a join b on a.k1 = b.k1 and a.k2 > 2 and b.k2 > 5
     *     where a.k1 > 1 and b.k1 > 2 and a.k2 > b.k2
     *
     * TODO(jakevin): following graph is wrong, we should add a new rule to extract
     *   a.k2 > 2 and b.k2 > 5, and pushdown.
     *
     * Logical plan tree:
     *                 project
     *                   |
     *                filter (a.k1 > 1 and b.k1 > 2 and a.k2 > b.k2)
     *                   |
     *                join (a.k1 = b.k1 and a.k2 > 2 and b.k2 > 5)
     *                 /   \
     *              scan  scan
     * transformed:
     *                      project
     *                        |
     *                filter(a.k2 > b.k2)
     *                        |
     *           join (otherConditions: a.k1 = b.k1)
     *                /                \
     * filter(a.k1 > 1 and a.k2 > 2)   filter(b.k1 > 2 and b.k2 > 5)
     *             |                                    |
     *            scan                                scan
     */
    @Override
    public Rule build() {
        return logicalFilter(logicalJoin()).then(filter -> {

            LogicalJoin<GroupPlan, GroupPlan> join = filter.child();

            List<Expression> predicates = ExpressionUtils.extractConjunction(filter.getPredicates());

            List<Expression> filterPredicates = Lists.newArrayList();
            List<Expression> joinConditions = Lists.newArrayList();

            Set<Slot> leftInput = join.left().getOutputSet();
            Set<Slot> rightInput = join.right().getOutputSet();

            // TODO: predicate slotReference should be not nullable.
            for (Expression predicate : predicates) {
                if (convertJoinCondition(predicate, leftInput, rightInput, join.getJoinType())) {
                    joinConditions.add(predicate);
                } else {
                    filterPredicates.add(predicate);
                }
            }

            List<Expression> leftPredicates = Lists.newArrayList();
            List<Expression> rightPredicates = Lists.newArrayList();
            List<Expression> remainingPredicates = Lists.newArrayList();
            for (Expression p : filterPredicates) {
                Set<Slot> slots = p.collect(SlotReference.class::isInstance);
                if (slots.isEmpty()) {
                    leftPredicates.add(p);
                    rightPredicates.add(p);
                    continue;
                }
                if (leftInput.containsAll(slots) && COULD_PUSH_THROUGH_LEFT.contains(join.getJoinType())) {
                    leftPredicates.add(p);
                } else if (rightInput.containsAll(slots) && COULD_PUSH_THROUGH_RIGHT.contains(join.getJoinType())) {
                    rightPredicates.add(p);
                } else {
                    remainingPredicates.add(p);
                }
            }

            joinConditions.addAll(join.getOtherJoinConjuncts());

            return PlanUtils.filterOrSelf(remainingPredicates,
                    new LogicalJoin<>(join.getJoinType(),
                            join.getHashJoinConjuncts(),
                            joinConditions,
                            PlanUtils.filterOrSelf(leftPredicates, join.left()),
                            PlanUtils.filterOrSelf(rightPredicates, join.right())));
        }).toRule(RuleType.PUSHDOWN_FILTER_THROUGH_JOIN);
    }

    private boolean convertJoinCondition(Expression predicate, Set<Slot> leftOutputs, Set<Slot> rightOutputs,
            JoinType joinType) {
        if (!COULD_PUSH_INSIDE.contains(joinType)) {
            return false;
        }
        if (!(predicate instanceof EqualTo)) {
            return false;
        }

        EqualTo equalTo = (EqualTo) predicate;

        Set<Slot> leftSlots = equalTo.left().collect(SlotReference.class::isInstance);
        Set<Slot> rightSlots = equalTo.right().collect(SlotReference.class::isInstance);

        if (leftSlots.size() == 0 || rightSlots.size() == 0) {
            return false;
        }

        if ((leftOutputs.containsAll(leftSlots) && rightOutputs.containsAll(rightSlots))
                || (leftOutputs.containsAll(rightSlots) && rightOutputs.containsAll(leftSlots))) {
            return true;
        }

        return false;
    }
}
