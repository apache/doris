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
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Push the predicate in the LogicalFilter to the join children.
 */
public class PushPredicatesThroughJoin extends OneRewriteRuleFactory {

    private static final ImmutableList<JoinType> NEED_RESERVE_LEFT = ImmutableList.of(
            JoinType.RIGHT_OUTER_JOIN,
            JoinType.RIGHT_ANTI_JOIN,
            JoinType.FULL_OUTER_JOIN
    );

    private static final ImmutableList<JoinType> NEED_RESERVE_RIGHT = ImmutableList.of(
            JoinType.LEFT_OUTER_JOIN,
            JoinType.LEFT_ANTI_JOIN,
            JoinType.FULL_OUTER_JOIN
    );

    /*
     * For example:
     * select a.k1,b.k1 from a join b on a.k1 = b.k1 and a.k2 > 2 and b.k2 > 5 where a.k1 > 1 and b.k1 > 2
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

            Expression filterPredicates = filter.getPredicates();

            List<Expression> filterConditions = Lists.newArrayList();
            List<Expression> joinConditions = Lists.newArrayList();

            Set<Slot> leftInput = join.left().getOutputSet();
            Set<Slot> rightInput = join.right().getOutputSet();

            ExpressionUtils.extractConjunction(filterPredicates)
                    .forEach(predicate -> {
                        if (Objects.nonNull(getJoinCondition(predicate, leftInput, rightInput))) {
                            joinConditions.add(predicate);
                        } else {
                            filterConditions.add(predicate);
                        }
                    });

            List<Expression> leftPredicates = Lists.newArrayList();
            List<Expression> rightPredicates = Lists.newArrayList();

            for (Expression p : filterConditions) {
                Set<Slot> slots = p.getInputSlots();
                if (slots.isEmpty()) {
                    leftPredicates.add(p);
                    rightPredicates.add(p);
                    continue;
                }
                if (leftInput.containsAll(slots) && !NEED_RESERVE_LEFT.contains(join.getJoinType())) {
                    leftPredicates.add(p);
                }
                if (rightInput.containsAll(slots) && !NEED_RESERVE_RIGHT.contains(join.getJoinType())) {
                    rightPredicates.add(p);
                }
            }

            if (!NEED_RESERVE_LEFT.contains(join.getJoinType())) {
                filterConditions.removeAll(leftPredicates);
            }
            if (!NEED_RESERVE_RIGHT.contains(join.getJoinType())) {
                filterConditions.removeAll(rightPredicates);
            }
            join.getOtherJoinCondition().map(joinConditions::add);

            return PlanUtils.filterOrSelf(filterConditions,
                    pushDownPredicate(join, joinConditions, leftPredicates, rightPredicates));
        }).toRule(RuleType.PUSH_DOWN_PREDICATE_THROUGH_JOIN);
    }

    private Plan pushDownPredicate(LogicalJoin<GroupPlan, GroupPlan> join,
            List<Expression> joinConditions, List<Expression> leftPredicates, List<Expression> rightPredicates) {
        // todo expr should optimize again using expr rewrite
        Plan leftPlan = PlanUtils.filterOrSelf(leftPredicates, join.left());
        Plan rightPlan = PlanUtils.filterOrSelf(rightPredicates, join.right());

        return new LogicalJoin<>(join.getJoinType(), join.getHashJoinConjuncts(),
                ExpressionUtils.optionalAnd(joinConditions), leftPlan, rightPlan);
    }

    private Expression getJoinCondition(Expression predicate, Set<Slot> leftOutputs, Set<Slot> rightOutputs) {
        if (!(predicate instanceof ComparisonPredicate)) {
            return null;
        }

        ComparisonPredicate comparison = (ComparisonPredicate) predicate;

        if (!(comparison instanceof EqualTo)) {
            return null;
        }

        Set<Slot> leftSlots = comparison.left().getInputSlots();
        Set<Slot> rightSlots = comparison.right().getInputSlots();

        if ((leftOutputs.containsAll(leftSlots) && rightOutputs.containsAll(rightSlots))
                || (leftOutputs.containsAll(rightSlots) && rightOutputs.containsAll(leftSlots))) {
            return predicate;
        }

        return null;
    }
}
