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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Push the predicate in the LogicalFilter or LogicalJoin to the join children.
 * todo: Now, only support eq on condition for inner join, support other case later
 */
public class PushPredicateThroughJoin extends OneRewriteRuleFactory {
    /*
     * For example:
     * select a.k1,b.k1 from a join b on a.k1 = b.k1 and a.k2 > 2 and b.k2 > 5 where a.k1 > 1 and b.k1 > 2
     * Logical plan tree:
     *                 project
     *                   |
     *                filter (a.k1 > 1 and b.k1 > 2)
     *                   |
     *                join (a.k1 = b.k1 and a.k2 > 2 and b.k2 > 5)
     *                 /   \
     *              scan  scan
     * transformed:
     *                      project
     *                        |
     *                join (a.k1 = b.k1)
     *                /                \
     * filter(a.k1 > 1 and a.k2 > 2 )   filter(b.k1 > 2 and b.k2 > 5)
     *             |                                    |
     *            scan                                scan
     */
    @Override
    public Rule build() {
        return logicalFilter(innerLogicalJoin()).then(filter -> {

            LogicalJoin<GroupPlan, GroupPlan> join = filter.child();

            Expression wherePredicates = filter.getPredicates();
            Expression onPredicates = join.getOtherJoinCondition().orElse(BooleanLiteral.TRUE);

            List<Expression> otherConditions = Lists.newArrayList();
            List<Expression> eqConditions = Lists.newArrayList();

            List<Slot> leftInput = join.left().getOutput();
            List<Slot> rightInput = join.right().getOutput();

            ExpressionUtils.extractConjunction(ExpressionUtils.and(onPredicates, wherePredicates))
                    .forEach(predicate -> {
                        if (Objects.nonNull(getJoinCondition(predicate, leftInput, rightInput))) {
                            eqConditions.add(predicate);
                        } else {
                            otherConditions.add(predicate);
                        }
                    });

            List<Expression> leftPredicates = Lists.newArrayList();
            List<Expression> rightPredicates = Lists.newArrayList();

            for (Expression p : otherConditions) {
                Set<Slot> slots = p.getInputSlots();
                if (slots.isEmpty()) {
                    leftPredicates.add(p);
                    rightPredicates.add(p);
                    continue;
                }
                if (leftInput.containsAll(slots)) {
                    leftPredicates.add(p);
                }
                if (rightInput.containsAll(slots)) {
                    rightPredicates.add(p);
                }
            }

            otherConditions.removeAll(leftPredicates);
            otherConditions.removeAll(rightPredicates);
            otherConditions.addAll(eqConditions);

            return pushDownPredicate(join, otherConditions, leftPredicates, rightPredicates);
        }).toRule(RuleType.PUSH_DOWN_PREDICATE_THROUGH_JOIN);
    }

    private Plan pushDownPredicate(LogicalJoin<GroupPlan, GroupPlan> joinPlan,
            List<Expression> joinConditions, List<Expression> leftPredicates, List<Expression> rightPredicates) {

        Expression left = ExpressionUtils.and(leftPredicates);
        Expression right = ExpressionUtils.and(rightPredicates);
        //todo expr should optimize again using expr rewrite
        Plan leftPlan = joinPlan.left();
        Plan rightPlan = joinPlan.right();
        if (!left.equals(BooleanLiteral.TRUE)) {
            leftPlan = new LogicalFilter(left, leftPlan);
        }

        if (!right.equals(BooleanLiteral.TRUE)) {
            rightPlan = new LogicalFilter(right, rightPlan);
        }

        return new LogicalJoin<>(joinPlan.getJoinType(), joinPlan.getHashJoinConjuncts(),
                Optional.of(ExpressionUtils.and(joinConditions)), leftPlan, rightPlan);
    }

    private Expression getJoinCondition(Expression predicate, List<Slot> leftOutputs, List<Slot> rightOutputs) {
        if (!(predicate instanceof ComparisonPredicate)) {
            return null;
        }

        ComparisonPredicate comparison = (ComparisonPredicate) predicate;

        Set<Slot> leftSlots = comparison.left().getInputSlots();
        Set<Slot> rightSlots = comparison.right().getInputSlots();

        if (!(leftSlots.size() >= 1 && rightSlots.size() >= 1)) {
            return null;
        }

        Set<Slot> left = Sets.newLinkedHashSet(leftOutputs);
        Set<Slot> right = Sets.newLinkedHashSet(rightOutputs);

        if ((left.containsAll(leftSlots) && right.containsAll(rightSlots)) || (left.containsAll(rightSlots)
                && right.containsAll(leftSlots))) {
            return predicate;
        }

        return null;
    }
}
