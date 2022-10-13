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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Lists;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Try to eliminate cross join via finding join conditions in filters and change the join orders.
 * <p>
 * <pre>
 * For example:
 *
 * input:
 * SELECT * FROM t1, t2, t3 WHERE t1.id=t3.id AND t2.id=t3.id
 *
 * output:
 * SELECT * FROM t1 JOIN t3 ON t1.id=t3.id JOIN t2 ON t2.id=t3.id
 * </pre>
 * </p>
 * TODO: This is tested by SSB queries currently, add more `unit` test for this rule
 * when we have a plan building and comparing framework.
 */
public class ReorderJoin extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalFilter(subTree(LogicalJoin.class, LogicalFilter.class)).thenApply(ctx -> {
            LogicalFilter<Plan> filter = ctx.root;

            MultiJoin multiJoin = (MultiJoin) joinToMultiJoin(filter);
            Plan plan = multiJoinToJoin(multiJoin);
            return plan;
        }).toRule(RuleType.REORDER_JOIN);
    }

    /**
     * Recursively convert to
     * {@link LogicalJoin} or
     * {@link LogicalFilter}--{@link LogicalJoin}
     * --> {@link MultiJoin}
     */
    public Plan joinToMultiJoin(Plan plan) {
        // subtree can't specify the end of Pattern. so end can be GroupPlan or Filter
        if (plan instanceof GroupPlan
                || (plan instanceof LogicalFilter && plan.child(0) instanceof GroupPlan)) {
            return plan;
        }

        List<Plan> inputs = Lists.newArrayList();
        List<Expression> joinFilter = Lists.newArrayList();
        List<Expression> notInnerJoinConditions = Lists.newArrayList();

        LogicalJoin<?, ?> join;
        if (plan instanceof LogicalFilter) {
            LogicalFilter<?> filter = (LogicalFilter<?>) plan;
            joinFilter.addAll(ExpressionUtils.extractConjunction(filter.getPredicates()));
            join = (LogicalJoin<?, ?>) filter.child();
        } else {
            join = (LogicalJoin<?, ?>) plan;
        }

        if (join.getJoinType().isInnerOrCrossJoin()) {
            joinFilter.addAll(join.getHashJoinConjuncts());
            joinFilter.addAll(join.getOtherJoinConjuncts());
        } else {
            notInnerJoinConditions.addAll(join.getHashJoinConjuncts());
            notInnerJoinConditions.addAll(join.getOtherJoinConjuncts());
        }

        // recursively convert children.
        Plan left = joinToMultiJoin(join.left());
        Plan right = joinToMultiJoin(join.right());

        boolean changeLeft = join.getJoinType().isRightJoin()
                || join.getJoinType().isFullOuterJoin();
        if (canCombine(left, changeLeft)) {
            MultiJoin leftMultiJoin = (MultiJoin) left;
            inputs.addAll(leftMultiJoin.children());
            joinFilter.addAll(leftMultiJoin.getJoinFilter());
        } else {
            inputs.add(left);
        }

        boolean changeRight = join.getJoinType().isLeftJoin()
                || join.getJoinType().isFullOuterJoin();
        if (canCombine(right, changeRight)) {
            MultiJoin rightMultiJoin = (MultiJoin) right;
            inputs.addAll(rightMultiJoin.children());
            joinFilter.addAll(rightMultiJoin.getJoinFilter());
        } else {
            inputs.add(right);
        }

        Optional<JoinType> joinType;
        if (join.getJoinType().isInnerOrCrossJoin()) {
            joinType = Optional.empty();
        } else {
            joinType = Optional.of(join.getJoinType());
        }
        return new MultiJoin(
                inputs,
                joinFilter,
                joinType,
                notInnerJoinConditions);
    }

    /**
     * Recursively convert to
     * {@link MultiJoin}
     * -->
     * {@link LogicalJoin} or
     * {@link LogicalFilter}--{@link LogicalJoin}
     */
    public Plan multiJoinToJoin(MultiJoin multiJoin) {
        if (multiJoin.arity() == 1) {
            return PlanUtils.filterOrSelf(multiJoin.getJoinFilter(), multiJoin.child(0));
        }

        Builder<Plan> builder = ImmutableList.builder();
        // recursively hanlde multiJoin children.
        for (Plan child : multiJoin.children()) {
            if (child instanceof MultiJoin) {
                MultiJoin childMultiJoin = (MultiJoin) child;
                builder.add(multiJoinToJoin(childMultiJoin));
            } else {
                builder.add(child);
            }
        }
        MultiJoin multiJoinHandleChildren = multiJoin.withChildren(builder.build());

        if (multiJoinHandleChildren.getOnlyJoinType().isPresent()) {
            List<Expression> leftFilter = Lists.newArrayList();
            List<Expression> rightFilter = Lists.newArrayList();
            List<Expression> remainingFilter = Lists.newArrayList();
            Plan left = multiJoinToJoin(new MultiJoin(
                    multiJoinHandleChildren.children().subList(0, multiJoinHandleChildren.arity() - 1),
                    leftFilter,
                    Optional.empty(),
                    ExpressionUtils.EMPTY_CONDITION));
            Plan right = multiJoinToJoin(new MultiJoin(
                    multiJoinHandleChildren.children().subList(1, multiJoinHandleChildren.arity()),
                    rightFilter,
                    Optional.empty(),
                    ExpressionUtils.EMPTY_CONDITION));
            if (multiJoinHandleChildren.getOnlyJoinType().get().isLeftJoin()) {
                right = multiJoinHandleChildren.child(multiJoinHandleChildren.arity() - 1);
            } else if (multiJoinHandleChildren.getOnlyJoinType().get().isRightJoin()) {
                left = multiJoinHandleChildren.child(0);
            }

            // split filter
            for (Expression expr : multiJoinHandleChildren.getJoinFilter()) {
                Set<Slot> exprInputSlots = expr.getInputSlots();
                Preconditions.checkState(!exprInputSlots.isEmpty());

                if (left.getOutputSet().containsAll(exprInputSlots)) {
                    leftFilter.add(expr);
                } else if (right.getOutputSet().containsAll(exprInputSlots)) {
                    rightFilter.add(expr);
                } else if (multiJoin.getOutputSet().containsAll(exprInputSlots)) {
                    remainingFilter.add(expr);
                } else {
                    throw new RuntimeException("invalid expression");
                }
            }

            return PlanUtils.filterOrSelf(remainingFilter, new LogicalJoin<>(
                    multiJoinHandleChildren.getOnlyJoinType().get(),
                    ExpressionUtils.EMPTY_CONDITION,
                    multiJoinHandleChildren.getNotInnerJoinConditions(),
                    PlanUtils.filterOrSelf(leftFilter, left), PlanUtils.filterOrSelf(rightFilter, right)));
        }

        // following this multiJoin just contain INNER/CROSS.
        List<Expression> joinFilter = multiJoinHandleChildren.getJoinFilter();

        Plan left = multiJoinHandleChildren.child(0);
        List<Plan> candidates = multiJoinHandleChildren.children().subList(1, multiJoinHandleChildren.arity());

        LogicalJoin<? extends Plan, ? extends Plan> join = findInnerJoin(left, candidates, joinFilter);
        List<Plan> newInputs = Lists.newArrayList();
        newInputs.add(join);
        newInputs.addAll(candidates.stream().filter(plan -> !join.right().equals(plan)).collect(Collectors.toList()));

        joinFilter.removeAll(join.getHashJoinConjuncts());
        joinFilter.removeAll(join.getOtherJoinConjuncts());
        return multiJoinToJoin(new MultiJoin(
                newInputs,
                joinFilter,
                Optional.empty(),
                ExpressionUtils.EMPTY_CONDITION));
    }

    /**
     * Returns whether an input can be merged without changing semantics.
     *
     * @param input input into a MultiJoin or (GroupPlan|LogicalFilter)
     * @param changeLeft generate nullable or left not exist.
     * @return true if the input can be combined into a parent MultiJoin
     */
    private static boolean canCombine(Plan input, boolean changeLeft) {
        return input instanceof MultiJoin
                && !((MultiJoin) input).getOnlyJoinType().isPresent()
                && !changeLeft;
    }

    private Set<Slot> getJoinOutput(Plan left, Plan right) {
        HashSet<Slot> joinOutput = new HashSet<>();
        joinOutput.addAll(left.getOutput());
        joinOutput.addAll(right.getOutput());
        return joinOutput;
    }

    /**
     * Find hash condition from joinFilter
     * Get InnerJoin from left, right from [candidates].
     *
     * @return InnerJoin or CrossJoin{left, last of [candidates]}
     */
    private LogicalJoin<? extends Plan, ? extends Plan> findInnerJoin(Plan left, List<Plan> candidates,
            List<Expression> joinFilter) {
        Set<Slot> leftOutputSet = left.getOutputSet();
        for (int i = 0; i < candidates.size(); i++) {
            Plan candidate = candidates.get(i);
            Set<Slot> rightOutputSet = candidate.getOutputSet();

            Set<Slot> joinOutput = getJoinOutput(left, candidate);

            List<Expression> currentJoinFilter = joinFilter.stream()
                    .filter(expr -> {
                        Set<Slot> exprInputSlots = expr.getInputSlots();
                        Preconditions.checkState(exprInputSlots.size() > 1,
                                "Predicate like table.col > 1 must have pushdown.");
                        if (leftOutputSet.containsAll(exprInputSlots)) {
                            return false;
                        }
                        if (rightOutputSet.containsAll(exprInputSlots)) {
                            return false;
                        }

                        return joinOutput.containsAll(exprInputSlots);
                    }).collect(Collectors.toList());

            Pair<List<Expression>, List<Expression>> pair = JoinUtils.extractExpressionForHashTable(
                    left.getOutput(), candidate.getOutput(), currentJoinFilter);
            List<Expression> hashJoinConditions = pair.first;
            List<Expression> otherJoinConditions = pair.second;
            if (!hashJoinConditions.isEmpty()) {
                return new LogicalJoin<>(JoinType.INNER_JOIN,
                        hashJoinConditions, otherJoinConditions,
                        left, candidate);
            }

            if (i == candidates.size() - 1) {
                return new LogicalJoin<>(JoinType.CROSS_JOIN,
                        hashJoinConditions, otherJoinConditions,
                        left, candidate);
            }
        }
        throw new RuntimeException("findInnerJoin: can't reach here");
    }
}
