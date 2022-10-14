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
import org.apache.doris.nereids.NereidsPlanner;
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
 * Using the {@link MultiJoin} to complete this task.
 * {Join cluster}: contain multiple join with filter inside.
 * <ul>
 * <li> {Join cluster} to MultiJoin</li>
 * <li> MultiJoin to {Join cluster}</li>
 * </ul>
 */
public class ReorderJoin extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalFilter(subTree(LogicalJoin.class, LogicalFilter.class)).thenApply(ctx -> {
            LogicalFilter<Plan> filter = ctx.root;

            Plan plan = joinToMultiJoin(filter);
            Preconditions.checkState(plan instanceof MultiJoin);
            MultiJoin multiJoin = (MultiJoin) plan;
            Plan after = multiJoinToJoin(multiJoin);
            return after;
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
        // Implicit rely on {rule: MergeFilters}, so don't exist filter--filter--join.
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

        return new MultiJoin(
                inputs,
                joinFilter,
                join.getJoinType(),
                notInnerJoinConditions);
    }

    /**
     * Recursively convert to
     * {@link MultiJoin}
     * -->
     * {@link LogicalJoin} or
     * {@link LogicalFilter}--{@link LogicalJoin}
     * <p>
     * When all input is CROSS/Inner Join, all join will be flattened.
     * Otherwise, we will split {join cluster} into multiple {@link MultiJoin}.
     * <p>
     * Here are examples of the {@link MultiJoin}s constructed after this rules has been applied.
     * <p>
     * simple example:
     * <ul>
     * <li>A JOIN B --> MJ(A, B)
     * <li>A JOIN B JOIN C JOIN D --> MJ(A, B, C, D)
     * <li>A LEFT (OUTER/SEMI/ANTI) JOIN B --> MJ([LOJ/LSJ/LAJ]A, B)
     * <li>A LEFT (OUTER/SEMI/ANTI) JOIN B --> MJ([ROJ/RSJ/RAJ]A, B)
     * <li>A FULL JOIN B --> MJ[FOJ](A, B)
     * </ul>
     * </p>
     * <p>
     * complex example:
     * <ul>
     * <li>A LEFT OUTER JOIN (B JOIN C) --> MJ([LOJ]A, MJ(B, C)))
     * <li>(A JOIN B) LEFT JOIN C --> MJ(A, B, C)
     * <li>(A LEFT OUTER JOIN B) JOIN C --> MJ(MJ(A, B), C)
     * <li>A LEFT JOIN (B FULL JOIN C) --> MJ(A, MJ[full](B, C))
     * <li>(A LEFT JOIN B) FULL JOIN (C RIGHT JOIN D) --> MJ[full](MJ(A, B), MJ(C, D))
     * </ul>
     * </p>
     * more complex example:
     * <ul>
     * <li> A JOIN B JOIN C LEFT JOIN D --> MJ([LOJ]A, B, C, D)
     * <li> A JOIN B JOIN C LEFT JOIN (D JOIN F) --> MJ([LOJ]A, B, C, MJ(D, F))
     * <li> A RIGHT JOIN (B JOIN C JOIN D)--> MJ([ROJ]A, B, C, D)
     * <li> A JOIN B RIGHT JOIN (C JOIN D) --> MJ(A, B, MJ([ROJ]C, D))
     * </ul>
     * </p>
     * <p>
     * Graphic presentation:
     * A JOIN B JOIN C LEFT JOIN D JOIN F
     *      left                  left│
     * A  B  C  D  F   ──►   A  B  C  │ D  F   ──►  MJ(LOJ A,B,C,MJ(DF)
     * <p>
     * A JOIN B RIGHT JOIN C JOIN D JOIN F
     *     right                  │right
     * A  B  C  D  F   ──►   A  B │  C  D  F   ──►  MJ(A,B,MJ(ROJ C,D,F)
     * <p>
     * (A JOIN B JOIN C) FULL JOIN (D JOIN F)
     *       full                    │
     * A  B  C  D  F   ──►   A  B  C │ D  F    ──►  MJ(FOJ MJ(A,B,C) MJ(D,F))
     * </p>
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

        if (!multiJoinHandleChildren.getJoinType().isInnerOrCrossJoin()) {
            List<Expression> leftFilter = Lists.newArrayList();
            List<Expression> rightFilter = Lists.newArrayList();
            List<Expression> remainingFilter = Lists.newArrayList();
            Plan left;
            Plan right;
            if (multiJoinHandleChildren.getJoinType().isLeftJoin()) {
                left = multiJoinToJoin(new MultiJoin(
                        multiJoinHandleChildren.children().subList(0, multiJoinHandleChildren.arity() - 1),
                        leftFilter,
                        JoinType.INNER_JOIN,
                        ExpressionUtils.EMPTY_CONDITION));
                right = multiJoinHandleChildren.child(multiJoinHandleChildren.arity() - 1);
            } else if (multiJoinHandleChildren.getJoinType().isRightJoin()) {
                left = multiJoinHandleChildren.child(0);
                right = multiJoinToJoin(new MultiJoin(
                        multiJoinHandleChildren.children().subList(1, multiJoinHandleChildren.arity()),
                        rightFilter,
                        JoinType.INNER_JOIN,
                        ExpressionUtils.EMPTY_CONDITION));
            } else {
                left = multiJoinToJoin(new MultiJoin(
                        multiJoinHandleChildren.children().subList(0, multiJoinHandleChildren.arity() - 1),
                        leftFilter,
                        JoinType.INNER_JOIN,
                        ExpressionUtils.EMPTY_CONDITION));
                right = multiJoinToJoin(new MultiJoin(
                        multiJoinHandleChildren.children().subList(1, multiJoinHandleChildren.arity()),
                        rightFilter,
                        JoinType.INNER_JOIN,
                        ExpressionUtils.EMPTY_CONDITION));
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
                    NereidsPlanner.LOG.error("invalid expression, exist slot that multiJoin don't contains.");
                    throw new RuntimeException("invalid expression, exist slot that multiJoin don't contains.");
                }
            }

            return PlanUtils.filterOrSelf(remainingFilter, new LogicalJoin<>(
                    multiJoinHandleChildren.getJoinType(),
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
        // TODO(wj): eliminate this recursion.
        return multiJoinToJoin(new MultiJoin(
                newInputs,
                joinFilter,
                JoinType.INNER_JOIN,
                ExpressionUtils.EMPTY_CONDITION));
    }

    /**
     * Returns whether an input can be merged without changing semantics.
     *
     * @param input input into a MultiJoin or (GroupPlan|LogicalFilter)
     * @param changeChildren generate nullable or one side child not exist.
     * @return true if the input can be combined into a parent MultiJoin
     */
    private static boolean canCombine(Plan input, boolean changeChildren) {
        return input instanceof MultiJoin
                && ((MultiJoin) input).getJoinType().isInnerOrCrossJoin()
                && !changeChildren;
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
