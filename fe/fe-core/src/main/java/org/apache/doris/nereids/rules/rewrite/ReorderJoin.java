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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.annotation.DependsRules;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
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
 * Using the {@link MultiJoin} to complete this task.
 * {Join cluster}: contain multiple join with filter inside.
 * <ul>
 * <li> {Join cluster} to MultiJoin</li>
 * <li> MultiJoin to {Join cluster}</li>
 * </ul>
 */
@DependsRules({
    MergeFilters.class
})
public class ReorderJoin extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalFilter(subTree(LogicalJoin.class, LogicalFilter.class))
            .whenNot(filter -> filter.child() instanceof LogicalJoin
                    && ((LogicalJoin<?, ?>) filter.child()).isMarkJoin())
            .thenApply(ctx -> {
                if (ctx.statementContext.getConnectContext().getSessionVariable().isDisableJoinReorder()
                        || ctx.cascadesContext.isLeadingDisableJoinReorder()
                        || ((LogicalJoin<?, ?>) ctx.root.child()).isLeadingJoin()) {
                    return null;
                }
                LogicalFilter<Plan> filter = ctx.root;

                Map<Plan, DistributeHint> planToHintType = Maps.newHashMap();
                Plan plan = joinToMultiJoin(filter, planToHintType);
                Preconditions.checkState(plan instanceof MultiJoin, "join to multi join should return MultiJoin,"
                        + " but return plan is " + plan.getType());
                MultiJoin multiJoin = (MultiJoin) plan;
                ctx.statementContext.addJoinFilters(multiJoin.getJoinFilter());
                ctx.statementContext.setMaxNAryInnerJoin(multiJoin.children().size());
                Plan after = multiJoinToJoin(multiJoin, planToHintType);
                return after;
            }).toRule(RuleType.REORDER_JOIN);
    }

    /**
     * Recursively convert to
     * {@link LogicalJoin} or {@link LogicalFilter}--{@link LogicalJoin}
     * --> {@link MultiJoin}
     */
    public Plan joinToMultiJoin(Plan plan, Map<Plan, DistributeHint> planToHintType) {
        // subtree can't specify the end of Pattern. so end can be GroupPlan or Filter
        if (nonJoinAndNonFilter(plan)
                || (plan instanceof LogicalFilter && nonJoinAndNonFilter(plan.child(0)))) {
            return plan;
        }

        List<Plan> inputs = Lists.newArrayList();
        List<Expression> joinFilter = Lists.newArrayList();
        List<Expression> notInnerJoinConditions = Lists.newArrayList();

        LogicalJoin<?, ?> join;
        // Implicit rely on {rule: MergeFilters}, so don't exist filter--filter--join.
        if (plan instanceof LogicalFilter) {
            LogicalFilter<?> filter = (LogicalFilter<?>) plan;
            joinFilter.addAll(filter.getConjuncts());
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
        planToHintType.put(join.left(), new DistributeHint(DistributeType.NONE));
        Plan left = joinToMultiJoin(join.left(), planToHintType);
        planToHintType.put(join.right(), join.getDistributeHint());
        Plan right = joinToMultiJoin(join.right(), planToHintType);

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
     * Graphic presentation:
     * <pre>
     * A JOIN B JOIN C LEFT JOIN D JOIN F
     *      left                  left│
     * A  B  C  D  F   ──►   A  B  C  │ D  F   ──►  MJ(LOJ A,B,C,MJ(DF)
     *
     * A JOIN B RIGHT JOIN C JOIN D JOIN F
     *     right                  │right
     * A  B  C  D  F   ──►   A  B │  C  D  F   ──►  MJ(A,B,MJ(ROJ C,D,F)
     *
     * (A JOIN B JOIN C) FULL JOIN (D JOIN F)
     *       full                    │
     * A  B  C  D  F   ──►   A  B  C │ D  F    ──►  MJ(FOJ MJ(A,B,C) MJ(D,F))
     * </pre>
     */
    public Plan multiJoinToJoin(MultiJoin multiJoin, Map<Plan, DistributeHint> planToHintType) {
        if (multiJoin.arity() == 1) {
            return PlanUtils.filterOrSelf(ImmutableSet.copyOf(multiJoin.getJoinFilter()), multiJoin.child(0));
        }

        Builder<Plan> builder = ImmutableList.builder();
        // recursively handle multiJoin children.
        for (Plan child : multiJoin.children()) {
            if (child instanceof MultiJoin) {
                MultiJoin childMultiJoin = (MultiJoin) child;
                builder.add(multiJoinToJoin(childMultiJoin, planToHintType));
            } else {
                builder.add(child);
            }
        }
        MultiJoin multiJoinHandleChildren = multiJoin.withChildren(builder.build());

        if (!multiJoinHandleChildren.getJoinType().isInnerOrCrossJoin()) {
            List<Expression> remainingFilter;

            Plan left;
            Plan right;
            if (multiJoinHandleChildren.getJoinType().isLeftJoin()) {
                right = multiJoinHandleChildren.child(multiJoinHandleChildren.arity() - 1);
                Set<ExprId> rightOutputExprIdSet = right.getOutputExprIdSet();
                Map<Boolean, List<Expression>> split = multiJoin.getJoinFilter().stream()
                        .collect(Collectors.partitioningBy(expr ->
                                Utils.isIntersecting(rightOutputExprIdSet, expr.getInputSlotExprIds())
                        ));
                remainingFilter = split.get(true);
                List<Expression> pushedFilter = split.get(false);
                left = multiJoinToJoin(new MultiJoin(
                        multiJoinHandleChildren.children().subList(0, multiJoinHandleChildren.arity() - 1),
                        pushedFilter,
                        JoinType.INNER_JOIN,
                        ExpressionUtils.EMPTY_CONDITION), planToHintType);
            } else if (multiJoinHandleChildren.getJoinType().isRightJoin()) {
                left = multiJoinHandleChildren.child(0);
                Set<ExprId> leftOutputExprIdSet = left.getOutputExprIdSet();
                Map<Boolean, List<Expression>> split = multiJoin.getJoinFilter().stream()
                        .collect(Collectors.partitioningBy(expr ->
                                Utils.isIntersecting(leftOutputExprIdSet, expr.getInputSlotExprIds())
                        ));
                remainingFilter = split.get(true);
                List<Expression> pushedFilter = split.get(false);
                right = multiJoinToJoin(new MultiJoin(
                        multiJoinHandleChildren.children().subList(1, multiJoinHandleChildren.arity()),
                        pushedFilter,
                        JoinType.INNER_JOIN,
                        ExpressionUtils.EMPTY_CONDITION), planToHintType);
            } else {
                remainingFilter = multiJoin.getJoinFilter();
                Preconditions.checkState(multiJoinHandleChildren.arity() == 2);
                List<Plan> children = multiJoinHandleChildren.children().stream().map(child -> {
                    if (child instanceof MultiJoin) {
                        return multiJoinToJoin((MultiJoin) child, planToHintType);
                    } else {
                        return child;
                    }
                }).collect(Collectors.toList());
                left = children.get(0);
                right = children.get(1);
            }

            return PlanUtils.filterOrSelf(ImmutableSet.copyOf(remainingFilter), new LogicalJoin<>(
                    multiJoinHandleChildren.getJoinType(),
                    ExpressionUtils.EMPTY_CONDITION, multiJoinHandleChildren.getNotInnerJoinConditions(),
                    planToHintType.getOrDefault(right, new DistributeHint(DistributeType.NONE)),
                    Optional.empty(),
                    left, right, null));
        }

        // following this multiJoin just contain INNER/CROSS.
        Set<Expression> joinFilter = new LinkedHashSet<>(multiJoinHandleChildren.getJoinFilter());

        Plan left = multiJoinHandleChildren.child(0);
        Set<Integer> usedPlansIndex = new LinkedHashSet<>();
        usedPlansIndex.add(0);

        while (usedPlansIndex.size() != multiJoinHandleChildren.children().size()) {
            LogicalJoin<? extends Plan, ? extends Plan> join = findInnerJoin(left, multiJoinHandleChildren.children(),
                    joinFilter, usedPlansIndex, planToHintType);
            join.getHashJoinConjuncts().forEach(joinFilter::remove);
            join.getOtherJoinConjuncts().forEach(joinFilter::remove);

            left = join;
        }

        return PlanUtils.filterOrSelf(joinFilter, left);
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

    /**
     * Find hash condition from joinFilter
     * Get InnerJoin from left, right from [candidates].
     *
     * @return InnerJoin or CrossJoin{left, last of [candidates]}
     */
    private LogicalJoin<? extends Plan, ? extends Plan> findInnerJoin(Plan left, List<Plan> candidates,
            Set<Expression> joinFilter, Set<Integer> usedPlansIndex, Map<Plan, DistributeHint> planToHintType) {
        List<Expression> firstOtherJoinConditions = ExpressionUtils.EMPTY_CONDITION;
        int firstCandidate = -1;
        Set<ExprId> leftOutputExprIdSet = left.getOutputExprIdSet();
        for (int i = 0; i < candidates.size(); i++) {
            if (usedPlansIndex.contains(i)) {
                continue;
            }

            Plan candidate = candidates.get(i);
            Set<ExprId> rightOutputExprIdSet = candidate.getOutputExprIdSet();

            Set<ExprId> joinOutputExprIdSet = JoinUtils.getJoinOutputExprIdSet(left, candidate);

            List<Expression> currentJoinFilter = joinFilter.stream()
                    .filter(expr -> {
                        Set<ExprId> exprInputSlotExprIds = expr.getInputSlotExprIds();
                        return !leftOutputExprIdSet.containsAll(exprInputSlotExprIds)
                                && !rightOutputExprIdSet.containsAll(exprInputSlotExprIds)
                                && joinOutputExprIdSet.containsAll(exprInputSlotExprIds);
                    }).collect(Collectors.toList());

            Pair<List<Expression>, List<Expression>> pair = JoinUtils.extractExpressionForHashTable(
                    left.getOutput(), candidate.getOutput(), currentJoinFilter);
            List<Expression> hashJoinConditions = pair.first;
            if (!hashJoinConditions.isEmpty()) {
                usedPlansIndex.add(i);
                return new LogicalJoin<>(JoinType.INNER_JOIN,
                        hashJoinConditions, pair.second,
                        planToHintType.getOrDefault(candidate, new DistributeHint(DistributeType.NONE)),
                        Optional.empty(),
                        left, candidate, null);
            } else {
                if (firstCandidate == -1) {
                    firstCandidate = i;
                    firstOtherJoinConditions = pair.second;
                }
            }
        }
        // All { left -> one in [candidates] } is CrossJoin
        // Generate a CrossJoin
        // NOTICE: we must traverse for head to tail to ensure result is stable.
        usedPlansIndex.add(firstCandidate);
        Plan right = candidates.get(firstCandidate);
        return new LogicalJoin<>(JoinType.CROSS_JOIN,
                ExpressionUtils.EMPTY_CONDITION,
                firstOtherJoinConditions,
                planToHintType.getOrDefault(right, new DistributeHint(DistributeType.NONE)),
                Optional.empty(),
                left, right, null);
    }

    private boolean nonJoinAndNonFilter(Plan plan) {
        return !(plan instanceof LogicalJoin) && !(plan instanceof LogicalFilter);
    }
}
