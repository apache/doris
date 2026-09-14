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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.copier.DeepCopierContext;
import org.apache.doris.nereids.trees.copier.LogicalPlanDeepCopier;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.NoneMovableFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.NullIgnoringAggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Merge the correlated predicate and agg in the filter under apply.
 * And keep the unCorrelated predicate under agg.
 * <p>
 * Use the correlated column as the group by column of agg,
 * the output column is the correlated column and the input column.
 * <pre>
 * before:
 *                 apply
 *             /          \
 *     Input(output:b)   Filter(this node's existence depends on having clause's existence)
 *                              |
 *                         agg(output:fn; group by:null)
 *                              |
 *              Filter(correlated predicate(Input.e = this.f)/Unapply predicate)
 *
 * end:
 *          apply(correlated predicate(Input.e = this.f))
 *         /              \
 * Input(output:b)   Filter(this node's existence depends on having clause's existence)
 *                             |
 *                        agg(output:fn,this.f; group by:this.f)
 *                              |
 *                    Filter(Uncorrelated predicate)
 * </pre>
 * <p>
 * The rewrite above keeps the aggregation of the subquery on the inner side, which is only
 * equivalent to the original subquery when the correlated predicate is an equality between the
 * outer side and the inner side: in that case the inner rows of one outer row are exactly the
 * groups of the aggregate whose key is the value of that inner side, so the HAVING clause of every
 * group is the HAVING clause of the outer row.
 * The aggregation of an EXISTS/NOT EXISTS subquery is built on the outer side instead when that
 * equivalence does not hold, see
 * {@link #pullUpCorrelatedPredicateByAggregatingOuter}.
 */
public class UnCorrelatedApplyAggregateFilter implements RewriteRuleFactory {

    /** name of the projected column which tells whether an inner row matched the correlated predicate */
    private static final String CORRELATION_MATCH_MARKER = "$correlation_match_marker";

    @Override
    public List<Rule> buildRules() {
        // The nodes between the apply and the aggregate of the subquery are projections and filters
        // in any order, so the rules cannot enumerate their shapes: match every correlated apply
        // whose right side starts with one of them and locate the aggregate in the rule.
        return ImmutableList.of(
                logicalApply(any(), subTree(LogicalAggregate.class, LogicalProject.class, LogicalFilter.class))
                        .when(LogicalApply::isCorrelated)
                        .when(apply -> locateAggregate(apply).isPresent())
                        .then(UnCorrelatedApplyAggregateFilter::pullUpCorrelatedFilter)
                        .toRule(RuleType.UN_CORRELATED_APPLY_AGGREGATE_FILTER));
    }

    /**
     * The aggregation of the subquery and the filter which holds the predicates of its HAVING clause
     * which were not pulled into the apply: the nodes between the apply and the aggregate are the
     * projections and the filters of the subquery, and the projections between the aggregate and its
     * filter only carry the columns which the aggregation needs.
     */
    private static Optional<Pair<LogicalAggregate<?>, Optional<LogicalFilter<Plan>>>> locateAggregate(
            LogicalApply<?, ?> apply) {
        Plan below = apply.right();
        Optional<LogicalFilter<Plan>> havingFilter = Optional.empty();
        while (!(below instanceof LogicalAggregate)) {
            if (below instanceof LogicalFilter) {
                havingFilter = Optional.of((LogicalFilter<Plan>) below);
            } else if (!(below instanceof LogicalProject)) {
                return Optional.empty();
            }
            below = below.child(0);
        }
        LogicalAggregate<?> agg = (LogicalAggregate<?>) below;
        Plan belowAggregate = agg.child(0);
        while (belowAggregate instanceof LogicalProject) {
            for (NamedExpression project : ((LogicalProject<?>) belowAggregate).getProjects()) {
                if (!(project instanceof Slot)) {
                    // the projection computes the arguments of the aggregation itself, so it cannot
                    // be replaced together with the aggregation
                    return Optional.empty();
                }
            }
            belowAggregate = belowAggregate.child(0);
        }
        if (!(belowAggregate instanceof LogicalFilter) || belowAggregate.child(0) == null) {
            return Optional.empty();
        }
        return Optional.of(Pair.of(agg, havingFilter));
    }

    /**
     * The filter below the aggregation of the subquery, which holds the predicates of its WHERE clause.
     * The projections between the aggregation and that filter only carry the columns which the
     * aggregation needs, so the rewrite replaces them together with the aggregation.
     */
    private static LogicalFilter<Plan> filterBelowAggregate(LogicalAggregate<?> agg) {
        Plan below = agg.child(0);
        while (below instanceof LogicalProject) {
            below = below.child(0);
        }
        return (LogicalFilter<Plan>) below;
    }

    private static Plan pullUpCorrelatedFilter(LogicalApply<?, ?> apply) {
        // the correlated predicates of the HAVING clause (the filter below the apply) may already have
        // been pulled into the apply, and a projection which only exposes the output of the aggregate
        // may sit between the apply and the aggregate. Walk down to the aggregate and remember the
        // filter which still holds predicates of the HAVING clause.
        Optional<Pair<LogicalAggregate<?>, Optional<LogicalFilter<Plan>>>> located = locateAggregate(apply);
        if (!located.isPresent()) {
            return apply;
        }
        LogicalAggregate<?> agg = located.get().first;
        Optional<LogicalFilter<Plan>> havingFilter = located.get().second;
        LogicalFilter<Plan> filter = filterBelowAggregate(agg);
        // split filter conjuncts to correlated and unCorrelated ones
        Map<Boolean, List<Expression>> split =
                Utils.splitCorrelatedConjuncts(filter.getConjuncts(), apply.getCorrelationSlot());
        List<Expression> correlatedPredicate = split.get(true);
        List<Expression> unCorrelatedPredicate = split.get(false);

        // the representative has experienced the rule and added the correlated predicate to the apply node
        if (correlatedPredicate.isEmpty()) {
            return apply;
        }

        CorrelatedAggregatePredicates predicates =
                CorrelatedAggregatePredicates.of(apply, agg, filter, havingFilter, correlatedPredicate);
        // The original rewrite appends the inner side of the correlated predicate to the group by of
        // the aggregation and pulls the predicate into the apply, so the join conditions of the apply
        // reference the correlation key which the aggregation returns. A projection between the apply
        // and the aggregation hides that key from the join, and a filter which sits above the HAVING
        // clause (a predicate over the projection of the select list, which filter pushdown cannot
        // push below that projection) can keep the row of the empty input, so the aggregation has to
        // be built on the outer side in both cases.
        boolean aggregationOnOuter = needCorrelatedAggregationOnOuter(
                apply, agg, havingFilter.isEmpty(), correlatedPredicate, predicates)
                || (apply.isExist() && (hasProjectionAboveAggregate(apply, agg)
                        || (havingFilter.isPresent() && hasFilterAboveHavingFilter(apply, havingFilter.get()))));
        if (aggregationOnOuter) {
            Plan aggregatedOuter = pullUpCorrelatedPredicateByAggregatingOuter(
                    apply, agg, filter, unCorrelatedPredicate, predicates);
            if (aggregatedOuter != null) {
                return aggregatedOuter;
            }
            // The original rewrite is known to be not equivalent for this subquery and the rewrite
            // above cannot be applied safely: report the subquery as unsupported instead of building
            // a plan whose result is wrong.
            throw new AnalysisException("Unsupported correlated subquery with grouping and/or aggregation "
                    + apply.right());
        }

        // pull up correlated filter into apply node
        List<NamedExpression> newAggOutput = new ArrayList<>(agg.getOutputExpressions());
        List<Expression> newGroupby =
                Utils.getUnCorrelatedExprs(correlatedPredicate, apply.getCorrelationSlot());
        newGroupby.addAll(agg.getGroupByExpressions());
        Map<Expression, Slot> unCorrelatedExprToSlot = Maps.newHashMap();
        for (Expression expression : newGroupby) {
            if (expression instanceof Slot) {
                newAggOutput.add((NamedExpression) expression);
            } else {
                Alias alias = new Alias(expression);
                unCorrelatedExprToSlot.put(expression, alias.toSlot());
                newAggOutput.add(alias);
            }
        }
        correlatedPredicate = ExpressionUtils.replace(correlatedPredicate, unCorrelatedExprToSlot);
        LogicalAggregate newAgg = new LogicalAggregate<>(newGroupby, newAggOutput,
                PlanUtils.filterOrSelf(ImmutableSet.copyOf(unCorrelatedPredicate), filter.child()));
        // the predicates which were already pulled into the apply are the predicates of the HAVING
        // clause of the subquery: they were evaluated on the rows of the old aggregate and have to
        // stay in the filter of the new apply, otherwise the subquery loses them
        List<Expression> newCorrelationFilter = Lists.newArrayList();
        apply.getCorrelationFilter().map(ExpressionUtils::extractConjunction)
                .ifPresent(newCorrelationFilter::addAll);
        newCorrelationFilter.addAll(correlatedPredicate);
        return new LogicalApply<>(apply.getCorrelationSlot(), apply.getSubqueryType(), apply.isNot(),
                apply.getCompareExpr(), apply.getTypeCoercionExpr(),
                ExpressionUtils.optionalAnd(newCorrelationFilter), apply.getMarkJoinSlotReference(),
                apply.isNeedAddSubOutputToProjects(), apply.isMarkJoinSlotNotNull(), apply.left(),
                replaceAggregate(apply.right(), newAgg));
    }

    /**
     * Whether a projection sits between the apply and the aggregation of the subquery: it only
     * exposes a part of the output of the aggregation (the select list of the subquery), so the
     * correlation keys the original rewrite adds to the group by are not visible above it.
     */
    private static boolean hasProjectionAboveAggregate(LogicalApply<?, ?> apply, LogicalAggregate<?> agg) {
        Plan below = apply.right();
        while (below != agg) {
            if (below instanceof LogicalProject) {
                return true;
            }
            below = below.child(0);
        }
        return false;
    }

    /**
     * Whether a filter sits between the apply and the HAVING clause of the subquery (the filter which
     * holds the predicates of the HAVING clause which were not pulled into the apply). Filter
     * pushdown creates such a filter above the projection of the select list when a predicate reads a
     * column of that projection which cannot be pushed below it (eg. a volatile column), and the
     * filter decides on the rows which the projection produces, so the rewrite has to keep it.
     */
    private static boolean hasFilterAboveHavingFilter(LogicalApply<?, ?> apply, LogicalFilter<Plan> havingFilter) {
        Plan below = apply.right();
        while (below != havingFilter) {
            if (below instanceof LogicalFilter) {
                return true;
            }
            below = below.child(0);
        }
        return false;
    }

    /**
     * Keep the nodes which wrap the aggregate (the HAVING clause, the projection of the subquery)
     * when the aggregate is replaced.
     */
    private static Plan replaceAggregate(Plan plan, Plan newAggregate) {
        if (plan instanceof LogicalAggregate) {
            return newAggregate;
        }
        return plan.withChildren(replaceAggregate(plan.child(0), newAggregate));
    }

    /**
     * The join which pairs an outer row with its correlation keys reads the keys from the output of
     * the right side, so every projection which sits above the aggregate has to expose them: the
     * keys which a projection does not carry are added to its output (nothing else reads the output
     * of an EXISTS subquery).
     */
    private static Plan exposeCorrelationKeys(Plan plan, Set<Slot> keys) {
        if (plan instanceof LogicalAggregate) {
            // the aggregation of the rewrite outputs the correlation keys itself
            return plan;
        }
        Plan child = exposeCorrelationKeys(plan.child(0), keys);
        if (plan instanceof LogicalProject) {
            LogicalProject<?> project = (LogicalProject<?>) plan;
            Set<Slot> exposed = project.getProjects().stream()
                    .map(NamedExpression::toSlot).collect(ImmutableSet.toImmutableSet());
            List<NamedExpression> projects = Lists.newArrayList(project.getProjects());
            boolean added = false;
            for (Slot key : keys) {
                if (!exposed.contains(key)) {
                    projects.add(key);
                    added = true;
                }
            }
            if (added) {
                return new LogicalProject<>(projects, project.isDistinct(), project.getAsteriskOutputs(), child);
            }
        }
        return plan.withChildren(child);
    }

    /**
     * The predicates which relate the correlated subquery to the outer query, classified by the node
     * on which they have to be evaluated:
     *
     * <ul>
     *   <li>domain predicates select the inner rows which belong to the correlated domain of one outer
     *       row. They are the predicates of the WHERE clause of the subquery, plus the predicates
     *       which were already pulled into the apply and do not reference the aggregation;</li>
     *   <li>aggregate predicates reference the output of the aggregate, they are the predicates of the
     *       HAVING clause which were pulled into the apply by {@link UnCorrelatedApplyFilter} (that
     *       rule runs before this one). They decide which rows of the aggregation the subquery
     *       returns for one outer row.</li>
     * </ul>
     */
    private static final class CorrelatedAggregatePredicates {
        private final List<Expression> whereConjuncts = Lists.newArrayList();
        private final List<Expression> aggregatePredicates = Lists.newArrayList();
        private final Set<Expression> havingConjuncts = Sets.newLinkedHashSet();

        private static CorrelatedAggregatePredicates of(LogicalApply<?, ?> apply,
                LogicalAggregate<?> agg, LogicalFilter<Plan> filter,
                Optional<LogicalFilter<Plan>> havingFilter, List<Expression> whereConjuncts) {
            CorrelatedAggregatePredicates predicates = new CorrelatedAggregatePredicates();
            predicates.whereConjuncts.addAll(whereConjuncts);
            // Every predicate which was pulled into the apply was pulled from the filter which sits
            // above the aggregate (the HAVING clause of the subquery), so it decides which rows of
            // the aggregation the subquery returns and has to be evaluated above the aggregation of
            // the rewrite. Its provenance cannot be recovered from the slots it uses: a predicate
            // such as `outer.flag = 1` references no aggregation output and no inner column but it
            // still rejects the row of the aggregation.
            apply.getCorrelationFilter()
                    .map(ExpressionUtils::extractConjunction)
                    .orElse(ImmutableList.of())
                    .forEach(predicates.aggregatePredicates::add);
            havingFilter.ifPresent(remaining -> predicates.havingConjuncts.addAll(remaining.getConjuncts()));
            return predicates;
        }

        private List<Expression> domainPredicates() {
            return ImmutableList.copyOf(whereConjuncts);
        }

        private List<Expression> havingPredicates() {
            return ImmutableList.<Expression>builder()
                    .addAll(havingConjuncts)
                    .addAll(aggregatePredicates)
                    .build();
        }

        private List<Expression> pulledPredicates() {
            return ImmutableList.copyOf(aggregatePredicates);
        }

        private Set<Expression> havingConjuncts() {
            return havingConjuncts;
        }

        private boolean hasHaving() {
            return !havingConjuncts.isEmpty() || !aggregatePredicates.isEmpty();
        }

        private boolean hasAggregatePredicates() {
            return !aggregatePredicates.isEmpty();
        }

        /**
         * The correlation slots which the keys of the aggregation have to contain: the domains of
         * two outer rows are the same as soon as the slots their predicates use are equal.
         */
        private Set<Slot> keySlots(List<Slot> correlationSlots) {
            Set<Slot> keys = new LinkedHashSet<>();
            for (Expression conjunct : domainPredicates()) {
                addCorrelationSlots(conjunct, correlationSlots, keys);
            }
            for (Expression conjunct : aggregatePredicates) {
                addCorrelationSlots(conjunct, correlationSlots, keys);
            }
            return keys;
        }

        private static void addCorrelationSlots(Expression conjunct, List<Slot> correlationSlots, Set<Slot> keys) {
            for (Slot slot : conjunct.getInputSlots()) {
                if (correlationSlots.contains(slot)) {
                    keys.add(slot);
                }
            }
        }

        /**
         * Whether every predicate can be evaluated by the plan of the aggregation, that is if it only
         * uses the correlation keys, the inner rows and the output of the aggregate. The predicates
         * which are still in the plan of the subquery keep their node, so only the predicates which
         * were pulled into the apply have to be evaluated above the aggregation of the rewrite.
         */
        private boolean isResolvable(LogicalApply<?, ?> apply, LogicalAggregate<?> agg,
                LogicalFilter<Plan> filter) {
            Set<ExprId> allowed = Sets.newHashSet();
            apply.getCorrelationSlot().forEach(slot -> allowed.add(slot.getExprId()));
            agg.getOutput().forEach(slot -> allowed.add(slot.getExprId()));
            filter.child().getOutput().forEach(slot -> allowed.add(slot.getExprId()));
            return domainPredicates().stream().allMatch(conjunct -> allowed.containsAll(conjunct.getInputSlotExprIds()))
                    && pulledPredicates().stream()
                            .allMatch(conjunct -> allowed.containsAll(conjunct.getInputSlotExprIds()));
        }
    }

    /**
     * Whether the aggregation of the correlated subquery has to be built on the outer side.
     * <p>
     * The original rewrite puts the inner side of the correlated predicate into the group by of the
     * aggregate, so the HAVING clause of one group is treated as the HAVING clause of one outer row.
     * That is wrong for:
     * <ul>
     *   <li>a correlated predicate which is not an equality (eg. `inner.k &lt; outer.k`): the inner
     *       rows of one outer row are the union of several groups, so group wide aggregates such as
     *       count(*) are computed for a part of the domain of the outer row only;</li>
     *   <li>a global aggregate (no group by) whose HAVING clause holds for an empty input
     *       (eg. `having count(*) = 0`): a global aggregate returns one row for every outer row,
     *       including the outer rows without any matching inner row, and that row disappears when
     *       the inner side of the correlated predicate becomes the group by key;</li>
     *   <li>a HAVING clause which references the outer query: the row kept by that HAVING clause is
     *       the one of the domain of the outer row, so it cannot be evaluated on a group of the
     *       inner side when the domain is empty.</li>
     * </ul>
     */
    private static boolean needCorrelatedAggregationOnOuter(LogicalApply<?, ?> apply, LogicalAggregate<?> agg,
            boolean havingFilterPulled, List<Expression> correlatedPredicate,
            CorrelatedAggregatePredicates predicates) {
        if (!apply.isExist()) {
            // scalar and IN subqueries need the aggregate output to be exposed by a join
            return false;
        }
        if (havingFilterPulled && !predicates.hasHaving()) {
            // EXISTS/NOT EXISTS without a HAVING clause only depends on the existence of the
            // aggregation result, which is kept by grouping the inner side
            return false;
        }
        if (predicates.hasAggregatePredicates() && agg.getGroupByExpressions().isEmpty()) {
            // a predicate of the HAVING clause which references the outer query decides whether the
            // row of a global aggregate is kept, and that row exists for every outer row including
            // the rows of an empty correlated domain, so the predicate cannot be evaluated on a
            // group of the inner side
            return true;
        }
        for (Expression conjunct : correlatedPredicate) {
            if (!isSupportedCorrelatedComparison(conjunct, apply.getCorrelationSlot())) {
                // keep the behavior of the original rewrite, which reports these predicates
                return false;
            }
        }
        if (predicates.domainPredicates().stream()
                .anyMatch(conjunct -> breaksDomainPartition(conjunct, apply.getCorrelationSlot()))) {
            // the domain of one outer row is the union of several groups of the aggregate
            return true;
        }
        if (predicates.hasAggregatePredicates()) {
            // the domain of one outer row is exactly one group and the rows of the subquery are the
            // rows of that group, so the original rewrite is still equivalent
            return false;
        }
        if (!agg.getGroupByExpressions().isEmpty()) {
            // an equality correlated predicate maps the domain of every outer row onto exactly one group
            return false;
        }
        return havingMayHoldWithEmptyInput(agg, predicates.havingConjuncts());
    }

    /**
     * Whether this predicate changes the domain of an outer row in a way which is not a group of the
     * aggregate. Only an equality between the outer side and the inner side partitions the inner rows
     * of one outer row into exactly the groups of the aggregate, while a predicate which does not
     * reference the outer query at all just filters the inner rows.
     */
    private static boolean breaksDomainPartition(Expression conjunct, List<Slot> correlationSlots) {
        if (conjunct instanceof EqualPredicate) {
            return false;
        }
        return conjunct.getInputSlots().stream().anyMatch(correlationSlots::contains);
    }

    /**
     * Whether the correlated predicate is a comparison whose sides do not mix the outer query and the
     * subquery, eg. `inner.k &lt; outer.k` or `outer.k = inner.abs(k)`. Those are the predicates which
     * can be evaluated by joining the two sides, and the ones supported by the original rewrite.
     */
    private static boolean isSupportedCorrelatedComparison(Expression conjunct, List<Slot> correlationSlots) {
        Expression predicate = conjunct;
        if (predicate instanceof Not && predicate.child(0) instanceof BinaryExpression) {
            predicate = predicate.child(0);
        }
        if (!(predicate instanceof BinaryExpression)) {
            return false;
        }
        Expression left = ((BinaryExpression) predicate).left();
        Expression right = ((BinaryExpression) predicate).right();
        Set<Slot> leftSlots = left.getInputSlots();
        Set<Slot> rightSlots = right.getInputSlots();
        boolean correlatedToLeft = !leftSlots.isEmpty() && leftSlots.stream().allMatch(correlationSlots::contains)
                && rightSlots.stream().noneMatch(correlationSlots::contains);
        boolean correlatedToRight = !rightSlots.isEmpty() && rightSlots.stream().allMatch(correlationSlots::contains)
                && leftSlots.stream().noneMatch(correlationSlots::contains);
        return correlatedToLeft || correlatedToRight;
    }

    /**
     * Rewrite `outer [not] exists (select agg from inner where &lt;correlated predicate&gt;
     * [group by ...] having ...)` into a semi/anti join whose right side aggregates the outer rows
     * together with their correlated inner rows, so that the aggregation of one outer row is the
     * aggregation of exactly the inner rows satisfying the correlated predicate:
     *
     * <pre>
     * before:
     *              Apply(EXISTS, correlationSlot=[outer.k])
     *             /                \
     *        outer             Filter(having)
     *                              +-- Aggregate(group by [inner.g], count(*))
     *                                    +-- Filter(correlated predicate(inner.k &lt; outer.k))
     *                                          +-- inner
     *
     * after:
     *          LEFT SEMI JOIN(otherJoinConjuncts=[outer.k &lt;=&gt; key.k])
     *         /                 \
     *     outer              Filter(having: count(*) =&gt; count(marker))
     *                           +-- Aggregate(group by [key.k, inner.g], count(marker))
     *                                 +-- LEFT OUTER JOIN(inner.k &lt; key.k)     // keeps the empty domain
     *                                       |-- Aggregate(group by [k], output=[k])   // distinct correlated keys
     *                                       |     +-- outer'
     *                                       +-- Project(marker, ...)
     *                                             +-- Filter(uncorrelated predicates)
     *                                                   +-- inner
     * </pre>
     * outer' is a deep copy of outer, so that the two references of the outer plan have their own
     * slots and relation ids. The LEFT OUTER JOIN and the marker are not needed (a plain inner join
     * is used) when the aggregate has a group by, because such an aggregate returns no row at all
     * for an empty input. The predicates of the HAVING clause which reference the outer query (they
     * were pulled into the apply by {@link UnCorrelatedApplyFilter}) are evaluated above the
     * aggregate of the outer rows, so that they decide on the aggregation of the whole domain of an
     * outer row, the empty domain included.
     *
     * @return null if this rewrite cannot be applied safely, the caller then reports the subquery as
     *         unsupported
     */
    private static Plan pullUpCorrelatedPredicateByAggregatingOuter(LogicalApply<?, ?> apply,
            LogicalAggregate<?> agg, LogicalFilter<Plan> filter,
            List<Expression> unCorrelatedPredicate, CorrelatedAggregatePredicates predicates) {
        Set<Slot> correlationSlots = predicates.keySlots(apply.getCorrelationSlot());
        if (containsSensitiveExpression(apply.left(), correlationSlots)
                || hasNonDeterministicRows(apply.left())
                || containsNoneMovableFunction(apply.right())
                || containsSensitiveSubqueryExpression(apply, predicates)
                || referencesOuterSlot(apply.right(), ImmutableSet.copyOf(predicates.whereConjuncts),
                        apply.getCorrelationSlot())
                || !predicates.isResolvable(apply, agg, filter)) {
            return null;
        }

        // the domains of two outer rows are the same as soon as their correlation slots are equal,
        // so the correlation slots are the only outer information the aggregation needs
        LogicalPlan outer = (LogicalPlan) apply.left();
        LogicalPlan outerCopy = LogicalPlanDeepCopier.INSTANCE.deepCopy(outer, new DeepCopierContext());
        List<Slot> outerOutput = outer.getOutput();
        List<Slot> outerCopyOutput = outerCopy.getOutput();
        Preconditions.checkState(outerOutput.size() == outerCopyOutput.size(),
                "the deep copy of the outer plan changed its output size");
        Set<ExprId> outerExprIds = outerOutput.stream().map(Slot::getExprId).collect(ImmutableSet.toImmutableSet());
        for (Slot copySlot : outerCopyOutput) {
            if (outerExprIds.contains(copySlot.getExprId())) {
                // the deep copier does not always separate the slots of the copy from the plan it
                // copied (eg. LogicalTVFRelation.withRelationId reuses the logical properties of the
                // relation): the join which pairs an outer row with its correlation key would then
                // compare a slot with itself and keep every outer row
                return null;
            }
        }
        Map<Expression, Expression> slotToKey = Maps.newLinkedHashMap();
        for (Slot slot : correlationSlots) {
            int index = outerOutput.indexOf(slot);
            if (index < 0) {
                return null;
            }
            slotToKey.put(slot, outerCopyOutput.get(index));
        }
        List<NamedExpression> keyExpressions = new ArrayList<>(slotToKey.size());
        for (Expression key : slotToKey.values()) {
            keyExpressions.add((NamedExpression) key);
        }
        LogicalAggregate<Plan> keyAggregate = new LogicalAggregate<>(
                ImmutableList.copyOf(slotToKey.values()), keyExpressions, outerCopy);

        Plan inner = PlanUtils.filterOrSelf(ImmutableSet.copyOf(unCorrelatedPredicate), filter.child());
        // An aggregate with a group by returns no row at all for an empty correlated domain, so the
        // inner join below reproduces the behaviour of the subquery, which has no group to report.
        // A global aggregate returns exactly one row for every outer row instead, the empty domain
        // included, so the rows of an empty correlated domain have to be kept: the left outer join
        // gives the aggregate one row whose inner columns are null, which is the same input the
        // subquery aggregates for an empty input. Only the count aggregations tell "no row" and "one
        // row of nulls" apart, and count(*) also counts the kept row itself, so the counts are
        // replaced by counts of the projected marker, which is null for the rows which were kept for
        // an empty correlated domain.
        boolean keepEmptyDomain = agg.getGroupByExpressions().isEmpty();
        Slot matchMarker = null;
        if (keepEmptyDomain) {
            Alias marker = new Alias(BooleanLiteral.TRUE, CORRELATION_MATCH_MARKER);
            matchMarker = marker.toSlot();
            List<NamedExpression> projects = Lists.newArrayList(marker);
            projects.addAll(inner.getOutput());
            inner = new LogicalProject<>(projects, inner);
        }

        List<Expression> domainConjuncts = predicates.domainPredicates().stream()
                .map(conjunct -> ExpressionUtils.replace(conjunct, slotToKey))
                .collect(ImmutableList.toImmutableList());
        // a grouped aggregate reports an empty domain as "no row", a global aggregate as "one row"
        Plan domainJoin = new LogicalJoin<>(keepEmptyDomain ? JoinType.LEFT_OUTER_JOIN : JoinType.INNER_JOIN,
                ExpressionUtils.EMPTY_CONDITION, domainConjuncts, new DistributeHint(DistributeType.NONE),
                Optional.empty(), keyAggregate, inner, null);

        List<Expression> havingPredicates = predicates.havingPredicates();
        Set<AggregateFunction> aggregates = Sets.newLinkedHashSet();
        if (keepEmptyDomain) {
            // the aggregation of the rewrite is computed over the rows of the inner side plus the
            // row which the left outer join keeps for an empty correlated domain, and only the
            // aggregates which ignore null arguments see that row as an empty input
            for (Expression expression : agg.getOutputExpressions()) {
                aggregates.addAll(expression.collect(AggregateFunction.class::isInstance));
            }
            for (Expression conjunct : havingPredicates) {
                aggregates.addAll(conjunct.collect(AggregateFunction.class::isInstance));
            }
            if (aggregates.stream().anyMatch(function -> !(function instanceof NullIgnoringAggregateFunction))) {
                // an aggregate which keeps null arguments cannot tell the row kept for an empty
                // correlated domain from a row of the inner side
                return null;
            }
        }
        Map<Expression, Expression> compensated = guardAggregateArguments(aggregates, matchMarker);

        List<Expression> newGroupBy = Lists.newArrayList(slotToKey.values());
        newGroupBy.addAll(agg.getGroupByExpressions());
        List<NamedExpression> newOutputs = Lists.newArrayList(keyExpressions);
        for (NamedExpression output : agg.getOutputExpressions()) {
            newOutputs.add((NamedExpression) ExpressionUtils.replace(output, compensated));
        }
        LogicalAggregate<Plan> newAggregate = new LogicalAggregate<>(newGroupBy, newOutputs, domainJoin);

        // the predicates which were pulled into the apply are not part of the plan of the subquery
        // any more: they were evaluated on the rows of the old aggregation and have to be evaluated
        // on the rows of the new one
        Set<Expression> movedPredicates = Sets.newLinkedHashSet();
        for (Expression conjunct : predicates.pulledPredicates()) {
            movedPredicates.add(ExpressionUtils.replace(ExpressionUtils.replace(conjunct, compensated), slotToKey));
        }
        Plan newBase = movedPredicates.isEmpty() ? newAggregate
                : new LogicalFilter<>(movedPredicates, newAggregate);
        // the nodes above the aggregation of the subquery (the HAVING clause, the filters over the
        // projection of the select list, that projection) are kept as they are: they are evaluated on
        // the rows of the new aggregation, which are the rows of the aggregation of the subquery for
        // the correlation key of one outer row
        Plan newRight = exposeCorrelationKeys(replaceAggregate(apply.right(), newBase),
                slotToKey.values().stream().map(key -> (Slot) key).collect(ImmutableSet.toImmutableSet()));

        // the aggregate of the subquery is now computed for the correlation keys of every outer row,
        // so the outer rows which own one of those groups are the rows for which the subquery has rows
        List<Expression> backConjuncts = new ArrayList<>(slotToKey.size());
        for (Map.Entry<Expression, Expression> entry : slotToKey.entrySet()) {
            backConjuncts.add(new NullSafeEqual(entry.getKey(), entry.getValue()));
        }
        return new LogicalJoin<>(apply.isNot() ? JoinType.LEFT_ANTI_JOIN : JoinType.LEFT_SEMI_JOIN,
                ExpressionUtils.EMPTY_CONDITION, backConjuncts, new DistributeHint(DistributeType.NONE),
                apply.getMarkJoinSlotReference(), outer, newRight, null);
    }

    /**
     * Replace the arguments of the aggregates so that the row which the left outer join keeps for an
     * empty correlated domain does not contribute to the aggregation: the marker of that row is
     * null, so every argument is null for it and the aggregates which ignore null arguments (the
     * caller only admits those) return the value of an empty input. `count(*)` has no argument to
     * build that guard on, so it counts the marker instead; a distinct count keeps its argument and
     * its distinct flag, because the null of the kept row must not be counted as a value.
     */
    private static Map<Expression, Expression> guardAggregateArguments(
            Set<AggregateFunction> aggregates, Slot matchMarker) {
        Map<Expression, Expression> replace = Maps.newHashMap();
        for (AggregateFunction function : aggregates) {
            if (function instanceof Count && ((Count) function).isCountStar() && !function.isDistinct()) {
                replace.put(function, new Count(matchMarker));
                continue;
            }
            List<Expression> arguments = Lists.newArrayListWithCapacity(function.arity());
            for (Expression argument : function.getArguments()) {
                arguments.add(new If(matchMarker, argument, new NullLiteral(argument.getDataType())));
            }
            replace.put(function, function.withChildren(arguments));
        }
        return replace;
    }

    /**
     * Whether the HAVING clause of a global aggregate can hold for the row which the aggregate
     * returns for an empty input.
     */
    private static boolean havingMayHoldWithEmptyInput(LogicalAggregate<?> agg, Set<Expression> havingConjuncts) {
        // the having clause usually references the output slots of the aggregate, but it may also
        // contain the aggregate functions themselves, so both of them are replaced by the value
        // which the aggregate returns for an empty input
        Map<Expression, Expression> emptyValues = Maps.newHashMap();
        for (NamedExpression output : agg.getOutputExpressions()) {
            Expression expression = output instanceof Alias ? ((Alias) output).child() : output;
            if (!(expression instanceof AggregateFunction)) {
                continue;
            }
            Expression emptyValue = emptyValueForEmptyInput((AggregateFunction) expression);
            if (emptyValue != null) {
                emptyValues.put(output.toSlot(), emptyValue);
                emptyValues.put(expression, emptyValue);
            }
        }
        for (Expression conjunct : havingConjuncts) {
            for (Expression simpleConjunct : ExpressionUtils.extractConjunction(conjunct)) {
                Expression substituted = emptyValues.isEmpty() ? simpleConjunct
                        : ExpressionUtils.replace(simpleConjunct, emptyValues);
                Expression folded = FoldConstantRuleOnFE.evaluateWithoutContext(substituted);
                if (folded instanceof Literal && !BooleanLiteral.TRUE.equals(folded)) {
                    // false or null: this conjunct rejects the row of the empty input, no matter what
                    // the other conjuncts evaluate to, even the ones whose value is unknown
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * The value which an aggregate function returns for an empty input, or null if it cannot be
     * decided.
     */
    private static Expression emptyValueForEmptyInput(AggregateFunction function) {
        if (function instanceof Count) {
            return new BigIntLiteral(0);
        }
        if (function instanceof Sum || function instanceof Avg
                || function instanceof Min || function instanceof Max) {
            // these aggregations return null for an empty input
            return new NullLiteral(function.getDataType());
        }
        // only the aggregations above are known to return a fixed value for an empty input
        return null;
    }

    /**
     * The outer plan is evaluated twice by this rewrite (the original plan on the left of the
     * resulting join and a deep copy which computes the correlation keys), so the two evaluations
     * have to return the same rows and the same values of the correlation keys. A volatile
     * expression (eg. random()) is rejected when it decides which rows the plan returns (the
     * predicates and the groupings of the plan) or when it contributes to the value of a correlation
     * key, directly or through the slots of the expressions below: the values are followed through
     * the slots of the plan, so a volatile column which only decorates the output with a value the
     * rewrite does not use is accepted.
     * <p>
     * A NoneMovableFunction (the only implementation today is assert_true) is rejected wherever it
     * appears instead, even in such a decorative output: its evaluation must not be duplicated,
     * because the query has to raise its error where the query writes the function, not a second
     * time in the copy.
     *
     * @param correlationKeys the slots of the outer plan whose values are consumed as correlation
     *        keys by the rewrite and by the aggregation it builds
     */
    private static boolean containsSensitiveExpression(Plan plan, Set<Slot> correlationKeys) {
        if (containsNoneMovableFunction(plan)) {
            return true;
        }
        Set<Slot> volatileSlots = collectVolatileSlots(plan);
        return volatileSlots == null || volatileSlots.stream().anyMatch(correlationKeys::contains);
    }

    /** whether an expression of the plan is a function whose evaluation must not be duplicated */
    private static boolean containsNoneMovableFunction(Plan plan) {
        for (Expression expression : plan.getExpressions()) {
            if (expression.containsType(NoneMovableFunction.class)) {
                return true;
            }
        }
        for (Plan child : plan.children()) {
            if (containsNoneMovableFunction(child)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The slots of the plan whose value is computed from a volatile expression, at any level of the
     * plan, or null if a volatile expression contributes to the rows which the plan returns.
     */
    private static Set<Slot> collectVolatileSlots(Plan plan) {
        Set<Slot> volatileInput = Sets.newHashSet();
        for (Plan child : plan.children()) {
            Set<Slot> volatileChild = collectVolatileSlots(child);
            if (volatileChild == null) {
                return null;
            }
            volatileInput.addAll(volatileChild);
        }
        Set<Slot> volatileSlots = Sets.newHashSet(volatileInput);
        if (plan instanceof LogicalProject) {
            volatileSlots.addAll(volatileSlotsOfOutputs(((LogicalProject<?>) plan).getProjects(), volatileInput));
            return volatileSlots;
        }
        if (plan instanceof LogicalAggregate) {
            LogicalAggregate<?> aggregate = (LogicalAggregate<?>) plan;
            if (usesVolatile(aggregate.getGroupByExpressions(), volatileInput)) {
                // the grouping decides which rows the aggregate returns
                return null;
            }
            volatileSlots.addAll(volatileSlotsOfOutputs(aggregate.getOutputExpressions(), volatileInput));
            return volatileSlots;
        }
        if (plan instanceof LogicalFilter) {
            if (usesVolatile(((LogicalFilter<?>) plan).getConjuncts(), volatileInput)) {
                // the predicate decides which rows the filter returns
                return null;
            }
            return volatileSlots;
        }
        if (plan instanceof LogicalJoin) {
            LogicalJoin<?, ?> join = (LogicalJoin<?, ?>) plan;
            if (usesVolatile(join.getHashJoinConjuncts(), volatileInput)
                    || usesVolatile(join.getOtherJoinConjuncts(), volatileInput)
                    || usesVolatile(join.getMarkJoinConjuncts(), volatileInput)) {
                // the conditions decide which rows the join returns
                return null;
            }
            return volatileSlots;
        }
        if (plan instanceof LogicalSort) {
            // the order of the rows does not change the values of the slots
            return volatileSlots;
        }
        // this rewrite does not know how the other plans compute their output from the values below
        // them, so it cannot prove that a volatile value which reaches one of them cannot change the
        // rows or the correlation keys
        if (!volatileInput.isEmpty()) {
            return null;
        }
        for (Expression expression : plan.getExpressions()) {
            if (expression.containsVolatileExpression()) {
                return null;
            }
        }
        return volatileSlots;
    }

    /** the slots of the given outputs whose value is computed from one of the given volatile slots */
    private static Set<Slot> volatileSlotsOfOutputs(List<? extends NamedExpression> outputs,
            Set<Slot> volatileInput) {
        Set<Slot> volatileSlots = Sets.newHashSet();
        for (NamedExpression output : outputs) {
            if (usesVolatile(ImmutableList.of(output), volatileInput)) {
                volatileSlots.add(output.toSlot());
            }
        }
        return volatileSlots;
    }

    /** whether one of the expressions is volatile, directly or through one of the given slots */
    private static boolean usesVolatile(Collection<? extends Expression> expressions, Set<Slot> volatileSlots) {
        return expressions.stream().anyMatch(expression -> expression.containsVolatileExpression()
                || expression.getInputSlots().stream().anyMatch(volatileSlots::contains));
    }

    /**
     * Every node of the subquery is evaluated once for every correlation key by this rewrite, while
     * the original subquery evaluates it once for every outer row: two outer rows with the same
     * correlation key share one evaluation, so no value which the result of the subquery depends on
     * may change between two evaluations.
     * <p>
     * The volatile values of the subquery are followed through the slots of its plan (see
     * {@link #collectVolatileSlots}): a volatile value which only decorates an output that nothing
     * reads is accepted, while a volatile value which decides the rows of a node, the grouping of the
     * aggregation or the value of a predicate which decides the result of the subquery is rejected.
     * The predicates which were pulled into the apply are not part of the plan of the subquery any
     * more, so they are checked against the volatile values of the aggregation they read as well.
     */
    private static boolean containsSensitiveSubqueryExpression(LogicalApply<?, ?> apply,
            CorrelatedAggregatePredicates predicates) {
        for (Expression conjunct : predicates.havingPredicates()) {
            if (conjunct.containsType(NoneMovableFunction.class)) {
                // the pulled predicates live on the apply instead of the plan of the subquery, whose
                // functions the caller checks itself
                return true;
            }
        }
        Set<Slot> volatileSlots = collectVolatileSlots(apply.right());
        return volatileSlots == null || usesVolatile(predicates.havingPredicates(), volatileSlots);
    }

    /**
     * The outer plan is evaluated twice by this rewrite (the original plan on the left of the
     * resulting join and a deep copy which computes the correlation keys), and the rows of the two
     * evaluations have to carry the same correlation keys: the outer rows whose key the deep copy
     * did not produce find no row of the aggregation and are dropped by the semi/anti join.
     * <p>
     * A sort alone does not change the rows a plan returns, only the operator which truncates the
     * sorted rows can keep different rows in the two evaluations. That truncation is deterministic
     * only when the order keys are a total order on the rows: when the limit falls inside a group
     * of rows which are equal on the order keys, the query semantics allows any subset of that
     * group to be returned, and the two evaluations are two instances of the same plan in different
     * places of the resulting plan, so they can keep rows with different correlation keys. This
     * rule does not prove that the order keys are total, so every topn is rejected, together with
     * the limit without an order (which returns arbitrary rows) and the sampled scan (whose two
     * evaluations sample different rows).
     */
    private static boolean hasNonDeterministicRows(Plan plan) {
        if (plan instanceof LogicalLimit || plan instanceof LogicalTopN) {
            return true;
        }
        if (plan instanceof LogicalOlapScan && ((LogicalOlapScan) plan).getTableSample().isPresent()) {
            return true;
        }
        for (Plan child : plan.children()) {
            if (hasNonDeterministicRows(child)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The aggregation below the correlated predicate can only provide the correlation keys, so the
     * correlated predicates must be the only place where the subquery references the outer query.
     * Nested subqueries keep their own correlation bookkeeping which this rewrite does not update,
     * so they are rejected as well.
     */
    private static boolean referencesOuterSlot(Plan plan, Set<Expression> correlatedPredicate,
            List<Slot> correlationSlots) {
        Set<ExprId> correlationSlotIds = correlationSlots.stream()
                .map(Slot::getExprId)
                .collect(ImmutableSet.toImmutableSet());
        return referencesOuterSlot(plan, correlatedPredicate, correlationSlotIds);
    }

    private static boolean referencesOuterSlot(Plan plan, Set<Expression> correlatedPredicate,
            Set<ExprId> correlationSlotIds) {
        if (plan instanceof LogicalApply) {
            return true;
        }
        for (Expression expression : plan.getExpressions()) {
            if (!correlatedPredicate.contains(expression)
                    && expression.getInputSlotExprIds().stream().anyMatch(correlationSlotIds::contains)) {
                return true;
            }
        }
        for (Plan child : plan.children()) {
            if (referencesOuterSlot(child, correlatedPredicate, correlationSlotIds)) {
                return true;
            }
        }
        return false;
    }
}
