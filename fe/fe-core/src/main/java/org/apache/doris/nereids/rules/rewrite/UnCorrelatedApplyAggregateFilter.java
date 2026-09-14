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
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
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
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
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
        return ImmutableList.of(
                logicalApply(any(), logicalAggregate(logicalFilter()))
                        .when(LogicalApply::isCorrelated)
                        .then(UnCorrelatedApplyAggregateFilter::pullUpCorrelatedFilter)
                        .toRule(RuleType.UN_CORRELATED_APPLY_AGGREGATE_FILTER),
                logicalApply(any(), logicalFilter(logicalAggregate(logicalFilter())))
                        .when(LogicalApply::isCorrelated)
                        .then(UnCorrelatedApplyAggregateFilter::pullUpCorrelatedFilter)
                        .toRule(RuleType.UN_CORRELATED_APPLY_FILTER_AGGREGATE_FILTER),
                // the analyzer can leave a passthrough projection above the aggregate (eg. the
                // projection of the select list of the subquery), the subquery has to be rewritten
                // through that projection as well, otherwise the correlated predicates of the
                // subquery are dropped after the apply has been turned into a join
                logicalApply(any(), logicalProject(logicalAggregate(logicalFilter())))
                        .when(LogicalApply::isCorrelated)
                        .then(UnCorrelatedApplyAggregateFilter::pullUpCorrelatedFilter)
                        .toRule(RuleType.UN_CORRELATED_APPLY_AGGREGATE_FILTER),
                logicalApply(any(), logicalProject(logicalFilter(logicalAggregate(logicalFilter()))))
                        .when(LogicalApply::isCorrelated)
                        .then(UnCorrelatedApplyAggregateFilter::pullUpCorrelatedFilter)
                        .toRule(RuleType.UN_CORRELATED_APPLY_FILTER_AGGREGATE_FILTER));
    }

    private static Plan pullUpCorrelatedFilter(LogicalApply<?, ?> apply) {
        // the correlated predicates of the HAVING clause (the filter below the apply) may already have
        // been pulled into the apply, and a projection which only exposes the output of the aggregate
        // may sit between the apply and the aggregate. Walk down to the aggregate and remember the
        // filter which still holds predicates of the HAVING clause.
        Plan below = apply.right();
        Optional<LogicalFilter<Plan>> havingFilter = Optional.empty();
        while (!(below instanceof LogicalAggregate)) {
            if (below instanceof LogicalFilter) {
                havingFilter = Optional.of((LogicalFilter<Plan>) below);
            }
            below = below.child(0);
        }
        LogicalAggregate<LogicalFilter<Plan>> agg = (LogicalAggregate<LogicalFilter<Plan>>) below;
        LogicalFilter<Plan> filter = agg.child();
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
        if (needCorrelatedAggregationOnOuter(apply, agg, havingFilter.isEmpty(), correlatedPredicate, predicates)) {
            Plan aggregatedOuter = pullUpCorrelatedPredicateByAggregatingOuter(
                    apply, agg, filter, unCorrelatedPredicate, predicates);
            if (aggregatedOuter != null) {
                return aggregatedOuter;
            }
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
        return new LogicalApply<>(apply.getCorrelationSlot(), apply.getSubqueryType(), apply.isNot(),
                apply.getCompareExpr(), apply.getTypeCoercionExpr(),
                ExpressionUtils.optionalAnd(correlatedPredicate), apply.getMarkJoinSlotReference(),
                apply.isNeedAddSubOutputToProjects(), apply.isMarkJoinSlotNotNull(), apply.left(),
                replaceAggregate(apply.right(), newAgg));
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
        private final List<Expression> pulledDomainPredicates = Lists.newArrayList();
        private final List<Expression> aggregatePredicates = Lists.newArrayList();
        private final Set<Expression> havingConjuncts = Sets.newLinkedHashSet();

        private static CorrelatedAggregatePredicates of(LogicalApply<?, ?> apply,
                LogicalAggregate<LogicalFilter<Plan>> agg, LogicalFilter<Plan> filter,
                Optional<LogicalFilter<Plan>> havingFilter, List<Expression> whereConjuncts) {
            CorrelatedAggregatePredicates predicates = new CorrelatedAggregatePredicates();
            predicates.whereConjuncts.addAll(whereConjuncts);
            // the slots which exist above the aggregate tell whether a predicate was evaluated on the
            // aggregation (HAVING clause) instead of on the inner rows (WHERE clause)
            Set<ExprId> belowAggregate = filter.child().getOutput().stream()
                    .map(Slot::getExprId)
                    .collect(ImmutableSet.toImmutableSet());
            Set<ExprId> aggregateOutput = agg.getOutput().stream()
                    .map(Slot::getExprId)
                    .filter(exprId -> !belowAggregate.contains(exprId))
                    .collect(ImmutableSet.toImmutableSet());
            apply.getCorrelationFilter()
                    .map(ExpressionUtils::extractConjunction)
                    .orElse(ImmutableList.of())
                    .forEach(conjunct -> {
                        if (conjunct.getInputSlotExprIds().stream().anyMatch(aggregateOutput::contains)) {
                            predicates.aggregatePredicates.add(conjunct);
                        } else {
                            predicates.pulledDomainPredicates.add(conjunct);
                        }
                    });
            havingFilter.ifPresent(remaining -> predicates.havingConjuncts.addAll(remaining.getConjuncts()));
            return predicates;
        }

        private List<Expression> domainPredicates() {
            return ImmutableList.<Expression>builder()
                    .addAll(whereConjuncts)
                    .addAll(pulledDomainPredicates)
                    .build();
        }

        private List<Expression> havingPredicates() {
            return ImmutableList.<Expression>builder()
                    .addAll(havingConjuncts)
                    .addAll(aggregatePredicates)
                    .build();
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
         * uses the correlation keys, the inner rows and the output of the aggregate.
         */
        private boolean isResolvable(LogicalApply<?, ?> apply, LogicalAggregate<?> agg,
                LogicalFilter<Plan> filter) {
            Set<ExprId> allowed = Sets.newHashSet();
            apply.getCorrelationSlot().forEach(slot -> allowed.add(slot.getExprId()));
            agg.getOutput().forEach(slot -> allowed.add(slot.getExprId()));
            filter.child().getOutput().forEach(slot -> allowed.add(slot.getExprId()));
            return domainPredicates().stream().allMatch(conjunct -> allowed.containsAll(conjunct.getInputSlotExprIds()))
                    && havingPredicates().stream()
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
     * @return null if this rewrite cannot be applied safely, the caller then keeps the original rewrite
     */
    private static Plan pullUpCorrelatedPredicateByAggregatingOuter(LogicalApply<?, ?> apply,
            LogicalAggregate<LogicalFilter<Plan>> agg, LogicalFilter<Plan> filter,
            List<Expression> unCorrelatedPredicate, CorrelatedAggregatePredicates predicates) {
        if (containsSensitiveExpression(apply.left())
                || referencesOuterSlot(apply.right(), ImmutableSet.copyOf(predicates.whereConjuncts),
                        apply.getCorrelationSlot())
                || !predicates.isResolvable(apply, agg, filter)) {
            return null;
        }
        Set<Slot> correlationSlots = predicates.keySlots(apply.getCorrelationSlot());

        // the domains of two outer rows are the same as soon as their correlation slots are equal,
        // so the correlation slots are the only outer information the aggregation needs
        LogicalPlan outer = (LogicalPlan) apply.left();
        LogicalPlan outerCopy = LogicalPlanDeepCopier.INSTANCE.deepCopy(outer, new DeepCopierContext());
        List<Slot> outerOutput = outer.getOutput();
        List<Slot> outerCopyOutput = outerCopy.getOutput();
        Preconditions.checkState(outerOutput.size() == outerCopyOutput.size(),
                "the deep copy of the outer plan changed its output size");
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
        Set<Expression> countExpressions = Sets.newLinkedHashSet();
        if (keepEmptyDomain) {
            // the count aggregations are the only ones which count the row kept for an empty domain
            for (Expression expression : agg.getOutputExpressions()) {
                countExpressions.addAll(expression.collect(Count.class::isInstance));
            }
            for (Expression conjunct : havingPredicates) {
                countExpressions.addAll(conjunct.collect(Count.class::isInstance));
            }
        }
        Map<Expression, Expression> compensated = compensateCounts(countExpressions, matchMarker);

        List<Expression> newGroupBy = Lists.newArrayList(slotToKey.values());
        newGroupBy.addAll(agg.getGroupByExpressions());
        List<NamedExpression> newOutputs = Lists.newArrayList(keyExpressions);
        for (NamedExpression output : agg.getOutputExpressions()) {
            newOutputs.add((NamedExpression) ExpressionUtils.replace(output, compensated));
        }
        LogicalAggregate<Plan> newAggregate = new LogicalAggregate<>(newGroupBy, newOutputs, domainJoin);

        Set<Expression> newHavingConjuncts = Sets.newLinkedHashSet();
        for (Expression conjunct : havingPredicates) {
            newHavingConjuncts.add(ExpressionUtils.replace(ExpressionUtils.replace(conjunct, compensated), slotToKey));
        }
        Plan newRight = newHavingConjuncts.isEmpty() ? newAggregate
                : new LogicalFilter<>(newHavingConjuncts, newAggregate);

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
     * Replace the count aggregations with a form which does not count the row that is kept for an
     * empty correlated domain: the argument of every count is null for that row, while count(*)
     * counts the row itself.
     */
    private static Map<Expression, Expression> compensateCounts(Set<Expression> countExpressions, Slot matchMarker) {
        Map<Expression, Expression> replace = Maps.newHashMap();
        for (Expression expression : countExpressions) {
            Count count = (Count) expression;
            if (count.isCountStar()) {
                replace.put(count, new Count(matchMarker));
            } else {
                List<Expression> arguments = Lists.newArrayListWithCapacity(count.arity());
                for (Expression argument : count.getArguments()) {
                    arguments.add(new If(matchMarker, argument, new NullLiteral(argument.getDataType())));
                }
                replace.put(count, count.withChildren(arguments));
            }
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
            Expression substituted = emptyValues.isEmpty() ? conjunct
                    : ExpressionUtils.replace(conjunct, emptyValues);
            Expression folded = FoldConstantRuleOnFE.evaluateWithoutContext(substituted);
            if (!(folded instanceof Literal)) {
                // the aggregate returns an unknown value for an empty input, rewrite conservatively
                return true;
            }
            if (!BooleanLiteral.TRUE.equals(folded)) {
                // false or null: this conjunct filters the row of the empty input out
                return false;
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
        // only count is known to return a non null value for an empty input
        return null;
    }

    /**
     * The outer plan is evaluated twice by this rewrite, which is not allowed for expressions with
     * side effects or with a value that changes between two evaluations.
     */
    private static boolean containsSensitiveExpression(Plan plan) {
        for (Expression expression : plan.getExpressions()) {
            if (expression.containsVolatileExpression() || expression.containsType(NoneMovableFunction.class)) {
                return true;
            }
        }
        for (Plan child : plan.children()) {
            if (containsSensitiveExpression(child)) {
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
