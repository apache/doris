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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.copier.DeepCopierContext;
import org.apache.doris.nereids.trees.copier.LogicalPlanDeepCopier;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.VolatileExpression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.NoneMovableFunction;
import org.apache.doris.nereids.trees.expressions.functions.Udf;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.NotNullableAggregateFunction;
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
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Pull the correlated predicates of a subquery which aggregates out of the subquery, so that the
 * aggregation computes the aggregation of the correlated domain of one outer row instead of the
 * aggregation of all the inner rows of the subquery.
 *
 * The correlated predicates sit in the filter below the aggregation of the subquery before the
 * rewrite (its WHERE clause): the right side of the apply is the nodes above that filter (the
 * HAVING clause, the projections of the select list, the aggregation), the filter itself and the
 * inner table. This rule pulls the correlated conjuncts of that filter into the apply (they become
 * the conditions which unnest the apply) and rebuilds the aggregation around them, keeping the
 * nodes above it in place; the uncorrelated conjuncts stay in the filter below the aggregation.
 *
 * The shape of the subquery selects one of two strategies for the new aggregation:
 *
 * The aggregation stays on the inner side when the inner rows of one outer row are exactly the
 * groups of the aggregation: the inner side of the correlated predicate is added to the group by
 * of the aggregation and to its output (so that the conditions of the apply can read the key), and
 * every outer row is paired with the group of its own domain. This needs an equality between the
 * outer side and the inner side (eg. t2.c1 = t1.c1), because the value of the inner side is what
 * selects the group of the outer row. For the subquery of
 *
 *     select t1.c1 from t1 where exists (select count(*) from t2
 *         where t2.c1 = t1.c1 group by t2.c2 having count(*) > 0)
 *
 * the rewritten plan is (the EXISTS reads the aggregation through a semi join, see below):
 *
 *     LEFT SEMI JOIN (t2.c1 = t1.c1)                       [the correlation filter of the apply]
 *       |-- t1
 *       +-- Filter(count(*) > 0)                           [the HAVING clause, kept in place]
 *             +-- Aggregate(group by [t2.c1, t2.c2], output [t2.c1, t2.c2, count(*)])
 *                   +-- t2
 *
 * The aggregation is built on the outer side in every other case: a deep copy of the outer plan
 * computes the distinct correlation keys, the keys are joined with the inner side on the
 * predicates of the domain, the result is grouped by the keys, and every outer row is paired with
 * the aggregation of its own key (see pullUpCorrelatedPredicateByAggregatingOuter). For the
 * subquery of
 *
 *     select t1.c1 from t1 where exists (select count(*) from t2
 *         where t2.c1 < t1.c1 group by t2.c2 having count(*) = 2)
 *
 * the rewritten plan is:
 *
 *     LEFT SEMI JOIN (t1.c1 <=> key.c1)
 *       |-- t1
 *       +-- Filter(count(*) = 2)
 *             +-- Aggregate(group by [key.c1, t2.c2], output [key.c1, t2.c2, count(*)])
 *                   +-- INNER JOIN (t2.c1 < key.c1)
 *                         |-- Aggregate(group by [t1.c1], output [t1.c1])    [the keys of t1]
 *                         |     +-- t1
 *                         +-- t2
 *
 * The two properties which select the strategy are independent: whether the correlated predicate
 * is an equality or not, and whether the aggregation has a group by or not (a global aggregate).
 * The four combinations:
 *
 * 1. equality + group by: inner side (the first example). The domain of an outer row is exactly
 *    the group whose key is the value of the inner side, so the HAVING clause of that group is the
 *    HAVING clause of the outer row, and an outer row with an empty domain has no group at all,
 *    exactly like the grouped aggregate of the subquery.
 *
 * 2. non-equality + group by: outer side (the second example). The domain of an outer row is the
 *    union of several groups, so the aggregates of the subquery (eg. count(*)) are computed over
 *    the union, while one group of the inner side holds a part of the domain only.
 *
 * 3. non-equality, no group by: outer side, for the reason of 2. For the subquery of
 *
 *        select t1.c1 from t1 where exists (select count(*) from t2
 *            where t2.c1 < t1.c1 having count(*) = 0)
 *
 *    the plan is the plan of 2. without the group by of the subquery, with the row of the empty
 *    domain kept by a left outer join (a global aggregate returns a row for an empty correlated
 *    domain, and the count of that row has to be 0):
 *
 *        LEFT SEMI JOIN (t1.c1 <=> key.c1)
 *          |-- t1
 *          +-- Filter(count($correlation_match_marker) = 0)
 *                +-- Aggregate(group by [key.c1], output [key.c1, count($correlation_match_marker)])
 *                      +-- LEFT OUTER JOIN (t2.c1 < key.c1)                 [keeps the empty domain]
 *                            |-- Aggregate(group by [t1.c1], output [t1.c1])
 *                            |     +-- t1
 *                            +-- Project([true AS $correlation_match_marker, t2.c1])
 *                                  +-- t2
 *
 * 4. equality, no group by: inner side when the subquery does not need the row which the global
 *    aggregate returns for an empty correlated domain, outer side when it does, because that row
 *    has no group on the inner side to be produced from. For the subquery of
 *
 *        select t1.c1 from t1 where exists (select count(*) from t2
 *            where t2.c1 = t1.c1 having count(*) > 0)
 *
 *    the HAVING clause is false for the empty input, so the row of the empty domain disappears
 *    from the result anyway and the aggregation stays on the inner side (the plan of 1. without
 *    the group by of the subquery); for the subquery of
 *
 *        select t1.c1 from t1 where exists (select count(*) from t2
 *            where t2.c1 = t1.c1 having count(*) = 0)
 *
 *    the row of the empty domain survives (its count is 0), so the aggregation is built on the
 *    outer side (the plan of 3. with the equality of the domain):
 *
 *        LEFT SEMI JOIN (t1.c1 <=> key.c1)
 *          |-- t1
 *          +-- Filter(count($correlation_match_marker) = 0)
 *                +-- Aggregate(group by [key.c1], output [key.c1, count($correlation_match_marker)])
 *                      +-- LEFT OUTER JOIN (t2.c1 = key.c1)                  [keeps the empty domain]
 *                            |-- Aggregate(group by [t1.c1], output [t1.c1])
 *                            |     +-- t1
 *                            +-- Project([true AS $correlation_match_marker, t2.c1])
 *                                  +-- t2
 *
 * The three subquery types differ in what they need from the aggregation (see
 * needCorrelatedAggregationOnOuter for the exact conditions):
 *
 * - EXISTS/NOT EXISTS is decided by the aggregation, which this rule reads through a LEFT SEMI
 *   JOIN (a LEFT ANTI JOIN for NOT EXISTS): it needs the row of an empty correlated domain only
 *   when that row can decide the subquery (a HAVING clause which may hold for it, or a node which
 *   the subquery keeps above its aggregation), see the examples of 4.
 * - IN/NOT IN compares the outer expression with the value of the subquery, which is the first
 *   column of the aggregation (the keys are appended after the outputs of the subquery, so this
 *   column does not move). The row which a global aggregate returns for an empty correlated domain
 *   is part of the values the IN compares: the subquery of
 *
 *       select t1.c1 from t1 where t1.c1 in (select count(*) from t2 where t2.c1 = t1.c1)
 *
 *   compares the outer row with the count 0 of its empty domain (k in (0) holds for the outer row
 *   0), so its aggregation is built on the outer side (the plan of 3. with the count of the
 *   marker) and the rewrite keeps the apply, whose correlation filter pairs every outer row with
 *   the aggregation of its own key; the rule which converts the IN into a join then compares the
 *   outer expression with the first column of that aggregation. That row is needed for every
 *   aggregate, whatever value it returns for an empty input: the aggregation of the inner side has
 *   no group for an outer row without a match, and the comparison which the rewrite of the IN builds
 *   reads the rows of the domain of one outer row, so the null which the left outer join keeps for it
 *   is not the value of its empty domain for the IN. The not in of
 *
 *       select t1.c1 from t1 where t1.c1 not in (select sum(t2.c2) from t2 where t2.c1 = t1.c1)
 *
 *   returns true for the outer rows without a match instead of the null which the sum of the empty
 *   domain produces. A grouped aggregate has no value for an empty domain, so the subquery of
 *
 *       select t1.c1 from t1 where t1.c1 in (select count(*) from t2
 *           where t2.c1 = t1.c1 group by t2.c2)
 *
 *   keeps the aggregation on the inner side (the plan of 1. with the projection of the select list
 *   kept below the apply, see PullUpProjectUnderApply):
 *
 *       Apply(IN, correlationFilter=[(t2.c1 = t1.c1)])
 *         |-- t1
 *         +-- Project([count(*), t2.c1])                    [the select list, the key appended]
 *               +-- Aggregate(group by [t2.c1, t2.c2], output [t2.c2, count(*), t2.c1])
 *                     +-- t2
 * - scalar exposes the value of the aggregation as the value of the subquery. The left outer join
 *   which pairs an outer row with the aggregation of its own key returns null for the outer rows
 *   whose domain is empty, which is the value of those outer rows for the aggregates whose value
 *   for an empty input the rewrite reads that way (see keepsTheValueOfAnEmptyDomain): the subquery of
 *
 *       select t1.c1, (select max(t2.c2) from t2 where t2.c1 = t1.c1) from t1
 *
 *   keeps the aggregation on the inner side (max returns null for an empty input, which is the
 *   null of the join), while the subquery of
 *
 *       select t1.c1, (select count(*) from t2 where t2.c1 < t1.c1) from t1
 *
 *   is built on the outer side (the correlated predicate is not an equality, and the count of an
 *   empty domain is 0, which no null of the inner side can produce):
 *
 *       LEFT OUTER JOIN (t1.c1 <=> key.c1)
 *         |-- t1
 *         +-- Aggregate(group by [key.c1], output [key.c1, count($correlation_match_marker)])
 *               +-- LEFT OUTER JOIN (t2.c1 < key.c1)                      [keeps the empty domain]
 *                     |-- Aggregate(group by [t1.c1], output [t1.c1])
 *                     |     +-- t1
 *                     +-- Project([true AS $correlation_match_marker, t2.c1])
 *                           +-- t2
 *
 * To summarize, the aggregation of the subquery stays on the inner side for the subqueries whose
 * correlated domain of one outer row is one group of the aggregation and whose empty domain needs
 * no row: every equality predicate with a group by, the global aggregates whose HAVING clause
 * rejects the empty input (or whose value for the empty input is the null which the left outer
 * join keeps), the grouped IN subqueries, and the scalar subqueries of the aggregates whose empty
 * value the rewrite reads from that null. It is built on the outer side for the subqueries whose
 * domain is several groups of the aggregation (a non-equality predicate, whatever the subquery type
 * and the aggregate are), and for the subqueries which need the row of an empty correlated domain (a
 * global aggregate whose HAVING clause may hold for the empty input, an IN subquery which compares
 * the value of a global aggregate, an EXISTS subquery which keeps nodes above its aggregation, a
 * scalar subquery whose empty value is not the null of the join).
 *
 * Keeping the aggregation on the inner side in the cases of the outer side returns a wrong result,
 * which is why the rewrite of the outer side has to exist. The subquery of
 *
 *     select cq_o.k from cq_o where exists (select count(*) from cq_i
 *         where cq_i.k = cq_o.k having count(*) = 0)
 *
 * returns the outer row 7 when its domain is empty (the count of an empty domain is 0, which the
 * HAVING clause accepts), while the aggregation of the inner side groups the inner rows by
 * cq_i.k: the outer row 7 has no group there, so its row would be dropped. The subquery of
 *
 *     select cn_o.k from cn_o where exists (select count(*) from cn_i
 *         where cn_i.k < cn_o.k group by cn_i.g having count(*) = 2)
 *
 * returns the outer row 3 when its domain is the two rows (1, 10) and (2, 10) of the single group
 * g = 10 (the count of the domain is 2), while the aggregation of the inner side groups the inner
 * rows by (cn_i.k, cn_i.g): those two rows are two groups of count 1 and the HAVING clause would
 * never hold.
 */
public class UnCorrelatedApplyAggregateFilter implements RewriteRuleFactory {

    /**
     * name of the projected column which tells whether an inner row matched the correlated
     * predicate: the left outer join of the aggregation of the outer side keeps one row whose
     * marker is null for every correlation key which has no inner row, so the aggregations of this
     * column see the empty correlated domain as an empty input (see guardAggregateArguments). For
     * example the plan of the second case of needCorrelatedAggregationOnOuter evaluates
     * count($correlation_match_marker) over the rows of Project([true AS
     * $correlation_match_marker, t2.c1]), and the row which the left outer join keeps for an empty
     * domain has a null marker, which that count does not count.
     */
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
     * The aggregation of the subquery: the aggregates between the apply and the filter which holds
     * the predicates of its WHERE clause (from the one below the apply to the one above that
     * filter), the filters whose predicates decide which rows of that aggregation survive (the
     * HAVING clause of the subquery), and the filter of the WHERE clause itself.
     *
     * The nodes of the subquery of
     *
     *     select t1.c1 from t1 where exists (
     *         select sum(x.c) from (select count(*) as c from t2
     *             where t2.c1 = t1.c1 group by t2.c2 having count(*) > 1) x having sum(x.c) > 2)
     *
     * before the rewrite, with the member which holds every node:
     *
     *     Apply(correlationFilter=empty)                                    [the apply of the subquery]
     *       |-- t1
     *       +-- Filter(sum(x.c) > 2)               [havingFilter, filtersAboveTheAggregation(0)]
     *             +-- Aggregate(group by [], output [sum(c) as sum(x.c)])     [chain(0) =
     *                   +-- Project([c])                                         topAggregation]
     *                         +-- Filter(c > 1)                       [filtersAboveTheAggregation(1)]
     *                               +-- Aggregate(group by [t2.c2], output [t2.c2, count(*) as c])
     *                               ...                                       [chain(1) =
     *                                     +-- Project([t2.c2])                 domainAggregation]
     *                                           +-- Filter(t2.c1 = t1.c1)     [domainFilter]
     *                                                 +-- t2
     *
     * - chain: every aggregate between the apply and the filter of the WHERE clause, from the top
     *   down (here the sum over the derived table, then the count of that table);
     * - topAggregation: chain(0), the aggregate below the apply, which the whole chain is rebuilt
     *   around;
     * - domainAggregation: the deepest aggregate of the chain, which reads the rows of the filter
     *   of the WHERE clause;
     * - filtersAboveTheAggregation: the filters whose predicates decide on the rows of the
     *   aggregation below them (here both HAVING clauses: the sum(x.c) > 2 of the subquery and the
     *   count(*) > 1 of the derived table); a filter whose predicate selects the rows of the
     *   domain of an outer row instead is not one of them, see selectsTheRowsOfTheDomain;
     * - havingFilter: the deepest filter above the topAggregation, i.e. the first filter met when
     *   walking down from the apply (here the filter of sum(x.c) > 2, which sits directly above
     *   the aggregation of the sum);
     * - domainFilter: the filter which holds the predicates of the WHERE clause (here
     *   t2.c1 = t1.c1, which the walk finds when it stops below the deepest aggregate).
     *
     * A subquery may wrap its aggregation with further aggregations: SubqueryToApply adds a
     * count(*)/any_value(*) aggregation above the aggregation of a correlated scalar subquery whose
     * output is used in the outer scope and which has no top level scalar aggregation (the count is
     * the runtime check of the scalar subquery, the any_value is its value), and the user may
     * aggregate the aggregation of a derived table (the example above). The rows of the subquery
     * for one outer row are the rows of the whole chain for the correlation key of that row, so
     * every aggregate of the chain has to keep the keys in its group by (see pullUpCorrelatedFilter
     * and withTheKeysInTheGroupBy).
     */
    private static final class TheAggregation {
        /** the aggregates between the apply and the filter of the WHERE clause, from the top down */
        private final List<LogicalAggregate<?>> chain;
        /** every filter above the deepest aggregate, whose predicates decide on the aggregation rows */
        private final List<LogicalFilter<Plan>> filtersAboveTheAggregation;
        /** the deepest filter above the top aggregate, which sits directly below the projections above it */
        private final Optional<LogicalFilter<Plan>> havingFilter;
        /** the filter which holds the predicates of the WHERE clause of the subquery */
        private final LogicalFilter<Plan> domainFilter;

        private TheAggregation(List<LogicalAggregate<?>> chain,
                List<LogicalFilter<Plan>> filtersAboveTheAggregation,
                Optional<LogicalFilter<Plan>> havingFilter, LogicalFilter<Plan> domainFilter) {
            this.chain = chain;
            this.filtersAboveTheAggregation = filtersAboveTheAggregation;
            this.havingFilter = havingFilter;
            this.domainFilter = domainFilter;
        }

        /** the aggregate which reads the rows of the filter, at the bottom of the chain */
        private LogicalAggregate<?> domainAggregation() {
            return chain.get(chain.size() - 1);
        }

        /** the aggregate below the apply, which the whole chain is rebuilt around */
        private LogicalAggregate<?> topAggregation() {
            return chain.get(0);
        }

        /** the aggregates between the apply and the filter, from the top down */
        private List<LogicalAggregate<?>> aggregationChain() {
            return chain;
        }

        /** every filter above the deepest aggregate, whose predicates decide on the aggregation rows */
        private List<LogicalFilter<Plan>> filtersAboveTheAggregation() {
            return filtersAboveTheAggregation;
        }

        /** the filter which holds the predicates of the WHERE clause of the subquery */
        private LogicalFilter<Plan> domainFilter() {
            return domainFilter;
        }

        /** whether the subquery does not wrap its aggregation with another aggregation */
        private boolean onlyTheAggregationOfTheDomain() {
            return chain.size() == 1;
        }
    }

    /**
     * Locate the aggregation of the subquery, walking down from the right side of the apply: the
     * nodes above its aggregates are the projections and the filters of the subquery, the
     * projections between its aggregates only carry the columns which they need, and the filter
     * below the deepest aggregate holds the predicates of its WHERE clause. The walk stops at a
     * filter whose child is not an aggregate: that filter is the filter of the WHERE clause, while
     * a filter which sits directly above an aggregate (through projections) is a HAVING clause of
     * that aggregate.
     *
     * The steps of the walk for the subquery of
     *
     *     select t1.c1 from t1 where exists (
     *         select sum(x.c) from (select count(*) as c from t2
     *             where t2.c1 = t1.c1 group by t2.c2 having count(*) > 1) x having sum(x.c) > 2)
     *
     * whose plan is (see TheAggregation):
     *
     *     Apply(t1, Filter(sum(x.c) > 2) - Aggregate(sum) - Project([c]) - Filter(c > 1)
     *         - Aggregate(count) - Project([t2.c2]) - Filter(t2.c1 = t1.c1) - t2)
     *
     * 1. the first loop walks down from the apply while the node is not an aggregate: the filter
     *    sum(x.c) > 2 is remembered as the havingFilter and added to filtersAboveTheAggregation,
     *    and the walk stops at the aggregate of the sum (the topAggregation of the chain);
     * 2. the second loop walks down from that aggregate: the projection of c carries a column of
     *    the node below it alone, so it is walked through; the filter c > 1 sits above the
     *    aggregate of the count (through that projection) and its predicate reads the output of
     *    that aggregate, so it decides on its rows (a HAVING clause of that aggregate): it is
     *    added to filtersAboveTheAggregation and the chain continues below it;
     * 3. the aggregate of the count is the deepest aggregate of the chain (the domainAggregation,
     *    which reads the rows of the filter of the WHERE clause);
     * 4. the projection of t2.c2 carries a column of the node below it alone, so it is walked
     *    through, and the walk stops at the filter t2.c1 = t1.c1: its child is a scan and not an
     *    aggregate, so it is the domainFilter, which holds the predicates of the WHERE clause (the
     *    correlated predicate is one of them). The walk returns an empty result (the rule leaves
     *    the apply alone) when a node of the subquery is neither a projection nor a filter nor an
     *    aggregate, when a projection below an aggregate computes one of its columns instead of
     *    passing a column of the node below it through, and when the filter of the WHERE clause
     *    has no child.
     */
    private static Optional<TheAggregation> locateAggregate(LogicalApply<?, ?> apply) {
        Plan below = apply.right();
        Optional<LogicalFilter<Plan>> havingFilter = Optional.empty();
        List<LogicalFilter<Plan>> filtersAboveTheAggregation = Lists.newArrayList();
        while (!(below instanceof LogicalAggregate)) {
            if (below instanceof LogicalFilter) {
                havingFilter = Optional.of((LogicalFilter<Plan>) below);
                filtersAboveTheAggregation.add((LogicalFilter<Plan>) below);
            } else if (!(below instanceof LogicalProject)) {
                return Optional.empty();
            }
            below = below.child(0);
        }
        List<LogicalAggregate<?>> chain = Lists.newArrayList();
        chain.add((LogicalAggregate<?>) below);
        Plan belowAggregate = below.child(0);
        LogicalFilter<Plan> domainFilter = null;
        while (true) {
            if (belowAggregate instanceof LogicalProject) {
                // the projections between the aggregates of the chain are kept by the rewrite, which
                // exposes the keys through them (see rebuildTheAggregationChain), so a projection
                // which computes its columns is accepted when it reads the rows of an aggregate of
                // the chain: the aggregation below it and the aggregation above it are rebuilt
                // around it. The projection which carries the columns of the rows below the
                // aggregation of the domain into that aggregation, on the other hand, is dropped
                // together with the filter of the WHERE clause (the rewritten aggregation reads the
                // child of that filter), so it may only pass those columns through. For example the
                // projection of the subquery of
                //
                //     select o.k, (select count(*) + 1 from i where i.k = o.k group by i.g) from o
                //
                // computes the value which the wrapper aggregation of the scalar subquery reads
                // (count(*) + 1), and it sits between that aggregation and the aggregation of the
                // count: it is kept, while the projection of the example of locateAggregate may only
                // carry the column below it (the sum of the subquery aggregates that column).
                Plan belowTheProjections = belowAggregate.child(0);
                while (belowTheProjections instanceof LogicalProject) {
                    belowTheProjections = belowTheProjections.child(0);
                }
                if (!(belowTheProjections instanceof LogicalAggregate)) {
                    for (NamedExpression project : ((LogicalProject<?>) belowAggregate).getProjects()) {
                        if (!(project instanceof Slot)) {
                            // the projection computes the columns of the nodes above it itself, so it
                            // cannot be replaced together with the aggregation below it
                            return Optional.empty();
                        }
                    }
                }
                belowAggregate = belowAggregate.child(0);
                continue;
            }
            if (belowAggregate instanceof LogicalAggregate) {
                chain.add((LogicalAggregate<?>) belowAggregate);
                belowAggregate = belowAggregate.child(0);
                continue;
            }
            if (belowAggregate instanceof LogicalFilter) {
                Plan belowTheFilter = belowAggregate.child(0);
                while (belowTheFilter instanceof LogicalProject) {
                    belowTheFilter = belowTheFilter.child(0);
                }
                if (belowTheFilter instanceof LogicalAggregate
                        && !selectsTheRowsOfTheDomain((LogicalFilter<Plan>) belowAggregate, apply)) {
                    // the filter decides which rows of the aggregate below it survive (a HAVING
                    // clause of that aggregate), the chain continues below it
                    filtersAboveTheAggregation.add((LogicalFilter<Plan>) belowAggregate);
                    belowAggregate = belowAggregate.child(0);
                    continue;
                }
                // The filter restricts the rows of the subquery (the predicate which defines the
                // domain of an outer row is one of its conjuncts), and the filters below it restrict
                // those rows as well: the whole chain is the filter of the WHERE clause of the
                // subquery. The rewrite replaces that filter with the aggregation which reads its
                // child, so the predicates of the chain are collected into one filter over the child
                // of the deepest one, which is kept as it is. For example the subquery of
                //
                //     select k from o where k in (
                //         select count(*) from (select i.k from i where i.k = o.k) x where x.k > o.k)
                //
                // keeps the predicate i.k = o.k in the filter below the projection of the derived
                // table and the predicate x.k > o.k in the filter above it, and the rewrite of the
                // subquery has to evaluate both of them on the rows of the domain. Only the filters
                // which the projections between them pass the columns of the nodes below them
                // through are collected: a projection which computes a column of its own cannot be
                // dropped, so the filters below such a projection stay where they are (see the
                // check below).
                List<Expression> domainConjuncts = Lists.newArrayList();
                Plan deepestFilter = belowAggregate;
                while (true) {
                    domainConjuncts.addAll(((LogicalFilter<Plan>) deepestFilter).getConjuncts());
                    Plan belowTheProjections = deepestFilter.child(0);
                    while (belowTheProjections instanceof LogicalProject) {
                        belowTheProjections = belowTheProjections.child(0);
                    }
                    // the filters below the one which holds the predicate of the outer row are the
                    // filters of the same WHERE clause, and the projections between the two filters
                    // are dropped when they are merged (the merged filter reads the child of the
                    // deepest one, which is kept as it is): a projection which computes a column
                    // cannot be dropped, so the filters below such a projection stay where they are,
                    // below the filter which reads the columns it produces
                    if (!(belowTheProjections instanceof LogicalFilter)
                            || !carriesTheColumnsBelowItThrough(deepestFilter.child(0), belowTheProjections)) {
                        break;
                    }
                    deepestFilter = belowTheProjections;
                }
                domainFilter = deepestFilter == belowAggregate
                        ? (LogicalFilter<Plan>) belowAggregate
                        : new LogicalFilter<>(Sets.newLinkedHashSet(domainConjuncts), deepestFilter.child(0));
                break;
            }
            return Optional.empty();
        }
        if (domainFilter.child(0) == null) {
            return Optional.empty();
        }
        return Optional.of(new TheAggregation(chain, filtersAboveTheAggregation, havingFilter, domainFilter));
    }

    /** whether the projections between these two nodes only carry the columns below them through */
    private static boolean carriesTheColumnsBelowItThrough(Plan upper, Plan lower) {
        for (Plan between = upper; between != lower; between = between.child(0)) {
            for (NamedExpression project : ((LogicalProject<?>) between).getProjects()) {
                if (!(project instanceof Slot)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * The rewrite of one correlated apply whose subquery aggregates: locate the aggregation (see
     * locateAggregate), split the conjuncts of the filter below it into the correlated ones (they
     * become the conditions of the apply) and the others (they stay in the filter above the inner
     * side), classify the predicates (see CorrelatedAggregatePredicates) and rebuild the subquery
     * with the strategy which needCorrelatedAggregationOnOuter selects: the aggregation of the
     * domain on the outer side (pullUpCorrelatedPredicateByAggregatingOuter), or the aggregation
     * of the domain on the inner side, with the inner side of the correlated predicate added to
     * the group by and to the output of every aggregate of the chain (see
     * rebuildTheAggregationChain).
     *
     * For example the subquery of
     *
     *     select t1.c1 from t1 where exists (select count(*) from t2
     *         where t2.c1 = t1.c1 having count(*) <= t1.c1 - 7)
     *
     * reaches this method with the predicate of its HAVING clause already pulled into the apply
     * (the earlier rules pull the HAVING predicates which reference the outer query, see
     * CorrelatedAggregatePredicates) and with the filter t2.c1 = t1.c1 below the aggregation: the
     * aggregation is built on the outer side, because the HAVING clause references the outer row
     * and decides on the row which the aggregation returns for an empty correlated domain as well.
     */
    private static Plan pullUpCorrelatedFilter(LogicalApply<?, ?> apply) {
        // the correlated predicates of the HAVING clause (the filter below the apply) may already have
        // been pulled into the apply, and projections which only expose the output of the aggregates
        // may sit between the apply and the aggregates. Walk down to the aggregation of the subquery
        // and remember the filters which still hold predicates of its HAVING clause.
        Optional<TheAggregation> located = locateAggregate(apply);
        if (!located.isPresent()) {
            return apply;
        }
        TheAggregation aggregation = located.get();
        LogicalFilter<Plan> filter = aggregation.domainFilter();
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
                CorrelatedAggregatePredicates.of(apply, correlatedPredicate,
                        aggregation.filtersAboveTheAggregation());
        // A global aggregate above an aggregate which can return no row for a correlation key
        // returns a row for the empty input of that key, and neither rewrite can reproduce it (see
        // observesTheEmptyInputOfAGlobalAggregate): report those subqueries instead of dropping the
        // row and evaluating the subquery to false.
        if (observesTheEmptyInputOfAGlobalAggregate(apply, aggregation, predicates)) {
            throw new AnalysisException("Unsupported correlated subquery with grouping and/or aggregation "
                    + apply.right());
        }
        if (needCorrelatedAggregationOnOuter(apply, aggregation, correlatedPredicate, predicates)) {
            Plan aggregatedOuter = pullUpCorrelatedPredicateByAggregatingOuter(
                    apply, aggregation, unCorrelatedPredicate, predicates);
            if (aggregatedOuter != null) {
                return aggregatedOuter;
            }
            // The original rewrite is known to be not equivalent for this subquery and the rewrite
            // above cannot be applied safely: report the subquery as unsupported instead of building
            // a plan whose result is wrong.
            throw new AnalysisException("Unsupported correlated subquery with grouping and/or aggregation "
                    + apply.right());
        }

        // pull up correlated filter into apply node: the inner side of every correlated predicate
        // becomes a group by column and an output column of the aggregation below the filter, so that
        // the aggregation of one outer row is the aggregation of the rows of its own key, and every
        // aggregate above that aggregation groups the rows of its child by the same keys (a scalar
        // subquery keeps the rows of its aggregation through an aggregation which SubqueryToApply adds
        // above it, and those rows may not be mixed between two correlation keys either)
        List<Expression> newGroupby = Utils.getUnCorrelatedExprs(correlatedPredicate, apply.getCorrelationSlot());
        Map<Expression, Slot> unCorrelatedExprToSlot = Maps.newHashMap();
        List<NamedExpression> newGroupbyOutputs = Lists.newArrayListWithCapacity(newGroupby.size());
        for (Expression expression : newGroupby) {
            if (expression instanceof Slot) {
                newGroupbyOutputs.add((NamedExpression) expression);
            } else {
                Alias alias = new Alias(expression);
                unCorrelatedExprToSlot.put(expression, alias.toSlot());
                newGroupbyOutputs.add(alias);
            }
        }
        // the keys which the aggregates above the deepest one group by: the slots the keys have in
        // the output of the aggregation below them
        List<NamedExpression> keySlots = newGroupbyOutputs.stream()
                .map(NamedExpression::toSlot).collect(ImmutableList.toImmutableList());
        correlatedPredicate = ExpressionUtils.replace(correlatedPredicate, unCorrelatedExprToSlot);
        Map<LogicalAggregate<?>, Plan> newAggregations = new IdentityHashMap<>();
        for (LogicalAggregate<?> aggregate : aggregation.aggregationChain()) {
            boolean isTheAggregationOfTheDomain = aggregate == aggregation.domainAggregation();
            List<Expression> groupBy = Lists.newArrayList(
                    isTheAggregationOfTheDomain ? newGroupby : keySlots);
            groupBy.addAll(aggregate.getGroupByExpressions());
            List<NamedExpression> outputs = Lists.newArrayList(aggregate.getOutputExpressions());
            outputs.addAll(isTheAggregationOfTheDomain ? newGroupbyOutputs : keySlots);
            Plan child = isTheAggregationOfTheDomain
                    // the projections below it only carry the columns which the aggregation needs, so
                    // the new aggregation reads the rows of the filter directly
                    ? PlanUtils.filterOrSelf(ImmutableSet.copyOf(unCorrelatedPredicate),
                            aggregation.domainFilter().child())
                    : aggregate.child(0);
            newAggregations.put(aggregate, new LogicalAggregate<>(groupBy, outputs, child));
        }
        // the predicates which were already pulled into the apply are the predicates of the HAVING
        // clause of the subquery: they were evaluated on the rows of the old aggregate and have to
        // stay in the filter of the new apply, otherwise the subquery loses them
        List<Expression> newCorrelationFilter = Lists.newArrayList();
        apply.getCorrelationFilter().map(ExpressionUtils::extractConjunction)
                .ifPresent(newCorrelationFilter::addAll);
        newCorrelationFilter.addAll(correlatedPredicate);
        // the join which unnests the apply reads the inner side of the correlation predicates from
        // the output of the right side, so the projections which wrap the new aggregate have to
        // expose the keys it added: an IN subquery keeps the projections of its select list above
        // the aggregate (for example the outputs [c1] and [c1, c2] which wrap an aggregate
        // computing count(*) as c1, random() as c2), and a projection which hides one of the keys
        // makes the apply unresolvable
        Set<Slot> keysToExpose = keySlots.stream()
                .map(NamedExpression::toSlot).collect(ImmutableSet.toImmutableSet());
        // The outputs of the top aggregate are exposed by the projections above that aggregate
        // alone, because the projections below it cannot produce them: the aggregate which defines
        // them sits above those projections. The predicates which were pulled into the apply read
        // the outputs of the top aggregate as well (for example the max(c) <= t1.c1 of the HAVING
        // clause), and the projection below the top aggregate has to carry the keys alone. For
        // example the subquery of
        //
        //     select t1.c1 from t1 where t1.c1 in (select max(c) from (select count(*) as c from t2
        //         where t2.c1 = t1.c1 group by t2.c2) x having max(c) <= t1.c1)
        //
        // reaches the rewrite with the plan
        //
        //     Apply(correlationFilter=[(max(c) <= t1.c1)])
        //       |-- t1
        //       +-- Project([max(c)])                                 [the select list]
        //             +-- Aggregate(group by [], output [max(c) as max(c)])
        //                   +-- Project([c])                         [the projection below the
        //                         +-- Aggregate(group by [t2.c2],     aggregate which defines
        //                               output [t2.c2, count(*) as c]) max(c)]
        //                               +-- Filter(t2.c1 = t1.c1)
        //                                     +-- t2
        //
        // and appending max(c) to the projection of the count (the projection below the aggregate
        // which defines it) would make that projection read a slot which its child cannot produce,
        // so the plan would be rejected by the slot check of the rewrite.
        Set<Slot> outputsOfTheTopAggregation = newCorrelationFilter.stream()
                .flatMap(conjunct -> conjunct.getInputSlots().stream())
                .filter(slot -> newAggregations.get(aggregation.topAggregation()).getOutput().contains(slot))
                .filter(slot -> !keysToExpose.contains(slot))
                .collect(ImmutableSet.toImmutableSet());
        // the predicates of the apply are evaluated on the nodes above the aggregation of the
        // subquery, which produce the outputs of that aggregation themselves, so no output of it has
        // to be appended to the projections below them
        return new LogicalApply<>(apply.getCorrelationSlot(), apply.getSubqueryType(), apply.isNot(),
                apply.getCompareExpr(), apply.getTypeCoercionExpr(),
                ExpressionUtils.optionalAnd(newCorrelationFilter), apply.getMarkJoinSlotReference(),
                apply.isNeedAddSubOutputToProjects(), apply.isMarkJoinSlotNotNull(), apply.left(),
                rebuildTheAggregationChain(apply.right(), aggregation, newAggregations, keysToExpose,
                        outputsOfTheTopAggregation, null, ImmutableSet.of(), false));
    }

    /**
     * The aggregation of the subquery with the keys of the correlation added to its group by and to
     * its output: the rows of one correlation key are the rows of the subquery for the outer rows
     * which own that key, so an aggregation above the aggregation of the domain may not mix them.
     * For example the aggregate of the sum of the example of TheAggregation is rewritten into
     *
     *     Aggregate(group by [key.c1], output [sum(c) as sum(x.c), key.c1])
     *
     * around the rewritten aggregation of the domain, whose rows carry the key as well (see
     * rebuildTheAggregationChain).
     */
    private static LogicalAggregate<?> withTheKeysInTheGroupBy(LogicalAggregate<?> aggregate,
            List<? extends Expression> keys, Slot matchMarkerOfTheEmptyDomain,
            boolean exposesTheMatchMarker) {
        List<Expression> groupBy = Lists.newArrayList(keys);
        groupBy.addAll(aggregate.getGroupByExpressions());
        if (matchMarkerOfTheEmptyDomain != null && exposesTheMatchMarker) {
            // The marker of the row which is kept for an empty domain is read by the guard of the
            // aggregates above the aggregation of the domain and by the projections and the filters
            // between the aggregates (see rebuildTheAggregationChain), so those aggregates expose it.
            // The marker is null for the row which is kept for an empty domain, so the grouping of the
            // rows of a correlation key does not change. The top aggregate does not expose it: the rows
            // which it produces are the rows of the subquery, and the marker belongs to the rows below
            // it (the aggregates above the aggregation of the domain read it from their own input).
            groupBy.add(matchMarkerOfTheEmptyDomain);
        }
        List<NamedExpression> outputs = Lists.newArrayList();
        if (matchMarkerOfTheEmptyDomain == null) {
            outputs.addAll(aggregate.getOutputExpressions());
        } else {
            // The row which the rewrite keeps for a correlation key whose rows below the aggregation of
            // the domain are missing reaches this aggregate as well (its marker is null for that row),
            // and the aggregation of the original subquery computes the aggregate out of the empty
            // input: the guard of the arguments makes the aggregates ignore that row, so that they
            // return the value of an empty input for it (see guardAggregateArguments), which is the
            // value the original subquery computes above this aggregate as well.
            Set<AggregateFunction> aggregates = Sets.newLinkedHashSet();
            for (NamedExpression output : aggregate.getOutputExpressions()) {
                aggregates.addAll(output.collect(AggregateFunction.class::isInstance));
            }
            Map<Expression, Expression> compensated = guardAggregateArguments(aggregates,
                    matchMarkerOfTheEmptyDomain);
            if (compensated == null) {
                // an aggregate of the aggregation cannot be guarded, so the row which is kept for an
                // empty input cannot be told apart from a row of the rows below the aggregation
                return null;
            }
            for (NamedExpression output : aggregate.getOutputExpressions()) {
                outputs.add((NamedExpression) ExpressionUtils.replace(output, compensated));
            }
        }
        keys.forEach(key -> outputs.add((NamedExpression) key));
        return new LogicalAggregate<>(groupBy, outputs, aggregate.child(0));
    }

    /**
     * Whether the rewrite of the outer side has to keep one row for the correlation keys whose rows
     * below the aggregation of the domain are missing, although that aggregation returns no row of its
     * own for them: every aggregate above it is global, so the aggregation of the original subquery
     * produces one row for the empty input of such a key, and the aggregates which the rewrite builds
     * above that aggregation can be guarded with the marker of the row which is kept for it (see
     * guardAggregateArguments and withTheKeysInTheGroupBy). The value which that row exposes is then
     * the value which the original subquery exposes for the key. For example the subquery of
     *
     *     select o.k from o where o.k in (
     *         select coalesce(max(c), 0) from
     *             (select count(*) as c from i where i.k = o.k group by i.g) x)
     *
     * returns one row whose value is 0 for the outer rows whose correlated domain is empty (the max of
     * the empty derived table is null and the coalesce turns that null into the 0), so the outer row
     * of the value 0 matches the subquery: the rewrite keeps a row for such a key, the max above it
     * ignores that row and returns the null of its empty input, and the coalesce of the plan of the
     * subquery turns that null into the 0 as well.
     *
     * An EXISTS subquery reads whether the row of such a key exists instead of the value it exposes,
     * so the row has to be kept when the HAVING clause of the subquery keeps the row of the empty
     * input (see the EXISTS branch below).
     */
    private static boolean keepsTheRowOfAnEmptyDomain(LogicalApply<?, ?> apply, TheAggregation aggregation,
            CorrelatedAggregatePredicates predicates) {
        List<LogicalAggregate<?>> chain = aggregation.aggregationChain();
        List<LogicalAggregate<?>> aboveTheDomain = chain.subList(0, chain.size() - 1);
        if (aboveTheDomain.isEmpty()) {
            // the aggregation of the domain is the only aggregation of the subquery: the rewrite of the
            // outer side keeps a row of its own for an empty domain when that aggregation is global,
            // and no aggregate above it observes such a row
            return false;
        }
        if (aboveTheDomain.stream().anyMatch(aggregate -> !aggregate.getGroupByExpressions().isEmpty())) {
            // an aggregate above the aggregation of the domain groups the rows which it reads, so the
            // row which is kept for an empty domain builds a group of its own in that aggregate, while
            // the aggregation of the original subquery produces no row at all for such an empty input
            return false;
        }
        if (!chain.stream()
                .flatMap(aggregate -> aggregate.getOutputExpressions().stream())
                .flatMap(output -> output.collect(AggregateFunction.class::isInstance).stream())
                .allMatch(function -> function instanceof NullIgnoringAggregateFunction)) {
            // only the aggregates which ignore null arguments can be guarded, so that the row which is
            // kept for an empty domain does not contribute to them (see guardAggregateArguments)
            return false;
        }
        if (apply.isExist()) {
            // The row which the aggregation of an empty correlated domain produces exists for the
            // subquery when the HAVING clause of the aggregation above the one of the domain holds for
            // the values of that empty input (see havingMayHoldWithEmptyInput): the EXISTS of the
            // subquery of
            //
            //     select t1.c1 from t1 where exists (select max(c) from (select count(*) as c from t2
            //         where t2.c1 = t1.c1 group by t2.c2) x having max(c) is null)
            //
            // is true for the outer rows whose correlated domain is empty (the max of the empty
            // derived table is null and the HAVING clause keeps that row). The rewrite keeps the row of
            // such a key and lets the aggregates above the aggregation of the domain return the values
            // of an empty input for it, so that the nodes above the aggregation decide on the row the
            // way the original subquery does (see rebuildTheAggregationChain and
            // guardAggregateArguments). A HAVING clause which rejects the row of the empty input
            // (having max(c) > 0, for example) drops it, and the nodes above the aggregation reject
            // the row which the rewrite keeps for such a key as well.
            List<Expression> havingConjuncts = predicates.havingPredicates();
            return aboveTheDomain.stream()
                    .filter(aggregate -> aggregate.getGroupByExpressions().isEmpty())
                    .anyMatch(aggregate -> havingMayHoldWithEmptyInput(aggregate,
                            Sets.newLinkedHashSet(havingConjuncts)));
        }
        return true;
    }

    /**
     * Whether the aggregation of the subquery holds a global aggregate above an aggregate which can
     * return no row for a correlation key, and the subquery observes the row which that global
     * aggregate returns for the empty input.
     *
     * The rewrite adds the correlation keys to the group by of every aggregate of the chain (see
     * pullUpCorrelatedFilter and withTheKeysInTheGroupBy), so a global aggregate above the
     * aggregation of the domain produces no row at all for a key whose rows below it are missing,
     * while the aggregation of the original subquery returns one row for that empty input. The
     * subquery of
     *
     *     select t1.c1 from t1 where exists (select max(c) from (select count(*) as c from t2
     *         where t2.c1 = t1.c1 group by t2.c2) x having max(c) is null)
     *
     * is true for the outer rows whose correlated domain is empty, because the max of the empty
     * derived table is null and the HAVING clause keeps that row, while a rewrite which dropped the
     * key would produce no row for it and the semi join would drop the outer row. The aggregation of
     * the inner side is not equivalent for such subqueries, and the aggregation of the outer side is
     * only equivalent when it keeps a row for the empty domain and lets the aggregates above the
     * aggregation of the domain return the values of an empty input for it (see
     * keepsTheRowOfAnEmptyDomain); the caller reports the subqueries which neither of them can
     * rewrite.
     */
    private static boolean observesTheEmptyInputOfAGlobalAggregate(LogicalApply<?, ?> apply,
            TheAggregation aggregation, CorrelatedAggregatePredicates predicates) {
        if (keepsTheRowOfAnEmptyDomain(apply, aggregation, predicates)) {
            // the rewrite of the outer side keeps the row which such a key is missing (see
            // keepsTheRowOfAnEmptyDomain), so the subquery is not reported
            return false;
        }
        return theEmptyInputOfAGlobalAggregateIsObservable(apply, aggregation, predicates);
    }

    /**
     * The detection of observesTheEmptyInputOfAGlobalAggregate on its own: the subqueries
     * which this detection reports are the subqueries whose rewrite would drop the row which the
     * aggregation of the original subquery returns for a correlation key whose rows below the
     * aggregation of the domain are missing. The rewrite of the outer side keeps that row and lets the
     * aggregates above the aggregation of the domain return the values of an empty input for it when
     * every one of them is a global aggregate which ignores null arguments (see
     * keepsTheRowOfAnEmptyDomain), and those subqueries are rewritten instead of reported.
     */
    private static boolean theEmptyInputOfAGlobalAggregateIsObservable(LogicalApply<?, ?> apply,
            TheAggregation aggregation, CorrelatedAggregatePredicates predicates) {
        List<LogicalAggregate<?>> chain = aggregation.aggregationChain();
        if (chain.get(chain.size() - 1).getGroupByExpressions().isEmpty()
                && theFiltersBetweenTheAggregationOfTheDomainAndTheOneAboveIt(aggregation).isEmpty()) {
            // the aggregation of the domain returns a row for every correlation key, so no
            // aggregate above it can observe an empty input (a filter between those aggregations is
            // the HAVING clause of the aggregation of the domain: it decides on the row of the empty
            // input and may reject it, which the aggregates above it observe)
            return false;
        }
        // the aggregates above the deepest one: the deepest one reads the rows of the domain of a
        // correlation key, and the predicates of that domain may leave them empty
        List<LogicalAggregate<?>> aboveTheDomain = chain.subList(0, chain.size() - 1);
        if (apply.isExist()) {
            // the row which the global aggregate returns for the empty input decides whether the
            // EXISTS reports the outer row, unless the nodes above the aggregation reject that row
            // (the predicates of the HAVING clause which reference the outer query were pulled into
            // the apply, and they decide on the row of the empty input as well)
            List<Expression> havingConjuncts = predicates.havingPredicates();
            return aboveTheDomain.stream()
                    .filter(aggregate -> aggregate.getGroupByExpressions().isEmpty())
                    .anyMatch(aggregate -> havingMayHoldWithEmptyInput(aggregate,
                            Sets.newLinkedHashSet(havingConjuncts)));
        }
        if (apply.isScalar()) {
            // A scalar subquery exposes the output of the aggregation of its domain: the join of
            // the rewrite reports a null for the keys whose rows below the aggregation are missing,
            // and SubqueryToApply repairs that null with the nvl of the value which the top
            // aggregate returns for an empty input. A global aggregate below the top aggregate
            // changes the value which the top one computes out of the row of the empty input.
            boolean hasAGlobalAggregateBelowTheTop = aboveTheDomain.stream().skip(1)
                    .anyMatch(aggregate -> aggregate.getGroupByExpressions().isEmpty());
            return hasAGlobalAggregateBelowTheTop
                    && (returnsAValueForAnEmptyInput(chain.get(0)) || aboveTheDomain.stream().skip(1)
                            .anyMatch(UnCorrelatedApplyAggregateFilter::returnsAValueForAnEmptyInput));
        }
        // An IN subquery compares the outer value with the value of the aggregation of its domain:
        // the value which a global aggregate returns for an empty input can match the outer value,
        // while the rewrite has no row to compare it with (a null value of the aggregation does not
        // match either, so an aggregation of nullable aggregates alone is left alone).
        if (aboveTheDomain.stream()
                .anyMatch(UnCorrelatedApplyAggregateFilter::returnsAValueForAnEmptyInput)) {
            return true;
        }
        // The nodes above the aggregation of the domain may expose a value of their own for the empty
        // input as well, even though the aggregates are nullable: the projection of the subquery of
        //
        //     select o.k from o where o.k in (
        //         select coalesce(max(c), 0) from
        //             (select count(*) as c from i where i.k = o.k group by i.g) x)
        //
        // turns the null which the max of the empty derived table returns into the 0 which an outer
        // row with the value 0 compares with, while the rewrite has no row to compare it with and the
        // semi join drops that row (see exposesAValueForAnEmptyInput).
        if (exposesAValueForAnEmptyInput(apply, aggregation, aboveTheDomain)) {
            return true;
        }
        // The missing row of a key is observable when the result of the IN is not read as the decision
        // on the outer row alone: the null which the subquery of the original query compares with
        // (the row of the aggregation of an empty derived table, for example) makes the IN unknown,
        // while the rewrite compares with nothing, which is false for an IN and true for a NOT IN.
        // The result of an IN which is used as a value is its mark, so its null and its false are
        // observable as well (the plan of such an IN is a mark join). For example the subquery of
        //
        //     select o.k from o where o.k not in (
        //         select max(c) from (select count(*) as c from i where i.k = o.k group by i.g) x)
        //
        // returns one row for the outer rows whose correlated domain is empty (the max of the empty
        // derived table is null), so their NOT IN is unknown and those rows are not returned, while
        // the rewrite produces no row for those keys and their NOT IN is true. A global aggregate
        // above the aggregation of the domain is what makes such a row disappear: the key is added to
        // the group by of that aggregate (see withTheKeysInTheGroupBy), so a key without rows below it
        // has no group at all.
        return (apply.isNot() || apply.getMarkJoinSlotReference().isPresent())
                && aboveTheDomain.stream()
                        .anyMatch(aggregate -> aggregate.getGroupByExpressions().isEmpty());
    }

    /**
     * Whether one of the aggregates of the aggregation declares a value of its own for an empty
     * input (the count 0 or the empty array of an array_agg, for example). An aggregate which
     * declares no such value is left to the rewrite of the other cases, which reads the value of an
     * empty input the way the rest of the engine does (see keepsTheValueOfAnEmptyDomain): the value
     * of a UDAF is written in the UDAF itself, so its declaration does not tell it.
     */
    private static boolean returnsAValueForAnEmptyInput(LogicalAggregate<?> aggregate) {
        for (NamedExpression output : aggregate.getOutputExpressions()) {
            if (output.collect(AggregateFunction.class::isInstance).stream()
                    .anyMatch(function -> function instanceof NotNullableAggregateFunction)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The filters which sit between the aggregation of the domain and the aggregation above it: they
     * decide on the rows which the aggregation of the domain produces (they are the HAVING clauses of
     * the aggregation below them), so they are not evaluated for a correlation key whose rows below
     * that aggregation are missing (see keepsTheRowOfAnEmptyDomain).
     */
    private static Set<LogicalFilter> theFiltersBetweenTheAggregationOfTheDomainAndTheOneAboveIt(
            TheAggregation aggregation) {
        if (aggregation.topAggregation() == aggregation.domainAggregation()) {
            // the aggregation of the domain is the only aggregation of the subquery, so there is no
            // aggregation above it and no filter between such aggregations either (the filter of the
            // domain itself reads the rows of the domain and is not a filter between the aggregates)
            return ImmutableSet.of();
        }
        List<LogicalAggregate<?>> chain = aggregation.aggregationChain();
        Plan below = chain.get(chain.size() - 2).child(0);
        Set<LogicalFilter> filters = Sets.newLinkedHashSet();
        while (below != aggregation.domainAggregation()) {
            if (below instanceof LogicalFilter) {
                filters.add((LogicalFilter) below);
            }
            below = below.child(0);
        }
        return filters;
    }

    /**
     * Whether the nodes above the top aggregate expose a value of their own for the empty input of a
     * correlation key: the input of such a key is empty when the aggregation of the domain has no row
     * for it and a global aggregate above that aggregation returns no row for that key in the
     * rewrite, while the aggregation of the original subquery computes the nodes above it out of the
     * row of the empty input. The projection of the subquery of
     *
     *     select o.k from o where o.k in (
     *         select coalesce(max(c), 0) from
     *             (select count(*) as c from i where i.k = o.k group by i.g) x)
     *
     * turns the null which the max of the empty derived table returns into the 0 which the outer row
     * with the value 0 compares with, while the rewrite has no row to compare it with: the semi join
     * would drop that outer row. The value which a node computes for the empty input is read by
     * replacing the outputs of the top aggregate with the values they return for it and folding the
     * expression.
     */
    private static boolean exposesAValueForAnEmptyInput(LogicalApply<?, ?> apply, TheAggregation aggregation,
            List<LogicalAggregate<?>> aboveTheDomain) {
        if (aboveTheDomain.stream().noneMatch(aggregate -> aggregate.getGroupByExpressions().isEmpty())) {
            // every aggregate above the aggregation of the domain groups the rows it reads, so a key
            // without rows below that aggregation has no group in those aggregates either
            return false;
        }
        Map<Expression, Expression> emptyValues = Maps.newHashMap();
        Set<Slot> outputsOfTheTopAggregation = Sets.newHashSet();
        for (NamedExpression output : aggregation.topAggregation().getOutputExpressions()) {
            Expression expression = output instanceof Alias ? ((Alias) output).child() : output;
            if (!(expression instanceof AggregateFunction)) {
                continue;
            }
            Expression emptyValue = emptyValueForEmptyInput((AggregateFunction) expression);
            if (emptyValue == null) {
                // the aggregate declares no value for an empty input: it returns the null of the empty
                // input (the max of no row, for example)
                emptyValue = new NullLiteral(output.getDataType());
            }
            emptyValues.put(output.toSlot(), emptyValue);
            outputsOfTheTopAggregation.add(output.toSlot());
        }
        if (emptyValues.isEmpty()) {
            return false;
        }
        Plan below = apply.right();
        while (below != aggregation.topAggregation()) {
            if (below instanceof LogicalProject) {
                for (NamedExpression project : ((LogicalProject<?>) below).getProjects()) {
                    Expression expression = project instanceof Alias ? ((Alias) project).child() : project;
                    if (Sets.intersection(expression.getInputSlots(), outputsOfTheTopAggregation).isEmpty()) {
                        // the projection does not read the aggregation of the domain
                        continue;
                    }
                    Expression folded = FoldConstantRuleOnFE.evaluateWithoutContext(
                            ExpressionUtils.replace(expression, emptyValues));
                    if (folded instanceof Literal && !(folded instanceof NullLiteral)) {
                        // the nodes above the aggregation expose this value for the empty input
                        return true;
                    }
                }
            }
            below = below.child(0);
        }
        return false;
    }

    /**
     * Replace every aggregate of the chain with its rewritten version and expose the keys through
     * the projections between them, so that every aggregate above the deepest one can group by the
     * keys. The walk stops at the deepest aggregate: its rewritten version already reads the rows of
     * the filter below it (see pullUpCorrelatedFilter).
     *
     * The projections below the top aggregate only expose the keys, while the projections above it
     * expose the outputs of the top aggregate as well: the aggregate which defines those outputs
     * sits above the projections below it, so they cannot produce them. The outputs to expose are
     * therefore dropped as soon as the walk reaches the top aggregate.
     *
     * For example the plan of the example of TheAggregation is rewritten into
     *
     *     Apply(correlationFilter=[t2.c1 = t1.c1])
     *       |-- t1
     *       +-- Filter(sum(x.c) > 2)
     *             +-- Aggregate(group by [key.c1], output [sum(c) as sum(x.c), key.c1])
     *                   +-- Project([c, key.c1])                    [the key exposed between the
     *                         +-- Filter(c > 1)                          aggregates]
     *                               +-- Aggregate(group by [key.c1, t2.c2],
     *                                     output [t2.c2, count(*) as c, key.c1])
     *                                     +-- t2
     */
    private static Plan rebuildTheAggregationChain(Plan plan, TheAggregation aggregation,
            Map<LogicalAggregate<?>, Plan> newAggregations, Set<Slot> keysToExpose,
            Set<Slot> outputsOfTheTopAggregation, Slot matchMarkerOfTheEmptyDomain,
            Set<LogicalFilter> filtersWhichKeepTheRowOfAnEmptyDomain, boolean belowTheTopAggregate) {
        Plan replacement = newAggregations.get(plan);
        if (plan == aggregation.domainAggregation()) {
            return replacement;
        }
        // the nodes below the top aggregate cannot produce its outputs, so they only carry the keys
        Plan child = rebuildTheAggregationChain(plan.child(0), aggregation, newAggregations, keysToExpose,
                plan == aggregation.topAggregation() ? ImmutableSet.of() : outputsOfTheTopAggregation,
                matchMarkerOfTheEmptyDomain, filtersWhichKeepTheRowOfAnEmptyDomain,
                belowTheTopAggregate || plan == aggregation.topAggregation());
        if (replacement != null) {
            return replacement.withChildren(child);
        }
        if (filtersWhichKeepTheRowOfAnEmptyDomain.contains(plan)) {
            // The row which the rewrite keeps for an empty domain is not a row of the rows below this
            // filter (its marker is null), and the aggregation of the original subquery does not
            // evaluate the filter for the empty input of such a key either: the row passes the filter,
            // so that the aggregates above it return the values of an empty input for the key (see
            // keepsTheRowOfAnEmptyDomain). Without the relaxation the filter would remove the row which
            // the rewrite keeps for the key, and the aggregates above it (which the rewrite grouped by
            // the correlation key) would produce no row at all for the key.
            List<Expression> conjuncts = Lists.newArrayList();
            for (Expression conjunct : ((LogicalFilter<Plan>) plan).getConjuncts()) {
                conjuncts.add(ExpressionUtils.or(conjunct, new IsNull(matchMarkerOfTheEmptyDomain)));
            }
            return new LogicalFilter<>(Sets.newLinkedHashSet(conjuncts), child);
        }
        if (plan instanceof LogicalProject) {
            // the projections between the aggregates carry the columns which the aggregates above
            // them need, the keys of the correlation included
            LogicalProject<?> project = (LogicalProject<?>) plan;
            Set<Slot> exposed = project.getProjects().stream()
                    .map(NamedExpression::toSlot).collect(ImmutableSet.toImmutableSet());
            List<NamedExpression> projects = Lists.newArrayList(project.getProjects());
            boolean added = false;
            for (Slot key : keysToExpose) {
                if (!exposed.contains(key)) {
                    projects.add(key);
                    added = true;
                }
            }
            for (Slot output : outputsOfTheTopAggregation) {
                if (!exposed.contains(output)) {
                    projects.add(output);
                    added = true;
                }
            }
            if (belowTheTopAggregate && matchMarkerOfTheEmptyDomain != null
                    && !exposed.contains(matchMarkerOfTheEmptyDomain)) {
                // the aggregates above the aggregation of the domain read the marker of the row which is
                // kept for an empty domain from their own input, and the filters between those
                // aggregations read it as well, so the projections below the top aggregate carry it
                // (the projections above that aggregate are not read by any node which needs it)
                projects.add(matchMarkerOfTheEmptyDomain);
                added = true;
            }
            if (added) {
                return new LogicalProject<>(projects, project.isDistinct(), project.getAsteriskOutputs(), child);
            }
        }
        return plan.withChildren(child);
    }

    /**
     * Whether an EXISTS subquery keeps a node above its aggregation which decides on the rows the
     * subquery returns: the projection of its select list, or a filter which sits above the
     * projection and above the HAVING clause of the subquery (see the two methods below). The
     * rewrite keeps those nodes (see rebuildTheAggregationChain), and the aggregation of
     * one outer row is the aggregation of the rows which they produce, so the aggregation is built
     * on the outer side and the nodes are evaluated on the aggregation of one correlation key.
     *
     * Only an EXISTS subquery is decided by the nodes above its aggregation this way: the value
     * which an IN or scalar subquery exposes is the output of the aggregation itself, which the
     * rewrite reads through the nodes above it (see rebuildTheAggregationChain).
     *
     * For example the subquery of
     *
     *     select t1.c1 from t1 where exists (select count(*) from t2 where t2.c1 = t1.c1
     *         having count(*) <= t1.c1 - 7 and count(*) >= 0)
     *
     * keeps the projection of its select list above the aggregation (see
     * hasProjectionAboveAggregate), and the subquery of
     *
     *     select t1.c1 from t1 where exists (select x.c from (select count(*) as c, random() as r
     *         from t2 where t2.c1 = t1.c1 having count(*) = 0) x where x.r < -1)
     *
     * keeps a filter above the projection and above the HAVING clause (see
     * hasFilterAboveHavingFilter); the nodes decide on the rows which the EXISTS has to report, so
     * the aggregation of one outer row is the aggregation of the rows which they produce and is
     * built on the outer side.
     */
    private static boolean keepsNodesAboveTheAggregation(LogicalApply<?, ?> apply, TheAggregation aggregation) {
        return apply.isExist() && (hasProjectionAboveAggregate(apply, aggregation.topAggregation())
                || (aggregation.havingFilter.isPresent()
                        && hasFilterAboveHavingFilter(apply, aggregation.havingFilter.get())));
    }

    /**
     * Whether a projection sits between the apply and the aggregation of the subquery: it exposes
     * the select list of the subquery, which may only carry a part of the output of the
     * aggregation, so the correlation key which the original rewrite adds to the group by of the
     * aggregation is not part of that output. For example the projection of the subquery of
     *
     *     select t1.c1 from t1 where exists (select count(*) from t2 where t2.c1 = t1.c1
     *         having count(*) <= t1.c1 - 7 and count(*) >= 0)
     *
     * sits between the apply and the aggregation when this rule matches: the right side of the
     * apply is the projection of the select list of the subquery, below it the filter of the
     * HAVING clause which stayed in the plan, below it the aggregation, the filter of the WHERE
     * clause and t2. A projection is not always kept there (a projection which only passes a
     * column of the aggregation through is merged into the nodes above it), so the strategy is
     * decided on the plan which this rule receives, not on the query text.
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
     * Whether a filter sits between the apply and the HAVING clause of the subquery (the filter
     * which holds the predicates of the HAVING clause which were not pulled into the apply), for
     * example the filter of
     *
     *     select t1.c1 from t1 where exists (select x.c from (select count(*) as c, random() as r
     *         from t2 where t2.c1 = t1.c1 having count(*) = 0) x where x.r < -1)
     *
     * Filter pushdown creates such a filter above the projection of the select list when a
     * predicate reads a column of that projection which cannot be pushed below it (the volatile
     * column r here), and the filter decides on the rows which the projection produces, so the
     * rewrite has to keep it.
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
     * The predicates which relate the correlated subquery to the outer query, classified by the
     * node on which they have to be evaluated.
     *
     * Domain predicates select the inner rows which belong to the correlated domain of one outer
     * row, for example where t2.c1 = t1.c1. They are the predicates of the WHERE clause of the
     * subquery (the filter below the aggregation) plus the predicates which were already pulled
     * into the apply and do not reference the aggregation.
     *
     * Aggregate predicates reference the output of the aggregation and decide which rows of the
     * aggregation the subquery returns for one outer row, for example having count(*) <= t1.c1 - 7.
     * They are the predicates of the HAVING clause of the subquery, and the earlier rules
     * (UnCorrelatedApplyFilter and UnCorrelatedApplyProjectFilter, they run before this one) split
     * them by the outer query: the ones which reference the outer query were pulled into the apply,
     * so they are not part of the plan of the subquery any more and this rewrite has to evaluate
     * them again above the aggregation it builds (aggregatePredicates); the ones which do not
     * reference the outer query stayed in the plan, in the filter between the apply and the
     * aggregation, and the rewrite keeps them unchanged (havingConjuncts). For example, the HAVING
     * clause of the subquery of
     *
     *     select t1.c1 from t1 where exists (select count(*) from t2 where t2.c1 = t1.c1
     *         having count(*) <= t1.c1 - 7 and count(*) >= 0)
     *
     * is split into count(*) <= t1.c1 - 7 (it references t1.c1, so it is the correlation filter
     * of the apply when this rule matches) and count(*) >= 0 (it does not reference the outer
     * query, so it is the filter which still sits between the apply and the aggregation and stays
     * there in the rewritten plan). The two collections are kept apart because of that difference,
     * and the methods which return both of them together (havingPredicates) are the ones which only
     * need to see the whole HAVING clause of the subquery, for example the volatility checks.
     */
    private static final class CorrelatedAggregatePredicates {
        /** the correlated conjuncts of the WHERE clause (the filter below the aggregation) */
        private final List<Expression> whereConjuncts = Lists.newArrayList();
        /** the conjuncts of the HAVING clause which were already pulled into the apply */
        private final List<Expression> aggregatePredicates = Lists.newArrayList();
        /** the conjuncts of the HAVING clause which are still in the plan of the subquery */
        private final Set<Expression> havingConjuncts = Sets.newLinkedHashSet();

        private static CorrelatedAggregatePredicates of(LogicalApply<?, ?> apply,
                List<Expression> whereConjuncts, List<LogicalFilter<Plan>> filtersAboveTheAggregation) {
            CorrelatedAggregatePredicates predicates = new CorrelatedAggregatePredicates();
            predicates.whereConjuncts.addAll(whereConjuncts);
            // Every predicate which was pulled into the apply was pulled from the filter which sits
            // above the aggregate (the HAVING clause of the subquery), so it decides which rows of
            // the aggregation the subquery returns and has to be evaluated above the aggregation
            // of the rewrite. Its provenance cannot be recovered from the slots it uses: a
            // predicate such as t1.c3 = 1 references no aggregation output and no inner column but
            // it still rejects the row of the aggregation.
            apply.getCorrelationFilter()
                    .map(ExpressionUtils::extractConjunction)
                    .orElse(ImmutableList.of())
                    .forEach(predicates.aggregatePredicates::add);
            filtersAboveTheAggregation.forEach(filter -> predicates.havingConjuncts.addAll(filter.getConjuncts()));
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
         * two outer rows are the same as soon as the slots their predicates use are equal, so
         * pullUpCorrelatedPredicateByAggregatingOuter groups the aggregation of the outer copy by
         * exactly these slots and pairs every outer row with the aggregation of its own key.
         *
         * A predicate may read slots of the outer query and slots of the subquery, and only the
         * slots which belong to the outer query (the ones of the correlation slots of the apply)
         * can take the place of the outer rows: the other slots a predicate reads are the columns
         * of the inner side (the new aggregation reads them below itself) or the output of the
         * aggregation itself (the new aggregation computes it), so intersecting the slots of the
         * predicate with the correlation slots keeps exactly the keys. For example the domain
         * predicate and the HAVING predicate of the subquery of
         *
         *     select t1.c1 from t1 where exists (select count(*) from t2 where t2.c1 = t1.c1
         *         having count(*) <= t1.c1 - 7)
         *
         * give the key t1.c1: it is the right operand of the domain predicate t2.c1 = t1.c1 and
         * it appears inside the arithmetic of the HAVING predicate count(*) <= t1.c1 - 7 (whose
         * count(*) is the output of the aggregation), and the aggregation of the rewrite is
         * grouped by it. A subquery may use several correlation slots and a slot may appear in any
         * position of the predicate: the domain predicates t2.c1 = t1.c1 and t2.c2 = t1.c2 give
         * the two keys t1.c1 and t1.c2 (the aggregation is grouped by both of them and the join of
         * the domain compares t2.c1 = key.c1 and t2.c2 = key.c2), while the predicate
         * t2.c1 = t1.c1 + 1 gives the key t1.c1 alone (the join of the domain compares
         * t2.c1 = key.c1 + 1 after the rewrite).
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
         * uses the correlation keys, the inner rows, the output of the aggregation of the domain and
         * the output of the aggregates above it (which the predicates evaluated above the chain may
         * read). The predicates which are still in the plan of the subquery keep their node, so only
         * the predicates which were pulled into the apply have to be evaluated above the aggregation
         * of the rewrite.
         *
         * The predicates which the earlier rules pulled out of a filter above the projection of the
         * select list may read a column of that projection, which is computed above the aggregation
         * and is not available to the aggregation of the rewrite: for example the predicate
         * x.c2 <= t1.c1 of the subquery of
         *
         *     select t1.c1 from t1 where exists (select x.c1 from (select count(*) as c1,
         *         count(*) + 1 as c2 from t2 where t2.c1 = t1.c1) x where x.c2 <= t1.c1)
         *
         * reads x.c2, which the projection [c1, (c1 + 1) as c2] computes above the aggregation,
         * so this rewrite cannot evaluate the predicate for the keys and reports the subquery with
         * the "Unsupported correlated subquery with grouping and/or aggregation" error. A predicate
         * which reads the output of the aggregation instead (for example x.c1 <= t1.c1 in the same
         * subquery) is evaluated without an error.
         */
        private boolean isResolvable(LogicalApply<?, ?> apply, TheAggregation aggregation,
                LogicalFilter<Plan> filter) {
            Set<ExprId> allowed = Sets.newHashSet();
            apply.getCorrelationSlot().forEach(slot -> allowed.add(slot.getExprId()));
            aggregation.domainAggregation().getOutput().forEach(slot -> allowed.add(slot.getExprId()));
            aggregation.topAggregation().getOutput().forEach(slot -> allowed.add(slot.getExprId()));
            filter.child().getOutput().forEach(slot -> allowed.add(slot.getExprId()));
            return domainPredicates().stream().allMatch(conjunct -> allowed.containsAll(conjunct.getInputSlotExprIds()))
                    && pulledPredicates().stream()
                            .allMatch(conjunct -> allowed.containsAll(conjunct.getInputSlotExprIds()));
        }
    }

    /**
     * Whether the aggregation of the correlated subquery has to be built on the outer side.
     *
     * The original rewrite puts the inner side of the correlated predicate into the group by of the
     * aggregate, so the HAVING clause of one group is treated as the HAVING clause of one outer
     * row. That is wrong in three cases: this rewrite then pairs every outer row with the
     * aggregation of its own correlation key (LEFT SEMI JOIN / LEFT ANTI JOIN of the outer plan
     * with the aggregation, the aggregation grouped by the key, and a join between the keys and
     * the inner side on the predicates of the domain). The three cases and their plans:
     *
     * A correlated predicate which is not an equality, eg. the subquery of
     *
     *     select t1.c1 from t1 where exists (select count(*) from t2
     *         where t2.c1 < t1.c1 group by t2.c2 having count(*) = 2)
     *
     * the inner rows of one outer row are the union of several groups, so group wide aggregates
     * such as count(*) are computed for a part of the domain of the outer row only. The domain is
     * not one group of the aggregation, so the keys are joined with the inner side and the keys of
     * the outer rows without any matching inner row have no row (the join of the domain is an
     * INNER JOIN, the domain of an empty key does not exist):
     *
     *     LEFT SEMI JOIN (t1.c1 <=> key.c1)
     *       |-- t1
     *       +-- Filter(count(*) = 2)
     *             +-- Aggregate(group by [key.c1, t2.c2], count(*))
     *                   +-- INNER JOIN (t2.c1 < key.c1)
     *                         |-- Aggregate(group by [t1.c1])            [the keys of t1]
     *                         |     +-- t1
     *                         +-- t2
     *
     * A global aggregate (no group by) whose HAVING clause holds for an empty input, eg. the
     * subquery of
     *
     *     select t1.c1 from t1 where exists (select count(*) from t2
     *         where t2.c1 = t1.c1 having count(*) = 0)
     *
     * a global aggregate returns one row for every outer row, including the outer rows without any
     * matching inner row, and that row disappears when the inner side of the correlated predicate
     * becomes the group by key. The keys are joined with the inner side by a LEFT OUTER JOIN,
     * which gives the aggregation one row for the keys without inner rows, and the marker of that
     * row is null, so the counts count the marker instead of the rows:
     *
     *     LEFT SEMI JOIN (t1.c1 <=> key.c1)
     *       |-- t1
     *       +-- Filter(count(marker) = 0)
     *             +-- Aggregate(group by [key.c1], count(marker))
     *                   +-- LEFT OUTER JOIN (t2.c1 = key.c1)              [keeps the empty domain]
     *                         |-- Aggregate(group by [t1.c1])            [the keys of t1]
     *                         |     +-- t1
     *                         +-- Project([true as marker, t2.c1])
     *                               +-- t2
     *
     * A HAVING clause which references the outer query, eg. the subquery of
     *
     *     select t1.c1 from t1 where exists (select count(*) from t2
     *         where t2.c1 = t1.c1 having count(*) <= t1.c1 - 7)
     *
     * the row kept by that HAVING clause is the one of the domain of the outer row, so it cannot
     * be evaluated on a group of the inner side when the domain is empty. The predicate is
     * evaluated above the new aggregation, with the correlation key in place of the outer slot:
     *
     *     LEFT SEMI JOIN (t1.c1 <=> key.c1)
     *       |-- t1
     *       +-- Project([key.c1])
     *             +-- Filter(count(marker) <= key.c1 - 7)
     *                   +-- Aggregate(group by [key.c1], count(marker))
     *                         +-- LEFT OUTER JOIN (t2.c1 = key.c1)
     *                               |-- Aggregate(group by [t1.c1])      [the keys of t1]
     *                               |     +-- t1
     *                               +-- Project([true as marker, t2.c1])
     *                                     +-- t2
     *
     * The same three cases apply to an IN subquery, which compares the outer expression with the
     * value of the aggregation: the row of an empty correlated domain has to exist for that
     * comparison as well. For example the subquery of
     *
     *     select t1.c1 from t1 where t1.c1 in (select count(*) from t2 where t2.c1 = t1.c1)
     *
     * is a global aggregate without a HAVING clause, and the value of the empty correlated domain
     * (the count 0, which the IN compares the outer row with instead of dropping it) has to be
     * produced for every outer row; the subquery of
     *
     *     select t1.c1 from t1 where t1.c1 in (select count(*) from t2
     *         where t2.c1 < t1.c1 having count(*) = t1.c1)
     *
     * has a correlated predicate which is not an equality and a HAVING clause which references the
     * outer query, so its aggregation is built on the outer side as well.
     *
     * A scalar subquery reads the output of the aggregation of the correlated domain of one outer
     * row, and the join which pairs the outer row with that aggregation (the left outer join of
     * ScalarApplyToJoin, or the join this rewrite builds) keeps the groups of the inner side whose
     * key is the value of the outer row: that is the aggregation of the domain of the outer row
     * exactly when every correlated predicate is an equality between the outer side and the inner
     * side, eg. t2.c1 = t1.c1. Every other predicate is reported by the join of the scalar
     * subquery (ScalarApplyToJoin accepts an equality alone, see its guard), so the aggregation of
     * the domain has to be built on the outer side: for example the scalar subquery of
     *
     *     select t1.c1, (select count(*) from t2 where t2.c1 < t1.c1) from t1
     *
     * is evaluated by the aggregation of the outer side (its plan is the plan of the EXISTS
     * subquery above with the LEFT SEMI JOIN replaced by a LEFT OUTER JOIN, which keeps the outer
     * rows whose aggregation has no row and returns null for them).
     */
    private static boolean needCorrelatedAggregationOnOuter(LogicalApply<?, ?> apply, TheAggregation aggregation,
            List<Expression> correlatedPredicate, CorrelatedAggregatePredicates predicates) {
        LogicalAggregate<?> agg = aggregation.domainAggregation();
        // The subqueries which are aggregated on the outer side whatever their correlated predicates
        // say, because the rewrite of the outer side evaluates those predicates itself: the plan
        // below is a set of domains, one for every correlation key, and the correlated predicates
        // are the conditions of the join which pairs a domain with the inner rows (see
        // pullUpCorrelatedPredicateByAggregatingOuter), so they do not have to be split into an
        // outer side and an inner side. An EXISTS subquery which has to keep the nodes above its
        // aggregation (they decide on the rows of the subquery) and a global aggregate whose HAVING
        // clause references the outer query (the row of the empty correlated domain exists as well)
        // are those subqueries; for the others, a predicate whose sides mix the outer query and the
        // subquery keeps the error of the original rewrite (see isSupportedCorrelatedConjuncts).
        if (!apply.isScalar()) {
            if (keepsNodesAboveTheAggregation(apply, aggregation)) {
                // an EXISTS subquery which has to keep such a node cannot be evaluated on the
                // groups of the inner side (see keepsNodesAboveTheAggregation)
                return true;
            }
            if (predicates.hasAggregatePredicates() && agg.getGroupByExpressions().isEmpty()) {
                // a predicate of the HAVING clause which references the outer query decides whether
                // the row of a global aggregate is kept, and that row exists for every outer row
                // including the rows of an empty correlated domain, so the predicate cannot be
                // evaluated on a group of the inner side (eg. the IN subquery of select t1.c1
                // from t1 where t1.c1 in (select count(*) from t2 where t2.c1 = t1.c1 having
                // count(*) <= t1.c1 - 7))
                return true;
            }
        }
        if (!isSupportedCorrelatedConjuncts(correlatedPredicate, apply.getCorrelationSlot())) {
            // every kind of subquery keeps the error of the original rewrite for these predicates
            return false;
        }
        if (theEmptyInputOfAGlobalAggregateIsObservable(apply, aggregation, predicates)
                && keepsTheRowOfAnEmptyDomain(apply, aggregation, predicates)) {
            // The aggregation of the original subquery produces one row for a correlation key whose
            // rows below the aggregation of the domain are missing, and every aggregate above that
            // aggregation is global: the aggregation of the outer side keeps such a row as well (it
            // marks the row which it keeps for the key, so the aggregates above it return the values of
            // an empty input, see pullUpCorrelatedPredicateByAggregatingOuter), while the aggregation
            // of the inner side drops the key entirely (it adds the keys to the group by of every
            // aggregate, see withTheKeysInTheGroupBy).
            return true;
        }
        if (apply.isScalar()) {
            // The left outer join of a scalar subquery pairs the outer row with the groups of the
            // inner side whose key is the value of the outer row, which is the aggregation of the
            // domain of the outer row exactly when every correlated predicate is an equality
            // between the outer side and the inner side (eg. t2.c1 = t1.c1). Every other comparison
            // (eg. t2.c1 < t1.c1, or t2.c1 <=> t1.c1 whose domain contains the inner rows of the
            // null key) is evaluated by the aggregation of the outer side: the join of the scalar
            // subquery reports those predicates (ScalarApplyToJoin admits an equality alone, see
            // its guard).
            if (!isEqualityBetweenTheOuterSideAndTheInnerSide(correlatedPredicate)) {
                return true;
            }
            if (!agg.getGroupByExpressions().isEmpty()) {
                // the domain of an equality correlated predicate is exactly one group of the
                // aggregation: the rows of the subquery for one outer row are the rows of that
                // group, and the outer rows of an empty correlated domain have no group at all (the
                // subquery returns no row for them, whatever its HAVING clause says)
                return false;
            }
            // A global aggregation returns a row for the outer rows of an empty correlated domain as
            // well: the left outer join of the original rewrite keeps a null for the output of the
            // subquery of those rows, which is the value of the aggregation of that domain (see
            // keepsTheValueOfAnEmptyDomain), but a HAVING clause decides on the row of the empty
            // domain instead, and a row of the empty domain exists whatever its predicate says. The
            // aggregation of the domain is built on the outer side for those subqueries as well.
            if (predicates.hasHaving()) {
                return havingMayHoldWithEmptyInput(agg, predicates.havingConjuncts());
            }
            return !keepsTheValueOfAnEmptyDomain(agg);
        }
        if (apply.isExist() && !predicates.hasHaving()) {
            // an EXISTS/NOT EXISTS subquery without a HAVING clause only depends on the existence of
            // the aggregation result, which is kept by grouping the inner side; an IN subquery
            // compares the value of the aggregation instead, so the row which a global aggregate
            // returns for an empty correlated domain (eg. select t1.c1 from t1 where t1.c1 in
            // (select count(*) from t2 where t2.c1 = t1.c1) compares the outer row with the count
            // 0 of its empty domain) has to be produced for it as well
            return false;
        }
        // The IN subqueries and the EXISTS/NOT EXISTS subqueries which have a HAVING clause: the
        // rows of the subquery for one outer row are the rows of the aggregation of its domain as
        // long as the correlated predicates map the domain of every outer row onto one group of the
        // aggregate, so that the HAVING clause of that group is the HAVING clause of the outer row.
        if (hasDomainPredicateWhichBreaksThePartitionOfTheGroups(predicates, apply)) {
            // the domain of one outer row is the union of several groups of the aggregate
            return true;
        }
        if (predicates.hasAggregatePredicates()) {
            // the domain of one outer row is exactly one group and the rows of the subquery are the
            // rows of that group, so the original rewrite is still equivalent
            return false;
        }
        if (!agg.getGroupByExpressions().isEmpty()) {
            // an equality correlated predicate maps the domain of every outer row onto exactly one
            // group
            return false;
        }
        // The IN subqueries and the EXISTS/NOT EXISTS subqueries which have a HAVING clause: the
        // row which a global aggregate returns for an empty correlated domain exists for every
        // outer row, so it has to be produced whenever a HAVING clause may hold for it. An IN
        // subquery without a HAVING clause needs that row as well, whatever the value which the
        // aggregate returns for an empty input is: the conversion of the IN into a join reports the
        // null of the inner side (the sum of an empty domain, for example) like a domain without a
        // row, which turns the not in of an outer row without a match into true instead of the
        // unknown which the null of the empty domain produces.
        return havingMayHoldWithEmptyInput(agg, predicates.havingConjuncts());
    }

    /**
     * Whether every correlated predicate is an equality between the outer side and the inner side,
     * eg. t2.c1 = t1.c1 or t2.abs(c1) = t1.c1 (the caller has already checked that one side of
     * every predicate is built from the outer query alone and the other one from the subquery
     * alone). Those are the predicates for which the rows of the domain of one outer row are
     * exactly one group of the inner side, which is what the left outer join of a scalar subquery
     * pairs the outer row with.
     *
     * The null safe equality is the counterexample: t2.c1 <=> t1.c1 is an equal predicate as well,
     * but its domain contains the inner rows whose key is null, which the condition of the left
     * outer join of ScalarApplyToJoin cannot express (that join admits an EqualTo alone). The
     * scalar subquery of
     *
     *     select nq_o.k, (select count(*) from nq_i where nq_i.k <=> nq_o.k) from nq_o
     *
     * is evaluated by the aggregation of the outer side, whose join of the domain compares the
     * inner key with the key of the outer row by the null safe equality, so that the inner rows
     * whose key is null are part of the domain of the outer row whose key is null.
     */
    private static boolean isEqualityBetweenTheOuterSideAndTheInnerSide(List<Expression> correlatedPredicate) {
        for (Expression conjunct : correlatedPredicate) {
            if (!(conjunct instanceof EqualTo)) {
                // t2.c1 <=> t1.c1 is an EqualPredicate as well but not an EqualTo: its domain
                // contains the inner rows whose key is null, which the condition of the left outer
                // join of ScalarApplyToJoin cannot express (that join admits an EqualTo alone)
                return false;
            }
        }
        return true;
    }

    /**
     * Whether the null which the original rewrite keeps for the outer rows of an empty correlated
     * domain is the value which the subquery has for that domain. The scalar subquery exposes the
     * output of its aggregation, and the left outer join of the original rewrite reports a null for
     * it:
     *
     * - an aggregate of the nullable family returns a null for an empty input, which is the value
     *   the subquery has for the empty domain;
     *
     * - SubqueryToApply wraps the aggregates which return a value of their own for an empty input
     *   (the count 0 of select count(*) from t2 where t2.c1 = t1.c1, the empty array of an
     *   array_agg, ...) with an nvl on that value, and the nvl turns the null of the join into the
     *   value of the aggregation of the empty domain. The count(*)/any_value wrapper which it adds
     *   around the select list of a scalar subquery is not part of the value: its any_value is null
     *   for the rows of the join, which is the value of the empty domain;
     *
     * - an aggregate which is neither of those (a UDAF, whose value for an empty input is written
     *   in the UDAF itself) keeps the null of the join as well: SubqueryToApply adds an nvl for the
     *   aggregates above alone (see addNvlForScalarSubqueryOutput), so no value of a UDAF replaces
     *   the null which the join reports for it.
     *
     * An aggregate which declares a not null result without being wrapped by that nvl (an agg_state
     * combinator, see UnionCombinator) has a value of its own for an empty input instead, so the
     * aggregation of the subquery has to be built on the outer side for it. For example the max of
     * the subquery of
     *
     *     select t1.c1, (select max(t2.c2) from t2 where t2.c1 = t1.c1) from t1
     *
     * keeps the aggregation on the inner side (the null which the left outer join of
     * ScalarApplyToJoin keeps for the outer rows without a group is the value of the empty
     * domain), and so do the count of the subquery of
     *
     *     select t1.c1, (select count(*) from t2 where t2.c1 = t1.c1) from t1
     *
     * (SubqueryToApply wraps that count as ifnull(count(*), 0) in the projection of the left outer
     * join, which turns the null of the join into the count 0 of the empty domain) and the UDAF of
     * the subquery of
     *
     *     select t1.c1, (select my_udaf(t2.c2) from t2 where t2.c1 = t1.c1) from t1
     *
     * (SubqueryToApply adds no nvl for a UDAF, so the null of the join is the value which the
     * subquery exposes for the empty domain).
     */
    private static boolean keepsTheValueOfAnEmptyDomain(LogicalAggregate<?> aggregate) {
        Set<AggregateFunction> functions = Sets.newLinkedHashSet();
        for (NamedExpression output : aggregate.getOutputExpressions()) {
            functions.addAll(output.collect(AggregateFunction.class::isInstance));
        }
        // the null of the join is the value of the empty domain for every aggregate whose result is
        // nullable, and SubqueryToApply repairs it for the aggregates which return a value of their
        // own for an empty input; only the aggregates which declare a not null result without such
        // a repair need the value of their own, which the aggregation of the outer side produces
        return functions.stream().allMatch(function -> !(function instanceof AlwaysNotNullable)
                || function instanceof NotNullableAggregateFunction);
    }

    /**
     * Whether a filter which sits directly above the aggregation below it selects the rows of the
     * domain of one outer row, instead of deciding which rows of the aggregation below it survive
     * (the HAVING clause of that aggregation). For example the WHERE clause of the subquery of
     *
     *     select t1.c1 from t1 where t1.c2 >
     *         (select col from (select c2 as col from t2 group by c2) tt where t1.c1 = tt.col)
     *
     * was pushed into the derived table, so it sits above the aggregation of that table and reads
     * the columns of its output. The aggregation below such a filter already produces the rows
     * which the outer row has to be paired with, while the aggregates above it have to group those
     * rows by the correlation key.
     *
     * A predicate whose inner side is the value of an aggregation decides on the rows of that
     * aggregation instead (the HAVING clause of the subquery): an aggregation cannot group by the
     * value of another aggregation, so such a filter keeps the chain of the aggregation.
     */
    private static boolean selectsTheRowsOfTheDomain(LogicalFilter<Plan> filter, LogicalApply<?, ?> apply) {
        Map<Boolean, List<Expression>> split =
                Utils.splitCorrelatedConjuncts(filter.getConjuncts(), apply.getCorrelationSlot());
        List<Expression> correlatedConjuncts = split.get(true);
        if (correlatedConjuncts.isEmpty()) {
            // a filter which does not read a column of the outer query cannot select the domain of
            // an outer row
            return false;
        }
        if (correlatedConjuncts.stream()
                .anyMatch(conjunct -> !(conjunct instanceof BinaryExpression)
                        && !(conjunct instanceof Not && conjunct.child(0) instanceof BinaryExpression))) {
            // the inner side of such a predicate cannot be read (see Utils.getUnCorrelatedExprs)
            return false;
        }
        for (Expression innerSide : Utils.getUnCorrelatedExprs(correlatedConjuncts, apply.getCorrelationSlot())) {
            if (innerSide.anyMatch(AggregateFunction.class::isInstance)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether one of the predicates of the domain of the outer rows partitions the inner rows of
     * one outer row into several groups of the aggregate (see breaksDomainPartition).
     */
    private static boolean hasDomainPredicateWhichBreaksThePartitionOfTheGroups(
            CorrelatedAggregatePredicates predicates, LogicalApply<?, ?> apply) {
        return predicates.domainPredicates().stream()
                .anyMatch(conjunct -> breaksDomainPartition(conjunct, apply.getCorrelationSlot()));
    }

    /**
     * Whether every correlated conjunct is a comparison which the rewrites can split into an outer
     * side and an inner side, see isSupportedCorrelatedComparison: those are the
     * predicates which the original rewrite groups the inner side by, and the predicates of which
     * the rewrite of the outer side replaces the outer side by a correlation key.
     */
    private static boolean isSupportedCorrelatedConjuncts(
            List<Expression> correlatedPredicate, List<Slot> correlationSlots) {
        for (Expression conjunct : correlatedPredicate) {
            if (!isSupportedCorrelatedComparison(conjunct, correlationSlots)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether this predicate changes the domain of an outer row in a way which is not a group of
     * the aggregate. Only an equality between the outer side and the inner side partitions the
     * inner rows of one outer row into exactly the groups of the aggregate, while a predicate which
     * does not reference the outer query at all just filters the inner rows: in the subquery of
     * select t1.c1 from t1 where exists (select count(*) from t2 where t2.c1 < t1.c1 group by
     * t2.c2 having count(*) = 2) the domain of one outer row is the union of the groups (t2.c1,
     * t2.c2), while the predicate t2.c1 = t1.c1 maps it onto exactly one group.
     */
    private static boolean breaksDomainPartition(Expression conjunct, List<Slot> correlationSlots) {
        if (conjunct instanceof EqualPredicate) {
            return false;
        }
        return conjunct.getInputSlots().stream().anyMatch(correlationSlots::contains);
    }

    /**
     * Whether the correlated predicate is a comparison whose sides do not mix the outer query and
     * the subquery, eg. t2.c1 < t1.c1 or t1.c1 = t2.abs(c1). Those are the predicates which can be
     * evaluated by joining the two sides, and the ones supported by the original rewrite.
     *
     * A comparison whose one side mixes the outer query and the subquery is not supported, for
     * example t2.c1 = t1.c1 + t2.c2 (its right side reads the outer slot t1.c1 and the inner
     * column t2.c2, so the two sides cannot be joined apart): the aggregation keeps the behavior
     * of the original rewrite and the subquery is reported with the "Unsupported correlated
     * subquery with correlated predicate t2.c1 = t1.c1 + t2.c2" error. The same comparison with a
     * side which is built from the outer query alone is supported, for example t2.c1 = t1.c1 + 1
     * (the key is t1.c1 and the domain is joined on t2.c1 = key.c1 + 1).
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
     * Rewrite the subquery which has a correlated predicate below its aggregation into a join whose
     * right side aggregates the outer rows together with their correlated inner rows, so that the
     * aggregation of one outer row is the aggregation of exactly the inner rows satisfying the
     * correlated predicate. An EXISTS/NOT EXISTS subquery is decided by that aggregation, which is
     * read through a semi/anti join: for example the subquery of select t1.c1 from t1 where exists
     * (select count(*) from t2 where t2.c1 < t1.c1 having count(*) = 0). An IN subquery compares
     * the outer expression with the value of that aggregation and a scalar subquery exposes it as
     * a value: those keep the left outer join of their apply (see the end of this method), for
     * example the subquery of select t1.c1, (select count(*) from t2 where t2.c1 < t1.c1) from t1.
     *
     * The right side of the join is built around two references of the outer plan, because the
     * domains of two outer rows are the same as soon as their correlation slots are equal: the
     * original outer plan is the left side of the semi join, and a deep copy of it (with its own
     * slots and relation ids) computes the distinct correlation keys. Those keys are joined with
     * the inner side on the predicates of the domain, and the result is aggregated again, this
     * time grouped by the correlation keys together with the group by columns of the subquery: the
     * aggregation of one correlation key is the aggregation of the subquery for the outer rows
     * which own that key. The filter of the HAVING clause sits above that aggregation, exactly as
     * it sat above the aggregation of the subquery, and the predicates of the HAVING clause which
     * reference the outer query (an earlier rule pulled them into the apply) are evaluated above
     * it as well, so that they decide on the aggregation of the whole domain of an outer row, the
     * empty domain included. The outer rows are then paired with the aggregation of their
     * correlation key by a LEFT SEMI JOIN (a LEFT ANTI JOIN when the subquery is NOT EXISTS), by a
     * LEFT OUTER JOIN when the subquery is a scalar subquery, or by the correlation filter of the
     * apply when it is an IN subquery.
     *
     * A grouped aggregate returns no row at all for an empty correlated domain, so a plain inner
     * join between the distinct keys and the inner side reproduces the behaviour of the subquery.
     * A global aggregate returns exactly one row for every outer row instead, the empty domain
     * included, so the rows of an empty correlated domain have to be kept: a left outer join gives
     * the aggregation one row whose inner columns are null, which is the same input the subquery
     * aggregates for an empty input. Only the count aggregations tell no row and one row of nulls
     * apart, and count(*) also counts the kept row itself, so the counts are replaced by counts of
     * the projected marker column, which is null for the rows which were kept for an empty
     * correlated domain.
     *
     * @return null if this rewrite cannot be applied safely, the caller then reports the subquery
     *         as unsupported
     */
    private static Plan pullUpCorrelatedPredicateByAggregatingOuter(LogicalApply<?, ?> apply,
            TheAggregation aggregation, List<Expression> unCorrelatedPredicate,
            CorrelatedAggregatePredicates predicates) {
        LogicalAggregate<?> agg = aggregation.domainAggregation();
        LogicalFilter<Plan> filter = aggregation.domainFilter();
        Set<Slot> correlationSlots = predicates.keySlots(apply.getCorrelationSlot());
        if (containsSensitiveExpression(apply.left(), correlationSlots)
                || hasNonDeterministicRows(apply.left())
                || containsNoneMovableFunction(apply.right())
                || containsSensitiveSubqueryExpression(apply, predicates)
                || referencesOuterSlot(apply.right(), ImmutableSet.copyOf(predicates.whereConjuncts),
                        apply.getCorrelationSlot())
                || !predicates.isResolvable(apply, aggregation, filter)) {
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
        boolean keepsTheRowOfAnEmptyDomain = keepsTheRowOfAnEmptyDomain(apply, aggregation, predicates);
        boolean keepEmptyDomain = agg.getGroupByExpressions().isEmpty() || keepsTheRowOfAnEmptyDomain;
        Slot matchMarker = null;
        // The left outer join below reports the columns of the side which it fills with nulls as
        // nullable (see JoinUtils.getJoinOutput): the expressions above it have to read the nullable
        // versions of those columns, because a reference to a not-nullable column of the inner side
        // reaches the join from below it and makes AdjustNullable convert that reference, and that
        // conversion is reported as an error while fe_debug is set. For example the subquery of
        //
        //     select * from t1 where t1.k1 =
        //         (select sum(k1) from t3 where t1.k1 != t3.v1 and t3.v2 = 2)
        //
        // aggregates k1 of t3, a column which is declared not null, and the row which the left outer
        // join keeps for an empty correlated domain turns the aggregation of the subquery into
        // sum(if(marker, k1, null)), whose argument has to be the nullable k1 which that join
        // produces:
        //
        //     Aggregate(group by [key.k1], output [sum(if(marker, k1, null)) AS sum(k1), key.k1])
        //       +-- LEFT OUTER JOIN (t3.v1 != key.k1 AND t3.v2 = 2)    [keeps the empty domain]
        //             |-- Aggregate(group by [t1.k1], output [t1.k1])
        //             |     +-- t1
        //             +-- Project([true AS marker, t3.k1, t3.k2, t3.k3, t3.v1, t3.v2])
        //                   +-- Filter(t3.v2 = 2)
        //                         +-- t3
        Map<Expression, Expression> nullableInnerSlots = Maps.newHashMap();
        if (keepEmptyDomain) {
            Alias marker = new Alias(BooleanLiteral.TRUE, CORRELATION_MATCH_MARKER);
            // the marker is null for the rows which the left outer join keeps for an empty
            // correlated domain, so the join reports it as a nullable column for the same reason
            matchMarker = marker.toSlot().withNullable(true);
            List<NamedExpression> projects = Lists.newArrayList(marker);
            projects.addAll(inner.getOutput());
            inner = new LogicalProject<>(projects, inner);
            for (Slot slot : inner.getOutput()) {
                if (!slot.nullable()) {
                    nullableInnerSlots.put(slot, slot.withNullable(true));
                }
            }
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
        if (compensated == null) {
            // an aggregate of the subquery cannot be guarded, so the aggregation of the outer side
            // cannot tell the row which was kept for an empty correlated domain apart (see
            // guardAggregateArguments)
            return null;
        }

        List<Expression> newGroupBy = Lists.newArrayList(slotToKey.values());
        newGroupBy.addAll(agg.getGroupByExpressions());
        List<NamedExpression> newOutputs = Lists.newArrayList();
        for (NamedExpression output : agg.getOutputExpressions()) {
            newOutputs.add((NamedExpression) ExpressionUtils.replace(
                    ExpressionUtils.replace(output, compensated), nullableInnerSlots));
        }
        // the keys are appended after the outputs of the subquery: the first column of the output is
        // the value which an IN subquery compares (eg. the count which k in (select count(*) ...)
        // compares the outer value with) and the value which a scalar subquery exposes, and neither
        // of them may move
        newOutputs.addAll(keyExpressions);
        if (keepsTheRowOfAnEmptyDomain) {
            // The aggregates above the aggregation of the domain have to tell the row which is kept
            // for an empty domain apart from the rows below them, so that they return the values of an
            // empty input for it (see withTheKeysInTheGroupBy), and the filters between those
            // aggregations let the row pass (see rebuildTheAggregationChain): the aggregation of the
            // domain exposes the marker of that row up to them. The marker is the same for every row of
            // one correlation key, so the grouping of the rows of a key does not change.
            newGroupBy.add(matchMarker);
            newOutputs.add(matchMarker);
        }
        LogicalAggregate<Plan> newAggregate = new LogicalAggregate<>(newGroupBy, newOutputs, domainJoin);

        // the predicates which were pulled into the apply are not part of the plan of the subquery
        // any more: they were evaluated on the rows of the old aggregation and have to be evaluated
        // on the rows of the new one, directly above the aggregate of the domain they were pulled
        // from when the subquery does not wrap that aggregate (otherwise they are evaluated above
        // the whole chain, whose aggregates are the ones their columns belong to)
        Set<Expression> movedPredicates = Sets.newLinkedHashSet();
        for (Expression conjunct : predicates.pulledPredicates()) {
            // the predicates which were pulled into the apply are evaluated above the join as well, so
            // they read the columns of the inner side through the nullable slots of the join
            movedPredicates.add(ExpressionUtils.replace(
                    ExpressionUtils.replace(ExpressionUtils.replace(conjunct, compensated), slotToKey),
                    nullableInnerSlots));
        }
        Map<LogicalAggregate<?>, Plan> newAggregations = new IdentityHashMap<>();
        for (LogicalAggregate<?> aggregate : aggregation.aggregationChain()) {
            if (aggregate == agg) {
                newAggregations.put(aggregate, movedPredicates.isEmpty() || !aggregation.onlyTheAggregationOfTheDomain()
                        ? newAggregate
                        : new LogicalFilter<>(movedPredicates, newAggregate));
                continue;
            }
            // the aggregates above the deepest one read the rows it produces, so they keep the rows
            // of one correlation key together as well (the keys are appended to their output, so
            // that the aggregate above them can group by them)
            LogicalAggregate<?> withTheKeys = withTheKeysInTheGroupBy(aggregate, keyExpressions,
                    keepsTheRowOfAnEmptyDomain ? matchMarker : null,
                    aggregate != aggregation.topAggregation());
            if (withTheKeys == null) {
                // an aggregate above the aggregation of the domain cannot be guarded, so the row which
                // is kept for an empty domain would contribute to it
                return null;
            }
            newAggregations.put(aggregate, withTheKeys);
        }
        // the nodes above the aggregation of the subquery (the HAVING clause, the filters over the
        // projection of the select list, that projection) are kept as they are: they are evaluated on
        // the rows of the new aggregation, which are the rows of the aggregation of the subquery for
        // the correlation key of one outer row
        Set<Slot> keysToExpose = keyExpressions.stream().map(NamedExpression::toSlot)
                .collect(ImmutableSet.toImmutableSet());
        Set<LogicalFilter> filtersWhichKeepTheRowOfAnEmptyDomain = ImmutableSet.of();
        if (keepsTheRowOfAnEmptyDomain) {
            // The filters between the aggregation of the domain and the aggregation above it read the
            // marker of the row which is kept for an empty domain, so that the row passes them (see
            // rebuildTheAggregationChain). The projections below the top aggregation carry the marker
            // up to the aggregates above the aggregation of the domain, which guard their arguments
            // with it, while the projections above the top aggregation are not read by a node which
            // needs the marker (the marker is not an output of the subquery).
            filtersWhichKeepTheRowOfAnEmptyDomain =
                    theFiltersBetweenTheAggregationOfTheDomainAndTheOneAboveIt(aggregation);
        }
        // the predicates of the apply are evaluated on the nodes above the aggregation of the
        // subquery, which produce the outputs of that aggregation themselves, so no output of it has
        // to be appended to the projections below them
        Plan newRight = rebuildTheAggregationChain(apply.right(), aggregation, newAggregations, keysToExpose,
                ImmutableSet.of(), keepsTheRowOfAnEmptyDomain ? matchMarker : null,
                filtersWhichKeepTheRowOfAnEmptyDomain, false);
        if (!movedPredicates.isEmpty() && !aggregation.onlyTheAggregationOfTheDomain()) {
            newRight = new LogicalFilter<>(movedPredicates, newRight);
        }

        // the aggregate of the subquery is now computed for the correlation keys of every outer row,
        // so the outer rows which own one of those groups are the rows for which the subquery has rows
        List<Expression> backConjuncts = new ArrayList<>(slotToKey.size());
        for (Map.Entry<Expression, Expression> entry : slotToKey.entrySet()) {
            backConjuncts.add(new NullSafeEqual(entry.getKey(), entry.getValue()));
        }
        if (apply.isScalar()) {
            // A scalar subquery exposes the output of the aggregation of its correlated domain, and
            // its value is read from the first column of the right side (the keys are appended
            // after the outputs of the subquery): the join which pairs an outer row with the
            // aggregation of its own key is the left outer join which ScalarApplyToJoin builds for
            // the scalar subqueries it can unnest itself, with the null safe equality which pairs
            // the outer row with the key of its own aggregation as its condition (the join of
            // ScalarApplyToJoin only admits a plain equality, see the guard there), and the value
            // of the outer rows whose aggregation has no row is the null of the outer side. For
            // example the scalar subquery of
            //
            //     select t1.c1, (select count(*) from t2 where t2.c1 < t1.c1) from t1
            //
            // becomes
            //
            //     LEFT OUTER JOIN (t1.c1 <=> key.c1)
            //       |-- t1
            //       +-- Aggregate(group by [key.c1], count(marker) as count(*))
            //             +-- LEFT OUTER JOIN (t2.c1 < key.c1)              [keeps the empty domain]
            //                   |-- Aggregate(group by [t1.c1])            [the keys of t1]
            //                   |     +-- t1
            //                   +-- Project([true as marker, t2.c1, t2.c2])
            //                         +-- t2
            return new LogicalJoin<>(apply.isNeedAddSubOutputToProjects()
                    ? JoinType.LEFT_OUTER_JOIN : JoinType.LEFT_SEMI_JOIN,
                    ExpressionUtils.EMPTY_CONDITION, backConjuncts, new DistributeHint(DistributeType.NONE),
                    apply.getMarkJoinSlotReference(), outer, newRight, null);
        }
        if (!apply.isExist()) {
            // an IN/NOT IN subquery keeps its apply: the correlation filter pairs an outer row with
            // the aggregation of its own key (the aggregation of the domain of the outer row, the
            // empty domain included), and the rule which converts the IN into a join (InApplyToJoin)
            // compares the outer expression with the first column of the output of the aggregation
            return new LogicalApply<>(apply.getCorrelationSlot(), apply.getSubqueryType(), apply.isNot(),
                    apply.getCompareExpr(), apply.getTypeCoercionExpr(),
                    ExpressionUtils.optionalAnd(backConjuncts), apply.getMarkJoinSlotReference(),
                    apply.isNeedAddSubOutputToProjects(), apply.isMarkJoinSlotNotNull(), outer, newRight);
        }
        return new LogicalJoin<>(apply.isNot() ? JoinType.LEFT_ANTI_JOIN : JoinType.LEFT_SEMI_JOIN,
                ExpressionUtils.EMPTY_CONDITION, backConjuncts, new DistributeHint(DistributeType.NONE),
                apply.getMarkJoinSlotReference(), outer, newRight, null);
    }

    /**
     * Replace the arguments of the aggregates so that the row which the left outer join keeps for
     * an empty correlated domain does not contribute to the aggregation: the marker of that row is
     * null, so every argument is null for it and the aggregates which ignore null arguments (the
     * caller only admits those) return the value of an empty input. count(*) has no argument to
     * build that guard on, so it counts the marker instead; a distinct count keeps its argument and
     * its distinct flag, because the null of the kept row must not be counted as a value. For
     * example count(*) becomes count(marker) and sum(1) becomes sum(if(marker, 1, null)), which
     * returns null for the row which was kept for an empty domain.
     *
     * An argument which controls the aggregate instead of providing the values to aggregate is kept
     * as it is, because the guard makes the aggregate illegal: the second argument of topn_array is
     * the number of values to keep and has to stay a positive literal. Whether an argument controls
     * the aggregate or provides values is decided by the checks which the aggregate runs after the
     * rewrite and before the type coercion (see isLegalWithTheGuardedArgument). For example the
     * subquery of
     *
     *     select (select topn_array(i.v, 2) from i where i.k < o.k) from o
     *
     * is rewritten to topn_array(if(marker, i.v, null), 2): the first argument is null for the row
     * which was kept for an empty domain, so topn_array ignores that row and returns the value of an
     * empty input (that is the contract of NullIgnoringAggregateFunction), while the guarded form
     * topn_array(if(marker, i.v, null), if(marker, 2, null)) is rejected by the check of
     * topn_array, which requires the number of values to keep to be a literal. An aggregate whose
     * every argument controls it cannot be given an argument which is null for the kept row, so the
     * caller does not rewrite it.
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
            boolean ignoresTheKeptRow = false;
            for (int index = 0; index < function.arity(); index++) {
                Expression argument = function.getArgument(index);
                Expression guarded = new If(matchMarker, argument,
                        new NullLiteral(argument.getDataType()));
                if (isLegalWithTheGuardedArgument(function, index, guarded)) {
                    arguments.add(guarded);
                    ignoresTheKeptRow = true;
                } else {
                    // the argument controls the aggregate: the guard would make the aggregate
                    // illegal, so the argument is kept and the arguments which provide the values
                    // to aggregate make the kept row invisible
                    arguments.add(argument);
                }
            }
            if (!ignoresTheKeptRow) {
                // every argument of the aggregate controls it, so the aggregate cannot tell the row
                // which was kept for an empty correlated domain from a row of the inner side
                return null;
            }
            replace.put(function, function.withChildren(arguments));
        }
        return replace;
    }

    /**
     * Whether the aggregate stays legal when the argument at the given position is replaced by its
     * guarded version (see guardAggregateArguments). An aggregate rejects a guarded argument when
     * that argument controls it instead of providing the values to aggregate: topn_array requires
     * the number of values to keep to be a literal (see checkLegalityAfterRewrite of TopNArray),
     * and bitmap_union_int requires its arguments to be constants (see
     * checkLegalityBeforeTypeCoercion of BitmapUnionInt).
     */
    private static boolean isLegalWithTheGuardedArgument(
            AggregateFunction function, int position, Expression guardedArgument) {
        List<Expression> arguments = Lists.newArrayList(function.getArguments());
        arguments.set(position, guardedArgument);
        try {
            AggregateFunction guarded = (AggregateFunction) function.withChildren(arguments);
            guarded.checkLegalityBeforeTypeCoercion();
            guarded.checkLegalityAfterRewrite();
            return true;
        } catch (AnalysisException e) {
            // the check of the aggregate does not accept the guarded argument
            return false;
        }
    }

    /**
     * Whether the HAVING clause of a global aggregate can hold for the row which the aggregate
     * returns for an empty input. For example having count(*) = 0 holds for an empty input (the row
     * of the aggregation of the empty domain has to survive), while having sum(t2.c1) is not null
     * rejects that row, so the aggregation of the subquery can stay on the inner side.
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
                // A comparison of a null with any other value is null as well, which the folding of
                // the expressions does not evaluate, and the predicate which holds it rejects the
                // row of the empty input: the comparison is replaced by a null boolean first.
                folded = FoldConstantRuleOnFE.evaluateWithoutContext(replaceComparisonsWithNull(folded));
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
     * Replace every comparison which has a null operand by a null boolean (the null safe equality
     * is the exception: it evaluates the comparison of a null with a null to true and the
     * comparison of a null with any other value to false).
     */
    private static Expression replaceComparisonsWithNull(Expression expression) {
        return expression.rewriteDownShortCircuit(node -> {
            if (!(node instanceof ComparisonPredicate) || node instanceof NullSafeEqual) {
                return node;
            }
            for (Expression child : node.children()) {
                if (child.isNullLiteral()) {
                    return NullLiteral.BOOLEAN_INSTANCE;
                }
            }
            return node;
        });
    }

    /**
     * The value which an aggregate function returns for an empty input, or null if it cannot be
     * decided. For example count(*) returns 0 and sum(t2.c1) returns null, while the value of an
     * array_agg(t2.c1) cannot be decided. Neither can the value of a UDAF: it is written in the
     * UDAF itself, so a UDAF whose state starts at 0 (an inline sum, for example) returns 0 for an
     * empty input although its result is declared nullable.
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
     * Whether the outer plan of the apply computes a value which this rewrite may not evaluate
     * twice.
     *
     * The rewrite evaluates the outer plan twice (the original plan on the left of the resulting
     * join, and a deep copy which computes the distinct correlation keys), so the two evaluations
     * have to return the same rows and the same values of the correlation keys. The values of the
     * plan are followed through its slots (see collectVolatileSlots), and a volatile value such as
     * random() is rejected when it decides which rows the plan returns (its predicates and its
     * groupings) and when it contributes to the value of a correlation key. A volatile column
     * which only decorates the output is accepted, because the rewrite reads the correlation keys
     * of the plan alone, and the output of the rewritten plan is the original plan.
     *
     * - a volatile correlation key is rejected: the outer plan of the query of
     *
     *       select t.k from (select random() as k from t1 e) t
     *       where exists (select count(*) from t2 i where i.k < t.k having count(*) = 0)
     *
     *   is the plan
     *
     *       LogicalProject[k = random()] over LogicalOlapScan(t1)
     *
     *   and the slot k of its output is the correlation key, so the two evaluations of the plan
     *   compute different keys: the aggregation of one key would not be the aggregation of the
     *   subquery for the outer rows which own it.
     *
     * - a volatile value which decides the rows of the outer plan is rejected as well, for example
     *   the plan of the outer query of
     *
     *       select e.k from t1 e
     *       where random() < 0.5 and exists (select count(*) from t2 i
     *           where i.k < e.k having count(*) = 0)
     *
     *       LogicalFilter[random() < 0.5] over LogicalOlapScan(t1)
     *
     *   (collectVolatileSlots returns null for it): the predicate decides which rows the two
     *   evaluations return, and the rows which only one of them returns cannot be paired with
     *   their aggregation.
     *
     * - a volatile column which only decorates the output is accepted, for example the column r of
     *   the outer query of
     *
     *       select t.k from (select e.k as k, random() as r from t1 e) t
     *       where exists (select count(*) from t2 i where i.k < t.k having count(*) = 0)
     *
     *       LogicalProject[k = e.k, r = random()] over LogicalOlapScan(t1)
     *
     *   because no predicate, no grouping and no correlation key reads r.
     *
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

    /**
     * Whether an expression of the plan is a function whose evaluation must not be duplicated.
     * The subquery of
     *
     *     select t1.c1 from t1 where exists (select count(*) from t2 where t2.c1 = t1.c1
     *         having count(*) = 0 and assert_true(t1.c2 > 0, 'positive'))
     *
     * is reported with the "Unsupported correlated subquery with grouping and/or aggregation"
     * error: the predicate of the HAVING clause which reads assert_true is pulled into the apply
     * (it references the outer row), and the rewrite would evaluate it once for every correlation
     * key instead of once for every outer row.
     */
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
            if (containsVolatileExpression(expression)) {
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

    /**
     * Whether one of the expressions is volatile, directly or through one of the given slots.
     */
    private static boolean usesVolatile(Collection<? extends Expression> expressions, Set<Slot> volatileSlots) {
        return expressions.stream().anyMatch(expression -> containsVolatileExpression(expression)
                || expression.getInputSlots().stream().anyMatch(volatileSlots::contains));
    }

    /**
     * Whether the expression contains a volatile expression, at any level of it.
     *
     * A UDF is reported as a volatile expression, because the planner cannot see the code of the
     * function (see Udf, which extends VolatileExpression), and a UDF does not restrict this
     * rewrite: a UDF which is declared volatile would otherwise forbid the rewrite of every
     * subquery which calls it. For example the subquery of
     *
     *     select t1.c1 from t1 where exists (select x.c2 from (select count(*) as c,
     *         my_udf(count(*)) as c2 from t2 where t2.c1 = t1.c1 having count(*) = 0) x
     *         where x.c2 < 0)
     *
     * reaches the check with the plan
     *
     *     Apply(exists)
     *       |-- t1
     *       +-- Filter(x.c2 < 0)
     *             +-- Project([count(*) as c, my_udf(count(*)) as c2])
     *                   +-- Filter(count(*) = 0)
     *                         +-- Aggregate(group by [], output [count(*) as c])
     *                               +-- Filter(t2.c1 = t1.c1)
     *                                     +-- t2
     *
     * whose filter above the HAVING clause reads the column of the UDF, and the subquery is
     * rewritten (the UDF is evaluated once for every correlation key).
     *
     * The other volatile expressions still restrict the rewrite (random, uuid and the others), for
     * example the subquery of
     *
     *     select t1.c1 from t1 where exists (select count(*) from t2 where t2.c1 = t1.c1
     *         and random() < 0.5 having count(*) = 0)
     */
    private static boolean containsVolatileExpression(Expression expression) {
        return expression.containsType(VolatileExpression.class)
                && expression.anyMatch(node -> node instanceof VolatileExpression
                        && !(node instanceof Udf)
                        && ((VolatileExpression) node).isVolatile());
    }

    /**
     * Every node of the subquery is evaluated once for every correlation key by this rewrite, while
     * the original subquery evaluates it once for every outer row: two outer rows with the same
     * correlation key share one evaluation, so no value which the result of the subquery depends on
     * may change between two evaluations.
     *
     * The volatile values of the subquery are followed through the slots of its plan (see
     * collectVolatileSlots): a volatile value which only decorates an output that nothing reads is
     * accepted, while a volatile value which decides the rows of a node, the grouping of the
     * aggregation or the value of a predicate which decides the result of the subquery is rejected.
     * The predicates which were pulled into the apply are not part of the plan of the subquery any
     * more, so they are checked against the volatile values of the aggregation they read as well.
     * For example the domain predicate ... where t2.c1 = t1.c1 and random() < 0.5 ..., the grouping
     * group by random() and a HAVING clause such as having random() > 0 are rejected (two outer rows
     * with the same correlation key would share one evaluation), while the value of sum(random()) is
     * accepted when nothing reads it, for example ... select count(*), sum(random()) as c2 from t2
     * where t2.c1 = t1.c1 having count(*) = 0.
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
     * Whether the outer plan returns rows which its two evaluations may choose differently.
     *
     * The rewrite evaluates the outer plan twice (the original plan on the left of the resulting
     * join, and a deep copy which computes the distinct correlation keys), and the rows of the two
     * evaluations have to carry the same correlation keys: the outer rows whose key the deep copy
     * did not produce find no row of the aggregation and are dropped by the semi/anti join.
     *
     * A sort alone does not change the rows a plan returns, only the operator which truncates the
     * sorted rows can keep different rows in the two evaluations. That truncation is deterministic
     * only when the order keys are a total order on the rows: when the limit falls inside a group
     * of rows which are equal on the order keys, the query semantics allows any subset of that
     * group to be returned, and the two evaluations are two instances of the same plan in
     * different places of the resulting plan, so they can keep rows with different correlation
     * keys. This rule does not prove that the order keys are total, so every topn and every limit
     * is rejected: the outer plan of
     *
     *     select t.k from (select e.k as k from t1 e limit 1) t
     *     where exists (select count(*) from t2 i where i.k < t.k having count(*) = 0)
     *
     * is the plan
     *
     *     LogicalLimit[limit 1] over LogicalProject[k = e.k] over LogicalOlapScan(t1)
     *
     * (the same plan with a LogicalTopN instead of the LogicalLimit when the query writes an order
     * by, for example select t.k from (select e.k as k from t1 e order by e.k limit 1) t where
     * exists (select count(*) from t2 i where i.k < t.k having count(*) = 0)), and the limit
     * without an order returns an arbitrary row: the row which one of the two evaluations keeps
     * can carry another correlation key than the row of the other one.
     *
     * A sampled scan is rejected as well, because its two evaluations sample different rows: the
     * scan of
     *
     *     select t.k from (select e.k as k from t1 e tablesample(1 rows)) t
     *     where exists (select count(*) from t2 i where i.k < t.k having count(*) = 0)
     *
     * is a LogicalOlapScan which carries the table sample.
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
     * Whether the right side of the apply reads a slot of the outer row outside the correlated
     * predicates of the subquery.
     *
     * The rewrite evaluates the right side of the apply once for every correlation key instead of
     * once for every outer row: the plan of the rewritten right side is the aggregation of a domain
     * of a key (see pullUpCorrelatedPredicateByAggregatingOuter), whose conditions are the
     * correlated predicates with their outer side replaced by that key. The correlated predicates
     * of the WHERE clause are therefore the only expressions of the right side which may read the
     * outer row, and the plan shapes below are reported as unsupported instead of producing a plan
     * whose aggregation cannot evaluate the value of the outer row:
     *
     * - a subquery of the subquery: the right side of the apply contains the nested apply, whose
     *   right side reads the rows of the subquery (and possibly the outer row) with its own
     *   correlation bookkeeping, which this rewrite does not update. For example the right side of
     *   the apply of the outer subquery of
     *
     *       select t1.c1 from t1 where exists (select count(*) from t2 where t2.c1 = t1.c1
     *           and exists (select count(*) from t3 where t3.c1 = t2.c2 having count(*) > 0))
     *
     *   is the plan
     *
     *       LogicalAggregate[count(*)] over LogicalFilter[t2.c1 = t1.c1] over
     *           LogicalFilter[mark slot] over LogicalProject[t2.c1, mark slot] over
     *               LogicalApply[EXITS_SUBQUERY, left = LogicalOlapScan(t2),
     *                   right = LogicalFilter[t3.c1 = t2.c2] over LogicalOlapScan(t3)]
     *
     *   while the nested apply has not been unnested (the other rules of the batch rewrite it by
     *   their own, see ExistsApplyToJoin).
     *
     * - a predicate which sits above the aggregation of the subquery and reads a slot of the outer
     *   row, when the rules which pull the predicates of the HAVING clause into the apply did not
     *   move it there (see UnCorrelatedApplyFilter and UnCorrelatedApplyProjectFilter). The filter
     *   of the derived table of
     *
     *       select t1.c1 from t1 where exists (select x.c from (select count(*) as c,
     *           count(*) + 1 as c2 from t2 where t2.c1 = t1.c1) x where x.c2 < t1.c1)
     *
     *   is an example of such a predicate, and the plan shape of the right side of the apply which
     *   this method reports is
     *
     *       LogicalFilter[c2 < t1.c1] over LogicalProject[c = count(*), c2 = count(*) + 1] over
     *           LogicalAggregate[count(*)] over LogicalFilter[t2.c1 = t1.c1] over LogicalOlapScan(t2)
     *
     *   (the projection computes the column which the filter reads, so filter pushdown cannot move
     *   the predicate below it either).
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
