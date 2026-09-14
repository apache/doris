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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

/**
 * The correlated aggregation of an EXISTS subquery can only be pushed into the group by of the
 * aggregate when the correlated predicate is an equality and the aggregate has a group by: then the
 * inner rows of one outer row are exactly one group of the aggregate.
 * Otherwise the aggregation of one outer row cannot be represented by a group of the inner side,
 * and the rule has to build the aggregation on the outer side:
 *
 * <pre>
 *   Apply(EXISTS, correlationSlot=[x])
 *     +-- L
 *     +-- Filter(having)                     // may hold for an empty input
 *          +-- Aggregate(group by [] or [r2], count(*))
 *               +-- Filter(correlated predicate(r1 &lt; x))
 *                    +-- R
 * </pre>
 */
class UnCorrelatedApplyAggregateFilterTest {

    @Test
    public void testNonEqualityCorrelatedPredicateWithHaving() {
        Plan rewritten = rewrite(true, false, new BigIntLiteral(2), LogicalApply.SubQueryType.EXITS_SUBQUERY);
        // the domain of an outer row is the union of several groups, so the aggregation has to be
        // computed for the correlation key of every outer row
        List<LogicalJoin> joins = rewritten.collectToList(LogicalJoin.class::isInstance);
        Assertions.assertTrue(rewritten.collectToList(LogicalApply.class::isInstance).isEmpty());
        Assertions.assertTrue(joins.stream().anyMatch(join -> join.getJoinType() == JoinType.LEFT_SEMI_JOIN),
                "the outer rows must be filtered by the aggregation result");
        Assertions.assertTrue(joins.stream().anyMatch(join -> join.getJoinType() == JoinType.INNER_JOIN),
                "the domain of an outer row has to be joined with its correlation key");
        Assertions.assertFalse(joins.stream().anyMatch(join -> join.getJoinType() == JoinType.LEFT_OUTER_JOIN),
                "a grouped aggregate returns no row for an empty correlated domain");
    }

    @Test
    public void testGlobalAggregateHavingWhichHoldsForEmptyInput() {
        Plan rewritten = rewrite(false, true, new BigIntLiteral(0), LogicalApply.SubQueryType.EXITS_SUBQUERY);
        // a global aggregate returns a row for the outer rows without any matching inner row, so the
        // aggregation has to keep such rows and count(*) must not count them as an input
        List<LogicalJoin> joins = rewritten.collectToList(LogicalJoin.class::isInstance);
        Assertions.assertTrue(rewritten.collectToList(LogicalApply.class::isInstance).isEmpty());
        Assertions.assertTrue(joins.stream().anyMatch(join -> join.getJoinType() == JoinType.LEFT_SEMI_JOIN),
                "the outer rows must be filtered by the aggregation result");
        Assertions.assertTrue(joins.stream().anyMatch(join -> join.getJoinType() == JoinType.LEFT_OUTER_JOIN),
                "the empty correlated domain has to be kept");
        Assertions.assertTrue(containsCompensatedCount(rewritten),
                "count(*) must not count the row which is kept for an empty correlated domain");
    }

    @Test
    public void testEqualityGroupedAggregateKeepsPlan() {
        // equality + group by: the domain of an outer row is exactly one group, keep the original rewrite
        Plan rewritten = rewrite(false, false, new BigIntLiteral(2), LogicalApply.SubQueryType.EXITS_SUBQUERY);
        Assertions.assertTrue(rewritten instanceof LogicalApply);
    }

    @Test
    public void testGlobalAggregateHavingWhichIsFalseForEmptyInputKeepsPlan() {
        // the row of an empty input is filtered out by the having clause, keep the original rewrite
        Plan rewritten = rewrite(false, true, new BigIntLiteral(2), LogicalApply.SubQueryType.EXITS_SUBQUERY);
        Assertions.assertTrue(rewritten instanceof LogicalApply);
    }

    @Test
    public void testNonExistsSubqueryTypeKeepsPlan() {
        // only EXISTS/NOT EXISTS are rewritten, other subquery types still need the aggregate output
        Plan rewritten = rewrite(true, false, new BigIntLiteral(2), LogicalApply.SubQueryType.SCALAR_SUBQUERY);
        Assertions.assertTrue(rewritten instanceof LogicalApply);
    }

    @Test
    public void testHavingPredicateWhichReferencesTheOuterQuery() {
        // a predicate of the HAVING clause which references the outer query was moved into the apply by
        // the rule which pulls the correlated predicates up, it decides on the aggregation of the whole
        // domain of an outer row, the empty domain included
        assertCorrelatedHavingEvaluatedOnAggregatedOuter(rewriteWithCorrelatedHaving(false, false));
    }

    @Test
    public void testHavingPredicateWhichReferencesTheOuterQueryUnderProjection() {
        // the analyzer can leave the projection of the select list of the subquery above the aggregate,
        // the subquery has to be rewritten through that projection
        assertCorrelatedHavingEvaluatedOnAggregatedOuter(rewriteWithCorrelatedHaving(true, false));
    }

    @Test
    public void testHavingPredicateWhichReferencesTheOuterQueryWithRemainingHavingPredicate() {
        // a predicate of the HAVING clause which does not reference the outer query stays below the
        // projection of the subquery, it has to be evaluated on the aggregation as well
        Plan rewritten = rewriteWithCorrelatedHaving(true, true);
        Plan right = assertCorrelatedHavingEvaluatedOnAggregatedOuter(rewritten);
        Assertions.assertEquals(2, ((LogicalFilter<?>) right).getConjuncts().size(),
                "both predicates of the HAVING clause have to be kept");
    }

    private static Plan assertCorrelatedHavingEvaluatedOnAggregatedOuter(Plan rewritten) {
        List<LogicalJoin> joins = rewritten.collectToList(LogicalJoin.class::isInstance);
        Assertions.assertTrue(rewritten.collectToList(LogicalApply.class::isInstance).isEmpty());
        Assertions.assertTrue(joins.stream().anyMatch(join -> join.getJoinType() == JoinType.LEFT_SEMI_JOIN),
                "the outer rows must be filtered by the aggregation result");
        Assertions.assertTrue(joins.stream().anyMatch(join -> join.getJoinType() == JoinType.LEFT_OUTER_JOIN),
                "the empty correlated domain has to be kept");
        Assertions.assertTrue(containsCompensatedCount(rewritten),
                "count(*) must not count the row which is kept for an empty correlated domain");
        Plan right = joins.stream()
                .filter(join -> join.getJoinType() == JoinType.LEFT_SEMI_JOIN)
                .findFirst().get().right();
        Assertions.assertTrue(right instanceof LogicalFilter,
                "the HAVING predicates have to be evaluated above the aggregate of the outer rows");
        LogicalFilter<?> having = (LogicalFilter<?>) right;
        Assertions.assertTrue(having.child() instanceof LogicalAggregate);
        Assertions.assertTrue(having.getConjuncts().stream()
                        .flatMap(conjunct -> conjunct.getInputSlots().stream())
                        .allMatch(slot -> having.child().getOutput().contains(slot)),
                "the HAVING predicates have to use the correlation key of the aggregated outer rows");
        return right;
    }

    private static boolean containsCompensatedCount(Plan plan) {
        List<LogicalAggregate> aggregates = plan.collectToList(LogicalAggregate.class::isInstance);
        for (LogicalAggregate<?> aggregate : aggregates) {
            for (NamedExpression output : aggregate.getOutputExpressions()) {
                for (Count count : ExpressionUtils.<Count>collectAll(ImmutableList.of(output),
                        Count.class::isInstance)) {
                    if (!count.isCountStar()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * build `L exists (select count(*) from R where r1 = x [having count(*) &gt; 0] having count(*) &lt;= x)`
     * after the rule which pulls the correlated predicates out of the filter under the apply
     * ({@link UnCorrelatedApplyFilter}) moved the predicate of the HAVING clause into the apply, and
     * apply the rule which matches that apply. The projection of the select list of the subquery and
     * the predicates of the HAVING clause which do not reference the outer query may stay below the
     * apply.
     */
    private Plan rewriteWithCorrelatedHaving(boolean withProjection, boolean withRemainingHavingPredicate) {
        LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "t1", 1);
        Slot x = left.getOutput().get(0); // t1.id
        LogicalOlapScan right = PlanConstructor.newLogicalOlapScan(1, "t2", 1);
        Slot r1 = right.getOutput().get(0); // t2.id

        LogicalFilter<LogicalOlapScan> where = new LogicalFilter<>(ImmutableSet.of(new EqualTo(r1, x)), right);
        Alias count = new Alias(new Count(), "count");
        LogicalAggregate<LogicalFilter<LogicalOlapScan>> agg =
                new LogicalAggregate<>(ImmutableList.of(), ImmutableList.of(count), where);
        Plan subquery = agg;
        if (withRemainingHavingPredicate) {
            subquery = new LogicalFilter<>(
                    ImmutableSet.of(new GreaterThan(count.toSlot(), new BigIntLiteral(0))), subquery);
        }
        if (withProjection) {
            subquery = new LogicalProject<>(ImmutableList.of(count.toSlot()), subquery);
        }
        Expression havingPredicate = new LessThanEqual(count.toSlot(), x);
        LogicalApply<LogicalOlapScan, Plan> apply =
                new LogicalApply<>(ImmutableList.of(x), LogicalApply.SubQueryType.EXITS_SUBQUERY, false,
                        Optional.empty(), Optional.empty(), Optional.of(havingPredicate), Optional.empty(),
                        false, false, left, subquery);

        ConnectContext connectContext = new ConnectContext();
        Rule rule = new UnCorrelatedApplyAggregateFilter().buildRules()
                .get(withProjection ? (withRemainingHavingPredicate ? 3 : 2) : 0);
        List<Plan> transformed = rule.transform(apply, MemoTestUtils.createCascadesContext(connectContext, apply));
        Assertions.assertEquals(1, transformed.size());
        return transformed.get(0);
    }

    /**
     * build `L exists (select count(*) from R where r1 OP x [group by r2] having count(*) = havingValue)`
     * and apply the rule which matches an apply on top of a filtered aggregate.
     */
    private Plan rewrite(boolean nonEqualityPredicate, boolean globalAggregate, Expression havingValue,
            LogicalApply.SubQueryType subQueryType) {
        LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "t1", 1);
        Slot x = left.getOutput().get(0); // t1.id
        LogicalOlapScan right = PlanConstructor.newLogicalOlapScan(1, "t2", 1);
        Slot r1 = right.getOutput().get(0); // t2.id
        Slot r2 = right.getOutput().get(1); // t2.name

        Expression predicate = nonEqualityPredicate ? new LessThan(r1, x) : new EqualTo(r1, x);
        LogicalFilter<LogicalOlapScan> where = new LogicalFilter<>(ImmutableSet.of(predicate), right);
        Alias count = new Alias(new Count(), "count");
        List<Expression> groupBy = globalAggregate ? ImmutableList.of() : ImmutableList.of(r2);
        List<NamedExpression> outputs = globalAggregate
                ? ImmutableList.of(count) : ImmutableList.of(r2, count);
        LogicalAggregate<LogicalFilter<LogicalOlapScan>> agg =
                new LogicalAggregate<>(groupBy, outputs, where);
        LogicalFilter<LogicalAggregate<LogicalFilter<LogicalOlapScan>>> having =
                new LogicalFilter<>(ImmutableSet.of(new EqualTo(count.toSlot(), havingValue)), agg);
        LogicalApply<LogicalOlapScan,
                LogicalFilter<LogicalAggregate<LogicalFilter<LogicalOlapScan>>>> apply =
                new LogicalApply<>(ImmutableList.of(x), subQueryType, false, Optional.empty(),
                        Optional.empty(), Optional.empty(), Optional.empty(), false, false, left, having);

        ConnectContext connectContext = new ConnectContext();
        Rule rule = new UnCorrelatedApplyAggregateFilter().buildRules().get(1);
        List<Plan> transformed = rule.transform(apply, MemoTestUtils.createCascadesContext(connectContext, apply));
        Assertions.assertEquals(1, transformed.size());
        return transformed.get(0);
    }
}
