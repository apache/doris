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
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.ArrayAgg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Random;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * The correlated aggregation of an EXISTS, IN or scalar subquery can only be pushed into the group
 * by of the aggregate when the correlated predicate is an equality and the aggregate has a group by:
 * then the inner rows of one outer row are exactly one group of the aggregate. Otherwise the
 * aggregation of one outer row cannot be represented by a group of the inner side, and the rule has
 * to build the aggregation on the outer side:
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
        Plan rewritten = rewrite(LessThan::new, false, new BigIntLiteral(2),
                LogicalApply.SubQueryType.EXITS_SUBQUERY);
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
        Plan rewritten = rewrite(EqualTo::new, true, new BigIntLiteral(0),
                LogicalApply.SubQueryType.EXITS_SUBQUERY);
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
        Plan rewritten = rewrite(EqualTo::new, false, new BigIntLiteral(2),
                LogicalApply.SubQueryType.EXITS_SUBQUERY);
        Assertions.assertTrue(rewritten instanceof LogicalApply);
    }

    @Test
    public void testGlobalAggregateHavingWhichIsFalseForEmptyInputKeepsPlan() {
        // the row of an empty input is filtered out by the having clause, keep the original rewrite
        Plan rewritten = rewrite(EqualTo::new, true, new BigIntLiteral(2),
                LogicalApply.SubQueryType.EXITS_SUBQUERY);
        Assertions.assertTrue(rewritten instanceof LogicalApply);
    }

    @Test
    public void testScalarSubqueryWithANonEqualityCorrelationIsAggregatedOnOuter() {
        // the left outer join which ScalarApplyToJoin builds pairs the outer row with the groups of
        // the inner side whose key is the value of the outer row, which the aggregation of the
        // correlated domain of the outer row is not as soon as the correlated predicate is not an
        // equality; the aggregation is built on the outer side instead, and the join keeps an outer
        // row which has no row in the aggregation (whose value is null)
        Plan rewritten = rewrite(LessThan::new, true, null, LogicalApply.SubQueryType.SCALAR_SUBQUERY);
        Assertions.assertTrue(rewritten.collectToList(LogicalApply.class::isInstance).isEmpty(),
                "the scalar apply is replaced by the join which pairs an outer row with its aggregation");
        LogicalJoin<?, ?> pairing = joinWhichPairsTheOuterRowsWithTheirKey(rewritten);
        Assertions.assertEquals(JoinType.LEFT_OUTER_JOIN, pairing.getJoinType(),
                "the outer rows whose aggregation has no row have to be kept");
        Assertions.assertTrue(containsCompensatedCount(rewritten),
                "count(*) must not count the row which is kept for an empty correlated domain");
    }

    @Test
    public void testScalarSubqueryWithANullSafeEqualityCorrelationIsAggregatedOnOuter() {
        // `r1 <=> x` is an equality whose domain contains the inner rows of the null key, which the
        // condition of the left outer join of ScalarApplyToJoin cannot express (it admits a plain
        // equality alone), so the aggregation of the domain is built on the outer side as well
        Plan rewritten = rewrite(NullSafeEqual::new, true, null, LogicalApply.SubQueryType.SCALAR_SUBQUERY);
        Assertions.assertTrue(rewritten.collectToList(LogicalApply.class::isInstance).isEmpty());
        Assertions.assertEquals(JoinType.LEFT_OUTER_JOIN,
                joinWhichPairsTheOuterRowsWithTheirKey(rewritten).getJoinType());
    }

    @Test
    public void testScalarSubqueryWithAnEqualityCorrelationKeepsPlan() {
        // the rows of the domain of an outer row are exactly one group of the inner side, so the left
        // outer join of ScalarApplyToJoin reproduces the aggregation of the domain of the outer row
        // and the aggregation stays on the inner side
        Plan rewritten = rewrite(EqualTo::new, true, null, LogicalApply.SubQueryType.SCALAR_SUBQUERY);
        Assertions.assertTrue(rewritten instanceof LogicalApply);
    }

    /**
     * the join which pairs every outer row with the aggregation of its own correlation key: the
     * rewrite returns it as the root of the rewritten plan, and its conditions are the null safe
     * equalities between the outer rows and the keys of the aggregation
     */
    private static LogicalJoin<?, ?> joinWhichPairsTheOuterRowsWithTheirKey(Plan plan) {
        Assertions.assertTrue(plan instanceof LogicalJoin,
                "the rewrite has to return the join which pairs an outer row with its aggregation");
        LogicalJoin<?, ?> pairing = (LogicalJoin<?, ?>) plan;
        Assertions.assertTrue(pairing.getOtherJoinConjuncts().stream()
                        .anyMatch(conjunct -> conjunct instanceof NullSafeEqual),
                "the join has to pair an outer row with the key of its own aggregation");
        return pairing;
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
        Assertions.assertEquals(2, havingConjunctsAboveAggregate(right).size(),
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
        Plan below = right;
        while (!(below instanceof LogicalAggregate)) {
            below = below.child(0);
        }
        LogicalAggregate<?> aggregate = (LogicalAggregate<?>) below;
        Assertions.assertTrue(havingConjunctsAboveAggregate(right).stream()
                        .flatMap(conjunct -> conjunct.getInputSlots().stream())
                        .allMatch(slot -> aggregate.getOutput().contains(slot)),
                "the HAVING predicates have to use the correlation key of the aggregated outer rows");
        assertJoinConditionsResolvable(rewritten);
        return right;
    }

    /** whether every input of a condition of a join of the plan is produced by one of its children */
    private static void assertJoinConditionsResolvable(Plan plan) {
        for (LogicalJoin<?, ?> join : plan.<LogicalJoin>collectToList(LogicalJoin.class::isInstance)) {
            List<Slot> available = new ArrayList<>(join.left().getOutput());
            available.addAll(join.right().getOutput());
            for (Expression condition : join.getOtherJoinConjuncts()) {
                Assertions.assertTrue(available.containsAll(condition.getInputSlots()),
                        "the condition " + condition + " has to be resolvable by the children of the join");
            }
        }
    }

    /** the predicates of the nodes which sit above the aggregate of the rewritten subquery */
    private static List<Expression> havingConjunctsAboveAggregate(Plan right) {
        List<Expression> conjuncts = new ArrayList<>();
        Plan below = right;
        while (!(below instanceof LogicalAggregate)) {
            if (below instanceof LogicalFilter) {
                conjuncts.addAll(((LogicalFilter<?>) below).getConjuncts());
            }
            below = below.child(0);
        }
        return conjuncts;
    }

    @Test
    public void testFilterAboveTheHavingClauseIsKept() {
        Plan rewritten = rewriteWithFilterAboveHaving(false);
        // the filter above the HAVING clause reads the projection of the select list, so it has to
        // survive the rewrite with that projection
        Assertions.assertTrue(rewritten.collectToList(LogicalApply.class::isInstance).isEmpty());
        List<LogicalJoin> joins = rewritten.collectToList(LogicalJoin.class::isInstance);
        Assertions.assertTrue(joins.stream().anyMatch(join -> join.getJoinType() == JoinType.LEFT_SEMI_JOIN));
        Plan right = joins.stream().filter(join -> join.getJoinType() == JoinType.LEFT_SEMI_JOIN)
                .findFirst().get().right();
        Assertions.assertTrue(havingConjunctsAboveAggregate(right).stream()
                        .flatMap(conjunct -> conjunct.getInputSlots().stream())
                        .anyMatch(slot -> "c2".equals(slot.getName())),
                "the filter above the HAVING clause has to be kept");
        Assertions.assertTrue(right.collectToList(LogicalProject.class::isInstance).stream()
                        .flatMap(plan -> ((LogicalProject<?>) plan).getProjects().stream())
                        .anyMatch(project -> "c2".equals(project.getName())),
                "the projection of the select list has to be kept");
        assertJoinConditionsResolvable(rewritten);
    }

    @Test
    public void testFilterOverAVolatileAliasAboveTheHavingClauseIsRejected() {
        // the filter above the HAVING clause reads a volatile column of the projection of the select
        // list: two outer rows with the same correlation key would share its evaluation
        Assertions.assertThrows(AnalysisException.class, () -> rewriteWithFilterAboveHaving(true));
    }

    @Test
    public void testHavingWhichRejectsTheNullOfSumKeepsThePlan() {
        Alias sum = new Alias(new Sum(new BigIntLiteral(1)), "s");
        // sum returns null for an empty input, so `sum(...) is not null` rejects the row of the
        // empty correlated domain: the original rewrite is still equivalent and has to be kept
        Plan rewritten = rewriteWithGlobalAggregate(sum, slot -> new Not(new IsNull(sum.toSlot())), null);
        Assertions.assertTrue(rewritten instanceof LogicalApply);
    }

    @Test
    public void testRejectingConjunctDominatesAnUnknownConjunctOfTheHaving() {
        Alias array = new Alias(new ArrayAgg(new BigIntLiteral(1)), "a");
        Alias count = new Alias(new Count(), "c");
        // the value of array_agg for an empty input is unknown for this rewrite, but the other
        // conjunct rejects the row of the empty input, so the original rewrite is still equivalent
        Plan rewritten = rewriteWithGlobalAggregate(ImmutableList.of(array, count),
                slot -> new And(new Not(new IsNull(array.toSlot())),
                        new EqualTo(count.toSlot(), new BigIntLiteral(999))), null, null);
        Assertions.assertTrue(rewritten instanceof LogicalApply);
    }

    /**
     * build `L exists (select x.c from (select count(*) c, c2 from R where r1 = x having count(*) = 0)
     * x where x.c2 &lt; 0)` where `c2` is a volatile expression or a deterministic one, and apply the
     * rule.
     */
    private Plan rewriteWithFilterAboveHaving(boolean overVolatileAlias) {
        LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "t1", 1);
        Slot x = left.getOutput().get(0); // t1.id
        LogicalOlapScan right = PlanConstructor.newLogicalOlapScan(1, "t2", 1);
        Slot r1 = right.getOutput().get(0); // t2.id

        LogicalFilter<LogicalOlapScan> where = new LogicalFilter<>(ImmutableSet.of(new EqualTo(r1, x)), right);
        Alias count = new Alias(new Count(), "c");
        LogicalAggregate<LogicalFilter<LogicalOlapScan>> agg =
                new LogicalAggregate<>(ImmutableList.of(), ImmutableList.of(count), where);
        LogicalFilter<LogicalAggregate<LogicalFilter<LogicalOlapScan>>> having = new LogicalFilter<>(
                ImmutableSet.of(new EqualTo(count.toSlot(), new BigIntLiteral(0))), agg);
        Alias projected = overVolatileAlias
                ? new Alias(new Random(), "c2")
                : new Alias(new Add(count.toSlot(), new BigIntLiteral(1)), "c2");
        Plan projection = new LogicalProject<>(ImmutableList.of(count.toSlot(), projected), having);
        Plan filterAboveHaving = new LogicalFilter<>(
                ImmutableSet.of(new LessThan(projected.toSlot(), new BigIntLiteral(0))), projection);
        LogicalApply<LogicalOlapScan, Plan> apply =
                new LogicalApply<>(ImmutableList.of(x), LogicalApply.SubQueryType.EXITS_SUBQUERY, false,
                        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), false, false,
                        left, filterAboveHaving);

        ConnectContext connectContext = new ConnectContext();
        Rule rule = new UnCorrelatedApplyAggregateFilter().buildRules().get(0);
        List<Plan> transformed = rule.transform(apply, MemoTestUtils.createCascadesContext(connectContext, apply));
        Assertions.assertEquals(1, transformed.size());
        return transformed.get(0);
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

    /** whether the argument of an aggregate of the given type is guarded by a condition */
    private static boolean containsGuardedAggregate(Plan plan, Class<? extends AggregateFunction> aggClass) {
        List<LogicalAggregate> aggregates = plan.collectToList(LogicalAggregate.class::isInstance);
        for (LogicalAggregate<?> aggregate : aggregates) {
            for (NamedExpression output : aggregate.getOutputExpressions()) {
                for (AggregateFunction function : ExpressionUtils.<AggregateFunction>collectAll(
                        ImmutableList.of(output), aggClass::isInstance)) {
                    if (function.arity() == 1 && function.child(0) instanceof If) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /** whether the plan aggregates a distinct count which keeps its argument guarded */
    private static boolean containsGuardedDistinctCount(Plan plan) {
        List<LogicalAggregate> aggregates = plan.collectToList(LogicalAggregate.class::isInstance);
        for (LogicalAggregate<?> aggregate : aggregates) {
            for (NamedExpression output : aggregate.getOutputExpressions()) {
                for (Count count : ExpressionUtils.<Count>collectAll(ImmutableList.of(output),
                        Count.class::isInstance)) {
                    if (count.isDistinct() && count.arity() == 1 && count.child(0) instanceof If) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /** whether a predicate which only references the outer row is evaluated on the aggregation */
    private static boolean hasHavingPredicateAboveAggregate(Plan plan) {
        List<LogicalFilter> filters = plan.collectToList(LogicalFilter.class::isInstance);
        for (LogicalFilter<?> filter : filters) {
            if (!(filter.child() instanceof LogicalAggregate)) {
                continue;
            }
            for (Expression conjunct : filter.getConjuncts()) {
                if (conjunct instanceof EqualTo && ((EqualTo) conjunct).right() instanceof BigIntLiteral) {
                    return true;
                }
            }
        }
        return false;
    }

    /** whether such a predicate was pushed into a condition of a join instead */
    private static boolean hasJoinConjunctWithLiteral(Plan plan) {
        List<LogicalJoin> joins = plan.collectToList(LogicalJoin.class::isInstance);
        for (LogicalJoin<?, ?> join : joins) {
            for (Expression conjunct : join.getOtherJoinConjuncts()) {
                if (conjunct instanceof EqualTo && ((EqualTo) conjunct).right() instanceof BigIntLiteral) {
                    return true;
                }
            }
        }
        return false;
    }

    @Test
    public void testSumOfTheEmptyCorrelatedDomainDoesNotSeeTheKeptRow() {
        Alias sum = new Alias(new Sum(new BigIntLiteral(1)), "s");
        Plan rewritten = rewriteWithGlobalAggregate(sum, slot -> new IsNull(sum.toSlot()), null);
        // the row which is kept for an empty correlated domain may not be an input of sum: an empty
        // input returns null, while the kept row would evaluate the argument of sum
        List<LogicalJoin> joins = rewritten.collectToList(LogicalJoin.class::isInstance);
        Assertions.assertTrue(joins.stream().anyMatch(join -> join.getJoinType() == JoinType.LEFT_OUTER_JOIN),
                "the empty correlated domain has to be kept");
        Assertions.assertTrue(containsGuardedAggregate(rewritten, Sum.class),
                "the argument of sum must be null for the row which is kept for an empty domain");
    }

    @Test
    public void testDistinctCountOfALiteralKeepsItsDistinctFlag() {
        Alias count = new Alias(new Count(true, new BigIntLiteral(1)), "c");
        Plan rewritten = rewriteWithGlobalAggregate(count, null,
                slot -> new EqualTo(count.toSlot(), slot));
        // two matching inner rows count as one value, so the distinct flag and the argument of the
        // count have to be kept: only the argument is guarded by the marker
        Assertions.assertTrue(containsGuardedDistinctCount(rewritten),
                "count(distinct 1) must keep its distinct flag and its guarded argument");
    }

    @Test
    public void testHavingPredicateWhichReferencesTheOuterRowOnly() {
        Alias count = new Alias(new Count(), "c");
        Plan rewritten = rewriteWithGlobalAggregate(count, null,
                slot -> new EqualTo(slot, new BigIntLiteral(1)));
        // the predicate decides whether the row of the aggregation survives, so it has to be
        // evaluated above the aggregation instead of filtering the domain of the outer row
        Assertions.assertTrue(hasHavingPredicateAboveAggregate(rewritten),
                "the predicate has to be evaluated on the aggregation");
        Assertions.assertFalse(hasJoinConjunctWithLiteral(rewritten),
                "the predicate may not become a condition of the join of the domain");
    }

    @Test
    public void testVolatileOutputWhichDoesNotFeedTheCorrelationIsAccepted() {
        LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "t1", 1);
        Slot x = left.getOutput().get(0); // t1.id
        Alias random = new Alias(new Random(), "r");
        LogicalProject<LogicalOlapScan> outer = new LogicalProject<>(ImmutableList.of(x, random), left);
        // the volatile column decorates the output of the outer query only: the subquery does not
        // reference it and it is not a correlation key, so duplicating it cannot change the result
        Plan rewritten = rewriteWithOuter(outer, ImmutableList.of(x));
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
    public void testVolatileCorrelationKeyIsRejected() {
        LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "t1", 1);
        Alias random = new Alias(new Random(), "k");
        LogicalProject<LogicalOlapScan> outer = new LogicalProject<>(ImmutableList.of(random), left);
        // the value of the correlation key changes between the two evaluations, so the key of the
        // copied plan would not match the value of the outer row
        Assertions.assertThrows(AnalysisException.class,
                () -> rewriteWithOuter(outer, ImmutableList.of(random.toSlot())));
    }

    @Test
    public void testVolatilePredicateOfTheOuterPlanIsRejected() {
        LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "t1", 1);
        Slot x = left.getOutput().get(0); // t1.id
        LogicalFilter<LogicalOlapScan> outer = new LogicalFilter<>(
                ImmutableSet.of(new GreaterThan(new Random(), new DoubleLiteral(0.5))), left);
        // the two evaluations of the outer plan would filter different rows, so the copied
        // correlation keys do not cover the outer rows
        Assertions.assertThrows(AnalysisException.class, () -> rewriteWithOuter(outer, ImmutableList.of(x)));
    }

    @Test
    public void testNoneMovableFunctionOfTheOuterPlanIsRejected() {
        LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "t1", 1);
        Slot x = left.getOutput().get(0); // t1.id
        Alias guard = new Alias(new AssertTrue(new GreaterThan(x, new BigIntLiteral(0)),
                new VarcharLiteral("the id has to be positive")), "guard");
        LogicalProject<LogicalOlapScan> outer = new LogicalProject<>(ImmutableList.of(x, guard), left);
        // assert_true is evaluated a second time in the copy of the outer plan, which could raise
        // its error for the rows the query of the user did not write it for
        Assertions.assertThrows(AnalysisException.class, () -> rewriteWithOuter(outer, ImmutableList.of(x)));
    }

    @Test
    public void testVolatileAggregateOutputWhichDoesNotFeedTheHavingIsAccepted() {
        Alias count = new Alias(new Count(), "c");
        Alias sum = new Alias(new Sum(new Random()), "s");
        Plan rewritten = rewriteWithGlobalAggregate(ImmutableList.of(count, sum),
                slot -> new EqualTo(count.toSlot(), new BigIntLiteral(0)), null, null);
        // the value of sum(random()) is not observed by the EXISTS (the HAVING clause only uses
        // count(*)), so the aggregation may compute the volatile argument once per correlation key
        Assertions.assertTrue(rewritten.collectToList(LogicalApply.class::isInstance).isEmpty());
        Assertions.assertTrue(containsCompensatedCount(rewritten));
        Assertions.assertTrue(containsGuardedAggregate(rewritten, Sum.class));
    }

    @Test
    public void testVolatileAggregateOutputWhichFeedsTheHavingIsRejected() {
        Alias count = new Alias(new Count(), "c");
        Alias sum = new Alias(new Sum(new Random()), "s");
        // the HAVING clause uses the aggregated value (`sum(<volatile>) is null` holds for the row of
        // an empty input, so the aggregation has to be built on the outer side), which would be
        // computed once for two outer rows with the same correlation key
        Assertions.assertThrows(AnalysisException.class, () -> rewriteWithGlobalAggregate(
                ImmutableList.of(count, sum), slot -> new IsNull(sum.toSlot()), null, null));
    }

    @Test
    public void testVolatilePredicateOfTheSubqueryDomainIsRejected() {
        Alias count = new Alias(new Count(), "c");
        // which inner rows belong to the domain of a correlation key cannot be shared by two outer
        // rows with the same key
        Assertions.assertThrows(AnalysisException.class, () -> rewriteWithGlobalAggregate(
                ImmutableList.of(count), slot -> new EqualTo(count.toSlot(), new BigIntLiteral(0)), null,
                new LessThan(new Random(), new DoubleLiteral(0.5))));
    }

    @Test
    public void testInSubqueryExposesTheCorrelationKeyThroughTheWrappers() {
        LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "t1", 1);
        Slot x = left.getOutput().get(0); // t1.id
        LogicalOlapScan right = PlanConstructor.newLogicalOlapScan(1, "t2", 1);
        Slot r1 = right.getOutput().get(0); // t2.id

        Alias count = new Alias(new Count(), "c");
        // select count(*) from t2 where t2.id = t1.id group by t2.id: the domain of one outer row
        // is one group of the aggregate, so the aggregation of the subquery stays on the inner side
        Plan subquery = new LogicalAggregate<>(ImmutableList.of(r1), ImmutableList.of(r1, count),
                new LogicalFilter<>(ImmutableSet.of(new EqualTo(r1, x)), right));
        // the projection of the select list of the IN subquery only exposes the value which the IN
        // compares, and it stays below the apply (see PullUpProjectUnderApply) so that this value
        // keeps its place in the output of the subquery
        subquery = new LogicalProject<>(ImmutableList.of(count.toSlot()), subquery);
        LogicalApply<LogicalOlapScan, Plan> apply = new LogicalApply<>(
                ImmutableList.of(x), LogicalApply.SubQueryType.IN_SUBQUERY, false,
                Optional.<Expression>of(x), Optional.empty(), Optional.empty(), Optional.empty(), false, false,
                left, subquery);

        ConnectContext connectContext = new ConnectContext();
        Rule rule = new UnCorrelatedApplyAggregateFilter().buildRules().get(0);
        List<Plan> transformed = rule.transform(apply, MemoTestUtils.createCascadesContext(connectContext, apply));
        Assertions.assertEquals(1, transformed.size());
        Plan rewritten = transformed.get(0);
        Assertions.assertTrue(rewritten instanceof LogicalApply,
                "the projection of the select list keeps the original rewrite of an IN subquery");
        LogicalApply<?, ?> newApply = (LogicalApply<?, ?>) rewritten;
        Assertions.assertTrue(newApply.getCorrelationFilter().isPresent());
        // the join which unnests the apply reads the inner side of the correlation predicate from the
        // output of the subquery: every slot which the correlation filter needs has to be part of the
        // output of the rewritten subquery, otherwise CheckAfterRewrite rejects the plan
        for (Expression conjunct : ExpressionUtils.extractConjunction(newApply.getCorrelationFilter().get())) {
            for (Slot slot : conjunct.getInputSlots()) {
                if (!newApply.getCorrelationSlot().contains(slot)) {
                    Assertions.assertTrue(newApply.right().getOutput().contains(slot),
                            "the correlation key of the IN subquery has to be exposed: " + conjunct);
                }
            }
        }
        // the value which the IN compares is the first column of the subquery, so the key which the
        // rewrite appends to the output of the subquery may not take its place
        Assertions.assertEquals(count.toSlot(), newApply.right().getOutput().get(0).toSlot());
    }

    @Test
    public void testInSubqueryWithAVolatileHavingIsRejected() {
        LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "t1", 1);
        Slot x = left.getOutput().get(0); // t1.id
        LogicalOlapScan right = PlanConstructor.newLogicalOlapScan(1, "t2", 1);
        Slot r1 = right.getOutput().get(0); // t2.id

        Alias count = new Alias(new Count(), "c");
        Plan subquery = new LogicalAggregate<>(ImmutableList.of(), ImmutableList.of(count),
                new LogicalFilter<>(ImmutableSet.of(new EqualTo(r1, x)), right));
        // `select c from (select count(*) as c, random() as r ...) x where x.r < 0.5`: the predicate
        // over the volatile column of the derived table decides whether the row of an empty
        // correlated domain is kept, which the groups of the inner side cannot reproduce, and the
        // aggregation of one outer row cannot be built on the outer side either while the predicate
        // is volatile
        Alias random = new Alias(new Random(), "r");
        subquery = new LogicalProject<>(ImmutableList.of(count.toSlot(), random), subquery);
        subquery = new LogicalFilter<>(ImmutableSet.of(new LessThan(random.toSlot(), new DoubleLiteral(0.5))),
                subquery);
        subquery = new LogicalProject<>(ImmutableList.of(count.toSlot()), subquery);
        LogicalApply<LogicalOlapScan, Plan> apply = new LogicalApply<>(
                ImmutableList.of(x), LogicalApply.SubQueryType.IN_SUBQUERY, false,
                Optional.<Expression>of(x), Optional.empty(), Optional.empty(), Optional.empty(), false, false,
                left, subquery);

        ConnectContext connectContext = new ConnectContext();
        Rule rule = new UnCorrelatedApplyAggregateFilter().buildRules().get(0);
        Assertions.assertThrows(AnalysisException.class,
                () -> rule.transform(apply, MemoTestUtils.createCascadesContext(connectContext, apply)));
    }

    /**
     * build `outer exists (select count(*) from R where r1 = x having count(*) = 0)` where the
     * aggregation of the subquery has to be computed on the outer side (the HAVING clause holds for
     * an empty input), and apply the rule.
     */
    private Plan rewriteWithOuter(Plan outer, List<Slot> correlationSlots) {
        LogicalOlapScan right = PlanConstructor.newLogicalOlapScan(1, "t2", 1);
        Slot r1 = right.getOutput().get(0); // t2.id
        LogicalFilter<LogicalOlapScan> where = new LogicalFilter<>(
                ImmutableSet.of(new EqualTo(r1, correlationSlots.get(0))), right);
        Alias count = new Alias(new Count(), "c");
        LogicalAggregate<LogicalFilter<LogicalOlapScan>> agg =
                new LogicalAggregate<>(ImmutableList.of(), ImmutableList.of(count), where);
        LogicalFilter<LogicalAggregate<LogicalFilter<LogicalOlapScan>>> having = new LogicalFilter<>(
                ImmutableSet.of(new EqualTo(count.toSlot(), new BigIntLiteral(0))), agg);
        LogicalApply<Plan, LogicalFilter<LogicalAggregate<LogicalFilter<LogicalOlapScan>>>> apply =
                new LogicalApply<>(correlationSlots, LogicalApply.SubQueryType.EXITS_SUBQUERY, false,
                        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), false, false,
                        outer, having);

        ConnectContext connectContext = new ConnectContext();
        Rule rule = new UnCorrelatedApplyAggregateFilter().buildRules().get(0);
        List<Plan> transformed = rule.transform(apply, MemoTestUtils.createCascadesContext(connectContext, apply));
        Assertions.assertEquals(1, transformed.size());
        return transformed.get(0);
    }

    /**
     * build `L exists (select &lt;outputs&gt; from R where r1 = x [and &lt;extraWherePredicate&gt;]
     * [having &lt;havingOnAggregate&gt;])` where a predicate of the HAVING clause which was pulled into
     * the apply may be provided as well, and apply the rule.
     */
    private Plan rewriteWithGlobalAggregate(List<NamedExpression> outputs,
            Function<Slot, Expression> havingOnAggregate, Function<Slot, Expression> pulledPredicate,
            Expression extraWherePredicate) {
        LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "t1", 1);
        Slot x = left.getOutput().get(0); // t1.id
        LogicalOlapScan right = PlanConstructor.newLogicalOlapScan(1, "t2", 1);
        Slot r1 = right.getOutput().get(0); // t2.id

        ImmutableSet.Builder<Expression> whereConjuncts = ImmutableSet.builder();
        whereConjuncts.add(new EqualTo(r1, x));
        if (extraWherePredicate != null) {
            whereConjuncts.add(extraWherePredicate);
        }
        LogicalFilter<LogicalOlapScan> where = new LogicalFilter<>(whereConjuncts.build(), right);
        LogicalAggregate<LogicalFilter<LogicalOlapScan>> agg =
                new LogicalAggregate<>(ImmutableList.of(), outputs, where);
        Plan subquery = agg;
        if (havingOnAggregate != null) {
            subquery = new LogicalFilter<>(ImmutableSet.of(havingOnAggregate.apply(x)), subquery);
        }
        LogicalApply<LogicalOlapScan, Plan> apply =
                new LogicalApply<>(ImmutableList.of(x), LogicalApply.SubQueryType.EXITS_SUBQUERY, false,
                        Optional.empty(), Optional.empty(),
                        pulledPredicate == null ? Optional.empty() : Optional.of(pulledPredicate.apply(x)),
                        Optional.empty(), false, false, left, subquery);

        ConnectContext connectContext = new ConnectContext();
        Rule rule = new UnCorrelatedApplyAggregateFilter().buildRules().get(0);
        List<Plan> transformed = rule.transform(apply, MemoTestUtils.createCascadesContext(connectContext, apply));
        Assertions.assertEquals(1, transformed.size());
        return transformed.get(0);
    }

    private Plan rewriteWithGlobalAggregate(NamedExpression output,
            Function<Slot, Expression> havingOnAggregate, Function<Slot, Expression> pulledPredicate) {
        return rewriteWithGlobalAggregate(ImmutableList.of(output), havingOnAggregate, pulledPredicate, null);
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
        Rule rule = new UnCorrelatedApplyAggregateFilter().buildRules().get(0);
        List<Plan> transformed = rule.transform(apply, MemoTestUtils.createCascadesContext(connectContext, apply));
        Assertions.assertEquals(1, transformed.size());
        return transformed.get(0);
    }

    /**
     * build `L &lt;subqueryType&gt; (select count(*) [group by r2] from R where &lt;predicate&gt;(r1, x)
     * [having count(*) = &lt;havingValue&gt;])` and apply the rule which matches an apply on top of a
     * filtered aggregate. A null {@code havingValue} builds the subquery without a HAVING clause.
     */
    private static Plan rewrite(BiFunction<Slot, Slot, Expression> predicate, boolean globalAggregate,
            Expression havingValue, LogicalApply.SubQueryType subQueryType) {
        LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "t1", 1);
        Slot x = left.getOutput().get(0); // t1.id
        LogicalOlapScan right = PlanConstructor.newLogicalOlapScan(1, "t2", 1);
        Slot r1 = right.getOutput().get(0); // t2.id
        Slot r2 = right.getOutput().get(1); // t2.name

        LogicalFilter<LogicalOlapScan> where = new LogicalFilter<>(
                ImmutableSet.of(predicate.apply(r1, x)), right);
        Alias count = new Alias(new Count(), "count");
        List<Expression> groupBy = globalAggregate ? ImmutableList.of() : ImmutableList.of(r2);
        List<NamedExpression> outputs = globalAggregate
                ? ImmutableList.of(count) : ImmutableList.of(r2, count);
        Plan subquery = new LogicalAggregate<>(groupBy, outputs, where);
        if (havingValue != null) {
            subquery = new LogicalFilter<>(ImmutableSet.of(new EqualTo(count.toSlot(), havingValue)), subquery);
        }
        // a scalar subquery of a query reads its value in the outer scope, the other subquery types do
        // not expose an output column
        boolean outputUsedInOuterScope = subQueryType == LogicalApply.SubQueryType.SCALAR_SUBQUERY;
        LogicalApply<LogicalOlapScan, Plan> apply =
                new LogicalApply<>(ImmutableList.of(x), subQueryType, false, Optional.empty(),
                        Optional.empty(), Optional.empty(), Optional.empty(), outputUsedInOuterScope, false,
                        left, subquery);

        ConnectContext connectContext = new ConnectContext();
        Rule rule = new UnCorrelatedApplyAggregateFilter().buildRules().get(0);
        List<Plan> transformed = rule.transform(apply, MemoTestUtils.createCascadesContext(connectContext, apply));
        Assertions.assertEquals(1, transformed.size());
        return transformed.get(0);
    }
}
