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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.executable.TimeRoundSeries;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DayCeil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DayFloor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Left;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthCeil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.QuarterCeil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Substring;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ToDate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.WeekFloor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Year;
import org.apache.doris.nereids.trees.expressions.functions.scalar.YearCeil;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimestampTzLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

class InferPredicateFromMonotonicFunctionTest extends ExpressionRewriteTestHelper {
    private final SlotReference stringSlot = new SlotReference("s", StringType.INSTANCE, true);
    private final SlotReference dateTimeSlot = new SlotReference(
            "dt", DateTimeV2Type.SYSTEM_DEFAULT, true);
    private final SlotReference nonPartitionDateTimeSlot = new SlotReference(
            "non_partition_dt", DateTimeV2Type.SYSTEM_DEFAULT, true);

    @Test
    void inferOnlyAfterExtractingPartitionPredicate() {
        EqualTo partitionPredicate = new EqualTo(new Year(dateTimeSlot), new IntegerLiteral(2026));
        EqualTo nonPartitionPredicate = new EqualTo(
                new Year(nonPartitionDateTimeSlot), new IntegerLiteral(2026));
        Expression filter = typeCoercion(ExpressionUtils.and(
                ImmutableList.of(partitionPredicate, nonPartitionPredicate)));

        Expression extracted = PartitionPruneExpressionExtractor.extract(
                filter, ImmutableSet.of(dateTimeSlot), cascadesContext);
        Expression inferred = InferPredicateFromMonotonicFunction.inferForPartitionPrune(extracted);

        assertInferredBound(inferred, GreaterThanEqual.class, dateTimeSlot);
        assertInferredBound(inferred, LessThan.class, dateTimeSlot);
        Assertions.assertTrue(inferred.getInputSlots().contains(dateTimeSlot));
        Assertions.assertFalse(inferred.getInputSlots().contains(nonPartitionDateTimeSlot));
    }

    @Test
    void inferPrefixLowerBoundFromSqlSyntax() {
        Map<String, Slot> slots = Maps.newHashMap();
        Expression original = typeCoercion(replaceUnboundSlot(PARSER.parseExpression(
                "substring(VS, 1, 4) >= 'a-value-longer-than-four'"), slots));

        Expression rewritten = InferPredicateFromMonotonicFunction.inferForPartitionPrune(original);

        assertInferredBound(rewritten, GreaterThanEqual.class, slots.get("VS"));
        assertOriginalPreserved(rewritten, original);
    }

    @Test
    void inferStrictPrefixLowerBound() {
        GreaterThan original = new GreaterThan(
                new Left(stringSlot, new IntegerLiteral(4)), new VarcharLiteral("abcd"));

        Expression rewritten = rewrite(original);

        assertInferredBound(rewritten, GreaterThan.class, stringSlot);
    }

    @Test
    void inferPrefixEqualityAsLowerBound() {
        EqualTo original = new EqualTo(substring(stringSlot, 1, 4), new VarcharLiteral("abcd"));

        Expression rewritten = rewrite(original);

        assertInferredBound(rewritten, GreaterThanEqual.class, stringSlot);
    }

    @Test
    void doNotInferNonPrefixOrUpperBound() {
        LessThan upperBound = new LessThan(
                substring(stringSlot, 1, 4), new VarcharLiteral("abcd"));
        GreaterThanEqual nonPrefix = new GreaterThanEqual(
                substring(stringSlot, 2, 4), new VarcharLiteral("abcd"));

        Assertions.assertEquals(typeCoercion(upperBound), rewrite(upperBound));
        Assertions.assertEquals(typeCoercion(nonPrefix), rewrite(nonPrefix));
    }

    @Test
    void inferDateTruncAndToDateLowerBounds() {
        GreaterThanEqual dateTrunc = new GreaterThanEqual(
                new DateTrunc(dateTimeSlot, new VarcharLiteral("month")),
                new DateTimeV2Literal("2026-07-01 00:00:00"));
        EqualTo toDate = new EqualTo(new ToDate(dateTimeSlot), new VarcharLiteral("2026-07-28"));

        assertInferredBound(rewrite(dateTrunc), GreaterThanEqual.class, dateTimeSlot);
        assertInferredBound(rewrite(toDate), GreaterThanEqual.class, dateTimeSlot);
    }

    @Test
    void inferFloorAndCeilBounds() {
        GreaterThan floor = new GreaterThan(
                new DayFloor(dateTimeSlot), new DateTimeV2Literal("2026-07-28 00:00:00"));
        GreaterThanEqual floorWithOrigin = new GreaterThanEqual(
                new DayFloor(dateTimeSlot, new IntegerLiteral(3),
                        new DateTimeV2Literal("2020-01-01 00:00:00")),
                new DateTimeV2Literal("2026-07-28 00:00:00"));
        LessThanEqual ceil = new LessThanEqual(
                new DayCeil(dateTimeSlot), new DateTimeV2Literal("2026-07-28 00:00:00"));
        EqualTo weekFloor = new EqualTo(
                new WeekFloor(dateTimeSlot), new DateTimeV2Literal("2026-07-27 00:00:00"));

        assertInferredBound(rewrite(floor), GreaterThan.class, dateTimeSlot);
        assertInferredBound(rewrite(floorWithOrigin), GreaterThanEqual.class, dateTimeSlot);
        assertInferredBound(rewrite(ceil), LessThanEqual.class, dateTimeSlot);
        assertInferredBound(rewrite(weekFloor), GreaterThanEqual.class, dateTimeSlot);
    }

    @Test
    void inferDateCeilBoundFromSqlSyntax() {
        Map<String, Slot> slots = Maps.newHashMap();
        Expression predicate = typeCoercion(replaceUnboundSlot(PARSER.parseExpression(
                "date_ceil(AA, interval 1 day) <= '2026-07-28 00:00:00'"), slots));
        Expression dateCeil = predicate.child(0);

        Assertions.assertInstanceOf(DayCeil.class, dateCeil);
        Assertions.assertEquals(3, dateCeil.arity());
        assertInferredBound(InferPredicateFromMonotonicFunction.inferForPartitionPrune(predicate),
                LessThanEqual.class, slots.get("AA"));
    }

    @Test
    void inferCalendarCeilWithDefaultOriginFromSqlSyntax() {
        Map<String, Slot> slots = Maps.newHashMap();
        Expression predicate = typeCoercion(replaceUnboundSlot(PARSER.parseExpression(
                "date_ceil(AA, interval 1 month) <= '2026-07-28 00:00:00'"), slots));
        Expression dateCeil = predicate.child(0);

        Assertions.assertInstanceOf(MonthCeil.class, dateCeil);
        Assertions.assertEquals(3, dateCeil.arity());
        assertInferredBound(InferPredicateFromMonotonicFunction.inferForPartitionPrune(predicate),
                LessThanEqual.class, slots.get("AA"));
    }

    @Test
    void inferDateV2BoundsFromSqlSyntax() {
        Map<String, Slot> slots = Maps.newHashMap();
        Expression ceilPredicate = typeCoercion(replaceUnboundSlot(PARSER.parseExpression(
                "date_ceil(CC, interval 1 day) < '2026-07-28 12:00:00'"), slots));
        Expression floorPredicate = typeCoercion(replaceUnboundSlot(PARSER.parseExpression(
                "date_floor(CC, interval 1 day) >= '2026-07-28 12:00:00'"), slots));

        Assertions.assertInstanceOf(Cast.class, ceilPredicate.child(0).child(0));
        Assertions.assertInstanceOf(Cast.class, floorPredicate.child(0).child(0));

        ComparisonPredicate ceilBound = findInferredBound(
                InferPredicateFromMonotonicFunction.inferForPartitionPrune(ceilPredicate),
                LessThan.class, slots.get("CC"));
        ComparisonPredicate floorBound = findInferredBound(
                InferPredicateFromMonotonicFunction.inferForPartitionPrune(floorPredicate),
                GreaterThanEqual.class, slots.get("CC"));
        DateV2Literal nextDay = new DateV2Literal("2026-07-29");
        Assertions.assertEquals(nextDay, ceilBound.right());
        Assertions.assertEquals(nextDay, floorBound.right());
    }

    @Test
    void inferWithCustomOriginButOnlyPositiveLiteralPeriod() {
        GreaterThanEqual zeroPeriod = new GreaterThanEqual(
                new DayFloor(dateTimeSlot, new IntegerLiteral(0)),
                new DateTimeV2Literal("2026-07-28 00:00:00"));
        LessThanEqual zeroPeriodWithOrigin = new LessThanEqual(
                new DayCeil(dateTimeSlot, new IntegerLiteral(0),
                        DateTimeV2Literal.USE_IN_FLOOR_CEIL),
                new DateTimeV2Literal("2026-07-28 00:00:00"));
        SlotReference periodSlot = new SlotReference("period", IntegerType.INSTANCE, false);
        LessThanEqual dynamicPeriod = new LessThanEqual(
                new DayCeil(dateTimeSlot, periodSlot, DateTimeV2Literal.USE_IN_FLOOR_CEIL),
                new DateTimeV2Literal("2026-07-28 00:00:00"));
        LessThanEqual withOrigin = new LessThanEqual(
                new DayCeil(dateTimeSlot, new DateTimeV2Literal("2020-01-01 00:00:00")),
                new DateTimeV2Literal("2026-07-28 00:00:00"));

        Assertions.assertEquals(typeCoercion(zeroPeriod), rewrite(zeroPeriod));
        Assertions.assertEquals(typeCoercion(zeroPeriodWithOrigin), rewrite(zeroPeriodWithOrigin));
        Assertions.assertEquals(typeCoercion(dynamicPeriod), rewrite(dynamicPeriod));
        assertInferredBound(rewrite(withOrigin), LessThanEqual.class, dateTimeSlot);
    }

    @Test
    void doNotInferCalendarCeilWithCustomOrigin() {
        DateTimeV2Literal boundary = new DateTimeV2Literal("2021-02-28 00:00:00");
        DateTimeV2Literal monthEndOrigin = new DateTimeV2Literal("2021-01-31 00:00:00");
        Assertions.assertEquals(boundary, TimeRoundSeries.monthCeil(
                new DateTimeV2Literal("2021-02-28 12:00:00"), new IntegerLiteral(1), monthEndOrigin));

        List<Expression> predicates = ImmutableList.of(
                new LessThanEqual(new MonthCeil(dateTimeSlot, monthEndOrigin), boundary),
                new LessThanEqual(new MonthCeil(dateTimeSlot, new IntegerLiteral(1), monthEndOrigin), boundary),
                new LessThanEqual(new QuarterCeil(dateTimeSlot, new IntegerLiteral(1),
                        new DateTimeV2Literal("2020-11-30 00:00:00")), boundary),
                new LessThanEqual(new YearCeil(dateTimeSlot, new IntegerLiteral(1),
                        new DateTimeV2Literal("2020-02-29 00:00:00")), boundary));

        for (Expression predicate : predicates) {
            Assertions.assertEquals(typeCoercion(predicate), rewrite(predicate));
        }
    }

    @Test
    void doNotInferTimestampTzRoundingBound() {
        SlotReference timestampTzSlot = new SlotReference(
                "ts", TimeStampTzType.SYSTEM_DEFAULT, true);
        LessThanEqual predicate = new LessThanEqual(
                new DayCeil(timestampTzSlot), new TimestampTzLiteral("2026-07-28 00:00:00+00:00"));

        Assertions.assertEquals(typeCoercion(predicate), rewrite(predicate));
    }

    @Test
    void inferExactYearRange() {
        EqualTo original = new EqualTo(new Year(dateTimeSlot), new IntegerLiteral(2026));

        Expression rewritten = rewrite(original);

        assertInferredBound(rewritten, GreaterThanEqual.class, dateTimeSlot);
        assertInferredBound(rewritten, LessThan.class, dateTimeSlot);
        Assertions.assertEquals(3, ExpressionUtils.extractConjunction(rewritten).size());
    }

    @Test
    void inferAllYearComparisonDirections() {
        assertInferredBound(rewrite(new GreaterThan(new Year(dateTimeSlot), new IntegerLiteral(2026))),
                GreaterThanEqual.class, dateTimeSlot);
        assertInferredBound(rewrite(new GreaterThanEqual(new Year(dateTimeSlot), new IntegerLiteral(2026))),
                GreaterThanEqual.class, dateTimeSlot);
        assertInferredBound(rewrite(new LessThan(new Year(dateTimeSlot), new IntegerLiteral(2026))),
                LessThan.class, dateTimeSlot);
        assertInferredBound(rewrite(new LessThanEqual(new Year(dateTimeSlot), new IntegerLiteral(2026))),
                LessThan.class, dateTimeSlot);
    }

    @Test
    void inferYearAtDateDomainBounds() {
        EqualTo yearZero = new EqualTo(new Year(dateTimeSlot), new IntegerLiteral(0));
        Expression rewrittenYearZero = rewrite(yearZero);
        assertInferredBound(rewrittenYearZero, GreaterThanEqual.class, dateTimeSlot);
        assertInferredBound(rewrittenYearZero, LessThan.class, dateTimeSlot);

        EqualTo year9999 = new EqualTo(new Year(dateTimeSlot), new IntegerLiteral(9999));
        Expression rewrittenYear9999 = rewrite(year9999);
        Expression expectedLowerBound = typeCoercion(
                new GreaterThanEqual(dateTimeSlot, new DateV2Literal(9999, 1, 1))).withInferred(true);
        Assertions.assertTrue(ExpressionUtils.extractConjunction(rewrittenYear9999).contains(expectedLowerBound));
        Assertions.assertEquals(2, ExpressionUtils.extractConjunction(rewrittenYear9999).size());

        GreaterThan afterLastYear = new GreaterThan(new Year(dateTimeSlot), new IntegerLiteral(9999));
        LessThanEqual throughLastYear = new LessThanEqual(new Year(dateTimeSlot), new IntegerLiteral(9999));
        EqualTo outsideDateDomain = new EqualTo(new Year(dateTimeSlot), new IntegerLiteral(10000));
        Assertions.assertEquals(typeCoercion(afterLastYear), rewrite(afterLastYear));
        Assertions.assertEquals(typeCoercion(throughLastYear), rewrite(throughLastYear));
        Assertions.assertEquals(typeCoercion(outsideDateDomain), rewrite(outsideDateDomain));
    }

    @Test
    void doNotInferThroughNarrowingYearCast() {
        EqualTo predicate = new EqualTo(
                new Cast(new Year(dateTimeSlot), TinyIntType.INSTANCE), new IntegerLiteral(1));

        Assertions.assertEquals(typeCoercion(predicate), rewrite(predicate));
    }

    @Test
    void doNotTruncateLargeYearLiteral() {
        LessThan predicate = new LessThan(new Year(dateTimeSlot),
                new LargeIntLiteral(new BigInteger("18446744073709553642")));

        Assertions.assertEquals(typeCoercion(predicate), rewrite(predicate));
    }

    @Test
    void preserveOriginalConjuncts() {
        GreaterThanEqual predicate = new GreaterThanEqual(
                dateTimeSlot, new DateTimeV2Literal("2026-07-28 00:00:00"));
        Expression duplicateConjunction = new And(ImmutableList.of(predicate, predicate));

        Expression rewritten = InferPredicateFromMonotonicFunction
                .inferForPartitionPrune(duplicateConjunction);

        Assertions.assertInstanceOf(And.class, rewritten);
        Assertions.assertEquals(2, rewritten.children().size());
    }

    @Test
    void preserveOriginalOrderAndAppendOnceInsideConjunction() {
        GreaterThanEqual prefix = new GreaterThanEqual(
                substring(stringSlot, 1, 4), new VarcharLiteral("abcd"));
        GreaterThan dateFloor = new GreaterThan(
                new DayFloor(dateTimeSlot), new DateTimeV2Literal("2026-07-28 00:00:00"));
        Expression original = ExpressionUtils.and(ImmutableList.of(prefix, dateFloor));

        Expression analyzedOriginal = typeCoercion(original);
        Expression once = InferPredicateFromMonotonicFunction.inferForPartitionPrune(analyzedOriginal);
        Expression twice = InferPredicateFromMonotonicFunction.inferForPartitionPrune(once);
        List<Expression> originalConjuncts = ExpressionUtils.extractConjunction(analyzedOriginal);
        List<Expression> rewrittenConjuncts = ExpressionUtils.extractConjunction(once);

        Assertions.assertEquals(once, twice);
        Assertions.assertEquals(originalConjuncts,
                rewrittenConjuncts.subList(0, originalConjuncts.size()));
        Assertions.assertEquals(4, rewrittenConjuncts.size());
    }

    @Test
    void doNotAppendBoundAlreadyPresent() {
        DateTimeV2Literal boundary = new DateTimeV2Literal("2026-07-28 00:00:00");
        GreaterThanEqual dateFloor = new GreaterThanEqual(new DayFloor(dateTimeSlot), boundary);
        GreaterThanEqual existingBound = new GreaterThanEqual(dateTimeSlot, boundary);
        Expression original = typeCoercion(ExpressionUtils.and(ImmutableList.of(dateFloor, existingBound)));

        Expression rewritten = InferPredicateFromMonotonicFunction.inferForPartitionPrune(original);

        Assertions.assertEquals(ExpressionUtils.extractConjunction(original),
                ExpressionUtils.extractConjunction(rewritten));
    }

    @Test
    void inferEachDisjunctionBranch() {
        EqualTo year2025 = new EqualTo(new Year(dateTimeSlot), new IntegerLiteral(2025));
        EqualTo year2026 = new EqualTo(new Year(dateTimeSlot), new IntegerLiteral(2026));

        Expression rewritten = rewrite(new Or(year2025, year2026));
        List<Expression> disjunctions = ExpressionUtils.extractDisjunction(rewritten);

        Assertions.assertEquals(2, disjunctions.size());
        for (Expression disjunction : disjunctions) {
            assertInferredBound(disjunction, GreaterThanEqual.class, dateTimeSlot);
            assertInferredBound(disjunction, LessThan.class, dateTimeSlot);
            Assertions.assertEquals(3, ExpressionUtils.extractConjunction(disjunction).size());
        }
        Assertions.assertEquals(rewritten,
                InferPredicateFromMonotonicFunction.inferForPartitionPrune(rewritten));
    }

    @Test
    void inferNestedConjunctionAndDisjunction() {
        EqualTo year2024 = new EqualTo(new Year(dateTimeSlot), new IntegerLiteral(2024));
        EqualTo year2025 = new EqualTo(new Year(dateTimeSlot), new IntegerLiteral(2025));
        EqualTo year2026 = new EqualTo(new Year(dateTimeSlot), new IntegerLiteral(2026));
        GreaterThan dayFloor = new GreaterThan(
                new DayFloor(dateTimeSlot), new DateTimeV2Literal("2026-07-28 00:00:00"));
        Expression original = ExpressionUtils.and(ImmutableList.of(
                year2024,
                new Or(year2025, ExpressionUtils.and(ImmutableList.of(year2026, dayFloor)))));

        Expression rewritten = rewrite(original);
        Or rewrittenOr = (Or) ExpressionUtils.extractConjunction(rewritten).stream()
                .filter(Or.class::isInstance)
                .findFirst()
                .orElseThrow(AssertionError::new);
        List<Expression> disjunctions = ExpressionUtils.extractDisjunction(rewrittenOr);

        Assertions.assertEquals(2, disjunctions.size());
        assertInferredBound(rewritten, GreaterThanEqual.class, dateTimeSlot);
        assertInferredBound(disjunctions.get(0), GreaterThanEqual.class, dateTimeSlot);
        assertInferredBound(disjunctions.get(0), LessThan.class, dateTimeSlot);
        assertInferredBound(disjunctions.get(1), GreaterThan.class, dateTimeSlot);
        assertInferredBound(disjunctions.get(1), LessThan.class, dateTimeSlot);
    }

    private Substring substring(Expression source, int position, int length) {
        return new Substring(source, new IntegerLiteral(position), new IntegerLiteral(length));
    }

    private Expression rewrite(Expression expression) {
        return InferPredicateFromMonotonicFunction.inferForPartitionPrune(typeCoercion(expression));
    }

    private void assertOriginalPreserved(Expression rewritten, Expression original) {
        Expression analyzedOriginal = typeCoercion(original);
        Assertions.assertTrue(ExpressionUtils.extractConjunction(rewritten).stream()
                .anyMatch(conjunct -> conjunct.equals(analyzedOriginal) && !conjunct.isInferred()));
    }

    private void assertInferredBound(Expression expression,
            Class<? extends ComparisonPredicate> predicateType, Expression source) {
        findInferredBound(expression, predicateType, source);
    }

    private ComparisonPredicate findInferredBound(Expression expression,
            Class<? extends ComparisonPredicate> predicateType, Expression source) {
        List<Expression> conjuncts = ExpressionUtils.extractConjunction(expression);
        return (ComparisonPredicate) conjuncts.stream()
                .filter(conjunct -> predicateType.isInstance(conjunct)
                        && conjunct.child(0).equals(source) && conjunct.isInferred())
                .findFirst()
                .orElseThrow(() -> new AssertionError("missing inferred " + predicateType.getSimpleName()
                        + " bound on " + source + " in " + expression));
    }
}
