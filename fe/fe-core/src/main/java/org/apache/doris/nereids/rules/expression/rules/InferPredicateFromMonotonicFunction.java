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

import org.apache.doris.nereids.exceptions.AnalysisException;
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
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.RoundingMonotonic;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateFormat;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DaysAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HoursAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Left;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MinutesAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthsAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SecondsAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StrToDate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Substring;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Year;
import org.apache.doris.nereids.trees.expressions.functions.scalar.YearsAdd;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.util.DateUtils;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Derives necessary bare-column predicates from predicates that wrap a column in a monotonic
 * function. The original predicate is always retained, so the result is safe for pruning even when
 * the derived predicate is only a relaxed bound rather than an equivalent rewrite.
 *
 * <p>For example, {@code date_trunc(dt, 'day') >= '2020-01-05'} derives
 * {@code dt >= '2020-01-05'}. Partition pruning uses the extra range for sorted-partition lookup;
 * physical scan filters use it for OLAP zone maps and ORC/Parquet min/max pruning.
 *
 * <p>Four kinds are supported:
 * <ul>
 *   <li>prefix: {@code substring(col, 1, n) >= 'abc'} derives {@code col >= 'abc'};</li>
 *   <li>rounding: {@code floor(col) >= c} derives {@code col >= c}, while
 *       {@code ceil(col) <= c} derives {@code col <= c};</li>
 *   <li>year: {@code year(col) op y} derives a boundary date or the half-open range
 *       {@code [y-01-01, (y+1)-01-01)}.</li>
 *   <li>date formatting: formats whose string order matches chronological order derive a parsed
 *       lower bound on the source date column.</li>
 * </ul>
 * Equality yields a two-sided range when the next or previous bucket boundary can be derived
 * safely; otherwise it retains the sound single-sided bound. {@code year(col) = y} always yields
 * the exact year range when both date boundaries are representable.
 *
 * <p>As with existing partition and storage predicate pushdown, rows rejected by a derived
 * necessary condition do not evaluate the wrapped function, so failures confined to those rows are
 * not observable.
 *
 * <p>{@code from_unixtime} and similar functions are intentionally excluded because their inverse
 * depends on timezone transitions rather than a globally fixed relation.
 */
public final class InferPredicateFromMonotonicFunction {
    private InferPredicateFromMonotonicFunction() {
    }

    /**
     * Append inferred predicates while preserving the original boolean structure. A necessary
     * condition cannot cross negation, so NOT and unsupported shapes are returned unchanged.
     */
    public static Expression inferForPruning(Expression predicate) {
        if (predicate instanceof ComparisonPredicate) {
            return appendInferredPredicate((ComparisonPredicate) predicate);
        }
        if (predicate instanceof And) {
            return rewriteConjunction((And) predicate);
        }
        if (predicate instanceof Or) {
            return rewriteDisjunction((Or) predicate);
        }
        return predicate;
    }

    // Append the derived bare-column predicate with AND; return the comparison unchanged if nothing is derived.
    private static Expression appendInferredPredicate(ComparisonPredicate comparison) {
        Optional<Expression> inferred = infer(comparison);
        if (!inferred.isPresent()) {
            return comparison;
        }
        // Some equalities derive both bounds; flatten them into the surrounding conjunction.
        List<Expression> conjuncts = Lists.newArrayList(comparison);
        conjuncts.addAll(ExpressionUtils.extractConjunction(inferred.get()));
        return ExpressionUtils.and(conjuncts);
    }

    // Preserve every original conjunct and append only new inferred predicates.
    private static Expression rewriteConjunction(And predicate) {
        List<Expression> conjuncts = ExpressionUtils.extractConjunction(predicate);
        Set<Expression> existing = Sets.newHashSet(conjuncts);
        List<Expression> rewrittenConjuncts = Lists.newArrayListWithCapacity(conjuncts.size());
        List<Expression> inferredConjuncts = Lists.newArrayList();
        for (Expression conjunct : conjuncts) {
            if (!(conjunct instanceof ComparisonPredicate)) {
                rewrittenConjuncts.add(inferForPruning(conjunct));
                continue;
            }
            rewrittenConjuncts.add(conjunct);
            Optional<Expression> inferred = infer((ComparisonPredicate) conjunct);
            if (!inferred.isPresent()) {
                continue;
            }
            for (Expression inferredConjunct : ExpressionUtils.extractConjunction(inferred.get())) {
                if (existing.add(inferredConjunct)) {
                    inferredConjuncts.add(inferredConjunct);
                }
            }
        }
        rewrittenConjuncts.addAll(inferredConjuncts);
        return predicate.withChildren(rewrittenConjuncts);
    }

    // Derive per branch and keep the OR: necessary conditions of different branches must not be
    // lifted out and ANDed together. PartitionPredicateToRange unions the branch ranges.
    private static Expression rewriteDisjunction(Or predicate) {
        List<Expression> children = predicate.children();
        List<Expression> rewrittenChildren = Lists.newArrayListWithCapacity(children.size());
        for (Expression child : children) {
            Expression rewritten = inferForPruning(child);
            rewrittenChildren.add(rewritten);
        }
        return predicate.withChildren(rewrittenChildren);
    }

    // Normalize the comparison, then try each supported inference family in order.
    private static Optional<Expression> infer(ComparisonPredicate comparison) {
        ComparisonPredicate normalized = comparison.left() instanceof Literal
                && !(comparison.right() instanceof Literal) ? comparison.commute() : comparison;
        if (!(normalized.right() instanceof Literal) || normalized.right() instanceof NullLiteral) {
            return Optional.empty();
        }

        return inferPrefixPredicate(normalized)
                .or(() -> inferYearPredicate(normalized))
                .or(() -> inferDateFormatPredicate(normalized))
                .or(() -> inferRoundingPredicate(normalized));
    }

    // Prefix inference: a prefix never sorts after the whole string (prefix(s) <= s), so
    // prefix(col) >= 'abc' => col >= 'abc'. >/>= keep the operator; equality also gets an
    // exclusive successor upper bound when one is safe. </<= do not yield a source bound.
    private static Optional<Expression> inferPrefixPredicate(ComparisonPredicate comparison) {
        if (!(comparison.right() instanceof StringLikeLiteral)) {
            return Optional.empty();
        }
        Optional<PrefixInfo> prefix = extractPrefix(comparison.left());
        if (!prefix.isPresent()) {
            return Optional.empty();
        }

        ComparisonPredicate inferred;
        if (comparison instanceof GreaterThan || comparison instanceof GreaterThanEqual) {
            inferred = (ComparisonPredicate) comparison.withChildren(prefix.get().source, comparison.right());
        } else if (comparison instanceof EqualTo) {
            Expression lower = inferredPredicate(new GreaterThanEqual(prefix.get().source, comparison.right()));
            String value = ((StringLikeLiteral) comparison.right()).getStringValue();
            if (BigDecimal.valueOf(value.codePointCount(0, value.length())).compareTo(prefix.get().length) > 0) {
                return Optional.of(lower);
            }
            Optional<String> successor = prefixSuccessor(value);
            if (!successor.isPresent()) {
                return Optional.of(lower);
            }
            Expression upper = inferredPredicate(
                    new LessThan(prefix.get().source, new VarcharLiteral(successor.get())));
            return Optional.of(ExpressionUtils.and(ImmutableList.of(lower, upper)));
        } else {
            return Optional.empty();
        }
        return Optional.of(inferredPredicate(inferred));
    }

    // Extract the bare column from a fixed-length, start-anchored prefix over a character Slot:
    // substring(col, 1, n) (position 1, length present) or left(col, n). Otherwise empty.
    private static Optional<PrefixInfo> extractPrefix(Expression expression) {
        Expression source;
        Expression length;
        if (expression instanceof Substring) {
            Substring substring = (Substring) expression;
            if (!(substring.getPosition() instanceof IntegerLikeLiteral)
                    || ((IntegerLikeLiteral) substring.getPosition()).getBigDecimalValue()
                            .compareTo(BigDecimal.ONE) != 0
                    || !substring.getLength().isPresent()) {
                return Optional.empty();
            }
            source = substring.getSource();
            length = substring.getLength().get();
        } else if (expression instanceof Left) {
            source = expression.child(0);
            length = expression.child(1);
        } else {
            return Optional.empty();
        }

        // A clean fixed-length prefix requires a character Slot source and a positive integer length.
        if (!(source instanceof Slot) || !(source.getDataType() instanceof CharacterType)
                || !(length instanceof IntegerLikeLiteral)
                || ((IntegerLikeLiteral) length).getBigDecimalValue().signum() <= 0) {
            return Optional.empty();
        }
        return Optional.of(new PrefixInfo(source, ((IntegerLikeLiteral) length).getBigDecimalValue()));
    }

    // Build the smallest clean-ASCII string that sorts after every string with the given prefix.
    // Non-ASCII bytes are excluded because storage readers do not all use the same signedness for
    // byte comparison; retaining only the lower bound is safe in those cases.
    private static Optional<String> prefixSuccessor(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        for (byte valueByte : bytes) {
            if ((valueByte & 0xFF) >= 0x80) {
                return Optional.empty();
            }
        }
        int index = bytes.length - 1;
        while (index >= 0 && (bytes[index] & 0xFF) == 0x7F) {
            index--;
        }
        if (index < 0) {
            return Optional.empty();
        }
        byte[] successor = Arrays.copyOf(bytes, index + 1);
        successor[index] = (byte) ((successor[index] & 0xFF) + 1);
        return Optional.of(new String(successor, StandardCharsets.UTF_8));
    }

    // Rounding inference over a bare date column. floor(x) <= x gives a lower bound;
    // ceil(x) >= x gives an upper bound. Equality also gets the opposite strict bound when the
    // adjacent bucket boundary is representable.
    //   floor(dt) >= '2020-01-05'  =>  dt >= '2020-01-05'
    //   ceil(dt)  <= '2020-01-05'  =>  dt <= '2020-01-05'
    private static Optional<Expression> inferRoundingPredicate(ComparisonPredicate comparison) {
        if (!(comparison.left() instanceof RoundingMonotonic)) {
            return Optional.empty();
        }
        RoundingMonotonic function = (RoundingMonotonic) comparison.left();
        // A rounding function may take several arguments (e.g. date_trunc(dt, 'day'));
        // getMonotonicFunctionChildIndex points at the rounded input column.
        Expression source = function.child(function.getMonotonicFunctionChildIndex());
        if (!function.isRoundingRelationGuaranteed()) {
            return Optional.empty();
        }

        ComparisonPredicate inferred;
        Optional<Expression> oppositeBoundary = Optional.empty();
        if (function.getRoundingType() == RoundingMonotonic.RoundingType.FLOOR) {
            if (comparison instanceof GreaterThan || comparison instanceof GreaterThanEqual) {
                inferred = (ComparisonPredicate) comparison.withChildren(source, comparison.right());
            } else if (comparison instanceof EqualTo) {
                inferred = new GreaterThanEqual(source, comparison.right());
                oppositeBoundary = function.nextBucketBoundary((Literal) comparison.right());
            } else {
                // floor gives no upper bound: floor(dt) <= c allows dt beyond c within the same bucket.
                return Optional.empty();
            }
        } else {
            if (comparison instanceof LessThan || comparison instanceof LessThanEqual) {
                inferred = (ComparisonPredicate) comparison.withChildren(source, comparison.right());
            } else if (comparison instanceof EqualTo) {
                inferred = new LessThanEqual(source, comparison.right());
                oppositeBoundary = function.previousBucketBoundary((Literal) comparison.right());
            } else {
                return Optional.empty();
            }
        }
        Optional<Expression> sameDirectionBound = inferredDatePredicate(inferred);
        if (!sameDirectionBound.isPresent() || !oppositeBoundary.isPresent()) {
            return sameDirectionBound;
        }
        Optional<Literal> foldedBoundary = foldLiteral(oppositeBoundary.get());
        if (!foldedBoundary.isPresent()) {
            return sameDirectionBound;
        }
        ComparisonPredicate oppositePredicate = function.getRoundingType() == RoundingMonotonic.RoundingType.FLOOR
                ? new LessThan(source, foldedBoundary.get())
                : new GreaterThan(source, foldedBoundary.get());
        Optional<Expression> oppositeDirectionBound = inferredDatePredicate(oppositePredicate);
        if (!oppositeDirectionBound.isPresent()) {
            return sameDirectionBound;
        }
        return Optional.of(ExpressionUtils.and(
                ImmutableList.of(sameDirectionBound.get(), oppositeDirectionBound.get())));
    }

    private static Optional<Literal> foldLiteral(Expression expression) {
        try {
            Expression foldable = expression instanceof BoundFunction
                    ? TypeCoercionUtils.processBoundFunction((BoundFunction) expression)
                    : expression;
            Expression folded = FoldConstantRuleOnFE.evaluateWithoutContext(foldable);
            return folded instanceof Literal && !(folded instanceof NullLiteral)
                    ? Optional.of((Literal) folded)
                    : Optional.empty();
        } catch (AnalysisException | ArithmeticException | DateTimeException e) {
            // Boundary arithmetic can overflow, and date-format constants can be invalid. In both
            // cases inference is optional, so omit the unavailable folded literal.
            return Optional.empty();
        }
    }

    // A whitelisted date format preserves chronological ordering in its string representation.
    // Parse the comparison constant with the same format and use it as a relaxed source lower bound.
    // Equality also gets the exclusive end of the represented calendar bucket.
    private static Optional<Expression> inferDateFormatPredicate(ComparisonPredicate comparison) {
        if (!(comparison instanceof GreaterThan) && !(comparison instanceof GreaterThanEqual)
                && !(comparison instanceof EqualTo)) {
            return Optional.empty();
        }
        if (!(comparison.left() instanceof DateFormat)
                || !(comparison.right() instanceof StringLikeLiteral)) {
            return Optional.empty();
        }
        DateFormat dateFormat = (DateFormat) comparison.left();
        Expression source = dateFormat.child(0);
        Expression format = dateFormat.child(1);
        if (!(format instanceof StringLikeLiteral)) {
            return Optional.empty();
        }
        String formatValue = ((StringLikeLiteral) format).getStringValue();
        if (!DateUtils.monoFormat.contains(formatValue)) {
            return Optional.empty();
        }
        Optional<Literal> lowerBoundary = foldLiteral(
                dateFormatBoundary((StringLikeLiteral) comparison.right(), formatValue));
        if (!lowerBoundary.isPresent()) {
            return Optional.empty();
        }
        Optional<Expression> lower = inferredDatePredicate(
                new GreaterThanEqual(source, lowerBoundary.get()));
        if (!(comparison instanceof EqualTo) || !lower.isPresent()) {
            return lower;
        }
        Optional<Expression> nextBoundary = nextDateFormatBoundary(formatValue, lowerBoundary.get());
        if (!nextBoundary.isPresent()) {
            return lower;
        }
        Optional<Literal> foldedBoundary = foldLiteral(nextBoundary.get());
        if (!foldedBoundary.isPresent()) {
            return lower;
        }
        Optional<Expression> upper = inferredDatePredicate(new LessThan(source, foldedBoundary.get()));
        return upper.isPresent()
                ? Optional.of(ExpressionUtils.and(ImmutableList.of(lower.get(), upper.get())))
                : lower;
    }

    // str_to_date does not synthesize missing month/day fields, so the year-only and month-only
    // monotonic formats are expanded explicitly to the first instant of their represented bucket.
    private static Expression dateFormatBoundary(StringLikeLiteral value, String format) {
        switch (format) {
            case "%Y":
                return new StrToDate(
                        new VarcharLiteral(value.getStringValue() + "-01-01"),
                        new VarcharLiteral("%Y-%m-%d"));
            case "%Y-%m":
                return new StrToDate(
                        new VarcharLiteral(value.getStringValue() + "-01"),
                        new VarcharLiteral("%Y-%m-%d"));
            case "%Y%m":
                return new StrToDate(
                        new VarcharLiteral(value.getStringValue() + "01"),
                        new VarcharLiteral("%Y%m%d"));
            default:
                return new StrToDate(value, new VarcharLiteral(format));
        }
    }

    private static Optional<Expression> nextDateFormatBoundary(String format, Literal value) {
        IntegerLiteral one = new IntegerLiteral(1);
        switch (format) {
            case "%Y":
                return Optional.of(new YearsAdd(value, one));
            case "%Y-%m":
            case "%Y%m":
                return Optional.of(new MonthsAdd(value, one));
            case "yyyyMMdd":
            case "yyyy-MM-dd":
            case "%Y-%m-%d":
            case "%Y%m%d":
                return Optional.of(new DaysAdd(value, one));
            case "%Y-%m-%d %H":
                return Optional.of(new HoursAdd(value, one));
            case "%Y-%m-%d %H:%i":
                return Optional.of(new MinutesAdd(value, one));
            case "yyyy-MM-dd HH:mm:ss":
            case "%Y-%m-%d %H:%i:%s":
            case "%Y-%m-%d %H:%i:%S":
            case "%Y-%m-%d %T":
                return Optional.of(new SecondsAdd(value, one));
            default:
                return Optional.empty();
        }
    }

    // Year inference: map year(dt) op y to a range on dt, where start = y-01-01, end = (y+1)-01-01,
    // so year(dt) = y is the half-open range [start, end). Operator mapping (y=2020 example):
    //     year(dt) >= 2020  ->  dt >= 2020-01-01
    //     year(dt) >  2020  ->  dt >= 2021-01-01   (after year y means not before next year's start)
    //     year(dt) <  2020  ->  dt <  2020-01-01
    //     year(dt) <= 2020  ->  dt <  2021-01-01   (not after year y means before next year's start)
    //     year(dt) =  2020  ->  dt >= 2020-01-01 AND dt < 2021-01-01
    private static Optional<Expression> inferYearPredicate(ComparisonPredicate comparison) {
        // year returns SmallInt, so comparing against a wider integer literal wraps it in an integer
        // cast (cast(year(dt) as INT)); such a widening cast is value-preserving, so see through it.
        Expression left = comparison.left();
        if (left instanceof Cast && left.child(0) instanceof Year
                && left.getDataType().isIntegerLikeType()
                && left.child(0).getDataType().width() <= left.getDataType().width()) {
            left = left.child(0);
        }
        if (!(left instanceof Year) || !(comparison.right() instanceof IntegerLikeLiteral)) {
            return Optional.empty();
        }
        Expression source = left.child(0);
        if (!isDateSlot(source)) {
            return Optional.empty();
        }

        BigInteger year = ((IntegerLikeLiteral) comparison.right()).getBigDecimalValue().toBigIntegerExact();
        Optional<DateV2Literal> start = firstDayOfYear(year);
        // y=9999 is the date upper bound and has no next year, so end is absent and branches that
        // depend on it (>, <=, and the upper side of =) cannot be derived.
        Optional<DateV2Literal> end = year.compareTo(BigInteger.valueOf(9999)) < 0
                ? firstDayOfYear(year.add(BigInteger.ONE)) : Optional.empty();
        if (comparison instanceof GreaterThanEqual && start.isPresent()) {
            return Optional.of(inferredPredicate(new GreaterThanEqual(source, start.get())));
        }
        if (comparison instanceof GreaterThan && end.isPresent()) {
            return Optional.of(inferredPredicate(new GreaterThanEqual(source, end.get())));
        }
        if (comparison instanceof LessThan && start.isPresent()) {
            return Optional.of(inferredPredicate(new LessThan(source, start.get())));
        }
        if (comparison instanceof LessThanEqual && end.isPresent()) {
            return Optional.of(inferredPredicate(new LessThan(source, end.get())));
        }
        if (!(comparison instanceof EqualTo) || !start.isPresent()) {
            return Optional.empty();
        }

        // EqualTo: lower bound dt >= start, plus upper bound dt < end when end is present.
        Expression lower = inferredPredicate(new GreaterThanEqual(source, start.get()));
        if (!end.isPresent()) {
            return Optional.of(lower);
        }
        Expression upper = inferredPredicate(new LessThan(source, end.get()));
        return Optional.of(ExpressionUtils.and(ImmutableList.of(lower, upper)));
    }

    // Build the year-01-01 literal; empty when the year is outside the valid [0, 9999] range.
    private static Optional<DateV2Literal> firstDayOfYear(BigInteger year) {
        return year.signum() >= 0 && year.compareTo(BigInteger.valueOf(9999)) <= 0
                ? Optional.of(new DateV2Literal(year.intValueExact(), 1, 1))
                : Optional.empty();
    }

    // A date column we can infer on: a Slot of date-like type, excluding timezone-aware timestamptz
    // (its comparison depends on the session timezone and is not a globally fixed relation).
    private static boolean isDateSlot(Expression expression) {
        return expression instanceof Slot && expression.getDataType().isDateLikeType()
                && !expression.getDataType().isTimeStampTzType();
    }

    // Type-coerce the derived predicate and mark it inferred, so consumers can distinguish it from
    // a predicate written by the user.
    private static Expression inferredPredicate(ComparisonPredicate predicate) {
        return coerceAndFoldRight(predicate).withInferred(true);
    }

    // The date floor/ceil binder may promote a DATE slot to DATETIMEV2 because its hidden default
    // origin is DATETIMEV2. Derive against that actual argument first, then reuse the comparison
    // simplifier to recover a bare date Slot without losing non-midnight boundary semantics.
    private static Optional<Expression> inferredDatePredicate(ComparisonPredicate predicate) {
        ComparisonPredicate coerced = coerceAndFoldRight(predicate);
        Expression simplified = SimplifyComparisonPredicate.simplify(coerced);
        return simplified instanceof ComparisonPredicate && isDateSlot(simplified.child(0))
                && simplified.child(1) instanceof Literal
                ? Optional.of(simplified.withInferred(true))
                : Optional.empty();
    }

    private static ComparisonPredicate coerceAndFoldRight(ComparisonPredicate predicate) {
        ComparisonPredicate coerced = (ComparisonPredicate) TypeCoercionUtils.processComparisonPredicate(predicate);
        Optional<Literal> foldedRight = foldLiteral(coerced.right());
        if (foldedRight.isPresent()) {
            coerced = (ComparisonPredicate) coerced.withChildren(coerced.left(), foldedRight.get());
        }
        return coerced;
    }

    private static final class PrefixInfo {
        private final Expression source;
        private final BigDecimal length;

        private PrefixInfo(Expression source, BigDecimal length) {
            this.source = source;
            this.length = length;
        }
    }
}
