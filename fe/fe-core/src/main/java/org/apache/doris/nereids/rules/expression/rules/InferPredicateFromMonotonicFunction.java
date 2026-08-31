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
import org.apache.doris.nereids.trees.expressions.functions.RoundingMonotonic;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Left;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Substring;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Year;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.Set;

// When a partition predicate wraps the partition column in a function
// (e.g. date_trunc(dt, 'day') >= '2020-01-01'), this class derives an extra predicate on the
// bare column (dt >= '2020-01-01') and appends it, so partition pruning has a range to work with.
//
// Why: the fast pruning path binary-searches a partition list sorted by the partition column
// (PartitionPredicateToRange). Binary search only understands "bare column op literal" shapes that
// map to a range; once the column is wrapped by date_trunc / year / substring it cannot, and pruning
// falls back to per-partition evaluation. The derived predicate is the key that unlocks binary
// search. It lives only inside pruning and is never written back as a runtime filter, so it adds no
// per-row cost. See the caller PartitionPruner.pruneInternal.
// Like the existing monotonic partition evaluator, it may avoid evaluating the wrapped function in
// a partition proven irrelevant; runtime errors from rows in such a partition are therefore not observable.
//
//   input:   date_trunc(dt, 'day') >= '2020-01-05'   (date_trunc is a floor, floor(x) <= x)
//   infer:   floor(dt) >= '2020-01-05'  =>  dt >= '2020-01-05'
//   output:  date_trunc(dt, 'day') >= '2020-01-05' AND dt >= '2020-01-05'
//
// Every derived predicate is a necessary condition of the original, resting only on the function's
// monotonic/rounding property and never on data distribution, so appending it never changes results.
// Three kinds are supported:
//   - prefix:   substring(col,1,n) >= 'abc'  =>  col >= 'abc'   (a prefix never sorts after the whole string)
//   - rounding: floor(col) >= c => col >= c;  ceil(col) <= c => col <= c
//   - year:     year(col) op y  =>  a boundary date or the half-open range [y-01-01, (y+1)-01-01)
// For each kind, EqualTo yields only a single-sided non-strict bound (floor(col)=c gives col >= c only).
//
// from_unixtime and similar are excluded on purpose: they map a unix timestamp to local time, so the
// inverse depends on timezone shifts (DST) rather than a globally fixed property, and no safe bare-column
// bound can be derived.
final class InferPredicateFromMonotonicFunction {
    private InferPredicateFromMonotonicFunction() {
    }

    // Comparison: derive directly. AND/OR: recurse into children and keep the boolean structure.
    // NOT and other shapes are returned unchanged, since a necessary condition cannot cross negation.
    static Expression inferForPartitionPrune(Expression partitionPredicate) {
        if (partitionPredicate instanceof ComparisonPredicate) {
            return appendInferredPredicate((ComparisonPredicate) partitionPredicate);
        }
        if (partitionPredicate instanceof And) {
            return rewriteConjunction((And) partitionPredicate);
        }
        if (partitionPredicate instanceof Or) {
            return rewriteDisjunction((Or) partitionPredicate);
        }
        return partitionPredicate;
    }

    // Append the derived bare-column predicate with AND; return the comparison unchanged if nothing is derived.
    private static Expression appendInferredPredicate(ComparisonPredicate comparison) {
        Optional<Expression> inferred = infer(comparison);
        if (!inferred.isPresent()) {
            return comparison;
        }
        // year EqualTo derives a lower AND upper bound; flatten so everything joins one AND.
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
                rewrittenConjuncts.add(inferForPartitionPrune(conjunct));
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
            Expression rewritten = inferForPartitionPrune(child);
            rewrittenChildren.add(rewritten);
        }
        return predicate.withChildren(rewrittenChildren);
    }

    // Try the three inference kinds in order. Precondition: the right side must be a literal.
    private static Optional<Expression> infer(ComparisonPredicate comparison) {
        if (!(comparison.right() instanceof Literal)) {
            return Optional.empty();
        }

        return inferPrefixPredicate(comparison)
                .or(() -> inferYearPredicate(comparison))
                .or(() -> inferRoundingPredicate(comparison));
    }

    // Prefix inference: a prefix never sorts after the whole string (prefix(s) <= s), so
    // prefix(col) >= 'abc' => col >= 'abc'. >/>= keep the operator; = yields only the lower bound;
    // </<= do not hold (prefix(col) <= 'abc' allows col = 'abd', which is larger).
    private static Optional<Expression> inferPrefixPredicate(ComparisonPredicate comparison) {
        if (!(comparison.right() instanceof StringLikeLiteral)) {
            return Optional.empty();
        }
        Optional<Expression> source = prefixSource(comparison.left());
        if (!source.isPresent()) {
            return Optional.empty();
        }

        ComparisonPredicate inferred;
        if (comparison instanceof GreaterThan || comparison instanceof GreaterThanEqual) {
            inferred = (ComparisonPredicate) comparison.withChildren(source.get(), comparison.right());
        } else if (comparison instanceof EqualTo) {
            inferred = new GreaterThanEqual(source.get(), comparison.right());
        } else {
            return Optional.empty();
        }
        return Optional.of(inferredPredicate(inferred));
    }

    // Extract the bare column from a fixed-length, start-anchored prefix over a character Slot:
    // substring(col, 1, n) (position 1, length present) or left(col, n). Otherwise empty.
    private static Optional<Expression> prefixSource(Expression expression) {
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
        return Optional.of(source);
    }

    // Rounding inference over a bare date column. floor(x) <= x gives a lower bound;
    // ceil(x) >= x gives an upper bound. EqualTo yields only the single-sided non-strict bound.
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
        if (function.getRoundingType() == RoundingMonotonic.RoundingType.FLOOR) {
            if (comparison instanceof GreaterThan || comparison instanceof GreaterThanEqual) {
                inferred = (ComparisonPredicate) comparison.withChildren(source, comparison.right());
            } else if (comparison instanceof EqualTo) {
                inferred = new GreaterThanEqual(source, comparison.right());
            } else {
                // floor gives no upper bound: floor(dt) <= c allows dt beyond c within the same bucket.
                return Optional.empty();
            }
        } else {
            if (comparison instanceof LessThan || comparison instanceof LessThanEqual) {
                inferred = (ComparisonPredicate) comparison.withChildren(source, comparison.right());
            } else if (comparison instanceof EqualTo) {
                inferred = new LessThanEqual(source, comparison.right());
            } else {
                return Optional.empty();
            }
        }
        return inferredDatePredicate(inferred);
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

    // Type-coerce the derived predicate and mark it inferred, so downstream keeps it inside pruning
    // and never writes it back as a user filter.
    private static Expression inferredPredicate(ComparisonPredicate predicate) {
        return TypeCoercionUtils.processComparisonPredicate(predicate).withInferred(true);
    }

    // The date floor/ceil binder may promote a DATE slot to DATETIMEV2 because its hidden default
    // origin is DATETIMEV2. Derive against that actual argument first, then reuse the comparison
    // simplifier to recover a bare date Slot without losing non-midnight boundary semantics.
    private static Optional<Expression> inferredDatePredicate(ComparisonPredicate predicate) {
        ComparisonPredicate coerced = (ComparisonPredicate) TypeCoercionUtils.processComparisonPredicate(predicate);
        Expression simplified = SimplifyComparisonPredicate.simplify(coerced);
        return simplified instanceof ComparisonPredicate && isDateSlot(simplified.child(0))
                ? Optional.of(simplified.withInferred(true))
                : Optional.empty();
    }
}
