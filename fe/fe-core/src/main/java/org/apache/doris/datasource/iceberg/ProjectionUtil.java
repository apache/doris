package org.apache.doris.datasource.iceberg;

import org.apache.iceberg.expressions.BoundLiteralPredicate;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.BoundSetPredicate;
import org.apache.iceberg.expressions.BoundTransform;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.transforms.Transform;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Set;
import java.util.function.Function;

class ProjectionUtil {

    private ProjectionUtil() {
    }

    static <T> UnboundPredicate<T> truncateInteger(
            String name, BoundLiteralPredicate<Integer> pred, Function<Integer, T> transform) {
        int boundary = pred.literal().value();
        switch (pred.op()) {
            case LT:
                // adjust closed and then transform ltEq
                return Expressions.predicate(Expression.Operation.LT_EQ, name, transform.apply(boundary - 1));
            case LT_EQ:
                return Expressions.predicate(Expression.Operation.LT_EQ, name, transform.apply(boundary));
            case GT:
                // adjust closed and then transform gtEq
                return Expressions.predicate(Expression.Operation.GT_EQ, name, transform.apply(boundary + 1));
            case GT_EQ:
                return Expressions.predicate(Expression.Operation.GT_EQ, name, transform.apply(boundary));
            case EQ:
                return Expressions.predicate(pred.op(), name, transform.apply(boundary));
            default:
                return null;
        }
    }

    static <T> UnboundPredicate<T> truncateIntegerStrict(
            String name, BoundLiteralPredicate<Integer> pred, Function<Integer, T> transform) {
        int boundary = pred.literal().value();
        switch (pred.op()) {
            case LT:
                return Expressions.predicate(Expression.Operation.LT, name, transform.apply(boundary));
            case LT_EQ:
                return Expressions.predicate(Expression.Operation.LT, name, transform.apply(boundary + 1));
            case GT:
                return Expressions.predicate(Expression.Operation.GT, name, transform.apply(boundary));
            case GT_EQ:
                return Expressions.predicate(Expression.Operation.GT, name, transform.apply(boundary - 1));
            case NOT_EQ:
                return Expressions.predicate(Expression.Operation.NOT_EQ, name, transform.apply(boundary));
            case EQ:
                // there is no predicate that guarantees equality because adjacent ints transform to the
                // same value
                return null;
            default:
                return null;
        }
    }

    static <T> UnboundPredicate<T> truncateLongStrict(
            String name, BoundLiteralPredicate<Long> pred, Function<Long, T> transform) {
        long boundary = pred.literal().value();
        switch (pred.op()) {
            case LT:
                return Expressions.predicate(Expression.Operation.LT, name, transform.apply(boundary));
            case LT_EQ:
                return Expressions.predicate(Expression.Operation.LT, name, transform.apply(boundary + 1L));
            case GT:
                return Expressions.predicate(Expression.Operation.GT, name, transform.apply(boundary));
            case GT_EQ:
                return Expressions.predicate(Expression.Operation.GT, name, transform.apply(boundary - 1L));
            case NOT_EQ:
                return Expressions.predicate(Expression.Operation.NOT_EQ, name, transform.apply(boundary));
            case EQ:
                // there is no predicate that guarantees equality because adjacent longs transform to the
                // same value
                return null;
            default:
                return null;
        }
    }

    static <T> UnboundPredicate<T> truncateLong(
            String name, BoundLiteralPredicate<Long> pred, Function<Long, T> transform) {
        long boundary = pred.literal().value();
        switch (pred.op()) {
            case LT:
                // adjust closed and then transform ltEq
                return Expressions.predicate(Expression.Operation.LT_EQ, name, transform.apply(boundary - 1L));
            case LT_EQ:
                return Expressions.predicate(Expression.Operation.LT_EQ, name, transform.apply(boundary));
            case GT:
                // adjust closed and then transform gtEq
                return Expressions.predicate(Expression.Operation.GT_EQ, name, transform.apply(boundary + 1L));
            case GT_EQ:
                return Expressions.predicate(Expression.Operation.GT_EQ, name, transform.apply(boundary));
            case EQ:
                return Expressions.predicate(pred.op(), name, transform.apply(boundary));
            default:
                return null;
        }
    }

    static <T> UnboundPredicate<T> truncateDecimal(
            String name, BoundLiteralPredicate<BigDecimal> pred, Function<BigDecimal, T> transform) {
        BigDecimal boundary = pred.literal().value();
        switch (pred.op()) {
            case LT:
                // adjust closed and then transform ltEq
                BigDecimal minusOne =
                        new BigDecimal(boundary.unscaledValue().subtract(BigInteger.ONE), boundary.scale());
                return Expressions.predicate(Expression.Operation.LT_EQ, name, transform.apply(minusOne));
            case LT_EQ:
                return Expressions.predicate(Expression.Operation.LT_EQ, name, transform.apply(boundary));
            case GT:
                // adjust closed and then transform gtEq
                BigDecimal plusOne =
                        new BigDecimal(boundary.unscaledValue().add(BigInteger.ONE), boundary.scale());
                return Expressions.predicate(Expression.Operation.GT_EQ, name, transform.apply(plusOne));
            case GT_EQ:
                return Expressions.predicate(Expression.Operation.GT_EQ, name, transform.apply(boundary));
            case EQ:
                return Expressions.predicate(pred.op(), name, transform.apply(boundary));
            default:
                return null;
        }
    }

    static <T> UnboundPredicate<T> truncateDecimalStrict(
            String name, BoundLiteralPredicate<BigDecimal> pred, Function<BigDecimal, T> transform) {
        BigDecimal boundary = pred.literal().value();

        BigDecimal minusOne =
                new BigDecimal(boundary.unscaledValue().subtract(BigInteger.ONE), boundary.scale());

        BigDecimal plusOne =
                new BigDecimal(boundary.unscaledValue().add(BigInteger.ONE), boundary.scale());

        switch (pred.op()) {
            case LT:
                return Expressions.predicate(Expression.Operation.LT, name, transform.apply(boundary));
            case LT_EQ:
                return Expressions.predicate(Expression.Operation.LT, name, transform.apply(plusOne));
            case GT:
                return Expressions.predicate(Expression.Operation.GT, name, transform.apply(boundary));
            case GT_EQ:
                return Expressions.predicate(Expression.Operation.GT, name, transform.apply(minusOne));
            case NOT_EQ:
                return Expressions.predicate(Expression.Operation.NOT_EQ, name, transform.apply(boundary));
            case EQ:
                // there is no predicate that guarantees equality because adjacent decimals transform to the
                // same value
                return null;
            default:
                return null;
        }
    }

    static <S, T> UnboundPredicate<T> truncateArray(
            String name, BoundLiteralPredicate<S> pred, Function<S, T> transform) {
        S boundary = pred.literal().value();
        switch (pred.op()) {
            case LT:
            case LT_EQ:
                return Expressions.predicate(Expression.Operation.LT_EQ, name, transform.apply(boundary));
            case GT:
            case GT_EQ:
                return Expressions.predicate(Expression.Operation.GT_EQ, name, transform.apply(boundary));
            case EQ:
                return Expressions.predicate(Expression.Operation.EQ, name, transform.apply(boundary));
            case STARTS_WITH:
                return Expressions.predicate(Expression.Operation.STARTS_WITH, name, transform.apply(boundary));
            default:
                return null;
        }
    }

    static <S, T> UnboundPredicate<T> truncateArrayStrict(
            String name, BoundLiteralPredicate<S> pred, Function<S, T> transform) {
        S boundary = pred.literal().value();
        switch (pred.op()) {
            case LT:
            case LT_EQ:
                return Expressions.predicate(Expression.Operation.LT, name, transform.apply(boundary));
            case GT:
            case GT_EQ:
                return Expressions.predicate(Expression.Operation.GT, name, transform.apply(boundary));
            case NOT_EQ:
                return Expressions.predicate(Expression.Operation.NOT_EQ, name, transform.apply(boundary));
            case EQ:
                // there is no predicate that guarantees equality because adjacent values transform to the
                // same partition
                return null;
            default:
                return null;
        }
    }

    /**
     * If the predicate has a transformed child that matches the given transform, return a predicate.
     */
    @SuppressWarnings("unchecked")
    static <T> UnboundPredicate<T> projectTransformPredicate(
            Transform<?, T> transform, String partitionName, BoundPredicate<?> pred) {
        if (pred.term() instanceof BoundTransform
                && transform.toString().equals(((BoundTransform<?, ?>) pred.term()).transform().toString())) {
            // the bound value must be a T because the transform matches
            return (UnboundPredicate<T>) removeTransform(partitionName, pred);
        }
        return null;
    }

    private static <T> UnboundPredicate<T> removeTransform(
            String partitionName, BoundPredicate<T> pred) {
        if (pred.isUnaryPredicate()) {
            return Expressions.predicate(pred.op(), partitionName);
        } else if (pred.isLiteralPredicate()) {
            return Expressions.predicate(pred.op(), partitionName, pred.asLiteralPredicate().literal());
        } else if (pred.isSetPredicate()) {
            return Expressions.predicate(pred.op(), partitionName, pred.asSetPredicate().literalSet());
        }
        throw new UnsupportedOperationException(
            "Cannot replace transform in unknown predicate: " + pred);
    }

    static <S, T> UnboundPredicate<T> transformSet(
            String fieldName, BoundSetPredicate<S> predicate, Function<S, T> transform) {
        return Expressions.predicate(
            predicate.op(),
            fieldName,
            Iterables.transform(predicate.asSetPredicate().literalSet(), transform::apply));
    }

    /**
     * Fixes an inclusive projection to account for incorrectly transformed values.
     *
     * <p>A bug in 0.10.0 and earlier caused negative values to be incorrectly transformed by date and
     * timestamp transforms to 1 larger than the correct value. For example, day(1969-12-31 10:00:00)
     * produced 0 instead of -1. To read data written by versions with this bug, this method adjusts
     * the inclusive projection. The current inclusive projection is correct, so this modifies the
     * "correct" projection when needed. For example, < day(1969-12-31 10:00:00) will produce <= -1 (=
     * 1969-12-31) and is adjusted to <= 0 (= 1970-01-01) because the incorrect transformed value was
     * 0.
     */
    static UnboundPredicate<Integer> fixInclusiveTimeProjection(UnboundPredicate<Integer> projected) {
        if (projected == null) {
            return projected;
        }

        // adjust the predicate for values that were 1 larger than the correct transformed value
        switch (projected.op()) {
            case LT:
                if (projected.literal().value() < 0) {
                    return Expressions.lessThan(projected.term(), projected.literal().value() + 1);
                }
                return projected;

            case LT_EQ:
                if (projected.literal().value() < 0) {
                    return Expressions.lessThanOrEqual(projected.term(), projected.literal().value() + 1);
                }
                return projected;

            case GT:
            case GT_EQ:
                // incorrect projected values are already greater than the bound for GT, GT_EQ
                return projected;

            case EQ:
                if (projected.literal().value() < 0) {
                    // match either the incorrect value (projectedValue + 1) or the correct value
                    // (projectedValue)
                    return Expressions.in(
                        projected.term(), projected.literal().value(), projected.literal().value() + 1);
                }
                return projected;

            case IN:
                Set<Integer> fixedSet = Sets.newHashSet();
                boolean hasNegativeValue = false;
                for (Literal<Integer> lit : projected.literals()) {
                    Integer value = lit.value();
                    fixedSet.add(value);
                    if (value < 0) {
                        hasNegativeValue = true;
                        fixedSet.add(value + 1);
                    }
                }

                if (hasNegativeValue) {
                    return Expressions.in(projected.term(), fixedSet);
                }
                return projected;

            case NOT_IN:
            case NOT_EQ:
                // there is no inclusive projection for NOT_EQ and NOT_IN
                return null;

            default:
                return projected;
        }
    }

    /**
     * Fixes a strict projection to account for incorrectly transformed values.
     *
     * <p>A bug in 0.10.0 and earlier caused negative values to be incorrectly transformed by date and
     * timestamp transforms to 1 larger than the correct value. For example, day(1969-12-31 10:00:00)
     * produced 0 instead of -1. To read data written by versions with this bug, this method adjusts
     * the strict projection.
     */
    static UnboundPredicate<Integer> fixStrictTimeProjection(UnboundPredicate<Integer> projected) {
        if (projected == null) {
            return null;
        }

        switch (projected.op()) {
            case LT:
            case LT_EQ:
                // the correct bound is a correct strict projection for the incorrectly transformed values.
                return projected;

            case GT:
                // GT and GT_EQ need to be adjusted because values that do not match the predicate may have
                // been transformed into partition values that match the projected predicate. For example, >=
                // month(1969-11-31) is > -2, but 1969-10-31 was previously transformed to month -2 instead
                // of -3. This must use the more strict value.
                if (projected.literal().value() <= 0) {
                    return Expressions.greaterThan(projected.term(), projected.literal().value() + 1);
                }
                return projected;

            case GT_EQ:
                if (projected.literal().value() <= 0) {
                    return Expressions.greaterThanOrEqual(projected.term(), projected.literal().value() + 1);
                }
                return projected;

            case EQ:
            case IN:
                // there is no strict projection for EQ and IN
                return null;

            case NOT_EQ:
                if (projected.literal().value() < 0) {
                    return Expressions.notIn(
                        projected.term(), projected.literal().value(), projected.literal().value() + 1);
                }
                return projected;

            case NOT_IN:
                Set<Integer> fixedSet = Sets.newHashSet();
                boolean hasNegativeValue = false;
                for (Literal<Integer> lit : projected.literals()) {
                    Integer value = lit.value();
                    fixedSet.add(value);
                    if (value < 0) {
                        hasNegativeValue = true;
                        fixedSet.add(value + 1);
                    }
                }

                if (hasNegativeValue) {
                    return Expressions.notIn(projected.term(), fixedSet);
                }
                return projected;

            default:
                return null;
        }
    }
}
