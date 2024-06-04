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

import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

/**
 * This class implements the function to simplify expression range.
 * for example:
 * a > 1 and a > 2 => a > 2
 * a > 1 or a > 2 => a > 1
 * a in (1,2,3) and a > 1 => a in (2,3)
 * a in (1,2,3) and a in (3,4,5) => a = 3
 * a in (1,2,3) and a in (4,5,6) => false
 * The logic is as follows:
 * 1. for `And` expression.
 *    1. extract conjunctions then build `ValueDesc` for each conjunction
 *    2. grouping according to `reference`, `ValueDesc` in the same group can perform intersect
 *    for example:
 *    a > 1 and a > 2
 *    1. a > 1 => RangeValueDesc((1...+∞)), a > 2 => RangeValueDesc((2...+∞))
 *    2. (1...+∞) intersect (2...+∞) => (2...+∞)
 * 2. for `Or` expression (similar to `And`).
 * todo: support a > 10 and (a < 10 or a > 20 ) => a > 20
 */
public class SimplifyRange implements ExpressionPatternRuleFactory {
    public static final SimplifyRange INSTANCE = new SimplifyRange();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesTopType(CompoundPredicate.class).then(SimplifyRange::rewrite)
        );
    }

    /** rewrite */
    public static Expression rewrite(CompoundPredicate expr) {
        ValueDesc valueDesc = expr.accept(new RangeInference(), null);
        Expression simplifiedExpr = valueDesc.toExpression();
        return simplifiedExpr == null ? valueDesc.expr : simplifiedExpr;
    }

    private static class RangeInference extends ExpressionVisitor<ValueDesc, Void> {

        @Override
        public ValueDesc visit(Expression expr, Void context) {
            return new UnknownValue(expr);
        }

        private ValueDesc buildRange(ComparisonPredicate predicate) {
            Expression right = predicate.child(1);
            if (right.isNullLiteral()) {
                // it's safe to return empty value if >, >=, <, <= and = with null
                if ((predicate instanceof GreaterThan || predicate instanceof GreaterThanEqual
                        || predicate instanceof LessThan || predicate instanceof LessThanEqual
                        || predicate instanceof EqualTo)) {
                    return new EmptyValue(predicate.child(0), predicate);
                } else {
                    return new UnknownValue(predicate);
                }
            }
            // only handle `NumericType` and `DateLikeType`
            if (right.isLiteral() && (right.getDataType().isNumericType() || right.getDataType().isDateLikeType())) {
                return ValueDesc.range(predicate);
            }
            return new UnknownValue(predicate);
        }

        @Override
        public ValueDesc visitGreaterThan(GreaterThan greaterThan, Void context) {
            return buildRange(greaterThan);
        }

        @Override
        public ValueDesc visitGreaterThanEqual(GreaterThanEqual greaterThanEqual, Void context) {
            return buildRange(greaterThanEqual);
        }

        @Override
        public ValueDesc visitLessThan(LessThan lessThan, Void context) {
            return buildRange(lessThan);
        }

        @Override
        public ValueDesc visitLessThanEqual(LessThanEqual lessThanEqual, Void context) {
            return buildRange(lessThanEqual);
        }

        @Override
        public ValueDesc visitEqualTo(EqualTo equalTo, Void context) {
            return buildRange(equalTo);
        }

        @Override
        public ValueDesc visitInPredicate(InPredicate inPredicate, Void context) {
            // only handle `NumericType` and `DateLikeType`
            if (ExpressionUtils.isAllLiteral(inPredicate.getOptions())
                    && (ExpressionUtils.matchNumericType(inPredicate.getOptions())
                    || ExpressionUtils.matchDateLikeType(inPredicate.getOptions()))) {
                return ValueDesc.discrete(inPredicate);
            }
            return new UnknownValue(inPredicate);
        }

        @Override
        public ValueDesc visitAnd(And and, Void context) {
            return simplify(and, ExpressionUtils.extractConjunction(and), ValueDesc::intersect, ExpressionUtils::and);
        }

        @Override
        public ValueDesc visitOr(Or or, Void context) {
            return simplify(or, ExpressionUtils.extractDisjunction(or), ValueDesc::union, ExpressionUtils::or);
        }

        private ValueDesc simplify(Expression originExpr, List<Expression> predicates,
                BinaryOperator<ValueDesc> op, BinaryOperator<Expression> exprOp) {

            Multimap<Expression, ValueDesc> groupByReference
                    = Multimaps.newListMultimap(new LinkedHashMap<>(), ArrayList::new);
            for (Expression predicate : predicates) {
                ValueDesc valueDesc = predicate.accept(this, null);
                List<ValueDesc> valueDescs = (List<ValueDesc>) groupByReference.get(valueDesc.reference);
                valueDescs.add(valueDesc);
            }

            List<ValueDesc> valuePerRefs = Lists.newArrayList();
            for (Entry<Expression, Collection<ValueDesc>> referenceValues : groupByReference.asMap().entrySet()) {
                List<ValueDesc> valuePerReference = (List) referenceValues.getValue();

                // merge per reference
                ValueDesc simplifiedValue = valuePerReference.get(0);
                for (int i = 1; i < valuePerReference.size(); i++) {
                    simplifiedValue = op.apply(simplifiedValue, valuePerReference.get(i));
                }

                valuePerRefs.add(simplifiedValue);
            }

            if (valuePerRefs.size() == 1) {
                return valuePerRefs.get(0);
            }

            // use UnknownValue to wrap different references
            return new UnknownValue(valuePerRefs, originExpr, exprOp);
        }
    }

    private abstract static class ValueDesc {
        Expression expr;
        Expression reference;

        public ValueDesc(Expression reference, Expression expr) {
            this.expr = expr;
            this.reference = reference;
        }

        public abstract ValueDesc union(ValueDesc other);

        public static ValueDesc union(RangeValue range, DiscreteValue discrete, boolean reverseOrder) {
            long count = discrete.values.stream().filter(x -> range.range.test(x)).count();
            if (count == discrete.values.size()) {
                return range;
            }
            Expression originExpr = ExpressionUtils.or(range.expr, discrete.expr);
            List<ValueDesc> sourceValues = reverseOrder
                    ? ImmutableList.of(discrete, range)
                    : ImmutableList.of(range, discrete);
            return new UnknownValue(sourceValues, originExpr, ExpressionUtils::or);
        }

        public abstract ValueDesc intersect(ValueDesc other);

        public static ValueDesc intersect(RangeValue range, DiscreteValue discrete) {
            DiscreteValue result = new DiscreteValue(discrete.reference, discrete.expr);
            discrete.values.stream().filter(x -> range.range.contains(x)).forEach(result.values::add);
            if (!result.values.isEmpty()) {
                return result;
            }
            return new EmptyValue(range.reference, ExpressionUtils.and(range.expr, discrete.expr));
        }

        public abstract Expression toExpression();

        public static ValueDesc range(ComparisonPredicate predicate) {
            Literal value = (Literal) predicate.right();
            if (predicate instanceof EqualTo) {
                return new DiscreteValue(predicate.left(), predicate, value);
            }
            RangeValue rangeValue = new RangeValue(predicate.left(), predicate);
            if (predicate instanceof GreaterThanEqual) {
                rangeValue.range = Range.atLeast(value);
            } else if (predicate instanceof GreaterThan) {
                rangeValue.range = Range.greaterThan(value);
            } else if (predicate instanceof LessThanEqual) {
                rangeValue.range = Range.atMost(value);
            } else if (predicate instanceof LessThan) {
                rangeValue.range = Range.lessThan(value);
            }

            return rangeValue;
        }

        public static ValueDesc discrete(InPredicate in) {
            // Set<Literal> literals = (Set) Utils.fastToImmutableSet(in.getOptions());
            Set<Literal> literals = in.getOptions().stream().map(Literal.class::cast).collect(Collectors.toSet());
            return new DiscreteValue(in.getCompareExpr(), in, literals);
        }
    }

    private static class EmptyValue extends ValueDesc {

        public EmptyValue(Expression reference, Expression expr) {
            super(reference, expr);
        }

        @Override
        public ValueDesc union(ValueDesc other) {
            return other;
        }

        @Override
        public ValueDesc intersect(ValueDesc other) {
            return this;
        }

        @Override
        public Expression toExpression() {
            return BooleanLiteral.FALSE;
        }
    }

    /**
     * use @see com.google.common.collect.Range to wrap `ComparisonPredicate`
     * for example:
     * a > 1 => (1...+∞)
     */
    private static class RangeValue extends ValueDesc {
        Range<Literal> range;

        public RangeValue(Expression reference, Expression expr) {
            super(reference, expr);
        }

        @Override
        public ValueDesc union(ValueDesc other) {
            if (other instanceof EmptyValue) {
                return other.union(this);
            }
            try {
                if (other instanceof RangeValue) {
                    RangeValue o = (RangeValue) other;
                    if (range.isConnected(o.range)) {
                        RangeValue rangeValue = new RangeValue(reference, ExpressionUtils.or(expr, other.expr));
                        rangeValue.range = range.span(o.range);
                        return rangeValue;
                    }
                    Expression originExpr = ExpressionUtils.or(expr, other.expr);
                    return new UnknownValue(ImmutableList.of(this, other), originExpr, ExpressionUtils::or);
                }
                return union(this, (DiscreteValue) other, false);
            } catch (Exception e) {
                Expression originExpr = ExpressionUtils.or(expr, other.expr);
                return new UnknownValue(ImmutableList.of(this, other), originExpr, ExpressionUtils::or);
            }
        }

        @Override
        public ValueDesc intersect(ValueDesc other) {
            if (other instanceof EmptyValue) {
                return other.intersect(this);
            }
            try {
                if (other instanceof RangeValue) {
                    RangeValue o = (RangeValue) other;
                    if (range.isConnected(o.range)) {
                        RangeValue rangeValue = new RangeValue(reference, ExpressionUtils.and(expr, other.expr));
                        rangeValue.range = range.intersection(o.range);
                        return rangeValue;
                    }
                    return new EmptyValue(reference, ExpressionUtils.and(expr, other.expr));
                }
                return intersect(this, (DiscreteValue) other);
            } catch (Exception e) {
                Expression originExpr = ExpressionUtils.and(expr, other.expr);
                return new UnknownValue(ImmutableList.of(this, other), originExpr, ExpressionUtils::and);
            }
        }

        @Override
        public Expression toExpression() {
            List<Expression> result = Lists.newArrayList();
            if (range.hasLowerBound()) {
                if (range.lowerBoundType() == BoundType.CLOSED) {
                    result.add(new GreaterThanEqual(reference, range.lowerEndpoint()));
                } else {
                    result.add(new GreaterThan(reference, range.lowerEndpoint()));
                }
            }
            if (range.hasUpperBound()) {
                if (range.upperBoundType() == BoundType.CLOSED) {
                    result.add(new LessThanEqual(reference, range.upperEndpoint()));
                } else {
                    result.add(new LessThan(reference, range.upperEndpoint()));
                }
            }
            if (!result.isEmpty()) {
                return ExpressionUtils.and(result);
            } else if (reference.nullable()) {
                // when reference is nullable, we should filter null slot.
                return new Not(new IsNull(reference));
            } else {
                return BooleanLiteral.TRUE;
            }
        }

        @Override
        public String toString() {
            return range == null ? "UnknownRange" : range.toString();
        }
    }

    /**
     * use `Set` to wrap `InPredicate`
     * for example:
     * a in (1,2,3) => [1,2,3]
     */
    private static class DiscreteValue extends ValueDesc {
        Set<Literal> values;

        public DiscreteValue(Expression reference, Expression expr, Literal... values) {
            this(reference, expr, Arrays.asList(values));
        }

        public DiscreteValue(Expression reference, Expression expr, Collection<Literal> values) {
            super(reference, expr);
            this.values = Sets.newTreeSet(values);
        }

        @Override
        public ValueDesc union(ValueDesc other) {
            if (other instanceof EmptyValue) {
                return other.union(this);
            }
            try {
                if (other instanceof DiscreteValue) {
                    DiscreteValue discreteValue = new DiscreteValue(reference, ExpressionUtils.or(expr, other.expr));
                    discreteValue.values.addAll(((DiscreteValue) other).values);
                    discreteValue.values.addAll(this.values);
                    return discreteValue;
                }
                return union((RangeValue) other, this, true);
            } catch (Exception e) {
                Expression originExpr = ExpressionUtils.or(expr, other.expr);
                return new UnknownValue(ImmutableList.of(this, other), originExpr, ExpressionUtils::or);
            }
        }

        @Override
        public ValueDesc intersect(ValueDesc other) {
            if (other instanceof EmptyValue) {
                return other.intersect(this);
            }
            try {
                if (other instanceof DiscreteValue) {
                    DiscreteValue discreteValue = new DiscreteValue(reference, ExpressionUtils.and(expr, other.expr));
                    discreteValue.values.addAll(((DiscreteValue) other).values);
                    discreteValue.values.retainAll(this.values);
                    if (discreteValue.values.isEmpty()) {
                        return new EmptyValue(reference, ExpressionUtils.and(expr, other.expr));
                    } else {
                        return discreteValue;
                    }
                }
                return intersect((RangeValue) other, this);
            } catch (Exception e) {
                Expression originExpr = ExpressionUtils.and(expr, other.expr);
                return new UnknownValue(ImmutableList.of(this, other), originExpr, ExpressionUtils::and);
            }
        }

        @Override
        public Expression toExpression() {
            // NOTICE: it's related with `InPredicateToEqualToRule`
            // They are same processes, so must change synchronously.
            if (values.size() == 1) {
                return new EqualTo(reference, values.iterator().next());

                // this condition should as same as OrToIn, or else meet dead loop
            } else if (values.size() < OrToIn.REWRITE_OR_TO_IN_PREDICATE_THRESHOLD) {
                Iterator<Literal> iterator = values.iterator();
                return new Or(new EqualTo(reference, iterator.next()), new EqualTo(reference, iterator.next()));
            } else {
                return new InPredicate(reference, Lists.newArrayList(values));
            }
        }

        @Override
        public String toString() {
            return values.toString();
        }
    }

    /**
     * Represents processing result.
     */
    private static class UnknownValue extends ValueDesc {
        private final List<ValueDesc> sourceValues;
        private final BinaryOperator<Expression> mergeExprOp;

        private UnknownValue(Expression expr) {
            super(expr, expr);
            sourceValues = ImmutableList.of();
            mergeExprOp = null;
        }

        public UnknownValue(List<ValueDesc> sourceValues, Expression originExpr,
                BinaryOperator<Expression> mergeExprOp) {
            super(sourceValues.get(0).reference, originExpr);
            this.sourceValues = ImmutableList.copyOf(sourceValues);
            this.mergeExprOp = mergeExprOp;
        }

        @Override
        public ValueDesc union(ValueDesc other) {
            Expression originExpr = ExpressionUtils.or(expr, other.expr);
            return new UnknownValue(ImmutableList.of(this, other), originExpr, ExpressionUtils::or);
        }

        @Override
        public ValueDesc intersect(ValueDesc other) {
            Expression originExpr = ExpressionUtils.and(expr, other.expr);
            return new UnknownValue(ImmutableList.of(this, other), originExpr, ExpressionUtils::and);
        }

        @Override
        public Expression toExpression() {
            if (sourceValues.isEmpty()) {
                return expr;
            }

            Expression result = sourceValues.get(0).toExpression();
            for (int i = 1; i < sourceValues.size(); i++) {
                result = mergeExprOp.apply(result, sourceValues.get(i).toExpression());
            }
            return result;
        }
    }
}
