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

package org.apache.doris.nereids.rules.expression.rewrite.rules;

import org.apache.doris.nereids.rules.expression.rewrite.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionRuleExecutor;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Objects;
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
 * a in(1,2,3) and a in (4,5,6) => false
 * The logic is as follows:
 * 1. for `And` expression.
 *    1. extract conjunctions then build `ValueDesc` for each conjunction
 *    2. grouping according to `reference`, `ValueDesc` in the same group can perform intersect
 *    for example:
 *    a > 1 and a > 2
 *    1. a > 1 => RangeValueDesc((1...+∞)), a > 2 => RangeValueDesc((2...+∞))
 *    2. (1...+∞) intersect (2...+∞) => (2...+∞)
 * 2. for `Or` expression (similar to `And`).
 */
public class SimplifyRange extends AbstractExpressionRewriteRule {

    public static final SimplifyRange INSTANCE = new SimplifyRange();

    @Override
    public Expression rewrite(Expression expr, ExpressionRewriteContext ctx) {
        return expr instanceof CompoundPredicate ? expr.accept(new RangeInference(), null).expr : expr;
    }

    private static class RangeInference extends ExpressionVisitor<ValueDesc, Void> {

        @Override
        public ValueDesc visit(Expression expr, Void context) {
            return ResultValue.of(expr);
        }

        private ValueDesc buildRange(ComparisonPredicate predicate) {
            Expression rewrite = ExpressionRuleExecutor.normalize(predicate);
            Expression right = rewrite.child(1);
            // only handle `NumericType`
            if (right.isLiteral() && right.getDataType().isNumericType()) {
                return ValueDesc.range((ComparisonPredicate) rewrite);
            }
            return ValueDesc.EMPTY;
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
            // only handle `NumericType`
            if (ExpressionUtils.isAllLiteral(inPredicate.getOptions())
                    && ExpressionUtils.matchNumericType(inPredicate.getOptions())) {
                return ValueDesc.discrete(inPredicate);
            }
            return ValueDesc.EMPTY;
        }

        @Override
        public ValueDesc visitAnd(And and, Void context) {
            List<Expression> result = simplify(ExpressionUtils.extractConjunction(and), ValueDesc::intersect);
            return ResultValue.of(ExpressionUtils.and(result));
        }

        @Override
        public ValueDesc visitOr(Or or, Void context) {
            List<Expression> result = simplify(ExpressionUtils.extractDisjunction(or), ValueDesc::union);
            return ResultValue.of(ExpressionUtils.or(result));
        }

        private List<Expression> simplify(List<Expression> predicates, BinaryOperator<ValueDesc> op) {
            List<Expression> result = Lists.newArrayList();
            List<ValueDesc> valueDescList = Lists.newArrayList();

            for (Expression predicate : predicates) {
                ValueDesc value = predicate.accept(this, null);
                // can not build `ValueDesc`, so does not handle `predicate`, skip it.
                if (value.equals(ValueDesc.EMPTY)) {
                    result.add(predicate);
                } else if (value instanceof ResultValue) {
                    // With simplified expression, it is possible to build `ValueDesc` as well
                    ValueDesc o = value.expr.accept(this, null);
                    if (o instanceof ResultValue) {
                        result.add(o.expr);
                    } else {
                        valueDescList.add(o);
                    }
                } else {
                    valueDescList.add(value);
                }
            }
            // grouping according to `reference`, `ValueDesc` in the same group can perform intersect or union
            valueDescList.stream()
                    .collect(Collectors.groupingBy(p -> p.reference))
                    .values()
                    .forEach(v -> v.stream()
                            .reduce(op)
                            .ifPresent(desc -> {
                                if (Objects.isNull(desc.toExpression())) {
                                    result.addAll(v.stream().map(i -> i.expr).collect(Collectors.toList()));
                                } else {
                                    result.add(desc.toExpression());
                                }
                            })
                    );
            return result;
        }
    }

    private static class ValueDesc {
        public static final ValueDesc EMPTY = new ValueDesc(null, null);
        public static final ValueDesc FALSE = new ValueDesc(BooleanLiteral.FALSE, BooleanLiteral.FALSE);
        Expression expr;
        Expression reference;

        public ValueDesc(Expression reference, Expression expr) {
            this.expr = expr;
            this.reference = reference;
        }

        public ValueDesc union(ValueDesc other) {
            throw new RuntimeException("not implements union()");
        }

        public ValueDesc union(RangeValue range, DiscreteValue discrete) {
            long count = discrete.values.stream().filter(x -> range.range.test(x)).count();
            if (count == discrete.values.size()) {
                return range;
            }
            return EMPTY;
        }

        public ValueDesc intersect(ValueDesc other) {
            throw new RuntimeException("not implements intersect()");
        }

        public ValueDesc intersect(RangeValue range, DiscreteValue discrete) {
            DiscreteValue result = new DiscreteValue(discrete.reference, discrete.expr, Sets.newHashSet());
            discrete.values.stream().filter(x -> range.range.contains(x)).forEach(result.values::add);
            if (result.values.size() > 0) {
                return result;
            }
            return FALSE;
        }

        public Expression toExpression() {
            if (this.equals(FALSE)) {
                return BooleanLiteral.FALSE;
            }
            return null;
        }

        public static ValueDesc range(ComparisonPredicate predicate) {
            Literal value = (Literal) predicate.right();
            if (predicate instanceof EqualTo) {
                return new DiscreteValue(predicate.left(), predicate, Sets.newHashSet(value));
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
            Set<Literal> literals = in.getOptions().stream().map(Literal.class::cast).collect(Collectors.toSet());
            return new DiscreteValue(in.getCompareExpr(), in, literals);
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
            try {
                if (other instanceof RangeValue) {
                    RangeValue o = (RangeValue) other;
                    if (range.isConnected(o.range)) {
                        RangeValue rangeValue = new RangeValue(reference, ExpressionUtils.or(expr, other.expr));
                        rangeValue.range = range.span(o.range);
                        return rangeValue;
                    }
                    return EMPTY;
                }
                return union(this, (DiscreteValue) other);
            } catch (Exception e) {
                return EMPTY;
            }
        }

        @Override
        public ValueDesc intersect(ValueDesc other) {
            if (this.equals(FALSE) || other.equals(FALSE)) {
                return FALSE;
            }
            try {
                if (other instanceof RangeValue) {
                    RangeValue o = (RangeValue) other;
                    if (range.isConnected(o.range)) {
                        RangeValue rangeValue = new RangeValue(reference, ExpressionUtils.and(expr, other.expr));
                        rangeValue.range = range.intersection(o.range);
                        return rangeValue;
                    }
                    return FALSE;
                }
                return intersect(this, (DiscreteValue) other);
            } catch (Exception e) {
                return EMPTY;
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
            return result.isEmpty() ? null : ExpressionUtils.and(result);
        }
    }

    /**
     * use `Set` to wrap `InPredicate`
     * for example:
     * a in (1,2,3) => [1,2,3]
     */
    private static class DiscreteValue extends ValueDesc {
        Set<Literal> values;

        public DiscreteValue(Expression reference, Expression expr, Set<Literal> values) {
            super(reference, expr);
            this.values = Sets.newLinkedHashSet(values);
        }

        @Override
        public ValueDesc union(ValueDesc other) {
            try {
                if (other instanceof DiscreteValue) {
                    DiscreteValue discreteValue = new DiscreteValue(reference, ExpressionUtils.or(expr, other.expr),
                            Sets.newHashSet());
                    discreteValue.values.addAll(((DiscreteValue) other).values);
                    discreteValue.values.addAll(this.values);
                    return discreteValue;
                }
                return union((RangeValue) other, this);
            } catch (Exception e) {
                return EMPTY;
            }
        }

        @Override
        public ValueDesc intersect(ValueDesc other) {
            if (this.equals(FALSE) || other.equals(FALSE)) {
                return FALSE;
            }
            try {
                if (other instanceof DiscreteValue) {
                    DiscreteValue discreteValue = new DiscreteValue(reference, ExpressionUtils.and(expr, other.expr),
                            Sets.newHashSet());
                    discreteValue.values.addAll(((DiscreteValue) other).values);
                    discreteValue.values.retainAll(this.values);
                    return discreteValue.values.isEmpty() ? FALSE : discreteValue;
                }
                return intersect((RangeValue) other, this);
            } catch (Exception e) {
                return EMPTY;
            }
        }

        @Override
        public Expression toExpression() {
            if (values.size() == 1) {
                return new EqualTo(reference, values.iterator().next());
            } else {
                return new InPredicate(reference, Lists.newArrayList(values));
            }
        }
    }

    /**
     * Represents processing result.
     */
    private static class ResultValue extends ValueDesc {
        public ResultValue(Expression reference, Expression expr) {
            super(reference, expr);
        }

        public static ValueDesc of(Expression result) {
            return new ResultValue(result, result);
        }
    }
}
