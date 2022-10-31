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
 * a in (1,2,3) and a > 1 => a in (1,2,3)
 * a in (1,2,3) and a in (3,4,5) => a = 3
 * a in(1,2,3) and a in (4,5,6) => a in(1,2,3) and a in (4,5,6)
 * The logic is as follows:
 * 1. for `And` expression.
 *    1. extract conjunctions then build `ValueDesc` for each conjunction
 *    2. grouping according to `reference`, `ValueDesc` in the same group can perform intersect
 *    for example:
 *    a > 1 and a > 2
 *    1. a > 1 => RangeValueDesc((1...+∞)), a > 2 => RangeValueDesc((2...+∞))
 *    2. (1...+∞) intersect (2...+∞) => (2...+∞)
 * 2. for `Or` expression (same as `And`).
 */
public class SimplifyRange extends AbstractExpressionRewriteRule {

    public static final SimplifyRange INSTANCE = new SimplifyRange();

    @Override
    public Expression rewrite(Expression expr, ExpressionRewriteContext ctx) {
        return expr instanceof CompoundPredicate ? (Expression) expr.accept(new RangeInference(), null) : expr;
    }

    private static class RangeInference extends ExpressionVisitor<Object, Void> {

        @Override
        public Object visit(Expression expr, Void context) {
            return expr;
        }

        private ValueDesc buildRange(ComparisonPredicate predicate) {
            Expression rewrite = ExpressionRuleExecutor.normalize(predicate);
            Expression right = rewrite.child(1);
            // only handle `NumericType`
            if (right.isLiteral() && right.getDataType().isNumericType()) {
                return ValueDesc.range((ComparisonPredicate) rewrite);
            }
            return RangeValue.EMPTY;
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
            return DiscreteValue.EMPTY;
        }

        @Override
        public Expression visitAnd(And and, Void context) {
            List<Expression> result = simplify(ExpressionUtils.extractConjunction(and), ValueDesc::intersect);
            return ExpressionUtils.and(result);
        }

        @Override
        public Expression visitOr(Or or, Void context) {
            List<Expression> result = simplify(ExpressionUtils.extractDisjunction(or), ValueDesc::union);
            return ExpressionUtils.or(result);
        }

        private List<Expression> simplify(List<Expression> predicates, BinaryOperator<ValueDesc> op) {
            List<Expression> result = Lists.newArrayList();
            List<ValueDesc> valueDescList = Lists.newArrayList();

            for (Expression predicate : predicates) {
                Object value = predicate.accept(this, null);
                // can not build `ValueDesc`, so does not handle `predicate`
                if (value.equals(RangeValue.EMPTY) || value.equals(DiscreteValue.EMPTY)) {
                    result.add(predicate);
                } else if (value instanceof Expression) {
                    // With simplified expression, it is possible to build `ValueDesc` as well
                    Object o = ((Expression) value).accept(this, null);
                    if (o instanceof Expression) {
                        result.add((Expression) o);
                    } else {
                        valueDescList.add((ValueDesc) o);
                    }
                } else if (value instanceof ValueDesc) {
                    valueDescList.add((ValueDesc) value);
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

    private abstract static class ValueDesc {
        Expression expr;
        Expression reference;

        public ValueDesc(Expression reference, Expression expr) {
            this.expr = expr;
            this.reference = reference;
        }

        public abstract ValueDesc union(ValueDesc other);

        public ValueDesc union(RangeValue range, DiscreteValue discrete) {
            long count = discrete.values.stream().filter(x -> range.range.test(x)).count();
            if (count == discrete.values.size()) {
                return range;
            }
            return RangeValue.EMPTY;
        }

        public abstract ValueDesc intersect(ValueDesc other);

        public ValueDesc intersect(RangeValue range, DiscreteValue discrete) {
            DiscreteValue result = new DiscreteValue(discrete.reference, discrete.expr, Sets.newHashSet());
            discrete.values.stream().filter(x -> range.range.test(x)).forEach(result.values::add);
            if (result.values.size() > 0) {
                return result;
            }
            return DiscreteValue.EMPTY;
        }

        public abstract Expression toExpression();

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

        public static final RangeValue EMPTY = new RangeValue(null, null);
        Range<Literal> range;

        public RangeValue(Expression reference, Expression expr) {
            super(reference, expr);
        }

        @Override
        public ValueDesc union(ValueDesc other) {
            if (other instanceof RangeValue) {
                RangeValue rangeValue = new RangeValue(reference, ExpressionUtils.or(expr, other.expr));
                rangeValue.range = range.span(((RangeValue) other).range);
                return rangeValue;
            }
            return union(this, (DiscreteValue) other);
        }

        @Override
        public ValueDesc intersect(ValueDesc other) {
            if (other instanceof RangeValue) {
                RangeValue rangeValue = new RangeValue(reference, ExpressionUtils.and(expr, other.expr));
                rangeValue.range = range.intersection(((RangeValue) other).range);
                return rangeValue;
            }
            return intersect(this, (DiscreteValue) other);
        }

        @Override
        public Expression toExpression() {
            if (this.equals(RangeValue.EMPTY)) {
                return null;
            }
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
                    result.add(new LessThanEqual(reference, range.lowerEndpoint()));
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

        public static final DiscreteValue EMPTY = new DiscreteValue(null, null, null);

        Set<Literal> values;

        public DiscreteValue(Expression reference, Expression expr, Set<Literal> values) {
            super(reference, expr);
            this.values = values;
        }

        @Override
        public ValueDesc union(ValueDesc other) {
            if (other instanceof DiscreteValue) {
                DiscreteValue discreteValue = new DiscreteValue(reference, ExpressionUtils.or(expr, other.expr),
                        Sets.newHashSet());
                discreteValue.values.addAll(((DiscreteValue) other).values);
                discreteValue.values.addAll(this.values);
                return discreteValue;
            }
            return union((RangeValue) other, this);
        }

        @Override
        public ValueDesc intersect(ValueDesc other) {
            if (other instanceof DiscreteValue) {
                DiscreteValue discreteValue = new DiscreteValue(reference, ExpressionUtils.and(expr, other.expr),
                        Sets.newHashSet());
                discreteValue.values.addAll(((DiscreteValue) other).values);
                discreteValue.values.retainAll(this.values);
                return discreteValue.values.isEmpty() ? DiscreteValue.EMPTY : discreteValue;
            }
            return intersect((RangeValue) other, this);
        }

        @Override
        public Expression toExpression() {
            if (this.equals(DiscreteValue.EMPTY)) {
                return null;
            }
            if (values.size() == 1) {
                return new EqualTo(reference, values.iterator().next());
            } else {
                return new InPredicate(reference, Lists.newArrayList(values));
            }
        }
    }
}
