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

import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

/**
 * collect range of expression
 */
public class RangeInference extends ExpressionVisitor<RangeInference.ValueDesc, ExpressionRewriteContext> {

    /*
     * get expression's value desc.
     */
    public ValueDesc getValue(Expression expr, ExpressionRewriteContext context) {
        return expr.accept(new RangeInference(), context);
    }

    @Override
    public ValueDesc visit(Expression expr, ExpressionRewriteContext context) {
        return new UnknownValue(context, expr);
    }

    private ValueDesc buildRange(ExpressionRewriteContext context, ComparisonPredicate predicate) {
        Expression right = predicate.child(1);
        if (right.isNullLiteral()) {
            return new UnknownValue(context, predicate);
        }
        // only handle `NumericType` and `DateLikeType`
        if (right.isLiteral() && (right.getDataType().isNumericType() || right.getDataType().isDateLikeType())) {
            return ValueDesc.range(context, predicate);
        }
        return new UnknownValue(context, predicate);
    }

    @Override
    public ValueDesc visitGreaterThan(GreaterThan greaterThan, ExpressionRewriteContext context) {
        return buildRange(context, greaterThan);
    }

    @Override
    public ValueDesc visitGreaterThanEqual(GreaterThanEqual greaterThanEqual, ExpressionRewriteContext context) {
        return buildRange(context, greaterThanEqual);
    }

    @Override
    public ValueDesc visitLessThan(LessThan lessThan, ExpressionRewriteContext context) {
        return buildRange(context, lessThan);
    }

    @Override
    public ValueDesc visitLessThanEqual(LessThanEqual lessThanEqual, ExpressionRewriteContext context) {
        return buildRange(context, lessThanEqual);
    }

    @Override
    public ValueDesc visitEqualTo(EqualTo equalTo, ExpressionRewriteContext context) {
        return buildRange(context, equalTo);
    }

    @Override
    public ValueDesc visitInPredicate(InPredicate inPredicate, ExpressionRewriteContext context) {
        // only handle `NumericType` and `DateLikeType`
        if (ExpressionUtils.isAllNonNullLiteral(inPredicate.getOptions())
                && (ExpressionUtils.matchNumericType(inPredicate.getOptions())
                || ExpressionUtils.matchDateLikeType(inPredicate.getOptions()))) {
            return ValueDesc.discrete(context, inPredicate);
        }
        return new UnknownValue(context, inPredicate);
    }

    @Override
    public ValueDesc visitAnd(And and, ExpressionRewriteContext context) {
        return simplify(context, and, ExpressionUtils.extractConjunction(and),
                ValueDesc::intersect, true);
    }

    @Override
    public ValueDesc visitOr(Or or, ExpressionRewriteContext context) {
        return simplify(context, or, ExpressionUtils.extractDisjunction(or),
                ValueDesc::union, false);
    }

    private ValueDesc simplify(ExpressionRewriteContext context,
            Expression originExpr, List<Expression> predicates,
            BinaryOperator<ValueDesc> op, boolean isAnd) {

        boolean convertIsNullToEmptyValue = isAnd && predicates.stream().anyMatch(expr -> expr instanceof NullLiteral);
        Multimap<Expression, ValueDesc> groupByReference
                = Multimaps.newListMultimap(new LinkedHashMap<>(), ArrayList::new);
        for (Expression predicate : predicates) {
            // EmptyValue(a) = IsNull(a) and null,  it doesn't equals to IsNull(a).
            // Only the and expression contains at least a null literal in its conjunctions,
            // then EmptyValue(a) can equivalent to IsNull(a).
            // so for expression and(IsNull(a), IsNull(b), ..., null), a, b can convert to EmptyValue.
            // What's more, if a is not nullable, then EmptyValue(a) always equals to IsNull(a),
            // but we don't consider this case here, we should fold IsNull(a) to FALSE using other rule.
            ValueDesc valueDesc = null;
            if (convertIsNullToEmptyValue && predicate instanceof IsNull) {
                valueDesc = new EmptyValue(context, ((IsNull) predicate).child(), predicate);
            } else {
                valueDesc = predicate.accept(this, context);
            }
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
        return new UnknownValue(context, originExpr, valuePerRefs, isAnd);
    }

    /**
     * value desc
     */
    public abstract static class ValueDesc {
        ExpressionRewriteContext context;
        Expression toExpr;
        Expression reference;

        public ValueDesc(ExpressionRewriteContext context, Expression reference, Expression toExpr) {
            this.context = context;
            this.toExpr = toExpr;
            this.reference = reference;
        }

        public Expression getReference() {
            return reference;
        }

        public Expression getOriginExpr() {
            return toExpr;
        }

        public ExpressionRewriteContext getExpressionRewriteContext() {
            return context;
        }

        public abstract ValueDesc union(ValueDesc other);

        /** or */
        public static ValueDesc union(ExpressionRewriteContext context,
                RangeValue range, DiscreteValue discrete, boolean reverseOrder) {
            long count = discrete.values.stream().filter(x -> range.range.test(x)).count();
            if (count == discrete.values.size()) {
                return range;
            }
            Expression toExpr = FoldConstantRuleOnFE.evaluate(
                    new Or(range.toExpr, discrete.toExpr), context);
            List<ValueDesc> sourceValues = reverseOrder
                    ? ImmutableList.of(discrete, range)
                    : ImmutableList.of(range, discrete);
            return new UnknownValue(context, toExpr, sourceValues, false);
        }

        /** intersect */
        public abstract ValueDesc intersect(ValueDesc other);

        /** intersect */
        public static ValueDesc intersect(ExpressionRewriteContext context, RangeValue range, DiscreteValue discrete) {
            DiscreteValue result = new DiscreteValue(context, discrete.reference, discrete.toExpr);
            discrete.values.stream().filter(x -> range.range.contains(x)).forEach(result.values::add);
            if (!result.values.isEmpty()) {
                return result;
            }
            Expression originExpr = FoldConstantRuleOnFE.evaluate(new And(range.toExpr, discrete.toExpr), context);
            return new EmptyValue(context, range.reference, originExpr);
        }

        private static ValueDesc range(ExpressionRewriteContext context, ComparisonPredicate predicate) {
            Literal value = (Literal) predicate.right();
            if (predicate instanceof EqualTo) {
                return new DiscreteValue(context, predicate.left(), predicate, value);
            }
            Range<Literal> range = null;
            if (predicate instanceof GreaterThanEqual) {
                range = Range.atLeast(value);
            } else if (predicate instanceof GreaterThan) {
                range = Range.greaterThan(value);
            } else if (predicate instanceof LessThanEqual) {
                range = Range.atMost(value);
            } else if (predicate instanceof LessThan) {
                range = Range.lessThan(value);
            }

            return new RangeValue(context, predicate.left(), predicate, range);
        }

        public static ValueDesc discrete(ExpressionRewriteContext context, InPredicate in) {
            // Set<Literal> literals = (Set) Utils.fastToImmutableSet(in.getOptions());
            Set<Literal> literals = in.getOptions().stream().map(Literal.class::cast).collect(Collectors.toSet());
            return new DiscreteValue(context, in.getCompareExpr(), in, literals);
        }
    }

    /**
     * empty range
     */
    public static class EmptyValue extends ValueDesc {

        public EmptyValue(ExpressionRewriteContext context, Expression reference, Expression toExpr) {
            super(context, reference, toExpr);
        }

        @Override
        public ValueDesc union(ValueDesc other) {
            return other;
        }

        @Override
        public ValueDesc intersect(ValueDesc other) {
            return this;
        }
    }

    /**
     * use @see com.google.common.collect.Range to wrap `ComparisonPredicate`
     * for example:
     * a > 1 => (1...+∞)
     */
    public static class RangeValue extends ValueDesc {
        Range<Literal> range;

        public RangeValue(ExpressionRewriteContext context, Expression reference,
                Expression toExpr, Range<Literal> range) {
            super(context, reference, toExpr);
            this.range = range;
        }

        public Range<Literal> getRange() {
            return range;
        }

        @Override
        public ValueDesc union(ValueDesc other) {
            if (other instanceof EmptyValue) {
                return other.union(this);
            }
            if (other instanceof RangeValue) {
                Expression originExpr = FoldConstantRuleOnFE.evaluate(new Or(toExpr, other.toExpr), context);
                RangeValue o = (RangeValue) other;
                if (range.isConnected(o.range)) {
                    return new RangeValue(context, reference, originExpr, range.span(o.range));
                }
                return new UnknownValue(context, originExpr,
                        ImmutableList.of(this, other), false);
            }
            if (other instanceof DiscreteValue) {
                return union(context, this, (DiscreteValue) other, false);
            }
            Expression originExpr = FoldConstantRuleOnFE.evaluate(new Or(toExpr, other.toExpr), context);
            return new UnknownValue(context, originExpr,
                    ImmutableList.of(this, other), false);
        }

        @Override
        public ValueDesc intersect(ValueDesc other) {
            if (other instanceof EmptyValue) {
                return other.intersect(this);
            }
            if (other instanceof RangeValue) {
                Expression originExpr = FoldConstantRuleOnFE.evaluate(new And(toExpr, other.toExpr), context);
                RangeValue o = (RangeValue) other;
                if (range.isConnected(o.range)) {
                    return new RangeValue(context, reference, originExpr, range.intersection(o.range));
                }
                return new EmptyValue(context, reference, originExpr);
            }
            if (other instanceof DiscreteValue) {
                return intersect(context, this, (DiscreteValue) other);
            }
            Expression originExpr = FoldConstantRuleOnFE.evaluate(new And(toExpr, other.toExpr), context);
            return new UnknownValue(context, originExpr,
                    ImmutableList.of(this, other), true);
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
    public static class DiscreteValue extends ValueDesc {
        Set<Literal> values;

        public DiscreteValue(ExpressionRewriteContext context,
                Expression reference, Expression toExpr, Literal... values) {
            this(context, reference, toExpr, Arrays.asList(values));
        }

        public DiscreteValue(ExpressionRewriteContext context,
                Expression reference, Expression toExpr, Collection<Literal> values) {
            super(context, reference, toExpr);
            this.values = Sets.newTreeSet(values);
        }

        public Set<Literal> getValues() {
            return values;
        }

        @Override
        public ValueDesc union(ValueDesc other) {
            if (other instanceof EmptyValue) {
                return other.union(this);
            }
            if (other instanceof DiscreteValue) {
                Expression originExpr = FoldConstantRuleOnFE.evaluate(
                        ExpressionUtils.or(toExpr, other.toExpr), context);
                DiscreteValue discreteValue = new DiscreteValue(context, reference, originExpr);
                discreteValue.values.addAll(((DiscreteValue) other).values);
                discreteValue.values.addAll(this.values);
                return discreteValue;
            }
            if (other instanceof RangeValue) {
                return union(context, (RangeValue) other, this, true);
            }
            Expression originExpr = FoldConstantRuleOnFE.evaluate(
                    ExpressionUtils.or(toExpr, other.toExpr), context);
            return new UnknownValue(context, originExpr,
                    ImmutableList.of(this, other), false);
        }

        @Override
        public ValueDesc intersect(ValueDesc other) {
            if (other instanceof EmptyValue) {
                return other.intersect(this);
            }
            if (other instanceof DiscreteValue) {
                Expression originExpr = FoldConstantRuleOnFE.evaluate(
                        ExpressionUtils.and(toExpr, other.toExpr), context);
                DiscreteValue discreteValue = new DiscreteValue(context, reference, originExpr);
                discreteValue.values.addAll(((DiscreteValue) other).values);
                discreteValue.values.retainAll(this.values);
                if (discreteValue.values.isEmpty()) {
                    return new EmptyValue(context, reference, originExpr);
                } else {
                    return discreteValue;
                }
            }
            if (other instanceof RangeValue) {
                return intersect(context, (RangeValue) other, this);
            }
            Expression originExpr = FoldConstantRuleOnFE.evaluate(
                    ExpressionUtils.and(toExpr, other.toExpr), context);
            return new UnknownValue(context, originExpr,
                    ImmutableList.of(this, other), true);
        }

        @Override
        public String toString() {
            return values.toString();
        }
    }

    /**
     * Represents processing result.
     */
    public static class UnknownValue extends ValueDesc {
        private final List<ValueDesc> sourceValues;
        private final boolean isAnd;

        private UnknownValue(ExpressionRewriteContext context, Expression expr) {
            super(context, expr, expr);
            sourceValues = ImmutableList.of();
            isAnd = false;
        }

        public UnknownValue(ExpressionRewriteContext context, Expression toExpr,
                List<ValueDesc> sourceValues, boolean isAnd) {
            super(context, getReference(sourceValues, toExpr), toExpr);
            this.sourceValues = ImmutableList.copyOf(sourceValues);
            this.isAnd = isAnd;
        }

        // reference is used to simplify multiple ValueDescs.
        // when ValueDesc A op ValueDesc B, only A and B's references equals,
        // can reduce them, like A op B = A.
        // If A and B's reference not equal, A op B will always get UnknownValue(A op B).
        //
        // for example:
        // 1. RangeValue(a < 10, reference=a) union RangeValue(a > 20, reference=a)
        //    = UnknownValue1(a < 10 or a > 20, reference=a)
        // 2. RangeValue(a < 10, reference=a) union RangeValue(b > 20, reference=b)
        //    = UnknownValue2(a < 10 or b > 20, reference=(a < 10 or b > 20))
        // then given EmptyValue(, reference=a) E,
        // 1. since E and UnknownValue1's reference equals, then
        //    E union UnknownValue1 = E.union(UnknownValue1) = UnknownValue1,
        // 2. since E and UnknownValue2's reference not equals, then
        //    E union UnknownValue2 = UnknownValue3(E union UnknownValue2, reference=E union UnknownValue2)
        private static Expression getReference(List<ValueDesc> sourceValues, Expression toExpr) {
            Expression reference = sourceValues.get(0).reference;
            for (int i = 1; i < sourceValues.size(); i++) {
                if (!reference.equals(sourceValues.get(i).reference)) {
                    return toExpr;
                }
            }
            return reference;
        }

        public List<ValueDesc> getSourceValues() {
            return sourceValues;
        }

        public boolean isAnd() {
            return this.isAnd;
        }

        @Override
        public ValueDesc union(ValueDesc other) {
            // for RangeValue/DiscreteValue/UnknownValue, when union with EmptyValue,
            // call EmptyValue.union(this) => this
            if (other instanceof EmptyValue) {
                return other.union(this);
            }
            Expression originExpr = FoldConstantRuleOnFE.evaluate(
                    ExpressionUtils.or(toExpr, other.toExpr), context);
            return new UnknownValue(context, originExpr,
                    ImmutableList.of(this, other), false);
        }

        @Override
        public ValueDesc intersect(ValueDesc other) {
            // for RangeValue/DiscreteValue/UnknownValue, when intersect with EmptyValue,
            // call EmptyValue.intersect(this) => EmptyValue
            if (other instanceof EmptyValue) {
                return other.intersect(this);
            }
            Expression originExpr = FoldConstantRuleOnFE.evaluate(
                    ExpressionUtils.and(toExpr, other.toExpr), context);
            return new UnknownValue(context, originExpr,
                    ImmutableList.of(this, other), true);
        }
    }
}
