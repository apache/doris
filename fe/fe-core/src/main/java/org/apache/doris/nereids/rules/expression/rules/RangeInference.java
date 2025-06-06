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
import org.apache.doris.nereids.trees.expressions.literal.ComparableLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeRangeSet;

import java.util.ArrayList;
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
        return expr.accept(this, context);
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
        if (right instanceof ComparableLiteral
                && (right.getDataType().isNumericType() || right.getDataType().isDateLikeType())) {
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
        if (inPredicate.getOptions().size() <= InPredicateDedup.REWRITE_OPTIONS_MAX_SIZE
                && ExpressionUtils.isAllNonNullComparableLiteral(inPredicate.getOptions())
                && (ExpressionUtils.matchNumericType(inPredicate.getOptions())
                || ExpressionUtils.matchDateLikeType(inPredicate.getOptions()))) {
            return ValueDesc.discrete(context, inPredicate);
        }
        return new UnknownValue(context, inPredicate);
    }

    @Override
    public ValueDesc visitAnd(And and, ExpressionRewriteContext context) {
        return simplify(context, ExpressionUtils.extractConjunction(and),
                ValueDesc::intersect, true);
    }

    @Override
    public ValueDesc visitOr(Or or, ExpressionRewriteContext context) {
        return simplify(context, ExpressionUtils.extractDisjunction(or),
                ValueDesc::union, false);
    }

    private ValueDesc simplify(ExpressionRewriteContext context, List<Expression> predicates,
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
                valueDesc = new EmptyValue(context, ((IsNull) predicate).child());
            } else {
                valueDesc = predicate.accept(this, context);
            }
            List<ValueDesc> valueDescs = (List<ValueDesc>) groupByReference.get(valueDesc.reference);
            valueDescs.add(valueDesc);
        }

        List<ValueDesc> valuePerRefs = Lists.newArrayList();
        for (Entry<Expression, Collection<ValueDesc>> referenceValues : groupByReference.asMap().entrySet()) {
            Expression reference = referenceValues.getKey();
            List<ValueDesc> valuePerReference = (List) referenceValues.getValue();
            if (!isAnd) {
                valuePerReference = ValueDesc.unionDiscreteAndRange(context, reference, valuePerReference);
            }

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
        return new UnknownValue(context, valuePerRefs, isAnd);
    }

    /**
     * value desc
     */
    public abstract static class ValueDesc {
        ExpressionRewriteContext context;
        Expression reference;

        public ValueDesc(ExpressionRewriteContext context, Expression reference) {
            this.context = context;
            this.reference = reference;
        }

        public Expression getReference() {
            return reference;
        }

        public ExpressionRewriteContext getExpressionRewriteContext() {
            return context;
        }

        public abstract ValueDesc union(ValueDesc other);

        /** or */
        public static ValueDesc union(ExpressionRewriteContext context,
                RangeValue range, DiscreteValue discrete, boolean reverseOrder) {
            if (discrete.values.stream().allMatch(x -> range.range.test(x))) {
                return range;
            }
            List<ValueDesc> sourceValues = reverseOrder
                    ? ImmutableList.of(discrete, range)
                    : ImmutableList.of(range, discrete);
            return new UnknownValue(context, sourceValues, false);
        }

        /** merge discrete and ranges only, no merge other value desc */
        public static List<ValueDesc> unionDiscreteAndRange(ExpressionRewriteContext context,
                Expression reference, List<ValueDesc> valueDescs) {
            // Since in-predicate's options is a list, the discrete values need to kept options' order.
            // If not keep options' order, the result in-predicate's option list will not equals to
            // the input in-predicate, later nereids will need to simplify the new in-predicate,
            // then cause dead loop.
            Set<ComparableLiteral> discreteValues = Sets.newLinkedHashSet();
            for (ValueDesc valueDesc : valueDescs) {
                if (valueDesc instanceof DiscreteValue) {
                    discreteValues.addAll(((DiscreteValue) valueDesc).getValues());
                }
            }

            // for 'a > 8 or a = 8', then range (8, +00) can convert to [8, +00)
            RangeSet<ComparableLiteral> rangeSet = TreeRangeSet.create();
            for (ValueDesc valueDesc : valueDescs) {
                if (valueDesc instanceof RangeValue) {
                    Range<ComparableLiteral> range = ((RangeValue) valueDesc).range;
                    rangeSet.add(range);
                    if (range.hasLowerBound()
                            && range.lowerBoundType() == BoundType.OPEN
                            && discreteValues.contains(range.lowerEndpoint())) {
                        rangeSet.add(Range.singleton(range.lowerEndpoint()));
                    }
                    if (range.hasUpperBound()
                            && range.upperBoundType() == BoundType.OPEN
                            && discreteValues.contains(range.upperEndpoint())) {
                        rangeSet.add(Range.singleton(range.upperEndpoint()));
                    }
                }
            }

            if (!rangeSet.isEmpty()) {
                discreteValues.removeIf(x -> rangeSet.contains(x));
            }

            List<ValueDesc> result = Lists.newArrayListWithExpectedSize(valueDescs.size());
            if (!discreteValues.isEmpty()) {
                result.add(new DiscreteValue(context, reference, discreteValues));
            }
            for (Range<ComparableLiteral> range : rangeSet.asRanges()) {
                result.add(new RangeValue(context, reference, range));
            }
            for (ValueDesc valueDesc : valueDescs) {
                if (!(valueDesc instanceof DiscreteValue) && !(valueDesc instanceof RangeValue)) {
                    result.add(valueDesc);
                }
            }

            return result;
        }

        /** intersect */
        public abstract ValueDesc intersect(ValueDesc other);

        /** intersect */
        public static ValueDesc intersect(ExpressionRewriteContext context, RangeValue range, DiscreteValue discrete) {
            // Since in-predicate's options is a list, the discrete values need to kept options' order.
            // If not keep options' order, the result in-predicate's option list will not equals to
            // the input in-predicate, later nereids will need to simplify the new in-predicate,
            // then cause dead loop.
            Set<ComparableLiteral> newValues = discrete.values.stream().filter(x -> range.range.contains(x))
                    .collect(Collectors.toCollection(
                            () -> Sets.newLinkedHashSetWithExpectedSize(discrete.values.size())));
            if (newValues.isEmpty()) {
                return new EmptyValue(context, range.reference);
            } else {
                return new DiscreteValue(context, range.reference, newValues);
            }
        }

        private static ValueDesc range(ExpressionRewriteContext context, ComparisonPredicate predicate) {
            ComparableLiteral value = (ComparableLiteral) predicate.right();
            if (predicate instanceof EqualTo) {
                return new DiscreteValue(context, predicate.left(), Sets.newHashSet(value));
            }
            Range<ComparableLiteral> range = null;
            if (predicate instanceof GreaterThanEqual) {
                range = Range.atLeast(value);
            } else if (predicate instanceof GreaterThan) {
                range = Range.greaterThan(value);
            } else if (predicate instanceof LessThanEqual) {
                range = Range.atMost(value);
            } else if (predicate instanceof LessThan) {
                range = Range.lessThan(value);
            }

            return new RangeValue(context, predicate.left(), range);
        }

        private static ValueDesc discrete(ExpressionRewriteContext context, InPredicate in) {
            // Since in-predicate's options is a list, the discrete values need to kept options' order.
            // If not keep options' order, the result in-predicate's option list will not equals to
            // the input in-predicate, later nereids will need to simplify the new in-predicate,
            // then cause dead loop.
            // Set<ComparableLiteral> literals = (Set) Utils.fastToImmutableSet(in.getOptions());
            Set<ComparableLiteral> literals = in.getOptions().stream()
                    .map(ComparableLiteral.class::cast)
                    .collect(Collectors.toCollection(
                            () -> Sets.newLinkedHashSetWithExpectedSize(in.getOptions().size())));
            return new DiscreteValue(context, in.getCompareExpr(), literals);
        }
    }

    /**
     * empty range
     */
    public static class EmptyValue extends ValueDesc {

        public EmptyValue(ExpressionRewriteContext context, Expression reference) {
            super(context, reference);
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
        Range<ComparableLiteral> range;

        public RangeValue(ExpressionRewriteContext context, Expression reference, Range<ComparableLiteral> range) {
            super(context, reference);
            this.range = range;
        }

        public Range<ComparableLiteral> getRange() {
            return range;
        }

        @Override
        public ValueDesc union(ValueDesc other) {
            if (other instanceof EmptyValue) {
                return other.union(this);
            }
            if (other instanceof RangeValue) {
                RangeValue o = (RangeValue) other;
                if (range.isConnected(o.range)) {
                    return new RangeValue(context, reference, range.span(o.range));
                }
                return new UnknownValue(context, ImmutableList.of(this, other), false);
            }
            if (other instanceof DiscreteValue) {
                return union(context, this, (DiscreteValue) other, false);
            }
            return new UnknownValue(context, ImmutableList.of(this, other), false);
        }

        @Override
        public ValueDesc intersect(ValueDesc other) {
            if (other instanceof EmptyValue) {
                return other.intersect(this);
            }
            if (other instanceof RangeValue) {
                RangeValue o = (RangeValue) other;
                if (range.isConnected(o.range)) {
                    Range<ComparableLiteral> newRange = range.intersection(o.range);
                    if (!newRange.isEmpty()) {
                        if (newRange.hasLowerBound() && newRange.hasUpperBound()
                                && newRange.lowerEndpoint().compareTo(newRange.upperEndpoint()) == 0
                                && newRange.lowerBoundType() == BoundType.CLOSED
                                && newRange.lowerBoundType() == BoundType.CLOSED) {
                            return new DiscreteValue(context, reference, Sets.newHashSet(newRange.lowerEndpoint()));
                        } else {
                            return new RangeValue(context, reference, newRange);
                        }
                    }
                }
                return new EmptyValue(context, reference);
            }
            if (other instanceof DiscreteValue) {
                return intersect(context, this, (DiscreteValue) other);
            }
            return new UnknownValue(context, ImmutableList.of(this, other), true);
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
        final Set<ComparableLiteral> values;

        public DiscreteValue(ExpressionRewriteContext context,
                Expression reference, Set<ComparableLiteral> values) {
            super(context, reference);
            this.values = values;
        }

        public Set<ComparableLiteral> getValues() {
            return values;
        }

        @Override
        public ValueDesc union(ValueDesc other) {
            if (other instanceof EmptyValue) {
                return other.union(this);
            }
            if (other instanceof DiscreteValue) {
                Set<ComparableLiteral> newValues = Sets.newLinkedHashSet();
                newValues.addAll(((DiscreteValue) other).values);
                newValues.addAll(this.values);
                return new DiscreteValue(context, reference, newValues);
            }
            if (other instanceof RangeValue) {
                return union(context, (RangeValue) other, this, true);
            }
            return new UnknownValue(context, ImmutableList.of(this, other), false);
        }

        @Override
        public ValueDesc intersect(ValueDesc other) {
            if (other instanceof EmptyValue) {
                return other.intersect(this);
            }
            if (other instanceof DiscreteValue) {
                Set<ComparableLiteral> newValues = Sets.newLinkedHashSet();
                newValues.addAll(this.values);
                newValues.retainAll(((DiscreteValue) other).values);
                if (newValues.isEmpty()) {
                    return new EmptyValue(context, reference);
                } else {
                    return new DiscreteValue(context, reference, newValues);
                }
            }
            if (other instanceof RangeValue) {
                return intersect(context, (RangeValue) other, this);
            }
            return new UnknownValue(context, ImmutableList.of(this, other), true);
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
            super(context, expr);
            sourceValues = ImmutableList.of();
            isAnd = false;
        }

        private UnknownValue(ExpressionRewriteContext context,
                List<ValueDesc> sourceValues, boolean isAnd) {
            super(context, getReference(context, sourceValues, isAnd));
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
        private static Expression getReference(ExpressionRewriteContext context,
                List<ValueDesc> sourceValues, boolean isAnd) {
            Expression reference = sourceValues.get(0).reference;
            for (int i = 1; i < sourceValues.size(); i++) {
                if (!reference.equals(sourceValues.get(i).reference)) {
                    return SimplifyRange.INSTANCE.getExpression(context, sourceValues, isAnd);
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
            return new UnknownValue(context, ImmutableList.of(this, other), false);
        }

        @Override
        public ValueDesc intersect(ValueDesc other) {
            // for RangeValue/DiscreteValue/UnknownValue, when intersect with EmptyValue,
            // call EmptyValue.intersect(this) => EmptyValue
            if (other instanceof EmptyValue) {
                return other.intersect(this);
            }
            return new UnknownValue(context, ImmutableList.of(this, other), true);
        }
    }
}
