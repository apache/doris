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
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.literal.ComparableLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeRangeSet;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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
        // only handle `NumericType` and `DateLikeType` and `StringLikeType`
        DataType rightDataType = right.getDataType();
        if (right instanceof ComparableLiteral
                && !right.isNullLiteral()
                && (rightDataType.isNumericType() || rightDataType.isDateLikeType()
                    || rightDataType.isStringLikeType())) {
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
        } else {
            return new UnknownValue(context, predicate);
        }
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
            Set<ComparableLiteral> values = Sets.newLinkedHashSetWithExpectedSize(inPredicate.getOptions().size());
            for (Expression value : inPredicate.getOptions()) {
                values.add((ComparableLiteral) value);
            }
            return new DiscreteValue(context, inPredicate.getCompareExpr(), values);
        } else {
            return new UnknownValue(context, inPredicate);
        }
    }

    @Override
    public ValueDesc visitNot(Not not, ExpressionRewriteContext context) {
        ValueDesc childValue = not.child().accept(this, context);
        if (childValue instanceof DiscreteValue) {
            return new NotDiscreteValue(context, childValue.getReference(), ((DiscreteValue) childValue).values);
        } else {
            return new UnknownValue(context, not);
        }
    }

    @Override
    public ValueDesc visitAnd(And and, ExpressionRewriteContext context) {
        return simplify(context, ExpressionUtils.extractConjunction(and), true);
    }

    @Override
    public ValueDesc visitOr(Or or, ExpressionRewriteContext context) {
        return simplify(context, ExpressionUtils.extractDisjunction(or), false);
    }

    private ValueDesc simplify(ExpressionRewriteContext context, List<Expression> predicates, boolean isAnd) {
        boolean convertIsNullToEmptyValue = isAnd && predicates.stream().anyMatch(expr -> expr instanceof NullLiteral);
        Map<Expression, ValueDescCollector> groupByReference = Maps.newLinkedHashMap();
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
            Expression reference = valueDesc.reference;
            groupByReference.computeIfAbsent(reference, key -> new ValueDescCollector()).add(valueDesc);
        }

        List<ValueDesc> valuePerRefs = Lists.newArrayList();
        for (Entry<Expression, ValueDescCollector> referenceValues : groupByReference.entrySet()) {
            Expression reference = referenceValues.getKey();
            ValueDescCollector collector = referenceValues.getValue();
            ValueDesc mergedValue;
            if (isAnd) {
                mergedValue = intersectForSameReference(context, reference, collector);
            } else {
                mergedValue = unionForSameReference(context, reference, collector);
            }
            valuePerRefs.add(mergedValue);
        }

        if (valuePerRefs.size() == 1) {
            return valuePerRefs.get(0);
        }

        return new CompoundValue(context, valuePerRefs, isAnd);
    }

    private ValueDesc intersectForSameReference(ExpressionRewriteContext context, Expression reference,
            ValueDescCollector collector) {
        if (collector.hasEmptyValue) {
            return new EmptyValue(context, reference);
        }

        ImmutableList.Builder<ValueDesc> result = ImmutableList.builder();

        RangeValue mergeRangeValue = null;
        if (!collector.rangeValues.isEmpty()) {
            for (RangeValue rangeValue : collector.rangeValues) {
                if (mergeRangeValue == null) {
                    mergeRangeValue = rangeValue;
                } else {
                    ValueDesc combineValue = mergeRangeValue.intersect(rangeValue);
                    if (combineValue instanceof EmptyValue) {
                        return combineValue;
                    } else if (combineValue instanceof RangeValue) {
                        mergeRangeValue = (RangeValue) combineValue;
                    } else {
                        collector.add(combineValue);
                    }
                }
            }
        }

        Set<ComparableLiteral> discreteValues = Sets.newLinkedHashSet();
        DiscreteValue mergeDiscreteValue = null;
        if (!collector.discreteValues.isEmpty()) {
            discreteValues.addAll(collector.discreteValues.get(0).values);
            for (int i = 1; i < collector.discreteValues.size(); i++) {
                discreteValues.retainAll(collector.discreteValues.get(i).values);
            }
            if (discreteValues.isEmpty()) {
                return new EmptyValue(context, reference);
            }
            mergeDiscreteValue = new DiscreteValue(context, reference, discreteValues);
        }

        ValueDesc mergeRangeDiscreteValue = null;
        if (mergeRangeValue != null && mergeDiscreteValue != null) {
            mergeRangeDiscreteValue = mergeRangeValue.intersect(mergeDiscreteValue);
        } else if (mergeRangeValue != null) {
            mergeRangeDiscreteValue = mergeRangeValue;
        } else if (mergeDiscreteValue != null) {
            mergeRangeDiscreteValue = mergeDiscreteValue;
        }
        if (mergeRangeDiscreteValue instanceof EmptyValue) {
            return mergeRangeDiscreteValue;
        } else if (mergeRangeDiscreteValue != null) {
            result.add(mergeRangeDiscreteValue);
        }

        if (!collector.notDiscreteValues.isEmpty()) {
            Set<ComparableLiteral> mergeNotDiscreteValues = Sets.newLinkedHashSet();
            for (NotDiscreteValue notDiscreteValue : collector.notDiscreteValues) {
                mergeNotDiscreteValues.addAll(notDiscreteValue.values);
            }
            if (mergeRangeDiscreteValue != null) {
                final ValueDesc inValue = mergeRangeDiscreteValue;
                mergeNotDiscreteValues.removeIf(value -> {
                    if (inValue instanceof DiscreteValue) {
                        return !((DiscreteValue) inValue).values.contains(value);
                    } else if (inValue instanceof RangeValue) {
                        return !((RangeValue) inValue).range.contains(value);
                    } else {
                        return false;
                    }
                });
            }
            if (!mergeNotDiscreteValues.isEmpty()) {
                result.add(new NotDiscreteValue(context, reference, mergeNotDiscreteValues));
            }
        }
        result.addAll(collector.compoundValues);
        result.addAll(collector.unknownValues);

        List<ValueDesc> resultValues = result.build();
        if (resultValues.isEmpty()) {
            Preconditions.checkArgument(collector.hasEmptyValue);
            return new EmptyValue(context, reference);
        } else if (resultValues.size() == 1) {
            return resultValues.get(0);
        } else {
            return new CompoundValue(context, resultValues, true);
        }
    }

    private ValueDesc unionForSameReference(ExpressionRewriteContext context, Expression reference,
            ValueDescCollector collector) {
        ImmutableList.Builder<ValueDesc> result = ImmutableList.builderWithExpectedSize(collector.size());
        // Since in-predicate's options is a list, the discrete values need to kept options' order.
        // If not keep options' order, the result in-predicate's option list will not equals to
        // the input in-predicate, later nereids will need to simplify the new in-predicate,
        // then cause dead loop.
        Set<ComparableLiteral> discreteValues = Sets.newLinkedHashSet();
        for (DiscreteValue discreteValue : collector.discreteValues) {
            discreteValues.addAll(discreteValue.values);
        }

        // for 'a > 8 or a = 8', then range (8, +00) can convert to [8, +00)
        RangeSet<ComparableLiteral> rangeSet = TreeRangeSet.create();
        for (RangeValue rangeValue : collector.rangeValues) {
            Range<ComparableLiteral> range = rangeValue.range;
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

        if (!rangeSet.isEmpty()) {
            discreteValues.removeIf(x -> rangeSet.contains(x));
        }

        if (!discreteValues.isEmpty()) {
            result.add(new DiscreteValue(context, reference, discreteValues));
        }
        for (Range<ComparableLiteral> range : rangeSet.asRanges()) {
            result.add(new RangeValue(context, reference, range));
        }

        if (!collector.notDiscreteValues.isEmpty()) {
            Set<ComparableLiteral> mergeNotDiscreteValues = Sets.newLinkedHashSet();
            mergeNotDiscreteValues.addAll(collector.notDiscreteValues.get(0).values);
            for (int i = 1; i < collector.notDiscreteValues.size(); i++) {
                mergeNotDiscreteValues.retainAll(collector.notDiscreteValues.get(i).values);
            }
            mergeNotDiscreteValues.removeIf(
                    value -> discreteValues.contains(value) || rangeSet.contains(value));
            if (mergeNotDiscreteValues.isEmpty()) {
                return new RangeValue(context, reference, Range.all());
            }
            result.add(new NotDiscreteValue(context, reference, mergeNotDiscreteValues));
        }

        result.addAll(collector.compoundValues);
        result.addAll(collector.unknownValues);

        List<ValueDesc> resultValues = result.build();
        if (resultValues.isEmpty()) {
            Preconditions.checkArgument(collector.hasEmptyValue);
            return new EmptyValue(context, reference);
        } else if (resultValues.size() == 1) {
            return resultValues.get(0);
        } else {
            return new CompoundValue(context, resultValues, false);
        }
    }

    /** value desc visitor */
    public interface ValueDescVisitor<R, C> {
        R visitEmptyValue(EmptyValue emptyValue, C context);

        R visitRangeValue(RangeValue rangeValue, C context);

        R visitDiscreteValue(DiscreteValue discreteValue, C context);

        R visitNotDiscreteValue(NotDiscreteValue notDiscreteValue, C context);

        R visitCompoundValue(CompoundValue compoundValue, C context);

        R visitUnknownValue(UnknownValue unknownValue, C context);
    }

    private static class ValueDescCollector {
        boolean hasEmptyValue = false;
        List<RangeValue> rangeValues = Lists.newArrayList();
        List<DiscreteValue> discreteValues = Lists.newArrayList();
        List<NotDiscreteValue> notDiscreteValues = Lists.newArrayList();
        List<CompoundValue> compoundValues = Lists.newArrayList();
        List<UnknownValue> unknownValues = Lists.newArrayList();

        void add(ValueDesc value) {
            if (value instanceof EmptyValue) {
                hasEmptyValue = true;
            } else if (value instanceof RangeValue) {
                rangeValues.add((RangeValue) value);
            } else if (value instanceof DiscreteValue) {
                discreteValues.add((DiscreteValue) value);
            } else if (value instanceof NotDiscreteValue) {
                notDiscreteValues.add((NotDiscreteValue) value);
            } else if (value instanceof CompoundValue) {
                compoundValues.add((CompoundValue) value);
            } else {
                Preconditions.checkArgument(value instanceof UnknownValue);
                unknownValues.add((UnknownValue) value);
            }
        }

        int size() {
            return rangeValues.size() + discreteValues.size() + compoundValues.size() + unknownValues.size();
        }
    }

    /**
     * value desc
     */
    public abstract static class ValueDesc {
        final ExpressionRewriteContext context;
        final Expression reference;

        public ValueDesc(ExpressionRewriteContext context, Expression reference) {
            this.context = context;
            this.reference = reference;
        }

        public ExpressionRewriteContext getExpressionRewriteContext() {
            return context;
        }

        public Expression getReference() {
            return reference;
        }

        public <R, C> R accept(ValueDescVisitor<R, C> visitor, C context) {
            return visit(visitor, context);
        }

        protected abstract <R, C> R visit(ValueDescVisitor<R, C> visitor, C context);
    }

    /**
     * empty range
     */
    public static class EmptyValue extends ValueDesc {

        public EmptyValue(ExpressionRewriteContext context, Expression reference) {
            super(context, reference);
        }

        @Override
        protected <R, C> R visit(ValueDescVisitor<R, C> visitor, C context) {
            return visitor.visitEmptyValue(this, context);
        }
    }

    /**
     * use @see com.google.common.collect.Range to wrap `ComparisonPredicate`
     * for example:
     * a > 1 => (1...+∞)
     */
    public static class RangeValue extends ValueDesc {

        final Range<ComparableLiteral> range;

        public RangeValue(ExpressionRewriteContext context, Expression reference, Range<ComparableLiteral> range) {
            super(context, reference);
            this.range = range;
        }

        public Range<ComparableLiteral> getRange() {
            return range;
        }

        @Override
        protected <R, C> R visit(ValueDescVisitor<R, C> visitor, C context) {
            return visitor.visitRangeValue(this, context);
        }

        private ValueDesc intersect(RangeValue other) {
            if (range.isConnected(other.range)) {
                Range<ComparableLiteral> newRange = range.intersection(other.range);
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

        private ValueDesc intersect(DiscreteValue other) {
            Set<ComparableLiteral> intersectValues = Sets.newLinkedHashSetWithExpectedSize(other.values.size());
            for (ComparableLiteral value : other.values) {
                if (range.contains(value)) {
                    intersectValues.add(value);
                }
            }
            if (intersectValues.isEmpty()) {
                return new EmptyValue(context, reference);
            } else {
                return new DiscreteValue(context, reference, intersectValues);
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
        protected <R, C> R visit(ValueDescVisitor<R, C> visitor, C context) {
            return visitor.visitDiscreteValue(this, context);
        }

        @Override
        public String toString() {
            return values.toString();
        }
    }

    /**
     * use `Set` to wrap `InPredicate`
     * for example:
     * a not in (1,2,3) => [1,2,3]
     */
    public static class NotDiscreteValue extends ValueDesc {
        final Set<ComparableLiteral> values;

        public NotDiscreteValue(ExpressionRewriteContext context,
                Expression reference, Set<ComparableLiteral> values) {
            super(context, reference);
            this.values = values;
        }

        @Override
        protected <R, C> R visit(ValueDescVisitor<R, C> visitor, C context) {
            return visitor.visitNotDiscreteValue(this, context);
        }
    }

    /**
     * Represents processing compound predicate.
     */
    public static class CompoundValue extends ValueDesc {
        private final List<ValueDesc> sourceValues;
        private final boolean isAnd;

        private CompoundValue(ExpressionRewriteContext context, List<ValueDesc> sourceValues, boolean isAnd) {
            super(context, getReference(context, sourceValues, isAnd));
            this.sourceValues = ImmutableList.copyOf(sourceValues);
            this.isAnd = isAnd;
        }

        // reference is used to simplify multiple ValueDescs.
        // when ValueDesc A op ValueDesc B, only A and B's references equals,
        // can reduce them, like A op B = A.
        // If A and B's reference not equal, A op B will always get CompoundValue(A op B).
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
                    return SimplifyRange.INSTANCE.getCompoundExpression(context, sourceValues, isAnd);
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
        protected <R, C> R visit(ValueDescVisitor<R, C> visitor, C context) {
            return visitor.visitCompoundValue(this, context);
        }
    }

    /**
     * Represents unknown value expression.
     */
    public static class UnknownValue extends ValueDesc {

        private UnknownValue(ExpressionRewriteContext context, Expression expression) {
            super(context, expression);
        }

        @Override
        protected <R, C> R visit(ValueDescVisitor<R, C> visitor, C context) {
            return visitor.visitUnknownValue(this, context);
        }
    }
}
