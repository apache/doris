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
import org.apache.doris.nereids.trees.expressions.literal.ComparableLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeRangeSet;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
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

    @Override
    public ValueDesc visitGreaterThan(GreaterThan greaterThan, ExpressionRewriteContext context) {
        Optional<ComparableLiteral> rightLiteral = tryGetComparableLiteral(greaterThan.right());
        if (rightLiteral.isPresent()) {
            return new RangeValue(context, greaterThan.left(), Range.greaterThan(rightLiteral.get()));
        } else {
            return new UnknownValue(context, greaterThan);
        }
    }

    @Override
    public ValueDesc visitGreaterThanEqual(GreaterThanEqual greaterThanEqual, ExpressionRewriteContext context) {
        Optional<ComparableLiteral> rightLiteral = tryGetComparableLiteral(greaterThanEqual.right());
        if (rightLiteral.isPresent()) {
            return new RangeValue(context, greaterThanEqual.left(), Range.atLeast(rightLiteral.get()));
        } else {
            return new UnknownValue(context, greaterThanEqual);
        }
    }

    @Override
    public ValueDesc visitLessThan(LessThan lessThan, ExpressionRewriteContext context) {
        Optional<ComparableLiteral> rightLiteral = tryGetComparableLiteral(lessThan.right());
        if (rightLiteral.isPresent()) {
            return new RangeValue(context, lessThan.left(), Range.lessThan(rightLiteral.get()));
        } else {
            return new UnknownValue(context, lessThan);
        }
    }

    @Override
    public ValueDesc visitLessThanEqual(LessThanEqual lessThanEqual, ExpressionRewriteContext context) {
        Optional<ComparableLiteral> rightLiteral = tryGetComparableLiteral(lessThanEqual.right());
        if (rightLiteral.isPresent()) {
            return new RangeValue(context, lessThanEqual.left(), Range.atMost(rightLiteral.get()));
        } else {
            return new UnknownValue(context, lessThanEqual);
        }
    }

    @Override
    public ValueDesc visitEqualTo(EqualTo equalTo, ExpressionRewriteContext context) {
        Optional<ComparableLiteral> rightLiteral = tryGetComparableLiteral(equalTo.right());
        if (rightLiteral.isPresent()) {
            return new DiscreteValue(context, equalTo.left(), ImmutableSet.of(rightLiteral.get()));
        } else {
            return new UnknownValue(context, equalTo);
        }
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

    private Optional<ComparableLiteral> tryGetComparableLiteral(Expression right) {
        // only handle `NumericType` and `DateLikeType` and `StringLikeType`
        DataType rightDataType = right.getDataType();
        if (right instanceof ComparableLiteral
                && !right.isNullLiteral()
                && (rightDataType.isNumericType() || rightDataType.isDateLikeType()
                || rightDataType.isStringLikeType())) {
            return Optional.of((ComparableLiteral) right);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public ValueDesc visitNot(Not not, ExpressionRewriteContext context) {
        ValueDesc childValue = not.child().accept(this, context);
        if (childValue instanceof DiscreteValue) {
            return new NotDiscreteValue(context, childValue.getReference(), ((DiscreteValue) childValue).values);
        } else if (childValue instanceof IsNullValue) {
            return new IsNotNullValue(context, childValue.getReference(), not);
        } else {
            return new UnknownValue(context, not);
        }
    }

    @Override
    public ValueDesc visitIsNull(IsNull isNull, ExpressionRewriteContext context) {
        return new IsNullValue(context, isNull.child());
    }

    @Override
    public ValueDesc visitAnd(And and, ExpressionRewriteContext context) {
        return processCompound(context, ExpressionUtils.extractConjunction(and), true);
    }

    @Override
    public ValueDesc visitOr(Or or, ExpressionRewriteContext context) {
        return processCompound(context, ExpressionUtils.extractDisjunction(or), false);
    }

    private ValueDesc processCompound(ExpressionRewriteContext context, List<Expression> predicates, boolean isAnd) {
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
                mergedValue = intersect(context, reference, collector);
            } else {
                mergedValue = union(context, reference, collector);
            }
            valuePerRefs.add(mergedValue);
        }

        if (valuePerRefs.size() == 1) {
            return valuePerRefs.get(0);
        }

        Expression reference = SimplifyRange.INSTANCE.getCompoundExpression(context, valuePerRefs, isAnd);
        return new CompoundValue(context, reference, valuePerRefs, isAnd);
    }

    private ValueDesc intersect(ExpressionRewriteContext context, Expression reference, ValueDescCollector collector) {
        ImmutableList.Builder<ValueDesc> result = ImmutableList.builder();

        // merge all the range values
        RangeValue mergeRangeValue = null;
        if (!collector.hasEmptyValue && !collector.rangeValues.isEmpty()) {
            for (RangeValue rangeValue : collector.rangeValues) {
                if (mergeRangeValue == null) {
                    mergeRangeValue = rangeValue;
                } else {
                    ValueDesc combineValue = mergeRangeValue.intersect(rangeValue);
                    if (combineValue instanceof RangeValue) {
                        mergeRangeValue = (RangeValue) combineValue;
                    } else {
                        collector.add(combineValue);
                        mergeRangeValue = null;
                        // no need to process the lefts.
                        if (combineValue instanceof EmptyValue) {
                            break;
                        }
                    }
                }
            }
        }

        // merge all the discrete values
        Set<ComparableLiteral> discreteValues = Sets.newLinkedHashSet();
        DiscreteValue mergeDiscreteValue = null;
        if (!collector.hasEmptyValue && !collector.discreteValues.isEmpty()) {
            discreteValues.addAll(collector.discreteValues.get(0).values);
            for (int i = 1; i < collector.discreteValues.size(); i++) {
                discreteValues.retainAll(collector.discreteValues.get(i).values);
            }
            if (discreteValues.isEmpty()) {
                collector.add(new EmptyValue(context, reference));
            } else {
                mergeDiscreteValue = new DiscreteValue(context, reference, discreteValues);
            }
        }

        // merge all the not discrete values
        Set<ComparableLiteral> mergeNotDiscreteValues = Sets.newLinkedHashSet();
        if (!collector.hasEmptyValue && !collector.notDiscreteValues.isEmpty()) {
            for (NotDiscreteValue notDiscreteValue : collector.notDiscreteValues) {
                mergeNotDiscreteValues.addAll(notDiscreteValue.values);
            }
            if (mergeRangeValue != null) {
                Range<ComparableLiteral> rangeValue = mergeRangeValue.range;
                mergeNotDiscreteValues.removeIf(value -> !rangeValue.contains(value));
            }
            if (mergeDiscreteValue != null) {
                Set<ComparableLiteral> values = mergeDiscreteValue.values;
                mergeNotDiscreteValues.removeIf(value -> !values.contains(value));
                Set<ComparableLiteral> newDiscreteValues = Sets.newLinkedHashSet(mergeDiscreteValue.values);
                newDiscreteValues.removeIf(mergeNotDiscreteValues::contains);
                if (newDiscreteValues.isEmpty()) {
                    collector.add(new EmptyValue(context, reference));
                }
            }
        }
        if (!collector.hasEmptyValue) {
            // merge range + discrete values
            if (mergeRangeValue != null && mergeDiscreteValue != null) {
                ValueDesc newMergeValue = mergeRangeValue.intersect(mergeDiscreteValue);
                result.add(newMergeValue);
            } else if (mergeRangeValue != null) {
                result.add(mergeRangeValue);
            } else if (mergeDiscreteValue != null) {
                result.add(mergeDiscreteValue);
            }
            if (!collector.hasEmptyValue && !mergeNotDiscreteValues.isEmpty()) {
                result.add(new NotDiscreteValue(context, reference, mergeNotDiscreteValues));
            }
        }

        // process empty value
        if (collector.hasEmptyValue) {
            if (!reference.nullable()) {
                return new UnknownValue(context, BooleanLiteral.FALSE);
            }
            result.add(new EmptyValue(context, reference));
        }

        // process is null and is not null
        // for non-nullable a: EmptyValue(a) = a is null and null
        boolean hasIsNullValue = collector.hasIsNullValue || collector.hasEmptyValue && reference.nullable();
        if (hasIsNullValue && collector.isNotNullValueOpt.isPresent()) {
            return new UnknownValue(context, BooleanLiteral.FALSE);
        }
        // nullable's EmptyValue have contains IsNull, no need to add
        if (!collector.hasEmptyValue && collector.hasIsNullValue) {
            result.add(new IsNullValue(context, reference));
        }
        if (collector.isNotNullValueOpt.isPresent()) {
            result.add(collector.isNotNullValueOpt.get());
        }
        for (CompoundValue compoundValue : collector.compoundValues) {
            // given 'EmptyValue AND x', for x:
            // 1) if x is IsNotNullValue or UnknownValue, their intersect result maybe FALSE, not EmptyValue;
            // 2) otherwise their intersect result is always EmptyValue, so can ignore this CompoundValue
            if (collector.hasEmptyValue && compoundValue.intersectWithEmptyIsEmpty()) {
                continue;
            }
            List<ValueDesc> otherValues = result.build();
            // in fact, the compoundValue's reference should be equals reference
            if (!compoundValue.isAnd && compoundValue.reference.equals(reference)) {
                ImmutableList.Builder<ValueDesc> newSourceValuesBuilder
                        = ImmutableList.builderWithExpectedSize(compoundValue.sourceValues.size());
                for (ValueDesc sourceValue : compoundValue.sourceValues) {
                    boolean containsAll = false;
                    for (ValueDesc other : otherValues) {
                        containsAll = containsAll || sourceValue.containsAll(other);
                    }
                    boolean intersectIsEmpty = collector.hasEmptyValue && sourceValue.intersectWithEmptyIsEmpty();
                    // if source value is big enough, that it contains one of the previous value desc's range,
                    // than no need it;
                    // or the source intersect with empty value is empty, than no need it too.
                    if (!containsAll && !intersectIsEmpty) {
                        newSourceValuesBuilder.add(sourceValue);
                    }
                }
                List<ValueDesc> newSourceValues = newSourceValuesBuilder.build();
                if (newSourceValues.size() == 1) {
                    result.add(newSourceValues.get(0));
                } else if (newSourceValues.size() > 1) {
                    result.add(new CompoundValue(context, reference, newSourceValues, compoundValue.isAnd));
                }
            } else {
                result.add(compoundValue);
            }
        }
        // unknownValue should be empty
        result.addAll(collector.unknownValues);

        List<ValueDesc> resultValues = result.build();
        Preconditions.checkArgument(!resultValues.isEmpty());
        if (resultValues.size() == 1) {
            return resultValues.get(0);
        } else {
            return new CompoundValue(context, reference, resultValues, true);
        }
    }

    private ValueDesc union(ExpressionRewriteContext context, Expression reference, ValueDescCollector collector) {
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
            discreteValues.removeIf(rangeSet::contains);
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

        if (collector.hasIsNullValue && collector.isNotNullValueOpt.isPresent()) {
            return new UnknownValue(context, BooleanLiteral.TRUE);
        } else if (collector.hasIsNullValue) {
            result.add(new IsNullValue(context, reference));
        } else {
            collector.isNotNullValueOpt.ifPresent(result::add);
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
            return new CompoundValue(context, reference, resultValues, false);
        }
    }

    /** value desc visitor */
    public interface ValueDescVisitor<R, C> {
        R visitEmptyValue(EmptyValue emptyValue, C context);

        R visitRangeValue(RangeValue rangeValue, C context);

        R visitDiscreteValue(DiscreteValue discreteValue, C context);

        R visitNotDiscreteValue(NotDiscreteValue notDiscreteValue, C context);

        R visitIsNullValue(IsNullValue isNullValue, C context);

        R visitIsNotNullValue(IsNotNullValue isNotNullValue, C context);

        R visitCompoundValue(CompoundValue compoundValue, C context);

        R visitUnknownValue(UnknownValue unknownValue, C context);
    }

    private static class ValueDescCollector implements ValueDescVisitor<Void, Void> {
        Optional<IsNotNullValue> isNotNullValueOpt = Optional.empty();
        boolean hasIsNullValue = false;
        boolean hasEmptyValue = false;
        List<RangeValue> rangeValues = Lists.newArrayList();
        List<DiscreteValue> discreteValues = Lists.newArrayList();
        List<NotDiscreteValue> notDiscreteValues = Lists.newArrayList();
        List<CompoundValue> compoundValues = Lists.newArrayList();
        List<UnknownValue> unknownValues = Lists.newArrayList();

        void add(ValueDesc value) {
            value.accept(this, null);
        }

        int size() {
            return rangeValues.size() + discreteValues.size() + compoundValues.size() + unknownValues.size();
        }

        @Override
        public Void visitEmptyValue(EmptyValue emptyValue, Void context) {
            hasEmptyValue = true;
            return null;
        }

        @Override
        public Void visitRangeValue(RangeValue rangeValue, Void context) {
            rangeValues.add(rangeValue);
            return null;
        }

        @Override
        public Void visitDiscreteValue(DiscreteValue discreteValue, Void context) {
            discreteValues.add(discreteValue);
            return null;
        }

        @Override
        public Void visitNotDiscreteValue(NotDiscreteValue notDiscreteValue, Void context) {
            notDiscreteValues.add(notDiscreteValue);
            return null;
        }

        @Override
        public Void visitIsNullValue(IsNullValue isNullValue, Void context) {
            hasIsNullValue = true;
            return null;
        }

        @Override
        public Void visitIsNotNullValue(IsNotNullValue isNotNullValue, Void context) {
            if (!isNotNullValueOpt.isPresent() || isNotNullValue.getNotExpression().isGeneratedIsNotNull()) {
                isNotNullValueOpt = Optional.of(isNotNullValue);
            }
            return null;
        }

        @Override
        public Void visitCompoundValue(CompoundValue compoundValue, Void context) {
            compoundValues.add(compoundValue);
            return null;
        }

        @Override
        public Void visitUnknownValue(UnknownValue unknownValue, Void context) {
            unknownValues.add(unknownValue);
            return null;
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

        // range contain other's range
        protected boolean containsAll(ValueDesc other) {
            return false;
        }

        public abstract boolean intersectWithEmptyIsEmpty();
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

        @Override
        public boolean intersectWithEmptyIsEmpty() {
            return true;
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
        protected boolean containsAll(ValueDesc other) {
            if (other instanceof RangeValue) {
                return range.encloses(((RangeValue) other).range);
            } else if (other instanceof DiscreteValue) {
                return range.containsAll(((DiscreteValue) other).values);
            } else {
                return false;
            }
        }

        @Override
        public boolean intersectWithEmptyIsEmpty() {
            return true;
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
        protected boolean containsAll(ValueDesc other) {
            if (other instanceof DiscreteValue) {
                return values.containsAll(((DiscreteValue) other).values);
            } else {
                return false;
            }
        }

        @Override
        public boolean intersectWithEmptyIsEmpty() {
            return true;
        }
    }

    /**
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

        @Override
        protected boolean containsAll(ValueDesc other) {
            if (other instanceof NotDiscreteValue) {
                return values.containsAll(((NotDiscreteValue) other).values);
            } else {
                return false;
            }
        }

        @Override
        public boolean intersectWithEmptyIsEmpty() {
            return true;
        }
    }

    /**
     * a is null
     */
    public static class IsNullValue extends ValueDesc {

        public IsNullValue(ExpressionRewriteContext context, Expression reference) {
            super(context, reference);
        }

        @Override
        protected <R, C> R visit(ValueDescVisitor<R, C> visitor, C context) {
            return visitor.visitIsNullValue(this, context);
        }

        @Override
        public boolean intersectWithEmptyIsEmpty() {
            return true;
        }
    }

    /**
     * a is not null
     */
    public static class IsNotNullValue extends ValueDesc {
        final Not not;

        public IsNotNullValue(ExpressionRewriteContext context, Expression reference, Not not) {
            super(context, reference);
            this.not = not;
        }

        public Not getNotExpression() {
            return this.not;
        }

        @Override
        protected <R, C> R visit(ValueDescVisitor<R, C> visitor, C context) {
            return visitor.visitIsNotNullValue(this, context);
        }

        @Override
        public boolean intersectWithEmptyIsEmpty() {
            return false;
        }
    }

    /**
     * Represents processing compound predicate.
     */
    public static class CompoundValue extends ValueDesc {
        private final List<ValueDesc> sourceValues;
        private final boolean isAnd;

        private CompoundValue(ExpressionRewriteContext context, Expression reference,
                List<ValueDesc> sourceValues, boolean isAnd) {
            super(context, reference);
            this.sourceValues = ImmutableList.copyOf(sourceValues);
            this.isAnd = isAnd;
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

        @Override
        public boolean intersectWithEmptyIsEmpty() {
            for (ValueDesc sourceValue : sourceValues) {
                if (!sourceValue.intersectWithEmptyIsEmpty()) {
                    return false;
                }
            }
            return true;
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

        @Override
        public boolean intersectWithEmptyIsEmpty() {
            return false;
        }
    }
}
