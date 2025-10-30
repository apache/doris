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
import org.apache.doris.nereids.types.DataType;
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
    public ValueDesc visitAnd(And and, ExpressionRewriteContext context) {
        return simplify(context, ExpressionUtils.extractConjunction(and), true);
    }

    @Override
    public ValueDesc visitOr(Or or, ExpressionRewriteContext context) {
        return simplify(context, ExpressionUtils.extractDisjunction(or), false);
    }

    private ValueDesc simplify(ExpressionRewriteContext context, List<Expression> predicates, boolean isAnd) {
        boolean convertIsNullToEmptyValue = isAnd && predicates.stream().anyMatch(expr -> expr instanceof NullLiteral);
        Multimap<Expression, ValueDesc> groupByReference
                = Multimaps.newListMultimap(new LinkedHashMap<>(), ArrayList::new);
        SameReferenceValueMerger merger = isAnd ? new ValueDescIntersect() : new ValueDescUnion();
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
            List<ValueDesc> valuePerReference = (List<ValueDesc>) referenceValues.getValue();
            valuePerRefs.add(merger.mergeSameReferenceValues(valuePerReference));
        }

        if (valuePerRefs.size() == 1) {
            return valuePerRefs.get(0);
        }

        return new CompoundValue(context, valuePerRefs, isAnd);
    }

    /** value desc visitor */
    public interface ValueDescVisitor<R, C> {
        R visitRangeValue(RangeValue rangeValue, C context);

        R visitDiscreteValue(DiscreteValue discreteValue, C context);

        R visitEmptyValue(EmptyValue emptyValue, C context);

        R visitCompoundValue(CompoundValue compoundValue, C context);

        R visitUnknownValue(UnknownValue unknownValue, C context);
    }

    private interface SameReferenceValueMerger extends ValueDescVisitor<ValueDesc, ValueDesc> {

        default ValueDesc mergeSameReferenceValues(List<ValueDesc> values) {
            values = preMerge(values);
            ValueDesc result = values.get(0);
            for (int i = 1; i < values.size(); i++) {
                result = values.get(i).accept(this, result);
            }
            return result;
        }

        // pre merge, merge value desc with the same type
        default List<ValueDesc> preMerge(List<ValueDesc> values) {
            return values;
        }
    }

    private static class ValueDescIntersect implements SameReferenceValueMerger {

        @Override
        public ValueDesc visitRangeValue(RangeValue rangeValue, ValueDesc source) {
            return source.intersect(rangeValue);
        }

        @Override
        public ValueDesc visitDiscreteValue(DiscreteValue discreteValue, ValueDesc source) {
            return source.intersect(discreteValue);
        }

        @Override
        public ValueDesc visitEmptyValue(EmptyValue emptyValue, ValueDesc source) {
            return source.intersect(emptyValue);
        }

        @Override
        public ValueDesc visitCompoundValue(CompoundValue compoundValue, ValueDesc source) {
            return source.intersect(compoundValue);
        }

        @Override
        public ValueDesc visitUnknownValue(UnknownValue unknownValue, ValueDesc source) {
            return source.intersect(unknownValue);
        }
    }

    private static class ValueDescUnion implements SameReferenceValueMerger {

        @Override
        public ValueDesc visitRangeValue(RangeValue rangeValue, ValueDesc source) {
            return source.union(rangeValue);
        }

        @Override
        public ValueDesc visitDiscreteValue(DiscreteValue discreteValue, ValueDesc source) {
            return source.union(discreteValue);
        }

        @Override
        public ValueDesc visitEmptyValue(EmptyValue emptyValue, ValueDesc source) {
            return source.union(emptyValue);
        }

        @Override
        public ValueDesc visitCompoundValue(CompoundValue compoundValue, ValueDesc source) {
            return source.union(compoundValue);
        }

        @Override
        public ValueDesc visitUnknownValue(UnknownValue unknownValue, ValueDesc source) {
            return source.union(unknownValue);
        }

        @Override
        public List<ValueDesc> preMerge(List<ValueDesc> valueDescs) {
            ExpressionRewriteContext context = valueDescs.get(0).context;
            Expression reference = valueDescs.get(0).getReference();
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

        protected abstract ValueDesc intersect(EmptyValue emptyValue);

        protected abstract ValueDesc intersect(RangeValue rangeValue);

        protected abstract ValueDesc intersect(DiscreteValue discreteValue);

        protected abstract ValueDesc intersect(CompoundValue compoundValue);

        protected abstract ValueDesc intersect(UnknownValue unknownValue);

        protected abstract ValueDesc union(EmptyValue emptyValue);

        protected abstract ValueDesc union(RangeValue rangeValue);

        protected abstract ValueDesc union(DiscreteValue discreteValue);

        protected abstract ValueDesc union(CompoundValue compoundValue);

        protected abstract ValueDesc union(UnknownValue unknownValue);
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
        protected ValueDesc intersect(EmptyValue other) {
            return this;
        }

        @Override
        protected ValueDesc intersect(RangeValue other) {
            return this;
        }

        @Override
        protected ValueDesc intersect(DiscreteValue other) {
            return this;
        }

        @Override
        protected ValueDesc intersect(CompoundValue other) {
            return this;
        }

        @Override
        protected ValueDesc intersect(UnknownValue other) {
            return this;
        }

        @Override
        protected ValueDesc union(EmptyValue other) {
            return other;
        }

        @Override
        protected ValueDesc union(RangeValue other) {
            return other;
        }

        @Override
        protected ValueDesc union(DiscreteValue other) {
            return other;
        }

        @Override
        protected ValueDesc union(CompoundValue other) {
            return other;
        }

        @Override
        protected ValueDesc union(UnknownValue other) {
            return other;
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

        @Override
        protected ValueDesc intersect(EmptyValue other) {
            return other;
        }

        @Override
        protected ValueDesc intersect(RangeValue other) {
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

        @Override
        protected ValueDesc intersect(DiscreteValue other) {
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
        protected ValueDesc intersect(CompoundValue other) {
            return other.compoundIntersectWith(this, true);
        }

        @Override
        protected ValueDesc intersect(UnknownValue other) {
            return new CompoundValue(context, ImmutableList.of(this, other), true);
        }

        @Override
        protected ValueDesc union(EmptyValue other) {
            return this;
        }

        @Override
        protected ValueDesc union(RangeValue other) {
            if (range.isConnected(other.range)) {
                return new RangeValue(context, reference, range.span(other.range));
            } else {
                return new CompoundValue(context, ImmutableList.of(this, other), false);
            }
        }

        @Override
        protected ValueDesc union(DiscreteValue other) {
            if (isCoverDiscreteValue(other)) {
                return this;
            } else {
                return new CompoundValue(context, ImmutableList.of(this, other), false);
            }
        }

        @Override
        protected ValueDesc union(CompoundValue other) {
            return other.compoundUnionWith(this, true);
        }

        @Override
        protected ValueDesc union(UnknownValue other) {
            return new CompoundValue(context, ImmutableList.of(this, other), false);
        }

        @Override
        public String toString() {
            return range == null ? "UnknownRange" : range.toString();
        }

        public boolean isCoverDiscreteValue(DiscreteValue discreteValue) {
            for (ComparableLiteral value : discreteValue.values) {
                if (!range.test(value)) {
                    return false;
                }
            }

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
        protected ValueDesc intersect(EmptyValue other) {
            return other;
        }

        @Override
        protected ValueDesc intersect(RangeValue other) {
            return other.intersect(this);
        }

        @Override
        protected ValueDesc intersect(DiscreteValue other) {
            Set<ComparableLiteral> newValues = Sets.newLinkedHashSet();
            newValues.addAll(this.values);
            newValues.retainAll(other.values);
            if (newValues.isEmpty()) {
                return new EmptyValue(context, reference);
            } else {
                return new DiscreteValue(context, reference, newValues);
            }
        }

        @Override
        protected ValueDesc intersect(CompoundValue other) {
            return other.compoundIntersectWith(this, true);
        }

        @Override
        protected ValueDesc intersect(UnknownValue other) {
            return new CompoundValue(context, ImmutableList.of(this, other), true);
        }

        @Override
        protected ValueDesc union(EmptyValue other) {
            return this;
        }

        @Override
        protected ValueDesc union(RangeValue other) {
            if (other.isCoverDiscreteValue(this)) {
                return other;
            } else {
                return new CompoundValue(context, ImmutableList.of(this, other), false);
            }
        }

        @Override
        protected ValueDesc union(DiscreteValue other) {
            Set<ComparableLiteral> newValues = Sets.newLinkedHashSet();
            newValues.addAll(other.values);
            newValues.addAll(this.values);
            return new DiscreteValue(context, reference, newValues);
        }

        @Override
        protected ValueDesc union(CompoundValue other) {
            return other.compoundUnionWith(this, true);
        }

        @Override
        protected ValueDesc union(UnknownValue other) {
            return new CompoundValue(context, ImmutableList.of(this, other), false);
        }

        @Override
        public String toString() {
            return values.toString();
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

        @Override
        protected ValueDesc intersect(EmptyValue other) {
            return other;
        }

        @Override
        protected ValueDesc intersect(RangeValue other) {
            return compoundIntersectWith(other, false);
        }

        @Override
        protected ValueDesc intersect(DiscreteValue other) {
            return compoundIntersectWith(other, false);
        }

        @Override
        protected ValueDesc intersect(CompoundValue other) {
            return compoundIntersectWith(other, false);
        }

        @Override
        protected ValueDesc intersect(UnknownValue other) {
            return new CompoundValue(context, ImmutableList.of(this, other), true);
        }

        @Override
        protected ValueDesc union(EmptyValue other) {
            return this;
        }

        @Override
        protected ValueDesc union(RangeValue other) {
            return compoundUnionWith(other, false);
        }

        @Override
        protected ValueDesc union(DiscreteValue other) {
            return compoundUnionWith(other, false);
        }

        @Override
        protected ValueDesc union(CompoundValue other) {
            return compoundUnionWith(other, false);
        }

        @Override
        protected ValueDesc union(UnknownValue other) {
            return new CompoundValue(context, ImmutableList.of(this, other), false);
        }

        public ValueDesc compoundIntersectWith(ValueDesc other, boolean reverse) {
            return mergeWith(other, reverse, true);
        }

        public ValueDesc compoundUnionWith(ValueDesc other, boolean reverse) {
            return mergeWith(other, reverse, false);
        }

        // merge with other, if isIntersect = true then intersect them, else union them
        private ValueDesc mergeWith(ValueDesc other, boolean reverse, boolean isIntersect) {
            // flatten
            List<ValueDesc> newSourceValues;
            if (isAnd == isIntersect && reference.equals(other.reference)) {
                List<ValueDesc> otherValues = ImmutableList.of(other);
                if (other instanceof CompoundValue && ((CompoundValue) other).isAnd == isIntersect) {
                    otherValues = ((CompoundValue) other).sourceValues;
                }
                ImmutableList.Builder<ValueDesc> builder
                        = ImmutableList.builderWithExpectedSize(sourceValues.size() + otherValues.size());
                if (reverse) {
                    builder.addAll(otherValues);
                    builder.addAll(sourceValues);
                } else {
                    builder.addAll(sourceValues);
                    builder.addAll(otherValues);
                }
                newSourceValues = builder.build();
            } else {
                newSourceValues = reverse ? ImmutableList.of(other, this) : ImmutableList.of(this, other);
            }
            return new CompoundValue(context, newSourceValues, isIntersect);
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
        protected ValueDesc intersect(EmptyValue other) {
            return other;
        }

        @Override
        protected ValueDesc intersect(RangeValue other) {
            return new CompoundValue(context, ImmutableList.of(this, other), true);
        }

        @Override
        protected ValueDesc intersect(DiscreteValue other) {
            return new CompoundValue(context, ImmutableList.of(this, other), true);
        }

        @Override
        protected ValueDesc intersect(CompoundValue other) {
            return new CompoundValue(context, ImmutableList.of(this, other), true);
        }

        @Override
        protected ValueDesc intersect(UnknownValue other) {
            return new CompoundValue(context, ImmutableList.of(this, other), true);
        }

        @Override
        protected ValueDesc union(EmptyValue other) {
            return this;
        }

        @Override
        protected ValueDesc union(RangeValue other) {
            return new CompoundValue(context, ImmutableList.of(this, other), false);
        }

        @Override
        protected ValueDesc union(DiscreteValue other) {
            return new CompoundValue(context, ImmutableList.of(this, other), false);
        }

        @Override
        protected ValueDesc union(CompoundValue other) {
            return new CompoundValue(context, ImmutableList.of(this, other), false);
        }

        @Override
        protected ValueDesc union(UnknownValue other) {
            return new CompoundValue(context, ImmutableList.of(this, other), false);
        }
    }
}
