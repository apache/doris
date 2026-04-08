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

import org.apache.doris.common.Pair;
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
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
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
                && ExpressionUtils.isAllNonNullComparableLiteral(inPredicate.getOptions())) {
            Set<ComparableLiteral> values = Sets.newLinkedHashSetWithExpectedSize(inPredicate.getOptions().size());
            boolean succ = true;
            for (Expression value : inPredicate.getOptions()) {
                Optional<ComparableLiteral> literal = tryGetComparableLiteral(value);
                if (!literal.isPresent()) {
                    succ = false;
                    break;
                }
                values.add(literal.get());
            }
            if (succ) {
                return new DiscreteValue(context, inPredicate.getCompareExpr(), values);
            }
        }

        return new UnknownValue(context, inPredicate);
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
        boolean hasNullExpression = false;
        boolean hasIsNullExpression = false;
        boolean hasNotIsNullExpression = false;
        Predicate<Expression> isNotNull = expression -> expression instanceof Not
                && expression.child(0) instanceof IsNull
                && !((Not) expression).isGeneratedIsNotNull();
        for (Expression predicate : predicates) {
            hasNullExpression = hasNullExpression || predicate.isNullLiteral();
            hasIsNullExpression = hasIsNullExpression || predicate instanceof IsNull;
            hasNotIsNullExpression = hasNotIsNullExpression || isNotNull.test(predicate);
        }
        boolean convertIsNullToEmptyValue = isAnd && hasNullExpression && hasIsNullExpression;
        boolean convertNotIsNullToRangeAll = !isAnd && hasNullExpression && hasNotIsNullExpression;
        Map<Pair<Expression, Integer>, ValueDescCollector> groupByReference = Maps.newLinkedHashMap();
        int nextUniqueNum = 1;
        for (Expression predicate : predicates) {
            // given an expression A, no matter A is nullable or not,
            // 'A is null and null' can represent as EmptyValue(A),
            // 'A is not null or null' can represent as RangeAll(A).
            ValueDesc valueDesc = null;
            if (predicate instanceof IsNull && convertIsNullToEmptyValue) {
                valueDesc = new EmptyValue(context, ((IsNull) predicate).child());
            } else if (isNotNull.test(predicate) && convertNotIsNullToRangeAll) {
                valueDesc = new RangeValue(context, predicate.child(0).child(0), Range.all());
            } else if (predicate.isNullLiteral() && (convertIsNullToEmptyValue || convertNotIsNullToRangeAll)) {
                continue;
            } else {
                valueDesc = predicate.accept(this, context);
            }

            int uniqueNum = 0;

            // for compound value with diff source value reference like 'a > 1 and b > 1',
            // don't merge it with other values, so give them a unique num > 0.
            // for other value desc, their unique num is always 0.
            if (valueDesc instanceof CompoundValue && !((CompoundValue) valueDesc).isSameReference) {
                nextUniqueNum++;
                uniqueNum = nextUniqueNum;
            }

            Expression reference = valueDesc.reference;
            groupByReference.computeIfAbsent(Pair.of(reference, uniqueNum),
                    key -> new ValueDescCollector()).add(valueDesc);
        }

        List<ValueDesc> valuePerRefs = Lists.newArrayList();
        for (Entry<Pair<Expression, Integer>, ValueDescCollector> referenceValues : groupByReference.entrySet()) {
            Expression reference = referenceValues.getKey().first;
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
        if (collector.hasIsNullValue) {
            if (!collector.rangeValues.isEmpty()
                    || !collector.discreteValues.isEmpty()
                    || !collector.notDiscreteValues.isEmpty()) {
                // TA is null and TA > 1
                // => TA is null and (null)
                // => TA is null and null
                // => EmptyValue(TA)
                collector.rangeValues.clear();
                collector.discreteValues.clear();
                collector.notDiscreteValues.clear();
                collector.add(new EmptyValue(context, reference));
            }
        }

        List<ValueDesc> resultValues = Lists.newArrayList();
        // merge all the range values
        Range<ComparableLiteral> mergeRangeValue = null;
        if (!collector.hasEmptyValue && !collector.rangeValues.isEmpty()) {
            RangeValue mergeRangeValueDesc = null;
            for (RangeValue rangeValue : collector.rangeValues) {
                // RangeAll(TA) and IsNotNull(TA)
                // = (TA is not null or null) and (TA is not null)
                // = TA is not null
                // = IsNotNull(TA)
                if (rangeValue.isRangeAll() && collector.isNotNullValueOpt.isPresent()) {
                    // Notice that if collector has only isGenerateNotNullValueOpt, we should not keep the rangeAll here
                    // for expression:  (Not(IsNull(TA)) OR NULL) AND GeneratedNot(IsNull(TA))
                    // will be converted to RangeAll(TA) AND IsNotNullValue(TA, generated=true)
                    // if we skip this RangeAll, the final result will be IsNotNullValue(TA, generated=true)
                    // then convert back to expression: GeneratedNot(IsNull(TA)),
                    // but later EliminateNotNull rule will remove this generated Not expression,
                    // then the final result will be TRUE, which is wrong.
                    continue;
                }
                if (mergeRangeValueDesc == null) {
                    mergeRangeValueDesc = rangeValue;
                } else {
                    ValueDesc combineValue = mergeRangeValueDesc.intersect(rangeValue);
                    if (combineValue instanceof RangeValue) {
                        mergeRangeValueDesc = (RangeValue) combineValue;
                    } else {
                        collector.add(combineValue);
                        mergeRangeValueDesc = null;
                        // no need to process the lefts.
                        if (combineValue instanceof EmptyValue) {
                            break;
                        }
                    }
                }
            }
            if (!collector.hasEmptyValue && mergeRangeValueDesc != null) {
                mergeRangeValue = mergeRangeValueDesc.range;
            }
        }

        // merge all the discrete values
        Set<ComparableLiteral> mergeDiscreteValues = null;
        if (!collector.hasEmptyValue && !collector.discreteValues.isEmpty()) {
            mergeDiscreteValues = Sets.newLinkedHashSet(collector.discreteValues.get(0).values);
            for (int i = 1; i < collector.discreteValues.size(); i++) {
                mergeDiscreteValues.retainAll(collector.discreteValues.get(i).values);
            }
            if (mergeDiscreteValues.isEmpty()) {
                collector.add(new EmptyValue(context, reference));
                mergeDiscreteValues = null;
            }
        }

        // merge all the not discrete values
        Set<ComparableLiteral> mergeNotDiscreteValues = Sets.newLinkedHashSet();
        if (!collector.hasEmptyValue && !collector.notDiscreteValues.isEmpty()) {
            for (NotDiscreteValue notDiscreteValue : collector.notDiscreteValues) {
                mergeNotDiscreteValues.addAll(notDiscreteValue.values);
            }
            if (mergeRangeValue != null) {
                Range<ComparableLiteral> finalValue = mergeRangeValue;
                mergeNotDiscreteValues.removeIf(value -> !finalValue.contains(value));
            }
            if (mergeDiscreteValues != null) {
                Set<ComparableLiteral> finalValues = mergeDiscreteValues;
                mergeNotDiscreteValues.removeIf(value -> !finalValues.contains(value));
                mergeDiscreteValues.removeIf(mergeNotDiscreteValues::contains);
                if (mergeDiscreteValues.isEmpty()) {
                    collector.add(new EmptyValue(context, reference));
                    mergeDiscreteValues = null;
                }
            }
        }
        if (!collector.hasEmptyValue) {
            // merge range + discrete values
            if (mergeRangeValue != null && mergeDiscreteValues != null) {
                ValueDesc newMergeValue = new RangeValue(context, reference, mergeRangeValue)
                        .intersect(new DiscreteValue(context, reference, mergeDiscreteValues));
                resultValues.add(newMergeValue);
            } else if (mergeRangeValue != null) {
                resultValues.add(new RangeValue(context, reference, mergeRangeValue));
            } else if (mergeDiscreteValues != null) {
                resultValues.add(new DiscreteValue(context, reference, mergeDiscreteValues));
            }
            if (!collector.hasEmptyValue && !mergeNotDiscreteValues.isEmpty()) {
                resultValues.add(new NotDiscreteValue(context, reference, mergeNotDiscreteValues));
            }
        }

        // process empty value
        if (collector.hasEmptyValue) {
            if (!reference.nullable()) {
                return new UnknownValue(context, BooleanLiteral.FALSE);
            }
            resultValues.add(new EmptyValue(context, reference));
        }
        if (collector.hasIsNullValue) {
            if (collector.hasIsNotNullValue()) {
                return new UnknownValue(context, BooleanLiteral.FALSE);
            }
            // nullable's EmptyValue have contains IsNull, no need to add
            if (!collector.hasEmptyValue) {
                resultValues.add(new IsNullValue(context, reference));
            }
        }
        if (collector.hasIsNotNullValue()) {
            if (collector.hasEmptyValue) {
                return new UnknownValue(context, BooleanLiteral.FALSE);
            }
            collector.isNotNullValueOpt.ifPresent(resultValues::add);
            collector.isGenerateNotNullValueOpt.ifPresent(resultValues::add);
        }
        Optional<ValueDesc> shortCutResult = mergeCompoundValues(context, reference, resultValues, collector, true);
        if (shortCutResult.isPresent()) {
            return shortCutResult.get();
        }
        // unknownValue should be empty
        resultValues.addAll(collector.unknownValues);

        Preconditions.checkArgument(!resultValues.isEmpty());
        if (resultValues.size() == 1) {
            return resultValues.get(0);
        } else {
            return new CompoundValue(context, reference, resultValues, true);
        }
    }

    private ValueDesc union(ExpressionRewriteContext context, Expression reference, ValueDescCollector collector) {
        if (collector.hasIsNotNullValue()) {
            if (!collector.rangeValues.isEmpty()
                    || !collector.discreteValues.isEmpty()
                    || !collector.notDiscreteValues.isEmpty()) {
                // TA is not null or TA > 1
                // => TA is not null or (null)
                // => TA is not null or null
                // => RangeAll(TA)
                collector.rangeValues.clear();
                collector.discreteValues.clear();
                collector.notDiscreteValues.clear();
                collector.add(new RangeValue(context, reference, Range.all()));
            }
        }

        List<ValueDesc> resultValues = Lists.newArrayListWithExpectedSize(collector.size() + 3);
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

        Set<ComparableLiteral> mergeNotDiscreteValues = Sets.newLinkedHashSet();
        boolean hasRangeAll = false;
        if (!collector.notDiscreteValues.isEmpty()) {
            mergeNotDiscreteValues.addAll(collector.notDiscreteValues.get(0).values);
            // a not in (1, 2) or a not in (1, 2, 3) => a not in (1, 2)
            for (int i = 1; i < collector.notDiscreteValues.size(); i++) {
                mergeNotDiscreteValues.retainAll(collector.notDiscreteValues.get(i).values);
            }
            // a not in (1, 2, 3) or a in (1, 2, 4) => a not in (3)
            mergeNotDiscreteValues.removeIf(
                    value -> discreteValues.contains(value) || rangeSet.contains(value));
            discreteValues.removeIf(mergeNotDiscreteValues::contains);
            if (mergeNotDiscreteValues.isEmpty()) {
                resultValues.add(new RangeValue(context, reference, Range.all()));
            } else {
                resultValues.add(new NotDiscreteValue(context, reference, mergeNotDiscreteValues));
            }
        } else {
            if (!discreteValues.isEmpty()) {
                resultValues.add(new DiscreteValue(context, reference, discreteValues));
            }
            for (Range<ComparableLiteral> range : rangeSet.asRanges()) {
                hasRangeAll = hasRangeAll || !range.hasUpperBound() && !range.hasLowerBound();
                resultValues.add(new RangeValue(context, reference, range));
            }
        }

        if (collector.hasIsNullValue) {
            if (collector.hasIsNotNullValue() || hasRangeAll) {
                return new UnknownValue(context, BooleanLiteral.TRUE);
            }
            resultValues.add(new IsNullValue(context, reference));
        }
        if (collector.hasIsNotNullValue()) {
            if (collector.hasEmptyValue) {
                // EmptyValue(TA) or TA is not null
                // = TA is null and null or TA is not null
                // = TA is not null or null
                // = RangeAll(TA)
                resultValues.add(new RangeValue(context, reference, Range.all()));
            } else {
                collector.isNotNullValueOpt.ifPresent(resultValues::add);
                collector.isGenerateNotNullValueOpt.ifPresent(resultValues::add);
            }
        }

        Optional<ValueDesc> shortCutResult = mergeCompoundValues(context, reference, resultValues, collector, false);
        if (shortCutResult.isPresent()) {
            return shortCutResult.get();
        }
        if (collector.hasEmptyValue) {
            // for IsNotNull OR EmptyValue, need keep the EmptyValue
            boolean ignoreEmptyValue = !resultValues.isEmpty() && !reference.nullable();
            for (ValueDesc valueDesc : resultValues) {
                if (valueDesc instanceof CompoundValue) {
                    ignoreEmptyValue = ignoreEmptyValue || !((CompoundValue) valueDesc).hasNoneNullable;
                } else if (valueDesc.nullable() || valueDesc instanceof IsNullValue) {
                    ignoreEmptyValue = true;
                }
                if (ignoreEmptyValue) {
                    break;
                }
            }
            if (!ignoreEmptyValue) {
                resultValues.add(new EmptyValue(context, reference));
            }
        }
        resultValues.addAll(collector.unknownValues);
        Preconditions.checkArgument(!resultValues.isEmpty());
        if (resultValues.size() == 1) {
            return resultValues.get(0);
        } else {
            return new CompoundValue(context, reference, resultValues, false);
        }
    }

    private Optional<ValueDesc> mergeCompoundValues(ExpressionRewriteContext context, Expression reference,
            List<ValueDesc> resultValues, ValueDescCollector collector, boolean isAnd) {
        // for A and (B or C):
        // if A and B is false/empty, then A and (B or C) = A and C
        // if B's range is bigger than A, then A and (B or C) = A
        // for A or (B and C):
        // if A's range is bigger than B, then A or (B and C) = A
        // if A or B is true/all, then A or (B and C) = A or C
        for (CompoundValue compoundValue : collector.compoundValues) {
            if (isAnd != compoundValue.isAnd && compoundValue.reference.equals(reference)) {
                ImmutableList.Builder<ValueDesc> newSourceValuesBuilder
                        = ImmutableList.builderWithExpectedSize(compoundValue.sourceValues.size());
                boolean skipWholeCompoundValue = false;
                for (ValueDesc innerValue : compoundValue.sourceValues) {
                    IntersectType intersectType = IntersectType.OTHERS;
                    UnionType unionType = UnionType.OTHERS;
                    for (ValueDesc outerValue : resultValues) {
                        if (isAnd) {
                            skipWholeCompoundValue = skipWholeCompoundValue || innerValue.containsAll(outerValue);
                            IntersectType type = outerValue.getIntersectType(innerValue);
                            if (type == IntersectType.EMPTY_VALUE && intersectType != IntersectType.FALSE) {
                                intersectType = type;
                            } else if (type == IntersectType.FALSE) {
                                intersectType = type;
                            }
                        } else {
                            skipWholeCompoundValue = skipWholeCompoundValue || outerValue.containsAll(innerValue);
                            UnionType type = outerValue.getUnionType(innerValue);
                            if (type == UnionType.RANGE_ALL && unionType != UnionType.TRUE) {
                                unionType = type;
                            } else if (type == UnionType.TRUE) {
                                unionType = type;
                            }
                        }
                    }
                    if (skipWholeCompoundValue) {
                        break;
                    }
                    if (isAnd) {
                        if (intersectType == IntersectType.OTHERS) {
                            newSourceValuesBuilder.add(innerValue);
                        } else if (intersectType == IntersectType.EMPTY_VALUE) {
                            newSourceValuesBuilder.add(new EmptyValue(context, reference));
                        }
                    } else {
                        if (unionType == UnionType.OTHERS) {
                            newSourceValuesBuilder.add(innerValue);
                        } else if (unionType == UnionType.RANGE_ALL) {
                            newSourceValuesBuilder.add(new RangeValue(context, reference, Range.all()));
                        }
                    }
                }
                if (!skipWholeCompoundValue) {
                    List<ValueDesc> newSourceValues = newSourceValuesBuilder.build();
                    if (newSourceValues.isEmpty()) {
                        // when isAnd = true,  A and (B or C or D)
                        // if A and B = FALSE, A and C = FALSE, A and D = FALSE, then newSourceValues is empty
                        // then A and (B or C or D) = FALSE
                        // when isAnd = false,  A or (B and C and D)
                        // if A or B = TRUE, A or C = TRUE, A or D = TRUE, then newSourceValues is empty
                        // then A or (B and C and D) = TRUE
                        return Optional.of(new UnknownValue(context, BooleanLiteral.of(!isAnd)));
                    } else if (newSourceValues.size() == 1) {
                        resultValues.add(newSourceValues.get(0));
                    } else {
                        resultValues.add(new CompoundValue(context, reference, newSourceValues, compoundValue.isAnd));
                    }
                }
            } else {
                resultValues.add(compoundValue);
            }
        }

        return Optional.empty();
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
        // generated not is null != not is null
        Optional<IsNotNullValue> isNotNullValueOpt = Optional.empty();
        Optional<IsNotNullValue> isGenerateNotNullValueOpt = Optional.empty();

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

        boolean hasIsNotNullValue() {
            return isNotNullValueOpt.isPresent() || isGenerateNotNullValueOpt.isPresent();
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
            if (isNotNullValue.not.isGeneratedIsNotNull()) {
                isGenerateNotNullValueOpt = Optional.of(isNotNullValue);
            } else {
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

    /** union two value result */
    public enum UnionType {
        TRUE, // equals TRUE
        RANGE_ALL, // trueOrNull(reference)
        OTHERS, // other case
    }

    /** intersect two value result */
    public enum IntersectType {
        FALSE, // equals FALSE
        EMPTY_VALUE, // falseOrNull(reference)
        OTHERS, // other case
    }

    /**
     * value desc
     */
    public abstract static class ValueDesc {
        protected final ExpressionRewriteContext context;
        protected final Expression reference;

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

        protected abstract boolean nullable();

        protected boolean nullForNullReference() {
            return nullable();
        }

        // X containsAll Y, means:
        // 1) when Y is TRUE, X is TRUE;
        // 2) when Y is FALSE, X can be any;
        // 3) when Y is null, X is null;
        // then will have:
        // use in 'A and (B or C)', if B containsAll A, then rewrite it to 'A',
        // use in 'A or (B and C)', if A containsAll B, then rewrite it to 'A'.
        @VisibleForTesting
        public final boolean containsAll(ValueDesc other) {
            return containsAll(other, 0);
        }

        protected abstract boolean containsAll(ValueDesc other, int depth);

        // X, Y intersectWithIsEmpty, means 'X and Y' is:
        // 1) FALSE && !X.nullable() && !Y.nullable();
        // 2) EmptyValue && X.nullable() && Y.nullable()), the nullable check no loss the null
        // use in 'A and (B or C)', if A, B intersectWithIsEmpty, then rewrite it to 'A and C'
        @VisibleForTesting
        public final IntersectType getIntersectType(ValueDesc other) {
            return getIntersectType(other, 0);
        }

        protected abstract IntersectType getIntersectType(ValueDesc other, int depth);

        // X, Y unionWithIsAll, means 'X union Y' is:
        // 1) TRUE && !X.nullable() && !Y.nullable();
        // 2) Range.all() && X.nullable() && Y.nullable(), the nullable check no loss the null;
        // use in 'A or (B and C)', if A, B unionWithIsAll, then rewrite it to 'A or C'
        @VisibleForTesting
        public final UnionType getUnionType(ValueDesc other) {
            return getUnionType(other, 0);
        }

        protected abstract UnionType getUnionType(ValueDesc other, int depth);
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
        protected boolean nullable() {
            return reference.nullable();
        }

        @Override
        protected boolean containsAll(ValueDesc other, int depth) {
            return other instanceof EmptyValue || (other instanceof IsNullValue && !reference.nullable());
        }

        @Override
        protected IntersectType getIntersectType(ValueDesc other, int depth) {
            if (other instanceof EmptyValue || other instanceof RangeValue
                    || other instanceof DiscreteValue || other instanceof NotDiscreteValue
                    || other instanceof IsNullValue) {
                return reference.nullable() ? IntersectType.EMPTY_VALUE : IntersectType.FALSE;
            } else if (other instanceof IsNotNullValue) {
                return IntersectType.FALSE;
            } else if (other instanceof CompoundValue) {
                return other.getIntersectType(this, depth);
            } else {
                return IntersectType.OTHERS;
            }
        }

        @Override
        protected UnionType getUnionType(ValueDesc other, int depth) {
            if (other instanceof RangeValue) {
                if (((RangeValue) other).isRangeAll()) {
                    return reference.nullable() ? UnionType.RANGE_ALL : UnionType.TRUE;
                }
            } else if (other instanceof IsNotNullValue) {
                if (!reference.nullable()) {
                    return UnionType.TRUE;
                }
            } else if (other instanceof CompoundValue) {
                return other.getUnionType(this, depth);
            }
            return UnionType.OTHERS;
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
        protected boolean nullable() {
            return reference.nullable();
        }

        @Override
        protected boolean containsAll(ValueDesc other, int depth) {
            if (other instanceof EmptyValue) {
                return true;
            } else if (other instanceof RangeValue) {
                return range.encloses(((RangeValue) other).range);
            } else if (other instanceof DiscreteValue) {
                return range.containsAll(((DiscreteValue) other).values);
            } else if (other instanceof NotDiscreteValue || other instanceof IsNotNullValue) {
                return isRangeAll();
            } else if (other instanceof CompoundValue) {
                return ((CompoundValue) other).isContainedAllBy(this, depth);
            } else {
                return false;
            }
        }

        @Override
        protected IntersectType getIntersectType(ValueDesc other, int depth) {
            if (other instanceof EmptyValue) {
                return reference.nullable() ? IntersectType.EMPTY_VALUE : IntersectType.FALSE;
            } else if (other instanceof RangeValue) {
                if (intersect((RangeValue) other) instanceof EmptyValue) {
                    return reference.nullable() ? IntersectType.EMPTY_VALUE : IntersectType.FALSE;
                }
            } else if (other instanceof DiscreteValue) {
                if (intersect((DiscreteValue) other) instanceof EmptyValue) {
                    return reference.nullable() ? IntersectType.EMPTY_VALUE : IntersectType.FALSE;
                }
            } else if (other instanceof IsNullValue) {
                return reference.nullable() ? IntersectType.EMPTY_VALUE : IntersectType.FALSE;
            } else if (other instanceof CompoundValue) {
                return other.getIntersectType(this, depth);
            }
            return IntersectType.OTHERS;
        }

        @Override
        protected UnionType getUnionType(ValueDesc other, int depth) {
            if ((other instanceof EmptyValue || other instanceof DiscreteValue) && isRangeAll()) {
                return reference.nullable() ? UnionType.RANGE_ALL : UnionType.TRUE;
            } else if (other instanceof RangeValue) {
                Range<ComparableLiteral> otherRange = ((RangeValue) other).range;
                if (range.isConnected(otherRange)) {
                    Range<ComparableLiteral> unionRange = range.span(otherRange);
                    if (!unionRange.hasLowerBound() && !unionRange.hasUpperBound()) {
                        return reference.nullable() ? UnionType.RANGE_ALL : UnionType.TRUE;
                    }
                }
            } else if (other instanceof NotDiscreteValue) {
                Set<ComparableLiteral> notDiscreteValues = ((NotDiscreteValue) other).values;
                boolean succ = true;
                for (ComparableLiteral value : notDiscreteValues) {
                    if (!range.contains(value)) {
                        succ = false;
                        break;
                    }
                }
                if (succ) {
                    return reference.nullable() ? UnionType.RANGE_ALL : UnionType.TRUE;
                }
            } else if (other instanceof IsNullValue && !reference.nullable() && isRangeAll()) {
                return UnionType.TRUE;
            } else if (other instanceof IsNotNullValue) {
                if (!reference.nullable()) {
                    return UnionType.TRUE;
                }
            } else if (other instanceof CompoundValue) {
                return other.getUnionType(this, depth);
            }
            return UnionType.OTHERS;
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

        @VisibleForTesting
        public boolean isRangeAll() {
            return !range.hasLowerBound() && !range.hasUpperBound();
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
        protected boolean nullable() {
            return reference.nullable();
        }

        @Override
        protected boolean containsAll(ValueDesc other, int depth) {
            if (other instanceof EmptyValue) {
                return true;
            } else if (other instanceof DiscreteValue) {
                return values.containsAll(((DiscreteValue) other).values);
            } else if (other instanceof CompoundValue) {
                return ((CompoundValue) other).isContainedAllBy(this, depth);
            } else {
                return false;
            }
        }

        @Override
        protected IntersectType getIntersectType(ValueDesc other, int depth) {
            if (other instanceof EmptyValue) {
                return reference.nullable() ? IntersectType.EMPTY_VALUE : IntersectType.FALSE;
            } else if (other instanceof RangeValue) {
                return other.getIntersectType(this, depth);
            } else if (other instanceof DiscreteValue) {
                Set<ComparableLiteral> otherValues = ((DiscreteValue) other).values;
                for (ComparableLiteral value : otherValues) {
                    if (values.contains(value)) {
                        return IntersectType.OTHERS;
                    }
                }
                return reference.nullable() ? IntersectType.EMPTY_VALUE : IntersectType.FALSE;
            } else if (other instanceof IsNullValue) {
                return reference.nullable() ? IntersectType.EMPTY_VALUE : IntersectType.FALSE;
            } else if (other instanceof CompoundValue) {
                return other.getIntersectType(this, depth);
            } else {
                return IntersectType.OTHERS;
            }
        }

        @Override
        protected UnionType getUnionType(ValueDesc other, int depth) {
            if (other instanceof RangeValue) {
                return other.getUnionType(this, depth);
            } else if (other instanceof NotDiscreteValue) {
                boolean succ = true;
                Set<ComparableLiteral> notDiscreteValues = ((NotDiscreteValue) other).values;
                for (ComparableLiteral value : notDiscreteValues) {
                    if (!values.contains(value)) {
                        succ = false;
                        break;
                    }
                }
                if (succ) {
                    return reference.nullable() ? UnionType.RANGE_ALL : UnionType.TRUE;
                }
            } else if (other instanceof IsNotNullValue) {
                if (!reference.nullable()) {
                    return UnionType.TRUE;
                }
            } else if (other instanceof CompoundValue) {
                return other.getUnionType(this, depth);
            }
            return UnionType.OTHERS;
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
        protected boolean nullable() {
            return reference.nullable();
        }

        @Override
        protected boolean containsAll(ValueDesc other, int depth) {
            if (other instanceof EmptyValue) {
                return true;
            } else if (other instanceof RangeValue) {
                Range<ComparableLiteral> range = ((RangeValue) other).range;
                for (ComparableLiteral value : values) {
                    if (range.contains(value)) {
                        return false;
                    }
                }
                return true;
            } else if (other instanceof DiscreteValue) {
                Set<ComparableLiteral> discreteValues = ((DiscreteValue) other).values;
                for (ComparableLiteral value : values) {
                    if (discreteValues.contains(value)) {
                        return false;
                    }
                }
                return true;
            } else if (other instanceof NotDiscreteValue) {
                return ((NotDiscreteValue) other).values.containsAll(values);
            } else if (other instanceof CompoundValue) {
                return ((CompoundValue) other).isContainedAllBy(this, depth);
            } else {
                return false;
            }
        }

        @Override
        protected IntersectType getIntersectType(ValueDesc other, int depth) {
            if (other instanceof EmptyValue) {
                return reference.nullable() ? IntersectType.EMPTY_VALUE : IntersectType.FALSE;
            } else if (other instanceof DiscreteValue) {
                if (values.containsAll(((DiscreteValue) other).values)) {
                    return reference.nullable() ? IntersectType.EMPTY_VALUE : IntersectType.FALSE;
                }
            } else if (other instanceof IsNullValue) {
                return reference.nullable() ? IntersectType.EMPTY_VALUE : IntersectType.FALSE;
            } else if (other instanceof CompoundValue) {
                return other.getIntersectType(this, depth);
            }
            return IntersectType.OTHERS;
        }

        @Override
        protected UnionType getUnionType(ValueDesc other, int depth) {
            if (other instanceof RangeValue) {
                return other.getUnionType(this, depth);
            } else if (other instanceof DiscreteValue) {
                return other.getUnionType(this, depth);
            } else if (other instanceof NotDiscreteValue) {
                Set<ComparableLiteral> notDiscreteValues = ((NotDiscreteValue) other).values;
                for (ComparableLiteral value : notDiscreteValues) {
                    if (values.contains(value)) {
                        return UnionType.OTHERS;
                    }
                }
                return reference.nullable() ? UnionType.RANGE_ALL : UnionType.TRUE;
            } else if (other instanceof IsNotNullValue) {
                if (!reference.nullable()) {
                    return UnionType.TRUE;
                }
            } else if (other instanceof CompoundValue) {
                return other.getUnionType(this, depth);
            }
            return UnionType.OTHERS;
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
        protected boolean nullable() {
            return false;
        }

        @Override
        protected boolean containsAll(ValueDesc other, int depth) {
            if (other instanceof EmptyValue) {
                return !reference.nullable();
            } else if (other instanceof IsNullValue) {
                return true;
            } else if (other instanceof CompoundValue) {
                return ((CompoundValue) other).isContainedAllBy(this, depth);
            } else {
                return false;
            }
        }

        @Override
        protected IntersectType getIntersectType(ValueDesc other, int depth) {
            if (other instanceof EmptyValue || other instanceof RangeValue
                    || other instanceof DiscreteValue || other instanceof NotDiscreteValue) {
                return reference.nullable() ? IntersectType.EMPTY_VALUE : IntersectType.FALSE;
            } else if (other instanceof IsNotNullValue) {
                return IntersectType.FALSE;
            } else if (other instanceof CompoundValue) {
                return other.getIntersectType(this, depth);
            }
            return IntersectType.OTHERS;
        }

        @Override
        protected UnionType getUnionType(ValueDesc other, int depth) {
            if (other instanceof RangeValue) {
                return ((RangeValue) other).getUnionType(this, depth);
            } else if (other instanceof IsNotNullValue) {
                return UnionType.TRUE;
            } else {
                return UnionType.OTHERS;
            }
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
        protected boolean nullable() {
            return false;
        }

        @Override
        protected boolean containsAll(ValueDesc other, int depth) {
            if (other instanceof IsNotNullValue) {
                return not.isGeneratedIsNotNull() == ((IsNotNullValue) other).not.isGeneratedIsNotNull();
            } else if (other instanceof CompoundValue) {
                return ((CompoundValue) other).isContainedAllBy(this, depth);
            } else {
                return false;
            }
        }

        @Override
        protected IntersectType getIntersectType(ValueDesc other, int depth) {
            if (other instanceof EmptyValue || other instanceof IsNullValue) {
                return IntersectType.FALSE;
            } else if (other instanceof CompoundValue) {
                return other.getIntersectType(this, depth);
            } else {
                return IntersectType.OTHERS;
            }
        }

        @Override
        protected UnionType getUnionType(ValueDesc other, int depth) {
            if (other instanceof EmptyValue || other instanceof RangeValue
                    || other instanceof DiscreteValue || other instanceof NotDiscreteValue) {
                if (!reference.nullable()) {
                    return UnionType.TRUE;
                }
            } else if (other instanceof IsNullValue) {
                return UnionType.TRUE;
            } else if (other instanceof CompoundValue) {
                return other.getUnionType(this, depth);
            }
            return UnionType.OTHERS;
        }
    }

    /**
     * Represents processing compound predicate.
     */
    public static class CompoundValue extends ValueDesc {
        private static final int MAX_SEARCH_DEPTH = 1;
        private final List<ValueDesc> sourceValues;
        private final boolean isAnd;
        private final Set<Class<? extends ValueDesc>> subClasses;
        private final boolean hasNullable;
        private final boolean hasNoneNullable;
        private final boolean isSameReference;

        /** constructor */
        public CompoundValue(ExpressionRewriteContext context, Expression reference,
                List<ValueDesc> sourceValues, boolean isAnd) {
            super(context, reference);
            this.sourceValues = ImmutableList.copyOf(sourceValues);
            this.isAnd = isAnd;
            this.subClasses = Sets.newHashSet();
            this.subClasses.add(getClass());
            boolean hasNullable = false;
            boolean hasNonNullable = false;
            boolean isSameReference = true;
            for (ValueDesc sourceValue : sourceValues) {
                if (sourceValue instanceof CompoundValue) {
                    CompoundValue compoundSource = (CompoundValue) sourceValue;
                    this.subClasses.addAll(compoundSource.subClasses);
                    hasNullable = hasNullable || compoundSource.hasNullable;
                    hasNonNullable = hasNonNullable || compoundSource.hasNoneNullable;
                    isSameReference = isSameReference && compoundSource.isSameReference;
                } else {
                    this.subClasses.add(sourceValue.getClass());
                    hasNullable = hasNullable || sourceValue.nullable();
                    hasNonNullable = hasNonNullable || !sourceValue.nullable();
                }
                isSameReference = isSameReference && sourceValue.getReference().equals(reference);
            }
            this.hasNullable = hasNullable;
            this.hasNoneNullable = hasNonNullable;
            this.isSameReference = isSameReference;
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
        protected boolean nullable() {
            return hasNullable;
        }

        @Override
        protected boolean nullForNullReference() {
            return reference.nullable() && !hasNoneNullable;
        }

        @Override
        protected boolean containsAll(ValueDesc other, int depth) {
            // in fact, when merge the value desc for the same reference,
            // all the value desc should not be unknown value
            if (depth > MAX_SEARCH_DEPTH || other instanceof UnknownValue || subClasses.contains(UnknownValue.class)) {
                return false;
            }
            if (!isAnd && (!other.nullable() || !hasNoneNullable)) {
                // for OR value desc:
                // 1) if other not nullable, then no need to consider other is null, this is null
                // 2) if other is nullable, then when other is null, then the reference is null,
                //    so if this OR no non-nullable, then this is null too.
                for (ValueDesc valueDesc : sourceValues) {
                    if (valueDesc.containsAll(other, depth + 1)) {
                        return true;
                    }
                }
                return false;
            } else {
                // when other is nullable, why OR should check all source values containsAll ?
                // give an example: for an OR:  (c1 or c2 or c3), suppose c1 containsAll other,
                // then when other is null, the OR = null or c2 or c3, it may not be null.
                // a example: 'a > 1 or a is null' not contains all 'a > 10', even if 'a > 1' contains all 'a > 10'
                for (ValueDesc valueDesc : sourceValues) {
                    if (!valueDesc.containsAll(other, depth + 1)) {
                        return false;
                    }
                }
                return true;
            }
        }

        // check other containsAll this
        private boolean isContainedAllBy(ValueDesc other, int depth) {
            // do want to process the complicate cases,
            // and in fact, when merge value desc for same reference,
            // all the value should not contain UnknownValue.
            if (depth > MAX_SEARCH_DEPTH || other instanceof UnknownValue || subClasses.contains(UnknownValue.class)) {
                return false;
            }
            if (isAnd) {
                // for C = c1 and c2 and c3, suppose other containsAll c1, then will have:
                // when c1 is true, other is true,
                // when c1 is null, other is null,
                // so, when C is true, then c1 is true, so other is true,
                //     when C is null, then the reference must be null, so, c1 is null too, then other is null
                for (ValueDesc valueDesc : sourceValues) {
                    if (other.containsAll(valueDesc, depth)) {
                        return true;
                    }
                }
                return false;
            } else {
                // for C = c1 or c2 or c3, suppose other contains c1, c2, c3.
                // so when C is true, then at least one ci is true, so other is true.
                //    when C is null, then at least one ci is null, so other is null.
                // so other will contain all C
                for (ValueDesc valueDesc : sourceValues) {
                    if (!other.containsAll(valueDesc, depth)) {
                        return false;
                    }
                }
                return true;
            }
        }

        @Override
        protected IntersectType getIntersectType(ValueDesc other, int depth) {
            if ((!nullable() && other.nullable()) || depth > MAX_SEARCH_DEPTH) {
                return IntersectType.OTHERS;
            }
            if (isAnd) {
                // process A and ((B and C) or ...)
                boolean hasEmptyValue = false;
                boolean hasIsNotNull = false;
                boolean allOtherNullForNullReference = true;
                for (ValueDesc valueDesc : sourceValues) {
                    IntersectType type = valueDesc.getIntersectType(other, depth + 1);
                    if (type == IntersectType.FALSE) {
                        return type;
                    }
                    if (type == IntersectType.EMPTY_VALUE) {
                        hasEmptyValue = true;
                    } else {
                        allOtherNullForNullReference = allOtherNullForNullReference && valueDesc.nullForNullReference();
                    }
                    hasIsNotNull = hasIsNotNull || valueDesc instanceof IsNotNullValue;
                }
                if (hasEmptyValue) {
                    if (hasIsNotNull) {
                        // EmptyValue and IsNotNull = FALSE
                        return IntersectType.FALSE;
                    }
                    // A and ((B and C) or ...)
                    // if A intersect B is EMPTY_VALUE, A intersect C is OTHERS, C is nullable
                    if (allOtherNullForNullReference) {
                        return IntersectType.EMPTY_VALUE;
                    }
                }
                return IntersectType.OTHERS;
            } else {
                // process A and (B or C) => A and B or A and C
                boolean hasEmptyValue = false;
                for (ValueDesc valueDesc : sourceValues) {
                    IntersectType type = valueDesc.getIntersectType(other, depth + 1);
                    if (type == IntersectType.OTHERS) {
                        return type;
                    }
                    hasEmptyValue = hasEmptyValue || type == IntersectType.EMPTY_VALUE;
                }

                // must hasEmptyValue or hasFalse
                return hasEmptyValue ? IntersectType.EMPTY_VALUE : IntersectType.FALSE;
            }
        }

        @Override
        protected UnionType getUnionType(ValueDesc other, int depth) {
            if ((!nullable() && other.nullable()) || depth > MAX_SEARCH_DEPTH) {
                return UnionType.OTHERS;
            }
            if (isAnd) {
                // process `A or (B and C)`:  => (A or B) and (A or C)
                boolean hasRangeAll = false;
                for (ValueDesc valueDesc : sourceValues) {
                    UnionType type = valueDesc.getUnionType(other, depth + 1);
                    if (type == UnionType.OTHERS) {
                        return type;
                    }
                    hasRangeAll = hasRangeAll || type == UnionType.RANGE_ALL;
                }
                // must hasRangeAll or hasTrue
                return hasRangeAll ? UnionType.RANGE_ALL : UnionType.TRUE;
            } else {
                // process 'A or ((B or C) and ...)'
                // then `this`: '(B or C)',  `other`: A
                boolean hasRangeAll = false;
                boolean hasIsNull = false;
                boolean allOtherNullForNullReference = true;
                for (ValueDesc valueDesc : sourceValues) {
                    UnionType type = valueDesc.getUnionType(other, depth + 1);
                    if (type == UnionType.TRUE) {
                        return type;
                    }
                    if (type == UnionType.RANGE_ALL) {
                        hasRangeAll = true;
                    } else {
                        allOtherNullForNullReference = allOtherNullForNullReference && valueDesc.nullForNullReference();
                    }
                    hasIsNull = hasIsNull || valueDesc instanceof IsNullValue;
                }
                if (hasRangeAll) {
                    if (hasIsNull) {
                        // A or ((B or C) and ....)
                        // if A union B is RANGE_ALL, C is IsNull
                        // then A or ((B or C) and ...) = A or (TRUE and ...)
                        // RangeAll or IsNull = TRUE
                        return UnionType.TRUE;
                    }
                    if (allOtherNullForNullReference) {
                        // A or ((B or C) and ....)
                        // if A union B is RANGE_ALL, and C is nullable
                        // then A or ((B or C) and ...) = A or (Range.all() and ...)
                        return UnionType.RANGE_ALL;
                    }
                }
                return UnionType.OTHERS;
            }
        }
    }

    /**
     * Represents unknown value expression.
     */
    public static class UnknownValue extends ValueDesc {

        public UnknownValue(ExpressionRewriteContext context, Expression expression) {
            super(context, expression);
        }

        @Override
        protected <R, C> R visit(ValueDescVisitor<R, C> visitor, C context) {
            return visitor.visitUnknownValue(this, context);
        }

        @Override
        protected boolean nullable() {
            return reference.nullable();
        }

        @Override
        protected boolean nullForNullReference() {
            return false;
        }

        @Override
        protected boolean containsAll(ValueDesc other, int depth) {
            // when merge all the value desc, the value desc's reference are the same.
            return other instanceof UnknownValue;
        }

        @Override
        protected IntersectType getIntersectType(ValueDesc other, int depth) {
            return IntersectType.OTHERS;
        }

        @Override
        protected UnionType getUnionType(ValueDesc other, int depth) {
            return UnionType.OTHERS;
        }
    }
}
