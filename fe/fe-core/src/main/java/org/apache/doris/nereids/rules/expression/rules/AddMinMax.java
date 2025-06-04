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
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.DiscreteValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.EmptyValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.RangeValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.UnknownValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.ValueDesc;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.ComparableLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.NotImplementedException;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class implements the function to add min max to or expression.
 * for example:
 *
 * a > 10 and a < 20 or a > 30 and a < 40 or a > 50 and a < 60
 *   => (a < 20 or a > 30 and a < 40 or a > 50) and a > 10 and a < 60
 *
 * a between 10 and 20 and b between 10 and 20 or a between 100 and 200 and b between 100 and 200
 *   => (a <= 20 and b <= 20 or a >= 100 and b >= 100) and a >= 10 and a <= 200 and b >= 10 and b <= 200
 */
public class AddMinMax implements ExpressionPatternRuleFactory {
    public static final AddMinMax INSTANCE = new AddMinMax();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesTopType(CompoundPredicate.class)
                        .thenApply(ctx -> rewrite(ctx.expr, ctx.rewriteContext))
                        .toRule(ExpressionRuleType.ADD_MIN_MAX)
        );
    }

    /** rewrite */
    public Expression rewrite(CompoundPredicate expr, ExpressionRewriteContext context) {
        ValueDesc valueDesc = (new RangeInference()).getValue(expr, context);
        Map<Expression, MinMaxValue> exprMinMaxValues = getExprMinMaxValues(valueDesc);
        removeUnnecessaryMinMaxValues(expr, exprMinMaxValues);
        if (!exprMinMaxValues.isEmpty()) {
            return addExprMinMaxValues(expr, context, exprMinMaxValues);
        }
        return expr;
    }

    private enum MatchMinMax {
        MATCH_MIN,
        MATCH_MAX,
        MATCH_NONE,
    }

    private static class MinMaxValue {
        // min max range, if range = null means empty
        Range<ComparableLiteral> range;

        // expression in range is discrete value
        boolean isDiscrete;

        // expr relative order, for keep order after add min-max to the expression
        int exprOrderIndex;

        public MinMaxValue(Range<ComparableLiteral> range, boolean isDiscrete, int exprOrderIndex) {
            this.range = range;
            this.isDiscrete = isDiscrete;
            this.exprOrderIndex = exprOrderIndex;
        }
    }

    private void removeUnnecessaryMinMaxValues(Expression expr, Map<Expression, MinMaxValue> exprMinMaxValues) {
        exprMinMaxValues.entrySet().removeIf(entry -> entry.getValue().isDiscrete || entry.getValue().range == null
                || (!entry.getValue().range.hasLowerBound() && !entry.getValue().range.hasUpperBound()));
        if (exprMinMaxValues.isEmpty()) {
            return;
        }

        // keep original expression order, don't rewrite a sub expression if it's in original conjunctions.
        // example: if original expression is:  '(a >= 100) AND (...)',  and after visiting got a's range is [100, 200],
        // because 'a >= 100' is in expression's conjunctions, don't add 'a >= 100' to expression,
        // then the rewritten expression is '((a >= 100) AND (...)) AND (a <= 200)'
        List<Expression> conjuncts = ExpressionUtils.extractConjunction(expr);
        for (Expression conjunct : conjuncts) {
            List<Expression> disjunctions = ExpressionUtils.extractDisjunction(conjunct);
            if (disjunctions.isEmpty() || !(disjunctions.get(0) instanceof ComparisonPredicate)) {
                continue;
            }
            Expression targetExpr = disjunctions.get(0).child(0);
            boolean matchMin = false;
            boolean matchMax = false;
            for (Expression disjunction : disjunctions) {
                MatchMinMax match = getExprMatchMinMax(disjunction, exprMinMaxValues);
                if (match == MatchMinMax.MATCH_NONE || !disjunction.child(0).equals(targetExpr)) {
                    matchMin = false;
                    matchMax = false;
                    break;
                }
                if (match == MatchMinMax.MATCH_MIN) {
                    matchMin = true;
                } else if (match == MatchMinMax.MATCH_MAX) {
                    matchMax = true;
                }
            }
            MinMaxValue targetValue = exprMinMaxValues.get(targetExpr);
            if (matchMin) {
                // remove targetValue's lower bound
                if (targetValue.range.hasUpperBound()) {
                    targetValue.range = Range.upTo(targetValue.range.upperEndpoint(),
                        targetValue.range.upperBoundType());
                } else {
                    exprMinMaxValues.remove(targetExpr);
                }
            }
            if (matchMax) {
                // remove targetValue's upper bound
                if (targetValue.range.hasLowerBound()) {
                    targetValue.range = Range.downTo(targetValue.range.lowerEndpoint(),
                        targetValue.range.lowerBoundType());
                } else {
                    exprMinMaxValues.remove(targetExpr);
                }
            }
        }
    }

    private Expression addExprMinMaxValues(Expression expr, ExpressionRewriteContext context,
            Map<Expression, MinMaxValue> exprMinMaxValues) {
        List<Map.Entry<Expression, MinMaxValue>> minMaxExprs = exprMinMaxValues.entrySet().stream()
                .sorted((a, b) -> Integer.compare(a.getValue().exprOrderIndex, b.getValue().exprOrderIndex))
                .collect(Collectors.toList());
        List<Expression> addExprs = Lists.newArrayListWithExpectedSize(minMaxExprs.size() * 2);
        for (Map.Entry<Expression, MinMaxValue> entry : minMaxExprs) {
            Expression targetExpr = entry.getKey();
            Range<ComparableLiteral> range = entry.getValue().range;
            if (range.hasLowerBound() && range.hasUpperBound()
                    && range.lowerEndpoint().equals(range.upperEndpoint())
                    && range.lowerBoundType() == BoundType.CLOSED
                    && range.upperBoundType() == BoundType.CLOSED) {
                Expression cmp = new EqualTo(targetExpr, (Literal) range.lowerEndpoint());
                addExprs.add(cmp);
                continue;
            }
            if (range.hasLowerBound()) {
                ComparableLiteral literal = range.lowerEndpoint();
                Expression cmp = range.lowerBoundType() == BoundType.CLOSED
                        ? new GreaterThanEqual(targetExpr, (Literal) literal)
                        : new GreaterThan(targetExpr, (Literal) literal);
                addExprs.add(cmp);
            }
            if (range.hasUpperBound()) {
                ComparableLiteral literal = range.upperEndpoint();
                Expression cmp = range.upperBoundType() == BoundType.CLOSED
                        ? new LessThanEqual(targetExpr, (Literal) literal)
                        : new LessThan(targetExpr, (Literal) literal);
                addExprs.add(cmp);
            }
        }

        // later will add `addExprs` to original expr, before doing that, remove duplicate expr in original expr
        Expression replaceOriginExpr = replaceCmpMinMax(expr, Sets.newHashSet(addExprs));

        addExprs.add(0, replaceOriginExpr);
        Expression result = FoldConstantRuleOnFE.evaluate(ExpressionUtils.and(addExprs), context);
        if (result.equals(expr)) {
            return expr;
        }

        return result;
    }

    private Expression replaceCmpMinMax(Expression expr, Set<Expression> cmpMinMaxExprs) {
        // even if expr is nullable, replace it to true is ok because expression will 'AND' it later
        if (cmpMinMaxExprs.contains(expr)) {
            return BooleanLiteral.TRUE;
        }

        // only replace those expression whose all its ancestors are AND / OR
        if (!(expr instanceof CompoundPredicate)) {
            return expr;
        }

        ImmutableList.Builder<Expression> newChildren = ImmutableList.builderWithExpectedSize(expr.arity());
        boolean changed = false;
        for (Expression child : expr.children()) {
            Expression newChild = replaceCmpMinMax(child, cmpMinMaxExprs);
            if (child != newChild) {
                changed = true;
            }
            newChildren.add(newChild);
        }

        if (changed) {
            return expr.withChildren(newChildren.build());
        } else {
            return expr;
        }
    }

    private MatchMinMax getExprMatchMinMax(Expression expr,
            Map<Expression, MinMaxValue> exprMinMaxValues) {
        if (!(expr instanceof ComparisonPredicate)) {
            return MatchMinMax.MATCH_NONE;
        }

        ComparisonPredicate cp = (ComparisonPredicate) expr;
        Expression left = cp.left();
        Expression right = cp.right();
        if (!(right instanceof ComparableLiteral)) {
            return MatchMinMax.MATCH_NONE;
        }

        MinMaxValue value = exprMinMaxValues.get(left);
        if (value == null) {
            return MatchMinMax.MATCH_NONE;
        }

        if (cp instanceof GreaterThan || cp instanceof GreaterThanEqual) {
            if (value.range.hasLowerBound() && value.range.lowerEndpoint().equals(right)) {
                BoundType boundType = value.range.lowerBoundType();
                if ((boundType == BoundType.CLOSED && expr instanceof GreaterThanEqual)
                        || (boundType == BoundType.OPEN && expr instanceof GreaterThan)) {
                    return MatchMinMax.MATCH_MIN;
                }
            }
        } else if (expr instanceof LessThan || expr instanceof LessThanEqual) {
            if (value.range.hasUpperBound() && value.range.upperEndpoint().equals(right)) {
                BoundType boundType = value.range.upperBoundType();
                if ((boundType == BoundType.CLOSED && expr instanceof LessThanEqual)
                        || (boundType == BoundType.OPEN && expr instanceof LessThan)) {
                    return MatchMinMax.MATCH_MAX;
                }
            }
        }

        return MatchMinMax.MATCH_NONE;
    }

    private boolean isExprNeedAddMinMax(Expression expr) {
        return (expr instanceof SlotReference) && ((SlotReference) expr).getOriginalColumn().isPresent();
    }

    private Map<Expression, MinMaxValue> getExprMinMaxValues(ValueDesc value) {
        if (value instanceof EmptyValue) {
            return getExprMinMaxValues((EmptyValue) value);
        } else if (value instanceof DiscreteValue) {
            return getExprMinMaxValues((DiscreteValue) value);
        } else if (value instanceof RangeValue) {
            return getExprMinMaxValues((RangeValue) value);
        } else if (value instanceof UnknownValue) {
            return getExprMinMaxValues((UnknownValue) value);
        } else {
            throw new NotImplementedException("not implements");
        }
    }

    private Map<Expression, MinMaxValue> getExprMinMaxValues(EmptyValue value) {
        Expression reference = value.getReference();
        Map<Expression, MinMaxValue> exprMinMaxValues = Maps.newHashMap();
        if (isExprNeedAddMinMax(reference)) {
            exprMinMaxValues.put(reference, new MinMaxValue(null, true, 0));
        }
        return exprMinMaxValues;
    }

    private Map<Expression, MinMaxValue> getExprMinMaxValues(DiscreteValue value) {
        Expression reference = value.getReference();
        Map<Expression, MinMaxValue> exprMinMaxValues = Maps.newHashMap();
        if (isExprNeedAddMinMax(reference)) {
            exprMinMaxValues.put(reference, new MinMaxValue(Range.encloseAll(value.getValues()), true, 0));
        }
        return exprMinMaxValues;
    }

    private Map<Expression, MinMaxValue> getExprMinMaxValues(RangeValue value) {
        Expression reference = value.getReference();
        Map<Expression, MinMaxValue> exprMinMaxValues = Maps.newHashMap();
        if (isExprNeedAddMinMax(reference)) {
            exprMinMaxValues.put(reference, new MinMaxValue(value.getRange(), false, 0));
        }
        return exprMinMaxValues;
    }

    private Map<Expression, MinMaxValue> getExprMinMaxValues(UnknownValue valueDesc) {
        List<ValueDesc> sourceValues = valueDesc.getSourceValues();
        if (sourceValues.isEmpty()) {
            return Maps.newHashMap();
        }
        Map<Expression, MinMaxValue> result = Maps.newHashMap(getExprMinMaxValues(sourceValues.get(0)));
        int nextExprOrderIndex = result.values().stream().mapToInt(k -> k.exprOrderIndex).max().orElse(0);
        for (int i = 1; i < sourceValues.size(); i++) {
            // process in sourceValues[i]
            Map<Expression, MinMaxValue> minMaxValues = getExprMinMaxValues(sourceValues.get(i));
            // merge values of sourceValues[i] into result.
            // also keep the value's relative order in sourceValues[i].
            // for example, if a and b in sourceValues[i], but not in result, then during merging,
            // a and b will assign a new exprOrderIndex (using nextExprOrderIndex).
            // if in sourceValues[i], a's exprOrderIndex < b's exprOrderIndex,
            // then make sure in result, a's new exprOrderIndex < b's new exprOrderIndex.
            // so that their relative order can preserve.
            List<Map.Entry<Expression, MinMaxValue>> minMaxValueList = minMaxValues.entrySet().stream()
                    .sorted((a, b) -> Integer.compare(a.getValue().exprOrderIndex, b.getValue().exprOrderIndex))
                    .collect(Collectors.toList());
            for (Map.Entry<Expression, MinMaxValue> entry : minMaxValueList) {
                Expression expr = entry.getKey();
                MinMaxValue value = result.get(expr);
                MinMaxValue otherValue = entry.getValue();
                if (valueDesc.isAnd()) {
                    if (value == null) { // value = null means range for all
                        nextExprOrderIndex++;
                        value = otherValue;
                        value.exprOrderIndex = nextExprOrderIndex;
                        result.put(expr, value);
                    } else if (otherValue.range == null) { // range = null means empty range
                        value.range = null;
                    } else if (value.range != null) {
                        if (value.range.isConnected(otherValue.range)) {
                            Range<ComparableLiteral> newRange = value.range.intersection(otherValue.range);
                            if (!newRange.isEmpty()) {
                                value.range = newRange;
                                // If newRange.lowerEndpoint().equals(newRange.upperEndpoint()),
                                // then isDiscrete should be true.
                                // But no need to do that because AddMinMax will not handle discrete value cases.
                                value.isDiscrete = value.isDiscrete && otherValue.isDiscrete;
                            } else {
                                value.range = null;
                            }
                        } else {
                            value.range = null;
                        }
                    }
                } else {
                    if (value == null) { // value = null means range for all
                        nextExprOrderIndex++;
                        value = new MinMaxValue(Range.all(), false, nextExprOrderIndex);
                        result.put(expr, value);
                    } else if (value.range == null) { // range = null means empty range
                        value.range = otherValue.range;
                        value.isDiscrete = otherValue.isDiscrete;
                    } else if (otherValue.range != null) {
                        value.range = value.range.span(otherValue.range);
                        value.isDiscrete = value.isDiscrete && otherValue.isDiscrete;
                    }
                }
            }

            // process not in sourceValues[i]
            if (!valueDesc.isAnd()) {
                for (Map.Entry<Expression, MinMaxValue> entry : result.entrySet()) {
                    Expression expr = entry.getKey();
                    MinMaxValue value = entry.getValue();
                    if (!minMaxValues.containsKey(expr)) {
                        value.range = Range.all();
                        value.isDiscrete = false;
                    }
                }
            }
        }
        return result;
    }
}
