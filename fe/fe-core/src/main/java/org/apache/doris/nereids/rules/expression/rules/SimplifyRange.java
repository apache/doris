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
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

/**
 * This class implements the function to simplify expression range.
 * for example:
 *
 * a > 1 and a > 2 => a > 2
 * a > 1 or a > 2 => a > 1
 * a in (1,2,3) and a > 1 => a in (2,3)
 * a in (1,2,3) and a in (3,4,5) => a = 3
 * a in (1,2,3) and a in (4,5,6) => false
 *
 * also add min max:
 *
 * a > 10 and a < 20 or a > 30 and a < 40 or a > 50 and a < 60
 *   => (a < 20 or a > 30 and a < 40 or a > 50) and a > 10 and a < 60
 *
 * a between 10 and 20 and b between 10 and 20 or a between 100 and 200 and b between 100 and 200
 *   => (a <= 20 and b <= 20 or a >= 100 and b >= 100) and a >= 10 and a <= 200 and b >= 10 and b <= 200
 *
 * The logic is as follows:
 * 1. for `And` expression.
 *    1. extract conjunctions then build `ValueDesc` for each conjunction
 *    2. grouping according to `reference`, `ValueDesc` in the same group can perform intersect
 *    for example:
 *    a > 1 and a > 2
 *    1. a > 1 => RangeValueDesc((1...+∞)), a > 2 => RangeValueDesc((2...+∞))
 *    2. (1...+∞) intersect (2...+∞) => (2...+∞)
 * 2. for `Or` expression (similar to `And`).
 * 3. after simplify the expression, also get each expression's min and max value,
 *    then add like `e > min and e < max` to the expression.
 * todo: support a > 10 and (a < 10 or a > 20 ) => a > 20
 */
public class SimplifyRange implements ExpressionPatternRuleFactory {
    public static final SimplifyRange INSTANCE = new SimplifyRange();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesTopType(CompoundPredicate.class)
                        .thenApply(ctx -> SimplifyRange.rewrite(ctx.expr, ctx.rewriteContext))
        );
    }

    /** rewrite */
    public static Expression rewrite(CompoundPredicate expr, ExpressionRewriteContext context) {
        ValueDesc valueDesc = expr.accept(new RangeInference(), context);
        Expression result = valueDesc.toExpression();
        Map<Expression, MinMaxValue> exprMinMaxValues = valueDesc.getExprMinMaxValues();
        removeUnnecessaryMinMaxValues(result, exprMinMaxValues);
        if (!exprMinMaxValues.isEmpty()) {
            result = addExprMinMaxValues(result, context, exprMinMaxValues);
        }
        return result;
    }

    private static void removeUnnecessaryMinMaxValues(Expression expr, Map<Expression, MinMaxValue> exprMinMaxValues) {
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

    private static Expression addExprMinMaxValues(Expression expr, ExpressionRewriteContext context,
            Map<Expression, MinMaxValue> exprMinMaxValues) {
        List<Map.Entry<Expression, MinMaxValue>> minMaxExprs = exprMinMaxValues.entrySet().stream()
                .sorted((a, b) -> Integer.compare(a.getValue().exprOrderIndex, b.getValue().exprOrderIndex))
                .collect(Collectors.toList());
        List<Expression> addExprs = Lists.newArrayListWithExpectedSize(minMaxExprs.size() * 2);
        for (Map.Entry<Expression, MinMaxValue> entry : minMaxExprs) {
            Expression targetExpr = entry.getKey();
            Range<Literal> range = entry.getValue().range;
            if (range.hasLowerBound() && range.hasUpperBound()
                    && range.lowerEndpoint().equals(range.upperEndpoint())
                    && range.lowerBoundType() == BoundType.CLOSED
                    && range.upperBoundType() == BoundType.CLOSED) {
                Expression cmp = new EqualTo(targetExpr, range.lowerEndpoint());
                addExprs.add(cmp);
                continue;
            }
            if (range.hasLowerBound()) {
                Literal literal = range.lowerEndpoint();
                Expression cmp = range.lowerBoundType() == BoundType.CLOSED
                        ? new GreaterThanEqual(targetExpr, literal) : new GreaterThan(targetExpr, literal);
                addExprs.add(cmp);
            }
            if (range.hasUpperBound()) {
                Literal literal = range.upperEndpoint();
                Expression cmp = range.upperBoundType() == BoundType.CLOSED
                        ? new LessThanEqual(targetExpr, literal) : new LessThan(targetExpr, literal);
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

    private static Expression replaceCmpMinMax(Expression expr, Set<Expression> cmpMinMaxExprs) {
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

    private static MatchMinMax getExprMatchMinMax(Expression expr,
            Map<Expression, MinMaxValue> exprMinMaxValues) {
        if (!(expr instanceof ComparisonPredicate)) {
            return MatchMinMax.MATCH_NONE;
        }

        ComparisonPredicate cp = (ComparisonPredicate) expr;
        Expression left = cp.left();
        Expression right = cp.right();
        if (!(right instanceof Literal)) {
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

    private static class RangeInference extends ExpressionVisitor<ValueDesc, ExpressionRewriteContext> {

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

            Multimap<Expression, ValueDesc> groupByReference
                    = Multimaps.newListMultimap(new LinkedHashMap<>(), ArrayList::new);
            for (Expression predicate : predicates) {
                ValueDesc valueDesc = predicate.accept(this, context);
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
    }

    private abstract static class ValueDesc {
        ExpressionRewriteContext context;
        Expression toExpr;
        Expression reference;

        public ValueDesc(ExpressionRewriteContext context, Expression reference, Expression toExpr) {
            this.context = context;
            this.toExpr = toExpr;
            this.reference = reference;
        }

        public abstract ValueDesc union(ValueDesc other);

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

        public abstract ValueDesc intersect(ValueDesc other);

        public static ValueDesc intersect(ExpressionRewriteContext context, RangeValue range, DiscreteValue discrete) {
            DiscreteValue result = new DiscreteValue(context, discrete.reference, discrete.toExpr);
            discrete.values.stream().filter(x -> range.range.contains(x)).forEach(result.values::add);
            if (!result.values.isEmpty()) {
                return result;
            }
            Expression originExpr = FoldConstantRuleOnFE.evaluate(new And(range.toExpr, discrete.toExpr), context);
            return new EmptyValue(context, range.reference, originExpr);
        }

        public abstract Expression toExpression();

        // expr => pair(minMaxValue, isDiscreteValue)
        public abstract Map<Expression, MinMaxValue> getExprMinMaxValues();

        protected boolean isExprNeedAddMinMax(Expression expr) {
            return (expr instanceof SlotReference) && ((SlotReference) expr).getColumn().isPresent();
        }

        public static ValueDesc range(ExpressionRewriteContext context, ComparisonPredicate predicate) {
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

    private static class EmptyValue extends ValueDesc {

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

        @Override
        public Expression toExpression() {
            if (reference.nullable()) {
                return new And(new IsNull(reference), new NullLiteral(BooleanType.INSTANCE));
            } else {
                return BooleanLiteral.FALSE;
            }
        }

        @Override
        public Map<Expression, MinMaxValue> getExprMinMaxValues() {
            Map<Expression, MinMaxValue> exprMinMaxValues = Maps.newHashMap();
            if (isExprNeedAddMinMax(reference)) {
                exprMinMaxValues.put(reference, new MinMaxValue(null, true, 0));
            }
            return exprMinMaxValues;
        }
    }

    /**
     * use @see com.google.common.collect.Range to wrap `ComparisonPredicate`
     * for example:
     * a > 1 => (1...+∞)
     */
    private static class RangeValue extends ValueDesc {
        Range<Literal> range;

        public RangeValue(ExpressionRewriteContext context, Expression reference,
                Expression toExpr, Range<Literal> range) {
            super(context, reference, toExpr);
            this.range = range;
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
            } else {
                if (reference.nullable()) {
                    return new Or(new Not(new IsNull(reference)), new NullLiteral(BooleanType.INSTANCE));
                } else {
                    return BooleanLiteral.TRUE;
                }
            }
        }

        @Override
        public Map<Expression, MinMaxValue> getExprMinMaxValues() {
            Map<Expression, MinMaxValue> exprMinMaxValues = Maps.newHashMap();
            if (isExprNeedAddMinMax(reference)) {
                exprMinMaxValues.put(reference, new MinMaxValue(range, false, 0));
            }
            return exprMinMaxValues;
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

        public DiscreteValue(ExpressionRewriteContext context,
                Expression reference, Expression toExpr, Literal... values) {
            this(context, reference, toExpr, Arrays.asList(values));
        }

        public DiscreteValue(ExpressionRewriteContext context,
                Expression reference, Expression toExpr, Collection<Literal> values) {
            super(context, reference, toExpr);
            this.values = Sets.newTreeSet(values);
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
        public Map<Expression, MinMaxValue> getExprMinMaxValues() {
            Map<Expression, MinMaxValue> exprMinMaxValues = Maps.newHashMap();
            if (isExprNeedAddMinMax(reference)) {
                exprMinMaxValues.put(reference, new MinMaxValue(Range.encloseAll(values), true, 0));
            }
            return exprMinMaxValues;
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

        @Override
        public ValueDesc union(ValueDesc other) {
            Expression originExpr = FoldConstantRuleOnFE.evaluate(
                    ExpressionUtils.or(toExpr, other.toExpr), context);
            return new UnknownValue(context, originExpr,
                    ImmutableList.of(this, other), false);
        }

        @Override
        public ValueDesc intersect(ValueDesc other) {
            Expression originExpr = FoldConstantRuleOnFE.evaluate(
                    ExpressionUtils.and(toExpr, other.toExpr), context);
            return new UnknownValue(context, originExpr,
                    ImmutableList.of(this, other), true);
        }

        @Override
        public Expression toExpression() {
            if (sourceValues.isEmpty()) {
                return toExpr;
            }
            List<Expression> sourceExprs = sourceValues.stream().map(ValueDesc::toExpression)
                    .collect(Collectors.toList());
            Expression result = isAnd ? ExpressionUtils.and(sourceExprs) : ExpressionUtils.or(sourceExprs);
            result = FoldConstantRuleOnFE.evaluate(result, context);
            // ATTN: we must return original expr, because OrToIn is implemented with MutableState,
            //   newExpr will lose these states leading to dead loop by OrToIn -> SimplifyRange -> FoldConstantByFE
            if (result.equals(toExpr)) {
                return toExpr;
            }
            return result;
        }

        @Override
        public Map<Expression, MinMaxValue> getExprMinMaxValues() {
            if (sourceValues.isEmpty()) {
                return Maps.newHashMap();
            }
            Map<Expression, MinMaxValue> result = Maps.newHashMap(sourceValues.get(0).getExprMinMaxValues());
            int nextExprOrderIndex = result.values().stream().mapToInt(k -> k.exprOrderIndex).max().orElse(0);
            for (int i = 1; i < sourceValues.size(); i++) {
                // process in sourceValues[i]
                Map<Expression, MinMaxValue> minMaxValues = sourceValues.get(i).getExprMinMaxValues();
                for (Map.Entry<Expression, MinMaxValue> entry : minMaxValues.entrySet()) {
                    Expression expr = entry.getKey();
                    MinMaxValue value = result.get(expr);
                    MinMaxValue otherValue = entry.getValue();
                    if (isAnd) {
                        if (value == null) { // value = null means range for all
                            nextExprOrderIndex++;
                            value = otherValue;
                            value.exprOrderIndex = nextExprOrderIndex;
                            result.put(expr, value);
                        } else if (otherValue.range == null) { // range = null means empty range
                            value.range = null;
                        } else if (value.range != null) {
                            if (value.range.isConnected(otherValue.range)) {
                                value.range = value.range.intersection(otherValue.range);
                                value.isDiscrete = value.isDiscrete && otherValue.isDiscrete;
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
                if (!isAnd) {
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

    private enum MatchMinMax {
        MATCH_MIN,
        MATCH_MAX,
        MATCH_NONE,
    }

    private static class MinMaxValue {
        // min max range, if range = null means empty
        Range<Literal> range;

        // expression in range is discrete value
        boolean isDiscrete;

        // expr relative order, for keep order after add min-max to the expression
        int exprOrderIndex;

        public MinMaxValue(Range<Literal> range, boolean isDiscrete, int exprOrderIndex) {
            this.range = range;
            this.isDiscrete = isDiscrete;
            this.exprOrderIndex = exprOrderIndex;
        }
    }
}
