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
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
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
import java.util.Map.Entry;
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
 * a in (1,2,3) and a in (4,5,6) => false
 * The logic is as follows:
 * 1. for `And` expression.
 *    1. extract conjunctions then build `ValueDesc` for each conjunction
 *    2. grouping according to `reference`, `ValueDesc` in the same group can perform intersect
 *    for example:
 *    a > 1 and a > 2
 *    1. a > 1 => RangeValueDesc((1...+∞)), a > 2 => RangeValueDesc((2...+∞))
 *    2. (1...+∞) intersect (2...+∞) => (2...+∞)
 * 2. for `Or` expression (similar to `And`).
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
        Expression toExpr = valueDesc.toExpression();
        if (toExpr == null) {
            // this mean cannot simplify
            return valueDesc.toExpr;
        }
        return toExpr;
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
                    ValueDesc::intersect, ExpressionUtils::and);
        }

        @Override
        public ValueDesc visitOr(Or or, ExpressionRewriteContext context) {
            return simplify(context, or, ExpressionUtils.extractDisjunction(or),
                    ValueDesc::union, ExpressionUtils::or);
        }

        private ValueDesc simplify(ExpressionRewriteContext context,
                Expression originExpr, List<Expression> predicates,
                BinaryOperator<ValueDesc> op, BinaryOperator<Expression> exprOp) {

            Multimap<Expression, ValueDesc> groupByReference
                    = Multimaps.newListMultimap(new LinkedHashMap<>(), ArrayList::new);
            for (Expression predicate : predicates) {
                ValueDesc valueDesc = predicate.accept(this, null);
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
            return new UnknownValue(context, originExpr, valuePerRefs, exprOp);
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
                    ExpressionUtils.or(range.toExpr, discrete.toExpr), context);
            List<ValueDesc> sourceValues = reverseOrder
                    ? ImmutableList.of(discrete, range)
                    : ImmutableList.of(range, discrete);
            return new UnknownValue(context, toExpr, sourceValues, ExpressionUtils::or);
        }

        public abstract ValueDesc intersect(ValueDesc other);

        public static ValueDesc intersect(ExpressionRewriteContext context, RangeValue range, DiscreteValue discrete) {
            DiscreteValue result = new DiscreteValue(context, discrete.reference, discrete.toExpr);
            discrete.values.stream().filter(x -> range.range.contains(x)).forEach(result.values::add);
            if (!result.values.isEmpty()) {
                return result;
            }
            Expression originExpr = FoldConstantRuleOnFE.evaluate(
                    ExpressionUtils.and(range.toExpr, discrete.toExpr), context);
            return new EmptyValue(context, range.reference, originExpr);
        }

        public abstract Expression toExpression();

        public static ValueDesc range(ExpressionRewriteContext context, ComparisonPredicate predicate) {
            Literal value = (Literal) predicate.right();
            if (predicate instanceof EqualTo) {
                return new DiscreteValue(context, predicate.left(), predicate, value);
            }
            RangeValue rangeValue = new RangeValue(context, predicate.left(), predicate);
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
    }

    /**
     * use @see com.google.common.collect.Range to wrap `ComparisonPredicate`
     * for example:
     * a > 1 => (1...+∞)
     */
    private static class RangeValue extends ValueDesc {
        Range<Literal> range;

        public RangeValue(ExpressionRewriteContext context, Expression reference, Expression toExpr) {
            super(context, reference, toExpr);
        }

        @Override
        public ValueDesc union(ValueDesc other) {
            if (other instanceof EmptyValue) {
                return other.union(this);
            }
            if (other instanceof RangeValue) {
                Expression originExpr = FoldConstantRuleOnFE.evaluate(
                        ExpressionUtils.or(toExpr, other.toExpr), context);
                RangeValue o = (RangeValue) other;
                if (range.isConnected(o.range)) {
                    RangeValue rangeValue = new RangeValue(context, reference, originExpr);
                    rangeValue.range = range.span(o.range);
                    return rangeValue;
                }
                return new UnknownValue(context, originExpr,
                        ImmutableList.of(this, other), ExpressionUtils::or);
            }
            if (other instanceof DiscreteValue) {
                return union(context, this, (DiscreteValue) other, false);
            }
            Expression originExpr = FoldConstantRuleOnFE.evaluate(
                    ExpressionUtils.or(toExpr, other.toExpr), context);
            return new UnknownValue(context, originExpr,
                    ImmutableList.of(this, other), ExpressionUtils::or);
        }

        @Override
        public ValueDesc intersect(ValueDesc other) {
            if (other instanceof EmptyValue) {
                return other.intersect(this);
            }
            if (other instanceof RangeValue) {
                Expression originExpr = FoldConstantRuleOnFE.evaluate(
                        ExpressionUtils.and(toExpr, other.toExpr), context);
                RangeValue o = (RangeValue) other;
                if (range.isConnected(o.range)) {
                    RangeValue rangeValue = new RangeValue(context, reference, originExpr);
                    rangeValue.range = range.intersection(o.range);
                    return rangeValue;
                }
                return new EmptyValue(context, reference, originExpr);
            }
            if (other instanceof DiscreteValue) {
                return intersect(context, this, (DiscreteValue) other);
            }
            Expression originExpr = FoldConstantRuleOnFE.evaluate(
                    ExpressionUtils.and(toExpr, other.toExpr), context);
            return new UnknownValue(context, originExpr,
                    ImmutableList.of(this, other), ExpressionUtils::and);
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
            this.values = Sets.newHashSet(values);
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
                    ImmutableList.of(this, other), ExpressionUtils::or);
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
                    ImmutableList.of(this, other), ExpressionUtils::and);
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
        public String toString() {
            return values.toString();
        }
    }

    /**
     * Represents processing result.
     */
    private static class UnknownValue extends ValueDesc {
        private final List<ValueDesc> sourceValues;
        private final BinaryOperator<Expression> mergeExprOp;

        private UnknownValue(ExpressionRewriteContext context, Expression expr) {
            super(context, expr, expr);
            sourceValues = ImmutableList.of();
            mergeExprOp = null;
        }

        public UnknownValue(ExpressionRewriteContext context, Expression toExpr,
                List<ValueDesc> sourceValues, BinaryOperator<Expression> mergeExprOp) {
            super(context, getReference(sourceValues, toExpr), toExpr);
            this.sourceValues = ImmutableList.copyOf(sourceValues);
            this.mergeExprOp = mergeExprOp;
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
                    ImmutableList.of(this, other), ExpressionUtils::or);
        }

        @Override
        public ValueDesc intersect(ValueDesc other) {
            Expression originExpr = FoldConstantRuleOnFE.evaluate(
                    ExpressionUtils.and(toExpr, other.toExpr), context);
            return new UnknownValue(context, originExpr,
                    ImmutableList.of(this, other), ExpressionUtils::and);
        }

        @Override
        public Expression toExpression() {
            if (sourceValues.isEmpty()) {
                return toExpr;
            }
            Expression result = sourceValues.get(0).toExpression();
            for (int i = 1; i < sourceValues.size(); i++) {
                result = mergeExprOp.apply(result, sourceValues.get(i).toExpression());
            }
            result = FoldConstantRuleOnFE.evaluate(result, context);
            // ATTN: we must return original expr, because OrToIn is implemented with MutableState,
            //   newExpr will lose these states leading to dead loop by OrToIn -> SimplifyRange -> FoldConstantByFE
            if (result.equals(toExpr)) {
                return toExpr;
            }
            return result;
        }
    }
}
