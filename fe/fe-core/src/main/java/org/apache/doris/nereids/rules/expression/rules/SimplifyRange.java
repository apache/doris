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
import org.apache.doris.nereids.rules.expression.rules.RangeInference.CompoundValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.DiscreteValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.EmptyValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.IsNotNullValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.IsNullValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.NotDiscreteValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.RangeValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.UnknownValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.ValueDesc;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.ValueDescVisitor;
import org.apache.doris.nereids.rules.rewrite.SkipSimpleExprs;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.literal.ComparableLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;

import java.util.List;
import java.util.Set;

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
public class SimplifyRange implements ExpressionPatternRuleFactory, ValueDescVisitor<Expression, Void> {
    public static final SimplifyRange INSTANCE = new SimplifyRange();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesTopType(CompoundPredicate.class)
                        .when(c -> c.containsType(Literal.class, IsNull.class))
                        .thenApply(ctx -> rewrite(ctx.expr, ctx.rewriteContext))
                        .toRule(ExpressionRuleType.SIMPLIFY_RANGE)
        );
    }

    /** rewrite */
    public Expression rewrite(CompoundPredicate expr, ExpressionRewriteContext context) {
        if (SkipSimpleExprs.isSimpleExpr(expr)) {
            return expr;
        }
        ValueDesc valueDesc = (new RangeInference()).getValue(expr, context);
        return valueDesc.accept(this, null);
    }

    @Override
    public Expression visitEmptyValue(EmptyValue value, Void context) {
        Expression reference = value.getReference();
        return ExpressionUtils.falseOrNull(reference);
    }

    @Override
    public Expression visitRangeValue(RangeValue value, Void context) {
        Expression reference = value.getReference();
        Range<ComparableLiteral> range = value.getRange();
        List<Expression> result = Lists.newArrayList();
        if (range.hasLowerBound()) {
            if (range.lowerBoundType() == BoundType.CLOSED) {
                result.add(new GreaterThanEqual(reference, (Literal) range.lowerEndpoint()));
            } else {
                result.add(new GreaterThan(reference, (Literal) range.lowerEndpoint()));
            }
        }
        if (range.hasUpperBound()) {
            if (range.upperBoundType() == BoundType.CLOSED) {
                result.add(new LessThanEqual(reference, (Literal) range.upperEndpoint()));
            } else {
                result.add(new LessThan(reference, (Literal) range.upperEndpoint()));
            }
        }
        if (!result.isEmpty()) {
            return ExpressionUtils.and(result);
        } else {
            return ExpressionUtils.trueOrNull(reference);
        }
    }

    @Override
    public Expression visitDiscreteValue(DiscreteValue value, Void context) {
        return getDiscreteExpression(value.getReference(), value.values);
    }

    @Override
    public Expression visitNotDiscreteValue(NotDiscreteValue value, Void context) {
        return new Not(getDiscreteExpression(value.getReference(), value.values));
    }

    @Override
    public Expression visitIsNullValue(IsNullValue value, Void context) {
        return new IsNull(value.getReference());
    }

    @Override
    public Expression visitIsNotNullValue(IsNotNullValue value, Void context) {
        return value.getNotExpression();
    }

    @Override
    public Expression visitCompoundValue(CompoundValue value, Void context) {
        return getCompoundExpression(value.getExpressionRewriteContext(), value.getSourceValues(), value.isAnd());
    }

    @Override
    public Expression visitUnknownValue(UnknownValue value, Void context) {
        return value.getReference();
    }

    private Expression getDiscreteExpression(Expression reference, Set<ComparableLiteral> values) {
        ImmutableList.Builder<Expression> options = ImmutableList.builderWithExpectedSize(values.size());
        for (ComparableLiteral value : values) {
            options.add((Expression) value);
        }
        return ExpressionUtils.toInPredicateOrEqualTo(reference, options.build());
    }

    /** getExpression */
    public Expression getCompoundExpression(ExpressionRewriteContext context,
            List<ValueDesc> sourceValues, boolean isAnd) {
        Preconditions.checkArgument(!sourceValues.isEmpty());
        List<Expression> sourceExprs = Lists.newArrayListWithExpectedSize(sourceValues.size());
        for (ValueDesc sourceValue : sourceValues) {
            Expression expr = sourceValue.accept(this, null);
            if (isAnd) {
                sourceExprs.addAll(ExpressionUtils.extractConjunction(expr));
            } else {
                sourceExprs.addAll(ExpressionUtils.extractDisjunction(expr));
            }
        }
        Expression result = isAnd ? ExpressionUtils.and(sourceExprs) : ExpressionUtils.or(sourceExprs);
        result = FoldConstantRuleOnFE.evaluate(result, context);
        return result;
    }
}
