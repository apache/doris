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
import org.apache.doris.nereids.rules.expression.rules.RangeInference.DiscreteValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.EmptyValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.RangeValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.UnknownValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.ValueDesc;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import org.apache.commons.lang3.NotImplementedException;

import java.util.Iterator;
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
public class SimplifyRange implements ExpressionPatternRuleFactory {
    public static final SimplifyRange INSTANCE = new SimplifyRange();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesTopType(CompoundPredicate.class)
                        .thenApply(ctx -> rewrite(ctx.expr, ctx.rewriteContext))
        );
    }

    /** rewrite */
    public static Expression rewrite(CompoundPredicate expr, ExpressionRewriteContext context) {
        ValueDesc valueDesc = (new RangeInference()).getValue(expr, context);
        return INSTANCE.getExpression(valueDesc);
    }

    private Expression getExpression(ValueDesc value) {
        if (value instanceof EmptyValue) {
            return getExpression((EmptyValue) value);
        } else if (value instanceof DiscreteValue) {
            return getExpression((DiscreteValue) value);
        } else if (value instanceof RangeValue) {
            return getExpression((RangeValue) value);
        } else if (value instanceof UnknownValue) {
            return getExpression((UnknownValue) value);
        } else {
            throw new NotImplementedException("not implements");
        }
    }

    private Expression getExpression(EmptyValue value) {
        Expression reference = value.getReference();
        return ExpressionUtils.falseOrNull(reference);
    }

    private Expression getExpression(RangeValue value) {
        Expression reference = value.getReference();
        Range<Literal> range = value.getRange();
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
            return ExpressionUtils.trueOrNull(reference);
        }
    }

    private Expression getExpression(DiscreteValue value) {
        Expression reference = value.getReference();
        Set<Literal> values = value.getValues();
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

    private Expression getExpression(UnknownValue value) {
        List<ValueDesc> sourceValues = value.getSourceValues();
        Expression originExpr = value.getOriginExpr();
        if (sourceValues.isEmpty()) {
            return originExpr;
        }
        List<Expression> sourceExprs = Lists.newArrayListWithExpectedSize(sourceValues.size());
        for (ValueDesc sourceValue : sourceValues) {
            Expression expr = getExpression(sourceValue);
            if (value.isAnd()) {
                sourceExprs.addAll(ExpressionUtils.extractConjunction(expr));
            } else {
                sourceExprs.addAll(ExpressionUtils.extractDisjunction(expr));
            }
        }
        Expression result = value.isAnd() ? ExpressionUtils.and(sourceExprs) : ExpressionUtils.or(sourceExprs);
        result = FoldConstantRuleOnFE.evaluate(result, value.getExpressionRewriteContext());
        // ATTN: we must return original expr, because OrToIn is implemented with MutableState,
        //   newExpr will lose these states leading to dead loop by OrToIn -> SimplifyRange -> FoldConstantByFE
        if (result.equals(originExpr)) {
            return originExpr;
        }
        return result;
    }
}
