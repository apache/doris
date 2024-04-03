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
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DaysAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DaysSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HoursAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HoursSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MinutesAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MinutesSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthsAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthsSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SecondsAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SecondsSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.WeeksAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.WeeksSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.YearsAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.YearsSub;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Simplify arithmetic comparison rule.
 * a + 1 > 1 => a > 0
 * a / -2 > 1 => a < -2
 */
public class SimplifyArithmeticComparisonRule implements ExpressionPatternRuleFactory {
    public static SimplifyArithmeticComparisonRule INSTANCE = new SimplifyArithmeticComparisonRule();

    // don't rearrange multiplication because divide may loss precision
    private static final Map<Class<? extends Expression>, Class<? extends Expression>> REARRANGEMENT_MAP = ImmutableMap
            .<Class<? extends Expression>, Class<? extends Expression>>builder()
            .put(Add.class, Subtract.class)
            .put(Subtract.class, Add.class)
            .put(Divide.class, Multiply.class)
            .put(YearsSub.class, YearsAdd.class)
            .put(YearsAdd.class, YearsSub.class)
            .put(MonthsSub.class, MonthsAdd.class)
            .put(MonthsAdd.class, MonthsSub.class)
            .put(WeeksSub.class, WeeksAdd.class)
            .put(WeeksAdd.class, WeeksSub.class)
            .put(DaysSub.class, DaysAdd.class)
            .put(DaysAdd.class, DaysSub.class)
            .put(HoursSub.class, HoursAdd.class)
            .put(HoursAdd.class, HoursSub.class)
            .put(MinutesSub.class, MinutesAdd.class)
            .put(MinutesAdd.class, MinutesSub.class)
            .put(SecondsSub.class, SecondsAdd.class)
            .put(SecondsAdd.class, SecondsSub.class)
            .build();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(ComparisonPredicate.class)
                        .thenApply(ctx -> simplify(ctx.expr, new ExpressionRewriteContext(ctx.cascadesContext)))
        );
    }

    /** simplify */
    public static Expression simplify(ComparisonPredicate comparison, ExpressionRewriteContext context) {
        if (!couldRearrange(comparison)) {
            return comparison;
        }
        ComparisonPredicate newComparison = normalize(comparison);
        if (newComparison == null) {
            return comparison;
        }
        try {
            List<Expression> children = tryRearrangeChildren(newComparison.left(), newComparison.right(), context);
            newComparison = (ComparisonPredicate) simplify(
                    (ComparisonPredicate) newComparison.withChildren(children), context);
            return TypeCoercionUtils.processComparisonPredicate(newComparison);
        } catch (Exception e) {
            return comparison;
        }
    }

    private static boolean couldRearrange(ComparisonPredicate cmp) {
        if (!REARRANGEMENT_MAP.containsKey(cmp.left().getClass()) || cmp.left().isConstant()) {
            return false;
        }

        for (Expression child : cmp.left().children()) {
            if (child.isConstant()) {
                return true;
            }
        }
        return false;
    }

    private static List<Expression> tryRearrangeChildren(Expression left, Expression right,
            ExpressionRewriteContext context) throws Exception {
        if (!left.child(1).isConstant()) {
            throw new RuntimeException(String.format("Expected literal when arranging children for Expr %s", left));
        }
        Literal leftLiteral = (Literal) FoldConstantRule.evaluate(left.child(1), context);
        Expression leftExpr = left.child(0);

        Class<? extends Expression> oppositeOperator = REARRANGEMENT_MAP.get(left.getClass());
        Expression newChild = oppositeOperator.getConstructor(Expression.class, Expression.class)
                .newInstance(right, leftLiteral);

        if (left instanceof Divide && leftLiteral.compareTo(new IntegerLiteral(0)) < 0) {
            // Multiplying by a negative number will change the operator.
            return Arrays.asList(newChild, leftExpr);
        }
        return Arrays.asList(leftExpr, newChild);
    }

    // Ensure that the second child must be Literal, such as
    private static @Nullable ComparisonPredicate normalize(ComparisonPredicate comparison) {
        Expression left = comparison.left();
        Expression leftRight = left.child(1);
        if (leftRight instanceof Literal) {
            return comparison;
        }
        if (left instanceof Add) {
            // 1 + a > 1 => a + 1 > 1
            Expression newLeft = left.withChildren(leftRight, left.child(0));
            return (ComparisonPredicate) comparison.withChildren(newLeft, comparison.right());
        } else if (left instanceof Subtract) {
            // 1 - a > 1 => a + 1 < 1
            Expression newLeft = left.child(0);
            Expression newRight = new Add(leftRight, comparison.right());
            comparison = (ComparisonPredicate) comparison.withChildren(newLeft, newRight);
            return comparison.commute();
        } else {
            // Don't normalize division/multiplication because the slot sign is undecided.
            return null;
        }
    }
}
