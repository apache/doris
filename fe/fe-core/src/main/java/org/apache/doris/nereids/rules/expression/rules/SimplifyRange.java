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
import org.apache.doris.nereids.rules.expression.rules.RangeInference.ValueDesc;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.ImmutableList;

import java.util.List;

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
                        .toRule(ExpressionRuleType.SIMPLIFY_RANGE)
        );
    }

    /** rewrite */
    public static Expression rewrite(CompoundPredicate expr, ExpressionRewriteContext context) {
        ValueDesc valueDesc = (new RangeInference()).getValue(expr, context);
        return valueDesc.toExpression();
    }
}
