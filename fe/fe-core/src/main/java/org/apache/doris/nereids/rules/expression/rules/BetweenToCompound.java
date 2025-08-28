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
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Between;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Rewrites BetweenPredicates into an equivalent conjunctive CompoundPredicate,
 * "not between" is first processed by the BetweenToCompoundRule and then by the SimplifyNotExprRule.
 * Examples:
 * A BETWEEN X AND Y ==> A >= X AND A <= Y
 */
public class BetweenToCompound implements ExpressionPatternRuleFactory {

    public static BetweenToCompound INSTANCE = new BetweenToCompound();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(Between.class)
                        .thenApply(ctx -> rewrite(ctx.expr, ctx.rewriteContext))
                        .toRule(ExpressionRuleType.BETWEEN_TO_COMPOUND)
        );
    }

    public Expression rewrite(Between expr, ExpressionRewriteContext context) {
        if (expr.getLowerBound().equals(expr.getUpperBound())) {
            return new EqualTo(expr.getCompareExpr(), expr.getLowerBound());
        } else {
            Expression left = new GreaterThanEqual(expr.getCompareExpr(), expr.getLowerBound());
            Expression right = new LessThanEqual(expr.getCompareExpr(), expr.getUpperBound());
            return new And(left, right);
        }
    }
}
