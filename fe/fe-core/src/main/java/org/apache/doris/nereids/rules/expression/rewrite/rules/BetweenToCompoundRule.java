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

package org.apache.doris.nereids.rules.expression.rewrite.rules;

import org.apache.doris.nereids.rules.expression.rewrite.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Between;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;

/**
 * Rewrites BetweenPredicates into an equivalent conjunctive CompoundPredicate,
 * "not between" is first processed by the BetweenToCompoundRule and then by the SimplifyNotExprRule.
 * Examples:
 * A BETWEEN X AND Y ==> A >= X AND A <= Y
 */
public class BetweenToCompoundRule extends AbstractExpressionRewriteRule {

    public static BetweenToCompoundRule INSTANCE = new BetweenToCompoundRule();

    @Override
    public Expression visitBetween(Between expr, ExpressionRewriteContext context) {
        Expression left = new GreaterThanEqual<>(expr.getCompareExpr(), expr.getLowerBound());
        Expression right = new LessThanEqual<>(expr.getCompareExpr(), expr.getUpperBound());
        return new And<>(left, right);
    }
}
