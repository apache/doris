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
import org.apache.doris.nereids.rules.expression.rewrite.RewriteHelper;
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;

/**
 * Normalizes binary predicates of the form 'expr' op 'slot' so that the slot is on the left-hand side.
 * For example:
 * 5 > id -> id < 5
 */
public class NormalizeExpressionRule extends AbstractExpressionRewriteRule {

    public static NormalizeExpressionRule INSTANCE = new NormalizeExpressionRule();

    @Override
    public Expression visitComparisonPredicate(ComparisonPredicate expr, ExpressionRewriteContext context) {

        if (RewriteHelper.isConstant(expr.left()) && !RewriteHelper.isConstant(expr.right())) {
            NodeType exprType = expr.getType();
            switch (exprType) {
                case EQUAL_TO:
                    return new EqualTo(expr.right(), expr.left());
                case GREATER_THAN:
                    return new LessThan(expr.right(), expr.left());
                case GREATER_THAN_EQUAL:
                    return new LessThanEqual(expr.right(), expr.left());
                case LESS_THAN:
                    return new GreaterThan(expr.right(), expr.left());
                case LESS_THAN_EQUAL:
                    return new GreaterThanEqual(expr.right(), expr.left());
                default:
                    return expr;
            }
        }
        return expr;
    }
}
