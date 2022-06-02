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
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Literal;
import org.apache.doris.nereids.trees.expressions.Not;

public class NotExpressionRule extends AbstractExpressionRewriteRule {

    public static NotExpressionRule INSTANCE = new NotExpressionRule();


    @Override
    public Expression visitNotExpression(Not expr, ExpressionRewriteContext context) {

        Expression child = expr.child();

        if (child instanceof ComparisonPredicate) {
            ComparisonPredicate cp = (ComparisonPredicate) expr.child();
            Expression left =  cp.left();
            Expression right = cp.right();
            NodeType type = cp.getType();
            switch (type) {
                case GREATER_THAN:
                    return new LessThanEqual(left, right);
                case GREATER_THAN_EQUAL:
                    return new LessThan(left, right);
                case LESS_THAN:
                    return new GreaterThanEqual(left, right);
                case LESS_THAN_EQUAL:
                    return new GreaterThan(left, right);
                default:
                    return expr;
            }
        }

        if (child instanceof Not) {
            Not son = (Not) child;
            return son.child();
        }

        return expr;
    }

    @Override
    public Expression visitLiteral(Literal expr, ExpressionRewriteContext context) {
        return expr;
    }

}
