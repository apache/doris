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
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;

/**
 * Rewrite rule of NOT expression.
 * For example:
 * not a -> not a.
 * not not a -> a.
 * not not not a -> not a.
 * not a > b -> a <= b.
 * not a < b -> a >= b.
 * not a >= b -> a < b.
 * not a <= b -> a > b.
 * not a=b -> not a=b.
 * not and(a >= b, a <= c) -> or(a < b, a > c)
 * not or(a >= b, a <= c) -> and(a < b, a > c)
 */
public class SimplifyNotExprRule extends AbstractExpressionRewriteRule {

    public static SimplifyNotExprRule INSTANCE = new SimplifyNotExprRule();

    @Override
    public Expression visitNot(Not expr, ExpressionRewriteContext context) {

        Expression child = expr.child();

        if (child instanceof ComparisonPredicate) {
            ComparisonPredicate cp = (ComparisonPredicate) expr.child();
            Expression left =  rewrite(cp.left(), context);
            Expression right = rewrite(cp.right(), context);
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
        } else if (child instanceof CompoundPredicate) {
            CompoundPredicate cp = (CompoundPredicate) expr.child();
            Expression left =  rewrite(new Not(cp.left()), context);
            Expression right = rewrite(new Not(cp.right()), context);
            NodeType type = cp.getType();
            switch (type) {
                case AND:
                    return new Or<>(left, right);
                case OR:
                    return new And<>(left, right);
                default:
                    return expr;
            }
        }

        if (child instanceof Not) {
            Not son = (Not) child;
            return rewrite(son.child(), context);
        }

        return expr;
    }

}
