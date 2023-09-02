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

import org.apache.doris.nereids.rules.expression.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.nereids.util.TypeUtils;

/**
 * Simplify arithmetic comparison rule.
 * a + 1 > 1 => a > 0
 * a / -2 > 1 => a < -2
 */
public class SimplifyArithmeticComparisonRule extends AbstractExpressionRewriteRule {
    public static final SimplifyArithmeticComparisonRule INSTANCE = new SimplifyArithmeticComparisonRule();

    @Override
    public Expression visit(Expression expr, ExpressionRewriteContext context) {
        return expr;
    }

    private Expression process(ComparisonPredicate predicate) {
        Expression left = predicate.left();
        Expression right = predicate.right();
        if (TypeUtils.isAddOrSubtract(left)) {
            Expression p = left.child(1);
            if (p.isConstant()) {
                if (TypeUtils.isAdd(left)) {
                    right = new Subtract(right, p);
                }
                if (TypeUtils.isSubtract(left)) {
                    right = new Add(right, p);
                }
                left = left.child(0);
            }
        }
        if (TypeUtils.isDivide(left)) {
            Expression p = left.child(1);
            if (p.isLiteral()) {
                right = new Multiply(right, p);
                left = left.child(0);
                if (p.toString().startsWith("-")) {
                    Expression tmp = right;
                    right = left;
                    left = tmp;
                }
            }
        }
        if (left != predicate.left() || right != predicate.right()) {
            predicate = (ComparisonPredicate) predicate.withChildren(left, right);
            return TypeCoercionUtils.processComparisonPredicate(predicate);
        } else {
            return predicate;
        }
    }

    @Override
    public Expression visitGreaterThan(GreaterThan greaterThan, ExpressionRewriteContext context) {
        return process(greaterThan);
    }

    @Override
    public Expression visitGreaterThanEqual(GreaterThanEqual greaterThanEqual, ExpressionRewriteContext context) {
        return process(greaterThanEqual);
    }

    @Override
    public Expression visitEqualTo(EqualTo equalTo, ExpressionRewriteContext context) {
        return process(equalTo);
    }

    @Override
    public Expression visitLessThan(LessThan lessThan, ExpressionRewriteContext context) {
        return process(lessThan);
    }

    @Override
    public Expression visitLessThanEqual(LessThanEqual lessThanEqual, ExpressionRewriteContext context) {
        return process(lessThanEqual);
    }
}
