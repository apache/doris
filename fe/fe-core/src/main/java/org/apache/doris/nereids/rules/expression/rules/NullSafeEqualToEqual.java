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
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;

import java.util.List;

/**
 * convert "A <=> null" to "A is null"
 * null <=> null : true
 * null <=> 1 : false
 * 1 <=> 2 : 1 = 2
 *
 * 1. if null safe equal is in a filter / join / case when / if condition, and at least one side is not nullable,
 *    then null safe equal can be converted to equal.
 * 2. otherwise if both sides are not nullable, then null safe equal can converted to equal too.
 *
 */
public class NullSafeEqualToEqual extends ConditionRewrite {
    public static final NullSafeEqualToEqual INSTANCE = new NullSafeEqualToEqual();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return buildCondRules(ExpressionRuleType.NULL_SAFE_EQUAL_TO_EQUAL);
    }

    // rewrite all the expression tree, not only the condition part.
    @Override
    protected boolean needRewrite(Expression expression, boolean isInsideCondition) {
        return expression.containsType(NullSafeEqual.class);
    }

    @Override
    public Expression visitNullSafeEqual(NullSafeEqual nullSafeEqual, Boolean isInsideCondition) {
        NullSafeEqual newNullSafeEqual = (NullSafeEqual) super.visitNullSafeEqual(nullSafeEqual, isInsideCondition);
        Expression newLeft = newNullSafeEqual.left();
        Expression newRight = newNullSafeEqual.right();
        boolean canConvertToEqual = (!newLeft.nullable() && !newRight.nullable())
                || (isInsideCondition && (newLeft.nullable() || newRight.nullable()));
        if (newLeft.equals(newRight)) {
            return BooleanLiteral.TRUE;
        } else if (newLeft.isNullLiteral() && newRight.isNullLiteral()) {
            return BooleanLiteral.TRUE;
        } else if (newLeft.isNullLiteral()) {
            return !newRight.nullable() ? BooleanLiteral.FALSE : new IsNull(newRight);
        } else if (newRight.isNullLiteral()) {
            return !newLeft.nullable() ? BooleanLiteral.FALSE : new IsNull(newLeft);
        } else if (canConvertToEqual) {
            return new EqualTo(newLeft, newRight);
        } else {
            return newNullSafeEqual;
        }
    }
}
