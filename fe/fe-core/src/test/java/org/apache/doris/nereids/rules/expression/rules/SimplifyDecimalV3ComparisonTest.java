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

import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.types.DecimalV3Type;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

class SimplifyDecimalV3ComparisonTest extends ExpressionRewriteTestHelper {

    @Test
    void testChildScaleLargerThanCast() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(SimplifyDecimalV3Comparison.INSTANCE)
        ));

        Expression leftChild = new DecimalV3Literal(new BigDecimal("1.23456"));
        Expression left = new Cast(leftChild, DecimalV3Type.createDecimalV3Type(3, 2));
        Expression right = new DecimalV3Literal(new BigDecimal("1.20"));
        Expression expression = new EqualTo(left, right);
        Expression rewrittenExpression = executor.rewrite(expression, context);
        Assertions.assertInstanceOf(Cast.class, rewrittenExpression.child(0));
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(3, 2),
                rewrittenExpression.child(0).getDataType());
    }

    @Test
    void testChildScaleSmallerThanCast() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(SimplifyDecimalV3Comparison.INSTANCE)
        ));

        Expression leftChild = new DecimalV3Literal(new BigDecimal("1.23456"));
        Expression left = new Cast(leftChild, DecimalV3Type.createDecimalV3Type(10, 9));
        Expression right = new DecimalV3Literal(new BigDecimal("1.200000000"));
        Expression expression = new EqualTo(left, right);
        Expression rewrittenExpression = executor.rewrite(expression, context);
        Assertions.assertInstanceOf(DecimalV3Literal.class, rewrittenExpression.child(0));
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(6, 5),
                rewrittenExpression.child(0).getDataType());
        Assertions.assertInstanceOf(DecimalV3Literal.class, rewrittenExpression.child(1));
        Assertions.assertEquals(new BigDecimal("1.20000"),
                ((DecimalV3Literal) rewrittenExpression.child(1)).getValue());
    }
}
