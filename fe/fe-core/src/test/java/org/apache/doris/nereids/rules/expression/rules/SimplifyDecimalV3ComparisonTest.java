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
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.types.DecimalV3Type;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

class SimplifyDecimalV3ComparisonTest extends ExpressionRewriteTestHelper {

    @Test
    void testChildScaleLargerThanCast() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(SimplifyDecimalV3Comparison.INSTANCE)
        ));

        DecimalV3Type castType = DecimalV3Type.createDecimalV3Type(10, 2);
        Expression a = new SlotReference("a", DecimalV3Type.createDecimalV3Type(6, 5));
        Expression left = new Cast(a, castType);
        Expression right = new DecimalV3Literal(castType, new BigDecimal("1.20"));
        assertRewrite(new EqualTo(left, right), new EqualTo(left, right));
    }

    @Test
    void testChildRangeLargerThanCast() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(SimplifyDecimalV3Comparison.INSTANCE)
        ));

        DecimalV3Type castType = DecimalV3Type.createDecimalV3Type(7, 5);
        Expression a = new SlotReference("a", DecimalV3Type.createDecimalV3Type(6, 3));
        Expression left = new Cast(a, castType);
        Expression right = new DecimalV3Literal(castType, new BigDecimal("1.20"));
        assertRewrite(new EqualTo(left, right), new EqualTo(left, right));
    }

    @Test
    void testChildScaleSmallerThanCast() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(SimplifyDecimalV3Comparison.INSTANCE)
        ));

        DecimalV3Type castType = DecimalV3Type.createDecimalV3Type(10, 9);
        Expression a = new SlotReference("a", DecimalV3Type.createDecimalV3Type(6, 5));
        Expression left = new Cast(a, castType);
        Expression right = new DecimalV3Literal(new BigDecimal("1.200000000"));
        assertRewrite(new EqualTo(left, right),
                new EqualTo(a, new DecimalV3Literal(new BigDecimal("1.20000"))));
    }
}
