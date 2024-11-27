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
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

class SimplifyComparisonPredicateTest extends ExpressionRewriteTestHelper {
    @Test
    void testSimplifyComparisonPredicateRule() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(
                    SimplifyCastRule.INSTANCE,
                    SimplifyComparisonPredicate.INSTANCE
                )
        ));

        Expression dtv2 = new DateTimeV2Literal(1, 1, 1, 1, 1, 1, 0);
        Expression dt = new DateTimeLiteral(1, 1, 1, 1, 1, 1);
        Expression dv2 = new DateV2Literal(1, 1, 1);
        Expression dv2PlusOne = new DateV2Literal(1, 1, 2);
        Expression d = new DateLiteral(1, 1, 1);
        // Expression dPlusOne = new DateLiteral(1, 1, 2);

        // DateTimeV2 -> DateTime
        assertRewrite(
                new GreaterThan(new Cast(dt, DateTimeV2Type.SYSTEM_DEFAULT), dtv2),
                new GreaterThan(dt, dt));

        // DateTimeV2 -> DateV2
        assertRewrite(
                new GreaterThan(new Cast(dv2, DateTimeV2Type.SYSTEM_DEFAULT), dtv2),
                new GreaterThan(dv2, dv2));
        assertRewrite(
                new LessThan(new Cast(dv2, DateTimeV2Type.SYSTEM_DEFAULT), dtv2),
                new LessThan(dv2, dv2PlusOne));
        assertRewrite(
                new EqualTo(new Cast(dv2, DateTimeV2Type.SYSTEM_DEFAULT), dtv2),
                new EqualTo(new Cast(dv2, DateTimeV2Type.SYSTEM_DEFAULT), dtv2));

        assertRewrite(
                new EqualTo(new Cast(d, DateTimeV2Type.SYSTEM_DEFAULT), dtv2),
                new EqualTo(new Cast(d, DateTimeV2Type.SYSTEM_DEFAULT), dtv2));

        // test hour, minute and second all zero
        Expression dtv2AtZeroClock = new DateTimeV2Literal(1, 1, 1, 0, 0, 0, 0);
        assertRewrite(
                new GreaterThan(new Cast(dv2, DateTimeV2Type.SYSTEM_DEFAULT), dtv2AtZeroClock),
                new GreaterThan(dv2, dv2));
        assertRewrite(
                new LessThan(new Cast(dv2, DateTimeV2Type.SYSTEM_DEFAULT), dtv2AtZeroClock),
                new LessThan(dv2, dv2));
        assertRewrite(
                new EqualTo(new Cast(dv2, DateTimeV2Type.SYSTEM_DEFAULT), dtv2AtZeroClock),
                new EqualTo(dv2, dv2));

    }

    @Test
    void testDateTimeV2CmpDateTimeV2() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(
                        SimplifyCastRule.INSTANCE,
                        SimplifyComparisonPredicate.INSTANCE
                )
        ));

        Expression dt = new DateTimeLiteral(1, 1, 1, 1, 1, 1);

        // cast(0001-01-01 01:01:01 as DATETIMEV2(0)
        Expression left = new Cast(dt, DateTimeV2Type.SYSTEM_DEFAULT);
        // 2021-01-01 00:00:00.001 (DATETIMEV2(3))
        Expression right = new DateTimeV2Literal("2021-01-01 00:00:00.001");

        // (cast(0001-01-01 01:01:01 as DATETIMEV2(0)) > 2021-01-01 00:00:00.001)
        Expression expression = new GreaterThan(left, right);
        Expression rewrittenExpression = executor.rewrite(typeCoercion(expression), context);
        Assertions.assertEquals(dt.getDataType(), rewrittenExpression.child(0).getDataType());

        // (cast(0001-01-01 01:01:01 as DATETIMEV2(0)) < 2021-01-01 00:00:00.001)
        expression = new GreaterThan(left, right);
        rewrittenExpression = executor.rewrite(typeCoercion(expression), context);
        Assertions.assertEquals(dt.getDataType(), rewrittenExpression.child(0).getDataType());
    }

    @Test
    void testRound() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(
                        SimplifyCastRule.INSTANCE,
                        SimplifyComparisonPredicate.INSTANCE
                )
        ));

        Expression left = new Cast(new DateTimeLiteral("2021-01-02 00:00:00.00"), DateTimeV2Type.of(1));
        Expression right = new DateTimeV2Literal("2021-01-01 23:59:59.99");
        // (cast(2021-01-02 00:00:00.00 as DATETIMEV2(1)) > 2021-01-01 23:59:59.99)
        Expression expression = new GreaterThan(left, right);
        Expression rewrittenExpression = executor.rewrite(typeCoercion(expression), context);

        // right should round to be 2021-01-01 23:59:59
        Assertions.assertEquals(new DateTimeLiteral("2021-01-01 23:59:59"), rewrittenExpression.child(1));
    }

    @Test
    void testDoubleLiteral() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(SimplifyComparisonPredicate.INSTANCE)
        ));

        Expression leftChild = new BigIntLiteral(999);
        Expression left = new Cast(leftChild, DoubleType.INSTANCE);
        Expression right = new DoubleLiteral(111);

        Expression expression = new GreaterThanEqual(left, right);
        Expression rewrittenExpression = executor.rewrite(expression, context);
        Assertions.assertEquals(left.child(0).getDataType(), rewrittenExpression.child(1).getDataType());
        Assertions.assertEquals(rewrittenExpression.child(0).getDataType(), rewrittenExpression.child(1).getDataType());
    }

    @Test
    void testDecimalV3Literal() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(SimplifyComparisonPredicate.INSTANCE)
        ));

        // should not simplify
        Expression leftChild = new DecimalV3Literal(new BigDecimal("1.24"));
        Expression left = new Cast(leftChild, DecimalV3Type.createDecimalV3Type(2, 1));
        Expression right = new DecimalV3Literal(new BigDecimal("1.2"));
        Expression expression = new EqualTo(left, right);
        Expression rewrittenExpression = executor.rewrite(expression, context);
        Assertions.assertInstanceOf(Cast.class, rewrittenExpression.child(0));
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(2, 1),
                rewrittenExpression.child(0).getDataType());

        // = round UNNECESSARY
        leftChild = new DecimalV3Literal(new BigDecimal("11.24"));
        left = new Cast(leftChild, DecimalV3Type.createDecimalV3Type(5, 3));
        right = new DecimalV3Literal(new BigDecimal("12.340"));
        expression = new EqualTo(left, right);
        rewrittenExpression = executor.rewrite(expression, context);
        Assertions.assertInstanceOf(DecimalV3Literal.class, rewrittenExpression.child(0));
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(4, 2),
                rewrittenExpression.child(0).getDataType());
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(4, 2),
                rewrittenExpression.child(1).getDataType());
        Assertions.assertInstanceOf(DecimalV3Literal.class, rewrittenExpression.child(1));
        Assertions.assertEquals(new BigDecimal("12.34"), ((DecimalV3Literal) rewrittenExpression.child(1)).getValue());

        // = always not equals not null
        leftChild = new DecimalV3Literal(new BigDecimal("11.24"));
        left = new Cast(leftChild, DecimalV3Type.createDecimalV3Type(5, 3));
        right = new DecimalV3Literal(new BigDecimal("12.345"));
        expression = new EqualTo(left, right);
        rewrittenExpression = executor.rewrite(expression, context);
        Assertions.assertEquals(BooleanLiteral.FALSE, rewrittenExpression);

        // = always not equals nullable
        leftChild = new SlotReference("slot", DecimalV3Type.createDecimalV3Type(4, 2), true);
        left = new Cast(leftChild, DecimalV3Type.createDecimalV3Type(5, 3));
        right = new DecimalV3Literal(new BigDecimal("12.345"));
        expression = new EqualTo(left, right);
        rewrittenExpression = executor.rewrite(expression, context);
        Assertions.assertEquals(new And(new IsNull(leftChild), new NullLiteral(BooleanType.INSTANCE)),
                rewrittenExpression);

        // <=> round UNNECESSARY
        leftChild = new DecimalV3Literal(new BigDecimal("11.24"));
        left = new Cast(leftChild, DecimalV3Type.createDecimalV3Type(5, 3));
        right = new DecimalV3Literal(new BigDecimal("12.340"));
        expression = new NullSafeEqual(left, right);
        rewrittenExpression = executor.rewrite(expression, context);
        Assertions.assertInstanceOf(DecimalV3Literal.class, rewrittenExpression.child(0));
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(4, 2),
                rewrittenExpression.child(0).getDataType());
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(4, 2),
                rewrittenExpression.child(1).getDataType());
        Assertions.assertInstanceOf(DecimalV3Literal.class, rewrittenExpression.child(1));
        Assertions.assertEquals(new BigDecimal("12.34"), ((DecimalV3Literal) rewrittenExpression.child(1)).getValue());

        // <=> always not equals
        leftChild = new DecimalV3Literal(new BigDecimal("11.24"));
        left = new Cast(leftChild, DecimalV3Type.createDecimalV3Type(5, 3));
        right = new DecimalV3Literal(new BigDecimal("12.345"));
        expression = new NullSafeEqual(left, right);
        rewrittenExpression = executor.rewrite(expression, context);
        Assertions.assertEquals(BooleanLiteral.FALSE, rewrittenExpression);

        // > right literal should round floor
        leftChild = new DecimalV3Literal(new BigDecimal("1.24"));
        left = new Cast(leftChild, DecimalV3Type.createDecimalV3Type(5, 3));
        right = new DecimalV3Literal(new BigDecimal("12.345"));
        expression = new GreaterThan(left, right);
        rewrittenExpression = executor.rewrite(expression, context);
        Assertions.assertInstanceOf(Cast.class, rewrittenExpression.child(0));
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(4, 2),
                rewrittenExpression.child(0).getDataType());
        Assertions.assertInstanceOf(DecimalV3Literal.class, rewrittenExpression.child(1));
        Assertions.assertEquals(new BigDecimal("12.34"), ((DecimalV3Literal) rewrittenExpression.child(1)).getValue());

        // <= right literal should round floor
        leftChild = new DecimalV3Literal(new BigDecimal("1.24"));
        left = new Cast(leftChild, DecimalV3Type.createDecimalV3Type(5, 3));
        right = new DecimalV3Literal(new BigDecimal("12.345"));
        expression = new LessThanEqual(left, right);
        rewrittenExpression = executor.rewrite(expression, context);
        Assertions.assertInstanceOf(Cast.class, rewrittenExpression.child(0));
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(4, 2),
                rewrittenExpression.child(0).getDataType());
        Assertions.assertInstanceOf(DecimalV3Literal.class, rewrittenExpression.child(1));
        Assertions.assertEquals(new BigDecimal("12.34"), ((DecimalV3Literal) rewrittenExpression.child(1)).getValue());

        // >= right literal should round ceiling
        leftChild = new DecimalV3Literal(new BigDecimal("1.24"));
        left = new Cast(leftChild, DecimalV3Type.createDecimalV3Type(5, 3));
        right = new DecimalV3Literal(new BigDecimal("12.345"));
        expression = new GreaterThanEqual(left, right);
        rewrittenExpression = executor.rewrite(expression, context);
        Assertions.assertInstanceOf(Cast.class, rewrittenExpression.child(0));
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(4, 2),
                rewrittenExpression.child(0).getDataType());
        Assertions.assertInstanceOf(DecimalV3Literal.class, rewrittenExpression.child(1));
        Assertions.assertEquals(new BigDecimal("12.35"), ((DecimalV3Literal) rewrittenExpression.child(1)).getValue());

        // < right literal should round ceiling
        leftChild = new DecimalV3Literal(new BigDecimal("1.24"));
        left = new Cast(leftChild, DecimalV3Type.createDecimalV3Type(5, 3));
        right = new DecimalV3Literal(new BigDecimal("12.345"));
        expression = new LessThan(left, right);
        rewrittenExpression = executor.rewrite(expression, context);
        Assertions.assertInstanceOf(Cast.class, rewrittenExpression.child(0));
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(4, 2),
                rewrittenExpression.child(0).getDataType());
        Assertions.assertInstanceOf(DecimalV3Literal.class, rewrittenExpression.child(1));
        Assertions.assertEquals(new BigDecimal("12.35"), ((DecimalV3Literal) rewrittenExpression.child(1)).getValue());

        // left's child range smaller than right literal
        leftChild = new DecimalV3Literal(new BigDecimal("1234.12"));
        left = new Cast(leftChild, DecimalV3Type.createDecimalV3Type(10, 5));
        right = new DecimalV3Literal(new BigDecimal("12345.12000"));
        expression = new EqualTo(left, right);
        rewrittenExpression = executor.rewrite(expression, context);
        Assertions.assertInstanceOf(Cast.class, rewrittenExpression.child(0));
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(7, 2),
                rewrittenExpression.child(0).getDataType());
        Assertions.assertInstanceOf(DecimalV3Literal.class, rewrittenExpression.child(1));
        Assertions.assertEquals(new BigDecimal("12345.12"), ((DecimalV3Literal) rewrittenExpression.child(1)).getValue());
    }
}
