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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;
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
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

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
                BooleanLiteral.FALSE);

        assertRewrite(
                new EqualTo(new Cast(d, DateTimeV2Type.SYSTEM_DEFAULT), dtv2),
                BooleanLiteral.FALSE);

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
    void testIntCompIntLiteral() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(
                        SimplifyCastRule.INSTANCE,
                        SimplifyComparisonPredicate.INSTANCE
                )
        ));

        Expression bigIntSlot = new SlotReference("a", BigIntType.INSTANCE);
        Expression intSlot = new SlotReference("a", IntegerType.INSTANCE);
        Expression smallIntSlot = new SlotReference("a", SmallIntType.INSTANCE);
        Expression tinyIntSlot = new SlotReference("a", TinyIntType.INSTANCE);

        assertRewrite(new LessThan(new Cast(bigIntSlot, LargeIntType.INSTANCE), new LargeIntLiteral(new BigInteger("10"))),
                new LessThan(bigIntSlot, new BigIntLiteral(10L)));
        assertRewrite(new LessThan(new Cast(intSlot, BigIntType.INSTANCE), new BigIntLiteral(10L)),
                new LessThan(intSlot, new IntegerLiteral(10)));
        assertRewrite(new LessThan(new Cast(smallIntSlot, BigIntType.INSTANCE), new BigIntLiteral(10L)),
                new LessThan(smallIntSlot, new SmallIntLiteral((short) 10)));
        assertRewrite(new LessThan(new Cast(tinyIntSlot, BigIntType.INSTANCE), new BigIntLiteral(10L)),
                new LessThan(tinyIntSlot, new TinyIntLiteral((byte) 10)));
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

        Expression date = new SlotReference("a", DateV2Type.INSTANCE);
        Expression datev1 = new SlotReference("a", DateType.INSTANCE);
        Expression datetime0 = new SlotReference("a", DateTimeV2Type.of(0));
        Expression datetime2 = new SlotReference("a", DateTimeV2Type.of(2));
        Expression datetimev1 = new SlotReference("a", DateTimeType.INSTANCE);

        // date
        // cast (date as datetimev1) cmp datetimev1
        assertRewrite(new EqualTo(new Cast(date, DateTimeType.INSTANCE), new DateTimeLiteral("2020-01-01 00:00:00")),
                new EqualTo(date, new DateV2Literal("2020-01-01")));
        assertRewrite(new EqualTo(new Cast(date, DateTimeType.INSTANCE), new DateTimeLiteral("2020-01-01 00:00:01")),
                ExpressionUtils.falseOrNull(date));
        assertRewrite(new NullSafeEqual(new Cast(date, DateTimeType.INSTANCE), new DateTimeLiteral("2020-01-01 00:00:01")),
                BooleanLiteral.FALSE);
        assertRewrite(new GreaterThan(new Cast(date, DateTimeType.INSTANCE), new DateTimeLiteral("2020-01-01 00:00:01")),
                new GreaterThan(date, new DateV2Literal("2020-01-01")));
        assertRewrite(new GreaterThanEqual(new Cast(date, DateTimeType.INSTANCE), new DateTimeLiteral("2020-01-01 00:00:01")),
                new GreaterThanEqual(date, new DateV2Literal("2020-01-02")));
        assertRewrite(new LessThan(new Cast(date, DateTimeType.INSTANCE), new DateTimeLiteral("2020-01-01 00:00:01")),
                new LessThan(date, new DateV2Literal("2020-01-02")));
        assertRewrite(new LessThanEqual(new Cast(date, DateTimeType.INSTANCE), new DateTimeLiteral("2020-01-01 00:00:01")),
                new LessThanEqual(date, new DateV2Literal("2020-01-01")));
        assertRewrite(new EqualTo(new Cast(date, DateTimeV2Type.SYSTEM_DEFAULT), new DateTimeV2Literal("2020-01-01 00:00:00")),
                new EqualTo(date, new DateV2Literal("2020-01-01")));
        assertRewrite(new EqualTo(new Cast(date, DateTimeV2Type.SYSTEM_DEFAULT), new DateTimeV2Literal("2020-01-01 00:00:01")),
                ExpressionUtils.falseOrNull(date));
        assertRewrite(new EqualTo(new Cast(date, DateTimeV2Type.of(2)), new DateTimeV2Literal("2020-01-01 00:00:00.01")),
                ExpressionUtils.falseOrNull(date));
        assertRewrite(new NullSafeEqual(new Cast(date, DateTimeV2Type.of(2)), new DateTimeV2Literal("2020-01-01 00:00:00.01")),
                BooleanLiteral.FALSE);
        assertRewrite(new GreaterThanEqual(new Cast(date, DateTimeV2Type.SYSTEM_DEFAULT), new DateTimeV2Literal("2020-01-01 00:00:01")),
                new GreaterThanEqual(date, new DateV2Literal("2020-01-02")));
        assertRewrite(new GreaterThanEqual(new Cast(date, DateTimeV2Type.of(2)), new DateTimeV2Literal("2020-01-01 00:00:00.01")),
                new GreaterThanEqual(date, new DateV2Literal("2020-01-02")));
        // cast (date as datev1) = datev1-literal
        // assertRewrite(new EqualTo(new Cast(date, DateType.INSTANCE), new DateLiteral("2020-01-01")),
        //        new EqualTo(date, new DateV2Literal("2020-01-01")));
        // assertRewrite(new GreaterThan(new Cast(date, DateType.INSTANCE), new DateLiteral("2020-01-01")),
        //        new GreaterThan(date, new DateV2Literal("2020-01-01")));

        // cast (datev1 as datetimev1) cmp datetimev1
        assertRewrite(new EqualTo(new Cast(datev1, DateTimeType.INSTANCE), new DateTimeLiteral("2020-01-01 00:00:00")),
                new EqualTo(datev1, new DateLiteral("2020-01-01")));
        assertRewrite(new EqualTo(new Cast(datev1, DateTimeType.INSTANCE), new DateTimeLiteral("2020-01-01 00:00:01")),
                ExpressionUtils.falseOrNull(datev1));
        assertRewrite(new NullSafeEqual(new Cast(datev1, DateTimeType.INSTANCE), new DateTimeLiteral("2020-01-01 00:00:01")),
                BooleanLiteral.FALSE);
        assertRewrite(new GreaterThan(new Cast(datev1, DateTimeType.INSTANCE), new DateTimeLiteral("2020-01-01 00:00:01")),
                new GreaterThan(datev1, new DateLiteral("2020-01-01")));
        assertRewrite(new GreaterThanEqual(new Cast(datev1, DateTimeType.INSTANCE), new DateTimeLiteral("2020-01-01 00:00:01")),
                new GreaterThanEqual(datev1, new DateLiteral("2020-01-02")));
        assertRewrite(new LessThan(new Cast(datev1, DateTimeType.INSTANCE), new DateTimeLiteral("2020-01-01 00:00:01")),
                new LessThan(datev1, new DateLiteral("2020-01-02")));
        assertRewrite(new LessThanEqual(new Cast(datev1, DateTimeType.INSTANCE), new DateTimeLiteral("2020-01-01 00:00:01")),
                new LessThanEqual(datev1, new DateLiteral("2020-01-01")));
        assertRewrite(new EqualTo(new Cast(datev1, DateV2Type.INSTANCE), new DateV2Literal("2020-01-01")),
                new EqualTo(datev1, new DateLiteral("2020-01-01")));
        assertRewrite(new GreaterThan(new Cast(datev1, DateV2Type.INSTANCE), new DateV2Literal("2020-01-01")),
                new GreaterThan(datev1, new DateLiteral("2020-01-01")));
        assertRewrite(new EqualTo(new Cast(datev1, DateTimeV2Type.SYSTEM_DEFAULT), new DateTimeV2Literal("2020-01-01 00:00:00")),
                new EqualTo(datev1, new DateLiteral("2020-01-01")));
        assertRewrite(new EqualTo(new Cast(datev1, DateTimeV2Type.SYSTEM_DEFAULT), new DateTimeV2Literal("2020-01-01 00:00:01")),
                ExpressionUtils.falseOrNull(datev1));
        assertRewrite(new EqualTo(new Cast(datev1, DateTimeV2Type.of(2)), new DateTimeV2Literal("2020-01-01 00:00:00.01")),
                ExpressionUtils.falseOrNull(datev1));
        assertRewrite(new NullSafeEqual(new Cast(datev1, DateTimeV2Type.of(2)), new DateTimeV2Literal("2020-01-01 00:00:00.01")),
                BooleanLiteral.FALSE);
        assertRewrite(new GreaterThanEqual(new Cast(datev1, DateTimeV2Type.SYSTEM_DEFAULT), new DateTimeV2Literal("2020-01-01 00:00:01")),
                new GreaterThanEqual(datev1, new DateLiteral("2020-01-02")));
        assertRewrite(new GreaterThanEqual(new Cast(datev1, DateTimeV2Type.of(2)), new DateTimeV2Literal("2020-01-01 00:00:00.01")),
                new GreaterThanEqual(datev1, new DateLiteral("2020-01-02")));

        // cast (datetimev1 as datetime) cmp datetime
        assertRewrite(new EqualTo(new Cast(datetimev1, DateTimeV2Type.of(0)), new DateTimeV2Literal("2020-01-01 00:00:00")),
                new EqualTo(datetimev1, new DateTimeLiteral("2020-01-01 00:00:00")));
        assertRewrite(new GreaterThan(new Cast(datetimev1, DateTimeV2Type.of(0)), new DateTimeV2Literal("2020-01-01 00:00:00")),
                new GreaterThan(datetimev1, new DateTimeLiteral("2020-01-01 00:00:00")));
        assertRewrite(new EqualTo(new Cast(datetimev1, DateTimeV2Type.of(2)), new DateTimeV2Literal("2020-01-01 00:00:00.12")),
                ExpressionUtils.falseOrNull(datetimev1));
        assertRewrite(new NullSafeEqual(new Cast(datetimev1, DateTimeV2Type.of(2)), new DateTimeV2Literal("2020-01-01 00:00:00.12")),
                BooleanLiteral.FALSE);
        assertRewrite(new GreaterThan(new Cast(datetimev1, DateTimeV2Type.of(2)), new DateTimeV2Literal("2020-01-01 00:00:00.12")),
                new GreaterThan(datetimev1, new DateTimeLiteral("2020-01-01 00:00:00")));
        assertRewrite(new GreaterThanEqual(new Cast(datetimev1, DateTimeV2Type.of(2)), new DateTimeV2Literal("2020-01-01 00:00:00.12")),
                new GreaterThanEqual(datetimev1, new DateTimeLiteral("2020-01-01 00:00:01")));
        assertRewrite(new LessThan(new Cast(datetimev1, DateTimeV2Type.of(2)), new DateTimeV2Literal("2020-01-01 00:00:00.12")),
                new LessThan(datetimev1, new DateTimeLiteral("2020-01-01 00:00:01")));
        assertRewrite(new LessThanEqual(new Cast(datetimev1, DateTimeV2Type.of(2)), new DateTimeV2Literal("2020-01-01 00:00:00.12")),
                new LessThanEqual(datetimev1, new DateTimeLiteral("2020-01-01 00:00:00")));

        // cast (datetime0 as datetime) cmp datetime
        assertRewrite(new EqualTo(new Cast(datetime0, DateTimeV2Type.of(2)), new DateTimeV2Literal("2020-01-01 00:00:00.12")),
                ExpressionUtils.falseOrNull(datetime0));
        assertRewrite(new NullSafeEqual(new Cast(datetime0, DateTimeV2Type.of(2)), new DateTimeV2Literal("2020-01-01 00:00:00.12")),
                BooleanLiteral.FALSE);
        assertRewrite(new GreaterThan(new Cast(datetime0, DateTimeV2Type.of(2)), new DateTimeV2Literal("2020-01-01 00:00:00.12")),
                new GreaterThan(datetime0, new DateTimeV2Literal("2020-01-01 00:00:00")));
        assertRewrite(new GreaterThanEqual(new Cast(datetime0, DateTimeV2Type.of(2)), new DateTimeV2Literal("2020-01-01 00:00:00.12")),
                new GreaterThanEqual(datetime0, new DateTimeV2Literal("2020-01-01 00:00:01")));
        assertRewrite(new LessThan(new Cast(datetime0, DateTimeV2Type.of(2)), new DateTimeV2Literal("2020-01-01 00:00:00.12")),
                new LessThan(datetime0, new DateTimeV2Literal("2020-01-01 00:00:01")));
        assertRewrite(new LessThanEqual(new Cast(datetime0, DateTimeV2Type.of(2)), new DateTimeV2Literal("2020-01-01 00:00:00.12")),
                new LessThanEqual(datetime0, new DateTimeV2Literal("2020-01-01 00:00:00")));

        // cast (datetime2 as datetime) cmp datetime
        assertRewrite(new EqualTo(new Cast(datetime2, DateTimeV2Type.of(3)), new DateTimeV2Literal("2020-01-01 00:00:00.123")),
                ExpressionUtils.falseOrNull(datetime2));
        assertRewrite(new NullSafeEqual(new Cast(datetime2, DateTimeV2Type.of(3)), new DateTimeV2Literal("2020-01-01 00:00:00.123")),
                BooleanLiteral.FALSE);
        assertRewrite(new GreaterThan(new Cast(datetime2, DateTimeV2Type.of(3)), new DateTimeV2Literal("2020-01-01 00:00:00.123")),
                new GreaterThan(datetime2, new DateTimeV2Literal("2020-01-01 00:00:00.12")));
        assertRewrite(new GreaterThanEqual(new Cast(datetime2, DateTimeV2Type.of(3)), new DateTimeV2Literal("2020-01-01 00:00:00.123")),
                new GreaterThanEqual(datetime2, new DateTimeV2Literal("2020-01-01 00:00:00.13")));
        assertRewrite(new LessThan(new Cast(datetime2, DateTimeV2Type.of(3)), new DateTimeV2Literal("2020-01-01 00:00:00.123")),
                new LessThan(datetime2, new DateTimeV2Literal("2020-01-01 00:00:00.13")));
        assertRewrite(new LessThanEqual(new Cast(datetime2, DateTimeV2Type.of(3)), new DateTimeV2Literal("2020-01-01 00:00:00.123")),
                new LessThanEqual(datetime2, new DateTimeV2Literal("2020-01-01 00:00:00.12")));

        // test with low bound
        assertRewrite(new LessThan(new Cast(date, DateTimeType.INSTANCE), new DateTimeLiteral("0000-01-01 00:00:00")),
                new LessThan(date, new DateV2Literal("0000-01-01")));
        assertRewrite(new LessThan(new Cast(date, DateTimeV2Type.SYSTEM_DEFAULT), new DateTimeV2Literal("0000-01-01 00:00:00")),
                new LessThan(date, new DateV2Literal("0000-01-01")));
        assertRewrite(new LessThan(new Cast(datev1, DateTimeType.INSTANCE), new DateTimeLiteral("0000-01-01 00:00:00")),
                new LessThan(datev1, new DateLiteral("0000-01-01")));
        assertRewrite(new LessThan(new Cast(datev1, DateTimeV2Type.SYSTEM_DEFAULT), new DateTimeV2Literal("0000-01-01 00:00:00")),
                new LessThan(datev1, new DateLiteral("0000-01-01")));
        assertRewrite(new LessThan(new Cast(datetime0, DateTimeV2Type.of(1)), new DateTimeV2Literal("0000-01-01 00:00:00.1")),
                new LessThan(datetime0, new DateTimeV2Literal("0000-01-01 00:00:01")));
        assertRewrite(new LessThan(new Cast(datetime2, DateTimeV2Type.of(3)), new DateTimeV2Literal("0000-01-01 00:00:00.991")),
                new LessThan(datetime2, new DateTimeV2Literal(DateTimeV2Type.of(2), "0000-01-01 00:00:01.00")));
        assertRewrite(new LessThan(new Cast(datetime2, DateTimeV2Type.of(6)), new DateTimeV2Literal("0000-01-01 00:00:00.999999")),
                new LessThan(datetime2, new DateTimeV2Literal(DateTimeV2Type.of(2), "0000-01-01 00:00:01.00")));
        assertRewrite(new LessThan(new Cast(datetimev1, DateTimeV2Type.of(1)), new DateTimeV2Literal(DateTimeV2Type.of(1), "0000-01-01 00:00:00.0")),
                new LessThan(datetimev1, new DateTimeLiteral("0000-01-01 00:00:00")));
        assertRewrite(new LessThan(new Cast(datetimev1, DateTimeV2Type.of(1)), new DateTimeV2Literal("0000-01-01 00:00:00.1")),
                new LessThan(datetimev1, new DateTimeLiteral("0000-01-01 00:00:01")));
        assertRewrite(new GreaterThan(new Cast(date, DateTimeType.INSTANCE), new DateTimeLiteral("0000-01-01 00:00:00")),
                new GreaterThan(date, new DateV2Literal("0000-01-01")));
        assertRewrite(new GreaterThan(new Cast(date, DateTimeV2Type.SYSTEM_DEFAULT), new DateTimeV2Literal("0000-01-01 00:00:00")),
                new GreaterThan(date, new DateV2Literal("0000-01-01")));
        assertRewrite(new GreaterThan(new Cast(datev1, DateTimeType.INSTANCE), new DateTimeLiteral("0000-01-01 00:00:00")),
                new GreaterThan(datev1, new DateLiteral("0000-01-01")));
        assertRewrite(new GreaterThan(new Cast(datev1, DateTimeV2Type.SYSTEM_DEFAULT), new DateTimeV2Literal("0000-01-01 00:00:00")),
                new GreaterThan(datev1, new DateLiteral("0000-01-01")));
        assertRewrite(new GreaterThan(new Cast(datetime0, DateTimeV2Type.of(1)), new DateTimeV2Literal("0000-01-01 00:00:00.1")),
                new GreaterThan(datetime0, new DateTimeV2Literal("0000-01-01 00:00:00")));
        assertRewrite(new GreaterThan(new Cast(datetime2, DateTimeV2Type.of(3)), new DateTimeV2Literal("0000-01-01 00:00:00.991")),
                new GreaterThan(datetime2, new DateTimeV2Literal("0000-01-01 00:00:00.99")));
        assertRewrite(new GreaterThan(new Cast(datetime2, DateTimeV2Type.of(6)), new DateTimeV2Literal("0000-01-01 00:00:00.999999")),
                new GreaterThan(datetime2, new DateTimeV2Literal("0000-01-01 00:00:00.99")));
        assertRewrite(new GreaterThan(new Cast(datetimev1, DateTimeV2Type.of(1)), new DateTimeV2Literal(DateTimeV2Type.of(1), "0000-01-01 00:00:00.0")),
                new GreaterThan(datetimev1, new DateTimeLiteral("0000-01-01 00:00:00")));
        assertRewrite(new GreaterThan(new Cast(datetimev1, DateTimeV2Type.of(1)), new DateTimeV2Literal("0000-01-01 00:00:00.1")),
                new GreaterThan(datetimev1, new DateTimeLiteral("0000-01-01 00:00:00")));

        assertRewrite(new LessThan(new Cast(date, DateTimeType.INSTANCE), new DateTimeLiteral("9999-12-31 23:59:59")),
                new LessThanEqual(date, new DateV2Literal("9999-12-31")));
        assertRewrite(new LessThan(new Cast(date, DateTimeV2Type.SYSTEM_DEFAULT), new DateTimeV2Literal("9999-12-31 23:59:59")),
                new LessThanEqual(date, new DateV2Literal("9999-12-31")));
        assertRewrite(new LessThan(new Cast(datev1, DateTimeType.INSTANCE), new DateTimeLiteral("9999-12-31 23:59:59")),
                new LessThanEqual(datev1, new DateLiteral("9999-12-31")));
        assertRewrite(new LessThan(new Cast(datev1, DateTimeV2Type.SYSTEM_DEFAULT), new DateTimeV2Literal("9999-12-31 23:59:59")),
                new LessThanEqual(datev1, new DateLiteral("9999-12-31")));
        assertRewrite(new GreaterThanEqual(new Cast(date, DateTimeType.INSTANCE), new DateTimeLiteral("9999-12-31 23:59:59")),
                new GreaterThan(date, new DateV2Literal("9999-12-31")));
        assertRewrite(new GreaterThanEqual(new Cast(date, DateTimeV2Type.SYSTEM_DEFAULT), new DateTimeV2Literal("9999-12-31 23:59:59")),
                new GreaterThan(date, new DateV2Literal("9999-12-31")));
        assertRewrite(new GreaterThanEqual(new Cast(datev1, DateTimeType.INSTANCE), new DateTimeLiteral("9999-12-31 23:59:59")),
                new GreaterThan(datev1, new DateLiteral("9999-12-31")));
        assertRewrite(new GreaterThanEqual(new Cast(datev1, DateTimeV2Type.SYSTEM_DEFAULT), new DateTimeV2Literal("9999-12-31 23:59:59")),
                new GreaterThan(datev1, new DateLiteral("9999-12-31")));

        // test from high datetime to low datetime or date
        assertRewrite(new GreaterThan(new Cast(datetime0, DateType.INSTANCE), new DateLiteral("9999-12-31")),
                new GreaterThan(datetime0, new DateTimeV2Literal(DateTimeV2Type.of(0), "9999-12-31 23:59:59")));
        assertRewrite(new GreaterThanEqual(new Cast(datetime0, DateType.INSTANCE), new DateLiteral("9999-12-31")),
                new GreaterThanEqual(datetime0, new DateTimeV2Literal(DateTimeV2Type.of(0), "9999-12-31 00:00:00")));
        assertRewrite(new LessThan(new Cast(datetime0, DateType.INSTANCE), new DateLiteral("9999-12-31")),
                new LessThan(datetime0, new DateTimeV2Literal(DateTimeV2Type.of(0), "9999-12-31 00:00:00")));
        assertRewrite(new LessThanEqual(new Cast(datetime0, DateType.INSTANCE), new DateLiteral("9999-12-31")),
                new LessThanEqual(datetime0, new DateTimeV2Literal(DateTimeV2Type.of(0), "9999-12-31 23:59:59")));
        assertRewrite(new EqualTo(new Cast(datetime0, DateType.INSTANCE), new DateLiteral("9999-12-31")),
                new And(
                        new GreaterThanEqual(datetime0, new DateTimeV2Literal(DateTimeV2Type.of(0), "9999-12-31 00:00:00")),
                        new LessThanEqual(datetime0, new DateTimeV2Literal(DateTimeV2Type.of(0), "9999-12-31 23:59:59"))));
        assertRewrite(new NullSafeEqual(new Cast(datetime0, DateType.INSTANCE), new DateLiteral("9999-12-31")),
                new And(ImmutableList.of(
                        new GreaterThanEqual(datetime0, new DateTimeV2Literal(DateTimeV2Type.of(0), "9999-12-31 00:00:00")),
                        new LessThanEqual(datetime0, new DateTimeV2Literal(DateTimeV2Type.of(0), "9999-12-31 23:59:59")),
                        new Not(new IsNull(datetime0)))));
        assertRewrite(new GreaterThan(new Cast(datetimev1, DateType.INSTANCE), new DateLiteral("9999-12-31")),
                new GreaterThan(datetimev1, new DateTimeLiteral("9999-12-31 23:59:59")));
        assertRewrite(new GreaterThanEqual(new Cast(datetimev1, DateType.INSTANCE), new DateLiteral("9999-12-31")),
                new GreaterThanEqual(datetimev1, new DateTimeLiteral("9999-12-31 00:00:00")));
        assertRewrite(new LessThan(new Cast(datetimev1, DateType.INSTANCE), new DateLiteral("9999-12-31")),
                new LessThan(datetimev1, new DateTimeLiteral("9999-12-31 00:00:00")));
        assertRewrite(new LessThanEqual(new Cast(datetimev1, DateType.INSTANCE), new DateLiteral("9999-12-31")),
                new LessThanEqual(datetimev1, new DateTimeLiteral("9999-12-31 23:59:59")));
        assertRewrite(new EqualTo(new Cast(datetimev1, DateType.INSTANCE), new DateLiteral("9999-12-31")),
                new And(
                        new GreaterThanEqual(datetimev1, new DateTimeLiteral("9999-12-31 00:00:00")),
                        new LessThanEqual(datetimev1, new DateTimeLiteral("9999-12-31 23:59:59"))));
        assertRewrite(new NullSafeEqual(new Cast(datetimev1, DateType.INSTANCE), new DateLiteral("9999-12-31")),
                new And(ImmutableList.of(
                        new GreaterThanEqual(datetimev1, new DateTimeLiteral("9999-12-31 00:00:00")),
                        new LessThanEqual(datetimev1, new DateTimeLiteral("9999-12-31 23:59:59")),
                        new Not(new IsNull(datetimev1)))));
        assertRewrite(new GreaterThan(new Cast(datetime2, DateType.INSTANCE), new DateLiteral("9999-12-31")),
                new GreaterThan(datetime2, new DateTimeV2Literal(DateTimeV2Type.of(2), "9999-12-31 23:59:59.99")));
        assertRewrite(new GreaterThanEqual(new Cast(datetime2, DateType.INSTANCE), new DateLiteral("9999-12-31")),
                new GreaterThanEqual(datetime2, new DateTimeV2Literal(DateTimeV2Type.of(2), "9999-12-31 00:00:00.00")));
        assertRewrite(new LessThan(new Cast(datetime2, DateType.INSTANCE), new DateLiteral("9999-12-31")),
                new LessThan(datetime2, new DateTimeV2Literal(DateTimeV2Type.of(2), "9999-12-31 00:00:00.00")));
        assertRewrite(new LessThanEqual(new Cast(datetime2, DateType.INSTANCE), new DateLiteral("9999-12-31")),
                new LessThanEqual(datetime2, new DateTimeV2Literal(DateTimeV2Type.of(2), "9999-12-31 23:59:59.99")));
        assertRewrite(new EqualTo(new Cast(datetime2, DateType.INSTANCE), new DateLiteral("9999-12-31")),
                new And(
                        new GreaterThanEqual(datetime2, new DateTimeV2Literal(DateTimeV2Type.of(2), "9999-12-31 00:00:00.00")),
                        new LessThanEqual(datetime2, new DateTimeV2Literal(DateTimeV2Type.of(2), "9999-12-31 23:59:59.99"))));
        assertRewrite(new NullSafeEqual(new Cast(datetime2, DateType.INSTANCE), new DateLiteral("9999-12-31")),
                new And(ImmutableList.of(
                        new GreaterThanEqual(datetime2, new DateTimeV2Literal(DateTimeV2Type.of(2), "9999-12-31 00:00:00.00")),
                        new LessThanEqual(datetime2, new DateTimeV2Literal(DateTimeV2Type.of(2), "9999-12-31 23:59:59.99")),
                        new Not(new IsNull(datetime2)))));
        assertRewrite(new GreaterThan(new Cast(datetime2, DateTimeV2Type.of(0)), new DateTimeV2Literal(DateTimeV2Type.of(0), "9999-12-31 12:34:56")),
                new GreaterThan(datetime2, new DateTimeV2Literal(DateTimeV2Type.of(2), "9999-12-31 12:34:56.99")));
        assertRewrite(new GreaterThanEqual(new Cast(datetime2, DateTimeV2Type.of(0)), new DateTimeV2Literal(DateTimeV2Type.of(0), "9999-12-31 12:34:56")),
                new GreaterThanEqual(datetime2, new DateTimeV2Literal(DateTimeV2Type.of(2), "9999-12-31 12:34:56.00")));
        assertRewrite(new LessThan(new Cast(datetime2, DateTimeV2Type.of(0)), new DateTimeV2Literal(DateTimeV2Type.of(0), "9999-12-31 12:34:56")),
                new LessThan(datetime2, new DateTimeV2Literal(DateTimeV2Type.of(2), "9999-12-31 12:34:56.00")));
        assertRewrite(new LessThanEqual(new Cast(datetime2, DateTimeV2Type.of(0)), new DateTimeV2Literal(DateTimeV2Type.of(0), "9999-12-31 12:34:56")),
                new LessThanEqual(datetime2, new DateTimeV2Literal(DateTimeV2Type.of(2), "9999-12-31 12:34:56.99")));
        assertRewrite(new EqualTo(new Cast(datetime2, DateTimeV2Type.of(0)), new DateTimeV2Literal(DateTimeV2Type.of(0), "9999-12-31 12:34:56")),
                new And(
                        new GreaterThanEqual(datetime2, new DateTimeV2Literal(DateTimeV2Type.of(2), "9999-12-31 12:34:56.00")),
                        new LessThanEqual(datetime2, new DateTimeV2Literal(DateTimeV2Type.of(2), "9999-12-31 12:34:56.99"))));
        assertRewrite(new NullSafeEqual(new Cast(datetime2, DateTimeV2Type.of(0)), new DateTimeV2Literal(DateTimeV2Type.of(0), "9999-12-31 12:34:56")),
                new And(ImmutableList.of(
                        new GreaterThanEqual(datetime2, new DateTimeV2Literal(DateTimeV2Type.of(2), "9999-12-31 12:34:56.00")),
                        new LessThanEqual(datetime2, new DateTimeV2Literal(DateTimeV2Type.of(2), "9999-12-31 12:34:56.99")),
                        new Not(new IsNull(datetime2)))));
        assertRewrite(new GreaterThan(new Cast(datetime2, DateTimeV2Type.of(1)), new DateTimeV2Literal(DateTimeV2Type.of(1), "9999-12-31 12:34:56.7")),
                new GreaterThan(datetime2, new DateTimeV2Literal(DateTimeV2Type.of(2), "9999-12-31 12:34:56.79")));
        assertRewrite(new GreaterThanEqual(new Cast(datetime2, DateTimeV2Type.of(1)), new DateTimeV2Literal(DateTimeV2Type.of(1), "9999-12-31 12:34:56.7")),
                new GreaterThanEqual(datetime2, new DateTimeV2Literal(DateTimeV2Type.of(2), "9999-12-31 12:34:56.70")));
        assertRewrite(new LessThan(new Cast(datetime2, DateTimeV2Type.of(1)), new DateTimeV2Literal(DateTimeV2Type.of(1), "9999-12-31 12:34:56.7")),
                new LessThan(datetime2, new DateTimeV2Literal(DateTimeV2Type.of(2), "9999-12-31 12:34:56.70")));
        assertRewrite(new LessThanEqual(new Cast(datetime2, DateTimeV2Type.of(1)), new DateTimeV2Literal(DateTimeV2Type.of(1), "9999-12-31 12:34:56.7")),
                new LessThanEqual(datetime2, new DateTimeV2Literal(DateTimeV2Type.of(2), "9999-12-31 12:34:56.79")));
        assertRewrite(new EqualTo(new Cast(datetime2, DateTimeV2Type.of(1)), new DateTimeV2Literal(DateTimeV2Type.of(1), "9999-12-31 12:34:56.7")),
                new And(
                        new GreaterThanEqual(datetime2, new DateTimeV2Literal(DateTimeV2Type.of(2), "9999-12-31 12:34:56.70")),
                        new LessThanEqual(datetime2, new DateTimeV2Literal(DateTimeV2Type.of(2), "9999-12-31 12:34:56.79"))));
        assertRewrite(new NullSafeEqual(new Cast(datetime2, DateTimeV2Type.of(1)), new DateTimeV2Literal(DateTimeV2Type.of(1), "9999-12-31 12:34:56.7")),
                new And(ImmutableList.of(
                        new GreaterThanEqual(datetime2, new DateTimeV2Literal(DateTimeV2Type.of(2), "9999-12-31 12:34:56.70")),
                        new LessThanEqual(datetime2, new DateTimeV2Literal(DateTimeV2Type.of(2), "9999-12-31 12:34:56.79")),
                        new Not(new IsNull(datetime2)))));

        assertRewrite(new EqualTo(new Cast(datetime0, DateTimeV2Type.of(1)), new DateTimeV2Literal("9999-12-31 23:59:59.1")),
                ExpressionUtils.falseOrNull(datetime0));
        assertRewrite(new NullSafeEqual(new Cast(datetime0, DateTimeV2Type.of(1)), new DateTimeV2Literal("9999-12-31 23:59:59.1")),
                BooleanLiteral.FALSE);
        assertRewrite(new GreaterThan(new Cast(datetime0, DateTimeV2Type.of(1)), new DateTimeV2Literal("9999-12-31 23:59:59.1")),
                new GreaterThan(datetime0, new DateTimeV2Literal(DateTimeV2Type.of(0), "9999-12-31 23:59:59")));
        assertRewrite(new GreaterThanEqual(new Cast(datetime0, DateTimeV2Type.of(1)), new DateTimeV2Literal("9999-12-31 23:59:59.1")),
                new GreaterThan(datetime0, new DateTimeV2Literal(DateTimeV2Type.of(0), "9999-12-31 23:59:59")));
        assertRewrite(new LessThan(new Cast(datetime0, DateTimeV2Type.of(1)), new DateTimeV2Literal("9999-12-31 23:59:59.1")),
                new LessThanEqual(datetime0, new DateTimeV2Literal(DateTimeV2Type.of(0), "9999-12-31 23:59:59")));
        assertRewrite(new LessThanEqual(new Cast(datetime0, DateTimeV2Type.of(1)), new DateTimeV2Literal("9999-12-31 23:59:59.1")),
                new LessThanEqual(datetime0, new DateTimeV2Literal(DateTimeV2Type.of(0), "9999-12-31 23:59:59")));
        assertRewrite(new EqualTo(new Cast(datetimev1, DateTimeV2Type.of(1)), new DateTimeV2Literal("9999-12-31 23:59:59.1")),
                ExpressionUtils.falseOrNull(datetimev1));
        assertRewrite(new NullSafeEqual(new Cast(datetimev1, DateTimeV2Type.of(1)), new DateTimeV2Literal("9999-12-31 23:59:59.1")),
                BooleanLiteral.FALSE);
        assertRewrite(new GreaterThan(new Cast(datetimev1, DateTimeV2Type.of(1)), new DateTimeV2Literal("9999-12-31 23:59:59.1")),
                new GreaterThan(datetimev1, new DateTimeLiteral("9999-12-31 23:59:59")));
        assertRewrite(new GreaterThanEqual(new Cast(datetimev1, DateTimeV2Type.of(1)), new DateTimeV2Literal("9999-12-31 23:59:59.1")),
                new GreaterThan(datetimev1, new DateTimeLiteral("9999-12-31 23:59:59")));
        assertRewrite(new LessThan(new Cast(datetimev1, DateTimeV2Type.of(1)), new DateTimeV2Literal("9999-12-31 23:59:59.1")),
                new LessThanEqual(datetimev1, new DateTimeLiteral("9999-12-31 23:59:59")));
        assertRewrite(new LessThanEqual(new Cast(datetimev1, DateTimeV2Type.of(1)), new DateTimeV2Literal("9999-12-31 23:59:59.1")),
                new LessThanEqual(datetimev1, new DateTimeLiteral("9999-12-31 23:59:59")));
        assertRewrite(new LessThan(new Cast(datetime2, DateTimeV2Type.of(3)), new DateTimeV2Literal("9999-12-31 23:59:59.991")),
                new LessThanEqual(datetime2, new DateTimeV2Literal("9999-12-31 23:59:59.99")));
        assertRewrite(new LessThan(new Cast(datetime2, DateTimeV2Type.of(6)), new DateTimeV2Literal("9999-12-31 23:59:59.999999")),
                new LessThanEqual(datetime2, new DateTimeV2Literal("9999-12-31 23:59:59.99")));
        assertRewrite(new GreaterThanEqual(new Cast(datetime2, DateTimeV2Type.of(3)), new DateTimeV2Literal("9999-12-31 23:59:59.991")),
                new GreaterThan(datetime2, new DateTimeV2Literal("9999-12-31 23:59:59.99")));
        assertRewrite(new GreaterThanEqual(new Cast(datetime2, DateTimeV2Type.of(6)), new DateTimeV2Literal("9999-12-31 23:59:59.999999")),
                new GreaterThan(datetime2, new DateTimeV2Literal("9999-12-31 23:59:59.99")));
        assertRewrite(new EqualTo(new Cast(datetime2, DateTimeV2Type.of(6)), new DateTimeV2Literal("9999-12-31 23:59:59.999999")),
                ExpressionUtils.falseOrNull(datetime2));
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

        Expression tinyIntSlot = new SlotReference("a", TinyIntType.INSTANCE);
        Expression smallIntSlot = new SlotReference("a", SmallIntType.INSTANCE);
        Expression intSlot = new SlotReference("a", IntegerType.INSTANCE);
        Expression bigIntSlot = new SlotReference("a", BigIntType.INSTANCE);

        // tiny int, literal not exceeds data type limit
        assertRewrite(new EqualTo(new Cast(tinyIntSlot, FloatType.INSTANCE), new FloatLiteral(12.0f)),
                new EqualTo(tinyIntSlot, new TinyIntLiteral((byte) 12)));
        assertRewrite(new EqualTo(new Cast(tinyIntSlot, DoubleType.INSTANCE), new DoubleLiteral(12.0f)),
                new EqualTo(tinyIntSlot, new TinyIntLiteral((byte) 12)));
        assertRewrite(new EqualTo(new Cast(tinyIntSlot, DoubleType.INSTANCE), new DoubleLiteral(12.3f)),
                ExpressionUtils.falseOrNull(tinyIntSlot));
        assertRewrite(new NullSafeEqual(new Cast(tinyIntSlot, DoubleType.INSTANCE), new DoubleLiteral(12.3f)),
                BooleanLiteral.FALSE);
        assertRewrite(new GreaterThan(new Cast(tinyIntSlot, DoubleType.INSTANCE), new DoubleLiteral(12.3f)),
                new GreaterThan(tinyIntSlot, new TinyIntLiteral((byte) 12)));
        assertRewrite(new GreaterThanEqual(new Cast(tinyIntSlot, DoubleType.INSTANCE), new DoubleLiteral(12.3f)),
                new GreaterThanEqual(tinyIntSlot, new TinyIntLiteral((byte) 13)));
        assertRewrite(new LessThan(new Cast(tinyIntSlot, DoubleType.INSTANCE), new DoubleLiteral(12.3f)),
                new LessThan(tinyIntSlot, new TinyIntLiteral((byte) 13)));
        assertRewrite(new LessThanEqual(new Cast(tinyIntSlot, DoubleType.INSTANCE), new DoubleLiteral(12.3f)),
                new LessThanEqual(tinyIntSlot, new TinyIntLiteral((byte) 12)));

        // tiny int, literal exceeds data type limit
        assertRewrite(new EqualTo(new Cast(tinyIntSlot, FloatType.INSTANCE), new FloatLiteral(200.0f)),
                ExpressionUtils.falseOrNull(tinyIntSlot));
        assertRewrite(new EqualTo(new Cast(tinyIntSlot, DoubleType.INSTANCE), new DoubleLiteral(200.0f)),
                ExpressionUtils.falseOrNull(tinyIntSlot));
        assertRewrite(new EqualTo(new Cast(tinyIntSlot, DoubleType.INSTANCE), new DoubleLiteral(200.3f)),
                ExpressionUtils.falseOrNull(tinyIntSlot));
        assertRewrite(new NullSafeEqual(new Cast(tinyIntSlot, DoubleType.INSTANCE), new DoubleLiteral(200.3f)),
                BooleanLiteral.FALSE);
        assertRewrite(new GreaterThan(new Cast(tinyIntSlot, DoubleType.INSTANCE), new DoubleLiteral(200.3f)),
                ExpressionUtils.falseOrNull(tinyIntSlot));
        assertRewrite(new GreaterThanEqual(new Cast(tinyIntSlot, DoubleType.INSTANCE), new DoubleLiteral(200.3f)),
                ExpressionUtils.falseOrNull(tinyIntSlot));
        assertRewrite(new LessThan(new Cast(tinyIntSlot, DoubleType.INSTANCE), new DoubleLiteral(200.3f)),
                ExpressionUtils.trueOrNull(tinyIntSlot));
        assertRewrite(new LessThanEqual(new Cast(tinyIntSlot, DoubleType.INSTANCE), new DoubleLiteral(200.3f)),
                ExpressionUtils.trueOrNull(tinyIntSlot));

        // small int
        assertRewrite(new EqualTo(new Cast(smallIntSlot, FloatType.INSTANCE), new FloatLiteral(12.0f)),
                new EqualTo(smallIntSlot, new SmallIntLiteral((short) 12)));
        assertRewrite(new EqualTo(new Cast(smallIntSlot, DoubleType.INSTANCE), new DoubleLiteral(12.0f)),
                new EqualTo(smallIntSlot, new SmallIntLiteral((short) 12)));
        assertRewrite(new EqualTo(new Cast(smallIntSlot, DoubleType.INSTANCE), new DoubleLiteral(12.3f)),
                ExpressionUtils.falseOrNull(smallIntSlot));
        assertRewrite(new NullSafeEqual(new Cast(smallIntSlot, DoubleType.INSTANCE), new DoubleLiteral(12.3f)),
                BooleanLiteral.FALSE);
        assertRewrite(new GreaterThan(new Cast(smallIntSlot, DoubleType.INSTANCE), new DoubleLiteral(12.3f)),
                new GreaterThan(smallIntSlot, new SmallIntLiteral((short) 12)));
        assertRewrite(new GreaterThanEqual(new Cast(smallIntSlot, DoubleType.INSTANCE), new DoubleLiteral(12.3f)),
                new GreaterThanEqual(smallIntSlot, new SmallIntLiteral((short) 13)));
        assertRewrite(new LessThan(new Cast(smallIntSlot, DoubleType.INSTANCE), new DoubleLiteral(12.3f)),
                new LessThan(smallIntSlot, new SmallIntLiteral((short) 13)));
        assertRewrite(new LessThanEqual(new Cast(smallIntSlot, DoubleType.INSTANCE), new DoubleLiteral(12.3f)),
                new LessThanEqual(smallIntSlot, new SmallIntLiteral((short) 12)));

        // int
        assertRewrite(new EqualTo(new Cast(intSlot, FloatType.INSTANCE), new FloatLiteral(12.0f)),
                new EqualTo(intSlot, new IntegerLiteral(12)));
        assertRewrite(new EqualTo(new Cast(intSlot, DoubleType.INSTANCE), new DoubleLiteral(12.0f)),
                new EqualTo(intSlot, new IntegerLiteral(12)));
        assertRewrite(new EqualTo(new Cast(intSlot, DoubleType.INSTANCE), new DoubleLiteral(12.3f)),
                ExpressionUtils.falseOrNull(intSlot));
        assertRewrite(new NullSafeEqual(new Cast(intSlot, DoubleType.INSTANCE), new DoubleLiteral(12.3f)),
                BooleanLiteral.FALSE);
        assertRewrite(new GreaterThan(new Cast(intSlot, DoubleType.INSTANCE), new DoubleLiteral(12.3f)),
                new GreaterThan(intSlot, new IntegerLiteral(12)));
        assertRewrite(new GreaterThanEqual(new Cast(intSlot, DoubleType.INSTANCE), new DoubleLiteral(12.3f)),
                new GreaterThanEqual(intSlot, new IntegerLiteral(13)));
        assertRewrite(new LessThan(new Cast(intSlot, DoubleType.INSTANCE), new DoubleLiteral(12.3f)),
                new LessThan(intSlot, new IntegerLiteral(13)));
        assertRewrite(new LessThanEqual(new Cast(intSlot, DoubleType.INSTANCE), new DoubleLiteral(12.3f)),
                new LessThanEqual(intSlot, new IntegerLiteral(12)));

        // big int
        assertRewrite(new EqualTo(new Cast(bigIntSlot, FloatType.INSTANCE), new FloatLiteral(12.0f)),
                new EqualTo(bigIntSlot, new BigIntLiteral(12L)));
        assertRewrite(new EqualTo(new Cast(bigIntSlot, DoubleType.INSTANCE), new DoubleLiteral(12.0f)),
                new EqualTo(bigIntSlot, new BigIntLiteral(12L)));
        assertRewrite(new EqualTo(new Cast(bigIntSlot, DoubleType.INSTANCE), new DoubleLiteral(12.3f)),
                ExpressionUtils.falseOrNull(bigIntSlot));
        assertRewrite(new NullSafeEqual(new Cast(bigIntSlot, DoubleType.INSTANCE), new DoubleLiteral(12.3f)),
                BooleanLiteral.FALSE);
        assertRewrite(new GreaterThan(new Cast(bigIntSlot, DoubleType.INSTANCE), new DoubleLiteral(12.3f)),
                new GreaterThan(bigIntSlot, new BigIntLiteral(12L)));
        assertRewrite(new GreaterThanEqual(new Cast(bigIntSlot, DoubleType.INSTANCE), new DoubleLiteral(12.3f)),
                new GreaterThanEqual(bigIntSlot, new BigIntLiteral(13L)));
        assertRewrite(new LessThan(new Cast(bigIntSlot, DoubleType.INSTANCE), new DoubleLiteral(12.3f)),
                new LessThan(bigIntSlot, new BigIntLiteral(13L)));
        assertRewrite(new LessThanEqual(new Cast(bigIntSlot, DoubleType.INSTANCE), new DoubleLiteral(12.3f)),
                new LessThanEqual(bigIntSlot, new BigIntLiteral(12L)));

        // big int and literal near no loss bound
        double noLossBound = 9007199254740992.0;
        assertRewrite(new EqualTo(new Cast(bigIntSlot, DoubleType.INSTANCE), new DoubleLiteral(-noLossBound)),
                new EqualTo(new Cast(bigIntSlot, DoubleType.INSTANCE), new DoubleLiteral(-noLossBound)));
        assertRewrite(new EqualTo(new Cast(bigIntSlot, DoubleType.INSTANCE), new DoubleLiteral(-9007199254740991.0)),
                new EqualTo(bigIntSlot, new BigIntLiteral(-9007199254740991L)));
        assertRewrite(new EqualTo(new Cast(bigIntSlot, DoubleType.INSTANCE), new DoubleLiteral(9007199254740991.0)),
                new EqualTo(bigIntSlot, new BigIntLiteral(9007199254740991L)));
        assertRewrite(new EqualTo(new Cast(bigIntSlot, DoubleType.INSTANCE), new DoubleLiteral(noLossBound)),
                new EqualTo(new Cast(bigIntSlot, DoubleType.INSTANCE), new DoubleLiteral(noLossBound)));
    }

    @Test
    void testIntCmpDecimalV3Literal() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(SimplifyComparisonPredicate.INSTANCE)
        ));

        Expression tinyIntSlot = new SlotReference("a", TinyIntType.INSTANCE);
        Expression smallIntSlot = new SlotReference("a", SmallIntType.INSTANCE);
        Expression intSlot = new SlotReference("a", IntegerType.INSTANCE);
        Expression bigIntSlot = new SlotReference("a", BigIntType.INSTANCE);

        // tiny int, literal not exceeds data type limit
        assertRewrite(new EqualTo(new Cast(tinyIntSlot, DecimalV3Type.createDecimalV3Type(3, 1)), new DecimalV3Literal(new BigDecimal("12.0"))),
                new EqualTo(tinyIntSlot, new TinyIntLiteral((byte) 12)));
        assertRewrite(new EqualTo(new Cast(tinyIntSlot, DecimalV3Type.createDecimalV3Type(3, 1)), new DecimalV3Literal(new BigDecimal("12.3"))),
                ExpressionUtils.falseOrNull(tinyIntSlot));
        assertRewrite(new NullSafeEqual(new Cast(tinyIntSlot, DecimalV3Type.createDecimalV3Type(3, 1)), new DecimalV3Literal(new BigDecimal("12.3"))),
                BooleanLiteral.FALSE);
        assertRewrite(new GreaterThan(new Cast(tinyIntSlot, DecimalV3Type.createDecimalV3Type(3, 1)), new DecimalV3Literal(new BigDecimal("12.3"))),
                new GreaterThan(tinyIntSlot, new TinyIntLiteral((byte) 12)));
        assertRewrite(new GreaterThanEqual(new Cast(tinyIntSlot, DecimalV3Type.createDecimalV3Type(3, 1)), new DecimalV3Literal(new BigDecimal("12.3"))),
                new GreaterThanEqual(tinyIntSlot, new TinyIntLiteral((byte) 13)));
        assertRewrite(new LessThan(new Cast(tinyIntSlot, DecimalV3Type.createDecimalV3Type(3, 1)), new DecimalV3Literal(new BigDecimal("12.3"))),
                new LessThan(tinyIntSlot, new TinyIntLiteral((byte) 13)));
        assertRewrite(new LessThanEqual(new Cast(tinyIntSlot, DecimalV3Type.createDecimalV3Type(3, 1)), new DecimalV3Literal(new BigDecimal("12.3"))),
                new LessThanEqual(tinyIntSlot, new TinyIntLiteral((byte) 12)));

        // tiny int, literal exceeds data type limit
        assertRewrite(new EqualTo(new Cast(tinyIntSlot, DecimalV3Type.createDecimalV3Type(4, 1)), new DecimalV3Literal(new BigDecimal("200.0"))),
                ExpressionUtils.falseOrNull(tinyIntSlot));
        assertRewrite(new EqualTo(new Cast(tinyIntSlot, DecimalV3Type.createDecimalV3Type(4, 1)), new DecimalV3Literal(new BigDecimal("200.3"))),
                ExpressionUtils.falseOrNull(tinyIntSlot));
        assertRewrite(new NullSafeEqual(new Cast(tinyIntSlot, DecimalV3Type.createDecimalV3Type(4, 1)), new DecimalV3Literal(new BigDecimal("200.3"))),
                BooleanLiteral.FALSE);
        assertRewrite(new GreaterThan(new Cast(tinyIntSlot, DecimalV3Type.createDecimalV3Type(4, 1)), new DecimalV3Literal(new BigDecimal("200.3"))),
                ExpressionUtils.falseOrNull(tinyIntSlot));
        assertRewrite(new GreaterThanEqual(new Cast(tinyIntSlot, DecimalV3Type.createDecimalV3Type(4, 1)), new DecimalV3Literal(new BigDecimal("200.3"))),
                ExpressionUtils.falseOrNull(tinyIntSlot));
        assertRewrite(new LessThan(new Cast(tinyIntSlot, DecimalV3Type.createDecimalV3Type(4, 1)), new DecimalV3Literal(new BigDecimal("200.3"))),
                ExpressionUtils.trueOrNull(tinyIntSlot));
        assertRewrite(new LessThanEqual(new Cast(tinyIntSlot, DecimalV3Type.createDecimalV3Type(4, 1)), new DecimalV3Literal(new BigDecimal("200.3"))),
                ExpressionUtils.trueOrNull(tinyIntSlot));

        // small int
        assertRewrite(new EqualTo(new Cast(smallIntSlot, DecimalV3Type.createDecimalV3Type(3, 1)), new DecimalV3Literal(new BigDecimal("12.0"))),
                new EqualTo(smallIntSlot, new SmallIntLiteral((short) 12)));
        assertRewrite(new EqualTo(new Cast(smallIntSlot, DecimalV3Type.createDecimalV3Type(3, 1)), new DecimalV3Literal(new BigDecimal("12.3"))),
                ExpressionUtils.falseOrNull(smallIntSlot));
        assertRewrite(new NullSafeEqual(new Cast(smallIntSlot, DecimalV3Type.createDecimalV3Type(3, 1)), new DecimalV3Literal(new BigDecimal("12.3"))),
                BooleanLiteral.FALSE);
        assertRewrite(new GreaterThan(new Cast(smallIntSlot, DecimalV3Type.createDecimalV3Type(3, 1)), new DecimalV3Literal(new BigDecimal("12.3"))),
                new GreaterThan(smallIntSlot, new SmallIntLiteral((short) 12)));
        assertRewrite(new GreaterThanEqual(new Cast(smallIntSlot, DecimalV3Type.createDecimalV3Type(3, 1)), new DecimalV3Literal(new BigDecimal("12.3"))),
                new GreaterThanEqual(smallIntSlot, new SmallIntLiteral((short) 13)));
        assertRewrite(new LessThan(new Cast(smallIntSlot, DecimalV3Type.createDecimalV3Type(3, 1)), new DecimalV3Literal(new BigDecimal("12.3"))),
                new LessThan(smallIntSlot, new SmallIntLiteral((short) 13)));
        assertRewrite(new LessThanEqual(new Cast(smallIntSlot, DecimalV3Type.createDecimalV3Type(3, 1)), new DecimalV3Literal(new BigDecimal("12.3"))),
                new LessThanEqual(smallIntSlot, new SmallIntLiteral((short) 12)));

        // int
        assertRewrite(new EqualTo(new Cast(intSlot, DecimalV3Type.createDecimalV3Type(3, 1)), new DecimalV3Literal(new BigDecimal("12.0"))),
                new EqualTo(intSlot, new IntegerLiteral(12)));
        assertRewrite(new EqualTo(new Cast(intSlot, DecimalV3Type.createDecimalV3Type(3, 1)), new DecimalV3Literal(new BigDecimal("12.3"))),
                ExpressionUtils.falseOrNull(intSlot));
        assertRewrite(new NullSafeEqual(new Cast(intSlot, DecimalV3Type.createDecimalV3Type(3, 1)), new DecimalV3Literal(new BigDecimal("12.3"))),
                BooleanLiteral.FALSE);
        assertRewrite(new GreaterThan(new Cast(intSlot, DecimalV3Type.createDecimalV3Type(3, 1)), new DecimalV3Literal(new BigDecimal("12.3"))),
                new GreaterThan(intSlot, new IntegerLiteral(12)));
        assertRewrite(new GreaterThanEqual(new Cast(intSlot, DecimalV3Type.createDecimalV3Type(3, 1)), new DecimalV3Literal(new BigDecimal("12.3"))),
                new GreaterThanEqual(intSlot, new IntegerLiteral(13)));
        assertRewrite(new LessThan(new Cast(intSlot, DecimalV3Type.createDecimalV3Type(3, 1)), new DecimalV3Literal(new BigDecimal("12.3"))),
                new LessThan(intSlot, new IntegerLiteral(13)));
        assertRewrite(new LessThanEqual(new Cast(intSlot, DecimalV3Type.createDecimalV3Type(3, 1)), new DecimalV3Literal(new BigDecimal("12.3"))),
                new LessThanEqual(intSlot, new IntegerLiteral(12)));

        // big int
        assertRewrite(new EqualTo(new Cast(bigIntSlot, DecimalV3Type.createDecimalV3Type(3, 1)), new DecimalV3Literal(new BigDecimal("12.0"))),
                new EqualTo(bigIntSlot, new BigIntLiteral(12L)));
        assertRewrite(new EqualTo(new Cast(bigIntSlot, DecimalV3Type.createDecimalV3Type(3, 1)), new DecimalV3Literal(new BigDecimal("12.3"))),
                ExpressionUtils.falseOrNull(bigIntSlot));
        assertRewrite(new NullSafeEqual(new Cast(bigIntSlot, DecimalV3Type.createDecimalV3Type(3, 1)), new DecimalV3Literal(new BigDecimal("12.3"))),
                BooleanLiteral.FALSE);
        assertRewrite(new GreaterThan(new Cast(bigIntSlot, DecimalV3Type.createDecimalV3Type(3, 1)), new DecimalV3Literal(new BigDecimal("12.3"))),
                new GreaterThan(bigIntSlot, new BigIntLiteral(12L)));
        assertRewrite(new GreaterThanEqual(new Cast(bigIntSlot, DecimalV3Type.createDecimalV3Type(3, 1)), new DecimalV3Literal(new BigDecimal("12.3"))),
                new GreaterThanEqual(bigIntSlot, new BigIntLiteral(13L)));
        assertRewrite(new LessThan(new Cast(bigIntSlot, DecimalV3Type.createDecimalV3Type(3, 1)), new DecimalV3Literal(new BigDecimal("12.3"))),
                new LessThan(bigIntSlot, new BigIntLiteral(13L)));
        assertRewrite(new LessThanEqual(new Cast(bigIntSlot, DecimalV3Type.createDecimalV3Type(3, 1)), new DecimalV3Literal(new BigDecimal("12.3"))),
                new LessThanEqual(bigIntSlot, new BigIntLiteral(12L)));

        assertRewrite(new LessThan(new Cast(bigIntSlot, DecimalV3Type.createDecimalV3Type(20, 1)), new DecimalV3Literal(new BigDecimal("-9223372036854775808.1"))),
                ExpressionUtils.falseOrNull(bigIntSlot));
        assertRewrite(new GreaterThanEqual(new Cast(bigIntSlot, DecimalV3Type.createDecimalV3Type(20, 1)), new DecimalV3Literal(new BigDecimal("-9223372036854775808.1"))),
                ExpressionUtils.trueOrNull(bigIntSlot));
        assertRewrite(new LessThan(new Cast(bigIntSlot, DecimalV3Type.createDecimalV3Type(20, 1)), new DecimalV3Literal(new BigDecimal("-9223372036854775807.1"))),
                new LessThan(bigIntSlot, new BigIntLiteral(-9223372036854775807L)));
        assertRewrite(new GreaterThanEqual(new Cast(bigIntSlot, DecimalV3Type.createDecimalV3Type(20, 1)), new DecimalV3Literal(new BigDecimal("9223372036854775807.1"))),
                ExpressionUtils.falseOrNull(bigIntSlot));
        assertRewrite(new LessThan(new Cast(bigIntSlot, DecimalV3Type.createDecimalV3Type(20, 1)), new DecimalV3Literal(new BigDecimal("9223372036854775806.1"))),
                new LessThan(bigIntSlot, new BigIntLiteral(9223372036854775807L)));
    }

    @Test
    void testDecimalCmpDecimalV3Literal() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(SimplifyComparisonPredicate.INSTANCE)
        ));

        // should not simplify
        Expression leftChild = new DecimalV3Literal(new BigDecimal("1.24"));
        Expression left = new Cast(leftChild, DecimalV3Type.createDecimalV3Type(2, 1));
        Expression right = new DecimalV3Literal(new BigDecimal("1.2"));
        Expression expression = new EqualTo(left, right);
        Expression rewrittenExpression = executor.rewrite(expression, context);
        Assertions.assertEquals(expression, rewrittenExpression);

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
        leftChild = new DecimalV3Literal(new BigDecimal("10.24"));
        left = new Cast(leftChild, DecimalV3Type.createDecimalV3Type(5, 3));
        right = new DecimalV3Literal(new BigDecimal("12.345"));
        expression = new GreaterThan(left, right);
        rewrittenExpression = executor.rewrite(expression, context);
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(4, 2),
                rewrittenExpression.child(0).getDataType());
        Assertions.assertInstanceOf(DecimalV3Literal.class, rewrittenExpression.child(1));
        Assertions.assertEquals(new BigDecimal("12.34"), ((DecimalV3Literal) rewrittenExpression.child(1)).getValue());

        // <= right literal should round floor
        leftChild = new DecimalV3Literal(new BigDecimal("10.24"));
        left = new Cast(leftChild, DecimalV3Type.createDecimalV3Type(5, 3));
        right = new DecimalV3Literal(new BigDecimal("12.345"));
        expression = new LessThanEqual(left, right);
        rewrittenExpression = executor.rewrite(expression, context);
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(4, 2),
                rewrittenExpression.child(0).getDataType());
        Assertions.assertInstanceOf(DecimalV3Literal.class, rewrittenExpression.child(1));
        Assertions.assertEquals(new BigDecimal("12.34"), ((DecimalV3Literal) rewrittenExpression.child(1)).getValue());

        // >= right literal should round ceiling
        leftChild = new DecimalV3Literal(new BigDecimal("10.24"));
        left = new Cast(leftChild, DecimalV3Type.createDecimalV3Type(5, 3));
        right = new DecimalV3Literal(new BigDecimal("12.345"));
        expression = new GreaterThanEqual(left, right);
        rewrittenExpression = executor.rewrite(expression, context);
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(4, 2),
                rewrittenExpression.child(0).getDataType());
        Assertions.assertInstanceOf(DecimalV3Literal.class, rewrittenExpression.child(1));
        Assertions.assertEquals(new BigDecimal("12.35"), ((DecimalV3Literal) rewrittenExpression.child(1)).getValue());

        // < right literal should round ceiling
        leftChild = new DecimalV3Literal(new BigDecimal("10.24"));
        left = new Cast(leftChild, DecimalV3Type.createDecimalV3Type(5, 3));
        right = new DecimalV3Literal(new BigDecimal("12.345"));
        expression = new LessThan(left, right);
        rewrittenExpression = executor.rewrite(expression, context);
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(4, 2),
                rewrittenExpression.child(0).getDataType());
        Assertions.assertInstanceOf(DecimalV3Literal.class, rewrittenExpression.child(1));
        Assertions.assertEquals(new BigDecimal("12.35"), ((DecimalV3Literal) rewrittenExpression.child(1)).getValue());

        // left's child range smaller than right literal, but cast's scale < leftChild's scale
        leftChild = new DecimalV3Literal(new BigDecimal("12340.12"));
        left = new Cast(leftChild, DecimalV3Type.createDecimalV3Type(10, 1));
        right = new DecimalV3Literal(new BigDecimal("12345.12000"));
        expression = new EqualTo(left, right);
        rewrittenExpression = executor.rewrite(expression, context);
        Assertions.assertEquals(expression, rewrittenExpression);

        // left's child range smaller than right literal
        leftChild = new DecimalV3Literal(new BigDecimal("12340.12"));
        left = new Cast(leftChild, DecimalV3Type.createDecimalV3Type(10, 5));
        right = new DecimalV3Literal(new BigDecimal("12345.12000"));
        expression = new EqualTo(left, right);
        rewrittenExpression = executor.rewrite(expression, context);
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(7, 2),
                rewrittenExpression.child(0).getDataType());
        Assertions.assertInstanceOf(DecimalV3Literal.class, rewrittenExpression.child(1));
        Assertions.assertEquals(new BigDecimal("12345.12"), ((DecimalV3Literal) rewrittenExpression.child(1)).getValue());

        // child scale bigger than literal's scale, child precision bigger than literal's precision
        leftChild = new DecimalV3Literal(new BigDecimal("1.23456"));
        left = new Cast(leftChild, DecimalV3Type.createDecimalV3Type(7, 5));
        right = new DecimalV3Literal(new BigDecimal("1.20"));
        expression = new EqualTo(left, right);
        rewrittenExpression = executor.rewrite(expression, context);
        Assertions.assertEquals(new EqualTo(leftChild, new DecimalV3Literal(new BigDecimal("1.20000"))),
                rewrittenExpression);

        // child scale bigger than literal's scale, child precision smaller than literal's precision,
        // exceed data type's limit
        leftChild = new DecimalV3Literal(new BigDecimal("1.23456"));
        left = new Cast(leftChild, DecimalV3Type.createDecimalV3Type(7, 1));
        right = new DecimalV3Literal(new BigDecimal("123456.7"));
        expression = new EqualTo(left, right);
        rewrittenExpression = executor.rewrite(expression, context);
        Assertions.assertEquals(BooleanLiteral.FALSE, rewrittenExpression);

        // child scale small than literal's scale
        leftChild = new DecimalV3Literal(new BigDecimal("1.23456"));
        left = new Cast(leftChild, DecimalV3Type.createDecimalV3Type(10, 9));
        right = new DecimalV3Literal(new BigDecimal("1.200000000"));
        expression = new EqualTo(left, right);
        rewrittenExpression = executor.rewrite(expression, context);
        Assertions.assertInstanceOf(DecimalV3Literal.class, rewrittenExpression.child(0));
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(6, 5),
                rewrittenExpression.child(0).getDataType());
        Assertions.assertInstanceOf(DecimalV3Literal.class, rewrittenExpression.child(1));
        Assertions.assertEquals(new BigDecimal("1.20000"),
                ((DecimalV3Literal) rewrittenExpression.child(1)).getValue());
    }

    private enum RangeLimitResult {
        TRUE, // eval to true
        FALSE, // eval to false
        EQUALS, // eval to equals
        NO_CHANGE_CP // no change cmp type
    }

    @Test
    void testTypeRangeLimit() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
            bottomUp(SimplifyComparisonPredicate.INSTANCE)
        ));

        checkTypeRangeLimit(TinyIntType.INSTANCE,
                ImmutableList.of(
                        Pair.of(new SmallIntLiteral((short) -129), null),
                        Pair.of(new DecimalV3Literal(new BigDecimal("-129")), null),
                        Pair.of(new DecimalV3Literal(new BigDecimal("-128.1")), null),
                        Pair.of(new DecimalV3Literal(new BigDecimal("-1000.1")), null),
                        Pair.of(new DoubleLiteral(-129.0), new SmallIntLiteral((short) -129)),
                        Pair.of(new DoubleLiteral(-128.1), new DecimalV3Literal(new BigDecimal("-128.1")))),
                ImmutableList.of(
                        Pair.of(new TinyIntLiteral((byte) -128), null),
                        Pair.of(new SmallIntLiteral((short) -128), new TinyIntLiteral((byte) -128)),
                        Pair.of(new IntegerLiteral(-128), new TinyIntLiteral((byte) -128)),
                        Pair.of(new DecimalV3Literal(new BigDecimal("-128")), new TinyIntLiteral((byte) -128)),
                        Pair.of(new DoubleLiteral(-128.0), new TinyIntLiteral((byte) -128))),
                ImmutableList.of(
                        Pair.of(new TinyIntLiteral((byte) -127), null),
                        Pair.of(new DecimalV3Literal(new BigDecimal("-127")), new TinyIntLiteral((byte) -127)),
                        Pair.of(new DoubleLiteral(-127.0), new TinyIntLiteral((byte) -127)),
                        Pair.of(new TinyIntLiteral((byte) 126), null),
                        Pair.of(new DoubleLiteral(126.0), new TinyIntLiteral((byte) 126))),
                ImmutableList.of(
                        Pair.of(new TinyIntLiteral((byte) 127), null),
                        Pair.of(new DecimalV3Literal(new BigDecimal("127")), new TinyIntLiteral((byte) 127)),
                        Pair.of(new DecimalV3Literal(new BigDecimal("127.00")), new TinyIntLiteral((byte) 127)),
                        Pair.of(new DoubleLiteral(127.0), new TinyIntLiteral((byte) 127))),
                ImmutableList.of(
                        Pair.of(new SmallIntLiteral((short) 128), null),
                        Pair.of(new DecimalV3Literal(new BigDecimal("128.02")), null),
                        Pair.of(new DoubleLiteral(128.0), new SmallIntLiteral((short) 128)),
                        Pair.of(new DoubleLiteral(127.1), new DecimalV3Literal(new BigDecimal("127.1")))));

        checkTypeRangeLimit(SmallIntType.INSTANCE,
                ImmutableList.of(
                        Pair.of(new IntegerLiteral(-32769), null),
                        Pair.of(new DecimalV3Literal(new BigDecimal("-32769")), null),
                        Pair.of(new DecimalV3Literal(new BigDecimal("-32768.1")), null),
                        Pair.of(new DoubleLiteral(-32769.0), new IntegerLiteral(-32769)),
                        Pair.of(new DoubleLiteral(-32769.1), new DecimalV3Literal(new BigDecimal("-32769.1")))),
                ImmutableList.of(
                        Pair.of(new SmallIntLiteral((short) -32768), null),
                        Pair.of(new DecimalV3Literal(new BigDecimal("-32768")), new SmallIntLiteral((short) -32768)),
                        Pair.of(new DoubleLiteral(-32768.0), new SmallIntLiteral((short) -32768))),
                ImmutableList.of(
                        Pair.of(new SmallIntLiteral((short) -32767), null),
                        Pair.of(new DecimalV3Literal(new BigDecimal("-32767")), new SmallIntLiteral((short) -32767)),
                        Pair.of(new DoubleLiteral(-32767.0), new SmallIntLiteral((short) -32767)),
                        Pair.of(new SmallIntLiteral((short) 32766), null),
                        Pair.of(new DoubleLiteral(32766.0), new SmallIntLiteral((short) 32766))),
                ImmutableList.of(
                        Pair.of(new SmallIntLiteral((short) 32767), null),
                        Pair.of(new DecimalV3Literal(new BigDecimal("32767")), new SmallIntLiteral((short) 32767)),
                        Pair.of(new DecimalV3Literal(new BigDecimal("32767.00")), new SmallIntLiteral((short) 32767)),
                        Pair.of(new DoubleLiteral(32767.0), new SmallIntLiteral((short) 32767))),
                ImmutableList.of(
                        Pair.of(new IntegerLiteral(32768), null),
                        Pair.of(new DecimalV3Literal(new BigDecimal("32768.02")), null),
                        Pair.of(new DoubleLiteral(32768.0), new IntegerLiteral(32768)),
                        Pair.of(new DoubleLiteral(32768.1), new DecimalV3Literal(new BigDecimal("32768.1")))));

        checkTypeRangeLimit(IntegerType.INSTANCE,
                ImmutableList.of(
                        Pair.of(new BigIntLiteral(-2147483649L), null),
                        Pair.of(new DecimalV3Literal(new BigDecimal("-2147483649")), null),
                        Pair.of(new DecimalV3Literal(new BigDecimal("-2147483649.1")), null),
                        Pair.of(new DoubleLiteral(-2147483649.0), new BigIntLiteral(-2147483649L)),
                        Pair.of(new DoubleLiteral(-2147483649.1), new DecimalV3Literal(new BigDecimal("-2147483649.1")))),
                ImmutableList.of(
                        Pair.of(new IntegerLiteral(-2147483648), null),
                        Pair.of(new DecimalV3Literal(new BigDecimal("-2147483648.0")), new IntegerLiteral(-2147483648)),
                        Pair.of(new DoubleLiteral(-2147483648.0), new IntegerLiteral(-2147483648))),
                ImmutableList.of(
                        Pair.of(new TinyIntLiteral((byte) 0), null),
                        Pair.of(new SmallIntLiteral((short) 0), null),
                        Pair.of(new IntegerLiteral(-2147483647), null),
                        Pair.of(new DecimalV3Literal(new BigDecimal("-2147483647")), new IntegerLiteral(-2147483647)),
                        Pair.of(new DoubleLiteral(-2147483647.0), new IntegerLiteral(-2147483647)),
                        Pair.of(new IntegerLiteral(2147483646), null),
                        Pair.of(new DoubleLiteral(2147483646.0), new IntegerLiteral(2147483646))),
                ImmutableList.of(
                        Pair.of(new IntegerLiteral(2147483647), null),
                        Pair.of(new DecimalV3Literal(new BigDecimal("2147483647")), new IntegerLiteral(2147483647)),
                        Pair.of(new DecimalV3Literal(new BigDecimal("2147483647.00")), new IntegerLiteral(2147483647)),
                        Pair.of(new DoubleLiteral(2147483647.0), new IntegerLiteral(2147483647))),
                ImmutableList.of(
                        Pair.of(new BigIntLiteral(2147483648L), null),
                        Pair.of(new DecimalV3Literal(new BigDecimal("2147483648.02")), null),
                        Pair.of(new DoubleLiteral(2147483648.0), new BigIntLiteral(2147483648L)),
                        Pair.of(new DoubleLiteral(2147483647.1), new DecimalV3Literal(new BigDecimal("2147483647.1")))));

        checkTypeRangeLimit(BigIntType.INSTANCE,
                ImmutableList.of(
                        Pair.of(new LargeIntLiteral(new BigInteger("-9223372036854775809")), null),
                        Pair.of(new DecimalV3Literal(new BigDecimal("-9223372036854775809")), null),
                        Pair.of(new DecimalV3Literal(new BigDecimal("-9223372036854775808.1")), null),
                        Pair.of(new DoubleLiteral(-9223372036854775809.0), new LargeIntLiteral(new BigInteger("-9223372036854775809"))),
                        Pair.of(new DoubleLiteral(-9223372036854775808.1), new DecimalV3Literal(new BigDecimal("-9223372036854775808.1")))),
                ImmutableList.of(
                        Pair.of(new BigIntLiteral(-9223372036854775808L), null),
                        Pair.of(new DecimalV3Literal(new BigDecimal("-9223372036854775808")), new BigIntLiteral(-9223372036854775808L))),
                ImmutableList.of(
                        Pair.of(new BigIntLiteral(-9223372036854775807L), null),
                        Pair.of(new DecimalV3Literal(new BigDecimal("-9223372036854775807")), new BigIntLiteral(-9223372036854775807L)),
                        Pair.of(new DoubleLiteral(-9223372036854775000.0), new BigIntLiteral(-9223372036854775000L)),
                        Pair.of(new BigIntLiteral(9223372036854775806L), null),
                        Pair.of(new DoubleLiteral(9223372036854775000.0), new BigIntLiteral(9223372036854775000L))),
                ImmutableList.of(
                        Pair.of(new BigIntLiteral(9223372036854775807L), null),
                        Pair.of(new DecimalV3Literal(new BigDecimal("9223372036854775807")), new BigIntLiteral(9223372036854775807L)),
                        Pair.of(new DecimalV3Literal(new BigDecimal("9223372036854775807.00")), new BigIntLiteral(9223372036854775807L))),
                ImmutableList.of(
                        Pair.of(new LargeIntLiteral(new BigInteger("9223372036854775808")), null),
                        Pair.of(new DecimalV3Literal(new BigDecimal("9223372036854775807.02")), null),
                        Pair.of(new DoubleLiteral(9223372036854775808.0), new LargeIntLiteral(new BigInteger("9223372036854775808"))),
                        Pair.of(new DoubleLiteral(9223372036854775807.1), new DecimalV3Literal(new BigDecimal("9223372036854775807.1")))));

        checkTypeRangeLimit(DecimalV3Type.createDecimalV3Type(5, 2),
                ImmutableList.of(
                        Pair.of(new IntegerLiteral(-1000), null),
                        Pair.of(new DoubleLiteral(-1000.1), null),
                        Pair.of(new DecimalV3Literal(new BigDecimal("-999.999")), null),
                        Pair.of(new DecimalV3Literal(new BigDecimal("-1000.00")), null),
                        Pair.of(new DecimalV3Literal(new BigDecimal("-1000.0123")), null)),
                ImmutableList.of(
                        Pair.of(new DecimalV3Literal(new BigDecimal("-999.99")), null)),
                ImmutableList.of(
                        Pair.of(new DecimalV3Literal(new BigDecimal("100.4")), null),
                        Pair.of(new DecimalV3Literal(new BigDecimal("100")), null)),
                ImmutableList.of(
                        Pair.of(new DecimalV3Literal(new BigDecimal("999.99")), null)),
                ImmutableList.of(
                        Pair.of(new IntegerLiteral(1000), null),
                        Pair.of(new DoubleLiteral(1000.1), null),
                        Pair.of(new DecimalV3Literal(new BigDecimal("1000")), null),
                        Pair.of(new DecimalV3Literal(new BigDecimal("999.999")), null)));
    }

    // each expr list item is: pair<origin right literal,  rewritten right literal>
    // if rewritten right literal = null, then rewritten right literal = origin right literal
    void checkTypeRangeLimit(DataType dataType, List<Pair<Expression, Expression>> lessThanMinExpr,
            List<Pair<Expression, Expression>> minExpr, List<Pair<Expression, Expression>> betweenMinMaxExpr,
            List<Pair<Expression, Expression>> maxExpr, List<Pair<Expression, Expression>> greaterThanMaxExpr) {
        // due to ComparisonPredicate constructor require not null left and right child,
        // use a dummyExpr as ComparisonPredicate's child
        Expression dummyExpr = new SmallIntLiteral((short) 100);
        // cp -> list of cp with lessThanMinExpr, minExpr, betweenMinMaxExpr, maxExpr, greaterThanMaxExpr
        List<Pair<ComparisonPredicate, List<RangeLimitResult>>> cmpResults = ImmutableList.of(
                Pair.of(new EqualTo(dummyExpr, dummyExpr), ImmutableList.of(
                        RangeLimitResult.FALSE, RangeLimitResult.NO_CHANGE_CP, RangeLimitResult.NO_CHANGE_CP,
                        RangeLimitResult.NO_CHANGE_CP, RangeLimitResult.FALSE)),
                Pair.of(new NullSafeEqual(dummyExpr, dummyExpr), ImmutableList.of(
                        RangeLimitResult.FALSE, RangeLimitResult.NO_CHANGE_CP, RangeLimitResult.NO_CHANGE_CP,
                        RangeLimitResult.NO_CHANGE_CP, RangeLimitResult.FALSE)),
                Pair.of(new GreaterThan(dummyExpr, dummyExpr), ImmutableList.of(
                        RangeLimitResult.TRUE, RangeLimitResult.NO_CHANGE_CP, RangeLimitResult.NO_CHANGE_CP,
                        RangeLimitResult.FALSE, RangeLimitResult.FALSE)),
                Pair.of(new GreaterThanEqual(dummyExpr, dummyExpr), ImmutableList.of(
                        RangeLimitResult.TRUE, RangeLimitResult.TRUE, RangeLimitResult.NO_CHANGE_CP,
                        RangeLimitResult.EQUALS, RangeLimitResult.FALSE)),
                Pair.of(new LessThan(dummyExpr, dummyExpr), ImmutableList.of(
                        RangeLimitResult.FALSE, RangeLimitResult.FALSE, RangeLimitResult.NO_CHANGE_CP,
                        RangeLimitResult.NO_CHANGE_CP, RangeLimitResult.TRUE)),
                Pair.of(new LessThanEqual(dummyExpr, dummyExpr), ImmutableList.of(
                        RangeLimitResult.FALSE, RangeLimitResult.EQUALS, RangeLimitResult.NO_CHANGE_CP,
                        RangeLimitResult.TRUE, RangeLimitResult.TRUE))
        );

        for (Pair<ComparisonPredicate, List<RangeLimitResult>> cmpResult : cmpResults) {
            ComparisonPredicate cp = cmpResult.first;
            List<RangeLimitResult> result = cmpResult.second;
            checkTypeRangeLimitWithComparison(dataType, cp, lessThanMinExpr, result.get(0));
            checkTypeRangeLimitWithComparison(dataType, cp, minExpr, result.get(1));
            checkTypeRangeLimitWithComparison(dataType, cp, betweenMinMaxExpr, result.get(2));
            checkTypeRangeLimitWithComparison(dataType, cp, maxExpr, result.get(3));
            checkTypeRangeLimitWithComparison(dataType, cp, greaterThanMaxExpr, result.get(4));
        }
    }

    void checkTypeRangeLimitWithComparison(DataType dataType, ComparisonPredicate cp,
            List<Pair<Expression, Expression>> exprs, RangeLimitResult result) {
        Expression slot = new SlotReference("slot", dataType, true);
        for (Pair<Expression, Expression> pair : exprs) {
            Expression right = pair.first;
            Expression rewriteRight = pair.second;
            if (rewriteRight == null) {
                rewriteRight = right;
            }
            Expression left = slot;
            if (!left.getDataType().equals(right.getDataType())) {
                left = new Cast(slot, right.getDataType());
            }
            Expression originExpr = cp.withChildren(left, right);
            Expression rewrittenExpr = executor.rewrite(originExpr, context);
            Expression expectExpr = null;
            // System.out.println("slot type: " + slot.getDataType() + ", literal type: " + right.getDataType());
            // System.out.println("origin expr: " + originExpr);
            // System.out.println("rewrite expr: " + rewrittenExpr);
            switch (result) {
                case TRUE:
                    expectExpr = cp instanceof NullSafeEqual ? BooleanLiteral.TRUE
                            : ExpressionUtils.trueOrNull(slot);
                    break;
                case FALSE:
                    expectExpr = cp instanceof NullSafeEqual ? BooleanLiteral.FALSE
                        : ExpressionUtils.falseOrNull(slot);
                    break;
                case EQUALS:
                    Expression expectLeft = slot.getDataType().equals(rewriteRight.getDataType()) ? slot : left;
                    expectExpr = new EqualTo(expectLeft, rewriteRight);
                    break;
                case NO_CHANGE_CP:
                    Assertions.assertInstanceOf(cp.getClass(), rewrittenExpr);
                    break;
                default:
                    Assertions.assertTrue(false);
            }
            if (expectExpr != null) {
                Assertions.assertEquals(expectExpr, rewrittenExpr);
            }
        }
    }
}
