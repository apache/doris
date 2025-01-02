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
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.util.ExpressionUtils;

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

        // test overflow, not cast
        assertRewrite(new LessThan(new Cast(date, DateTimeType.INSTANCE), new DateTimeLiteral("9999-12-31 23:59:59")),
                new LessThan(new Cast(date, DateTimeType.INSTANCE), new DateTimeLiteral("9999-12-31 23:59:59")));
        assertRewrite(new LessThan(new Cast(date, DateTimeV2Type.SYSTEM_DEFAULT), new DateTimeV2Literal("9999-12-31 23:59:59")),
                new LessThan(new Cast(date, DateTimeV2Type.SYSTEM_DEFAULT), new DateTimeV2Literal("9999-12-31 23:59:59")));
        assertRewrite(new LessThan(new Cast(datev1, DateTimeType.INSTANCE), new DateTimeLiteral("9999-12-31 23:59:59")),
                new LessThan(new Cast(datev1, DateTimeType.INSTANCE), new DateTimeLiteral("9999-12-31 23:59:59")));
        assertRewrite(new LessThan(new Cast(datev1, DateTimeV2Type.SYSTEM_DEFAULT), new DateTimeV2Literal("9999-12-31 23:59:59")),
                new LessThan(new Cast(datev1, DateTimeV2Type.SYSTEM_DEFAULT), new DateTimeV2Literal("9999-12-31 23:59:59")));
        assertRewrite(new LessThan(new Cast(datetime0, DateTimeV2Type.of(1)), new DateTimeV2Literal("9999-12-31 23:59:59.1")),
                new LessThan(new Cast(datetime0, DateTimeV2Type.of(1)), new DateTimeV2Literal("9999-12-31 23:59:59.1")));
        assertRewrite(new LessThan(new Cast(datetime2, DateTimeV2Type.of(3)), new DateTimeV2Literal("9999-12-31 23:59:59.991")),
                new LessThan(new Cast(datetime2, DateTimeV2Type.of(3)), new DateTimeV2Literal("9999-12-31 23:59:59.991")));
        assertRewrite(new LessThan(new Cast(datetime2, DateTimeV2Type.of(6)), new DateTimeV2Literal("9999-12-31 23:59:59.999999")),
                new LessThan(new Cast(datetime2, DateTimeV2Type.of(6)), new DateTimeV2Literal("9999-12-31 23:59:59.999999")));
        assertRewrite(new LessThan(new Cast(datetimev1, DateTimeV2Type.of(1)), new DateTimeV2Literal("9999-12-31 23:59:59.1")),
                new LessThan(new Cast(datetimev1, DateTimeV2Type.of(1)), new DateTimeV2Literal("9999-12-31 23:59:59.1")));
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
                new LessThan(new Cast(bigIntSlot, DecimalV3Type.createDecimalV3Type(20, 1)), new DecimalV3Literal(new BigDecimal("-9223372036854775808.1"))));
        assertRewrite(new LessThan(new Cast(bigIntSlot, DecimalV3Type.createDecimalV3Type(20, 1)), new DecimalV3Literal(new BigDecimal("-9223372036854775807.1"))),
                new LessThan(bigIntSlot, new BigIntLiteral(-9223372036854775807L)));
        assertRewrite(new GreaterThanEqual(new Cast(bigIntSlot, DecimalV3Type.createDecimalV3Type(20, 1)), new DecimalV3Literal(new BigDecimal("9223372036854775807.1"))),
                new GreaterThanEqual(new Cast(bigIntSlot, DecimalV3Type.createDecimalV3Type(20, 1)), new DecimalV3Literal(new BigDecimal("9223372036854775807.1"))));
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
