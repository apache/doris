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

package org.apache.doris.nereids.rules.expression;

import org.apache.doris.nereids.rules.expression.rules.SimplifyTimeFieldFromUnixtime;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Tests for {@link SimplifyTimeFieldFromUnixtime}.
 */
public class SimplifyTimeFieldFromUnixtimeTest extends ExpressionRewriteTestHelper {
    public SimplifyTimeFieldFromUnixtimeTest() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
            bottomUp(SimplifyTimeFieldFromUnixtime.INSTANCE)));
    }

    @Test
    public void testRewriteSimple() {
        assertRewriteAfterTypeCoercion("hour(from_unixtime(IA))", "hour_from_unixtime(IA)");
        assertRewriteAfterTypeCoercion("minute(from_unixtime(IA))", "minute_from_unixtime(IA)");
        assertRewriteAfterTypeCoercion("second(from_unixtime(IA))", "second_from_unixtime(IA)");
        assertRewriteAfterTypeCoercion("microsecond(from_unixtime(DECIMAL_V3_A))", "microsecond_from_unixtime(cast(DECIMAL_V3_A as DECIMALV3(18, 6)))");
    }

    @Test
    public void testRewriteWithCast() {
        String hourWithCast = "hour(cast(from_unixtime(IA) as datetime))";
        assertRewriteAfterTypeCoercion(hourWithCast, "hour_from_unixtime(IA)");

        String minuteWithCast = "minute(cast(from_unixtime(IA) as datetime))";
        assertRewriteAfterTypeCoercion(minuteWithCast, "minute_from_unixtime(IA)");

        String secondWithCast = "second(cast(from_unixtime(IA) as datetimev2(0)))";
        assertRewriteAfterTypeCoercion(secondWithCast, "second_from_unixtime(IA)");

        String microsecondWithCast =
                "microsecond(cast(from_unixtime(DECIMAL_V3_A) as datetimev2(6)))";
        assertRewriteAfterTypeCoercion(microsecondWithCast,
                "microsecond_from_unixtime(cast(DECIMAL_V3_A as DECIMALV3(18, 6)))");

        assertRewriteAfterTypeCoercion(
                "hour(cast(from_unixtime(DECIMAL_V3_A) as datetimev2(6)))",
                "hour_from_unixtime(cast(DECIMAL_V3_A as DECIMALV3(18, 6)))");
        assertRewriteAfterTypeCoercion(
                "minute(cast(from_unixtime(DECIMAL_V3_A) as datetimev2(6)))",
                "minute_from_unixtime(cast(DECIMAL_V3_A as DECIMALV3(18, 6)))");
        assertRewriteAfterTypeCoercion(
                "second(cast(from_unixtime(DECIMAL_V3_A) as datetimev2(6)))",
                "second_from_unixtime(cast(DECIMAL_V3_A as DECIMALV3(18, 6)))");
    }

    @Test
    public void testNoRewriteWithLossyCast() {
        String hourWithSecondPrecisionCast =
                "hour(cast(from_unixtime(DECIMAL_V3_A) as datetime))";
        assertRewriteAfterTypeCoercion(hourWithSecondPrecisionCast, hourWithSecondPrecisionCast);

        String microsecondWithMillisecondCast =
                "microsecond(cast(from_unixtime(DECIMAL_V3_A) as datetimev2(3)))";
        assertRewriteAfterTypeCoercion(microsecondWithMillisecondCast, microsecondWithMillisecondCast);

        for (String function : ImmutableList.of("hour", "minute", "second")) {
            for (int scale : ImmutableList.of(0, 3)) {
                String expression = String.format(
                        "%s(cast(from_unixtime(DECIMAL_V3_A) as datetimev2(%d)))",
                        function, scale);
                assertRewriteAfterTypeCoercion(expression, expression);
            }
        }
    }

    @Test
    public void testNoRewriteWithTryCast() {
        String hourWithTryCast = "hour(try_cast(from_unixtime(IA) as datetime))";
        assertRewriteAfterTypeCoercion(hourWithTryCast, hourWithTryCast);

        String microsecondWithTryCast =
                "microsecond(try_cast(from_unixtime(DECIMAL_V3_A) as datetimev2(6)))";
        assertRewriteAfterTypeCoercion(microsecondWithTryCast, microsecondWithTryCast);
    }

    @Test
    public void testNoRewriteWithStrictCast() {
        Expression hour = replaceUnboundSlot(
                PARSER.parseExpression("hour(from_unixtime(IA))"), Maps.newHashMap());
        hour = typeCoercion(hour);
        Assertions.assertInstanceOf(Cast.class, hour.child(0));

        Cast implicitCast = (Cast) hour.child(0);
        Assertions.assertFalse(implicitCast.isExplicitType());
        Assertions.assertFalse(implicitCast.isStrict());

        Cast strictCast = new Cast(implicitCast.child(), implicitCast.getDataType(), false, true);
        Expression hourWithStrictCast = hour.withChildren(ImmutableList.of(strictCast));
        Assertions.assertEquals(hourWithStrictCast, executor.rewrite(hourWithStrictCast, context));
    }

    @Test
    public void testNoRewriteOnFormattedCall() {
        Map<String, org.apache.doris.nereids.trees.expressions.Slot> memo = Maps.newHashMap();
        Expression expression = replaceUnboundSlot(
                PARSER.parseExpression("hour(from_unixtime(IA, 'yyyy-MM-dd'))"), memo);
        expression = typeCoercion(expression);
        Expression rewritten = executor.rewrite(expression, context);
        Assertions.assertEquals(expression, rewritten);

        expression = replaceUnboundSlot(
                PARSER.parseExpression("minute(from_unixtime(IA, 'yyyy-MM-dd'))"), memo);
        expression = typeCoercion(expression);
        rewritten = executor.rewrite(expression, context);
        Assertions.assertEquals(expression, rewritten);

        expression = replaceUnboundSlot(
                PARSER.parseExpression("second(from_unixtime(IA, 'yyyy-MM-dd'))"), memo);
        expression = typeCoercion(expression);
        rewritten = executor.rewrite(expression, context);
        Assertions.assertEquals(expression, rewritten);

        expression = replaceUnboundSlot(
                PARSER.parseExpression("microsecond(from_unixtime(DECIMAL_V3_A, 'yyyy-MM-dd'))"), memo);
        expression = typeCoercion(expression);
        rewritten = executor.rewrite(expression, context);
        Assertions.assertEquals(expression, rewritten);
    }
}
