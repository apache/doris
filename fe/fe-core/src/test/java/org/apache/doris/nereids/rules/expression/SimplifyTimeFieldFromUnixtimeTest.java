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
        assertRewriteAfterTypeCoercion("hour(cast(from_unixtime(IA) as datetime))",
                "hour_from_unixtime(IA)");
        assertRewriteAfterTypeCoercion("minute(cast(from_unixtime(IA) as datetime))",
                "minute_from_unixtime(IA)");
        assertRewriteAfterTypeCoercion("second(cast(from_unixtime(IA) as datetime))",
                "second_from_unixtime(IA)");
        assertRewriteAfterTypeCoercion("microsecond(cast(from_unixtime(DECIMAL_V3_A) as datetimev2(6)))",
                "microsecond_from_unixtime(cast(DECIMAL_V3_A as DECIMALV3(18, 6)))");
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
