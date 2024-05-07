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

import org.apache.doris.nereids.rules.expression.ExpressionRewrite;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class SimplifyArithmeticComparisonRuleTest extends ExpressionRewriteTestHelper {

    @Test
    public void testProcess() {
        Map<String, Slot> nameToSlot = new HashMap<>();
        nameToSlot.put("a", new SlotReference("a", IntegerType.INSTANCE));
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                ExpressionRewrite.bottomUp(SimplifyArithmeticComparisonRule.INSTANCE)
        ));
        assertRewriteAfterSimplify("a + 1 > 1", "a > cast((1 - 1) as INT)", nameToSlot);
        assertRewriteAfterSimplify("a - 1 > 1", "a > cast((1 + 1) as INT)", nameToSlot);
        assertRewriteAfterSimplify("a / -2 > 1", "cast((1 * -2) as INT) > a", nameToSlot);

        // test integer type
        assertRewriteAfterSimplify("1 + a > 2", "a > cast((2 - 1) as INT)", nameToSlot);
        assertRewriteAfterSimplify("-1 + a > 2", "a > cast((2 - (-1)) as INT)", nameToSlot);
        assertRewriteAfterSimplify("1 - a > 2", "a < cast((1 - 2) as INT)", nameToSlot);
        assertRewriteAfterSimplify("-1 - a > 2", "a < cast(((-1) - 2) as INT)", nameToSlot);
        assertRewriteAfterSimplify("2 * a > 1", "((2 * a) > 1)", nameToSlot);
        assertRewriteAfterSimplify("-2 * a > 1", "((-2 * a) > 1)", nameToSlot);
        assertRewriteAfterSimplify("2 / a > 1", "((2 / a) > 1)", nameToSlot);
        assertRewriteAfterSimplify("-2 / a > 1", "((-2 / a) > 1)", nameToSlot);
        assertRewriteAfterSimplify("a * 2 > 1", "((a * 2) > 1)", nameToSlot);
        assertRewriteAfterSimplify("a * (-2) > 1", "((a * (-2)) > 1)", nameToSlot);
        assertRewriteAfterSimplify("a / 2 > 1", "(a > cast((1 * 2) as INT))", nameToSlot);

        // test decimal type
        assertRewriteAfterSimplify("1.1 + a > 2.22", "(cast(a as DECIMALV3(12, 2)) > cast((2.22 - 1.1) as DECIMALV3(12, 2)))", nameToSlot);
        assertRewriteAfterSimplify("-1.1 + a > 2.22", "(cast(a as DECIMALV3(12, 2)) > cast((2.22 - (-1.1)) as DECIMALV3(12, 2)))", nameToSlot);
        assertRewriteAfterSimplify("1.1 - a > 2.22", "(cast(a as DECIMALV3(11, 1)) < cast((1.1 - 2.22) as DECIMALV3(11, 1)))", nameToSlot);
        assertRewriteAfterSimplify("-1.1 - a > 2.22", "(cast(a as DECIMALV3(11, 1)) < cast((-1.1 - 2.22) as DECIMALV3(11, 1)))", nameToSlot);
        assertRewriteAfterSimplify("2.22 * a > 1.1", "((2.22 * a) > 1.1)", nameToSlot);
        assertRewriteAfterSimplify("-2.22 * a > 1.1", "-2.22 * a > 1.1", nameToSlot);
        assertRewriteAfterSimplify("2.22 / a > 1.1", "((2.22 / a) > 1.1)", nameToSlot);
        assertRewriteAfterSimplify("-2.22 / a > 1.1", "((-2.22 / a) > 1.1)", nameToSlot);
        assertRewriteAfterSimplify("a * 2.22 > 1.1", "a * 2.22 > 1.1", nameToSlot);
        assertRewriteAfterSimplify("a * (-2.22) > 1.1", "a * (-2.22) > 1.1", nameToSlot);
        assertRewriteAfterSimplify("a / 2.22 > 1.1", "(cast(a as DECIMALV3(13, 3)) > cast((1.1 * 2.22) as DECIMALV3(13, 3)))", nameToSlot);
        assertRewriteAfterSimplify("a / (-2.22) > 1.1", "(cast((1.1 * -2.22) as DECIMALV3(13, 3)) > cast(a as DECIMALV3(13, 3)))", nameToSlot);

        // test (1 + a) can be processed
        assertRewriteAfterSimplify("2 - (1 + a) > 3", "(a < ((2 - 3) - 1))", nameToSlot);
        assertRewriteAfterSimplify("(1 - a) / 2 > 3", "(a < (1 - 6))", nameToSlot);
        assertRewriteAfterSimplify("1 - a / 2 > 3", "(a < ((1 - 3) * 2))", nameToSlot);
        assertRewriteAfterSimplify("(1 - (a + 4)) / 2 > 3", "(cast(a as BIGINT) < ((1 - 6) - 4))", nameToSlot);
        assertRewriteAfterSimplify("2 * (1 + a) > 1", "(2 * (1 + a)) > 1", nameToSlot);
    }

    private void assertRewriteAfterSimplify(String expr, String expected, Map<String, Slot> slotNameToSlot) {
        Expression needRewriteExpression = PARSER.parseExpression(expr);
        if (slotNameToSlot != null) {
            needRewriteExpression = replaceUnboundSlot(needRewriteExpression, slotNameToSlot);
        }
        Expression rewritten = executor.rewrite(needRewriteExpression, context);
        Expression expectedExpression = PARSER.parseExpression(expected);
        Assertions.assertEquals(expectedExpression.toSql(), rewritten.toSql());
    }
}
