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

import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class InPredicateExtractNonConstantTest extends ExpressionRewriteTestHelper {

    public InPredicateExtractNonConstantTest() {
        executor = new ExpressionRuleExecutor(ExpressionNormalization.NORMALIZE_REWRITE_RULES);
    }

    @Test
    public void testAssertTrueOption() {
        assertNormalized("true in (true, true, assert_true(IA > 0, 'invalid value'))",
                "true in (true, assert_true(IA > 0, 'invalid value'))");
        assertNormalized("true not in (true, true, assert_true(IA > 0, 'invalid value'))",
                "true not in (true, assert_true(IA > 0, 'invalid value'))");
        assertNormalized("true in (true, not assert_true(IA > 0, 'invalid value'))",
                "true in (true, not assert_true(IA > 0, 'invalid value'))");
    }

    @Test
    public void testAssertTrueCompareExpression() {
        assertNormalized("assert_true(IA > 0, 'invalid value') in (true, BA)",
                "assert_true(IA > 0, 'invalid value') in (true, BA)");
    }

    @Test
    public void testToBitmapWithCheck() {
        assertNormalized("1 in (1, bitmap_count(to_bitmap_with_check(SA)))",
                "1 in (1, bitmap_count(to_bitmap_with_check(SA)))");
        assertNormalized("bitmap_count(to_bitmap_with_check(SA)) in (TA, TB)",
                "bitmap_count(to_bitmap_with_check(SA)) in (TA, TB)");
    }

    @Test
    public void testVolatileOption() {
        assertNormalized("true in (true, rand() > 0)",
                "true in (true, rand() > 0)");
        assertNormalized("SA in (SB, uuid())", "SA in (SB, uuid())");
    }

    @Test
    public void testVolatileCompareExpression() {
        assertNormalized("uuid() in (SA, SB)", "uuid() in (SA, SB)");
    }

    @Test
    public void testOrdinaryExtraction() {
        assertNormalized("TA in (3, 2, 1, TB, TC, TB)",
                "TA in (3, 2, 1) or TA = TB or TA = TC");
        assertNormalized("true in (true, true, BA)", "true");
    }

    private void assertNormalized(String expression, String expected) {
        Map<String, Slot> slots = Maps.newHashMap();
        Expression input = typeCoercion(replaceUnboundSlot(PARSER.parseExpression(expression), slots));
        Expression expectedExpression = typeCoercion(replaceUnboundSlot(PARSER.parseExpression(expected), slots));
        expectedExpression = FoldConstantRuleOnFE.evaluate(expectedExpression, context);
        Expression rewritten = executor.rewrite(input, context);
        Assertions.assertEquals(expectedExpression.toSql(), rewritten.toSql());
        Assertions.assertEquals(rewritten, executor.rewrite(rewritten, context));
    }
}
