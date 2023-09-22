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
        executor = new ExpressionRuleExecutor(ImmutableList.of(SimplifyArithmeticComparisonRule.INSTANCE));
        assertRewriteAfterSimplify("a + 1 > 1", "a > cast((1 - 1) as INT)", nameToSlot);
        assertRewriteAfterSimplify("a - 1 > 1", "a > cast((1 + 1) as INT)", nameToSlot);
        assertRewriteAfterSimplify("a / -2 > 1", "cast((1 * -2) as INT) > a", nameToSlot);
    }

    private void assertRewriteAfterSimplify(String expr, String expected, Map<String, Slot> slotNameToSlot) {
        Expression needRewriteExpression = PARSER.parseExpression(expr);
        if (slotNameToSlot != null) {
            needRewriteExpression = replaceUnboundSlot(needRewriteExpression, slotNameToSlot);
        }
        Expression rewritten = SimplifyArithmeticComparisonRule.INSTANCE.rewrite(needRewriteExpression, context);
        Expression expectedExpression = PARSER.parseExpression(expected);
        Assertions.assertEquals(expectedExpression.toSql(), rewritten.toSql());
    }
}
