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

import org.apache.doris.nereids.rules.expression.rules.FoldConstantRule;
import org.apache.doris.nereids.rules.expression.rules.SimplifyInPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class SimplifyInPredicateTest extends ExpressionRewriteTestHelper {

    private ExpressionRuleExecutor newRewriteExecutor() {
        return new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(
                        FoldConstantRule.INSTANCE,
                        SimplifyInPredicate.INSTANCE)));
    }

    private ExpressionRuleExecutor newFoldExecutor() {
        return new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(
                        FoldConstantRule.INSTANCE)));
    }

    @Test
    public void test() {
        executor = newRewriteExecutor();
        Map<String, Slot> mem = Maps.newHashMap();
        Expression rewrittenExpression = PARSER.parseExpression("cast(CA as DATETIME) in ('1992-01-31 00:00:00', '1992-02-01 00:00:00')");
        // after parse and type coercion: CAST(CAST(CA AS DATETIMEV2(0)) AS DATETIMEV2(6)) IN ('1992-01-31 00:00:00.000000', '1992-02-01 00:00:00.000000')
        rewrittenExpression = typeCoercion(replaceUnboundSlot(rewrittenExpression, mem));
        // after first rewrite: CAST(CA AS DATETIMEV2(0)) IN ('1992-01-31 00:00:00', '1992-02-01 00:00:00')
        rewrittenExpression = executor.rewrite(rewrittenExpression, context);
        // after second rewrite: CA IN ('1992-01-31', '1992-02-01')
        rewrittenExpression = executor.rewrite(rewrittenExpression, context);
        Expression expectedExpression = PARSER.parseExpression("CA in (cast('1992-01-31' as date), cast('1992-02-01' as date))");
        expectedExpression = replaceUnboundSlot(expectedExpression, mem);
        executor = newFoldExecutor();
        expectedExpression = executor.rewrite(expectedExpression, context);
        Assertions.assertEquals(expectedExpression, rewrittenExpression);
    }

    @Test
    public void testDoNotEliminateNarrowingDateTimeV2Cast() {
        ExpressionRuleExecutor rewriteExecutor = newRewriteExecutor();
        ExpressionRuleExecutor foldExecutor = newFoldExecutor();
        Map<String, Slot> mem = Maps.newHashMap();
        Expression rewrittenExpression = PARSER.parseExpression("cast(AA as DATETIMEV2(3)) in "
                + "('2024-01-01 12:34:56.123000', '2024-01-01 09:30:01.000000', '2024-01-01 22:00:00.000000')");
        rewrittenExpression = typeCoercion(replaceUnboundSlot(rewrittenExpression, mem));
        rewrittenExpression = rewriteExecutor.rewrite(rewrittenExpression, context);
        Expression rewrittenAgain = rewriteExecutor.rewrite(rewrittenExpression, context);

        Expression expectedExpression = PARSER.parseExpression("cast(AA as DATETIMEV2(3)) in "
                + "(cast('2024-01-01 12:34:56.123' as DATETIMEV2(3)), "
                + "cast('2024-01-01 09:30:01.000' as DATETIMEV2(3)), "
                + "cast('2024-01-01 22:00:00.000' as DATETIMEV2(3)))");
        expectedExpression = replaceUnboundSlot(expectedExpression, mem);
        expectedExpression = foldExecutor.rewrite(expectedExpression, context);

        Assertions.assertEquals(expectedExpression, rewrittenExpression);
        Assertions.assertEquals(rewrittenExpression, rewrittenAgain);
    }

    @Test
    public void testDateTimeV2LiteralMustAlignWithTargetScale() {
        ExpressionRuleExecutor rewriteExecutor = newRewriteExecutor();
        Map<String, Slot> mem = Maps.newHashMap();
        Expression rewrittenExpression = PARSER.parseExpression("cast(cast(AA as DATETIMEV2(3)) as DATETIMEV2(6)) "
                + "in ('2024-01-01 12:34:56.123128')");
        rewrittenExpression = typeCoercion(replaceUnboundSlot(rewrittenExpression, mem));
        Expression originalExpression = rewrittenExpression;

        rewrittenExpression = rewriteExecutor.rewrite(rewrittenExpression, context);

        Assertions.assertEquals(originalExpression, rewrittenExpression);
    }
}
