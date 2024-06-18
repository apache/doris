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

    @Test
    public void test() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(
                    FoldConstantRule.INSTANCE,
                    SimplifyInPredicate.INSTANCE
                )
        ));
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
        executor = new ExpressionRuleExecutor(ImmutableList.of(
            bottomUp(
                FoldConstantRule.INSTANCE
            )
        ));
        expectedExpression = executor.rewrite(expectedExpression, context);
        Assertions.assertEquals(expectedExpression, rewrittenExpression);
    }
}
