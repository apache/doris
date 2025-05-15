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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ConstantPropagationTest {

    private final ConstantPropagation executor = new ConstantPropagation();
    private final NereidsParser parser = new NereidsParser();
    private final ExpressionRewriteContext context;
    private final LogicalOlapScan dummyPlan = PlanConstructor.newLogicalOlapScan(1, "tbl", 0);

    ConstantPropagationTest() {
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(
                new UnboundRelation(new RelationId(1), ImmutableList.of("tbl")));
        context = new ExpressionRewriteContext(cascadesContext);
    }

    @Test
    public void testRewriteExpression() {
        assertRewrite("a = 1 and a = b", "a = 1 and b = 1");
        assertRewrite("a = 1 and a + b = 2 and b + c = 2 and c + d = 2 and d + e = 2 and e + f = 2",
                "a = 1 and b = 1 and c = 1 and d = 1 and e = 1 and f = 1");
        assertRewrite("(a = 1 and a + 1 = b or b = 2 and b + c = 5) and d = 4",
                "(a = 1 and b = 2 or b = 2 and c = 3) and d = 4");

        // Multiple equalities chained together
        assertRewrite("a = 10 and a = b and b = c", "a = 10 and b = 10 and c = 10");

        // Conflicting constants
        assertRewrite("a = 10 and a = 20", "false");
        assertRewrite("a = 10 and b = a and b = 20", "false");

        // Different data types
        assertRewrite("a = 1.5 and a = b", "cast(a as decimal(21, 1)) = cast(1.5 as decimal(21, 1)) and a = b");
        assertRewrite("SA = 'test' and SB = SA", "SA = 'test' and SB = 'test'");
        assertRewrite("BA = true and BB = BA", "BA = true and BB = true");

        // Complex arithmetic expressions
        assertRewrite("a = 10 and b = a + 5 and c = b * 2",
                "a = 10 and b = 15 and c = 30");
        assertRewrite("x = 5 and y = x * 2 and z = y + x",
                "x = 5 and y = 10 and z = 15");

        // OR conditions
        assertRewrite("(a = 1 and b = a) or (a = 2 and b = a)",
                "(a = 1 and b = 1) or (a = 2 and b = 2)");
        assertRewrite("a = 5 or (b = a and c = b)",
                "a = 5 or (b = a and c = b)");

        // Mixed AND/OR conditions
        assertRewrite("(a = 1 and b = a) or (c = 2 and d = c and e = d)",
                "(a = 1 and b = 1) or (c = 2 and d = 2 and e = 2)");

        // Multiple arithmetic operations
        assertRewrite("x = 10 and y = x + 5 and z = y * 2 and w = z - x",
                "x = 10 and y = 15 and z = 30 and w = 20");

        // Complex expressions with multiple variables
        assertRewrite("a = 5 and b = a and c = a + b and d = b + c",
                "a = 5 and c = 10 and d = 15 and b = 5");

        // Transitive equality relationships
        assertRewrite("x = y and y = z and z = 100",
                "z = 100 and x = 100 and y = 100");

        // Redundant conditions
        assertRewrite("a = 10 and a = 10 and b = a",
                "a = 10 and b = 10");

        // Mixed constant types in expressions
        assertRewrite("x = 10 and y = x/2 and z = y + 5",
                "x = 10 and y = 5 and z = 10");
    }

    private void assertRewrite(String expression, String expected) {
        Expression rewriteExpression = parser.parseExpression(expression);
        rewriteExpression = ExpressionRewriteTestHelper.typeCoercion(
                ExpressionRewriteTestHelper.replaceUnboundSlot(rewriteExpression, Maps.newHashMap()));
        rewriteExpression = executor.replaceConstantsAndRewriteExpr(dummyPlan, rewriteExpression, context);
        Expression expectedExpression = parser.parseExpression(expected);
        Assertions.assertEquals(expectedExpression.toSql(), rewriteExpression.toSql());
    }

}
