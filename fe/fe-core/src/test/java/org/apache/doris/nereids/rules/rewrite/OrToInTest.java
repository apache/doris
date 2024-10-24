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

import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.rules.expression.rules.OrToIn;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

class OrToInTest extends ExpressionRewriteTestHelper {

    @Test
    void test1() {
        String expr = "col1 = 1 or col1 = 2 or col1 = 3 and (col2 = 4)";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("(col1 IN (1, 2, 3) AND (col1 IN (1, 2) OR ((col1 = 3) AND (col2 = 4))))",
                rewritten.toSql());
    }

    @Test
    void test2() {
        String expr = "col1 = 1 and col1 = 3 and col2 = 3 or col2 = 4";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("(col2 = 4)",
                rewritten.toSql());
    }

    @Test
    void test3() {
        String expr = "(col1 = 1 or col1 = 2) and  (col2 = 3 or col2 = 4)";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.INSTANCE.rewriteTree(expression, context);
        List<InPredicate> inPredicates = rewritten.collectToList(e -> e instanceof InPredicate);
        Assertions.assertEquals(2, inPredicates.size());
        InPredicate in1 = inPredicates.get(0);
        Assertions.assertEquals("col1", ((NamedExpression) in1.getCompareExpr()).getName());
        Set<Integer> opVals1 = ImmutableSet.of(1, 2);
        for (Expression op : in1.getOptions()) {
            Literal literal = (Literal) op;
            Assertions.assertTrue(opVals1.contains(((Byte) literal.getValue()).intValue()));
        }
        InPredicate in2 = inPredicates.get(1);
        Assertions.assertEquals("col2", ((NamedExpression) in2.getCompareExpr()).getName());
        Set<Integer> opVals2 = ImmutableSet.of(3, 4);
        for (Expression op : in2.getOptions()) {
            Literal literal = (Literal) op;
            Assertions.assertTrue(opVals2.contains(((Byte) literal.getValue()).intValue()));
        }
    }

    @Test
    void test4() {
        String expr = "case when col = 1 or col = 2 or col = 3 then 1"
                + "         when col = 4 or col = 5 or col = 6 then 1 else 0 end";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("CASE WHEN col IN (1, 2, 3) THEN 1 WHEN col IN (4, 5, 6) THEN 1 ELSE 0 END",
                rewritten.toSql());
    }

    @Test
    void test5() {
        String expr = "col = 1 or (col = 2 and (col = 3 or col = 4 or col = 5))";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("(col = 1)",
                rewritten.toSql());
    }

    @Test
    void test6() {
        String expr = "col = 1 or col = 2 or col in (1, 2, 3)";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("col IN (1, 2, 3)", rewritten.toSql());
    }

    @Test
    void test7() {
        String expr = "A = 1 or A = 2 or abs(A)=5 or A in (1, 2, 3) or B = 1 or B = 2 or B in (1, 2, 3) or B+1 in (4, 5, 7)";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("(((A IN (1, 2, 3) OR (abs(A) = 5)) OR B IN (1, 2, 3)) OR (B + 1) IN (4, 5, 7))", rewritten.toSql());
    }

    @Test
    void test8() {
        String expr = "col = 1 or (col = 2 and (col = 3 or col = '4' or col = 5.0))";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("((col = 1) OR ((col = 2) AND col IN ('4', 3, 5.0)))",
                rewritten.toSql());
    }

    @Test
    void testEnsureOrder() {
        // ensure not rewrite to col2 in (1, 2) or  cor 1 in (1, 2)
        String expr = "col1 IN (1, 2) OR col2 IN (1, 2)";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("(col1 IN (1, 2) OR col2 IN (1, 2))",
                rewritten.toSql());
    }

    @Test
    void test9() {
        String expr = "col1=1 and (col2=1 or col2=2)";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("((col1 = 1) AND col2 IN (1, 2))",
                rewritten.toSql());
    }

    @Test
    void test10() {
        // recursive rewrites
        String expr = "col1=1 or (col2 = 2 and (col3=4 or col3=5))";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("((col1 = 1) OR ((col2 = 2) AND col3 IN (4, 5)))",
                rewritten.toSql());
    }

    @Test
    void test11() {
        // rewrite multi-inPredicates
        String expr = "(a=1 and b=2 and c=3) or (a=2 and b=2 and c=4)";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("((b = 2) AND ((a IN (1, 2) AND c IN (3, 4)) AND (((a = 1) AND (c = 3)) OR ((a = 2) AND (c = 4)))))",
                rewritten.toSql());
    }

    @Test
    void test12() {
        // no rewrite
        String expr = "a in (1, 2) and a in (3, 4)";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("FALSE",
                rewritten.toSql());
    }

    @Test
    void test13() {
        // no rewrite, because of "a like 'xyz'"
        String expr = "a like 'xyz% or a=1 or a=2': no extract";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("(a like 'xyz% or a=1 or a=2')",
                rewritten.toSql());
    }

    @Test
    void test14() {
        // no rewrite, because of "f(a)"
        String expr = "(a=1 and f(a)=2) or a=3";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("(((a = 1) AND (f(a) = 2)) OR (a = 3))",
                rewritten.toSql());
    }

    @Test
    void test15() {
        // no rewrite, because of "a like 'xyz'"
        String expr = "x=1 or (a=1 and b=2) or (a=2 and c=3)";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("((x = 1) OR (((a = 1) AND (b = 2)) OR ((a = 2) AND (c = 3))))",
                rewritten.toSql());
    }

    @Test
    void test16() {
        String expr = "a=1 or a=1 or a=1";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("(a = 1)",
                rewritten.toSql());
    }

    @Test
    void test17() {
        String expr = "(a=1 and b=2) or (a in (2, 3) and ((a=2 and c=3) or (a=3 and d=4)))";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.INSTANCE.rewriteTree(expression, context);
        System.out.println(rewritten);
    }
}
