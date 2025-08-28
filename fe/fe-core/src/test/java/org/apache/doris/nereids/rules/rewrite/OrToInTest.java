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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class OrToInTest extends ExpressionRewriteTestHelper {

    @Test
    void testDeDup() {
        // (a=1 and b=1) or (a=2 and c=2) infers "a in (1, 2)"
        // (a=1 and d=1) or (a=2 and e=2) infers "a in (1, 2)" again
        // duplicated one should be removed
        Expression expression = PARSER.parseExpression("((a=1 and b=1) or (a=2 and c=2)) and ((a=1 and d=1) or (a=2 and e=2))");
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("AND[a IN (1, 2),OR[AND[(a = 1),(b = 1)],AND[(a = 2),(c = 2)]],OR[AND[(a = 1),(d = 1)],AND[(a = 2),(e = 2)]]]",
                rewritten.toSql());
    }

    // (a=1 and b=1) or (a=2 and c=2) infers "a in (1, 2)", but "a in (1, 2)" is not in root level,
    // and cannot be push down, and hence, do not extract "a in (1, 2)"
    @Test
    void testExtractNothing() {
        Expression expression = PARSER.parseExpression("((a=1 and b=1) or (a=2 and c=2) or d=1) and e = 2");
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("AND[OR[AND[(a = 1),(b = 1)],AND[(a = 2),(c = 2)],(d = 1)],(e = 2)]",
                rewritten.toSql());
    }

    // replace mode
    @Test
    void test1() {
        String expr = "col1 = 1 or col1 = 2 or col1 = 3 and (col2 = 4)";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("AND[col1 IN (1, 2, 3),OR[col1 IN (1, 2),AND[(col1 = 3),(col2 = 4)]]]",
                rewritten.toSql());
        Expression rewritten2 = OrToIn.REPLACE_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("OR[col1 IN (1, 2),AND[(col1 = 3),(col2 = 4)]]", rewritten2.toSql());
    }

    @Test
    void test2() {
        String expr = "col1 = 1 and col1 = 3 and col2 = 3 or col2 = 4";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("(col2 = 4)", rewritten.toSql());
    }

    @Test
    void test3() {
        String expr = "(A = 1 or A = 2) and  (B = 3 or B = 4)";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("AND[A IN (1, 2),B IN (3, 4)]", rewritten.toSql());
    }

    @Test
    void test4() {
        String expr = "case when col = 1 or col = 2 or col = 3 then 1"
                + "         when col = 4 or col = 5 or col = 6 then 1 else 0 end";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.REPLACE_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("CASE WHEN col IN (1, 2, 3) THEN 1 WHEN col IN (4, 5, 6) THEN 1 ELSE 0 END",
                rewritten.toSql());
    }

    @Test
    void test5() {
        String expr = "col = 1 or (col = 2 and (col = 3 or col = 4 or col = 5))";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("(col = 1)", rewritten.toSql());
    }

    @Test
    void test6() {
        String expr = "col = 1 or col = 2 or col in (1, 2, 3)";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("col IN (1, 2, 3)", rewritten.toSql());
    }

    @Test
    void test7() {
        String expr = "A = 1 or A = 2 or abs(A)=5 or A in (1, 2, 3) or B = 1 or B = 2 or B in (1, 2, 3) or B+1 in (4, 5, 7)";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("OR[A IN (1, 2, 3),(abs(A) = 5),B IN (1, 2, 3),(B + 1) IN (4, 5, 7)]",
                rewritten.toSql());
    }

    @Test
    void testEnsureOrder() {
        // ensure not rewrite to col2 in (1, 2) or  cor 1 in (1, 2)
        String expr = "col1 IN (1, 2) OR col2 IN (1, 2)";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("OR[col1 IN (1, 2),col2 IN (1, 2)]", rewritten.toSql());
    }

    @Test
    void test9() {
        String expr = "col1=1 and (col2=1 or col2=2)";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("AND[(col1 = 1),col2 IN (1, 2)]", rewritten.toSql());
    }

    @Test
    void test10() {
        // recursive rewrites
        String expr = "col1=1 or (col2 = 2 and (col3=4 or col3=5))";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("OR[(col1 = 1),AND[(col2 = 2),col3 IN (4, 5)]]", rewritten.toSql());
    }

    // replace mode
    @Test
    void test11() {
        // rewrite multi-inPredicates
        String expr = "(a=1 and b=2 and c=3) or (a=2 and b=2 and c=4)";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("AND[(b = 2),a IN (1, 2),c IN (3, 4),OR[AND[(a = 1),(c = 3)],AND[(a = 2),(c = 4)]]]",
                rewritten.toSql());
    }

    @Test
    void test12() {
        // no rewrite
        String expr = "a in (1, 2) and a in (3, 4)";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("FALSE", rewritten.toSql());
    }

    @Test
    void test13() {
        // no rewrite, because of "a like 'xyz'"
        String expr = "a like 'xyz% or a=1 or a=2'";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("(a like 'xyz% or a=1 or a=2')", rewritten.toSql());
    }

    @Test
    void test14() {
        // no rewrite, because of "f(a)"
        String expr = "(a=1 and f(a)=2) or a=3";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("OR[AND[(a = 1),(f(a) = 2)],(a = 3)]", rewritten.toSql());
    }

    @Test
    void test15() {
        // no rewrite, because of "a like 'xyz'"
        String expr = "x=1 or (a=1 and b=2) or (a=2 and c=3)";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("OR[(x = 1),AND[(a = 1),(b = 2)],AND[(a = 2),(c = 3)]]", rewritten.toSql());
    }

    @Test
    void test16() {
        String expr = "a=1 or a=1 or a=1";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("(a = 1)", rewritten.toSql());
    }

    //replace mode
    @Test
    void test17() {
        String expr = "(a=1 and b=2) or (a in (2, 3) and ((a=2 and c=3) or (a=3 and d=4)))";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        System.out.println(rewritten);
    }
}
