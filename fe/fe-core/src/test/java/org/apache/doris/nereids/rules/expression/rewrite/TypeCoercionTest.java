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

package org.apache.doris.nereids.rules.expression.rewrite;

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.expression.rewrite.rules.TypeCoercion;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.Avg;
import org.apache.doris.nereids.trees.expressions.functions.Substring;
import org.apache.doris.nereids.trees.expressions.functions.Sum;
import org.apache.doris.nereids.trees.expressions.functions.Year;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TypeCoercionTest {

    private static final NereidsParser PARSER = new NereidsParser();
    private ExpressionRuleExecutor executor;

    @Test
    public void testSubStringImplicitCast() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(TypeCoercion.INSTANCE));
        Expression expression = new Substring(
                new StringLiteral("abc"),
                new StringLiteral("1"),
                new StringLiteral("3")
        );
        Expression expected = new Substring(
                new StringLiteral("abc"),
                new Cast(new StringLiteral("1"), IntegerType.INSTANCE),
                new Cast(new StringLiteral("3"), IntegerType.INSTANCE)
        );
        assertRewrite(expression, expected);
    }

    @Test
    public void testLikeImplicitCast() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(TypeCoercion.INSTANCE));
        String expression = "1 like 5";
        String expected = "cast(1 as string) like cast(5 as string)";
        assertRewrite(expression, expected);
    }

    @Test
    public void testRegexImplicitCast() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(TypeCoercion.INSTANCE));
        String expression = "1 regexp 5";
        String expected = "cast(1 as string) regexp cast(5 as string)";
        assertRewrite(expression, expected);
    }

    @Test
    public void testSumImplicitCast() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(TypeCoercion.INSTANCE));
        Expression expression = new Sum(new StringLiteral("1"));
        Expression expected = new Sum(new Cast(new StringLiteral("1"), DoubleType.INSTANCE));
        assertRewrite(expression, expected);
    }

    @Test
    public void testYearImplicitCast() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(TypeCoercion.INSTANCE));
        Expression expression = new Year(new DateLiteral("2022-01-01"));
        Expression expected = new Year(new DateLiteral("2022-01-01"));
        assertRewrite(expression, expected);
    }

    @Test
    public void testAvgImplicitCast() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(TypeCoercion.INSTANCE));
        Expression expression = new Avg(new StringLiteral("1"));
        Expression expected = new Avg(new Cast(new StringLiteral("1"), DoubleType.INSTANCE));
        assertRewrite(expression, expected);
    }

    private void assertRewrite(Expression expression, Expression expected) {
        Expression rewrittenExpression = executor.rewrite(expression);
        Assertions.assertEquals(expected, rewrittenExpression);
    }

    private void assertRewrite(String expression, String expected) {
        Expression needRewriteExpression = PARSER.parseExpression(expression);
        Expression expectedExpression = PARSER.parseExpression(expected);
        assertRewrite(needRewriteExpression, expectedExpression);
    }
}
