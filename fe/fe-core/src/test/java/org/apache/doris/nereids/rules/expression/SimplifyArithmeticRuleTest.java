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

import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRule;
import org.apache.doris.nereids.rules.expression.rules.SimplifyArithmeticComparisonRule;
import org.apache.doris.nereids.rules.expression.rules.SimplifyArithmeticRule;
import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SimplifyArithmeticRuleTest extends ExpressionRewriteTestHelper {
    @Test
    void testSimplifyArithmetic() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(SimplifyArithmeticRule.INSTANCE, FoldConstantRule.INSTANCE)
        ));
        assertRewriteAfterSimplify("IA", "IA");
        assertRewriteAfterSimplify("IA + 1", "IA + 1");
        assertRewriteAfterSimplify("IA + IB", "IA + IB");
        assertRewriteAfterSimplify("1 * 3 / IA", "(3 / IA)");
        assertRewriteAfterSimplify("1 - IA", "1 - IA");
        assertRewriteAfterSimplify("IA + 2 - ((1 - IB) - (3 + IC))", "(((IA + IB) + IC) + 4)");
        assertRewriteAfterSimplify("IA * IB + 2 - IC * 2", "(IA * IB) - (IC * 2) + 2");
        assertRewriteAfterSimplify("IA * IB", "IA * IB");

        assertRewriteAfterSimplify("IA * IB / 2 * 2", "((IA * IB) / (2 / 2))");
        assertRewriteAfterSimplify("IA * IB / (2 * 2)", "((IA * IB) / 4)");
        assertRewriteAfterSimplify("IA * IB / (2 * 2)", "((IA * IB) / 4)");
        assertRewriteAfterSimplify("IA * (IB / 2) * 2", "((IA * IB) / (2 / 2))");
        assertRewriteAfterSimplify("IA * (IB / 2) * (IC + 1)", "(((IA * IB) * (IC + 1)) / 2)");
        assertRewriteAfterSimplify("IA * IB / 2 / IC * 2 * ID / 4", "((((IA * IB) / IC) * ID) / ((2 / 2) * 4))");
        assertRewriteAfterSimplify("IA - 10 + (IB * 2 * 3) + 20", "IA + (IB * 6) - (-10)");
        assertRewriteAfterSimplify("IA / 10 * (IB - 2 + 3) * 20", "((IA * (IB - -1)) / (10 / 20))");
        assertRewriteAfterSimplify("1 + ((IA * 2 * 3) * 10 / 10)", "((IA * (60 / 10)) + 1)");
        assertRewriteAfterSimplify("1 + (IA * 2 * 20 / (IB + 5 + (IC * 10 * 20 / 50 + 5 + 6) + 20) / 20) * (ID * 5 * 6 / (IE + 20 + 30)) + 200",
                "(((((IA / ((IB + (IC * (-56 / 50))) + 36)) * ID) / (IE + 50)) * (((40 / 20) * 5) * 6)) + (1 + 200))");
    }

    @Test
    void testSimplifyArithmeticRuleOnly() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(SimplifyArithmeticRule.INSTANCE)
        ));

        // add and subtract
        assertRewriteAfterSimplify("-IA - ((1 + IB) - (3 - IC))", "(((((0 - 1) + 3) - IA) - IB) - IC)");
        assertRewriteAfterSimplify("-2 - IA - ((1 - IB) - (3 + IC))", "(((((-2 - 1) + 3) - IA) + IB) + IC)");
        assertRewriteAfterSimplify("-IA - 2 - ((IB + 1) - (3 - (IC - 4)))", "(((((((0 - 2) - 1) + 3) + 4) - IA) - IB) - IC)");
        assertRewriteAfterSimplify("-IA - 2 - ((IB - 1) - (3 - (IC + 4)))", "(((((((0 - 2) + 1) + 3) - 4) - IA) - IB) - IC)");
        assertRewriteAfterSimplify("-IA - 2 - ((-IB - 1) - (3 + (IC + 4)))", "((((((((0 - 2) - 0) + 1) + 3) + 4) - IA) + IB) + IC)");
        assertRewriteAfterSimplify("IA - 2 - ((-IB - 1) - (3 + (IC + 4)))", "(((IA + IB) + IC) - ((((2 + 0) - 1) - 3) - 4))");

        // multiply and divide
        assertRewriteAfterSimplify("2 / IA / ((1 / IB) / (3 * IC))", "(((((2 / 1) * 3) / IA) * IB) * IC)");
        assertRewriteAfterSimplify("IA / 2 / ((IB * 1) / (3 / (IC / 4)))", "(((IA / IB) / IC) / (((2 * 1) / 3) / 4))");
        assertRewriteAfterSimplify("IA / 2 / ((IB / 1) / (3 / (IC * 4)))", "(((IA / IB) / IC) / (((2 / 1) / 3) * 4))");
        assertRewriteAfterSimplify("IA / 2 / ((IB / 1) / (3 * (IC * 4)))", "(((IA / IB) * IC) / (((2 / 1) / 3) / 4))");

        // hybrid
        // root is subtract
        assertRewriteAfterSimplify("-2 - IA * ((1 - IB) - (3 / IC))", "(-2 - (IA * ((1 - IB) - (3 / IC))))");
        assertRewriteAfterSimplify("-IA - 2 - ((IB * 1) - (3 * (IC / 4)))", "((((0 - 2) - IA) - (IB * 1)) + (IC * (3 / 4)))");
        // root is add
        assertRewriteAfterSimplify("-IA * 2 + ((IB - 1) / (3 - (IC + 4)))", "(((0 - IA) * 2) + ((IB - 1) / ((3 - 4) - IC)))");
        assertRewriteAfterSimplify("-IA + 2 + ((IB - 1) - (3 * (IC + 4)))", "(((((0 + 2) - 1) - IA) + IB) - ((IC + 4) * 3))");
        // root is multiply
        assertRewriteAfterSimplify("-IA / 2 * ((-IB - 1) - (3 + (IC + 4)))", "(((0 - IA) * (((((0 - 1) - 3) - 4) - IB) - IC)) / 2)");
        assertRewriteAfterSimplify("-IA / 2 * ((-IB - 1) * (3 / (IC + 4)))", "((((0 - IA) * ((0 - 1) - IB)) / (IC + 4)) / (2 / 3))");
        // root is divide
        assertRewriteAfterSimplify("(-IA / 2) / ((-IB - 1) - (3 + (IC + 4)))", "(((0 - IA) / (((((0 - 1) - 3) - 4) - IB) - IC)) / 2)");
        assertRewriteAfterSimplify("(-IA / 2) / ((-IB - 1) / (3 + (IC * 4)))", "((((0 - IA) / ((0 - 1) - IB)) * ((IC * 4) + 3)) / 2)");

        // unsupported decimal
        assertRewriteAfterSimplify("-2 - MA - ((1 - IB) - (3 + IC))", "((-2 - MA) - ((1 - IB) - (3 + IC)))");
        assertRewriteAfterSimplify("-IA / 2.0 * ((-IB - 1) - (3 + (IC + 4)))", "(((0 - IA) / 2.0) * (((0 - IB) - 1) - (3 + (IC + 4))))");
    }

    @Test
    void testSimplifyArithmeticComparison() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(
                    SimplifyArithmeticRule.INSTANCE,
                    FoldConstantRule.INSTANCE,
                    SimplifyArithmeticComparisonRule.INSTANCE,
                    SimplifyArithmeticRule.INSTANCE
                ),
                bottomUp(
                    FoldConstantRule.INSTANCE
                )
        ));
        assertRewriteAfterSimplify("IA", "IA");
        assertRewriteAfterSimplify("IA > IB", "IA > IB");
        assertRewriteAfterSimplify("IA - 1 > 1", "(IA > 2)");
        assertRewriteAfterSimplify("IA + 1 - 1 >= 1", "(IA >= 1)");
        assertRewriteAfterSimplify("IA - 1 - 1 < 1", "(IA < 3)");
        assertRewriteAfterSimplify("IA - 1 - 1 = 1", "(IA = 3)");
        assertRewriteAfterSimplify("IA + (1 - 1) >= 1", "(IA >= 1)");
        assertRewriteAfterSimplify("IA - 1 * 1 = 1", "(IA = 2)");
        assertRewriteAfterSimplify("IA + 1 + 1 > IB", "(IA > (IB - 2))");
        assertRewriteAfterSimplify("IA + 1 > IB", "(IA > (IB - 1))");
        assertRewriteAfterSimplify("IA + 1 - (IB - 1) > 1", "IA - IB > -1");
        assertRewriteAfterSimplify("IA + 1 - (IB * IC - 1) > 1", "IA - IB * IC > -1");
        assertRewriteAfterSimplify("IA + 1 - (IB * IC - 1) > 1", "IA - IB * IC > -1");
        assertRewriteAfterSimplify("IA + 1 - (IB * IC - 1) > 1", "((IA - (IB * IC)) > -1)");
        assertRewriteAfterSimplify("(IA - 1) + (IB + 1) - (IC - 1) > 1", "(((IA + IB) - IC) > 0)");
        assertRewriteAfterSimplify("(IA - 1) + (IB + 1) - ((IC - 1) - (ID + 1)) > 1", "((((IA + IB) - IC) + ID) > -1)");
        assertRewriteAfterSimplify("(IA - 1) + (IB + 1) - ((IC - 1) - (ID * IE + 1)) > 1", "((((IA + IB) - IC) + (ID * IE)) > -1)");
        assertRewriteAfterSimplify("IA + 1 - (IB - 1) > IC + 1 - 1 + ID", "((IA - IB) > ((IC + ID) + -2))");
        assertRewriteAfterSimplify("IA + 1 - (IB - 1) > IC - 1", "((IA - IB) > (IC - 3))");
        assertRewriteAfterSimplify("IA + 1 > IB - 1", "(IA > (IB - 2))");
        assertRewriteAfterSimplify("IA > IB - 1", "IA > IB - 1");
        assertRewriteAfterSimplify("IA + 1 > IB", "IA > (IB - 1)");
        assertRewriteAfterSimplify("IA + 1 > IB * IC", "IA > ((IB * IC) - 1)");
        assertRewriteAfterSimplify("IA * ID > IB * IC", "IA * ID > IB * IC");
        assertRewriteAfterSimplify("IA * ID / 2 > IB * IC", "((IA * ID) > ((IB * IC) * 2))");
        assertRewriteAfterSimplify("IA * ID / -2 > IB * IC", "(((IB * IC) * -2) > (IA * ID))");
        assertRewriteAfterSimplify("1 - IA > 1", "(IA < 0)");
        assertRewriteAfterSimplify("1 - IA + 1 * 3 - 5 > 1", "(IA < -2)");
    }

    @Test
    void testSimplifyDateTimeComparison() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(
                    SimplifyArithmeticRule.INSTANCE,
                    FoldConstantRule.INSTANCE,
                    SimplifyArithmeticComparisonRule.INSTANCE,
                    SimplifyArithmeticRule.INSTANCE
                ),
                ExpressionAnalyzer.FUNCTION_ANALYZER_RULE,
                bottomUp(
                    FoldConstantRule.INSTANCE
                )
        ));
        assertRewriteAfterTypeCoercion("years_add(IA, 1) > '2021-01-01 00:00:00'", "(years_add(cast(IA as DATETIMEV2(0)), 1) > '2021-01-01 00:00:00')");
        assertRewriteAfterTypeCoercion("years_sub(IA, 1) > '2021-01-01 00:00:00'", "(years_sub(cast(IA as DATETIMEV2(0)), 1) > '2021-01-01 00:00:00')");
        assertRewriteAfterTypeCoercion("months_add(IA, 1) > '2021-01-01 00:00:00'", "(months_add(cast(IA as DATETIMEV2(0)), 1) > '2021-01-01 00:00:00')");
        assertRewriteAfterTypeCoercion("months_sub(IA, 1) > '2021-01-01 00:00:00'", "(months_sub(cast(IA as DATETIMEV2(0)), 1) > '2021-01-01 00:00:00')");
        assertRewriteAfterTypeCoercion("weeks_add(IA, 1) > '2021-01-01 00:00:00'", "(cast(IA as DATETIMEV2(0)) > '2020-12-25 00:00:00')");
        assertRewriteAfterTypeCoercion("weeks_sub(IA, 1) > '2021-01-01 00:00:00'", "(cast(IA as DATETIMEV2(0)) > '2021-01-08 00:00:00')");
        assertRewriteAfterTypeCoercion("days_add(IA, 1) > '2021-01-01 00:00:00'", "(cast(IA as DATETIMEV2(0)) > '2020-12-31 00:00:00')");
        assertRewriteAfterTypeCoercion("days_sub(IA, 1) > '2021-01-01 00:00:00'", "(cast(IA as DATETIMEV2(0)) > '2021-01-02 00:00:00')");
        assertRewriteAfterTypeCoercion("hours_add(IA, 1) > '2021-01-01 00:00:00'", "(cast(IA as DATETIMEV2(0)) > '2020-12-31 23:00:00')");
        assertRewriteAfterTypeCoercion("hours_sub(IA, 1) > '2021-01-01 00:00:00'", "(cast(IA as DATETIMEV2(0)) > '2021-01-01 01:00:00')");
        assertRewriteAfterTypeCoercion("minutes_add(IA, 1) > '2021-01-01 00:00:00'", "(cast(IA as DATETIMEV2(0)) > '2020-12-31 23:59:00')");
        assertRewriteAfterTypeCoercion("minutes_sub(IA, 1) > '2021-01-01 00:00:00'", "(cast(IA as DATETIMEV2(0)) > '2021-01-01 00:01:00')");
        assertRewriteAfterTypeCoercion("seconds_add(IA, 1) > '2021-01-01 00:00:00'", "(cast(IA as DATETIMEV2(0)) > '2020-12-31 23:59:59')");
        assertRewriteAfterTypeCoercion("seconds_sub(IA, 1) > '2021-01-01 00:00:00'", "(cast(IA as DATETIMEV2(0)) > '2021-01-01 00:00:01')");

    }

    private void assertRewriteAfterSimplify(String expr, String expected) {
        Expression needRewriteExpression = PARSER.parseExpression(expr);
        needRewriteExpression = replaceUnboundSlot(needRewriteExpression, Maps.newHashMap());
        Expression rewritten = executor.rewrite(needRewriteExpression, context);
        Expression expectedExpression = PARSER.parseExpression(expected);
        Assertions.assertEquals(expectedExpression.toSql(), rewritten.toSql());
    }
}
