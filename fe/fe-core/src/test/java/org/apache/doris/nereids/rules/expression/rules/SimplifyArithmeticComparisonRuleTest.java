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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SimplifyArithmeticComparisonRuleTest extends ExpressionRewriteTestHelper {

    @Test
    public void testNumeric() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                ExpressionRewrite.bottomUp(SimplifyArithmeticComparisonRule.INSTANCE)
        ));

        // test tinyint type
        assertRewriteAfterSimplify("TA + 2 > 1", "(TA > (1 - 2))");
        assertRewriteAfterSimplify("TA - 2 > 1", "(TA > (1 + 2))");
        assertRewriteAfterSimplify("1 + TA > 2", "TA > (2 - 1)");
        assertRewriteAfterSimplify("-1 + TA > 2", "TA > (2 - (-1))");
        assertRewriteAfterSimplify("1 - TA > 2", "TA < (1 - 2)");
        assertRewriteAfterSimplify("-1 - TA > 2", "TA < ((-1) - 2)");
        assertRewriteAfterSimplify("2 * TA > 1", "((2 * TA) > 1)");
        assertRewriteAfterSimplify("-2 * TA > 1", "((-2 * TA) > 1)");
        assertRewriteAfterSimplify("2 / TA > 1", "((2 / TA) > 1)");
        assertRewriteAfterSimplify("-2 / TA > 1", "((-2 / TA) > 1)");
        assertRewriteAfterSimplify("TA * 2 > 1", "((TA * 2) > 1)");
        assertRewriteAfterSimplify("TA * (-2) > 1", "((TA * (-2)) > 1)");
        assertRewriteAfterSimplify("TA / 2 > 1", "TA > (1 * 2)");
        assertRewriteAfterSimplify("TA / -2 > 1", "(1 * -2) > TA");

        // test integer type
        assertRewriteAfterSimplify("IA + 2 > 1", "IA > cast((1 - 2) as INT)");
        assertRewriteAfterSimplify("IA - 2 > 1", "IA > cast((1 + 2) as INT)");
        assertRewriteAfterSimplify("1 + IA > 2", "IA > cast((2 - 1) as INT)");
        assertRewriteAfterSimplify("-1 + IA > 2", "IA > cast((2 - (-1)) as INT)");
        assertRewriteAfterSimplify("1 - IA > 2", "IA < cast((1 - 2) as INT)");
        assertRewriteAfterSimplify("-1 - IA > 2", "IA < cast(((-1) - 2) as INT)");
        assertRewriteAfterSimplify("2 * IA > 1", "((2 * IA) > 1)");
        assertRewriteAfterSimplify("-2 * IA > 1", "((-2 * IA) > 1)");
        assertRewriteAfterSimplify("2 / IA > 1", "((2 / IA) > 1)");
        assertRewriteAfterSimplify("-2 / IA > 1", "((-2 / IA) > 1)");
        assertRewriteAfterSimplify("IA * 2 > 1", "((IA * 2) > 1)");
        assertRewriteAfterSimplify("IA * (-2) > 1", "((IA * (-2)) > 1)");
        assertRewriteAfterSimplify("IA / 2 > 1", "(IA > cast((1 * 2) as INT))");
        assertRewriteAfterSimplify("IA / -2 > 1", "cast((1 * -2) as INT) > IA");

        // test integer type
        assertRewriteAfterSimplify("TA + 2 > 200", "cast(TA as SMALLINT) > (200 - 2)");
        assertRewriteAfterSimplify("TA - 2 > 200", "cast(TA as SMALLINT) > (200 + 2)");
        assertRewriteAfterSimplify("1 + TA > 200", "cast(TA as SMALLINT) > (200 - 1)");
        assertRewriteAfterSimplify("-1 + TA > 200", "cast(TA as SMALLINT) > (200 - (-1))");
        assertRewriteAfterSimplify("1 - TA > 200", "cast(TA as SMALLINT) < (1 - 200)");
        assertRewriteAfterSimplify("-1 - TA > 200", "cast(TA as SMALLINT) < ((-1) - 200)");
        assertRewriteAfterSimplify("2 * TA > 200", "((2 * TA) > 200)");
        assertRewriteAfterSimplify("-2 * TA > 200", "((-2 * TA) > 200)");
        assertRewriteAfterSimplify("2 / TA > 200", "((2 / TA) > 200)");
        assertRewriteAfterSimplify("-2 / TA > 200", "((-2 / TA) > 200)");
        assertRewriteAfterSimplify("TA * 2 > 200", "((TA * 2) > 200)");
        assertRewriteAfterSimplify("TA * (-2) > 200", "((TA * (-2)) > 200)");
        assertRewriteAfterSimplify("TA / 2 > 200", "cast(TA as SMALLINT) > (200 * 2)");
        assertRewriteAfterSimplify("TA / -2 > 200", "(200 * -2) > cast(TA as SMALLINT)");

        // test decimal type
        assertRewriteAfterSimplify("1.1 + IA > 2.22", "(cast(IA as DECIMALV3(12, 2)) > cast((2.22 - 1.1) as DECIMALV3(12, 2)))");
        assertRewriteAfterSimplify("-1.1 + IA > 2.22", "(cast(IA as DECIMALV3(12, 2)) > cast((2.22 - (-1.1)) as DECIMALV3(12, 2)))");
        assertRewriteAfterSimplify("1.1 - IA > 2.22", "(cast(IA as DECIMALV3(11, 1)) < cast((1.1 - 2.22) as DECIMALV3(11, 1)))");
        assertRewriteAfterSimplify("-1.1 - IA > 2.22", "(cast(IA as DECIMALV3(11, 1)) < cast((-1.1 - 2.22) as DECIMALV3(11, 1)))");
        assertRewriteAfterSimplify("2.22 * IA > 1.1", "((2.22 * IA) > 1.1)");
        assertRewriteAfterSimplify("-2.22 * IA > 1.1", "-2.22 * IA > 1.1");
        assertRewriteAfterSimplify("2.22 / IA > 1.1", "((2.22 / IA) > 1.1)");
        assertRewriteAfterSimplify("-2.22 / IA > 1.1", "((-2.22 / IA) > 1.1)");
        assertRewriteAfterSimplify("IA * 2.22 > 1.1", "IA * 2.22 > 1.1");
        assertRewriteAfterSimplify("IA * (-2.22) > 1.1", "IA * (-2.22) > 1.1");
        assertRewriteAfterSimplify("IA / 2.22 > 1.1", "(cast(IA as DECIMALV3(13, 3)) > cast((1.1 * 2.22) as DECIMALV3(13, 3)))");
        assertRewriteAfterSimplify("IA / (-2.22) > 1.1", "(cast((1.1 * -2.22) as DECIMALV3(13, 3)) > cast(IA as DECIMALV3(13, 3)))");

        // test (1 + IA) can be processed
        assertRewriteAfterSimplify("2 - (1 + IA) > 3", "(IA < cast(((2 - 3) - 1) as INT))");
        assertRewriteAfterSimplify("(1 - IA) / 2 > 3", "(IA < cast((1 - 6) as INT))");
        assertRewriteAfterSimplify("1 - IA / 2 > 3", "(IA < cast(((1 - 3) * 2) as INT))");
        assertRewriteAfterSimplify("(1 - (IA + 4)) / 2 > 3", "(IA < cast(((1 - 6) - 4) as INT))");
        assertRewriteAfterSimplify("2 * (1 + IA) > 1", "(2 * (1 + IA)) > 1");

        // test (IA + IB) can be processed
        assertRewriteAfterSimplify("2 - (1 + (IA + IB)) > 3", "(IA + IB) < cast(((2 - 3) - 1) as INT)");
        assertRewriteAfterSimplify("(1 - (IA + IB)) / 2 > 3", "(IA + IB) < cast((1 - 6) as INT)");
        assertRewriteAfterSimplify("1 - (IA + IB) / 2 > 3", "(IA + IB) < cast(((1 - 3) * 2) as INT)");
        assertRewriteAfterSimplify("2 * (1 + (IA + IB)) > 1", "(2 * (1 + (IA + IB))) > 1");
    }

    @Test
    public void testDateLike() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(
                        SimplifyArithmeticRule.INSTANCE,
                        SimplifyArithmeticComparisonRule.INSTANCE,
                        FoldConstantRuleOnFE.VISITOR_INSTANCE
                )
        ));

        // test datetimev2 type
        assertRewriteAfterTypeCoercion("years_add(AA, 1) > '2021-01-01 00:00:00'", "(years_add(AA, 1) > '2021-01-01 00:00:00')");
        assertRewriteAfterTypeCoercion("years_sub(AA, 1) > '2021-01-01 00:00:00'", "(years_sub(AA, 1) > '2021-01-01 00:00:00')");
        assertRewriteAfterTypeCoercion("months_add(AA, 1) > '2021-01-01 00:00:00'", "(months_add(AA, 1) > '2021-01-01 00:00:00')");
        assertRewriteAfterTypeCoercion("months_sub(AA, 1) > '2021-01-01 00:00:00'", "(months_sub(AA, 1) > '2021-01-01 00:00:00')");
        assertRewriteAfterTypeCoercion("weeks_add(AA, 1) > '2021-01-01 00:00:00'", "(AA > '2020-12-25 00:00:00')");
        assertRewriteAfterTypeCoercion("weeks_sub(AA, 1) > '2021-01-01 00:00:00'", "(AA > '2021-01-08 00:00:00')");
        assertRewriteAfterTypeCoercion("days_add(AA, 1) > '2021-01-01 00:00:00'", "(AA > '2020-12-31 00:00:00')");
        assertRewriteAfterTypeCoercion("days_sub(AA, 1) > '2021-01-01 00:00:00'", "(AA > '2021-01-02 00:00:00')");
        assertRewriteAfterTypeCoercion("hours_add(AA, 1) > '2021-01-01 00:00:00'", "(AA > '2020-12-31 23:00:00')");
        assertRewriteAfterTypeCoercion("hours_sub(AA, 1) > '2021-01-01 00:00:00'", "(AA > '2021-01-01 01:00:00')");
        assertRewriteAfterTypeCoercion("minutes_add(AA, 1) > '2021-01-01 00:00:00'", "(AA > '2020-12-31 23:59:00')");
        assertRewriteAfterTypeCoercion("minutes_sub(AA, 1) > '2021-01-01 00:00:00'", "(AA > '2021-01-01 00:01:00')");
        assertRewriteAfterTypeCoercion("seconds_add(AA, 1) > '2021-01-01 00:00:00'", "(AA > '2020-12-31 23:59:59')");
        assertRewriteAfterTypeCoercion("seconds_sub(AA, 1) > '2021-01-01 00:00:00'", "(AA > '2021-01-01 00:00:01')");

        assertRewriteAfterTypeCoercion("years_add(AA, 1) > '2021-01-01'", "(years_add(AA, 1) > '2021-01-01 00:00:00')");
        assertRewriteAfterTypeCoercion("years_sub(AA, 1) > '2021-01-01'", "(years_sub(AA, 1) > '2021-01-01 00:00:00')");
        assertRewriteAfterTypeCoercion("months_add(AA, 1) > '2021-01-01'", "(months_add(AA, 1) > '2021-01-01 00:00:00')");
        assertRewriteAfterTypeCoercion("months_sub(AA, 1) > '2021-01-01'", "(months_sub(AA, 1) > '2021-01-01 00:00:00')");
        assertRewriteAfterTypeCoercion("weeks_add(AA, 1) > '2021-01-01'", "(AA > '2020-12-25 00:00:00')");
        assertRewriteAfterTypeCoercion("weeks_sub(AA, 1) > '2021-01-01'", "(AA > '2021-01-08 00:00:00')");
        assertRewriteAfterTypeCoercion("days_add(AA, 1) > '2021-01-01'", "(AA > '2020-12-31 00:00:00')");
        assertRewriteAfterTypeCoercion("days_sub(AA, 1) > '2021-01-01'", "(AA > '2021-01-02 00:00:00')");
        assertRewriteAfterTypeCoercion("hours_add(AA, 1) > '2021-01-01'", "(AA > '2020-12-31 23:00:00')");
        assertRewriteAfterTypeCoercion("hours_sub(AA, 1) > '2021-01-01'", "(AA > '2021-01-01 01:00:00')");
        assertRewriteAfterTypeCoercion("minutes_add(AA, 1) > '2021-01-01'", "(AA > '2020-12-31 23:59:00')");
        assertRewriteAfterTypeCoercion("minutes_sub(AA, 1) > '2021-01-01'", "(AA > '2021-01-01 00:01:00')");
        assertRewriteAfterTypeCoercion("seconds_add(AA, 1) > '2021-01-01'", "(AA > '2020-12-31 23:59:59')");
        assertRewriteAfterTypeCoercion("seconds_sub(AA, 1) > '2021-01-01'", "(AA > '2021-01-01 00:00:01')");

        // test date type
        assertRewriteAfterTypeCoercion("years_add(CA, 1) > '2021-01-01'", "(years_add(CA, 1) > date '2021-01-01')");
        assertRewriteAfterTypeCoercion("years_sub(CA, 1) > '2021-01-01'", "(years_sub(CA, 1) > date '2021-01-01')");
        assertRewriteAfterTypeCoercion("months_add(CA, 1) > '2021-01-01'", "(months_add(CA, 1) > date '2021-01-01')");
        assertRewriteAfterTypeCoercion("months_sub(CA, 1) > '2021-01-01'", "(months_sub(CA, 1) > date '2021-01-01')");
        assertRewriteAfterTypeCoercion("weeks_add(CA, 1) > '2021-01-01'", "(CA > date '2020-12-25')");
        assertRewriteAfterTypeCoercion("weeks_sub(CA, 1) > '2021-01-01'", "(CA > date '2021-01-08')");
        assertRewriteAfterTypeCoercion("days_add(CA, 1) > '2021-01-01'", "(CA > date '2020-12-31')");
        assertRewriteAfterTypeCoercion("days_sub(CA, 1) > '2021-01-01'", "(CA > date '2021-01-02')");
        assertRewriteAfterTypeCoercion("hours_add(CA, 1) > '2021-01-01'", "(cast(CA as DATETIMEV2(0)) > '2020-12-31 23:00:00')");
        assertRewriteAfterTypeCoercion("hours_sub(CA, 1) > '2021-01-01'", "(cast(CA as DATETIMEV2(0)) > '2021-01-01 01:00:00')");
        assertRewriteAfterTypeCoercion("minutes_add(CA, 1) > '2021-01-01'", "(cast(CA as DATETIMEV2(0)) > '2020-12-31 23:59:00')");
        assertRewriteAfterTypeCoercion("minutes_sub(CA, 1) > '2021-01-01'", "(cast(CA as DATETIMEV2(0)) > '2021-01-01 00:01:00')");
        assertRewriteAfterTypeCoercion("seconds_add(CA, 1) > '2021-01-01'", "(cast(CA as DATETIMEV2(0)) > '2020-12-31 23:59:59')");
        assertRewriteAfterTypeCoercion("seconds_sub(CA, 1) > '2021-01-01'", "(cast(CA as DATETIMEV2(0)) > '2021-01-01 00:00:01')");

        assertRewriteAfterTypeCoercion("years_add(CA, 1) > '2021-01-01 00:00:00'", "(years_add(CA, 1) > date '2021-01-01')");
        assertRewriteAfterTypeCoercion("years_sub(CA, 1) > '2021-01-01 00:00:00'", "(years_sub(CA, 1) > date '2021-01-01')");
        assertRewriteAfterTypeCoercion("months_add(CA, 1) > '2021-01-01 00:00:00'", "(months_add(CA, 1) > date '2021-01-01')");
        assertRewriteAfterTypeCoercion("months_sub(CA, 1) > '2021-01-01 00:00:00'", "(months_sub(CA, 1) > date '2021-01-01')");
        assertRewriteAfterTypeCoercion("weeks_add(CA, 1) > '2021-01-01 00:00:00'", "(CA > date '2020-12-25')");
        assertRewriteAfterTypeCoercion("weeks_sub(CA, 1) > '2021-01-01 00:00:00'", "(CA > date '2021-01-08')");
        assertRewriteAfterTypeCoercion("days_add(CA, 1) > '2021-01-01 00:00:00'", "(CA > date '2020-12-31')");
        assertRewriteAfterTypeCoercion("days_sub(CA, 1) > '2021-01-01 00:00:00'", "(CA > date '2021-01-02')");
        assertRewriteAfterTypeCoercion("hours_add(CA, 1) > '2021-01-01 00:00:00'", "(cast(CA as DATETIMEV2(0)) > '2020-12-31 23:00:00')");
        assertRewriteAfterTypeCoercion("hours_sub(CA, 1) > '2021-01-01 00:00:00'", "(cast(CA as DATETIMEV2(0)) > '2021-01-01 01:00:00')");
        assertRewriteAfterTypeCoercion("minutes_add(CA, 1) > '2021-01-01 00:00:00'", "(cast(CA as DATETIMEV2(0)) > '2020-12-31 23:59:00')");
        assertRewriteAfterTypeCoercion("minutes_sub(CA, 1) > '2021-01-01 00:00:00'", "(cast(CA as DATETIMEV2(0)) > '2021-01-01 00:01:00')");
        assertRewriteAfterTypeCoercion("seconds_add(CA, 1) > '2021-01-01 00:00:00'", "(cast(CA as DATETIMEV2(0)) > '2020-12-31 23:59:59')");
        assertRewriteAfterTypeCoercion("seconds_sub(CA, 1) > '2021-01-01 00:00:00'", "(cast(CA as DATETIMEV2(0)) > '2021-01-01 00:00:01')");
    }

    private void assertRewriteAfterSimplify(String expr, String expected) {
        Expression needRewriteExpression = PARSER.parseExpression(expr);
        needRewriteExpression = replaceUnboundSlot(needRewriteExpression, Maps.newHashMap());
        Expression rewritten = executor.rewrite(needRewriteExpression, context);
        Expression expectedExpression = PARSER.parseExpression(expected);
        Assertions.assertEquals(expectedExpression.toSql(), rewritten.toSql());
    }
}
