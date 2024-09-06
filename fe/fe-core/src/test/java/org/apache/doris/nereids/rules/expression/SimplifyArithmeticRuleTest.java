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

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

class SimplifyArithmeticRuleTest extends ExpressionRewriteTestHelper {
    @Test
    void testSimplifyArithmetic() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(SimplifyArithmeticRule.INSTANCE),
                ExpressionAnalyzer.FUNCTION_ANALYZER_RULE,
                bottomUp(
                    FoldConstantRule.INSTANCE
                )
        ));
        assertRewriteAfterTypeCoercion("IA", "IA");
        assertRewriteAfterTypeCoercion("IA + 1", "IA + 1");
        assertRewriteAfterTypeCoercion("IA + IB", "IA + IB");
        assertRewriteAfterTypeCoercion("1 * 3 / IA", "(3.0 / cast(IA as DOUBLE))");
        assertRewriteAfterTypeCoercion("1 - IA", "1 - IA");
        assertRewriteAfterTypeCoercion("1 + 1", "2");
        assertRewriteAfterTypeCoercion("IA + 2 - 1", "IA + 1");
        assertRewriteAfterTypeCoercion("IA + 2 - (1 - 1)", "IA + 2");
        assertRewriteAfterTypeCoercion("IA + 2 - ((1 - IB) - (3 + IC))", "IA + IB + IC + 4");
        assertRewriteAfterTypeCoercion("IA * IB + 2 - IC * 2", "(IA * IB) - (IC * 2) + 2");
        assertRewriteAfterTypeCoercion("IA * IB", "IA * IB");
        assertRewriteAfterTypeCoercion("IA * IB / 2 * 2", "cast((IA * IB) as DOUBLE) / 1.0");
        assertRewriteAfterTypeCoercion("IA * IB / (2 * 2)", "cast((IA * IB) as DOUBLE) / 4.0");
        assertRewriteAfterTypeCoercion("IA * IB / (2 * 2)", "cast((IA * IB) as DOUBLE) / 4.0");
        assertRewriteAfterTypeCoercion("IA * (IB / 2) * 2)", "cast(IA as DOUBLE) * cast(IB as DOUBLE) / 1.0");
        assertRewriteAfterTypeCoercion("IA * (IB / 2) * (IC + 1))", "cast(IA as DOUBLE) * cast(IB as DOUBLE) * cast((IC + 1) as DOUBLE) / 2.0");
        assertRewriteAfterTypeCoercion("IA * IB / 2 / IC * 2 * ID / 4", "(((cast((IA * IB) as DOUBLE) / cast(IC as DOUBLE)) * cast(ID as DOUBLE)) / 4.0)");
    }

    @Test
    void testSimplifyArithmeticRuleOnly() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(SimplifyArithmeticRule.INSTANCE)
        ));

        // add and subtract
        assertRewriteAfterTypeCoercion("-IA - ((1 + IB) - (3 - IC))", "(((((0 - 1) + 3) - IA) - IB) - IC)");
        assertRewriteAfterTypeCoercion("-2 - IA - ((1 - IB) - (3 + IC))", "(((((-2 - 1) + 3) - IA) + IB) + IC)");
        assertRewriteAfterTypeCoercion("-IA - 2 - ((IB + 1) - (3 - (IC - 4)))", "(((((((0 - 2) - 1) + 3) + 4) - IA) - IB) - IC)");
        assertRewriteAfterTypeCoercion("-IA - 2 - ((IB - 1) - (3 - (IC + 4)))", "(((((((0 - 2) + 1) + 3) - 4) - IA) - IB) - IC)");
        assertRewriteAfterTypeCoercion("-IA - 2 - ((-IB - 1) - (3 + (IC + 4)))", "((((((((0 - 2) - 0) + 1) + 3) + 4) - IA) + IB) + IC)");
        assertRewriteAfterTypeCoercion("IA - 2 - ((-IB - 1) - (3 + (IC + 4)))", "(((IA + IB) + IC) - ((((2 + 0) - 1) - 3) - 4))");

        // multiply and divide
        assertRewriteAfterTypeCoercion("2 / IA / ((1 / IB) / (3 * IC))", "((((cast(2 as DOUBLE) / cast(1 as DOUBLE)) / cast(IA as DOUBLE)) * cast(IB as DOUBLE)) * cast((IC * 3) as DOUBLE))");
        assertRewriteAfterTypeCoercion("IA / 2 / ((IB * 1) / (3 / (IC / 4)))", "(((cast(IA as DOUBLE) / cast((IB * 1) as DOUBLE)) / cast(IC as DOUBLE)) / ((cast(2 as DOUBLE) / cast(3 as DOUBLE)) / cast(4 as DOUBLE)))");
        assertRewriteAfterTypeCoercion("IA / 2 / ((IB / 1) / (3 / (IC * 4)))", "(((cast(IA as DOUBLE) / cast(IB as DOUBLE)) / cast((IC * 4) as DOUBLE)) / ((cast(2 as DOUBLE) / cast(1 as DOUBLE)) / cast(3 as DOUBLE)))");
        assertRewriteAfterTypeCoercion("IA / 2 / ((IB / 1) / (3 * (IC * 4)))", "(((cast(IA as DOUBLE) / cast(IB as DOUBLE)) * cast((IC * (3 * 4)) as DOUBLE)) / (cast(2 as DOUBLE) / cast(1 as DOUBLE)))");

        // hybrid
        // root is subtract
        assertRewriteAfterTypeCoercion("-2 - IA * ((1 - IB) - (3 / IC))", "(cast(-2 as DOUBLE) - (cast(IA as DOUBLE) * (cast((1 - IB) as DOUBLE) - (cast(3 as DOUBLE) / cast(IC as DOUBLE)))))");
        assertRewriteAfterTypeCoercion("-IA - 2 - ((IB * 1) - (3 * (IC / 4)))", "((cast(((0 - 2) - IA) as DOUBLE) - cast((IB * 1) as DOUBLE)) + (cast(3 as DOUBLE) * (cast(IC as DOUBLE) / cast(4 as DOUBLE))))");
        // root is add
        assertRewriteAfterTypeCoercion("-IA * 2 + ((IB - 1) / (3 - (IC + 4)))", "(cast(((0 - IA) * 2) as DOUBLE) + (cast((IB - 1) as DOUBLE) / cast(((3 - 4) - IC) as DOUBLE)))");
        assertRewriteAfterTypeCoercion("-IA + 2 + ((IB - 1) - (3 * (IC + 4)))", "(((((0 + 2) - 1) - IA) + IB) - (3 * (IC + 4)))");
        // root is multiply
        assertRewriteAfterTypeCoercion("-IA / 2 * ((-IB - 1) - (3 + (IC + 4)))", "((cast((0 - IA) as DOUBLE) * cast((((((0 - 1) - 3) - 4) - IB) - IC) as DOUBLE)) / cast(2 as DOUBLE))");
        assertRewriteAfterTypeCoercion("-IA / 2 * ((-IB - 1) * (3 / (IC + 4)))", "(((cast((0 - IA) as DOUBLE) * cast(((0 - 1) - IB) as DOUBLE)) / cast((IC + 4) as DOUBLE)) / (cast(2 as DOUBLE) / cast(3 as DOUBLE)))");
        // root is divide
        assertRewriteAfterTypeCoercion("(-IA / 2) / ((-IB - 1) - (3 + (IC + 4)))", "((cast((0 - IA) as DOUBLE) / cast((((((0 - 1) - 3) - 4) - IB) - IC) as DOUBLE)) / cast(2 as DOUBLE))");
        assertRewriteAfterTypeCoercion("(-IA / 2) / ((-IB - 1) / (3 + (IC * 4)))", "(((cast((0 - IA) as DOUBLE) / cast(((0 - 1) - IB) as DOUBLE)) * cast(((IC * 4) + 3) as DOUBLE)) / cast(2 as DOUBLE))");

        // unsupported decimal
        assertRewriteAfterTypeCoercion("-2 - MA - ((1 - IB) - (3 + IC))", "((cast(-2 as DECIMALV3(38, 9)) - MA) - cast((((1 - 3) - IB) - IC) as DECIMALV3(38, 9)))");
        assertRewriteAfterTypeCoercion("-IA / 2.0 * ((-IB - 1) - (3 + (IC + 4)))", "((cast((0 - IA) as DECIMALV3(25, 5)) / 2.0) * cast((((((0 - 1) - 3) - 4) - IB) - IC) as DECIMALV3(20, 0)))");
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
                ExpressionAnalyzer.FUNCTION_ANALYZER_RULE,
                bottomUp(
                    FoldConstantRule.INSTANCE
                )
        ));
        assertRewriteAfterTypeCoercion("IA", "IA");
        assertRewriteAfterTypeCoercion("IA > IB", "IA > IB");
        assertRewriteAfterTypeCoercion("IA - 1 > 1", "(cast(IA as BIGINT) > 2)");
        assertRewriteAfterTypeCoercion("IA + 1 - 1 >= 1", "(cast(IA as BIGINT) >= 1)");
        assertRewriteAfterTypeCoercion("IA - 1 - 1 < 1", "(cast(IA as BIGINT) < 3)");
        assertRewriteAfterTypeCoercion("IA - 1 - 1 = 1", "(cast(IA as BIGINT) = 3)");
        assertRewriteAfterTypeCoercion("IA + (1 - 1) >= 1", "(cast(IA as BIGINT) >= 1)");
        assertRewriteAfterTypeCoercion("IA - 1 * 1 = 1", "(cast(IA as BIGINT) = 2)");
        assertRewriteAfterTypeCoercion("IA + 1 + 1 > IB", "(cast(IA as BIGINT) > (cast(IB as BIGINT) - 2))");
        assertRewriteAfterTypeCoercion("IA + 1 > IB", "(cast(IA as BIGINT) > (cast(IB as BIGINT) - 1))");
        assertRewriteAfterTypeCoercion("IA + 1 - (IB - 1) > 1", "IA - IB > -1");
        assertRewriteAfterTypeCoercion("IA + 1 - (IB * IC - 1) > 1", "IA - IB * IC > -1");
        assertRewriteAfterTypeCoercion("IA + 1 - (IB * IC - 1) > 1", "IA - IB * IC > -1");
        assertRewriteAfterTypeCoercion("IA + 1 - (IB * IC - 1) > 1", "((IA - (IB * IC)) > -1)");
        assertRewriteAfterTypeCoercion("(IA - 1) + (IB + 1) - (IC - 1) > 1", "(((IA + IB) - IC) > 0)");
        assertRewriteAfterTypeCoercion("(IA - 1) + (IB + 1) - ((IC - 1) - (ID + 1)) > 1", "((((IA + IB) - IC) + ID) > -1)");
        assertRewriteAfterTypeCoercion("(IA - 1) + (IB + 1) - ((IC - 1) - (ID * IE + 1)) > 1", "((((IA + IB) - IC) + (ID * IE)) > -1)");
        assertRewriteAfterTypeCoercion("IA + 1 - (IB - 1) > IC + 1 - 1 + ID", "((IA - IB) > ((IC + ID) + -2))");
        assertRewriteAfterTypeCoercion("IA + 1 - (IB - 1) > IC - 1", "((IA - IB) > (IC - 3))");
        assertRewriteAfterTypeCoercion("IA + 1 > IB - 1", "(cast(IA as BIGINT) > (IB - 2))");
        assertRewriteAfterTypeCoercion("IA > IB - 1", "cast(IA as bigint) > IB - 1");
        assertRewriteAfterTypeCoercion("IA + 1 > IB", "cast(IA as BIGINT) > (cast(IB as BIGINT) - 1)");
        assertRewriteAfterTypeCoercion("IA + 1 > IB * IC", "cast(IA as BIGINT) > ((IB * IC) - 1)");
        assertRewriteAfterTypeCoercion("IA * ID > IB * IC", "IA * ID > IB * IC");
        assertRewriteAfterTypeCoercion("IA * ID / 2 > IB * IC", "cast((IA * ID) as DOUBLE) > cast((IB * IC) as DOUBLE) * 2.0");
        assertRewriteAfterTypeCoercion("IA * ID / -2 > IB * IC", "cast((IB * IC) as DOUBLE) * -2.0 > cast((IA * ID) as DOUBLE)");
        assertRewriteAfterTypeCoercion("1 - IA > 1", "(cast(IA as BIGINT) < 0)");
        assertRewriteAfterTypeCoercion("1 - IA + 1 * 3 - 5 > 1", "(cast(IA as BIGINT) < -2)");
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
        assertRewriteAfterTypeCoercion("years_add(IA, 1) > '2021-01-01 00:00:00'", "(cast(IA as DATETIMEV2(0)) > '2020-01-01 00:00:00')");
        assertRewriteAfterTypeCoercion("years_sub(IA, 1) > '2021-01-01 00:00:00'", "(cast(IA as DATETIMEV2(0)) > '2022-01-01 00:00:00')");
        assertRewriteAfterTypeCoercion("months_add(IA, 1) > '2021-01-01 00:00:00'", "(cast(IA as DATETIMEV2(0)) > '2020-12-01 00:00:00')");
        assertRewriteAfterTypeCoercion("months_sub(IA, 1) > '2021-01-01 00:00:00'", "(cast(IA as DATETIMEV2(0)) > '2021-02-01 00:00:00')");
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
}
