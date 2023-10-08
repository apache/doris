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

import org.apache.doris.nereids.rules.expression.rules.DistinctPredicatesRule;
import org.apache.doris.nereids.rules.expression.rules.ExtractCommonFactorRule;
import org.apache.doris.nereids.rules.expression.rules.InPredicateDedup;
import org.apache.doris.nereids.rules.expression.rules.InPredicateToEqualToRule;
import org.apache.doris.nereids.rules.expression.rules.NormalizeBinaryPredicatesRule;
import org.apache.doris.nereids.rules.expression.rules.SimplifyCastRule;
import org.apache.doris.nereids.rules.expression.rules.SimplifyDecimalV3Comparison;
import org.apache.doris.nereids.rules.expression.rules.SimplifyNotExprRule;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.CharLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

/**
 * all expr rewrite rule test case.
 */
class ExpressionRewriteTest extends ExpressionRewriteTestHelper {

    @Test
    void testNotRewrite() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(SimplifyNotExprRule.INSTANCE));

        assertRewrite("not x", "not x");
        assertRewrite("not not x", "x");
        assertRewrite("not not not x", "not x");
        assertRewrite("not not not not x", "x");
        assertRewrite("not (x > y)", "x <= y");
        assertRewrite("not (x < y)", "x >= y");
        assertRewrite("not (x >= y)", "x < y");
        assertRewrite("not (x <= y)", "x > y");
        assertRewrite("not (x = y)", "not (x = y)");
        assertRewrite("not not (x > y)", "x > y");
        assertRewrite("not not not (x > y)", "x <= y");
        assertRewrite("not not not (x > (not not y))", "x <= y");
        assertRewrite("not (x > (not not y))", "x <= y");

        assertRewrite("not (a and b)", "(not a) or (not b)");
        assertRewrite("not (a or b)", "(not a) and (not b)");

        assertRewrite("not (a and b and (c or d))", "(not a) or (not b) or ((not c) and (not d))");

    }

    @Test
    void testNormalizeExpressionRewrite() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(NormalizeBinaryPredicatesRule.INSTANCE));

        assertRewrite("1 = 1", "1 = 1");
        assertRewrite("2 > x", "x < 2");
        assertRewrite("y > x", "y > x");
        assertRewrite("1 + 2 > x", "x < 1 + 2");
        assertRewrite("1 + 2 > x + 1", "x + 1 < 1 + 2");
        assertRewrite("y + 2 > x + 1", "y + 2 > x + 1");
    }

    @Test
    void testDistinctPredicatesRewrite() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(DistinctPredicatesRule.INSTANCE));

        assertRewrite("a = 1", "a = 1");
        assertRewrite("a = 1 and a = 1", "a = 1");
        assertRewrite("a = 1 and b > 2 and a = 1", "a = 1 and b > 2");
        assertRewrite("a = 1 and a = 1 and b > 2 and a = 1 and a = 1", "a = 1 and b > 2");
        assertRewrite("a = 1 or a = 1", "a = 1");
        assertRewrite("a = 1 or a = 1 or b >= 1", "a = 1 or b >= 1");
    }

    @Test
    void testExtractCommonFactorRewrite() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(ExtractCommonFactorRule.INSTANCE));

        assertRewrite("a", "a");

        assertRewrite("a = 1", "a = 1");
        assertRewrite("a and b", "a and b");
        assertRewrite("a = 1 and b > 2", "a = 1 and b > 2");

        assertRewrite("(a and b) or (c and d)", "(a and b) or (c and d)");
        assertRewrite("(a and b) and (c and d)", "((a and b) and c) and d");

        assertRewrite("(a or b) and (a or c)", "a or (b and c)");
        assertRewrite("(a and b) or (a and c)", "a and (b or c)");

        assertRewrite("(a or b) and (a or c) and (a or d)", "a or (b and c and d)");
        assertRewrite("(a and b) or (a and c) or (a and d)", "a and (b or c or d)");
        assertRewrite("(a and b) or (a or c) or (a and d)", "((((a and b) or a) or c) or (a and d))");
        assertRewrite("(a and b) or (a and c) or (a or d)", "(((a and b) or (a and c) or a) or d))");
        assertRewrite("(a or b) or (a and c) or (a and d)", "(a or b) or (a and c) or (a and d)");
        assertRewrite("(a or b) or (a and c) or (a or d)", "(((a or b) or (a and c)) or d)");
        assertRewrite("(a or b) or (a or c) or (a and d)", "((a or b) or c) or (a and d)");
        assertRewrite("(a or b) or (a or c) or (a or d)", "(((a or b) or c) or d)");

        assertRewrite("(a and b) or (d and c) or (d and e)", "(a and b) or (d and c) or (d and e)");
        assertRewrite("(a or b) and (d or c) and (d or e)", "(a or b) and (d or c) and (d or e)");

        assertRewrite("(a and b) or ((d and c) and (d and e))", "(a and b) or (d and c and e)");
        assertRewrite("(a or b) and ((d or c) or (d or e))", "(a or b) and (d or c or e)");

        assertRewrite("(a and b) or (a and b and c)", "a and b");
        assertRewrite("(a or b) and (a or b or c)", "a or b");

        assertRewrite("a and true", "a");
        assertRewrite("a or false", "a");

        assertRewrite("a and false", "false");
        assertRewrite("a or true", "true");

        assertRewrite("a or false or false or false", "a");
        assertRewrite("a and true and true and true", "a");

        assertRewrite("(a and b) or a ", "a");
        assertRewrite("(a or b) and a ", "a");

        assertRewrite("(a and b) or (a and true)", "a");
        assertRewrite("(a or b) and (a and true)", "a");

        assertRewrite("(a or b) and (a or true)", "a or b");

    }

    @Test
    void testInPredicateToEqualToRule() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(InPredicateToEqualToRule.INSTANCE));

        assertRewrite("a in (1)", "a = 1");
        assertRewrite("a not in (1)", "not a = 1");
        assertRewrite("a in (a in (1))", "a = (a = 1)");
        assertRewrite("(a in (1)) in (1)", "(a = 1) = 1");
        assertRewrite("(a in (1, 2)) in (1)", "(a in (1, 2)) = 1");
        assertRewrite("(a in (1)) in (1, 2)", "((a = 1) in (1, 2))");
        assertRewrite("case a when b in (1) then a else c end in (1)",
                "case a when b = 1 then a else c end = 1");
        assertRewrite("case a when b not in (1) then a else c end not in (1)",
                "not case a when not b = 1 then a else c end = 1");
    }

    @Test
    void testInPredicateDedup() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(InPredicateDedup.INSTANCE));

        assertRewrite("a in (1, 2, 1, 2)", "a in (1, 2)");
    }

    @Test
    void testSimplifyCastRule() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(SimplifyCastRule.INSTANCE));

        // deduplicate
        assertRewrite("CAST(1 AS tinyint)", "1");
        assertRewrite("CAST('str' AS varchar)", "'str'");
        assertRewrite("CAST(CAST(1 AS tinyint) AS tinyint)", "1");

        // deduplicate inside
        assertRewrite("CAST(CAST('str' AS varchar) AS double)", "CAST('str' AS double)");
        assertRewrite("CAST(CAST(1 AS tinyint) AS double)", "CAST(1 AS double)");

        // string literal
        assertRewrite(new Cast(new CharLiteral("123", 3), VarcharType.createVarcharType(10)),
                new VarcharLiteral("123", 10));
        assertRewrite(new Cast(new VarcharLiteral("123", 3), VarcharType.createVarcharType(10)),
                new VarcharLiteral("123", 10));
        assertRewrite(new Cast(new CharLiteral("123", 3), StringType.INSTANCE), new StringLiteral("123"));
        assertRewrite(new Cast(new VarcharLiteral("123", 3), StringType.INSTANCE), new StringLiteral("123"));

        // decimal literal
        assertRewrite(new Cast(new TinyIntLiteral((byte) 1), DecimalV2Type.createDecimalV2Type(15, 9)),
                new DecimalLiteral(new BigDecimal(1)));
        assertRewrite(new Cast(new SmallIntLiteral((short) 1), DecimalV2Type.createDecimalV2Type(15, 9)),
                new DecimalLiteral(new BigDecimal(1)));
        assertRewrite(new Cast(new IntegerLiteral(1), DecimalV2Type.createDecimalV2Type(15, 9)),
                new DecimalLiteral(new BigDecimal(1)));
        assertRewrite(new Cast(new BigIntLiteral(1L), DecimalV2Type.createDecimalV2Type(15, 9)),
                new DecimalLiteral(new BigDecimal(1)));
    }

    @Test
    void testSimplifyDecimalV3Comparison() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(SimplifyDecimalV3Comparison.INSTANCE));

        // do rewrite
        Expression left = new DecimalV3Literal(new BigDecimal("12345.67"));
        Expression cast = new Cast(left, DecimalV3Type.createDecimalV3Type(27, 9));
        Expression right = new DecimalV3Literal(DecimalV3Type.createDecimalV3Type(27, 9), new BigDecimal("0.01"));
        Expression expectedRight = new DecimalV3Literal(DecimalV3Type.createDecimalV3Type(7, 2), new BigDecimal("0.01"));
        Expression comparison = new EqualTo(cast, right);
        Expression expected = new EqualTo(left, expectedRight);
        assertRewrite(comparison, expected);

        // not cast
        comparison = new EqualTo(new DecimalV3Literal(new BigDecimal("12345.67")), new DecimalV3Literal(new BigDecimal("76543.21")));
        assertRewrite(comparison, comparison);
    }
}
