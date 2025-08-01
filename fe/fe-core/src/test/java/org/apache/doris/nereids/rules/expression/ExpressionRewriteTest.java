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

import org.apache.doris.nereids.rules.expression.rules.AddMinMax;
import org.apache.doris.nereids.rules.expression.rules.DistinctPredicatesRule;
import org.apache.doris.nereids.rules.expression.rules.ExtractCommonFactorRule;
import org.apache.doris.nereids.rules.expression.rules.InPredicateDedup;
import org.apache.doris.nereids.rules.expression.rules.InPredicateExtractNonConstant;
import org.apache.doris.nereids.rules.expression.rules.InPredicateToEqualToRule;
import org.apache.doris.nereids.rules.expression.rules.NormalizeBinaryPredicatesRule;
import org.apache.doris.nereids.rules.expression.rules.SimplifyCastRule;
import org.apache.doris.nereids.rules.expression.rules.SimplifyComparisonPredicate;
import org.apache.doris.nereids.rules.expression.rules.SimplifyConflictCompound;
import org.apache.doris.nereids.rules.expression.rules.SimplifyNotExprRule;
import org.apache.doris.nereids.rules.expression.rules.SimplifyRange;
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
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                ExpressionRewrite.bottomUp(SimplifyNotExprRule.INSTANCE)
        ));

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
    void testSimplifyConflictPredicate() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                ExpressionRewrite.bottomUp(SimplifyConflictCompound.INSTANCE)
        ));

        assertRewriteAfterTypeCoercion("a > b and not(a > b)",
                "(a > b) IS NULL AND NULL");
        assertRewriteAfterTypeCoercion("not(a > b) and a > b",
                "(a > b) IS NULL AND NULL");
        assertRewriteAfterTypeCoercion("a > b and not(a > b) and a > b and not(a > b)",
                "(a > b) IS NULL AND NULL");
        assertRewriteAfterTypeCoercion("a > b and not(a > b) and not (not (a > b)) ",
                "(a > b) IS NULL AND NULL");
        assertRewriteAfterTypeCoercion("a > c and a > b and not(a > b) and a > d",
                "a > c AND (a > b) IS NULL AND NULL and a > d");
        assertRewriteAfterTypeCoercion("a > b or not(a > b)",
                "NOT (a > b) IS NULL OR NULL");
        assertRewriteAfterTypeCoercion("not(a > b) or a > b",
                "NOT (a > b) IS NULL OR NULL");
        assertRewriteAfterTypeCoercion("a > b or not(a > b) or a > b or not(a > b)",
                "NOT (a > b) IS NULL OR NULL");
        assertRewriteAfterTypeCoercion("a > b or not(a > b) or not (not (a > b)) ",
                "NOT (a > b) IS NULL OR NULL");
        assertRewriteAfterTypeCoercion("a > c or a > b or not(a > b) or a > d",
                "a > c OR NOT (a > b) IS NULL OR NULL OR a > d");
        assertRewriteAfterTypeCoercion("a > b and (c > d or not (c > d))",
                "a > b AND (NOT (c > d) IS NULL OR NULL)");
        assertRewriteAfterTypeCoercion("(a > b or not(a > b)) and (c > d or not (c > d))",
                "(NOT (a > b) IS NULL OR NULL) AND (NOT (c > d) IS NULL OR NULL)");
        assertRewriteAfterTypeCoercion("a > b or (c > d and not (c > d))",
                "a > b OR ((c > d) IS NULL AND NULL)");
        assertRewriteAfterTypeCoercion("(a > b and not(a > b)) or (c > d and not (c > d))",
                "((a > b) IS NULL AND NULL) OR ((c > d) IS NULL AND NULL)");

        assertRewriteAfterTypeCoercion("a is null and not a is null", "FALSE");
        assertRewriteAfterTypeCoercion("a is null or not a is null", "TRUE");

        // not rewrite non-foldable expression
        assertRewriteAfterTypeCoercion("a > b and not(a > b) and c > random(1, 10) and not (c > random(1, 10))",
                "(a > b) IS NULL AND NULL AND c > random(1, 10) AND NOT (c > random(1, 10))");
    }

    @Test
    void testNormalizeExpressionRewrite() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                ExpressionRewrite.bottomUp(NormalizeBinaryPredicatesRule.INSTANCE)
        ));

        assertRewrite("1 = 1", "1 = 1");
        assertRewrite("2 > x", "x < 2");
        assertRewrite("y > x", "y > x");
        assertRewrite("1 + 2 > x", "x < 1 + 2");
        assertRewrite("1 + 2 > x + 1", "x + 1 < 1 + 2");
        assertRewrite("y + 2 > x + 1", "y + 2 > x + 1");
    }

    @Test
    void testDistinctPredicatesRewrite() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(DistinctPredicatesRule.INSTANCE)
        ));

        assertRewrite("a = 1", "a = 1");
        assertRewrite("a = 1 and a = 1", "a = 1");
        assertRewrite("a = 1 and b > 2 and a = 1", "a = 1 and b > 2");
        assertRewrite("a = 1 and a = 1 and b > 2 and a = 1 and a = 1", "a = 1 and b > 2");
        assertRewrite("a = 1 or a = 1", "a = 1");
        assertRewrite("a = 1 or a = 1 or b >= 1", "a = 1 or b >= 1");
    }

    @Test
    void testExtractCommonFactorRewrite() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(ExtractCommonFactorRule.INSTANCE)
        ));

        assertRewrite("a", "a");

        assertRewrite("a = 1", "a = 1");
        assertRewrite("a and b", "a and b");
        assertRewrite("a = 1 and b > 2", "a = 1 and b > 2");

        assertRewrite("(a and b) or (c and d)", "(a and b) or (c and d)");
        assertRewrite("(a and b) and (c and d)", "((a and b) and (c and d))");
        assertRewrite("(a and (b and c)) and (b or c)", "((b and c) and a)");

        assertRewrite("(a or b) and (a or c)", "a or (b and c)");
        assertRewrite("(a and b) or (a and c)", "a and (b or c)");

        assertRewrite("(a or b) and (a or c) and (a or d)", "a or (b and c and d)");
        assertRewrite("(a and b) or (a and c) or (a and d)", "a and (b or c or d)");
        assertRewrite("(a or b) and (a or d)", "a or (b and d)");
        assertRewrite("(a and b) or (a or c) or (a and d)", "a or c");
        assertRewrite("(a and b) or (a and c) or (a or d)", "(a or d)");
        assertRewrite("(a or b) or (a and c) or (a and d)", "(a or b)");
        assertRewrite("(a or b) or (a and c) or (a or d)", "((a or b) or d)");
        assertRewrite("(a or b) or (a or c) or (a and d)", "((a or b) or c)");
        assertRewrite("(a or b) or (a or c) or (a or d)", "(((a or b) or c) or d)");

        assertRewrite("(a and b) or (d and c) or (d and e)", "((d and (c or e)) or (a and b))");
        assertRewrite("(a or b) and (d or c) and (d or e)", "((d or (c and e)) and (a or b))");

        assertRewrite("(a and b) or ((d and c) and (d and e))", "(a and b) or (d and c and e)");
        assertRewrite("(a or b) and ((d or c) or (d or e))", "(a or b) and (d or c or e)");

        assertRewrite("(a and b) or (a and b and c)", "a and b");
        assertRewrite("(a or b) and (a or b or c)", "a or b");

        assertRewrite("(a and b) or a ", "a");
        assertRewrite("(a or b) and a ", "a");

        assertRewrite("(a and b) or (a and true)", "a");
        assertRewrite("(a or b) and (a and true)", "a");

        assertRewrite("(a or b) and (a or true)", "a or b");

        assertRewrite("a and (b or ((a and e) or (a and f))) and (b or d)", "(b or ((a and (e or f)) and d)) and a");

        assertRewrite("a = 1 and (b = 1 and d < 1 or c = 1 and b = 1 and d > 4)",
                "a  = 1 and b = 1 and (d < 1 or c = 1 and d > 4)");

        assertRewrite("a = 1 and (b = 1 and c = 1 and d < 1 or c = 1 and b = 1 and d > 4)",
                "a  = 1 and b = 1 and c = 1 and (d < 1 or d > 4)");

        assertRewrite("a = 1 and (b = 1 and c = 1 and d < 1 or c = 1 and b = 1 and d > 4) and (b = 1 and c = 1 and e < 1 or b = 1 and c = 1 and e > 4) ",
                "a  = 1 and b = 1 and c = 1 and (d < 1 or d > 4) and (e < 1 or e > 4)");

        assertRewrite("((a = 1 and b = 1) or (a = 1 and b = 2) or c < 1) and ((a = 1 and b = 1) or (a = 1 and b = 2) or c > 2)",
                "a = 1 and (b = 1 or b = 2) or (c < 1 and c > 2)");
    }

    @Test
    void testInPredicateToEqualToRule() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(InPredicateToEqualToRule.INSTANCE)
        ));

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
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(InPredicateDedup.INSTANCE)
        ));

        assertRewrite("a in (1, 2, 1, 2)", "a in (1, 2)");
    }

    @Test
    void testSimplifyCastRule() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(SimplifyCastRule.INSTANCE)
        ));

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
                new DecimalLiteral(DecimalV2Type.createDecimalV2Type(15, 9), new BigDecimal("1.000000000")));
        assertRewrite(new Cast(new SmallIntLiteral((short) 1), DecimalV2Type.createDecimalV2Type(15, 9)),
                new DecimalLiteral(DecimalV2Type.createDecimalV2Type(15, 9), new BigDecimal("1.000000000")));
        assertRewrite(new Cast(new IntegerLiteral(1), DecimalV2Type.createDecimalV2Type(15, 9)),
                new DecimalLiteral(DecimalV2Type.createDecimalV2Type(15, 9), new BigDecimal("1.000000000")));
        assertRewrite(new Cast(new BigIntLiteral(1L), DecimalV2Type.createDecimalV2Type(15, 9)),
                new DecimalLiteral(DecimalV2Type.createDecimalV2Type(15, 9), new BigDecimal("1.000000000")));
    }

    @Test
    void testSimplifyDecimalV3Comparison() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(SimplifyComparisonPredicate.INSTANCE)
        ));

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

    @Test
    void testDeadLoop() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
            bottomUp(
                SimplifyRange.INSTANCE,
                ExtractCommonFactorRule.INSTANCE
            )
        ));

        assertRewrite("a and (b > 0 and b < 10)", "a and (b > 0 and b < 10)");
    }

    @Test
    void testAddMinMax() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
            bottomUp(
                AddMinMax.INSTANCE
            )
        ));

        assertRewriteAfterTypeCoercion("TA >= 10", "TA >= 10");
        assertRewriteAfterTypeCoercion("TA between 10 and 20", "TA >= 10 and TA <= 20");
        assertRewriteAfterTypeCoercion("TA between 10 and 20 or TA >= 30",
                "(TA <= 20 OR TA >= 30) AND (TA >= 10)");
        assertRewriteAfterTypeCoercion("TA >= 10 and TA <= 20 or TA >= 50 and TA <= 60 or TA >= 100 and TA <= 120",
                "(TA <= 20 or TA >= 50 and TA <= 60 or TA >= 100) AND TA >= 10 and TA <= 120");
        assertRewriteAfterTypeCoercion("TA > 10 and TA < 20 or TA > 50 and TA < 60 or TA > 100 and TA < 120",
                "(TA < 20 or TA > 50 and TA < 60 or TA > 100) AND TA > 10 and TA < 120");
        // only slot reference add min max
        assertRewriteAfterTypeCoercion("TA + TB > 10 and TA + TB < 20 or TA + TB > 50 and TA + TB < 60 or TA + TB > 100 and TA + TB < 120",
                "TA + TB > 10 and TA + TB < 20 or TA + TB > 50 and TA + TB < 60 or TA + TB > 100 and TA + TB < 120");
        assertRewriteAfterTypeCoercion("ISNULL (TA > 10) and TA > 10 and TA < 20 or TA > 50 and TA < 60 or TA > 100 and TA < 120",
                "(ISNULL(TA > 10) and TA < 20 or TA > 50 and TA < 60 or TA > 100) AND TA > 10 and TA < 120");
        assertRewriteAfterTypeCoercion("ISNULL (TA > 10) or TA > 10 and TA < 20 or TA > 50 and TA < 60 or TA > 100 and TA < 120",
                "ISNULL (TA > 10) or TA > 10 and TA < 20 or TA > 50 and TA < 60 or TA > 100 and TA < 120");
        assertRewriteAfterTypeCoercion("TA = 4 or (TA > 4 and TB is null)", "(TA = 4 or (TA > 4 and TB is null)) and TA >= 4");
        assertRewriteAfterTypeCoercion("TA in (10, 50, 100) or TA in (20, 40)", "TA in (10, 50, 100) or TA in (20, 40)");
        assertRewriteAfterTypeCoercion("TA in (10, 50, 100) or TA >= 70",
                "(TA in (10, 50, 100) or TA >= 70) AND TA >= 10");
        assertRewriteAfterTypeCoercion("TA in (10, 50, 100) or TA < 70",
                "(TA in (10, 50, 100) or TA < 70) AND TA <= 100");
        assertRewriteAfterTypeCoercion("TA in (10, 50, 100) or TA >= 70 and TA <= 90",
                "(TA in (10, 50, 100) or TA >= 70 AND TA <= 90) AND TA >= 10 AND TA <= 100");
        assertRewriteAfterTypeCoercion("TA in (10, 50, 100) or TA >= 70 and TA < 120",
                "(TA in (10, 50, 100) or TA >= 70) AND TA >= 10 AND TA < 120");
        assertRewriteAfterTypeCoercion("TA between 10 and 20 and TB between 10 and 20", "TA >= 10 and TA <= 20 and TB >= 10 and TB <= 20");
        assertRewriteAfterTypeCoercion("TA between 10 and 20 and TB between 10 and 20 or TA between 100 and 120 and TB between 100 and 120",
                "(TA <= 20 and TB <= 20 or TA >= 100 and TB >= 100) AND TA >= 10 AND TA <= 120 AND TB >= 10 AND TB <= 120");
        assertRewriteAfterTypeCoercion("TA >= 10 AND (TA between 12 and 20 and TB between 10 and 20 or TA between 100 and 120 and TB between 100 and 120)",
                "TA >= 10 and (TA <= 20 and TB <= 20 or TA >= 100 and TB >= 100) AND TA >= 12 AND TA <= 120 AND TB >= 10 AND TB <= 120");
        assertRewriteAfterTypeCoercion("TA between 10 and 20 and TB between 100 and 120 or TA between 100 and 120 and TB between 10 and 20",
                "(TA <= 20 and TB >= 100 or TA >= 100 and TB <= 20) AND TA >= 10 AND TA <= 120 AND TB >= 10 AND TB <= 120");
        assertRewriteAfterTypeCoercion("TA between 10 and 20 and TB between 10 and 20 AND TC between 10 and 20 or TA between 100 and 120 and TB between 100 and 120",
                "(TA <= 20 and TB <= 20 and TC >= 10 and TC <= 20 or TA >= 100 and TB >= 100) AND TA >= 10 AND TA <= 120 AND TB >= 10 AND TB <= 120");
        assertRewriteAfterTypeCoercion("TA between 10 and 20 and TB between 10 and 20 AND TC between 10 and 20 or TA between 100 and 120 and TB between 100 and 120 and TC between 100 AND 120",
                "(TA <= 20 and TB <= 20 and TC <= 20 or TA >= 100 and TB >= 100 and TC >= 100) AND TA >= 10 AND TA <= 120 AND TB >= 10 AND TB <= 120 AND TC >= 10 AND TC <= 120");
        assertRewriteAfterTypeCoercion("TA between 10 and 20 and TB between 100 and 120 OR TB between 10 and 20 AND TC between 100 and 120 OR TA between 100 and 120 AND TC between 10 and 20",
                "TA >= 10 and TA <= 20 and TB >= 100 AND TB <= 120 OR TB >= 10 AND TB <= 20 AND TC >= 100 AND TC <= 120 OR TA >= 100 AND TA <= 120 AND TC >= 10 AND TC <= 20");
        assertRewriteAfterTypeCoercion("((TA = 1 AND SC ='1') OR SC = '1212') AND TA =1", "(SC = '1' OR SC = '1212') AND TA =1");
        assertRewriteAfterTypeCoercion("(TA + TC > 10 and TB > 10) or (TB > 10 and TB > 20)", "(TA + TC > 10 or TB > 20) AND TB > 10");
        assertRewriteAfterTypeCoercion("((TA + TC > 10 or TA + TC > 5) and TB > 10) or (TB > 10 and (TB > 20 or TB < 10))",
                "((TA + TC > 10 or TA + TC > 5) or (TB > 20 or TB < 10)) and TB > 10");
        assertRewriteAfterTypeCoercion("TA >= 8 and TB >= 1 or TA < 8 and TB <= 10",
                "TA >= 8 and TB >= 1 or TA < 8 and TB <= 10");
        assertRewriteAfterTypeCoercion("TA >= 8 and TB >= 1 and TA < 8 and TB <= 10",
                "TA >= 8 and TB >= 1 and TA < 8 and TB <= 10");
        assertRewriteAfterTypeCoercion("(CA >= date '2024-01-01' and CA <= date '2024-01-03') or (CA > date '2024-01-05' and CA < date '2024-01-07')",
                "(CA <= date '2024-01-03' or CA > date '2024-01-05') and CA >= date '2024-01-01' and CA < date '2024-01-07'");
        assertRewriteAfterTypeCoercion("CA in (date '2024-01-01',date '2024-01-02',date '2024-01-03') or CA < date '2024-01-01'",
                "(CA in (date '2024-01-01',date '2024-01-02',date '2024-01-03') or CA < date '2024-01-01') AND CA <= date '2024-01-03'");
        assertRewriteAfterTypeCoercion("(AA >= timestamp '2024-01-01 00:00:00' and AA <= timestamp '2024-01-03 00:00:00') or (AA > timestamp '2024-01-05 00:00:00' and AA < timestamp '2024-01-07 00:00:00')",
                "(AA <= timestamp '2024-01-03 00:00:00' or AA > timestamp '2024-01-05 00:00:00') and AA >= timestamp '2024-01-01 00:00:00' and AA < timestamp '2024-01-07 00:00:00'");
        assertRewriteAfterTypeCoercion("AA in (timestamp '2024-01-01 02:00:00',timestamp '2024-01-02 02:00:00',timestamp '2024-01-03 02:00:00') or AA < timestamp '2024-01-01 01:00:00'",
                "(AA in (timestamp '2024-01-01 02:00:00',timestamp '2024-01-02 02:00:00',timestamp '2024-01-03 02:00:00') or AA < timestamp '2024-01-01 01:00:00' ) and AA <= timestamp '2024-01-03 02:00:00'");

    }

    @Test
    void testSimplifyRangeAndAddMinMax() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(
                        SimplifyRange.INSTANCE,
                        AddMinMax.INSTANCE
                )
        ));

        assertRewriteAfterTypeCoercion("ISNULL(TA)", "ISNULL(TA)");
        assertRewriteAfterTypeCoercion("ISNULL(TA) and null", "ISNULL(TA) and null");
        assertRewriteAfterTypeCoercion("ISNULL(TA) and ISNULL(TA)", "ISNULL(TA)");
        assertRewriteAfterTypeCoercion("ISNULL(TA) or ISNULL(TA)", "ISNULL(TA)");
        assertRewriteAfterTypeCoercion("ISNULL(TA) and TA between 20 and 10", "ISNULL(TA) and null");
        // assertRewriteAfterTypeCoercion("ISNULL(TA) and TA > 10", "ISNULL(TA) and null"); // should be, but not support now
        assertRewriteAfterTypeCoercion("ISNULL(TA) and TA > 10 and null", "ISNULL(TA) and null");
        assertRewriteAfterTypeCoercion("ISNULL(TA) or TA > 10", "ISNULL(TA) or TA > 10");
        // assertRewriteAfterTypeCoercion("(TA < 30 or TA > 40) and TA between 20 and 10", "TA IS NULL AND NULL"); // should be, but not support because flatten and
        assertRewriteAfterTypeCoercion("(TA < 30 or TA > 40) and TA is null and null", "TA IS NULL AND NULL");
        assertRewriteAfterTypeCoercion("(TA < 30 or TA > 40) or TA between 20 and 10", "TA < 30 or TA > 40");

        assertRewriteAfterTypeCoercion("TA between 10 and 20 or TA between 30 and 40 or TA between 60 and 50",
                "(TA <= 20 or TA >= 30) and TA >= 10 and TA <= 40");
        // should be, but not support yet, because 'TA is null and null' => UnknownValue(EmptyValue(TA) and null)
        //assertRewriteAfterTypeCoercion("TA between 10 and 20 or TA between 30 and 40 or TA is null and null",
        //        "(TA <= 20 or TA >= 30) and TA >= 10 and TA <= 40");
        assertRewriteAfterTypeCoercion("TA between 10 and 20 or TA between 30 and 40 or TA is null and null",
                "(TA <= 20 or TA >= 30 or TA is null and null) and TA >= 10 and TA <= 40");
        assertRewriteAfterTypeCoercion("TA between 10 and 20 or TA between 30 and 40 or TA is null",
                "TA >= 10 and TA <= 20 or TA >= 30 and TA <= 40 or TA is null");
        assertRewriteAfterTypeCoercion("ISNULL(TB) and (TA between 10 and 20 or TA between 30 and 40 or TA between 60 and 50)",
                "ISNULL(TB) and ((TA <= 20 or TA >= 30) and TA >= 10 and TA <= 40)");
        assertRewriteAfterTypeCoercion("ISNULL(TB) and (TA between 10 and 20 or TA between 30 and 40 or TA is null)",
                "ISNULL(TB) and (TA >= 10 and TA <= 20 or TA >= 30 and TA <= 40 or TA is null)");
        assertRewriteAfterTypeCoercion("TB between 20 and 10 and (TA between 10 and 20 or TA between 30 and 40 or TA between 60 and 50)",
                "TB IS NULL AND NULL and (TA <= 20 or TA >= 30) and TA >= 10 and TA <= 40");
        assertRewriteAfterTypeCoercion("TA between 10 and 20 and TB between 10 and 20 or TA between 30 and 40 and TB between 30 and 40 or TA between 60 and 50 and TB between 60 and 50",
                "(TA <= 20 and TB <= 20 or TA >= 30 and TB >= 30 or TA is null and null and TB is null) and TA >= 10 and TA <= 40 and TB >= 10 and TB <= 40");
    }

    @Test
    public void testInPredicateExtractNonConstant() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(
                        InPredicateExtractNonConstant.INSTANCE
                )
        ));

        assertRewriteAfterTypeCoercion("TA in (3, 2, 1)", "TA in (3, 2, 1)");
        assertRewriteAfterTypeCoercion("TA in (TB, TC, TB)", "TA = TB or TA = TC");
        assertRewriteAfterTypeCoercion("TA in (3, 2, 1, TB, TC, TB)", "TA in (3, 2, 1) or TA = TB or TA = TC");
        assertRewriteAfterTypeCoercion("IA in (1 + 2, 2 + 3, 3 + TB)", "IA in (cast(1 + 2 as int), cast(2 + 3 as int)) or IA = cast(3 + TB as int)");
    }
}
