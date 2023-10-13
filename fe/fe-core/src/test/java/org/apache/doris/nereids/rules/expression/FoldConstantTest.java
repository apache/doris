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

import org.apache.doris.analysis.ArithmeticExpr.Operator;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.rules.expression.rules.FunctionBinder;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.TimestampArithmetic;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeArithmetic;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeExtractAndTransform;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Interval.TimeUnit;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.MemoTestUtils;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Locale;

public class FoldConstantTest extends ExpressionRewriteTestHelper {

    @Test
    public void testCaseWhenFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(FoldConstantRuleOnFE.INSTANCE));
        // assertRewriteAfterTypeCoercion("case when 1 = 2 then 1 when '1' < 2 then 2 else 3 end", "2");
        // assertRewriteAfterTypeCoercion("case when 1 = 2 then 1 when '1' > 2 then 2 end", "null");
        assertRewriteAfterTypeCoercion("case when (1 + 5) / 2 > 2 then 4  when '1' < 2 then 2 else 3 end", "4");
        assertRewriteAfterTypeCoercion("case when not 1 = 2 then 1 when '1' > 2 then 2 end", "1");
        assertRewriteAfterTypeCoercion("case when 1 = 2 then 1 when 3 in ('1',2 + 8 / 2,3,4) then 2 end", "2");
        assertRewriteAfterTypeCoercion("case when TA = 2 then 1 when 3 in ('1',2 + 8 / 2,3,4) then 2 end", "CASE  WHEN (TA = 2) THEN 1 ELSE 2 END");
        assertRewriteAfterTypeCoercion("case when TA = 2 then 5 when 3 in (2,3,4) then 2 else 4 end", "CASE  WHEN (TA = 2) THEN 5 ELSE 2 END");
        assertRewriteAfterTypeCoercion("case when TA = 2 then 1 when TB in (2,3,4) then 2 else 4 end", "CASE  WHEN (TA = 2) THEN 1 WHEN TB IN (2, 3, 4) THEN 2 ELSE 4 END");
        assertRewriteAfterTypeCoercion("case when null = 2 then 1 when 3 in (2,3,4) then 2 else 4 end", "2");
        assertRewriteAfterTypeCoercion("case when null = 2 then 1 else 4 end", "4");
        assertRewriteAfterTypeCoercion("case when null = 2 then 1 end", "null");
        assertRewriteAfterTypeCoercion("case when TA = TB then 1 when TC is null then 2 end", "CASE WHEN (TA = TB) THEN 1 WHEN TC IS NULL THEN 2 END");
    }

    @Test
    public void testInFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(FoldConstantRuleOnFE.INSTANCE));
        assertRewriteAfterTypeCoercion("1 in (1,2,3,4)", "true");
        // Type Coercion trans all to string.
        assertRewriteAfterTypeCoercion("3 in ('1', 2 + 8 / 2, 3, 4)", "true");
        assertRewriteAfterTypeCoercion("4 / 2 * 1 - (5 / 2) in ('1', 2 + 8 / 2, 3, 4)", "false");
        assertRewriteAfterTypeCoercion("null in ('1', 2 + 8 / 2, 3, 4)", "null");
        assertRewriteAfterTypeCoercion("3 in ('1', null, 3, 4)", "true");
        assertRewriteAfterTypeCoercion("TA in (1, null, 3, 4)", "TA in (1, null, 3, 4)");
        assertRewriteAfterTypeCoercion("IA in (IB, IC, null)", "IA in (IB, IC, null)");
    }

    @Test
    public void testLogicalFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(FoldConstantRuleOnFE.INSTANCE));
        assertRewriteAfterTypeCoercion("10 + 1 > 1 and 1 > 2", "false");
        assertRewriteAfterTypeCoercion("10 + 1 > 1 and 1 < 2", "true");
        assertRewriteAfterTypeCoercion("null + 1 > 1 and 1 < 2", "null");
        assertRewriteAfterTypeCoercion("10 < 3 and 1 > 2", "false");
        assertRewriteAfterTypeCoercion("6 / 2 - 10 * (6 + 1) > 2 and 10 > 3 and 1 > 2", "false");

        assertRewriteAfterTypeCoercion("10 + 1 > 1 or 1 > 2", "true");
        assertRewriteAfterTypeCoercion("null + 1 > 1 or 1 > 2", "null");
        assertRewriteAfterTypeCoercion("6 / 2 - 10 * (6 + 1) > 2 or 10 > 3 or 1 > 2", "true");

        assertRewriteAfterTypeCoercion("(1 > 5 and 8 < 10 or 1 = 3) or (1 > 8 + 9 / (10 * 2) or ( 10 = 3))", "false");
        assertRewriteAfterTypeCoercion("(TA > 1 and 8 < 10 or 1 = 3) or (1 > 3 or ( 10 = 3))", "TA > 1");

        assertRewriteAfterTypeCoercion("false or false", "false");
        assertRewriteAfterTypeCoercion("false or true", "true");
        assertRewriteAfterTypeCoercion("true or false", "true");
        assertRewriteAfterTypeCoercion("true or true", "true");

        assertRewriteAfterTypeCoercion("true and true", "true");
        assertRewriteAfterTypeCoercion("false and true", "false");
        assertRewriteAfterTypeCoercion("true and false", "false");
        assertRewriteAfterTypeCoercion("false and false", "false");

        assertRewriteAfterTypeCoercion("true and null", "null");
        assertRewriteAfterTypeCoercion("false and null", "false");
        assertRewriteAfterTypeCoercion("null and IA is null", "null and IA is null");
        assertRewriteAfterTypeCoercion("true or null", "true");
        assertRewriteAfterTypeCoercion("false or null", "null");
        assertRewriteAfterTypeCoercion("null or IA is null", "null or IA is null");

        assertRewriteAfterTypeCoercion("null and null", "null");
        assertRewriteAfterTypeCoercion("null or null", "null");

    }

    @Test
    public void testIsNullFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(FoldConstantRuleOnFE.INSTANCE));
        assertRewriteAfterTypeCoercion("100 is null", "false");
        assertRewriteAfterTypeCoercion("null is null", "true");
        assertRewriteAfterTypeCoercion("null is not null", "false");
        assertRewriteAfterTypeCoercion("100 is not null", "true");
        assertRewriteAfterTypeCoercion("IA is not null", "IA is not null");
        assertRewriteAfterTypeCoercion("IA is null", "IA is null");
    }

    @Test
    public void testNotPredicateFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(FoldConstantRuleOnFE.INSTANCE));
        assertRewriteAfterTypeCoercion("not 1 > 2", "true");
        assertRewriteAfterTypeCoercion("not null + 1 > 2", "null");
        assertRewriteAfterTypeCoercion("not (1 + 5) / 2 + (10 - 1) * 3 > 3 * 5 + 1", "false");
    }

    @Test
    public void testCastFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(FoldConstantRuleOnFE.INSTANCE));

        // cast '1' as tinyint
        Cast c = new Cast(Literal.of("1"), TinyIntType.INSTANCE);
        Expression rewritten = executor.rewrite(c, context);
        Literal expected = Literal.of((byte) 1);
        Assertions.assertEquals(rewritten, expected);
    }

    @Test
    public void testCompareFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(FoldConstantRuleOnFE.INSTANCE));
        assertRewriteAfterTypeCoercion("'1' = 2", "false");
        assertRewriteAfterTypeCoercion("1 = 2", "false");
        assertRewriteAfterTypeCoercion("1 != 2", "true");
        assertRewriteAfterTypeCoercion("2 > 2", "false");
        assertRewriteAfterTypeCoercion("3 * 10 + 1 / 2 >= 2", "true");
        assertRewriteAfterTypeCoercion("3 < 2", "false");
        assertRewriteAfterTypeCoercion("3 <= 2", "false");
        assertRewriteAfterTypeCoercion("3 <= null", "null");
        assertRewriteAfterTypeCoercion("3 >= null", "null");
        assertRewriteAfterTypeCoercion("null <=> null", "true");
        assertRewriteAfterTypeCoercion("2 <=> null", "false");
        assertRewriteAfterTypeCoercion("2 <=> 2", "true");
    }

    @Test
    public void testArithmeticFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(FoldConstantRuleOnFE.INSTANCE));
        assertRewrite("1 + 1", Literal.of((short) 2));
        assertRewrite("1 - 1", Literal.of((short) 0));
        assertRewrite("100 + 100", Literal.of((short) 200));
        assertRewrite("1 - 2", Literal.of((short) -1));
        assertRewriteAfterTypeCoercion("1 - 2 > 1", "false");
        assertRewriteAfterTypeCoercion("1 - 2 + 1 > 1 + 1 - 100", "true");
        assertRewriteAfterTypeCoercion("10 * 2 / 1 + 1 > (1 + 1) - 100", "true");

        // a + 1 > 2
        Slot a = SlotReference.of("a", IntegerType.INSTANCE);

        // a > (1 + 10) / 2 * (10 + 1)
        Expression e3 = PARSER.parseExpression("(1 + 10) / 2 * (10 + 1)");
        Expression e4 = new GreaterThan(a, e3);
        Expression e5 = new GreaterThan(new Cast(a, DoubleType.INSTANCE), Literal.of(60.5D));
        assertRewrite(e4, e5);

        // a > 1
        Expression e6 = new GreaterThan(a, Literal.of(1));
        assertRewrite(e6, e6);
        assertRewrite(a, a);

        // a
        assertRewrite(a, a);

        // 1
        Literal one = Literal.of(1);
        assertRewrite(one, one);
    }

    @Test
    public void testTimestampFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(FoldConstantRuleOnFE.INSTANCE));
        String interval = "'1991-05-01' - interval 1 day";
        Expression e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        Expression e8 = Config.enable_date_conversion
                ? new DateTimeV2Literal(1991, 4, 30, 0, 0, 0)
                : new DateTimeLiteral(1991, 4, 30, 0, 0, 0);
        assertRewrite(e7, e8);

        interval = "'1991-05-01' + interval '1' day";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = Config.enable_date_conversion
                ? new DateTimeV2Literal(1991, 5, 2, 0, 0, 0)
                : new DateTimeLiteral(1991, 5, 2, 0, 0, 0);
        assertRewrite(e7, e8);

        interval = "'1991-05-01' + interval 1+1 day";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = Config.enable_date_conversion
                ? new DateTimeV2Literal(1991, 5, 3, 0, 0, 0)
                : new DateTimeLiteral(1991, 5, 3, 0, 0, 0);
        assertRewrite(e7, e8);

        interval = "date '1991-05-01' + interval 10 / 2 + 1 day";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = Config.enable_date_conversion
                ? new DateV2Literal(1991, 5, 7)
                : new DateLiteral(1991, 5, 7);
        assertRewrite(e7, e8);

        interval = "interval '1' day + '1991-05-01'";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = Config.enable_date_conversion
                ? new DateTimeV2Literal(1991, 5, 2, 0, 0, 0)
                : new DateTimeLiteral(1991, 5, 2, 0, 0, 0);
        assertRewrite(e7, e8);

        interval = "interval '3' month + '1991-05-01'";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = Config.enable_date_conversion
                ? new DateTimeV2Literal(1991, 8, 1, 0, 0, 0)
                : new DateTimeLiteral(1991, 8, 1, 0, 0, 0);
        assertRewrite(e7, e8);

        interval = "interval 3 + 1 month + '1991-05-01'";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = Config.enable_date_conversion
                ? new DateTimeV2Literal(1991, 9, 1, 0, 0, 0)
                : new DateTimeLiteral(1991, 9, 1, 0, 0, 0);
        assertRewrite(e7, e8);

        interval = "interval 3 + 1 year + '1991-05-01'";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = Config.enable_date_conversion
                ? new DateTimeV2Literal(1995, 5, 1, 0, 0, 0)
                : new DateTimeLiteral(1995, 5, 1, 0, 0, 0);
        assertRewrite(e7, e8);

        interval = "interval 3 + 3 / 2 hour + '1991-05-01 10:00:00'";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = Config.enable_date_conversion
                ? new DateTimeV2Literal(1991, 5, 1, 14, 0, 0)
                : new DateTimeLiteral(1991, 5, 1, 14, 0, 0);
        assertRewrite(e7, e8);

        interval = "interval 3 * 2 / 3 minute + '1991-05-01 10:00:00'";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = Config.enable_date_conversion
                ? new DateTimeV2Literal(1991, 5, 1, 10, 2, 0)
                : new DateTimeLiteral(1991, 5, 1, 10, 2, 0);
        assertRewrite(e7, e8);

        interval = "interval 3 / 2 + 1 second + '1991-05-01 10:00:00'";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = Config.enable_date_conversion
                ? new DateTimeV2Literal(1991, 5, 1, 10, 0, 2)
                : new DateTimeLiteral(1991, 5, 1, 10, 0, 2);
        assertRewrite(e7, e8);

        // a + interval 1 day
        Slot a = SlotReference.of("a", DateTimeV2Type.SYSTEM_DEFAULT);
        TimestampArithmetic arithmetic = new TimestampArithmetic(Operator.ADD, a, Literal.of(1), TimeUnit.DAY, false);
        Expression process = process(arithmetic);
        assertRewrite(process, process);
    }

    public Expression process(TimestampArithmetic arithmetic) {
        String funcOpName;
        if (arithmetic.getFuncName() == null) {
            funcOpName = String.format("%sS_%s", arithmetic.getTimeUnit(),
                    (arithmetic.getOp() == Operator.ADD) ? "ADD" : "SUB");
        } else {
            funcOpName = arithmetic.getFuncName();
        }
        return arithmetic.withFuncName(funcOpName.toLowerCase(Locale.ROOT));
    }

    @Test
    public void testDateTypeDateTimeArithmeticFunctions() {
        DateLiteral dateLiteral = new DateLiteral("1999-12-31");
        IntegerLiteral integerLiteral = new IntegerLiteral(30);
        VarcharLiteral format = new VarcharLiteral("%Y-%m-%d");

        String[] answer = {
                "'2000-01-30'", "'1999-12-01'", "'2029-12-31'", "'1969-12-31'",
                "'2002-06-30'", "'1997-06-30'", "'2000-07-28'", "'1999-06-04'",
                "'2000-01-30'", "'1999-12-01'",
                "1999", "4", "12", "6", "31", "365", "31",
                "'1999-12-31'", "'1999-12-27'", "'1999-12-31'"
        };
        int answerIdx = 0;

        Assertions.assertEquals(DateTimeArithmetic.dateAdd(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.dateSub(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.yearsAdd(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.yearsSub(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.monthsAdd(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.monthsSub(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.weeksAdd(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.weeksSub(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.daysAdd(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.daysSub(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);

        Assertions.assertEquals(DateTimeExtractAndTransform.year(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.quarter(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.month(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.dayOfWeek(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.dayOfMonth(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.dayOfYear(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.day(dateLiteral).toSql(), answer[answerIdx++]);

        Assertions.assertEquals(DateTimeExtractAndTransform.dateFormat(dateLiteral, format).toSql(),
                answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.toMonday(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.lastDay(dateLiteral).toSql(), answer[answerIdx]);
    }

    @Test
    public void testDateTimeTypeDateTimeArithmeticFunctions() {
        DateTimeLiteral dateLiteral = new DateTimeLiteral("1999-12-31 23:59:59");
        IntegerLiteral integerLiteral = new IntegerLiteral(30);
        VarcharLiteral format = new VarcharLiteral("%Y-%m-%d");

        String[] answer = {
                "'2000-01-30 23:59:59'", "'1999-12-01 23:59:59'", "'2029-12-31 23:59:59'", "'1969-12-31 23:59:59'",
                "'2002-06-30 23:59:59'", "'1997-06-30 23:59:59'", "'2000-01-30 23:59:59'", "'1999-12-01 23:59:59'",
                "'2000-01-02 05:59:59'", "'1999-12-30 17:59:59'", "'2000-01-01 00:29:59'",
                "'1999-12-31 23:29:59'", "'2000-01-01 00:00:29'", "'1999-12-31 23:59:29'",
                "1999", "4", "12", "6", "31", "365", "31", "23", "59", "59",
                "'1999-12-31'", "'1999-12-27'", "'1999-12-31'", "'1999-12-31'", "730484", "'1999-12-31'"
        };
        int answerIdx = 0;

        Assertions.assertEquals(DateTimeArithmetic.dateAdd(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.dateSub(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.yearsAdd(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.yearsSub(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.monthsAdd(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.monthsSub(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.daysAdd(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.daysSub(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.hoursAdd(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.hoursSub(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.minutesAdd(dateLiteral, integerLiteral).toSql(),
                answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.minutesSub(dateLiteral, integerLiteral).toSql(),
                answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.secondsAdd(dateLiteral, integerLiteral).toSql(),
                answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.secondsSub(dateLiteral, integerLiteral).toSql(),
                answer[answerIdx++]);

        Assertions.assertEquals(DateTimeExtractAndTransform.year(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.quarter(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.month(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.dayOfWeek(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.dayOfMonth(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.dayOfYear(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.day(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.hour(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.minute(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.second(dateLiteral).toSql(), answer[answerIdx++]);

        Assertions.assertEquals(DateTimeExtractAndTransform.dateFormat(dateLiteral, format).toSql(),
                answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.toMonday(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.lastDay(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.toDate(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.toDays(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.date(dateLiteral).toSql(), answer[answerIdx]);
    }

    @Test
    public void testDateV2TypeDateTimeArithmeticFunctions() {
        DateV2Literal dateLiteral = new DateV2Literal("1999-12-31");
        IntegerLiteral integerLiteral = new IntegerLiteral(30);
        VarcharLiteral format = new VarcharLiteral("%Y-%m-%d");

        String[] answer = {
                "'2000-01-30'", "'1999-12-01'", "'2029-12-31'", "'1969-12-31'",
                "'2002-06-30'", "'1997-06-30'", "'2000-01-30'", "'1999-12-01'",
                "1999", "4", "12", "6", "31", "365", "31",
                "'1999-12-31'", "'1999-12-27'", "'1999-12-31'"
        };
        int answerIdx = 0;

        Assertions.assertEquals(DateTimeArithmetic.dateAdd(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.dateSub(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.yearsAdd(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.yearsSub(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.monthsAdd(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.monthsSub(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.daysAdd(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.daysSub(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);

        Assertions.assertEquals(DateTimeExtractAndTransform.year(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.quarter(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.month(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.dayOfWeek(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.dayOfMonth(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.dayOfYear(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.day(dateLiteral).toSql(), answer[answerIdx++]);

        Assertions.assertEquals(DateTimeExtractAndTransform.dateFormat(dateLiteral, format).toSql(),
                answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.toMonday(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.lastDay(dateLiteral).toSql(), answer[answerIdx]);
    }

    @Test
    public void testDateTimeV2TypeDateTimeArithmeticFunctions() {
        DateTimeV2Literal dateLiteral = new DateTimeV2Literal(DateTimeV2Type.SYSTEM_DEFAULT, "1999-12-31 23:59:59");
        IntegerLiteral integerLiteral = new IntegerLiteral(30);
        VarcharLiteral format = new VarcharLiteral("%Y-%m-%d");

        String[] answer = {
                "'2000-01-30 23:59:59'", "'1999-12-01 23:59:59'", "'2029-12-31 23:59:59'", "'1969-12-31 23:59:59'",
                "'2002-06-30 23:59:59'", "'1997-06-30 23:59:59'", "'2000-01-30 23:59:59'", "'1999-12-01 23:59:59'",
                "'2000-01-02 05:59:59'", "'1999-12-30 17:59:59'", "'2000-01-01 00:29:59'",
                "'1999-12-31 23:29:59'", "'2000-01-01 00:00:29'", "'1999-12-31 23:59:29'", "'1999-12-31 23:59:59'",
                "1999", "4", "12", "6", "31", "365", "31", "23", "59", "59",
                "'1999-12-31'", "'1999-12-27'", "'1999-12-31'", "'1999-12-31'", "730484", "'1999-12-31'", "'1999-12-31'"
        };
        int answerIdx = 0;

        Assertions.assertEquals(DateTimeArithmetic.dateAdd(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.dateSub(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.yearsAdd(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.yearsSub(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.monthsAdd(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.monthsSub(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.daysAdd(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.daysSub(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.hoursAdd(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.hoursSub(dateLiteral, integerLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.minutesAdd(dateLiteral, integerLiteral).toSql(),
                answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.minutesSub(dateLiteral, integerLiteral).toSql(),
                answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.secondsAdd(dateLiteral, integerLiteral).toSql(),
                answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.secondsSub(dateLiteral, integerLiteral).toSql(),
                answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.microSecondsAdd(dateLiteral, integerLiteral).toSql(),
                answer[answerIdx++]);

        Assertions.assertEquals(DateTimeExtractAndTransform.year(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.quarter(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.month(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.dayOfWeek(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.dayOfMonth(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.dayOfYear(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.day(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.hour(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.minute(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.second(dateLiteral).toSql(), answer[answerIdx++]);

        Assertions.assertEquals(DateTimeExtractAndTransform.dateFormat(dateLiteral, format).toSql(),
                answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.toMonday(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.lastDay(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.toDate(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.toDays(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.date(dateLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.dateV2(dateLiteral).toSql(), answer[answerIdx]);

        Assertions.assertEquals("'2021 52 2022 01'", DateTimeExtractAndTransform.dateFormat(
                new DateLiteral("2022-01-01 00:12:42"),
                new VarcharLiteral("%x %v %X %V")).toSql());
        Assertions.assertEquals("'2023 18 2023 19'", DateTimeExtractAndTransform.dateFormat(
                new DateLiteral("2023-05-07 02:41:42"),
                new VarcharLiteral("%x %v %X %V")).toSql());
    }

    @Test
    public void testDateDiff() {
        DateTimeLiteral dateTimeLiteral = new DateTimeLiteral("2001-12-31 00:00:01");
        DateV2Literal dateV2Literal = new DateV2Literal("2001-12-31");
        DateTimeV2Literal dateTimeV2Literal = new DateTimeV2Literal("2001-12-31 00:00:01");

        DateTimeLiteral dateTimeLiteral1 = new DateTimeLiteral("2006-12-31 00:00:01");
        DateV2Literal dateV2Literal1 = new DateV2Literal("2006-12-31");
        DateTimeV2Literal dateTimeV2Literal1 = new DateTimeV2Literal("2006-12-31 01:00:01");

        Assertions.assertEquals(DateTimeArithmetic.dateDiff(dateTimeLiteral, dateTimeLiteral1).toSql(), "-1826");
        Assertions.assertEquals(DateTimeArithmetic.dateDiff(dateTimeLiteral1, dateTimeLiteral).toSql(), "1826");
        Assertions.assertEquals(DateTimeArithmetic.dateDiff(dateV2Literal, dateV2Literal1).toSql(), "-1826");
        Assertions.assertEquals(DateTimeArithmetic.dateDiff(dateV2Literal1, dateV2Literal).toSql(), "1826");
        Assertions.assertEquals(DateTimeArithmetic.dateDiff(dateV2Literal, dateTimeV2Literal1).toSql(), "-1826");
        Assertions.assertEquals(DateTimeArithmetic.dateDiff(dateTimeV2Literal1, dateV2Literal).toSql(), "1826");
        Assertions.assertEquals(DateTimeArithmetic.dateDiff(dateTimeV2Literal, dateV2Literal1).toSql(), "-1826");
        Assertions.assertEquals(DateTimeArithmetic.dateDiff(dateV2Literal1, dateTimeV2Literal).toSql(), "1826");
        Assertions.assertEquals(DateTimeArithmetic.dateDiff(dateTimeV2Literal, dateTimeV2Literal1).toSql(), "-1826");
        Assertions.assertEquals(DateTimeArithmetic.dateDiff(dateTimeV2Literal1, dateTimeV2Literal).toSql(), "1826");
    }

    @Test
    public void testDateTrunc() {
        DateTimeLiteral dateTimeLiteral = new DateTimeLiteral("2001-12-31 01:01:01");
        DateTimeV2Literal dateTimeV2Literal = new DateTimeV2Literal("2001-12-31 01:01:01");

        String[] tags = {"year", "month", "day", "hour", "minute", "second"};

        String[] answer = {
                "'2001-01-01 00:00:00'", "'2001-01-01 00:00:00'", "'2001-12-01 00:00:00'", "'2001-12-01 00:00:00'",
                "'2001-12-31 00:00:00'", "'2001-12-31 00:00:00'", "'2001-12-31 01:00:00'", "'2001-12-31 01:00:00'",
                "'2001-12-31 01:01:00'", "'2001-12-31 01:01:00'", "'2001-12-31 01:01:01'", "'2001-12-31 01:01:01'",
                "'2001-01-01 00:00:00'", "'2001-01-01 00:00:00'", "'2001-01-01 00:00:00'",
                "'2001-04-01 00:00:00'", "'2001-04-01 00:00:00'", "'2001-04-01 00:00:00'",
                "'2001-07-01 00:00:00'", "'2001-07-01 00:00:00'", "'2001-07-01 00:00:00'",
                "'2001-10-01 00:00:00'", "'2001-10-01 00:00:00'", "'2001-10-01 00:00:00'",
                "'2001-01-15 00:00:00'", "'2001-02-12 00:00:00'", "'2001-03-12 00:00:00'",
        };
        int answerIdx = 0;

        for (String tag : tags) {
            Assertions.assertEquals(DateTimeExtractAndTransform.dateTrunc(dateTimeLiteral, new VarcharLiteral(tag)).toSql(),
                    answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.dateTrunc(dateTimeV2Literal, new VarcharLiteral(tag)).toSql(),
                    answer[answerIdx++]);
        }

        VarcharLiteral quarter = new VarcharLiteral("quarter");
        for (int i = 1; i != 13; ++i) {
            Assertions.assertEquals(DateTimeExtractAndTransform.dateTrunc(
                    new DateTimeLiteral(2001, i, 15, 0, 0, 0), quarter).toSql(), answer[answerIdx++]);
        }
        VarcharLiteral week = new VarcharLiteral("week");
        for (int i = 1; i != 4; ++i) {
            Assertions.assertEquals(DateTimeExtractAndTransform.dateTrunc(
                    new DateTimeLiteral(2001, i, 15, 0, 0, 0), week).toSql(), answer[answerIdx++]);
        }
    }

    @Test
    public void testDateConstructFunction() {
        String[] answer = {
                "'2001-07-19'", "'6411-08-17'", "'0000-01-01'", "'1977-06-03 17:57:24'",
                "'1977-06-03'", "1008909293", "1008864000"
        };
        int answerIdx = 0;

        Assertions.assertEquals(DateTimeExtractAndTransform.makeDate(
                new IntegerLiteral(2001),
                new IntegerLiteral(200)
        ).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.fromDays(
                new IntegerLiteral(2341798)
        ).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.fromDays(
                new IntegerLiteral(1)
        ).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.fromUnixTime(
                new IntegerLiteral(234179844)
        ).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.fromUnixTime(
                new IntegerLiteral(234179844),
                new VarcharLiteral("%Y-%m-%d")
        ).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.unixTimestamp(
                new DateTimeLiteral("2001-12-21 12:34:53")
        ).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.unixTimestamp(
                new VarcharLiteral("2001-12-21"),
                new VarcharLiteral("%Y-%m-%d")
        ).toSql(), answer[answerIdx]);
    }

    @Test
    public void testFoldNestedExpression() {
        assertRewriteExpression("makedate(year('2010-04-10'), dayofyear('2010-04-11'))", "'2010-04-11'");
        assertRewriteExpression("null in ('d', null)", "NULL");
        assertRewriteExpression("null not in ('d', null)", "NULL");
        assertRewriteExpression("'a' in ('d', null)", "NULL");
        assertRewriteExpression("'a' not in ('d', null)", "NULL");
        assertRewriteExpression("'a' in ('d', 'c')", "FALSE");
        assertRewriteExpression("'a' not in ('d', 'c')", "TRUE");
        assertRewriteExpression("'d' in ('d', 'c')", "TRUE");
        assertRewriteExpression("'d' not in ('d', 'c')", "FALSE");
        assertRewriteExpression("1 in (2, NULL, 3)", "NULL");
    }

    @Test
    public void testFoldTypeOfNullLiteral() {
        String actualExpression = "append_trailing_char_if_absent(cast(version() as varchar), cast(null as varchar))";
        ExpressionRewriteContext context = new ExpressionRewriteContext(
                MemoTestUtils.createCascadesContext(new UnboundRelation(new RelationId(1), ImmutableList.of("test_table"))));
        NereidsParser parser = new NereidsParser();
        Expression e1 = parser.parseExpression(actualExpression);
        e1 = new ExpressionNormalization().rewrite(FunctionBinder.INSTANCE.rewrite(e1, context), context);
        Assertions.assertTrue(e1.getDataType() instanceof VarcharType);
    }

    private void assertRewriteExpression(String actualExpression, String expectedExpression) {
        ExpressionRewriteContext context = new ExpressionRewriteContext(
                MemoTestUtils.createCascadesContext(new UnboundRelation(new RelationId(1), ImmutableList.of("test_table"))));

        NereidsParser parser = new NereidsParser();
        Expression e1 = parser.parseExpression(actualExpression);
        e1 = new ExpressionNormalization().rewrite(FunctionBinder.INSTANCE.rewrite(e1, context), context);
        Assertions.assertEquals(expectedExpression, e1.toSql());
    }
}
