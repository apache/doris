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
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.NotSupportedException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRule;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.rules.expression.rules.SimplifyConditionalFunction;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.TimestampArithmetic;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeArithmetic;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeExtractAndTransform;
import org.apache.doris.nereids.trees.expressions.functions.executable.TimeRoundSeries;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Acos;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Acosh;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AppendTrailingCharIfAbsent;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Asin;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Asinh;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Atan;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Atanh;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Bin;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitCount;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Cbrt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Ceil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CharacterLength;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ConvertTz;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Cos;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Cosh;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Cot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Csc;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateFormat;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Degrees;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Dexp;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Dlog10;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Dsqrt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Even;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Exp;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Floor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Fmod;
import org.apache.doris.nereids.trees.expressions.functions.scalar.FromUnixtime;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HoursAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Left;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Ln;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Locate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Log;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Log10;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Log2;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MicroSecondsAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MilliSecondsAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MinutesAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthsBetween;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NextDay;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Overlay;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Power;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Radians;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ReplaceEmpty;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Right;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Round;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Sec;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SecondsAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Sign;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Sin;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Sinh;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Soundex;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Sqrt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StrToDate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Substring;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Tan;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Tanh;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ToDays;
import org.apache.doris.nereids.trees.expressions.functions.scalar.UnixTimestamp;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.ComparableLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Interval.TimeUnit;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.MemoTestUtils;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Locale;

class FoldConstantTest extends ExpressionRewriteTestHelper {

    @Test
    void testCaseWhenFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(FoldConstantRuleOnFE.VISITOR_INSTANCE)
        ));
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

        // make sure the case when return datetime(6)
        Expression analyzedCaseWhen = ExpressionAnalyzer.analyzeFunction(null, null, PARSER.parseExpression(
                "case when true then cast('2025-04-17' as datetime(0)) else cast('2025-04-18 01:02:03.123456' as datetime(6)) end"));
        Assertions.assertEquals(DateTimeV2Type.of(6), analyzedCaseWhen.getDataType());
        Assertions.assertEquals(DateTimeV2Type.of(6), ((CaseWhen) analyzedCaseWhen).getWhenClauses().get(0).getResult().getDataType());
        Assertions.assertEquals(DateTimeV2Type.of(6), ((CaseWhen) analyzedCaseWhen).getDefaultValue().get().getDataType());
        Expression foldCaseWhen = executor.rewrite(analyzedCaseWhen, context);
        Assertions.assertEquals(new DateTimeV2Literal(DateTimeV2Type.of(6), "2025-04-17"), foldCaseWhen);
    }

    @Test
    void testInFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(FoldConstantRuleOnFE.VISITOR_INSTANCE)
        ));
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
    void testLogicalFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(FoldConstantRuleOnFE.VISITOR_INSTANCE)
        ));
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
    void testIsNullFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(FoldConstantRuleOnFE.VISITOR_INSTANCE)
        ));
        assertRewriteAfterTypeCoercion("100 is null", "false");
        assertRewriteAfterTypeCoercion("null is null", "true");
        assertRewriteAfterTypeCoercion("null is not null", "false");
        assertRewriteAfterTypeCoercion("100 is not null", "true");
        assertRewriteAfterTypeCoercion("IA is not null", "IA is not null");
        assertRewriteAfterTypeCoercion("IA is null", "IA is null");
    }

    @Test
    void testNotPredicateFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(FoldConstantRuleOnFE.VISITOR_INSTANCE)
        ));
        assertRewriteAfterTypeCoercion("not 1 > 2", "true");
        assertRewriteAfterTypeCoercion("not null + 1 > 2", "null");
        assertRewriteAfterTypeCoercion("not (1 + 5) / 2 + (10 - 1) * 3 > 3 * 5 + 1", "false");
    }

    @Test
    void testCastFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(FoldConstantRuleOnFE.VISITOR_INSTANCE)
        ));

        // cast '1' as tinyint
        Cast c = new Cast(Literal.of("1"), TinyIntType.INSTANCE);
        Expression rewritten = executor.rewrite(c, context);
        Literal expected = Literal.of((byte) 1);
        Assertions.assertEquals(rewritten, expected);
    }

    @Test
    void testFoldDate() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
            bottomUp(FoldConstantRuleOnFE.VISITOR_INSTANCE)
        ));

        HoursAdd hoursAdd = new HoursAdd(DateTimeV2Literal.fromJavaDateType(LocalDateTime.of(1, 1, 1, 1, 1, 1), 0),
                new IntegerLiteral(1));
        Expression rewritten = executor.rewrite(hoursAdd, context);
        Assertions.assertEquals(new DateTimeV2Literal("0001-01-01 02:01:01"), rewritten);
        // fail to fold, because the result is out of range
        hoursAdd = new HoursAdd(DateTimeV2Literal.fromJavaDateType(LocalDateTime.of(9999, 12, 31, 23, 1, 1), 0),
                new IntegerLiteral(24));
        rewritten = executor.rewrite(hoursAdd, context);
        Assertions.assertEquals(hoursAdd, rewritten);
        hoursAdd = new HoursAdd(DateTimeV2Literal.fromJavaDateType(LocalDateTime.of(0, 1, 1, 1, 1, 1), 0),
                new IntegerLiteral(-25));
        rewritten = executor.rewrite(hoursAdd, context);
        Assertions.assertEquals(hoursAdd, rewritten);

        MinutesAdd minutesAdd = new MinutesAdd(
                        DateTimeV2Literal.fromJavaDateType(LocalDateTime.of(1, 1, 1, 1, 1, 1), 0),
                        new BigIntLiteral(1));
        rewritten = executor.rewrite(minutesAdd, context);
        Assertions.assertEquals(new DateTimeV2Literal("0001-01-01 01:02:01"), rewritten);
        // fail to fold, because the result is out of range
        minutesAdd = new MinutesAdd(DateTimeV2Literal.fromJavaDateType(LocalDateTime.of(9999, 12, 31, 23, 59, 1), 0),
                new BigIntLiteral(1440));
        rewritten = executor.rewrite(minutesAdd, context);
        Assertions.assertEquals(minutesAdd, rewritten);
        minutesAdd = new MinutesAdd(DateTimeV2Literal.fromJavaDateType(LocalDateTime.of(0, 1, 1, 0, 1, 1), 0),
                new BigIntLiteral(-2));
        rewritten = executor.rewrite(minutesAdd, context);
        Assertions.assertEquals(minutesAdd, rewritten);

        SecondsAdd secondsAdd = new SecondsAdd(
                        DateTimeV2Literal.fromJavaDateType(LocalDateTime.of(1, 1, 1, 1, 1, 1), 0),
                        new BigIntLiteral(1));
        rewritten = executor.rewrite(secondsAdd, context);
        Assertions.assertEquals(new DateTimeV2Literal("0001-01-01 01:01:02"), rewritten);
        // fail to fold, because the result is out of range
        secondsAdd = new SecondsAdd(DateTimeV2Literal.fromJavaDateType(LocalDateTime.of(9999, 12, 31, 23, 59, 59), 0),
                new BigIntLiteral(86400));
        rewritten = executor.rewrite(secondsAdd, context);
        Assertions.assertEquals(secondsAdd, rewritten);
        secondsAdd = new SecondsAdd(DateTimeV2Literal.fromJavaDateType(LocalDateTime.of(0, 1, 1, 0, 1, 1), 0),
                new BigIntLiteral(-61000));
        rewritten = executor.rewrite(secondsAdd, context);
        Assertions.assertEquals(secondsAdd, rewritten);

        MilliSecondsAdd millisecondsAdd = new MilliSecondsAdd(
                        DateTimeV2Literal.fromJavaDateType(LocalDateTime.of(1, 1, 1, 1, 1, 1)), new BigIntLiteral(1));
        rewritten = executor.rewrite(millisecondsAdd, context);
        Assertions.assertEquals(new DateTimeV2Literal(DateTimeV2Type.MAX, "0001-01-01 01:01:01.001000"), rewritten);
        // fail to fold, because the result is out of range
        millisecondsAdd = new MilliSecondsAdd(
                        DateTimeV2Literal.fromJavaDateType(LocalDateTime.of(9999, 12, 31, 23, 59, 59)),
                        new BigIntLiteral(86400000));
        rewritten = executor.rewrite(millisecondsAdd, context);
        Assertions.assertEquals(millisecondsAdd, rewritten);
        millisecondsAdd = new MilliSecondsAdd(DateTimeV2Literal.fromJavaDateType(LocalDateTime.of(0, 1, 1, 0, 1, 1)),
                        new BigIntLiteral(-61000000000000L));
        rewritten = executor.rewrite(millisecondsAdd, context);
        Assertions.assertEquals(millisecondsAdd, rewritten);

        MicroSecondsAdd microsecondsAdd = new MicroSecondsAdd(
                        DateTimeV2Literal.fromJavaDateType(LocalDateTime.of(1, 1, 1, 1, 1, 1)), new BigIntLiteral(1));
        rewritten = executor.rewrite(microsecondsAdd, context);
        Assertions.assertEquals(new DateTimeV2Literal("0001-01-01 01:01:01.000001"), rewritten);
        // fail to fold, because the result is out of range
        microsecondsAdd = new MicroSecondsAdd(
                        DateTimeV2Literal.fromJavaDateType(LocalDateTime.of(9999, 12, 31, 23, 59, 59)),
                        new BigIntLiteral(86400000000L));
        rewritten = executor.rewrite(microsecondsAdd, context);
        Assertions.assertEquals(microsecondsAdd, rewritten);
        microsecondsAdd = new MicroSecondsAdd(DateTimeV2Literal.fromJavaDateType(LocalDateTime.of(0, 1, 1, 0, 1, 1)),
                        new BigIntLiteral(-61000000000000L));
        rewritten = executor.rewrite(microsecondsAdd, context);
        Assertions.assertEquals(microsecondsAdd, rewritten);

        ToDays toDays = new ToDays(DateLiteral.fromJavaDateType(LocalDateTime.of(1, 1, 1, 1, 1, 1)));
        rewritten = executor.rewrite(toDays, context);
        Assertions.assertEquals(new IntegerLiteral(366), rewritten);
        toDays = new ToDays(DateV2Literal.fromJavaDateType(LocalDateTime.of(1, 1, 1, 1, 1, 1)));
        rewritten = executor.rewrite(toDays, context);
        Assertions.assertEquals(new IntegerLiteral(366), rewritten);
        toDays = new ToDays(DateV2Literal.fromJavaDateType(LocalDateTime.of(9999, 12, 31, 1, 1, 1)));
        rewritten = executor.rewrite(toDays, context);
        Assertions.assertEquals(new IntegerLiteral(3652424), rewritten);

        MonthsBetween monthsBetween = new MonthsBetween(
                        DateV2Literal.fromJavaDateType(LocalDateTime.of(1, 1, 1, 1, 1, 1)),
                        DateV2Literal.fromJavaDateType(LocalDateTime.of(2, 2, 1, 1, 1, 1)));
        rewritten = executor.rewrite(monthsBetween, context);
        Assertions.assertEquals(new DoubleLiteral(-13.0), rewritten);
        monthsBetween = new MonthsBetween(DateV2Literal.fromJavaDateType(LocalDateTime.of(1, 1, 1, 1, 1, 1)),
                        DateV2Literal.fromJavaDateType(LocalDateTime.of(2, 2, 1, 1, 1, 1)), BooleanLiteral.FALSE);
        rewritten = executor.rewrite(monthsBetween, context);
        Assertions.assertEquals(new DoubleLiteral(-13.0), rewritten);
        monthsBetween = new MonthsBetween(DateV2Literal.fromJavaDateType(LocalDateTime.of(2024, 3, 31, 1, 1, 1)),
                        DateV2Literal.fromJavaDateType(LocalDateTime.of(2024, 2, 29, 1, 1, 1)));
        rewritten = executor.rewrite(monthsBetween, context);
        Assertions.assertEquals(new DoubleLiteral(1.0), rewritten);
        NextDay nextDay = new NextDay(DateV2Literal.fromJavaDateType(LocalDateTime.of(2020, 1, 28, 1, 1, 1)),
                        StringLiteral.of("MON"));
        rewritten = executor.rewrite(nextDay, context);
        Assertions.assertEquals(new DateV2Literal("2020-02-03"), rewritten);
        nextDay = new NextDay(DateV2Literal.fromJavaDateType(LocalDateTime.of(2020, 1, 31, 1, 1, 1)),
                        StringLiteral.of("SAT"));
        rewritten = executor.rewrite(nextDay, context);
        Assertions.assertEquals(new DateV2Literal("2020-02-01"), rewritten);
        nextDay = new NextDay(DateV2Literal.fromJavaDateType(LocalDateTime.of(2020, 12, 28, 1, 1, 1)),
                        StringLiteral.of("FRI"));
        rewritten = executor.rewrite(nextDay, context);
        Assertions.assertEquals(new DateV2Literal("2021-01-01"), rewritten);
        nextDay = new NextDay(DateV2Literal.fromJavaDateType(LocalDateTime.of(2020, 12, 31, 1, 1, 1)),
                        StringLiteral.of("THU"));
        rewritten = executor.rewrite(nextDay, context);
        Assertions.assertEquals(new DateV2Literal("2021-01-07"), rewritten);
        nextDay = new NextDay(DateV2Literal.fromJavaDateType(LocalDateTime.of(2020, 2, 27, 1, 1, 1)),
                        StringLiteral.of("SAT"));
        rewritten = executor.rewrite(nextDay, context);
        Assertions.assertEquals(new DateV2Literal("2020-02-29"), rewritten);
        nextDay = new NextDay(DateV2Literal.fromJavaDateType(LocalDateTime.of(2020, 2, 29, 1, 1, 1)),
                        StringLiteral.of("MON"));
        rewritten = executor.rewrite(nextDay, context);
        Assertions.assertEquals(new DateV2Literal("2020-03-02"), rewritten);
        nextDay = new NextDay(DateV2Literal.fromJavaDateType(LocalDateTime.of(2019, 2, 26, 1, 1, 1)),
                        StringLiteral.of("THU"));
        rewritten = executor.rewrite(nextDay, context);
        Assertions.assertEquals(new DateV2Literal("2019-02-28"), rewritten);
        nextDay = new NextDay(DateV2Literal.fromJavaDateType(LocalDateTime.of(2019, 2, 28, 1, 1, 1)),
                        StringLiteral.of("SUN"));
        rewritten = executor.rewrite(nextDay, context);
        Assertions.assertEquals(new DateV2Literal("2019-03-03"), rewritten);
        nextDay = new NextDay(DateV2Literal.fromJavaDateType(LocalDateTime.of(2020, 4, 29, 1, 1, 1)),
                        StringLiteral.of("FRI"));
        rewritten = executor.rewrite(nextDay, context);
        Assertions.assertEquals(new DateV2Literal("2020-05-01"), rewritten);
        nextDay = new NextDay(DateV2Literal.fromJavaDateType(LocalDateTime.of(2020, 5, 31, 1, 1, 1)),
                        StringLiteral.of("MON"));
        rewritten = executor.rewrite(nextDay, context);
        Assertions.assertEquals(new DateV2Literal("2020-06-01"), rewritten);
    }

    @Test
    void testFoldString() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(FoldConstantRuleOnFE.VISITOR_INSTANCE)
        ));
        ConvertTz c = new ConvertTz(DateTimeV2Literal.fromJavaDateType(LocalDateTime.of(1, 1, 1, 1, 1, 1)),
                StringLiteral.of("Asia/Shanghai"), StringLiteral.of("GMT"));
        Expression rewritten = executor.rewrite(c, context);
        Assertions.assertTrue(new DateTimeV2Literal("0000-12-31 16:55:18.000000").compareTo((ComparableLiteral) rewritten) == 0);
        c = new ConvertTz(DateTimeV2Literal.fromJavaDateType(LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999999000)),
                        StringLiteral.of("Pacific/Galapagos"), StringLiteral.of("Pacific/Galapagos"));
        rewritten = executor.rewrite(c, context);
        Assertions.assertTrue(new DateTimeV2Literal("9999-12-31 23:59:59.999999").compareTo((ComparableLiteral) rewritten) == 0);

        DateFormat d = new DateFormat(DateTimeLiteral.fromJavaDateType(LocalDateTime.of(1, 1, 1, 1, 1, 1)),
                StringLiteral.of("%y %m %d"));
        rewritten = executor.rewrite(d, context);
        Assertions.assertEquals(new VarcharLiteral("01 01 01"), rewritten);
        d = new DateFormat(DateTimeV2Literal.fromJavaDateType(LocalDateTime.of(1, 1, 1, 1, 1, 1)),
                StringLiteral.of("%y %m %d"));
        rewritten = executor.rewrite(d, context);
        Assertions.assertEquals(new VarcharLiteral("01 01 01"), rewritten);
        d = new DateFormat(DateLiteral.fromJavaDateType(LocalDateTime.of(1, 1, 1, 1, 1, 1)),
                StringLiteral.of("%y %m %d"));
        rewritten = executor.rewrite(d, context);
        Assertions.assertEquals(new VarcharLiteral("01 01 01"), rewritten);
        d = new DateFormat(DateV2Literal.fromJavaDateType(LocalDateTime.of(1, 1, 1, 1, 1, 1)),
                StringLiteral.of("%y %m %d"));
        rewritten = executor.rewrite(d, context);
        Assertions.assertEquals(new VarcharLiteral("01 01 01"), rewritten);

        DateTrunc t = new DateTrunc(DateTimeLiteral.fromJavaDateType(LocalDateTime.of(1, 1, 1, 1, 1, 1)),
                StringLiteral.of("week"));
        rewritten = executor.rewrite(t, context);
        Assertions.assertEquals(new DateTimeLiteral("0001-01-01 00:00:00"), rewritten);
        t = new DateTrunc(DateTimeV2Literal.fromJavaDateType(LocalDateTime.of(1, 1, 1, 1, 1, 1)),
                StringLiteral.of("week"));
        rewritten = executor.rewrite(t, context);
        Assertions.assertTrue(((ComparableLiteral) rewritten).compareTo(new DateTimeV2Literal("0001-01-01 00:00:00.000000")) == 0);
        t = new DateTrunc(DateLiteral.fromJavaDateType(LocalDateTime.of(1, 1, 1, 1, 1, 1)),
                StringLiteral.of("week"));
        rewritten = executor.rewrite(t, context);
        Assertions.assertEquals(new DateLiteral("0001-01-01"), rewritten);
        t = new DateTrunc(DateV2Literal.fromJavaDateType(LocalDateTime.of(1, 1, 1, 1, 1, 1)),
                StringLiteral.of("week"));
        rewritten = executor.rewrite(t, context);
        Assertions.assertEquals(new DateV2Literal("0001-01-01"), rewritten);

        FromUnixtime f = new FromUnixtime(BigIntLiteral.of(123456789L), StringLiteral.of("%y %m %d"));
        rewritten = executor.rewrite(f, context);
        Assertions.assertEquals(new VarcharLiteral("73 11 30"), rewritten);

        UnixTimestamp ut = new UnixTimestamp(StringLiteral.of("2021-11-11"), StringLiteral.of("%Y-%m-%d"));
        rewritten = executor.rewrite(ut, context);
        Assertions.assertEquals(new DecimalV3Literal(DecimalV3Type.createDecimalV3TypeNoCheck(18, 6),
                        new BigDecimal("1636560000.000000")), rewritten);

        StrToDate sd = new StrToDate(StringLiteral.of("2021-11-11"), StringLiteral.of("%Y-%m-%d"));
        rewritten = executor.rewrite(sd, context);
        Assertions.assertEquals(new DateV2Literal("2021-11-11"), rewritten);

        AppendTrailingCharIfAbsent a = new AppendTrailingCharIfAbsent(StringLiteral.of("1"), StringLiteral.of("3"));
        rewritten = executor.rewrite(a, context);
        Assertions.assertEquals(new StringLiteral("13"), rewritten);

        Left left = new Left(StringLiteral.of("hello world"), IntegerLiteral.of(5));
        rewritten = executor.rewrite(left, context);
        Assertions.assertEquals(new StringLiteral("hello"), rewritten);
        left = new Left(StringLiteral.of("test"), IntegerLiteral.of(10));
        rewritten = executor.rewrite(left, context);
        Assertions.assertEquals(new StringLiteral("test"), rewritten);
        left = new Left(StringLiteral.of("data"), IntegerLiteral.of(0));
        rewritten = executor.rewrite(left, context);
        Assertions.assertEquals(new StringLiteral(""), rewritten);
        left = new Left(StringLiteral.of("data"), IntegerLiteral.of(-3));
        rewritten = executor.rewrite(left, context);
        Assertions.assertEquals(new StringLiteral(""), rewritten);

        Right right = new Right(StringLiteral.of("hello world"), IntegerLiteral.of(5));
        rewritten = executor.rewrite(right, context);
        Assertions.assertEquals(new StringLiteral("world"), rewritten);
        right = new Right(StringLiteral.of("test"), IntegerLiteral.of(10));
        rewritten = executor.rewrite(right, context);
        Assertions.assertEquals(new StringLiteral("test"), rewritten);
        right = new Right(StringLiteral.of("data"), IntegerLiteral.of(0));
        rewritten = executor.rewrite(right, context);
        Assertions.assertEquals(new StringLiteral(""), rewritten);
        right = new Right(StringLiteral.of("data"), IntegerLiteral.of(-3));
        rewritten = executor.rewrite(right, context);
        Assertions.assertEquals(new StringLiteral("ta"), rewritten);

        Substring substr = new Substring(
                StringLiteral.of("database"),
                IntegerLiteral.of(1),
                IntegerLiteral.of(4)
        );
        rewritten = executor.rewrite(substr, context);
        Assertions.assertEquals(new StringLiteral("data"), rewritten);
        substr = new Substring(
            StringLiteral.of("database"),
                IntegerLiteral.of(-4),
                IntegerLiteral.of(4)
        );
        rewritten = executor.rewrite(substr, context);
        Assertions.assertEquals(new StringLiteral("base"), rewritten);
        substr = new Substring(
                StringLiteral.of("example"),
                IntegerLiteral.of(3),
                IntegerLiteral.of(10)
        );
        rewritten = executor.rewrite(substr, context);
        Assertions.assertEquals(new StringLiteral("ample"), rewritten);

        Locate locate = new Locate(
                StringLiteral.of("world"),
                StringLiteral.of("hello world")
        );
        rewritten = executor.rewrite(locate, context);
        Assertions.assertEquals(new IntegerLiteral(7), rewritten);
        locate = new Locate(
                StringLiteral.of("test"),
                StringLiteral.of("hello world")
        );
        rewritten = executor.rewrite(locate, context);
        Assertions.assertEquals(new IntegerLiteral(0), rewritten);
        locate = new Locate(
                StringLiteral.of("l"),
                StringLiteral.of("hello world"),
                IntegerLiteral.of(3)
        );
        rewritten = executor.rewrite(locate, context);
        Assertions.assertEquals(new IntegerLiteral(3), rewritten);

        CharacterLength len = new CharacterLength(StringLiteral.of("hello"));
        rewritten = executor.rewrite(len, context);
        Assertions.assertEquals(new IntegerLiteral(5), rewritten);
        len = new CharacterLength(StringLiteral.of(""));
        rewritten = executor.rewrite(len, context);
        Assertions.assertEquals(new IntegerLiteral(0), rewritten);
        len = new CharacterLength(StringLiteral.of("😊"));
        rewritten = executor.rewrite(len, context);
        Assertions.assertEquals(new IntegerLiteral(1), rewritten);

        Overlay overlay = new Overlay(
                StringLiteral.of("snow"),
                IntegerLiteral.of(2),
                IntegerLiteral.of(2),
                StringLiteral.of("new")
        );
        rewritten = executor.rewrite(overlay, context);
        Assertions.assertEquals(new StringLiteral("sneww"), rewritten);
        overlay = new Overlay(
                StringLiteral.of("snow"),
                IntegerLiteral.of(2),
                IntegerLiteral.of(0),
                StringLiteral.of("n")
        );
        rewritten = executor.rewrite(overlay, context);
        Assertions.assertEquals(new StringLiteral("snnow"), rewritten);
        overlay = new Overlay(
                StringLiteral.of("snow"),
                IntegerLiteral.of(2),
                IntegerLiteral.of(-1),
                StringLiteral.of("n")
        );
        rewritten = executor.rewrite(overlay, context);
        Assertions.assertEquals(new StringLiteral("sn"), rewritten);
        overlay = new Overlay(
                StringLiteral.of("snow"),
                IntegerLiteral.of(-1),
                IntegerLiteral.of(3),
                StringLiteral.of("n")
        );
        rewritten = executor.rewrite(overlay, context);
        Assertions.assertEquals(new StringLiteral("snow"), rewritten);

        ReplaceEmpty replace = new ReplaceEmpty(
                StringLiteral.of(""),
                StringLiteral.of(""),
                StringLiteral.of("default")
        );
        rewritten = executor.rewrite(replace, context);
        Assertions.assertEquals(new StringLiteral("default"), rewritten);

        Soundex soundex = new Soundex(StringLiteral.of("Ashcraft"));
        rewritten = executor.rewrite(soundex, context);
        Assertions.assertEquals(new StringLiteral("A261"), rewritten);
        soundex = new Soundex(StringLiteral.of("Robert"));
        rewritten = executor.rewrite(soundex, context);
        Assertions.assertEquals(new StringLiteral("R163"), rewritten);
        soundex = new Soundex(StringLiteral.of("R@bert"));
        rewritten = executor.rewrite(soundex, context);
        Assertions.assertEquals(new StringLiteral("R163"), rewritten);
        soundex = new Soundex(StringLiteral.of("Honeyman"));
        rewritten = executor.rewrite(soundex, context);
        Assertions.assertEquals(new StringLiteral("H555"), rewritten);
        soundex = new Soundex(StringLiteral.of("Apache Doris你好"));
        rewritten = executor.rewrite(soundex, context);
        Assertions.assertEquals(new StringLiteral("A123"), rewritten);
        soundex = new Soundex(StringLiteral.of(""));
        rewritten = executor.rewrite(soundex, context);
        Assertions.assertEquals(new StringLiteral(""), rewritten);

        Assertions.assertThrows(NotSupportedException.class, () -> {
            Soundex soundexThrow = new Soundex(new StringLiteral("Doris你好"));
            executor.rewrite(soundexThrow, context);
        }, "soundex only supports ASCII");
    }

    @Test
    void testleFoldNumeric() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
            bottomUp(FoldConstantRuleOnFE.VISITOR_INSTANCE)
        ));
        Coalesce c = new Coalesce(new NullLiteral(), new NullLiteral());
        Expression rewritten = executor.rewrite(c, context);
        Assertions.assertEquals(new NullLiteral(), rewritten);
        c = new Coalesce(new NullLiteral(), new IntegerLiteral(1));
        rewritten = executor.rewrite(c, context);
        Assertions.assertEquals(new IntegerLiteral(1), rewritten);
        c = new Coalesce(new IntegerLiteral(3), new IntegerLiteral(5));
        rewritten = executor.rewrite(c, context);
        Assertions.assertEquals(new IntegerLiteral(3), rewritten);

        Round round = new Round(new DoubleLiteral(3.4d));
        rewritten = executor.rewrite(round, context);
        Assertions.assertEquals(new DoubleLiteral(3d), rewritten);
        round = new Round(new DoubleLiteral(3.4d), new IntegerLiteral(5));
        rewritten = executor.rewrite(round, context);
        Assertions.assertEquals(new DoubleLiteral(3.4d), rewritten);
        round = new Round(new DoubleLiteral(3.5d));
        rewritten = executor.rewrite(round, context);
        Assertions.assertEquals(new DoubleLiteral(4d), rewritten);

        Ceil ceil = new Ceil(new DoubleLiteral(3.4d));
        rewritten = executor.rewrite(ceil, context);
        Assertions.assertEquals(new DoubleLiteral(4d), rewritten);
        ceil = new Ceil(new DoubleLiteral(3.4d), new IntegerLiteral(5));
        rewritten = executor.rewrite(ceil, context);
        Assertions.assertEquals(new DoubleLiteral(3.4d), rewritten);

        Floor floor = new Floor(new DoubleLiteral(3.4d));
        rewritten = executor.rewrite(floor, context);
        Assertions.assertEquals(new DoubleLiteral(3d), rewritten);
        floor = new Floor(new DoubleLiteral(3.4d), new IntegerLiteral(5));
        rewritten = executor.rewrite(floor, context);
        Assertions.assertEquals(new DoubleLiteral(3.4d), rewritten);

        Exp exp = new Exp(new DoubleLiteral(0d));
        rewritten = executor.rewrite(exp, context);
        Assertions.assertEquals(new DoubleLiteral(1.0), rewritten);
        Expression exExp = new Exp(new DoubleLiteral(1000d));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new DoubleLiteral(Double.POSITIVE_INFINITY), rewritten);

        Dexp dexp = new Dexp(new DoubleLiteral(0d));
        rewritten = executor.rewrite(dexp, context);
        Assertions.assertEquals(new DoubleLiteral(1.0), rewritten);
        dexp = new Dexp(new DoubleLiteral(1000d));
        rewritten = executor.rewrite(dexp, context);
        Assertions.assertEquals(new DoubleLiteral(Double.POSITIVE_INFINITY), rewritten);

        Ln ln = new Ln(new DoubleLiteral(1d));
        rewritten = executor.rewrite(ln, context);
        Assertions.assertEquals(new DoubleLiteral(0.0), rewritten);
        exExp = new Ln(new DoubleLiteral(0.0d));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new NullLiteral(DoubleType.INSTANCE), rewritten);
        exExp = new Ln(new DoubleLiteral(Double.POSITIVE_INFINITY));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new DoubleLiteral(Double.POSITIVE_INFINITY), rewritten);

        exExp = new Ln(new DoubleLiteral(-1d));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new NullLiteral(DoubleType.INSTANCE), rewritten);
        exExp = new Log(new DoubleLiteral(1.0d), new DoubleLiteral(1.0d));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new NullLiteral(DoubleType.INSTANCE), rewritten);
        exExp = new Log(new DoubleLiteral(10d), new DoubleLiteral(Double.POSITIVE_INFINITY));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new DoubleLiteral(Double.POSITIVE_INFINITY), rewritten);
        exExp = new Log2(new DoubleLiteral(Double.POSITIVE_INFINITY));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new DoubleLiteral(Double.POSITIVE_INFINITY), rewritten);
        exExp = new Log2(new DoubleLiteral(-1d));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new NullLiteral(DoubleType.INSTANCE), rewritten);
        exExp = new Log10(new DoubleLiteral(Double.POSITIVE_INFINITY));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new DoubleLiteral(Double.POSITIVE_INFINITY), rewritten);
        exExp = new Log10(new DoubleLiteral(-1d));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new NullLiteral(DoubleType.INSTANCE), rewritten);
        exExp = new Dlog10(new DoubleLiteral(Double.POSITIVE_INFINITY));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new DoubleLiteral(Double.POSITIVE_INFINITY), rewritten);
        exExp = new Dlog10(new DoubleLiteral(-1d));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new NullLiteral(DoubleType.INSTANCE), rewritten);

        Sqrt sqrt = new Sqrt(new DoubleLiteral(16d));
        rewritten = executor.rewrite(sqrt, context);
        Assertions.assertEquals(new DoubleLiteral(4d), rewritten);
        sqrt = new Sqrt(new DoubleLiteral(0d));
        rewritten = executor.rewrite(sqrt, context);
        Assertions.assertEquals(new DoubleLiteral(0d), rewritten);
        exExp = new Sqrt(new DoubleLiteral(-1d));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new NullLiteral(DoubleType.INSTANCE), rewritten);
        sqrt = new Sqrt(new DoubleLiteral(Double.POSITIVE_INFINITY));
        rewritten = executor.rewrite(sqrt, context);
        Assertions.assertEquals(new DoubleLiteral(Double.POSITIVE_INFINITY), rewritten);

        Dsqrt dsqrt = new Dsqrt(new DoubleLiteral(16d));
        rewritten = executor.rewrite(dsqrt, context);
        Assertions.assertEquals(new DoubleLiteral(4d), rewritten);
        dsqrt = new Dsqrt(new DoubleLiteral(0d));
        rewritten = executor.rewrite(dsqrt, context);
        Assertions.assertEquals(new DoubleLiteral(0d), rewritten);
        dsqrt = new Dsqrt(new DoubleLiteral(-1d));
        rewritten = executor.rewrite(dsqrt, context);
        Assertions.assertEquals(new NullLiteral(DoubleType.INSTANCE), rewritten);
        dsqrt = new Dsqrt(new DoubleLiteral(Double.POSITIVE_INFINITY));
        rewritten = executor.rewrite(dsqrt, context);
        Assertions.assertEquals(new DoubleLiteral(Double.POSITIVE_INFINITY), rewritten);

        Power power = new Power(new DoubleLiteral(2d), new DoubleLiteral(3));
        rewritten = executor.rewrite(power, context);
        Assertions.assertEquals(new DoubleLiteral(8d), rewritten);
        exExp = new Power(new DoubleLiteral(2d), new DoubleLiteral(10000d));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new DoubleLiteral(Double.POSITIVE_INFINITY), rewritten);
        exExp = new Power(new DoubleLiteral(-1d), new DoubleLiteral(1.1d));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);
        exExp = new Power(new DoubleLiteral(1d), new DoubleLiteral(0d));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new DoubleLiteral(1d), rewritten);
        exExp = new Power(new DoubleLiteral(0d), new DoubleLiteral(1d));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new DoubleLiteral(0d), rewritten);
        exExp = new Power(new DoubleLiteral(1.1), new DoubleLiteral(Double.POSITIVE_INFINITY));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(exExp, rewritten);
        exExp = new Power(new DoubleLiteral(-1d), new DoubleLiteral(Double.POSITIVE_INFINITY));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(exExp, rewritten);
        exExp = new Power(new DoubleLiteral(-1d), new DoubleLiteral(Double.NaN));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(exExp, rewritten);

        Sin sin = new Sin(new DoubleLiteral(Math.PI / 2));
        rewritten = executor.rewrite(sin, context);
        Assertions.assertEquals(new DoubleLiteral(1d), rewritten);
        sin = new Sin(new DoubleLiteral(0d));
        rewritten = executor.rewrite(sin, context);
        Assertions.assertEquals(new DoubleLiteral(0d), rewritten);
        exExp = new Sin(new DoubleLiteral(Double.POSITIVE_INFINITY));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);
        exExp = new Sin(new DoubleLiteral(Double.NEGATIVE_INFINITY));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);
        exExp = new Sin(new DoubleLiteral(Double.NaN));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);

        Sinh sinh = new Sinh(new DoubleLiteral(Double.NEGATIVE_INFINITY));
        rewritten = executor.rewrite(sinh, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NEGATIVE_INFINITY), rewritten);
        sinh = new Sinh(new DoubleLiteral(Double.POSITIVE_INFINITY));
        rewritten = executor.rewrite(sinh, context);
        Assertions.assertEquals(new DoubleLiteral(Double.POSITIVE_INFINITY), rewritten);
        sinh = new Sinh(new DoubleLiteral(Double.NaN));
        rewritten = executor.rewrite(sinh, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);

        Cos cos = new Cos(new DoubleLiteral(0d));
        rewritten = executor.rewrite(cos, context);
        Assertions.assertEquals(new DoubleLiteral(1d), rewritten);
        exExp = new Cos(new DoubleLiteral(Double.POSITIVE_INFINITY));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);
        exExp = new Cos(new DoubleLiteral(Double.NEGATIVE_INFINITY));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);

        Tan tan = new Tan(new DoubleLiteral(0d));
        rewritten = executor.rewrite(tan, context);
        Assertions.assertEquals(new DoubleLiteral(0d), rewritten);
        exExp = new Tan(new DoubleLiteral(Double.POSITIVE_INFINITY));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);
        exExp = new Tan(new DoubleLiteral(Double.NEGATIVE_INFINITY));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);

        exExp = new Cot(new DoubleLiteral(Double.POSITIVE_INFINITY));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);
        exExp = new Cot(new DoubleLiteral(Double.NEGATIVE_INFINITY));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);

        exExp = new Sec(new DoubleLiteral(Double.POSITIVE_INFINITY));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);
        exExp = new Sec(new DoubleLiteral(Double.NEGATIVE_INFINITY));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);

        exExp = new Csc(new DoubleLiteral(Double.POSITIVE_INFINITY));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);
        exExp = new Csc(new DoubleLiteral(Double.NEGATIVE_INFINITY));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);

        Asin asin = new Asin(new DoubleLiteral(1d));
        rewritten = executor.rewrite(asin, context);
        Assertions.assertEquals(new DoubleLiteral(Math.PI / 2), rewritten);
        exExp = new Asin(new DoubleLiteral(2d));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new NullLiteral(DoubleType.INSTANCE), rewritten);

        Acos acos = new Acos(new DoubleLiteral(1d));
        rewritten = executor.rewrite(acos, context);
        Assertions.assertEquals(new DoubleLiteral(0), rewritten);
        exExp = new Acos(new DoubleLiteral(2d));
        rewritten = executor.rewrite(exExp, context);
        Assertions.assertEquals(new NullLiteral(DoubleType.INSTANCE), rewritten);

        Atan atan = new Atan(new DoubleLiteral(Double.POSITIVE_INFINITY));
        rewritten = executor.rewrite(atan, context);
        Assertions.assertEquals(new DoubleLiteral(1.5707963267948966), rewritten);
        atan = new Atan(new DoubleLiteral(Double.NEGATIVE_INFINITY));
        rewritten = executor.rewrite(atan, context);
        Assertions.assertEquals(new DoubleLiteral(-1.5707963267948966), rewritten);
        atan = new Atan(new DoubleLiteral(Double.NaN));
        rewritten = executor.rewrite(atan, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);

        Asinh asinh = new Asinh(new DoubleLiteral(Double.POSITIVE_INFINITY));
        rewritten = executor.rewrite(asinh, context);
        Assertions.assertEquals(new DoubleLiteral(Double.POSITIVE_INFINITY), rewritten);
        asinh = new Asinh(new DoubleLiteral(Double.NEGATIVE_INFINITY));
        rewritten = executor.rewrite(asinh, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NEGATIVE_INFINITY), rewritten);
        asinh = new Asinh(new DoubleLiteral(Double.NaN));
        rewritten = executor.rewrite(asinh, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);

        Acosh acosh = new Acosh(new DoubleLiteral(Double.POSITIVE_INFINITY));
        rewritten = executor.rewrite(acosh, context);
        Assertions.assertEquals(new DoubleLiteral(Double.POSITIVE_INFINITY), rewritten);
        acosh = new Acosh(new DoubleLiteral(Double.NaN));
        rewritten = executor.rewrite(acosh, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);
        acosh = new Acosh(new DoubleLiteral(0));
        rewritten = executor.rewrite(acosh, context);
        Assertions.assertEquals(new NullLiteral(DoubleType.INSTANCE), rewritten);

        Atanh atanh = new Atanh(new DoubleLiteral(Double.POSITIVE_INFINITY));
        rewritten = executor.rewrite(atanh, context);
        Assertions.assertEquals(new NullLiteral(DoubleType.INSTANCE), rewritten);
        atanh = new Atanh(new DoubleLiteral(Double.NEGATIVE_INFINITY));
        rewritten = executor.rewrite(atanh, context);
        Assertions.assertEquals(new NullLiteral(DoubleType.INSTANCE), rewritten);
        atanh = new Atanh(new DoubleLiteral(Double.NaN));
        rewritten = executor.rewrite(atanh, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);

        Even even = new Even(new DoubleLiteral(Double.POSITIVE_INFINITY));
        rewritten = executor.rewrite(even, context);
        Assertions.assertEquals(new DoubleLiteral(Double.POSITIVE_INFINITY), rewritten);
        even = new Even(new DoubleLiteral(Double.NEGATIVE_INFINITY));
        rewritten = executor.rewrite(even, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NEGATIVE_INFINITY), rewritten);
        even = new Even(new DoubleLiteral(Double.NaN));
        rewritten = executor.rewrite(even, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);

        Cbrt cbrt = new Cbrt(new DoubleLiteral(Double.POSITIVE_INFINITY));
        rewritten = executor.rewrite(cbrt, context);
        Assertions.assertEquals(new DoubleLiteral(Double.POSITIVE_INFINITY), rewritten);
        cbrt = new Cbrt(new DoubleLiteral(Double.NEGATIVE_INFINITY));
        rewritten = executor.rewrite(cbrt, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NEGATIVE_INFINITY), rewritten);
        cbrt = new Cbrt(new DoubleLiteral(Double.NaN));
        rewritten = executor.rewrite(cbrt, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);

        Cosh cosh = new Cosh(new DoubleLiteral(Double.POSITIVE_INFINITY));
        rewritten = executor.rewrite(cosh, context);
        Assertions.assertEquals(new DoubleLiteral(Double.POSITIVE_INFINITY), rewritten);
        cosh = new Cosh(new DoubleLiteral(Double.NEGATIVE_INFINITY));
        rewritten = executor.rewrite(cosh, context);
        Assertions.assertEquals(new DoubleLiteral(Double.POSITIVE_INFINITY), rewritten);
        cosh = new Cosh(new DoubleLiteral(Double.NaN));
        rewritten = executor.rewrite(cosh, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);

        Tanh tanh = new Tanh(new DoubleLiteral(Double.POSITIVE_INFINITY));
        rewritten = executor.rewrite(tanh, context);
        Assertions.assertEquals(new DoubleLiteral(1d), rewritten);
        tanh = new Tanh(new DoubleLiteral(Double.NEGATIVE_INFINITY));
        rewritten = executor.rewrite(tanh, context);
        Assertions.assertEquals(new DoubleLiteral(-1d), rewritten);
        tanh = new Tanh(new DoubleLiteral(Double.NaN));
        rewritten = executor.rewrite(tanh, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);

        Sign sign = new Sign(new DoubleLiteral(1d));
        rewritten = executor.rewrite(sign, context);
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), rewritten);
        sign = new Sign(new DoubleLiteral(-1d));
        rewritten = executor.rewrite(sign, context);
        Assertions.assertEquals(new TinyIntLiteral((byte) -1), rewritten);
        sign = new Sign(new DoubleLiteral(0d));
        rewritten = executor.rewrite(sign, context);
        Assertions.assertEquals(new TinyIntLiteral((byte) 0), rewritten);

        Bin bin = new Bin(new BigIntLiteral(5));
        rewritten = executor.rewrite(bin, context);
        Assertions.assertEquals(new VarcharLiteral("101"), rewritten);

        BitCount bitCount = new BitCount(new BigIntLiteral(16));
        rewritten = executor.rewrite(bitCount, context);
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), rewritten);
        bitCount = new BitCount(new BigIntLiteral(-1));
        rewritten = executor.rewrite(bitCount, context);
        Assertions.assertEquals(new TinyIntLiteral((byte) 64), rewritten);

        Add add = new Add(new DoubleLiteral(Double.POSITIVE_INFINITY), new DoubleLiteral(100));
        rewritten = executor.rewrite(add, context);
        Assertions.assertEquals(new DoubleLiteral(Double.POSITIVE_INFINITY), rewritten);
        add = new Add(new DoubleLiteral(Double.POSITIVE_INFINITY), new DoubleLiteral(Double.POSITIVE_INFINITY));
        rewritten = executor.rewrite(add, context);
        Assertions.assertEquals(new DoubleLiteral(Double.POSITIVE_INFINITY), rewritten);
        add = new Add(new DoubleLiteral(Double.NEGATIVE_INFINITY), new DoubleLiteral(Double.POSITIVE_INFINITY));
        rewritten = executor.rewrite(add, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);
        add = new Add(new DoubleLiteral(Double.NaN), new DoubleLiteral(1));
        rewritten = executor.rewrite(add, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);

        Subtract subtract = new Subtract(new DoubleLiteral(Double.POSITIVE_INFINITY), new DoubleLiteral(100));
        rewritten = executor.rewrite(subtract, context);
        Assertions.assertEquals(new DoubleLiteral(Double.POSITIVE_INFINITY), rewritten);
        subtract = new Subtract(new DoubleLiteral(1), new DoubleLiteral(Double.POSITIVE_INFINITY));
        rewritten = executor.rewrite(subtract, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NEGATIVE_INFINITY), rewritten);
        subtract = new Subtract(new DoubleLiteral(Double.POSITIVE_INFINITY), new DoubleLiteral(Double.POSITIVE_INFINITY));
        rewritten = executor.rewrite(subtract, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);

        Multiply multiply = new Multiply(new DoubleLiteral(1e300), new DoubleLiteral(1e100));
        rewritten = executor.rewrite(multiply, context);
        Assertions.assertEquals(new DoubleLiteral(Double.POSITIVE_INFINITY), rewritten);
        multiply = new Multiply(new DoubleLiteral(-1e300), new DoubleLiteral(1e100));
        rewritten = executor.rewrite(multiply, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NEGATIVE_INFINITY), rewritten);
        multiply = new Multiply(new DoubleLiteral(Double.POSITIVE_INFINITY), new DoubleLiteral(100));
        rewritten = executor.rewrite(multiply, context);
        Assertions.assertEquals(new DoubleLiteral(Double.POSITIVE_INFINITY), rewritten);
        multiply = new Multiply(new DoubleLiteral(Double.POSITIVE_INFINITY), new DoubleLiteral(Double.NEGATIVE_INFINITY));
        rewritten = executor.rewrite(multiply, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NEGATIVE_INFINITY), rewritten);

        Divide divide = new Divide(new DoubleLiteral(Double.POSITIVE_INFINITY), new DoubleLiteral(1e100));
        rewritten = executor.rewrite(divide, context);
        Assertions.assertEquals(new DoubleLiteral(Double.POSITIVE_INFINITY), rewritten);
        divide = new Divide(new DoubleLiteral(Double.NEGATIVE_INFINITY), new DoubleLiteral(1e100));
        rewritten = executor.rewrite(divide, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NEGATIVE_INFINITY), rewritten);
        divide = new Divide(new DoubleLiteral(Double.NEGATIVE_INFINITY), new DoubleLiteral(Double.NEGATIVE_INFINITY));
        rewritten = executor.rewrite(divide, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);

        Fmod fmod = new Fmod(new DoubleLiteral(Double.POSITIVE_INFINITY), new DoubleLiteral(1));
        rewritten = executor.rewrite(fmod, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);
        fmod = new Fmod(new DoubleLiteral(Double.NEGATIVE_INFINITY), new DoubleLiteral(1));
        rewritten = executor.rewrite(fmod, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);
        fmod = new Fmod(new DoubleLiteral(Double.NaN), new DoubleLiteral(1));
        rewritten = executor.rewrite(fmod, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);
        fmod = new Fmod(new DoubleLiteral(Double.NaN), new DoubleLiteral(0));
        rewritten = executor.rewrite(fmod, context);
        Assertions.assertEquals(new NullLiteral(DoubleType.INSTANCE), rewritten);

        fmod = new Fmod(new FloatLiteral(Float.POSITIVE_INFINITY), new FloatLiteral(1));
        rewritten = executor.rewrite(fmod, context);
        Assertions.assertEquals(new FloatLiteral(Float.NaN), rewritten);
        fmod = new Fmod(new FloatLiteral(Float.NEGATIVE_INFINITY), new FloatLiteral(1));
        rewritten = executor.rewrite(fmod, context);
        Assertions.assertEquals(new FloatLiteral(Float.NaN), rewritten);
        fmod = new Fmod(new FloatLiteral(Float.NaN), new FloatLiteral(1));
        rewritten = executor.rewrite(fmod, context);
        Assertions.assertEquals(new FloatLiteral(Float.NaN), rewritten);
        fmod = new Fmod(new FloatLiteral(Float.NaN), new FloatLiteral(0));
        rewritten = executor.rewrite(fmod, context);
        Assertions.assertEquals(new NullLiteral(FloatType.INSTANCE), rewritten);

        Radians radians = new Radians(new DoubleLiteral(Double.POSITIVE_INFINITY));
        rewritten = executor.rewrite(radians, context);
        Assertions.assertEquals(new DoubleLiteral(Double.POSITIVE_INFINITY), rewritten);
        radians = new Radians(new DoubleLiteral(Double.NEGATIVE_INFINITY));
        rewritten = executor.rewrite(radians, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NEGATIVE_INFINITY), rewritten);
        radians = new Radians(new DoubleLiteral(Double.NaN));
        rewritten = executor.rewrite(radians, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);

        Degrees degrees = new Degrees(new DoubleLiteral(Double.POSITIVE_INFINITY));
        rewritten = executor.rewrite(degrees, context);
        Assertions.assertEquals(new DoubleLiteral(Double.POSITIVE_INFINITY), rewritten);
        degrees = new Degrees(new DoubleLiteral(Double.NEGATIVE_INFINITY));
        rewritten = executor.rewrite(degrees, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NEGATIVE_INFINITY), rewritten);
        degrees = new Degrees(new DoubleLiteral(Double.NaN));
        rewritten = executor.rewrite(degrees, context);
        Assertions.assertEquals(new DoubleLiteral(Double.NaN), rewritten);
    }

    @Test
    void testCompareFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(FoldConstantRuleOnFE.VISITOR_INSTANCE)
        ));
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
    void testArithmeticFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(FoldConstantRuleOnFE.VISITOR_INSTANCE)
        ));
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
    void testTimestampFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(FoldConstantRuleOnFE.VISITOR_INSTANCE)
        ));
        String interval = "'1991-05-01' - interval 1 day";
        Expression e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        Expression e8 = new DateV2Literal(1991, 4, 30);
        assertRewrite(e7, e8);

        interval = "'1991-05-01' + interval '1' day";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = new DateV2Literal(1991, 5, 2);
        assertRewrite(e7, e8);

        interval = "'1991-05-01' + interval 1+1 day";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = new DateV2Literal(1991, 5, 3);
        assertRewrite(e7, e8);

        interval = "date '1991-05-01' + interval 10 / 2 + 1 day";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = new DateV2Literal(1991, 5, 7);
        assertRewrite(e7, e8);

        interval = "interval '1' day + '1991-05-01'";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = new DateV2Literal(1991, 5, 2);
        assertRewrite(e7, e8);

        interval = "interval '3' month + '1991-05-01'";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = new DateV2Literal(1991, 8, 1);
        assertRewrite(e7, e8);

        interval = "interval 3 + 1 month + '1991-05-01'";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = new DateV2Literal(1991, 9, 1);
        assertRewrite(e7, e8);

        interval = "interval 3 + 1 year + '1991-05-01'";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = new DateV2Literal(1995, 5, 1);
        assertRewrite(e7, e8);

        interval = "interval 3 + 3 / 3 hour + '1991-05-01 10:00:00'";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = new DateTimeV2Literal(1991, 5, 1, 14, 0, 0);
        assertRewrite(e7, e8);

        interval = "interval 3 * 2 / 3 minute + '1991-05-01 10:00:00'";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = new DateTimeV2Literal(1991, 5, 1, 10, 2, 0);
        assertRewrite(e7, e8);

        interval = "interval 3 / 3 + 1 second + '1991-05-01 10:00:00'";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = new DateTimeV2Literal(1991, 5, 1, 10, 0, 2);
        assertRewrite(e7, e8);

        // a + interval 1 day
        Slot a = SlotReference.of("a", DateTimeV2Type.SYSTEM_DEFAULT);
        TimestampArithmetic arithmetic = new TimestampArithmetic(Operator.ADD, a, Literal.of(1), TimeUnit.DAY);
        Expression process = process(arithmetic);
        assertRewrite(process, process);
    }

    Expression process(TimestampArithmetic arithmetic) {
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
    void testDateTypeDateTimeArithmeticFunctions() {
        DateLiteral dateLiteral = new DateLiteral("1999-12-31");
        VarcharLiteral format = new VarcharLiteral("%Y-%m-%d");

        String[] answer = {
                "1999", "4", "12", "6", "31", "365", "31",
                "'1999-12-31'", "'1999-12-27'", "'1999-12-31'"
        };
        int answerIdx = 0;

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
    void testDateTimeTypeDateTimeArithmeticFunctions() {
        DateTimeLiteral dateLiteral = new DateTimeLiteral("1999-12-31 23:59:59");
        VarcharLiteral format = new VarcharLiteral("%Y-%m-%d");

        String[] answer = {
                "1999", "4", "12", "6", "31", "365", "31", "23", "59", "59",
                "'1999-12-31'", "'1999-12-27'", "'1999-12-31'", "'1999-12-31'", "730484", "'1999-12-31'"
        };
        int answerIdx = 0;

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
    void testDateV2TypeDateTimeArithmeticFunctions() {
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
    void testDateError() {
        boolean isError = false;
        try {
            new DateV2Literal("0000-02-29");
        } catch (AnalysisException e) {
            Assertions.assertEquals(e.getMessage(), "date/datetime literal [0000-02-29] is out of range");
            isError = true;
        }
        Assertions.assertEquals(isError, true);
    }

    @Test
    void testDateTimeV2TypeDateTimeArithmeticFunctions() {
        DateTimeV2Literal dateLiteral = new DateTimeV2Literal(DateTimeV2Type.SYSTEM_DEFAULT, "1999-12-31 23:59:59");
        IntegerLiteral integerLiteral = new IntegerLiteral(30);
        BigIntLiteral bigIntLiteral = new BigIntLiteral(30);
        VarcharLiteral format = new VarcharLiteral("%Y-%m-%d");

        String[] answer = {
                "'2000-01-30 23:59:59'", "'1999-12-01 23:59:59'", "'2029-12-31 23:59:59'",
                "'1969-12-31 23:59:59'",
                "'2002-06-30 23:59:59'", "'1997-06-30 23:59:59'", "'2000-01-30 23:59:59'",
                "'1999-12-01 23:59:59'",
                "'2000-01-02 05:59:59'", "'1999-12-30 17:59:59'", "'2000-01-01 00:29:59'",
                "'1999-12-31 23:29:59'", "'2000-01-01 00:00:29'", "'1999-12-31 23:59:29'",
                "'1999-12-31 23:59:59.000030'", "'1999-12-31 23:59:58.999970'", "'1999-12-31 23:59:59.030000'",
                "'1999-12-31 23:59:58.970000'",
                "1999", "4", "12", "6", "31", "365", "31", "23", "59", "59",
                "'1999-12-31'", "'1999-12-27'", "'1999-12-31'", "'1999-12-31'", "730484", "'1999-12-31'",
                "'1999-12-31'"
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
        Assertions.assertEquals(DateTimeArithmetic.minutesAdd(dateLiteral, bigIntLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.minutesSub(dateLiteral, bigIntLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.secondsAdd(dateLiteral, bigIntLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.secondsSub(dateLiteral, bigIntLiteral).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.microSecondsAdd(dateLiteral, bigIntLiteral).toSql(),
                        answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.microSecondsSub(dateLiteral, bigIntLiteral).toSql(),
                        answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.milliSecondsAdd(dateLiteral, bigIntLiteral).toSql(),
                        answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.milliSecondsSub(dateLiteral, bigIntLiteral).toSql(),
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

        Assertions.assertEquals("'2021 52 2021 52'", DateTimeExtractAndTransform.dateFormat(
                new DateTimeLiteral("2022-01-01 00:12:42"),
                new VarcharLiteral("%x %v %X %V")).toSql());
        Assertions.assertEquals("'2023 18 2023 19'", DateTimeExtractAndTransform.dateFormat(
                new DateTimeLiteral("2023-05-07 02:41:42"),
                new VarcharLiteral("%x %v %X %V")).toSql());

        Assertions.assertTrue(new DateTimeV2Literal("2021-01-01 12:12:14.000000").compareTo((ComparableLiteral) TimeRoundSeries
                .secondCeil(new DateTimeV2Literal("2021-01-01 12:12:12.123"), new IntegerLiteral(2))) == 0);
        Assertions.assertTrue(new DateTimeV2Literal("2021-01-01 12:12:12.000000").compareTo((ComparableLiteral) TimeRoundSeries
                .secondFloor(new DateTimeV2Literal("2021-01-01 12:12:12.123"), new IntegerLiteral(2))) == 0);
    }

    @Test
    void testDateDiff() {
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
    void testDateTrunc() {
        DateTimeLiteral dateTimeLiteral = new DateTimeLiteral("2001-12-31 01:01:01");
        DateTimeV2Literal dateTimeV2Literal = new DateTimeV2Literal("2001-12-31 01:01:01");

        String[] tags = {"year", "month", "day", "hour", "minute", "second"};

        String[] answer = {
                "'2001-01-01 00:00:00'", "'2001-01-01 00:00:00.000000'", "'2001-12-01 00:00:00'",
                "'2001-12-01 00:00:00.000000'", "'2001-12-31 00:00:00'", "'2001-12-31 00:00:00.000000'",
                "'2001-12-31 01:00:00'", "'2001-12-31 01:00:00.000000'", "'2001-12-31 01:01:00'",
                "'2001-12-31 01:01:00.000000'", "'2001-12-31 01:01:01'", "'2001-12-31 01:01:01.000000'",
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
    void testDateConstructFunction() {
        String[] answer = {
                "'2001-07-19'", "'6411-08-17'", "'0000-01-01'", "'1977-06-03 17:57:24'",
                "'1977-06-03'", "1008909293", "1008864000.000000"
        };
        int answerIdx = 0;

        Assertions.assertEquals(answer[answerIdx++], DateTimeExtractAndTransform.makeDate(
                new IntegerLiteral(2001),
                new IntegerLiteral(200)
        ).toSql());
        Assertions.assertEquals(answer[answerIdx++], DateTimeExtractAndTransform.fromDays(
                new IntegerLiteral(2341798)
        ).toSql());
        Assertions.assertEquals(answer[answerIdx++], DateTimeExtractAndTransform.fromDays(
                new IntegerLiteral(1)
        ).toSql());
        Assertions.assertEquals(answer[answerIdx++], DateTimeExtractAndTransform.fromUnixTime(
                new BigIntLiteral(234179844)
        ).toSql());
        Assertions.assertEquals(answer[answerIdx++], DateTimeExtractAndTransform.fromUnixTime(
                new BigIntLiteral(234179844),
                new VarcharLiteral("%Y-%m-%d")
        ).toSql());
        Assertions.assertEquals(answer[answerIdx++], DateTimeExtractAndTransform.unixTimestamp(
                new DateTimeLiteral("2001-12-21 12:34:53")
        ).toSql());
        Assertions.assertEquals(answer[answerIdx], DateTimeExtractAndTransform.unixTimestamp(
                new VarcharLiteral("2001-12-21"),
                new VarcharLiteral("%Y-%m-%d")
        ).toSql());
    }

    @Test
    void testFoldNestedExpression() {
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
    void testFoldCastStringToDate() {
        assertRewriteExpression("cast('2021-01-01' as date)", "'2021-01-01'");
        assertRewriteExpression("cast('20210101' as date)", "'2021-01-01'");
        assertRewriteExpression("cast('2021-01-01T00:00:00' as date)", "'2021-01-01'");
        assertRewriteExpression("cast('2021-01-01' as datetime)", "'2021-01-01 00:00:00'");
        assertRewriteExpression("cast('20210101' as datetime)", "'2021-01-01 00:00:00'");
        assertRewriteExpression("cast('2021-01-01T00:00:00' as datetime)", "'2021-01-01 00:00:00'");
        assertRewriteExpression("cast ('2022-12-02 22:23:24.999999' as datetimev2(3))", "'2022-12-02 22:23:25.000'");
    }

    @Test
    void testFoldTypeOfNullLiteral() {
        String actualExpression = "append_trailing_char_if_absent(cast(version() as varchar), cast(null as varchar))";
        ExpressionRewriteContext context = new ExpressionRewriteContext(
                MemoTestUtils.createCascadesContext(new UnboundRelation(new RelationId(1), ImmutableList.of("test_table"))));
        NereidsParser parser = new NereidsParser();
        Expression e1 = parser.parseExpression(actualExpression);
        e1 = new ExpressionNormalization().rewrite(ExpressionAnalyzer.FUNCTION_ANALYZER_RULE.rewrite(e1, context), context);
        Assertions.assertTrue(e1.getDataType() instanceof VarcharType);
    }

    @Test
    void testFoldNvl() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                ExpressionAnalyzer.FUNCTION_ANALYZER_RULE,
                bottomUp(
                        FoldConstantRule.INSTANCE,
                        SimplifyConditionalFunction.INSTANCE
                )
        ));

        assertRewriteExpression("nvl(NULL, 1)", "1");
        assertRewriteExpression("nvl(NULL, NULL)", "NULL");
        assertRewriteAfterTypeCoercion("nvl(IA, NULL)", "ifnull(IA, NULL)");
        assertRewriteAfterTypeCoercion("nvl(IA, 1)", "ifnull(IA, 1)");

        Expression foldNvl = executor.rewrite(
                PARSER.parseExpression("nvl(cast('2025-04-17' as datetime(0)), cast('2025-04-18 01:02:03.123456' as datetime(6)))"),
                context
        );
        Assertions.assertEquals(new DateTimeV2Literal(DateTimeV2Type.of(6), "2025-04-17"), foldNvl);
    }

    private void assertRewriteExpression(String actualExpression, String expectedExpression) {
        ExpressionRewriteContext context = new ExpressionRewriteContext(
                MemoTestUtils.createCascadesContext(new UnboundRelation(new RelationId(1), ImmutableList.of("test_table"))));

        NereidsParser parser = new NereidsParser();
        Expression e1 = parser.parseExpression(actualExpression);
        e1 = new ExpressionNormalization().rewrite(ExpressionAnalyzer.FUNCTION_ANALYZER_RULE.rewrite(e1, context), context);
        Assertions.assertEquals(expectedExpression, e1.toSql());
    }
}
