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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.NotSupportedException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRule;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.TimestampArithmetic;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeArithmetic;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeExtractAndTransform;
import org.apache.doris.nereids.trees.expressions.functions.executable.TimeRoundSeries;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Acos;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AppendTrailingCharIfAbsent;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Asin;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Bin;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitCount;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Ceil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ConvertTz;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Cos;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateFormat;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Exp;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Floor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.FromUnixtime;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HoursAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Ln;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MinutesAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Power;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Round;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SecondsAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Sign;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Sin;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Sqrt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StrToDate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Tan;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ToDays;
import org.apache.doris.nereids.trees.expressions.functions.scalar.UnixTimestamp;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Interval.TimeUnit;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
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
        HoursAdd hoursAdd = new HoursAdd(DateLiteral.fromJavaDateType(LocalDateTime.of(1, 1, 1, 1, 1, 1)),
                new IntegerLiteral(1));
        Expression rewritten = executor.rewrite(hoursAdd, context);
        Assertions.assertEquals(new DateTimeLiteral("0001-01-01 01:00:00"), rewritten);
        hoursAdd = new HoursAdd(DateV2Literal.fromJavaDateType(LocalDateTime.of(1, 1, 1, 1, 1, 1)),
                new IntegerLiteral(1));
        rewritten = executor.rewrite(hoursAdd, context);
        Assertions.assertEquals(new DateTimeV2Literal("0001-01-01 01:00:00"), rewritten);
        hoursAdd = new HoursAdd(DateV2Literal.fromJavaDateType(LocalDateTime.of(9999, 12, 31, 23, 1, 1)),
                new IntegerLiteral(24));
        rewritten = executor.rewrite(hoursAdd, context);
        Assertions.assertEquals(new NullLiteral(hoursAdd.getDataType()), rewritten);
        hoursAdd = new HoursAdd(DateV2Literal.fromJavaDateType(LocalDateTime.of(0, 1, 1, 1, 1, 1)),
                new IntegerLiteral(-25));
        rewritten = executor.rewrite(hoursAdd, context);
        Assertions.assertEquals(new NullLiteral(hoursAdd.getDataType()), rewritten);

        MinutesAdd minutesAdd = new MinutesAdd(DateLiteral.fromJavaDateType(LocalDateTime.of(1, 1, 1, 1, 1, 1)),
                new IntegerLiteral(1));
        rewritten = executor.rewrite(minutesAdd, context);
        Assertions.assertEquals(new DateTimeLiteral("0001-01-01 00:01:00"), rewritten);
        minutesAdd = new MinutesAdd(DateV2Literal.fromJavaDateType(LocalDateTime.of(1, 1, 1, 1, 1, 1)),
                new IntegerLiteral(1));
        rewritten = executor.rewrite(minutesAdd, context);
        Assertions.assertEquals(new DateTimeV2Literal("0001-01-01 00:01:00"), rewritten);
        minutesAdd = new MinutesAdd(DateV2Literal.fromJavaDateType(LocalDateTime.of(9999, 12, 31, 23, 59, 1)),
                new IntegerLiteral(1440));
        rewritten = executor.rewrite(minutesAdd, context);
        Assertions.assertEquals(new NullLiteral(minutesAdd.getDataType()), rewritten);
        minutesAdd = new MinutesAdd(DateV2Literal.fromJavaDateType(LocalDateTime.of(0, 1, 1, 0, 1, 1)),
                new IntegerLiteral(-2));
        rewritten = executor.rewrite(minutesAdd, context);
        Assertions.assertEquals(new NullLiteral(minutesAdd.getDataType()), rewritten);

        SecondsAdd secondsAdd = new SecondsAdd(DateLiteral.fromJavaDateType(LocalDateTime.of(1, 1, 1, 1, 1, 1)),
                new IntegerLiteral(1));
        rewritten = executor.rewrite(secondsAdd, context);
        Assertions.assertEquals(new DateTimeLiteral("0001-01-01 00:00:01"), rewritten);
        secondsAdd = new SecondsAdd(DateV2Literal.fromJavaDateType(LocalDateTime.of(1, 1, 1, 1, 1, 1)),
                new IntegerLiteral(1));
        rewritten = executor.rewrite(secondsAdd, context);
        Assertions.assertEquals(new DateTimeV2Literal("0001-01-01 00:00:01"), rewritten);
        secondsAdd = new SecondsAdd(DateV2Literal.fromJavaDateType(LocalDateTime.of(9999, 12, 31, 23, 59, 59)),
                new IntegerLiteral(86400));
        rewritten = executor.rewrite(secondsAdd, context);
        Assertions.assertEquals(new NullLiteral(secondsAdd.getDataType()), rewritten);
        secondsAdd = new SecondsAdd(DateV2Literal.fromJavaDateType(LocalDateTime.of(0, 1, 1, 0, 1, 1)),
                new IntegerLiteral(-61));
        rewritten = executor.rewrite(secondsAdd, context);
        Assertions.assertEquals(new NullLiteral(secondsAdd.getDataType()), rewritten);

        ToDays toDays = new ToDays(DateLiteral.fromJavaDateType(LocalDateTime.of(1, 1, 1, 1, 1, 1)));
        rewritten = executor.rewrite(toDays, context);
        Assertions.assertEquals(new IntegerLiteral(366), rewritten);
        toDays = new ToDays(DateV2Literal.fromJavaDateType(LocalDateTime.of(1, 1, 1, 1, 1, 1)));
        rewritten = executor.rewrite(toDays, context);
        Assertions.assertEquals(new IntegerLiteral(366), rewritten);
        toDays = new ToDays(DateV2Literal.fromJavaDateType(LocalDateTime.of(9999, 12, 31, 1, 1, 1)));
        rewritten = executor.rewrite(toDays, context);
        Assertions.assertEquals(new IntegerLiteral(3652424), rewritten);
    }

    @Test
    void testFoldString() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(FoldConstantRuleOnFE.VISITOR_INSTANCE)
        ));
        ConvertTz c = new ConvertTz(DateTimeV2Literal.fromJavaDateType(LocalDateTime.of(1, 1, 1, 1, 1, 1)),
                StringLiteral.of("Asia/Shanghai"), StringLiteral.of("GMT"));
        Expression rewritten = executor.rewrite(c, context);
        Assertions.assertTrue(new DateTimeV2Literal("0000-12-31 16:55:18.000000").compareTo((Literal) rewritten) == 0);
        c = new ConvertTz(DateTimeV2Literal.fromJavaDateType(LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999999000)),
                        StringLiteral.of("Pacific/Galapagos"), StringLiteral.of("Pacific/Galapagos"));
        rewritten = executor.rewrite(c, context);
        Assertions.assertTrue(new DateTimeV2Literal("9999-12-31 23:59:59.999999").compareTo((Literal) rewritten) == 0);

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
        Assertions.assertTrue(((Literal) rewritten).compareTo(new DateTimeV2Literal("0001-01-01 00:00:00.000000")) == 0);
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
        Assertions.assertEquals(new DecimalV3Literal(new BigDecimal("1636560000.000000")), rewritten);

        StrToDate sd = new StrToDate(StringLiteral.of("2021-11-11"), StringLiteral.of("%Y-%m-%d"));
        rewritten = executor.rewrite(sd, context);
        Assertions.assertEquals(new DateV2Literal("2021-11-11"), rewritten);

        AppendTrailingCharIfAbsent a = new AppendTrailingCharIfAbsent(StringLiteral.of("1"), StringLiteral.of("3"));
        rewritten = executor.rewrite(a, context);
        Assertions.assertEquals(new StringLiteral("13"), rewritten);
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
        Assertions.assertThrows(NotSupportedException.class, () -> {
            Exp exExp = new Exp(new DoubleLiteral(1000d));
            executor.rewrite(exExp, context);
        }, "infinite result is invalid");

        Ln ln = new Ln(new DoubleLiteral(1d));
        rewritten = executor.rewrite(ln, context);
        Assertions.assertEquals(new DoubleLiteral(0.0), rewritten);
        Assertions.assertThrows(NotSupportedException.class, () -> {
            Ln exExp = new Ln(new DoubleLiteral(0.0d));
            executor.rewrite(exExp, context);
        }, "input 0.0 is out of boundary");
        Assertions.assertThrows(NotSupportedException.class, () -> {
            Ln exExp = new Ln(new DoubleLiteral(-1d));
            executor.rewrite(exExp, context);
        }, "input -1 is out of boundary");

        Sqrt sqrt = new Sqrt(new DoubleLiteral(16d));
        rewritten = executor.rewrite(sqrt, context);
        Assertions.assertEquals(new DoubleLiteral(4d), rewritten);
        sqrt = new Sqrt(new DoubleLiteral(0d));
        rewritten = executor.rewrite(sqrt, context);
        Assertions.assertEquals(new DoubleLiteral(0d), rewritten);
        Assertions.assertThrows(NotSupportedException.class, () -> {
            Sqrt exExp = new Sqrt(new DoubleLiteral(-1d));
            executor.rewrite(exExp, context);
        }, "input -1 is out of boundary");

        Power power = new Power(new DoubleLiteral(2d), new DoubleLiteral(3));
        rewritten = executor.rewrite(power, context);
        Assertions.assertEquals(new DoubleLiteral(8d), rewritten);
        Assertions.assertThrows(NotSupportedException.class, () -> {
            Power exExp = new Power(new DoubleLiteral(2d), new DoubleLiteral(10000d));
            executor.rewrite(exExp, context);
        }, "infinite result is invalid");

        Sin sin = new Sin(new DoubleLiteral(Math.PI / 2));
        rewritten = executor.rewrite(sin, context);
        Assertions.assertEquals(new DoubleLiteral(1d), rewritten);
        sin = new Sin(new DoubleLiteral(0d));
        rewritten = executor.rewrite(sin, context);
        Assertions.assertEquals(new DoubleLiteral(0d), rewritten);
        Assertions.assertThrows(NotSupportedException.class, () -> {
            Sin exExp = new Sin(new DoubleLiteral(Double.POSITIVE_INFINITY));
            executor.rewrite(exExp, context);
        }, "input infinity is out of boundary");

        Cos cos = new Cos(new DoubleLiteral(0d));
        rewritten = executor.rewrite(cos, context);
        Assertions.assertEquals(new DoubleLiteral(1d), rewritten);
        Assertions.assertThrows(NotSupportedException.class, () -> {
            Cos exExp = new Cos(new DoubleLiteral(Double.POSITIVE_INFINITY));
            executor.rewrite(exExp, context);
        }, "input infinity is out of boundary");

        Tan tan = new Tan(new DoubleLiteral(0d));
        rewritten = executor.rewrite(tan, context);
        Assertions.assertEquals(new DoubleLiteral(0d), rewritten);
        Assertions.assertThrows(NotSupportedException.class, () -> {
            Tan exExp = new Tan(new DoubleLiteral(Double.POSITIVE_INFINITY));
            executor.rewrite(exExp, context);
        }, "input infinity is out of boundary");

        Asin asin = new Asin(new DoubleLiteral(1d));
        rewritten = executor.rewrite(asin, context);
        Assertions.assertEquals(new DoubleLiteral(Math.PI / 2), rewritten);
        Assertions.assertThrows(NotSupportedException.class, () -> {
            Asin exExp = new Asin(new DoubleLiteral(2d));
            executor.rewrite(exExp, context);
        }, "input 2.0 is out of boundary");

        Acos acos = new Acos(new DoubleLiteral(1d));
        rewritten = executor.rewrite(acos, context);
        Assertions.assertEquals(new DoubleLiteral(0), rewritten);
        Assertions.assertThrows(NotSupportedException.class, () -> {
            Acos exExp = new Acos(new DoubleLiteral(2d));
            executor.rewrite(exExp, context);
        }, "input 2.0 is out of boundary");

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

        interval = "interval 3 + 3 / 3 hour + '1991-05-01 10:00:00'";
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

        interval = "interval 3 / 3 + 1 second + '1991-05-01 10:00:00'";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = Config.enable_date_conversion
                ? new DateTimeV2Literal(1991, 5, 1, 10, 0, 2)
                : new DateTimeLiteral(1991, 5, 1, 10, 0, 2);
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
    void testDateTimeTypeDateTimeArithmeticFunctions() {
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
        Assertions.assertEquals(DateTimeArithmetic.microSecondsSub(dateLiteral, integerLiteral).toSql(),
                answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.milliSecondsAdd(dateLiteral, integerLiteral).toSql(),
                answer[answerIdx++]);
        Assertions.assertEquals(DateTimeArithmetic.milliSecondsSub(dateLiteral, integerLiteral).toSql(),
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

        Assertions.assertTrue(new DateTimeV2Literal("2021-01-01 12:12:14.000000").compareTo((Literal) TimeRoundSeries
                .secondCeil(new DateTimeV2Literal("2021-01-01 12:12:12.123"), new IntegerLiteral(2))) == 0);
        Assertions.assertTrue(new DateTimeV2Literal("2021-01-01 12:12:12.000000").compareTo((Literal) TimeRoundSeries
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
                        FoldConstantRule.INSTANCE
                )
        ));

        assertRewriteExpression("nvl(NULL, 1)", "1");
        assertRewriteExpression("nvl(NULL, NULL)", "NULL");
        assertRewriteAfterTypeCoercion("nvl(IA, NULL)", "ifnull(IA, NULL)");
        assertRewriteAfterTypeCoercion("nvl(IA, 1)", "ifnull(IA, 1)");
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
