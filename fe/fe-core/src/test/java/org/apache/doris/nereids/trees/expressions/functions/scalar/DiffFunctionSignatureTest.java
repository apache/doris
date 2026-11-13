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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.ComputeSignature;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

/**
 * Binding of the 12 Nereids {@code *_diff} scalar functions on string input (apache/doris#66120).
 *
 * <p>All 12 list their TIMESTAMPTZ signature first. {@code SearchSignature} scores a string literal
 * against that overload by whether it carries a zone and otherwise breaks the tie between the
 * temporal overloads by list order; a string-typed slot (varchar column, subquery or UNION output)
 * gives it nothing to score, so the slot bound TIMESTAMPTZ and was cast through the session
 * {@code time_zone}. {@code SearchSignatureForDateDiff} tries the TIMESTAMPTZ overload last for
 * exactly that case, string-typed arguments with no inspectable string literal, and leaves every
 * other call to the shared search. These tests pin both sides of that line.
 */
public class DiffFunctionSignatureTest {

    private static final List<NamedDiffConstructor> DIFF_CONSTRUCTORS = ImmutableList.of(
            new NamedDiffConstructor("DateDiff", DateDiff::new),
            new NamedDiffConstructor("DaysDiff", DaysDiff::new),
            new NamedDiffConstructor("HoursDiff", HoursDiff::new),
            new NamedDiffConstructor("MicroSecondsDiff", MicroSecondsDiff::new),
            new NamedDiffConstructor("MilliSecondsDiff", MilliSecondsDiff::new),
            new NamedDiffConstructor("MinutesDiff", MinutesDiff::new),
            new NamedDiffConstructor("MonthsDiff", MonthsDiff::new),
            new NamedDiffConstructor("QuartersDiff", QuartersDiff::new),
            new NamedDiffConstructor("SecondsDiff", SecondsDiff::new),
            new NamedDiffConstructor("TimeDiff", TimeDiff::new),
            new NamedDiffConstructor("WeeksDiff", WeeksDiff::new),
            new NamedDiffConstructor("YearsDiff", YearsDiff::new));

    @Test
    public void testVarcharSlotsBindToDateTimeV2NotTimeStampTz() {
        SlotReference left = SlotReference.of("a", VarcharType.SYSTEM_DEFAULT);
        SlotReference right = SlotReference.of("b", VarcharType.SYSTEM_DEFAULT);
        for (NamedDiffConstructor c : DIFF_CONSTRUCTORS) {
            FunctionSignature signature = c.constructor.apply(left, right).getSignature();
            Assertions.assertInstanceOf(DateTimeV2Type.class, signature.getArgType(0),
                    c.name + ": varchar arg0 should bind to DateTimeV2Type, not TimeStampTzType (issue #66120)");
            Assertions.assertInstanceOf(DateTimeV2Type.class, signature.getArgType(1),
                    c.name + ": varchar arg1 should bind to DateTimeV2Type, not TimeStampTzType (issue #66120)");
        }
    }

    @Test
    public void testTimeStampTzSlotsStillBindToTimeStampTz() {
        SlotReference left = SlotReference.of("a", TimeStampTzType.of(6));
        SlotReference right = SlotReference.of("b", TimeStampTzType.of(6));
        for (NamedDiffConstructor c : DIFF_CONSTRUCTORS) {
            FunctionSignature signature = c.constructor.apply(left, right).getSignature();
            Assertions.assertInstanceOf(TimeStampTzType.class, signature.getArgType(0),
                    c.name + ": timestamptz arg0 should still bind to TimeStampTzType");
            Assertions.assertInstanceOf(TimeStampTzType.class, signature.getArgType(1),
                    c.name + ": timestamptz arg1 should still bind to TimeStampTzType");
        }
    }

    /**
     * A varchar slot beside a typed TIMESTAMPTZ slot is decided by the number of casts, not by list
     * order, so moving the TIMESTAMPTZ overload last for string slots must not pull such a pair to
     * DATETIMEV2: the typed side keeps instant semantics, in either argument order.
     */
    @Test
    public void testVarcharSlotWithTimeStampTzSlotKeepsTimeStampTz() {
        SlotReference varchar = SlotReference.of("a", VarcharType.SYSTEM_DEFAULT);
        SlotReference timeStampTz = SlotReference.of("b", TimeStampTzType.of(6));
        for (NamedDiffConstructor c : DIFF_CONSTRUCTORS) {
            FunctionSignature signature = c.constructor.apply(varchar, timeStampTz).getSignature();
            Assertions.assertInstanceOf(TimeStampTzType.class, signature.getArgType(0),
                    c.name + ": varchar slot + timestamptz slot: arg0 should bind to TimeStampTzType");
            Assertions.assertInstanceOf(TimeStampTzType.class, signature.getArgType(1),
                    c.name + ": varchar slot + timestamptz slot: arg1 should bind to TimeStampTzType");

            FunctionSignature reversed = c.constructor.apply(timeStampTz, varchar).getSignature();
            Assertions.assertInstanceOf(TimeStampTzType.class, reversed.getArgType(0),
                    c.name + ": timestamptz slot + varchar slot: arg0 should bind to TimeStampTzType");
            Assertions.assertInstanceOf(TimeStampTzType.class, reversed.getArgType(1),
                    c.name + ": timestamptz slot + varchar slot: arg1 should bind to TimeStampTzType");
        }
    }

    /**
     * A call pairing one zoned literal with one zone-less literal has a literal to inspect, so it
     * is searched as on master: the scores cancel to {@code 0} and TIMESTAMPTZ wins by list order.
     * Under {@code time_zone='America/Los_Angeles'},
     * {@code hours_diff('2021-03-14 03:30:00-07:00', '2021-03-14 01:30:00')} therefore stays one
     * elapsed hour across the DST spring-forward boundary, not two civil hours.
     */
    @Test
    public void testMixedZonedAndZonelessLiteralsKeepTimeStampTz() {
        FunctionSignature signature = new HoursDiff(
                new VarcharLiteral("2021-03-14 03:30:00-07:00"),
                new VarcharLiteral("2021-03-14 01:30:00")).getSignature();
        Assertions.assertInstanceOf(TimeStampTzType.class, signature.getArgType(0),
                "zoned literal + zone-less literal: arg0 should bind to TimeStampTzType");
        Assertions.assertInstanceOf(TimeStampTzType.class, signature.getArgType(1),
                "zoned literal + zone-less literal: arg1 should bind to TimeStampTzType");

        // Order must not matter: zone-less first, zoned second must also keep TIMESTAMPTZ.
        FunctionSignature reversed = new HoursDiff(
                new VarcharLiteral("2021-03-14 01:30:00"),
                new VarcharLiteral("2021-03-14 03:30:00-07:00")).getSignature();
        Assertions.assertInstanceOf(TimeStampTzType.class, reversed.getArgType(0),
                "zone-less literal + zoned literal: arg0 should bind to TimeStampTzType");
        Assertions.assertInstanceOf(TimeStampTzType.class, reversed.getArgType(1),
                "zone-less literal + zoned literal: arg1 should bind to TimeStampTzType");
    }

    /**
     * Both literals carry an explicit zone, but {@code DateTimeChecker.hasTimeZone} requires a colon
     * before the offset minutes, so the compact {@code -0700} scores as zone-less even though the
     * cast path accepts it, and the scores cancel to {@code 0} again. The call is searched as on
     * master, so TIMESTAMPTZ wins by list order and, under {@code time_zone='America/Los_Angeles'},
     * {@code hours_diff('2021-03-15 00:00:00-0700', '2021-03-14 00:00:00-08:00')} stays 23 elapsed
     * hours, not 24 civil hours.
     */
    @Test
    public void testCompactAndColonOffsetLiteralsKeepTimeStampTz() {
        FunctionSignature signature = new HoursDiff(
                new VarcharLiteral("2021-03-15 00:00:00-0700"),
                new VarcharLiteral("2021-03-14 00:00:00-08:00")).getSignature();
        Assertions.assertInstanceOf(TimeStampTzType.class, signature.getArgType(0),
                "compact offset + colon offset: arg0 should bind to TimeStampTzType");
        Assertions.assertInstanceOf(TimeStampTzType.class, signature.getArgType(1),
                "compact offset + colon offset: arg1 should bind to TimeStampTzType");

        // Order must not matter: colon offset first, compact offset second must also keep TIMESTAMPTZ.
        FunctionSignature reversed = new HoursDiff(
                new VarcharLiteral("2021-03-14 00:00:00-08:00"),
                new VarcharLiteral("2021-03-15 00:00:00-0700")).getSignature();
        Assertions.assertInstanceOf(TimeStampTzType.class, reversed.getArgType(0),
                "colon offset + compact offset: arg0 should bind to TimeStampTzType");
        Assertions.assertInstanceOf(TimeStampTzType.class, reversed.getArgType(1),
                "colon offset + compact offset: arg1 should bind to TimeStampTzType");
    }

    /**
     * Guards apache/doris#64127's intent: when every inspectable literal is zone-less, the
     * call should still prefer DATETIMEV2 (civil semantics), not TIMESTAMPTZ.
     */
    @Test
    public void testAllZonelessLiteralsUseDateTimeV2() {
        FunctionSignature signature = new HoursDiff(
                new VarcharLiteral("2021-03-14 03:30:00"),
                new VarcharLiteral("2021-03-14 01:30:00")).getSignature();
        Assertions.assertInstanceOf(DateTimeV2Type.class, signature.getArgType(0),
                "all zone-less literals: arg0 should bind to DateTimeV2Type");
        Assertions.assertInstanceOf(DateTimeV2Type.class, signature.getArgType(1),
                "all zone-less literals: arg1 should bind to DateTimeV2Type");
    }

    /**
     * A varchar slot beside a string literal has a literal to inspect, so the literal's zone decides
     * as on master: a zoned literal keeps TIMESTAMPTZ and a zone-less one binds DATETIMEV2.
     */
    @Test
    public void testVarcharSlotWithLiteralFollowsTheLiteral() {
        SlotReference slot = SlotReference.of("a", VarcharType.SYSTEM_DEFAULT);

        FunctionSignature zoned = new HoursDiff(slot, new VarcharLiteral("2021-03-14 03:30:00-07:00"))
                .getSignature();
        Assertions.assertInstanceOf(TimeStampTzType.class, zoned.getArgType(0),
                "varchar slot + zoned literal: arg0 should bind to TimeStampTzType");
        Assertions.assertInstanceOf(TimeStampTzType.class, zoned.getArgType(1),
                "varchar slot + zoned literal: arg1 should bind to TimeStampTzType");

        FunctionSignature zoneless = new HoursDiff(slot, new VarcharLiteral("2021-03-14 03:30:00"))
                .getSignature();
        Assertions.assertInstanceOf(DateTimeV2Type.class, zoneless.getArgType(0),
                "varchar slot + zone-less literal: arg0 should bind to DateTimeV2Type");
        Assertions.assertInstanceOf(DateTimeV2Type.class, zoneless.getArgType(1),
                "varchar slot + zone-less literal: arg1 should bind to DateTimeV2Type");
    }

    /**
     * Binding DATETIMEV2 alone would hand the slot to the DATETIME string parser, which rejects zone
     * offsets the TIMESTAMPTZ parser accepts, such as {@code -14:21}. A call whose overload moved from
     * TIMESTAMPTZ to DATETIMEV2 therefore converts each string argument through TIMESTAMPTZ(6), the
     * cast it got before the overload moved, and reads the instant back as DATETIMEV2(6).
     */
    @Test
    public void testVarcharSlotsAreConvertedThroughTimeStampTz() {
        SlotReference left = SlotReference.of("a", VarcharType.SYSTEM_DEFAULT);
        SlotReference right = SlotReference.of("b", VarcharType.SYSTEM_DEFAULT);
        for (NamedDiffConstructor c : DIFF_CONSTRUCTORS) {
            Expression coerced = TypeCoercionUtils.processBoundFunction(
                    (BoundFunction) c.constructor.apply(left, right));
            Assertions.assertEquals(throughTimeStampTz(left), coerced.child(0),
                    c.name + ": varchar arg0 should be cast to TIMESTAMPTZ(6) and then to DATETIMEV2(6)");
            Assertions.assertEquals(throughTimeStampTz(right), coerced.child(1),
                    c.name + ": varchar arg1 should be cast to TIMESTAMPTZ(6) and then to DATETIMEV2(6)");
            FunctionSignature signature = ((ComputeSignature) coerced).getSignature();
            Assertions.assertInstanceOf(DateTimeV2Type.class, signature.getArgType(0),
                    c.name + ": the converted call should still bind DateTimeV2Type on arg0");
            Assertions.assertInstanceOf(DateTimeV2Type.class, signature.getArgType(1),
                    c.name + ": the converted call should still bind DateTimeV2Type on arg1");

            // The analyzer prepares a function more than once, so a converted call comes back unchanged.
            Assertions.assertSame(coerced, coerced.prepareBeforeTypeCoercion(),
                    c.name + ": a converted call should not be converted again");
            Assertions.assertEquals(coerced, TypeCoercionUtils.processBoundFunction((BoundFunction) coerced),
                    c.name + ": a converted call should be coerced to itself");
        }
    }

    /**
     * A call that binds the overload it bound before is coerced as before: one cast to the bound type
     * and no conversion through TIMESTAMPTZ. That covers a varchar slot beside a typed temporal slot,
     * where the number of casts decides, and beside a string literal, where the literal decides.
     */
    @Test
    public void testCallsThatKeepTheirOverloadAreNotConverted() {
        SlotReference varchar = SlotReference.of("a", VarcharType.SYSTEM_DEFAULT);
        SlotReference timeStampTz = SlotReference.of("b", TimeStampTzType.of(6));
        SlotReference dateTime = SlotReference.of("c", DateTimeV2Type.of(6));
        VarcharLiteral zoned = new VarcharLiteral("2021-03-14 03:30:00-07:00");
        VarcharLiteral zoneless = new VarcharLiteral("2021-03-14 03:30:00");
        for (HoursDiff call : ImmutableList.of(
                new HoursDiff(varchar, timeStampTz), new HoursDiff(timeStampTz, varchar),
                new HoursDiff(varchar, dateTime), new HoursDiff(dateTime, varchar),
                new HoursDiff(varchar, zoned), new HoursDiff(zoned, varchar),
                new HoursDiff(varchar, zoneless), new HoursDiff(zoneless, varchar),
                new HoursDiff(timeStampTz, timeStampTz), new HoursDiff(dateTime, dateTime))) {
            Assertions.assertSame(call, call.prepareBeforeTypeCoercion(), call.toSql());
        }

        Assertions.assertEquals(new Cast(varchar, TimeStampTzType.of(6)),
                TypeCoercionUtils.processBoundFunction(new HoursDiff(varchar, timeStampTz)).child(0),
                "varchar slot + timestamptz slot: the slot should be cast straight to TIMESTAMPTZ(6)");
        Assertions.assertEquals(new Cast(varchar, DateTimeV2Type.of(6)),
                TypeCoercionUtils.processBoundFunction(new HoursDiff(varchar, dateTime)).child(0),
                "varchar slot + datetimev2 slot: the slot should be cast straight to DATETIMEV2(6)");
        Assertions.assertEquals(new Cast(varchar, TimeStampTzType.of(6)),
                TypeCoercionUtils.processBoundFunction(new HoursDiff(varchar, zoned)).child(0),
                "varchar slot + zoned literal: the slot should be cast straight to TIMESTAMPTZ(6)");
        Assertions.assertEquals(new Cast(varchar, DateTimeV2Type.of(6)),
                TypeCoercionUtils.processBoundFunction(new HoursDiff(varchar, zoneless)).child(0),
                "varchar slot + zone-less literal: the slot should be cast straight to DATETIMEV2(6)");
    }

    /**
     * The conversion is reached from SQL: the string outputs of a subquery, the shape of the
     * apache/doris#66120 reproducer, are converted when the statement is analyzed.
     */
    @Test
    public void testSubqueryStringOutputsAreConvertedByTheAnalyzer() {
        String sql = "select hours_diff(a, b)"
                + " from (select '1800-01-01 01:00:00-14:21' as a, '1800-01-01 00:00:00-14:21' as b) t";
        Plan plan = PlanChecker.from(MemoTestUtils.createConnectContext()).analyze(sql).getPlan();
        List<Expression> calls = new ArrayList<>();
        plan.foreach(node -> {
            for (Expression expression : ((Plan) node).getExpressions()) {
                calls.addAll(expression.collectToList(HoursDiff.class::isInstance));
            }
        });
        Assertions.assertEquals(1, calls.size(), sql);
        for (Expression argument : calls.get(0).children()) {
            List<Expression> slots = argument.collectToList(SlotReference.class::isInstance);
            Assertions.assertEquals(1, slots.size(), argument.toSql());
            Assertions.assertEquals(throughTimeStampTz(slots.get(0)), argument, sql);
        }
    }

    /**
     * What the conversion computes, through FE constant folding of string subquery outputs: an offset
     * the DATETIME parser rejects is read, an offset is applied rather than dropped, and a zone-less
     * value keeps its wall-clock time, so the reproducer of apache/doris#66120 still counts 63 civil
     * days. The last case is where the conversion shows: 02:30 does not exist on that day in Los
     * Angeles, TIMESTAMPTZ reads it as 03:00, the first instant after the gap, and the difference is
     * taken from there.
     */
    @Test
    public void testConvertedSubqueryOutputsFoldToWallClockResults() {
        assertFolded("63", "+08:00", "datediff", "2025-08-01 00:00:00", "2025-05-30 11:40:09");
        assertFolded("1", "+08:00", "hours_diff", "1800-01-01 01:00:00-14:21", "1800-01-01 00:00:00-14:21");
        assertFolded("30", "+08:00", "minutes_diff", "2025-08-01 01:30:00+09:00", "2025-08-01 00:00:00");
        assertFolded("150", "America/Los_Angeles", "minutes_diff", "2021-03-14 02:30:00", "2021-03-14 00:30:00");
    }

    private static void assertFolded(String expected, String timeZone, String function, String left,
            String right) {
        String sql = "select " + function + "(a, b) from (select '" + left + "' as a, '" + right + "' as b) t";
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        connectContext.getSessionVariable().setTimeZone(timeZone);
        Plan plan = PlanChecker.from(connectContext).analyze(sql).rewrite().getPlan();
        List<Expression> literals = new ArrayList<>();
        plan.foreach(node -> {
            for (Expression expression : ((Plan) node).getExpressions()) {
                literals.addAll(expression.collectToList(Literal.class::isInstance));
            }
        });
        Assertions.assertEquals(1, literals.size(), sql + " under time_zone " + timeZone + " should fold");
        Assertions.assertEquals(expected, ((Literal) literals.get(0)).getStringValue(),
                sql + " under time_zone " + timeZone);
    }

    private static Expression throughTimeStampTz(Expression argument) {
        return new Cast(new Cast(argument, TimeStampTzType.of(6)), DateTimeV2Type.of(6));
    }

    private static final class NamedDiffConstructor {
        private final String name;
        private final BiFunction<Expression, Expression, ComputeSignature> constructor;

        private NamedDiffConstructor(String name, BiFunction<Expression, Expression, ComputeSignature> constructor) {
            this.name = name;
            this.constructor = constructor;
        }
    }
}
