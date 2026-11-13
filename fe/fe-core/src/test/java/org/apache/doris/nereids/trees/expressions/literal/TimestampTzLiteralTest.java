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

package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeArithmetic;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DaysAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DaysSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HoursAdd;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;

class TimestampTzLiteralTest {

    @Test
    void testLocalCalendarDayArithmetic() {
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setTimeZone("America/New_York");
        context.setThreadLocalInfo();
        try {
            String[][] cases = {
                {"2024-03-09 12:00:00.123456-05:00", "1", "2024-03-10 12:00:00.123456-04:00", "82800"},
                {"2024-11-02 12:00:00.123456-04:00", "1", "2024-11-03 12:00:00.123456-05:00", "90000"},
                {"2024-03-10 12:00:00.123456-04:00", "-1", "2024-03-09 12:00:00.123456-05:00", "-82800"},
                {"2024-11-03 12:00:00.123456-05:00", "-1", "2024-11-02 12:00:00.123456-04:00", "-90000"},
                {"2024-02-01 12:00:00.123456-05:00", "1", "2024-02-02 12:00:00.123456-05:00", "86400"},
                {"2024-11-03 01:30:00.123456-05:00", "0", "2024-11-03 01:30:00.123456-05:00", "0"},
                {"2024-11-02 01:30:00.123456-04:00", "1", "2024-11-03 01:30:00.123456-04:00", "86400"},
                {"2024-11-04 01:30:00.123456-05:00", "-1", "2024-11-03 01:30:00.123456-05:00", "-86400"},
                {"2024-03-09 02:30:00.123456-05:00", "1", "2024-03-10 03:30:00.123456-04:00", "86400"},
                {"2024-03-11 02:30:00.123456-04:00", "-1", "2024-03-10 03:30:00.123456-04:00", "-82800"}
            };
            for (String[] testCase : cases) {
                TimestampTzLiteral input = new TimestampTzLiteral(TimeStampTzType.of(6), testCase[0]);
                IntegerLiteral days = new IntegerLiteral(Integer.parseInt(testCase[1]));
                TimestampTzLiteral expected = new TimestampTzLiteral(TimeStampTzType.of(6), testCase[2]);
                Expression result = DateTimeArithmetic.daysAdd(input, days);
                Assertions.assertEquals(expected, result, testCase[0]);
                Assertions.assertEquals(expected, DateTimeArithmetic.dateAdd(input, days));
                Assertions.assertEquals(expected, DateTimeArithmetic.daysSub(input,
                        new IntegerLiteral(-days.getValue())));
                Assertions.assertEquals(Long.parseLong(testCase[3]), Duration.between(
                        input.toJavaDateType(), ((TimestampTzLiteral) result).toJavaDateType()).getSeconds());
            }
            TimestampTzLiteral input = new TimestampTzLiteral(TimeStampTzType.of(6), cases[0][0]);
            Assertions.assertEquals(new TimestampTzLiteral(TimeStampTzType.of(6),
                    "2024-03-10 13:00:00.123456-04:00"),
                    DateTimeArithmetic.hoursAdd(input, new IntegerLiteral(24)));
            context.getSessionVariable().setTimeZone("UTC");
            Assertions.assertEquals(new TimestampTzLiteral(TimeStampTzType.of(6),
                    "2024-03-10 17:00:00.123456+00:00"),
                    DateTimeArithmetic.daysAdd(input, new IntegerLiteral(1)));
            TimestampTzLiteral minimum = new TimestampTzLiteral(0, 1, 1, 0, 0, 0);
            TimestampTzLiteral maximum = new TimestampTzLiteral(9999, 12, 31, 1, 0, 0);
            Assertions.assertThrows(AnalysisException.class, () -> minimum.plusDays(-1));
            Assertions.assertThrows(AnalysisException.class, () -> maximum.plusDays(1));
            context.getSessionVariable().setTimeZone("-08:00");
            Assertions.assertThrows(AnalysisException.class, () -> maximum.plusDays(1));
        } finally {
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

    @Test
    void testCalendarDayMonotonicity() {
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        try {
            SlotReference timestamp = new SlotReference("ts", TimeStampTzType.of(6));
            TimestampTzLiteral lower = new TimestampTzLiteral("2024-11-03 01:50:00-04:00");
            TimestampTzLiteral upper = new TimestampTzLiteral("2024-11-03 01:10:00-05:00");
            DaysAdd add = new DaysAdd(timestamp, new IntegerLiteral(1));
            DaysSub sub = new DaysSub(timestamp, new IntegerLiteral(1));
            context.getSessionVariable().setTimeZone("America/New_York");
            Assertions.assertFalse(add.isMonotonic(lower, upper));
            Assertions.assertFalse(sub.isMonotonic(lower, upper));
            Assertions.assertTrue(lower.toJavaDateType().isBefore(upper.toJavaDateType()));
            Assertions.assertTrue(((TimestampTzLiteral) lower.plusDays(1)).toJavaDateType().isAfter(
                    ((TimestampTzLiteral) upper.plusDays(1)).toJavaDateType()));
            Assertions.assertTrue(new HoursAdd(timestamp, new IntegerLiteral(24)).isMonotonic(lower, upper));
            SlotReference datetime = new SlotReference("dt", DateTimeV2Type.of(6));
            Assertions.assertTrue(new DaysAdd(datetime, new IntegerLiteral(1)).isMonotonic(lower, upper));
            for (String zone : new String[] {"UTC", "+08:00"}) {
                context.getSessionVariable().setTimeZone(zone);
                Assertions.assertTrue(add.isMonotonic(lower, upper));
                Assertions.assertTrue(sub.isMonotonic(lower, upper));
            }
        } finally {
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

    @Test
    void testCalendarDaysAtUtcBoundaries() {
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        try {
            TimestampTzLiteral minimum = new TimestampTzLiteral(0, 1, 1, 0, 0, 0);
            TimestampTzLiteral maximum = new TimestampTzLiteral(9999, 12, 31, 23, 0, 0);
            context.getSessionVariable().setTimeZone("America/New_York");
            Assertions.assertEquals(minimum, minimum.plusDays(0));
            context.getSessionVariable().setTimeZone("+08:00");
            Assertions.assertEquals(maximum, maximum.plusDays(0));
            TimestampTzLiteral east = new TimestampTzLiteral(TimeStampTzType.of(6),
                    0, 2, 28, 20, 0, 0, 123456);
            Assertions.assertEquals(new TimestampTzLiteral(TimeStampTzType.of(6),
                    0, 2, 27, 20, 0, 0, 123456), east.plusDays(-1));
            context.getSessionVariable().setTimeZone("-08:00");
            TimestampTzLiteral west = new TimestampTzLiteral(TimeStampTzType.of(6),
                    0, 3, 1, 1, 0, 0, 123456);
            Assertions.assertEquals(new TimestampTzLiteral(TimeStampTzType.of(6),
                    0, 3, 2, 1, 0, 0, 123456), west.plusDays(1));
        } finally {
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

    @Test
    void testConstructorsAndParsing() {
        TimestampTzLiteral literal;

        literal = new TimestampTzLiteral("2022-01-01 14:00:00+02:00");
        Assertions.assertEquals(2022, literal.year);
        Assertions.assertEquals(1, literal.month);
        Assertions.assertEquals(1, literal.day);
        Assertions.assertEquals(12, literal.hour);
        Assertions.assertEquals(0, literal.minute);
        Assertions.assertEquals(0, literal.second);
        Assertions.assertEquals(0, literal.microSecond);

        literal = new TimestampTzLiteral("20220101120000Z");
        Assertions.assertEquals(2022, literal.year);
        Assertions.assertEquals(1, literal.month);
        Assertions.assertEquals(1, literal.day);
        Assertions.assertEquals(12, literal.hour);
        Assertions.assertEquals(0, literal.minute);
        Assertions.assertEquals(0, literal.second);
        Assertions.assertEquals(0, literal.microSecond);

        literal = new TimestampTzLiteral("2022-12-31 21:45:14.1234567-06:00");
        Assertions.assertEquals(2023, literal.year);
        Assertions.assertEquals(1, literal.month);
        Assertions.assertEquals(1, literal.day);
        Assertions.assertEquals(3, literal.hour);
        Assertions.assertEquals(45, literal.minute);
        Assertions.assertEquals(14, literal.second);
        Assertions.assertEquals(123457, literal.microSecond);
        Assertions.assertEquals(6, literal.getDataType().getScale());

        literal = new TimestampTzLiteral("2022-12-31 21:00:14.1234567  -06:45");
        Assertions.assertEquals(2023, literal.year);
        Assertions.assertEquals(1, literal.month);
        Assertions.assertEquals(1, literal.day);
        Assertions.assertEquals(3, literal.hour);
        Assertions.assertEquals(45, literal.minute);
        Assertions.assertEquals(14, literal.second);
        Assertions.assertEquals(123457, literal.microSecond);
        Assertions.assertEquals(6, literal.getDataType().getScale());

        literal = new TimestampTzLiteral("2022-01-01 12:00:00    uTc");
        Assertions.assertEquals(2022, literal.year);
        Assertions.assertEquals(1, literal.month);
        Assertions.assertEquals(1, literal.day);
        Assertions.assertEquals(12, literal.hour);
        Assertions.assertEquals(0, literal.minute);
        Assertions.assertEquals(0, literal.second);
        Assertions.assertEquals(0, literal.microSecond);

        literal = new TimestampTzLiteral(TimeStampTzType.of(6), "2024-03-10 02:30:00 America/New_York");
        Assertions.assertEquals(2024, literal.year);
        Assertions.assertEquals(3, literal.month);
        Assertions.assertEquals(10, literal.day);
        Assertions.assertEquals(7, literal.hour);
        Assertions.assertEquals(0, literal.minute);
        Assertions.assertEquals(0, literal.second);
        Assertions.assertEquals(0, literal.microSecond);

        literal = new TimestampTzLiteral(TimeStampTzType.of(6), "2024-11-03 01:30:00 America/New_York");
        Assertions.assertEquals(2024, literal.year);
        Assertions.assertEquals(11, literal.month);
        Assertions.assertEquals(3, literal.day);
        Assertions.assertEquals(5, literal.hour);
        Assertions.assertEquals(30, literal.minute);
        Assertions.assertEquals(0, literal.second);
        Assertions.assertEquals(0, literal.microSecond);
    }

    @Test
    void testFromSessionTimeZoneWithoutExplicitOffset() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setTimeZone("America/New_York");
        context.setThreadLocalInfo();
        try {
            TimestampTzLiteral literal = TimestampTzLiteral.fromSessionTimeZone(
                    TimeStampTzType.of(6), "2024-01-15 12:00:00");

            Assertions.assertEquals(2024, literal.year);
            Assertions.assertEquals(1, literal.month);
            Assertions.assertEquals(15, literal.day);
            Assertions.assertEquals(17, literal.hour);
            Assertions.assertEquals(0, literal.minute);
            Assertions.assertEquals(0, literal.second);
            Assertions.assertEquals(0, literal.microSecond);
            Assertions.assertEquals(6, literal.getDataType().getScale());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    void testGetStringValueKeepsUtcSuffix() {
        TimestampTzLiteral literal = new TimestampTzLiteral(TimeStampTzType.of(6),
                "2024-01-15 12:00:00 +00:00");

        Assertions.assertEquals("2024-01-15 12:00:00.000000+00:00", literal.getStringValue());
    }

    @Test
    void testBasicArithmetic() {
        TimestampTzLiteral base1 = new TimestampTzLiteral("2025-12-31 23:59:59.999999");
        TimestampTzLiteral base2 = new TimestampTzLiteral("2024-2-29 0:0:0");

        // plusDays
        Expression expr = base1.plusDays(1);
        Assertions.assertEquals(2026, ((TimestampTzLiteral) expr).year);
        Assertions.assertEquals(1, ((TimestampTzLiteral) expr).month);
        Assertions.assertEquals(1, ((TimestampTzLiteral) expr).day);
        Assertions.assertEquals(23, ((TimestampTzLiteral) expr).hour);
        Assertions.assertEquals(59, ((TimestampTzLiteral) expr).minute);
        Assertions.assertEquals(59, ((TimestampTzLiteral) expr).second);
        Assertions.assertEquals(999999, ((TimestampTzLiteral) expr).microSecond);

        // plusMonths
        expr = base1.plusMonths(2);
        Assertions.assertEquals(2026, ((TimestampTzLiteral) expr).year);
        Assertions.assertEquals(2, ((TimestampTzLiteral) expr).month);
        Assertions.assertEquals(28, ((TimestampTzLiteral) expr).day);
        Assertions.assertEquals(23, ((TimestampTzLiteral) expr).hour);
        Assertions.assertEquals(59, ((TimestampTzLiteral) expr).minute);
        Assertions.assertEquals(59, ((TimestampTzLiteral) expr).second);
        Assertions.assertEquals(999999, ((TimestampTzLiteral) expr).microSecond);

        // plusYears
        expr = base2.plusYears(1);
        Assertions.assertEquals(2025, ((TimestampTzLiteral) expr).year);
        Assertions.assertEquals(2, ((TimestampTzLiteral) expr).month);
        Assertions.assertEquals(28, ((TimestampTzLiteral) expr).day);
        Assertions.assertEquals(0, ((TimestampTzLiteral) expr).hour);
        Assertions.assertEquals(0, ((TimestampTzLiteral) expr).minute);
        Assertions.assertEquals(0, ((TimestampTzLiteral) expr).second);
        Assertions.assertEquals(0, ((TimestampTzLiteral) expr).microSecond);

        // plusHours
        expr = base1.plusHours(1);
        Assertions.assertEquals(2026, ((TimestampTzLiteral) expr).year);
        Assertions.assertEquals(1, ((TimestampTzLiteral) expr).month);
        Assertions.assertEquals(1, ((TimestampTzLiteral) expr).day);
        Assertions.assertEquals(0, ((TimestampTzLiteral) expr).hour);
        Assertions.assertEquals(59, ((TimestampTzLiteral) expr).minute);
        Assertions.assertEquals(59, ((TimestampTzLiteral) expr).second);
        Assertions.assertEquals(999999, ((TimestampTzLiteral) expr).microSecond);

        // plusMinutes
        expr = base1.plusMinutes(1);
        Assertions.assertEquals(2026, ((TimestampTzLiteral) expr).year);
        Assertions.assertEquals(1, ((TimestampTzLiteral) expr).month);
        Assertions.assertEquals(1, ((TimestampTzLiteral) expr).day);
        Assertions.assertEquals(0, ((TimestampTzLiteral) expr).hour);
        Assertions.assertEquals(0, ((TimestampTzLiteral) expr).minute);
        Assertions.assertEquals(59, ((TimestampTzLiteral) expr).second);
        Assertions.assertEquals(999999, ((TimestampTzLiteral) expr).microSecond);

        // plusSeconds
        expr = base1.plusSeconds(1);
        Assertions.assertEquals(2026, ((TimestampTzLiteral) expr).year);
        Assertions.assertEquals(1, ((TimestampTzLiteral) expr).month);
        Assertions.assertEquals(1, ((TimestampTzLiteral) expr).day);
        Assertions.assertEquals(0, ((TimestampTzLiteral) expr).hour);
        Assertions.assertEquals(0, ((TimestampTzLiteral) expr).minute);
        Assertions.assertEquals(0, ((TimestampTzLiteral) expr).second);
        Assertions.assertEquals(999999, ((TimestampTzLiteral) expr).microSecond);

        // plusMicroSeconds
        expr = base1.plusMicroSeconds(2);
        Assertions.assertEquals(2026, ((TimestampTzLiteral) expr).year);
        Assertions.assertEquals(1, ((TimestampTzLiteral) expr).month);
        Assertions.assertEquals(1, ((TimestampTzLiteral) expr).day);
        Assertions.assertEquals(0, ((TimestampTzLiteral) expr).hour);
        Assertions.assertEquals(0, ((TimestampTzLiteral) expr).minute);
        Assertions.assertEquals(0, ((TimestampTzLiteral) expr).second);
        Assertions.assertEquals(1, ((TimestampTzLiteral) expr).microSecond);

        // plusMilliSeconds
        expr = base1.plusMilliSeconds(1);
        Assertions.assertEquals(2026, ((TimestampTzLiteral) expr).year);
        Assertions.assertEquals(1, ((TimestampTzLiteral) expr).month);
        Assertions.assertEquals(1, ((TimestampTzLiteral) expr).day);
        Assertions.assertEquals(0, ((TimestampTzLiteral) expr).hour);
        Assertions.assertEquals(0, ((TimestampTzLiteral) expr).minute);
        Assertions.assertEquals(0, ((TimestampTzLiteral) expr).second);
        Assertions.assertEquals(999, ((TimestampTzLiteral) expr).microSecond);
    }

    @Test
    void testPlusDayHour() {
        TimestampTzLiteral timestampTzLiteral = new TimestampTzLiteral(2021, 1, 1, 0, 0, 0);
        Expression expression = timestampTzLiteral.plusDayHour(new VarcharLiteral("1 1"));
        TimestampTzLiteral result = (TimestampTzLiteral) expression;
        Assertions.assertEquals(2021, result.year);
        Assertions.assertEquals(1, result.month);
        Assertions.assertEquals(2, result.day);
        Assertions.assertEquals(1, result.hour);
        Assertions.assertEquals(0, result.minute);
        Assertions.assertEquals(0, result.second);

        expression = timestampTzLiteral.plusDayHour(new VarcharLiteral("1 -1"));
        result = (TimestampTzLiteral) expression;
        Assertions.assertEquals(2021, result.year);
        Assertions.assertEquals(1, result.month);
        Assertions.assertEquals(2, result.day);
        Assertions.assertEquals(1, result.hour);

        expression = timestampTzLiteral.plusDayHour(new VarcharLiteral("-1 1"));
        result = (TimestampTzLiteral) expression;
        Assertions.assertEquals(2020, result.year);
        Assertions.assertEquals(12, result.month);
        Assertions.assertEquals(30, result.day);
        Assertions.assertEquals(23, result.hour);

        expression = timestampTzLiteral.plusDayHour(new VarcharLiteral("-1 -1"));
        result = (TimestampTzLiteral) expression;
        Assertions.assertEquals(2020, result.year);
        Assertions.assertEquals(12, result.month);
        Assertions.assertEquals(30, result.day);
        Assertions.assertEquals(23, result.hour);
    }

    @Test
    void testPlusDaySecond() {
        TimestampTzLiteral timestampTzLiteral = new TimestampTzLiteral(2021, 1, 1, 0, 0, 0);
        Expression expression = timestampTzLiteral.plusDaySecond(new VarcharLiteral("1 1:1:1"));
        TimestampTzLiteral result = (TimestampTzLiteral) expression;
        Assertions.assertEquals(2, result.day);
        Assertions.assertEquals(1, result.hour);
        Assertions.assertEquals(1, result.minute);
        Assertions.assertEquals(1, result.second);

        expression = timestampTzLiteral.plusDaySecond(new VarcharLiteral("-1 1:1:1"));
        result = (TimestampTzLiteral) expression;
        Assertions.assertEquals(2020, result.year);
        Assertions.assertEquals(12, result.month);
        Assertions.assertEquals(30, result.day);
        Assertions.assertEquals(22, result.hour);
        Assertions.assertEquals(58, result.minute);
        Assertions.assertEquals(59, result.second);
    }

    @Test
    void testPlusMinuteSecond() {
        TimestampTzLiteral timestampTzLiteral = new TimestampTzLiteral(2021, 1, 1, 0, 0, 0);
        Expression expression = timestampTzLiteral.plusMinuteSecond(new VarcharLiteral("1:1"));
        TimestampTzLiteral result = (TimestampTzLiteral) expression;
        Assertions.assertEquals(2021, result.year);
        Assertions.assertEquals(1, result.month);
        Assertions.assertEquals(1, result.day);
        Assertions.assertEquals(0, result.hour);
        Assertions.assertEquals(1, result.minute);
        Assertions.assertEquals(1, result.second);

        expression = timestampTzLiteral.plusMinuteSecond(new VarcharLiteral("1:-1"));
        result = (TimestampTzLiteral) expression;
        Assertions.assertEquals(2021, result.year);
        Assertions.assertEquals(1, result.month);
        Assertions.assertEquals(1, result.day);
        Assertions.assertEquals(0, result.hour);
        Assertions.assertEquals(1, result.minute);
        Assertions.assertEquals(1, result.second);

        expression = timestampTzLiteral.plusMinuteSecond(new VarcharLiteral("-1:1"));
        result = (TimestampTzLiteral) expression;
        Assertions.assertEquals(2020, result.year);
        Assertions.assertEquals(12, result.month);
        Assertions.assertEquals(31, result.day);
        Assertions.assertEquals(23, result.hour);
        Assertions.assertEquals(58, result.minute);
        Assertions.assertEquals(59, result.second);

        expression = timestampTzLiteral.plusMinuteSecond(new VarcharLiteral("-1:-1"));
        result = (TimestampTzLiteral) expression;
        Assertions.assertEquals(2020, result.year);
        Assertions.assertEquals(12, result.month);
        Assertions.assertEquals(31, result.day);
        Assertions.assertEquals(23, result.hour);
        Assertions.assertEquals(58, result.minute);
        Assertions.assertEquals(59, result.second);
    }

    @Test
    void testPlusSecondMicrosecond() {
        TimestampTzLiteral timestampTzLiteral = new TimestampTzLiteral(TimeStampTzType.of(6), 2021, 1, 1, 0, 0, 0, 0);
        Expression expression = timestampTzLiteral.plusSecondMicrosecond(new VarcharLiteral("1.1"));
        TimestampTzLiteral result = (TimestampTzLiteral) expression;
        Assertions.assertEquals(2021, result.year);
        Assertions.assertEquals(1, result.month);
        Assertions.assertEquals(1, result.day);
        Assertions.assertEquals(0, result.hour);
        Assertions.assertEquals(0, result.minute);
        Assertions.assertEquals(1, result.second);
        Assertions.assertEquals(100000, result.microSecond);

        expression = timestampTzLiteral.plusSecondMicrosecond(new VarcharLiteral("1.-1"));
        result = (TimestampTzLiteral) expression;
        Assertions.assertEquals(2021, result.year);
        Assertions.assertEquals(1, result.month);
        Assertions.assertEquals(1, result.day);
        Assertions.assertEquals(0, result.hour);
        Assertions.assertEquals(0, result.minute);
        Assertions.assertEquals(1, result.second);
        Assertions.assertEquals(100000, result.microSecond);

        expression = timestampTzLiteral.plusSecondMicrosecond(new VarcharLiteral("-1.1"));
        result = (TimestampTzLiteral) expression;
        Assertions.assertEquals(2020, result.year);
        Assertions.assertEquals(12, result.month);
        Assertions.assertEquals(31, result.day);
        Assertions.assertEquals(23, result.hour);
        Assertions.assertEquals(59, result.minute);
        Assertions.assertEquals(58, result.second);
        Assertions.assertEquals(900000, result.microSecond);

        expression = timestampTzLiteral.plusSecondMicrosecond(new VarcharLiteral("-1.-1"));
        result = (TimestampTzLiteral) expression;
        Assertions.assertEquals(2020, result.year);
        Assertions.assertEquals(12, result.month);
        Assertions.assertEquals(31, result.day);
        Assertions.assertEquals(23, result.hour);
        Assertions.assertEquals(59, result.minute);
        Assertions.assertEquals(58, result.second);
        Assertions.assertEquals(900000, result.microSecond);
    }

    @Test
    void testRoundMicroSecondWithOverflow() {
        // Scale=3: rounding 9995 → 10000, overflow microsecond to 1000000, should carry 1 second
        // This previously crashed with DateTimeParseException because roundMicroSecond
        // tried to reparse getStringValue() which includes a "+00:00" suffix.
        TimestampTzLiteral literal = new TimestampTzLiteral(
                TimeStampTzType.of(3), "2024-01-01 00:00:00.9995 +00:00");
        Assertions.assertEquals(2024, literal.year);
        Assertions.assertEquals(1, literal.month);
        Assertions.assertEquals(1, literal.day);
        Assertions.assertEquals(0, literal.hour);
        Assertions.assertEquals(0, literal.minute);
        Assertions.assertEquals(1, literal.second);
        Assertions.assertEquals(0, literal.microSecond);
        Assertions.assertEquals(3, literal.getDataType().getScale());
        Assertions.assertEquals("2024-01-01 00:00:01.000+00:00", literal.getStringValue());
    }

    @Test
    void testRoundMicroSecondWithOverflowAndTimezone() {
        // Rounding overflow with a non-UTC timezone string (parsed via explicit zone path)
        TimestampTzLiteral literal = new TimestampTzLiteral(
                TimeStampTzType.of(3), "2024-01-01 00:00:00.9995 America/New_York");
        Assertions.assertEquals(2024, literal.year);
        Assertions.assertEquals(1, literal.month);
        Assertions.assertEquals(1, literal.day);
        Assertions.assertEquals(5, literal.hour);
        Assertions.assertEquals(0, literal.minute);
        Assertions.assertEquals(1, literal.second);
        Assertions.assertEquals(0, literal.microSecond);
        Assertions.assertEquals(3, literal.getDataType().getScale());
    }

    @Test
    void testRoundMicroSecondWithoutOverflow() {
        // Scale=3: rounding 9994 → 999, no overflow
        TimestampTzLiteral literal = new TimestampTzLiteral(
                TimeStampTzType.of(3), "2024-06-15 12:30:45.9994 +00:00");
        Assertions.assertEquals(2024, literal.year);
        Assertions.assertEquals(6, literal.month);
        Assertions.assertEquals(15, literal.day);
        Assertions.assertEquals(12, literal.hour);
        Assertions.assertEquals(30, literal.minute);
        Assertions.assertEquals(45, literal.second);
        // microSecond stores microseconds: 999ms = 999000µs
        Assertions.assertEquals(999000, literal.microSecond);
    }

    @Test
    void testRoundMicroSecondOverflowCrossDateBoundary() {
        // Scale=3: 23:59:59.9995 → overflows to next day 00:00:00
        TimestampTzLiteral literal = new TimestampTzLiteral(
                TimeStampTzType.of(3), "2024-01-01 23:59:59.9995 +00:00");
        Assertions.assertEquals(2024, literal.year);
        Assertions.assertEquals(1, literal.month);
        Assertions.assertEquals(2, literal.day);
        Assertions.assertEquals(0, literal.hour);
        Assertions.assertEquals(0, literal.minute);
        Assertions.assertEquals(0, literal.second);
        Assertions.assertEquals(0, literal.microSecond);
    }

    @Test
    void testFromTimeZoneExplicit() {
        // fromTimeZone with explicit timezone should convert from given tz to UTC
        TimestampTzLiteral literal = TimestampTzLiteral.fromTimeZone(
                TimeStampTzType.of(6), "2024-01-15 00:00:00", "America/New_York");
        Assertions.assertEquals(2024, literal.year);
        Assertions.assertEquals(1, literal.month);
        Assertions.assertEquals(15, literal.day);
        Assertions.assertEquals(5, literal.hour);
        Assertions.assertEquals(0, literal.minute);
        Assertions.assertEquals(0, literal.second);
        Assertions.assertEquals(0, literal.microSecond);
    }

    @Test
    void testFromTimeZoneExplicitWithOffsetString() {
        // fromTimeZone with a string that already has a timezone should use the string's timezone
        TimestampTzLiteral literal = TimestampTzLiteral.fromTimeZone(
                TimeStampTzType.of(6), "2024-01-15 00:00:00 +05:00", "America/New_York");
        // +05:00 in string should take precedence; 00:00:00 +05:00 → 2024-01-14 19:00:00 UTC
        Assertions.assertEquals(2024, literal.year);
        Assertions.assertEquals(1, literal.month);
        Assertions.assertEquals(14, literal.day);
        Assertions.assertEquals(19, literal.hour);
        Assertions.assertEquals(0, literal.minute);
        Assertions.assertEquals(0, literal.second);
        Assertions.assertEquals(0, literal.microSecond);
    }
}
