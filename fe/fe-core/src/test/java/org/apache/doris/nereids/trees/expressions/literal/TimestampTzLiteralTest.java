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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.TimeStampTzType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TimestampTzLiteralTest {

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

    /**
     * Regression test for DST spring-forward gap handling.
     *
     * 2024-03-10 02:30:00 does not exist in America/New_York (clocks spring forward
     * from 02:00 EST to 03:00 EDT). Java's ZonedDateTime snaps this to 03:30 EDT
     * = 07:30 UTC. Both the named-timezone path and the implicit-session-timezone path
     * must resolve to the same UTC instant.
     *
     * Bug: the named-timezone path applied the *post-gap* UTC offset (-04:00 EDT) to
     * the *pre-snap* local time (02:30), producing 06:30 UTC instead of 07:30 UTC.
     */
    @Test
    void testSpringForwardGapConsistency() {
        // Path 1: named timezone suffix in the literal
        // 2024-03-10 02:30:00 America/New_York is inside the spring-forward gap.
        // Java ZonedDateTime snaps it to 03:30 EDT = 07:30 UTC.
        TimestampTzLiteral namedTz = new TimestampTzLiteral("2024-03-10 02:30:00 America/New_York");
        // UTC fields stored in the literal must reflect 07:30 UTC
        Assertions.assertEquals(2024, namedTz.year, "year mismatch for named-tz path");
        Assertions.assertEquals(3, namedTz.month, "month mismatch for named-tz path");
        Assertions.assertEquals(10, namedTz.day, "day mismatch for named-tz path");
        Assertions.assertEquals(7, namedTz.hour, "hour mismatch for named-tz path (expected 07:30 UTC after snap)");
        Assertions.assertEquals(30, namedTz.minute, "minute mismatch for named-tz path");
        Assertions.assertEquals(0, namedTz.second, "second mismatch for named-tz path");

        // Times just before/after the gap should be unaffected.
        // 01:30 EST (-05:00) = 06:30 UTC
        TimestampTzLiteral beforeGap = new TimestampTzLiteral("2024-03-10 01:30:00 America/New_York");
        Assertions.assertEquals(6, beforeGap.hour, "before-gap hour should be 06 UTC");
        Assertions.assertEquals(30, beforeGap.minute, "before-gap minute should be 30");

        // 03:30 EDT (-04:00) = 07:30 UTC
        TimestampTzLiteral afterGap = new TimestampTzLiteral("2024-03-10 03:30:00 America/New_York");
        Assertions.assertEquals(7, afterGap.hour, "after-gap hour should be 07 UTC");
        Assertions.assertEquals(30, afterGap.minute, "after-gap minute should be 30");

        // The gap-time result must equal the after-gap result (both snap to 07:30 UTC).
        Assertions.assertEquals(afterGap.hour, namedTz.hour,
                "gap time and post-gap time must resolve to same UTC hour");
        Assertions.assertEquals(afterGap.minute, namedTz.minute,
                "gap time and post-gap time must resolve to same UTC minute");
    }
}
