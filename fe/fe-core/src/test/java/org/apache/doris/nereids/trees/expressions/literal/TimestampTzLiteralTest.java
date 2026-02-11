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
}
