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

import org.apache.doris.nereids.types.DateTimeV2Type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

class DateTimeLiteralTest {
    @Test
    void reject() {
        // Assertions.assertThrows(IllegalArgumentException.class, () -> {
        //     new DateTimeV2Literal("2022-08-01T01:01:01-00:00");
        // });
    }

    @Test
    void mysqlStrangeCase() {
        new DateTimeV2Literal("0-08-01 13:21:03");
        new DateTimeV2Literal("0001-01-01: 00:01:01.001");
        new DateTimeV2Literal("2021?01?01 00.00.00");
    }

    @Test
    void testBasic() {
        Consumer<DateTimeV2Literal> assertFunc = (datetime) -> {
            Assertions.assertEquals(2022, datetime.year);
            Assertions.assertEquals(8, datetime.month);
            Assertions.assertEquals(1, datetime.day);
            Assertions.assertEquals(1, datetime.hour);
            Assertions.assertEquals(1, datetime.minute);
            Assertions.assertEquals(2, datetime.second);
        };

        assertFunc.accept(new DateTimeV2Literal("20220801010102"));
        assertFunc.accept(new DateTimeV2Literal("20220801T010102"));
        assertFunc.accept(new DateTimeV2Literal("220801010102"));
        assertFunc.accept(new DateTimeV2Literal("220801T010102"));
        assertFunc.accept(new DateTimeV2Literal("20220801010101.9999999"));
    }

    @Test
    void testMicrosecond() {
        DateTimeV2Literal literal;
        literal = new DateTimeV2Literal("2016-07-02 00:00:00.123");
        Assertions.assertEquals(123000, literal.microSecond);
        literal = new DateTimeV2Literal("2016-07-02 00:00:00.123456");
        Assertions.assertEquals(123456, literal.microSecond);
        literal = new DateTimeV2Literal("2016-07-02 00:00:00.1");
        Assertions.assertEquals(100000, literal.microSecond);
        literal = new DateTimeV2Literal("2016-07-02 00:00:00.000001");
        Assertions.assertEquals(1, literal.microSecond);
        literal = new DateTimeV2Literal("2016-07-02 00:00:00.12345");
        Assertions.assertEquals(123450, literal.microSecond);
    }

    @Test
    void testWithoutZoneOrOffset() {
        new DateTimeV2Literal("2022-08-01");

        new DateTimeV2Literal("2022-08-01 01:01:01");
        new DateTimeV2Literal("2022-08-01 01:01");
        new DateTimeV2Literal("2022-08-01 01");

        new DateTimeV2Literal("2022-08-01T01:01:01");
        new DateTimeV2Literal("2022-08-01T01:01");
        new DateTimeV2Literal("2022-08-01T01");

        new DateTimeV2Literal("22-08-01T01:01:01");
        new DateTimeV2Literal("22-08-01T01:01");
        new DateTimeV2Literal("22-08-01T01");
    }

    @Test
    void testDetermineScale() {
        int scale = DateTimeLiteral.determineScale("2022-08-01T01:01:01.0");
        Assertions.assertEquals(0, scale);
        scale = DateTimeLiteral.determineScale("2022-08-01T01:01:01.00000");
        Assertions.assertEquals(0, scale);
        scale = DateTimeLiteral.determineScale("2022-08-01T01:01:01.000001");
        Assertions.assertEquals(6, scale);
        scale = DateTimeLiteral.determineScale("2022-08-01T01:01:01.123456");
        Assertions.assertEquals(6, scale);
        scale = DateTimeLiteral.determineScale("2022-08-01T01:01:01.0001");
        Assertions.assertEquals(4, scale);
        scale = DateTimeLiteral.determineScale("2022-08-01T01:01:01.00010");
        Assertions.assertEquals(4, scale);
        scale = DateTimeLiteral.determineScale("2022-08-01T01:01:01.12010");
        Assertions.assertEquals(4, scale);
        scale = DateTimeLiteral.determineScale("2022-08-01T01:01:01.02010");
        Assertions.assertEquals(4, scale);
    }

    @Test
    void testTwoDigitYear() {
        new DateTimeV2Literal("22-08-01T01");
        new DateTimeV2Literal("22-08-01 01");
        new DateTimeV2Literal("22-08-01T01:01");
        new DateTimeV2Literal("22-08-01 01:01");
        new DateTimeV2Literal("22-08-01T01:01:01");
        new DateTimeV2Literal("22-08-01 01:01:01");
        new DateTimeV2Literal("22-08-01T01");
        new DateTimeV2Literal("22-08-01 01");
        new DateTimeV2Literal("22-08-01T01:01");
        new DateTimeV2Literal("22-08-01 01:01");
        new DateTimeV2Literal("22-08-01T01:01:01");
        new DateTimeV2Literal("22-08-01 01:01:01");
    }

    @Test
    void testZone() {
        new DateTimeV2Literal("2022-08-01 01:01:01UTC");
        new DateTimeV2Literal("2022-08-01 01:01:01UT");
        new DateTimeV2Literal("2022-08-01 01:01:01GMT");
        new DateTimeV2Literal("2022-08-01 01:01:01Z");
        new DateTimeV2Literal("2022-08-01 01:01:01Europe/London");
        new DateTimeV2Literal("2022-08-01 01:01:01America/New_York");
        new DateTimeV2Literal("2022-08-01 01:01:01Z");
        new DateTimeV2Literal("2022-08-01 01:01:01Europe/Berlin");
        new DateTimeV2Literal("2022-08-01 01:01:01Europe/London");
        new DateTimeV2Literal("2022-08-01 00:00:00Asia/Shanghai");
    }

    @Test
    void testTwoDigitalYearZone() {
        new DateTimeV2Literal("22-08-01 01:01:01UTC");
        new DateTimeV2Literal("22-08-01 01:01:01UT");
        new DateTimeV2Literal("22-08-01 01:01:01GMT");
        new DateTimeV2Literal("22-08-01 01:01:01Z");
        new DateTimeV2Literal("22-08-01 01:01:01Europe/London");
        new DateTimeV2Literal("22-08-01 01:01:01UTC");
        new DateTimeV2Literal("22-08-01 01:01:01America/New_York");
        new DateTimeV2Literal("22-08-01 01:01:01Z");
        new DateTimeV2Literal("22-08-01 01:01:01Europe/Berlin");
        new DateTimeV2Literal("22-08-01 01:01:01Europe/London");
    }

    @Test
    @Disabled
    void testTwoDigitalYearZoneOffset() {
        new DateTimeV2Literal("22-08-01 01:01:01UTC+01:01:01");
        new DateTimeV2Literal("22-08-01 01:01:01UTC+1:1:1");

        new DateTimeV2Literal("22-08-01 01:01:01UTC+01:01");

        new DateTimeV2Literal("22-08-01 01:01:01UTC+01");
        new DateTimeV2Literal("22-08-01 01:01:01UTC+1");
    }

    @Test
    void testOffset() {
        new DateTimeV2Literal("2022-05-01 01:02:55+02:30");
        new DateTimeV2Literal("2022-05-01 01:02:55.123-02:30");
        new DateTimeV2Literal("2022-06-01T01:02:55+04:30");
        new DateTimeV2Literal("2022-06-01 01:02:55.123-07:30");
        new DateTimeV2Literal("2022-05-01 01:02:55+02:30");

        new DateTimeV2Literal("2022-05-01 01:02:55.123-02:30");
        new DateTimeV2Literal("2022-06-01T01:02:55+04:30");
        new DateTimeV2Literal("2022-06-01 01:02:55.123-07:30");

        new DateTimeV2Literal("20220701010255+07:00");
        new DateTimeV2Literal("20220701010255-05:00");
    }

    @Test
    @Disabled
    void testDateTimeZone() {
        new DateTimeV2Literal("0001-01-01 00:01:01");
        new DateTimeV2Literal("0001-01-01 00:01:01.001");
        new DateTimeV2Literal("0001-01-01 00:01:01.00305");

        new DateTimeV2Literal("2022-01-01 01:02:55");
        new DateTimeV2Literal("2022-01-01 01:02:55.123");
        new DateTimeV2Literal("2022-02-01 01:02:55Z");
        new DateTimeV2Literal("2022-02-01 01:02:55.123Z");
        new DateTimeV2Literal("2022-03-01 01:02:55UTC+8");
        new DateTimeV2Literal("2022-03-01 01:02:55.123UTC");
        new DateTimeV2Literal("2022-04-01 01:02:55UTC-6");
        new DateTimeV2Literal("2022-04-01T01:02:55UTC-6");
        new DateTimeV2Literal("2022-04-01T01:02:55.123UTC+6");

        new DateTimeV2Literal("2022-01-01 01:02:55");
        new DateTimeV2Literal("2022-01-01 01:02:55.123");
        new DateTimeV2Literal("2022-02-01 01:02:55Z");
        new DateTimeV2Literal("2022-02-01 01:02:55.123Z");
        new DateTimeV2Literal("2022-03-01 01:02:55UTC+8");
        new DateTimeV2Literal("2022-03-01 01:02:55.123UTC");
        new DateTimeV2Literal("2022-04-01T01:02:55UTC-6");

        new DateTimeV2Literal("0001-01-01");
    }

    @Test
    void testDateTimeZone1() {
        Consumer<DateTimeV2Literal> assertFunc = (datetime) -> {
            Assertions.assertEquals(2022, datetime.year);
            Assertions.assertEquals(1, datetime.month);
            Assertions.assertEquals(2, datetime.day);
            Assertions.assertEquals(12, datetime.hour);
            Assertions.assertEquals(0, datetime.minute);
            Assertions.assertEquals(0, datetime.second);
        };
        DateTimeV2Literal literal;
        literal = new DateTimeV2Literal("2022-01-02 12:00:00UTC+08:00");
        assertFunc.accept(literal);
        literal = new DateTimeV2Literal("2022-01-02 04:00:00UTC");
        assertFunc.accept(literal);
        literal = new DateTimeV2Literal("2022-01-01 20:00:00UTC-08:00");
        assertFunc.accept(literal);
        literal = new DateTimeV2Literal("2022-01-02 04:00:00Z");
        assertFunc.accept(literal);
    }

    @Test
    void testIrregularDateTime() {

        new DateTimeV2Literal("2016-7-02 01:01:00");
        new DateTimeV2Literal("2016-07-2 01:01:00");
        new DateTimeV2Literal("2016-7-2 01:01:00");

        new DateTimeV2Literal("2016-07-02 1:01:00");
        new DateTimeV2Literal("2016-07-02 01:1:00");
        new DateTimeV2Literal("2016-07-02 01:01:0");
        new DateTimeV2Literal("2016-07-02 1:1:00");
        new DateTimeV2Literal("2016-07-02 1:01:0");
        new DateTimeV2Literal("2016-07-02 10:1:0");
        new DateTimeV2Literal("2016-07-02 1:1:0");

        new DateTimeV2Literal("2016-7-2 1:1:0");
        new DateTimeV2Literal("2016-7-02 1:01:0");
        new DateTimeV2Literal("2016-07-2 1:1:0");
        new DateTimeV2Literal("2016-7-02 01:01:0");
        new DateTimeV2Literal("2016-7-2 01:1:0");
    }

    @Test
    void testIrregularDateTimeHour() {
        new DateTimeV2Literal("2016-07-02 01");
        new DateTimeV2Literal("2016-07-02 1");

        new DateTimeV2Literal("2016-7-02 1");
        new DateTimeV2Literal("2016-7-02 01");

        new DateTimeV2Literal("2016-07-2 1");
        new DateTimeV2Literal("2016-07-2 01");

        new DateTimeV2Literal("2016-7-2 1");
        new DateTimeV2Literal("2016-7-2 01");
    }

    @Test
    void testIrregularDateTimeHourMinute() {
        new DateTimeV2Literal("2016-07-02 01:01");
        new DateTimeV2Literal("2016-07-02 1:01");
        new DateTimeV2Literal("2016-07-02 01:1");
        new DateTimeV2Literal("2016-07-02 1:1");

        new DateTimeV2Literal("2016-7-02 01:01");
        new DateTimeV2Literal("2016-7-02 1:01");
        new DateTimeV2Literal("2016-7-02 01:1");
        new DateTimeV2Literal("2016-7-02 1:1");

        new DateTimeV2Literal("2016-07-2 01:01");
        new DateTimeV2Literal("2016-07-2 1:01");
        new DateTimeV2Literal("2016-07-2 01:1");
        new DateTimeV2Literal("2016-07-2 1:1");

        new DateTimeV2Literal("2016-7-2 01:01");
        new DateTimeV2Literal("2016-7-2 1:01");
        new DateTimeV2Literal("2016-7-2 01:1");
        new DateTimeV2Literal("2016-7-2 1:1");
    }

    @Test
    void testIrregularDateTimeHourMinuteSecond() {
        new DateTimeV2Literal("2016-07-02 01:01:01");
        new DateTimeV2Literal("2016-07-02 1:01:01");
        new DateTimeV2Literal("2016-07-02 01:1:01");
        new DateTimeV2Literal("2016-07-02 1:1:01");
        new DateTimeV2Literal("2016-07-02 01:01:1");
        new DateTimeV2Literal("2016-07-02 1:01:1");
        new DateTimeV2Literal("2016-07-02 01:1:1");
        new DateTimeV2Literal("2016-07-02 1:1:1");

        new DateTimeV2Literal("2016-7-02 01:01:01");
        new DateTimeV2Literal("2016-7-02 1:01:01");
        new DateTimeV2Literal("2016-7-02 01:1:01");
        new DateTimeV2Literal("2016-7-02 1:1:01");
        new DateTimeV2Literal("2016-7-02 01:01:1");
        new DateTimeV2Literal("2016-7-02 1:01:1");
        new DateTimeV2Literal("2016-7-02 01:1:1");
        new DateTimeV2Literal("2016-7-02 1:1:1");

        new DateTimeV2Literal("2016-07-2 01:01:01");
        new DateTimeV2Literal("2016-07-2 1:01:01");
        new DateTimeV2Literal("2016-07-2 01:1:01");
        new DateTimeV2Literal("2016-07-2 1:1:01");
        new DateTimeV2Literal("2016-07-2 01:01:1");
        new DateTimeV2Literal("2016-07-2 1:01:1");
        new DateTimeV2Literal("2016-07-2 01:1:1");
        new DateTimeV2Literal("2016-07-2 1:1:1");

        new DateTimeV2Literal("2016-7-2 01:01:01");
        new DateTimeV2Literal("2016-7-2 1:01:01");
        new DateTimeV2Literal("2016-7-2 01:1:01");
        new DateTimeV2Literal("2016-7-2 1:1:01");
        new DateTimeV2Literal("2016-7-2 01:01:1");
        new DateTimeV2Literal("2016-7-2 1:01:1");
        new DateTimeV2Literal("2016-7-2 01:1:1");
        new DateTimeV2Literal("2016-7-2 1:1:1");
    }

    @Test
    void testIrregularDateTimeHourMinuteSecondMicrosecond() {
        new DateTimeV2Literal("2016-07-02 01:01:01.1");
        new DateTimeV2Literal("2016-07-02 1:01:01.1");
        new DateTimeV2Literal("2016-07-02 01:1:01.1");
        new DateTimeV2Literal("2016-07-02 1:1:01.1");
        new DateTimeV2Literal("2016-07-02 01:01:1.1");
        new DateTimeV2Literal("2016-07-02 1:01:1.1");
        new DateTimeV2Literal("2016-07-02 01:1:1.1");
        new DateTimeV2Literal("2016-07-02 1:1:1.1");

        new DateTimeV2Literal("2016-7-02 01:01:01.1");
        new DateTimeV2Literal("2016-7-02 1:01:01.1");
        new DateTimeV2Literal("2016-7-02 01:1:01.1");
        new DateTimeV2Literal("2016-7-02 1:1:01.1");
        new DateTimeV2Literal("2016-7-02 01:01:1.1");
        new DateTimeV2Literal("2016-7-02 1:01:1.1");
        new DateTimeV2Literal("2016-7-02 01:1:1.1");
        new DateTimeV2Literal("2016-7-02 1:1:1.1");

        new DateTimeV2Literal("2016-07-2 01:01:01.1");
        new DateTimeV2Literal("2016-07-2 1:01:01.1");
        new DateTimeV2Literal("2016-07-2 01:1:01.1");
        new DateTimeV2Literal("2016-07-2 1:1:01.1");
        new DateTimeV2Literal("2016-07-2 01:01:1.1");
        new DateTimeV2Literal("2016-07-2 1:01:1.1");
        new DateTimeV2Literal("2016-07-2 01:1:1.1");
        new DateTimeV2Literal("2016-07-2 1:1:1.1");

        new DateTimeV2Literal("2016-7-2 01:01:01.1");
        new DateTimeV2Literal("2016-7-2 1:01:01.1");
        new DateTimeV2Literal("2016-7-2 01:1:01.1");
        new DateTimeV2Literal("2016-7-2 1:1:01.1");
        new DateTimeV2Literal("2016-7-2 01:01:1.1");
        new DateTimeV2Literal("2016-7-2 1:01:1.1");
        new DateTimeV2Literal("2016-7-2 01:1:1.1");
        new DateTimeV2Literal("2016-7-2 1:1:1.1");

        // Testing with microsecond of length 2
        new DateTimeV2Literal("2016-07-02 01:01:01.12");
        new DateTimeV2Literal("2016-7-02 01:01:01.12");

        // Testing with microsecond of length 3
        new DateTimeV2Literal("2016-07-02 01:01:01.123");
        new DateTimeV2Literal("2016-7-02 01:01:01.123");

        // Testing with microsecond of length 4
        new DateTimeV2Literal("2016-07-02 01:01:01.1234");
        new DateTimeV2Literal("2016-7-02 01:01:01.1234");

        // Testing with microsecond of length 5
        new DateTimeV2Literal("2016-07-02 01:01:01.12345");
        new DateTimeV2Literal("2016-7-02 01:01:01.12345");

        // Testing with microsecond of length 6
        new DateTimeV2Literal("2016-07-02 01:01:01.123456");
        new DateTimeV2Literal("2016-7-02 01:01:01.123456");

        // Testing with microsecond of length 7
        DateTimeV2Literal literal = new DateTimeV2Literal("2016-07-02 01:01:01.12345678");
        Assertions.assertEquals(123457, literal.microSecond);

        literal = new DateTimeV2Literal("2016-07-02 01:01:01.44444444");
        Assertions.assertEquals(444444, literal.microSecond);

        literal = new DateTimeV2Literal("2016-07-02 01:01:01.44444445");
        Assertions.assertEquals(444444, literal.microSecond);

        literal = new DateTimeV2Literal("2016-07-02 01:01:01.4444445");
        Assertions.assertEquals(444445, literal.microSecond);

        literal = new DateTimeV2Literal("2016-07-02 01:01:01.9999995");
        Assertions.assertEquals(0, literal.microSecond);
        Assertions.assertEquals(2, literal.second);

        literal = new DateTimeV2Literal("2021-01-01 23:59:59.9999995");
        Assertions.assertEquals(0, literal.microSecond);
        Assertions.assertEquals(0, literal.second);
        Assertions.assertEquals(0, literal.minute);
        Assertions.assertEquals(0, literal.hour);
    }

    @Test
    void testDateTimeV2Scale() {
        Assertions.assertEquals(
                new DateTimeV2Literal(DateTimeV2Type.of(3), "2016-07-02 00:00:00.123"),
                new DateTimeV2Literal(DateTimeV2Type.of(3), "2016-07-02 00:00:00.123"));

        Assertions.assertEquals(
                new DateTimeV2Literal(DateTimeV2Type.of(3), "2016-07-02 00:00:00.123456"),
                new DateTimeV2Literal(DateTimeV2Type.of(3), "2016-07-02 00:00:00.123"));

        Assertions.assertEquals(
                new DateTimeV2Literal(DateTimeV2Type.of(4), "2016-07-02 00:00:00.12345"),
                new DateTimeV2Literal(DateTimeV2Type.of(4), "2016-07-02 00:00:00.1235"));

        Assertions.assertEquals(
                new DateTimeV2Literal(DateTimeV2Type.of(0), "2016-07-02 00:00:00.12345"),
                new DateTimeV2Literal(DateTimeV2Type.of(0), "2016-07-02 00:00:00"));

        Assertions.assertEquals(
                new DateTimeV2Literal(DateTimeV2Type.of(0), "2016-07-02 00:00:00.5123"),
                new DateTimeV2Literal(DateTimeV2Type.of(0), "2016-07-02 00:00:01"));

        Assertions.assertEquals(
                new DateTimeV2Literal(DateTimeV2Type.of(5), "2016-07-02 00:00:00.999999"),
                new DateTimeV2Literal(DateTimeV2Type.of(5), "2016-07-02 00:00:01.00000"));

        // test overflow
        Assertions.assertEquals(
                new DateTimeV2Literal(DateTimeV2Type.of(5), "2016-12-31 23:59:59.999999"),
                new DateTimeV2Literal(DateTimeV2Type.of(5), "2017-01-01 00:00:00.00000"));
    }

    @Test
    void testRoundFloor() {
        DateTimeV2Literal literal;
        literal = new DateTimeV2Literal(DateTimeV2Type.of(6), 2000, 2, 2, 2, 2, 2, 222222);
        Assertions.assertEquals(222222, literal.roundFloor(6).microSecond);
        Assertions.assertEquals(222220, literal.roundFloor(5).microSecond);
        Assertions.assertEquals(222200, literal.roundFloor(4).microSecond);
        Assertions.assertEquals(222000, literal.roundFloor(3).microSecond);
        Assertions.assertEquals(220000, literal.roundFloor(2).microSecond);
        Assertions.assertEquals(200000, literal.roundFloor(1).microSecond);
        Assertions.assertEquals(0, literal.roundFloor(0).microSecond);
    }

    @Test
    void testRoundCeiling() {
        DateTimeV2Literal literal;
        literal = new DateTimeV2Literal(DateTimeV2Type.of(6), 2000, 12, 31, 23, 59, 59, 111111);
        Assertions.assertEquals(111111, literal.roundCeiling(6).microSecond);
        Assertions.assertEquals(111120, literal.roundCeiling(5).microSecond);
        Assertions.assertEquals(111200, literal.roundCeiling(4).microSecond);
        Assertions.assertEquals(112000, literal.roundCeiling(3).microSecond);
        Assertions.assertEquals(120000, literal.roundCeiling(2).microSecond);
        Assertions.assertEquals(200000, literal.roundCeiling(1).microSecond);
        Assertions.assertEquals(0, literal.roundCeiling(0).microSecond);
        Assertions.assertEquals(0, literal.roundCeiling(0).second);
        Assertions.assertEquals(0, literal.roundCeiling(0).minute);
        Assertions.assertEquals(0, literal.roundCeiling(0).hour);
        Assertions.assertEquals(1, literal.roundCeiling(0).day);
        Assertions.assertEquals(1, literal.roundCeiling(0).month);
        Assertions.assertEquals(2001, literal.roundCeiling(0).year);

        literal = new DateTimeV2Literal(DateTimeV2Type.of(6), 2000, 12, 31, 23, 59, 59, 888888);
        Assertions.assertEquals(888888, literal.roundCeiling(6).microSecond);
        Assertions.assertEquals(888890, literal.roundCeiling(5).microSecond);
        Assertions.assertEquals(888900, literal.roundCeiling(4).microSecond);
        Assertions.assertEquals(889000, literal.roundCeiling(3).microSecond);
        Assertions.assertEquals(890000, literal.roundCeiling(2).microSecond);
        Assertions.assertEquals(900000, literal.roundCeiling(1).microSecond);
        Assertions.assertEquals(0, literal.roundCeiling(0).microSecond);
        Assertions.assertEquals(0, literal.roundCeiling(0).second);
        Assertions.assertEquals(0, literal.roundCeiling(0).minute);
        Assertions.assertEquals(0, literal.roundCeiling(0).hour);
        Assertions.assertEquals(1, literal.roundCeiling(0).day);
        Assertions.assertEquals(1, literal.roundCeiling(0).month);
        Assertions.assertEquals(2001, literal.roundCeiling(0).year);
    }
}
