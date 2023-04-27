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

package org.apache.doris.nereids.expression;

import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeArithmetic;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeExtractAndTransform;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.DateTimeV2Type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestFoldConstantByFe {

    @Test
    public void testDateTypeDateTimeArithmeticFunctions() {
        DateLiteral dateLiteral = new DateLiteral("1999-12-31");
        IntegerLiteral integerLiteral = new IntegerLiteral(30);
        VarcharLiteral format = new VarcharLiteral("%Y-%m-%d");

        String[] answer = {
                "2000-01-30", "1999-12-01", "2029-12-31", "1969-12-31",
                "2002-06-30", "1997-06-30", "2000-01-30", "1999-12-01",
                "1999", "4", "12", "6", "31", "365", "31",
                "'1999-12-31'", "1999-12-27", "1999-12-31"
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
    public void testDateTimeTypeDateTimeArithmeticFunctions() {
        DateTimeLiteral dateLiteral = new DateTimeLiteral("1999-12-31 23:59:59");
        IntegerLiteral integerLiteral = new IntegerLiteral(30);
        VarcharLiteral format = new VarcharLiteral("%Y-%m-%d");

        String[] answer = {
                "2000-01-30 23:59:59", "1999-12-01 23:59:59", "2029-12-31 23:59:59", "1969-12-31 23:59:59",
                "2002-06-30 23:59:59", "1997-06-30 23:59:59", "2000-01-30 23:59:59", "1999-12-01 23:59:59",
                "2000-01-02 05:59:59", "1999-12-30 17:59:59", "2000-01-01 00:29:59",
                "1999-12-31 23:29:59", "2000-01-01 00:00:29", "1999-12-31 23:59:29",
                "1999", "4", "12", "6", "31", "365", "31", "23", "59", "59",
                "'1999-12-31'", "1999-12-27", "1999-12-31", "1999-12-31", "730484", "1999-12-31"
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
                "2000-01-30", "1999-12-01", "2029-12-31", "1969-12-31",
                "2002-06-30", "1997-06-30", "2000-01-30", "1999-12-01",
                "1999", "4", "12", "6", "31", "365", "31",
                "'1999-12-31'", "1999-12-27", "1999-12-31"
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
        DateTimeV2Literal dateLiteral = new DateTimeV2Literal(DateTimeV2Type.MAX, "1999-12-31 23:59:59");
        IntegerLiteral integerLiteral = new IntegerLiteral(30);
        VarcharLiteral format = new VarcharLiteral("%Y-%m-%d");

        String[] answer = {
                "2000-01-30 23:59:59", "1999-12-01 23:59:59", "2029-12-31 23:59:59", "1969-12-31 23:59:59",
                "2002-06-30 23:59:59", "1997-06-30 23:59:59", "2000-01-30 23:59:59", "1999-12-01 23:59:59",
                "2000-01-02 05:59:59", "1999-12-30 17:59:59", "2000-01-01 00:29:59",
                "1999-12-31 23:29:59", "2000-01-01 00:00:29", "1999-12-31 23:59:29", "1999-12-31 23:59:59",
                "1999", "4", "12", "6", "31", "365", "31", "23", "59", "59",
                "'1999-12-31'", "1999-12-27", "1999-12-31", "1999-12-31", "730484", "1999-12-31", "1999-12-31"
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
                "2001-01-01 00:00:00", "2001-01-01 00:00:00", "2001-12-01 00:00:00", "2001-12-01 00:00:00",
                "2001-12-31 00:00:00", "2001-12-31 00:00:00", "2001-12-31 01:00:00", "2001-12-31 01:00:00",
                "2001-12-31 01:01:00", "2001-12-31 01:01:00", "2001-12-31 01:01:01", "2001-12-31 01:01:01",
                "2001-01-01 00:00:00", "2001-01-01 00:00:00", "2001-01-01 00:00:00",
                "2001-04-01 00:00:00", "2001-04-01 00:00:00", "2001-04-01 00:00:00",
                "2001-07-01 00:00:00", "2001-07-01 00:00:00", "2001-07-01 00:00:00",
                "2001-10-01 00:00:00", "2001-10-01 00:00:00", "2001-10-01 00:00:00",
                "2001-01-15 00:00:00", "2001-02-12 00:00:00", "2001-03-12 00:00:00",
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
                "2001-07-19", "6411-08-17", "'1977-06-03 17:57:24'",
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
        Assertions.assertEquals(DateTimeExtractAndTransform.fromUnixTime(
                new IntegerLiteral(234179844)
        ).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.fromUnixTime(
                new IntegerLiteral(234179844),
                new VarcharLiteral("%Y-%m-%d")
        ).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.unixTimestamp(
                new VarcharLiteral("2001-12-21 12:34:53")
        ).toSql(), answer[answerIdx++]);
        Assertions.assertEquals(DateTimeExtractAndTransform.unixTimestamp(
                new VarcharLiteral("2001-12-21"),
                new VarcharLiteral("%Y-%m-%d")
        ).toSql(), answer[answerIdx]);
    }
}
